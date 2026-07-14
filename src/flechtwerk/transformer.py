"""Transformer base class and runner for event-driven stream processing."""
import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import AsyncIterator, Never

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from reactor_di import lookup

from .configs import ConfigStore, EnrichConfigFn, bootstrap_config_store, drain_config_updates
from .kafka import encode_json, datetime_to_millis, parse_message
from .observer import Observer
from .stage import ExtractStateKeyFn, Stage
from .state import ChangelogStateStore, StateStore
from .types import IncomingMessage, Message, State

log = logging.getLogger(__name__)

TransformFn = Callable[[IncomingMessage, State], AsyncIterator[Message | State]]


@dataclass(frozen=True, slots=True)
class BucketResult:
    """One state-key bucket's contribution to a batch.

    ``state_change`` is ``None`` when nothing should be written to the
    state store — either because the transform didn't yield ``State``,
    or because what it yielded matched the baseline.
    """
    outputs: list[Message]
    state_change: State | None


class Transformer(Stage, ABC):
    """Event transformer (stateless or stateful).

    Three ways to construct one:

    * Declaratively with the ``@transformer(...)`` decorator, which binds a
      transform function to its input topics — the decorated name becomes the
      stage::

          @transformer(input_topics=["my-topic"])
          async def stage(msg, state):
              ...

    * Functionally with ``Transformer.of(...)``, the factory the decorator
      wraps, when the transform function must stay callable under its own name::

          stage = Transformer.of(
              input_topics=["my-topic"],
              transform=my_transform_fn,
          )

    * As a subclass for lifecycle management (HTTP clients, dedup instances)::

          class MyTransformer(Transformer):
              input_topics = ["my-topic"]

              async def __aenter__(self):
                  self.http = httpx.AsyncClient()
                  return self

              async def __aexit__(self, *exc_info):
                  await self.http.aclose()

              async def transform(self, msg, state):
                  ...

    The Kafka consumer group ID (driving consumer group membership,
    transactional offset commits, and changelog topic naming) is set on
    `Flechtwerk` by the caller; stages don't carry it.

    A transformer may additionally declare ``config_topics`` (see `Stage`)
    and look their entries up via `configs` — a config table joined against
    the partitioned input stream.
    """

    input_topics: list[str]

    configs: ConfigStore
    """The stage's config store, injected by the runner before ``__aenter__``.

    Keyed by wire key, merged across all declared ``config_topics``.
    Lookups are eventually consistent: the store is updated between batches
    and is NOT part of any task transaction (Kafka Streams' GlobalKTable
    caveat) — a record produced to a config topic is visible here no later
    than the next batch. Treat it as **read-only** — look entries up with
    ``configs.get(key)``; mutating the store (``put``/``delete``) from stage
    code is an error (see `ConfigStore`). Tests seed this directly::

        stage.configs = ConfigStore.of({key: config})
    """

    @classmethod
    def of(
            cls,
            *,
            input_topics: list[str],
            transform: TransformFn,
            enrich_config: EnrichConfigFn | None = None,
            extract_state_key: ExtractStateKeyFn | None = None,
    ) -> "Transformer":
        """Build a Transformer from a transform function and input topics.

        Use this for stateless or simply-stateful stages that don't need
        lifecycle management; subclass directly for stages that own resources
        (HTTP clients, dedup instances, etc.).

        Patches the supplied callables in as instance attributes that
        shadow the class-level abstract method ``transform`` (and, when
        provided, the default ``enrich_config`` / ``extract_state_key``). The ABC
        discipline still applies to every other construction path —
        ``Transformer()`` and any abstract subclass remain uninstantiable.
        """
        instance = _FunctionalTransformer()
        instance.input_topics = input_topics
        instance.transform = transform
        if enrich_config is not None:
            instance.enrich_config = enrich_config
        if extract_state_key is not None:
            instance.extract_state_key = extract_state_key
        return instance

    @abstractmethod
    def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        """Transform an incoming message into zero or more output Messages.

        Declared without ``async`` so that implementations — ``async def``
        functions containing ``yield``, i.e. async generator functions whose
        call returns an ``AsyncIterator`` directly — are compatible overrides
        under strict type checking. A coroutine-typed abstract (``async def``
        with no ``yield``) would make every real override incompatible.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted (stateless behavior). Yielding an empty/falsy State deletes
        the entry from the state store (and writes a Kafka tombstone to the
        changelog) atomically with the output messages.

        Both parameters are read-only. The runner hands ``state`` a defensive
        copy and never reuses ``msg`` after the call, so mutating either in
        place has no effect — it is silently discarded. Produce output only by
        yielding, and enrich by spreading (``Event({**msg.value, ...})``) rather
        than mutating in place.
        """


class _FunctionalTransformer(Transformer):
    """Shell subclass used solely as the instantiation target for ``Transformer.of``.

    The class-level ``transform = None`` is a placeholder that satisfies
    ``ABCMeta``'s abstract-method check; ``of()`` shadows it with an
    instance attribute on every call.
    """
    transform = None  # type: ignore[assignment]


def transformer(
        *,
        input_topics: list[str],
        enrich_config: EnrichConfigFn | None = None,
        extract_state_key: ExtractStateKeyFn | None = None,
) -> Callable[[TransformFn], Transformer]:
    """Decorator form of `Transformer.of` — bind a transform function to its input topics.

    The decorated async generator becomes the built `Transformer`, so the name
    you define *is* the stage, ready to hand to `Flechtwerk.of`::

        @transformer(input_topics=["my-input"])
        async def stage(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
            ...

    ``enrich_config`` and ``extract_state_key`` are the same optional overrides as on
    `Transformer.of` — this is exactly that call with ``transform`` supplied by
    the decoration. Call `Transformer.of` directly when the transform function
    must stay callable under its own name, and subclass `Transformer` when you
    need lifecycle management (``__aenter__`` / ``__aexit__``).
    """
    def decorator(transform: TransformFn) -> Transformer:
        return Transformer.of(
            enrich_config=enrich_config,
            extract_state_key=extract_state_key,
            input_topics=input_topics,
            transform=transform,
        )
    return decorator


@dataclass(slots=True)
class Task:
    """Per-input-partition processing context.

    A task owns partition ``partition`` of every input topic, a transactional
    producer whose static transactional ID fences any previous owner of the
    same task (Kafka Streams EOS-v1 — aiokafka has no KIP-447 generation
    fencing), and a partition-scoped state store restored from the matching
    changelog partition. State identity is *(partition, extract_state_key)* — the
    framework makes no assumptions about what ``extract_state_key()`` returns.
    """
    partition: int
    producer: AIOKafkaProducer
    store: ChangelogStateStore


class TaskRebalanceListener(ConsumerRebalanceListener):
    """Bridges aiokafka's eager rebalance protocol to the runner's task lifecycle.

    aiokafka logs-and-swallows exceptions raised inside these callbacks, so
    "let it crash" cannot fire from here — failures are recorded on the
    runner's ``fatal`` slot and re-raised by the main loop instead.
    """

    def __init__(self, runner: "TransformerRunner") -> None:
        self.runner = runner

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        try:
            # The batch lock guarantees no transaction is in flight while
            # tasks are torn down; the rebalance blocks until the current
            # batch (if any) commits. Heartbeats keep flowing meanwhile —
            # aiokafka runs this callback before it rejoins the group.
            async with self.runner.batch_lock:
                await self.runner.close_tasks()
        except Exception as e:
            self.runner.fatal = e

    def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        try:
            # Bookkeeping only — no I/O. Restoring state here would stall the
            # whole group past rebalance_timeout_ms. The paused partitions are
            # resumed one task at a time as the main loop initializes them.
            self.runner.consumer.pause(*assigned)
            self.runner.pending = {tp.partition for tp in assigned}
        except Exception as e:
            self.runner.fatal = e


class TransformerRunner:
    """Runs a Transformer as a Kafka consumer-producer loop with exactly-once semantics.

    Work is partitioned into per-input-partition tasks (see ``Task``). Each
    ``getmany()`` batch commits one Kafka transaction per task — that task's
    output messages, state changelog writes, and consumer offset commits are
    atomic. Tasks are torn down on partition revocation and rebuilt from
    scratch on assignment (fresh producer fences any previous owner, state
    re-restored from the task's changelog partition) — retaining either
    across a rebalance is unsafe once a rebalance has been missed.

    A small in-memory state overlay scoped to the batch ensures that records
    sharing a (task, state key) bucket see each other's yielded mutations
    within the batch; only the final state per key is written to the
    changelog at commit time.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    Each task's store shares that task's producer — state writes inside
    send_transactional() participate in the same Kafka transaction.
    """

    application_id: str
    config_consumer: AIOKafkaConsumer | None
    config_store: ConfigStore
    consumer: AIOKafkaConsumer
    observer: Observer
    create_restore_consumer: Callable[[], AIOKafkaConsumer]
    create_task_producer: Callable[[int], AIOKafkaProducer]
    create_task_store: Callable[[int, AIOKafkaProducer], ChangelogStateStore]
    transformer: lookup[Transformer, "configured_stage"]  # noqa: PyUnresolvedReferences

    def __init__(self) -> None:
        self.batch_lock = asyncio.Lock()
        self.fatal: BaseException | None = None
        self.pending: set[int] = set()
        self.tasks: dict[int, Task] = {}

    async def run(self) -> Never:
        """Main event loop. Consumes batches and processes each transactionally.

        The batch lock covers task initialization and batch processing, and
        the revoke callback acquires it before tearing tasks down — so a
        rebalance can never interleave with an open transaction. The fetch
        itself MUST stay outside the lock: getmany() blocks until an active
        rebalance completes, and the rebalance waits for the revoke callback,
        so fetching under the lock deadlocks the consumer. The price is that
        a rebalance can strike between fetch and processing — records whose
        task was torn down are dropped (their offsets were never committed;
        the new owner reprocesses them).

        Resource lifecycle (consumer start/stop) is managed by Flechtwerk, not
        the runner; per-task producers and stores are owned by the runner.

        The config store is instance-level, not per-task: it is
        bootstrapped once, BEFORE the subscribe joins the consumer group (a
        failing bootstrap crashes without causing rebalance churn for
        healthy instances), and rebalances never touch it.
        """
        if self.config_consumer is not None:
            await bootstrap_config_store(
                self.config_consumer, self.transformer.config_topics,
                self.config_store, self.transformer.enrich_config,
            )
            self.observer.config_store_restored(len(self.config_store))
            self.observer.config_store_entries(len(self.config_store))
        self.transformer.configs = self.config_store
        self.consumer.subscribe(self.transformer.input_topics, listener=TaskRebalanceListener(self))
        try:
            async with self.transformer:
                while True:
                    if self.fatal is not None:
                        raise self.fatal
                    async with self.batch_lock:
                        await self.start_pending_tasks()
                    records = await self.consumer.getmany(timeout_ms=1000)
                    await self.check_config_updates()
                    if not records:
                        continue
                    async with self.batch_lock:
                        records = {tp: msgs for tp, msgs in records.items() if tp.partition in self.tasks}
                        if records:
                            await self.process_batch(records)
        finally:
            # The rebalance listener is NOT invoked on consumer.stop() —
            # live tasks must be torn down explicitly on the way out.
            await self.close_tasks()

    async def check_config_updates(self) -> None:
        """Non-blocking check for config changes.

        Runs once per loop iteration, outside the batch lock — it touches no
        tasks or transactions, and the loop is sequential, so every record
        of a batch sees one consistent config snapshot (updates land at
        batch boundaries; that is slightly stronger than Kafka Streams'
        concurrent GlobalKTable updates).
        """
        if self.config_consumer is None:
            return
        records = await drain_config_updates(self.config_consumer, self.config_store, self.transformer.enrich_config)
        for msg in records:
            self.observer.config_message_in(msg.topic)
        if records:
            self.observer.config_store_entries(len(self.config_store))

    async def start_pending_tasks(self) -> None:
        """Initialize every task marked pending by the rebalance listener."""
        pending, self.pending = self.pending, set()
        if not pending:
            return
        await asyncio.gather(*(self.start_task(p) for p in sorted(pending)))
        self.observer.tasks_assigned(len(self.tasks))
        log.info("Initialized tasks %s (now running %d)", sorted(pending), len(self.tasks))

    async def start_task(self, partition: int) -> None:
        """Fence the previous owner, restore state, resume fetching — in that order.

        ``producer.start()`` issues InitProducerId, which bumps the producer
        epoch for this task's static transactional ID: any previous owner's
        in-flight transaction is aborted and its producer fenced. Only then
        is the changelog end offset final, so the restore MUST come after.
        """
        producer = self.create_task_producer(partition)
        await producer.start()
        store = self.create_task_store(partition, producer)
        consumer = self.create_restore_consumer()
        await consumer.start()
        try:
            entries = await store.restore(consumer, partitions={partition})
        finally:
            await consumer.stop()
        self.tasks[partition] = Task(partition, producer, store)
        self.observer.state_restored(partition, entries)
        self.consumer.resume(*(tp for tp in self.consumer.assignment() if tp.partition == partition))

    async def close_tasks(self) -> None:
        """Tear down every live task: producers stopped, stores closed and deleted."""
        tasks, self.tasks = self.tasks, {}
        if not tasks:
            return
        await asyncio.gather(*(task.producer.stop() for task in tasks.values()))
        await asyncio.gather(*(task.store.close() for task in tasks.values()))
        self.observer.tasks_assigned(0)
        log.info("Closed tasks %s", sorted(tasks))

    async def process_batch(self, records: dict) -> None:
        """Process all records in a getmany batch, one transaction per task.

        Records are bucketed by (task, state key). Same-bucket records are
        processed serially (so each one sees the previous one's yielded
        state); buckets run concurrently via ``asyncio.gather``, which lets
        I/O-bound ``transform()`` calls overlap. Cross-bucket ordering is not
        preserved. Within a bucket, records appear in ``input_topics`` order
        then Kafka offset order. Task transactions commit independently and
        concurrently — each covers exactly one task's outputs, state changes,
        and offsets.
        """
        topic_order = {t: i for i, t in enumerate(self.transformer.input_topics)}
        ordered_tps = sorted(records, key=lambda p: topic_order[p.topic])

        total = sum(len(msgs) for msgs in records.values())

        buckets: dict[tuple[int, str], list] = {}
        offsets: dict[int, dict[TopicPartition, int]] = {}

        for tp in ordered_tps:
            task_offsets = offsets.setdefault(tp.partition, {})
            for raw_msg in records[tp]:
                msg = parse_message(raw_msg)
                key = self.transformer.extract_state_key(msg)
                buckets.setdefault((tp.partition, key), []).append(msg)
                task_offsets[tp] = max(task_offsets.get(tp, 0), msg.offset + 1)

        with self.observer.batch_scope(total):
            results = await asyncio.gather(*(
                self._process_key_bucket(self.tasks[partition].store, key, msgs)
                for (partition, key), msgs in buckets.items()
            ))

            output: dict[int, list[Message]] = {}
            state_changes: dict[int, dict[str, State]] = {}
            for (partition, key), result in zip(buckets, results):
                output.setdefault(partition, []).extend(result.outputs)
                if result.state_change is not None:
                    state_changes.setdefault(partition, {})[key] = result.state_change

            await asyncio.gather(*(
                self.send_transactional(
                    self.tasks[partition],
                    output.get(partition, []),
                    state_changes.get(partition, {}),
                    task_offsets,
                )
                for partition, task_offsets in offsets.items()
            ))

    async def _process_key_bucket(self, store: StateStore, key: str, msgs: list) -> BucketResult:
        """Process all records sharing one (task, state key) bucket, serially."""
        baseline = State(await store.get(key) or {})
        current = baseline
        final_state: State | None = None
        outputs: list[Message] = []

        for msg in msgs:
            self.observer.message_in(msg.topic)
            # Defensive copy: in-place mutation without a yield must not
            # leak into either the running state or a later same-key record.
            state_for_call = deepcopy(current)
            with self.observer.dispatch_scope():
                async for item in self.transformer.transform(msg, state_for_call):
                    if isinstance(item, State):
                        current = item
                        final_state = item
                    elif isinstance(item, Message):
                        outputs.append(item)
                        self.observer.message_out(item.topic)
                    else:
                        raise TypeError(
                            f"transform() yielded {type(item).__name__}, expected Message or State"
                        )

        changed = final_state is not None and final_state != baseline
        return BucketResult(outputs, final_state if changed else None)

    async def send_transactional(
            self,
            task: Task,
            messages: list[Message],
            state_changes: dict[str, State],
            offsets: dict,
    ) -> None:
        """Send one task's messages, state, and offsets in a single Kafka transaction.

        ``state_changes`` is the per-key final state for keys whose value
        differs from baseline (already filtered by the caller). A truthy
        value is ``put``; a falsy value is ``delete`` (writing a Kafka
        tombstone via the same transactional producer).
        """
        async with task.producer.transaction():
            for msg in messages:
                await task.producer.send(
                    msg.topic,
                    key=encode_json(msg.key),
                    value=encode_json(msg.value),
                    timestamp_ms=datetime_to_millis(msg.timestamp),
                )
            for key, new_state in state_changes.items():
                if new_state:
                    await task.store.put(key, new_state)
                else:
                    await task.store.delete(key)
            await task.producer.send_offsets_to_transaction(offsets, self.application_id)

        self.observer.transaction_committed()
        log.debug("Task %d transaction committed: %d messages, %d state changes",
                  task.partition, len(messages), len(state_changes))
