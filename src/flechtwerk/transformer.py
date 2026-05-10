"""Transformer base class and runner for event-driven stream processing."""
import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import AsyncIterator, Never

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reactor_di import lookup

from .kafka import encode_json, datetime_to_millis, parse_message
from .observer import Observer
from .state import StateStore
from .types import IncomingMessage, Message, State

log = logging.getLogger(__name__)

TransformFn = Callable[[IncomingMessage, State], AsyncIterator[Message | State]]
ExtractKeyFn = Callable[[IncomingMessage], str]


@dataclass(frozen=True, slots=True)
class BucketResult:
    """One state-key bucket's contribution to a batch.

    ``state_change`` is ``None`` when nothing should be written to the
    state store — either because the transform didn't yield ``State``,
    or because what it yielded matched the baseline.
    """
    outputs: list[Message]
    state_change: State | None


class Transformer(ABC):
    """Event transformer (stateless or stateful).

    Two ways to construct one:

    * Functionally with ``Transformer.of(...)``, supplying a transform
      function and the input topics::

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
    `Fretworx` by the caller; stages don't carry it.
    """

    input_topics: list[str]

    @classmethod
    def of(
            cls,
            *,
            input_topics: list[str],
            transform: TransformFn,
            extract_key: ExtractKeyFn | None = None,
    ) -> Transformer:
        """Build a Transformer from a transform function and input topics.

        Use this for stateless or simply-stateful stages that don't need
        lifecycle management; subclass directly for stages that own resources
        (HTTP clients, dedup instances, etc.).

        Patches the supplied callables in as instance attributes that
        shadow the class-level abstract method ``transform`` (and, when
        provided, the default ``extract_key``). The ABC discipline still
        applies to every other construction path — ``Transformer()`` and
        any abstract subclass remain uninstantiable.
        """
        instance = _FunctionalTransformer()
        instance.input_topics = input_topics
        instance.transform = transform
        if extract_key is not None:
            instance.extract_key = extract_key
        return instance

    def extract_key(self, msg: IncomingMessage) -> str:
        """Extract the state key from the incoming message. Default: msg.key."""
        return msg.key

    async def __aenter__(self) -> Transformer:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    @abstractmethod
    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        """Transform an incoming message into zero or more output Messages.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted (stateless behavior). Yielding an empty/falsy State deletes
        the entry from the state store (and writes a Kafka tombstone to the
        changelog) atomically with the output messages.
        """


class _FunctionalTransformer(Transformer):
    """Shell subclass used solely as the instantiation target for ``Transformer.of``.

    The class-level ``transform = None`` is a placeholder that satisfies
    ``ABCMeta``'s abstract-method check; ``of()`` shadows it with an
    instance attribute on every call.
    """
    transform = None  # type: ignore[assignment]


class TransformerRunner:
    """Runs a Transformer as a Kafka consumer-producer loop with exactly-once semantics.

    One Kafka transaction per ``getmany()`` batch — output messages, state
    changelog writes, and the consumer offset commit are all atomic.
    A small in-memory state overlay scoped to the batch ensures that
    records sharing a state key see each other's yielded mutations within
    the batch; only the final state per key is written to the changelog
    at commit time.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the ChangelogStateStore — state writes inside
    send_transactional() participate in the same Kafka transaction.
    """

    application_id: str
    consumer: AIOKafkaConsumer
    observer: Observer
    producer: AIOKafkaProducer
    state_store: StateStore
    transformer: lookup[Transformer, "stage"]  # noqa: PyUnresolvedReferences

    async def run(self) -> Never:
        """Main event loop. Consumes batches and processes each transactionally.

        Resource lifecycle (consumer/producer start/stop) is managed by
        Fretworx, not the runner.
        """
        self.consumer.subscribe(self.transformer.input_topics)
        async with self.transformer:
            while True:
                records = await self.consumer.getmany(timeout_ms=1000)
                if not records:
                    continue
                await self.process_batch(records)

    async def process_batch(self, records: dict) -> None:
        """Process all records in a getmany batch under a single transaction.

        Records are bucketed by state key. Same-key records are processed
        serially inside their bucket (so each one sees the previous one's
        yielded state); buckets run concurrently via ``asyncio.gather``,
        which lets I/O-bound ``transform()`` calls overlap. Cross-key
        ordering is not preserved. Within a bucket, records appear in
        ``input_topics`` order then Kafka offset order.
        """
        topic_order = {t: i for i, t in enumerate(self.transformer.input_topics)}
        ordered_tps = sorted(records, key=lambda p: topic_order[p.topic])

        total = sum(len(msgs) for msgs in records.values())

        buckets: dict[str, list] = {}
        offsets: dict = {}

        for tp in ordered_tps:
            for raw_msg in records[tp]:
                msg = parse_message(raw_msg)
                key = self.transformer.extract_key(msg)
                buckets.setdefault(key, []).append(msg)
                offsets[tp] = max(offsets.get(tp, 0), msg.offset + 1)

        with self.observer.batch_scope(total):
            results = await asyncio.gather(*(
                self._process_key_bucket(key, msgs) for key, msgs in buckets.items()
            ))

            output: list[Message] = []
            state_changes: dict[str, State] = {}
            for key, result in zip(buckets, results):
                output.extend(result.outputs)
                if result.state_change is not None:
                    state_changes[key] = result.state_change

            await self.send_transactional(output, state_changes, offsets)

    async def _process_key_bucket(self, key: str, msgs: list) -> BucketResult:
        """Process all records sharing one state key, serially."""
        baseline = State(await self.state_store.get(key) or {})
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
            messages: list[Message],
            state_changes: dict[str, State],
            offsets: dict,
    ) -> None:
        """Send messages, persist state, and commit offsets in a single Kafka transaction.

        ``state_changes`` is the per-key final state for keys whose value
        differs from baseline (already filtered by the caller). A truthy
        value is ``put``; a falsy value is ``delete`` (writing a Kafka
        tombstone via the same transactional producer).
        """
        async with self.producer.transaction():
            for msg in messages:
                await self.producer.send(
                    msg.topic,
                    key=encode_json(msg.key),
                    value=encode_json(msg.value),
                    timestamp_ms=datetime_to_millis(msg.timestamp),
                )
            for key, new_state in state_changes.items():
                if new_state:
                    await self.state_store.put(key, new_state)
                else:
                    await self.state_store.delete(key)
            await self.producer.send_offsets_to_transaction(offsets, self.application_id)

        self.observer.transaction_committed()
        log.debug("Transaction committed: %d messages, %d state changes",
                  len(messages), len(state_changes))
