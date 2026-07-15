"""Extractor base class and runner for poll-driven data extraction."""
import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator, Never

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.partitioner import murmur2
from reactor_di import lookup

from flechtwerk.attribute import Attribute, BOOL
from .configs import ConfigStore, EnrichConfigFn, bootstrap_config_store, drain_config_updates
from .kafka import encode_json, datetime_to_millis, parse_message
from .observer import Observer
from .stage import ExtractStateKeyFn, Stage
from .state import ChangelogStateStore
from .types import Config, IncomingMessage, Message, State

log = logging.getLogger(__name__)

SUSPENDED = Attribute("suspended", BOOL, optional=True)

PollFn = Callable[[Config, State], AsyncIterator[Message | State]]


def token_for(state_key: str, num_tokens: int) -> int:
    """Map a state key to its ownership token — the default partitioner's math.

    Sharded extractor instances compute ownership consumer-side: a config
    belongs to the instance currently holding token ``token_for(state_key,
    N)``, where N is the config topics' validated common partition count.
    Because ownership is a pure function of the *state key*, record
    placement on the config topics stays irrelevant (any producer — Kafka
    UI included — may write them), and no state entry can ever have two
    owners: configs sharing a state key co-locate by construction.

    Deliberately the exact math of aiokafka's ``DefaultPartitioner``
    (Java-compatible murmur2, sign-cleared, modulo) — deterministic across
    processes, unlike Python's per-process-salted ``hash()`` — so ownership
    coincides with where a key-hashing producer would have placed the key.
    Changing this function remaps ownership across a running fleet; treat
    it as a compatibility promise (a unit test locks it against aiokafka's
    partitioner).
    """
    return (murmur2(state_key.encode("utf-8")) & 0x7FFFFFFF) % num_tokens


@dataclass(frozen=True, slots=True)
class ConfigEntry:
    """Paired Config and state key — always created, updated, and deleted together."""
    config: Config
    state_key: str


class Extractor(Stage, ABC):
    """Poll-driven data extractor (stateful or stateless).

    Three ways to construct one:

    * Declaratively with the ``@extractor(...)`` decorator, which binds a poll
      function to its config topics — the decorated name becomes the stage::

          @extractor(config_topics=["my-config"])
          async def stage(config, state):
              ...

    * Functionally with ``Extractor.of(...)``, the factory the decorator wraps,
      when the poll function must stay callable under its own name::

          stage = Extractor.of(
              config_topics=["my-config"],
              poll=my_poll_fn,
          )

    * As a subclass for lifecycle management (HTTP clients, MQTT sessions)::

          class MyExtractor(Extractor):
              config_topics = ["my-config"]

              async def __aenter__(self):
                  self.http = httpx.AsyncClient()
                  return self

              async def __aexit__(self, *exc_info):
                  await self.http.aclose()

              async def poll(self, config, state):
                  ...

    Config topics are re-read from the earliest on every startup — never
    through a committed-offset consumer group. The runner's membership
    consumer does join the ``application_id`` group, but purely for
    ownership leases: it never commits offsets and every record it fetches
    is discarded (see `ExtractorRunner`). Replicas up to the config topics'
    partition count split the configs between them; further replicas are
    hot standbys. ``self.configs`` (inherited from `Stage`) always holds
    the GLOBAL config store — scale-out only narrows which configs ``poll``
    is invoked for. The caller sets the ``application_id`` used for
    changelog topic naming (and the membership group) on `Flechtwerk`;
    stages don't carry it.
    """

    wakeup: asyncio.Event | None = None
    """Optional wakeup for push-driven sources.

    ``None`` (the default) keeps the runner on a plain
    ``poll_interval`` sleep between cycles. A stage whose input
    arrives asynchronously (e.g. an MQTT subscription) sets this in
    ``__aenter__`` and fires it on arrival; the runner then treats the
    interval as an upper bound, polling as soon as the event is set —
    the interval degrades to the idle/config-drain cadence.
    """

    @classmethod
    def of(
            cls,
            *,
            config_topics: list[str],
            poll: PollFn,
            enrich_config: EnrichConfigFn | None = None,
            extract_state_key: ExtractStateKeyFn | None = None,
    ) -> "Extractor":
        """Build an Extractor from a poll function and config topics.

        ``enrich_config`` and ``extract_state_key`` are optional overrides; omit them
        to use the defaults (no enrichment, ``extract_state_key`` returns the
        Kafka message key).

        Patches the supplied callables in as instance attributes that
        shadow the class-level abstract method ``poll`` (and, when
        provided, the default ``enrich_config`` / ``extract_state_key`` methods). The
        ABC discipline still applies to every other construction path —
        ``Extractor()`` and any abstract subclass remain uninstantiable.
        """
        instance = _FunctionalExtractor()
        instance.config_topics = config_topics
        instance.poll = poll
        if enrich_config is not None:
            instance.enrich_config = enrich_config
        if extract_state_key is not None:
            instance.extract_state_key = extract_state_key
        return instance

    @abstractmethod
    def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Poll an external API and yield Messages.

        Declared without ``async`` so that implementations — ``async def``
        functions containing ``yield``, i.e. async generator functions whose
        call returns an ``AsyncIterator`` directly — are compatible overrides
        under strict type checking. A coroutine-typed abstract (``async def``
        with no ``yield``) would make every real override incompatible.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted. Yielding an empty/falsy State deletes the entry from the
        state store (and writes a Kafka tombstone to the changelog). On crash,
        the last-persisted state is retained.

        Both parameters are read-only. The runner hands each poll a private copy
        of ``config`` and ``state``, so mutating either in place has no effect —
        it is silently discarded. Emit records by yielding a ``Message`` and
        persist your resume cursor by yielding a ``State``.
        """


class _FunctionalExtractor(Extractor):
    """Shell subclass used solely as the instantiation target for ``Extractor.of``.

    The class-level ``poll = None`` is a placeholder that satisfies
    ``ABCMeta``'s abstract-method check; ``of()`` shadows it with an
    instance attribute on every call.
    """
    poll = None  # type: ignore[assignment]


def extractor(
        *,
        config_topics: list[str],
        enrich_config: EnrichConfigFn | None = None,
        extract_state_key: ExtractStateKeyFn | None = None,
) -> Callable[[PollFn], Extractor]:
    """Decorator form of `Extractor.of` — bind a poll function to its config topics.

    The decorated async generator becomes the built `Extractor`, so the name you
    define *is* the stage, ready to hand to `Flechtwerk.of`::

        @extractor(config_topics=["my-config"])
        async def stage(config: Config, state: State) -> AsyncIterator[Message | State]:
            ...

    ``enrich_config`` and ``extract_state_key`` are the same optional overrides as on
    `Extractor.of` — this is exactly that call with ``poll`` supplied by the
    decoration. Call `Extractor.of` directly when the poll function must stay
    callable under its own name, and subclass `Extractor` when you need
    lifecycle management (``__aenter__`` / ``__aexit__``).
    """
    def decorator(poll: PollFn) -> Extractor:
        return Extractor.of(
            config_topics=config_topics,
            enrich_config=enrich_config,
            extract_state_key=extract_state_key,
            poll=poll,
        )
    return decorator


class TokenRebalanceListener(ConsumerRebalanceListener):
    """Bridges the membership consumer's eager rebalance protocol to token ownership.

    aiokafka logs-and-swallows exceptions raised inside these callbacks, so
    "let it crash" cannot fire from here — failures are recorded on the
    runner's ``fatal`` slot and re-raised by the main loop instead.
    """

    def __init__(self, runner: "ExtractorRunner") -> None:
        self.runner = runner

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        try:
            # Runs before the group re-forms (aiokafka's protocol is eager):
            # cancelling the cycle, flushing straggler changelog writes, and
            # wiping the local store HERE is the barrier that lets the next
            # owner restore a complete changelog before it starts polling.
            # The rebalance lock keeps this teardown from interleaving with
            # a restore running in the main loop.
            async with self.runner.rebalance_lock:
                # A not-yet-consumed assignment belongs to the generation
                # this revoke ends — consuming it later would resurrect
                # ownership the group has meanwhile handed elsewhere.
                self.runner.pending = None
                await self.runner.suspend_tokens()
        except Exception as e:
            self.runner.fatal = e

    def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        try:
            # Bookkeeping only — no I/O. Restoring state here would stall the
            # whole group past rebalance_timeout_ms; the main loop does it.
            # Tokens are partition NUMBERS: the Range assignor co-assigns
            # partition p of every config topic (validated-equal counts) to
            # one member, so the set comprehension merges topics losslessly.
            self.runner.pending = {tp.partition for tp in assigned}
        except Exception as e:
            self.runner.fatal = e


class ExtractorRunner:
    """Orchestrates concurrent polling for an Extractor subclass.

    Attributes are set by the DI container (reactor-di) or directly in tests.

    Re-entry contract: for any given config, ``poll()`` is re-entered only
    after the previous COMPLETED invocation's yielded messages were sent to
    Kafka and the producer was flushed (``poll_one`` awaits ``send_batch``
    before returning; a send failure crashes the process). A CANCELLED
    invocation — poll-cycle teardown on a token handover — sends nothing,
    and ``poll_one`` closes its generator deterministically, so a
    cancellation-aware source (the MQTT template's buffer rollback) restores
    its unconfirmed input before any re-entry. Sources that defer an
    acknowledgement to their upstream system until the data is durable in
    Kafka — e.g. the MQTT template's ACK-the-previous-batch-at-the-top-of-
    the-next-poll pattern — depend on both orderings; do not weaken either.

    Ownership: config ownership is sharded across the instances of one
    ``application_id`` via consumer-group leases on the config topics'
    partitions ("tokens") — one replica owns everything (the degenerate,
    and most common, deployment), replicas up to the partition count split
    the configs, and further replicas are hot standbys. An instance polls
    only the configs whose ``token_for(state_key, N)`` falls on a held
    token. The data plane is unaffected by scale-out — every instance reads
    all config topics group-less into the global store (and
    ``extractor.configs`` exposes exactly that store) — so config-record
    *placement* is irrelevant; only the lease mechanism uses the
    partitions. Delivery is at-least-once: nothing fences a zombie, so
    around rebalances and network partitions a cursor can regress —
    typically by one in-flight poll, at worst by the window until an
    evicted instance notices and suspends (bounded re-import, never loss).
    The revoke barrier (``suspend_tokens``) minimizes that window.
    """

    config_store: ConfigStore
    consumer: AIOKafkaConsumer
    create_restore_consumer: Callable[[], AIOKafkaConsumer]
    extractor: lookup[Extractor, "configured_stage"]  # noqa: PyUnresolvedReferences
    membership_consumer: AIOKafkaConsumer
    observer: Observer
    poll_interval: timedelta
    producer: AIOKafkaProducer
    state_store: ChangelogStateStore

    def __init__(self):
        self.cycle: asyncio.Task[Never] | None = None
        self.entries: dict[str, ConfigEntry] = {}
        self.fatal: BaseException | None = None
        self.num_tokens = 0
        self.pending: set[int] | None = None  # None = no assignment pending; set() = assigned nothing (standby)
        self.rebalance_lock = asyncio.Lock()
        self.tokens: frozenset[int] = frozenset()

    async def run(self) -> Never:
        """Main event loop. Runs until cancelled or an unrecoverable error occurs.

        Resource lifecycle (consumer/producer start/stop) is managed by
        Flechtwerk, not the runner. The main consumer is assigned (not
        subscribed) to every partition of every config topic by the
        bootstrap. The membership consumer joins the ``application_id``
        group purely for the partition leases — every record it fetches is
        DISCARDED. Do not "fix" that: records arrive by *placement* (Kafka
        UI writes to partition 0), ownership is decided by the consumer-side
        ``token_for`` hash, and the data plane is the group-less main
        consumer feeding the config store. Poll cycles run on a background
        task (``cycle_loop``) so this loop keeps heartbeating the group
        (max_poll_interval_ms is enforced against getmany cadence) and
        draining config updates while a long backfill is in flight.

        The rebalance lock around ``start_pending_tokens`` pairs with the
        revoke callback's acquisition: without it, a revoke landing mid-
        restore would tear down and REASSIGN ownership elsewhere, and the
        restore's completion here would then resurrect the revoked tokens —
        two live owners for one config until the next rebalance. Do not
        remove either side.
        """
        self.extractor.configs = self.config_store
        async with self.extractor:
            if not self.num_tokens:  # tests may pre-set the token space
                self.num_tokens = await self.count_tokens()
            await self.load_initial_configs()
            self.membership_consumer.subscribe(
                self.extractor.config_topics, listener=TokenRebalanceListener(self),
            )
            try:
                while True:
                    if self.fatal is not None:
                        raise self.fatal
                    if self.cycle is not None and self.cycle.done():
                        self.cycle.result()  # cycle_loop never returns — surface its error
                    # Pump the membership consumer: group liveness + rebalance
                    # processing. Records are discarded by design (see above).
                    await self.membership_consumer.getmany(timeout_ms=1000)
                    await self.check_config_updates()
                    async with self.rebalance_lock:
                        await self.start_pending_tokens()
            finally:
                # The rebalance listener is NOT invoked on consumer.stop() —
                # tear down explicitly on the way out.
                await self.suspend_tokens()

    async def count_tokens(self) -> int:
        """The token space: the config topics' common partition count.

        Validated equal across a stage's config topics at startup
        (`module.py`) and pinned here for the process lifetime — growing the
        partition count takes a rolling restart, during which instances may
        briefly disagree on ownership (absorbed by at-least-once). Primes
        the main consumer's metadata the same way `bootstrap_config_store`
        does (the one documented private-API coupling, locked down by the
        integration tests).
        """
        topics = self.extractor.config_topics
        await self.consumer._client.set_topics(list(topics))
        partitions = self.consumer.partitions_for_topic(topics[0])
        if not partitions:
            raise RuntimeError(f"no partitions known for config topic {topics[0]}")
        return len(partitions)

    async def cycle_loop(self) -> Never:
        """Poll cycles + idle pacing for the currently-held tokens.

        Runs as a background task owned by ``start_pending_tokens`` and
        cancelled by ``suspend_tokens``, so the sharded main loop stays
        responsive while a cycle — an epoch backfill, say — runs long.
        """
        while True:
            await self.poll_cycle()
            await self.idle()

    async def poll_cycle(self) -> None:
        """One concurrent poll pass over the active configs this instance owns."""
        active = {
            k: e for k, e in self.entries.items()
            if not e.config.get(SUSPENDED) and self.owns(e.state_key)
        }
        self.observer.active_configs(len(active))
        if not active:
            return
        log.debug("Polling %d active config(s)", len(active))
        with self.observer.poll_cycle_scope():
            results = await asyncio.gather(
                *(self.poll_one(e) for e in active.values()),
                return_exceptions=True,
            )
        for key, result in zip(active.keys(), results):
            if isinstance(result, Exception):
                log.error("Poll failed for key %s", key, exc_info=result)
                raise result

    def owns(self, state_key: str) -> bool:
        """Whether this instance holds the token for a state key.

        Ownership follows *state* identity, so no state entry can ever have
        two owners (configs sharing a state key co-locate on one instance by
        construction). A single replica holds every token and owns
        everything.
        """
        return token_for(state_key, self.num_tokens) in self.tokens

    async def start_pending_tokens(self) -> None:
        """Restore state and resume cycling for a freshly-assigned token set.

        Runs in the main loop under the rebalance lock — never in the assign
        callback, where the changelog read would stall the whole group past
        rebalance_timeout_ms. The full changelog is re-read on every
        assignment: extractor state is small by the same contract that keeps
        config topics small, and ``suspend_tokens`` wiped the local store,
        so what this restores is exactly the changelog — including the
        previous owner's final flush (its revoke barrier completed before
        this assignment existed).
        """
        pending, self.pending = self.pending, None
        if pending is None:
            return
        # Defensive: a cycle must never survive into a new generation — the
        # revoke barrier already cancelled it, but a leaked survivor here
        # would double-poll every config for the rest of the process.
        await self.cancel_cycle()
        self.tokens = frozenset(pending)
        self.observer.tokens_assigned(len(self.tokens))
        if not pending:
            log.info("No tokens assigned — hot standby")
            return
        consumer = self.create_restore_consumer()
        await consumer.start()
        try:
            entries = await self.state_store.restore(consumer)
        finally:
            await consumer.stop()
        self.cycle = asyncio.create_task(self.cycle_loop())
        log.info("Acquired tokens %s (%d changelog records restored)", sorted(pending), entries)

    async def suspend_tokens(self) -> None:
        """Give up all tokens: cancel the cycle, flush stragglers, wipe the store.

        Called from the revoke callback (the barrier before the group
        re-forms) and from the sharded loop's shutdown path. The producer
        flush pushes out changelog writes that ``ChangelogStateStore.put``
        sent but never awaited to delivery, so the next owner's restore —
        which reads to end offsets captured after this barrier — sees the
        final cursor. The store wipe keeps a later assignment from serving
        keys the changelog no longer contains; ``start_pending_tokens``
        restores from scratch. When nothing ran since the last handover —
        the first-ever revoke, a standby losing its empty assignment, or a
        repeated teardown — there is nothing to barrier and this is a no-op.
        """
        if self.cycle is None and not self.tokens:
            return
        self.tokens = frozenset()
        await self.cancel_cycle()
        await self.producer.flush()
        await self.state_store.close()
        self.observer.active_configs(0)  # nothing is polled until the next assignment
        self.observer.tokens_assigned(0)

    async def cancel_cycle(self) -> None:
        """Cancel the in-flight cycle task and wait for it to unwind.

        A poll cycle can run for minutes (epoch backfills), so the revoke
        barrier cancels instead of awaiting completion — blocking the group
        rebalance on a running backfill would stall every member past the
        rebalance timeout. Cancellation is safe under at-least-once:
        messages already sent stay sent (the new owner re-polls them as
        duplicates), and ``poll_one`` persists state only after its send
        batch, so a cancelled invocation advances no cursor. A
        non-cancellation error from the dying cycle propagates to the
        caller (the revoke callback records it on ``fatal``).
        """
        cycle, self.cycle = self.cycle, None
        if cycle is None:
            return
        cycle.cancel()
        task = asyncio.current_task()
        before = task.cancelling() if task is not None else 0
        try:
            await cycle
        except asyncio.CancelledError:
            # Suppress the CHILD's cancellation only. If a fresh cancellation
            # of THIS task arrived while awaiting the unwind, swallowing it
            # would make shutdown uncancellable — a later await (the barrier
            # flush against an unreachable broker, say) could hang with no
            # way to interrupt. cancelling() counts requested cancellations,
            # so a delta means the CancelledError is (also) ours.
            if task is not None and task.cancelling() > before:
                raise

    async def idle(self) -> None:
        """Wait for the next poll cycle.

        Plain sleep unless the stage exposes a ``wakeup`` event (push-driven
        sources); then the first inbound message ends the wait early. The
        clear-after-wait ordering can't lose messages: producers set the
        event *after* buffering, and the next cycle drains the buffer — a
        set landing between clear() and the drain merely costs one extra
        (empty) cycle.
        """
        wakeup = self.extractor.wakeup
        if wakeup is None:
            await asyncio.sleep(self.poll_interval.total_seconds())
            return
        with suppress(TimeoutError):
            await asyncio.wait_for(wakeup.wait(), self.poll_interval.total_seconds())
        wakeup.clear()

    async def load_initial_configs(self) -> None:
        """Read all existing configs from the config topics on startup.

        `bootstrap_config_store` compacts by wire key, treating empty
        values as tombstones that remove the key entirely — matching Kafka
        log compaction — reads to the end offsets captured at entry, and
        enriches once per surviving entry. Every surviving entry becomes
        a config.
        """
        latest = await bootstrap_config_store(
            self.consumer, self.extractor.config_topics, self.config_store, self.extractor.enrich_config,
        )
        for raw_msg in latest.values():
            await self.apply_config(parse_message(raw_msg))
        self.observer.config_store_restored(len(self.config_store))
        self.observer.config_store_entries(len(self.config_store))
        log.info("Loaded %d initial config(s)", len(self.entries))

    async def check_config_updates(self) -> None:
        """Non-blocking check for config changes."""
        records = await drain_config_updates(self.consumer, self.config_store, self.extractor.enrich_config)
        for raw_msg in records:
            self.observer.config_message_in(raw_msg.topic)
            await self.apply_config(parse_message(raw_msg))
        if records:
            self.observer.config_store_entries(len(self.config_store))

    async def apply_config(self, msg: IncomingMessage) -> None:
        """Sync this runner's config entry for one config record.

        The record has already been applied to the (enriched) config store;
        this reads the store rather than re-enriching the raw message.
        """
        self.observer.message_in(msg.topic)
        key = msg.key
        config = self.config_store.get(key)
        if config is None:
            if key in self.entries:
                del self.entries[key]
                log.info("Removed config for key %s", key)
            return

        present = key in self.entries
        self.entries[key] = ConfigEntry(
            config=config,
            state_key=self.extractor.extract_state_key(msg),
        )
        log.info("%s config for key %s", "Updated" if present else "Added", key)

    async def poll_one(self, entry: ConfigEntry) -> None:
        """Poll a single config: send messages as yielded, persist state on success.

        Each ``Message`` is handed to the producer BEFORE the generator is
        resumed — the generator sits suspended at its yield while the send
        awaits — so a cancellation-aware source (the MQTT template) can only
        mark a message pending-ACK after the producer accepted it. Combined
        with the flush here (completed invocations) and in ``suspend_tokens``
        (cancelled ones), a pending-ACK can never outlive Kafka durability.
        Sending incrementally also keeps memory flat during epoch backfills —
        nothing buffers the whole batch.
        """
        state = State(await self.state_store.get(entry.state_key) or {})
        baseline = deepcopy(state)

        new_state: State | None = None
        with self.observer.dispatch_scope():
            # Hand poll() a private copy of both mutable inputs. `state` is a
            # fresh read above; `entry.config` is cached and reused across every
            # poll cycle, so without this copy an in-place edit would leak into
            # later polls. Mutating a parameter is thus a no-op by construction.
            generator = self.extractor.poll(deepcopy(entry.config), state)
            try:
                async for item in generator:
                    if isinstance(item, State):
                        new_state = item
                    elif isinstance(item, Message):
                        await self.producer.send(
                            item.topic,
                            key=encode_json(item.key),
                            value=encode_json(item.value),
                            timestamp_ms=datetime_to_millis(item.timestamp),
                        )
                        self.observer.message_out(item.topic)
                    else:
                        raise TypeError(f"poll() yielded {type(item).__name__}, expected Message or State")
            finally:
                # Close the generator deterministically. On poll-cycle
                # cancellation (a token handover) this raises GeneratorExit at
                # the generator's current yield NOW — not at a later GC pass —
                # so cancellation-aware templates (the MQTT buffer rollback)
                # clean up before the next owner, or the next cycle, runs.
                if (aclose := getattr(generator, "aclose", None)) is not None:
                    await aclose()

        await self.producer.flush()
        if new_state is not None and new_state != baseline:
            if new_state:
                await self.state_store.put(entry.state_key, new_state)
            else:
                await self.state_store.delete(entry.state_key)
