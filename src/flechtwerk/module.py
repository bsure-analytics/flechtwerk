"""Dependency injection module for Flechtwerk Kafka resources.

Flechtwerk is the single place where all Kafka resources (admin client,
consumer, producer, state store) are created and shared. The module uses
reactor-di for lazy dependency resolution and provides an async context
manager for lifecycle management (start/stop).

Key design: each transformer task (input partition) owns one transactional
producer, shared between the runner and that task's ChangelogStateStore.
This closes the transactional gap — state changelog writes participate in
the same Kafka transaction as output messages and offset commits — and the
static per-task transactional ID fences any previous owner of the task
(Kafka Streams EOS-v1), which is what makes running multiple transformer
instances safe. Extractors mirror the same design per ownership token: a
membership consumer's partition leases distribute config ownership across
replicas (one replica simply owns every lease), and each held token owns a
transactional producer — ``{application_id}-{token}`` — whose per-page
transactions make cursor and output atomic (see ``flechtwerk.extractor``).
A broker-dispatched extractor (MQTT shared subscriptions) skips the Kafka leases
entirely and holds ONE producer for the whole replica —
``{application_id}-{client_id}``. That ID fences nothing, which is exactly
why such a stage is stateless by contract; its per-page transactions are
otherwise identical.
"""
import logging
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, Never, Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from prometheus_client import REGISTRY, CollectorRegistry
from reactor_di import CachingStrategy, module, lookup

from .configs import ConfigStore
from .extractor import Extractor, ExtractorRunner
from .keyring import Keyring, install_keyring, set_secret_observer
from .metrics import Metrics
from .observer import Observer, PrometheusObserver
from .state import ChangelogStateStore, RocksDBStateStore, ensure_changelog_topic
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)

__all__ = ["CompressionType", "Flechtwerk", "MqttBrokerConfig"]

CompressionType = Literal["gzip", "snappy", "lz4", "zstd"]
"""Kafka producer compression codec — closed set matching aiokafka's accepted values."""


@dataclass(frozen=True, kw_only=True)
class MqttBrokerConfig:
    """Shared MQTT connection settings — one MQTT broker serves the whole platform.

    Keyword-only by construction. The fields are kept alphabetical (this
    repo's convention), so any future one lands in the MIDDLE of the list —
    which under positional construction would silently re-bind a caller's
    arguments to the wrong fields. ``kw_only`` makes field order a
    non-event: a stale call fails loudly at construction instead of
    producing a config whose ``session_expiry`` is somebody's username.

    Defined here rather than in ``flechtwerk/mqtt.py`` so the container can
    annotate its ``mqtt`` slot without importing paho: reactor-di evaluates
    all class annotations at decoration time, and paho must stay an opt-in
    import confined to ``flechtwerk/mqtt.py`` (what makes the ``flechtwerk[mqtt]``
    optional extra work).

    Deliberately MQTT-broker-only: the identity of the instance's persistent MQTT
    session is the module-wide ``client_id`` (see ``Flechtwerk.of``), and
    the shared-subscription group is the ``application_id`` — both injected
    onto the stage by ``configured_stage`` alongside these settings.

    ``session_expiry`` is the MQTT 5 ``Session Expiry Interval`` sent in
    CONNECT: how long the MQTT broker keeps an offline session — and the
    QoS ≥ 1 messages queued into it — alive after a disconnect. At a single
    replica it IS the data-protection window: if it lapses while the stage is
    away, the session's subscription goes with it and everything published in
    the gap is lost for good.

    It is always sent, and cannot be delegated to the broker. A v5 CONNECT
    that omits the property gets **0** — session discarded at disconnect, no
    backlog at all; a broker's own ``session_expiry_interval`` applies only
    to MQTT 3.1.1 clients, which have no way to ask (both verified on EMQX
    5.8.9). So the framework has to pick a number, and 24 hours is that pick:
    long enough to ride out an outage nobody is awake for, and nearly free,
    because how much an offline session may hold is capped by the broker's
    per-session queue limit rather than by this value. The two error
    directions are not symmetric — too short loses data silently and
    permanently, too long only leaves a session lingering that an operator
    can kick.

    Treat it as an upper bound rather than a promise: the real budget is
    ``min(session_expiry, queue limit / message rate)`` and the queue usually
    binds first. Shorten it deliberately when running more than one replica,
    where a lingering session withholds traffic from its survivors — see the
    MQTT guide's "Sizing the Outage Budget".
    """
    broker: str
    port: int
    password: str = ""
    qos: int = 1
    session_expiry: timedelta = timedelta(hours=24)
    username: str = ""


def validate_topics(stage: Extractor | Transformer) -> None:
    """Structural checks on a stage's topic declarations — no Kafka broker needed.

    A transformer's task model hangs off its partitioned input topics, so at
    least one is required; an extractor consumes only config topics. A topic
    can't be both input and config — config consumption bypasses the task
    model entirely.
    """
    if isinstance(stage, Transformer):
        if not stage.input_topics:
            raise ValueError("a transformer needs at least one (partitioned) input topic")
        if overlap := set(stage.input_topics) & set(stage.config_topics):
            raise ValueError(f"topics declared both input and config: {sorted(overlap)}")
    elif not stage.config_topics:
        raise ValueError("an extractor needs at least one config topic")


def validate_poll_interval(stage: Extractor | Transformer, poll_interval: timedelta | None) -> None:
    """An extractor needs a positive poll cadence — no Kafka broker needed.

    ``poll_interval`` defaults to ``None`` ("unset") and only extractors consume
    it (the runner's idle / wakeup wait), so a transformer may leave it unset
    while an extractor must set a positive duration.
    """
    if isinstance(stage, Extractor) and (poll_interval is None or poll_interval <= timedelta(0)):
        raise ValueError("an extractor needs a positive poll_interval")


async def partition_counts(admin: Any, topics: list[str]) -> dict[str, int]:
    """Partition count per topic, from Kafka broker metadata.

    Raises the Kafka broker's error (e.g. UnknownTopicOrPartitionError) when a
    topic doesn't exist — transformer input topics must exist before the
    stage starts, since the changelog partition count derives from them.

    Args:
        admin: An already-started AIOKafkaAdminClient.
        topics: Topic names to describe.
    """
    from aiokafka.errors import for_code

    response = await admin.describe_topics(topics)
    for t in response:
        if t["error_code"]:
            raise for_code(t["error_code"])(t["topic"])
    return {t["topic"]: len(t["partitions"]) for t in response}


async def ensure_topics(admin: Any, stage: Extractor | Transformer, changelog_topic: str, application_id: str) -> None:
    """Kafka-broker-side topic checks and changelog creation — the counterpart to the offline ``validate_topics``.

    Config topics are existence-checked (a missing one must fail fast: the
    assign-based bootstrap would otherwise yield a silently empty store and
    never discover a topic created later). A transformer's input topics must
    share one partition count, and the compacted changelog is created with
    that count; a pre-existing changelog is re-described and validated, a
    just-created one is not — see ``ensure_changelog_topic``.

    Nothing here is extractor-specific, deliberately: an extractor has two
    ownership models, and the resources each needs are acquired by the loop
    that needs them (``ExtractorRunner.count_tokens`` validates the token
    space; ``ensure_changelog`` creates the changelog) rather than by the
    container working out which model it is looking at.
    """
    if stage.config_topics:
        # Existence check only. Partition counts on config topics are
        # unconstrained here for every stage shape — a token-sharded extractor
        # needs them equal, but that is its token space, so `count_tokens`
        # owns the rule.
        await partition_counts(admin, stage.config_topics)
    if not isinstance(stage, Transformer):
        return
    # Tasks are identified by partition number across all input topics, and
    # a task's explicit-partition changelog write must have somewhere to
    # land — so all input topics must share one partition count (Range only
    # co-assigns same-numbered partitions when counts match) and the
    # changelog must match it.
    counts = await partition_counts(admin, stage.input_topics)
    if len(set(counts.values())) != 1:
        raise ValueError(f"input topics of {application_id} must have equal partition counts, got {counts}")
    num_partitions = next(iter(counts.values()))
    created = await ensure_changelog_topic(admin, changelog_topic, num_partitions)
    if not created:
        # Only validate a changelog we did NOT just create: a pre-existing one
        # may carry a partition count from an earlier topology that no longer
        # matches the input topics (repartitioning needs a state migration). A
        # just-created changelog was made with num_partitions, so re-describing
        # it here is redundant — and races the Kafka broker's metadata cache,
        # which lags CreateTopics and would raise a spurious
        # UnknownTopicOrPartitionError on a cold broker.
        changelog_count = (await partition_counts(admin, [changelog_topic]))[changelog_topic]
        if changelog_count != num_partitions:
            raise ValueError(
                f"changelog topic {changelog_topic} has {changelog_count} partitions, "
                f"input topics have {num_partitions} — repartitioning requires a state migration"
            )


class Flechtwerk(ABC):
    """The application-facing handle for a Flechtwerk stage.

    An application constructs one with the ``of`` factory and calls
    ``run()``::

        await Flechtwerk.of(
            application_id="my-extractor",
            bootstrap_servers="localhost:9092",
            client_id="my-extractor-0",
            poll_interval=timedelta(minutes=1),
            stage=my_extractor,
        ).run()

    This is deliberately a narrow surface — ``of`` / ``run`` / the async
    context manager, nothing else. The Kafka resources (consumers,
    transactional producers, state stores, runners) live on the private
    ``_FlechtwerkModule`` reactor-di container that ``of`` returns; an
    application must not reach past this handle into the wiring, whose
    invariants (EOS-v1 fencing, changelog restore ordering, config-store
    bootstrap sequencing) are the framework's to keep. Same idiom as
    ``Extractor.of`` / ``Transformer.of``: a factory on the public
    abstraction that returns a private concrete subclass typed as the
    abstraction.

    To embed Flechtwerk as a component of a larger reactor-di module, wire
    the concrete container directly — declare ``make[Flechtwerk,
    _FlechtwerkModule]`` and let the parent module fill every ``lookup``
    field by attribute name.
    """

    @classmethod
    def of(
        cls,
        *,
        application_id: str,
        bootstrap_servers: str,
        client_id: str,
        stage: Extractor | Transformer,
        compression_type: CompressionType | None = "zstd",
        keyring: Keyring | None = None,
        max_poll_records: int = 500,
        metrics_labels: dict[str, str] | None = None,
        metrics_port: int = 0,
        mqtt: MqttBrokerConfig | None = None,
        poll_interval: timedelta | None = None,
    ) -> "Flechtwerk":
        """Build a fully-configured application handle.

        Use this when running Flechtwerk as the program's entry point.
        ``client_id`` is the process identity: every Kafka client this
        module opens derives its ID from it, and for an MQTT-sourced stage
        it also names the persistent MQTT session and completes that
        stage's transactional ID (``{application_id}-{client_id}``) — so it
        must be unique per instance and stable across restarts (production
        K8s passes the pod name; a StatefulSet's ordinal identity is what
        lets a restarted replica resume its own session and its own
        undelivered tail). ``compression_type`` defaults to ``"zstd"`` because
        Flechtwerk outputs JSON everywhere (encode_json) and JSON
        compresses ~13×; pass ``None`` to disable. ``keyring`` carries the
        process keyring for ``flechtwerk.secrets`` encrypted attributes — it is
        installed once per process at stage startup (``__aenter__``; a
        conflicting second install raises) and may be left ``None`` by stages
        without encrypted attributes. Constructing a handle has no
        side effects; standalone producers/tooling call
        ``flechtwerk.secrets.install_keyring(...)`` directly.
        ``max_poll_records`` caps how many records one ``getmany()`` fetch
        may return — Kafka's ``max.poll.records``, and the same default of
        500 (aiokafka alone leaves it unbounded). The cap is what makes a
        backlog drain as many ordinary batches instead of one giant one: an
        unbounded first fetch after downtime returns the entire backlog as
        a single batch, which pins it in memory in parsed form, floods any
        per-message I/O in ``transform`` with one concurrent call per state
        key, and outlives ``max.poll.interval.ms`` — a mid-batch group
        eviction. ``metrics_labels``
        defaults to an empty dict and ``metrics_port`` defaults to 0
        (Prometheus disabled). ``mqtt`` carries the platform's shared MQTT
        MQTT broker settings; it is used only by MQTT-sourced stages and ignored
        everywhere else, so the caller may pass it unconditionally.
        ``poll_interval`` is likewise consumed only by extractors — the poll
        cadence (the runner's idle / wakeup wait). It defaults to ``None``
        ("unset"); an extractor requires a positive ``timedelta`` and a
        transformer ignores it, so this too may be passed unconditionally.
        ``application_id``, ``bootstrap_servers``, ``client_id`` and ``stage``
        are required. An extractor scales out by itself: replicas of the
        same ``application_id`` split the configs along the config topics'
        partitions (see ``ExtractorRunner``), so the deployment's replica
        count is the only scaling knob.
        """
        instance = _FlechtwerkModule()
        instance.application_id = application_id
        instance.bootstrap_servers = bootstrap_servers
        instance.client_id = client_id
        instance.compression_type = compression_type
        instance.keyring = keyring
        instance.max_poll_records = max_poll_records
        instance.metrics_labels = dict(metrics_labels) if metrics_labels else {}
        instance.metrics_port = metrics_port
        instance.mqtt = mqtt
        instance.poll_interval = poll_interval
        instance.stage = stage
        return instance

    @abstractmethod
    async def run(self) -> Never:
        """Bootstrap resources and run the configured stage forever.

        The runner's main loop is ``while True``, so under normal operation
        this coroutine never returns — it terminates only by cancellation
        or an unrecovered exception.
        """
        ...

    @abstractmethod
    async def __aenter__(self) -> Self:
        ...

    @abstractmethod
    async def __aexit__(self, *exc_info: object) -> None:
        ...


@module(CachingStrategy.NOT_THREAD_SAFE)
class _FlechtwerkModule(Flechtwerk):
    """Private DI container for all Kafka resources — the concrete
    ``Flechtwerk`` returned by ``Flechtwerk.of`` and the type wired into a
    parent reactor-di module via ``make[Flechtwerk, _FlechtwerkModule]``.

    Flechtwerk is the single place where all Kafka resources (admin client,
    consumers, producers, state stores) are created and shared. Not part of
    the application-facing surface: like the runners, it may leak aiokafka
    types freely.

    Keep the ``Flechtwerk`` base annotation-free. ``@module`` walks
    ``get_type_hints`` over the whole MRO, so any annotated attribute added
    to the base would silently become a DI-managed name here — and would
    leak back onto the public handle's type. ``test_public_handle_*`` pins
    the empty-surface invariant.
    """

    application_id: lookup[str]
    bootstrap_servers: lookup[str]
    client_id: lookup[str]
    compression_type: lookup[CompressionType | None]
    extractor_runner: ExtractorRunner
    inner_store: RocksDBStateStore
    keyring: lookup[Keyring | None]
    max_poll_records: lookup[int]
    metrics: Metrics
    metrics_labels: lookup[dict[str, str]]
    metrics_port: lookup[int]
    mqtt: lookup[MqttBrokerConfig | None]
    poll_interval: lookup[timedelta | None]
    prometheus_observer: PrometheusObserver
    registry: CollectorRegistry = REGISTRY
    stage: lookup[Extractor | Transformer]
    transformer_runner: TransformerRunner

    @cached_property
    def changelog_topic(self) -> str:
        return self.application_id + "-changelog"

    @cached_property
    def config_consumer(self) -> AIOKafkaConsumer | None:
        """Consumer feeding a transformer's config store (None without config topics).

        Group-less and read_committed, like the restore consumers. Extractors
        don't need one — their main consumer is already group-less and IS the
        config consumer.
        """
        if not (isinstance(self.stage, Transformer) and self.stage.config_topics):
            return None
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id + "-config",
            group_id=None,
            isolation_level="read_committed",
        )

    @cached_property
    def config_store(self) -> ConfigStore:
        """The stage's per-process config store, fed by the runner."""
        return ConfigStore()

    @cached_property
    def configured_stage(self) -> Extractor | Transformer:
        """The caller's stage, completed with its module-owned collaborators.

        An MQTT-sourced stage receives the MQTT broker settings verbatim, the
        module-wide ``client_id`` (identity resolution is the caller's job —
        see ``Flechtwerk.of``), the shared-subscription group (the
        ``application_id``, which the stage validates in its own
        ``__aenter__``),
        and the observer; the runners consume the
        stage through this factory, so
        completion strictly precedes the stage's ``__aenter__``. Lazy import:
        flechtwerk.mqtt is the only framework module importing paho, so an
        application that never configures MQTT never loads it (the seam for
        a ``flechtwerk[mqtt]`` extra at extraction time). Configured MQTT
        broker settings on a non-MQTT stage are ignored — the caller passes platform-wide
        settings for every stage, MQTT-sourced or not.
        """
        if self.mqtt is not None:
            from .mqtt import MqttExtractor
            if isinstance(self.stage, MqttExtractor):
                self.stage.client_id = self.client_id
                self.stage.group = self.application_id
                self.stage.mqtt = self.mqtt
                self.stage.observer = self.observer
        return self.stage

    @cached_property
    def consumer(self) -> AIOKafkaConsumer:
        # read_committed everywhere: identical to read_uncommitted on
        # non-transactional topics, and required for EOS chaining — records
        # from aborted upstream transactions must never be (re-)processed.
        # The Range assignor (not aiokafka's RoundRobin default) co-assigns
        # partition p of every input topic to the same member, which is what
        # makes a task span all input topics.
        # max_poll_records bounds every getmany() batch (aiokafka's default
        # is UNBOUNDED, unlike the Java client's 500) — see Flechtwerk.of.
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            client_id=self.client_id,
            enable_auto_commit=False,
            group_id=self.application_id if isinstance(self.stage, Transformer) else None,
            isolation_level="read_committed",
            max_poll_records=self.max_poll_records,
            partition_assignment_strategy=(RangePartitionAssignor,),  # noqa: aiokafka's docstring says list, but its own default is a tuple
        )

    async def ensure_changelog(self) -> None:
        """Create the extractor's compacted changelog topic, on demand.

        Called by ``ExtractorRunner.run_sharded`` and by nobody else. That
        is the whole mechanism: a broker-dispatched stage is stateless, so
        no changelog is created for it — not because anything checked, but
        because that loop never asks. A transformer's changelog is different
        in kind (its partition count must match the input topics) and is
        created by ``ensure_topics`` at startup instead.

        The admin client is opened and closed here: resource lifecycle is
        the container's, and one short-lived connection once per process is
        cheaper than holding an admin client open for the run.
        """
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            await ensure_changelog_topic(admin, self.changelog_topic)
        finally:
            await admin.close()

    def create_instance_producer(self) -> AIOKafkaProducer:
        """Builds the one transactional producer of a broker-dispatched extractor.

        The transactional ID is ``{application_id}-{client_id}``: work is
        sharded by an external MQTT broker rather than by Kafka leases, so there
        is no token to key it on — and, unlike a token ID, it fences
        nothing. Two replicas hold two distinct IDs by construction, which is
        exactly why such a stage is stateless (see
        ``ExtractorRunner.run_dispatched``); for per-message output that is
        harmless, since at-least-once relay is commutative. The IDs are
        client-supplied, so scaling the deployment needs no Kafka-broker-side
        change. The 10-minute transaction timeout matches
        ``create_token_producer`` — same page semantics, same coordinator
        headroom.
        """
        kwargs: dict = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
            "transaction_timeout_ms": 600_000,
            "transactional_id": f"{self.application_id}-{self.client_id}",
        }
        if self.compression_type:
            kwargs["compression_type"] = self.compression_type
        return AIOKafkaProducer(**kwargs)

    def create_restore_consumer(self) -> AIOKafkaConsumer:
        """Builds throwaway consumers for per-task changelog restore.

        read_committed so a restore stops at the last stable offset and never
        sees writes of in-flight (or fenced-and-aborted) transactions.
        """
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            group_id=None,
            isolation_level="read_committed",
        )

    def create_task_producer(self, partition: int) -> AIOKafkaProducer:
        """Builds one transactional producer per task (input partition).

        The transactional ID is static per task — ``{application_id}-{p}`` —
        so whichever instance is assigned partition p fences the previous
        owner via InitProducerId (Kafka Streams EOS-v1; aiokafka has no
        KIP-447 generation fencing).
        """
        kwargs: dict = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": f"{self.client_id}-{partition}",
            "transactional_id": f"{self.application_id}-{partition}",
        }
        if self.compression_type:
            kwargs["compression_type"] = self.compression_type
        return AIOKafkaProducer(**kwargs)

    def create_task_store(self, partition: int, producer: AIOKafkaProducer) -> ChangelogStateStore:
        """Builds one partition-scoped changelog-backed store per task.

        The store shares the task's transactional producer (state writes join
        the task's transaction) and pins changelog writes to the task's
        partition, so state lands where the task's restore reads it.
        """
        inner = RocksDBStateStore()
        inner.path = self.path / str(partition)
        store = ChangelogStateStore()
        store.inner = inner
        store.observer = self.observer
        store.partition = partition
        store.producer = producer
        store.topic = self.changelog_topic
        return store

    def create_token_producer(self, token: int) -> AIOKafkaProducer:
        """Builds one transactional producer per extractor ownership token.

        The transactional ID is static per token — ``{application_id}-{t}`` —
        so whichever instance acquires token t fences the previous owner via
        InitProducerId (EOS-v1), exactly like transformer tasks. The
        transaction timeout is raised to 10 minutes: an extractor page (the
        span between two ``State`` yields) may legitimately run far longer
        than the 60s default, and the coordinator aborts any transaction
        that outlives the timeout. 10 minutes stays under the Kafka broker's
        default ``transaction.max.timeout.ms`` cap of 15.
        """
        kwargs: dict = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": f"{self.client_id}-{token}",
            "transaction_timeout_ms": 600_000,
            "transactional_id": f"{self.application_id}-{token}",
        }
        if self.compression_type:
            kwargs["compression_type"] = self.compression_type
        return AIOKafkaProducer(**kwargs)

    @cached_property
    def membership_consumer(self) -> AIOKafkaConsumer | None:
        """Consumer holding an extractor's group membership (None for transformers).

        Built lazily and STARTED by ``run_sharded``, not by ``__aenter__``:
        the broker-dispatched loop never reaches for one, so a stage sharded
        by an MQTT broker silently never joins a group — no flag, no check.

        Joins the ``application_id`` consumer group on the stage's config
        topics purely for the partition leases (ownership tokens): it never
        commits offsets — no auto-commit and no commit calls, so config-topic
        offsets cannot leak anywhere — and the runner discards every record
        it fetches (the data plane is the group-less main consumer). The
        Range assignor makes a token a partition *number*: with the
        validated-equal partition counts, partition p of every config topic
        lands on the same member. ``auto_offset_reset="latest"`` keeps the
        pump cheap — the records carry no information the global store
        doesn't already have.
        """
        if not isinstance(self.stage, Extractor):
            return None
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="latest",
            client_id=self.client_id + "-membership",
            enable_auto_commit=False,
            group_id=self.application_id,
            isolation_level="read_committed",
            partition_assignment_strategy=(RangePartitionAssignor,),  # noqa: aiokafka's docstring says list, but its own default is a tuple
        )

    @cached_property
    def metrics_server(self) -> tuple[Any, Any] | None:
        """The Prometheus scrape HTTP server (None when disabled).

        Port collisions raise OSError — let it crash so K8s surfaces the
        problem in pod logs and CrashLoopBackOff makes it impossible to
        ignore.
        """
        if self.metrics_port <= 0:
            return None
        from prometheus_client import start_http_server
        return start_http_server(addr="0.0.0.0", port=self.metrics_port, registry=self.registry)

    @cached_property
    def observer(self) -> Observer:
        """No-op when metrics are disabled, PrometheusObserver otherwise."""
        return self.prometheus_observer if self.metrics_port > 0 else Observer()

    @cached_property
    def path(self) -> Path:
        return Path(tempfile.mkdtemp()) / "state"

    @cached_property
    def runner(self) -> ExtractorRunner | TransformerRunner:
        """Select the correct runner based on stage type.

        Each runner type is wired lazily by reactor-di via lookup annotations.
        This factory only handles the conditional selection.
        """
        return self.extractor_runner if isinstance(self.stage, Extractor) else self.transformer_runner

    async def __aenter__(self) -> Self:
        # Bring up the scrape endpoint first so health probes see it as
        # soon as the process is up.
        _ = self.metrics_server

        # Install the process keyring (idempotent if of() already did; this
        # also covers the embedded-module path). Wire the observer so the
        # ENCRYPTED codec's plaintext/decrypt events — fired deep in a lazy
        # config read, with no observer in scope — still reach Prometheus, and
        # publish the startup keyring gauge.
        if self.keyring is not None:
            install_keyring(self.keyring)
            set_secret_observer(self.observer)
            for kid in self.keyring.kids():
                self.observer.keyring_key_loaded(kid)

        validate_topics(self.stage)
        validate_poll_interval(self.stage, self.poll_interval)
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            await ensure_topics(admin, self.stage, self.changelog_topic, self.application_id)
        finally:
            await admin.close()

        # No startup state restore here for either stage shape: transformers
        # restore per task on partition assignment, and extractors likewise —
        # the runner wipes and re-reads the changelog behind the revoke
        # barrier on every token assignment.
        try:
            await self.consumer.start()
            if self.config_consumer is not None:
                await self.config_consumer.start()
        except BaseException:
            await self.consumer.stop()
            if self.config_consumer is not None:
                await self.config_consumer.stop()
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.consumer.stop()
        if self.__dict__.get("config_consumer") is not None:
            await self.config_consumer.stop()
        if self.__dict__.get("membership_consumer") is not None:
            await self.membership_consumer.stop()
        if self.__dict__.get("inner_store") is not None:
            # Token producers are runner-owned (stopped in its teardown); the
            # shared inner store just needs its final wipe. Close what was
            # actually opened rather than asking what kind of stage this is:
            # a broker-dispatched (stateless) extractor never touches
            # ``inner_store``, so reactor-di never builds one, and reaching
            # for it here would mint a temp directory for a store nothing
            # ever wrote.
            await self.inner_store.close()
        # Stop the scrape server last so a final scrape can land mid-shutdown.
        # Access via __dict__ to avoid triggering the cached_property if the
        # endpoint was never started.
        server_tuple = self.__dict__.get("metrics_server")
        if server_tuple is not None:
            server, _thread = server_tuple
            server.shutdown()

    async def run(self) -> Never:  # noqa: return type — PyCharm misreads await of Never inside async with
        """Run the configured stage (see ``Flechtwerk.run`` for the contract).

        On Ctrl-C, ``asyncio.run`` / ``uvloop.run`` translates SIGINT into
        a ``Task.cancel()`` on the main task, so what propagates *through*
        this coroutine is ``asyncio.CancelledError`` — not
        ``KeyboardInterrupt``. ``async with self`` still runs ``__aexit__``
        (Kafka clients stopped, metrics server torn down) before the
        cancellation unwinds. ``KeyboardInterrupt`` is re-raised by the
        event-loop runner at the ``_loop.run(...)`` boundary *after* this
        coroutine has finished cleaning up; the application is expected to
        catch it there.
        """
        async with self:
            await self.runner.run()
