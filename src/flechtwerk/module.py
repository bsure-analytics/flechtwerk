"""Dependency injection module for flechtwerk Kafka resources.

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
instances safe. Extractors keep a single shared non-transactional producer.
"""
import logging
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
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
from .metrics import Metrics
from .observer import Observer, PrometheusObserver
from .state import ChangelogStateStore, RocksDBStateStore, ensure_changelog_topic, partition_counts
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)

CompressionType = Literal["gzip", "snappy", "lz4", "zstd"]
"""Kafka producer compression codec — closed set matching aiokafka's accepted values."""


@dataclass(frozen=True)
class MqttBrokerConfig:
    """Shared MQTT connection settings — one broker serves the whole platform.

    Defined here rather than in ``flechtwerk/mqtt.py`` so the container can
    annotate its ``mqtt`` slot without importing paho: reactor-di evaluates
    all class annotations at decoration time, and paho must stay an opt-in
    import confined to ``flechtwerk/mqtt.py`` (the seam for a ``flechtwerk[mqtt]``
    extra at extraction time).

    ``client_id`` is the identity of the instance's persistent MQTT session
    (``clean_session=False``): the caller must resolve it to something unique
    per instance and stable across restarts (the application entry point
    cascades ``MQTT_CLIENT_ID`` → ``FLECHTWERK_CLIENT_ID`` → the application
    id; production K8s sets the pod name). The framework does no identity
    defaulting — ``MqttExtractor`` rejects an empty ``client_id`` at startup,
    since MQTT 3.1.1 forbids an empty client id with a persistent session.
    """
    broker: str
    port: int
    client_id: str = ""
    password: str = ""
    qos: int = 1
    username: str = ""


def validate_topics(stage: Extractor | Transformer) -> None:
    """Structural checks on a stage's topic declarations — broker-free.

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


class Flechtwerk(ABC):
    """The application-facing handle for a Flechtwerk stage.

    An application constructs one with the ``of`` factory and calls
    ``run()``::

        await Flechtwerk.of(
            application_id="ariadne-extractor",
            bootstrap_servers="localhost:9092",
            client_id="ariadne-extractor-0",
            poll_interval_seconds=60,
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
        poll_interval_seconds: int,
        stage: Extractor | Transformer,
        compression_type: CompressionType | None = "zstd",
        metrics_labels: dict[str, str] | None = None,
        metrics_port: int = 0,
        mqtt: MqttBrokerConfig | None = None,
    ) -> Flechtwerk:
        """Build a fully-configured application handle.

        Use this when running Flechtwerk as the program's entry point.
        ``compression_type`` defaults to ``"zstd"`` because Flechtwerk
        outputs JSON everywhere (encode_json) and JSON compresses ~13×;
        pass ``None`` to disable. ``metrics_labels`` defaults to an empty
        dict and ``metrics_port`` defaults to 0 (Prometheus disabled).
        ``mqtt`` carries the platform's shared MQTT broker settings; it is
        used only by MQTT-sourced stages and ignored everywhere else, so
        the caller may pass it unconditionally. Everything else is required.
        """
        instance = _FlechtwerkModule()
        instance.application_id = application_id
        instance.bootstrap_servers = bootstrap_servers
        instance.client_id = client_id
        instance.compression_type = compression_type
        instance.metrics_labels = dict(metrics_labels) if metrics_labels else {}
        instance.metrics_port = metrics_port
        instance.mqtt = mqtt
        instance.poll_interval_seconds = poll_interval_seconds
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
    metrics: Metrics
    metrics_labels: lookup[dict[str, str]]
    metrics_port: lookup[int]
    mqtt: lookup[MqttBrokerConfig | None]
    poll_interval_seconds: lookup[int]
    prometheus_observer: PrometheusObserver
    registry: CollectorRegistry = REGISTRY
    stage: lookup[Extractor | Transformer]
    state_store: ChangelogStateStore
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

        An MQTT-sourced stage receives the broker settings verbatim (identity
        resolution is the caller's job — see ``MqttBrokerConfig``) plus the
        observer; the runners consume the stage through this factory, so
        completion strictly precedes the stage's ``__aenter__``. Lazy import:
        flechtwerk.mqtt is the only framework module importing paho, so an
        application that never configures MQTT never loads it (the seam for
        a ``flechtwerk[mqtt]`` extra at extraction time). A configured broker
        on a non-MQTT stage is ignored — the caller passes platform-wide
        settings for every stage, MQTT-sourced or not.
        """
        if self.mqtt is not None:
            from .mqtt import MqttExtractor
            if isinstance(self.stage, MqttExtractor):
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
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            client_id=self.client_id,
            enable_auto_commit=False,
            group_id=self.application_id if isinstance(self.stage, Transformer) else None,
            isolation_level="read_committed",
            partition_assignment_strategy=(RangePartitionAssignor,),  # noqa: aiokafka's docstring says list, but its own default is a tuple
        )

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
        store.partition = partition
        store.producer = producer
        store.topic = self.changelog_topic
        return store

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
    def producer(self) -> AIOKafkaProducer:
        """Shared non-transactional producer (extractor stages only) — no serializers.

        Runners encode output to bytes via encode_json().
        ChangelogStateStore sends pickle bytes directly. No serializers
        avoids the conflict between str-encoded output and bytes-encoded state.
        Transformers don't use this — they build one transactional producer
        per task via ``create_task_producer``.
        """
        kwargs: dict = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
        }
        if self.compression_type:
            kwargs["compression_type"] = self.compression_type
        return AIOKafkaProducer(**kwargs)

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

        validate_topics(self.stage)
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            if self.stage.config_topics:
                # Existence check only — a missing config topic must fail
                # fast: the assign-based bootstrap would otherwise yield a
                # silently empty store and never discover a topic created
                # later. Partition counts are unconstrained — config topics
                # are exempt from co-partitioning.
                await partition_counts(admin, self.stage.config_topics)
            num_partitions = -1
            if isinstance(self.stage, Transformer):
                # Tasks are identified by partition number across all input
                # topics, and a task's explicit-partition changelog write must
                # have somewhere to land — so all input topics must share one
                # partition count (Range only co-assigns same-numbered
                # partitions when counts match) and the changelog must match it.
                counts = await partition_counts(admin, self.stage.input_topics)
                if len(set(counts.values())) != 1:
                    raise ValueError(f"input topics of {self.application_id} must have equal partition counts, got {counts}")
                num_partitions = next(iter(counts.values()))
            await ensure_changelog_topic(admin, self.changelog_topic, num_partitions)
            if isinstance(self.stage, Transformer):
                changelog_count = (await partition_counts(admin, [self.changelog_topic]))[self.changelog_topic]
                if changelog_count != num_partitions:
                    raise ValueError(
                        f"changelog topic {self.changelog_topic} has {changelog_count} partitions, "
                        f"input topics have {num_partitions} — repartitioning requires a state migration"
                    )
        finally:
            await admin.close()

        if isinstance(self.stage, Extractor):
            # Transformers restore per task on partition assignment instead.
            restore = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=None,
                isolation_level="read_committed",
            )
            await restore.start()
            try:
                await self.state_store.restore(restore)
            finally:
                await restore.stop()

        try:
            await self.consumer.start()
            if self.config_consumer is not None:
                await self.config_consumer.start()
            if isinstance(self.stage, Extractor):
                await self.producer.start()
        except BaseException:
            await self.consumer.stop()
            if self.config_consumer is not None:
                await self.config_consumer.stop()
            if isinstance(self.stage, Extractor):
                await self.state_store.close()
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.consumer.stop()
        if self.__dict__.get("config_consumer") is not None:
            await self.config_consumer.stop()
        if isinstance(self.stage, Extractor):
            await self.producer.stop()
            await self.state_store.close()
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
