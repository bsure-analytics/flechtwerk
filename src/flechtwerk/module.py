"""Dependency injection module for fretworx Kafka resources.

Fretworx is the single place where all Kafka resources (admin client,
consumer, producer, state store) are created and shared. The module uses
reactor-di for lazy dependency resolution and provides an async context
manager for lifecycle management (start/stop).

Key design: a single transactional producer is shared between the runner
and the ChangelogStateStore. For transformers, this closes the transactional
gap — state changelog writes participate in the same Kafka transaction as
output messages and offset commits.
"""
import logging
import tempfile
from functools import cached_property
from pathlib import Path
from typing import Any, Literal, Never

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from prometheus_client import REGISTRY, CollectorRegistry
from reactor_di import CachingStrategy, module, lookup

from .extractor import Extractor, ExtractorRunner
from .metrics import Metrics
from .observer import Observer, PrometheusObserver
from .state import ChangelogStateStore, RocksDBStateStore, ensure_changelog_topic
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)

CompressionType = Literal["gzip", "snappy", "lz4", "zstd"]
"""Kafka producer compression codec — closed set matching aiokafka's accepted values."""


@module(CachingStrategy.NOT_THREAD_SAFE)
class Fretworx:
    """DI container for all Kafka resources.

    Two ways to construct one:

    * As a top-level application container, use the ``of`` factory and
      call ``run()``::

          await Fretworx.of(
              application_id="ariadne-extractor",
              bootstrap_servers="localhost:9092",
              client_id="ariadne-extractor-0",
              poll_interval_seconds=60,
              stage=my_extractor,
          ).run()

    * As a component of a larger reactor-di module, construct with no
      args (``Fretworx()``) and let the parent module wire every
      ``lookup`` field by attribute name. The bare constructor sets
      nothing, leaving every slot for the parent to fill.
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
    poll_interval_seconds: lookup[int]
    prometheus_observer: PrometheusObserver
    registry: CollectorRegistry = REGISTRY
    stage: lookup[Extractor | Transformer]
    state_store: ChangelogStateStore
    transformer_runner: TransformerRunner

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
    ) -> Fretworx:
        """Build a fully-configured top-level application container.

        Use this when running Fretworx as the program's entry point.
        ``compression_type`` defaults to ``"zstd"`` because Fretworx
        outputs JSON everywhere (encode_json) and JSON compresses ~13×;
        pass ``None`` to disable. ``metrics_labels`` defaults to an empty
        dict and ``metrics_port`` defaults to 0 (Prometheus disabled);
        everything else is required.
        """
        instance = cls()
        instance.application_id = application_id
        instance.bootstrap_servers = bootstrap_servers
        instance.client_id = client_id
        instance.compression_type = compression_type
        instance.metrics_labels = dict(metrics_labels) if metrics_labels else {}
        instance.metrics_port = metrics_port
        instance.poll_interval_seconds = poll_interval_seconds
        instance.stage = stage
        return instance

    @cached_property
    def changelog_topic(self) -> str:
        return self.application_id + "-changelog"

    @cached_property
    def consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            client_id=self.client_id,
            enable_auto_commit=False,
            group_id=self.application_id if isinstance(self.stage, Transformer) else None,
        )

    @cached_property
    def path(self) -> Path:
        return Path(tempfile.mkdtemp()) / "state"

    @cached_property
    def producer(self) -> AIOKafkaProducer:
        """Single shared producer — no serializers.

        Runners encode output to bytes via encode_json().
        ChangelogStateStore sends pickle bytes directly. No serializers
        avoids the conflict between str-encoded output and bytes-encoded state.
        """
        kwargs: dict = {
            "bootstrap_servers": self.bootstrap_servers,
            "client_id": self.client_id,
        }
        if self.compression_type:
            kwargs["compression_type"] = self.compression_type
        if isinstance(self.stage, Transformer):
            kwargs["transactional_id"] = self.client_id
        return AIOKafkaProducer(**kwargs)

    @cached_property
    def runner(self) -> ExtractorRunner | TransformerRunner:
        """Select the correct runner based on stage type.

        Each runner type is wired lazily by reactor-di via lookup annotations.
        This factory only handles the conditional selection.
        """
        return self.extractor_runner if isinstance(self.stage, Extractor) else self.transformer_runner

    @cached_property
    def observer(self) -> Observer:
        """No-op when metrics are disabled, PrometheusObserver otherwise."""
        return self.prometheus_observer if self.metrics_port > 0 else Observer()

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

    async def __aenter__(self) -> Fretworx:
        # Bring up the scrape endpoint first so health probes see it as
        # soon as the process is up.
        _ = self.metrics_server

        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            await ensure_changelog_topic(admin, self.changelog_topic)
        finally:
            await admin.close()

        restore = AIOKafkaConsumer(bootstrap_servers=self.bootstrap_servers, group_id=None)
        await restore.start()
        try:
            await self.state_store.restore(restore)
        finally:
            await restore.stop()

        try:
            await self.consumer.start()
            await self.producer.start()
        except BaseException:
            await self.consumer.stop()
            await self.state_store.close()
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.consumer.stop()
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
        """Bootstrap resources and run the configured stage.

        The runner's main loop is ``while True``, so under normal operation
        this coroutine never returns — it terminates only by cancellation
        or an unrecovered exception.

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
