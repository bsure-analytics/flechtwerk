"""Dependency injection module for fretworx Kafka resources.

FretworxModule is the single place where all Kafka resources (admin client,
consumer, producer, state store) are created and shared. The module uses
reactor-di for lazy dependency resolution and provides an async context
manager for lifecycle management (start/stop).

Key design: a single transactional producer is shared between the runner
and the ChangelogStateStore. For transformers, this closes the transactional
gap — state changelog writes participate in the same Kafka transaction as
output messages and offset commits.
"""
from __future__ import annotations

import logging
import tempfile
from functools import cached_property
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient
from reactor_di import CachingStrategy, module, make

from .extractor import Extractor, ExtractorRunner
from .state import ChangelogStateStore, RocksDBStateStore, ensure_changelog_topic, StateStore
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)


@module(CachingStrategy.NOT_THREAD_SAFE)
class FretworxModule:
    """DI container for all Kafka resources.

    Set client_id, group_id, and stage before first access.
    Then use as an async context manager to start/stop resources::

        mod = FretworxModule()
        mod.client_id = "ariadne-extractor-0"
        mod.group_id = "ariadne-extractor"
        mod.bootstrap_servers = "localhost:9092"
        mod.stage = my_extractor

        async with mod:
            await mod.runner.run()
    """

    bootstrap_servers: str
    client_id: str
    group_id: str
    extractor_runner: ExtractorRunner
    inner_store: make[StateStore, RocksDBStateStore]
    stage: Extractor | Transformer
    state_store: make[StateStore, ChangelogStateStore]
    transformer_runner: TransformerRunner

    @cached_property
    def changelog_topic(self) -> str:
        return self.group_id + "-changelog"

    @cached_property
    def consumer(self) -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            client_id=self.client_id,
            enable_auto_commit=False,
            group_id=self.group_id,
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

    async def __aenter__(self) -> FretworxModule:
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
