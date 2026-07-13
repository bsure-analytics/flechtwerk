"""Integration tests for config topics against a real Kafka broker.

Covers what mocks cannot: the multi-topic `_client.set_topics()` metadata
priming, reading every partition of differently-sized topics, and the
end-to-end reproduction of the co-partitioning bug that motivated config
topics — a config produced to partition 0 serving a request that lands on
a different partition (see the Co-Partitioning Trap section in CLAUDE.md).
"""
import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import AsyncIterator

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.errors import KafkaError

from flechtwerk.module import Flechtwerk
from flechtwerk.observer import Observer
from flechtwerk.configs import ConfigStore, bootstrap_config_store
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import InMemoryStateStore
from flechtwerk.transformer import Transformer, TransformerRunner
from flechtwerk.types import Message, State

pytestmark = pytest.mark.integration


async def _create_topics(bootstrap: str, partitions: dict[str, int], compacted: tuple[str, ...] = ()) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=n,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if topic in compacted else {},
            )
            for topic, n in partitions.items()
        ])
    finally:
        await admin.close()


async def _produce(bootstrap: str, records: list[tuple[str, int | None, bytes, bytes | None]]) -> None:
    """Send (topic, partition, key, value) records; partition None uses key hashing."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for topic, partition, key, value in records:
            await producer.send(topic, key=key, value=value, partition=partition)
        await producer.flush()
    finally:
        await producer.stop()


async def _read_all(bootstrap: str, topic: str, timeout_ms: int = 1000) -> list:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        records = []
        while True:
            batch = await consumer.getmany(timeout_ms=timeout_ms)
            if not batch:
                return records
            for msgs in batch.values():
                records.extend(msgs)
    finally:
        await consumer.stop()


async def _identity(config):
    return config


async def test_bootstrap_reads_all_partitions_of_multiple_topics(
    kafka_bootstrap: str, unique_topic: str,
) -> None:
    """One set_topics() call primes metadata for several topics with different
    partition counts; topics merge into one key namespace; compaction and
    tombstones apply per wire key."""
    cfg_a = f"cfg-a-{unique_topic}"
    cfg_b = f"cfg-b-{unique_topic}"
    await _create_topics(kafka_bootstrap, {cfg_a: 2, cfg_b: 3}, compacted=(cfg_a, cfg_b))
    await _produce(kafka_bootstrap, [
        (cfg_a, 0, b"gone", b'{"n":1}'),
        (cfg_a, 1, b"kept", b'{"n":2}'),
        (cfg_a, 0, b"gone", None),  # tombstone
        (cfg_b, 2, b"far", b'{"n":3}'),
    ])

    store = ConfigStore()
    consumer = AIOKafkaConsumer(
        bootstrap_servers=kafka_bootstrap, group_id=None, isolation_level="read_committed",
    )
    await consumer.start()
    try:
        latest = await bootstrap_config_store(consumer, [cfg_a, cfg_b], store, _identity)
    finally:
        await consumer.stop()

    assert store.get("kept").raw == {"n": 2}
    assert store.get("far").raw == {"n": 3}
    assert "gone" not in store
    assert set(latest) == {"far", "kept"}


class ConfigJoin(Transformer):
    """Joins each request against the config store by wire key."""

    def __init__(self, config_topic: str, requests_topic: str, output_topic: str) -> None:
        self.config_topics = [config_topic]
        self.input_topics = [requests_topic]
        self.output_topic = output_topic

    async def transform(self, msg, state) -> AsyncIterator[Message | State]:
        config = self.configs.get(msg.key)
        if config is not None:
            yield Message(key=msg.key, topic=self.output_topic, value=config)


def make_runner(bootstrap: str, group_id: str, transformer: Transformer, changelog_topic: str) -> TransformerRunner:
    """Wire a TransformerRunner against a real broker, mirroring Flechtwerk's DI."""
    def make_producer(partition: int) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            transactional_id=f"{group_id}-{partition}",
        )

    def make_store(partition: int, producer: AIOKafkaProducer) -> ChangelogStateStore:
        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.partition = partition
        store.producer = producer
        store.topic = changelog_topic
        return store

    def make_restore_consumer() -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=None,
            isolation_level="read_committed",
        )

    runner = TransformerRunner()
    runner.application_id = group_id
    runner.consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        isolation_level="read_committed",
        partition_assignment_strategy=(RangePartitionAssignor,),
    )
    runner.observer = Observer()
    runner.create_restore_consumer = make_restore_consumer
    runner.config_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        isolation_level="read_committed",
    )
    runner.config_store = ConfigStore()
    runner.create_task_producer = make_producer
    runner.create_task_store = make_store
    runner.transformer = transformer
    return runner


async def _await_output(bootstrap: str, topic: str, count: int, runner_task: asyncio.Task) -> list:
    for _ in range(60):
        if runner_task.done():
            runner_task.result()  # re-raise the runner's crash
        records = await _read_all(bootstrap, topic)
        if len(records) >= count:
            return records
        await asyncio.sleep(0.2)
    pytest.fail(f"fewer than {count} output record(s) within timeout")


async def test_request_finds_config_regardless_of_partition_placement(
    kafka_bootstrap: str, unique_topic: str, unique_group_id: str,
) -> None:
    """The co-partitioning bug reproduction, fixed: the config sits on partition
    0 of a 2-partition config topic, the request on partition 3 of a 4-partition
    input topic — under partitioned state these never meet; via the config
    store the join succeeds. Also proves the partition counts need not match,
    and that a config produced AFTER startup is picked up by the drain."""
    config_topic = f"config-{unique_topic}"
    requests_topic = f"requests-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    changelog_topic = f"changelog-{unique_topic}"
    await _create_topics(
        kafka_bootstrap,
        {config_topic: 2, requests_topic: 4, output_topic: 1, changelog_topic: 4},
        compacted=(config_topic, changelog_topic),
    )
    # Config for k1 lands on partition 0 (the Kafka UI default); its request
    # lands on partition 3 — a different task by partition number.
    await _produce(kafka_bootstrap, [
        (config_topic, 0, b"k1", b'{"secret":"s1"}'),
        (requests_topic, 3, b"k1", b'{"type":"import_requested"}'),
    ])

    transformer = ConfigJoin(config_topic, requests_topic, output_topic)
    runner = make_runner(kafka_bootstrap, unique_group_id, transformer, changelog_topic)
    await runner.consumer.start()
    await runner.config_consumer.start()
    task = asyncio.create_task(runner.run())
    try:
        records = await _await_output(kafka_bootstrap, output_topic, 1, task)
        assert records[0].key == b"k1"
        assert records[0].value == b'{"secret":"s1"}'

        # Live update: a config produced after startup reaches the store via
        # the per-batch drain — the subsequent request joins successfully.
        await _produce(kafka_bootstrap, [(config_topic, 1, b"k2", b'{"secret":"s2"}')])
        await asyncio.sleep(1.0)  # let a drain cycle apply the config first
        await _produce(kafka_bootstrap, [(requests_topic, 0, b"k2", b'{"type":"import_requested"}')])
        records = await _await_output(kafka_bootstrap, output_topic, 2, task)
        assert {r.key for r in records} == {b"k1", b"k2"}
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await runner.consumer.stop()
        await runner.config_consumer.stop()


async def test_missing_config_topic_fails_startup(
    kafka_bootstrap: str, unique_topic: str, unique_group_id: str,
) -> None:
    """A nonexistent config topic must crash startup — the assign-based
    bootstrap would otherwise yield a silently empty store forever.

    The exact error depends on broker config: UnknownTopicOrPartitionError
    with auto-creation off (production), LeaderNotAvailableError when the
    describe triggers auto-creation (this test broker). Either way the
    existence check crashes the stage instead of letting it idle.
    """
    input_topic = f"input-{unique_topic}"
    await _create_topics(kafka_bootstrap, {input_topic: 1})

    transformer = ConfigJoin(f"missing-{unique_topic}", input_topic, f"out-{unique_topic}")
    mod = Flechtwerk.of(
        application_id=unique_group_id,
        bootstrap_servers=kafka_bootstrap,
        client_id=unique_group_id,
        poll_interval=timedelta(seconds=1),
        stage=transformer,
    )
    with pytest.raises(KafkaError):
        async with mod:
            pytest.fail("startup must not succeed")
