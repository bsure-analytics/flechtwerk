"""Integration tests for restore_changelog against a real Kafka broker.

Verifies behavior that unit tests with mocks cannot: the `_client.set_topics()`
metadata-priming call actually populating partition info against a live broker,
real Kafka compaction semantics, and round-tripping JSON-serialized state
entries through the wire format.
"""
import pickle

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.kafka import restore_changelog
from flechtwerk.state import serialize
from flechtwerk.types import State

pytestmark = pytest.mark.integration


async def _create_compacted_topic(bootstrap: str, topic: str) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"},
            ),
        ])
    finally:
        await admin.close()


async def _produce(bootstrap: str, topic: str, records: list[tuple[bytes, bytes | None]]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for key, value in records:
            await producer.send(topic, key=key, value=value)
        await producer.flush()
    finally:
        await producer.stop()


async def test_restore_reconstructs_state_from_changelog(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """put-only records → restore writes wire bytes for each record."""
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)
    bytes1 = serialize(State.wrap({"cursor": "2024-01-01"}))
    bytes2 = serialize(State.wrap({"cursor": "2024-02-01"}))
    bytes3 = serialize(State.wrap({"cursor": "2024-03-01"}))
    await _produce(kafka_bootstrap, unique_changelog_topic, [
        (b"key1", bytes1),
        (b"key2", bytes2),
        (b"key3", bytes3),
    ])

    restored: dict[str, bytes] = {}

    async def put_bytes(k, raw):
        restored[k] = raw

    async def delete(k):
        restored.pop(k, None)

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(consumer, unique_changelog_topic, put_bytes, delete)
    finally:
        await consumer.stop()

    assert count == 3
    assert restored == {"key1": bytes1, "key2": bytes2, "key3": bytes3}


async def test_restore_applies_kafka_tombstones(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """A value=None record (Kafka tombstone) removes the key from restored state."""
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)
    alive_bytes = serialize(State.wrap({"n": 1}))
    await _produce(kafka_bootstrap, unique_changelog_topic, [
        (b"alive", alive_bytes),
        (b"gone", serialize(State.wrap({"n": 2}))),
        (b"gone", None),  # Kafka compaction tombstone
    ])

    restored: dict[str, bytes] = {}

    async def put_bytes(k, raw):
        restored[k] = raw

    async def delete(k):
        restored.pop(k, None)

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(consumer, unique_changelog_topic, put_bytes, delete)
    finally:
        await consumer.stop()

    assert count == 3  # all three records processed
    assert restored == {"alive": alive_bytes}


async def test_restore_passes_legacy_pickle_bytes_through_unchanged(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """Legacy pickle bytes are passed through `put_bytes` like any other wire bytes —
    deserialization is deferred to the first `get()` for the key. TODO(legacy-pickle-state):
    remove once all changelog topics have rolled over."""
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)
    legacy_bytes = pickle.dumps(State.wrap({"cursor": "from-the-past"}))
    modern_bytes = serialize(State.wrap({"cursor": "current"}))
    await _produce(kafka_bootstrap, unique_changelog_topic, [
        (b"legacy", legacy_bytes),
        (b"modern", modern_bytes),
    ])

    restored: dict[str, bytes] = {}

    async def put_bytes(k, raw):
        restored[k] = raw

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(consumer, unique_changelog_topic, put_bytes, lambda k: None)
    finally:
        await consumer.stop()

    assert count == 2
    assert restored == {"legacy": legacy_bytes, "modern": modern_bytes}


async def test_restore_returns_zero_for_empty_topic(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """An empty (but existing) compacted topic restores to zero entries.

    Exercises the metadata-priming call (`consumer._client.set_topics([topic])`)
    against a real broker — verifying it actually makes the topic visible to
    `partitions_for_topic()`. The public `consumer.topics()` is insufficient
    because it returns a separate ClusterMetadata object without updating the
    consumer's internal cache.
    """
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)

    async def _unreachable_put(k, v):
        raise AssertionError(f"unexpected put({k!r}, {v!r})")

    async def _unreachable_delete(k):
        raise AssertionError(f"unexpected delete({k!r})")

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(
            consumer, unique_changelog_topic, _unreachable_put, _unreachable_delete,
        )
    finally:
        await consumer.stop()

    assert count == 0
