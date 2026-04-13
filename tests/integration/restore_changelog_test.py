"""Integration tests for restore_changelog against a real Kafka broker.

Verifies behavior that unit tests with mocks cannot: the private-API reach-in
(`consumer._client.set_topics`), actual Kafka compaction semantics, and
round-tripping pickled state entries through the wire format.
"""
from __future__ import annotations

import pickle

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from fretworx.kafka import restore_changelog

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
    """put-only records → restore recreates the full state map."""
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)
    await _produce(kafka_bootstrap, unique_changelog_topic, [
        (b"key1", pickle.dumps({"cursor": "2024-01-01"})),
        (b"key2", pickle.dumps({"cursor": "2024-02-01"})),
        (b"key3", pickle.dumps({"cursor": "2024-03-01"})),
    ])

    restored: dict[str, dict] = {}

    async def put(k, v):
        restored[k] = v

    async def delete(k):
        restored.pop(k, None)

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(consumer, unique_changelog_topic, put, delete)
    finally:
        await consumer.stop()

    assert count == 3
    assert restored == {
        "key1": {"cursor": "2024-01-01"},
        "key2": {"cursor": "2024-02-01"},
        "key3": {"cursor": "2024-03-01"},
    }


async def test_restore_applies_kafka_tombstones(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """A value=None record (Kafka tombstone) removes the key from restored state."""
    await _create_compacted_topic(kafka_bootstrap, unique_changelog_topic)
    await _produce(kafka_bootstrap, unique_changelog_topic, [
        (b"alive", pickle.dumps({"n": 1})),
        (b"gone", pickle.dumps({"n": 2})),
        (b"gone", None),  # Kafka compaction tombstone
    ])

    restored: dict[str, dict] = {}

    async def put(k, v):
        restored[k] = v

    async def delete(k):
        restored.pop(k, None)

    consumer = AIOKafkaConsumer(bootstrap_servers=kafka_bootstrap, group_id=None)
    await consumer.start()
    try:
        count = await restore_changelog(consumer, unique_changelog_topic, put, delete)
    finally:
        await consumer.stop()

    assert count == 3
    assert restored == {"alive": {"n": 1}}


async def test_restore_returns_zero_for_empty_topic(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """An empty (but existing) compacted topic restores to zero entries.

    This exercises the private-API reach-in (`consumer._client.set_topics`) that
    forces metadata fetch — the coupling unit-test mocks cannot validate.
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
