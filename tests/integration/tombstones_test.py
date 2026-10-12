"""Integration tests for tombstones against a real Kafka broker.

Pins the one wire distinction the framework must never blur: a NULL value is
a tombstone that log compaction removes, an empty value (``b""``) is an
ordinary record that compaction retains forever. Every framework deletion —
a falsy `State`, a ``Message(value=None)`` — must therefore reach the wire as
null, or deleted keys accumulate in the topic and every restore replays them.
"""
import asyncio
import time

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import InMemoryStateStore
from flechtwerk.types import State

pytestmark = pytest.mark.integration


async def _create_eagerly_compacted_topic(bootstrap: str, topic: str) -> None:
    """A compacted topic tuned so the log cleaner runs within seconds.

    Short segments roll the probe records out of the active segment (which is
    never cleaned), and a zero ``delete.retention.ms`` lets the cleaner drop a
    tombstone itself on its next pass.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1,
                topic_configs={
                    "cleanup.policy": "compact",
                    "delete.retention.ms": "0",
                    "min.cleanable.dirty.ratio": "0.001",
                    "min.compaction.lag.ms": "0",
                    "segment.ms": "100",
                },
            ),
        ])
    finally:
        await admin.close()


async def _read_all(bootstrap: str, topic: str) -> list[tuple[bytes, bytes | None]]:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap, group_id=None)
    await consumer.start()
    try:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        await consumer.seek_to_beginning(tp)
        end = (await consumer.end_offsets([tp]))[tp]
        records: list[tuple[bytes, bytes | None]] = []
        while await consumer.position(tp) < end:
            for batch in (await consumer.getmany(tp, timeout_ms=1000)).values():
                records += [(r.key, r.value) for r in batch]
        return records
    finally:
        await consumer.stop()


async def test_changelog_delete_writes_a_tombstone_compaction_removes(
    kafka_bootstrap: str, unique_changelog_topic: str,
) -> None:
    """`ChangelogStateStore.delete` writes a real null, so compaction removes
    the key from the changelog — whereas a b"" record for a control key
    survives compaction as its latest value."""
    topic = unique_changelog_topic
    await _create_eagerly_compacted_topic(kafka_bootstrap, topic)
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.producer = producer
        store.topic = topic

        await store.put("deleted", State.wrap({"n": 1}))
        await store.delete("deleted")
        await producer.send(topic, key=b"emptied", value=b"")
        await producer.flush()

        assert (b"deleted", None) in await _read_all(kafka_bootstrap, topic)

        # Keep rolling segments until the cleaner has run past the probe
        # records; the broker's cleaner backoff bounds how long that takes.
        deadline = time.monotonic() + 120
        while True:
            await producer.send_and_wait(topic, key=b"filler", value=b"x")
            records = [r for r in await _read_all(kafka_bootstrap, topic) if r[0] != b"filler"]
            if not any(key == b"deleted" for key, _ in records):
                break
            assert time.monotonic() < deadline, f"log cleaner never ran: {records}"
            await asyncio.sleep(1)
    finally:
        await producer.stop()

    assert records == [(b"emptied", b"")]
