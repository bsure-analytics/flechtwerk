"""Tests for fretworx state store."""
import asyncio
from datetime import datetime, timezone

from fretworx.state import ChangelogStateStore, InMemoryStateStore, RocksDBStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer
from fretworx.types import IncomingMessage, State


def test_in_memory_get_missing_returns_none():
    async def run():
        store = InMemoryStateStore()
        assert await store.get("nonexistent") is None
    asyncio.run(run())


def test_in_memory_put_and_get():
    async def run():
        store = InMemoryStateStore()
        await store.put("key1", {"cursor": "2024-01-01"})
        assert await store.get("key1") == {"cursor": "2024-01-01"}
    asyncio.run(run())


def test_in_memory_delete():
    async def run():
        store = InMemoryStateStore()
        await store.put("key1", {"x": 1})
        await store.delete("key1")
        assert await store.get("key1") is None
    asyncio.run(run())


def test_in_memory_delete_missing_is_noop():
    async def run():
        store = InMemoryStateStore()
        await store.delete("nonexistent")  # should not raise
    asyncio.run(run())


def test_in_memory_datetime_round_trip():
    async def run():
        store = InMemoryStateStore()
        dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        await store.put("key", {"last_time": dt})
        restored = await store.get("key")
        assert restored["last_time"] == dt
        assert isinstance(restored["last_time"], datetime)
    asyncio.run(run())


def test_in_memory_set_round_trip():
    async def run():
        store = InMemoryStateStore()
        await store.put("key", {"hashes": {"abc", "def", "ghi"}})
        restored = await store.get("key")
        assert restored["hashes"] == {"abc", "def", "ghi"}
        assert isinstance(restored["hashes"], set)
    asyncio.run(run())


def test_in_memory_nested_state():
    async def run():
        store = InMemoryStateStore()
        state = {
            "changes_since": "2024-01-01T00:00:00",
            "last_ids": [1, 2, 3],
            "nested": {"a": 1, "b": [4, 5]},
        }
        await store.put("key", state)
        assert await store.get("key") == state
    asyncio.run(run())


def test_rocksdb_put_and_get(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore(str(tmp_path / "test-db"))
        try:
            await store.put("key1", {"cursor": "2024-01-01", "count": 42})
            result = await store.get("key1")
            assert result == {"cursor": "2024-01-01", "count": 42}
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_get_missing_returns_none(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore(str(tmp_path / "test-db"))
        try:
            assert await store.get("nonexistent") is None
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_delete(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore(str(tmp_path / "test-db"))
        try:
            await store.put("key1", {"x": 1})
            await store.delete("key1")
            assert await store.get("key1") is None
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_datetime_round_trip(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore(str(tmp_path / "test-db"))
        try:
            dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
            await store.put("key", {"last_time": dt})
            restored = await store.get("key")
            assert restored["last_time"] == dt
            assert isinstance(restored["last_time"], datetime)
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_set_round_trip(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore(str(tmp_path / "test-db"))
        try:
            await store.put("key", {"hashes": {"abc", "def"}})
            restored = await store.get("key")
            assert restored["hashes"] == {"abc", "def"}
            assert isinstance(restored["hashes"], set)
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_persistence_across_reopen(tmp_path):
    async def run(tmp_path):
        db_path = str(tmp_path / "test-db")

        store = RocksDBStateStore(db_path)
        await store.put("key", {"persisted": True})
        await store.close()

        store2 = RocksDBStateStore(db_path)
        try:
            assert await store2.get("key") == {"persisted": True}
        finally:
            await store2.close()
    asyncio.run(run(tmp_path))


# --- ChangelogStateStore tests ---


def test_changelog_put_writes_to_inner_and_producer():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        await store.put("k1", State({"cursor": 42}))

        assert await inner.get("k1") == {"cursor": 42}
        assert len(producer.sent) == 1
        assert producer.sent[0].key == "k1"
        assert producer.sent[0].topic == "test-changelog"
        assert producer.sent[0].value == {"cursor": 42}

    asyncio.run(run())


def test_changelog_get_reads_from_inner():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        await inner.put("k1", State({"data": 1}))
        result = await store.get("k1")
        assert result == {"data": 1}
        assert len(producer.sent) == 0

    asyncio.run(run())


def test_changelog_delete_writes_tombstone():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        await store.put("k1", State({"data": 1}))
        await store.delete("k1")

        assert await inner.get("k1") is None
        assert len(producer.sent) == 2
        assert producer.sent[1].value == {}

    asyncio.run(run())


def test_changelog_restore_rebuilds_state():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        changelog_messages = [
            IncomingMessage(key="k1", offset=0, partition=0, timestamp=None, topic="test-changelog",
                            value={"cursor": 10}),
            IncomingMessage(key="k2", offset=1, partition=0, timestamp=None, topic="test-changelog",
                            value={"cursor": 20}),
            IncomingMessage(key="k1", offset=2, partition=0, timestamp=None, topic="test-changelog",
                            value={"cursor": 15}),
        ]
        consumer = FakeKafkaConsumer(changelog_messages)

        await store.restore(consumer)

        assert await inner.get("k1") == {"cursor": 15}
        assert await inner.get("k2") == {"cursor": 20}

    asyncio.run(run())


def test_changelog_restore_handles_tombstones():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        changelog_messages = [
            IncomingMessage(key="k1", offset=0, partition=0, timestamp=None, topic="test-changelog",
                            value={"cursor": 10}),
            IncomingMessage(key="k1", offset=1, partition=0, timestamp=None, topic="test-changelog",
                            value={}),
        ]
        consumer = FakeKafkaConsumer(changelog_messages)

        await store.restore(consumer)

        assert await inner.get("k1") is None

    asyncio.run(run())


def test_changelog_restore_empty_topic():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        consumer = FakeKafkaConsumer([])
        await store.restore(consumer)

        assert await inner.get("anything") is None

    asyncio.run(run())


def test_changelog_close_flushes_producer():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore(inner, producer, "test-changelog")

        await store.put("k1", State({"data": 1}))
        await store.close()

        assert producer.flushed

    asyncio.run(run())
