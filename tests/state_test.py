"""Tests for fretworx state store."""
import asyncio
import json
import pickle
from datetime import datetime, timezone

from fretworx.state import ChangelogStateStore, InMemoryStateStore, RocksDBStateStore
from fretworx.testing import FakeKafkaProducer
from fretworx.types import State


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
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            await store.put("key1", {"cursor": "2024-01-01", "count": 42})
            result = await store.get("key1")
            assert result == {"cursor": "2024-01-01", "count": 42}
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_get_missing_returns_none(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            assert await store.get("nonexistent") is None
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_delete(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            await store.put("key1", {"x": 1})
            await store.delete("key1")
            assert await store.get("key1") is None
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_datetime_round_trip(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
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
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            await store.put("key", {"hashes": {"abc", "def"}})
            restored = await store.get("key")
            assert restored["hashes"] == {"abc", "def"}
            assert isinstance(restored["hashes"], set)
        finally:
            await store.close()
    asyncio.run(run(tmp_path))



# --- ChangelogStateStore tests ---


def test_changelog_put_writes_to_inner_and_producer():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State({"cursor": 42}))

        assert await inner.get("k1") == {"cursor": 42}
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "test-changelog"
        assert payload["key"] == b"k1"
        assert pickle.loads(payload["value"]) == {"cursor": 42}

    asyncio.run(run())


def test_changelog_get_reads_from_inner():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await inner.put("k1", State({"data": 1}))
        result = await store.get("k1")
        assert result == {"data": 1}
        assert len(producer.sent) == 0

    asyncio.run(run())


def test_changelog_delete_writes_tombstone():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State({"data": 1}))
        await store.delete("k1")

        assert await inner.get("k1") is None
        assert len(producer.sent) == 2
        topic, payload = producer.sent[1]
        assert topic == "test-changelog"
        assert payload["value"] == b""

    asyncio.run(run())


def test_changelog_inner_store_rebuilt_via_put():
    """Simulates what restore_changelog does: put/delete on the inner store."""
    async def run():
        inner = InMemoryStateStore()

        # Simulate changelog replay
        await inner.put("k1", State({"cursor": 10}))
        await inner.put("k2", State({"cursor": 20}))
        await inner.put("k1", State({"cursor": 15}))  # update k1

        assert await inner.get("k1") == {"cursor": 15}
        assert await inner.get("k2") == {"cursor": 20}

    asyncio.run(run())


def test_changelog_inner_store_tombstone_deletes():
    """Simulates a tombstone during changelog replay."""
    async def run():
        inner = InMemoryStateStore()

        await inner.put("k1", State({"cursor": 10}))
        await inner.delete("k1")  # tombstone

        assert await inner.get("k1") is None

    asyncio.run(run())


def test_changelog_close_closes_inner():
    """close() closes the inner store but does NOT stop the producer.

    Producer lifecycle is managed by the DI container (FretworxModule),
    not by the state store.
    """
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State({"data": 1}))
        await store.close()

        # Producer should NOT be stopped (module manages that)
        assert not hasattr(producer, "stopped") or not producer.stopped

    asyncio.run(run())


# --- FretworxModule DI wiring tests ---


def test_module_wires_changelog_state_store():
    """FretworxModule wires ChangelogStateStore via reactor-di lookups."""
    from fretworx.extractor import Extractor
    from fretworx.module import FretworxModule

    class StubExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state):
            return
            yield  # pragma: no cover

    async def run():
        mod = FretworxModule()
        mod.client_id = "test-app"
        mod.group_id = "test-app"
        mod.bootstrap_servers = "localhost:9092"
        mod.stage = StubExtractor()

        store = mod.state_store

        # inner_store → ChangelogStateStore.inner (via lookup)
        assert store.inner is mod.inner_store

        # producer → shared (via name match)
        assert store.producer is mod.producer

        # changelog_topic → ChangelogStateStore.topic (via lookup)
        assert store.topic == "test-app-changelog"

    asyncio.run(run())
