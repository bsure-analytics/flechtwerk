"""Tests for fretworx state store."""
from datetime import datetime, timezone

from fretworx.state import InMemoryStateStore, RocksDBStateStore


def test_in_memory_get_missing_returns_none():
    store = InMemoryStateStore()
    assert store.get("nonexistent") is None


def test_in_memory_put_and_get():
    store = InMemoryStateStore()
    store.put("key1", {"cursor": "2024-01-01"})
    assert store.get("key1") == {"cursor": "2024-01-01"}


def test_in_memory_delete():
    store = InMemoryStateStore()
    store.put("key1", {"x": 1})
    store.delete("key1")
    assert store.get("key1") is None


def test_in_memory_delete_missing_is_noop():
    store = InMemoryStateStore()
    store.delete("nonexistent")  # should not raise


def test_in_memory_datetime_round_trip():
    store = InMemoryStateStore()
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    store.put("key", {"last_time": dt})
    restored = store.get("key")
    assert restored["last_time"] == dt
    assert isinstance(restored["last_time"], datetime)


def test_in_memory_set_round_trip():
    store = InMemoryStateStore()
    store.put("key", {"hashes": {"abc", "def", "ghi"}})
    restored = store.get("key")
    assert restored["hashes"] == {"abc", "def", "ghi"}
    assert isinstance(restored["hashes"], set)


def test_in_memory_nested_state():
    store = InMemoryStateStore()
    state = {
        "changes_since": "2024-01-01T00:00:00",
        "last_ids": [1, 2, 3],
        "nested": {"a": 1, "b": [4, 5]},
    }
    store.put("key", state)
    assert store.get("key") == state


def test_rocksdb_put_and_get(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "test-db"))
    try:
        store.put("key1", {"cursor": "2024-01-01", "count": 42})
        result = store.get("key1")
        assert result == {"cursor": "2024-01-01", "count": 42}
    finally:
        store.close()


def test_rocksdb_get_missing_returns_none(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "test-db"))
    try:
        assert store.get("nonexistent") is None
    finally:
        store.close()


def test_rocksdb_delete(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "test-db"))
    try:
        store.put("key1", {"x": 1})
        store.delete("key1")
        assert store.get("key1") is None
    finally:
        store.close()


def test_rocksdb_datetime_round_trip(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "test-db"))
    try:
        dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
        store.put("key", {"last_time": dt})
        restored = store.get("key")
        assert restored["last_time"] == dt
        assert isinstance(restored["last_time"], datetime)
    finally:
        store.close()


def test_rocksdb_set_round_trip(tmp_path):
    store = RocksDBStateStore(str(tmp_path / "test-db"))
    try:
        store.put("key", {"hashes": {"abc", "def"}})
        restored = store.get("key")
        assert restored["hashes"] == {"abc", "def"}
        assert isinstance(restored["hashes"], set)
    finally:
        store.close()


def test_rocksdb_persistence_across_reopen(tmp_path):
    db_path = str(tmp_path / "test-db")

    store = RocksDBStateStore(db_path)
    store.put("key", {"persisted": True})
    store.close()

    store2 = RocksDBStateStore(db_path)
    try:
        assert store2.get("key") == {"persisted": True}
    finally:
        store2.close()
