"""Tests for Flechtwerk state store."""
import asyncio
from datetime import datetime, timezone
from typing import Final

import pytest

from flechtwerk.attribute import ANY, Attribute, DATETIME, DICT, INT, SET, STR
from flechtwerk.state import ChangelogStateStore, RocksDBStateStore
from flechtwerk.testing import FakeKafkaProducer, InMemoryStateStore
from flechtwerk.types import State


CHANGES_SINCE: Final = Attribute("changes_since", STR)
COUNT: Final = Attribute("count", INT)
DATA: Final = Attribute("data", INT)
HASHES: Final = Attribute("hashes", SET(STR))
LAST_TIME: Final = Attribute("last_time", DATETIME)
NESTED: Final = Attribute("nested", DICT(ANY))
X: Final = Attribute("x", INT)


def test_in_memory_get_missing_returns_none():
    async def run():
        store = InMemoryStateStore()
        assert await store.get("nonexistent") is None
    asyncio.run(run())


def test_in_memory_put_and_get():
    async def run():
        store = InMemoryStateStore()
        await store.put("key1", State.wrap({"cursor": "2024-01-01"}))
        result = await store.get("key1")
        assert result.raw == {"cursor": "2024-01-01"}
    asyncio.run(run())


def test_in_memory_delete():
    async def run():
        store = InMemoryStateStore()
        await store.put("key1", State.wrap({"x": 1}))
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
        state = State()
        state[LAST_TIME] = dt   # encoder runs → ISO string in raw
        await store.put("key", state)
        restored = await store.get("key")
        assert restored[LAST_TIME] == dt
        assert isinstance(restored[LAST_TIME], datetime)
    asyncio.run(run())


def test_in_memory_set_round_trip():
    async def run():
        store = InMemoryStateStore()
        state = State()
        state[HASHES] = {"abc", "def", "ghi"}   # encoder runs → sorted list in raw
        await store.put("key", state)
        restored = await store.get("key")
        assert restored[HASHES] == {"abc", "def", "ghi"}
        assert isinstance(restored[HASHES], set)
    asyncio.run(run())


def test_in_memory_nested_state():
    async def run():
        store = InMemoryStateStore()
        raw = {
            "changes_since": "2024-01-01T00:00:00",
            "last_ids": [1, 2, 3],
            "nested": {"a": 1, "b": [4, 5]},
        }
        await store.put("key", State.wrap(raw))
        result = await store.get("key")
        assert result.raw == raw
    asyncio.run(run())


def test_rocksdb_put_and_get(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            await store.put("key1", State.wrap({"cursor": "2024-01-01", "count": 42}))
            result = await store.get("key1")
            assert result.raw == {"cursor": "2024-01-01", "count": 42}
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
            await store.put("key1", State.wrap({"x": 1}))
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
            state = State()
            state[LAST_TIME] = dt   # encoder runs → ISO string in raw
            await store.put("key", state)
            restored = await store.get("key")
            assert restored[LAST_TIME] == dt
            assert isinstance(restored[LAST_TIME], datetime)
        finally:
            await store.close()
    asyncio.run(run(tmp_path))


def test_rocksdb_set_round_trip(tmp_path):
    async def run(tmp_path):
        store = RocksDBStateStore()
        store.path = tmp_path / "test-db"
        try:
            state = State()
            state[HASHES] = {"abc", "def"}   # encoder runs → sorted list in raw
            await store.put("key", state)
            restored = await store.get("key")
            assert restored[HASHES] == {"abc", "def"}
            assert isinstance(restored[HASHES], set)
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

        await store.put("k1", State.wrap({"cursor": 42}))

        result = await inner.get("k1")
        assert result.raw == {"cursor": 42}
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "test-changelog"
        assert payload["key"] == b"k1"
        assert payload["value"] == b'{"cursor":42}'

    asyncio.run(run())


def test_changelog_get_reads_from_inner():
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await inner.put("k1", State.wrap({"data": 1}))
        result = await store.get("k1")
        assert result.raw == {"data": 1}
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

        await store.put("k1", State.wrap({"data": 1}))
        await store.delete("k1")

        assert await inner.get("k1") is None
        assert len(producer.sent) == 2
        topic, payload = producer.sent[1]
        assert topic == "test-changelog"
        assert payload["value"] == b""

    asyncio.run(run())


def test_changelog_partition_pins_every_send():
    """A partition-scoped store (transformer task) sends puts and tombstones
    to its explicit changelog partition, bypassing key hashing."""
    async def run():
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.partition = 3
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State.wrap({"data": 1}))
        await store.delete("k1")

        assert [payload["partition"] for _, payload in producer.sent] == [3, 3]

    asyncio.run(run())


def test_changelog_partition_defaults_to_key_hashing():
    """Without a partition (extractor path), routing is left to the producer."""
    async def run():
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State.wrap({"data": 1}))

        assert producer.sent[0][1]["partition"] is None

    asyncio.run(run())


def test_changelog_inner_store_rebuilt_via_put():
    """Simulates what restore_changelog does: put/delete on the inner store."""
    async def run():
        inner = InMemoryStateStore()

        # Simulate changelog replay
        await inner.put("k1", State.wrap({"cursor": 10}))
        await inner.put("k2", State.wrap({"cursor": 20}))
        await inner.put("k1", State.wrap({"cursor": 15}))  # update k1

        result_k1 = await inner.get("k1")
        result_k2 = await inner.get("k2")
        assert result_k1.raw == {"cursor": 15}
        assert result_k2.raw == {"cursor": 20}

    asyncio.run(run())


def test_changelog_inner_store_tombstone_deletes():
    """Simulates a tombstone during changelog replay."""
    async def run():
        inner = InMemoryStateStore()

        await inner.put("k1", State.wrap({"cursor": 10}))
        await inner.delete("k1")  # tombstone

        assert await inner.get("k1") is None

    asyncio.run(run())


def test_changelog_close_closes_inner():
    """close() closes the inner store but does NOT stop the producer.

    Producer lifecycle is managed by the DI container (Flechtwerk),
    not by the state store.
    """
    async def run():
        inner = InMemoryStateStore()
        producer = FakeKafkaProducer()
        store = ChangelogStateStore()
        store.inner = inner
        store.producer = producer
        store.topic = "test-changelog"

        await store.put("k1", State.wrap({"data": 1}))
        await store.close()

        # Producer should NOT be stopped (module manages that)
        assert not hasattr(producer, "stopped") or not producer.stopped

    asyncio.run(run())


# --- Flechtwerk DI wiring tests ---


def test_module_wires_changelog_state_store():
    """The container wires ChangelogStateStore via reactor-di lookups."""
    from flechtwerk.extractor import Extractor
    from flechtwerk.module import _FlechtwerkModule

    class StubExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state):
            return
            yield  # pragma: no cover

    async def run():
        mod = _FlechtwerkModule()
        mod.application_id = "test-app"
        mod.bootstrap_servers = "localhost:9092"
        mod.client_id = "test-app"
        mod.compression_type = None
        mod.stage = StubExtractor()

        store = mod.state_store

        # inner_store → ChangelogStateStore.inner (via lookup)
        assert store.inner is mod.inner_store

        # producer → shared (via name match)
        assert store.producer is mod.producer

        # changelog_topic → ChangelogStateStore.topic (via lookup)
        assert store.topic == "test-app-changelog"

    asyncio.run(run())


def test_module_lookups_resolve_from_parent():
    """A parent reactor-di module can inject every container lookup field by
    name when the Flechtwerk container is embedded as a child component.

    Applications get the narrow ``Flechtwerk`` handle from ``Flechtwerk.of``;
    embedding wires the concrete container directly via ``make[Flechtwerk,
    _FlechtwerkModule]``. Reactor-di calls the child's no-arg constructor and
    then writes a dependency map into ``instance.__dict__``; subsequent
    access to a ``lookup[X]`` field falls through to ``__getattr__`` only if
    the field is *not* already in ``__dict__``. The no-arg constructor sets
    nothing, so it leaves every slot for the parent to fill.
    """
    from flechtwerk.extractor import Extractor
    from flechtwerk.module import Flechtwerk, _FlechtwerkModule
    from reactor_di import CachingStrategy, make, module

    class StubExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state):
            return
            yield  # pragma: no cover

    stage = StubExtractor()

    @module(CachingStrategy.NOT_THREAD_SAFE)
    class App:
        # Primitive-typed annotations are skipped by @module — values come
        # from instance attributes set after construction.
        application_id: str
        bootstrap_servers: str
        client_id: str
        metrics_labels: dict
        metrics_port: int
        poll_interval_seconds: int
        stage: Extractor

        # Child component — reactor-di instantiates the private concrete
        # container and builds its dependency map from the parent's matching
        # attribute names.
        flechtwerk: make[Flechtwerk, _FlechtwerkModule]

    app = App()
    app.application_id = "aid"
    app.bootstrap_servers = "broker:9092"
    app.client_id = "cid"
    app.metrics_labels = {"env": "test"}
    app.metrics_port = 9464
    app.poll_interval_seconds = 30
    app.stage = stage

    f = app.flechtwerk

    assert f.application_id == "aid"
    assert f.bootstrap_servers == "broker:9092"
    assert f.client_id == "cid"
    assert f.metrics_labels == {"env": "test"}
    assert f.metrics_port == 9464
    assert f.poll_interval_seconds == 30
    assert f.stage is stage


def test_app_factory_defaults_metrics_when_omitted():
    """Flechtwerk.of fills metrics_labels={} / metrics_port=0 when those
    args are omitted, so the resulting Flechtwerk is fully configured even
    when the caller doesn't care about Prometheus."""
    from flechtwerk.extractor import Extractor
    from flechtwerk.module import Flechtwerk

    class StubExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state):
            return
            yield  # pragma: no cover

    f = Flechtwerk.of(
        application_id="g",
        bootstrap_servers="b:9092",
        client_id="c",
        poll_interval_seconds=60,
        stage=StubExtractor(),
    )

    assert f.metrics_labels == {}
    assert f.metrics_port == 0


def test_bare_constructor_leaves_every_lookup_unbound_for_parent():
    """The container's bare constructor (no args) sets nothing, so a parent
    module can inject every lookup field — including metrics_labels and
    metrics_port."""
    from flechtwerk.module import _FlechtwerkModule

    f = _FlechtwerkModule()
    for name in (
        "application_id",
        "bootstrap_servers",
        "client_id",
        "compression_type",
        "metrics_labels",
        "metrics_port",
        "mqtt",
        "poll_interval_seconds",
        "stage",
    ):
        assert name not in f.__dict__


def test_public_handle_exposes_no_container_internals():
    """The public ``Flechtwerk`` handle carries only of()/run()/the async
    context manager — never the DI container's resource factories.

    This is the encapsulation gate: ``@module`` walks ``get_type_hints``
    over the MRO, so a stray annotated attribute on the base would silently
    become a DI-managed name AND leak onto this public type. Keep the base
    annotation-free.
    """
    from typing import get_type_hints

    from flechtwerk.module import Flechtwerk

    assert get_type_hints(Flechtwerk) == {}
    assert {n for n in dir(Flechtwerk) if not n.startswith("_")} == {"of", "run"}


def test_bare_public_handle_cannot_be_constructed():
    """``Flechtwerk`` is abstract — applications must go through ``of``.

    The loud failure is the point: a bare ``Flechtwerk()`` (or a parent module
    annotating the abstract base instead of ``make[Flechtwerk,
    _FlechtwerkModule]``) raises immediately rather than silently mis-wiring.
    """
    from flechtwerk.module import Flechtwerk

    with pytest.raises(TypeError, match="abstract"):
        Flechtwerk()
