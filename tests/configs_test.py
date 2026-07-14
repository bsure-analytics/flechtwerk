"""Tests for ConfigStore and the config-topic bootstrap/drain machinery."""
import logging
from unittest.mock import AsyncMock, MagicMock

from aiokafka import TopicPartition

from flechtwerk.configs import ConfigStore, bootstrap_config_store, drain_config_updates
from flechtwerk.testing import make_record
from flechtwerk.types import Config


async def identity(config: Config) -> Config:
    return config


# --- ConfigStore ---


def test_of_seeds_entries_and_get_decodes():
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    assert store.get("k1") == Config.wrap({"a": 1})
    assert isinstance(store.get("k1"), Config)
    assert "k1" in store
    assert len(store) == 1


def test_get_missing_key_returns_none():
    assert ConfigStore().get("nope") is None


def test_get_returns_a_fresh_config_per_call():
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    first = store.get("k1")
    first.raw["a"] = 666
    assert store.get("k1") == Config.wrap({"a": 1})


def test_get_decodes_malformed_value_to_empty_config(caplog):
    store = ConfigStore()
    # Seed the internal dict directly: put() only accepts a Record and always
    # encodes valid JSON, so there's no public way to inject a malformed value.
    store._raw["bad"] = b"{not json"
    with caplog.at_level(logging.WARNING):
        assert store.get("bad") == Config.wrap({})
    assert any("Malformed" in rec.message for rec in caplog.records)


def test_put_overwrites_earlier_value():
    store = ConfigStore()
    store.put("k1", Config.wrap({"a": 1}))
    store.put("k1", Config.wrap({"a": 2}))
    assert store.get("k1") == Config.wrap({"a": 2})


def test_delete_removes_entry():
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    store.delete("k1")
    store.delete("never-there")
    assert len(store) == 0


# --- bootstrap_config_store ---


def make_config_consumer(batches, partitions_by_topic):
    """MagicMock consumer that bootstrap_config_store can drive.

    End offsets derive from the supplied batches (max offset + 1 per
    partition) and the fetch position advances as batches are consumed,
    matching read_to_end's position-vs-end-offset termination.
    """
    remaining = list(batches)
    positions: dict = {}
    end_offsets: dict = {}
    for batch in batches:
        for tp, records in batch.items():
            end_offsets[tp] = max(end_offsets.get(tp, 0), max(r.offset for r in records) + 1)

    async def end_offsets_fn(tps):
        return {tp: end_offsets.get(tp, 0) for tp in tps}

    async def getmany(*tps, timeout_ms=0):
        if not remaining:
            return {}
        batch = remaining.pop(0)
        for tp, records in batch.items():
            positions[tp] = max(positions.get(tp, 0), max(r.offset for r in records) + 1)
        return batch

    async def position(tp):
        return positions.get(tp, 0)

    consumer = MagicMock()
    consumer._client = MagicMock()
    consumer._client.set_topics = AsyncMock()
    consumer.partitions_for_topic = lambda topic: partitions_by_topic.get(topic, set())
    consumer.assign = MagicMock()
    consumer.seek_to_beginning = AsyncMock()
    consumer.end_offsets = end_offsets_fn
    consumer.getmany = getmany
    consumer.position = position
    return consumer


async def test_bootstrap_merges_topics_into_one_namespace():
    store = ConfigStore()
    consumer = make_config_consumer(
        batches=[{
            TopicPartition("cfg-a", 0): [make_record(topic="cfg-a", key=b"k1", value=b'{"a":1}')],
            TopicPartition("cfg-b", 0): [make_record(topic="cfg-b", key=b"k2", value=b'{"b":2}')],
        }],
        partitions_by_topic={"cfg-a": {0}, "cfg-b": {0}},
    )

    latest = await bootstrap_config_store(consumer, ["cfg-a", "cfg-b"], store, identity)

    consumer._client.set_topics.assert_awaited_once_with(["cfg-a", "cfg-b"])
    consumer.assign.assert_called_once_with(
        [TopicPartition("cfg-a", 0), TopicPartition("cfg-b", 0)]
    )
    assert store.get("k1") == Config.wrap({"a": 1})
    assert store.get("k2") == Config.wrap({"b": 2})
    assert set(latest) == {"k1", "k2"}


async def test_bootstrap_compacts_by_key_and_drops_tombstoned():
    store = ConfigStore()
    tp = TopicPartition("cfg", 0)
    consumer = make_config_consumer(
        batches=[{tp: [
            make_record(topic="cfg", key=b"stale", value=b'{"a":1}', offset=0),
            make_record(topic="cfg", key=b"stale", value=b'{"a":2}', offset=1),
            make_record(topic="cfg", key=b"gone", value=b'{"b":1}', offset=2),
            make_record(topic="cfg", key=b"gone", value=b"", offset=3),
        ]}],
        partitions_by_topic={"cfg": {0}},
    )

    latest = await bootstrap_config_store(consumer, ["cfg"], store, identity)

    assert store.get("stale") == Config.wrap({"a": 2})
    assert "gone" not in store
    assert set(latest) == {"stale"}
    assert latest["stale"].offset == 1


async def test_bootstrap_enriches_once_per_surviving_entry():
    calls: list[dict] = []

    async def spy_enrich_config(config: Config) -> Config:
        calls.append(dict(config.raw))
        config.raw["enriched"] = True
        return config

    store = ConfigStore()
    tp = TopicPartition("cfg", 0)
    consumer = make_config_consumer(
        batches=[{tp: [
            make_record(topic="cfg", key=b"k1", value=b'{"a":1}', offset=0),
            make_record(topic="cfg", key=b"k1", value=b'{"a":2}', offset=1),
        ]}],
        partitions_by_topic={"cfg": {0}},
    )

    await bootstrap_config_store(consumer, ["cfg"], store, spy_enrich_config)

    # Compaction first: only the surviving record is enriched.
    assert calls == [{"a": 2}]
    assert store.get("k1") == Config.wrap({"a": 2, "enriched": True})


async def test_bootstrap_without_topics_touches_nothing():
    consumer = MagicMock()
    assert await bootstrap_config_store(consumer, [], ConfigStore(), identity) == {}
    consumer.assign.assert_not_called()


async def test_bootstrap_topic_without_partitions_yields_empty_store():
    store = ConfigStore()
    consumer = make_config_consumer(batches=[], partitions_by_topic={})

    latest = await bootstrap_config_store(consumer, ["cfg"], store, identity)

    assert latest == {}
    assert len(store) == 0


# --- drain_config_updates ---


async def test_drain_applies_enriches_and_returns_records():
    async def tagging_enrich_config(config: Config) -> Config:
        config.raw["enriched"] = True
        return config

    store = ConfigStore.of({"gone": Config.wrap({"a": 1})})
    records = {
        TopicPartition("cfg", 0): [
            make_record(topic="cfg", key=b"k1", value=b'{"a":1}'),
            make_record(topic="cfg", key=b"gone", value=b""),
        ],
    }
    consumer = MagicMock()
    consumer.getmany = AsyncMock(return_value=records)

    drained = await drain_config_updates(consumer, store, tagging_enrich_config)

    consumer.getmany.assert_awaited_once_with(timeout_ms=0)
    assert [msg.key for msg in drained] == [b"k1", b"gone"]
    assert store.get("k1") == Config.wrap({"a": 1, "enriched": True})
    assert "gone" not in store


async def test_drain_keyless_record_warns_but_applies(caplog):
    store = ConfigStore()
    records = {
        TopicPartition("cfg", 0): [make_record(topic="cfg", key=None, value=b'{"a":1}')],
    }
    consumer = MagicMock()
    consumer.getmany = AsyncMock(return_value=records)

    with caplog.at_level(logging.WARNING):
        await drain_config_updates(consumer, store, identity)

    assert store.get("") == Config.wrap({"a": 1})
    assert any("without a key" in rec.message for rec in caplog.records)


async def test_drain_without_records_returns_empty():
    consumer = MagicMock()
    consumer.getmany = AsyncMock(return_value={})
    assert await drain_config_updates(consumer, ConfigStore(), identity) == []
