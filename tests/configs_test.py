"""Tests for ConfigStore and the config-topic bootstrap/drain machinery."""
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka import TopicPartition

from flechtwerk.configs import ConfigStore, bootstrap_config_store, drain_config_updates
from flechtwerk.testing import make_record
from flechtwerk.types import Config, Event, InvalidMessageError


async def identity(config: Config) -> Config:
    return config


def raising(error: InvalidMessageError):
    """The default `Stage.on_invalid_message` policy, as a bare handler."""
    raise error


def skipping(error: InvalidMessageError):
    """Handler that skips the record."""
    return None


def substituting(record):
    """Handler factory: substitute ``record`` for the undecodable part."""
    return lambda error: record


def recording(outcome):
    """Handler factory: record every invocation, then apply ``outcome``."""
    seen: list[InvalidMessageError] = []

    def handler(error: InvalidMessageError):
        seen.append(error)
        return outcome(error)

    return handler, seen


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


def test_get_crashes_on_malformed_store_bytes():
    """A malformed stored value is a framework bug, not an app policy question.

    `put` re-encodes every value, so the store can only ever hold
    `encode_json` output — the application's `on_invalid_message` already ran
    on the way in. If a malformed value appears anyway, crash rather than
    launder it into an empty Config.
    """
    store = ConfigStore()
    # Seed the internal dict directly: put() only accepts a Record and always
    # encodes valid JSON, so there's no public way to inject a malformed value.
    store._raw["bad"] = b"{not json"
    with pytest.raises(ValueError):
        store.get("bad")


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

    latest = await bootstrap_config_store(consumer, ["cfg-a", "cfg-b"], store, identity, raising)

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

    latest = await bootstrap_config_store(consumer, ["cfg"], store, identity, raising)

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

    await bootstrap_config_store(consumer, ["cfg"], store, spy_enrich_config, raising)

    # Compaction first: only the surviving record is enriched.
    assert calls == [{"a": 2}]
    assert store.get("k1") == Config.wrap({"a": 2, "enriched": True})


async def test_bootstrap_returns_decoded_messages():
    """The extractor runner builds its entries from these — no re-parse."""
    store = ConfigStore()
    tp = TopicPartition("cfg", 0)
    consumer = make_config_consumer(
        batches=[{tp: [make_record(topic="cfg", key=b"k1", value=b'{"a":1}', offset=4, partition=0)]}],
        partitions_by_topic={"cfg": {0}},
    )

    latest = await bootstrap_config_store(consumer, ["cfg"], store, identity, raising)

    msg = latest["k1"]
    assert msg.key == "k1"
    assert msg.offset == 4
    assert msg.topic == "cfg"
    assert msg.value == Event.wrap({"a": 1})


async def test_bootstrap_without_topics_touches_nothing():
    consumer = MagicMock()
    assert await bootstrap_config_store(consumer, [], ConfigStore(), identity, raising) == {}
    consumer.assign.assert_not_called()


async def test_bootstrap_topic_without_partitions_yields_empty_store():
    store = ConfigStore()
    consumer = make_config_consumer(batches=[], partitions_by_topic={})

    latest = await bootstrap_config_store(consumer, ["cfg"], store, identity, raising)

    assert latest == {}
    assert len(store) == 0


# --- Invalid config records ---


def broken_value_consumer(*, key=b"k1", value=b"{not json", offset=0):
    """One config topic carrying a single undecodable surviving record."""
    tp = TopicPartition("cfg", 0)
    return make_config_consumer(
        batches=[{tp: [make_record(topic="cfg", key=key, value=value, offset=offset)]}],
        partitions_by_topic={"cfg": {0}},
    )


async def test_bootstrap_default_policy_crashes_on_an_undecodable_value():
    """One bad config record crash-loops the stage — deliberate for an ops-written topic."""
    store = ConfigStore()
    with pytest.raises(InvalidMessageError) as excinfo:
        await bootstrap_config_store(broken_value_consumer(), ["cfg"], store, identity, raising)
    assert excinfo.value.part == "value"
    assert len(store) == 0


async def test_bootstrap_skip_leaves_the_key_absent():
    """The app chose masquerade-as-missing; the key appears in neither result nor store."""
    store = ConfigStore()

    latest = await bootstrap_config_store(broken_value_consumer(), ["cfg"], store, identity, skipping)

    assert latest == {}
    assert "k1" not in store
    assert len(store) == 0


async def test_bootstrap_substitute_lands_enriched_in_the_store():
    async def tagging_enrich_config(config: Config) -> Config:
        config.raw["enriched"] = True
        return config

    store = ConfigStore()
    handler = substituting(Event.wrap({"recovered": True}))

    latest = await bootstrap_config_store(broken_value_consumer(), ["cfg"], store, tagging_enrich_config, handler)

    assert store.get("k1") == Config.wrap({"recovered": True, "enriched": True})
    # The returned message carries the substituted value PRE-enrichment.
    assert latest["k1"].value == Event.wrap({"recovered": True})


async def test_bootstrap_fires_the_handler_once_per_broken_record():
    """Decode-once: compaction reads keys, the surviving record's value is read once."""
    handler, seen = recording(skipping)
    tp = TopicPartition("cfg", 0)
    consumer = make_config_consumer(
        batches=[{tp: [
            make_record(topic="cfg", key=b"k1", value=b'{"a":1}', offset=0),
            make_record(topic="cfg", key=b"k1", value=b"{not json", offset=1),
        ]}],
        partitions_by_topic={"cfg": {0}},
    )

    await bootstrap_config_store(consumer, ["cfg"], ConfigStore(), identity, handler)

    assert [e.part for e in seen] == ["value"]
    assert seen[0].offset == 1


async def test_bootstrap_skips_a_stale_records_undecodable_value_entirely():
    """A compacted-away record's value is never read, so it never reaches the handler."""
    handler, seen = recording(skipping)
    store = ConfigStore()
    tp = TopicPartition("cfg", 0)
    consumer = make_config_consumer(
        batches=[{tp: [
            make_record(topic="cfg", key=b"k1", value=b"{not json", offset=0),
            make_record(topic="cfg", key=b"k1", value=b'{"a":2}', offset=1),
        ]}],
        partitions_by_topic={"cfg": {0}},
    )

    await bootstrap_config_store(consumer, ["cfg"], store, identity, handler)

    assert seen == []
    assert store.get("k1") == Config.wrap({"a": 2})


async def test_bootstrap_undecodable_key_reaches_the_handler_as_part_key():
    """Compaction needs the key, so a key failure surfaces during collect."""
    handler, seen = recording(skipping)
    store = ConfigStore()

    latest = await bootstrap_config_store(
        broken_value_consumer(key=b"\xff\xfe", value=b'{"a":1}'), ["cfg"], store, identity, handler,
    )

    assert [e.part for e in seen] == ["key"]
    assert latest == {}
    assert len(store) == 0


async def test_bootstrap_rejects_key_substitution():
    store = ConfigStore()
    handler = substituting(Event.wrap({"key": "invented"}))
    with pytest.raises(TypeError, match="state identity"):
        await bootstrap_config_store(
            broken_value_consumer(key=b"\xff\xfe", value=b'{"a":1}'), ["cfg"], store, identity, handler,
        )


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

    drained = await drain_config_updates(consumer, store, tagging_enrich_config, raising)

    consumer.getmany.assert_awaited_once_with(timeout_ms=0)
    # Decoded, in arrival order — a tombstone comes back with an empty value.
    assert [(msg.key, msg.value) for msg in drained] == [
        ("k1", Event.wrap({"a": 1})),
        ("gone", Event.wrap({})),
    ]
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
        await drain_config_updates(consumer, store, identity, raising)

    assert store.get("") == Config.wrap({"a": 1})
    assert any("without a key" in rec.message for rec in caplog.records)


async def test_drain_without_records_returns_empty():
    consumer = MagicMock()
    consumer.getmany = AsyncMock(return_value={})
    assert await drain_config_updates(consumer, ConfigStore(), identity, raising) == []


def drain_consumer(*records):
    consumer = MagicMock()
    consumer.getmany = AsyncMock(return_value={TopicPartition("cfg", 0): list(records)})
    return consumer


async def test_returned_value_is_pre_enrichment_even_for_nested_edits():
    """Callers derive `extract_state_key` from the returned value, so it must be
    the record as it arrived — `enrich_config` gets a private deep copy, and a
    plain `Config(value)` would share nested structure with it."""
    async def nesting_enrich_config(config: Config) -> Config:
        config.raw["nested"]["added"] = True
        config.raw["top"] = "added"
        return config

    store = ConfigStore()
    consumer = drain_consumer(make_record(topic="cfg", key=b"k1", value=b'{"nested":{"a":1}}'))

    drained = await drain_config_updates(consumer, store, nesting_enrich_config, raising)

    assert drained[0].value == Event.wrap({"nested": {"a": 1}})
    assert store.get("k1") == Config.wrap({"nested": {"a": 1, "added": True}, "top": "added"})


async def test_drain_skip_retains_the_previous_value():
    """A skipped update changes nothing — the store keeps what it held."""
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    consumer = drain_consumer(make_record(topic="cfg", key=b"k1", value=b"{not json"))

    drained = await drain_config_updates(consumer, store, identity, skipping)

    assert drained == []
    assert store.get("k1") == Config.wrap({"a": 1})


async def test_drain_substitute_replaces_the_value():
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    consumer = drain_consumer(make_record(topic="cfg", key=b"k1", value=b"\xff\xfe"))

    drained = await drain_config_updates(
        consumer, store, identity, substituting(Event.wrap({"recovered": True})),
    )

    assert [msg.key for msg in drained] == ["k1"]
    assert store.get("k1") == Config.wrap({"recovered": True})


async def test_drain_tombstone_with_an_undecodable_key_fires_part_key():
    """A tombstone's value never reaches the handler, but its key still does —
    and skipping means the deletion is not applied."""
    handler, seen = recording(skipping)
    store = ConfigStore.of({"": Config.wrap({"a": 1})})
    consumer = drain_consumer(make_record(topic="cfg", key=b"\xff\xfe", value=b""))

    drained = await drain_config_updates(consumer, store, identity, handler)

    assert [e.part for e in seen] == ["key"]
    assert drained == []
    assert len(store) == 1


async def test_drain_tombstone_value_never_reaches_the_handler():
    """`is_tombstone` runs on raw bytes, before any value decode."""
    handler, seen = recording(raising)
    store = ConfigStore.of({"k1": Config.wrap({"a": 1})})
    consumer = drain_consumer(make_record(topic="cfg", key=b"k1", value=None))

    drained = await drain_config_updates(consumer, store, identity, handler)

    assert seen == []
    assert [msg.key for msg in drained] == ["k1"]
    assert "k1" not in store


async def test_drain_default_policy_crashes():
    store = ConfigStore()
    consumer = drain_consumer(make_record(topic="cfg", key=b"k1", value=b"[1,2,3]", offset=6))

    with pytest.raises(InvalidMessageError) as excinfo:
        await drain_config_updates(consumer, store, identity, raising)

    assert (excinfo.value.part, excinfo.value.offset) == ("value", 6)
    assert isinstance(excinfo.value.__cause__, ValueError)
