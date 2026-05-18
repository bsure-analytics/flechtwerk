"""Tests for fretworx Kafka utilities."""
import asyncio
import json
import logging
import pickle
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiokafka
import pytest
from hypothesis import given, strategies as st

from fretworx.kafka import (
    datetime_to_millis,
    encode_json,
    millis_to_datetime,
    parse_message,
    restore_changelog,
)
from fretworx.state import serialize
from fretworx.types import Event, State


def test_encode_json_string_passthrough():
    assert encode_json("already a string") == b"already a string"


def test_encode_json_dict():
    result = encode_json({"b": 2, "a": 1})
    assert result == b'{"a":1,"b":2}'  # sorted keys, compact


def test_encode_json_unicode():
    result = encode_json({"name": "Müller"})
    assert "Müller".encode("utf-8") in result  # ensure_ascii=False


def test_encode_json_rejects_nan():
    with pytest.raises(ValueError):
        encode_json({"x": float("nan")})


def test_encode_json_nested():
    result = encode_json({"outer": {"inner": [1, 2, 3]}})
    assert result == b'{"outer":{"inner":[1,2,3]}}'


def test_datetime_to_millis():
    dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert datetime_to_millis(dt) == 1704067200000


def test_datetime_to_millis_none():
    assert datetime_to_millis(None) is None


def test_millis_to_datetime():
    dt = millis_to_datetime(1704067200000)
    assert dt == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_millis_to_datetime_none():
    assert millis_to_datetime(None) is None


def test_millis_round_trip():
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    assert millis_to_datetime(datetime_to_millis(dt)) == dt


# --- parse_message ---


# JSON-safe leaves (no NaN/Infinity; floats finite).
_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(),
)
# Non-string JSON values (since encode_json treats str specially — raw UTF-8 passthrough).
_json_non_strings = st.recursive(
    st.one_of(
        st.none(), st.booleans(),
        st.integers(min_value=-(2**53), max_value=2**53),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=5),
        st.dictionaries(st.text(), children, max_size=5),
    ),
    max_leaves=20,
).filter(lambda v: not isinstance(v, str))
# Dicts only — parse_message normalizes non-dict JSON payloads to {} by design.
_json_dicts = st.dictionaries(
    st.text(),
    st.recursive(_json_scalars, lambda c: st.one_of(
        st.lists(c, max_size=5),
        st.dictionaries(st.text(), c, max_size=5),
    ), max_leaves=10),
    max_size=5,
)


@given(_json_non_strings)
def test_encode_json_round_trips_non_string_values(value):
    """Non-string values round-trip through encode_json → json.loads."""
    assert json.loads(encode_json(value).decode("utf-8")) == value


@given(_json_non_strings)
def test_encode_json_dict_keys_are_sorted(value):
    """Dict keys at every nesting level must appear in sorted order."""
    encoded = encode_json(value).decode("utf-8")
    parsed = json.loads(encoded, object_pairs_hook=list)

    def _assert_sorted(obj):
        if isinstance(obj, list) and obj and isinstance(obj[0], tuple):
            keys = [k for k, _ in obj]
            assert keys == sorted(keys)
            for _, v in obj:
                _assert_sorted(v)
        elif isinstance(obj, list):
            for item in obj:
                _assert_sorted(item)

    _assert_sorted(parsed)


@given(st.text())
def test_encode_json_string_is_utf8_passthrough(s):
    """Plain strings are written as raw UTF-8 bytes (not JSON-quoted)."""
    assert encode_json(s) == s.encode("utf-8")


@given(
    key=st.one_of(st.none(), st.binary(), st.text()),
    value=_json_dicts,
    offset=st.integers(min_value=0, max_value=2**63 - 1),
    partition=st.integers(min_value=0, max_value=999),
    timestamp=st.one_of(
        st.none(),
        st.integers(min_value=0, max_value=2**40),
    ),
)
def test_parse_message_round_trips_dict_payloads(key, value, offset, partition, timestamp):
    """encode_json → parse_message round-trips for dict payloads.

    Non-dict payloads are normalized to {} by parse_message, so the
    round-trip contract only holds for dicts.
    """
    encoded_value = encode_json(value)
    raw = SimpleNamespace(
        key=key, value=encoded_value, offset=offset, partition=partition,
        timestamp=timestamp, topic="some-topic",
    )
    msg = parse_message(raw)
    assert msg.value.raw == value
    assert msg.offset == offset
    assert msg.partition == partition
    assert isinstance(msg.key, str)


@given(st.binary(max_size=200))
def test_parse_message_never_raises_on_arbitrary_value_bytes(data):
    """Arbitrary bytes in the value position either decode or fall back to Event({})."""
    raw = SimpleNamespace(
        key=b"k", value=data, offset=0, partition=0, timestamp=None, topic="t",
    )
    msg = parse_message(raw)
    assert isinstance(msg.value, Event)


def test_parse_message_non_dict_json_falls_back_to_empty_event(caplog):
    """Valid JSON that decodes to a non-dict (e.g. a scalar or array) → Event({})."""
    raw = SimpleNamespace(
        key=b"k", value=b"42", offset=7, partition=0, timestamp=None, topic="t",
    )
    with caplog.at_level(logging.WARNING, logger="fretworx.kafka"):
        msg = parse_message(raw)
    assert msg.value == Event({})
    assert any(
        "Non-dict JSON payload" in rec.message and "int" in rec.message
        for rec in caplog.records
    )


def test_parse_message_invalid_json_falls_back_to_empty_event(caplog):
    raw = SimpleNamespace(
        key=b"some-key",
        value=b"not valid json {",
        offset=42,
        partition=1,
        timestamp=1704067200000,
        topic="my-topic",
    )
    with caplog.at_level(logging.WARNING, logger="fretworx.kafka"):
        msg = parse_message(raw)
    assert msg.key == "some-key"
    assert msg.value == Event({})
    assert msg.offset == 42
    assert any("Malformed" in rec.message and "JSONDecodeError" in rec.message for rec in caplog.records)


# --- restore_changelog ---


def _make_record(key, value, partition=0, offset=0):
    """Construct a minimal record that matches what aiokafka yields from getmany()."""
    return SimpleNamespace(key=key, value=value, partition=partition, offset=offset)


def _make_restore_consumer(batches, partitions=(0,)):
    """Build a MagicMock consumer that restore_changelog can drive.

    Args:
        batches: Sequence of dicts {tp: [record, ...]} — one returned per getmany call.
                 An empty dict is appended to signal end-of-stream.
        partitions: Partition numbers to report from partitions_for_topic().
                    Pass an empty set/None to simulate a missing topic.
    """
    consumer = MagicMock()
    consumer._client = MagicMock()
    consumer._client.set_topics = AsyncMock()
    consumer.partitions_for_topic = MagicMock(
        return_value=set(partitions) if partitions else partitions,
    )
    consumer.assign = MagicMock()
    consumer.seek_to_beginning = AsyncMock()
    consumer.getmany = AsyncMock(side_effect=[*batches, {}])
    return consumer


def test_restore_changelog_returns_zero_when_topic_has_no_partitions():
    async def run():
        consumer = _make_restore_consumer(batches=[], partitions=None)
        put = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "missing-topic", put, delete)

        assert count == 0
        consumer._client.set_topics.assert_awaited_once_with(["missing-topic"])
        consumer.assign.assert_not_called()
        consumer.seek_to_beginning.assert_not_called()
        put.assert_not_called()
        delete.assert_not_called()
    asyncio.run(run())


def test_restore_changelog_primes_metadata_before_querying_partitions():
    async def run():
        consumer = _make_restore_consumer(batches=[], partitions=(0,))
        await restore_changelog(consumer, "cl-topic", AsyncMock(), AsyncMock())

        consumer._client.set_topics.assert_awaited_once_with(["cl-topic"])
        consumer.partitions_for_topic.assert_called_once_with("cl-topic")
    asyncio.run(run())


def test_restore_changelog_assigns_all_partitions_and_seeks_to_beginning():
    async def run():
        consumer = _make_restore_consumer(batches=[], partitions=(0, 1, 2))
        await restore_changelog(consumer, "cl-topic", AsyncMock(), AsyncMock())

        (assigned_tps,), _ = consumer.assign.call_args
        assert set(assigned_tps) == {
            aiokafka.TopicPartition("cl-topic", p) for p in (0, 1, 2)
        }
        consumer.seek_to_beginning.assert_awaited_once()
    asyncio.run(run())


def test_restore_changelog_calls_put_bytes_for_truthy_value():
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"k1", value=serialize(State.wrap({"cursor": 123})))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        assert count == 1
        put_bytes.assert_awaited_once_with("k1", serialize(State.wrap({"cursor": 123})))
        delete.assert_not_called()
    asyncio.run(run())


def test_restore_changelog_calls_delete_on_kafka_tombstone():
    """Empty bytes value = Kafka compaction tombstone."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"gone", value=b"")
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        assert count == 1
        put_bytes.assert_not_called()
        delete.assert_awaited_once_with("gone")
    asyncio.run(run())


def test_restore_changelog_calls_delete_on_empty_state():
    """`{}` JSON = state-store tombstone — caught at the bytes layer without deserialize."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"empty", value=serialize(State({})))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        assert count == 1
        put_bytes.assert_not_called()
        delete.assert_awaited_once_with("empty")
    asyncio.run(run())


def test_restore_changelog_handles_none_key():
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=None, value=serialize(State.wrap({"v": 1})))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, AsyncMock())

        assert count == 1
        put_bytes.assert_awaited_once_with("", serialize(State.wrap({"v": 1})))
    asyncio.run(run())


def test_restore_changelog_passes_each_record_through_put_bytes():
    """Wire bytes are passed through verbatim — per-key dedup is the storage layer's
    responsibility (RocksDB overwrites on the same key)."""
    async def run():
        tp0 = aiokafka.TopicPartition("cl", 0)
        tp1 = aiokafka.TopicPartition("cl", 1)
        batch1 = {
            tp0: [
                _make_record(key=b"a", value=serialize(State.wrap({"n": 1})), partition=0, offset=0),
                _make_record(key=b"b", value=serialize(State.wrap({"n": 2})), partition=0, offset=1),
            ],
            tp1: [
                _make_record(key=b"c", value=serialize(State.wrap({"n": 3})), partition=1, offset=0),
            ],
        }
        batch2 = {
            tp0: [
                _make_record(key=b"a", value=b"", partition=0, offset=2),  # tombstone for "a"
            ],
        }
        consumer = _make_restore_consumer(batches=[batch1, batch2], partitions=(0, 1))
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        # All four records processed: 3 puts + 1 tombstone delete.
        assert count == 4
        assert put_bytes.await_count == 3
        delete.assert_awaited_once_with("a")
    asyncio.run(run())


def test_restore_changelog_passes_legacy_pickle_bytes_through_unchanged():
    """Legacy pickle bytes flow through `put_bytes` like any other wire bytes —
    deserialization is deferred to the first `get()` for that key, which uses
    the pickle fallback. TODO(legacy-pickle-state): remove once all changelog
    topics have rolled over to JSON."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        legacy_bytes = pickle.dumps(State.wrap({"cursor": 123}))
        record = _make_record(key=b"k1", value=legacy_bytes)
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        assert count == 1
        put_bytes.assert_awaited_once_with("k1", legacy_bytes)
        delete.assert_not_called()
    asyncio.run(run())
