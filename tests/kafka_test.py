"""Tests for Flechtwerk Kafka utilities."""
import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiokafka
import pytest
from hypothesis import given, strategies as st

from flechtwerk.attribute import Record
from flechtwerk.kafka import (
    datetime_to_millis,
    decode_record,
    encode_json,
    millis_to_datetime,
    parse_message,
    restore_changelog,
)
from flechtwerk.state import serialize
from flechtwerk.types import Config, Event, InvalidMessageError, State


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


def test_encode_json_string_passthrough():
    assert encode_json("already a string") == b"already a string"


def test_encode_json_bytes_passthrough():
    assert encode_json(b"\x00pre-encoded\xff") == b"\x00pre-encoded\xff"


def test_encode_json_record():
    result = encode_json(Event.wrap({"b": 2, "a": 1}))
    assert result == b'{"a":1,"b":2}'  # sorted keys, compact


def test_encode_json_unicode():
    result = encode_json(Event.wrap({"name": "Müller"}))
    assert "Müller".encode("utf-8") in result  # ensure_ascii=False


def test_encode_json_rejects_nan():
    with pytest.raises(ValueError):
        encode_json(Event.wrap({"x": float("nan")}))


def test_encode_json_nested():
    result = encode_json(Event.wrap({"outer": {"inner": [1, 2, 3]}}))
    assert result == b'{"outer":{"inner":[1,2,3]}}'


def test_encode_json_rejects_raw_dict():
    """Raw dicts are rejected — Event.wrap(d) produces identical wire bytes plus validation."""
    with pytest.raises(TypeError, match="got dict"):
        encode_json({"a": 1})


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
# Non-string JSON values — encode_json rejects them all when raw (str is UTF-8
# passthrough; dicts must arrive wrapped in a Record).
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
# Dicts only — parse_message rejects non-dict JSON payloads by design.
_json_dicts = st.dictionaries(
    st.text(),
    st.recursive(_json_scalars, lambda c: st.one_of(
        st.lists(c, max_size=5),
        st.dictionaries(st.text(), c, max_size=5),
    ), max_leaves=10),
    max_size=5,
)


@given(_json_non_strings)
def test_encode_json_rejects_unwrapped_json_values(value):
    """Every raw JSON-native value is rejected — dicts arrive as Records, scalars/arrays as bytes."""
    with pytest.raises(TypeError):
        encode_json(value)


@given(_json_dicts)
def test_encode_json_round_trips_records(value):
    """Record payloads round-trip through encode_json → json.loads."""
    assert json.loads(encode_json(Event.wrap(value)).decode("utf-8")) == value


@given(_json_dicts)
def test_encode_json_dict_keys_are_sorted(value):
    """Dict keys at every nesting level must appear in sorted order."""
    encoded = encode_json(Event.wrap(value)).decode("utf-8")
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


def test_decode_record_returns_the_callers_type():
    """The wire carries no type, so the CALLER names it — no copy to relabel."""
    for cls in (Config, Event, Record, State):
        decoded = decode_record(b'{"a":1}', cls)
        assert type(decoded) is cls
        assert decoded.raw == {"a": 1}


def test_decode_record_empty_value_is_an_empty_record_of_that_type():
    for empty in (b"", None, ""):
        assert decode_record(empty, Config) == Config({})


def _raw(key=b"k", value=b"{}", offset=0, partition=0, timestamp=None, topic="t"):
    """A minimal stand-in for what aiokafka's getmany() yields."""
    return SimpleNamespace(
        key=key, value=value, offset=offset, partition=partition,
        timestamp=timestamp, topic=topic,
    )


@given(
    key=st.one_of(st.none(), st.text().map(str.encode), st.text()),
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

    Non-dict payloads are rejected by parse_message, so the round-trip
    contract only holds for dicts.
    """
    raw = _raw(
        key=key, value=encode_json(Event.wrap(value)), offset=offset,
        partition=partition, timestamp=timestamp, topic="some-topic",
    )
    msg = parse_message(raw, raising)
    assert msg.value.raw == value
    assert msg.offset == offset
    assert msg.partition == partition
    assert isinstance(msg.key, str)


@given(st.binary(max_size=200))
def test_parse_message_either_decodes_or_raises_invalid_message(data):
    """Arbitrary value bytes decode to an Event or surface as InvalidMessageError.

    The default policy raises, and the ONLY exception type an application ever
    has to catch is `InvalidMessageError` — never the raw UnicodeDecodeError /
    JSONDecodeError underneath, which stays available as ``__cause__``.
    """
    try:
        msg = parse_message(_raw(value=data), raising)
    except InvalidMessageError as e:
        assert e.part == "value"
        assert e.value == data
        return
    assert isinstance(msg.value, Event)


def test_parse_message_raises_on_non_dict_json():
    """Valid JSON that decodes to a non-dict (a scalar or array) is not an event."""
    raw = _raw(key=b"k", value=b"42", offset=7, partition=3, topic="t")
    with pytest.raises(InvalidMessageError) as excinfo:
        parse_message(raw, raising)
    error = excinfo.value
    assert error.part == "value"
    assert (error.topic, error.partition, error.offset) == ("t", 3, 7)
    assert error.key == b"k"
    assert error.value == b"42"
    assert isinstance(error.__cause__, ValueError)
    assert "int" in str(error.__cause__)
    # Forensically complete on its own: the traceback is the only announcement.
    assert "value" in str(error) and "t/3/7" in str(error)


def test_parse_message_raises_on_invalid_json():
    raw = _raw(key=b"some-key", value=b"not valid json {", offset=42, partition=1,
               timestamp=1704067200000, topic="my-topic")
    with pytest.raises(InvalidMessageError) as excinfo:
        parse_message(raw, raising)
    assert excinfo.value.part == "value"
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)


def test_parse_message_raises_on_cesu8_surrogate():
    """CESU-8-encoded surrogates must fail the decode, not slip through.

    Pins the strict UTF-8 decode in decode_record: json.loads(bytes) would
    accept b'"\\xed\\xa0\\x80"' via errors="surrogatepass" and return the
    ill-formed str '\\ud800', which crashes encode_json when a stage re-emits
    it — far from the source, on every redelivery.
    """
    with pytest.raises(InvalidMessageError) as excinfo:
        parse_message(_raw(value=b'"\xed\xa0\x80"'), raising)
    assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)


def test_parse_message_raises_on_non_utf8_key():
    """A key is never repaired with errors="replace" — it is state identity."""
    with pytest.raises(InvalidMessageError) as excinfo:
        parse_message(_raw(key=b"\xff\xfe", value=b'{"a":1}'), raising)
    error = excinfo.value
    assert error.part == "key"
    assert error.key == b"\xff\xfe"
    assert isinstance(error.__cause__, UnicodeDecodeError)


def test_parse_message_decodes_key_before_value():
    """Both parts broken → exactly one handler call, for the key."""
    handler, seen = recording(skipping)
    assert parse_message(_raw(key=b"\xff", value=b"not json {"), handler) is None
    assert [e.part for e in seen] == ["key"]


def test_parse_message_rejects_key_substitution():
    """Identity must not be synthesized for a record whose identity is unreadable."""
    with pytest.raises(TypeError, match="state identity") as excinfo:
        parse_message(_raw(key=b"\xff"), substituting(Event.wrap({"key": "invented"})))
    assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)


def test_parse_message_skip_returns_none():
    assert parse_message(_raw(value=b"not json {"), skipping) is None


def test_parse_message_substituted_value_becomes_the_event():
    """The call site assigns the semantic type: a substitute arrives as an Event."""
    msg = parse_message(
        _raw(key=b"k", value=b"\xff\xfe", offset=9),
        substituting(Event.wrap({"recovered": True})),
    )
    assert msg.key == "k"
    assert msg.offset == 9
    assert msg.value == Event.wrap({"recovered": True})
    assert isinstance(msg.value, Event)


def test_parse_message_empty_substitute_is_a_substitution_not_a_skip():
    """Only ``None`` means skip. A falsy Record is a deliberate empty value.

    Elsewhere in the framework a falsy `State` tombstones its key, so the skip
    check here must stay ``is None`` — widening it to ``if not substitute``
    would silently turn this substitution into a dropped record.
    """
    msg = parse_message(_raw(value=b"not json {"), substituting(Event({})))
    assert msg is not None
    assert msg.value == Event({})


def test_parse_message_rejects_a_non_record_substitute():
    """A raw dict would die opaquely inside Record.__init__ — teach at the decision site."""
    with pytest.raises(TypeError, match=r"Event\.wrap"):
        parse_message(_raw(value=b"not json {"), substituting({"a": 1}))


def test_parse_message_empty_value_needs_no_handler():
    """Empty is not an error: {} means "empty value", never "garbage"."""
    handler, seen = recording(raising)
    for empty in (b"", None):
        msg = parse_message(_raw(key=None, value=empty), handler)
        assert msg.key == ""
        assert msg.value == Event({})
    assert seen == []


# --- restore_changelog ---


def _make_record(key, value, partition=0, offset=0):
    """Construct a minimal record that matches what aiokafka yields from getmany()."""
    return SimpleNamespace(key=key, value=value, partition=partition, offset=offset)


def _make_restore_consumer(batches, partitions=(0,)):
    """Build a MagicMock consumer that restore_changelog can drive.

    End offsets are derived from the supplied batches (max offset + 1 per
    partition) and the fetch position advances as batches are consumed —
    matching the position-vs-end-offset end-detection of restore_changelog.

    Args:
        batches: Sequence of dicts {tp: [record, ...]} — one returned per getmany call.
        partitions: Partition numbers to report from partitions_for_topic().
                    Pass an empty set/None to simulate a missing topic.
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
    consumer.partitions_for_topic = MagicMock(
        return_value=set(partitions) if partitions else partitions,
    )
    consumer.assign = MagicMock()
    consumer.seek_to_beginning = AsyncMock()
    consumer.end_offsets = end_offsets_fn
    consumer.getmany = getmany
    consumer.position = position
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


def test_restore_changelog_restricted_to_explicit_partitions():
    """An explicit partition subset skips discovery and assigns only those partitions."""
    async def run():
        tp1 = aiokafka.TopicPartition("cl", 1)
        record = _make_record(key=b"k", value=serialize(State.wrap({"n": 1})), partition=1)
        consumer = _make_restore_consumer(batches=[{tp1: [record]}])
        put_bytes = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, AsyncMock(), partitions={1})

        assert count == 1
        consumer._client.set_topics.assert_not_awaited()
        consumer.partitions_for_topic.assert_not_called()
        (assigned_tps,), _ = consumer.assign.call_args
        assert list(assigned_tps) == [tp1]
    asyncio.run(run())


def test_restore_changelog_survives_empty_polls_until_end_offset():
    """Empty getmany() results don't terminate the restore — only the fetch
    position reaching the end offset captured at entry does. The previous
    empty-poll heuristic silently truncated restores on any broker stall."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"k", value=serialize(State.wrap({"n": 1})))
        consumer = _make_restore_consumer(batches=[{}, {tp: [record]}])
        put_bytes = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, AsyncMock())

        assert count == 1
        put_bytes.assert_awaited_once()
    asyncio.run(run())


def test_restore_changelog_crashes_on_an_undecodable_key():
    """The changelog is framework-owned — a broken key there is corruption.

    No `on_invalid_message` mediation: every changelog key is written from a
    `str`, so an undecodable one means foreign writes or corruption, which is
    an unrecoverable data error (crash, then reset the affected state) and not
    an application policy question.
    """
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"\xff\xfe", value=serialize(State.wrap({"n": 1})))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])

        with pytest.raises(UnicodeDecodeError):
            await restore_changelog(consumer, "cl", AsyncMock(), AsyncMock())
    asyncio.run(run())


def test_restore_changelog_passes_raw_bytes_through_uninterpreted():
    """Restore never deserializes — bytes flow through `put_bytes` verbatim
    even when they are not valid JSON; decoding is deferred to the first
    `get()` for that key."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        opaque_bytes = b"\x80\x04\x95not-json"
        record = _make_record(key=b"k1", value=opaque_bytes)
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put_bytes = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put_bytes, delete)

        assert count == 1
        put_bytes.assert_awaited_once_with("k1", opaque_bytes)
        delete.assert_not_called()
    asyncio.run(run())
