"""Tests for fretworx Kafka utilities."""
import asyncio
import logging
import pickle
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import aiokafka
import pytest

from fretworx.kafka import (
    datetime_to_millis,
    encode_json,
    millis_to_datetime,
    parse_message,
    restore_changelog,
)
from fretworx.types import Event


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
    assert any("Invalid JSON" in rec.message for rec in caplog.records)


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


def test_restore_changelog_calls_put_for_truthy_value():
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"k1", value=pickle.dumps({"cursor": 123}))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put, delete)

        assert count == 1
        put.assert_awaited_once_with("k1", {"cursor": 123})
        delete.assert_not_called()
    asyncio.run(run())


def test_restore_changelog_calls_delete_on_kafka_tombstone():
    """Empty bytes value = Kafka compaction tombstone."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"gone", value=b"")
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put, delete)

        assert count == 1
        put.assert_not_called()
        delete.assert_awaited_once_with("gone")
    asyncio.run(run())


def test_restore_changelog_calls_delete_on_pickled_empty_dict():
    """Non-empty bytes that unpickle to a falsy dict — state-store tombstone convention."""
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=b"empty", value=pickle.dumps({}))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put, delete)

        assert count == 1
        put.assert_not_called()
        delete.assert_awaited_once_with("empty")
    asyncio.run(run())


def test_restore_changelog_handles_none_key():
    async def run():
        tp = aiokafka.TopicPartition("cl", 0)
        record = _make_record(key=None, value=pickle.dumps({"v": 1}))
        consumer = _make_restore_consumer(batches=[{tp: [record]}])
        put = AsyncMock()

        count = await restore_changelog(consumer, "cl", put, AsyncMock())

        assert count == 1
        put.assert_awaited_once_with("", {"v": 1})
    asyncio.run(run())


def test_restore_changelog_counts_across_multiple_batches_and_partitions():
    async def run():
        tp0 = aiokafka.TopicPartition("cl", 0)
        tp1 = aiokafka.TopicPartition("cl", 1)
        batch1 = {
            tp0: [
                _make_record(key=b"a", value=pickle.dumps({"n": 1}), partition=0, offset=0),
                _make_record(key=b"b", value=pickle.dumps({"n": 2}), partition=0, offset=1),
            ],
            tp1: [
                _make_record(key=b"c", value=pickle.dumps({"n": 3}), partition=1, offset=0),
            ],
        }
        batch2 = {
            tp0: [
                _make_record(key=b"a", value=b"", partition=0, offset=2),  # tombstone for "a"
            ],
        }
        consumer = _make_restore_consumer(batches=[batch1, batch2], partitions=(0, 1))
        put = AsyncMock()
        delete = AsyncMock()

        count = await restore_changelog(consumer, "cl", put, delete)

        assert count == 4
        assert put.await_count == 3
        delete.assert_awaited_once_with("a")
    asyncio.run(run())
