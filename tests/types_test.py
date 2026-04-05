"""Tests for fretworx core types."""
from datetime import datetime, timezone

from fretworx.types import IncomingMessage, Message


def test_message_is_frozen():
    msg = Message(key="k", topic="t", value={"a": 1})
    assert msg.key == "k"
    assert msg.topic == "t"
    assert msg.value == {"a": 1}
    assert msg.timestamp is None


def test_message_with_timestamp():
    ts = datetime(2024, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
    msg = Message(key="k", topic="t", value={}, timestamp=ts)
    assert msg.timestamp == ts


def test_incoming_message():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    msg = IncomingMessage(
        key="k",
        offset=42,
        partition=0,
        timestamp=ts,
        topic="t",
        value={"data": 1},
    )
    assert msg.key == "k"
    assert msg.offset == 42
    assert msg.value == {"data": 1}
