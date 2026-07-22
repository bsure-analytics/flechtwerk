"""Tests for Flechtwerk core types."""
from datetime import datetime, timezone

import pytest

from flechtwerk.types import Config, Event, IncomingMessage, Message, State


def test_message_is_frozen():
    msg = Message(key="k", topic="t", value=Event.wrap({"a": 1}))
    assert msg.key == "k"
    assert msg.topic == "t"
    assert msg.value == Event.wrap({"a": 1})
    assert msg.timestamp is None


def test_message_with_timestamp():
    ts = datetime(2024, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
    msg = Message(key="k", topic="t", value=Event(), timestamp=ts)
    assert msg.timestamp == ts


def test_message_accepts_every_payload_shape():
    """Each Payload member is accepted for key and value alike."""
    for payload in (b"pre-encoded", "plain text", Event.wrap({"b": 2})):
        msg = Message(key=payload, topic="t", value=payload)
        assert msg.key is payload
        assert msg.value is payload


def test_message_rejects_state_with_yield_guidance():
    """A State inside a Message would be emitted, not persisted — the error teaches the fix."""
    with pytest.raises(TypeError, match="emitted, not persisted"):
        Message(key="k", topic="t", value=State.wrap({"cursor": 1}))


def test_message_rejects_config_with_wrap_guidance():
    """On the wire a config travels as data — the error teaches the Event(config) handoff."""
    with pytest.raises(TypeError, match="travels as data"):
        Message(key="k", topic="t", value=Config.wrap({"tenant": "a"}))


def test_message_rejects_raw_dict_with_wrap_guidance():
    with pytest.raises(TypeError, match=r"Event\.wrap"):
        Message(key="k", topic="t", value={"a": 1})


def test_message_key_is_validated_like_value():
    with pytest.raises(TypeError, match=r"Message\.key"):
        Message(key={"composite": 1}, topic="t", value=Event())


def test_message_rejects_other_shapes():
    """Non-Payload shapes get the generic teaching error (encode to bytes yourself)."""
    with pytest.raises(TypeError, match=r"bytes \| str \| Event"):
        Message(key="k", topic="t", value=42)


def test_incoming_message():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    msg = IncomingMessage(
        key="k",
        offset=42,
        partition=0,
        timestamp=ts,
        topic="t",
        value=Event.wrap({"data": 1}),
    )
    assert msg.key == "k"
    assert msg.offset == 42
    assert msg.value == Event.wrap({"data": 1})
