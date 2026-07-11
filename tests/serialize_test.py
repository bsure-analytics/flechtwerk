"""Tests for `state.serialize` / `state.deserialize`."""
import json
from datetime import datetime, timezone
from typing import Final

import pytest

from flechtwerk.attribute import Attribute, DATETIME, SET, STR, TUPLE
from flechtwerk.state import deserialize, serialize
from flechtwerk.types import State


HASHES: Final = Attribute("hashes", SET(STR), optional=True)
LAST_TIME: Final = Attribute("last_time", DATETIME)
RESULT_IDS: Final = Attribute("result_ids", TUPLE(STR))


# --- JSON round-trip ---


def test_serialize_produces_compact_sorted_json():
    state = State()
    state.raw = {"b": 2, "a": 1}
    assert serialize(state) == b'{"a":1,"b":2}'


def test_json_round_trip_set():
    state = State()
    state[HASHES] = {"abc", "def", "ghi"}
    bytes_ = serialize(state)
    assert bytes_ == b'{"hashes":["abc","def","ghi"]}'   # sorted, deterministic
    restored = deserialize(bytes_)
    assert restored[HASHES] == {"abc", "def", "ghi"}


def test_json_round_trip_datetime():
    state = State()
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    state[LAST_TIME] = dt
    bytes_ = serialize(state)
    assert bytes_ == b'{"last_time":"2024-06-15T14:30:00Z"}'
    restored = deserialize(bytes_)
    assert restored[LAST_TIME] == dt


def test_json_round_trip_tuple():
    state = State()
    state[RESULT_IDS] = ("a", "b", "c")
    bytes_ = serialize(state)
    assert bytes_ == b'{"result_ids":["a","b","c"]}'
    restored = deserialize(bytes_)
    assert restored[RESULT_IDS] == ("a", "b", "c")


def test_serialize_set_diff_stability():
    """Two consecutive serializations of the same set produce identical bytes."""
    s1 = State()
    s1[HASHES] = {"x", "y", "z"}
    s2 = State()
    s2[HASHES] = {"z", "y", "x"}
    assert serialize(s1) == serialize(s2)


# --- no fallback for undecodable bytes ---


def test_deserialize_rejects_non_json_bytes():
    """JSON-only — there is no pickle fallback anymore. Undecodable bytes
    (e.g. a pre-JSON-migration pickle record) are an unrecoverable data
    error: crash, then reset the affected state."""
    with pytest.raises(UnicodeDecodeError):
        deserialize(b"\x80\x04\x95binary-pickle-garbage")
    with pytest.raises(json.JSONDecodeError):
        deserialize(b"not json")
