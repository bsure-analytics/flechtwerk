"""Tests for `state.serialize` / `state.deserialize`."""
import pickle
from datetime import datetime, timezone
from typing import Final

from fretworx.attribute import DATETIME, OptionalAttribute, RequiredAttribute, SET, STR, TUPLE
from fretworx.state import deserialize, serialize
from fretworx.types import State


HASHES: Final = OptionalAttribute("hashes", SET(STR))
LAST_TIME: Final = RequiredAttribute("last_time", DATETIME)
RESULT_IDS: Final = RequiredAttribute("result_ids", TUPLE(STR))


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
    assert bytes_ == b'{"last_time":"2024-06-15T14:30:00.000Z"}'
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


# --- legacy pickle fallback ---


def test_deserialize_legacy_pickle_with_native_datetime():
    """Pickle bytes from before the JSON era — native datetime in raw —
    get walked through the encoder so subsequent attribute reads succeed."""
    legacy_state = State()
    legacy_state.raw = {"last_time": datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)}
    bytes_ = pickle.dumps(legacy_state)
    restored = deserialize(bytes_)
    assert restored.raw == {"last_time": "2024-06-15T14:30:00.000Z"}
    assert restored[LAST_TIME] == datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)


def test_deserialize_legacy_pickle_with_native_set():
    legacy_state = State()
    legacy_state.raw = {"hashes": {"a", "b", "c"}}
    bytes_ = pickle.dumps(legacy_state)
    restored = deserialize(bytes_)
    assert restored.raw == {"hashes": ["a", "b", "c"]}
    assert restored[HASHES] == {"a", "b", "c"}


def test_deserialize_legacy_pickle_with_nested_datetime_and_tuple():
    """Emotivo's case: dict-of-dicts containing datetime + tuple. The walker
    recurses into the outer dict, hits inner values, applies leaf encoders."""
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    legacy_state = State()
    legacy_state.raw = {
        "operation_state": {
            "op1": {"last_release_time": dt, "result_ids": (1, 2)},
            "op2": {"last_release_time": dt, "result_ids": (3,)},
        }
    }
    bytes_ = pickle.dumps(legacy_state)
    restored = deserialize(bytes_)
    assert restored.raw == {
        "operation_state": {
            "op1": {"last_release_time": "2024-06-15T14:30:00.000Z", "result_ids": [1, 2]},
            "op2": {"last_release_time": "2024-06-15T14:30:00.000Z", "result_ids": [3]},
        }
    }


def test_deserialize_distinguishes_json_from_pickle():
    """JSON bytes always decode as JSON; pickle bytes hit the fallback. The
    chain-of-responsibility never tries pickle on JSON or vice versa."""
    json_bytes = serialize(State({"x": 1}))
    pickle_bytes = pickle.dumps(State({"x": 1}))
    assert deserialize(json_bytes).raw == {"x": 1}
    assert deserialize(pickle_bytes).raw == {"x": 1}
