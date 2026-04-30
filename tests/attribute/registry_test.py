"""Tests for the type-keyed codec registry."""
from datetime import datetime, timezone
from typing import NewType

import pytest

from fretworx.attribute import Dict, OptionalAttribute, RequiredAttribute
from fretworx.attribute.registry import (
    CodecError,
    decoder,
    encoder,
    lookup_decoder,
    lookup_encoder,
)


# --- registration ---


def test_decoder_registers_and_is_callable():
    Token = NewType("Token", str)  # placeholder unique type
    class _T1: ...
    @decoder(_T1)
    def _decode(x):
        return _T1()
    assert lookup_decoder(_T1) is _decode
    assert isinstance(_decode("anything"), _T1)


def test_encoder_registers_and_is_callable():
    class _T2: ...
    @encoder(_T2)
    def _encode(x):
        return "encoded"
    assert lookup_encoder(_T2) is _encode
    assert _encode(_T2()) == "encoded"


# --- duplicate registration ---


def test_decoder_duplicate_raises():
    class _T3: ...
    decoder(_T3)(lambda x: x)
    with pytest.raises(CodecError, match="already registered"):
        decoder(_T3)(lambda x: x)


def test_encoder_duplicate_raises():
    class _T4: ...
    encoder(_T4)(lambda x: x)
    with pytest.raises(CodecError, match="already registered"):
        encoder(_T4)(lambda x: x)


# --- missing registration ---


def test_decoder_lookup_missing_raises():
    class _T5: ...
    with pytest.raises(CodecError, match="no decoder registered"):
        lookup_decoder(_T5)


def test_encoder_lookup_missing_raises():
    class _T6: ...
    with pytest.raises(CodecError, match="no encoder registered"):
        lookup_encoder(_T6)


# --- decoder/encoder independence ---


def test_decoder_encoder_independent():
    class _T7: ...
    decoder(_T7)(lambda x: x)
    # encoder for _T7 still missing
    with pytest.raises(CodecError, match="no encoder registered"):
        lookup_encoder(_T7)


# --- built-in codecs (smoke) ---


def test_str_codec_validates():
    attr = RequiredAttribute[str]("name")
    assert attr.decode("hello") == "hello"
    assert attr.encode("world") == "world"
    with pytest.raises(TypeError):
        attr.decode(42)


def test_int_codec_validates():
    attr = RequiredAttribute[int]("count")
    assert attr.decode(42) == 42
    with pytest.raises(TypeError):
        attr.decode("42")


def test_set_codec_round_trips_via_sorted_list():
    attr = OptionalAttribute[set]("hashes")
    encoded = attr.encode({"b", "a", "c"})
    assert encoded == ["a", "b", "c"]   # sorted, deterministic
    decoded = attr.decode(["a", "b", "c"])
    assert decoded == {"a", "b", "c"}
    assert isinstance(decoded, set)


def test_tuple_codec_round_trips_via_list():
    attr = OptionalAttribute[tuple]("ids")
    assert attr.encode((1, 2, 3)) == [1, 2, 3]
    assert attr.decode([1, 2, 3]) == (1, 2, 3)
    assert isinstance(attr.decode([1, 2, 3]), tuple)


def test_datetime_codec_round_trips_via_iso():
    attr = RequiredAttribute[datetime]("ts")
    dt = datetime(2024, 6, 15, 14, 30, 0, tzinfo=timezone.utc)
    encoded = attr.encode(dt)
    assert encoded == "2024-06-15T14:30:00.000Z"
    decoded = attr.decode(encoded)
    assert decoded == dt


# --- recursive container walker ---


def test_dict_encoder_walks_nested_datetime():
    encoded = lookup_encoder(dict)({
        "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "name": "alice",
    })
    assert encoded == {"ts": "2024-01-01T00:00:00.000Z", "name": "alice"}


def test_dict_encoder_walks_nested_set():
    encoded = lookup_encoder(dict)({"hashes": {"b", "a"}})
    assert encoded == {"hashes": ["a", "b"]}


def test_dict_encoder_walks_nested_dict_of_dicts():
    encoded = lookup_encoder(dict)({
        "op1": {"last_time": datetime(2024, 1, 1, tzinfo=timezone.utc), "ids": (1, 2)},
        "op2": {"last_time": datetime(2024, 2, 1, tzinfo=timezone.utc), "ids": (3,)},
    })
    assert encoded == {
        "op1": {"last_time": "2024-01-01T00:00:00.000Z", "ids": [1, 2]},
        "op2": {"last_time": "2024-02-01T00:00:00.000Z", "ids": [3]},
    }


def test_list_encoder_walks_nested_datetimes():
    encoded = lookup_encoder(list)([
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 2, 1, tzinfo=timezone.utc),
    ])
    assert encoded == ["2024-01-01T00:00:00.000Z", "2024-02-01T00:00:00.000Z"]


def test_dict_encoder_raises_on_unknown_leaf_type():
    class _Mystery: ...
    with pytest.raises(CodecError, match="no encoder registered for .*_Mystery"):
        lookup_encoder(dict)({"x": _Mystery()})


def test_dict_decoder_is_identity():
    raw = {"a": 1, "b": "two"}
    assert lookup_decoder(dict)(raw) is raw


# --- Dict subclass auto-registration ---


def test_dict_subclass_auto_registers():
    class _AutoReg(Dict):
        pass

    # decoder wraps a raw dict in the subclass
    decoded = lookup_decoder(_AutoReg)({"x": 1})
    assert isinstance(decoded, _AutoReg)
    assert decoded.raw == {"x": 1}

    # encoder returns a shallow copy of `.raw`. The constructor /
    # `__setitem__` invariants guarantee `.raw` is already JSON-native, so
    # re-walking it would be redundant — the encoder trusts the invariant.
    nested = [1, 2, 3]
    instance = _AutoReg({"x": 1, "nested": nested})
    encoded = lookup_encoder(_AutoReg)(instance)
    assert encoded == {"x": 1, "nested": [1, 2, 3]}
    assert encoded is not instance.raw  # top-level isolated
    assert encoded["nested"] is instance.raw["nested"]  # nested shared
