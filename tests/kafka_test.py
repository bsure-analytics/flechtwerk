"""Tests for fretworx Kafka utilities."""
from datetime import datetime, timezone

from fretworx.kafka import datetime_to_millis, encode_json, millis_to_datetime


def test_encode_json_string_passthrough():
    assert encode_json("already a string") == "already a string"


def test_encode_json_dict():
    result = encode_json({"b": 2, "a": 1})
    assert result == '{"a":1,"b":2}'  # sorted keys, compact


def test_encode_json_unicode():
    result = encode_json({"name": "Müller"})
    assert "Müller" in result  # ensure_ascii=False


def test_encode_json_rejects_nan():
    import pytest
    with pytest.raises(ValueError):
        encode_json({"x": float("nan")})


def test_encode_json_nested():
    result = encode_json({"outer": {"inner": [1, 2, 3]}})
    assert result == '{"outer":{"inner":[1,2,3]}}'


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
