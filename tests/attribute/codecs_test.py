from datetime import datetime, time, timedelta, timezone

from fretworx.attribute import ANY, DATETIME


def test_datetime_to_iso_utc():
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert DATETIME.encode(dt) == "2024-01-01T00:00:00.000Z"


def test_datetime_to_iso_preserves_offset():
    dt = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    assert DATETIME.encode(dt) == "2024-01-01T02:00:00.000+02:00"


def test_datetime_from_iso_round_trip():
    original = datetime(2024, 6, 15, 14, 30, 45, 123000, tzinfo=timezone.utc)
    encoded = DATETIME.encode(original)
    assert DATETIME.decode(encoded) == original


def test_datetime_from_iso_with_offset():
    decoded = DATETIME.decode("2024-01-01T02:00:00+02:00")
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def test_any_encodes_time_as_iso_string():
    """Regression: BA-3033 — pandas reads time-only Excel cells as datetime.time,
    and the ANY codec used to raise TypeError on those, blocking every
    Promoterstunden import. Encoded form is JSON-native ISO 8601."""
    assert ANY.encode(time(13, 30)) == "13:30:00"
    assert ANY.encode(time(0, 0, 0)) == "00:00:00"
    assert ANY.encode(time(23, 59, 59, 123456)) == "23:59:59.123456"
