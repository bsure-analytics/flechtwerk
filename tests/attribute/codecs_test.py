from datetime import datetime, timedelta, timezone

from fretworx.attribute.codecs import datetime_from_iso, datetime_to_iso


def test_datetime_to_iso_utc():
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert datetime_to_iso(dt) == "2024-01-01T00:00:00.000Z"


def test_datetime_to_iso_normalizes_to_utc():
    dt = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    assert datetime_to_iso(dt) == "2024-01-01T00:00:00.000Z"


def test_datetime_from_iso_round_trip():
    original = datetime(2024, 6, 15, 14, 30, 45, 123000, tzinfo=timezone.utc)
    encoded = datetime_to_iso(original)
    decoded = datetime_from_iso(encoded)
    assert decoded == original


def test_datetime_from_iso_with_offset():
    decoded = datetime_from_iso("2024-01-01T02:00:00+02:00")
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=2)))
