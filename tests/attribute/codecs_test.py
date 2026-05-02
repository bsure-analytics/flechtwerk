from datetime import datetime, timedelta, timezone

from fretworx.attribute import DATETIME


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
