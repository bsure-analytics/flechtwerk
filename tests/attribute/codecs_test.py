from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from fretworx.attribute import ANY, DATE, DATETIME, TIME


def test_datetime_from_iso_utc_round_trip():
    original = "2024-06-15T14:30:45.123Z"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 6, 15, 14, 30, 45, 123000, tzinfo=timezone.utc)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_from_iso_whole_second_round_trip():
    """Whole seconds still render `.000` — millisecond precision is fixed, not elided."""
    original = "2024-01-01T00:00:00.000Z"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, tzinfo=timezone.utc)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_from_iso_with_offset_round_trip():
    original = "2024-01-01T02:00:00.123+02:00"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, 123000, tzinfo=timezone(timedelta(hours=2)))
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_from_iso_without_offset_round_trip():
    original = "2024-01-01T02:00:00.123"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, 123000)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_zoneinfo_utc_encodes_z():
    """`Z` keys off the zone being UTC, not the concrete tzinfo class —
    `ZoneInfo("UTC")` qualifies just like `timezone.utc`."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC")))
    assert encoded == "2024-01-01T00:00:00.000Z"


def test_datetime_zero_offset_zone_keeps_offset():
    """Europe/London in winter has a zero offset but is not UTC (it names
    itself GMT) — `Z` asserts UTC, so `+00:00` survives verbatim."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=ZoneInfo("Europe/London")))
    assert encoded == "2024-01-01T00:00:00.000+00:00"


def test_datetime_named_fixed_zero_offset_keeps_offset():
    """Same for a fixed zero offset under a non-UTC name — the discriminator
    is `tzname()`, not the offset."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=timezone(timedelta(0), "GMT")))
    assert encoded == "2024-01-01T00:00:00.000+00:00"


def test_datetime_from_space_separated_with_offset():
    decoded = DATETIME.decode("2024-01-01 02:00:00+02:00")
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=2)))


def test_datetime_from_space_separated_without_offset():
    decoded = DATETIME.decode("2024-01-01 02:00:00")
    assert decoded == datetime(2024, 1, 1, 2, 0, 0)


def test_date_from_iso_round_trip():
    original = "2026-03-15"
    decoded = DATE.decode(original)
    assert decoded == date(2026, 3, 15)
    encoded = DATE.encode(decoded)
    assert encoded == original


def test_any_encodes_date_as_iso_string():
    """ANY routes datetime.date through DATE.encode."""
    assert ANY.encode(date(2026, 3, 15)) == "2026-03-15"


def test_any_dispatches_datetime_before_date():
    """datetime ⊂ date — ANY must route datetime through DATETIME, not DATE."""
    dt = datetime(2026, 3, 15, 10, 30, tzinfo=timezone.utc)
    assert ANY.encode(dt) == "2026-03-15T10:30:00.000Z"


def test_time_from_iso_round_trip():
    original = "13:30:00"
    decoded = TIME.decode(original)
    assert decoded == time(13, 30)
    encoded = TIME.encode(decoded)
    assert encoded == original


def test_time_from_iso_midnight_round_trip():
    original = "00:00:00"
    decoded = TIME.decode(original)
    assert decoded == time(0, 0, 0)
    encoded = TIME.encode(decoded)
    assert encoded == original


def test_time_from_iso_with_offset_round_trip():
    original = "13:30:00+02:00"
    decoded = TIME.decode(original)
    assert decoded == time(13, 30, tzinfo=timezone(timedelta(hours=2)))
    encoded = TIME.encode(decoded)
    assert encoded == original


def test_time_from_iso_utc_round_trip():
    """Unlike DATETIME, TIME does not map `+00:00` to `Z` — the offset survives verbatim."""
    original = "13:30:00+00:00"
    decoded = TIME.decode(original)
    assert decoded == time(13, 30, tzinfo=timezone.utc)
    encoded = TIME.encode(decoded)
    assert encoded == original


def test_time_from_iso_with_microseconds_round_trip():
    """Unlike DATETIME's fixed milliseconds, TIME keeps full microseconds and
    elides the fraction entirely when zero (`isoformat` timespec defaults)."""
    original = "23:59:59.123456"
    decoded = TIME.decode(original)
    assert decoded == time(23, 59, 59, 123456)
    encoded = TIME.encode(decoded)
    assert encoded == original


def test_any_encodes_time_as_iso_string():
    """ANY routes datetime.time through TIME.encode."""
    assert ANY.encode(time(13, 30)) == "13:30:00"
    assert ANY.encode(time(0, 0, 0)) == "00:00:00"
    assert ANY.encode(time(23, 59, 59, 123456)) == "23:59:59.123456"


def test_time_rejects_timedelta():
    """TIME is strict — `datetime.timedelta` is a duration, not a time of day.
    Callers that need wall-clock semantics must convert at the application
    boundary (see ds/excel_import/parsers/promoter_time.py)."""
    import pytest

    with pytest.raises((AttributeError, TypeError)):
        TIME.encode(timedelta(hours=13, minutes=30))


def test_any_rejects_timedelta():
    """ANY similarly does not magically dispatch timedelta — the framework
    stays strict so a domain-specific shape doesn't silently get treated as
    time-of-day. Callers convert at their own boundary."""
    import pytest

    with pytest.raises(TypeError, match="no encoder for timedelta"):
        ANY.encode(timedelta(hours=13, minutes=30))
