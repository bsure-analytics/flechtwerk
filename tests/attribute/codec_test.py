import ast
import base64
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import flechtwerk.attribute as package
from flechtwerk.attribute import ANY, BYTES, DATE, DATETIME, DICT, LIST, TIME, Attribute, Codec, Record, codec


def test_datetime_from_iso_utc_round_trip():
    original = "2024-06-15T14:30:45.123456Z"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 6, 15, 14, 30, 45, 123456, tzinfo=timezone.utc)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_from_iso_whole_second_round_trip():
    """Whole seconds elide the fraction entirely (`isoformat` defaults)."""
    original = "2024-01-01T00:00:00Z"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, tzinfo=timezone.utc)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_millisecond_input_normalizes_to_microseconds():
    """Legacy 3-digit fractions decode fine but re-encode at full microsecond
    width — the canonical wire form is `isoformat` defaults, not a fixed
    millisecond `timespec`."""
    decoded = DATETIME.decode("2024-06-15T14:30:45.123Z")
    assert decoded == datetime(2024, 6, 15, 14, 30, 45, 123000, tzinfo=timezone.utc)
    assert DATETIME.encode(decoded) == "2024-06-15T14:30:45.123000Z"


def test_datetime_from_iso_with_offset_round_trip():
    original = "2024-01-01T02:00:00.123456+02:00"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, 123456, tzinfo=timezone(timedelta(hours=2)))
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_from_iso_without_offset_round_trip():
    original = "2024-01-01T02:00:00.123456"
    decoded = DATETIME.decode(original)
    assert decoded == datetime(2024, 1, 1, 2, 0, 0, 123456)
    encoded = DATETIME.encode(decoded)
    assert encoded == original


def test_datetime_zoneinfo_utc_encodes_z():
    """`Z` keys off the zone being UTC, not the concrete tzinfo class —
    `ZoneInfo("UTC")` qualifies just like `timezone.utc`."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=ZoneInfo("UTC")))
    assert encoded == "2024-01-01T00:00:00Z"


def test_datetime_zero_offset_zone_keeps_offset():
    """Europe/London in winter has a zero offset but is not UTC (it names
    itself GMT) — `Z` asserts UTC, so `+00:00` survives verbatim."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=ZoneInfo("Europe/London")))
    assert encoded == "2024-01-01T00:00:00+00:00"


def test_datetime_named_fixed_zero_offset_keeps_offset():
    """Same for a fixed zero offset under a non-UTC name — the discriminator
    is `tzname()`, not the offset."""
    encoded = DATETIME.encode(datetime(2024, 1, 1, tzinfo=timezone(timedelta(0), "GMT")))
    assert encoded == "2024-01-01T00:00:00+00:00"


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
    assert ANY.encode(dt) == "2026-03-15T10:30:00Z"


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
    """Like DATETIME, TIME keeps full microseconds and elides the fraction
    entirely when zero (`isoformat` timespec defaults)."""
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
    boundary."""
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


def test_bytes_round_trip():
    original = "SGVsbG8sIEZsZWNodHdlcms="
    decoded = BYTES.decode(original)
    assert decoded == b"Hello, Flechtwerk"
    assert BYTES.encode(decoded) == original


def test_bytes_empty_round_trip():
    """b"" is a legal value, not an absence — the empty string is its wire form."""
    assert BYTES.decode("") == b""
    assert BYTES.encode(b"") == ""


def test_bytes_covers_the_full_octet_range():
    """The whole 0..255 range survives, including the bytes no UTF-8 decoder
    would accept — that is the point of the codec."""
    blob = bytes(range(256))
    assert BYTES.decode(BYTES.encode(blob)) == blob


def test_bytes_encodes_standard_alphabet_with_padding():
    """RFC 4648 §4, not §5: `+` and `/`, and the padding stays. A urlsafe
    variant would be a different codec, not a lenient reading of this one."""
    blob = bytes([0xFB, 0xEF, 0xFF])
    assert BYTES.encode(blob) == "++//"
    assert BYTES.encode(b"A") == "QQ=="
    with pytest.raises(ValueError):
        BYTES.decode("--__")


def test_bytes_rejects_whitespace_and_non_alphabet():
    """`validate=True`: the stdlib default would discard these characters and
    hand back a plausible value — repairing the input, which the framework's
    decoders never do."""
    for wire in ("SGVs bG8=", "SGVsbG8=\n", "*"):
        with pytest.raises(ValueError):
            BYTES.decode(wire)


def test_bytes_rejects_non_canonical_trailing_bits():
    """`"QR=="` decodes to `b"A"` under the stdlib just like `"QQ=="` does.
    Accepting it would make the decoder non-injective, so a decode/re-encode
    cycle would silently rewrite the wire form."""
    assert base64.b64decode("QR==", validate=True) == b"A"
    with pytest.raises(ValueError, match="non-canonical base64"):
        BYTES.decode("QR==")


def test_bytes_rejects_excess_padding_and_trailing_data():
    for wire in ("QUJD=", "QUJDRA==extra"):
        with pytest.raises(ValueError):
            BYTES.decode(wire)


def test_bytes_rejects_non_str_wire_value():
    """JSON only ever yields `str` here; a raw `bytes` would slip past
    `b64decode` itself, so the codec names the type instead."""
    with pytest.raises(TypeError, match="expected a base64 str, got bytes"):
        BYTES.decode(b"QUJD")


def test_bytes_encode_rejects_bytearray_and_memoryview():
    """Exact-type discipline (the int/bool precedent): both encode happily but
    read back as `bytes`, so the codec would not round-trip its own values."""
    for value in (bytearray(b"AB"), memoryview(b"AB")):
        with pytest.raises(AssertionError, match="expected bytes"):
            BYTES.encode(value)  # type: ignore[arg-type]


def test_bytes_encode_rejects_str():
    with pytest.raises(AssertionError, match="expected bytes, got str"):
        BYTES.encode("AB")  # type: ignore[arg-type]


def test_any_rejects_bytes():
    """BYTES is deliberately not wired into the ANY walker: an ANY field would
    encode binary to a base64 string and read it back as that string, with
    nothing on the wire to tell it from text. Binary in a record is an explicit
    `Attribute(name, BYTES)`."""
    with pytest.raises(TypeError, match="no encoder for bytes"):
        ANY.encode(b"AB")


def test_bytes_attribute_round_trips_through_a_record():
    THUMBNAIL = Attribute("thumbnail", BYTES)
    record = Record({THUMBNAIL: b"\x89PNG\r\n\x1a\n"})
    assert record.raw == {"thumbnail": "iVBORw0KGgo="}
    assert json.loads(json.dumps(record.raw)) == record.raw
    assert Record.wrap(record.raw)[THUMBNAIL] == b"\x89PNG\r\n\x1a\n"


def test_bytes_composes_with_the_container_constructors():
    assert LIST(BYTES).encode([b"AB", b"CD"]) == ["QUI=", "Q0Q="]
    assert LIST(BYTES).decode(["QUI=", "Q0Q="]) == [b"AB", b"CD"]
    assert DICT(BYTES).encode({"k": b"AB"}) == {"k": "QUI="}


def test_codec_module_is_the_complete_catalogue():
    """`flechtwerk.attribute.codec` is the one namespace holding every codec —
    the disambiguating import (`codec.DATE`) must never miss one the package
    exports, or an application with a colliding name has no way to reach it."""
    assert set(codec.__all__) == {
        "ANY", "BOOL", "BYTES", "Codec", "DATE", "DATETIME", "DICT", "Decoder", "Encoder",
        "FLOAT", "INT", "LIST", "RECORD", "SET", "STR", "TIME", "TUPLE", "record_codec",
    }
    package_codecs = {name for name in package.__all__ if isinstance(getattr(package, name), Codec)}
    assert package_codecs <= set(codec.__all__)


def test_package_reexports_the_codec_module_verbatim():
    assert set(codec.__all__) <= set(package.__all__)
    for name in codec.__all__:
        assert getattr(package, name) is getattr(codec, name), name


def test_identity_codec_stays_internal():
    assert "IDENTITY" not in codec.__all__
    assert not hasattr(codec, "IDENTITY")
    assert not hasattr(package, "IDENTITY")


def _imports_codec_facade(tree: ast.Module) -> bool:
    """Whether a module imports `flechtwerk.attribute.codec`, in any spelling."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "flechtwerk.attribute.codec" for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if (node.level, node.module) in {(0, "flechtwerk.attribute.codec"), (1, "codec")}:
                return True
            if (node.level, node.module) in {(0, "flechtwerk.attribute"), (1, None)} and any(
                alias.name == "codec" for alias in node.names
            ):
                return True
    return False


@pytest.mark.parametrize(
    "source",
    [
        "from .codec import Codec",
        "from . import codec",
        "from flechtwerk.attribute.codec import DATE",
        "from flechtwerk.attribute import codec",
        "import flechtwerk.attribute.codec",
    ],
)
def test_import_rule_detector_recognizes_every_spelling(source):
    assert _imports_codec_facade(ast.parse(source))


def test_package_internals_never_import_the_codec_facade():
    """Modules inside `flechtwerk.attribute` import `_codec`, never `codec`.

    `codec` re-exports `RECORD` / `ANY` / `record_codec` from `record.py`, so
    an internal module importing it closes the cycle
    `attribute -> codec -> record -> attribute` — which surfaces as an
    `ImportError` about a partially initialized module, naming the symptom
    instead of this rule. Only the package `__init__` (the public face) and
    `codec.py` itself are exempt; modules outside the package (`secrets.py`)
    sit downstream of the cycle and may use the facade freely.
    """
    root = Path(package.__file__).parent
    exempt = {"__init__.py", "codec.py"}
    offenders = [
        path.name
        for path in sorted(root.glob("*.py"))
        if path.name not in exempt and _imports_codec_facade(ast.parse(path.read_text()))
    ]
    assert offenders == []
