"""Implementation home of `Codec[V]` and the built-in atoms and constructors.

Applications import these from `flechtwerk.attribute.codec` (or the
`flechtwerk.attribute` package); this module exists so that `attribute.py`
and `record.py` can reach the codec primitives without importing the public
`codec` module, which re-exports `RECORD` / `ANY` / `record_codec` from
`record.py` and would otherwise close an import cycle.

The catalogue is composable: atoms (`STR`, `INT`, `BOOL`, `BYTES`, `DATE`,
`FLOAT`, `DATETIME`, `TIME`, `ZONE_INFO`) are fixed leaves; constructors
(`LIST`, `SET`, `TUPLE`, `DICT`) take an inner codec and return a
parameterized container codec.
Element validation is uniform — `LIST(STR).encode([1, 2, 3])` rejects
non-strings (each element runs through `STR.encode`).

`RECORD` and `ANY` live in `record.py` because they need the `Record`
class and the recursive `_encode_any` walker, respectively.

Naming convention: all atoms and constructors use uppercase identifiers
matching the ALL_CAPS style of the typed-attribute call sites.
"""
import base64
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time
from functools import cache
from typing import Any, Final
from zoneinfo import ZoneInfo, available_timezones

# Framework-internal on purpose: the public surface is `flechtwerk.attribute.codec`,
# which re-exports everything here except `IDENTITY` and the private helpers.
__all__: list[str] = []

type Decoder[V] = Callable[[Any], V]
"""A function that decodes a JSON-native wire value to a Python value of type `V`."""

type Encoder[V] = Callable[[V], Any]
"""A function that encodes a Python value of type `V` to a JSON-native wire value."""


@dataclass(frozen=True, slots=True, eq=False)
class Codec[V]:
    """A pair of `(decode, encode)` callables for a value of type `V`.

    Both directions are required — there is no fallback registry. The value
    type `V` is the single source of truth: pass a `Codec[V]` to an
    `Attribute` and the type checker infers the `Attribute[V]` parameter.

    Equality is identity (`eq=False`): the fields are functions, so a
    generated field-wise `__eq__` would compare them by object identity
    anyway while *looking* like value equality — composite codecs like
    `LIST(STR)` rebuild fresh closures per call and would never compare
    equal. Identity is the only honest contract, and `Attribute` already
    excludes the codec from its own equality for this reason.
    """
    decode: Decoder[V]
    encode: Encoder[V]


def _encode_datetime(dt: datetime) -> str:
    """Encoder for `DATETIME` — lossless `isoformat` defaults, `Z` for UTC.

    Default `isoformat` precision (full microseconds when nonzero, no
    fraction when zero) keeps the encoder lossless — a fixed `timespec`
    would silently truncate sub-millisecond values and break round-trips.

    The `Z` suffix replaces `+00:00` only when the zone *is* UTC
    (`tzname() == "UTC"` — matches `timezone.utc`, `ZoneInfo("UTC")` and
    plain `timezone(timedelta(0))`). A zone that merely coincides with UTC
    (e.g. `Europe/London` in winter, `tzname() == "GMT"`) keeps its
    `+00:00` offset — `Z` asserts UTC, not a zero offset.
    """
    encoded = dt.isoformat()
    return encoded.replace("+00:00", "Z") if dt.tzname() == "UTC" else encoded


def _decode_bytes(v: Any) -> bytes:
    """Decoder for `BYTES` — strict, canonical RFC 4648 §4 base64.

    Two strictness steps beyond the stdlib default, both deliberate:

    `validate=True` rejects whitespace and any non-alphabet character
    instead of silently discarding it — `b64decode("QU JD")` returning
    `b"ABC"` is exactly the repair-the-input behaviour this codebase
    refuses (the `decode_key` argument).

    The re-encode comparison then rejects a token whose trailing bits are
    non-zero (`"QR=="` decodes to `b"A"`, same as `"QQ=="`), which
    `validate=True` still lets through. Without it the decoder is not
    injective: two distinct wire strings would decode to one value and
    a decode/re-encode cycle would rewrite the wire form underneath a
    `State` dedup or a value-equality check. This is the opposite call to
    `DATETIME`, which accepts a 3-digit fraction and normalizes on write —
    a millisecond timestamp is a legitimate alternative spelling that other
    systems emit, whereas no correct base64 encoder emits stray trailing
    bits.

    Neither error names the value: a blob attribute is large by nature and
    a megabyte in an exception message helps nobody.
    """
    if type(v) is not str:
        raise TypeError(f"expected a base64 str, got {type(v).__name__}")
    decoded = base64.b64decode(v, validate=True)
    if base64.b64encode(decoded).decode("ascii") != v:
        raise ValueError(f"non-canonical base64 ({len(v)} chars): re-encoding does not reproduce the input")
    return decoded


def _encode_bytes(b: bytes) -> str:
    """Encoder for `BYTES` — standard alphabet, padded, ASCII output.

    Exact-type check (the `_validate` discipline): `bytearray` and
    `memoryview` encode just as happily but read back as `bytes`, so
    accepting them would make the codec silently non-round-tripping.
    """
    assert type(b) is bytes, f"expected bytes, got {type(b).__name__}"
    return base64.b64encode(b).decode("ascii")


@cache
def _zone_keys() -> frozenset[str]:
    """The canonical IANA keys on this host, scanned once on first `ZONE_INFO` decode (~600 entries)."""
    return frozenset(available_timezones())


def _decode_zone_info(v: Any) -> ZoneInfo:
    """Decoder for `ZONE_INFO` — an IANA key, spelled exactly as the database spells it.

    An unknown key raises `ValueError`, like every other codec's decode
    failure; `ZoneInfo` itself would raise `ZoneInfoNotFoundError`, which is
    a `KeyError` and would slip past a caller that rejects bad payloads by
    catching `ValueError`.

    The membership check against `available_timezones()` is stricter than
    `ZoneInfo(v)` alone, deliberately: `ZoneInfo` resolves a key by opening a
    file, so on a case-insensitive filesystem (macOS) `"europe/berlin"`
    decodes fine and on Linux it doesn't. A record accepted on a developer
    machine and rejected in production is the portability trap a strict
    decoder exists to close — the `decode_key` argument again.
    """
    if type(v) is not str:
        raise TypeError(f"expected an IANA time zone key as str, got {type(v).__name__}")
    if v not in _zone_keys():
        raise ValueError(f"unknown time zone: {v!r}")
    return ZoneInfo(v)


def _encode_zone_info(z: ZoneInfo) -> str:
    """Encoder for `ZONE_INFO` — the zone's IANA key.

    Exact-type check (the `_validate` discipline), plus a key check: a zone
    built with `ZoneInfo.from_file` has `key is None` and no wire form at all.
    """
    assert type(z) is ZoneInfo, f"expected ZoneInfo, got {type(z).__name__}: {z!r}"
    assert z.key is not None, f"ZoneInfo without a key has no wire form: {z!r}"
    return z.key


def _validate[T](t: type[T]) -> Decoder[T]:
    """Codec helper that asserts `type(x) is t`.

    Exact-type check, not `isinstance` — this matters for the int/bool split
    (`bool` is a subclass of `int`, but `INT.encode(True)` should reject).
    Statically enforced by the `Codec[T]` parameter at the call site; the
    runtime `assert` is a safety net that disappears under `python -O`.
    """

    def check(x: Any) -> T:
        assert type(x) is t, f"expected {t.__name__}, got {type(x).__name__}: {x!r}"
        return x

    return check


# --- atoms ---


STR: Final = Codec[str](_validate(str), _validate(str))
INT: Final = Codec[int](_validate(int), _validate(int))
BOOL: Final = Codec[bool](_validate(bool), _validate(bool))
BYTES: Final = Codec[bytes](_decode_bytes, _encode_bytes)
"""Codec for `bytes` — RFC 4648 §4 base64 (standard alphabet, padded) as a JSON string.

The one atom whose Python type is not JSON-native, so the wire form is a
lossy-looking `str` that only this codec knows how to read back. Two
consequences worth declaring:

`bytes` is deliberately absent from the `ANY` walker — an `ANY` field
holding binary would encode to a base64 string and decode back as that
string, indistinguishable from text, and `Record.wrap` would start
committing raw dicts to a base64 wire form nobody declared. Binary in a
record is an explicit `Attribute(name, BYTES)` or a `TypeError`.

Base64 costs 4/3 of the payload before JSON escaping, against Kafka's
1 MiB record ceiling (see `state_record_bytes`). For a whole message that
*is* a blob, `Payload`'s `bytes` member ships it pre-encoded with no
framing at all — `BYTES` is for a binary *field* beside other typed ones.
"""
DATE: Final = Codec[date](date.fromisoformat, date.isoformat)
FLOAT: Final = Codec[float](_validate(float), _validate(float))
DATETIME: Final = Codec[datetime](datetime.fromisoformat, _encode_datetime)
TIME: Final = Codec[time](time.fromisoformat, time.isoformat)
ZONE_INFO: Final = Codec[ZoneInfo](_decode_zone_info, _encode_zone_info)
"""Codec for an IANA time zone (`Europe/Berlin`, `UTC`, `CET`) as its key string.

Fixed-offset `datetime.timezone` values are out of scope: they have no IANA
key, and an offset is not a zone (it knows nothing about DST). `ZoneInfo`
reads the host's tz database, falling back to the `tzdata` package — an
image without either rejects every key.

Like `BYTES`, the zone is deliberately absent from the `ANY` walker: its wire
form is a plain string that would read back as text.
"""


# --- constructors ---


def LIST[V](inner: Codec[V]) -> Codec[list[V]]:
    """Codec for `list[V]` — runs `inner` over each element."""
    return Codec(
        decode=lambda lst: [inner.decode(x) for x in lst],
        encode=lambda lst: [inner.encode(x) for x in lst],
    )


def SET[V](inner: Codec[V]) -> Codec[set[V]]:
    """Codec for `set[V]` — runs `inner` over each element.

    Encode emits a sorted list (deterministic wire form for diff stability).
    Sort is on raw values, then each is encoded — preserves the natural
    ordering of the source type. V must be hashable (a Python set
    requirement) and the raw values must be comparable to each other.
    """
    return Codec(
        decode=lambda items: {inner.decode(x) for x in items},
        encode=lambda s: [inner.encode(x) for x in sorted(s)],
    )


def TUPLE[V](inner: Codec[V]) -> Codec[tuple[V, ...]]:
    """Codec for `tuple[V, ...]` — runs `inner` over each element, preserves order."""
    return Codec(
        decode=lambda items: tuple(inner.decode(x) for x in items),
        encode=lambda t: [inner.encode(x) for x in t],
    )


def DICT[V](inner: Codec[V]) -> Codec[dict[str, V]]:
    """Codec for `dict[str, V]` — runs `inner` over each value.

    Keys must be exactly `str` — enforced statically by the
    `dict[str, V]` parameter and asserted at runtime via exact-type
    check (same discipline as `_validate`). Str subclasses (e.g.
    `StrEnum` members) are rejected — accepting them silently would
    lose type information at the boundary.
    """

    def encode(d: dict[str, V]) -> dict[str, Any]:
        for k in d:
            assert type(k) is str, f"DICT key must be str, got {type(k).__name__}: {k!r}"
        return {k: inner.encode(v) for k, v in d.items()}

    return Codec(
        decode=lambda d: {k: inner.decode(v) for k, v in d.items()},
        encode=encode,
    )


IDENTITY: Final = Codec[Any](decode=lambda x: x, encode=lambda x: x)
"""Identity codec — used by `ViewAttribute` where the value is already in wire form.

Deliberately not re-exported from `flechtwerk.attribute` or
`flechtwerk.attribute.codec`: an application that wants pass-through behavior
should declare a real codec for the underlying type rather than reach for
`IDENTITY`. The framework internals that need it import it from here.
"""
