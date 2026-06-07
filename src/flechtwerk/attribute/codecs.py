"""Built-in codec atoms and constructors.

The catalogue is composable: atoms (`STR`, `INT`, `BOOL`, `DATE`, `FLOAT`,
`DATETIME`, `TIME`) are fixed leaves; constructors (`LIST`, `SET`, `TUPLE`,
`DICT`) take an inner codec and return a parameterized container codec.
Element validation is uniform — `LIST(STR).encode([1, 2, 3])` rejects
non-strings (each element runs through `STR.encode`).

`RECORD` and `ANY` live in `record.py` because they need the `Record`
class and the recursive `_encode_any` walker, respectively.

Naming convention: all atoms and constructors use uppercase identifiers
matching the ALL_CAPS style of the typed-attribute call sites.
"""
from datetime import date, datetime, time
from typing import Any, Final

from .codec import Codec, Decoder


def _encode_datetime(dt: datetime) -> str:
    """Encoder for `DATETIME` — fixed millisecond precision, `Z` for UTC.

    The `Z` suffix replaces `+00:00` only when the zone *is* UTC
    (`tzname() == "UTC"` — matches `timezone.utc`, `ZoneInfo("UTC")` and
    plain `timezone(timedelta(0))`). A zone that merely coincides with UTC
    (e.g. `Europe/London` in winter, `tzname() == "GMT"`) keeps its
    `+00:00` offset — `Z` asserts UTC, not a zero offset.
    """
    encoded = dt.isoformat(timespec="milliseconds")
    return encoded.replace("+00:00", "Z") if dt.tzname() == "UTC" else encoded


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
DATE: Final = Codec[date](date.fromisoformat, date.isoformat)
FLOAT: Final = Codec[float](_validate(float), _validate(float))
DATETIME: Final = Codec[datetime](datetime.fromisoformat, _encode_datetime)
TIME: Final = Codec[time](time.fromisoformat, time.isoformat)


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

Deliberately not re-exported from `fretworx.attribute`: an application
that wants pass-through behavior should declare a real codec for the
underlying type rather than reach for `IDENTITY`. Available via the
fully-qualified import for the framework internals that need it.
"""

__all__ = [
    "BOOL",
    "DATETIME",
    "DICT",
    "FLOAT",
    "INT",
    "LIST",
    "SET",
    "STR",
    "TIME",
    "TUPLE",
]
