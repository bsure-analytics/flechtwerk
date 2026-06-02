"""Built-in codec atoms and constructors.

The catalogue is composable: atoms (`STR`, `INT`, `BOOL`, `FLOAT`,
`DATETIME`, `TIME`) are fixed leaves; constructors (`LIST`, `SET`, `TUPLE`,
`DICT`) take an inner codec and return a parameterized container codec.
Element validation is uniform — `LIST(STR).encode([1, 2, 3])` rejects
non-strings (each element runs through `STR.encode`).

`RECORD` and `ANY` live in `record.py` because they need the `Record`
class and the recursive `_encode_any` walker, respectively.

Naming convention: all atoms and constructors use uppercase identifiers
matching the ALL_CAPS style of the typed-attribute call sites.
"""
from datetime import datetime, time, timedelta
from typing import Any, Final

from .codec import Codec, Decoder


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


def _datetime_from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _datetime_to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _time_from_iso(s: str) -> time:
    return time.fromisoformat(s)


def _timedelta_to_time(td: timedelta) -> time:
    """Wrap a duration into a wall-clock time-of-day (modulo 24h).

    Pandas reads some Excel time cells as `datetime.timedelta` instead of
    `datetime.time` — depends on the cell format. Promoterstunden break /
    work-time columns are wall-clock by intent; midnight crossings are
    already recovered downstream in `expand_modules_and_transform` (the
    `if data_end < data_start: data_end += 86400000` branch), so wrapping
    here is safe.
    """
    seconds = int(td.total_seconds()) % 86_400
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return time(h, m, s)


def _time_to_iso(t: time | timedelta) -> str:
    if isinstance(t, timedelta):
        t = _timedelta_to_time(t)
    return t.isoformat()


# --- atoms ---


STR: Final = Codec[str](_validate(str), _validate(str))
INT: Final = Codec[int](_validate(int), _validate(int))
BOOL: Final = Codec[bool](_validate(bool), _validate(bool))
FLOAT: Final = Codec[float](_validate(float), _validate(float))
DATETIME: Final = Codec[datetime](_datetime_from_iso, _datetime_to_iso)
TIME: Final = Codec[time](_time_from_iso, _time_to_iso)


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
