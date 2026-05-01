"""Built-in codec atoms and constructors.

The catalogue is composable: atoms (`STR`, `INT`, `BOOL`, `FLOAT`,
`DATETIME`) are fixed leaves; constructors (`LIST`, `SET`, `TUPLE`,
`DICT`) take an inner codec and return a parameterized container codec.
Element validation is uniform — `LIST(STR).encode([1, 2, 3])` rejects
non-strings (each element runs through `STR.encode`).

`RECORD` and `ANY` live in `record.py` because they need the `Record`
class and the recursive `_encode_any` walker, respectively.

Naming convention: all atoms and constructors use uppercase identifiers
matching the ALL_CAPS style of the typed-attribute call sites.
"""
from datetime import datetime, timezone
from typing import Any, Final

from .attribute import Attribute
from .codec import Codec, Decoder


def _validate[T](t: type[T]) -> Decoder[T]:
    """Codec helper that asserts `type(x) is t`, raising `TypeError` on mismatch.

    Exact-type check, not `isinstance` — this matters for the int/bool split
    (`bool` is a subclass of `int`, but `INT.encode(True)` should reject).
    """

    def check(x: Any) -> T:
        if type(x) is not t:
            raise TypeError(
                f"expected {t.__name__}, got {type(x).__name__}: {x!r}"
            )
        return x

    return check


def _datetime_from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _datetime_to_iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# --- atoms ---


STR: Final[Codec[str]] = Codec(_validate(str), _validate(str))
INT: Final[Codec[int]] = Codec(_validate(int), _validate(int))
BOOL: Final[Codec[bool]] = Codec(_validate(bool), _validate(bool))
FLOAT: Final[Codec[float]] = Codec(_validate(float), _validate(float))
DATETIME: Final[Codec[datetime]] = Codec(_datetime_from_iso, _datetime_to_iso)


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

    Encode also handles `Attribute` keys: an `Attribute` key rekeys to
    `attr.name` and runs `attr.encode` on its value (overriding `inner`),
    so a `Record({SOME_ATTR: v, "literal_key": v2})` produces JSON-native
    `.raw` even when keys are mixed.
    """
    return Codec(
        decode=lambda d: {k: inner.decode(v) for k, v in d.items()},
        encode=lambda d: {
            (k.name if isinstance(k, Attribute) else k):
                (k.encode(v) if isinstance(k, Attribute) else inner.encode(v))
            for k, v in d.items()
        },
    )


__all__ = [
    "BOOL",
    "DATETIME",
    "DICT",
    "FLOAT",
    "INT",
    "LIST",
    "SET",
    "STR",
    "TUPLE",
]
