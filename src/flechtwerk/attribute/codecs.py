"""Built-in codec registrations.

Importing this module populates the registry with codecs for the JSON-native
primitives, the JSON-friendly containers (with recursive walkers for `dict`
and `list`), and the small set of non-JSON-native types we round-trip through
JSON: `set` ⇄ sorted `list`, `tuple` ⇄ `list`, `datetime` ⇄ ISO 8601 string.
"""
from datetime import datetime, timezone
from typing import Any, TypeVar

from .attribute import Attribute
from .registry import Decoder, decoder, encoder, lookup_encoder

T = TypeVar("T")


# --- helpers (used by registrations below) ---


def _identity(x: T) -> T:
    return x


def _validate(t: type[T]) -> Decoder[T]:
    """Codec that asserts `isinstance(x, t)`, raising `TypeError` on mismatch."""

    def check(x: Any) -> T:
        if not isinstance(x, t):
            raise TypeError(
                f"expected {t.__name__}, got {type(x).__name__}: {x!r}"
            )
        return x

    return check


def encode_any(v: Any) -> Any:
    """Encode any value to JSON-native form via the codec registry.

    Dispatches strictly on `type(v)`: the registered encoder for the exact
    type runs (recursive walkers for `dict` and `list`, codec round-trips
    for `datetime` / `set` / `tuple`, identity-with-validate for primitives,
    auto-registered shallow copy for `Dict` subclasses).

    Raises `CodecError` on unknown types — silent passthrough would let
    non-JSON-native values land in `Dict.raw` and crash later in `json.dumps`.
    Subclasses (`OrderedDict`, `MappingProxyType`, etc.) aren't matched by
    exact-type lookup; if you need them, register an encoder explicitly.
    """
    return lookup_encoder(type(v))(v)


# --- primitives: validate-on-pass-through ---


for _t in (str, int, bool, float):
    decoder(_t)(_validate(_t))
    encoder(_t)(_validate(_t))

# --- JSON's `null` ---
# Registered separately because `_validate(NoneType)` would always pass anyway
# (only `None` is NoneType), and the recursive walker needs a registered
# encoder for the type so it doesn't raise on `None` values nested inside
# dicts/lists.


decoder(type(None))(_identity)
encoder(type(None))(_identity)


# --- containers: recursive walkers on encode, identity on decode ---


decoder(dict)(_identity)
decoder(list)(_identity)


@encoder(dict)
def _encode_dict(d: dict) -> dict:
    """Encode a mapping to a JSON-native dict.

    Keys are passed through unchanged unless they are `Attribute` instances,
    in which case the key is rekeyed to `attr.name` and the value is run
    through the attribute's encoder. Other-typed keys' values go through
    `encode_any` (recursive for nested dicts/lists). This lets dict literals
    mix typed (`Attribute`) and plain string keys at the call site —
    `Event({DATA: payload, "literal_key": dt})` produces the right `.raw`.
    """
    return {
        (k.name if isinstance(k, Attribute) else k):
            (k.encode(v) if isinstance(k, Attribute) else encode_any(v))
        for k, v in d.items()
    }


@encoder(list)
def _encode_list(items: list) -> list:
    return [encode_any(v) for v in items]


# --- non-JSON-native types: real conversion ---


decoder(set)(lambda items: set(items))
encoder(set)(lambda s: sorted(s))  # deterministic wire form for diff stability

decoder(tuple)(lambda t: tuple(t))
encoder(tuple)(lambda t: list(t))


@decoder(datetime)
def datetime_from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


@encoder(datetime)
def datetime_to_iso(dt: datetime) -> str:
    return (
        dt.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
