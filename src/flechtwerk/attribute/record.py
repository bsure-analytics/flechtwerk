"""A typed dict wrapper, keyed exclusively by `Attribute` objects.

`Record` wraps an underlying `dict[str, Any]` (exposed as `raw`) whose
string keys serialize directly to JSON. All public access uses
`Attribute` instances:

    event[CHANNEL_ID]              # RequiredAttribute → decoded V or raise
    event[CHANNEL_ID] = value      # any Attribute → encodes via attr.encode
    event.get(STATUS, default)     # OptionalAttribute → decoded V or default
    event.pop(LAST_TIME, default)  # OptionalAttribute → decoded V or default
    del event[CHANNEL_ID]          # any Attribute → removes the entry
    CHANNEL_ID in event            # any Attribute → presence check

Indexing with a string raises `TypeError`. The `__getitem__` / `get` /
`pop` signatures encode the schema intent: required fields use `[]`,
optional fields use `.get()` / `.pop()`.

Iteration yields the raw name strings of the wrapped dict — useful for
inspection but not for re-indexing back into the `Record`.
"""
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime
from typing import Any, Final, Self, overload

from .attribute import Attribute, OptionalAttribute, RequiredAttribute
from .codec import Codec
from .codecs import DATETIME, DICT, LIST, SET, TUPLE


class MissingAttributeError(KeyError):
    """To raise by `Record.__getitem__` when a required attribute is missing or `None`."""


def _encode_any(v: Any) -> Any:
    """Recursively encode any value to JSON-native form via isinstance dispatch.

    Walks dicts/lists/sets/tuples through their `(ANY)` codecs; converts
    `datetime` to ISO 8601, `Record` to a shallow copy of its `raw`. The
    JSON-native primitives (`str`, `int`, `float`, `bool`, `None`) pass
    through unchanged. Raises `TypeError` on any other type — silent
    passthrough would let non-JSON-native values land in `Record.raw` and
    crash later in `json.dumps`.

    This is the implementation of `ANY.encode` and the runtime-dispatch
    layer that container codecs delegate into when their inner is `ANY`.
    """
    if v is None:
        return v
    if isinstance(v, (str, int, float)):  # bool ⊂ int — passes through as bool
        return v
    if isinstance(v, Record):
        return RECORD.encode(v)
    if isinstance(v, dict):
        return _DICT_OF_ANY.encode(v)
    if isinstance(v, list):
        return _LIST_OF_ANY.encode(v)
    if isinstance(v, datetime):
        return DATETIME.encode(v)
    if isinstance(v, set):
        return _SET_OF_ANY.encode(v)
    if isinstance(v, tuple):
        return _TUPLE_OF_ANY.encode(v)
    raise TypeError(f"no encoder for {type(v).__name__}: {v!r}")


class Record:
    """Wrapper around `dict[str, Any]` with `Attribute`-only access."""

    raw: dict[str, Any]

    # TODO(legacy-pickle-compat): once all changelog topics in every environment
    # have been fully replaced with new-format entries (the str-key __setitem__
    # branch below is no longer reached), remove this `__new__` and move the
    # `self.raw = {}` initialization back into `__init__`.
    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        # Initialize `raw` in __new__ so it exists even when pickle skips __init__.
        instance = super().__new__(cls)
        instance.raw = {}
        return instance

    def __init__(self, source: dict[str | Attribute, Any] | Record | None = None, /) -> None:
        if source is None:
            return  # raw already {} from __new__
        if isinstance(source, Record):
            self.raw = source.raw.copy()
            return
        # Top-level Attribute keys rekey to `attr.name` and run the
        # attribute's encoder on the value; plain string keys pass
        # through and the value goes through the recursive `_encode_any`
        # walker. This is the only place in the framework that accepts
        # mixed `Attribute | str` keys — every codec downstream is
        # strict (`DICT(V)` rejects non-str keys).
        self.raw = {
            (k.name if isinstance(k, Attribute) else k):
                (k.encode(v) if isinstance(k, Attribute) else _encode_any(v))
            for k, v in source.items()
        }

    def __reduce__(self) -> tuple:
        # Clean modern pickle format: (cls, (raw,)) → reconstruct via cls(raw).
        # Legacy changelog entries (saved when Event/Config/State were dict
        # subclasses) are restored via the str-key path in __setitem__ below.
        return self.__class__, (self.raw,)

    # --- indexing ---

    def __getitem__[V](self, attr: RequiredAttribute[V]) -> V:
        v = self.raw.get(attr.name)
        if v is None:
            raise MissingAttributeError(f"attribute {attr!r} is missing")
        return attr.decode(v)

    def __setitem__[V](self, attr: Attribute[V], value: V) -> None:
        # TODO(legacy-pickle-compat): once all changelog topics in every
        # environment have been fully replaced with new-format entries, remove
        # the str-key branch below — it exists only for unpickling legacy
        # dict-subclass State/Config/Event records.
        if isinstance(attr, str):
            # Backwards-compat path for unpickling legacy dict-subclass
            # State/Config/Event entries from changelog topics. Type checker
            # rejects str keys; only pickle's SETITEMS opcode reaches this branch.
            self.raw[attr] = value
            return
        encoded = attr.encode(value)
        if encoded is None:
            raise ValueError(f"encoder for {attr!r} returned None")
        self.raw[attr.name] = encoded

    def __delitem__(self, attr: Attribute) -> None:
        del self.raw[attr.name]

    def __contains__(self, attr: Attribute) -> bool:
        return attr.name in self.raw

    # --- container protocol ---

    def __len__(self) -> int:
        return len(self.raw)

    def __iter__(self) -> Iterator[str]:
        return iter(self.raw)

    def __bool__(self) -> bool:
        return bool(self.raw)

    # --- equality, repr, copy, hash ---

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and self.raw == other.raw  # type: ignore[attr-defined]

    # Defining `__eq__` implicitly sets `__hash__ = None`, marking the class
    # unhashable — no need to spell it out.

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    def __copy__(self) -> Record:
        return type(self)(self.raw)  # type: ignore[arg-type]

    def __deepcopy__(self, memo: dict) -> Record:
        return type(self)(deepcopy(self.raw, memo))  # type: ignore[arg-type]

    # --- Pythonic helpers (Optional only) ---

    @overload
    def get[V](self, attr: OptionalAttribute[V]) -> V | None: ...
    @overload
    def get[V](self, attr: OptionalAttribute[V], default: V) -> V: ...
    def get[V](self, attr: OptionalAttribute[V], default: V | None = None) -> V | None:
        """Return the decoded value, or `default` if missing or `None`."""
        v = self.raw.get(attr.name)
        return default if v is None else attr.decode(v)

    def pop[V](self, attr: OptionalAttribute[V], /, *default: V) -> V:
        """Remove and return the decoded value; raise KeyError if missing and no default given."""
        if attr.name in self.raw:
            return attr.decode(self.raw.pop(attr.name))
        if default:
            return default[0]
        raise KeyError(attr)

    def update(self, other: Record) -> None:
        """Merge another `Record` into this one."""
        self.raw.update(other.raw)


def record_codec[T: Record](cls: type[T]) -> Codec[T]:
    """Build a `Codec[T]` for a `Record` subclass `T`.

    Decode wraps the raw dict in `cls`; encode returns a shallow copy of
    `.raw`. Top-level isolation is preserved (the new owner gets its own
    dict to mutate via `__setitem__`); nested dicts/lists are shared,
    matching the framework's "treat `.raw` as immutable from outside"
    contract.
    """
    return Codec(
        decode=lambda raw: cls(raw),
        encode=lambda r: r.raw.copy(),
    )


RECORD: Final = record_codec(Record)
"""Codec for an untyped `Record` value.

Use as `RequiredAttribute("data", RECORD)` for fields whose value is a
plain `Record`. For `Record` subclasses, build a per-subclass codec via
`record_codec(cls)` (or use the constants exported from `fretworx.types`
for `Config`, `Event`, `State`).
"""

ANY: Final = Codec[Any](
    decode=lambda x: x,
    encode=_encode_any,
)
"""The universal escape-hatch codec for `Any`-typed values.

Decode is identity (the wire value passes through unchanged — JSON load
already produced JSON-native shape). Encode runs the recursive
`_encode_any` walker, which dispatches on runtime type and handles
`Record`, `dict`, `list`, `set`, `tuple`, `datetime`, and the JSON-native
primitives.

Compose with the container constructors for typed-but-heterogeneous
fields: `LIST(DICT(ANY))` for `list[dict[str, Any]]`, `DICT(ANY)` for
`dict[str, Any]`, etc.
"""

# Pre-built bare-Any container codecs, used by `_encode_any`'s isinstance
# dispatch. Building these once at module load avoids reconstructing a
# fresh `Codec` per recursive call.
_DICT_OF_ANY: Final = DICT(ANY)
_LIST_OF_ANY: Final = LIST(ANY)
_SET_OF_ANY: Final = SET(ANY)
_TUPLE_OF_ANY: Final = TUPLE(ANY)
