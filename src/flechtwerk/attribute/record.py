"""A typed dict wrapper, keyed exclusively by `Attribute` objects.

`Record` wraps an underlying `dict[str, Any]` (exposed as `raw`) whose
string keys serialize directly to JSON. All public access uses
`Attribute` instances:

    event[CHANNEL_ID]              # decoded V, or raise if absent/null
    event[CHANNEL_ID] = value      # dispatches via attr.write_to
    event.get(STATUS, default)     # decoded V or default
    event.pop(LAST_TIME, default)  # decoded V or default
    del event[STATUS]              # removes the entry
    STATUS in event                # presence check

Indexing with a string raises `TypeError`. The *method* encodes the
presence intent — `[]` asserts the value is present (returns `V`),
`.get()` / `.pop()` tolerate absence (return `V | None`) — independent
of whether the attribute was declared `optional`.

Iteration (`iter(record)`, `for attr in record`) yields the same
synthesized `ViewAttribute` handles that `keys()` produces — aligning
with the standard `Mapping` convention that `iter(d) == iter(d.keys())`.
Use `record.raw` directly if you need the wire-form key strings.

Dict-spread is supported (`Record({**other_record, NEW_ATTR: value})`):
``keys()`` yields `ViewAttribute` handles that override `read_from`
(raw passthrough, None-tolerant) and `write_to` (identity store) so the
spread roundtrip flows through the standard Record paths without any
special-casing in Record itself.
"""
from collections.abc import Iterable, Iterator
from copy import deepcopy
from datetime import date, datetime, time
from typing import Any, Final, Self, overload

from .attribute import (
    Attribute,
    RawDict,
    ViewAttribute,
)
from .codec import Codec
from .codecs import DATE, DATETIME, DICT, LIST, SET, TIME, TUPLE


def _encode_any(v: Any) -> Any:
    """Recursively encode any value to JSON-native form via isinstance dispatch.

    Walks dicts/lists/sets/tuples through their `(ANY)` codecs; converts
    `datetime` to ISO 8601, `date` to ISO 8601 (`YYYY-MM-DD`), `time` to
    ISO 8601 (`HH:MM:SS[.ffffff]`), `Record` to a shallow copy of its
    `raw`. The JSON-native primitives
    (`str`, `int`, `float`, `bool`, `None`) pass through unchanged.
    Raises `TypeError` on any other type — silent passthrough would let
    non-JSON-native values land in `Record.raw` and crash later in
    `json.dumps`.

    This is the implementation of `ANY.encode` and the runtime-dispatch
    layer that container codecs delegate into when their inner is `ANY`.
    """
    if v is None:
        return v
    if isinstance(v, (str, int, float)):  # bool ⊂ int — passes through as bool
        return v
    if isinstance(v, datetime):
        return DATETIME.encode(v)
    if isinstance(v, date):  # datetime ⊂ date — must come after the datetime check
        return DATE.encode(v)
    if isinstance(v, time):
        return TIME.encode(v)
    if isinstance(v, Record):
        return RECORD.encode(v)
    if isinstance(v, dict):
        return _DICT_OF_ANY.encode(v)
    if isinstance(v, list):
        return _LIST_OF_ANY.encode(v)
    if isinstance(v, set):
        return _SET_OF_ANY.encode(v)
    if isinstance(v, tuple):
        return _TUPLE_OF_ANY.encode(v)
    raise TypeError(f"no encoder for {type(v).__name__}: {v!r}")


class Record:
    """Wrapper around a `RawDict` with `Attribute`-only access."""

    raw: RawDict

    # TODO(legacy-pickle-compat): once all changelog topics in every environment
    # have been fully replaced with new-format entries (the str-key __setitem__
    # branch below is no longer reached), remove this `__new__` and move the
    # `self.raw = {}` initialization back into `__init__`.
    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        # Initialize `raw` in __new__ so it exists even when pickle skips __init__.
        instance = super().__new__(cls)
        instance.raw = {}
        return instance

    def __init__(self, source: Record | dict[Attribute, Any] | None = None, /) -> None:
        if source is None:
            return
        if isinstance(source, Record):
            self.raw = source.raw.copy()
            return
        for k, v in source.items():
            k.write_to(self.raw, v)

    @classmethod
    def wrap(cls, source: RawDict, /) -> Self:
        self = cls()
        for k, v in source.items():
            self.raw[k] = _encode_any(v)
        return self

    def __reduce__(self) -> tuple:
        # Clean modern pickle format: (cls.wrap, (raw,)) → reconstruct via cls.wrap(raw).
        # Legacy changelog entries (saved when Event/Config/State were dict
        # subclasses) are restored via the str-key path in __setitem__ below.
        return self.__class__.wrap, (self.raw,)

    # --- indexing ---

    def __getitem__[V](self, attr: Attribute[V]) -> V:
        return attr.read_from(self.raw)

    def __setitem__[V](self, attr: Attribute[V], value: V | None) -> None:
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
        attr.write_to(self.raw, value)

    def __delitem__(self, attr: Attribute) -> None:
        """Remove the key from the underlying dict. Raises `KeyError` if absent."""
        attr.delete_from(self.raw)

    def __contains__(self, attr: Attribute) -> bool:
        """Check key presence — kind-agnostic, since a question neither decodes
        a value nor mutates the absence contract."""
        return attr.present_in(self.raw)

    # --- container protocol ---

    def __len__(self) -> int:
        return len(self.raw)

    def __iter__(self) -> Iterator[Attribute[Any]]:
        """Yield a `ViewAttribute` per key in `raw`, lazily.

        The primary iteration protocol — `keys()` materializes this for
        the dict-spread path. Aligns with Python's dict idiom where
        `__iter__` is the fundamental lazy walk and `keys()` is the
        view-returning convenience.
        """
        for name in self.raw:
            yield ViewAttribute(name)

    def keys(self) -> Iterable[Attribute[Any]]:
        """Materialize a list of `ViewAttribute` handles from `__iter__`.

        Enables dict-spread: ``Record({**other, NEW_ATTR: value})`` calls
        ``other.keys()`` and then ``other[view_attr]`` for each — both
        landing on the view's overridden read/write methods. The constructor
        stores the values as-is via ``ViewAttribute.write_to``.
        """
        return list(self)

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
        return type(self)(self)

    def __deepcopy__(self, memo: dict) -> Record:
        new = type(self)()
        new.raw = deepcopy(self.raw, memo)
        return new

    # --- Pythonic helpers ---

    @overload
    def get[V](self, attr: Attribute[V]) -> V | None:
        ...

    @overload
    def get[V](self, attr: Attribute[V], default: V) -> V:
        ...

    def get[V](self, attr: Attribute[V], default: V | None = None) -> V | None:
        """Return the decoded value, or `default` if missing or `None`."""
        return attr.get_from(self.raw, default)

    def pop[V](self, attr: Attribute[V], /, *default: V) -> V | None:
        """Remove and return the decoded value; raise KeyError if missing and no default given.

        A stored `None` is returned as `None` (no decode) and the key is removed —
        mirroring `dict.pop` semantics and matching how an optional attribute's
        `write_to` allows `None` writes.
        """
        return attr.pop_from(self.raw, *default)

    def coalesce[V](self, *attrs: Attribute[V]) -> V | None:
        """Return the first non-`None` decoded value among the given attributes.

        Equivalent to a chain of `.get()` falls-through: returns
        `.get(attrs[0])` if present, else `.get(attrs[1])`, ..., else
        `None`. Useful for wire formats where the same logical field
        appears under several names (e.g. pagination variants like
        `pages` vs `total_page` vs `total_pages`).
        """
        for attr in attrs:
            v = attr.get_from(self.raw)
            if v is not None:
                return v
        return None

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
        decode=cls.wrap,
        encode=lambda r: r.raw.copy(),
    )


RECORD: Final = record_codec(Record)
"""Codec for an untyped `Record` value.

Use as `Attribute("data", RECORD)` for fields whose value is a
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
`Record`, `dict`, `list`, `set`, `tuple`, `datetime`, `date`, `time`,
and the JSON-native primitives.

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
