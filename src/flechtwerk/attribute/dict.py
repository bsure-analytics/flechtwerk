"""A typed dict wrapper, keyed exclusively by `Attribute` objects.

`Dict` wraps an underlying `dict[str, Any]` (exposed as `raw`) whose
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
inspection but not for re-indexing back into the `Dict`.
"""
from collections.abc import Iterator
from copy import deepcopy
from typing import Any, TypeVar, overload

from .attribute import Attribute, OptionalAttribute, RequiredAttribute
from .codecs import encode_any
from .registry import Codec, decoder, encoder

V = TypeVar("V")


class MissingAttributeError(KeyError):
    """Raised by `Dict.__getitem__` when a required attribute is missing or `None`."""


class Dict:
    """Wrapper around `dict[str, Any]` with `Attribute`-only access."""

    raw: dict[str, Any]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Auto-register encode/decode for every Dict subclass. Encode returns
        # a shallow copy of `.raw` directly — it's already JSON-native by the
        # constructor / `__setitem__` invariants, so re-walking it through the
        # `dict` encoder would be redundant work. Top-level isolation is
        # preserved (the new owner gets its own dict to mutate via
        # `__setitem__`); nested dicts/lists are shared, matching the
        # framework's "treat `.raw` as immutable from outside" contract.
        # Decode rewraps the raw dict in the subclass.
        encoder(cls)(lambda d: d.raw.copy())
        decoder(cls)(cls)

    # TODO(legacy-pickle-compat): once all changelog topics in every environment
    # have been fully replaced with new-format entries (the str-key __setitem__
    # branch below is no longer reached), remove this `__new__` and move the
    # `self.raw = {}` initialization back into `__init__`.
    def __new__(cls, *args: Any, **kwargs: Any) -> Dict:
        # Initialize `raw` in __new__ so it exists even when pickle skips __init__.
        instance = super().__new__(cls)
        instance.raw = {}
        return instance

    def __init__(self, source: dict[Attribute | str, Any] | Dict | None = None, /) -> None:
        if source is None:
            return  # raw already {} from __new__
        # Delegate to `encode_any` — its dispatch handles every shape we care
        # about: a Dict subclass goes through the registered shallow-copy
        # encoder; a plain dict / Mapping goes through `_encode_dict`, which
        # rekeys Attribute keys to `attr.name`, runs the attribute's encoder
        # on their values, and recursively encodes everything else. The
        # invariant — `.raw` is JSON-native — is enforced by the codec layer.
        self.raw = encode_any(source)

    def __reduce__(self) -> tuple:
        # Clean modern pickle format: (cls, (raw,)) → reconstruct via cls(raw).
        # Legacy changelog entries (saved when Event/Config/State were dict
        # subclasses) are restored via the str-key path in __setitem__ below.
        return self.__class__, (self.raw,)

    # --- indexing ---

    def __getitem__(self, attr: RequiredAttribute[V]) -> V:
        v = self.raw.get(attr.name)
        if v is None:
            raise MissingAttributeError(f"attribute {attr!r} is missing")
        return attr.decode(v)

    def __setitem__(self, attr: Attribute[V], value: V) -> None:
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

    __hash__ = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    def __copy__(self) -> Dict:
        return type(self)(self.raw)

    def __deepcopy__(self, memo: dict) -> Dict:
        return type(self)(deepcopy(self.raw, memo))

    # --- Pythonic helpers (Optional only) ---

    @overload
    def get(self, attr: OptionalAttribute[V]) -> V | None: ...
    @overload
    def get(self, attr: OptionalAttribute[V], default: V) -> V: ...
    def get(self, attr: OptionalAttribute[V], default: V | None = None) -> V | None:
        """Return the decoded value, or `default` if missing or `None`."""
        v = self.raw.get(attr.name)
        return default if v is None else attr.decode(v)

    def pop(self, attr: OptionalAttribute[V], /, *default: V) -> V:
        """Remove and return the decoded value; raise KeyError if missing and no default given."""
        if attr.name in self.raw:
            return attr.decode(self.raw.pop(attr.name))
        if default:
            return default[0]
        raise KeyError(attr)

    def update(self, other: Dict) -> None:
        """Merge another `Dict` into this one."""
        self.raw.update(other.raw)


# `__init_subclass__` only fires for subclasses, so register the base `Dict`
# class manually with the same shallow-copy encoder. This lets `encode_any`
# dispatch on a base-class instance (i.e. someone instantiated `Dict`
# directly) the same way it handles subclasses.
encoder(Dict)(lambda d: d.raw.copy())
decoder(Dict)(Dict)


def list_of() -> Codec[list[Dict]]:
    """Codec for an `Attribute` whose value is a list of `Dict` instances.

    Use as `RequiredAttribute[list[Dict]](name, codec=list_of())`. The
    `decode` wraps each list item in `Dict`; the `encode` unwraps each
    `Dict` back to its raw dict (passing plain dicts through unchanged so
    existing callers that haven't migrated still work). The registry has
    no built-in codec for `list[Dict]` because the parametrization isn't
    matched at runtime — this helper is the per-attribute override.

    For a list of `Dict`-subclass instances, write the `Codec` inline.
    """
    return Codec(
        decode=lambda lst: [Dict(d) for d in lst],
        encode=lambda lst: [d.raw if isinstance(d, Dict) else d for d in lst],
    )
