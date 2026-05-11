"""Type-safe handles on keys in a dict, with a paired encode/decode codec.

`Attribute` is abstract — instantiate `RequiredAttribute` or
`OptionalAttribute` to declare schema intent. `Record.__getitem__` only
accepts `RequiredAttribute`; `Record.get` and `Record.pop` only accept
`OptionalAttribute`. `OptionalAttribute[V].required` and
`RequiredAttribute[V].optional` are `cached_property`s returning the
other-kind view of the same attribute (same name and codec); use them at
sites where the runtime presence semantic doesn't match the declared
schema kind (e.g. `state[OPT.required]` immediately after writing).

Each `Attribute` carries a `Codec[V]` that drives both the static type
parameter and the runtime encode/decode. The type checker infers the
`[V]` from the codec — `RequiredAttribute("name", STR)` produces a
`RequiredAttribute[str]` without an explicit subscript. Built-in codecs
are exported from `fretworx.attribute` (`STR`, `INT`, `DATETIME`,
`RECORD`, `LIST(RECORD)`, …).
"""
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any

from .codec import Codec
from .codecs import IDENTITY

type RawDict = dict[str, Any]
"""Wire-form JSON-native dict — the underlying storage of `Record.raw` and the
argument type for every `Attribute` dict-op (`read_from`, `write_to`, …)."""


class MissingAttributeError(KeyError):
    """Raised by `RequiredAttribute.read_from` when the value is absent or `None`."""


class Attribute[V](ABC):
    """A typed handle on one key in a `RawDict`, paired with an encode/decode codec.

    Abstract — instantiate `RequiredAttribute` or `OptionalAttribute`.

    Public attributes:

    - `name`: the wire-level dict key this attribute reads/writes.
    - `codec`: the `Codec[V]` driving encode/decode. Exposed so callers can
      compose codecs (e.g. `LIST(some_attr.codec)` lifts an attribute's
      codec into a list-of-V codec). Direct encode/decode access goes
      through `attr.codec.encode` / `attr.codec.decode`; the Attribute
      itself only surfaces dict-operating methods (`read_from`,
      `write_to`, `present_in`, `delete_from`, `get_from`, `pop_from`).
    """

    def __init__(self, name: str, codec: Codec[V]) -> None:
        self.name = name
        self.codec = codec

    def read_from(self, raw: RawDict) -> V:
        """Look up this attribute in `raw` and return the decoded value.

        Raises `MissingAttributeError` if the key is absent or the stored
        value is `None`. Both `RequiredAttribute` and `OptionalAttribute`
        share this semantic at the `__getitem__` call site — Optional's
        null-tolerance lives in `Record.get` / `Record.pop`. Subclasses
        with different read semantics (e.g. `ViewAttribute`
        synthesized for dict-spread) override this method directly rather
        than forcing `Record.__getitem__` to branch on attribute type.
        """
        v = raw.get(self.name)
        if v is None:
            raise MissingAttributeError(f"attribute {self!r} is missing")
        return self.codec.decode(v)

    @abstractmethod
    def write_to(self, raw: RawDict, value: V | None) -> None:
        """Encode `value` and store it under this attribute's name in `raw`.

        Kind-specific: `RequiredAttribute` rejects `None`,
        `OptionalAttribute` stores `None` as JSON null, `ViewAttribute`
        skips encoding entirely. This is the single point of dispatch for
        all writes — `Record.__init__` and `Record.__setitem__` both
        route through it, so new Attribute kinds can change write
        semantics without touching Record.
        """

    def present_in(self, raw: RawDict) -> bool:
        """Whether this attribute is present in `raw` (key exists, value may be `None`).

        Default checks for key existence. Subclasses override only if
        "present" means something more than "key in dict."
        """
        return self.name in raw

    def delete_from(self, raw: RawDict) -> None:
        """Remove this attribute's entry from `raw`. Raises `KeyError` if absent."""
        del raw[self.name]

    def get_from(self, raw: RawDict, default: V | None = None) -> V | None:
        """Return the decoded value, or `default` if missing or `None`.

        Default composes `read_from` with a `MissingAttributeError` catch.
        Subclasses that override `read_from` get the right `get_from`
        behavior for free.
        """
        try:
            return self.read_from(raw)
        except MissingAttributeError:
            return default

    def pop_from(self, raw: RawDict, *default: V) -> V | None:
        """Remove and return the decoded value; raise `KeyError` if missing and no default.

        A stored `None` is returned as `None` (no decode) and the key is
        removed — mirroring `dict.pop` semantics for `OptionalAttribute`
        writes of `None`. Default composes `present_in`, `read_from`, and
        `delete_from`; subclasses that override the primitives get the
        right `pop_from` for free.
        """
        if not self.present_in(raw):
            if default:
                return default[0]
            raise KeyError(self)
        try:
            v = self.read_from(raw)
        except MissingAttributeError:
            v = None  # stored as JSON null
        self.delete_from(raw)
        return v

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.name == self.name  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class OptionalAttribute[V](Attribute[V]):
    """An attribute that may be absent or `None`."""

    def write_to(self, raw: RawDict, value: V | None) -> None:
        """`None` is stored as JSON `null` (codec encoder bypassed)."""
        raw[self.name] = None if value is None else self.codec.encode(value)

    @cached_property
    def required(self) -> RequiredAttribute[V]:
        """The required view of this attribute (same name and codec).

        Use at sites where the value is known to be present (e.g. immediately
        after writing it) so `Record.__getitem__` accepts it without a checker
        downgrade.
        """
        return RequiredAttribute(self.name, self.codec)


class RequiredAttribute[V](Attribute[V]):
    """An attribute that must be present and non-`None`."""

    def write_to(self, raw: RawDict, value: V) -> None:
        """`None` is rejected at the write site so it can't land silently as `null`.

        The explicit check fires regardless of optimization level; the
        codec's type assertion would catch it under normal Python but is
        stripped under ``python -O``, so we don't rely on it for the
        Required-vs-None invariant.
        """
        if value is None:
            raise ValueError(f"cannot assign None to required {self!r}")
        raw[self.name] = self.codec.encode(value)

    @cached_property
    def optional(self) -> OptionalAttribute[V]:
        """The optional view of this attribute (same name and codec).

        Use at sites where you want `.get()` / `.pop()` semantics on a
        normally-required field (e.g. presence-checked reads, defaults).
        """
        return OptionalAttribute(self.name, self.codec)


class ViewAttribute(RequiredAttribute[Any]):
    """Synthesized view onto a `Record` key, produced by `Record.keys()`.

    Overrides `read_from` (raw passthrough, None-tolerant) and `write_to`
    (identity store, no encoding) — the spread roundtrip must enumerate
    every key including stored JSON ``null``, and the value coming back
    in is already in wire form. Same dispatch protocol as every other
    Attribute kind, just with no-op codec behavior.

    Public class but deliberately not re-exported from `fretworx.attribute`:
    application code constructs Attributes via `RequiredAttribute` /
    `OptionalAttribute`; `ViewAttribute` is a framework-internal mechanism
    that powers `Record`'s dict-spread support. Reaching it requires the
    fully-qualified import, which serves as the "you know what you're
    doing" signal in lieu of the leading underscore.
    """

    def __init__(self, name: str):
        super().__init__(name, IDENTITY)

    def read_from(self, raw: RawDict) -> Any:
        return raw[self.name]

    def write_to(self, raw: RawDict, value: Any) -> None:
        raw[self.name] = value
