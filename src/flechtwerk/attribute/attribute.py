"""Type-safe handles on keys in a dict, with a paired encode/decode codec.

`Attribute(name, codec)` declares a required field; `Attribute(name, codec,
optional=True)` declares one that may be absent or `None`. The keyword-only
`optional` flag drives exactly one write-side guard: a required attribute
rejects `None` writes (so a missing value can't land silently as JSON
`null`), an optional attribute stores `None` as `null`. Read semantics are
identical for both kinds and are carried by the *method*, not the flag —
`Record[attr]` reads-or-raises (returns `V`), `Record.get` / `Record.pop`
tolerate absence (return `V | None`). The `required` property is the
inverse of `optional`, for the few sites that branch on mandatory-ness
(e.g. the auditor's missing-field check).

Each `Attribute` carries a `Codec[V]` that drives both the static type
parameter and the runtime encode/decode. The type checker infers the
`[V]` from the codec — `Attribute("name", STR)` produces an
`Attribute[str]` without an explicit subscript. Built-in codecs are
exported from `fretworx.attribute` (`STR`, `INT`, `DATETIME`, `RECORD`,
`LIST(RECORD)`, …).
"""
from typing import Any

from .codec import Codec
from .codecs import IDENTITY

type RawDict = dict[str, Any]
"""Wire-form JSON-native dict — the underlying storage of `Record.raw` and the
argument type for every `Attribute` dict-op (`read_from`, `write_to`, …)."""


class MissingAttributeError(KeyError):
    """Raised by `Attribute.read_from` when the value is absent or `None`."""


class Attribute[V]:
    """A typed handle on one key in a `RawDict`, paired with an encode/decode codec.

    `optional=False` (the default) declares a required field whose
    `write_to` rejects `None`; `optional=True` allows `None` (stored as
    JSON `null`). The read methods are kind-agnostic — which presence
    semantic applies is chosen by the caller's method (`[]` vs
    `.get` / `.pop`), not by this flag.

    Public attributes:

    - `name`: the wire-level dict key this attribute reads/writes.
    - `codec`: the `Codec[V]` driving encode/decode. Exposed so callers can
      compose codecs (e.g. `LIST(some_attr.codec)` lifts an attribute's
      codec into a list-of-V codec). Direct encode/decode access goes
      through `attr.codec.encode` / `attr.codec.decode`; the Attribute
      itself only surfaces dict-operating methods (`read_from`,
      `write_to`, `present_in`, `delete_from`, `get_from`, `pop_from`).
    - `optional`: whether `None` / absence is permitted on write.
    """

    def __init__(self, name: str, codec: Codec[V], *, optional: bool = False) -> None:
        self.name = name
        self.codec = codec
        self.optional = optional

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Reject subclasses outside this module — the Attribute hierarchy is sealed."""
        super().__init_subclass__(**kwargs)
        if cls.__module__ != Attribute.__module__:
            raise TypeError(f"{cls.__qualname__} cannot extend the sealed Attribute hierarchy")

    @property
    def required(self) -> bool:
        """Whether this attribute must be present and non-`None` — the inverse of `optional`."""
        return not self.optional

    def read_from(self, raw: RawDict) -> V:
        """Look up this attribute in `raw` and return the decoded value.

        Raises `MissingAttributeError` if the key is absent or the stored
        value is `None`. This is kind-agnostic — a `[]` read raises on
        absence regardless of `optional`; absence-tolerance lives in
        `Record.get` / `Record.pop`.
        """
        # Null ≡ missing is duplicated in get_from and pop_from — change all in lockstep.
        v = raw.get(self.name)
        if v is None:
            raise MissingAttributeError(f"attribute {self!r} is missing")
        return self.codec.decode(v)

    def write_to(self, raw: RawDict, value: V | None) -> None:
        """Encode `value` and store it under this attribute's name in `raw`.

        A required attribute (`optional=False`) rejects `None` at the
        write-site so it can't land silently as JSON `null`; an optional
        attribute stores `None` as `null`. The `not self.optional` check
        rides the `None` branch only, so the common non-`None` write pays
        nothing for it.
        """
        if value is None:
            if not self.optional:
                raise ValueError(f"cannot assign None to required {self!r}")
            raw[self.name] = None
        else:
            raw[self.name] = self.codec.encode(value)

    def present_in(self, raw: RawDict) -> bool:
        """Whether this attribute is present in `raw` (key exists, value may be `None`)."""
        return self.name in raw

    def delete_from(self, raw: RawDict) -> None:
        """Remove this attribute's entry from `raw`. Raises `KeyError` if absent."""
        del raw[self.name]

    def get_from(self, raw: RawDict, default: V | None = None) -> V | None:
        """Return the decoded value, or `default` if missing or `None`."""
        v = raw.get(self.name)
        return self.codec.decode(v) if v is not None else default

    def pop_from(self, raw: RawDict, *default: V) -> V | None:
        """Remove and return the decoded value; raise `KeyError` if missing and no default.

        A stored `None` is returned as `None` (no decode), and the key is
        removed — mirroring `dict.pop` semantics for `None` writes.
        """
        if not self.present_in(raw):
            if default:
                return default[0]
            raise KeyError(self)
        v = raw.get(self.name)
        if v is not None:
            v = self.codec.decode(v)
        self.delete_from(raw)
        return v

    # Identity is (type, name) — never the codec or `optional`. The name is the
    # wire key this handle addresses, so two handles for the same slot are equal.
    # The codec is excluded on purpose: codecs compare by object identity, so a
    # composite codec like LIST(STR) is rebuilt per call and would make
    # "same field" attributes unequal. `type` is in the key so a ViewAttribute
    # stays distinct from a plain Attribute of the same name — the dict-spread
    # override relies on both write_to calls running, the typed one landing last.
    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.name == self.name  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self) -> str:
        kind = ", optional=True" if self.optional else ""
        return f"{type(self).__name__}({self.name!r}{kind})"


class ViewAttribute(Attribute[Any]):
    """Synthesized view onto a `Record` key, produced by `Record.keys()`.

    Overrides `read_from` (raw passthrough, None-tolerant) and `write_to`
    (identity store, no encoding) — the spread roundtrip must list
    every key including stored JSON "null", and the value coming back
    in is already in wire form. The same dispatch protocol as every other
    Attribute, just with no-op codec behavior.

    Public class but deliberately not re-exported from `fretworx.attribute`:
    application code constructs handles via `Attribute(...)`; `ViewAttribute`
    is a framework-internal mechanism that powers `Record`'s dict-spread
    support. Reaching it requires the fully qualified import, which serves
    as the "you know what you're doing" signal in lieu of the leading
    underscore.
    """

    def __init__(self, name: str):
        super().__init__(name, IDENTITY)

    def read_from(self, raw: RawDict) -> Any:
        return raw[self.name]

    def write_to(self, raw: RawDict, value: Any) -> None:
        raw[self.name] = value
