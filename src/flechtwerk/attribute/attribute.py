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

from .codec import Codec, Decoder, Encoder


class Attribute[V](ABC):
    """A typed handle on one key in a `dict[str, Any]`, paired with an encode/decode codec.

    Abstract — instantiate `RequiredAttribute` or `OptionalAttribute`.

    Public attributes:

    - `name`: the wire-level dict key this attribute reads/writes.
    - `codec`: the `Codec[V]` driving encode/decode. Exposed so callers can
      compose codecs (e.g. `LIST(some_attr.codec)` lifts an attribute's
      codec into a list-of-V codec).
    - `decode` / `encode`: convenience properties delegating to
      `codec.decode` / `codec.encode`.
    """

    def __init__(self, name: str, codec: Codec[V]) -> None:
        self.name = name
        self.codec = codec

    @property
    def decode(self) -> Decoder[V]:
        return self.codec.decode

    @property
    def encode(self) -> Encoder[V]:
        return self.codec.encode

    @abstractmethod
    def encode_or_null(self, value: V | None) -> Any:
        """Encode `value` for storage in `Record.raw`.

        `None` handling is type-specific — see the subclass overrides.
        For non-`None` values both subclasses run the codec encoder and
        reject a `None` result as a codec bug (`_encoded_non_null`).
        """

    def _encoded_non_null(self, value: V) -> Any:
        encoded = self.encode(value)
        if encoded is None:
            raise ValueError(f"encoder for {self!r} returned None")
        return encoded

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.name == self.name  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class OptionalAttribute[V](Attribute[V]):
    """An attribute that may be absent or `None`."""

    def encode_or_null(self, value: V | None) -> Any:
        """`None` is stored as JSON `null` (codec encoder bypassed)."""
        return None if value is None else self._encoded_non_null(value)

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

    def encode_or_null(self, value: V) -> Any:
        """`None` is rejected at the write site so it can't land silently as `null`."""
        if value is None:
            raise ValueError(f"cannot assign None to required {self!r}")
        return self._encoded_non_null(value)

    @cached_property
    def optional(self) -> OptionalAttribute[V]:
        """The optional view of this attribute (same name and codec).

        Use at sites where you want `.get()` / `.pop()` semantics on a
        normally-required field (e.g. presence-checked reads, defaults).
        """
        return OptionalAttribute(self.name, self.codec)
