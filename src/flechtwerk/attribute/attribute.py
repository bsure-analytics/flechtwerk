"""Type-safe handles on keys in a dict, with paired encode/decode codecs.

`Attribute` is abstract — instantiate `RequiredAttribute` or
`OptionalAttribute` to declare schema intent. `Dict.__getitem__` only
accepts `RequiredAttribute`; `Dict.get` and `Dict.pop` only accept
`OptionalAttribute`.

The `[V]` subscript is **required** — bare `RequiredAttribute("name")` raises
on first decode/encode access. `[Any]` is also rejected — `Any` carries no
information about the value's shape, so it can't drive a meaningful codec.

Codecs are looked up in the type-keyed registry (`fretworx/attribute/registry.py`)
on first decode/encode access. Built-in codecs cover the JSON-native primitives,
`set` ⇄ sorted `list`, `tuple` ⇄ `list`, `datetime` ⇄ ISO 8601, and any `Dict`
subclass via `Dict.__init_subclass__`. Asymmetric or domain-specific codecs are
registered with `@encoder(T)` / `@decoder(T)` decorators in `codecs.py` (or
wherever `T` is defined).
"""
from collections.abc import Callable
from functools import cached_property
from typing import Any, Generic, TypeVar, get_args, get_origin

from .registry import lookup_decoder, lookup_encoder

V = TypeVar("V")


class Attribute(Generic[V]):
    """A typed handle on one key in a `dict[str, Any]`, paired with an encode/decode codec.

    Abstract — direct instantiation is rejected; use `RequiredAttribute` or
    `OptionalAttribute`.
    """

    def __init__(self, name: str) -> None:
        if type(self) is Attribute:
            raise TypeError(
                "Attribute is abstract; instantiate RequiredAttribute or OptionalAttribute"
            )
        self.name = name

    @cached_property
    def decode(self) -> Callable[[Any], V]:
        return lookup_decoder(self._required_value_type)

    @cached_property
    def encode(self) -> Callable[[V], Any]:
        return lookup_encoder(self._required_value_type)

    @property
    def _required_value_type(self) -> type[V]:
        orig = getattr(self, "__orig_class__", None)
        if orig is not None:
            args = get_args(orig)
            if args and args[0] is Any:
                raise TypeError(
                    f"{type(self).__name__}({self.name!r}) uses [Any], which is "
                    "not meaningful for an Attribute; pick a concrete type"
                )
        t = self._value_type
        if t is None:
            raise TypeError(
                f"{type(self).__name__}({self.name!r}) is missing its [V] type "
                "parameter; subscript with a concrete type, e.g. "
                f"{type(self).__name__}[str]({self.name!r})"
            )
        return t  # type: ignore[return-value]

    @property
    def _value_type(self) -> type | None:
        """The runtime type extracted from the `[V]` subscript, or `None` if unsubscripted."""
        orig = getattr(self, "__orig_class__", None)
        if orig is None:
            return None
        args = get_args(orig)
        if not args:
            return None
        v = args[0]
        if isinstance(v, type):
            return v
        # Generic alias like `dict[str, Any]` — extract the bare origin class.
        origin = get_origin(v)
        return origin if isinstance(origin, type) else None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class OptionalAttribute(Attribute[V]):
    """An attribute that may be absent or `None`."""


class RequiredAttribute(Attribute[V]):
    """An attribute that must be present and non-`None`."""
