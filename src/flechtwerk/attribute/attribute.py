"""Type-safe handles on keys in a dict, with paired encode/decode codecs.

`Attribute` is abstract — instantiate `RequiredAttribute` or
`OptionalAttribute` to declare schema intent. `Dict.__getitem__` only
accepts `RequiredAttribute`; `Dict.get` and `Dict.pop` only accept
`OptionalAttribute`. `OptionalAttribute[V].required` and
`RequiredAttribute[V].optional` are `cached_property`s returning the
other-kind view of the same attribute (same name and `[V]`); use them at
sites where the runtime presence semantic doesn't match the declared
schema kind (e.g. `state[OPT.required]` immediately after writing).

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
from functools import cached_property
from typing import Any, Generic, TypeVar, get_args, get_origin

from .registry import Codec, Decoder, Encoder, lookup_decoder, lookup_encoder

V = TypeVar("V")


class Attribute(Generic[V]):
    """A typed handle on one key in a `dict[str, Any]`, paired with an encode/decode codec.

    Abstract — direct instantiation is rejected; use `RequiredAttribute` or
    `OptionalAttribute`.

    Codecs default to the registry lookup keyed on `V`. The `codec` keyword
    argument overrides either or both directions for this attribute alone —
    useful when two attributes share a Python value type but need different
    wire formats (e.g. one `[datetime]` field encoded as epoch millis,
    another as ISO 8601). When both directions are overridden, the `[V]`
    subscript is no longer strictly required by the codec lookup (it still
    drives the static type of `event[ATTR]`).
    """

    def __init__(self, name: str, *, codec: Codec[V] = Codec()) -> None:
        if type(self) is Attribute:
            raise TypeError(
                "Attribute is abstract; instantiate RequiredAttribute or OptionalAttribute"
            )
        self.name = name
        # Override the cached_property by pre-filling its slot in `__dict__`.
        # The descriptor's `__get__` checks `__dict__` first, so we just hand
        # it the answer up front and it never runs the lookup.
        if codec.decode is not None:
            self.decode = codec.decode
        if codec.encode is not None:
            self.encode = codec.encode

    @cached_property
    def decode(self) -> Decoder[V]:
        return lookup_decoder(self._required_value_type)

    @cached_property
    def encode(self) -> Encoder[V]:
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

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.name == self.name  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), self.name))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"


class OptionalAttribute(Attribute[V]):
    """An attribute that may be absent or `None`."""

    @cached_property
    def required(self) -> RequiredAttribute[V]:
        """The required view of this attribute (same name, `[V]`, and resolved codecs).

        Use at sites where the value is known to be present (e.g. immediately
        after writing it) so `Dict.__getitem__` accepts it without a checker
        downgrade.
        """
        new: RequiredAttribute[V] = RequiredAttribute(
            self.name,
            codec=Codec(decode=self.decode, encode=self.encode),
        )
        _copy_value_type(self, new, RequiredAttribute)
        return new


class RequiredAttribute(Attribute[V]):
    """An attribute that must be present and non-`None`."""

    @cached_property
    def optional(self) -> OptionalAttribute[V]:
        """The optional view of this attribute (same name, `[V]`, and resolved codecs).

        Use at sites where you want `.get()` / `.pop()` semantics on a
        normally-required field (e.g. presence-checked reads, defaults).
        """
        new: OptionalAttribute[V] = OptionalAttribute(
            self.name,
            codec=Codec(decode=self.decode, encode=self.encode),
        )
        _copy_value_type(self, new, OptionalAttribute)
        return new


def _copy_value_type(src: Attribute, dst: Attribute, dst_cls: type[Attribute]) -> None:
    """Carry the `[V]` parametrization from `src` to `dst`.

    `Attribute._value_type` reads off the instance's `__orig_class__`, which
    `_GenericAlias.__call__` sets when `Foo[T]("name")` is instantiated.
    A plain `Foo("name")` doesn't have one, so we synthesize the equivalent
    alias on `dst`. If `src` itself has no `[V]` (a framework-level error
    already), `dst` inherits the same problem and raises identically on first
    decode/encode access.
    """
    orig = getattr(src, "__orig_class__", None)
    if orig is not None:
        args = get_args(orig)
        if args:
            dst.__orig_class__ = dst_cls[args[0]]  # type: ignore[attr-defined]
