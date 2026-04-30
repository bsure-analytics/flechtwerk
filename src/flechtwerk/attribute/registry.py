"""Type-keyed codec registry.

`Attribute[V]` looks up an encode/decode pair by `V` at first use. Codecs are
registered via the `@encoder(T)` / `@decoder(T)` decorators — each direction
independently — and the lookup raises if a codec is missing.

The `dict` and `list` encoders are recursive walkers: they apply registered
encoders to leaf values, raising on any unknown type. This keeps `Record.raw`
JSON-native by construction, which means `json.dumps(state.raw)` doesn't need
a `default=` callback — every conversion happens at write-time, before the
value lands in `raw`.
"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

type Decoder[V] = Callable[[Any], V]
"""A function that decodes a wire value to a Python value of type `V`."""

type Encoder[V] = Callable[[V], Any]
"""A function that encodes a Python value of type `V` to a JSON-native wire value."""


@dataclass(frozen=True, slots=True)
class Codec[V]:
    """A pair of `(decode, encode)` callables overriding the registry default.

    Either field may be `None` to keep that direction on the registry
    default. Pass to `Attribute` as the `codec` argument.
    """
    decode: Decoder[V] | None = None
    encode: Encoder[V] | None = None


_decoders: dict[type, Decoder[Any]] = {}
_encoders: dict[type, Encoder[Any]] = {}


class CodecError(LookupError):
    """Raised on missing or duplicate codec registration."""


def decoder[T](t: type[T]) -> Callable[[Decoder[T]], Decoder[T]]:
    """Register `fn` as the decoder for `t`. Raises if one is already registered."""

    def register(fn: Decoder[T]) -> Decoder[T]:
        if t in _decoders:
            raise CodecError(f"decoder for {t!r} already registered")
        _decoders[t] = fn
        return fn

    return register


def encoder[T](t: type[T]) -> Callable[[Encoder[T]], Encoder[T]]:
    """Register `fn` as the encoder for `t`. Raises if one is already registered."""

    def register(fn: Encoder[T]) -> Encoder[T]:
        if t in _encoders:
            raise CodecError(f"encoder for {t!r} already registered")
        _encoders[t] = fn
        return fn

    return register


def lookup_decoder[T](t: type[T]) -> Decoder[T]:
    try:
        return _decoders[t]
    except KeyError:
        raise CodecError(f"no decoder registered for {t!r}") from None


def lookup_encoder[T](t: type[T]) -> Encoder[T]:
    try:
        return _encoders[t]
    except KeyError:
        raise CodecError(f"no encoder registered for {t!r}") from None
