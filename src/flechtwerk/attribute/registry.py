"""Type-keyed codec registry.

`Attribute[V]` looks up an encode/decode pair by `V` at first use. Codecs are
registered via the `@encoder(T)` / `@decoder(T)` decorators — each direction
independently — and the lookup raises if a codec is missing.

The `dict` and `list` encoders are recursive walkers: they apply registered
encoders to leaf values, raising on any unknown type. This keeps `Dict.raw`
JSON-native by construction, which means `json.dumps(state.raw)` doesn't need
a `default=` callback — every conversion happens at write time, before the
value lands in `raw`.
"""
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")

_decoders: dict[type, Callable[[Any], Any]] = {}
_encoders: dict[type, Callable[[Any], Any]] = {}


class CodecError(LookupError):
    """Raised on missing or duplicate codec registration."""


def decoder(t: type) -> Callable[[Callable[[Any], T]], Callable[[Any], T]]:
    """Register `fn` as the decoder for `t`. Raises if one is already registered."""
    def register(fn: Callable[[Any], T]) -> Callable[[Any], T]:
        if t in _decoders:
            raise CodecError(f"decoder for {t!r} already registered")
        _decoders[t] = fn
        return fn
    return register


def encoder(t: type) -> Callable[[Callable[[T], Any]], Callable[[T], Any]]:
    """Register `fn` as the encoder for `t`. Raises if one is already registered."""
    def register(fn: Callable[[T], Any]) -> Callable[[T], Any]:
        if t in _encoders:
            raise CodecError(f"encoder for {t!r} already registered")
        _encoders[t] = fn
        return fn
    return register


def lookup_decoder(t: type) -> Callable[[Any], Any]:
    try:
        return _decoders[t]
    except KeyError:
        raise CodecError(f"no decoder registered for {t!r}") from None


def lookup_encoder(t: type) -> Callable[[Any], Any]:
    try:
        return _encoders[t]
    except KeyError:
        raise CodecError(f"no encoder registered for {t!r}") from None
