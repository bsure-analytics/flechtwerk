"""The `Codec[V]` dataclass — paired encode/decode for a Python value of type `V`.

`Attribute[V]` is constructed with a `Codec[V]`. Both directions are required:
the codec is the single source of truth for how a `V` round-trips through
JSON-native form. The `[V]` type parameter on `Attribute` is inferred from
the codec by the type checker — there's no runtime type introspection.

Built-in codec constants (`STR`, `INT`, `DATETIME`, `RECORD`, …) live in
`codecs.py`.
"""
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

type Decoder[V] = Callable[[Any], V]
"""A function that decodes a JSON-native wire value to a Python value of type `V`."""

type Encoder[V] = Callable[[V], Any]
"""A function that encodes a Python value of type `V` to a JSON-native wire value."""


@dataclass(frozen=True, slots=True)
class Codec[V]:
    """A pair of `(decode, encode)` callables for a value of type `V`.

    Both directions are required — there is no fallback registry. The value
    type `V` is the single source of truth: pass a `Codec[V]` to an
    `Attribute` and the type checker infers the `Attribute[V]` parameter.
    """
    decode: Decoder[V]
    encode: Encoder[V]
