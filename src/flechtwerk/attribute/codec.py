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

__all__ = ["Codec", "Decoder", "Encoder"]

type Decoder[V] = Callable[[Any], V]
"""A function that decodes a JSON-native wire value to a Python value of type `V`."""

type Encoder[V] = Callable[[V], Any]
"""A function that encodes a Python value of type `V` to a JSON-native wire value."""


@dataclass(frozen=True, slots=True, eq=False)
class Codec[V]:
    """A pair of `(decode, encode)` callables for a value of type `V`.

    Both directions are required — there is no fallback registry. The value
    type `V` is the single source of truth: pass a `Codec[V]` to an
    `Attribute` and the type checker infers the `Attribute[V]` parameter.

    Equality is identity (`eq=False`): the fields are functions, so a
    generated field-wise `__eq__` would compare them by object identity
    anyway while *looking* like value equality — composite codecs like
    `LIST(STR)` rebuild fresh closures per call and would never compare
    equal. Identity is the only honest contract, and `Attribute` already
    excludes the codec from its own equality for this reason.
    """
    decode: Decoder[V]
    encode: Encoder[V]
