"""Type-safe handles on dict keys with paired encode/decode codecs."""
from .attribute import (
    Attribute,
    OptionalAttribute,
    RequiredAttribute,
)
from .dict import Dict, LIST_OF_DICTS, MissingAttributeError
from .registry import Codec, Decoder, Encoder
from . import codecs  # noqa: F401  — populates the registry as a side-effect

__all__ = [
    "Attribute",
    "Codec",
    "Decoder",
    "Dict",
    "Encoder",
    "LIST_OF_DICTS",
    "MissingAttributeError",
    "OptionalAttribute",
    "RequiredAttribute",
]
