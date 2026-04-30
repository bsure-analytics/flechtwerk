"""Type-safe handles on dict keys with paired encode/decode codecs."""
from .attribute import (
    Attribute,
    OptionalAttribute,
    RequiredAttribute,
)
from .record import Record, LIST_OF_RECORDS, MissingAttributeError
from .registry import Codec, Decoder, Encoder
from . import codecs  # noqa: F401  — populates the registry as a side-effect

__all__ = [
    "Attribute",
    "Codec",
    "Decoder",
    "Record",
    "Encoder",
    "LIST_OF_RECORDS",
    "MissingAttributeError",
    "OptionalAttribute",
    "RequiredAttribute",
]
