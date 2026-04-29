"""Type-safe handles on dict keys with paired encode/decode codecs."""
from .attribute import (
    Attribute,
    OptionalAttribute,
    RequiredAttribute,
)
from .dict import Dict, MissingAttributeError
from . import codecs  # noqa: F401  — populates the registry as a side-effect

__all__ = [
    "Attribute",
    "Dict",
    "MissingAttributeError",
    "OptionalAttribute",
    "RequiredAttribute",
]
