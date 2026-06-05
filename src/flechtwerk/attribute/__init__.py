"""Type-safe handles on dict keys, paired with explicit encode/decode codecs."""
from .attribute import (
    Attribute,
    MissingAttributeError,
    OptionalAttribute,
    RawDict,
    RequiredAttribute,
)
from .codec import Codec, Decoder, Encoder
from .codecs import (
    BOOL,
    DATE,
    DATETIME,
    DICT,
    FLOAT,
    INT,
    LIST,
    SET,
    STR,
    TIME,
    TUPLE,
)
from .record import (
    ANY,
    RECORD,
    Record,
    record_codec,
)

__all__ = [
    "ANY",
    "Attribute",
    "BOOL",
    "Codec",
    "DATE",
    "DATETIME",
    "DICT",
    "Decoder",
    "Encoder",
    "FLOAT",
    "INT",
    "LIST",
    "MissingAttributeError",
    "OptionalAttribute",
    "RawDict",
    "RECORD",
    "Record",
    "RequiredAttribute",
    "SET",
    "STR",
    "TIME",
    "TUPLE",
    "record_codec",
]
