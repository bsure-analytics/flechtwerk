"""Every built-in codec, in one namespace.

`Attribute[V]` is constructed with a `Codec[V]`. Both directions are required:
the codec is the single source of truth for how a `V` round-trips through
JSON-native form. The `[V]` type parameter on `Attribute` is inferred from
the codec by the type checker — there's no runtime type introspection.

This module is the complete codec catalogue: the `Codec` type and its
`Decoder` / `Encoder` aliases, the atoms (`STR`, `INT`, `BOOL`, `BYTES`,
`DATE`, `FLOAT`, `DATETIME`, `TIME`, `ZONE_INFO`, `RECORD`, `ANY`), the constructors
(`LIST`, `SET`, `TUPLE`, `DICT`), and the `record_codec` factory for
`Record` subclasses. The `flechtwerk.attribute` package re-exports all of
them, so the short import works too; this module exists for the
disambiguating one, when an application name collides with a codec:

    from flechtwerk.attribute import codec

    DATE = Attribute("date", codec.DATE)

or `from flechtwerk.attribute.codec import DATE as DATE_CODEC`.

The one codec deliberately absent is `ENCRYPTED`: it lives in
`flechtwerk.secrets`, the only framework module allowed to import joserfc
(the `flechtwerk[secrets]` extra).
"""
from ._codec import (
    BOOL,
    BYTES,
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
    ZONE_INFO,
    Codec,
    Decoder,
    Encoder,
)
from .record import ANY, RECORD, record_codec

__all__ = [
    "ANY",
    "BOOL",
    "BYTES",
    "Codec",
    "DATE",
    "DATETIME",
    "DICT",
    "Decoder",
    "Encoder",
    "FLOAT",
    "INT",
    "LIST",
    "RECORD",
    "SET",
    "STR",
    "TIME",
    "TUPLE",
    "ZONE_INFO",
    "record_codec",
]
