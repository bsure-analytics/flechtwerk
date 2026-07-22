"""Core types for the Flechtwerk framework."""
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from flechtwerk.attribute import Codec, Record, record_codec

__all__ = ["Config", "Event", "IncomingMessage", "Message", "Payload", "State"]


class Config(Record):
    """Configuration object read from a Kafka config topic."""


class Event(Record):
    """Event object read from or written to a Kafka data topic."""


class State(Record):
    """Mutable per-key state managed by the framework."""


CONFIG: Final[Codec[Config]] = record_codec(Config)
EVENT: Final[Codec[Event]] = record_codec(Event)
STATE: Final[Codec[State]] = record_codec(State)


Payload = bytes | str | Event
"""What a `Message` may carry as key or value — one wire encoding per member.

- ``bytes``: sent as-is; the application has already encoded it. The escape
  hatch for foreign wire formats (Avro, msgpack, JSON scalars/arrays, ...).
- ``str``: UTF-8 text, deliberately NOT JSON-quoted — a wire-format
  commitment: string keys are ``decode_key``'s exact mirror, and plain-text
  values feed foreign readers (e.g. Druid lookup tables). Quoting them would
  remap every partition and state identity.
- ``Event``: canonical JSON — compact, sorted keys — so equal records
  produce identical bytes (stable partitioning for structured keys).

``Event`` is the one Flechtwerk-schema payload: the wire carries no type,
so the reader assigns the semantics — a config topic's consumers wrap the
same bytes as ``Config`` regardless of what the producer held. Raw dicts
are rejected: wrap them in ``Event.wrap(d)`` for identical wire bytes plus
codec validation. ``State`` and ``Config`` are excluded on purpose — a
``State`` inside a ``Message`` would be *emitted*, not persisted (yield it
bare to persist it), and a ``Config`` travels as data (wrap it in
``Event(config)``); the explicit conversion marks the semantic handoff.
"""


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    """A message read from Kafka."""

    key: str
    offset: int
    partition: int
    timestamp: datetime | None
    topic: str
    value: Event


@dataclass(frozen=True, slots=True)
class Message:
    """A message to be written to Kafka.

    ``key`` and ``value`` each accept any `Payload` — see its docs for the
    encoding rules and for how to express other shapes. Construction
    validates both fields so a mistake fails at the yield site, not inside
    the runner's transactional send path.
    """

    key: Payload
    topic: str
    value: Payload
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("key", "value"):
            v = getattr(self, name)
            if isinstance(v, (bytes, str, Event)):
                continue
            if isinstance(v, Config):
                raise TypeError(
                    f"Message.{name} must not be a Config: on the wire it travels as"
                    " data, and the reader assigns the semantics. Wrap it in"
                    " Event(config) — identical bytes — to mark the handoff."
                )
            if isinstance(v, State):
                raise TypeError(
                    f"Message.{name} must not be a State: a State inside a Message is"
                    " emitted, not persisted. Yield the State bare to persist it, or"
                    " wrap it in Event(state) to emit its contents."
                )
            if isinstance(v, dict):
                raise TypeError(
                    f"Message.{name} must not be a raw dict: wrap it in Event.wrap(...)"
                    " for identical wire bytes plus codec validation."
                )
            raise TypeError(
                f"Message.{name} must be bytes | str | Event, got"
                f" {type(v).__name__}. Encode other shapes to bytes yourself."
            )
