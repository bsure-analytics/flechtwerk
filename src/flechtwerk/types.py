"""Core types for the Flechtwerk framework."""
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from flechtwerk.attribute import Codec, Record, record_codec


class Config(Record):
    """Configuration object read from a Kafka config topic."""


class Event(Record):
    """Event object read from or written to a Kafka data topic."""


class State(Record):
    """Mutable per-key state managed by the framework."""


CONFIG: Final[Codec[Config]] = record_codec(Config)
EVENT: Final[Codec[Event]] = record_codec(Event)
STATE: Final[Codec[State]] = record_codec(State)


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
    """A message to be written to Kafka."""

    key: str
    topic: str
    value: Event
    timestamp: datetime | None = None
