"""Core types for the fretworx framework."""
from dataclasses import dataclass
from datetime import datetime

from fretworx.attribute import Dict


class Config(Dict):
    """Configuration object read from a Kafka config topic."""


class Event(Dict):
    """Event object read from or written to a Kafka data topic."""


class State(Dict):
    """Mutable per-key state managed by the framework."""


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
