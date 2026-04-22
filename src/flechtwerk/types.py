"""Core types for the fretworx framework."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any


class Config(dict[str, Any]):
    """Configuration object read from a Kafka config topic."""
    pass


class Event(dict[str, Any]):
    """Event object read from or written to a Kafka data topic."""
    pass


class State(dict[str, Any]):
    """Mutable per-key state managed by the framework."""
    pass


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
