"""Core types for the fretworx framework."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    """A message read from Kafka."""

    key: str
    offset: int
    partition: int
    timestamp: datetime | None
    topic: str
    value: str


@dataclass(frozen=True, slots=True)
class Message:
    """A message to be written to Kafka."""

    key: str
    topic: str
    value: dict[str, Any]
    timestamp: datetime | None = None
