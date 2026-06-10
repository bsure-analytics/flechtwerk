"""Core types for the fretworx framework."""
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from fretworx.attribute import Codec, Record, record_codec


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


class Stage:
    """Common base of `Extractor` and `Transformer` — owns the config-topic declaration.

    Config topics are read in full by every instance into ONE per-process
    `ConfigStore` keyed by wire key (see `fretworx.configs`) — Kafka
    Streams' GlobalKTable pattern, specialized to configuration. For an
    Extractor they are the topics whose entries feed `poll`; a Transformer
    may declare them in addition to its partitioned `input_topics` and look
    entries up via `self.configs`. Config topics are exempt from
    co-partitioning: their partition count is unconstrained and irrelevant,
    so any producer (Kafka UI included) can write to them.
    """

    config_topics: list[str] = []

    async def enrich(self, config: Config) -> Config:
        """One-time enrichment when a config first arrives or updates.

        Applied by the framework once per config record — the startup
        bootstrap compacts first, so once per surviving entry — never per
        poll tick or per lookup. The enriched value is what the store,
        `poll`, and `Transformer.configs` lookups see. Override for e.g.
        SumUp merchant code lookup.
        """
        return config
