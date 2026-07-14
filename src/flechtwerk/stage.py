"""The common base of `Extractor` and `Transformer`."""
from collections.abc import Callable
from typing import Self

from .types import Config, IncomingMessage

ExtractKeyFn = Callable[[IncomingMessage], str]


class Stage:
    """Common base of `Extractor` and `Transformer` — owns the config-topic declaration.

    Config topics are read in full by every instance into ONE per-process
    `ConfigStore` keyed by wire key (see `flechtwerk.configs`) — Kafka
    Streams' GlobalKTable pattern, specialized to configuration. For an
    Extractor they are the topics whose entries feed `poll`; a Transformer
    may declare them in addition to its partitioned `input_topics` and look
    entries up via `self.configs`. Config topics are exempt from
    co-partitioning: their partition count is unconstrained and irrelevant,
    so any producer (Kafka UI included) can write to them.
    """

    config_topics: list[str] = []

    async def __aenter__(self) -> Self:
        """Default lifecycle: no-op. The runner enters the stage before
        processing starts and exits it on shutdown — override both methods
        to acquire and release resources (HTTP clients, connections)."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    async def enrich(self, config: Config) -> Config:
        """One-time enrichment when a config first arrives or updates.

        Applied by the framework once per config record — the startup
        bootstrap compacts first, so once per surviving entry — never per
        poll tick or per lookup. The enriched value is what the store,
        `poll`, and `Transformer.configs` lookups see. Override for e.g.
        resolving an account name from an API key.
        """
        return config

    def extract_key(self, msg: IncomingMessage) -> str:
        """Extract the state key from the incoming message. Default: msg.key.

        For an Extractor the message is the config record; for a Transformer
        it is the input record. The default is the Kafka message key, which
        typically carries the operator-facing identity (e.g. a tenant or
        channel ID) — for an Extractor this is stable across credential
        rotations, so rotating an API key via a new config message preserves
        the state entry. Override only if the operator-facing identity
        doesn't match the desired state namespace.
        """
        return msg.key
