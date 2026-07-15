"""The common base of `Extractor` and `Transformer`."""
from collections.abc import Callable
from typing import Self

from .configs import ConfigStore
from .types import Config, IncomingMessage

ExtractStateKeyFn = Callable[[IncomingMessage], str]


class Stage:
    """Common base of `Extractor` and `Transformer` — owns the config-topic declaration.

    Config topics are read in full by every instance into ONE per-process
    `ConfigStore` keyed by wire key (see `flechtwerk.configs`) — Kafka
    Streams' GlobalKTable pattern, specialized to configuration. For an
    Extractor they are the topics whose entries feed `poll`; a Transformer
    may declare them in addition to its partitioned `input_topics`. Either
    stage shape may look any entry up via `self.configs`. Config topics are
    exempt from co-partitioning: their partition count is unconstrained and
    irrelevant, so any producer (Kafka UI included) can write to them. The
    one exception is an extractor's own config topics, which must share one
    partition count — the token space for the ownership leases that shard
    configs across its replicas (see `flechtwerk.extractor`); placement
    stays irrelevant even there.
    """

    config_topics: list[str] = []

    configs: ConfigStore
    """The stage's per-process config store, injected by the runner before ``__aenter__``.

    Keyed by wire key, merged across all declared ``config_topics`` — the
    GLOBAL view, regardless of scale-out: an extractor's ``poll`` is
    invoked only for the configs its replica owns, but ``self.configs``
    still reaches every entry (cross-config lookups). Lookups are
    eventually consistent and NOT part of any task transaction (Kafka
    Streams' GlobalKTable caveat). How fresh updates land depends on the
    runner: a transformer sees one consistent snapshot per batch, while an
    extractor's store is drained continuously by the runner's main loop —
    two lookups inside one long ``poll`` may straddle an update, so
    re-read per lookup only what you can afford to see move. For an
    extractor the store is fully populated only after the startup
    bootstrap — during ``__aenter__`` it is still empty. Treat it as
    **read-only** — look entries up with ``configs.get(key)``; mutating the
    store (``put``/``delete``) from stage code is an error (see
    `ConfigStore`). Tests seed this directly::

        stage.configs = ConfigStore.of({key: config})
    """

    async def __aenter__(self) -> Self:
        """Default lifecycle: no-op. The runner enters the stage before
        processing starts and exits it on shutdown — override both methods
        to acquire and release resources (HTTP clients, connections)."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    async def enrich_config(self, config: Config) -> Config:
        """One-time enrichment when a config first arrives or updates.

        Applied by the framework once per config record — the startup
        bootstrap compacts first, so once per surviving entry — never per
        poll tick or per lookup. The enriched value is what the store,
        `poll`, and `configs` lookups see. Override for e.g. resolving an
        account name from an API key.
        """
        return config

    def extract_state_key(self, msg: IncomingMessage) -> str:
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
