"""The common base of `Extractor` and `Transformer`."""
from collections.abc import Callable
from typing import Self

from flechtwerk.attribute import Record

from .configs import ConfigStore
from .observer import Observer
from .types import Config, IncomingMessage, InvalidMessageError

# Framework-internal on purpose: applications build stages via `Extractor` /
# `Transformer`, never against `Stage` directly.
__all__: list[str] = []

ExtractStateKeyFn = Callable[[IncomingMessage], str]
OnInvalidMessageFn = Callable[[InvalidMessageError], Record | None]


def counting_on_invalid(on_invalid: OnInvalidMessageFn, observer: Observer) -> OnInvalidMessageFn:
    """Wrap an `on_invalid_message` handler so every invocation is counted.

    Each runner wraps its stage's handler exactly once at setup and passes
    the result to every mediated decode site — the parse loop and the config
    machinery. That is what keeps `flechtwerk.configs` observer-free and what
    makes the counter non-optional: an override that skips records cannot
    silence the telemetry, so loss lands on the scrape rather than in a log
    nobody tails.

    ``BaseException`` on purpose: whatever a handler raises, the outcome was
    "raised" and must be counted before it propagates.
    """
    def handler(error: InvalidMessageError) -> Record | None:
        try:
            substitute = on_invalid(error)
        except BaseException:
            observer.message_invalid(error.topic, "raised")
            raise
        observer.message_invalid(error.topic, "skipped" if substitute is None else "substituted")
        return substitute
    return handler


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

    def on_invalid_message(self, error: InvalidMessageError) -> Record | None:
        """Policy hook for records whose key or value cannot be decoded.

        Fires for a transformer's input records and for either stage shape's
        config records (bootstrap and drain alike) when the key is not valid
        UTF-8 or the value is not a JSON dict. Three outcomes:

        - **raise** (the default: ``raise error``) — the record crashes the
          stage. No transaction has begun and no offset advances, so the
          restart re-fetches the same record and crash-loops until the
          producer or the topic is fixed. That is deliberate: silent
          laundering of garbage is worse.
        - **return None** — skip the record. A transformer's input offset
          still advances and commits with its batch (the record is simply
          never dispatched); a config record is not applied, so the store
          keeps whatever it held (nothing, during bootstrap).
        - **return a Record** — substitute the decoded value. The call site
          assigns the semantic type: an input record proceeds as
          ``Event(record)``, a config record as ``Config(record)`` (and is
          enriched like any other). Legal for ``error.part == "value"``
          only — substituting a **key** raises ``TypeError``, because a key
          is state identity (bucketing, changelog keys, ``token_for``
          ownership) and identity must not be synthesized for a record whose
          identity is unreadable. Key failures accept raise or skip.

        Every invocation is counted (``message_invalid(topic, outcome)``,
        with outcome ``raised`` / ``skipped`` / ``substituted``) — an
        override cannot silence the telemetry. The framework logs nothing on
        any outcome: on a raise the traceback is the announcement, and a
        recovered outcome is an informed decision whose announcement belongs
        to whoever made it — log it here if you want one.

        The hook is **synchronous and must be deterministic**: substitution
        runs once per record *occurrence*, and config topics are re-read in
        full on every startup, so a handler whose output varies would build a
        store that diverges from what a fresh boot builds (the same reason
        `enrich_config` runs inside the config machinery). Prefetch anything
        a recovery needs — a schema, a lookup table — in ``__aenter__``.
        """
        raise error
