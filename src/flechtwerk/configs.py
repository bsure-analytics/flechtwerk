"""Config topics — shared lookup tables read in full by every instance.

This is Kafka Streams' GlobalKTable pattern, specialized to what stages
actually share: configuration. Config topics are consumed with no consumer
group across *all* their partitions and compacted by wire key into ONE
per-process `ConfigStore` — a single key namespace regardless of how many
config topics a stage declares, matching what the extractor runner's config
dict has always done. Partition placement on a config topic is therefore
irrelevant — any task on any instance finds any key — which is exactly what
partitioned task state cannot offer (see the Co-Partitioning Trap section
in CLAUDE.md).

The source topics are their own changelog: no separate changelog topic, no
committed offsets, a full re-read on every startup. They must be compacted
and stay small (the whole store lives in memory per instance). Lookups are
eventually consistent — config updates are NOT part of any task
transaction, matching the GlobalKTable caveat.

`Stage.enrich_config` is applied here, once per config record — the startup
bootstrap compacts first, so once per *surviving* entry — never per poll
tick or per lookup. Kafka Streams forbids transforming records on their way
into a global store (KIP-813) because a checkpoint-based restore would
bypass the transformation; Flechtwerk re-reads the topics through this same
enrich_config path on every startup, so the enriched store cannot diverge from
what a fresh boot would build.

Decoding is strict and mediated by `Stage.on_invalid_message` for exactly
the same reason: an undecodable key or value is a policy question the
application answers (crash — the default — skip, or substitute), and the
answer must be deterministic, because every boot re-reads the topics through
this path. Compaction decodes keys first, so a stale record's undecodable
*value* never reaches the handler — decode-once follows enrich-once.
"""
import logging
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import TYPE_CHECKING, Any

import aiokafka
from aiokafka import ConsumerRecord

from flechtwerk.attribute import Record
from .kafka import (
    decode_event_mediated,
    decode_key_mediated,
    decode_record,
    encode_json,
    is_tombstone,
    millis_to_datetime,
    read_to_end,
    topic_partitions,
)
from .types import Config, Event, IncomingMessage

if TYPE_CHECKING:
    # Quoted below: importing the alias at runtime would close a
    # stage → configs → stage cycle (see `flechtwerk.kafka`).
    from .stage import OnInvalidMessageFn

log = logging.getLogger(__name__)

__all__ = ["ConfigStore"]

EnrichConfigFn = Callable[[Config], Awaitable[Config]]


class ConfigStore:
    """Latest config per wire key, merged across a stage's config topics.

    Values are kept as wire bytes and parsed on every `get()` — each call
    returns a fresh `Config` (a protective copy by construction).

    From a stage's perspective the store is **read-only**: query it with
    `get()` (and ``in`` / ``len``). `put`/`delete` exist for the config
    machinery alone — calling them, or otherwise mutating the store, from
    application code is an error. The store is a projection of the config
    topics, fed exclusively by `bootstrap_config_store` /
    `drain_config_updates`; a stage-side write never reaches Kafka (see the
    "config topics never participate in a Kafka transaction" invariant),
    corrupts only this instance, and is silently reverted on the next record
    for the key or on the next restart.
    """

    def __init__(self) -> None:
        self._raw: dict[str, bytes] = {}

    def __contains__(self, key: str) -> bool:
        return key in self._raw

    def __len__(self) -> int:
        return len(self._raw)

    @classmethod
    def of(cls, entries: dict[str, Record]) -> "ConfigStore":
        """Build a pre-seeded store — the test-side entry point."""
        store = cls()
        store._raw = {key: encode_json(value) for key, value in entries.items()}
        return store

    def get(self, key: str) -> Config | None:
        """Return the latest config for ``key``, or None if absent.

        The store only ever holds `encode_json` output — `put` re-encodes
        every value — so a malformed value is impossible by construction. If
        one appears anyway it is a framework bug, and the ``ValueError`` from
        the strict decode crashes rather than laundering it into an empty
        `Config`; an application's policy hook (`Stage.on_invalid_message`)
        already ran on the way in.
        """
        raw = self._raw.get(key)
        return None if raw is None else decode_record(raw, Config)

    def put(self, key: str, config: Record) -> None:
        self._raw[key] = encode_json(config)

    def delete(self, key: str) -> None:
        self._raw.pop(key, None)


async def apply_config_record(
    msg: ConsumerRecord[Any, Any],
    store: ConfigStore,
    enrich_config: EnrichConfigFn,
    on_invalid: "OnInvalidMessageFn",
) -> IncomingMessage | None:
    """Apply one config record: tombstones delete, values are enriched then stored.

    Returns the decoded record so callers never re-parse it, or ``None`` when
    ``on_invalid`` skipped it — in which case the store is left untouched
    (the key stays absent during a bootstrap, keeps its previous value during
    a drain). The returned value is post-substitution but PRE-enrichment: the
    enriched config is what the store holds, and callers that want it read it
    back with `ConfigStore.get`. A tombstone's value is ``Event.wrap({})``.
    """
    key = decode_key_mediated(msg, on_invalid)
    if key is None:
        return None
    if not key:
        log.warning("Config record without a key at %s/%d — the store is keyed by wire key",
                    msg.topic, msg.offset)
    # Tombstone check BEFORE the value decode: a tombstone's value is raw
    # emptiness by definition, never garbage, so it must not reach the handler.
    if is_tombstone(msg.value):
        store.delete(key)
        value = Event.wrap({})
    else:
        decoded = decode_event_mediated(msg, on_invalid)
        if decoded is None:
            return None
        value = decoded
        # Enrich a PRIVATE deep copy. `enrich_config`'s idiom is
        # mutate-and-return, and the message returned below must carry the
        # value PRE-enrichment — a plain `Config(value)` shares nested
        # structure, so an in-place edit one level down would leak into the
        # caller's `extract_state_key` input. Same defence `poll_one` applies
        # to the cached config; config records are rare and small by the
        # config-topic contract, so the copy is free in practice.
        store.put(key, await enrich_config(Config(deepcopy(value))))
    return IncomingMessage(
        key=key,
        offset=msg.offset,
        partition=msg.partition,
        timestamp=millis_to_datetime(msg.timestamp),
        topic=msg.topic,
        value=value,
    )


async def bootstrap_config_store(
    consumer: aiokafka.AIOKafkaConsumer,
    topics: list[str],
    store: ConfigStore,
    enrich_config: EnrichConfigFn,
    on_invalid: "OnInvalidMessageFn",
) -> dict[str, IncomingMessage]:
    """Read every config topic in full and populate the store.

    Reads to the end offsets captured at entry and compacts by wire key
    across ALL topics — one namespace; a tombstone on any topic deletes the
    key. `enrich_config` runs once per surviving entry, not per record.
    Returns the surviving record per key, decoded, so callers can react once
    per live config without re-parsing (the extractor runner builds its
    config entries from these); a key whose surviving record ``on_invalid``
    skipped is absent from both the result and the store.

    Compaction needs the keys, so an undecodable KEY reaches the handler here
    — for every record, stale ones included. An undecodable VALUE reaches it
    only from the surviving record per key, which is where the value is first
    read at all.

    Leaves the consumer assigned to the union of all config partitions,
    positioned at the captured end offsets — `drain_config_updates`
    continues from there seamlessly.
    """
    if not topics:
        return {}
    tps = await topic_partitions(consumer, topics)
    latest: dict[str, ConsumerRecord[Any, Any]] = {}

    async def collect(msg: ConsumerRecord[Any, Any]) -> None:
        key = decode_key_mediated(msg, on_invalid)
        if key is None:
            return  # skipped by the handler: it takes part in no compaction
        if is_tombstone(msg.value):
            latest.pop(key, None)
        else:
            latest[key] = msg

    count = await read_to_end(consumer, tps, collect)
    applied = {
        key: decoded
        for key, msg in latest.items()
        if (decoded := await apply_config_record(msg, store, enrich_config, on_invalid)) is not None
    }
    log.info("Bootstrapped config store with %d entries from %d record(s) on %s",
             len(store), count, topics)
    return applied


async def drain_config_updates(
    consumer: aiokafka.AIOKafkaConsumer,
    store: ConfigStore,
    enrich_config: EnrichConfigFn,
    on_invalid: "OnInvalidMessageFn",
) -> list[IncomingMessage]:
    """Apply newly-arrived config records without blocking and return them, decoded.

    Per-partition arrival order is preserved; cross-partition order is not.
    Records ``on_invalid`` skipped are excluded — they changed nothing, so
    callers must not count or react to them. The consumer must be the one
    `bootstrap_config_store` left assigned.
    """
    records = await consumer.getmany(timeout_ms=0)
    return [
        decoded
        for msgs in records.values()
        for msg in msgs
        if (decoded := await apply_config_record(msg, store, enrich_config, on_invalid)) is not None
    ]
