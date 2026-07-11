"""Config topics — shared lookup tables read in full by every instance.

This is Kafka Streams' GlobalKTable pattern, specialized to what stages
actually share: configuration. Config topics are consumed with no consumer
group across *all* their partitions and compacted by wire key into ONE
per-process `ConfigStore` — a single key namespace regardless of how many
config topics a stage declares, matching what the extractor runner's config
dict has always done. Partition placement on a config topic is therefore
irrelevant — any task on any instance finds any key — which is exactly what
partitioned task state cannot offer (see the Co-Partitioning Trap in the
project docs).

The source topics are their own changelog: no separate changelog topic, no
committed offsets, a full re-read on every startup. They must be compacted
and stay small (the whole store lives in memory per instance). Lookups are
eventually consistent — config updates are NOT part of any task
transaction, matching the GlobalKTable caveat.

`Stage.enrich` is applied here, once per config record — the startup
bootstrap compacts first, so once per *surviving* entry — never per poll
tick or per lookup. Kafka Streams forbids transforming records on their way
into a global store (KIP-813) because a checkpoint-based restore would
bypass the transformation; Flechtwerk re-reads the topics through this same
enrich path on every startup, so the enriched store cannot diverge from
what a fresh boot would build.
"""
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aiokafka
from aiokafka import ConsumerRecord

from flechtwerk.attribute import Record
from .kafka import decode_event, decode_key, encode_json, is_tombstone, read_to_end
from .types import Config

log = logging.getLogger(__name__)

EnrichFn = Callable[[Config], Awaitable[Config]]


class ConfigStore:
    """Latest config per wire key, merged across a stage's config topics.

    Values are kept as wire bytes and parsed on every `get()` — each call
    returns a fresh `Config` (a protective copy by construction). Malformed
    values decode to an empty `Config` with a warning, so they flow into
    the caller's validation instead of masquerading as a missing key.
    """

    def __init__(self) -> None:
        self._raw: dict[str, bytes] = {}

    def __contains__(self, key: str) -> bool:
        return key in self._raw

    def __len__(self) -> int:
        return len(self._raw)

    @classmethod
    def of(cls, entries: dict[str, Record]) -> ConfigStore:
        """Build a pre-seeded store — the test-side entry point."""
        store = cls()
        store._raw = {key: encode_json(value) for key, value in entries.items()}
        return store

    def get(self, key: str) -> Config | None:
        """Return the latest config for ``key``, or None if absent."""
        raw = self._raw.get(key)
        return None if raw is None else Config(decode_event(raw, key))

    def put(self, key: str, config: Record) -> None:
        self._raw[key] = encode_json(config)

    def delete(self, key: str) -> None:
        self._raw.pop(key, None)


async def apply_config_record(
    msg: ConsumerRecord[Any, Any],
    store: ConfigStore,
    enrich: EnrichFn,
) -> None:
    """Apply one config record: tombstones delete, values are enriched then stored."""
    key = decode_key(msg.key)
    if not key:
        log.warning("Config record without a key at %s/%d — the store is keyed by wire key",
                    msg.topic, msg.offset)
    if is_tombstone(msg.value):
        store.delete(key)
    else:
        config = Config(decode_event(msg.value, f"{msg.topic}/{msg.offset}"))
        store.put(key, await enrich(config))


async def bootstrap_config_store(
    consumer: aiokafka.AIOKafkaConsumer,
    topics: list[str],
    store: ConfigStore,
    enrich: EnrichFn,
) -> dict[str, ConsumerRecord[Any, Any]]:
    """Read every config topic in full and populate the store.

    Reads to the end offsets captured at entry and compacts by wire key
    across ALL topics — one namespace; a tombstone on any topic deletes the
    key. `enrich` runs once per surviving entry, not per record. Returns the
    surviving record per key so callers can react once per live config (the
    extractor runner builds its config entries from these).

    Leaves the consumer assigned to the union of all config partitions,
    positioned at the captured end offsets — `drain_config_updates`
    continues from there seamlessly.
    """
    if not topics:
        return {}
    # Prime the consumer's internal cluster metadata so partitions_for_topic()
    # returns data — same private-API coupling as restore_changelog, locked
    # down by the integration tests under test/flechtwerk/integration/.
    await consumer._client.set_topics(list(topics))
    tps = [
        aiokafka.TopicPartition(topic, partition)
        for topic in topics
        for partition in sorted(consumer.partitions_for_topic(topic) or ())
    ]
    latest: dict[str, ConsumerRecord[Any, Any]] = {}

    async def collect(msg: ConsumerRecord[Any, Any]) -> None:
        if is_tombstone(msg.value):
            latest.pop(decode_key(msg.key), None)
        else:
            latest[decode_key(msg.key)] = msg

    count = await read_to_end(consumer, tps, collect)
    for msg in latest.values():
        await apply_config_record(msg, store, enrich)
    log.info("Bootstrapped config store with %d entries from %d record(s) on %s",
             len(store), count, topics)
    return latest


async def drain_config_updates(
    consumer: aiokafka.AIOKafkaConsumer,
    store: ConfigStore,
    enrich: EnrichFn,
) -> list[ConsumerRecord[Any, Any]]:
    """Apply newly-arrived config records without blocking and return them.

    Per-partition arrival order is preserved; cross-partition order is not.
    The consumer must be the one `bootstrap_config_store` left assigned.
    """
    records = await consumer.getmany(timeout_ms=0)
    flat = [msg for msgs in records.values() for msg in msgs]
    for msg in flat:
        await apply_config_record(msg, store, enrich)
    return flat
