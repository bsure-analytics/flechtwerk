"""Kafka utilities and changelog restore."""
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import aiokafka
from aiokafka import ConsumerRecord

from flechtwerk.attribute import Record

from .types import Event, IncomingMessage

log = logging.getLogger(__name__)

# Framework-internal on purpose: these are the shared building blocks of the
# runners and the config machinery, not an application-facing surface.
__all__: list[str] = []


# --- Utilities ---


def encode_json(value: Any) -> bytes:
    """Encode a value to compact, sorted-key JSON bytes for Kafka.

    Returns UTF-8 bytes ready for the producer (no serializer needed).
    """
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, Record):
        value = value.raw
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def datetime_to_millis(dt: datetime | None) -> int | None:
    """Convert a datetime to Kafka millisecond epoch, or None."""
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def millis_to_datetime(millis: int | None) -> datetime | None:
    """Convert Kafka millisecond epoch to a UTC datetime, or None."""
    if millis is None:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def decode_key(key: bytes | str | None) -> str:
    """Decode a Kafka message key to a string; missing keys become ``""``."""
    return (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key) or ""


def decode_event(value: bytes | str | None, context: str) -> Event:
    """Decode a Kafka message value into an Event.

    Malformed payloads fall back to ``Event.wrap({})`` rather than raising — a
    single bad message must not crash the stage into an infinite
    CrashLoopBackOff on its own offset. ``context`` identifies the message in
    the warning (e.g. ``"topic/offset"``). Handles:
      - non-UTF-8 bytes in value
      - invalid JSON
      - valid JSON that decodes to a non-dict (e.g. scalar or array)
    """
    try:
        raw_value = value.decode("utf-8") if isinstance(value, bytes) else value
        parsed = json.loads(raw_value) if raw_value else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        log.warning("Malformed message at %s (%s), using {}", context, type(e).__name__)
        parsed = {}
    if not isinstance(parsed, dict):
        log.warning("Non-dict JSON payload at %s (%s), using {}", context, type(parsed).__name__)
        parsed = {}
    return Event.wrap(parsed)


def parse_message(msg: ConsumerRecord[Any, Any]) -> IncomingMessage:
    """Parse an aiokafka ConsumerRecord into an IncomingMessage.

    Value decoding is lenient — see `decode_event`.
    """
    return IncomingMessage(
        key=decode_key(msg.key),
        offset=msg.offset,
        partition=msg.partition,
        timestamp=millis_to_datetime(msg.timestamp),
        topic=msg.topic,
        value=decode_event(msg.value, f"{msg.topic}/{msg.offset}"),
    )


# --- Reading topics to their end ---


def is_tombstone(raw: bytes | str | None) -> bool:
    """True for an empty Kafka value or a serialized falsy record.

    Covers ``b""``/``None`` (a real Kafka tombstone) and ``b"{}"`` (a falsy
    Record serialized to JSON). Accepts ``str`` like the decode helpers —
    aiokafka delivers bytes, test doubles may carry strings.
    """
    return not raw or raw in (b"{}", "{}")


async def read_to_end(
    consumer: aiokafka.AIOKafkaConsumer,
    tps: list[aiokafka.TopicPartition],
    apply: Callable[[ConsumerRecord[Any, Any]], Awaitable[None]],
) -> int:
    """Read the given partitions from the beginning to their current end.

    Uses manual partition assignment (no consumer group) on an
    already-started consumer. Reads to the end offsets captured at entry —
    under isolation_level="read_committed" that is the last stable offset,
    so records of in-flight transactions are never applied. Leaves the
    consumer assigned to ``tps`` and positioned at the captured end offsets,
    so the caller can keep polling for later records seamlessly.

    Returns the number of records passed to ``apply``.
    """
    consumer.assign(tps)
    await consumer.seek_to_beginning(*tps)
    end_offsets = await consumer.end_offsets(tps)

    count = 0
    pending = {tp for tp in tps if end_offsets[tp] > 0}
    while pending:
        records = await consumer.getmany(*pending, timeout_ms=2000)
        for msgs in records.values():
            for msg in msgs:
                await apply(msg)
                count += 1
        # An empty poll is not end-of-log — broker stalls and fetch backoff
        # yield empty results too. Only the fetch position reaching the end
        # offset captured at entry terminates a partition's read.
        pending = {tp for tp in pending if await consumer.position(tp) < end_offsets[tp]}
    return count


async def topic_partitions(
    consumer: aiokafka.AIOKafkaConsumer,
    topics: list[str],
) -> list[aiokafka.TopicPartition]:
    """Prime metadata and return every partition of every topic in ``topics``.

    Shared by the config bootstrap (`configs.bootstrap_config_store`) and the
    secrets scan (`secrets.scan_config_topics`) — the one place the
    `consumer._client.set_topics` private-API coupling lives (no fully public
    API primes the consumer's own metadata cache; the integration tests under
    tests/integration/ lock this down against aiokafka upgrades). Unknown
    topics contribute no partitions; a caller that must fail on a missing topic
    checks the returned set itself.
    """
    await consumer._client.set_topics(list(topics))
    return [
        aiokafka.TopicPartition(topic, partition)
        for topic in topics
        for partition in sorted(consumer.partitions_for_topic(topic) or ())
    ]


# --- Changelog restore ---


async def restore_changelog(
    consumer: aiokafka.AIOKafkaConsumer,
    topic: str,
    put_raw: Callable[[str, bytes], Awaitable[None]],
    delete: Callable[[str], Awaitable[None]],
    partitions: set[int] | None = None,
) -> int:
    """Read a compacted changelog topic (or a subset of its partitions) to rebuild state.

    Uses manual partition assignment (no consumer group). The consumer
    must already be started with group_id=None. Reads to the end offsets
    captured at entry — under isolation_level="read_committed" that is the
    last stable offset, so records of in-flight transactions are never
    restored. Callers restoring a task partition must fence the previous
    owner (InitProducerId via producer.start()) *before* calling this, so
    that owner's pending transaction is aborted and the captured end offset
    is final.

    Args:
        consumer: An already-started AIOKafkaConsumer (group_id=None).
        topic: Changelog topic name.
        put_raw: async callable(key, raw_bytes) to store wire bytes for a key.
            Per-key deduplication happens at the storage layer (RocksDB
            overwrites earlier writes for the same key on disk), so memory
            usage stays bounded by the inner store's cache, not by the topic
            size. Deserialization is deferred to the first `get()` for the
            key — keys that are never read by the running stage never pay
            the deserialize cost.
        delete: async callable(key) to remove a state entry.
        partitions: Restrict the restore to these partition numbers.
            None restores every partition of the topic.

    Returns:
        Number of records processed.
    """
    subset = partitions is not None
    if partitions is None:
        # Prime the consumer's internal cluster metadata for this topic so
        # partitions_for_topic() returns data. No fully public API achieves this:
        # consumer.topics() / fetch_all_metadata() returns a *separate* ClusterMetadata
        # object that doesn't update the consumer's own cache, and assign() requires
        # the partition set we're about to fetch. `_client.set_topics()` is a public
        # method on AIOKafkaClient (the `_client` attribute is the only underscore).
        # The integration tests under tests/integration/ lock down this
        # coupling against aiokafka upgrades.
        await consumer._client.set_topics([topic])
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            log.info("No partitions found for changelog topic %s", topic)
            return 0

    async def apply(msg: ConsumerRecord[Any, Any]) -> None:
        # Tombstones delete; anything else is wire bytes — pass through to
        # the inner store.
        key = decode_key(msg.key)
        if is_tombstone(msg.value):
            await delete(key)
        else:
            await put_raw(key, msg.value)

    tps = [aiokafka.TopicPartition(topic, p) for p in sorted(partitions)]
    count = await read_to_end(consumer, tps, apply)

    log.info("Restored %d state entries from %s%s", count, topic,
             f" partitions {sorted(partitions)}" if subset else "")
    return count
