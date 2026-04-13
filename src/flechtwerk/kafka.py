"""Kafka utilities and changelog restore."""
from __future__ import annotations

import json
import logging
import pickle
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import aiokafka

from .types import Event, IncomingMessage, State

log = logging.getLogger(__name__)


# --- Utilities ---


def encode_json(value: Any) -> bytes:
    """Encode a value to compact, sorted-key JSON bytes for Kafka.

    Returns UTF-8 bytes ready for the producer (no serializer needed).
    """
    if isinstance(value, str):
        return value.encode("utf-8")
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


def parse_message(msg: Any) -> IncomingMessage:
    """Parse an aiokafka ConsumerRecord into an IncomingMessage."""
    key = (msg.key.decode("utf-8") if isinstance(msg.key, bytes) else msg.key) or ""
    raw_value = (msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value) or ""
    try:
        value = json.loads(raw_value) if raw_value else {}
    except json.JSONDecodeError:
        log.warning("Invalid JSON in message at %s/%d, using {}", msg.topic, msg.offset)
        value = {}
    return IncomingMessage(
        key=key,
        offset=msg.offset,
        partition=msg.partition,
        timestamp=millis_to_datetime(msg.timestamp),
        topic=msg.topic,
        value=Event(value),
    )


# --- Changelog restore ---


async def restore_changelog(
    consumer: aiokafka.AIOKafkaConsumer,
    topic: str,
    put: Callable[[str, State], Awaitable[None]],
    delete: Callable[[str], Awaitable[None]],
) -> int:
    """Read an entire compacted changelog topic to rebuild state.

    Uses manual partition assignment (no consumer group). The consumer
    must already be started with group_id=None.

    Args:
        consumer: An already-started AIOKafkaConsumer (group_id=None).
        topic: Changelog topic name.
        put: async callable(key, value) to store a state entry.
        delete: async callable(key) to remove a state entry.

    Returns:
        Number of entries restored.
    """
    # Prime the consumer's internal cluster metadata for this topic so
    # partitions_for_topic() returns data. No fully public API achieves this:
    # consumer.topics() / fetch_all_metadata() returns a *separate* ClusterMetadata
    # object that doesn't update the consumer's own cache, and assign() requires
    # the partition set we're about to fetch. `_client.set_topics()` is a public
    # method on AIOKafkaClient (the `_client` attribute is the only underscore).
    # The integration tests under test/fretworx/integration/ lock down this
    # coupling against aiokafka upgrades.
    await consumer._client.set_topics([topic])
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        log.info("No partitions found for changelog topic %s", topic)
        return 0

    tps = [aiokafka.TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)
    await consumer.seek_to_beginning()

    count = 0
    while True:
        records = await consumer.getmany(timeout_ms=2000)
        if not records:
            break
        for tp, msgs in records.items():
            for msg in msgs:
                key = (msg.key.decode("utf-8") if isinstance(msg.key, bytes) else msg.key) or ""
                if msg.value:
                    value = pickle.loads(msg.value)  # noqa: S301
                    if value:
                        await put(key, value)
                    else:
                        await delete(key)
                else:
                    await delete(key)
                count += 1

    log.info("Restored %d state entries from %s", count, topic)
    return count
