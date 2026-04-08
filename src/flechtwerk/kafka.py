"""Kafka Protocols, utilities, and changelog restore."""
from __future__ import annotations

import json
import logging
import pickle
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

import aiokafka

from .types import Event, IncomingMessage, State

log = logging.getLogger(__name__)


# --- Protocols ---


@runtime_checkable
class KafkaConsumer(Protocol):
    """Protocol matching the subset of aiokafka.AIOKafkaConsumer we use."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getmany(self, timeout_ms: int = 0) -> dict: ...
    async def commit(self, offsets: dict | None = None) -> None: ...
    def subscribe(self, topics: list[str]) -> None: ...
    async def seek_to_beginning(self) -> None: ...
    async def position(self, tp: Any) -> int: ...
    def assignment(self) -> set: ...


@runtime_checkable
class KafkaProducer(Protocol):
    """Protocol matching the subset of aiokafka.AIOKafkaProducer we use."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, topic: str, *, key: Any = None, value: Any = None, timestamp_ms: int | None = None) -> Any: ...
    async def flush(self) -> None: ...
    def transaction(self) -> Any: ...
    async def send_offsets_to_transaction(self, offsets: dict, group_id: str) -> None: ...


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
    # Force metadata fetch for this topic (start() only fetches for subscribed topics)
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
