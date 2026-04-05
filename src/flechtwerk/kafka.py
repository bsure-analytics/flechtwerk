"""Kafka Protocols, utilities, and changelog restore."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

import aiokafka

from .types import IncomingMessage, Message, State

log = logging.getLogger(__name__)


# --- Protocols ---


@runtime_checkable
class KafkaConsumer(Protocol):
    """Protocol matching the subset of aiokafka.AIOKafkaConsumer we use."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def getmany(self, timeout_ms: int = 0) -> dict: ...
    async def commit(self, offsets: dict | None = None) -> None: ...
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


def encode_json(value: Any) -> str:
    """Encode a value to compact, sorted-key JSON matching Bytewax's serialization."""
    if isinstance(value, str):
        return value
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


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


def parse_message(msg: Any) -> IncomingMessage[dict[str, Any]]:
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
        value=value,
    )


# --- Changelog restore ---


async def restore_changelog(
    bootstrap_servers: str,
    topic: str,
    put: Any,
    delete: Any,
) -> int:
    """Read an entire compacted changelog topic to rebuild state.

    Uses manual partition assignment (no consumer group). Seeks to
    beginning and reads until no more messages arrive.

    Args:
        bootstrap_servers: Kafka bootstrap servers (comma-separated)
        topic: Changelog topic name
        put: async callable(key, value) to store a state entry
        delete: async callable(key) to remove a state entry

    Returns:
        Number of entries restored.
    """
    consumer = aiokafka.AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: v.decode("utf-8") if v else "",
        key_deserializer=lambda k: k.decode("utf-8") if k else "",
        enable_auto_commit=False,
    )
    await consumer.start()

    try:
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
                    key = msg.key or ""
                    raw_value = msg.value or ""
                    if raw_value:
                        try:
                            value = json.loads(raw_value)
                        except json.JSONDecodeError:
                            continue
                        if value:
                            await put(key, State(value))
                        else:
                            await delete(key)
                    else:
                        await delete(key)
                    count += 1

        log.info("Restored %d state entries from %s", count, topic)
        return count
    finally:
        await consumer.stop()
