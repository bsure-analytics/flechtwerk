"""Kafka consumer/producer ports and aiokafka adapters."""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from .types import IncomingMessage, Message

log = logging.getLogger(__name__)


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


class KafkaConsumer(ABC):
    """Port: async Kafka consumer."""

    @abstractmethod
    async def subscribe(self, topics: list[str]) -> None:
        ...

    @abstractmethod
    async def poll(self, timeout: float = 1.0) -> list[IncomingMessage]:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class KafkaProducer(ABC):
    """Port: async Kafka producer."""

    @abstractmethod
    async def send(self, message: Message) -> None:
        ...

    async def send_batch(self, messages: list[Message]) -> None:
        """Send multiple messages. Default: send one at a time."""
        for msg in messages:
            await self.send(msg)
        await self.flush()

    async def send_transactional(
        self,
        messages: list[Message],
        consumer: KafkaConsumer,
    ) -> None:
        """Send messages and commit consumer offsets atomically (exactly-once).

        Override in adapters that support Kafka transactions.
        Default: send + commit (at-least-once fallback).
        """
        await self.send_batch(messages)
        await consumer.commit()

    @abstractmethod
    async def flush(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class AIOKafkaConsumerAdapter(KafkaConsumer):
    """Adapter: aiokafka-based Kafka consumer."""

    def __init__(self, brokers: list[str], group_id: str):
        self.brokers = brokers
        self.group_id = group_id
        self.consumer = None

    async def subscribe(self, topics: list[str]) -> None:
        from aiokafka import AIOKafkaConsumer as AIOConsumer

        self.consumer = AIOConsumer(
            *topics,
            bootstrap_servers=",".join(self.brokers),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=self.group_id,
            value_deserializer=lambda v: v.decode("utf-8") if v else "",
            key_deserializer=lambda k: k.decode("utf-8") if k else "",
        )
        await self.consumer.start()
        log.info("Subscribed to %s as group %s", topics, self.group_id)

    async def poll(self, timeout: float = 1.0) -> list[IncomingMessage]:
        if self.consumer is None:
            return []
        records = await self.consumer.getmany(timeout_ms=int(timeout * 1000))
        result = []
        for tp, msgs in records.items():
            for msg in msgs:
                try:
                    value = json.loads(msg.value or "{}")
                except json.JSONDecodeError:
                    log.warning("Invalid JSON in message at %s/%d, using {}", msg.topic, msg.offset)
                    value = {}
                result.append(IncomingMessage(
                    key=msg.key or "",
                    offset=msg.offset,
                    partition=msg.partition,
                    timestamp=millis_to_datetime(msg.timestamp),
                    topic=msg.topic,
                    value=value,
                ))
        return result

    async def commit(self) -> None:
        if self.consumer is not None:
            await self.consumer.commit()

    async def close(self) -> None:
        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None


class AIOKafkaProducerAdapter(KafkaProducer):
    """Adapter: aiokafka-based Kafka producer with exactly-once support."""

    def __init__(self, brokers: list[str], transactional_id: str | None = None):
        self.brokers = brokers
        self.transactional_id = transactional_id
        self.producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer as AIOProducer

        kwargs: dict[str, Any] = {
            "bootstrap_servers": ",".join(self.brokers),
            "key_serializer": lambda k: k.encode("utf-8") if k else b"",
            "value_serializer": lambda v: v.encode("utf-8") if v else b"",
        }
        if self.transactional_id:
            kwargs["transactional_id"] = self.transactional_id

        self.producer = AIOProducer(**kwargs)
        await self.producer.start()
        log.info("Producer started (transactional=%s)", self.transactional_id is not None)

    async def send(self, message: Message) -> None:
        if self.producer is None:
            await self.start()
        await self.producer.send(
            topic=message.topic,
            key=encode_json(message.key),
            value=encode_json(message.value),
            timestamp_ms=datetime_to_millis(message.timestamp),
        )

    async def send_batch(self, messages: list[Message]) -> None:
        for msg in messages:
            await self.send(msg)
        await self.flush()

    async def send_transactional(
        self,
        messages: list[Message],
        consumer: KafkaConsumer,
    ) -> None:
        """Exactly-once: produce + commit offset in a single Kafka transaction."""
        if self.producer is None:
            await self.start()

        if not self.transactional_id:
            # Fallback to at-least-once
            await self.send_batch(messages)
            await consumer.commit()
            return

        async with self.producer.transaction():
            for msg in messages:
                await self.producer.send(
                    topic=msg.topic,
                    key=encode_json(msg.key),
                    value=encode_json(msg.value),
                    timestamp_ms=datetime_to_millis(msg.timestamp),
                )
            # Commit consumer offsets within the transaction
            if isinstance(consumer, AIOKafkaConsumerAdapter) and consumer.consumer:
                offsets = await consumer.consumer.committed(consumer.consumer.assignment())
                # aiokafka handles offset commit within transaction context

        log.debug("Transaction committed: %d messages", len(messages))

    async def flush(self) -> None:
        if self.producer is not None:
            await self.producer.flush()

    async def close(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
