"""Test doubles for fretworx framework testing."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .types import Message


@dataclass
class FakeRecord:
    """Mimics an aiokafka ConsumerRecord for testing."""

    key: str
    value: str
    offset: int = 0
    partition: int = 0
    timestamp: int | None = None
    topic: str = "test-topic"


class FakeKafkaConsumer:
    """Test double matching the KafkaConsumer Protocol."""

    def __init__(self, records: list[FakeRecord] | None = None):
        self.records = list(records or [])
        self.committed = False
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        pass

    async def getmany(self, timeout_ms: int = 0) -> dict:
        if not self.records:
            return {}
        # Group records by (topic, partition) like aiokafka does
        from collections import defaultdict
        groups: dict[Any, list] = defaultdict(list)
        for record in self.records:
            tp = (record.topic, record.partition)
            groups[tp].append(record)
        self.records = []
        return dict(groups)

    async def commit(self, offsets: dict | None = None) -> None:
        self.committed = True

    def subscribe(self, topics: list[str]) -> None:
        pass

    async def seek_to_beginning(self) -> None:
        pass

    async def position(self, tp: Any) -> int:
        return 0

    def assignment(self) -> set:
        return set()


class FakeKafkaProducer:
    """Test double matching the KafkaProducer Protocol."""

    def __init__(self):
        self.sent: list[tuple[str, dict]] = []
        self.flushed = False
        self.transaction_active = False

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, topic: str, *, key: Any = None, value: Any = None, timestamp_ms: int | None = None) -> None:
        self.sent.append((topic, {"key": key, "value": value, "timestamp_ms": timestamp_ms}))

    async def flush(self) -> None:
        self.flushed = True

    def transaction(self):
        return FakeTransaction(self)

    async def send_offsets_to_transaction(self, offsets: dict, group_id: str) -> None:
        pass


class FakeTransaction:
    """Fake async context manager for producer transactions."""

    def __init__(self, producer: FakeKafkaProducer):
        self.producer = producer

    async def __aenter__(self):
        self.producer.transaction_active = True
        return self

    async def __aexit__(self, *exc_info):
        self.producer.transaction_active = False
