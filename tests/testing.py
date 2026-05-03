"""Test doubles for fretworx framework testing."""
from contextlib import contextmanager
from typing import Any

from aiokafka import ConsumerRecord, TopicPartition

from fretworx.observer import Observer
from fretworx.state import StateStore, deserialize
from fretworx.types import Message, State


class RecordingObserver(Observer):
    """Captures every observer hook call into a list for assertion."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def message_in(self, topic: str) -> None:
        self.calls.append(("message_in", topic))

    def message_out(self, topic: str) -> None:
        self.calls.append(("message_out", topic))

    def transaction_committed(self) -> None:
        self.calls.append(("transaction_committed",))

    def active_configs(self, n: int) -> None:
        self.calls.append(("active_configs", n))

    @contextmanager
    def dispatch_scope(self):
        self.calls.append(("dispatch_enter",))
        yield
        self.calls.append(("dispatch_exit",))

    @contextmanager
    def batch_scope(self, size: int):
        self.calls.append(("batch_enter", size))
        yield
        self.calls.append(("batch_exit",))

    @contextmanager
    def poll_cycle_scope(self):
        self.calls.append(("poll_cycle_enter",))
        yield
        self.calls.append(("poll_cycle_exit",))


class InMemoryStateStore(StateStore):
    """In-memory state store for testing — mirrors `RocksDBStateStore`'s
    bytes-on-disk semantics so test assertions match production behavior."""

    def __init__(self):
        self.store: dict[str, bytes] = {}

    async def get(self, key: str) -> State | None:
        raw = self.store.get(key)
        if raw is None:
            return None
        return deserialize(raw)

    async def put_bytes(self, key: str, raw: bytes) -> None:
        self.store[key] = raw

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    async def close(self) -> None:
        pass


def make_record(
    *,
    key: bytes | str | None = None,
    value: bytes | str | None = None,
    topic: str = "test-topic",
    partition: int = 0,
    offset: int = 0,
    timestamp: int = 0,
) -> ConsumerRecord[Any, Any]:
    """Build a real ``aiokafka.ConsumerRecord`` with sensible defaults for tests.

    Only the six fields ``parse_message`` actually reads are exposed — the
    remaining aiokafka-internal fields (``timestamp_type``, ``checksum``, the
    ``serialized_*_size`` fields, ``headers``) get placeholder values.
    """
    return ConsumerRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
        headers=(),
    )


class FakeKafkaConsumer:
    """Test double implementing the subset of aiokafka.AIOKafkaConsumer used by runners."""

    def __init__(self, records: list[ConsumerRecord[Any, Any]] | None = None):
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
        # Group records by TopicPartition like aiokafka does
        from collections import defaultdict
        groups: dict[TopicPartition, list] = defaultdict(list)
        for record in self.records:
            groups[TopicPartition(record.topic, record.partition)].append(record)
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
    """Test double implementing the subset of aiokafka.AIOKafkaProducer used by runners."""

    def __init__(self):
        self.sent: list[tuple[str, dict]] = []
        self.flushed = False
        self.transaction_active = False
        self.transaction_count = 0

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    async def send(self, topic: str, *, key: Any = None, value: Any = None, timestamp_ms: int | None = None) -> None:
        self.sent.append((topic, {"key": key, "value": value, "timestamp_ms": timestamp_ms}))

    async def flush(self) -> None:
        self.flushed = True

    def transaction(self):
        self.transaction_count += 1
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
