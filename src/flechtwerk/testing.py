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

    def config_message_in(self, topic: str) -> None:
        self.calls.append(("config_message_in", topic))

    def config_store_entries(self, n: int) -> None:
        self.calls.append(("config_store_entries", n))

    def config_store_restored(self, entries: int) -> None:
        self.calls.append(("config_store_restored", entries))

    def state_restored(self, partition: int, entries: int) -> None:
        self.calls.append(("state_restored", partition, entries))

    def tasks_assigned(self, n: int) -> None:
        self.calls.append(("tasks_assigned", n))

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


class FakeKafkaClient:
    """Stands in for ``consumer._client`` — records `set_topics` metadata priming."""

    def __init__(self) -> None:
        self.topics: list[str] = []

    async def set_topics(self, topics: list[str]) -> None:
        self.topics = list(topics)


class FakeKafkaConsumer:
    """Test double implementing the subset of aiokafka.AIOKafkaConsumer used by runners.

    ``records`` is the unread backlog: `getmany` drains it (optionally
    filtered to the requested partitions) and advances per-partition fetch
    positions, and `end_offsets` derives ends from position + backlog — so
    the position-vs-end-offset termination of ``read_to_end`` works against
    this fake.
    """

    def __init__(self, records: list[ConsumerRecord[Any, Any]] | None = None):
        self._client = FakeKafkaClient()
        self.assigned: set[TopicPartition] = set()
        self.committed = False
        self.listener: Any = None
        self.paused: set[TopicPartition] = set()
        self.positions: dict[TopicPartition, int] = {}
        self.records = list(records or [])
        self.started = False
        self.subscribed: list[str] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        pass

    async def getmany(self, *partitions: TopicPartition, timeout_ms: int = 0) -> dict:
        if not self.records:
            return {}
        # Group records by TopicPartition like aiokafka does
        from collections import defaultdict
        groups: dict[TopicPartition, list] = defaultdict(list)
        for record in self.records:
            groups[TopicPartition(record.topic, record.partition)].append(record)
        selected = {tp: msgs for tp, msgs in groups.items() if not partitions or tp in partitions}
        self.records = [
            r for r in self.records
            if TopicPartition(r.topic, r.partition) not in selected
        ]
        for tp, msgs in selected.items():
            self.positions[tp] = max(self.positions.get(tp, 0), max(m.offset for m in msgs) + 1)
        return selected

    async def commit(self, offsets: dict | None = None) -> None:
        self.committed = True

    def subscribe(self, topics: list[str], listener: Any = None) -> None:
        self.listener = listener
        self.subscribed = list(topics)

    def assign(self, tps: list[TopicPartition]) -> None:
        self.assigned = set(tps)

    async def seek_to_beginning(self, *tps: TopicPartition) -> None:
        pass

    async def position(self, tp: TopicPartition) -> int:
        return self.positions.get(tp, 0)

    async def end_offsets(self, tps: list[TopicPartition]) -> dict[TopicPartition, int]:
        return {
            tp: max(
                self.positions.get(tp, 0),
                *(r.offset + 1 for r in self.records if TopicPartition(r.topic, r.partition) == tp),
                0,
            )
            for tp in tps
        }

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {r.partition for r in self.records if r.topic == topic}

    def assignment(self) -> set:
        return set(self.assigned)

    def pause(self, *tps: TopicPartition) -> None:
        self.paused |= set(tps)

    def resume(self, *tps: TopicPartition) -> None:
        self.paused -= set(tps)


class FakeKafkaProducer:
    """Test double implementing the subset of aiokafka.AIOKafkaProducer used by runners."""

    def __init__(self):
        self.flushed = False
        self.offsets_sent: list[tuple[dict, str]] = []
        self.sent: list[tuple[str, dict]] = []
        self.started = False
        self.stopped = False
        self.transaction_active = False
        self.transaction_count = 0

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send(
        self,
        topic: str,
        *,
        key: Any = None,
        value: Any = None,
        partition: int | None = None,
        timestamp_ms: int | None = None,
    ) -> None:
        self.sent.append((topic, {"key": key, "partition": partition, "value": value, "timestamp_ms": timestamp_ms}))

    async def flush(self) -> None:
        self.flushed = True

    def transaction(self):
        self.transaction_count += 1
        return FakeTransaction(self)

    async def send_offsets_to_transaction(self, offsets: dict, group_id: str) -> None:
        self.offsets_sent.append((dict(offsets), group_id))


class FakeTransaction:
    """Fake async context manager for producer transactions."""

    def __init__(self, producer: FakeKafkaProducer):
        self.producer = producer

    async def __aenter__(self):
        self.producer.transaction_active = True
        return self

    async def __aexit__(self, *exc_info):
        self.producer.transaction_active = False
