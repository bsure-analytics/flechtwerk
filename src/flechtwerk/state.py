"""State store port and adapters (RocksDB, in-memory, changelog-backed)."""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from .kafka import KafkaConsumer, KafkaProducer
from .types import Message, State

log = logging.getLogger(__name__)


class StateEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime and set.

    Note: tuples are natively serialized as JSON arrays and round-trip as lists.
    This is acceptable — no business logic depends on the tuple/list distinction.
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, (set, frozenset)):
            return {"__type__": "set", "value": sorted(obj, key=str)}
        return super().default(obj)


def state_decoder_hook(obj: dict) -> Any:
    """JSON object hook that restores datetime and set."""
    type_tag = obj.get("__type__")
    if type_tag == "datetime":
        return datetime.fromisoformat(obj["value"])
    if type_tag == "set":
        return set(obj["value"])
    return obj


class StateStore(ABC):
    """Port: persistent key-value state store.

    Contract: get() returns a protective copy. Callers may mutate the
    returned dict without affecting the store's internal state.
    """

    @abstractmethod
    async def get(self, key: str) -> State | None:
        ...

    @abstractmethod
    async def put(self, key: str, state: State) -> None:
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class RocksDBStateStore(StateStore):
    """Adapter: RocksDB-backed state store.

    State values are JSON-serialized with custom encoding for datetime/set/tuple.
    Every put() writes to the RocksDB WAL immediately — no periodic snapshots.
    """

    def __init__(self, path: str):
        from rocksdict import Rdict

        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        db_path = str(self.path / "state.db")
        self.db = Rdict(db_path)
        log.info("Opened RocksDB state store at %s", db_path)

    async def get(self, key: str) -> State | None:
        try:
            raw = self.db[key]
        except KeyError:
            return None
        return json.loads(raw, object_hook=state_decoder_hook)

    async def put(self, key: str, state: State) -> None:
        raw = json.dumps(state, cls=StateEncoder, sort_keys=True)
        self.db[key] = raw

    async def delete(self, key: str) -> None:
        try:
            del self.db[key]
        except KeyError:
            pass

    async def close(self) -> None:
        self.db.close()
        log.info("Closed RocksDB state store at %s", self.path)


class InMemoryStateStore(StateStore):
    """Adapter: in-memory state store for testing."""

    def __init__(self):
        self.store: dict[str, State] = {}

    async def get(self, key: str) -> State | None:
        raw = self.store.get(key)
        if raw is None:
            return None
        # Round-trip through JSON to match RocksDB behavior (datetime/set encoding)
        serialized = json.dumps(raw, cls=StateEncoder, sort_keys=True)
        return json.loads(serialized, object_hook=state_decoder_hook)

    async def put(self, key: str, state: State) -> None:
        self.store[key] = state

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    async def close(self) -> None:
        pass


class ChangelogStateStore(StateStore):
    """Adapter: state store backed by a compacted Kafka changelog topic.

    Wraps an inner StateStore (typically RocksDB) and produces every state
    change to a Kafka topic. On startup, restore() rebuilds the inner store
    from the changelog, making the local store ephemeral.
    """

    def __init__(self, inner: StateStore, producer: KafkaProducer, topic: str):
        self.inner = inner
        self.producer = producer
        self.topic = topic

    async def get(self, key: str) -> State | None:
        return await self.inner.get(key)

    async def put(self, key: str, state: State) -> None:
        await self.inner.put(key, state)
        await self.producer.send(Message(key=key, topic=self.topic, value=state))

    async def delete(self, key: str) -> None:
        await self.inner.delete(key)
        await self.producer.send(Message(key=key, topic=self.topic, value={}))

    async def close(self) -> None:
        await self.producer.flush()
        await self.inner.close()

    async def restore(self, consumer: KafkaConsumer) -> None:
        """Consume the entire changelog topic to rebuild the inner state store."""
        await consumer.subscribe([self.topic])
        count = 0
        while True:
            messages = await consumer.poll(timeout=2.0)
            if not messages:
                break
            for msg in messages:
                if msg.value:
                    await self.inner.put(msg.key, State(msg.value))
                else:
                    await self.inner.delete(msg.key)
                count += 1
        await consumer.close()
        log.info("Restored %d state entries from %s", count, self.topic)


async def ensure_changelog_topic(brokers: list[str], topic: str, num_partitions: int) -> None:
    """Create the changelog topic if it doesn't exist.

    Uses the Kafka AdminClient API (CreateTopicsRequest), which works even
    when auto.create.topics.enable=false on the broker. This is the same
    mechanism Kafka Streams uses for internal topics.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=",".join(brokers))
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=3,
                topic_configs={"cleanup.policy": "compact"},
            ),
        ])
        log.info("Created changelog topic %s (%d partitions, compacted)", topic, num_partitions)
    except TopicAlreadyExistsError:
        log.debug("Changelog topic %s already exists", topic)
    finally:
        await admin.close()
