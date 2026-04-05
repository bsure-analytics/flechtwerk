"""State store port and adapters (RocksDB, in-memory, changelog-backed)."""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from .kafka import KafkaProducer, encode_json, restore_changelog
from .types import State

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
    """RocksDB-backed state store.

    State values are JSON-serialized with custom encoding for datetime/set/tuple.
    Every put() writes to the RocksDB WAL immediately — no periodic snapshots.
    """

    def __init__(self, path: Path):
        from rocksdict import Rdict

        path.mkdir(parents=True, exist_ok=True)
        self.db_path = path / "state.db"
        self.db = Rdict(str(self.db_path))
        log.info("Opened RocksDB state store at %s", self.db_path)

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
        log.info("Closed RocksDB state store at %s", self.db_path)


class InMemoryStateStore(StateStore):
    """In-memory state store for testing."""

    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key: str) -> State | None:
        raw = self.store.get(key)
        if raw is None:
            return None
        return json.loads(raw, object_hook=state_decoder_hook)

    async def put(self, key: str, state: State) -> None:
        self.store[key] = json.dumps(state, cls=StateEncoder, sort_keys=True)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    async def close(self) -> None:
        pass


class ChangelogStateStore(StateStore):
    """State store backed by a compacted Kafka changelog topic.

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
        await self.producer.send(
            self.topic,
            key=encode_json(key),
            value=encode_json(dict(state)),
        )

    async def delete(self, key: str) -> None:
        await self.inner.delete(key)
        await self.producer.send(
            self.topic,
            key=encode_json(key),
            value=encode_json({}),
        )

    async def close(self) -> None:
        await self.producer.flush()
        await self.inner.close()

    async def restore(self, bootstrap_servers: str) -> None:
        """Rebuild the inner store from the changelog topic."""
        await restore_changelog(bootstrap_servers, self.topic, self.inner.put, self.inner.delete)


async def get_max_partition_count(bootstrap_servers: str, topics: list[str]) -> int:
    """Query the broker for the maximum partition count across topics."""
    import aiokafka

    consumer = aiokafka.AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    await consumer.start()
    try:
        return max([len(consumer.partitions_for_topic(t) or []) for t in topics], default=1)
    finally:
        await consumer.stop()


async def ensure_changelog_topic(bootstrap_servers: str, topic: str, num_partitions: int) -> None:
    """Create the changelog topic if it doesn't exist.

    Uses the Kafka AdminClient API (CreateTopicsRequest), which works even
    when auto.create.topics.enable=false on the broker. This is the same
    mechanism Kafka Streams uses for internal topics.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
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
