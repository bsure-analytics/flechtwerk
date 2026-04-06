"""State store port and adapters (RocksDB, in-memory, changelog-backed)."""
from __future__ import annotations

import json
import logging
import pickle
import shutil
from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any

from reactor_di import lookup

from .kafka import KafkaProducer, restore_changelog
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

    The ``path`` attribute is set by the DI container (reactor-di) or directly
    in tests. The database is opened lazily on first access.
    """

    path: Path

    @cached_property
    def db(self):
        from rocksdict import Rdict

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "state.db"
        log.info("Opened RocksDB state store at %s", db_path)
        return Rdict(str(db_path))

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
        shutil.rmtree(self.path, ignore_errors=True)
        log.info("Closed and removed RocksDB state store at %s", self.path)


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

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the runner — for transformers, put() calls
    participate in the runner's open transaction automatically.
    """

    inner: lookup[StateStore, "inner_store"]
    producer: KafkaProducer
    topic: lookup[str, "changelog_topic"]

    async def get(self, key: str) -> State | None:
        return await self.inner.get(key)

    async def put(self, key: str, state: State) -> None:
        await self.inner.put(key, state)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=pickle.dumps(dict(state)),
        )

    async def delete(self, key: str) -> None:
        await self.inner.delete(key)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=b"",
        )

    async def close(self) -> None:
        await self.inner.close()

    async def restore(self, consumer: Any) -> None:
        """Rebuild the inner store from the changelog topic.

        Args:
            consumer: An already-started AIOKafkaConsumer (group_id=None).
        """
        await restore_changelog(consumer, self.topic, self.inner.put, self.inner.delete)


async def ensure_changelog_topic(admin: Any, topic: str) -> None:
    """Create the changelog topic if it doesn't exist.

    Uses broker defaults for partition count and replication factor.
    Uses the Kafka AdminClient API (CreateTopicsRequest), which works even
    when auto.create.topics.enable=false on the broker.

    Args:
        admin: An already-started AIOKafkaAdminClient.
        topic: Changelog topic name.
    """
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError, for_code

    response = await admin.create_topics([
        NewTopic(
            name=topic,
            num_partitions=-1,
            replication_factor=-1,
            replica_assignments={},
            topic_configs={"cleanup.policy": "compact"},
        ),
    ])
    for t, error_code, *rest in response.topic_errors:
        error = for_code(error_code)
        if error is TopicAlreadyExistsError:
            log.debug("Changelog topic %s already exists", t)
        elif error_code != 0:
            error_message = rest[0] if rest else ""
            raise error(f"{t}: {error_message}")
        else:
            log.info("Created changelog topic %s (compacted)", t)
