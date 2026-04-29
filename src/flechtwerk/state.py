"""State store port and adapters (RocksDB, changelog-backed) + JSON serialization."""
import json
import logging
import pickle
import shutil
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer
from reactor_di import lookup

from .attribute.registry import lookup_encoder
from .kafka import encode_json, restore_changelog
from .types import State

log = logging.getLogger(__name__)


# --- Serialization ---


def serialize(state: State) -> bytes:
    """JSON-only. Reuses `encode_json` so changelog bytes share the same
    settings as event-topic bytes (sort_keys, compact, ensure_ascii=False,
    allow_nan=False)."""
    return encode_json(state)


def deserialize(b: bytes) -> State:
    """Try JSON first; fall back to pickle for legacy bytes from before the
    JSON migration. The pickle path walks raw values through the recursive
    `dict` encoder so any native datetime/set/tuple inside the legacy state
    lands in JSON-native form before being returned."""
    try:
        return State(json.loads(b))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # TODO(legacy-pickle-state): remove this branch once all changelog
        # topics in every environment have rolled over to JSON.
        legacy = pickle.loads(b)  # noqa: S301
        return State(lookup_encoder(dict)(legacy.raw))


# --- Stores ---


class StateStore(ABC):
    """Port: persistent key-value state store.

    Contract: get() returns a protective copy. Callers may mutate the
    returned dict without affecting the store's internal state.

    The abstract storage primitive is `put_bytes` — wire bytes go straight
    to the inner store. The concrete `put` builds on it by serializing the
    `State` first. This keeps changelog restore zero-copy: bytes flow from
    Kafka through `put_bytes` into the store without being deserialized
    until the running stage calls `get` for that specific key.
    """

    @abstractmethod
    async def get(self, key: str) -> State | None:
        ...

    @abstractmethod
    async def put_bytes(self, key: str, raw: bytes) -> None:
        ...

    async def put(self, key: str, state: State) -> None:
        await self.put_bytes(key, serialize(state))

    @abstractmethod
    async def delete(self, key: str) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

class RocksDBStateStore(StateStore):
    """RocksDB-backed state store.

    State values are JSON-serialized via `serialize` (which goes through the
    codec registry). Every put() writes to the RocksDB WAL immediately — no
    periodic snapshots.

    The ``path`` attribute is set by the DI container (reactor-di) or directly
    in tests. The database is opened lazily on first access, so stages that
    never touch state (stateless transformers with zero restored entries)
    never create the RocksDB file at all — and close() is a no-op in that
    case, preserving the "nothing happened" shutdown path.
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
        return deserialize(raw)  # noqa: PyTypeChecker

    async def put_bytes(self, key: str, raw: bytes) -> None:
        # Bytes go straight to RocksDB — both `put` (via the default
        # serialize→put_bytes path) and `restore_changelog` (passing raw
        # wire bytes) land here.
        self.db[key] = raw

    async def delete(self, key: str) -> None:
        try:
            del self.db[key]
        except KeyError:
            pass

    async def close(self) -> None:
        # self.db is a cached_property — accessing it triggers the lazy
        # open. For stages that never touch state (stateless transformers
        # with 0 restored entries, no put()/get() during operation), the
        # DB is never opened, and close() should be a no-op. Otherwise we
        # would open the DB file on shutdown just to close it, producing a
        # confusing "Opened … Closed" pair in the logs.
        if "db" not in self.__dict__:
            return
        self.db.close()
        shutil.rmtree(self.path, ignore_errors=True)
        log.info("Closed and removed RocksDB state store at %s", self.path)


class ChangelogStateStore(StateStore):
    """State store backed by a compacted Kafka changelog topic.

    Wraps an inner StateStore (typically RocksDB) and produces every state
    change to a Kafka topic. On startup, restore() rebuilds the inner store
    from the changelog, making the local store ephemeral.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the runner — for transformers, put() calls
    participate in the runner's open transaction automatically.
    """

    inner: lookup[StateStore, "inner_store"]  # noqa: PyUnresolvedReferences
    producer: AIOKafkaProducer
    topic: lookup[str, "changelog_topic"]  # noqa: PyUnresolvedReferences

    async def get(self, key: str) -> State | None:
        return await self.inner.get(key)

    async def put_bytes(self, key: str, raw: bytes) -> None:
        await self.inner.put_bytes(key, raw)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=raw,
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
        await restore_changelog(consumer, self.topic, self.inner.put_bytes, self.inner.delete)


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
