"""State store port and adapters (RocksDB, changelog-backed) + JSON serialization."""
import json
import logging
import shutil
from abc import ABC, abstractmethod
from functools import cached_property
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer
from reactor_di import lookup

from .kafka import encode_json, restore_changelog
from .types import State

log = logging.getLogger(__name__)

__all__ = ["StateStore"]


# --- Serialization ---


def serialize(state: State) -> bytes:
    """JSON-only. Reuses `encode_json` so changelog bytes share the same
    settings as event-topic bytes (sort_keys, compact, ensure_ascii=False,
    allow_nan=False)."""
    return encode_json(state)


def deserialize(b: bytes) -> State:
    """JSON-only counterpart of `serialize`. Undecodable bytes are an
    unrecoverable data error — crash, then reset the affected state.
    Deliberately no pickle fallback: pickle resolves classes by the module
    path baked into the bytes, so any package rename would invalidate every
    record."""
    return State.wrap(json.loads(b))


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

    State values are JSON-serialized via `serialize` (which uses each
    `Attribute`'s codec). Every put() writes to the RocksDB WAL immediately — no
    periodic snapshots.

    The ``path`` attribute is set by the DI container (reactor-di) or directly
    in tests. The database is opened lazily on first access, so stages that
    never touch state (stateless transformers with zero restored entries)
    never create the RocksDB file at all — and close() is a no-op in that
    case, preserving the "nothing happened" shutdown path.

    close() is a wipe, not an end-of-life: it drops the cached database so a
    later put/get lazily reopens a fresh, empty store at the same path. The
    sharded extractor runner relies on this to discard local state on token
    revocation and re-restore from the changelog on the next assignment.
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
        assert isinstance(raw, bytes)  # we only ever put bytes; rocksdict's stub widens to Any
        return deserialize(raw)

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
        # Drop the cached_property so the next access reopens a fresh, empty
        # store — close() must be a wipe, not an end-of-life (see class doc).
        del self.__dict__["db"]
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

    ``partition`` pins every changelog write to one explicit partition —
    transformers use one store per task (input partition) so state lands in
    the changelog partition matching the records that produced it, regardless
    of what the state key hashes to. ``None`` (extractors) leaves routing to
    the default key-hash partitioner.
    """

    inner: lookup[StateStore, "inner_store"]  # noqa: PyUnresolvedReferences
    partition: int | None = None
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
            partition=self.partition,
        )

    async def delete(self, key: str) -> None:
        await self.inner.delete(key)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=b"",
            partition=self.partition,
        )

    async def close(self) -> None:
        await self.inner.close()

    async def restore(self, consumer: Any, partitions: set[int] | None = None) -> int:
        """Rebuild the inner store from the changelog topic.

        Args:
            consumer: An already-started AIOKafkaConsumer (group_id=None).
            partitions: Restrict the restore to these changelog partitions
                (transformer task restore). None restores all partitions.

        Returns:
            Number of records processed.
        """
        return await restore_changelog(
            consumer, self.topic, self.inner.put_bytes, self.inner.delete, partitions,
        )


async def ensure_changelog_topic(admin: Any, topic: str, num_partitions: int = -1) -> bool:
    """Create the changelog topic if it doesn't exist.

    Uses the Kafka AdminClient API (CreateTopicsRequest), which works even
    when auto.create.topics.enable=false on the broker.

    Args:
        admin: An already-started AIOKafkaAdminClient.
        topic: Changelog topic name.
        num_partitions: Partition count for a newly created topic.
            Transformers pass their (validated) input topic partition count
            so task p's explicit-partition state writes have somewhere to
            land; -1 (extractors) uses the broker default.

    Returns:
        True if this call created the topic, False if it already existed.
        Callers use this to skip re-describing a topic they just created:
        CreateTopics returns once the controller commits, but the broker's
        metadata cache catches up asynchronously, so an immediate describe
        can still raise UnknownTopicOrPartitionError.
    """
    from aiokafka.admin import NewTopic
    from aiokafka.errors import TopicAlreadyExistsError, for_code

    response = await admin.create_topics([
        NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=-1,
            replica_assignments={},
            topic_configs={"cleanup.policy": "compact"},
        ),
    ])
    for t, error_code, *rest in response.topic_errors:
        error = for_code(error_code)
        if error is TopicAlreadyExistsError:
            log.debug("Changelog topic %s already exists", t)
            return False
        elif error_code != 0:
            error_message = rest[0] if rest else ""
            raise error(f"{t}: {error_message}")
        else:
            log.info("Created changelog topic %s (compacted)", t)
    return True
