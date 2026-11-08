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
from .observer import Observer
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

    async def flush(self) -> None:
        """Move buffered writes out of memory. Runners call it once a restore completes.

        A no-op by default: only a store that buffers writes in memory has
        anything to hand back.
        """


class RocksDBMemory:
    """One bounded memory pool shared by every RocksDB store of a stage.

    RocksDB's defaults are per database: each one gets its own 64 MiB memtable
    and its own 32 MiB block cache. A transformer opens one database per task,
    so one replica holding all eight tasks of an eight-partition topic grew
    eight memtables, and a restore — which replays EVERY changelog record, the
    superseded versions of a not-yet-compacted segment included — filled each
    of them before the first flush. Memory then scaled with the number of
    tasks a replica held: measured on rocksdict 0.3.29 (arm64 macOS, RSS),
    eight stores restored from 56 MiB each took +463 MiB, and eight from
    150 MiB each +600 MiB once reads had filled their caches.

    This pool caps the whole stage instead, the bounded-memory setup Kafka
    Streams documents: ONE block cache, and ONE write-buffer manager charging
    every memtable against that same cache, so the memtables cannot grow on
    top of it. The cap is soft: a flush starts at 90% of the write budget,
    writers are not stalled, and what arrives during a flush overshoots a
    little (1.09x measured on a loaded CI runner). Index and filter blocks live in the cache too (level-0's
    pinned), so the bound covers the per-file metadata as well. Measured the
    same way, with each store flushed after its restore (see
    `RocksDBStateStore.flush` — without it, restores ran 4x slower): eight
    stores of 56 MiB took +122 MiB, eight of 150 MiB +127 MiB, at the
    restore speed of the defaults. A store no longer keeps its data in RAM —
    it flushes to disk at the budget, which is the point of having RocksDB.

    The budget is deliberately not a knob: state is small by design (a
    config-topic-sized extractor table, or one partition's keys), and a cache
    only buys read speed the framework has never needed. One pool per stage,
    never per process — several stages in one process each own theirs, like
    everything else keyed by stage (see "Several Stages in One Process").

    ``rocksdict`` is imported only when the first store opens, so a stateless
    stage never loads it.
    """

    CACHE_BYTES = 64 * 2**20
    MEMTABLE_BYTES = 32 * 2**20  # charged against CACHE_BYTES, not added to it

    @cached_property
    def _pool(self) -> tuple[Any, Any]:
        from rocksdict import Cache, WriteBufferManager

        cache = Cache(self.CACHE_BYTES)
        manager = WriteBufferManager.new_write_buffer_manager_with_cache(
            self.MEMTABLE_BYTES, False, cache,
        )
        return cache, manager

    def options(self) -> Any:
        """Fresh `rocksdict.Options` drawing on this pool, one per opened database."""
        from rocksdict import BlockBasedOptions, Options

        cache, manager = self._pool
        table = BlockBasedOptions()
        table.set_block_cache(cache)
        table.set_cache_index_and_filter_blocks(True)
        table.set_pin_l0_filter_and_index_blocks_in_cache(True)
        options = Options()
        options.set_block_based_table_factory(table)
        options.set_write_buffer_manager(manager)
        return options


class RocksDBStateStore(StateStore):
    """RocksDB-backed state store.

    State values are JSON-serialized via `serialize` (which uses each
    `Attribute`'s codec). Every put() writes to the RocksDB WAL immediately — no
    periodic snapshots.

    Memory is bounded per stage, not per store: every store of a stage opens
    with options drawn from the one ``rocksdb_memory`` pool (see
    `RocksDBMemory`).

    The ``path`` and ``rocksdb_memory`` attributes are set by the DI container
    (reactor-di) or directly in tests. The database is opened lazily on first access, so stages that
    never touch state (stateless transformers with zero restored entries)
    never create the RocksDB file at all — and close() is a no-op in that
    case, preserving the "nothing happened" shutdown path.

    close() is a wipe, not an end-of-life: it drops the cached database so a
    later put/get lazily reopens a fresh, empty store at the same path. The
    sharded extractor runner relies on this to discard local state on token
    revocation and re-restore from the changelog on the next assignment.
    """

    path: Path
    rocksdb_memory: RocksDBMemory

    @cached_property
    def db(self):
        from rocksdict import Rdict

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "state.db"
        log.info("Opened RocksDB state store at %s", db_path)
        return Rdict(str(db_path), self.rocksdb_memory.options())

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

    async def flush(self) -> None:
        # A restore fills the memtable and then the store may go quiet for
        # good, holding its share of the stage's write budget forever. The
        # write-buffer manager can only flush the store being WRITTEN, so
        # with restores running store after store, every later store was
        # flushed in slivers of a few MiB — a flood of tiny files that made
        # restoring eight 150 MiB stores take 64 s instead of 17 s. Flushed
        # after restore: 17 s again, with the bound intact.
        if "db" not in self.__dict__:
            return  # never opened: nothing written, and no file to create
        self.db.flush()

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
        del self.db
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

    Every changelog write is weighed through ``observer`` — a state key's whole
    value is ONE Kafka record, so it is subject to the producer's
    ``max_request_size`` (1 MiB by default), and the framework deliberately
    does not guard: an oversized record crashes the stage exactly as aiokafka
    makes it. The metric is how an operator watches the approach instead of
    doing forensics after the fall.
    """

    inner: lookup[StateStore, "inner_store"]  # pyright: ignore[reportInvalidTypeForm, reportUndefinedVariable]  # noqa: PyUnresolvedReferences
    observer: Observer = Observer()
    partition: int | None = None
    producer: AIOKafkaProducer
    topic: lookup[str, "changelog_topic"]  # pyright: ignore[reportInvalidTypeForm, reportUndefinedVariable]  # noqa: PyUnresolvedReferences

    async def get(self, key: str) -> State | None:
        return await self.inner.get(key)

    async def put_bytes(self, key: str, raw: bytes) -> None:
        # Observe BEFORE the send: when a record does blow the ceiling, its
        # fatal size is the last value on the scrape. `restore` bypasses this
        # method entirely (it feeds `inner.put_bytes`), so a replay of the
        # whole changelog can never inflate the distribution.
        self.observer.state_record_bytes(len(raw))
        await self.inner.put_bytes(key, raw)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=raw,
            partition=self.partition,
        )

    async def delete(self, key: str) -> None:
        await self.inner.delete(key)
        # A NULL value, never b"": only null is a Kafka tombstone. Log
        # compaction retains an empty value as the key's latest record
        # forever, so every deleted key would linger in the changelog and be
        # replayed by every restore. (Restore still reads a legacy b"" as a
        # deletion — see `is_tombstone`.)
        await self.producer.send(
            self.topic,
            key=key.encode("utf-8"),
            value=None,
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
        count = await restore_changelog(
            consumer, self.topic, self.inner.put_bytes, self.inner.delete, partitions,
        )
        await self.inner.flush()
        return count


class StatelessStateStore(StateStore):
    """Null store for broker-dispatched extractors — stateless by contract.

    Framework-internal (not exported): `ExtractorRunner` gives it to the
    single synthetic token of a stage whose work an external message broker
    — an MQTT broker, not Kafka — shards across replicas (a stage that took
    ``ExtractorRunner.run_dispatched``). ``get`` reports an
    empty `State` — there is never anything to resume from — and any write
    raises, so a ``State`` yield fails loudly instead of splitting one
    entry's history across replicas that cannot fence each other.

    The error is deliberately not a warning: MQTT-broker dispatch offers
    neither a fenced handover (replicas hold *different* Kafka transactional
    IDs, so both can commit for the same key) nor an assignment event at
    which a newly picked replica could restore. Sticky dispatch narrows the
    race but cannot close it — it is an MQTT-broker-side setting the
    framework can neither set nor observe, and the framework must stay
    correct under ``round_robin`` too.
    """

    async def get(self, key: str) -> State | None:
        return State()

    async def put_bytes(self, key: str, raw: bytes) -> None:
        raise RuntimeError(self._refusal)

    async def delete(self, key: str) -> None:
        raise RuntimeError(self._refusal)

    async def close(self) -> None:
        pass

    @property
    def _refusal(self) -> str:
        return (
            "broker-dispatched stages are stateless by contract — an external message broker "
            "(an MQTT broker, not Kafka) shards their work across replicas with no fenced handover "
            "and no restore point, so a State yield would split one entry's history across "
            "replicas. Subclass Extractor directly for a stateful source: Kafka-leased token "
            "sharding gives it exclusive, fenced state."
        )


CHANGELOG_MAX_COMPACTION_LAG_MS = 60 * 60 * 1000  # 1 h


async def ensure_changelog_topic(admin: Any, topic: str, num_partitions: int = -1) -> bool:
    """Create the changelog topic if it doesn't exist.

    Uses the Kafka AdminClient API (CreateTopicsRequest), which works even
    when auto.create.topics.enable=false on the Kafka broker.

    Args:
        admin: An already-started AIOKafkaAdminClient.
        topic: Changelog topic name.
        num_partitions: Partition count for a newly created topic.
            Transformers pass their (validated) input topic partition count
            so task p's explicit-partition state writes have somewhere to
            land; -1 (extractors) uses the Kafka broker default.

    Returns:
        True if this call created the topic, False if it already existed.
        Callers use this to skip re-describing a topic they just created:
        CreateTopics returns once the controller commits, but the Kafka broker's
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
            topic_configs={
                "cleanup.policy": "compact",
                # Compaction never touches the active segment, and under the
                # defaults (1 GiB, 7 days) a young changelog IS its active
                # segment — every restore would replay every state version ever
                # written. This rolls the segment at the first write after an
                # hour and compacts regardless of the dirty ratio. Measured on
                # Kafka 4.3.1: 500 keys x 20 versions replayed 10000 records
                # under compact alone, 501 with this.
                "max.compaction.lag.ms": str(CHANGELOG_MAX_COMPACTION_LAG_MS),
            },
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
