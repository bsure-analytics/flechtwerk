"""State store port and adapters (SQLite, changelog-backed) + JSON serialization."""
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


class StateDatabase:
    """One SQLite database per stage: every local store of the stage is a table in it.

    The local store is a disposable copy of the changelog, never a source of
    truth: it lives in a fresh temporary directory per stage, a revoke wipes
    it, an assignment restores it, and a crashed process never reopens it.
    So the database turns off everything that exists to survive a crash —
    no rollback journal, no fsync, an exclusive lock — and keeps only what
    the stage needs: point reads and writes that can spill to disk.

    Memory is bounded per stage, not per store. A transformer holds one store
    per task, and a store per database would give each its own page cache,
    so a replica's memory would grow with the tasks it held (measured on the
    RocksDB backend this replaced: eight stores restored from 56 MiB each
    took +463 MiB until they shared one budget). Every store of the stage
    is therefore a table behind ONE connection with ONE page cache of
    ``CACHE_BYTES``; state beyond it lives in the file and the OS page
    cache, like any other file. The budget is deliberately not a knob: state
    is small by design (a config-topic-sized extractor table, or one
    partition's keys). One database per stage, never per process — several
    stages in one process each own theirs, like everything else keyed by
    stage (see "Several Stages in One Process").

    Writes are grouped into transactions of up to ``COMMIT_EVERY`` writes,
    committed early by `commit` (each store's ``flush``). A transaction is
    the unit SQLite writes out, so committing every write separately made
    restoring state ten times the cache budget five times slower (58 s
    instead of 17 s). Uncommitted writes are visible to every store of the
    stage — there is only the one connection — and nothing outside it ever
    reads the file, so grouping changes no result.

    Measured against rocksdict 0.3.29 (eight stores, Linux container,
    512 MiB memory limit): the production shape (8 x 62k keys, 120 B
    records) restored in 0.5 s instead of 0.8 s with equal memory, and
    state ten times the budget (4M records) in 17 s instead of 7 s — the
    one regime where an LSM tree wins, and not one the framework targets.

    Dropping the last table closes the connection and deletes the file, so
    the database costs nothing once no store holds state, and the next
    store to open starts from an empty file. ``sqlite3`` is imported only
    when the first store opens, so a stateless stage never loads it.
    """

    CACHE_BYTES = 64 * 2**20
    COMMIT_EVERY = 10_000

    path: Path

    def __init__(self) -> None:
        self.pending = 0
        self.tables: set[str] = set()

    @cached_property
    def connection(self) -> Any:
        import sqlite3

        self.path.mkdir(parents=True, exist_ok=True)
        file = self.path / "state.sqlite"
        # isolation_level=None: the module issues no implicit BEGIN; the
        # transactions are ours (see `write`).
        connection = sqlite3.connect(file, isolation_level=None)
        for pragma in (
            "journal_mode = OFF",       # no rollback: a failed process discards the file anyway
            "synchronous = OFF",        # no fsync: the file never outlives the process
            "locking_mode = EXCLUSIVE",  # one connection, so take the lock once
            f"cache_size = -{self.CACHE_BYTES // 1024}",  # negative = KiB
        ):
            connection.execute(f"PRAGMA {pragma}")
        log.info("Opened SQLite state database at %s", file)
        return connection

    def open_table(self, table: str) -> None:
        self.connection.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (k TEXT PRIMARY KEY, v BLOB NOT NULL) WITHOUT ROWID')
        self.tables.add(table)

    def write(self, sql: str, params: tuple[Any, ...]) -> None:
        connection = self.connection
        if not connection.in_transaction:
            connection.execute("BEGIN")
        connection.execute(sql, params)
        self.pending += 1
        if self.pending >= self.COMMIT_EVERY:
            self.commit()

    def commit(self) -> None:
        if "connection" in self.__dict__ and self.connection.in_transaction:
            self.connection.execute("COMMIT")
        self.pending = 0

    def drop_table(self, table: str) -> None:
        self.tables.discard(table)
        if self.tables:
            self.connection.execute(f'DROP TABLE IF EXISTS "{table}"')
            return
        # The last table: delete the file instead, which is instant, where a
        # DROP TABLE frees page by page — and leaves no file behind.
        self.connection.close()
        del self.connection
        self.pending = 0
        shutil.rmtree(self.path, ignore_errors=True)
        log.info("Closed and removed SQLite state database at %s", self.path)


class SQLiteStateStore(StateStore):
    """SQLite-backed state store: one table in the stage's `StateDatabase`.

    State values are JSON-serialized via `serialize` (which uses each
    `Attribute`'s codec) and stored as the changelog's wire bytes.

    The ``state_database`` attribute is set by the DI container (reactor-di) or
    directly in tests; ``table`` names this store's table, one per
    transformer task. The table is created lazily on first access, so stages
    that never touch state (stateless transformers with zero restored
    entries) never create the database file at all — and close() and
    flush() are no-ops in that case, preserving the "nothing happened"
    shutdown path.

    close() is a wipe, not an end-of-life: it drops the table so a later
    put/get lazily recreates a fresh, empty one. The sharded extractor runner
    relies on this to discard local state on token revocation and re-restore
    from the changelog on the next assignment.
    """

    state_database: StateDatabase
    table: str = "state"

    @property
    def opened(self) -> bool:
        return self.table in self.state_database.tables

    def open(self) -> None:
        if not self.opened:
            self.state_database.open_table(self.table)

    async def get(self, key: str) -> State | None:
        self.open()
        row = self.state_database.connection.execute(f'SELECT v FROM "{self.table}" WHERE k = ?', (key,)).fetchone()
        return None if row is None else deserialize(row[0])

    async def put_bytes(self, key: str, raw: bytes) -> None:
        # Bytes go straight to SQLite — both `put` (via the default
        # serialize→put_bytes path) and `restore_changelog` (passing raw
        # wire bytes) land here.
        self.open()
        self.state_database.write(f'INSERT OR REPLACE INTO "{self.table}" VALUES (?, ?)', (key, raw))

    async def delete(self, key: str) -> None:
        self.open()
        self.state_database.write(f'DELETE FROM "{self.table}" WHERE k = ?', (key,))

    async def flush(self) -> None:
        # A restore ends with a transaction of up to COMMIT_EVERY writes
        # still open; committing turns its dirty pages into clean ones the
        # shared cache may evict, rather than leaving them pinned until the
        # store's next write, which may never come.
        if self.opened:
            self.state_database.commit()

    async def close(self) -> None:
        if not self.opened:
            return
        self.state_database.drop_table(self.table)


class ChangelogStateStore(StateStore):
    """State store backed by a compacted Kafka changelog topic.

    Wraps an inner StateStore (typically SQLite) and produces every state
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
