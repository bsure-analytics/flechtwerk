"""Bytewax → fretworx state migration.

Reads Bytewax SQLite recovery databases, extracts pickled state and Kafka
offsets, writes state through the changelog-backed state store (so it's
durably stored in Kafka), and commits Kafka consumer offsets.

Bytewax does not use Kafka consumer groups (group.id="BYTEWAX_IGNORED").
It stores per-partition offsets in its SQLite recovery database as ints
keyed by "{partition_idx}-{topic}". This script extracts those offsets
and commits them to the fretworx consumer group so transformers resume
from where Bytewax left off.

Called automatically by the fretworx runner on first startup when SQLite
state exists.
"""
from __future__ import annotations

import logging
import pickle
import re
import sqlite3
from pathlib import Path
from typing import Any

from .state import StateStore

log = logging.getLogger(__name__)

# Bytewax partition key format: "{partition_idx}-{topic}"
BYTEWAX_PARTITION_KEY = re.compile(r"^(\d+)-(.+)$")


def read_bytewax_sqlite(sqlite_path: Path) -> tuple[dict[str, Any], dict[str, int]]:
    """Read state and Kafka offsets from a Bytewax SQLite recovery database.

    Returns (states, offsets) where:
    - states: {key: state_dict} — application state
    - offsets: {"{partition_idx}-{topic}": offset_int} — Kafka partition offsets
    """
    states: dict[str, Any] = {}
    offsets: dict[str, int] = {}

    try:
        conn = sqlite3.connect(str(sqlite_path))
        cursor = conn.cursor()

        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        for table_name in table_names:
            try:
                rows = cursor.execute(f"SELECT * FROM \"{table_name}\"").fetchall()  # noqa: S608
                for row in rows:
                    for cell in row:
                        if isinstance(cell, bytes):
                            try:
                                unpickled = pickle.loads(cell)  # noqa: S301
                                if isinstance(unpickled, dict):
                                    for k, v in unpickled.items():
                                        if isinstance(k, str) and isinstance(v, int) and BYTEWAX_PARTITION_KEY.match(k):
                                            offsets[k] = v
                                        elif isinstance(k, str) and isinstance(v, dict):
                                            states[k] = v
                                elif isinstance(unpickled, (list, tuple)) and len(unpickled) == 2:
                                    k, v = unpickled
                                    if isinstance(k, str) and isinstance(v, dict):
                                        states[k] = v
                                    elif isinstance(k, str) and isinstance(v, int) and BYTEWAX_PARTITION_KEY.match(k):
                                        offsets[k] = v
                            except (pickle.UnpicklingError, TypeError, ValueError):
                                pass
            except sqlite3.OperationalError:
                log.warning("Could not read table %s in %s", table_name, sqlite_path)

        conn.close()
    except sqlite3.DatabaseError:
        log.warning("Could not open SQLite database: %s", sqlite_path)

    return states, offsets


async def commit_consumer_offsets(
    brokers: list[str],
    consumer_group: str,
    offsets: dict[str, int],
) -> None:
    """Commit Bytewax partition offsets to the fretworx consumer group."""
    from aiokafka import AIOKafkaConsumer as AIOConsumer
    from aiokafka import TopicPartition

    tp_offsets: dict[TopicPartition, int] = {}
    for key, offset in offsets.items():
        match = BYTEWAX_PARTITION_KEY.match(key)
        if match:
            partition_idx = int(match.group(1))
            topic = match.group(2)
            tp_offsets[TopicPartition(topic, partition_idx)] = offset

    if not tp_offsets:
        log.warning("No valid partition offsets found — skipping Kafka offset commit")
        return

    topics = sorted({tp.topic for tp in tp_offsets})

    consumer = AIOConsumer(
        *topics,
        bootstrap_servers=",".join(brokers),
        group_id=consumer_group,
        enable_auto_commit=False,
    )
    await consumer.start()

    try:
        await consumer.commit(tp_offsets)
        for tp, offset in sorted(tp_offsets.items(), key=lambda x: (x[0].topic, x[0].partition)):
            log.info("Committed offset %d for %s/%d in group %s", offset, tp.topic, tp.partition, consumer_group)
    finally:
        await consumer.stop()


async def migrate_bytewax_to_fretworx(
    state_store: StateStore,
    state_dir: str,
    brokers: list[str],
    application_id: str,
) -> None:
    """Migrate Bytewax SQLite state to the changelog-backed state store and commit Kafka offsets."""
    state_path = Path(state_dir)
    sqlite_files = sorted(state_path.glob("part-*.sqlite3"))

    if not sqlite_files:
        log.info("No SQLite recovery databases found in %s — nothing to migrate", state_dir)
        return

    log.info("Found %d SQLite recovery database(s) in %s", len(sqlite_files), state_dir)

    # Collect state and offsets from all partition files
    all_states: dict[str, Any] = {}
    all_offsets: dict[str, int] = {}
    for sqlite_file in sqlite_files:
        log.info("Reading %s", sqlite_file.name)
        partition_states, partition_offsets = read_bytewax_sqlite(sqlite_file)
        all_states.update(partition_states)
        all_offsets.update(partition_offsets)

    # Write state through the changelog-backed store (durably persisted to Kafka)
    if all_states:
        for key, state in all_states.items():
            if isinstance(state, dict):
                await state_store.put(key, state)
                log.info("Migrated state for key: %s", key)
            else:
                log.warning("Skipping non-dict state for key %s: %s", key, type(state))
        log.info("State migration complete: %d state(s) written", len(all_states))
    else:
        log.info("No application state found in SQLite databases")

    # Commit Kafka consumer offsets
    if all_offsets:
        log.info("Found %d Kafka partition offset(s) — committing to group %s", len(all_offsets), application_id)
        await commit_consumer_offsets(brokers, application_id, all_offsets)
    else:
        log.warning("No Kafka offsets found in SQLite databases — transformer may reprocess from earliest")
