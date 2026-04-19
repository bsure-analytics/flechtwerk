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

import io
import logging
import pickle
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import aiokafka

from .state import StateStore

KeyRemap = Callable[[str, Any], str | None]

log = logging.getLogger(__name__)

# Bytewax partition key format: "{partition_idx}-{topic}"
BYTEWAX_PARTITION_KEY = re.compile(r"^(\d+)-(.+)$")


class BytewaxUnpickler(pickle.Unpickler):
    """Unpickler that maps Bytewax-era ds.shared types to fretworx.types."""

    def find_class(self, module: str, name: str) -> type:
        if module == "ds.shared" and name in ("Config", "Event", "State"):
            module = "fretworx.types"
        return super().find_class(module, name)


def unpickle(data: bytes) -> Any:
    return BytewaxUnpickler(io.BytesIO(data)).load()


def read_bytewax_sqlite(sqlite_path: Path) -> tuple[dict[str, Any], dict[str, int]]:
    """Read state and Kafka offsets from a Bytewax SQLite recovery database.

    Bytewax stores state snapshots in a `snaps` table with columns:
    - step_id: Bytewax operator name (e.g., "...kafka_input")
    - state_key: "{partition_idx}-{topic}" for Kafka offsets, or app state key
    - snap_epoch: snapshot epoch number
    - ser_change: pickled value (int for offsets, dict for app state)

    Returns (states, offsets) where:
    - states: {key: state_dict} — application state
    - offsets: {"{partition_idx}-{topic}": offset_int} — Kafka partition offsets
    """
    states: dict[str, Any] = {}
    offsets: dict[str, int] = {}

    try:
        conn = sqlite3.connect(str(sqlite_path))
        rows = conn.execute(
            "SELECT step_id, state_key, ser_change FROM snaps"
        ).fetchall()

        for step_id, state_key, ser_change in rows:
            if not isinstance(ser_change, bytes):
                continue
            try:
                value = unpickle(ser_change)
            except (pickle.UnpicklingError, TypeError, ValueError):
                log.warning("Could not unpickle snap for step_id=%s, state_key=%s", step_id, state_key)
                continue

            if isinstance(value, int) and value >= 0 and BYTEWAX_PARTITION_KEY.match(state_key):
                offsets[state_key] = value
                log.debug("Found offset %s = %d (step_id=%s)", state_key, value, step_id)
            elif isinstance(value, dict):
                states[state_key] = value
                log.debug("Found state for key %s (step_id=%s)", state_key, step_id)
            elif isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
                states[state_key] = value[0]
                log.debug("Found state for key %s (step_id=%s, unwrapped list)", state_key, step_id)

        conn.close()
    except sqlite3.DatabaseError:
        log.warning("Could not open SQLite database: %s", sqlite_path)

    return states, offsets


async def migrate_bytewax_to_fretworx(
    state_store: StateStore,
    state_dir: Path,
    group_id: str,
    producer: Any | None = None,
    *,
    key_remap: KeyRemap | None = None,
) -> None:
    """Migrate Bytewax SQLite state to the changelog-backed state store and optionally commit Kafka offsets.

    When producer is provided (transformer path), Kafka consumer offsets are
    committed via send_offsets_to_transaction. When omitted (extractor path),
    offset commits are skipped — extractors re-read config topics from earliest.

    key_remap, if provided, maps each Bytewax-era state key to the key under
    which it should be written to the Fretworx changelog. Returning None
    drops the entry. Passing through the original key (the default) is
    achieved by returning it unchanged.
    """
    sqlite_files = sorted(state_dir.glob("part-*.sqlite3"))

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
        written = 0
        dropped = 0
        for bytewax_key, state in all_states.items():
            if not isinstance(state, dict):
                log.warning("Skipping non-dict state for key %s: %s", bytewax_key, type(state))
                continue
            target_key = key_remap(bytewax_key, state) if key_remap else bytewax_key
            if target_key is None:
                log.info("Dropping state for Bytewax key %s (remap returned None)", bytewax_key)
                dropped += 1
                continue
            await state_store.put(target_key, state)
            if target_key == bytewax_key:
                log.info("Migrated state for key: %s", target_key)
            else:
                log.info("Migrated state, re-keyed %s -> %s", bytewax_key, target_key)
            written += 1
        log.info("State migration complete: %d written, %d dropped", written, dropped)
    else:
        log.info("No application state found in SQLite databases")

    # Commit Kafka consumer offsets via the transactional producer (transformers only)
    if all_offsets and producer is not None:
        tp_offsets = {
            aiokafka.TopicPartition(m.group(2), int(m.group(1))): offset
            for key, offset in all_offsets.items()
            if (m := BYTEWAX_PARTITION_KEY.match(key))
        }
        if tp_offsets:
            await producer.send_offsets_to_transaction(tp_offsets, group_id)
            for tp, offset in sorted(tp_offsets.items(), key=lambda x: (x[0].topic, x[0].partition)):
                log.info("Committed offset %d for %s/%d in group %s", offset, tp.topic, tp.partition, group_id)
        else:
            log.warning("No valid partition offsets found — skipping Kafka offset commit")
    elif all_offsets and producer is None:
        log.info("Skipping Kafka offset commit (extractor — config topics re-read from earliest)")
    else:
        log.warning("No Kafka offsets found in SQLite databases — transformer may reprocess from earliest")
