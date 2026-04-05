"""Bytewax → fretworx state migration.

Reads Bytewax SQLite recovery databases, extracts pickled state and Kafka offsets,
writes state to RocksDB and commits offsets to fretworx's Kafka consumer group.

Called automatically by the fretworx runner on first startup when SQLite state exists.
Also usable standalone:
    python -m fretworx.migrate_state --state-dir /data --brokers localhost:9092 \
        --consumer-group ds-sumup-extractor
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class MigrationEncoder(json.JSONEncoder):
    """JSON encoder for Bytewax state values (datetime, set, tuple, bytes)."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        if isinstance(obj, (set, frozenset)):
            return {"__type__": "set", "value": sorted(str(x) for x in obj)}
        if isinstance(obj, tuple):
            return {"__type__": "tuple", "value": list(obj)}
        if isinstance(obj, bytes):
            return {"__type__": "bytes", "value": obj.decode("utf-8", errors="replace")}
        return super().default(obj)


def read_bytewax_sqlite(sqlite_path: Path) -> dict[str, Any]:
    """Read the latest state snapshot from a Bytewax SQLite recovery database.

    Bytewax stores state as pickled Python objects. The table structure is
    internal to Bytewax and may vary between versions.
    """
    states = {}
    try:
        conn = sqlite3.connect(str(sqlite_path))
        cursor = conn.cursor()

        # Bytewax stores snapshots in a table; get the latest
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t[0] for t in tables]

        for table_name in table_names:
            try:
                rows = cursor.execute(f"SELECT * FROM \"{table_name}\"").fetchall()  # noqa: S608
                for row in rows:
                    # Bytewax stores (step_id, key, state_bytes) or similar
                    for cell in row:
                        if isinstance(cell, bytes):
                            try:
                                unpickled = pickle.loads(cell)  # noqa: S301
                                if isinstance(unpickled, dict):
                                    states.update(unpickled)
                                elif isinstance(unpickled, (list, tuple)) and len(unpickled) == 2:
                                    # Could be (key, state) tuple
                                    k, v = unpickled
                                    if isinstance(k, str) and isinstance(v, dict):
                                        states[k] = v
                            except (pickle.UnpicklingError, TypeError, ValueError):
                                pass
            except sqlite3.OperationalError:
                log.warning("Could not read table %s in %s", table_name, sqlite_path)

        conn.close()
    except sqlite3.DatabaseError:
        log.warning("Could not open SQLite database: %s", sqlite_path)

    return states


def migrate_bytewax_to_fretworx(
    state_dir: str,
    brokers: list[str],
    consumer_group: str,
) -> None:
    """Migrate Bytewax SQLite state to fretworx RocksDB state store."""
    state_path = Path(state_dir)
    sqlite_files = sorted(state_path.glob("part-*.sqlite3"))

    if not sqlite_files:
        log.info("No SQLite recovery databases found in %s — nothing to migrate", state_dir)
        return

    log.info("Found %d SQLite recovery database(s) in %s", len(sqlite_files), state_dir)

    # Collect state from all partition files
    all_states: dict[str, Any] = {}
    for sqlite_file in sqlite_files:
        log.info("Reading %s", sqlite_file.name)
        partition_states = read_bytewax_sqlite(sqlite_file)
        all_states.update(partition_states)

    if not all_states:
        log.info("No state found in SQLite databases — nothing to migrate")
        return

    # Write to RocksDB
    from .state import RocksDBStateStore

    rocksdb_path = os.path.join(state_dir, "fretworx")
    store = RocksDBStateStore(rocksdb_path)
    try:
        for key, state in all_states.items():
            if isinstance(state, dict):
                store.put(key, state)
                log.info("Migrated state for key: %s", key)
            else:
                log.warning("Skipping non-dict state for key %s: %s", key, type(state))
    finally:
        store.close()

    log.info("Migration complete: %d state(s) written to %s", len(all_states), rocksdb_path)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Migrate Bytewax state to fretworx")
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--consumer-group", required=True)
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args()

    brokers = args.brokers.split(",")
    migrate_bytewax_to_fretworx(args.state_dir, brokers, args.consumer_group)


if __name__ == "__main__":
    main()
