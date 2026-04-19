"""Tests for the Bytewax → Fretworx migration, focused on the key-remap path."""
import asyncio
import pickle
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock

import aiokafka
import pytest

from fretworx.migrate_state import migrate_bytewax_to_fretworx
from fretworx.state import InMemoryStateStore


def write_bytewax_sqlite(path: Path, snaps: list[tuple[str, str, bytes]]) -> None:
    """Write a minimal Bytewax recovery database.

    snaps: list of (step_id, state_key, pickled ser_change) tuples.
    """
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE snaps ("
        "step_id TEXT, state_key TEXT, snap_epoch INTEGER, ser_change BLOB)"
    )
    for step_id, state_key, ser_change in snaps:
        conn.execute(
            "INSERT INTO snaps (step_id, state_key, snap_epoch, ser_change) VALUES (?, ?, ?, ?)",
            (step_id, state_key, 0, ser_change),
        )
    conn.commit()
    conn.close()


def test_migrate_passes_keys_through_when_no_remap_given(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "api_key_A", pickle.dumps({"cursor": 1})),
            ("step_id", "api_key_B", pickle.dumps({"cursor": 2})),
        ])

        store = InMemoryStateStore()
        await migrate_bytewax_to_fretworx(store, tmp_path, group_id="g")

        assert await store.get("api_key_A") == {"cursor": 1}
        assert await store.get("api_key_B") == {"cursor": 2}

    asyncio.run(run())


def test_migrate_remap_rekeys_matching_entries(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "api_key_A", pickle.dumps({"cursor": 1})),
            ("step_id", "api_key_B", pickle.dumps({"cursor": 2})),
            ("step_id", "*** pickle.dumps({"seen": True})),  # already msg.key-shaped
        ])

        api_key_to_msg_key = {"api_key_A": "*** "api_key_B": "***

        def remap(bytewax_key: str, _state: object) -> str:
            return api_key_to_msg_key.get(bytewax_key, bytewax_key)

        store = InMemoryStateStore()
        await migrate_bytewax_to_fretworx(store, tmp_path, group_id="g", key_remap=remap)

        # Re-keyed: api_keys gone, msg.keys in their place with the same state values.
        assert await store.get("*** == {"cursor": 1}
        assert await store.get("*** == {"cursor": 2}
        assert await store.get("api_key_A") is None
        assert await store.get("api_key_B") is None
        # Pass-through: custom-keyed state survives unchanged.
        assert await store.get("*** == {"seen": True}

    asyncio.run(run())


def test_migrate_commits_offsets_when_topics_match_input_topics(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "0-sumup-details", pickle.dumps(100)),
            ("step_id", "1-sumup-details", pickle.dumps(200)),
        ])

        producer = AsyncMock()

        store = InMemoryStateStore()
        await migrate_bytewax_to_fretworx(
            store, tmp_path, group_id="g", producer=producer,
            input_topics=["sumup-details"],
        )

        producer.send_offsets_to_transaction.assert_awaited_once()
        committed, group_id = producer.send_offsets_to_transaction.await_args.args
        assert group_id == "g"
        assert committed == {
            aiokafka.TopicPartition("sumup-details", 0): 100,
            aiokafka.TopicPartition("sumup-details", 1): 200,
        }

    asyncio.run(run())


def test_migrate_raises_when_offset_topic_not_in_input_topics(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "0-fret-sumup-details", pickle.dumps(100)),
        ])

        producer = AsyncMock()

        store = InMemoryStateStore()
        with pytest.raises(ValueError, match="fret-sumup-details"):
            await migrate_bytewax_to_fretworx(
                store, tmp_path, group_id="g", producer=producer,
                input_topics=["sumup-details"],
            )

        producer.send_offsets_to_transaction.assert_not_awaited()

    asyncio.run(run())


def test_migrate_without_input_topics_commits_offsets_verbatim(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "0-any-topic", pickle.dumps(100)),
        ])

        producer = AsyncMock()

        store = InMemoryStateStore()
        await migrate_bytewax_to_fretworx(
            store, tmp_path, group_id="g", producer=producer,
        )

        producer.send_offsets_to_transaction.assert_awaited_once()
        committed, _ = producer.send_offsets_to_transaction.await_args.args
        assert committed == {aiokafka.TopicPartition("any-topic", 0): 100}

    asyncio.run(run())


def test_migrate_remap_returning_none_drops_entry(tmp_path):
    async def run():
        sqlite_path = tmp_path / "part-0.sqlite3"
        write_bytewax_sqlite(sqlite_path, [
            ("step_id", "keep_me", pickle.dumps({"x": 1})),
            ("step_id", "drop_me", pickle.dumps({"x": 2})),
        ])

        def remap(bytewax_key: str, _state: object) -> str | None:
            return None if bytewax_key == "drop_me" else bytewax_key

        store = InMemoryStateStore()
        await migrate_bytewax_to_fretworx(store, tmp_path, group_id="g", key_remap=remap)

        assert await store.get("keep_me") == {"x": 1}
        assert await store.get("drop_me") is None

    asyncio.run(run())
