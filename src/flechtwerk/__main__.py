"""CLI entry point for fretworx.

Usage:
    python -m fretworx ds.ariadne.extractor

Bytewax CLI args are accepted for drop-in compatibility:
    python -m fretworx -w 2 -r /data -s 60 -b 0 ds.ariadne.extractor

The -r/--state-dir flag enables one-time Bytewax SQLite state migration.
Without it, no migration is attempted.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Final

import aiokafka

from .extractor import Extractor, ExtractorRunner
from .migrate_state import migrate_bytewax_to_fretworx
from .state import ChangelogStateStore, InMemoryStateStore, RocksDBStateStore, StateStore, ensure_changelog_topic
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)

# Comma-separated Kafka broker addresses
KAFKA_BOOTSTRAP_SERVERS: Final = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Log level (DEBUG, INFO, WARNING, ERROR)
LOGLEVEL: Final = os.getenv("LOGLEVEL", "INFO")


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI args, accepting both Bytewax and fretworx formats."""
    parser = argparse.ArgumentParser(
        prog="fretworx",
        description="fretworx stream processing framework",
    )
    # Bytewax compatibility flags (accepted but ignored by fretworx)
    parser.add_argument("-b", type=int, default=0, help="(Bytewax compat) backup interval — ignored")
    parser.add_argument("-s", type=int, default=60, help="(Bytewax compat) snapshot interval — ignored")
    parser.add_argument("-w", type=int, default=1, help="(Bytewax compat) worker count — ignored")

    # Bytewax state directory — only used for migration (finding SQLite files)
    parser.add_argument(
        "-r", "--state-dir",
        default=None,
        help="Bytewax state directory for migration",
    )

    # Module path (positional)
    parser.add_argument("module", help="Python module path (e.g., ds.ariadne.extractor)")

    return parser.parse_args(argv[1:])


def resolve_stage(module_path: str) -> tuple[Extractor | Transformer, str] | None:
    """Import the module and look for a fretworx stage instance.

    Returns (stage, application_id) or None if not a fretworx module.
    """
    module = importlib.import_module(module_path)
    stage = getattr(module, "stage", None)
    if not isinstance(stage, (Extractor, Transformer)):
        return None
    default_id = module_path.removeprefix("ds.").replace(".", "-").replace("_", "-")
    application_id = str(getattr(module, "KAFKA_APPLICATION_ID", default_id))
    return stage, application_id


async def setup_state_store(application_id: str) -> ChangelogStateStore:
    """Create an ephemeral RocksDB state store backed by a Kafka changelog topic."""
    changelog_topic = application_id + "-changelog"

    await ensure_changelog_topic(KAFKA_BOOTSTRAP_SERVERS, changelog_topic)

    changelog_producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await changelog_producer.start()
    inner_store = RocksDBStateStore(Path(tempfile.mkdtemp()) / "state")
    state_store = ChangelogStateStore(inner_store, changelog_producer, changelog_topic)
    await state_store.restore(KAFKA_BOOTSTRAP_SERVERS)
    return state_store


async def auto_migrate_if_needed(
        state_dir: str,
        bootstrap_servers: str,
        application_id: str,
        state_store: ChangelogStateStore,
) -> None:
    """Auto-migrate Bytewax SQLite state to the changelog-backed state store."""
    sentinel = Path(state_dir) / ".migration-complete"
    if sentinel.exists():
        return

    try:
        await migrate_bytewax_to_fretworx(state_store, state_dir, bootstrap_servers, application_id)
    except Exception:
        log.exception("State migration failed — starting with empty state")

    sentinel.touch()


async def create_runner(
        stage: Extractor | Transformer,
        application_id: str,
        state_store: StateStore,
) -> ExtractorRunner | TransformerRunner:
    """Create a consumer, producer, and runner for the given stage."""
    consumer = aiokafka.AIOKafkaConsumer(
        *stage.input_topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=application_id,
    )
    producer_kwargs = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "key_serializer": lambda k: k.encode("utf-8") if k else b"",
        "value_serializer": lambda v: v.encode("utf-8") if v else b"",
    }

    if isinstance(stage, Transformer):
        producer_kwargs["transactional_id"] = application_id + "-txn"

    producer = aiokafka.AIOKafkaProducer(**producer_kwargs)
    await consumer.start()
    await producer.start()

    if isinstance(stage, Extractor):
        return ExtractorRunner(stage, consumer, producer, state_store)
    return TransformerRunner(stage, consumer, producer, state_store, application_id)


def run_bytewax_fallback() -> None:
    """Delegate to bytewax.run for non-migrated stages."""
    log.info("Module exports a Bytewax Dataflow — delegating to bytewax.run")
    sys.argv[0] = "bytewax.run"
    from bytewax.run import cli_main
    cli_main()


async def main() -> None:
    logging.basicConfig(
        level=LOGLEVEL.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args(sys.argv)
    result = resolve_stage(args.module)
    if result is None:
        run_bytewax_fallback()
        return

    stage, application_id = result
    log.info("Running %s via fretworx (application_id=%s)", args.module, application_id)

    stateless = isinstance(stage, Transformer) and stage.stateless
    state_store = InMemoryStateStore() if stateless else await setup_state_store(application_id)
    try:
        if not stateless and args.state_dir:
            await auto_migrate_if_needed(args.state_dir, KAFKA_BOOTSTRAP_SERVERS, application_id, state_store)

        runner = await create_runner(stage, application_id, state_store)
        await runner.run()
    finally:
        await state_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
