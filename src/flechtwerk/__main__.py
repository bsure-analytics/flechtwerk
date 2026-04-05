"""CLI entry point for fretworx.

Accepts Bytewax CLI args for drop-in compatibility:
    python -m fretworx -w 2 -r /data -s 60 -b 0 ds.ariadne.extractor

Also supports native fretworx args:
    python -m fretworx --state-dir /data ds.ariadne.extractor
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

from .extractor import Extractor, ExtractorRunner
from .kafka import AIOKafkaConsumer, AIOKafkaProducer
from .state import ChangelogStateStore, RocksDBStateStore, ensure_changelog_topic
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)


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
        default=os.getenv("STATE_DIR", "/data"),
        help="Bytewax state directory for migration (default: /data)",
    )

    # Module path (positional)
    parser.add_argument("module", help="Python module path (e.g., ds.ariadne.extractor)")

    return parser.parse_args(argv)


def resolve_stage(module_path: str) -> Extractor | Transformer | None:
    """Import the module and look for a fretworx stage instance."""
    module = importlib.import_module(module_path)
    stage = getattr(module, "stage", None)
    if isinstance(stage, (Extractor, Transformer)):
        return stage
    return None


async def auto_migrate_if_needed(
    state_dir: str,
    brokers: list[str],
    application_id: str,
    state_store: ChangelogStateStore,
) -> None:
    """Auto-migrate Bytewax SQLite state to the changelog-backed state store."""
    state_path = Path(state_dir)
    sentinel = state_path / ".migration-complete"

    if sentinel.exists():
        return

    sqlite_files = list(state_path.glob("part-*.sqlite3"))
    if not sqlite_files:
        # Fresh deployment or no Bytewax state — nothing to migrate
        sentinel.touch()
        return

    log.info("Detected Bytewax SQLite state — running migration")
    try:
        from .migrate_state import migrate_bytewax_to_fretworx
        await migrate_bytewax_to_fretworx(state_store, state_dir, brokers, application_id)
    except Exception:
        log.exception("State migration failed — starting with empty state")

    sentinel.touch()


def run_bytewax_fallback(argv: list[str]) -> None:
    """Delegate to bytewax.run for non-migrated stages."""
    log.info("Module exports a Bytewax Dataflow — delegating to bytewax.run")
    sys.argv = ["bytewax.run"] + argv
    from bytewax.run import cli_main
    cli_main()


async def main(argv: list[str]) -> None:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    application_id = os.getenv("KAFKA_APPLICATION_ID", args.module.replace(".", "-"))
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

    stage = resolve_stage(args.module)

    if stage is None:
        # Not a fretworx module — fall back to Bytewax
        run_bytewax_fallback(argv)
        return

    log.info("Running %s via fretworx (application_id=%s)", args.module, application_id)

    # State store: ephemeral RocksDB backed by Kafka changelog
    state_dir = tempfile.mkdtemp()
    inner_store = RocksDBStateStore(os.path.join(state_dir, "state"))
    changelog_topic = application_id + "-changelog"
    changelog_producer = AIOKafkaProducer(brokers)
    state_store = ChangelogStateStore(inner_store, changelog_producer, changelog_topic)

    # Create changelog topic if needed
    num_partitions = len(stage.input_topics)  # match input topic count
    await ensure_changelog_topic(brokers, changelog_topic, num_partitions)

    # Restore state from changelog
    changelog_consumer = AIOKafkaConsumer(brokers, application_id + "-changelog-restore")
    await state_store.restore(changelog_consumer)

    # Auto-migrate Bytewax state if needed
    await auto_migrate_if_needed(args.state_dir, brokers, application_id, state_store)

    try:
        if isinstance(stage, Extractor):
            consumer = AIOKafkaConsumer(brokers, application_id)
            producer = AIOKafkaProducer(brokers)
            runner = ExtractorRunner(stage, consumer, producer, state_store)
        elif isinstance(stage, Transformer):
            consumer = AIOKafkaConsumer(brokers, application_id)
            producer = AIOKafkaProducer(
                brokers,
                transactional_id=application_id + "-txn",
            )
            runner = TransformerRunner(stage, consumer, producer, state_store)
        else:
            sys.exit(f"Unknown stage type: {type(stage)}")

        await runner.run()
    finally:
        await state_store.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
