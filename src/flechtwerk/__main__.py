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
from pathlib import Path

from .extractor import Extractor, ExtractorRunner
from .kafka import AIOKafkaConsumerAdapter, AIOKafkaProducerAdapter
from .state import RocksDBStateStore
from .transformer import Transformer, TransformerRunner

log = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI args, accepting both Bytewax and fretworx formats."""
    parser = argparse.ArgumentParser(
        prog="fretworx",
        description="fretworx stream processing framework",
    )
    # Bytewax compatibility flags (accepted but ignored by fretworx)
    parser.add_argument("-b", type=int, default=0, help="(Bytewax compat) backup interval — ignored")
    parser.add_argument("-s", type=int, default=60, help="(Bytewax compat) snapshot interval — ignored")
    parser.add_argument("-w", type=int, default=1, help="(Bytewax compat) worker count — ignored")

    # Shared: state directory (-r for Bytewax, --state-dir for fretworx)
    parser.add_argument(
        "-r", "--state-dir",
        default=os.getenv("STATE_DIR", "/data"),
        help="State directory for RocksDB (default: /data)",
    )

    # Module path (positional)
    parser.add_argument("module", help="Python module path (e.g., ds.ariadne.extractor)")

    return parser.parse_args(argv)


def resolve_stage(module_path: str) -> Extractor | Transformer | None:
    """Import the module and look for a fretworx Extractor or Transformer."""
    module = importlib.import_module(module_path)

    # Check for fretworx stage instances
    for attr_name in ("extractor", "transformer"):
        obj = getattr(module, attr_name, None)
        if isinstance(obj, (Extractor, Transformer)):
            return obj

    return None


def auto_migrate_if_needed(state_dir: str, brokers: list[str], group_id: str) -> None:
    """Auto-migrate Bytewax SQLite state to RocksDB if not already done."""
    state_path = Path(state_dir)
    sentinel = state_path / ".migration-complete"

    if sentinel.exists():
        return

    sqlite_files = list(state_path.glob("part-*.sqlite3"))
    if not sqlite_files:
        # Fresh deployment or no Bytewax state — nothing to migrate
        sentinel.touch()
        return

    log.info("Detected Bytewax SQLite state — running migration to RocksDB")
    try:
        from .migrate_state import migrate_bytewax_to_fretworx
        migrate_bytewax_to_fretworx(state_dir, brokers, group_id)
    except Exception:
        log.exception("State migration failed — starting with empty state")

    sentinel.touch()


def run_bytewax_fallback(argv: list[str]) -> None:
    """Delegate to bytewax.run for non-migrated stages."""
    log.info("Module exports a Bytewax Dataflow — delegating to bytewax.run")
    sys.argv = ["bytewax.run"] + argv
    from bytewax.run import cli_main
    cli_main()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=os.getenv("LOGLEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    raw_argv = argv if argv is not None else sys.argv[1:]
    args = parse_args(raw_argv)

    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    group_id = args.module.replace(".", "-")

    stage = resolve_stage(args.module)

    if stage is None:
        # Not a fretworx module — fall back to Bytewax
        run_bytewax_fallback(raw_argv)
        return

    log.info("Running %s via fretworx", args.module)
    auto_migrate_if_needed(args.state_dir, brokers, group_id)

    state_store = RocksDBStateStore(os.path.join(args.state_dir, "fretworx"))

    try:
        if isinstance(stage, Extractor):
            consumer = AIOKafkaConsumerAdapter(brokers, group_id)
            producer = AIOKafkaProducerAdapter(brokers)
            runner = ExtractorRunner(stage, consumer, producer, state_store)
        elif isinstance(stage, Transformer):
            consumer = AIOKafkaConsumerAdapter(brokers, group_id)
            producer = AIOKafkaProducerAdapter(
                brokers,
                transactional_id=f"{group_id}-txn",
            )
            runner = TransformerRunner(stage, consumer, producer, state_store)
        else:
            sys.exit(f"Unknown stage type: {type(stage)}")

        asyncio.run(runner.run())
    finally:
        state_store.close()


if __name__ == "__main__":
    main()
