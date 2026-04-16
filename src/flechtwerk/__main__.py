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
from pathlib import Path
from typing import Final

from aiokafka import AIOKafkaProducer

from .extractor import Extractor
from .migrate_state import migrate_bytewax_to_fretworx
from .module import FretworxModule
from .state import StateStore
from .transformer import Transformer

log = logging.getLogger(__name__)

# Comma-separated Kafka broker addresses
KAFKA_BOOTSTRAP_SERVERS: Final = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Kafka client ID — identifies this instance (defaults to group_id for single-instance local dev)
KAFKA_CLIENT_ID: Final = os.getenv("KAFKA_CLIENT_ID")
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
        type=Path,
    )

    # Module path (positional)
    parser.add_argument("module", help="Python module path (e.g., ds.ariadne.extractor)")

    return parser.parse_args(argv[1:])


def module_path_to_group_id(module_path: str) -> str:
    """Derive a group_id from a module path.

    ds.sumup.extractor → sumup-extractor
    ds.sm_registrations_nt.extractor → sm-registrations-nt-extractor
    """
    return module_path.removeprefix("ds.").replace("_", "-").replace(".", "-")


def resolve_stage(module_path: str) -> tuple[Extractor | Transformer, str] | None:
    """Import the module and look for a fretworx stage instance.

    Returns (stage, group_id) or None if not a fretworx module.
    Transformers declare group_id explicitly (needed for consumer groups
    and transactional offset commits). Extractors derive their group_id
    from the module path — they don't use consumer groups but still need
    an identifier for changelog topic naming and client ID defaults.
    """
    module = importlib.import_module(module_path)
    stage = getattr(module, "stage", None)
    if not isinstance(stage, (Extractor, Transformer)):
        return None
    if isinstance(stage, Transformer):
        if not hasattr(stage, "group_id"):
            raise ValueError(f"{module_path}: Transformer stage must set group_id")
        return stage, stage.group_id
    return stage, module_path_to_group_id(module_path)


async def auto_migrate_if_needed(
        state_dir: Path,
        group_id: str,
        stage: Extractor | Transformer,
        state_store: StateStore,
        producer: AIOKafkaProducer,
) -> None:
    """Auto-migrate Bytewax SQLite state to the changelog-backed state store.

    Transformers use a Kafka transaction to atomically commit offsets.
    Extractors skip offset commits (config topics are re-read from earliest
    on every startup) and migrate state without a transaction.
    """
    sentinel = state_dir / ".migration-complete"
    if sentinel.exists():
        return

    if isinstance(stage, Transformer):
        async with producer.transaction():
            await migrate_bytewax_to_fretworx(state_store, state_dir, group_id, producer)
    else:
        await migrate_bytewax_to_fretworx(state_store, state_dir, group_id)
    sentinel.touch()


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

    stage, group_id = result
    client_id = KAFKA_CLIENT_ID or group_id
    log.info("Running %s via fretworx (group_id=%s, client_id=%s)", args.module, group_id, client_id)

    fretworx = FretworxModule()
    fretworx.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
    fretworx.client_id = client_id
    fretworx.group_id = group_id
    fretworx.stage = stage

    async with fretworx:
        if args.state_dir:
            await auto_migrate_if_needed(args.state_dir, group_id, stage, fretworx.state_store, fretworx.producer)
        await fretworx.runner.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
