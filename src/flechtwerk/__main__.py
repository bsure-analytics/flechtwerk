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
import json
import logging
import os
import sys
from pathlib import Path
from typing import Final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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

API_KEY = "api_key"


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

    Returns (stage, group_id) or None if not a fretworx module. For both
    extractors and transformers, group_id precedence is:
        stage.group_id (if set) > $KAFKA_GROUP_ID > module-path derivation.
    Transformers use group_id as the actual consumer group (transactional
    offset commits depend on it); extractors use it only for changelog
    topic naming and client ID defaults.
    """
    module = importlib.import_module(module_path)
    stage = getattr(module, "stage", None)
    if not isinstance(stage, (Extractor, Transformer)):
        return None
    group_id = (
        getattr(stage, "group_id", None)
        or os.getenv("KAFKA_GROUP_ID")
        or module_path_to_group_id(module_path)
    )
    return stage, group_id


async def build_api_key_to_msg_key(
    bootstrap_servers: str,
    input_topics: list[str],
) -> dict[str, str]:
    """Read the extractor's config topics and return {api_key: msg.key}.

    Used by auto_migrate_if_needed() to re-key Bytewax-era state that was
    keyed by msg.value["api_key"] under the old Extractor.extract_key default
    into the new msg.key-keyed layout. Configs without an api_key field
    (Ariadne, Xovis, …) are simply absent from the map, so any Bytewax state
    with those custom keys falls through the remap unchanged.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        await consumer._client.set_topics(input_topics)  # noqa: SLF001
        mapping: dict[str, str] = {}
        consumer.subscribe(input_topics)
        while True:
            records = await consumer.getmany(timeout_ms=2000)
            if not records:
                break
            for _tp, msgs in records.items():
                for msg in msgs:
                    if not msg.value:
                        continue  # tombstone
                    try:
                        value = json.loads(msg.value)
                    except json.JSONDecodeError:
                        continue
                    api_key = value.get(API_KEY)
                    if not api_key:
                        continue
                    key = msg.key.decode("utf-8") if isinstance(msg.key, bytes) else msg.key
                    mapping[api_key] = key
        return mapping
    finally:
        await consumer.stop()


async def auto_migrate_if_needed(
        state_dir: Path,
        group_id: str,
        stage: Extractor | Transformer,
        state_store: StateStore,
        producer: AIOKafkaProducer,
        bootstrap_servers: str,
) -> None:
    """Auto-migrate Bytewax SQLite state to the changelog-backed state store.

    Transformers use a Kafka transaction to atomically commit offsets.
    Extractors skip offset commits (config topics are re-read from earliest
    on every startup) and migrate state without a transaction. For
    extractors, state keys are also re-keyed from api_key (the old
    Extractor.extract_key default) to msg.key (the new default) using the
    config topic's api_key → msg.key mapping. Entries whose Bytewax key
    isn't a known api_key pass through unchanged, which covers datasources
    that set their own extract_key (Ariadne, Xovis) or whose state keys
    were already msg.key-shaped.
    """
    sentinel = state_dir / ".migration-complete"
    if sentinel.exists():
        return

    if isinstance(stage, Transformer):
        async with producer.transaction():
            await migrate_bytewax_to_fretworx(state_store, state_dir, group_id, producer)
    else:
        api_key_to_msg_key = await build_api_key_to_msg_key(bootstrap_servers, stage.input_topics)
        log.info("Loaded %d api_key → msg.key mapping(s) for Bytewax state remap",
                 len(api_key_to_msg_key))

        def remap(bytewax_key: str, _state: object) -> str:
            # If the Bytewax key is a known api_key, re-key to the matching
            # msg.key. Otherwise pass it through — the state is already keyed
            # by a custom extract_key (e.g. Ariadne's tenant/channel/location).
            return api_key_to_msg_key.get(bytewax_key, bytewax_key)

        await migrate_bytewax_to_fretworx(
            state_store, state_dir, group_id, key_remap=remap,
        )
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
            await auto_migrate_if_needed(
                args.state_dir, group_id, stage,
                fretworx.state_store, fretworx.producer, KAFKA_BOOTSTRAP_SERVERS,
            )
        await fretworx.runner.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
