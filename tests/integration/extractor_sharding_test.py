"""Integration tests for sharded extractors (token-leased config ownership).

Verifies the properties mocks fundamentally cannot validate against a real
broker and a real consumer group:

1. Placement independence — every config record is written to partition 0
   (the Kafka-UI scenario), yet ownership still splits across instances,
   because it hashes the state key consumer-side (`token_for`).
2. Split ownership — two runners in one group poll disjoint config sets
   that together cover everything.
3. Handover — tokens move on join/leave; state continues across the
   handover via the changelog restore, and the per-page transactions make
   it exactly-once end to end: counters never reset, never skip, and never
   repeat (a handover aborts the in-flight page before it becomes visible).
4. The changelog needs no partition alignment — it deliberately has one
   partition here while the config topic has two (restore-all-per-
   assignment, not per-partition restore).
"""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from itertools import count
from typing import AsyncIterator

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.coordinator.assignors.range import RangePartitionAssignor

from flechtwerk.attribute import Attribute, INT, STR
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import Extractor, ExtractorRunner, token_for
from flechtwerk.module import Flechtwerk
from flechtwerk.observer import Observer
from flechtwerk.state import RocksDBStateStore
from flechtwerk.types import Event, Message, State

pytestmark = pytest.mark.integration

COUNTER = Attribute("counter", INT)
NAME = Attribute("name", STR)


async def _create_topics(bootstrap: str, *topics: str, num_partitions: int = 1, compacted: tuple[str, ...] = ()) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=t,
                num_partitions=num_partitions,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if t in compacted else {},
            )
            for t in topics
        ])
    finally:
        await admin.close()


async def _eventually(condition, timeout: float = 60.0, interval: float = 0.2, tasks: tuple = ()) -> None:
    """Poll a sync condition until truthy; fail fast if a runner task died."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        for task in tasks:
            if task.done():
                task.result()  # re-raise the runner's crash
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("condition not met within timeout")
        await asyncio.sleep(interval)


def _make_stage(config_topic: str, output_topic: str, owner: str, polled: set[str]) -> Extractor:
    """A counting extractor that records which configs THIS instance polled
    and stamps every output with its own identity — the owner sequence per
    key is what detects dual ownership that counter steps alone cannot."""

    async def poll(config, state) -> AsyncIterator[Message | State]:
        name = config[NAME]
        polled.add(name)
        n = state.get(COUNTER, 0) + 1
        yield Message(key=name, topic=output_topic, value=Event.wrap({"count": n, "key": name, "owner": owner}))
        yield State.wrap({"counter": n})

    return Extractor.of(config_topics=[config_topic], poll=poll)


def _make_runner(bootstrap: str, group_id: str, changelog_topic: str, stage: Extractor, state_path) -> ExtractorRunner:
    """Wire a sharded ExtractorRunner against a real broker, mirroring Flechtwerk's DI."""
    inner = RocksDBStateStore()
    inner.path = state_path

    def make_token_producer(token: int) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id=f"{group_id}-{token}",
            transaction_timeout_ms=600_000,
            transactional_id=f"{group_id}-{token}",
        )

    runner = ExtractorRunner()
    runner.changelog_topic = changelog_topic
    runner.config_store = ConfigStore()
    runner.consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
    )
    runner.create_restore_consumer = lambda: AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        isolation_level="read_committed",
    )
    runner.create_token_producer = make_token_producer
    runner.extractor = stage
    runner.inner_store = inner
    runner.membership_consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        group_id=group_id,
        isolation_level="read_committed",
        partition_assignment_strategy=(RangePartitionAssignor,),
    )
    runner.observer = Observer()
    runner.poll_interval = timedelta(milliseconds=200)
    return runner


async def _start_runner(runner: ExtractorRunner) -> asyncio.Task:
    await runner.consumer.start()
    await runner.membership_consumer.start()
    return asyncio.create_task(runner.run())


async def _stop_runner(runner: ExtractorRunner, task: asyncio.Task) -> None:
    # Cancelling run() triggers its suspend_tokens barrier — the cancelled
    # poll aborts its open page and the token producers stop — before we
    # leave the group.
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    await runner.membership_consumer.stop()
    await runner.consumer.stop()


async def test_two_sharded_extractors_split_configs_and_hand_over_state(
    kafka_bootstrap: str,
    unique_topic: str,
    unique_changelog_topic: str,
    unique_group_id: str,
    tmp_path,
) -> None:
    """Token leases split the config set; counters survive every handover.

    Phase 1: runner A owns both tokens and counts both configs upward.
    Phase 2: runner B joins — the eager rebalance splits the tokens 1/1;
        each config keeps counting where it left off (changelog restore),
        and the two instances poll DISJOINT config sets.
    Phase 3: runner A leaves — B takes over both tokens and both counters
        keep going.
    """
    config_topic = f"config-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    await _create_topics(kafka_bootstrap, config_topic, num_partitions=2, compacted=(config_topic,))
    await _create_topics(kafka_bootstrap, output_topic)
    # One changelog partition against two config partitions — deliberate:
    # restore is restore-ALL per assignment, no co-partitioning required.
    await _create_topics(kafka_bootstrap, unique_changelog_topic, compacted=(unique_changelog_topic,))

    key_a = next(f"cfg{i}" for i in count() if token_for(f"cfg{i}", 2) == 0)
    key_b = next(f"cfg{i}" for i in count() if token_for(f"cfg{i}", 2) == 1)

    seed = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await seed.start()
    # The Kafka-UI scenario: EVERY config record lands on partition 0.
    # Ownership must still split — it never looks at record placement.
    for key in (key_a, key_b):
        await seed.send(config_topic, key=key.encode(), value=json.dumps({"name": key}).encode(), partition=0)
    await seed.flush()

    outputs: list[dict] = []
    verifier = AIOKafkaConsumer(
        output_topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
    )
    await verifier.start()

    async def drain_outputs() -> None:
        batch = await verifier.getmany(timeout_ms=200)
        outputs.extend(json.loads(msg.value) for msgs in batch.values() for msg in msgs)

    def counts_for(key: str) -> list[int]:
        return [o["count"] for o in outputs if o["key"] == key]

    async def await_progress(minimum: dict[str, int], tasks: tuple) -> None:
        """Wait until every key's counter reached its minimum."""
        deadline = asyncio.get_running_loop().time() + 60.0
        while True:
            await drain_outputs()
            if all(max(counts_for(k), default=0) >= n for k, n in minimum.items()):
                return
            for task in tasks:
                if task.done():
                    task.result()
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"expected progress {minimum}, got {outputs}")

    polled_a: set[str] = set()
    polled_b: set[str] = set()
    runner_a = _make_runner(kafka_bootstrap, unique_group_id, unique_changelog_topic,
                            _make_stage(config_topic, output_topic, "a", polled_a), tmp_path / "a")
    runner_b = _make_runner(kafka_bootstrap, unique_group_id, unique_changelog_topic,
                            _make_stage(config_topic, output_topic, "b", polled_b), tmp_path / "b")

    task_a = await _start_runner(runner_a)
    task_b = None
    try:
        # Phase 1: A alone owns both tokens and polls both configs.
        await _eventually(lambda: runner_a.tokens == frozenset({0, 1}), tasks=(task_a,))
        await await_progress({key_a: 1, key_b: 1}, tasks=(task_a,))

        # Phase 2: B joins; tokens split 1/1; ownership is disjoint and
        # complete; both counters continue past their pre-split maximum.
        baseline = {k: max(counts_for(k), default=0) for k in (key_a, key_b)}
        task_b = await _start_runner(runner_b)
        await _eventually(
            lambda: len(runner_a.tokens) == 1 and len(runner_b.tokens) == 1,
            tasks=(task_a, task_b),
        )
        assert runner_a.tokens.isdisjoint(runner_b.tokens)
        polled_a.clear()
        polled_b.clear()
        await _eventually(lambda: polled_a | polled_b == {key_a, key_b}, tasks=(task_a, task_b))
        assert polled_a.isdisjoint(polled_b)
        await await_progress({k: n + 1 for k, n in baseline.items()}, tasks=(task_a, task_b))

        # Phase 3: A leaves; B takes over both tokens; counting continues.
        baseline = {k: max(counts_for(k), default=0) for k in (key_a, key_b)}
        await _stop_runner(runner_a, task_a)
        task_a = None
        await _eventually(lambda: runner_b.tokens == frozenset({0, 1}), tasks=(task_b,))
        await await_progress({k: n + 1 for k, n in baseline.items()}, tasks=(task_b,))

        # Exactly-once across handovers: per key the counter is STRICTLY
        # consecutive — a handover aborts the in-flight page, so its
        # messages were never visible and re-polling duplicates nothing; a
        # lost state handover would reset to 1; a lost restore would skip.
        for key in (key_a, key_b):
            counts = counts_for(key)
            assert counts, key
            assert counts == list(range(1, len(counts) + 1)), f"{key}: {counts}"

        # LOCKSTEP dual ownership — two owners advancing the same restored
        # counter in step — would satisfy the step assertion above. The owner
        # stamp catches it: per key, ownership legitimately changes at most
        # twice across the three phases; sustained interleaving means two
        # live owners. (The output topic has one partition, so the verifier
        # sees emission order.)
        for key in (key_a, key_b):
            owners = [o["owner"] for o in outputs if o["key"] == key]
            transitions = sum(1 for a, b in zip(owners, owners[1:]) if a != b)
            assert transitions <= 2, f"{key}: ownership interleaving — {owners}"
    finally:
        if task_a is not None:
            await _stop_runner(runner_a, task_a)
        if task_b is not None:
            await _stop_runner(runner_b, task_b)
        await verifier.stop()
        await seed.stop()


async def test_sharded_extractor_rejects_unequal_config_partition_counts(
    kafka_bootstrap: str,
    unique_topic: str,
) -> None:
    """The token space is the config topics' common partition count — a
    sharded stage with mismatched counts must fail fast at startup."""
    cfg_two = f"cfg-two-{unique_topic}"
    cfg_three = f"cfg-three-{unique_topic}"
    await _create_topics(kafka_bootstrap, cfg_two, num_partitions=2, compacted=(cfg_two,))
    await _create_topics(kafka_bootstrap, cfg_three, num_partitions=3, compacted=(cfg_three,))

    async def noop_poll(config, state) -> AsyncIterator[Message | State]:
        return
        yield  # pragma: no cover

    flechtwerk = Flechtwerk.of(
        application_id=f"app-{unique_topic}",
        bootstrap_servers=kafka_bootstrap,
        client_id=f"client-{unique_topic}",
        poll_interval=timedelta(seconds=1),
        stage=Extractor.of(config_topics=[cfg_three, cfg_two], poll=noop_poll),
    )
    with pytest.raises(ValueError, match="equal partition counts"):
        async with flechtwerk:
            pass  # pragma: no cover
