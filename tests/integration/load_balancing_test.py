"""Integration tests for multi-instance exactly-once (EOS-v1 task model).

Verifies the three properties mocks fundamentally cannot validate against a
real broker:

1. Zombie fencing — a new owner's InitProducerId (same static transactional
   ID) fences the previous owner's producer and aborts its in-flight
   transaction.
2. Restore boundary — a changelog restore under read_committed reads exactly
   to the last stable offset, never into an open transaction.
3. Rebalance handover — two TransformerRunners in one consumer group split
   the tasks; state continues across the handover with no duplicate and no
   lost output.
"""
import asyncio
import json
import uuid
from contextlib import suppress

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.errors import ProducerFenced

from fretworx.attribute import INT, RequiredAttribute
from fretworx.kafka import restore_changelog
from fretworx.observer import Observer
from fretworx.configs import ConfigStore
from fretworx.state import ChangelogStateStore
from fretworx.testing import InMemoryStateStore
from fretworx.transformer import Transformer, TransformerRunner
from fretworx.types import Message, State

pytestmark = pytest.mark.integration

COUNT = RequiredAttribute("count", INT)


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


async def _eventually(condition, timeout: float = 30.0, interval: float = 0.2, tasks: tuple = ()) -> None:
    """Poll a sync condition until truthy; fail fast if a runner task died."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        for task in tasks:
            if task.done():
                task.result()  # re-raise the runner's crash
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("condition not met within timeout")
        await asyncio.sleep(interval)


async def test_zombie_producer_is_fenced_and_its_transaction_aborted(
    kafka_bootstrap: str,
    unique_topic: str,
) -> None:
    """A second producer with the same transactional ID fences the first;
    the first's in-flight transaction aborts and stays invisible."""
    await _create_topics(kafka_bootstrap, unique_topic)
    transactional_id = f"task-{uuid.uuid4().hex[:12]}-0"

    zombie = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap, transactional_id=transactional_id)
    await zombie.start()
    await zombie.begin_transaction()
    await zombie.send(unique_topic, key=b"k", value=b'{"from":"zombie"}')
    await zombie.flush()

    # New owner of the task: InitProducerId bumps the epoch, aborting the
    # zombie's open transaction.
    successor = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap, transactional_id=transactional_id)
    await successor.start()
    try:
        with pytest.raises(ProducerFenced):
            await zombie.commit_transaction()
    finally:
        with suppress(ProducerFenced):
            await zombie.stop()
        await successor.stop()

    consumer = AIOKafkaConsumer(
        unique_topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        batch = await consumer.getmany(timeout_ms=2000)
        assert batch == {}, "zombie's aborted records must be invisible under read_committed"
    finally:
        await consumer.stop()


async def test_restore_reads_exactly_to_last_stable_offset(
    kafka_bootstrap: str,
    unique_changelog_topic: str,
) -> None:
    """A restore must include committed entries and exclude an open transaction's
    writes past the LSO — without hanging on it."""
    await _create_topics(kafka_bootstrap, unique_changelog_topic, compacted=(unique_changelog_topic,))

    committed = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        transactional_id=f"committed-{uuid.uuid4().hex[:8]}",
    )
    await committed.start()
    try:
        async with committed.transaction():
            await committed.send(unique_changelog_topic, key=b"k1", value=b'{"n":1}')
    finally:
        await committed.stop()

    pending = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        transactional_id=f"pending-{uuid.uuid4().hex[:8]}",
    )
    await pending.start()
    await pending.begin_transaction()
    await pending.send(unique_changelog_topic, key=b"k2", value=b'{"n":2}')
    await pending.flush()
    try:
        store = InMemoryStateStore()
        consumer = AIOKafkaConsumer(
            bootstrap_servers=kafka_bootstrap,
            group_id=None,
            isolation_level="read_committed",
        )
        await consumer.start()
        try:
            count = await restore_changelog(consumer, unique_changelog_topic, store.put_bytes, store.delete)
        finally:
            await consumer.stop()

        assert count == 1
        assert (await store.get("k1")).raw == {"n": 1}
        assert await store.get("k2") is None
    finally:
        await pending.abort_transaction()
        await pending.stop()


def _make_runner(bootstrap: str, group_id: str, input_topic: str, changelog_topic: str, transform) -> TransformerRunner:
    """Wire a TransformerRunner against a real broker, mirroring Fretworx's DI."""
    def make_producer(partition: int) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            transactional_id=f"{group_id}-{partition}",
        )

    def make_store(partition: int, producer: AIOKafkaProducer) -> ChangelogStateStore:
        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.partition = partition
        store.producer = producer
        store.topic = changelog_topic
        return store

    def make_restore_consumer() -> AIOKafkaConsumer:
        return AIOKafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=None,
            isolation_level="read_committed",
        )

    runner = TransformerRunner()
    runner.application_id = group_id
    runner.consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        isolation_level="read_committed",
        partition_assignment_strategy=(RangePartitionAssignor,),
    )
    runner.observer = Observer()
    runner.restore_consumer_factory = make_restore_consumer
    runner.config_consumer = None
    runner.config_store = ConfigStore()
    runner.task_producer_factory = make_producer
    runner.task_store_factory = make_store
    runner.transformer = transform
    return runner


async def _start_runner(runner: TransformerRunner) -> asyncio.Task:
    await runner.consumer.start()
    return asyncio.create_task(runner.run())


async def _stop_runner(runner: TransformerRunner, task: asyncio.Task) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    await runner.consumer.stop()


async def test_two_runners_split_tasks_and_hand_over_state(
    kafka_bootstrap: str,
    unique_topic: str,
    unique_changelog_topic: str,
    unique_group_id: str,
) -> None:
    """State continues across a rebalance, with no duplicate and no lost output.

    Phase 1: runner A owns both tasks and processes one record per partition.
    Phase 2: runner B joins — eager rebalance tears everything down and
        redistributes one task each, both re-restored from the changelog;
        a second round of records must continue each key's counter at 2.
    Phase 3: runner A dies — B takes over both tasks; a third round
        continues at 3.
    """
    input_topic = f"input-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    await _create_topics(kafka_bootstrap, input_topic, output_topic, num_partitions=2)
    await _create_topics(kafka_bootstrap, unique_changelog_topic, num_partitions=2, compacted=(unique_changelog_topic,))

    async def counter(msg, state):
        count = state.get(COUNT, 0) + 1
        yield Message(key=msg.key, topic=output_topic, value={"count": count, "key": msg.key})
        yield State.wrap({"count": count})

    transform = Transformer.of(input_topics=[input_topic], transform=counter)

    seed = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await seed.start()

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
        outputs.extend(
            json.loads(msg.value) for msgs in batch.values() for msg in msgs
        )

    async def produce_round() -> None:
        await seed.send(input_topic, key=b"p0", value=b"{}", partition=0)
        await seed.send(input_topic, key=b"p1", value=b"{}", partition=1)
        await seed.flush()

    async def await_outputs(n: int) -> None:
        deadline = asyncio.get_running_loop().time() + 60.0
        while len(outputs) < n:
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"expected {n} outputs, got {len(outputs)}: {outputs}")
            await drain_outputs()

    runner_a = _make_runner(kafka_bootstrap, unique_group_id, input_topic, unique_changelog_topic, transform)
    task_a = await _start_runner(runner_a)
    runner_b = _make_runner(kafka_bootstrap, unique_group_id, input_topic, unique_changelog_topic, transform)
    task_b = None
    try:
        # Phase 1: A owns both tasks.
        await _eventually(lambda: len(runner_a.tasks) == 2, timeout=60.0, tasks=(task_a,))
        await produce_round()
        await await_outputs(2)

        # Phase 2: B joins; tasks split 1/1; state survives the handover.
        task_b = await _start_runner(runner_b)
        await _eventually(
            lambda: len(runner_a.tasks) == 1 and len(runner_b.tasks) == 1,
            timeout=60.0, tasks=(task_a, task_b),
        )
        await produce_round()
        await await_outputs(4)

        # Phase 3: A dies; B takes over both tasks.
        await _stop_runner(runner_a, task_a)
        task_a = None
        await _eventually(lambda: len(runner_b.tasks) == 2, timeout=60.0, tasks=(task_b,))
        await produce_round()
        await await_outputs(6)

        # Exactly-once: per key, counts are exactly [1, 2, 3] — a lost state
        # handover would repeat a count, a lost record would skip one.
        for key in ("p0", "p1"):
            counts = [o["count"] for o in outputs if o["key"] == key]
            assert counts == [1, 2, 3], f"{key}: {counts}"
    finally:
        if task_a is not None:
            await _stop_runner(runner_a, task_a)
        if task_b is not None:
            await _stop_runner(runner_b, task_b)
        await verifier.stop()
        await seed.stop()
