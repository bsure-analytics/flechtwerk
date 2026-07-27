"""Integration test for `Stage.on_invalid_message` against a real broker.

The unit tier covers the whole contract; the one thing it cannot show is that a
SKIPPED record's offset really lands in the batch transaction on a broker. That
is the subtle half of the skip semantics: skip without an offset commit would
re-fetch the same undecodable record forever, turning "skip" into a crash-loop
with extra steps.
"""

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import InMemoryStateStore, RecordingObserver
from flechtwerk.transformer import Task, Transformer, TransformerRunner
from flechtwerk.types import Message

pytestmark = pytest.mark.integration


class Skipping(Transformer):
    """Passthrough that skips undecodable records instead of crashing."""

    def __init__(self, input_topic: str, output_topic: str) -> None:
        self.input_topics = [input_topic]
        self.output_topic = output_topic

    def on_invalid_message(self, error):
        return None

    async def transform(self, msg, state):
        yield Message(key=msg.key, topic=self.output_topic, value=msg.value)


async def _create_topics(bootstrap: str, *topics: str, compacted: tuple[str, ...] = ()) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=t,
                num_partitions=1,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if t in compacted else {},
            )
            for t in topics
        ])
    finally:
        await admin.close()


async def _read_all(bootstrap: str, topic: str, timeout_ms: int = 2000) -> list:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=None,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        records = []
        while True:
            batch = await consumer.getmany(timeout_ms=timeout_ms)
            if not batch:
                break
            for msgs in batch.values():
                records.extend(msgs)
        return records
    finally:
        await consumer.stop()


async def test_skipped_record_commits_its_offset_with_the_batch(
    kafka_bootstrap: str,
    unique_topic: str,
    unique_changelog_topic: str,
    unique_group_id: str,
) -> None:
    input_topic = f"input-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    await _create_topics(
        kafka_bootstrap, input_topic, output_topic, unique_changelog_topic,
        compacted=(unique_changelog_topic,),
    )

    # Offset 0 is undecodable JSON, offset 1 is a healthy record.
    seed = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await seed.start()
    try:
        await seed.send(input_topic, key=b"k", value=b"{not json")
        await seed.send(input_topic, key=b"k", value=b'{"n":1}')
        await seed.flush()
    finally:
        await seed.stop()

    consumer = AIOKafkaConsumer(
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=unique_group_id,
        isolation_level="read_committed",
    )
    txn_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        transactional_id=f"tx-{unique_group_id}",
    )
    await consumer.start()
    await txn_producer.start()
    try:
        consumer.assign([TopicPartition(input_topic, 0)])
        records = {}
        while sum(len(msgs) for msgs in records.values()) < 2:
            for tp, msgs in (await consumer.getmany(timeout_ms=2000)).items():
                records.setdefault(tp, []).extend(msgs)

        store = ChangelogStateStore()
        store.inner = InMemoryStateStore()
        store.partition = 0
        store.producer = txn_producer
        store.topic = unique_changelog_topic

        runner = TransformerRunner()
        runner.application_id = unique_group_id
        runner.observer = RecordingObserver()
        runner.transformer = Skipping(input_topic, output_topic)
        runner.tasks[0] = Task(0, txn_producer, store)

        await runner.process_batch(records)
    finally:
        await txn_producer.stop()
        await consumer.stop()

    # Only the healthy record was forwarded.
    output = await _read_all(kafka_bootstrap, output_topic)
    assert [r.value for r in output] == [b'{"n":1}']

    # Both offsets committed: the skipped record is never re-fetched.
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(unique_group_id)
        assert offsets[TopicPartition(input_topic, 0)].offset == 2
    finally:
        await admin.close()

    # The skip is on the scrape, not in a log.
    assert ("message_invalid", input_topic, "skipped") in runner.observer.calls
