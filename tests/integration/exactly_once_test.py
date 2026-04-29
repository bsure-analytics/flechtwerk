"""Integration tests for TransformerRunner exactly-once semantics.

Verifies transactional atomicity against a real broker — the one thing mocks
fundamentally cannot validate. A successful transaction materializes output +
state-changelog + offset commit together; an aborted transaction materializes
none of them.
"""

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from fretworx.state import ChangelogStateStore
from testing import InMemoryStateStore
from fretworx.transformer import TransformerRunner
from fretworx.types import Message, State

pytestmark = pytest.mark.integration


async def _create_topics(bootstrap: str, *topics: str, compacted: tuple[str, ...] = ()) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        new_topics = [
            NewTopic(
                name=t,
                num_partitions=1,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if t in compacted else {},
            )
            for t in topics
        ]
        await admin.create_topics(new_topics)
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
            for _, msgs in batch.items():
                records.extend(msgs)
        return records
    finally:
        await consumer.stop()


async def test_successful_transaction_commits_output_state_and_offsets(
    kafka_bootstrap: str,
    unique_topic: str,
    unique_changelog_topic: str,
    unique_group_id: str,
) -> None:
    """A successful send_transactional() atomically commits all three sides."""
    input_topic = f"input-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    await _create_topics(
        kafka_bootstrap,
        input_topic,
        output_topic,
        unique_changelog_topic,
        compacted=(unique_changelog_topic,),
    )

    # Seed the input topic so there's an offset to commit
    seed_producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await seed_producer.start()
    try:
        await seed_producer.send(input_topic, key=b"k", value=b'{"n":1}')
        await seed_producer.flush()
    finally:
        await seed_producer.stop()

    # Build the transactional producer + changelog-backed state store
    txn_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        transactional_id=f"tx-{unique_group_id}",
    )
    await txn_producer.start()
    try:
        state_store = ChangelogStateStore()
        state_store.inner = InMemoryStateStore()
        state_store.producer = txn_producer
        state_store.topic = unique_changelog_topic

        runner = TransformerRunner()
        runner.producer = txn_producer
        runner.state_store = state_store
        runner.group_id = unique_group_id

        tp = TopicPartition(input_topic, 0)
        await runner.send_transactional(
            messages=[Message(
                topic=output_topic, key="out-key", value={"derived": "yes"}, timestamp=None,
            )],
            state_changes={"k": State({"cursor": "done"})},
            offsets={tp: 1},
        )
    finally:
        await txn_producer.stop()

    # 1. Output topic contains our message (read_committed reveals committed txns only)
    output_records = await _read_all(kafka_bootstrap, output_topic)
    assert len(output_records) == 1
    assert output_records[0].key == b"out-key"
    assert output_records[0].value == b'{"derived":"yes"}'

    # 2. Changelog topic contains the state update
    changelog_records = await _read_all(kafka_bootstrap, unique_changelog_topic)
    assert len(changelog_records) == 1
    assert changelog_records[0].key == b"k"
    assert changelog_records[0].value == b'{"cursor":"done"}'

    # 3. Consumer group offset was committed to offset 1
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(unique_group_id)
        assert offsets[tp].offset == 1
    finally:
        await admin.close()


async def test_aborted_transaction_materializes_nothing(
    kafka_bootstrap: str,
    unique_topic: str,
    unique_changelog_topic: str,
    unique_group_id: str,
) -> None:
    """If the transaction body raises, no output/state/offset is visible."""
    input_topic = f"input-{unique_topic}"
    output_topic = f"output-{unique_topic}"
    await _create_topics(
        kafka_bootstrap,
        input_topic,
        output_topic,
        unique_changelog_topic,
        compacted=(unique_changelog_topic,),
    )

    txn_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        transactional_id=f"tx-{unique_group_id}",
    )
    await txn_producer.start()
    try:
        state_store = ChangelogStateStore()
        state_store.inner = InMemoryStateStore()
        state_store.producer = txn_producer
        state_store.topic = unique_changelog_topic

        class BoomError(RuntimeError):
            pass

        tp = TopicPartition(input_topic, 0)
        with pytest.raises(BoomError):
            async with txn_producer.transaction():
                await txn_producer.send(
                    output_topic, key=b"k", value=b'{"phantom":true}',
                )
                await state_store.put("k", State({"phantom": True}))
                await txn_producer.send_offsets_to_transaction({tp: 99}, unique_group_id)
                raise BoomError("abort me")
    finally:
        await txn_producer.stop()

    # read_committed must see no records in either topic
    output_records = await _read_all(kafka_bootstrap, output_topic, timeout_ms=1000)
    changelog_records = await _read_all(kafka_bootstrap, unique_changelog_topic, timeout_ms=1000)
    assert output_records == []
    assert changelog_records == []

    # No offset committed for the aborted group
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(unique_group_id)
        assert tp not in offsets or offsets[tp].offset == -1
    finally:
        await admin.close()
