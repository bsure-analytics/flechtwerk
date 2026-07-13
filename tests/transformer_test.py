"""Tests for Flechtwerk Transformer and TransformerRunner."""
import asyncio
import json
from typing import AsyncIterator, Final

import pytest

from flechtwerk.attribute import Attribute, BOOL, INT, LIST, RECORD, STR
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.transformer import Task, Transformer, transformer
from flechtwerk.types import Event, Message, State
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record

COUNT: Final = Attribute("count", INT)
CURSOR: Final = Attribute("cursor", INT)
DATA: Final = Attribute("data", STR)
FORM_ID: Final = Attribute("form_id", STR)
ID: Final = Attribute("id", INT)
LEAKED: Final = Attribute("leaked", BOOL)
MUTATED: Final = Attribute("mutated", BOOL)
PRODUCTS: Final = Attribute("products", LIST(RECORD), optional=True)
TYPE: Final = Attribute("type", STR)
X: Final = Attribute("x", INT)


# --- Subclass-based transformers (for backward compat testing) ---


class PassthroughTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = msg.value
        yield Message(key=msg.key, topic="output-topic", value=event)


class FilterTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = msg.value
        if event.get(TYPE) == "wanted":
            yield Message(key=msg.key, topic="output", value=event)


class FanOutTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = msg.value
        for product in event.get(PRODUCTS, []):
            yield Message(key=msg.key, topic="products", value=Event(product))


class StatefulCounterTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message | State]:
        count = state.get(COUNT, 0) + 1
        yield Message(
            key=msg.key,
            topic="output",
            value=Event.wrap({"count": count}),
        )
        yield State.wrap({"count": count})


def json_record(key="k", value=None, topic="input-topic", offset=0, partition=0):
    if value is None:
        value = {}
    return make_record(key=key, value=json.dumps(value), topic=topic, offset=offset, partition=partition)


def make_incoming(key="k", value=None, topic="input-topic"):
    """Build an IncomingMessage-like object via parse_message on a ConsumerRecord."""
    from flechtwerk.kafka import parse_message
    return parse_message(json_record(key=key, value=value, topic=topic))


def make_module(transformer, consumer=None, producer=None, state_store=None):
    """Create a Flechtwerk container with monkey-patched fake resources.

    The fake producer and state store are pre-wired as task 0 on the runner —
    records built by ``json_record`` default to partition 0, so single-task
    tests work unchanged. Multi-task tests register further tasks themselves.
    """
    mod = _FlechtwerkModule()
    mod.application_id = "test-group"
    mod.client_id = "test-group"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.stage = transformer
    mod.consumer = consumer or FakeKafkaConsumer()
    mod.runner.tasks[0] = Task(0, producer or FakeKafkaProducer(), state_store or InMemoryStateStore())
    return mod


# --- Subclass transform tests ---


def test_passthrough_transform():
    async def run():
        t = PassthroughTransformer()
        msg = make_incoming(value={"data": "1"})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 1
        assert result[0].value.raw == {"data": "1"}

    asyncio.run(run())


def test_filter_transform_drops():
    async def run():
        t = FilterTransformer()
        msg = make_incoming(value={"type": "unwanted"})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 0

    asyncio.run(run())


def test_filter_transform_passes():
    async def run():
        t = FilterTransformer()
        msg = make_incoming(value={"type": "wanted"})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 1

    asyncio.run(run())


def test_fan_out_transform():
    async def run():
        t = FanOutTransformer()
        msg = make_incoming(value={"products": [{"id": 1}, {"id": 2}, {"id": 3}]})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 3
        assert [m.value[ID] for m in result] == [1, 2, 3]

    asyncio.run(run())


def test_stateful_counter():
    async def run():
        t = StatefulCounterTransformer()
        state = State()

        msg = make_incoming(value={})
        items = [i async for i in t.transform(msg, state)]
        messages = [i for i in items if isinstance(i, Message)]
        states = [i for i in items if isinstance(i, State)]
        assert messages[0].value[COUNT] == 1
        assert states[0][COUNT] == 1

        items = [i async for i in t.transform(msg, states[0])]
        messages = [i for i in items if isinstance(i, Message)]
        states = [i for i in items if isinstance(i, State)]
        assert messages[0].value[COUNT] == 2
        assert states[0][COUNT] == 2

    asyncio.run(run())


# --- Functional Transformer tests ---


def test_functional_transformer():
    """Functional Transformer with a transform function (no subclass)."""

    async def my_transform(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=my_transform)

    async def run():
        msg = make_incoming(value={"x": 1})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 1
        assert result[0].value.raw == {"x": 1}

    asyncio.run(run())


def test_functional_transformer_with_extract_key():
    """Functional Transformer with custom extract_key."""

    async def my_transform(msg, state):
        yield Message(key=msg.key, topic="out", value=msg.value)

    def my_extract_key(msg):
        return msg.value.raw.get("id", msg.key)

    t = Transformer.of(
        input_topics=["in"],
        extract_key=my_extract_key,
        transform=my_transform,
    )

    msg = make_incoming(value={"id": "custom-key"})
    assert t.extract_key(msg) == "custom-key"


def test_functional_transformer_default_extract_key():
    """Functional Transformer without extract_key uses msg.key."""

    async def my_transform(msg, _):
        yield Message(key=msg.key, topic="out", value=Event())

    t = Transformer.of(input_topics=["in"], transform=my_transform)
    msg = make_incoming(key="my-key")
    assert t.extract_key(msg) == "my-key"


# --- Decorator API tests ---


def test_transformer_decorator_builds_equivalent_stage():
    """@transformer binds a transform function to its input topics, yielding a Transformer."""

    @transformer(input_topics=["in"])
    async def stage(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    assert isinstance(stage, Transformer)
    assert stage.input_topics == ["in"]

    async def run():
        result = [m async for m in stage.transform(make_incoming(value={"x": 1}), State())]
        assert len(result) == 1
        assert result[0].value.raw == {"x": 1}

    asyncio.run(run())


def test_transformer_decorator_threads_enrich_and_extract_key():
    """@transformer forwards the same enrich / extract_key overrides as Transformer.of."""

    async def my_enrich(config):
        config.raw["enriched"] = True
        return config

    def my_extract_key(msg):
        return msg.value.raw.get("id", msg.key)

    @transformer(input_topics=["in"], enrich=my_enrich, extract_key=my_extract_key)
    async def stage(msg, state) -> AsyncIterator[Message]:
        return
        yield  # pragma: no cover

    assert stage.extract_key(make_incoming(value={"id": "custom-key"})) == "custom-key"

    async def run():
        from flechtwerk.types import Config
        enriched = await stage.enrich(Config.wrap({"a": 1}))
        assert enriched.raw == {"a": 1, "enriched": True}

    asyncio.run(run())


def test_transformer_is_abstract():
    """Transformer is abstract — direct instantiation raises TypeError."""
    with pytest.raises(TypeError, match="abstract"):
        Transformer()


def test_subclass_defaults_not_overridden_by_init():
    """Subclass class attributes are not overridden by __init__ defaults."""
    t = StatefulCounterTransformer()
    assert t.input_topics == ["input-topic"]


# --- TransformerRunner tests ---


def test_transformer_runner_processes_messages():
    async def run():
        record = json_record(key="k1", value={"data": "hello"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(PassthroughTransformer(), consumer, producer, state_store)
        runner = mod.runner

        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "output-topic"
        assert json.loads(payload["value"]) == {"data": "hello"}

    asyncio.run(run())


def test_transformer_runner_stateful():
    async def run():
        recs = [
            json_record(key="form1", value={}, offset=0),
            json_record(key="form1", value={}, offset=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(StatefulCounterTransformer(), consumer, producer, state_store)
        runner = mod.runner

        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert len(producer.sent) == 2
        assert json.loads(producer.sent[0][1]["value"])["count"] == 1
        assert json.loads(producer.sent[1][1]["value"])["count"] == 2
        stored = await state_store.get("form1")
        assert stored.raw == {"count": 2}

    asyncio.run(run())


def test_transformer_runner_wraps_value_in_event():
    """Runner wraps msg.value in Event() before passing to transform."""
    received_types = []

    async def spy_transform(msg, _):
        received_types.append(type(msg.value).__name__)
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=spy_transform)

    async def run():
        record = json_record(value={"data": 1}, topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert received_types == ["Event"]

    asyncio.run(run())


def test_transformer_runner_event_is_protective_copy():
    """Runner's Event() wrapping creates a protective copy."""
    original = {"data": 1}
    received_values = []

    async def mutating_transform(msg, _):
        received_values.append(msg.value)
        msg.value[MUTATED] = True
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=mutating_transform)

    async def run():
        record = json_record(value=original, topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        # Transform mutated its copy, but original is untouched
        assert "mutated" not in original

    asyncio.run(run())


def test_transformer_runner_stateless_gets_empty_state():
    """Stateless transformer receives an empty State, not None."""
    received_states = []

    async def spy_transform(msg, state):
        received_states.append(state)
        yield Message(key=msg.key, topic="out", value=Event())

    t = Transformer.of(input_topics=["in"], transform=spy_transform)

    async def run():
        record = json_record(topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert received_states == [State()]
        assert isinstance(received_states[0], State)

    asyncio.run(run())


def test_transformer_runner_stateless_does_not_persist_state():
    """Transformer that never yields State does not persist to the store."""

    async def stateless_transform(msg, state):
        yield Message(key=msg.key, topic="out", value=Event())

    t = Transformer.of(input_topics=["in"], transform=stateless_transform)

    async def run():
        state_store = InMemoryStateStore()
        record = json_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_in_place_state_mutation_is_persisted():
    """A transform that mutates `state` in place and yields it must still be persisted."""

    async def in_place_transform(msg, state):
        state[CURSOR] = state.get(CURSOR, 0) + 1
        yield state

    t = Transformer.of(input_topics=["in"], transform=in_place_transform)

    async def run():
        state_store = InMemoryStateStore()
        record = json_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert (await state_store.get("k1"))[CURSOR] == 1

    asyncio.run(run())


def test_transformer_runner_mutation_without_yield_is_not_persisted():
    """Mutating `state` without yielding must not be persisted (contract)."""

    async def mutate_without_yield(msg, state):
        state[CURSOR] = 42
        yield Message(key=msg.key, topic="out", value=Event())

    t = Transformer.of(input_topics=["in"], transform=mutate_without_yield)

    async def run():
        state_store = InMemoryStateStore()
        record = json_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_yielding_empty_state_deletes_existing_entry():
    """Yielding an empty/falsy State deletes the entry from the state store."""

    async def tombstoning_transform(msg, state):
        yield State()

    t = Transformer.of(input_topics=["in"], transform=tombstoning_transform)

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k1", {"cursor": 5})

        record = json_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_yielding_empty_state_no_op_when_already_absent():
    """Yielding empty State with no baseline is a no-op (no delete call)."""

    async def tombstoning_transform(msg, state):
        yield State()

    t = Transformer.of(input_topics=["in"], transform=tombstoning_transform)

    async def run():
        deleted: list[str] = []

        class SpyingStore(InMemoryStateStore):
            async def delete(self, key):
                deleted.append(key)
                await super().delete(key)

        state_store = SpyingStore()
        record = json_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert deleted == []  # baseline {} == yielded {}, no change
        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_functional_stateful():
    """Functional stateful transformer with custom extract_key via runner."""

    async def my_transform(msg, state):
        count = state.get(COUNT, 0) + 1
        yield Message(key=msg.key, topic="out", value=Event.wrap({"count": count}))
        yield State.wrap({"count": count})

    def my_extract_key(msg):
        return msg.value.raw.get("form_id", msg.key)

    t = Transformer.of(
        input_topics=["in"],
        extract_key=my_extract_key,
        transform=my_transform,
    )

    async def run():
        recs = [
            json_record(value={"form_id": "f1"}, topic="in", offset=0),
            json_record(value={"form_id": "f1"}, topic="in", offset=1),
            json_record(value={"form_id": "f2"}, topic="in", offset=2),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert json.loads(producer.sent[0][1]["value"])["count"] == 1
        assert json.loads(producer.sent[1][1]["value"])["count"] == 2
        assert json.loads(producer.sent[2][1]["value"])["count"] == 1  # different key
        f1 = await state_store.get("f1")
        f2 = await state_store.get("f2")
        assert f1.raw == {"count": 2}
        assert f2.raw == {"count": 1}

    asyncio.run(run())


def test_transformer_context_manager():
    class TrackedTransformer(Transformer):
        input_topics = ["t"]
        entered = False
        exited = False

        async def __aenter__(self):
            self.entered = True
            return self

        async def __aexit__(self, *exc_info):
            self.exited = True

        async def transform(self, msg, state) -> AsyncIterator[Message]:
            yield Message(key="k", topic="t", value=Event())

    async def run():
        t = TrackedTransformer()
        async with t:
            assert t.entered
        assert t.exited

    asyncio.run(run())


def test_transformer_runner_error_propagates_from_run():
    """Errors from transform propagate out of run()."""

    class FailingTransformer(Transformer):
        input_topics = ["in"]

        async def transform(self, msg, state) -> AsyncIterator[Message]:
            raise RuntimeError("boom")
            yield  # pragma: no cover

    async def run():
        record = json_record(topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(FailingTransformer(), consumer, producer, state_store)
        runner = mod.runner

        with pytest.raises(RuntimeError, match="boom"):
            await runner.run()

    asyncio.run(run())


def test_transformer_runner_filter_yields_nothing():
    """Runner handles transforms that yield zero messages."""

    async def skip_all(msg, _):
        return
        yield  # pragma: no cover

    t = Transformer.of(input_topics=["in"], transform=skip_all)

    async def run():
        record = json_record(topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert len(producer.sent) == 0

    asyncio.run(run())


def test_transformer_runner_same_key_in_batch_sees_overlay():
    """Two records with the same state key in one batch — second sees first's yielded state."""
    seen_states: list[dict] = []

    async def counter(msg, state):
        seen_states.append(dict(state.raw))
        count = state.get(COUNT, 0) + 1
        yield Message(key=msg.key, topic="out", value=Event.wrap({"count": count}))
        yield State.wrap({"count": count})

    t = Transformer.of(input_topics=["in"], transform=counter)

    async def run():
        recs = [
            json_record(key="k", topic="in", offset=0),
            json_record(key="k", topic="in", offset=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert seen_states == [{}, {"count": 1}]
        stored = await state_store.get("k")
        assert stored.raw == {"count": 2}
        assert json.loads(producer.sent[0][1]["value"])["count"] == 1
        assert json.loads(producer.sent[1][1]["value"])["count"] == 2

    asyncio.run(run())


def test_transformer_runner_one_transaction_per_batch():
    """A batch of N records opens exactly one Kafka transaction."""

    async def passthrough(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=passthrough)

    async def run():
        recs = [json_record(key=f"k{i}", topic="in", offset=i) for i in range(5)]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert producer.transaction_count == 1
        assert len(producer.sent) == 5

    asyncio.run(run())


def test_transformer_runner_in_place_mutation_without_yield_does_not_leak_in_batch():
    """In-place mutation without a yield must not leak to the next same-key record."""
    seen_states: list[dict] = []

    async def mutating_no_yield(msg, state):
        seen_states.append(dict(state.raw))
        state[LEAKED] = True
        yield Message(key=msg.key, topic="out", value=Event())

    t = Transformer.of(input_topics=["in"], transform=mutating_no_yield)

    async def run():
        recs = [
            json_record(key="k", topic="in", offset=0),
            json_record(key="k", topic="in", offset=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert seen_states == [{}, {}]
        assert await state_store.get("k") is None

    asyncio.run(run())


def test_transformer_runner_offsets_are_max_per_partition():
    """A multi-record batch commits the highest offset+1 per partition."""
    captured_offsets: list[dict] = []

    async def passthrough(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=passthrough)

    class CapturingProducer(FakeKafkaProducer):
        async def send_offsets_to_transaction(self, offsets, group_id):
            captured_offsets.append(dict(offsets))

    async def run():
        recs = [
            json_record(key="k", topic="in", offset=10, partition=0),
            json_record(key="k", topic="in", offset=11, partition=0),
            json_record(key="k", topic="in", offset=12, partition=0),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = CapturingProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert len(captured_offsets) == 1
        from aiokafka import TopicPartition
        assert captured_offsets[0] == {TopicPartition("in", 0): 13}

    asyncio.run(run())


# --- Task / rebalance tests ---


class FakeTaskStore(InMemoryStateStore):
    """InMemoryStateStore that also records the restore/close calls a task makes."""

    def __init__(self):
        super().__init__()
        self.closed = False
        self.restored: list[set[int] | None] = []

    async def restore(self, consumer, partitions=None):
        self.restored.append(partitions)
        return 0

    async def close(self):
        self.closed = True


def test_transformer_runner_same_key_on_different_partitions_uses_separate_tasks():
    """State identity is (task, extract_key) — the same key on two partitions
    yields two independent state entries and two independent transactions."""

    async def counter(msg, state):
        count = state.get(COUNT, 0) + 1
        yield Message(key=msg.key, topic="out", value=Event.wrap({"count": count}))
        yield State.wrap({"count": count})

    t = Transformer.of(input_topics=["in"], transform=counter)

    async def run():
        from aiokafka import TopicPartition
        recs = [
            json_record(key="k", topic="in", offset=5, partition=0),
            json_record(key="k", topic="in", offset=9, partition=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer0 = FakeKafkaProducer()
        store0 = InMemoryStateStore()
        mod = make_module(t, consumer, producer0, store0)
        runner = mod.runner
        producer1 = FakeKafkaProducer()
        store1 = InMemoryStateStore()
        runner.tasks[1] = Task(1, producer1, store1)

        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        # Independent per-task state — no cross-partition sharing for "k".
        assert (await store0.get("k")).raw == {"count": 1}
        assert (await store1.get("k")).raw == {"count": 1}
        # One transaction per task, offsets routed to the owning producer.
        assert producer0.transaction_count == 1
        assert producer1.transaction_count == 1
        assert producer0.offsets_sent == [({TopicPartition("in", 0): 6}, "test-group")]
        assert producer1.offsets_sent == [({TopicPartition("in", 1): 10}, "test-group")]
        # Outputs routed to the producer of the record's task.
        assert len(producer0.sent) == 1
        assert len(producer1.sent) == 1

    asyncio.run(run())


def test_transformer_runner_outputs_routed_to_owning_task_producer():
    """Each task's outputs go through that task's producer only."""

    async def passthrough(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer.of(input_topics=["in"], transform=passthrough)

    async def run():
        recs = [
            json_record(key="a", topic="in", offset=0, partition=0),
            json_record(key="b", topic="in", offset=0, partition=1),
            json_record(key="c", topic="in", offset=1, partition=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer0 = FakeKafkaProducer()
        mod = make_module(t, consumer, producer0, InMemoryStateStore())
        runner = mod.runner
        producer1 = FakeKafkaProducer()
        runner.tasks[1] = Task(1, producer1, InMemoryStateStore())

        records = await runner.consumer.getmany(timeout_ms=1000)
        await runner.process_batch(records)

        assert [v["key"] for _, v in producer0.sent] == [b"a"]
        assert sorted(v["key"] for _, v in producer1.sent) == [b"b", b"c"]

    asyncio.run(run())


def test_rebalance_listener_assign_pauses_and_marks_pending():
    from aiokafka import TopicPartition
    from flechtwerk.transformer import TaskRebalanceListener

    async def run():
        consumer = FakeKafkaConsumer()
        mod = make_module(PassthroughTransformer(), consumer)
        runner = mod.runner
        listener = TaskRebalanceListener(runner)

        assigned = {TopicPartition("in", 0), TopicPartition("in", 2)}
        listener.on_partitions_assigned(assigned)

        assert consumer.paused == assigned
        assert runner.pending == {0, 2}

    asyncio.run(run())


def test_rebalance_listener_revoke_tears_down_all_tasks():
    from flechtwerk.transformer import TaskRebalanceListener

    async def run():
        producer = FakeKafkaProducer()
        store = FakeTaskStore()
        mod = make_module(PassthroughTransformer(), FakeKafkaConsumer(), producer, store)
        runner = mod.runner
        listener = TaskRebalanceListener(runner)

        await listener.on_partitions_revoked(set())

        assert runner.tasks == {}
        assert producer.stopped
        assert store.closed
        assert runner.fatal is None

    asyncio.run(run())


def test_rebalance_listener_revoke_waits_for_batch_lock():
    """Revocation must not tear down tasks while a batch is in flight."""
    from flechtwerk.transformer import TaskRebalanceListener

    async def run():
        producer = FakeKafkaProducer()
        mod = make_module(PassthroughTransformer(), FakeKafkaConsumer(), producer, FakeTaskStore())
        runner = mod.runner
        listener = TaskRebalanceListener(runner)

        async with runner.batch_lock:  # simulate a batch in flight
            revoke = asyncio.create_task(listener.on_partitions_revoked(set()))
            await asyncio.sleep(0.01)
            assert not revoke.done()
            assert runner.tasks != {}
        await revoke
        assert runner.tasks == {}

    asyncio.run(run())


def test_rebalance_listener_records_fatal_instead_of_raising():
    """aiokafka swallows listener exceptions — failures must land on runner.fatal."""
    from flechtwerk.transformer import TaskRebalanceListener

    class FailingProducer(FakeKafkaProducer):
        async def stop(self):
            raise RuntimeError("stop failed")

    async def run():
        mod = make_module(PassthroughTransformer(), FakeKafkaConsumer(), FailingProducer(), FakeTaskStore())
        runner = mod.runner
        listener = TaskRebalanceListener(runner)

        await listener.on_partitions_revoked(set())  # must not raise

        assert isinstance(runner.fatal, RuntimeError)

    asyncio.run(run())


def test_transformer_runner_run_reraises_fatal():
    async def run():
        mod = make_module(PassthroughTransformer(), FakeKafkaConsumer())
        runner = mod.runner
        runner.fatal = RuntimeError("from listener")

        with pytest.raises(RuntimeError, match="from listener"):
            await runner.run()

    asyncio.run(run())


def test_start_pending_tasks_fences_then_restores_then_resumes():
    """Task init order: producer.start() (fencing point) → restore → resume."""
    from aiokafka import TopicPartition

    calls: list = []

    class OrderedProducer(FakeKafkaProducer):
        async def start(self):
            calls.append("producer.start")
            await super().start()

    class OrderedStore(FakeTaskStore):
        async def restore(self, consumer, partitions=None):
            calls.append(("restore", partitions))
            return 7

    class OrderedRestoreConsumer(FakeKafkaConsumer):
        async def start(self):
            calls.append("restore_consumer.start")
            await super().start()

    async def run():
        tp = TopicPartition("in", 3)
        consumer = FakeKafkaConsumer()
        consumer.assigned = {tp}
        consumer.paused = {tp}
        mod = make_module(PassthroughTransformer(), consumer)
        runner = mod.runner
        runner.tasks.clear()
        producer = OrderedProducer()
        store = OrderedStore()
        runner.create_task_producer = lambda p: producer
        runner.create_task_store = lambda p, prod: store
        runner.create_restore_consumer = OrderedRestoreConsumer
        runner.pending = {3}

        await runner.start_pending_tasks()

        assert calls == ["producer.start", "restore_consumer.start", ("restore", {3})]
        assert runner.pending == set()
        assert runner.tasks[3].producer is producer
        assert runner.tasks[3].store is store
        assert consumer.paused == set()  # resumed after restore

    asyncio.run(run())


# --- Config topics ---


class ConfigLookupTransformer(Transformer):
    input_topics = ["input-topic"]
    config_topics = ["cfg-topic"]
    entered_with = None

    async def __aenter__(self):
        self.entered_with = self.configs
        return self

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        config = self.configs.get(msg.key)
        if config is not None:
            yield Message(key=msg.key, topic="out", value=config)


def test_configs_unseeded_access_raises_attribute_error():
    with pytest.raises(AttributeError):
        ConfigLookupTransformer().configs


def test_configs_lookup_with_seeded_store():
    async def run():
        from flechtwerk.configs import ConfigStore
        from flechtwerk.types import Config

        t = ConfigLookupTransformer()
        t.configs = ConfigStore.of({"k": Config.wrap({"a": 1})})
        result = [m async for m in t.transform(make_incoming(value={"data": "x"}), State())]
        assert result[0].value == Config.wrap({"a": 1})

    asyncio.run(run())


def test_run_bootstraps_config_store_and_subscribes_input_topics_only():
    """Bootstrap fills the store BEFORE injection and __aenter__; the group
    consumer never subscribes to config topics."""

    async def run():
        from flechtwerk.types import Config

        t = ConfigLookupTransformer()
        consumer = FakeKafkaConsumer()
        mod = make_module(t, consumer)
        mod.config_consumer = FakeKafkaConsumer([
            make_record(topic="cfg-topic", key=b"k", value=b'{"a":1}'),
        ])
        runner = mod.runner
        runner.fatal = RuntimeError("stop the loop")

        with pytest.raises(RuntimeError, match="stop the loop"):
            await runner.run()

        assert consumer.subscribed == ["input-topic"]
        assert t.entered_with is not None
        assert t.entered_with.get("k") == Config.wrap({"a": 1})

    asyncio.run(run())


def test_check_config_updates_applies_enriches_and_observes():
    async def run():
        from flechtwerk.testing import RecordingObserver
        from flechtwerk.types import Config

        class EnrichingLookup(ConfigLookupTransformer):
            async def enrich(self, config):
                config.raw["enriched"] = True
                return config

        t = EnrichingLookup()
        mod = make_module(t)
        mod.observer = RecordingObserver()
        mod.config_consumer = FakeKafkaConsumer([
            make_record(topic="cfg-topic", key=b"k", value=b'{"a":1}'),
        ])
        runner = mod.runner

        await runner.check_config_updates()

        assert runner.config_store.get("k") == Config.wrap({"a": 1, "enriched": True})
        assert ("config_message_in", "cfg-topic") in mod.observer.calls
        assert ("config_store_entries", 1) in mod.observer.calls

    asyncio.run(run())


def test_check_config_updates_noop_without_config_consumer():
    async def run():
        mod = make_module(PassthroughTransformer())
        await mod.runner.check_config_updates()

    asyncio.run(run())


def test_functional_transformer_with_enrich():
    async def my_transform(msg, state) -> AsyncIterator[Message]:
        return
        yield  # pragma: no cover

    async def my_enrich(config):
        config.raw["enriched"] = True
        return config

    t = Transformer.of(input_topics=["in"], transform=my_transform, enrich=my_enrich)

    async def run():
        from flechtwerk.types import Config
        enriched = await t.enrich(Config.wrap({"a": 1}))
        assert enriched.raw == {"a": 1, "enriched": True}

    asyncio.run(run())
