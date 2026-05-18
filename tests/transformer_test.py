"""Tests for fretworx Transformer and TransformerRunner."""
import asyncio
import json
from typing import AsyncIterator, Final

import pytest

from fretworx.attribute import BOOL, INT, LIST, OptionalAttribute, RECORD, RequiredAttribute, STR
from fretworx.module import Fretworx
from fretworx.transformer import Transformer
from fretworx.types import Event, Message, State
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record

COUNT: Final = RequiredAttribute("count", INT)
CURSOR: Final = RequiredAttribute("cursor", INT)
DATA: Final = RequiredAttribute("data", STR)
FORM_ID: Final = RequiredAttribute("form_id", STR)
ID: Final = RequiredAttribute("id", INT)
LEAKED: Final = RequiredAttribute("leaked", BOOL)
MUTATED: Final = RequiredAttribute("mutated", BOOL)
PRODUCTS: Final = OptionalAttribute("products", LIST(RECORD))
TYPE: Final = RequiredAttribute("type", STR)
X: Final = RequiredAttribute("x", INT)


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
    from fretworx.kafka import parse_message
    return parse_message(json_record(key=key, value=value, topic=topic))


def make_module(transformer, consumer=None, producer=None, state_store=None):
    """Create a Fretworx with monkey-patched fake resources."""
    mod = Fretworx()
    mod.application_id = "test-group"
    mod.client_id = "test-group"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.stage = transformer
    mod.consumer = consumer or FakeKafkaConsumer()
    mod.producer = producer or FakeKafkaProducer()
    mod.state_store = state_store or InMemoryStateStore()
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
