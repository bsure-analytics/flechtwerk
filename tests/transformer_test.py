"""Tests for fretworx Transformer and TransformerRunner."""
import asyncio
import json
from typing import AsyncIterator

import pytest

from fretworx.module import FretworxModule
from fretworx.state import InMemoryStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer, FakeRecord
from fretworx.transformer import Transformer, TransformerRunner
from fretworx.types import Event, Message, State


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
        if event.get("type") == "wanted":
            yield Message(key=msg.key, topic="output", value=event)


class FanOutTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = msg.value
        for product in event.get("products", []):
            yield Message(key=msg.key, topic="products", value=product)


class StatefulCounterTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message | State]:
        count = state.get("count", 0) + 1
        yield Message(
            key=msg.key,
            topic="output",
            value={"count": count},
        )
        yield State(count=count)


def make_record(key="k", value=None, topic="input-topic", offset=0, partition=0):
    if value is None:
        value = {}
    return FakeRecord(key=key, value=json.dumps(value), topic=topic, offset=offset, partition=partition)


def make_incoming(key="k", value=None, topic="input-topic"):
    """Build an IncomingMessage-like object via parse_message on a FakeRecord."""
    from fretworx.kafka import parse_message
    return parse_message(make_record(key=key, value=value, topic=topic))


def make_module(transformer, consumer=None, producer=None, state_store=None):
    """Create a FretworxModule with monkey-patched fake resources."""
    mod = FretworxModule()
    mod.application_id = "test-group"
    mod.bootstrap_servers = "localhost:9092"
    mod.stage = transformer
    mod.consumer = consumer or FakeKafkaConsumer()
    mod.producer = producer or FakeKafkaProducer()
    mod.state_store = state_store or InMemoryStateStore()
    return mod


# --- Subclass transform tests ---


def test_passthrough_transform():
    async def run():
        t = PassthroughTransformer()
        msg = make_incoming(value={"data": 1})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 1
        assert result[0].value == {"data": 1}

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
        assert [m.value["id"] for m in result] == [1, 2, 3]

    asyncio.run(run())


def test_stateful_counter():
    async def run():
        t = StatefulCounterTransformer()
        state = State()

        msg = make_incoming(value={})
        items = [i async for i in t.transform(msg, state)]
        messages = [i for i in items if isinstance(i, Message)]
        states = [i for i in items if isinstance(i, State)]
        assert messages[0].value["count"] == 1
        assert states[0]["count"] == 1

        items = [i async for i in t.transform(msg, states[0])]
        messages = [i for i in items if isinstance(i, Message)]
        states = [i for i in items if isinstance(i, State)]
        assert messages[0].value["count"] == 2
        assert states[0]["count"] == 2

    asyncio.run(run())


# --- Functional Transformer tests ---


def test_functional_transformer():
    """Functional Transformer with a transform function (no subclass)."""
    async def my_transform(msg, _):
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer(input_topics=["in"], transform=my_transform)

    async def run():
        msg = make_incoming(value={"x": 1})
        result = [m async for m in t.transform(msg, State())]
        assert len(result) == 1
        assert result[0].value == {"x": 1}

    asyncio.run(run())


def test_functional_transformer_with_key_fn():
    """Functional Transformer with custom key_fn."""
    async def my_transform(msg, state):
        yield Message(key=msg.key, topic="out", value=msg.value)

    def my_key_fn(msg):
        return msg.value.get("id", msg.key)

    t = Transformer(
        input_topics=["in"],
        key_fn=my_key_fn,
        transform=my_transform,
    )

    msg = make_incoming(value={"id": "custom-key"})
    assert t.key_fn(msg) == "custom-key"


def test_functional_transformer_default_key_fn():
    """Functional Transformer without key_fn uses msg.key."""
    async def my_transform(msg, _):
        yield Message(key=msg.key, topic="out", value={})

    t = Transformer(input_topics=["in"], transform=my_transform)
    msg = make_incoming(key="my-key")
    assert t.key_fn(msg) == "my-key"


def test_transformer_no_transform_raises():
    """Transformer without transform function raises when called."""
    async def run():
        t = Transformer(input_topics=["in"])
        msg = make_incoming()
        with pytest.raises(TypeError):
            async for _ in t.transform(msg, State()):
                pass

    asyncio.run(run())


def test_subclass_defaults_not_overridden_by_init():
    """Subclass class attributes are not overridden by __init__ defaults."""
    t = StatefulCounterTransformer()
    assert t.input_topics == ["input-topic"]


# --- TransformerRunner tests ---


def test_transformer_runner_processes_messages():
    async def run():
        record = make_record(key="k1", value={"data": "hello"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(PassthroughTransformer(), consumer, producer, state_store)
        runner = mod.runner

        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "output-topic"
        assert json.loads(payload["value"]) == {"data": "hello"}

    asyncio.run(run())


def test_transformer_runner_stateful():
    async def run():
        recs = [
            make_record(key="form1", value={}, offset=0),
            make_record(key="form1", value={}, offset=1),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(StatefulCounterTransformer(), consumer, producer, state_store)
        runner = mod.runner

        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert len(producer.sent) == 2
        assert json.loads(producer.sent[0][1]["value"])["count"] == 1
        assert json.loads(producer.sent[1][1]["value"])["count"] == 2
        assert await state_store.get("form1") == {"count": 2}

    asyncio.run(run())


def test_transformer_runner_wraps_value_in_event():
    """Runner wraps msg.value in Event() before passing to transform."""
    received_types = []

    async def spy_transform(msg, _):
        received_types.append(type(msg.value).__name__)
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer(input_topics=["in"], transform=spy_transform)

    async def run():
        record = make_record(value={"data": 1}, topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert received_types == ["Event"]

    asyncio.run(run())


def test_transformer_runner_event_is_protective_copy():
    """Runner's Event() wrapping creates a protective copy."""
    original = {"data": 1}
    received_values = []

    async def mutating_transform(msg, _):
        received_values.append(msg.value)
        msg.value["mutated"] = True
        yield Message(key=msg.key, topic="out", value=msg.value)

    t = Transformer(input_topics=["in"], transform=mutating_transform)

    async def run():
        record = make_record(value=original, topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        # Transform mutated its copy, but original is untouched
        assert "mutated" not in original

    asyncio.run(run())


def test_transformer_runner_stateless_gets_empty_state():
    """Stateless transformer receives an empty State, not None."""
    received_states = []

    async def spy_transform(msg, state):
        received_states.append(state)
        yield Message(key=msg.key, topic="out", value={})

    t = Transformer(input_topics=["in"], transform=spy_transform)

    async def run():
        record = make_record(topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert received_states == [{}]
        assert isinstance(received_states[0], State)

    asyncio.run(run())


def test_transformer_runner_stateless_does_not_persist_state():
    """Transformer that never yields State does not persist to the store."""
    async def stateless_transform(msg, state):
        yield Message(key=msg.key, topic="out", value={})

    t = Transformer(input_topics=["in"], transform=stateless_transform)

    async def run():
        state_store = InMemoryStateStore()
        record = make_record(key="k1", topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_functional_stateful():
    """Functional stateful transformer with custom key_fn via runner."""
    async def my_transform(msg, state):
        count = state.get("count", 0) + 1
        yield Message(key=msg.key, topic="out", value={"count": count})
        yield State(count=count)

    def my_key_fn(msg):
        return msg.value.get("form_id", msg.key)

    t = Transformer(
        input_topics=["in"],
        key_fn=my_key_fn,
        transform=my_transform,
    )

    async def run():
        recs = [
            make_record(value={"form_id": "f1"}, topic="in", offset=0),
            make_record(value={"form_id": "f1"}, topic="in", offset=1),
            make_record(value={"form_id": "f2"}, topic="in", offset=2),
        ]
        consumer = FakeKafkaConsumer(recs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(t, consumer, producer, state_store)
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert json.loads(producer.sent[0][1]["value"])["count"] == 1
        assert json.loads(producer.sent[1][1]["value"])["count"] == 2
        assert json.loads(producer.sent[2][1]["value"])["count"] == 1  # different key
        assert await state_store.get("f1") == {"count": 2}
        assert await state_store.get("f2") == {"count": 1}

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
            yield Message(key="k", topic="t", value={})

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
        record = make_record(topic="in")
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

    t = Transformer(input_topics=["in"], transform=skip_all)

    async def run():
        record = make_record(topic="in")
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        mod = make_module(t, consumer, producer, InMemoryStateStore())
        runner = mod.runner
        records = await runner.consumer.getmany(timeout_ms=1000)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await runner.process_one(raw_msg)

        assert len(producer.sent) == 0

    asyncio.run(run())
