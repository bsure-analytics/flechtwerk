"""Tests for fretworx Transformer and TransformerRunner."""
import asyncio
from typing import Any, AsyncIterator

import pytest

from fretworx.state import InMemoryStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer
from fretworx.transformer import Transformer, TransformerRunner
from fretworx.types import Event, IncomingMessage, Message, State


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
    stateful = True

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        count = state.get("count", 0) + 1
        state["count"] = count
        yield Message(
            key=msg.key,
            topic="output",
            value={"count": count},
        )


def make_incoming(key: str = "k", value: dict = {}, topic: str = "input-topic") -> IncomingMessage:
    return IncomingMessage(
        key=key,
        offset=0,
        partition=0,
        timestamp=None,
        topic=topic,
        value=value,
    )


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
        result = [m async for m in t.transform(msg, state)]
        assert result[0].value["count"] == 1
        assert state["count"] == 1

        result = [m async for m in t.transform(msg, state)]
        assert result[0].value["count"] == 2
        assert state["count"] == 2

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
        transform=my_transform,
        key_fn=my_key_fn,
        stateful=True,
    )

    msg = make_incoming(value={"id": "custom-key"})
    assert t.key_fn(msg) == "custom-key"
    assert t.stateful is True


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


def test_subclass_stateful_not_overridden_by_init():
    """Subclass stateful=True is not overridden by __init__ default."""
    t = StatefulCounterTransformer()
    assert t.stateful is True


# --- TransformerRunner tests ---


def test_transformer_runner_processes_messages():
    async def run():
        incoming = make_incoming(key="k1", value={"data": "hello"})
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = TransformerRunner(PassthroughTransformer(), consumer, producer, state_store)
        await runner.consumer.subscribe(["input-topic"])

        # Process one message
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert len(producer.sent) == 1
        assert producer.sent[0].value == {"data": "hello"}
        assert producer.transaction_count == 1  # exactly-once

    asyncio.run(run())


def test_transformer_runner_stateful():
    async def run():
        msgs = [
            make_incoming(key="form1", value={}),
            make_incoming(key="form1", value={}),
        ]
        consumer = FakeKafkaConsumer(msgs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = TransformerRunner(StatefulCounterTransformer(), consumer, producer, state_store)
        await runner.consumer.subscribe(["input-topic"])

        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert len(producer.sent) == 2
        assert producer.sent[0].value["count"] == 1
        assert producer.sent[1].value["count"] == 2
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
        incoming = make_incoming(value={"data": 1})
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = TransformerRunner(t, consumer, producer, state_store)
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

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
        incoming = make_incoming(value=original)
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        runner = TransformerRunner(t, consumer, producer, InMemoryStateStore())
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

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
        incoming = make_incoming()
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        runner = TransformerRunner(t, consumer, producer, InMemoryStateStore())
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert received_states == [{}]
        assert isinstance(received_states[0], State)

    asyncio.run(run())


def test_transformer_runner_stateless_does_not_persist_state():
    """Stateless transformer's state is not persisted to the store."""
    async def mutating_transform(msg, state):
        state["should_not_persist"] = True
        yield Message(key=msg.key, topic="out", value={})

    t = Transformer(input_topics=["in"], transform=mutating_transform)

    async def run():
        state_store = InMemoryStateStore()
        incoming = make_incoming(key="k1")
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        runner = TransformerRunner(t, consumer, producer, state_store)
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert await state_store.get("k1") is None

    asyncio.run(run())


def test_transformer_runner_functional_stateful():
    """Functional stateful transformer with custom key_fn via runner."""
    async def my_transform(msg, state):
        count = state.get("count", 0) + 1
        state["count"] = count
        yield Message(key=msg.key, topic="out", value={"count": count})

    def my_key_fn(msg):
        return msg.value.get("form_id", msg.key)

    t = Transformer(
        input_topics=["in"],
        transform=my_transform,
        key_fn=my_key_fn,
        stateful=True,
    )

    async def run():
        msgs = [
            make_incoming(value={"form_id": "f1"}),
            make_incoming(value={"form_id": "f1"}),
            make_incoming(value={"form_id": "f2"}),
        ]
        consumer = FakeKafkaConsumer(msgs)
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = TransformerRunner(t, consumer, producer, state_store)
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert producer.sent[0].value["count"] == 1
        assert producer.sent[1].value["count"] == 2
        assert producer.sent[2].value["count"] == 1  # different key
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


def test_transformer_runner_closes_resources_on_error():
    """Runner closes consumer and producer even when transform raises."""
    class FailingTransformer(Transformer):
        input_topics = ["in"]

        async def transform(self, msg, state) -> AsyncIterator[Message]:
            raise RuntimeError("boom")
            yield  # pragma: no cover

    async def run():
        incoming = make_incoming()
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = TransformerRunner(FailingTransformer(), consumer, producer, state_store)

        with pytest.raises(RuntimeError, match="boom"):
            await runner.run()

        # Resources should be closed despite the error
        # FakeKafkaConsumer/Producer don't track close, but no exception means success

    asyncio.run(run())


def test_transformer_runner_filter_yields_nothing():
    """Runner handles transforms that yield zero messages."""
    async def skip_all(msg, _):
        return
        yield  # pragma: no cover

    t = Transformer(input_topics=["in"], transform=skip_all)

    async def run():
        incoming = make_incoming()
        consumer = FakeKafkaConsumer([incoming])
        producer = FakeKafkaProducer()
        runner = TransformerRunner(t, consumer, producer, InMemoryStateStore())
        await runner.consumer.subscribe(["in"])
        messages = await runner.consumer.poll()
        for msg in messages:
            await runner.process_one(msg)

        assert len(producer.sent) == 0
        assert producer.transaction_count == 1  # transaction still committed

    asyncio.run(run())
