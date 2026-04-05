"""Tests for fretworx Transformer and TransformerRunner."""
import asyncio
import json
from typing import Any, AsyncIterator

import pytest

from fretworx.state import InMemoryStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer
from fretworx.transformer import Transformer, TransformerRunner
from fretworx.types import IncomingMessage, Message


class PassthroughTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = json.loads(msg.value)
        yield Message(key=msg.key, topic="output-topic", value=event)


class FilterTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = json.loads(msg.value)
        if event.get("type") == "wanted":
            yield Message(key=msg.key, topic="output", value=event)


class FanOutTransformer(Transformer):
    input_topics = ["input-topic"]

    async def transform(self, msg, state) -> AsyncIterator[Message]:
        event = json.loads(msg.value)
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


def make_incoming(key: str = "k", value: dict | str = "", topic: str = "input-topic") -> IncomingMessage:
    if isinstance(value, dict):
        value = json.dumps(value)
    return IncomingMessage(
        key=key,
        offset=0,
        partition=0,
        timestamp=None,
        topic=topic,
        value=value,
    )


def test_passthrough_transform():
    async def run():
        t = PassthroughTransformer()
        msg = make_incoming(value={"data": 1})
        result = [m async for m in t.transform(msg, None)]
        assert len(result) == 1
        assert result[0].value == {"data": 1}

    asyncio.run(run())


def test_filter_transform_drops():
    async def run():
        t = FilterTransformer()
        msg = make_incoming(value={"type": "unwanted"})
        result = [m async for m in t.transform(msg, None)]
        assert len(result) == 0

    asyncio.run(run())


def test_filter_transform_passes():
    async def run():
        t = FilterTransformer()
        msg = make_incoming(value={"type": "wanted"})
        result = [m async for m in t.transform(msg, None)]
        assert len(result) == 1

    asyncio.run(run())


def test_fan_out_transform():
    async def run():
        t = FanOutTransformer()
        msg = make_incoming(value={"products": [{"id": 1}, {"id": 2}, {"id": 3}]})
        result = [m async for m in t.transform(msg, None)]
        assert len(result) == 3
        assert [m.value["id"] for m in result] == [1, 2, 3]

    asyncio.run(run())


def test_stateful_counter():
    async def run():
        t = StatefulCounterTransformer()
        state: dict[str, Any] = {}

        msg = make_incoming(value="{}")
        result = [m async for m in t.transform(msg, state)]
        assert result[0].value["count"] == 1
        assert state["count"] == 1

        result = [m async for m in t.transform(msg, state)]
        assert result[0].value["count"] == 2
        assert state["count"] == 2

    asyncio.run(run())


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
            make_incoming(key="form1", value="{}"),
            make_incoming(key="form1", value="{}"),
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
        assert state_store.get("form1") == {"count": 2}

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
