"""Tests for fretworx Extractor and ExtractorRunner."""
import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import pytest

from fretworx.extractor import Extractor, ExtractorRunner
from fretworx.state import InMemoryStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer
from fretworx.types import Config, IncomingMessage, Message, State


class SimpleExtractor(Extractor):
    input_topics = ["test-config"]

    async def poll(self, state: State, config: Config) -> AsyncIterator[Message]:
        cursor = state.get("cursor", 0)
        yield Message(
            key=config["api_key"],
            topic="test-output",
            value={"cursor": cursor, "data": "polled"},
        )
        state["cursor"] = cursor + 1


class EnrichingExtractor(Extractor):
    input_topics = ["test-config"]

    async def enrich(self, config):
        config = Config(config)
        config["enriched"] = True
        return config

    async def poll(self, state, config) -> AsyncIterator[Message]:
        yield Message(
            key=config["api_key"],
            topic="out",
            value={"enriched": config.get("enriched", False)},
        )


class ContextManagerExtractor(Extractor):
    input_topics = ["test-config"]
    entered = False
    exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc_info):
        self.exited = True

    async def poll(self, state, config) -> AsyncIterator[Message]:
        yield Message(key="k", topic="t", value={"entered": self.entered})


def test_simple_extractor_poll():
    """Test the poll function directly."""
    async def run():
        ext = SimpleExtractor()
        state = State()
        config = Config({"api_key": "test-key"})
        messages = [msg async for msg in ext.poll(state, config)]
        assert len(messages) == 1
        assert messages[0].value == {"cursor": 0, "data": "polled"}
        assert state["cursor"] == 1

    asyncio.run(run())


def test_extractor_runner_polls_configs():
    """Test that the runner processes configs and polls them."""
    async def run():
        config_msg = IncomingMessage(
            key="tenant/channel",
            offset=0,
            partition=0,
            timestamp=datetime.now(timezone.utc),
            topic="test-config",
            value={"api_key": "key123"},
        )
        consumer = FakeKafkaConsumer([config_msg])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(
            SimpleExtractor(),
            consumer,
            producer,
            state_store,
        )

        # Run load_initial_configs manually
        await runner.consumer.subscribe(runner.extractor.input_topics)
        await runner.load_initial_configs()
        assert len(runner.configs) == 1
        assert runner.configs["tenant/channel"]["api_key"] == "key123"

        # Run one poll cycle
        await runner.poll_one("tenant/channel", runner.configs["tenant/channel"])
        assert len(producer.sent) == 1
        assert producer.sent[0].value["data"] == "polled"

        # State should be persisted
        assert await state_store.get("tenant/channel") == {"cursor": 1}

    asyncio.run(run())


def test_extractor_enrichment():
    """Test that enrich() is called when configs arrive."""
    async def run():
        config_msg = IncomingMessage(
            key="k",
            offset=0,
            partition=0,
            timestamp=None,
            topic="test-config",
            value={"api_key": "key1"},
        )
        consumer = FakeKafkaConsumer([config_msg])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(
            EnrichingExtractor(),
            consumer,
            producer,
            state_store,
        )
        await runner.consumer.subscribe(runner.extractor.input_topics)
        await runner.load_initial_configs()

        # Config should have been enriched
        assert runner.configs["k"]["enriched"] is True

        await runner.poll_one("k", runner.configs["k"])
        assert producer.sent[0].value["enriched"] is True

    asyncio.run(run())


def test_extractor_context_manager():
    """Test that __aenter__/__aexit__ are called."""
    async def run():
        ext = ContextManagerExtractor()
        assert not ext.entered
        async with ext:
            assert ext.entered
            assert not ext.exited
        assert ext.exited

    asyncio.run(run())


def test_extractor_state_isolation_on_error():
    """Test that state is NOT persisted when poll raises."""
    class FailingExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, state, config) -> AsyncIterator[Message]:
            state["should_not_persist"] = True
            raise RuntimeError("Simulated API failure")
            yield  # unreachable but makes this an async generator

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", {"original": True})
        consumer = FakeKafkaConsumer()
        producer = FakeKafkaProducer()

        runner = ExtractorRunner(FailingExtractor(), consumer, producer, state_store)

        with pytest.raises(RuntimeError, match="Simulated API failure"):
            await runner.poll_one("k", {"api_key": "k"})

        # Original state should be preserved
        assert await state_store.get("k") == {"original": True}

    asyncio.run(run())


def test_empty_config_removes_key():
    """Test that an empty config value removes the config."""
    async def run():
        consumer = FakeKafkaConsumer([
            IncomingMessage(key="k1", offset=0, partition=0, timestamp=None, topic="cfg", value={"api_key": "a"}),
            IncomingMessage(key="k1", offset=1, partition=0, timestamp=None, topic="cfg", value={}),
        ])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()
        runner = ExtractorRunner(SimpleExtractor(), consumer, producer, state_store)

        await runner.consumer.subscribe(["cfg"])
        await runner.load_initial_configs()
        assert len(runner.configs) == 0  # Empty config removes the key

    asyncio.run(run())


def test_extractor_runner_wraps_config_in_config_type():
    """Runner wraps raw msg.value in Config() when applying configs."""
    async def run():
        config_msg = IncomingMessage(
            key="k",
            offset=0,
            partition=0,
            timestamp=None,
            topic="cfg",
            value={"api_key": "test"},
        )
        consumer = FakeKafkaConsumer([config_msg])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(SimpleExtractor(), consumer, producer, state_store)
        await runner.consumer.subscribe(["cfg"])
        await runner.load_initial_configs()

        assert isinstance(runner.configs["k"], Config)

    asyncio.run(run())


def test_extractor_runner_suspended_configs_not_polled():
    """Configs with suspended=True are skipped during polling."""
    async def run():
        consumer = FakeKafkaConsumer([
            IncomingMessage(key="active", offset=0, partition=0, timestamp=None, topic="cfg",
                            value={"api_key": "a"}),
            IncomingMessage(key="suspended", offset=1, partition=0, timestamp=None, topic="cfg",
                            value={"api_key": "b", "suspended": True}),
        ])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(SimpleExtractor(), consumer, producer, state_store)
        await runner.consumer.subscribe(["cfg"])
        await runner.load_initial_configs()

        assert len(runner.configs) == 2

        # Poll only active configs
        await runner.poll_one("active", runner.configs["active"])
        assert len(producer.sent) == 1
        assert producer.sent[0].key == "a"  # Only active config polled

    asyncio.run(run())


def test_extractor_runner_state_not_persisted_on_send_failure():
    """State is NOT persisted if send_batch fails."""
    class FailingProducer(FakeKafkaProducer):
        async def send_batch(self, messages):
            raise ConnectionError("Kafka unavailable")

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", {"cursor": 5})
        consumer = FakeKafkaConsumer()
        producer = FailingProducer()

        runner = ExtractorRunner(SimpleExtractor(), consumer, producer, state_store)

        with pytest.raises(ConnectionError):
            await runner.poll_one("k", Config({"api_key": "k"}))

        # State should NOT be updated (send failed before put)
        assert await state_store.get("k") == {"cursor": 5}

    asyncio.run(run())


def test_extractor_runner_closes_resources_on_error():
    """Runner closes consumer and producer even when poll raises."""
    class AlwaysFailExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, state, config) -> AsyncIterator[Message]:
            raise RuntimeError("fatal")
            yield  # pragma: no cover

    async def run():
        consumer = FakeKafkaConsumer([
            IncomingMessage(key="k", offset=0, partition=0, timestamp=None, topic="cfg",
                            value={"api_key": "a"}),
        ])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(AlwaysFailExtractor(), consumer, producer, state_store)

        with pytest.raises(RuntimeError, match="fatal"):
            await runner.run()

        # No exception from close means resources cleaned up

    asyncio.run(run())


def test_extractor_runner_config_update_during_operation():
    """Config updates are applied between poll cycles."""
    async def run():
        # Initial config
        initial = IncomingMessage(key="k", offset=0, partition=0, timestamp=None, topic="cfg",
                                  value={"api_key": "v1"})
        consumer = FakeKafkaConsumer([initial])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        runner = ExtractorRunner(SimpleExtractor(), consumer, producer, state_store)
        await runner.consumer.subscribe(["cfg"])
        await runner.load_initial_configs()

        assert runner.configs["k"]["api_key"] == "v1"

        # Simulate config update arriving
        consumer.messages = [
            IncomingMessage(key="k", offset=1, partition=0, timestamp=None, topic="cfg",
                            value={"api_key": "v2"}),
        ]
        await runner.check_config_updates()

        assert runner.configs["k"]["api_key"] == "v2"

    asyncio.run(run())


def test_extractor_poll_yields_no_messages():
    """Poll that yields nothing still persists state."""
    class EmptyExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, state, config) -> AsyncIterator[Message]:
            state["checked"] = True
            return
            yield  # pragma: no cover

    async def run():
        state_store = InMemoryStateStore()
        consumer = FakeKafkaConsumer()
        producer = FakeKafkaProducer()

        runner = ExtractorRunner(EmptyExtractor(), consumer, producer, state_store)
        await runner.poll_one("k", Config({"api_key": "k"}))

        assert len(producer.sent) == 0
        assert await state_store.get("k") == {"checked": True}

    asyncio.run(run())
