"""Tests for fretworx Extractor and ExtractorRunner."""
import asyncio
import json
from typing import AsyncIterator

import pytest

from fretworx.extractor import Extractor, ExtractorRunner
from fretworx.module import FretworxModule
from fretworx.state import InMemoryStateStore
from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer, FakeRecord
from fretworx.types import Config, Message, State


class SimpleExtractor(Extractor):
    input_topics = ["test-config"]

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        cursor = state.get("cursor", 0)
        yield Message(
            key=config["api_key"],
            topic="test-output",
            value={"cursor": cursor, "data": "polled"},
        )
        yield State(cursor=cursor + 1)


class EnrichingExtractor(Extractor):
    input_topics = ["test-config"]

    async def enrich(self, config):
        config = Config(config)
        config["enriched"] = True
        return config

    async def poll(self, config, state) -> AsyncIterator[Message | State]:
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

    async def poll(self, config, state) -> AsyncIterator[Message | State]:
        yield Message(key="k", topic="t", value={"entered": self.entered})


def make_record(key="k", value=None, topic="test-config", offset=0, partition=0):
    if value is None:
        value = {}
    return FakeRecord(key=key, value=json.dumps(value), topic=topic, offset=offset, partition=partition)


def make_module(extractor, consumer=None, producer=None, state_store=None):
    """Create a FretworxModule with monkey-patched fake resources."""
    mod = FretworxModule()
    mod.client_id = "test"
    mod.group_id = "test"
    mod.bootstrap_servers = "localhost:9092"
    mod.stage = extractor
    mod.consumer = consumer or FakeKafkaConsumer()
    mod.producer = producer or FakeKafkaProducer()
    mod.state_store = state_store or InMemoryStateStore()
    return mod


def test_simple_extractor_poll():
    """Test the poll function directly."""
    async def run():
        ext = SimpleExtractor()
        state = State()
        config = Config({"api_key": "test-key"})
        items = [item async for item in ext.poll(config, state)]
        messages = [i for i in items if isinstance(i, Message)]
        states = [i for i in items if isinstance(i, State)]
        assert len(messages) == 1
        assert messages[0].value == {"cursor": 0, "data": "polled"}
        assert len(states) == 1
        assert states[0]["cursor"] == 1

    asyncio.run(run())


def test_extractor_runner_polls_configs():
    """Test that the runner processes configs and polls them."""
    async def run():
        record = make_record(
            key="tenant/channel",
            value={"api_key": "key123"},
        )
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(SimpleExtractor(), consumer, producer, state_store)
        runner = mod.runner

        # Run load_initial_configs manually
        await runner.load_initial_configs()
        assert len(runner.configs) == 1
        assert runner.configs["tenant/channel"]["api_key"] == "key123"

        # Run one poll cycle
        await runner.poll_one("tenant/channel", runner.configs["tenant/channel"])
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "test-output"
        assert json.loads(payload["value"])["data"] == "polled"

        # State should be persisted under the api_key (extract_key default)
        assert await state_store.get("key123") == {"cursor": 1}

    asyncio.run(run())


def test_extractor_enrichment():
    """Test that enrich() is called when configs arrive."""
    async def run():
        record = make_record(key="k", value={"api_key": "key1"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(EnrichingExtractor(), consumer, producer, state_store)
        runner = mod.runner
        await runner.load_initial_configs()

        # Config should have been enriched
        assert runner.configs["k"]["enriched"] is True

        await runner.poll_one("k", runner.configs["k"])
        topic, payload = producer.sent[0]
        assert json.loads(payload["value"])["enriched"] is True

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

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            raise RuntimeError("Simulated API failure")
            yield  # unreachable but makes this an async generator

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", {"original": True})

        mod = make_module(FailingExtractor(), state_store=state_store)
        runner = mod.runner
        runner.state_keys["k"] = "k"

        with pytest.raises(RuntimeError, match="Simulated API failure"):
            await runner.poll_one("k", {"api_key": "k"})

        # Original state should be preserved
        assert await state_store.get("k") == {"original": True}

    asyncio.run(run())


def test_empty_config_removes_key():
    """Test that an empty config value removes the config."""
    async def run():
        consumer = FakeKafkaConsumer([
            make_record(key="k1", value={"api_key": "a"}, offset=0),
            make_record(key="k1", value={}, offset=1),
        ])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner

        await runner.load_initial_configs()
        assert len(runner.configs) == 0  # Empty config removes the key

    asyncio.run(run())


def test_extractor_runner_wraps_config_in_config_type():
    """Runner wraps raw msg.value in Config() when applying configs."""
    async def run():
        record = make_record(key="k", value={"api_key": "test"}, topic="cfg")
        consumer = FakeKafkaConsumer([record])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert isinstance(runner.configs["k"], Config)

    asyncio.run(run())


def test_extractor_runner_suspended_configs_not_polled():
    """Configs with suspended=True are skipped during polling."""
    async def run():
        consumer = FakeKafkaConsumer([
            make_record(key="active", value={"api_key": "a"}, offset=0, topic="cfg"),
            make_record(key="suspended", value={"api_key": "b", "suspended": True}, offset=1, topic="cfg"),
        ])
        producer = FakeKafkaProducer()

        mod = make_module(SimpleExtractor(), consumer, producer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert len(runner.configs) == 2

        # Poll only active configs
        await runner.poll_one("active", runner.configs["active"])
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert payload["key"] == b"a"

    asyncio.run(run())


def test_extractor_runner_state_not_persisted_on_send_failure():
    """State is NOT persisted if send fails."""
    class FailingProducer(FakeKafkaProducer):
        async def send(self, topic, *, key=None, value=None, timestamp_ms=None):
            raise ConnectionError("Kafka unavailable")

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", {"cursor": 5})
        producer = FailingProducer()

        mod = make_module(SimpleExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        runner.state_keys["k"] = "k"

        with pytest.raises(ConnectionError):
            await runner.poll_one("k", Config({"api_key": "k"}))

        # State should NOT be updated (send failed before put)
        assert await state_store.get("k") == {"cursor": 5}

    asyncio.run(run())


def test_extractor_runner_error_propagates_from_run():
    """Errors from poll propagate out of run()."""
    class AlwaysFailExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            raise RuntimeError("fatal")
            yield  # pragma: no cover

    async def run():
        record = make_record(key="k", value={"api_key": "a"}, topic="cfg")
        consumer = FakeKafkaConsumer([record])

        mod = make_module(AlwaysFailExtractor(), consumer)
        runner = mod.runner

        with pytest.raises(RuntimeError, match="fatal"):
            await runner.run()

    asyncio.run(run())


def test_extractor_runner_config_update_during_operation():
    """Config updates are applied between poll cycles."""
    async def run():
        initial = make_record(key="k", value={"api_key": "v1"}, topic="cfg")
        consumer = FakeKafkaConsumer([initial])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert runner.configs["k"]["api_key"] == "v1"

        consumer.records = [
            make_record(key="k", value={"api_key": "v2"}, offset=1, topic="cfg"),
        ]
        await runner.check_config_updates()

        assert runner.configs["k"]["api_key"] == "v2"

    asyncio.run(run())


def test_extractor_poll_yields_no_state():
    """Poll that yields no State does not persist."""
    class EmptyExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            return
            yield  # pragma: no cover

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(EmptyExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        runner.state_keys["k"] = "k"
        await runner.poll_one("k", Config({"api_key": "k"}))

        assert len(producer.sent) == 0
        assert await state_store.get("k") is None

    asyncio.run(run())
