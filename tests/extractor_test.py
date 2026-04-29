"""Tests for fretworx Extractor and ExtractorRunner."""
import asyncio
import json
from typing import AsyncIterator, Final

import pytest

from fretworx.attribute import Attribute, OptionalAttribute, RequiredAttribute
from fretworx.extractor import ConfigEntry, Extractor, ExtractorRunner
from fretworx.module import FretworxModule
from testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from fretworx.types import Config, Message, State


API_KEY: Final = RequiredAttribute[str]("api_key")
CURSOR: Final = RequiredAttribute[int]("cursor")
DATA: Final = RequiredAttribute[str]("data")
ENRICHED: Final = RequiredAttribute[bool]("enriched")
ENTERED: Final = RequiredAttribute[bool]("entered")
ID: Final = OptionalAttribute[str]("id")
ORIGINAL: Final = RequiredAttribute[bool]("original")
POLLED: Final = RequiredAttribute[bool]("polled")
SUSPENDED: Final = RequiredAttribute[bool]("suspended")
TAG: Final = OptionalAttribute[str]("tag")


class SimpleExtractor(Extractor):
    input_topics = ["test-config"]

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        cursor = state.get(CURSOR, 0)
        yield Message(
            key=config[API_KEY],
            topic="test-output",
            value={"cursor": cursor, "data": "polled"},
        )
        yield State({"cursor": cursor + 1})


class EnrichingExtractor(Extractor):
    input_topics = ["test-config"]

    async def enrich(self, config):
        config[ENRICHED] = True
        return config

    async def poll(self, config, state) -> AsyncIterator[Message | State]:
        yield Message(
            key=config[API_KEY],
            topic="out",
            value={"enriched": config.get(ENRICHED, False)},
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


def json_record(key="k", value=None, topic="test-config", offset=0, partition=0):
    if value is None:
        value = {}
    return make_record(key=key, value=json.dumps(value), topic=topic, offset=offset, partition=partition)


def make_module(extractor, consumer=None, producer=None, state_store=None):
    """Create a FretworxModule with monkey-patched fake resources."""
    mod = FretworxModule()
    mod.client_id = "test"
    mod.group_id = "test"
    mod.bootstrap_servers = "localhost:9092"
    mod.poll_interval_seconds = 0
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
        assert states[0][CURSOR] == 1

    asyncio.run(run())


def test_extractor_runner_polls_configs():
    """Test that the runner processes configs and polls them."""
    async def run():
        record = json_record(
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
        assert runner.configs["tenant/channel"].config[API_KEY] == "key123"

        # Run one poll cycle
        await runner.poll_one(runner.configs["tenant/channel"])
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "test-output"
        assert json.loads(payload["value"])["data"] == "polled"

        # State should be persisted under msg.key (extract_key default)
        assert (await state_store.get("tenant/channel")).raw == {"cursor": 1}

    asyncio.run(run())


def test_extractor_enrichment():
    """Test that enrich() is called when configs arrive."""
    async def run():
        record = json_record(key="k", value={"api_key": "key1"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(EnrichingExtractor(), consumer, producer, state_store)
        runner = mod.runner
        await runner.load_initial_configs()

        # Config should have been enriched
        assert runner.configs["k"].config[ENRICHED] is True

        await runner.poll_one(runner.configs["k"])
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
        await state_store.put("k", State({"original": True}))

        mod = make_module(FailingExtractor(), state_store=state_store)
        runner = mod.runner
        entry = ConfigEntry(config=Config({"api_key": "k"}), state_key="k")

        with pytest.raises(RuntimeError, match="Simulated API failure"):
            await runner.poll_one(entry)

        # Original state should be preserved
        assert (await state_store.get("k")).raw == {"original": True}

    asyncio.run(run())


def test_empty_config_removes_key():
    """Test that an empty config value removes the config."""
    async def run():
        consumer = FakeKafkaConsumer([
            json_record(key="k1", value={"api_key": "a"}, offset=0),
            json_record(key="k1", value={}, offset=1),
        ])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner

        await runner.load_initial_configs()
        assert len(runner.configs) == 0  # Empty config removes the key

    asyncio.run(run())


def test_extractor_runner_wraps_config_in_config_type():
    """Runner wraps raw msg.value in Config() when applying configs."""
    async def run():
        record = json_record(key="k", value={"api_key": "test"}, topic="cfg")
        consumer = FakeKafkaConsumer([record])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert isinstance(runner.configs["k"].config, Config)

    asyncio.run(run())


def test_extractor_runner_suspended_configs_not_polled():
    """Configs with suspended=True are skipped during polling."""
    async def run():
        consumer = FakeKafkaConsumer([
            json_record(key="active", value={"api_key": "a"}, offset=0, topic="cfg"),
            json_record(key="suspended", value={"api_key": "b", "suspended": True}, offset=1, topic="cfg"),
        ])
        producer = FakeKafkaProducer()

        mod = make_module(SimpleExtractor(), consumer, producer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert len(runner.configs) == 2

        # Poll only active configs
        await runner.poll_one(runner.configs["active"])
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
        await state_store.put("k", State({"cursor": 5}))
        producer = FailingProducer()

        mod = make_module(SimpleExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        entry = ConfigEntry(config=Config({"api_key": "k"}), state_key="k")

        with pytest.raises(ConnectionError):
            await runner.poll_one(entry)

        # State should NOT be updated (send failed before put)
        assert (await state_store.get("k")).raw == {"cursor": 5}

    asyncio.run(run())


def test_extractor_runner_error_propagates_from_run():
    """Errors from poll propagate out of run()."""
    class AlwaysFailExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            raise RuntimeError("fatal")
            yield  # pragma: no cover

    async def run():
        record = json_record(key="k", value={"api_key": "a"}, topic="cfg")
        consumer = FakeKafkaConsumer([record])

        mod = make_module(AlwaysFailExtractor(), consumer)
        runner = mod.runner

        with pytest.raises(RuntimeError, match="fatal"):
            await runner.run()

    asyncio.run(run())


def test_extractor_runner_config_update_during_operation():
    """Config updates are applied between poll cycles."""
    async def run():
        initial = json_record(key="k", value={"api_key": "v1"}, topic="cfg")
        consumer = FakeKafkaConsumer([initial])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert runner.configs["k"].config[API_KEY] == "v1"

        consumer.records = [
            json_record(key="k", value={"api_key": "v2"}, offset=1, topic="cfg"),
        ]
        await runner.check_config_updates()

        assert runner.configs["k"].config[API_KEY] == "v2"

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
        await runner.poll_one(ConfigEntry(config=Config({"api_key": "k"}), state_key="k"))

        assert len(producer.sent) == 0
        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_in_place_state_mutation_is_persisted():
    """A poll that mutates `state` in place and yields it must still be persisted."""
    class InPlaceMutatingExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            state[CURSOR] = state.get(CURSOR, 0) + 1
            yield state

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(InPlaceMutatingExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config({"api_key": "k"}), state_key="k"))

        assert (await state_store.get("k"))[CURSOR] == 1

    asyncio.run(run())


def test_extractor_poll_yielding_empty_state_deletes_existing_entry():
    """Yielding an empty/falsy State deletes the entry from the state store."""
    class TombstoningExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            yield State()

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", State({"cursor": 5}))

        mod = make_module(TombstoningExtractor(), state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config({"api_key": "k"}), state_key="k"))

        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_yielding_empty_state_no_op_when_already_absent():
    """Yielding empty State when no baseline state exists is a no-op (no delete call)."""
    class TombstoningExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            yield State()

    async def run():
        deleted: list[str] = []

        class SpyingStore(InMemoryStateStore):
            async def delete(self, key):
                deleted.append(key)
                await super().delete(key)

        state_store = SpyingStore()
        mod = make_module(TombstoningExtractor(), state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config({"api_key": "k"}), state_key="k"))

        assert deleted == []  # baseline {} == yielded {}, no change
        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_mutation_without_yield_is_not_persisted():
    """Mutating `state` without yielding must not be persisted (contract)."""
    class MutateWithoutYieldExtractor(Extractor):
        input_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            state[CURSOR] = 42
            return
            yield  # pragma: no cover

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(MutateWithoutYieldExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config({"api_key": "k"}), state_key="k"))

        assert await state_store.get("k") is None

    asyncio.run(run())


# --- Functional Extractor tests ---


def test_functional_extractor():
    """Functional Extractor with a poll function (no subclass)."""
    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        yield Message(key=config[API_KEY], topic="out", value={"polled": True})

    ext = Extractor(input_topics=["cfg"], poll=my_poll)

    async def run():
        config = Config({"api_key": "k"})
        items = [item async for item in ext.poll(config, State())]
        assert len(items) == 1
        assert items[0].value == {"polled": True}

    asyncio.run(run())


def test_functional_extractor_with_enrich_and_extract_key():
    """Functional Extractor with custom enrich and extract_key."""
    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        yield Message(key=config[API_KEY], topic="out", value={"tag": config.get(TAG)})

    async def my_enrich(config):
        config[TAG] = "enriched"
        return config

    def my_extract_key(msg):
        return msg.value.get(ID, msg.value.get(API_KEY))

    ext = Extractor(
        input_topics=["cfg"],
        poll=my_poll,
        enrich=my_enrich,
        extract_key=my_extract_key,
    )

    async def run():
        enriched = await ext.enrich(Config({"api_key": "k"}))
        assert enriched[TAG] == "enriched"

        msg = json_record(key="ignored", value={"api_key": "a", "id": "custom"})
        from fretworx.kafka import parse_message
        assert ext.extract_key(parse_message(msg)) == "custom"

    asyncio.run(run())


def test_functional_extractor_default_extract_key():
    """Functional Extractor without extract_key falls back to msg.key."""
    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        return
        yield  # pragma: no cover

    ext = Extractor(input_topics=["cfg"], poll=my_poll)

    from fretworx.kafka import parse_message
    msg = parse_message(json_record(key="tenant/channel", value={"api_key": "a"}))
    assert ext.extract_key(msg) == "tenant/channel"


def test_extractor_no_poll_raises():
    """Extractor without poll function raises when called."""
    async def run():
        ext = Extractor(input_topics=["cfg"])
        with pytest.raises(NotImplementedError):
            await ext.poll(Config({"api_key": "k"}), State())

    asyncio.run(run())


def test_subclass_defaults_not_overridden_by_init():
    """Subclass class attributes are not overridden by __init__ defaults."""
    ext = SimpleExtractor()
    assert ext.input_topics == ["test-config"]


def test_functional_extractor_end_to_end_with_runner():
    """Functional Extractor works through the runner with config-topic processing."""
    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        cursor = state.get(CURSOR, 0)
        yield Message(key=config[API_KEY], topic="out", value={"cursor": cursor})
        yield State({"cursor": cursor + 1})

    async def run():
        record = json_record(key="t/c", value={"api_key": "k1"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        ext = Extractor(input_topics=["test-config"], poll=my_poll)
        mod = make_module(ext, consumer, producer, state_store)
        runner = mod.runner

        await runner.load_initial_configs()
        await runner.poll_one(runner.configs["t/c"])

        assert len(producer.sent) == 1
        assert (await state_store.get("t/c")).raw == {"cursor": 1}

    asyncio.run(run())
