"""Tests for Flechtwerk Extractor and ExtractorRunner."""
import asyncio
import json
from typing import AsyncIterator, Final

import pytest

from flechtwerk.attribute import Attribute, BOOL, INT, STR
from flechtwerk.extractor import ConfigEntry, Extractor
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.types import Config, Message, State

API_KEY: Final = Attribute("api_key", STR)
CURSOR: Final = Attribute("cursor", INT)
DATA: Final = Attribute("data", STR)
ENRICHED: Final = Attribute("enriched", BOOL)
ENTERED: Final = Attribute("entered", BOOL)
ID: Final = Attribute("id", STR, optional=True)
ORIGINAL: Final = Attribute("original", BOOL)
POLLED: Final = Attribute("polled", BOOL)
SUSPENDED: Final = Attribute("suspended", BOOL)
TAG: Final = Attribute("tag", STR, optional=True)


class SimpleExtractor(Extractor):
    config_topics = ["test-config"]

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        cursor = state.get(CURSOR, 0)
        yield Message(
            key=config[API_KEY],
            topic="test-output",
            value={"cursor": cursor, "data": "polled"},
        )
        yield State.wrap({"cursor": cursor + 1})


class EnrichingExtractor(Extractor):
    config_topics = ["test-config"]

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
    config_topics = ["test-config"]
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
    """Create a Flechtwerk container with monkey-patched fake resources."""
    mod = _FlechtwerkModule()
    mod.application_id = "test"
    mod.client_id = "test"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
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
        config = Config.wrap({"api_key": "test-key"})
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
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            raise RuntimeError("Simulated API failure")
            yield  # unreachable but makes this an async generator

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", State.wrap({"original": True}))

        mod = make_module(FailingExtractor(), state_store=state_store)
        runner = mod.runner
        entry = ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k")

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
        record = json_record(key="k", value={"api_key": "test"})
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
            json_record(key="active", value={"api_key": "a"}, offset=0),
            json_record(key="suspended", value={"api_key": "b", "suspended": True}, offset=1),
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
        await state_store.put("k", State.wrap({"cursor": 5}))
        producer = FailingProducer()

        mod = make_module(SimpleExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        entry = ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k")

        with pytest.raises(ConnectionError):
            await runner.poll_one(entry)

        # State should NOT be updated (send failed before put)
        assert (await state_store.get("k")).raw == {"cursor": 5}

    asyncio.run(run())


def test_extractor_runner_error_propagates_from_run():
    """Errors from poll propagate out of run()."""

    class AlwaysFailExtractor(Extractor):
        config_topics = ["cfg"]

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
        initial = json_record(key="k", value={"api_key": "v1"})
        consumer = FakeKafkaConsumer([initial])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert runner.configs["k"].config[API_KEY] == "v1"

        consumer.records = [
            json_record(key="k", value={"api_key": "v2"}, offset=1),
        ]
        await runner.check_config_updates()

        assert runner.configs["k"].config[API_KEY] == "v2"

    asyncio.run(run())


def test_extractor_poll_yields_no_state():
    """Poll that yields no State does not persist."""

    class EmptyExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            return
            yield  # pragma: no cover

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(EmptyExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert len(producer.sent) == 0
        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_in_place_state_mutation_is_persisted():
    """A poll that mutates `state` in place and yields it must still be persisted."""

    class InPlaceMutatingExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            state[CURSOR] = state.get(CURSOR, 0) + 1
            yield state

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(InPlaceMutatingExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert (await state_store.get("k"))[CURSOR] == 1

    asyncio.run(run())


def test_extractor_poll_yielding_empty_state_deletes_existing_entry():
    """Yielding an empty/falsy State deletes the entry from the state store."""

    class TombstoningExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            yield State()

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", State.wrap({"cursor": 5}))

        mod = make_module(TombstoningExtractor(), state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_yielding_empty_state_no_op_when_already_absent():
    """Yielding empty State when no baseline state exists is a no-op (no delete call)."""

    class TombstoningExtractor(Extractor):
        config_topics = ["cfg"]

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
        await runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert deleted == []  # baseline {} == yielded {}, no change
        assert await state_store.get("k") is None

    asyncio.run(run())


def test_extractor_poll_mutation_without_yield_is_not_persisted():
    """Mutating `state` without yielding must not be persisted (contract)."""

    class MutateWithoutYieldExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            state[CURSOR] = 42
            return
            yield  # pragma: no cover

    async def run():
        state_store = InMemoryStateStore()
        producer = FakeKafkaProducer()

        mod = make_module(MutateWithoutYieldExtractor(), producer=producer, state_store=state_store)
        runner = mod.runner
        await runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert await state_store.get("k") is None

    asyncio.run(run())


# --- Functional Extractor tests ---


def test_functional_extractor():
    """Functional Extractor with a poll function (no subclass)."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        yield Message(key=config[API_KEY], topic="out", value={"polled": True})

    ext = Extractor.of(config_topics=["cfg"], poll=my_poll)

    async def run():
        config = Config.wrap({"api_key": "k"})
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

    ext = Extractor.of(
        config_topics=["cfg"],
        poll=my_poll,
        enrich=my_enrich,
        extract_key=my_extract_key,
    )

    async def run():
        enriched = await ext.enrich(Config.wrap({"api_key": "k"}))
        assert enriched[TAG] == "enriched"

        msg = json_record(key="ignored", value={"api_key": "a", "id": "custom"})
        from flechtwerk.kafka import parse_message
        assert ext.extract_key(parse_message(msg)) == "custom"

    asyncio.run(run())


def test_functional_extractor_default_extract_key():
    """Functional Extractor without extract_key falls back to msg.key."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        return
        yield  # pragma: no cover

    ext = Extractor.of(config_topics=["cfg"], poll=my_poll)

    from flechtwerk.kafka import parse_message
    msg = parse_message(json_record(key="tenant/channel", value={"api_key": "a"}))
    assert ext.extract_key(msg) == "tenant/channel"


def test_extractor_is_abstract():
    """Extractor is abstract — direct instantiation raises TypeError."""
    with pytest.raises(TypeError, match="abstract"):
        Extractor()


def test_subclass_defaults_not_overridden_by_init():
    """Subclass class attributes are not overridden by __init__ defaults."""
    ext = SimpleExtractor()
    assert ext.config_topics == ["test-config"]


def test_reentry_contract_flush_strictly_precedes_next_poll():
    """Pin the runner's re-entry contract: poll() is re-entered only after the
    previous invocation's messages were sent AND the producer was flushed.
    The MQTT template's ACK-previous-batch pattern depends on this ordering."""
    events: list[str] = []

    class StopRunner(Exception):
        pass

    class OrderRecordingProducer(FakeKafkaProducer):
        async def send(self, topic, *, key=None, value=None, timestamp_ms=None):
            events.append("send")
            await super().send(topic, key=key, value=value, timestamp_ms=timestamp_ms)

        async def flush(self):
            events.append("flush")
            await super().flush()

    class OrderRecordingExtractor(Extractor):
        config_topics = ["test-config"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            events.append("poll")
            if events.count("poll") >= 3:
                raise StopRunner
            yield Message(key="k", topic="out", value={"data": "x"})

    async def run():
        record = json_record(key="k", value={"api_key": "a"})
        mod = make_module(OrderRecordingExtractor(), FakeKafkaConsumer([record]), OrderRecordingProducer())

        with pytest.raises(StopRunner):
            await mod.runner.run()

        assert events == ["poll", "send", "flush", "poll", "send", "flush", "poll"]

    asyncio.run(run())


def test_run_loop_honors_wakeup():
    """run() waits on the stage's wakeup between cycles: with a prohibitive
    interval, cycles still proceed as long as the extractor keeps firing it.
    A regression to a plain sleep would hang this test into its timeout."""
    polls: list[int] = []

    class StopRunner(Exception):
        pass

    class WakeupExtractor(Extractor):
        config_topics = ["test-config"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            polls.append(1)
            if len(polls) >= 3:
                raise StopRunner
            self.wakeup.set()  # a message "arrives" right after this cycle
            return
            yield  # pragma: no cover

    async def run():
        ext = WakeupExtractor()
        ext.wakeup = asyncio.Event()
        record = json_record(key="k", value={"api_key": "a"})
        mod = make_module(ext, FakeKafkaConsumer([record]))
        mod.poll_interval_seconds = 3600  # a plain sleep would hang here

        with pytest.raises(StopRunner):
            await asyncio.wait_for(mod.runner.run(), timeout=5)

        assert len(polls) == 3

    asyncio.run(run())


def test_idle_sleeps_when_no_wakeup():
    """Without a wakeup event, idle() is the plain interval sleep."""

    async def run():
        mod = make_module(SimpleExtractor())
        assert mod.stage.wakeup is None
        await mod.runner.idle()  # poll_interval_seconds=0 → returns immediately

    asyncio.run(run())


def test_idle_returns_early_on_wakeup():
    """A set wakeup event ends the wait before the interval elapses."""

    async def run():
        ext = SimpleExtractor()
        ext.wakeup = asyncio.Event()
        mod = make_module(ext)
        mod.poll_interval_seconds = 3600  # would block for an hour without the wakeup

        ext.wakeup.set()
        await asyncio.wait_for(mod.runner.idle(), timeout=1)

        assert not ext.wakeup.is_set()  # cleared for the next cycle

    asyncio.run(run())


def test_idle_times_out_at_interval_when_wakeup_never_fires():
    async def run():
        ext = SimpleExtractor()
        ext.wakeup = asyncio.Event()
        mod = make_module(ext)  # poll_interval_seconds=0 → immediate timeout

        await asyncio.wait_for(mod.runner.idle(), timeout=1)

        assert not ext.wakeup.is_set()

    asyncio.run(run())


def test_functional_extractor_end_to_end_with_runner():
    """Functional Extractor works through the runner with config-topic processing."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        cursor = state.get(CURSOR, 0)
        yield Message(key=config[API_KEY], topic="out", value={"cursor": cursor})
        yield State.wrap({"cursor": cursor + 1})

    async def run():
        record = json_record(key="t/c", value={"api_key": "k1"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        ext = Extractor.of(config_topics=["test-config"], poll=my_poll)
        mod = make_module(ext, consumer, producer, state_store)
        runner = mod.runner

        await runner.load_initial_configs()
        await runner.poll_one(runner.configs["t/c"])

        assert len(producer.sent) == 1
        assert (await state_store.get("t/c")).raw == {"cursor": 1}

    asyncio.run(run())
