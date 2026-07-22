"""Tests for Flechtwerk Extractor and ExtractorRunner."""
import asyncio
import json
from contextlib import suppress
from datetime import timedelta
from itertools import count
from typing import AsyncIterator, Final

import pytest
from aiokafka import TopicPartition
from aiokafka.partitioner import DefaultPartitioner

from flechtwerk.attribute import Attribute, BOOL, INT, STR
from flechtwerk.configs import ConfigStore
from flechtwerk.extractor import ConfigEntry, Extractor, ExtractorRunner, TokenTask, extractor, token_for
from flechtwerk.module import _FlechtwerkModule
from flechtwerk.observer import Observer
from flechtwerk.state import ChangelogStateStore
from flechtwerk.testing import FakeKafkaConsumer, FakeKafkaProducer, InMemoryStateStore, make_record
from flechtwerk.types import Config, Event, Message, State

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
            value=Event.wrap({"cursor": cursor, "data": "polled"}),
        )
        yield State.wrap({"cursor": cursor + 1})


class EnrichingExtractor(Extractor):
    config_topics = ["test-config"]

    async def enrich_config(self, config):
        config[ENRICHED] = True
        return config

    async def poll(self, config, state) -> AsyncIterator[Message | State]:
        yield Message(
            key=config[API_KEY],
            topic="out",
            value=Event.wrap({"enriched": config.get(ENRICHED, False)}),
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
        yield Message(key="k", topic="t", value=Event.wrap({"entered": self.entered}))


class ConfigMutatingExtractor(Extractor):
    config_topics = ["test-config"]

    async def poll(self, config, state) -> AsyncIterator[Message | State]:
        # Report the current cursor, then mutate the config parameter in place.
        # The runner must discard that mutation so the next poll sees the original.
        yield Message(key="k", topic="out", value=Event.wrap({"cursor": config.get(CURSOR, 0)}))
        config[CURSOR] = config.get(CURSOR, 0) + 1


def json_record(key="k", value=None, topic="test-config", offset=0, partition=0):
    if value is None:
        value = {}
    return make_record(key=key, value=json.dumps(value), topic=topic, offset=offset, partition=partition)


class AutoJoinMembershipConsumer(FakeKafkaConsumer):
    """Membership double that completes the group join on the first pump:
    an eager revoke of nothing, then an assignment of partitions
    0..num_tokens-1 of every subscribed topic — the single-replica default,
    where this instance owns every token."""

    def __init__(self, num_tokens: int = 1):
        super().__init__()
        self.joined = False
        self.num_tokens = num_tokens

    async def getmany(self, *partitions: TopicPartition, timeout_ms: int = 0) -> dict:
        await asyncio.sleep(0)
        if not self.joined and self.listener is not None:
            self.joined = True
            await self.listener.on_partitions_revoked(set())
            self.listener.on_partitions_assigned({
                TopicPartition(topic, partition)
                for topic in self.subscribed
                for partition in range(self.num_tokens)
            })
        return {}


def make_token_task(producer, inner):
    """A TokenTask over the given producer and shared inner store. Changelog
    sends go through a private fake so the token producer's ``sent`` records
    output messages only — mirroring what most assertions want to see."""
    store = ChangelogStateStore()
    store.inner = inner
    store.producer = FakeKafkaProducer()
    store.topic = "test-changelog"
    return TokenTask(asyncio.Lock(), producer, store)


def make_module(extractor, consumer=None, producer=None, state_store=None, membership=None):
    """Create a Flechtwerk container with monkey-patched fake resources.

    ``producer`` becomes the token producer of every token task (messages
    AND transaction calls land on it); ``state_store`` is the shared INNER
    store the changelog views wrap. A single token task (token 0 of 1) is
    pre-installed so tests can call poll_one/poll_cycle directly; tests that
    drive run() rebuild it through the same fakes. The membership double
    completes the group join on the first pump — the single-replica default
    where this instance owns every token.
    """
    producer = producer or FakeKafkaProducer()
    state_store = state_store or InMemoryStateStore()
    mod = _FlechtwerkModule()
    mod.application_id = "test"
    mod.client_id = "test"
    mod.bootstrap_servers = "localhost:9092"
    mod.metrics_labels = {}
    mod.metrics_port = 0
    mod.mqtt = None
    mod.poll_interval = timedelta(0)
    mod.stage = extractor
    mod.consumer = consumer or FakeKafkaConsumer()
    mod.create_restore_consumer = lambda: FakeKafkaConsumer()
    mod.create_token_producer = lambda token: producer
    mod.inner_store = state_store
    mod.membership_consumer = membership or AutoJoinMembershipConsumer()
    runner = mod.runner
    runner.num_tokens = 1
    runner.tokens = frozenset({0})
    runner.tasks[0] = make_token_task(producer, state_store)
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
        assert messages[0].value == Event.wrap({"cursor": 0, "data": "polled"})
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
        assert len(runner.entries) == 1
        assert runner.entries["tenant/channel"].config[API_KEY] == "key123"

        # Run one poll cycle
        await runner.poll_one(runner.entries["tenant/channel"])
        assert len(producer.sent) == 1
        topic, payload = producer.sent[0]
        assert topic == "test-output"
        assert json.loads(payload["value"])["data"] == "polled"

        # State should be persisted under msg.key (extract_state_key default)
        assert (await state_store.get("tenant/channel")).raw == {"cursor": 1}

    asyncio.run(run())


def test_poll_config_mutation_does_not_leak_across_polls():
    """A poll() that mutates its config in place must not corrupt the cached
    config: the runner hands each poll a private copy, so the mutation is
    discarded and the next poll sees the original."""

    async def run():
        record = json_record(key="k", value={"api_key": "key1", "cursor": 0})
        producer = FakeKafkaProducer()
        mod = make_module(ConfigMutatingExtractor(), FakeKafkaConsumer([record]), producer)
        runner = mod.runner
        await runner.load_initial_configs()

        entry = runner.entries["k"]
        await runner.poll_one(entry)
        await runner.poll_one(entry)

        cursors = [json.loads(payload["value"])["cursor"] for _, payload in producer.sent]
        assert cursors == [0, 0]          # each poll saw the original cursor
        assert entry.config[CURSOR] == 0  # the cached config was never mutated

    asyncio.run(run())


def test_extractor_enrichment():
    """Test that enrich_config() is called when configs arrive."""

    async def run():
        record = json_record(key="k", value={"api_key": "key1"})
        consumer = FakeKafkaConsumer([record])
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()

        mod = make_module(EnrichingExtractor(), consumer, producer, state_store)
        runner = mod.runner
        await runner.load_initial_configs()

        # Config should have been enriched
        assert runner.entries["k"].config[ENRICHED] is True

        await runner.poll_one(runner.entries["k"])
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
        assert len(runner.entries) == 0  # Empty config removes the key

    asyncio.run(run())


def test_extractor_runner_wraps_config_in_config_type():
    """Runner wraps raw msg.value in Config() when applying configs."""

    async def run():
        record = json_record(key="k", value={"api_key": "test"})
        consumer = FakeKafkaConsumer([record])

        mod = make_module(SimpleExtractor(), consumer)
        runner = mod.runner
        await runner.load_initial_configs()

        assert isinstance(runner.entries["k"].config, Config)

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

        assert len(runner.entries) == 2

        # Poll only active configs
        await runner.poll_one(runner.entries["active"])
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

        assert runner.entries["k"].config[API_KEY] == "v1"

        consumer.records = [
            json_record(key="k", value={"api_key": "v2"}, offset=1),
        ]
        await runner.check_config_updates()

        assert runner.entries["k"].config[API_KEY] == "v2"

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
        yield Message(key=config[API_KEY], topic="out", value=Event.wrap({"polled": True}))

    ext = Extractor.of(config_topics=["cfg"], poll=my_poll)

    async def run():
        config = Config.wrap({"api_key": "k"})
        items = [item async for item in ext.poll(config, State())]
        assert len(items) == 1
        assert items[0].value == Event.wrap({"polled": True})

    asyncio.run(run())


def test_functional_extractor_with_enrich_and_extract_state_key():
    """Functional Extractor with custom enrich_config and extract_state_key."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        yield Message(key=config[API_KEY], topic="out", value=Event.wrap({"tag": config.get(TAG)}))

    async def my_enrich_config(config):
        config[TAG] = "enriched"
        return config

    def my_extract_state_key(msg):
        return msg.value.get(ID, msg.value.get(API_KEY))

    ext = Extractor.of(
        config_topics=["cfg"],
        poll=my_poll,
        enrich_config=my_enrich_config,
        extract_state_key=my_extract_state_key,
    )

    async def run():
        enriched = await ext.enrich_config(Config.wrap({"api_key": "k"}))
        assert enriched[TAG] == "enriched"

        msg = json_record(key="ignored", value={"api_key": "a", "id": "custom"})
        from flechtwerk.kafka import parse_message
        assert ext.extract_state_key(parse_message(msg)) == "custom"

    asyncio.run(run())


def test_functional_extractor_default_extract_state_key():
    """Functional Extractor without extract_state_key falls back to msg.key."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        return
        yield  # pragma: no cover

    ext = Extractor.of(config_topics=["cfg"], poll=my_poll)

    from flechtwerk.kafka import parse_message
    msg = parse_message(json_record(key="tenant/channel", value={"api_key": "a"}))
    assert ext.extract_state_key(msg) == "tenant/channel"


# --- Decorator API tests ---


def test_extractor_decorator_builds_equivalent_stage():
    """@extractor binds a poll function to its config topics, yielding an Extractor."""

    @extractor(config_topics=["cfg"])
    async def stage(config, state) -> AsyncIterator[Message | State]:
        yield Message(key=config[API_KEY], topic="out", value=Event.wrap({"polled": True}))

    assert isinstance(stage, Extractor)
    assert stage.config_topics == ["cfg"]

    async def run():
        items = [item async for item in stage.poll(Config.wrap({"api_key": "k"}), State())]
        assert len(items) == 1
        assert items[0].value == Event.wrap({"polled": True})

    asyncio.run(run())


def test_extractor_decorator_threads_enrich_config_and_extract_state_key():
    """@extractor forwards the same enrich_config / extract_state_key overrides as Extractor.of."""

    async def my_enrich_config(config):
        config[TAG] = "enriched"
        return config

    def my_extract_state_key(msg):
        return msg.value.get(ID, msg.value.get(API_KEY))

    @extractor(config_topics=["cfg"], enrich_config=my_enrich_config, extract_state_key=my_extract_state_key)
    async def stage(config, state) -> AsyncIterator[Message | State]:
        return
        yield  # pragma: no cover

    async def run():
        enriched = await stage.enrich_config(Config.wrap({"api_key": "k"}))
        assert enriched[TAG] == "enriched"

        from flechtwerk.kafka import parse_message
        msg = parse_message(json_record(key="ignored", value={"api_key": "a", "id": "custom"}))
        assert stage.extract_state_key(msg) == "custom"

    asyncio.run(run())


def test_extractor_is_abstract():
    """Extractor is abstract — direct instantiation raises TypeError."""
    with pytest.raises(TypeError, match="abstract"):
        Extractor()


def test_subclass_defaults_not_overridden_by_init():
    """Subclass class attributes are not overridden by __init__ defaults."""
    ext = SimpleExtractor()
    assert ext.config_topics == ["test-config"]


def test_reentry_contract_commit_strictly_precedes_next_poll():
    """Pin the runner's re-entry contract: poll() is re-entered only after
    the previous invocation's final transaction COMMITTED — messages and
    cursor durable, atomically. The MQTT template's ACK-previous-batch
    pattern depends on this ordering."""
    events: list[str] = []

    class StopRunner(Exception):
        pass

    class OrderRecordingProducer(FakeKafkaProducer):
        async def send(self, topic, *, key=None, value=None, timestamp_ms=None):
            events.append("send")
            return await super().send(topic, key=key, value=value, timestamp_ms=timestamp_ms)

        async def commit_transaction(self):
            events.append("commit")
            await super().commit_transaction()

    class OrderRecordingExtractor(Extractor):
        config_topics = ["test-config"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            events.append("poll")
            if events.count("poll") >= 3:
                raise StopRunner
            yield Message(key="k", topic="out", value=Event.wrap({"data": "x"}))

    async def run():
        record = json_record(key="k", value={"api_key": "a"})
        mod = make_module(OrderRecordingExtractor(), FakeKafkaConsumer([record]), OrderRecordingProducer())

        with pytest.raises(StopRunner):
            await mod.runner.run()

        assert events == ["poll", "send", "commit", "poll", "send", "commit", "poll"]

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
        mod.poll_interval = timedelta(seconds=3600)  # a plain sleep would hang here

        with pytest.raises(StopRunner):
            await asyncio.wait_for(mod.runner.run(), timeout=5)

        assert len(polls) == 3

    asyncio.run(run())


def test_idle_sleeps_when_no_wakeup():
    """Without a wakeup event, idle() is the plain interval sleep."""

    async def run():
        mod = make_module(SimpleExtractor())
        assert mod.stage.wakeup is None
        await mod.runner.idle()  # poll_interval=timedelta(0) → returns immediately

    asyncio.run(run())


def test_idle_returns_early_on_wakeup():
    """A set wakeup event ends the wait before the interval elapses."""

    async def run():
        ext = SimpleExtractor()
        ext.wakeup = asyncio.Event()
        mod = make_module(ext)
        mod.poll_interval = timedelta(seconds=3600)  # would block for an hour without the wakeup

        ext.wakeup.set()
        await asyncio.wait_for(mod.runner.idle(), timeout=1)

        assert not ext.wakeup.is_set()  # cleared for the next cycle

    asyncio.run(run())


def test_idle_times_out_at_interval_when_wakeup_never_fires():
    async def run():
        ext = SimpleExtractor()
        ext.wakeup = asyncio.Event()
        mod = make_module(ext)  # poll_interval=timedelta(0) → immediate timeout

        await asyncio.wait_for(mod.runner.idle(), timeout=1)

        assert not ext.wakeup.is_set()

    asyncio.run(run())


def test_functional_extractor_end_to_end_with_runner():
    """Functional Extractor works through the runner with config-topic processing."""

    async def my_poll(config, state) -> AsyncIterator[Message | State]:
        cursor = state.get(CURSOR, 0)
        yield Message(key=config[API_KEY], topic="out", value=Event.wrap({"cursor": cursor}))
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
        await runner.poll_one(runner.entries["t/c"])

        assert len(producer.sent) == 1
        assert (await state_store.get("t/c")).raw == {"cursor": 1}

    asyncio.run(run())


# --- Sharded runner tests ---


def key_with_token(token: int, num_tokens: int) -> str:
    """First synthetic key whose consumer-side hash lands on ``token``."""
    return next(f"key{i}" for i in count() if token_for(f"key{i}", num_tokens) == token)


async def until(predicate) -> None:
    """Poll a condition — wrap in asyncio.wait_for for the timeout."""
    while not predicate():
        await asyncio.sleep(0.001)


class FakeMembershipConsumer(FakeKafkaConsumer):
    """Membership pump double whose ``getmany`` yields to the event loop, so
    the sharded main loop can be driven and cancelled from a test."""

    async def getmany(self, *partitions: TopicPartition, timeout_ms: int = 0) -> dict:
        await asyncio.sleep(0)
        return {}


def make_sharded_runner(stage, consumer=None, restore_records=None, observer=None, producer=None):
    """Wire an ExtractorRunner directly (no DI).

    ``restore_records`` seed the throwaway changelog-restore consumers built
    by ``create_restore_consumer`` — each token assignment gets a fresh fake
    positioned at the same backlog, like re-reading a compacted topic. Every
    token task shares ``producer``, so assertions have one place to look.
    """
    producer = producer or FakeKafkaProducer()
    runner = ExtractorRunner()
    runner.changelog_topic = "test-changelog"
    runner.config_store = ConfigStore()
    runner.consumer = consumer or FakeKafkaConsumer()
    runner.create_restore_consumer = lambda: FakeKafkaConsumer(list(restore_records or []))
    runner.create_token_producer = lambda token: producer
    runner.extractor = stage
    runner.inner_store = InMemoryStateStore()
    runner.membership_consumer = FakeMembershipConsumer()
    runner.observer = observer or Observer()
    runner.poll_interval = timedelta(0)
    return runner


def test_token_for_matches_default_partitioner():
    """token_for is a compatibility promise: the exact DefaultPartitioner
    math, so ownership coincides with where a key-hashing producer would
    have placed the key (and never uses Python's per-process-salted hash)."""
    partitioner = DefaultPartitioner()
    partitions = list(range(8))
    for key in ("tenant/channel", "a", "key42", "ütf-8 ✓", "x" * 100):
        assert token_for(key, 8) == partitioner(key.encode("utf-8"), partitions, partitions)


def test_owns_everything_with_all_tokens_held():
    """The single-replica default: every token held, every key owned."""
    runner = ExtractorRunner()
    runner.num_tokens = 8
    runner.tokens = frozenset(range(8))
    assert runner.owns("anything")


def test_owns_only_held_tokens():
    runner = ExtractorRunner()
    runner.num_tokens = 8
    runner.tokens = frozenset({token_for("mine", 8)})
    assert runner.owns("mine")
    other = next(f"key{i}" for i in count() if token_for(f"key{i}", 8) not in runner.tokens)
    assert not runner.owns(other)


def test_poll_cycle_skips_suspended_configs():
    """The cycle-level filter (not just poll_one) skips suspended configs."""

    async def run():
        consumer = FakeKafkaConsumer([
            json_record(key="active", value={"api_key": "a"}, offset=0),
            json_record(key="suspended", value={"api_key": "b", "suspended": True}, offset=1),
        ])
        producer = FakeKafkaProducer()
        mod = make_module(SimpleExtractor(), consumer, producer)
        runner = mod.runner
        await runner.load_initial_configs()

        await runner.poll_cycle()

        assert [payload["key"] for _, payload in producer.sent] == [b"a"]

    asyncio.run(run())


def test_sharded_poll_cycle_filters_by_ownership():
    """Only configs whose state key hashes onto a held token are polled."""

    async def run():
        key_t0 = key_with_token(0, 2)
        key_t1 = key_with_token(1, 2)
        records = [
            json_record(key=key_t0, value={"api_key": "A"}, offset=0),
            json_record(key=key_t1, value={"api_key": "B"}, offset=1),
        ]
        producer = FakeKafkaProducer()
        runner = make_sharded_runner(SimpleExtractor(), consumer=FakeKafkaConsumer(records), producer=producer)
        await runner.load_initial_configs()
        runner.num_tokens = 2
        runner.tokens = frozenset({0})
        runner.tasks[0] = make_token_task(producer, runner.inner_store)

        await runner.poll_cycle()

        assert [payload["key"] for _, payload in producer.sent] == [b"A"]

    asyncio.run(run())


def test_run_injects_global_config_store_before_aenter():
    """The holy grail wiring: ``self.configs`` is the GLOBAL store on the
    stage — injected before ``__aenter__``, populated by the bootstrap, and
    reaching entries beyond the one handed to ``poll``."""

    class StopRunner(Exception):
        pass

    checks = {}

    class LookupExtractor(Extractor):
        config_topics = ["test-config"]

        async def __aenter__(self):
            checks["injected_before_enter"] = isinstance(self.configs, ConfigStore)
            return self

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            other = self.configs.get("other")
            checks["lookup"] = None if other is None else other[TAG]
            raise StopRunner
            yield  # pragma: no cover

    async def run():
        records = [
            json_record(key="k", value={"api_key": "a"}, offset=0),
            json_record(key="other", value={"api_key": "b", "tag": "shared"}, offset=1),
        ]
        mod = make_module(LookupExtractor(), FakeKafkaConsumer(records))

        with pytest.raises(StopRunner):
            await mod.runner.run()

        assert checks == {"injected_before_enter": True, "lookup": "shared"}

    asyncio.run(run())


def test_sharded_runner_polls_only_owned_and_hands_over_on_rebalance():
    """The full token dance: subscribe → assign → restore → poll only owned
    configs; an eager revoke→assign round hands ownership over cleanly."""

    async def run():
        key_t0 = key_with_token(0, 2)
        key_t1 = key_with_token(1, 2)
        polled: list[str] = []
        cycled = asyncio.Event()

        class ShardedExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                polled.append(config[API_KEY])
                cycled.set()
                return
                yield  # pragma: no cover

        records = [
            json_record(key=key_t0, value={"api_key": "A"}, offset=0),
            json_record(key=key_t1, value={"api_key": "B"}, offset=1),
        ]
        runner = make_sharded_runner(ShardedExtractor(), consumer=FakeKafkaConsumer(records))
        runner.num_tokens = 2
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener

            listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(cycled.wait(), 5)
            assert set(polled) == {"A"}
            assert runner.tokens == frozenset({0})

            # Eager handover: revoke (the barrier — cycle fully unwound when
            # it returns), then assign the other token.
            await listener.on_partitions_revoked({TopicPartition("test-config", 0)})
            polled.clear()
            cycled.clear()
            listener.on_partitions_assigned({TopicPartition("test-config", 1)})
            await asyncio.wait_for(cycled.wait(), 5)
            assert set(polled) == {"B"}
            assert runner.tokens == frozenset({1})
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_sharded_restore_precedes_polling():
    """A freshly-assigned token sees the changelog state (the previous
    owner's final flush) on its very first poll."""

    async def run():
        key = key_with_token(0, 1)
        seen: list[int] = []
        cycled = asyncio.Event()

        class CursorExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                seen.append(state.get(CURSOR, -1))
                cycled.set()
                return
                yield  # pragma: no cover

        runner = make_sharded_runner(
            CursorExtractor(),
            consumer=FakeKafkaConsumer([json_record(key=key, value={"api_key": "a"})]),
            restore_records=[make_record(key=key, value=b'{"cursor":7}', topic="test-changelog")],
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(cycled.wait(), 5)
            assert seen[0] == 7
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_revoke_barrier_cancels_inflight_poll_flushes_and_clears():
    """The revoke callback is the handover barrier: it cancels a poll that
    is still in flight (an epoch backfill, say), waits for it to unwind,
    and flushes straggler changelog writes — only then may the group
    re-form and the next owner restore."""

    async def run():
        key = key_with_token(0, 1)
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class BlockingExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                started.set()
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
                yield  # pragma: no cover

        stop_saw_cancel_complete: list[bool] = []

        class BarrierOrderProducer(FakeKafkaProducer):
            async def stop(self):
                # The barrier stops the token producers only after the cycle
                # has fully unwound — stopping earlier would pull the
                # producer out from under a poll that is still aborting.
                stop_saw_cancel_complete.append(cancelled.is_set())
                await super().stop()

        runner = make_sharded_runner(
            BlockingExtractor(),
            consumer=FakeKafkaConsumer([json_record(key=key, value={"api_key": "a"})]),
            producer=BarrierOrderProducer(),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener
            listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(started.wait(), 5)

            await listener.on_partitions_revoked({TopicPartition("test-config", 0)})

            assert cancelled.is_set()
            assert runner.cycle is None
            assert runner.tokens == frozenset()
            assert stop_saw_cancel_complete == [True]  # exactly one producer stop, strictly after the cancel
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_sharded_standby_instance_polls_nothing():
    """An instance assigned zero tokens (more replicas than partitions) is a
    hot standby: entries stay warm, nothing is polled."""

    async def run():
        polled: list[str] = []

        class RecordingExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                polled.append(config[API_KEY])
                return
                yield  # pragma: no cover

        from flechtwerk.testing import RecordingObserver
        observer = RecordingObserver()
        runner = make_sharded_runner(
            RecordingExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
            observer=observer,
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener
            await listener.on_partitions_revoked(set())
            listener.on_partitions_assigned(set())
            await asyncio.wait_for(until(lambda: ("tokens_assigned", 0) in observer.calls), 5)

            assert polled == []
            assert runner.tokens == frozenset()
            assert runner.cycle is None
            assert len(runner.entries) == 1  # the config table stays warm

        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_cancel_cycle_reraises_fresh_cancellation_of_the_caller():
    """The suppress in cancel_cycle must swallow ONLY the child's
    cancellation: a fresh cancel of the caller while it awaits the cycle's
    unwind must propagate, or shutdown becomes uncancellable (a later await
    — the barrier flush against an unreachable broker — would hang with no
    way to interrupt)."""

    async def run():
        polling = asyncio.Event()
        unwind_gate = asyncio.Event()

        class SlowUnwindExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                polling.set()
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    await unwind_gate.wait()  # slow cleanup — the unwind blocks here
                    raise
                yield  # pragma: no cover

        runner = make_sharded_runner(
            SlowUnwindExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(polling.wait(), 5)

            suspender = asyncio.create_task(runner.suspend_tokens())
            await asyncio.sleep(0.05)  # suspender is now awaiting the blocked unwind
            suspender.cancel()         # a FRESH cancellation of the caller
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(suspender, 5)
            assert suspender.cancelled()  # propagated — not swallowed as the child's
        finally:
            unwind_gate.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_config_update_lands_next_cycle_not_mid_poll():
    """The main loop drains config updates while a cycle is in flight: the
    running poll keeps the entry snapshot it started with, and the update
    takes effect on the next cycle — pinning both the mid-cycle drain and
    the per-invocation snapshot."""

    async def run():
        gate = asyncio.Event()
        seen: list[str] = []

        class GatedExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                seen.append(config[API_KEY])
                if len(seen) == 1:
                    await gate.wait()
                return
                yield  # pragma: no cover

        consumer = FakeKafkaConsumer([json_record(key="k", value={"api_key": "v1"})])
        runner = make_sharded_runner(GatedExtractor(), consumer=consumer)
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(until(lambda: len(seen) == 1), 5)  # first poll in flight, blocked

            consumer.records = [json_record(key="k", value={"api_key": "v2"}, offset=1)]
            await asyncio.wait_for(until(lambda: runner.entries["k"].config[API_KEY] == "v2"), 5)
            assert seen == ["v1"]  # the in-flight poll still holds its start-of-poll snapshot

            gate.set()
            await asyncio.wait_for(until(lambda: len(seen) >= 2), 5)
            assert seen[1] == "v2"  # the next cycle picks the update up
        finally:
            gate.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_state_yields_are_commit_boundaries():
    """Every State yield commits its page — the messages since the previous
    boundary plus the cursor — in one transaction; an equal re-yield opens
    no transaction; the trailing page commits at generator end."""

    events: list[str] = []

    class TxnRecordingProducer(FakeKafkaProducer):
        async def begin_transaction(self):
            events.append("begin")
            await super().begin_transaction()

        async def commit_transaction(self):
            events.append("commit")
            await super().commit_transaction()

        async def send(self, topic, *, key=None, value=None, partition=None, timestamp_ms=None):
            events.append("send")
            return await super().send(topic, key=key, value=value, partition=partition, timestamp_ms=timestamp_ms)

    class PagedExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            yield Message(key="k", topic="out", value=Event.wrap({"page": 1}))
            yield State.wrap({"cursor": 1})
            yield State.wrap({"cursor": 1})  # unchanged — must open nothing
            yield Message(key="k", topic="out", value=Event.wrap({"page": 2}))
            yield Message(key="k", topic="out", value=Event.wrap({"page": 2}))
            yield State.wrap({"cursor": 2})
            yield Message(key="k", topic="out", value=Event.wrap({"tail": True}))

    async def run():
        producer = TxnRecordingProducer()
        state_store = InMemoryStateStore()
        mod = make_module(PagedExtractor(), producer=producer, state_store=state_store)
        await mod.runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert events == [
            "begin", "send", "commit",          # page 1 → cursor 1
            "begin", "send", "send", "commit",  # page 2 → cursor 2
            "begin", "send", "commit",          # trailing page, no cursor change
        ]
        assert producer.committed == 3
        assert producer.aborted == 0
        assert (await state_store.get("k")).raw == {"cursor": 2}

    asyncio.run(run())


def test_error_after_boundary_keeps_committed_pages_and_aborts_the_open_one():
    """A poll failing mid-extraction keeps every committed page — messages
    durable, cursor advanced — while the open page is aborted, so re-polling
    from the committed cursor duplicates nothing downstream."""

    class FailingAfterPageExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            yield Message(key="k", topic="out", value=Event.wrap({"page": 1}))
            yield State.wrap({"cursor": 1})
            yield Message(key="k", topic="out", value=Event.wrap({"page": 2}))
            raise RuntimeError("source went away")

    async def run():
        producer = FakeKafkaProducer()
        state_store = InMemoryStateStore()
        mod = make_module(FailingAfterPageExtractor(), producer=producer, state_store=state_store)
        with pytest.raises(RuntimeError, match="source went away"):
            await mod.runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert producer.committed == 1
        assert producer.aborted == 1
        assert (await state_store.get("k")).raw == {"cursor": 1}

    asyncio.run(run())


def test_empty_poll_opens_no_transaction():
    """An idle cycle costs no transaction-coordinator round-trips."""

    class IdleExtractor(Extractor):
        config_topics = ["cfg"]

        async def poll(self, config, state) -> AsyncIterator[Message | State]:
            return
            yield  # pragma: no cover

    async def run():
        producer = FakeKafkaProducer()
        mod = make_module(IdleExtractor(), producer=producer)
        await mod.runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert producer.committed == 0
        assert producer.aborted == 0
        assert not producer.in_transaction

    asyncio.run(run())


def test_delivery_failure_crashes_before_state_is_persisted():
    """aiokafka's flush() never retrieves delivery results — poll_one must
    (before every commit), so a delivery-stage failure (broker-side
    non-retriable produce error, batch TTL expiry) aborts the page before
    the advanced cursor is persisted or an MQTT pending is ACKed. Without
    the retrieval, the lost records would never be re-polled."""

    class DeliveryFailingProducer(FakeKafkaProducer):
        async def send(self, topic, *, key=None, value=None, partition=None, timestamp_ms=None):
            await super().send(topic, key=key, value=value, partition=partition, timestamp_ms=timestamp_ms)
            delivery = asyncio.get_running_loop().create_future()
            delivery.set_exception(ConnectionError("delivery failed"))
            return delivery

    async def run():
        state_store = InMemoryStateStore()
        await state_store.put("k", State.wrap({"cursor": 5}))

        mod = make_module(SimpleExtractor(), producer=DeliveryFailingProducer(), state_store=state_store)
        with pytest.raises(ConnectionError, match="delivery failed"):
            await mod.runner.poll_one(ConfigEntry(config=Config.wrap({"api_key": "k"}), state_key="k"))

        assert (await state_store.get("k")).raw == {"cursor": 5}

    asyncio.run(run())


def test_count_tokens_requires_known_partitions():
    """A config topic without partition metadata must fail fast — a silent
    zero would divide-by-zero ownership or own nothing forever."""

    async def run():
        runner = make_sharded_runner(SimpleExtractor(), consumer=FakeKafkaConsumer())
        with pytest.raises(RuntimeError, match="no partitions known"):
            await runner.count_tokens()

    asyncio.run(run())


def test_rebalance_lock_serializes_revoke_against_inflight_restore():
    """Pins the lock pairing run()'s docstring forbids removing: a revoke
    landing while start_pending_tokens is mid-restore must wait for the
    restore to finish — otherwise the restore's completion would resurrect
    tokens the group has meanwhile handed elsewhere (dual ownership)."""

    async def run():
        release = asyncio.Event()
        restoring = asyncio.Event()

        class BlockingRestoreConsumer(FakeKafkaConsumer):
            async def getmany(self, *tps: TopicPartition, timeout_ms: int = 0) -> dict:
                restoring.set()
                await release.wait()
                return await super().getmany(*tps, timeout_ms=timeout_ms)

        runner = make_sharded_runner(
            SimpleExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        # A changelog backlog makes the restore actually read — and block.
        runner.create_restore_consumer = lambda: BlockingRestoreConsumer(
            [make_record(key="k", value=b'{"cursor":1}', topic="test-changelog")],
        )
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener
            listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(restoring.wait(), 5)  # the main loop holds the lock, blocked in restore

            revoke = asyncio.create_task(listener.on_partitions_revoked({TopicPartition("test-config", 0)}))
            await asyncio.sleep(0.05)
            assert not revoke.done()  # the barrier waits behind the rebalance lock

            release.set()
            await asyncio.wait_for(revoke, 5)  # restore finished, then the revoke tore down
            assert runner.tokens == frozenset()
            assert runner.cycle is None
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_revoke_discards_pending_assignment_from_a_dead_generation():
    """A revoke invalidates any not-yet-consumed assignment: consuming it
    later would resurrect ownership the group has meanwhile handed to
    another instance (dual owners), and starting a second cycle over a
    surviving one would double-poll forever."""

    async def run():
        from flechtwerk.extractor import TokenRebalanceListener

        runner = make_sharded_runner(SimpleExtractor())
        runner.num_tokens = 1
        listener = TokenRebalanceListener(runner)

        listener.on_partitions_assigned({TopicPartition("test-config", 0)})
        assert runner.pending == {0}

        await listener.on_partitions_revoked(set())  # next generation begins
        assert runner.pending is None

        await runner.start_pending_tokens()  # the stale assignment is gone
        assert runner.tokens == frozenset()
        assert runner.cycle is None

    asyncio.run(run())


def test_listener_failure_is_fatal_for_the_runner():
    """aiokafka swallows listener exceptions — the runner re-raises them
    from its main loop instead ("let it crash" still fires)."""

    async def run():
        class BrokenStore(InMemoryStateStore):
            async def close(self):
                raise RuntimeError("store exploded")

        runner = make_sharded_runner(
            SimpleExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
        listener = runner.membership_consumer.listener
        listener.on_partitions_assigned({TopicPartition("test-config", 0)})
        await asyncio.wait_for(until(lambda: runner.tokens), 5)
        runner.inner_store = BrokenStore()

        await listener.on_partitions_revoked({TopicPartition("test-config", 0)})

        with pytest.raises(RuntimeError, match="store exploded"):
            await asyncio.wait_for(task, 5)

    asyncio.run(run())


def test_runner_reconciles_active_configs_before_each_cycle():
    """The runner hands on_active_configs the owned, non-suspended config
    set at the cycle top — suspended configs never reach the hook, and a
    tombstone empties it. This is the hook the MQTT template's unsubscribe
    lifecycle rides."""

    async def run():
        seen: list[set[str]] = []

        class ReconcilingExtractor(Extractor):
            config_topics = ["test-config"]

            async def on_active_configs(self, configs) -> None:
                seen.append(set(configs))

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                return
                yield  # pragma: no cover

        consumer = FakeKafkaConsumer([
            json_record(key="active", value={"api_key": "a"}, offset=0),
            json_record(key="suspended", value={"api_key": "b", "suspended": True}, offset=1),
        ])
        runner = make_sharded_runner(ReconcilingExtractor(), consumer=consumer)
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(until(lambda: {"active"} in seen), 5)
            assert all(keys == {"active"} for keys in seen)  # the suspended config never appears

            consumer.records = [json_record(key="active", value={}, offset=2)]  # tombstone
            await asyncio.wait_for(until(lambda: set() in seen), 5)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_standby_assignment_reconciles_empty_active_set():
    """An instance left a hot standby has no cycle loop to reconcile from —
    the runner hands the stage an empty active set right at the (settled)
    assignment, releasing the per-config resources its lost tokens leave
    behind (MQTT: everything unsubscribed)."""

    async def run():
        reconciled = asyncio.Event()
        seen: list[set[str]] = []

        class ReconcilingExtractor(Extractor):
            config_topics = ["test-config"]

            async def on_active_configs(self, configs) -> None:
                seen.append(set(configs))
                reconciled.set()

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                return
                yield  # pragma: no cover

        runner = make_sharded_runner(
            ReconcilingExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener
            await listener.on_partitions_revoked(set())
            listener.on_partitions_assigned(set())
            await asyncio.wait_for(reconciled.wait(), 5)

            assert seen == [set()]
            assert runner.cycle is None
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_revoke_barrier_never_reconciles():
    """Loss-safety pin: suspend_tokens must NOT call on_active_configs — a
    transient revoke→assign self-handover must find the MQTT template's
    rolled-back buffer intact, and a reconcile against the momentarily
    empty token set would unsubscribe and drop it. Reconciliation belongs
    to settled assignments only: the cycle top, or the standby branch of
    start_pending_tokens."""

    async def run():
        cycled = asyncio.Event()
        seen: list[set[str]] = []

        class ReconcilingExtractor(Extractor):
            config_topics = ["test-config"]

            async def on_active_configs(self, configs) -> None:
                seen.append(set(configs))
                cycled.set()

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                return
                yield  # pragma: no cover

        runner = make_sharded_runner(
            ReconcilingExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            listener = runner.membership_consumer.listener
            listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(cycled.wait(), 5)
            assert {"k"} in seen

            await listener.on_partitions_revoked({TopicPartition("test-config", 0)})

            assert set() not in seen  # the barrier itself reconciled nothing
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_mqtt_extractor_unsubscribes_tombstoned_config_through_runner():
    """End-to-end wiring of the subscription lifecycle: a config tombstone
    reaches MqttExtractor.on_active_configs via the cycle-top reconcile,
    which unsubscribes the topic and latches an empty desired set."""

    async def run():
        from flechtwerk.mqtt import MqttExtractor
        from flechtwerk.testing import FakeMqttConnection

        ext = MqttExtractor.of(config_topics=["test-config"], relay=lambda config, topic, payload: None)
        ext.connection = FakeMqttConnection()
        consumer = FakeKafkaConsumer([json_record(key="k", value={"topic": "t/+/events"})])
        runner = make_sharded_runner(ext, consumer=consumer)
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        try:
            await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
            runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})
            await asyncio.wait_for(until(lambda: "t/+/events" in ext.connection.subscriptions), 5)
            assert ext.connection.desired == {"t/+/events"}

            consumer.records = [json_record(key="k", value={}, offset=1)]  # tombstone
            await asyncio.wait_for(until(lambda: "t/+/events" in ext.connection.unsubscribed), 5)
            assert ext.connection.subscriptions == {}
            assert ext.connection.desired == set()
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(run())


def test_poll_error_in_sharded_cycle_crashes_the_runner():
    """An error inside a background poll cycle surfaces from run()."""

    async def run():
        class FailingExtractor(Extractor):
            config_topics = ["test-config"]

            async def poll(self, config, state) -> AsyncIterator[Message | State]:
                raise RuntimeError("boom")
                yield  # pragma: no cover

        runner = make_sharded_runner(
            FailingExtractor(),
            consumer=FakeKafkaConsumer([json_record(key="k", value={"api_key": "a"})]),
        )
        runner.num_tokens = 1
        task = asyncio.create_task(runner.run())
        await asyncio.wait_for(until(lambda: runner.membership_consumer.listener is not None), 5)
        runner.membership_consumer.listener.on_partitions_assigned({TopicPartition("test-config", 0)})

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(task, 5)

    asyncio.run(run())
