"""Tests for flechtwerk.module topic-declaration validation and MQTT wiring."""
import asyncio
from datetime import timedelta
from typing import AsyncIterator

import pytest
from prometheus_client import CollectorRegistry

from flechtwerk.extractor import Extractor
from flechtwerk.module import (
    MqttBrokerConfig,
    _FlechtwerkModule,
    ensure_topics,
    validate_poll_interval,
    validate_topics,
)
from flechtwerk.mqtt import MqttExtractor
from flechtwerk.transformer import Transformer
from flechtwerk.types import Message, State


async def noop_poll(config, state) -> AsyncIterator[Message | State]:
    return
    yield  # pragma: no cover


async def noop_transform(msg, state) -> AsyncIterator[Message | State]:
    return
    yield  # pragma: no cover


def noop_relay(config, topic, payload) -> Message | None:
    return None


def test_transformer_without_input_topics_is_rejected():
    stage = Transformer.of(input_topics=[], transform=noop_transform)
    with pytest.raises(ValueError, match="at least one"):
        validate_topics(stage)


def test_topic_declared_both_input_and_config_is_rejected():
    stage = Transformer.of(input_topics=["dual", "in"], transform=noop_transform)
    stage.config_topics = ["dual"]
    with pytest.raises(ValueError, match="both input and config.*dual"):
        validate_topics(stage)


def test_extractor_without_config_topics_is_rejected():
    stage = Extractor.of(config_topics=[], poll=noop_poll)
    with pytest.raises(ValueError, match="at least one config"):
        validate_topics(stage)


def test_valid_declarations_pass():
    validate_topics(Extractor.of(config_topics=["cfg"], poll=noop_poll))
    validate_topics(Transformer.of(input_topics=["in"], transform=noop_transform))
    mixed = Transformer.of(input_topics=["in"], transform=noop_transform)
    mixed.config_topics = ["cfg"]
    validate_topics(mixed)


def test_extractor_requires_positive_poll_interval():
    stage = Extractor.of(config_topics=["cfg"], poll=noop_poll)
    for bad in (None, timedelta(0)):
        with pytest.raises(ValueError, match="positive poll_interval"):
            validate_poll_interval(stage, bad)


def test_poll_interval_optional_for_transformer_positive_for_extractor():
    # a transformer never reads poll_interval, so leaving it unset is fine
    validate_poll_interval(Transformer.of(input_topics=["in"], transform=noop_transform), None)
    # a positive duration satisfies an extractor
    validate_poll_interval(Extractor.of(config_topics=["cfg"], poll=noop_poll), timedelta(seconds=60))


def test_broker_config_is_keyword_only():
    """Fields are alphabetical, so a new one lands mid-list — positional
    construction would silently re-bind every argument after it. Keyword-only
    turns that into a loud TypeError and makes field order a non-event."""
    assert MqttBrokerConfig(broker="b", port=1883).session_expiry == timedelta(hours=24)
    with pytest.raises(TypeError):
        MqttBrokerConfig("b", 1883)  # noqa: the point of the test


# -- ensure_topics (Kafka-broker-side startup checks) --------------------------------


class _FakeCreateResponse:
    def __init__(self, topic_errors):
        self.topic_errors = topic_errors


class _FakeAdmin:
    """AIOKafkaAdminClient stand-in for ensure_topics: canned describe/create replies.

    ``partitions`` maps topic -> partition count; a topic absent from it
    describes as UnknownTopicOrPartitionError. ``changelog_exists`` decides
    whether create_topics reports the changelog as freshly created or
    pre-existing.
    """

    def __init__(self, partitions, changelog_exists=False):
        self._partitions = partitions
        self._changelog_exists = changelog_exists
        self.created = []
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def describe_topics(self, topics):
        from aiokafka.errors import UnknownTopicOrPartitionError

        return [
            {"topic": t, "error_code": 0, "partitions": list(range(self._partitions[t]))}
            if t in self._partitions
            else {"topic": t, "error_code": UnknownTopicOrPartitionError.errno, "partitions": []}
            for t in topics
        ]

    async def create_topics(self, new_topics):
        from aiokafka.errors import TopicAlreadyExistsError

        self.created.extend(nt.name for nt in new_topics)
        errno = TopicAlreadyExistsError.errno if self._changelog_exists else 0
        return _FakeCreateResponse([(nt.name, errno) for nt in new_topics])


def test_ensure_topics_transformer_creates_changelog_and_skips_validation():
    """A just-created changelog is never re-described (the topic isn't even in
    the fake's partition map, so a describe would raise — passing proves the
    ``not created`` short-circuit held)."""
    async def run():
        stage = Transformer.of(input_topics=["in"], transform=noop_transform)
        admin = _FakeAdmin({"in": 3}, changelog_exists=False)
        await ensure_topics(admin, stage, "cl", "app")
        assert admin.created == ["cl"]

    asyncio.run(run())


def test_ensure_topics_transformer_accepts_matching_preexisting_changelog():
    async def run():
        stage = Transformer.of(input_topics=["in"], transform=noop_transform)
        admin = _FakeAdmin({"in": 3, "cl": 3}, changelog_exists=True)
        await ensure_topics(admin, stage, "cl", "app")

    asyncio.run(run())


def test_ensure_topics_transformer_rejects_mismatched_preexisting_changelog():
    async def run():
        stage = Transformer.of(input_topics=["in"], transform=noop_transform)
        admin = _FakeAdmin({"in": 3, "cl": 2}, changelog_exists=True)
        with pytest.raises(ValueError, match="repartitioning requires a state migration"):
            await ensure_topics(admin, stage, "cl", "app")

    asyncio.run(run())


def test_ensure_topics_rejects_unequal_input_partition_counts():
    async def run():
        stage = Transformer.of(input_topics=["a", "b"], transform=noop_transform)
        admin = _FakeAdmin({"a": 2, "b": 3})
        with pytest.raises(ValueError, match="must have equal partition counts"):
            await ensure_topics(admin, stage, "cl", "app")

    asyncio.run(run())


def test_ensure_topics_never_constrains_config_partition_counts():
    """The equal-count rule belongs to `ExtractorRunner.count_tokens` — the only
    code that defines a token space — so the container imposes nothing here,
    and a broker-dispatched stage's differing counts cannot fail its startup.
    The describe still runs, so a missing config topic fails as fast as ever."""
    async def run():
        for stage in (
            Extractor.of(config_topics=["c1", "c2"], poll=noop_poll),
            MqttExtractor.of(config_topics=["c1", "c2"], relay=noop_relay),
        ):
            await ensure_topics(_FakeAdmin({"c1": 2, "c2": 3}), stage, "cl", "app")

    asyncio.run(run())


def test_ensure_topics_still_requires_config_topics_to_exist():
    async def run():
        stage = MqttExtractor.of(config_topics=["missing"], relay=noop_relay)
        with pytest.raises(Exception, match="missing"):
            await ensure_topics(_FakeAdmin({}), stage, "cl", "app")

    asyncio.run(run())


def test_ensure_topics_creates_no_extractor_changelog():
    """An extractor's changelog is created by the loop that needs one
    (``ExtractorRunner.run_sharded`` → ``ensure_changelog``), so a stateless
    broker-dispatched stage gets none — no check, it simply never asks."""
    async def run():
        for stage in (
            Extractor.of(config_topics=["c1"], poll=noop_poll),
            MqttExtractor.of(config_topics=["c1"], relay=noop_relay),
        ):
            admin = _FakeAdmin({"c1": 2})
            await ensure_topics(admin, stage, "cl", "app")
            assert admin.created == []

    asyncio.run(run())


def test_ensure_changelog_creates_the_topic_and_closes_its_admin(monkeypatch):
    """The factory ``run_sharded`` calls when it wants a changelog. Only that
    loop calls it, which is the whole mechanism: a broker-dispatched stage is
    stateless, so no changelog is created for it — nothing checks, it just
    never asks."""
    async def run():
        admin = _FakeAdmin({})
        monkeypatch.setattr("flechtwerk.module.AIOKafkaAdminClient", lambda **_: admin)

        mod = _FlechtwerkModule()
        mod.application_id = "app"
        mod.bootstrap_servers = "localhost:9092"

        await mod.ensure_changelog()

        assert admin.created == ["app-changelog"]
        assert admin.started and admin.closed

    asyncio.run(run())


def test_ensure_changelog_creates_the_topic_and_closes_its_admin(monkeypatch):
    """The factory ``run_sharded`` calls when it wants a changelog. Only that
    loop calls it, which is the whole mechanism: a stage sharded by an MQTT
    broker is stateless, so no changelog is created for it — nothing checks,
    it simply never asks."""
    async def run():
        admin = _FakeAdmin({})
        monkeypatch.setattr("flechtwerk.module.AIOKafkaAdminClient", lambda **_: admin)

        mod = _FlechtwerkModule()
        mod.application_id = "app"
        mod.bootstrap_servers = "localhost:9092"

        await mod.ensure_changelog()

        assert admin.created == ["app-changelog"]
        assert admin.started and admin.closed

    asyncio.run(run())


def test_aenter_calls_ensure_topics_under_admin_try_finally(monkeypatch):
    """__aenter__ runs ensure_topics against a started admin and closes it even
    when a topic check fails (the failure propagates before Kafka client startup)."""
    async def run():
        admin = _FakeAdmin({"a": 2, "b": 3})
        monkeypatch.setattr("flechtwerk.module.AIOKafkaAdminClient", lambda **_: admin)

        mod = _FlechtwerkModule()
        mod.application_id = "app"
        mod.bootstrap_servers = "localhost:9092"
        mod.client_id = "pod-0"
        mod.keyring = None
        mod.metrics_port = 0
        mod.mqtt = None
        mod.poll_interval = None
        mod.stage = Transformer.of(input_topics=["a", "b"], transform=noop_transform)

        with pytest.raises(ValueError, match="must have equal partition counts"):
            await mod.__aenter__()
        assert admin.started and admin.closed

    asyncio.run(run())


# -- membership ----------------------------------------------------------------


def test_membership_consumer_exists_only_for_extractors():
    """Every extractor gets one built lazily; a transformer's work is already
    partitioned by its input topics. Only ``run_sharded`` ever starts it, so a
    broker-dispatched stage never joins a group — no flag needed."""
    def make(stage):
        mod = _FlechtwerkModule()
        mod.application_id = "app"
        mod.bootstrap_servers = "localhost:9092"
        mod.client_id = "pod-0"
        mod.stage = stage
        return mod

    async def run():
        transformer = Transformer.of(input_topics=["in"], transform=noop_transform)
        assert make(transformer).membership_consumer is None

        mod = make(Extractor.of(config_topics=["cfg"], poll=noop_poll))
        consumer = mod.membership_consumer
        assert consumer is not None
        await consumer.stop()  # never started; stop() keeps the double-check ledger clean

    asyncio.run(run())


# -- batch cap -----------------------------------------------------------------


def test_consumer_caps_getmany_batches():
    """The main consumer bounds every getmany() batch via max_poll_records.

    aiokafka's own default is UNBOUNDED (unlike the Java client's 500), so
    without the cap a backlog returns as ONE giant batch: pinned in memory in
    parsed form, one concurrent transform per state key, and long enough to
    outlive max.poll.interval.ms — a mid-batch group eviction. An invalid cap
    must fail at consumer construction (startup), not at the first fetch.
    """
    from flechtwerk.module import Flechtwerk

    def make(**kwargs):
        return Flechtwerk.of(
            application_id="app",
            bootstrap_servers="localhost:9092",
            client_id="pod-0",
            stage=Transformer.of(input_topics=["in"], transform=noop_transform),
            **kwargs,
        )

    async def run():
        # No public accessor on aiokafka's consumer — pin the private slot
        # getmany() reads (the kafka.py `_client` precedent).
        consumer = make().consumer
        assert consumer._max_poll_records == 500  # Kafka's max.poll.records default
        await consumer.stop()  # never started; stop() keeps the double-check ledger clean

        consumer = make(max_poll_records=100).consumer
        assert consumer._max_poll_records == 100
        await consumer.stop()

        with pytest.raises(ValueError, match="max_poll_records"):
            _ = make(max_poll_records=0).consumer

    asyncio.run(run())


def test_batch_size_buckets_follow_max_poll_records():
    """reactor-di wires the getmany() cap onto Metrics.

    The bucket ladder must end at cap-1/cap: a top bucket below the cap pins
    saturated histogram_quantile panels flat, one above it never receives a
    sample, and the cap-1 boundary makes at-cap batches (the consumer-falling-
    behind signal) a single bucket subtraction in PromQL.
    """
    from flechtwerk.module import Flechtwerk

    module = Flechtwerk.of(
        application_id="app",
        bootstrap_servers="localhost:9092",
        client_id="pod-0",
        stage=Transformer.of(input_topics=["in"], transform=noop_transform),
        max_poll_records=100,
    )
    module.registry = CollectorRegistry()  # keep the process-global REGISTRY clean
    # No public accessor on prometheus_client's Histogram — pin the private
    # slot (the module_test `_max_poll_records` precedent).
    assert module.metrics.batch_size._upper_bounds == [
        1, 2, 5, 10, 25, 50, 99, 100, float("inf"),
    ]


# -- configured_stage ----------------------------------------------------------


def make_mqtt_module(stage, mqtt: MqttBrokerConfig | None) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
    mod.application_id = "app"
    mod.client_id = "pod-0"
    mod.metrics_port = 0  # observer resolves to the no-op Observer
    mod.mqtt = mqtt
    mod.stage = stage
    return mod


def test_configured_stage_without_mqtt_is_untouched():
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    assert make_mqtt_module(stage, None).configured_stage is stage
    assert not hasattr(stage, "mqtt")


def test_configured_stage_injects_settings_verbatim():
    """Identity resolution is the entry point's job — the factory completes
    the stage with the settings unchanged, plus the container's client_id
    and observer."""
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    mqtt = MqttBrokerConfig(broker="b", port=1883)
    mod = make_mqtt_module(stage, mqtt)

    assert mod.configured_stage is stage
    assert stage.client_id == "pod-0"  # the container's client_id, not the class default
    assert stage.group == "app"  # the share group IS the application_id
    assert stage.mqtt is mqtt
    assert stage.observer is mod.observer  # the container's observer, not the class default


def test_configured_stage_ignores_non_mqtt_stage():
    """__main__ passes the platform MQTT settings unconditionally for every
    stage; only MQTT-sourced stages receive them."""
    stage = Extractor.of(config_topics=["cfg"], poll=noop_poll)
    assert make_mqtt_module(stage, MqttBrokerConfig(broker="b", port=1883)).configured_stage is stage
    assert not hasattr(stage, "mqtt")


def test_runner_consumes_the_configured_stage():
    """The runner's `extractor` lookup sources `configured_stage`, so the
    stage is complete strictly before the runner enters it."""
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    mqtt = MqttBrokerConfig(broker="b", port=1883)
    mod = make_mqtt_module(stage, mqtt)

    assert mod.runner.extractor is stage
    assert stage.mqtt is mqtt
