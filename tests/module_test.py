"""Tests for flechtwerk.module topic-declaration validation and MQTT wiring."""
import asyncio
from datetime import timedelta
from typing import AsyncIterator

import pytest

from flechtwerk.extractor import Extractor
from flechtwerk.module import (
    MqttBrokerConfig,
    _FlechtwerkModule,
    validate_poll_interval,
    validate_topics,
)
from flechtwerk.mqtt import MqttExtractor
from flechtwerk.observer import Observer
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


# -- membership ----------------------------------------------------------------


def test_membership_consumer_exists_only_for_extractors():
    """Every extractor gets the lease-holding membership consumer; a
    transformer's work is already partitioned by its input topics."""
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


# -- configured_stage ----------------------------------------------------------


def make_mqtt_module(stage, mqtt: MqttBrokerConfig | None) -> _FlechtwerkModule:
    mod = _FlechtwerkModule()
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
