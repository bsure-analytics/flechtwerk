"""Tests for fretworx.module topic-declaration validation and MQTT wiring."""
from typing import AsyncIterator

import pytest

from fretworx.extractor import Extractor
from fretworx.module import MqttBrokerConfig, _FretworxModule, validate_topics
from fretworx.mqtt import MqttExtractor
from fretworx.observer import Observer
from fretworx.transformer import Transformer
from fretworx.types import Message, State


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


# -- configured_stage ----------------------------------------------------------


def make_mqtt_module(stage, mqtt: MqttBrokerConfig | None) -> _FretworxModule:
    mod = _FretworxModule()
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
    the stage with the settings unchanged, plus the container's observer."""
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    mqtt = MqttBrokerConfig(broker="b", port=1883, client_id="pod-0")
    mod = make_mqtt_module(stage, mqtt)

    assert mod.configured_stage is stage
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
    mqtt = MqttBrokerConfig(broker="b", port=1883, client_id="pod-0")
    mod = make_mqtt_module(stage, mqtt)

    assert mod.runner.extractor is stage
    assert stage.mqtt is mqtt
