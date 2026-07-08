"""Tests for fretworx.module topic-declaration validation and MQTT wiring."""
from typing import AsyncIterator

import pytest

from fretworx.extractor import Extractor
from fretworx.module import Fretworx, MqttBrokerConfig, validate_topics
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


# -- configure_mqtt -----------------------------------------------------------


def make_mqtt_module(stage, mqtt: MqttBrokerConfig | None) -> Fretworx:
    mod = Fretworx()
    mod.client_id = "pod-0"
    mod.metrics_port = 0  # observer resolves to the no-op Observer
    mod.mqtt = mqtt
    mod.stage = stage
    return mod


def test_configure_mqtt_none_is_noop():
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    make_mqtt_module(stage, None).configure_mqtt()
    assert not hasattr(stage, "mqtt")


def test_configure_mqtt_injects_settings_verbatim():
    """Identity resolution is the entry point's job — the container places
    the settings on the stage unchanged, plus its observer."""
    stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    mqtt = MqttBrokerConfig(broker="b", port=1883, client_id="pod-0")
    mod = make_mqtt_module(stage, mqtt)

    mod.configure_mqtt()

    assert stage.mqtt is mqtt
    assert stage.observer is mod.observer  # the container's observer, not the class default


def test_configure_mqtt_ignores_non_mqtt_stage():
    """__main__ passes the platform MQTT settings unconditionally for every
    stage; only MQTT-sourced stages receive them."""
    stage = Extractor.of(config_topics=["cfg"], poll=noop_poll)
    make_mqtt_module(stage, MqttBrokerConfig(broker="b", port=1883)).configure_mqtt()
    assert not hasattr(stage, "mqtt")


def test_configure_mqtt_tolerates_unwired_slot():
    """A parent module that never wires the mqtt slot must not break (the
    bare-constructor path)."""
    mod = Fretworx()
    mod.stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
    mod.configure_mqtt()


def test_aenter_injects_mqtt_settings(monkeypatch):
    """Fretworx.__aenter__ runs configure_mqtt — the stage is configured
    before the runner ever enters it."""
    import asyncio
    from types import SimpleNamespace

    from fretworx.testing import FakeKafkaConsumer, FakeKafkaProducer

    class FakeAdminClient:
        def __init__(self, **kwargs) -> None:
            pass

        async def start(self) -> None:
            pass

        async def close(self) -> None:
            pass

        async def describe_topics(self, topics):
            return [{"topic": t, "error_code": 0, "partitions": [{"partition": 0}]} for t in topics]

        async def create_topics(self, new_topics):
            return SimpleNamespace(topic_errors=[(t.name, 36, "exists") for t in new_topics])  # 36 = already exists

    class FakeStateStore:
        async def restore(self, consumer) -> None:
            pass

        async def close(self) -> None:
            pass

    async def run():
        stage = MqttExtractor.of(config_topics=["cfg"], relay=noop_relay)
        mod = make_mqtt_module(stage, MqttBrokerConfig(broker="b", port=1883, client_id="pod-0"))
        mod.application_id = "test"
        mod.bootstrap_servers = "localhost:9092"
        mod.compression_type = None
        mod.consumer = FakeKafkaConsumer()
        mod.producer = FakeKafkaProducer()
        mod.state_store = FakeStateStore()
        monkeypatch.setattr("fretworx.module.AIOKafkaAdminClient", FakeAdminClient)
        monkeypatch.setattr("fretworx.module.AIOKafkaConsumer", lambda **kwargs: FakeKafkaConsumer())

        async with mod:
            assert stage.mqtt == MqttBrokerConfig(broker="b", port=1883, client_id="pod-0")
            assert stage.observer is mod.observer

    asyncio.run(run())
