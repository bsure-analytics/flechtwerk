"""Tests for fretworx.module topic-declaration validation."""
from typing import AsyncIterator

import pytest

from fretworx.extractor import Extractor
from fretworx.module import validate_topics
from fretworx.transformer import Transformer
from fretworx.types import Message, State


async def noop_poll(config, state) -> AsyncIterator[Message | State]:
    return
    yield  # pragma: no cover


async def noop_transform(msg, state) -> AsyncIterator[Message | State]:
    return
    yield  # pragma: no cover


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
