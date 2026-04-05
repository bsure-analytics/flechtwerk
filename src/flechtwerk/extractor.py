"""Extractor base class and runner for poll-driven data extraction."""
from __future__ import annotations

import asyncio
import copy
import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from os import getenv
from typing import AsyncIterator

from .kafka import KafkaConsumer, KafkaProducer
from .state import StateStore
from .types import Config, Message, State

log = logging.getLogger(__name__)

API_KEY = "api_key"
SUSPENDED = "suspended"


class Extractor(ABC):
    """Base class for poll-driven extractors.

    Subclass contract:
    - Set `input_topics` to the Kafka config topic(s)
    - Override `poll()` to yield Messages from an external API
    - Optionally override `enrich()`, `pre_poll()`, `key_fn()`
    - Optionally override `__aenter__`/`__aexit__` for resource management
    """

    input_topics: list[str]
    poll_interval_seconds: int | None = None

    def key_fn(self, config: Config) -> str:
        """Extract the partitioning key from a config. Default: config["api_key"]."""
        return config[API_KEY]

    async def enrich(self, config: Config) -> Config:
        """One-time enrichment when a config first arrives or updates.

        Called once per config message, NOT on every poll tick.
        Override for e.g. SumUp merchant code lookup.
        """
        return config

    async def pre_poll(self, config: Config) -> Config:
        """Per-cycle enrichment before each poll invocation.

        Called on every poll tick. Override for e.g. TillHub token refresh.
        """
        return config

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        pass

    @abstractmethod
    async def poll(self, state: State, config: Config) -> AsyncIterator[Message]:
        """Poll an external API. Mutate state in place. Yield Messages.

        The framework passes a defensive copy of state. On successful completion
        of the generator, the mutated copy is persisted to the state store. On
        crash, the copy is discarded and the last-persisted state is retained.
        """
        ...
        yield  # pragma: no cover


class ExtractorRunner:
    """Orchestrates concurrent polling for an Extractor subclass."""

    def __init__(
        self,
        extractor: Extractor,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        state_store: StateStore,
    ):
        self.extractor = extractor
        self.consumer = consumer
        self.producer = producer
        self.state_store = state_store
        self.configs: dict[str, Config] = {}
        self.poll_interval = timedelta(
            seconds=extractor.poll_interval_seconds
            or int(getenv("POLL_INTERVAL_SECONDS", "60"))
        )

    async def run(self) -> None:
        """Main event loop. Runs until cancelled or an unrecoverable error occurs."""
        async with self.extractor:
            await self.consumer.subscribe(self.extractor.input_topics)
            await self.load_initial_configs()

            while True:
                await self.check_config_updates()
                active = {
                    k: v for k, v in self.configs.items()
                    if not v.get(SUSPENDED)
                }
                if active:
                    log.debug("Polling %d active config(s)", len(active))
                    results = await asyncio.gather(
                        *(self.poll_one(k, v) for k, v in active.items()),
                        return_exceptions=True,
                    )
                    for key, result in zip(active.keys(), results):
                        if isinstance(result, Exception):
                            log.error("Poll failed for key %s", key, exc_info=result)
                            raise result

                await asyncio.sleep(self.poll_interval.total_seconds())

    async def load_initial_configs(self) -> None:
        """Read all existing configs from the topic on startup."""
        while True:
            messages = await self.consumer.poll(timeout=2.0)
            if not messages:
                break
            for msg in messages:
                await self.apply_config(msg.key, msg.value)
        log.info("Loaded %d initial config(s)", len(self.configs))

    async def check_config_updates(self) -> None:
        """Non-blocking check for config changes."""
        messages = await self.consumer.poll(timeout=0)
        for msg in messages:
            await self.apply_config(msg.key, msg.value)

    async def apply_config(self, key: str, config: Config) -> None:
        """Enrich and store a config update."""
        if not config:
            if key in self.configs:
                del self.configs[key]
                log.info("Removed config for key %s", key)
            return

        config = await self.extractor.enrich(config)
        present = key in self.configs
        self.configs[key] = config
        log.info("%s config for key %s", "Updated" if present else "Added", key)

    async def poll_one(self, key: str, config: Config) -> None:
        """Poll a single config: copy state, run poll, persist on success."""
        state = self.state_store.get(key) or {}
        state_copy = copy.deepcopy(state)

        config = await self.extractor.pre_poll(config)

        messages: list[Message] = []
        async for msg in self.extractor.poll(state_copy, config):
            messages.append(msg)

        await self.producer.send_batch(messages)
        self.state_store.put(key, state_copy)
