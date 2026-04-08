"""Extractor base class and runner for poll-driven data extraction."""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from os import getenv
from typing import AsyncIterator, Final

from reactor_di import lookup

from .kafka import KafkaConsumer, KafkaProducer, encode_json, datetime_to_millis, parse_message
from .state import StateStore
from .types import Config, Message, State

log = logging.getLogger(__name__)

# Seconds between poll cycles
POLL_INTERVAL_SECONDS: Final = int(getenv("POLL_INTERVAL_SECONDS", "60"))

API_KEY = "api_key"
SUSPENDED = "suspended"


class Extractor(ABC):
    """Base class for poll-driven extractors.

    Subclass contract:
    - Set `group_id` to the Kafka consumer group ID
    - Set `input_topics` to the Kafka config topic(s)
    - Override `poll()` to yield Messages from an external API
    - Optionally override `enrich()`, `pre_poll()`, `key_fn()`
    - Optionally override `__aenter__`/`__aexit__` for resource management
    """

    group_id: str
    input_topics: list[str]

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

    async def __aenter__(self) -> Extractor:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    @abstractmethod
    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Poll an external API and yield Messages.

        Yield a State to persist it. The runner collects the last yielded State
        and writes it to the state store after the generator is exhausted. If no
        State is yielded, nothing is persisted. On crash, the last-persisted
        state is retained.
        """
        raise NotImplementedError("Override poll() in a subclass")


class ExtractorRunner:
    """Orchestrates concurrent polling for an Extractor subclass.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    """

    consumer: KafkaConsumer
    extractor: lookup[Extractor, "stage"]
    producer: KafkaProducer
    state_store: StateStore

    def __init__(self):
        self.configs: dict[str, Config] = {}

    async def run(self) -> None:
        """Main event loop. Runs until cancelled or an unrecoverable error occurs.

        Resource lifecycle (consumer/producer start/stop) is managed by
        FretworxModule, not the runner.
        """
        self.consumer.subscribe(self.extractor.input_topics)
        async with self.extractor:
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

                await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def load_initial_configs(self) -> None:
        """Read all existing configs from the topic on startup."""
        while True:
            records = await self.consumer.getmany(timeout_ms=2000)
            if not records:
                break
            for tp, msgs in records.items():
                for raw_msg in msgs:
                    msg = parse_message(raw_msg)
                    await self.apply_config(msg.key, Config(msg.value))
        log.info("Loaded %d initial config(s)", len(self.configs))

    async def check_config_updates(self) -> None:
        """Non-blocking check for config changes."""
        records = await self.consumer.getmany(timeout_ms=0)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                msg = parse_message(raw_msg)
                await self.apply_config(msg.key, Config(msg.value))

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
        """Poll a single config: run poll, persist state on success."""
        state = State(await self.state_store.get(key) or {})
        config = await self.extractor.pre_poll(config)

        messages: list[Message] = []
        new_state = None
        async for item in self.extractor.poll(config, state):
            if isinstance(item, State):
                new_state = item
            else:
                messages.append(item)

        await self.send_batch(messages)
        if new_state is not None:
            await self.state_store.put(key, new_state)

    async def send_batch(self, messages: list[Message]) -> None:
        """Send messages to Kafka."""
        for msg in messages:
            await self.producer.send(
                msg.topic,
                key=encode_json(msg.key),
                value=encode_json(msg.value),
                timestamp_ms=datetime_to_millis(msg.timestamp),
            )
        await self.producer.flush()
