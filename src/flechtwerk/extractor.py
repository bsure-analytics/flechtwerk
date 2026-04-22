"""Extractor base class and runner for poll-driven data extraction."""
import asyncio
import logging
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from reactor_di import lookup

from .kafka import encode_json, datetime_to_millis, parse_message
from .state import StateStore
from .types import Config, IncomingMessage, Message, State

log = logging.getLogger(__name__)

SUSPENDED = "suspended"

PollFn = Callable[[Config, State], AsyncIterator[Message | State]]
EnrichFn = Callable[[Config], Awaitable[Config]]
ExtractKeyFn = Callable[[IncomingMessage], str]


@dataclass
class ConfigEntry:
    """Paired Config and state key — always created, updated, and deleted together."""
    config: Config
    state_key: str


class Extractor:
    """Poll-driven data extractor (stateful or stateless).

    Can be used directly with a poll function::

        extractor = Extractor(
            input_topics=["my-config"],
            poll=my_poll_fn,
        )

    Or subclassed for lifecycle management (HTTP clients, MQTT sessions):

        class MyExtractor(Extractor):
            input_topics = ["my-config"]

            async def __aenter__(self):
                self.http = httpx.AsyncClient()
                return self

            async def __aexit__(self, *exc_info):
                await self.http.aclose()

            async def poll(self, config, state):
                ...

    Extractors do not use Kafka consumer groups — config topics are re-read
    from earliest on every startup. The `group_id` used for changelog topic
    naming and client ID defaults is set on `FretworxModule` by the caller;
    stages don't carry it.
    """

    input_topics: list[str]

    def __init__(
        self,
        *,
        input_topics: list[str] | None = None,
        poll: PollFn | None = None,
        enrich: EnrichFn | None = None,
        extract_key: ExtractKeyFn | None = None,
    ):
        if input_topics is not None:
            self.input_topics = input_topics
        if poll is not None:
            self.poll = poll
        if enrich is not None:
            self.enrich = enrich
        if extract_key is not None:
            self.extract_key = extract_key

    def extract_key(self, msg: IncomingMessage) -> str:
        """Extract the state key from the incoming message. Default: msg.key.

        The default is the Kafka message key, which by convention carries the
        operator-facing identity (e.g. `{tenancy_id}/{channel_id}`). This is
        stable across credential rotations — rotating an API key via a new
        config message preserves the state entry. Override only if the
        operator-facing identity doesn't match the desired state namespace.
        """
        return msg.key

    async def enrich(self, config: Config) -> Config:
        """One-time enrichment when a config first arrives or updates.

        Called once per config message, NOT on every poll tick.
        Override for e.g. SumUp merchant code lookup.
        """
        return config

    async def __aenter__(self) -> Extractor:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Poll an external API and yield Messages.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted. Yielding an empty/falsy State deletes the entry from the
        state store (and writes a Kafka tombstone to the changelog). On crash,
        the last-persisted state is retained.
        """
        raise NotImplementedError("Provide a poll function or override in a subclass")


class ExtractorRunner:
    """Orchestrates concurrent polling for an Extractor subclass.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    """

    consumer: AIOKafkaConsumer
    extractor: lookup[Extractor, "stage"]  # noqa: PyUnresolvedReferences
    poll_interval_seconds: int
    producer: AIOKafkaProducer
    state_store: StateStore

    def __init__(self):
        self.configs: dict[str, ConfigEntry] = {}

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
                    k: e for k, e in self.configs.items()
                    if not e.config.get(SUSPENDED)
                }
                if active:
                    log.debug("Polling %d active config(s)", len(active))
                    results = await asyncio.gather(
                        *(self.poll_one(e) for e in active.values()),
                        return_exceptions=True,
                    )
                    for key, result in zip(active.keys(), results):
                        if isinstance(result, Exception):
                            log.error("Poll failed for key %s", key, exc_info=result)
                            raise result

                await asyncio.sleep(self.poll_interval_seconds)

    async def load_initial_configs(self) -> None:
        """Read all existing configs from the topic on startup.

        Compacts messages by key, treating empty values as tombstones
        that remove the key entirely — matching Kafka log compaction.
        """
        latest: dict[str, IncomingMessage] = {}
        while True:
            records = await self.consumer.getmany(timeout_ms=2000)
            if not records:
                break
            for tp, msgs in records.items():
                for raw_msg in msgs:
                    msg = parse_message(raw_msg)
                    if msg.value:
                        latest[msg.key] = msg
                    else:
                        latest.pop(msg.key, None)
        for msg in latest.values():
            await self.apply_config(msg)
        log.info("Loaded %d initial config(s)", len(self.configs))

    async def check_config_updates(self) -> None:
        """Non-blocking check for config changes."""
        records = await self.consumer.getmany(timeout_ms=0)
        for tp, msgs in records.items():
            for raw_msg in msgs:
                await self.apply_config(parse_message(raw_msg))

    async def apply_config(self, msg: IncomingMessage) -> None:
        """Enrich and store a config update."""
        key = msg.key
        config = Config(msg.value)
        if not config:
            if key in self.configs:
                del self.configs[key]
                log.info("Removed config for key %s", key)
            return

        config = await self.extractor.enrich(config)
        present = key in self.configs
        self.configs[key] = ConfigEntry(
            config=config,
            state_key=self.extractor.extract_key(msg),
        )
        log.info("%s config for key %s", "Updated" if present else "Added", key)

    async def poll_one(self, entry: ConfigEntry) -> None:
        """Poll a single config: run poll, persist state on success."""
        state = State(await self.state_store.get(entry.state_key) or {})
        baseline = deepcopy(state)

        messages: list[Message] = []
        new_state: State | None = None
        async for item in self.extractor.poll(entry.config, state):
            if isinstance(item, State):
                new_state = item
            elif isinstance(item, Message):
                messages.append(item)
            else:
                raise TypeError(f"poll() yielded {type(item).__name__}, expected Message or State")

        await self.send_batch(messages)
        if new_state is not None and new_state != baseline:
            if new_state:
                await self.state_store.put(entry.state_key, new_state)
            else:
                await self.state_store.delete(entry.state_key)

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
