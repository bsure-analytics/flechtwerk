"""Extractor base class and runner for poll-driven data extraction."""
import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import AsyncIterator, Never

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from reactor_di import lookup

from fretworx.attribute import BOOL, OptionalAttribute
from .kafka import encode_json, datetime_to_millis, parse_message
from .observer import Observer
from .state import StateStore
from .types import Config, IncomingMessage, Message, State

log = logging.getLogger(__name__)

SUSPENDED = OptionalAttribute("suspended", BOOL)

PollFn = Callable[[Config, State], AsyncIterator[Message | State]]
EnrichFn = Callable[[Config], Awaitable[Config]]
ExtractKeyFn = Callable[[IncomingMessage], str]


@dataclass(frozen=True, slots=True)
class ConfigEntry:
    """Paired Config and state key — always created, updated, and deleted together."""
    config: Config
    state_key: str


class Extractor(ABC):
    """Poll-driven data extractor (stateful or stateless).

    Two ways to construct one:

    * Functionally with ``Extractor.of(...)``, supplying a poll function
      and the input topics::

          stage = Extractor.of(
              input_topics=["my-config"],
              poll=my_poll_fn,
          )

    * As a subclass for lifecycle management (HTTP clients, MQTT sessions)::

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
    from the earliest on every startup. The caller sets the ``application_id``
    used for changelog topic naming on `Fretworx`; stages don't carry it.
    """

    input_topics: list[str]

    @classmethod
    def of(
            cls,
            *,
            input_topics: list[str],
            poll: PollFn,
            enrich: EnrichFn | None = None,
            extract_key: ExtractKeyFn | None = None,
    ) -> Extractor:
        """Build an Extractor from a poll function and input topics.

        ``enrich`` and ``extract_key`` are optional overrides; omit them
        to use the defaults (no enrichment, ``extract_key`` returns the
        Kafka message key).

        Patches the supplied callables in as instance attributes that
        shadow the class-level abstract method ``poll`` (and, when
        provided, the default ``enrich`` / ``extract_key`` methods). The
        ABC discipline still applies to every other construction path —
        ``Extractor()`` and any abstract subclass remain uninstantiable.
        """
        instance = _FunctionalExtractor()
        instance.input_topics = input_topics
        instance.poll = poll
        if enrich is not None:
            instance.enrich = enrich
        if extract_key is not None:
            instance.extract_key = extract_key
        return instance

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

    @abstractmethod
    async def poll(self, config: Config, state: State) -> AsyncIterator[Message | State]:
        """Poll an external API and yield Messages.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted. Yielding an empty/falsy State deletes the entry from the
        state store (and writes a Kafka tombstone to the changelog). On crash,
        the last-persisted state is retained.
        """


class _FunctionalExtractor(Extractor):
    """Shell subclass used solely as the instantiation target for ``Extractor.of``.

    The class-level ``poll = None`` is a placeholder that satisfies
    ``ABCMeta``'s abstract-method check; ``of()`` shadows it with an
    instance attribute on every call.
    """
    poll = None  # type: ignore[assignment]


class ExtractorRunner:
    """Orchestrates concurrent polling for an Extractor subclass.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    """

    consumer: AIOKafkaConsumer
    extractor: lookup[Extractor, "stage"]  # noqa: PyUnresolvedReferences
    observer: Observer
    poll_interval_seconds: int
    producer: AIOKafkaProducer
    state_store: StateStore

    def __init__(self):
        self.configs: dict[str, ConfigEntry] = {}

    async def run(self) -> Never:
        """Main event loop. Runs until cancelled or an unrecoverable error occurs.

        Resource lifecycle (consumer/producer start/stop) is managed by
        Fretworx, not the runner.
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
                self.observer.active_configs(len(active))
                if active:
                    log.debug("Polling %d active config(s)", len(active))
                    with self.observer.poll_cycle_scope():
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
        self.observer.message_in(msg.topic)
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
        with self.observer.dispatch_scope():
            async for item in self.extractor.poll(entry.config, state):
                if isinstance(item, State):
                    new_state = item
                elif isinstance(item, Message):
                    messages.append(item)
                    self.observer.message_out(item.topic)
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
