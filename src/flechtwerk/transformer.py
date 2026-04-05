"""Transformer base class and runner for event-driven stream processing."""
from __future__ import annotations

import copy
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

from .kafka import KafkaConsumer, KafkaProducer
from .state import StateStore
from .types import IncomingMessage, Message

log = logging.getLogger(__name__)


class Transformer(ABC):
    """Base class for event transformers (stateless or stateful).

    Subclass contract:
    - Set `input_topics` to the Kafka topic(s) to consume
    - Override `transform()` to yield output Messages
    - Set `stateful = True` if the transformer maintains per-key state
    - Optionally override `key_fn()`, `__aenter__`/`__aexit__`
    """

    input_topics: list[str]
    stateful: bool = False

    def key_fn(self, msg: IncomingMessage) -> str:
        """Extract partition key for stateful processing. Default: message key."""
        return msg.key

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        pass

    @abstractmethod
    async def transform(
        self, msg: IncomingMessage, state: dict[str, Any] | None,
    ) -> AsyncIterator[Message]:
        """Transform an incoming message into zero or more output Messages.

        For stateful=False: state is always None.
        For stateful=True: state is a mutable dict scoped to key_fn(msg).
            Mutate in place; the framework persists after successful completion.
        For transformers with no async I/O, simply don't await anything.
        """
        ...
        yield  # pragma: no cover — make this a valid async generator


class TransformerRunner:
    """Runs a Transformer as a Kafka consumer-producer loop with exactly-once semantics."""

    def __init__(
        self,
        transformer: Transformer,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        state_store: StateStore,
    ):
        self.transformer = transformer
        self.consumer = consumer
        self.producer = producer
        self.state_store = state_store

    async def run(self) -> None:
        """Main event loop. Consumes messages and processes them sequentially."""
        async with self.transformer:
            await self.consumer.subscribe(self.transformer.input_topics)

            while True:
                messages = await self.consumer.poll(timeout=1.0)
                if not messages:
                    continue
                for msg in messages:
                    await self.process_one(msg)

    async def process_one(self, msg: IncomingMessage) -> None:
        """Process a single incoming message with exactly-once semantics."""
        state = None
        state_copy = None
        key = None

        if self.transformer.stateful:
            key = self.transformer.key_fn(msg)
            state = self.state_store.get(key) or {}
            state_copy = copy.deepcopy(state)

        output: list[Message] = []
        async for out_msg in self.transformer.transform(msg, state_copy):
            output.append(out_msg)

        # Exactly-once: produce all messages and commit offset atomically
        await self.producer.send_transactional(
            messages=output,
            consumer=self.consumer,
        )

        if self.transformer.stateful and key is not None:
            self.state_store.put(key, state_copy)
