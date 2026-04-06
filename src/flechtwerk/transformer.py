"""Transformer base class and runner for event-driven stream processing."""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import AsyncIterator

import aiokafka

from reactor_di import lookup

from .kafka import KafkaConsumer, KafkaProducer, encode_json, datetime_to_millis, parse_message
from .state import StateStore
from .types import Event, IncomingMessage, Message, State

log = logging.getLogger(__name__)

TransformFn = Callable[[IncomingMessage[Event], State], AsyncIterator[Message | State]]
KeyFn = Callable[[IncomingMessage[Event]], str]


class Transformer:
    """Event transformer (stateless or stateful).

    Can be used directly with a transform function::

        transformer = Transformer(
            input_topics=["my-topic"],
            transform=my_transform_fn,
        )

    Or subclassed for lifecycle management (HTTP clients, dedup instances)::

        class MyTransformer(Transformer):
            async def __aenter__(self):
                self.http = httpx.AsyncClient()
                return self

            async def __aexit__(self, *exc_info):
                await self.http.aclose()

            async def transform(self, msg, state):
                ...
    """

    input_topics: list[str]

    def __init__(
        self,
        *,
        input_topics: list[str] | None = None,
        transform: TransformFn | None = None,
        key_fn: KeyFn | None = None,
    ):
        if input_topics is not None:
            self.input_topics = input_topics
        if transform is not None:
            self.transform = transform
        if key_fn is not None:
            self.key_fn = key_fn

    def key_fn(self, msg: IncomingMessage[Event]) -> str:
        """Extract partition key for stateful processing. Default: message key."""
        return msg.key

    async def __aenter__(self) -> Transformer:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    async def transform(self, msg: IncomingMessage[Event], state: State) -> AsyncIterator[Message | State]:
        """Transform an incoming message into zero or more output Messages.

        Yield a State to persist it. The runner collects the last yielded State
        and writes it to the state store after the generator is exhausted. If no
        State is yielded, nothing is persisted (stateless behavior).
        """
        raise NotImplementedError("Provide a transform function or override in a subclass")


class TransformerRunner:
    """Runs a Transformer as a Kafka consumer-producer loop with exactly-once semantics.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the ChangelogStateStore — state writes inside
    send_transactional() participate in the same Kafka transaction.
    """

    consumer: KafkaConsumer
    group_id: lookup[str, "application_id"]
    producer: KafkaProducer
    state_store: StateStore
    transformer: lookup[Transformer, "stage"]

    async def run(self) -> None:
        """Main event loop. Consumes messages and processes them sequentially.

        Resource lifecycle (consumer/producer start/stop) is managed by
        FretworxModule, not the runner.
        """
        async with self.transformer:
            while True:
                records = await self.consumer.getmany(timeout_ms=1000)
                if not records:
                    continue
                topic_order = {t: i for i, t in enumerate(self.transformer.input_topics)}
                for tp in sorted(records, key=lambda tp: topic_order.get(getattr(tp, "topic", tp[0]), 0)):
                    for raw_msg in records[tp]:
                        await self.process_one(raw_msg)

    async def process_one(self, raw_msg) -> None:
        """Process a single incoming message with exactly-once semantics."""
        msg = parse_message(raw_msg)
        event_msg = IncomingMessage(
            key=msg.key,
            offset=msg.offset,
            partition=msg.partition,
            timestamp=msg.timestamp,
            topic=msg.topic,
            value=Event(msg.value),
        )

        key = self.transformer.key_fn(event_msg)
        state = State(await self.state_store.get(key) or {})

        output: list[Message] = []
        new_state = None
        async for item in self.transformer.transform(event_msg, state):
            if isinstance(item, State):
                new_state = item
            else:
                output.append(item)

        # Exactly-once: produce output, persist state, and commit offset atomically
        tp = aiokafka.TopicPartition(msg.topic, msg.partition)
        await self.send_transactional(output, new_state, key, {tp: msg.offset + 1})

    async def send_transactional(
        self,
        messages: list[Message],
        new_state: State | None,
        state_key: str,
        offsets: dict,
    ) -> None:
        """Send messages, persist state, and commit offsets in a single Kafka transaction.

        The state_store.put() call sends to the changelog topic via the same
        transactional producer, so state is committed atomically with output.
        """
        async with self.producer.transaction():
            for msg in messages:
                await self.producer.send(
                    msg.topic,
                    key=encode_json(msg.key),
                    value=encode_json(msg.value),
                    timestamp_ms=datetime_to_millis(msg.timestamp),
                )
            if new_state is not None:
                await self.state_store.put(state_key, new_state)
            await self.producer.send_offsets_to_transaction(offsets, self.group_id)

        log.debug("Transaction committed: %d messages", len(messages))
