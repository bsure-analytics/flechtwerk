"""Transformer base class and runner for event-driven stream processing."""
from __future__ import annotations

import logging
from collections.abc import Callable
from copy import deepcopy
from typing import AsyncIterator

import aiokafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reactor_di import lookup

from .kafka import encode_json, datetime_to_millis, parse_message
from .state import StateStore
from .types import IncomingMessage, Message, State

log = logging.getLogger(__name__)

TransformFn = Callable[[IncomingMessage, State], AsyncIterator[Message | State]]
ExtractKeyFn = Callable[[IncomingMessage], str]


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

    group_id: str
    input_topics: list[str]

    def __init__(
        self,
        *,
        group_id: str | None = None,
        input_topics: list[str] | None = None,
        transform: TransformFn | None = None,
        extract_key: ExtractKeyFn | None = None,
    ):
        if group_id is not None:
            self.group_id = group_id
        if input_topics is not None:
            self.input_topics = input_topics
        if transform is not None:
            self.transform = transform
        if extract_key is not None:
            self.extract_key = extract_key

    def extract_key(self, msg: IncomingMessage) -> str:
        """Extract partition key for stateful processing. Default: message key."""
        return msg.key

    async def __aenter__(self) -> Transformer:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        """Transform an incoming message into zero or more output Messages.

        Yield a State to signal the desired state. The runner persists it only
        if it differs from the current state. If no State is yielded, nothing
        is persisted (stateless behavior). Yielding an empty/falsy State deletes
        the entry from the state store (and writes a Kafka tombstone to the
        changelog) atomically with the output messages.
        """
        raise NotImplementedError("Provide a transform function or override in a subclass")


class TransformerRunner:
    """Runs a Transformer as a Kafka consumer-producer loop with exactly-once semantics.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the ChangelogStateStore — state writes inside
    send_transactional() participate in the same Kafka transaction.
    """

    consumer: AIOKafkaConsumer
    group_id: str
    producer: AIOKafkaProducer
    state_store: StateStore
    transformer: lookup[Transformer, "stage"]

    async def run(self) -> None:
        """Main event loop. Consumes messages and processes them sequentially.

        Resource lifecycle (consumer/producer start/stop) is managed by
        FretworxModule, not the runner.
        """
        self.consumer.subscribe(self.transformer.input_topics)
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

        key = self.transformer.extract_key(msg)
        state = State(await self.state_store.get(key) or {})
        baseline = deepcopy(state)

        output: list[Message] = []
        new_state: State | None = None
        async for item in self.transformer.transform(msg, state):
            if isinstance(item, State):
                new_state = item
            elif isinstance(item, Message):
                output.append(item)
            else:
                raise TypeError(f"transform() yielded {type(item).__name__}, expected Message or State")

        # Exactly-once: produce output, persist state, and commit offset atomically
        tp = aiokafka.TopicPartition(msg.topic, msg.partition)
        changed_state = new_state if new_state is not None and new_state != baseline else None
        await self.send_transactional(output, changed_state, key, {tp: msg.offset + 1})

    async def send_transactional(
        self,
        messages: list[Message],
        new_state: State | None,
        state_key: str,
        offsets: dict,
    ) -> None:
        """Send messages, persist state, and commit offsets in a single Kafka transaction.

        The state_store.put()/delete() call sends to the changelog topic via the
        same transactional producer, so state is committed atomically with
        output. A falsy new_state deletes the entry (and writes a Kafka
        tombstone); a truthy new_state is put; None leaves state untouched.
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
                if new_state:
                    await self.state_store.put(state_key, new_state)
                else:
                    await self.state_store.delete(state_key)
            await self.producer.send_offsets_to_transaction(offsets, self.group_id)

        log.debug("Transaction committed: %d messages", len(messages))
