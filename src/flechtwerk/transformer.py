"""Transformer base class and runner for event-driven stream processing."""
import logging
from collections.abc import Callable
from copy import deepcopy
from typing import AsyncIterator

import aiokafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from reactor_di import lookup

from .kafka import encode_json, datetime_to_millis, parse_message
from .observer import Observer
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

    Or subclassed for lifecycle management (HTTP clients, dedup instances):

        class MyTransformer(Transformer):
            async def __aenter__(self):
                self.http = httpx.AsyncClient()
                return self

            async def __aexit__(self, *exc_info):
                await self.http.aclose()

            async def transform(self, msg, state):
                ...

    The Kafka consumer group ID (driving consumer group membership,
    transactional offset commits, and changelog topic naming) is set on
    `FretworxModule` by the caller; stages don't carry it.
    """

    input_topics: list[str]

    def __init__(
            self,
            *,
            input_topics: list[str] | None = None,
            transform: TransformFn | None = None,
            extract_key: ExtractKeyFn | None = None,
    ):
        if input_topics is not None:
            self.input_topics = input_topics
        if transform is not None:
            self.transform = transform
        if extract_key is not None:
            self.extract_key = extract_key

    def extract_key(self, msg: IncomingMessage) -> str:
        """Extract the state key from the incoming message. Default: msg.key."""
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

    One Kafka transaction per ``getmany()`` batch — output messages, state
    changelog writes, and the consumer offset commit are all atomic.
    A small in-memory state overlay scoped to the batch ensures that
    records sharing a state key see each other's yielded mutations within
    the batch; only the final state per key is written to the changelog
    at commit time.

    Attributes are set by the DI container (reactor-di) or directly in tests.
    The producer is shared with the ChangelogStateStore — state writes inside
    send_transactional() participate in the same Kafka transaction.
    """

    consumer: AIOKafkaConsumer
    group_id: str
    observer: Observer
    producer: AIOKafkaProducer
    state_store: StateStore
    transformer: lookup[Transformer, "stage"]  # noqa: PyUnresolvedReferences

    async def run(self) -> None:
        """Main event loop. Consumes batches and processes each transactionally.

        Resource lifecycle (consumer/producer start/stop) is managed by
        FretworxModule, not the runner.
        """
        self.consumer.subscribe(self.transformer.input_topics)
        async with self.transformer:
            while True:
                records = await self.consumer.getmany(timeout_ms=1000)
                if not records:
                    continue
                await self.process_batch(records)

    async def process_batch(self, records: dict) -> None:
        """Process all records in a getmany batch under a single transaction.

        Records sharing a state key see each other's yielded state via an
        in-memory overlay scoped to this call. Records are walked in
        ``input_topics`` order (e.g. config before requests).
        """
        topic_order = {t: i for i, t in enumerate(self.transformer.input_topics)}
        ordered_tps = sorted(records, key=lambda p: topic_order.get(p.topic, 0))

        total = sum(len(msgs) for msgs in records.values())

        output: list[Message] = []
        baseline: dict[str, State] = {}
        overlay: dict[str, State] = {}
        yielded_keys: set[str] = set()
        offsets: dict = {}

        with self.observer.batch_scope(total):
            for tp in ordered_tps:
                for raw_msg in records[tp]:
                    msg = parse_message(raw_msg)
                    self.observer.message_in(msg.topic)
                    key = self.transformer.extract_key(msg)

                    if key not in overlay:
                        stored = State(await self.state_store.get(key) or {})
                        baseline[key] = deepcopy(stored)
                        overlay[key] = stored

                    # Defensive copy: in-place mutation without a yield must not
                    # leak into either the overlay or a later same-key record.
                    state_for_call = deepcopy(overlay[key])

                    new_state: State | None = None
                    with self.observer.dispatch_scope():
                        async for item in self.transformer.transform(msg, state_for_call):
                            if isinstance(item, State):
                                new_state = item
                            elif isinstance(item, Message):
                                output.append(item)
                                self.observer.message_out(item.topic)
                            else:
                                raise TypeError(
                                    f"transform() yielded {type(item).__name__}, expected Message or State"
                                )

                    if new_state is not None:
                        overlay[key] = new_state
                        yielded_keys.add(key)

                    tp_obj = aiokafka.TopicPartition(msg.topic, msg.partition)
                    offsets[tp_obj] = max(offsets.get(tp_obj, 0), msg.offset + 1)

            state_changes = {
                key: overlay[key]
                for key in yielded_keys
                if overlay[key] != baseline[key]
            }

            await self.send_transactional(output, state_changes, offsets)

    async def send_transactional(
            self,
            messages: list[Message],
            state_changes: dict[str, State],
            offsets: dict,
    ) -> None:
        """Send messages, persist state, and commit offsets in a single Kafka transaction.

        ``state_changes`` is the per-key final state for keys whose value
        differs from baseline (already filtered by the caller). A truthy
        value is ``put``; a falsy value is ``delete`` (writing a Kafka
        tombstone via the same transactional producer).
        """
        async with self.producer.transaction():
            for msg in messages:
                await self.producer.send(
                    msg.topic,
                    key=encode_json(msg.key),
                    value=encode_json(msg.value),
                    timestamp_ms=datetime_to_millis(msg.timestamp),
                )
            for key, new_state in state_changes.items():
                if new_state:
                    await self.state_store.put(key, new_state)
                else:
                    await self.state_store.delete(key)
            await self.producer.send_offsets_to_transaction(offsets, self.group_id)

        self.observer.transaction_committed()
        log.debug("Transaction committed: %d messages, %d state changes",
                  len(messages), len(state_changes))
