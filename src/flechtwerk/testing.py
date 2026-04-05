"""Test doubles for fretworx framework testing."""
from __future__ import annotations

from .kafka import KafkaConsumer, KafkaProducer
from .types import IncomingMessage, Message


class FakeKafkaConsumer(KafkaConsumer):
    """Test double: feeds a list of IncomingMessage objects, then returns empty."""

    def __init__(self, messages: list[IncomingMessage] | None = None):
        self.messages = list(messages or [])
        self.committed = False
        self.subscribed_topics: list[str] = []

    async def subscribe(self, topics: list[str]) -> None:
        self.subscribed_topics = topics

    async def poll(self, timeout: float = 1.0) -> list[IncomingMessage]:
        if not self.messages:
            return []
        # Return all remaining messages in one batch
        batch = self.messages
        self.messages = []
        return batch

    async def commit(self) -> None:
        self.committed = True

    async def close(self) -> None:
        pass


class FakeKafkaProducer(KafkaProducer):
    """Test double: collects sent messages for assertions."""

    def __init__(self):
        self.sent: list[Message] = []
        self.flushed = False
        self.transaction_count = 0

    async def send(self, message: Message) -> None:
        self.sent.append(message)

    async def send_batch(self, messages: list[Message]) -> None:
        self.sent.extend(messages)
        self.flushed = True

    async def send_transactional(
        self,
        messages: list[Message],
        consumer: KafkaConsumer,
    ) -> None:
        self.sent.extend(messages)
        self.transaction_count += 1
        await consumer.commit()

    async def flush(self) -> None:
        self.flushed = True

    async def close(self) -> None:
        pass
