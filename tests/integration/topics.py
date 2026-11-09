"""Topic creation that waits for the Kafka broker's metadata to catch up.

Under KRaft, CreateTopics returns once the controller has accepted the topics,
not once the Kafka broker's metadata cache serves them: a Metadata or
DescribeTopics request sent right after can still answer
UnknownTopicOrPartition. The framework's own startup check (``ensure_topics``)
is exactly such a request, so a test that creates topics and starts a stage
immediately would race it — and did, on cp-kafka 8.4 (Kafka 4.4).
"""
import asyncio
from typing import Any

from aiokafka.admin import NewTopic

TIMEOUT = 30.0
INTERVAL = 0.1


async def create_topics(admin: Any, new_topics: list[NewTopic]) -> None:
    """Create ``new_topics`` and return only once metadata reports them all.

    A topic counts as visible when DescribeTopics answers it without an error
    code and with every requested partition.

    Args:
        admin: An already-started AIOKafkaAdminClient.
        new_topics: The topics to create.
    """
    await admin.create_topics(new_topics)
    expected = {t.name: t.num_partitions for t in new_topics}
    deadline = asyncio.get_running_loop().time() + TIMEOUT
    while True:
        response = await admin.describe_topics(list(expected))
        visible = {t["topic"]: len(t["partitions"]) for t in response if not t["error_code"]}
        pending = sorted(name for name, n in expected.items() if visible.get(name) != n)
        if not pending:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"topics not visible after {TIMEOUT}s: {pending}")
        await asyncio.sleep(INTERVAL)
