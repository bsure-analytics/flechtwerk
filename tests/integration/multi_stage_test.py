"""Integration test for several stages in one process.

Two full ``Flechtwerk.of(...)`` handles — an extractor feeding a raw topic and
a transformer refining it — run as sibling tasks under one ``TaskGroup`` on
one event loop, sharing one ``metrics_port`` and one ``Keyring``. Pins the
user-facing contract that no unit test can: the pipeline flows end to end,
the shared scrape endpoint serves both stages' series apart by label value
(process metrics once), a secret decrypted inside the transformer is counted
under the TRANSFORMER's labels, and the endpoint closes when the last stage
leaves.
"""
import asyncio
import json
import urllib.request
from datetime import timedelta
from typing import AsyncIterator
from urllib.error import URLError

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from flechtwerk.attribute import Attribute, INT, STR
from flechtwerk.extractor import Extractor
from flechtwerk.module import Flechtwerk
from flechtwerk.secrets import ENCRYPTED, encrypt_value
from flechtwerk.testing import fixture_keyring, installed_keyring
from flechtwerk.transformer import Transformer
from flechtwerk.types import Event, Message, State

pytestmark = pytest.mark.integration

COUNT = Attribute("count", INT)
NAME = Attribute("name", STR)
SECRET = Attribute("secret", ENCRYPTED(STR, scope="multi-stage"))


class _Done(Exception):
    """Raised by the watcher to end the TaskGroup once the pipeline has flowed."""


async def _create_topics(bootstrap: str, partitions: dict[str, int], compacted: tuple[str, ...] = ()) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=n,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if topic in compacted else {},
            )
            for topic, n in partitions.items()
        ])
    finally:
        await admin.close()


async def _produce(bootstrap: str, records: list[tuple[str, bytes, bytes]]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for topic, key, value in records:
            await producer.send(topic, key=key, value=value)
        await producer.flush()
    finally:
        await producer.stop()


async def _read_all(bootstrap: str, topic: str, timeout_ms: int = 1000) -> list:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
    )
    await consumer.start()
    try:
        records = []
        while True:
            batch = await consumer.getmany(timeout_ms=timeout_ms)
            if not batch:
                return records
            for msgs in batch.values():
                records.extend(msgs)
    finally:
        await consumer.stop()


def _scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def _series(body: str, family: str) -> list[str]:
    return [line for line in body.splitlines() if line.startswith(family + "{") or line.startswith(family + " ")]


class Enrich(Transformer):
    """Refines each raw record with a secret looked up in the config store."""

    def __init__(self, secret_topic: str, raw_topic: str, out_topic: str) -> None:
        self.config_topics = [secret_topic]
        self.input_topics = [raw_topic]
        self.out_topic = out_topic

    async def transform(self, msg, state) -> AsyncIterator[Message | State]:
        config = self.configs.get("k")
        assert config is not None, "the secret config is seeded before the stages start"
        yield Message(
            key=msg.key,
            topic=self.out_topic,
            value=Event.wrap({"n": msg.value.raw["n"], "secret": config[SECRET]}),
        )


async def test_two_stages_share_one_process_and_one_scrape_endpoint(
    kafka_bootstrap: str, unique_topic: str, free_port: int,
) -> None:
    cfg_topic = f"cfg-{unique_topic}"
    secret_topic = f"secret-{unique_topic}"
    raw_topic = f"raw-{unique_topic}"
    out_topic = f"out-{unique_topic}"
    await _create_topics(
        kafka_bootstrap,
        {cfg_topic: 1, secret_topic: 1, raw_topic: 1, out_topic: 1},
        compacted=(cfg_topic, secret_topic),
    )
    with installed_keyring(fixture_keyring()):
        token = encrypt_value(SECRET, "s3cr3t")
    await _produce(kafka_bootstrap, [
        (cfg_topic, b"c1", b'{"name":"c1"}'),
        (secret_topic, b"k", json.dumps({"secret": token}).encode()),
    ])

    async def poll(config, state) -> AsyncIterator[Message | State]:
        n = state.get(COUNT, 0) + 1
        yield Message(key=config[NAME], topic=raw_topic, value=Event.wrap({"n": n}))
        yield State({COUNT: n})

    def handle(stage: str, target: Extractor | Transformer) -> Flechtwerk:
        return Flechtwerk.of(
            application_id=f"{stage}-{unique_topic}",
            bootstrap_servers=kafka_bootstrap,
            client_id=f"{unique_topic}-{stage}",  # one instance, distinct per stage
            keyring=fixture_keyring(),
            metrics_labels={"stage": stage},
            metrics_port=free_port,
            poll_interval=timedelta(seconds=1),
            stage=target,
        )

    extract = handle("extract", Extractor.of(config_topics=[cfg_topic], poll=poll))
    transform = handle("transform", Enrich(secret_topic, raw_topic, out_topic))

    body = ""

    async def watch() -> None:
        nonlocal body
        deadline = asyncio.get_running_loop().time() + 90
        while not await _read_all(kafka_bootstrap, out_topic):
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("no refined record within the deadline")
        body = await asyncio.to_thread(_scrape, free_port)
        raise _Done

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(extract.run())
            tg.create_task(transform.run())
            tg.create_task(watch())
    except* _Done:
        pass

    # The pipeline flowed: the refined record carries the decrypted secret.
    records = await _read_all(kafka_bootstrap, out_topic)
    assert records and records[0].key == b"c1"
    assert b'"secret":"s3cr3t"' in records[0].value

    # One endpoint, both stages apart by label value, and the default
    # registry's platform series once (`process_*` exists only on Linux, so
    # `python_info` is the portable witness that the endpoint is the default
    # REGISTRY served a single time).
    assert any('stage="extract"' in line for line in _series(body, "flechtwerk_messages_out_total"))
    assert any('stage="transform"' in line for line in _series(body, "flechtwerk_messages_in_total"))
    assert len(_series(body, "python_info")) == 1

    # The decrypt happened inside the transformer's context, so it is counted
    # under the transformer's labels — and nowhere else.
    decrypts = _series(body, "flechtwerk_secret_decrypts_total")
    assert decrypts and all('stage="transform"' in line for line in decrypts)

    # Both run()s unwound through __aexit__: the last stage out closed the port.
    with pytest.raises(URLError):
        await asyncio.to_thread(_scrape, free_port)
