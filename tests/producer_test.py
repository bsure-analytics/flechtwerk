"""Tests for the Flechtwerk Kafka producer factories.

These tests construct a real aiokafka producer (no Kafka broker needed — the
constructor's codec-library check fires before any network I/O), so they
catch missing optional dependencies that the fake-producer-based wiring
tests don't exercise.
"""
import pytest

from flechtwerk.extractor import Extractor
from flechtwerk.module import CompressionType, Flechtwerk


class StubExtractor(Extractor):
    config_topics = ["cfg"]

    async def poll(self, config, state):
        return
        yield  # pragma: no cover


@pytest.mark.parametrize("codec", [None, "gzip", "snappy", "lz4", "zstd"])
async def test_producer_constructs_with_configured_compression(codec: CompressionType | None):
    """AIOKafkaProducer's __init__ raises immediately if the codec library
    is not installed (e.g. RuntimeError: Compression library for zstd not
    found), so building one producer through each factory is enough to
    catch a missing extras dependency without any Kafka broker traffic.

    aiokafka touches the running event loop in __init__, so the test runs
    under pytest-asyncio's auto mode (event loop in scope) rather than
    constructing aiokafka synchronously.
    """
    f = Flechtwerk.of(
        application_id="t",
        bootstrap_servers="localhost:9092",
        client_id="t",
        stage=StubExtractor(),
        compression_type=codec,
    )

    assert f.create_task_producer(0) is not None
    assert f.create_token_producer(0) is not None
    assert f.create_instance_producer() is not None


async def test_transactional_ids_identify_task_token_and_instance():
    """The three factories' static IDs. A task and a token ID key on the
    partition/token they fence; the instance ID of a broker-dispatched stage
    keys on the client_id and fences nothing — two replicas hold two
    distinct IDs by construction, which is why such a stage is stateless."""
    f = Flechtwerk.of(
        application_id="app",
        bootstrap_servers="localhost:9092",
        client_id="pod-0",
        stage=StubExtractor(),
    )

    assert f.create_task_producer(3)._txn_manager.transactional_id == "app-3"
    assert f.create_token_producer(3)._txn_manager.transactional_id == "app-3"
    assert f.create_instance_producer()._txn_manager.transactional_id == "app-pod-0"
