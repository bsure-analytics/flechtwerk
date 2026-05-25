"""Tests for the Fretworx Kafka producer cached_property.

These tests construct a real aiokafka producer (no broker needed — the
constructor's codec-library check fires before any network I/O), so they
catch missing optional dependencies that the fake-producer-based wiring
tests don't exercise.
"""
import pytest

from fretworx.extractor import Extractor
from fretworx.module import CompressionType, Fretworx


class StubExtractor(Extractor):
    input_topics = ["cfg"]

    async def poll(self, config, state):
        return
        yield  # pragma: no cover


@pytest.mark.parametrize("codec", [None, "gzip", "snappy", "lz4", "zstd"])
async def test_producer_constructs_with_configured_compression(codec: CompressionType | None):
    """AIOKafkaProducer's __init__ raises immediately if the codec library
    is not installed (e.g. RuntimeError: Compression library for zstd not
    found), so accessing the producer cached_property is enough to catch
    a missing extras dependency without any broker traffic.

    aiokafka touches the running event loop in __init__, so the test runs
    under pytest-asyncio's auto mode (event loop in scope) rather than
    constructing aiokafka synchronously.
    """
    f = Fretworx.of(
        application_id="t",
        bootstrap_servers="localhost:9092",
        client_id="t",
        poll_interval_seconds=0,
        stage=StubExtractor(),
        compression_type=codec,
    )

    assert f.producer is not None
