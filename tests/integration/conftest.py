"""Session-scoped Kafka fixture for integration tests.

Uses testcontainers-python to spin up an ephemeral Kafka broker in Docker.
The broker is started once per test session and shared across all integration
tests in this directory; per-test isolation is achieved via unique topic names.

Run with:
    uv run pytest -m integration

Skipped automatically if Docker is not reachable.
"""
from __future__ import annotations

import uuid

import pytest


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def kafka_bootstrap() -> str:
    """Start a Kafka container for the whole test session.

    Returns the bootstrap server address (host:port). The broker is tuned for
    single-broker transactional tests: `__transaction_state`, `__consumer_offsets`,
    and related internal topics default to replication-factor 3, which fails with
    one broker. We override them to 1 to match the ephemeral test setup.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.kafka import KafkaContainer

    kafka = (
        KafkaContainer()
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_MIN_INSYNC_REPLICAS", "1")
    )
    with kafka:
        yield kafka.get_bootstrap_server()


@pytest.fixture
def unique_topic() -> str:
    """Per-test topic name to avoid cross-test contamination."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def unique_changelog_topic() -> str:
    """Per-test changelog topic name."""
    return f"changelog-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def unique_group_id() -> str:
    """Per-test consumer group ID."""
    return f"group-{uuid.uuid4().hex[:12]}"
