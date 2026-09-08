"""Shared pytest fixtures."""
import socket

import pytest
from prometheus_client.metrics import MetricWrapperBase

from flechtwerk.keyring import _override_secret_runtime, _restore_secret_runtime
from flechtwerk.metrics import _exporters


@pytest.fixture(autouse=True)
def _clean_secret_runtime():
    """Isolate the secret runtime (process-global keyring, context-bound observer) per test.

    The keyring installs into module-global state and the observer binds into
    the current context; this saves both before each test, resets them to
    empty, and restores them after — so an install in one test cannot leak
    into another. A no-op for tests that never touch secrets.
    """
    previous = _override_secret_runtime(None, None)
    yield
    _restore_secret_runtime(previous)


@pytest.fixture(autouse=True)
def _clean_exporters():
    """Tear down any scrape endpoint a test left behind, and its table entry.

    The exporter table is process-level by design (a port is a process
    resource), so a test that acquires one must not leak its server — or its
    adopted `Metrics` — into the next test's acquire on the same port. The
    adopted families are unregistered too: a test on the default REGISTRY
    would otherwise leave collectors that trip the next `Metrics` there.
    """
    yield
    for exporter in _exporters.values():
        if exporter.server is not None:
            exporter.stop()
        metrics = exporter.metrics
        for collector in list(metrics.__dict__.values()):
            if isinstance(collector, MetricWrapperBase):
                metrics.registry.unregister(collector)
    _exporters.clear()


@pytest.fixture
def free_port() -> int:
    """A TCP port nothing listens on right now (bound and released on 127.0.0.1)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
