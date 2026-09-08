"""Tests for the per-port scrape endpoint in flechtwerk.metrics (`acquire_exporter` / `release_exporter`)."""
import socket
import urllib.request
from urllib.error import URLError

import pytest
from prometheus_client import CollectorRegistry

from flechtwerk.metrics import Metrics, _exporters, acquire_exporter, release_exporter
from flechtwerk.observer import PrometheusObserver


def make_metrics(registry: CollectorRegistry, labels: dict[str, str], max_poll_records: int = 500) -> Metrics:
    metrics = Metrics()
    metrics.max_poll_records = max_poll_records
    metrics.metrics_labels = labels
    metrics.registry = registry
    return metrics


def make_observer(metrics: Metrics, labels: dict[str, str]) -> PrometheusObserver:
    observer = PrometheusObserver()
    observer.metrics = metrics
    observer.metrics_labels = labels
    return observer


def scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()


def test_stages_on_one_port_share_server_and_families(free_port):
    """Two stages naming one port get ONE exporter and ONE Metrics — the first
    stage's, adopted — and their series differ by label VALUE only."""
    registry = CollectorRegistry()
    a_labels, b_labels = {"stage": "a"}, {"stage": "b"}
    a_metrics = make_metrics(registry, a_labels)
    a = acquire_exporter(free_port, a_metrics, a_labels)
    b = acquire_exporter(free_port, make_metrics(registry, b_labels), b_labels)
    assert a is b
    assert a.metrics is a_metrics
    assert a.holders == {frozenset(a_labels.items()), frozenset(b_labels.items())}

    make_observer(a.metrics, a_labels).message_in("t")
    make_observer(b.metrics, b_labels).message_in("t")
    make_observer(b.metrics, b_labels).message_in("t")
    body = scrape(free_port)
    assert 'flechtwerk_messages_in_total{stage="a",topic="t"} 1.0' in body
    assert 'flechtwerk_messages_in_total{stage="b",topic="t"} 2.0' in body


@pytest.mark.parametrize(
    ("registry_differs", "labels", "max_poll_records", "match"),
    [
        (True, {"stage": "b"}, 500, "share one CollectorRegistry"),
        (False, {"service": "b"}, 500, "same metrics_labels names"),
        (False, {"stage": "b"}, 100, "agree on max_poll_records"),
    ],
)
def test_incompatible_stage_is_rejected_at_acquire(free_port, registry_differs, labels, max_poll_records, match):
    """A stage that could not adopt the port's families fails at startup and
    leaves the stage already serving untouched."""
    registry = CollectorRegistry()
    first = acquire_exporter(free_port, make_metrics(registry, {"stage": "a"}), {"stage": "a"})
    candidate = make_metrics(CollectorRegistry() if registry_differs else registry, labels, max_poll_records)
    with pytest.raises(ValueError, match=match):
        acquire_exporter(free_port, candidate, labels)
    assert first.holders == {frozenset({("stage", "a")})}
    assert first.server is not None
    scrape(free_port)


def test_identical_labels_are_rejected(free_port):
    """Same label values would merge two stages' series — the default {} included."""
    registry = CollectorRegistry()
    acquire_exporter(free_port, make_metrics(registry, {}), {})
    with pytest.raises(ValueError, match="identical metrics_labels"):
        acquire_exporter(free_port, make_metrics(registry, {}), {})


def test_last_release_stops_server_and_next_acquire_restarts_it(free_port):
    """The server stops when the last holder leaves and restarts on the next
    acquire; the families — registration AND values — are process-lifetime."""
    registry = CollectorRegistry()
    a_labels, b_labels = {"stage": "a"}, {"stage": "b"}
    exporter = acquire_exporter(free_port, make_metrics(registry, a_labels), a_labels)
    acquire_exporter(free_port, make_metrics(registry, b_labels), b_labels)
    make_observer(exporter.metrics, a_labels).message_in("t")

    release_exporter(exporter, a_labels)
    assert exporter.server is not None  # b still holds the port
    scrape(free_port)

    release_exporter(exporter, b_labels)
    assert exporter.server is None
    with pytest.raises(URLError):
        scrape(free_port)

    again = acquire_exporter(free_port, make_metrics(registry, a_labels), a_labels)
    assert again is exporter
    assert again.metrics is exporter.metrics
    assert 'flechtwerk_messages_in_total{stage="a",topic="t"} 1.0' in scrape(free_port)


def test_port_held_by_a_foreign_process_still_crashes(free_port):
    """Only stages of THIS process may share a port; anyone else holding it is a
    deployment error — let it crash, and leave no half-made table entry."""
    with socket.socket() as sock:
        sock.bind(("0.0.0.0", free_port))
        sock.listen()
        with pytest.raises(OSError):
            acquire_exporter(free_port, make_metrics(CollectorRegistry(), {}), {})
    assert free_port not in _exporters
