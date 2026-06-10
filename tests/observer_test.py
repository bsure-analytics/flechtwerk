"""Tests for fretworx.observer.PrometheusObserver."""
from prometheus_client import CollectorRegistry

from fretworx.metrics import Metrics
from fretworx.observer import Observer, PrometheusObserver


def make_observer() -> tuple[PrometheusObserver, CollectorRegistry]:
    registry = CollectorRegistry()
    metrics = Metrics()
    metrics.metrics_labels = {"datasource": "ds1", "stage": "extractor"}
    metrics.registry = registry

    observer = PrometheusObserver()
    observer.metrics = metrics
    observer.metrics_labels = {"datasource": "ds1", "stage": "extractor"}
    return observer, registry


def test_no_op_observer_does_nothing():
    observer = Observer()
    observer.message_in("t")
    observer.message_out("t")
    observer.transaction_committed()
    observer.active_configs(3)
    with observer.dispatch_scope():
        pass
    with observer.batch_scope(5):
        pass
    with observer.poll_cycle_scope():
        pass


def test_message_in_increments_counter():
    observer, registry = make_observer()
    observer.message_in("topic-a")
    observer.message_in("topic-a")
    observer.message_in("topic-b")
    assert registry.get_sample_value(
        "fretworx_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "topic-a"},
    ) == 2
    assert registry.get_sample_value(
        "fretworx_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "topic-b"},
    ) == 1


def test_message_out_increments_counter():
    observer, registry = make_observer()
    observer.message_out("out-topic")
    assert registry.get_sample_value(
        "fretworx_messages_out_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "out-topic"},
    ) == 1


def test_transaction_committed_increments_counter():
    observer, registry = make_observer()
    observer.transaction_committed()
    observer.transaction_committed()
    assert registry.get_sample_value(
        "fretworx_transactions_committed_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 2


def test_active_configs_sets_gauge():
    observer, registry = make_observer()
    observer.active_configs(7)
    assert registry.get_sample_value(
        "fretworx_active_configs",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 7
    observer.active_configs(3)
    assert registry.get_sample_value(
        "fretworx_active_configs",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 3


def test_config_message_in_increments_counter():
    observer, registry = make_observer()
    observer.config_message_in("cfg")
    observer.config_message_in("cfg")
    assert registry.get_sample_value(
        "fretworx_config_messages_in_total",
        {"datasource": "ds1", "stage": "extractor", "topic": "cfg"},
    ) == 2


def test_config_store_entries_sets_gauge():
    observer, registry = make_observer()
    observer.config_store_entries(9)
    observer.config_store_entries(4)
    assert registry.get_sample_value(
        "fretworx_config_store_entries",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 4


def test_config_store_restored_increments_counter_by_entries():
    observer, registry = make_observer()
    observer.config_store_restored(5)
    assert registry.get_sample_value(
        "fretworx_config_store_restored_entries_total",
        {"datasource": "ds1", "stage": "extractor"},
    ) == 5


def test_dispatch_scope_records_histogram():
    observer, registry = make_observer()
    with observer.dispatch_scope():
        pass
    count = registry.get_sample_value(
        "fretworx_message_processing_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert count == 1


def test_batch_scope_records_size_and_duration():
    observer, registry = make_observer()
    with observer.batch_scope(42):
        pass
    size_count = registry.get_sample_value(
        "fretworx_batch_size_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    duration_count = registry.get_sample_value(
        "fretworx_batch_processing_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert size_count == 1
    assert duration_count == 1


def test_poll_cycle_scope_records_histogram():
    observer, registry = make_observer()
    with observer.poll_cycle_scope():
        pass
    count = registry.get_sample_value(
        "fretworx_poll_cycle_seconds_count",
        {"datasource": "ds1", "stage": "extractor"},
    )
    assert count == 1
