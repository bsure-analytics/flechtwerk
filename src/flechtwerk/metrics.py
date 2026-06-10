"""Prometheus metrics for the Fretworx framework.

The framework declares metric *names* and *types* here. Label names and
values are caller-provided via `metrics_labels` — Fretworx itself doesn't
know what they're called, which keeps it reusable beyond this repo.
"""
from functools import cached_property

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    """Lazy registry for the framework's metric set.

    reactor-di wires `metrics_labels` and `registry` from `Fretworx`
    by attribute name. Each metric is a `cached_property` that builds its
    prometheus_client object on first access, taking
    `list(self.metrics_labels.keys()) + per_metric_extras` as `labelnames`.
    """

    metrics_labels: dict[str, str]
    registry: CollectorRegistry

    @cached_property
    def _label_names(self) -> list[str]:
        return list(self.metrics_labels.keys())

    @cached_property
    def messages_in_total(self) -> Counter:
        return Counter(
            "fretworx_messages_in_total",
            "Input messages consumed and dispatched to user code",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def messages_out_total(self) -> Counter:
        return Counter(
            "fretworx_messages_out_total",
            "Output messages yielded by user code (i.e. produced to Kafka)",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def message_processing_seconds(self) -> Histogram:
        return Histogram(
            "fretworx_message_processing_seconds",
            "Time spent in a single transform()/poll() dispatch (excluding Kafka I/O)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def batch_size(self) -> Histogram:
        return Histogram(
            "fretworx_batch_size",
            "Records returned by a single getmany() call",
            self._label_names,
            registry=self.registry,
            buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
        )

    @cached_property
    def batch_processing_seconds(self) -> Histogram:
        return Histogram(
            "fretworx_batch_processing_seconds",
            "Wall time to fully process a batch (incl. Kafka transaction commit)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def transactions_committed_total(self) -> Counter:
        return Counter(
            "fretworx_transactions_committed_total",
            "Kafka transactions successfully committed",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def active_configs(self) -> Gauge:
        return Gauge(
            "fretworx_active_configs",
            "Currently-active (non-suspended) configs being polled",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def poll_cycle_seconds(self) -> Histogram:
        return Histogram(
            "fretworx_poll_cycle_seconds",
            "Wall time for one poll cycle across all active configs",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def config_messages_in_total(self) -> Counter:
        return Counter(
            "fretworx_config_messages_in_total",
            "Records consumed from config topics into the per-process config store",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def config_store_entries(self) -> Gauge:
        return Gauge(
            "fretworx_config_store_entries",
            "Entries currently held in the config store (latest config per wire key)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def config_store_restored_entries_total(self) -> Counter:
        return Counter(
            "fretworx_config_store_restored_entries_total",
            "Entries surviving the startup bootstrap of the config store",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def state_restored_entries_total(self) -> Counter:
        return Counter(
            "fretworx_state_restored_entries_total",
            "Changelog records replayed into the local state store on task initialization",
            self._label_names + ["partition"],
            registry=self.registry,
        )

    @cached_property
    def tasks_assigned(self) -> Gauge:
        return Gauge(
            "fretworx_tasks_assigned",
            "Tasks (input partitions) currently owned and initialized by this instance",
            self._label_names,
            registry=self.registry,
        )
