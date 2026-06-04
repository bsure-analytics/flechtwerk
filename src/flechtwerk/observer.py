"""Observer hooks for runners — keeps Prometheus calls out of runner code.

Runners emit a stream of events (message in/out, batch start/end,
transaction committed, poll cycle, …). The `Observer` class lets a
pluggable implementation decide what to do with those events.
"""
from contextlib import AbstractContextManager, nullcontext

from .metrics import Metrics


class Observer:
    """Hook surface the runners emit events through, AND the default no-op.

    Subclasses override the hooks they care about; bare `Observer()` is
    a usable no-op default. All `*_scope()` methods return a context
    manager whose duration is the timed event; other methods are
    point-in-time notifications.
    """

    def message_in(self, topic: str) -> None: pass
    def message_out(self, topic: str) -> None: pass
    def transaction_committed(self) -> None: pass
    def active_configs(self, n: int) -> None: pass
    def state_restored(self, partition: int, entries: int) -> None: pass
    def tasks_assigned(self, n: int) -> None: pass

    def dispatch_scope(self) -> AbstractContextManager[None]: return nullcontext()
    def batch_scope(self, size: int) -> AbstractContextManager[None]: return nullcontext()
    def poll_cycle_scope(self) -> AbstractContextManager[None]: return nullcontext()


class PrometheusObserver(Observer):
    """Splats `metrics_labels` over the `Metrics` instance once per call.

    reactor-di wires `metrics` and `metrics_labels` from `Fretworx`
    by attribute name. This is the single place the framework converts
    the caller's label dict into prometheus_client `.labels(...)` calls.
    """

    metrics: Metrics
    metrics_labels: dict[str, str]

    def message_in(self, topic: str) -> None:
        self.metrics.messages_in_total.labels(**self.metrics_labels, topic=topic).inc()

    def message_out(self, topic: str) -> None:
        self.metrics.messages_out_total.labels(**self.metrics_labels, topic=topic).inc()

    def transaction_committed(self) -> None:
        self.metrics.transactions_committed_total.labels(**self.metrics_labels).inc()

    def active_configs(self, n: int) -> None:
        self.metrics.active_configs.labels(**self.metrics_labels).set(n)

    def state_restored(self, partition: int, entries: int) -> None:
        self.metrics.state_restored_entries_total.labels(**self.metrics_labels, partition=str(partition)).inc(entries)

    def tasks_assigned(self, n: int) -> None:
        self.metrics.tasks_assigned.labels(**self.metrics_labels).set(n)

    def dispatch_scope(self) -> AbstractContextManager[None]:
        return self.metrics.message_processing_seconds.labels(**self.metrics_labels).time()

    def batch_scope(self, size: int) -> AbstractContextManager[None]:
        self.metrics.batch_size.labels(**self.metrics_labels).observe(size)
        return self.metrics.batch_processing_seconds.labels(**self.metrics_labels).time()

    def poll_cycle_scope(self) -> AbstractContextManager[None]:
        return self.metrics.poll_cycle_seconds.labels(**self.metrics_labels).time()
