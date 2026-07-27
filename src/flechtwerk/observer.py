"""Observer hooks for runners — keeps Prometheus calls out of runner code.

Runners emit a stream of events (message in/out, batch start/end,
transaction committed, poll cycle, …). The `Observer` class lets a
pluggable implementation decide what to do with those events.
"""
from contextlib import AbstractContextManager, nullcontext
from functools import cached_property

from .metrics import Metrics

# `Observer` is the extension surface (subclassed by `RecordingObserver` in
# `testing`, or wired in by a parent reactor-di module); `PrometheusObserver`
# is internal — the DI container selects it via `metrics_port`.
__all__ = ["Observer"]


class Observer:
    """Hook surface the runners emit events through, AND the default no-op.

    Subclasses override the hooks they care about; bare `Observer()` is
    a usable no-op default. All `*_scope()` methods return a context
    manager whose duration is the timed event; other methods are
    point-in-time notifications.
    """

    # The `*_bytes` hooks are separate from their counting siblings rather than
    # widened signatures on them: `RecordingObserver` tuples are asserted by
    # shape here and downstream, so widening would break test helpers for no
    # gain. `state_record_bytes` carries no key and no partition — see
    # `Metrics.state_record_bytes` for why.
    def message_in(self, topic: str) -> None: pass
    def message_in_bytes(self, topic: str, n: int) -> None: pass
    # One call per `Stage.on_invalid_message` invocation, `outcome` being
    # "raised" / "skipped" / "substituted" — mandatory, so an override that
    # skips records cannot silence the telemetry. The framework logs nothing
    # about invalid records on any outcome; this counter IS the announcement
    # for the recovered ones.
    def message_invalid(self, topic: str, outcome: str) -> None: pass
    def message_out(self, topic: str) -> None: pass
    def message_out_bytes(self, topic: str, n: int) -> None: pass
    def transaction_committed(self) -> None: pass
    def active_configs(self, n: int) -> None: pass
    def config_message_in(self, topic: str) -> None: pass
    def config_store_entries(self, n: int) -> None: pass
    def config_store_restored(self, entries: int) -> None: pass
    def state_restored(self, partition: int, entries: int) -> None: pass
    def state_record_bytes(self, n: int) -> None: pass
    def tasks_assigned(self, n: int) -> None: pass
    def tokens_assigned(self, n: int) -> None: pass

    # Secret / keyring events (flechtwerk.secrets). `keyring_key_loaded` fires
    # once per key at startup; the other two fire from the `ENCRYPTED` codec
    # deep in a lazy config read, through the process-global observer installed
    # alongside the keyring.
    def keyring_key_loaded(self, kid: str) -> None: pass
    def secret_plaintext_read(self, scope: str) -> None: pass
    def secret_decrypted(self, scope: str, kid: str) -> None: pass

    # MQTT events — `topic` is always the subscription filter from config,
    # or the "(unmatched)" sentinel on `stale` drops (`flechtwerk.mqtt.
    # UNMATCHED`) — never the per-device publish topic, whose cardinality
    # is unbounded.
    def mqtt_buffered(self, topic: str, n: int) -> None: pass
    def mqtt_connected(self) -> None: pass
    def mqtt_disconnected(self) -> None: pass
    def mqtt_message_dropped(self, topic: str, reason: str) -> None: pass
    def mqtt_message_in(self, topic: str) -> None: pass

    def dispatch_scope(self) -> AbstractContextManager[None]: return nullcontext()
    def batch_scope(self, size: int) -> AbstractContextManager[None]: return nullcontext()
    def poll_cycle_scope(self) -> AbstractContextManager[None]: return nullcontext()


class PrometheusObserver(Observer):
    """Splats `metrics_labels` over the `Metrics` instance once per call.

    reactor-di wires `metrics` and `metrics_labels` from `Flechtwerk`
    by attribute name. This is the single place the framework converts
    the caller's label dict into prometheus_client `.labels(...)` calls.
    """

    metrics: Metrics
    metrics_labels: dict[str, str]

    # High-water-mark trackers for the byte histograms' paired `*_max_bytes`
    # gauges. Deliberately unannotated / lazy: reactor-di wires this class's
    # annotated fields by name, and a mutable class-level default would be
    # shared across instances. The int rebinds into the instance on first
    # write; a single event loop makes read-then-set race-free.
    _state_record_max = 0

    @cached_property
    def _message_in_max(self) -> dict[str, int]:
        return {}

    @cached_property
    def _message_out_max(self) -> dict[str, int]:
        return {}

    def message_in(self, topic: str) -> None:
        self.metrics.messages_in_total.labels(**self.metrics_labels, topic=topic).inc()

    def message_in_bytes(self, topic: str, n: int) -> None:
        self.metrics.message_in_bytes.labels(**self.metrics_labels, topic=topic).observe(n)
        if n > self._message_in_max.get(topic, 0):
            self._message_in_max[topic] = n
            self.metrics.message_in_max_bytes.labels(**self.metrics_labels, topic=topic).set(n)

    def message_invalid(self, topic: str, outcome: str) -> None:
        self.metrics.messages_invalid_total.labels(
            **self.metrics_labels, outcome=outcome, topic=topic,
        ).inc()

    def message_out(self, topic: str) -> None:
        self.metrics.messages_out_total.labels(**self.metrics_labels, topic=topic).inc()

    def message_out_bytes(self, topic: str, n: int) -> None:
        self.metrics.message_out_bytes.labels(**self.metrics_labels, topic=topic).observe(n)
        if n > self._message_out_max.get(topic, 0):
            self._message_out_max[topic] = n
            self.metrics.message_out_max_bytes.labels(**self.metrics_labels, topic=topic).set(n)

    def transaction_committed(self) -> None:
        self.metrics.transactions_committed_total.labels(**self.metrics_labels).inc()

    def active_configs(self, n: int) -> None:
        self.metrics.active_configs.labels(**self.metrics_labels).set(n)

    def config_message_in(self, topic: str) -> None:
        self.metrics.config_messages_in_total.labels(**self.metrics_labels, topic=topic).inc()

    def config_store_entries(self, n: int) -> None:
        self.metrics.config_store_entries.labels(**self.metrics_labels).set(n)

    def config_store_restored(self, entries: int) -> None:
        self.metrics.config_store_restored_entries_total.labels(**self.metrics_labels).inc(entries)

    def state_restored(self, partition: int, entries: int) -> None:
        self.metrics.state_restored_entries_total.labels(**self.metrics_labels, partition=str(partition)).inc(entries)

    def state_record_bytes(self, n: int) -> None:
        self.metrics.state_record_bytes.labels(**self.metrics_labels).observe(n)
        if n > self._state_record_max:
            self._state_record_max = n
            self.metrics.state_record_max_bytes.labels(**self.metrics_labels).set(n)

    def tasks_assigned(self, n: int) -> None:
        self.metrics.tasks_assigned.labels(**self.metrics_labels).set(n)

    def tokens_assigned(self, n: int) -> None:
        self.metrics.tokens_assigned.labels(**self.metrics_labels).set(n)

    def keyring_key_loaded(self, kid: str) -> None:
        self.metrics.keyring_keys_loaded.labels(**self.metrics_labels, kid=kid).set(1)

    def secret_plaintext_read(self, scope: str) -> None:
        self.metrics.secret_plaintext_reads_total.labels(**self.metrics_labels, scope=scope).inc()

    def secret_decrypted(self, scope: str, kid: str) -> None:
        self.metrics.secret_decrypts_total.labels(**self.metrics_labels, scope=scope, kid=kid).inc()

    def mqtt_buffered(self, topic: str, n: int) -> None:
        self.metrics.mqtt_buffered_messages.labels(**self.metrics_labels, topic=topic).set(n)

    def mqtt_connected(self) -> None:
        self.metrics.mqtt_connects_total.labels(**self.metrics_labels).inc()

    def mqtt_disconnected(self) -> None:
        self.metrics.mqtt_disconnects_total.labels(**self.metrics_labels).inc()

    def mqtt_message_dropped(self, topic: str, reason: str) -> None:
        self.metrics.mqtt_messages_dropped_total.labels(**self.metrics_labels, reason=reason, topic=topic).inc()

    def mqtt_message_in(self, topic: str) -> None:
        self.metrics.mqtt_messages_in_total.labels(**self.metrics_labels, topic=topic).inc()

    def dispatch_scope(self) -> AbstractContextManager[None]:
        return self.metrics.message_processing_seconds.labels(**self.metrics_labels).time()

    def batch_scope(self, size: int) -> AbstractContextManager[None]:
        self.metrics.batch_size.labels(**self.metrics_labels).observe(size)
        return self.metrics.batch_processing_seconds.labels(**self.metrics_labels).time()

    def poll_cycle_scope(self) -> AbstractContextManager[None]:
        return self.metrics.poll_cycle_seconds.labels(**self.metrics_labels).time()
