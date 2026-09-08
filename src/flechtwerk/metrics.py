"""Prometheus metrics for the Flechtwerk framework.

The framework declares metric *names* and *types* here. Label names and
values are caller-provided via `metrics_labels` — Flechtwerk itself doesn't
know what they're called, which keeps it application-agnostic.
"""
from functools import cached_property
from itertools import count, takewhile
from threading import Thread
from typing import Final
from wsgiref.simple_server import WSGIServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

# Framework-internal on purpose: `PrometheusObserver` is the only consumer;
# the application-facing surface is `metrics_port` / `metrics_labels`.
__all__: list[str] = []

# prometheus_client's default buckets top out at 10 s, and histogram_quantile
# never returns more than the largest finite bound — one slow source and every
# latency panel pins at a flat "10 s". Extend the ladder to the transaction
# timeout (10 minutes), the longest a single poll page may legally run.
_DURATION_BUCKETS: Final = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
    1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
)

# One ladder for every byte histogram: a state changelog record and a produced
# message face the SAME ceiling — aiokafka's `max_request_size` and the Kafka broker's
# `max.message.bytes`, both 1 MiB by default — so they deserve the same
# boundaries and directly comparable panels.
#
# The actionable signal is the TOP of the distribution approaching that ceiling,
# not the median, which is why the ladder is fine at the bottom (typical
# messages run hundreds of bytes), coarse through the middle, and dense from
# 256 KiB up: the crash this metric exists to predict died at 1 014 623 bytes —
# 97 % of the ceiling — and a ladder that jumped 512 KiB → 1 MiB would have
# shown that only as "somewhere in the last decade". A boundary sits exactly on
# 1 048 576 so "did any record cross the default ceiling?" is one bucket
# subtraction, and two beyond it serve deployments that raised their limits.
#
# Same histogram_quantile caveat as _DURATION_BUCKETS: the quantile never
# exceeds the largest finite bound, so a raised-limit deployment reading
# quantiles (rather than the paired `*_max_bytes` gauge) will see them pin at
# 4 MiB.
_RECORD_BYTE_BUCKETS: Final = (
    256, 1_024, 4_096, 16_384, 65_536, 131_072, 262_144, 393_216, 524_288,
    655_360, 786_432, 917_504, 1_048_576, 2_097_152, 4_194_304,
)


def _batch_size_buckets(max_poll_records: int) -> tuple[int, ...]:
    """Bucket ladder for `batch_size`, derived from the `getmany()` cap.

    The top boundary must be the cap itself: histogram_quantile never returns
    more than the largest finite bound, so a ladder topping out below the cap
    pins saturated panels flat (the _DURATION_BUCKETS problem), while any
    boundary above it can never receive a sample. The extra ``cap - 1``
    boundary isolates batches at exactly the cap — the consumer-falling-behind
    signal — as a single bucket subtraction in PromQL.
    """
    # 1-2.5-5 per decade; the floor division makes decade zero 1, 2, 5.
    ladder = (m * 10**e // 10 for e in count() for m in (10, 25, 50))
    below = takewhile(lambda step: step < max_poll_records - 1, ladder)
    return *below, max_poll_records - 1, max_poll_records


class Metrics:
    """Lazy registry for the framework's metric set.

    reactor-di wires `max_poll_records`, `metrics_labels`, and `registry`
    from `Flechtwerk` by attribute name. Each metric is a `cached_property`
    that builds its prometheus_client object on first access, taking
    `list(self.metrics_labels.keys()) + per_metric_extras` as `labelnames`.

    One instance serves every stage on a scrape port (`acquire_exporter`
    below adopts the first stage's); each stage's `PrometheusObserver` splats
    its own `metrics_labels` VALUES over the shared families.
    """

    max_poll_records: int
    metrics_labels: dict[str, str]
    registry: CollectorRegistry

    @cached_property
    def _label_names(self) -> list[str]:
        return list(self.metrics_labels.keys())

    @cached_property
    def messages_in_total(self) -> Counter:
        return Counter(
            "flechtwerk_messages_in_total",
            "Input messages consumed and dispatched to user code",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    # `outcome` is bounded to three values and `topic` to the stage's declared
    # topics. The "raised" increment rarely survives to a scrape — the process
    # is about to die — so restart counts and the traceback carry that case;
    # it is counted anyway, so the three outcomes read symmetrically in PromQL
    # and a handler that raises *selectively* (per topic, say) is visible.
    @cached_property
    def messages_invalid_total(self) -> Counter:
        return Counter(
            "flechtwerk_messages_invalid_total",
            "Records whose key or value could not be decoded, by what on_invalid_message did with them",
            self._label_names + ["outcome", "topic"],
            registry=self.registry,
        )

    @cached_property
    def messages_out_total(self) -> Counter:
        return Counter(
            "flechtwerk_messages_out_total",
            "Output messages yielded by user code (i.e. produced to Kafka)",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    # Byte twins of the two counters above, plus their high-water marks. A
    # histogram's buckets detect a ceiling crossing exactly but cannot report
    # the actual maximum ("between 917 504 and 1 048 576" when the operator
    # wants "1 014 623 = 97 % of the ceiling"), and a last-value gauge would
    # lose every peak between scrapes — hence a RUNNING max, which is
    # scrape-timing-proof. It is the largest observation since process start
    # and resets on restart; state buckets are rewritten whole on every commit,
    # so the mark re-establishes itself within minutes.

    @cached_property
    def message_in_bytes(self) -> Histogram:
        return Histogram(
            "flechtwerk_message_in_bytes",
            "Serialized size (key + value) of one consumed record (bytes)",
            self._label_names + ["topic"],
            registry=self.registry,
            buckets=_RECORD_BYTE_BUCKETS,
        )

    @cached_property
    def message_in_max_bytes(self) -> Gauge:
        return Gauge(
            "flechtwerk_message_in_max_bytes",
            "Largest consumed record since process start (high-water mark, bytes)",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def message_out_bytes(self) -> Histogram:
        return Histogram(
            "flechtwerk_message_out_bytes",
            "Serialized size (key + value) of one produced record (bytes)",
            self._label_names + ["topic"],
            registry=self.registry,
            buckets=_RECORD_BYTE_BUCKETS,
        )

    @cached_property
    def message_out_max_bytes(self) -> Gauge:
        return Gauge(
            "flechtwerk_message_out_max_bytes",
            "Largest produced record since process start (high-water mark, bytes)",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def message_processing_seconds(self) -> Histogram:
        return Histogram(
            "flechtwerk_message_processing_seconds",
            "Time spent in a single transform()/poll() dispatch (a transformer's transaction is outside; an extractor's per-page sends and commits are inside)",
            self._label_names,
            registry=self.registry,
            buckets=_DURATION_BUCKETS,
        )

    @cached_property
    def batch_size(self) -> Histogram:
        return Histogram(
            "flechtwerk_batch_size",
            "Records returned by a single getmany() call (capped at max_poll_records)",
            self._label_names,
            registry=self.registry,
            buckets=_batch_size_buckets(self.max_poll_records),
        )

    @cached_property
    def batch_processing_seconds(self) -> Histogram:
        return Histogram(
            "flechtwerk_batch_processing_seconds",
            "Wall time to fully process a batch (incl. Kafka transaction commit)",
            self._label_names,
            registry=self.registry,
            buckets=_DURATION_BUCKETS,
        )

    @cached_property
    def transactions_committed_total(self) -> Counter:
        return Counter(
            "flechtwerk_transactions_committed_total",
            "Kafka transactions successfully committed",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def active_configs(self) -> Gauge:
        return Gauge(
            "flechtwerk_active_configs",
            "Currently-active (non-suspended) configs being polled",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def poll_cycle_seconds(self) -> Histogram:
        return Histogram(
            "flechtwerk_poll_cycle_seconds",
            "Wall time for one poll cycle across all active configs",
            self._label_names,
            registry=self.registry,
            buckets=_DURATION_BUCKETS,
        )

    @cached_property
    def config_messages_in_total(self) -> Counter:
        return Counter(
            "flechtwerk_config_messages_in_total",
            "Records consumed from config topics into the per-process config store",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def config_store_bytes(self) -> Gauge:
        return Gauge(
            "flechtwerk_config_store_bytes",
            "Wire size of the config store (UTF-8 keys plus encoded values) — the store lives "
            "in RAM on every instance, so this is the gauge for its size contract",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def config_store_entries(self) -> Gauge:
        return Gauge(
            "flechtwerk_config_store_entries",
            "Entries currently held in the config store (latest config per wire key)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def config_store_restored_entries_total(self) -> Counter:
        return Counter(
            "flechtwerk_config_store_restored_entries_total",
            "Entries surviving the startup bootstrap of the config store",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def state_restored_entries_total(self) -> Counter:
        return Counter(
            "flechtwerk_state_restored_entries_total",
            "Changelog records replayed into the local state store on task initialization",
            self._label_names + ["partition"],
            registry=self.registry,
        )

    # Deliberately unlabelled beyond the caller's own: a state key is unbounded
    # cardinality (one examples-repo scenario runs 343 keys) and per-partition
    # series would multiply for no operational gain — the question is "is ANY
    # key approaching the ceiling?", and the paired gauge answers it.

    @cached_property
    def state_record_bytes(self) -> Histogram:
        return Histogram(
            "flechtwerk_state_record_bytes",
            "Serialized size of one state changelog record, observed at every write (bytes)",
            self._label_names,
            registry=self.registry,
            buckets=_RECORD_BYTE_BUCKETS,
        )

    @cached_property
    def state_record_max_bytes(self) -> Gauge:
        return Gauge(
            "flechtwerk_state_record_max_bytes",
            "Largest state changelog record since process start (high-water mark, bytes)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def tasks_assigned(self) -> Gauge:
        return Gauge(
            "flechtwerk_tasks_assigned",
            "Tasks (input partitions) currently owned and initialized by this instance",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def tokens_assigned(self) -> Gauge:
        return Gauge(
            "flechtwerk_tokens_assigned",
            "Ownership tokens (config-partition leases) currently held by this extractor instance — 0 means hot standby",
            self._label_names,
            registry=self.registry,
        )

    # Secret / keyring metrics (flechtwerk.secrets). `kid` and `scope` labels
    # are bounded: kids by keyring size, scopes are static declarations (empty
    # string for an unscoped attribute).

    @cached_property
    def keyring_keys_loaded(self) -> Gauge:
        return Gauge(
            "flechtwerk_keyring_keys_loaded",
            "Keys present in the installed keyring (1 per kid) — makes 'every reader has the new key' checkable fleet-wide",
            self._label_names + ["kid"],
            registry=self.registry,
        )

    @cached_property
    def secret_plaintext_reads_total(self) -> Counter:
        return Counter(
            "flechtwerk_secret_plaintext_reads_total",
            "Reads of a secret value that took the legacy-plaintext branch — should reach zero before ending the migration",
            self._label_names + ["scope"],
            registry=self.registry,
        )

    @cached_property
    def secret_decrypts_total(self) -> Counter:
        return Counter(
            "flechtwerk_secret_decrypts_total",
            "Successful secret decryptions, by scope and kid — 'decrypts under the old kid are flat' gates a rotation",
            self._label_names + ["scope", "kid"],
            registry=self.registry,
        )

    # MQTT metrics — the `topic` label carries the subscription filter from
    # config (bounded cardinality), never the per-device publish topic.

    @cached_property
    def mqtt_buffered_messages(self) -> Gauge:
        return Gauge(
            "flechtwerk_mqtt_buffered_messages",
            "MQTT messages left buffered for a subscription after the last drain",
            self._label_names + ["topic"],
            registry=self.registry,
        )

    @cached_property
    def mqtt_connects_total(self) -> Counter:
        return Counter(
            "flechtwerk_mqtt_connects_total",
            "Successful MQTT (re)connects — more than one per process lifetime means session churn",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def mqtt_disconnects_total(self) -> Counter:
        return Counter(
            "flechtwerk_mqtt_disconnects_total",
            "Unexpected MQTT disconnects (clean shutdown is not counted)",
            self._label_names,
            registry=self.registry,
        )

    @cached_property
    def mqtt_messages_dropped_total(self) -> Counter:
        return Counter(
            "flechtwerk_mqtt_messages_dropped_total",
            "MQTT messages dropped without forwarding (filtered: relay returned None; poison: relay raised)",
            self._label_names + ["reason", "topic"],
            registry=self.registry,
        )

    @cached_property
    def mqtt_messages_in_total(self) -> Counter:
        return Counter(
            "flechtwerk_mqtt_messages_in_total",
            "MQTT messages routed into a subscription's buffer",
            self._label_names + ["topic"],
            registry=self.registry,
        )


# --- one scrape endpoint per port ---
#
# A TCP port is a process resource, and so is the CollectorRegistry served on
# it: prometheus_client refuses a second collector under a name the registry
# already holds. So the scrape endpoint is reached through this table, keyed by
# port. The first stage to name a port starts its server and lends it its
# `Metrics` — those become the port's metric families — and every later stage
# on that port adopts them, distinguished by its `metrics_labels` VALUES (the
# Kafka Streams idiom: one JVM, many instances, a `client-id` tag apart).
# Process-level state is legitimate here for the same reason it is for the
# keyring: the resource it guards is process-scoped by nature, not by choice.

_LabelSet = frozenset[tuple[str, str]]

_exporters: dict[int, "Exporter"] = {}


class Exporter:
    """The scrape endpoint on one port, shared by every stage that names it.

    `metrics` is adopted from the first stage on the port and outlives any one
    stage: a counter is a process-lifetime quantity, so when the last holder
    leaves only the server stops — the families keep their values and their
    registration (which is what lets a later stage on this port join without
    tripping `Duplicated timeseries`), and the server restarts on the next
    acquire.
    """

    def __init__(self, port: int, metrics: Metrics) -> None:
        self.holders: set[_LabelSet] = set()
        self.metrics = metrics
        self.port = port
        self.server: WSGIServer | None = None
        self.thread: Thread | None = None

    def start(self) -> None:
        self.server, self.thread = start_http_server(
            port=self.port, addr="0.0.0.0", registry=self.metrics.registry,
        )

    def stop(self) -> None:
        """Stop serving AND release the port — `shutdown()` alone keeps the socket bound."""
        assert self.server is not None and self.thread is not None
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.server = self.thread = None

    def check_compatible(self, candidate: Metrics) -> None:
        """A later stage's `Metrics` must be able to adopt this port's families."""
        adopted = self.metrics
        if candidate.registry is not adopted.registry:
            raise ValueError(f"stages sharing metrics port {self.port} must share one CollectorRegistry")
        if set(candidate.metrics_labels) != set(adopted.metrics_labels):
            raise ValueError(
                f"stages sharing metrics port {self.port} must declare the same metrics_labels names — "
                f"a Prometheus metric family has one label set: "
                f"{sorted(adopted.metrics_labels)} vs {sorted(candidate.metrics_labels)}"
            )
        if candidate.max_poll_records != adopted.max_poll_records:
            raise ValueError(
                f"stages sharing metrics port {self.port} must agree on max_poll_records — "
                f"the batch_size bucket ladder derives from it: "
                f"{adopted.max_poll_records} vs {candidate.max_poll_records}"
            )


def acquire_exporter(port: int, metrics: Metrics, metrics_labels: dict[str, str]) -> Exporter:
    """Join the scrape endpoint on `port`, starting its server if nobody serves it.

    `metrics` is the calling stage's own, still-unregistered `Metrics`: the
    first stage's is adopted as the port's families; a later stage's is read
    for compatibility only — `registry`, `metrics_labels`, `max_poll_records`
    — and must never have a metric property touched, since a second set of
    collectors under the same names is exactly the `Duplicated timeseries`
    failure this table exists to prevent. Stages on one port must agree on
    the registry, the label NAMES (a family has one label set) and
    `max_poll_records` (the `batch_size` ladder derives from it), and must
    DIFFER in label values, or their series would merge indistinguishably.
    Each violation is a `ValueError` at startup, before any metric exists. A
    port held by a foreign process still raises `OSError` — let it crash so
    the orchestrator surfaces it.
    """
    holder: _LabelSet = frozenset(metrics_labels.items())
    exporter = _exporters.get(port)
    if exporter is None:
        exporter = Exporter(port, metrics)
        exporter.start()
        _exporters[port] = exporter
    else:
        exporter.check_compatible(metrics)
        if holder in exporter.holders:
            raise ValueError(
                f"two stages on metrics port {port} carry identical metrics_labels "
                f"{dict(sorted(holder))}; their series would merge — give each stage a "
                f"distinguishing value, e.g. metrics_labels={{..., 'stage': '<name>'}}"
            )
        if exporter.server is None:
            exporter.start()
    exporter.holders.add(holder)
    return exporter


def release_exporter(exporter: Exporter, metrics_labels: dict[str, str]) -> None:
    """Leave the endpoint; the last stage out stops the server (the table entry stays — see `Exporter`)."""
    exporter.holders.discard(frozenset(metrics_labels.items()))
    if not exporter.holders and exporter.server is not None:
        exporter.stop()
