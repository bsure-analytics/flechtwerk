# Observability — Prometheus Metrics

Every Flechtwerk runner emits a stream of **observer events** — a message
consumed, a batch committed, a poll cycle timed, a config drained. When you
enable Prometheus, those events become
[prometheus-client](https://github.com/prometheus/client_python) metrics served
on a scrape endpoint. When you don't, the same events hit a no-op observer at
essentially zero cost.

## Enabling Metrics

Metrics are off by default. Turn them on per instance through
[`Flechtwerk.of(...)`](getting-started.md#running-a-stage):

```python
await Flechtwerk.of(
    application_id="my-stage",
    bootstrap_servers="localhost:9092",
    client_id="my-stage-0",
    poll_interval_seconds=60,
    metrics_port=9000,                                # >0 starts the scrape server; 0 disables
    metrics_labels={"service": "my-stage", "env": "prod"},
    stage=stage,
).run()
```

- **`metrics_port`** — a port `> 0` starts an HTTP scrape server on
  `0.0.0.0:<port>`; scrape it at `http://<host>:<port>/metrics`. The default,
  `0`, disables Prometheus entirely (see [When Metrics Are Off](#when-metrics-are-off)).
- **`metrics_labels`** — a dict of label **name → value** stamped onto *every*
  metric. The framework owns the metric **names**; you own the **label** names,
  so this is where you attach whatever dimensions your monitoring needs — service,
  environment, tenant. Including your `client_id` here keeps each instance
  distinguishable.

!!! note "Who Owns What"

    Flechtwerk declares the metric names (all prefixed `flechtwerk_`) and their
    types; it knows nothing about your labels. Each metric's label set is *your*
    `metrics_labels` keys plus, on some metrics, a framework-owned extra (`topic`,
    `partition`, or `reason`) noted below.

## The Metric Catalog

All names are prefixed `flechtwerk_`. Every metric additionally carries your
`metrics_labels`; the **Extra labels** column lists the framework-owned labels it
adds on top.

### Throughput and Timing

Emitted by both stage shapes unless noted.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `messages_in_total` | Counter | `topic` | Input messages consumed and dispatched to user code. |
| `messages_out_total` | Counter | `topic` | Output messages yielded by user code (produced to Kafka). |
| `message_processing_seconds` | Histogram | — | Time in a single `transform()` / `poll()` dispatch (excluding Kafka I/O). |
| `batch_size` | Histogram | — | Records returned by one `getmany()` call. *Transformer only.* |
| `batch_processing_seconds` | Histogram | — | Wall time to process a batch, including the transaction commit. *Transformer only.* |
| `transactions_committed_total` | Counter | — | Kafka transactions successfully committed. *Transformer only.* |
| `poll_cycle_seconds` | Histogram | — | Wall time for one poll cycle across all active configs. *Extractor only.* |

### Config Store (GlobalKTable)

Emitted by any stage that declares `config_topics`.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `config_messages_in_total` | Counter | `topic` | Records consumed from config topics into the per-process store. |
| `config_store_entries` | Gauge | — | Entries currently held (latest config per wire key) — your **"did my config arrive?"** gauge. |
| `config_store_restored_entries_total` | Counter | — | Entries surviving the startup bootstrap of the store. |
| `active_configs` | Gauge | — | Currently-active (non-suspended) configs being polled. *Extractor only.* |

### State and Tasks

Emitted by transformers, whose work is split into per-partition tasks.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `tasks_assigned` | Gauge | — | Tasks (input partitions) currently owned and initialized by this instance. |
| `state_restored_entries_total` | Counter | `partition` | Changelog records replayed into the local state store on task initialization. |

### MQTT

Emitted by an [`MqttExtractor`](mqtt.md). The `topic` label is always the
**subscription filter** from config (bounded cardinality) — never the per-device
publish topic.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `mqtt_messages_in_total` | Counter | `topic` | Messages routed into a subscription's buffer. |
| `mqtt_messages_dropped_total` | Counter | `reason`, `topic` | Messages dropped without forwarding (`reason=filtered`: relay returned `None`; `reason=poison`: relay raised). |
| `mqtt_buffered_messages` | Gauge | `topic` | Messages left buffered for a subscription after the last drain. |
| `mqtt_connects_total` | Counter | — | Successful MQTT (re)connects — more than one per process lifetime means session churn. |
| `mqtt_disconnects_total` | Counter | — | Unexpected disconnects (a clean shutdown is not counted). |

## Signals Worth Watching

- **`config_store_entries`** — the fastest answer to *"did my config actually
  arrive?"* If a config you wrote to the topic isn't reflected here, the store
  never accepted it (wrong topic, tombstoned, or malformed — a bad value decodes
  to empty). See [Config topics](../concepts/config-topics.md).
- **`poll_cycle_seconds` approaching `poll_interval_seconds`** — the extractor is
  barely keeping up. A poll cycle nearly as long as the interval is the documented
  signal to consider config-level sharding across deployments (see
  [Extractors](extractor.md) and [Architecture](../concepts/architecture.md)).
- **`transactions_committed_total` flat while `messages_in_total` climbs** — a
  transformer is consuming but not committing: transactions are stalling or
  aborting. Read it alongside `batch_processing_seconds`.
- **`mqtt_connects_total > 1`** — session churn; each reconnect replays the
  persistent-session backlog. **`mqtt_buffered_messages` trending up** — a
  subscription drains slower than it fills, or a stale subscription is holding the
  shared inflight window (see [MQTT Extractors](mqtt.md)).
- **`mqtt_messages_dropped_total{reason="poison"}` rising** — broken payloads are
  reaching `relay`. Filtered drops are routine; poison drops warrant a look at the
  source.

## When Metrics Are Off

`metrics_port = 0` (the default) installs the no-op `Observer`: no scrape server
starts and no `prometheus_client` objects are created, so the event hooks cost
nothing. Local runs and tests need no metrics configuration at all.

## Next Steps

- **[Getting Started → Running a Stage](getting-started.md#running-a-stage)** — where `metrics_port` and `metrics_labels` are passed.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — the transactions `transactions_committed_total` counts.
- **[MQTT Extractors](mqtt.md)** — the source of the `mqtt_*` metrics.
