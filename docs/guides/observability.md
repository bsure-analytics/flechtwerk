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
| `message_processing_seconds` | Histogram | — | Time in a single `transform()` / `poll()` dispatch (a transformer's transaction is outside; an extractor's per-page sends and commits are inside). |
| `batch_size` | Histogram | — | Records returned by one `getmany()` call — bounded above by `max_poll_records` (default 500). *Transformer only.* |
| `batch_processing_seconds` | Histogram | — | Wall time to process a batch, including the transaction commit. *Transformer only.* |
| `transactions_committed_total` | Counter | — | Kafka transactions successfully committed — a transformer's per-task batches, an extractor's per-page commits. |
| `poll_cycle_seconds` | Histogram | — | Wall time for one poll cycle across all active configs. *Extractor only.* |

The three duration histograms (`message_processing_seconds`,
`batch_processing_seconds`, `poll_cycle_seconds`) bucket out to **10 minutes** —
the transaction timeout, and the longest a single poll page may legally run.
prometheus_client's default ladder tops out at 10 s, and `histogram_quantile`
never returns more than the largest finite bound, so a single slow source would
otherwise peg every latency quantile at a flat 10 s.

### Config Store (GlobalKTable)

Emitted by any stage that declares `config_topics`.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `config_messages_in_total` | Counter | `topic` | Records consumed from config topics into the per-process store. |
| `config_store_entries` | Gauge | — | Entries currently held (latest config per wire key) — your **"did my config arrive?"** gauge. |
| `config_store_restored_entries_total` | Counter | — | Entries surviving the startup bootstrap of the store. |
| `active_configs` | Gauge | — | Currently-active (non-suspended) configs being polled. *Extractor only.* |

### State, Tasks, and Tokens

Ownership and restore metrics: tasks for transformers (per-input-partition
work), tokens for extractors (config-partition ownership leases).

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `tasks_assigned` | Gauge | — | Tasks (input partitions) currently owned and initialized by this instance. |
| `tokens_assigned` | Gauge | — | Ownership tokens (config-partition leases) held by this extractor instance — 0 means hot standby. |
| `state_restored_entries_total` | Counter | `partition` | Changelog records replayed into the local state store on task initialization. |

### MQTT

Emitted by an [`MqttExtractor`](mqtt.md). The `topic` label is always the
**subscription filter** from config — or the `(unmatched)` sentinel on `stale`
drops — never the per-device publish topic, so cardinality stays bounded.

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `mqtt_messages_in_total` | Counter | `topic` | Messages routed into a subscription's buffer. |
| `mqtt_messages_dropped_total` | Counter | `reason`, `topic` | Messages dropped without forwarding (`reason=filtered`: relay returned `None`; `reason=poison`: relay raised; `reason=unsubscribed`: undelivered buffer disposed when a removed config's topic was unsubscribed; `reason=stale`, `topic="(unmatched)"`: post-latch arrival matching no declared filter). |
| `mqtt_buffered_messages` | Gauge | `topic` | Messages left buffered for a subscription after the last drain. |
| `mqtt_connects_total` | Counter | — | Successful MQTT (re)connects — more than one per process lifetime means session churn. |
| `mqtt_disconnects_total` | Counter | — | Unexpected disconnects (a clean shutdown is not counted). |

### Secrets

Emitted when the [`flechtwerk[secrets]`](../concepts/secrets.md) extra is in
use. The `kid` and `scope` labels are bounded (kids by keyring size, scopes are
static declarations — empty string for an unscoped attribute).

| Metric | Type | Extra labels | Meaning |
| --- | --- | --- | --- |
| `keyring_keys_loaded` | Gauge | `kid` | One series per key in the installed keyring (value 1) — makes *"has every reader got the new key?"* checkable fleet-wide before a rotation. |
| `secret_plaintext_reads_total` | Counter | `scope` | Reads of a secret value that took the legacy-plaintext branch (a `read_plaintext` attribute) — should reach zero before turning `read_plaintext` off. |
| `secret_decrypts_total` | Counter | `scope`, `kid` | Successful secret decryptions — *"decrypts under the old kid are flat"* gates a key rotation. |

## Signals Worth Watching

- **`config_store_entries`** — the fastest answer to *"did my config actually
  arrive?"* If a config you wrote to the topic isn't reflected here, the store
  never accepted it (wrong topic, tombstoned, or malformed — a bad value decodes
  to empty). See [Config topics](../concepts/config-topics.md).
- **`poll_cycle_seconds` approaching your `poll_interval`** — the extractor is
  barely keeping up. A poll cycle nearly as long as the interval is the documented
  signal to add replicas — extractors shard config ownership across instances
  automatically (see [Extractors](extractor.md#scaling-out) and
  [Architecture](../concepts/architecture.md)).
- **`tokens_assigned`** — an extractor's ownership-lease count per instance.
  The sum across instances should equal the config topics' partition count; an
  instance sitting at 0 is a hot standby.
- **`transactions_committed_total` flat while `messages_in_total` climbs** — a
  transformer is consuming but not committing: transactions are stalling or
  aborting. Read it alongside `batch_processing_seconds`.
- **`mqtt_connects_total > 1`** — session churn; each reconnect replays the
  persistent-session backlog. **`mqtt_buffered_messages` trending up** — a
  subscription drains slower than it fills (see [MQTT Extractors](mqtt.md)).
- **`mqtt_messages_dropped_total{reason="poison"}` rising** — broken payloads are
  reaching `relay`. Filtered drops are routine; poison drops warrant a look at the
  source.
- **`mqtt_messages_dropped_total{reason="unsubscribed"}` nonzero** — a config was
  removed while its publisher was still sending; the undelivered tail was dropped
  (stop the publisher first to make it zero). **`reason="stale"` rising steadily** —
  an earlier deployment's filter is still subscribed in the persistent session and
  its publisher is still active; the traffic is discarded safely, but consider a
  fresh `client_id` (a new broker session) to stop it at the source.
- **`secret_plaintext_reads_total` nonzero** — a `read_plaintext` secret is still
  being read from legacy plaintext; it must reach zero (and a topic scan come
  back clean) before turning `read_plaintext` off. **`secret_decrypts_total`
  under an old `kid` flat** confirms a key rotation's re-encryption sweep is
  complete (see [Encrypted Secrets](../concepts/secrets.md)).

## When Metrics Are Off

`metrics_port = 0` (the default) installs the no-op `Observer`: no scrape server
starts and no `prometheus_client` objects are created, so the event hooks cost
nothing. Local runs and tests need no metrics configuration at all.

## Next Steps

- **[Getting Started → Running a Stage](getting-started.md#running-a-stage)** — where `metrics_port` and `metrics_labels` are passed.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — the transactions `transactions_committed_total` counts.
- **[MQTT Extractors](mqtt.md)** — the source of the `mqtt_*` metrics.
