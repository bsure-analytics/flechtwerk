# Architecture

Flechtwerk is built as a **hexagonal architecture** — ports and adapters. The core is a pure async stream-processing engine that depends only on abstract ports; every concrete piece of infrastructure plugs in from the outside as an adapter, wired through dependency injection.

## Ports and Adapters

The core (`Extractor`, `Transformer`, and their runners) depends only on abstract ports: `StateStore`, `Observer`, and the `aiokafka` consumer/producer interfaces. It knows nothing about RocksDB, Prometheus, MQTT, or where configuration comes from.

Adapters plug into those ports:

- `RocksDBStateStore` and `ChangelogStateStore` implement the `StateStore` port.
- `PrometheusObserver` implements the `Observer` port; `metrics_port == 0` swaps in the no-op `Observer`.
- The `flechtwerk.mqtt` transport bridges a push-driven MQTT source into the extractor model.
- `FakeKafkaConsumer`, `FakeKafkaProducer`, `FakeMqttConnection`, and friends (in `testing.py`) stand in for the real transports under test.

Wiring is done through **reactor-di**. `Flechtwerk.of(...)` returns a private `_FlechtwerkModule` DI container typed as the narrow public `Flechtwerk` ABC, so an application never sees the wiring — the same idiom as `Extractor.of(...)` and `Transformer.of(...)`. All consumers run `read_committed`.

!!! note "When a Transport Belongs in the Framework"
    A transport adapter earns a place in the framework only when its correctness depends on runner delivery semantics. MQTT qualifies because its manual-ACK protocol (ACK only after Kafka durability) leans on the extractor runner's re-entry contract; a plain HTTP poller does not, and stays in application code.

The framework has no CLI, no module-level `os.getenv`, no `load_dotenv`, and no opinions about how applications are packaged or deployed. All configuration is injected by the caller.

## Module Map

Everything ships under `src/flechtwerk/`.

| Module | Responsibility |
| --- | --- |
| `attribute/` | `Attribute[V]` — a single, type-safe handle on a dict key carrying an explicit `Codec[V]`. Composable codec atoms (`STR`, `INT`, `BOOL`, `DATE`, `FLOAT`, `DATETIME`, `TIME`, `RECORD`, `ANY`) and constructors (`LIST`, `SET`, `TUPLE`, `DICT`). `Record` wraps a JSON-native `dict[str, Any]` (`.raw`) and runs a codec on every write. |
| `types.py` | `Config`, `Event`, `State` (`Record` subclasses) plus the `IncomingMessage` / `Message` dataclass envelopes (timestamps are real `datetime`, not millis) and the `Payload` union (`bytes` / `str` / `Event`) that `Message` accepts — and validates at construction — for outgoing keys and values. |
| `stage.py` | The non-exported `Stage` base shared by both stage kinds: the `config_topics` declaration, the global `configs` store handle, the `enrich_config` / `extract_state_key` hooks, and the default no-op async context-manager lifecycle. |
| `extractor.py` | `Extractor` (ABC; `poll` is abstract), `ExtractorRunner`, `TokenRebalanceListener`, and `token_for`. Async polling with `asyncio.gather` for concurrent config processing; config ownership is token-sharded across replicas. |
| `transformer.py` | `Transformer` (ABC; `transform` is abstract), `TransformerRunner`, `Task`, and `TaskRebalanceListener`. Splits work into per-input-partition tasks, each with its own transactional producer and changelog store. |
| `kafka.py` | Wire helpers: `parse_message()`, `encode_json()`, `restore_changelog()`, `read_to_end()`, `is_tombstone()`, `decode_key()` / `decode_event()`. Runners type-hint aiokafka directly — no wrapper classes. |
| `configs.py` | `ConfigStore` plus `bootstrap_config_store` and `drain_config_updates` — Kafka Streams' GlobalKTable pattern, specialized to configuration. |
| `state.py` | The `StateStore` port and its `RocksDBStateStore` / `ChangelogStateStore` adapters. `rocksdict` is imported lazily on first RocksDB open. |
| `module.py` | `Flechtwerk` — the narrow application-facing handle — plus the private `_FlechtwerkModule` reactor-di container that lazily creates and shares all Kafka resources. Also hosts `MqttBrokerConfig`. |
| `mqtt.py` | The MQTT→Kafka bridge: `MqttConnection`, `MqttSubscription`, and `MqttExtractor`. The only framework module importing paho-mqtt eagerly (shipped as the `flechtwerk[mqtt]` extra). |
| `metrics.py` / `observer.py` | The `Observer` port and its `PrometheusObserver` adapter. Runners emit observer events; label *names* are caller-provided via `metrics_labels`. |
| `testing.py` | Duck-typed test doubles (`FakeKafkaConsumer` / `FakeKafkaProducer`, `make_record()`, `RecordingObserver`, `InMemoryStateStore`, and MQTT doubles). Imports no paho. |

## The Two Stage Engines

### Extractor

A plain `Extractor` consumes only its `config_topics`. Config handling rides the config machinery in `configs.py`: a `group_id=None` consumer is assigned all partitions of every config topic, bootstrapped to the end offsets captured at startup, then drained non-blocking each poll cycle. `poll(config, state)` yields `Message | State`; the runner persists `State` only when it differs from the current value, and a falsy `State` deletes the entry.

!!! note "Scaling Is the Replica Count"
    Replicas of one `application_id` shard the *config set* between them via consumer-group leases; a single replica owns everything. Each held token owns a transactional producer — the transformer's fencing primitive, applied per token instead of per input partition.

Instances join the `application_id` consumer group and lease the config topics' partitions as ownership **tokens**: each instance polls only the configs whose state key hashes onto a held token (`token_for` — the default partitioner's murmur2 math, computed consumer-side, so config-record *placement* stays irrelevant and Kafka UI writes keep working). The data plane is unaffected — every instance reads every config topic group-less into the global store, and `self.configs` exposes exactly that store. The rebalance protocol mirrors transformer tasks: the revoke callback is the handover barrier (cancel the in-flight cycle — aborting its open page — stop the token producers, wipe the local store), and the new owner fences its predecessors before re-reading the full changelog — the changelog needs no partition alignment. An extractor's config topics must share one partition count (the token space, validated at startup); extra replicas beyond it are hot standbys. Delivery is transactional per page — a `State` yield commits its page's messages and the cursor atomically — so a zombie's open page is aborted by the fence before the restore: cursors never regress, and handovers neither lose nor duplicate records. MQTT-sourced extractors participate too — the runner reconciles the stage's subscriptions against the owned config set before every cycle, so a handover unsubscribes what the old owner loses — but each handover is at-most-once for the in-flight MQTT tail; run one replica when that matters (see [Replicas and the Handover Window](../guides/mqtt.md#replicas-and-the-handover-window)).

The runner also exposes an optional `wakeup` event so a push-driven stage (MQTT) can end the between-cycles wait early; `poll_interval` then degrades to the idle / config-drain cadence.

### Transformer

Transformer work is partitioned into per-input-partition **tasks**, one per partition number spanning that partition of every input topic (the consumer uses the Range assignor, which co-assigns same-numbered partitions). Each task owns a transactional producer with the static transactional ID `{application_id}-{partition}` (EOS-v1 fencing) and a partition-scoped `ChangelogStateStore` sharing that producer via DI.

Exactly-once delivery is one Kafka transaction per task per `getmany()` batch, covering that task's output messages, state changes (deduped to one final write per key), and offset commits. Task transactions commit concurrently and independently. On a rebalance, all tasks are torn down under the batch lock and rebuilt for the assigned partitions — never retained, since a missed rebalance would make retained producers or stores silently stale.

A transformer may additionally declare `config_topics` and look entries up via `self.configs.get(wire_key)`. Config topics are read by a dedicated group-less consumer and never participate in any task transaction; lookups are eventually consistent (the GlobalKTable caveat).

## Application Lifecycle

`Flechtwerk` is an async context manager. On entry the Prometheus scrape server starts (outermost layer), then `validate_topics` runs (a transformer needs at least one input topic, an extractor at least one config topic, and the two lists must be disjoint), input and changelog partition counts are validated for transformers (config topics are exempt — except an extractor's own, which must share one count: the token space), and config topics are existence-checked so a missing one fails fast. `compression_type` defaults to `"zstd"` — JSON compresses roughly 13x, which is why the package depends on `aiokafka[zstd]`.
