# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Practices

### Git Operations

- **Never push without permission**: Only push commits when explicitly asked to do so
- Commits can be made freely, but pushing requires explicit user request
- **Don't commit code automatically**
- Always use PGP signatures when committing (`git commit -S`)

### Code Style

- **Alphabetical ordering**: Keep dictionary keys, constants, imports, and other collections in alphabetical order where possible
- **Declared public surface**: every framework module declares `__all__` — the curated public names; anything outside it is internal regardless of naming. Purely internal modules (`stage.py`, `kafka.py`, `metrics.py`) declare an empty `__all__` with a comment saying why. New names default to internal; adding one to `__all__` is an API commitment
- **Comprehensions over accumulator loops**: Build lists, dicts, and sets with comprehensions or generator expressions — not by starting with an empty container and mutating it in a `for` loop. Exception: loop bodies with real side effects (logging, I/O, external mutation)
- **Markdown formatting**: Always include empty lines after headings in Markdown files
- **File endings**: All text files must end with a newline
- **Typing**: Use `X | None` instead of `Optional[X]` for typing

## Commands

```bash
uv sync                        # venv + all dependencies
uv run pytest                  # unit tier — Docker-free
uv run pytest -m integration   # integration tier — ephemeral Kafka/Mosquitto via testcontainers; skips when Docker is unreachable
uv run coverage run -m pytest -m "integration or not integration" && uv run coverage report   # both tiers, matching CI
uv build                       # sdist + wheel; version derived from the latest vX.Y.Z tag via hatch-vcs
```

### Releasing

Tag `vX.Y.Z` and push the tag; `.github/workflows/publish.yaml` runs the test suite, builds the package with the tag-derived version (hatch-vcs), and publishes it to PyPI via trusted publishing. Nothing else sets the version.

## Architecture

Flechtwerk is an async stream processing framework for Kafka with hexagonal architecture (ports and adapters), packaged under `src/flechtwerk/`. Applications build stages in two shapes:

1. **Extractor** (abstract — construct via `Extractor.of(...)` or subclass): Async polls external sources based on Kafka configuration, maintains state in RocksDB for incremental processing
2. **Transformer** (abstract — construct via `Transformer.of(...)` or subclass): Consumes partitioned input topics and publishes to Kafka with exactly-once delivery

Module by module:

- **`attribute/`**: `Attribute[V]` is a single, type-safe handle on a dict key carrying an explicit `Codec[V]` — `Attribute(name, codec)` declares a required field, `Attribute(name, codec, optional=True)` (keyword-only) one that may be absent or `None`. Codecs are compositional: atoms (`STR`, `INT`, `BOOL`, `DATE`, `FLOAT`, `DATETIME`, `TIME`, `RECORD`, `ANY`) and constructors (`LIST(V)`, `SET(V)`, `TUPLE(V)`, `DICT(V)`). `Record` wraps a `dict[str, Any]` (`.raw`) and accepts only `Attribute` keys for indexing — codecs run on every write to enforce that `.raw` stays JSON-native. **Two constructor paths:** `Record(source)` accepts another `Record` (copy via `raw.copy()`) or `dict[Attribute, Any]` (typed literal — runs each Attribute's `write_to`); `Record.wrap(raw_dict)` is the wire-format entry point that accepts `dict[str, Any]` (raw JSON, `.raw` payloads, pickle restore) and runs every value through the recursive `_encode_any` walker. Pick the path that matches the input shape — typed literals and Record-spread go through `__init__`, raw JSON goes through `.wrap`. Subclasses (`Event`, `State`, `Config`) inherit both. A required attribute (the default) rejects `None` writes so a missing value can't land silently as JSON `null`; `optional=True` accepts `None` (stored as JSON `null`, encoder bypassed). This write-side guard is the *only* behavior the flag drives — the read distinction (`V` vs `V | None`) is carried by the method, not the attribute (`[]` reads-or-raises, `.get` / `.pop` tolerate absence), and `required` is a derived bool property (the inverse of `optional`). `ANY` is the escape hatch: a recursive walker over JSON-native primitives + `Record` + `datetime` + `date` + `time` + `set` + `tuple` that raises `TypeError` on anything else. Dict-spread (`Record({**other, NEW_ATTR: value})`) is supported — `Record.keys()` yields `ViewAttribute` handles whose overridden `read_from` / `write_to` round-trip raw wire values through the standard Record paths, so the spread itself stays in the typed-`__init__` path. **Polymorphic dispatch over isinstance branches:** every `Record` dict-op (`__getitem__`, `__setitem__`, `__contains__`, `__delitem__`, `get`, `pop`) is a one-line delegation to a method on the Attribute (`read_from`, `write_to`, `present_in`, `delete_from`, `get_from`, `pop_from`) — new Attribute kinds (e.g. `ViewAttribute`, declared in `attribute.py` but not re-exported from the package) override the primitives and inherit the derived ops via default impls on the base. The hierarchy is sealed (`__init_subclass__` rejects subclasses outside `attribute.py`), so kind dispatch is exhaustive and cross-method invariants only need policing in that one module. `IDENTITY` codec lives in `codecs.py` for `ViewAttribute`'s wire-form pass-through but is similarly non-exported. Pickle reconstructs via `__reduce__` = `(cls.wrap, (raw,))`, landing back in `wrap`; `copy.copy`/`copy.deepcopy` use the dedicated `__copy__`/`__deepcopy__` instead.
- **`types.py`**: `Config`, `Event`, `State` are `Record` subclasses + `IncomingMessage`/`Message` dataclasses (timestamps are `datetime`, not millis).
- **`stage.py`**: `Stage` (non-exported) is the common base of `Extractor`/`Transformer`: it owns the default no-op async context-manager lifecycle (runners enter the stage before processing and exit on shutdown; override to own resources — `MqttExtractor` does), the `config_topics: list[str]` declaration (default `[]`), the `enrich_config(config)` hook — one-time enrichment applied by the config machinery once per config record (bootstrap compacts first: once per surviving entry), never per poll tick or lookup — and the `extract_state_key(msg)` hook (default: the Kafka message key; `ExtractStateKeyFn` alias lives here too), which derives state identity: for an extractor it runs on config records, for a transformer on input records — and, for an extractor, ownership identity too. `Stage` also declares `configs: ConfigStore` — the GLOBAL per-process config store, injected by both runners before `__aenter__` (a transformer's is bootstrapped before `__aenter__`; an extractor's fills right after, so it is empty during `__aenter__` and complete by the first poll). Partitioned `input_topics: list[str]` is declared by `Transformer` alone — a plain `Extractor` consumes only config topics.
- **`extractor.py`**: `Extractor` (ABC, `poll` is `@abstractmethod`) + `ExtractorRunner` + `TokenRebalanceListener` + `token_for`. Async polling with `asyncio.gather` for concurrent config processing. Build via `Extractor.of(config_topics=..., poll=...)` (returns a private `_FunctionalExtractor` concrete subclass) or subclass directly for lifecycle needs. Config handling rides the config machinery (`configs.py`): the `group_id=None` consumer is assigned all partitions of every config topic, bootstrapped to the end offsets captured at startup (compacted by wire key, empty values are tombstones, `enrich_config` once per surviving entry), then drained non-blocking each cycle; the runner's per-key config entries (`runner.entries`) are read back from the (already enriched) store. `poll(config, state)` yields `Message | State`, and every `State` yield is a COMMIT BOUNDARY: `poll_one` runs per-page Kafka transactions (`TokenTask` producers, one per held token, static ID `{application_id}-{token}`, `create_token_producer` sets a 10-minute transaction timeout) — the page's messages, the state change (persisted only when it differs from the last committed value; empty/falsy deletes with a tombstone), and its changelog record commit atomically, KIP-618-style. Yield a page's messages BEFORE the `State` that accounts for them — the inverse order splits cursor and messages across two transactions, and a crash between them skips the page for good; messages after the last boundary — or from a poll that never yields `State` — are not lost: they commit as one trailing page when the generator completes, just without a cursor. The transaction begins lazily (an idle poll costs no coordinator round-trips), delivery futures are retrieved before every commit (aiokafka's flush/commit error paths are belt-and-braces'd), and same-token polls serialize on the task lock — parallelism = held token count. Config ownership is sharded across replicas by consumer-group leases (see "Extractor Scaling" below), with poll cycles on a background `cycle_loop` task so the main loop keeps pumping the membership consumer (max_poll_interval liveness) and draining configs during long backfills; `count_tokens` pins the token space at startup. `token_for(state_key, N)` is deliberately the DefaultPartitioner's murmur2 math — a compatibility promise pinned by test; changing it remaps ownership across a fleet. **Re-entry contract** (pinned by test): for a given config, `poll()` is re-entered only after the previous COMPLETED invocation's final transaction committed. A CANCELLED or FAILED invocation aborted its open page — invisible under read_committed, so re-polling duplicates nothing — and `poll_one` closed the generator deterministically first, so cancellation-aware sources (the MQTT template's buffer rollback) restored unconfirmed input before any re-entry. **Wakeup**: `Extractor.wakeup` (an optional `asyncio.Event`, default `None`) lets a push-driven stage end the runner's between-cycles wait early; `poll_interval` then degrades to the idle/config-drain cadence. **Reconciliation hook**: `Extractor.on_active_configs(configs)` (default no-op) receives the owned, non-suspended config set at quiescent points — the top of every poll cycle, plus once with an empty set when an assignment leaves the instance a hot standby — never from the revoke barrier and never on shutdown; it is how the MQTT template's unsubscribe lifecycle rides the runner (see the subscription-lifecycle invariant below).
- **`transformer.py`**: `Transformer` (ABC, `transform` is `@abstractmethod`) + `TransformerRunner` + `Task` + `TaskRebalanceListener`. Build via `Transformer.of(input_topics=..., transform=...)` (returns a private `_FunctionalTransformer` concrete subclass) or subclass directly. Work is partitioned into per-input-partition **tasks**, each owning a transactional producer (static ID `{application_id}-{partition}` — EOS-v1 fencing) and a partition-scoped `ChangelogStateStore`. Exactly-once delivery: one Kafka transaction per task per `getmany()` batch covering that task's output messages, state changes (deduped to one final write per key), and offset commits; task transactions commit concurrently and independently. Within a batch, records are bucketed by *(task, state key)*; same-bucket records run serially (so each one sees the previous one's yielded state), and buckets run concurrently via `asyncio.gather` so I/O-bound `transform()` calls overlap. Cross-bucket ordering is not preserved. Within a bucket, records appear in `input_topics` order then Kafka offset order. Each `transform` call receives a defensive deepcopy of the running state so in-place mutation without a yield can't leak. Stateless transformers simply never yield `State`. Rebalances (eager only in aiokafka) tear down all tasks under the batch lock and rebuild assigned ones in the main loop (producer start = fencing point → changelog restore → resume); the listener records failures on `runner.fatal` because aiokafka swallows callback exceptions. A transformer may additionally declare `config_topics` and look entries up via `self.configs.get(wire_key)`: the runner bootstraps the store before the subscribe joins the group, injects `configs` onto the transformer before `__aenter__`, and drains updates once per loop iteration outside the batch lock — every record of a batch sees one consistent config snapshot, and rebalances never touch the instance-level store. Lookups are eventually consistent and NOT part of any task transaction (Kafka Streams' GlobalKTable caveat). `Transformer.of(...)` accepts `enrich_config` (machinery-applied, works without `self`); store lookups need a subclass.
- **`kafka.py`**: `parse_message()` (JSON → `Event`), `encode_json()` (`Record` → UTF-8 bytes), `restore_changelog()` (rebuilds state from a compacted topic, optionally restricted to a partition subset for task restore; reads to the end offsets captured at entry — the LSO under `read_committed` — instead of treating an empty poll as end-of-log; primes aiokafka's metadata cache via `consumer._client.set_topics()` when discovering partitions — no fully public API exists for this, integration tests lock down the coupling). `read_to_end()` (the assign/seek/read-until-captured-end loop), `is_tombstone()`, `decode_key()`/`decode_event()` are the shared building blocks of `restore_changelog` and the config machinery. Runners type-hint `aiokafka.AIOKafkaConsumer`/`AIOKafkaProducer` directly — no wrapper classes.
- **`configs.py`**: `ConfigStore` (exported) + `bootstrap_config_store` + `drain_config_updates` — Kafka Streams' **GlobalKTable** pattern, specialized to configuration. Config topics are read in full (`group_id=None`, all partitions) into ONE per-process latest-value store keyed by **wire key** — a single key namespace merged across all of a stage's config topics (a tombstone on any topic deletes the key). The source topics are their own changelog (must be compacted, should stay small — the store lives in RAM per instance), no offsets are committed, and everything is re-read on every startup. `Stage.enrich_config` is applied here, once per record (bootstrap: once per surviving entry); Kafka Streams forbids transforming records into global stores (KIP-813) because a checkpoint restore would bypass the transformation — Flechtwerk has no checkpoints, every boot re-reads through the same enrich_config path, so the store can't diverge. Values are kept as wire bytes and parsed lazily on `get()` — each call returns a fresh `Config`; malformed values decode to an empty `Config` with a warning so they hit the caller's validation instead of masquerading as missing. Tests seed via `ConfigStore.of({key: config})`.
- **`keyring.py`**: the joserfc-free half of the secrets feature — `Keyring` (a frozen value object = pure key material: `kid → 32-byte key` + `primary`; built via `Keyring.of(...)` or `Keyring.from_json(...)` = an RFC 7517 JWK Set with a `primary` extension member) plus the process-global secret runtime (`install_keyring`/`active_keyring`, `set_secret_observer`/`active_observer`; `current_keyring` and the `_override`/`_restore` snapshot pair are internal). Imports NO joserfc, so `module.py` can annotate its `keyring: lookup[Keyring | None]` slot at decoration time (the `MqttBrokerConfig` precedent). `install_keyring` is idempotent for byte-identical material and raises `KeyringConflictError` on a conflicting second install (one keyring per process, v1); `set_secret_observer` is first-real-observer-wins-with-warning. The `_override_secret_runtime`/`_restore_secret_runtime` internals back `flechtwerk.testing.installed_keyring`.
- **`secrets.py`**: the `flechtwerk[secrets]` extra and the ONLY framework module importing `joserfc` (the paho-in-`mqtt.py` confinement discipline). `ENCRYPTED(inner, *, scope="", read_plaintext=False)` is a plain `Codec` constructor (like `LIST`/`DICT`) — no binding machinery; it returns a slim `_EncryptedCodec(Codec)` marker carrying `scope` (how the tooling recognizes an encrypted attribute and `reencrypt` re-stamps the scope; `read_plaintext` needs no field — `decode` closes over it). Any composition is legal (`LIST(ENCRYPTED(STR))`, `ENCRYPTED(RECORD)`). Wire form is `flenc:jwe:<compact JWE>` — `dir` + `A256GCM`, `kid` for rotation, and an OPTIONAL `flenc_scope` header (a single pinned `JWERegistry` enforces the `(alg, enc)` allowlist). **Scope is a one-way ratchet** enforced in decode: a scoped codec rejects a token with a *different* scope (relocation) but accepts an unscoped one (upgrade / add-scope-later); an unscoped codec rejects a scoped token (downgrade blocked). `reencrypt` promotes unscoped→scope but can't strip. Decode classifies by scheme (`flenc:` = ciphertext-form; unknown envelope → `SecretFormatError`; else a plaintext candidate — accepted only if the codec's `read_plaintext=True`, loudly via WARNING + `secret_plaintext_read`, otherwise `PlaintextSecretError`). `SecretDecryptError` carries scope/kid (+ topic/wire_key from callers that have them). Tooling: `encrypt_value`, `is_encrypted`, `kid_of`, `reencrypt`, `scan_config_topics`. Secret metrics are labelled by `scope`. Encryption is randomized (fresh nonce) → Record value-equality is unusable for encrypted fields; primary home is `Config`, `State` is usable (only an explicit typed re-write re-encrypts, so carried-forward state stays byte-stable and dedup holds), `Event` is unfit (per-message nonce budget + non-decrypting analytics consumers). See the [Encrypted Secrets concept page](docs/concepts/secrets.md).
- **`state.py`**: `StateStore` port + `RocksDBStateStore` and `ChangelogStateStore` adapters. Storage primitive is `put_bytes(key, raw)`; `put(key, state)` is `serialize`-then-`put_bytes`. `deserialize` is JSON-only — undecodable bytes are an unrecoverable data error (crash, then reset the affected state). `ChangelogStateStore` writes the same wire bytes to both inner store and Kafka; its optional `partition` pins changelog writes to one explicit partition (transformer tasks) while `None` (extractors) uses key hashing; the producer is shared with the owning runner/task via DI so transformer `put()` calls join the task's open transaction. `restore` replays the changelog (optionally one partition) through `inner.put_bytes` (raw bytes pass through; deserialization is deferred to first `get()` per key). `get()` returns a protective copy. `RocksDBStateStore.close()` is a wipe, not an end-of-life: it drops the cached DB handle so the next access lazily reopens a fresh, empty store — the extractor's revoke-wipe/assign-restore cycle depends on this. `partition_counts()` reads per-topic partition counts for the startup co-partitioning validation. `rocksdict` is imported lazily on first RocksDB open — stateless stages never load it.
- **`module.py`**: `Flechtwerk` — the narrow application-facing handle (an ABC exposing only `of(...)` / `run()` / the async context manager), plus the private `_FlechtwerkModule` reactor-di DI container that lazily creates and shares all Kafka resources. `Flechtwerk.of(...)` returns a `_FlechtwerkModule` typed as `Flechtwerk`, so an application never sees the wiring (same idiom as `Extractor.of` / `Transformer.of`). To embed as a child of a larger reactor-di module, declare `make[Flechtwerk, _FlechtwerkModule]` and let the parent wire every `lookup` field. The `Flechtwerk` base stays annotation-free — `@module` walks `get_type_hints` over the MRO, so any annotated attribute on it would leak onto the public type; `test_public_handle_exposes_no_container_internals` pins this. Both stage shapes get factory methods from which their runners build per-task resources: transformers `create_task_producer`/`create_task_store`/`create_restore_consumer`, extractors `create_token_producer` (transactional, 10-minute timeout)/`create_restore_consumer` plus the shared `inner_store` and `changelog_topic` their token-store views wrap. The runners consume the caller's stage through the `configured_stage` factory, which completes an MQTT-sourced stage with the broker settings and the observer — the module stays pure factory + mediation, no behavioral methods. All consumers run `read_committed`; the transformer input consumer uses the Range assignor. Transformers with config topics get a dedicated group-less `config_consumer` (client id suffix `-config`); extractors reuse their main consumer (already group-less); both get the `config_store`. Every extractor additionally gets a `membership_consumer` (group_id = `application_id`, Range assignor, auto-commit off, `auto_offset_reset="latest"`, client id suffix `-membership`), and there is no module-level startup state restore — the runner restores per token assignment. Async context manager — Prometheus scrape HTTP server is the outermost layer; startup runs `validate_topics` (transformer needs ≥1 input topic, extractor needs ≥1 config topic, the lists must be disjoint), validates input/changelog partition counts for transformers (config topics are exempt — their counts are unconstrained — EXCEPT an extractor's own config topics, which must share one count: the token space), and existence-checks config topics (a missing one must fail fast: the assign-based bootstrap would never discover a topic created later). `compression_type` defaults to `"zstd"` (JSON compresses ~13×), which is why the package depends on `aiokafka[zstd]`.
- **`mqtt.py`**: the MQTT→Kafka bridge for push-driven extractors — the only framework module importing paho-mqtt eagerly (`module.py` reaches it only via a lazy import; the dependency ships as the `flechtwerk[mqtt]` extra). `MqttConnection` owns ONE paho client per process, driven entirely by the asyncio event loop via socket callbacks (no threads); `clean_session=False` + a stable client_id (the module-wide `client_id`) + `manual_ack=True` give at-least-once for QoS ≥ 1 publishers. Inbound messages route by topic filter to per-topic `MqttSubscription` views (buffer + pending-ACK list each); unmatched QoS 0 messages are warned and dropped, unmatched QoS ≥ 1 messages are **held un-ACKed** and re-routed the moment a matching subscription registers — the persistent session replays its backlog right after CONNACK, *before* the Kafka config bootstrap has subscribed anything, so ACKing (or dropping) there would silently destroy the very backlog `clean_session=False` protects. The subscription lifecycle is reconciliation-driven: `MqttExtractor.on_active_configs` (the runner's cycle-top hook) unsubscribes every topic no active config declares — pending ACKs sent (Kafka-durable at a quiescent point), undelivered buffer ACK-dropped with warn + counter — and latches the desired-filter set, after which unmatched QoS ≥ 1 arrivals are ACK-dropped as `stale` instead of held (see the subscription-lifecycle invariant below). No in-process reconnect: an unexpected disconnect surfaces as a `ConnectionError` from `drain()` once the buffer empties → crash → orchestrator restart. `MqttExtractor` opens the connection eagerly in `__aenter__`, sets the runner `wakeup` event (`on_message` fires it after buffering → sub-second delivery), and owns a template `poll()`: ACK-previous-batch (safe by the runner's re-entry contract) → `drain(drain_limit)` → per message `relay(config, topic, payload)` with JSON decoding. The `relay` hook returns a `Message` (forward + ACK with next batch), `None` (drop + immediate ACK), or raises (poison-drop: warn + ACK + counter — crashing would redeliver the poison forever at QoS 1). The template is cancellation-safe: when the runner closes a poll generator mid-batch (token handover), everything drained-but-unconfirmed is un-marked and rolled back to the buffer front, so the next entry's ACK-previous-batch covers only what a COMPLETED poll forwarded. Build via `MqttExtractor.of(config_topics=..., relay=...)` or subclass; the `topic` config attribute (`flechtwerk.mqtt.TOPIC`) is framework-owned (one config record = one MQTT topic filter = one subscription). Sources that don't fit (stateful, 1:N, non-JSON) override `poll()` — the connection layer works without the template. Broker settings are injected via `Flechtwerk.of(mqtt=MqttBrokerConfig(...))`; the dataclass lives in `module.py` (reactor-di resolves annotations at decoration time, and the container must stay paho-free).
- **`metrics.py` / `observer.py`**: Runners emit observer events (`message_in`, `message_out`, `transaction_committed`, `active_configs` — for an extractor, the *owned* active count — `config_message_in`, `config_store_entries` — the "did my config arrive?" gauge — `config_store_restored`, `state_restored`, `tasks_assigned`, `tokens_assigned` — the extractor's lease count; 0 = hot standby — `dispatch_scope`, `batch_scope`, `poll_cycle_scope`; MQTT: `mqtt_connected`, `mqtt_disconnected`, `mqtt_message_in`, `mqtt_message_dropped` with reason `filtered`/`poison`/`stale`/`unsubscribed`, `mqtt_buffered` — the MQTT `topic` label is always the subscription filter or the `(unmatched)` sentinel for `stale` drops, bounded cardinality either way); `PrometheusObserver` translates these into prometheus-client metrics. Label *names* are caller-provided via `metrics_labels` — the framework itself is application-agnostic and knows nothing about what the labels are called. `metrics_port == 0` disables Prometheus and uses the no-op `Observer`.
- **`testing.py`**: `FakeKafkaConsumer`/`FakeKafkaProducer` (duck-typed aiokafka subset), `make_record()` factory (real `aiokafka.ConsumerRecord` instances), `RecordingObserver`, `InMemoryStateStore`, and MQTT doubles (`FakeMqttConnection`/`FakeMqttSubscription` with an `acked` record, `make_mqtt_message()`; paho imports deferred so importing `testing` never loads paho). Pre-set `extractor.connection = FakeMqttConnection()` to test relays without a broker.

The framework has no CLI, no module-level `os.getenv`, and no `load_dotenv` — all configuration is injected by the caller (the keyring included: `Flechtwerk.of(keyring=...)`, or `install_keyring(...)` for standalone producer/ops tooling).

## Key Architectural Patterns

**Stateful Processing**: State lives in ephemeral RocksDB instances backed by a compacted Kafka changelog topic (Kafka Streams pattern — no PVC, pods are ephemeral). Generators yield `State` to persist — a transformer's batch writes the final value per key when it differs; an extractor commits a page per `State` yield, persisted when it differs from the last committed value. Yielding empty/falsy `State` deletes the entry (Kafka tombstone). Stateless stages never open a RocksDB file. For transformers, state identity is *(input partition, extract_state_key)*: each task owns its own RocksDB store, writes its changelog entries to its own changelog partition (explicit-partition produce, not key hashing), and restores exactly that partition on assignment. The framework makes no assumptions about what `extract_state_key()` returns — a key produced from several partitions yields independent per-task entries (with the default `extract_state_key = msg.key`, Kafka's key partitioning makes this indistinguishable from a global key). An extractor wipes and re-reads the full changelog on every token assignment (extractor state is small by the config-topic contract, so restore-all costs what a startup restore used to — and the changelog therefore needs no partition alignment with anything).

**Co-Partitioning Trap**: If one logical state entry must see records from *several* input topics, those topics must be co-partitioned by key — same key bytes, same partitioner, same partition count. Only the partition count is validated at startup; key/partitioner alignment cannot be checked and is the application's responsibility (exactly as in Kafka Streams). Get it wrong and the same `extract_state_key` arrives on different partition numbers, yielding independent state shards owned by different tasks — possibly on different instances. This is a silent split, not an error. Kafka Streams' DSL protects against the related case (key changed inside the topology) by auto-inserting a repartition topic; Flechtwerk is a Processor-API-level framework and has no equivalent — a mid-pipeline key change requires an explicit intermediate topic, i.e. another transformer hop. Output keys and changelog keys impose no constraints: output partitioning is decoupled from task identity, and changelog placement is explicit-partition, ignoring keys entirely.

**The escape hatch for config lookups is a config topic**: when one side of the "join" is a config table rather than a keyed stream, declare it in `config_topics` instead of `input_topics` (Kafka Streams' GlobalKTable, specialized to configuration). Every instance reads it in full into the per-process `ConfigStore`; lookups go through `self.configs.get(wire_key)`; partition placement and count are irrelevant, so any producer — Kafka UI included — can write to it. The trade: the source topic must be compacted and stay small, the wire key is authoritative (tombstones carry no body), and lookups are eventually consistent, outside the task transaction.

**Exactly-Once Delivery & Load Balancing**: Transformer work is split into **tasks** — one per input partition number, spanning that partition of every input topic (the consumer uses the Range assignor, which co-assigns same-numbered partitions). Each task owns a transactional producer with the static transactional ID `{application_id}-{partition}`; one Kafka transaction per task per batch covers that task's output messages, state changelog writes (the task's `ChangelogStateStore` shares its producer), and offset commits. Multiple instances are safe: when a partition moves, the new owner's `InitProducerId` fences the previous owner's producer and aborts its in-flight transaction (Kafka Streams EOS-v1 — aiokafka has no KIP-447 generation fencing), and state is re-restored from the changelog's last stable offset before processing resumes. On rebalance, all tasks are torn down and rebuilt — never retained, since a missed rebalance would make retained producers/stores silently stale. All framework consumers run `read_committed`. Constraints: all input topics of a transformer must have equal partition counts (validated at startup, matching changelog created); the partition count is frozen once state exists (repartitioning requires a state migration); instances beyond the partition count sit idle.

**Extractor Scaling: Token-Sharded Config Ownership**: Extractors scale out by construction — there is no mode flag. Instances of one `application_id` join a consumer group on the config topics purely for partition **leases** ("tokens") — the membership consumer commits nothing and every record it fetches is discarded (do not "fix" that: records arrive by *placement*, ownership does not). The data plane is unaffected: every instance reads every config topic group-less into the global `ConfigStore`, and `self.configs` exposes exactly that store. Ownership is computed consumer-side: an instance polls the configs whose `token_for(extract_state_key(config), N)` lands on a held token, where `token_for` is the DefaultPartitioner's murmur2 math and N is the config topics' validated common partition count (the maximum useful replica count; extra replicas are hot standbys that take over on failure). One replica — the common deployment — owns every token and behaves exactly like the historical single-instance runner. Ownership follows *state identity*, so no state entry can ever have two owners; and it ignores placement, so Kafka UI's write-everything-to-partition-0 habit stays harmless (pinned by integration test). The rebalance protocol mirrors transformer tasks: eager revoke → `suspend_tokens` barrier (cancel the in-flight cycle — even mid-backfill; the cancelled poll aborts its open page — stop the token producers, wipe the local store) → assign (bookkeeping only) → main-loop fence-then-restore under the rebalance lock (`start_task` per token issues InitProducerId BEFORE the full-changelog restore, so even a zombie that never ran its barrier has its open page aborted and its producer fenced first) → fresh `cycle_loop` task. Poll cycles run on that background task so the main loop keeps pumping the membership consumer (max_poll_interval liveness) and draining configs during long backfills. Delivery is **exactly-once from cursor to Kafka** for re-readable sources: per-page transactions plus fencing mean cursors never regress and handovers neither lose nor duplicate; only side effects outside Kafka (the external read, an MQTT ACK) stay at-least-once by nature — and every downstream consumer must read `read_committed` or it will see aborted pages. `poll_cycle_scope` approaching `poll_interval` is the signal to add replicas. MQTT extractors CAN run multiple replicas — the reconciliation lifecycle unsubscribes disowned topics at handover — but each handover has a bounded at-most-once window (see the subscription-lifecycle invariant below); run one replica when that loss is unacceptable. Per-config work is indivisible: one epoch backfill is a sequential cursor walk no sharding can split.

**"Let It Crash" Error Strategy**: No framework-level retry logic. Errors propagate; recovery is infrastructure: orchestrator restarts (e.g. Kubernetes `CrashLoopBackOff`), changelog replay restores state, transformer transactions catch any duplicates from partial writes. The key distinction is recoverable vs non-recoverable: only use try/except when the catch block can actually *remedy* the problem (e.g. refresh an expired token, skip a 400 on an endpoint that doesn't exist for this tenant). For transient errors like timeouts or 5xx, crash — sleeping and retrying in-process is reimplementing `CrashLoopBackOff` poorly. Never catch-and-skip data errors (silent data loss).

## Invariant: config topics never participate in a Kafka transaction

In a transformer, config topics must have no contact with any task
transaction. This holds by construction, through three independent
mechanisms — keep all of them intact:

- **Separate, group-less consumer.** A transformer's config topics are read
  by a dedicated `config_consumer` with `group_id=None` (`module.py`). No
  consumer group means no committed offsets — config-topic offsets can never
  appear in `send_offsets_to_transaction`. The offsets that DO enter a task
  transaction are built exclusively from the main consumer's input-topic
  batch, and `validate_topics` keeps `config_topics` disjoint from
  `input_topics`, closing that path too.
- **Updates land outside the transaction boundary.** `check_config_updates`
  runs once per loop iteration, outside the batch lock, and only mutates the
  in-memory `ConfigStore` — no task, producer, or transaction involved. The
  fetch-then-drain-then-process sequencing gives every record of a batch one
  consistent config snapshot; that is a scheduling courtesy, not
  transactional coupling.
- **No write path through the task producers.** The framework never produces
  to a config topic; the store is fed only by `bootstrap_config_store` and
  `drain_config_updates`.

The extractor's membership consumer changes none of this. It is the one
framework consumer with a `group_id` on config topics, but it exists
purely for partition leases: `enable_auto_commit=False`, no commit call
anywhere, every fetched record discarded. And the extractor's per-page
token transactions contain output messages and changelog writes ONLY —
`send_offsets_to_transaction` is never called on them, and the framework
still never produces to a config topic.

Lookups via `self.configs.get(...)` are therefore eventually consistent —
Kafka Streams' GlobalKTable caveat, stated on `Stage.configs` and in
`configs.py`.

### Why the config consumer still runs read_committed

The isolation level is a consumption-side filter; it does not enroll config
reads in any transaction, so it cannot violate the invariant above. For the
normal case — non-transactional producers (ops tooling, Kafka UI) writing
config — it makes no difference at all: records are visible immediately
either way. It matters only when a *transactional* producer writes to a
config topic (nothing forbids a transformer emitting an output `Message`
onto one): `read_uncommitted` would apply records from aborted transactions
to the store — and a startup bootstrap would compact them in until the next
boot — while `read_committed` merely delays visibility until commit, which
the eventually-consistent contract already absorbs. `read_committed` also
gives `bootstrap_config_store` / `read_to_end` a well-defined end offset
(the LSO). Switching to `read_uncommitted` buys nothing and opens the
aborted-write hole — keep `read_committed`, matching every other framework
consumer.

## Invariant: the extractor runner's re-entry contract

For any given config, `ExtractorRunner` re-enters `poll()` only after the
previous COMPLETED invocation's final transaction committed — its messages
and cursor durable, atomically. `poll_one` sends each yielded `Message`
immediately — BEFORE resuming the generator, so a cancellation-aware source
can only mark a message pending-ACK after the producer accepted it — and
retrieves every delivery result before each commit: aiokafka's `flush()` is
a bare `asyncio.wait` that never raises, so without the retrieval a
delivery-stage failure could pass silently
(`test_delivery_failure_crashes_before_state_is_persisted` pins this; the
transaction's own error tracking is the second net). A send, delivery, or
commit failure crashes the process with the page aborted. A CANCELLED
invocation (poll-cycle teardown on a token handover) is closed
deterministically at its current yield, rolls its unconfirmed input back
(the MQTT template), and aborts its open page — invisible under
read_committed, so nothing it sent can ever be ACKed or duplicated. The
MQTT template's ACK-the-previous-batch-at-the-top-of-the-next-poll pattern
is correct *only* because of these orderings — do not weaken any of them.
`test_reentry_contract_commit_strictly_precedes_next_poll` and
`test_runner_cancellation_mid_send_leaves_no_false_pendings` pin them.

## Invariant: paho-mqtt stays confined to flechtwerk.mqtt

- `flechtwerk.mqtt` is the only framework module that imports paho eagerly.
  `module.py` must never import `.mqtt` at module level — the lazy import
  inside the `configured_stage` factory is both what keeps `mqtt → module`
  acyclic and what makes the `flechtwerk[mqtt]` optional extra work (an
  application that never configures MQTT never loads paho).
  `testing.py`'s MQTT doubles defer their paho imports for the same reason.
- `MqttBrokerConfig` lives in `module.py`, not `mqtt.py`: reactor-di's
  `@module` decorator resolves all class annotations at decoration time, so
  the `mqtt: lookup[MqttBrokerConfig | None]` slot needs a runtime-importable,
  paho-free name.
- The framework reads no environment and does no identity defaulting: broker
  settings arrive fully resolved through `Flechtwerk.of(mqtt=...)` (or
  parent-module wiring), and the session identity is the module-wide
  `client_id` (injected onto the stage by `configured_stage`; applications
  typically pass a per-instance stable identity — e.g. the pod name in
  Kubernetes). `MqttExtractor` rejects an empty `client_id` at startup
  (MQTT 3.1.1 forbids one with a persistent session).

## Invariant: joserfc stays confined to flechtwerk.secrets

- `flechtwerk.secrets` is the only framework module that imports `joserfc`
  (eagerly). `module.py` must never import `.secrets` at module level — it
  reaches key material through the joserfc-free `flechtwerk.keyring` seam, which
  is what keeps `secrets → module` acyclic and makes the `flechtwerk[secrets]`
  optional extra work (an application that never declares an `ENCRYPTED`
  attribute never loads joserfc; `test_importing_flechtwerk_does_not_load_joserfc`
  pins this). `testing.py`'s keyring fixtures import only `flechtwerk.keyring`,
  staying joserfc-free the same way.
- `Keyring` lives in `keyring.py`, not `secrets.py`: reactor-di resolves the
  `keyring: lookup[Keyring | None]` annotation at decoration time, so the slot
  needs a runtime-importable, joserfc-free name (the `MqttBrokerConfig`
  precedent exactly).
- The commitment is to the *format* (`flenc:jwe:` compact JWE, dir+A256GCM),
  not the library — joserfc could be replaced by a vendored ~50-line
  implementation without a wire change; the pinned panva-jose and
  independent-pyca interop vectors guard that boundary.

## Boundary rule: which transport adapters belong in the framework

Flechtwerk may own a transport adapter when its *correctness depends on runner
delivery semantics* — MQTT qualifies because manual-ACK-after-Kafka-durable
leans on the re-entry contract above. It must never own payload semantics,
source-specific parsing, or per-source config schemas (those stay in
application code); an adapter that would work identically as application code
stays application code. Outbound MQTT (Kafka→MQTT command publishing) is
explicitly out of scope for now: an MQTT publish can never join a Kafka
transaction, so any future sink is at-least-once by construction and needs
its own design. `MqttConnection` is deliberately direction-neutral so a sink
can ride the same connection later.

## Invariant: the MQTT subscription lifecycle is config-driven reconciliation

Tombstoning a config, suspending it, editing its `topic` filter, and losing
its token at a rebalance all converge on ONE mechanism: the runner hands
`Extractor.on_active_configs` the owned, non-suspended config set at
quiescent points — before every poll cycle, and once with an empty set when
an assignment leaves the instance a hot standby (the standby branch of
`start_pending_tokens`, which has no cycle loop to reconcile from) — and
`MqttExtractor` reconciles the broker session against it
(`MqttConnection.reconcile`). Topics no active config declares are
UNSUBSCRIBEd and their views disposed: pending ACKs are sent (provably
Kafka-durable at a quiescent point, by the re-entry contract), and buffered
messages that never reached Kafka are ACK-dropped with a warning and an
`mqtt_message_dropped(reason="unsubscribed")` count. Dropping is deliberate:
MQTT 3.1.1 has no NACK and cannot requeue for another consumer, so the
alternatives are silent loss or the historical inflight-window wedge. Stop
the publisher before removing a config and the dropped tail is empty.

The first reconciliation also LATCHES the desired-filter set as
authoritative (`MqttConnection.desired`): from then on, QoS ≥ 1 messages
matching no desired filter are ACK-dropped on receipt (reason `stale`,
topic label the `(unmatched)` sentinel — the concrete publish topic would
be unbounded metric cardinality) instead of held. That mops up
post-UNSUBSCRIBE stragglers and traffic replayed for filters an earlier
deployment left in the persistent session — which 3.1.1 can neither
enumerate nor selectively remove. BEFORE the latch — the startup window —
unmatched QoS ≥ 1 messages are still held un-ACKed: the persistent session
replays its backlog right after CONNACK, before the config bootstrap has
declared any filter, and dropping there would destroy the very backlog
`clean_session=False` protects. Do not weaken the latch ordering.

Three boundaries the mechanism deliberately respects (all pinned by test):

- **Shutdown never unsubscribes** — `run()`'s teardown calls no reconcile;
  the persistent session keeps buffering for the next incarnation. Removal
  is config-driven, not lifecycle-driven.
- **The revoke barrier never reconciles** — `suspend_tokens` calls no hook:
  a cancelled poll's rollback restores drained-but-unconfirmed messages to
  the buffer, and the transient revoke→assign window of a self-handover
  must find them intact. Reconciliation belongs to settled assignments
  only (cycle top, standby branch).
- **Suspension means discard** for an MQTT config: the topic is
  unsubscribed and interim messages are lost (kept-but-un-ACKed is exactly
  the wedge). Resume re-subscribes on the next poll — the template's
  `subscribe` is idempotent.

Multi-replica MQTT is thereby possible but **at-most-once at handovers**: a
disowned topic's undelivered buffer is dropped on the old owner, and the
unsubscribe→subscribe gap delivers to neither session. Run one replica when
that loss window is unacceptable; the lossless multi-replica story is MQTT 5
(shared subscriptions, session expiry, subscription identifiers) — a
separate, future design. (Single-replica self-handovers remain fully
lossless: sends precede generator resumption, so nothing unsent is ever
marked pending, and the cancellation rollback in `mqtt.py` restores
unconfirmed messages before any re-entry.)

## graphify (optional, local)

[graphify](https://pypi.org/project/graphifyy/) builds a queryable knowledge graph of this codebase (god nodes, community structure, cross-file relationships). It is a per-developer convenience, not part of the project: `graphify-out/` is gitignored, nothing in the build or CI depends on it, and its Claude Code hooks live in the machine-local `.claude/settings.local.json`. Skip this section entirely if `graphify-out/graph.json` does not exist.

Initialization (once per clone, opt-in):

```bash
uv tool install graphifyy     # or: pip install graphifyy
```

Then run `/graphify .` in a Claude Code session to build the graph, and optionally `graphify hook install` to add git post-commit/post-checkout hooks that rebuild it automatically (AST-only, no API cost).

Rules (only when graphify-out/graph.json exists):

- For codebase questions, first run `graphify query "<question>"`. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
