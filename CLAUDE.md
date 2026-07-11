# CLAUDE.md

Guidance for the Flechtwerk framework code in this directory. The architecture
is documented in the repository root `CLAUDE.md`; this file holds framework
invariants that must survive refactoring.

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

Lookups via `self.configs.get(...)` are therefore eventually consistent —
Kafka Streams' GlobalKTable caveat, stated on `Transformer.configs` and in
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
previous invocation's yielded messages were sent to Kafka and the producer
was flushed (`poll_one` awaits `send_batch` before returning; a send failure
crashes the process). The MQTT template's ACK-the-previous-batch-at-the-top-
of-the-next-poll pattern is correct *only* because of this ordering — do not
weaken it. `test_reentry_contract_flush_strictly_precedes_next_poll` pins it.

## Invariant: paho-mqtt stays confined to flechtwerk/mqtt.py

- `flechtwerk/mqtt.py` is the only framework module that imports paho eagerly.
  `module.py` must never import `.mqtt` at module level — the lazy import
  inside the `configured_stage` factory is both what keeps `mqtt → module`
  acyclic and the seam for a `flechtwerk[mqtt]` optional extra at extraction
  time (an application that never configures MQTT never loads paho).
  `testing.py`'s MQTT doubles defer their paho imports for the same reason.
- `MqttBrokerConfig` lives in `module.py`, not `mqtt.py`: reactor-di's
  `@module` decorator resolves all class annotations at decoration time, so
  the `mqtt: lookup[MqttBrokerConfig | None]` slot needs a runtime-importable,
  paho-free name.
- The framework reads no environment and does no identity defaulting: broker
  settings arrive fully resolved through `Flechtwerk.of(mqtt=...)` (or
  parent-module wiring), the session identity is the module-wide `client_id`
  (injected onto the stage by `configured_stage`; the application entry
  point resolves `FLECHTWERK_CLIENT_ID` → `application_id`, the pod name in
  K8s), and `MqttExtractor` rejects an empty `client_id` at startup (MQTT
  3.1.1 forbids one with a persistent session).

## Boundary rule: which transport adapters belong in the framework

Flechtwerk may own a transport adapter when its *correctness depends on runner
delivery semantics* — MQTT qualifies because manual-ACK-after-Kafka-durable
leans on the re-entry contract above. It must never own payload semantics,
source-specific parsing, or per-datasource config schemas (those stay in
`ds/`); an adapter that would work identically as application code stays
application code. Outbound MQTT (Kafka→MQTT command publishing, BA-975) is
explicitly out of scope for now: an MQTT publish can never join a Kafka
transaction, so any future sink is at-least-once by construction and needs
its own design. `MqttConnection` is deliberately direction-neutral so a sink
can ride the same connection later.

## Known limitation: no subscription lifecycle for removed configs

Removing (tombstoning) or suspending a config deletes its runner entry, but
nothing unsubscribes the MQTT topic: the broker session keeps the
subscription, the in-process view keeps buffering, and — at QoS ≥ 1 — its
un-ACKed messages occupy the shared session's inflight window until every
slot is taken and the broker pauses delivery for ALL topics of this client.
Messages held for a genuinely stale session subscription (a topic subscribed
under this client_id in an earlier deployment and never unsubscribed) stall
the window the same way. This predates the framework move and is
deliberately deferred: a correct fix needs a runner→stage config-removal
hook, a broker UNSUBSCRIBE, and a decision about un-ACKed buffered messages
(they were never written to Kafka). Until then: stop the publisher before
removing a config, and recover a wedged session with a fresh
`FLECHTWERK_CLIENT_ID` (a new broker session) or broker-side session cleanup.
