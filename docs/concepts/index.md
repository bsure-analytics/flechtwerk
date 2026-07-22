---
opener: true
title: Concepts
tagline: The operational model Kafka Streams nailed a decade ago, rewoven in async Python.
---

# Concepts

Flechtwerk (German: *interlacing, wickerwork*) is a small async stream processing framework for Kafka. It takes the operational design that Kafka Streams proved in production — consumer groups for partition assignment, compacted changelog topics as the durable state of record, Kafka transactions tying state writes, output messages, and offset commits into one atomic unit — and ports it to modern async Python built on `asyncio` and `aiokafka`. It runs on stock `asyncio` (and therefore on Windows); the event loop is the application's choice, and `uvloop` is the recommended pick for best throughput.

If you have run Kafka Streams, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via transactions, and ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

## Two Stage Shapes

An application builds stages in one of two shapes. Both express their whole contract as an async generator that yields `Message` (emit an output record) and `State` (persist state for the current key; a falsy `State` tombstones the key).

- **Extractor** — async-polls an external source once per config record per poll cycle, using `State` as its resume cursor. Its only Kafka input is its `config_topics`. The MQTT bridge is a push-driven extractor.
- **Transformer** — consumes partitioned `input_topics`, runs `transform(msg, state)` per record, and publishes to Kafka with exactly-once delivery.

Both are ABCs. Use the `.of(...)` factory for stateless or simply-stateful stages, or subclass directly when you need lifecycle management (HTTP clients, dedup instances) via `__aenter__` / `__aexit__`. Stateless stages simply never yield `State` and never open a RocksDB file.

## The Operational Model

- **Consumer groups** drive partition assignment and rebalancing — standard Kafka semantics, no custom coordination.
- **Changelog topics** (compacted) are the durable state of record. RocksDB is a local cache rebuilt by replay on startup. Pods are ephemeral; no PVCs.
- **Kafka transactions** span all output messages, all state changelog writes, and offset commits for a single processing batch. Transformer work is split into per-input-partition tasks; each task owns a transactional producer (static transactional ID — EOS-v1 fencing) shared with its changelog state store via DI.
- **Per-batch parallelism by state key.** Within a `getmany()` batch (capped at `max_poll_records`, default 500 — so a backlog drains as many ordinary batches, not one giant one), records are bucketed by state key within each task. Buckets run concurrently via `asyncio.gather` so I/O-bound `transform` calls overlap, while records sharing a key run serially inside their bucket — each one sees the previous one's yielded state. Cross-key ordering is not preserved.

!!! note "Let It Crash"
    There is no framework-level retry logic. The line is recoverable vs non-recoverable, not transient vs persistent: catch only when the handler can actually *remedy* the problem (refresh an expired token, skip a 400 on an endpoint that does not exist for this tenant). Timeouts and 5xx crash — recovery is infrastructure: orchestrator restart, changelog replay, transaction abort. Never catch-and-skip a data error; that is silent data loss.

## Where State Lives

State lives in ephemeral RocksDB instances backed by a compacted Kafka changelog topic. Because all durable state lives in Kafka — input topics, output topics, and changelogs — a killed pod restarts, replays its changelog into a fresh RocksDB, and resumes exactly where it left off. There are no PVCs to snapshot and no opaque local state directories to copy.

The same property that makes pods disposable in production makes them reproducible on a laptop: mirror the relevant topics and committed consumer-group offsets into a local Kafka cluster, and a locally-run stage replays the changelog into a fresh RocksDB and resumes exactly where its production counterpart left off. Frameworks that hide state in framework-managed local stores cannot offer this cleanly; the Kafka Streams model can, and Flechtwerk inherits it. It is a property of the model, not a feature of the framework — the small mirror script lives in application code.

## Why Flechtwerk Exists

Existing Python options each fail one of the constraints that matter for I/O-bound, transactional, multi-instance stream processing:

- **Faust**: stateful, but RocksDB + multi-instance recovery is fragile, and "exactly-once" is idempotent-producer-plus-careful-offsets rather than real Kafka transactions spanning state and output.
- **Quix Streams**: pleasant API, but the core loop is synchronous — fatal for workloads driven by concurrent async I/O (HTTP polling, MQTT subscriptions, etc.).
- **Bytewax**: a Rust dataflow engine with Python bindings; excellent for CPU-bound partitioned dataflow, awkward for async I/O and heavier than the operational model needs.
- **Apache Beam (on Flink)**: the Python SDK runs in a separate worker process and shuttles data to JVM operators over gRPC via the Beam portability framework. Setup is a maze of portable runners and SDK harnesses; failures span two runtimes and produce errors that are hard to localize. If Flink is the right answer, Java or Scala is a saner way to reach it.

Flechtwerk assumes modern Python, `asyncio`, and `aiokafka` are the right primitives and builds directly on them.

## What's Deliberately Not Here

- **Event-time windowing with watermarks** — if you need this, use Flink. Flechtwerk targets the much larger class of problems where processing-time semantics are sufficient.
- **Stream–stream joins, complex topologies** — operators compose by writing to and reading from intermediate Kafka topics, not by chaining method calls.
- **Savepoints / state migrations** — recovery is changelog replay; schema evolution is the application's responsibility.
- **A DSL** — the stream-processing vocabulary is `Extractor`, `Transformer`, `Message`, `State`, `Event`, `Config`, `ConfigStore`, plus the typed-record handles of `flechtwerk.attribute`. That's it.
