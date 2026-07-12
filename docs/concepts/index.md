---
opener: true
title: Concepts
tagline: The operational model Kafka Streams nailed a decade ago, rewoven in async Python.
---

# Concepts

Flechtwerk (German: *interlacing, wickerwork*) is a small async stream processing framework for Kafka. It takes the operational design that Kafka Streams proved in production — consumer groups for partition assignment, compacted changelog topics as the durable state of record, Kafka transactions tying state writes, output messages, and offset commits into one atomic unit — and ports it to modern async Python built on `asyncio` and `aiokafka`. It runs on stock `asyncio` (and therefore on Windows); the event loop is the application's choice, and `uvloop` is the recommended pick for best throughput.

If you have run Kafka Streams, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via transactions, and ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

## Two stage shapes

An application builds stages in one of two shapes. Both express their whole contract as an async generator that yields `Message` (emit an output record) and `State` (persist state for the current key; a falsy `State` tombstones the key).

- **Extractor** — async-polls an external source once per config record per poll cycle, using `State` as its resume cursor. Its only Kafka input is its `config_topics`. The MQTT bridge is a push-driven extractor.
- **Transformer** — consumes partitioned `input_topics`, runs `transform(msg, state)` per record, and publishes to Kafka with exactly-once delivery.

Both are ABCs. Use the `.of(...)` factory for stateless or simply-stateful stages, or subclass directly when you need lifecycle management (HTTP clients, dedup instances) via `__aenter__` / `__aexit__`. Stateless stages simply never yield `State` and never open a RocksDB file.

## The operational model

- **Consumer groups** drive partition assignment and rebalancing — standard Kafka semantics, no custom coordination.
- **Changelog topics** (compacted) are the durable state of record. RocksDB is a local cache rebuilt by replay on startup. Pods are ephemeral; no PVCs.
- **Kafka transactions** span all output messages, all state changelog writes, and offset commits for a single processing batch. Transformer work is split into per-input-partition tasks; each task owns a transactional producer (static transactional ID — EOS-v1 fencing) shared with its changelog state store via DI.
- **Per-batch parallelism by state key.** Within a `getmany()` batch, records are bucketed by state key within each task. Buckets run concurrently via `asyncio.gather` so I/O-bound `transform` calls overlap, while records sharing a key run serially inside their bucket — each one sees the previous one's yielded state. Cross-key ordering is not preserved.

!!! note "Let it crash"
    There is no framework-level retry logic. The line is recoverable vs non-recoverable, not transient vs persistent: catch only when the handler can actually *remedy* the problem (refresh an expired token, skip a 400 on an endpoint that does not exist for this tenant). Timeouts and 5xx crash — recovery is infrastructure: orchestrator restart, changelog replay, transaction abort. Never catch-and-skip a data error; that is silent data loss.

## Where state lives

State lives in ephemeral RocksDB instances backed by a compacted Kafka changelog topic. Because all durable state lives in Kafka — input topics, output topics, and changelogs — a killed pod restarts, replays its changelog into a fresh RocksDB, and resumes exactly where it left off. There are no PVCs to snapshot and no opaque local state directories to copy; the same property that makes pods disposable in production makes them reproducible on a laptop.
