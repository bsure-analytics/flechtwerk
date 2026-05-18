# Fretworx

An async Python port of the Kafka Streams operational model.

## What it is

Fretworx is a small async stream processing framework for Kafka. It takes the operational design that Kafka Streams nailed a decade ago — consumer groups for partition assignment, compacted changelog topics as the durable state of record, Kafka transactions tying state writes, output messages, and offset commits into a single atomic unit — and ports it to modern async Python.

If you've run Kafka Streams in production, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via transactions, ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

## Why it exists

Existing Python options each fail one of the constraints that matter for I/O-bound, transactional, multi-instance stream processing:

- **Faust**: stateful but RocksDB + multi-instance recovery is fragile, and "exactly-once" is idempotent-producer-plus-careful-offsets rather than real Kafka transactions spanning state and output.
- **Quix Streams**: pleasant API, but the core loop is synchronous — fatal for workloads driven by concurrent async I/O (HTTP polling, MQTT subscriptions, etc.).
- **Bytewax**: a Rust dataflow engine with Python bindings; excellent for CPU-bound partitioned dataflow, awkward for async I/O and heavier than the operational model needs.
- **Apache Beam (on Flink)**: the Python SDK runs in a separate worker process and shuttles data to JVM operators over gRPC via the Beam portability framework. Setup is a maze of portable runners and SDK harnesses; failures span two runtimes and produce errors that are hard to localize. If Flink is the right answer, Java or Scala is a saner way to reach it.

Fretworx assumes Python 3.14, `asyncio`, `aiokafka`, and `uvloop` are the right primitives and builds directly on them.

## The API

The whole contract is two `yield` statements inside an async generator:

- `yield Message(...)` to emit an output record.
- `yield State(...)` to persist state for the current key.
- `yield <falsy State>` to tombstone the key.

That's it. There are no agents, no tables, no DSL, no `@app.topic` decorators, no fluent builders. An async generator is already the right shape — pull-based, backpressure-friendly, naturally composable — and every Python developer already knows how to read one.

```python
async def transform(message, state):
    event = enrich(message.event, state)
    yield Message(topic="output", key=message.key, event=event)
    yield State({LAST_SEEN: event.timestamp})
```

Stateless stages simply never yield `State` and never open a RocksDB file.

## Operational model

- **Consumer groups** drive partition assignment and rebalancing — standard Kafka semantics, no custom coordination.
- **Changelog topics** (compacted) are the durable state of record. RocksDB is a local cache rebuilt by replay on startup. Pods are ephemeral; no PVCs.
- **Kafka transactions** span all output messages, all state changelog writes, and offset commits for a single processing batch. A single transactional producer is shared between the runner and the changelog state store via DI — closing the gap that lets duplicates leak in frameworks that treat these as separate concerns.
- **"Let it crash"** error strategy: transient failures propagate. Recovery is infrastructure (Kubernetes restart, changelog replay, transaction abort) rather than in-process retry loops.

## Architecture

Hexagonal: ports and adapters. The core (`Extractor`, `Transformer`, their runners) depends only on abstract ports (`StateStore`, `Observer`, `aiokafka` consumer/producer interfaces). Adapters (`RocksDBStateStore`, `ChangelogStateStore`, `PrometheusObserver`, `FakeKafkaConsumer`/`FakeKafkaProducer` for tests) plug in via DI through `reactor-di`.

The framework has no CLI, no module-level `os.getenv`, no `load_dotenv`, and no opinions about how applications are packaged or deployed. All configuration is injected by the caller.

## Local development falls out for free

Because all durable state lives in Kafka — input topics, output topics, and compacted changelog topics — replicating production state on a developer laptop is just a matter of mirroring the relevant topics and committed consumer-group offsets into a local Kafka cluster. A locally-run stage then starts up against the local cluster, replays the changelog into a fresh RocksDB, and resumes processing exactly where its production counterpart left off.

No PVCs to snapshot, no opaque local state directories to copy, no VPN into a shared dev cluster. The same property that makes Kubernetes pods disposable in production makes them reproducible on a laptop. Frameworks that hide state in framework-managed local stores cannot offer this cleanly; the Kafka Streams model can, and Fretworx inherits it.

This is a property of the model, not a feature of the framework — building a small mirror script is straightforward and lives in application code.

## What's deliberately not here

- **Event-time windowing with watermarks** — if you need this, use Flink. Fretworx targets the much larger class of problems where processing-time semantics are sufficient.
- **Stream-stream joins, complex topologies** — operators compose by writing to and reading from intermediate Kafka topics, not by chaining method calls.
- **Savepoints / state migrations** — recovery is changelog replay; schema evolution is the application's responsibility.
- **A DSL** — the API surface is `Extractor`, `Transformer`, `Message`, `State`, `Event`, `Config`. That's the full vocabulary.

## Status

Fretworx is currently developed inside a host application repository. It is designed to be extracted into its own package: no references to application paths, modules, or environment variables exist in framework code.
