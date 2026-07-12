# Flechtwerk

<div align="center">
  <img src="assets/flechtwerk-ornament.svg" alt="Flechtwerk — Celtic interlace" width="100%" height="60">
  <a href="https://bsure-analytics.github.io/flechtwerk/"><img src="https://img.shields.io/badge/docs-online-6d2530" alt="Documentation"></a>
  <a href="https://github.com/bsure-analytics/flechtwerk/actions/workflows/ci.yaml"><img src="https://github.com/bsure-analytics/flechtwerk/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/bsure-analytics/flechtwerk"><img src="https://codecov.io/gh/bsure-analytics/flechtwerk/branch/main/graph/badge.svg" alt="Coverage Status"></a>
  <a href="https://pypi.org/project/flechtwerk/"><img src="https://img.shields.io/pypi/v/flechtwerk.svg" alt="PyPI version"></a>
  <a href="https://pypi.org/project/flechtwerk/"><img src="https://img.shields.io/pypi/pyversions/flechtwerk.svg" alt="Python versions"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <img src="assets/flechtwerk-ornament.svg" alt="Flechtwerk — Celtic interlace" width="100%" height="60">
</div>

Truly async Python stream processing with real Kafka transactions for exactly-once delivery, and an MQTT→Kafka bridge that ACKs only after Kafka has the data.

📖 **Documentation: [bsure-analytics.github.io/flechtwerk](https://bsure-analytics.github.io/flechtwerk/)** — guides, concepts, and the full API reference.

## What it is

Flechtwerk (German: *interlacing, wickerwork*) is a small async stream processing framework for Kafka. It takes the operational design that Kafka Streams nailed a decade ago — consumer groups for partition assignment, compacted changelog topics as the durable state of record, Kafka transactions tying state writes, output messages, and offset commits into a single atomic unit — and ports it to modern async Python.

If you've run Kafka Streams in production, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via transactions, ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

## Why it exists

Existing Python options each fail one of the constraints that matter for I/O-bound, transactional, multi-instance stream processing:

- **Faust**: stateful but RocksDB + multi-instance recovery is fragile, and "exactly-once" is idempotent-producer-plus-careful-offsets rather than real Kafka transactions spanning state and output.
- **Quix Streams**: pleasant API, but the core loop is synchronous — fatal for workloads driven by concurrent async I/O (HTTP polling, MQTT subscriptions, etc.).
- **Bytewax**: a Rust dataflow engine with Python bindings; excellent for CPU-bound partitioned dataflow, awkward for async I/O and heavier than the operational model needs.
- **Apache Beam (on Flink)**: the Python SDK runs in a separate worker process and shuttles data to JVM operators over gRPC via the Beam portability framework. Setup is a maze of portable runners and SDK harnesses; failures span two runtimes and produce errors that are hard to localize. If Flink is the right answer, Java or Scala is a saner way to reach it.

Flechtwerk assumes modern Python, `asyncio`, `aiokafka`, and `uvloop` are the right primitives and builds directly on them.

## Installation

```bash
pip install flechtwerk            # or: uv add flechtwerk
pip install "flechtwerk[mqtt]"    # with the MQTT→Kafka bridge (paho-mqtt)
```

Python 3.12+. Runtime dependencies: `aiokafka[zstd]`, `prometheus-client`, `reactor-di`, and `rocksdict`. Run it on `uvloop` for best throughput — the framework works on stock `asyncio` (and therefore on Windows), but the event loop is the application's choice.

## The API

The whole contract is two `yield` statements inside an async generator:

- `yield Message(...)` to emit an output record.
- `yield State(...)` to persist state for the current key.
  - `yield <falsy State>` to tombstone the key.

That's it. There are no agents, no tables, no DSL, no `@app.topic` decorators, no fluent builders. An async generator is already the right shape — pull-based, backpressure-friendly, naturally composable — and every Python developer already knows how to read one.

```python
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, Transformer
from flechtwerk.attribute import Attribute, DATETIME, INT

SEEN = Attribute("seen", INT)
"""How many events this key has produced so far."""
TIMESTAMP = Attribute("timestamp", DATETIME)
"""When the event happened at the source."""

async def transform(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    seen = (state.get(SEEN) or 0) + 1
    yield Message(key=msg.key, topic="my-output", value=Event({**msg.value, SEEN: seen}))
    yield State({SEEN: seen, TIMESTAMP: msg.value[TIMESTAMP]})

stage = Transformer.of(input_topics=["my-input"], transform=transform)
```

The typed `Attribute` handles indexing those records are explained in [Typed records, not bare dicts](#typed-records-not-bare-dicts) below.

An `Extractor` is the same two-yield contract driven from the other end: `poll(config, state)` runs once per config record per poll cycle, pulls from the external source, and uses `State` as its resume cursor:

```python
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Attribute, DATETIME, INT, STR

CYCLE = Attribute("cycle", INT)
"""Resume cursor — stands in for whatever your source pages by."""
NAME = Attribute("name", STR)
POLLED_AT = Attribute("polled_at", DATETIME)

async def poll(config: Config, state: State) -> AsyncIterator[Message | State]:
    cycle = (state.get(CYCLE) or 0) + 1               # your API call goes here
    yield Message(
        key=config[NAME],
        topic="my-extract",
        value=Event({CYCLE: cycle, POLLED_AT: datetime.now(timezone.utc)}),
    )
    yield State({CYCLE: cycle})

stage = Extractor.of(config_topics=["my-config"], poll=poll)
```

`Extractor` and `Transformer` are ABCs. Use the `.of(...)` factory for stateless or simply-stateful stages, or subclass directly when you need lifecycle management (HTTP clients, dedup instances, etc.) via `__aenter__` / `__aexit__`. Stateless stages simply never yield `State` and never open a RocksDB file.

Running a stage is one call — all configuration is injected, nothing is read from the environment:

```python
import asyncio

from flechtwerk import Flechtwerk

async def main() -> None:
    await Flechtwerk.of(
        application_id="my-transformer",
        bootstrap_servers="localhost:9092",
        client_id="my-transformer-0",   # process identity: unique per instance, stable across restarts
        poll_interval_seconds=60,
        stage=stage,                    # from above
    ).run()

if __name__ == "__main__":
    asyncio.run(main())
```

This plus one stage definition from above is the whole program — point it at any Kafka broker.

### Typed records, not bare dicts

You met `Attribute` above. It exists because a stream processor lives on the JSON boundary: every input is a dict decoded from the wire, and every output and every state write goes back through `json.dumps`. Handled as bare dicts, that boundary leaks into everything — each read re-checks presence and re-parses timestamps, a `datetime` assigned three hops earlier blows up only when the record is finally serialized, and a field that silently became `null` surfaces as a `KeyError` in some consumer far from the code that dropped it.

The `flechtwerk.attribute` library moves all of that to the write site. Each field is declared exactly once, as a typed handle pairing a wire name with an explicit `Codec[V]`:

```python
from datetime import datetime, timezone

from flechtwerk import Event
from flechtwerk.attribute import Attribute, DATETIME, LIST, STR

DEVICE = Attribute("device", STR)
LAST_SEEN = Attribute("last_seen", DATETIME)
TAGS = Attribute("tags", LIST(STR), optional=True)

event = Event({
    DEVICE: "sensor-1",
    LAST_SEEN: datetime(2026, 7, 12, 9, 30, tzinfo=timezone.utc),
    TAGS: ["a", "b"],
})

event[LAST_SEEN]   # datetime(2026, 7, 12, 9, 30, tzinfo=timezone.utc) — a real datetime
event.raw          # {'device': 'sensor-1', 'last_seen': '2026-07-12T09:30:00Z', 'tags': ['a', 'b']}

# Both of these raise at the write site, not at serialization time:
#   event[DEVICE] = 42     # expected str, got int
#   event[DEVICE] = None   # cannot assign None to required Attribute('device')
```

`Event`, `State`, and `Config` are `Record` subclasses — dict-like containers indexed by these handles rather than string keys. (`Message` is a frozen dataclass envelope carrying a key, topic, `Event` value, and optional timestamp.) The codec runs on **every write**, so the underlying `.raw` payload stays JSON-native by construction: wire encoding is a straight `json.dumps(event.raw)`, decoding is a straight `Event.wrap(raw)`, and nothing in between ever needs to re-validate. A required attribute (the default) rejects `None` so a dropped value can't silently land as JSON `null`; declare fields where absence is legal with `optional=True`. The read distinction is carried by the method, not the declaration — `event[LAST_SEEN]` reads-or-raises, `event.get(TAGS)` tolerates absence and returns `V | None`.

Codecs compose: atoms (`STR`, `INT`, `BOOL`, `DATE`, `FLOAT`, `DATETIME`, `TIME`, `RECORD`, `ANY`) plus constructors (`LIST(V)`, `SET(V)`, `TUPLE(V)`, `DICT(V)`). And records spread like dicts — `Event({**event, LAST_SEEN: later})` — so enrichment never has to mutate its input.

The point isn't ceremony; it's that the boundary between "Python object graph" and "JSON on the wire" is enforced at assignment time, once per Attribute declaration, rather than re-derived on every serialize/deserialize.

### Config topics — shared lookup tables

A stage declares two kinds of topics. `input_topics` (transformers only) are partitioned: their records drive `transform()` and define the task model. `config_topics` are read **in full by every instance** into one per-process `ConfigStore` keyed by wire key — Kafka Streams' GlobalKTable, specialized to configuration:

```python
from collections.abc import AsyncIterator

from flechtwerk import Extractor, IncomingMessage, Message, State, Transformer

class MyExtractor(Extractor):
    config_topics = ["my-config"]          # an extractor's inputs ARE config topics
    ...                                    # plus your poll()

class RequestDriven(Transformer):
    input_topics = ["my-requests"]         # partitioned, keyed stream
    config_topics = ["my-config"]          # config table, joined by key

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        config = self.configs.get(msg.key)  # eventually consistent lookup
        if config is None:
            return                          # no config for this key (yet)
        yield Message(key=msg.key, topic="my-results", value=msg.value)

stage = RequestDriven()
```

For extractors this is not an extra mechanism but the baseline: config topics are the only Kafka input an extractor has. For transformers it is the escape hatch from the co-partitioning requirement: a config topic's partition placement and count are irrelevant, so any producer (Kafka UI included) can write configs without routing them to the "right" partition. The source topics are their own changelog — compacted, small, re-read on every startup — and lookups are eventually consistent, outside the task transaction (the GlobalKTable caveat).

`Stage.enrich(config)` hooks one-time derivation (e.g. an API lookup) into the config path: the framework applies it once per config record — never per poll tick or lookup — and both stage kinds inherit it. Kafka Streams forbids transforming records on their way into a global store (KIP-813) because a checkpoint-based restore would bypass the transformation; Flechtwerk re-reads the topics through the same enrich path on every startup, so the enriched store cannot diverge.

### MQTT sources — push into the poll loop

`flechtwerk.mqtt` bridges a push-driven MQTT source into the extractor model out of the box. The framework owns everything protocol-shaped — one shared paho connection per process driven by the asyncio event loop (no threads), persistent sessions with a stable client id, manual-ACK at-least-once (a batch is ACKed to the broker only once it is provably durable in Kafka — at the top of the next poll, per the runner's re-entry contract), per-topic subscriptions fed by config records, an arrival wakeup so delivery latency is sub-second rather than poll-interval-bound, and Prometheus metrics. An application writes one pure function:

```python
from datetime import datetime, timezone

from flechtwerk import Config, Event, Message
from flechtwerk.attribute import Attribute, DATETIME, RECORD, Record, STR
from flechtwerk.mqtt import MqttExtractor

DATA = Attribute("data", RECORD)
DEVICE_ID = Attribute("device_id", STR)
PROCESSING_TIME = Attribute("processing_time", DATETIME)

def relay(config: Config, topic: str, payload: Record) -> Message | None:
    return Message(
        key=payload[DEVICE_ID],             # missing → the framework poison-drops
        topic="my-extract",
        value=Event({DATA: payload, PROCESSING_TIME: datetime.now(timezone.utc)}),
    )

stage = MqttExtractor.of(config_topics=["my-config"], relay=relay)
```

Return a `Message` to forward, `None` to drop (ACKed immediately), or raise to poison-drop (logged, ACKed, counted — never a crash loop on a broken payload). Sources that don't fit the one-in-at-most-one-out shape override `poll()`; the connection layer works without the template. Broker settings are injected via `Flechtwerk.of(mqtt=MqttBrokerConfig(...))`, and paho stays confined to `flechtwerk.mqtt` — `import flechtwerk` never loads it, and the dependency ships as the optional `flechtwerk[mqtt]` extra.

## Operational model

- **Consumer groups** drive partition assignment and rebalancing — standard Kafka semantics, no custom coordination.
- **Changelog topics** (compacted) are the durable state of record. RocksDB is a local cache rebuilt by replay on startup. Pods are ephemeral; no PVCs.
- **Kafka transactions** span all output messages, all state changelog writes, and offset commits for a single processing batch. Transformer work is split into per-input-partition tasks; each task owns a transactional producer (static transactional ID — EOS-v1 fencing) shared with its changelog state store via DI — closing the gap that lets duplicates leak in frameworks that treat these as separate concerns.
- **Per-batch parallelism by state key.** Within a `getmany()` batch, records are bucketed by state key within each task. Buckets run concurrently via `asyncio.gather` so I/O-bound `transform` calls overlap, while records sharing a key run serially inside their bucket — each one sees the previous one's yielded state. Each `transform` call receives a defensive deepcopy of the running state, so in-place mutation without a `yield State(...)` can't leak. Cross-key ordering is not preserved; within a bucket, records appear in `input_topics` order then Kafka offset order.
- **"Let it crash"** error strategy. The line is recoverable vs non-recoverable, not transient vs persistent: catch only when the handler can actually *remedy* the problem (refresh an expired token, skip a 400 on an endpoint that doesn't exist for this tenant). Timeouts and 5xx crash — sleeping and retrying in-process is reimplementing `CrashLoopBackOff` poorly. Never catch-and-skip data errors; that's silent data loss. Recovery is infrastructure: Kubernetes restart, changelog replay, transaction abort.

## Architecture

Hexagonal: ports and adapters. The core (`Extractor`, `Transformer`, their runners) depends only on abstract ports (`StateStore`, `Observer`, `aiokafka` consumer/producer interfaces). Adapters (`RocksDBStateStore`, `ChangelogStateStore`, `PrometheusObserver`, the `flechtwerk.mqtt` transport, `FakeKafkaConsumer`/`FakeKafkaProducer`/`FakeMqttConnection` for tests) plug in via DI through `reactor-di`. Transport adapters earn a place in the framework only when their correctness depends on runner delivery semantics — MQTT's manual-ACK protocol does; a plain HTTP poller does not.

The framework has no CLI, no module-level `os.getenv`, no `load_dotenv`, and no opinions about how applications are packaged or deployed. All configuration is injected by the caller.

## Local development falls out for free

Because all durable state lives in Kafka — input topics, output topics, and compacted changelog topics — replicating production state on a developer laptop is just a matter of mirroring the relevant topics and committed consumer-group offsets into a local Kafka cluster. A locally-run stage then starts up against the local cluster, replays the changelog into a fresh RocksDB, and resumes processing exactly where its production counterpart left off.

No PVCs to snapshot, no opaque local state directories to copy, no VPN into a shared dev cluster. The same property that makes Kubernetes pods disposable in production makes them reproducible on a laptop. Frameworks that hide state in framework-managed local stores cannot offer this cleanly; the Kafka Streams model can, and Flechtwerk inherits it.

This is a property of the model, not a feature of the framework — building a small mirror script is straightforward and lives in application code.

## What's deliberately not here

- **Event-time windowing with watermarks** — if you need this, use Flink. Flechtwerk targets the much larger class of problems where processing-time semantics are sufficient.
- **Stream-stream joins, complex topologies** — operators compose by writing to and reading from intermediate Kafka topics, not by chaining method calls.
- **Savepoints / state migrations** — recovery is changelog replay; schema evolution is the application's responsibility.
- **A DSL** — the stream-processing vocabulary is `Extractor`, `Transformer`, `Message`, `State`, `Event`, `Config`, `ConfigStore`, plus the typed-record handles of `flechtwerk.attribute`. That's it.

## Development

```bash
uv sync                        # venv + all dependencies
uv run pytest                  # unit tier — Docker-free
uv run pytest -m integration   # integration tier — ephemeral Kafka/Mosquitto via testcontainers
uv run coverage run -m pytest -m "integration or not integration" && uv run coverage report
```

These commands are also exposed as [poe](https://poethepoet.natn.io/) tasks — `uv run poe test | cov | build | docs | docs-build`. The documentation site is built with MkDocs Material; `uv run poe docs` serves it locally with live reload, and it deploys to [GitHub Pages](https://bsure-analytics.github.io/flechtwerk/) on every push to `main`.

Releases are cut by tagging: pushing a `vX.Y.Z` tag runs the test suite, builds the package with the tag-derived version (hatch-vcs), and publishes it to PyPI via trusted publishing.

## Status

Flechtwerk was extracted from the data integration platform it was developed in, where it runs every stage in production. The API is small and settled in shape, but pre-1.0 — minor releases may still move things around. One design rule carries over from its origin as an embedded framework: framework code references no application paths, modules, or environment variables; all configuration is injected by the caller.
