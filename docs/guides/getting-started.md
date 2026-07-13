# Getting Started

This guide takes you from an empty environment to a working stream processor:
install Flechtwerk, write a minimal `Transformer`, and run it against a Kafka
broker with a single call. Everything is injected by the caller — the framework
reads nothing from the environment.

A `Transformer` is the workhorse for **stream-to-stream** processing: it consumes
one or more input topics and publishes derived records to another. Typical jobs
are enriching records with extra fields, filtering or reshaping them, aggregating
a stream (running counts, sessionization, deduplication), or joining a stream
against a [config table](../concepts/config-topics.md). The minimal example below
does a small aggregation — a per-key running count.

!!! note "Exactly-Once Delivery for Free"

    Every transformer gets exactly-once delivery for free — no opt-in, no extra
    configuration, no transaction code to write: the output records, state changes,
    and input-offset commits of each batch all ride a single Kafka transaction, so
    a record is never lost and never double-counted — even across restarts and
    rebalances. You write the per-record logic; the framework owns the transaction.
    See [Exactly-once delivery](../concepts/exactly-once.md) for the full mechanism.
    (Free *for transformers* — an [extractor](extractor.md) can't have it; see
    below on that page.)

## Prerequisites

- **Python 3.12+.**
- **A running Kafka broker** reachable at the `bootstrap_servers` you pass below — any broker works, and a local single-node cluster is fine for development.
- **Your input and output topics created.** At startup Flechtwerk reads each input topic's partition count (so the input topics must already exist) and creates a matching compacted changelog topic for you. Output topics are created on first write only if your broker has topic auto-creation enabled — otherwise create them too.

## Installation

```bash
pip install flechtwerk            # or: uv add flechtwerk
pip install "flechtwerk[mqtt]"    # with the MQTT→Kafka bridge (paho-mqtt)
```

Flechtwerk requires **Python 3.12+**. Its runtime dependencies are
`aiokafka[zstd]`, `prometheus-client`, `reactor-di`, and `rocksdict`.

!!! tip "The `mqtt` Extra Is Optional"

    Install `flechtwerk[mqtt]` only if you plan to build a push-driven source
    with the MQTT→Kafka bridge. It pulls in `paho-mqtt`, which stays confined to
    `flechtwerk.mqtt` — a plain `import flechtwerk` never loads it.

## The Two-Yield Contract

The whole contract is two `yield` statements inside an async generator:

- `yield Message(...)` to emit an output record.
- `yield State(...)` to persist state for the current key.
    - `yield <falsy State>` to tombstone the key.

That's it. There are no agents, no tables, no DSL, no `@app.topic`
decorators, and no fluent builders. An async generator is already the right
shape — pull-based, backpressure-friendly, naturally composable — and every
Python developer already knows how to read one.

A `Transformer` consumes partitioned input topics and applies this contract per
record; the runner only persists a `State` when it differs from the current one,
so a stage that never yields `State` is stateless and never opens a RocksDB
file.

## A Minimal Transformer

The example below counts how many events each key has produced, stamps that
count onto every outgoing record, and remembers it as per-key state. Build the
stage with the `Transformer.of(...)` factory — no subclass needed.

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

What each yield does here:

- `yield Message(...)` emits an output record. `Message` is a frozen dataclass
  envelope carrying a `key`, a `topic`, an `Event` value, and an optional
  timestamp.
- `yield State(...)` persists the running count for `msg.key`. On the next
  record for that key, `state.get(SEEN)` reads it back.

!!! note "Parameters are read-only"

    `msg` and `state` are yours to read, not to change: the runner hands
    `transform` a private copy of `state` and discards any in-place edit, so the
    only way to emit a record or persist state is to `yield`. Enrich by
    spreading (`Event({**msg.value, SEEN: seen})`), never by mutating in place.

!!! note "Typed Records, Not Bare Dicts"

    `SEEN` and `TIMESTAMP` are `Attribute` handles: each pairs a wire name with
    an explicit codec (`INT`, `DATETIME`, …) so the JSON boundary is enforced at
    assignment time. `Event`, `State`, and `Config` are dict-like `Record`
    containers indexed by these handles rather than string keys. Records spread
    like dicts — `Event({**msg.value, SEEN: seen})` — so enrichment never
    mutates its input.

!!! tip "Factory or Subclass?"

    `Transformer` and `Extractor` are ABCs. Use the `.of(...)` factory for
    stateless or simply-stateful stages. Subclass directly when you need
    lifecycle management (HTTP clients, dedup instances, etc.) via `__aenter__`
    / `__aexit__`.

## Running It

Running a stage is one call. All configuration is injected — nothing is read
from the environment:

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

This plus one stage definition is the whole program — point it at any Kafka
broker. Produce a record to `my-input` and it comes straight back out on
`my-output` with a running `seen` count attached.

The same `Flechtwerk.of(...).run()` call runs *any* stage — swap the transformer
for an [Extractor](extractor.md) and nothing about the invocation changes.

!!! note "Event Loop"

    Run it on `uvloop` for best throughput. The framework works on stock
    `asyncio` too (and therefore on Windows) — the event loop is the
    application's choice.

!!! warning "`client_id` Is the Process Identity"

    Give each instance a `client_id` that is unique per instance but stable
    across restarts (in Kubernetes, the pod name works well). It anchors the
    transactional producer's fencing and the MQTT session identity.

## Next Steps

- **[Extractor](extractor.md)** — the same two-yield contract driven from the other end: poll an external source on a timer instead of consuming an input topic.
- **[Typed records](../concepts/typed-records.md)** — the `Attribute` library that keeps the JSON boundary honest.
- **[Config topics](../concepts/config-topics.md)** — a shared, eventually-consistent lookup table for every instance (Kafka Streams' GlobalKTable).
- **[MQTT Extractor](mqtt.md)** — push into the poll loop with `MqttExtractor.of(...)`, ACKing to the broker only once a batch is durable in Kafka.
