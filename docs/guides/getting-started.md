# Getting started

This guide takes you from an empty environment to a working stream processor:
install Flechtwerk, write a minimal `Transformer`, and run it against a Kafka
broker with a single call. Everything is injected by the caller — the framework
reads nothing from the environment.

## Installation

```bash
pip install flechtwerk            # or: uv add flechtwerk
pip install "flechtwerk[mqtt]"    # with the MQTT→Kafka bridge (paho-mqtt)
```

Flechtwerk requires **Python 3.12+**. Its runtime dependencies are
`aiokafka[zstd]`, `prometheus-client`, `reactor-di`, and `rocksdict`.

!!! tip "The `mqtt` extra is optional"

    Install `flechtwerk[mqtt]` only if you plan to build a push-driven source
    with the MQTT→Kafka bridge. It pulls in `paho-mqtt`, which stays confined to
    `flechtwerk.mqtt` — a plain `import flechtwerk` never loads it.

!!! note "Event loop"

    Run it on `uvloop` for best throughput. The framework works on stock
    `asyncio` too (and therefore on Windows) — the event loop is the
    application's choice.

## The two-yield contract

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

## A minimal transformer

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

!!! note "Typed records, not bare dicts"

    `SEEN` and `TIMESTAMP` are `Attribute` handles: each pairs a wire name with
    an explicit codec (`INT`, `DATETIME`, …) so the JSON boundary is enforced at
    assignment time. `Event`, `State`, and `Config` are dict-like `Record`
    containers indexed by these handles rather than string keys. Records spread
    like dicts — `Event({**msg.value, SEEN: seen})` — so enrichment never
    mutates its input.

!!! tip "Factory or subclass?"

    `Transformer` and `Extractor` are ABCs. Use the `.of(...)` factory for
    stateless or simply-stateful stages. Subclass directly when you need
    lifecycle management (HTTP clients, dedup instances, etc.) via `__aenter__`
    / `__aexit__`.

## Running it

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
broker.

!!! warning "`client_id` is the process identity"

    Give each instance a `client_id` that is unique per instance but stable
    across restarts (in Kubernetes, the pod name works well). It anchors the
    transactional producer's fencing and the MQTT session identity.

## Next steps

- **Extractors** apply the same two-yield contract from the other end:
  `poll(config, state)` runs once per config record per poll cycle, pulls from
  an external source, and uses `State` as its resume cursor. Build one with
  `Extractor.of(config_topics=..., poll=...)`.
- **Config topics** give every instance a shared, eventually-consistent lookup
  table (Kafka Streams' GlobalKTable, specialized to configuration).
- **MQTT sources** push into the poll loop out of the box with
  `MqttExtractor.of(config_topics=..., relay=...)`, ACKing to the broker only
  once a batch is durable in Kafka.
