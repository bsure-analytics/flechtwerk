---
opener: true
title: Flechtwerk
tagline: Truly async Kafka stream processing for modern Python — exactly-once, ephemeral, and immediately familiar.
---

# Flechtwerk

Flechtwerk (German: *interlacing, wickerwork*) is a small async stream processing framework for Kafka. It takes the operational design that Kafka Streams nailed a decade ago — consumer groups for partition assignment, compacted changelog topics as the durable state of record, Kafka transactions tying state writes, output messages, and offset commits into a single atomic unit — and ports it to modern async Python.

If you've run Kafka Streams in production, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via real Kafka transactions, ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

## The whole contract is two yields

There are no agents, no tables, no DSL, no `@app.topic` decorators, no fluent builders. A stage is an async generator, and the entire contract is two `yield` statements:

- `yield Message(...)` to emit an output record.
- `yield State(...)` to persist state for the current key — or `yield` a falsy `State` to tombstone the key.

An async generator is already the right shape — pull-based, backpressure-friendly, naturally composable — and every Python developer already knows how to read one.

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

That plus one `Flechtwerk.of(...).run()` call is the whole program — point it at any Kafka broker.

!!! tip "Two shapes, one contract"

    An `Extractor` is the same two-yield contract driven from the other end: `poll(config, state)` pulls from an external source once per config record per poll cycle and uses `State` as its resume cursor. A `Transformer` consumes partitioned input topics and publishes with exactly-once delivery. Both are ABCs — reach for the `.of(...)` factory for stateless or simply-stateful stages, or subclass directly when you need lifecycle management. Stateless stages simply never yield `State` and never open a RocksDB file.

## Where to next

From here, dig into the typed-record boundary that keeps the JSON wire honest, the task model behind exactly-once delivery, config topics as shared lookup tables, and the MQTT bridge that ACKs only once Kafka has the data.
