# Transformers — Stream-to-Stream Processing

A `Transformer` consumes one or more Kafka input topics and publishes derived
records to another — the workhorse for stream-to-stream processing. It applies
the [two-yield contract](getting-started.md#the-two-yield-contract) once per input
record: `yield Message(...)` to emit, `yield State(...)` to remember.

New to Flechtwerk? Read [Getting Started](getting-started.md) first for the
install, the contract, and how any stage is run — this guide covers only what is
specific to transformers.

## What Transformers Are For

Typical jobs are enriching records with extra fields, filtering or reshaping them,
aggregating a stream (running counts, sessionization, deduplication), or joining a
stream against a [config table](../concepts/config-topics.md). The minimal example
below does a small aggregation — a per-key running count.

!!! note "Exactly-Once Delivery for Free"

    Every transformer gets exactly-once delivery for free — no opt-in, no extra
    configuration, no transaction code to write: the output records, state changes,
    and input-offset commits of each batch all ride a single Kafka transaction, so
    a record is never lost and never double-counted — even across restarts and
    rebalances. You write the per-record logic; the framework owns the transaction.
    See [Exactly-once delivery](../concepts/exactly-once.md) for the full mechanism.
    (Free *for transformers* — an [extractor](extractor.md) can't have it.)

## Prerequisites

Beyond the [Getting Started prerequisites](getting-started.md#prerequisites), a
transformer needs its topics in place:

- **Your input topics, created before startup.** Flechtwerk reads each input topic's partition count at startup (so they must already exist) and creates a matching compacted changelog topic for any state you keep. All input topics of one transformer must have equal partition counts.
- **Output topics** are created on first write only if your broker has topic auto-creation enabled — otherwise create them too.

## A Minimal Transformer

The example below counts how many events each key has produced, stamps that
count onto every outgoing record, and remembers it as per-key state. Build the
stage by decorating a `transform` function with `@transformer(...)` — no
subclass needed.

```python
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, transformer
from flechtwerk.attribute import Attribute, DATETIME, INT

SEEN = Attribute("seen", INT)
"""How many events this key has produced so far."""
TIMESTAMP = Attribute("timestamp", DATETIME)
"""When the event happened at the source."""

@transformer(input_topics=["my-input"])
async def transform(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    seen = (state.get(SEEN) or 0) + 1
    yield Message(key=msg.key, topic="my-output", value=Event({**msg.value, SEEN: seen}))
    yield State({SEEN: seen, TIMESTAMP: msg.value[TIMESTAMP]})
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
    mutates its input. See [Typed attributes & records](../concepts/typed-attributes.md).

!!! tip "Decorator, Factory, or Subclass?"

    `Transformer` and `Extractor` are ABCs. Decorate a function with
    `@transformer(...)` / `@extractor(...)` — or call the `.of(...)` factory it
    wraps — for stateless or simply-stateful stages. Subclass directly when you
    need lifecycle management (HTTP clients, dedup instances, etc.) via
    `__aenter__` / `__aexit__`.

## Running It

Build the stage as above and run it with the single `Flechtwerk.of(...).run()`
call from [Getting Started → Running a Stage](getting-started.md#running-a-stage).
A transformer takes no stage-specific run parameters — pass the decorated
`transform` as the `stage` and you're done:

```python
await Flechtwerk.of(
    application_id="my-transformer",
    bootstrap_servers="localhost:9092",
    client_id="my-transformer-0",
    stage=transform,                     # the decorated transform above
).run()
```

## Next Steps

- **[Extractors](extractor.md)** — the same contract from the other end: bring an external source into Kafka.
- **[Best Practices](best-practices.md)** — pair a transformer with an extractor so you can reprocess without re-ingesting.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — how the per-batch transaction ties output, state, and offsets together.
- **[Typed attributes & records](../concepts/typed-attributes.md)** — the `Attribute` library behind `Event`, `State`, and `Config`.
