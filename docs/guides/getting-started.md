# Getting Started

This guide takes you from an empty environment to a running stage: install
Flechtwerk, learn the one contract every stage is built on, and run a complete
stage against a Kafka broker with a single call. Everything is injected by the
caller — the framework reads nothing from the environment.

Flechtwerk has exactly two stage shapes, both built on the same contract:

- an **[Extractor](extractor.md)** brings an external source into Kafka — polling
  it on a timer, or receiving pushed messages with the
  **[MQTT Extractor](mqtt.md)** — at-least-once;
- a **[Transformer](transformer.md)** consumes Kafka topics and publishes derived
  records, with exactly-once delivery.

Read this page once for the basics — the contract and how any stage is run. Each
stage guide then covers only what is specific to its shape, and
**[Best Practices](best-practices.md)** shows how the two shapes work together.

## Prerequisites

- **Python 3.12+.**
- **A running Kafka broker** reachable at the `bootstrap_servers` you pass below — any broker works, and a local single-node cluster is fine for development.
- **The topics your stage needs, created up front.** Which topics depend on the shape — each stage guide lists them. Flechtwerk reads their partition counts at startup, creates the compacted changelog topic for any stateful stage, and (if your broker has topic auto-creation enabled) writes output topics on first use.

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

Both stage shapes apply this contract — an `Extractor` once per poll cycle, a
`Transformer` once per input record. The runner persists a `State` only when it
differs from the current one, so a stage that never yields `State` is stateless
and never opens a RocksDB file.

## Running a Stage

Running a stage is one call, whatever its shape. Below is a complete, runnable
program: a trivial **identity transformer** that forwards every input record to
an output topic unchanged, wired to a broker. All configuration is injected —
nothing is read from the environment.

```python
import asyncio
from collections.abc import AsyncIterator

from flechtwerk import Flechtwerk, IncomingMessage, Message, State, Transformer

async def transform(msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
    yield Message(key=msg.key, topic="my-output", value=msg.value)   # forward unchanged

stage = Transformer.of(input_topics=["my-input"], transform=transform)

async def main() -> None:
    await Flechtwerk.of(
        application_id="my-stage",
        bootstrap_servers="localhost:9092",
        client_id="my-stage-0",          # process identity: unique per instance, stable across restarts
        stage=stage,
    ).run()

if __name__ == "__main__":
    asyncio.run(main())
```

Produce a record to `my-input` and it comes straight back out on `my-output`.
That is the whole program. To do real work, swap in a stage from one of the
guides below — the `Flechtwerk.of(...).run()` call is identical for every shape;
only the `stage` you pass changes. The required knobs:

- **`application_id`** — the stage's application identity: a transformer's Kafka consumer group, and the prefix of its changelog topic and transactional IDs.
- **`bootstrap_servers`** — your broker.
- **`client_id`** — the process identity (see the warning below).
- **`stage`** — the `Extractor` or `Transformer` you've built.

Optional knobs: `compression_type` (defaults to `"zstd"` — JSON compresses ~13×;
pass `None` to disable), `metrics_port` / `metrics_labels`
([Prometheus](observability.md); disabled while `metrics_port` is `0`), `mqtt`
(broker settings, used only by an [MQTT Extractor](mqtt.md)), and
`poll_interval` (a `timedelta` — an [extractor](extractor.md)'s poll cadence,
required for extractors, ignored by transformers). Like `mqtt`, the last two are
shape-specific and may be passed unconditionally. See the
[API reference](../api/index.md) for the full signature.

!!! note "Event Loop"

    Run it on `uvloop` for best throughput. The framework works on stock
    `asyncio` too (and therefore on Windows) — the event loop is the
    application's choice.

!!! warning "`client_id` Is the Process Identity"

    Give each instance a `client_id` that is unique per instance but stable
    across restarts (in Kubernetes, the pod name works well). It anchors the
    transactional producer's fencing and the MQTT session identity.

## Next Steps

- **[Extractors](extractor.md)** — bring an external source into Kafka on a timer.
- **[MQTT Extractors](mqtt.md)** — a push-driven extractor fed over MQTT.
- **[Transformers](transformer.md)** — consume topics and publish derived records, with exactly-once delivery.
- **[Best Practices](best-practices.md)** — pair an extractor with a transformer into a raw-then-refined pipeline you can reprocess without re-ingesting.
- **[Observability](observability.md)** — Prometheus metrics for throughput, transactions, config arrival, and MQTT health.
- **[Typed attributes & records](../concepts/typed-records.md)** — the `Attribute` library that keeps the JSON boundary honest.
- **[Config topics](../concepts/config-topics.md)** — a shared, eventually-consistent lookup table for every instance (Kafka Streams' GlobalKTable).
