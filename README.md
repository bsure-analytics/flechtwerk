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

If you've run Kafka Streams in production, the model is immediately familiar: stateful operators backed by RocksDB, recovery via changelog replay, exactly-once delivery via real Kafka transactions, ephemeral compute that can be killed and rescheduled freely because all durable state lives in Kafka.

It exists because the existing Python options each miss a constraint that matters for I/O-bound, transactional, multi-instance stream processing — Faust's multi-instance recovery is fragile and its "exactly-once" isn't real transactions; Quix Streams' core loop is synchronous; Bytewax is awkward for async I/O; Beam-on-Flink spans two runtimes. See [Why Flechtwerk exists](https://bsure-analytics.github.io/flechtwerk/concepts/#why-flechtwerk-exists) for the full comparison.

## Installation

```bash
pip install flechtwerk            # or: uv add flechtwerk
pip install "flechtwerk[mqtt]"    # with the MQTT→Kafka bridge (paho-mqtt)
```

Python 3.12+. Runtime dependencies: `aiokafka[zstd]`, `prometheus-client`, `reactor-di`, and `rocksdict`. Run it on `uvloop` for best throughput — the framework works on stock `asyncio` (and therefore on Windows), but the event loop is the application's choice.

## Quickstart

The whole contract is two `yield` statements inside an async generator:

- `yield Message(...)` to emit an output record.
- `yield State(...)` to persist state for the current key — or `yield` a falsy `State` to tombstone it.

That's it. There are no agents, no tables, no DSL, no `@app.topic` decorators, no fluent builders — a stage is an async generator, and every Python developer already knows how to read one.

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

That plus one stage definition is the whole program — point it at any Kafka broker. The same two-yield contract drives an `Extractor` from the other end (polling an external source, with `State` as its resume cursor), and an `MqttExtractor` pushes into the poll loop.

## Learn more

The [documentation](https://bsure-analytics.github.io/flechtwerk/) has the full story:

- **[Typed records](https://bsure-analytics.github.io/flechtwerk/concepts/typed-records/)** — the `flechtwerk.attribute` library that enforces the JSON boundary at the write site, once per field declaration.
- **[Config topics](https://bsure-analytics.github.io/flechtwerk/concepts/config-topics/)** — shared, eventually-consistent lookup tables (Kafka Streams' GlobalKTable, specialized to configuration).
- **[Getting started](https://bsure-analytics.github.io/flechtwerk/guides/getting-started/)** — install, a minimal transformer and extractor, and running a stage.
- **[MQTT Extractor](https://bsure-analytics.github.io/flechtwerk/guides/mqtt/)** — a push-driven MQTT source that ACKs to the broker only once a batch is durable in Kafka.
- **[Concepts](https://bsure-analytics.github.io/flechtwerk/concepts/)** — the operational model, the exactly-once task model, and the hexagonal architecture.
- **[API reference](https://bsure-analytics.github.io/flechtwerk/api/)** — generated from the source docstrings.

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
