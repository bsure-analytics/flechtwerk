# Extractors — Poll External Sources Into Kafka

An `Extractor` is the [same two-yield contract](getting-started.md#the-two-yield-contract)
as a [`Transformer`](transformer.md), driven from the other end. A transformer consumes an input
topic and reacts to each record; an extractor has no input topic — it polls an
external source on a timer and turns what it finds into Kafka records. Its only
Kafka input is its `config_topics`: one config record per thing to poll (a
tenant, an endpoint, a device), and `poll(config, state)` runs once per config
record per cycle.

A typical extractor polls a RESTful API: one config record per tenant or
endpoint, a paginated `GET` each cycle, a cursor (page token or high-water
timestamp) kept in `State` so the next cycle resumes where the last one stopped,
and one Kafka record emitted per item. The same shape fits any *pull* source — a
database queried on a schedule, an object store or file drop scanned for new
objects, a paged export endpoint. For *push* sources that arrive on their own
schedule rather than a timer, see the [MQTT Extractor](mqtt.md).

!!! warning "At-Least-Once, Never Exactly-Once"

    Exactly-once is a **transformer-only** guarantee: a transformer gets it for
    free, but an extractor **cannot** have it at any price — it is not a
    framework-wide free lunch. An extractor delivers **at-least-once** because
    polling an external source can't be made atomic with a Kafka transaction, so a
    crash between emitting a record and advancing the cursor re-emits that record
    on restart. Its output is deliberately non-transactional. Design downstream
    consumers to tolerate duplicates — an idempotent key, or a dedup transformer on
    the way in. See [Exactly-once delivery](../concepts/exactly-once.md), which
    spells out why the guarantee stops at the transformer.

Everything else you already know from [Getting Started](getting-started.md) — the
installation, the `yield Message` / `yield State` contract, and the single
`Flechtwerk.of(...).run()` call — carries over unchanged. This guide covers only
what is different about the extractor end.

## Prerequisites

The same [prerequisites as Getting Started](getting-started.md#prerequisites),
with these differences:

- **At least one config topic, created before startup and compacted.** It is the
  extractor's only Kafka input — each surviving record drives one poll target.
  Flechtwerk existence-checks config topics at startup and fails fast if one is
  missing (its assign-based reader would never discover a topic created later).
  Keep the topic compacted and small; it is read in full into memory on every
  boot. See [Config topics](../concepts/config-topics.md).
- **No input topics.** Partitioned input topics belong to transformers; an
  extractor never declares them.
- **Output topics** follow the same auto-creation caveat as Getting Started, and
  Flechtwerk still creates the compacted changelog topic for you if your
  extractor keeps state.

## A Minimal Extractor

The example below polls once per config record, advances a per-key cursor, and
emits one record per cycle. Build it with the `Extractor.of(...)` factory — no
subclass needed.

```python
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from flechtwerk import Config, Event, Extractor, Message, State
from flechtwerk.attribute import Attribute, DATETIME, INT, STR

CYCLE = Attribute("cycle", INT)
"""Resume cursor — stands in for whatever your source pages by."""
NAME = Attribute("name", STR)
"""Carried on the config record; names the thing this poll target extracts."""
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

`poll` receives the `Config` record that selects this poll target and the `State`
persisted for it last time. The real work — the API call, the pagination — goes
where the comment is; everything around it is the framework contract.

!!! note "Config Selects, State Resumes"

    `config` is the *what* (which tenant, endpoint, or device to poll) and comes
    from your config topic; `state` is the *where you left off* and is persisted
    by the framework. Add a poll target by writing a record to the config topic —
    any producer will do, Kafka UI included — and remove one by tombstoning its
    key.

## State Is a Resume Cursor Here — but State Is Shape-Agnostic

State behaves exactly as it does for a transformer: `yield State(...)` persists it
for the current key, a falsy `State` tombstones the key, and a stage that never
yields `State` is stateless and never opens a RocksDB file. Statefulness is
orthogonal to a stage's shape — it is the same mechanism on both ends, and either
shape can use it or skip it. Most transformers are stateless; many extractors are
not, because a poll loop usually needs to remember progress.

What differs is the *idiom*. An extractor typically uses state as a **resume
cursor** — "which page, offset, or timestamp did I reach?" — so the next cycle
continues instead of re-importing from the start. In the example, `CYCLE` stands
in for whatever your source pages by: a page token, a high-water timestamp, an
opaque continuation handle. A fire-and-forget extractor that re-scans everything
each cycle simply never yields `State`.

## Running It

An `Extractor` runs with the same single `Flechtwerk.of(...).run()` call as any
stage — see [Getting Started → Running a Stage](getting-started.md#running-a-stage).
The one knob an extractor must set is `poll_interval` (a positive `timedelta`):
the interval between polls, and — for a push source like the
[MQTT Extractor](mqtt.md) — the idle wait that the arrival wakeup cuts short.
Startup fails fast if it is missing.

## Run Exactly One Instance

The multi-instance, exactly-once safety story is **transformer-only**. Nothing
fences concurrent extractor instances: each one reads *all* configs
(`group_id=None`, no partition assignment), polls every external source
redundantly, and writes the same state keys to a key-hashed changelog — so a slow
instance can overwrite an advanced cursor with a stale one (a silent re-import,
not just duplicates). Extractor output is deliberately non-transactional
(at-least-once — polling an external API can't be atomic with anything).

**Run exactly one replica per extractor.** Orchestrator restart latency is
invisible at poll-interval timescales, so a second replica buys no availability
either. See [Extractors Are Single-Instance](../concepts/architecture.md#extractor)
for the full rationale.

## Next Steps

- **[MQTT Extractors](mqtt.md)** — a push-driven extractor: instead of polling on a
  timer, messages arrive over MQTT and wake the poll loop, ACKed to the broker
  only once a batch is durable in Kafka.
- **[Best Practices](best-practices.md)** — pair this extractor with a transformer
  so you can reprocess and adapt to schema changes without re-ingesting.
- **[Transformers](transformer.md)** — the same contract on the consuming end, with
  exactly-once delivery.
- **[Config topics](../concepts/config-topics.md)** — how the config topic that
  drives your poll targets is read into a shared, eventually-consistent lookup
  table (Kafka Streams' GlobalKTable).
- **[Typed attributes & records](../concepts/typed-records.md)** — the `Attribute`
  library that keeps the JSON boundary honest.
