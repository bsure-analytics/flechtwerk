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

!!! note "Exactly-Once, Page by Page"

    An extractor's `poll()` runs inside **per-page Kafka transactions**: every
    `State` yield is a commit boundary that makes the page's messages and its
    cursor durable atomically. A crash or ownership handover replays only the
    uncommitted page — whose messages were aborted and are invisible under
    `read_committed` — so for a re-readable source, delivery is exactly-once
    from cursor to Kafka, the way Kafka Connect's KIP-618 source connectors
    work. What stays at-least-once is the world outside Kafka: the external
    read itself is re-executed for a replayed page. Three rules follow: yield
    a page's messages **first** and the `State` that accounts for them
    **last** — the `State` yield is what closes the page, and a cursor
    committed ahead of its messages skips past them for good if the process
    crashes between the two transactions; yield your cursor once per page —
    and at least every 10 minutes, since one transaction may not outlive the
    transaction timeout; and make sure every downstream consumer reads
    `read_committed`, or it will see aborted pages.
    See [Exactly-once delivery](../concepts/exactly-once.md) for the task
    model this borrows from.

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
emits one record per cycle. Build it by decorating a `poll` function with
`@extractor(...)` — no subclass needed.

```python
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from flechtwerk import Config, Event, extractor, Message, State
from flechtwerk.attribute import Attribute, DATETIME, INT, STR

CYCLE = Attribute("cycle", INT)
"""Resume cursor — stands in for whatever your source pages by."""
NAME = Attribute("name", STR)
"""Carried on the config record; names the thing this poll target extracts."""
POLLED_AT = Attribute("polled_at", DATETIME)

@extractor(config_topics=["my-config"])
async def poll(config: Config, state: State) -> AsyncIterator[Message | State]:
    cycle = (state.get(CYCLE) or 0) + 1               # your API call goes here
    yield Message(
        key=config[NAME],
        topic="my-extract",
        value=Event({CYCLE: cycle, POLLED_AT: datetime.now(timezone.utc)}),
    )
    yield State({CYCLE: cycle})
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

## Wrapping the Source Payload

The example above builds its `Event` from a **typed literal** —
`Event({CYCLE: cycle, ...})`, a `dict` keyed by `Attribute` handles, for values
*you* construct. But what an extractor gets back from a real source is **raw
JSON**: a `dict[str, Any]` with string keys you didn't choose. To bring that
across the JSON boundary into a typed record, use the `wrap` classmethod — the
wire-format entry point inherited by `Event`, `State`, and `Config`:

```python
raw = await your_api_call(config, state)   # dict[str, Any] straight from the source
event = Event.wrap(raw)                    # verbatim, JSON-boundary-checked
```

`Event.wrap(raw)` stores each value under its original wire key after running it
through the same JSON-native check every write goes through, so the record is safe
to serialize and to read back through `Attribute` handles whose names match the
JSON keys. It is the exact entry point the framework itself uses to decode every
inbound record (`parse_message` → `Event.wrap`).

Two constructor paths, picked by the shape of your input:

- **`Event({ATTR: value, ...})`** — a typed literal keyed by `Attribute`s, for values you build (synthetic or computed fields).
- **`Event.wrap(raw)`** — raw JSON you received, keyed by strings.

To keep the payload as-is *and* stamp on your own fields, wrap then spread:

```python
event = Event({**Event.wrap(raw), POLLED_AT: datetime.now(timezone.utc)})
```

This "wrap the source verbatim, add ingestion metadata" shape is the backbone of
the raw-then-refined pattern in [Best Practices](best-practices.md). See also
[Typed attributes & records](../concepts/typed-attributes.md) for the `wrap` vs.
typed-literal distinction in full.

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

Not yielding `State` never loses messages: everything yielded after the last
commit boundary — or by a poll that yields no `State` at all — commits as one
**trailing page** when the generator completes. What such a poll gives up is
the cursor and the pagination: the next cycle re-enters with the same state,
and the whole invocation stands or falls as a single transaction, which must
still fit the 10-minute transaction timeout.

## Running It

An `Extractor` runs with the same single `Flechtwerk.of(...).run()` call as any
stage — see [Getting Started → Running a Stage](getting-started.md#running-a-stage)
for the shared knobs. The one extractor-specific knob is `poll_interval` (a
positive `timedelta`): the interval between polls, and — for a push source like
the [MQTT Extractor](mqtt.md) — the idle wait that the arrival wakeup cuts short.
Startup fails fast if it is missing. Pass the decorated `poll` as the `stage`:

```python
await Flechtwerk.of(
    application_id="my-extractor",
    bootstrap_servers="localhost:9092",
    client_id="my-extractor-0",
    poll_interval=timedelta(minutes=1),  # required for extractors
    stage=poll,                          # the decorated poll above
).run()
```

## Scaling Out

An extractor scales by replica count alone — there is no mode to configure.
Every instance of one `application_id` joins a consumer group on the config
topics and leases their partitions as ownership **tokens**: an instance polls
only the configs whose *state key* hashes onto a token it currently holds. One
replica — the common deployment — holds every token and owns everything;
replicas up to the config topics' partition count split the configs between
them; further replicas become hot standbys that take over on failure.

Ownership is computed consumer-side, so where config records physically land
stays irrelevant — writing config via Kafka UI (which defaults everything to
one partition) keeps working. On a rebalance the leaving owner cancels its
in-flight cycle — aborting its open page — and wipes its local store *before*
the group re-forms; the new owner fences it (`InitProducerId` on the static
per-token transactional ID), restores from the changelog, and continues each
cursor exactly where the last committed page left it. `self.configs` still
holds the **global** config store on every instance — scale-out only narrows
which configs `poll` is invoked for.

Two things to know when sizing a deployment:

- The config topics of one extractor must share one partition count (validated
  at startup): that count is the token space, so it caps the useful replica
  count. `poll_cycle_seconds` approaching your `poll_interval` — or CPU-bound
  decoding saturating the one core an event loop can use — is the signal to
  add replicas.
- Delivery is **transactional**: each held token owns a producer whose static
  transactional ID fences any previous owner, so even a zombie's in-flight
  page is aborted before the new owner restores — cursors never regress, and
  handovers neither lose nor duplicate records (see the exactly-once note
  above).

**Run MQTT extractors as one replica** for now: a token handover leaves the
old owner's persistent broker session subscribed (there is no unsubscribe
lifecycle yet), which at QoS ≥ 1 wedges that session's inflight window. A
single replica is fully safe — handovers back to itself roll drained-but-
unconfirmed messages back into the buffer instead of ACKing them unsent. See
[the architecture notes](../concepts/architecture.md#extractor) for the full
model.

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
- **[Typed attributes & records](../concepts/typed-attributes.md)** — the `Attribute`
  library that keeps the JSON boundary honest.
