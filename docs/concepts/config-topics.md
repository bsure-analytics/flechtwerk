# Config Topics — Shared Lookup Tables

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

For extractors this is not an extra mechanism but the baseline: config topics are the only Kafka input an extractor has. For transformers it is the escape hatch from the co-partitioning requirement: a config topic's partition placement and count are irrelevant, so any producer (Kafka UI included) can write configs without routing them to the "right" partition. One refinement for an [extractor](../guides/extractor.md#scaling-out): its config topics must share one partition *count* (the partitions double as the ownership leases that shard configs across replicas), but *placement* stays irrelevant even there — ownership is a consumer-side hash of the state key, never the record's partition.

The source topics are their own changelog — compacted, small, re-read on every startup — and lookups are eventually consistent, outside the task transaction (the GlobalKTable caveat). A stage can also *maintain* a config topic instead of only reading one — see [Writing to a Config Topic](#writing-to-a-config-topic).

## Enrichment on the Way In

`Stage.enrich_config(config)` hooks one-time derivation (e.g. an API lookup) into the config path: the framework applies it **once per config record** — never per poll tick or lookup — and both stage kinds inherit it.

!!! note "Why Re-Reading Is Safe"

    Kafka Streams forbids transforming records on their way into a global store (KIP-813) because a checkpoint-based restore would bypass the transformation. Flechtwerk re-reads the topics through the same `enrich_config` path on every startup, so the enriched store cannot diverge.

## When a Config Record Won't Decode

Config records are usually written by hand or by ops tooling, which is exactly where a bad one comes from: a stray array, a Latin-1 key, a half-written value. Decoding is strict, and the policy is [`Stage.on_invalid_message`](invalid-messages.md) — by default it crashes the stage, at startup during the bootstrap or in the main loop during a drain. That is deliberate for a table every instance depends on: a config that silently read as `{}` would look like a missing key at the lookup site, far from the record that caused it.

The same determinism argument as above applies to the hook itself: every boot re-reads the topics through it, so a handler whose substitution varies would build a store that diverges from what a fresh boot builds.

## Writing to a Config Topic

Config topics are usually written by someone else — a team, ops tooling, a UI. A stage may also **maintain** one: declare it, then yield a `Message` whose topic is that config topic. The use case is a durable memo of an *external observation* — a third-party answer that is timestamped rather than derived from your own data, so recomputing it later asks a different question. Record it once and read it back forever, including across a full reprocess.

```python
from collections import OrderedDict
from collections.abc import AsyncIterator

from flechtwerk import Event, IncomingMessage, Message, State, Transformer

class Memoizing(Transformer):
    input_topics = ["my-requests"]
    config_topics = ["my-observations"]

    def __init__(self, cache_size: int = 10_000):
        # Bridges the gap between producing a row and seeing it through
        # `configs.get` after the next drain. Bounded, and safe to lose.
        self.recent: OrderedDict[str, dict] = OrderedDict()
        self.cache_size = cache_size

    async def transform(self, msg: IncomingMessage, state: State) -> AsyncIterator[Message | State]:
        key = observation_key(msg.value)
        row = self.recent.get(key)
        if row is None:
            config = self.configs.get(key)
            row = None if config is None else config.raw
        if row is None:
            row = await ask_third_party(key)
            # The topic is the write path — `ConfigStore` is read-only here.
            yield Message(key=key, topic="my-observations", value=Event.wrap(row))
        self.remember(key, row)
        yield Message(key=msg.key, topic="my-results", value=enrich(msg.value, row))

    def remember(self, key: str, row: dict) -> None:
        self.recent[key] = row
        self.recent.move_to_end(key)
        while len(self.recent) > self.cache_size:
            self.recent.popitem(last=False)
```

The write rides the task transaction like any other output — the runner does not care which topic a yielded `Message` names — so the new row, the output derived from it, and the input offsets commit atomically. And because the config consumer runs `read_committed`, an aborted transaction's row never reaches any instance's store.

!!! warning "What a Stage-Maintained Table Must Not Assume"

    - **No read-your-writes.** `configs.get(...)` cannot see the row until the next drain, the next loop iteration at the earliest — hence the bridge above. Keep it **bounded** (the store holds wire bytes and re-decodes per `get`, so an unbounded parsed shadow of it is a memory leak) and keep every entry **safe to lose**: an eviction, or the process dying with its transaction aborted, must degrade to "resolve it again and emit an identical row", never to a silently skipped write.
    - **Writes are not serialized.** Two instances — or two state-key buckets of one batch — can resolve the same brand-new key concurrently and both emit. Compaction makes that last-write-wins, so rows must be idempotent and order-insensitive. No counters, no read-modify-write: that needs partitioned task [state](exactly-once.md), not a config topic.
    - **The `ConfigStore` is not a write path.** Its `_put`/`_delete` are internal to the config machinery; a stage-side write never reaches Kafka, corrupts one instance, and is reverted by the next record for that key or the next restart.
    - **The size contract still binds.** The whole table lives in RAM per instance and is re-read in full on every boot — Flechtwerk's store is a plain dict of wire bytes, not Kafka Streams' RocksDB-materialized, checkpointed GlobalKTable. A key space that grows without bound outgrows this and belongs in partitioned state, or outside Kafka.
    - **The topic is not reproducible.** Unlike team-managed configuration, it is the sole copy of observations nothing can recompute: compact it, retain it forever, and keep reset tooling away from it.
