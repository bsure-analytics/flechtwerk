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

For extractors this is not an extra mechanism but the baseline: config topics are the only Kafka input an extractor has. For transformers it is the escape hatch from the co-partitioning requirement: a config topic's partition placement and count are irrelevant, so any producer (Kafka UI included) can write configs without routing them to the "right" partition.

The source topics are their own changelog — compacted, small, re-read on every startup — and lookups are eventually consistent, outside the task transaction (the GlobalKTable caveat).

## Enrichment on the Way In

`Stage.enrich(config)` hooks one-time derivation (e.g. an API lookup) into the config path: the framework applies it **once per config record** — never per poll tick or lookup — and both stage kinds inherit it.

!!! note "Why Re-Reading Is Safe"

    Kafka Streams forbids transforming records on their way into a global store (KIP-813) because a checkpoint-based restore would bypass the transformation. Flechtwerk re-reads the topics through the same `enrich` path on every startup, so the enriched store cannot diverge.
