# Best Practices

## Split Ingestion From Transformation

For any external datasource, run **two** stages, not one: an
[Extractor](extractor.md) that captures the source data and a
[Transformer](transformer.md) that shapes it for your applications.

```
external source ──▶ [Extractor] ──▶ raw topic ──▶ [Transformer] ──▶ refined topic ──▶ your apps
                    at-least-once   (faithful     exactly-once      (query model)
                                     backup)
```

- The **extractor** writes the source data to a **raw topic** as faithfully as
  possible — the payload as received, amended only with ingestion metadata (fetch
  time, source identity, the config key it came from, a schema version). This raw
  topic is your durable, replayable backup of everything the source ever gave you.
- The **transformer** consumes that raw topic and produces the **refined topic**
  your applications actually query — enriched, reshaped, validated, joined against
  config, keyed the way you want.

!!! tip "Wrap the Source Verbatim"

    The extractor's job is to preserve, not interpret. Take the raw JSON the source
    returns and wrap it with `Event.wrap(payload)` — the wire-format entry point
    that brings a `dict[str, Any]` across the JSON boundary unchanged — then spread
    on your ingestion metadata: `Event({**Event.wrap(payload), FETCHED_AT: now})`.
    Resist reshaping here: every transformation you do in the extractor is one you
    can't redo from the raw topic without going back to the source. See
    [Wrapping the source payload](extractor.md#wrapping-the-source-payload).

### Why the Split Pays Off

The external source is the one input you may not be able to get back: it
rate-limits, it ages out history, it costs money per call, or it simply won't let
you ask for the past again. So capture it once, verbatim, and never make
correctness depend on asking twice.

Everything downstream of the raw topic then becomes **replayable**. When a schema
changes — a new field upstream, a new query shape downstream, or a bug in your
enrichment logic — you fix the transformer and **reprocess from the raw topic**
instead of re-ingesting:

1. stop the transformer;
2. delete its state (the changelog topic) and reset its consumer-group offsets to
   the start of the raw topic;
3. restart — it rebuilds the refined topic from scratch, at Kafka speed, without a
   single call back to the external source.

Because the transformer has [exactly-once delivery](../concepts/exactly-once.md),
a full replay produces the refined topic exactly as if the new logic had always
been running — no duplicates, no gaps. The raw topic absorbs upstream change; the
transformer absorbs downstream change; the external source is queried exactly
once per record, ever.

!!! tip "Keep the Raw Topic Retained, Not Compacted"

    Replay reaches only as far back as the raw topic still holds. Give it
    retention that matches how far you might need to reprocess — often effectively
    forever (large or infinite `retention.ms`). This is the opposite of a
    [config topic](../concepts/config-topics.md), which is *compacted* to the
    latest value per key: the raw topic is a **history**, so keep the history.

!!! note "The Raw Layer Absorbs At-Least-Once"

    An extractor is [at-least-once](extractor.md): a retried poll can write the
    same record to the raw topic twice. Carry a stable, source-level identifier in
    the raw payload so the transformer can deduplicate as it refines (or make the
    refined write idempotent on that key). Duplicates in the raw log are cheap;
    duplicates leaking into the query model are not.

## Next Steps

- **[Extractors](extractor.md)** — build the ingestion half that writes the raw topic.
- **[Transformers](transformer.md)** — build the refinement half that reads it back.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — why a transformer replay is safe to run to completion.
