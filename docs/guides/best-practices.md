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

## Defer Aggregation to Query Time

The split above pushes downstream change from "re-ingest the source" to "replay
the raw topic" — cheaper, but not free: a replay still costs wall-clock time
proportional to how much history you hold, and on a large dataset that is hours,
not seconds. So push one rung further. Anything you *can* compute at the moment
the question is asked — **windowed aggregations, running totals, rankings,
rollups** — leave out of the transformer and let your OLAP query engine (e.g.
[Apache Druid](https://druid.apache.org/)) compute it at query time.

```
… ──▶ refined topic  ──▶  [ OLAP query engine ] ──▶ your apps
      clean, granular,    aggregates, windows,
      not pre-aggregated  ranks — at query time
```

The payoff is the replayability argument taken to its limit: a query is
recomputed from scratch every time it runs, so **changing an aggregation is
instant and reprocesses nothing.** Windowing is exactly where this matters most.
It is error-prone, and you *will* rewrite it repeatedly — especially early on,
before its shape has settled. Bake it into the transformer and every tweak,
however small, triggers a full replay before you can see the result; keep it at
query time and the same tweak is a one-line edit to a query. This is not merely
convenient, it is strategic: **KISS** (no window state, no window abstraction to
maintain) and **YAGNI** (materialize a view only once its shape has stopped
moving).

!!! note "You Moved the Cost — You Didn't Delete It"

    Query-time aggregation trades re-transformation cost for query-time compute
    and for storing finer-grained data. The trade wins because a columnar OLAP
    engine is built for exactly this: ingestion-time rollup, fast scans, and
    aggregation as a first-class operation. So the lesson is *defer aggregation
    to the engine built to aggregate* — not "defer everything to query time"
    regardless of where it lands.

!!! warning "When a Window Must Live in the Pipeline"

    Query time absorbs any window that produces a **read-side view** — a
    dashboard, a report, an aggregate your apps read. It cannot absorb a window
    that must **drive an action inside the pipeline**: alerting on a threshold,
    deduplicating within a time gap, stitching sessions in a way that changes
    *what gets stored*. Those need stateful stream processing, and a
    [transformer](transformer.md) can do them with its RocksDB state and event
    timestamps — Flechtwerk simply ships no window *abstraction*, so you build
    the state machine explicitly. Rule of thumb: aggregate at query time when the
    window feeds a **dashboard**; keep it in the transformer when it feeds a
    **decision**.

## Next Steps

- **[Extractors](extractor.md)** — build the ingestion half that writes the raw topic.
- **[Transformers](transformer.md)** — build the refinement half that reads it back.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — why a transformer replay is safe to run to completion.
