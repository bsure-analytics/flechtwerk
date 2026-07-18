# Best Practices

## Split Ingestion From Transformation

For any external datasource, run **two** stages, not one: an
[Extractor](extractor.md) that captures the source data and a
[Transformer](transformer.md) that shapes it for your applications.

```
external source ──▶ [Extractor] ──▶ raw topic ──▶ [Transformer] ──▶ refined topic ──▶ your apps
                    exactly-once    (faithful     exactly-once      (query model)
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

!!! note "The Raw Layer and Duplicates"

    An extractor's own delivery is [exactly-once from cursor to
    Kafka](extractor.md) — a replayed page was aborted, never seen downstream.
    What it cannot vouch for is the *source*: an upstream API that re-serves
    records (shifting pages, overlapping time windows) writes genuine
    duplicates into the raw log. Carry a stable, source-level identifier in
    the raw payload so the transformer can deduplicate as it refines (or make
    the refined write idempotent on that key). Duplicates in the raw log are cheap;
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

## Model the Wire Boundary Once

Records cross the JSON boundary through [typed
attributes](../concepts/typed-attributes.md), which enforce it at the write
site. A few rules keep that boundary honest:

- **Declare each field once, as a module-level `Attribute` constant**, and share
  it across every stage that touches the field. The attribute name is the wire
  key; one declaration means one source of truth for both the key and its codec.
- **Prefer a specific codec over `ANY`.** `STR`, `INT`, `DATETIME`,
  `LIST(...)`, `RECORD` validate on every write and document the shape; `ANY` is
  the escape hatch for genuinely heterogeneous edges, not the default. The more
  precise the codec, the earlier a bad value fails.
- **Required by default; `optional=True` only when absence is meaningful.** A
  required attribute rejects `None` at the write site so a missing value can't
  land silently as JSON `null`.
- **Pick the constructor by input shape:** `Record.wrap(raw_dict)` for
  wire-format JSON (an API payload, a `.raw`), the `Record({ATTR: value})`
  constructor for typed literals. Wrapping raw source data verbatim is the
  extractor rule above; typed construction is for records you build yourself.
- **Treat `.raw` as read-only from outside.** Read and write through attributes
  (`record[ATTR]`), not by reaching into the underlying dict — that is what
  keeps `.raw` JSON-native and the codecs in force.

## Handle Secrets at the Boundary

Secret fields — API keys, tokens, passwords — are encrypted in place with the
[`flechtwerk[secrets]`](../concepts/secrets.md) extra. The operational rules:

- **Encrypt only what is secret.** Wrap the secret field's codec with
  `ENCRYPTED(...)` (`Attribute("api_key", ENCRYPTED(STR))`); leave non-secret
  fields plaintext so the record stays browsable in a topic UI.
- **Config and State, not Event.** Config is the primary home; `State` works
  too (a fresh nonce re-encrypts only on an explicit write, so carried-forward
  state stays byte-stable), but an `Event` stream re-encrypts per message and
  hits the AES-GCM nonce budget — and encrypted event fields are opaque to
  analytics engines anyway (see the
  [caveats](../concepts/secrets.md#scope-caveats)).
- **Inject the keyring; encrypt at the write boundary.** Pass the keyring via
  `Flechtwerk.of(keyring=...)`, and have producers write secrets through
  `encrypt_value(ATTR, value)` — never hand-assemble a token. Reading is
  transparent.
- **Rotate reader-first.** Add a new key to every reader before promoting it to
  primary on the writers; between those steps, a reader rollback is a
  deterministic crash-loop (see [Rotation](../concepts/secrets.md#rotating-keys)).
- **Decide `scope` up front, if at all.** `ENCRYPTED(STR, scope="…")` binds a
  token to a compartment so it can't be relocated into a differently-scoped
  field. Adding a scope later is non-breaking (a scoped codec still reads
  unscoped tokens; sweep with `reencrypt`), but *removing* one is blocked, so
  don't scope a field unless you mean to keep it scoped.
- **Turn `read_plaintext` off after the migration.** It exists to accept legacy
  plaintext during a transition; every such read logs a WARNING and bumps
  `secret_plaintext_reads_total`. Flipping it back on to silence a
  `PlaintextSecretError` is the anti-pattern — a plaintext value in a strict
  field means a secret was pasted in the clear: treat it as compromised, rotate
  the credential, and re-produce the record encrypted.
- **After migrating a plaintext topic, rotate the credentials.** Any value that
  ever rested in plaintext is disclosed (backups and pre-compaction segments
  keep it); encryption protects only what is written after it.

## Next Steps

- **[Extractors](extractor.md)** — build the ingestion half that writes the raw topic.
- **[Transformers](transformer.md)** — build the refinement half that reads it back.
- **[Exactly-once delivery](../concepts/exactly-once.md)** — why a transformer replay is safe to run to completion.
- **[Typed Attributes & Records](../concepts/typed-attributes.md)** — the model behind the wire-boundary rules above.
- **[Encrypted Secrets](../concepts/secrets.md)** — the wire format, keyring, rotation, and migration behind the secret-handling rules above.
