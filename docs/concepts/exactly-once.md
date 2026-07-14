# Exactly-Once Delivery and the Task Model

Flechtwerk gives a transformer exactly-once delivery the way Kafka Streams does: a single Kafka transaction ties together every output message, every state changelog write, and the input offset commits for one processing batch. Either the whole batch becomes visible downstream — outputs, state, and committed offsets alike — or none of it does. This closes the gap that lets duplicates leak in frameworks which treat output, state, and offsets as separate concerns.

This page covers how that works: how work is split into tasks, what one transaction spans, how a moved partition is fenced, and how the framework recovers when something goes wrong.

!!! note "Transformers Only"

    Everything here is about `Transformer`. Extractors are deliberately single-instance and at-least-once — their output can't be atomic with an external API poll, so the fencing primitive below simply doesn't exist for them. See the extractor documentation for that model.

## The Task Model

A transformer's work is split into **tasks** — one task per input partition *number*, spanning that partition of every input topic. The consumer uses Kafka's **Range assignor**, which co-assigns same-numbered partitions, so a single task owns partition `N` of every topic it consumes.

Each task owns three things:

- a **transactional producer** with the static transactional ID `{application_id}-{partition}`;
- a **partition-scoped `ChangelogStateStore`**, restored from the matching changelog partition;
- the input offsets for its partitions.

State identity is *(input partition, `extract_key`)*: each task keeps its own RocksDB store, writes its changelog entries to its own changelog partition (explicit-partition produce, not key hashing), and restores exactly that partition when it is assigned. The framework makes no assumptions about what `extract_key()` returns — with the default `extract_key = msg.key`, Kafka's key partitioning makes per-task state indistinguishable from a global key.

The store shares the task's producer via dependency injection, so a `put()` inside the task's transaction joins that open transaction rather than writing out-of-band.

## What One Transaction Spans

The runner consumes with `getmany()`. For each batch, it commits **one Kafka transaction per task**, and that transaction covers exactly that task's:

1. output messages,
2. state changelog writes (via the task's `ChangelogStateStore`, sharing the producer), and
3. consumer offset commits (`send_offsets_to_transaction(offsets, application_id)`).

State writes are **deduped to one final write per key** — only the final state a key reached in the batch is written at commit time. A truthy `State` is a `put`; a falsy `State` is a `delete`, which writes a Kafka tombstone to the changelog through the same transactional producer. Task transactions commit **concurrently and independently**: each covers exactly one task's outputs, state, and offsets, and there is no cross-task atomicity.

### Ordering and Parallelism Within a Batch

Inside a batch, records are bucketed by *(task, state key)*:

- **Same-bucket records run serially**, so each one sees the previous one's yielded `State`.
- **Buckets run concurrently** via `asyncio.gather`, so I/O-bound `transform()` calls overlap.
- **Cross-bucket ordering is not preserved.** Within a bucket, records appear in `input_topics` order, then Kafka offset order.

Each `transform()` call receives a defensive deepcopy of the running state, so in-place mutation without a `yield State(...)` can't leak into either the running state or a later same-key record. Stateless transformers simply never yield `State` and never open a RocksDB file.

## Fencing: Static Transactional IDs

The static transactional ID `{application_id}-{partition}` is what makes multiple instances safe. When a partition moves to a new owner, that owner's producer calls `InitProducerId`, which bumps the producer epoch for that task's transactional ID. Any previous owner's in-flight transaction is aborted and its producer is fenced — its subsequent writes are rejected by the broker.

!!! note "EOS-v1 Fencing"

    This is Kafka Streams' EOS-v1 model. aiokafka has no KIP-447 generation fencing, so Flechtwerk relies on the static-ID `InitProducerId` fence rather than consumer-group-generation fencing.

Because the producer start *is* the fencing point, ordering matters at task startup:

```text
producer.start()  →  InitProducerId (fences previous owner, aborts its transaction)
                  →  changelog end offset is now final
                  →  restore state from the changelog partition (up to the LSO)
                  →  resume fetching that partition
```

State is restored only *after* fencing, because only then is the changelog end offset (the last stable offset under `read_committed`) final. All framework consumers run `read_committed`, so a restore never replays a record from an aborted transaction.

## Rebalance: Teardown and Rebuild

aiokafka's rebalance protocol is **eager**, and Flechtwerk never retains tasks across a rebalance — a missed rebalance would leave retained producers and stores silently stale.

- **On partition revocation**, the rebalance listener acquires the batch lock and tears down *all* tasks (producers stopped, stores closed and deleted). The batch lock guarantees no transaction is in flight while tasks are torn down; the rebalance blocks until the current batch, if any, commits.
- **On partition assignment**, the listener does bookkeeping only — no I/O. It pauses the newly assigned partitions and records them as pending. Restoring state in the callback would stall the whole group past `rebalance_timeout_ms`.
- **The main loop** then initializes each pending task one at a time (fresh producer → fence → restore → resume), so paused partitions come back online as their tasks are ready.

The fetch itself stays outside the batch lock: `getmany()` blocks until an active rebalance completes, and the rebalance waits for the revoke callback, so fetching under the lock would deadlock the consumer. The price is a narrow window where a rebalance strikes between fetch and processing — records whose task was torn down are dropped, their offsets were never committed, and the new owner reprocesses them.

!!! warning "Callback Exceptions Are Swallowed"

    aiokafka logs and swallows exceptions raised inside rebalance callbacks, so "let it crash" cannot fire from there. Failures are instead recorded on the runner's `fatal` slot and re-raised by the main loop.

## "Let It Crash" Recovery

There is no framework-level retry logic. Errors propagate; recovery is infrastructure:

- an orchestrator restart (e.g. Kubernetes `CrashLoopBackOff`),
- changelog replay to restore state,
- transaction abort to catch any duplicates from a partial write.

The line to draw is **recoverable vs non-recoverable**, not transient vs persistent. Catch only when the handler can actually *remedy* the problem — refresh an expired token, skip a 400 on an endpoint that doesn't exist for this tenant. Timeouts and 5xx should crash: sleeping and retrying in-process is reimplementing `CrashLoopBackOff` poorly.

!!! warning "Never Catch-and-Skip a Data Error"

    Swallowing a data error is silent data loss. If a record can't be processed correctly, crash — the transaction guarantees nothing partial was committed, and replay will bring the state back.

## Constraints

- **Equal partition counts.** All input topics of a transformer must have equal partition counts — the changelog is created with that same count. It must match *exactly* (not a multiple or a sum): each task writes its changelog to its own partition number, so partition `N` of the inputs maps one-to-one to partition `N` of the changelog. If a changelog already exists at a different count, startup fails — repartitioning requires a state migration.
- **Co-partitioning is the application's job.** If one logical state entry must see records from several input topics, those topics must be co-partitioned by key — same key bytes, same partitioner, same partition count. Only the partition count is checked; key and partitioner alignment cannot be, exactly as in Kafka Streams. Get it wrong and the same `extract_key` arrives on different partition numbers, producing independent state shards owned by different tasks — a silent split, not an error.
- **Idle instances are fine.** Instances beyond the partition count sit idle; when a partition moves to one of them, the fencing above makes the handover safe.
