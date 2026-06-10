# CLAUDE.md

Guidance for the Fretworx framework code in this directory. The architecture
is documented in the repository root `CLAUDE.md`; this file holds framework
invariants that must survive refactoring.

## Invariant: config topics never participate in a Kafka transaction

In a transformer, config topics must have no contact with any task
transaction. This holds by construction, through three independent
mechanisms — keep all of them intact:

- **Separate, group-less consumer.** A transformer's config topics are read
  by a dedicated `config_consumer` with `group_id=None` (`module.py`). No
  consumer group means no committed offsets — config-topic offsets can never
  appear in `send_offsets_to_transaction`. The offsets that DO enter a task
  transaction are built exclusively from the main consumer's input-topic
  batch, and `validate_topics` keeps `config_topics` disjoint from
  `input_topics`, closing that path too.
- **Updates land outside the transaction boundary.** `check_config_updates`
  runs once per loop iteration, outside the batch lock, and only mutates the
  in-memory `ConfigStore` — no task, producer, or transaction involved. The
  fetch-then-drain-then-process sequencing gives every record of a batch one
  consistent config snapshot; that is a scheduling courtesy, not
  transactional coupling.
- **No write path through the task producers.** The framework never produces
  to a config topic; the store is fed only by `bootstrap_config_store` and
  `drain_config_updates`.

Lookups via `self.configs.get(...)` are therefore eventually consistent —
Kafka Streams' GlobalKTable caveat, stated on `Transformer.configs` and in
`configs.py`.

### Why the config consumer still runs read_committed

The isolation level is a consumption-side filter; it does not enroll config
reads in any transaction, so it cannot violate the invariant above. For the
normal case — non-transactional producers (ops tooling, Kafka UI) writing
config — it makes no difference at all: records are visible immediately
either way. It matters only when a *transactional* producer writes to a
config topic (nothing forbids a transformer emitting an output `Message`
onto one): `read_uncommitted` would apply records from aborted transactions
to the store — and a startup bootstrap would compact them in until the next
boot — while `read_committed` merely delays visibility until commit, which
the eventually-consistent contract already absorbs. `read_committed` also
gives `bootstrap_config_store` / `read_to_end` a well-defined end offset
(the LSO). Switching to `read_uncommitted` buys nothing and opens the
aborted-write hole — keep `read_committed`, matching every other framework
consumer.
