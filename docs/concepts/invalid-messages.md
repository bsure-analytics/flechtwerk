# Invalid Messages — One Hook, Three Outcomes

Every record a stage reads crosses the JSON boundary: the key is UTF-8 text, the
value is a JSON object. Sometimes it isn't. A misconfigured producer writes a
bare array, a foreign tool writes Latin-1 keys, a truncated write leaves half a
document on the topic. Flechtwerk decodes **strictly** and hands the decision
about what to do about it to exactly one hook:

```python
from collections.abc import AsyncIterator

from flechtwerk import Event, InvalidMessageError, Message, State, Transformer
from flechtwerk.attribute import Record


class Tolerant(Transformer):
    input_topics = ["my-input"]

    def on_invalid_message(self, error: InvalidMessageError) -> Record | None:
        if error.part == "key":
            raise error                      # identity is not negotiable
        return None                          # skip an undecodable value

    async def transform(self, msg, state) -> AsyncIterator[Message | State]:
        yield Message(key=msg.key, topic="my-output", value=msg.value)
```

The hook is inherited from `Stage`, so it is available on `Extractor`,
`Transformer`, and `MqttExtractor` alike, and it can also be passed to the
`.of(...)` factories and the decorators:

```python
stage = Transformer.of(
    input_topics=["my-input"],
    transform=my_transform,
    on_invalid_message=lambda error: None,
)
```

## The default is to crash

With no override, `on_invalid_message` raises the `InvalidMessageError` it was
given. Nothing has been committed at that point — for a transformer the parse
loop runs *before* any transaction begins — so the offset does not advance and
the restart re-fetches the same record. The stage crash-loops until the producer
or the topic is fixed.

That is a deliberate trade. The alternative Flechtwerk used to implement was to
decode a broken value into an empty `Event` with a warning, and it is strictly
worse: a lenient stage (`value.get(ATTR, default)`) processes garbage as
defaults and writes plausible-looking output, while a strict one crashes on a
required-attribute read three calls away from the actual problem — and then
crash-loops anyway, only without saying why. A crash-loop on a named record
beats silent laundering.

The `InvalidMessageError` is forensically complete on its own, because the
traceback is the *only* announcement the framework makes:

```
flechtwerk.types.InvalidMessageError: Undecodable value in the record at
my-input/3/4711 — see __cause__ for the decode error, and the key/value
attributes for the raw wire bytes. …
```

| Attribute | Meaning |
| --- | --- |
| `part` | `"key"` or `"value"` — which side failed |
| `topic`, `partition`, `offset` | where the record is, so you can go read it |
| `key`, `value` | the raw wire bytes, undecoded |
| `__cause__` | the original `UnicodeDecodeError` / `json.JSONDecodeError` / non-dict `ValueError` |

The raw bytes stay off the message on purpose: a value may be a megabyte.

## The three outcomes

| Return | Input record (transformer) | Config record (either stage shape) |
| --- | --- | --- |
| **raise** (default) | Crash before any transaction. Offset uncommitted → the record is re-fetched on restart. | Crash in the bootstrap or the drain. One bad config record crash-loops the stage. |
| **`None`** (skip) | The record never reaches `transform()`, but **its offset still commits** with the batch — the stage must not wedge on a record its own policy chose to skip. | The record is not applied: during a bootstrap the key stays absent, during a drain the previous value is retained. |
| **a `Record`** (substitute) | Proceeds exactly as if the value had decoded: `extract_state_key`, bucketing, and `transform()` all see the substituted value as an `Event`. | Proceeds as if decoded: `enrich_config` runs on it and the result lands in the store as a `Config`. |

The call site assigns the semantic type, so return any `Record` — `Event.wrap(…)`
is the usual choice — and the framework retypes it for the topic it came from.

!!! warning "A key can never be substituted"
    Returning a `Record` when `error.part == "key"` raises `TypeError`. A key is
    **state identity**: it selects the state-key bucket, names the changelog
    record, and — for an extractor — decides via `token_for` which replica owns
    the config. Synthesizing identity for a record whose identity is unreadable
    would silently merge unrelated records into one state entry, or move
    ownership. Key failures accept **raise** or **skip** only.

## What never reaches the hook

- **An empty or absent value.** It decodes to `Event.wrap({})`, as it always
  did. `{}` therefore means "empty value" unambiguously — never "garbage".
- **A tombstone's value.** The tombstone check runs on raw bytes, before any
  value decode. A tombstone whose *key* is undecodable does fire the hook with
  `part="key"`; skipping there means the deletion is not applied.
- **State and changelog records.** Changelog keys and `State` bytes are written
  by the framework itself, so corruption there is not a policy question — it is
  an unrecoverable data error. It crashes; the fix is to reset the affected
  state.
- **MQTT payloads.** An undecodable MQTT payload stays a poison-drop (warn, ACK,
  count). At QoS 1 with manual ACK there is no offset to hold back, so crashing
  would redeliver the poison on every restart and wedge ingestion forever. The
  hook still governs an MQTT stage's Kafka *config* records.

## Two requirements on your handler

**It is synchronous.** No `await`. If a recovery needs I/O — fetching a schema,
loading a lookup table — prefetch it in `__aenter__` and read it from `self`
here.

**It must be deterministic.** Config topics are re-read in full on every
startup, so a handler whose substitution varies would build a store that
diverges from what a fresh boot builds. This is the same constraint
`enrich_config` carries, for the same reason: there are no checkpoints, only
replay.

## The counter is not optional

Every invocation is counted, whatever it returns:

```
flechtwerk_messages_invalid_total{outcome="skipped", topic="my-input", …}
```

`outcome` is `raised`, `skipped`, or `substituted`. An override cannot silence
its own data loss — loss lands on a scrape, not in a log nobody tails. (A
`raised` increment rarely survives to the next scrape, since the process is
about to die; restart counts and the traceback carry that case.)

The framework logs **nothing** on any outcome. On a raise, the traceback is the
announcement and a warning before it would be duplicate telemetry about one
incident. On a recovered outcome, the application made an informed decision and
owns the announcement — it has the whole error object, so log it in the override
if you want one:

```python
def on_invalid_message(self, error: InvalidMessageError) -> Record | None:
    log.warning("Skipping %s at %s/%d/%d", error.part, error.topic, error.partition, error.offset)
    return None
```

## Substitution is a stopgap, not a bridge

Substitution earns its place on a low-volume topic you don't control: a vendor
writes bare-string values on a config topic, and one line of policy turns each
into `Event.wrap({"value": error.value.decode()})` until they fix it. Set
against a steady-state foreign wire format it is the wrong tool — the decoding
lives in an exception handler, off the main path, invisible in the stage's
logic, and it runs once per record forever. For that, put a small bridge stage
in front: read the foreign topic with the value as `bytes` (a `Message`'s
[`Payload`](../api/index.md) accepts pre-encoded bytes in either direction),
decode it explicitly, and write Flechtwerk-schema JSON to an intermediate topic.

## Migrating from the lenient fallback

Before this behavior existed, an undecodable **value** became an empty `Event`
or `Config` with a warning, and a non-UTF-8 **key** was silently repaired with
`errors="replace"`. Both now crash by default. Two things to check before
rolling out:

1. **Does any stage rely on the empty fallback?** A stage that reads its value
   leniently may have been processing garbage as defaults for a while, with only
   a WARN to show it. Grep your logs for the old `Malformed message` /
   `Non-dict JSON payload` warnings; if they are there, decide the policy
   explicitly with an override before upgrading.
2. **Any state accumulated under a replacement-character key is orphaned** once
   the producer is fixed — it was corrupt identity to begin with, and distinct
   broken keys could even have collided on it.
