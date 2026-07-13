# Typed Attributes & Records, Not Bare Dicts

A stream processor lives on the JSON boundary: every input is a dict decoded from the wire, and every output and every state write goes back through `json.dumps`. Handled as bare dicts, that boundary leaks into everything — each read re-checks presence and re-parses timestamps, a `datetime` assigned three hops earlier blows up only when the record is finally serialized, and a field that silently became `null` surfaces as a `KeyError` in some consumer far from the code that dropped it.

The `flechtwerk.attribute` library moves all of that to the **write site**. Each field is declared exactly once, as a typed handle pairing a wire name with an explicit `Codec[V]`:

```python
from datetime import datetime, timezone

from flechtwerk import Event
from flechtwerk.attribute import Attribute, DATETIME, LIST, STR

DEVICE = Attribute("device", STR)
LAST_SEEN = Attribute("last_seen", DATETIME)
TAGS = Attribute("tags", LIST(STR), optional=True)

event = Event({
    DEVICE: "sensor-1",
    LAST_SEEN: datetime(2026, 7, 12, 9, 30, tzinfo=timezone.utc),
    TAGS: ["a", "b"],
})

event[LAST_SEEN]   # datetime(2026, 7, 12, 9, 30, tzinfo=timezone.utc) — a real datetime
event.raw          # {'device': 'sensor-1', 'last_seen': '2026-07-12T09:30:00Z', 'tags': ['a', 'b']}

# Both of these raise at the write site, not at serialization time:
#   event[DEVICE] = 42     # expected str, got int
#   event[DEVICE] = None   # cannot assign None to required Attribute('device')
```

`Event`, `State`, and `Config` are `Record` subclasses — dict-like containers indexed by these handles rather than string keys. (`Message` is a frozen dataclass envelope carrying a key, topic, `Event` value, and optional timestamp.) The codec runs on **every write**, so the underlying `.raw` payload stays JSON-native by construction: wire encoding is a straight `json.dumps(event.raw)`, decoding is a straight `Event.wrap(raw)`, and nothing in between ever needs to re-validate.

!!! note "JSON Is the Only Wire Format — For Now"

    JSON is currently the only supported wire format for Kafka messages, and it covers every `Event`, `State`, and `Config`. Support for other serialization protocols (Avro, Protobuf, and the like) is a possible future extension, but is not currently planned.

## Required vs. Optional

A required attribute (the default) rejects `None` so a dropped value can't silently land as JSON `null`; declare fields where absence is legal with `optional=True`.

The read distinction is carried by the **method**, not the declaration:

- `event[LAST_SEEN]` reads-or-raises.
- `event.get(TAGS)` tolerates absence and returns `V | None`.

## Codecs Compose

Codecs are built from atoms and constructors:

- **Atoms:** `STR`, `INT`, `BOOL`, `DATE`, `FLOAT`, `DATETIME`, `TIME`, `RECORD`, `ANY`.
- **Constructors:** `LIST(V)`, `SET(V)`, `TUPLE(V)`, `DICT(V)`.

Nest them freely — `DICT(LIST(INT))`, `LIST(RECORD)`, and so on — and the whole tree is validated on every write.

## Spreading: Enrich Without Mutating

`Record` subclasses **spread like plain dicts**, so enrichment is always a copy-with-overrides — never an in-place edit:

```python
later = datetime.now(timezone.utc)

# a new Event with one field overridden — the original `event` is untouched
enriched = Event({**event, LAST_SEEN: later})

# spread across records too: carry an incoming payload forward and add a field
out = Event({**msg.value, SEEN: seen})
```

This is why enrichment never has to mutate its input. `{**record, NEW: value}` builds a **fresh** record from the old one's fields plus your overrides, leaving the original untouched — so inside a `transform` (or a `poll`, or a `relay`) you derive the next output `Event` or the next `State` by spreading, rather than editing in place.

And it is a backstop, not merely a convention:

!!! warning "Parameters are read-only — mutations are ignored"

    The runner hands every stage hook a **private copy of its mutable parameters** — the running `state`, and, for extractors, the `config` — and it never reuses the single-use `msg` after the call returns. **Mutating a passed parameter in place therefore has no effect; the change is silently discarded.** The only way to emit output or change state is to `yield`, and the way to enrich is to spread into a fresh record.

And spreading is not an escape hatch around the type system: the spread runs through the **same typed-write path**, so every field — carried over or overridden — is codec-checked, and the result's `.raw` stays JSON-native by construction.

!!! tip "Why It Matters"

    The point isn't ceremony; it's that the boundary between "Python object graph" and "JSON on the wire" is enforced at assignment time, once per `Attribute` declaration, rather than re-derived on every serialize/deserialize.
