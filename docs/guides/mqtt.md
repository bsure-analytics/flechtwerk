# MQTT Extractors — Push Into the Poll Loop

An `MqttExtractor` is a push-driven [`Extractor`](extractor.md): instead of polling on a timer, messages arrive over MQTT and wake the poll loop. Read the [Extractor guide](extractor.md) first for the base model — this guide covers only the MQTT-specific surface.

`flechtwerk.mqtt` bridges a push-driven MQTT source into the extractor model out of the box. The framework owns everything protocol-shaped:

- one shared paho connection per process driven by the asyncio event loop (no threads);
- persistent sessions with a stable client id;
- manual ACKs — a batch is ACKed to the broker only once its transaction committed in Kafka (at the top of the next poll, per the runner's re-entry contract). Within a process lifetime that makes delivery into Kafka exactly-once — an aborted page is rolled back, never ACKed. Across a crash it is at-least-once: the broker ACK cannot join a Kafka transaction, so messages committed but not yet ACKed are redelivered and written again — carry a payload identity and dedupe downstream if that matters;
- per-topic subscriptions fed by config records;
- an arrival wakeup so delivery latency is sub-second rather than poll-interval-bound;
- and Prometheus metrics.

An application writes one pure function:

```python
from datetime import datetime, timezone

from flechtwerk import Config, Event, Message
from flechtwerk.attribute import Attribute, DATETIME, RECORD, Record, STR
from flechtwerk.mqtt import mqtt_extractor

DATA = Attribute("data", RECORD)
DEVICE_ID = Attribute("device_id", STR)
PROCESSING_TIME = Attribute("processing_time", DATETIME)

@mqtt_extractor(config_topics=["my-config"])
def relay(config: Config, topic: str, payload: Record) -> Message | None:
    return Message(
        key=payload[DEVICE_ID],             # missing → the framework poison-drops
        topic="my-extract",
        value=Event({DATA: payload, PROCESSING_TIME: datetime.now(timezone.utc)}),
    )
```

The `relay` return value decides the record's fate:

- return a `Message` to **forward** it;
- return `None` to **drop** it (ACKed immediately);
- **raise** to poison-drop it (logged, ACKed, counted — never a crash loop on a broken payload).

Sources that don't fit the one-in-at-most-one-out shape override `poll()`; the connection layer works without the template.

## Running It

An `MqttExtractor` is just an extractor, so you run it exactly like any other stage — a single `Flechtwerk.of(...).run()` call, with the broker settings injected alongside the rest of the configuration:

```python
await Flechtwerk.of(
    application_id="my-mqtt-source",
    bootstrap_servers="localhost:9092",
    client_id="my-mqtt-source-0",       # also the MQTT session identity
    poll_interval=timedelta(minutes=1), # the arrival wakeup keeps latency sub-second
    mqtt=MqttBrokerConfig(broker="localhost", port=1883),
    stage=relay,                        # the decorated relay above
).run()
```

See [Getting Started → Running a Stage](getting-started.md#running-a-stage) for the full walkthrough — an `MqttExtractor` runs the same way, plus the `mqtt=` broker settings shown above.

!!! note "Broker Settings and the Optional Extra"

    `MqttBrokerConfig` carries the broker settings, and paho stays confined to `flechtwerk.mqtt` — `import flechtwerk` never loads it, and the dependency ships as the optional `flechtwerk[mqtt]` extra (see [Getting Started](getting-started.md#installation)).

## Subscription Lifecycle

Subscriptions follow the config set, by reconciliation: before every poll
cycle the runner hands the stage its owned, non-suspended configs, and the
stage unsubscribes every topic filter no active config declares. Tombstoning
a config, suspending it, editing its `topic`, and losing its ownership at a
rebalance therefore all converge on the same clean-up — no wedged session,
no manual broker surgery.

Disposal is deliberately **at-most-once for the in-flight tail**: messages
already ACK-pending are ACKed (they are durable in Kafka by then), while
buffered messages that never reached Kafka are dropped — ACKed, warned
about, and counted as `mqtt_message_dropped{reason="unsubscribed"}`. MQTT
3.1.1 has no NACK and cannot requeue for another consumer, so the only
alternative would be holding them un-ACKed until they wedge the session's
shared inflight window. **Stop the publisher before removing a config** and
the dropped tail is empty. Suspension follows the same rule: the topic is
unsubscribed, interim messages are discarded, and resuming re-subscribes on
the next poll.

The first reconciliation also latches the declared filter set as
authoritative: from then on, QoS ≥ 1 messages matching no declared filter —
stragglers behind an UNSUBSCRIBE, or replay for filters an earlier
deployment left in the persistent session — are ACK-dropped on receipt and
counted as `mqtt_message_dropped{reason="stale", topic="(unmatched)"}`
instead of held. Before that point (the startup window) unmatched messages
are held un-ACKed, so the persistent session's replayed backlog is never
lost. Shutdown never unsubscribes: the session keeps buffering for the next
incarnation.

## Replicas and the Handover Window

An MQTT-sourced extractor **can** [scale out like any other
extractor](extractor.md#scaling-out) — a rebalance unsubscribes the topics
the old owner loses. But because a broker ACK can never join a Kafka
transaction, each ownership handover carries a bounded at-most-once window:
the old owner's undelivered buffer is dropped on unsubscribe, and messages
published between the old owner's UNSUBSCRIBE and the new owner's SUBSCRIBE
are delivered to neither session. Run **one replica** when that loss window
is unacceptable — single-replica self-handovers are fully lossless
(drained-but-unconfirmed messages roll back into the buffer instead of being
ACKed unsent). The lossless multi-replica story is MQTT 5 shared
subscriptions — a future design.

## Next Steps

- **[Extractors](extractor.md)** — the poll-based base model an `MqttExtractor` specializes.
- **[Best Practices](best-practices.md)** — back the pushed data up to a raw topic and refine it with a transformer, so you can reprocess without losing what the devices sent.
- **[Observability](observability.md)** — the `mqtt_*` Prometheus metrics for connection health, drops, and buffering.
- **[Getting Started](getting-started.md)** — the install, the two-yield contract, and how any stage is run.
