# MQTT Extractors — Push Into the Poll Loop

An `MqttExtractor` is a push-driven [`Extractor`](extractor.md): instead of polling on a timer, messages arrive over MQTT and wake the poll loop. Read the [Extractor guide](extractor.md) first for the base model — this guide covers only the MQTT-specific surface.

`flechtwerk.mqtt` bridges a push-driven MQTT source into the extractor model out of the box. The framework owns everything protocol-shaped:

- one shared paho connection per process driven by the asyncio event loop (no threads);
- persistent sessions with a stable client id;
- manual-ACK at-least-once — a batch is ACKed to the broker only once it is provably durable in Kafka (at the top of the next poll, per the runner's re-entry contract);
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

## One Replica for Now

Run an MQTT-sourced extractor as **one replica**. Extractor replicas normally
[shard the config set between them](extractor.md#scaling-out), but there is no
MQTT unsubscribe lifecycle yet: when a config's ownership moves to another
replica, the old owner's persistent broker session keeps its subscription, and
at QoS ≥ 1 the un-ACKed messages it keeps receiving occupy that session's
shared inflight window until the broker pauses delivery for *all* topics of
the client. A single replica is unaffected — ownership handovers back to
itself are cancellation-safe (drained-but-unconfirmed messages roll back into
the buffer instead of being ACKed unsent), and a second replica still buys a
hot standby only if you accept the wedged-session risk on failover.

## Next Steps

- **[Extractors](extractor.md)** — the poll-based base model an `MqttExtractor` specializes.
- **[Best Practices](best-practices.md)** — back the pushed data up to a raw topic and refine it with a transformer, so you can reprocess without losing what the devices sent.
- **[Observability](observability.md)** — the `mqtt_*` Prometheus metrics for connection health, drops, and buffering.
- **[Getting Started](getting-started.md)** — the install, the two-yield contract, and how any stage is run.
