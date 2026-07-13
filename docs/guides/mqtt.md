# MqttExtractor — Push Into the Poll Loop

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
from flechtwerk.mqtt import MqttExtractor

DATA = Attribute("data", RECORD)
DEVICE_ID = Attribute("device_id", STR)
PROCESSING_TIME = Attribute("processing_time", DATETIME)

def relay(config: Config, topic: str, payload: Record) -> Message | None:
    return Message(
        key=payload[DEVICE_ID],             # missing → the framework poison-drops
        topic="my-extract",
        value=Event({DATA: payload, PROCESSING_TIME: datetime.now(timezone.utc)}),
    )

stage = MqttExtractor.of(config_topics=["my-config"], relay=relay)
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
    poll_interval_seconds=60,           # the arrival wakeup keeps latency sub-second
    mqtt=MqttBrokerConfig(broker="localhost", port=1883),
    stage=stage,                        # from above
).run()
```

See [Running a stage](getting-started.md#running-it) in Getting Started for the full walkthrough — an `MqttExtractor` runs the same way.

!!! note "Broker Settings and the Optional Extra"

    `MqttBrokerConfig` carries the broker settings, and paho stays confined to `flechtwerk.mqtt` — `import flechtwerk` never loads it, and the dependency ships as the optional `flechtwerk[mqtt]` extra (see [Getting Started](getting-started.md#installation)).
