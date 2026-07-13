# MQTT sources — push into the poll loop

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

!!! note "Broker settings and the optional extra"

    Broker settings are injected via `Flechtwerk.of(mqtt=MqttBrokerConfig(...))`, and paho stays confined to `flechtwerk.mqtt` — `import flechtwerk` never loads it, and the dependency ships as the optional `flechtwerk[mqtt]` extra (see [Getting started](getting-started.md#installation)).
