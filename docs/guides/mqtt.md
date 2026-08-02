# MQTT Extractors — Push Into the Poll Loop

An `MqttExtractor` is a push-driven [`Extractor`](extractor.md): instead of polling on a timer, messages arrive over MQTT and wake the poll loop. Read the [Extractor guide](extractor.md) first for the base model — this guide covers only the MQTT-specific surface.

`flechtwerk.mqtt` bridges a push-driven MQTT source into the extractor model out of the box. The framework owns everything protocol-shaped:

- one shared paho connection per process driven by the asyncio event loop (no threads);
- persistent MQTT 5 sessions with a stable client id and a configurable session expiry;
- shared subscriptions, so the rare deployment that needs a second replica divides the traffic without any Kafka-side coordination (see [Replicas and Scaling](#replicas-and-scaling));
- manual ACKs — a batch is ACKed to the MQTT broker only once its transaction committed in Kafka (at the top of the next poll, per the runner's re-entry contract). Within a process lifetime that makes delivery into Kafka exactly-once — an aborted page is rolled back, never ACKed. Across a crash it is at-least-once: the MQTT broker ACK cannot join a Kafka transaction, so messages committed but not yet ACKed are redelivered and written again — carry a payload identity and dedupe downstream if that matters;
- per-topic subscriptions fed by config records;
- an arrival wakeup so delivery latency is sub-second rather than poll-interval-bound;
- and Prometheus metrics.

!!! warning "Broker Requirement: MQTT 5 with Shared Subscriptions"

    Flechtwerk 0.9 speaks MQTT 5 only — EMQX 3.0+ or Mosquitto 2.0+. There is
    no protocol fallback and no mode flag. Upgrading from 0.8? See
    [Upgrading from 0.8](#upgrading-from-08).

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

An `MqttExtractor` is just an extractor, so you run it exactly like any other stage — a single `Flechtwerk.of(...).run()` call, with the MQTT broker settings injected alongside the rest of the configuration:

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

See [Getting Started → Running a Stage](getting-started.md#running-a-stage) for the full walkthrough — an `MqttExtractor` runs the same way, plus the `mqtt=` MQTT broker settings shown above.

!!! note "Broker Settings and the Optional Extra"

    `MqttBrokerConfig` carries the broker settings, and paho stays confined to `flechtwerk.mqtt` — `import flechtwerk` never loads it, and the dependency ships as the optional `flechtwerk[mqtt]` extra (see [Getting Started](getting-started.md#installation)).

## Subscription Lifecycle

Subscriptions follow the config set, by reconciliation: before every poll
cycle the runner hands the stage its active (non-suspended) configs, and the
stage unsubscribes every topic filter no active config declares. Tombstoning
a config, suspending it, and editing its `topic` therefore all converge on
the same clean-up — no wedged session, no manual MQTT broker surgery. Every
replica reconciles its own session against the same set; there is no
ownership for them to disagree about.

Disposal is deliberately **at-most-once for the in-flight tail**: messages
already ACK-pending are ACKed (they are durable in Kafka by then), while
buffered messages that never reached Kafka are dropped — ACKed, warned
about, and counted as `mqtt_message_dropped{reason="unsubscribed"}`. MQTT
has no NACK and cannot requeue for another consumer, so the only
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

## Replicas and Scaling

**Run one replica.** An MQTT extractor is a decode-and-forward bridge, and
the MQTT hop is not a scaling unit. Unlike Kafka, MQTT has no rebalancing at
all — not a dumber one, *none* — because there is nothing to rebalance to:
messages are queued **into a consumer's session**, and sessions do not move.
One replica keeps that entire problem out of your deployment, and a stage
whose only work is decode-and-produce is rarely the bottleneck anyway.

Scale **after** Kafka instead. Land the payloads on a topic and put the real
work — parsing, enrichment, state, aggregation — in
[transformers](transformer.md), where partitions, consumer groups and
rebalancing behave the way you expect. That is the
[raw-then-refined](best-practices.md) shape, and it answers almost every
"the bridge cannot keep up" question.

If the bridge itself genuinely saturates, split it by **topic filter**. One
`$share` group hands each filter to one member at a time, so a single
wildcard is one consumer's work however many replicas run; splitting
`sensors/+/events` into several config records — one per sub-tree, tenant or
device class — is what makes the traffic divisible in the first place.

Once it is divisible, **run several single-replica stages rather than one
stage with several replicas.** Give each stage its own `application_id` and
its own subset of filters, and every filter has exactly one consumer,
permanently. That is not merely simpler — it is better on the things that
actually bite:

- **The assignment is fixed**, so it cannot be re-picked mid-stream. A
  multi-replica stage's filters move whenever a publisher or a replica
  reconnects.
- **Per-source ordering is guaranteed**, rather than preserved only between
  re-pins.
- **The broker's dispatch strategy stops mattering.** Each `$share` group has
  one member, so `sticky` and `round_robin` become indistinguishable and you
  have one less piece of broker configuration to get right.
- **Scaling down stops being a routine operation.** You add and retire whole
  stages deliberately instead of changing a replica count — and a deliberate
  retirement is the one time you need the session cleanup described below.

What the multi-replica shape buys you is one deployment instead of several.
Take it when that genuinely matters, and read the next section first.

### If You Must Run More Than One

Replicas of one `application_id` subscribe `$share/{application_id}/{filter}`
for every active config, and the MQTT broker decides which replica each
filter's traffic goes to. None of that ownership travels through Kafka: an
MQTT extractor joins no Kafka consumer group and negotiates no handover, so
adding a replica is a broker-side event with no Flechtwerk-side coordination
at all, and no replica ever sits idle as a standby. Three things to get
right first.

**Set `shared_subscription_strategy = sticky`** (EMQX). The framework is
correct under any strategy — delivery is at-least-once either way — but
sticky keeps one publisher's stream on one replica, so per-source order
survives; `round_robin` sprays consecutive messages from the same sensor
across replicas and they reach Kafka interleaved.

`round_robin` looks attractive because it seems to spread the scale-down
damage. It does not avoid it: no strategy filters on connection state, so
round robin dispatches into a departed member's session too — just 1/N of
the traffic rather than all of one publisher's (measured on EMQX 5.8.9: a
survivor received 5 of 10). You would be paying scrambled order on every
message, permanently and irreversibly, to halve a loss that expiry or a kick
already recovers. Prefer the strategy whose failure is visible in a
throughput metric over the one whose failure surfaces months later as
inexplicable analytics.

**Expect no failover, and shorten `session_expiry`.** When a replica goes
away its session stays alive and keeps receiving its share: a broker
re-picks only once that session *ends*, not when the replica merely
disconnects. So the un-drained tail *and* everything arriving meanwhile wait
for the same `client_id` to come back — which makes stable per-replica
identities mandatory (a Kubernetes StatefulSet, whose pod ordinals give
exactly that), and makes a pod restart cost latency rather than messages.
A replica that stays down, though, holds its share until its session
expires. At two or more replicas that argues for a *short*
`MqttBrokerConfig.session_expiry` — minutes, not the 24-hour default —
because expiry is what hands the backlog to a survivor. See [Sizing the
Outage Budget](#sizing-the-outage-budget) for the trade in full.

**Have a scale-down runbook.** Removing a replica redirects nothing on its
own: its session lingers and the traffic pinned to it keeps flowing into
that session, where the survivors never see it. Two endings, trading data
against latency (both measured on EMQX 5.8.9):

- **Do nothing.** At `session_expiry` the session ends and the broker
  redispatches its whole queued backlog to a surviving member; new traffic
  follows. Nothing is lost — *provided the backlog never exceeded the
  broker's per-session queue depth*, past which messages were already
  discarded silently. On a busy filter that ceiling arrives in minutes, so
  waiting out a long expiry is usually the worse choice despite being the
  lossless one on paper.
- **Kick the orphan session** (`emqx ctl clients kick <client_id>`). New
  traffic re-pins to a survivor immediately, and whatever the session still
  held is dropped — the broker does not hand it to the other members. The
  loss is bounded, visible, and chosen.

So **kick promptly rather than waiting**, and stop the publisher first if
the tail matters — then the dropped tail is empty. A kick or an expiry is
also the only way a session ends, since Flechtwerk never ends one itself.

## MQTT Stages Are Stateless by Contract

Yielding `State` from an MQTT stage's `poll()` raises, at any replica count.
That is not a gap in the plumbing. When the MQTT broker moves a filter, two
replicas are briefly working the same stream — the departing one finishes
what it already holds while the new one takes the fresh traffic — and
neither is told that it happened. For per-message relay the overlap is
harmless: each message is still forwarded exactly once by whoever received
it. For state it is a write race, with no moment at which the new replica
could have loaded the current value. No broker strategy setting closes that
gap, which is why this is a contract rather than a warning.

A **stateful** push-driven source subclasses `Extractor` directly, overrides
`poll()`, and drives `MqttConnection` itself. It keeps the token-sharded
ownership model — one owner per key, with a clean handover point — and its
state store along with it.

## Sizing the Outage Budget

How long may an MQTT stage be down before it loses data? That is one number,
and two settings decide it — whichever binds first:

```text
outage budget  =  min( session_expiry ,  queue limit / message rate )
```

- **`MqttBrokerConfig.session_expiry`** (24 hours by default) is how long the
  MQTT broker keeps the stage's session, and the queue with it, after the pod
  goes away. The stage always sends it in CONNECT and gets what it asks for —
  a broker's own session-expiry setting applies only to clients that *cannot*
  ask (MQTT 3.1.1). So this is set in your application, in
  `Flechtwerk.of(mqtt=...)`, and not in broker configuration.
- **The broker's per-session queue limit** (EMQX `mqtt.max_mqueue_len`,
  Mosquitto `max_queued_messages`) is how many messages that session may hold
  while the stage is away. Broker-side only; a client cannot raise it.

Whichever binds first is the real budget, and the other number is decoration.
A 24-hour expiry over a 10 000-message queue is a 24-hour promise only if the
stage receives fewer than 10 000 messages in 24 hours — at ten messages a
second that queue fills in about seventeen minutes. Work out your peak rate
and make the two agree.

!!! warning "Overflow is silent, and which end you lose is broker-specific"

    When the queue is full the MQTT broker discards messages and tells no
    one — and brokers disagree about which end goes. Measured: **EMQX 5.8.9
    keeps the newest** (20 messages published to an offline session with
    `max_mqueue_len = 5` came back as the last five), so a stage returns to
    a recent tail with a hole where the *start* of the outage was. **Mosquitto
    2.1.2 keeps the oldest** (1 200 published against its default queue of
    1 000 came back as the first thousand), so you get an unbroken prefix and
    lose the tail instead. Either way the loss is invisible — size the queue
    so you never discover which kind your broker is.

**At one replica the expiry is the entire protection.** There is no other
member to inherit the backlog, so if the session lapses while the stage is
away its subscription goes with it, and everything published in the gap is
gone for good (also measured). Size the expiry for the worst outage you
intend to survive, then size the queue for the traffic that fits in it.

**At two or more replicas the trade inverts** — a *short* expiry is better,
because the backlog is redispatched to a surviving member rather than waiting
for you. See [If You Must Run More Than One](#if-you-must-run-more-than-one).

### What to Watch

The queue that can lose data is the broker's, and the stage cannot see it:
`mqtt_buffered_messages` counts what this process has already received and
not yet drained, which is a different and much smaller thing. Alert on the
**MQTT broker's** per-session queue depth, and read `mqtt_disconnects_total`
plus a climbing `mqtt_connects_total` as the early warning that a queue is
filling somewhere.

## Upgrading from 0.8

- **Broker floor.** MQTT 5 with shared subscriptions: EMQX 3.0+, Mosquitto
  2.0+. Check your MQTT broker before deploying; there is no fallback.
- **Per stage:** quiesce the publishers, deploy 0.9.0. Flechtwerk sends an
  UNSUBSCRIBE for the *bare* filter every time it subscribes the shared
  form, so a session resumed from 0.8 is cleaned up automatically — whether
  or not the MQTT broker carried it across the protocol switch. Optionally
  `emqx ctl clients kick <client_id>` after draining for a clean slate. Only
  QoS ≥ 1 stages need the drain step; at QoS 0 a deploy is just a restart.
- **No Kafka broker or topic changes.** The new transactional IDs
  (`{application_id}-{client_id}`) are client-supplied. Optional cleanup:
  the changelog topics of existing MQTT stages become permanently unused —
  they were always empty, since the relay template never yields `State` —
  and may be deleted.
- **`MqttBrokerConfig` is now keyword-only**, and gains `session_expiry`.
  If you built one with positional arguments, name them:
  `MqttBrokerConfig(broker=..., port=...)`.
- **Code that builds a stage by hand must set `group`** — typically tests. In
  production `Flechtwerk.of(...)` injects the `application_id` there, but a
  test that constructs a stage and enters it directly has to supply it
  itself, exactly as it already supplies `client_id`. An empty group is
  rejected at `__aenter__`, since it would subscribe the malformed
  `$share//<filter>`:

    ```python
    extractor = MqttExtractor.of(config_topics=["my-config"], relay=relay)
    extractor.client_id = "my-stage-0"   # per-instance identity
    extractor.group = "my-stage"         # shared-subscription group  ← new in 0.9
    extractor.mqtt = MqttBrokerConfig(broker="localhost", port=1883)
    ```

    Tests that pre-set `FakeMqttConnection` are unaffected — they bypass the
    connect entirely.
- **No other application changes** for a stage built from
  `MqttExtractor.of(...)` or `@mqtt_extractor(...)` and run through
  `Flechtwerk.of(...)`. A stage that overrides `poll()` and yields
  `State` must move to a plain `Extractor` subclass — see [MQTT Stages Are
  Stateless by Contract](#mqtt-stages-are-stateless-by-contract).
- **Behaviour at one replica is unchanged.** A sole group member still
  receives its backlog from an offline session, so a single-replica
  deployment sees no observable difference.

## Next Steps

- **[Extractors](extractor.md)** — the poll-based base model an `MqttExtractor` specializes.
- **[Best Practices](best-practices.md)** — back the pushed data up to a raw topic and refine it with a transformer, so you can reprocess without losing what the devices sent.
- **[Observability](observability.md)** — the `mqtt_*` Prometheus metrics for connection health, drops, and buffering.
- **[Getting Started](getting-started.md)** — the install, the two-yield contract, and how any stage is run.
