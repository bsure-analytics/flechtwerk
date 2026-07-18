"""MQTT→Kafka bridge machinery for push-driven extractors.

One process, one connection: a stage talks to the platform broker with one
stable ``client_id`` (one persistent session). ``MqttConnection`` owns the
single paho client and the asyncio socket-loop integration; inbound messages
are routed by topic to per-topic ``MqttSubscription`` views. Each view keeps
its own buffer and pending-ACK list, so the manual-ACK "ack the previous
batch" invariant holds per topic even when the runner polls several configs
(topics) concurrently — provided each config owns a distinct topic (the
config-key → topic model).

paho-mqtt's I/O is driven entirely by the asyncio event loop via socket
callbacks (``on_socket_open`` → ``add_reader``, ``on_socket_register_write``
→ ``add_writer``). No background threads — all callbacks run in the asyncio
thread, so messages accumulate in plain lists without synchronization.

At-least-once delivery:

- QoS comes from the broker config; ``clean_session=False`` + a stable
  ``client_id`` make the broker buffer messages while disconnected and replay
  on reconnect.
- paho runs in ``manual_ack`` mode: messages are NOT ACKed on receipt. The
  template ``poll()`` ACKs a batch only once it is provably durable
  downstream: ``ExtractorRunner`` re-enters ``poll()`` strictly after the
  previous batch's ``send_batch()`` + flush succeeded (the contract is
  documented on the runner), so ACKing the previous batch at the top of the
  next poll never ACKs anything Kafka hasn't stored.
- No auto-reconnect in this integration (paho only auto-reconnects in
  ``loop_forever``/``loop_start``), so an unexpected disconnect is surfaced
  as a ``ConnectionError`` by ``drain()`` → the stage crashes → the platform
  restarts it into a fresh connect.

Applications implement ``relay(config, topic, payload) -> Message | None``
(or pass a function to ``MqttExtractor.of``) and never touch the protocol:

- return a ``Message`` — forwarded to Kafka, ACKed with the next batch;
- return ``None`` — dropped and ACKed immediately (nothing to make durable);
- raise — poison-dropped: logged with traceback, ACKed, counted. At QoS 1
  with manual ACK, crashing on a poison payload would redeliver it on every
  restart and wedge ingestion forever; drop-warn-count is the only policy
  that can't, and the ``poison`` drop counter is the alarm that keeps it
  honest. Connection failures are unaffected — they surface from ``drain()``
  outside the hook and still crash the process.

Subscription lifecycle — config-driven, reconciled before every poll cycle:

- The runner hands ``MqttExtractor.on_active_configs`` the owned,
  non-suspended config set at quiescent points; the stage reconciles the
  broker session against it. Topics no active config declares (tombstoned,
  suspended, rewritten, or disowned at a rebalance) are UNSUBSCRIBEd and
  their views disposed: pending ACKs are sent — provably Kafka-durable at a
  quiescent point — and buffered messages that never reached Kafka are
  ACK-dropped with a warning and counter (MQTT 3.1.1 has no NACK and cannot
  requeue for another consumer; holding them un-ACKed would pin
  inflight-window slots until the whole shared session stalls). Stop the
  publisher before removing a config and the dropped tail is empty.
- The first reconciliation latches the desired-filter set as authoritative:
  from then on, unmatched QoS >= 1 messages are ACK-dropped on receipt
  (reason ``stale``) instead of held — mopping up post-UNSUBSCRIBE
  stragglers and traffic replayed for filters an earlier deployment left in
  the persistent session, which 3.1.1 can neither enumerate nor selectively
  remove. Before the latch — the startup window — unmatched messages are
  still held un-ACKed, protecting the backlog ``clean_session=False`` exists
  to protect.
- Shutdown never unsubscribes: the persistent session keeps buffering for
  the next incarnation. Removal is config-driven, not lifecycle-driven.

Sources that don't fit this shape (stateful, 1:N fan-out, non-JSON payloads)
override ``poll()`` wholesale — the connection/subscription layer works
without the template.

Configuration is injected, never read from the environment: the application
passes a fully resolved ``mqtt=MqttBrokerConfig(...)`` to ``Flechtwerk.of(...)``,
whose module-wide ``client_id`` doubles as the persistent session's identity —
the container places both on the stage verbatim before startup.
"""
import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING, AsyncIterator, Final, Self

from paho.mqtt.client import CallbackAPIVersion, Client, MQTTMessage, topic_matches_sub
from paho.mqtt.reasoncodes import ReasonCode

from .attribute import Attribute, Record, STR
from .configs import EnrichConfigFn
from .extractor import Extractor
from .module import MqttBrokerConfig
from .observer import Observer
from .stage import ExtractStateKeyFn
from .types import Config, Message, State

if TYPE_CHECKING:
    from paho.mqtt.client import SocketLike

log = logging.getLogger(__name__)

__all__ = ["MqttBrokerConfig", "MqttConnection", "MqttExtractor", "mqtt_extractor", "MqttSubscription", "TOPIC"]

TOPIC: Final = Attribute("topic", STR)
"""Required config field: the MQTT topic filter a config subscribes to.

Read by the template ``poll()`` (subscribe) and the reconciliation hook
``on_active_configs`` (unsubscribe) — one config record per MQTT topic,
each becoming a per-topic subscription over the shared connection.
Two configs must never declare the same (or an overlapping) topic filter:
they would share one subscription view and ACK each other's batches before
the other's messages reached Kafka.
"""

UNMATCHED: Final = "(unmatched)"
"""Metric topic label for drops of messages matching no subscription filter.

A sentinel instead of the concrete publish topic, which would have unbounded
label cardinality — the observer contract is that the MQTT ``topic`` label is
always a config-declared filter or this sentinel. The real topic goes to the
log, not the metric.
"""

RelayFn = Callable[[Config, str, Record], Message | None]


class MqttConnection:
    """One paho MQTT client shared by per-topic ``MqttSubscription`` views.

    Owns the asyncio socket-loop integration and the single broker session.
    Inbound messages are routed by topic (``on_message``) to the matching
    subscription; a connection-level failure (failed connect / unexpected
    disconnect) is recorded once and surfaced by every subscription's
    ``drain``.
    """

    def __init__(
            self,
            *,
            broker: MqttBrokerConfig,
            client_id: str,
            loop: asyncio.AbstractEventLoop,
            observer: Observer | None = None,
            wakeup: asyncio.Event | None = None,
    ) -> None:
        self.broker = broker
        self.client_id = client_id
        self.loop = loop
        self.observer = observer or Observer()
        self.wakeup = wakeup
        self.desired: set[str] | None = None
        self.subscriptions: dict[str, MqttSubscription] = {}
        self.unrouted: list[MQTTMessage] = []
        self.error: Exception | None = None
        self.misc_task: asyncio.Task[None] | None = None

        self.client = Client(
            CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=False,
            manual_ack=True,
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

        if broker.username:
            self.client.username_pw_set(broker.username, broker.password)

    async def __aenter__(self) -> Self:
        log.info(
            "Connecting to MQTT %s:%d as %s",
            self.broker.broker, self.broker.port, self.client_id,
        )
        # Blocking TCP handshake + MQTT CONNECT packet queued. The socket
        # callbacks (on_socket_open, on_socket_register_write) fire during this
        # call, registering the socket with the event loop.
        self.client.connect(self.broker.broker, self.broker.port)
        # Switch to non-blocking so asyncio can drive I/O from here.
        sock = self.client.socket()
        assert sock is not None
        sock.setblocking(False)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        # disconnect() triggers on_socket_close which removes reader/writer
        # and cancels the misc task.
        self.client.disconnect()

    def subscribe(self, topic: str) -> "MqttSubscription":
        """Return the per-topic subscription for `topic`, creating it (and
        telling the broker to subscribe) on first use. Idempotent."""
        sub = self.subscriptions.get(topic)
        if sub is None:
            sub = MqttSubscription(connection=self, topic=topic)
            self.subscriptions[topic] = sub
            if self.client.is_connected():
                log.info("Subscribing to %s (QoS %d)", topic, self.broker.qos)
                self.client.subscribe(topic, qos=self.broker.qos)
            # Otherwise on_connect (re)subscribes every topic once connected.
            if matching := [m for m in self.unrouted if topic_matches_sub(topic, m.topic)]:
                # Messages the persistent session replayed before this
                # subscription was registered (the startup window) — route
                # them now, in arrival order, ahead of anything newer.
                log.info("Routing %d held message(s) into new subscription %s", len(matching), topic)
                sub.items[:0] = matching
                self.unrouted = [m for m in self.unrouted if not topic_matches_sub(topic, m.topic)]
        return sub

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe `topic` at the broker and dispose its view.

        Disposal is the deliberate at-most-once tail of config removal:
        pending ACKs are sent — the runner reconciles only at quiescent
        points, where everything pending is committed to Kafka by the
        re-entry contract — and buffered messages that never reached Kafka
        are ACK-dropped with a warning and counter. MQTT 3.1.1 has no NACK
        and cannot requeue for another consumer, so the alternatives are
        silent loss or pinning inflight-window slots until the shared
        session stalls; stop the publisher before removing a config to make
        the dropped tail empty. Unknown topics are a no-op.
        """
        sub = self.subscriptions.pop(topic, None)
        if sub is None:
            return
        log.info("Unsubscribing from %s", topic)
        if self.client.is_connected():
            self.client.unsubscribe(topic)
        sub.ack_all_pending()
        if sub.items:
            log.warning(
                "Dropping %d undelivered MQTT message(s) buffered for unsubscribed topic %s",
                len(sub.items), topic,
            )
        for msg in sub.items:
            self.ack(msg)
            self.observer.mqtt_message_dropped(topic, "unsubscribed")
        sub.items.clear()
        self.observer.mqtt_buffered(topic, 0)

    def reconcile(self, desired: set[str]) -> None:
        """Align the session with the topic filters the active configs declare.

        Idempotent. Unsubscribes every subscription outside ``desired`` and
        latches ``desired`` as the authoritative filter set: held unrouted
        messages matching none of it are ACK-dropped now, and later
        unmatched arrivals are ACK-dropped on receipt (``on_message``).
        MQTT 3.1.1 can neither enumerate a session's subscriptions nor say
        which filter matched a delivery, so a broker-side unsubscribe of a
        stale filter is impossible — dropping its traffic is what keeps a
        leftover subscription from wedging the shared inflight window.
        Subscribing is the template ``poll()``'s job, not this method's.
        """
        for topic in [t for t in self.subscriptions if t not in desired]:
            self.unsubscribe(topic)
        self.desired = set(desired)
        stale = [m for m in self.unrouted if not self._matches_desired(m.topic)]
        self.unrouted = [m for m in self.unrouted if self._matches_desired(m.topic)]
        for msg in stale:
            log.warning(
                "Dropping stale MQTT message held for topic %s — no active config declares a matching filter",
                msg.topic,
            )
            self.ack(msg)
            self.observer.mqtt_message_dropped(UNMATCHED, "stale")

    def _matches_desired(self, topic: str) -> bool:
        """Whether `topic` matches the reconciled desired-filter set.

        Vacuously true before the first ``reconcile`` call — until the
        config bootstrap has declared the full set, no message may be
        judged stale (the startup window protects the persistent session's
        replayed backlog)."""
        return self.desired is None or any(topic_matches_sub(f, topic) for f in self.desired)

    def ack(self, msg: MQTTMessage) -> None:
        """Send PUBACK for a QoS 1 message. Logs but doesn't raise on failure."""
        if msg.qos == 0:
            return
        try:
            rc = self.client.ack(msg.mid, msg.qos)
            if rc != 0:
                log.warning("MQTT ACK failed for mid %d: rc=%d (broker will redeliver)", msg.mid, rc)
        except Exception:
            log.warning("MQTT ACK raised for mid %d (broker will redeliver)", msg.mid, exc_info=True)

    # -- Socket callbacks: drive paho I/O from the asyncio event loop ---------

    def on_socket_open(self, client: Client, _userdata: object, sock: "SocketLike") -> None:
        self.loop.add_reader(sock, client.loop_read)  # noqa
        self.misc_task = self.loop.create_task(self.run_misc_loop())

    def on_socket_close(self, _client: Client, _userdata: object, sock: "SocketLike") -> None:
        self.loop.remove_reader(sock)
        self.loop.remove_writer(sock)
        if self.misc_task:
            self.misc_task.cancel()
            self.misc_task = None

    def on_socket_register_write(self, client: Client, _userdata: object, sock: "SocketLike") -> None:
        self.loop.add_writer(sock, client.loop_write)  # noqa

    def on_socket_unregister_write(self, _client: Client, _userdata: object, sock: "SocketLike") -> None:
        self.loop.remove_writer(sock)

    async def run_misc_loop(self) -> None:
        """Periodically call loop_misc() for MQTT keepalives."""
        while True:
            self.client.loop_misc()
            await asyncio.sleep(1)

    # -- MQTT callbacks: run in the asyncio thread (driven by loop_read) ------

    def on_connect(
            self,
            client: Client,
            _userdata: object,
            _flags: object,
            reason_code: ReasonCode,
            _properties: object,
    ) -> None:
        if reason_code != 0:
            self.error = ConnectionError("MQTT connect failed: " + str(reason_code))
            self.wake()
            return
        # Successful (re)connect: clear any prior error and re-subscribe every
        # topic. The broker redelivers unACKed messages with fresh mids, so old
        # pending_acks — and held unrouted messages — reference stale mids;
        # clear them too.
        self.error = None
        self.observer.mqtt_connected()
        self.unrouted.clear()
        for topic, sub in self.subscriptions.items():
            sub.pending_acks.clear()
            log.info("Subscribing to %s (QoS %d)", topic, self.broker.qos)
            client.subscribe(topic, qos=self.broker.qos)

    def on_disconnect(
            self,
            _client: Client,
            _userdata: object,
            _flags: object,
            reason_code: ReasonCode,
            _properties: object,
    ) -> None:
        # No auto-reconnect in paho's asyncio socket integration (that lives only
        # in loop_forever/loop_start), and on_socket_close has already removed the
        # reader and cancelled the misc task — so an unexpected drop would leave us
        # silently inert. Record it so the next drain() re-raises: poll() crashes,
        # the platform restarts the process into a fresh connect. A clean,
        # app-initiated disconnect (__aexit__) reports reason_code 0 and is ignored.
        if reason_code != 0:
            self.observer.mqtt_disconnected()
            self.error = ConnectionError("MQTT disconnected: " + str(reason_code))
            # Wake the runner so the error surfaces from the next drain() now
            # instead of up to poll_interval later — the wakeup makes
            # long intervals the norm, and a dead connection shouldn't wait
            # one out before crashing.
            self.wake()

    def wake(self) -> None:
        """Fire the runner's wakeup event, if one is attached."""
        if self.wakeup is not None:
            self.wakeup.set()

    def on_message(self, _client: Client, _userdata: object, msg: MQTTMessage) -> None:
        for topic, sub in self.subscriptions.items():
            if topic_matches_sub(topic, msg.topic):
                sub.items.append(msg)
                self.observer.mqtt_message_in(topic)
                # Append-then-set: the runner's next drain() sees the message
                # that woke it, so a wakeup can never be lost.
                self.wake()
                return
        if msg.qos == 0:
            # No session state to protect — dropping is all QoS 0 offers.
            log.warning("No subscription matches MQTT topic %s — dropping", msg.topic)
            return
        if not self._matches_desired(msg.topic):
            # The reconciled session says no active config wants this: a
            # straggler behind an UNSUBSCRIBE, or replay for a filter some
            # earlier deployment left in the persistent session (3.1.1 gives
            # no way to unsubscribe those). ACK-drop — held un-ACKed it would
            # pin an inflight-window slot forever and eventually stall every
            # topic of this client.
            log.warning(
                "Dropping stale MQTT message from topic %s — no active config declares a matching filter",
                msg.topic,
            )
            self.ack(msg)
            self.observer.mqtt_message_dropped(UNMATCHED, "stale")
            return
        # QoS >= 1, matching a desired filter whose view isn't registered yet
        # (subscribe happens in the next poll) — or the desired set is not
        # yet authoritative at all: hold un-ACKed and re-route when the
        # matching subscription appears. The latter is the normal startup
        # path — the persistent session replays its backlog right after
        # CONNACK, before the Kafka config bootstrap has registered any
        # subscription — so ACKing here would permanently drop the very
        # backlog clean_session=False exists to protect.
        log.warning("No subscription matches MQTT topic %s yet — holding un-ACKed", msg.topic)
        self.unrouted.append(msg)


class MqttSubscription:
    """Per-topic view over a shared ``MqttConnection``.

    Holds this topic's inbound buffer and pending-ACK list. Because each topic
    has its own buffer/pending list, concurrent per-config poll() calls never
    cross-ACK one another. Manual-ACK semantics: messages are buffered un-ACKed
    and ACKed only once provably durable downstream.
    """

    def __init__(self, *, connection: MqttConnection, topic: str) -> None:
        self.connection = connection
        self.topic = topic
        self.items: list[MQTTMessage] = []
        self.pending_acks: list[MQTTMessage] = []

    def drain(self, limit: int) -> list[MQTTMessage]:
        """Take up to `limit` buffered messages, returning [] immediately if none.

        Synchronous — no wait. Messages accumulate between polls (the runner
        sleeps or waits on the stage's wakeup event in between). Buffered
        messages are returned first; a connection-level failure surfaces
        (raises) only once the buffer is empty, so an in-flight batch is never
        lost to it.
        """
        batch = self.items[:limit]
        del self.items[:limit]
        if not batch and self.connection.error is not None:
            raise self.connection.error
        return batch

    def ack_all_pending(self) -> None:
        """ACK everything in pending_acks. Called after confirming Kafka durability."""
        for msg in self.pending_acks:
            self.ack(msg)
        self.pending_acks.clear()

    def ack(self, msg: MQTTMessage) -> None:
        """Send PUBACK for a QoS 1 message. Logs but doesn't raise on failure."""
        self.connection.ack(msg)

    def mark_pending(self, msg: MQTTMessage) -> None:
        self.pending_acks.append(msg)


class MqttExtractor(Extractor, ABC):
    """Extractor that owns one shared ``MqttConnection`` for its whole lifetime.

    Opens the connection eagerly in ``__aenter__`` — so it is connected (and
    visible in the broker dashboard) even before any config arrives — and
    closes it in ``__aexit__``. Concrete stages declare ``config_topics`` and
    implement ``relay()`` (or pass one to ``of`` / the ``@mqtt_extractor``
    decorator); the template ``poll()`` owns subscription, draining, JSON
    decoding, and the manual-ACK protocol.
    One config per topic: the connection routes inbound by topic and each
    per-topic view owns its buffer + pending-ACK list.
    """

    client_id: str = ""
    """The persistent MQTT session's identity (``clean_session=False``) —
    must be unique per instance and stable across restarts. Set on the
    caller's stage by ``Flechtwerk.configured_stage`` from the module-wide
    ``client_id``; ``__aenter__`` rejects an empty value, since MQTT 3.1.1
    forbids an empty client id with a persistent session."""

    connection: MqttConnection | None = None
    """Built in ``__aenter__`` from the injected settings — pre-set only by
    tests, which thereby bypass the connect."""

    drain_limit: int = 1000
    """Max messages drained per poll() invocation and topic."""

    mqtt: MqttBrokerConfig
    """Broker settings. The stage is application-constructed — never created
    by reactor-di — so this bare annotation is a convention, not a checked
    dependency: ``Flechtwerk.configured_stage`` mutates it onto the caller's
    instance before ``__aenter__``, whose ``getattr`` guard is the only
    enforcement."""

    observer: Observer = Observer()
    """Set on the caller's stage by ``Flechtwerk.configured_stage``; no-op by
    default."""

    @classmethod
    def of(
            cls,
            *,
            config_topics: list[str],
            relay: RelayFn,
            drain_limit: int = 1000,
            enrich_config: EnrichConfigFn | None = None,
            extract_state_key: ExtractStateKeyFn | None = None,
    ) -> "MqttExtractor":
        """Build an MqttExtractor from a relay function and config topics.

        Mirrors ``Extractor.of``: patches the supplied callables in as
        instance attributes that shadow the class-level abstract method
        ``relay`` (and, when provided, the default ``enrich_config`` /
        ``extract_state_key`` methods). The ABC discipline still applies to every
        other construction path.
        """
        instance = _FunctionalMqttExtractor()
        instance.config_topics = config_topics
        instance.drain_limit = drain_limit
        instance.relay = relay
        if enrich_config is not None:
            instance.enrich_config = enrich_config
        if extract_state_key is not None:
            instance.extract_state_key = extract_state_key
        return instance

    async def __aenter__(self) -> Self:
        if self.connection is None:
            if getattr(self, "mqtt", None) is None:
                raise RuntimeError("no MQTT broker configured — pass mqtt=MqttBrokerConfig(...) to Flechtwerk.of")
            if not self.client_id:
                raise RuntimeError(
                    "no MQTT client_id configured — the persistent session (clean_session=False) "
                    "needs a stable, per-instance-unique identity"
                )
            self.wakeup = asyncio.Event()
            self.connection = MqttConnection(
                broker=self.mqtt,
                client_id=self.client_id,
                loop=asyncio.get_running_loop(),
                observer=self.observer,
                wakeup=self.wakeup,
            )
        await self.connection.__aenter__()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.connection.__aexit__(*exc_info)

    def subscribe(self, topic: str) -> MqttSubscription:
        """The per-topic subscription for `topic` over the shared connection."""
        return self.connection.subscribe(topic)

    async def on_active_configs(self, configs: dict[str, Config]) -> None:
        """Reconcile the broker session with the active config set.

        Unsubscribes every topic filter no active config declares and
        latches the declared set as authoritative for the stale-message
        policy (`MqttConnection.reconcile`). Subscribing is not this hook's
        job: the template ``poll()`` (re)subscribes idempotently, so a
        resumed or re-added config reconnects its topic on its next poll.
        """
        self.connection.reconcile({config[TOPIC] for config in configs.values()})

    @abstractmethod
    def relay(self, config: Config, topic: str, payload: Record) -> Message | None:
        """Turn one MQTT message into at most one Kafka message.

        Pure and synchronous — `topic` is the concrete publish topic (not the
        subscription filter), `payload` the JSON-decoded body. Return ``None``
        to drop (the template ACKs immediately); raise — or let a parse error
        escape — to poison-drop (the template logs, ACKs, counts, continues).
        Message-level only: connection failures never reach this hook. Sources
        needing state, 1:N fan-out, or non-JSON payloads override ``poll()``
        instead.

        ``config`` and ``payload`` are read-only and private to each call, so
        mutating either in place has no effect and is silently discarded.
        """

    async def poll(self, config: Config, _: State) -> AsyncIterator[Message | State]:
        sub = self.subscribe(config[TOPIC])

        # ACK the previous batch. ExtractorRunner marks nothing pending that
        # the producer hasn't accepted (it sends each message BEFORE resuming
        # this generator), flushes AND retrieves every delivery result after
        # a completed poll (aiokafka's flush alone never raises), and flushes
        # again in the suspend barrier after a cancelled one; a send or
        # delivery failure crashes the process before reaching here. So
        # everything still pending is provably durable in Kafka now — a
        # cancelled invocation's unsent messages were rolled back to the
        # buffer (below), never marked pending.
        sub.ack_all_pending()

        batch = sub.drain(self.drain_limit)
        self.observer.mqtt_buffered(sub.topic, len(sub.items))
        if sub.items and self.wakeup is not None:
            # More than drain_limit was buffered — re-arm the wakeup so the
            # leftovers drain next cycle instead of one interval later.
            self.wakeup.set()
        if not batch:
            log.debug("No MQTT messages from topic %s", sub.topic)
            return

        log.debug("Draining %d MQTT message(s) from topic %s", len(batch), sub.topic)
        index = 0
        forwarded: list[MQTTMessage] = []
        try:
            for index, msg in enumerate(batch):
                try:
                    # A private config copy per call: relay() is a pure hook, so a
                    # mutation of any parameter must not leak to the next message.
                    message = self.relay(deepcopy(config), msg.topic, Record.wrap(json.loads(msg.payload)))
                except Exception:
                    log.warning("Dropping poison MQTT message from topic %s", msg.topic, exc_info=True)
                    self.observer.mqtt_message_dropped(sub.topic, "poison")
                    sub.ack(msg)
                    continue
                if message is None:
                    self.observer.mqtt_message_dropped(sub.topic, "filtered")
                    sub.ack(msg)
                    continue
                yield message
                forwarded.append(msg)
                sub.mark_pending(msg)
        except BaseException:
            # Interrupted at a yield — GeneratorExit from the runner closing
            # this generator when a poll cycle is cancelled (a token handover
            # mid-batch). Nothing yielded by THIS invocation reached Kafka:
            # the runner sends only after the generator completes. So ACKing
            # any of it at the next entry would silently drop data, and
            # leaving the tail out of the buffer would strand it un-ACKed
            # until the next session restart. Un-mark and restore everything
            # unconfirmed to the buffer front in arrival order —
            # ``batch[index]`` is the message whose yield was interrupted
            # (neither ACKed nor marked pending). Deliberately dropped
            # messages stay dropped; their ACKs are already on the wire.
            for msg in forwarded:
                sub.pending_acks.remove(msg)
            sub.items[:0] = forwarded + batch[index:]
            log.info("Rolled back %d undelivered MQTT message(s) to the %s buffer",
                     len(forwarded) + len(batch) - index, sub.topic)
            raise


class _FunctionalMqttExtractor(MqttExtractor):
    """Shell subclass used solely as the instantiation target for ``MqttExtractor.of``.

    The class-level ``relay = None`` is a placeholder that satisfies
    ``ABCMeta``'s abstract-method check; ``of()`` shadows it with an
    instance attribute on every call.
    """
    relay = None  # type: ignore[assignment]


def mqtt_extractor(
        *,
        config_topics: list[str],
        drain_limit: int = 1000,
        enrich_config: EnrichConfigFn | None = None,
        extract_state_key: ExtractStateKeyFn | None = None,
) -> Callable[[RelayFn], MqttExtractor]:
    """Decorator form of `MqttExtractor.of` — bind a relay function to its config topics.

    The decorated relay function becomes the built `MqttExtractor`, so the name
    you define *is* the stage, ready to hand to ``Flechtwerk.of``::

        @mqtt_extractor(config_topics=["my-config"])
        def stage(config: Config, topic: str, payload: Record) -> Message | None:
            ...

    ``drain_limit``, ``enrich_config``, and ``extract_state_key`` are the same optional
    overrides as on `MqttExtractor.of` — this is exactly that call with ``relay``
    supplied by the decoration. Sources needing state, 1:N fan-out, or non-JSON
    payloads subclass `MqttExtractor` and override ``poll()`` instead.
    """
    def decorator(relay: RelayFn) -> MqttExtractor:
        return MqttExtractor.of(
            config_topics=config_topics,
            drain_limit=drain_limit,
            enrich_config=enrich_config,
            extract_state_key=extract_state_key,
            relay=relay,
        )
    return decorator
