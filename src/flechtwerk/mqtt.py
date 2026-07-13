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
from .configs import EnrichFn
from .extractor import ExtractKeyFn, Extractor
from .module import MqttBrokerConfig
from .observer import Observer
from .types import Config, Message, State

if TYPE_CHECKING:
    from paho.mqtt.client import SocketLike

log = logging.getLogger(__name__)

__all__ = ["MqttBrokerConfig", "MqttConnection", "MqttExtractor", "mqtt_extractor", "MqttSubscription", "TOPIC"]

TOPIC: Final = Attribute("topic", STR)
"""Required config field: the MQTT topic filter a config subscribes to.

The template ``poll()`` is its only reader — one config record per MQTT
topic, each becoming a per-topic subscription over the shared connection.
Two configs must never declare the same (or an overlapping) topic filter:
they would share one subscription view and ACK each other's batches before
the other's messages reached Kafka.
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
        # QoS >= 1: hold un-ACKed and re-route when a matching subscription
        # appears. This is the normal startup path — the persistent session
        # replays its backlog right after CONNACK, before the Kafka config
        # bootstrap has registered any subscription — so ACKing here would
        # permanently drop the very backlog clean_session=False exists to
        # protect. Held messages are bounded by the broker's per-session
        # inflight window (un-ACKed QoS 1 deliveries pause the session), and
        # messages for a genuinely stale session subscription therefore stall
        # that window — the documented cost of deferring unsubscribe.
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
            enrich: EnrichFn | None = None,
            extract_key: ExtractKeyFn | None = None,
    ) -> "MqttExtractor":
        """Build an MqttExtractor from a relay function and config topics.

        Mirrors ``Extractor.of``: patches the supplied callables in as
        instance attributes that shadow the class-level abstract method
        ``relay`` (and, when provided, the default ``enrich`` /
        ``extract_key`` methods). The ABC discipline still applies to every
        other construction path.
        """
        instance = _FunctionalMqttExtractor()
        instance.config_topics = config_topics
        instance.drain_limit = drain_limit
        instance.relay = relay
        if enrich is not None:
            instance.enrich = enrich
        if extract_key is not None:
            instance.extract_key = extract_key
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

        # ACK the previous batch. ExtractorRunner only re-enters poll() after
        # the previous poll's send_batch() + flush succeeded (see its
        # documented re-entry contract) — any failure would have crashed the
        # process before reaching here — so everything pending is provably
        # durable in Kafka now.
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

        log.info("Draining %d MQTT message(s) from topic %s", len(batch), sub.topic)
        for msg in batch:
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
            sub.mark_pending(msg)


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
        enrich: EnrichFn | None = None,
        extract_key: ExtractKeyFn | None = None,
) -> Callable[[RelayFn], MqttExtractor]:
    """Decorator form of `MqttExtractor.of` — bind a relay function to its config topics.

    The decorated relay function becomes the built `MqttExtractor`, so the name
    you define *is* the stage, ready to hand to ``Flechtwerk.of``::

        @mqtt_extractor(config_topics=["my-config"])
        def stage(config: Config, topic: str, payload: Record) -> Message | None:
            ...

    ``drain_limit``, ``enrich``, and ``extract_key`` are the same optional
    overrides as on `MqttExtractor.of` — this is exactly that call with ``relay``
    supplied by the decoration. Sources needing state, 1:N fan-out, or non-JSON
    payloads subclass `MqttExtractor` and override ``poll()`` instead.
    """
    def decorator(relay: RelayFn) -> MqttExtractor:
        return MqttExtractor.of(
            config_topics=config_topics,
            drain_limit=drain_limit,
            enrich=enrich,
            extract_key=extract_key,
            relay=relay,
        )
    return decorator
