"""Integration tests for the framework MQTT delivery guarantees.

Spins up a real Mosquitto broker (>= 2.0: MQTT 5 and shared subscriptions)
via testcontainers and verifies what unit tests with mocked paho cannot
prove:

1. **At-least-once redelivery** — a message received but not ACKed before the
   connection closes is redelivered on reconnect with the same client_id,
   proving that `clean_start=False` + a non-zero session expiry + QoS 1 + a
   stable client_id delivers the at-least-once guarantee end-to-end through
   a real broker.
2. **ACK stops redelivery** — an inline `sub.ack(msg)` (the drop branch)
   actually prevents broker redelivery, i.e. dropped messages are not leaked
   into the broker's inflight buffer.
3. **Shared-subscription dispatch** — the two facts the 0.9 design leans on:
   a `$share` subscription with a SOLE member still queues into that
   member's offline session (so the common single-replica deployment keeps
   its backlog protection), and two members of one group split the traffic
   without double delivery, with a departure failing subsequent messages
   over to the survivor.

Skipped automatically when Docker is not reachable.
"""
import asyncio
import json
import shlex
from datetime import timedelta

import pytest

from flechtwerk.mqtt import MqttBrokerConfig, MqttConnection, MqttSubscription

pytestmark = pytest.mark.integration

GROUP = "flechtwerk-integ"


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


_MOSQUITTO_CONFIG = """\
listener 1883
protocol mqtt
allow_anonymous true
log_dest stdout
log_type error
log_type warning
log_type notice
log_type information
persistence true
persistence_location /mosquitto/data/
"""


@pytest.fixture(scope="session")
def mosquitto_broker():
    """Start a Mosquitto broker once per test session.

    Injects the config via the container command rather than a host
    bind-mount. The default testcontainers config writes to /data/ which
    is not writable in the eclipse-mosquitto image, so a custom config is
    required — but bind-mounting a single file fails on Docker-outside-of-
    Docker CI runners (the daemon's filesystem view differs from the
    runner pod's). Writing the config inside the container sidesteps both.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.mqtt import MosquittoContainer

    container = MosquittoContainer()
    container.with_exposed_ports(MosquittoContainer.MQTT_PORT)
    container.with_command([
        "sh", "-c",
        f"printf %s {shlex.quote(_MOSQUITTO_CONFIG)} > /tmp/mosquitto.conf"
        " && exec mosquitto -c /tmp/mosquitto.conf",
    ])
    try:
        # Skip MosquittoContainer.start() because it always bind-mounts a
        # config file; call DockerContainer.start() directly.
        super(MosquittoContainer, container).start()
        container._wait()
        yield container
    finally:
        container.stop()


@pytest.fixture
def broker(mosquitto_broker) -> MqttBrokerConfig:
    return MqttBrokerConfig(
        broker=mosquitto_broker.get_container_host_ip(),
        port=int(mosquitto_broker.get_exposed_port(1883)),
    )


def publish_qos1(broker, topic: str, payload: str) -> None:
    """Publish a QoS 1 message. MosquittoContainer.publish_message() defaults
    to QoS 0, which has no session state — useless for redelivery testing.
    """
    info = broker.get_client().publish(topic, payload, qos=1)
    info.wait_for_publish(timeout=5)
    if not info.is_published():
        raise RuntimeError(f"Publish to {topic} did not complete: {info}")


def connect(broker: MqttBrokerConfig, client_id: str, group: str = GROUP) -> MqttConnection:
    """An MqttConnection on the running loop — every subscription it makes
    goes on the wire as ``$share/{group}/{filter}``."""
    return MqttConnection(
        broker=broker, client_id=client_id, group=group, loop=asyncio.get_running_loop(),
    )


async def wait_for_items(sub: MqttSubscription, timeout: float) -> None:
    """Poll-sleep until `sub.items` is non-empty or `timeout` elapses.

    Production drain() is synchronous and returns [] immediately when nothing
    is buffered — the runner's idle wait provides the cadence. Integration
    tests still need to wait for real MQTT round-trips, so they block here
    before draining.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if sub.items:
            return
        await asyncio.sleep(0.05)


async def test_unacked_message_is_redelivered(broker, mosquitto_broker) -> None:
    topic_pattern = "flechtwerk/test-redeliver/+/events"
    device_topic = "flechtwerk/test-redeliver/001122334455/events"
    payload = json.dumps({"anything": "here"})
    client_id = "flechtwerk-integ-redeliver"

    conn1 = connect(broker, client_id)
    async with conn1:
        sub1 = conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
        publish_qos1(mosquitto_broker, device_topic, payload)
        await wait_for_items(sub1, timeout=10.0)
        batch = sub1.drain(limit=10)
        assert len(batch) == 1
        sub1.mark_pending(batch[0])
        # No ack_all_pending() — simulates a crash between Kafka send and the
        # next poll() where the ACK would normally happen.

    # Let conn1's DISCONNECT packet flush to the broker before conn2 connects;
    # otherwise conn2 triggers a session takeover that races with conn1's close.
    await asyncio.sleep(1.0)

    conn2 = connect(broker, client_id)
    async with conn2:
        sub2 = conn2.subscribe(topic_pattern)
        await wait_for_items(sub2, timeout=15.0)
        batch = sub2.drain(limit=10)

    assert len(batch) == 1
    assert batch[0].topic == device_topic
    assert batch[0].payload == payload.encode()


async def test_acked_message_is_not_redelivered(broker, mosquitto_broker) -> None:
    topic_pattern = "flechtwerk/test-badack/+/info"
    device_topic = "flechtwerk/test-badack/aabb/info"
    payload = json.dumps({"anything": "here"})
    client_id = "flechtwerk-integ-badack"

    conn1 = connect(broker, client_id)
    async with conn1:
        sub1 = conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)
        publish_qos1(mosquitto_broker, device_topic, payload)
        await wait_for_items(sub1, timeout=10.0)
        batch = sub1.drain(limit=10)
        assert len(batch) == 1
        sub1.ack(batch[0])  # the drop branch: ACK inline, drop the message
        await asyncio.sleep(0.5)  # let the PUBACK flush to the broker

    conn2 = connect(broker, client_id)
    async with conn2:
        sub2 = conn2.subscribe(topic_pattern)
        await wait_for_items(sub2, timeout=3.0)
        batch = sub2.drain(limit=10)

    assert batch == []


async def test_backlog_replayed_before_subscribe_is_held_and_routed(broker, mosquitto_broker) -> None:
    """The startup window: on reconnect the persistent session replays its
    queued backlog right after CONNACK — before the config bootstrap has
    registered any subscription. Those messages must be held un-ACKed and
    routed once the subscription registers, never ACK-dropped."""
    topic_pattern = "flechtwerk/test-startup/+/events"
    device_topic = "flechtwerk/test-startup/aabb/events"
    payload = json.dumps({"anything": "here"})
    client_id = "flechtwerk-integ-startup"
    loop = asyncio.get_running_loop()

    # Session setup: subscribe, then go away with a queued backlog.
    conn1 = connect(broker, client_id)
    async with conn1:
        conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
    await asyncio.sleep(0.5)
    publish_qos1(mosquitto_broker, device_topic, payload)  # queued for the offline session

    # Restart: connect WITHOUT subscribing — mirroring production, where the
    # Kafka config bootstrap delays subscribe() well past CONNACK.
    conn2 = connect(broker, client_id)
    async with conn2:
        deadline = loop.time() + 10.0
        while loop.time() < deadline and not conn2.unrouted:
            await asyncio.sleep(0.05)
        assert len(conn2.unrouted) == 1  # replayed, held un-ACKed — not dropped

        sub = conn2.subscribe(topic_pattern)
        batch = sub.drain(limit=10)

    assert len(batch) == 1
    assert batch[0].topic == device_topic
    assert batch[0].payload == payload.encode()


async def test_wakeup_fires_on_arrival(broker, mosquitto_broker) -> None:
    """The wakeup event is set by a real broker round-trip — the runner's
    idle wait would end the moment the message lands, not at the interval."""
    topic_pattern = "flechtwerk/test-wakeup/+/events"
    wakeup = asyncio.Event()
    loop = asyncio.get_running_loop()

    conn = MqttConnection(
        broker=broker, client_id="flechtwerk-integ-wakeup", group=GROUP, loop=loop, wakeup=wakeup,
    )
    async with conn:
        sub = conn.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
        publish_qos1(mosquitto_broker, "flechtwerk/test-wakeup/aabb/events", "{}")

        await asyncio.wait_for(wakeup.wait(), timeout=10.0)

        assert len(sub.items) == 1  # append-then-set: drainable once woken


# -- shared-subscription dispatch ----------------------------------------------


async def test_shared_subscription_queues_for_a_sole_offline_member(broker, mosquitto_broker) -> None:
    """The fact the whole dispatch design leans on: with ONE member in the
    group, a `$share` subscription still queues into that member's offline
    session and delivers on resume. Without it, the common single-replica
    deployment would lose everything published while its pod restarts —
    exactly the backlog protection the pre-0.9 direct subscription gave."""
    topic_pattern = "flechtwerk/test-share-backlog/+/events"
    device_topic = "flechtwerk/test-share-backlog/aabb/events"
    payload = json.dumps({"anything": "here"})
    client_id = "flechtwerk-integ-share-backlog"

    conn1 = connect(broker, client_id)
    async with conn1:
        conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
    await asyncio.sleep(0.5)
    publish_qos1(mosquitto_broker, device_topic, payload)  # nobody is online

    conn2 = connect(broker, client_id)
    async with conn2:
        sub = conn2.subscribe(topic_pattern)
        await wait_for_items(sub, timeout=15.0)
        batch = sub.drain(limit=10)

    assert [m.payload for m in batch] == [payload.encode()]


async def test_group_members_split_traffic_and_survive_a_departure(broker, mosquitto_broker) -> None:
    """Two members of one share group: every message reaches exactly one of
    them (no double delivery — which is what makes running two replicas of
    an MQTT stage safe), and once one leaves for good the survivor picks up
    the subsequent traffic.

    The departing member connects with ``session_expiry=0`` so its session
    really ends at DISCONNECT, making the failover deterministic. Flechtwerk
    itself never does that: ending a session silently discards its un-ACKed
    inflight messages, which no other member is given.
    """
    topic_pattern = "flechtwerk/test-share-split/+/events"
    device_topic = "flechtwerk/test-share-split/aabb/events"
    group = "flechtwerk-integ-split"
    ephemeral = MqttBrokerConfig(
        broker=broker.broker, port=broker.port, session_expiry=timedelta(0),
    )

    staying = connect(broker, "flechtwerk-integ-split-b", group=group)
    async with staying:
        sub_b = staying.subscribe(topic_pattern)
        leaving = MqttConnection(
            broker=ephemeral, client_id="flechtwerk-integ-split-a", group=group,
            loop=asyncio.get_running_loop(),
        )
        async with leaving:
            sub_a = leaving.subscribe(topic_pattern)
            await asyncio.sleep(1.0)  # wait for both SUBACKs

            for i in range(6):
                publish_qos1(mosquitto_broker, device_topic, json.dumps({"i": i}))
            deadline = asyncio.get_event_loop().time() + 15.0
            while asyncio.get_event_loop().time() < deadline:
                if len(sub_a.items) + len(sub_b.items) >= 6:
                    break
                await asyncio.sleep(0.05)

            # ACK everything before the leaving member goes: an un-ACKed tail
            # is discarded with its session, never handed to the survivor.
            batch_a, batch_b = sub_a.drain(limit=20), sub_b.drain(limit=20)
            for sub, batch in ((sub_a, batch_a), (sub_b, batch_b)):
                for msg in batch:
                    sub.ack(msg)
            payloads = [m.payload for m in batch_a + batch_b]
            assert sorted(payloads) == sorted(json.dumps({"i": i}).encode() for i in range(6))
            assert len(set(payloads)) == 6  # each message reached exactly one member
            await asyncio.sleep(0.5)  # let the PUBACKs flush

        # The staying member is now the group's only member.
        await asyncio.sleep(1.0)
        publish_qos1(mosquitto_broker, device_topic, json.dumps({"i": "after"}))
        await wait_for_items(sub_b, timeout=10.0)

    assert [m.payload for m in sub_b.drain(limit=10)] == [json.dumps({"i": "after"}).encode()]
