"""Integration tests for the framework MQTT delivery guarantees.

Spins up a real Mosquitto broker via testcontainers and verifies the two
guarantees that unit tests with mocked paho cannot prove:

1. **At-least-once redelivery** — a message received but not ACKed before the
   connection closes is redelivered on reconnect with the same client_id,
   proving that `clean_session=False` + QoS 1 + a stable client_id delivers
   the at-least-once guarantee end-to-end through a real broker.
2. **ACK stops redelivery** — an inline `sub.ack(msg)` (the drop branch)
   actually prevents broker redelivery, i.e. dropped messages are not leaked
   into the broker's inflight buffer.

Skipped automatically when Docker is not reachable.
"""
import asyncio
import json
import shlex

import pytest

from fretworx.mqtt import MqttBrokerConfig, MqttConnection, MqttSubscription

pytestmark = pytest.mark.integration


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
    topic_pattern = "fretworx/test-redeliver/+/events"
    device_topic = "fretworx/test-redeliver/001122334455/events"
    payload = json.dumps({"anything": "here"})
    client_id = "fretworx-integ-redeliver"
    loop = asyncio.get_running_loop()

    conn1 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
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

    conn2 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
    async with conn2:
        sub2 = conn2.subscribe(topic_pattern)
        await wait_for_items(sub2, timeout=15.0)
        batch = sub2.drain(limit=10)

    assert len(batch) == 1
    assert batch[0].topic == device_topic
    assert batch[0].payload == payload.encode()


async def test_acked_message_is_not_redelivered(broker, mosquitto_broker) -> None:
    topic_pattern = "fretworx/test-badack/+/info"
    device_topic = "fretworx/test-badack/aabb/info"
    payload = json.dumps({"anything": "here"})
    client_id = "fretworx-integ-badack"
    loop = asyncio.get_running_loop()

    conn1 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
    async with conn1:
        sub1 = conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)
        publish_qos1(mosquitto_broker, device_topic, payload)
        await wait_for_items(sub1, timeout=10.0)
        batch = sub1.drain(limit=10)
        assert len(batch) == 1
        sub1.ack(batch[0])  # the drop branch: ACK inline, drop the message
        await asyncio.sleep(0.5)  # let the PUBACK flush to the broker

    conn2 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
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
    topic_pattern = "fretworx/test-startup/+/events"
    device_topic = "fretworx/test-startup/aabb/events"
    payload = json.dumps({"anything": "here"})
    client_id = "fretworx-integ-startup"
    loop = asyncio.get_running_loop()

    # Session setup: subscribe, then go away with a queued backlog.
    conn1 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
    async with conn1:
        conn1.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
    await asyncio.sleep(0.5)
    publish_qos1(mosquitto_broker, device_topic, payload)  # queued for the offline session

    # Restart: connect WITHOUT subscribing — mirroring production, where the
    # Kafka config bootstrap delays subscribe() well past CONNACK.
    conn2 = MqttConnection(broker=broker, client_id=client_id, loop=loop)
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
    topic_pattern = "fretworx/test-wakeup/+/events"
    wakeup = asyncio.Event()
    loop = asyncio.get_running_loop()

    conn = MqttConnection(broker=broker, client_id="fretworx-integ-wakeup", loop=loop, wakeup=wakeup)
    async with conn:
        sub = conn.subscribe(topic_pattern)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
        publish_qos1(mosquitto_broker, "fretworx/test-wakeup/aabb/events", "{}")

        await asyncio.wait_for(wakeup.wait(), timeout=10.0)

        assert len(sub.items) == 1  # append-then-set: drainable once woken
