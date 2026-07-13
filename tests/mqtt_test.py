"""Tests for flechtwerk.mqtt — connection machinery and the relay template."""
import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from flechtwerk import Config, Event, Message, State
from flechtwerk.attribute import Record
from flechtwerk.mqtt import MqttBrokerConfig, MqttConnection, MqttExtractor, mqtt_extractor
from flechtwerk.testing import FakeMqttConnection, RecordingObserver, make_mqtt_message as make_message

DEFAULT_BROKER = MqttBrokerConfig(broker="localhost", port=1883, qos=1)


def make_mqtt_message(topic: str, payload: dict, qos: int = 1, mid: int = 42):
    return make_message(topic=topic, payload=json.dumps(payload).encode(), mid=mid, qos=qos)


def make_connection(
    loop: asyncio.AbstractEventLoop,
    *,
    broker: MqttBrokerConfig = DEFAULT_BROKER,
    client_id: str = "c-0",
    observer: RecordingObserver | None = None,
    wakeup: asyncio.Event | None = None,
) -> MqttConnection:
    return MqttConnection(broker=broker, client_id=client_id, loop=loop, observer=observer, wakeup=wakeup)


@pytest.fixture
def mock_client():
    with patch("flechtwerk.mqtt.Client") as MockClient:
        client = MagicMock()
        client._client_id = b"test-client"
        client.socket.return_value = MagicMock()
        MockClient.return_value = client
        yield MockClient, client


# -- Connection: connect / credentials --------------------------------------


@pytest.mark.asyncio
async def test_connects_with_at_least_once_params(mock_client):
    """Client constructed with manual_ack=True, clean_session=False, the client_id."""
    MockClient, client = mock_client
    conn = make_connection(asyncio.get_running_loop(), client_id="pod-0")
    await conn.__aenter__()

    _, kwargs = MockClient.call_args
    assert kwargs["client_id"] == "pod-0"
    assert kwargs["clean_session"] is False
    assert kwargs["manual_ack"] is True
    client.connect.assert_called_once()
    client.socket().setblocking.assert_called_once_with(False)
    await conn.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_uses_broker_and_port_from_config(mock_client):
    _, client = mock_client
    conn = make_connection(
        asyncio.get_running_loop(),
        broker=MqttBrokerConfig(broker="broker.example", port=8883, qos=1),
    )
    await conn.__aenter__()

    client.connect.assert_called_once_with("broker.example", 8883)
    await conn.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_applies_credentials_when_username_set(mock_client):
    _, client = mock_client
    make_connection(
        asyncio.get_running_loop(),
        broker=MqttBrokerConfig(broker="b", port=1883, qos=1, username="u", password="secret"),
    )

    client.username_pw_set.assert_called_once_with("u", "secret")


@pytest.mark.asyncio
async def test_skips_credentials_when_username_empty(mock_client):
    _, client = mock_client
    make_connection(asyncio.get_running_loop(), broker=MqttBrokerConfig(broker="b", port=1883, qos=1))

    client.username_pw_set.assert_not_called()


@pytest.mark.asyncio
async def test_aexit_disconnects(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())

    await conn.__aexit__(None, None, None)

    client.disconnect.assert_called_once()


# -- subscribe / routing -----------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_is_idempotent_per_topic(mock_client):
    conn = make_connection(asyncio.get_running_loop())

    a = conn.subscribe("xovis/+/+/events")
    b = conn.subscribe("xovis/+/+/events")

    assert a is b
    assert list(conn.subscriptions) == ["xovis/+/+/events"]


@pytest.mark.asyncio
async def test_subscribe_when_connected_subscribes_immediately(mock_client):
    _, client = mock_client
    client.is_connected.return_value = True
    conn = make_connection(asyncio.get_running_loop())

    conn.subscribe("t/+/events")

    client.subscribe.assert_called_once_with("t/+/events", qos=1)


@pytest.mark.asyncio
async def test_subscribe_when_disconnected_defers_to_on_connect(mock_client):
    _, client = mock_client
    client.is_connected.return_value = False
    conn = make_connection(asyncio.get_running_loop())

    conn.subscribe("a/+/events")
    conn.subscribe("b/+/events")
    client.subscribe.assert_not_called()

    conn.on_connect(client, None, None, 0, None)

    assert client.subscribe.call_count == 2
    client.subscribe.assert_any_call("a/+/events", qos=1)
    client.subscribe.assert_any_call("b/+/events", qos=1)


@pytest.mark.asyncio
async def test_on_message_routes_to_matching_subscription(mock_client):
    conn = make_connection(asyncio.get_running_loop())
    xovis = conn.subscribe("xovis/+/+/events")
    other = conn.subscribe("whatsapp")
    msg = make_mqtt_message("xovis/stage/aabb/events", {"x": 1})

    conn.on_message(None, None, msg)

    assert xovis.items == [msg]
    assert other.items == []


@pytest.mark.asyncio
async def test_on_message_holds_unmatched_qos1_without_ack(mock_client):
    """An unmatched QoS 1 message is held un-ACKed — ACKing (or dropping)
    would permanently lose the persistent session's backlog, which the broker
    replays right after CONNACK, before the config bootstrap has registered
    any subscription."""
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("xovis/+/+/events")
    msg = make_mqtt_message("whatsapp/messages", {}, qos=1, mid=7)

    conn.on_message(None, None, msg)

    assert sub.items == []
    assert conn.unrouted == [msg]
    client.ack.assert_not_called()


@pytest.mark.asyncio
async def test_on_message_drops_unmatched_qos0(mock_client):
    """QoS 0 carries no session state to protect — unmatched is dropped."""
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    conn.subscribe("xovis/+/+/events")

    conn.on_message(None, None, make_mqtt_message("whatsapp/messages", {}, qos=0, mid=8))

    assert conn.unrouted == []
    client.ack.assert_not_called()


@pytest.mark.asyncio
async def test_subscribe_routes_held_messages_into_new_subscription(mock_client):
    """The startup window: backlog replayed before subscriptions exist is
    held, then routed — in arrival order, ahead of newer messages — the
    moment the matching subscription registers."""
    conn = make_connection(asyncio.get_running_loop())
    early_1 = make_mqtt_message("t/aa/events", {"i": 1}, mid=1)
    early_2 = make_mqtt_message("t/bb/events", {"i": 2}, mid=2)
    other = make_mqtt_message("other/topic", {}, mid=3)
    for msg in (early_1, early_2, other):
        conn.on_message(None, None, msg)
    assert conn.unrouted == [early_1, early_2, other]

    sub = conn.subscribe("t/+/events")
    conn.on_message(None, None, make_mqtt_message("t/aa/events", {"i": 4}, mid=4))

    assert [m.mid for m in sub.items] == [1, 2, 4]  # held first, then live
    assert conn.unrouted == [other]  # still held for a later subscription


@pytest.mark.asyncio
async def test_reconnect_clears_held_unrouted_messages(mock_client):
    """On reconnect the broker redelivers with fresh mids — held messages
    reference stale mids and must be cleared like pending_acks."""
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    conn.on_message(None, None, make_mqtt_message("nobody/listens", {}, mid=1))
    assert conn.unrouted != []

    conn.on_connect(client, None, None, 0, None)

    assert conn.unrouted == []


@pytest.mark.asyncio
async def test_on_message_sets_wakeup_after_buffering(mock_client):
    """Append-then-set: the wakeup fires only after the message is drainable."""
    wakeup = asyncio.Event()
    conn = make_connection(asyncio.get_running_loop(), wakeup=wakeup)
    sub = conn.subscribe("t/+/events")

    conn.on_message(None, None, make_mqtt_message("t/aa/events", {}))

    assert wakeup.is_set()
    assert len(sub.items) == 1


@pytest.mark.asyncio
async def test_connection_failures_set_wakeup(mock_client):
    """Connect failures and unexpected disconnects wake the runner so the
    error surfaces from the next drain() immediately, not up to a full
    poll interval later."""
    _, client = mock_client
    wakeup = asyncio.Event()
    conn = make_connection(asyncio.get_running_loop(), wakeup=wakeup)

    conn.on_connect(client, None, None, 134, None)
    assert wakeup.is_set()

    wakeup.clear()
    conn.on_disconnect(client, None, None, 7, None)
    assert wakeup.is_set()

    wakeup.clear()
    conn.on_disconnect(client, None, None, 0, None)  # clean shutdown: no wake
    assert not wakeup.is_set()


@pytest.mark.asyncio
async def test_connection_emits_observer_events(mock_client):
    _, client = mock_client
    observer = RecordingObserver()
    conn = make_connection(asyncio.get_running_loop(), observer=observer)
    conn.subscribe("t/+/events")

    conn.on_connect(client, None, None, 0, None)
    conn.on_message(None, None, make_mqtt_message("t/aa/events", {}))
    conn.on_disconnect(client, None, None, 7, None)

    assert ("mqtt_connected",) in observer.calls
    assert ("mqtt_message_in", "t/+/events") in observer.calls
    assert ("mqtt_disconnected",) in observer.calls


@pytest.mark.asyncio
async def test_clean_disconnect_emits_no_observer_event(mock_client):
    _, client = mock_client
    observer = RecordingObserver()
    conn = make_connection(asyncio.get_running_loop(), observer=observer)

    conn.on_disconnect(client, None, None, 0, None)

    assert observer.calls == []


# -- connection-level errors surface via drain -------------------------------


@pytest.mark.asyncio
async def test_connect_failure_surfaces_in_drain(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")

    conn.on_connect(client, None, None, 134, None)

    with pytest.raises(ConnectionError, match="MQTT connect failed: 134"):
        sub.drain(limit=10)


@pytest.mark.asyncio
async def test_unexpected_disconnect_surfaces_in_drain(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")

    conn.on_disconnect(client, None, None, 7, None)

    with pytest.raises(ConnectionError, match="MQTT disconnected: 7"):
        sub.drain(limit=10)


@pytest.mark.asyncio
async def test_clean_disconnect_is_ignored(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")

    conn.on_disconnect(client, None, None, 0, None)

    assert sub.drain(limit=10) == []


@pytest.mark.asyncio
async def test_buffered_messages_drain_before_error(mock_client):
    """Messages that arrived before a failure are drained first; the error
    surfaces only once the buffer is empty."""
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    conn.on_message(None, None, make_mqtt_message("t/aa/events", {}, mid=1))
    conn.on_connect(client, None, None, 134, None)

    assert len(sub.drain(limit=10)) == 1
    with pytest.raises(ConnectionError):
        sub.drain(limit=10)


@pytest.mark.asyncio
async def test_on_connect_clears_stale_pending_acks(mock_client):
    """On reconnect, pending_acks must be cleared — old mids are stale."""
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    sub.pending_acks.append(make_mqtt_message("t/aa/events", {}, mid=1))
    sub.pending_acks.append(make_mqtt_message("t/aa/events", {}, mid=2))

    conn.on_connect(client, None, None, 0, None)

    assert sub.pending_acks == []


# -- subscription: drain / ack ----------------------------------------------


@pytest.mark.asyncio
async def test_drain_returns_accumulated_messages(mock_client):
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    for i in range(5):
        conn.on_message(None, None, make_mqtt_message("t/aa/events", {}, mid=i))

    batch = sub.drain(limit=10)

    assert len(batch) == 5
    assert sub.items == []


@pytest.mark.asyncio
async def test_drain_returns_empty_when_no_messages(mock_client):
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    assert sub.drain(limit=10) == []


@pytest.mark.asyncio
async def test_drain_respects_limit(mock_client):
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    for i in range(5):
        conn.on_message(None, None, make_mqtt_message("t/aa/events", {}, mid=i))

    batch = sub.drain(limit=3)

    assert len(batch) == 3
    assert len(sub.items) == 2


@pytest.mark.asyncio
async def test_ack_all_pending_acks_qos_1_messages(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    sub.pending_acks = [
        make_mqtt_message("t/aa/events", {}, qos=1, mid=1),
        make_mqtt_message("t/aa/events", {}, qos=1, mid=2),
    ]

    sub.ack_all_pending()

    assert client.ack.call_count == 2
    client.ack.assert_any_call(1, 1)
    client.ack.assert_any_call(2, 1)
    assert sub.pending_acks == []


@pytest.mark.asyncio
async def test_ack_all_pending_skips_qos_0_messages(mock_client):
    _, client = mock_client
    conn = make_connection(asyncio.get_running_loop())
    sub = conn.subscribe("t/+/events")
    sub.pending_acks = [make_mqtt_message("t/aa/events", {}, qos=0, mid=1)]

    sub.ack_all_pending()

    client.ack.assert_not_called()
    assert sub.pending_acks == []


# -- socket callbacks --------------------------------------------------------


@pytest.mark.asyncio
async def test_on_socket_open_registers_reader(mock_client):
    _, client = mock_client
    loop = asyncio.get_running_loop()
    conn = make_connection(loop)
    mock_sock = MagicMock()

    with patch.object(loop, "add_reader") as add_reader:
        conn.on_socket_open(client, None, mock_sock)
        add_reader.assert_called_once_with(mock_sock, client.loop_read)

    if conn.misc_task:
        conn.misc_task.cancel()
        try:
            await conn.misc_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_on_socket_close_removes_reader_writer(mock_client):
    _, client = mock_client
    loop = asyncio.get_running_loop()
    conn = make_connection(loop)
    mock_sock = MagicMock()

    with (
        patch.object(loop, "remove_reader") as remove_reader,
        patch.object(loop, "remove_writer") as remove_writer,
    ):
        conn.on_socket_close(client, None, mock_sock)
        remove_reader.assert_called_once_with(mock_sock)
        remove_writer.assert_called_once_with(mock_sock)


# -- MqttExtractor: lifecycle ------------------------------------------------


def forward_relay(config: Config, topic: str, payload: Record) -> Message | None:
    return Message(key=topic, topic="out", value=Event.wrap(payload.raw))


def test_extractor_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        MqttExtractor()


@pytest.mark.asyncio
async def test_extractor_aenter_without_settings_raises():
    ext = MqttExtractor.of(config_topics=["cfg"], relay=forward_relay)
    with pytest.raises(RuntimeError, match="no MQTT broker configured"):
        await ext.__aenter__()


@pytest.mark.asyncio
async def test_extractor_aenter_without_client_id_raises():
    """An empty client_id would collide-or-be-rejected at the broker (MQTT
    3.1.1 forbids it with clean_session=False) — fail fast instead."""
    ext = MqttExtractor.of(config_topics=["cfg"], relay=forward_relay)
    ext.mqtt = MqttBrokerConfig(broker="b", port=1883)
    with pytest.raises(RuntimeError, match="no MQTT client_id configured"):
        await ext.__aenter__()


@pytest.mark.asyncio
async def test_extractor_aenter_builds_connection_from_injected_settings(mock_client):
    MockClient, client = mock_client
    ext = MqttExtractor.of(config_topics=["cfg"], relay=forward_relay)
    ext.client_id = "pod-0"
    ext.mqtt = MqttBrokerConfig(broker="broker.example", port=8883)

    async with ext:
        _, kwargs = MockClient.call_args
        assert kwargs["client_id"] == "pod-0"
        client.connect.assert_called_once_with("broker.example", 8883)
        assert isinstance(ext.wakeup, asyncio.Event)
        assert ext.connection.wakeup is ext.wakeup
        assert ext.connection.observer is ext.observer
    client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_extractor_aenter_skips_connect_when_connection_preset():
    """The test seam: a pre-set connection (e.g. FakeMqttConnection) bypasses
    both the broker connect and the injected-settings requirement."""
    ext = MqttExtractor.of(config_topics=["cfg"], relay=forward_relay)
    ext.connection = FakeMqttConnection()

    async with ext:
        assert isinstance(ext.connection, FakeMqttConnection)


# -- MqttExtractor: the relay template ---------------------------------------


CONFIG = Config.wrap({"topic": "t/+/events"})


def make_template_extractor(relay, drain_limit: int = 1000) -> MqttExtractor:
    ext = MqttExtractor.of(config_topics=["cfg"], relay=relay, drain_limit=drain_limit)
    ext.connection = FakeMqttConnection()
    # Subscribe upfront, mirroring production where the SUBACK always precedes
    # delivery — the fake routes publishes only to existing subscriptions.
    ext.connection.subscribe(CONFIG.raw["topic"])
    ext.observer = RecordingObserver()
    return ext


async def collect_poll(ext: MqttExtractor, config: Config = CONFIG) -> list[Message]:
    return [item async for item in ext.poll(config, State())]


@pytest.mark.asyncio
async def test_template_forwards_and_acks_previous_batch():
    ext = make_template_extractor(forward_relay)
    ext.connection.publish(topic="t/aa/events", payload=b'{"x": 1}')

    messages = await collect_poll(ext)

    assert len(messages) == 1
    assert messages[0].key == "t/aa/events"
    assert messages[0].value.raw == {"x": 1}
    sub = ext.connection.subscriptions["t/+/events"]
    assert len(sub.pending_acks) == 1
    assert sub.acked == []

    # The next poll ACKs the previous batch — provably durable in Kafka by
    # then (the runner's documented re-entry contract).
    assert await collect_poll(ext) == []
    assert len(sub.acked) == 1
    assert sub.pending_acks == []


@pytest.mark.asyncio
async def test_template_filtered_drop_acks_immediately():
    ext = make_template_extractor(lambda config, topic, payload: None)
    ext.connection.publish(topic="t/aa/events", payload=b"{}")

    assert await collect_poll(ext) == []

    sub = ext.connection.subscriptions["t/+/events"]
    assert len(sub.acked) == 1
    assert sub.pending_acks == []
    assert ("mqtt_message_dropped", "t/+/events", "filtered") in ext.observer.calls


@pytest.mark.asyncio
async def test_template_poison_drop_on_relay_exception():
    def bomb(config, topic, payload):
        raise ValueError("boom")

    ext = make_template_extractor(bomb)
    ext.connection.publish(topic="t/aa/events", payload=b"{}")

    assert await collect_poll(ext) == []

    sub = ext.connection.subscriptions["t/+/events"]
    assert len(sub.acked) == 1
    assert sub.pending_acks == []
    assert ("mqtt_message_dropped", "t/+/events", "poison") in ext.observer.calls


@pytest.mark.asyncio
async def test_template_poison_drop_on_malformed_json():
    ext = make_template_extractor(forward_relay)
    ext.connection.publish(topic="t/aa/events", payload=b"not json")

    assert await collect_poll(ext) == []

    sub = ext.connection.subscriptions["t/+/events"]
    assert len(sub.acked) == 1
    assert ("mqtt_message_dropped", "t/+/events", "poison") in ext.observer.calls


@pytest.mark.asyncio
async def test_template_poison_does_not_stall_following_messages():
    ext = make_template_extractor(forward_relay)
    ext.connection.publish(topic="t/aa/events", payload=b"not json")
    ext.connection.publish(topic="t/aa/events", payload=b'{"x": 2}')

    messages = await collect_poll(ext)

    assert len(messages) == 1
    assert messages[0].value.raw == {"x": 2}


@pytest.mark.asyncio
async def test_template_respects_drain_limit_and_reports_backlog():
    ext = make_template_extractor(forward_relay, drain_limit=2)
    for i in range(3):
        ext.connection.publish(topic="t/aa/events", payload=json.dumps({"i": i}).encode())

    messages = await collect_poll(ext)

    assert len(messages) == 2
    assert len(ext.connection.subscriptions["t/+/events"].items) == 1
    assert ("mqtt_buffered", "t/+/events", 1) in ext.observer.calls


@pytest.mark.asyncio
async def test_template_rearms_wakeup_when_backlog_exceeds_drain_limit():
    ext = make_template_extractor(forward_relay, drain_limit=1)
    ext.wakeup = asyncio.Event()
    for i in range(2):
        ext.connection.publish(topic="t/aa/events", payload=json.dumps({"i": i}).encode())

    await collect_poll(ext)

    assert ext.wakeup.is_set()  # leftovers drain next cycle, not one interval later


@pytest.mark.asyncio
async def test_template_leaves_wakeup_unset_when_fully_drained():
    ext = make_template_extractor(forward_relay)
    ext.wakeup = asyncio.Event()
    ext.connection.publish(topic="t/aa/events", payload=b'{"x": 1}')

    await collect_poll(ext)

    assert not ext.wakeup.is_set()


@pytest.mark.asyncio
async def test_template_propagates_connection_errors():
    ext = make_template_extractor(forward_relay)
    assert await collect_poll(ext) == []  # creates the subscription

    ext.connection.error = ConnectionError("MQTT disconnected: 7")

    with pytest.raises(ConnectionError, match="MQTT disconnected: 7"):
        await collect_poll(ext)


@pytest.mark.asyncio
async def test_template_yields_nothing_when_no_messages():
    ext = make_template_extractor(forward_relay)
    assert await collect_poll(ext) == []


# -- MqttExtractor: the decorator form ---------------------------------------


def test_mqtt_extractor_decorator_builds_equivalent_stage():
    """@mqtt_extractor binds a relay to its config topics, yielding an MqttExtractor."""

    @mqtt_extractor(config_topics=["cfg"], drain_limit=7)
    def stage(config: Config, topic: str, payload: Record) -> Message | None:
        return Message(key=topic, topic="out", value=Event.wrap(payload.raw))

    assert isinstance(stage, MqttExtractor)
    assert stage.config_topics == ["cfg"]
    assert stage.drain_limit == 7


@pytest.mark.asyncio
async def test_mqtt_extractor_decorator_drives_relay_template():
    """The decorated relay rides the same template poll() as MqttExtractor.of."""

    @mqtt_extractor(config_topics=["cfg"])
    def stage(config: Config, topic: str, payload: Record) -> Message | None:
        return Message(key=topic, topic="out", value=Event.wrap(payload.raw))

    stage.connection = FakeMqttConnection()
    stage.connection.subscribe(CONFIG.raw["topic"])
    stage.observer = RecordingObserver()
    stage.connection.publish(topic="t/aa/events", payload=b'{"x": 1}')

    messages = [item async for item in stage.poll(CONFIG, State())]

    assert len(messages) == 1
    assert messages[0].key == "t/aa/events"
    assert messages[0].value.raw == {"x": 1}
