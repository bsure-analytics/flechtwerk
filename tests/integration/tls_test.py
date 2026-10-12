"""Integration tests for TLS — and mutual TLS — to the MQTT broker and the Kafka broker.

A throwaway PKI (CA, server and client certificates) is generated once per
session, and both brokers run listeners that REQUIRE a client certificate:
Mosquitto on 8883 with ``require_certificate true``, Kafka on an ``SSL``
listener with ``ssl.client.auth=required``. Pins what unit tests with
recorded kwargs cannot:

1. **The listeners really demand mTLS** — a context without the client
   certificate is rejected by each broker, so the passing tests below prove
   a certificate was presented, not merely that the channel was encrypted.
2. **MQTT backlog over mTLS** — a persistent session queues QoS 1 messages
   while offline and replays them through the asyncio socket integration
   promptly, far inside the keepalive a stalled TLS reader would wait for.
3. **End to end over both** — an ``MqttExtractor``, a stateful transformer
   and a stateful sharded extractor run in one process with ``kafka_tls``
   and ``MqttBrokerConfig(tls=...)``, exercising every Kafka client the
   container builds (consumers, transactional producers, admin, changelog).

Skipped automatically when Docker is not reachable.
"""
import asyncio
import json
import shlex
import ssl
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from ipaddress import IPv4Address
from pathlib import Path
from typing import AsyncIterator

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from paho.mqtt.client import CallbackAPIVersion, Client, MQTTv5

from flechtwerk.attribute import Attribute, INT, STR
from flechtwerk.extractor import Extractor
from flechtwerk.module import Flechtwerk
from flechtwerk.mqtt import MqttBrokerConfig, MqttConnection, MqttExtractor, TOPIC
from flechtwerk.transformer import Transformer
from flechtwerk.types import Event, Message, State

pytestmark = pytest.mark.integration

COUNT = Attribute("count", INT)
GROUP = "flechtwerk-tls"
MQTT_TLS_PORT = 8883
NAME = Attribute("name", STR)


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


# -- PKI ------------------------------------------------------------------------


def _pem_key(key: ec.EllipticCurvePrivateKey) -> str:
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    ).decode()


def _pem_cert(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _certificate(
        common_name: str,
        key: ec.EllipticCurvePrivateKey,
        *,
        issuer: x509.Certificate | None = None,
        issuer_key: ec.EllipticCurvePrivateKey | None = None,
        sans: list[x509.GeneralName] | None = None,
) -> x509.Certificate:
    """A certificate Python >= 3.13's VERIFY_X509_STRICT accepts.

    Strict verification rejects private-CA certificates that lack an
    AuthorityKeyIdentifier ("Missing Authority Key Identifier") or a KeyUsage
    ("CA cert does not include key usage extension"), so every certificate
    here carries both, beside BasicConstraints and a SubjectKeyIdentifier.
    ``issuer=None`` makes a self-signed CA.
    """
    is_ca = issuer is None
    signing_key = key if is_ca else issuer_key
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject if is_ca else issuer.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(signing_key.public_key()), critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=not is_ca,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=is_ca,
                crl_sign=is_ca,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
    )
    if sans:
        builder = builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
    return builder.sign(signing_key, hashes.SHA256())


@dataclass(frozen=True, kw_only=True)
class Pki:
    """A throwaway CA with one server and one client certificate, as PEM strings and files."""
    ca_file: Path
    ca_pem: str
    client_cert_file: Path
    client_key_file: Path
    server_cert_pem: str
    server_key_pem: str

    def client_context(self, *, with_certificate: bool) -> ssl.SSLContext:
        """What an application builds: trust the private CA, add the client certificate for mTLS."""
        context = ssl.create_default_context(cafile=self.ca_file)
        if with_certificate:
            context.load_cert_chain(self.client_cert_file, self.client_key_file)
        return context


@pytest.fixture(scope="session")
def pki(tmp_path_factory) -> Pki:
    directory = tmp_path_factory.mktemp("pki")
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = _certificate("flechtwerk test CA", ca_key)
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = _certificate(
        "localhost", server_key, issuer=ca, issuer_key=ca_key,
        sans=[x509.DNSName("localhost"), x509.IPAddress(IPv4Address("127.0.0.1"))],
    )
    client_key = ec.generate_private_key(ec.SECP256R1())
    client = _certificate("flechtwerk test client", client_key, issuer=ca, issuer_key=ca_key)
    files = {
        "ca.pem": _pem_cert(ca),
        "client.pem": _pem_cert(client),
        "client.key": _pem_key(client_key),
    }
    for name, pem in files.items():
        (directory / name).write_text(pem)
    return Pki(
        ca_file=directory / "ca.pem",
        ca_pem=files["ca.pem"],
        client_cert_file=directory / "client.pem",
        client_key_file=directory / "client.key",
        server_cert_pem=_pem_cert(server),
        server_key_pem=_pem_key(server_key),
    )


# -- Mosquitto with a TLS listener requiring client certificates ----------------

_MOSQUITTO_TLS_CONFIG = """\
listener 8883
cafile /tmp/tls/ca.pem
certfile /tmp/tls/server.pem
keyfile /tmp/tls/server.key
require_certificate true
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
def tls_mosquitto(pki: Pki):
    """A Mosquitto broker whose only listener is mTLS on 8883.

    Every file is written inside the container by the start command, as in
    ``mqtt_delivery_test.mosquitto_broker`` (bind mounts break on
    Docker-outside-of-Docker CI). Mosquitto drops root privileges before
    reading the server key, so the key is handed to the ``mosquitto`` user.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.mqtt import MosquittoContainer

    files = {
        "/tmp/mosquitto.conf": _MOSQUITTO_TLS_CONFIG,
        "/tmp/tls/ca.pem": pki.ca_pem,
        "/tmp/tls/server.key": pki.server_key_pem,
        "/tmp/tls/server.pem": pki.server_cert_pem,
    }
    writes = " && ".join(f"printf %s {shlex.quote(content)} > {path}" for path, content in files.items())
    container = MosquittoContainer()
    container.with_exposed_ports(MQTT_TLS_PORT)
    container.with_command([
        "sh", "-c",
        f"mkdir -p /tmp/tls && {writes}"
        " && chown mosquitto /tmp/tls/server.key && chmod 600 /tmp/tls/server.key"
        " && exec mosquitto -c /tmp/mosquitto.conf",
    ])
    try:
        # MosquittoContainer.start() always bind-mounts a config file;
        # call DockerContainer.start() directly.
        super(MosquittoContainer, container).start()
        container._wait()
        yield container
    finally:
        container.stop()


def _mqtt_broker(tls_mosquitto, context: ssl.SSLContext) -> MqttBrokerConfig:
    return MqttBrokerConfig(
        broker=tls_mosquitto.get_container_host_ip(),
        port=int(tls_mosquitto.get_exposed_port(MQTT_TLS_PORT)),
        tls=context,
    )


class _Publisher:
    """A plain paho MQTTv5 client over mTLS, publishing QoS 1 from a background thread.

    ``loop_start`` runs paho's own network thread so ``wait_for_publish`` can
    complete; call ``publish`` through ``asyncio.to_thread`` from a test.
    """

    def __init__(self, broker: MqttBrokerConfig) -> None:
        self.client = Client(CallbackAPIVersion.VERSION2, client_id=f"publisher-{uuid.uuid4().hex[:8]}", protocol=MQTTv5)
        self.client.tls_set_context(broker.tls)
        self.client.connect(broker.broker, broker.port)
        self.client.loop_start()
        deadline = time.monotonic() + 10
        while not self.client.is_connected():
            if time.monotonic() > deadline:
                raise RuntimeError("publisher did not connect over mTLS")
            time.sleep(0.05)

    def publish(self, topic: str, payloads: list[str]) -> None:
        for payload in payloads:
            info = self.client.publish(topic, payload, qos=1)
            info.wait_for_publish(timeout=5)
            if not info.is_published():
                raise RuntimeError(f"Publish to {topic} did not complete: {info}")

    def close(self) -> None:
        self.client.disconnect()
        self.client.loop_stop()


# -- Kafka with an SSL listener requiring client certificates ------------------


@pytest.fixture(scope="session")
def tls_kafka_bootstrap(pki: Pki) -> Iterator[str]:
    """A Kafka broker whose client listener is mTLS (``ssl.client.auth=required``).

    PEM key and trust stores are written with ``create_file`` (a
    ``put_archive``, as testcontainers writes its own start script) before the
    start script runs. The listener is named ``TLS``, not ``SSL``: the
    Confluent image's configure script demands JKS keystore settings whenever
    the advertised listeners contain ``SSL://``.
    """
    if not _docker_available():
        pytest.skip("Docker not available — skipping integration tests")

    from testcontainers.kafka import KafkaContainer

    class _TlsKafkaContainer(KafkaContainer):
        def tc_start(self) -> None:
            self.create_file((pki.server_key_pem + pki.server_cert_pem).encode(), "/etc/kafka/tls/keystore.pem")
            self.create_file(pki.ca_pem.encode(), "/etc/kafka/tls/truststore.pem")
            super().tc_start()

    kafka = (
        _TlsKafkaContainer(listener_name="TLS", security_protocol="SSL")
        .with_env("KAFKA_SSL_CLIENT_AUTH", "required")
        .with_env("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/tls/keystore.pem")
        .with_env("KAFKA_SSL_KEYSTORE_TYPE", "PEM")
        .with_env("KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/tls/truststore.pem")
        .with_env("KAFKA_SSL_TRUSTSTORE_TYPE", "PEM")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_MIN_INSYNC_REPLICAS", "1")
    )
    with kafka:
        yield kafka.get_bootstrap_server()


def _tls(context: ssl.SSLContext) -> dict:
    return {"security_protocol": "SSL", "ssl_context": context}


async def _create_topics(
        bootstrap: str, context: ssl.SSLContext, partitions: dict[str, int], compacted: tuple[str, ...] = (),
) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, **_tls(context))
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=n,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"} if topic in compacted else {},
            )
            for topic, n in partitions.items()
        ])
    finally:
        await admin.close()


async def _produce(bootstrap: str, context: ssl.SSLContext, records: list[tuple[str, bytes, bytes]]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, **_tls(context))
    await producer.start()
    try:
        for topic, key, value in records:
            await producer.send(topic, key=key, value=value)
        await producer.flush()
    finally:
        await producer.stop()


async def _read_all(bootstrap: str, context: ssl.SSLContext, topic: str, timeout_ms: int = 1000) -> list:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=None,
        isolation_level="read_committed",
        **_tls(context),
    )
    await consumer.start()
    try:
        records = []
        while True:
            batch = await consumer.getmany(timeout_ms=timeout_ms)
            if not batch:
                return records
            for msgs in batch.values():
                records.extend(msgs)
    finally:
        await consumer.stop()


# -- 1-2: the listeners really require a client certificate --------------------


async def test_mqtt_listener_rejects_a_client_without_a_certificate(tls_mosquitto, pki: Pki) -> None:
    """Under TLS 1.3 the server rejects a missing client certificate only
    after the client's side of the handshake completed, so ``__aenter__``
    normally returns and the rejection surfaces on the first read, as the
    ``ConnectionError`` ``drain`` raises. A rejection already inside the
    handshake is a timing race and proves the same thing."""
    broker = _mqtt_broker(tls_mosquitto, pki.client_context(with_certificate=False))
    loop = asyncio.get_running_loop()
    conn = MqttConnection(broker=broker, client_id="flechtwerk-tls-nocert", group=GROUP, loop=loop)
    try:
        await conn.__aenter__()
    except OSError:  # ssl.SSLError included: rejected inside the handshake
        return
    try:
        sub = conn.subscribe("flechtwerk/tls-nocert/#")
        deadline = loop.time() + 5
        while loop.time() < deadline:
            try:
                sub.drain(limit=10)
            except ConnectionError:
                return
            await asyncio.sleep(0.05)
        pytest.fail("a client without a certificate stayed connected to the mTLS listener")
    finally:
        await conn.__aexit__(None, None, None)


async def test_kafka_listener_rejects_a_client_without_a_certificate(tls_kafka_bootstrap: str, pki: Pki) -> None:
    admin = AIOKafkaAdminClient(
        bootstrap_servers=tls_kafka_bootstrap,
        request_timeout_ms=5000,
        **_tls(pki.client_context(with_certificate=False)),
    )
    try:
        with pytest.raises(KafkaConnectionError):
            await asyncio.wait_for(admin.start(), timeout=30)
    finally:
        await admin.close()


# -- 3: MQTT backlog over mTLS ---------------------------------------------------


async def test_mqtt_backlog_replays_promptly_over_mtls(tls_mosquitto, pki: Pki) -> None:
    """A persistent session's QoS 1 backlog drains over mTLS in a few seconds.

    The bound is far below the 60 s keepalive: were decrypted bytes left
    stranded in OpenSSL's buffer with no fd readiness to wake the reader, the
    drain would stall until the next keepalive and blow it."""
    broker = _mqtt_broker(tls_mosquitto, pki.client_context(with_certificate=True))
    topic_filter = "flechtwerk/tls-backlog/+/events"
    device_topic = "flechtwerk/tls-backlog/aabb/events"
    client_id = "flechtwerk-tls-backlog"
    payloads = [json.dumps({"n": i}) for i in range(300)]
    loop = asyncio.get_running_loop()

    conn1 = MqttConnection(broker=broker, client_id=client_id, group=GROUP, loop=loop)
    async with conn1:
        conn1.subscribe(topic_filter)
        await asyncio.sleep(1.0)  # wait for CONNACK + SUBACK
    await asyncio.sleep(1.0)  # let the DISCONNECT flush before the session is resumed

    publisher = await asyncio.to_thread(_Publisher, broker)
    try:
        await asyncio.to_thread(publisher.publish, device_topic, payloads)
    finally:
        await asyncio.to_thread(publisher.close)

    received: list[bytes] = []
    conn2 = MqttConnection(broker=broker, client_id=client_id, group=GROUP, loop=loop)
    async with conn2:
        sub = conn2.subscribe(topic_filter)
        started = loop.time()
        while len(received) < len(payloads) and loop.time() - started < 10:
            for msg in sub.drain(limit=100):
                received.append(msg.payload)
                assert sub.ack(msg)  # free the inflight window as we go
            await asyncio.sleep(0.01)
        elapsed = loop.time() - started

    assert sorted(received) == sorted(p.encode() for p in payloads)
    assert elapsed < 10


# -- 4: end to end over both brokers --------------------------------------------


class _Done(Exception):
    """Raised by the watcher to end the TaskGroup once the pipeline has flowed."""


async def test_three_stages_run_end_to_end_over_mtls(tls_kafka_bootstrap: str, tls_mosquitto, pki: Pki) -> None:
    """MQTT relay → stateful transformer, beside a stateful sharded extractor —
    every Kafka client the container builds, and the MQTT connection, over mTLS."""
    run_id = uuid.uuid4().hex[:12]
    context = pki.client_context(with_certificate=True)
    bootstrap = tls_kafka_bootstrap
    mqtt_cfg_topic = f"mqtt-cfg-{run_id}"
    raw_topic = f"raw-{run_id}"
    out_topic = f"out-{run_id}"
    ext_cfg_topic = f"ext-cfg-{run_id}"
    ext_out_topic = f"ext-out-{run_id}"
    mqtt_filter = f"flechtwerk/tls-e2e-{run_id}/+/events"
    device_topic = f"flechtwerk/tls-e2e-{run_id}/aabb/events"

    await _create_topics(
        bootstrap,
        context,
        {mqtt_cfg_topic: 1, raw_topic: 1, out_topic: 1, ext_cfg_topic: 1, ext_out_topic: 1},
        compacted=(mqtt_cfg_topic, ext_cfg_topic),
    )
    await _produce(bootstrap, context, [
        (mqtt_cfg_topic, b"m1", json.dumps({TOPIC.name: mqtt_filter}).encode()),
        (ext_cfg_topic, b"c1", b'{"name":"c1"}'),
    ])

    def relay(config, topic, payload) -> Message | None:
        return Message(key=topic, topic=raw_topic, value=Event.wrap({"n": payload.raw["n"]}))

    async def refine(msg, state) -> AsyncIterator[Message | State]:
        count = state.get(COUNT, 0) + 1
        yield Message(key=msg.key, topic=out_topic, value=Event.wrap({"count": count, "n": msg.value.raw["n"]}))
        yield State({COUNT: count})

    async def poll(config, state) -> AsyncIterator[Message | State]:
        n = state.get(COUNT, 0) + 1
        yield Message(key=config[NAME], topic=ext_out_topic, value=Event.wrap({"n": n}))
        yield State({COUNT: n})

    mqtt_broker = _mqtt_broker(tls_mosquitto, context)

    def handle(stage: str, target: Extractor | Transformer) -> Flechtwerk:
        return Flechtwerk.of(
            application_id=f"{stage}-{run_id}",
            bootstrap_servers=bootstrap,
            client_id=f"{run_id}-{stage}",
            kafka_tls=context,
            mqtt=mqtt_broker,
            poll_interval=timedelta(seconds=1),
            stage=target,
        )

    relay_stage = handle("relay", MqttExtractor.of(config_topics=[mqtt_cfg_topic], relay=relay))
    refine_stage = handle("refine", Transformer.of(input_topics=[raw_topic], transform=refine))
    extract_stage = handle("extract", Extractor.of(config_topics=[ext_cfg_topic], poll=poll))

    publisher = await asyncio.to_thread(_Publisher, mqtt_broker)

    async def watch() -> None:
        # The relay subscribes only on its first poll, after the config
        # bootstrap, and its fresh client_id has no session yet — a QoS 1
        # publish before that SUBSCRIBE is discarded. So publish a fresh
        # message on EVERY iteration: early ones are lost, a later one flows.
        deadline = asyncio.get_running_loop().time() + 90
        n = 0
        while True:
            n += 1
            await asyncio.to_thread(publisher.publish, device_topic, [json.dumps({"n": n})])
            refined = await _read_all(bootstrap, context, out_topic)
            extracted = await _read_all(bootstrap, context, ext_out_topic)
            if refined and extracted:
                raise _Done
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail(f"pipeline did not flow over mTLS (refined={len(refined)}, extracted={len(extracted)})")

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(relay_stage.run())
            tg.create_task(refine_stage.run())
            tg.create_task(extract_stage.run())
            tg.create_task(watch())
    except* _Done:
        pass
    finally:
        await asyncio.to_thread(publisher.close)

    refined = await _read_all(bootstrap, context, out_topic)
    assert refined and refined[0].key == device_topic.encode()
    assert json.loads(refined[0].value)["count"] == 1
    extracted = await _read_all(bootstrap, context, ext_out_topic)
    assert extracted and extracted[0].key == b"c1"
