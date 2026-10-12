"""Field-level encryption for attributes — the ``flechtwerk[secrets]`` extra.

The token format itself — seal, open, sign, verify, the scope ratchet, the key
bindings, the pinned registries — lives in the standalone ``flechtctl`` package
(joserfc confined to ``flechtctl.tokens``), which ``flechtwerk[secrets]``
installs. This module adds what needs the framework: the ``ENCRYPTED`` and
``SIGNED`` codecs, the process keyring, the secret telemetry, and the Kafka
config-topic scan. It is the only framework module that reaches ``flechtctl``'s
token code, so an application that never declares an ``ENCRYPTED`` attribute
never loads joserfc; ``module.py`` reaches the keyring through the joserfc-free
``flechtwerk.keyring`` seam, so importing the framework does not require the
extra.

``ENCRYPTED(inner)`` turns a field into an RFC 7516 compact JWE token whose
JSON-string wire form is ``flenc:jwe:<compact JWE>``:

    from flechtwerk.attribute import Attribute, STR
    from flechtwerk.secrets import ENCRYPTED

    API_KEY = Attribute("api_key", ENCRYPTED(STR))

The token is sealed under an injected keyring in one of two generations:
symmetric ``dir`` + ``A256GCM`` under an oct key, or ``ECDH-ES+A256KW`` +
``A256GCM`` to an X25519 recipient key (whose public half seals and whose
private half opens). Readers pin a strict ``(alg, enc)`` allowlist per
generation, and the **kind of the key behind the token's ``kid``** selects
which one applies — a token never chooses. A key bound to scopes in the
keyring serves only those scopes, on write and on read. Two optional per-attribute
knobs: ``scope`` binds a token into a domain-separation compartment
(``flenc_scope`` in the integrity-protected header) so it can't be relocated
into a differently-scoped field — a one-way ratchet (adding a scope is
non-breaking, dropping it is blocked); ``read_plaintext`` tolerates a legacy
plaintext value on read during a migration.

``SIGNED(inner, scope=...)`` turns a field into an RFC 7515 compact JWS signed
with an Ed25519 key from the keyring, wire form ``flsig:jws:<compact JWS>`` —
it vouches for *who wrote* a value, which a recipient token cannot (anyone
holding the public recipient key can seal). The recommended composition is
sign-then-encrypt, ``ENCRYPTED(SIGNED(inner, scope=s), scope=s)``. See
``docs/concepts/secrets.md`` for the full spec (format longevity, rotation,
migration, post-quantum posture).
"""
import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer

from flechtwerk.attribute import Attribute
from flechtwerk.attribute.codec import Codec

from .kafka import decode_key, decode_record, is_tombstone, read_to_end, topic_partitions
from .keyring import (
    Ed25519Key,
    Keyring,
    X25519Key,
    active_keyring,
    active_observer,
    install_keyring,
)
from .types import Config

try:
    from flechtctl.errors import (
        PlaintextSecretError,
        SecretDecryptError,
        SecretError,
        SecretFormatError,
        SecretSignatureError,
        UnsignedSecretError,
    )
    from flechtctl.tokens import (
        PREFIX,
        SCHEME,
        SIGNATURE_PREFIX,
        SIGNATURE_SCHEME,
        canonical_json,
        decrypt_bytes,
        encrypt_bytes,
        is_encrypted,
        is_signed,
        kid_of,
        sign_bytes,
        signer_of,
        verify_bytes,
    )
except ModuleNotFoundError as e:
    if e.name is None or e.name.partition(".")[0] not in ("flechtctl", "joserfc"):
        raise
    raise ModuleNotFoundError(
        "flechtwerk.secrets needs the flechtctl package — install flechtwerk[secrets]", name=e.name,
    ) from e

log = logging.getLogger(__name__)

__all__ = [
    "ENCRYPTED",
    "Ed25519Key",
    "Keyring",
    "PREFIX",
    "PlaintextSecretError",
    "SCHEME",
    "SIGNATURE_PREFIX",
    "SIGNATURE_SCHEME",
    "SIGNED",
    "ScanEntry",
    "SecretDecryptError",
    "SecretError",
    "SecretFormatError",
    "SecretSignatureError",
    "UnsignedSecretError",
    "X25519Key",
    "encrypt_value",
    "install_keyring",
    "is_encrypted",
    "is_signed",
    "kid_of",
    "reencrypt",
    "scan_config_topics",
    "sign_value",
    "signer_of",
]


# --- the process keyring and telemetry around flechtctl.tokens ---


def _encrypt_payload(scope: str, payload: bytes) -> str:
    """Encrypt raw payload bytes under the process keyring's sealing key for ``scope``."""
    return encrypt_bytes(active_keyring(), payload, scope=scope)


def _decrypt_to_payload(scope: str, token: str, *, topic: str | None = None,
                        wire_key: str | None = None) -> bytes:
    """Decrypt a ``flenc`` token under the process keyring, enforcing the scope ratchet.

    The rules live in `flechtctl.tokens.decrypt_bytes`; this adds the keyring
    lookup and the ``secret_decrypted`` event.
    """
    # Resolve the keyring first: a missing keyring is a deployment error that
    # should win over any complaint about the token's shape.
    keyring = active_keyring()
    payload = decrypt_bytes(keyring, token, scope=scope, topic=topic, wire_key=wire_key)
    active_observer().secret_decrypted(scope, kid_of(token))
    return payload


def _verify_to_payload(scope: str, token: str) -> bytes:
    """Verify a ``flsig`` token under the process keyring, enforcing scope and binding.

    The rules live in `flechtctl.tokens.verify_bytes`; this adds the keyring
    lookup and the ``secret_verified`` event.
    """
    keyring = active_keyring()
    payload = verify_bytes(keyring, token, scope=scope)
    active_observer().secret_verified(scope, signer_of(token))
    return payload


# --- the codecs ---


@dataclass(frozen=True, slots=True)
class _EncryptedCodec(Codec):
    """The plain `Codec` returned by `ENCRYPTED(...)`.

    A marker the tooling recognizes via `isinstance` — it carries the `scope`
    so `reencrypt` re-stamps the same one. `read_plaintext` needs no field:
    `decode` closes over it."""

    scope: str = ""


def ENCRYPTED[V](inner: Codec[V], *, scope: str = "", read_plaintext: bool = False) -> Codec[V]:
    """Wrap ``inner`` so the attribute's value is stored as an encrypted token.

    A plain composable codec: ``ENCRYPTED(LIST(STR))``, ``LIST(ENCRYPTED(STR))``,
    and ``ENCRYPTED(RECORD)`` are all valid.

    ``scope`` (default none) binds the token into a domain-separation
    compartment — a token stamped with one scope is rejected by a codec
    declaring a different one. It is a **one-way ratchet**: a scoped codec still
    accepts an unscoped token (so a scope can be added to an already-encrypted
    field without a flag-day, then swept in with ``reencrypt``), but an unscoped
    codec rejects a scoped token (a scope cannot be silently dropped).

    ``read_plaintext`` (default off) tolerates a legacy plaintext value on read
    for this attribute during a migration, logging a WARNING and emitting
    ``secret_plaintext_read``; off, a plaintext value raises
    ``PlaintextSecretError``.
    """
    def encode(value: V) -> str:
        return _encrypt_payload(scope, canonical_json(inner.encode(value)))

    def decode(value: object) -> object:
        if is_encrypted(value):
            return inner.decode(json.loads(_decrypt_to_payload(scope, value)))  # type: ignore[arg-type]
        if read_plaintext:
            log.warning("Secret value read as legacy plaintext (scope=%r) — migrate this record to ciphertext", scope)
            active_observer().secret_plaintext_read(scope)
            return inner.decode(value)
        raise PlaintextSecretError(scope=scope)

    return _EncryptedCodec(decode=decode, encode=encode, scope=scope)


@dataclass(frozen=True, slots=True)
class _SignedCodec(Codec):
    """The plain `Codec` returned by `SIGNED(...)` — a marker carrying its `scope`."""

    scope: str = ""


def SIGNED[V](inner: Codec[V], *, scope: str, read_unsigned: bool = False) -> Codec[V]:
    """Wrap ``inner`` so the attribute's value is stored as an Ed25519-signed token.

    The token is ``flsig:jws:<compact JWS>``, signed by the keyring's signing
    key for ``scope`` (``signing[scope]``). ``scope`` is required: a signature
    always names the compartment it vouches for, and a reader accepts it only
    for exactly that scope, from any signing key in its keyring bound to it.

    Composes like any codec; the recommended composition is sign-then-encrypt,
    ``ENCRYPTED(SIGNED(inner, scope=s), scope=s)`` — the signer's kid stays
    inside the ciphertext, and the signature's scope keeps a decrypting reader
    from re-sealing the value into another field.

    ``read_unsigned`` (default off) tolerates an unsigned value on read during
    a migration, logging a WARNING and emitting ``secret_unsigned_read``; off,
    an unsigned value raises ``UnsignedSecretError``.
    """
    if not scope:
        raise ValueError("SIGNED needs a non-empty scope — a signature always names the compartment it vouches for")

    def encode(value: V) -> str:
        return sign_bytes(active_keyring(), canonical_json(inner.encode(value)), scope=scope)

    def decode(value: object) -> object:
        if is_signed(value):
            return inner.decode(json.loads(_verify_to_payload(scope, value)))  # type: ignore[arg-type]
        if read_unsigned:
            log.warning("Signed value read unsigned (scope=%r) — migrate this record to a signed one", scope)
            active_observer().secret_unsigned_read(scope)
            return inner.decode(value)
        raise UnsignedSecretError(scope=scope)

    return _SignedCodec(decode=decode, encode=encode, scope=scope)


# --- ops tooling primitives ---


def _require_encrypted(attribute: Attribute) -> _EncryptedCodec:
    codec = attribute.codec
    if not isinstance(codec, _EncryptedCodec):
        raise SecretError(f"{attribute!r} is not an encrypted attribute (its codec is not ENCRYPTED)")
    return codec


def encrypt_value[V](attribute: Attribute[V], value: V) -> str:
    """Encrypt ``value`` for ``attribute`` and return the ``flenc:jwe:`` token.

    The token a typed write (``config[attribute] = value``) would store, for
    writers that build their JSON without a ``Record`` — a form backend, say.
    Raises ``SecretError`` if ``attribute`` is not an encrypted attribute.
    """
    return _require_encrypted(attribute).encode(value)


def sign_value[V](attribute: Attribute[V], value: V) -> str:
    """Sign ``value`` for ``attribute`` and return the ``flsig:jws:`` token.

    The counterpart of `encrypt_value` for a ``SIGNED`` attribute — for an
    ``ENCRYPTED(SIGNED(...))`` attribute use `encrypt_value`. Raises ``SecretError`` if ``attribute`` is
    not a signed attribute.
    """
    codec = attribute.codec
    if not isinstance(codec, _SignedCodec):
        raise SecretError(f"{attribute!r} is not a signed attribute (its codec is not SIGNED)")
    return codec.encode(value)


def reencrypt[V](token: str, attribute: Attribute[V]) -> str:
    """Re-encrypt ``token`` under the scope's current sealing key (decrypt-with-named-kid).

    The building block for a rotation sweep and for cross-environment
    promotion: it decrypts with the key the token names, then re-encrypts the
    same payload under the keyring's sealing key for the attribute's scope
    (``primaries[scope]``, else ``primary``) — which moves a token from ``dir``
    to a recipient key, from one recipient key to another, and back. Opening a
    recipient token needs that recipient's private half. The exact inner bytes
    are preserved, never round-tripped through the decoded value. Also promotes an unscoped token to
    the attribute's ``scope`` (the ratchet's upgrade path); it cannot strip a
    scope (an unscoped attribute's decrypt rejects a scoped token). Raises if
    ``attribute`` is not encrypted, or ``SecretDecryptError`` if the token
    cannot be read.
    """
    scope = _require_encrypted(attribute).scope
    return _encrypt_payload(scope, _decrypt_to_payload(scope, token))


@dataclass(frozen=True, slots=True)
class ScanEntry:
    """One secret-bearing value found on a config topic by ``scan_config_topics``.

    ``kid`` is the key a ``flenc`` token depends on; ``None`` means the value
    is either legacy plaintext — an unencrypted secret still on the topic, the
    thing the migration scan is looking for — or (when ``error`` is set) a
    ``flenc`` value the scan could not read the ``kid`` from. ``error`` is the
    reason a ciphertext-form value could not be classified (a malformed header
    or envelope); ``None`` for a cleanly-read ciphertext or a plaintext value.
    A non-``None`` ``error`` is neither a clean ciphertext nor plaintext — it
    keeps a corrupt record from silently reading as either, and from aborting
    the whole scan.

    ``attribute`` is empty for a **record-level** finding: the record's key or
    value could not be decoded at all, so no attribute could be inspected.
    Such an entry always carries an ``error``, and its ``wire_key`` falls back
    to ``repr(raw_key)`` when the key itself is what failed.
    """

    attribute: str
    error: str | None
    kid: str | None
    partition: int
    topic: str
    wire_key: str


async def scan_config_topics(
    consumer: AIOKafkaConsumer,
    topics: Iterable[str],
    attributes: Iterable[Attribute],
) -> AsyncIterator[ScanEntry]:
    """Yield a ``ScanEntry`` for each secret-bearing value across ``topics``.

    The engine behind the migration and rotation topic scans: for every
    surviving record it reports which secret attributes carry ciphertext (with
    the ``kid``), which still carry plaintext (``kid=None``, ``error=None``),
    and which carry a ``flenc`` value whose ``kid`` could not be read
    (``error`` set). Reads each topic to its end group-less on the given
    consumer, exactly as the config bootstrap does. Config topics are small by
    contract, so results are gathered then yielded.

    Since the scan is the authoritative gate before destructive steps (removing
    a key, ending plaintext tolerance), it must produce a COMPLETE report: a
    topic that does not exist raises `SecretError` rather than silently
    contributing zero records — a missing topic must not read as a clean "no
    plaintext / no old-kid" result — and a single corrupt token is reported as
    a `ScanEntry` with ``error`` set rather than aborting the whole scan. That
    extends to record-level decode: an undecodable key or value becomes a
    `ScanEntry` with an empty ``attribute``. The scan has no stage, so it never
    consults `Stage.on_invalid_message` — a diagnostic tool must report, not
    adopt a runtime policy that could crash it.
    """
    names = [a.name for a in attributes]
    topics = list(topics)
    found: list[ScanEntry] = []

    def report_record(msg, wire_key: str, error: ValueError) -> None:
        log.warning("Undecodable config record at %s/%d (key %s): %s",
                    msg.topic, msg.offset, wire_key, error)
        found.append(ScanEntry(
            attribute="",
            error=str(error),
            kid=None,
            partition=msg.partition,
            topic=msg.topic,
            wire_key=wire_key,
        ))

    async def collect(msg) -> None:
        if is_tombstone(msg.value):
            return
        try:
            wire_key = decode_key(msg.key)
        except ValueError as e:
            # An undecodable key has no readable identity, so the report falls
            # back to the raw bytes' repr — lossy-decoding it would invent an
            # identity, which is exactly what the runtime path refuses to do.
            report_record(msg, repr(msg.key), e)
            return
        try:
            raw = decode_record(msg.value, Config).raw
        except ValueError as e:
            report_record(msg, wire_key, e)
            return
        for name in names:
            value = raw.get(name)
            if value is None:
                # Absent, or present-but-null — no secret value either way, so
                # not a plaintext leak (`kid=None` is reserved for a value that
                # IS present and unencrypted).
                continue
            kid: str | None = None
            error: str | None = None
            if is_encrypted(value):
                try:
                    kid = kid_of(value)
                except SecretFormatError as e:
                    # A corrupt ciphertext token: report it, don't abort the
                    # scan. The scan is the gate before destructive steps, so a
                    # complete report across every record matters more than
                    # failing fast on the first bad one.
                    log.warning("Unreadable %s token at %s/%d key %r: %s",
                                SCHEME, msg.topic, msg.offset, wire_key, e)
                    error = str(e)
            found.append(ScanEntry(
                attribute=name,
                error=error,
                kid=kid,
                partition=msg.partition,
                topic=msg.topic,
                wire_key=wire_key,
            ))

    tps = await topic_partitions(consumer, topics)
    present = {tp.topic for tp in tps}
    if missing := [t for t in topics if t not in present]:
        raise SecretError(f"cannot scan — config topic(s) not found: {sorted(missing)}")
    await read_to_end(consumer, tps, collect)
    for entry in found:
        yield entry
