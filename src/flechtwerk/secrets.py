"""Field-level encryption for attributes — the ``flechtwerk[secrets]`` extra.

The **only** framework module that imports ``joserfc`` (the confinement
discipline paho follows in ``flechtwerk.mqtt``): an application that never
declares an ``ENCRYPTED`` attribute never loads it. ``module.py`` reaches the
keyring through the joserfc-free ``flechtwerk.keyring`` seam, so importing the
framework does not require the extra.

``ENCRYPTED(inner)`` turns a field into an RFC 7516 compact JWE token whose
JSON-string wire form is ``flenc:jwe:<compact JWE>``:

    from flechtwerk.attribute import Attribute, STR
    from flechtwerk.secrets import ENCRYPTED

    API_KEY = Attribute("api_key", ENCRYPTED(STR))

The token is symmetric ``dir`` + ``A256GCM`` under an injected keyring, and
readers pin a strict ``(alg, enc)`` allowlist. Two optional per-attribute
knobs: ``scope`` binds a token into a domain-separation compartment
(``flenc_scope`` in the integrity-protected header) so it can't be relocated
into a differently-scoped field — a one-way ratchet (adding a scope is
non-breaking, dropping it is blocked); ``read_plaintext`` tolerates a legacy
plaintext value on read during a migration. See ``docs/concepts/secrets.md``
for the full spec (format longevity, rotation, migration, post-quantum posture).
"""
import base64
import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer
from joserfc import jwe
from joserfc.jwk import OctKey
from joserfc.registry import HeaderParameter

from flechtwerk.attribute import Attribute
from flechtwerk.attribute.codec import Codec

from .kafka import decode_event, decode_key, is_tombstone, read_to_end, topic_partitions
from .keyring import (
    Keyring,
    UnknownKeyError,
    active_keyring,
    active_observer,
    install_keyring,
)

log = logging.getLogger(__name__)

__all__ = [
    "ENCRYPTED",
    "Keyring",
    "PREFIX",
    "PlaintextSecretError",
    "SCHEME",
    "ScanEntry",
    "SecretDecryptError",
    "SecretError",
    "SecretFormatError",
    "encrypt_value",
    "install_keyring",
    "is_encrypted",
    "kid_of",
    "reencrypt",
    "scan_config_topics",
]

SCHEME = "flenc"
"""The URI scheme naming a Flechtwerk-encrypted value (and the backend dispatch point)."""

_ENVELOPE = "jwe"  # the encoding segment: RFC 7516 *compact* serialization, this design's profile
PREFIX = f"{SCHEME}:{_ENVELOPE}:"
"""The full token prefix, ``flenc:jwe:`` — a valid URI scheme + envelope segment."""

_SCHEME_PREFIX = f"{SCHEME}:"  # any value under this scheme is ciphertext-form
_ALG = "dir"
_ENC = "A256GCM"
_FLENC_SCOPE = "flenc_scope"  # optional protected-header param — the domain-separation scope

# The v1 acceptance rule, pinned at the single call site: exactly dir+A256GCM;
# flenc_scope is an OPTIONAL header (a token may carry a scope or not). Widening
# this allowlist (a future hybrid post-quantum mode) is a deliberate reader-side
# change, never token content.
_REGISTRY = jwe.JWERegistry(
    header_registry={_FLENC_SCOPE: HeaderParameter("Flechtwerk domain-separation scope", "str", False)},
    algorithms=[_ALG, _ENC],
    strict_check_header=True,
)


# --- exceptions ---


class SecretError(Exception):
    """Base class for secret encode/decode failures."""


class SecretFormatError(SecretError):
    """A value is not a well-formed ``flenc`` token (unknown envelope, bad header)."""


class SecretDecryptError(SecretError):
    """A ``flenc`` token could not be decrypted or failed scope verification.

    Carries what the codec knows — the codec's ``scope`` (may be empty) and the
    token's ``kid`` — plus ``topic`` / ``wire_key`` when a caller with that
    context (the scan helper) raises. The hook an application uses to quarantine
    a single config under the "only catch what you can remedy" rule. An unknown
    ``kid`` and a GCM tag failure are distinct incident signatures (missing key
    vs. wrong bytes for a known key); the ``__cause__`` preserves which one.
    """

    def __init__(self, *, scope: str = "", kid: str | None = None,
                 topic: str | None = None, wire_key: str | None = None,
                 reason: str | None = None) -> None:
        self.scope = scope
        self.kid = kid
        self.topic = topic
        self.wire_key = wire_key
        detail = ["cannot decrypt secret value"]
        if scope:
            detail.append(f"scope={scope!r}")
        if kid is not None:
            detail.append(f"kid={kid!r}")
        if wire_key is not None:
            detail.append(f"key={wire_key!r}")
        if topic is not None:
            detail.append(f"topic={topic!r}")
        if reason is not None:
            detail.append(reason)
        super().__init__("; ".join(detail))


class PlaintextSecretError(SecretError):
    """A secret value is plaintext but the codec's ``read_plaintext`` is off.

    The read-time paste-guard firing: the record must be re-produced encrypted
    (and the pasted value treated as compromised). Flipping ``read_plaintext``
    back on to silence it is the anti-pattern — it re-opens the hole for that
    field.
    """

    def __init__(self, *, scope: str = "", topic: str | None = None, wire_key: str | None = None) -> None:
        self.scope = scope
        self.topic = topic
        self.wire_key = wire_key
        detail = ["secret value is plaintext but read_plaintext is off for this attribute"]
        if scope:
            detail.append(f"scope={scope!r}")
        if wire_key is not None:
            detail.append(f"key={wire_key!r}")
        if topic is not None:
            detail.append(f"topic={topic!r}")
        super().__init__("; ".join(detail))


# --- classification & crypto helpers ---


def is_encrypted(value: object) -> bool:
    """True if ``value`` is ciphertext-form — a string under the ``flenc:`` scheme.

    Classification is by scheme, not by exact token: a plaintext config value
    that merely looks like a URL under a *different* scheme is not
    ciphertext-form. The envelope segment is validated later, at decode.
    """
    return isinstance(value, str) and value.startswith(_SCHEME_PREFIX)


def _compact(token: str) -> str:
    """Strip ``flenc:jwe:`` and return the compact JWE, or raise on a bad envelope.

    An unknown envelope segment (a future ``flenc:`` form this reader does not
    understand) raises in every keyring mode — it is never handed to an inner
    codec as legacy plaintext.
    """
    body = token[len(_SCHEME_PREFIX):]
    envelope, sep, compact = body.partition(":")
    if not sep or envelope != _ENVELOPE or not compact:
        raise SecretFormatError(f"unknown {SCHEME} envelope {envelope!r} — expected {_ENVELOPE!r}")
    return compact


def _read_kid(compact: str) -> str:
    """Read the ``kid`` from a compact JWE header without the key (base64url decode).

    Used only to *select* which key to hand the decryptor; the header is
    re-parsed and authenticated as the AEAD's associated data during the
    actual decrypt, so a tampered kid fails the tag check regardless.
    """
    header_segment = compact.split(".", 1)[0]
    try:
        padded = header_segment + "=" * (-len(header_segment) % 4)
        header = json.loads(base64.urlsafe_b64decode(padded))
    except Exception as e:
        raise SecretFormatError(f"malformed JWE header: {e}") from e
    kid = header.get("kid")
    if not isinstance(kid, str) or not kid:
        raise SecretFormatError(f"JWE header carries no usable kid: {kid!r}")
    return kid


def kid_of(token: str) -> str:
    """Return the ``kid`` a ``flenc`` token was encrypted under (no key needed).

    A scan/rotation building block: reports which key a surviving record still
    depends on. Raises ``SecretFormatError`` for a non-``flenc`` value.
    """
    if not is_encrypted(token):
        raise SecretFormatError("not a flenc token")
    return _read_kid(_compact(token))


def _encrypt_payload(scope: str, payload: bytes) -> str:
    """Encrypt raw payload bytes under the primary key, stamping ``scope`` if set."""
    kid, key_bytes = active_keyring().primary_pair()
    key = OctKey.import_key(key_bytes, {"kid": kid})
    protected = {"alg": _ALG, "enc": _ENC, "kid": kid}
    if scope:
        protected[_FLENC_SCOPE] = scope
    token = jwe.encrypt_compact(protected, payload, key, registry=_REGISTRY)
    return PREFIX + token


def _decrypt_to_payload(scope: str, token: str, *, topic: str | None = None,
                        wire_key: str | None = None) -> bytes:
    """Decrypt a ``flenc`` token to raw payload bytes, enforcing the scope ratchet.

    Selects the key by the token's ``kid`` and enforces the ``(alg, enc)``
    allowlist. Then, comparing the codec's ``scope`` to the token's stamped
    ``flenc_scope``:

    - ``scope`` set: reject only when the token carries a *different* scope
      (relocation). An unscoped token is accepted (upgrade / fallback) — a
      key-less attacker can neither mint nor strip a scope (it's the AEAD's
      associated data), so this is equivalent to strict once every token is
      scoped.
    - ``scope`` empty: reject when the token *carries* a scope (downgrade
      blocked — a scoped token may not be read by an unscoped codec).

    Every failure surfaces as ``SecretDecryptError``.
    """
    # Resolve the keyring first: a missing keyring is a deployment error that
    # should win over any complaint about the token's shape.
    keyring = active_keyring()
    compact = _compact(token)
    kid = _read_kid(compact)
    try:
        key_bytes = keyring.key_for(kid)
    except UnknownKeyError as e:
        raise SecretDecryptError(scope=scope, kid=kid, topic=topic, wire_key=wire_key) from e
    key = OctKey.import_key(key_bytes, {"kid": kid})
    try:
        obj = jwe.decrypt_compact(compact, key, registry=_REGISTRY)
    except Exception as e:
        raise SecretDecryptError(scope=scope, kid=kid, topic=topic, wire_key=wire_key) from e
    stamped = obj.protected.get(_FLENC_SCOPE)  # str | None
    if scope:
        if stamped is not None and stamped != scope:
            raise SecretDecryptError(
                scope=scope, kid=kid, topic=topic, wire_key=wire_key,
                reason=f"scope mismatch: token scoped {stamped!r}, read as {scope!r}",
            )
    elif stamped is not None:
        raise SecretDecryptError(
            scope=scope, kid=kid, topic=topic, wire_key=wire_key,
            reason=f"token is scoped {stamped!r} but this codec declares no scope (downgrade blocked)",
        )
    active_observer().secret_decrypted(scope, kid)
    return obj.plaintext


# --- the codec ---


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
    def encode(value: object) -> str:
        # sort_keys + allow_nan=False match the framework's canonical JSON
        # (kafka.encode_json), so encrypted RECORD/DICT payloads are
        # deterministic and NaN is rejected rather than emitted.
        payload = json.dumps(
            inner.encode(value), allow_nan=False, ensure_ascii=False,
            separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")
        return _encrypt_payload(scope, payload)

    def decode(value: object) -> object:
        if is_encrypted(value):
            return inner.decode(json.loads(_decrypt_to_payload(scope, value)))  # type: ignore[arg-type]
        if read_plaintext:
            log.warning("Secret value read as legacy plaintext (scope=%r) — migrate this record to ciphertext", scope)
            active_observer().secret_plaintext_read(scope)
            return inner.decode(value)
        raise PlaintextSecretError(scope=scope)

    return _EncryptedCodec(decode=decode, encode=encode, scope=scope)


# --- ops tooling primitives ---


def _require_encrypted(attribute: Attribute) -> _EncryptedCodec:
    codec = attribute.codec
    if not isinstance(codec, _EncryptedCodec):
        raise SecretError(f"{attribute!r} is not an encrypted attribute (its codec is not ENCRYPTED)")
    return codec


def encrypt_value[V](attribute: Attribute[V], value: V) -> str:
    """Encrypt ``value`` for ``attribute`` and return the ``flenc:jwe:`` token.

    The write-side boundary for producers and form backends — the primary
    defense that keeps malformed tokens off the topic. Raises ``SecretError``
    if ``attribute`` is not an encrypted attribute.
    """
    return _require_encrypted(attribute).encode(value)


def reencrypt[V](token: str, attribute: Attribute[V]) -> str:
    """Re-encrypt ``token`` under the current primary key (decrypt-with-named-kid).

    The building block for a rotation sweep and for cross-environment
    promotion: it decrypts with the key the token names, then re-encrypts the
    same payload under the primary — preserving the exact inner bytes without
    round-tripping through the decoded value. Also promotes an unscoped token to
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
    a `ScanEntry` with ``error`` set rather than aborting the whole scan.
    """
    names = [a.name for a in attributes]
    topics = list(topics)
    found: list[ScanEntry] = []

    async def collect(msg) -> None:
        if is_tombstone(msg.value):
            return
        wire_key = decode_key(msg.key)
        raw = decode_event(msg.value, f"{msg.topic}/{msg.offset}").raw
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
