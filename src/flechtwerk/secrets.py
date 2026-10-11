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
import base64
import functools
import json
import logging
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer
from joserfc import jwe, jws
from joserfc.jwk import OctKey, OKPKey
from joserfc.registry import HeaderParameter

from flechtwerk.attribute import Attribute
from flechtwerk.attribute.codec import Codec

from .kafka import decode_key, decode_record, is_tombstone, read_to_end, topic_partitions
from .types import Config
from .keyring import (
    Ed25519Key,
    Keyring,
    KeyringError,
    UnknownKeyError,
    X25519Key,
    active_keyring,
    active_observer,
    install_keyring,
)

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

SCHEME = "flenc"
"""The URI scheme naming a Flechtwerk-encrypted value (and the backend dispatch point)."""

_ENVELOPE = "jwe"  # the encoding segment: RFC 7516 *compact* serialization, this design's profile
PREFIX = f"{SCHEME}:{_ENVELOPE}:"
"""The full token prefix, ``flenc:jwe:`` — a valid URI scheme + envelope segment."""

SIGNATURE_SCHEME = "flsig"
"""The URI scheme naming a Flechtwerk-signed value — ``flenc:`` stays reserved for ciphertext forms."""

_SIGNATURE_ENVELOPE = "jws"  # RFC 7515 *compact* serialization
SIGNATURE_PREFIX = f"{SIGNATURE_SCHEME}:{_SIGNATURE_ENVELOPE}:"
"""The full signed-value prefix, ``flsig:jws:``."""

_SCHEME_PREFIX = f"{SCHEME}:"  # any value under this scheme is ciphertext-form
_SIGNATURE_SCHEME_PREFIX = f"{SIGNATURE_SCHEME}:"  # any value under this scheme is signed-form
_ALG = "dir"
_ENC = "A256GCM"
_FLENC_SCOPE = "flenc_scope"  # optional protected-header param — the domain-separation scope
_RECIPIENT_ALG = "ECDH-ES+A256KW"
_SIGNATURE_ALG = "Ed25519"  # RFC 9864, fully specified — never the deprecated polymorphic "EdDSA"

# The one extension header every registry admits: flenc_scope is OPTIONAL (a
# token may carry a scope or not).
_HEADER_REGISTRY = {_FLENC_SCOPE: HeaderParameter("Flechtwerk domain-separation scope", "str", False)}

# The acceptance rule: exactly two (alg, enc) pairs, one registry each —
# dir+A256GCM for an oct kid, ECDH-ES+A256KW+A256GCM for an X25519 recipient
# kid. The KIND OF THE KEY behind the token's kid selects the registry; the
# token's own header never chooses, so neither generation can be confused for
# the other. Widening an allowlist (a future hybrid post-quantum mode) is a
# deliberate reader-side change, never token content.
_REGISTRY = jwe.JWERegistry(
    header_registry=_HEADER_REGISTRY,
    algorithms=[_ALG, _ENC],
    strict_check_header=True,
)
_RECIPIENT_REGISTRY = jwe.JWERegistry(
    header_registry=_HEADER_REGISTRY,
    algorithms=[_RECIPIENT_ALG, _ENC],
    strict_check_header=True,
)
# Signatures: exactly Ed25519. A signed token always carries flenc_scope (the
# codec checks it), the registry merely admits the header.
_SIGNATURE_REGISTRY = jws.JWSRegistry(
    header_registry=_HEADER_REGISTRY,
    algorithms=[_SIGNATURE_ALG],
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


class SecretSignatureError(SecretError):
    """A ``flsig`` token could not be verified, or vouches for a different scope.

    Shaped like `SecretDecryptError`: the codec's ``scope``, the token's signer
    ``kid``, ``topic`` / ``wire_key`` when a caller has them, and a ``reason``.
    An unknown signer keeps `UnknownKeyError` as ``__cause__``.
    """

    def __init__(self, *, scope: str = "", kid: str | None = None,
                 topic: str | None = None, wire_key: str | None = None,
                 reason: str | None = None) -> None:
        self.scope = scope
        self.kid = kid
        self.topic = topic
        self.wire_key = wire_key
        detail = ["cannot verify signed value"]
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


class UnsignedSecretError(SecretError):
    """A ``SIGNED`` value is unsigned but the codec's ``read_unsigned`` is off."""

    def __init__(self, *, scope: str = "", topic: str | None = None, wire_key: str | None = None) -> None:
        self.scope = scope
        self.topic = topic
        self.wire_key = wire_key
        detail = ["value is unsigned but read_unsigned is off for this attribute"]
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


def is_signed(value: object) -> bool:
    """True if ``value`` is signed-form — a string under the ``flsig:`` scheme."""
    return isinstance(value, str) and value.startswith(_SIGNATURE_SCHEME_PREFIX)


def _strip_envelope(token: str, scheme: str, expected: str) -> str:
    body = token[len(scheme) + 1:]
    envelope, sep, compact = body.partition(":")
    if not sep or envelope != expected or not compact:
        raise SecretFormatError(f"unknown {scheme} envelope {envelope!r} — expected {expected!r}")
    return compact


def _compact(token: str) -> str:
    """Strip ``flenc:jwe:`` and return the compact JWE, or raise on a bad envelope.

    An unknown envelope segment (a future ``flenc:`` form this reader does not
    understand) raises in every keyring mode — it is never handed to an inner
    codec as legacy plaintext.
    """
    return _strip_envelope(token, SCHEME, _ENVELOPE)


def _signature_compact(token: str) -> str:
    """Strip ``flsig:jws:`` and return the compact JWS, or raise on a bad envelope (as `_compact`)."""
    return _strip_envelope(token, SIGNATURE_SCHEME, _SIGNATURE_ENVELOPE)


def _read_kid(compact: str) -> str:
    """Read the ``kid`` from a compact JWE or JWS header without the key (base64url decode).

    Used only to *select* which key to hand the decryptor or verifier; the
    header is re-parsed and authenticated during the actual decrypt (as the
    AEAD's associated data) or verify (as part of the signing input), so a
    tampered kid fails regardless.
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


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


@functools.cache
def _okp_private_key(kid: str, crv: str, public: bytes, private: bytes) -> OKPKey:
    """Import a private OKP key and check that its halves belong together.

    joserfc imports a mismatched pair without complaint, so the check derives
    the public half from the private one. Cached: it runs once per key
    material. A mismatch is a keyring misconfiguration and propagates as
    `KeyringError`, unwrapped.
    """
    key = OKPKey.import_key({"crv": crv, "d": _b64url(private), "kty": "OKP", "x": _b64url(public)})
    if key.private_key.public_key().public_bytes_raw() != public:
        raise KeyringError(f"{crv} key {kid!r}: the private key does not match its public key")
    return key


def _encrypt_payload(scope: str, payload: bytes) -> str:
    """Encrypt raw payload bytes under the scope's sealing key, stamping ``scope`` if set.

    The keyring picks the kid (``primaries[scope]``, else ``primary``) and
    refuses a scope outside that key's binding. An oct kid seals ``dir``; an
    X25519 recipient kid seals ``ECDH-ES+A256KW`` to its public half.
    """
    keyring = active_keyring()
    kid = keyring.sealing_kid(scope)
    if kid in keyring.recipients:
        recipient = keyring.recipient_for(kid)
        if recipient.private is not None:
            _okp_private_key(kid, "X25519", recipient.public, recipient.private)
        key = OKPKey.import_key({"crv": "X25519", "kty": "OKP", "x": _b64url(recipient.public)})
        protected = {"alg": _RECIPIENT_ALG, "enc": _ENC, "kid": kid}
        registry = _RECIPIENT_REGISTRY
    else:
        key = OctKey.import_key(keyring.key_for(kid), {"kid": kid})
        protected = {"alg": _ALG, "enc": _ENC, "kid": kid}
        registry = _REGISTRY
    if scope:
        protected[_FLENC_SCOPE] = scope
    token = jwe.encrypt_compact(protected, payload, key, registry=registry)
    return PREFIX + token


def _decrypt_to_payload(scope: str, token: str, *, topic: str | None = None,
                        wire_key: str | None = None) -> bytes:
    """Decrypt a ``flenc`` token to raw payload bytes, enforcing the scope ratchet.

    Selects the key by the token's ``kid`` — a recipient kid opens with its
    private half under the ``ECDH-ES+A256KW`` allowlist, any other kid is an oct
    key under the ``dir`` allowlist. Then, comparing the codec's ``scope`` to the token's stamped
    ``flenc_scope``:

    - ``scope`` set: reject only when the token carries a *different* scope
      (relocation). An unscoped token is accepted (upgrade / fallback) — a
      key-less attacker can neither mint nor strip a scope (it's the AEAD's
      associated data), so this is equivalent to strict once every token is
      scoped.
    - ``scope`` empty: reject when the token *carries* a scope (downgrade
      blocked — a scoped token may not be read by an unscoped codec).

    Finally, a key bound to scopes in the keyring rejects a token whose
    stamped scope (possibly none) is outside its binding. That check runs
    after decryption, because only then is the stamped header authenticated.

    Every token failure surfaces as ``SecretDecryptError``; a keyring whose
    recipient halves do not match raises ``KeyringError``.
    """
    # Resolve the keyring first: a missing keyring is a deployment error that
    # should win over any complaint about the token's shape.
    keyring = active_keyring()
    compact = _compact(token)
    kid = _read_kid(compact)
    if kid in keyring.recipients:
        recipient = keyring.recipient_for(kid)
        if recipient.private is None:
            raise SecretDecryptError(
                scope=scope, kid=kid, topic=topic, wire_key=wire_key,
                reason=f"keyring holds only the public half of {kid!r}: it can seal but not open",
            )
        key = _okp_private_key(kid, "X25519", recipient.public, recipient.private)
        registry = _RECIPIENT_REGISTRY
    else:
        try:
            key_bytes = keyring.key_for(kid)
        except UnknownKeyError as e:
            raise SecretDecryptError(scope=scope, kid=kid, topic=topic, wire_key=wire_key) from e
        key = OctKey.import_key(key_bytes, {"kid": kid})
        registry = _REGISTRY
    try:
        obj = jwe.decrypt_compact(compact, key, registry=registry)
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
    bound = keyring.scopes.get(kid)
    if bound is not None and stamped not in bound:
        raise SecretDecryptError(
            scope=scope, kid=kid, topic=topic, wire_key=wire_key,
            reason=f"key {kid!r} is bound to scopes {sorted(bound)}; token stamped {stamped!r}",
        )
    active_observer().secret_decrypted(scope, kid)
    return obj.plaintext


def _verify_to_payload(scope: str, token: str) -> bytes:
    """Verify a ``flsig`` token to raw payload bytes, enforcing scope and binding.

    Selects the signer by the token's ``kid`` and pins ``Ed25519``. The token's
    stamped ``flenc_scope`` must **equal** ``scope`` — there is no ratchet, as
    no unscoped signature exists — and ``scope`` must be within the signer's
    binding. Every failure surfaces as ``SecretSignatureError``.
    """
    keyring = active_keyring()
    compact = _signature_compact(token)
    kid = _read_kid(compact)
    try:
        signer = keyring.signer_for(kid)
    except UnknownKeyError as e:
        raise SecretSignatureError(scope=scope, kid=kid) from e
    key = OKPKey.import_key({"crv": "Ed25519", "kty": "OKP", "x": _b64url(signer.public)})
    try:
        obj = jws.deserialize_compact(compact, key, registry=_SIGNATURE_REGISTRY)
    except Exception as e:
        raise SecretSignatureError(scope=scope, kid=kid) from e
    stamped = obj.protected.get(_FLENC_SCOPE)
    if stamped != scope:
        raise SecretSignatureError(
            scope=scope, kid=kid, reason=f"scope mismatch: signature scoped {stamped!r}, read as {scope!r}",
        )
    bound = keyring.scopes[kid]
    if scope not in bound:
        raise SecretSignatureError(
            scope=scope, kid=kid, reason=f"signer {kid!r} is bound to scopes {sorted(bound)}",
        )
    active_observer().secret_verified(scope, kid)
    return obj.payload


def _canonical_json(value: object) -> bytes:
    # sort_keys + allow_nan=False match the framework's canonical JSON
    # (kafka.encode_json), so encrypted / signed RECORD/DICT payloads are
    # deterministic and NaN is rejected rather than emitted.
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")


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
    def encode(value: object) -> str:
        return _encrypt_payload(scope, _canonical_json(inner.encode(value)))

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

    def encode(value: object) -> str:
        keyring = active_keyring()
        kid = keyring.signing_kid(scope)
        signer = keyring.signer_for(kid)
        key = _okp_private_key(kid, "Ed25519", signer.public, signer.private)
        protected = {"alg": _SIGNATURE_ALG, _FLENC_SCOPE: scope, "kid": kid}
        token = jws.serialize_compact(protected, _canonical_json(inner.encode(value)), key,
                                      registry=_SIGNATURE_REGISTRY)
        return SIGNATURE_PREFIX + token

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

    The write-side boundary for producers and form backends — the primary
    defense that keeps malformed tokens off the topic. Raises ``SecretError``
    if ``attribute`` is not an encrypted attribute.
    """
    return _require_encrypted(attribute).encode(value)


def signer_of(token: str) -> str:
    """Return the ``kid`` of the key that signed a ``flsig`` token (no key needed).

    Raises ``SecretFormatError`` for a non-``flsig`` value.
    """
    if not is_signed(token):
        raise SecretFormatError("not a flsig token")
    return _read_kid(_signature_compact(token))


def sign_value[V](attribute: Attribute[V], value: V) -> str:
    """Sign ``value`` for ``attribute`` and return the ``flsig:jws:`` token.

    The write-side boundary for a ``SIGNED`` attribute, like `encrypt_value`
    for an encrypted one — for an ``ENCRYPTED(SIGNED(...))`` attribute the
    boundary stays `encrypt_value`. Raises ``SecretError`` if ``attribute`` is
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
