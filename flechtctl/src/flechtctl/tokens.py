"""The `flenc:jwe:` / `flsig:jws:` token format — seal, open, sign, verify — over an explicit keyring.

The **only** flechtctl module that imports ``joserfc`` (and importing it is
lazy: ``import flechtctl`` does not load it, the first access of a name from
this module through the package does). Nothing here reads process-global
state: every function takes the `Keyring` it works with, and none emits
telemetry — that is the framework's layer.

A token is sealed under a keyring in one of two generations: symmetric
``dir`` + ``A256GCM`` under an oct key, or ``ECDH-ES+A256KW`` + ``A256GCM`` to
an X25519 recipient key (whose public half seals and whose private half
opens). Readers pin a strict ``(alg, enc)`` allowlist per generation, and the
**kind of the key behind the token's ``kid``** selects which one applies — a
token never chooses. A key bound to scopes in the keyring serves only those
scopes, on write and on read. The optional ``scope`` binds a token into a
domain-separation compartment (``flenc_scope`` in the integrity-protected
header) — a one-way ratchet: adding a scope is non-breaking, dropping it is
blocked.

``sign`` / ``verify`` produce and check ``flsig:jws:`` Ed25519 compact JWS
tokens, whose scope is required and checked exactly.

The JSON-level functions (`encrypt`, `decrypt`, `sign`, `verify`) work on any
JSON value; the ``*_bytes`` variants on raw payload bytes. ``encrypt(k, v,
scope=s)`` writes the same token flechtwerk's ``ENCRYPTED(inner, scope=s)``
writes for ``v == inner.encode(x)``.

A keyring file may be password-protected: `encrypt_keyring` / `decrypt_keyring`
wrap the JWK Set in an RFC 7517 §7 encrypted JWK Set (``PBES2-HS512+A256KW``).
"""
import base64
import functools
import json
import secrets

from joserfc import jwe, jws
from joserfc.jwk import OctKey, OKPKey
from joserfc.registry import HeaderParameter

from .errors import SecretDecryptError, SecretFormatError, SecretSignatureError
from .keyring import (
    Ed25519Key,
    KEY_BYTES,
    Keyring,
    KeyringError,
    UnknownKeyError,
    X25519Key,
    _decode_key,
)

__all__ = [
    "PREFIX",
    "SCHEME",
    "SIGNATURE_PREFIX",
    "SIGNATURE_SCHEME",
    "canonical_json",
    "decrypt",
    "decrypt_bytes",
    "decrypt_keyring",
    "encrypt",
    "encrypt_bytes",
    "encrypt_keyring",
    "generate_ed25519_key",
    "generate_oct_key",
    "generate_x25519_key",
    "is_encrypted",
    "is_signed",
    "kid_of",
    "read_header",
    "reencrypt",
    "sign",
    "sign_bytes",
    "signer_of",
    "verify",
    "verify_bytes",
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

# A password-protected keyring file (an encrypted JWK Set) is not a token: its
# own pinned registry, separate from the three above.
_KEYRING_ALG = "PBES2-HS512+A256KW"
_KEYRING_CTY = "jwk-set+json"
_KEYRING_ITERATIONS = 210000  # OWASP's figure for PBKDF2-HMAC-SHA512; joserfc rejects >= 300000
_KEYRING_REGISTRY = jwe.JWERegistry(algorithms=[_KEYRING_ALG, _ENC])
_MIN_PASSWORD_BYTES = 14  # joserfc warns below 112 bits; the check here refuses first


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


def _read_header(compact: str) -> dict:
    """Decode the protected header of a compact JWE or JWS without any key (base64url decode)."""
    header_segment = compact.split(".", 1)[0]
    try:
        padded = header_segment + "=" * (-len(header_segment) % 4)
        header = json.loads(base64.urlsafe_b64decode(padded))
    except Exception as e:
        raise SecretFormatError(f"malformed JWE header: {e}") from e
    if not isinstance(header, dict):
        raise SecretFormatError(f"malformed JWE header: expected a JSON object, got {type(header).__name__}")
    return header


def _read_kid(compact: str) -> str:
    """Read the ``kid`` from a compact JWE or JWS header without the key (base64url decode).

    Used only to *select* which key to hand the decryptor or verifier; the
    header is re-parsed and authenticated during the actual decrypt (as the
    AEAD's associated data) or verify (as part of the signing input), so a
    tampered kid fails regardless.
    """
    kid = _read_header(compact).get("kid")
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


def signer_of(token: str) -> str:
    """Return the ``kid`` of the key that signed a ``flsig`` token (no key needed).

    Raises ``SecretFormatError`` for a non-``flsig`` value.
    """
    if not is_signed(token):
        raise SecretFormatError("not a flsig token")
    return _read_kid(_signature_compact(token))


def read_header(token: str) -> dict:
    """The decoded protected header of a ``flenc`` or ``flsig`` token — **unauthenticated**.

    No key is involved, so nothing here is verified: the header is only
    trustworthy once `decrypt` / `verify` has succeeded. For inspection and
    tooling. Raises ``SecretFormatError`` for any other value or a malformed header.
    """
    if is_encrypted(token):
        return _read_header(_compact(token))
    if is_signed(token):
        return _read_header(_signature_compact(token))
    raise SecretFormatError("not a flenc or flsig token")


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


def canonical_json(value: object) -> bytes:
    """Canonical JSON bytes: compact, sorted keys, UTF-8, NaN rejected.

    Matches the framework's canonical JSON, so encrypted / signed RECORD/DICT
    payloads are deterministic.
    """
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")


# --- seal / open / sign / verify over an explicit keyring ---


def encrypt_bytes(keyring: Keyring, payload: bytes, *, scope: str = "") -> str:
    """Encrypt raw payload bytes under the scope's sealing key, stamping ``scope`` if set.

    The keyring picks the kid (``primaries[scope]``, else ``primary``) and
    refuses a scope outside that key's binding. An oct kid seals ``dir``; an
    X25519 recipient kid seals ``ECDH-ES+A256KW`` to its public half.
    """
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


def decrypt_bytes(keyring: Keyring, token: str, *, scope: str = "", topic: str | None = None,
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
    return obj.plaintext


def _require_scope(verb: str, scope: str) -> None:
    if not scope:
        raise ValueError(f"{verb} needs a non-empty scope — a signature always names the compartment it vouches for")


def sign_bytes(keyring: Keyring, payload: bytes, *, scope: str) -> str:
    """Sign raw payload bytes with the keyring's signing key for ``scope``; return the ``flsig:jws:`` token.

    ``scope`` is required: a signature always names the compartment it vouches
    for. The keyring's ``signing[scope]`` names the key; none configured raises
    ``KeyringError``.
    """
    _require_scope("sign", scope)
    kid = keyring.signing_kid(scope)
    signer = keyring.signer_for(kid)
    key = _okp_private_key(kid, "Ed25519", signer.public, signer.private)
    protected = {"alg": _SIGNATURE_ALG, _FLENC_SCOPE: scope, "kid": kid}
    token = jws.serialize_compact(protected, payload, key, registry=_SIGNATURE_REGISTRY)
    return SIGNATURE_PREFIX + token


def verify_bytes(keyring: Keyring, token: str, *, scope: str) -> bytes:
    """Verify a ``flsig`` token to raw payload bytes, enforcing scope and binding.

    Selects the signer by the token's ``kid`` and pins ``Ed25519``. The token's
    stamped ``flenc_scope`` must **equal** ``scope`` — there is no ratchet, as
    no unscoped signature exists — and ``scope`` must be within the signer's
    binding. Every failure surfaces as ``SecretSignatureError``.
    """
    _require_scope("verify", scope)
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
    return obj.payload


def encrypt(keyring: Keyring, value: object, *, scope: str = "") -> str:
    """Encrypt a JSON value (as canonical JSON) and return the ``flenc:jwe:`` token."""
    return encrypt_bytes(keyring, canonical_json(value), scope=scope)


def decrypt(keyring: Keyring, token: str, *, scope: str = "") -> object:
    """Decrypt a ``flenc`` token and parse its payload as JSON."""
    return json.loads(decrypt_bytes(keyring, token, scope=scope))


def sign(keyring: Keyring, value: object, *, scope: str) -> str:
    """Sign a JSON value (as canonical JSON) and return the ``flsig:jws:`` token."""
    return sign_bytes(keyring, canonical_json(value), scope=scope)


def verify(keyring: Keyring, token: str, *, scope: str) -> object:
    """Verify a ``flsig`` token and parse its payload as JSON."""
    return json.loads(verify_bytes(keyring, token, scope=scope))


def reencrypt(keyring: Keyring, token: str, *, scope: str = "") -> str:
    """Re-encrypt ``token`` under the scope's current sealing key (decrypt-with-named-kid).

    The building block for a rotation sweep and for cross-environment
    promotion: it decrypts with the key the token names, then re-encrypts the
    same payload under the keyring's sealing key for ``scope``
    (``primaries[scope]``, else ``primary``) — which moves a token from ``dir``
    to a recipient key, from one recipient key to another, and back. Opening a
    recipient token needs that recipient's private half. The exact inner bytes
    are preserved, never round-tripped through the decoded value. Also promotes an
    unscoped token to ``scope`` (the ratchet's upgrade path); it cannot strip a
    scope. Raises ``SecretDecryptError`` if the token cannot be read.
    """
    return encrypt_bytes(keyring, decrypt_bytes(keyring, token, scope=scope), scope=scope)


# --- password-protected keyring documents ---


def encrypt_keyring(keyring: Keyring, password: str) -> str:
    """The keyring as a password-protected document: an encrypted JWK Set (RFC 7517 §7).

    A compact JWE over ``keyring.to_json()``, ``PBES2-HS512+A256KW`` +
    ``A256GCM``, 210 000 iterations, a fresh salt per call. Raises
    ``KeyringError`` for a password shorter than 14 bytes.
    """
    key = _password_key(password)
    header = {"alg": _KEYRING_ALG, "cty": _KEYRING_CTY, "enc": _ENC, "p2c": _KEYRING_ITERATIONS}
    return jwe.encrypt_compact(header, keyring.to_json().encode("utf-8"), key, registry=_KEYRING_REGISTRY)


def decrypt_keyring(text: str, password: str) -> Keyring:
    """Open a password-protected keyring document.

    Any failure to open it — malformed, a different algorithm, a wrong
    password — raises ``KeyringError`` chaining the cause.
    """
    message = "cannot open the password-protected keyring — wrong password or damaged file"
    try:
        obj = jwe.decrypt_compact(text.strip(), _password_key(password), registry=_KEYRING_REGISTRY)
        plaintext = obj.plaintext.decode("utf-8")
    except Exception as e:
        raise KeyringError(message) from e
    return Keyring.from_json(plaintext)


def _password_key(password: str) -> OctKey:
    raw = password.encode("utf-8")
    if len(raw) < _MIN_PASSWORD_BYTES:
        raise KeyringError(f"a keyring password must be at least {_MIN_PASSWORD_BYTES} bytes")
    return OctKey.import_key(raw)


# --- key generation ---


def generate_oct_key() -> bytes:
    """A fresh 32-byte AES-256 key, for the `keys` of a keyring."""
    return secrets.token_bytes(KEY_BYTES)


def _generate_okp(crv: str) -> tuple[bytes, bytes]:
    jwk = OKPKey.generate_key(crv).as_dict(private=True)
    return _decode_key(jwk["x"], "x"), _decode_key(jwk["d"], "d")


def generate_x25519_key() -> X25519Key:
    """A fresh X25519 recipient key pair (both halves present)."""
    public, private = _generate_okp("X25519")
    return X25519Key(public=public, private=private)


def generate_ed25519_key() -> Ed25519Key:
    """A fresh Ed25519 signing key pair (both halves present)."""
    public, private = _generate_okp("Ed25519")
    return Ed25519Key(public=public, private=private)
