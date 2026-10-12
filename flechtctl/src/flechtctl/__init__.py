"""Keyrings and `flenc:`/`flsig:` secret tokens for Flechtwerk — without the framework.

This package owns the keyring model (`Keyring`, `X25519Key`, `Ed25519Key`) and
the token format (seal, open, sign, verify, the scope ratchet, the key-binding
checks), and depends on joserfc only — never on flechtwerk. The API takes the
keyring explicitly, with no process-global state:

    from flechtctl import Keyring, decrypt, encrypt

    keyring = Keyring.from_json(open("keyring.json").read())
    token = encrypt(keyring, "s3cret", scope="api_key")   # flenc:jwe:...
    decrypt(keyring, token, scope="api_key")              # "s3cret"

`encrypt(k, v, scope=s)` writes the same token flechtwerk's
`ENCRYPTED(inner, scope=s)` codec writes for `v == inner.encode(x)` — for a
`BYTES` field, pass the base64 string. The `flechtctl` command wraps the same
functions for the shell (`flechtctl --help`).

`import flechtctl` loads no joserfc: the keyring and error names are imported
eagerly, every `flechtctl.tokens` name lazily on first access (PEP 562).
"""
import importlib

from .errors import (
    PlaintextSecretError,
    SecretDecryptError,
    SecretError,
    SecretFormatError,
    SecretSignatureError,
    UnsignedSecretError,
)
from .keyring import (
    Ed25519Key,
    KEY_BYTES,
    Keyring,
    KeyringError,
    UnknownKeyError,
    X25519Key,
    is_encrypted_keyring,
    keyring_from_jwk,
)

__all__ = [
    "Ed25519Key",
    "KEY_BYTES",
    "Keyring",
    "KeyringError",
    "PREFIX",
    "PlaintextSecretError",
    "SCHEME",
    "SIGNATURE_PREFIX",
    "SIGNATURE_SCHEME",
    "SecretDecryptError",
    "SecretError",
    "SecretFormatError",
    "SecretSignatureError",
    "UnknownKeyError",
    "UnsignedSecretError",
    "X25519Key",
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
    "is_encrypted_keyring",
    "is_signed",
    "keyring_from_jwk",
    "kid_of",
    "read_header",
    "reencrypt",
    "sign",
    "sign_bytes",
    "signer_of",
    "verify",
    "verify_bytes",
]

_TOKEN_NAMES = frozenset(name for name in __all__ if name not in globals())  # everything the eager imports did not bind


def __getattr__(name: str) -> object:
    if name in _TOKEN_NAMES:
        return getattr(importlib.import_module(".tokens", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
