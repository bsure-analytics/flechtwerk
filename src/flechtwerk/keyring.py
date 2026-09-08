"""The `Keyring` value object and the process-global secret runtime.

This module holds **no cryptography and imports no joserfc** — it is the
joserfc-free seam that `module.py` imports at decoration time to annotate its
`keyring` slot (the `MqttBrokerConfig` precedent, for the same
reactor-di-resolves-annotations-at-decoration reason). The actual encryption
lives in `flechtwerk.secrets`, which reads the process-global installed here.

A key is 32 random bytes named by a `kid`. The `Keyring` is the small value
object holding a few of them plus which one is `primary` (used to encrypt). It
parses from — and its file form is — an RFC 7517 JWK Set with a single
`primary` extension member, so the same document loads in joserfc, panva jose,
and Nimbus.

The keyring is a **process-global** installed once at startup: attributes are
module-level constants, so their `ENCRYPTED` codecs cannot carry key material
at declaration time and must resolve it lazily at encode/decode time. This is
process-global mutable state — the honest price of module-level `Attribute`
constants — not per-instance state like `Stage.configs`. `install_keyring` is
idempotent for byte-identical material and raises on a conflicting install, so
two embedded modules with different keyrings fail loudly rather than
silently last-writer-wins into wrong-key encryption.
"""
import base64
import json
import logging
from contextvars import ContextVar, Token
from dataclasses import dataclass

from .observer import Observer

log = logging.getLogger(__name__)

__all__ = [
    "Keyring",
    "KeyringConflictError",
    "KeyringError",
    "KeyringNotInstalledError",
    "UnknownKeyError",
    "active_keyring",
    "active_observer",
    "install_keyring",
    "set_secret_observer",
]

KEY_BYTES = 32
"""AES-256 key length — the only key size this framework mints or accepts."""


class KeyringError(Exception):
    """Base class for keyring problems (construction, install, key lookup)."""


class KeyringConflictError(KeyringError):
    """A second, different keyring was installed in a process that already has one."""


class KeyringNotInstalledError(KeyringError):
    """An `ENCRYPTED` codec ran with no keyring installed in this process."""


class UnknownKeyError(KeyringError):
    """A token names a `kid` absent from the active keyring.

    In an incident this is the "the key is missing here" signature, distinct
    from a GCM tag failure (same `kid` bound to different bytes).
    """

    def __init__(self, kid: str) -> None:
        super().__init__(f"no key {kid!r} in the active keyring")
        self.kid = kid


def _decode_key(raw: str) -> bytes:
    """Decode an unpadded base64url JWK `k` value to raw key bytes.

    Validates strictly: `urlsafe_b64decode` alone (validate off) silently
    scrubs stray characters and returns wrong-length bytes; here a malformed
    `k` raises `ValueError` (`binascii.Error`) so `from_json` reports it as a
    `KeyringError` about the base64, not a confusing wrong-length key error.
    """
    if not isinstance(raw, str):
        raise ValueError(f"'k' must be a base64url string, got {type(raw).__name__}")
    standard = raw.translate(_B64URL_TO_STD)
    return base64.b64decode(standard + "=" * (-len(raw) % 4), validate=True)


_B64URL_TO_STD = str.maketrans("-_", "+/")


@dataclass(frozen=True)
class Keyring:
    """The keys and which one is primary — pure key material.

    Build via `Keyring.of(...)` (raw bytes) or `Keyring.from_json(...)` (a
    JWK Set document). Both validate: every key is 32 bytes and `primary`
    names a held key.
    """

    keys: dict[str, bytes]
    primary: str

    def __post_init__(self) -> None:
        if not self.keys:
            raise KeyringError("a keyring needs at least one key")
        for kid, key in self.keys.items():
            if not kid:
                raise KeyringError("a key id must be a non-empty string")
            if len(key) != KEY_BYTES:
                raise KeyringError(f"key {kid!r} is {len(key)} bytes, expected {KEY_BYTES} (AES-256)")
        if self.primary not in self.keys:
            raise KeyringError(f"primary {self.primary!r} is not among the keys {sorted(self.keys)}")

    @classmethod
    def of(cls, keys: dict[str, bytes], *, primary: str) -> "Keyring":
        """Build a keyring from raw key bytes — the programmatic / test entry point."""
        return cls(keys=dict(keys), primary=primary)

    @classmethod
    def from_json(cls, text: str) -> "Keyring":
        """Parse an RFC 7517 JWK Set with a `primary` extension member.

        Accepts oct keys with an unpadded-base64url `k` and a `kid`; unknown
        members (top-level and per-key) are ignored, per JWK. This is the
        deployment entry point — the application reads the document from
        wherever it lives (a mounted secret, a file, an env var it reads
        itself) and hands the text in.
        """
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as e:
            raise KeyringError(f"keyring document is not valid JSON: {e}") from e
        if not isinstance(doc, dict):
            raise KeyringError(f"keyring document must be a JSON object, got {type(doc).__name__}")
        raw_keys = doc.get("keys", [])
        if not isinstance(raw_keys, list):
            raise KeyringError("keyring 'keys' must be a JSON array")
        keys: dict[str, bytes] = {}
        for jwk in raw_keys:
            if not isinstance(jwk, dict):
                raise KeyringError(f"each JWK must be a JSON object, got {type(jwk).__name__}")
            if jwk.get("kty") != "oct":
                raise KeyringError(f"unsupported key type {jwk.get('kty')!r} — only oct (symmetric) keys are used")
            kid = jwk.get("kid")
            if not isinstance(kid, str) or not kid:
                raise KeyringError(f"every JWK needs a non-empty string kid, got {kid!r}")
            if kid in keys:
                raise KeyringError(f"duplicate kid {kid!r} in the keyring")
            if "k" not in jwk:
                raise KeyringError(f"JWK {kid!r} has no 'k' (key material)")
            try:
                keys[kid] = _decode_key(jwk["k"])
            except (TypeError, ValueError) as e:  # binascii.Error ⊂ ValueError
                raise KeyringError(f"JWK {kid!r} has malformed base64url 'k': {e}") from e
        if "primary" not in doc:
            raise KeyringError("keyring document has no top-level 'primary' member")
        return cls(keys=keys, primary=doc["primary"])

    def key_for(self, kid: str) -> bytes:
        """Return the key bytes for `kid`, or raise `UnknownKeyError`."""
        try:
            return self.keys[kid]
        except KeyError:
            raise UnknownKeyError(kid) from None

    def primary_pair(self) -> tuple[str, bytes]:
        """Return `(primary_kid, primary_key_bytes)` — what encryption stamps and uses."""
        return self.primary, self.keys[self.primary]

    def kids(self) -> list[str]:
        """Loaded key ids, sorted — the labels for the startup keyring gauge."""
        return sorted(self.keys)


# --- secret runtime: process-global keyring, context-scoped observer ---

_keyring: Keyring | None = None
_observer: ContextVar[Observer] = ContextVar("flechtwerk_secret_observer", default=Observer())


def install_keyring(keyring: Keyring) -> None:
    """Install THE process keyring. Idempotent for identical material; else raises.

    Called by `Flechtwerk.of(keyring=...)` and directly by ops tooling that
    encrypts without running a stage. A second install of byte-identical
    material is a no-op; a second install of *different* material raises
    `KeyringConflictError`.
    """
    global _keyring
    if _keyring is not None and _keyring != keyring:
        raise KeyringConflictError(
            "a different keyring is already installed in this process — one keyring per process (v1)"
        )
    if _keyring is None:
        log.info("Installed keyring: %d key(s), primary %r", len(keyring.keys), keyring.primary)
    _keyring = keyring


def current_keyring() -> Keyring | None:
    """The installed keyring, or None — the non-raising query.

    Internal (not in `__all__`): no production caller yet, so it is not an API
    commitment. Used by the test fixtures to observe install state.
    """
    return _keyring


def active_keyring() -> Keyring:
    """The installed keyring, or raise `KeyringNotInstalledError`.

    The codec resolves the keyring through this on every encode/decode: an
    `ENCRYPTED` attribute with no keyring installed is a deployment error to
    surface, never a plaintext pass-through.
    """
    if _keyring is None:
        raise KeyringNotInstalledError(
            "no keyring installed — call flechtwerk.secrets.install_keyring(...) "
            "or pass keyring= to Flechtwerk.of(...)"
        )
    return _keyring


def set_secret_observer(observer: Observer) -> Token[Observer]:
    """Bind the observer the codec emits secret events through — in THIS context.

    A stage binds its own observer in `__aenter__` and resets it with the
    returned token in `__aexit__` (`token.var.reset(token)`, same task). The
    binding lives in the asyncio context, and every task the runner spawns
    afterwards — poll cycles, gathered buckets, token tasks, the paho socket
    callbacks — inherits a copy, so a `secret_decrypted` /
    `secret_plaintext_read` fired deep in a lazy `ConfigStore.get()`, where
    no observer is in scope, reaches THIS stage's observer and carries its
    `metrics_labels`. Several stages in one process are several tasks with
    several contexts, so each stage's secret metrics are its own — nothing is
    shared, nothing is first-wins. (Fanning one event out to every stage's
    observer would count each decrypt once per stage.) Outside any stage —
    ops tooling that only encrypts — the default is the no-op `Observer`.

    Unlike the keyring, which stays process-global: co-hosted stages reading
    one config topic must agree on key material anyway.
    """
    return _observer.set(observer)


def active_observer() -> Observer:
    """The secret observer bound in the current context (no-op `Observer` by default)."""
    return _observer.get()


def _override_secret_runtime(
    keyring: Keyring | None, observer: Observer | None,
) -> tuple[Keyring | None, Token[Observer]]:
    """Test-only: force the runtime and return the previous state for restore.

    Not public API — the sanctioned test entry point is
    `flechtwerk.testing.installed_keyring`, which pairs this with
    `_restore_secret_runtime` so suites cannot leak keyrings across tests.
    The observer half is a context binding, so both halves must run in the
    same context (a fixture's setup and teardown do).
    """
    global _keyring
    previous = (_keyring, _observer.set(observer if observer is not None else Observer()))
    _keyring = keyring
    return previous


def _restore_secret_runtime(previous: tuple[Keyring | None, Token[Observer]]) -> None:
    """Test-only: restore a runtime snapshot taken by `_override_secret_runtime`."""
    global _keyring
    _keyring, token = previous
    _observer.reset(token)
