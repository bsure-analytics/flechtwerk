"""The `Keyring` value object and the process-global secret runtime.

This module holds **no cryptography and imports no joserfc** — it is the
joserfc-free seam that `module.py` imports at decoration time to annotate its
`keyring` slot (the `MqttBrokerConfig` precedent, for the same
reactor-di-resolves-annotations-at-decoration reason). The actual encryption
lives in `flechtwerk.secrets`, which reads the process-global installed here.

A key is named by a `kid` and comes in one of three kinds: a 32-byte
symmetric (oct) key in `keys`, an X25519 recipient key in `recipients` (its
public half seals, its private half opens), or an Ed25519 signing key in
`signers` (its private half signs, its public half verifies). The `Keyring` is
the small value object holding a few of them plus which one is `primary` (used
to encrypt). Any key may be **bound** to the scopes it serves (`scopes`);
public-key material must be, because holding a public half is enough to seal
or to check, and an unbound one would serve every compartment. Sealing and
signing keys are chosen per scope explicitly (`primaries`, `signing`). The
keyring parses from — and its file form is — an RFC 7517 JWK Set with a
`primary` extension member (plus the optional `flenc_primaries`,
`flenc_signing` and per-key `flenc_scopes` members), so the same document
loads in joserfc, panva jose, and Nimbus.

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
from dataclasses import dataclass, field

from .observer import Observer

log = logging.getLogger(__name__)

__all__ = [
    "Ed25519Key",
    "Keyring",
    "KeyringConflictError",
    "KeyringError",
    "KeyringNotInstalledError",
    "UnknownKeyError",
    "X25519Key",
    "active_keyring",
    "active_observer",
    "install_keyring",
    "set_secret_observer",
]

KEY_BYTES = 32
"""AES-256 key length — the only key size this framework mints or accepts.

X25519 and Ed25519 key halves happen to be 32 bytes too.
"""


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


def _decode_key(raw: str, member: str = "k") -> bytes:
    """Decode an unpadded base64url JWK member (`k`, `x` or `d`) to raw key bytes.

    Validates strictly: `urlsafe_b64decode` alone (validate off) silently
    scrubs stray characters and returns wrong-length bytes; here a malformed
    value raises `ValueError` (`binascii.Error`) so `from_json` reports it as a
    `KeyringError` about the base64, not a confusing wrong-length key error.
    """
    if not isinstance(raw, str):
        raise ValueError(f"{member!r} must be a base64url string, got {type(raw).__name__}")
    standard = raw.translate(_B64URL_TO_STD)
    return base64.b64decode(standard + "=" * (-len(raw) % 4), validate=True)


_B64URL_TO_STD = str.maketrans("-_", "+/")


@dataclass(frozen=True)
class Ed25519Key:
    """An Ed25519 signing key: `private` (the 32-byte seed) signs, `public` verifies.

    `private=None` makes a verify-only key — what a reader holds.
    """

    public: bytes
    private: bytes | None = field(default=None, repr=False)


@dataclass(frozen=True)
class X25519Key:
    """An X25519 recipient key: `public` seals, `private` opens.

    `private=None` makes a seal-only key — what a writer holds.
    """

    public: bytes
    private: bytes | None = field(default=None, repr=False)


@dataclass(frozen=True)
class Keyring:
    """The keys and which one is primary — pure key material.

    Build via `Keyring.of(...)` (raw bytes) or `Keyring.from_json(...)` (a
    JWK Set document). Both validate, raising `KeyringError` naming the
    offending kid or scope:

    1. at least one key in `keys` or `recipients`;
    2. every kid is a non-empty string, held in only one of `keys`,
       `recipients` and `signers`;
    3. every oct key is 32 bytes, every recipient / signing key has a 32-byte
       `public` and a `private` that is `None` or 32 bytes;
    4. `primary` names a key in `keys` or `recipients`;
    5. every kid in `scopes` is held, bound to a non-empty set of non-empty
       scope names;
    6. every recipient and signing kid is bound — public-key material always
       is; an oct key may be or not;
    7. every `primaries` entry maps a non-empty scope to a kid in `keys` or
       `recipients` whose binding, if any, covers that scope;
    8. every `signing` entry maps a non-empty scope to a signing kid holding a
       `private` half, whose binding covers that scope.
    """

    keys: dict[str, bytes]
    primary: str
    primaries: dict[str, str] = field(default_factory=dict)
    recipients: dict[str, X25519Key] = field(default_factory=dict)
    scopes: dict[str, frozenset[str]] = field(default_factory=dict)
    signers: dict[str, Ed25519Key] = field(default_factory=dict)
    signing: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "scopes", {
            kid: bound if isinstance(bound, str) else frozenset(bound) for kid, bound in self.scopes.items()
        })
        if not self.keys and not self.recipients:
            raise KeyringError("a keyring needs at least one key")
        kinds = (self.keys, self.recipients, self.signers)
        held = [kid for kind in kinds for kid in kind]
        for kid in held:
            if not isinstance(kid, str) or not kid:
                raise KeyringError("a key id must be a non-empty string")
            if held.count(kid) > 1:
                raise KeyringError(f"kid {kid!r} is held as more than one kind of key")
        for kid, key in self.keys.items():
            if len(key) != KEY_BYTES:
                raise KeyringError(f"key {kid!r} is {len(key)} bytes, expected {KEY_BYTES} (AES-256)")
        for kind, pairs in (("X25519", self.recipients), ("Ed25519", self.signers)):
            for kid, pair in pairs.items():
                if len(pair.public) != KEY_BYTES:
                    raise KeyringError(f"{kind} key {kid!r} has a {len(pair.public)}-byte public half, expected {KEY_BYTES}")
                if pair.private is not None and len(pair.private) != KEY_BYTES:
                    raise KeyringError(f"{kind} key {kid!r} has a {len(pair.private)}-byte private half, expected {KEY_BYTES}")
        if self.primary not in self.keys and self.primary not in self.recipients:
            raise KeyringError(
                f"primary {self.primary!r} is not among the keys {sorted(self.keys) + sorted(self.recipients)}"
            )
        for kid, bound in self.scopes.items():
            if kid not in held:
                raise KeyringError(f"scopes name kid {kid!r}, which the keyring does not hold")
            if (not isinstance(bound, frozenset) or not bound
                    or not all(isinstance(scope, str) and scope for scope in bound)):
                raise KeyringError(f"key {kid!r} must be bound to a non-empty set of non-empty scope names")
        for kid in [*self.recipients, *self.signers]:
            if kid not in self.scopes:
                raise KeyringError(f"public-key kid {kid!r} must be bound to the scopes it serves")
        for scope, kid in self.primaries.items():
            if not isinstance(scope, str) or not scope:
                raise KeyringError(f"primaries map an empty scope to {kid!r}")
            if kid not in self.keys and kid not in self.recipients:
                raise KeyringError(f"primaries map scope {scope!r} to {kid!r}, which is not a sealing key")
            if kid in self.scopes and scope not in self.scopes[kid]:
                raise KeyringError(f"primaries map scope {scope!r} to {kid!r}, which is not bound to it")
        for scope, kid in self.signing.items():
            if not isinstance(scope, str) or not scope:
                raise KeyringError(f"signing maps an empty scope to {kid!r}")
            if kid not in self.signers or self.signers[kid].private is None:
                raise KeyringError(f"signing maps scope {scope!r} to {kid!r}, which is not a private signing key")
            if scope not in self.scopes[kid]:
                raise KeyringError(f"signing maps scope {scope!r} to {kid!r}, which is not bound to it")

    @classmethod
    def of(
        cls,
        keys: dict[str, bytes],
        *,
        primary: str,
        primaries: dict[str, str] | None = None,
        recipients: dict[str, X25519Key] | None = None,
        scopes: dict[str, set[str] | frozenset[str]] | None = None,
        signers: dict[str, Ed25519Key] | None = None,
        signing: dict[str, str] | None = None,
    ) -> "Keyring":
        """Build a keyring from raw key bytes — the programmatic / test entry point."""
        return cls(
            keys=dict(keys),
            primary=primary,
            primaries=dict(primaries or {}),
            recipients=dict(recipients or {}),
            scopes={kid: bound if isinstance(bound, str) else frozenset(bound) for kid, bound in (scopes or {}).items()},
            signers=dict(signers or {}),
            signing=dict(signing or {}),
        )

    @classmethod
    def from_json(cls, text: str) -> "Keyring":
        """Parse an RFC 7517 JWK Set with a `primary` extension member.

        Accepts oct keys with an unpadded-base64url `k`, and OKP keys with
        `crv` `X25519` (recipients) or `Ed25519` (signers), a public `x` and an
        optional private `d`; every key needs a `kid`. A key's optional
        `flenc_scopes` member (a non-empty array of scope names) binds it —
        required for OKP keys. Two optional top-level members map scopes to
        kids: `flenc_primaries` (the sealing key per scope) and `flenc_signing`
        (the signing key per scope). Unknown members (top-level and per-key)
        are ignored, per JWK. This is the deployment entry point — the
        application reads the document from wherever it lives (a mounted
        secret, a file, an env var it reads itself) and hands the text in.
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
        recipients: dict[str, X25519Key] = {}
        scopes: dict[str, frozenset[str]] = {}
        signers: dict[str, Ed25519Key] = {}
        for jwk in raw_keys:
            if not isinstance(jwk, dict):
                raise KeyringError(f"each JWK must be a JSON object, got {type(jwk).__name__}")
            kty = jwk.get("kty")
            if kty not in ("OKP", "oct"):
                raise KeyringError(
                    f"unsupported key type {kty!r} — only oct (symmetric) and OKP (X25519, Ed25519) keys are used"
                )
            kid = jwk.get("kid")
            if not isinstance(kid, str) or not kid:
                raise KeyringError(f"every JWK needs a non-empty string kid, got {kid!r}")
            if kid in keys or kid in recipients or kid in signers:
                raise KeyringError(f"duplicate kid {kid!r} in the keyring")
            if "flenc_scopes" in jwk:
                scopes[kid] = _parse_scopes(kid, jwk["flenc_scopes"])
            if kty == "oct":
                keys[kid] = _decode_member(kid, jwk, "k")
                continue
            crv = jwk.get("crv")
            if crv not in ("Ed25519", "X25519"):
                raise KeyringError(f"JWK {kid!r} has unsupported curve {crv!r} — only X25519 and Ed25519 are used")
            public = _decode_member(kid, jwk, "x")
            private = _decode_member(kid, jwk, "d") if "d" in jwk else None
            if crv == "X25519":
                recipients[kid] = X25519Key(public=public, private=private)
            else:
                signers[kid] = Ed25519Key(public=public, private=private)
        if "primary" not in doc:
            raise KeyringError("keyring document has no top-level 'primary' member")
        return cls(
            keys=keys,
            primary=doc["primary"],
            primaries=_parse_scope_map(doc, "flenc_primaries"),
            recipients=recipients,
            scopes=scopes,
            signers=signers,
            signing=_parse_scope_map(doc, "flenc_signing"),
        )

    def key_for(self, kid: str) -> bytes:
        """Return the oct key bytes for `kid`, or raise `UnknownKeyError`.

        A recipient or signing kid is unknown here: this lookup serves `dir`
        tokens only.
        """
        try:
            return self.keys[kid]
        except KeyError:
            raise UnknownKeyError(kid) from None

    def recipient_for(self, kid: str) -> X25519Key:
        """Return the X25519 recipient key for `kid`, or raise `UnknownKeyError`."""
        try:
            return self.recipients[kid]
        except KeyError:
            raise UnknownKeyError(kid) from None

    def signer_for(self, kid: str) -> Ed25519Key:
        """Return the Ed25519 signing key for `kid`, or raise `UnknownKeyError`."""
        try:
            return self.signers[kid]
        except KeyError:
            raise UnknownKeyError(kid) from None

    def primary_pair(self) -> tuple[str, bytes]:
        """Return `(primary_kid, primary_key_bytes)` — what `dir` encryption stamps and uses.

        Raises `KeyringError` when the primary is a recipient key, which has no
        symmetric bytes to hand out.
        """
        if self.primary in self.recipients:
            raise KeyringError(f"primary {self.primary!r} is a recipient key, not a symmetric one")
        return self.primary, self.keys[self.primary]

    def sealing_kid(self, scope: str) -> str:
        """The kid that seals a value of `scope`: `primaries[scope]`, else `primary`.

        Raises `KeyringError` when that kid is bound and `scope` is outside its
        binding — an unscoped value (`scope == ""`) is outside every binding.
        """
        kid = self.primaries.get(scope, self.primary)
        bound = self.scopes.get(kid)
        if bound is not None and scope not in bound:
            what = f"scope {scope!r}" if scope else "an unscoped value"
            raise KeyringError(f"key {kid!r} is bound to scopes {sorted(bound)} and cannot seal {what}")
        return kid

    def signing_kid(self, scope: str) -> str:
        """The kid that signs a value of `scope`, or raise `KeyringError` when none is configured."""
        try:
            return self.signing[scope]
        except KeyError:
            raise KeyringError(f"no signing key for scope {scope!r} in the active keyring") from None

    def kids(self) -> list[str]:
        """Loaded key ids of every kind, sorted — the labels for the startup keyring gauge."""
        return sorted([*self.keys, *self.recipients, *self.signers])


def _decode_member(kid: str, jwk: dict, member: str) -> bytes:
    """Decode JWK member `member` of key `kid`, reporting absence or bad base64url as `KeyringError`."""
    if member not in jwk:
        raise KeyringError(f"JWK {kid!r} has no {member!r} (key material)")
    try:
        return _decode_key(jwk[member], member)
    except (TypeError, ValueError) as e:  # binascii.Error ⊂ ValueError
        raise KeyringError(f"JWK {kid!r} has malformed base64url {member!r}: {e}") from e


def _parse_scopes(kid: str, raw: object) -> frozenset[str]:
    """Parse a JWK's `flenc_scopes` member: a non-empty array of non-empty strings."""
    if not isinstance(raw, list) or not raw or not all(isinstance(scope, str) and scope for scope in raw):
        raise KeyringError(f"JWK {kid!r} 'flenc_scopes' must be a non-empty array of non-empty strings")
    return frozenset(raw)


def _parse_scope_map(doc: dict, member: str) -> dict[str, str]:
    """Parse an optional top-level scope → kid object (`flenc_primaries`, `flenc_signing`)."""
    raw = doc.get(member, {})
    if not isinstance(raw, dict) or not all(isinstance(kid, str) for kid in raw.values()):
        raise KeyringError(f"keyring {member!r} must be a JSON object of scope to kid strings")
    return dict(raw)


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
        log.info("Installed keyring: %d key(s), primary %r", len(keyring.kids()), keyring.primary)
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
