"""The `Keyring` value object — pure key material, no cryptography, no joserfc.

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

The module holds no process-global state: the crypto functions in
`flechtctl.tokens` take the keyring explicitly. A keyring file can also be
password-protected (an encrypted JWK Set); `is_encrypted_keyring` recognizes
one, and `flechtctl.tokens.decrypt_keyring` opens it.
"""
import base64
import json
from collections.abc import Mapping, Set
from dataclasses import dataclass, field, replace
from typing import Any

__all__ = [
    "Ed25519Key",
    "KEY_BYTES",
    "Keyring",
    "KeyringError",
    "UnknownKeyError",
    "X25519Key",
    "is_encrypted_keyring",
    "keyring_from_jwk",
]

KEY_BYTES = 32
"""AES-256 key length — the only key size this framework mints or accepts.

X25519 and Ed25519 key halves happen to be 32 bytes too.
"""


class KeyringError(Exception):
    """Base class for keyring problems (construction, install, key lookup)."""


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


def _encode_key(raw: bytes) -> str:
    """Encode raw key bytes as an unpadded base64url JWK member."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


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
        scopes: Mapping[str, Set[str]] | None = None,
        signers: dict[str, Ed25519Key] | None = None,
        signing: dict[str, str] | None = None,
    ) -> "Keyring":
        """Build a keyring from raw key bytes — the programmatic / test entry point."""
        return cls(
            keys=dict(keys),
            primary=primary,
            primaries=dict(primaries or {}),
            recipients=dict(recipients or {}),
            # `__post_init__` normalizes every bound to a frozenset.
            scopes=dict(scopes or {}),  # pyright: ignore[reportArgumentType]
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
        if is_encrypted_keyring(text):
            raise KeyringError(
                "keyring document is password-protected — open it with flechtctl (decrypt_keyring) "
                "or write a plain copy with flechtctl keyring unprotect"
            )
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
            kid, material, bound = _parse_jwk(jwk, held=frozenset([*keys, *recipients, *signers]))
            if bound is not None:
                scopes[kid] = bound
            if isinstance(material, X25519Key):
                recipients[kid] = material
            elif isinstance(material, Ed25519Key):
                signers[kid] = material
            else:
                keys[kid] = material
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

    def to_json(self) -> str:
        """The inverse of `from_json`: this keyring as an RFC 7517 JWK Set document.

        Keys sorted by `kid`, members sorted, two-space indent, newline-terminated;
        `Keyring.from_json(k.to_json()) == k` for every valid keyring. The private
        halves are included — this is the file form, not a shareable one (see `public_jwk`).
        """
        jwks: list[dict[str, Any]] = [
            {"k": _encode_key(key), "kid": kid, "kty": "oct"} for kid, key in self.keys.items()
        ] + [
            _okp_jwk("X25519", kid, key) for kid, key in self.recipients.items()
        ] + [
            _okp_jwk("Ed25519", kid, key) for kid, key in self.signers.items()
        ]
        for jwk in jwks:
            if jwk["kid"] in self.scopes:
                jwk["flenc_scopes"] = sorted(self.scopes[jwk["kid"]])
        doc: dict = {"keys": sorted(jwks, key=lambda jwk: jwk["kid"]), "primary": self.primary}
        if self.primaries:
            doc["flenc_primaries"] = dict(self.primaries)
        if self.signing:
            doc["flenc_signing"] = dict(self.signing)
        return json.dumps(doc, indent=2, sort_keys=True) + "\n"

    def public_jwk(self, kid: str) -> dict:
        """The public half of entry `kid` as one JWK — never the private half.

        What `flechtctl keyring export` prints: hand it to the party that should be
        able to seal (an X25519 recipient) or to check signatures (an Ed25519
        signer). The keyring itself is unchanged. An oct entry has no public half.
        """
        if kid in self.keys:
            raise KeyringError(f"key {kid!r} is a symmetric (oct) key and has no public half")
        if kid in self.recipients:
            jwk = _okp_jwk("X25519", kid, X25519Key(public=self.recipients[kid].public))
        elif kid in self.signers:
            jwk = _okp_jwk("Ed25519", kid, Ed25519Key(public=self.signers[kid].public))
        else:
            raise UnknownKeyError(kid)
        jwk["flenc_scopes"] = sorted(self.scopes[kid])
        return jwk

    def with_jwk(self, jwk: dict) -> "Keyring":
        """A new keyring with one more entry, parsed from a single JWK.

        The private half is kept if the JWK carries one, and so are its scopes.
        A kid already held raises `KeyringError`; the constructor's validation runs.
        """
        kid, material, bound = _parse_jwk(jwk, held=frozenset(self.kids()))
        return replace(
            self,
            keys={**self.keys, kid: material} if isinstance(material, bytes) else self.keys,
            recipients={**self.recipients, kid: material} if isinstance(material, X25519Key) else self.recipients,
            scopes={**self.scopes, kid: bound} if bound is not None else self.scopes,
            signers={**self.signers, kid: material} if isinstance(material, Ed25519Key) else self.signers,
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


def is_encrypted_keyring(text: str) -> bool:
    """Whether `text` is a password-protected keyring: a compact JWE whose `cty` is `jwk-set+json`.

    Never raises — anything else (a plain JWK Set, garbage, an ordinary token) is `False`.
    """
    try:
        segments = text.strip().split(".")
        if len(segments) != 5:
            return False
        header = json.loads(_decode_key(segments[0], "header"))
        return isinstance(header, dict) and header.get("cty") == "jwk-set+json"
    except (AttributeError, TypeError, ValueError):  # JSONDecodeError and binascii.Error ⊂ ValueError
        return False


def keyring_from_jwk(jwk: dict) -> Keyring:
    """A one-entry keyring whose `primary` is that entry — a new keyring file made from an imported JWK.

    An Ed25519 entry cannot be primary, so the constructor's own validation rejects it.
    """
    kid, material, bound = _parse_jwk(jwk, held=frozenset())
    return Keyring(
        keys={kid: material} if isinstance(material, bytes) else {},
        primary=kid,
        recipients={kid: material} if isinstance(material, X25519Key) else {},
        scopes={kid: bound} if bound is not None else {},
        signers={kid: material} if isinstance(material, Ed25519Key) else {},
    )


def _okp_jwk(crv: str, kid: str, key: X25519Key | Ed25519Key) -> dict:
    """One OKP JWK member dict: `d` only when the private half is present."""
    jwk = {"crv": crv, "kid": kid, "kty": "OKP", "x": _encode_key(key.public)}
    if key.private is not None:
        jwk["d"] = _encode_key(key.private)
    return jwk


def _parse_jwk(
    jwk: object, *, held: frozenset[str],
) -> tuple[str, bytes | X25519Key | Ed25519Key, frozenset[str] | None]:
    """Parse one JWK into `(kid, material, scopes)`; the material's type is the key's kind.

    Checks in order: object, `kty`, `kid`, a kid already in `held`, scopes, key
    material — so a duplicate kid with malformed material still reports the duplicate.
    """
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
    if kid in held:
        raise KeyringError(f"duplicate kid {kid!r} in the keyring")
    bound = _parse_scopes(kid, jwk["flenc_scopes"]) if "flenc_scopes" in jwk else None
    if kty == "oct":
        return kid, _decode_member(kid, jwk, "k"), bound
    crv = jwk.get("crv")
    if crv not in ("Ed25519", "X25519"):
        raise KeyringError(f"JWK {kid!r} has unsupported curve {crv!r} — only X25519 and Ed25519 are used")
    public = _decode_member(kid, jwk, "x")
    private = _decode_member(kid, jwk, "d") if "d" in jwk else None
    if crv == "X25519":
        return kid, X25519Key(public=public, private=private), bound
    return kid, Ed25519Key(public=public, private=private), bound


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
