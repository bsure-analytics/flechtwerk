"""The secret-token error family — joserfc-free, so a caller can catch them without the crypto."""

__all__ = [
    "PlaintextSecretError",
    "SecretDecryptError",
    "SecretError",
    "SecretFormatError",
    "SecretSignatureError",
    "UnsignedSecretError",
]


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
