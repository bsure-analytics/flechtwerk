"""Tests for flechtwerk.secrets — the ENCRYPTED codec, scope ratchet, and tooling.

The interop tests mint tokens with an *independent* AES-256-GCM compact-JWE
encoder built directly on pyca/cryptography (no joserfc), standing in for the
panva-jose (TypeScript) and Nimbus (Java) producers the design names: if our
reader accepts a token this repo did not produce with joserfc, the wire format
is genuinely cross-implementation, not a joserfc dialect.
"""
import base64
import json
import subprocess
import sys

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from flechtwerk.attribute import ANY, Attribute, DICT, LIST, RECORD, Record, STR
from flechtwerk.keyring import (
    Keyring,
    KeyringNotInstalledError,
)
from flechtwerk.secrets import (
    ENCRYPTED,
    PREFIX,
    PlaintextSecretError,
    SecretDecryptError,
    SecretError,
    SecretFormatError,
    encrypt_value,
    is_encrypted,
    kid_of,
    reencrypt,
    scan_config_topics,
)
from flechtwerk.testing import FakeKafkaConsumer, RecordingObserver, fixture_keyring, installed_keyring, make_record

KEY_A = bytes(range(32))
KEY_B = bytes(range(32, 64))
API_KEY = Attribute("api_key", ENCRYPTED(STR, scope="api_key"))
ADMIN_TOKEN = Attribute("admin_token", ENCRYPTED(STR, scope="admin_token"))
UNSCOPED = Attribute("plain", ENCRYPTED(STR))  # scope=""

# The autouse `_clean_secret_runtime` fixture (tests/conftest.py) isolates the
# process-global keyring/observer per test.


def _independent_token(header: dict, plaintext: bytes, key: bytes) -> str:
    """A `flenc:jwe:` token built without joserfc — the cross-language oracle."""
    header_b64 = base64.urlsafe_b64encode(
        json.dumps(header, separators=(",", ":")).encode()).rstrip(b"=")
    import os
    iv = os.urandom(12)
    ct_tag = AESGCM(key).encrypt(iv, plaintext, header_b64)  # AAD = the ASCII protected header
    ct, tag = ct_tag[:-16], ct_tag[-16:]

    def b64(b: bytes) -> str:
        return base64.urlsafe_b64encode(b).rstrip(b"=").decode()

    return PREFIX + f"{header_b64.decode()}..{b64(iv)}.{b64(ct)}.{b64(tag)}"


# --- round trip & token shape ---


def test_round_trip_str():
    with installed_keyring():
        token = API_KEY.codec.encode("sk-live-123")
        assert token.startswith(PREFIX)
        assert API_KEY.codec.decode(token) == "sk-live-123"


def test_token_is_ciphertext_form_and_reports_kid():
    with installed_keyring():
        token = encrypt_value(API_KEY, "s")
    assert is_encrypted(token) is True
    assert is_encrypted("sk-live-plain") is False
    assert is_encrypted(42) is False
    assert kid_of(token) == "test-key"


def test_encoding_is_randomized():
    """Fresh nonce per encode — two tokens for the same value differ (the caveat)."""
    with installed_keyring():
        assert encrypt_value(API_KEY, "s") != encrypt_value(API_KEY, "s")


def test_round_trip_container_and_record():
    with installed_keyring():
        listed = Attribute("scopes", ENCRYPTED(LIST(STR)))
        token = listed.codec.encode(["a", "b"])
        assert listed.codec.decode(token) == ["a", "b"]

        nested = Attribute("creds", ENCRYPTED(RECORD))
        rec = Record.wrap({"user": "u", "pass": "p"})
        token = nested.codec.encode(rec)
        assert nested.codec.decode(token) == rec

        mapped = Attribute("headers", ENCRYPTED(DICT(ANY)))
        token = mapped.codec.encode({"h": 1})
        assert mapped.codec.decode(token) == {"h": 1}


def test_encrypted_composes_inside_a_container():
    """LIST(ENCRYPTED(STR)) is now legal — each element is its own token."""
    with installed_keyring():
        codec = LIST(ENCRYPTED(STR))
        token_list = codec.encode(["a", "b"])
        assert all(is_encrypted(t) for t in token_list)
        assert codec.decode(token_list) == ["a", "b"]


# --- scope ratchet ---


def test_scope_mismatch_rejects_relocation():
    with installed_keyring():
        stolen = encrypt_value(ADMIN_TOKEN, "sk-live-123")  # scoped "admin_token"
        with pytest.raises(SecretDecryptError) as ei:
            API_KEY.codec.decode(stolen)                    # read as scope "api_key"
        assert ei.value.scope == "api_key"
        assert ei.value.kid == "test-key"


def test_scoped_codec_accepts_unscoped_token_upgrade():
    """A scoped codec still reads an unscoped token — the ratchet's upgrade path."""
    with installed_keyring():
        unscoped = encrypt_value(UNSCOPED, "s")
        assert API_KEY.codec.decode(unscoped) == "s"


def test_unscoped_codec_rejects_scoped_token_downgrade():
    """An unscoped codec refuses a scoped token — a scope may not be silently dropped."""
    with installed_keyring():
        scoped = encrypt_value(API_KEY, "s")
        with pytest.raises(SecretDecryptError) as ei:
            UNSCOPED.codec.decode(scoped)
        assert "downgrade" in str(ei.value)


def test_unscoped_round_trip():
    with installed_keyring():
        assert UNSCOPED.codec.decode(encrypt_value(UNSCOPED, "s")) == "s"


def test_same_scope_decodes():
    with installed_keyring():
        assert ADMIN_TOKEN.codec.decode(encrypt_value(ADMIN_TOKEN, "x")) == "x"


# --- acceptance rule / allowlist ---


def test_foreign_enc_is_rejected_before_crypto():
    """A token using an enc outside the allowlist is rejected (not decrypted)."""
    with installed_keyring():
        token = _independent_token(
            {"alg": "dir", "enc": "A128GCM", "kid": "test-key", "flenc_scope": "api_key"},
            b'"s"', bytes(range(16)),
        )
        with pytest.raises(SecretDecryptError):
            API_KEY.codec.decode(token)


def test_unknown_envelope_segment_raises():
    with installed_keyring():
        with pytest.raises(SecretFormatError):
            API_KEY.codec.decode("flenc:cbor:whatever")


# --- failure semantics ---


def test_unknown_kid_raises_decrypt_error_with_kid():
    with installed_keyring(Keyring.of({"old": KEY_A}, primary="old")):
        token = encrypt_value(API_KEY, "s")
    with installed_keyring(Keyring.of({"new": KEY_B}, primary="new")):
        with pytest.raises(SecretDecryptError) as ei:
            API_KEY.codec.decode(token)
        assert ei.value.kid == "old"
        assert isinstance(ei.value.__cause__, Exception)


def test_tampered_ciphertext_raises_decrypt_error():
    with installed_keyring():
        token = encrypt_value(API_KEY, "s")
        tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
        with pytest.raises(SecretDecryptError):
            API_KEY.codec.decode(tampered)


def test_encode_without_keyring_raises():
    with pytest.raises(KeyringNotInstalledError):
        API_KEY.codec.encode("s")


def test_decode_without_keyring_raises():
    with pytest.raises(KeyringNotInstalledError):
        API_KEY.codec.decode("flenc:jwe:eyJ...")


# --- migration: read_plaintext ---


def test_plaintext_tolerated_when_read_plaintext_set():
    rec = RecordingObserver()
    migrating = Attribute("m", ENCRYPTED(STR, scope="m", read_plaintext=True))
    with installed_keyring(observer=rec):
        assert migrating.codec.decode("legacy-plaintext") == "legacy-plaintext"
    assert ("secret_plaintext_read", "m") in rec.calls


def test_plaintext_rejected_by_default():
    with installed_keyring():
        with pytest.raises(PlaintextSecretError) as ei:
            API_KEY.codec.decode("legacy-plaintext")
        assert ei.value.scope == "api_key"


def test_ciphertext_still_decodes_with_read_plaintext():
    migrating = Attribute("m", ENCRYPTED(STR, scope="m", read_plaintext=True))
    with installed_keyring():
        assert migrating.codec.decode(migrating.codec.encode("s")) == "s"


# --- observer events ---


def test_decrypt_emits_observer_event():
    rec = RecordingObserver()
    with installed_keyring(observer=rec):
        token = encrypt_value(API_KEY, "s")
        API_KEY.codec.decode(token)
    assert ("secret_decrypted", "api_key", "test-key") in rec.calls


# --- tooling primitives ---


def test_encrypt_value_rejects_non_encrypted_attribute():
    plain = Attribute("name", STR)
    with installed_keyring():
        with pytest.raises(SecretError):
            encrypt_value(plain, "x")


def test_kid_of_rejects_non_token():
    with pytest.raises(SecretFormatError):
        kid_of("not-a-token")


def test_kid_of_rejects_malformed_header():
    with pytest.raises(SecretFormatError):
        kid_of("flenc:jwe:@@not-base64@@..a.b.c")


def test_kid_of_rejects_header_without_kid():
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "dir", "enc": "A256GCM"}).encode()).rstrip(b"=").decode()
    with pytest.raises(SecretFormatError):
        kid_of(f"flenc:jwe:{header}..a.b.c")


def test_reencrypt_rotates_to_primary_and_still_decodes():
    with installed_keyring(Keyring.of({"old": KEY_A}, primary="old")):
        token = encrypt_value(API_KEY, "s")
        assert kid_of(token) == "old"
    with installed_keyring(Keyring.of({"old": KEY_A, "new": KEY_B}, primary="new")):
        assert API_KEY.codec.decode(token) == "s"        # old key still decrypts
        rotated = reencrypt(token, API_KEY)
        assert kid_of(rotated) == "new"                  # re-stamped under primary
        assert API_KEY.codec.decode(rotated) == "s"


def test_reencrypt_promotes_unscoped_to_scope_but_cannot_strip():
    with installed_keyring():
        unscoped = encrypt_value(UNSCOPED, "s")
        promoted = reencrypt(unscoped, API_KEY)          # upgrade: stamp scope="api_key"
        assert API_KEY.codec.decode(promoted) == "s"
        header = json.loads(base64.urlsafe_b64decode(
            promoted[len(PREFIX):].split(".")[0] + "=="))
        assert header["flenc_scope"] == "api_key"

        scoped = encrypt_value(API_KEY, "s")
        with pytest.raises(SecretDecryptError):           # cannot strip a scope
            reencrypt(scoped, UNSCOPED)


async def test_scan_config_topics_classifies_ciphertext_and_plaintext():
    with installed_keyring():
        enc = encrypt_value(API_KEY, "s")
    records = [
        make_record(topic="cfg", partition=0, offset=0, key=b"tenant-a",
                    value=json.dumps({"api_key": enc, "url": "u"}).encode()),
        make_record(topic="cfg", partition=0, offset=1, key=b"tenant-b",
                    value=json.dumps({"api_key": "still-plaintext"}).encode()),
        make_record(topic="cfg", partition=0, offset=2, key=b"tenant-c",
                    value=json.dumps({"url": "no-secret-here"}).encode()),
    ]
    consumer = FakeKafkaConsumer(records)
    entries = [e async for e in scan_config_topics(consumer, ["cfg"], [API_KEY])]
    by_key = {e.wire_key: e for e in entries}
    assert by_key["tenant-a"].kid == "test-key"
    assert by_key["tenant-b"].kid is None            # legacy plaintext still present
    assert "tenant-c" not in by_key                  # attribute absent


async def test_scan_raises_on_unknown_topic():
    """A missing topic must not read as a clean scan (false all-clear before key removal)."""
    consumer = FakeKafkaConsumer([])  # no records → no partitions for any topic
    with pytest.raises(SecretError):
        [e async for e in scan_config_topics(consumer, ["ghost-topic"], [API_KEY])]


def test_secret_observer_first_wins():
    from flechtwerk.keyring import active_observer, set_secret_observer
    first, second = RecordingObserver(), RecordingObserver()
    set_secret_observer(first)
    assert active_observer() is first
    set_secret_observer(second)                      # differs → keep first, warn
    assert active_observer() is first


# --- cross-language interop ---


def test_reads_token_minted_without_joserfc():
    """Independent AES-256-GCM encoder → our reader: proves format interop."""
    with installed_keyring():
        token = _independent_token(
            {"alg": "dir", "enc": "A256GCM", "kid": "test-key", "flenc_scope": "api_key"},
            b'"from-another-language"', fixture_keyring().key_for("test-key"),
        )
        assert API_KEY.codec.decode(token) == "from-another-language"


# A token minted by panva `jose` (npm) — the TypeScript producer the design
# names — over the fixture key (bytes(range(32)), kid "test-key"), scope
# "api_key", payload JSON.stringify("from-panva-jose"). Pinned so the interop
# guarantee is a regression test needing no node at run time. Regenerate with:
#   new CompactEncrypt(TextEncoder().encode(JSON.stringify(value)))
#     .setProtectedHeader({alg:'dir',enc:'A256GCM',kid:'test-key',flenc_scope:'api_key'})
#     .encrypt(Uint8Array.from({length:32},(_,i)=>i))
_PANVA_JOSE_TOKEN = (
    "flenc:jwe:eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIiwia2lkIjoidGVzdC1rZXkiLCJmbGVuY19zY29wZSI6ImFwaV9rZXkifQ"
    "..t3sQTnklWjnT__YL.acd-wnFvj_vPgEQkg4YeCe0.gguXKLgXvVLPHgaNy_gerw"
)


def test_reads_pinned_panva_jose_token():
    """A real panva-`jose` (TypeScript) token decodes with our reader."""
    with installed_keyring():
        assert API_KEY.codec.decode(_PANVA_JOSE_TOKEN) == "from-panva-jose"


def test_our_token_decodes_with_independent_reader():
    """Our token → an independent AES-256-GCM reader: the reverse direction."""
    with installed_keyring():
        token = encrypt_value(API_KEY, "round-trips-out")
    compact = token[len(PREFIX):]
    header_b64, _, iv_b64, ct_b64, tag_b64 = compact.split(".")

    def unb64(s: str) -> bytes:
        return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

    header = json.loads(unb64(header_b64))
    assert header == {"alg": "dir", "enc": "A256GCM", "kid": "test-key", "flenc_scope": "api_key"}
    plaintext = AESGCM(fixture_keyring().key_for("test-key")).decrypt(
        unb64(iv_b64), unb64(ct_b64) + unb64(tag_b64), header_b64.encode())
    assert json.loads(plaintext) == "round-trips-out"


# --- import isolation ---


def test_importing_flechtwerk_does_not_load_joserfc():
    """The base install must not drag in joserfc — the [secrets] extra seam."""
    code = "import flechtwerk.module, sys; assert 'joserfc' not in sys.modules, sorted(m for m in sys.modules if 'jose' in m)"
    subprocess.run([sys.executable, "-c", code], check=True)
