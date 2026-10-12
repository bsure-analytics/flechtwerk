import base64
from pathlib import Path

import pytest

from flechtctl import (
    Ed25519Key,
    Keyring,
    KeyringError,
    SecretDecryptError,
    SecretFormatError,
    SecretSignatureError,
    X25519Key,
    decrypt,
    decrypt_bytes,
    encrypt,
    encrypt_bytes,
    generate_ed25519_key,
    generate_oct_key,
    generate_x25519_key,
    is_encrypted,
    is_signed,
    kid_of,
    read_header,
    reencrypt,
    sign,
    sign_bytes,
    signer_of,
    verify,
    verify_bytes,
)

OCT = Keyring.of({"test-key": bytes(range(32))}, primary="test-key")

# The camera-inference compatibility vector. Provenance: livia-nt
# camera-inference/tests/fixtures/face_vector_token.txt, last changed by livia-nt
# commit 042da99 ("face_vector is sealed to one recipient key per environment, and a
# box holds only its public half"), sha256
# 16ee84502b81d4a931aaff83df9ededee868ddf5c521a97cbedd38e9ca4e738b. Copied byte for byte;
# if livia-nt changes its format again, this test is the alarm.
TOKEN = (Path(__file__).parent / "fixtures" / "face_vector_token.txt").read_text().strip()
RECIPIENT = Keyring.of(
    {},
    primary="test-recipient",
    recipients={"test-recipient": X25519Key(
        public=base64.urlsafe_b64decode("j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8="),
        private=bytes(range(32)),
    )},
    scopes={"test-recipient": {"face_vector"}},
)

SIGNING = Keyring.of(
    {"k": bytes(range(32))},
    primary="k",
    scopes={"sg": {"demo"}},
    signers={"sg": Ed25519Key(public=base64.urlsafe_b64decode("A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="), private=bytes(range(32)))},
    signing={"demo": "sg"},
)


def asymmetric() -> Keyring:
    return Keyring.of(
        {"k": bytes(range(32))},
        primary="r",
        recipients={"r": generate_x25519_key()},
        scopes={"r": {"demo"}, "sg": {"demo"}},
        signers={"sg": generate_ed25519_key()},
        signing={"demo": "sg"},
    )


# --- encrypt / decrypt ---


@pytest.mark.parametrize("value", ["s3cret", 42, None, {"b": [1, 2], "a": "x"}, ["ä"]])
def test_oct_round_trip(value):
    assert decrypt(OCT, encrypt(OCT, value)) == value


def test_recipient_round_trip():
    keyring = Keyring.of({}, primary="r", recipients={"r": generate_x25519_key()}, scopes={"r": {"demo"}})
    token = encrypt(keyring, {"a": 1}, scope="demo")
    assert token.startswith("flenc:jwe:") and is_encrypted(token) and kid_of(token) == "r"
    assert decrypt(keyring, token, scope="demo") == {"a": 1}


def test_a_recipient_key_bound_to_a_scope_cannot_seal_an_unscoped_value():
    keyring = Keyring.of({}, primary="r", recipients={"r": generate_x25519_key()}, scopes={"r": {"demo"}})
    with pytest.raises(KeyringError, match="an unscoped value"):
        encrypt(keyring, "v")


def test_a_seal_only_keyring_cannot_open():
    keyring = asymmetric()
    sealed = encrypt(keyring, "v", scope="demo")
    only_public = Keyring.of(
        {}, primary="r", recipients={"r": X25519Key(public=keyring.recipients["r"].public)}, scopes={"r": {"demo"}},
    )
    assert encrypt(only_public, "w", scope="demo").startswith("flenc:jwe:")
    with pytest.raises(SecretDecryptError, match="can seal but not open"):
        decrypt(only_public, sealed, scope="demo")


def test_scope_ratchet_both_directions():
    unscoped, scoped = encrypt(OCT, "v"), encrypt(OCT, "v", scope="demo")
    assert decrypt(OCT, unscoped, scope="demo") == "v"  # a scoped reader accepts an unscoped token
    with pytest.raises(SecretDecryptError, match="downgrade blocked"):
        decrypt(OCT, scoped)
    with pytest.raises(SecretDecryptError, match="scope mismatch"):
        decrypt(OCT, scoped, scope="other")


def test_a_bound_key_rejects_a_token_outside_its_binding():
    bound = Keyring.of({"test-key": bytes(range(32))}, primary="test-key", scopes={"test-key": {"demo"}})
    with pytest.raises(KeyringError, match="cannot seal"):
        encrypt(bound, "v", scope="other")
    foreign = encrypt(OCT, "v", scope="other")
    with pytest.raises(SecretDecryptError, match="is bound to scopes"):
        decrypt(bound, foreign, scope="other")


def test_an_unknown_kid_is_a_decrypt_error():
    other = Keyring.of({"other": bytes(range(32))}, primary="other")
    with pytest.raises(SecretDecryptError) as error:
        decrypt(other, encrypt(OCT, "v"))
    assert error.value.kid == "test-key"


def test_bytes_variants_keep_the_exact_payload():
    token = encrypt_bytes(OCT, b"\x00raw\xff", scope="demo")
    assert decrypt_bytes(OCT, token, scope="demo") == b"\x00raw\xff"


def test_reencrypt_moves_dir_to_recipient_keeping_the_payload_bytes():
    keyring = Keyring.of(
        {"test-key": bytes(range(32))}, primary="r", recipients={"r": generate_x25519_key()}, scopes={"r": {"demo"}},
    )
    original = encrypt_bytes(OCT, b'{"a":1}', scope="demo")
    moved = reencrypt(keyring, original, scope="demo")
    assert read_header(original)["alg"] == "dir" and read_header(moved)["alg"] == "ECDH-ES+A256KW"
    assert decrypt_bytes(keyring, moved, scope="demo") == b'{"a":1}'


def test_reencrypt_promotes_an_unscoped_token():
    promoted = reencrypt(OCT, encrypt(OCT, "v"), scope="demo")
    assert read_header(promoted)["flenc_scope"] == "demo"


# --- sign / verify ---


def test_sign_and_verify_round_trip():
    keyring = asymmetric()
    token = sign(keyring, {"a": 1}, scope="demo")
    assert token.startswith("flsig:jws:") and is_signed(token) and signer_of(token) == "sg"
    assert verify(keyring, token, scope="demo") == {"a": 1}
    assert verify_bytes(keyring, sign_bytes(keyring, b"raw", scope="demo"), scope="demo") == b"raw"


def test_verify_rejects_a_wrong_scope():
    token = sign(SIGNING, "v", scope="demo")
    with pytest.raises(SecretSignatureError, match="scope mismatch"):
        verify(SIGNING, token, scope="other")


def test_verify_rejects_an_unknown_signer():
    token = sign(SIGNING, "v", scope="demo")
    with pytest.raises(SecretSignatureError) as error:
        verify(asymmetric(), token, scope="demo")
    assert error.value.kid == "sg"


def test_verify_rejects_a_tampered_token():
    token = sign(SIGNING, "v", scope="demo")
    head, payload, signature = token.rsplit(".", 2)
    with pytest.raises(SecretSignatureError):
        verify(SIGNING, f"{head}.{payload[:-2]}AA.{signature}", scope="demo")


def test_sign_and_verify_need_a_scope():
    with pytest.raises(ValueError, match="sign needs a non-empty scope"):
        sign(SIGNING, "v", scope="")
    with pytest.raises(ValueError, match="verify needs a non-empty scope"):
        verify(SIGNING, "flsig:jws:x", scope="")


def test_sign_without_a_signing_key_for_the_scope():
    with pytest.raises(KeyringError, match="no signing key for scope 'other'"):
        sign(SIGNING, "v", scope="other")


def test_sign_then_encrypt_composes():
    keyring = asymmetric()
    token = encrypt(keyring, sign(keyring, "hello", scope="demo"), scope="demo")
    assert verify(keyring, decrypt(keyring, token, scope="demo"), scope="demo") == "hello"


# --- read_header ---


def test_read_header_of_both_envelopes_and_garbage():
    assert read_header(sign(SIGNING, "v", scope="demo")) == {"alg": "Ed25519", "flenc_scope": "demo", "kid": "sg"}
    with pytest.raises(SecretFormatError, match="not a flenc or flsig token"):
        read_header("plain")
    with pytest.raises(SecretFormatError, match="malformed JWE header"):
        read_header("flenc:jwe:@@@.x.y.z.w")
    with pytest.raises(SecretFormatError, match="unknown flenc envelope"):
        read_header("flenc:jwt:abc")


# --- generators ---


def test_generated_keys_are_valid_keyring_material():
    assert len(generate_oct_key()) == 32 and generate_oct_key() != generate_oct_key()
    x, ed = generate_x25519_key(), generate_ed25519_key()
    assert len(x.public) == len(x.private) == len(ed.public) == len(ed.private) == 32
    keyring = Keyring.of(
        {"k": generate_oct_key()}, primary="r", recipients={"r": x}, scopes={"r": {"s"}, "sg": {"s"}},
        signers={"sg": ed}, signing={"s": "sg"},
    )
    assert decrypt(keyring, encrypt(keyring, "v", scope="s"), scope="s") == "v"
    assert verify(keyring, sign(keyring, "v", scope="s"), scope="s") == "v"


# --- the camera-inference vector ---


def test_the_camera_inference_vector_decrypts():
    vector = decrypt(RECIPIENT, TOKEN, scope="face_vector")
    assert len(base64.b64decode(vector)) == 2048


def test_the_camera_inference_vector_header():
    header = read_header(TOKEN)
    assert sorted(header) == ["alg", "enc", "epk", "flenc_scope", "kid"]
    assert (header["alg"], header["enc"], header["kid"], header["flenc_scope"]) == (
        "ECDH-ES+A256KW", "A256GCM", "test-recipient", "face_vector",
    )


def test_a_fresh_recipient_token_has_the_same_header_shape():
    fresh = read_header(encrypt(RECIPIENT, "AAAA", scope="face_vector"))
    vector = read_header(TOKEN)
    assert sorted(fresh) == sorted(vector)
    assert {k: v for k, v in fresh.items() if k != "epk"} == {k: v for k, v in vector.items() if k != "epk"}
    assert fresh["epk"] != vector["epk"]


def test_a_fresh_dir_token_has_exactly_the_documented_header():
    assert read_header(encrypt(OCT, "AAAA", scope="face_vector")) == {
        "alg": "dir", "enc": "A256GCM", "flenc_scope": "face_vector", "kid": "test-key",
    }
