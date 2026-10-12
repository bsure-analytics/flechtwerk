import json

import pytest

from flechtctl import (
    Ed25519Key,
    Keyring,
    KeyringError,
    UnknownKeyError,
    X25519Key,
    generate_ed25519_key,
    generate_x25519_key,
    keyring_from_jwk,
)

OCT = bytes(range(32))


def all_kinds() -> Keyring:
    return Keyring.of(
        {"k1": OCT, "k2": bytes(reversed(range(32)))},
        primary="k1",
        primaries={"demo": "r1"},
        recipients={"r1": X25519Key(public=bytes(range(1, 33)), private=bytes(range(2, 34)))},
        scopes={"k2": {"a", "b"}, "r1": {"demo"}, "s1": {"demo"}},
        signers={"s1": Ed25519Key(public=bytes(range(3, 35)), private=bytes(range(4, 36)))},
        signing={"demo": "s1"},
    )


def recipient_keyring() -> Keyring:
    return Keyring.of(
        {}, primary="r1", recipients={"r1": X25519Key(public=bytes(range(1, 33)), private=bytes(range(2, 34)))},
        scopes={"r1": {"demo"}},
    )


# --- validation, reached through the constructors ---


def test_keyring_needs_one_sealing_key():
    with pytest.raises(KeyringError, match="at least one key"):
        Keyring.of({}, primary="x")


def test_wrong_length_oct_key_is_rejected():
    with pytest.raises(KeyringError, match="expected 32"):
        Keyring.of({"k": b"short"}, primary="k")


def test_primary_must_be_held():
    with pytest.raises(KeyringError, match="primary 'nope'"):
        Keyring.of({"k": OCT}, primary="nope")


def test_public_key_material_must_be_bound():
    with pytest.raises(KeyringError, match="must be bound"):
        Keyring.of({"k": OCT}, primary="k", recipients={"r": X25519Key(public=OCT)})


def test_signing_needs_a_private_half():
    with pytest.raises(KeyringError, match="not a private signing key"):
        Keyring.of(
            {"k": OCT}, primary="k", scopes={"s": {"x"}}, signers={"s": Ed25519Key(public=OCT)}, signing={"x": "s"},
        )


def test_from_json_rejects_bad_documents():
    with pytest.raises(KeyringError, match="not valid JSON"):
        Keyring.from_json("{")
    with pytest.raises(KeyringError, match="must be a JSON object"):
        Keyring.from_json("[]")
    with pytest.raises(KeyringError, match="no top-level 'primary'"):
        Keyring.from_json('{"keys": []}')


# --- from_json messages after the _parse_jwk refactor ---


def jwk_doc(*jwks: dict, primary: str = "k") -> str:
    return json.dumps({"keys": list(jwks), "primary": primary})


K = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"  # bytes(range(32))


@pytest.mark.parametrize(("jwk", "message"), [
    ("nope", "each JWK must be a JSON object, got str"),
    ({"kty": "RSA", "kid": "k"}, "unsupported key type 'RSA'"),
    ({"kty": "oct", "kid": ""}, "needs a non-empty string kid"),
    ({"kty": "oct", "kid": "k", "flenc_scopes": []}, "'flenc_scopes' must be a non-empty array"),
    ({"kty": "oct", "kid": "k"}, "JWK 'k' has no 'k' \\(key material\\)"),
    ({"kty": "oct", "kid": "k", "k": "!!"}, "malformed base64url 'k'"),
    ({"kty": "OKP", "kid": "k", "crv": "P-256"}, "unsupported curve 'P-256'"),
])
def test_from_json_error_messages_are_unchanged(jwk, message):
    with pytest.raises(KeyringError, match=message):
        Keyring.from_json(jwk_doc(jwk))


def test_duplicate_kid_with_malformed_material_reports_the_duplicate():
    good = {"kty": "oct", "kid": "k", "k": K}
    bad = {"kty": "oct", "kid": "k", "k": "!!"}
    with pytest.raises(KeyringError, match="duplicate kid 'k'"):
        Keyring.from_json(jwk_doc(good, bad))


# --- to_json ---


@pytest.mark.parametrize("keyring", [
    Keyring.of({"k": OCT}, primary="k"),
    recipient_keyring(),
    all_kinds(),
], ids=["oct", "recipient", "all-kinds"])
def test_to_json_round_trips(keyring):
    assert Keyring.from_json(keyring.to_json()) == keyring


def test_to_json_is_sorted_stable_and_newline_terminated():
    text = all_kinds().to_json()
    assert text.endswith("}\n") and not text.endswith("\n\n")
    doc = json.loads(text)
    assert [jwk["kid"] for jwk in doc["keys"]] == ["k1", "k2", "r1", "s1"]
    assert doc["primary"] == "k1"
    assert doc["flenc_primaries"] == {"demo": "r1"}
    assert doc["flenc_signing"] == {"demo": "s1"}
    by_kid = {jwk["kid"]: jwk for jwk in doc["keys"]}
    assert "flenc_scopes" not in by_kid["k1"]
    assert by_kid["k2"]["flenc_scopes"] == ["a", "b"]
    assert sorted(by_kid["r1"]) == ["crv", "d", "flenc_scopes", "kid", "kty", "x"]
    assert text == all_kinds().to_json()


def test_to_json_omits_empty_optional_members():
    doc = json.loads(Keyring.of({"k": OCT}, primary="k").to_json())
    assert sorted(doc) == ["keys", "primary"]
    assert doc["keys"] == [{"k": K, "kid": "k", "kty": "oct"}]


# --- per-entry access ---


def test_public_jwk_never_carries_the_private_half():
    keyring = all_kinds()
    before = keyring.to_json()
    jwk = keyring.public_jwk("r1")
    assert sorted(jwk) == ["crv", "flenc_scopes", "kid", "kty", "x"]
    assert jwk["crv"] == "X25519" and jwk["flenc_scopes"] == ["demo"]
    assert keyring.public_jwk("s1")["crv"] == "Ed25519"
    assert keyring.to_json() == before


def test_public_jwk_unknown_and_oct_entries():
    keyring = all_kinds()
    with pytest.raises(UnknownKeyError) as unknown:
        keyring.public_jwk("nope")
    assert unknown.value.kid == "nope"
    with pytest.raises(KeyringError, match="key 'k1' is a symmetric \\(oct\\) key and has no public half"):
        keyring.public_jwk("k1")


def test_with_jwk_adds_an_entry_and_keeps_scopes():
    reader = recipient_keyring()
    signer = generate_ed25519_key()
    extended = Keyring.of(
        {}, primary="x", recipients={"x": generate_x25519_key()}, scopes={"x": {"demo"}, "sg": {"demo"}},
        signers={"sg": signer}, signing={"demo": "sg"},
    ).public_jwk("sg")
    updated = reader.with_jwk(extended)
    assert sorted(updated.kids()) == ["r1", "sg"]
    assert updated.signers["sg"] == Ed25519Key(public=signer.public)
    assert updated.scopes["sg"] == {"demo"}
    assert sorted(reader.kids()) == ["r1"]


def test_with_jwk_rejects_a_duplicate_kid():
    keyring = all_kinds()
    with pytest.raises(KeyringError, match="duplicate kid 'r1' in the keyring"):
        keyring.with_jwk(keyring.public_jwk("r1"))


def test_with_jwk_runs_the_keyring_validation():
    jwk = {"crv": "X25519", "kid": "r2", "kty": "OKP", "x": K}  # public-key material without scopes
    with pytest.raises(KeyringError, match="must be bound"):
        recipient_keyring().with_jwk(jwk)


def test_keyring_from_jwk_makes_a_recipient_primary():
    jwk = all_kinds().public_jwk("r1")
    keyring = keyring_from_jwk(jwk)
    assert keyring.primary == "r1"
    assert keyring.recipients["r1"].private is None
    assert keyring.scopes["r1"] == {"demo"}


def test_keyring_from_jwk_rejects_an_ed25519_entry():
    with pytest.raises(KeyringError):
        keyring_from_jwk(all_kinds().public_jwk("s1"))


def test_from_json_on_a_protected_document_says_so():
    from flechtctl import encrypt_keyring

    document = encrypt_keyring(all_kinds(), "correct horse battery staple")
    with pytest.raises(KeyringError, match="password-protected"):
        Keyring.from_json(document)
