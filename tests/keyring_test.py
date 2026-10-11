"""Tests for the Keyring value object and the process-global secret runtime."""
import base64
import json

import pytest

from flechtwerk.keyring import (
    Ed25519Key,
    Keyring,
    KeyringConflictError,
    KeyringError,
    KeyringNotInstalledError,
    UnknownKeyError,
    X25519Key,
    active_keyring,
    current_keyring,
    install_keyring,
)

KEY_A = bytes(range(32))
KEY_B = bytes(range(32, 64))

# The autouse `_clean_secret_runtime` fixture (tests/conftest.py) isolates the
# secret runtime (process-global keyring, context-bound observer) per test.


# --- construction & validation ---


def test_of_builds_and_exposes_primary():
    kr = Keyring.of({"k1": KEY_A, "k2": KEY_B}, primary="k2")
    assert kr.primary_pair() == ("k2", KEY_B)
    assert kr.kids() == ["k1", "k2"]


def test_key_for_returns_named_key_and_raises_on_unknown():
    kr = Keyring.of({"k1": KEY_A}, primary="k1")
    assert kr.key_for("k1") == KEY_A
    with pytest.raises(UnknownKeyError) as ei:
        kr.key_for("nope")
    assert ei.value.kid == "nope"


def test_rejects_wrong_key_length():
    with pytest.raises(KeyringError):
        Keyring.of({"k1": b"too-short"}, primary="k1")


def test_rejects_primary_not_among_keys():
    with pytest.raises(KeyringError):
        Keyring.of({"k1": KEY_A}, primary="k2")


def test_rejects_empty_keyring():
    with pytest.raises(KeyringError):
        Keyring.of({}, primary="k1")


def test_rejects_empty_kid():
    with pytest.raises(KeyringError):
        Keyring.of({"": KEY_A}, primary="")


# --- JWK Set parsing ---


def test_from_json_round_trips_a_jwk_set():
    doc = {
        "keys": [
            {"kty": "oct", "kid": "prod-2026-07", "k": base64.urlsafe_b64encode(KEY_A).rstrip(b"=").decode()},
            {"kty": "oct", "kid": "prod-2025-01", "k": base64.urlsafe_b64encode(KEY_B).rstrip(b"=").decode()},
        ],
        "primary": "prod-2026-07",
    }
    kr = Keyring.from_json(json.dumps(doc))
    assert kr.primary_pair() == ("prod-2026-07", KEY_A)
    assert kr.key_for("prod-2025-01") == KEY_B


def test_from_json_ignores_unknown_members():
    doc = {
        "keys": [{"kty": "oct", "kid": "k1", "k": base64.urlsafe_b64encode(KEY_A).rstrip(b"=").decode(), "alg": "A256GCM"}],
        "primary": "k1",
        "some_future_field": "ignored",
    }
    kr = Keyring.from_json(json.dumps(doc))
    assert kr.kids() == ["k1"]


def test_from_json_rejects_non_oct_key():
    doc = {"keys": [{"kty": "RSA", "kid": "k1"}], "primary": "k1"}
    with pytest.raises(KeyringError):
        Keyring.from_json(json.dumps(doc))


_GOOD_KEY = base64.urlsafe_b64encode(KEY_A).rstrip(b"=").decode()


@pytest.mark.parametrize("text", [
    "{ not json",                                                                    # invalid JSON
    "[]",                                                                            # not an object
    json.dumps({"keys": {"k1": "x"}, "primary": "k1"}),                              # keys not an array
    json.dumps({"keys": [{"kty": "oct", "kid": "k1", "k": _GOOD_KEY}]}),             # no primary
    json.dumps({"keys": [{"kty": "oct", "kid": "k1"}], "primary": "k1"}),            # JWK missing k
    json.dumps({"keys": [{"kty": "oct", "kid": "k1", "k": "!!not-base64!!"}], "primary": "k1"}),  # bad base64
    json.dumps({"keys": [{"kty": "oct", "kid": "k1", "k": _GOOD_KEY},
                         {"kty": "oct", "kid": "k1", "k": _GOOD_KEY}], "primary": "k1"}),  # duplicate kid
    json.dumps({"keys": [{"kty": "oct", "kid": 5, "k": _GOOD_KEY}], "primary": "k1"}),  # non-string kid
    json.dumps({"keys": ["not-an-object"], "primary": "k1"}),                        # JWK not a JSON object
    json.dumps({"keys": [{"kty": "oct", "kid": "k1", "k": 123}], "primary": "k1"}),  # non-string k
])
def test_from_json_wraps_malformed_documents_as_keyring_error(text):
    """Ops-authored files are the expected error case — all surface as KeyringError, not raw tracebacks."""
    with pytest.raises(KeyringError):
        Keyring.from_json(text)


# --- install contract ---


def test_active_keyring_raises_when_none_installed():
    assert current_keyring() is None
    with pytest.raises(KeyringNotInstalledError):
        active_keyring()


def test_install_then_active_returns_it():
    kr = Keyring.of({"k1": KEY_A}, primary="k1")
    install_keyring(kr)
    assert active_keyring() is kr
    assert current_keyring() is kr


def test_install_identical_material_is_idempotent():
    install_keyring(Keyring.of({"k1": KEY_A}, primary="k1"))
    # A distinct-but-equal keyring value must not raise.
    install_keyring(Keyring.of({"k1": KEY_A}, primary="k1"))
    assert active_keyring().primary == "k1"


def test_install_conflicting_material_raises():
    install_keyring(Keyring.of({"k1": KEY_A}, primary="k1"))
    with pytest.raises(KeyringConflictError):
        install_keyring(Keyring.of({"k1": KEY_B}, primary="k1"))


# --- asymmetric keys and scope bindings ---


def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


PUB_R = bytes(range(64, 96))
PRIV_R = bytes(range(96, 128))
PUB_S = bytes(range(128, 160))
PRIV_S = bytes(range(160, 192))


def _asymmetric_keyring(**overrides) -> Keyring:
    kwargs = {
        "keys": {"prod": KEY_A},
        "primaries": {"face_vector": "livia"},
        "primary": "prod",
        "recipients": {"livia": X25519Key(public=PUB_R)},
        "scopes": {"box-17": {"face_vector"}, "livia": {"face_vector"}},
        "signers": {"box-17": Ed25519Key(public=PUB_S, private=PRIV_S)},
        "signing": {"face_vector": "box-17"},
    } | overrides
    return Keyring.of(kwargs.pop("keys"), **kwargs)


def _asymmetric_doc() -> dict:
    return {
        "flenc_primaries": {"face_vector": "livia"},
        "flenc_signing": {"face_vector": "box-17"},
        "keys": [
            {"k": _b64(KEY_A), "kid": "prod", "kty": "oct"},
            {"crv": "X25519", "flenc_scopes": ["face_vector"], "kid": "livia", "kty": "OKP", "x": _b64(PUB_R)},
            {"crv": "Ed25519", "d": _b64(PRIV_S), "flenc_scopes": ["face_vector"], "kid": "box-17", "kty": "OKP",
             "x": _b64(PUB_S)},
        ],
        "primary": "prod",
    }


def test_asymmetric_keyring_builds_via_of_and_from_json_equally():
    kr = _asymmetric_keyring()
    assert Keyring.from_json(json.dumps(_asymmetric_doc())) == kr
    assert kr.scopes == {"box-17": frozenset({"face_vector"}), "livia": frozenset({"face_vector"})}


def test_asymmetric_lookups():
    kr = _asymmetric_keyring()
    assert kr.kids() == ["box-17", "livia", "prod"]
    assert kr.sealing_kid("face_vector") == "livia"
    assert kr.sealing_kid("api_key") == "prod"
    assert kr.sealing_kid("") == "prod"
    assert kr.signing_kid("face_vector") == "box-17"
    assert kr.recipient_for("livia") == X25519Key(public=PUB_R)
    assert kr.signer_for("box-17").private == PRIV_S
    with pytest.raises(UnknownKeyError):
        kr.recipient_for("prod")
    with pytest.raises(UnknownKeyError):
        kr.signer_for("livia")
    with pytest.raises(UnknownKeyError):
        kr.key_for("livia")
    with pytest.raises(KeyringError, match="no signing key"):
        kr.signing_kid("api_key")


def test_asymmetric_private_halves_stay_out_of_repr():
    assert "private" not in repr(X25519Key(public=PUB_R, private=PRIV_R))
    assert "private" not in repr(Ed25519Key(public=PUB_S, private=PRIV_S))


def test_asymmetric_seal_only_keyring_builds_and_has_no_primary_pair():
    kr = Keyring.of({}, primary="livia", recipients={"livia": X25519Key(public=PUB_R)},
                    scopes={"livia": {"face_vector"}})
    assert kr.sealing_kid("face_vector") == "livia"
    with pytest.raises(KeyringError, match="recipient key"):
        kr.primary_pair()


@pytest.mark.parametrize("overrides", [
    # 1. no oct or recipient key at all
    {"keys": {}, "primary": "box-17", "recipients": {}, "primaries": {}},
    # 2. empty kid / kid held as two kinds
    {"recipients": {"": X25519Key(public=PUB_R)}, "scopes": {"": {"x"}, "box-17": {"face_vector"}}, "primaries": {}},
    {"recipients": {"prod": X25519Key(public=PUB_R)}, "scopes": {"prod": {"x"}, "box-17": {"face_vector"}},
     "primaries": {}},
    # 3. wrong public / private lengths
    {"recipients": {"livia": X25519Key(public=b"short")}},
    {"recipients": {"livia": X25519Key(public=PUB_R, private=b"short")}},
    {"signers": {"box-17": Ed25519Key(public=PUB_S, private=b"short")}},
    # 4. a signing kid as primary
    {"primary": "box-17"},
    # 5. scopes for an unheld kid, empty binding, empty scope name, string binding
    {"scopes": {"box-17": {"face_vector"}, "livia": {"face_vector"}, "ghost": {"x"}}},
    {"scopes": {"box-17": {"face_vector"}, "livia": set()}},
    {"scopes": {"box-17": {"face_vector"}, "livia": {""}}},
    {"scopes": {"box-17": {"face_vector"}, "livia": "face_vector"}},
    # 6. unbound recipient / unbound signer
    {"scopes": {"box-17": {"face_vector"}}},
    {"scopes": {"livia": {"face_vector"}}, "signing": {}},
    # 7. primaries: empty scope, non-sealing kid, scope outside the binding
    {"primaries": {"": "livia"}},
    {"primaries": {"face_vector": "box-17"}},
    {"primaries": {"other": "livia"}},
    # 8. signing: empty scope, verify-only signer, non-signer, scope outside the binding
    {"signing": {"": "box-17"}},
    {"signers": {"box-17": Ed25519Key(public=PUB_S)}},
    {"signing": {"face_vector": "livia"}},
    {"signing": {"other": "box-17"}},
])
def test_asymmetric_validation_rules_reject(overrides):
    with pytest.raises(KeyringError):
        _asymmetric_keyring(**overrides)


def test_asymmetric_sealing_kid_refuses_values_outside_the_binding():
    kr = Keyring.of({"prod": KEY_A}, primary="livia", recipients={"livia": X25519Key(public=PUB_R)},
                    scopes={"livia": {"face_vector"}})
    with pytest.raises(KeyringError, match="an unscoped value"):
        kr.sealing_kid("")
    with pytest.raises(KeyringError, match="'api_key'"):
        kr.sealing_kid("api_key")


def test_asymmetric_bound_oct_key_loads_from_json_and_binds():
    doc = {"keys": [{"flenc_scopes": ["api_key"], "k": _b64(KEY_A), "kid": "prod", "kty": "oct"}], "primary": "prod"}
    kr = Keyring.from_json(json.dumps(doc))
    assert kr.scopes == {"prod": frozenset({"api_key"})}
    assert kr.sealing_kid("api_key") == "prod"
    with pytest.raises(KeyringError):
        kr.sealing_kid("")


def _mutated_doc(mutate) -> str:
    doc = _asymmetric_doc()
    mutate(doc)
    return json.dumps(doc)


@pytest.mark.parametrize("mutate", [
    lambda d: d["keys"][1].update(crv="X448"),                     # unknown curve
    lambda d: d["keys"][1].pop("x"),                               # missing x
    lambda d: d["keys"][1].update(x="!!not-base64!!"),             # malformed x
    lambda d: d["keys"][2].update(d="!!not-base64!!"),             # malformed d
    lambda d: d["keys"][2].update(d=17),                           # non-string d
    lambda d: d["keys"][1].update(flenc_scopes="face_vector"),     # flenc_scopes not a list
    lambda d: d["keys"][1].update(flenc_scopes=[]),                # empty flenc_scopes
    lambda d: d["keys"][1].update(flenc_scopes=[7]),               # non-string scope
    lambda d: d["keys"][1].update(flenc_scopes=[""]),              # empty scope name
    lambda d: d["keys"][1].pop("flenc_scopes"),                    # unbound OKP key
    lambda d: d["keys"][2].update(kid="livia"),                    # duplicate kid across kinds
    lambda d: d.update(flenc_primaries=["face_vector"]),           # flenc_primaries not an object
    lambda d: d.update(flenc_signing={"face_vector": 5}),          # flenc_signing value not a string
])
def test_asymmetric_from_json_rejects_malformed_okp_documents(mutate):
    with pytest.raises(KeyringError):
        Keyring.from_json(_mutated_doc(mutate))


def test_asymmetric_install_of_equal_keyrings_is_idempotent():
    install_keyring(_asymmetric_keyring())
    install_keyring(Keyring.from_json(json.dumps(_asymmetric_doc())))
    assert active_keyring().kids() == ["box-17", "livia", "prod"]
    with pytest.raises(KeyringConflictError):
        install_keyring(_asymmetric_keyring(signing={}))
