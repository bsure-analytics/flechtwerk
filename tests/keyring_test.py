"""Tests for the Keyring value object and the process-global secret runtime."""
import base64
import json

import pytest

from flechtwerk.keyring import (
    Keyring,
    KeyringConflictError,
    KeyringError,
    KeyringNotInstalledError,
    UnknownKeyError,
    active_keyring,
    current_keyring,
    install_keyring,
)

KEY_A = bytes(range(32))
KEY_B = bytes(range(32, 64))

# The autouse `_clean_secret_runtime` fixture (tests/conftest.py) isolates the
# process-global keyring/observer per test.


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
