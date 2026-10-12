import base64
import json

import pytest
from joserfc import jwe
from joserfc.jwk import OctKey

from flechtctl import Keyring, KeyringError, decrypt_keyring, encrypt_keyring, is_encrypted_keyring

PASSWORD = "correct horse battery staple"
KEYRING = Keyring.of({"k1": bytes(range(32))}, primary="k1", scopes={"k1": {"demo"}})


def header_of(document: str) -> dict:
    segment = document.split(".")[0]
    return json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))


def test_round_trip():
    assert decrypt_keyring(encrypt_keyring(KEYRING, PASSWORD), PASSWORD) == KEYRING


def test_the_protected_header_is_exact():
    header = header_of(encrypt_keyring(KEYRING, PASSWORD))
    assert sorted(header) == ["alg", "cty", "enc", "p2c", "p2s"]
    assert {key: header[key] for key in ("alg", "cty", "enc", "p2c")} == {
        "alg": "PBES2-HS512+A256KW", "cty": "jwk-set+json", "enc": "A256GCM", "p2c": 210000,
    }


def test_every_call_draws_a_fresh_salt():
    first, second = encrypt_keyring(KEYRING, PASSWORD), encrypt_keyring(KEYRING, PASSWORD)
    assert header_of(first)["p2s"] != header_of(second)["p2s"]
    assert first != second


def test_a_wrong_password_is_a_keyring_error():
    with pytest.raises(KeyringError, match="wrong password or damaged file"):
        decrypt_keyring(encrypt_keyring(KEYRING, PASSWORD), "another long password!")


def test_a_short_password_is_refused_on_encrypt():
    with pytest.raises(KeyringError, match="at least 14 bytes"):
        encrypt_keyring(KEYRING, "short")


def test_a_short_password_cannot_open_anything():
    with pytest.raises(KeyringError, match="wrong password or damaged file"):
        decrypt_keyring(encrypt_keyring(KEYRING, PASSWORD), "short")


def test_the_password_length_counts_utf8_bytes():
    assert decrypt_keyring(encrypt_keyring(KEYRING, "äöüäöüä"), "äöüäöüä") == KEYRING  # 7 chars, 14 bytes


def test_another_algorithm_is_rejected():
    registry = jwe.JWERegistry(algorithms=["PBES2-HS256+A128KW", "A256GCM"])
    header = {"alg": "PBES2-HS256+A128KW", "cty": "jwk-set+json", "enc": "A256GCM"}
    document = jwe.encrypt_compact(header, KEYRING.to_json().encode(), OctKey.import_key(PASSWORD.encode()), registry=registry)
    assert is_encrypted_keyring(document)
    with pytest.raises(KeyringError, match="wrong password or damaged file"):
        decrypt_keyring(document, PASSWORD)


def test_garbage_is_a_keyring_error():
    with pytest.raises(KeyringError, match="wrong password or damaged file"):
        decrypt_keyring("not a document", PASSWORD)


def test_trailing_whitespace_is_tolerated():
    assert decrypt_keyring(encrypt_keyring(KEYRING, PASSWORD) + "\n", PASSWORD) == KEYRING


def test_is_encrypted_keyring():
    assert is_encrypted_keyring(encrypt_keyring(KEYRING, PASSWORD))
    assert is_encrypted_keyring(encrypt_keyring(KEYRING, PASSWORD) + "\n")
    assert not is_encrypted_keyring(KEYRING.to_json())
    assert not is_encrypted_keyring("")
    assert not is_encrypted_keyring("a.b.c.d.e")
    assert not is_encrypted_keyring("flenc:jwe:eyJhbGciOiJkaXIifQ..x.y.z")
    other = jwe.encrypt_compact(
        {"alg": "dir", "enc": "A256GCM"}, b"{}", OctKey.import_key(bytes(range(32))),
        registry=jwe.JWERegistry(algorithms=["dir", "A256GCM"]),
    )
    assert not is_encrypted_keyring(other)  # a five-segment JWE without the keyring cty
    assert not is_encrypted_keyring(None)  # type: ignore[arg-type]
