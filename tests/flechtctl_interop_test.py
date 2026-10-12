"""flechtwerk's codecs and flechtctl's functions read and write the same tokens.

`flechtwerk.keyring` / `flechtwerk.secrets` are thin layers over `flechtctl`: the
classes are the same objects, and `flechtctl.encrypt(k, v, scope=s)` is what
`ENCRYPTED(inner, scope=s)` writes for `v == inner.encode(x)`.
"""
import base64
import importlib.abc
import importlib.util
import subprocess
import sys
from pathlib import Path

import flechtctl
import flechtwerk.keyring
import flechtwerk.secrets
import pytest
from flechtwerk.attribute import Attribute, BYTES, RECORD, Record, STR
from flechtwerk.secrets import ENCRYPTED, SIGNED
from flechtwerk.testing import RecordingObserver, fixture_asymmetric_keyring, installed_keyring

SCOPE = "api_key"


@pytest.mark.parametrize("name", [
    "Ed25519Key", "Keyring", "KeyringError", "UnknownKeyError", "X25519Key",
])
def test_keyring_names_are_the_flechtctl_objects(name):
    assert getattr(flechtwerk.keyring, name) is getattr(flechtctl, name)


@pytest.mark.parametrize("name", [
    "PREFIX", "PlaintextSecretError", "SCHEME", "SIGNATURE_PREFIX", "SIGNATURE_SCHEME", "SecretDecryptError",
    "SecretError", "SecretFormatError", "SecretSignatureError", "UnsignedSecretError", "is_encrypted", "is_signed",
    "kid_of", "signer_of",
])
def test_secrets_names_are_the_flechtctl_objects(name):
    assert getattr(flechtwerk.secrets, name) is getattr(flechtctl, name)


def test_flechtwerk_secrets_reexports_the_keyring_classes():
    assert flechtwerk.secrets.Keyring is flechtctl.Keyring
    assert flechtwerk.secrets.Ed25519Key is flechtctl.Ed25519Key
    assert flechtwerk.secrets.X25519Key is flechtctl.X25519Key


# --- ENCRYPTED <-> flechtctl.encrypt / decrypt ---


@pytest.fixture
def keyring():
    with installed_keyring(fixture_asymmetric_keyring()) as k:
        yield k


def test_a_flechtctl_token_decodes_through_the_codec_and_back(keyring):
    attribute = Attribute("a", ENCRYPTED(STR, scope=SCOPE))
    record = Record({attribute: "hello"})
    assert flechtctl.decrypt(keyring, record.raw["a"], scope=SCOPE) == "hello"
    assert Record.wrap({"a": flechtctl.encrypt(keyring, "world", scope=SCOPE)})[attribute] == "world"


def test_bytes_travel_as_their_base64_string(keyring):
    attribute = Attribute("a", ENCRYPTED(BYTES, scope=SCOPE))
    record = Record({attribute: b"\x00\x01binary"})
    assert flechtctl.decrypt(keyring, record.raw["a"], scope=SCOPE) == base64.b64encode(b"\x00\x01binary").decode()
    token = flechtctl.encrypt(keyring, base64.b64encode(b"\xffraw").decode(), scope=SCOPE)
    assert Record.wrap({"a": token})[attribute] == b"\xffraw"


def test_records_travel_as_dicts(keyring):
    attribute = Attribute("a", ENCRYPTED(RECORD, scope=SCOPE))
    record = Record({attribute: Record.wrap({"x": 1, "y": ["z"]})})
    assert flechtctl.decrypt(keyring, record.raw["a"], scope=SCOPE) == {"x": 1, "y": ["z"]}
    token = flechtctl.encrypt(keyring, {"k": "v"}, scope=SCOPE)
    assert Record.wrap({"a": token})[attribute].raw == {"k": "v"}


# --- SIGNED <-> flechtctl.sign / verify ---


def test_signed_interoperates_both_ways(keyring):
    attribute = Attribute("a", SIGNED(STR, scope=SCOPE))
    record = Record({attribute: "vouched"})
    assert flechtctl.verify(keyring, record.raw["a"], scope=SCOPE) == "vouched"
    assert Record.wrap({"a": flechtctl.sign(keyring, "other", scope=SCOPE)})[attribute] == "other"


def test_sign_then_encrypt_composition_interoperates(keyring):
    attribute = Attribute("a", ENCRYPTED(SIGNED(STR, scope=SCOPE), scope=SCOPE))
    record = Record({attribute: "both"})
    inner = flechtctl.decrypt(keyring, record.raw["a"], scope=SCOPE)
    assert flechtctl.verify(keyring, inner, scope=SCOPE) == "both"
    token = flechtctl.encrypt(keyring, flechtctl.sign(keyring, "made by the CLI", scope=SCOPE), scope=SCOPE)
    assert Record.wrap({"a": token})[attribute] == "made by the CLI"


# --- telemetry stays in the framework ---


def test_codec_decode_still_fires_the_observer_events():
    rec = RecordingObserver()
    keyring = fixture_asymmetric_keyring()
    with installed_keyring(keyring, observer=rec):
        encrypted = Attribute("e", ENCRYPTED(STR, scope=SCOPE))
        signed = Attribute("s", SIGNED(STR, scope=SCOPE))
        Record.wrap({"e": flechtctl.encrypt(keyring, "v", scope=SCOPE)})[encrypted]
        Record.wrap({"s": flechtctl.sign(keyring, "v", scope=SCOPE)})[signed]
    assert ("secret_decrypted", SCOPE, "test-recipient") in rec.calls
    assert ("secret_verified", SCOPE, "test-signer") in rec.calls


def test_flechtctl_functions_fire_no_events():
    rec = RecordingObserver()
    keyring = fixture_asymmetric_keyring()
    with installed_keyring(keyring, observer=rec):
        flechtctl.decrypt(keyring, flechtctl.encrypt(keyring, "v", scope=SCOPE), scope=SCOPE)
        flechtctl.verify(keyring, flechtctl.sign(keyring, "v", scope=SCOPE), scope=SCOPE)
    assert rec.calls == []


# --- without flechtctl ---


def run_without_flechtctl(code: str) -> subprocess.CompletedProcess:
    prelude = "import sys; sys.modules['flechtctl'] = None\n"
    return subprocess.run([sys.executable, "-c", prelude + code], capture_output=True, text=True)


def test_the_framework_imports_without_flechtctl():
    result = run_without_flechtctl("import flechtwerk, flechtwerk.module, flechtwerk.testing")
    assert result.returncode == 0, result.stderr


def test_building_a_keyring_without_flechtctl_says_what_to_install():
    result = run_without_flechtctl(
        "import flechtwerk.keyring as k\n"
        "try:\n"
        "    k.Keyring.of({}, primary='x')\n"
        "except ModuleNotFoundError as e:\n"
        "    print(e)\n"
        "    print(e.name)\n"
    )
    assert result.returncode == 0, result.stderr
    message, name = result.stdout.splitlines()
    assert "flechtwerk[secrets]" in message and name == "flechtctl"


def test_importing_secrets_without_flechtctl_says_what_to_install():
    result = run_without_flechtctl(
        "try:\n"
        "    import flechtwerk.secrets\n"
        "except ModuleNotFoundError as e:\n"
        "    print(e)\n"
    )
    assert result.returncode == 0, result.stderr
    assert "flechtwerk[secrets]" in result.stdout


# --- the absent-flechtctl seams, executed in-process ---
#
# The subprocess tests above prove the behaviour; these load a private COPY of the
# module under another name (the real modules, and the class identities everything
# else relies on, stay untouched) so that coverage sees the fallback branches run.


class RefuseImport(importlib.abc.MetaPathFinder):
    """Fails an import of `blocked` with a ModuleNotFoundError that names `culprit`."""

    def __init__(self, blocked: str, culprit: str) -> None:
        self.blocked, self.culprit = blocked, culprit

    def find_spec(self, fullname, path, target=None):
        if fullname == self.blocked:
            raise ModuleNotFoundError(f"cannot import {fullname}", name=self.culprit)
        return None


def load_copy(monkeypatch, module: str, *, hide: tuple[str, ...], refuse: RefuseImport | None = None):
    for name in hide:
        monkeypatch.setitem(sys.modules, name, None)
    if refuse is not None:
        for name in ("flechtctl", "flechtctl.errors", "flechtctl.keyring", "flechtctl.tokens"):
            monkeypatch.delitem(sys.modules, name, raising=False)
        monkeypatch.setattr(sys, "meta_path", [refuse, *sys.meta_path])
    path = Path(flechtwerk.keyring.__file__).with_name(f"{module}.py")
    spec = importlib.util.spec_from_file_location(f"flechtwerk.{module}_probe", path)
    copy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(copy)
    return copy


def test_keyring_placeholders_stand_in_without_flechtctl(monkeypatch):
    copy = load_copy(monkeypatch, "keyring", hide=("flechtctl", "flechtctl.keyring"))
    assert copy.Keyring is not flechtctl.Keyring and copy.KEY_BYTES == 32
    for placeholder in (copy.Keyring, copy.Ed25519Key, copy.X25519Key):
        for build in (lambda: placeholder(), lambda: placeholder.of({}), lambda: placeholder.from_json("{}")):
            with pytest.raises(ModuleNotFoundError, match="flechtwerk\\[secrets\\]") as error:
                build()
            assert error.value.name == "flechtctl" and placeholder.__name__ in str(error.value)
    assert issubclass(copy.UnknownKeyError, copy.KeyringError)
    assert issubclass(copy.KeyringConflictError, copy.KeyringError)
    unknown = copy.UnknownKeyError("k1")
    assert unknown.kid == "k1" and "'k1'" in str(unknown)


def test_a_foreign_import_failure_in_keyring_is_not_swallowed(monkeypatch):
    with pytest.raises(ModuleNotFoundError) as error:
        load_copy(monkeypatch, "keyring", hide=(), refuse=RefuseImport("flechtctl", culprit="something_else"))
    assert error.value.name == "something_else"


def test_secrets_names_what_to_install_when_flechtctl_is_missing(monkeypatch):
    with pytest.raises(ModuleNotFoundError, match="flechtwerk\\[secrets\\]") as error:
        load_copy(monkeypatch, "secrets", hide=("flechtctl", "flechtctl.errors", "flechtctl.tokens"))
    assert error.value.name.startswith("flechtctl") and isinstance(error.value.__cause__, ModuleNotFoundError)


def test_a_foreign_import_failure_in_secrets_is_not_swallowed(monkeypatch):
    with pytest.raises(ModuleNotFoundError) as error:
        load_copy(monkeypatch, "secrets", hide=(), refuse=RefuseImport("flechtctl.errors", culprit="something_else"))
    assert error.value.name == "something_else" and "flechtwerk[secrets]" not in str(error.value)
