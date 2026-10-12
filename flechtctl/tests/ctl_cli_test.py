import getpass
import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from flechtctl import Keyring, is_encrypted_keyring, read_header
from flechtctl.cli import main

PASSWORD = "correct horse battery staple"

# The command sequence the guide docs/guides/flechtctl.md prints, step by step. Keep the two in sync.
WALKTHROUGH = [
    "keyring new reader.json --kid demo-recipient --type x25519 --scope demo",
    "keyring export reader.json demo-recipient -o recipient.jwk",
    "keyring import writer.json recipient.jwk --primary",
    "keyring add writer.json --kid demo-signer --type ed25519 --scope demo --signing-for demo",
    "keyring export writer.json demo-signer -o signer.jwk",
    "keyring import reader.json signer.jwk",
    "keyring show reader.json",
    "keyring show writer.json",
]


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.fixture
def run(monkeypatch, capsys):
    def run(*argv: str, stdin: str | None = None) -> tuple[int, str, str]:
        if stdin is not None:
            monkeypatch.setattr(sys, "stdin", io.StringIO(stdin))
        capsys.readouterr()
        try:
            code = main(list(argv))
        except SystemExit as e:  # argparse usage errors
            code = e.code
        out, err = capsys.readouterr()
        return code, out, err

    return run


@pytest.fixture(autouse=True)
def not_a_terminal(monkeypatch):
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))


def ok(result: tuple[int, str, str]) -> str:
    code, out, err = result
    assert (code, err) == (0, ""), err
    return out


@pytest.fixture
def keyring(run, tmp_path) -> Path:
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1"))
    return path


@pytest.fixture
def password_file(tmp_path) -> Path:
    path = tmp_path / "pw"
    path.write_text(PASSWORD + "\n")
    return path


# --- keyring management ---


def test_new_writes_an_oct_keyring_with_mode_0600(run, tmp_path):
    path = tmp_path / "k.json"
    assert ok(run("keyring", "new", str(path), "--kid", "k1")) == ""
    keyring = Keyring.from_json(path.read_text())
    assert keyring.primary == "k1" and list(keyring.keys) == ["k1"]
    assert path.stat().st_mode & 0o777 == 0o600


def test_new_refuses_to_overwrite_without_force(run, keyring):
    before = keyring.read_bytes()
    code, _, err = run("keyring", "new", str(keyring), "--kid", "other")
    assert code == 1 and "already exists" in err
    assert keyring.read_bytes() == before
    ok(run("keyring", "new", str(keyring), "--kid", "other", "--force"))
    assert Keyring.from_json(keyring.read_text()).primary == "other"


def test_new_x25519_needs_a_scope(run, tmp_path):
    code, _, err = run("keyring", "new", str(tmp_path / "k.json"), "--kid", "r", "--type", "x25519")
    assert code == 1 and "must be bound" in err and not (tmp_path / "k.json").exists()
    ok(run("keyring", "new", str(tmp_path / "k.json"), "--kid", "r", "--type", "x25519", "--scope", "s"))
    assert list(Keyring.from_json((tmp_path / "k.json").read_text()).recipients) == ["r"]


def test_add_primary_signing_and_remove(run, keyring):
    ok(run("keyring", "add", str(keyring), "--kid", "k2", "--scope", "a", "--scope", "b", "--primary-for", "a"))
    ok(run("keyring", "add", str(keyring), "--kid", "sg", "--type", "ed25519", "--scope", "a", "--scope", "b",
           "--signing-for", "a"))
    ok(run("keyring", "add", str(keyring), "--kid", "r", "--type", "x25519", "--scope", "b"))
    loaded = Keyring.from_json(keyring.read_text())
    assert loaded.primaries == {"a": "k2"} and loaded.signing == {"a": "sg"}
    assert loaded.scopes["k2"] == {"a", "b"} and sorted(loaded.kids()) == ["k1", "k2", "r", "sg"]
    ok(run("keyring", "primary", str(keyring), "r"))
    ok(run("keyring", "primary", str(keyring), "k1", "--scope", "b"))
    ok(run("keyring", "signing", str(keyring), "sg", "--scope", "b"))
    loaded = Keyring.from_json(keyring.read_text())
    assert loaded.primary == "r" and loaded.primaries == {"a": "k2", "b": "k1"} and loaded.signing == {"a": "sg", "b": "sg"}
    ok(run("keyring", "remove", str(keyring), "k2"))
    loaded = Keyring.from_json(keyring.read_text())
    assert "k2" not in loaded.keys and "k2" not in loaded.scopes and loaded.primaries == {"b": "k1"}


def test_add_rejects_a_duplicate_kid(run, keyring):
    code, _, err = run("keyring", "add", str(keyring), "--kid", "k1")
    assert code == 1 and "duplicate kid 'k1'" in err


def test_remove_refuses_the_primary_and_unknown_kids(run, keyring):
    code, _, err = run("keyring", "remove", str(keyring), "k1")
    assert code == 1 and "primary" in err and "set another primary first" in err
    code, _, err = run("keyring", "remove", str(keyring), "nope")
    assert code == 1 and "no key 'nope'" in err


def test_show_lists_entries_without_key_material(run, keyring):
    ok(run("keyring", "add", str(keyring), "--kid", "sg", "--type", "ed25519", "--scope", "a", "--signing-for", "a"))
    ok(run("keyring", "add", str(keyring), "--kid", "r", "--type", "x25519", "--scope", "a", "--primary-for", "a"))
    out = ok(run("keyring", "show", str(keyring)))
    assert out.splitlines() == [
        "k1  oct  private  scopes=-  roles=primary",
        "r  x25519  private  scopes=a  roles=primary-for:a",
        "sg  ed25519  private  scopes=a  roles=signing-for:a",
    ]
    document = json.loads(keyring.read_text())
    secrets_in_file = [member for jwk in document["keys"] for name, member in jwk.items() if name in ("k", "d", "x")]
    assert secrets_in_file and not any(secret in out for secret in secrets_in_file)


def test_show_on_an_invalid_file_exits_1(run, tmp_path):
    (tmp_path / "bad.json").write_text("{")
    code, out, err = run("keyring", "show", str(tmp_path / "bad.json"))
    assert (code, out) == (1, "") and err.startswith("flechtctl: error:") and "Traceback" not in err


def test_export_prints_the_public_half_and_leaves_the_keyring_alone(run, tmp_path):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "r", "--type", "x25519", "--scope", "s"))
    before = path.read_bytes()
    out = ok(run("keyring", "export", str(path), "r"))
    jwk = json.loads(out)
    assert sorted(jwk) == ["crv", "flenc_scopes", "kid", "kty", "x"] and "d" not in jwk
    assert out == json.dumps(jwk, indent=2, sort_keys=True) + "\n"
    assert path.read_bytes() == before
    ok(run("keyring", "export", str(path), "r", "-o", str(tmp_path / "r.jwk")))
    assert json.loads((tmp_path / "r.jwk").read_text()) == jwk


def test_export_of_an_oct_entry_or_an_unknown_kid_is_an_error(run, keyring):
    code, _, err = run("keyring", "export", str(keyring), "k1")
    assert code == 1 and "no public half" in err
    assert run("keyring", "export", str(keyring), "nope")[0] == 1


def test_import_adds_an_entry_and_rejects_a_collision(run, tmp_path):
    reader, writer = tmp_path / "reader.json", tmp_path / "writer.json"
    ok(run("keyring", "new", str(reader), "--kid", "r", "--type", "x25519", "--scope", "s"))
    ok(run("keyring", "export", str(reader), "r", "-o", str(tmp_path / "r.jwk")))
    code, _, err = run("keyring", "import", str(writer), str(tmp_path / "r.jwk"))
    assert code == 1 and "needs a primary" in err and not writer.exists()
    ok(run("keyring", "import", str(writer), str(tmp_path / "r.jwk"), "--primary"))
    assert Keyring.from_json(writer.read_text()).recipients["r"].private is None
    code, _, err = run("keyring", "import", str(writer), str(tmp_path / "r.jwk"))
    assert code == 1 and "duplicate kid 'r'" in err


def test_import_of_a_signer_cannot_create_a_keyring(run, tmp_path):
    source = tmp_path / "s.json"
    ok(run("keyring", "new", str(source), "--kid", "k1"))
    ok(run("keyring", "add", str(source), "--kid", "sg", "--type", "ed25519", "--scope", "s", "--signing-for", "s"))
    ok(run("keyring", "export", str(source), "sg", "-o", str(tmp_path / "sg.jwk")))
    code, _, err = run("keyring", "import", str(tmp_path / "new.json"), str(tmp_path / "sg.jwk"), "--primary")
    assert code == 1 and err.startswith("flechtctl: error:")


# --- usage errors and the error format ---


def test_a_crypto_command_without_a_keyring_is_a_usage_error(run):
    for command in ("encrypt", "decrypt", "sign", "verify", "reencrypt"):
        assert run(command, "--scope", "s", "x")[0] == 2, command


def test_the_command_reads_no_environment_variable(run, monkeypatch, keyring):
    monkeypatch.setenv("FLECHTCTL_KEYRING", str(keyring))
    monkeypatch.setenv("FLECHTCTL_PASSWORD", PASSWORD)
    assert run("encrypt", "--string", "x")[0] == 2


def test_a_missing_subcommand_is_a_usage_error(run):
    assert run()[0] == 2
    assert run("keyring")[0] == 2


def test_errors_are_one_line_without_a_traceback(run, tmp_path):
    code, out, err = run("encrypt", "-k", str(tmp_path / "missing.json"), "--string", "x")
    assert (code, out) == (1, "")
    assert err.startswith("flechtctl: error:") and err.count("\n") == 1 and "Traceback" not in err


def test_version(run):
    code, out, _ = run("--version")
    assert code == 0 and out.startswith("flechtctl ") and len(out.split()) == 2


# --- value crypto ---


def test_encrypt_and_decrypt_round_trip(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "--scope", "demo", "--string", "hello")).strip()
    assert token.startswith("flenc:jwe:")
    assert ok(run("decrypt", "-k", str(keyring), "--scope", "demo", "--string", token)) == "hello\n"
    assert ok(run("decrypt", "-k", str(keyring), "--scope", "demo", token)) == '"hello"\n'


def test_encrypt_parses_json_unless_string(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), '{"b": [1, "ä"], "a": null}')).strip()
    assert ok(run("decrypt", "-k", str(keyring), token)) == '{"a": null, "b": [1, "ä"]}\n'
    code, _, err = run("encrypt", "-k", str(keyring), "not json")
    assert code == 1 and "--string" in err


def test_decrypt_string_needs_a_string_value(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "42")).strip()
    code, _, err = run("decrypt", "-k", str(keyring), "--string", token)
    assert code == 1 and "not a JSON string" in err


def test_stdin_is_read_when_the_operand_is_omitted_or_dash(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "--string", stdin="from stdin\n")).strip()
    assert ok(run("decrypt", "-k", str(keyring), "--string", stdin=token + "\n")) == "from stdin\n"
    assert ok(run("decrypt", "-k", str(keyring), "--string", "-", stdin=token + "\n")) == "from stdin\n"
    # exactly one trailing newline is stripped
    token = ok(run("encrypt", "-k", str(keyring), "--string", stdin="a\n\n")).strip()
    assert ok(run("decrypt", "-k", str(keyring), "--string", token)) == "a\n\n"


def test_a_wrong_scope_exits_1(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "--scope", "demo", "--string", "x")).strip()
    code, out, err = run("decrypt", "-k", str(keyring), "--scope", "other", "--string", token)
    assert (code, out) == (1, "") and "scope mismatch" in err


def test_sign_and_verify(run, tmp_path):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1"))
    ok(run("keyring", "add", str(path), "--kid", "sg", "--type", "ed25519", "--scope", "demo", "--signing-for", "demo"))
    token = ok(run("sign", "-k", str(path), "--scope", "demo", "--string", "hello")).strip()
    assert token.startswith("flsig:jws:")
    assert ok(run("verify", "-k", str(path), "--scope", "demo", "--string", token)) == "hello\n"
    assert ok(run("verify", "-k", str(path), "--scope", "demo", token)) == '"hello"\n'
    code, out, err = run("verify", "-k", str(path), "--scope", "other", token)
    assert (code, out) == (1, "") and "scope mismatch" in err
    assert run("sign", "-k", str(path), "--string", "x")[0] == 2  # --scope is required


def test_sign_then_encrypt_pipe_composition(run, tmp_path):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1"))
    ok(run("keyring", "add", str(path), "--kid", "sg", "--type", "ed25519", "--scope", "demo", "--signing-for", "demo"))
    signed = ok(run("sign", "-k", str(path), "--scope", "demo", "--string", "hello"))
    sealed = ok(run("encrypt", "-k", str(path), "--scope", "demo", "--string", stdin=signed))
    opened = ok(run("decrypt", "-k", str(path), "--scope", "demo", "--string", stdin=sealed))
    assert ok(run("verify", "-k", str(path), "--scope", "demo", "--string", stdin=opened)) == "hello\n"


def test_inspect_prints_the_unauthenticated_header(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "--scope", "demo", "--string", "x")).strip()
    out = ok(run("inspect", token))
    assert json.loads(out) == {
        "authenticated": False,
        "envelope": "flenc:jwe",
        "header": {"alg": "dir", "enc": "A256GCM", "flenc_scope": "demo", "kid": "k1"},
    }
    assert out == json.dumps(json.loads(out), indent=2, sort_keys=True) + "\n"
    assert json.loads(ok(run("inspect", stdin=token + "\n")))["header"]["kid"] == "k1"
    code, _, err = run("inspect", "plain")
    assert code == 1 and "not a flenc or flsig token" in err


def test_inspect_names_the_signature_envelope(run, tmp_path):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1"))
    ok(run("keyring", "add", str(path), "--kid", "sg", "--type", "ed25519", "--scope", "demo", "--signing-for", "demo"))
    token = ok(run("sign", "-k", str(path), "--scope", "demo", "--string", "x")).strip()
    assert json.loads(ok(run("inspect", token)))["envelope"] == "flsig:jws"


def test_reencrypt_moves_a_token_to_the_scopes_sealing_key(run, keyring):
    token = ok(run("encrypt", "-k", str(keyring), "--scope", "demo", "--string", "x")).strip()
    ok(run("keyring", "add", str(keyring), "--kid", "r", "--type", "x25519", "--scope", "demo", "--primary-for", "demo"))
    moved = ok(run("reencrypt", "-k", str(keyring), "--scope", "demo", token)).strip()
    assert read_header(token)["alg"] == "dir" and read_header(moved)["alg"] == "ECDH-ES+A256KW"
    assert ok(run("decrypt", "-k", str(keyring), "--scope", "demo", "--string", moved)) == "x\n"


# --- password-protected keyring files ---


def test_new_encrypt_writes_a_protected_file(run, tmp_path, password_file):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt", "--password-file", str(password_file)))
    assert path.read_text().startswith("ey") and is_encrypted_keyring(path.read_text())
    assert path.stat().st_mode & 0o777 == 0o600
    assert PASSWORD not in path.read_text()


def test_every_crypto_command_opens_a_protected_keyring(run, tmp_path, password_file):
    path, pw = tmp_path / "k.json", str(password_file)
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt", "--password-file", pw))
    ok(run("keyring", "add", str(path), "--kid", "sg", "--type", "ed25519", "--scope", "demo",
           "--signing-for", "demo", "--password-file", pw))
    token = ok(run("encrypt", "-k", str(path), "--scope", "demo", "--string", "hello", "--password-file", pw)).strip()
    assert ok(run("decrypt", "-k", str(path), "--scope", "demo", "--string", token, "--password-file", pw)) == "hello\n"
    signed = ok(run("sign", "-k", str(path), "--scope", "demo", "--string", "v", "--password-file", pw)).strip()
    assert ok(run("verify", "-k", str(path), "--scope", "demo", "--string", signed, "--password-file", pw)) == "v\n"
    assert ok(run("reencrypt", "-k", str(path), "--scope", "demo", token, "--password-file", pw)).startswith("flenc:jwe:")
    assert "k1  oct  private" in ok(run("keyring", "show", str(path), "--password-file", pw))


def test_an_edit_keeps_the_file_protected_with_a_fresh_salt(run, tmp_path, password_file):
    path, pw = tmp_path / "k.json", str(password_file)
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt", "--password-file", pw))
    before = path.read_text()
    ok(run("keyring", "add", str(path), "--kid", "k2", "--password-file", pw))
    after = path.read_text()
    assert is_encrypted_keyring(after) and after.split(".")[0] != before.split(".")[0]  # p2s differs → header differs
    assert "k2  oct" in ok(run("keyring", "show", str(path), "--password-file", pw))


def test_protect_and_unprotect(run, keyring, tmp_path, password_file):
    pw = str(password_file)
    plain_before = Keyring.from_json(keyring.read_text())
    ok(run("keyring", "protect", str(keyring), "--password-file", pw))
    assert is_encrypted_keyring(keyring.read_text())
    code, _, err = run("keyring", "protect", str(keyring), "--password-file", pw)
    assert code == 1 and "already password-protected" in err
    out = tmp_path / "plain.json"
    ok(run("keyring", "unprotect", str(keyring), "-o", str(out), "--password-file", pw))
    assert out.read_text().startswith("{") and out.stat().st_mode & 0o777 == 0o600
    assert Keyring.from_json(out.read_text()) == plain_before
    assert is_encrypted_keyring(keyring.read_text())  # the original stays protected
    code, _, err = run("keyring", "unprotect", str(keyring), "-o", str(out), "--password-file", pw)
    assert code == 1 and "already exists" in err
    ok(run("keyring", "unprotect", str(keyring), "-o", str(out), "--force", "--password-file", pw))


def test_unprotect_of_a_plain_keyring_is_an_error(run, keyring, tmp_path):
    code, _, err = run("keyring", "unprotect", str(keyring), "-o", str(tmp_path / "o.json"))
    assert code == 1 and "not password-protected" in err


def test_a_wrong_password_exits_1(run, tmp_path, password_file):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt", "--password-file", str(password_file)))
    wrong = tmp_path / "wrong"
    wrong.write_text("another long password\n")
    code, _, err = run("keyring", "show", str(path), "--password-file", str(wrong))
    assert code == 1 and "wrong password or damaged file" in err


def test_a_short_password_is_refused(run, tmp_path):
    short = tmp_path / "short"
    short.write_text("short\n")
    code, _, err = run("keyring", "new", str(tmp_path / "k.json"), "--kid", "k1", "--encrypt", "--password-file", str(short))
    assert code == 1 and "at least 14 bytes" in err and not (tmp_path / "k.json").exists()


def test_without_a_terminal_a_protected_keyring_needs_a_password_file(run, tmp_path, password_file):
    path = tmp_path / "k.json"
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt", "--password-file", str(password_file)))
    code, _, err = run("keyring", "show", str(path))
    assert code == 1 and "pass --password-file" in err
    code, _, err = run("keyring", "new", str(tmp_path / "n.json"), "--kid", "k1", "--encrypt")
    assert code == 1 and "pass --password-file" in err


def test_the_prompt_path_on_a_terminal(run, monkeypatch, tmp_path):
    path = tmp_path / "k.json"
    answers = iter([PASSWORD, PASSWORD, PASSWORD])
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": next(answers))
    monkeypatch.setattr(sys, "stdin", Terminal())
    ok(run("keyring", "new", str(path), "--kid", "k1", "--encrypt"))  # prompts twice
    assert is_encrypted_keyring(path.read_text())
    monkeypatch.setattr(sys, "stdin", Terminal())
    assert "k1  oct" in ok(run("keyring", "show", str(path)))  # prompts once
    assert list(answers) == []


def test_a_mismatched_confirmation_exits_1(run, monkeypatch, tmp_path):
    answers = iter([PASSWORD, "something else entirely"])
    monkeypatch.setattr(getpass, "getpass", lambda prompt="": next(answers))
    monkeypatch.setattr(sys, "stdin", Terminal())
    code, _, err = run("keyring", "new", str(tmp_path / "k.json"), "--kid", "k1", "--encrypt")
    assert code == 1 and "do not match" in err and not (tmp_path / "k.json").exists()


def test_import_encrypt_protects_a_newly_created_keyring_only(run, tmp_path, password_file):
    source = tmp_path / "s.json"
    ok(run("keyring", "new", str(source), "--kid", "r", "--type", "x25519", "--scope", "s"))
    ok(run("keyring", "export", str(source), "r", "-o", str(tmp_path / "r.jwk")))
    target = tmp_path / "t.json"
    ok(run("keyring", "import", str(target), str(tmp_path / "r.jwk"), "--primary", "--encrypt",
           "--password-file", str(password_file)))
    assert is_encrypted_keyring(target.read_text())
    code, _, err = run("keyring", "import", str(source), str(tmp_path / "r.jwk"), "--encrypt")
    assert code == 1 and "only applies when import creates" in err


# --- the walkthrough the guide prints ---


def test_walkthrough(run, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    shows = []
    for step in WALKTHROUGH:
        out = ok(run(*step.split()))
        if step.startswith("keyring show"):
            shows.append(out)
    reader_show, writer_show = shows
    assert reader_show.splitlines() == [
        "demo-recipient  x25519  private  scopes=demo  roles=primary",
        "demo-signer  ed25519  public  scopes=demo  roles=-",
    ]
    assert writer_show.splitlines() == [
        "demo-recipient  x25519  public  scopes=demo  roles=primary",
        "demo-signer  ed25519  private  scopes=demo  roles=signing-for:demo",
    ]
    # TOKEN=$(flechtctl sign -k writer.json --scope demo --string hello | flechtctl encrypt -k writer.json --scope demo --string)
    signed = ok(run("sign", "-k", "writer.json", "--scope", "demo", "--string", "hello"))
    token = ok(run("encrypt", "-k", "writer.json", "--scope", "demo", "--string", stdin=signed)).strip()
    # flechtctl inspect "$TOKEN"
    header = json.loads(ok(run("inspect", token)))["header"]
    assert header["alg"] == "ECDH-ES+A256KW" and header["kid"] == "demo-recipient" and header["flenc_scope"] == "demo"
    # flechtctl decrypt -k reader.json --scope demo --string "$TOKEN" | flechtctl verify -k reader.json --scope demo --string
    opened = ok(run("decrypt", "-k", "reader.json", "--scope", "demo", "--string", token))
    assert ok(run("verify", "-k", "reader.json", "--scope", "demo", "--string", stdin=opened)) == "hello\n"
    # the writer can seal and sign but cannot open
    code, out, err = run("decrypt", "-k", "writer.json", "--scope", "demo", "--string", token)
    assert (code, out) == (1, "") and "can seal but not open" in err


# --- the installed console script ---


def test_the_console_script_runs():
    script = Path(sys.executable).with_name("flechtctl")
    result = subprocess.run([str(script), "--version"], capture_output=True, text=True, check=True)
    assert result.stdout.startswith("flechtctl ") and result.stderr == ""
    assert subprocess.run([sys.executable, "-m", "flechtctl", "--version"], capture_output=True, text=True).stdout == result.stdout
