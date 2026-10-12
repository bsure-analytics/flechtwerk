"""The `flechtctl` command: keyring management, value crypto, token tooling.

Stdlib `argparse` only. Every crypto command takes its keyring explicitly with
`-k/--keyring PATH` — the command reads no environment variables, by decision.
A keyring file may be password-protected (an encrypted JWK Set): it is detected
on read and opened with `--password-file PATH`, or a prompt on a terminal.
"""
import argparse
import contextlib
import getpass
import json
import os
import sys
import tempfile
from collections.abc import Callable
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

from . import tokens
from .errors import SecretError
from .keyring import Keyring, KeyringError, is_encrypted_keyring, keyring_from_jwk

__all__ = ["main"]

KEY_TYPES = ("oct", "x25519", "ed25519")


def main(argv: list[str] | None = None) -> int:
    """Run the command; return the exit status (0 ok, 1 error, 2 usage)."""
    args = build_parser().parse_args(argv)
    try:
        args.run(args)
    except (KeyringError, SecretError, OSError, ValueError, ModuleNotFoundError) as e:
        print(f"flechtctl: error: {e}", file=sys.stderr)
        return 1
    return 0


# --- the parser ---


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flechtctl",
        description="Keyrings and flenc:/flsig: secret tokens for Flechtwerk. "
                    "Reads no environment variables: every crypto command needs -k/--keyring.",
    )
    parser.add_argument("--version", action="version", version=f"flechtctl {version('flechtctl')}")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    def command(name: str, run: Callable[[argparse.Namespace], None], help: str) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=help, description=help)
        sub.set_defaults(run=run)
        return sub

    decrypt = command("decrypt", run_decrypt, "decrypt a flenc token and print its value")
    add_keyring_option(decrypt)
    add_scope_option(decrypt)
    add_value_arguments(decrypt, "TOKEN", "print the value raw (it must be a JSON string)")

    encrypt = command("encrypt", run_encrypt, "encrypt a value and print the flenc token")
    add_keyring_option(encrypt)
    add_scope_option(encrypt)
    add_value_arguments(encrypt, "VALUE", "take VALUE as a raw string, not as JSON")

    inspect = command("inspect", run_inspect, "show the unauthenticated header of a flenc or flsig token (no keyring)")
    inspect.add_argument("token", metavar="TOKEN", nargs="?", help="the token; omitted or - reads stdin")

    keyring = command("keyring", lambda args: None, "create and edit keyring files")
    add_keyring_commands(keyring.add_subparsers(dest="keyring_command", metavar="SUBCOMMAND", required=True))

    reencrypt = command("reencrypt", run_reencrypt, "re-seal a flenc token under the scope's current sealing key")
    add_keyring_option(reencrypt)
    add_scope_option(reencrypt)
    reencrypt.add_argument("token", metavar="TOKEN", nargs="?", help="the token; omitted or - reads stdin")

    sign = command("sign", run_sign, "sign a value and print the flsig token")
    add_keyring_option(sign)
    add_scope_option(sign, required=True)
    add_value_arguments(sign, "VALUE", "take VALUE as a raw string, not as JSON")

    verify = command("verify", run_verify, "verify a flsig token and print its value")
    add_keyring_option(verify)
    add_scope_option(verify, required=True)
    add_value_arguments(verify, "TOKEN", "print the value raw (it must be a JSON string)")
    return parser


def add_keyring_commands(subcommands: argparse._SubParsersAction) -> None:
    def sub(name: str, run: Callable[[argparse.Namespace], None], help: str) -> argparse.ArgumentParser:
        parser = subcommands.add_parser(name, help=help, description=help)
        parser.set_defaults(run=run)
        return parser

    def keyring_argument(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("keyring", metavar="KEYRING", help="the keyring file")

    def role_options(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--primary", action="store_true", help="make the entry the primary sealing key")
        parser.add_argument("--primary-for", action="append", default=[], metavar="SCOPE",
                            help="make the entry the sealing key for SCOPE (repeatable)")
        parser.add_argument("--signing-for", action="append", default=[], metavar="SCOPE",
                            help="make the entry the signing key for SCOPE (repeatable)")

    add = sub("add", run_add, "add a freshly generated key to a keyring")
    keyring_argument(add)
    add_password_option(add)
    add.add_argument("--kid", required=True, help="the new key's id")
    add.add_argument("--type", choices=KEY_TYPES, default="oct", help="the kind of key (default: oct)")
    add.add_argument("--scope", action="append", default=[], metavar="SCOPE", help="bind the key to SCOPE (repeatable)")
    role_options(add)

    export = sub("export", run_export, "print the public half of one entry as a JWK (the keyring is left alone)")
    keyring_argument(export)
    add_password_option(export)
    export.add_argument("kid", metavar="KID", help="the entry to export")
    export.add_argument("-o", "--output", metavar="OUT", help="write the JWK to OUT instead of stdout")

    import_ = sub("import", run_import, "add the single JWK in JWKFILE (typically an export) as a new entry")
    keyring_argument(import_)
    add_password_option(import_)
    import_.add_argument("jwkfile", metavar="JWKFILE", help="a file holding one JWK")
    import_.add_argument("--encrypt", action="store_true", help="when creating the keyring, write it password-protected")
    role_options(import_)

    new = sub("new", run_new, "create a keyring holding one freshly generated sealing key")
    keyring_argument(new)
    add_password_option(new)
    new.add_argument("--kid", required=True, help="the key's id")
    new.add_argument("--type", choices=KEY_TYPES[:2], default="oct", help="the kind of key (default: oct)")
    new.add_argument("--scope", action="append", default=[], metavar="SCOPE", help="bind the key to SCOPE (repeatable)")
    new.add_argument("--force", action="store_true", help="overwrite an existing file")
    new.add_argument("--encrypt", action="store_true", help="write the keyring password-protected")

    primary = sub("primary", run_primary, "set the primary sealing key, or the sealing key for one scope")
    keyring_argument(primary)
    add_password_option(primary)
    primary.add_argument("kid", metavar="KID")
    primary.add_argument("--scope", metavar="SCOPE", help="set the sealing key for SCOPE instead of the primary")

    protect = sub("protect", run_protect, "rewrite a plain keyring file in place as password-protected")
    keyring_argument(protect)
    add_password_option(protect)

    remove = sub("remove", run_remove, "drop an entry with its scope binding and roles")
    keyring_argument(remove)
    add_password_option(remove)
    remove.add_argument("kid", metavar="KID")

    show = sub("show", run_show, "list the entries (never any key material)")
    keyring_argument(show)
    add_password_option(show)

    signing = sub("signing", run_signing, "set the signing key for one scope")
    keyring_argument(signing)
    add_password_option(signing)
    signing.add_argument("kid", metavar="KID")
    signing.add_argument("--scope", required=True, metavar="SCOPE")

    unprotect = sub("unprotect", run_unprotect, "write a plain copy of a protected keyring (it stays protected)")
    keyring_argument(unprotect)
    add_password_option(unprotect)
    unprotect.add_argument("-o", "--output", required=True, metavar="OUT", help="where to write the plain copy")
    unprotect.add_argument("--force", action="store_true", help="overwrite OUT if it exists")


def add_keyring_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-k", "--keyring", required=True, metavar="PATH", help="the keyring file")
    add_password_option(parser)


def add_password_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--password-file", metavar="PATH", help="file holding the keyring password")


def add_scope_option(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--scope", default="", required=required, metavar="SCOPE", help="the domain-separation scope")


def add_value_arguments(parser: argparse.ArgumentParser, name: str, string_help: str) -> None:
    parser.add_argument("--string", action="store_true", help=string_help)
    parser.add_argument(name.lower(), metavar=name, nargs="?", help=f"the {name.lower()}; omitted or - reads stdin")


# --- input, output, files ---


def read_operand(operand: str | None) -> str:
    """The operand itself, or stdin (one trailing newline stripped) when omitted or `-`."""
    if operand is None or operand == "-":
        return sys.stdin.read().removesuffix("\n")
    return operand


def read_value(args: argparse.Namespace) -> object:
    text = read_operand(args.value)
    if args.string:
        return text
    try:
        return json.loads(text)
    except ValueError as e:
        raise ValueError(f"VALUE is not valid JSON ({e}) — pass --string for a plain string") from e


def print_value(value: object, *, raw: bool) -> None:
    if raw:
        if not isinstance(value, str):
            raise ValueError("the value is not a JSON string — drop --string to print it as JSON")
        print(value)
    else:
        print(json.dumps(value, ensure_ascii=False))


def write_private(path: Path, text: str) -> None:
    """Write `path` atomically, mode 0600: a temp file next to it, then a rename."""
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(text)
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def read_password_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").removesuffix("\n")


def existing_password(args: argparse.Namespace) -> str:
    if args.password_file:
        return read_password_file(args.password_file)
    if sys.stdin.isatty():
        return getpass.getpass("Keyring password: ")
    raise ValueError("this keyring is password-protected — pass --password-file")


def new_password(args: argparse.Namespace) -> str:
    if args.password_file:
        return read_password_file(args.password_file)
    if sys.stdin.isatty():
        password = getpass.getpass("Keyring password: ")
        if getpass.getpass("Repeat the password: ") != password:
            raise ValueError("the passwords do not match")
        return password
    raise ValueError("a password-protected keyring needs a password — pass --password-file")


def open_keyring(args: argparse.Namespace) -> tuple[Keyring, str | None]:
    """The keyring in `args.keyring` and, when its file is protected, the password that opened it."""
    text = Path(args.keyring).read_text(encoding="utf-8")
    if is_encrypted_keyring(text):
        password = existing_password(args)
        return tokens.decrypt_keyring(text, password), password
    return Keyring.from_json(text), None


def save_keyring(path: str, keyring: Keyring, password: str | None) -> None:
    """Write the keyring — protected again when it was opened with a password, never silently plain."""
    write_private(Path(path), keyring.to_json() if password is None else tokens.encrypt_keyring(keyring, password) + "\n")


def crypto_keyring(args: argparse.Namespace) -> Keyring:
    return open_keyring(args)[0]


# --- value crypto ---


def run_decrypt(args: argparse.Namespace) -> None:
    value = tokens.decrypt(crypto_keyring(args), read_operand(args.token), scope=args.scope)
    print_value(value, raw=args.string)


def run_encrypt(args: argparse.Namespace) -> None:
    print(tokens.encrypt(crypto_keyring(args), read_value(args), scope=args.scope))


def run_reencrypt(args: argparse.Namespace) -> None:
    print(tokens.reencrypt(crypto_keyring(args), read_operand(args.token), scope=args.scope))


def run_sign(args: argparse.Namespace) -> None:
    print(tokens.sign(crypto_keyring(args), read_value(args), scope=args.scope))


def run_verify(args: argparse.Namespace) -> None:
    print_value(tokens.verify(crypto_keyring(args), read_operand(args.token), scope=args.scope), raw=args.string)


def run_inspect(args: argparse.Namespace) -> None:
    token = read_operand(args.token)
    header = tokens.read_header(token)
    envelope = "flenc:jwe" if tokens.is_encrypted(token) else "flsig:jws"
    print(json.dumps({"authenticated": False, "envelope": envelope, "header": header}, indent=2, sort_keys=True))


# --- keyring management ---


def generated_entry(keyring: Keyring, kid: str, kind: str, scopes: list[str]) -> Keyring:
    """`keyring` plus one freshly generated key of `kind`; validation runs in the constructor."""
    if kid in keyring.kids():
        raise KeyringError(f"duplicate kid {kid!r} in the keyring")
    return replace(
        keyring,
        keys={**keyring.keys, kid: tokens.generate_oct_key()} if kind == "oct" else keyring.keys,
        recipients={**keyring.recipients, kid: tokens.generate_x25519_key()} if kind == "x25519" else keyring.recipients,
        scopes={**keyring.scopes, kid: frozenset(scopes)} if scopes else keyring.scopes,
        signers={**keyring.signers, kid: tokens.generate_ed25519_key()} if kind == "ed25519" else keyring.signers,
    )


def with_roles(keyring: Keyring, kid: str, args: argparse.Namespace) -> Keyring:
    return replace(
        keyring,
        primary=kid if args.primary else keyring.primary,
        primaries={**keyring.primaries, **{scope: kid for scope in args.primary_for}},
        signing={**keyring.signing, **{scope: kid for scope in args.signing_for}},
    )


def run_add(args: argparse.Namespace) -> None:
    keyring, password = open_keyring(args)
    save_keyring(args.keyring, with_roles(generated_entry(keyring, args.kid, args.type, args.scope), args.kid, args), password)


def run_export(args: argparse.Namespace) -> None:
    text = json.dumps(open_keyring(args)[0].public_jwk(args.kid), indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


def run_import(args: argparse.Namespace) -> None:
    jwk = json.loads(Path(args.jwkfile).read_text(encoding="utf-8"))
    if not isinstance(jwk, dict):
        raise KeyringError(f"each JWK must be a JSON object, got {type(jwk).__name__}")
    if Path(args.keyring).exists():
        if args.encrypt:
            raise ValueError("--encrypt only applies when import creates the keyring; use flechtctl keyring protect")
        keyring, password = open_keyring(args)
        keyring = keyring.with_jwk(jwk)
    else:
        if not args.primary:
            raise ValueError("a new keyring needs a primary: pass --primary")
        keyring, password = keyring_from_jwk(jwk), new_password(args) if args.encrypt else None
    save_keyring(args.keyring, with_roles(keyring, jwk["kid"], args), password)


def run_new(args: argparse.Namespace) -> None:
    path = Path(args.keyring)
    if path.exists() and not args.force:
        raise ValueError(f"{path} already exists — pass --force to overwrite it")
    password = new_password(args) if args.encrypt else None
    if args.type == "oct":
        keyring = Keyring.of({args.kid: tokens.generate_oct_key()}, primary=args.kid, scopes=bound(args))
    else:
        keyring = Keyring.of(
            {}, primary=args.kid, recipients={args.kid: tokens.generate_x25519_key()}, scopes=bound(args),
        )
    save_keyring(args.keyring, keyring, password)


def bound(args: argparse.Namespace) -> dict[str, set[str]]:
    return {args.kid: set(args.scope)} if args.scope else {}


def run_primary(args: argparse.Namespace) -> None:
    keyring, password = open_keyring(args)
    if args.scope:
        keyring = replace(keyring, primaries={**keyring.primaries, args.scope: args.kid})
    else:
        keyring = replace(keyring, primary=args.kid)
    save_keyring(args.keyring, keyring, password)


def run_protect(args: argparse.Namespace) -> None:
    text = Path(args.keyring).read_text(encoding="utf-8")
    if is_encrypted_keyring(text):
        raise ValueError(f"{args.keyring} is already password-protected")
    save_keyring(args.keyring, Keyring.from_json(text), new_password(args))


def run_remove(args: argparse.Namespace) -> None:
    keyring, password = open_keyring(args)
    kid = args.kid
    if kid not in keyring.kids():
        raise KeyringError(f"the keyring holds no key {kid!r}")
    if kid == keyring.primary:
        raise KeyringError(f"{kid!r} is the primary key — set another primary first (flechtctl keyring primary)")
    save_keyring(args.keyring, replace(
        keyring,
        keys={k: v for k, v in keyring.keys.items() if k != kid},
        primaries={scope: k for scope, k in keyring.primaries.items() if k != kid},
        recipients={k: v for k, v in keyring.recipients.items() if k != kid},
        scopes={k: v for k, v in keyring.scopes.items() if k != kid},
        signers={k: v for k, v in keyring.signers.items() if k != kid},
        signing={scope: k for scope, k in keyring.signing.items() if k != kid},
    ), password)


def run_show(args: argparse.Namespace) -> None:
    keyring = open_keyring(args)[0]
    for kid in keyring.kids():
        if kid in keyring.keys:
            kind, half = "oct", "private"
        elif kid in keyring.recipients:
            kind, half = "x25519", "private" if keyring.recipients[kid].private is not None else "public"
        else:
            kind, half = "ed25519", "private" if keyring.signers[kid].private is not None else "public"
        roles = [
            *(["primary"] if kid == keyring.primary else []),
            *(f"primary-for:{scope}" for scope, k in sorted(keyring.primaries.items()) if k == kid),
            *(f"signing-for:{scope}" for scope, k in sorted(keyring.signing.items()) if k == kid),
        ]
        scopes = ",".join(sorted(keyring.scopes.get(kid, ()))) or "-"
        print(f"{kid}  {kind}  {half}  scopes={scopes}  roles={','.join(roles) or '-'}")


def run_signing(args: argparse.Namespace) -> None:
    keyring, password = open_keyring(args)
    save_keyring(args.keyring, replace(keyring, signing={**keyring.signing, args.scope: args.kid}), password)


def run_unprotect(args: argparse.Namespace) -> None:
    text = Path(args.keyring).read_text(encoding="utf-8")
    if not is_encrypted_keyring(text):
        raise ValueError(f"{args.keyring} is not password-protected")
    output = Path(args.output)
    if output.exists() and not args.force:
        raise ValueError(f"{output} already exists — pass --force to overwrite it")
    write_private(output, tokens.decrypt_keyring(text, existing_password(args)).to_json())
