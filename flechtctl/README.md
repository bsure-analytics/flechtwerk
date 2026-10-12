# flechtctl

Keyrings and `flenc:` / `flsig:` secret tokens for
[Flechtwerk](https://github.com/bsure-analytics/flechtwerk) — as a small library and a
command line tool, **without the framework**. `flechtctl` depends on
[joserfc](https://jose.authlib.org/) only; it never imports `flechtwerk`, so it installs and
runs wherever Python does (including interpreters the full stream-processing framework does
not support yet).

It does three things:

- **Keyrings.** Create and edit keyring files — an RFC 7517 JWK Set holding oct (symmetric),
  X25519 (sealing) and Ed25519 (signing) keys, each optionally bound to scopes. Optionally
  password-protected.
- **Value crypto.** Seal, open, sign and verify values: `flenc:jwe:…` compact JWE tokens and
  `flsig:jws:…` Ed25519 compact JWS tokens.
- **Token tooling.** Inspect a token's header, re-seal a token under another key.

## Install

```bash
uvx flechtctl --help            # run it without installing
uv tool install flechtctl       # or install the command
pip install flechtctl           # or use it as a library
```

Python 3.12 or newer.

## Command line quick start

```bash
flechtctl keyring new keyring.json --kid prod-2026-10          # a keyring with one fresh 32-byte key
TOKEN=$(flechtctl encrypt -k keyring.json --scope api_key --string 's3cret')
flechtctl inspect "$TOKEN"                                     # the unauthenticated header: kid, scope, alg
flechtctl decrypt -k keyring.json --scope api_key --string "$TOKEN"
```

`-k/--keyring` is required for every command that needs key material: the command reads no
environment variables. `flechtctl keyring --help` lists the keyring commands — `new`, `add`,
`remove`, `primary`, `signing`, `show`, `export`, `import`, `protect` and `unprotect`. A keyring
file works like a keystore: a set of named entries, handed on one public half at a time with
`export` and `import`.

## Library quick start

The functions take the keyring explicitly — there is no global state:

```python
from pathlib import Path

from flechtctl import Keyring, decrypt, encrypt, sign, verify

keyring = Keyring.from_json(Path("keyring.json").read_text())

token = encrypt(keyring, "s3cret", scope="api_key")        # "flenc:jwe:…"
assert decrypt(keyring, token, scope="api_key") == "s3cret"

signed = sign(keyring, {"order": 17}, scope="orders")       # "flsig:jws:…" — needs a signing key for the scope
assert verify(keyring, signed, scope="orders") == {"order": 17}
```

`encrypt`, `decrypt`, `sign` and `verify` work on any JSON value; the `*_bytes` variants take
raw payload bytes. `reencrypt` moves a token to the scope's current sealing key, `read_header`
shows a token's unauthenticated header, and `generate_oct_key`, `generate_x25519_key` and
`generate_ed25519_key` mint key material.

## Equivalence with Flechtwerk's codecs

`flechtctl.encrypt(keyring, value, scope=s)` writes exactly the token that Flechtwerk's
`ENCRYPTED(inner, scope=s)` codec writes for `value == inner.encode(x)`, and every token
Flechtwerk writes opens here. For a `BYTES` field, pass the base64 string. Flechtwerk itself
is built on this package: `pip install "flechtwerk[secrets]"` installs the matching `flechtctl`.

## More

The guide [Keyrings and Secret Tokens with flechtctl](https://bsure-analytics.github.io/flechtwerk/guides/flechtctl/)
walks through a complete reader/writer setup, password-protected keyrings and the library API.
The format itself is specified on the
[Encrypted Secrets](https://bsure-analytics.github.io/flechtwerk/concepts/secrets/) page.

MIT licensed.
