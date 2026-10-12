---
branch: main
closeout_push: false
created: 2026-10-11
executor:
  effort: high
  model: claude-sonnet-5-5
  permission_mode: auto
permalink: none
plan_file: 2026-10-11-flechtctl-plan.md
planner:
  effort: high
  model: claude-opus-5-5
  permission_mode: auto
plugin: pdca
plugin_url: https://github.com/christian-schlichtherle/cs7-claude-plugins
plugin_version: 0.16.1
review_rounds: 10
sources: []
status: done
ticket: none
---

# Ship `flechtctl`: a standalone keyring and secret-token library with a CLI

## Goal

Today the `flenc:jwe:` / `flsig:jws:` secret-token format can only be produced and
read through `flechtwerk.secrets`, which imports aiokafka (`scan_config_topics`),
the `Record`/`Codec` machinery and — through `flechtwerk/__init__.py` — the whole
framework. A third-party app that only wants to seal a value therefore re-implements
the format by hand: `../../livia-nt/camera-inference/src/camera_inference/event/face_vector_cipher.py`
does exactly that with joserfc and keeps in sync with Flechtwerk through a shared
test-vector file. There is also no tool for creating or editing keyrings, or for
encrypting, decrypting, signing and verifying values while setting a deployment up
— `docs/concepts/secrets.md` even says "no CLI".

After this work there is a second distribution in this repository, **`flechtctl`**
(PyPI name, import name and console-script name are all `flechtctl`), that:

1. owns the keyring model (`Keyring`, `X25519Key`, `Ed25519Key`, their errors) and
   the token format (seal/open/sign/verify, the scope ratchet, key-binding checks,
   the pinned joserfc registries) — depending on **joserfc only**, never on
   flechtwerk;
2. offers a small explicit-keyring library API (`encrypt` / `decrypt` / `sign` /
   `verify` / `reencrypt` over JSON values, plus `*_bytes` variants, key generation,
   `Keyring.to_json()`) that third parties import instead of hand-rolling JOSE;
3. ships a `flechtctl` command (stdlib `argparse`) for keyring management, value
   crypto, and token tooling, runnable with `uvx flechtctl …` on any supported
   Python — including 3.15, where the full framework does not even install today.

`flechtwerk.keyring` and `flechtwerk.secrets` become thin layers over `flechtctl`
with **every existing public name, class identity, error message and wire byte
unchanged**: the existing test suite passes without edits. `flechtctl` stays
**install-optional** for flechtwerk: `flechtwerk[secrets]` pulls it (pinned to the
exact same version), and a plain `flechtwerk` install keeps working without it.

## Requirements

No requirements source — no ticket and no spec; the goal and the decisions below
came from the planning conversation. Decisions taken there, which phase 2 must not
revisit:

| # | Decision | Why |
|---|---|---|
| D1 | Name `flechtctl` for distribution, import package and command | `uvx X` runs distribution X's command X, so dist and command must be one word, and `flechtwerk` is the framework. User picked `flechtctl` over `flechtwerkctl`, `flenctl`, `flctl`. |
| D2 | `flechtctl` is optional for flechtwerk: only `flechtwerk[secrets]` depends on it | User decision. Costs a fallback seam in `flechtwerk/keyring.py` (task 3). |
| D3 | CLI scope: keyring management, value crypto, token tooling | User decision. **Out:** a Kafka config scan command (would need aiokafka). |
| D4 | Migrating `../../livia-nt/camera-inference` onto `flechtctl` is a follow-up, not this plan | Needs a PyPI release first (a tag push, which is the user's), and that repo has uncommitted work. This plan instead pins compatibility with its committed token vector (task 2). |
| D5 | No release: no tag, no push, no PyPI action | Releasing is the user's act; see "After this run" under Closeout. |
| D6 | A keyring file is managed like a Java keystore: a set of **named entries** (one per `kid`). Moving key material between parties is per entry: `keyring export` writes the public half of **one** entry as a single JWK and leaves the keyring untouched; `keyring import` adds one JWK entry to another keyring | User decision. Replaces an earlier draft's whole-keyring "public view" transform. |
| D7 | The CLI reads **no environment variables**: every crypto command requires `-k/--keyring PATH` | User decision: no hidden environment dependencies. |
| D8 | `Keyring`'s validation rules stay as they are — in particular a keyring must hold at least one key that can encrypt (oct or X25519 recipient), so a file holding only signature-checking keys stays invalid | Planner proposal, user did not ask for a change; relaxing it would edit a pinned test (`tests/keyring_test.py:56`). |
| D10 | Keyring files may be **password-protected, opt-in**: an *encrypted JWK Set* (RFC 7517 §7) — the JWK Set document as the plaintext of a compact JWE, `alg` `PBES2-HS512+A256KW`, `enc` `A256GCM`, `cty` `jwk-set+json`, `p2c` 210000. Chosen at creation (`--encrypt`), detected on read, kept protected on every edit; `keyring protect` / `keyring unprotect` convert. **flechtwerk keeps reading plain JSON only** — a running stage would need the password delivered beside the file, i.e. the same secret twice; the deployment copy is protected by the secret store. | User decision ("Opt-in, in this plan"). PKCS12 rejected: verified it cannot hold oct keys (no secret bags in `cryptography`) or X25519 keys, and would need a direct `cryptography` import. |
| D9 | Documentation is part of the work, not an afterthought: CLAUDE.md (task 5) and the user docs — PyPI README, a new guide, the secrets concept page, the API reference, the root README (task 6) | User reminder. |

## Verified Context

All commands below ran in the planning session, under permission mode `auto`, on
2026-10-11, in `/Users/christian/projects/bsure-analytics/flechtwerk`, and were
re-verified on 2026-10-12 at `e88d511` after main gained the MQTT/Kafka TLS work
(8 commits touching `module.py`, `mqtt.py`, `CLAUDE.md`, two guides and tests — none
of the files this plan moves or edits except `CLAUDE.md`, which changed 3 TLS lines:
`git diff 86bbe24 HEAD --stat -- src/flechtwerk/keyring.py src/flechtwerk/secrets.py src/flechtwerk/testing.py pyproject.toml uv.lock .github docs/concepts/secrets.md docs/api/index.md README.md mkdocs.yml`
prints nothing).

**Repository and baseline**

- Branch `main` at `e88d511`, clean tree apart from this plan file, latest tag `v0.14.0`:
  `git status --porcelain` (empty), `git tag --sort=-v:refname | head -1`.
- `uv run pytest -q` → `680 passed, 26 deselected` (~6 s).
- `uv run pytest -m integration -q` → `26 passed, 680 deselected` (~70 s, Docker
  reachable: `docker info`). On 2026-10-11 a first cold run once produced 16 setup
  **errors** (container start-up); the immediate re-run passed them all. Treat setup errors on a
  first run the same way: re-run once before concluding anything.
- `uv run poe docs-build` (mkdocs `--strict`) exits 0.
- `.venv` is CPython 3.14.7; uv's default interpreter on this machine is 3.15.0
  (`uv python find 3.15` → `~/.local/share/uv/python/cpython-3.15-macos-aarch64-none/bin/python3.15`).
- Commits are GPG-signed non-interactively: `git config commit.gpgsign` → `true`,
  signing key `4459B10B5535E920`, and
  `echo probe | gpg --batch --pinentry-mode error --local-user 4459B10B5535E920 --clearsign >/dev/null` succeeds.

**Current code (what moves where)**

- `src/flechtwerk/keyring.py` (479 lines): imports only stdlib + `.observer.Observer`.
  Holds `KEY_BYTES`, `KeyringError`, `KeyringConflictError`, `KeyringNotInstalledError`,
  `UnknownKeyError`, `_decode_key`, `Ed25519Key`, `X25519Key`, `Keyring` (validation in
  `__post_init__`, `of`, `from_json`, `key_for`, `recipient_for`, `signer_for`,
  `primary_pair`, `sealing_kid`, `signing_kid`, `kids`), `_decode_member`,
  `_parse_scopes`, `_parse_scope_map`, then the process runtime (`_keyring`,
  `_observer` ContextVar, `install_keyring`, `current_keyring`, `active_keyring`,
  `set_secret_observer`, `active_observer`, `_override_secret_runtime`,
  `_restore_secret_runtime`). There is **no** `Keyring.to_json`.
  `grep -nE "^(class|def) " src/flechtwerk/keyring.py`
- `src/flechtwerk/secrets.py` (769 lines) imports `aiokafka.AIOKafkaConsumer`,
  `joserfc` (`jwe`, `jws`, `OctKey`, `OKPKey`, `HeaderParameter`),
  `flechtwerk.attribute`, `.kafka`, `.types.Config`, `.keyring`. Format constants
  `SCHEME`/`PREFIX`/`SIGNATURE_SCHEME`/`SIGNATURE_PREFIX` and the private `_ALG`,
  `_ENC`, `_FLENC_SCOPE`, `_RECIPIENT_ALG`, `_SIGNATURE_ALG`, `_HEADER_REGISTRY`,
  `_REGISTRY`, `_RECIPIENT_REGISTRY`, `_SIGNATURE_REGISTRY` (lines 96–145); the
  error family `SecretError`, `SecretFormatError`, `SecretDecryptError`,
  `PlaintextSecretError`, `SecretSignatureError`, `UnsignedSecretError` (150–260);
  `is_encrypted`, `is_signed`, `_strip_envelope`, `_compact`, `_signature_compact`,
  `_read_kid`, `kid_of`, `_b64url`, `_okp_private_key` (`functools.cache`d),
  `_encrypt_payload`, `_decrypt_to_payload` (fires
  `active_observer().secret_decrypted(scope, kid)`), `_verify_to_payload` (fires
  `secret_verified`), `_canonical_json`; then the flechtwerk-only part:
  `_EncryptedCodec`, `ENCRYPTED`, `_SignedCodec`, `SIGNED` (whose `encode` signs
  inline), `_require_encrypted`, `encrypt_value`, `signer_of`, `sign_value`,
  `reencrypt`, `ScanEntry`, `scan_config_topics`.
  `grep -nE "^(class|def|[A-Z_]+ *=)" src/flechtwerk/secrets.py`
- `_decrypt_to_payload` calls `active_keyring()` **before** looking at the token, so a
  missing keyring wins over any token-shape complaint (comment at its first line).
- `src/flechtwerk/module.py:43` imports `Keyring, install_keyring, set_secret_observer`
  from `.keyring`; `:433` declares `keyring: lookup[Keyring | None]` — reactor-di
  resolves this annotation at decoration time, so `Keyring` must be a
  runtime-importable name even without `flechtctl` installed.
- `src/flechtwerk/testing.py:15` imports `Keyring` (and more) from `flechtwerk.keyring`;
  `fixture_keyring()` = `Keyring.of({"test-key": bytes(range(32))}, primary="test-key")`.
- Existing tests reach only public names, plus `tests/conftest.py`'s
  `_override_secret_runtime` / `_restore_secret_runtime` and
  `tests/integration/config_topics_test.py`'s `current_keyring` — all of which stay in
  `flechtwerk.keyring`. `tests/secrets_test.py` imports `joserfc` directly and holds
  `test_importing_flechtwerk_does_not_load_joserfc` (subprocess:
  `import flechtwerk.module` must leave `joserfc` out of `sys.modules`).
- Import costs: `joserfc.jwe` ≈ 257 ms cold, `import flechtwerk` ≈ 394 ms
  (`python -X importtime`). Keeping joserfc lazy is worth it.

**Packaging (proved by a spike in a scratch clone — not in this repository)**

The spike: `git clone --local` of this repository into the session scratchpad, the
workspace member and `hatch_build.py` below added, an annotated tag `v0.15.0`, then
`uv build --all-packages -o out`, `unzip -p out/flechtwerk-*.whl "*/METADATA" | grep -E "^(Name|Version|Requires-Dist: (flechtctl|joserfc))"`,
`uv sync --python 3.14 --all-extras`, `uv run flechtctl`, `uv lock`, and
`uvx --python 3.15 --from out/flechtctl-0.15.0-py3-none-any.whl flechtctl`. Task 1
re-proves every one of these facts in this repository; nothing depends on the clone.

- Root `pyproject.toml`: hatchling + hatch-vcs, `dynamic = ["version"]`,
  `[project.optional-dependencies] mqtt = ["paho-mqtt~=2.1"]`,
  `secrets = ["joserfc~=1.7"]`; dev group includes `joserfc~=1.7` and
  `cryptography>=44`; docs group includes `joserfc~=1.7`; `testpaths = ["tests"]`;
  `[tool.coverage.run] source = ["src"]`; poe task `build = "uv build"`.
- A uv workspace member works: root `[tool.uv.workspace] members = ["flechtctl"]` +
  `[tool.uv.sources] flechtctl = { workspace = true }`, member at
  `flechtctl/pyproject.toml` with `[tool.hatch.version] source = "vcs"` and
  `raw-options = { root = ".." }`. `uv sync` then installs both editable;
  `uv run flechtctl` runs the console script.
- `uv build --all-packages -o <dir>` builds sdist + wheel for **both**, each versioned
  from the same tag (`0.15.0` at a tag; `0.15.1.dev1+g<sha>` for both between tags).
- An exact pin for the extra is possible with a hatch **metadata hook**: root
  `dynamic = ["optional-dependencies", "version"]`, the static
  `[project.optional-dependencies]` table removed, an empty
  `[tool.hatch.metadata.hooks.custom]` table, and a root `hatch_build.py`:

  ```python
  from hatchling.metadata.plugin.interface import MetadataHookInterface


  class CustomMetadataHook(MetadataHookInterface):
      def update(self, metadata):
          metadata["optional-dependencies"] = {
              "mqtt": ["paho-mqtt~=2.1"],
              "secrets": [f"flechtctl=={metadata['version']}"],
          }
  ```

  The built flechtwerk wheel's METADATA then carries
  `Requires-Dist: flechtctl==<its own version>; extra == 'secrets'` and no joserfc
  line; the flechtctl wheel carries only `Requires-Dist: joserfc~=1.7`.
- Locally, `uv lock` and `uv sync --all-extras` resolve cleanly with that pin even when
  the editable installs' recorded versions differ (uv takes workspace members as-is).
  That staleness is cosmetic; only built wheels carry the real pin.
- `uvx --python 3.15 --from <path to flechtctl wheel> flechtctl` installs 5 packages and
  runs. By contrast `uvx --from 'flechtwerk[secrets]==0.14.0' …` **fails** on 3.15
  (rocksdict has no cp315 wheel) and costs 13 packages / 61 MB / 3.6 s on 3.14.
- `pip`/`uv` keep accepting `flechtwerk[mqtt,secrets]==0.13.0`-style pins: the
  `secrets` extra still exists, it just points at `flechtctl` now.

**Import machinery for the fallback seam**

- With `flechtctl` truly absent, `from flechtctl.keyring import Keyring` raises
  `ModuleNotFoundError` with `e.name == "flechtctl"`; when simulated with
  `sys.modules["flechtctl"] = None`, `e.name == "flechtctl.keyring"`. A guard must
  therefore accept any `e.name` whose first dotted component is `flechtctl`.

**camera-inference compatibility vector** (re-verified 2026-10-12 — it changed overnight)

- `../../livia-nt/camera-inference/tests/fixtures/face_vector_token.txt` is one line
  (3987 characters plus a newline, 3988 bytes), sha256
  `16ee84502b81d4a931aaff83df9ededee868ddf5c521a97cbedd38e9ca4e738b`, last changed by
  livia-nt commit `042da99` ("face_vector is sealed to one recipient key per
  environment, and a box holds only its public half"). It is a **recipient** token:
  protected header keys exactly `alg, enc, epk, flenc_scope, kid` with
  `alg` `ECDH-ES+A256KW`, `enc` `A256GCM`, `kid` `test-recipient`, `flenc_scope`
  `face_vector`.
- It decrypts as `ENCRYPTED(BYTES, scope="face_vector")` to **2048 bytes** under
  `Keyring.of({}, primary="test-recipient", recipients={"test-recipient": X25519Key(public=<base64url "j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8">, private=bytes(range(32)))}, scopes={"test-recipient": {"face_vector"}})`
  — the same X25519 material as flechtwerk's `fixture_asymmetric_keyring()`, bound to
  `face_vector` instead of `api_key`. (Verified by decrypting it with today's
  flechtwerk.) Its writer seals the canonical JSON of the vector's base64 **string**.
- Reading the file from this repository works under `auto`
  (`shasum -a 256 ../../livia-nt/camera-inference/tests/fixtures/face_vector_token.txt`).

**Password-protected keyrings (verified in joserfc 1.7.4)**

- `jwe.encrypt_compact({"alg": "PBES2-HS512+A256KW", "enc": "A256GCM", "cty": "jwk-set+json", "p2c": 210000}, payload, OctKey.import_key(password_bytes), registry=jwe.JWERegistry(algorithms=["PBES2-HS512+A256KW", "A256GCM"]))`
  round-trips through `jwe.decrypt_compact(token, key, registry=…)` in ≈0.18 s.
- joserfc generates the salt itself: two encryptions carry different 16-byte `p2s`
  values. A wrong password raises `joserfc.errors.DecodeError`.
- joserfc **rejects** `p2c` ≥ 300000 (`InvalidHeaderValueError: 'p2c' in header must be
  less than 300000`), hence 210000 (OWASP's figure for PBKDF2-HMAC-SHA512).
- `OctKey.import_key` emits `SecurityWarning: Key size should be >= 112 bits` for a
  password shorter than 14 bytes — the CLI refuses such passwords before joserfc sees
  them, so the warning never fires.
- `cryptography`'s `pkcs12.PKCS12PrivateKeyTypes` = RSA, DSA, EC, Ed25519, Ed448 (no
  X25519) and the module has no secret-bag API: PKCS12 cannot carry this keyring.

**Docs**

- `mkdocs.yml`: mkdocstrings `paths: [src]`; nav has Guides (index, getting-started,
  extractor, mqtt, transformer, best-practices, observability), Concepts (…,
  `concepts/secrets.md`, …), API Reference.
- `docs/api/index.md:58-76` documents `flechtwerk.secrets.ENCRYPTED`, `SIGNED`,
  `flechtwerk.keyring.Keyring`, `X25519Key`, `Ed25519Key`, `encrypt_value`, … via
  `:::` blocks.
- `docs/concepts/secrets.md` "## Tooling" (line 547) opens "`flechtwerk.secrets` ships
  the primitives the runbooks build on (no CLI — those belong in application tooling
  per the framework's boundary rule)". "## Where It Lives" (742) and the install note
  (lines 17–21) describe joserfc confinement.
- `README.md:33` — `pip install "flechtwerk[secrets]" # with field-level secret encryption (joserfc)`.
- `.github/workflows/publish.yaml` runs `uv build` then `uv publish`;
  `.github/workflows/test.yaml` runs `uv sync` then `uv run poe test-all`.

## Constraints & Non-Goals

- **Wire format is frozen.** Same prefixes, headers, registries, allowlists, canonical
  JSON, scope ratchet, binding checks. Move the existing code; do not rewrite the
  logic. Error messages move **verbatim** (existing tests match on them), including
  wording such as "in the active keyring". That wording will surface in CLI errors
  too (e.g. `sign` with no signing key for the scope) — accepted knowingly; the CLI
  prints the message as it is.
- **Existing tests are not edited.** No file that exists under `tests/` today may be
  modified (acceptance criterion 2 checks this). New test files are fine.
- **Public surface of flechtwerk unchanged.** `flechtwerk.keyring.__all__` and
  `flechtwerk.secrets.__all__` keep exactly their current names; re-exported classes
  are the *same objects* as in `flechtctl` (`flechtwerk.keyring.Keyring is flechtctl.Keyring`).
- **`flechtctl` never imports `flechtwerk`** (not in `src`, not in its tests) and
  declares exactly one runtime dependency, `joserfc~=1.7`.
- **joserfc stays lazy on both sides:** `import flechtctl` and `import flechtwerk.module`
  (with flechtctl installed) must not load `joserfc`. Only `flechtctl.tokens` imports
  it, and only `flechtwerk.secrets` imports `flechtctl.tokens` (directly or via a
  lazy `flechtctl` attribute).
- **No direct `cryptography` import** in either `src/` tree — OKP keys go through
  joserfc's key objects (existing repo rule).
- **The process runtime stays in flechtwerk:** `install_keyring`, `current_keyring`,
  `active_keyring`, `set_secret_observer`, `active_observer`, the `_override`/`_restore`
  pair, `KeyringConflictError`, `KeyringNotInstalledError`. `flechtctl` has no
  process-global state; every function takes the keyring explicitly.
- Code style per `CLAUDE.md`: alphabetical ordering of imports, `__all__` entries,
  dict keys; `__all__` declared in every flechtctl module (empty with a comment for
  purely internal ones); comprehensions over accumulator loops; `X | None`; Markdown
  headings followed by a blank line; files end with a newline.
- **Commits:** this plan authorizes committing (overriding CLAUDE.md's "don't commit
  automatically" for this run only), always signed (`git commit -S`), no ticket
  prefix (matching `git log`), on `main`. **Never push, never tag.**
- Out of scope: a `scan` CLI command; changing `Keyring`'s validation rules (e.g. a
  keyring with only signing keys stays invalid — rule 1); any change in
  `../../livia-nt`; any release.

## Pre-Flight

Before check 1: if the frontmatter carries a `blocked_report`, this is a relaunch.
Fold that report into the Run Log below — its `date`, the check it named, what was
tried, its `next` — then delete it and remove `blocked_report` from the frontmatter,
in this same turn, and only then run the checks. An inherited report never satisfies
this plan's goal and is never left in place.

1. **Driven by this plan's /goal; model, effort and permission mode.**

   ```bash
   T=$(ls -t "$HOME"/.claude/projects/*/"$CLAUDE_CODE_SESSION_ID".jsonl | head -1)
   echo "T=$T"
   G=$(grep '"type":"goal_status"' "$T" | tail -1)
   echo "$G" | grep -o '"met":[a-z]*'
   echo "$G" | grep -o '"condition":"[^"]\{0,120\}'
   grep '"type":"assistant"' "$T" | tail -1 | grep -o '"model":"[^"]*"' | head -1
   grep -o '"permissionMode":"[^"]*"' "$T" | tail -1
   echo "effort=$CLAUDE_EFFORT"
   ```

   Expected: `"met":false` with a condition naming `2026-10-11-flechtctl-plan.md`;
   `"model":"claude-sonnet-5-5"`, `"permissionMode":"auto"`, `effort=high`. A last
   record with `"met":true`, a different goal's condition, or no record at all in a
   readable transcript mean no evaluator guards this run: stop and write the blocked
   report; the fix is relaunching with the Handoff command. If the `T=` line prints
   empty, the transcript could not be found: record that in the Run Log, take the
   model from what this session states about itself, the effort from
   `CLAUDE_EFFORT`, the permission mode from the launch flags (walk up from `$PPID`
   until the argv starts with `claude`, match `--permission-mode <mode>`), treat the
   goal check as unverifiable, and continue. If the `permissionMode` line is empty in a
   readable transcript, use the same launch-flag fallback; if neither answers, say the
   mode is unverifiable rather than assuming it.

2. **Privileges** — skip only if the mode above is `bypassPermissions`, and say so in
   the Run Log.
   - Signing works without a prompt:
     `echo probe | gpg --batch --pinentry-mode error --local-user 4459B10B5535E920 --clearsign >/dev/null && echo SIGN-OK`
     prints `SIGN-OK`. If it fails, the gpg-agent cache has expired — blocked
     report, `next: relaunch`, telling the user to unlock the key (e.g. sign anything
     once in a terminal) before relaunching.
   - Docker answers: `docker info >/dev/null && echo DOCKER-OK` prints `DOCKER-OK`
     (integration tests need it).
   - Python 3.15 is available to uv: `uv python find 3.15` prints a path.
   - The sibling repository's fixture can be copied (task 2 needs it):
     `D=$(mktemp -d) && cp ../../livia-nt/camera-inference/tests/fixtures/face_vector_token.txt "$D/probe" && shasum -a 256 "$D/probe" && echo COPY-OK`
     prints a hash and `COPY-OK`. A denial is a pre-flight failure (blocked report,
     `next: relaunch`). A different hash is not: task 2 says how to handle it.

3. **Ground.**
   - This file exists at `2026-10-11-flechtctl-plan.md`.
   - `git branch --show-current` prints `main`, and `git status --porcelain` is empty —
     or this is a resume: the frontmatter says `status: executing` or
     `status: blocked`, and the only changes are this file, the blocked report just
     consumed, and files the Run Log names as work in progress.

4. **Preconditions from the Verified Context.**
   - `test -e flechtctl && echo EXISTS || echo ABSENT` prints `ABSENT` (on a fresh
     run; on a resume, the Run Log explains what exists).
   - `uv run pytest -q 2>&1 | tail -1` reports `680 passed` and no failures (on a
     fresh run).
   - `grep -c "def to_json" src/flechtwerk/keyring.py` prints `0`.

## Tasks

Order matters: task 1 creates the package skeleton and workspace wiring that every
later task imports; task 2 moves the core before task 3 rewires flechtwerk onto it;
the CLI (task 4) uses the finished library API; docs (task 6) describe what exists.
Run `uv run pytest -q` after every task — it must stay green (plus new tests).

- [x] **1. Create the `flechtctl` workspace member and wire packaging**
  - Create:
    - `flechtctl/pyproject.toml` — hatchling + hatch-vcs build system;
      `[project]` with `name = "flechtctl"`, `dynamic = ["version"]`,
      `dependencies = ["joserfc~=1.7"]`, `requires-python = ">=3.12"`,
      `description` (one line: keyrings and `flenc:`/`flsig:` secret tokens for
      Flechtwerk, without the framework), `readme = "README.md"`, `license`,
      `authors`, `maintainers`, `classifiers` (Python 3.12/3.13/3.14, MIT, "Topic ::
      Security :: Cryptography"), `keywords`, `[project.urls]` copied from the root
      where they apply; `[project.scripts] flechtctl = "flechtctl.cli:main"`;
      `[tool.hatch.build.targets.wheel] packages = ["src/flechtctl"]`;
      `[tool.hatch.version] source = "vcs"`, `raw-options = { root = ".." }`.
      Keys alphabetical, like the root file.
    - `flechtctl/README.md` — filled in task 6; for now a heading and one sentence.
    - `flechtctl/src/flechtctl/__init__.py`, `flechtctl/src/flechtctl/py.typed` (empty),
      `flechtctl/tests/` (test files land in tasks 2 and 4).
  - Root `pyproject.toml`:
    - `dynamic = ["optional-dependencies", "version"]`; delete the static
      `[project.optional-dependencies]` table; add `[tool.hatch.metadata.hooks.custom]`
      (empty table) and the root `hatch_build.py` exactly as shown in the Verified
      Context (it must keep `mqtt = ["paho-mqtt~=2.1"]`).
    - `[tool.uv.workspace] members = ["flechtctl"]` and
      `[tool.uv.sources] flechtctl = { workspace = true }`.
    - dev group: add `"flechtctl"` (keep `joserfc~=1.7` — `tests/secrets_test.py`
      imports joserfc directly — and `cryptography>=44`); docs group: replace
      `joserfc~=1.7` with `"flechtctl"` and fix its comment.
    - `testpaths = ["flechtctl/tests", "tests"]`;
      `[tool.coverage.run] source = ["flechtctl/src", "src"]`;
      poe `build = { cmd = "uv build --all-packages", … }`.
  - `.github/workflows/publish.yaml`: `uv build` → `uv build --all-packages`
    (`uv publish` uploads everything in `dist/`).
  - `uv sync` and commit `uv.lock` with this task.
  - Test-file naming: pytest's default rootdir import mode needs unique basenames
    across `tests/` and `flechtctl/tests/`, so every flechtctl test file is named
    `ctl_<topic>_test.py`.
  - Verify: `uv sync` exits 0; `uv run python -c "import flechtctl; print(flechtctl.__file__)"`
    prints a path under `flechtctl/src`; `uv build --all-packages -o build/spike`
    builds four artifacts and the flechtwerk wheel's METADATA has
    `Requires-Dist: flechtctl==<same version>; extra == 'secrets'`
    (`unzip -p build/spike/flechtwerk-*.whl "*/METADATA" | grep -E "^(Version|Requires-Dist: (flechtctl|joserfc))"`);
    then `rm -rf build/spike`. `uv run pytest -q` still 680 passed. After the task
    commit, `uv lock --check` exits 0 (the lock must not churn per commit).
  - Commit: "Add the flechtctl workspace member and pin it from the secrets extra".

- [x] **2. Move the keyring model and the token format into `flechtctl`**
  - `flechtctl/src/flechtctl/keyring.py` (joserfc-free, stdlib only): move from
    `flechtwerk/keyring.py`, verbatim, `KEY_BYTES`, `KeyringError`, `UnknownKeyError`,
    `_decode_key`, `_B64URL_TO_STD`, `Ed25519Key`, `X25519Key`, `Keyring` (all
    methods), `_decode_member`, `_parse_scopes`, `_parse_scope_map`. Adapt only the
    module docstring (drop the process-global and reactor-di paragraphs; say it is
    pure key material with no crypto and no joserfc). `__all__ = ["Ed25519Key",
    "KEY_BYTES", "Keyring", "KeyringError", "UnknownKeyError", "X25519Key",
    "is_encrypted_keyring", "keyring_from_jwk"]`. In `from_json`, before
    `json.loads`: if `is_encrypted_keyring(text)`, raise
    `KeyringError("keyring document is password-protected — open it with flechtctl (decrypt_keyring) or write a plain copy with flechtctl keyring unprotect")`.
    Add to `Keyring`:
    - `to_json(self) -> str` — the inverse of `from_json`: a JWK Set document,
      `json.dumps(doc, indent=2, sort_keys=True) + "\n"`; `keys` sorted by `kid`;
      oct keys `{"k", "kid", "kty": "oct"}`; OKP keys `{"crv": "X25519"|"Ed25519",
      "kid", "kty": "OKP", "x"}` plus `"d"` when the private half is present; a
      `flenc_scopes` member (sorted list) only for bound kids; top-level `primary`;
      `flenc_primaries` / `flenc_signing` only when non-empty. Base64url unpadded
      (reuse the encoding `_b64url` in secrets uses). Property:
      `Keyring.from_json(k.to_json()) == k` for every valid keyring.
    - Per-entry JWK access (the keystore model, decision D6). First refactor the body
      of `from_json`'s per-key loop into a private module function
      `_parse_jwk(jwk: object) -> tuple[str, str, bytes | X25519Key | Ed25519Key, frozenset[str] | None]`
      returning `(kind, kid, material, scopes)` with kind `"oct"`, `"X25519"` or
      `"Ed25519"`, raising exactly the `KeyringError`s the loop raises today (same
      messages); `from_json` keeps its duplicate-kid check and calls it. Preserve
      today's check order — `kty`, `kid`, duplicate kid, scopes, key material — so
      a duplicate kid with malformed material still reports the duplicate: e.g.
      `_parse_jwk(jwk, held=frozenset(...))` raising the duplicate error right after
      the `kid` check (`with_jwk` passes its own kids the same way). Then add:
      - `public_jwk(self, kid: str) -> dict` — the public half of entry `kid` as one
        JWK dict: `{"crv", "kid", "kty": "OKP", "x"}` plus `flenc_scopes` (sorted
        list); never `d`. Raises `UnknownKeyError(kid)` for an unknown kid and
        `KeyringError(f"key {kid!r} is a symmetric (oct) key and has no public half")`
        for an oct entry. The keyring itself is unchanged (it is frozen anyway).
      - `with_jwk(self, jwk: dict) -> "Keyring"` — a new keyring with one more entry
        parsed by `_parse_jwk` (private half kept if present, scopes kept); a kid
        already held raises `KeyringError(f"duplicate kid {kid!r} in the keyring")`.
        Built through the constructor, so all validation runs.
      - module function `keyring_from_jwk(jwk: dict) -> Keyring` (in `__all__`) — a
        one-entry keyring whose `primary` is that entry; for a new keyring file
        created by `keyring import` (an Ed25519 entry cannot be primary — the
        constructor's own error says so).
  - `flechtctl/src/flechtctl/errors.py` (joserfc-free): move the six `SecretError`
    classes verbatim. `__all__` lists them.
  - `flechtctl/src/flechtctl/tokens.py` (the only module importing joserfc): move
    the format constants, registries, `is_encrypted`, `is_signed`, `_strip_envelope`,
    `_compact`, `_signature_compact`, `_read_kid`, `kid_of`, `signer_of`, `_b64url`,
    `_okp_private_key`, and the canonical JSON helper (public as `canonical_json`).
    Turn the payload helpers into explicit-keyring public functions (same bodies,
    `keyring` parameter instead of `active_keyring()`, **no observer calls**):
    - `encrypt_bytes(keyring: Keyring, payload: bytes, *, scope: str = "") -> str`
    - `decrypt_bytes(keyring: Keyring, token: str, *, scope: str = "", topic: str | None = None, wire_key: str | None = None) -> bytes`
    - `sign_bytes(keyring: Keyring, payload: bytes, *, scope: str) -> str` (the body of
      `SIGNED.encode` after `inner.encode`)
    - `verify_bytes(keyring: Keyring, token: str, *, scope: str) -> bytes`
    - `sign_bytes` / `verify_bytes` raise `ValueError` on an empty scope (same text as
      `SIGNED`'s: "… needs a non-empty scope — a signature always names the
      compartment it vouches for", with `sign`/`verify` as the subject).
    - JSON level: `encrypt(keyring, value, *, scope="") -> str` =
      `encrypt_bytes(keyring, canonical_json(value), scope=scope)`; `decrypt(keyring,
      token, *, scope="") -> object` = `json.loads(decrypt_bytes(…))`; likewise `sign`
      and `verify`.
    - `reencrypt(keyring, token, *, scope="") -> str` =
      `encrypt_bytes(keyring, decrypt_bytes(keyring, token, scope=scope), scope=scope)`
      (exact inner bytes preserved).
    - Password-protected keyring documents (decision D10), pinned to one registry
      `jwe.JWERegistry(algorithms=["PBES2-HS512+A256KW", "A256GCM"])`, its own
      constant, separate from the token registries:
      - `encrypt_keyring(keyring: Keyring, password: str) -> str` — compact JWE over
        `keyring.to_json()`, header exactly `{"alg": "PBES2-HS512+A256KW", "cty":
        "jwk-set+json", "enc": "A256GCM", "p2c": 210000}` (joserfc adds `p2s`).
        Raises `KeyringError` for a password shorter than 14 bytes in UTF-8 ("a keyring
        password must be at least 14 bytes").
      - `decrypt_keyring(text: str, password: str) -> Keyring` — any failure
        (malformed, wrong algorithm, wrong password) raises `KeyringError`
        ("cannot open the password-protected keyring — wrong password or damaged
        file"), chaining the cause.
      - `is_encrypted_keyring(text: str) -> bool` — true when the stripped text is a
        five-segment compact JWE whose header `cty` is `jwk-set+json`; never raises.
        It lives in `keyring.py` (joserfc-free, in that module's `__all__`) so
        `Keyring.from_json` can use it; `tokens.py` imports it from there.
    - `read_header(token: str) -> dict` — the decoded, **unauthenticated** protected
      header of a `flenc` or `flsig` token (no key; `SecretFormatError` for anything
      else or a malformed header). Generalise `_read_kid` on top of it.
    - Key generation: `generate_oct_key() -> bytes` (`secrets.token_bytes(KEY_BYTES)`),
      `generate_x25519_key() -> X25519Key` and `generate_ed25519_key() -> Ed25519Key`
      via `joserfc.jwk.OKPKey.generate_key("X25519" | "Ed25519")` and its
      `as_dict(private=True)` `x` / `d` members decoded to raw bytes (verified:
      `as_dict(private=True)` yields keys `crv, d, kty, x`).
    - `__all__` lists the public names alphabetically.
  - `flechtctl/src/flechtctl/__init__.py`: eagerly re-export everything from
    `keyring` and `errors`; expose every `tokens` name **lazily** through a module
    `__getattr__` (PEP 562) that imports `.tokens` on first access, so
    `import flechtctl` loads no joserfc. `__all__` = all public names of the three
    modules, alphabetical. Module docstring: what the package is, the explicit-keyring
    API, and that `flechtctl.encrypt(k, v, scope=s)` writes the same token
    `ENCRYPTED(inner, scope=s)` writes for `v == inner.encode(x)` (e.g. a `BYTES`
    field: pass the base64 string).
  - Tests (`flechtctl/tests/`, none importing flechtwerk):
    - `ctl_keyring_test.py` — validation rules reach via `Keyring.of`/`from_json`
      (a representative subset; the exhaustive ones stay in flechtwerk's suite),
      `to_json` round-trip for an oct-only, a recipient and an all-kinds keyring,
      `to_json` output stable (sorted, newline-terminated); `from_json` error messages
      unchanged after the `_parse_jwk` refactor; `public_jwk` (no `d`, scopes kept,
      `UnknownKeyError` for an unknown kid, `KeyringError` for an oct entry, keyring
      unchanged); `with_jwk` (adds an entry, rejects a duplicate kid, runs
      validation); `keyring_from_jwk` (recipient entry becomes primary; Ed25519
      entry rejected by validation); `Keyring.from_json` on a password-protected
      document raises `KeyringError` whose message says it is password-protected.
    - `ctl_keyring_file_test.py` — `encrypt_keyring`/`decrypt_keyring` round-trip
      (equal `Keyring`), the exact protected header, fresh `p2s` per call, wrong
      password → `KeyringError`, short password → `KeyringError`, a token sealed with
      another `alg` (e.g. `PBES2-HS256+A128KW`) → `KeyringError`,
      `is_encrypted_keyring` true/false cases.
    - `ctl_tokens_test.py` — encrypt/decrypt round-trips (oct `dir`, X25519 recipient,
      scoped, unscoped), the scope ratchet both directions, sign/verify incl. wrong
      scope and unknown signer, `reencrypt` dir→recipient keeps exact payload bytes,
      `read_header`, generators produce valid 32-byte material usable in a keyring,
      the **camera-inference vector**: copy
      `../../livia-nt/camera-inference/tests/fixtures/face_vector_token.txt` byte for
      byte to `flechtctl/tests/fixtures/face_vector_token.txt` and first check its
      sha256 against the Verified Context. If it differs, the sibling project changed
      its format again: record that in the Run Log, and use the new file only if it
      still decrypts as below (adapting the expected header to what it carries);
      otherwise stop with a blocked report (`next: reopen`). Put the provenance (path,
      livia-nt commit, sha256) in a comment in the test, not in the fixture file. Assert,
      with the recipient keyring from the Verified Context (`RECIPIENT`):
      `len(base64.b64decode(decrypt(RECIPIENT, TOKEN, scope="face_vector"))) == 2048`;
      `sorted(read_header(TOKEN)) == ["alg", "enc", "epk", "flenc_scope", "kid"]` with
      `alg` `ECDH-ES+A256KW`, `kid` `test-recipient`, `flenc_scope` `face_vector`; and
      that a fresh `encrypt(RECIPIENT, "AAAA", scope="face_vector")` carries the same
      header shape (same key set and values, its own `epk`). Also one `dir` case:
      `read_header(encrypt(Keyring.of({"test-key": bytes(range(32))}, primary="test-key"), "AAAA", scope="face_vector"))`
      equals exactly `{"alg": "dir", "enc": "A256GCM", "flenc_scope": "face_vector", "kid": "test-key"}`.
    - `ctl_hygiene_test.py` — (a) subprocess `import flechtctl` leaves `joserfc` out of
      `sys.modules`, and accessing `flechtctl.encrypt` then loads it; (b) an AST scan
      of every `flechtctl/src/flechtctl/*.py` finds no import of `flechtwerk` and no
      import of `cryptography`, and finds `joserfc` imported only in `tokens.py`; the
      same scan over `flechtctl/tests/*.py` finds no import of `flechtwerk`;
      (c) `flechtctl`'s declared `Requires-Dist` (via `importlib.metadata.requires`) is
      exactly `["joserfc~=1.7"]`.
  - Do not touch `src/flechtwerk` in this task — task 3 rewires it. Both copies of
    the code coexist for one commit; the suite stays green.
  - Commit: "Move the keyring model and the token format into flechtctl".

- [x] **3. Rewire `flechtwerk.keyring` and `flechtwerk.secrets` onto `flechtctl`**
  - `src/flechtwerk/keyring.py`: delete the moved definitions; import them:

    ```python
    try:
        from flechtctl.keyring import KEY_BYTES, Ed25519Key, Keyring, KeyringError, UnknownKeyError, X25519Key
    except ModuleNotFoundError as e:
        if e.name is None or e.name.partition(".")[0] != "flechtctl":
            raise
        # flechtwerk without the [secrets] extra: placeholders so module.py's
        # keyring slot still resolves at decoration time ...
    ```

    In the `except` branch define, locally and minimally: `KEY_BYTES = 32`;
    `class KeyringError(Exception)`; `class UnknownKeyError(KeyringError)` (same
    `__init__(self, kid)` shape); and placeholder classes `Keyring`, `Ed25519Key`,
    `X25519Key` whose `__init__`, `of` and `from_json` all raise
    `ModuleNotFoundError("<name> needs the flechtctl package — install flechtwerk[secrets]", name="flechtctl")`.
    Keep the comment short and say why (reactor-di resolves the annotation at
    decoration time; a keyring can only be built with flechtctl installed).
    `KeyringConflictError(KeyringError)`, `KeyringNotInstalledError(KeyringError)`
    and the whole runtime stay here unchanged. `__all__` unchanged. Rewrite the module
    docstring: the `Keyring` value object now lives in `flechtctl`; this module is the
    process runtime and the joserfc-free seam `module.py` imports.
  - `src/flechtwerk/secrets.py`: delete the moved definitions and import them from
    `flechtctl` (`errors`, `keyring`, `tokens`). Guard the import: a
    `ModuleNotFoundError` whose `e.name` starts with `flechtctl` or `joserfc` is
    re-raised as `ModuleNotFoundError("flechtwerk.secrets needs the flechtctl package — install flechtwerk[secrets]", name=e.name)`
    `from e`. Keep the private wrappers so callers inside the module are unchanged:
    - `_encrypt_payload(scope, payload)` → `encrypt_bytes(active_keyring(), payload, scope=scope)`
    - `_decrypt_to_payload(scope, token, *, topic=None, wire_key=None)` →
      `keyring = active_keyring()` **first**, then `decrypt_bytes(keyring, token, …)`,
      then `active_observer().secret_decrypted(scope, kid_of(token))`, return payload
    - `_verify_to_payload(scope, token)` → `active_keyring()` first, `verify_bytes`,
      then `active_observer().secret_verified(scope, signer_of(token))`
    - `SIGNED.encode` → `sign_bytes(active_keyring(), canonical_json(inner.encode(value)), scope=scope)`
    - `ENCRYPTED`, `SIGNED`, `encrypt_value`, `sign_value`, `reencrypt`, `ScanEntry`,
      `scan_config_topics` keep their bodies otherwise. `__all__` unchanged; the
      re-exported names must be the flechtctl objects themselves. Update the module
      docstring: the format lives in `flechtctl`; this module adds the codecs, the
      process keyring, telemetry and the Kafka scan.
  - New test `tests/flechtctl_interop_test.py` (new file; existing ones untouched):
    - identity: `flechtwerk.keyring.Keyring is flechtctl.Keyring`, same for every
      re-exported class and function (`SecretError`, `kid_of`, `PREFIX`, …);
    - `flechtctl.encrypt(k, "s", scope="x")` decodes through
      `Attribute("a", ENCRYPTED(STR, scope="x"))` under `installed_keyring(k)`, and the
      reverse; same for `ENCRYPTED(BYTES)` (base64 string) and `ENCRYPTED(RECORD)`
      (dict);
    - `SIGNED` ↔ `flechtctl.sign`/`verify`; `ENCRYPTED(SIGNED(STR, scope=s), scope=s)`
      ↔ `flechtctl.encrypt(k, flechtctl.sign(k, v, scope=s), scope=s)`;
    - the observer still fires: `RecordingObserver` sees `secret_decrypted` /
      `secret_verified` with the right `(scope, kid)` after a codec decode;
    - **without flechtctl**: a subprocess running
      `import sys; sys.modules["flechtctl"] = None` then `import flechtwerk, flechtwerk.module, flechtwerk.testing`
      succeeds; `flechtwerk.keyring.Keyring.of({}, primary="x")` raises
      `ModuleNotFoundError` whose message contains `flechtwerk[secrets]`;
      `import flechtwerk.secrets` raises `ModuleNotFoundError` whose message contains
      `flechtwerk[secrets]`.
  - Verify: `uv run pytest -q` — all pre-existing tests pass unchanged (incl.
    `test_importing_flechtwerk_does_not_load_joserfc`) plus the new ones;
    `git diff --diff-filter=M --name-only e88d511 -- tests` prints nothing.
  - Commit: "Build flechtwerk.keyring and flechtwerk.secrets on flechtctl".

- [x] **4. The `flechtctl` command**
  - `flechtctl/src/flechtctl/cli.py` (`__all__ = ["main"]`) + `__main__.py`
    (`python -m flechtctl`). Stdlib `argparse` with subparsers; no other dependency.
    `main(argv: list[str] | None = None) -> int`; the console script calls
    `sys.exit(main())`-equivalent (hatch entry point `flechtctl.cli:main` → make `main`
    return an int and let the entry point's `sys.exit(main())` wrapper handle it — the
    generated script already does `sys.exit(main())`).
  - Conventions (all subcommands):
    - `--version` prints `flechtctl <importlib.metadata.version("flechtctl")>`.
    - Errors: `KeyringError`, `SecretError`, `OSError`, `ValueError` (incl. JSON
      decode) and `ModuleNotFoundError` are caught in `main`, printed as
      `flechtctl: error: <message>` on stderr, exit **1**, no traceback. argparse
      usage errors exit **2** (its default).
    - Keyring for crypto commands: `-k/--keyring PATH`, **required** (argparse
      `required=True`). The CLI reads no environment variables at all (decision D7) —
      do not add one, not even as a fallback.
    - Value/token input: optional positional `VALUE`/`TOKEN`; omitted or `-` reads
      stdin with exactly one trailing newline stripped. Output ends with one newline.
    - Password-protected keyring files (decision D10): every command that reads a
      keyring file detects protection with `is_encrypted_keyring` and opens it with
      `decrypt_keyring`. The password comes from `--password-file PATH` (accepted by
      every command that reads or creates a keyring; file content with one trailing
      newline stripped), else — only when `sys.stdin.isatty()` — from
      `getpass.getpass("Keyring password: ")`; otherwise error exit 1 ("this keyring is
      password-protected — pass --password-file"). Creating a protected file prompts
      twice and requires both entries to match. An edit of a protected file writes it
      back protected with the same password (re-encrypted, fresh salt) — never
      silently as plain. No environment variable is ever consulted.
    - Writing keyring files: atomic (temp file in the same directory, `os.chmod 0o600`,
      `os.replace`), content `Keyring.to_json()`. Every edit reads, transforms via
      `dataclasses.replace` or `with_jwk` (so validation runs), then writes. Commands
      that only read a keyring (`show`, `export`, the crypto commands) never write it.
    - Never print private key material. `export` prints only a public half; a private
      half only ever lands in a keyring file.
  - Keyring management (`flechtctl keyring …`):
    - `new KEYRING --kid KID [--type oct|x25519] [--scope S]... [--force]` — new file
      holding one freshly generated sealing key as `primary` (`--type` default `oct`;
      `x25519` requires at least one `--scope`, which the Keyring's own validation
      enforces). Refuses an existing file unless `--force`.
    - `add KEYRING --kid KID [--type oct|x25519|ed25519] [--scope S]... [--primary] [--primary-for S]... [--signing-for S]...`
      — adds a freshly generated key and the requested roles; `--type` defaults to
      `oct`.
    - `export KEYRING KID [-o OUT]` — writes `Keyring.public_jwk(KID)` (one JWK,
      `json.dumps(…, indent=2, sort_keys=True)` + newline) to OUT or stdout. Like
      `keytool -exportcert`: the keyring is left alone. An oct entry is an error (it has
      no public half — a shared secret key is copied by the operator, not exported).
    - `import KEYRING JWKFILE [--primary] [--primary-for S]... [--signing-for S]...` —
      adds the single JWK in JWKFILE (typically an `export`) as a new entry via
      `with_jwk`, plus the requested roles; a kid collision is an error. If KEYRING
      does not exist yet, it is created from `keyring_from_jwk` and `--primary` is
      required (error otherwise: a new keyring needs a primary).
    - `new` and `import` (when it creates the file) take `--encrypt`: the new file is
      written password-protected.
    - `protect KEYRING` — rewrites a plain keyring file in place as protected (error
      if already protected).
    - `unprotect KEYRING -o OUT [--force]` — writes a plain copy to OUT (0600) for a
      deployment secret; KEYRING stays protected; OUT must not exist unless `--force`;
      error if KEYRING is not protected.
    - `remove KEYRING KID` — drops the key, its scope binding, and any
      `flenc_primaries` / `flenc_signing` entry naming it; removing the `primary` is
      an error telling the user to set another primary first.
    - `primary KEYRING KID [--scope S]` — sets `primary`, or `flenc_primaries[S]`.
    - `signing KEYRING KID --scope S` — sets `flenc_signing[S]`.
    - `show KEYRING` — one line per kid, sorted:
      `<kid>  <oct|x25519|ed25519>  <private|public>  scopes=<s1,s2|->  roles=<primary,primary-for:S,signing-for:S|->`
      (oct is always `private`). Prints no key material. Exit 1 on an invalid file.
  - Value crypto:
    - `encrypt -k K [--scope S] [--string] [VALUE]` — VALUE parsed as JSON unless
      `--string` (then taken as a raw string); prints the `flenc:jwe:` token.
    - `decrypt -k K [--scope S] [--string] [TOKEN]` — prints the value as compact JSON
      (`json.dumps(v, ensure_ascii=False)`); with `--string` the value must be a JSON
      string and is printed raw.
    - `sign -k K --scope S [--string] [VALUE]` / `verify -k K --scope S [--string] [TOKEN]`
      — same conventions; `verify` prints the value only when the signature verifies.
    - Composition (documented and tested): `sign --string` piped into
      `encrypt --string` yields exactly what `ENCRYPTED(SIGNED(STR, scope=s), scope=s)`
      writes; `decrypt --string` piped into `verify --string` reverses it.
  - Token tooling:
    - `inspect [TOKEN]` — no keyring; prints
      `{"authenticated": false, "envelope": "flenc:jwe"|"flsig:jws", "header": {…}}`
      (sorted keys, `indent=2`).
    - `reencrypt -k K [--scope S] [TOKEN]` — prints the token re-sealed under the
      scope's current sealing key.
  - Tests `flechtctl/tests/ctl_cli_test.py`, calling `main([...])` in-process with
    `capsys` and `tmp_path` (fast), plus **one** subprocess smoke test of the installed
    console script (`flechtctl --version`, found as
    `Path(sys.executable).with_name("flechtctl")` so it works with or without
    `uv run`): every subcommand's happy path; exit codes
    1/2; `show` output contains no base64 of any key; file mode 0600 after `new`;
    `new` refuses to overwrite; a crypto command without `-k` exits 2; `export` leaves
    the keyring file byte-identical and prints no `d`; protected keyrings: `new
    --encrypt --password-file`, every crypto command via `--password-file`, an edit
    keeps the file protected, `protect`/`unprotect`, the prompt path (monkeypatch
    `sys.stdin.isatty` and `getpass.getpass`), a non-TTY run without a password
    exits 1, mismatched confirmation exits 1; stdin input; the
    sign→encrypt pipe composition decrypts+verifies back; a **walkthrough test** that
    runs exactly the command sequence the guide in task 6 prints (keep them in sync —
    put the sequence in the test as a list and reference the test from the guide).
  - Commit: "Add the flechtctl command".

- [x] **5. CLAUDE.md**
  - Architecture: add a `flechtctl/` bullet (the standalone distribution: modules
    `keyring` / `errors` / `tokens` / `cli`, explicit-keyring API, lazy joserfc via
    PEP 562, never imports flechtwerk, one runtime dependency, exact-version pin from
    `flechtwerk[secrets]` via `hatch_build.py`, the keystore-style per-entry
    export/import, the CLI reading no environment variables, and the opt-in
    password-protected keyring file — an encrypted JWK Set that only flechtctl opens,
    while stages read plain JSON, with D10's reason). Update the `keyring.py` and
    `secrets.py` bullets to say what now lives in `flechtctl` and what stays (runtime,
    codecs, telemetry, scan, the absent-flechtctl placeholder seam).
  - Rewrite "Invariant: joserfc stays confined to flechtwerk.secrets" as
    "joserfc stays confined to flechtctl.tokens": only `flechtctl.tokens` imports
    joserfc; within flechtwerk only `flechtwerk.secrets` reaches it; `import flechtctl`
    and `import flechtwerk.module` load no joserfc (name the pinning tests);
    `module.py`'s `keyring` slot resolves with or without flechtctl installed.
  - Commands: `uv build --all-packages`. Releasing: one tag publishes both
    distributions; before the **first** tag that includes flechtctl, a PyPI *pending
    trusted publisher* for project `flechtctl` must exist (same repository, workflow
    `publish.yaml`, environment `pypi`) — otherwise `uv publish` fails for it.
  - Commit: "Document flechtctl in CLAUDE.md".

- [x] **6. User docs**
  - `flechtctl/README.md` — the PyPI page: what it is; install (`uvx flechtctl --help`,
    `uv tool install flechtctl`, `pip install flechtctl`); CLI quick start (keyring
    new → encrypt → decrypt); library quick start (`Keyring.from_json`, `encrypt`,
    `decrypt`, `sign`, `verify`); the equivalence with flechtwerk's `ENCRYPTED` /
    `SIGNED` codecs; link to the docsite guide.
  - New guide `docs/guides/flechtctl.md` ("Keyrings and Secret Tokens with
    flechtctl"), added to `mkdocs.yml` nav under Guides (after Observability) and to
    the prose of `docs/guides/index.md`. Content: install; the walkthrough from the
    task-4 walkthrough test, explained step by step in plain language:

    ```bash
    flechtctl keyring new reader.json --kid demo-recipient --type x25519 --scope demo
    flechtctl keyring export reader.json demo-recipient -o recipient.jwk
    flechtctl keyring import writer.json recipient.jwk --primary
    flechtctl keyring add writer.json --kid demo-signer --type ed25519 --scope demo --signing-for demo
    flechtctl keyring export writer.json demo-signer -o signer.jwk
    flechtctl keyring import reader.json signer.jwk
    flechtctl keyring show reader.json
    flechtctl keyring show writer.json
    TOKEN=$(flechtctl sign -k writer.json --scope demo --string hello | flechtctl encrypt -k writer.json --scope demo --string)
    flechtctl inspect "$TOKEN"
    flechtctl decrypt -k reader.json --scope demo --string "$TOKEN" | flechtctl verify -k reader.json --scope demo --string
    ```

    (the reader can open and check, the writer can seal and sign but cannot open —
    show that `decrypt -k writer.json` fails). Explain the keystore model: a keyring
    file is a set of named entries; `export` hands out the public half of one entry
    and changes nothing; `import` adds one entry. Plus an oct-only quick path (one
    shared secret key, `new` → `encrypt` → `decrypt`); a section "Protecting the
    master keyring": why the operator's copy should be password-protected and the
    deployed copy is not (D10's reasoning, in plain words), `new --encrypt`,
    `protect`, `unprotect -o` for the deployment secret; and the library API for
    third-party producers (seal a value without the framework). The walkthrough test
    runs exactly this sequence (in-process, with the pipes done in Python) — keep
    the guide and the test in sync.
  - `docs/concepts/secrets.md`: replace the "no CLI" sentence in "## Tooling" with a
    pointer to the guide and the `flechtctl` package; in "## The Keyring", mention
    `Keyring.to_json()` and the per-entry `public_jwk` / `with_jwk` next to
    `Keyring.of` / `Keyring.from_json`, the optional password-protected file form
    (an encrypted JWK Set, RFC 7517 §7 — operator copies only; stages read plain JSON),
    and that `Keyring` now lives in `flechtctl`
    (still importable from `flechtwerk.keyring`); update the install note
    (lines 17–21) and "## Where It Lives" to say the format lives in `flechtctl`
    (joserfc confined to `flechtctl.tokens`), installed by `flechtwerk[secrets]`.
  - `docs/api/index.md`: add a "flechtctl" section and point the moved classes'
    entries at their `flechtctl` paths. Use the **defining module** paths —
    `::: flechtctl.keyring.Keyring`, `::: flechtctl.errors.SecretError`,
    `::: flechtctl.tokens.encrypt` (`decrypt`, `sign`, `verify`, `reencrypt`,
    `read_header`, `encrypt_keyring`, `decrypt_keyring`, the generators) — never the
    lazy `flechtctl.encrypt` re-export, which griffe cannot see through the PEP 562
    `__getattr__` (strict mode would fail);
    `mkdocs.yml` mkdocstrings `paths: [flechtctl/src, src]`.
  - `README.md`: one line under install — `uvx flechtctl --help` for keyring and token
    tooling without the framework; keep the `flechtwerk[secrets]` line (now "installs
    flechtctl").
  - Sweep for stale statements: `grep -rn "joserfc\|no CLI\|flechtwerk.secrets is the only" README.md docs CLAUDE.md`
    and fix every hit that the move made untrue.
  - Verify: `uv run poe docs-build` exits 0.
  - Commit: "Document flechtctl: guide, README, API reference".

## Acceptance Criteria

Work criteria — re-run in full before the Closeout:

1. `uv lock --check` exits 0 and `uv sync` exits 0.
2. `git diff --diff-filter=M --name-only e88d511 -- tests` prints nothing (no
   pre-existing test file modified).
3. `uv run pytest -q` ends with a line reporting more than 680 passed and no failed
   or error count.
4. `uv run pytest -m integration -q` ends with `26 passed` (re-run once if the first
   run shows only container setup errors).
5. `uv run poe docs-build` exits 0.
6. `rm -rf build/release-check && uv build --all-packages -o build/release-check && unzip -p build/release-check/flechtwerk-*.whl "*/METADATA" | grep -E "^(Version|Requires-Dist: (flechtctl|joserfc))" && unzip -p build/release-check/flechtctl-*.whl "*/METADATA" | grep -E "^(Version|Requires-Dist)"`
   prints the same Version twice, a flechtwerk Requires-Dist pinning flechtctl to
   exactly that version under the secrets extra, no joserfc line for flechtwerk, and
   exactly one Requires-Dist for flechtctl, joserfc~=1.7.
7. `uvx --python 3.15 --from build/release-check/flechtctl-*-py3-none-any.whl flechtctl --version`
   prints flechtctl followed by that same version.
8. `D=$(mktemp -d) && uv run flechtctl keyring new $D/k.json --kid k1 && T=$(uv run flechtctl encrypt -k $D/k.json --scope demo --string hello) && uv run flechtctl inspect "$T" && uv run flechtctl decrypt -k $D/k.json --scope demo --string "$T"`
   prints the inspect JSON with kid k1 and flenc_scope demo, then hello on the last line.
9. `D=$(mktemp -d) && printf "correct horse battery staple\n" > $D/pw && uv run flechtctl keyring new $D/k.json --kid k1 --encrypt --password-file $D/pw && head -c 2 $D/k.json && echo && T=$(uv run flechtctl encrypt -k $D/k.json --password-file $D/pw --string hello) && uv run flechtctl decrypt -k $D/k.json --password-file $D/pw --string "$T" && uv run flechtctl keyring unprotect $D/k.json -o $D/plain.json --password-file $D/pw && head -c 1 $D/plain.json && echo`
   prints ey (the protected file is a compact JWE, not JSON), then hello, then a
   single opening brace (the plain copy is JSON).
10. `uv run python -c "import sys, flechtctl, flechtwerk.module; print(sorted(m for m in sys.modules if m.startswith(chr(106) + chr(111) + chr(115) + chr(101))))"`
   prints `[]` (no module whose name starts with jose is loaded).

Closeout criteria — hold only once the Closeout section has run, and are shown there:

11. The final state of this plan was committed in a commit of its own (the
    preservation commit), and a later separate commit deletes it. Shown by Closeout
    steps 2 and 3.
12. `git status --porcelain` is empty after the deletion commit (build/ is
    gitignored). Shown after Closeout step 3.

## Rollback

All work is local commits on `main`, none pushed. To undo: `git reset --hard` to the handoff commit (the one adding this file)
only if the user asks; otherwise `git revert` the work commits newest first. The
`build/release-check` directory is gitignored scratch and can be deleted.

## Closeout

No ticket — so no permalink and no comment; `closeout_push` is false, nothing is pushed.

1. Bring this file to its final state: every task checkbox ticked, the Run Log
   complete, the outcome recorded, and `status: done` in the frontmatter.
2. Commit this file on its own — the preservation commit:

   ```bash
   git add 2026-10-11-flechtctl-plan.md
   git commit -S -m "Record the final state of the flechtctl plan"
   git rev-parse HEAD
   ```

   Do not squash it into a work commit and do not delete the file in it.
3. Only now delete this file in a **separate** commit, then show the tree is clean:

   ```bash
   git rm 2026-10-11-flechtctl-plan.md
   git commit -S -m "Remove the flechtctl plan file"
   git status --porcelain
   ```

If either commit cannot be completed, leave this file in place and write the blocked
report.

**After this run (for the user, not the executor):** register a PyPI pending trusted
publisher for `flechtctl`, push `main`, tag the release; then migrate
`../../livia-nt/camera-inference` (`face_vector_cipher.py`) onto
`flechtctl.encrypt(keyring, vector, scope="face_vector")`.

## Execution Protocol

You are executing this plan file autonomously. There is nobody to ask, so decide
from what is written here plus the repository, and record what you decided.

1. Read this file completely before starting.
2. Run the **Pre-Flight** section first, in your first turn, before changing
   anything. Show each check and its real output. If any check fails, stop there:
   write the blocked report described in step 11, naming the failed check, what was
   expected, what was actually there, and what has to change about the launch. Do not
   start the tasks, do not substitute a different model or mode, and do not proceed
   at a lower spec — a mismatch discovered here costs a relaunch, and the same
   mismatch discovered at task 5 costs the whole run. When every check passes, set
   `status: executing` in the frontmatter — removing `blocked_report` if this relaunch
   just consumed one, since that key moves with the status — and append the gate's
   outcome to the Run Log — the file now says a run is under way, and a later reopen
   reads that rather than guessing. On a resume, continue at the first unticked task; a task the Run
   Log shows started but not finished is inspected before it is redone, because the
   tree may already hold part of it.
3. Work the tasks in order. Append a line to the Run Log when you start a task, and
   tick its checkbox with another line when you finish it. Do this as you go, not at
   the end — if this run is interrupted, the ticked boxes and the last started-line
   are how the next one knows where to resume and what half-done work it may find.
   Mirror them in this session's task list — TaskCreate and TaskUpdate, or TodoWrite
   where that is the tool — so that whoever attaches to this session or follows it
   over Remote Control sees the checklist without opening the file: create it once
   the gate has passed, one item per task plus one for the Closeout, with the ticked
   tasks already completed; mark a task in progress when you start it and completed
   when you tick it. The list is a view and ends with this session; this file is the
   record, and where the two disagree the file is right. A session with no task-list
   tool skips the mirror.
4. Run each task's verification and the Acceptance Criteria for real, and show their
   output. A summary is not evidence.
5. Immediately before the Closeout — after the last work commit — re-run every work
   criterion in the Acceptance Criteria and show every command and result again. The
   closeout criteria are not part of this re-run: they describe the Closeout itself,
   cannot hold before it, and are shown passing as its steps complete, so their not
   holding here is not a failure. The evaluator judging whether you are done reads the
   condition text and the recent conversation — a result proved twenty turns ago may
   no longer be visible to it, and this file is about to stop existing.
6. If reality contradicts the Verified Context, stop and record the contradiction in
   the Run Log before deciding anything. Then proceed only if the plan's Goal still
   makes sense; otherwise treat it as blocked.
7. `graphify-out/` exists and is gitignored; the user's global instructions ask for
   `graphify update .` after code changes. Run it once after each task commit — it
   only touches the gitignored directory, so the tree stays clean. If it is not
   installed or fails, note that in the Run Log and carry on; it is not part of
   this plan's acceptance.
8. Commit as you go, on the current branch — `main`, which the gate confirmed — signed
   (`git commit -S`) and with no ticket prefix: a task, or a coherent slice of one, per
   commit, so progress is durable and a resume or a rollback has real boundaries to
   work with. Never create a branch. Never push, never tag.
9. Run the **Closeout** section — after the commit in step 8 and before you remove
   this file in step 10. That order is the whole point: this file is the record of the
   run and it is about to stop existing, so its final state is committed before
   anything is deleted. If the closeout cannot be completed, do not remove this file —
   write the blocked report in step 11 instead, naming the closeout as what failed.
10. When every acceptance criterion passes and the closeout is done, remove this plan
   file exactly as the Closeout section prescribes: in a separate commit after the
   preservation commit, never folded into it and never folded into the work commit.
11. If a criterion cannot be made to pass — or the run cannot finish for any other
    reason: a denied command, a contradiction with the Verified Context that leaves the
    Goal standing but the plan wrong, a run that has to stop clean at a task boundary —
    write the blocked report at **exactly the path named in the goal condition**:
    `2026-10-11-flechtctl-plan.BLOCKED.md`. It names the failing check, what you
    tried, and why it cannot pass; lists the working tree as you leave it — every
    uncommitted file and the task it belongs to; and opens with a frontmatter block of
    exactly three keys, alphabetically — `date:` the day the run stopped; `next:`
    `relaunch` when the Handoff command can resume once the environment is fixed, or
    `reopen` when the plan itself has to change; and `plan_file:`
    `2026-10-11-flechtctl-plan.md`. Set `status: blocked` **and** `blocked_report:`
    naming the report in this file's frontmatter — the two are written in the same
    commit and cleared in the same turn. Bring the Run Log current, and commit this
    file and the report together — the blocked commit — leaving uncommitted only the
    work that has not reached a commit boundary. Then stop. Getting the path wrong is
    not cosmetic: the evaluator looks for the path the condition names. Do not weaken a
    check, skip it, or declare success without it. A report written by mistake — on an
    estimate rather than a check that ran — is retracted in the same session: delete
    it, say so in the Run Log, and continue.

## Handoff

```bash
claude --model sonnet --effort high --permission-mode auto --remote-control 2026-10-11-flechtctl-plan '/goal Execute the plan at 2026-10-11-flechtctl-plan.md to completion. Before changing anything, run the Pre-Flight section of that plan and show the output of every check: the model must be claude-sonnet-5-5, the effort high, the permission mode auto, and the signing, Docker, Python 3.15 and fixture-copy probes must pass. If any pre-flight check fails, write the blocked report named below and stop - do not start the tasks and do not continue on a different model or mode. Done when all of the following hold, each shown in this session in a final re-run after the last work commit: uv lock --check and uv sync exited 0; git diff --diff-filter=M --name-only e88d511 -- tests printed nothing; uv run pytest -q reported more than 680 passed and no failures or errors; uv run pytest -m integration -q reported 26 passed; uv run poe docs-build exited 0; uv build --all-packages -o build/release-check built both packages, and their wheel METADATA showed one shared Version, flechtwerk pinning flechtctl to exactly that version under the secrets extra with no joserfc requirement, and flechtctl requiring only joserfc~=1.7; uvx --python 3.15 --from the built flechtctl wheel ran flechtctl --version and printed that version; the plain-keyring CLI round trip (keyring new, encrypt --string hello, inspect, decrypt) printed hello; the password-protected round trip (keyring new --encrypt --password-file, encrypt, decrypt, keyring unprotect) printed ey, hello and an opening brace; importing flechtctl and flechtwerk.module loaded no jose module (printed []); and every remaining criterion in the Acceptance Criteria section of that plan was run in this session, its command and output shown, and passed. Every task checkbox in that file was ticked before the file was removed. The final state of the plan file was committed on its own in this session, and only then was the plan file deleted in a separate later commit, after which git status --porcelain was empty. All commits are GPG-signed, on the current branch main, without a ticket prefix; nothing was pushed or tagged. Do not create a branch. Do not relax, skip, or declare passed any check that was not actually run, do not edit any pre-existing file under tests, and do not route around a permission denial - a refused command is a blocked-report situation. If a pre-flight or acceptance check cannot be made to pass, write 2026-10-11-flechtctl-plan.BLOCKED.md in this session, naming a check this session ran, what was tried, and why it cannot pass, commit it with the plan, then stop - that also satisfies this goal. A blocked report left by an earlier launch does not.'
```

`/pdca:execute 2026-10-11-flechtctl-plan.md` runs that command as a background session
with Remote Control, so nothing needs pasting. If this plan file has been sitting for a
while, do not launch it blindly — reopen it first with
`/pdca:plan 2026-10-11-flechtctl-plan.md` to re-verify the Verified Context and get a
freshened handoff.

The condition inlines acceptance criteria 1–10 in summarized form (commands named,
expected results stated); the closeout criteria 11–12 are stated as what must have
happened in the session. Nothing is left only in this file.

Short form, for typing into a session already started with the flags above. The
evaluator then holds only the plan path, the protocol and the escape hatch — not the
acceptance criteria — so it has to trust this session's own account of what it ran.
Prefer the full command; use this when the full one is not to hand. A bare
`/goal 2026-10-11-flechtctl-plan.md` is not a launch at all: the evaluator cannot open
the file, so it would have nothing to judge.

```
/goal Execute the plan at 2026-10-11-flechtctl-plan.md to completion per its Execution Protocol: Pre-Flight first, every Acceptance Criterion shown passing in a final re-run, the Closeout done before the file was removed in a separate later commit; or 2026-10-11-flechtctl-plan.BLOCKED.md was written in this session.
```

## Run Log
- 2026-10-12 Pre-Flight passed: goal met:false naming this plan, model claude-sonnet-5-5, permissionMode auto, effort high; SIGN-OK, DOCKER-OK, python3.15 found, fixture copy hash 16ee8450… COPY-OK; branch main, tree clean, flechtctl ABSENT, 680 passed, to_json count 0.
- 2026-10-12 Task 1 started and finished: workspace member, hatch metadata hook, publish.yaml; sync ok, both wheels same version, flechtctl==<ver>; extra == 'secrets', 680 passed, lock --check 0. Plan file deliberately left uncommitted until Closeout.
- 2026-10-12 Task 2 started: wrote flechtctl/src/flechtctl/{keyring,errors,tokens,__init__}.py (uncommitted), smoke-tested by hand; tests not yet written.
- 2026-10-12 BLOCKED at task 2: the copy of ../../livia-nt/camera-inference/tests/fixtures/face_vector_token.txt into flechtctl/tests/fixtures/ was denied by the auto-mode classifier ([Sensitive-Source Provenance]). The Pre-Flight probe copying it into a mktemp dir had passed. No workaround attempted. See 2026-10-11-flechtctl-plan.BLOCKED.md; next: relaunch.
\n- 2026-10-12 Report amended after a Stop-hook review: the plan's Pre-Flight step 2 classes a denial of the fixture copy as a pre-flight failure, so the report now cites that rule.\n
- 2026-10-12 Resumed after the user wrote "permission granted". Folded in the earlier blocked report (date 2026-10-12; check: Pre-Flight step 2 fixture copy into flechtctl/tests/fixtures/ denied by the auto-mode classifier, [Sensitive-Source Provenance]; tried: one compound cp + git log command, no workaround; next: relaunch). Report file deleted, blocked_report removed, status back to executing. Uncommitted task-2 files from before the block are inspected and kept.
- 2026-10-12 Fixture copied (sha256 16ee8450…, 3988 bytes, matches). Task 2 finished: keyring/errors/tokens/__init__ + 4 ctl test files; `uv run pytest -q` 750 passed. Committed (the blocked report removal separately).
- 2026-10-12 Task 3 started and finished: flechtwerk.keyring/secrets rewired (placeholder seam, wrappers, SIGNED.encode via sign_bytes), tests/flechtctl_interop_test.py added; `uv run pytest -q` 780 passed, `git diff --diff-filter=M --name-only e88d511 -- tests` empty. Committed.
- 2026-10-12 Task 4 started and finished: cli.py + __main__.py + ctl_cli_test.py (incl. the walkthrough list WALKTHROUGH, to be mirrored by the guide); `uv run pytest -q` 820 passed; criteria 8 and 9 command lines run by hand and printed hello / ey,hello,{. Committed.
- 2026-10-12 Task 5 started and finished: CLAUDE.md (flechtctl bullet, keyring/secrets bullets, joserfc invariant renamed to flechtctl.tokens, Commands, Releasing incl. the pending trusted publisher note). Committed.
- 2026-10-12 Task 6 started and finished: flechtctl/README.md, docs/guides/flechtctl.md (+ nav, guides index, a guide-sync test in ctl_cli_test.py), secrets concept page, API reference, root README, architecture one-liner, mkdocs paths; a page ornament pair for guides-flechtctl (braid4 + verdigris, generate.py PAGES entry — every style was already taken, so this page reuses one with a new accent). `uv run poe docs-build` exit 0, the new api entries rendered. Committed.
- 2026-10-12 Final re-run after the last work commit (c13d771), all shown in the session: criterion 1 lock --check 0 and sync 0; 2 no modified test files vs e88d511; 3 `821 passed, 26 deselected`; 4 `26 passed, 821 deselected` (first run, no setup errors); 5 docs-build exit 0; 6 both wheels Version 0.15.1.dev10+gc13d771b9.d20261012, `Requires-Dist: flechtctl==<that>; extra == 'secrets'`, no joserfc line for flechtwerk, flechtctl only `joserfc~=1.7`; 7 `uvx --python 3.15 … flechtctl --version` printed `flechtctl 0.15.1.dev10+gc13d771b9.d20261012`; 8 inspect JSON kid k1 / flenc_scope demo, then hello; 9 `ey`, `hello`, `{`; 10 `[]`.
- 2026-10-12 Outcome: done. Deviations worth knowing: one block mid-run (fixture copy denied by the auto-mode classifier, then permission granted by the user — see the 2026-10-12 entries above); the page ornament for docs/guides/flechtctl.md reuses style braid4 with another accent (all 16 styles were taken); the interop test file is tests/flechtctl_interop_test.py and flechtctl/tests holds ctl_{cli,hygiene,keyring,keyring_file,tokens}_test.py. Nothing pushed, nothing tagged, no branch created.
