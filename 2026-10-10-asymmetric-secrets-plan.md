---
branch: main
closeout_push: false
created: 2026-10-10
executor:
  effort: medium
  model: claude-opus-5-5
  permission_mode: auto
permalink: none
plan_file: 2026-10-10-asymmetric-secrets-plan.md
planner:
  effort: xhigh
  model: claude-opus-5-5
  permission_mode: auto
plugin: pdca
plugin_url: https://github.com/christian-schlichtherle/cs7-claude-plugins
plugin_version: 0.16.1
review_rounds: 10
sources: []
status: executing
ticket: none
---

# Asymmetric recipients, optional signatures and scope-bound keys for `flechtwerk.secrets`

## Goal

`flechtwerk.secrets` seals a value only as `dir` + `A256GCM`, so every writer holds a
key that also opens everything sealed under its `kid` — the sharpest limitation
`docs/concepts/secrets.md` names (Scope & Caveats, item 4). After this work, three
things exist that do not exist now, all as pure extensions of the current API:

1. **Recipient keys.** A second token generation under the same `flenc:jwe:` prefix:
   `alg: ECDH-ES+A256KW`, `enc: A256GCM`, sealed to an X25519 recipient key. A writer
   whose keyring holds only the recipient's public half can seal and cannot open; a
   reader holding the private half opens. Both generations coexist in one topic.
2. **Optional signatures.** A new composable codec `SIGNED(inner, *, scope, read_unsigned=False)`
   that wraps a value in an Ed25519 compact JWS under a new `flsig:jws:` prefix. The
   recommended composition is sign-then-encrypt, `ENCRYPTED(SIGNED(inner, scope=s), scope=s)`.
   It answers what recipient keys alone cannot: anyone holding a public recipient key
   can mint a valid token, so "it decrypts" no longer says who wrote it.
3. **Scope-bound keys.** Any key may declare the scopes it serves; recipient and
   signing keys **must**. Readers reject a token whose stamped scope is outside its
   key's binding, writers refuse to seal or sign outside it, and an optional per-scope
   key map lets one process seal different scopes under different keys (`dir` for
   `api_key`, a recipient for `face_vector`).

The design rule throughout is **safe by design for everything new, unchanged for
everything old**: new kinds of keys and the new codec start strict, because loosening a
rule later is a non-breaking extension and tightening one later is a breaking change;
existing oct keys, `dir` tokens, keyring documents and calls behave exactly as today.

The first consumer is LIVIA (`livia-nt`), whose edge boxes seal a biometric
`face_vector` per event under a per-box symmetric key: a stolen box opens every vector
it ever sealed, adding a box means restarting the readers, and one key per event stream
runs into the AES-GCM nonce budget. A recipient key answers all three; a signing key
lets the readers tell a genuine box from anyone holding the public key. The LIVIA
migration is a separate plan, after a flechtwerk release.

## Requirements

No requirements source: no ticket, no spec. The plan was derived in conversation; the
decisions made there are recorded under "Settled decisions" in Constraints & Non-Goals.

## Verified Context

All commands ran on 2026-10-11 in the worktree
`/Users/christian/projects/bsure-analytics/flechtwerk/.claude/worktrees/encryption`,
in a planning session under permission mode `auto` — the mode the executor runs in.
Every command this plan prescribes ran cleanly under it. (The first draft was written
on 2026-10-10 in the main checkout; every fact below was re-established here.)

**Retargeted to the main checkout at handoff.** The run works in
`/Users/christian/projects/bsure-analytics/flechtwerk` on branch `main`, which was
fast-forwarded to the handoff commit `c765986`, the tip of `worktree-encryption`, so
the two hold identical files. There, `git branch --show-current` → `main`,
`git status --porcelain` → empty, `uv run python -c "import sys; print(sys.version_info[:2])"`
→ `(3, 14)`, and `uv run pytest -q` → `587 passed, 22 deselected`. The facts below that
name the worktree hold for this checkout as well.

- **Repository and branch.** The worktree is on branch `worktree-encryption`, clean,
  fast-forwarded to `main` at `231e7f5 Make Record.raw a read-only snapshot`; remote
  `https://github.com/bsure-analytics/flechtwerk.git`; commit subjects are plain
  sentences with no ticket prefix: `git branch --show-current`, `git status --porcelain`,
  `git log --oneline -3`, `git remote -v`. The new `Record.raw` snapshot does not touch
  this work: `secrets.py` only reads `.raw` (`grep -n '\.raw' src/flechtwerk/secrets.py`
  → line 447, inside `scan_config_topics`).
- **Interpreter.** The worktree venv is CPython 3.14 (`grep version_info .venv/pyvenv.cfg`
  → `version_info = 3.14`). A bare `uv run` in a fresh worktree picked CPython 3.15, for
  which `rocksdict` 0.3.29 has no wheel (`can't be installed because it doesn't have a
  source distribution or wheel for the current platform`); `uv sync --python 3.14` fixed
  it and left the tree clean. Pre-Flight check 4 pins the version.
- **Today's acceptance rule** is pinned in one registry: `grep -n '_ALG = \|_ENC = \|_REGISTRY = ' src/flechtwerk/secrets.py`
  → lines 78 (`_ALG = "dir"`), 79 (`_ENC = "A256GCM"`), 86 (`_REGISTRY = jwe.JWERegistry(`).
  Encryption is `_encrypt_payload` (line 216), which calls `active_keyring().primary_pair()`;
  decryption, the `kid` lookup via `keyring.key_for(kid)` and the scope ratchet are
  `_decrypt_to_payload` (line 227). Every crypto failure there is wrapped as
  `SecretDecryptError(... ) from e`, and an unknown `kid` keeps `UnknownKeyError` as
  `__cause__`.
- **`Keyring` today** (`src/flechtwerk/keyring.py`) is a frozen dataclass with exactly
  `keys: dict[str, bytes]` (32 bytes each) and `primary: str`; methods `of`, `from_json`,
  `key_for`, `primary_pair`, `kids`; `from_json` rejects any `kty` other than `oct`
  (line 142). `install_keyring` logs `len(keyring.keys)`. The module imports neither
  joserfc nor cryptography, and importing `flechtwerk.module`, `flechtwerk.keyring` and
  `flechtwerk.testing` loads neither:
  `uv run python -c "import flechtwerk.module, flechtwerk.keyring, flechtwerk.testing, sys; print(sorted(m for m in sys.modules if m.startswith(('joserfc', 'cryptography'))))"`
  → `[]`.
- **Exports.** `flechtwerk.secrets.__all__` and `flechtwerk.keyring.__all__` are each in
  `sorted()` order (checked with `assert m.__all__ == sorted(m.__all__)`);
  `flechtwerk.testing.__all__` is not (`RecordingObserver` comes last). The API reference
  lists the secrets surface explicitly, one `::: flechtwerk.…` line per name, in
  `docs/api/index.md` lines 56–72.
- **The keyring gauge** is fed by `Keyring.kids()` in `module.py` lines 670–673
  (`for kid in self.keyring.kids(): self.observer.keyring_key_loaded(kid)`).
- **Observer and metrics.** The secret events are `keyring_key_loaded(kid)`,
  `secret_plaintext_read(scope)` and `secret_decrypted(scope, kid)` on `Observer`
  (`src/flechtwerk/observer.py` lines 60–62, no-op), `PrometheusObserver` (lines
  161–168) and `RecordingObserver` (`src/flechtwerk/testing.py` lines 91–98); their
  metrics are `@cached_property` methods in `src/flechtwerk/metrics.py` from line 314;
  their tests are `tests/observer_test.py` lines 337–360; the metric table is
  `docs/guides/observability.md` lines 180–190.
- **joserfc.** The lock pins 1.7.4 (`grep -A1 '^name = "joserfc"' uv.lock`). Under it
  (scratch scripts run with `uv run python`):
  - `ECDH-ES+A256KW` with an X25519 key round-trips; decrypting with a public-only key
    raises `DecodeError: Invalid key for exchanging shared key`; a registry allowing
    only `dir` refuses an `ECDH-ES+A256KW` token with `UnsupportedAlgorithmError`.
  - Hostile `epk`s are refused, each with a different exception type: an all-zero
    (low-order) X25519 `epk` → `ValueError: Error computing shared key.`; an X448 `epk`
    → `DecodeError`; a P-256 `epk` → `KeyError: 'P-256'`. Wrapping **every** exception
    into `SecretDecryptError` is therefore necessary.
  - **joserfc imports a mismatched X25519 pair without complaint**
    (`OKPKey.import_key({... "x": <other>, "d": <d>})` succeeds, `is_private` true).
    The mismatch is detectable with joserfc's own key object, no `cryptography`
    import needed: `key.private_key.public_key().public_bytes_raw() != public` — true for
    a mismatch, false for a correct pair, for both X25519 and Ed25519.
  - JWS: the registry lists `Ed25519`, `Ed448`, `EdDSA`; `Ed25519` (RFC 9864, fully
    specified) signs and verifies; `EdDSA` works but emits
    `SecurityWarning: EdDSA is deprecated via RFC 9864`. An X25519 key used for
    `Ed25519` raises `InvalidKeyCurveError`. A custom `flenc_scope` header survives
    `jws.JWSRegistry(header_registry=..., algorithms=["Ed25519"], strict_check_header=True)`.
    A tampered payload raises `BadSignatureError`.
- **HPKE is still unavailable.** joserfc 1.7.5 is the latest release on PyPI
  (`curl -s https://pypi.org/pypi/joserfc/json`), and under it `HPKE-0`, `HPKE-3` and
  `HPKE-4` all raise `UnsupportedAlgorithmError: ... is not supported` on encrypt; no
  ML-KEM/KEM algorithm is registered (`uv run --with joserfc==1.7.5 python <probe>`).
- **`cryptography` is a dev-only dependency** (`pyproject.toml` line 9, comment
  "secrets_test.py's independent AES-GCM oracle (not via joserfc)"); the `secrets`
  extra is `joserfc~=1.7` alone. Production code must not import `cryptography`
  directly; tests may.
- **Independent readers** built on pyca/cryptography alone, verified against joserfc
  1.7.4 tokens (scratch spike): the JWE reader below opened a joserfc
  `ECDH-ES+A256KW` token, and the JWS verifier below verified a joserfc `Ed25519`
  token. Both are given verbatim in task 2 and task 3.
- **Test key material** (deterministic, used by the fixtures and the pinned vectors):
  - Recipient `test-recipient`: X25519 private `d` = `bytes(range(32))`, public
    `x` = `j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8`.
  - Signer `test-signer`: Ed25519 private seed `d` = `bytes(range(32, 64))`, public
    `x` = `Kay64UG8yvCyLhqU000LxzYeUm0L_hLIl5S8kyKWbdc`.
- **Pinned panva-jose vectors** — minted with `jose` 6.2.12 (npm, installed with
  `--ignore-scripts` into a scratch directory) and verified to decode with joserfc
  1.7.4 (scratch scripts). All three carry scope `api_key`; each inner payload is
  `JSON.stringify("from-panva-jose")`.
  - **ECDH token** (minted 2026-10-10; header `alg ECDH-ES+A256KW`, `enc A256GCM`,
    `kid test-recipient`, `flenc_scope api_key`). Prefix it with `flenc:jwe:`:

    ```
    eyJhbGciOiJFQ0RILUVTK0EyNTZLVyIsImVuYyI6IkEyNTZHQ00iLCJraWQiOiJ0ZXN0LXJlY2lwaWVudCIsImZsZW5jX3Njb3BlIjoiYXBpX2tleSIsImVwayI6eyJ4IjoiQ05Kd3dRVkhjQzc4eUxLcFo3NkZYTFk2aVNkc1U5el90N2R0T0VIc2JHdyIsImNydiI6IlgyNTUxOSIsImt0eSI6Ik9LUCJ9fQ.25AYx4I-5kAOer_IxHPTEWdw1-06HwobWuxKoZST6Ck-7dh4XfdwZQ.XtWiNILmXZl60Vkl.Ysf9xPVrhFB_t2KoRNyh7Jc.ZrUOixIdsD2XSHONnGIzmw
    ```

  - **JWS** (minted 2026-10-11; header `alg Ed25519`, `kid test-signer`,
    `flenc_scope api_key`). Prefix it with `flsig:jws:`:

    ```
    eyJhbGciOiJFZDI1NTE5Iiwia2lkIjoidGVzdC1zaWduZXIiLCJmbGVuY19zY29wZSI6ImFwaV9rZXkifQ.ImZyb20tcGFudmEtam9zZSI.4GFO76IqssQLUrXDsxzczkHyCVs4W5iwW7_zhG4AUtHcvtXtW2rHGgIhbNklx1qPB0uxtSXbv-aB5vUSYngODg
    ```

  - **Nested token** (minted 2026-10-11): a JWE as above whose plaintext is
    `JSON.stringify("flsig:jws:" + <the JWS above>)`. Prefix it with `flenc:jwe:`:

    ```
    eyJhbGciOiJFQ0RILUVTK0EyNTZLVyIsImVuYyI6IkEyNTZHQ00iLCJraWQiOiJ0ZXN0LXJlY2lwaWVudCIsImZsZW5jX3Njb3BlIjoiYXBpX2tleSIsImVwayI6eyJ4IjoibVVYS1NaU2J6MFRDSjNWaUNYRjZZa05KYVA3WHh5YjhnMVlEcFBlTnZ4byIsImNydiI6IlgyNTUxOSIsImt0eSI6Ik9LUCJ9fQ.UxK6uIEE0C9gKmcNQ9xBAM_3FPkSTC3Dd5lxCuzU6pQQfNJ72EK_2g.8O-im68xAS5XFeEc.6CzCRw0CoYNdwuyusitVuQ9JhEJG0_4mxtz4K7rkfiYad1maYoXiQWSI4WooKAX07flU4CQ0bZ4MGEQzXvEq2LL8plfw9rNuS7g9ewayA2mv2lZt-stjuREMZG5pj-Qyoy4n7DsrRD05DtSGKHiePQiTa60ggXJ5oFZKO88Sou6cBLc-Tpuy8RMoS_XXeaa_RAZiVHhZw-0NqoXA4XUWWUYnCO80EWdP1plGjwyHEW91EMR1gTZs6Lq1zXWE9qjC4p-RVszdmZ8mNfyv1g.J--jcRUA0jQ_SOF_sqkr0A
    ```

  The minting script, for the test comments: `importJWK({kty:'OKP', crv:'Ed25519', x, d}, 'Ed25519')`
  then `new CompactSign(payload).setProtectedHeader({alg:'Ed25519', kid:'test-signer', flenc_scope:'api_key'}).sign(key)`;
  `importJWK({kty:'OKP', crv:'X25519', x}, 'ECDH-ES+A256KW')` then
  `new CompactEncrypt(payload).setProtectedHeader({alg:'ECDH-ES+A256KW', enc:'A256GCM', kid:'test-recipient', flenc_scope:'api_key'}).encrypt(key)`.
- **The existing interop tests** live at the end of `tests/secrets_test.py`:
  `_independent_token` (line 47), `test_reads_token_minted_without_joserfc` (459),
  `_PANVA_JOSE_TOKEN` (476), `test_reads_pinned_panva_jose_token` (482),
  `test_our_token_decodes_with_independent_reader` (488),
  `test_importing_flechtwerk_does_not_load_joserfc` (508). `tests/conftest.py` has an
  autouse fixture isolating the secret runtime per test. No existing test name in
  `tests/keyring_test.py` or `tests/secrets_test.py` contains `asymmetric` or `signed`,
  and exactly one contains `panva`. `test_from_json_rejects_non_oct_key` uses
  `kty: RSA`, which stays rejected.
- **Baseline.** `uv run poe test-all -q` → `609 passed in 64.57s`; `uv run poe cov`
  exits 0 with total coverage 97 % and `src/flechtwerk/secrets.py` and
  `src/flechtwerk/keyring.py` each at 100 %; `uv run poe docs-build` (strict) prints
  `Documentation built` and exits 0.
- **Privileges.** `docker info --format "{{.ServerVersion}}"` → `29.8.2`. Commits are
  signed (`git config --get commit.gpgsign` → `true`, key `4459B10B5535E920`), and
  non-interactive signing works while the passphrase is cached:
  `echo probe | gpg --batch --pinentry-mode error --local-user 4459B10B5535E920 --clearsign >/dev/null && echo gpg-ok`
  → `gpg-ok` (it failed on 2026-10-10 with the cache cold; Pre-Flight check 2 tests it).
- **Command shape.** In the planning session, compound shell commands that run `git`
  (pipes, `&&` chains, `$(…)` around git) were refused by the worktree guard; every
  plain single `git` command ran. This plan therefore prescribes each `git` command
  on its own.

## Constraints & Non-Goals

- Every commit is signed: `git commit -S` (flechtwerk `CLAUDE.md`). Launching phase 2
  is the owner's explicit request for its commits; **never push**.
- **Open/closed — extensions only.** Every existing public name keeps its signature
  and behaviour for every input that is valid today: `Keyring(keys, primary)`,
  `Keyring.of(keys, *, primary)`, `from_json` on an oct-only document, `key_for`,
  `primary_pair` and `kids` on an oct-only keyring, `ENCRYPTED`, `encrypt_value`,
  `reencrypt`, `kid_of`, `is_encrypted`, `scan_config_topics`, every exception, every
  observer hook and metric. New parameters are keyword-only with defaults; new
  dataclass fields come after the existing ones and have defaults. **Every existing
  test keeps passing unmodified.**
- `keyring.py` and `testing.py` stay free of joserfc **and** of `cryptography` (the
  import check in Acceptance Criterion 6). Validation that needs cryptography (does a
  private key match its public key?) lives in `secrets.py`, which uses joserfc's key
  objects for it — never a direct `cryptography` import in `src/`.
- joserfc stays confined to `flechtwerk.secrets`; `SIGNED` therefore lives there too.
- Code style (flechtwerk `CLAUDE.md`): alphabetical ordering of keys, constants,
  imports and dataclass-free collections; `__all__` of `secrets` and `keyring` stays
  in `sorted()` order; comprehensions over accumulator loops; `X | None` typing; an
  empty line after every Markdown heading; files end with a newline.
- Out of scope: JOSE HPKE and any post-quantum hybrid (joserfc cannot encrypt HPKE);
  certificate chains or any trust model beyond the keyring (`x5c`, a CA); a per-attribute
  allowlist of signer kids; keyring hot-reload; `scan_config_topics` support for
  `SIGNED`-only attributes; a release or tag; the LIVIA migration; any change to the
  labels of an existing metric.

### Settled decisions

Each was put to the owner in planning and decided; do not re-open any of them.

1. **`ECDH-ES+A256KW` now**, as one more allowed `alg` that HPKE can join later. Not
   direct `ECDH-ES`: `+A256KW` is what the pinned panva vectors use. The docs state that
   this mode reintroduces "harvest now, decrypt later", which `dir` does not.
2. **Signing is in scope, optional, and a separate codec**:
   `SIGNED(inner, *, scope: str, read_unsigned: bool = False)`, Ed25519 compact JWS,
   wire prefix `flsig:jws:` (the docs reserve `flenc:` for ciphertext forms and give a
   non-encryption backend its own scheme). Not a keyword on `ENCRYPTED`; not ECDH-1PU.
3. **Recommended composition: sign, then encrypt** — `ENCRYPTED(SIGNED(x, scope=s), scope=s)`,
   the RFC 7519 nested-JWT order: the signer's `kid` stays inside the ciphertext, and
   the signature's own scope stamp keeps a decrypting reader from re-sealing it into
   another field. Every composition stays legal; the docs recommend this one.
4. **Keyring shape: separate dictionaries per key kind** beside `keys` — `recipients`
   (X25519) and `signers` (Ed25519) — so `keys` and every existing caller stay as they are.
5. **Scope-bound keys, safe by design.** Any key may declare scopes; recipient and
   signing keys must; readers and writers enforce the binding; sealing and signing keys
   are chosen per scope explicitly (`primaries`, `signing`), never implicitly; `SIGNED`
   requires a non-empty scope and checks it exactly; a reader trusts any signing key in
   its keyring whose binding covers the attribute's scope. The rules are in task 1.
6. **Recipient and signing key granularity** (per environment, site, box or period) is
   the application's choice; flechtwerk only documents the trade-offs (what destroying
   a recipient key erases; per-box versus fleet-wide signing keys).

## Pre-Flight

Run in the first turn, before task 1. Each check is one command with one expected
result, and any failure stops the run: write the blocked report named in step 10 of
the Execution Protocol and do not start the tasks. Model, effort and permission mode
are spelled out literally here, in the same spelling as the frontmatter's `executor`
block, so the comparison is a string match, not a judgement.

Before check 1: if the frontmatter carries a `blocked_report` — or
`2026-10-10-asymmetric-secrets-plan.BLOCKED.md` exists — this is a relaunch. Fold that
report into the Run Log below — its `date`, the check it named, what was tried, its
`next` — then delete it and remove `blocked_report` from the frontmatter, in this same
turn, and only then run the checks. The key and the report are consumed together, so a
plan never names a report that is no longer there. An inherited report never satisfies
this plan's goal and is never left in place: the Run Log is the record, the report was
the message.

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

   Expected: `"met":false` with a condition naming this plan file — the last goal
   set in this session is this plan's and was never met and never cleared. A last
   record with `"met":true`, a different goal's condition, or no record at all in a
   readable transcript each mean no evaluator guards this run: stop and write the
   blocked report; the fix is relaunching with the command in the Handoff section. If
   the `T=` line prints empty, the transcript could not be found and none of the
   transcript-derived facts can be read — they are unverifiable, not false: record
   that in the Run Log, take the model from what this session states about itself,
   the effort from `CLAUDE_EFFORT`, and the permission mode from the launch flags
   described below; treat the goal check as unverifiable, and continue. Otherwise
   expect `"model":"claude-opus-5-5"`, `"permissionMode":"auto"`, `effort=medium`.

   The `permissionMode` line prints nothing in a headless (`-p`) session started with
   a slash command as its prompt; the Handoff command launches interactively, where it
   does print. If it is empty anyway, fall back to the launch flags, walking up from
   `$PPID` until the argv starts with `claude`, and matching `--permission-mode <mode>`
   or `--dangerously-skip-permissions`. If neither source answers, the mode is
   unverifiable: say so rather than assuming it.

2. **Privileges** — skip this group only if the mode above is `bypassPermissions`,
   and say in the Run Log that you skipped it.
   - `echo probe | gpg --batch --pinentry-mode error --local-user 4459B10B5535E920 --clearsign >/dev/null && echo gpg-ok`
     prints `gpg-ok`. Without it no signed commit can be made unattended; the fix is
     caching the passphrase in gpg-agent before launching.
   - `docker info --format "{{.ServerVersion}}"` prints a version: the integration
     tier needs Docker.

3. **Ground.** Run each `git` command on its own.
   - This file exists at `2026-10-10-asymmetric-secrets-plan.md`, the `plan_file` in
     its frontmatter, in the working directory
     `/Users/christian/projects/bsure-analytics/flechtwerk`.
   - `git remote -v` names `bsure-analytics/flechtwerk`.
   - `git branch --show-current` prints the `branch` the frontmatter names.
   - `git status --porcelain` is empty — or this is a resume: the frontmatter says
     `status: executing` or `status: blocked`, and the only changes are this file, the
     blocked report just consumed, and the files the Run Log names as work in progress.
     Anything else dirty is a failed check.

4. **Preconditions from the Verified Context.**
   - `uv run python -c "import sys; print(sys.version_info[:2])"` prints `(3, 14)`. If
     it prints anything else, run `uv sync --python 3.14` once and repeat; a second
     mismatch is a failed check.
   - `grep -n '_ALG = "dir"' src/flechtwerk/secrets.py` prints one line.
   - `grep -A1 '^name = "joserfc"' uv.lock` shows `version = "1.7.4"` or a later 1.7.x.
   - `uv run pytest -q` passes.

## Tasks

Ordered by dependency: the keyring (task 1) is what sealing (task 2) and signing
(task 3) read; task 3's nested tests need task 2's recipient sealing; the documentation
(task 4) describes what the tests prove. Each task ends in a signed commit — subject a
plain sentence, no prefix, matching `git log`.

- [x] **1. Teach `Keyring` recipient keys, signing keys and scope bindings** (`src/flechtwerk/keyring.py`, `tests/keyring_test.py`)
  - Add two frozen dataclasses, both exported from `flechtwerk.keyring.__all__`:

    ```python
    @dataclass(frozen=True)
    class Ed25519Key:
        """An Ed25519 signing key: `private` (the 32-byte seed) signs, `public` verifies.

        `private=None` makes a verify-only key — what a reader holds.
        """

        public: bytes
        private: bytes | None = field(default=None, repr=False)


    @dataclass(frozen=True)
    class X25519Key:
        """An X25519 recipient key: `public` seals, `private` opens.

        `private=None` makes a seal-only key — what a writer holds.
        """

        public: bytes
        private: bytes | None = field(default=None, repr=False)
    ```

  - Extend `Keyring` with five fields **after** `primary`, all defaulting to empty
    (`field(default_factory=dict)`), in this order:
    `primaries: dict[str, str]` (scope → sealing kid), `recipients: dict[str, X25519Key]`,
    `scopes: dict[str, frozenset[str]]` (kid → the scopes it may serve; absent = unbound),
    `signers: dict[str, Ed25519Key]`, `signing: dict[str, str]` (scope → signing kid).
    `__post_init__` normalizes `scopes` values to `frozenset` (via `object.__setattr__`,
    a dict comprehension) and then validates, raising `KeyringError` with a message
    naming the offending kid or scope:
    1. At least one key in `keys` or `recipients` — keep the existing message
       `a keyring needs at least one key`.
    2. Every kid in all three dictionaries is a non-empty string, and no kid appears in
       two of them.
    3. Every oct key is 32 bytes (existing message); every `X25519Key`/`Ed25519Key` has
       a 32-byte `public` and a `private` that is `None` or 32 bytes.
    4. `primary` names a key in `keys` or `recipients` — keep the existing message for
       the not-found case, listing `sorted(self.keys) + sorted(self.recipients)` as the
       held keys; a signing kid as `primary` is the same error.
    5. Every kid in `scopes` is held (in any of the three dictionaries); every value is
       a non-empty set of non-empty strings.
    6. **Every recipient and every signing kid has a `scopes` entry** — public-key
       material is always scope-bound. Oct keys may have one or not.
    7. Every `primaries` entry maps a non-empty scope to a kid in `keys` or
       `recipients`, and when that kid is bound, the scope is in its binding.
    8. Every `signing` entry maps a non-empty scope to a kid in `signers` whose
       `private` is not `None`, and the scope is in its binding.
  - `Keyring.of(keys, *, primary, primaries=None, recipients=None, scopes=None, signers=None, signing=None)`
    copies each mapping (`dict(...)`, `{kid: frozenset(v) ...}` for `scopes`), treating
    `None` as empty. The existing call shape is unchanged.
  - New methods: `recipient_for(kid) -> X25519Key` and `signer_for(kid) -> Ed25519Key`
    (raise `UnknownKeyError(kid)` like `key_for`); `sealing_kid(scope: str) -> str` =
    `primaries.get(scope, primary)`, raising `KeyringError` when that kid is bound and
    `scope` is not in its binding (an unscoped value, `scope == ""`, is never in a
    binding — say "an unscoped value" in the message); `signing_kid(scope: str) -> str`
    = `signing[scope]`, raising `KeyringError("no signing key for scope ...")` when absent.
  - Unchanged methods, extended data: `key_for` still returns oct bytes only (a
    recipient or signing kid raises `UnknownKeyError`); `primary_pair()` keeps its
    result for an oct primary and raises `KeyringError` saying the primary is a
    recipient key otherwise; `kids()` returns the sorted kids of all three dictionaries.
    `install_keyring` logs the count of `kids()` instead of `len(keyring.keys)`.
  - `from_json` accepts, per JWK in `keys`:
    - `kty: oct` as today, plus an optional `flenc_scopes` member;
    - `kty: OKP` with `crv: X25519` → `recipients`, or `crv: Ed25519` → `signers`:
      `kid` and `x` required, `d` optional, both decoded with the existing strict
      `_decode_key` (generalize its `'k' must be a base64url string` message to name
      the member being decoded) (malformed base64url → `KeyringError`), `flenc_scopes`
      required by rule 6;
    - any other `crv` → `KeyringError` naming the curve; any other `kty` → `KeyringError`
      (reword the message to name the accepted kinds — tests only check the type).
    `flenc_scopes`, when present, must be a non-empty JSON array of non-empty strings.
    Duplicate kids across all kinds are rejected (existing message). Two optional
    top-level members: `flenc_primaries` and `flenc_signing`, each a JSON object of
    string to string, mapped onto `primaries` and `signing`. Unknown members stay
    ignored. A keyring document example for the tests and docs:

    ```json
    {
        "flenc_primaries": {"face_vector": "livia-2026-10"},
        "flenc_signing": {"face_vector": "box-17"},
        "keys": [
            {"k": "<32 bytes>", "kid": "prod-2026-07", "kty": "oct"},
            {"crv": "X25519", "flenc_scopes": ["face_vector"], "kid": "livia-2026-10", "kty": "OKP", "x": "<public>"},
            {"crv": "Ed25519", "d": "<seed>", "flenc_scopes": ["face_vector"], "kid": "box-17", "kty": "OKP", "x": "<public>"}
        ],
        "primary": "prod-2026-07"
    }
    ```

  - Tests in `tests/keyring_test.py`, every one named `test_asymmetric_*`: a keyring
    with all three kinds builds via `of` and via `from_json` and the two are equal;
    `kids`, `sealing_kid`, `signing_kid`, `recipient_for`, `signer_for`; a seal-only
    keyring (recipient primary, `private=None`) builds and its `primary_pair` raises;
    rules 1–8 each rejected (parametrize); `sealing_kid` refuses an unscoped value and
    an out-of-binding scope on a bound key; an oct key with `flenc_scopes` loads and
    binds; `from_json` rejects an unknown `crv`, a missing `x`, malformed `x`/`d`, a
    non-list, empty or non-string `flenc_scopes`, and non-object `flenc_primaries` /
    `flenc_signing`; two installs of equal asymmetric keyrings are idempotent.
  - Verify: `uv run pytest -q tests/keyring_test.py` passes, then commit.

- [ ] **2. Seal to recipients, enforce bindings on read** (`src/flechtwerk/secrets.py`, `src/flechtwerk/testing.py`, `tests/secrets_test.py`)
  - Constants (keep `_ALG`, `_ENC`, `_REGISTRY` and their names; Pre-Flight greps
    `_ALG = "dir"`): add `_RECIPIENT_ALG = "ECDH-ES+A256KW"` and
    `_RECIPIENT_REGISTRY = jwe.JWERegistry(header_registry=<the same flenc_scope header>, algorithms=[_RECIPIENT_ALG, _ENC], strict_check_header=True)`.
    Hoist the shared `header_registry` dict into one module constant used by every
    registry. Update the comment above `_REGISTRY` and the module docstring: two
    `(alg, enc)` pairs are allowed, and the **kind of the key behind the `kid`** selects
    which one — a token never chooses.
  - A private helper that builds a joserfc private OKP key **and checks the pair**,
    cached so the check runs once per key material:

    ```python
    @functools.cache
    def _okp_private_key(kid: str, crv: str, public: bytes, private: bytes) -> OKPKey:
        key = OKPKey.import_key({"crv": crv, "d": _b64url(private), "kty": "OKP", "x": _b64url(public)})
        if key.private_key.public_key().public_bytes_raw() != public:
            raise KeyringError(f"{crv} key {kid!r}: the private key does not match its public key")
        return key
    ```

    with `_b64url(b) = base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")`.
    `KeyringError` and `UnknownKeyError` come from `.keyring`. A mismatch is a keyring
    misconfiguration and propagates as `KeyringError`, unwrapped — like
    `KeyringNotInstalledError` today.
  - `_encrypt_payload(scope, payload)`: `kid = keyring.sealing_kid(scope)` (the binding
    refusal surfaces as its `KeyringError` at the write site). An oct kid seals exactly
    as today. A recipient kid seals with protected header
    `{"alg": _RECIPIENT_ALG, "enc": _ENC, "kid": kid}` plus `flenc_scope` when set, to
    `OKPKey.import_key({"crv": "X25519", "kty": "OKP", "x": _b64url(recipient.public)})`
    under `_RECIPIENT_REGISTRY`; when that recipient also holds a `private`, run
    `_okp_private_key` first so a mismatched pair fails at the writer too.
  - `_decrypt_to_payload`: after `kid = _read_kid(compact)`, a kid in
    `keyring.recipients` selects the recipient path — a `private` of `None` raises
    `SecretDecryptError(..., reason=f"keyring holds only the public half of {kid!r}: it can seal but not open")`;
    otherwise the key is `_okp_private_key(kid, "X25519", ...)` and the registry
    `_RECIPIENT_REGISTRY`. Every other kid goes through `keyring.key_for(kid)` exactly as
    today, so a signing kid or an unknown kid keeps raising `SecretDecryptError` with
    `UnknownKeyError` as `__cause__`. Keep wrapping every decrypt exception. After the
    existing scope ratchet, **enforce the binding**: when `keyring.scopes.get(kid)` is
    not `None` and the token's stamped scope (possibly `None`) is not in it, raise
    `SecretDecryptError(..., reason=f"key {kid!r} is bound to scopes {sorted(bound)}; token stamped {stamped!r}")`.
    The check runs after decryption because only then is the stamped header
    authenticated.
  - `reencrypt` needs no code change: it decrypts with the named kid and re-seals under
    `sealing_kid(scope)`, which moves a token from `dir` to a recipient, from one
    recipient to another, and back. Update its docstring.
  - Re-export `Ed25519Key` and `X25519Key` from `flechtwerk.secrets.__all__`, beside
    `Keyring`.
  - `flechtwerk.testing`: add `fixture_asymmetric_keyring(*, recipient_private: bool = True) -> Keyring`,
    exported beside `fixture_keyring` in `__all__`. It returns `Keyring.of(...)` with:
    oct `test-key` = `bytes(range(32))` as `primary` (unbound); recipient
    `test-recipient` (public decoded from `j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8`,
    private `bytes(range(32))` or `None`); signer `test-signer` (public decoded from
    `Kay64UG8yvCyLhqU000LxzYeUm0L_hLIl5S8kyKWbdc`, private `bytes(range(32, 64))`);
    `scopes={"test-recipient": {"api_key"}, "test-signer": {"api_key"}}`;
    `primaries={"api_key": "test-recipient"}`; `signing={"api_key": "test-signer"}`.
    Decode the public halves with `base64.urlsafe_b64decode(s + "=")` — no joserfc,
    no cryptography in `testing.py`.
  - Tests in `tests/secrets_test.py`, every one named `test_asymmetric_*`:
    - seal with a seal-only keyring (`recipient_private=False`), open with the full one;
    - a seal-only keyring cannot open — `SecretDecryptError` with the reason above;
    - token shape: the header is `alg ECDH-ES+A256KW`, `enc A256GCM`, `kid test-recipient`,
      `flenc_scope api_key`, carries an `epk`, and the second compact segment is non-empty;
    - algorithm confusion both ways: a `dir` token whose header names `test-recipient`
      (minted with `_independent_token`) and an `ECDH-ES+A256KW` token whose header names
      `test-key` (minted with joserfc directly) are both refused with `SecretDecryptError`;
    - hostile `epk`s — all-zero X25519, X448, P-256 — each refused with
      `SecretDecryptError` (parametrize; rewrite the header of a genuine token);
    - a mismatched recipient pair raises `KeyringError` on first use;
    - `dir` and recipient tokens decode side by side under one keyring;
    - binding on read: a recipient token stamped with another scope, and an unscoped
      recipient token (both minted with joserfc directly, bypassing the writer), are
      refused; a bound oct key refuses a token stamped outside its binding;
    - binding on write: an unscoped `ENCRYPTED` attribute under a keyring whose
      `primary` is a bound recipient raises `KeyringError` at encode;
    - per-scope sealing: one keyring seals `api_key` to the recipient and an unscoped
      attribute with `dir`;
    - the scope ratchet still blocks relocation for recipient tokens;
    - `reencrypt` moves a `dir` token to the recipient and a recipient token to a
      second recipient (switch `primaries` between installs);
    - `secret_decrypted` fires with `test-recipient` (use `RecordingObserver`);
    - `test_asymmetric_reads_pinned_panva_jose_token`: the ECDH vector decodes to
      `from-panva-jose` (store it as `_PANVA_JOSE_ECDH_TOKEN` beside the existing
      constant, with the regeneration comment);
    - `test_asymmetric_our_token_decodes_with_independent_reader`, using this verified
      reader built on pyca/cryptography alone:

      ```python
      def _independent_ecdh_es_a256kw_decrypt(compact: str, private: bytes) -> bytes:
          """RFC 7518 §4.6: Z = X25519(d, epk); KEK = Concat KDF(SHA-256, Z, alg id); CEK = unwrap; AES-GCM."""
          h64, ek64, iv64, ct64, tag64 = compact.split(".")
          header = json.loads(_unb64(h64))
          epk = X25519PublicKey.from_public_bytes(_unb64(header["epk"]["x"]))
          z = X25519PrivateKey.from_private_bytes(private).exchange(epk)
          alg = header["alg"].encode()
          apu, apv = _unb64(header.get("apu", "")), _unb64(header.get("apv", ""))
          other = (struct.pack(">I", len(alg)) + alg + struct.pack(">I", len(apu)) + apu
                   + struct.pack(">I", len(apv)) + apv + struct.pack(">I", 256))
          kek = ConcatKDFHash(algorithm=hashes.SHA256(), length=32, otherinfo=other).derive(z)
          cek = aes_key_unwrap(kek, _unb64(ek64))
          return AESGCM(cek).decrypt(_unb64(iv64), _unb64(ct64) + _unb64(tag64), h64.encode("ascii"))
      ```

      (imports: `struct`; `hashes` from `cryptography.hazmat.primitives`;
      `ConcatKDFHash` from `...kdf.concatkdf`; `aes_key_unwrap` from `...keywrap`;
      `X25519PrivateKey`, `X25519PublicKey` from `...asymmetric.x25519`; add a
      module-level `_unb64` (the same body as the local `unb64` in
      `test_our_token_decodes_with_independent_reader`) and leave that existing test
      untouched).
  - Verify: `uv run pytest -q tests/secrets_test.py tests/keyring_test.py` passes, then
    commit.

- [ ] **3. The `SIGNED` codec, its tooling and its telemetry** (`src/flechtwerk/secrets.py`, `src/flechtwerk/observer.py`, `src/flechtwerk/metrics.py`, `src/flechtwerk/testing.py`, `tests/secrets_test.py`, `tests/observer_test.py`)
  - Public constants beside `SCHEME`/`PREFIX`: `SIGNATURE_SCHEME = "flsig"` and
    `SIGNATURE_PREFIX = "flsig:jws:"` (the envelope segment `jws` names RFC 7515 compact
    serialization). Private: `_SIGNATURE_ALG = "Ed25519"` (RFC 9864 — never `EdDSA`,
    which joserfc flags as deprecated) and
    `_SIGNATURE_REGISTRY = jws.JWSRegistry(header_registry=<shared>, algorithms=[_SIGNATURE_ALG], strict_check_header=True)`.
  - Two exceptions, subclasses of `SecretError`, shaped like their encryption
    siblings: `SecretSignatureError` (keyword-only `scope`, `kid`, `topic`, `wire_key`,
    `reason`, message starting `cannot verify signed value`, the same `; `-joined
    details as `SecretDecryptError`) and `UnsignedSecretError` (`scope`, `topic`,
    `wire_key`; message `value is unsigned but read_unsigned is off for this attribute`).
  - `SIGNED[V](inner: Codec[V], *, scope: str, read_unsigned: bool = False) -> Codec[V]`
    returns a `_SignedCodec(Codec)` marker (frozen, slots, `scope: str = ""`, like
    `_EncryptedCodec`). An empty `scope` raises `ValueError` at declaration — a
    signature always names the compartment it vouches for.
    - encode: canonical JSON of `inner.encode(value)` — factor the existing
      `json.dumps(..., allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")`
      out of `ENCRYPTED` into one private helper both use; `kid = keyring.signing_kid(scope)`;
      the key is `_okp_private_key(kid, "Ed25519", signer.public, signer.private)`;
      header `{"alg": _SIGNATURE_ALG, "flenc_scope": scope, "kid": kid}`; return
      `SIGNATURE_PREFIX + jws.serialize_compact(...)`.
    - decode: a value under the `flsig:` scheme is verified (below) and its payload
      JSON-parsed into `inner.decode`; anything else is unsigned — with
      `read_unsigned=True` it logs a WARNING, fires `secret_unsigned_read(scope)` and
      decodes through `inner`; otherwise it raises `UnsignedSecretError(scope=scope)`.
  - Verification, a private `_verify_to_payload(scope, token)`: resolve the keyring
    first; strip the prefix — an unknown envelope under `flsig:` raises
    `SecretFormatError` exactly as `_compact` does for `flenc:`; read the `kid` with the
    existing `_read_kid` (generalize its docstring to both envelopes); a kid not in
    `keyring.signers` raises `SecretSignatureError` from `UnknownKeyError(kid)`; verify
    with the public key under `_SIGNATURE_REGISTRY`, wrapping every exception as
    `SecretSignatureError`; then require the stamped `flenc_scope` to **equal** `scope`
    (no ratchet — no unscoped signatures exist) and `scope` to be in the signer's
    binding, each failure a `SecretSignatureError` with a `reason`; finally fire
    `secret_verified(scope, kid)` and return the payload.
  - Tooling, mirroring the encryption tooling: `is_signed(value) -> bool` (a string
    under `flsig:`), `signer_of(token) -> str` (the signer's kid, no key needed;
    `SecretFormatError` for a non-`flsig` value), `sign_value(attribute, value) -> str`
    (requires a `_SignedCodec`, else `SecretError` — like `encrypt_value`). For an
    `ENCRYPTED(SIGNED(...))` attribute the write boundary stays `encrypt_value`.
  - `__all__` gains `SIGNATURE_PREFIX`, `SIGNATURE_SCHEME`, `SIGNED`,
    `SecretSignatureError`, `UnsignedSecretError`, `is_signed`, `sign_value`,
    `signer_of` — kept in `sorted()` order.
  - Telemetry, appended after `secret_decrypted` in each class: `Observer` gains no-op
    `secret_unsigned_read(self, scope: str)` and `secret_verified(self, scope: str, kid: str)`;
    `PrometheusObserver` increments new counters `flechtwerk_secret_unsigned_reads_total`
    (`scope`) and `flechtwerk_secret_verifications_total` (`scope`, `kid`) — two
    `@cached_property` methods in `metrics.py` after `secret_decrypts_total`, help texts
    in the style of their siblings; `RecordingObserver` records
    `("secret_unsigned_read", scope)` and `("secret_verified", scope, kid)`. Add one
    test per counter in `tests/observer_test.py` beside the existing secret ones, named
    `test_secret_unsigned_read_*` and `test_secret_verified_*`.
  - Tests in `tests/secrets_test.py`, every one named `test_signed_*`: round trip under
    `fixture_asymmetric_keyring` with the `flsig:jws:` prefix and `signer_of` →
    `test-signer`; empty scope → `ValueError`; tampered payload; a signature for scope
    `api_key` read by a `SIGNED(..., scope="other")` codec; a signer whose binding does
    not cover the scope (a second keyring); an unknown signer kid (cause
    `UnknownKeyError`); unknown envelope `flsig:xyz:`; an unsigned value rejected by
    default and accepted with `read_unsigned=True` (WARNING + `secret_unsigned_read`);
    encode without a signing key for the scope → `KeyringError`; `secret_verified`
    fires; `sign_value`, `is_signed`, and `sign_value` refusing a non-`SIGNED`
    attribute; `signer_of` raises `SecretFormatError` for a non-`flsig` value; `ENCRYPTED(SIGNED(STR, scope="api_key"), scope="api_key")` round trips,
    and its outer header names `test-recipient` while the signer kid appears nowhere in
    the token; `reencrypt` of a nested token keeps the inner signature verifiable;
    `test_signed_reads_pinned_panva_jose_token` (the JWS vector) and
    `test_signed_reads_pinned_panva_jose_nested_token` (the nested vector, through the
    nested attribute), both decoding to `from-panva-jose`;
    `test_signed_our_token_verifies_with_independent_reader`, using this verified
    verifier:

    ```python
    def _independent_ed25519_verify(compact: str, public: bytes) -> bytes:
        h64, p64, s64 = compact.split(".")
        assert json.loads(_unb64(h64))["alg"] == "Ed25519"
        Ed25519PublicKey.from_public_bytes(public).verify(_unb64(s64), f"{h64}.{p64}".encode("ascii"))
        return _unb64(p64)
    ```

  - Verify: `uv run pytest -q tests/secrets_test.py tests/observer_test.py` passes,
    then commit.

- [ ] **4. Document recipients, signatures and scope-bound keys** (`docs/concepts/secrets.md`, `docs/guides/observability.md`, `docs/api/index.md`, `CLAUDE.md`)
  - `docs/concepts/secrets.md` — keep its voice and structure; touch these places:
    - **Quick Start**: two sentences pointing at recipient keys (seal-only writers) and
      at `SIGNED`, linking to the new sections.
    - **The Wire Format**: the second generation — header with `alg ECDH-ES+A256KW` and
      an `epk`, a non-empty encrypted-key segment (the wrapped per-token content key).
    - **Scope (Domain Separation)**: under a recipient key, holding the *public* half is
      enough to mint a token with any content and any scope, so "a key-less attacker
      can neither mint nor strip" holds for `dir` only; relocation of an existing
      token is still blocked. Introduce key scope bindings here and link to the keyring.
    - **The Acceptance Rule**: exactly two `(alg, enc)` pairs for JWE, selected by the
      kind of key the `kid` names; exactly `Ed25519` for JWS; the binding check.
    - **The Prefix**: `flsig:jws:` for signed values, following the existing rule that
      `flenc:` is reserved for ciphertext forms.
    - **The Keyring**: the JWK Set with OKP keys, `flenc_scopes`, `flenc_primaries`,
      `flenc_signing` (the example from task 1), the eight validation rules in prose,
      seal-only and verify-only keyrings, and why public-key material must be bound.
    - New section **Signed Values** after "Declaring Encrypted Attributes": `SIGNED`,
      the exact scope rule, `read_unsigned` for migrating existing data, sign-then-encrypt
      as the recommended composition and why (signer kid hidden, re-seal blocked), and
      the trust rule — any signing key in the keyring whose binding covers the scope,
      with the per-box versus fleet-wide key trade-off (per-box: adding a box means
      updating every reader's keyring; fleet-wide: a stolen box can forge for the whole
      fleet within the scope).
    - **Rotating Keys**: recipient rotation (readers gain the new private key first,
      then writers switch `flenc_primaries`; `reencrypt` needs the old private key) and
      signer rotation (readers gain the new public key first, then the writer swaps its
      private key); destroying a recipient private key crypto-shreds everything sealed
      to it.
    - **Tooling**: `sign_value`, `is_signed`, `signer_of`; `reencrypt` across kinds.
    - **Post-Quantum Posture**: recipient tokens are X25519, so "harvest now, decrypt
      later" applies to them, unlike `dir`; signatures are not retroactively
      endangered by a future quantum computer the same way (forgery is forward-only);
      HPKE with a hybrid KEM is the way forward once joserfc can encrypt it, under a
      new `alg`.
    - **Scope & Caveats**: item 2 — the nonce budget does not apply to recipient tokens
      (each carries a fresh content key), which makes `Event` viable in recipient mode,
      subject still to items 1 and 3; item 4 — answered for writers holding only public
      keys, the operational-separation conditions stated there still apply, now without
      the "(HPKE)" qualifier; a new item: recipient tokens authenticate nothing about the
      writer — `SIGNED` or Kafka producer authentication does that.
    - **Not in v1**: HPKE / post-quantum hybrid; trust beyond the keyring (`x5c`, a CA);
      per-attribute signer allowlists; `scan_config_topics` for `SIGNED`-only
      attributes; recipient and signing key granularity as an application decision.
    - **Where It Lives**: the format commitment now covers two JWE generations and a
      JWS form; replace "a vendored ~50-line `dir`+A256GCM implementation" with "a
      vendored implementation of the three pinned forms (`dir`+A256GCM,
      `ECDH-ES+A256KW`+A256GCM over X25519, Ed25519 JWS)".
    - **Not in v1 → Per-tenant keys**: note that recipient keys now make
      crypto-shredding by key destruction available per recipient key (link to
      Rotating Keys).
  - `docs/guides/observability.md`: two rows in the Secrets table for the new counters.
  - `docs/api/index.md`: `::: flechtwerk.secrets.SIGNED`, `::: flechtwerk.keyring.X25519Key`,
    `::: flechtwerk.keyring.Ed25519Key`, `::: flechtwerk.secrets.sign_value`,
    `::: flechtwerk.secrets.is_signed`, `::: flechtwerk.secrets.signer_of`, each placed
    beside its encryption sibling.
  - `CLAUDE.md`: the `keyring.py` and `secrets.py` bullets in Architecture say what
    changed (key kinds, bindings, per-scope selection, `SIGNED`, `flsig:jws:`); the
    sentence calling `Event` unfit notes that the nonce half does not apply to recipient
    tokens; and in the `attribute/` bullet, change "`ENCRYPTED` is the one codec
    outside it" to "`ENCRYPTED` and `SIGNED` are the codecs outside it"; and in
    "Invariant: joserfc stays confined to flechtwerk.secrets", widen "(`flenc:jwe:`
    compact JWE, dir+A256GCM)" to name the `ECDH-ES+A256KW` generation and the
    `flsig:jws:` Ed25519 form, and say the panva-jose and independent-pyca vectors now
    pin all three.
  - Verify: `uv run poe docs-build` exits 0, then commit.

## Acceptance Criteria

Work criteria — re-run in full before the Closeout:

1. `uv run poe cov` exits 0; its pytest summary reports at least 650 passed and no
   failed; its coverage report shows `src/flechtwerk/keyring.py` and
   `src/flechtwerk/secrets.py` each at 100%.
2. `uv run pytest -q tests/keyring_test.py tests/secrets_test.py -k asymmetric` exits 0
   and reports at least 20 passed.
3. `uv run pytest -q tests/secrets_test.py -k signed` exits 0 and reports at least 14
   passed.
4. `uv run pytest -q tests/secrets_test.py -k panva` exits 0 and reports exactly 4 passed.
5. `uv run pytest -q tests/observer_test.py -k "unsigned_read or verified"` exits 0 and
   reports at least 2 passed.
6. `uv run python -c "import flechtwerk.module, flechtwerk.keyring, flechtwerk.testing, sys; print(sorted(m for m in sys.modules if m.startswith((chr(106)+chr(111)+chr(115)+chr(101)+chr(114)+chr(102)+chr(99), chr(99)+chr(114)+chr(121)+chr(112)+chr(116)+chr(111)))))"`
   prints `[]` — neither joserfc nor cryptography is loaded by the base install or the
   test helpers. (The `chr` spelling keeps the command free of quote characters.)
7. `uv run python -c "import flechtwerk.secrets as s, flechtwerk.keyring as k; assert s.__all__ == sorted(s.__all__) and k.__all__ == sorted(k.__all__); print(s.SIGNED, k.X25519Key, k.Ed25519Key)"`
   exits 0.
8. `uv run poe docs-build` exits 0.

Closeout criteria — hold only once the Closeout section has run, and are shown there:

9. The final state of this plan was committed, signed, in a commit of its own — the
   last commit in which the file exists. Shown by Closeout step 2.
10. `git status --porcelain` is empty — everything committed, plan file gone. Shown
    after Closeout step 5.

## Rollback

- Every change is local and unpushed on branch `main`. Undo with `git revert` of the
  work commits, newest first, each signed; nothing outside the repository changes.
  Pushing is the owner's step after the run, not part of it.

## Closeout

Run after the work is committed and the work criteria have been re-run and shown to
pass, and before this file is removed. This section is not a decision point: run the
recorded commands; if they cannot complete, the answer is the blocked report, not a
question. Run each `git` command on its own.

1. Bring this file to its final state: every task checkbox ticked, the Run Log
   complete, the outcome recorded, and `status: done` in the frontmatter.
2. Commit this file on its own — the preservation commit — then print its SHA:

   ```bash
   git add 2026-10-10-asymmetric-secrets-plan.md
   ```

   ```bash
   git commit -S -m "Record the final state of the asymmetric-secrets plan"
   ```

   ```bash
   git rev-parse HEAD
   ```

   It is a commit of its own because the final Run Log entries are written after the
   work commits. Do not squash it into a work commit and do not delete the file in it.
3. No ticket: no permalink is built, and nothing is pushed (`closeout_push` is false).
4. No ticket: no comment is posted.
5. Only now delete this file, in a **separate follow-up commit**:

   ```bash
   git rm 2026-10-10-asymmetric-secrets-plan.md
   ```

   ```bash
   git commit -S -m "Remove the asymmetric-secrets plan file"
   ```

   Then show `git status --porcelain` empty — closeout criterion 10.
6. No ticket: nothing to leave untouched.

If the preservation commit cannot be completed, **leave this file in place** and
write the blocked report — that commit is the only thing that preserves this record
once the file is gone.

## Execution Protocol

You are executing this plan file autonomously. There is nobody to ask, so decide
from what is written here plus the repository, and record what you decided.

1. Read this file completely before starting.
2. Run the **Pre-Flight** section first, in your first turn, before changing
   anything. Show each check and its real output. If any check fails, stop there:
   write the blocked report described in step 10, naming the failed check, what was
   expected, what was actually there, and what has to change about the launch. Do not
   start the tasks, do not substitute a different model or mode, and do not proceed
   at a lower spec — a mismatch discovered here costs a relaunch, and the same
   mismatch discovered at task 5 costs the whole run. When every check passes, set
   `status: executing` in the frontmatter — removing `blocked_report` if this relaunch
   just consumed one, since that key moves with the status — and append the gate's
   outcome to the Run Log — the file now says a run is under way, and a later reopen
   reads that rather than guessing. On a resume, continue at the first unticked task; a
   task the Run Log shows started but not finished is inspected before it is redone,
   because the tree may already hold part of it.
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
7. Commit as you go, on the current branch — `branch` in the frontmatter, which the
   gate confirmed — signed (`git commit -S`), with no ticket prefix: a task, or a
   coherent slice of one, per commit, so progress is durable and a resume or a
   rollback has real boundaries to work with. Run each `git` command on its own. Never
   create a branch: when this plan wanted one, the planning session created it at
   handoff and this file already sits on it.
8. Run the **Closeout** section — after the commit in step 7 and before you remove
   this file in step 9. That order is the whole point: this file is the record of the
   run and it is about to stop existing, so its final state is committed before
   anything is deleted. If the closeout cannot be completed, do not remove this file —
   write the blocked report in step 10 instead, naming the closeout as what failed.
9. When every acceptance criterion passes and the closeout is done, remove this plan
   file exactly as the Closeout section prescribes: in a separate commit after the
   preservation commit, never folded into it and never folded into the work commit.
   The plan is always tracked, so there is no `rm` path.
10. If a criterion cannot be made to pass — or the run cannot finish for any other
    reason: a denied command, a contradiction with the Verified Context that leaves the
    Goal standing but the plan wrong, a run that has to stop clean at a task boundary —
    write the blocked report at **exactly the path named in the goal condition**: the
    plan path with `.md` replaced by `.BLOCKED.md`, so
    `2026-10-10-asymmetric-secrets-plan.md` becomes
    `2026-10-10-asymmetric-secrets-plan.BLOCKED.md`. It names the failing check, what
    you tried, and why it cannot pass; lists the working tree as you leave it — every
    uncommitted file and the task it belongs to; and opens with a frontmatter block of
    exactly three keys, alphabetically — `date:` the day the run stopped; `next:`
    `relaunch` when the Handoff command can resume once the environment is fixed, or
    `reopen` when the plan itself has to change; and `plan_file:` the path of this plan,
    `2026-10-10-asymmetric-secrets-plan.md`, so the report names what it belongs to
    instead of leaving a reader to reconstruct it from the filename. Set
    `status: blocked` **and** `blocked_report:` naming the report you just wrote in this
    file's frontmatter — the two are written in the same commit and cleared in the same
    turn, so the plan and the report point at each other for exactly as long as both
    exist. Bring the Run Log current, and commit this file and the report together —
    the blocked commit, signed — leaving uncommitted only the work that has not reached
    a commit boundary. Then stop. Getting the path wrong is not cosmetic: the evaluator
    looks for the path the condition names, so a different spelling means the report
    does not register and the session cannot stop. Do not weaken a check, skip it, or
    declare success without it. A report written by mistake — on an estimate rather
    than a check that ran — is retracted in the same session: delete it, say so in the
    Run Log, and continue.

## Handoff

Run from the main checkout `/Users/christian/projects/bsure-analytics/flechtwerk`
(branch `main`). Cache the gpg passphrase first (Pre-Flight check 2).

```bash
claude --model opus --effort medium --permission-mode auto --remote-control 2026-10-10-asymmetric-secrets-plan '/goal Execute the plan at 2026-10-10-asymmetric-secrets-plan.md to completion. Before changing anything, run the Pre-Flight section of that plan and show the output of every check: the model must be claude-opus-5-5, the effort medium, the permission mode auto, and every privilege probe must pass. If any pre-flight check fails, write the blocked report named below and stop - do not start the tasks and do not continue on a different model or mode. Done when all of the following hold: the Pre-Flight checks were run first in this session and passed; every task checkbox in that file was ticked before the file was removed; after the last work commit and before the Closeout, each of these was run in this session with its output shown: uv run poe cov exited 0, its pytest summary reported at least 650 passed and none failed, and its coverage report showed src/flechtwerk/keyring.py and src/flechtwerk/secrets.py each at 100%; uv run pytest -q tests/keyring_test.py tests/secrets_test.py -k asymmetric exited 0 with at least 20 passed; uv run pytest -q tests/secrets_test.py -k signed exited 0 with at least 14 passed; uv run pytest -q tests/secrets_test.py -k panva exited 0 with exactly 4 passed; uv run pytest -q tests/observer_test.py -k "unsigned_read or verified" exited 0 with at least 2 passed; uv run python -c "import flechtwerk.module, flechtwerk.keyring, flechtwerk.testing, sys; print(sorted(m for m in sys.modules if m.startswith((chr(106)+chr(111)+chr(115)+chr(101)+chr(114)+chr(102)+chr(99), chr(99)+chr(114)+chr(121)+chr(112)+chr(116)+chr(111)))))" printed []; uv run python -c "import flechtwerk.secrets as s, flechtwerk.keyring as k; assert s.__all__ == sorted(s.__all__) and k.__all__ == sorted(k.__all__); print(s.SIGNED, k.X25519Key, k.Ed25519Key)" exited 0; uv run poe docs-build exited 0. Every existing test passed without being modified. The final state of the plan file was committed on its own, signed, in this session, and only then was the plan file deleted in a separate later signed commit; git status --porcelain was empty afterwards; all work was committed signed on the current branch main and nothing was pushed. Do not create a branch. Do not relax, skip, or declare passed any check that was not actually run, and do not route around a permission denial - a refused command is a blocked-report situation. If a pre-flight or acceptance check cannot be made to pass, write 2026-10-10-asymmetric-secrets-plan.BLOCKED.md in this session, naming a check this session ran, what was tried, and why it cannot pass, commit it with the plan, then stop - that also satisfies this goal. A blocked report left by an earlier launch does not.'
```

`/pdca:execute <this file>` runs that command as a background session with Remote
Control, so nothing needs pasting. If this plan file has been sitting for a while, do
not launch it blindly — reopen it first with `/pdca:plan <this file>` to re-verify the
Verified Context and get a freshened handoff.

Short form, for typing into a session already started with the flags above. The
evaluator then holds only the plan path, the protocol and the escape hatch — not the
acceptance criteria — so it has to trust this session's own account of what it ran.
Prefer the full command; use this when the full one is not to hand. A bare
`/goal 2026-10-10-asymmetric-secrets-plan.md` is not a launch at all: the evaluator
cannot open the file, so it would have nothing to judge.

```
/goal Execute the plan at 2026-10-10-asymmetric-secrets-plan.md to completion per its Execution Protocol: Pre-Flight first, every Acceptance Criterion shown passing in a final re-run, the Closeout done before the file was removed in a separate later commit; or 2026-10-10-asymmetric-secrets-plan.BLOCKED.md was written in this session.
```

## Run Log

- 2026-10-10, planning: first draft written. Open decisions 1 to 5 were not settled;
  the review loop had not run.
- 2026-10-11, planning (reopen): re-verified in the `encryption` worktree after
  fast-forwarding `worktree-encryption` to `main` (`231e7f5`); synced the venv on
  CPython 3.14. The owner settled the open decisions: `ECDH-ES+A256KW` now; optional
  signing as a separate `SIGNED` codec, sign-then-encrypt recommended; separate
  dictionaries per key kind; scope-bound keys, safe by design. Executor changed from
  Sonnet 5.5 at high to Opus 5.5 at medium; review budget stays 10. Draft rewritten
  around those decisions.
- 2026-10-11, planning: review round 1 (Opus 5.5, medium, plan mode, cold) → AGREED,
  no blockers. Nits applied as worded: the module-level `_unb64`, the rule-4 message,
  the `CLAUDE.md` `attribute/` bullet, `repr=False` on private key halves. Not applied:
  hard-coding a `Co-Authored-By` trailer into the commit commands — the executing
  session's harness supplies its own attribution trailers.
- 2026-10-11, planning: the owner said proceed on the reviewed version; Handoff
  written (condition 2676 characters, every acceptance criterion inlined), status
  `handed-off`, committed alone on `worktree-encryption`, not pushed.
- 2026-10-11, planning: the launch from the worktree was refused by that session's
  worktree-isolation guard. At the owner's request the plan was retargeted to the main
  checkout on `main` (already fast-forwarded to `c765986`): `branch`, the Ground check's
  directory, Rollback and the Handoff now name `main` and the main checkout.
- 2026-10-11, planning: review of the retargeted plan (Opus 5.5, medium, plan mode,
  cold) → AGREED, no blockers. Nits 1–5 applied as worded (`Where It Lives`, the
  `CLAUDE.md` joserfc invariant, the per-tenant-keys note, the `_decode_key` message,
  a `signer_of` test). Nit 6 not applied: a `git diff` criterion with no removed lines
  in `tests/` would fail on a legitimately widened import line, and "existing tests
  unmodified" is already in the goal condition. Committed on `main` and launched at
  the owner's request ("launch it when the review agrees").
- 2026-10-11, execution: Pre-Flight passed in the executing session — goal record
  `"met":false` naming this plan, `claude-opus-5-5`, `permissionMode auto`,
  `effort=medium`; `gpg-ok`; Docker `29.8.2`; remote `bsure-analytics/flechtwerk`,
  branch `main`, tree clean; Python `(3, 14)`; `_ALG = "dir"` at line 78; joserfc
  `1.7.4`; `uv run pytest -q` → `587 passed, 22 deselected`. No blocked report to
  consume. Status set to `executing`.
- 2026-10-11, execution: task 1 started.
- 2026-10-11, execution: task 1 done — `uv run pytest -q tests/keyring_test.py` →
  `63 passed`; keyring.py fully covered by the wider suite. Ticked. Task 2 started.
