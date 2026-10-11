---
created: 2026-10-10
executor:
  effort: high
  model: claude-sonnet-5-5
  permission_mode: auto
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
status: drafting
ticket: none
---

# Asymmetric `ENCRYPTED` tokens: writers that can seal but not open

## Goal

`flechtwerk.secrets` seals a value only as `dir` + `A256GCM`, so every writer holds a
key that also opens everything sealed under its `kid` — the sharpest limitation
`docs/concepts/secrets.md` names (Scope & Caveats, item 4). After this work, a
second token generation exists under the same `flenc:jwe:` prefix:
`alg: ECDH-ES+A256KW`, `enc: A256GCM`, sealed to an X25519 **recipient** key pair. A
writer whose keyring holds only the recipient's public key can seal and cannot open;
a reader holding the private key opens it. Both generations coexist in one topic,
the scope ratchet and the plaintext guard keep their meaning, and readers adopt the
new `alg` reader-first.

The first consumer is LIVIA (`livia-nt`), whose edge boxes seal a biometric
`face_vector` per event under a per-box symmetric key: a stolen box opens every
vector it ever sealed, adding a box means restarting the readers, and one key per
event stream runs into the AES-GCM nonce budget. An asymmetric recipient key answers
all three. The LIVIA migration is a separate plan, after a flechtwerk release.

## Requirements

No requirements source: no ticket, no spec. The plan was derived in conversation.

## Verified Context

All commands ran in `/Users/christian/projects/bsure-analytics/flechtwerk` on
2026-10-10, in a planning session under permission mode `auto`.

- The repository is a git repository on `main`, remote
  `https://github.com/bsure-analytics/flechtwerk.git`; commit subjects are plain
  sentences with no ticket prefix: `git rev-parse --show-toplevel`,
  `git branch --show-current`, `git remote -v`, `git log --oneline -20`.
- The acceptance rule pins exactly `dir` + `A256GCM` in one registry:
  `grep -n '_ALG = \|_ENC = \|_REGISTRY = ' src/flechtwerk/secrets.py` → lines 78, 79,
  86. Encryption is `_encrypt_payload` (line 216); decryption, the key lookup by `kid`
  and the scope ratchet are `_decrypt_to_payload` (line 227), which raises
  `SecretDecryptError` with an optional `reason`.
- `Keyring` is a frozen dataclass holding **oct keys only**: `keys: dict[str, bytes]`
  (32 bytes each) and `primary: str`; `from_json` rejects any `kty` other than
  `oct`; the module imports no cryptography and no joserfc by design:
  `sed -n 1,30p src/flechtwerk/keyring.py`, `grep -n 'kty' src/flechtwerk/keyring.py`.
- `flechtwerk.testing.fixture_keyring()` returns `Keyring.of({"test-key": bytes(range(32))}, primary="test-key")`:
  `grep -n 'def fixture_keyring' -A7 src/flechtwerk/testing.py`.
- The interop tests mint tokens with an independent pyca/cryptography encoder and pin
  a panva-jose `dir` token: `grep -n 'panva' tests/secrets_test.py`.
- flechtwerk locks joserfc 1.7.4: `grep -A1 '^name = "joserfc"' uv.lock`. Under it,
  `ECDH-ES+A256KW` with an X25519 key round-trips; decrypting with the public key
  alone raises `DecodeError`; an all-zero (low-order) `epk` raises
  `ValueError: Error computing shared key.` (a throwaway `uv run python -c` script).
- joserfc registers `HPKE-0` … `HPKE-4` but cannot encrypt with them:
  `UnsupportedAlgorithmError` (checked in joserfc 1.7.5). JOSE HPKE is therefore not
  an option yet.
- Cost, measured with joserfc 1.7.5 on the planning laptop for a 2731-byte payload:
  `dir` seals in 11 µs (token 3723 chars); `ECDH-ES+A256KW`/X25519 seals in 56 µs
  and opens in 39 µs (token 3905 chars).
- **A panva-jose interop token for the new mode** — minted with `jose` 6.2.12 (npm)
  in a scratch directory, and verified to decrypt with joserfc 1.7.4. Recipient key:
  X25519, private `d` = `bytes(range(32))` =
  `AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8`, public
  `x` = `j0DFrbaPJWJK5bIU6nZ6bslNgp09e14a0bpvPiE4KF8`, kid `test-recipient`, scope
  `api_key`, payload `JSON.stringify("from-panva-jose")`. The compact token (prefix
  it with `flenc:jwe:`):

  ```
  eyJhbGciOiJFQ0RILUVTK0EyNTZLVyIsImVuYyI6IkEyNTZHQ00iLCJraWQiOiJ0ZXN0LXJlY2lwaWVudCIsImZsZW5jX3Njb3BlIjoiYXBpX2tleSIsImVwayI6eyJ4IjoiQ05Kd3dRVkhjQzc4eUxLcFo3NkZYTFk2aVNkc1U5el90N2R0T0VIc2JHdyIsImNydiI6IlgyNTUxOSIsImt0eSI6Ik9LUCJ9fQ.25AYx4I-5kAOer_IxHPTEWdw1-06HwobWuxKoZST6Ck-7dh4XfdwZQ.XtWiNILmXZl60Vkl.Ysf9xPVrhFB_t2KoRNyh7Jc.ZrUOixIdsD2XSHONnGIzmw
  ```

  Minting script: `importJWK({kty:'OKP', crv:'X25519', x}, 'ECDH-ES+A256KW')`, then
  `new CompactEncrypt(...).setProtectedHeader({alg:'ECDH-ES+A256KW', enc:'A256GCM', kid:'test-recipient', flenc_scope:'api_key'}).encrypt(pub)`.
- Baseline: `uv run poe test-all` (unit and integration, needs Docker) passed 607
  tests in 57 s; `uv run coverage report` totals 97 %; `uv run poe docs-build`
  (strict) exits 0; the unit tier alone (`uv run pytest -q`) passed 585.
- Docker answers: `docker info` exits 0.
- **Commits are signed** (`git config --get commit.gpgsign` → `true`, signing key
  `4459B10B5535E920`), and non-interactive signing **failed** at planning time:
  `echo probe | gpg --batch --pinentry-mode error --local-user 4459B10B5535E920 --clearsign >/dev/null`
  exited non-zero. An unattended run can only sign with the passphrase cached in
  gpg-agent at launch; Pre-Flight check 2 tests exactly that.

## Constraints & Non-Goals

- Every commit is signed: `git commit -S` (flechtwerk `CLAUDE.md`). Launching phase 2
  is the owner's explicit request for its commits; never push.
- `keyring.py` stays free of joserfc and of any cryptography import. Key material is
  bytes; validation that needs cryptography (does a private key match its public
  key?) belongs in `secrets.py`.
- Backward compatible: existing `dir` tokens, existing keyring documents and
  `Keyring.of(keys, primary=...)` keep working unchanged; every existing test keeps
  passing.
- Alphabetical ordering of keys, constants and imports; `__all__` declares the
  public surface; comprehensions over accumulator loops; an empty line after every
  Markdown heading (flechtwerk `CLAUDE.md`, Code Style).
- Out of scope: **sender authentication** (asymmetric encryption alone says nothing
  about who sealed a token; documented as a caveat, not solved here); JOSE HPKE (not
  implementable with joserfc yet); keyring hot-reload; a release or tag; the LIVIA
  migration; any change to the `flechtwerk_keyring_keys_loaded` metric's labels.

### Open decisions — settled before this plan can be handed off

The tasks below assume the **proposed** answer to each. The follow-up planning
session confirms or changes them with the owner first, then adds exact code snippets
at the executor's altitude, and only then runs the review loop.

1. **`ECDH-ES+A256KW` now, or wait for JOSE HPKE?** Proposed: `ECDH-ES+A256KW` now,
   as one more `alg` that HPKE can join later. The cost to weigh: X25519 is not
   post-quantum, so long-lived sealed data (backups, recordings) is exposed to
   "harvest now, decrypt later", which `dir` is not.
2. **Sender authentication**: proposed out of scope for flechtwerk (documented), left
   to the application's transport. Alternatives: a separate signing codec (JWS nested
   in the JWE, Ed25519); ECDH-1PU (joserfc supports it, but it is an expired IETF
   draft — recommended against).
3. **`ECDH-ES+A256KW` or direct `ECDH-ES`**: proposed `+A256KW`, which the pinned
   panva token already uses.
4. **The keyring shape** — proposed in task 1: a second dictionary for X25519 keys
   beside `keys`, so `keys` and every existing caller stay as they are.
5. **Recipient key granularity** (per environment, per site, per period) is the
   application's choice and decides what destroying one key erases; flechtwerk only
   documents the trade-off.

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
turn, and only then run the checks.

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
   expect `"model":"claude-sonnet-5-5"`, `"permissionMode":"auto"`, `effort=high`.

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

3. **Ground.**
   - This file exists at `2026-10-10-asymmetric-secrets-plan.md`, the `plan_file` in
     its frontmatter.
   - `git remote -v` names `bsure-analytics/flechtwerk`, `git branch --show-current`
     prints the `branch` the frontmatter names, and `git status --porcelain` is empty —
     or this is a resume: the frontmatter says `status: executing` or `status: blocked`,
     and the only changes are this file, the blocked report just consumed, and the
     files the Run Log names as work in progress. Anything else dirty is a failed check.

4. **Preconditions from the Verified Context.**
   - `grep -n '_ALG = "dir"' src/flechtwerk/secrets.py` prints one line.
   - `grep -A1 '^name = "joserfc"' uv.lock` shows `version = "1.7.4"` or a later 1.7.x.
   - `uv run pytest -q` passes.

## Tasks

Ordered: the keyring shape (task 1) is what the codec (task 2) reads, the tests
(task 3) pin both, and the documentation (task 4) describes what the tests prove.
Each task ends in a signed commit.

- [ ] **1. Teach `Keyring` to hold X25519 recipient keys** (`src/flechtwerk/keyring.py`, `tests/keyring_test.py`)
  - Proposed shape (open decision 4): a frozen dataclass `X25519Key` with
    `public: bytes` (32 bytes) and `private: bytes | None` (32 bytes, or `None` for
    a seal-only keyring), exported in `__all__`; `Keyring` gains
    `recipients: dict[str, X25519Key]` with an empty default, after `primary`. `keys`
    keeps its meaning (oct keys only), so `Keyring.of(keys, primary=...)` is
    unchanged.
  - Validation in `__post_init__`: at least one key in `keys` or `recipients`; a `kid`
    appears in only one of the two; lengths are 32 bytes; `primary` names a held key
    of either kind.
  - `from_json` accepts `kty: OKP` with `crv: X25519`, a `kid`, `x`, and optionally
    `d`; it rejects any other `crv` with a `KeyringError` naming it, and keeps every
    existing error.
  - `kids()` lists the kids of both kinds; `primary_pair()` keeps working for an oct
    primary and raises a `KeyringError` saying the primary is a recipient key otherwise.
  - Verify: `uv run pytest -q tests/keyring_test.py` passes, with new tests named
    `test_asymmetric_*`.

- [ ] **2. Seal to a recipient and open with its private key** (`src/flechtwerk/secrets.py`)
  - Two registries replace `_REGISTRY`: the existing one for `(dir, A256GCM)`, and one
    for `(ECDH-ES+A256KW, A256GCM)`, both with the `flenc_scope` header parameter and
    `strict_check_header=True`.
  - `_encrypt_payload`: an oct primary seals as today; a recipient primary seals with
    `ECDH-ES+A256KW` to its public key, under the same `kid` and scope stamping.
  - `_decrypt_to_payload`: the kind of the key behind the token's `kid` selects the
    registry, so a token cannot choose a key of a type its `alg` did not expect. A
    recipient without a private half raises `SecretDecryptError` with the reason
    `keyring holds only the public half of <kid>: it can seal but not open`. Before the
    first use of a recipient key, check that its private key matches its public key.
  - `encrypt_value`, `reencrypt`, `kid_of`, `is_encrypted` and the scans keep working
    for both generations; `reencrypt` can move a token from `dir` to a recipient and
    from one recipient to another.
  - Verify: `uv run pytest -q tests/secrets_test.py` passes.

- [ ] **3. Pin the new mode with tests and interop vectors** (`tests/secrets_test.py`, `src/flechtwerk/testing.py`)
  - Add `fixture_recipient_keyring(private=True)` to `flechtwerk.testing`: kid
    `test-recipient`, the key pair from the Verified Context.
  - New tests, all named `test_asymmetric_*`: seal with a public-only keyring and open
    with the private one; a public-only keyring cannot open, with the reason above; a
    `dir` token under a recipient `kid` and an `ECDH-ES+A256KW` token under an oct `kid`
    are both refused; an `epk` on a different curve and a low-order `epk` are refused;
    `dir` and `ECDH-ES` tokens decode side by side; the scope ratchet holds for the
    new tokens; `reencrypt` both ways; the pinned panva token from the Verified Context
    decodes to `from-panva-jose`; a token sealed by flechtwerk decodes with an
    independent reader built on pyca/cryptography (X25519, Concat KDF per RFC 7518
    section 4.6 with algorithm id `ECDH-ES+A256KW`, AES key unwrap, AES-GCM).
  - Verify: `uv run poe test-all` passes.

- [ ] **4. Document the asymmetric mode** (`docs/concepts/secrets.md`, `CLAUDE.md`)
  - `secrets.md`: the wire format (the non-empty second segment, `epk`), the
    acceptance rule (two allowed pairs; the key's kind decides), the keyring (recipient
    keys, seal-only keyrings), rotating recipient keys and what destroying a private
    key erases, the post-quantum posture (this mode reintroduces "harvest now,
    decrypt later"), Scope & Caveats item 2 (no nonce budget per recipient key, since
    every token has a fresh content key) and item 4 (answered for writers holding only
    public keys), sender authentication as a stated caveat, and "Not in v1".
  - `CLAUDE.md`: the `keyring.py` and `secrets.py` bullets say what changed.
  - Verify: `uv run poe docs-build` exits 0.

## Acceptance Criteria

Work criteria — re-run in full before the Closeout:

1. `uv run poe test-all` exits 0 and its summary reports no failed test.
2. `uv run pytest -q tests/secrets_test.py tests/keyring_test.py -k asymmetric`
   exits 0 and reports at least 12 passed.
3. `uv run pytest -q tests/secrets_test.py -k panva` exits 0 and reports 2 passed.
4. `uv run poe docs-build` exits 0.

Closeout criteria — hold only once the Closeout section has run, and are shown there:

5. The final state of this plan was committed, signed, in a commit of its own —
   the last commit in which the file exists. Shown by Closeout step 2.
6. `git status --porcelain` is empty — everything committed, plan file gone. Shown
   after Closeout step 5.

## Rollback

- `git revert` the work commits, newest first, each signed. Nothing outside the
  repository changes, and nothing is pushed.

## Closeout

Run after the work is committed and the work criteria have been re-run and shown to
pass, and before this file is removed. This section is not a decision point: run the
recorded commands; if they cannot complete, the answer is the blocked report, not a
question.

1. Bring this file to its final state: every task checkbox ticked, the Run Log
   complete, the outcome recorded, and `status: done` in the frontmatter.
2. Commit this file on its own — the preservation commit:

   ```bash
   git add 2026-10-10-asymmetric-secrets-plan.md
   git commit -S -m "Record the final state of the asymmetric-secrets plan"
   SHA=$(git rev-parse HEAD); echo "$SHA"
   ```

   It is a commit of its own because the final Run Log entries are written after the
   work commit. Do not squash it into the work commit and do not delete the file in it.
3. No ticket: no permalink is built.
4. No ticket: no comment is posted.
5. Only now delete this file, in a **separate follow-up commit**:

   ```bash
   git rm 2026-10-10-asymmetric-secrets-plan.md
   git commit -S -m "Remove the asymmetric-secrets plan file"
   ```

   Then show `git status --porcelain` empty — closeout criterion 6.
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
   rollback has real boundaries to work with. Never create a branch: when this plan
   wanted one, the planning session created it at handoff and this file already sits
   on it.
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

Not written yet: this plan is a draft (`status: drafting`). The follow-up planning
session settles the open decisions, takes the plan through the review loop, and
writes the Handoff here — with `branch`, `closeout_push` and `permalink` in the
frontmatter — at handoff time. Reopen it with
`/pdca:plan 2026-10-10-asymmetric-secrets-plan.md`.

## Run Log

- 2026-10-10, planning: first draft written. Open decisions 1 to 5 are not settled;
  the review loop has not run; the plan is not committed.
