# Encrypted Secrets

Config records routinely carry secrets — API keys, OAuth tokens, database
passwords. A [config topic](config-topics.md) is readable by every
principal with a read ACL on it: every consumer, every topic-browsing UI, every
backup and mirror of the broker's log segments. TLS protects the wire and disk
encryption protects stolen hardware, but neither hides
`{"api_key": "sk-live-…"}` from anyone who can *read the topic* — which is
exactly the audience config topics are designed to have.

Flechtwerk encrypts confidential fields in place, declared per attribute by a
codec. The record stays browsable JSON with one opaque string where the secret
was; non-secret fields remain readable and editable in any topic UI.

!!! note "The `flechtwerk[secrets]` extra"

    Encrypted attributes require `pip install "flechtwerk[secrets]"` (it pulls
    `joserfc`). `flechtwerk.secrets` is the only module that imports it, so a
    stage without encrypted attributes never loads it. The generated surface is
    in the [API reference](../api/index.md#secrets); the [appendix](#why-attribute-level-encryption)
    records why this approach over a proxy or secret references.

## Quick Start

Declare a secret field with `ENCRYPTED(inner)`, wrapping the codec the value
would otherwise use:

```python
from typing import Final

from flechtwerk.attribute import Attribute, STR
from flechtwerk.secrets import ENCRYPTED

API_KEY: Final = Attribute("api_key", ENCRYPTED(STR))
```

`ENCRYPTED` composes like any codec and takes two optional knobs —
`ENCRYPTED(STR, scope="api_key")` binds the token to a
[compartment](#scope-domain-separation) so it can't be relocated into another
field, and `read_plaintext=True` tolerates legacy plaintext during a
[migration](#migrating-from-plaintext).

Reading and writing are both transparent, exactly as with a plain
`Attribute("api_key", STR)`: `config[API_KEY]` returns the decrypted `str`, and
`config[API_KEY] = "sk-live-…"` — like a typed literal
`Config({API_KEY: "sk-live-…"})` — stores the `flenc:jwe:…` token. Both
directions need a keyring. Build one and inject it (a mounted secret, a file, an
env var the *application* reads — the framework reads none of these itself):

```python
from pathlib import Path

from flechtwerk import Flechtwerk
from flechtwerk.secrets import Keyring

keyring = Keyring.from_json(Path("/etc/keys/flechtwerk.json").read_text())
await Flechtwerk.of(..., stage=my_stage, keyring=keyring).run()
```

A process that writes config without running a stage — a producer, ops tooling
— installs the keyring itself and then writes the same way:

```python
from flechtwerk import Config
from flechtwerk.secrets import install_keyring

install_keyring(keyring)
config = Config({API_KEY: "sk-live-…"})    # stores the "flenc:jwe:…" token
```

A writer that assembles the record without a `Record` — a form backend emitting
its own JSON, say — encrypts the single value with
`encrypt_value(API_KEY, "sk-live-…")`, which returns the same kind of token.

A writer that should seal but never open can hold only the public half of an
X25519 [recipient key](#the-keyring); and where a reader must know *who* wrote a
value, [`SIGNED`](#signed-values) adds an Ed25519 signature.

That is the whole application-facing surface. The rest of this page specifies
the wire format, the keyring, and the operational procedures (rotation,
migration) it supports.

## The Wire Format

An encrypted value is a JSON string: a URI-shaped prefix plus an RFC 7516
compact JWE.

```text
flenc:jwe:eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIiwia2lkIjoicHJvZC0yMDI2LTA3IiwiZmxlbmNfc2NvcGUiOiJhcGlfa2V5In0..<iv>.<ciphertext>.<tag>
```

JWE — JSON Web Encryption — is the encryption member of the JOSE family (its
sibling JWS *signs* a readable payload; JWE *encrypts* it). It is an
IETF-standardized string for a ciphertext plus everything needed to decrypt it.
The compact serialization is five base64url segments joined by dots:

| Segment | Content here |
| --- | --- |
| header | `{"alg": "dir", "enc": "A256GCM", "kid": "prod-2026-07", "flenc_scope": "api_key"}` |
| encrypted key | *empty* — `dir` means both sides already share the key |
| iv | fresh random nonce for this encryption |
| ciphertext | AES-256-GCM over the JSON-encoded inner wire value |
| tag | the GCM authentication tag |

A second token generation shares the prefix: a value sealed to an X25519
[recipient key](#the-keyring) carries `ECDH-ES+A256KW` instead of `dir`.

| Segment | Content in a recipient token |
| --- | --- |
| header | `{"alg": "ECDH-ES+A256KW", "enc": "A256GCM", "kid": "livia-2026-10", "flenc_scope": "face_vector", "epk": {…}}` |
| encrypted key | the per-token AES-256 content key, wrapped (AES-KW) under a key agreed between the ephemeral `epk` and the recipient |
| iv, ciphertext, tag | as above, under that fresh content key |

The writer needs only the recipient's *public* half; opening needs the private
half. Both generations coexist in one topic, and the kind of key the `kid` names
decides which one a reader accepts.

`flenc_scope` is an **optional** header — present only when the attribute
declares a [`scope`](#scope-domain-separation); a plain `ENCRYPTED(STR)` omits
it. `A256GCM` is an AEAD (authenticated encryption): the tag is computed over
the ciphertext **and** the protected header. Four properties follow:

- **Self-describing.** The header names the algorithm and the key id, so a
  reader years later needs no out-of-band convention to decrypt — and `alg`/
  `enc` are the built-in agility to swap algorithms later without a format break.
- **Tamper-evident header.** The header is readable but authenticated (it is
  the AEAD's associated data): flipping any bit of the ciphertext, IV, tag, or
  header fails tag verification and yields no plaintext. So the optional
  `flenc_scope` (below) is trustworthy — visible, but unforgeable and
  unstrippable.
- **Strictly accepted.** A reader enforces an allowlist (below) and rejects
  everything else before any cryptographic work.
- **Cross-language.** Compact JWE with `dir` + `A256GCM` is a few lines in
  every relevant ecosystem — `joserfc` (Python), `jose` (npm, for TypeScript
  producers), Nimbus JOSE+JWT (Java, e.g. a topic-UI serde). Flechtwerk defines
  no crypto format of its own; interop is regression-tested against a pinned
  panva-`jose` token and an independent AES-256-GCM encoder.

### Scope (Domain Separation)

`scope` is an **optional** per-attribute string — `ENCRYPTED(STR, scope="api_key")`
— stamped into the header as `flenc_scope` and, being part of the AEAD's
associated data, integrity-protected for free. It binds a token to a
compartment: a codec declaring one scope rejects a token stamped with a
*different* one, so a principal with a write ACL but *no keyring* can't relocate
an authentic ciphertext across differently-scoped fields (copy the `admin_token`
token into the `api_key` field). It is **opt-in** — the default (`scope=""`)
stamps nothing and binds nothing — because that protection is narrow: every
legitimate writer already holds the key (symmetric `dir`, so a key-holder can
just re-encrypt correctly), and it never covered cross-*tenant* relocation
(same scope, different record). Set a scope where you have write-capable-but-keyless
principals and want the extra hardening.

**Scope is a one-way ratchet**, enforced on decode:

- A **scoped** codec accepts a token stamped with the *same* scope, and *also*
  an **unscoped** token — the *upgrade* path. So a scope can be added to an
  already-encrypted field without a flag day: deploy the scope-reading codec
  (it still reads the old unscoped tokens), then sweep them into the scope with
  `reencrypt`. A key-less attacker can neither mint an unscoped token nor strip
  `flenc_scope` (it's in the AEAD's AAD), so once every token is scoped this is
  equivalent to strict.
- An **unscoped** codec accepts only unscoped tokens; a *scoped* token is
  rejected — the *downgrade* is blocked. A scope can't be silently dropped
  (and `reencrypt` can't strip one either), so removing protection fails loudly
  rather than passing unnoticed.

Decide `scope` up front, then: adding it later is a migration you can run, but
removing it is deliberately hard.

Under a [recipient key](#the-keyring) the "key-less attacker" premise thins:
holding the *public* half is enough to mint a token with any content and any
scope, so "can neither mint nor strip" holds for `dir` only. Relocating an
*existing* token is still blocked — its scope is authenticated like before — but
a recipient token says nothing about who wrote it ([`SIGNED`](#signed-values)
does). That is why a key itself can be **bound to scopes** in the keyring
(`flenc_scopes`): a reader rejects a token whose stamped scope lies outside its
key's binding, and a writer refuses to seal outside it. Recipient and signing
keys must be bound; oct keys may be.

### The Acceptance Rule

Binding on **all** implementations — the Python reader and any TS/Java producer
or serde alike:

- A JWE reader accepts exactly two `(alg, enc)` pairs: `dir` + `A256GCM` for a
  `kid` naming an oct key, and `ECDH-ES+A256KW` + `A256GCM` for a `kid` naming an
  X25519 recipient key. The **kind of key the `kid` names** selects the pair — a
  token never chooses, so neither generation can be passed off as the other. Any
  other `alg` or `enc`, any `zip` member, and any unrecognized `crit` member is
  rejected with a distinct error *before* any cryptographic work.
  `flechtwerk.secrets` pins each allowlist in its own `JWERegistry`, never
  library defaults.
- A JWS reader accepts exactly `alg: Ed25519` (RFC 9864 — never the deprecated
  polymorphic `EdDSA`), from a `kid` naming a signing key.
- The `kid` → key mapping is authoritative: a token cannot select a key of a
  type its `alg` did not expect.
- A key bound to scopes serves only those scopes: after decryption or
  verification (only then is the header authenticated), a token stamped outside
  its key's binding is rejected.
- Widening the allowlist (a future hybrid post-quantum mode) is a deliberate,
  reader-first deployment change — never something token content can trigger.

This is not about confidentiality (the header is authenticated, and every
symmetric `alg` still needs the key bytes — an attacker without the keyring can
only cause a crash). An *unpinned* reader is open to CPU-exhaustion via PBES2's
`p2c` iteration count — re-fired on every lazy `ConfigStore.get()`, wedging the
stage in a crash-loop while the poison record survives in the compacted topic —
and would silently accept future algorithms years before anyone decided it
should.

### The Prefix

Every token starts `flenc:jwe:`, a **valid URI** by construction: `flenc`
satisfies RFC 3986's scheme grammar and compact JWE's alphabet (base64url plus
dots) is entirely URI-unreserved, so the whole value parses with stock URI
tooling. The two segments split the naming duty:

- **`flenc`**, the scheme, is the Flechtwerk-encrypted-value namespace and the
  backend dispatch point. It is reserved for ciphertext token forms; a future
  non-encryption backend (a resolver, say) gets its own scheme, introduced
  reader-first.
- **`jwe`**, the envelope segment, names the encoding: always the RFC 7516
  *Compact* Serialization, under this profile (five segments, the acceptance
  rule). No sibling competes for the name — the JSON serialization is
  a JSON object, and this container requires a single-line string with no
  JSON-escaping hazards, so it is structurally excluded; a genuinely new
  envelope gets a new segment name, an incompatible profile revision would be
  `jwe2`.

Why a prefix at all: bare JWE starts with `eyJ` — as does every JWT on the
internet — useless to grep for. `flenc:` is distinctive in topics, logs, and
backups; it lets tooling scan a topic for unencrypted secrets offline; and it
makes *pasted plaintext in a secret field* a detectable error rather than
silent plaintext-at-rest (see [Ending the Transition](#ending-the-transition)).
The prefix sits *outside* the JWE and is **unauthenticated** — no
confidentiality or integrity property depends on it; the meaningful bindings are
the enforced `(alg, enc)` allowlist and, when set, `flenc_scope`. Classification is by scheme:
any value under `flenc:` is ciphertext-form (an unknown envelope segment raises
in every keyring mode, never falling through to plaintext), and any value under
a different scheme — a plaintext config value that happens to be a URL, say — is
not ciphertext-form at all.

Signed values follow the same rule with their own scheme: a
[`SIGNED`](#signed-values) value is `flsig:jws:<compact JWS>` — `flsig` because
`flenc:` is reserved for ciphertext forms, `jws` for the RFC 7515 Compact
Serialization. A sign-then-encrypt value is a `flenc:jwe:` token whose
plaintext is the `flsig:jws:` string.

Symmetric `dir` mode is a considered default: AES-256 is post-quantum-safe
(below). The second segment, empty under `dir`, is where the recipient
generation carries its wrapped content key, and where a hybrid post-quantum mode
would slot in later, under a different `alg`, with every token generation
coexisting in one topic.

## Declaring Encrypted Attributes

`ENCRYPTED(inner, *, scope="", read_plaintext=False)` is a plain codec
constructor exported from `flechtwerk.secrets`, exactly like `LIST` or `DICT`:

- **encode**: run `inner.encode`, JSON-serialize the result, encrypt under the
  keyring's primary key (stamping `flenc_scope` if a `scope` is set), emit the
  prefixed token.
- **decode**: check the prefix grammar, decrypt with the key the token's `kid`
  names, apply the [scope ratchet](#scope-domain-separation), JSON-parse, run
  `inner.decode`.

The wire form is a plain JSON string, so `Record`'s invariant that `.raw` stays
JSON-native holds untouched. Because it's an ordinary codec, it **composes
freely**: `ENCRYPTED(LIST(STR))` (one token wrapping the whole list),
`LIST(ENCRYPTED(STR))` (each element its own token), and `ENCRYPTED(RECORD)` all
work — with the caveat that `ENCRYPTED(RECORD)` is for a sub-object secret *in
its entirety* — it trades away per-field browsability inside.

The two optional knobs are covered above: [`scope`](#scope-domain-separation)
(relocation binding, a one-way ratchet) and `read_plaintext` (tolerate a legacy
plaintext value on read during a migration — see [Migrating From
Plaintext](#migrating-from-plaintext)).

Because the `ConfigStore` parses lazily on `get()`, decryption happens only when
the attribute is actually read — microseconds per read, dominated by token
parsing rather than AES-GCM. A pleasant side effect: anything that dumps
`Config.raw` into a log emits ciphertext.

### Where Encode Runs

The framework runtime never produces config records to Kafka — `encode` runs on
every typed write — `config[ATTR] = value`, typed `Config` literals (which run
codecs at construction), in-place writes inside `enrich_config` — in application
producers (typed writes, or `encrypt_value` for JSON built without a `Record`),
and in test seeding (`ConfigStore.of`). Two consequences:

- **Encode with no keyring installed raises** the same deployment error as
  decode — never a plaintext pass-through, which would silently produce the
  exact plaintext-at-rest the prefix guard exists to prevent. A `Config` literal
  with an `ENCRYPTED` attribute therefore cannot be a module-level constant
  evaluated at import time; the keyring (or the testing fixture) must be
  installed first.
- **Tokens pass through everything else untouched.** `Record.wrap`, the
  Record-copy constructor, `copy`/`deepcopy`, and dict-spread all move the
  wire-form string as-is; only typed-literal construction and `__setitem__`
  re-encrypt.

One testing consequence: `ENCRYPTED` makes the enclosing Record's value-equality
useless — encoding is randomized, so two Records built from the same plaintext
compare unequal on `.raw`, and `store.get(key) == expected_literal` assertions
always fail. Compare decoded attribute reads, never whole Records:
`store.get(key)[API_KEY] == "k"`. The fixture keyring cannot help — the nonce,
not the key, drives the inequality.

### Failure Semantics

Failures follow the framework's let-it-crash strategy, with the blast radius
stated honestly: one unreadable secret — pasted plaintext after strictness, a
garbled token, an unknown `kid` — crashes **every replica of every stage that
reads that attribute**, for all configs those stages process, and recurs on
every restart until the record or the keyring is fixed (config topics are
re-read in full on boot). Lazy decryption also lets such a record lie dormant on
a hot standby and detonate at failover. A garbled or pasted value is one
writer's data error with fleet-wide blast radius — the design accepts that trade
(silent plaintext is worse) and mitigates it three ways:

- Decode failures raise a **dedicated exception** — `SecretDecryptError`
  (`PlaintextSecretError` for the paste case) carrying the codec's `scope` and
  the token's `kid` (plus topic/wire_key when a caller such as the scan helper
  has them). This is the hook an application uses to quarantine a single config
  under the "only catch what you can remedy" rule. An unknown `kid` and a GCM
  tag failure are distinct incident signatures (missing key vs. wrong bytes for
  a known key), preserved on `__cause__`.
- **Write-side validation is the primary defense**: producers encrypt through
  the codec — a typed write or `encrypt_value` — so malformed tokens should never reach the
  topic; the read-side crash is the backstop.
- A **recovery runbook**: suspend or tombstone the offending record to restore
  the fleet, then re-produce it correctly encrypted (or fix the keyring). For
  pasted plaintext, follow the compromise procedure in
  [Ending the Transition](#ending-the-transition).

## Signed Values

Encryption says nothing about *who wrote* a value: under `dir` every writer is a
key holder anyway, and under a recipient key anyone holding the public half can
seal. `SIGNED(inner, *, scope, read_unsigned=False)` answers that question — a
plain codec constructor beside `ENCRYPTED`, wrapping the inner wire value in an
Ed25519 compact JWS, wire form `flsig:jws:<compact JWS>`:

```python
from flechtwerk.attribute import Attribute, FLOAT, LIST
from flechtwerk.secrets import ENCRYPTED, SIGNED

FACE_VECTOR = Attribute("face_vector", ENCRYPTED(SIGNED(LIST(FLOAT), scope="face_vector"), scope="face_vector"))
```

- **The scope is required and exact.** A signature always names the compartment
  it vouches for (`flenc_scope` in its protected header), and a reader accepts it
  only for exactly that scope — there are no unscoped signatures, so there is no
  ratchet.
- **The trust rule.** A reader accepts a signature from *any* signing key in its
  keyring whose binding covers the attribute's scope. Readers hold verify-only
  keys (public half); writers hold the private half and sign with the key
  `flenc_signing` names for the scope.
- **Sign, then encrypt** is the recommended composition (the RFC 7519 nested-JWT
  order): the signer's `kid` stays inside the ciphertext, and because the
  signature carries its own scope stamp, a reader that can decrypt cannot
  re-seal the value into another field and have it verify there. Every other
  composition stays legal.
- **`read_unsigned=True`** tolerates an unsigned value on read while existing
  data is migrated, loudly — a WARNING and `secret_unsigned_read` per read —
  exactly like `read_plaintext`; off, an unsigned value raises
  `UnsignedSecretError`. A failed verification raises `SecretSignatureError`.

Key granularity is the application's choice, with a trade-off: a **per-box**
signing key means adding a box updates every reader's keyring, but a stolen box
forges only as itself; a **fleet-wide** key spares the keyring updates, but a
stolen box can forge for the whole fleet within the scope.

## The Keyring

A key is 32 random bytes. The keyring document is an **RFC 7517 JWK Set** —
natively loadable by joserfc, panva jose, and Nimbus — with one extension
member: top-level `primary`. Encodings are pinned by JWK: `k` is unpadded
base64url; unknown members are ignored.

```json
{
    "keys": [
        { "k": "<base64url, 32 bytes>", "kid": "prod-2025-01", "kty": "oct" },
        { "k": "<base64url, 32 bytes>", "kid": "prod-2026-07", "kty": "oct" }
    ],
    "primary": "prod-2026-07"
}
```

Two more kinds of key are OKP JWKs, and three more members map scopes to keys:

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

- An **X25519 recipient key** (`crv: X25519`) seals with its public `x` and
  opens with its private `d`. Without `d` it is a **seal-only** key — what a
  writer holds; it cannot open a single token, its own included.
- An **Ed25519 signing key** (`crv: Ed25519`) signs with its private seed `d`
  and verifies with `x`. Without `d` it is a **verify-only** key — what a reader
  holds.
- **`flenc_scopes`** binds a key to the scopes it serves. Public-key material is
  *always* bound: holding a public half is enough to seal or to check, so an
  unbound one would serve every compartment. An oct key may be bound or not.
- **`flenc_primaries`** picks the sealing key per scope (falling back to
  `primary`); **`flenc_signing`** picks the signing key per scope. Both are
  explicit — a key is never chosen for a scope implicitly.

The keyring validates itself on construction and refuses: a keyring without an
oct or recipient key; an empty kid, or one kid held as two kinds; a key of the
wrong length (32 bytes for oct keys and for each half of an OKP key); a
`primary` that is not an oct or recipient key; a binding for a kid it does not
hold, or an empty one; an unbound recipient or signing key; a `flenc_primaries`
entry naming a non-sealing key or a scope outside that key's binding; and a
`flenc_signing` entry naming a key that is not a private signing key, or a
scope outside its binding. A private half that does not belong to its public
half is refused on first use.

Encryption uses the scope's sealing key and stamps its `kid`; decryption uses
whatever key the token's `kid` names. `Keyring` is a pure value object — key
bytes plus the scope mappings, nothing else; no crypto, no `joserfc` import — built
via `Keyring.of(...)` (raw bytes) or `Keyring.from_json(...)` (the JWK Set). The
framework never learns *where* the document came from: the application
constructs the keyring and injects it via `Flechtwerk.of(keyring=...)`, exactly
as it injects broker settings.

### Install Contract

Attributes are module-level constants, so key material cannot appear at
declaration time; codecs resolve the keyring lazily from a process-global
installed at startup:

- A stage installs the keyring at **startup** (`__aenter__`), **once per
  process**. A second install with byte-identical material is an idempotent
  no-op; a second install with *different* material raises immediately. "One
  keyring per process" is a stated constraint — two embedded modules with
  different keyrings must fail loudly, because silent last-writer-wins would
  encrypt under the wrong key and the in-process encrypt-then-decrypt round-trip
  would never notice. `Flechtwerk.of(...)` itself is side-effect-free —
  constructing a handle installs nothing — so tests and tooling can build one
  freely.
- Standalone producers and ops tooling call `install_keyring(...)` directly (no
  Flechtwerk handle needed).
- The secret **observer** is bound per stage, not per process: each stage's
  `__aenter__` binds its own into its task context, and asyncio hands that
  binding to every task the runner spawns, so `secret_decrypts_total` and
  `secret_plaintext_reads_total` carry the `metrics_labels` of the stage whose
  config read fired them — [several stages in one
  process](../guides/getting-started.md#several-stages-in-one-process)
  included. Outside any stage (ops tooling that only encrypts) the observer is
  the no-op default.
- `flechtwerk.testing.installed_keyring` is a context manager that installs a
  keyring and **restores the previous state on exit**, so test suites cannot
  leak keyrings across tests.

The keyring is process-global mutable state — the price of module-level
`Attribute` constants, distinct from `Stage.configs` (per-instance, injected by
its own runner) — and deliberately so: co-hosted stages reading one config
topic must agree on key material anyway. The observer already rides the
contextvar seam that could carry a per-stage keyring, should multi-keyring
processes ever be needed.

### Kid Hygiene

`kid`s must be unique across every environment that shares a config-promotion or
copy path — use environment-scoped names (`prod-2026-07`, `stage-2026-07`), not
bare dates that invite dev, stage, and prod to mint colliding names in the same
month. A `flenc:jwe:` token is portable only between deployments holding the
named key: promotion pipelines must re-encrypt in transit (`reencrypt`, below).
Two failure signatures are worth telling apart in an incident: *unknown kid*
(the key is absent) versus a *GCM tag failure* on a known `kid` (the same name
bound to different bytes — reads as "ciphertext tampered" and misdirects the
investigation).

## Rotating Keys

Compacted topics change the meaning of "rotate": old ciphertext survives until
someone re-produces the record, so rotation is keyring surgery, not a key swap.

1. Add the new key to every **reader's** keyring first (readers must know a key
   before any writer uses it — this ordering is the whole protocol).
2. Promote it to **primary** on the writers. New and edited records carry the new
   `kid`; old records keep decrypting under the old one.
3. Re-encrypt survivors — lazily via natural edits, or eagerly with a sweep
   (requirements below). At config-topic sizes that is seconds of runtime.
4. When the topic scan confirms no surviving token names the old `kid`, **remove
   it from active keyrings** (routine, reversible). **Destroying the key
   material** is a separate, deliberate gesture — crypto-shredding, the
   compromised-key procedure. Keep retired keys in offline escrow for at least
   the backup-retention window: a disaster-recovery restore from a pre-sweep
   backup resurrects old-`kid` ciphertexts, and with the key destroyed that is
   an unknown-`kid` fleet crash-loop mid-incident.

A *compromised* key is the same procedure executed immediately, sweep first. The
`kid` header makes each decrypt O(1) rather than trial-decrypting every key.

The same reader-first order governs the other kinds:

- **Recipient keys.** Readers gain the new private key first; then writers
  switch `flenc_primaries` for the scope to the new kid. `reencrypt` of a
  surviving token needs the *old* private key, so a sweep runs where that is
  held. Destroying a recipient private key crypto-shreds everything ever sealed
  to it — by design, and irreversibly.
- **Signing keys.** Readers gain the new public key first; then the writer swaps
  its private key and `flenc_signing`. `secret_verifications_total{scope, kid}`
  going flat for the old kid says the old signatures are gone.

Two operational rails:

- **Observability.** `keyring_keys_loaded{kid}` at startup makes step 1's
  completion checkable fleet-wide; `secret_decrypts_total{scope, kid}` makes
  "decrypts under the old kid are flat" a dashboard question before step 4. See
  [Observability](../guides/observability.md#secrets).
- **Rollback warning.** Between steps 1 and 4, rolling a reader back to a
  pre-rotation keyring while writers stamp the new primary is a deterministic
  unknown-`kid` crash-loop. Reader rollbacks must be coupled with demoting the
  writer primary first.

### Sweep Requirements

A sweep re-produces records, which has sharp edges the tooling must respect:

- Preserve record **key bytes** exactly.
- Produce each re-encrypted record **explicitly to the partition it was read
  from** — never via key hashing. Compaction is per-partition, the framework
  tolerates off-key-hash placement on config topics, and the bootstrap's
  cross-partition winner is undefined per boot — so a key-hashed re-produce can
  leave the stale-`kid` original alive on another partition and resurrect it
  nondeterministically after the old key is deleted.
- **Quiesce other config writers** for the sweep, or verify via end offsets
  afterwards that no writes interleaved and re-sweep if they did — a concurrent
  edit is otherwise silently reverted.
- Follow with the **topic scan** as verification before any key removal.

## Tooling

`flechtwerk.secrets` ships the primitives the runbooks build on (no CLI — those
belong in application tooling per the framework's boundary rule):

- `encrypt_value(attribute, value) -> str` — the token a typed write would
  store, for writers that build their JSON without a `Record`.
- `is_encrypted(value) -> bool` and `kid_of(token) -> str` — classification and
  scan building blocks.
- `reencrypt(token, attribute) -> str` — decrypt-with-named-kid,
  re-encrypt with the scope's sealing key: the building block for sweeps and
  cross-environment promotion. It moves tokens across key kinds — from `dir` to a
  recipient key, between recipient keys, and back — and keeps an inner signature
  intact, since the payload bytes pass through untouched.
- `sign_value(attribute, value) -> str` — the same for a `SIGNED` attribute (an
  `ENCRYPTED(SIGNED(...))` attribute still goes through `encrypt_value`).
- `is_signed(value) -> bool` and `signer_of(token) -> str` — classification and
  the signer's `kid`, no key needed.
- `scan_config_topics(consumer, topics, attributes)` — an async iterator of
  `ScanEntry` *(attribute, error, kid, partition, topic, wire key)* for every
  secret-bearing value, the engine behind the migration and rotation scans.
  `kid=None` with `error=None` marks a value still in plaintext; a set `error`
  marks a `flenc:` value whose `kid` could not be read (a corrupt token — it is
  reported, not raised, so one bad record never aborts the scan); a
  present-but-null field is skipped (absence, not a plaintext leak). Because the
  scan gates destructive steps, it stays a *complete* report: a missing topic
  raises rather than reading as a clean all-clear.

## Migrating From Plaintext

Existing deployments have config topics where the secret field is plaintext. The
codec swap is invisible on the wire — `Attribute("api_key", STR)` becomes
`Attribute("api_key", ENCRYPTED(STR))` under the same wire key — and
classification is by scheme (`flenc:` = ciphertext, everything else — plaintext
URLs included — a legacy candidate). The interesting question is not detection
but how tolerance *ends*, because "reader accepts plaintext" and "reader refuses
pasted plaintext" are opposites.

Tolerance is a per-attribute `read_plaintext` flag on the codec — declared in
code, visible in the attribute declaration, greppable and code-reviewed:

```python
API_KEY = Attribute("api_key", ENCRYPTED(STR, read_plaintext=True))   # transition
API_KEY = Attribute("api_key", ENCRYPTED(STR))                        # strict (default)
```

While `read_plaintext=True`, a non-`flenc:` value decodes through the inner
codec directly — each such read logs at WARNING *and* emits
`secret_plaintext_read`, so tolerance is loud even without dashboards. The
default is strict; greenfield attributes never set it. (There is no keyring
involvement and no dated expiry: the keyring is pure key material, and a dated
"kill switch" is either a silent leak — enforced only at construction — or a
scheduled crash. The WARNING and metric are the forcing function to turn it
off.)

Reader-first, like every rotation:

1. Deploy readers with `ENCRYPTED(..., read_plaintext=True)`. Nothing is
   encrypted yet; everything still works.
2. Flip the writers to encrypt. New and edited records go out as `flenc:jwe:`
   tokens.
3. Sweep the survivors (or let natural edits re-encrypt them lazily).
4. When `secret_plaintext_read` is flat and the topic scan finds no unprefixed
   secrets, drop `read_plaintext=True` and redeploy.
5. **Rotate the underlying credentials.** Any value that ever rested plaintext on
   the topic must be treated as disclosed: pre-compaction segments and every
   backup from the plaintext epoch retain it forever, and re-encrypting the
   surviving record reaches none of that. Encryption protects what is written
   *after* it; it cannot un-disclose the past.

### Ending the Transition

After step 4 the paste-guard is active — but it is an **exposure-window bound and
a remediation trigger, not prevention**. It fires at read, not write, so a
pasted secret is plaintext-at-rest and backup-captured from the paste instant,
and detection lags until something reads the record. When `PlaintextSecretError`
fires, the response is the compromise procedure:

1. Treat the pasted secret as **compromised** and rotate it at its source.
2. Re-produce the record encrypted with the **new** value.
3. Treat any backup taken during the window as containing the plaintext.

Flipping `read_plaintext` back on to silence the crash is the anti-pattern — it
re-opens the hole for that field. The sanctioned fleet-restoring move is
suspending or tombstoning the one offending record (see
[Failure Semantics](#failure-semantics)), which is just as fast and scoped to
the actual mistake. (`read_plaintext` is per-attribute, so at least the blast
radius of leaving it on is one field, not every secret at once.)

## Post-Quantum Posture

The scheme is purely symmetric: AES-256-GCM faces only Grover's algorithm
(≈128-bit effective security, considered adequate indefinitely — BSI TR-02102-1
prefers AES-256 for long-term confidentiality, and NIST IR 8547's 2030/2035
phase-outs target RSA/ECDH-class asymmetric schemes, which this design does not
use). "Harvest now, decrypt later" against topic backups is a non-threat *for
values that were born encrypted* — values that spent time plaintext are covered
by migration step 5, not by cryptography.

Post-quantum pressure enters only if asymmetric encryption is ever wanted — e.g.
producers that can encrypt but not decrypt, which symmetric keys cannot express.
The committed property is header-driven algorithm agility on a symmetric-only
scheme: the in-progress JOSE HPKE work (past working-group last call, not yet an
RFC; hybrid ML-KEM codepoints in the IANA HPKE registry) targets exactly this
extension point — the encapsulated key in the currently-empty second segment,
under a new `alg`, coexisting with `dir` tokens, adopted reader-first through the
acceptance-rule allowlist. If compact serialization ever cannot carry a future
mode, that is what a new envelope segment under `flenc:` is for.

**Recipient tokens are the exception.** `ECDH-ES+A256KW` over X25519 is exactly
the RSA/ECDH class those phase-outs target, so "harvest now, decrypt later"
applies to them, unlike `dir`: a token captured today opens once X25519 falls.
Use recipient keys where the write-surface separation is worth that horizon.
Signatures are different in kind — a future quantum computer could forge *new*
Ed25519 signatures, but a value verified today was not retroactively forged, so
the exposure is forward-only. HPKE with a hybrid KEM is the way forward once
joserfc can encrypt it, as one more `alg` beside the two.

## Scope & Caveats

1. **`Config` first, but `State` is usable.** JWE draws a fresh nonce, so
   encoding the same value twice yields different bytes. This matters only on an
   *explicit typed write* to the attribute (`state[SECRET] = value`): reading,
   carrying the state forward, deepcopy, and even dict-spread
   (`State({**state, OTHER: x})`, where the secret rides a pass-through
   `ViewAttribute`) all preserve the exact ciphertext token, so an untouched
   encrypted field re-serializes byte-identically and the extractor's
   persist-only-when-bytes-differ dedup keeps working. The non-determinism bites
   in one case — re-writing the secret from the same plaintext each cycle, which
   normal state-carrying patterns never do. So `State` is fine in practice;
   `Event` is not (next two caveats). JOSE has no deterministic mode (AES-SIV
   territory — a possible `ENCRYPTED_DETERMINISTIC` later) if byte-stable
   ciphertext is ever needed.
2. **Nonce budget.** AES-GCM with random 96-bit IVs is bounded to ~2³²
   *encryptions per key* (NIST SP 800-38D), and a collision is *catastrophic*
   (it leaks the XOR of two plaintexts and the GHASH subkey, enabling universal
   forgery). The governing quantity is the number of encrypt operations — i.e.
   *writes* — under one key, not the number of distinct values or state keys.
   State secrets are (re-)encrypted only on write (see above), which is rare, so
   even millions of keys stay far below the bound; an `Event` stream re-encrypts
   per message and blows it. (Value-domain cardinality is a red herring here —
   it only matters on the *determinism* axis: low cardinality plus a
   deterministic cipher would leak which entries share a value, which is a
   reason to keep encryption randomized, not a nonce concern.) `Event` support
   would need both a deterministic-AEAD answer and a per-key encryption-count /
   rotation policy first. The budget does **not** apply to recipient tokens:
   each carries a fresh, randomly drawn content key, so no key encrypts twice —
   which makes `Event` viable in recipient mode, subject still to items 1 and 3.
3. **Non-decrypting consumers.** An encrypted field is opaque to any consumer
   without the keyring — including analytics pipelines reading event topics
   directly. Fields such consumers need must stay plaintext or be decrypted by a
   transformer hop. For config topics, whose only readers are stages and humans,
   this is empty.
4. **Key holders can decrypt — and every writer is a key holder.** This is the
   design's sharpest limitation. What it *does* buy: disclosure narrows from the
   broad, low-privilege audience (Kafka-UI browsers, backup operators, arbitrary
   consumers, leaked log segments) to **keyring holders** — a real reduction
   against "curious employee" or "leaked backup" threats. What it does *not*
   stop: with symmetric `dir`, every config-*producing* surface (an ops laptop,
   a topic-UI serde, an internet-exposed self-service backend) holds the key
   that decrypts every secret under that `kid`, so compromising the most exposed
   writer discloses everything. There is also no per-decrypt audit trail; a
   transit-engine backend could sit behind the same codec seam if that is ever
   required. Two directions improve the shared-key problem, both conditional:

    1. **Asymmetric encryption** — [recipient keys](#the-keyring), available
       now — helps *only* to the extent the private
       (decrypt) key stays off the write surfaces — writers hold the public key
       and encrypt-only, the consuming stage holds the private key. Then a
       compromised writer leaks nothing already encrypted. But the moment a
       surface must decrypt (verify, edit-in-place, `reencrypt` during rotation)
       it needs the private key and the gain is gone — so it pays off only with
       strict operational key separation (rotation as an in-cluster job, laptops
       public-key-only), and it reintroduces the PQC concern.
    2. **Secret references** (the rejected [alternative](#why-attribute-level-encryption))
       are the *direct* answer to a compromised writer — the writer stores a
       pointer, never the secret or any decrypt key — but require the runtime
       secret store this design deliberately avoids.

5. **A256GCM is not key-committing.** Plain AES-GCM guarantees decryption fails
   under the *wrong* key, but not that a ciphertext maps to only one plaintext
   across *different* keys: an attacker holding two keys can craft one ciphertext
   that decrypts successfully — to different plaintexts — under each (the
   "invisible-salamander" class). Harmless here, because all keyring holders are
   mutually trusting, `kid` fixes which single key a token uses, and no property
   depends on a token *not* opening to something else under a key nobody has. It
   would matter only if mutually *distrusting* parties held different keys —
   untrusted per-tenant keys, or franking-style uses — which would need a
   committing AEAD under a new `enc`/`alg`, which the format's agility permits.
6. **Recipient tokens authenticate nothing about the writer.** Anyone holding
   the public recipient key can seal a token with any content and any scope it
   is bound to. Where provenance matters, sign the value with
   [`SIGNED`](#signed-values) — or rely on Kafka producer authentication and
   ACLs on the topic.

## Where It Lives

`flechtwerk.secrets` (the extra, the only `joserfc` importer) holds the codec,
tooling, and exceptions; `flechtwerk.keyring` holds the joserfc-free `Keyring`
value object and the process-global runtime — so `module.py` can annotate its
`keyring` slot at decoration time without importing joserfc (the same discipline
paho follows in `flechtwerk.mqtt`). The decade-scale commitment is to the
*format*, not the library: two JWE generations and one JWS form. joserfc could
be replaced by a vendored implementation of the three pinned forms (`dir`+A256GCM,
`ECDH-ES+A256KW`+A256GCM over X25519, Ed25519 JWS) without a wire change, which
the pinned panva-jose and independent-pyca interop vectors guard for each.

## Not in v1

- **Per-tenant keys.** The `kid` header makes per-tenant keys a keyring-policy
  change, not a format change — relevant only if crypto-shredding (erasure by key
  destruction) becomes a requirement (mind the key-commitment caveat first).
  Recipient keys already make crypto-shredding available per recipient key (see
  [Rotating Keys](#rotating-keys)).
- **HPKE and post-quantum hybrids.** joserfc cannot encrypt JOSE HPKE yet; it
  joins as one more `alg` when it can.
- **Trust beyond the keyring.** No certificate chains (`x5c`) and no CA: a
  signature is trusted because its key is in the keyring, bound to the scope.
- **Per-attribute signer allowlists.** Any signing key bound to the scope is
  accepted; narrowing that per attribute is not offered.
- **`scan_config_topics` for `SIGNED`-only attributes.** The scan classifies
  `flenc:` values; a signed-only attribute is not reported.
- **Key granularity.** Whether recipient and signing keys are per environment,
  site, box or period is an application decision — the trade-offs are in
  [Signed Values](#signed-values) and [Rotating Keys](#rotating-keys).
- **Keyring hot-reload.** The keyring loads at startup; rotation therefore
  implies a rolling restart — cheap for transformers and re-readable extractors,
  and cheap for an MQTT extractor too, whose persistent session holds its
  backlog across the restart (see [Sizing the Outage
  Budget](../guides/mqtt.md#sizing-the-outage-budget) for what bounds that).
  A file-watching reload could come later without API changes.
- **Nimbus (Java) interop vector.** The format is standard RFC 7516 dir+A256GCM,
  which Nimbus supports; a pinned Java-minted vector (like the panva one) is a
  follow-up once a JVM build exists in CI.

## Why Attribute-Level Encryption

Two alternatives were weighed and set aside; the reasoning is worth keeping.

**Topic-level encryption (a proxy).** Apache Kafka has no broker-side payload
encryption (KIP-317 was never adopted), and disk encryption only addresses
stolen hardware, not topic readers. A record-encrypting proxy (Kroxylicious) is
real and EOS-compatible, and for clients routed through it even browsability
survives — but it demands the KMS and proxy-fleet runtime dependency Flechtwerk
avoids, and any producer that bypasses the proxy lands plaintext in the
immutable log silently (where this design's read-time guard at least detects the
mistake). Where keeping plaintext off the broker log *entirely* is the
requirement, a proxy is the stronger tool.

**Secret references (indirection).** The industry-standard answer (Kafka
Connect's KIP-297) stores `${provider:path:key}` placeholders and resolves them
at use time; Kafka never holds the secret. But every deployment then needs a
runtime-reachable — and, for self-service writers, runtime-*writable* — secret
store, the config record stops being self-contained, dangling references fail at
resolve time, and a reference can never crypto-shred data already written to
immutable topics. It makes the confidentiality story conditional on external
infrastructure the framework otherwise doesn't need.

Encrypting the field itself keeps the record self-contained and browsable, the
topic contract unchanged, compaction and tombstones untouched, and adds no
infrastructure — and the typed-attribute layer already owns the exact boundary
where encode/decode happens. One lesson from the references option is kept: the
decrypt path is a single narrow seam, so a resolver-style backend could slot in
later (under its own `flenc`-sibling scheme) without touching the `Attribute`
API.
