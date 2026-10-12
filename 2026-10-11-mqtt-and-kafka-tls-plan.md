---
branch: worktree-mqtts
closeout_push: false
created: 2026-10-11
executor:
  effort: high
  model: claude-opus-5-5
  permission_mode: auto
permalink: none
plan_file: 2026-10-11-mqtt-and-kafka-tls-plan.md
planner:
  effort: medium
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

# TLS (and mutual TLS) for MQTT broker and Kafka broker connections

## Goal

Today Flechtwerk connects to both the MQTT broker and the Kafka broker in
plaintext only: `MqttBrokerConfig` has `broker`/`port`/`username`/`password`,
and `Flechtwerk.of` passes nothing but `bootstrap_servers` to aiokafka. After
this work an application can hand Flechtwerk a stdlib `ssl.SSLContext` for
each side — `MqttBrokerConfig(tls=...)` for the MQTT broker and
`Flechtwerk.of(kafka_tls=...)` for the Kafka broker — and get
server-authenticated TLS, or mutual TLS when the context carries a client
certificate. A context that would not authenticate the remote end is refused
loudly. Both paths are pinned by unit tests and by integration tests against
a Mosquitto and a Kafka broker that *require* client certificates, and the
docs and CLAUDE.md describe the feature.

## Requirements

No requirements source — no ticket, no spec. The user's decisions from the
planning session are recorded under Constraints & Non-Goals.

## Verified Context

All commands below ran in the planning session under permission mode `auto`
(the executor's mode), from the repository root
`/Users/christian/projects/bsure-analytics/flechtwerk/.claude/worktrees/mqtts`,
on branch `worktree-mqtts`, Docker Engine 29.8.2.

**Environment**

- `uv run python --version` → `Python 3.14.7`. uv's *default* interpreter on
  this machine is CPython 3.15, for which `rocksdict` has no wheel; the first
  sync failed with exactly that. The `.venv` was therefore created with
  `uv sync --python 3.14`, and plain `uv run …` now reuses it. If `.venv` is
  ever missing, recreate it with `uv sync --python 3.14` — never a bare
  `uv sync`.
- `uv run pytest -q` → `668 passed, 22 deselected in 5.32s` on HEAD `86bbe24`.
- `uv run pytest -q -m integration tests/integration/mqtt_delivery_test.py tests/integration/multi_stage_test.py`
  → `7 passed in 23.35s`.
- Full integration tier `uv run pytest -q -m integration` →
  `22 passed, 668 deselected in 53.35s`, nothing skipped.
- Acceptance criterion 6's paho probe was checked both ways: with
  `import flechtwerk` it exits 0; with `import flechtwerk.mqtt` it raises
  `AssertionError` — so it really detects a paho import. (It spells "paho"
  via `chr()` only to keep the goal condition free of quote characters.)
- The planning session ran as a worktree-isolated session, and its guard
  refused any compound command (`;`, `&&`, `$(…)`, sed with a `$VAR` path)
  that runs `git` or `uv`, because it could not prove the command stayed
  inside the worktree. The same commands ran fine as plain single commands,
  and compound commands that run neither `git` nor `uv` (the Pre-Flight
  transcript block) ran fine too. Run every `git` and `uv` command here as
  its own plain single command, exactly as written; never chain them.
- `uv run poe docs-build` (= `mkdocs build --strict`) → `Documentation built`,
  exit 0. `site/` and `.venv` are gitignored; `git status --porcelain` stays
  empty after a build.
- `cryptography>=44` is already a dev dependency (`pyproject.toml:9`);
  `testcontainers[kafka]>=4.8` (4.14.2 installed), `paho-mqtt~=2.1`,
  `aiokafka` 0.14.0. **No new dependency is needed.**

**MQTT side**

- `MqttBrokerConfig` is a `@dataclass(frozen=True, kw_only=True)` at
  `src/flechtwerk/module.py:56-119`, fields `broker`, `port`, `password`,
  `qos`, `session_expiry`, `username` (required first, then alphabetical; the
  docstring explains why `kw_only`). It lives in `module.py` so the container
  stays paho-free.
- `MqttConnection.__init__` (`src/flechtwerk/mqtt.py:198-238`) builds the paho
  `Client` and calls `username_pw_set` when a username is set;
  `__aenter__` (`:240-262`) logs `"Connecting to MQTT %s:%d as %s …"`, then
  `self.client.connect(...)` (blocking) and `sock.setblocking(False)`.
- The socket callbacks (`mqtt.py:423-438`) register
  `self.loop.add_reader(sock, client.loop_read)`.
- paho's `Client.tls_set_context(context)`
  (`.venv/lib/python3.14/site-packages/paho/mqtt/client.py:1182-1203`) must be
  called before `connect()`, raises `ValueError` if called twice, and sets
  `_tls_insecure = not context.check_hostname`. `_ssl_wrap_socket`
  (`:4642-4679`) wraps with `server_hostname=host`, runs a **blocking**
  `do_handshake()` inside `connect()`, and only calls the removed-in-3.12
  `ssl.match_hostname` when the context does *not* check hostnames — a path
  the insecure-context refusal below makes unreachable.
- **Spike (scratch, not in the repo):** `MqttConnection` with
  `client.tls_set_context(ctx)` added, against `eclipse-mosquitto:2`
  (2.1.2) with `require_certificate true`, mTLS context
  (`ssl.create_default_context(cafile=ca)` + `load_cert_chain(cert, key)`):
  subscribe, go offline, publish 300 QoS 1 messages, reconnect, drain+ACK →
  `received 300/300 in 0.78s, error=None`. With the pending-drain reader
  (below) → `300/300 in 0.80s`. So TLS over the existing asyncio socket
  integration needs nothing beyond `tls_set_context`.
- **Spike (scratch): MQTT without a client certificate** against the same
  `require_certificate true` listener, context
  `ssl.create_default_context(cafile=ca)` with no `load_cert_chain`:
  under the default TLS 1.3, `MqttConnection.__aenter__` **returns
  normally** (TLS 1.3 servers reject a missing client cert after the client
  side of the handshake completes, and `__aenter__` never reads), and the
  rejection surfaces on the first read: `drain()` raised
  `ConnectionError: MQTT disconnected: Unspecified error` 0.06 s later. With
  `ctx.maximum_version = ssl.TLSVersion.TLSv1_2`, `__aenter__` itself raised
  `ssl.SSLError: [SSL: SSLV3_ALERT_HANDSHAKE_FAILURE]`. The test below pins
  the TLS 1.3 behaviour, which is what a real deployment gets.
- **Latent hazard, read from paho's source, NOT reproduced:** `loop_read`
  (`client.py:2082-2105`) reads `max(1, len(out)+len(in))` packets per call.
  On an `ssl.SSLSocket`, bytes OpenSSL already decrypted into its buffer do
  not make the fd readable again, so after one packet the rest of a
  multi-packet TLS record can sit unread until the next network event (up to
  the 60 s keepalive). paho's own `_loop` guards this with `sock.pending()`
  (`client.py:1646-1653`); our `add_reader` path has no such guard. Mosquitto
  writes one TLS record per MQTT packet, which is presumably why the spike
  did not stall. The fix is defensive and cheap; it is pinned by a unit test,
  not an integration test.

**Kafka side**

- `Flechtwerk.of` (`module.py:260-332`) sets one lookup field per parameter on
  `_FlechtwerkModule`; lookups are declared at `module.py:368-382`
  (`keyring: lookup[Keyring | None]` and `mqtt: lookup[MqttBrokerConfig | None]`
  are the optional-object precedent).
- aiokafka clients are constructed at **nine** sites, all in `module.py`:
  `grep -n "bootstrap_servers" src/flechtwerk/module.py` → lines 400, 449,
  473, 496, 512, 527, 565, 612, 677 (consumers, kwargs-dict producers, and two
  `AIOKafkaAdminClient(bootstrap_servers=...)`). No other `src/` module builds
  a Kafka client; `secrets.scan_config_topics` takes a caller-built consumer.
- aiokafka TLS = `security_protocol="SSL"` + `ssl_context=<SSLContext>` on
  every client (`aiokafka/consumer/consumer.py:165-168,258-259`).
- `__aenter__` runs `validate_topics(self.stage)` and
  `validate_poll_interval(...)` at `module.py:675-676` — the place for a
  container-level startup check, reached by both `Flechtwerk.of` and
  parent-module embedding.
- **Spike (scratch):** `testcontainers.kafka.KafkaContainer`
  (`confluentinc/cp-kafka:7.6.0`) subclassed so `tc_start` first writes two
  PEM files with `create_file`, constructed with
  `listener_name="TLS", security_protocol="SSL"` and env
  `KAFKA_SSL_KEYSTORE_TYPE=PEM`,
  `KAFKA_SSL_KEYSTORE_LOCATION=/etc/kafka/tls/keystore.pem` (PKCS#8 key +
  cert, unencrypted), `KAFKA_SSL_TRUSTSTORE_TYPE=PEM`,
  `KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/tls/truststore.pem`,
  `KAFKA_SSL_CLIENT_AUTH=required`, plus the replication/ISR envs the
  existing fixture sets (four, `tests/integration/conftest.py:42-45`): admin `create_topics`,
  a **transactional** producer and a `read_committed` consumer all
  round-tripped over mTLS (`mTLS roundtrip: [b'hello']`), and an admin client
  without a client certificate failed with `KafkaConnectionError: Unable to
  bootstrap`. The listener is named `TLS`, not `SSL`, on purpose: the
  Confluent image's configure script demands JKS keystore env vars whenever
  the advertised listeners contain `SSL://`.

**Test certificates**

- Python ≥ 3.13's default context sets `VERIFY_X509_STRICT`. Certificates
  generated with `cryptography` failed verification until they carried
  **AuthorityKeyIdentifier**, **SubjectKeyIdentifier** and a critical
  **KeyUsage** (CA: `key_cert_sign`+`crl_sign`; leaf: `digital_signature`)
  next to `BasicConstraints`. Errors seen on the way:
  `Missing Authority Key Identifier`, then
  `CA cert does not include key usage extension`. The server cert needs SANs
  `DNS:localhost` and `IP:127.0.0.1` (`get_container_host_ip()` returned
  `localhost`). EC P-256 keys worked.
- Mosquitto drops root privileges: the server key must be
  `chown mosquitto` (+ `chmod 600`), or it fails with
  `Unable to load server key file … Permission denied`.
- Container files must be injected without bind mounts (the existing
  `mosquitto_broker` fixture's docstring, `mqtt_delivery_test.py:61-71`,
  explains: bind mounts break on Docker-outside-of-Docker CI). Mosquitto:
  `printf %s <shlex.quote(pem)> > /tmp/…` chained in the `sh -c` command, as
  the existing fixture does for its config. Kafka: `create_file` (a
  `put_archive`), as testcontainers itself does for `tc-start.sh`.

## Constraints & Non-Goals

Decisions the user made in the planning session — do not reopen them:

- **API shape: a stdlib `ssl.SSLContext`, not path fields.**
  `MqttBrokerConfig.tls: ssl.SSLContext | None = None` and
  `Flechtwerk.of(..., kafka_tls: ssl.SSLContext | None = None)`. The caller
  builds the context (CA file or system CAs, client cert for mTLS, TLS
  version policy); the framework reads no files and grows no
  `ca_file`/`cert_file`/`key_file`/`insecure` vocabulary. `None` means
  plaintext, exactly as today. The Kafka parameter is `kafka_tls`, never a
  bare `tls`, because on `Flechtwerk.of` a bare `tls` would be ambiguous
  between the MQTT broker and the Kafka broker (this repo's rule: never an
  undisambiguated two-referent noun).
- **Refuse an unauthenticating context.** A context whose
  `verify_mode != ssl.CERT_REQUIRED` or whose `check_hostname` is false is
  rejected with a teaching `ValueError` (encryption without authentication is
  not offered; a self-signed MQTT broker or Kafka broker is trusted via
  `cafile`, a cert reached by IP needs an IP SAN). A value that is not an
  `ssl.SSLContext` at all is a `TypeError`. One shared internal validator in
  `module.py` (stdlib-only, so `module.py` stays paho-free) — not in `__all__`. MQTT: called from
  `MqttBrokerConfig.__post_init__` (fails at construction). Kafka: called in
  `_FlechtwerkModule.__aenter__` beside `validate_topics` /
  `validate_poll_interval`, so parent-module embedding is covered too.
- **Both brokers in this plan** — MQTT and Kafka.
- Out of scope: SASL (Kafka `SASL_SSL`/`SASL_PLAINTEXT`, SCRAM, OAUTHBEARER),
  MQTT over WebSockets, TLS-PSK, certificate hot-reload. A rotated
  certificate takes effect at the next process start (an `SSLContext` loads
  files once) — document that, do not build reloading. No new dependency.
- Keep `MqttBrokerConfig` fields alphabetical after the required ones: `tls`
  goes between `session_expiry` and `username`. Keep `Flechtwerk.of`
  parameters alphabetical: `kafka_tls` goes between `compression_type` and
  `keyring`; the matching `kafka_tls: lookup[ssl.SSLContext | None]` goes in
  the same alphabetical slot among the lookups.
- Repo conventions from `CLAUDE.md` apply: `X | None` typing, comprehensions
  over accumulator loops, empty line after Markdown headings, files end with
  a newline, every commit PGP-signed (`git commit -S`), **never push**.
- The invariant "paho-mqtt stays confined to flechtwerk.mqtt" must hold:
  `module.py` may import `ssl` (stdlib) but never `.mqtt` at module level.
- **`kafka_tls` is an ordinary required lookup, like `keyring`** (decided
  after review round 2 raised it). reactor-di raises `AttributeError` for a
  `lookup` field nobody set (`.venv/lib/python3.14/site-packages/reactor_di/module.py:30-45`),
  and `kafka_tls` will be read on every Kafka client build and in
  `__aenter__`. The repo already lives with this for `keyring`, which
  `__aenter__` reads unconditionally: hand-built containers in tests set
  `mod.keyring = None` (`tests/module_test.py:270`), and a parent module
  embedding `make[Flechtwerk, _FlechtwerkModule]` must wire every lookup the
  stage reaches. Do NOT invent a class default or a `__dict__.get` fallback
  for `kafka_tls` — that is unverified reactor-di territory and would make it
  the one lookup a parent cannot inject. Consequences, all in scope:
  - Existing tests that hand-build `_FlechtwerkModule()` and then build a
    Kafka client or enter it get exactly one added line,
    `mod.kafka_tls = None`, and nothing else. Known ones:
    `tests/module_test.py` — both `test_ensure_changelog_creates_the_topic_and_closes_its_admin`
    definitions (`:217`, `:238`; the name is duplicated, leave that alone),
    `test_aenter_calls_ensure_topics_under_admin_try_finally` (`:259`),
    `test_membership_consumer_exists_only_for_extractors` (`:286`) (tests
    that build through `Flechtwerk.of` — e.g.
    `test_consumer_caps_getmany_batches` — need nothing; `of` sets
    `kafka_tls`);
    `tests/state_test.py` — `test_module_wires_extractor_runner_state_collaborators`
    (`:472`). Any other existing test that fails ONLY with
    `AttributeError: … 'kafka_tls'` gets the same one-line addition; a test
    failing any other way is a real regression to fix in the source.
  - Add `"kafka_tls"` to the name tuple in
    `test_bare_constructor_leaves_every_lookup_unbound_for_parent`
    (`tests/state_test.py:632`, alphabetical slot).
  - The docs (task 5) say that an embedding parent module must now wire
    `kafka_tls` (to `None` for plaintext).
  Apart from these additions, do not change existing tests, fixtures, or
  `secrets.scan_config_topics`; the existing plaintext fixtures stay as they
  are.
- The Python/rocksdict issue above is environmental; do not add a
  `.python-version` file or touch `requires-python`.

## Pre-Flight

Before check 1: if the frontmatter carries a `blocked_report`, this is a
relaunch. Fold that report into the Run Log below — its `date`, the check it
named, what was tried, its `next` — then delete it and remove
`blocked_report` from the frontmatter, in this same turn, and only then run
the checks. The key and the report are consumed together, so a plan never
names a report that is no longer there. An inherited report never satisfies
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

   Expected: `"met":false` with a condition naming this plan file — the last
   goal set in this session is this plan's and was never met and never
   cleared. A last record with `"met":true`, a different goal's condition, or
   no record at all in a readable transcript each mean no evaluator guards
   this run: stop and write the blocked report; the fix is relaunching with
   the command in the Handoff section. If the `T=` line prints empty, the
   transcript could not be found and none of the transcript-derived facts can
   be read — they are unverifiable, not false: record that in the Run Log,
   take the model from what this session states about itself, the effort from
   `CLAUDE_EFFORT`, and the permission mode from the launch flags described
   below; treat the goal check as unverifiable, and continue. Otherwise expect
   `"model":"claude-opus-5-5"`, `"permissionMode":"auto"`, `effort=high`.

   The `permissionMode` line prints nothing in a headless (`-p`) session
   started with a slash command as its prompt; the Handoff command launches
   interactively, where it does print. If it is empty anyway, fall back to the
   launch flags, walking up from `$PPID` until the argv starts with `claude`,
   and matching `--permission-mode <mode>` or
   `--dangerously-skip-permissions`. If neither source answers, the mode is
   unverifiable: say so rather than assuming it.

2. **Privileges** — skip this group only if the mode above is
   `bypassPermissions`, and say in the Run Log that you skipped it.
   - `docker info --format "{{.ServerVersion}}"` prints a version (the
     integration tier needs Docker; without it the TLS tests *skip*, and a
     skip does not satisfy the acceptance criteria).
   - `gpg --batch -u 4459B10B5535E920 -o /dev/null --detach-sign CLAUDE.md`
     exits 0 — every commit is signed (`commit.gpgsign` is `true`, key
     `4459B10B5535E920`); in planning this succeeded through the agent's
     pinentry, while `--pinentry-mode error` failed with `No pinentry`, so a
     hang or failure here means the passphrase is not reachable unattended.
   - `uv run pytest -q` exits 0 — proves the venv and the test command run
     under this mode. (No push, no network write, no MCP call is prescribed
     anywhere in this plan; the closeout does not push.)

3. **Ground.**
   - This file exists at `2026-10-11-mqtt-and-kafka-tls-plan.md`, the
     `plan_file` in its frontmatter, at the repository root.
   - `git rev-parse --show-toplevel` ends in `flechtwerk/.claude/worktrees/mqtts`,
     `git branch --show-current` prints `worktree-mqtts`, and
     `git status --porcelain` is empty — or this is a resume: the frontmatter
     says `status: executing` or `status: blocked`, and the only changes are
     this file, the blocked report just consumed, and the files the Run Log
     names as work in progress. Anything else dirty is a failed check.

4. **Preconditions from the Verified Context.** The two `grep` checks below
   apply to a **fresh start only** (no task ticked). On a resume they are
   expected to differ — task 1 adds `tls` text, task 3 drops the count to 1 —
   so skip them, say so in the Run Log, and instead confirm that
   `git log --oneline` shows a work commit for every ticked task. The Python
   check applies always.
   - `uv run python --version` prints `Python 3.14.x` (if `.venv` is gone:
     `uv sync --python 3.14`, then re-check).
   - `grep -c "bootstrap_servers=self.bootstrap_servers\|\"bootstrap_servers\": self.bootstrap_servers" src/flechtwerk/module.py`
     prints `9` — the nine aiokafka construction sites are still where this
     plan says.
   - `grep -n "tls" src/flechtwerk/mqtt.py src/flechtwerk/module.py` prints
     nothing — no TLS support exists yet.

## Tasks

The order matters: task 1 creates the validator that tasks 2 and 3 call;
task 4's integration tests exercise what 2 and 3 build; docs come last so
they describe what was actually built.

- [x] **1. Shared TLS validator in `module.py`**
  - `import ssl`; add an internal function (not in `__all__`), e.g.
    `validate_tls(context: ssl.SSLContext | None, name: str) -> None`:
    `None` passes; a non-`SSLContext` raises `TypeError`;
    `verify_mode != ssl.CERT_REQUIRED` or `not check_hostname` raises
    `ValueError` whose message names the parameter (`"mqtt.tls"` /
    `"kafka_tls"`), says the context would not authenticate the remote
    broker, and says what to do instead (trust the CA via `cafile`; add an IP
    SAN rather than disabling hostname checks). Note that
    `ssl.create_default_context()` already satisfies it.
  - Unit tests in `tests/module_test.py`: `None` passes; a default context
    passes; `CERT_NONE` (requires `check_hostname = False` first),
    `CERT_OPTIONAL`, and `check_hostname = False` each raise `ValueError`;
    a non-context raises `TypeError`.
  - Commit.

- [ ] **2. MQTT broker TLS**
  - `MqttBrokerConfig.tls: ssl.SSLContext | None = None` (slot between
    `session_expiry` and `username`), a `__post_init__` calling the validator
    with `"mqtt.tls"`, and a docstring paragraph in the existing style: what
    it is, server-auth vs mTLS (client cert in the context), that unverifying
    contexts are refused and why, that the port is the caller's (8883 by
    convention — no default changes), and that a rotated certificate is
    picked up at the next start.
  - `MqttConnection.__init__`: when `broker.tls` is set, call
    `self.client.tls_set_context(broker.tls)` (beside `username_pw_set`).
    The `__aenter__` log line states the transport (e.g. `"Connecting to MQTT
    %s:%d over TLS as %s …"` vs plaintext).
  - Reader-callback hardening in `on_socket_open`, **branching at
    registration time**: if `isinstance(sock, ssl.SSLSocket)`, register a
    draining wrapper that calls `client.loop_read()` and then keeps calling
    it while `client.socket() is sock` and `sock.pending() > 0`; otherwise
    register `client.loop_read` itself, exactly as today — so the pinned
    `test_on_socket_open_registers_reader` (`tests/mqtt_test.py:690-698`,
    asserting `add_reader.assert_called_once_with(mock_sock, client.loop_read)`)
    stays untouched and keeps passing. A short comment says why (decrypted
    bytes buffered inside OpenSSL never make the fd readable; paho's own
    `_loop` has the same guard; Mosquitto writes one record per packet so it
    was not reproducible there, but nothing obliges an MQTT broker to).
  - Unit tests in `tests/mqtt_test.py` (use the existing `mock_client`
    fixture and `make_connection` helper): `tls_set_context` called once with
    the context when `tls` is set, not called when it is `None`;
    `MqttBrokerConfig(tls=<unverifying>)` raises `ValueError` at
    construction; for an SSL socket the reader registered by
    `on_socket_open` drains `pending()`: the fake socket is
    `MagicMock(spec=ssl.SSLSocket)` with `pending.side_effect = [2, 1, 0]`,
    and it must ALSO be set as `client.socket.return_value` (the
    `mock_client` fixture's default `socket()` returns a different mock, and
    the `client.socket() is sock` guard would end the loop after one call);
    invoking the registered callback calls `loop_read` 3 times. The non-SSL
    case is already covered by the existing
    `test_on_socket_open_registers_reader`. Keep every existing test passing
    unchanged.
  - Commit.

- [ ] **3. Kafka broker TLS** — see the `kafka_tls` bullet in Constraints
  for the required-lookup decision and the one-line test additions it
  entails; make those additions in this task's commit.
  - `Flechtwerk.of(..., kafka_tls: ssl.SSLContext | None = None)` (alphabetical
    slot), `instance.kafka_tls = kafka_tls`, the matching
    `kafka_tls: lookup[ssl.SSLContext | None]` on `_FlechtwerkModule`, and a
    sentence in the `of` docstring.
  - One private helper on `_FlechtwerkModule` (a `cached_property` or
    method — your choice; the `Flechtwerk` ABC must stay annotation-free,
    `test_public_handle_exposes_no_container_internals` pins that) yielding
    the connection kwargs every aiokafka client shares: `bootstrap_servers`
    plus, when `kafka_tls` is set, `security_protocol="SSL"` and
    `ssl_context=kafka_tls`. Spread it into **all nine** construction sites
    so none can drift; afterwards `bootstrap_servers=self.bootstrap_servers`
    appears in that helper only.
  - `__aenter__`: call the validator with `"kafka_tls"` beside
    `validate_topics` / `validate_poll_interval`.
  - Unit tests in `tests/module_test.py`: monkeypatch `AIOKafkaConsumer`,
    `AIOKafkaProducer` and `AIOKafkaAdminClient` in `flechtwerk.module` with
    kwargs recorders, then exercise every factory that builds a client
    (config consumer, main consumer, restore consumer, membership consumer,
    task/token/instance producers, `ensure_changelog`, the `__aenter__`
    admin — reach them the way existing tests in that file do) and assert
    each recorded call carries `security_protocol="SSL"` and the same
    `ssl_context` object; with `kafka_tls=None` none carries either key. A
    container built with an unverifying `kafka_tls` fails in `__aenter__`
    with `ValueError` before any Kafka client is built.
  - Commit.

- [ ] **4. Integration tests: `tests/integration/tls_test.py` (new file)**
  - `pytestmark = pytest.mark.integration`; skip like the existing fixtures
    when Docker is unreachable.
  - A session-scoped fixture generating a throwaway PKI with `cryptography`
    into a `tmp_path_factory` directory: CA, server cert (SAN
    `DNS:localhost`, `IP:127.0.0.1`), client cert — each with
    BasicConstraints, SubjectKeyIdentifier, AuthorityKeyIdentifier and
    critical KeyUsage, or Python 3.14's strict verification rejects them (see
    Verified Context). Expose the PEM strings, the file paths, and a factory
    for client contexts with and without the client certificate.
  - A session-scoped Mosquitto fixture with a TLS listener (start from
    `_MOSQUITTO_CONFIG` in `mqtt_delivery_test.py` verbatim and replace
    `listener 1883` with `listener 8883` plus `cafile`/`certfile`/`keyfile`
    and `require_certificate true`; keep `allow_anonymous true` and the
    `log_*` lines, because Mosquitto 2 refuses anonymous clients otherwise
    and the readiness wait greps the log),
    files written via the `sh -c` / `printf %s` / `shlex.quote` pattern of
    `mqtt_delivery_test.py`, server key `chown mosquitto` + `chmod 600`, and
    `DockerContainer.start()` called directly (the existing fixture's
    docstring says why). Use `MosquittoContainer()`'s default image
    (`eclipse-mosquitto:latest`; the spikes ran on `eclipse-mosquitto:2`, i.e.
    2.1.2), as the existing fixture does, and expose
    port 8883 instead of `MQTT_PORT`. The existing `publish_qos1` /
    `get_client()` helpers connect in plaintext to 1883 and cannot be reused;
    publish with a plain paho `Client` (MQTTv5) given `tls_set_context`,
    started with `loop_start()` so `wait_for_publish` can complete, and
    called from the async watcher through `asyncio.to_thread`. Each payload
    is a JSON object (for example `json.dumps({"n": i})`), because the
    `MqttExtractor` template drops any payload that isn't JSON.
  - A session-scoped Kafka fixture with an mTLS listener exactly as the
    Verified Context spike: `KafkaContainer` subclass writing the two PEM
    files in `tc_start` before delegating, `listener_name="TLS"`,
    `security_protocol="SSL"`, the five `KAFKA_SSL_*` env vars, plus the four
    replication/ISR envs `tests/integration/conftest.py` already sets.
    Yield its bootstrap address.
  - Tests (names are yours; these are the behaviours):
    1. Fixture sanity, MQTT: a `MqttConnection` whose `tls` context has no
       client certificate (default TLS version, i.e. 1.3) — `__aenter__`
       returns, and then `drain()` on a subscription raises
       `ConnectionError` within 5 s (poll `drain` with short sleeps until it
       raises or the bound passes; the bound passing is a failure). This is
       the spiked behaviour in Verified Context — do not expect the failure
       inside `__aenter__`. If `__aenter__` itself raises `ssl.SSLError` or
       `OSError`, which is a timing race under TLS 1.3, that also passes:
       the test proves the listener rejects a context with no client
       certificate, not where the rejection shows up. It proves the
       listener really requires mTLS.
    2. Fixture sanity, Kafka: an `AIOKafkaAdminClient` with
       `security_protocol="SSL"` and a context without the client
       certificate fails to start (bound it, e.g. `request_timeout_ms=5000`
       and `asyncio.wait_for`).
    3. MQTT backlog over mTLS: the spike scenario — register a persistent
       session + shared subscription, disconnect, publish ≥ 100 QoS 1
       messages over mTLS (a plain paho client with `tls_set_context`),
       reconnect with the same `client_id`, drain + ACK, and assert all
       arrive within a few seconds (far below the 60 s keepalive, so a stall
       would fail it).
    4. End-to-end over both: in one `asyncio.TaskGroup` (model it on
       `tests/integration/multi_stage_test.py`), run three `Flechtwerk.of`
       handles, all with `kafka_tls=` the client mTLS context and distinct
       `application_id`/`client_id`: an `MqttExtractor.of(...)` with
       `mqtt=MqttBrokerConfig(..., tls=<mTLS context>)` relaying MQTT
       messages to a raw topic (its config record carries
       `flechtwerk.mqtt.TOPIC`; the relay's `Message` carries a non-empty
       `str` key, so the stateful transformer has a state key); a stateful `Transformer` refining raw →
       out (yields `State`, so changelog writes happen); and a stateful
       `Extractor.of` writing to a third topic (exercises the sharded path:
       membership consumer, token producers, changelog creation). Topic
       creation, seeding and output reads use aiokafka clients with the same
       TLS settings. **Publishing races the stage's subscription**: the
       `MqttExtractor` subscribes only on its first poll, after the config
       bootstrap, and its fresh `client_id` has no session yet, so a QoS 1
       publish before that SUBSCRIBE is silently discarded by Mosquitto.
       Therefore the watcher publishes one fresh QoS 1 message over mTLS on
       **every** iteration, before checking the output topics; early
       messages are simply lost and a later one carries the data. The
       watcher ends the group once at least one refined record (from any of
       those publishes) and the sharded extractor's record are visible
       `read_committed`, bounded by a 90 s deadline with `pytest.fail`, as
       `multi_stage_test.py:161-164` does. Never use `asyncio.sleep` as a
       synchronisation device in this test; short pauses between watcher
       iterations are fine.
  - Verify: `uv run pytest -q -m integration tests/integration/tls_test.py -rs`
    → all passed, nothing skipped.
  - Commit.

- [ ] **5. Documentation**
  - `docs/guides/mqtt.md`: a `## TLS` section after "Running It" — a
    server-auth example and an mTLS example
    (`ssl.create_default_context(cafile=...)`, `load_cert_chain(...)`,
    `MqttBrokerConfig(..., port=8883, tls=ctx)`), the refusal rule, the
    Python ≥ 3.13 strict-verification note (AKI + KeyUsage on private-CA
    certificates, with the exact error strings), and rotation-at-restart.
  - `docs/guides/getting-started.md`: add `kafka_tls` to the optional-knobs
    paragraph (alphabetical position, link to an anchor) and a short
    subsection showing a Kafka mTLS context, noting that one context may be
    shared by both brokers when the same CA and client certificate serve
    both.
  - `Flechtwerk` class docstring, embedding paragraph: an embedding parent
    must wire `kafka_tls` (`None` for plaintext).
  - `src/flechtwerk/module.py` / `mqtt.py` docstrings as touched above;
    the `mqtt.py` module docstring gets one line on TLS.
  - `CLAUDE.md`: in the `module.py` bullet, `kafka_tls` (the shared kwargs
    helper, all nine sites, the `__aenter__` check); in the `mqtt.py`
    bullet, `MqttBrokerConfig.tls`, `tls_set_context`, the pending-drain
    reader and why; one sentence on the shared refusal rule; extend the
    closing "all configuration is injected by the caller" sentence to
    mention the TLS contexts. Match the file's dense house style.
  - Verify: `uv run poe docs-build` exits 0.
  - Commit.

- [ ] **6. Final verification** — run every work criterion below and show the
  output (Execution Protocol step 5), and run `graphify update .` only if
  `graphify-out/graph.json` exists (it does not as of planning).

## Acceptance Criteria

Work criteria — re-run in full before the Closeout:

1. `uv run pytest -q` exits 0 with 0 failed, and the passed count exceeds 668.
2. `uv run pytest -q -m integration tests/integration/tls_test.py -rs` exits
   0, reports at least 4 passed, and reports no skipped and no failed tests.
3. `uv run pytest -q -m integration` exits 0 with 0 failed and 0 skipped.
4. `uv run poe docs-build` exits 0.
5. `grep -c "bootstrap_servers=self.bootstrap_servers" src/flechtwerk/module.py`
   prints 1 (only the shared kwargs helper names it — so the helper must use
   keyword syntax, e.g. `dict(bootstrap_servers=self.bootstrap_servers, …)`;
   a dict literal `{"bootstrap_servers": …}` makes the count 0), and
   `grep -n "tls_set_context" src/flechtwerk/mqtt.py` prints at least one line.
6. `uv run python -c "import flechtwerk, sys; assert not any(m.startswith(chr(112)+chr(97)+chr(104)+chr(111)) for m in sys.modules)"`
   exits 0 — importing `flechtwerk` still does not load paho.

Closeout criteria — hold only once the Closeout section has run, and are shown
there:

7. The final state of this plan was committed in a commit of its own,
   immediately followed by a separate commit that deletes it. Shown by
   Closeout steps 2 and 5.
8. `git status --porcelain` is empty. Shown after Closeout step 5.

## Rollback

- `git revert` the work commits, newest first (they are local only — nothing
  is pushed). The feature is purely additive: `tls=None` / `kafka_tls=None`
  keep today's plaintext behaviour, so reverting affects no existing caller.

## Closeout

No ticket, and `closeout_push` is false: steps 3, 4 and 6 (permalink, ticket
comment, ticket fields) do not apply, and nothing is pushed.

1. Bring this file to its final state: every task checkbox ticked, the Run Log
   complete, the outcome recorded, and `status: done` in the frontmatter.
2. Commit this file on its own — the preservation commit:

   ```bash
   git add 2026-10-11-mqtt-and-kafka-tls-plan.md
   git commit -S -m "Record the final state of the MQTT and Kafka TLS plan"
   git rev-parse HEAD
   ```

   (Three separate commands, one per tool call; add the attribution trailer
   as a further `-m` argument.)

   It is a commit of its own because the final Run Log entries are written
   after the work commits. Do not squash it and do not delete the file in it.
3. (No ticket — no permalink.)
4. (No ticket — no comment.)
5. Only now delete this file, in a separate follow-up commit:

   ```bash
   git rm 2026-10-11-mqtt-and-kafka-tls-plan.md
   git commit -S -m "Remove the MQTT and Kafka TLS plan file"
   ```

   Then show `git status --porcelain` empty — closeout criterion 8.
6. (No ticket — nothing to leave untouched.)

If the preservation commit cannot be completed, **leave this file in place**
and write the blocked report.

Every commit in this run, work and closeout alike, is PGP-signed (`-S`) and
ends with the attribution trailer your own session's instructions prescribe.

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
7. Commit as you go, on the current branch — `branch` in the frontmatter, which the
   gate confirmed — with no ticket prefix (this repository uses none): a task, or a
   coherent slice of one, per commit, so progress is durable and a resume or a
   rollback has real boundaries to work with. Every commit is PGP-signed
   (`git commit -S`). Never push. Never create a branch.
8. Run the **Closeout** section — after the commit in step 7 and before you remove
   this file in step 9. That order is the whole point: this file is the record of the
   run and it is about to stop existing, so its final state is committed before
   anything is deleted. If the closeout cannot be completed, do not remove this file —
   write the blocked report in step 10 instead, naming the closeout as what failed.
9. When every acceptance criterion passes and the closeout is done, remove this plan
   file exactly as the Closeout section prescribes: in a separate commit after the
   preservation commit, never folded into it and never folded into the work commit.
10. If a criterion cannot be made to pass — or the run cannot finish for any other
    reason: a denied command, a contradiction with the Verified Context that leaves the
    Goal standing but the plan wrong, a run that has to stop clean at a task boundary —
    write the blocked report at **exactly the path named in the goal condition**:
    `2026-10-11-mqtt-and-kafka-tls-plan.BLOCKED.md`. It names the failing check, what
    you tried, and why it cannot pass; lists the working tree as you leave it — every
    uncommitted file and the task it belongs to; and opens with a frontmatter block of
    exactly three keys, alphabetically — `date:` the day the run stopped; `next:`
    `relaunch` when the Handoff command can resume once the environment is fixed, or
    `reopen` when the plan itself has to change; and `plan_file:`
    `2026-10-11-mqtt-and-kafka-tls-plan.md`. Set `status: blocked` **and**
    `blocked_report:` naming the report in this file's frontmatter — written in the
    same commit and cleared in the same turn. Bring the Run Log current, and commit
    this file and the report together (signed, not pushed), leaving uncommitted only
    the work that has not reached a commit boundary. Then stop. Do not weaken a check,
    skip it, or declare success without it. A report written by mistake — on an
    estimate rather than a check that ran — is retracted in the same session: delete
    it, say so in the Run Log, and continue.

## Handoff

```bash
claude --model opus --effort high --permission-mode auto --remote-control 2026-10-11-mqtt-and-kafka-tls-plan '/goal Execute the plan at 2026-10-11-mqtt-and-kafka-tls-plan.md to completion. Before changing anything, run the Pre-Flight section of that plan and show the output of every check: the model must be claude-opus-5-5, the effort high, the permission mode auto, and every privilege probe must pass (docker info prints a version, the gpg detach-sign probe exits 0, uv run pytest -q exits 0). If any pre-flight check fails, write the blocked report named below and stop - do not start the tasks and do not continue on a different model or mode. Done when all of the following hold: the Pre-Flight checks were run first in this session and passed; every task checkbox in that file was ticked before the file was removed; immediately before the Closeout, in this session, each of these was run and its output shown: uv run pytest -q exited 0 with 0 failed and more than 668 passed; uv run pytest -q -m integration tests/integration/tls_test.py -rs exited 0 with at least 4 passed and nothing skipped or failed; uv run pytest -q -m integration exited 0 with 0 failed and 0 skipped; uv run poe docs-build exited 0; grep -c "bootstrap_servers=self.bootstrap_servers" src/flechtwerk/module.py printed 1; grep -n "tls_set_context" src/flechtwerk/mqtt.py printed at least one line; uv run python -c "import flechtwerk, sys; assert not any(m.startswith(chr(112)+chr(97)+chr(104)+chr(111)) for m in sys.modules)" exited 0. The final state of the plan file was committed on its own in this session, and only then was the plan file deleted in a separate later commit; git status --porcelain then printed nothing. All work is committed on the current branch worktree-mqtts, every commit PGP-signed, nothing pushed. Do not create a branch. Do not push. Do not relax, skip, or declare passed any check that was not actually run, and do not route around a permission denial - a refused command is a blocked-report situation. If a pre-flight or acceptance check cannot be made to pass, write 2026-10-11-mqtt-and-kafka-tls-plan.BLOCKED.md in this session, naming a check this session ran, what was tried, and why it cannot pass, commit it with the plan, then stop - that also satisfies this goal. A blocked report left by an earlier launch does not.'
```

`/pdca:execute 2026-10-11-mqtt-and-kafka-tls-plan.md` runs that command as a
background session with Remote Control, so nothing needs pasting. If this plan
file has been sitting for a while, do not launch it blindly — reopen it first
with `/pdca:plan 2026-10-11-mqtt-and-kafka-tls-plan.md` to re-verify the
Verified Context and get a freshened handoff. The condition inlines every work
criterion; none is summarized.

Short form, for typing into a session already started with the flags above.
The evaluator then holds only the plan path, the protocol and the escape
hatch — not the acceptance criteria — so it has to trust this session's own
account of what it ran. Prefer the full command; use this when the full one is
not to hand. A bare `/goal 2026-10-11-mqtt-and-kafka-tls-plan.md` is not a
launch at all: the evaluator cannot open the file, so it would have nothing to
judge.

```
/goal Execute the plan at 2026-10-11-mqtt-and-kafka-tls-plan.md to completion per its Execution Protocol: Pre-Flight first, every Acceptance Criterion shown passing in a final re-run, the Closeout done before the file was removed in a separate later commit; or 2026-10-11-mqtt-and-kafka-tls-plan.BLOCKED.md was written in this session.
```

## Run Log

<Appended by the execution session.>

- 2026-10-12 — Pre-Flight gate PASSED (fresh start; no `blocked_report`).
  1. Transcript found (`…/b9544bfe-….jsonl`); last goal record `"met":false`
     with a condition naming this plan; model `claude-opus-5-5`;
     `effort=high`. The transcript's `permissionMode` grep printed nothing
     and the argv walk from `$PPID` reached a daemon spare
     (`claude bg-spare …`, no mode flag); the job's launch record
     (`$CLAUDE_JOB_DIR/state.json` `respawnFlags`) reads
     `--permission-mode auto --model opus --effort high` — mode `auto`.
  2. `docker info` → `29.8.2`; gpg detach-sign probe exit 0;
     `uv run pytest -q` → `668 passed, 22 deselected`.
  3. Plan file present; toplevel `…/.claude/worktrees/mqtts`; branch
     `worktree-mqtts`; `git status --porcelain` empty.
  4. `Python 3.14.7`; bootstrap_servers grep → `9`; `tls` grep → nothing.
  - Contradiction (minor, Goal unaffected): `graphify-out/graph.json` now
    EXISTS (the plan says it did not at planning). Only task 6's conditional
    `graphify update .` depends on it — it will be run there.
- Task 1 started.
- Task 1 done: `validate_tls(context, name)` in `module.py` (not in
  `__all__`); 5 tests in `tests/module_test.py` (None/default pass,
  CERT_NONE/CERT_OPTIONAL/check_hostname=False → ValueError, str → TypeError).
  `uv run pytest -q` → `673 passed, 22 deselected`.
