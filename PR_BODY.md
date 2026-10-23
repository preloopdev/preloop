# feat(sandbox): database-authoritative Sandbox Service state and multi-replica routing

The Sandbox Service keeps sandbox state in memory today: one process owns the
inventory, and a restart loses every record it holds. This PR moves the
authority into the database. Memory keeps only bounded live channels — an
exec's output queue, one transfer's rendezvous, the callers currently waiting.

## Design

- **Typed commands over targeted SQL.** Every durable decision (inventory,
  leases, provider generations, work queues, exec lifecycle, ticket metadata,
  snapshot receipts) is one command in one short SQL transaction. Commands are
  written once against a tiny portable surface and run on both engines; a
  dropped transaction never leaves a dirty pooled connection. Multi-row
  decisions use conditional updates and shared pure functions, and the lock
  order `pools → providers → sandboxes → leases → work_items → runtime
  handles` is fixed so two commands cannot deadlock by construction.
- **SQLite is the single-node default** (`<state>/sandbox.db`, WAL,
  `synchronous=FULL`, `BEGIN IMMEDIATE`). One writer means one attempt per
  command and no lock races.
- **Postgres serves several replicas** (`PRELOOP_SANDBOX_STORE_URL`). Each
  replica opens its own pool, claims work with `SKIP LOCKED`, and coordinates
  through conditional updates and rows — never a global lock, never a write
  to the control plane.
- **Live streams are owned and forwarded.** An exec or transfer stream belongs
  to the replica that opened it, recorded as a fenced node/session handle. A
  request that lands elsewhere looks up the owner and forwards over an
  authenticated internal hop: the sender proves its registered node id,
  session and credential, the receiver checks them against its node registry,
  peer base URLs come only from configuration, and the shared service CA
  verifies the TLS. If the owner dies, its node session expires and only its
  own channels and leases fail.
- **Greenfield schema v1, refused rather than imported.** On an empty store
  the schema is created (replicas may start together: creation takes a
  transaction-scoped advisory lock and the key rows insert `ON CONFLICT` and
  re-read). A store that was not created by this service, has a different
  schema version, or was sealed with a different key fails closed at open.
  There is no import, backfill, dual-write or upgrade path, because no legacy
  sandbox state exists.
- **Secrets stay out of the database.** Lease and exec environment is sealed
  with AES-256-GCM from the already-locked `ring` dependency; bearer, join and
  ticket tokens are stored only as SHA-256 hashes; inject payloads are never
  persisted.

## Contract behaviour the cutover had dropped

Reviewing the rewrite against the service it replaced found behaviour that had
regressed. Each is restored against durable state:

| # | Behaviour | Restored in |
|---|---|---|
| 1 | Pool-acquire validation (template, revision, shape, requires) | `3bf7e8268` |
| 2 | An idempotent retry returns a stopped/paused sandbox instead of destroying it | `3bf7e8268` |
| 3 | Reconcile proves ownership before acting on a record | `3bf7e8268` |
| 4 | Reconcile never reaps another provider's machine | `d2a2fad00` |
| 5 | Destroy retry accounting (attempts, requeue) | `3bf7e8268` |
| 6 | Tombstones are kept while the machine is still reported, then reaped | `3bf7e8268` |
| 7 | Snapshot is refused while an exec is running | `3bf7e8268` |
| 8 | Destroy ends running execs and fails them with a reason | `3bf7e8268` |
| 9 | Preserve windows survive start/resume | `3bf7e8268` |
| 10 | Pool revision is the build inputs only; members are not reused across a revision | `3bf7e8268` |
| 11 | Preserve-release keeps paused/stopped power state | `d2a2fad00` |
| 12 | Preserve-release keeps a working debug capability | `d2a2fad00` |
| 13 | Zero unclaimed TTL expires immediately | `d2a2fad00` |
| 14 | Template deletion is refused while a clone is live | `d2a2fad00` |
| 15 | Ticket guest paths reject parent components | `d2a2fad00` |
| 16 | Exec env keys must be POSIX identifiers | `d2a2fad00` |
| 17 | Inject files default to `0600` | `d2a2fad00` |
| 18 | Work the agent accepted outlives a fenced session (reports follow the newest registration; an `aborted` report waits bounded for it) | `a665bd6d8` |
| 19 | An accepted `ReportExec` renews the delivery window instead of failing a live command | `a665bd6d8` |

Plus multi-replica hardening: schema/key creation no longer races between
replicas starting together, and an idle connection left dead by a store outage
is replaced on checkout instead of costing one failed request per stale
socket.

**One test was rewritten, deliberately.**
`a_creating_record_is_failed_after_restart` asserted the pre-cutover contract
that a create in flight when the process restarts is failed whole. The
database now carries the accepted create work (the request is part of the
sandbox spec row), so the documented behaviour is the opposite: a replica
resumes the accepted provisioning and the caller's retry with the same
idempotency key returns the same lease. The test is now
`a_creating_record_resumes_after_restart` and asserts the durable create work
survives and the record stays `PROVISIONING` with no error.

## Transaction retry (`40001` / `40P01`)

A transaction that loses a lock race is rolled back and the whole command is
re-run against the winner's state: up to five attempts, 5 ms doubling to a
200 ms cap, jittered so colliding replicas do not retry in step. The decision
keys off a typed conflict marker on the engine error's source, never a code or
message, so a command's own `aborted`/`unavailable` result is returned
unchanged. Commands are SQL-only closures and the losing attempt wrote
nothing, so re-entry cannot duplicate an effect; SQLite keeps one attempt.
`store.rs` covers the policy: a conflict retried once performs its effect
exactly once, a domain failure is not re-run, and retries stop at the bound.

## Replay boundary

Delivery is at-least-once, execution is at-most-once. A redelivered work item
does not re-run a command: the agent records a started exec on its host and
the service records the winning start attempt before the guest command runs,
so a later delivery that tries to start the same exec fails it with an
explicit error instead of replaying it. An ambiguous outcome — the command may
or may not have run — is reported to the caller explicitly and never retried
automatically. Ordinary results (create, snapshot, power, copy) are persisted
before any waiter observes success, so a lost response is answered from the
durable result rather than repeated.

## Postgres multi-replica matrix

`crates/preloop-cli/tests/sandbox_pg_matrix.rs`: two real service processes on
one Postgres database, a real enrolled provider whose polls and reports split
across both replicas, and `psql` inspection of the durable rows. Skip without
`PRELOOP_TEST_POSTGRES_URL`; `PRELOOP_TEST_REQUIRE_POSTGRES=1` makes a missing
server a failure.

| Test | Observed |
|---|---|
| `two_hundred_acquires_over_two_replicas_produce_fifty_leases` | pass |
| `duplicate_live_name_has_exactly_one_winner` | pass |
| `driver_slots_and_pool_max_never_overshoot` | pass |
| `acknowledged_writes_survive_sigkill_with_their_queued_destroy` | pass |
| `concurrent_reapers_queue_one_destroy` | pass |
| `provider_registers_on_one_replica_and_reports_through_another` | pass |
| `exec_streams_through_one_replica_while_the_provider_reports_through_another` | pass |
| `transfers_across_replicas_are_byte_identical_without_staging` | pass |
| `killing_the_owner_fails_its_stream_and_keeps_unrelated_leases` | pass |
| `store_outage_fails_writes_and_lists_nothing_new` | pass |

**10/10** on a fresh cluster (PostgreSQL 18.3, `initdb -A trust`, `fsync=on`),
`--test-threads=1`, 110 s. The burst test was also run six times on its own:
before the reconcile fix below it failed 5 of 6 runs; after it, 6 of 6.

The matrix exposed the two concurrency defects fixed here: a command failing
with `unavailable` because it lost a lock race, and a live sandbox failed as
"lost: missing from provider inventory" because the provider's inventory
sample predated the machine's create.

## Live smoke (SQLite)

Service, provider enrollment, template, acquire, exec, a transfer each way, a
service restart, then release — one isolated `PRELOOP_HOME`, real TLS, the
built-in fake driver attached as an external plugin:

```
service 127.0.0.1:<port>   sqlite store
provider smoke             generation 1, driver fake
template base              put + listed
sandbox sbx-… (LEASED)     lease lse-…
exec                       hello-from-sandbox; cwd inside the sandbox root
transfer                   byte-identical in both directions
restart                    sandbox still LEASED; the same lease still execs
release                    sandbox destroyed and gone from the listing
```

Listener and authorization matrix, against the same service in split-listener
mode (`PRELOOP_SANDBOX_AGENT_LISTEN`):

| Probe | Observed |
|---|---|
| client + bearer → `Sandboxes/List` | 200 |
| client + hostile `Host` header | 403 |
| client without bearer | 401 |
| client → `Providers/Register` | 404 (route not mounted) |
| client → `Sandboxes/ListProviders` (control) | 200 |
| agent certificate → `Sandboxes/List` | 404 (route not mounted) |
| agent listener without a client certificate | 401 |
| agent certificate → `Providers/Register` (control) | 400 |
| `PRELOOP_SANDBOX_AUTHORIZER=shadow-policy` | refuses startup, names the setting |

## Verification

- `cargo fmt --all -- --check` clean.
- `cargo test --locked -p preloop-sandbox -p preloop-orchestrator`: 147 + 190
  lib tests and every integration suite (conformance 94, mtls 23, external
  driver 7, smolvm CLI 32, multi-replica 1, …) pass.
- `cargo test --locked -p preloop-cli sandbox`: 22 pass.
- `cargo clippy --locked -p preloop-sandbox -p preloop-orchestrator -p preloop-cli -p preloop-runner-server --all-targets -- -D warnings`: clean.
- `PROPTEST_CASES=8 cargo test --locked --workspace` under Postgres: every
  suite passed except the burst test, which failed on the reconcile defect
  fixed here (5 of 6 runs before the fix); after the fix it passed six
  consecutive runs and the full matrix passed 10/10 on the committed tree.
- `supply-chain/guard-policy-files.sh public/main HEAD`: clean (no policy
  files touched).

## Known limitations

- **`preloop-runner` is not lint-clean** (`unnecessary_sort_by` in
  `job_extension_tests.rs`), so the workspace-level `just test-ci` stops at
  its clippy step. The failure predates this branch: the file is untouched
  here and the same line is flagged on the base branch by clippy 1.99. The
  recipes after clippy were run separately.
- **Reconcile still needs a small time window** (3 s) before it treats a
  missing inventory entry as a loss, because the protocol's report is a
  snapshot with no observation timestamp. A per-row "missing since" column or
  an observation timestamp would remove the window; both need a schema or
  wire change and are left for a follow-up.
- **Roles and per-caller credentials are follow-ups** (#777/#792). The
  built-in authorizer allows every authenticated principal to use every action
  on its surface; `PRELOOP_SANDBOX_AUTHORIZER` rejects any other value.
- **A Postgres store is not migrated**: schema v1 is greenfield, and a store
  from an earlier build of this branch (missing a column) must be recreated.
