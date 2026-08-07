# Durable store — known issues

Tracked follow-ups for the SQLite/Postgres control-plane store
(`crates/aksh-runner-server/src/store.rs`, `store_pg.rs`). Five blockers found
in the same review were fixed in `fix(store): five restart-correctness blockers
in the durable store`; what follows is everything that was deliberately *not*
fixed there, with enough detail to pick any item up cold.

Severities use the `REVIEW.md` vocabulary: `concern:` should be fixed or
explicitly justified, `nit:` is optional polish.

Read alongside the **State Model** section of [architecture.md](architecture.md).

---

## Performance

### `concern:` `store_meta_only` serializes the whole world on every call

`build_meta_snapshot` clones ~25 collections, JSON-serializes them, AES-seals
the result, and writes it to a single `runtime_snapshots` row. Cost is linear in
total in-memory state, and several of those collections
(`timeline_events`, `timeline_records`, `artifacts`, `log_metadata`,
`artifact_v2_registry`) only ever grow.

Measured, release build, isolating `timeline_events`:

| accumulated events | one `store_meta_only` |
| --- | --- |
| 100 | 334 µs |
| 400 | 865 µs |
| 1 600 | 3.24 ms |
| 6 400 | 13.3 ms |

≈2 µs per event per call. Callers are per-step, not per-run:

| call site | trigger |
| --- | --- |
| `timeline_logs.rs` `patch_timeline_records` | every timeline PATCH |
| `timeline_logs.rs` `create_log` | every log created |
| `results_twirp.rs` ×3 | step summary, step log, job log metadata |
| `cache_artifacts.rs` | cache reserve, cache commit, artifact put |
| `artifact_twirp.rs` ×3 | artifact v2 reserve, finalize, delete |

Because nothing prunes the snapshotted collections, total work over a server's
life is quadratic in events. At 100k accumulated events each timeline patch
costs ~200 ms, with the global state lock held (see the locking item below).

**The fix that fits this codebase**: move the large append-only collections into
their own tables with incremental writes, exactly as `store_log_chunk` already
does for log bytes. That is an established pattern here, not a new convention.
The blocker fix removed the worst offender (`PendingCache.bytes`, which made
`cache_upload` quadratic in *cache size* — 75 ms per accumulated MiB per chunk);
the residual growth is bounded by run history rather than payload size, which is
why it was left.

### `concern:` The global state lock is held across every store `.await`

`state.rs::emit` takes `self.inner.lock().await` and calls
`store.store_run_event(&inner, …).await` with the guard alive; every
`store_meta_only` call site does the same. Ordering is correct (persist, drop
guard, then broadcast) and it is a tokio mutex, so there is no `!Send` break —
but the whole server serializes behind database latency.

Worse for SQLite: the `Store` impl is a thin `async fn` wrapper over blocking
`rusqlite` with no `spawn_blocking`, so a commit blocks a tokio worker thread
*and* the global lock.

**Fix**: build the byte payload under the lock, release, then write.

### `concern:` `store_inner` rewrites the entire database on session churn

`store_inner` deletes nine tables and reinserts every run, job, runner, session
and request, plus the meta blob. Callers: `register_runner`, `create_session`,
`create_session_disttask`, `delete_session` (`runner_lifecycle.rs`). Broker
sessions are per-job, so this is a full-database rewrite per job dispatch,
O(runs) each.

### `concern:` No retention anywhere

`log_chunks`, `log_files` and `control_events` are never deleted by any code
path, including `store_inner`. `timeline_events` is never pruned in memory. On
every restart `load_into` replays *all* historical log bytes for *all* runs back
into `inner.logs`. Memory and startup time grow without bound.

### `nit:` `Envelope::encrypt_sealed` allocates per call

`SessionEncryption::from_key(self.aead.to_vec())` allocates a fresh `Vec` on
every seal, and `store_inner` seals once per run, per job and per request.

---

## Correctness

### `concern:` Store-failure policy is inconsistent with itself

`AGENTS.md` and `state.rs::emit` say the store is best-effort. But
`register_runner` and `create_session` return **500** when the store fails,
while `delete_session` three lines below only warns. `submit_run_inner` returns
500 *after* already incrementing the in-memory run counter, so a failure burns a
run number and rejects the run — the exact inconsistency the strict path was
meant to prevent.

Pick one policy per surface and state it in the module docs.

### `concern:` `append_log` chunk index collides on an empty append

```rust
// timeline_logs.rs
let chunk_index = meta.byte_count as i64;
```

The comment claims this is unique per append, but a zero-byte append leaves
`byte_count` unchanged and collides with `PRIMARY KEY (log_key, chunk_index)`.
The insert fails, the transaction rolls back, and the chunk is dropped with a
`warn!`. Use a monotonic per-log sequence.

### `concern:` `store_inner` aborts wholesale on a queue-membership collision

`jobs` is `PRIMARY KEY (run_id, job_id)`, but `store_inner` inserts from
`queue`, `pending_jobs`, `concurrency_blocked` and `held_runs` into that one
table. If a job is ever in two of those simultaneously the entire snapshot
transaction aborts and all state for that write is lost, not just the job.

### `concern:` Storage constraints are driving runner-facing behavior

`dedupe_labels_ci` exists only because the `runner_labels`
`PRIMARY KEY (runner_id, label)` would otherwise 500 — the comment says so. The
result is that a runner-facing surface now silently drops labels to satisfy a
schema choice, with no golden capture or `just dogfood` evidence, which
`REVIEW.md` §1 requires for a protocol-touching change.

Inverting it keeps the protocol layer authoritative:
`PRIMARY KEY (runner_id, ordinal)`, with a `UNIQUE` only where the matcher
actually needs one.

### `concern:` `dedupe_labels_ci` is implemented three times

`store.rs` (`pub(crate)`), `runner_lifecycle.rs` (private — and it *shadows* the
glob-imported one via `use store::*`), and inlined again in
`aksh-runner/src/configure.rs`. `REVIEW.md` §5: one way to do each thing.

---

## Security

None of these are exploitable as written. They are recorded because the
surrounding comments currently overstate the guarantees, which is how a future
cleanup reintroduces a real bug.

### `concern:` The HKDF salt comment is false

```rust
// store.rs
// Salt = the root key itself, so two aksh installs that happen to load
// the same weak env var don't end up with the same sub-keys.
let salt = Sha256::digest(root);
```

`salt = SHA256(root)` is a deterministic function of `root`, so two installs
sharing a root derive **identical** sub-keys. The construction is fine — HKDF
with a deterministic salt is sound, and domain separation between the AEAD and
MAC sub-keys works. Only the stated reason is wrong. Either delete the claim or
persist a real random salt beside the key.

### `concern:` The envelope version byte is not covered by the MAC

`encrypt_sealed` authenticates `MIGRATION_DOMAIN || iv || ciphertext`; `seal`
then prepends the version byte outside that. Harmless at one version, a
downgrade vector the moment there are two. Note the tamper test appears to cover
byte 0 but actually passes via the version equality check, not the MAC.

### `concern:` A Postgres URL without `sslmode` connects in plaintext, silently

`tls_connector` returns `None` for any URL lacking an explicit `sslmode`; libpq
defaults to `prefer`. A remote `postgres://user:pass@host/db` therefore sends
credentials and every sealed blob in the clear with no warning.

TLS itself is implemented correctly — rustls with the system root store, full
chain and hostname verification, no permissive builder, and `verify-ca` /
`verify-full` correctly remapped onto `require`. This is only about the default.
At minimum, warn when the host is not loopback.

### `nit:` Session key material is not zeroized

`SessionKeyPayload(Vec<u8>)` serializes a 32-byte AES key as a JSON array of
decimal numbers (~4× bloat), and the intermediate plaintext buffer is dropped
without zeroizing.

---

## Dead weight and layering

- `nit:` **`runner_commands`** is created by the SQLite migration, absent from
  Postgres, and read or written by neither. `store_pg.rs` documents its own
  omission. Delete it.
- `nit:` **`workflow_run_counters.repository_key`** is always `''`. Dead column,
  and it makes the primary key look repo-scoped when it is not.
- `nit:` **`insert_job`** (SQLite) iterates
  `for (dependency, _) in job.needs.iter().enumerate()` and then indexes back
  into `job.needs`. The Postgres twin already has the clean
  `for depends_on in &job.needs`.
- `nit:` **`Envelope::seal`** returns `Result` but cannot fail.
- `nit:` **`aksh_gha_expressions::Context`** gained public `Serialize` /
  `Deserialize` solely so the server can persist `QueuedJob.condition_context`.
  That makes a library crate's private field layout part of its public serde
  contract for a downstream persistence concern.
- `nit:` **`connect_url`** relies on
  `let x = if … { "" } else { &format!(…) }` temporary-lifetime extension.
  Legal, unnecessary.

---

## Test coverage gaps

The restart contracts fixed in the blocker commit now have tests. Still
uncovered:

- **Corrupt, truncated, or wrong-key blobs.** Nothing proves the store fails
  closed rather than panicking on a slice index. `Envelope::unseal` length
  checks should be exercised at the exact boundary (`sealed.len() == 49`).
- **Restart with a live lease.** No test restarts with a job actually claimed by
  a runner and an active session lease, then asserts the runner can renew.
- **Postgres in CI.** Every `postgres_*` test `return`s unless
  `AKSH_TEST_PG_URL` is set, so the backend is unexercised by `just test-ci`.
  Verified manually against PostgreSQL 16 in Docker:

  ```sh
  docker run --rm -e POSTGRES_PASSWORD=aksh -e POSTGRES_USER=aksh \
    -e POSTGRES_DB=aksh -p 55432:5432 postgres:16-alpine
  AKSH_TEST_PG_URL=postgres://aksh:aksh@127.0.0.1:55432/aksh \
    cargo test -p aksh-runner-server --lib -- postgres_
  ```

  Wiring that into the gate is the single highest-value coverage addition here.

---

## cubic.dev review of PR #27 (2026-08-07)

The P1 blockers from that review were fixed in
`fix(store): address the cubic.dev P1 blockers on the durable store PR`.
What follows is the disposition of every P2 concern and P3 nit that was
deliberately **not** fixed, so a future pass does not have to re-derive them.

### P2 concerns — still open

1. **`JobCompleted` is not encoded in `control_events`.** `insert_event_tx`
   (both backends) handles `run_accepted` / `run_status` / `job_status` only;
   `JobCompleted` falls into `_ => return Ok(())`. The event still drives the
   run-projection write, so only the durable *event history* is missing it.
   Fix when something consumes `control_events` as a replay log; add
   `job_completed` to both encoders.
2. **`store_meta_only` still serializes the whole world.** The lock is now
   released before the write (fixed), but every call still clones, JSON
   serializes and AES-seals ~20 collections (`timeline_events`,
   `timeline_records`, `artifacts`, `log_metadata`, …), and those collections
   are never pruned. Measured ≈2 µs per accumulated event per call; at 100k
   events each timeline PATCH costs ~200 ms of CPU. The fix that fits this
   codebase is incremental tables for the append-only collections, exactly
   like `store_log_chunk` and `job_request_messages`.
3. **`append_log` chunk index collides on an empty append.** `chunk_index =
   meta.byte_count` is unchanged by a zero-byte append, so the next empty
   append violates `PRIMARY KEY (log_key, chunk_index)` and the chunk is
   dropped with a `warn!`. Use a monotonic per-log sequence.
4. **Postgres tests are not isolated.** They share one `AKSH_TEST_PG_URL`
   database and each starts with a `TRUNCATE`, which erases concurrent tests'
   state when run with the default test threads. They now run with
   `--test-threads=1` locally; the durable fix is a unique database per test
   (or a schema per test).
5. **The concurrent-migration test forces `NoTls`.** `postgres_concurrent_open_serializes_migrations`
   builds its temporary database URL without preserving the query string, so
   it fails against a TLS `AKSH_TEST_PG_URL`. Reuse `tls_connector` and keep
   the query when splitting the URL.

### P2 concerns — fixed as a byproduct of the P1 work

- **Lock held across store awaits** (artifact_twirp, timeline_logs,
  results_twirp, runs): the `Store` trait now takes owned projections, so
  *every* call site captures under the lock and persists after release, and
  SQLite moves blocking I/O to `spawn_blocking`.

### P3 nits — still open

- `docs/concurrency-plan.md:446` still calls the module `persist.rs`; line 334
  was renamed to `store.rs`. Fix the stale reference.
- `AgentRsaPublicKey::to_xml_string` (aksh-gha-protocol/src/crypto.rs:61) is a
  byte-for-byte copy of `AgentRsaKeypair::public_key_xml`, and a third copy
  lives in `runner_lifecycle::rsa_public_key_xml_from_value`. Extract one
  helper on `rsa::RsaPublicKey` and delegate.
- `--store` help text omits `sslmode=verify-ca` (server main.rs): the doc
  string lists `require|verify-full` only.
- `postgres_concurrent_open_serializes_migrations` leaves an `aksh_race_*`
  database behind per run. Drop it in the test teardown.
