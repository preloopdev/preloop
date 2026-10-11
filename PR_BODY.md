# fix/round3-submit-cap-chunking — cap giant submits at 500 jobs + chunk the submit transaction

## Finding (round-3 report §4 + Postgres follow-up)

Giant submit → writer hold. There was no default cap on jobs per run
(the `namespace_limits.max_jobs_per_run` quota is opt-in; `NULL` =
unlimited), and one HTTP request meant one DB transaction: the whole
insert ran inside a single transaction holding one writer for its entire
duration.

Measured pre-fix: 5,000-job submit → 200 in 19.5s; 20,000-job submit →
200 in 81.85s on SQLite with one concurrent control write blocked
**68.8s**; on Postgres the 20k submit never completed (>600s, writer
connection pinned idle-in-transaction 8.5+ min → pool exhaustion →
writes fail after the 30s checkout timeout).

## Fix

1. **Server-wide `max_jobs_per_run`, default 500** (`crates/preloop-runner-server/src/config.rs`,
   `state.rs`): new `ConfigFile` key `max_jobs_per_run` (serde default
   500) with `PRELOOP_MAX_JOBS_PER_RUN` env override, following the
   `retention_days` pattern (unparseable env value fails closed). `0`
   disables the cap. Documented in `docs/self-hosting.md` (§ Core env
   table + new "Submit limits" section).
2. **Fail-fast 400 after expansion, before the insert transaction**
   (`runs.rs::submit_run_inner_with_webhook_delivery_unreserved`): a
   non-webhook submit expanding past the cap is rejected with
   `400 Bad Request` naming the expanded count and the limit, before the
   per-job build loop and before any transaction opens — no giant
   transaction ever starts. Webhook deliveries bypass the cap, mirroring
   the existing namespace submit quotas (`admit_submit`: "a push is never
   refused"); their writer hold is bounded by chunking instead. A 400 on
   the webhook path would otherwise be misread as "workflow not
   triggered" and silently drop the push.
3. **Chunked submit transactions** (`control/lite/submit.rs`,
   `control/pg/dispatch.rs`, shared `SUBMIT_JOB_CHUNK_SIZE = 250` in
   `control/types.rs`): each backend's `submit_run` is now phase 1
   (replay check, run-number allocation, namespace admission, run row,
   workflow gate — one tx), phase 2 (one tx per ~250-job slice:
   classification + job/message/request/spec rows + per-job gate +
   ready enqueue), phase 3 (one tx: `remaining_needs` recompute,
   promotion sweep, run status, outbox, persisted events). Between
   chunks the SQLite writer mutex is released and the pooled Postgres
   writer connection is checked back in, so a concurrent control write
   waits for one chunk at most. If a chunk fails after phase 1
   committed, the run is poisoned (workflow-gate hold released, run
   marked completed/failed, `run.completed.v1` emitted) so a partial
   submit can never dispatch or wedge a concurrency group. The
   arrival-cancelled fast path still inserts in its single phase-1
   transaction (bounded by the cap).

## Regression tests

- `config::max_jobs_per_run_config_tests`: default 500, TOML override,
  `0` escape hatch, env-wins/file/blank/invalid handling,
  `ConfigFile::default()`.
- `runs.rs`: >cap → 400 with the limit named; exactly-at-cap accepted;
  cap counts **expanded** jobs (2 declared matrix jobs × 200 cells = 400
  rejected at cap 300, accepted at cap 500 — the accepted case also
  exercises the chunked insert end to end); webhook delivery bypasses
  the cap.
- `control::suite::chunked_submit_matches_single_chunk_state` (runs on
  **both** backends): 300-job run (3 full-payload jobs + 297 plain)
  across two chunks — outcome, all 300 job rows with `Queued` status,
  request/message correlation round-trip on both sides of each chunk
  boundary, deferred token-mint requests + OIDC grants for the full
  jobs, and all 3 declared step manifests via `run_step_manifests`.

## Repro verification (patched local server, port 19103, SQLite)

Server: `target/debug/preloop-server serve --listen 127.0.0.1:19103
--state-dir <worktree>/wt-state/submit-cap/data` (DB migrated with the 8
`migrations/sqlite` files; workflows in `wt-state/submit-cap/repro/`).

1. **20k-job workflow rejected fast**: `POST /api/v1/runs` with 80 matrix
   jobs × 250 cells (20,000 expanded jobs) → `400` in **1.48s** (pre-fix:
   81.85s with a 68.8s writer stall):
   `{"error":"run expands to 20000 jobs, exceeding the server's
   max_jobs_per_run limit of 500 (config 'max_jobs_per_run', env
   'PRELOOP_MAX_JOBS_PER_RUN')"}`. No `runs` row persisted.
2. **500-job workflow accepted**: 2 matrix jobs × 250 cells → `200` in
   **12.15s**; run recorded with all **500 jobs** in `queued` status
   (verified via `GET /api/v1/runs` and a direct `jobs`-table count).
3. **No writer stall**: a 1-job submit fired 0.3s into the 500-job
   insert completed in **0.18s** with `200` — the chunked transactions
   release the writer between 250-job chunks.

## Residual notes

- Deferred (needs-dependent) matrix legs materialize at runtime via
  `apply_expansion`, outside the submit cap; each expansion is still
  bounded by the existing 256-combination matrix cap.
- The `ArrivalCancelled` concurrency path inserts its (cap-bounded) jobs
  in the single phase-1 transaction rather than chunked.
- `0` disables the cap; the per-namespace quotas remain the opt-in
  fine-grained control.
