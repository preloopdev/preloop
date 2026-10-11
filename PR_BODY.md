# fix/round3-diag-reaping — evicted diag tokens no longer upload; diag staging is reaped

Fixes round-3 finding §10 (diag re-mint loop → unbounded disk).

## The bug

`GetJobDiagLogsSignedBlobURL` mints a bearerless diag upload token per call and
records it in the in-memory `diag_upload_tokens` map (per-job cap 32,
evict-oldest). But `authorize_blob_request` validated diag PUTs by JWT
signature + owner-job liveness only — never map membership. Minting 33 tokens
evicted the first from the map, yet a PUT with the evicted token still
returned **201**. And `blobs/diag/` staging directories were never reaped by
any sweeper. A live job could therefore accumulate unlimited 3600s-valid
512 MiB staging dirs → ENOSPC → SQLite failures → engine 500s.

## The fix

1. **Eviction actually invalidates** (`auth.rs::authorize_blob_request`): diag
   PUTs now require the token's `jti` to still be registered in
   `diag_upload_tokens`. Evicted, TTL-swept, or never-registered tokens get
   404 (`blob not found`), on top of the existing signature and owner-job
   liveness checks. Reads are unchanged (unguessable URL stays the
   credential, same SAS-style model as `/replay/results/*`).
2. **Expired-token staging dirs are reaped** (`bootstrap.rs::reap_once`): the
   same pass that already deletes `blobs/cache/` and `blobs/artifact/`
   staging dirs for TTL-expired reservations now does it for `blobs/diag/`
   (map key == `jti` == staging dir name).
3. **Age-based backstop sweep** (`blob_store.rs::sweep_diag_staging`, wired
   into the 10s reaper): deletes any `blobs/diag/` staging directory older
   than `diag_staging_max_age_hours` (default **24**, `0` disables the
   age-based sweep; the TTL-expired cleanup in (2) always runs). Covers
   orphans the token sweep cannot see (crash between staging and
   registration, pre-fix leftovers). Only real directories are candidates —
   never the `diag` root, never symlinks; every I/O error is logged and
   skipped.
4. **Config**: new top-level key `diag_staging_max_age_hours` (default 24),
   env `PRELOOP_DIAG_STAGING_MAX_AGE_HOURS` wins; resolved once at startup
   into `AppState::diag_staging_max_age_seconds`. Documented in
   `docs/self-hosting.md` ("Diagnostic-log staging").

## Deliberate trade-offs

- **Diag URLs are now process-lifetime.** The registry is in-memory, so an
  engine restart invalidates outstanding diag URLs (they 404 until the
  runner re-mints). Diag uploads happen immediately after minting, so a
  restart in between was already a lost upload; documented in
  `docs/self-hosting.md`.
- **No per-job staged-bytes cap (deliberately skipped).** Each PUT is already
  capped at 512 MiB (`MAX_ASSEMBLED_BYTES`) and the live token set per job
  is now actually bounded at 32 by the mint-side eviction (which this PR
  makes effective). A byte-accounting cap would add streaming-enforcement
  and cross-restart complexity for marginal gain on top of the membership
  gate + TTL reaping + age backstop. Revisit if a job legitimately needs
  more than 32 concurrent diag uploads.

## Tests

New `crates/preloop-runner-server/tests/diag_reaping.rs`
(registered as a `[[test]]` target with `test-support`):

- `diag_evicted_upload_token_put_is_rejected` — the round-3 repro: 33 real
  mints via `GetJobDiagLogsSignedBlobURL` → map holds 32 → PUT with the
  evicted token's URL → **404** (was 201); PUT with a never-registered diag
  JWT → 404; PUT with a live token → 201.
- `diag_reaper_removes_expired_token_staging_dirs` — stale map entry +
  staging dir → `reap_once` removes both; fresh token's dir is kept.
- `diag_disk_sweep_removes_only_aged_staging_dirs` — injected clock (no
  sleeps): >24h-old dir swept, fresh dir kept, stray files untouched,
  missing root is a no-op.
- `diag_staging_max_age_config_defaults_and_env_override` — 24h default,
  env override wins (serialized via `GITHUB_ENV_LOCK`).

## Repro verification (patched server, port 19110)

`preloop store migrate` + `preloop serve --listen 127.0.0.1:19110` with
`PRELOOP_HOME` under the worktree, a known `PRELOOP_SYSTEM_TOKEN`, and a
known `PRELOOP_HMAC_KEY` (script: `.repro/repro_diag_reaping.py`):

1. Submitted a workflow via `/api/v1/runs`; read the queued job's
   `agent_job_id` from `<state>/state/preloop.db`; minted a job runtime JWT
   locally (HS256, `sub: preloop-job-{id}`,
   `scp: Actions.Results:{plan}:{id}`).
2. Called `GetJobDiagLogsSignedBlobURL` 33 times with the job bearer.
3. PUT each of the 33 bearerless URLs: **32 × 201, 1 × 404** — the evicted
   token is rejected (pre-fix, per the round-3 report, the evicted token's
   PUT returned 201). Note: the evicted index varies run to run (26, 27…
   observed) because `created_unix` has second granularity and the
   BTreeMap/jti order breaks ties — the invariant is 32-live/1-dead, which
   the script asserts. Also verified a never-registered diag JWT → 404.
4. Seeded `state/blobs/diag/` with an aged staging dir (mtime backdated 25h)
   and a fresh one; after the next 10s reaper tick the aged dir was gone
   and the fresh dir remained.
5. Restarted with `PRELOOP_DIAG_STAGING_MAX_AGE_HOURS=0`: the aged dir was
   kept across reaper ticks (age sweep disabled), confirming the opt-out.

Existing suites: `cargo fmt --check` clean; `cargo clippy -p
preloop-runner-server --all-targets --features test-support` clean (one
real finding fixed along the way: a unit-test `ConfigFile` literal missing
the new field); new `diag_reaping` integration tests 4/4 pass; existing
`logs` suite 52/52 pass (incl.
`twirp_diag_route_issues_random_blob_url_and_accepts_bearerless_upload`).
