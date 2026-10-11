# Fix: bind legacy artifact handlers to the caller's run; stop leaking `storage_key` in list responses

Round-3 pentest finding §6 — cross-run legacy artifact squat (+ path leak).

## The bug

`POST /_apis/pipelines/workflows/<run_id>/artifacts` accepted any valid
local JWT and never checked that `:run_id` belonged to the caller, so run
A's LIVE job token could create artifacts catalogued under run B
(`200`, squatting the victim run's artifact namespace), and list/get
run B's artifacts. The list response also returned the server-side
`storage_key` — a filesystem path — in each entry.

## The fix (`crates/preloop-runner-server/src/cache_artifacts.rs`)

- New `require_legacy_artifact_run_binding()`: resolves the caller's
  `job_id` from its job token through the job request record and requires
  `record.run_id == :run_id`, else `403 "artifact access requires a token
  for that workflow run"` (same message as the v2 artifact path's
  `artifact_v2_authorized_run_scope`). Applied to `artifact_create`,
  `artifact_list`, and `artifact_get_compat`. The system bearer bypasses,
  mirroring the other legacy artifact checks; a request with no resolvable
  job claims is rejected (401/403).
- `artifact_list` no longer emits `path`/`storage_key` — entries are
  `{id, run_id, name, file_name, size}`. (`ArtifactRecord` on create still
  carries `path`; that surface returns only the artifact the caller just
  created and was left as-is.)
- Scoping note: the adjacent native handlers (`POST /api/v1/artifacts`,
  `GET /api/v1/artifacts/:artifact_id`) sit behind the system-token-only
  `require_native_bearer` middleware, so they have no job-token run-binding
  gap — no change there.

## Tests

- Updated `legacy_artifact_create_rejects_stale_job_token` to bind the
  legacy path to the job's real run (`register_live_job_with_run` now
  returns the run key), since a random `:run_id` is now 403 by design.
- New `legacy_artifact_handlers_bind_to_caller_run` (in
  `crates/preloop-runner-server/tests/concurrency.rs`): two runs, each with
  a live job token —
  - run A's live token → create/list/get under run B → 403 (all three);
  - same-run create/list/get → 200 for both tokens;
  - list response contains no `path` or `storage_key`.

## Repro verification (live patched server, port 19106)

Built `preloop-server` from this branch, migrated state, served on
127.0.0.1:19106. Submitted two runs
(run A `48002afe-…`, run B `ecb1b11c-…`), registered two runners over the
real distributedtask/broker protocol, acquired each run's job, and used the
jobs' live `SystemVssConnection` access tokens:

| # | request | result |
|---|---------|--------|
| 1 | token A → `POST /_apis/pipelines/workflows/<run-B>/artifacts` | **403** |
| 2 | token A → `POST …/<run-A>/artifacts` | 200 |
| 3 | token B → `POST …/<run-B>/artifacts` | 200 |
| 4 | token A → `GET …/<run-B>/artifacts` | **403** |
| 5 | token A → `GET …/<run-B>/artifacts/<id>` | **403** |
| 6 | token B → `GET …/<run-B>/artifacts` | 200, body `{"count":1,"value":[{"id":…,"run_id":…,"name":"b-art","size":0}]}` — no storage path |
| 7 | token B → `GET …/<run-B>/artifacts/<id>` | 200 |
| 8 | headerless → `GET …/<run-B>/artifacts` | 401 |

Checks run: `cargo fmt --check`, `cargo clippy -p preloop-runner-server
--all-targets` (clean), `cargo test -p preloop-runner-server --features
test-support --test concurrency legacy_artifact` → 2 passed.

Note: `preloop serve` (the engine wrapper) failed its own `/readyz`
readiness check in this sandbox, so the repro ran against the
`preloop-server serve` binary directly — same control plane, same routes.
