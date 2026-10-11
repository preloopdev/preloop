# PR-E: liveness gate for legacy reporting writes (round-3 §7)

## Problem
A dead job's runtime token could still write through the legacy `/_apis/v1`
reporting plane, which never got the liveness gate the Twirp results writes
carry (`auth::require_live_results_job`):

- `WS /ws/live-logs/:job_id` upgraded to 101 with **no liveness check**; forged
  lines sent afterwards hit `record_live_log_wrapper`, which reopens the closed
  feed on fresh input — the log-read API then showed **only the forged lines**;
  the original tail was wiped (round-3 §7, reproduced).
- Legacy timeline PATCH (`/_apis/v1/Timeline/…`, `/_apis/v1/plans/…/timelines/…`,
  `/:org/_apis/v1/Timeline/…`) accepted a settled job's token with 200.
- Legacy log append/create and the `TimeLineWebConsoleLog` live-console feed
  likewise accepted stale tokens.

## Fix
Reused the existing Twirp-side liveness check — same authoritative lookup
(`auth::job_is_live` → `auth::require_live_job`, which resolves the request
record from the control backend and rejects settled/unknown jobs with 403) —
no parallel liveness logic was invented.

- `crates/preloop-runner-server/src/live_logs.rs` — `ws_live_logs`: after the
  existing identity/ownership checks, `require_live_job(agent_job_id)` runs
  before the 101 upgrade. The system credential still bypasses, matching the
  results-plane rule.
- `crates/preloop-runner-server/src/timeline_logs.rs` —
  `authorize_reporting_callback` now returns the authorized request record, and
  a new `require_live_reporting_job` helper applies the gate (system token
  bypasses; anything else needs a live job). Applied to every legacy reporting
  **write**: timeline PATCH (scoped, plan-level), log create, log append,
  `TimeLineWebConsoleLog` console feed.
- `crates/preloop-runner-server/src/compat_ghes.rs` — the `/:org/…` variants of
  the same four writes delegate to the same helper, so both URL forms are
  gated.

Deliberately **not** gated: the GET/read paths (reads were never gated on the
results plane either) and the `FinishJob` completion endpoints (a runner's
final completion report is a lifecycle write that lands while the job is live;
settling is an idempotent guarded UPDATE, and gating it would 403 a
legitimate retried completion).

## Tests
4 new regression tests in `crates/preloop-runner-server/tests/recovery.rs`:
- `live_log_websocket_rejects_stale_job_token` — live token streams (101),
  job settles via `complete_job_inner`, stale token gets no 101, retained tail
  intact and still marked closed.
- `legacy_timeline_patch_rejects_stale_job_token` — 200 while live, 403 after
  settle.
- `legacy_log_append_rejects_stale_job_token` — create + append 200/202 while
  live, 403 after settle.
- `legacy_console_log_rejects_stale_job_token` — 200 while live, 403 after
  settle.

## Verification
- `cargo check -p preloop-runner-server`: clean.
- `cargo fmt -p preloop-runner-server -- --check`: clean.
- `cargo clippy -p preloop-runner-server --features test-support --all-targets`:
  no warnings in touched files (remaining warnings are pre-existing in test
  helpers).
- New tests: 4/4 pass. Pre-existing `live_log_*` suite: 11/11 pass.
- Live-server repro of the round-3 wipe (patched server, port 19105; script at
  `.repro/repro-wipe.sh`, run manually in steps):
  1. `preloop store migrate --store sqlite://<state>/preloop.db`
     (state dir `.repro/home`, i.e. `PRELOOP_HOME` under the worktree).
  2. `preloop serve --listen 127.0.0.1:19105` (note: the CLI's 30 s readyz
     wait is too short on a loaded box — the server took ~50 s to listen;
     polled `/readyz` manually).
  3. Submitted a run via `POST /api/v1/runs` (system token from
     `<PRELOOP_HOME>/engine.token`); read `agent_job_id`/`timeline_id` from
     the `job_requests` table (`plan_id` is derived = agent job id string).
  4. Minted a job runtime token in Python: HS256 with `<state>/hmac-key.bin`,
     claims `sub=preloop-job-<agent>`,
     `scp=Actions.Results:<plan>:<agent>`, `iss/nbf/iat/exp/jti` (mirrors
     `AppState::local_jwt`).
  5. **Live**: WS `GET /ws/live-logs/<agent>` → 101, sent
     `{"stepId":"step-1","value":["original-tail-line"]}` → accepted.
  6. Settled the job through the real runner path:
     `POST /_apis/v1/FinishJob/s/h/<plan>` with the job token
     (`{"jobId","result":"succeeded","timelineId"}`) → 200; DB row shows
     `result=success` (token now stale).
  7. **Attack with the stale token** — all rejected:
     - WS connect → `HTTP 403` (no 101; `InvalidStatus` client-side)
     - `PATCH /_apis/v1/plans/<plan>/timelines/<timeline>/records` → 403
     - `POST /_apis/v1/Logfiles/s/h/<plan>/1` → 403
     - `POST /_apis/v1/TimeLineWebConsoleLog/s/h/<plan>/<timeline>/step-1`
       → 403
  8. **Tail intact**: `GET /api/v1/runs/<run>/jobs/build/logs/live` (system
     token) returns `"value":["original-tail-line"]` only — no forged lines.

## Notes
- No new dependencies; no supply-chain policy files touched.
- The `.repro/` directory in the worktree holds the repro script, server log,
  and local state; it is intentionally untracked scratch, not part of the PR.
