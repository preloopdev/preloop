# Plan 003: Conform job-lease renewal semantics to the official runner (runner + server)

> **Executor instructions**: Follow step by step; run every verification; STOP conditions are
> binding. Update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 839791c..HEAD -- crates/aksh-runner/src/worker/job_runner.rs crates/aksh-runner-server/src/lib.rs`
> `crates/aksh-runner-server/src/lib.rs` had large uncommitted concurrency changes at planning
> time — line numbers WILL have shifted; anchor on symbol names (`spawn_renew_loop`,
> `agent_request_locked_until`, `reap_once`) and compare excerpts. Mismatch beyond line
> numbers = STOP.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED (touches the keep-alive path; a bug can fail healthy long jobs)
- **Depends on**: none
- **Category**: bug (protocol compatibility)
- **Planned at**: commit `839791c`, 2026-07-13

## Why this matters

Official semantics (`src/Runner.Listener/JobDispatcher.cs:761-823` in the mirror at
`~/mitm-proxy/experiments/mitm/.cache/runner.server/src/`): the runner renews every 60 s,
tracks the server-returned `lockedUntil`, backs off 5–30 s on transient errors, keeps retrying
only until `lockedUntil + 5 min`, and treats job-not-found (404) as terminal (job lost — stop
renewing, cancel the worker). The aksh runner ignores `lockedUntil` entirely, never backs off,
and keeps renewing a lost job every 60 s forever. The aksh server is internally inconsistent:
it *advertises* `lockedUntil = 2099-12-31T23:59:59Z` (a lie) while its reaper *enforces* a
120 s since-last-renew disconnect window. Interchangeability demands: server advertises the
lease it enforces; runner honors the lease it is given.

## Current state

- `crates/aksh-runner/src/worker/job_runner.rs:602-632` — renew loop:

  ```rust
  /// the fallback interval until `lockedUntil` parsing is made exact.   // (doc comment, line 602)
  fn spawn_renew_loop(
      rpt: Arc<ReportingContext>,
      cancel_rx: watch::Receiver<bool>,
  ) -> tokio::task::JoinHandle<()> {
      ...
      match rpt.run_service.renew_job(&rpt.access_token, &body).await {
          Ok(resp) => {
              let locked_until = resp.get("lockedUntil").and_then(|v| v.as_str()).unwrap_or("unknown");
              info!("Job lock renewed, lockedUntil={locked_until}");
          }
          Err(e) => { warn!("renewjob failed: {e:#}"); }
      }
  ```

  The loop then sleeps a fixed 60 s (later in the fn) racing `cancel_rx`.

- `crates/aksh-runner-server/src/lib.rs:4259-4261`:

  ```rust
  fn agent_request_locked_until() -> String {
      "2099-12-31T23:59:59Z".to_owned()
  }
  ```

- `crates/aksh-runner-server/src/lib.rs:151-158` (inside `reap_once`): 120 s disconnect
  threshold on `last_renewed_at`.
- Official constants to mirror: renew interval 60 s; retry window until `lockedUntil + 5 min`;
  first-renew retry limit 5; transient backoff 5–30 s; 404 (`TaskOrchestrationJobNotFoundException`)
  = terminal (`JobDispatcher.cs:761-823`, `src/Runner.Common/RunServer.cs:49-55`).
- GitHub's real advertised lease is ~10 min (inferred from the +5 min grace math; exact value
  not observable in sources — see STOP conditions).
- Error type available runner-side: `crates/aksh-runner/src/client/http.rs` (check how HTTP
  status errors surface from `run_service.renew_job`; `crates/aksh-runner/src/client/run_service.rs:18-50`).
- Existing test patterns: server lease reaper test `runner_lease_expiration_disconnect_reaper`
  (search in `lib.rs` tests), runner-side unit tests colocated in `job_runner.rs`.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Server tests | `cargo test -p aksh-runner-server --quiet` | all pass |
| Runner tests | `cargo test -p aksh-runner --quiet` | all pass |
| Full gate | `cargo fmt --all --check && cargo clippy --workspace --all-targets && cargo test --workspace --quiet` | exit 0 |

## Scope

**In scope**:
- `crates/aksh-runner/src/worker/job_runner.rs` (renew loop only)
- `crates/aksh-runner-server/src/lib.rs` (`agent_request_locked_until`, its call sites, the
  reaper threshold constant)

**Out of scope** (do NOT touch):
- `crates/aksh-runner/src/listener/broker_listener.rs` — has uncommitted concurrency work.
- The renew-loop's first-renew health probes (`job_runner.rs:634-676`) — leave as is.
- `broker_renew_job` request/response field names — audited clean.

## Git workflow

- Branch: `advisor/003-renew-lease-conformance`; conventional commits; no push/PR unless told.

## Steps

### Step 1: Server — advertise the lease it enforces

1. Introduce one constant near the reaper: `const JOB_LEASE_SECONDS: u64 = 600;` (10 min,
   official-like).
2. `agent_request_locked_until()` → `now + JOB_LEASE_SECONDS` formatted like the server's other
   ISO timestamps (find the existing helper — grep `server_iso_now` in `lib.rs` — and match its
   format exactly).
3. Reaper: replace the literal 120 s with `JOB_LEASE_SECONDS` so enforcement == advertisement.
   Keep the separate job-timeout check (21600 s) untouched.
4. Update the lease-reaper test to the new window (it manipulates `last_renewed_at`; shift its
   fixture by the new constant, don't sleep).

**Verify**: `cargo test -p aksh-runner-server --quiet` → all pass;
`grep -n "2099-12-31" crates/aksh-runner-server/src/lib.rs` → no matches.

### Step 2: Runner — honor lockedUntil, back off, treat 404 as terminal

Rework the body of `spawn_renew_loop` (keep the signature and the health-probe block):

1. Parse `lockedUntil` (RFC3339 via `chrono`, already a workspace dep — confirm in
   `crates/aksh-runner/Cargo.toml`; if absent, STOP) into `lease_deadline: Option<Instant>`-style
   tracking (store as `DateTime<Utc>`, compare with `Utc::now()`).
2. Success → sleep 60 s (unchanged).
3. Error → if HTTP status is 404: log `error!("job lease lost (404); stopping renewal")`,
   signal the job to cancel via the existing cancel channel IF a sender is reachable — if the
   renew loop only holds a `watch::Receiver` (it does), instead return from the task after
   setting a new `Arc<AtomicBool>` `lease_lost` flag passed in by the caller; the caller
   (`run_job`) checks it after `run_steps` and reports the job result as `Failed` with an
   annotation "runner lease lost". Wire the flag: `spawn_renew_loop(rpt, cancel_rx, lease_lost.clone())`.
4. Transient error → exponential backoff 5 s→10 s→20 s→30 s (cap 30 s), reset on success,
   and give up (set `lease_lost`, exit) once `Utc::now() > lockedUntil + 5 min` (mirror
   `JobDispatcher.cs:820`). If no `lockedUntil` was ever received, fall back to 5 consecutive
   failures (mirror the first-renew retry limit of 5).
5. Update the stale doc comment on line 602.

**Verify**: `cargo test -p aksh-runner --quiet` → all pass.

### Step 3: Unit tests (runner)

In `job_runner.rs` tests (match the existing inline test style):
- `renew_backoff_caps_at_30s` — pure function test: extract backoff computation into
  `fn renew_backoff(attempt: u32) -> Duration` and assert 5/10/20/30/30.
- `lease_deadline_gives_up_after_grace` — extract the give-up predicate
  `fn lease_expired(locked_until: DateTime<Utc>, now: DateTime<Utc>) -> bool` (true past
  +5 min) and assert boundary cases.
- Parsing: `"2026-07-13T12:00:00Z"` parses; garbage → None (no panic).

**Verify**: `cargo test -p aksh-runner --quiet` → all pass, incl. 3+ new tests.

## Test plan

Steps 1 & 3 above. The server side reuses the existing reaper test (shifted constant); the
runner side tests the extracted pure functions — do not spawn real timers in tests.

## Done criteria

- [ ] Full gate exits 0
- [ ] `grep -rn "2099-12-31" crates/` → no matches
- [ ] Server reaper threshold and advertised `lockedUntil` derive from the same constant
- [ ] Runner: 404 on renew sets the lease-lost path (unit-tested predicate + flag wiring)
- [ ] No files outside scope modified (`git status`)
- [ ] `plans/README.md` row updated

## STOP conditions

- Symbols `spawn_renew_loop` / `agent_request_locked_until` / `reap_once` missing or reshaped
  vs the excerpts (drift — likely the concurrency work landed on top).
- `chrono` (or equivalent RFC3339 parsing) is not already a dependency of `aksh-runner` —
  report; adding deps is an operator decision.
- `run_service.renew_job` errors don't expose HTTP status — report what they expose instead of
  string-matching error text.
- The official runner turns out to renew against `lockedUntil` differently than described when
  you read `JobDispatcher.cs:761-823` — trust the C# source, report the delta.

## Maintenance notes

- If the production profile later shortens leases for utilization, only `JOB_LEASE_SECONDS`
  moves; the runner now adapts automatically — that was the point.
- Reviewer: scrutinize the lease-lost path for double-completion (job completing normally while
  the flag is set); completion must win if the worker already finished.
- Deferred: blocking job start on first successful renew (official does; aksh starts
  concurrently) — recorded as rejected in `plans/README.md`, revisit only if lease races
  appear in E2E.
