# Concurrency Implementation Log

Started: 2026-07-13. Plan: `docs/concurrency-plan.md` (Phases 1–4 + 6; Phase 5 production profile **skipped**).

## Goals

- Server enforces GitHub Actions `concurrency:` (workflow- and job-level).
- Runner only observes `JobCancellation` (official wire shape).
- Unit/integration tests, official-runner E2E, live GitHub conformance, chaos tests.

## Phase status

| Phase | Status | Notes |
|-------|--------|-------|
| 1 JobCancellation wire | **done** | `{jobId, timeout}`, type `JobCancellation` |
| 2 Parser + protocol | **done** | Concurrency types, Pending, NDJSON reason |
| 3 Server enforcement | **done** | groups, pending, cancel, release, matrix |
| 4 aksh-runner cancel | **done** | non-blocking, kill-at, jobId match |
| 5 Production profile | **skipped** | per request |
| 6 Verification | **mostly done** | unit + local E2E + live GH probes |

## Change log

### Wire / protocol

- `ExecutionStatus::Pending`; `NdjsonEvent::{Job,Run}Status.reason`
- `JobPlan` concurrency fields; parser `Concurrency` / `ConcurrencyQueue`
- Fixed `azdo::message_type::JOB_CANCELLED` = **`"JobCancellation"`** (was `"JobCancelled"`)
- Cancel body: `{"jobId": <agent GUID>, "timeout": "00:05:00"}`

### Server (`aksh-runner-server`)

- New `concurrency.rs` module (groups, holders, eval, queue modes)
- `QueuedCancellation.agent_job_id`; `cancel_run_inner` / job cancel / release / promote
- Workflow-level + job-level gates; `queue: single|max`; case-insensitive groups; repo scope
- Broker **root** path `/runner/server/message` delivers JobCancellation (was missing)
- Cancel messageIds in high range (`>= 1_000_000`) so they never collide with `request_id` job messageIds (runner dedup bug)
- Free `session_active_requests` on complete so successor jobs poll immediately
- Late Success/Failure cannot overwrite Cancelled

### Runner (`aksh-runner`)

- Parse `{jobId, timeout}`; ignore mismatched jobId
- Non-blocking cancel + `kill_at`; 45s busy-job drain

### Tests

- Parser: bare string, mapping, expression cancel, queue:max+cancel error, job plan fields
- Server: FIFO, cancel-in-progress, queue max/overflow, case, job-level, matrix, needs gate order, shared namespace, chaos, broker-root cancel + successor job, late-success lock, etc.
- Runner: `parse_timespan_secs`

### Live GitHub (`Bnjoroge1/aksh-concurrency-probe`)

| Probe | Result |
|-------|--------|
| Empty group expression | **failure** (workflow file issue) → reject is correct |
| cancel-in-progress | first **cancelled**, second **success** |
| FIFO pending | both **success** serially |
| Cancel timeout MITM | not captured; keep `00:05:00` |

### Local E2E (aksh-runner + aksh server, 2026-07-13)

```
CANCEL_E2E PASS   # A cancelled mid-sleep; B success
PENDING_E2E PASS  # B pending until A success, then B success
```

Runner log shows: `JobCancellation` (messageId 1000001) → cancel → next `RunnerJobRequest` → success.

### Remaining / known gaps

- Official runner E2E needs port-80 redirect (`sudo ./scripts/e2e-setup.sh`)
- Real GitHub cancel `timeout` not MITM-verified (assumption `00:05:00`)
- Two pre-existing `aksh-runner` worker dispatch unit tests fail (`--via` / test binary path) — unrelated

### Concurrency deep review bug fixes (2026-07-13)

Review: `plans/concurrency-deep-review.md`. All 7 bugs fixed; 6 regression tests added.

| ID | Fix | Files |
|---|---|---|
| C-01 | `promote_next_from_group` no longer installs `Holder::Job` as running before `max-parallel` check passes; deferred jobs stay at front of pending queue | `lib.rs` |
| C-02 | Reusable workflow `Holder::JobSet` now constructed and acquired for caller and embedded concurrency; all members gated before dispatch | `lib.rs` |
| C-03 | `evaluate_concurrency` accepts typed `ConcurrencyContext` with scope; `WorkflowSubmission.inputs` added; job evaluation receives strategy/needs from context_data | `concurrency.rs`, `lib.rs`, protocol `lib.rs` |
| C-04 | Broker listener cancels active worker immediately on overlap instead of blocking 45s; successor never dropped after acknowledge | `broker_listener.rs` |
| C-05 | `submit_run_inner` derives initial `run.status` from `summarize_run`; `promote_ready_jobs` calls `summarize_run` after eval failures | `lib.rs` |
| C-06 | `cancel-in-progress` evaluated with `eval_bool` (expression truthiness); `queue:max` + effective true rejected before state mutation | `concurrency.rs` |
| C-07 | `holder_keys` removed per-run in `release_concurrency_for_run`; per-key in `release_concurrency_for_job` | `lib.rs` |

Capture harness (`concurrency-log-compare.py`, `run-concurrency-aksh-capture.sh`) hardened: cross-run markers, SHOULD_NOT_REACH execution, and contradictory job/run conclusions are now hard failures. Multi-job captures identify per-job UUIDs from runner log deltas.

Tests: 182 pass across `aksh-gha-expressions`, `aksh-gha-parser`, `aksh-gha-protocol`, `aksh-runner-server` (87 server, including 6 new regressions: `c01_`, `c02_`, `c05_`, `c06_` ×2, `c07_`).

## Key files

- `crates/aksh-gha-protocol/src/{lib,azdo}.rs`
- `crates/aksh-gha-parser/src/lib.rs`
- `crates/aksh-runner-server/src/{lib,concurrency}.rs`
- `crates/aksh-runner/src/listener/{broker_listener,job_dispatcher}.rs`
- `fixtures/concurrency-*.yml`
- `docs/concurrency-plan.md`

### Live GitHub varied probes (2026-07-13, 12 scenarios)

Capture: `benchmarks/real-world/results/concurrency-live/2026-07-13T13-19-42Z/`  
Report: `.../VERIFICATION-REPORT.md`

| # | Scenario | GH result |
|---|----------|-----------|
| 01 | bare-string serialize | PASS |
| 02 | cancel-in-progress | PASS (step cancelled + Complete job success) |
| 03 | fifo pending | PASS |
| 04 | cancel expr true | PASS |
| 05 | cancel expr false | PASS |
| 06 | queue:single replace pending | PASS |
| 07 | case CaseGroup vs casegroup | **OVERLAP** — live GH appears case-sensitive |
| 08 | job-level serial | PASS |
| 09 | multi-job workflow hold | PASS |
| 10 | empty group | PASS (failure, 0 jobs) |
| 11 | expr group ref | PASS |
| 12 | matrix same group | PASS (serial cells; 1 pending cancelled) |

**Score: 11/12** (07 is fidelity gap vs docs/plan).

### GitHub vs aksh log/step content compare (2026-07-13)

Harness:
- `benchmarks/real-world/run-concurrency-aksh-capture.sh`
- `benchmarks/real-world/concurrency-log-compare.py`

Artifacts:
- GH: `benchmarks/real-world/results/concurrency-live/2026-07-13T13-19-42Z/`
- aksh: `benchmarks/real-world/results/concurrency-live/aksh-compare-2026-07-13T13-54-52Z/`
- Report: `.../aksh-compare-.../LOG-CONTENT-COMPARE.md`

Compared: run conclusion, job conclusions, user step conclusions, SCENARIO/DONE markers in step log blobs, cancel annotation presence.

Result: **12/12 soft-pass** after scoping (run conclusions + scenario markers + step outcomes match). Remaining fidelity note: GH cancelled steps include `##[error]The operation was canceled.` in step log; aksh step blobs may omit that annotation (job still `cancelled`).
