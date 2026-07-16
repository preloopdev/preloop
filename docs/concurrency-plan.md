# GitHub Actions `concurrency:` — Implementation Plan

Status: planned 2026-07-13. Companion audit: `docs/fidelity-gap.md` §3a.

Official runner sources cited as `src/...` (actions/runner layout); local mirror for line
numbers: `~/mitm-proxy/experiments/mitm/.cache/runner.server/src`.

## Context

Implement the GitHub Actions `concurrency:` feature end-to-end:

1. **aksh server** enforces GitHub's concurrency-group semantics (workflow-level and
   job-level `concurrency:`, `cancel-in-progress`, `queue: single|max`) for the local
   in-memory server.
2. **aksh-runner** handles the runner-visible half — `JobCancellation` — exactly as the
   official runner does (`Runner.Listener/JobDispatcher.cs` semantics), since concurrency's
   only runner-observable effect is job cancellation. The official runner has **zero**
   knowledge of concurrency groups; enforcement is purely control-plane.
3. A production profile for a remote CI platform: persistent state + a utilization-maximizing
   scheduler. **Strictly additive**: same binary, same protocol, zero divergence from GitHub
   behavior in anything a runner or workflow can observe.

Prerequisite discovered during audit: aksh's `JobCancellation` wire message cannot be
consumed by the unmodified official runner at all (wrong id, missing timeout) — fixed in
Phase 1 because every concurrency cancellation path depends on it.

## Compatibility invariants (binding for every phase)

Composability is the contract: any runner ⇄ any server. Therefore:

1. **Runner-facing wire surfaces are byte-shape-identical to GitHub's.** No aksh-only
   fields, no renamed keys, no extra message types on `/_apis`, broker, run-service, or
   results-service routes. The `JobCancellation` body is exactly the official
   `JobCancelMessage` shape (Phase 1) — nothing else.
2. **The runner learns nothing about concurrency.** Enforcement is 100% control-plane, as on
   GitHub (no concurrency symbol exists anywhere under `src/Runner.*`). aksh-runner changes
   (Phase 4) only bring cancellation handling to exact `JobDispatcher.cs` semantics so it
   behaves identically against real GitHub.
3. **Workflow semantics match GitHub exactly** in both profiles: same pending/cancel
   decisions, same validation errors, same statuses. The production profile may not alter
   any of them.
4. **Production features land only on aksh-native surfaces** (`/api/v1/...` NDJSON/REST,
   CLI flags, persistence): additive, never observable through the runner protocol. A
   workflow run must be indistinguishable between local and production profiles.
5. **Where GitHub's exact behavior is not derivable from docs/sources** (two cases:
   cancel-timeout value, empty-group handling), the value is verified against real GitHub
   before shipping (see Phase 6 conformance checks), not guessed silently.

## Normative semantics (from GitHub docs, retrieved 2026-07-13)

Source: docs.github.com “Control the concurrency of workflows and jobs”.

- A concurrency **group** admits **at most one running** job/workflow-run at a time.
- A newly queued run/job whose group is busy becomes **`pending`**.
- **`queue: single`** (default): at most one pending holder; a newer arrival **cancels and
  replaces** any existing pending holder(s).
- **`queue: max`**: up to **100** pending holders wait FIFO; arrivals beyond the cap are
  cancelled. `queue: max` + literal `cancel-in-progress: true` is a **validation error**.
- **`cancel-in-progress`**: `true`/`false` or an **expression**; when truthy at arrival time,
  the currently *running* holder in the group is cancelled and the arrival takes the slot.
- Group names are **case-insensitive** (`prod` == `Prod`).
- Ordering: FIFO by time each holder **started waiting** on the group (GitHub explicitly
  does not guarantee ordering; FIFO is conformant).
- Allowed expression contexts for `group` and `cancel-in-progress`: `github`, `inputs`,
  `vars`; job-level additionally `needs`, `strategy`, `matrix`.
- `concurrency:` accepts a **bare string shorthand** (`concurrency: ci-${{ github.ref }}`)
  or a mapping with `group`, `cancel-in-progress`, `queue`.
- Groups are scoped to the **repository**; workflow-level runs and job-level jobs share one
  namespace.
- Reusable workflows: the caller's `concurrency:` on the `uses:` job applies to the whole
  invocation; the callee's workflow-level `concurrency:` (upstream field name
  `EmbeddedConcurrency`, see `docs/runner/workflow-call-plan.md`) is enforced separately.

## Phase 1 — `JobCancellation` wire conformance (prerequisite, independently shippable)

Official shape: `JobCancelMessage { JobId: Guid, Timeout: TimeSpan }`
(`src/Sdk/DTWebApi/WebApi/JobCancelMessage.cs:18-36`), consumed at
`src/Runner.Listener/Runner.cs:732-735`, matched against the `AgentJobRequestMessage.jobId`
GUID in `JobDispatcher.Cancel` (`src/Runner.Listener/JobDispatcher.cs:141-159` — mismatch ⇒
`return false`, message ignored).

Server (`crates/aksh-runner-server/src/lib.rs`):

1. Extend `QueuedCancellation` (lib.rs:1009-1012) with `agent_job_id: uuid::Uuid` — the GUID
   sent as `jobId` in the job message (`aksh-gha-protocol/src/azdo.rs:219-220`).
   Add helper `fn agent_job_id_for(inner: &InnerState, run_id: RunId, job_id: &JobId) -> Option<uuid::Uuid>`
   resolving via `inner.job_requests` / `inner.inflight_requests` (no existing equivalent
   found). At every enqueue site — `cancel_run` (lib.rs:1393-1395), matrix fail-fast
   (lib.rs:3819-3861), reaper timeout (lib.rs:119-138) — resolve it; if `None` (job not in
   flight) do **not** enqueue a message, just mark the job `Cancelled` (nothing to deliver).
2. Replace both message bodies (`lib.rs:2021-2024` broker, `lib.rs:3199-3202` AzDO) with the
   official shape, exactly:
   `json!({ "jobId": cancellation.agent_job_id, "timeout": "00:05:00" })`.
   Drop `runId` — clean cutover; no consumer other than aksh-runner exists, and it is updated
   in the same phase. `00:05:00` = upstream default cancel grace (5 min; matches the existing
   comment in `broker_listener.rs:300-301`).

Runner (`crates/aksh-runner`):

3. `listener/job_dispatcher.rs`: store `job_id: Option<uuid::Uuid>` on `RunningJob`, parsed
   from the job message body's `"jobId"` at `spawn_job` time.
4. `listener/broker_listener.rs:295-321` (`"JobCancellation"` arm): parse body
   `{ jobId, timeout }`. If `jobId` parses and does not equal the active job's GUID →
   `debug!` and ignore (mirrors `JobDispatcher.cs:146-149`). Parse `timeout` with a new
   helper `fn parse_timespan_secs(s: &str) -> Option<u64>` accepting `hh:mm:ss` and
   `d.hh:mm:ss[.fffffff]` (no existing equivalent found); default 300 on absence/parse
   failure.

Edge handling: a cancellation for an already-completed job → `agent_job_id_for` returns
`None` or the runner's GUID check misses; both paths no-op, matching official.

## Phase 2 — Parser + protocol types

`crates/aksh-gha-parser/src/lib.rs`:

1. New types next to `Strategy` (lib.rs:583-595):

   ```rust
   #[derive(Debug, Clone, PartialEq)]
   pub struct Concurrency {
       /// Raw group string; may contain `${{ }}` — evaluated server-side.
       pub group: String,
       /// Raw `cancel-in-progress` value: "true" / "false" / a `${{ }}` expression.
       pub cancel_in_progress: Option<String>,
       pub queue: ConcurrencyQueue,
   }

   #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
   pub enum ConcurrencyQueue { #[default] Single, Max }
   ```

   Custom `Deserialize`: bare YAML string ⇒ `{ group: s, cancel_in_progress: None, queue: Single }`;
   mapping ⇒ `group` (required, string), `cancel-in-progress` (bool or string, stored raw),
   `queue` (`"single"`/`"max"`). Validation error (parser `Err`, same style as existing parse
   errors) when `queue == Max` and `cancel_in_progress == Some("true")` (literal only —
   expressions cannot be validated statically), message:
   `"concurrency: `queue: max` cannot be combined with `cancel-in-progress: true`"`.
2. Add `pub concurrency: Option<Concurrency>` to `Workflow` (lib.rs:86-160) and to `Job`
   (lib.rs:465-511), wired through the existing YAML extraction the same way `strategy` is.
3. Reusable workflows: in `expand_jobs_with_reusables` (lib.rs:655-717) carry the caller
   job's `concurrency` and the callee workflow's `concurrency` (as `embedded_concurrency`)
   onto the expansion result so the server can enforce both (see Phase 3 step 6). Fields on
   the existing reusable-call bookkeeping struct that `RunRecord.reusable_calls` is built
   from.

`crates/aksh-gha-protocol/src/lib.rs`:

4. Add `Pending` variant to `ExecutionStatus` (lib.rs:156-195), serialized following the
   existing rename pattern (`"pending"`). Fix every non-wildcard `match` the compiler flags
   (`cargo check --workspace` enumerates them) — semantic rule everywhere: `Pending` is
   non-terminal, not runnable, counts as "not started" (i.e., wherever `Queued` is treated as
   awaiting execution, `Pending` behaves the same except it must **not** be eligible for
   runner assignment).
5. `NdjsonEvent::RunStatus` and `JobStatus` gain
   `#[serde(skip_serializing_if = "Option::is_none")] reason: Option<String>` with the two
   literals `"concurrency_pending"` and `"concurrency_cancelled"` (emitted in Phase 3).
   Existing emitters pass `None`.

The runner job payload is untouched — concurrency never reaches the runner (parity with
GitHub: no concurrency fields exist anywhere under `src/Runner.*`).

## Phase 3 — Server enforcement (local, in-memory)

All in `crates/aksh-runner-server/src/lib.rs`.

### State

```rust
/// Key: (lowercased github.repository or "", lowercased evaluated group name).
concurrency_groups: BTreeMap<(String, String), ConcurrencyGroup>,
held_runs: BTreeMap<RunId, Vec<QueuedJob>>,          // workflow-level pending runs
concurrency_blocked: Vec<QueuedJob>,                  // job-level pending jobs (FIFO)

struct ConcurrencyGroup {
    running: Option<Holder>,
    pending: VecDeque<Holder>,   // FIFO by arrival
}
enum Holder {
    Run(RunId),                                   // workflow-level
    Job { run_id: RunId, job_id: JobId },         // job-level
    JobSet { run_id: RunId, job_ids: BTreeSet<JobId> }, // reusable invocation
}
```

`QueuedJob` (lib.rs:996-1007) gains `concurrency: Option<parser::Concurrency>` (raw).

### Group key + evaluation

- `fn concurrency_key(repo: &str, group: &str) -> (String, String)` — both lowercased;
  repo from the submission's `github.repository` context value, `""` when absent. Preserve
  the original-case group string inside `ConcurrencyGroup` for display/events.
- Evaluate `group` / `cancel-in-progress` strings with the existing interpolation used for
  env/with/run in `aksh-gha-parser/src/job_builder.rs` (expression resolution entry points
  there) over a `Context` from `build_context` (`aksh-gha-parser/src/eval.rs:100-125`).
  Workflow-level: `github`, `inputs`, `vars`. Job-level: additionally `needs` (hydrate via
  the existing pattern at lib.rs:3863-3872), `strategy`, `matrix`.
- `cancel-in-progress` truthiness: after interpolation, `"true"` (case-insensitive) ⇒ true,
  anything else ⇒ false (GitHub coerces expression results to boolean; boolean-literal YAML
  was already stored as `"true"`/`"false"`).
- Evaluated group empty ⇒ reject: workflow-level → HTTP 422 from `submit_run` with body
  message `"concurrency group name must not be empty"`; job-level → mark the job `Failure`
  with an NDJSON annotation. (Assumption #2 below.)

### Workflow-level flow (in `submit_run_inner`, lib.rs:1135-1315)

After parsing and building `QueuedJob`s, before the existing enqueue at lib.rs:1269-1283:

1. No `workflow.concurrency` ⇒ existing behavior, untouched.
2. Evaluate group + cancel-in-progress. Look up `ConcurrencyGroup`:
   - **Slot free** → `running = Some(Holder::Run(run_id))`; enqueue jobs normally.
   - **Slot busy, cancel-in-progress true** → cancel the running holder (below), install
     this run as `running`, enqueue jobs normally.
   - **Slot busy, cancel-in-progress false** → run becomes **pending**: every job status set
     `ExecutionStatus::Pending`, jobs stashed in `held_runs`, run **not** enqueued. Emit
     `RunStatus`/`JobStatus` events with `reason: "concurrency_pending"`. Then apply the
     arrival's own queue mode:
     - `Single`: cancel **all** existing pending holders of the group
       (`reason: "concurrency_cancelled"`), push this one.
     - `Max`: if `pending.len() < 100` push; else cancel **this** run immediately.
3. Cancelling a holder = for `Run`/`JobSet`: mark all non-terminal jobs of that run(s)
   `Cancelled`; for in-flight jobs enqueue `QueuedCancellation` (Phase 1 shape); remove its
   held jobs / queued entries (`queue`, `pending_jobs`, `held_runs`, `concurrency_blocked`);
   finalize run status `Cancelled`; emit events. Reuse the body of `cancel_run`
   (lib.rs:1379-1420) — extract it into `fn cancel_run_inner(inner: &mut InnerState, run_id) -> usize`
   called by both the route handler and concurrency code (route keeps HTTP concerns).

### Job-level flow

At the two points a job becomes *ready* — the empty-`needs` branch in `submit_run_inner`
(lib.rs:1272-1276) and `promote_ready_jobs` (lib.rs:3705-3731) after `needs_satisfied` +
`under_max_parallel` pass:

1. `job.concurrency` absent ⇒ push to `queue` (existing behavior).
2. Else evaluate; slot free ⇒ `running = Holder::Job{..}`, push to `queue`.
   Busy + cancel-in-progress true ⇒ cancel running holder (job holder ⇒ mark that one job
   `Cancelled` + `QueuedCancellation` if in flight; run holder ⇒ `cancel_run_inner`), take
   slot, push to `queue`. Busy otherwise ⇒ job status `Pending`, park in
   `concurrency_blocked`, apply queue mode exactly as above (per-job granularity).

### Release

`fn release_concurrency(inner: &mut InnerState, done: &Holder)` — called from every terminal
transition:

- job completion paths (`broker_complete_job` lib.rs:3407-3690 and the AzDO finish path),
- `cancel_run_inner`,
- reaper lease-expiry/timeout failures (lib.rs:87-193).

Behavior: a `Job` holder releases when that job is terminal; `Run`/`JobSet` release when all
member jobs are terminal. On release: pop `pending.pop_front()`:

- `Holder::Run` → move its `held_runs` jobs into `queue`/`pending_jobs` per `needs`, statuses
  `Pending → Queued`, emit events, `message_notify` notify.
- `Holder::Job` → move the matching `concurrency_blocked` entry into `queue`, status
  `Queued`, notify.
- Empty group (no running, no pending) → remove the map entry (no leak).

Note: a pending job re-checks nothing else — `max_parallel`/`needs` were satisfied at
park time and `needs` results cannot regress; `under_max_parallel` **is** re-checked at
release (call it before queueing; if it now fails, return the job to `pending_jobs`).

### Reusable workflows

When an expansion carries caller `concurrency` / callee `embedded_concurrency` (Phase 2.3):
acquire a `Holder::JobSet` over the expanded job ids at the first member's ready-time; both
groups (caller's and embedded) must be free before any member queues; release each when all
members are terminal. Pending behavior identical to job-level with the JobSet as the unit.

### Ordering with existing gates

Gate order for a job: `needs` → `max_parallel` → concurrency. A job never occupies a
concurrency slot before it is otherwise runnable (matches GitHub: FIFO by time it *started
waiting on the group*).

## Phase 4 — aksh-runner cancellation parity with the official runner

Reference behavior (all runner-side; concurrency-agnostic):

- `src/Runner.Listener/Runner.cs:496-511` — poll loop never blocks on job/cancel handling;
  `Runner.cs:732-738` — `JobCancelMessage` routed to `jobDispatcher.Cancel`, fire-and-forget.
- `src/Runner.Listener/JobDispatcher.cs:1282-1305` (`WorkerDispatcher.Cancel`) — cancel token
  fires **immediately**; timeout clamped `max(timeout, 60s)`; hard-kill token scheduled at
  `timeout − 15s`.
- `src/Runner.Worker/Worker.cs:75-82` — worker receives `CancelRequest` over IPC →
  `jobRequestCancellationToken.Cancel()`.
- `src/Runner.Worker/JobRunner.cs:190-235` — job result `Canceled`; `FinalizeJob` +
  completion reporting run in `finally` under a **fresh, non-cancelled** token.
- `src/Runner.Worker/StepsRunner.cs:259-305` — remaining steps evaluated so `always()` /
  post steps still run after cancellation.

Changes in `crates/aksh-runner`:

1. `listener/broker_listener.rs:295-321` — make cancellation **non-blocking** (currently
   `await`s worker exit inline for up to 300 s, freezing the poll loop):
   - compute `timeout = max(parse_timespan_secs(body.timeout).unwrap_or(300), 60)`
     (clamp per `JobDispatcher.cs:1293-1296`); `kill_after = timeout - 15`
     (`JobDispatcher.cs:1298`);
   - send IPC cancel (`job.cancel(timeout)`) and set a new field
     `RunningJob.kill_at: Option<tokio::time::Instant>` = `now + kill_after`;
   - **return to the loop immediately** (no `job.wait()` here);
   - add a branch to the existing `tokio::select!`:
     `_ = tokio::time::sleep_until(kill_at), if active_job kill_at set => { job.kill().await; }`
     — single ownership preserved, no watchdog task;
   - the existing `active_job.wait()` branch keeps doing cleanup (`active_job = None`,
     ephemeral/once exit — move the lines currently at `broker_listener.rs:313-318` there).
2. `worker/job_runner.rs` — on cancellation confirm the completion report sends the
   `Canceled` result (mirror `JobRunner.cs:196`) and that final flush/log upload/completion
   (lines 458-496) run even when `cancel_rx` fired — they are after `run_steps` returns, so
   only verify no early-return path skips them; post/`always()` steps already handled in
   `steps_runner.rs:188`.
3. Busy-runner new-job arrival (`broker_listener.rs:264-267`, `:284-287`): replace
   silent-ignore with the official drain semantics (`JobDispatcher.cs:301-311`): wait up to
   45 s for the active job to finish; if it finishes, dispatch the new job; else log an
   error and drop the message. Before implementing, read the official broker path once more
   to confirm whether the server-side request-status probe (`JobDispatcher.cs:269-296`,
   `GetAgentRequestAsync`) also runs for run-service jobs; if it does, mirror it via the
   equivalent run-service call — 100% match takes precedence over the simplification noted
   in `docs/fidelity-gap.md` §3a.
4. Mid-job step-update flush cadence (closes the audit's live-status gap; needed so a
   concurrency-cancelled job shows accurate step states): in `worker/job_runner.rs`, after
   the `ReportingContext` is built (lines 213-245), spawn a flusher task calling
   `flush_step_updates` every **1000 ms** (matches results-upload dequeue,
   `src/Runner.Common/JobServerQueue.cs:36`); abort it in cleanup before the final drain at
   line 458 (final drain stays — mirrors `JobServerQueue.ShutdownAsync`,
   `JobServerQueue.cs:190-224`).

## Phase 5 — Production profile (remote CI, maximize utilization)

Everything here is **additive on aksh-native surfaces only** (invariant 4): CLI flags,
`/api/v1` endpoints, on-disk state. Runner protocol and workflow semantics are untouched.
Single control-plane node; scale = many runners (Mac fleet / smolvm VMs). No multi-node
control plane — runners are the horizontal axis.

1. **Persistence** — new `crates/aksh-runner-server/src/persist.rs`, `rusqlite` (WAL) at
   `<state-dir>/aksh.sqlite`, enabled by `serve --persist` (default off; local mode stays
   pure in-memory). Write-through on state transitions only (submit, status change, group
   acquire/release, pending queue mutation); tables: `runs`, `jobs`,
   `concurrency_groups(repo, name, running_holder_json)`,
   `concurrency_pending(repo, name, seq, holder_json)`, `held_jobs(run_id, payload_json)`.
   Boot recovery: reload all; jobs that were `InProgress` re-enter reaper supervision and
   fail on lease expiry exactly like a live disconnect (reaper rules at lib.rs:141-159);
   `Pending`/`Queued` state restores verbatim.
2. **Label-indexed ready queues** — replace the linear scan in `take_matching_job`
   (lib.rs:3783-3791) with `ready: BTreeMap<String, VecDeque<QueuedJob>>` keyed by the
   sorted-joined `runs_on` label set. A poll iterates the (few) label-set keys, selects
   entries whose label set ⊆ runner labels, dequeues the globally oldest by enqueue
   timestamp (add `enqueued_at: Instant` to `QueuedJob`). O(#label-sets) per poll instead of
   O(queue).
3. **Work-conserving wakeups** — every enqueue and every concurrency release calls
   `message_notify.notify_waiters()` (not `notify_one`) so all long-polling runners race for
   work; verify current call sites and switch where needed.
4. **Utilization metrics** — `GET /api/v1/metrics` (JSON): `registered_runners`,
   `busy_runners`, `queue_depth`, `pending_by_group` (map), `held_runs`,
   `cancelled_by_concurrency_total`, `group_wait_seconds{p50,p95}` (from
   pending-enter/leave timestamps), `utilization = busy/registered`. Counters live in
   `InnerState`; no external metrics dependency.
5. **Hygiene at scale** — group entries removed when empty (Phase 3 release); pending caps
   enforced (100); reaper already bounds leases (120 s) and job timeout (21600 s).

Head-of-line blocking note: concurrency-blocked work never enters the ready queues
(`held_runs` / `concurrency_blocked`), so a blocked group can never stall unrelated jobs —
this is the core utilization property and is asserted by test in Phase 6.

## Phase 6 — Verification

Unit/integration tests inline in `crates/aksh-runner-server/src/lib.rs`, patterned on
`matrix_max_parallel_and_fail_fast_are_enforced` (lib.rs:6061) and
`cancel_run_delivers_cancellation_message` (lib.rs:8263):

- `workflow_concurrency_serializes_runs_fifo` — submit run A then B, same literal group; B's
  jobs are `pending`; complete A via broker flow; B's jobs become `queued`.
- `workflow_concurrency_cancel_in_progress_cancels_running` — group busy +
  `cancel-in-progress: true`: A's in-flight job produces a `JobCancellation` whose body is
  exactly `{"jobId": "<A's agent_job_id GUID>", "timeout": "00:05:00"}`; B runs.
- `pending_run_replaced_by_newer_submission` — default `queue: single`: A running, B
  pending, C arrives ⇒ B `cancelled` with `reason: "concurrency_cancelled"`, C pending.
- `queue_max_holds_multiple_pending_fifo` — `queue: max`: A running, B/C/D pending in order;
  completions drain B → C → D.
- `queue_max_overflow_cancels_arrival` — with 100 pending, the 101st is cancelled.
- `concurrency_group_names_case_insensitive` — `Prod` vs `prod` collide.
- `job_level_concurrency_gates_single_job` — two jobs in one run sharing a job-level group
  run serially; group expression using `matrix` evaluates per expansion.
- `concurrency_blocked_jobs_do_not_block_unrelated_work` — group-blocked job parked; a
  runner poll still receives an unrelated queued job (utilization property).
- Parser tests in `aksh-gha-parser`: bare-string shorthand; mapping form; raw expression
  preservation; `queue: max` + `cancel-in-progress: true` ⇒ parse error.
- Runner tests in `crates/aksh-runner` (pattern: existing `message_listener.rs` tests):
  `parse_timespan_secs` cases (`"00:05:00"` → 300, `"1.00:00:00"` → 86400, garbage → None);
  cancellation with mismatched `jobId` GUID is ignored; clamp `max(timeout,60)` and
  `kill_at = timeout − 15` computation.

Commands (workspace root):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace --quiet
```

E2E (exercises the new behavior end-to-end, per `AGENTS.md` runner-facing-change rule):

1. `just serve`, register the **official runner** (`sudo ./scripts/e2e-setup.sh` once,
   then `./scripts/e2e-start.sh`).
2. Add `concurrency: { group: dogfood-${{ github.ref }}, cancel-in-progress: true }` to a
   copy of `.github/workflows/dogfood.yml`; submit it twice back-to-back via
   `cargo run -p aksh-runner-client -- submit -W <file>`.
3. Expected observable: first run's job is cancelled **by the official runner** (proves the
   Phase 1 wire fix: GUID + timeout accepted by `JobDispatcher.Cancel`), second run executes
   to completion; `logs/e2e/` shows the cancellation; `/api/v1/runs/<id>` reports run 1
   `cancelled`, run 2 `success`.
4. Repeat with `cancel-in-progress` absent: run 2 stays `pending` until run 1 finishes.
5. Same scenario against the Rust runner (`aksh-runner`) validates Phase 4 (non-blocking
   cancel, kill-at deadline unused on graceful exit).

Conformance checks against **real GitHub** (settles the two underivable behaviors before
ship; use a scratch repo + the runner-watch MITM tooling, `.runner-watch/record-18080.sh`
pattern):

6. Cancel a running workflow on real GitHub with an MITM'd official runner; read the
   captured `JobCancellation` body's `timeout` value. Set the server literal to exactly
   that value (expected `00:05:00`).
7. Push a workflow with `concurrency: { group: "${{ github.event.head_commit.id_missing }}" }`
   (evaluates empty) to real GitHub; record whether the run fails validation or runs without
   concurrency. Implement whichever is observed (default in this plan: reject; see
   Assumption 2 for the flip).

## Assumptions & contingencies

1. **Cancel timeout literal `00:05:00`.** Not derivable from runner sources; 5 min matches
   the prior aksh grace comment. **Settled before ship** by Phase 6 conformance check 6; the
   literal lives in exactly two server body-build sites.
2. **Empty evaluated group ⇒ reject (422 / job failure).** **Settled before ship** by
   Phase 6 conformance check 7. If real GitHub runs without concurrency instead, replace the
   reject branch with a no-op passthrough at the same two evaluation sites; tests
   `workflow_concurrency_*` are unaffected.
3. **`queue` mode applied per-arrival** (an arrival's own `queue:` decides how *it* joins a
   contended group). GitHub docs don't specify mixed-mode groups; this is the least
   surprising reading. Fallback if disproven: latest arrival's mode governs the whole
   group's pending queue.
4. **Busy-runner drain**: default is the 45 s wait; Phase 4 step 3 requires confirming
   whether the official broker path also performs the server-side request-status probe
   (`JobDispatcher.cs:269-296`) and mirroring it if so — compatibility wins over
   simplification.
5. **Production persistence = SQLite WAL, single node.** Chosen for the Mac-fleet target and
   the repo's file-backed-store pattern; runners are the scaling axis. If multi-node control
   plane ever becomes a requirement, the `persist.rs` table schema is the migration surface
   (swap rusqlite for Postgres behind the same functions); nothing else in the plan assumes
   single-node except the in-process `Notify` wakeups.
