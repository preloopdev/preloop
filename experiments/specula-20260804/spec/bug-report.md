# Bug Report — preloop (aksh GitHub Actions control plane)

## Summary

- Scenarios tested: 6 (claim lifecycle, job concurrency, deferred expansion, cancel/kill, reporting queue, masking/tokenizer)
- Bugs found: 4
- Configs run: MC_hunt_s1_claim_lifecycle.safety.cfg, MC_hunt_s2_concurrency.safety.cfg, MC_hunt_s3_deferred_expansion.safety.cfg, MC_hunt_s4_cancel_kill.safety.cfg, MC_hunt_s5_reporting_queue.safety.cfg, MC_hunt_s6_masking_tokenizer.safety.cfg, plus repaired-spec reruns (MC_hunt_s2_rerun, MC_hunt_s3_rerun, MC_hunt_s6_rerun, and an isolated NoLeakedRequest config)
- Evidence: TLC counterexamples saved under `spec/output/`; hunt logs `spec/tlc-hunt-*.out`; post-repair reruns `spec/output/MC_hunt_*_rerun.out` / `MC_hunt_s3_NoLeakedRequest_isolated_rerun.out`
- All four findings below were reconfirmed against the repaired spec (lifecycle/concurrency repairs, 2026-08-04); the repairs changed the spec but not the classifications.

## Bug 1: Terminal run leaves concurrency holder_keys populated after promote Skip/Error settlement

- **Scenario**: Scenario 2 (job concurrency)
- **Severity**: High
- **Invariant violated**: TerminalRunReleasesKeys
- **Config**: MC_hunt_s2_concurrency.safety.cfg
- **Counterexample**: 5 states, `spec/output/MC_hunt_s2_TerminalRunReleasesKeys_violation.out`

### Trace Summary

1. `SubmitRun2(run1,{job1})` — a one-job run is submitted.
2. `EnqueuePending(run1,job1)` — the job is parked pending (needs/concurrency path).
3. `ArriveRunFree(run1,g2)` — a concurrency group slot admits the run; `holderKeys[run1]` registers `g2` with a `HolderRun` holder.
4. `SkipJob(run1,job1)` — promote_ready_jobs settles the job as Skipped; the run becomes terminal.
5. Invariant check: the run is terminal, but `holderKeys[run1]` still contains `g2` and the group still holds the dead run — the slot is never released.

### Root Cause

`promote_ready_jobs`' `Skip | Error` arm (crates/aksh-runner-server/src/runtime_scheduling.rs:858-875) inserts the terminal job status, re-summarizes the run, and calls `finalize_run_if_complete` — but never calls `release_concurrency_for_job`. That function (runtime_scheduling.rs:259-315) is the only path that releases group slots and prunes `holder_keys`, and it is called from exactly four sites: `cancel_job_inner` (:226), the expansion-failure path (:1851), and completion handling (distributed_task.rs:618-620). `finalize_run_if_complete` only stamps `completed_at`/`conclusion`. So any run whose last job settles through the promote Skip/Error arm terminates with its concurrency group permanently occupied. Later submissions to the same group wait on a dead holder.

### Affected Code

- `crates/aksh-runner-server/src/runtime_scheduling.rs:858-875`: promote Skip/Error arm missing the release call
- `crates/aksh-runner-server/src/runtime_scheduling.rs:259-315`: `release_concurrency_for_job` — the only slot/key cleanup path

### Recommendation

Call `release_concurrency_for_job(inner, run_id, &job_id)` in the promote `Skip | Error` arm before `finalize_run_if_complete`, mirroring `complete_job_inner` (distributed_task.rs:618).

---

## Bug 2: Submit-time correlation record leaks when expansion removes its placeholder node

- **Scenario**: Scenario 3 (deferred expansion)
- **Severity**: Medium
- **Invariant violated**: NoLeakedRequest
- **Config**: MC_hunt_s3_deferred_expansion.safety.cfg
- **Counterexample**: 5 states, `spec/output/MC_hunt_s3_NoLeakedRequest_violation.out`

### Trace Summary

1. `SubmitRun2(run1,{job1})` — submit creates the request/correlation record for `job1` (state `SQueued`, result `RNone`).
2. `EnqueuePending(run1,job1)` — the expandable node is parked.
3. `DeferExpansion(run1,job1)` — the node is routed to deferred expansion.
4. `ApplyMatrix(run1,job1,{job2})` — the matrix is applied; the placeholder `job1` is removed from `runJobs` and replaced by child `job2`.
5. Invariant check: the original request still references `job1`, which no longer exists in `runJobs[run1]` — an unresolvable record with `result = RNone` forever.

### Root Cause

`apply_expansion`'s Matrix arm removes the placeholder node from `run.jobs` (`run.jobs.remove(&node_id)`, runtime_scheduling.rs:1888-1895: "GitHub shows the fan-out, never the node that produced it"), but nothing settles or removes the submit-time request/correlation record created for that node. The same leak shape applies to gate-skipped jobs (documented as F-4 in the Phase 1 modeling brief).

### Affected Code

- `crates/aksh-runner-server/src/runtime_scheduling.rs:1888-1895`: placeholder removal without request settlement

### Recommendation

When `apply_expansion` replaces a placeholder, settle its correlation record (e.g., mark it resolved/superseded by the expanded children) instead of orphaning it; alternatively retain the placeholder node with a terminal status.

---

## Bug 3: Protocol-crate format builder escapes only quotes, not braces, producing expressions the parser rejects

- **Scenario**: Scenario 6 (masking/format tokenizer)
- **Severity**: Medium
- **Invariant violated**: FormatEscapeClosed
- **Config**: MC_hunt_s6_masking_tokenizer.safety.cfg
- **Counterexample**: 3 states, `spec/output/MC_hunt_s6_FormatEscapeClosed_violation.out`

### Trace Summary

1. `SetEscapeBracesFalse` — the protocol crate's escaping mode (quotes only).
2. `BuildFormat(TRUE,TRUE)` — build a format expression from a literal containing `{` plus one `${{ }}` expression.
3. `formatError` is set: the emitted `format('<literal-with-stray-brace>', arg0)` is invalid.

### Root Cause

The protocol crate escapes only single quotes (`crates/aksh-gha-protocol/src/azdo/job.rs:612`: `literal.replace('\'', "''")`) before emitting `format('<literal>', <args>)`. The parser-side builder escapes quotes AND braces (`crates/aksh-gha-parser/src/job_builder.rs:126-137`: `'{'` → `{{`, `'}'` → `}}`), and the expression evaluator's `format()` (crates/aksh-gha-expressions/src/evaluator.rs:347) enforces that convention: a lone `{` must start a `{N}` placeholder and a lone `}` is rejected. A step literal containing `{` mixed with `${{ }}` (JSON snippets, curl bodies) therefore produces a format string that fails with `InvalidFormat` — diverging from GitHub, where the escaping makes the same workflow run. This breaks the repo's drop-in-workflow guarantee.

### Affected Code

- `crates/aksh-gha-protocol/src/azdo/job.rs:612-613`: quote-only escaping in the format builder
- `crates/aksh-gha-parser/src/job_builder.rs:126-137`: the parser's quote+brace escaping (the convention the evaluator expects)
- `crates/aksh-gha-expressions/src/evaluator.rs:347-390`: `format()` placeholder/brace validation

### Recommendation

Escape `{`/`}` the same way in the protocol crate's format builder (`append_format_literal` parity): `'{'` → `{{`, `'}'` → `}}`.

---

## Bug 4: Job-level concurrency gate is evaluated only at submit; promote dispatches a gated job without re-acquiring the gate (F8)

- **Scenario**: Scenario 3 (deferred expansion) / Scenario 2 (job concurrency)
- **Severity**: High
- **Invariant violated**: GateBeforeDispatch
- **Config**: MC_hunt_s3_deferred_expansion.cfg
- **Counterexample**: 5 states, `spec/output/MC_hunt_s3_GateBeforeDispatch_violation.out`

### Trace Summary

1. `SubmitRun2(run1,{job1})` — a one-job run is submitted; the job carries a declared concurrency gate (`runs.rs:877`, `concurrency_from_plan_fields`).
2. `DeclareGate(run1,job1,g1)` — gate `g1` is attached to the job at submit time, before any evaluation.
3. `EnqueuePending(run1,job1)` — the job is parked in `pending_jobs` (needs-gated).
4. `PromoteDispatchJob(run1,job1)` — once dependency-ready, `promote_ready_jobs` dispatches the job onto the dispatch queue while gate `g1` is still held by a different owner.
5. Invariant check: the job has a gate, sits in the dispatch queue, but `<<run1, job1>> \notin gateHeld` — dispatched without holding its own group slot.

### Root Cause

Job-level concurrency gates are evaluated exactly once — at submit time, inside `try_enqueue_with_job_concurrency` (`runtime_scheduling.rs:5-90`), reachable from the single call site at `runs.rs:1098` and only for needs-empty jobs under max-parallel. A needs-gated job skips that path entirely and lands in `pending_jobs`. When it later becomes dependency-ready, `promote_ready_jobs`' plain Run arm (`runtime_scheduling.rs:842-857`) checks only `dependency_decision`, `under_max_parallel`, and the per-base promotion counter — it never evaluates or acquires the job's concurrency gate, and it cannot: `try_enqueue_with_job_concurrency` takes ownership of the `QueuedJob` and has no call path from promote. On GitHub, a job evaluates its `concurrency:` group when it becomes ready, just before execution. Preloop therefore ignores the gate for every job that parks, letting more concurrent instances of a `concurrency: { group, cancel-in-progress: false }` job run than the group permits. This is the documented F8 fidelity gap, now confirmed by model checking.

### Affected Code

- `crates/aksh-runner-server/src/runtime_scheduling.rs:842-857`: promote Run arm dispatches without gate evaluation
- `crates/aksh-runner-server/src/runtime_scheduling.rs:5-90`: `try_enqueue_with_job_concurrency` — the only gate-acquisition path
- `crates/aksh-runner-server/src/runs.rs:1098-1100`: the sole call site, submit-time only

### Recommendation

In `promote_ready_jobs`' Run arm, route gated jobs through `try_enqueue_with_job_concurrency` (or an equivalent acquire-or-park step) before promotion, so the gate is evaluated when the job becomes ready — matching GitHub's semantics. The spec's `PromoteDispatchJob`/`PromoteReadyJob` were deliberately left un-gated to model this real behavior.

---

## Not Reproduced

| Scenario | Config | States Explored | Result |
|------------|--------|-----------------|--------|
| S1 claim lifecycle | MC_hunt_s1_claim_lifecycle.safety.cfg | 215,668,334 generated (33.8M distinct), bounded run, queue non-empty | No violation |
| S2 CancelledJobNeverDispatched | MC_hunt_s2_concurrency.safety.cfg (isolated) | 59,159,488 generated + 6,303 simulated traces (mean depth 58) | No violation after atomicity repair; original counterexample was a submit-race serialization artifact (EnqueuePending re-adding an already-Cancelled job), guarded since |
| S2 GateBeforeDispatch | MC_hunt_s3_deferred_expansion.cfg (repaired-spec rerun) | 3min 25s, halted at first violation | **Violation — classified as Bug 4 (F8)**, real preloop bug, confirmed against `promote_ready_jobs` Run arm |
| S2 NoTerminalRunHoldsSlot | MC_hunt_s2_concurrency.safety.cfg | run halted at first violation (TerminalRunReleasesKeys) | Not independently verified |
| S3 FanoutCorrelationRegistered, CancelledJobNeverDispatched | MC_hunt_s3_deferred_expansion.safety.cfg | pre-repair run halted at NoLeakedRequest; repaired-spec rerun halted at GateBeforeDispatch | Not independently verified |
| S3 NoLeakedRequest (isolated) | /tmp/s3-noleak.cfg (isolated from s3 cfg, NoLeakedRequest only) | 1s, halted at first violation | **Violation reproduced independently on repaired spec** — Bug 2 reconfirmed (`MC_hunt_s3_NoLeakedRequest_isolated_rerun.out`) |
| S4 cancel/kill | MC_hunt_s4_cancel_kill.safety.cfg | 54,514,782 generated (10.4M distinct), bounded run, queue non-empty | No violation |
| S5 StepTransitionDelivered | MC_hunt_s5_reporting_queue.safety.cfg | 1,502 generated | Violation classified as **spec artifact** (invariant models the `dirty` set implementation detail, not deliverability; real `take_steps_update_body` clears `dirty_keys` before publish and republishes via generation max — server_queue.rs). No code change; invariant withdrawn |
| S6 MaskNeverLeaks | MC_hunt_s6_masking_tokenizer.safety.cfg | 22,422 generated, run halted at first violation (FormatEscapeClosed) | Not independently verified |
| S1 trace replay (Trace.tla) | Trace-replay-s1.cfg | 505 distinct, depth 10 | **Pass** (`Model checking completed. No error has been found`, `tlc-trace-s1-replay5.out`): trace normalized to model constants; replay config switched to `SPECIFICATION TraceSpec` + `CHECK_DEADLOCK FALSE` with `WF_TraceVars(TraceNext)` (Specula reference convention), and `CompleteJobApply` releases the session (`distributed_task.rs:651-654`) so the second `AzdoPollClaim` consumes. All 9 trace events matched. |
| F1 AzDO cancel misdelivery | (code review, Phase 1) | — | Not reproduced by model checking; code-review candidate carried from modeling-brief for Phase 4 confirmation |

## Spec Fixes During Hunting

1. **Type-homogeneous sentinels** — replaced the universal `NONE = "NONE"` sentinel with per-type sentinels so TLC's strict runtime typing stops throwing cross-type equality exceptions beyond depth 6.
2. **Dead-action repairs** — `AckMessage` and `BuildExpansionStart` assigned primed variables and then asserted `UNCHANGED vars` over the same variables; restructured (`BuildExpansionStart` is now a pure `pendingExp` pop, matching real phase-1 `drain_expansions`; the `expanding` reservation is removed only by apply/fail actions).
3. **Release-concurrency fidelity** — `CompleteJobApply`'s non-discard branch and `CancelJob` now model `release_concurrency_for_job` (distributed_task.rs:618-620, runtime_scheduling.rs:226), including pending-holder pruning, terminal-holder release, next-holder promotion, and C-07 `holder_keys` cleanup.
4. **Submit atomicity guard** — `EnqueuePending` gained `~IsTerminal(jobStatus[run][job])`: real submission installs jobs under the global lock, so no external cancel can settle a job between build and pending-enqueue. This eliminated the spurious `CancelledJobNeverDispatched` counterexample (Submit→CancelRun→EnqueuePending→Promote interleaving).
5. **Faithful SkipJob** — an early repair added release semantics to `SkipJob`; it was reverted after verifying the real promote Skip arm does **not** release (runtime_scheduling.rs:858-875). The resulting `TerminalRunReleasesKeys` violation is Bug 1, not a model artifact.
