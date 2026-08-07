# Phase 1 Investigation for MC-S3-job-gate-bypass

## Code audit (runtime_scheduling.rs:842-857, runs.rs:1075-1094)
- try_enqueue_with_job_concurrency (runtime_scheduling.rs:5) has single call site in runs.rs:1098, guarded by `if needs_empty && under_mp` (runs.rs:1096).
- Jobs with `needs` or that fail initial max-parallel check take the else branch: EnqueuePending + push to inner.pending_jobs (runs.rs:1117-1127).
- promote_ready_jobs (runtime_scheduling.rs:888+) evaluates DependencyDecision and under_max_parallel but for general Run case does `inner.queue.extend(promoted)` (line ~1048) without calling try_enqueue_with_job_concurrency, try_acquire_concurrency, or on_job_enqueued.
- Counterexample trace matches: SubmitRun2 -> DeclareGate -> EnqueuePending -> PromoteDispatchJob bypasses gate.
- Trigger scenario: Workflow job using both `needs:` and job-level `concurrency:` group. Needs resolve → promote dispatches without acquiring gate. Reachable via normal submit_run_inner public API.
- No downstream guard observed in promote path; dispatch to queue is permanent.

## Developer-knowledge search
- git log -S concurrency -- crates/preloop-runner-server/src/runtime_scheduling.rs shows deferred-expansion commit (aafe5b77) introduced pending_jobs path; no commit message or blame cites gate bypass on promote.
- REVIEW.md discusses concurrency correctness but no mention of this specific on_job_enqueued omission or GateBeforeDispatch.
- No TODO/FIXME, no test asserting concurrency with needs+promote path. Code comment in try_enqueue_with_job_concurrency notes model refinement for EnqueuePending+PromoteReadyJob path divergence.

## Known-status / precedent
- Searched git history, REVIEW.md, docs/* for "gate bypass", "promote_ready_jobs", "job concurrency needs", "GateBeforeDispatch", "on_job_enqueued promote": no prior reports of this exact mechanism (gate acquisition skipped on needs/promote path).
- No matching upstream issues or recently merged PRs found for this site/mechanism. Therefore **NEW**.

Proceed to Phase 2 reproduction. (MC-sourced with counterexample → no DROPPED prefilter.)