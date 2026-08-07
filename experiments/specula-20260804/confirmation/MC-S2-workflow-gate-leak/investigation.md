# Phase 1 Investigation for MC-S2-workflow-gate-leak

## Code Audit
Cited locations:
- runtime_scheduling.rs:862-879 (promote_ready_jobs Skip/Error arm)
- runtime_scheduling.rs:151 (only caller of release_concurrency_for_run is cancel_run_inner)
- runtime_scheduling.rs:2027-2041 (finalize_run_if_complete only sets timestamps, no release)

Call chain: submit_run_inner or complete_job_inner → promote_ready_jobs (for dependency decision on pending jobs).

Trigger scenario: Submit workflow with workflow-level concurrency group. A job evaluates to Skip (e.g. unmet needs or if: false) or Error. The Skip/Error arm in promote_ready_jobs (line 993) sets terminal status, calls finalize_run_if_complete (which marks run terminal), but does NOT call release_concurrency_for_run. A second submission to same group sees the Holder::Run still held and parks in pending (no cancel-in-progress).

Reachability: Yes, via normal workflow submission with conditional jobs or needs. No caller guard prevents the skip path from reaching terminal without release. Safeguard note: cancel_run_inner does release, but skip path bypasses it.

## Developer-knowledge search
No TODO/FIXME or comments in runtime_scheduling.rs near the Skip arm or finalize_run_if_complete mentioning the missing release for terminal-skip paths. Git log on the file shows only refactor commit. No tests in scheduling_tests.rs specifically assert release after SkipJob/EvalFailJob for Run holder. Property tests in concurrency_properties.rs test some terminal cases but not this exact skip path for Run holder.

## Known-status / precedent
Performed git log and grep for "TerminalRunReleasesKeys", "skip", "release_concurrency_for_run", "promote_ready_jobs". No prior reports or fixes mentioning this exact leak on terminal-by-skip for workflow Run holder. No matching issues or PRs found in history for this mechanism. Therefore Novelty: NEW.

Proceed to Phase 2 reproduction. No drop (this is MC-sourced with counterexample).