# Phase 1 Investigation for MC-S5-step-transition-loss

## Code audit (server_queue.rs:178, reporting.rs:19)
- take_steps_update_body (lines 179-190): if steps_generation != published_generation, builds steps from dirty_keys, **clears dirty_keys**, returns (body, current_gen). 
- mark_steps_published only called on HTTP success (reporting.rs:90).
- On failure (lines 91-99, WorkerPublishFail), dirty stays cleared, gen mismatch persists but next take_steps_update_body yields empty steps vec (dirty= {}).
- Call chain: worker reporting loop -> flush_step_updates -> take... -> HTTP client (results.update_workflow_steps or AzDO).
- Reachability: normal job step completion queues terminal update (queue_update increments gen and dirties), flush at step boundary or periodic. Real API: job execution with network flake on last report.
- Trigger scenario: Terminal step completes (STerminal status queued) -> take&POST fails -> no mark, subsequent take sends empty body -> terminal StepTransition lost. No re-dirty on fail.
- Safeguards: none for retry of steps on publish fail (only warn non-fatal); has_pending() would still see gen mismatch but body empty.

## Developer-knowledge search
- git blame/log on server_queue.rs:190 (clear) and reporting.rs:80 (published=): commits mention "delta step updates", "send cumulative", "non-fatal" for HTTP fails. No comment acknowledging loss of terminal on fail.
- No TODO/FIXME for reporting queue retry. Tests cover happy path only (queue_and_take_steps_update tests mark on success).
- No issues/PRs found for "step transition" loss or "dirty_keys clear on failed POST" in git history (searched "dirty", "published_generation", "flush_step_updates").

## Known-status / precedent
- No upstream issue, PR, or prior Specula entry describes exact mechanism (clear on take + no re-dirty on publish fail for terminal gen). Searched git log, closed PRs via `git log --grep=step --merges`. Thus **NEW**.

Proceed to Phase 2 reproduction.