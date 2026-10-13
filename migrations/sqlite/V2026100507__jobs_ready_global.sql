-- The ready queue is read in ONE global order: priority DESC, run_order,
-- job_order, then run_id, job_id (a total order, so OFFSET paging is stable).
-- This partial index carries exactly that key, so the claim, the front gauge
-- and the reaper's ready scans stay index reads instead of sorting the whole
-- ready set. `jobs_ready` (pool_key first) stays for the per-pool predicates.
-- Additive: no column changes and nothing is rewritten.
CREATE INDEX jobs_ready_global ON jobs(priority DESC, run_order, job_order, run_id, job_id)
    WHERE queue_state = 'ready';
