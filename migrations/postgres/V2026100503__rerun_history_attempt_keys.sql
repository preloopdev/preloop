-- Rerun history keys (PostgreSQL).
--
-- History rows are per attempt now, so run/job history needs `run_attempt` in
-- its primary key: rerun 2 of a run must coexist with rerun 1 instead of
-- colliding on `(run_id, created_at)` / `(run_id, job_id, run_created_at)`.
-- `run_history` already carries `run_attempt` (the archive row records which
-- attempt it belongs to); only its primary key changes. `job_history` gains
-- the column: every pre-change row is attempt-1 history by definition (the
-- old code only ever wrote one snapshot per run), so it is added with a
-- temporary `DEFAULT 1` that backfills existing rows, the primary keys are
-- replaced, and the default is dropped so the column matches the fresh
-- schema. PostgreSQL DDL is transactional, so a failure rolls the whole step
-- back.
SET search_path = control;

ALTER TABLE control.job_history ADD COLUMN run_attempt integer NOT NULL DEFAULT 1;
ALTER TABLE control.run_history DROP CONSTRAINT run_history_pkey;
ALTER TABLE control.run_history ADD PRIMARY KEY (run_id, created_at, run_attempt);
ALTER TABLE control.job_history DROP CONSTRAINT job_history_pkey;
ALTER TABLE control.job_history ADD PRIMARY KEY (run_id, job_id, run_created_at, run_attempt);
ALTER TABLE control.job_history ALTER COLUMN run_attempt DROP DEFAULT;
