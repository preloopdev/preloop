-- Rerun history keys (SQLite).
--
-- History rows are per attempt now, so run/job history needs `run_attempt`
-- in its primary key: rerun 2 of a run must coexist with rerun 1 instead of
-- colliding on `(run_id, created_at)` / `(run_id, job_id, run_created_at)`.
-- Every pre-change row is attempt-1 history by definition (the old code only
-- ever wrote one snapshot per run), so the rebuild backfills `run_attempt = 1`
-- and preserves every other column verbatim.
--
-- SQLite cannot alter a primary key in place, so both tables are rebuilt
-- (create/copy/drop/rename) and the `run_history` indexes are recreated.
-- Additive and data-preserving; refinery runs it in a transaction, so a
-- failure rolls the whole rebuild back.
-- migrate:up is implicit: refinery executes the file.

CREATE TABLE run_history_migrating (
    run_id                  TEXT NOT NULL,
    namespace_id            TEXT NOT NULL,
    repository              TEXT NOT NULL,
    workflow_path           TEXT NOT NULL,
    run_number              INTEGER NOT NULL,
    run_attempt             INTEGER NOT NULL,
    run_name                TEXT,
    event                   TEXT NOT NULL,
    ref                     TEXT NOT NULL,
    ref_type                TEXT NOT NULL,
    head_ref                TEXT,
    base_ref                TEXT,
    head_sha                TEXT NOT NULL,
    conclusion              TEXT,
    submission              TEXT NOT NULL,
    record_details          TEXT NOT NULL DEFAULT '{}',
    fork_approval_pending   INTEGER NOT NULL DEFAULT 0,
    fork_approval_requested_at INTEGER,
    fork_approval_approved_at  INTEGER,
    fork_approval_note      TEXT,
    reports_check_runs      INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL,
    started_at              INTEGER,
    completed_at            INTEGER,
    PRIMARY KEY (run_id, created_at, run_attempt)
);
INSERT INTO run_history_migrating (
    run_id, namespace_id, repository, workflow_path, run_number,
    run_attempt, run_name, event, ref, ref_type, head_ref, base_ref,
    head_sha, conclusion, submission, record_details,
    fork_approval_pending, fork_approval_requested_at,
    fork_approval_approved_at, fork_approval_note, reports_check_runs,
    created_at, started_at, completed_at)
SELECT run_id, namespace_id, repository, workflow_path, run_number,
    1, run_name, event, ref, ref_type, head_ref, base_ref,
    head_sha, conclusion, submission, record_details,
    fork_approval_pending, fork_approval_requested_at,
    fork_approval_approved_at, fork_approval_note, reports_check_runs,
    created_at, started_at, completed_at
FROM run_history;
DROP TABLE run_history;
ALTER TABLE run_history_migrating RENAME TO run_history;
CREATE INDEX run_history_namespace ON run_history(namespace_id, created_at DESC);
CREATE INDEX run_history_repo_ref ON run_history(namespace_id, repository, ref, created_at DESC);

CREATE TABLE job_history_migrating (
    run_id                  TEXT NOT NULL,
    run_created_at          INTEGER NOT NULL,
    run_attempt             INTEGER NOT NULL,
    namespace_id            TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    kind                    TEXT NOT NULL,
    parent_job_id           TEXT,
    base_id                 TEXT NOT NULL,
    display_name            TEXT NOT NULL,
    status                  TEXT NOT NULL,
    pool_key                TEXT NOT NULL,
    outputs                 TEXT,
    annotations             TEXT,
    check_run_id            INTEGER,
    created_at              INTEGER NOT NULL,
    deps_ready_at           INTEGER,
    started_at              INTEGER,
    completed_at            INTEGER,
    PRIMARY KEY (run_id, job_id, run_created_at, run_attempt)
);
INSERT INTO job_history_migrating (
    run_id, run_created_at, run_attempt, namespace_id, job_id, kind,
    parent_job_id, base_id, display_name, status, pool_key, outputs,
    annotations, check_run_id, created_at, deps_ready_at, started_at,
    completed_at)
SELECT run_id, run_created_at, 1, namespace_id, job_id, kind,
    parent_job_id, base_id, display_name, status, pool_key, outputs,
    annotations, check_run_id, created_at, deps_ready_at, started_at,
    completed_at
FROM job_history;
DROP TABLE job_history;
ALTER TABLE job_history_migrating RENAME TO job_history;
