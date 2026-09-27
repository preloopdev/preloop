//! The unified schema: the table families every `ControlBackend` implements.
//!
//! One schema serves SQLite and Postgres. The DDL below is the SQLite
//! dialect; `postgres` translates the few constructs that differ (`?` → `$N`,
//! `INSERT OR REPLACE` → `ON CONFLICT … DO UPDATE`, autoincrement →
//! `BIGSERIAL`, `BEGIN IMMEDIATE` → `SELECT … FOR UPDATE`). The logical
//! tables, columns and invariants are identical. The shared behavioral
//! suite runs the same commands against both and expects the same results.
//!
//! Design rules
//! - `jobs.status` (the `ExecutionStatus` in the run record) is canonical
//!   workflow truth; `queue_kind` is the derived dispatch copy — never
//!   independently mutable.
//! - Every claim/lease carries a fencing carrier (`expand_generation`,
//!   `lease_until_us` + owner) so a stale worker racing a successor loses
//!   the write.
//! - Payloads a command never inspects (job message, submission, request
//!   blob) are opaque sealed/JSON blobs; columns exist only for what the
//!   backend itself must query (status, queue position, owner, lease).

/// SQLite schema version, recorded in `PRAGMA user_version`.
///
/// Greenfield: there is no migration chain. A database stamped with any
/// other version is refused at open and must be recreated.
pub(crate) const SQLITE_SCHEMA_VERSION: i64 = 1;

/// The SQLite DDL. `IF NOT EXISTS` makes it idempotent across opens;
/// `SQLITE_SCHEMA_VERSION` is stamped into `PRAGMA user_version`.
pub(crate) const SQLITE_DDL: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;

-- ── Runs ─────────────────────────────────────────────────────────────
-- The run record. `jobs` (the status map) is NOT stored here — it is
-- derived from the `jobs` table so a job transition is one row update,
-- not a whole-record rewrite. Everything else is the run's own state.
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY NOT NULL,  -- RunId (uuid string)
    namespace           TEXT NOT NULL DEFAULT 'default',
    status              TEXT NOT NULL,              -- ExecutionStatus
    run_number          INTEGER NOT NULL,
    run_attempt         INTEGER NOT NULL DEFAULT 1,
    run_name            TEXT,
    event               TEXT NOT NULL DEFAULT '',
    workflow_path       TEXT NOT NULL DEFAULT '',
    conclusion          TEXT,
    webhook_delivery_id TEXT,                       -- dedup key for replays
    head_sha            TEXT NOT NULL DEFAULT '',
    workflow_ref        TEXT NOT NULL DEFAULT '',
    push_state_json     TEXT,
    snapshot_timing_json TEXT,
    created_at_us       INTEGER NOT NULL,
    started_at_us       INTEGER,
    completed_at_us     INTEGER,
    archived_at_us      INTEGER
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id, workflow_path)
    WHERE webhook_delivery_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS runs_archive_pending ON runs(completed_at_us, run_id)
    WHERE archived_at_us IS NULL AND completed_at_us IS NOT NULL;
-- The request a run was created from. Only secret values are sealed;
-- everything else is plain JSON so operators can inspect it.
CREATE TABLE IF NOT EXISTS run_submissions (
    run_id              TEXT PRIMARY KEY NOT NULL,
    submission_json     TEXT NOT NULL,              -- WorkflowSubmission minus secrets
    secrets_blob        BLOB NOT NULL,           -- sealed secret name -> value map
    github_json         TEXT NOT NULL,              -- github context at submission
    workspace_snapshot_json TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Per-job workflow facts, one row per job id in the run (including caller
-- and planned jobs that never get a dispatch row). Columns are NULL when
-- the fact does not apply to the job.
CREATE TABLE IF NOT EXISTS run_jobs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    base_id             TEXT,
    display_name        TEXT,
    needs_json          TEXT,
    outputs_json        TEXT,
    check_run_id        INTEGER,
    detail_json         TEXT,                       -- JobDetail (annotations, conclusion)
    detail_position     INTEGER,
    caller_plan_json    TEXT,                       -- deferred reusable caller plan
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Per base-job strategy flags (keyed by the matrix base id).
CREATE TABLE IF NOT EXISTS run_base_jobs (
    run_id              TEXT NOT NULL,
    base_id             TEXT NOT NULL,
    fail_fast           INTEGER,
    continue_on_error   INTEGER,
    PRIMARY KEY (run_id, base_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Reusable-workflow callers and the inner jobs they expanded into.
CREATE TABLE IF NOT EXISTS reusable_calls (
    run_id              TEXT NOT NULL,
    caller_job_id       TEXT NOT NULL,
    metadata_json       TEXT NOT NULL,              -- ReusableCallMetadata
    PRIMARY KEY (run_id, caller_job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- ── Jobs ─────────────────────────────────────────────────────────────
-- One row per (run, logical job). `status` is canonical workflow truth;
-- `queue_kind` is the derived dispatch classification. A job is in exactly
-- one queue collection at a time (ready / pending / blocked / held /
-- expanding / claimed) or none (terminal / placeholder).
CREATE TABLE IF NOT EXISTS jobs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    status              TEXT NOT NULL,              -- ExecutionStatus
    queue_kind          TEXT NOT NULL DEFAULT 'none',
    queue_position      INTEGER,                    -- order within 'ready'
    seq                 INTEGER,                    -- FIFO order for non-ready kinds
    base_id             TEXT NOT NULL DEFAULT '',
    runs_on             TEXT NOT NULL DEFAULT '[]', -- JSON label list
    runner_group        TEXT,
    enqueued_at_us      INTEGER,                    -- ready-queue entry time
    reaper_first_seen_us INTEGER,                   -- starvation clock
    claimed_by          INTEGER,                    -- runner_id holding claim
    claimed_at_us       INTEGER,                    -- when the claim was taken
    expand_generation   INTEGER NOT NULL DEFAULT 0, -- expansion fencing
    created_at_ns       INTEGER,                    -- job creation (ns)
    deps_ready_at_ns    INTEGER,                    -- `needs:` satisfied (ns)
    concurrency_wait_at_ns INTEGER,                 -- first gate wait (ns)
    concurrency_acquired_at_ns INTEGER,             -- gate admitted (ns)
    if_condition        TEXT,
    max_parallel        INTEGER,
    environment_json    TEXT,
    concurrency_json    TEXT,                       -- raw job-level concurrency
    matrix_json         TEXT NOT NULL DEFAULT '{}',
    deferred_matrix     TEXT,                       -- dynamic matrix expression
    reusable_call_json  TEXT,                       -- deferred `uses:` plan
    namespace_id        TEXT NOT NULL DEFAULT 'default',
    priority            INTEGER NOT NULL DEFAULT 0,
    run_order           INTEGER NOT NULL DEFAULT 0,
    job_order           INTEGER NOT NULL DEFAULT 0,
    pool_key            TEXT NOT NULL DEFAULT '',
    not_before_us       INTEGER,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- The ready queue: claim candidates in FIFO / policy order.
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(pool_key, namespace_id, priority DESC,
                                              run_order, job_order, run_id, job_id)
    WHERE queue_kind = 'ready';
CREATE INDEX IF NOT EXISTS jobs_ready_pos ON jobs(queue_position)
    WHERE queue_kind = 'ready';
-- Per-run scans (cancel, fail-fast, promote).
CREATE INDEX IF NOT EXISTS jobs_run ON jobs(run_id, queue_kind);
-- `needs:` edges, one row per dependency in declaration order. The reverse
-- index answers "who waits on X" when X settles.
CREATE TABLE IF NOT EXISTS job_needs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    position            INTEGER NOT NULL,
    needs_job_id        TEXT NOT NULL,
    PRIMARY KEY (run_id, job_id, position),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS job_needs_reverse ON job_needs(run_id, needs_job_id);
-- The runner-bound message and the `if:` evaluation context both embed
-- secrets, so they are the only sealed part of a job. Replayed verbatim,
-- never queried.
CREATE TABLE IF NOT EXISTS job_messages (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    message_blob        BLOB NOT NULL,           -- sealed AgentJobRequestMessage
    condition_context_blob BLOB NOT NULL,        -- sealed expression Context
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

-- Terminal runs move into these tables atomically. Runner-facing payloads
-- and request message blobs contain credentials and are not archived.
CREATE TABLE IF NOT EXISTS job_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         INTEGER NOT NULL,
    run_created_at_us   INTEGER NOT NULL,
    job_id              TEXT NOT NULL,
    status              TEXT NOT NULL,
    base_id             TEXT NOT NULL,
    pool_key            TEXT NOT NULL,
    priority            INTEGER NOT NULL,
    run_order           INTEGER NOT NULL,
    job_order           INTEGER NOT NULL,
    PRIMARY KEY (run_id, run_attempt, job_id)
);
CREATE INDEX IF NOT EXISTS job_history_run ON job_history(run_id, run_attempt);

CREATE TABLE IF NOT EXISTS attempt_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         INTEGER NOT NULL,
    run_created_at_us   INTEGER NOT NULL,
    request_id          INTEGER NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    timeline_id         TEXT NOT NULL,
    result              TEXT,
    owner_runner_id     INTEGER,
    claimed_at_us       INTEGER,
    started_at_us       INTEGER,
    PRIMARY KEY (run_id, run_attempt, request_id)
);
CREATE INDEX IF NOT EXISTS attempt_history_run ON attempt_history(run_id, job_id, request_id);
CREATE INDEX IF NOT EXISTS attempt_history_agent ON attempt_history(agent_job_id);
CREATE TABLE IF NOT EXISTS step_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         INTEGER NOT NULL,
    run_created_at_us   INTEGER NOT NULL,
    agent_job_id        TEXT NOT NULL,
    step_id             TEXT NOT NULL,              -- TaskStep.id (protocol identity)
    position            INTEGER NOT NULL,           -- manifest order
    kind                TEXT NOT NULL,              -- workflow | synthetic
    workflow_index      INTEGER,                    -- what `--step N` indexes
    runner_number       INTEGER,                    -- runner timeline position
    context_name        TEXT,                       -- stable across runs
    name                TEXT NOT NULL,
    conclusion          TEXT NOT NULL,
    started_at_us       INTEGER,
    finished_at_us      INTEGER,
    PRIMARY KEY (agent_job_id, step_id)
);
CREATE INDEX IF NOT EXISTS step_history_run ON step_history(run_id, run_attempt);


-- ── Job requests (execution attempts) ────────────────────────────────
-- One row per dispatch attempt. `result IS NULL` = inflight. The lease
-- (`locked_until_us` + `owner_runner_id`) is the fencing carrier.
CREATE TABLE IF NOT EXISTS job_requests (
    request_id          INTEGER PRIMARY KEY,
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL,              -- uuid
    plan_id             TEXT NOT NULL,
    plan_type           TEXT NOT NULL DEFAULT '',
    timeline_id         TEXT NOT NULL,              -- uuid
    result              TEXT,                       -- NULL = inflight
    locked_until        TEXT NOT NULL DEFAULT '',   -- wire ISO form
    locked_until_us     INTEGER,                    -- comparable form
    claimed_at_us       INTEGER,
    owner_runner_id     INTEGER,
    started_at_us       INTEGER,
    last_renewed_at_us  INTEGER,
    timeout_triggered   INTEGER NOT NULL DEFAULT 0,
    debug_token_issued  INTEGER NOT NULL DEFAULT 0,
    request_blob        BLOB,                       -- AgentJobRequestMessage
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS job_requests_agent ON job_requests(agent_job_id);
CREATE INDEX IF NOT EXISTS job_requests_plan ON job_requests(plan_id);
CREATE INDEX IF NOT EXISTS job_requests_timeline ON job_requests(timeline_id);
CREATE INDEX IF NOT EXISTS job_requests_inflight ON job_requests(run_id, job_id)
    WHERE result IS NULL;

-- Deferred App-token mint requests, one per job request.
CREATE TABLE IF NOT EXISTS github_token_requests (
    request_id          INTEGER PRIMARY KEY,
    request_blob        BLOB NOT NULL,              -- GitHubTokenRequest
    FOREIGN KEY (request_id) REFERENCES job_requests(request_id) ON DELETE CASCADE
);

-- Per-job grants and OIDC context (read by acquire/oidc paths).
CREATE TABLE IF NOT EXISTS id_token_grants (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    granted             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS oidc_job_contexts (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    context_blob        BLOB NOT NULL,              -- OidcJobContext
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- One row per step per execution attempt. A step transition is a one-row
-- upsert; the attempt's request row owns its steps (cascade on delete).
CREATE TABLE IF NOT EXISTS job_steps (
    agent_job_id        TEXT NOT NULL,              -- uuid (execution attempt)
    step_id             TEXT NOT NULL,              -- TaskStep.id (protocol identity)
    position            INTEGER NOT NULL,           -- manifest order
    kind                TEXT NOT NULL,              -- workflow | synthetic
    workflow_index      INTEGER,                    -- what `--step N` indexes
    runner_number       INTEGER,                    -- runner timeline position
    context_name        TEXT,                       -- stable across runs
    name                TEXT NOT NULL,
    conclusion          TEXT NOT NULL,
    started_at_us       INTEGER,
    finished_at_us      INTEGER,
    PRIMARY KEY (agent_job_id, step_id),
    FOREIGN KEY (agent_job_id) REFERENCES job_requests(agent_job_id) ON DELETE CASCADE
);

-- ── Runners and sessions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS runners (
    runner_id           INTEGER PRIMARY KEY,
    name                TEXT NOT NULL,
    labels              TEXT NOT NULL DEFAULT '[]', -- JSON list
    ephemeral           INTEGER NOT NULL DEFAULT 0,
    public_key          TEXT,
    rsa_public_key      BLOB,                       -- AgentRsaPublicKey XML
    runner_group_id     INTEGER,
    runner_group_name   TEXT,
    client_id           TEXT,                       -- dedup on re-register
    pool_proven         INTEGER NOT NULL DEFAULT 0,
    registered_at_us    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS runners_client ON runners(client_id)
    WHERE client_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS runner_sessions (
    session_id          TEXT PRIMARY KEY NOT NULL,
    -- NULL for compatibility sessions (e.g. the implicit `default` session)
    -- that have no registered runner. No FK: the binding is a claimed id
    -- recorded at session-create time; the runner may register later or
    -- never. Runner teardown removes sessions explicitly.
    runner_id           INTEGER,
    protocol            TEXT NOT NULL DEFAULT 'broker', -- broker | azdo | compat
    client_id           TEXT,
    encryption_blob     BLOB,                       -- sealed SessionEncryption
    active_request_id   INTEGER,
    last_seen_at_us     INTEGER,
    -- 1 when the session was created under a verified listen token (the token
    -- named the runner). Only verified sessions count toward the duplicate
    -- live-session conflict; unverified compat sessions never block a runner.
    verified            INTEGER NOT NULL DEFAULT 0,
    created_at_us       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_runner ON runner_sessions(runner_id);
CREATE INDEX IF NOT EXISTS sessions_active ON runner_sessions(active_request_id)
    WHERE active_request_id IS NOT NULL;

-- Undelivered session messages (broker protocol).
CREATE TABLE IF NOT EXISTS broker_messages (
    session_id          TEXT NOT NULL,
    message_id          INTEGER NOT NULL,
    message_blob        BLOB NOT NULL,              -- TaskAgentMessage
    created_at_us       INTEGER NOT NULL,
    PRIMARY KEY (session_id, message_id),
    FOREIGN KEY (session_id) REFERENCES runner_sessions(session_id) ON DELETE CASCADE
);

-- ── Concurrency ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobset_ready (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    PRIMARY KEY (run_id, job_ids),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_concurrency (
    run_id              TEXT PRIMARY KEY NOT NULL,
    concurrency_blob    BLOB NOT NULL,              -- parser::Concurrency
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- ── Concurrency gates as queryable rows ──────────────────────────────
-- Holders and waiters replace the sealed running_holder/pending_holders
-- blobs; queue depth and oldest waiter are plain SQL. holder_keys is
-- derived at load and no longer persisted. jobset_gates replaces
-- gates_blob/acquired_keys the same way.
CREATE TABLE IF NOT EXISTS concurrency_holds (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    holder_kind         TEXT NOT NULL,
    holder_run_id       TEXT NOT NULL,
    holder_job_id       TEXT,
    holder_job_ids      TEXT NOT NULL DEFAULT '[]',
    held_at_us          INTEGER NOT NULL,
    PRIMARY KEY (repo, group_name)
);
CREATE TABLE IF NOT EXISTS concurrency_waits (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    position            INTEGER NOT NULL,
    holder_kind         TEXT NOT NULL,
    holder_run_id       TEXT NOT NULL,
    holder_job_id       TEXT,
    holder_job_ids      TEXT NOT NULL DEFAULT '[]',
    queued_at_us        INTEGER NOT NULL,
    PRIMARY KEY (repo, group_name, position)
);
CREATE TABLE IF NOT EXISTS jobset_gates (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    gate_index          INTEGER NOT NULL,
    gate_repo           TEXT NOT NULL,
    gate_group          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    cancel_in_progress  INTEGER NOT NULL DEFAULT 0,
    queue_mode          TEXT NOT NULL DEFAULT 'single',
    acquired            INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, job_ids, gate_index),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS concurrency_waits_run ON concurrency_waits(holder_run_id);

-- ── Assignments, pool waitlist, cancellations ────────────────────────
CREATE TABLE IF NOT EXISTS job_assignments (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    runner_id           INTEGER,                    -- NULL = released/ownerless
    at_us               INTEGER NOT NULL,
    first_at_us         INTEGER NOT NULL,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS pool_pending (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    at_us               INTEGER NOT NULL,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS cancellation_queue (
    seq                 INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL               -- uuid
);

-- ── Expansion ────────────────────────────────────────────────────────
-- Expansion state lives on the jobs row: queue_kind='expand' with
-- expand_generation=0 is a pending expansion; >0 is claimed/in-progress
-- (the generation is the fencing token). Held runs are queue_kind='held'
-- jobs grouped by run_id. No separate tables needed.

-- ── Durable events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS control_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT,
    event_blob          BLOB NOT NULL,
    created_at_us       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS control_events_run ON control_events(run_id, id);
-- ── Durable webhook inbox and repair state ───────────────────────────
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id         TEXT PRIMARY KEY NOT NULL,
    event               TEXT NOT NULL,
    payload_blob        BLOB NOT NULL,
    received_at_us      INTEGER NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('received','processing','done','failed')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    lease_until_us      INTEGER,
    lease_token         TEXT,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS webhook_deliveries_claim
    ON webhook_deliveries(state, received_at_us);
CREATE TABLE IF NOT EXISTS webhook_watchdog (
    scope                       TEXT PRIMARY KEY NOT NULL,
    cursor_delivered_at_us      INTEGER,
    cursor_delivered_at_guid    TEXT,
    scan_cursor                 TEXT,
    last_poll_at_us             INTEGER,
    last_success_at_us          INTEGER
);
CREATE TABLE IF NOT EXISTS webhook_redeliveries (
    delivery_guid       TEXT PRIMARY KEY NOT NULL,
    github_delivery_id  INTEGER NOT NULL,
    app_id              TEXT NOT NULL,
    reason              TEXT NOT NULL CHECK (reason IN ('remote_failure','phantom_ack')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    first_seen_at_us    INTEGER NOT NULL,
    last_attempt_at_us  INTEGER,
    resolved_at_us      INTEGER,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS webhook_redeliveries_open
    ON webhook_redeliveries(resolved_at_us, first_seen_at_us);

-- ── Counters, run counters, meta ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS counters (
    name                TEXT PRIMARY KEY NOT NULL,
    value               INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workflow_run_counters (
    key                 TEXT PRIMARY KEY NOT NULL,  -- repo+workflow dedup
    value               INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
    key                 TEXT PRIMARY KEY NOT NULL,
    value               BLOB NOT NULL
);

"#;

/// Postgres schema version, recorded in `control.schema_migrations`.
///
/// Greenfield: there is no migration chain. A database stamped with any
/// other version is refused at connect and must be recreated.
pub(crate) const POSTGRES_SCHEMA_VERSION: i64 = 1;

/// The Postgres DDL: the same table families as [`SQLITE_DDL`] in Postgres
/// dialect. `AUTOINCREMENT` → `BIGSERIAL`, `BLOB` → `BYTEA`, `INTEGER` →
/// `BIGINT`, `PRAGMA` dropped, and a `schema_migrations` table replaces
/// `user_version`. `ON CONFLICT` and partial `CREATE INDEX` are valid in
/// both dialects.
///
/// Every table lives in a dedicated `control` schema — the analogue of the
/// separate `control.db` file on SQLite. `store_pg` already owns `runs`,
/// `jobs`, `job_requests`, `runners` and `runner_sessions` in `public` with
/// incompatible columns; `CREATE TABLE IF NOT EXISTS` would silently no-op
/// against them and every control write would then fail. `SET search_path`
/// at the top makes the DDL self-contained and leaves the session resolving
/// unqualified names to `control.*` for every later statement.
pub(crate) const POSTGRES_DDL: &str = r#"
CREATE SCHEMA IF NOT EXISTS control;
SET search_path TO control;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version             BIGINT PRIMARY KEY,
    applied_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Runs ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS runs (
    run_id              TEXT PRIMARY KEY,
    namespace           TEXT NOT NULL DEFAULT 'default',
    status              TEXT NOT NULL,
    run_number          BIGINT NOT NULL,
    run_attempt         BIGINT NOT NULL DEFAULT 1,
    run_name            TEXT,
    event               TEXT NOT NULL DEFAULT '',
    workflow_path       TEXT NOT NULL DEFAULT '',
    conclusion          TEXT,
    webhook_delivery_id TEXT,
    head_sha            TEXT NOT NULL DEFAULT '',
    workflow_ref        TEXT NOT NULL DEFAULT '',
    push_state_json     TEXT,
    snapshot_timing_json TEXT,
    created_at_us       BIGINT NOT NULL,
    started_at_us       BIGINT,
    completed_at_us     BIGINT,
    archived_at_us      BIGINT
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id, workflow_path)
    WHERE webhook_delivery_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS runs_archive_pending ON runs(completed_at_us, run_id)
    WHERE archived_at_us IS NULL AND completed_at_us IS NOT NULL;
-- The request a run was created from. Only secret values are sealed;
-- everything else is plain JSON so operators can inspect it.
CREATE TABLE IF NOT EXISTS run_submissions (
    run_id              TEXT PRIMARY KEY,
    submission_json     TEXT NOT NULL,              -- WorkflowSubmission minus secrets
    secrets_blob        BYTEA NOT NULL,           -- sealed secret name -> value map
    github_json         TEXT NOT NULL,              -- github context at submission
    workspace_snapshot_json TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Per-job workflow facts, one row per job id in the run (including caller
-- and planned jobs that never get a dispatch row). Columns are NULL when
-- the fact does not apply to the job.
CREATE TABLE IF NOT EXISTS run_jobs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    base_id             TEXT,
    display_name        TEXT,
    needs_json          TEXT,
    outputs_json        TEXT,
    check_run_id        BIGINT,
    detail_json         TEXT,                       -- JobDetail (annotations, conclusion)
    detail_position     BIGINT,
    caller_plan_json    TEXT,                       -- deferred reusable caller plan
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Per base-job strategy flags (keyed by the matrix base id).
CREATE TABLE IF NOT EXISTS run_base_jobs (
    run_id              TEXT NOT NULL,
    base_id             TEXT NOT NULL,
    fail_fast           BIGINT,
    continue_on_error   BIGINT,
    PRIMARY KEY (run_id, base_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- Reusable-workflow callers and the inner jobs they expanded into.
CREATE TABLE IF NOT EXISTS reusable_calls (
    run_id              TEXT NOT NULL,
    caller_job_id       TEXT NOT NULL,
    metadata_json       TEXT NOT NULL,              -- ReusableCallMetadata
    PRIMARY KEY (run_id, caller_job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

-- ── Jobs ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    status              TEXT NOT NULL,
    queue_kind          TEXT NOT NULL DEFAULT 'none',
    queue_position      BIGINT,
    seq                 BIGINT,
    base_id             TEXT NOT NULL DEFAULT '',
    runs_on             TEXT NOT NULL DEFAULT '[]',
    runner_group        TEXT,
    enqueued_at_us      BIGINT,
    reaper_first_seen_us BIGINT,
    claimed_by          BIGINT,
    claimed_at_us       BIGINT,
    expand_generation   BIGINT NOT NULL DEFAULT 0,
    created_at_ns       BIGINT,
    deps_ready_at_ns    BIGINT,
    concurrency_wait_at_ns BIGINT,
    concurrency_acquired_at_ns BIGINT,
    if_condition        TEXT,
    max_parallel        BIGINT,
    environment_json    TEXT,
    concurrency_json    TEXT,
    matrix_json         TEXT NOT NULL DEFAULT '{}',
    deferred_matrix     TEXT,
    reusable_call_json  TEXT,
    namespace_id        TEXT NOT NULL DEFAULT 'default',
    priority            SMALLINT NOT NULL DEFAULT 0,
    run_order           BIGINT NOT NULL DEFAULT 0,
    job_order           BIGINT NOT NULL DEFAULT 0,
    pool_key            TEXT NOT NULL DEFAULT '',
    not_before_us       BIGINT,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(pool_key, namespace_id, priority DESC,
                                              run_order, job_order, run_id, job_id)
    WHERE queue_kind = 'ready';
CREATE INDEX IF NOT EXISTS jobs_ready_pos ON jobs(queue_position)
    WHERE queue_kind = 'ready';
CREATE INDEX IF NOT EXISTS jobs_run ON jobs(run_id, queue_kind);
-- `needs:` edges, one row per dependency in declaration order. The reverse
-- index answers "who waits on X" when X settles.
CREATE TABLE IF NOT EXISTS job_needs (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    position            BIGINT NOT NULL,
    needs_job_id        TEXT NOT NULL,
    PRIMARY KEY (run_id, job_id, position),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS job_needs_reverse ON job_needs(run_id, needs_job_id);
-- The runner-bound message and the `if:` evaluation context both embed
-- secrets, so they are the only sealed part of a job. Replayed verbatim,
-- never queried.
CREATE TABLE IF NOT EXISTS job_messages (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    message_blob        BYTEA NOT NULL,           -- sealed AgentJobRequestMessage
    condition_context_blob BYTEA NOT NULL,        -- sealed expression Context
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         BIGINT NOT NULL,
    run_created_at_us   BIGINT NOT NULL,
    job_id              TEXT NOT NULL,
    status              TEXT NOT NULL,
    base_id             TEXT NOT NULL,
    pool_key            TEXT NOT NULL,
    priority            SMALLINT NOT NULL,
    run_order           BIGINT NOT NULL,
    job_order           BIGINT NOT NULL,
    PRIMARY KEY (run_id, run_attempt, job_id, run_created_at_us)
) PARTITION BY RANGE (run_created_at_us);
CREATE TABLE IF NOT EXISTS job_history_default PARTITION OF job_history DEFAULT;
CREATE INDEX IF NOT EXISTS job_history_run ON job_history(run_id, run_attempt, run_created_at_us);

CREATE TABLE IF NOT EXISTS attempt_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         BIGINT NOT NULL,
    run_created_at_us   BIGINT NOT NULL,
    request_id          BIGINT NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    timeline_id         TEXT NOT NULL,
    result              TEXT,
    owner_runner_id     BIGINT,
    claimed_at_us       BIGINT,
    started_at_us       BIGINT,
    PRIMARY KEY (run_id, run_attempt, request_id, run_created_at_us)
) PARTITION BY RANGE (run_created_at_us);
CREATE TABLE IF NOT EXISTS attempt_history_default PARTITION OF attempt_history DEFAULT;
CREATE INDEX IF NOT EXISTS attempt_history_run ON attempt_history(run_id, job_id, request_id, run_created_at_us);
CREATE INDEX IF NOT EXISTS attempt_history_agent ON attempt_history(agent_job_id);
CREATE TABLE IF NOT EXISTS step_history (
    namespace_id        TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    run_attempt         BIGINT NOT NULL,
    run_created_at_us   BIGINT NOT NULL,
    agent_job_id        TEXT NOT NULL,
    step_id             TEXT NOT NULL,
    position            BIGINT NOT NULL,
    kind                TEXT NOT NULL,
    workflow_index      BIGINT,
    runner_number       BIGINT,
    context_name        TEXT,
    name                TEXT NOT NULL,
    conclusion          TEXT NOT NULL,
    started_at_us       BIGINT,
    finished_at_us      BIGINT,
    PRIMARY KEY (agent_job_id, step_id, run_created_at_us)
) PARTITION BY RANGE (run_created_at_us);
CREATE TABLE IF NOT EXISTS step_history_default PARTITION OF step_history DEFAULT;
CREATE INDEX IF NOT EXISTS step_history_run ON step_history(run_id, run_attempt, run_created_at_us);


-- ── Job requests ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS job_requests (
    request_id          BIGINT PRIMARY KEY,
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL,
    plan_id             TEXT NOT NULL,
    plan_type           TEXT NOT NULL DEFAULT '',
    timeline_id         TEXT NOT NULL,
    result              TEXT,
    locked_until        TEXT NOT NULL DEFAULT '',
    locked_until_us     BIGINT,
    claimed_at_us       BIGINT,
    owner_runner_id     BIGINT,
    started_at_us       BIGINT,
    last_renewed_at_us  BIGINT,
    timeout_triggered   BIGINT NOT NULL DEFAULT 0,
    debug_token_issued  BIGINT NOT NULL DEFAULT 0,
    request_blob        BYTEA,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS job_requests_agent ON job_requests(agent_job_id);
CREATE INDEX IF NOT EXISTS job_requests_plan ON job_requests(plan_id);
CREATE INDEX IF NOT EXISTS job_requests_timeline ON job_requests(timeline_id);
CREATE INDEX IF NOT EXISTS job_requests_inflight ON job_requests(run_id, job_id)
    WHERE result IS NULL;

CREATE TABLE IF NOT EXISTS github_token_requests (
    request_id          BIGINT PRIMARY KEY,
    request_blob        BYTEA NOT NULL,
    FOREIGN KEY (request_id) REFERENCES job_requests(request_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS id_token_grants (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    granted             BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS oidc_job_contexts (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    context_blob        BYTEA NOT NULL,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS job_steps (
    agent_job_id        TEXT NOT NULL,
    step_id             TEXT NOT NULL,
    position            BIGINT NOT NULL,
    kind                TEXT NOT NULL,
    workflow_index      BIGINT,
    runner_number       BIGINT,
    context_name        TEXT,
    name                TEXT NOT NULL,
    conclusion          TEXT NOT NULL,
    started_at_us       BIGINT,
    finished_at_us      BIGINT,
    PRIMARY KEY (agent_job_id, step_id),
    FOREIGN KEY (agent_job_id) REFERENCES job_requests(agent_job_id) ON DELETE CASCADE
);

-- ── Runners and sessions ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS runners (
    runner_id           BIGINT PRIMARY KEY,
    name                TEXT NOT NULL,
    labels              TEXT NOT NULL DEFAULT '[]',
    ephemeral           BIGINT NOT NULL DEFAULT 0,
    public_key          TEXT,
    rsa_public_key      TEXT,
    runner_group_id     BIGINT,
    runner_group_name   TEXT,
    client_id           TEXT,
    pool_proven         BIGINT NOT NULL DEFAULT 0,
    registered_at_us    BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS runner_sessions (
    session_id          TEXT PRIMARY KEY,
    -- NULL for compatibility sessions (e.g. the implicit `default` session)
    -- that have no registered runner. No FK: the binding is a claimed id
    -- recorded at session-create time; the runner may register later or
    -- never. Runner teardown removes sessions explicitly.
    runner_id           BIGINT,
    protocol            TEXT NOT NULL DEFAULT 'broker',
    client_id           TEXT,
    encryption_blob     BYTEA,
    active_request_id   BIGINT,
    last_seen_at_us     BIGINT,
    -- 1 when created under a verified listen token; only verified sessions
    -- count toward the duplicate live-session conflict.
    verified            BIGINT NOT NULL DEFAULT 0,
    created_at_us       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_runner ON runner_sessions(runner_id);
CREATE INDEX IF NOT EXISTS sessions_active ON runner_sessions(active_request_id)
    WHERE active_request_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS broker_messages (
    session_id          TEXT NOT NULL,
    message_id          BIGINT NOT NULL,
    message_blob        BYTEA NOT NULL,
    created_at_us       BIGINT NOT NULL,
    PRIMARY KEY (session_id, message_id),
    FOREIGN KEY (session_id) REFERENCES runner_sessions(session_id) ON DELETE CASCADE
);

-- ── Concurrency ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobset_ready (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    PRIMARY KEY (run_id, job_ids),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_concurrency (
    run_id              TEXT PRIMARY KEY,
    concurrency_blob    BYTEA NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS concurrency_holds (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    holder_kind         TEXT NOT NULL,
    holder_run_id       TEXT NOT NULL,
    holder_job_id       TEXT,
    holder_job_ids      TEXT NOT NULL DEFAULT '[]',
    held_at_us          BIGINT NOT NULL,
    PRIMARY KEY (repo, group_name)
);
CREATE TABLE IF NOT EXISTS concurrency_waits (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    position            BIGINT NOT NULL,
    holder_kind         TEXT NOT NULL,
    holder_run_id       TEXT NOT NULL,
    holder_job_id       TEXT,
    holder_job_ids      TEXT NOT NULL DEFAULT '[]',
    queued_at_us        BIGINT NOT NULL,
    PRIMARY KEY (repo, group_name, position)
);
CREATE TABLE IF NOT EXISTS jobset_gates (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    gate_index          BIGINT NOT NULL,
    gate_repo           TEXT NOT NULL,
    gate_group          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    cancel_in_progress  BIGINT NOT NULL DEFAULT 0,
    queue_mode          TEXT NOT NULL DEFAULT 'single',
    acquired            BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, job_ids, gate_index),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS concurrency_waits_run ON concurrency_waits(holder_run_id);

-- ── Assignments, pool waitlist, cancellations ────────────────────────
CREATE TABLE IF NOT EXISTS job_assignments (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    runner_id           BIGINT,
    at_us               BIGINT NOT NULL,
    first_at_us         BIGINT NOT NULL,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS pool_pending (
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    at_us               BIGINT NOT NULL,
    PRIMARY KEY (run_id, job_id)
);
CREATE TABLE IF NOT EXISTS cancellation_queue (
    seq                 BIGSERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL,
    job_id              TEXT NOT NULL,
    agent_job_id        TEXT NOT NULL
);

-- ── Durable events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS control_events (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT,
    event_blob          BYTEA NOT NULL,
    created_at_us       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS control_events_run ON control_events(run_id, id);

-- ── Durable webhook inbox and repair state ───────────────────────────
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id         TEXT PRIMARY KEY,
    event               TEXT NOT NULL,
    payload_blob        BYTEA NOT NULL,
    received_at_us      BIGINT NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('received','processing','done','failed')),
    attempts            BIGINT NOT NULL DEFAULT 0,
    lease_until_us      BIGINT,
    lease_token         TEXT,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS webhook_deliveries_claim
    ON webhook_deliveries(state, received_at_us);
CREATE TABLE IF NOT EXISTS webhook_watchdog (
    scope                       TEXT PRIMARY KEY,
    cursor_delivered_at_us      BIGINT,
    cursor_delivered_at_guid    TEXT,
    scan_cursor                 TEXT,
    last_poll_at_us             BIGINT,
    last_success_at_us          BIGINT
);
CREATE TABLE IF NOT EXISTS webhook_redeliveries (
    delivery_guid       TEXT PRIMARY KEY,
    github_delivery_id  BIGINT NOT NULL,
    app_id              TEXT NOT NULL,
    reason              TEXT NOT NULL CHECK (reason IN ('remote_failure','phantom_ack')),
    attempts            BIGINT NOT NULL DEFAULT 0,
    first_seen_at_us    BIGINT NOT NULL,
    last_attempt_at_us  BIGINT,
    resolved_at_us      BIGINT,
    last_error          TEXT
);
CREATE INDEX IF NOT EXISTS webhook_redeliveries_open
    ON webhook_redeliveries(resolved_at_us, first_seen_at_us);

-- ── Counters, run counters, meta ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS counters (
    name                TEXT PRIMARY KEY,
    value               BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workflow_run_counters (
    key                 TEXT PRIMARY KEY,
    value               BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
    key                 TEXT PRIMARY KEY,
    value               BYTEA NOT NULL
);

"#;
