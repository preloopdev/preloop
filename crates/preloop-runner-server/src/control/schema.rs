//! The unified schema: the table families every `ControlBackend` implements.
//!
//! One schema serves SQLite and Postgres. The DDL below is the SQLite
//! dialect; `postgres` translates the few constructs that differ (`?` → `$N`,
//! `INSERT OR REPLACE` → `ON CONFLICT … DO UPDATE`, autoincrement →
//! `BIGSERIAL`, `BEGIN IMMEDIATE` → `SELECT … FOR UPDATE`). The logical
//! tables, columns and invariants are identical — the shared behavioral
//! suite runs the same commands against both and expects the same results.
//!
//! Design rules (from `docs/internal/arch/02-control-model.md`):
//! - `jobs.status` (the `ExecutionStatus` in the run record) is canonical
//!   workflow truth; `queue_kind` is the derived dispatch copy — never
//!   independently mutable.
//! - Every claim/lease carries a fencing carrier (`claim_generation`,
//!   `expand_generation`, `lease_until_us` + owner) so a stale worker racing
//!   a successor loses the write.
//! - Payloads a command never inspects (job message, submission, request
//!   blob) are opaque sealed/JSON blobs; columns exist only for what the
//!   backend itself must query (status, queue position, owner, lease).

/// SQLite schema version. Bumped on any DDL change; `PRAGMA user_version`
/// records what a database was created/migrated to.
pub(crate) const SQLITE_SCHEMA_VERSION: i64 = 1;

/// The SQLite DDL, applied as one migration. `IF NOT EXISTS` makes it
/// idempotent for a fresh database; the migration runner records
/// `SQLITE_SCHEMA_VERSION` in `PRAGMA user_version`.
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
    run_id              TEXT PRIMARY KEY,           -- RunId (uuid string)
    namespace           TEXT NOT NULL DEFAULT 'default',
    status              TEXT NOT NULL,              -- ExecutionStatus
    run_number          INTEGER NOT NULL,
    run_attempt         INTEGER NOT NULL DEFAULT 1,
    run_name            TEXT,
    event               TEXT NOT NULL DEFAULT '',
    workflow_path       TEXT NOT NULL DEFAULT '',
    conclusion          TEXT,
    webhook_delivery_id TEXT,                       -- dedup key for replays
    -- The full RunRecord minus the derived `jobs` map, as sealed JSON.
    -- Contains submission, github, head_sha, workflow_ref, snapshot,
    -- caller_plans, reusable_calls, job_* maps, timestamps, push_state.
    record_blob         BLOB NOT NULL,
    created_at_us       INTEGER NOT NULL,
    started_at_us       INTEGER,
    completed_at_us     INTEGER
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id)
    WHERE webhook_delivery_id IS NOT NULL;

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
    claim_generation    INTEGER NOT NULL DEFAULT 0, -- fencing
    expand_generation   INTEGER NOT NULL DEFAULT 0, -- expansion fencing
    payload_blob        BLOB,                       -- QueuedJob (sealed JSON)
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
-- The ready queue: claim candidates in FIFO order.
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue_position)
    WHERE queue_kind = 'ready';
-- Per-run scans (cancel, fail-fast, promote).
CREATE INDEX IF NOT EXISTS jobs_run ON jobs(run_id, queue_kind);

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

-- Step manifests per attempt, with a revision for conditional updates.
CREATE TABLE IF NOT EXISTS job_steps (
    agent_job_id        TEXT PRIMARY KEY,           -- uuid
    steps_blob          BLOB NOT NULL,              -- Vec<StepRecord>
    revision            INTEGER NOT NULL DEFAULT 0
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
    session_id          TEXT PRIMARY KEY,
    runner_id           INTEGER NOT NULL,
    protocol            TEXT NOT NULL DEFAULT 'broker', -- broker | azdo
    client_id           TEXT,
    encryption_blob     BLOB,                       -- sealed SessionEncryption
    active_request_id   INTEGER,
    last_seen_at_us     INTEGER,
    created_at_us       INTEGER NOT NULL,
    FOREIGN KEY (runner_id) REFERENCES runners(runner_id) ON DELETE CASCADE
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
CREATE TABLE IF NOT EXISTS concurrency_groups (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    running_holder      BLOB,                       -- concurrency::Holder JSON
    pending_holders     BLOB NOT NULL DEFAULT '[]', -- Vec<Holder> JSON
    PRIMARY KEY (repo, group_name)
);
CREATE TABLE IF NOT EXISTS holder_keys (
    run_id              TEXT NOT NULL,
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    PRIMARY KEY (run_id, repo, group_name),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS jobset_admissions (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,              -- sorted JobId list JSON
    gates_blob          BLOB NOT NULL,              -- Vec<JobSetGate>
    acquired_keys       BLOB NOT NULL DEFAULT '[]', -- Vec<(repo,group)>
    PRIMARY KEY (run_id, job_ids),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS jobset_ready (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    PRIMARY KEY (run_id, job_ids),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_concurrency (
    run_id              TEXT PRIMARY KEY,
    concurrency_blob    BLOB NOT NULL,              -- parser::Concurrency
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

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

-- ── Counters, run counters, meta ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS counters (
    name                TEXT PRIMARY KEY,
    value               INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workflow_run_counters (
    key                 TEXT PRIMARY KEY,           -- repo+workflow dedup
    value               INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
    key                 TEXT PRIMARY KEY,
    value               BLOB NOT NULL
);

-- ── Transactional outbox ─────────────────────────────────────────────
-- Durable effects committed with a state transition. Workers lease rows
-- per sink and acknowledge by generation; a crash replays, never loses.
CREATE TABLE IF NOT EXISTS outbox (
    effect_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace           TEXT NOT NULL DEFAULT 'default',
    kind                TEXT NOT NULL,              -- check_run|event|token|…
    payload             BLOB NOT NULL,
    lease_generation    INTEGER NOT NULL DEFAULT 0,
    leased_by           TEXT,
    lease_until_us      INTEGER,
    attempts            INTEGER NOT NULL DEFAULT 0,
    created_at_us       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_lease ON outbox(kind, lease_until_us);
"#;

/// Postgres schema version. Bumped on any DDL change; the
/// `control.schema_migrations` table records what a database was migrated
/// to (Postgres has no `PRAGMA user_version`).
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
    record_blob         BYTEA NOT NULL,
    created_at_us       BIGINT NOT NULL,
    started_at_us       BIGINT,
    completed_at_us     BIGINT
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id)
    WHERE webhook_delivery_id IS NOT NULL;

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
    claim_generation    BIGINT NOT NULL DEFAULT 0,
    expand_generation   BIGINT NOT NULL DEFAULT 0,
    payload_blob        BYTEA,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(queue_position)
    WHERE queue_kind = 'ready';
CREATE INDEX IF NOT EXISTS jobs_run ON jobs(run_id, queue_kind);

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
    agent_job_id        TEXT PRIMARY KEY,
    steps_blob          BYTEA NOT NULL,
    revision            BIGINT NOT NULL DEFAULT 0
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
CREATE UNIQUE INDEX IF NOT EXISTS runners_client ON runners(client_id)
    WHERE client_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS runner_sessions (
    session_id          TEXT PRIMARY KEY,
    runner_id           BIGINT NOT NULL,
    protocol            TEXT NOT NULL DEFAULT 'broker',
    client_id           TEXT,
    encryption_blob     BYTEA,
    active_request_id   BIGINT,
    last_seen_at_us     BIGINT,
    created_at_us       BIGINT NOT NULL,
    FOREIGN KEY (runner_id) REFERENCES runners(runner_id) ON DELETE CASCADE
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
CREATE TABLE IF NOT EXISTS concurrency_groups (
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    display_name        TEXT NOT NULL DEFAULT '',
    running_holder      BYTEA,
    pending_holders     BYTEA NOT NULL,
    PRIMARY KEY (repo, group_name)
);
CREATE TABLE IF NOT EXISTS holder_keys (
    run_id              TEXT NOT NULL,
    repo                TEXT NOT NULL,
    group_name          TEXT NOT NULL,
    PRIMARY KEY (run_id, repo, group_name),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS jobset_admissions (
    run_id              TEXT NOT NULL,
    job_ids             TEXT NOT NULL,
    gates_blob          BYTEA NOT NULL,
    acquired_keys       BYTEA NOT NULL,
    PRIMARY KEY (run_id, job_ids),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
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

-- ── Transactional outbox ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    effect_id           BIGSERIAL PRIMARY KEY,
    namespace           TEXT NOT NULL DEFAULT 'default',
    kind                TEXT NOT NULL,
    payload             BYTEA NOT NULL,
    lease_generation    BIGINT NOT NULL DEFAULT 0,
    leased_by           TEXT,
    lease_until_us      BIGINT,
    attempts            BIGINT NOT NULL DEFAULT 0,
    created_at_us       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_lease ON outbox(kind, lease_until_us);
"#;
