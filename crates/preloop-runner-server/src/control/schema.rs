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

/// SQLite schema version. Bumped on any DDL change; `PRAGMA user_version`
/// records what a database was created/migrated to.
///
/// - 2: `runner_sessions.runner_id` nullable for compatibility sessions.
/// - 3: `runner_sessions.runner_id` foreign key dropped. The binding is a
///   *claimed* id recorded at session-create time; the runner may register
///   later or never. Runner teardown removes sessions explicitly.
/// - 4: `runs_delivery` widened to `(webhook_delivery_id, workflow_path)` —
///   one delivery legitimately fans out to several workflow files, so the
///   dedup key is the pair, not the delivery alone.
/// - 5: `runner_sessions.verified` marks sessions created under a verified
///   listen token; only they count toward the duplicate live-session conflict.
/// - 6: `jobs.claim_generation` dropped — job-claim fencing lives on
///   `job_requests.locked_until_us` + `owner_runner_id`, never this column.
/// - 7: `outbox` dropped — declared but never read or written.
/// - 8: SQLite text primary keys made explicitly `NOT NULL`.
/// - 9: durable control events moved into the authoritative backend.
pub(crate) const SQLITE_SCHEMA_VERSION: i64 = 9;

/// SQLite migrations as `(version, sql)` steps, applied in order to any
/// database whose `user_version` predates them — the same append-only model
/// as `store.rs::MIGRATIONS`. Each step is one `execute_batch`; the runner
/// holds `foreign_keys` off for the whole pass (the v3 table rebuild needs
/// it) and re-enables it after. The idempotent [`SQLITE_DDL`] then runs to
/// create fresh tables/indexes, and `user_version` is stamped to
/// [`SQLITE_SCHEMA_VERSION`].
///
/// Steps are append-only and idempotent: a step only runs when the database
/// is older than its version, so `IF NOT EXISTS` guards are unnecessary
/// (unlike the DDL, which runs every open).
pub(crate) const SQLITE_MIGRATIONS: &[(i64, &str)] = &[
    // v3: `runner_sessions.runner_id` had an FK into `runners` and (in v1)
    // was NOT NULL. Neither is correct — the session→runner binding is a
    // claimed id recorded at session-create time, before the runner may
    // exist. SQLite cannot ALTER COLUMN/drop constraints, so rebuild the
    // table preserving rows. (`broker_messages` holds an FK into
    // `runner_sessions`; the runner disables FK enforcement for the pass.)
    (
        3,
        "CREATE TABLE runner_sessions_v3 (
            session_id          TEXT PRIMARY KEY,
            runner_id           INTEGER,
            protocol            TEXT NOT NULL DEFAULT 'broker',
            client_id           TEXT,
            encryption_blob     BLOB,
            active_request_id   INTEGER,
            last_seen_at_us     INTEGER,
            created_at_us       INTEGER NOT NULL
        );
        INSERT INTO runner_sessions_v3
            SELECT session_id, runner_id, protocol, client_id, encryption_blob,
                   active_request_id, last_seen_at_us, created_at_us
            FROM runner_sessions;
        DROP TABLE runner_sessions;
        ALTER TABLE runner_sessions_v3 RENAME TO runner_sessions;",
    ),
    // v4: `runs_delivery` widened to `(webhook_delivery_id, workflow_path)` —
    // one delivery fans out to several workflow files. Drop the single-column
    // index; the DDL recreates the composite one.
    (4, "DROP INDEX IF EXISTS runs_delivery;"),
    // v5: `runner_sessions.verified` marks sessions created under a verified
    // listen token. Existing rows predate the flag; they keep the default 0
    // (unverified) — a stale unverified row can only fail to conflict, never
    // wrongly block a runner.
    (
        5,
        "ALTER TABLE runner_sessions ADD COLUMN verified INTEGER NOT NULL DEFAULT 0;",
    ),
    // v6: `jobs.claim_generation` dropped — job-claim fencing lives on
    // `job_requests.locked_until_us` + `owner_runner_id`; the column was
    // declared but never read or written.
    (6, "ALTER TABLE jobs DROP COLUMN claim_generation;"),
    // v7: `outbox` dropped — the transactional-outbox table was declared but
    // never read or written (no producer or consumer exists).
    (7, "DROP TABLE IF EXISTS outbox;"),
    // v8: SQLite preserves a historical quirk for rowid tables: `TEXT
    // PRIMARY KEY` does not imply `NOT NULL`. Rebuild the affected tables so
    // the SQLite schema enforces the same identity invariant as Postgres.
    //
    // The migration runner disables foreign keys for the transaction. Build
    // each replacement under a temporary name, drop the old table, then
    // rename the replacement so child FK declarations keep targeting the
    // stable table name. The idempotent DDL recreates dropped indexes.
    (
        8,
        r#"
        CREATE TABLE runs_v8 (
            run_id              TEXT PRIMARY KEY NOT NULL,
            namespace           TEXT NOT NULL DEFAULT 'default',
            status              TEXT NOT NULL,
            run_number          INTEGER NOT NULL,
            run_attempt         INTEGER NOT NULL DEFAULT 1,
            run_name            TEXT,
            event               TEXT NOT NULL DEFAULT '',
            workflow_path       TEXT NOT NULL DEFAULT '',
            conclusion          TEXT,
            webhook_delivery_id TEXT,
            record_blob         BLOB NOT NULL,
            created_at_us       INTEGER NOT NULL,
            started_at_us       INTEGER,
            completed_at_us     INTEGER
        );
        INSERT INTO runs_v8 (
            run_id, namespace, status, run_number, run_attempt, run_name,
            event, workflow_path, conclusion, webhook_delivery_id, record_blob,
            created_at_us, started_at_us, completed_at_us
        )
        SELECT
            run_id, namespace, status, run_number, run_attempt, run_name,
            event, workflow_path, conclusion, webhook_delivery_id, record_blob,
            created_at_us, started_at_us, completed_at_us
        FROM runs;
        DROP TABLE runs;
        ALTER TABLE runs_v8 RENAME TO runs;

        CREATE TABLE job_steps_v8 (
            agent_job_id TEXT PRIMARY KEY NOT NULL,
            steps_blob   BLOB NOT NULL,
            revision     INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO job_steps_v8 (agent_job_id, steps_blob, revision)
        SELECT agent_job_id, steps_blob, revision FROM job_steps;
        DROP TABLE job_steps;
        ALTER TABLE job_steps_v8 RENAME TO job_steps;

        CREATE TABLE runner_sessions_v8 (
            session_id       TEXT PRIMARY KEY NOT NULL,
            runner_id        INTEGER,
            protocol         TEXT NOT NULL DEFAULT 'broker',
            client_id        TEXT,
            encryption_blob  BLOB,
            active_request_id INTEGER,
            last_seen_at_us  INTEGER,
            verified         INTEGER NOT NULL DEFAULT 0,
            created_at_us    INTEGER NOT NULL
        );
        INSERT INTO runner_sessions_v8 (
            session_id, runner_id, protocol, client_id, encryption_blob,
            active_request_id, last_seen_at_us, verified, created_at_us
        )
        SELECT
            session_id, runner_id, protocol, client_id, encryption_blob,
            active_request_id, last_seen_at_us, verified, created_at_us
        FROM runner_sessions;
        DROP TABLE runner_sessions;
        ALTER TABLE runner_sessions_v8 RENAME TO runner_sessions;

        CREATE TABLE run_concurrency_v8 (
            run_id          TEXT PRIMARY KEY NOT NULL,
            concurrency_blob BLOB NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );
        INSERT INTO run_concurrency_v8 (run_id, concurrency_blob)
        SELECT run_id, concurrency_blob FROM run_concurrency;
        DROP TABLE run_concurrency;
        ALTER TABLE run_concurrency_v8 RENAME TO run_concurrency;

        CREATE TABLE counters_v8 (
            name  TEXT PRIMARY KEY NOT NULL,
            value INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO counters_v8 (name, value)
        SELECT name, value FROM counters;
        DROP TABLE counters;
        ALTER TABLE counters_v8 RENAME TO counters;

        CREATE TABLE workflow_run_counters_v8 (
            key   TEXT PRIMARY KEY NOT NULL,
            value INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO workflow_run_counters_v8 (key, value)
        SELECT key, value FROM workflow_run_counters;
        DROP TABLE workflow_run_counters;
        ALTER TABLE workflow_run_counters_v8 RENAME TO workflow_run_counters;

        CREATE TABLE meta_v8 (
            key   TEXT PRIMARY KEY NOT NULL,
            value BLOB NOT NULL
        );
        INSERT INTO meta_v8 (key, value)
        SELECT key, value FROM meta;
        DROP TABLE meta;
        ALTER TABLE meta_v8 RENAME TO meta;
        "#,
    ),
];

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
    -- The full RunRecord minus the derived `jobs` map, as sealed JSON.
    -- Contains submission, github, head_sha, workflow_ref, snapshot,
    -- caller_plans, reusable_calls, job_* maps, timestamps, push_state.
    record_blob         BLOB NOT NULL,
    created_at_us       INTEGER NOT NULL,
    started_at_us       INTEGER,
    completed_at_us     INTEGER
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(status);
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id, workflow_path)
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
    agent_job_id        TEXT PRIMARY KEY NOT NULL,  -- uuid
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
    run_id              TEXT PRIMARY KEY NOT NULL,
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

-- ── Durable events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS control_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              TEXT,
    event_blob          BLOB NOT NULL,
    created_at_us       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS control_events_run ON control_events(run_id, id);

-- ── Live log recovery buffer ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS log_files (
    log_key             TEXT PRIMARY KEY NOT NULL,
    byte_count          INTEGER NOT NULL DEFAULT 0,
    line_count          INTEGER NOT NULL DEFAULT 0,
    updated_at_us       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS log_chunks (
    log_key             TEXT NOT NULL,
    chunk_index         INTEGER NOT NULL,
    payload             BLOB NOT NULL,
    written_at_us       INTEGER NOT NULL,
    PRIMARY KEY (log_key, chunk_index),
    FOREIGN KEY (log_key) REFERENCES log_files(log_key) ON DELETE CASCADE
);

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

/// Postgres schema version. Bumped on any DDL change; the
/// `control.schema_migrations` table records what a database was migrated
/// to (Postgres has no `PRAGMA user_version`).
///
/// - 2: `runner_sessions.runner_id` nullable for compatibility sessions.
/// - 3: `runner_sessions.runner_id` foreign key dropped (claimed binding,
///   not a referential constraint; see the SQLite history note).
/// - 4: `runs_delivery` widened to `(webhook_delivery_id, workflow_path)` —
///   one delivery legitimately fans out to several workflow files, so the
///   dedup key is the pair, not the delivery alone.
/// - 5: `runner_sessions.verified` marks sessions created under a verified
///   listen token; only they count toward the duplicate live-session conflict.
/// - 7: `outbox` dropped — declared but never read or written.
/// - 8: durable control events moved into the authoritative backend.
pub(crate) const POSTGRES_SCHEMA_VERSION: i64 = 8;

/// Postgres migrations as `(version, sql)` steps, applied in order to any
/// database whose `schema_migrations` max predates them — the same
/// append-only model as [`SQLITE_MIGRATIONS`]. Postgres drops constraints in
/// place, so no table rebuild is needed; every step is a plain
/// `batch_execute`. The idempotent [`POSTGRES_DDL`] then runs to create
/// fresh tables/indexes, and the version is recorded in
/// `control.schema_migrations`.
pub(crate) const POSTGRES_MIGRATIONS: &[(i64, &str)] = &[
    // v2: `runner_sessions.runner_id` nullable for compatibility sessions.
    (
        2,
        "ALTER TABLE control.runner_sessions \
         ALTER COLUMN runner_id DROP NOT NULL",
    ),
    // v3: `runner_sessions.runner_id` foreign key dropped (claimed binding,
    // not a referential constraint; see the SQLite history note).
    (
        3,
        "ALTER TABLE control.runner_sessions \
         DROP CONSTRAINT IF EXISTS runner_sessions_runner_id_fkey",
    ),
    // v4: `runs_delivery` widened to `(webhook_delivery_id, workflow_path)` —
    // one delivery fans out to several workflow files. Drop the single-column
    // index; the DDL recreates the composite one.
    (4, "DROP INDEX IF EXISTS control.runs_delivery"),
    // v5: `runner_sessions.verified` marks sessions created under a verified
    // listen token. Existing rows keep the default 0.
    (
        5,
        "ALTER TABLE control.runner_sessions \
         ADD COLUMN IF NOT EXISTS verified BIGINT NOT NULL DEFAULT 0",
    ),
    // v6: `jobs.claim_generation` dropped — job-claim fencing lives on
    // `job_requests.locked_until_us` + `owner_runner_id`; the column was
    // declared but never read or written.
    (
        6,
        "ALTER TABLE control.jobs DROP COLUMN IF EXISTS claim_generation",
    ),
    // v7: `outbox` dropped — the transactional-outbox table was declared but
    // never read or written (no producer or consumer exists).
    (7, "DROP TABLE IF EXISTS control.outbox"),
];

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
CREATE UNIQUE INDEX IF NOT EXISTS runs_delivery ON runs(webhook_delivery_id, workflow_path)
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

-- ── Durable events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS control_events (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              TEXT,
    event_blob          BYTEA NOT NULL,
    created_at_us       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS control_events_run ON control_events(run_id, id);

-- ── Live log recovery buffer ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS log_files (
    log_key             TEXT PRIMARY KEY,
    byte_count          BIGINT NOT NULL DEFAULT 0,
    line_count          BIGINT NOT NULL DEFAULT 0,
    updated_at_us       BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS log_chunks (
    log_key             TEXT NOT NULL,
    chunk_index         BIGINT NOT NULL,
    payload             BYTEA NOT NULL,
    written_at_us       BIGINT NOT NULL,
    PRIMARY KEY (log_key, chunk_index),
    FOREIGN KEY (log_key) REFERENCES log_files(log_key) ON DELETE CASCADE
);

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
