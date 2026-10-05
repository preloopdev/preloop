-- Preloop control plane — SQLite translation of docs/control-schema.sql (v1).
--
-- Mechanical translation of the agreed Postgres schema; the design rules in
-- its header are binding here too. Type mapping:
--   uuid / text                 -> TEXT
--   timestamptz                 -> INTEGER (microseconds since the Unix epoch)
--   jsonb                       -> TEXT (JSON)
--   bytea                       -> BLOB
--   boolean                     -> INTEGER 0/1
--   GENERATED ALWAYS AS IDENTITY-> INTEGER PRIMARY KEY AUTOINCREMENT
--                                  (AUTOINCREMENT: ids are never reused)
-- Dropped: partitioning, GIN, fillfactor, xid8 (the outbox is ordered by
-- event_id alone: SQLite has one writer, so commit order = id order).
-- One writer (`BEGIN IMMEDIATE`) replaces FOR UPDATE / SKIP LOCKED.
--
-- Plus `schema_meta` (the contract's one allowed addition): schema_version
-- and the cluster key fingerprint.

-- ── Tenancy ──────────────────────────────────────────────────────────
CREATE TABLE namespaces (
    namespace_id            TEXT PRIMARY KEY,
    state                   TEXT NOT NULL DEFAULT 'active' CHECK (state IN
                                ('active','suspended','draining','deleted')),
    -- Platform-owned: cell fencing and config push bookkeeping; the engine
    -- neither reads nor writes these two.
    cell_generation         INTEGER NOT NULL DEFAULT 1,
    config_version          INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    updated_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);

CREATE TABLE namespace_limits (
    namespace_id            TEXT PRIMARY KEY REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    max_queued_jobs         INTEGER,
    max_running_jobs        INTEGER,
    submit_rate_per_minute  INTEGER,
    max_jobs_per_run        INTEGER,
    -- Platform-owned: written by the hosted platform; the engine does not
    -- enforce these three yet.
    max_job_timeout_minutes INTEGER,
    priority_tier           INTEGER NOT NULL DEFAULT 0,
    run_history_retention_days INTEGER
);

CREATE TABLE namespace_pool_limits (
    namespace_id            TEXT NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    pool_key                TEXT NOT NULL,
    max_running_jobs        INTEGER NOT NULL,
    PRIMARY KEY (namespace_id, pool_key)
);

-- Platform-owned: the hosted platform writes per-tenant admission policy
-- here; the engine does not read it yet.
CREATE TABLE namespace_policies (
    namespace_id            TEXT PRIMARY KEY REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    fork_pr_policy          TEXT NOT NULL DEFAULT 'untrusted' CHECK (fork_pr_policy IN
                                ('untrusted','require_approval','deny')),
    allowed_actions         TEXT,
    max_token_permissions   TEXT,
    oidc_enabled            INTEGER NOT NULL DEFAULT 1,
    allowed_oidc_audiences  TEXT,
    self_hosted_runners_allowed INTEGER NOT NULL DEFAULT 1,
    debug_sessions_allowed  INTEGER NOT NULL DEFAULT 1,
    local_execution_allowed INTEGER NOT NULL DEFAULT 0,
    local_secrets_policy    TEXT NOT NULL DEFAULT 'none' CHECK (local_secrets_policy IN
                                ('none','allowlist','all')),
    local_secrets_allowlist TEXT
);

-- ── Runs ─────────────────────────────────────────────────────────────
CREATE TABLE runs (
    run_id                  TEXT PRIMARY KEY,
    namespace_id            TEXT NOT NULL REFERENCES namespaces(namespace_id),
    repository              TEXT NOT NULL,
    workflow_path           TEXT NOT NULL,
    run_number              INTEGER NOT NULL,
    run_attempt             INTEGER NOT NULL DEFAULT 1,
    run_name                TEXT,
    event                   TEXT NOT NULL,
    ref                     TEXT NOT NULL,
    ref_type                TEXT NOT NULL CHECK (ref_type IN ('branch','tag','pull_request','other')),
    head_ref                TEXT,
    base_ref                TEXT,
    head_sha                TEXT NOT NULL,
    workflow_ref            TEXT NOT NULL,
    status                  TEXT NOT NULL CHECK (status IN
                                ('queued','in_progress','completed')),
    conclusion              TEXT CHECK (conclusion IN
                                ('success','failure','cancelled','skipped','timed_out')),
    webhook_delivery_id     TEXT,
    origin                  TEXT NOT NULL CHECK (origin IN
                                ('webhook','cli','api','schedule','rerun')),
    actor                   TEXT,
    tree_digest             TEXT,
    concurrency_group       TEXT,
    concurrency_cancel_in_progress INTEGER NOT NULL DEFAULT 0,
    version                 INTEGER NOT NULL DEFAULT 0,
    -- Fork-PR policy: the run is held at scheduler admission until the
    -- operator approves it; a hold past the window fails closed (reaper).
    -- Real columns so the expiry sweep filters in SQL.
    fork_approval_pending   INTEGER NOT NULL DEFAULT 0,
    fork_approval_requested_at INTEGER,
    fork_approval_approved_at  INTEGER,
    fork_approval_note      TEXT,
    -- Intake reported GitHub check runs for this run (late check-run mint).
    reports_check_runs      INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    started_at              INTEGER,
    completed_at            INTEGER
);
CREATE UNIQUE INDEX runs_number ON runs(namespace_id, repository, workflow_path, run_number, run_attempt);
CREATE UNIQUE INDEX runs_delivery ON runs(webhook_delivery_id, workflow_path)
    WHERE webhook_delivery_id IS NOT NULL;
CREATE INDEX runs_namespace_recent ON runs(namespace_id, created_at DESC);
CREATE INDEX runs_repo_ref ON runs(namespace_id, repository, ref, created_at DESC);
CREATE INDEX runs_archivable ON runs(completed_at) WHERE status = 'completed';
CREATE INDEX runs_fork_approval_sweep ON runs(fork_approval_requested_at) WHERE fork_approval_pending = 1;

-- `version` counts status/conclusion changes of the run (see `jobs_version`).
CREATE TRIGGER runs_version AFTER UPDATE OF status, conclusion ON runs
    FOR EACH ROW WHEN OLD.status IS NOT NEW.status OR OLD.conclusion IS NOT NEW.conclusion
BEGIN
    UPDATE runs SET version = version + 1 WHERE run_id = NEW.run_id;
END;

CREATE TABLE workflow_run_numbers (
    namespace_id            TEXT NOT NULL REFERENCES namespaces(namespace_id),
    repository              TEXT NOT NULL,
    workflow_path           TEXT NOT NULL,
    last_run_number         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, repository, workflow_path)
);

CREATE TABLE run_submissions (
    run_id                  TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    submission              TEXT NOT NULL,
    github_context          TEXT NOT NULL,
    workspace_snapshot      TEXT,
    snapshot_timing         TEXT,
    -- Record-level per-job maps the agreed schema has no table for
    -- (old backend's run_jobs): job_base_ids, job_names, job_needs,
    -- job_check_run_ids, caller_plans, jobs_list, reusable_calls,
    -- fail_fast/continue_on_error. Jobs without a `jobs` row live here.
    record_details          TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE run_push_states (
    run_id                  TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    status                  TEXT NOT NULL CHECK (status IN ('pending','synced','blocked')),
    error                   TEXT,
    pr_number               INTEGER,
    effective_sha           TEXT,
    updated_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX run_push_states_effective_sha ON run_push_states(effective_sha)
    WHERE effective_sha IS NOT NULL;

-- ── Jobs (hot state) ─────────────────────────────────────────────────
CREATE TABLE jobs (
    run_id                  TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    job_id                  TEXT NOT NULL,
    namespace_id            TEXT NOT NULL,
    kind                    TEXT NOT NULL CHECK (kind IN
                                ('job','matrix_parent','matrix_leg','reusable_caller')),
    parent_job_id           TEXT,
    base_id                 TEXT NOT NULL,
    status                  TEXT NOT NULL CHECK (status IN
                                ('pending','queued','in_progress','success','failure',
                                 'cancelled','skipped','timed_out')),
    queue_state             TEXT NOT NULL CHECK (queue_state IN
                                ('none','blocked','held','ready','claimed',
                                 'pending_expansion','expanding')),
    remaining_needs         INTEGER NOT NULL DEFAULT 0,
    pool_key                TEXT NOT NULL DEFAULT '',
    runs_on                 TEXT NOT NULL DEFAULT '[]',
    runner_group            TEXT,
    priority                INTEGER NOT NULL DEFAULT 0,
    run_order               INTEGER NOT NULL DEFAULT 0,
    job_order               INTEGER NOT NULL DEFAULT 0,
    enqueued_at             INTEGER,
    claimed_by_runner_id    INTEGER,
    claimed_at              INTEGER,
    expand_generation       INTEGER NOT NULL DEFAULT 0,
    version                 INTEGER NOT NULL DEFAULT 0,
    outputs                 TEXT,
    annotations             TEXT,
    check_run_id            INTEGER,
    -- GitHub deployment id for jobs with `environment:` (created when the
    -- run reports checks; deployment statuses update on gate decisions and
    -- job completion). `NULL` for unreported or environment-less jobs.
    deployment_id           INTEGER,
    -- The job's `environment.url`, evaluated by the runner after its steps
    -- and reported in the completion (`completejob` `environmentUrl`). The
    -- server posts it as the deployment status's `environment_url`; `NULL`
    -- until a completion reports one (or for environment-less jobs).
    environment_url         TEXT,
    -- Environment protection gate state (`EnvironmentGateState` JSON): armed
    -- at scheduler admission, updated on approval, cleared when satisfied.
    -- Fail-closed reload: a lost stamp re-arms the gate, never the reverse.
    environment_gate        TEXT,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    deps_ready_at           INTEGER,
    concurrency_wait_at     INTEGER,
    concurrency_acquired_at INTEGER,
    started_at              INTEGER,
    completed_at            INTEGER, deployment_id INTEGER, environment_url TEXT,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, parent_job_id) REFERENCES jobs(run_id, job_id)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
-- Claim order: `SELECT .. WHERE queue_state = 'ready' ORDER BY pool_key,
-- priority DESC, run_order, job_order`. The key columns are exactly the
-- ORDER BY (pg's index carries the same key), so the ready front is read in
-- order instead of sorting the whole ready set on every poll.
CREATE INDEX jobs_ready ON jobs(pool_key, priority DESC, run_order, job_order)
    WHERE queue_state = 'ready';
CREATE INDEX jobs_pending_expansion ON jobs(enqueued_at) WHERE queue_state = 'pending_expansion';
CREATE INDEX jobs_run_active ON jobs(run_id, queue_state) WHERE queue_state <> 'none';
CREATE INDEX jobs_run_base ON jobs(run_id, base_id);
CREATE INDEX jobs_namespace_running ON jobs(namespace_id, pool_key) WHERE queue_state = 'claimed';
CREATE INDEX jobs_namespace_queued ON jobs(namespace_id)
    WHERE queue_state IN ('blocked','held','ready','pending_expansion');

-- `version` counts status changes of the job (see the pg schema's
-- `jobs_version`). SQLite cannot rewrite NEW in a BEFORE trigger, so an AFTER
-- trigger bumps the row; recursive triggers are off, so it does not re-fire.
CREATE TRIGGER jobs_version AFTER UPDATE OF status ON jobs
    FOR EACH ROW WHEN OLD.status IS NOT NEW.status
BEGIN
    UPDATE jobs SET version = version + 1
    WHERE run_id = NEW.run_id AND job_id = NEW.job_id;
END;

-- ── Job spec (immutable, written once at submit/expansion) ───────────
CREATE TABLE job_specs (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    display_name            TEXT NOT NULL,
    display_order           INTEGER NOT NULL,
    if_condition            TEXT,
    matrix                  TEXT NOT NULL DEFAULT '{}',
    deferred_matrix         TEXT,
    max_parallel            INTEGER,
    environment             TEXT,
    concurrency             TEXT,
    reusable_call           TEXT,
    fail_fast               INTEGER,
    continue_on_error       INTEGER,
    id_token_granted        INTEGER NOT NULL DEFAULT 0,
    oidc_environment        TEXT,
    oidc_job_workflow_ref   TEXT,
    oidc_job_workflow_sha   TEXT,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

CREATE TABLE job_needs (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    needs_job_id            TEXT NOT NULL,
    position                INTEGER NOT NULL,
    PRIMARY KEY (run_id, job_id, needs_job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, needs_job_id) REFERENCES jobs(run_id, job_id)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX job_needs_reverse ON job_needs(run_id, needs_job_id);

CREATE TABLE job_messages (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    message_template        TEXT NOT NULL,
    secret_names            TEXT NOT NULL DEFAULT '[]',
    condition_context       TEXT NOT NULL,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

-- ── Execution attempts ───────────────────────────────────────────────
CREATE TABLE job_requests (
    request_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    namespace_id            TEXT NOT NULL,
    agent_job_id            TEXT NOT NULL UNIQUE,
    -- shared cross-attempt correlate: the same timeline_id can appear on
    -- several requests (one per retry/run); NOT UNIQUE, lookup picks newest.
    timeline_id             TEXT NOT NULL,
    runner_id               INTEGER,
    session_id              TEXT,
    result                  TEXT CHECK (result IN
                                ('success','failure','cancelled','skipped','timed_out')),
    timeout_triggered       INTEGER NOT NULL DEFAULT 0,
    debug_token_issued      INTEGER NOT NULL DEFAULT 0,
    claimed_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    started_at              INTEGER,
    finished_at             INTEGER,
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX job_requests_inflight ON job_requests(run_id, job_id) WHERE result IS NULL;
CREATE INDEX job_requests_session ON job_requests(session_id) WHERE result IS NULL;
-- Latest-attempt lookups (`WHERE run_id = ? AND job_id = ? ORDER BY
-- request_id DESC LIMIT 1`) and the jobs -> job_requests cascade: the partial
-- inflight index cannot serve settled attempts. `request_id` is the rowid;
-- naming it keeps the key identical to pg's index.
CREATE INDEX job_requests_attempts ON job_requests(run_id, job_id, request_id DESC);

CREATE TABLE job_leases (
    request_id              INTEGER PRIMARY KEY REFERENCES job_requests(request_id) ON DELETE CASCADE,
    runner_id               INTEGER NOT NULL,
    expires_at              INTEGER NOT NULL,
    renewed_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX job_leases_expiry ON job_leases(expires_at);

CREATE TABLE github_token_requests (
    request_id              INTEGER PRIMARY KEY REFERENCES job_requests(request_id) ON DELETE CASCADE,
    repository              TEXT NOT NULL,
    permissions             TEXT NOT NULL,
    declared                INTEGER NOT NULL,
    untrusted               INTEGER NOT NULL
);

CREATE TABLE job_steps (
    agent_job_id            TEXT NOT NULL REFERENCES job_requests(agent_job_id) ON DELETE CASCADE,
    step_id                 TEXT NOT NULL,
    position                INTEGER NOT NULL,
    kind                    TEXT NOT NULL CHECK (kind IN ('workflow','synthetic')),
    workflow_index          INTEGER,
    runner_number           INTEGER,
    context_name            TEXT,
    name                    TEXT NOT NULL,
    conclusion              TEXT NOT NULL,
    started_at              INTEGER,
    finished_at             INTEGER,
    PRIMARY KEY (agent_job_id, step_id)
);

CREATE TABLE timelines (
    -- job_requests.timeline_id is not unique (cross-attempt correlate), so
    -- this PK cannot FK to it; lifecycle is owned by the request that mints
    -- it and pruned with its run.
    timeline_id             TEXT PRIMARY KEY,
    change_id               INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE timeline_records (
    timeline_id             TEXT NOT NULL REFERENCES timelines(timeline_id) ON DELETE CASCADE,
    record_id               TEXT NOT NULL,
    change_id               INTEGER NOT NULL,
    record                  TEXT NOT NULL,
    PRIMARY KEY (timeline_id, record_id)
);
-- The timeline goes with the last request that referenced it: the FK pg
-- declares (a timeline belongs to one attempt) cannot be expressed on a
-- shared column, and the run archive deletes `job_requests` rows through
-- several cascade levels, so no Rust-side delete can cover every path.
CREATE TRIGGER job_requests_timeline_cascade
AFTER DELETE ON job_requests
WHEN NOT EXISTS (SELECT 1 FROM job_requests WHERE timeline_id = OLD.timeline_id)
BEGIN
    DELETE FROM timelines WHERE timeline_id = OLD.timeline_id;
END;

-- Contract addition (decision round 1, Q3): per-plan log ids. `log_key` is
-- '{plan_id}/{log_id}'; UNIQUE (plan_id, log_id) arbitrates allocation.
CREATE TABLE log_files (
    log_key                 TEXT PRIMARY KEY,
    run_id                  TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    plan_id                 TEXT NOT NULL REFERENCES job_requests(agent_job_id) ON DELETE CASCADE,
    log_id                  INTEGER NOT NULL CHECK (log_id > 0),
    updated_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    UNIQUE (plan_id, log_id)
);
CREATE INDEX log_files_run ON log_files(run_id);

-- ── Runners and sessions ─────────────────────────────────────────────
CREATE TABLE runners (
    runner_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace_id            TEXT NOT NULL REFERENCES namespaces(namespace_id),
    name                    TEXT NOT NULL,
    labels                  TEXT NOT NULL DEFAULT '[]',
    ephemeral               INTEGER NOT NULL DEFAULT 0,
    runner_group_id         INTEGER,
    runner_group_name       TEXT,
    client_id               TEXT UNIQUE,
    public_key              TEXT,
    rsa_public_key          BLOB,
    pool_proven             INTEGER NOT NULL DEFAULT 0,
    registered_at           INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    last_seen_at            INTEGER
);

CREATE TABLE runner_sessions (
    session_id              TEXT PRIMARY KEY,
    -- Declared agent.id on unverified creates can name a runner that has not
    -- registered yet (legacy order); no FK — session_owner LEFT JOINs runners
    -- so dangling ids read as unknown capabilities until registration lands.
    runner_id               INTEGER,
    protocol                TEXT NOT NULL CHECK (protocol IN ('broker','azdo')),
    client_id               TEXT,
    verified                INTEGER NOT NULL DEFAULT 0,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    last_seen_at            INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX runner_sessions_runner ON runner_sessions(runner_id);
CREATE INDEX runner_sessions_liveness ON runner_sessions(last_seen_at);

CREATE TABLE session_messages (
    -- Ids start above 1e6 (seeded at open via sqlite_sequence): broker job
    -- refs carry request_id as messageId; a cancel in the same range would
    -- collide in the runner's in-memory dedup and be silently dropped.
    message_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id              TEXT NOT NULL REFERENCES runner_sessions(session_id) ON DELETE CASCADE,
    message_type            TEXT NOT NULL,
    request_id              INTEGER REFERENCES job_requests(request_id) ON DELETE CASCADE,
    body                    TEXT,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX session_messages_session ON session_messages(session_id, message_id);

-- ── Dispatch side queues ─────────────────────────────────────────────
CREATE TABLE job_cancellations (
    cancellation_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id              INTEGER NOT NULL REFERENCES job_requests(request_id) ON DELETE CASCADE,
    reason                  TEXT,
    requested_at            INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    delivered_at            INTEGER
);
CREATE INDEX job_cancellations_pending ON job_cancellations(requested_at) WHERE delivered_at IS NULL;

CREATE TABLE job_assignments (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    runner_id               INTEGER REFERENCES runners(runner_id) ON DELETE SET NULL,
    assigned_at             INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    first_assigned_at       INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX job_assignments_runner ON job_assignments(runner_id);

CREATE TABLE provision_requests (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    namespace_id            TEXT NOT NULL,
    pool_key                TEXT NOT NULL,
    labels                  TEXT NOT NULL,
    requested_at            INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX provision_requests_queue ON provision_requests(pool_key, requested_at);

-- ── Concurrency gates ────────────────────────────────────────────────
CREATE TABLE concurrency_holds (
    namespace_id            TEXT NOT NULL,
    repository              TEXT NOT NULL,
    group_name              TEXT NOT NULL,
    display_name            TEXT NOT NULL,
    holder_kind             TEXT NOT NULL CHECK (holder_kind IN ('run','job','jobset')),
    holder_run_id           TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    holder_job_id           TEXT,
    holder_jobset_id        INTEGER,
    held_at                 INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    PRIMARY KEY (namespace_id, repository, group_name)
);
CREATE INDEX concurrency_holds_run ON concurrency_holds(holder_run_id);

CREATE TABLE concurrency_waits (
    wait_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace_id            TEXT NOT NULL,
    repository              TEXT NOT NULL,
    group_name              TEXT NOT NULL,
    holder_kind             TEXT NOT NULL CHECK (holder_kind IN ('run','job','jobset')),
    holder_run_id           TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    holder_job_id           TEXT,
    holder_jobset_id        INTEGER,
    queued_at               INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX concurrency_waits_group ON concurrency_waits(namespace_id, repository, group_name, wait_id);
CREATE INDEX concurrency_waits_run ON concurrency_waits(holder_run_id);

CREATE TABLE jobsets (
    jobset_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                  TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    job_ids                 TEXT NOT NULL,
    state                   TEXT NOT NULL CHECK (state IN ('waiting','ready','done')),
    UNIQUE (run_id, job_ids)
);
CREATE TABLE jobset_gates (
    jobset_id               INTEGER NOT NULL REFERENCES jobsets(jobset_id) ON DELETE CASCADE,
    gate_index              INTEGER NOT NULL,
    repository              TEXT NOT NULL,
    group_name              TEXT NOT NULL,
    display_name            TEXT NOT NULL,
    cancel_in_progress      INTEGER NOT NULL DEFAULT 0,
    queue_mode              TEXT NOT NULL CHECK (queue_mode IN ('single','max')),
    acquired                INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (jobset_id, gate_index)
);

-- ── Events (transactional outbox) ────────────────────────────────────
-- Insert-only. One writer: event_id order is commit order, so readers
-- bookmark event_id alone (no txid safe point needed). `version` is the
-- `jobs.version` / `runs.version` the event reports (NULL: no ordering
-- claim). One process owns the database, so there is no other node to read
-- events from; rows are kept only for the live window and pruned by
-- `created_at` (`bootstrap::run_history_archiver`).
CREATE TABLE outbox_events (
    event_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace_id            TEXT NOT NULL,
    run_id                  TEXT,
    job_id                  TEXT,
    version                 INTEGER,
    topic                   TEXT NOT NULL,
    payload                 TEXT NOT NULL,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);
CREATE INDEX outbox_events_age ON outbox_events(created_at);

CREATE TABLE consumer_offsets (
    consumer_name           TEXT PRIMARY KEY,
    last_event_id           INTEGER NOT NULL DEFAULT 0,
    lease_owner             TEXT,
    lease_until             INTEGER,
    updated_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
);

-- ── GitHub integration ───────────────────────────────────────────────
CREATE TABLE webhook_deliveries (
    delivery_id             TEXT PRIMARY KEY,
    installation_id         INTEGER,
    event                   TEXT NOT NULL,
    payload                 TEXT NOT NULL,
    received_at             INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    state                   TEXT NOT NULL CHECK (state IN ('received','processing','done','failed')),
    attempts                INTEGER NOT NULL DEFAULT 0,
    lease_until             INTEGER,
    lease_token             TEXT,
    last_error              TEXT
);
CREATE INDEX webhook_deliveries_claim ON webhook_deliveries(received_at)
    WHERE state IN ('received','processing');
CREATE INDEX webhook_deliveries_prune ON webhook_deliveries(received_at)
    WHERE state IN ('done','failed');

CREATE TABLE webhook_watchdog (
    scope                   TEXT PRIMARY KEY,
    cursor_delivered_at     INTEGER,
    cursor_delivery_guid    TEXT,
    scan_cursor             TEXT,
    last_poll_at            INTEGER,
    last_success_at         INTEGER
);

CREATE TABLE webhook_redeliveries (
    delivery_guid           TEXT PRIMARY KEY,
    github_delivery_id      INTEGER NOT NULL,
    app_id                  TEXT NOT NULL,
    reason                  TEXT NOT NULL CHECK (reason IN ('remote_failure','phantom_ack')),
    attempts                INTEGER NOT NULL DEFAULT 0,
    first_seen_at           INTEGER NOT NULL,
    last_attempt_at         INTEGER,
    resolved_at             INTEGER,
    last_error              TEXT
);
CREATE INDEX webhook_redeliveries_open ON webhook_redeliveries(first_seen_at)
    WHERE resolved_at IS NULL;

CREATE TABLE check_run_updates (
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    installation_id         INTEGER NOT NULL,
    check_run_id            INTEGER,
    version                 INTEGER NOT NULL DEFAULT 0,
    payload                 TEXT NOT NULL,
    not_before              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    attempts                INTEGER NOT NULL DEFAULT 0,
    leased_until            INTEGER,
    lease_owner             TEXT,
    PRIMARY KEY (run_id, job_id)
);
CREATE INDEX check_run_updates_queue ON check_run_updates(installation_id, not_before);

-- ── Environment approvals (durable audit) ────────────────────────────
-- One row per recorded environment review decision (approval or
-- rejection), written in the same transaction that flips the gate, so a
-- crash cannot separate the decision from its record. Deliberately NOT
-- archived with the run and never deleted by retention: GitHub keeps an
-- environment's review history after the run is gone, and this table is
-- the only durable record of who released a gate. `run_id`/`job_id` carry
-- no foreign key for exactly that reason — the run row they name may be
-- deleted while the audit row must survive.
CREATE TABLE environment_approvals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace_id    TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    repository      TEXT NOT NULL,
    environment     TEXT NOT NULL,
    decision        TEXT NOT NULL CHECK (decision IN ('approved','rejected')),
    -- GitHub login of the reviewing user; NULL for the operator's
    -- system-token (admin) override, which carries no user identity.
    actor           TEXT,
    admin_override  INTEGER NOT NULL DEFAULT 0,
    -- Reviewer comment, when one was supplied (native approve endpoint).
    comment         TEXT,
    decided_at      INTEGER NOT NULL
);
CREATE INDEX environment_approvals_gate ON environment_approvals(run_id, job_id);

-- ── Artifacts ────────────────────────────────────────────────────────
-- Artifact catalog rows. Bytes live in the file-backed ArtifactStore; the
-- only statement today is the run-archive `DELETE FROM artifacts`.
CREATE TABLE artifacts (
    artifact_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace_id            TEXT NOT NULL,
    run_id                  TEXT NOT NULL,
    job_backend_id          TEXT NOT NULL,
    name                    TEXT NOT NULL,
    state                   TEXT NOT NULL CHECK (state IN ('pending','finalized')),
    size_bytes              INTEGER,
    digest                  TEXT,
    storage_key             TEXT NOT NULL,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    finalized_at            INTEGER,
    expires_at              INTEGER,
    UNIQUE (run_id, job_backend_id, name)
);
CREATE INDEX artifacts_run ON artifacts(run_id);
CREATE INDEX artifacts_expiry ON artifacts(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX artifacts_pending ON artifacts(created_at) WHERE state = 'pending';

-- ── History (terminal runs) ──────────────────────────────────────────
CREATE TABLE "run_history" (
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
CREATE INDEX run_history_namespace ON run_history(namespace_id, created_at DESC);
CREATE INDEX run_history_repo_ref ON run_history(namespace_id, repository, ref, created_at DESC);

CREATE TABLE "job_history" (
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

CREATE TABLE attempt_history (
    request_id              INTEGER NOT NULL,
    run_id                  TEXT NOT NULL,
    run_created_at          INTEGER NOT NULL,
    job_id                  TEXT NOT NULL,
    namespace_id            TEXT NOT NULL,
    agent_job_id            TEXT NOT NULL,
    timeline_id             TEXT NOT NULL,
    runner_id               INTEGER,
    result                  TEXT,
    claimed_at              INTEGER,
    started_at              INTEGER,
    finished_at             INTEGER,
    PRIMARY KEY (request_id, run_created_at)
);
CREATE INDEX attempt_history_run ON attempt_history(run_id, job_id);
CREATE INDEX attempt_history_agent ON attempt_history(agent_job_id);

CREATE TABLE step_history (
    agent_job_id            TEXT NOT NULL,
    step_id                 TEXT NOT NULL,
    run_id                  TEXT NOT NULL,
    run_created_at          INTEGER NOT NULL,
    namespace_id            TEXT NOT NULL,
    position                INTEGER NOT NULL,
    kind                    TEXT NOT NULL,
    workflow_index          INTEGER,
    runner_number           INTEGER,
    context_name            TEXT,
    name                    TEXT NOT NULL,
    conclusion              TEXT NOT NULL,
    started_at              INTEGER,
    finished_at             INTEGER,
    PRIMARY KEY (agent_job_id, step_id, run_created_at)
);
CREATE INDEX step_history_run ON step_history(run_id);

-- ── Schema metadata (contract addition) ──────────────────────────────
CREATE TABLE schema_meta (
    key                     TEXT PRIMARY KEY,
    value                   BLOB NOT NULL
);

-- Until the platform pushes namespaces, everything lives in 'default'.
INSERT INTO namespaces (namespace_id) VALUES ('default');
