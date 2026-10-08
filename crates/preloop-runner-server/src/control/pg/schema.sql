-- Preloop control plane — target production schema (greenfield, v1).
--
-- Source of truth for docs/control-schema-chartdb.json and
-- docs/control-schema.{dot,png}. Postgres is the production backend; the
-- SQLite backend mirrors every table with the obvious type mapping
-- (uuid/text -> TEXT, timestamptz -> INTEGER µs, jsonb -> TEXT, bytea -> BLOB,
-- identity -> INTEGER PRIMARY KEY) and skips partitioning.
--
-- Design rules
--   * One command = one short transaction of targeted statements. No
--     working-set load, no write-back, no advisory locks.
--   * Transitions are conditional UPDATEs (`WHERE state = <expected>`);
--     zero rows affected = someone else won.
--   * Queues are tables consumed with FOR UPDATE SKIP LOCKED.
--   * Ids come from identity columns / atomic UPDATE .. RETURNING. No
--     in-memory counters.
--   * Hot mutable state is split from immutable spec (jobs vs job_specs,
--     job_requests vs job_leases) so high-frequency updates touch narrow
--     rows and stay HOT-updatable.
--   * No secret values or session keys at rest: secrets are resolved from
--     the SecretProvider at job acquire; session keys live in memory.
--   * Every tenant-owned row carries namespace_id (future shard key).
--   * Live tables hold in-flight work; terminal runs are archived into
--     time-partitioned *_history tables and dropped by partition.

CREATE SCHEMA IF NOT EXISTS control;
SET search_path = control;

-- Cell-local boot invariants. `schema_version` is exactly 1 (greenfield;
-- other values are refused) and `key_fingerprint` fences nodes with
-- different cluster HMAC keys from sharing the same database.
CREATE TABLE schema_meta (
    key                     text PRIMARY KEY,
    value                   bytea NOT NULL
);


-- ── Tenancy ──────────────────────────────────────────────────────────
-- The cell-local copy of what the platform decided for a tenant. Only the
-- values the control engine enforces in its own transactions (submit,
-- claim, job build, archive) live here; storage limits belong to the
-- artifact/log module, fleet limits to fleet, cache limits to the hosts.
-- Running/queued counts are never stored: they are read from `jobs` via
-- the partial indexes, so there is no per-tenant hot counter row.
CREATE TABLE namespaces (
    namespace_id            text PRIMARY KEY,
    -- active: submit + claim. draining: claims queued work, refuses submits.
    -- suspended/deleted: neither (queued jobs stay queued, nothing starts).
    state                   text NOT NULL DEFAULT 'active' CHECK (state IN
                                ('active','suspended','draining','deleted')),
    -- Platform-owned: cell fencing and config push bookkeeping; the engine
    -- neither reads nor writes these two.
    cell_generation         bigint NOT NULL DEFAULT 1,   -- bumped on cell move; fences stale writers
    config_version          bigint NOT NULL DEFAULT 0,   -- last platform push applied
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Execution quotas (NULL = unlimited). Enforced today: max_queued_jobs,
-- submit_rate_per_minute and max_jobs_per_run at API/CLI submit (webhook
-- runs are never refused; they wait at claim); max_running_jobs in the claim
-- predicate (a capped claim locks this row first, so nodes cannot
-- overshoot). Not yet read: max_job_timeout_minutes, priority_tier,
-- run_history_retention_days.
-- Those three are platform-owned: written by the hosted platform, not yet
-- enforced by the engine.
CREATE TABLE namespace_limits (
    namespace_id            text PRIMARY KEY REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    max_queued_jobs         integer,
    max_running_jobs        integer,
    submit_rate_per_minute  integer,
    max_jobs_per_run        integer,
    max_job_timeout_minutes integer,
    priority_tier           integer NOT NULL DEFAULT 0,  -- input to the (private) ordering policy
    run_history_retention_days integer
);

-- Per-pool running caps (e.g. scarce macOS capacity), enforced at claim.
CREATE TABLE namespace_pool_limits (
    namespace_id            text NOT NULL REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    pool_key                text NOT NULL,
    max_running_jobs        integer NOT NULL,
    PRIMARY KEY (namespace_id, pool_key)
);

-- Admission and job-build restrictions.
-- Platform-owned: the hosted platform writes per-tenant admission policy
-- here; the engine does not read it yet.
CREATE TABLE namespace_policies (
    namespace_id            text PRIMARY KEY REFERENCES namespaces(namespace_id) ON DELETE CASCADE,
    fork_pr_policy          text NOT NULL DEFAULT 'untrusted' CHECK (fork_pr_policy IN
                                ('untrusted','require_approval','deny')),
    allowed_actions         jsonb,              -- `uses:` allow-list patterns; NULL = any
    max_token_permissions   jsonb,              -- GITHUB_TOKEN permission ceiling
    oidc_enabled            boolean NOT NULL DEFAULT true,
    allowed_oidc_audiences  jsonb,              -- NULL = any
    self_hosted_runners_allowed boolean NOT NULL DEFAULT true,
    debug_sessions_allowed  boolean NOT NULL DEFAULT true,
    -- local execution (`preloop run -local`, personal pools)
    local_execution_allowed boolean NOT NULL DEFAULT false,
    local_secrets_policy    text NOT NULL DEFAULT 'none' CHECK (local_secrets_policy IN
                                ('none','allowlist','all')),
    local_secrets_allowlist jsonb               -- secret names when policy = allowlist
);

-- ── Runs ─────────────────────────────────────────────────────────────
CREATE TABLE runs (
    run_id                  uuid PRIMARY KEY,
    namespace_id            text NOT NULL REFERENCES namespaces(namespace_id),
    repository              text NOT NULL,
    workflow_path           text NOT NULL,
    run_number              bigint NOT NULL,
    run_attempt             integer NOT NULL DEFAULT 1,
    run_name                text,
    event                   text NOT NULL,
    -- trigger ref (github.ref): refs/heads/main, refs/tags/v1, refs/pull/42/merge
    ref                     text NOT NULL,
    ref_type                text NOT NULL CHECK (ref_type IN ('branch','tag','pull_request','other')),
    head_ref                text,               -- PR source branch
    base_ref                text,               -- PR target branch
    head_sha                text NOT NULL,
    workflow_ref            text NOT NULL,      -- workflow file @ ref (job_workflow_ref)
    status                  text NOT NULL CHECK (status IN
                                ('queued','in_progress','completed')),
    conclusion              text CHECK (conclusion IN
                                ('success','failure','cancelled','skipped','timed_out')),
    webhook_delivery_id     text,
    origin                  text NOT NULL CHECK (origin IN
                                ('webhook','cli','api','schedule','rerun')),
    actor                   text,               -- platform user id; plain text, no FK
    tree_digest             text,               -- workspace tree hash (cli submissions)
    -- workflow-level `concurrency:` (was run_concurrency.concurrency_blob)
    concurrency_group       text,
    concurrency_cancel_in_progress boolean NOT NULL DEFAULT false,
    version                 bigint NOT NULL DEFAULT 0,   -- status/conclusion-change counter (trigger `runs_version`)
    -- Fork-PR policy: the run is held at scheduler admission until the
    -- operator approves it; a hold past the window fails closed (reaper).
    -- Real columns so the expiry sweep filters in SQL.
    fork_approval_pending   boolean NOT NULL DEFAULT false,
    fork_approval_requested_at bigint,
    fork_approval_approved_at  bigint,
    fork_approval_note      text,
    -- Intake reported GitHub check runs for this run (late check-run mint).
    reports_check_runs      boolean NOT NULL DEFAULT false,
    created_at              timestamptz NOT NULL DEFAULT now(),
    started_at              timestamptz,
    completed_at            timestamptz
) WITH (fillfactor = 90);
CREATE UNIQUE INDEX runs_number ON runs(namespace_id, repository, workflow_path, run_number, run_attempt);
CREATE UNIQUE INDEX runs_delivery ON runs(webhook_delivery_id, workflow_path)
    WHERE webhook_delivery_id IS NOT NULL;
CREATE INDEX runs_namespace_recent ON runs(namespace_id, created_at DESC);
CREATE INDEX runs_repo_ref ON runs(namespace_id, repository, ref, created_at DESC);
-- archiver scan: terminal runs not yet moved to history
CREATE INDEX runs_archivable ON runs(completed_at) WHERE status = 'completed';

-- `version` counts status/conclusion changes of the run (see `jobs_version`).
CREATE FUNCTION bump_run_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version := OLD.version + 1;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_version BEFORE UPDATE ON runs
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status
                       OR OLD.conclusion IS DISTINCT FROM NEW.conclusion)
    EXECUTE FUNCTION bump_run_version();

-- Per-workflow run numbers: `UPDATE .. SET next = next + 1 RETURNING`.
CREATE TABLE workflow_run_numbers (
    namespace_id            text NOT NULL REFERENCES namespaces(namespace_id),
    repository              text NOT NULL,
    workflow_path           text NOT NULL,
    last_run_number         bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, repository, workflow_path)
);

-- The request a run was created from. No secret values are ever stored:
-- secrets are resolved from the SecretProvider when a job is acquired.
CREATE TABLE run_submissions (
    run_id                  uuid PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    submission              jsonb NOT NULL,     -- WorkflowSubmission minus secrets
    github_context          jsonb NOT NULL,
    workspace_snapshot      jsonb,
    snapshot_timing         jsonb,              -- duration_ms, object_count, pack_bytes
    -- Record-level per-job maps the table layout has no column for (jobs
    -- with no `jobs` row yet — a check run minted before its matrix leg
    -- materializes). Mirrors lite's `run_submissions.record_details`.
    record_details          jsonb NOT NULL DEFAULT '{}'
);

-- Push-back for local submissions (`preloop push`): acted on after the run
-- is terminal. `effective_sha` is the commit actually published; the echo
-- webhook is matched against it so our own push does not start a new run.
CREATE TABLE run_push_states (
    run_id                  uuid PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    status                  text NOT NULL CHECK (status IN ('pending','synced','blocked')),
    error                   text,
    pr_number               bigint,
    effective_sha           text,
    updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX run_push_states_effective_sha ON run_push_states(effective_sha)
    WHERE effective_sha IS NOT NULL;

-- ── Jobs (hot state) ─────────────────────────────────────────────────
-- Every node of the run graph: dispatchable jobs, matrix legs, matrix
-- parents awaiting expansion, reusable-workflow callers. Only this row is
-- updated during scheduling; the immutable spec lives in job_specs.
CREATE TABLE jobs (
    run_id                  uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    job_id                  text NOT NULL,
    namespace_id            text NOT NULL,
    kind                    text NOT NULL CHECK (kind IN
                                ('job','matrix_parent','matrix_leg','reusable_caller')),
    parent_job_id           text,               -- matrix parent / reusable caller it expanded from
    base_id                 text NOT NULL,      -- matrix base (fail-fast sibling group)
    status                  text NOT NULL CHECK (status IN
                                ('pending','queued','in_progress','success','failure',
                                 'cancelled','skipped','timed_out')),
    queue_state             text NOT NULL CHECK (queue_state IN
                                ('none','blocked','held','ready','claimed',
                                 'pending_expansion','expanding')),
    remaining_needs         integer NOT NULL DEFAULT 0,  -- decremented as needs settle; 0 => promotable
    -- dispatch / claim ordering
    pool_key                text NOT NULL DEFAULT '',
    runs_on                 jsonb NOT NULL DEFAULT '[]', -- label list matched at claim
    runner_group            text,
    priority                integer NOT NULL DEFAULT 0,
    run_order               bigint NOT NULL DEFAULT 0,
    job_order               integer NOT NULL DEFAULT 0,
    enqueued_at             timestamptz,
    claimed_by_runner_id    bigint,
    claimed_at              timestamptz,
    expand_generation       integer NOT NULL DEFAULT 0,  -- expansion fencing token
    version                 bigint NOT NULL DEFAULT 0,   -- status-change counter (trigger `jobs_version`)
    -- results
    outputs                 jsonb,
    annotations             jsonb,
    check_run_id            bigint,
    -- Environment protection gate state (`EnvironmentGateState` JSON): armed
    -- at scheduler admission, updated on approval, cleared when satisfied.
    -- Fail-closed reload: a lost stamp re-arms the gate, never the reverse.
    environment_gate        jsonb,
    -- latency clocks
    created_at              timestamptz NOT NULL DEFAULT now(),
    deps_ready_at           timestamptz,
    concurrency_wait_at     timestamptz,
    concurrency_acquired_at timestamptz,
    started_at              timestamptz,
    completed_at            timestamptz,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, parent_job_id) REFERENCES jobs(run_id, job_id)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
) WITH (fillfactor = 80);
-- claim and ready-queue front: SELECT .. WHERE queue_state='ready' ORDER BY
-- pool_key, priority DESC, run_order, job_order. The key columns are exactly
-- the ORDER BY so the first rows are read in order; a column between the
-- pool key and the priority (this index used to carry `namespace_id` there)
-- forces a sort of the whole ready queue on every call.
CREATE INDEX jobs_ready ON jobs(pool_key, priority DESC, run_order, job_order)
    WHERE queue_state = 'ready';
CREATE INDEX jobs_pending_expansion ON jobs(enqueued_at) WHERE queue_state = 'pending_expansion';
CREATE INDEX jobs_run_active ON jobs(run_id, queue_state) WHERE queue_state <> 'none';
CREATE INDEX jobs_run_base ON jobs(run_id, base_id);
-- read-side quota checks (namespace_limits / namespace_pool_limits)
CREATE INDEX jobs_namespace_running ON jobs(namespace_id, pool_key) WHERE queue_state = 'claimed';
CREATE INDEX jobs_namespace_queued ON jobs(namespace_id)
    WHERE queue_state IN ('blocked','held','ready','pending_expansion');

-- `version` counts status changes of the job, bumped under the row lock the
-- changing statement already holds. Events carry it so a consumer can drop
-- an older state that arrives after a newer one.
CREATE FUNCTION bump_job_version() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version := OLD.version + 1;
    RETURN NEW;
END
$$;
CREATE TRIGGER jobs_version BEFORE UPDATE ON jobs
    FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION bump_job_version();

-- ── Job spec (immutable, written once at submit/expansion) ───────────
CREATE TABLE job_specs (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    display_name            text NOT NULL,
    display_order           integer NOT NULL,
    if_condition            text,
    matrix                  jsonb NOT NULL DEFAULT '{}',
    deferred_matrix         text,               -- dynamic matrix expression
    max_parallel            integer,
    environment             jsonb,
    concurrency             jsonb,              -- job-level `concurrency:`
    reusable_call           jsonb,              -- deferred `uses:` plan / caller metadata
    fail_fast               boolean,
    continue_on_error       boolean,
    id_token_granted        boolean NOT NULL DEFAULT false,
    oidc_environment        text,
    oidc_job_workflow_ref   text,
    oidc_job_workflow_sha   text,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

-- `needs:` edges. Reverse index finds dependents when a job settles.
CREATE TABLE job_needs (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    needs_job_id            text NOT NULL,
    position                integer NOT NULL,
    PRIMARY KEY (run_id, job_id, needs_job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id, needs_job_id) REFERENCES jobs(run_id, job_id)
        ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX job_needs_reverse ON job_needs(run_id, needs_job_id);

-- Runner-bound message as a TEMPLATE: no secret values, no GitHub or
-- runtime token. At acquire the engine resolves `secret_names` through the
-- SecretProvider, mints both tokens, fills the template and encrypts it
-- with the session key; the filled message exists only in memory/on wire.
CREATE TABLE job_messages (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    message_template        jsonb NOT NULL,     -- AgentJobRequestMessage minus secrets/tokens
    secret_names            jsonb NOT NULL DEFAULT '[]',
    condition_context       jsonb NOT NULL,     -- `if:` context; secrets by name only
    -- `timeout-minutes` in seconds, extracted once at write. The reaper reads
    -- it for every unfinished attempt on every tick; extracting it from the
    -- (large, toasted) template per read cost ~15x the rest of that query.
    job_timeout_s           bigint GENERATED ALWAYS AS (
        CASE WHEN jsonb_typeof(message_template->'jobTimeout') = 'number'
             THEN (message_template->>'jobTimeout')::numeric::int8 END) STORED,
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);

-- ── Execution attempts ───────────────────────────────────────────────
CREATE TABLE job_requests (
    request_id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    namespace_id            text NOT NULL,
    -- runner-protocol ids: request_id for acquire/renew/complete, agent_job_id
    -- (message `jobId`) for cancel/timeline. The message's plan reference is
    -- derived (plan_id = agent_job_id, plan_type = 'actions'), not stored;
    -- plan-addressed callbacks route by agent_job_id.
    agent_job_id            uuid NOT NULL UNIQUE,
    timeline_id             uuid NOT NULL UNIQUE,
    runner_id               bigint,
    session_id              uuid,
    result                  text CHECK (result IN
                                ('success','failure','cancelled','skipped','timed_out')),
    timeout_triggered       boolean NOT NULL DEFAULT false,
    debug_token_issued      boolean NOT NULL DEFAULT false,
    claimed_at              timestamptz NOT NULL DEFAULT now(),
    started_at              timestamptz,
    finished_at             timestamptz,
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
) WITH (fillfactor = 90);
CREATE UNIQUE INDEX job_requests_inflight ON job_requests(run_id, job_id) WHERE result IS NULL;
CREATE INDEX job_requests_session ON job_requests(session_id) WHERE result IS NULL;
-- Latest-attempt lookups (`ORDER BY request_id DESC LIMIT 1` per job) and the
-- `jobs` -> `job_requests` cascade: the partial inflight index above cannot
-- serve settled attempts, so both fell back to a seq scan of the table.
CREATE INDEX job_requests_attempts ON job_requests(run_id, job_id, request_id DESC);

-- Lease (heartbeat target). Narrow row: a renewal is one tiny UPDATE.
-- Reaper: SELECT .. WHERE expires_at < now() FOR UPDATE SKIP LOCKED LIMIT n.
CREATE TABLE job_leases (
    request_id              bigint PRIMARY KEY REFERENCES job_requests(request_id) ON DELETE CASCADE,
    runner_id               bigint NOT NULL,
    expires_at              timestamptz NOT NULL,
    renewed_at              timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);
CREATE INDEX job_leases_expiry ON job_leases(expires_at);

-- Deferred GitHub App token mint recipe (not a token). Deleted at terminal.
CREATE TABLE github_token_requests (
    request_id              bigint PRIMARY KEY REFERENCES job_requests(request_id) ON DELETE CASCADE,
    repository              text NOT NULL,
    permissions             jsonb NOT NULL,
    declared                boolean NOT NULL,
    untrusted               boolean NOT NULL
);

-- Step manifest + progress per attempt. Written in per-node batches.
CREATE TABLE job_steps (
    agent_job_id            uuid NOT NULL REFERENCES job_requests(agent_job_id) ON DELETE CASCADE,
    step_id                 text NOT NULL,      -- TaskStep.id
    position                integer NOT NULL,
    kind                    text NOT NULL CHECK (kind IN ('workflow','synthetic')),
    workflow_index          integer,
    runner_number           integer,
    context_name            text,
    name                    text NOT NULL,
    conclusion              text NOT NULL,
    started_at              timestamptz,
    finished_at             timestamptz,
    PRIMARY KEY (agent_job_id, step_id)
) WITH (fillfactor = 80);

-- Azure-protocol timeline (runner PATCH/GET replay). Batched upserts.
CREATE TABLE timelines (
    timeline_id             uuid PRIMARY KEY REFERENCES job_requests(timeline_id) ON DELETE CASCADE,
    change_id               integer NOT NULL DEFAULT 0
);
CREATE TABLE timeline_records (
    timeline_id             uuid NOT NULL REFERENCES timelines(timeline_id) ON DELETE CASCADE,
    record_id               uuid NOT NULL,
    change_id               integer NOT NULL,
    record                  jsonb NOT NULL,
    PRIMARY KEY (timeline_id, record_id)
) WITH (fillfactor = 80);

-- Per-plan log ids fit the official runner's 32-bit TaskLog.Id and remain
-- stable across nodes. The unique pair arbitrates concurrent allocations.
-- Content and sizes live in file segments; this row only allocates ids.
CREATE TABLE log_files (
    log_key                 text PRIMARY KEY,
    run_id                  uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    plan_id                 uuid NOT NULL REFERENCES job_requests(agent_job_id) ON DELETE CASCADE,
    log_id                  integer NOT NULL CHECK (log_id > 0),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (plan_id, log_id)
);
CREATE INDEX log_files_run ON log_files(run_id);

-- ── Runners and sessions ─────────────────────────────────────────────
CREATE TABLE runners (
    runner_id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id            text NOT NULL REFERENCES namespaces(namespace_id),
    name                    text NOT NULL,
    labels                  jsonb NOT NULL DEFAULT '[]',
    ephemeral               boolean NOT NULL DEFAULT false,
    runner_group_id         bigint,
    runner_group_name       text,
    client_id               text UNIQUE,        -- dedup on re-register
    public_key              text,
    rsa_public_key          bytea,
    pool_proven             boolean NOT NULL DEFAULT false,
    registered_at           timestamptz NOT NULL DEFAULT now(),
    last_seen_at            timestamptz
);

CREATE TABLE runner_sessions (
    session_id              uuid PRIMARY KEY,
    runner_id               bigint REFERENCES runners(runner_id) ON DELETE CASCADE,
    protocol                text NOT NULL CHECK (protocol IN ('broker','azdo')),
    client_id               text,
    verified                boolean NOT NULL DEFAULT false,
    created_at              timestamptz NOT NULL DEFAULT now(),
    last_seen_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX runner_sessions_runner ON runner_sessions(runner_id);
CREATE INDEX runner_sessions_liveness ON runner_sessions(last_seen_at);

-- Per-session outbound messages (queue). Poll: oldest for session, ack = DELETE.
CREATE TABLE session_messages (
    -- Ids start above 1e6: broker job refs carry request_id (small ints) as
    -- messageId; a cancel in the same range would collide in the runner's
    -- in-memory dedup and be silently dropped.
    message_id              bigint GENERATED ALWAYS AS IDENTITY (START WITH 1000001) PRIMARY KEY,
    session_id              uuid NOT NULL REFERENCES runner_sessions(session_id) ON DELETE CASCADE,
    message_type            text NOT NULL,
    -- job assignments carry only request_id; the real message is built and
    -- session-encrypted when the poll is answered
    request_id              bigint REFERENCES job_requests(request_id) ON DELETE CASCADE,
    body                    jsonb,              -- small non-secret messages (cancel, …)
    created_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX session_messages_session ON session_messages(session_id, message_id);

-- ── Dispatch side queues ─────────────────────────────────────────────
-- Cancels awaiting delivery to the runner holding the attempt.
CREATE TABLE job_cancellations (
    cancellation_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id              bigint NOT NULL REFERENCES job_requests(request_id) ON DELETE CASCADE,
    reason                  text,
    requested_at            timestamptz NOT NULL DEFAULT now(),
    delivered_at            timestamptz
);
CREATE INDEX job_cancellations_pending ON job_cancellations(requested_at) WHERE delivered_at IS NULL;

-- Strict job -> machine binding.
CREATE TABLE job_assignments (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    runner_id               bigint REFERENCES runners(runner_id) ON DELETE SET NULL,
    assigned_at             timestamptz NOT NULL DEFAULT now(),
    first_assigned_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX job_assignments_runner ON job_assignments(runner_id);

-- Jobs waiting on a provisioned runner (autoscaler queue).
CREATE TABLE provision_requests (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    namespace_id            text NOT NULL,
    pool_key                text NOT NULL,
    labels                  jsonb NOT NULL,
    requested_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, job_id),
    FOREIGN KEY (run_id, job_id) REFERENCES jobs(run_id, job_id) ON DELETE CASCADE
);
CREATE INDEX provision_requests_queue ON provision_requests(pool_key, requested_at);

-- ── Concurrency gates ────────────────────────────────────────────────
-- A hold IS the lock: INSERT .. ON CONFLICT DO NOTHING; 1 row = acquired.
CREATE TABLE concurrency_holds (
    namespace_id            text NOT NULL,
    repository              text NOT NULL,
    group_name              text NOT NULL,
    display_name            text NOT NULL,
    holder_kind             text NOT NULL CHECK (holder_kind IN ('run','job','jobset')),
    holder_run_id           uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    holder_job_id           text,
    holder_jobset_id        bigint,
    held_at                 timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, repository, group_name)
);
CREATE INDEX concurrency_holds_run ON concurrency_holds(holder_run_id);

-- FIFO waiters; wait_id orders the queue (no position renumbering).
CREATE TABLE concurrency_waits (
    wait_id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id            text NOT NULL,
    repository              text NOT NULL,
    group_name              text NOT NULL,
    holder_kind             text NOT NULL CHECK (holder_kind IN ('run','job','jobset')),
    holder_run_id           uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    holder_job_id           text,
    holder_jobset_id        bigint,
    queued_at               timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX concurrency_waits_group ON concurrency_waits(namespace_id, repository, group_name, wait_id);
CREATE INDEX concurrency_waits_run ON concurrency_waits(holder_run_id);

-- Reusable-call matrix cells admitted as a unit (replaces jobset_ready).
CREATE TABLE jobsets (
    jobset_id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                  uuid NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    job_ids                 jsonb NOT NULL,     -- sorted member job ids
    state                   text NOT NULL CHECK (state IN ('waiting','ready','done')),
    UNIQUE (run_id, job_ids)
);
CREATE TABLE jobset_gates (
    jobset_id               bigint NOT NULL REFERENCES jobsets(jobset_id) ON DELETE CASCADE,
    gate_index              integer NOT NULL,
    repository              text NOT NULL,
    group_name              text NOT NULL,
    display_name            text NOT NULL,
    cancel_in_progress      boolean NOT NULL DEFAULT false,
    queue_mode              text NOT NULL CHECK (queue_mode IN ('single','max')),
    acquired                boolean NOT NULL DEFAULT false,
    PRIMARY KEY (jobset_id, gate_index)
);

-- ── Events (transactional outbox) ────────────────────────────────────
-- Written in the command's transaction; insert-only (never updated).
-- Readers read only below the safe point: rows with
-- txid < pg_snapshot_xmin(pg_current_snapshot()) belong to finished
-- transactions, so nothing new can appear below it. Order by
-- (txid, event_id). That order is not commit order (a txid is assigned at
-- a transaction's first write), so a consumer that needs "latest state
-- wins" compares `version` per entity instead of trusting row order.
--
-- Retention: no durable consumer exists yet, so rows are kept only for
-- the live fan-out window (`bootstrap::run_history_archiver` prunes by
-- `created_at`). A durable consumer must hold a `consumer_offsets`
-- bookmark and the prune must then stop at the slowest bookmark.
CREATE TABLE outbox_events (
    event_id                bigint GENERATED ALWAYS AS IDENTITY,
    txid                    xid8 NOT NULL DEFAULT pg_current_xact_id(),
    namespace_id            text NOT NULL,
    run_id                  uuid,
    job_id                  text,               -- job the event is about, when it is about one
    -- `jobs.version` / `runs.version` right after the change the event
    -- reports. NULL makes no ordering claim (events that are not state:
    -- annotations, check-run ids, or a status that no longer matches the row).
    version                 bigint,
    -- Writer's `preloop.origin` (one id per node process). A node skips its
    -- own rows: it already broadcast those events directly.
    origin                  text NOT NULL DEFAULT '',
    topic                   text NOT NULL,      -- versioned, e.g. job.completed.v1
    payload                 jsonb NOT NULL,     -- ids and states only, never secrets
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, created_at)
) PARTITION BY RANGE (created_at);
CREATE TABLE outbox_events_default PARTITION OF outbox_events DEFAULT;
CREATE INDEX outbox_events_read ON outbox_events(txid, event_id);
CREATE INDEX outbox_events_age ON outbox_events(created_at);

-- One row per registered consumer (or consumer shard): its bookmark.
-- Advanced once per batch, in the same transaction as the consumer's
-- own writes when it has any.
CREATE TABLE consumer_offsets (
    consumer_name           text PRIMARY KEY,   -- e.g. usage-meter, runner-wakeup/shard-3
    last_txid               xid8 NOT NULL DEFAULT '0',
    last_event_id           bigint NOT NULL DEFAULT 0,
    lease_owner             text,               -- node currently reading
    lease_until             timestamptz,
    updated_at              timestamptz NOT NULL DEFAULT now()
);

-- ── GitHub integration ───────────────────────────────────────────────
CREATE TABLE webhook_deliveries (
    delivery_id             text PRIMARY KEY,
    installation_id         bigint,
    event                   text NOT NULL,
    payload                 jsonb NOT NULL,
    received_at             timestamptz NOT NULL DEFAULT now(),
    state                   text NOT NULL CHECK (state IN ('received','processing','done','failed')),
    attempts                integer NOT NULL DEFAULT 0,
    lease_until             timestamptz,
    lease_token             uuid,
    last_error              text
);
CREATE INDEX webhook_deliveries_claim ON webhook_deliveries(received_at)
    WHERE state IN ('received','processing');
CREATE INDEX webhook_deliveries_prune ON webhook_deliveries(received_at)
    WHERE state IN ('done','failed');

CREATE TABLE webhook_watchdog (
    scope                   text PRIMARY KEY,
    cursor_delivered_at     timestamptz,
    cursor_delivery_guid    text,
    scan_cursor             text,
    last_poll_at            timestamptz,
    last_success_at         timestamptz
);

CREATE TABLE webhook_redeliveries (
    delivery_guid           text PRIMARY KEY,
    github_delivery_id      bigint NOT NULL,
    app_id                  text NOT NULL,
    reason                  text NOT NULL CHECK (reason IN ('remote_failure','phantom_ack')),
    attempts                integer NOT NULL DEFAULT 0,
    first_seen_at           timestamptz NOT NULL,
    last_attempt_at         timestamptz,
    resolved_at             timestamptz,
    last_error              text
);
CREATE INDEX webhook_redeliveries_open ON webhook_redeliveries(first_seen_at)
    WHERE resolved_at IS NULL;

-- Outbound check-run updates, coalesced per job (latest state wins) and
-- drained per installation under its rate-limit budget.
CREATE TABLE check_run_updates (
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    installation_id         bigint NOT NULL,
    check_run_id            bigint,
    version                 bigint NOT NULL DEFAULT 0,
    payload                 jsonb NOT NULL,
    not_before              timestamptz NOT NULL DEFAULT now(),
    attempts                integer NOT NULL DEFAULT 0,
    leased_until            timestamptz,
    lease_owner             text,
    PRIMARY KEY (run_id, job_id)
);
CREATE INDEX check_run_updates_queue ON check_run_updates(installation_id, not_before);

-- ── Artifacts (replaces the artifact part of the `meta` blob) ────────
-- Blobs live in object storage; upload state lives in the file-backed
-- ArtifactStore. These rows are the catalog; the only statement today is
-- the run-archive DELETE.
--
-- The Actions cache is deliberately NOT here: it is a bounded cache
-- colocated with the runner hosts (preloop-cache CAS: key index ->
-- sha256 object pool), owned host-side. The control plane gains cache
-- tables only if scheduling starts using cache locality.
CREATE TABLE artifacts (
    artifact_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id            text NOT NULL,
    run_id                  uuid NOT NULL,      -- no FK: outlives archived runs
    job_backend_id          text NOT NULL,
    name                    text NOT NULL,
    state                   text NOT NULL CHECK (state IN ('pending','finalized')),
    size_bytes              bigint,
    digest                  text,
    storage_key             text NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    finalized_at            timestamptz,
    expires_at              timestamptz,
    UNIQUE (run_id, job_backend_id, name)
);
CREATE INDEX artifacts_run ON artifacts(run_id);
CREATE INDEX artifacts_expiry ON artifacts(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX artifacts_pending ON artifacts(created_at) WHERE state = 'pending';

-- ── History (terminal runs, partitioned by run creation, dropped by partition) ──
CREATE TABLE run_history (
    run_id                  uuid NOT NULL,
    namespace_id            text NOT NULL,
    repository              text NOT NULL,
    workflow_path           text NOT NULL,
    run_number              bigint NOT NULL,
    run_attempt             integer NOT NULL,
    run_name                text,
    event                   text NOT NULL,
    ref                     text NOT NULL,
    ref_type                text NOT NULL,
    head_ref                text,
    base_ref                text,
    head_sha                text NOT NULL,
    conclusion              text,
    submission              jsonb NOT NULL,     -- secrets never archived
    record_details          jsonb NOT NULL DEFAULT '{}',
    fork_approval_pending   boolean NOT NULL DEFAULT false,
    fork_approval_requested_at bigint,
    fork_approval_approved_at  bigint,
    fork_approval_note      text,
    reports_check_runs      boolean NOT NULL DEFAULT false,
    created_at              timestamptz NOT NULL,
    started_at              timestamptz,
    completed_at            timestamptz,
    PRIMARY KEY (run_id, created_at)
) PARTITION BY RANGE (created_at);
CREATE TABLE run_history_default PARTITION OF run_history DEFAULT;
CREATE INDEX run_history_namespace ON run_history(namespace_id, created_at DESC);
CREATE INDEX run_history_repo_ref ON run_history(namespace_id, repository, ref, created_at DESC);

CREATE TABLE job_history (
    run_id                  uuid NOT NULL,
    run_created_at          timestamptz NOT NULL,
    job_id                  text NOT NULL,
    namespace_id            text NOT NULL,
    kind                    text NOT NULL,
    parent_job_id           text,
    base_id                 text NOT NULL,
    display_name            text NOT NULL,
    status                  text NOT NULL,
    pool_key                text NOT NULL,
    outputs                 jsonb,
    annotations             jsonb,
    check_run_id            bigint,
    created_at              timestamptz NOT NULL,
    deps_ready_at           timestamptz,
    started_at              timestamptz,
    completed_at            timestamptz,
    PRIMARY KEY (run_id, job_id, run_created_at)
) PARTITION BY RANGE (run_created_at);
CREATE TABLE job_history_default PARTITION OF job_history DEFAULT;

CREATE TABLE attempt_history (
    request_id              bigint NOT NULL,
    run_id                  uuid NOT NULL,
    run_created_at          timestamptz NOT NULL,
    job_id                  text NOT NULL,
    namespace_id            text NOT NULL,
    agent_job_id            uuid NOT NULL,
    timeline_id             uuid NOT NULL,
    runner_id               bigint,
    result                  text,
    claimed_at              timestamptz,
    started_at              timestamptz,
    finished_at             timestamptz,
    PRIMARY KEY (request_id, run_created_at)
) PARTITION BY RANGE (run_created_at);
CREATE TABLE attempt_history_default PARTITION OF attempt_history DEFAULT;
CREATE INDEX attempt_history_run ON attempt_history(run_id, job_id);
CREATE INDEX attempt_history_agent ON attempt_history(agent_job_id);

CREATE TABLE step_history (
    agent_job_id            uuid NOT NULL,
    step_id                 text NOT NULL,      -- TaskStep.id
    run_id                  uuid NOT NULL,
    run_created_at          timestamptz NOT NULL,
    namespace_id            text NOT NULL,
    position                integer NOT NULL,
    kind                    text NOT NULL,
    workflow_index          integer,
    runner_number           integer,
    context_name            text,
    name                    text NOT NULL,
    conclusion              text NOT NULL,
    started_at              timestamptz,
    finished_at             timestamptz,
    PRIMARY KEY (agent_job_id, step_id, run_created_at)
) PARTITION BY RANGE (run_created_at);
CREATE TABLE step_history_default PARTITION OF step_history DEFAULT;
CREATE INDEX step_history_run ON step_history(run_id);
