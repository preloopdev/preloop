-- Sandbox Service schema v1, Postgres.
--
-- A dedicated `sandbox` schema, never the CI control schema. Greenfield: no
-- legacy import and no foreign keys into any control table. Every payload is
-- a row with queryable columns; JSON columns hold only nested wire detail.

CREATE SCHEMA IF NOT EXISTS sandbox;
SET search_path TO sandbox;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_seq (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    next BIGINT NOT NULL
);
INSERT INTO version_seq (id, next) VALUES (1, 1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS templates (
    name          TEXT PRIMARY KEY,
    digest        TEXT NOT NULL,
    spec_json     TEXT NOT NULL,
    provider_id   TEXT NOT NULL DEFAULT '',
    native_ref    TEXT NOT NULL DEFAULT '',
    driver        TEXT NOT NULL DEFAULT '',
    created_at    BIGINT NOT NULL,
    version       BIGINT NOT NULL,
    retired       BOOLEAN NOT NULL DEFAULT FALSE,
    pending_drop  BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS templates_live ON templates(retired, name);

CREATE TABLE IF NOT EXISTS template_artifacts (
    template   TEXT NOT NULL,
    format     TEXT NOT NULL,
    arch       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    ref        TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    PRIMARY KEY (template, format, arch)
);
CREATE INDEX IF NOT EXISTS template_artifacts_digest ON template_artifacts(digest);

CREATE TABLE IF NOT EXISTS template_pins (
    base_ref TEXT NOT NULL,
    work_id  TEXT NOT NULL,
    PRIMARY KEY (base_ref, work_id)
);


-- Native snapshot bases that outlive a deleted/replaced logical template until
-- every queued CreateWork embedding them has finished.
CREATE TABLE IF NOT EXISTS snapshot_bases (
    native_ref  TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    driver      TEXT NOT NULL,
    drop_pending BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS pools (
    name           TEXT PRIMARY KEY,
    template       TEXT NOT NULL,
    spec_json      TEXT NOT NULL,
    revision       BIGINT NOT NULL,
    version        BIGINT NOT NULL,
    warm           INTEGER NOT NULL,
    max_members    INTEGER NOT NULL,
    lease_ttl_secs BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS pools_template ON pools(template);

CREATE TABLE IF NOT EXISTS sandbox_specs (
    id               TEXT PRIMARY KEY,
    template_json    TEXT NOT NULL,
    shape_json       TEXT NOT NULL,
    network_json     TEXT NOT NULL,
    mounts_json      TEXT NOT NULL,
    sockets_json     TEXT NOT NULL,
    options_json     TEXT NOT NULL,
    requires_json    TEXT NOT NULL,
    snapshot_capable BOOLEAN NOT NULL,
    purpose          TEXT NOT NULL,
    pool             TEXT NOT NULL DEFAULT '',
    pool_revision    BIGINT NOT NULL DEFAULT 0,
    driver           TEXT NOT NULL,
    created_at       BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sandboxes (
    id             TEXT PRIMARY KEY REFERENCES sandbox_specs(id) ON DELETE CASCADE,
    name           TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL,
    lease_id       TEXT NOT NULL DEFAULT '',
    native_ref     TEXT NOT NULL DEFAULT '',
    error          TEXT NOT NULL DEFAULT '',
    provider_id    TEXT NOT NULL,
    created_at     BIGINT NOT NULL,
    preserve_until BIGINT NOT NULL DEFAULT 0,
    delete_attempts INTEGER NOT NULL DEFAULT 0,
    version        BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS sandboxes_live_name
    ON sandboxes(name) WHERE name <> '' AND state NOT IN ('gone', 'failed');
CREATE UNIQUE INDEX IF NOT EXISTS sandboxes_live_lease
    ON sandboxes(lease_id) WHERE lease_id <> '';
CREATE INDEX IF NOT EXISTS sandboxes_provider ON sandboxes(provider_id, state);
CREATE INDEX IF NOT EXISTS sandboxes_pool ON sandboxes(state);


-- Same-key reservation: one acquire wins the key before it checks names or
-- capacity, so a duplicate request waits for the winner instead of racing it.
CREATE TABLE IF NOT EXISTS lease_keys (
    key        TEXT PRIMARY KEY,
    lease_id   TEXT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    id                     TEXT PRIMARY KEY,
    sandbox_id             TEXT NOT NULL,
    pool                   TEXT NOT NULL DEFAULT '',
    idempotency_key        TEXT NOT NULL DEFAULT '',
    expires_at             BIGINT NOT NULL,
    requested_ttl_secs     BIGINT NOT NULL DEFAULT 0,
    env_sealed             BYTEA,
    request_hash           TEXT NOT NULL DEFAULT '',
    inject_state           TEXT NOT NULL DEFAULT 'pending',
    inject_owner_node      TEXT NOT NULL DEFAULT '',
    inject_owner_session   TEXT NOT NULL DEFAULT '',
    inject_claim_expires_at BIGINT NOT NULL DEFAULT 0,
    version                BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS leases_live_key
    ON leases(idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS leases_sandbox ON leases(sandbox_id);
CREATE INDEX IF NOT EXISTS leases_expiry ON leases(expires_at);

CREATE TABLE IF NOT EXISTS acquire_waiters (
    key        TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    lease_id   TEXT NOT NULL DEFAULT '',
    node_id    TEXT NOT NULL,
    session_id TEXT NOT NULL,
    expires_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS acquire_waiters_sandbox ON acquire_waiters(sandbox_id);

-- One expiring, node-session-fenced snapshot claim per sandbox. It reserves
-- the source while a capture is in flight so a second snapshot or a release
-- cannot race it; the row is deleted when the work finishes or expires.
CREATE TABLE IF NOT EXISTS snapshot_claims (
    sandbox_id TEXT PRIMARY KEY,
    work_id    TEXT NOT NULL DEFAULT '',
    node_id    TEXT NOT NULL,
    session_id TEXT NOT NULL,
    expires_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS snapshot_claims_expiry ON snapshot_claims(expires_at);

CREATE TABLE IF NOT EXISTS providers (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    generation   BIGINT NOT NULL,
    drivers_json TEXT NOT NULL,
    last_seen    BIGINT NOT NULL,
    seen_at      BIGINT NOT NULL,
    cordoned     BOOLEAN NOT NULL DEFAULT FALSE,
    host_json    TEXT,
    host_sampled_at BIGINT,
    stats_json   TEXT NOT NULL DEFAULT '[]',
    owner_node    TEXT NOT NULL DEFAULT '',
    owner_session TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS provider_certificates (
    provider_id TEXT NOT NULL,
    sha256      TEXT PRIMARY KEY,
    expires_at  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_certificates_provider ON provider_certificates(provider_id);

CREATE TABLE IF NOT EXISTS join_tokens (
    id            TEXT PRIMARY KEY,
    secret_sha256 TEXT NOT NULL,
    expires_at    BIGINT NOT NULL,
    -- Empty: may enroll a new provider name only. Non-empty: the one name
    -- the token may enroll or re-enroll (replacing its certificates).
    provider_name TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS join_receipts (
    token_id        TEXT PRIMARY KEY,
    secret_sha256   TEXT NOT NULL,
    csr_sha256      TEXT NOT NULL,
    provider_name   TEXT NOT NULL,
    cert_sha256     TEXT NOT NULL,
    cert_pem        TEXT NOT NULL,
    cert_expires_at BIGINT NOT NULL,
    expires_at      BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS join_receipts_expiry ON join_receipts(expires_at);

CREATE TABLE IF NOT EXISTS work_items (
    id           TEXT PRIMARY KEY,
    provider_id  TEXT NOT NULL,
    generation   BIGINT NOT NULL,
    kind         TEXT NOT NULL,
    dedup_key    TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    secret_sealed BYTEA,
    result_json  TEXT,
    attempt      BIGINT NOT NULL DEFAULT 0,
    visible_at   BIGINT NOT NULL,
    deadline_at  BIGINT NOT NULL,
    created_at   BIGINT NOT NULL,
    completed_at BIGINT
);
CREATE UNIQUE INDEX IF NOT EXISTS work_active_dedup
    ON work_items(dedup_key) WHERE dedup_key <> '' AND status <> 'done';
CREATE INDEX IF NOT EXISTS work_claim
    ON work_items(provider_id, status, visible_at, created_at);
CREATE INDEX IF NOT EXISTS work_done_dedup
    ON work_items(dedup_key, completed_at) WHERE dedup_key <> '' AND status = 'done';

CREATE TABLE IF NOT EXISTS execs (
    id            TEXT PRIMARY KEY,
    sandbox_id    TEXT NOT NULL,
    work_id       TEXT NOT NULL,
    provider_id   TEXT NOT NULL,
    sequence      BIGINT NOT NULL DEFAULT 0,
    deadline      BIGINT NOT NULL,
    status        TEXT NOT NULL,
    exit_code     INTEGER,
    error         TEXT NOT NULL DEFAULT '',
    started       BOOLEAN NOT NULL DEFAULT FALSE,
    started_attempt BIGINT NOT NULL DEFAULT 0,
    owner_node    TEXT NOT NULL,
    owner_session TEXT NOT NULL,
    version       BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS execs_owner ON execs(owner_node, owner_session, status);
CREATE INDEX IF NOT EXISTS execs_deadline ON execs(status, deadline);

CREATE TABLE IF NOT EXISTS tickets (
    id               TEXT PRIMARY KEY,
    sandbox_id       TEXT NOT NULL,
    lease_id         TEXT NOT NULL,
    kind             TEXT NOT NULL,
    guest_path       TEXT NOT NULL,
    token_sha256     TEXT NOT NULL,
    expires_at       BIGINT NOT NULL,
    producer_claimed BOOLEAN NOT NULL DEFAULT FALSE,
    consumer_claimed BOOLEAN NOT NULL DEFAULT FALSE,
    status           TEXT NOT NULL DEFAULT 'open',
    reason           TEXT NOT NULL DEFAULT '',
    owner_node       TEXT NOT NULL,
    owner_session    TEXT NOT NULL,
    version          BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS tickets_owner ON tickets(owner_node, owner_session, status);
CREATE INDEX IF NOT EXISTS tickets_expiry ON tickets(status, expires_at);

CREATE TABLE IF NOT EXISTS nodes (
    node_id           TEXT PRIMARY KEY,
    session_id        TEXT NOT NULL,
    credential_sha256 TEXT NOT NULL,
    peer_url          TEXT NOT NULL,
    started_at        BIGINT NOT NULL,
    last_seen         BIGINT NOT NULL,
    alive             BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS nodes_liveness ON nodes(alive, last_seen);

CREATE TABLE IF NOT EXISTS outbox (
    id            BIGSERIAL PRIMARY KEY,
    resource_kind TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    version       BIGINT NOT NULL,
    event         TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    created_at    BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_resource ON outbox(resource_kind, resource_id, version);
