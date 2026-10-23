-- Sandbox Service schema v1, SQLite.
--
-- Greenfield: no legacy import, no control-plane tables, no foreign keys into
-- any CI schema. Every payload is a row with queryable columns; JSON columns
-- hold only the nested wire detail that is never filtered on.
--
-- Lock order for multi-table commands:
--   pools -> providers -> sandboxes -> leases -> work_items -> execs/tickets.

PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Database-global monotonic version source. Versions are assigned to every
-- template/pool/sandbox/provider/lease/exec/ticket mutation and are never
-- reused, including after delete + recreate (no ABA for version-keyed caches).
CREATE TABLE IF NOT EXISTS version_seq (
    id   INTEGER PRIMARY KEY CHECK (id = 1),
    next INTEGER NOT NULL
);
INSERT OR IGNORE INTO version_seq (id, next) VALUES (1, 1);

CREATE TABLE IF NOT EXISTS templates (
    name          TEXT PRIMARY KEY,
    digest        TEXT NOT NULL,
    spec_json     TEXT NOT NULL,
    provider_id   TEXT NOT NULL DEFAULT '',
    native_ref    TEXT NOT NULL DEFAULT '',
    driver        TEXT NOT NULL DEFAULT '',
    created_at    INTEGER NOT NULL,
    version       INTEGER NOT NULL,
    retired       INTEGER NOT NULL DEFAULT 0,
    pending_drop  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS templates_live ON templates(retired, name);

-- Per-format artifacts are queryable (digest identity, format, arch).
CREATE TABLE IF NOT EXISTS template_artifacts (
    template   TEXT NOT NULL,
    format     TEXT NOT NULL,
    arch       TEXT NOT NULL,
    digest     TEXT NOT NULL,
    ref        TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    PRIMARY KEY (template, format, arch)
);
CREATE INDEX IF NOT EXISTS template_artifacts_digest ON template_artifacts(digest);

-- A queued/in-flight CreateWork embeds the template's native snapshot base.
-- The pin keeps the base alive until that work finishes; the last pin drops it.
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
    revision       INTEGER NOT NULL,
    version        INTEGER NOT NULL,
    warm           INTEGER NOT NULL,
    max_members    INTEGER NOT NULL,
    lease_ttl_secs INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS pools_template ON pools(template);

-- Immutable creation spec versus mutable sandbox state.
CREATE TABLE IF NOT EXISTS sandbox_specs (
    id               TEXT PRIMARY KEY,
    template_json    TEXT NOT NULL,
    shape_json       TEXT NOT NULL,
    network_json     TEXT NOT NULL,
    mounts_json      TEXT NOT NULL,
    sockets_json     TEXT NOT NULL,
    options_json     TEXT NOT NULL,
    requires_json    TEXT NOT NULL,
    snapshot_capable INTEGER NOT NULL,
    purpose          TEXT NOT NULL,
    pool             TEXT NOT NULL DEFAULT '',
    pool_revision    INTEGER NOT NULL DEFAULT 0,
    driver           TEXT NOT NULL,
    created_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sandboxes (
    id             TEXT PRIMARY KEY REFERENCES sandbox_specs(id) ON DELETE CASCADE,
    name           TEXT NOT NULL DEFAULT '',
    state          TEXT NOT NULL,
    lease_id       TEXT NOT NULL DEFAULT '',
    native_ref     TEXT NOT NULL DEFAULT '',
    error          TEXT NOT NULL DEFAULT '',
    provider_id    TEXT NOT NULL,
    created_at     INTEGER NOT NULL,
    preserve_until INTEGER NOT NULL DEFAULT 0,
    delete_attempts INTEGER NOT NULL DEFAULT 0,
    version        INTEGER NOT NULL
);
-- Live names are unique; empty names are not names.
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
    expires_at             INTEGER NOT NULL,
    env_sealed             BLOB,
    request_hash           TEXT NOT NULL DEFAULT '',
    inject_state           TEXT NOT NULL DEFAULT 'pending',
    inject_owner_node      TEXT NOT NULL DEFAULT '',
    inject_owner_session   TEXT NOT NULL DEFAULT '',
    inject_claim_expires_at INTEGER NOT NULL DEFAULT 0,
    version                INTEGER NOT NULL
);
-- Released/preserved leases clear their key; only live keys are unique.
CREATE UNIQUE INDEX IF NOT EXISTS leases_live_key
    ON leases(idempotency_key) WHERE idempotency_key <> '';
CREATE INDEX IF NOT EXISTS leases_sandbox ON leases(sandbox_id);
CREATE INDEX IF NOT EXISTS leases_expiry ON leases(expires_at);

-- A caller waiting on another replica is durable state, so that replica's
-- reaper does not expire the sandbox it is still waiting for.
CREATE TABLE IF NOT EXISTS acquire_waiters (
    key        TEXT PRIMARY KEY,
    sandbox_id TEXT NOT NULL,
    lease_id   TEXT NOT NULL DEFAULT '',
    node_id    TEXT NOT NULL,
    session_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS acquire_waiters_sandbox ON acquire_waiters(sandbox_id);

CREATE TABLE IF NOT EXISTS providers (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    generation   INTEGER NOT NULL,
    drivers_json TEXT NOT NULL,
    last_seen    INTEGER NOT NULL,
    seen_at      INTEGER NOT NULL,
    cordoned     INTEGER NOT NULL DEFAULT 0,
    host_json    TEXT,
    host_sampled_at INTEGER,
    stats_json   TEXT NOT NULL DEFAULT '[]',
    owner_node    TEXT NOT NULL DEFAULT '',
    owner_session TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS provider_certificates (
    provider_id TEXT NOT NULL,
    sha256      TEXT PRIMARY KEY,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_certificates_provider ON provider_certificates(provider_id);

CREATE TABLE IF NOT EXISTS join_tokens (
    id            TEXT PRIMARY KEY,
    secret_sha256 TEXT NOT NULL,
    expires_at    INTEGER NOT NULL
);

-- Durable provider work. `attempt` is the delivery identity; a Report echoes
-- the delivery id and is fenced when it names an older attempt.
CREATE TABLE IF NOT EXISTS work_items (
    id           TEXT PRIMARY KEY,
    provider_id  TEXT NOT NULL,
    generation   INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    dedup_key    TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    secret_sealed BLOB,
    result_json  TEXT,
    attempt      INTEGER NOT NULL DEFAULT 0,
    visible_at   INTEGER NOT NULL,
    deadline_at  INTEGER NOT NULL,
    created_at   INTEGER NOT NULL,
    completed_at INTEGER
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
    sequence      INTEGER NOT NULL DEFAULT 0,
    deadline      INTEGER NOT NULL,
    status        TEXT NOT NULL,
    exit_code     INTEGER,
    error         TEXT NOT NULL DEFAULT '',
    started       INTEGER NOT NULL DEFAULT 0,
    owner_node    TEXT NOT NULL,
    owner_session TEXT NOT NULL,
    version       INTEGER NOT NULL
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
    expires_at       INTEGER NOT NULL,
    producer_claimed INTEGER NOT NULL DEFAULT 0,
    consumer_claimed INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'open',
    reason           TEXT NOT NULL DEFAULT '',
    owner_node       TEXT NOT NULL,
    owner_session    TEXT NOT NULL,
    version          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS tickets_owner ON tickets(owner_node, owner_session, status);
CREATE INDEX IF NOT EXISTS tickets_expiry ON tickets(status, expires_at);

-- Node registry for live-channel routing. A peer must prove the credential
-- hash and live session; URLs are configuration, never request input.
CREATE TABLE IF NOT EXISTS nodes (
    node_id           TEXT PRIMARY KEY,
    session_id        TEXT NOT NULL,
    credential_sha256 TEXT NOT NULL,
    peer_url          TEXT NOT NULL,
    started_at        INTEGER NOT NULL,
    last_seen         INTEGER NOT NULL,
    alive             INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS nodes_liveness ON nodes(alive, last_seen);

-- Durable change hints. Readers consume by resource version and tolerate
-- gaps; a naked increasing event id is not a safe commit-order cursor.
CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_kind TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    version       INTEGER NOT NULL,
    event         TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_resource ON outbox(resource_kind, resource_id, version);
