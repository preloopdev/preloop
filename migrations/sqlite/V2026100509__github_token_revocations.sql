-- Durable ledger of job-owned GitHub App installation tokens that still owe a
-- remote revocation. The token is stored sealed with the cluster key
-- (`store::Envelope`), never in plaintext: `DELETE /installation/token` must
-- be authenticated with the token itself, so the engine cannot use the App
-- private key. Rows are independent of `job_requests` (no foreign key): the
-- attempt may be archived or purged before GitHub is reachable, and the row
-- must survive that to be retried. After a successful revoke (or a 401) the row
-- becomes a tombstone with no sealed token, kept until the token's own
-- `expires_at` so a stale online-validation cache cannot re-accept it;
-- cleanup deletes rows once that expiry plus a short grace has passed.
CREATE TABLE github_token_revocations (
    token_hash              TEXT PRIMARY KEY,     -- sha256 hex of the token
    request_id              INTEGER,              -- attempt that owns it
    run_id                  TEXT NOT NULL,
    job_id                  TEXT NOT NULL,
    agent_job_id            TEXT NOT NULL,
    app_id                  TEXT NOT NULL,
    installation_id         INTEGER NOT NULL,
    repository              TEXT NOT NULL,
    sealed_token            BLOB,                 -- Envelope ciphertext; NULL after settle
    expires_at              INTEGER NOT NULL,     -- unix microseconds
    state                   TEXT NOT NULL DEFAULT 'pending'
                              CHECK (state IN ('pending','revoked','spent')),
    attempts                INTEGER NOT NULL DEFAULT 0,
    next_attempt_at         INTEGER NOT NULL,     -- claim/backoff deadline
    last_error              TEXT,                 -- status/message, never a token
    created_at              INTEGER NOT NULL
);
CREATE INDEX github_token_revocations_due
    ON github_token_revocations(next_attempt_at);
CREATE INDEX github_token_revocations_request
    ON github_token_revocations(request_id);
CREATE INDEX github_token_revocations_expiry
    ON github_token_revocations(expires_at);
