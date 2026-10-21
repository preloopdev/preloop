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
SET search_path = control;

CREATE TABLE github_token_revocations (
    token_hash              text PRIMARY KEY,     -- sha256 hex of the token
    request_id              bigint,               -- attempt that owns it
    run_id                  uuid NOT NULL,
    job_id                  text NOT NULL,
    agent_job_id            uuid NOT NULL,
    app_id                  text NOT NULL,
    installation_id         bigint NOT NULL,
    repository              text NOT NULL,
    sealed_token            bytea,                -- Envelope ciphertext; NULL after settle
    expires_at              timestamptz NOT NULL,
    state                   text NOT NULL DEFAULT 'pending'
                              CHECK (state IN ('pending','revoked','spent')),
    attempts                integer NOT NULL DEFAULT 0,
    next_attempt_at         timestamptz NOT NULL, -- claim/backoff deadline
    last_error              text,                 -- status/message, never a token
    created_at              timestamptz NOT NULL
);
CREATE INDEX github_token_revocations_due
    ON github_token_revocations(next_attempt_at);
CREATE INDEX github_token_revocations_request
    ON github_token_revocations(request_id);
CREATE INDEX github_token_revocations_expiry
    ON github_token_revocations(expires_at);
