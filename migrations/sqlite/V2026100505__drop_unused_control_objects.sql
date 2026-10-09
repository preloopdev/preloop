-- Drop schema objects no code reads or writes (lossy by design: the columns
-- only ever held their defaults), and align two SQLite indexes with the key
-- Postgres already carries.
--
-- `run_submissions.secret_refs`: secret values are resolved at acquire and
-- never stored. `jobs.not_before`: retry-backoff leftover; the queue orders by
-- priority, run_order, job_order. `provision_requests.lease_owner` /
-- `leased_until`: the provisioner queue has no lease protocol.
-- `runner_sessions.engine_node_id`: long-poll wake routing is in-process.
-- `artifacts.upload_token_hash`: pending uploads are tracked in memory.
ALTER TABLE run_submissions DROP COLUMN secret_refs;
ALTER TABLE jobs DROP COLUMN not_before;
ALTER TABLE provision_requests DROP COLUMN lease_owner;
ALTER TABLE provision_requests DROP COLUMN leased_until;
ALTER TABLE runner_sessions DROP COLUMN engine_node_id;
ALTER TABLE artifacts DROP COLUMN upload_token_hash;

-- `jobs_ready` is exactly the claim `ORDER BY` (pool_key, priority DESC,
-- run_order, job_order): `namespace_id` in the middle forced a sort of the
-- last three terms on every poll. `job_requests_attempts` serves the
-- latest-attempt lookups and the `jobs` -> `job_requests` cascade.
DROP INDEX jobs_ready;
CREATE INDEX jobs_ready ON jobs(pool_key, priority DESC, run_order, job_order)
    WHERE queue_state = 'ready';
CREATE INDEX job_requests_attempts ON job_requests(run_id, job_id, request_id DESC);
