-- Drop schema objects no code reads or writes (lossy by design: the columns
-- only ever held their defaults).
--
-- `run_submissions.secret_refs`: secret values are resolved at acquire and
-- never stored. `jobs.not_before`: retry-backoff leftover; the queue orders by
-- priority, run_order, job_order. `provision_requests.lease_owner` /
-- `leased_until`: the provisioner queue has no lease protocol.
-- `runner_sessions.engine_node_id`: long-poll wake routing is in-process.
-- `artifacts.upload_token_hash`: pending uploads are tracked in memory.
-- `runners_labels`: a GIN index no query can use; runner matching reads
-- `runners.labels` in Rust.
SET search_path = control;

DROP INDEX IF EXISTS runners_labels;
ALTER TABLE run_submissions DROP COLUMN secret_refs;
ALTER TABLE jobs DROP COLUMN not_before;
ALTER TABLE provision_requests DROP COLUMN lease_owner;
ALTER TABLE provision_requests DROP COLUMN leased_until;
ALTER TABLE runner_sessions DROP COLUMN engine_node_id;
ALTER TABLE artifacts DROP COLUMN upload_token_hash;
