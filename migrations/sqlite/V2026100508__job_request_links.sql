-- Link each live logical job and each archived attempt row to its execution.
-- Existing rows remain NULL: the pointer was not persisted before this migration.
ALTER TABLE jobs ADD COLUMN request_id INTEGER;
ALTER TABLE job_history ADD COLUMN request_id INTEGER;
