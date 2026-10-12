-- See the SQLite twin: the public UUID is how the v1 artifact endpoints
-- address a stored artifact, and it must survive a restart.
SET search_path = control;
ALTER TABLE artifacts ADD COLUMN public_id text;
CREATE UNIQUE INDEX artifacts_public_id ON artifacts(public_id) WHERE public_id IS NOT NULL;
