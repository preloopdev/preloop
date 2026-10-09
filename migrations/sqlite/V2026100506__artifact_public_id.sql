-- Legacy v1 artifact uploads are addressed by a public UUID; the control
-- `artifacts` catalog stores it so the v1 GET/list endpoints survive a
-- restart (and an imported legacy catalog is served instead of vanishing
-- with the process).
ALTER TABLE artifacts ADD COLUMN public_id TEXT;
CREATE UNIQUE INDEX artifacts_public_id ON artifacts(public_id) WHERE public_id IS NOT NULL;
