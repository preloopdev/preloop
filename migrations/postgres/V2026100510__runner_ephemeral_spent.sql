-- One-way lifetime marker on the runner identity. Set atomically with the
-- first claim; it must outlive attempt/run archival and any failed cleanup so
-- a single-use runner can never become claimable or credential-usable again.
ALTER TABLE runners ADD COLUMN ephemeral_spent boolean NOT NULL DEFAULT false;
