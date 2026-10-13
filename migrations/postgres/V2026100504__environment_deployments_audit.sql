-- Environment deployments and review audit (PostgreSQL).
--
-- `jobs.deployment_id` is the GitHub deployment id for jobs with an
-- `environment:` (created when the run reports checks; deployment statuses
-- update on gate decisions and job completion); `jobs.environment_url` is the
-- runner-evaluated `environment.url` reported in the completion. Both are
-- NULL for environment-less jobs and for jobs reported before this migration.
--
-- `environment_approvals` is the durable review audit: one row per recorded
-- approval or rejection, written in the same transaction that flips the gate.
-- Deliberately not archived with the run and never deleted by retention:
-- GitHub keeps an environment's review history after the run is gone, and
-- this table is the only durable record of who released a gate.
--
-- Additive and data-preserving: existing `jobs` rows keep every column and
-- gain NULL in the two new ones; nothing is rewritten.
SET search_path = control;

ALTER TABLE jobs ADD COLUMN deployment_id bigint;
ALTER TABLE jobs ADD COLUMN environment_url text;

CREATE TABLE environment_approvals (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    namespace_id    text NOT NULL,
    run_id          uuid NOT NULL,
    job_id          text NOT NULL,
    repository      text NOT NULL,
    environment     text NOT NULL,
    decision        text NOT NULL CHECK (decision IN ('approved','rejected')),
    actor           text,
    admin_override  boolean NOT NULL DEFAULT false,
    comment         text,
    decided_at      timestamptz NOT NULL
);
CREATE INDEX environment_approvals_gate ON environment_approvals(run_id, job_id);
