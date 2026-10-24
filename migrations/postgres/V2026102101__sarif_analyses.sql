-- See the SQLite twin. Raw SARIF bytes live in the run-scoped file store;
-- these rows carry the analysis metadata and its normalized findings.
SET search_path = control;

CREATE TABLE sarif_analyses (
    analysis_id             text PRIMARY KEY,
    namespace_id            text NOT NULL DEFAULT 'default',
    run_id                  uuid NOT NULL,
    repository              text NOT NULL,
    commit_sha              text NOT NULL,
    git_ref                 text NOT NULL,
    tool_name               text NOT NULL,
    source                  text NOT NULL CHECK (source IN ('github','api')),
    status                  text NOT NULL CHECK (status IN ('pending','complete','failed')),
    uploader                text NOT NULL,
    dedupe_key              text NOT NULL,
    storage_key             text NOT NULL,
    size_bytes              bigint NOT NULL,
    raw_size_bytes          bigint NOT NULL,
    finding_count           bigint NOT NULL,
    errors                  text,
    checkout_uri            text,
    analysis_key            text,
    environment             text,
    started_at              text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    completed_at            timestamptz,
    UNIQUE (run_id, dedupe_key)
);
CREATE INDEX sarif_analyses_run ON sarif_analyses(run_id, created_at DESC);
CREATE INDEX sarif_analyses_repository ON sarif_analyses(repository, commit_sha);

CREATE TABLE sarif_findings (
    analysis_id             text NOT NULL,
    fingerprint             text NOT NULL,
    tool_name               text NOT NULL,
    rule_id                 text,
    rule_name               text,
    level                   text NOT NULL,
    message                 text NOT NULL,
    path                    text,
    start_line              bigint,
    end_line                bigint,
    start_column            bigint,
    end_column              bigint,
    partial_fingerprint     text,
    suppression             text,
    PRIMARY KEY (analysis_id, fingerprint)
);
CREATE INDEX sarif_findings_path ON sarif_findings(analysis_id, path, start_line);
