-- SARIF analyses uploaded through the GitHub code-scanning shim
-- (`POST /api/v3/repos/{owner}/{repo}/code-scanning/sarifs`) or the general
-- run API (`POST /api/v1/runs/{run_id}/sarif`). The raw SARIF bytes live in
-- the run-scoped file store (`<state_dir>/sarif/`); this row carries the
-- metadata and the normalized findings a UI or check annotation reads.
CREATE TABLE sarif_analyses (
    analysis_id             TEXT PRIMARY KEY,
    namespace_id            TEXT NOT NULL DEFAULT 'default',
    run_id                  TEXT NOT NULL,
    repository              TEXT NOT NULL,
    commit_sha              TEXT NOT NULL,
    git_ref                 TEXT NOT NULL,
    tool_name               TEXT NOT NULL,
    source                  TEXT NOT NULL CHECK (source IN ('github','api')),
    status                  TEXT NOT NULL CHECK (status IN ('pending','complete','failed')),
    uploader                TEXT NOT NULL,
    dedupe_key              TEXT NOT NULL,
    storage_key             TEXT NOT NULL,
    size_bytes              INTEGER NOT NULL,
    raw_size_bytes          INTEGER NOT NULL,
    finding_count           INTEGER NOT NULL,
    errors                  TEXT,
    checkout_uri            TEXT,
    analysis_key            TEXT,
    environment             TEXT,
    started_at              TEXT,
    created_at              INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER)),
    completed_at            INTEGER,
    UNIQUE (run_id, dedupe_key)
);
CREATE INDEX sarif_analyses_run ON sarif_analyses(run_id, created_at DESC);
CREATE INDEX sarif_analyses_repository ON sarif_analyses(repository, commit_sha);

CREATE TABLE sarif_findings (
    analysis_id             TEXT NOT NULL,
    fingerprint             TEXT NOT NULL,
    tool_name               TEXT NOT NULL,
    rule_id                 TEXT,
    rule_name               TEXT,
    level                   TEXT NOT NULL,
    message                 TEXT NOT NULL,
    path                    TEXT,
    start_line              INTEGER,
    end_line                INTEGER,
    start_column            INTEGER,
    end_column              INTEGER,
    partial_fingerprint     TEXT,
    suppression             TEXT,
    PRIMARY KEY (analysis_id, fingerprint)
);
CREATE INDEX sarif_findings_path ON sarif_findings(analysis_id, path, start_line);
