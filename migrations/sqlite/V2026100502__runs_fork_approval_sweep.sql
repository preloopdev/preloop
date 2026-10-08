-- Additive migration template: the partial index the fork-approval expiry
-- sweep (`runs WHERE fork_approval_pending = 1 AND fork_approval_requested_at < ?`)
-- scans. Additive and data-preserving: no column changes, no rewrite, no
-- backfill — existing rows are untouched. Copy this file as the shape for a
-- new migration (see docs/control-migrations.md), or `refinery new`.
CREATE INDEX runs_fork_approval_sweep ON runs(fork_approval_requested_at) WHERE fork_approval_pending = 1;
