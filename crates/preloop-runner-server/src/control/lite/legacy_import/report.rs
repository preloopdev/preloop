//! Operator-facing import report.
//!
//! The report is the contract that nothing was dropped quietly: every legacy
//! family the importer does not carry is listed with its row count and a
//! reason, and the CLI exits non-zero on a refused import without writing a
//! target.

use serde::Serialize;
use std::path::PathBuf;

/// What to do with work that was in flight when the legacy store stopped:
/// claimed attempts (a runner may still be executing them) and the
/// concurrency holds those attempts were admitted under.
///
/// `Refuse` is the default so an operator cannot silently double-run or
/// silently release a deployment gate by importing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ActivePolicy {
    /// Abort the import and list the active items.
    Refuse,
    /// Release claims/holds and requeue the affected jobs (explicit drain).
    Requeue,
    /// Settle the affected attempts and jobs as cancelled (explicit drain).
    Cancel,
}

impl ActivePolicy {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "refuse" => Ok(Self::Refuse),
            "requeue" => Ok(Self::Requeue),
            "cancel" => Ok(Self::Cancel),
            other => anyhow::bail!(
                "unknown --active policy {other:?}; expected refuse, requeue, or cancel"
            ),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Refuse => "refuse",
            Self::Requeue => "requeue",
            Self::Cancel => "cancel",
        }
    }
}

/// One family of legacy rows that was not carried into the target, with the
/// row count and why. Never empty for an import that touched a state family.
#[derive(Debug, Clone, Serialize)]
pub struct SkippedFamily {
    pub family: String,
    pub rows: u64,
    pub reason: String,
}

/// Work the importer refused to carry and could not leave for later (a live
/// claim, a concurrency hold). Only produced under `ActivePolicy::Refuse`,
/// which then aborts the import.
#[derive(Debug, Clone, Serialize)]
pub struct ActiveItem {
    pub kind: String,
    pub id: String,
    pub detail: String,
}

/// Counts of rows written to the target.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ImportedRows {
    pub runs: u64,
    pub jobs: u64,
    pub job_specs: u64,
    pub job_needs: u64,
    pub job_messages: u64,
    pub job_requests: u64,
    pub job_leases: u64,
    pub job_steps: u64,
    pub timelines: u64,
    pub timeline_records: u64,
    pub log_files: u64,
    pub log_bytes: u64,
    pub run_secrets: u64,
    pub runners: u64,
    pub runner_sessions: u64,
    pub webhook_deliveries: u64,
    pub webhook_watchdog_cursors: u64,
    pub webhook_redeliveries: u64,
    pub workflow_run_numbers: u64,
    pub github_token_requests: u64,
    pub job_assignments: u64,
    pub job_cancellations: u64,
    pub run_push_states: u64,
    /// Legacy `control_events` rows carried into `outbox_events`.
    pub outbox_events: u64,
    /// Per-attempt job messages verified against their stored templates.
    pub job_request_messages: u64,
    /// Legacy artifact registry entries carried into `artifacts`.
    pub artifacts: u64,
    /// Legacy v1 artifact files copied into `<state-dir>/legacy-artifacts/`.
    pub legacy_artifact_bytes: u64,
}

/// The result of a completed import. Serializable for `--json`.
#[derive(Debug, Clone, Serialize)]
pub struct ImportReport {
    pub source: PathBuf,
    pub target: PathBuf,
    pub state_dir: PathBuf,
    /// Hex SHA-256 of the source before the import. Equal to
    /// `source_digest_after` or the import failed.
    pub source_digest_before: String,
    pub source_digest_after: String,
    pub legacy_user_version: i64,
    pub active_policy: ActivePolicy,
    pub imported: ImportedRows,
    /// Legacy rows explicitly not carried, each with a reason.
    pub skipped: Vec<SkippedFamily>,
    /// Free-form notes (drain decisions, derived fields).
    pub notes: Vec<String>,
}

impl ImportReport {
    pub fn render_human(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "imported legacy store {}\n  -> {}\n",
            self.source.display(),
            self.target.display()
        ));
        out.push_str(&format!(
            "  source: user_version={} sha256={}.. (unchanged: {})\n",
            self.legacy_user_version,
            &self.source_digest_before[..16.min(self.source_digest_before.len())],
            self.source_digest_before == self.source_digest_after
        ));
        out.push_str(&format!(
            "  runs={} jobs={} requests={} steps={} logs={} ({} bytes) secrets={}\n",
            self.imported.runs,
            self.imported.jobs,
            self.imported.job_requests,
            self.imported.job_steps,
            self.imported.log_files,
            self.imported.log_bytes,
            self.imported.run_secrets,
        ));
        out.push_str(&format!(
            "  runners={} sessions={} webhooks={} counters={}\n",
            self.imported.runners,
            self.imported.runner_sessions,
            self.imported.webhook_deliveries,
            self.imported.workflow_run_numbers,
        ));
        if !self.skipped.is_empty() {
            out.push_str("  not imported (explicit):\n");
            for skip in &self.skipped {
                out.push_str(&format!(
                    "    - {}: {} rows: {}\n",
                    skip.family, skip.rows, skip.reason
                ));
            }
        }
        for note in &self.notes {
            out.push_str(&format!("  note: {note}\n"));
        }
        out
    }
}
