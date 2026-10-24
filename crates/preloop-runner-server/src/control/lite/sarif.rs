//! Code-scanning SARIF analyses (`sarif_analyses` / `sarif_findings`).
//!
//! The raw SARIF document lives in the run-scoped file store; these tables
//! hold the analysis metadata and the normalized findings. An upload is
//! idempotent on `(run_id, dedupe_key)` — the content fingerprint the handler
//! computes — so re-uploading the same tool/commit/ref/findings set returns
//! the original row instead of duplicating it.

use super::codec::{self, now_us};
use super::{LiteBackend, db};
use crate::control::types::*;
use preloop_gha_protocol::RunId;
use rusqlite::{OptionalExtension, Row, params};

/// Columns of [`SarifAnalysisRecord`], in `analysis_from_row` order.
const ANALYSIS_COLUMNS: &str = "analysis_id, run_id, repository, commit_sha, git_ref, \
     tool_name, source, status, uploader, dedupe_key, storage_key, size_bytes, \
     raw_size_bytes, finding_count, errors, checkout_uri, analysis_key, environment, \
     started_at, created_at, completed_at";

fn analysis_from_row(row: &Row<'_>) -> Result<SarifAnalysisRecord, ControlError> {
    let source: String = row.get(6).map_err(db)?;
    let status: String = row.get(7).map_err(db)?;
    Ok(SarifAnalysisRecord {
        analysis_id: row.get(0).map_err(db)?,
        run_id: codec::run_id(&row.get::<_, String>(1).map_err(db)?),
        repository: row.get(2).map_err(db)?,
        commit_sha: row.get(3).map_err(db)?,
        git_ref: row.get(4).map_err(db)?,
        tool_name: row.get(5).map_err(db)?,
        source: SarifAnalysisSource::parse(&source).ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!("invalid sarif analysis source: {source}"))
        })?,
        status: SarifAnalysisStatus::parse(&status).ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!("invalid sarif analysis status: {status}"))
        })?,
        uploader: row.get(8).map_err(db)?,
        dedupe_key: row.get(9).map_err(db)?,
        storage_key: row.get(10).map_err(db)?,
        size_bytes: row.get(11).map_err(db)?,
        raw_size_bytes: row.get(12).map_err(db)?,
        finding_count: row.get(13).map_err(db)?,
        errors: row.get(14).map_err(db)?,
        checkout_uri: row.get(15).map_err(db)?,
        analysis_key: row.get(16).map_err(db)?,
        environment: row.get(17).map_err(db)?,
        started_at: row.get(18).map_err(db)?,
        created_at_us: row.get(19).map_err(db)?,
        completed_at_us: row.get(20).map_err(db)?,
    })
}

const FINDING_COLUMNS: &str = "fingerprint, tool_name, rule_id, rule_name, level, message, \
     path, start_line, end_line, start_column, end_column, partial_fingerprint, suppression";

fn finding_from_row(row: &Row<'_>) -> Result<SarifFinding, ControlError> {
    Ok(SarifFinding {
        fingerprint: row.get(0).map_err(db)?,
        tool_name: row.get(1).map_err(db)?,
        rule_id: row.get(2).map_err(db)?,
        rule_name: row.get(3).map_err(db)?,
        level: row.get(4).map_err(db)?,
        message: row.get(5).map_err(db)?,
        path: row.get(6).map_err(db)?,
        start_line: row.get(7).map_err(db)?,
        end_line: row.get(8).map_err(db)?,
        start_column: row.get(9).map_err(db)?,
        end_column: row.get(10).map_err(db)?,
        partial_fingerprint: row.get(11).map_err(db)?,
        suppression: row.get(12).map_err(db)?,
    })
}

impl LiteBackend {
    /// Insert one analysis with its findings unless `(run_id, dedupe_key)`
    /// already exists; see [`crate::control::backend::ControlBackend::put_sarif_analysis`].
    pub(crate) async fn put_sarif_analysis(
        &self,
        analysis: NewSarifAnalysis,
        findings: Vec<SarifFinding>,
    ) -> Result<(SarifAnalysisRecord, bool), ControlError> {
        self.write(move |tx| {
            let inserted = tx
                .prepare_cached(
                    "INSERT INTO sarif_analyses (analysis_id, namespace_id, run_id, \
                         repository, commit_sha, git_ref, tool_name, source, status, uploader, \
                         dedupe_key, storage_key, size_bytes, raw_size_bytes, finding_count, \
                         errors, checkout_uri, analysis_key, environment, started_at, \
                         created_at, completed_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'complete', ?9, ?10, ?11, ?12, \
                         ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?20) \
                     ON CONFLICT (run_id, dedupe_key) DO NOTHING",
                )
                .map_err(db)?
                .execute(params![
                    analysis.analysis_id,
                    DEFAULT_NAMESPACE,
                    codec::run_key(analysis.run_id),
                    analysis.repository,
                    analysis.commit_sha,
                    analysis.git_ref,
                    analysis.tool_name,
                    analysis.source.as_str(),
                    analysis.uploader,
                    analysis.dedupe_key,
                    analysis.storage_key,
                    analysis.size_bytes,
                    analysis.raw_size_bytes,
                    analysis.finding_count,
                    analysis.errors,
                    analysis.checkout_uri,
                    analysis.analysis_key,
                    analysis.environment,
                    analysis.started_at,
                    now_us(),
                ])
                .map_err(db)?
                == 1;
            if inserted {
                let mut statement = tx
                    .prepare_cached(
                        "INSERT OR IGNORE INTO sarif_findings (analysis_id, fingerprint, \
                             tool_name, rule_id, rule_name, level, message, path, start_line, \
                             end_line, start_column, end_column, partial_fingerprint, suppression) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    )
                    .map_err(db)?;
                for finding in &findings {
                    statement
                        .execute(params![
                            analysis.analysis_id,
                            finding.fingerprint,
                            finding.tool_name,
                            finding.rule_id,
                            finding.rule_name,
                            finding.level,
                            finding.message,
                            finding.path,
                            finding.start_line,
                            finding.end_line,
                            finding.start_column,
                            finding.end_column,
                            finding.partial_fingerprint,
                            finding.suppression,
                        ])
                        .map_err(db)?;
                }
            }
            let record = tx
                .prepare_cached(&format!(
                    "SELECT {ANALYSIS_COLUMNS} FROM sarif_analyses \
                     WHERE run_id = ?1 AND dedupe_key = ?2"
                ))
                .map_err(db)?
                .query_row(
                    params![codec::run_key(analysis.run_id), analysis.dedupe_key],
                    |row| Ok(analysis_from_row(row)),
                )
                .map_err(db)?;
            Ok((record?, inserted))
        })
    }

    /// One analysis by id.
    pub(crate) async fn sarif_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<SarifAnalysisRecord>, ControlError> {
        let analysis_id = analysis_id.to_owned();
        self.read(move |tx| {
            let row = tx
                .prepare_cached(&format!(
                    "SELECT {ANALYSIS_COLUMNS} FROM sarif_analyses WHERE analysis_id = ?1"
                ))
                .map_err(db)?
                .query_row([&analysis_id], |row| Ok(analysis_from_row(row)))
                .optional()
                .map_err(db)?;
            row.transpose()
        })
    }

    /// One analysis by its idempotency slot.
    pub(crate) async fn sarif_analysis_by_dedupe(
        &self,
        run_id: RunId,
        dedupe_key: &str,
    ) -> Result<Option<SarifAnalysisRecord>, ControlError> {
        let run = codec::run_key(run_id);
        let dedupe_key = dedupe_key.to_owned();
        self.read(move |tx| {
            let row = tx
                .prepare_cached(&format!(
                    "SELECT {ANALYSIS_COLUMNS} FROM sarif_analyses \
                     WHERE run_id = ?1 AND dedupe_key = ?2"
                ))
                .map_err(db)?
                .query_row(params![run, dedupe_key], |row| Ok(analysis_from_row(row)))
                .optional()
                .map_err(db)?;
            row.transpose()
        })
    }

    /// A run's analyses, newest first.
    pub(crate) async fn sarif_analyses(
        &self,
        run_id: RunId,
    ) -> Result<Vec<SarifAnalysisRecord>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(move |tx| {
            let mut statement = tx
                .prepare_cached(&format!(
                    "SELECT {ANALYSIS_COLUMNS} FROM sarif_analyses \
                     WHERE run_id = ?1 ORDER BY created_at DESC, analysis_id"
                ))
                .map_err(db)?;
            let rows = statement
                .query_map([&run], |row| Ok(analysis_from_row(row)))
                .map_err(db)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(db)?;
            rows.into_iter().collect()
        })
    }

    /// Findings of one analysis in a stable render order.
    pub(crate) async fn sarif_findings(
        &self,
        analysis_id: &str,
    ) -> Result<Vec<SarifFinding>, ControlError> {
        let analysis_id = analysis_id.to_owned();
        self.read(move |tx| {
            let mut statement = tx
                .prepare_cached(&format!(
                    "SELECT {FINDING_COLUMNS} FROM sarif_findings WHERE analysis_id = ?1 \
                     ORDER BY COALESCE(path, ''), COALESCE(start_line, 0), fingerprint"
                ))
                .map_err(db)?;
            let rows = statement
                .query_map([&analysis_id], |row| Ok(finding_from_row(row)))
                .map_err(db)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(db)?;
            rows.into_iter().collect()
        })
    }
}
