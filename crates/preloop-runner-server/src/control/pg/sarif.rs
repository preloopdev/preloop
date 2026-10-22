//! Code-scanning SARIF analyses (`sarif_analyses` / `sarif_findings`).
//!
//! Mirror of `control/lite/sarif.rs`: an upload is idempotent on
//! `(run_id, dedupe_key)`, findings are keyed by `(analysis_id, fingerprint)`,
//! and the raw document lives in the run-scoped file store addressed by
//! `storage_key`.

use super::codec::{self, us};
use super::{PgBackend, db};
use crate::control::types::*;
use preloop_gha_protocol::RunId;

/// Columns of [`SarifAnalysisRecord`], in `analysis_from_row` order.
fn analysis_columns() -> String {
    format!(
        "analysis_id, run_id::text, repository, commit_sha, git_ref, tool_name, source, \
         status, uploader, dedupe_key, storage_key, size_bytes, raw_size_bytes, \
         finding_count, errors, checkout_uri, analysis_key, environment, started_at, {}, {}",
        us!("created_at"),
        us!("completed_at")
    )
}

fn analysis_from_row(row: &tokio_postgres::Row) -> Result<SarifAnalysisRecord, ControlError> {
    let source: String = row.get(6);
    let status: String = row.get(7);
    Ok(SarifAnalysisRecord {
        analysis_id: row.get(0),
        run_id: codec::run_id(row.get::<_, &str>(1))?,
        repository: row.get(2),
        commit_sha: row.get(3),
        git_ref: row.get(4),
        tool_name: row.get(5),
        source: SarifAnalysisSource::parse(&source).ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!("invalid sarif analysis source: {source}"))
        })?,
        status: SarifAnalysisStatus::parse(&status).ok_or_else(|| {
            ControlError::backend(anyhow::anyhow!("invalid sarif analysis status: {status}"))
        })?,
        uploader: row.get(8),
        dedupe_key: row.get(9),
        storage_key: row.get(10),
        size_bytes: row.get(11),
        raw_size_bytes: row.get(12),
        finding_count: row.get(13),
        errors: row.get(14),
        checkout_uri: row.get(15),
        analysis_key: row.get(16),
        environment: row.get(17),
        started_at: row.get(18),
        created_at_us: row.get(19),
        completed_at_us: row.get(20),
    })
}

const FINDING_COLUMNS: &str = "fingerprint, tool_name, rule_id, rule_name, level, message, \
     path, start_line, end_line, start_column, end_column, partial_fingerprint, suppression";

fn finding_from_row(row: &tokio_postgres::Row) -> SarifFinding {
    SarifFinding {
        fingerprint: row.get(0),
        tool_name: row.get(1),
        rule_id: row.get(2),
        rule_name: row.get(3),
        level: row.get(4),
        message: row.get(5),
        path: row.get(6),
        start_line: row.get(7),
        end_line: row.get(8),
        start_column: row.get(9),
        end_column: row.get(10),
        partial_fingerprint: row.get(11),
        suppression: row.get(12),
    }
}

impl PgBackend {
    /// Insert one analysis with its findings unless `(run_id, dedupe_key)`
    /// already exists; see [`crate::control::backend::ControlBackend::put_sarif_analysis`].
    pub(super) async fn put_sarif_analysis(
        &self,
        analysis: NewSarifAnalysis,
        findings: Vec<SarifFinding>,
    ) -> Result<(SarifAnalysisRecord, bool), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let inserted = tx
            .execute(
                "INSERT INTO sarif_analyses (analysis_id, namespace_id, run_id, repository, \
                     commit_sha, git_ref, tool_name, source, status, uploader, dedupe_key, \
                     storage_key, size_bytes, raw_size_bytes, finding_count, errors, \
                     checkout_uri, analysis_key, environment, started_at, created_at, \
                     completed_at) \
                 VALUES ($1, 'default', $2::text::uuid, $3, $4, $5, $6, $7, 'complete', $8, \
                     $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, now(), now()) \
                 ON CONFLICT (run_id, dedupe_key) DO NOTHING",
                &[
                    &analysis.analysis_id,
                    &analysis.run_id.to_string(),
                    &analysis.repository,
                    &analysis.commit_sha,
                    &analysis.git_ref,
                    &analysis.tool_name,
                    &analysis.source.as_str(),
                    &analysis.uploader,
                    &analysis.dedupe_key,
                    &analysis.storage_key,
                    &analysis.size_bytes,
                    &analysis.raw_size_bytes,
                    &analysis.finding_count,
                    &analysis.errors,
                    &analysis.checkout_uri,
                    &analysis.analysis_key,
                    &analysis.environment,
                    &analysis.started_at,
                ],
            )
            .await
            .map_err(db)?
            == 1;
        if inserted {
            for finding in &findings {
                tx.execute(
                    "INSERT INTO sarif_findings (analysis_id, fingerprint, tool_name, rule_id, \
                         rule_name, level, message, path, start_line, end_line, start_column, \
                         end_column, partial_fingerprint, suppression) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
                     ON CONFLICT (analysis_id, fingerprint) DO NOTHING",
                    &[
                        &analysis.analysis_id,
                        &finding.fingerprint,
                        &finding.tool_name,
                        &finding.rule_id,
                        &finding.rule_name,
                        &finding.level,
                        &finding.message,
                        &finding.path,
                        &finding.start_line,
                        &finding.end_line,
                        &finding.start_column,
                        &finding.end_column,
                        &finding.partial_fingerprint,
                        &finding.suppression,
                    ],
                )
                .await
                .map_err(db)?;
            }
        }
        let row = tx
            .query_opt(
                &format!(
                    "SELECT {} FROM sarif_analyses WHERE run_id = $1::text::uuid \
                     AND dedupe_key = $2",
                    analysis_columns()
                ),
                &[&analysis.run_id.to_string(), &analysis.dedupe_key],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| ControlError::backend(anyhow::anyhow!("sarif analysis row vanished")))?;
        let record = analysis_from_row(&row)?;
        tx.commit().await.map_err(db)?;
        Ok((record, inserted))
    }

    /// One analysis by id.
    pub(super) async fn sarif_analysis(
        &self,
        analysis_id: &str,
    ) -> Result<Option<SarifAnalysisRecord>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {} FROM sarif_analyses WHERE analysis_id = $1",
                    analysis_columns()
                ),
                &[&analysis_id],
            )
            .await
            .map_err(db)?;
        row.map(|row| analysis_from_row(&row)).transpose()
    }

    /// One analysis by its idempotency slot.
    pub(super) async fn sarif_analysis_by_dedupe(
        &self,
        run_id: RunId,
        dedupe_key: &str,
    ) -> Result<Option<SarifAnalysisRecord>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT {} FROM sarif_analyses WHERE run_id = $1::text::uuid \
                     AND dedupe_key = $2",
                    analysis_columns()
                ),
                &[&run_id.to_string(), &dedupe_key],
            )
            .await
            .map_err(db)?;
        row.map(|row| analysis_from_row(&row)).transpose()
    }

    /// A run's analyses, newest first.
    pub(super) async fn sarif_analyses(
        &self,
        run_id: RunId,
    ) -> Result<Vec<SarifAnalysisRecord>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT {} FROM sarif_analyses WHERE run_id = $1::text::uuid \
                     ORDER BY created_at DESC, analysis_id",
                    analysis_columns()
                ),
                &[&run_id.to_string()],
            )
            .await
            .map_err(db)?;
        rows.iter().map(analysis_from_row).collect()
    }

    /// Findings of one analysis in a stable render order.
    pub(super) async fn sarif_findings(
        &self,
        analysis_id: &str,
    ) -> Result<Vec<SarifFinding>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query(
                &format!(
                    "SELECT {FINDING_COLUMNS} FROM sarif_findings WHERE analysis_id = $1 \
                     ORDER BY COALESCE(path, ''), COALESCE(start_line, 0), fingerprint"
                ),
                &[&analysis_id],
            )
            .await
            .map_err(db)?;
        Ok(rows.iter().map(finding_from_row).collect())
    }
}
