//! Durable ledger of job-owned GitHub App installation tokens awaiting remote
//! revocation.
//!
//! The token itself must authenticate its own `DELETE /installation/token`
//! call, so the row keeps `store::Envelope` ciphertext keyed by the token
//! hash. Rows have no FK to `job_requests`: archiving or purging an attempt
//! must not erase the revocation obligation. The reconciler
//! (`github_token_revocation::reconcile_at`) deletes a row once GitHub
//! confirms the revoke, the token is already invalid (`401`), or the token's
//! own `expires_at` has passed.

use super::codec;
use super::{LiteBackend, db};
use crate::control::types::*;
use rusqlite::{OptionalExtension, params};

/// Terminal `jobs.status` values: a token whose logical job reached one of
/// these no longer needs to stay valid for workflow code.
const TERMINAL_JOB_STATUSES: &str = "('success','failure','cancelled','skipped','timed_out')";

/// `auth::job_is_live`'s renewal bound: a lease not renewed for 180s belongs
/// to a dead or wedged worker even though `job_leases.expires_at` (45m) is
/// still in the future.
const HUNG_WORKER_LEASE_US: i64 =
    (crate::distributed_task::HUNG_WORKER_LEASE_SECONDS as i64) * 1_000_000;

impl LiteBackend {
    pub(super) async fn record_github_token_revocation(
        &self,
        revocation: NewGitHubTokenRevocation,
    ) -> Result<(), ControlError> {
        let token_hash = revocation.token_hash;
        let request_id = revocation.request_id;
        let run_id = codec::run_key(revocation.run_id);
        let job_id = revocation.job_id.0;
        let agent_job_id = revocation.agent_job_id.to_string();
        let app_id = revocation.app_id;
        let installation_id = i64::try_from(revocation.installation_id).unwrap_or(i64::MAX);
        let repository = revocation.repository;
        let sealed_token = revocation.sealed_token;
        let expires_at_us = revocation.expires_at_us;
        let next_attempt_at_us = revocation.next_attempt_at_us;
        let created_at_us = revocation.created_at_us;
        self.write(move |tx| {
            tx.prepare_cached(
                "INSERT INTO github_token_revocations \
                 (token_hash, request_id, run_id, job_id, agent_job_id, app_id, \
                  installation_id, repository, sealed_token, expires_at, attempts, \
                  next_attempt_at, last_error, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11, NULL, ?12) \
                 ON CONFLICT(token_hash) DO NOTHING",
            )
            .map_err(db)?
            .execute(params![
                token_hash,
                request_id,
                run_id,
                job_id,
                agent_job_id,
                app_id,
                installation_id,
                repository,
                sealed_token,
                expires_at_us,
                next_attempt_at_us,
                created_at_us,
            ])
            .map_err(db)?;
            Ok(())
        })
    }

    pub(super) async fn due_github_token_revocations(
        &self,
        now_us: i64,
        limit: usize,
        claim_lease_us: i64,
    ) -> Result<Vec<GitHubTokenRevocation>, ControlError> {
        self.write(move |tx| {
            let rows = {
                let mut stmt = tx
                    .prepare_cached(&format!(
                        "SELECT r.token_hash, r.request_id, r.run_id, r.job_id, \
                                r.agent_job_id, r.app_id, r.installation_id, r.repository, \
                                r.sealed_token, r.expires_at, r.attempts, r.last_error \
                         FROM github_token_revocations r \
                         LEFT JOIN job_requests q ON q.request_id = r.request_id \
                         LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                         LEFT JOIN job_leases l ON l.request_id = q.request_id \
                         WHERE r.state = 'pending' \
                           AND r.next_attempt_at <= ?1 \
                           AND (r.expires_at <= ?1 \
                                OR r.request_id IS NULL \
                                OR q.request_id IS NULL \
                                OR q.agent_job_id <> r.agent_job_id \
                                OR q.result IS NOT NULL \
                                OR q.timeout_triggered <> 0 \
                                OR j.status IN {TERMINAL_JOB_STATUSES} \
                                OR (l.renewed_at IS NOT NULL \
                                    AND l.renewed_at <= ?1 - {HUNG_WORKER_LEASE_US}) \
                                OR (l.renewed_at IS NULL \
                                    AND l.expires_at IS NOT NULL \
                                    AND l.expires_at <= ?1)) \
                         ORDER BY r.next_attempt_at, r.token_hash LIMIT ?2"
                    ))
                    .map_err(db)?;
                let rows = stmt
                    .query_map(
                        params![now_us, limit.min(i64::MAX as usize) as i64],
                        |row| {
                            Ok(GitHubTokenRevocation {
                                token_hash: row.get(0)?,
                                request_id: row.get(1)?,
                                run_id: codec::run_id(&row.get::<_, String>(2)?),
                                job_id: preloop_gha_protocol::JobId(row.get(3)?),
                                agent_job_id: codec::uuid(&row.get::<_, String>(4)?),
                                app_id: row.get(5)?,
                                installation_id: u64::try_from(row.get::<_, i64>(6)?).unwrap_or(0),
                                repository: row.get(7)?,
                                sealed_token: row.get(8)?,
                                expires_at_us: row.get(9)?,
                                attempts: u32::try_from(row.get::<_, i64>(10)?).unwrap_or(u32::MAX),
                                last_error: row.get(11)?,
                            })
                        },
                    )
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            // Claim each row for this pass: a crash after this commit retries
            // after the lease instead of losing the obligation.
            for row in &rows {
                tx.prepare_cached(
                    "UPDATE github_token_revocations SET next_attempt_at = ?2 \
                     WHERE token_hash = ?1",
                )
                .map_err(db)?
                .execute(params![
                    row.token_hash,
                    now_us.saturating_add(claim_lease_us)
                ])
                .map_err(db)?;
            }
            Ok(rows)
        })
    }

    pub(super) async fn settle_github_token_revocation(
        &self,
        token_hash: &str,
        resolution: TokenRevocationResolution,
    ) -> Result<(), ControlError> {
        let token_hash = token_hash.to_owned();
        let state = match resolution {
            TokenRevocationResolution::Revoked => "revoked",
            TokenRevocationResolution::Spent => "spent",
        };
        self.write(move |tx| {
            // Keep the row as a tombstone: dispatch authentication must reject
            // the token even if a 60s online-validation cache still holds it.
            // The sealed credential is no longer needed.
            tx.prepare_cached(
                "UPDATE github_token_revocations \
                 SET state = ?2, sealed_token = NULL, last_error = NULL \
                 WHERE token_hash = ?1",
            )
            .map_err(db)?
            .execute(params![token_hash, state])
            .map_err(db)?;
            Ok(())
        })
    }

    pub(super) async fn expire_github_token_revocations(
        &self,
        cleanup_before_us: i64,
    ) -> Result<usize, ControlError> {
        self.write(move |tx| {
            tx.prepare_cached("DELETE FROM github_token_revocations WHERE expires_at <= ?1")
                .map_err(db)?
                .execute([cleanup_before_us])
                .map_err(db)
        })
    }

    pub(super) async fn job_token_dispatchable(
        &self,
        token_hash: &str,
        now_us: i64,
    ) -> Result<Option<bool>, ControlError> {
        let token_hash = token_hash.to_owned();
        self.read(move |tx| {
            let row: Option<(String, i64)> = tx
                .prepare_cached(&format!(
                    "SELECT r.state, \
                            CASE WHEN r.expires_at <= ?2 THEN 1 \
                                 WHEN q.request_id IS NULL THEN 1 \
                                 WHEN q.agent_job_id <> r.agent_job_id THEN 1 \
                                 WHEN q.result IS NOT NULL THEN 1 \
                                 WHEN q.timeout_triggered <> 0 THEN 1 \
                                 WHEN j.status IN {TERMINAL_JOB_STATUSES} THEN 1 \
                                 WHEN l.renewed_at IS NOT NULL \
                                      AND l.renewed_at <= ?2 - {HUNG_WORKER_LEASE_US} THEN 1 \
                                 WHEN l.renewed_at IS NULL \
                                      AND l.expires_at IS NOT NULL \
                                      AND l.expires_at <= ?2 THEN 1 \
                                 ELSE 0 END \
                     FROM github_token_revocations r \
                     LEFT JOIN job_requests q ON q.request_id = r.request_id \
                     LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                     LEFT JOIN job_leases l ON l.request_id = q.request_id \
                     WHERE r.token_hash = ?1"
                ))
                .map_err(db)?
                .query_row(params![token_hash, now_us], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()
                .map_err(db)?;
            Ok(row.map(|(state, dead)| state == "pending" && dead == 0))
        })
    }

    pub(super) async fn retry_github_token_revocation(
        &self,
        token_hash: &str,
        error: &str,
        next_attempt_at_us: i64,
    ) -> Result<(), ControlError> {
        let token_hash = token_hash.to_owned();
        let error = truncate_error(error);
        self.write(move |tx| {
            tx.prepare_cached(
                "UPDATE github_token_revocations \
                 SET attempts = attempts + 1, last_error = ?2, next_attempt_at = ?3 \
                 WHERE token_hash = ?1",
            )
            .map_err(db)?
            .execute(params![token_hash, error, next_attempt_at_us])
            .map_err(db)?;
            Ok(())
        })
    }
}

/// Bound stored error text; a repeated failure must not grow the row without
/// limit. The message comes from an HTTP status or transport error and never
/// contains the token.
fn truncate_error(error: &str) -> String {
    error.chars().take(512).collect()
}
