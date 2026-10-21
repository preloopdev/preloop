//! Postgres twin of `lite/token_revocation.rs`.
//!
//! Claiming is `SELECT … FOR UPDATE OF r SKIP LOCKED` so concurrent engine
//! nodes never attempt the same revocation in parallel; the row's
//! `next_attempt_at` becomes the retry/lease deadline before the transaction
//! commits.

use super::codec::{self, ts, us};
use super::{PgBackend, db};
use crate::control::types::*;

const TERMINAL_JOB_STATUSES: &str = "('success','failure','cancelled','skipped','timed_out')";

/// `auth::job_is_live`'s renewal bound: a lease not renewed for 180s belongs
/// to a dead or wedged worker even though `job_leases.expires_at` (45m) is
/// still in the future.
const HUNG_WORKER_LEASE_US: i64 =
    (crate::distributed_task::HUNG_WORKER_LEASE_SECONDS as i64) * 1_000_000;

impl PgBackend {
    pub(super) async fn record_github_token_revocation(
        &self,
        revocation: NewGitHubTokenRevocation,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                &format!(
                    "INSERT INTO github_token_revocations \
                     (token_hash, request_id, run_id, job_id, agent_job_id, app_id, \
                      installation_id, repository, sealed_token, expires_at, attempts, \
                      next_attempt_at, last_error, created_at) \
                     VALUES ($1, $2, $3::text::uuid, $4, $5::text::uuid, $6, $7, $8, $9, \
                             {}, 0, {}, NULL, {}) \
                     ON CONFLICT (token_hash) DO NOTHING",
                    ts!("$10"),
                    ts!("$11"),
                    ts!("$12"),
                ),
                &[
                    &revocation.token_hash,
                    &revocation.request_id,
                    &revocation.run_id.0.to_string(),
                    &revocation.job_id.0,
                    &revocation.agent_job_id.to_string(),
                    &revocation.app_id,
                    &(revocation.installation_id.min(i64::MAX as u64) as i64),
                    &revocation.repository,
                    &revocation.sealed_token,
                    &revocation.expires_at_us,
                    &revocation.next_attempt_at_us,
                    &revocation.created_at_us,
                ],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    pub(super) async fn due_github_token_revocations(
        &self,
        now_us: i64,
        limit: usize,
        claim_lease_us: i64,
    ) -> Result<Vec<GitHubTokenRevocation>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let rows = tx
            .query(
                &format!(
                    "SELECT r.token_hash, r.request_id, r.run_id::text, r.job_id, \
                            r.agent_job_id::text, r.app_id, r.installation_id, r.repository, \
                            r.sealed_token, {}, r.attempts, r.last_error \
                     FROM github_token_revocations r \
                     LEFT JOIN job_requests q ON q.request_id = r.request_id \
                     LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                     LEFT JOIN job_leases l ON l.request_id = q.request_id \
                     WHERE r.state = 'pending' \
                       AND r.next_attempt_at <= {} \
                       AND (r.expires_at <= {} \
                            OR r.request_id IS NULL \
                            OR q.request_id IS NULL \
                            OR q.agent_job_id::text <> r.agent_job_id::text \
                            OR q.result IS NOT NULL \
                            OR q.timeout_triggered \
                            OR j.status IN {TERMINAL_JOB_STATUSES} \
                            OR (l.renewed_at IS NOT NULL \
                                AND {} <= $1 - {HUNG_WORKER_LEASE_US}) \
                            OR (l.renewed_at IS NULL \
                                AND l.expires_at IS NOT NULL \
                                AND {} <= $1)) \
                     ORDER BY r.next_attempt_at, r.token_hash LIMIT $2 \
                     FOR UPDATE OF r SKIP LOCKED",
                    us!("r.expires_at"),
                    ts!("$1"),
                    ts!("$1"),
                    us!("l.renewed_at"),
                    us!("l.expires_at"),
                ),
                &[&now_us, &(limit.min(i64::MAX as usize) as i64)],
            )
            .await
            .map_err(db)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in &rows {
            let token_hash: String = row.get(0);
            claimed.push(GitHubTokenRevocation {
                token_hash: token_hash.clone(),
                request_id: row.get(1),
                run_id: codec::run_id(&row.get::<_, String>(2))?,
                job_id: preloop_gha_protocol::JobId(row.get(3)),
                agent_job_id: codec::uuid(&row.get::<_, String>(4))?,
                app_id: row.get(5),
                installation_id: u64::try_from(row.get::<_, i64>(6)).unwrap_or(0),
                repository: row.get(7),
                sealed_token: row.get(8),
                expires_at_us: row.get(9),
                attempts: u32::try_from(row.get::<_, i32>(10)).unwrap_or(u32::MAX),
                last_error: row.get(11),
            });
            tx.execute(
                &format!(
                    "UPDATE github_token_revocations SET next_attempt_at = {} \
                     WHERE token_hash = $1",
                    ts!("$2")
                ),
                &[&token_hash, &now_us.saturating_add(claim_lease_us)],
            )
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(claimed)
    }

    pub(super) async fn settle_github_token_revocation(
        &self,
        token_hash: &str,
        resolution: TokenRevocationResolution,
    ) -> Result<(), ControlError> {
        let state = match resolution {
            TokenRevocationResolution::Revoked => "revoked",
            TokenRevocationResolution::Spent => "spent",
        };
        let client = self.writer().await?;
        // Keep the row as a tombstone so dispatch authentication rejects the
        // token before any online/cache fallback; the credential itself is no
        // longer needed.
        client
            .execute(
                "UPDATE github_token_revocations \
                 SET state = $2, sealed_token = NULL, last_error = NULL \
                 WHERE token_hash = $1",
                &[&token_hash, &state],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    pub(super) async fn expire_github_token_revocations(
        &self,
        cleanup_before_us: i64,
    ) -> Result<usize, ControlError> {
        let client = self.writer().await?;
        let removed = client
            .execute(
                &format!(
                    "DELETE FROM github_token_revocations WHERE expires_at <= {}",
                    ts!("$1")
                ),
                &[&cleanup_before_us],
            )
            .await
            .map_err(db)?;
        Ok(usize::try_from(removed).unwrap_or(usize::MAX))
    }

    pub(super) async fn job_token_dispatchable(
        &self,
        token_hash: &str,
        now_us: i64,
    ) -> Result<Option<bool>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT r.state, \
                            CASE WHEN r.expires_at <= {} THEN 1 \
                                 WHEN q.request_id IS NULL THEN 1 \
                                 WHEN q.agent_job_id::text <> r.agent_job_id::text THEN 1 \
                                 WHEN q.result IS NOT NULL THEN 1 \
                                 WHEN q.timeout_triggered THEN 1 \
                                 WHEN j.status IN {TERMINAL_JOB_STATUSES} THEN 1 \
                                 WHEN l.renewed_at IS NOT NULL \
                                      AND {} <= $2 - {HUNG_WORKER_LEASE_US} THEN 1 \
                                 WHEN l.renewed_at IS NULL \
                                      AND l.expires_at IS NOT NULL \
                                      AND {} <= $2 THEN 1 \
                                 ELSE 0 END \
                     FROM github_token_revocations r \
                     LEFT JOIN job_requests q ON q.request_id = r.request_id \
                     LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                     LEFT JOIN job_leases l ON l.request_id = q.request_id \
                     WHERE r.token_hash = $1",
                    ts!("$2"),
                    us!("l.renewed_at"),
                    us!("l.expires_at"),
                ),
                &[&token_hash, &now_us],
            )
            .await
            .map_err(db)?;
        Ok(row.map(|row| {
            let state: String = row.get(0);
            let dead: i32 = row.get(1);
            state == "pending" && dead == 0
        }))
    }

    pub(super) async fn retry_github_token_revocation(
        &self,
        token_hash: &str,
        error: &str,
        next_attempt_at_us: i64,
    ) -> Result<(), ControlError> {
        let error = truncate_error(error);
        let client = self.writer().await?;
        client
            .execute(
                &format!(
                    "UPDATE github_token_revocations \
                     SET attempts = attempts + 1, last_error = $2, next_attempt_at = {} \
                     WHERE token_hash = $1",
                    ts!("$3")
                ),
                &[&token_hash, &error, &next_attempt_at_us],
            )
            .await
            .map_err(db)?;
        Ok(())
    }
}

/// Bound stored error text; repeated failures must not grow the row without
/// limit. The message never contains the token.
fn truncate_error(error: &str) -> String {
    error.chars().take(512).collect()
}
