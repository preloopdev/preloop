//! Acquire-path reads and the per-attempt message write: everything the
//! `acquirejob`/`refreshjob` handlers need, plus `oidc_grant` and the
//! claim-time `store_request_message` overwrite.
//!
//! Translated from `control/pg/lifecycle.rs` (`acquire_context`,
//! `acquire_for_runner`, `store_request_message`, `oidc_grant`). The run
//! lock `pg` takes on `store_request_message` is implicit in the single
//! writer.

use super::codec;
use super::requests;
use super::{LiteBackend, db, jobs};
use crate::control::types::*;
use preloop_gha_protocol::{JobId, RunId, azdo};
use rusqlite::{OptionalExtension, params};

impl LiteBackend {
    /// `acquire_context`: the request record, its job message, the deferred
    /// token-mint request, the recorded id-token grant, and the run's
    /// repository — one read, no locks.
    pub(crate) async fn acquire_context(
        &self,
        request_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        self.read(move |tx| {
            let select = format!(
                "SELECT {} FROM {} WHERE q.request_id = ?1",
                requests::RECORD_COLUMNS,
                requests::RECORD_FROM
            );
            let Some(request) = tx
                .prepare_cached(&select)
                .map_err(db)?
                .query_row([request_id], requests::record_row)
                .optional()
                .map_err(db)?
            else {
                return Err(ControlError::NotFound(format!("request {request_id}")));
            };
            let run = codec::run_key(request.run_id);
            let Some(template) = tx
                .prepare_cached(
                    "SELECT message_template FROM job_messages \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![run, request.job_id.0], |row| {
                    row.get::<_, String>(0)
                })
                .optional()
                .map_err(db)?
            else {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "request {request_id} has no message template"
                )));
            };
            let message: azdo::AgentJobRequestMessage =
                serde_json::from_str(&template).map_err(ControlError::backend)?;
            let token_request = tx
                .prepare_cached(
                    "SELECT repository, permissions, declared, untrusted \
                     FROM github_token_requests WHERE request_id = ?1",
                )
                .map_err(db)?
                .query_row([request_id], |row| {
                    Ok(crate::models::GitHubTokenRequest {
                        repository: row.get(0)?,
                        permissions: serde_json::from_str(&row.get::<_, String>(1)?)
                            .unwrap_or_default(),
                        declared: row.get::<_, i64>(2)? != 0,
                        untrusted: row.get::<_, i64>(3)? != 0,
                    })
                })
                .optional()
                .map_err(db)?;
            let id_token_granted = tx
                .prepare_cached(
                    "SELECT id_token_granted FROM job_specs \
                     WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![run, request.job_id.0], |row| row.get::<_, i64>(0))
                .optional()
                .map_err(db)?
                .map(|granted| granted != 0);
            let repository = tx
                .prepare_cached("SELECT repository FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .query_row([&run], |row| row.get::<_, String>(0))
                .optional()
                .map_err(db)?
                .unwrap_or_default();
            let trust_tier = tx
                .prepare_cached("SELECT submission FROM run_submissions WHERE run_id = ?1")
                .map_err(db)?
                .query_row([&run], |row| row.get::<_, String>(0))
                .optional()
                .map_err(db)?
                .and_then(|json| {
                    serde_json::from_str::<preloop_gha_protocol::WorkflowSubmission>(&json).ok()
                })
                .and_then(|submission| submission.trust_tier);
            Ok(AcquireContext {
                request,
                message,
                token_request,
                id_token_granted,
                repository,
                trust_tier,
            })
        })
    }

    /// `acquire_for_runner` = `acquire_context` + the ownership/liveness
    /// check: the runner must own the request (recorded owner or the
    /// claiming session's runner) and the attempt must not be settled.
    pub(crate) async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        let settled: Option<Option<String>> = self.read(move |tx| {
            tx.prepare_cached("SELECT result FROM job_requests WHERE request_id = ?1")
                .map_err(db)?
                .query_row([request_id], |row| row.get(0))
                .optional()
                .map_err(db)
        })?;
        let Some(result) = settled else {
            return Err(ControlError::NotFound(format!("request {request_id}")));
        };
        if result.is_some() {
            return Err(ControlError::Conflict(format!(
                "request {request_id} already settled"
            )));
        }
        let owner = self.request_owner(request_id).await?;
        let Some((owner, session_runner, has_session)) = owner else {
            return Err(ControlError::NotFound(format!("request {request_id}")));
        };
        crate::control::types::ensure_request_owner(owner, session_runner, has_session, runner_id)?;
        self.acquire_context(request_id).await
    }

    /// `record_token_request`: upsert the derived token-mint request for a
    /// re-claim. The stored template is never rewritten after claim (the
    /// minted message is secret-filled in memory only), so this is the whole
    /// method. pg locks the run row; the single writer covers it.
    pub(crate) async fn record_token_request(
        &self,
        _run_id: RunId,
        request_id: i64,
        token_request: &crate::models::GitHubTokenRequest,
    ) -> Result<(), ControlError> {
        let repository = token_request.repository.clone();
        let permissions =
            serde_json::to_string(&token_request.permissions).unwrap_or_else(|_| "{}".to_owned());
        let declared = token_request.declared as i64;
        let untrusted = token_request.untrusted as i64;
        self.write(move |tx| {
            tx.prepare_cached(
                "INSERT INTO github_token_requests (request_id, repository, \
                 permissions, declared, untrusted) VALUES (?1,?2,?3,?4,?5) \
                 ON CONFLICT (request_id) DO UPDATE SET \
                     repository = excluded.repository, \
                     permissions = excluded.permissions, \
                     declared = excluded.declared, \
                     untrusted = excluded.untrusted",
            )
            .map_err(db)?
            .execute(params![
                request_id,
                repository,
                permissions,
                declared,
                untrusted
            ])
            .map_err(db)?;
            Ok(())
        })
    }

    /// `oidc_grant`: the attempt's run + job's recorded id-token grant and
    /// OIDC context. `plan_id` names the attempt (the plan id is the
    /// request's `agent_job_id`).
    pub(crate) async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<OidcGrant, ControlError> {
        let plan_id = plan_id.to_owned();
        self.read(move |tx| {
            let row = tx
                .prepare_cached(
                    "SELECT q.run_id, q.job_id, s.id_token_granted, \
                            s.oidc_environment, s.oidc_job_workflow_ref, \
                            s.oidc_job_workflow_sha \
                     FROM job_requests q \
                     LEFT JOIN job_specs s ON s.run_id = q.run_id AND s.job_id = q.job_id \
                     WHERE q.agent_job_id = ?1",
                )
                .map_err(db)?
                .query_row([agent_job_id.to_string()], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            let Some((run, job_id, granted, oidc_env, oidc_ref, oidc_sha)) = row else {
                return Err(ControlError::NotFound(format!("plan {plan_id}")));
            };
            if granted.is_none() {
                return Err(ControlError::backend(anyhow::anyhow!(
                    "job {job_id} has no OIDC context"
                )));
            }
            let run_id = codec::run_id(&run);
            let record = jobs::run_record(tx, run_id)?
                .ok_or_else(|| ControlError::NotFound(format!("run {run_id}")))?;
            Ok(OidcGrant {
                run: record,
                job_id: JobId(job_id),
                granted: granted.unwrap_or(0) != 0,
                context: crate::state::OidcJobContext {
                    environment: oidc_env,
                    job_workflow_ref: oidc_ref,
                    job_workflow_sha: oidc_sha,
                },
            })
        })
    }
}
