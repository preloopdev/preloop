//! Execution attempts (`job_requests` + `job_leases`): lookups, lease
//! renewal, release and settlement bookkeeping, callback resolution.
//!
//! `plan_id` is not stored: it is the attempt's `agent_job_id` and
//! `plan_type` is always `"actions"`. A request is bound to a session by
//! `job_requests.session_id` while in flight; the binding counts only while
//! that session row exists.

use super::codec::{self, now_us};
use super::{LiteBackend, db};
use crate::control::backend::RequestKey;
use crate::control::types::*;
use crate::models::TaskAgentJobRequestRecord;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId, azdo};
use rusqlite::{OptionalExtension, params};
use std::collections::{BTreeMap, BTreeSet};

/// Columns of [`record_row`], from `job_requests q LEFT JOIN job_leases l`.
pub(super) const RECORD_COLUMNS: &str = "q.request_id, q.run_id, q.job_id, q.agent_job_id, \
     q.timeline_id, q.result, l.expires_at, q.claimed_at, q.runner_id, q.started_at, \
     l.renewed_at, q.timeout_triggered, q.debug_token_issued";

/// The FROM clause [`RECORD_COLUMNS`] selects over.
pub(super) const RECORD_FROM: &str =
    "job_requests q LEFT JOIN job_leases l ON l.request_id = q.request_id";

/// Decode a [`RECORD_COLUMNS`] row.
pub(super) fn record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskAgentJobRequestRecord> {
    let agent_job_id = codec::uuid(&row.get::<_, String>(3)?);
    let result: Option<String> = row.get(5)?;
    Ok(TaskAgentJobRequestRecord {
        request_id: row.get(0)?,
        run_id: codec::run_id(&row.get::<_, String>(1)?),
        job_id: JobId(row.get(2)?),
        agent_job_id,
        plan_id: codec::plan_id(&agent_job_id),
        plan_type: codec::PLAN_TYPE.to_owned(),
        timeline_id: codec::uuid(&row.get::<_, String>(4)?),
        result: result.as_deref().map(status_parse),
        locked_until: codec::lease_string(row.get(6)?),
        claimed_at: row.get::<_, Option<i64>>(7)?.map(codec::us_to_system),
        owner_runner_id: row.get(8)?,
        started_at: row.get::<_, Option<i64>>(9)?.map(codec::us_to_system),
        last_renewed_at: row.get::<_, Option<i64>>(10)?.map(codec::us_to_system),
        timeout_triggered: row.get(11)?,
        debug_token_issued: row.get(12)?,
    })
}

fn attempt_tuple(row: &rusqlite::Row<'_>) -> rusqlite::Result<(RunId, JobId, uuid::Uuid)> {
    Ok((
        codec::run_id(&row.get::<_, String>(0)?),
        JobId(row.get(1)?),
        codec::uuid(&row.get::<_, String>(2)?),
    ))
}

/// Upsert the attempt's lease to `expires_at` for its recorded owner.
/// No-op when the attempt has no owner (`job_leases.runner_id` is NOT NULL).
fn set_lease(
    tx: &rusqlite::Transaction<'_>,
    request_id: i64,
    expires_at: i64,
    now: i64,
) -> Result<(), ControlError> {
    tx.prepare_cached(
        "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
         SELECT request_id, runner_id, ?2, ?3 FROM job_requests \
         WHERE request_id = ?1 AND runner_id IS NOT NULL \
         ON CONFLICT (request_id) DO UPDATE SET \
             expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
    )
    .map_err(db)?
    .execute(params![request_id, expires_at, now])
    .map_err(db)?;
    Ok(())
}

/// Synchronous settle tail for the scheduling commands (`settle.rs`): the
/// async trait method wraps the same statement.
pub(super) fn stamp_result_row(
    tx: &rusqlite::Transaction<'_>,
    request_id: i64,
    result: ExecutionStatus,
    locked_until: &str,
) -> Result<(), ControlError> {
    stamp_result(tx, request_id, result, locked_until).map(|_| ())
}

/// The shared settle tail: stamp `result`/`finished_at` iff still in
/// flight and record the final lease expiry. Returns whether this call
/// settled the attempt (first result wins).
fn stamp_result(
    tx: &rusqlite::Transaction<'_>,
    request_id: i64,
    result: ExecutionStatus,
    locked_until: &str,
) -> Result<bool, ControlError> {
    let expires_at = codec::parse_lease(locked_until)?;
    let now = now_us();
    // `result` is CHECK-constrained to terminal verdicts; a non-terminal
    // retire (the reusable caller's placeholder request marked InProgress
    // while its subtree runs) only tightens the lease row.
    if !result.is_terminal() {
        set_lease(tx, request_id, expires_at, now)?;
        return Ok(true);
    }
    let settled = tx
        .prepare_cached(
            "UPDATE job_requests SET result = ?2, finished_at = ?3 \
             WHERE request_id = ?1 AND result IS NULL",
        )
        .map_err(db)?
        .execute(params![request_id, status_str(result), now])
        .map_err(db)?;
    if settled == 1 {
        set_lease(tx, request_id, expires_at, now)?;
    }
    Ok(settled == 1)
}

impl LiteBackend {
    /// A request record by id, plan id (= agent job id), agent job id,
    /// timeline id, or the latest attempt of a `(run, job)` pair.
    pub(crate) async fn request(
        &self,
        key: RequestKey,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        let not_found = || ControlError::NotFound("request".to_owned());
        let (filter, param): (&str, Vec<String>) = match &key {
            RequestKey::Id(id) => ("q.request_id = ?1", vec![id.to_string()]),
            RequestKey::PlanId(plan) => match codec::plan_agent_job_id(plan) {
                Some(agent) => ("q.agent_job_id = ?1", vec![agent.to_string()]),
                None => return Err(not_found()),
            },
            RequestKey::AgentJobId(id) => ("q.agent_job_id = ?1", vec![id.to_string()]),
            RequestKey::TimelineId(id) => ("q.timeline_id = ?1", vec![id.to_string()]),
            RequestKey::Job(run_id, job_id) => (
                "q.run_id = ?1 AND q.job_id = ?2",
                vec![codec::run_key(*run_id), job_id.0.clone()],
            ),
        };
        self.read(|tx| {
            tx.prepare_cached(&format!(
                "SELECT {RECORD_COLUMNS} FROM {RECORD_FROM} WHERE {filter} \
                 ORDER BY q.request_id DESC LIMIT 1"
            ))
            .map_err(db)?
            .query_row(rusqlite::params_from_iter(&param), record_row)
            .optional()
            .map_err(db)?
            .ok_or_else(not_found)
        })
    }

    /// The logical job an attempt belongs to.
    pub(crate) async fn attempt_job(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<(RunId, JobId)>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached("SELECT run_id, job_id FROM job_requests WHERE agent_job_id = ?1")
                .map_err(db)?
                .query_row([agent_job_id.to_string()], |row| {
                    Ok((codec::run_id(&row.get::<_, String>(0)?), JobId(row.get(1)?)))
                })
                .optional()
                .map_err(db)
        })
    }

    /// The run an attempt belongs to.
    pub(crate) async fn run_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<RunId>, ControlError> {
        Ok(self.attempt_job(agent_job_id).await?.map(|(run, _)| run))
    }

    /// Repository of the live run owning the attempt
    /// (`run_submissions.submission.repository`).
    pub(crate) async fn attempt_repository(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        let json = self.submission_json_for_attempt(agent_job_id).await?;
        json.map(|json| {
            serde_json::from_str::<serde_json::Value>(&json)
                .ok()
                .and_then(|v| v["repository"].as_str().map(str::to_owned))
                .ok_or_else(|| {
                    ControlError::backend(anyhow::anyhow!("submission missing repository"))
                })
        })
        .transpose()
    }

    /// The submission JSON of the run owning the attempt.
    pub(crate) async fn submission_json_for_attempt(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<Option<String>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT s.submission FROM job_requests q \
                 JOIN run_submissions s ON s.run_id = q.run_id \
                 WHERE q.agent_job_id = ?1",
            )
            .map_err(db)?
            .query_row([agent_job_id.to_string()], |row| row.get(0))
            .optional()
            .map_err(db)
        })
    }

    /// Whether `agent_job_id` is an attempt of `run_id` under `plan_id`
    /// (the plan id is the agent job id, so both must name it).
    pub(crate) async fn attempt_in_run(
        &self,
        run_id: RunId,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<bool, ControlError> {
        if codec::plan_agent_job_id(plan_id) != Some(agent_job_id) {
            return Ok(false);
        }
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM job_requests \
                 WHERE run_id = ?1 AND agent_job_id = ?2)",
            )
            .map_err(db)?
            .query_row(
                params![codec::run_key(run_id), agent_job_id.to_string()],
                |row| row.get(0),
            )
            .map_err(db)
        })
    }

    /// Resolve a runner callback to its attempt: plan id first, then
    /// timeline id, then agent job id; newest attempt wins within a match.
    pub(crate) async fn callback_job(
        &self,
        plan_id: &str,
        timeline_id: Option<uuid::Uuid>,
        agent_job_id: Option<uuid::Uuid>,
    ) -> Result<Option<CallbackJob>, ControlError> {
        let plan = codec::plan_agent_job_id(plan_id).map(|id| id.to_string());
        let timeline = timeline_id.map(|id| id.to_string());
        let agent = agent_job_id.map(|id| id.to_string());
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT q.request_id, q.run_id, q.job_id, q.agent_job_id, j.status \
                 FROM job_requests q \
                 LEFT JOIN jobs j ON j.run_id = q.run_id AND j.job_id = q.job_id \
                 WHERE q.agent_job_id = ?1 OR q.timeline_id = ?2 OR q.agent_job_id = ?3 \
                 ORDER BY (q.agent_job_id = ?1) DESC, (q.timeline_id = ?2) DESC, \
                          q.request_id DESC \
                 LIMIT 1",
            )
            .map_err(db)?
            .query_row(params![plan, timeline, agent], |row| {
                Ok(CallbackJob {
                    request_id: row.get(0)?,
                    run_id: codec::run_id(&row.get::<_, String>(1)?),
                    job_id: JobId(row.get(2)?),
                    agent_job_id: codec::uuid(&row.get::<_, String>(3)?),
                    job_status: row.get::<_, Option<String>>(4)?.map(|s| status_parse(&s)),
                })
            })
            .optional()
            .map_err(db)
        })
    }

    /// The one in-flight, session-bound attempt when exactly one exists.
    pub(crate) async fn sole_inflight_request(
        &self,
    ) -> Result<Option<(i64, RunId, JobId, uuid::Uuid)>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT q.request_id, q.run_id, q.job_id, q.agent_job_id \
                     FROM job_requests q JOIN runner_sessions s ON s.session_id = q.session_id \
                     WHERE q.result IS NULL LIMIT 2",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        codec::run_id(&row.get::<_, String>(1)?),
                        JobId(row.get(2)?),
                        codec::uuid(&row.get::<_, String>(3)?),
                    ))
                })
                .map_err(db)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db)?;
            Ok(match <[_; 1]>::try_from(rows) {
                Ok([only]) => Some(only),
                Err(_) => None,
            })
        })
    }

    /// Renew an owned in-flight attempt's lease in one conditional upsert.
    /// Misses are classified by [`renew_miss`]: unknown → `NotFound`,
    /// settled → `Conflict`, foreign → `Forbidden`, no recorded owner →
    /// `Ok(false)`.
    pub(crate) async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let lease = codec::parse_lease(locked_until);
        let agent = agent_job_id.to_string();
        self.write(|tx| {
            if let Ok(expires_at) = lease {
                let renewed = tx
                    .prepare_cached(
                        "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                         SELECT request_id, runner_id, ?3, ?4 FROM job_requests \
                         WHERE agent_job_id = ?1 AND result IS NULL AND runner_id = ?2 \
                         ON CONFLICT (request_id) DO UPDATE SET \
                             expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
                    )
                    .map_err(db)?
                    .execute(params![agent, runner_id, expires_at, now_us()])
                    .map_err(db)?;
                if renewed == 1 {
                    return Ok(true);
                }
            }
            let row = tx
                .prepare_cached(
                    "SELECT result, runner_id FROM job_requests WHERE agent_job_id = ?1",
                )
                .map_err(db)?
                .query_row([&agent], |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()
                .map_err(db)?;
            let owned_inflight = matches!(row, Some((None, Some(owner))) if owner == runner_id);
            let miss = renew_miss(row, runner_id)?;
            match lease {
                Err(error) if owned_inflight => Err(error),
                _ => Ok(miss),
            }
        })
    }

    /// `(recorded owner, owner-session runner, session-bound)` for a request.
    pub(crate) async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        self.read(|tx| {
            tx.prepare_cached(
                "SELECT q.runner_id, s.runner_id, s.session_id IS NOT NULL \
                 FROM job_requests q LEFT JOIN runner_sessions s ON s.session_id = q.session_id \
                 WHERE q.request_id = ?1",
            )
            .map_err(db)?
            .query_row([request_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .optional()
            .map_err(db)
        })
    }

    /// Broker-path renew: recorded owner → session owner → session-bound
    /// ladder, then renew, in one transaction.
    pub(crate) async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError> {
        let expires_at = codec::parse_lease(locked_until);
        self.write(|tx| {
            let row = tx
                .prepare_cached(
                    "SELECT q.request_id, q.runner_id, q.result IS NOT NULL, s.runner_id, \
                         s.session_id IS NOT NULL \
                     FROM job_requests q \
                     LEFT JOIN runner_sessions s ON s.session_id = q.session_id \
                     WHERE q.agent_job_id = ?1",
                )
                .map_err(db)?
                .query_row([agent_job_id.to_string()], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, bool>(4)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            let Some((request_id, owner, settled, session_runner, has_session)) = row else {
                return Err(ControlError::NotFound(
                    "broker renew request not found".to_owned(),
                ));
            };
            match owner.or(session_runner) {
                Some(owner) if owner != runner_id => {
                    return Err(ControlError::Forbidden(
                        "broker request belongs to another runner".to_owned(),
                    ));
                }
                None if !has_session => {
                    return Err(ControlError::NotFound(
                        "broker request is not assigned to a session".to_owned(),
                    ));
                }
                _ => {}
            }
            if settled {
                return Err(ControlError::Conflict(
                    "broker request already completed".to_owned(),
                ));
            }
            // The lease row is owned by the renewing runner: an attempt
            // accepted through the session ladder records it as owner.
            tx.prepare_cached(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (request_id) DO UPDATE SET \
                     expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
            )
            .map_err(db)?
            .execute(params![request_id, runner_id, expires_at?, now_us()])
            .map_err(db)?;
            Ok(())
        })
    }

    /// Unfinished attempts whose claiming session no longer exists (or
    /// whose session's runner is gone): owned by a runner, or bound to a
    /// session id, with no live session+runner behind the binding.
    pub(crate) async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT q.request_id, q.run_id, q.job_id FROM job_requests q \
                     WHERE q.result IS NULL \
                       AND (q.runner_id IS NOT NULL OR q.session_id IS NOT NULL) \
                       AND NOT EXISTS (SELECT 1 FROM runner_sessions s \
                                       JOIN runners r ON r.runner_id = s.runner_id \
                                       WHERE s.session_id = q.session_id) \
                     ORDER BY q.request_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get(0)?,
                        codec::run_id(&row.get::<_, String>(1)?),
                        JobId(row.get(2)?),
                    ))
                })
                .map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }

    /// Release a claimed attempt for redelivery: clear the session binding,
    /// owner, start stamp and timeout flag, and drop its lease (a lease
    /// needs an owner). `Ok(false)` when settled or unknown.
    pub(crate) async fn release_claimed_request(
        &self,
        request_id: i64,
        _locked_until: &str,
    ) -> Result<bool, ControlError> {
        self.write(|tx| {
            let released = tx
                .prepare_cached(
                    "UPDATE job_requests SET runner_id = NULL, session_id = NULL, \
                         started_at = NULL, timeout_triggered = 0 \
                     WHERE request_id = ?1 AND result IS NULL",
                )
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            if released == 0 {
                return Ok(false);
            }
            tx.prepare_cached("DELETE FROM job_leases WHERE request_id = ?1")
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            Ok(true)
        })
    }

    /// Settle an attempt once (first result wins), with the settle
    /// bookkeeping: drop the deferred token-mint request, drop undelivered
    /// cancellations for it (queued `job_cancellations` and the session's
    /// `JobCancellation` messages), clear the session binding, stamp
    /// `result`/`finished_at` and the final lease expiry. Returns the
    /// attempt's `(run_id, job_id, agent_job_id)` when it exists.
    pub(crate) async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        self.write(|tx| {
            let Some(attempt) = tx
                .prepare_cached(
                    "SELECT run_id, job_id, agent_job_id FROM job_requests WHERE request_id = ?1",
                )
                .map_err(db)?
                .query_row([request_id], attempt_tuple)
                .optional()
                .map_err(db)?
            else {
                return Ok(None);
            };
            tx.prepare_cached("DELETE FROM github_token_requests WHERE request_id = ?1")
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            tx.prepare_cached(
                "DELETE FROM job_cancellations WHERE request_id = ?1 AND delivered_at IS NULL",
            )
            .map_err(db)?
            .execute([request_id])
            .map_err(db)?;
            tx.prepare_cached(
                "DELETE FROM session_messages WHERE request_id = ?1 AND message_type = ?2",
            )
            .map_err(db)?
            .execute(params![request_id, azdo::message_type::JOB_CANCELLED])
            .map_err(db)?;
            tx.prepare_cached("UPDATE job_requests SET session_id = NULL WHERE request_id = ?1")
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            stamp_result(tx, request_id, result, locked_until)?;
            Ok(Some(attempt))
        })
    }

    /// Renew an in-flight AgentRequest's lease; `false` when settled,
    /// unknown, or holder-less (the PATCH contract renews nothing silently).
    /// The holder ladder is the recorded owner, else the claiming session's
    /// runner: an attempt with neither has no lease to extend (pg's
    /// `LEASE_UPSERT`).
    pub(crate) async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let expires_at = codec::parse_lease(locked_until)?;
        self.write(|tx| {
            let renewed = tx
                .prepare_cached(
                    "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                     SELECT q.request_id, COALESCE(q.runner_id, s.runner_id), ?2, ?3 \
                     FROM job_requests q \
                     LEFT JOIN runner_sessions s ON s.session_id = q.session_id \
                     WHERE q.request_id = ?1 AND q.result IS NULL \
                       AND COALESCE(q.runner_id, s.runner_id) IS NOT NULL \
                     ON CONFLICT (request_id) DO UPDATE SET \
                         expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
                )
                .map_err(db)?
                .execute(params![request_id, expires_at, now_us()])
                .map_err(db)?;
            Ok(renewed == 1)
        })
    }

    /// Settle an AgentRequest iff still in flight. `None` = already settled
    /// or unknown (duplicate PATCH).
    pub(crate) async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        self.write(|tx| {
            if !stamp_result(tx, request_id, result, locked_until)? {
                return Ok(None);
            }
            tx.prepare_cached(
                "SELECT run_id, job_id, agent_job_id FROM job_requests WHERE request_id = ?1",
            )
            .map_err(db)?
            .query_row([request_id], attempt_tuple)
            .optional()
            .map_err(db)
        })
    }

    /// DELETE-ack one session message; a missing row is not an error.
    pub(crate) async fn delete_inflight(
        &self,
        session_id: &str,
        message_id: i64,
    ) -> Result<(), ControlError> {
        let session_uuid = crate::control::logic::session_uuid(session_id).to_string();
        self.write(|tx| {
            tx.prepare_cached(
                "DELETE FROM session_messages WHERE session_id = ?1 AND message_id = ?2",
            )
            .map_err(db)?
            .execute(params![session_uuid, message_id])
            .map_err(db)?;
            Ok(())
        })
    }

    /// Plan ids (= agent job ids) of every in-flight attempt.
    pub(crate) async fn active_plan_ids(&self) -> Result<BTreeSet<String>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached("SELECT agent_job_id FROM job_requests WHERE result IS NULL")
                .map_err(db)?;
            let rows = stmt.query_map([], |row| row.get(0)).map_err(db)?;
            rows.collect::<Result<_, _>>().map_err(db)
        })
    }

    /// Latest attempt's run per plan id; unknown plan ids are omitted.
    pub(crate) async fn artifact_scopes(
        &self,
        plan_ids: &[String],
    ) -> Result<BTreeMap<String, RunId>, ControlError> {
        let agents: Vec<(String, String)> = plan_ids
            .iter()
            .filter_map(|plan| {
                codec::plan_agent_job_id(plan).map(|agent| (agent.to_string(), plan.clone()))
            })
            .collect();
        if agents.is_empty() {
            return Ok(BTreeMap::new());
        }
        let by_agent: BTreeMap<String, String> = agents.iter().cloned().collect();
        self.read(|tx| {
            let placeholders = vec!["?"; agents.len()].join(",");
            let mut stmt = tx
                .prepare(&format!(
                    "SELECT agent_job_id, run_id FROM job_requests \
                     WHERE agent_job_id IN ({placeholders})"
                ))
                .map_err(db)?;
            let rows = stmt
                .query_map(
                    rusqlite::params_from_iter(agents.iter().map(|(agent, _)| agent)),
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(db)?;
            let mut scopes = BTreeMap::new();
            for row in rows {
                let (agent, run) = row.map_err(db)?;
                if let Some(plan) = by_agent.get(&agent) {
                    scopes.insert(plan.clone(), codec::run_id(&run));
                }
            }
            Ok(scopes)
        })
    }

    /// Issue the one-shot pause-on-failure debug credential for an
    /// in-flight attempt: `NotFound` (no in-flight attempt), `Forbidden`
    /// (run did not set `preserve_on_failure`), `Conflict` (already issued).
    /// The flip is a conditional UPDATE, so two racing issuers cannot both
    /// succeed.
    pub(crate) async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError> {
        let agent = agent_job_id.to_string();
        self.write(|tx| {
            let row = tx
                .prepare_cached(
                    "SELECT q.request_id, q.run_id, q.debug_token_issued, s.submission \
                     FROM job_requests q LEFT JOIN run_submissions s ON s.run_id = q.run_id \
                     WHERE q.agent_job_id = ?1 AND q.result IS NULL",
                )
                .map_err(db)?
                .query_row([&agent], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            let Some((request_id, run_id, issued, submission)) = row else {
                return Err(ControlError::NotFound(format!(
                    "no active job request for agent job {agent}"
                )));
            };
            let preserve = submission
                .and_then(|json| serde_json::from_str::<serde_json::Value>(&json).ok())
                .and_then(|value| value["preserve_on_failure"].as_bool())
                .unwrap_or(false);
            if !preserve {
                return Err(ControlError::Forbidden(
                    "this run did not enable pause-on-failure".to_owned(),
                ));
            }
            let flipped = if issued {
                0
            } else {
                tx.prepare_cached(
                    "UPDATE job_requests SET debug_token_issued = 1 \
                     WHERE request_id = ?1 AND debug_token_issued = 0",
                )
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?
            };
            if flipped == 0 {
                return Err(ControlError::Conflict(format!(
                    "debug-worker token already issued for agent job {agent}"
                )));
            }
            Ok((codec::run_id(&run_id), codec::plan_id(&agent_job_id)))
        })
    }

    /// Allocate a plan-local log id and create its `log_files` row:
    /// `INSERT .. SELECT COALESCE(MAX(log_id), 0) +
    /// 1 .. ON CONFLICT DO NOTHING RETURNING log_id`, arbitrated by
    /// `UNIQUE (plan_id, log_id)`. The plan's run comes from the attempt
    /// (`agent_job_id = plan_id`); an unknown plan is `NotFound`.
    pub(crate) async fn create_log(&self, plan_id: &str) -> Result<i64, ControlError> {
        let not_found = || ControlError::NotFound(format!("plan {plan_id}"));
        let agent = codec::plan_agent_job_id(plan_id).ok_or_else(not_found)?;
        let plan = agent.to_string();
        self.write(|tx| {
            let run_id: String = tx
                .prepare_cached("SELECT run_id FROM job_requests WHERE agent_job_id = ?1")
                .map_err(db)?
                .query_row([&plan], |row| row.get(0))
                .optional()
                .map_err(db)?
                .ok_or_else(not_found)?;
            // One writer: the first attempt cannot conflict, but the
            // bounded retry keeps the statement identical to Postgres'.
            for _ in 0..8 {
                let id: Option<i64> = tx
                    .prepare_cached(
                        "INSERT INTO log_files (log_key, run_id, plan_id, log_id) \
                         SELECT ?1 || '/' || (COALESCE(MAX(log_id), 0) + 1), ?2, ?1, \
                                COALESCE(MAX(log_id), 0) + 1 \
                         FROM log_files WHERE plan_id = ?1 \
                         ON CONFLICT DO NOTHING RETURNING log_id",
                    )
                    .map_err(db)?
                    .query_row(params![plan, run_id], |row| row.get(0))
                    .optional()
                    .map_err(db)?;
                if let Some(id) = id {
                    return Ok(id);
                }
            }
            Err(ControlError::Conflict(format!(
                "could not allocate a log id for plan {plan_id}"
            )))
        })
    }

    /// Live-log key for `job_id` of `run_id` plus whether the run or job is
    /// terminal. The key is the latest attempt's agent job id (matching
    /// `job_id` as a logical id or as an agent job id), else the logical id
    /// when the run owns that job. `None` when the run does not exist.
    pub(crate) async fn live_log_key(
        &self,
        run_id: RunId,
        job_id: &str,
    ) -> Result<Option<(String, bool)>, ControlError> {
        let run = codec::run_key(run_id);
        self.read(|tx| {
            let Some(run_status) = tx
                .prepare_cached("SELECT status FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .query_row([&run], |row| row.get::<_, String>(0))
                .optional()
                .map_err(db)?
            else {
                return Ok(None);
            };
            let agent: Option<String> = tx
                .prepare_cached(
                    "SELECT agent_job_id FROM job_requests \
                     WHERE run_id = ?1 AND (job_id = ?2 OR agent_job_id = ?2) \
                     ORDER BY request_id DESC LIMIT 1",
                )
                .map_err(db)?
                .query_row(params![run, job_id], |row| row.get(0))
                .optional()
                .map_err(db)?;
            let job_status: Option<String> = tx
                .prepare_cached("SELECT status FROM jobs WHERE run_id = ?1 AND job_id = ?2")
                .map_err(db)?
                .query_row(params![run, job_id], |row| row.get(0))
                .optional()
                .map_err(db)?;
            let key = match agent {
                Some(agent) => agent,
                // A bare logical key is valid only when the run owns it.
                None if job_status.is_some() => job_id.to_owned(),
                None => return Ok(None),
            };
            let terminal = run_status == "completed"
                || job_status.is_some_and(|s| status_parse(&s).is_terminal());
            Ok(Some((key, terminal)))
        })
    }
}
