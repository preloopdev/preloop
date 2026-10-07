//! Runner and session lifecycle commands: registration, pairing, purge,
//! sessions (open/close/touch/owner), boot reconcile, dispatch-intent
//! rebuild, and the durable event outbox append.
//!
//! Translated statement-for-statement from the Postgres backend
//! (`control/pg/lifecycle.rs`, `control/pg/runners.rs`, and
//! `control/pg/dispatch.rs::requeue_claimed_tx`/`append_event_tx`). SQLite
//! is single-writer: `FOR UPDATE`/`SKIP LOCKED` carry no meaning and drop
//! away; every conditional `WHERE` stays.

use super::codec::{self, now_us};
use super::{LiteBackend, concurrency, db, jobs, promote, settle};
use crate::control::backend::{self, ReconcileOutcome, event_run_id};
use crate::control::logic;
use crate::control::types::*;
use crate::models::RunnerCapabilities;
use preloop_gha_protocol::{JobId, RegisteredRunner, RunId};
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;

/// Columns [`runner_row`] decodes from `runners`.
const RUNNER_COLUMNS: &str = "runner_id, name, labels, ephemeral, public_key, \
     rsa_public_key, client_id, runner_group_id, runner_group_name, \
     pool_proven, registered_at";

/// Decode a [`RUNNER_COLUMNS`] row into a `RunnerRow`.
fn runner_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunnerRow> {
    let runner = RegisteredRunner {
        id: row.get(0)?,
        name: row.get(1)?,
        labels: serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
        ephemeral: row.get::<_, i64>(3)? != 0,
        public_key: row.get(4)?,
        runner_group_id: row.get(7)?,
        runner_group_name: row.get(8)?,
    };
    Ok(RunnerRow {
        runner,
        rsa_public_key: row
            .get::<_, Option<Vec<u8>>>(5)?
            .and_then(|xml| String::from_utf8(xml).ok())
            .and_then(|xml| preloop_gha_protocol::crypto::AgentRsaPublicKey::parse(&xml).ok()),
        client_id: row.get(6)?,
        pool_proven: row.get::<_, i64>(9)? != 0,
        registered_at_us: row.get(10)?,
    })
}

/// The runner's dispatch capabilities (`labels` + runner group) for
/// matching queries.
fn runner_caps(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunnerCapabilities> {
    Ok(RunnerCapabilities {
        known: true,
        labels: serde_json::from_str(&row.get::<_, String>(0)?).unwrap_or_default(),
        runner_group_id: row.get(1)?,
        runner_group_name: row.get(2)?,
    })
}

/// `requeue_claimed_tx` (pg dispatch.rs): a claimed job whose runner is
/// gone drops the claim — its attempt is replaced via [`retry_attempt`],
/// whose abandoned runtime identity lands in `retired` — and returns to the
/// head of the ready queue.
pub(super) fn requeue_claimed(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    retired: &mut Vec<uuid::Uuid>,
) -> Result<bool, ControlError> {
    let run = codec::run_key(run_id);
    let state: Option<String> = tx
        .prepare_cached("SELECT queue_state FROM jobs WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .query_row(params![run, job_id.0], |row| row.get(0))
        .optional()
        .map_err(db)?;
    if state.as_deref() != Some("claimed") {
        return Ok(false);
    }
    retired.extend(retry_attempt(tx, run_id, job_id)?);
    tx.prepare_cached(
        "UPDATE jobs SET status = 'queued', queue_state = 'ready', \
         claimed_by_runner_id = NULL, claimed_at = NULL, \
         enqueued_at = COALESCE(enqueued_at, ?3) \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![run, job_id.0, now_us()])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    Ok(true)
}

/// Replace the job's in-flight attempt so no credential of the abandoned
/// one survives the retry. The old row settles `cancelled` — its runtime
/// token, debug token and renewals stop validating — while its step
/// manifest, logs and timeline stay addressable. The replacement gets a
/// fresh runtime identity (`jobId` and the derived plan id), its own
/// timeline, a pending step manifest and an unissued debug token; the stored
/// message and the deferred token-mint recipe move to it. Returns the
/// abandoned runtime identity, `None` when nothing was in flight.
pub(super) fn retry_attempt(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<uuid::Uuid>, ControlError> {
    let Some((request_id, abandoned, namespace)) = tx
        .prepare_cached(
            "SELECT request_id, agent_job_id, namespace_id FROM job_requests \
             WHERE run_id = ?1 AND job_id = ?2 AND result IS NULL \
             ORDER BY request_id DESC LIMIT 1",
        )
        .map_err(db)?
        .query_row(params![codec::run_key(run_id), job_id.0], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .optional()
        .map_err(db)?
    else {
        return Ok(None);
    };
    let Some(mut message) = jobs::stored_job_message(tx, run_id, job_id)? else {
        return Err(ControlError::backend(anyhow::anyhow!(
            "request {request_id} has no message template"
        )));
    };
    tx.prepare_cached(
        "UPDATE job_requests SET result = 'cancelled', finished_at = ?2, \
         runner_id = NULL, session_id = NULL WHERE request_id = ?1",
    )
    .map_err(db)?
    .execute(params![request_id, now_us()])
    .map_err(db)?;
    tx.prepare_cached("DELETE FROM job_leases WHERE request_id = ?1")
        .map_err(db)?
        .execute([request_id])
        .map_err(db)?;
    let agent_job_id = uuid::Uuid::new_v4();
    let timeline_id = uuid::Uuid::new_v4();
    let retry = super::submit::insert_attempt(
        tx,
        &namespace,
        run_id,
        job_id,
        agent_job_id,
        timeline_id,
        crate::models::StepRecord::manifest(&message.steps),
    )?;
    message.job_id = agent_job_id;
    message.request_id = retry;
    message.plan.plan_id = codec::plan_id(&agent_job_id);
    message.timeline.id = timeline_id;
    jobs::update_job_message(tx, run_id, job_id, &message)?;
    tx.prepare_cached("UPDATE github_token_requests SET request_id = ?2 WHERE request_id = ?1")
        .map_err(db)?
        .execute(params![request_id, retry])
        .map_err(db)?;
    Ok(Some(codec::uuid(&abandoned)))
}

/// `append_event_tx` (pg dispatch.rs): append one durable event to the
/// outbox. A status event is stamped with the version of the row it reports
/// on, or not published when that row has already settled on a different
/// final state (`logic::EventStamp`).
pub(super) fn append_event_tx(
    tx: &Transaction<'_>,
    event: &preloop_gha_protocol::NdjsonEvent,
) -> Result<(), ControlError> {
    use preloop_gha_protocol::NdjsonEvent;
    let run_id = event_run_id(event);
    let payload = serde_json::to_string(event).map_err(ControlError::backend)?;
    // The versioned topic is the event's serde tag (`job_status` ->
    // `job_status.v1`).
    let topic = serde_json::from_str::<serde_json::Value>(&payload)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(|t| t.as_str())
                .map(str::to_owned)
        })
        .map(|kind| format!("{kind}.v1"))
        .unwrap_or_else(|| "event.v1".to_owned());
    let namespace = match run_id {
        Some(run_id) => jobs::namespace_of(tx, run_id)?,
        None => DEFAULT_NAMESPACE.to_owned(),
    };
    let (job_id, stamp) = match event {
        NdjsonEvent::JobStatus {
            run_id,
            job_id,
            status,
            ..
        } => {
            let stamp = tx
                .prepare_cached(
                    "SELECT status, version FROM jobs WHERE run_id = ?1 AND job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![codec::run_key(*run_id), job_id.0], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                })
                .optional()
                .map_err(db)?
                .map(|(row_status, version)| logic::job_event_stamp(*status, &row_status, version))
                .unwrap_or(logic::EventStamp::Unversioned);
            (Some(job_id.0.as_str()), stamp)
        }
        NdjsonEvent::RunStatus { run_id, status, .. } => {
            let stamp = tx
                .prepare_cached("SELECT status, conclusion, version FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .query_row([codec::run_key(*run_id)], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .optional()
                .map_err(db)?
                .map(|(row_status, conclusion, version)| {
                    logic::run_event_stamp(*status, &row_status, conclusion.as_deref(), version)
                })
                .unwrap_or(logic::EventStamp::Unversioned);
            (None, stamp)
        }
        NdjsonEvent::Annotation { job_id, .. } => {
            (Some(job_id.0.as_str()), logic::EventStamp::Unversioned)
        }
        _ => (None, logic::EventStamp::Unversioned),
    };
    let version = match stamp {
        logic::EventStamp::Stale => return Ok(()),
        logic::EventStamp::Version(version) => Some(version),
        logic::EventStamp::Unversioned => None,
    };
    jobs::insert_outbox(tx, &namespace, run_id, job_id, version, &topic, &payload)
}

/// `pair_runner_tx` (pg lifecycle.rs) + the trait doc: release stale or
/// dead-runner bindings back to the pool waitlist, then bind the oldest
/// pool-pending ready job this runner can serve.
///
/// Two deliberate divergences from the pg text, both required by the trait
/// doc for `pair_runner` ("mark the runner pool-proven" and "re-marked
/// pool-pending at `now`"): the runner's `pool_proven` column is stamped,
/// and released bindings gain a `provision_requests` row so they remain
/// pairable. Pg only clears `runner_id`, which would strand the job.
fn pair_runner_tx(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    runner_id: i64,
) -> Result<(), ControlError> {
    let (pool_assignments, require_assignments, _) = backend.config();
    if !pool_assignments && !require_assignments {
        return Ok(());
    }
    tx.prepare_cached("UPDATE runners SET pool_proven = 1 WHERE runner_id = ?1")
        .map_err(db)?
        .execute([runner_id])
        .map_err(db)?;
    let caps = tx
        .prepare_cached(
            "SELECT labels, runner_group_id, runner_group_name FROM runners \
             WHERE runner_id = ?1",
        )
        .map_err(db)?
        .query_row([runner_id], runner_caps)
        .optional()
        .map_err(db)?;
    let Some(caps) = caps else { return Ok(()) };

    // Release bindings on this runner, stale bindings, and bindings whose
    // runner is gone; each released job rejoins the pool waitlist at now.
    let stale_cutoff = now_us() - crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
    let released: Vec<(String, String)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT run_id, job_id FROM job_assignments \
                 WHERE runner_id IS NOT NULL AND (runner_id = ?1 \
                   OR assigned_at < ?2 \
                   OR NOT EXISTS (SELECT 1 FROM runners r \
                                  WHERE r.runner_id = job_assignments.runner_id))",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map(params![runner_id, stale_cutoff], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    let mut released_count = 0usize;
    let now = now_us();
    for (run, job) in &released {
        tx.prepare_cached(
            "UPDATE job_assignments SET runner_id = NULL \
             WHERE run_id = ?1 AND job_id = ?2",
        )
        .map_err(db)?
        .execute(params![run, job])
        .map_err(db)?;
        if pool_assignments {
            tx.prepare_cached(
                "INSERT INTO provision_requests (run_id, job_id, namespace_id, \
                 pool_key, labels, requested_at) \
                 SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, j.runs_on, ?3 \
                 FROM jobs j WHERE j.run_id = ?1 AND j.job_id = ?2 \
                 ON CONFLICT (run_id, job_id) DO UPDATE SET requested_at = ?3",
            )
            .map_err(db)?
            .execute(params![run, job, now])
            .map_err(db)?;
            released_count += 1;
        }
    }
    backend.count_released_bindings(released_count);

    // Oldest pool-pending job this runner can serve (pending-mark order,
    // ties by queue position). A job its namespace would not let start
    // (state or running caps) is not paired: binding it would park this
    // warm runner on work that cannot run.
    let pending: Vec<(String, String, Vec<String>, Option<String>)> = {
        let mut stmt = tx
            .prepare_cached(&format!(
                "SELECT p.run_id, p.job_id, j.runs_on, j.runner_group \
                 FROM provision_requests p JOIN jobs j \
                 ON j.run_id = p.run_id AND j.job_id = p.job_id \
                 WHERE j.queue_state = 'ready' AND ({}) \
                 ORDER BY p.requested_at, j.priority DESC, j.run_order, j.job_order \
                 LIMIT 64",
                crate::control::types::NAMESPACE_ADMITS_CLAIM
            ))
            .map_err(db)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    for (run, job, runs_on, runner_group) in pending {
        if crate::runtime_scheduling::job_matches_runner(&runs_on, &caps.labels)
            && crate::runtime_scheduling::job_matches_runner_group(runner_group.as_deref(), &caps)
        {
            tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
                .map_err(db)?
                .execute(params![run, job])
                .map_err(db)?;
            tx.prepare_cached(
                "INSERT INTO job_assignments (run_id, job_id, runner_id, \
                 assigned_at, first_assigned_at) VALUES (?1, ?2, ?3, ?4, ?4) \
                 ON CONFLICT (run_id, job_id) DO UPDATE SET runner_id = ?3, \
                 assigned_at = ?4",
            )
            .map_err(db)?
            .execute(params![run, job, runner_id, now])
            .map_err(db)?;
            return Ok(());
        }
    }
    Ok(())
}

/// `purge_runner_tx` (pg lifecycle.rs): capture the runner's claimed jobs,
/// delete the runner and its sessions, then requeue each job.
/// `runner_sessions.runner_id` carries no FK (a declared id may precede
/// registration), so the sessions are deleted explicitly — a revoked
/// long-poll must find no session and get `Forbidden`.
fn purge_runner_tx(tx: &Transaction<'_>, runner_id: i64) -> Result<Vec<uuid::Uuid>, ControlError> {
    let requeue: Vec<(String, String)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT DISTINCT j.run_id, j.job_id FROM jobs j \
                 WHERE j.queue_state = 'claimed' AND ( \
                   j.claimed_by_runner_id = ?1 \
                   OR EXISTS (SELECT 1 FROM job_assignments a \
                     WHERE a.run_id = j.run_id AND a.job_id = j.job_id \
                     AND a.runner_id = ?1) \
                   OR EXISTS (SELECT 1 FROM job_requests q JOIN runner_sessions s \
                     ON s.session_id = q.session_id WHERE q.run_id = j.run_id \
                     AND q.job_id = j.job_id AND q.result IS NULL \
                     AND s.runner_id = ?1))",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([runner_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    // Requests bound through this runner's sessions lose owner, session and
    // start stamps before the session rows are deleted below.
    tx.prepare_cached(
        "UPDATE job_requests SET runner_id = NULL, session_id = NULL, \
         started_at = NULL, timeout_triggered = 0 \
         WHERE result IS NULL AND (runner_id = ?1 OR session_id IN \
         (SELECT session_id FROM runner_sessions WHERE runner_id = ?1))",
    )
    .map_err(db)?
    .execute([runner_id])
    .map_err(db)?;
    // Ready-but-assigned jobs of the dead runner return to pool-pending so
    // provisioning re-runs for them. Collected BEFORE the runner row goes
    // away — job_assignments.runner_id is ON DELETE SET NULL, so the delete
    // below would hide them.
    let unclaimed: Vec<(String, String)> = {
        let mut stmt = tx
            .prepare_cached(
                "SELECT j.run_id, j.job_id FROM jobs j \
                 WHERE j.queue_state = 'ready' AND EXISTS ( \
                   SELECT 1 FROM job_assignments a WHERE a.run_id = j.run_id \
                   AND a.job_id = j.job_id AND a.runner_id = ?1)",
            )
            .map_err(db)?;
        let rows = stmt
            .query_map([runner_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
    };
    tx.prepare_cached("DELETE FROM job_leases WHERE runner_id = ?1")
        .map_err(db)?
        .execute([runner_id])
        .map_err(db)?;
    // Session messages cascade from `runner_sessions` (FK on session_id).
    tx.prepare_cached("DELETE FROM runner_sessions WHERE runner_id = ?1")
        .map_err(db)?
        .execute([runner_id])
        .map_err(db)?;
    tx.prepare_cached("DELETE FROM runners WHERE runner_id = ?1")
        .map_err(db)?
        .execute([runner_id])
        .map_err(db)?;
    let mut retired = Vec::new();
    for (run, job) in requeue {
        requeue_claimed(tx, codec::run_id(&run), &JobId(job), &mut retired)?;
    }
    let now = now_us();
    for (run, job) in unclaimed {
        tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
            .map_err(db)?
            .execute(params![run, job])
            .map_err(db)?;
        tx.prepare_cached(
            "INSERT INTO provision_requests (run_id, job_id, namespace_id, \
             pool_key, labels, requested_at) \
             SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, j.runs_on, ?3 \
             FROM jobs j WHERE j.run_id = ?1 AND j.job_id = ?2 \
             ON CONFLICT (run_id, job_id) DO UPDATE SET requested_at = ?3",
        )
        .map_err(db)?
        .execute(params![run, job, now])
        .map_err(db)?;
    }
    Ok(retired)
}

impl LiteBackend {
    /// `create_session` (pg lifecycle.rs): insert a caller-protocol session
    /// row. Compat sessions persist `azdo` with a NULL runner (that is what
    /// marks them compat). The sealed crypto material is never stored —
    /// keys are derived — it is echoed back on the returned `SessionRow`.
    pub(crate) async fn create_session(
        &self,
        session: backend::CreateSession,
    ) -> Result<SessionRow, ControlError> {
        let session_id = uuid::Uuid::new_v4().to_string();
        let protocol = match session.protocol {
            SessionProtocol::Compat => "azdo",
            SessionProtocol::Broker => "broker",
            SessionProtocol::Azdo => "azdo",
        };
        let runner_id = match session.protocol {
            SessionProtocol::Compat => None,
            _ => Some(session.runner_id),
        };
        self.write(|tx| {
            if let Some(runner_id) = runner_id {
                let alive = tx
                    .prepare_cached("SELECT 1 FROM runners WHERE runner_id = ?1")
                    .map_err(db)?
                    .query_row([runner_id], |row| row.get::<_, i64>(0))
                    .optional()
                    .map_err(db)?
                    .is_some();
                if !alive {
                    return Err(ControlError::Forbidden(
                        "runner registration no longer exists".to_owned(),
                    ));
                }
            }
            let now = now_us();
            tx.prepare_cached(
                "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                 client_id, verified, created_at, last_seen_at) \
                 VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5)",
            )
            .map_err(db)?
            .execute(params![
                session_id,
                runner_id,
                protocol,
                session.client_id,
                now
            ])
            .map_err(db)?;
            Ok(SessionRow {
                session_id,
                runner_id: session.runner_id,
                protocol: session.protocol,
                client_id: session.client_id,
                active_request_id: None,
                last_seen_at_us: Some(now),
            })
        })
    }

    /// `open_runner_session` (pg lifecycle.rs): insert a caller-minted
    /// session. Verified sessions are exclusive per runner (Conflict on a
    /// second live one); `runner_id: None` writes nothing (compat);
    /// `require_live_runner` rejects an unregistered runner in the insert's
    /// transaction.
    pub(crate) async fn open_runner_session(
        &self,
        open: OpenRunnerSession,
    ) -> Result<(), ControlError> {
        let Some(runner_id) = open.runner_id else {
            return Ok(());
        };
        let session_uuid = logic::session_uuid(&open.session_id).to_string();
        let protocol = match open.protocol {
            SessionProtocol::Broker => "broker",
            _ => "azdo",
        };
        self.write(|tx| {
            // The liveness check and the insert share this transaction: the
            // liveness sweep may have purged the runner after token
            // validation but before the insert (the handler maps `Forbidden`
            // to 401).
            if open.require_live_runner {
                let alive = tx
                    .prepare_cached("SELECT 1 FROM runners WHERE runner_id = ?1")
                    .map_err(db)?
                    .query_row([runner_id], |row| row.get::<_, i64>(0))
                    .optional()
                    .map_err(db)?
                    .is_some();
                if !alive {
                    return Err(ControlError::Forbidden(
                        "runner registration no longer exists".to_owned(),
                    ));
                }
            }
            if open.verified {
                let conflict = tx
                    .prepare_cached(
                        "SELECT 1 FROM runner_sessions WHERE runner_id = ?1 \
                         AND verified AND session_id <> ?2",
                    )
                    .map_err(db)?
                    .query_row(params![runner_id, session_uuid], |row| row.get::<_, i64>(0))
                    .optional()
                    .map_err(db)?
                    .is_some();
                if conflict {
                    return Err(ControlError::Conflict(format!(
                        "runner {runner_id} already owns a verified session"
                    )));
                }
            }
            // `runner_id` has no FK (see schema.sql), so liveness is checked
            // above: a session never outlives its runner registration.
            tx.prepare_cached(
                "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                 verified) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (session_id) DO UPDATE SET runner_id = ?2, \
                 protocol = ?3, verified = ?4, \
                 last_seen_at = CAST(unixepoch('subsec') * 1000000 AS INTEGER)",
            )
            .map_err(db)?
            .execute(params![session_uuid, runner_id, protocol, open.verified])
            .map_err(db)?;
            Ok(())
        })
    }

    /// `close_runner_session` (pg lifecycle.rs): delete the row when the
    /// caller owns it (or presents no runner). `false` = no row;
    /// `Forbidden` = other runner's. Its in-flight requests release for
    /// retry; claimed jobs are left for the reaper (see the trait doc).
    pub(crate) async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError> {
        let session_uuid = logic::session_uuid(session_id).to_string();
        self.write(|tx| {
            let owner: Option<Option<i64>> = tx
                .prepare_cached("SELECT runner_id FROM runner_sessions WHERE session_id = ?1")
                .map_err(db)?
                .query_row([session_uuid.as_str()], |row| row.get(0))
                .optional()
                .map_err(db)?;
            let Some(owner) = owner else { return Ok(false) };
            if let (Some(owner), Some(caller)) = (owner, caller_runner_id)
                && owner != caller
            {
                return Err(ControlError::Forbidden(
                    "session belongs to another runner".to_owned(),
                ));
            }
            // Release the session's live requests so the jobs can be retried.
            // Only the session binding goes: runner_id stays, so the
            // attempt's owner survives session teardown and only that runner
            // may still complete/renew it (legacy AgentRequest semantics —
            // verified by
            // legacy_agent_requests_are_bound_to_runner_identity). The start
            // stamp and the lease stay too: the trait doc leaves the active
            // request and its claimed job to the lease reaper, which reads
            // both of them.
            let requests: Vec<i64> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT request_id FROM job_requests WHERE session_id = ?1 \
                         AND result IS NULL",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([session_uuid.as_str()], |row| row.get(0))
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            tx.prepare_cached("DELETE FROM runner_sessions WHERE session_id = ?1")
                .map_err(db)?
                .execute([session_uuid.as_str()])
                .map_err(db)?;
            for request_id in requests {
                tx.prepare_cached(
                    "UPDATE job_requests SET session_id = NULL \
                     WHERE request_id = ?1 AND result IS NULL",
                )
                .map_err(db)?
                .execute([request_id])
                .map_err(db)?;
            }
            Ok(true)
        })
    }

    /// `delete_session` (pg lifecycle.rs): unconditional close.
    pub(crate) async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        self.close_runner_session(session_id, None).await?;
        Ok(())
    }

    /// `touch_session` (pg lifecycle.rs): stamp liveness; return the stored
    /// protocol for poll routing.
    pub(crate) async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        let session_uuid = logic::session_uuid(session_id).to_string();
        self.write(|tx| {
            let protocol: Option<String> = tx
                .prepare_cached(
                    "UPDATE runner_sessions SET last_seen_at = ?2 \
                     WHERE session_id = ?1 RETURNING protocol",
                )
                .map_err(db)?
                .query_row(params![session_uuid, now_us()], |row| row.get(0))
                .optional()
                .map_err(db)?;
            Ok(protocol.map(|p| SessionProtocol::parse(&p)))
        })
    }

    /// `session_owner` (pg lifecycle.rs): the session's runner plus its
    /// dispatch capabilities. `None` for unknown or runner-less sessions.
    pub(crate) async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, RunnerCapabilities)>, ControlError> {
        let session_uuid = logic::session_uuid(session_id).to_string();
        self.read(|tx| {
            let row = tx
                .prepare_cached(
                    "SELECT s.runner_id, r.labels, r.runner_group_id, \
                     r.runner_group_name FROM runner_sessions s \
                     LEFT JOIN runners r ON r.runner_id = s.runner_id \
                     WHERE s.session_id = ?1",
                )
                .map_err(db)?
                .query_row([session_uuid.as_str()], |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            Ok(row.and_then(|(runner_id, labels, group_id, group_name)| {
                runner_id.map(|runner_id| {
                    (
                        runner_id,
                        RunnerCapabilities {
                            known: labels.is_some(),
                            labels: labels
                                .and_then(|t| serde_json::from_str(&t).ok())
                                .unwrap_or_default(),
                            runner_group_id: group_id,
                            runner_group_name: group_name,
                        },
                    )
                })
            }))
        })
    }

    /// `register_runner` (pg lifecycle.rs + runners.rs): dedup on
    /// `client_id`, else insert; a pool-proven registration pairs with a
    /// pending job it can serve.
    pub(crate) async fn register_runner(
        &self,
        reg: backend::RegisterRunner,
    ) -> Result<RunnerRow, ControlError> {
        self.write(|tx| {
            let labels = serde_json::to_string(&reg.labels).map_err(ControlError::backend)?;
            let rsa = reg
                .rsa_public_key
                .as_ref()
                .map(|key| key.to_xml_string().into_bytes());
            let inserted = tx
                .prepare_cached(&format!(
                    "INSERT INTO runners (namespace_id, name, labels, ephemeral, \
                     public_key, rsa_public_key, runner_group_id, runner_group_name, \
                     client_id, pool_proven) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
                     ON CONFLICT (client_id) DO NOTHING RETURNING {RUNNER_COLUMNS}"
                ))
                .map_err(db)?
                .query_row(
                    params![
                        DEFAULT_NAMESPACE,
                        reg.name,
                        labels,
                        reg.ephemeral,
                        reg.public_key,
                        rsa,
                        reg.runner_group_id,
                        reg.runner_group_name,
                        reg.client_id,
                        reg.pool_proven,
                    ],
                    runner_row,
                )
                .optional()
                .map_err(db)?;
            let (row, inserted) = match inserted {
                Some(row) => (row, true),
                None => (
                    tx.prepare_cached(&format!(
                        "SELECT {RUNNER_COLUMNS} FROM runners WHERE client_id = ?1"
                    ))
                    .map_err(db)?
                    .query_row([&reg.client_id], runner_row)
                    .map_err(db)?,
                    false,
                ),
            };
            if inserted && reg.pool_proven {
                pair_runner_tx(tx, self, row.runner.id)?;
            }
            Ok(row)
        })
    }

    /// `pair_runner` (pg lifecycle.rs): pair this runner with the oldest
    /// pool-pending job it can serve.
    pub(crate) async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        self.write(|tx| pair_runner_tx(tx, self, runner_id))
    }

    /// `update_runner` (pg lifecycle.rs): in-place name/labels update;
    /// `NotFound` for an unknown runner.
    pub(crate) async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError> {
        self.write(|tx| {
            if let Some(name) = name {
                tx.prepare_cached("UPDATE runners SET name = ?2 WHERE runner_id = ?1")
                    .map_err(db)?
                    .execute(params![runner_id, name])
                    .map_err(db)?;
            }
            if let Some(labels) = labels {
                let labels = serde_json::to_string(&labels).map_err(ControlError::backend)?;
                tx.prepare_cached("UPDATE runners SET labels = ?2 WHERE runner_id = ?1")
                    .map_err(db)?
                    .execute(params![runner_id, labels])
                    .map_err(db)?;
            }
            tx.prepare_cached(&format!(
                "SELECT {RUNNER_COLUMNS} FROM runners WHERE runner_id = ?1"
            ))
            .map_err(db)?
            .query_row([runner_id], runner_row)
            .optional()
            .map_err(db)?
            .ok_or_else(|| ControlError::NotFound(format!("runner {runner_id}")))
        })
    }

    /// `purge_runner` (pg lifecycle.rs): unconditional purge; returns the
    /// runtime identities of the attempts it retired.
    pub(crate) async fn purge_runner(
        &self,
        runner_id: i64,
    ) -> Result<Vec<uuid::Uuid>, ControlError> {
        self.write(|tx| purge_runner_tx(tx, runner_id))
    }

    /// `purge_runner_guarded` (pg lifecycle.rs): apply `guard`, then purge.
    /// `IfPhantom`/`RegistrationToken` refuse while the runner owns a
    /// session; `Runner(caller)` refuses another runner's identity. `None`
    /// when the guard refused; otherwise the retired attempts' identities.
    pub(crate) async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<Option<Vec<uuid::Uuid>>, ControlError> {
        self.write(|tx| {
            let exists = tx
                .prepare_cached("SELECT 1 FROM runners WHERE runner_id = ?1")
                .map_err(db)?
                .query_row([runner_id], |row| row.get::<_, i64>(0))
                .optional()
                .map_err(db)?
                .is_some();
            let owns_session = || -> Result<bool, ControlError> {
                Ok(tx
                    .prepare_cached("SELECT 1 FROM runner_sessions WHERE runner_id = ?1")
                    .map_err(db)?
                    .query_row([runner_id], |row| row.get::<_, i64>(0))
                    .optional()
                    .map_err(db)?
                    .is_some())
            };
            let allowed = match guard {
                PurgeGuard::System => exists,
                PurgeGuard::Runner(caller) => {
                    if caller != runner_id {
                        return Err(ControlError::Forbidden(
                            "runner token cannot purge another runner".to_owned(),
                        ));
                    }
                    exists
                }
                PurgeGuard::RegistrationToken => exists && !owns_session()?,
                PurgeGuard::IfPhantom => exists && !owns_session()?,
            };
            if !allowed {
                return Ok(None);
            }
            purge_runner_tx(tx, runner_id).map(Some)
        })
    }

    /// `ephemeral_runner_ids` (pg lifecycle.rs).
    pub(crate) async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached("SELECT runner_id FROM runners WHERE ephemeral ORDER BY runner_id")
                .map_err(db)?;
            let rows = stmt.query_map([], |row| row.get(0)).map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// `runner_ids_named` (pg lifecycle.rs).
    pub(crate) async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare_cached("SELECT runner_id FROM runners WHERE name = ?1 ORDER BY runner_id")
                .map_err(db)?;
            let rows = stmt.query_map([name], |row| row.get(0)).map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// `lookup_agent` per the trait doc: the LOWEST-id runner named `name`
    /// plus its client id, synthesizing and recording
    /// `{:08x}-0000-4000-8000-000000000000` when the runner has none so a
    /// later token request resolves. (Pg reads the highest id and never
    /// records the synthesized id — the doc's semantics win here.)
    pub(crate) async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(RegisteredRunner, String)>, ControlError> {
        self.write(|tx| {
            let row = tx
                .prepare_cached(
                    "SELECT runner_id, name, labels, ephemeral, public_key, \
                     runner_group_id, runner_group_name, client_id \
                     FROM runners WHERE name = ?1 ORDER BY runner_id LIMIT 1",
                )
                .map_err(db)?
                .query_row([name], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            let Some((id, name, labels, ephemeral, public_key, group_id, group_name, client)) = row
            else {
                return Ok(None);
            };
            let client_id = match client {
                Some(client_id) => client_id,
                None => {
                    let synthesized =
                        format!("{id:08x}-0000-4000-8000-000000000000", id = id as u32);
                    tx.prepare_cached("UPDATE runners SET client_id = ?2 WHERE runner_id = ?1")
                        .map_err(db)?
                        .execute(params![id, synthesized])
                        .map_err(db)?;
                    synthesized
                }
            };
            Ok(Some((
                RegisteredRunner {
                    id,
                    name,
                    labels: serde_json::from_str(&labels).unwrap_or_default(),
                    ephemeral: ephemeral != 0,
                    public_key,
                    runner_group_id: group_id,
                    runner_group_name: group_name,
                },
                client_id,
            )))
        })
    }

    /// `bind_runner_client` (pg lifecycle.rs): attach a stable client
    /// identity; optionally re-pair with a pending job.
    pub(crate) async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.prepare_cached("UPDATE runners SET client_id = ?2 WHERE runner_id = ?1")
                .map_err(db)?
                .execute(params![runner_id, client_id])
                .map_err(db)?;
            if pair_with_pending_job {
                pair_runner_tx(tx, self, runner_id)?;
            }
            Ok(())
        })
    }

    /// `runner_rsa_public_key` (pg lifecycle.rs).
    pub(crate) async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>, ControlError> {
        self.read(|tx| {
            Ok(tx
                .prepare_cached("SELECT rsa_public_key FROM runners WHERE runner_id = ?1")
                .map_err(db)?
                .query_row([runner_id], |row| row.get::<_, Option<Vec<u8>>>(0))
                .optional()
                .map_err(db)?
                .flatten()
                .and_then(|xml| String::from_utf8(xml).ok())
                .and_then(|xml| preloop_gha_protocol::crypto::AgentRsaPublicKey::parse(&xml).ok()))
        })
    }

    /// `list_runners` (pg lifecycle.rs): every runner, plus the named run's
    /// ready-queue depth and how many registered runners could take one of
    /// those jobs.
    pub(crate) async fn list_runners(
        &self,
        run_id: Option<RunId>,
    ) -> Result<RunnerListing, ControlError> {
        self.read(|tx| {
            let mut runners = Vec::new();
            {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT runner_id, name, labels, ephemeral, public_key, \
                         runner_group_id, runner_group_name FROM runners \
                         ORDER BY runner_id",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        let labels: Vec<String> =
                            serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default();
                        Ok((
                            RegisteredRunner {
                                id: row.get(0)?,
                                name: row.get(1)?,
                                labels: labels.clone(),
                                ephemeral: row.get::<_, i64>(3)? != 0,
                                public_key: row.get(4)?,
                                runner_group_id: row.get(5)?,
                                runner_group_name: row.get(6)?,
                            },
                            RunnerCapabilities {
                                known: true,
                                labels,
                                runner_group_id: row.get(5)?,
                                runner_group_name: row.get(6)?,
                            },
                        ))
                    })
                    .map_err(db)?;
                for row in rows {
                    runners.push(row.map_err(db)?);
                }
            }
            let run_queue = match run_id {
                None => None,
                Some(run_id) => {
                    let ready: Vec<(Vec<String>, Option<String>)> = {
                        let mut stmt = tx
                            .prepare_cached(
                                "SELECT runs_on, runner_group FROM jobs \
                                 WHERE run_id = ?1 AND queue_state = 'ready'",
                            )
                            .map_err(db)?;
                        let rows = stmt
                            .query_map([codec::run_key(run_id)], |row| {
                                Ok((
                                    serde_json::from_str(&row.get::<_, String>(0)?)
                                        .unwrap_or_default(),
                                    row.get::<_, Option<String>>(1)?,
                                ))
                            })
                            .map_err(db)?;
                        rows.collect::<Result<Vec<_>, _>>().map_err(db)?
                    };
                    let mut claimable = BTreeSet::new();
                    for (runs_on, runner_group) in &ready {
                        for (runner, caps) in &runners {
                            if crate::runtime_scheduling::job_matches_runner(runs_on, &caps.labels)
                                && crate::runtime_scheduling::job_matches_runner_group(
                                    runner_group.as_deref(),
                                    caps,
                                )
                            {
                                claimable.insert(runner.id);
                            }
                        }
                    }
                    Some(RunQueueClaimability {
                        queued: ready.len(),
                        claimable: claimable.len(),
                    })
                }
            };
            Ok(RunnerListing {
                runners: runners.into_iter().map(|(runner, _)| runner).collect(),
                run_queue,
            })
        })
    }

    /// `create_broker_session` (pg lifecycle.rs): a broker `runner_sessions`
    /// row owned by `runner_id`. Dropped the encryption argument — the
    /// session key is caller-derived (`AppState::session_encryption`) and
    /// never stored.
    pub(crate) async fn create_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<(), ControlError> {
        self.open_runner_session(OpenRunnerSession {
            session_id: session_id.to_owned(),
            runner_id: Some(runner_id),
            protocol: SessionProtocol::Broker,
            verified: false,
            require_live_runner: true,
        })
        .await
    }

    /// `delete_broker_session` (pg lifecycle.rs): ownership-checked close.
    pub(crate) async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        self.close_runner_session(session_id, Some(runner_id)).await
    }

    /// `append_event` (pg lifecycle.rs → dispatch.rs `append_event_tx`):
    /// one outbox row, stamped with the reported row's version or dropped
    /// when that row already settled differently.
    pub(crate) async fn append_event(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        self.write(|tx| append_event_tx(tx, event))
    }

    /// `prune_outbox` (pg outbox.rs): delete up to `limit` outbox rows
    /// older than `older_than`, oldest first. Returns rows removed.
    pub(crate) async fn prune_outbox(
        &self,
        older_than: std::time::Duration,
        limit: usize,
    ) -> Result<u64, ControlError> {
        let cutoff = now_us() - older_than.as_micros() as i64;
        self.write(|tx| {
            tx.execute(
                "DELETE FROM outbox_events WHERE event_id IN ( \
                     SELECT e.event_id FROM outbox_events e \
                     WHERE e.created_at < ?1 AND \
                       e.event_id <= COALESCE((SELECT MIN(last_event_id) FROM consumer_offsets), \
                                              e.event_id) \
                     ORDER BY e.created_at LIMIT ?2)",
                params![cutoff, limit as i64],
            )
            .map(|n| n as u64)
            .map_err(db)
        })
    }

    /// The SQLite outbox position at the current end of the single-writer
    /// stream. SQLite event ids are commit order, so no txid safe point is
    /// needed.
    pub(crate) async fn outbox_head(&self) -> Result<OutboxBookmark, ControlError> {
        self.read(|tx| {
            let event_id: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(event_id),0) FROM outbox_events",
                    [],
                    |r| r.get(0),
                )
                .map_err(db)?;
            Ok(OutboxBookmark { txid: 0, event_id })
        })
    }

    pub(crate) async fn outbox_read(
        &self,
        after: OutboxBookmark,
        limit: usize,
    ) -> Result<Vec<OutboxRow>, ControlError> {
        self.read(|tx| {
            let mut stmt = tx
                .prepare(
                    "SELECT event_id,run_id,job_id,version,'' AS origin,topic,payload \
                 FROM outbox_events WHERE event_id > ?1 ORDER BY event_id LIMIT ?2",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map(params![after.event_id, limit as i64], |row| {
                    let run_id = match row.get::<_, Option<String>>(1)? {
                        Some(value) => Some(
                            value
                                .parse()
                                .map(RunId)
                                .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        ),
                        None => None,
                    };
                    Ok(OutboxRow {
                        bookmark: OutboxBookmark {
                            txid: 0,
                            event_id: row.get(0)?,
                        },
                        run_id,
                        job_id: row.get(2)?,
                        version: row.get(3)?,
                        origin: row.get(4)?,
                        topic: row.get(5)?,
                        payload: serde_json::from_str(&row.get::<_, String>(6)?)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                    })
                })
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)
        })
    }

    /// `reconcile_on_boot` (pg lifecycle.rs): drop holds/waits of dead or
    /// terminal runs, requeue claimed jobs whose session is gone, reset
    /// `expanding` nodes to `pending_expansion`.
    pub(crate) async fn reconcile_on_boot(&self) -> Result<ReconcileOutcome, ControlError> {
        self.write(|tx| {
            // Holds on dead runs release; their oldest waiters promote.
            let dead_holds: Vec<(String, String, String)> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT namespace_id, repository, group_name \
                         FROM concurrency_holds h WHERE NOT EXISTS ( \
                         SELECT 1 FROM runs r WHERE r.run_id = h.holder_run_id \
                         AND r.status <> 'completed')",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            let mut holders_dropped = 0;
            for (ns, repo, group) in dead_holds {
                // pg's promote_after_release: pop the oldest waiter and hand
                // it the slot; no waiter → delete the hold row.
                if concurrency::first_waiter(tx, &ns, &repo, &group)?.is_some() {
                    settle::promote_next_in_group(
                        tx,
                        self,
                        &ns,
                        &repo,
                        &group,
                        concurrency::hold_display_name(tx, &ns, &repo, &group)?.as_deref(),
                    )?;
                } else {
                    tx.prepare_cached(
                        "DELETE FROM concurrency_holds WHERE namespace_id = ?1 \
                         AND repository = ?2 AND group_name = ?3",
                    )
                    .map_err(db)?
                    .execute(params![ns, repo, group])
                    .map_err(db)?;
                }
                holders_dropped += 1;
            }
            // Wait rows of dead runs drop without promotion effects.
            tx.prepare_cached(
                "DELETE FROM concurrency_waits WHERE NOT EXISTS ( \
                 SELECT 1 FROM runs r WHERE r.run_id = concurrency_waits.holder_run_id \
                 AND r.status <> 'completed')",
            )
            .map_err(db)?
            .execute([])
            .map_err(db)?;
            // Orphaned claims: sessions gone (restart) → claimed jobs
            // release back to ready.
            let orphaned: Vec<(String, String)> = {
                let mut stmt = tx
                    .prepare_cached(
                        "SELECT j.run_id, j.job_id FROM jobs j \
                         WHERE j.queue_state = 'claimed' AND NOT EXISTS ( \
                         SELECT 1 FROM job_requests q WHERE q.run_id = j.run_id \
                         AND q.job_id = j.job_id AND q.result IS NULL \
                         AND q.session_id IS NOT NULL)",
                    )
                    .map_err(db)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(db)?;
                rows.collect::<Result<Vec<_>, _>>().map_err(db)?
            };
            let mut recovered = 0usize;
            let mut failed = 0usize;
            // A booting node has no live-log followers to close, so the
            // retired identities are not reported.
            let mut retired = Vec::new();
            for (run, job) in &orphaned {
                if requeue_claimed(tx, codec::run_id(run), &JobId(job.clone()), &mut retired)? {
                    recovered += 1;
                } else {
                    failed += 1;
                }
            }
            // Expanding nodes with no live lease reset to
            // pending_expansion: the generation fence already invalidates
            // stale applies.
            tx.prepare_cached(
                "UPDATE jobs SET queue_state = 'pending_expansion' \
                 WHERE queue_state = 'expanding'",
            )
            .map_err(db)?
            .execute([])
            .map_err(db)?;
            Ok(ReconcileOutcome {
                recovered,
                failed,
                holders_dropped,
            })
        })
    }

    /// `rebuild_dispatch_intent` (pg lifecycle.rs): reconcile assignment
    /// rows with the ready queue once the effective config is known —
    /// pair every unbound ready job, or clear bindings when assignments
    /// are off.
    pub(crate) async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError> {
        self.write(|tx| {
            let (pool_assignments, require_assignments, _) = self.config();
            if !pool_assignments && !require_assignments {
                tx.prepare_cached("DELETE FROM job_assignments")
                    .map_err(db)?
                    .execute([])
                    .map_err(db)?;
                tx.prepare_cached("DELETE FROM provision_requests")
                    .map_err(db)?
                    .execute([])
                    .map_err(db)?;
                return Ok(());
            }
            for job in jobs::ready_jobs(tx)? {
                let exists = tx
                    .prepare_cached(
                        "SELECT EXISTS(SELECT 1 FROM job_assignments \
                         WHERE run_id = ?1 AND job_id = ?2)",
                    )
                    .map_err(db)?
                    .query_row(params![codec::run_key(job.run_id), job.job_id.0], |row| {
                        row.get::<_, bool>(0)
                    })
                    .map_err(db)?;
                if !exists {
                    promote::on_job_enqueued(tx, self, &job)?;
                }
            }
            Ok(())
        })
    }

    /// `set_fork_approval`: write the run's fork-PR hold stamps. Scoped to
    /// the `runs` row (the expiry sweep filters on these columns); callers
    /// own promotion/fail-closed effects.
    pub(crate) async fn set_fork_approval(
        &self,
        update: ForkApprovalStamp,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.prepare_cached(
                "UPDATE runs SET fork_approval_pending = ?2, \
                     fork_approval_requested_at = ?3, \
                     fork_approval_approved_at = ?4, \
                     fork_approval_note = ?5 \
                 WHERE run_id = ?1",
            )
            .map_err(db)?
            .execute(params![
                codec::run_key(update.run_id),
                update.pending as i64,
                update.requested_at_unix_nanos,
                update.approved_at_unix_nanos,
                update.note,
            ])
            .map_err(db)?;
            Ok(())
        })
    }

    /// `set_environment_gate`: write one job's environment protection gate
    /// state (`None` clears it). The JSON blob round-trips through `jobs`
    /// so a restart re-arms an armed gate (fail closed).
    pub(crate) async fn set_environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
        gate: Option<crate::models::EnvironmentGateState>,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            let gate_json = gate
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(ControlError::backend)?;
            tx.prepare_cached(
                "UPDATE jobs SET environment_gate = ?3 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![codec::run_key(run_id), job_id.0, gate_json])
            .map_err(db)?;
            Ok(())
        })
    }

    /// `record_environment_approval`: append one operator approval to a job
    /// parked on its environment's required-reviewer gate, then re-run the
    /// run's promotion pass so a satisfied gate releases the job.
    ///
    /// A window that has already lapsed records nothing: the promotion pass
    /// (run next) re-evaluates the same window and settles the job
    /// `Failure` — one fail-closed decision over the durable rows.
    pub(crate) async fn record_environment_approval(
        &self,
        approval: EnvironmentApproval,
    ) -> Result<EnvironmentApprovalOutcome, ControlError> {
        let rules = self.environment_rules();
        let tx_rules = rules.clone();
        let run_id = approval.run_id;
        let job_id = approval.job_id;
        let note = approval.note.clone();
        let now = crate::models::now_unix_nanos();
        let result = self.write(move |tx| {
            let repository: String = tx
                .prepare_cached("SELECT repository FROM runs WHERE run_id = ?1")
                .map_err(db)?
                .query_row([codec::run_key(run_id)], |row| row.get(0))
                .optional()
                .map_err(db)?
                .ok_or_else(|| ControlError::NotFound("run not found".to_owned()))?;
            let job = jobs::job(tx, run_id, &job_id)?
                .ok_or_else(|| ControlError::NotFound("job not found".to_owned()))?;
            if job.status.is_terminal() {
                return Ok(EnvironmentApprovalResult::AlreadyTerminal);
            }
            let spec = jobs::load_spec(tx, run_id, &job_id)?;
            let environment = spec.and_then(|spec| spec.environment);
            let Some(env_name) =
                crate::runtime_scheduling::environment_gate_name_of(environment.as_ref())
            else {
                return Ok(EnvironmentApprovalResult::NotAwaiting);
            };
            let required = tx_rules
                .get(&repository)
                .and_then(|envs| envs.get(env_name))
                .map(|rule| rule.required_reviewers)
                .unwrap_or(0);
            let Some(mut gate) = job.environment_gate.clone() else {
                return Ok(EnvironmentApprovalResult::NotAwaiting);
            };
            let Some(requested_at) = gate.approval_requested_at_unix_nanos else {
                return Ok(EnvironmentApprovalResult::NotAwaiting);
            };
            if required == 0 {
                return Ok(EnvironmentApprovalResult::NotAwaiting);
            }
            if now.saturating_sub(requested_at)
                > crate::runtime_scheduling::ENVIRONMENT_APPROVAL_WINDOW_NANOS
            {
                return Ok(EnvironmentApprovalResult::Expired);
            }
            gate.approvals_unix_nanos.push(now);
            let approvals = gate.approvals_unix_nanos.len();
            let satisfied = (approvals as u32) >= required;
            let gate_json = serde_json::to_string(&gate).map_err(ControlError::backend)?;
            tx.prepare_cached(
                "UPDATE jobs SET environment_gate = ?3 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![codec::run_key(run_id), job_id.0, gate_json])
            .map_err(db)?;
            tracing::info!(
                run_id = %run_id.0,
                job_id = %job_id.0,
                environment = env_name,
                approvals,
                required,
                note = note.as_deref().unwrap_or_default(),
                "environment approval recorded"
            );
            Ok(EnvironmentApprovalResult::Recorded {
                approvals,
                required,
                satisfied,
            })
        })?;
        // Release or fail closed over the run's durable rows: a satisfied gate
        // unparks the job, an expired window settles it Failure (and its
        // dependents).
        let promote = self.promote_ready_jobs(Some(run_id), &rules).await?;
        Ok(EnvironmentApprovalOutcome {
            result,
            next_runs_on: promote.next_runs_on,
            promoted: promote.promoted,
        })
    }
    /// `set_reports_check_runs`: stamp whether intake reported GitHub check
    /// runs for this run (the late check-run mint gate).
    pub(crate) async fn set_reports_check_runs(
        &self,
        run_id: RunId,
        reported: bool,
    ) -> Result<(), ControlError> {
        self.write(|tx| {
            tx.prepare_cached("UPDATE runs SET reports_check_runs = ?2 WHERE run_id = ?1")
                .map_err(db)?
                .execute(params![codec::run_key(run_id), reported as i64])
                .map_err(db)?;
            Ok(())
        })
    }
}
