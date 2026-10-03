//! Claim/poll commands: `poll_session` (broker shape) and
//! `poll_azdo_session` (distributedtask shape). Translated from pg's
//! `dispatch.rs` claim path (sessions, redelivery, cancellation drain,
//! `claim_one`, `bind_claim`, message queueing). SQLite is single-writer:
//! `FOR UPDATE SKIP LOCKED` drops away; the claim's conditional
//! `UPDATE .. WHERE queue_state = 'ready'` stays.

use super::codec::{self, now_us};
use super::jobs;
use super::requests;
use super::{LiteBackend, db};
use crate::control::backend::PollRequest;
use crate::control::logic;
use crate::control::types::*;
use crate::models::{QueuedJob, RunnerCapabilities, TaskAgentJobRequestRecord};
use preloop_gha_protocol::{JobId, RunId, azdo};
use rusqlite::{OptionalExtension, Transaction, params};

/// One session row's identity as the claim path needs it.
struct SessionRef {
    session_uuid: String,
    runner_id: Option<i64>,
}

/// The session's row (mapped id), or `None` when it is unknown/expired.
fn session_ref(tx: &Transaction<'_>, session_id: &str) -> Result<Option<SessionRef>, ControlError> {
    let uuid = logic::session_uuid(session_id).to_string();
    tx.prepare_cached("SELECT runner_id FROM runner_sessions WHERE session_id = ?1")
        .map_err(db)?
        .query_row([uuid.as_str()], |row| row.get::<_, Option<i64>>(0))
        .optional()
        .map_err(db)
        .map(|runner_id| {
            runner_id.map(|runner_id| SessionRef {
                session_uuid: uuid.clone(),
                runner_id,
            })
        })
        .map(|opt| {
            opt.or_else(|| {
                tx.prepare_cached("SELECT runner_id FROM runner_sessions WHERE session_id = ?1")
                    .ok()
                    .and_then(|mut stmt| {
                        stmt.query_row([uuid.as_str()], |row| row.get::<_, Option<i64>>(0))
                            .optional()
                            .ok()
                            .flatten()
                            .map(|_| SessionRef {
                                session_uuid: uuid.clone(),
                                runner_id: None,
                            })
                    })
            })
        })
}

/// Touch a session's liveness stamp (a poll proves the runner is alive).
fn touch_session_row(tx: &Transaction<'_>, session_uuid: &str) -> Result<(), ControlError> {
    tx.prepare_cached("UPDATE runner_sessions SET last_seen_at = ?2 WHERE session_id = ?1")
        .map_err(db)?
        .execute(params![session_uuid, now_us()])
        .map_err(db)?;
    Ok(())
}

/// The oldest unacknowledged message of a session (redelivery-first).
/// `plaintext` is true for runner-less (compat) sessions: they never did
/// the key exchange, so their messages stay plaintext (core 8824a2ec).
fn oldest_session_message(
    tx: &Transaction<'_>,
    session_uuid: &str,
) -> Result<Option<SessionMessage>, ControlError> {
    tx.prepare_cached(
        "SELECT m.message_id, m.message_type, m.request_id, m.body, \
         s.runner_id IS NULL \
         FROM session_messages m \
         LEFT JOIN runner_sessions s ON s.session_id = m.session_id \
         WHERE m.session_id = ?1 ORDER BY m.message_id LIMIT 1",
    )
    .map_err(db)?
    .query_row([session_uuid], |row| {
        Ok(SessionMessage {
            message_id: row.get(0)?,
            message_type: row.get(1)?,
            request_id: row.get(2)?,
            body: row.get(3)?,
            plaintext: row.get::<_, Option<bool>>(4)?.unwrap_or(true),
        })
    })
    .optional()
    .map_err(db)
}

/// The session's live request, when it holds one.
fn session_active_request(
    tx: &Transaction<'_>,
    session_uuid: &str,
) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
    let select = format!(
        "SELECT {} FROM {} WHERE q.session_id = ?1 AND q.result IS NULL \
         ORDER BY q.request_id DESC LIMIT 1",
        requests::RECORD_COLUMNS,
        requests::RECORD_FROM
    );
    tx.prepare_cached(&select)
        .map_err(db)?
        .query_row([session_uuid], requests::record_row)
        .optional()
        .map_err(db)
}

/// Queue a `JobCancellation` for a session's active attempt: the
/// cancellation row is marked delivered and a session message carrying the
/// official body is appended.
fn queue_cancellation_message(
    tx: &Transaction<'_>,
    session_uuid: &str,
    runner_id: Option<i64>,
    request_id: i64,
    agent_job_id: uuid::Uuid,
) -> Result<SessionMessage, ControlError> {
    tx.prepare_cached(
        "UPDATE job_cancellations SET delivered_at = ?2 \
         WHERE request_id = ?1 AND delivered_at IS NULL",
    )
    .map_err(db)?
    .execute(params![request_id, now_us()])
    .map_err(db)?;
    let body = crate::concurrency::job_cancel_body(agent_job_id);
    let message_id: i64 = tx
        .prepare_cached(
            "INSERT INTO session_messages (session_id, message_type, request_id, body) \
             VALUES (?1, ?2, ?3, ?4) RETURNING message_id",
        )
        .map_err(db)?
        .query_row(
            params![
                session_uuid,
                azdo::message_type::JOB_CANCELLED,
                request_id,
                body,
            ],
            |row| row.get(0),
        )
        .map_err(db)?;
    Ok(SessionMessage {
        message_id,
        message_type: azdo::message_type::JOB_CANCELLED.to_owned(),
        request_id: Some(request_id),
        body: Some(body),
        // Session-less (compat) runners never exchanged a key.
        plaintext: runner_id.is_none(),
    })
}

/// A pending (undelivered) cancellation for the attempt, if any.
fn pending_cancellation(
    tx: &Transaction<'_>,
    request_id: i64,
) -> Result<Option<u64>, ControlError> {
    tx.prepare_cached(
        "SELECT request_id FROM job_cancellations \
         WHERE request_id = ?1 AND delivered_at IS NULL LIMIT 1",
    )
    .map_err(db)?
    .query_row([request_id], |row| row.get::<_, i64>(0))
    .optional()
    .map_err(db)
    .map(|opt| opt.map(|id| id.max(0) as u64))
}

/// `claim_one` (pg dispatch.rs): the ready job this runner should take,
/// chosen by the shared four-tier preference over the ready queue in
/// dispatch order. The queue is read in pages until a candidate matches —
/// a 64-row window must not hide a job a runner can serve. The conditional
/// UPDATE is the claim fence.
fn claim_one(
    tx: &Transaction<'_>,
    runner_id: Option<i64>,
    verified_runner_id: Option<i64>,
    caps: &RunnerCapabilities,
    require_assignments: bool,
) -> Result<Option<(RunId, JobId)>, ControlError> {
    type ReadyRow = (
        String,
        String,
        Vec<String>,
        Option<String>,
        i64,
        Option<(Option<i64>, bool, bool, bool)>,
        Option<i64>,
    );
    let now = now_us();
    let fresh_after = now - crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
    let verified = verified_runner_id.is_some();
    let runner_match = logic::RunnerMatchRow {
        labels: caps.labels.clone(),
        known: caps.known,
        group_id: caps.runner_group_id,
        group_name: caps.runner_group_name.clone(),
    };
    // Page the ready queue in dispatch order until a candidate matches or the
    // ready set is exhausted: a runner whose own pool sorts past the first
    // batch must still see the jobs it can serve.
    let mut offset: i64 = 0;
    loop {
        let rows: Vec<ReadyRow> = {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT j.run_id, j.job_id, j.runs_on, j.runner_group, \
                     j.enqueued_at, \
                     a.runner_id, \
                     (a.run_id IS NOT NULL), \
                     (a.assigned_at IS NOT NULL AND a.assigned_at > ?1), \
                     (a.first_assigned_at IS NOT NULL AND a.first_assigned_at > ?1), \
                     (a.runner_id IS NOT NULL AND EXISTS( \
                        SELECT 1 FROM runners r WHERE r.runner_id = a.runner_id)), \
                     p.requested_at \
                     FROM jobs j \
                     LEFT JOIN job_assignments a ON a.run_id = j.run_id \
                        AND a.job_id = j.job_id \
                     LEFT JOIN provision_requests p ON p.run_id = j.run_id \
                        AND p.job_id = j.job_id \
                     WHERE j.queue_state = 'ready' \
                     ORDER BY j.pool_key, j.priority DESC, j.run_order, j.job_order \
                     LIMIT 64 OFFSET ?2",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map(params![fresh_after, offset], |row| {
                    let assigned: Option<i64> = row.get(5)?;
                    let assignment_exists: bool = row.get(6)?;
                    let fresh: bool = row.get(7)?;
                    let first_fresh: bool = row.get(8)?;
                    let registered: bool = row.get(9)?;
                    let enqueued: Option<i64> = row.get(4)?;
                    // Raw `p.requested_at`: NULL marks a non-matching LEFT JOIN
                    // (no provision row) — `provision_fresh` is tri-state in Rust.
                    let provision_at: Option<i64> = row.get(10)?;
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        serde_json::from_str(&row.get::<_, String>(2)?).unwrap_or_default(),
                        row.get::<_, Option<String>>(3)?,
                        enqueued.unwrap_or(0),
                        assignment_exists.then_some((assigned, fresh, first_fresh, registered)),
                        provision_at,
                    ))
                })
                .map_err(db)?;
            rows.collect::<Result<Vec<_>, _>>().map_err(db)?
        };
        let exhausted = rows.len() < 64;
        let mut candidates = Vec::with_capacity(rows.len());
        for (position, (run, job, runs_on, runner_group, enqueued_at, assignment, provision_at)) in
            rows.into_iter().enumerate()
        {
            let provision_fresh = provision_at.map(|at| at > fresh_after);
            let (assigned_runner_id, assignment_fresh, first_assigned_fresh, runner_registered) =
                assignment.unwrap_or((None, false, false, false));
            // `claim_permitted` verbatim: assignment rows bind verified sessions
            // while fresh, stale/orphaned bindings open to any verified caller,
            // a fresh pool-pending row blocks everyone, and unassigned jobs are
            // only claimable when strict assignments are off.
            let enqueue_ceiling_expired = enqueued_at > 0
                && now.saturating_sub(enqueued_at)
                    >= crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
            // The old model swept stale bindings before checking in permissive
            // mode — a stale assignment/pool_pending row counts as absent there.
            let assignment = if !require_assignments && !assignment_fresh {
                None
            } else {
                assignment
            };
            let provision_fresh = if !require_assignments && provision_fresh == Some(false) {
                None
            } else {
                provision_fresh
            };
            let claimable = if assignment.is_some() {
                if !first_assigned_fresh || enqueue_ceiling_expired {
                    verified
                } else {
                    match assigned_runner_id {
                        None => verified,
                        Some(id) => {
                            if !runner_registered || !assignment_fresh {
                                verified
                            } else {
                                Some(id) == verified_runner_id
                            }
                        }
                    }
                }
            } else if provision_fresh == Some(true) && !enqueue_ceiling_expired {
                false
            } else if provision_fresh.is_some() {
                verified
            } else {
                !require_assignments
            };
            candidates.push(logic::ClaimCandidate {
                run_id: codec::run_id(&run),
                job_id: JobId(job),
                runs_on,
                runner_group,
                assigned_runner_id,
                assignment_fresh,
                queue_position: position as u64,
                claimable,
            });
        }
        // `assigned_to_this_runner` keys off the proven runner id: an
        // unverified session cannot satisfy an assignment binding even by
        // name.
        let Some(index) = logic::claim_preference(
            &candidates,
            verified_runner_id,
            &caps.labels,
            None,
            &runner_match,
        ) else {
            if exhausted {
                return Ok(None);
            }
            offset += 64;
            continue;
        };
        let chosen = candidates.swap_remove(index);
        let claimed = tx
            .prepare_cached(
                "UPDATE jobs SET queue_state = 'claimed', status = 'in_progress', \
                 claimed_by_runner_id = ?3, claimed_at = ?4 \
                 WHERE run_id = ?1 AND job_id = ?2 AND queue_state = 'ready'",
            )
            .map_err(db)?
            .execute(params![
                codec::run_key(chosen.run_id),
                chosen.job_id.0,
                runner_id,
                now_us()
            ])
            .map_err(db)?;
        if claimed == 0 {
            return Ok(None);
        }
        return Ok(Some((chosen.run_id, chosen.job_id)));
    }
}

/// `bind_claim` (pg dispatch.rs): bind the claimed attempt — request
/// owner/session/start stamps, the lease row, the run's `in_progress`
/// transition, the dispatch-intent drop.
fn bind_claim(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    session_uuid: &str,
    runner_id: Option<i64>,
) -> Result<Option<TaskAgentJobRequestRecord>, ControlError> {
    let run = codec::run_key(run_id);
    let select = format!(
        "SELECT {} FROM {} WHERE q.run_id = ?1 AND q.job_id = ?2 \
         AND q.result IS NULL ORDER BY q.request_id DESC LIMIT 1",
        requests::RECORD_COLUMNS,
        requests::RECORD_FROM
    );
    let record = tx
        .prepare_cached(&select)
        .map_err(db)?
        .query_row(params![run, job_id.0], requests::record_row)
        .optional()
        .map_err(db)?;
    let Some(record) = record else {
        return Ok(None);
    };
    let now = now_us();
    tx.prepare_cached(
        "UPDATE job_requests SET session_id = ?2, runner_id = ?3, \
         claimed_at = ?4, started_at = ?4 WHERE request_id = ?1",
    )
    .map_err(db)?
    .execute(params![record.request_id, session_uuid, runner_id, now])
    .map_err(db)?;
    // The lease is the heartbeat target; only claimed attempts carry one.
    if let Some(runner_id) = runner_id {
        let expires = codec::parse_lease(&crate::distributed_task::agent_request_locked_until())?;
        tx.prepare_cached(
            "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (request_id) DO UPDATE SET runner_id = excluded.runner_id, \
             expires_at = excluded.expires_at, renewed_at = excluded.renewed_at",
        )
        .map_err(db)?
        .execute(params![record.request_id, runner_id, expires, now])
        .map_err(db)?;
    }
    tx.prepare_cached("DELETE FROM job_assignments WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    tx.prepare_cached("DELETE FROM provision_requests WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![run, job_id.0])
        .map_err(db)?;
    tx.prepare_cached(
        "UPDATE runs SET status = 'in_progress', \
         started_at = COALESCE(started_at, ?2) \
         WHERE run_id = ?1 AND status = 'queued'",
    )
    .map_err(db)?
    .execute(params![run, now])
    .map_err(db)?;
    touch_session_row(tx, session_uuid)?;
    Ok(Some(record))
}

/// Append the job-request session message a poll answers with.
fn queue_job_message(
    tx: &Transaction<'_>,
    session_uuid: &str,
    runner_id: Option<i64>,
    request_id: i64,
) -> Result<SessionMessage, ControlError> {
    let message_id: i64 = tx
        .prepare_cached(
            "INSERT INTO session_messages (session_id, message_type, request_id) \
             VALUES (?1, ?2, ?3) RETURNING message_id",
        )
        .map_err(db)?
        .query_row(
            params![
                session_uuid,
                azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
                request_id
            ],
            |row| row.get(0),
        )
        .map_err(db)?;
    Ok(SessionMessage {
        message_id,
        message_type: azdo::message_type::PIPELINE_AGENT_JOB_REQUEST.to_owned(),
        request_id: Some(request_id),
        body: None,
        plaintext: runner_id.is_none(),
    })
}

/// A registered runner's capabilities for matching (labels + group).
fn runner_capabilities(
    tx: &Transaction<'_>,
    runner_id: i64,
) -> Result<Option<RunnerCapabilities>, ControlError> {
    tx.prepare_cached(
        "SELECT labels, runner_group_id, runner_group_name FROM runners \
         WHERE runner_id = ?1",
    )
    .map_err(db)?
    .query_row([runner_id], |row| {
        Ok(RunnerCapabilities {
            known: true,
            labels: serde_json::from_str(&row.get::<_, String>(0)?).unwrap_or_default(),
            runner_group_id: row.get(1)?,
            runner_group_name: row.get(2)?,
        })
    })
    .optional()
    .map_err(db)
}

/// The claim bookkeeping shared by both polls after `bind_claim`: stamp
/// the job's start markers already written by `claim_one`'s UPDATE
/// (`started_at` on jobs) and emit the dispatch outbox row.
fn finish_claim(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
) -> Result<Option<QueuedJob>, ControlError> {
    tx.prepare_cached(
        "UPDATE jobs SET started_at = COALESCE(started_at, ?3) \
         WHERE run_id = ?1 AND job_id = ?2",
    )
    .map_err(db)?
    .execute(params![codec::run_key(run_id), job_id.0, now_us()])
    .map_err(db)?;
    jobs::emit_outbox(
        tx,
        jobs::namespace_of(tx, run_id)?.as_str(),
        Some(run_id),
        "job.started.v1",
        serde_json::json!({"job_id": job_id.0}),
    )?;
    jobs::queued_job(tx, run_id, job_id)
}

impl LiteBackend {
    /// `poll_session` (pg dispatch.rs): redelivery, cancellation, active
    /// request, then a claim — all inside the claim transaction so a
    /// liveness sweep cannot rebind mid-poll.
    pub(crate) async fn poll_session(
        &self,
        poll: PollRequest,
    ) -> Result<PollOutcome, ControlError> {
        let (_, require_assignments, _) = self.config();
        self.write(|tx| {
            // Ownership is revalidated inside the claim transaction: the
            // handler caches the runner across a long poll, and a liveness
            // sweep can purge the session while it waits.
            let Some(session) = session_ref(tx, &poll.session_id)? else {
                return Err(ControlError::Forbidden(
                    "session has no runner owner".to_owned(),
                ));
            };
            if poll.verified_runner_id.is_some() && poll.verified_runner_id != session.runner_id {
                return Err(ControlError::Forbidden(
                    "session belongs to another runner".to_owned(),
                ));
            }
            touch_session_row(tx, &session.session_uuid)?;
            if let Some(message) = oldest_session_message(tx, &session.session_uuid)? {
                return Ok(PollOutcome::Inflight(azdo::TaskAgentMessage {
                    message_id: message.message_id,
                    message_type: message.message_type.clone(),
                    body: message.runner_body(),
                    iv: None,
                }));
            }
            if let Some(request) = session_active_request(tx, &session.session_uuid)? {
                if pending_cancellation(tx, request.request_id)?.is_some() {
                    let message = queue_cancellation_message(
                        tx,
                        &session.session_uuid,
                        session.runner_id,
                        request.request_id,
                        request.agent_job_id,
                    )?;
                    return Ok(PollOutcome::Cancel(azdo::TaskAgentMessage {
                        message_id: message.message_id,
                        message_type: message.message_type,
                        body: message.body.unwrap_or_default(),
                        iv: None,
                    }));
                }
                let runner_id = session.runner_id.unwrap_or(0);
                return Ok(PollOutcome::ActiveRequest { request, runner_id });
            }
            if poll.busy {
                return Ok(PollOutcome::Empty);
            }
            let Some((run_id, job_id)) = claim_one(
                tx,
                session.runner_id,
                // Only a token-verified identity may satisfy a binding: the
                // session's self-declared id is not proof (see the azdo twin).
                poll.verified_runner_id,
                &poll.runner,
                require_assignments,
            )?
            else {
                return Ok(PollOutcome::Empty);
            };
            let Some(request) = bind_claim(
                tx,
                run_id,
                &job_id,
                &session.session_uuid,
                session.runner_id,
            )?
            else {
                return Ok(PollOutcome::Empty);
            };
            // A `PollOutcome::Claimed` is answer-shaped, not a session
            // message: parking a JobRequest row here would shadow the
            // cancel/active checks on the next poll (the azdo variant does
            // park one — that contract is message-based).
            let Some(queued) = finish_claim(tx, run_id, &job_id)? else {
                return Ok(PollOutcome::Empty);
            };
            let runner_id = session.runner_id.unwrap_or(0);
            Ok(PollOutcome::Claimed(Box::new(ClaimedJob {
                queued,
                request,
                runner_id,
                queue_depth: jobs::ready_count(tx)?,
                next_runs_on: jobs::next_ready_labels(tx)?,
            })))
        })
    }

    /// `poll_azdo_session` (pg dispatch.rs): the distributedtask poll shape
    /// — redeliver, cancel, then claim — returning the session message row
    /// the handler renders.
    pub(crate) async fn poll_azdo_session(
        &self,
        poll: AzdoPoll,
    ) -> Result<AzdoPollOutcome, ControlError> {
        let (_, require_assignments, _) = self.config();
        self.write(|tx| {
            let session = match session_ref(tx, &poll.session_id)? {
                Some(session) => session,
                None if poll.session_id.parse::<uuid::Uuid>().is_err() => {
                    // The implicit compat session (legacy `sessionId=default`,
                    // any non-UUID id) owns no runner: its row is materialized
                    // lazily so `session_messages`/`job_requests` foreign keys
                    // resolve, and `plaintext` rendering keeps messages
                    // decodable without a key exchange.
                    let uuid = logic::session_uuid(&poll.session_id).to_string();
                    tx.prepare_cached(
                        "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                         verified, created_at, last_seen_at) \
                         VALUES (?1, NULL, 'azdo', 0, ?2, ?2) ON CONFLICT DO NOTHING",
                    )
                    .map_err(db)?
                    .execute(params![uuid, now_us()])
                    .map_err(db)?;
                    SessionRef {
                        session_uuid: uuid,
                        runner_id: None,
                    }
                }
                // An unknown keyed session is answered like a foreign one:
                // the runner must re-register rather than receive work it
                // cannot decode.
                None => return Ok(AzdoPollOutcome::Forbidden),
            };
            if let Some(verified) = poll.verified_runner_id
                && session.runner_id != Some(verified)
            {
                return Ok(AzdoPollOutcome::Forbidden);
            }
            touch_session_row(tx, &session.session_uuid)?;
            if let Some(message) = oldest_session_message(tx, &session.session_uuid)? {
                return Ok(AzdoPollOutcome::Redeliver(message));
            }
            if let Some(request) = session_active_request(tx, &session.session_uuid)? {
                if pending_cancellation(tx, request.request_id)?.is_some() {
                    let message = queue_cancellation_message(
                        tx,
                        &session.session_uuid,
                        session.runner_id,
                        request.request_id,
                        request.agent_job_id,
                    )?;
                    return Ok(AzdoPollOutcome::Cancel(message));
                }
                // Still executing: the runner keeps polling until it
                // finishes.
                return Ok(AzdoPollOutcome::Wait);
            }
            let caps = match session.runner_id {
                // A session can name a runner id whose registration has not
                // landed (or is gone): capabilities stay unknown, matching the
                // old model — permissive matching, no `Wait` starvation.
                Some(runner_id) => match runner_capabilities(tx, runner_id)? {
                    Some(caps) => caps,
                    None => RunnerCapabilities {
                        known: false,
                        labels: Vec::new(),
                        runner_group_id: None,
                        runner_group_name: None,
                    },
                },
                // A compat session has no registered runner: its labels are
                // unknown, which the shared matcher treats as permissive.
                None => RunnerCapabilities {
                    known: false,
                    labels: Vec::new(),
                    runner_group_id: None,
                    runner_group_name: None,
                },
            };
            let Some((run_id, job_id)) = claim_one(
                tx,
                session.runner_id,
                poll.verified_runner_id,
                &caps,
                require_assignments,
            )?
            else {
                return Ok(AzdoPollOutcome::Wait);
            };
            let Some(request) = bind_claim(
                tx,
                run_id,
                &job_id,
                &session.session_uuid,
                session.runner_id,
            )?
            else {
                return Ok(AzdoPollOutcome::Wait);
            };
            let message = queue_job_message(
                tx,
                &session.session_uuid,
                session.runner_id,
                request.request_id,
            )?;
            finish_claim(tx, run_id, &job_id)?;
            Ok(AzdoPollOutcome::Claimed {
                message,
                run_id,
                job_id,
                queue_depth: jobs::ready_count(tx)?,
                next_runs_on: jobs::next_ready_labels(tx)?,
            })
        })
    }
}
