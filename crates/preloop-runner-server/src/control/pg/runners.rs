//! Runner registration, attempt leases (`job_leases`), attempt settlement
//! and release, reaper inputs and binding sweeps.
//!
//! Lease model: `job_leases` holds one row per claimed attempt
//! (`runner_id`, `expires_at`, `renewed_at`). A renewal is one upsert on
//! that narrow row. The protocol's `lockedUntil` string is `expires_at`
//! rendered by the minting helper; an attempt without a lease row reads
//! `lockedUntil = ""`. Clearing an attempt's owner deletes its lease.
//!
//! "The session claims the request" is `job_requests.session_id IS NOT NULL
//! AND result IS NULL`.

use super::codec::{self, now_us, ts, us};
use super::lookups::{REQUEST_SELECT, request_from_row};
use super::{PgBackend, db};
use crate::control::backend::RegisterRunner;
use crate::control::types::{
    ActiveRequest, ControlError, DEFAULT_NAMESPACE, ReadyRow, ReapInputs, RunnerRow, renew_miss,
    status_str,
};
use crate::models::TaskAgentJobRequestRecord;
use preloop_gha_protocol::crypto::AgentRsaPublicKey;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use tokio_postgres::GenericClient;

/// Columns of every runner read, in [`runner_from_row`] order.
pub(super) const RUNNER_COLUMNS: &str = concat!(
    "runner_id, name, labels::text, ephemeral, public_key, rsa_public_key, runner_group_id, \
     runner_group_name, client_id, pool_proven, ",
    us!("registered_at")
);

/// Decode one row selected with [`RUNNER_COLUMNS`].
pub(super) fn runner_from_row(row: &tokio_postgres::Row) -> Result<RunnerRow, ControlError> {
    let rsa: Option<&[u8]> = row.get(5);
    Ok(RunnerRow {
        runner: preloop_gha_protocol::RegisteredRunner {
            id: row.get(0),
            name: row.get(1),
            labels: codec::from_json(row.get(2))?,
            ephemeral: row.get(3),
            public_key: row.get(4),
            runner_group_id: row.get(6),
            runner_group_name: row.get(7),
        },
        rsa_public_key: rsa
            .and_then(|xml| std::str::from_utf8(xml).ok())
            .and_then(|xml| AgentRsaPublicKey::parse(xml).ok()),
        client_id: row.get(8),
        pool_proven: row.get(9),
        registered_at_us: row.get(10),
    })
}

/// Upsert of an attempt's lease: `$1` request id, `$2` expiry µs, `$3`
/// renewal µs. The holder is the recorded owner, else the claiming
/// session's runner; an attempt with neither (a compat session) has no
/// lease row. Only in-flight attempts are leased.
const LEASE_UPSERT: &str = concat!(
    "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
     SELECT q.request_id, COALESCE(q.runner_id, s.runner_id), ",
    ts!("$2"),
    ", ",
    ts!("$3"),
    " FROM job_requests q LEFT JOIN runner_sessions s ON s.session_id = q.session_id \
     WHERE q.request_id = $1 AND q.result IS NULL \
       AND COALESCE(q.runner_id, s.runner_id) IS NOT NULL \
     ON CONFLICT (request_id) DO UPDATE SET expires_at = EXCLUDED.expires_at, \
       renewed_at = EXCLUDED.renewed_at"
);

/// The lease expiry a fresh claim/renewal gets.
fn fresh_lease_us() -> Result<i64, ControlError> {
    codec::locked_until_us(&crate::distributed_task::agent_request_locked_until())?
        .ok_or_else(|| ControlError::backend(anyhow::anyhow!("empty lease deadline")))
}

impl PgBackend {
    /// Register a runner, deduplicated on `client_id`: a re-register returns
    /// the existing row unchanged.
    ///
    /// Statements: `INSERT INTO runners .. ON CONFLICT (client_id) DO
    /// NOTHING RETURNING ..`; on conflict `SELECT .. FROM runners WHERE
    /// client_id = $1`.
    pub(super) async fn register_runner_row(
        &self,
        reg: RegisterRunner,
    ) -> Result<(RunnerRow, bool), ControlError> {
        let labels = codec::json(&reg.labels)?;
        let rsa = reg
            .rsa_public_key
            .as_ref()
            .map(|key| key.to_xml_string().into_bytes());
        let client = self.writer().await?;
        let inserted = client
            .query_opt(
                &format!(
                    "INSERT INTO runners (namespace_id, name, labels, ephemeral, public_key, \
                     rsa_public_key, runner_group_id, runner_group_name, client_id, pool_proven) \
                     VALUES ($1, $2, $3::text::jsonb, $4, $5, $6, $7, $8, $9, $10) \
                     ON CONFLICT (client_id) DO NOTHING RETURNING {RUNNER_COLUMNS}"
                ),
                &[
                    &DEFAULT_NAMESPACE,
                    &reg.name,
                    &labels,
                    &reg.ephemeral,
                    &reg.public_key,
                    &rsa,
                    &reg.runner_group_id,
                    &reg.runner_group_name,
                    &reg.client_id,
                    &reg.pool_proven,
                ],
            )
            .await
            .map_err(db)?;
        if let Some(row) = inserted {
            return Ok((runner_from_row(&row)?, true));
        }
        let row = client
            .query_one(
                &format!("SELECT {RUNNER_COLUMNS} FROM runners WHERE client_id = $1"),
                &[&reg.client_id],
            )
            .await
            .map_err(db)?;
        Ok((runner_from_row(&row)?, false))
    }

    /// Renew a claimed attempt's lease by request id for its recorded owner.
    /// `NotFound` for an unknown request, `Stale` when `runner_id` is not
    /// the owner. Returns the renewed record.
    ///
    /// Statements (one transaction): `SELECT runner_id FROM job_requests
    /// WHERE request_id FOR NO KEY UPDATE`; lease upsert; request read.
    pub(super) async fn renew_request(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<TaskAgentJobRequestRecord, ControlError> {
        let expires = fresh_lease_us()?;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let owner: Option<i64> = tx
            .query_opt(
                "SELECT runner_id FROM job_requests WHERE request_id = $1 FOR NO KEY UPDATE",
                &[&request_id],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| ControlError::NotFound(format!("request {request_id}")))?
            .get(0);
        if owner != Some(runner_id) {
            return Err(ControlError::Stale(format!(
                "request {request_id} not owned by runner {runner_id}"
            )));
        }
        tx.execute(LEASE_UPSERT, &[&request_id, &expires, &now_us()])
            .await
            .map_err(db)?;
        let row = tx
            .query_one(
                &format!("{REQUEST_SELECT} WHERE q.request_id = $1"),
                &[&request_id],
            )
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        request_from_row(&row)
    }

    /// Release an interrupted claim for redelivery: an in-flight attempt
    /// drops its session binding, owner, start stamp and timeout flag; its
    /// lease is deleted. Returns whether an in-flight attempt was released.
    ///
    /// Statements (one transaction): `UPDATE job_requests SET session_id =
    /// NULL, runner_id = NULL, started_at = NULL, timeout_triggered = false
    /// WHERE request_id AND result IS NULL`; `DELETE FROM job_leases WHERE
    /// request_id`.
    pub(super) async fn release_claimed_request(
        &self,
        request_id: i64,
        _locked_until: &str,
    ) -> Result<bool, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let released = tx
            .execute(
                "UPDATE job_requests SET session_id = NULL, runner_id = NULL, started_at = NULL, \
                 timeout_triggered = false WHERE request_id = $1 AND result IS NULL",
                &[&request_id],
            )
            .await
            .map_err(db)?;
        if released > 0 {
            tx.execute(
                "DELETE FROM job_leases WHERE request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(released > 0)
    }

    /// `release_request` = [`Self::release_claimed_request`] without a
    /// result (an unknown or settled request is a no-op).
    pub(super) async fn release_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.release_claimed_request(request_id, "")
            .await
            .map(|_| ())
    }

    /// Renew an attempt's lease by agent job id for its recorded owner with
    /// one conditional upsert. On a miss, classify: unknown attempt
    /// `NotFound`, finished `Conflict`, foreign owner `Forbidden`, no owner
    /// `Ok(false)` (caller falls back to the broker path). An unparsable
    /// `locked_until` is `BadRequest` only once the owner checks pass.
    ///
    /// Statements: `INSERT INTO job_leases .. SELECT .. FROM job_requests
    /// WHERE agent_job_id AND result IS NULL AND runner_id = $runner ON
    /// CONFLICT DO UPDATE`; on a miss `SELECT result, runner_id FROM
    /// job_requests WHERE agent_job_id`.
    pub(super) async fn renew_lease(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let agent = agent_job_id.to_string();
        let client = self.writer().await?;
        let expires = codec::locked_until_us(locked_until);
        if let Ok(Some(expires)) = expires {
            let renewed = client
                .execute(
                    concat!(
                        "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                         SELECT request_id, runner_id, ",
                        ts!("$3"),
                        ", ",
                        ts!("$4"),
                        " FROM job_requests WHERE agent_job_id = $1::text::uuid \
                         AND result IS NULL AND runner_id = $2 \
                         ON CONFLICT (request_id) DO UPDATE SET expires_at = EXCLUDED.expires_at, \
                         renewed_at = EXCLUDED.renewed_at"
                    ),
                    &[&agent, &runner_id, &expires, &now_us()],
                )
                .await
                .map_err(db)?;
            if renewed == 1 {
                return Ok(true);
            }
        }
        let row = client
            .query_opt(
                "SELECT result, runner_id FROM job_requests WHERE agent_job_id = $1::text::uuid",
                &[&agent],
            )
            .await
            .map_err(db)?
            .map(|row| {
                (
                    row.get::<_, Option<String>>(0),
                    row.get::<_, Option<i64>>(1),
                )
            });
        let owned_in_flight = matches!(row, Some((None, Some(owner))) if owner == runner_id);
        let miss = renew_miss(row, runner_id)?;
        match expires {
            Err(error) if owned_in_flight => Err(error),
            Ok(None) if owned_in_flight => {
                Err(ControlError::BadRequest("empty lockedUntil".to_owned()))
            }
            _ => Ok(miss),
        }
    }

    /// `(recorded owner, claiming session's runner, session-bound)` for a
    /// request; `None` when the request does not exist.
    ///
    /// Statement: `SELECT q.runner_id, s.runner_id, s.session_id IS NOT NULL
    /// FROM job_requests q LEFT JOIN runner_sessions s ON s.session_id =
    /// q.session_id AND q.result IS NULL WHERE q.request_id = $1`.
    pub(super) async fn request_owner(
        &self,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        Self::request_owner_on(&*self.reader().await?, request_id).await
    }

    /// [`Self::request_owner`] on a caller's connection or open transaction.
    pub(super) async fn request_owner_on(
        client: &impl GenericClient,
        request_id: i64,
    ) -> Result<Option<(Option<i64>, Option<i64>, bool)>, ControlError> {
        Ok(client
            .query_opt(
                "SELECT q.runner_id, s.runner_id, s.session_id IS NOT NULL FROM job_requests q \
                 LEFT JOIN runner_sessions s ON s.session_id = q.session_id AND q.result IS NULL \
                 WHERE q.request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(db)?
            .map(|row| (row.get(0), row.get(1), row.get(2))))
    }

    /// Broker-path renew: resolve the attempt, apply the recorded-owner →
    /// session-runner → session-bound ownership ladder, then renew its lease.
    /// Errors: `NotFound` (unknown / never assigned), `Forbidden` (foreign
    /// owner), `Conflict` (already completed).
    ///
    /// Statements (one transaction): `SELECT q.request_id, q.runner_id,
    /// q.result IS NOT NULL, s.runner_id, s.session_id IS NOT NULL .. FOR NO
    /// KEY UPDATE OF q`; lease upsert.
    pub(super) async fn renew_broker_request(
        &self,
        agent_job_id: uuid::Uuid,
        runner_id: i64,
        locked_until: &str,
    ) -> Result<(), ControlError> {
        let agent = agent_job_id.to_string();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let row = tx
            .query_opt(
                "SELECT q.request_id, q.runner_id, q.result IS NOT NULL, s.runner_id, \
                 s.session_id IS NOT NULL FROM job_requests q \
                 LEFT JOIN runner_sessions s ON s.session_id = q.session_id AND q.result IS NULL \
                 WHERE q.agent_job_id = $1::text::uuid FOR NO KEY UPDATE OF q",
                &[&agent],
            )
            .await
            .map_err(db)?
            .ok_or_else(|| ControlError::NotFound("broker renew request not found".to_owned()))?;
        let request_id: i64 = row.get(0);
        let owner: Option<i64> = row.get(1);
        let settled: bool = row.get(2);
        let session_runner: Option<i64> = row.get(3);
        let has_session: bool = row.get(4);
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
        let expires = codec::locked_until_us(locked_until)?
            .ok_or_else(|| ControlError::BadRequest("empty lockedUntil".to_owned()))?;
        tx.execute(LEASE_UPSERT, &[&request_id, &expires, &now_us()])
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// Renew an in-flight AgentRequest's lease; `false` for a completed,
    /// unknown or holder-less request (the PATCH contract renews nothing
    /// silently, and an attempt with neither a recorded owner nor a claiming
    /// session's runner has no lease to extend).
    ///
    /// Statements (one transaction): `SELECT 1 FROM job_requests WHERE
    /// request_id AND result IS NULL FOR NO KEY UPDATE`; lease upsert.
    pub(super) async fn renew_agent_request(
        &self,
        request_id: i64,
        locked_until: &str,
    ) -> Result<bool, ControlError> {
        let expires = codec::locked_until_us(locked_until)?
            .ok_or_else(|| ControlError::BadRequest("empty lockedUntil".to_owned()))?;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let in_flight = tx
            .query_opt(
                "SELECT 1 FROM job_requests WHERE request_id = $1 AND result IS NULL \
                 FOR NO KEY UPDATE",
                &[&request_id],
            )
            .await
            .map_err(db)?
            .is_some();
        let renewed = if in_flight {
            tx.execute(LEASE_UPSERT, &[&request_id, &expires, &now_us()])
                .await
                .map_err(db)?
        } else {
            0
        };
        tx.commit().await.map_err(db)?;
        Ok(renewed > 0)
    }

    /// Settle a claimed request once (first result wins) with the full
    /// bookkeeping: drop its deferred token-mint request, its undelivered
    /// cancellations and the claiming session's queued cancel messages, then
    /// stamp `result`/`finished_at` and move the lease expiry to
    /// `locked_until`. Returns `(run_id, job_id, agent_job_id)` whenever the
    /// request exists (also when it was already settled).
    ///
    /// Statements (one transaction): `SELECT run_id, job_id, agent_job_id,
    /// session_id, result IS NULL FROM job_requests WHERE request_id FOR NO
    /// KEY UPDATE`; `DELETE FROM github_token_requests`; `DELETE FROM
    /// job_cancellations WHERE delivered_at IS NULL`; `DELETE FROM
    /// session_messages WHERE session_id AND message_type = JobCancellation`;
    /// `UPDATE job_requests SET result, finished_at WHERE result IS NULL`;
    /// `UPDATE job_leases SET expires_at`.
    pub(super) async fn settle_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        let expires = codec::locked_until_us(locked_until)?;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let Some(row) = tx
            .query_opt(
                "SELECT run_id::text, job_id, agent_job_id::text, session_id::text, \
                 result IS NULL FROM job_requests WHERE request_id = $1 FOR NO KEY UPDATE",
                &[&request_id],
            )
            .await
            .map_err(db)?
        else {
            return Ok(None);
        };
        let attempt = (
            codec::run_id(row.get(0))?,
            codec::job_id(row.get(1)),
            codec::uuid(row.get(2))?,
        );
        let session: Option<String> = row.get(3);
        let in_flight: bool = row.get(4);
        if in_flight {
            tx.execute(
                "DELETE FROM github_token_requests WHERE request_id = $1",
                &[&request_id],
            )
            .await
            .map_err(db)?;
            tx.execute(
                "DELETE FROM job_cancellations WHERE request_id = $1 AND delivered_at IS NULL",
                &[&request_id],
            )
            .await
            .map_err(db)?;
            if let Some(session) = &session {
                tx.execute(
                    "DELETE FROM session_messages WHERE session_id = $1::text::uuid \
                     AND message_type = $2",
                    &[
                        session,
                        &preloop_gha_protocol::azdo::message_type::JOB_CANCELLED,
                    ],
                )
                .await
                .map_err(db)?;
            }
            tx.execute(
                concat!(
                    "UPDATE job_requests SET result = $2, finished_at = ",
                    ts!("$3"),
                    " WHERE request_id = $1 AND result IS NULL"
                ),
                &[&request_id, &status_str(result), &now_us()],
            )
            .await
            .map_err(db)?;
            if let Some(expires) = expires {
                tx.execute(
                    concat!(
                        "UPDATE job_leases SET expires_at = ",
                        ts!("$2"),
                        " WHERE request_id = $1"
                    ),
                    &[&request_id, &expires],
                )
                .await
                .map_err(db)?;
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(Some(attempt))
    }

    /// Settle an AgentRequest iff it is still in flight (duplicate PATCH =>
    /// `None`): stamp `result`/`finished_at`, move the lease expiry to
    /// `locked_until`.
    ///
    /// Statements (one transaction): `UPDATE job_requests SET result,
    /// finished_at WHERE request_id AND result IS NULL RETURNING run_id,
    /// job_id, agent_job_id`; `UPDATE job_leases SET expires_at`.
    pub(super) async fn settle_agent_request(
        &self,
        request_id: i64,
        result: ExecutionStatus,
        locked_until: &str,
    ) -> Result<Option<(RunId, JobId, uuid::Uuid)>, ControlError> {
        let expires = codec::locked_until_us(locked_until)?;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let Some(row) = tx
            .query_opt(
                concat!(
                    "UPDATE job_requests SET result = $2, finished_at = ",
                    ts!("$3"),
                    " WHERE request_id = $1 AND result IS NULL \
                     RETURNING run_id::text, job_id, agent_job_id::text"
                ),
                &[&request_id, &status_str(result), &now_us()],
            )
            .await
            .map_err(db)?
        else {
            return Ok(None);
        };
        if let Some(expires) = expires {
            tx.execute(
                concat!(
                    "UPDATE job_leases SET expires_at = ",
                    ts!("$2"),
                    " WHERE request_id = $1"
                ),
                &[&request_id, &expires],
            )
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(Some((
            codec::run_id(row.get(0))?,
            codec::job_id(row.get(1)),
            codec::uuid(row.get(2))?,
        )))
    }

    /// Unfinished requests whose claim lost its holder: owned by a runner or
    /// bound to a session, but no live session of a registered runner still
    /// claims them. Boot reconcile input.
    ///
    /// Statement: `SELECT q.request_id, q.run_id, q.job_id FROM job_requests
    /// q WHERE q.result IS NULL AND (q.runner_id IS NOT NULL OR q.session_id
    /// IS NOT NULL) AND NOT EXISTS (runner_sessions s JOIN runners r ..
    /// WHERE s.session_id = q.session_id)`.
    pub(super) async fn orphaned_claims(&self) -> Result<Vec<(i64, RunId, JobId)>, ControlError> {
        let client = self.reader().await?;
        client
            .query(
                "SELECT q.request_id, q.run_id::text, q.job_id FROM job_requests q \
                 WHERE q.result IS NULL \
                   AND (q.runner_id IS NOT NULL OR q.session_id IS NOT NULL) \
                   AND NOT EXISTS (SELECT 1 FROM runner_sessions s \
                                   JOIN runners r ON r.runner_id = s.runner_id \
                                   WHERE s.session_id = q.session_id) \
                 ORDER BY q.request_id",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| {
                Ok((
                    row.get::<_, i64>(0),
                    codec::run_id(row.get(1))?,
                    codec::job_id(row.get(2)),
                ))
            })
            .collect()
    }

    /// One reaper tick's inputs, read without locks: in-flight attempts
    /// (with the job's `timeout-minutes` from its message template), the
    /// ready queue in claim order, every runner's labels, whether any
    /// binding exists, stale-session runners and session-less registrations
    /// older than the liveness timeout.
    ///
    /// Statements: five independent `SELECT`s (see body).
    pub(super) async fn reap_inputs(&self) -> Result<ReapInputs, ControlError> {
        let (_, _, liveness) = self.config();
        let cutoff = now_us() - liveness.as_micros() as i64;
        let client = self.reader().await?;
        let mut inputs = ReapInputs::default();
        for row in client
            .query(
                concat!(
                    "SELECT q.request_id, q.run_id::text, q.job_id, ",
                    us!("q.started_at"),
                    ", ",
                    us!("l.renewed_at"),
                    ", q.timeout_triggered, (m.message_template->>'jobTimeout')::int8 \
                     FROM job_requests q \
                     LEFT JOIN job_leases l ON l.request_id = q.request_id \
                     LEFT JOIN job_messages m ON m.run_id = q.run_id AND m.job_id = q.job_id \
                     WHERE q.result IS NULL ORDER BY q.request_id"
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            inputs.active.push(ActiveRequest {
                request_id: row.get(0),
                run_id: codec::run_id(row.get(1))?,
                job_id: codec::job_id(row.get(2)),
                started_at: row.get::<_, Option<i64>>(3).map(codec::us_to_system),
                last_renewed_at: row.get::<_, Option<i64>>(4).map(codec::us_to_system),
                timeout_triggered: row.get(5),
                job_timeout_s: row.get(6),
            });
        }
        for row in client
            .query(
                concat!(
                    "SELECT run_id::text, job_id, runs_on::text, ",
                    us!("enqueued_at"),
                    " FROM jobs WHERE queue_state = 'ready' \
                     ORDER BY priority DESC, run_order, job_order, run_id, job_id"
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            inputs.ready.push(ReadyRow {
                run_id: codec::run_id(row.get(0))?,
                job_id: codec::job_id(row.get(1)),
                runs_on: codec::from_json(row.get(2))?,
                enqueued_at_unix_nanos: row.get::<_, Option<i64>>(3).unwrap_or(0) * 1000,
            });
        }
        for row in client
            .query("SELECT labels::text FROM runners ORDER BY runner_id", &[])
            .await
            .map_err(db)?
        {
            inputs.runner_labels.push(codec::from_json(row.get(0))?);
        }
        inputs.has_bindings = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM job_assignments) \
                 OR EXISTS (SELECT 1 FROM provision_requests)",
                &[],
            )
            .await
            .map_err(db)?
            .get(0);
        for row in client
            .query(
                concat!(
                    "SELECT DISTINCT runner_id FROM runner_sessions \
                     WHERE runner_id IS NOT NULL AND last_seen_at < ",
                    ts!("$1")
                ),
                &[&cutoff],
            )
            .await
            .map_err(db)?
        {
            inputs.stale_runners.insert(row.get(0));
        }
        for row in client
            .query(
                concat!(
                    "SELECT r.runner_id FROM runners r WHERE r.registered_at < ",
                    ts!("$1"),
                    " AND NOT EXISTS (SELECT 1 FROM runner_sessions s \
                     WHERE s.runner_id = r.runner_id)"
                ),
                &[&cutoff],
            )
            .await
            .map_err(db)?
        {
            inputs.phantom_runners.insert(row.get(0));
        }
        Ok(inputs)
    }

    /// Drop or release stale job → runner bindings. Pool assignments on:
    /// delete assignments and provision requests of jobs no longer ready.
    /// Both flags off: delete assignments / provision requests older than
    /// the assignment TTL. Otherwise: release (not delete) bindings older
    /// than the claim-binding TTL or naming a runner that is gone — the
    /// binding keeps the job, loses the runner, and the job re-enters the
    /// provision queue. Returns rows changed.
    ///
    /// Statements (one transaction): conditional `DELETE`s; or one
    /// `WITH stale AS (UPDATE job_assignments SET runner_id = NULL ..
    /// RETURNING) INSERT INTO provision_requests .. ON CONFLICT DO UPDATE`.
    pub(super) async fn sweep_stale_bindings(&self) -> Result<usize, ControlError> {
        let (pool_on, require_on, _) = self.config();
        let now = now_us();
        let assignment_cutoff = now - crate::control::logic::ASSIGNMENT_TTL.as_micros() as i64;
        let binding_cutoff = now - crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let mut swept = 0u64;
        if pool_on {
            for sql in [
                "DELETE FROM job_assignments a WHERE NOT EXISTS (SELECT 1 FROM jobs j \
                 WHERE j.run_id = a.run_id AND j.job_id = a.job_id AND j.queue_state = 'ready')",
                "DELETE FROM provision_requests p WHERE NOT EXISTS (SELECT 1 FROM jobs j \
                 WHERE j.run_id = p.run_id AND j.job_id = p.job_id AND j.queue_state = 'ready')",
            ] {
                swept += tx.execute(sql, &[]).await.map_err(db)?;
            }
        }
        if !require_on && !pool_on {
            swept += tx
                .execute(
                    concat!(
                        "DELETE FROM job_assignments WHERE assigned_at < ",
                        ts!("$1")
                    ),
                    &[&assignment_cutoff],
                )
                .await
                .map_err(db)?;
            swept += tx
                .execute(
                    concat!(
                        "DELETE FROM provision_requests WHERE requested_at < ",
                        ts!("$1")
                    ),
                    &[&assignment_cutoff],
                )
                .await
                .map_err(db)?;
        } else {
            swept += tx
                .execute(
                    concat!(
                        "WITH stale AS (UPDATE job_assignments a SET runner_id = NULL \
                         WHERE a.runner_id IS NOT NULL AND (a.assigned_at < ",
                        ts!("$1"),
                        " OR NOT EXISTS (SELECT 1 FROM runners r WHERE r.runner_id = a.runner_id)) \
                         RETURNING a.run_id, a.job_id) \
                         INSERT INTO provision_requests (run_id, job_id, namespace_id, pool_key, \
                         labels, requested_at) \
                         SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, j.runs_on, ",
                        ts!("$2"),
                        " FROM stale s JOIN jobs j ON j.run_id = s.run_id AND j.job_id = s.job_id \
                         ON CONFLICT (run_id, job_id) DO UPDATE SET \
                         requested_at = EXCLUDED.requested_at"
                    ),
                    &[&binding_cutoff, &now],
                )
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        self.released_bindings
            .fetch_add(swept, std::sync::atomic::Ordering::Relaxed);
        Ok(swept as usize)
    }
}
