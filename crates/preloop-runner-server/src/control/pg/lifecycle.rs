//! Runner/session lifecycle commands plus the small writes that don't touch
//! scheduling state. Each is one short transaction; reads go to the reader
//! pool (a command that must see its own earlier writes takes a writer).

use super::codec::{self, from_json, json, now_us, ts, us};
use super::{PgBackend, db, lookups};
use crate::control::backend::RegisterRunner;
use crate::control::logic;
use crate::control::types::{
    AcquireContext, ControlError, EnvironmentApproval, EnvironmentApprovalAudit,
    EnvironmentApprovalOutcome, EnvironmentApprovalResult, EnvironmentDecision, OpenRunnerSession,
    PendingEnvironmentApproval, PurgeGuard, RunQueueClaimability, RunnerListing, RunnerRow,
    SessionProtocol, SessionRow,
};
use crate::models::{RunRecord, RunnerCapabilities};
use crate::runtime_scheduling;
use preloop_gha_protocol::azdo;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::BTreeSet;

/// `(repository, permissions, declared, untrusted)` of a stored token
/// request; `acquire_context` needs the full row.
async fn token_request_for(
    client: &tokio_postgres::Client,
    request_id: i64,
) -> Result<Option<crate::models::GitHubTokenRequest>, ControlError> {
    client
        .query_opt(
            "SELECT repository, permissions::text, declared, untrusted \
             FROM github_token_requests WHERE request_id=$1",
            &[&request_id],
        )
        .await
        .map_err(db)?
        .map(|row| {
            Ok(crate::models::GitHubTokenRequest {
                repository: row.get(0),
                permissions: from_json(row.get::<_, String>(1).as_str()).unwrap_or_default(),
                declared: row.get(2),
                untrusted: row.get(3),
            })
        })
        .transpose()
}

impl PgBackend {
    // ── Sessions ─────────────────────────────────────────────────────

    /// `create_session`: mint a session row for `runner_id` (compat
    /// sessions insert with NULL runner). The row stores no key material —
    /// session crypto is caller-derived, never persisted (schema design
    /// rule).
    pub(super) async fn create_session(
        &self,
        session: crate::control::backend::CreateSession,
    ) -> Result<SessionRow, ControlError> {
        let session_id = uuid::Uuid::new_v4().to_string();
        let now = now_us();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let protocol = match session.protocol {
            SessionProtocol::Compat => "azdo", // compat rows persist azdo (no runner)
            SessionProtocol::Broker => "broker",
            SessionProtocol::Azdo => "azdo",
        };
        let runner_id = match session.protocol {
            SessionProtocol::Compat => None,
            _ => Some(session.runner_id),
        };
        if let Some(runner_id) = runner_id {
            let alive = tx
                .query_opt("SELECT 1 FROM runners WHERE runner_id=$1", &[&runner_id])
                .await
                .map_err(db)?
                .is_some();
            if !alive {
                return Err(ControlError::Forbidden(
                    "runner registration no longer exists".to_owned(),
                ));
            }
        }
        tx.execute(
            concat!(
                "INSERT INTO runner_sessions (session_id, runner_id, protocol, \
                 client_id, verified, created_at, last_seen_at) \
                 VALUES ($1::text::uuid,$2,$3,$4,$5,",
                ts!("$6"),
                ",",
                ts!("$6"),
                ")"
            ),
            &[
                &session_id,
                &runner_id,
                &protocol,
                &session.client_id,
                &true,
                &now,
            ],
        )
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(SessionRow {
            session_id,
            runner_id: session.runner_id,
            protocol: session.protocol,
            client_id: session.client_id,
            active_request_id: None,
            last_seen_at_us: Some(now),
        })
    }

    /// `open_runner_session`: insert a caller-minted session. Verified
    /// sessions are exclusive per runner (Conflict on a second live one);
    /// `runner_id: None` writes nothing (compat); `require_live_runner`
    /// rejects an unregistered runner in the insert's transaction.
    pub(super) async fn open_runner_session(
        &self,
        open: OpenRunnerSession,
    ) -> Result<(), ControlError> {
        let Some(runner_id) = open.runner_id else {
            return Ok(());
        };
        let session_uuid = logic::session_uuid(&open.session_id);
        let protocol = match open.protocol {
            SessionProtocol::Broker => "broker",
            _ => "azdo",
        };
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        // The liveness check and the insert share this transaction: the
        // liveness sweep may have purged the runner after token validation
        // but before the insert (the broker route maps `Forbidden` to 401).
        if open.require_live_runner {
            let alive = tx
                .query_opt("SELECT 1 FROM runners WHERE runner_id=$1", &[&runner_id])
                .await
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
                .query_opt(
                    "SELECT 1 FROM runner_sessions WHERE runner_id=$1 AND verified \
                     AND session_id <> $2::text::uuid",
                    &[&runner_id, &session_uuid.to_string()],
                )
                .await
                .map_err(db)?
                .is_some();
            if conflict {
                return Err(ControlError::Conflict(format!(
                    "runner {runner_id} already owns a verified session"
                )));
            }
        }
        tx.execute(
            "INSERT INTO runner_sessions (session_id, runner_id, protocol, verified) \
             VALUES ($1::text::uuid,$2,$3,$4) \
             ON CONFLICT (session_id) DO UPDATE SET runner_id=$2, protocol=$3, \
             verified=$4, last_seen_at=now()",
            &[
                &session_uuid.to_string(),
                &runner_id,
                &protocol,
                &open.verified,
            ],
        )
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// `close_runner_session`: delete the row when the caller owns it (or
    /// presents no runner). `false` = no row; `Forbidden` = other runner's.
    pub(super) async fn close_runner_session(
        &self,
        session_id: &str,
        caller_runner_id: Option<i64>,
    ) -> Result<bool, ControlError> {
        let session_uuid = logic::session_uuid(session_id);
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let owner = tx
            .query_opt(
                "SELECT runner_id FROM runner_sessions WHERE session_id=$1::text::uuid",
                &[&session_uuid.to_string()],
            )
            .await
            .map_err(db)?;
        let Some(row) = owner else { return Ok(false) };
        let owner: Option<i64> = row.get(0);
        if let (Some(owner), Some(caller)) = (owner, caller_runner_id)
            && owner != caller
        {
            return Err(ControlError::Forbidden(
                "session belongs to another runner".to_owned(),
            ));
        }
        // Release the session's live request so the job can be retried. The
        // recorded owner survives (see below), so only that runner may still
        // complete/renew the attempt.
        let requests = tx
            .query(
                "SELECT request_id FROM job_requests WHERE session_id=$1::text::uuid \
                 AND result IS NULL",
                &[&session_uuid.to_string()],
            )
            .await
            .map_err(db)?;
        tx.execute(
            "DELETE FROM runner_sessions WHERE session_id=$1::text::uuid",
            &[&session_uuid.to_string()],
        )
        .await
        .map_err(db)?;
        for row in requests {
            let request_id: i64 = row.get(0);
            // Only the session binding goes: runner_id stays, so the
            // attempt's owner survives session teardown and only that runner
            // may still complete/renew it (legacy AgentRequest semantics —
            // verified by legacy_agent_requests_are_bound_to_runner_identity).
            // Its start stamp and lease stay too: the trait doc leaves the
            // request and its claimed job to the lease reaper, which reads
            // both of them.
            tx.execute(
                "UPDATE job_requests SET session_id = NULL \
                 WHERE request_id = $1 AND result IS NULL",
                &[&request_id],
            )
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    /// `delete_session`: same release + delete, unconditional.
    pub(super) async fn delete_session(&self, session_id: &str) -> Result<(), ControlError> {
        self.close_runner_session(session_id, None).await?;
        Ok(())
    }

    /// `touch_session`: stamp liveness. Returns the protocol for the
    /// caller's poll routing.
    pub(super) async fn touch_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionProtocol>, ControlError> {
        let session_uuid = logic::session_uuid(session_id);
        let client = self.writer().await?;
        let row = client
            .query_opt(
                "UPDATE runner_sessions SET last_seen_at=now() \
                 WHERE session_id=$1::text::uuid RETURNING protocol",
                &[&session_uuid.to_string()],
            )
            .await
            .map_err(db)?;
        Ok(row.map(|row| SessionProtocol::parse(row.get(0))))
    }

    /// `session_owner`: the session's runner plus its capabilities.
    pub(super) async fn session_owner(
        &self,
        session_id: &str,
    ) -> Result<Option<(i64, RunnerCapabilities)>, ControlError> {
        let session_uuid = logic::session_uuid(session_id);
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT s.runner_id, r.labels::text, r.runner_group_id, \
                 r.runner_group_name \
                 FROM runner_sessions s LEFT JOIN runners r \
                 ON r.runner_id = s.runner_id WHERE s.session_id=$1::text::uuid",
                &[&session_uuid.to_string()],
            )
            .await
            .map_err(db)?;
        Ok(row.and_then(|row| {
            row.get::<_, Option<i64>>(0).map(|runner_id| {
                (
                    runner_id,
                    RunnerCapabilities {
                        known: row.get::<_, Option<String>>(1).is_some(),
                        labels: row
                            .get::<_, Option<String>>(1)
                            .map(|t| from_json::<Vec<String>>(t.as_str()).unwrap_or_default())
                            .unwrap_or_default(),
                        runner_group_id: row.get(2),
                        runner_group_name: row.get(3),
                    },
                )
            })
        }))
    }

    /// `delete_inflight`: acknowledgement of one session message.
    pub(super) async fn delete_inflight(
        &self,
        session_id: &str,
        message_id: i64,
    ) -> Result<(), ControlError> {
        let session_uuid = logic::session_uuid(session_id);
        let client = self.writer().await?;
        client
            .execute(
                "DELETE FROM session_messages WHERE session_id=$1::text::uuid \
                 AND message_id=$2",
                &[&session_uuid.to_string(), &message_id],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    // ── Runners ──────────────────────────────────────────────────────

    /// `register_runner`: dedup on `client_id`, else insert
    /// ([`PgBackend::register_runner_row`] owns the statement), then pair a
    /// pool-proven registration with a pending job.
    pub(super) async fn register_runner(
        &self,
        reg: RegisterRunner,
    ) -> Result<RunnerRow, ControlError> {
        let pool_proven = reg.pool_proven;
        let (row, inserted) = self.register_runner_row(reg).await?;
        if inserted && pool_proven {
            self.pair_runner(row.runner.id).await?;
        }
        Ok(row)
    }

    /// One runner row decoded for `RunnerRow`.
    async fn runner_row_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        runner_id: i64,
    ) -> Result<Option<RunnerRow>, ControlError> {
        let row = tx
            .query_opt(
                concat!(
                    "SELECT runner_id, name, labels::text, ephemeral, public_key, \
                     rsa_public_key, client_id, runner_group_id, \
                     runner_group_name, pool_proven, ",
                    us!("registered_at"),
                    " FROM runners WHERE runner_id=$1"
                ),
                &[&runner_id],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let runner = preloop_gha_protocol::RegisteredRunner {
            id: row.get(0),
            name: row.get(1),
            labels: from_json(row.get::<_, String>(2).as_str())?,
            ephemeral: row.get(3),
            public_key: row.get(4),
            runner_group_id: row.get(7),
            runner_group_name: row.get(8),
        };
        Ok(Some(RunnerRow {
            runner,
            rsa_public_key: row
                .get::<_, Option<&[u8]>>(5)
                .and_then(|xml| std::str::from_utf8(xml).ok())
                .and_then(|xml| preloop_gha_protocol::crypto::AgentRsaPublicKey::parse(xml).ok()),
            client_id: row.get(6),
            pool_proven: row.get(9),
            registered_at_us: row.get(10),
        }))
    }

    /// `pair_runner` per the trait doc: mark the runner pool-proven, release
    /// stale/dead bindings back to the pool waitlist, then bind the oldest
    /// pool-pending ready job this runner can serve.
    async fn pair_runner_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        runner_id: i64,
    ) -> Result<(), ControlError> {
        let (pool_assignments, require_assignments, _l) = self.config();
        if !pool_assignments && !require_assignments {
            return Ok(());
        }
        // "mark the runner pool-proven" comes first: enqueue-time binding
        // gates on this flag (pg/dispatch.rs `on_job_enqueued`), and
        // registration always writes `pool_proven = false`.
        tx.execute(
            "UPDATE runners SET pool_proven = true WHERE runner_id = $1",
            &[&runner_id],
        )
        .await
        .map_err(db)?;
        let row = tx
            .query_opt(
                "SELECT labels::text, runner_group_id, runner_group_name \
                 FROM runners WHERE runner_id=$1",
                &[&runner_id],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else { return Ok(()) };
        let caps = RunnerCapabilities {
            known: true,
            labels: from_json(row.get::<_, String>(0).as_str())?,
            runner_group_id: row.get(1),
            runner_group_name: row.get(2),
        };
        // Bindings on this runner, stale bindings, and bindings whose runner
        // is gone lose their runner; each released job rejoins the pool
        // waitlist at now (trait doc: "re-marked pool-pending at `now`").
        let now = now_us();
        let stale_cutoff = now - crate::control::logic::CLAIM_BINDING_TTL.as_micros() as i64;
        let released = tx
            .query(
                concat!(
                    "SELECT a.run_id::text, a.job_id FROM job_assignments a \
                     WHERE a.runner_id IS NOT NULL AND (a.runner_id = $1 \
                       OR ",
                    us!("a.assigned_at"),
                    " < $2 \
                       OR NOT EXISTS (SELECT 1 FROM runners r WHERE r.runner_id = a.runner_id))"
                ),
                &[&runner_id, &stale_cutoff],
            )
            .await
            .map_err(db)?;
        let mut released_count = 0usize;
        for row in &released {
            let run_id = row.get::<_, String>(0);
            let job_id = JobId(row.get::<_, String>(1));
            tx.execute(
                "UPDATE job_assignments SET runner_id = NULL \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id, &job_id.0],
            )
            .await
            .map_err(db)?;
            if pool_assignments {
                tx.execute(
                    concat!(
                        "INSERT INTO provision_requests (run_id, job_id, namespace_id, \
                         pool_key, labels, requested_at) \
                         SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, j.runs_on, ",
                        ts!("$3"),
                        " FROM jobs j WHERE j.run_id = $1::text::uuid AND j.job_id = $2 \
                         ON CONFLICT (run_id, job_id) DO UPDATE SET \
                         requested_at = EXCLUDED.requested_at"
                    ),
                    &[&run_id, &job_id.0, &now],
                )
                .await
                .map_err(db)?;
                released_count += 1;
            }
        }
        self.released_bindings
            .fetch_add(released_count as u64, std::sync::atomic::Ordering::Relaxed);
        // Oldest pool-pending job this runner can serve (pending-mark order,
        // ties by queue position). A job its namespace would not let start
        // (state or running caps) is not paired: binding it would park this
        // warm runner on work that cannot run.
        let pending = tx
            .query(
                &format!(
                    "SELECT p.run_id::text, p.job_id, j.runs_on::text, j.runner_group \
                     FROM provision_requests p JOIN jobs j \
                     ON j.run_id=p.run_id AND j.job_id=p.job_id \
                     WHERE j.queue_state='ready' AND ({}) \
                     ORDER BY p.requested_at, j.priority DESC, j.run_order, j.job_order LIMIT 64",
                    crate::control::types::NAMESPACE_ADMITS_CLAIM
                ),
                &[],
            )
            .await
            .map_err(db)?;
        for row in pending {
            let runs_on: Vec<String> = from_json(row.get::<_, String>(2).as_str())?;
            let runner_group: Option<String> = row.get(3);
            if runtime_scheduling::job_matches_runner(&runs_on, &caps.labels)
                && runtime_scheduling::job_matches_runner_group(runner_group.as_deref(), &caps)
            {
                let run_id = codec::run_id(&row.get::<_, String>(0))?;
                let job_id = JobId(row.get::<_, String>(1));
                tx.execute(
                    "DELETE FROM provision_requests WHERE run_id=$1::text::uuid AND job_id=$2",
                    &[&run_id.0.to_string(), &job_id.0],
                )
                .await
                .map_err(db)?;
                // Upsert: `sweep_stale_bindings` releases a binding by
                // clearing `runner_id` and keeps the row, and the trait doc
                // rebinds any existing assignment (`first_at` kept).
                tx.execute(
                    concat!(
                        "INSERT INTO job_assignments (run_id, job_id, runner_id, \
                         assigned_at, first_assigned_at) \
                         VALUES ($1::text::uuid, $2, $3, ",
                        ts!("$4"),
                        ", ",
                        ts!("$4"),
                        ") \
                         ON CONFLICT (run_id, job_id) DO UPDATE SET \
                         runner_id = EXCLUDED.runner_id, assigned_at = EXCLUDED.assigned_at"
                    ),
                    &[&run_id.0.to_string(), &job_id.0, &runner_id, &now],
                )
                .await
                .map_err(db)?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// `pair_runner`: pool-proven registration retry — pair this runner.
    pub(super) async fn pair_runner(&self, runner_id: i64) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        self.pair_runner_tx(&tx, runner_id).await?;
        tx.commit().await.map_err(db)
    }

    /// `update_runner`: in-place name/labels update.
    pub(super) async fn update_runner(
        &self,
        runner_id: i64,
        name: Option<String>,
        labels: Option<Vec<String>>,
    ) -> Result<RunnerRow, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if let Some(name) = name {
            tx.execute(
                "UPDATE runners SET name=$2 WHERE runner_id=$1",
                &[&runner_id, &name],
            )
            .await
            .map_err(db)?;
        }
        if let Some(labels) = labels {
            tx.execute(
                "UPDATE runners SET labels=$2::text::jsonb WHERE runner_id=$1",
                &[&runner_id, &json(&labels)?],
            )
            .await
            .map_err(db)?;
        }
        let out = self.runner_row_tx(&tx, runner_id).await?;
        tx.commit().await.map_err(db)?;
        out.ok_or_else(|| ControlError::NotFound(format!("runner {runner_id}")))
    }

    /// `purge_runner` internals shared by the guards: capture owned claimed
    /// jobs, delete the runner (sessions cascade; requests' sessions clear),
    /// then requeue each job. Returns the retired attempts' identities.
    async fn purge_runner_tx(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        runner_id: i64,
    ) -> Result<Vec<uuid::Uuid>, ControlError> {
        // Claimed jobs this runner owns: request owner, session owner, or
        // assignment binding.
        let requeue = tx
            .query(
                "SELECT DISTINCT j.run_id::text, j.job_id FROM jobs j \
                 WHERE j.queue_state='claimed' AND ( \
                 j.claimed_by_runner_id=$1 \
                 OR EXISTS (SELECT 1 FROM job_assignments a \
                  WHERE a.run_id=j.run_id AND a.job_id=j.job_id AND a.runner_id=$1) \
                 OR EXISTS (SELECT 1 FROM job_requests q JOIN runner_sessions s \
                  ON s.session_id=q.session_id WHERE q.run_id=j.run_id \
                  AND q.job_id=j.job_id AND q.result IS NULL AND s.runner_id=$1))",
                &[&runner_id],
            )
            .await
            .map_err(db)?;
        // Ready-but-assigned jobs of the dead runner return to pool-pending
        // so provisioning re-runs for them. Collected BEFORE the runner row
        // goes away — `job_assignments.runner_id` is ON DELETE SET NULL, so
        // the delete below would hide them.
        let unclaimed = tx
            .query(
                "SELECT j.run_id::text, j.job_id FROM jobs j \
                 WHERE j.queue_state='ready' AND EXISTS ( \
                   SELECT 1 FROM job_assignments a WHERE a.run_id=j.run_id \
                   AND a.job_id=j.job_id AND a.runner_id=$1)",
                &[&runner_id],
            )
            .await
            .map_err(db)?;
        tx.execute("DELETE FROM runners WHERE runner_id=$1", &[&runner_id])
            .await
            .map_err(db)?;
        // Unbind requests whose session died (FK SET NULL via session delete)
        // and clear their lease rows so a fresh claim starts clean.
        tx.execute("DELETE FROM job_leases WHERE runner_id=$1", &[&runner_id])
            .await
            .map_err(db)?;
        let mut retired = Vec::new();
        for row in requeue {
            let run_id = codec::run_id(row.get(0))?;
            let job_id = JobId(row.get(1));
            super::dispatch::requeue_claimed_tx(tx, run_id, &job_id, &mut retired).await?;
        }
        let now = now_us();
        for row in unclaimed {
            let run_id = row.get::<_, String>(0);
            let job_id = JobId(row.get::<_, String>(1));
            tx.execute(
                "DELETE FROM job_assignments WHERE run_id=$1::text::uuid AND job_id=$2",
                &[&run_id, &job_id.0],
            )
            .await
            .map_err(db)?;
            tx.execute(
                concat!(
                    "INSERT INTO provision_requests (run_id, job_id, namespace_id, \
                     pool_key, labels, requested_at) \
                     SELECT j.run_id, j.job_id, j.namespace_id, j.pool_key, j.runs_on, ",
                    ts!("$3"),
                    " FROM jobs j WHERE j.run_id = $1::text::uuid AND j.job_id = $2 \
                     ON CONFLICT (run_id, job_id) DO UPDATE SET \
                     requested_at = EXCLUDED.requested_at"
                ),
                &[&run_id, &job_id.0, &now],
            )
            .await
            .map_err(db)?;
        }
        Ok(retired)
    }

    /// `purge_runner` (engine credential): unconditional; returns the
    /// retired attempts' identities.
    pub(super) async fn purge_runner(
        &self,
        runner_id: i64,
    ) -> Result<Vec<uuid::Uuid>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let retired = self.purge_runner_tx(&tx, runner_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(retired)
    }

    /// `purge_runner_guarded`: `None` when the guard refused.
    pub(super) async fn purge_runner_guarded(
        &self,
        runner_id: i64,
        guard: PurgeGuard,
    ) -> Result<Option<Vec<uuid::Uuid>>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let exists = tx
            .query_opt("SELECT 1 FROM runners WHERE runner_id=$1", &[&runner_id])
            .await
            .map_err(db)?
            .is_some();
        let owns_session = || async {
            Ok::<bool, ControlError>(
                tx.query_opt(
                    "SELECT 1 FROM runner_sessions WHERE runner_id=$1",
                    &[&runner_id],
                )
                .await
                .map_err(db)?
                .is_some(),
            )
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
            PurgeGuard::RegistrationToken => exists && !owns_session().await?,
            PurgeGuard::IfPhantom => exists && !owns_session().await?,
        };
        if !allowed {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        }
        let retired = self.purge_runner_tx(&tx, runner_id).await?;
        tx.commit().await.map_err(db)?;
        Ok(Some(retired))
    }

    pub(super) async fn ephemeral_runner_ids(&self) -> Result<Vec<i64>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query(
                "SELECT runner_id FROM runners WHERE ephemeral ORDER BY runner_id",
                &[],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    pub(super) async fn runner_ids_named(&self, name: &str) -> Result<Vec<i64>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query(
                "SELECT runner_id FROM runners WHERE name=$1 ORDER BY runner_id",
                &[&name],
            )
            .await
            .map_err(db)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// `lookup_agent`: the lowest-id runner named `name` plus its client id,
    /// synthesizing and recording `{:08x}-0000-4000-8000-000000000000` when
    /// the runner has none (so a later token request resolves), per the
    /// trait doc.
    pub(super) async fn lookup_agent(
        &self,
        name: &str,
    ) -> Result<Option<(preloop_gha_protocol::RegisteredRunner, String)>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let row = tx
            .query_opt(
                "SELECT runner_id, name, labels::text, ephemeral, public_key, \
                 runner_group_id, runner_group_name, client_id \
                 FROM runners WHERE name=$1 ORDER BY runner_id LIMIT 1 \
                 FOR UPDATE",
                &[&name],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let runner_id: i64 = row.get(0);
        let client_id = match row.get::<_, Option<String>>(7) {
            Some(client_id) => client_id,
            None => {
                let synthesized = format!("{:08x}-0000-4000-8000-000000000000", runner_id as u32);
                tx.execute(
                    "UPDATE runners SET client_id=$2 WHERE runner_id=$1",
                    &[&runner_id, &synthesized],
                )
                .await
                .map_err(db)?;
                synthesized
            }
        };
        let runner = preloop_gha_protocol::RegisteredRunner {
            id: runner_id,
            name: row.get(1),
            labels: from_json::<Vec<String>>(row.get::<_, String>(2).as_str()).unwrap_or_default(),
            ephemeral: row.get(3),
            public_key: row.get(4),
            runner_group_id: row.get(5),
            runner_group_name: row.get(6),
        };
        tx.commit().await.map_err(db)?;
        Ok(Some((runner, client_id)))
    }

    /// `bind_runner_client`: attach a stable client identity; optionally
    /// re-pair with a pending job.
    pub(super) async fn bind_runner_client(
        &self,
        runner_id: i64,
        client_id: &str,
        pair_with_pending_job: bool,
    ) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        tx.execute(
            "UPDATE runners SET client_id=$2 WHERE runner_id=$1",
            &[&runner_id, &client_id],
        )
        .await
        .map_err(db)?;
        if pair_with_pending_job {
            self.pair_runner_tx(&tx, runner_id).await?;
        }
        tx.commit().await.map_err(db)
    }

    /// `runner_rsa_public_key`.
    pub(super) async fn runner_rsa_public_key(
        &self,
        runner_id: i64,
    ) -> Result<Option<preloop_gha_protocol::crypto::AgentRsaPublicKey>, ControlError> {
        let client = self.reader().await?;
        Ok(client
            .query_opt(
                "SELECT rsa_public_key FROM runners WHERE runner_id=$1",
                &[&runner_id],
            )
            .await
            .map_err(db)?
            .and_then(|row| row.get::<_, Option<Vec<u8>>>(0))
            .and_then(|xml| String::from_utf8(xml).ok())
            .and_then(|xml| preloop_gha_protocol::crypto::AgentRsaPublicKey::parse(&xml).ok()))
    }

    /// `list_runners`: every runner, plus a run's claimability when asked.
    pub(super) async fn list_runners(
        &self,
        run_id: Option<RunId>,
    ) -> Result<RunnerListing, ControlError> {
        let client = self.reader().await?;
        let mut runners = Vec::new();
        for row in client
            .query(
                "SELECT runner_id, name, labels::text, ephemeral, public_key, \
                 runner_group_id, runner_group_name FROM runners ORDER BY runner_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            runners.push((
                preloop_gha_protocol::RegisteredRunner {
                    id: row.get(0),
                    name: row.get(1),
                    labels: from_json(row.get::<_, String>(2).as_str())?,
                    ephemeral: row.get(3),
                    public_key: row.get(4),
                    runner_group_id: row.get(5),
                    runner_group_name: row.get(6),
                },
                RunnerCapabilities {
                    known: true,
                    labels: from_json(row.get::<_, String>(2).as_str())?,
                    runner_group_id: row.get(5),
                    runner_group_name: row.get(6),
                },
            ));
        }
        let run_queue = match run_id {
            None => None,
            Some(run_id) => {
                let ready = client
                    .query(
                        "SELECT runs_on::text, runner_group FROM jobs \
                         WHERE run_id=$1::text::uuid AND queue_state='ready'",
                        &[&run_id.0.to_string()],
                    )
                    .await
                    .map_err(db)?;
                let mut claimable = BTreeSet::new();
                for row in &ready {
                    let runs_on: Vec<String> = from_json(row.get::<_, String>(0).as_str())?;
                    let runner_group: Option<String> = row.get(1);
                    for (runner, caps) in &runners {
                        if runtime_scheduling::job_matches_runner(&runs_on, &caps.labels)
                            && runtime_scheduling::job_matches_runner_group(
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
    }

    // ── Broker-session facade (legacy trait surface) ─────────────────

    /// `create_broker_session` folds into `open_runner_session`. The AES key
    /// is caller-derived (`AppState::session_encryption`) — never stored.
    pub(super) async fn create_broker_session(
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

    /// `delete_broker_session` = ownership-checked close.
    pub(super) async fn delete_broker_session(
        &self,
        session_id: &str,
        runner_id: i64,
    ) -> Result<bool, ControlError> {
        self.close_runner_session(session_id, Some(runner_id)).await
    }

    // ── Acquire path (message/template read) ─────────────────────────

    /// `acquire_context`: request record + stored message + token request +
    /// grant flag, all in one read.
    pub(super) async fn acquire_context(
        &self,
        request_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        let client = self.reader().await?;
        let select = format!("{} WHERE q.request_id=$1", lookups::REQUEST_SELECT);
        let row = client
            .query_opt(&select, &[&request_id])
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Err(ControlError::NotFound(format!("request {request_id}")));
        };
        let request = lookups::request_from_row(&row)?;
        let message_row = client
            .query_opt(
                "SELECT message_template::text FROM job_messages \
                 WHERE run_id=$1::text::uuid AND job_id=$2",
                &[&request.run_id.0.to_string(), &request.job_id.0],
            )
            .await
            .map_err(db)?;
        let Some(message_row) = message_row else {
            return Err(ControlError::backend(anyhow::anyhow!(
                "request {request_id} has no message template"
            )));
        };
        let message: azdo::AgentJobRequestMessage =
            serde_json::from_str(message_row.get::<_, String>(0).as_str())
                .map_err(ControlError::backend)?;
        let token_request = token_request_for(&client, request_id).await?;
        let grant = client
            .query_opt(
                "SELECT id_token_granted FROM job_specs \
                 WHERE run_id=$1::text::uuid AND job_id=$2",
                &[&request.run_id.0.to_string(), &request.job_id.0],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0));
        let (repository, trust_tier): (String, Option<String>) = client
            .query_opt(
                "SELECT r.repository, s.submission->>'trust_tier' \
                 FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
                 WHERE r.run_id=$1::text::uuid",
                &[&request.run_id.0.to_string()],
            )
            .await
            .map_err(db)?
            .map(|row| (row.get(0), row.get(1)))
            .unwrap_or_default();
        Ok(AcquireContext {
            request,
            message,
            token_request,
            id_token_granted: grant,
            repository,
            trust_tier,
        })
    }

    /// `acquire_for_runner` = `acquire_context` + ownership/liveness check.
    pub(super) async fn acquire_for_runner(
        &self,
        request_id: i64,
        runner_id: i64,
    ) -> Result<AcquireContext, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT q.runner_id, q.result, s.runner_id AS session_runner, \
                 s.session_id IS NOT NULL AS has_session \
                 FROM job_requests q LEFT JOIN runner_sessions s \
                 ON s.session_id = q.session_id WHERE q.request_id=$1",
                &[&request_id],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Err(ControlError::NotFound(format!("request {request_id}")));
        };
        let owner_runner_id: Option<i64> = row.get(0);
        let result: Option<String> = row.get(1);
        let session_runner: Option<i64> = row.get(2);
        crate::control::types::ensure_request_owner(
            owner_runner_id,
            session_runner,
            row.get(3),
            runner_id,
        )?;
        if result.is_some() {
            return Err(ControlError::Conflict(format!(
                "request {request_id} already settled"
            )));
        }
        // Release this connection before `acquire_context` checks out its
        // own: holding one reader while waiting for another deadlocks the
        // pool once every reader is held by a concurrent acquire.
        drop(row);
        drop(client);
        self.acquire_context(request_id).await
    }

    /// `record_token_request`: upsert the deferred `github_token_requests`
    /// row. The run-row lock serializes the insert against archival —
    /// `archive_finished_runs` selects candidates `FOR UPDATE SKIP LOCKED`,
    /// so a run locked here cannot vanish under the FK.
    pub(super) async fn record_token_request(
        &self,
        run_id: RunId,
        request_id: i64,
        token_request: &crate::models::GitHubTokenRequest,
    ) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        tx.execute(
            "SELECT 1 FROM runs WHERE run_id=$1::text::uuid FOR NO KEY UPDATE",
            &[&run_id.0.to_string()],
        )
        .await
        .map_err(db)?;
        tx.execute(
            "INSERT INTO github_token_requests (request_id, repository, \
             permissions, declared, untrusted) VALUES ($1,$2,$3::text::jsonb,$4,$5) \
             ON CONFLICT (request_id) DO UPDATE SET repository=$2, \
             permissions=$3::text::jsonb, declared=$4, untrusted=$5",
            &[
                &request_id,
                &token_request.repository,
                &json(&token_request.permissions)?,
                &token_request.declared,
                &token_request.untrusted,
            ],
        )
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    // ── Small reads/writes ───────────────────────────────────────────

    /// `issue_debug_token`: the trait doc's gates in one transaction —
    /// `NotFound` when no in-flight request owns the attempt, `Forbidden`
    /// when the run did not opt into pause-on-failure, `Conflict` on a
    /// second issue. Returns `(run_id, plan_id)`; the plan id is the
    /// attempt's `agent_job_id`.
    pub(super) async fn issue_debug_token(
        &self,
        agent_job_id: uuid::Uuid,
    ) -> Result<(RunId, String), ControlError> {
        let agent = agent_job_id.to_string();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let row = tx
            .query_opt(
                "SELECT q.request_id, q.run_id::text, q.debug_token_issued, \
                 coalesce(s.submission->>'preserve_on_failure', 'false') = 'true' \
                 FROM job_requests q \
                 LEFT JOIN run_submissions s ON s.run_id = q.run_id \
                 WHERE q.agent_job_id = $1::text::uuid AND q.result IS NULL \
                 FOR UPDATE OF q",
                &[&agent],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Err(ControlError::NotFound(format!(
                "no active job request for agent job {agent}"
            )));
        };
        let request_id: i64 = row.get(0);
        let run_id = codec::run_id(row.get(1))?;
        let issued: bool = row.get(2);
        if !row.get::<_, bool>(3) {
            return Err(ControlError::Forbidden(
                "this run did not enable pause-on-failure".to_owned(),
            ));
        }
        if issued {
            return Err(ControlError::Conflict(format!(
                "debug-worker token already issued for agent job {agent}"
            )));
        }
        // Conditional flip: two racing issuers cannot both succeed.
        let flipped = tx
            .execute(
                "UPDATE job_requests SET debug_token_issued = true \
                 WHERE request_id = $1 AND debug_token_issued = false",
                &[&request_id],
            )
            .await
            .map_err(db)?;
        if flipped == 0 {
            return Err(ControlError::Conflict(format!(
                "debug-worker token already issued for agent job {agent}"
            )));
        }
        tx.commit().await.map_err(db)?;
        Ok((run_id, agent))
    }

    /// `oidc_grant`: the attempt's run + job's recorded grant/context.
    pub(super) async fn oidc_grant(
        &self,
        plan_id: &str,
        agent_job_id: uuid::Uuid,
    ) -> Result<crate::control::types::OidcGrant, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT q.run_id::text, q.job_id, s.id_token_granted, \
                 s.oidc_environment, s.oidc_job_workflow_ref, s.oidc_job_workflow_sha \
                 FROM job_requests q LEFT JOIN job_specs s \
                 ON s.run_id=q.run_id AND s.job_id=q.job_id \
                 WHERE q.agent_job_id=$1::text::uuid",
                &[&agent_job_id.to_string()],
            )
            .await
            .map_err(db)?;
        let Some(row) = row else {
            return Err(ControlError::NotFound(format!("plan {plan_id}")));
        };
        let run_id = codec::run_id(row.get(0))?;
        let job_id = JobId(row.get(1));
        if row.get::<_, Option<bool>>(2).is_none() {
            return Err(ControlError::backend(anyhow::anyhow!(
                "job {job_id} has no OIDC context"
            )));
        }
        // Never hold one pooled connection while checking out another.
        drop(client);
        let record = self.run_record(run_id).await?;
        Ok(crate::control::types::OidcGrant {
            run: record,
            job_id,
            granted: row.get::<_, Option<bool>>(2).unwrap_or(false),
            context: crate::state::OidcJobContext {
                environment: row.get(3),
                job_workflow_ref: row.get(4),
                job_workflow_sha: row.get(5),
            },
        })
    }

    // ── Events / archive / reconcile ─────────────────────────────────

    /// `append_event`: one outbox row, in its own transaction after the
    /// producing command committed. A status event is stamped with the
    /// version of the row it reports on, or dropped when that row has
    /// already settled on a different final state (`append_event_tx`).
    pub(super) async fn append_event(
        &self,
        event: &preloop_gha_protocol::NdjsonEvent,
    ) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        super::dispatch::append_event_tx(&tx, event).await?;
        tx.commit().await.map_err(db)
    }

    /// `archive_finished_runs`: copy `completed` runs older than 60s into the
    /// history tables, then delete the run row (cascades the live tree).
    ///
    /// A run whose push-back is still pending — or was touched within the
    /// last 3 days — is skipped: `preloop push` and the webhook watchdog can
    /// still act on it, and the echo of our own push must find the row.
    ///
    /// Statements (one transaction): `SELECT.. FROM runs WHERE status =
    /// 'completed' AND completed_at <= now() - 60s AND NOT EXISTS (pending
    /// push-back state) ORDER BY completed_at LIMIT $1 FOR UPDATE SKIP
    /// LOCKED`; four `INSERT INTO *_history.. SELECT`; `DELETE FROM runs`.
    pub(super) async fn archive_finished_runs(
        &self,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let limit = codec::limit(limit);
        let runs = tx
            .query(
                "SELECT run_id::text FROM runs r WHERE r.status = 'completed' \
                 AND r.completed_at IS NOT NULL \
                 AND r.completed_at <= now() - interval '60 seconds' \
                 AND NOT EXISTS (SELECT 1 FROM run_push_states p \
                                 WHERE p.run_id = r.run_id \
                                   AND (p.status = 'pending' \
                                        OR p.updated_at > now() - interval '3 days')) \
                 ORDER BY r.completed_at, r.run_id LIMIT $1 FOR UPDATE OF r SKIP LOCKED",
                &[&limit],
            )
            .await
            .map_err(db)?;
        let mut archived = Vec::new();
        for row in runs {
            let run_id: String = row.get(0);
            tx.execute(
                "INSERT INTO run_history (run_id, namespace_id, repository, \
                 workflow_path, run_number, run_attempt, run_name, event, ref, \
                 ref_type, head_ref, base_ref, head_sha, conclusion, submission, \
                 record_details, \
                 fork_approval_pending, fork_approval_requested_at, \
                 fork_approval_approved_at, fork_approval_note, \
                 reports_check_runs, \
                 created_at, started_at, completed_at) \
                 SELECT r.run_id, r.namespace_id, r.repository, r.workflow_path, \
                 r.run_number, r.run_attempt, r.run_name, r.event, r.ref, \
                 r.ref_type, r.head_ref, r.base_ref, r.head_sha, r.conclusion, \
                 COALESCE(s.submission, '{}'::text::jsonb), \
                 COALESCE(s.record_details, '{}'::text::jsonb), \
                 r.fork_approval_pending, r.fork_approval_requested_at, \
                 r.fork_approval_approved_at, r.fork_approval_note, \
                 r.reports_check_runs, \
                 r.created_at, \
                 r.started_at, r.completed_at \
                 FROM runs r LEFT JOIN run_submissions s ON s.run_id = r.run_id \
                 WHERE r.run_id = $1::text::uuid",
                &[&run_id],
            )
            .await
            .map_err(db)?;
            tx.execute(
                "INSERT INTO job_history (run_id, run_created_at, job_id, \
                 namespace_id, kind, parent_job_id, base_id, display_name, \
                 status, pool_key, outputs, annotations, check_run_id, created_at, \
                 deps_ready_at, started_at, completed_at) \
                 SELECT j.run_id, r.created_at, j.job_id, j.namespace_id, j.kind, \
                 j.parent_job_id, j.base_id, \
                 COALESCE(s.display_name, j.job_id), j.status, j.pool_key, \
                 j.outputs, j.annotations, j.check_run_id, j.created_at, \
                 j.deps_ready_at, j.started_at, j.completed_at \
                 FROM jobs j JOIN runs r ON r.run_id = j.run_id \
                 LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                 WHERE j.run_id = $1::text::uuid",
                &[&run_id],
            )
            .await
            .map_err(db)?;
            tx.execute(
                "INSERT INTO attempt_history (request_id, run_id, run_created_at, \
                 job_id, namespace_id, agent_job_id, timeline_id, runner_id, \
                 result, claimed_at, started_at, finished_at) \
                 SELECT q.request_id, q.run_id, r.created_at, q.job_id, \
                 q.namespace_id, q.agent_job_id, q.timeline_id, q.runner_id, \
                 q.result, q.claimed_at, q.started_at, q.finished_at \
                 FROM job_requests q JOIN runs r ON r.run_id = q.run_id \
                 WHERE q.run_id = $1::text::uuid",
                &[&run_id],
            )
            .await
            .map_err(db)?;
            tx.execute(
                "INSERT INTO step_history (agent_job_id, step_id, run_id, \
                 run_created_at, namespace_id, position, kind, workflow_index, \
                 runner_number, context_name, name, conclusion, started_at, \
                 finished_at) \
                 SELECT s.agent_job_id, s.step_id, q.run_id, r.created_at, \
                 r.namespace_id, s.position, s.kind, s.workflow_index, \
                 s.runner_number, s.context_name, s.name, s.conclusion, \
                 s.started_at, s.finished_at \
                 FROM job_steps s \
                 JOIN job_requests q ON q.agent_job_id = s.agent_job_id \
                 JOIN runs r ON r.run_id = q.run_id \
                 WHERE q.run_id = $1::text::uuid",
                &[&run_id],
            )
            .await
            .map_err(db)?;
            let deleted = tx
                .execute("DELETE FROM runs WHERE run_id = $1::text::uuid", &[&run_id])
                .await
                .map_err(db)?;
            if deleted > 0 {
                archived.push(codec::run_id(&run_id)?);
            }
        }
        tx.commit().await.map_err(db)?;
        Ok(archived)
    }

    /// `expired_terminal_runs`: terminal runs finished (else created) before
    /// `cutoff_us`, oldest first, `limit` at most.
    ///
    /// Archived runs are selected from `run_history`: the archiver moves a
    /// settled run there within a minute of completion, so that table is
    /// where an expired run actually lives — a retention pass that only
    /// looked at `runs` would delete nothing. A live run with an unfinished
    /// or session-bound attempt is skipped (the archiver's own guard): its
    /// callbacks are still in play and deleting it would strand them.
    ///
    /// Statement: one `SELECT .. FROM (runs UNION ALL run_history) GROUP BY
    /// run_id HAVING MIN(finished_at) < $1 ORDER BY .. LIMIT $2` on the
    /// reader pool.
    pub(super) async fn expired_terminal_runs(
        &self,
        cutoff_us: i64,
        limit: usize,
    ) -> Result<Vec<RunId>, ControlError> {
        let client = self.reader().await?;
        let limit = codec::limit(limit);
        let rows = client
            .query(
                concat!(
                    "SELECT run_id::text FROM ( \
                         SELECT r.run_id AS run_id, \
                                COALESCE(r.completed_at, r.created_at) AS finished_at \
                         FROM runs r \
                         WHERE r.status = 'completed' \
                           AND NOT EXISTS (SELECT 1 FROM job_requests q \
                                           WHERE q.run_id = r.run_id \
                                             AND (q.result IS NULL OR q.session_id IS NOT NULL)) \
                         UNION ALL \
                         SELECT h.run_id, COALESCE(h.completed_at, h.created_at) \
                         FROM run_history h) expired \
                     GROUP BY run_id \
                     HAVING MIN(finished_at) < ",
                    ts!("$1"),
                    " \
                     ORDER BY MIN(finished_at), run_id \
                     LIMIT $2"
                ),
                &[&cutoff_us, &limit],
            )
            .await
            .map_err(db)?;
        rows.iter()
            .map(|row| codec::run_id(&row.get::<_, String>(0)))
            .collect()
    }

    /// `delete_expired_run`: drop one terminal run for good — its live rows
    /// (the `runs` row cascades its jobs, requests, attempts, steps, logs
    /// and gates) and its history rows, plus the artifact metadata keyed by
    /// the run.
    ///
    /// A live row that is not `completed` is `Conflict` and nothing is
    /// written: retention must never delete a run that can still schedule,
    /// whatever the caller's filter said. An unknown run is `Ok(())`, so an
    /// interrupted pass can simply be repeated. The run's advisory lock
    /// serializes this with scheduling transactions that also rewrite its
    /// scalars. On-disk artifacts and the node-local key set are the
    /// caller's to remove (the durable rows here answer neither once
    /// deleted).
    pub(super) async fn delete_expired_run(&self, run_id: RunId) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let key = run_id.0.to_string();
        PgBackend::lock_run(&tx, run_id).await?;
        let status: Option<String> = tx
            .query_opt(
                "SELECT status FROM runs WHERE run_id = $1::text::uuid",
                &[&key],
            )
            .await
            .map_err(db)?
            .map(|row| row.get(0));
        if let Some(status) = status.as_deref()
            && status != "completed"
        {
            return Err(ControlError::Conflict(format!(
                "run {run_id} is {status}, not terminal"
            )));
        }
        // Timelines are keyed by id and shared across attempts of different
        // runs (a rerun correlates on the same id), and they carry no FK to
        // `runs`, so the weekly `prune_timelines` is the only other owner —
        // and it can only match while a request row still names them. Drop
        // the ones this run is the last referent of, records first.
        for table in ["timeline_records", "timelines"] {
            tx.execute(
                &format!(
                    "DELETE FROM {table} WHERE timeline_id IN ( \
                         SELECT q.timeline_id FROM job_requests q \
                         WHERE q.run_id = $1::text::uuid \
                           AND NOT EXISTS (SELECT 1 FROM job_requests o \
                                           WHERE o.timeline_id = q.timeline_id \
                                             AND o.run_id <> $1::text::uuid))"
                ),
                &[&key],
            )
            .await
            .map_err(db)?;
        }
        // The archive tables have no FK to `runs`: without this the run would
        // survive in `job_history`/`attempt_history` (and keep surfacing
        // through `terminal_jobs`) after its record is gone. They are
        // partitioned on time, so this scans partitions — acceptable for an
        // hourly housekeeping pass that deletes a handful of runs.
        for table in [
            "step_history",
            "attempt_history",
            "job_history",
            "run_history",
        ] {
            tx.execute(
                &format!("DELETE FROM {table} WHERE run_id = $1::text::uuid"),
                &[&key],
            )
            .await
            .map_err(db)?;
        }
        tx.execute(
            "DELETE FROM artifacts WHERE run_id = $1::text::uuid",
            &[&key],
        )
        .await
        .map_err(db)?;
        tx.execute("DELETE FROM runs WHERE run_id = $1::text::uuid", &[&key])
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }

    /// `reconcile_on_boot`: drop holds of missing/terminal runs, release
    /// claimed jobs whose session is gone, reset `expanding` nodes to
    /// `pending_expansion`.
    pub(super) async fn reconcile_on_boot(
        &self,
    ) -> Result<crate::control::backend::ReconcileOutcome, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        // Holds on dead runs release; their oldest waiters promote.
        let dead_holds = tx
            .query(
                "SELECT namespace_id, repository, group_name, holder_run_id::text \
                 FROM concurrency_holds h WHERE NOT EXISTS ( \
                 SELECT 1 FROM runs r WHERE r.run_id=h.holder_run_id \
                 AND r.status <> 'completed')",
                &[],
            )
            .await
            .map_err(db)?;
        let mut holders_dropped = 0;
        for row in dead_holds {
            let namespace: String = row.get(0);
            let key = (row.get::<_, String>(1), row.get::<_, String>(2));
            super::dispatch::promote_after_release(self, &tx, &namespace, &key).await?;
            holders_dropped += 1;
        }
        // Wait rows of dead runs drop without promotion effects.
        tx.execute(
            "DELETE FROM concurrency_waits w WHERE NOT EXISTS ( \
             SELECT 1 FROM runs r WHERE r.run_id=w.holder_run_id \
             AND r.status <> 'completed')",
            &[],
        )
        .await
        .map_err(db)?;
        // Orphaned claims: sessions gone (restart) → claimed jobs release
        // back to ready; expired-lease rows requeue.
        let orphaned = tx
            .query(
                "SELECT j.run_id::text, j.job_id FROM jobs j \
                 WHERE j.queue_state='claimed' AND NOT EXISTS ( \
                 SELECT 1 FROM job_requests q WHERE q.run_id=j.run_id \
                 AND q.job_id=j.job_id AND q.result IS NULL AND q.session_id IS NOT NULL)",
                &[],
            )
            .await
            .map_err(db)?;
        let mut recovered = 0usize;
        let mut failed = 0usize;
        // A booting node has no live-log followers to close, so the retired
        // identities are not reported.
        let mut retired = Vec::new();
        for row in &orphaned {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            let job_id = JobId(row.get(1));
            if super::dispatch::requeue_claimed_tx(&tx, run_id, &job_id, &mut retired).await? {
                recovered += 1;
            } else {
                failed += 1;
            }
        }
        // Expanding nodes with no live lease reset to pending_expansion:
        // the boot generation fence already invalidated stale applies.
        tx.execute(
            "UPDATE jobs SET queue_state='pending_expansion' \
             WHERE queue_state='expanding'",
            &[],
        )
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(crate::control::backend::ReconcileOutcome {
            recovered,
            failed,
            holders_dropped,
        })
    }

    /// `rebuild_dispatch_intent`: reconcile pool-assignment rows with the
    /// ready queue after a config change (assignments enabled): pair every
    /// unbound ready job, or clear bindings when assignments are off.
    pub(super) async fn rebuild_dispatch_intent(&self) -> Result<(), ControlError> {
        let (pool_assignments, require_assignments, _l) = self.config();
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        if !pool_assignments && !require_assignments {
            tx.execute("DELETE FROM job_assignments", &[])
                .await
                .map_err(db)?;
            tx.execute("DELETE FROM provision_requests", &[])
                .await
                .map_err(db)?;
            tx.commit().await.map_err(db)?;
            return Ok(());
        }
        let ready = tx
            .query(
                "SELECT run_id::text, job_id FROM jobs WHERE queue_state='ready' \
                 ORDER BY priority DESC, run_order, job_order",
                &[],
            )
            .await
            .map_err(db)?;
        for row in ready {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            let job_id = JobId(row.get(1));
            let exists = tx
                .query_opt(
                    "SELECT 1 FROM job_assignments WHERE run_id=$1::text::uuid \
                     AND job_id=$2",
                    &[&run_id.0.to_string(), &job_id.0],
                )
                .await
                .map_err(db)?
                .is_some();
            if !exists {
                // Reuse the enqueue pairing logic against a minimal graph.
                let mut sweep_graph = match PgBackend::load_graph(self, &tx, run_id).await? {
                    Some(graph) => graph,
                    None => continue,
                };
                super::dispatch::on_job_enqueued(self, &tx, &sweep_graph, &job_id).await?;
                let _ = &mut sweep_graph;
            }
        }
        tx.commit().await.map_err(db)
    }
    /// `run_record`: the run's record — live rows when the run still exists,
    /// the history tables when it was archived.
    ///
    /// Statements: the graph load; `SELECT job_id, status FROM jobs UNION ALL
    /// job_history`; on the archived path one `run_history` + one
    /// `job_history` read.
    pub(super) async fn run_record(&self, run_id: RunId) -> Result<RunRecord, ControlError> {
        let run = run_id.0.to_string();
        let mut client = self.reader().await?;
        let tx = client.transaction().await.map_err(db)?;
        let record = match PgBackend::load_graph(self, &tx, run_id).await? {
            Some(graph) => {
                let mut record = graph.record;
                // Statuses of archived attempts of the same run (a rerun
                // archives the attempts, not the run): previous-attempt jobs
                // the rerun dropped stay visible.
                for row in tx
                    .query(
                        "SELECT job_id, status FROM job_history \
                         WHERE run_id = $1::text::uuid",
                        &[&run],
                    )
                    .await
                    .map_err(db)?
                {
                    record
                        .jobs
                        .entry(JobId(row.get::<_, String>(0)))
                        .or_insert_with(|| {
                            crate::control::types::status_parse(row.get::<_, String>(1).as_str())
                        });
                }
                // A run parked behind its workflow-level concurrency gate is
                // stored `queued`; its `holder_kind = 'run'` wait row is what
                // makes it Pending (lite's `run_record` and `list_runs`
                // project it the same way).
                if record.status == ExecutionStatus::Queued {
                    let held: bool = tx
                        .query_one(
                            "SELECT EXISTS (SELECT 1 FROM concurrency_waits \
                             WHERE holder_run_id = $1::text::uuid AND holder_kind = 'run')",
                            &[&run],
                        )
                        .await
                        .map_err(db)?
                        .get(0);
                    if held {
                        record.status = ExecutionStatus::Pending;
                    }
                }
                record
            }
            // Archived: the run row is gone, its history rows are not.
            None => super::lookups::archived_record_tx(&tx, run_id).await?,
        };
        tx.commit().await.map_err(db)?;
        Ok(record)
    }

    /// `record_environment_approval`: append one review decision to a job
    /// parked on its environment's required-reviewer gate, then re-run the
    /// run's promotion pass so a satisfied gate releases the job (or a
    /// rejection fails it closed).
    ///
    /// A lapsed window records nothing: the promotion pass (run next)
    /// re-evaluates the same window and settles the job `Failure` — one
    /// fail-closed decision over the durable rows. A `Reject` records the
    /// rejecting identity and lets the same pass conclude the job.
    pub(crate) async fn record_environment_approval(
        &self,
        approval: EnvironmentApproval,
    ) -> Result<EnvironmentApprovalOutcome, ControlError> {
        let run_id = approval.run_id;
        let job_id = approval.job_id.clone();
        let decision = approval.decision;
        let actor = approval.actor.clone();
        let admin_override = approval.admin_override;
        let note = approval.note.clone();
        let now = crate::models::now_unix_nanos();
        let result = {
            let mut client = self.writer().await?;
            let tx = client.transaction().await.map_err(db)?;
            let run_key = run_id.0.to_string();
            let row = tx
                .query_opt(
                    "SELECT j.status, j.environment_gate::text, r.namespace_id, r.repository \
                     FROM jobs j JOIN runs r ON r.run_id = j.run_id \
                     WHERE j.run_id = $1::text::uuid AND j.job_id = $2",
                    &[&run_key, &job_id.0],
                )
                .await
                .map_err(db)?
                .ok_or_else(|| ControlError::NotFound("job not found".to_owned()))?;
            let status = crate::control::types::status_parse(row.get::<_, String>(0).as_str());
            let namespace_id: String = row.get(2);
            let repository: String = row.get(3);
            let result = if status.is_terminal() {
                EnvironmentApprovalResult::AlreadyTerminal
            } else {
                let gate = row
                    .get::<_, Option<String>>(1)
                    .and_then(|json| from_json::<crate::models::EnvironmentGateState>(&json).ok());
                match gate {
                    Some(mut gate) => match gate.approval_requested_at_unix_nanos {
                        None => EnvironmentApprovalResult::NotAwaiting,
                        Some(requested_at) => {
                            // The required count was stamped when the gate
                            // armed — rules are deliberately not re-resolved
                            // inside this transaction (a rule edit must not
                            // move a waiting gate). Gates armed before the
                            // stamp existed fall back to one: every prior
                            // rule shape required exactly that.
                            let required = gate.approvals_required.unwrap_or(1);
                            if now.saturating_sub(requested_at)
                                > crate::runtime_scheduling::ENVIRONMENT_APPROVAL_WINDOW_NANOS
                            {
                                EnvironmentApprovalResult::Expired
                            } else {
                                let env_name = gate.environment_name.clone().unwrap_or_default();
                                match decision {
                                    EnvironmentDecision::Approve => {
                                        gate.approvals.push(
                                            crate::models::EnvironmentApprovalRecord {
                                                at_unix_nanos: now,
                                                actor: actor.clone(),
                                                admin_override,
                                                note: note.clone(),
                                            },
                                        );
                                        let approvals = gate.approvals.len();
                                        let satisfied = (approvals as u32) >= required;
                                        tx.execute(
                                            "UPDATE jobs SET environment_gate = $3::text::jsonb \
                                                 WHERE run_id = $1::text::uuid AND job_id = $2",
                                            &[&run_key, &job_id.0, &json(&gate)?],
                                        )
                                        .await
                                        .map_err(db)?;
                                        insert_environment_approval_audit(
                                            &tx,
                                            run_id,
                                            &job_id,
                                            &repository,
                                            &namespace_id,
                                            &env_name,
                                            "approved",
                                            actor.as_deref(),
                                            admin_override,
                                            note.as_deref(),
                                            now,
                                        )
                                        .await?;
                                        if admin_override {
                                            tracing::warn!(
                                                run_id = %run_id.0,
                                                job_id = %job_id.0,
                                                environment = env_name,
                                                note = note.as_deref().unwrap_or_default(),
                                                "environment approval recorded via admin \
                                                 override (reviewer list bypassed)"
                                            );
                                        } else {
                                            tracing::info!(
                                                run_id = %run_id.0,
                                                job_id = %job_id.0,
                                                environment = env_name,
                                                actor = actor.as_deref().unwrap_or_default(),
                                                approvals,
                                                required,
                                                note = note.as_deref().unwrap_or_default(),
                                                "environment approval recorded"
                                            );
                                        }
                                        EnvironmentApprovalResult::Recorded {
                                            approvals,
                                            required,
                                            satisfied,
                                        }
                                    }
                                    EnvironmentDecision::Reject => {
                                        gate.rejected_by = actor.clone();
                                        gate.rejected_at_unix_nanos = Some(now);
                                        gate.rejected_note = note.clone();
                                        tx.execute(
                                            "UPDATE jobs SET environment_gate = $3::text::jsonb \
                                                 WHERE run_id = $1::text::uuid AND job_id = $2",
                                            &[&run_key, &job_id.0, &json(&gate)?],
                                        )
                                        .await
                                        .map_err(db)?;
                                        insert_environment_approval_audit(
                                            &tx,
                                            run_id,
                                            &job_id,
                                            &repository,
                                            &namespace_id,
                                            &env_name,
                                            "rejected",
                                            actor.as_deref(),
                                            admin_override,
                                            note.as_deref(),
                                            now,
                                        )
                                        .await?;
                                        tracing::warn!(
                                            run_id = %run_id.0,
                                            job_id = %job_id.0,
                                            environment = env_name,
                                            actor = actor.as_deref().unwrap_or_default(),
                                            admin_override,
                                            note = note.as_deref().unwrap_or_default(),
                                            "environment deployment rejected"
                                        );
                                        EnvironmentApprovalResult::Rejected
                                    }
                                }
                            }
                        }
                    },
                    None => EnvironmentApprovalResult::NotAwaiting,
                }
            };
            tx.commit().await.map_err(db)?;
            result
        };
        // Release or fail closed over the run's durable rows: a satisfied
        // gate unparks the job; a rejection or expired window settles it
        // `Failure` (and its dependents).
        let promote = self.promote_ready_jobs(Some(run_id)).await?;
        Ok(EnvironmentApprovalOutcome {
            result,
            next_runs_on: promote.next_runs_on,
            promoted: promote.promoted,
        })
    }

    /// The jobs still parked on an armed required-reviewer gate — the
    /// announce scan and the check-run webhook lookup read these. `run_id`
    /// narrows to one run (submit path); `None` scans every run (the reaper
    /// sweep).
    pub(crate) async fn pending_environment_approvals(
        &self,
        run_id: Option<RunId>,
    ) -> Result<Vec<PendingEnvironmentApproval>, ControlError> {
        self.pending_environment_approvals_with_announced(run_id, false)
            .await
    }

    async fn pending_environment_approvals_with_announced(
        &self,
        run_id: Option<RunId>,
        include_announced: bool,
    ) -> Result<Vec<PendingEnvironmentApproval>, ControlError> {
        let client = self.reader().await?;
        let run_key: Option<String> = run_id.map(|run_id| run_id.0.to_string());
        let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = match &run_key {
            Some(key) => vec![key],
            None => Vec::new(),
        };
        let where_run = if run_key.is_some() {
            "AND j.run_id = $1::text::uuid"
        } else {
            ""
        };
        let rows = client
            .query(
                &format!(
                    "SELECT j.run_id::text, j.job_id, r.repository, j.environment_gate::text, \
                            j.check_run_id, j.deployment_id, \
                            s.environment::text, m.message_template \
                     FROM jobs j \
                     JOIN runs r ON r.run_id = j.run_id \
                     LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     LEFT JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
                     WHERE j.queue_state = 'held' AND j.status = 'pending' \
                       AND j.environment_gate IS NOT NULL {where_run} \
                     ORDER BY j.run_id, j.job_order"
                ),
                &params,
            )
            .await
            .map_err(db)?;
        let mut out = Vec::new();
        for row in rows {
            let gate: Option<crate::models::EnvironmentGateState> = row
                .get::<_, Option<String>>(3)
                .and_then(|json| from_json(&json).ok());
            let Some(gate) = gate else { continue };
            // Only armed approval gates are reportable: the pending row
            // exists so the announce loop PATCHes the check run once, not
            // every tick.
            if gate.approval_requested_at_unix_nanos.is_none()
                || (!include_announced && gate.approval_announced)
            {
                continue;
            }
            let spec_env: Option<serde_json::Value> = row
                .get::<_, Option<String>>(6)
                .and_then(|json| from_json(&json).ok());
            let template_env: Option<serde_json::Value> = row
                .get::<_, Option<String>>(7)
                .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
                .and_then(|message| {
                    message
                        .get("environment")
                        .or_else(|| message.get("actionsEnvironment"))
                        .cloned()
                });
            let environment_name = gate
                .environment_name
                .clone()
                .or_else(|| {
                    template_env
                        .as_ref()
                        .and_then(|env| env.get("name"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .or_else(|| {
                    crate::runtime_scheduling::environment_gate_name_of(spec_env.as_ref())
                        .map(str::to_owned)
                });
            // Only a statically known literal is reported before the job
            // runs: the runner reports the evaluated `environment.url` with
            // its completion (`jobs.environment_url`), which the deployment
            // read prefers. A raw `${{ }}` template is never posted.
            let environment_url = template_env
                .as_ref()
                .and_then(runtime_scheduling::environment_url_literal)
                .map(str::to_owned)
                .or_else(|| {
                    spec_env
                        .as_ref()
                        .and_then(runtime_scheduling::environment_url_literal)
                        .map(str::to_owned)
                });
            let Some(environment_name) = environment_name else {
                continue;
            };
            out.push(PendingEnvironmentApproval {
                run_id: codec::run_id(&row.get::<_, String>(0))?,
                job_id: JobId(row.get::<_, String>(1)),
                repository: row.get(2),
                environment_name,
                environment_url,
                check_run_id: row.get::<_, Option<i64>>(4).map(|id| id as u64),
                deployment_id: row.get::<_, Option<i64>>(5).map(|id| id as u64),
            });
        }
        Ok(out)
    }

    /// One held approval-gated job by its GitHub check run id — the
    /// `check_run.requested_action` webhook's join key.
    pub(crate) async fn pending_environment_approval_for_check_run(
        &self,
        check_run_id: u64,
    ) -> Result<Option<PendingEnvironmentApproval>, ControlError> {
        Ok(self
            .pending_environment_approvals_with_announced(None, true)
            .await?
            .into_iter()
            .find(|row| row.check_run_id == Some(check_run_id)))
    }

    /// Stamp `approval_announced` on the job's gate — the announce PATCH was
    /// delivered (or the job reports no checks, so nothing would show).
    /// In-place `jsonb_set`: approvals recorded concurrently are preserved.
    pub(crate) async fn mark_environment_approval_announced(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "UPDATE jobs SET environment_gate = \
                     jsonb_set(environment_gate, '{approval_announced}', 'true'::jsonb) \
                 WHERE run_id = $1::text::uuid AND job_id = $2 \
                   AND environment_gate IS NOT NULL",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        Ok(())
    }

    /// The job's GitHub deployment id, when one was created for it.
    pub(crate) async fn job_deployment_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let client = self.reader().await?;
        let row = client
            .query_opt(
                "SELECT deployment_id FROM jobs \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        Ok(row.and_then(|row| row.get::<_, Option<i64>>(0).map(|id| id as u64)))
    }

    /// Record the GitHub deployment id the reporting path created for the
    /// job's `environment:`.
    pub(crate) async fn set_job_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
        deployment_id: u64,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client
            .execute(
                "UPDATE jobs SET deployment_id = $3 \
                 WHERE run_id = $1::text::uuid AND job_id = $2",
                &[&run_id.0.to_string(), &job_id.0, &(deployment_id as i64)],
            )
            .await
            .map_err(db)?;
        Ok(())
    }
}

impl PgBackend {
    /// `environment_approvals`: the durable review-decision audit rows for
    /// one job, oldest first. Read straight from the audit table — it is not
    /// archived with the run, so archived runs answer here too.
    pub(crate) async fn environment_approvals(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Vec<EnvironmentApprovalAudit>, ControlError> {
        let client = self.reader().await?;
        let rows = client
            .query(
                "SELECT repository, environment, decision, actor, admin_override, comment, \
                        (extract(epoch from decided_at) * 1000000)::int8 \
                 FROM environment_approvals \
                 WHERE run_id = $1::text::uuid AND job_id = $2 \
                 ORDER BY id",
                &[&run_id.0.to_string(), &job_id.0],
            )
            .await
            .map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|row| EnvironmentApprovalAudit {
                run_id,
                job_id: job_id.clone(),
                repository: row.get(0),
                environment: row.get(1),
                decision: row.get(2),
                actor: row.get(3),
                admin_override: row.get(4),
                comment: row.get(5),
                decided_at_unix_nanos: row.get::<_, i64>(6) * 1000,
            })
            .collect())
    }
}

/// Append one durable environment-review audit row
/// (`environment_approvals`): who decided what on which environment, when,
/// and with which comment. Called inside the decision's own transaction so
/// the gate flip and its audit record commit together — a decision can never
/// exist without its record, and vice versa.
///
/// The repository and namespace are read from the run row here rather than
/// carried by the caller: the audit row is what survives the run, so it
/// denormalizes the facts retention will delete. Run archival never touches
/// this table (see the schema comment).
#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_environment_approval_audit(
    tx: &tokio_postgres::Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    repository: &str,
    namespace_id: &str,
    environment: &str,
    decision: &str,
    actor: Option<&str>,
    admin_override: bool,
    comment: Option<&str>,
    decided_at_unix_nanos: i64,
) -> Result<(), ControlError> {
    let run_key = run_id.0.to_string();
    let decided_at_us = decided_at_unix_nanos / 1000;
    tx.execute(
        &format!(
            "INSERT INTO environment_approvals \
             (namespace_id, run_id, job_id, repository, environment, decision, actor, \
              admin_override, comment, decided_at) \
             VALUES ($1, $2::text::uuid, $3, $4, $5, $6, $7, $8, $9, {})",
            ts!("$10")
        ),
        &[
            &namespace_id,
            &run_key,
            &job_id.0,
            &repository,
            &environment,
            &decision,
            &actor,
            &admin_override,
            &comment,
            &decided_at_us,
        ],
    )
    .await
    .map_err(db)?;
    Ok(())
}
