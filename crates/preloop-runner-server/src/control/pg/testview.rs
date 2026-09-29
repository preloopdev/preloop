//! `#[cfg(test)]` read view: rebuild the old `TxState` field shape from the
//! control schema so pre-cutover assertions keep their vocabulary. Read-only
//! mirror of `control/lite/testview.rs`; there is no write path — mutations
//! must go through `ControlBackend` commands.

use super::*;
use crate::concurrency;
use crate::control::testview::TestState;
use crate::models::{
    AssignmentRecord, GitHubTokenRequest, QueuedCancellation, StepKind, StepRecord,
};
use crate::state::{JobSetAdmission, JobSetGate, JobSetId, OidcJobContext};
use codec::us;
use graph::queued_of;
use lookups::{REQUEST_SELECT, request_from_row};
use preloop_gha_protocol::crypto::AgentRsaPublicKey;
use preloop_gha_protocol::{JobId, RegisteredRunner, RunnerSession, SessionId, azdo};
use std::collections::{BTreeMap, BTreeSet};

impl PgBackend {
    /// Snapshot every scheduling table into a [`TestState`] inside one read
    /// transaction on a pooled connection — commit-consistent for test
    /// assertions (commands in tests are awaited before reads).
    pub(crate) async fn test_working_set(&self) -> Result<TestState, ControlError> {
        let mut pooled = self.writer().await?;
        let tx = pooled.transaction().await.map_err(db)?;
        let client = &tx;
        let mut t = TestState::default();
        let (pool_assign, require_assign, liveness) = self.config();
        t.pool_assignments_enabled = pool_assign;
        t.require_job_assignments = require_assign;
        t.runner_liveness_timeout = liveness;
        t.released_bindings_count = self
            .released_bindings
            .load(std::sync::atomic::Ordering::Acquire);

        // ── Runs ─────────────────────────────────────────────────────
        let run_ids = client
            .query("SELECT run_id::text FROM runs ORDER BY created_at", &[])
            .await
            .map_err(db)?;
        for row in run_ids {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            if let Ok(record) = self.run_record(run_id).await {
                t.runs.insert(run_id, record);
            }
        }
        for row in client
            .query(
                "SELECT run_id::text, namespace_id FROM jobs GROUP BY run_id, namespace_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            let run: String = row.get(0);
            t.run_namespaces.insert(codec::run_id(&run)?, row.get(1));
        }
        for row in client
            .query(
                "SELECT repository, workflow_path, last_run_number FROM workflow_run_numbers",
                &[],
            )
            .await
            .map_err(db)?
        {
            let (repository, path): (String, String) = (row.get(0), row.get(1));
            t.workflow_run_counters.insert(
                format!("{repository}\x1f{path}"),
                row.get::<_, i64>(2) as u64,
            );
        }

        // ── Jobs: graph → QueuedJob per node ─────────────────────────
        for row in client
            .query("SELECT run_id::text, job_id, queue_state::text FROM jobs ORDER BY run_order, job_order", &[])
            .await
            .map_err(db)?
        {
            let run_id = codec::run_id(row.get::<_, String>(0).as_str())?;
            let job_id = codec::job_id(row.get::<_, String>(1));
            let state: String = row.get(2);
            let key = (run_id, job_id);
            // Status from the run projection when loaded (which is what old
            // assertions compared against), else from the row itself.
            if let Some(run) = t.runs.get(&run_id)
                && let Some(status) = run.jobs.get(&key.1)
            {
                t.job_status.insert(key.clone(), *status);
            }
            if let Some(graph) = self.load_graph(client, run_id).await?
                && let Some(node) = graph.nodes.get(&key.1)
            {
                // `queued_at` only while the job sits in the ready queue.
                if state == "ready"
                    && let Some(us) = node.enqueued_at_us
                {
                    t.queued_at.insert(key.clone(), codec::us_to_system(us));
                }
                let Some(message) = Self::node_message(client, run_id, &key.1).await?
                else {
                    continue;
                };
                let job = queued_of(&key.1, run_id, node, message);
                match state.as_str() {
                    "ready" => {
                        if job.enqueued_at_unix_nanos > 0 || job.created_at_unix_nanos > 0 {
                            t.ready_index.push_back(job);
                        }
                    }
                    // Claimed jobs are off the ready queue (the runner
                    // owns the claim); `claimed_jobs` holds them.
                    "claimed" => {
                        t.claimed_jobs.insert(key.clone(), job);
                    }
                    "blocked" => t.pending_jobs.push_back(job),
                    "held" => {
                        t.concurrency_blocked.push_back(job.clone());
                        t.held_runs.entry(run_id).or_default().push(job);
                    }
                    "pending_expansion" => t.pending_expansions.push_back(job),
                    "expanding" => {
                        t.expanding.insert(key.clone());
                        t.expanding_jobs.insert(key, job);
                    }
                    _ => {}
                }
            }
        }
        t.ready_count = t.ready_index.len() as i64;
        t.ready_queue_loaded = true;
        if let Some(front) = t.ready_index.front() {
            t.next_queue_labels = front.runs_on.clone();
        }

        // ── Requests ─────────────────────────────────────────────────
        for row in client
            .query(&format!("{REQUEST_SELECT} ORDER BY q.request_id"), &[])
            .await
            .map_err(db)?
        {
            let record = request_from_row(&row)?;
            if record.result.is_none() {
                // session_id is not part of REQUEST_SELECT; backfill the
                // active-request index separately below.
            }
            t.agent_job_requests
                .insert(record.agent_job_id, record.request_id);
            match t.timeline_requests.get(&record.timeline_id) {
                Some(existing) if *existing >= record.request_id => {}
                _ => {
                    t.timeline_requests
                        .insert(record.timeline_id, record.request_id);
                }
            }
            t.job_requests.insert(record.request_id, record);
        }
        for row in client
            .query(
                "SELECT session_id::text, request_id FROM job_requests \
                 WHERE result IS NULL AND session_id IS NOT NULL",
                &[],
            )
            .await
            .map_err(db)?
        {
            t.session_active_requests.insert(row.get(0), row.get(1));
        }
        for row in client
            .query(
                "SELECT request_id, repository, permissions::text, declared, untrusted \
                 FROM github_token_requests",
                &[],
            )
            .await
            .map_err(db)?
        {
            t.github_token_requests.insert(
                row.get(0),
                GitHubTokenRequest {
                    repository: row.get(1),
                    permissions: serde_json::from_str(&row.get::<_, String>(2)).unwrap_or_default(),
                    declared: row.get(3),
                    untrusted: row.get(4),
                },
            );
        }
        t.reindex_requests();
        for row in client
            .query(
                "SELECT q.request_id, m.message_template::text FROM job_requests q \
                 JOIN job_messages m ON m.run_id = q.run_id AND m.job_id = q.job_id \
                 WHERE q.request_id = ( \
                     SELECT MAX(request_id) FROM job_requests \
                     WHERE run_id = q.run_id AND job_id = q.job_id)",
                &[],
            )
            .await
            .map_err(db)?
        {
            if let Ok(message) =
                serde_json::from_str::<azdo::AgentJobRequestMessage>(row.get::<_, &str>(1))
            {
                t.broker_messages.insert(row.get(0), message);
            }
        }
        for row in client
            .query(
                "SELECT run_id::text, job_id, id_token_granted, oidc_environment, \
                        oidc_job_workflow_ref, oidc_job_workflow_sha \
                 FROM job_specs \
                 WHERE id_token_granted OR oidc_environment IS NOT NULL \
                    OR oidc_job_workflow_ref IS NOT NULL",
                &[],
            )
            .await
            .map_err(db)?
        {
            let key = (
                codec::run_id(row.get::<_, String>(0).as_str())?,
                codec::job_id(row.get(1)),
            );
            t.id_token_grants.insert(key.clone(), row.get(2));
            let env: Option<String> = row.get(3);
            let wf_ref: Option<String> = row.get(4);
            let wf_sha: Option<String> = row.get(5);
            if env.is_some() || wf_ref.is_some() || wf_sha.is_some() {
                t.oidc_job_contexts.insert(
                    key,
                    OidcJobContext {
                        environment: env,
                        job_workflow_ref: wf_ref,
                        job_workflow_sha: wf_sha,
                    },
                );
            }
        }

        // ── Runners / sessions / session messages ────────────────────
        for row in client
            .query(
                &format!(
                    "SELECT runner_id, name, labels::text, ephemeral, runner_group_id, \
                            runner_group_name, client_id, public_key, rsa_public_key, \
                            pool_proven, {} FROM runners ORDER BY runner_id",
                    us!("registered_at")
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            let id: i64 = row.get(0);
            let labels: String = row.get(2);
            let public_key: Option<String> = row.get(7);
            t.runners.insert(
                id,
                RegisteredRunner {
                    id,
                    name: row.get(1),
                    labels: serde_json::from_str(&labels).unwrap_or_default(),
                    ephemeral: row.get(3),
                    public_key: public_key.clone(),
                    runner_group_id: row.get::<_, Option<i64>>(4),
                    runner_group_name: row.get(5),
                },
            );
            let registered: i64 = row.get(10);
            t.runner_registered_at
                .insert(id, codec::us_to_system(registered));
            if let Some(client_id) = row.get::<_, Option<String>>(6) {
                t.runner_client_ids.insert(client_id, id);
            }
            if let Some(pem) = public_key
                && let Ok(key) = AgentRsaPublicKey::parse(&pem)
            {
                t.runner_rsa_public_keys.insert(id, key);
            }
            if row.get::<_, bool>(9) {
                t.pool_proven_runners.insert(id);
            }
        }
        for row in client
            .query(
                &format!(
                    "SELECT session_id::text, runner_id, protocol, verified, {} \
                     FROM runner_sessions",
                    us!("last_seen_at")
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            let session_id: String = row.get(0);
            let runner_id: Option<i64> = row.get(1);
            if let Some(runner_id) = runner_id {
                t.broker_session_runners
                    .insert(session_id.clone(), runner_id);
            }
            let last_seen: i64 = row.get(4);
            t.session_last_seen
                .insert(session_id.clone(), codec::us_to_system(last_seen));
            let protocol: &str = row.get(2);
            if protocol == "azdo" {
                t.azdo_sessions.insert(session_id.clone());
            }
            if row.get::<_, bool>(3) {
                t.verified_sessions.insert(session_id.clone());
            }
            t.sessions.insert(
                session_id.clone(),
                RunnerSession {
                    session_id: SessionId(codec::uuid(&session_id)?),
                    runner_id: runner_id.unwrap_or(0),
                },
            );
        }
        for row in client
            .query(
                "SELECT message_id, session_id::text, message_type, body::text \
                 FROM session_messages ORDER BY message_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            let message_id: i64 = row.get(0);
            let session_id: String = row.get(1);
            let message_type: String = row.get(2);
            let body: String = row.get::<_, Option<String>>(3).unwrap_or_default();
            let msg = match serde_json::from_str::<azdo::TaskAgentMessage>(&body) {
                Ok(msg) => msg,
                Err(_) => azdo::TaskAgentMessage {
                    message_id,
                    message_type,
                    body,
                    iv: None,
                },
            };
            t.inflight_messages
                .entry(session_id)
                .or_default()
                .insert(message_id, msg);
        }

        // ── Steps ────────────────────────────────────────────────────
        for row in client
            .query(
                &format!(
                    "SELECT agent_job_id::text, step_id, kind, workflow_index, \
                            runner_number, context_name, name, conclusion, {}, {} \
                     FROM job_steps ORDER BY agent_job_id, position",
                    us!("started_at"),
                    us!("finished_at")
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            let agent = codec::uuid(row.get::<_, String>(0).as_str())?;
            let kind: &str = row.get(2);
            let record = StepRecord {
                id: row.get(1),
                kind: if kind == "workflow" {
                    StepKind::Workflow
                } else {
                    StepKind::Synthetic
                },
                workflow_index: row
                    .get::<_, Option<i32>>(3)
                    .and_then(|i| usize::try_from(i).ok()),
                runner_number: row
                    .get::<_, Option<i32>>(4)
                    .and_then(|n| u32::try_from(n).ok()),
                context_name: row.get(5),
                name: row.get(6),
                conclusion: row.get(7),
                started_at: row.get::<_, Option<i64>>(8).map(codec::us_to_chrono),
                finished_at: row.get::<_, Option<i64>>(9).map(codec::us_to_chrono),
            };
            t.job_steps.entry(agent).or_default().push(record);
        }

        // ── Concurrency / jobsets ────────────────────────────────────
        let mut jobset_ids: BTreeMap<i64, JobSetId> = BTreeMap::new();
        for row in client
            .query(
                "SELECT jobset_id, run_id::text, job_ids::text, state FROM jobsets",
                &[],
            )
            .await
            .map_err(db)?
        {
            let id: i64 = row.get(0);
            let job_ids: BTreeSet<JobId> =
                serde_json::from_str::<Vec<String>>(&row.get::<_, String>(2))
                    .unwrap_or_default()
                    .into_iter()
                    .map(codec::job_id)
                    .collect();
            let jid = JobSetId {
                run_id: codec::run_id(row.get::<_, String>(1).as_str())?,
                job_ids,
            };
            if row.get::<_, &str>(3) == "ready" {
                t.jobset_ready.insert(jid.clone());
            }
            t.jobset_admissions.insert(
                jid.clone(),
                JobSetAdmission {
                    gates: Vec::new(),
                    acquired_keys: BTreeSet::new(),
                },
            );
            jobset_ids.insert(id, jid);
        }
        for row in client
            .query(
                "SELECT jobset_id, repository, group_name, display_name, \
                        cancel_in_progress, queue_mode, acquired \
                 FROM jobset_gates ORDER BY jobset_id, gate_index",
                &[],
            )
            .await
            .map_err(db)?
        {
            let Some(jid) = jobset_ids.get(&row.get::<_, i64>(0)) else {
                continue;
            };
            let repo: String = row.get(1);
            let group: String = row.get(2);
            let gate = JobSetGate {
                key: (repo.to_lowercase(), group.to_lowercase()),
                display_name: row.get(3),
                cancel_in_progress: row.get(4),
                queue: if row.get::<_, &str>(5) == "max" {
                    preloop_gha_parser::ConcurrencyQueue::Max
                } else {
                    preloop_gha_parser::ConcurrencyQueue::Single
                },
            };
            if let Some(adm) = t.jobset_admissions.get_mut(jid) {
                if row.get::<_, bool>(6) {
                    adm.acquired_keys.insert(gate.key.clone());
                }
                adm.gates.push(gate);
            }
        }
        for row in client
            .query(
                "SELECT repository, group_name, display_name, holder_kind, \
                        holder_run_id::text, holder_job_id, holder_jobset_id \
                 FROM concurrency_holds",
                &[],
            )
            .await
            .map_err(db)?
        {
            let repo: String = row.get(0);
            let group: String = row.get(1);
            let display: String = row.get(2);
            let kind: &str = row.get(3);
            let run: String = row.get(4);
            let job: Option<String> = row.get(5);
            let set_id: Option<i64> = row.get(6);
            let holder = match kind {
                "run" => Some(concurrency::Holder::Run(codec::run_id(&run)?)),
                "job" => job
                    .map(|j| {
                        Ok::<_, ControlError>(concurrency::Holder::Job {
                            run_id: codec::run_id(&run).unwrap_or_default(),
                            job_id: codec::job_id(j),
                        })
                    })
                    .transpose()?,
                "jobset" => set_id.and_then(|id| jobset_ids.get(&id)).map(|jid| {
                    concurrency::Holder::JobSet {
                        run_id: jid.run_id,
                        job_ids: jid.job_ids.clone(),
                    }
                }),
                _ => None,
            };
            let key = (repo.to_lowercase(), group.to_lowercase());
            t.holder_keys
                .entry(codec::run_id(&run)?)
                .or_default()
                .push(key.clone());
            let entry = t.concurrency_groups.entry(key).or_default();
            entry.display_name = display;
            entry.running = holder;
        }
        for row in client
            .query(
                "SELECT repository, group_name, holder_kind, holder_run_id::text, \
                        holder_job_id, holder_jobset_id \
                 FROM concurrency_waits ORDER BY wait_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            let repo: String = row.get(0);
            let group: String = row.get(1);
            let kind: &str = row.get(2);
            let run: String = row.get(3);
            let job: Option<String> = row.get(4);
            let set_id: Option<i64> = row.get(5);
            let holder = match kind {
                "run" => Some(concurrency::Holder::Run(codec::run_id(&run)?)),
                "job" => job.map(|j| concurrency::Holder::Job {
                    run_id: codec::run_id(&run).unwrap_or_default(),
                    job_id: codec::job_id(j),
                }),
                "jobset" => set_id.and_then(|id| jobset_ids.get(&id)).map(|jid| {
                    concurrency::Holder::JobSet {
                        run_id: jid.run_id,
                        job_ids: jid.job_ids.clone(),
                    }
                }),
                _ => None,
            };
            if let Some(holder) = holder {
                t.concurrency_groups
                    .entry((repo.to_lowercase(), group.to_lowercase()))
                    .or_default()
                    .pending
                    .push_back(holder);
            }
        }

        // ── Assignments / provisioning / cancellations ───────────────
        for row in client
            .query(
                &format!(
                    "SELECT run_id::text, job_id, runner_id, {}, {} FROM job_assignments",
                    us!("assigned_at"),
                    us!("first_assigned_at")
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            let at: i64 = row.get(3);
            let first: i64 = row.get(4);
            t.job_assignments.insert(
                (
                    codec::run_id(row.get::<_, String>(0).as_str())?,
                    codec::job_id(row.get(1)),
                ),
                AssignmentRecord {
                    runner_id: row.get(2),
                    at: codec::us_to_system(at),
                    first_at: codec::us_to_system(first),
                },
            );
        }
        for row in client
            .query(
                &format!(
                    "SELECT run_id::text, job_id, {} FROM provision_requests",
                    us!("requested_at")
                ),
                &[],
            )
            .await
            .map_err(db)?
        {
            let at: i64 = row.get(2);
            t.pool_pending.insert(
                (
                    codec::run_id(row.get::<_, String>(0).as_str())?,
                    codec::job_id(row.get(1)),
                ),
                codec::us_to_system(at),
            );
        }
        for row in client
            .query(
                "SELECT q.run_id::text, q.job_id, q.agent_job_id::text \
                 FROM job_cancellations c \
                 JOIN job_requests q ON q.request_id = c.request_id \
                 WHERE c.delivered_at IS NULL ORDER BY c.cancellation_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            t.cancellation_queue.push_back(QueuedCancellation {
                run_id: codec::run_id(row.get::<_, String>(0).as_str())?,
                job_id: codec::job_id(row.get(1)),
                agent_job_id: codec::uuid(row.get::<_, String>(2).as_str())?,
            });
        }
        for row in client
            .query(
                "SELECT q.run_id::text, q.job_id, c.reason \
                 FROM job_cancellations c \
                 JOIN job_requests q ON q.request_id = c.request_id \
                 ORDER BY c.cancellation_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            t.cancellation_reasons.push((
                codec::run_id(row.get::<_, String>(0).as_str())?,
                codec::job_id(row.get(1)),
                row.get::<_, Option<String>>(2),
            ));
        }
        for row in client
            .query(
                "SELECT run_id::text, topic FROM outbox_events ORDER BY event_id",
                &[],
            )
            .await
            .map_err(db)?
        {
            let run: Option<String> = row.get(0);
            t.outbox_topics
                .push((run.map(|run| codec::run_id(&run)).transpose()?, row.get(1)));
        }

        // ── Counters ─────────────────────────────────────────────────
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(request_id), 0) + 1 FROM job_requests",
                &[],
            )
            .await
            .map_err(db)?;
        t.next_request_id = row.get(0);
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(message_id), 0) + 1 FROM session_messages",
                &[],
            )
            .await
            .map_err(db)?;
        t.next_message_id = row.get(0);
        let row = client
            .query_one("SELECT COALESCE(MAX(runner_id), 0) + 1 FROM runners", &[])
            .await
            .map_err(db)?;
        t.next_runner_id = row.get(0);
        Ok(t)
    }
}
