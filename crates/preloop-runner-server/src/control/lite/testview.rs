//! `#[cfg(test)]` read view: rebuild the old `TxState` field shape from the
//! relational tables so pre-cutover assertions keep their vocabulary.
//! Read-only; there is deliberately no write path — tests that need to
//! mutate go through real `ControlBackend` commands or `test_db_mutate`.

use super::*;
use crate::concurrency;
use crate::control::testview::TestState;
use crate::models::{
    AssignmentRecord, GitHubTokenRequest, QueuedCancellation, TaskAgentJobRequestRecord,
};
use crate::state::{JobSetAdmission, JobSetGate, JobSetId, OidcJobContext};
use jobs::{JOB_COLUMNS, JobRow, job_row, queued_job_of};
use preloop_gha_protocol::crypto::AgentRsaPublicKey;
use preloop_gha_protocol::{
    ExecutionStatus, JobId, RegisteredRunner, RunId, RunnerSession, SessionId, azdo,
};
use std::collections::{BTreeMap, BTreeSet};
use steps::{STEP_COLUMNS, step_row};

impl LiteBackend {
    /// Snapshot every scheduling table into a [`TestState`]. Single read
    /// transaction: the view is commit-consistent even while commands run.
    pub(crate) fn test_working_set(&self) -> Result<TestState, ControlError> {
        let mut conn = self.writer.lock();
        let tx = conn.transaction().map_err(db)?;
        let mut t = TestState {
            pool_assignments_enabled: self.pool_assignments_enabled.load(Ordering::Relaxed),
            require_job_assignments: self.require_job_assignments.load(Ordering::Relaxed),
            runner_liveness_timeout: Duration::from_nanos(
                self.runner_liveness_timeout.load(Ordering::Relaxed),
            ),
            released_bindings_count: self.released_bindings.load(Ordering::Relaxed),
            ..Default::default()
        };

        // ── Runs ─────────────────────────────────────────────────────
        let run_ids: Vec<RunId> = tx
            .prepare_cached("SELECT run_id FROM runs ORDER BY created_at")
            .map_err(db)?
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(db)?
            .map(|r| r.map(|s| codec::run_id(&s)))
            .collect::<Result<_, _>>()
            .map_err(db)?;
        for run_id in run_ids {
            if let Some(record) = jobs::run_record(&tx, run_id)? {
                t.runs.insert(run_id, record);
            }
        }
        for row in tx
            .prepare_cached("SELECT run_id, namespace_id FROM jobs GROUP BY run_id, namespace_id")
            .map_err(db)?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db)?
        {
            let (run, ns) = row.map_err(db)?;
            t.run_namespaces.insert(codec::run_id(&run), ns);
        }
        for row in tx
            .prepare_cached(
                "SELECT repository, workflow_path, last_run_number FROM workflow_run_numbers",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(db)?
        {
            // TestState keys the counter by `repository\x1fworkflow_path`
            // (the old TxState map shape); the table's natural key adds
            // namespace, always 'default' for single-node lite.
            let (repository, path, n) = row.map_err(db)?;
            t.workflow_run_counters
                .insert(format!("{repository}\x1f{path}"), n as u64);
        }

        // ── Jobs ─────────────────────────────────────────────────────
        let job_rows: Vec<JobRow> = tx
            .prepare_cached(&format!(
                "SELECT {JOB_COLUMNS} FROM jobs ORDER BY run_order, job_order"
            ))
            .map_err(db)?
            .query_map([], job_row)
            .map_err(db)?
            .collect::<Result<_, _>>()
            .map_err(db)?;
        for jr in job_rows {
            let key = (jr.run_id, jr.job_id.clone());
            t.job_status.insert(key.clone(), jr.status);
            // Old `queued_at` was the reaper's ready-queue mark, retained only
            // while the job sits in the ready queue (`commands.rs` retain).
            if jr.queue_state == "ready"
                && let Some(us) = jr.enqueued_at
            {
                t.queued_at.insert(key.clone(), codec::us_to_system(us));
            }
            match jr.queue_state.as_str() {
                "ready" => {
                    let job = queued_job_of(&tx, jr)?;
                    if job.enqueued_at_unix_nanos > 0 || job.created_at_unix_nanos > 0 {
                        t.ready_index.push_back(job);
                    }
                }
                // Claimed jobs are off the ready queue (the runner owns the
                // claim); `claimed_jobs` holds them for the callers that ask.
                "claimed" => {
                    t.claimed_jobs.insert(key.clone(), queued_job_of(&tx, jr)?);
                }
                "blocked" => t.pending_jobs.push_back(queued_job_of(&tx, jr)?),
                "held" => {
                    let job = queued_job_of(&tx, jr)?;
                    t.concurrency_blocked.push_back(job.clone());
                    t.held_runs.entry(key.0).or_default().push(job);
                }
                "pending_expansion" => t.pending_expansions.push_back(queued_job_of(&tx, jr)?),
                "expanding" => {
                    t.expanding.insert(key.clone());
                    t.expanding_jobs
                        .insert(key.clone(), queued_job_of(&tx, jr)?);
                }
                _ => {}
            }
        }
        t.ready_count = t.ready_index.len() as i64;
        t.ready_queue_loaded = true;
        if let Some(front) = t.ready_index.front() {
            t.next_queue_labels = front.runs_on.clone();
        }

        // ── Job requests / leases / token requests ───────────────────
        {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT q.request_id, q.run_id, q.job_id, q.agent_job_id, \
                            q.timeline_id, q.result, q.timeout_triggered, \
                            q.debug_token_issued, q.claimed_at, q.started_at, \
                            q.runner_id, q.session_id, l.expires_at, l.renewed_at \
                     FROM job_requests q LEFT JOIN job_leases l \
                       ON l.request_id = q.request_id \
                     ORDER BY q.request_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                        row.get::<_, Option<String>>(11)?,
                        row.get::<_, Option<i64>>(12)?,
                        row.get::<_, Option<i64>>(13)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (
                    request_id,
                    run,
                    job,
                    agent,
                    timeline,
                    result,
                    timeout,
                    dbg,
                    claimed,
                    started,
                    runner_id,
                    session_id,
                    expires,
                    renewed,
                ) = row.map_err(db)?;
                let run_id = codec::run_id(&run);
                let job_id = codec::job_id(job);
                let agent_job_id = codec::uuid(&agent);
                let timeline_id = codec::uuid(&timeline);
                let result_status = result.as_deref().map(status_of);
                let record = TaskAgentJobRequestRecord {
                    request_id,
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id,
                    plan_id: codec::plan_id(&agent_job_id),
                    plan_type: "actions".to_owned(),
                    timeline_id,
                    result: result_status,
                    locked_until: codec::lease_string(expires),
                    claimed_at: claimed.map(codec::us_to_system),
                    owner_runner_id: runner_id,
                    started_at: started.map(codec::us_to_system),
                    last_renewed_at: renewed.map(codec::us_to_system),
                    timeout_triggered: timeout != 0,
                    debug_token_issued: dbg != 0,
                };
                if result.is_none()
                    && let Some(sid) = session_id
                {
                    t.session_active_requests.insert(sid, request_id);
                }
                // Correlation indexes: the live map is keyed by request;
                // timeline/agent lookups resolve to the NEWEST request.
                t.agent_job_requests.insert(agent_job_id, request_id);
                match t.timeline_requests.get(&timeline_id) {
                    Some(existing) if *existing >= request_id => {}
                    _ => {
                        t.timeline_requests.insert(timeline_id, request_id);
                    }
                }
                t.job_requests.insert(request_id, record);
            }
        }
        for row in tx
            .prepare_cached(
                "SELECT request_id, repository, permissions, declared, untrusted \
                 FROM github_token_requests",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(db)?
        {
            let (request_id, repo, perms, declared, untrusted) = row.map_err(db)?;
            t.github_token_requests.insert(
                request_id,
                GitHubTokenRequest {
                    repository: repo,
                    permissions: serde_json::from_str(&perms).unwrap_or_default(),
                    declared: declared != 0,
                    untrusted: untrusted != 0,
                },
            );
        }
        t.reindex_requests();
        // Message bodies: the template lives per job; the view exposes it
        // under the job's newest request id (pre-cutover shape).
        for row in tx
            .prepare_cached(
                "SELECT q.request_id, m.message_template FROM job_requests q \
                 JOIN job_messages m ON m.run_id = q.run_id AND m.job_id = q.job_id \
                 WHERE q.request_id = ( \
                     SELECT MAX(request_id) FROM job_requests \
                     WHERE run_id = q.run_id AND job_id = q.job_id)",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(db)?
        {
            let (request_id, body) = row.map_err(db)?;
            if let Ok(message) = serde_json::from_str::<azdo::AgentJobRequestMessage>(&body) {
                t.broker_messages.insert(request_id, message);
            }
        }
        for row in tx
            // Every job row contributes an entry: the old model recorded a
            // grant flag and an (often empty) OIDC context unconditionally
            // at submit — the maps are presence-keyed, not content-keyed.
            .prepare_cached(
                "SELECT run_id, job_id, id_token_granted, oidc_environment, \
                        oidc_job_workflow_ref, oidc_job_workflow_sha \
                 FROM job_specs",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })
            .map_err(db)?
        {
            let (run, job, granted, env, wf_ref, wf_sha) = row.map_err(db)?;
            let key = (codec::run_id(&run), codec::job_id(job));
            t.id_token_grants.insert(key.clone(), granted != 0);
            // Presence-keyed like the old model: the context row exists for
            // every job even when all three fields are empty.
            t.oidc_job_contexts.insert(
                key,
                OidcJobContext {
                    environment: env,
                    job_workflow_ref: wf_ref,
                    job_workflow_sha: wf_sha,
                },
            );
        }

        // ── Runners / sessions / session messages ────────────────────
        {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT runner_id, name, labels, ephemeral, runner_group_id, \
                            runner_group_name, client_id, public_key, \
                            rsa_public_key, pool_proven, registered_at \
                     FROM runners ORDER BY runner_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<Vec<u8>>>(8)?,
                        row.get::<_, i64>(9)?,
                        row.get::<_, i64>(10)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (
                    id,
                    name,
                    labels,
                    ephemeral,
                    group_id,
                    group_name,
                    client_id,
                    public_key,
                    rsa_key,
                    pool_proven,
                    registered_at,
                ) = row.map_err(db)?;
                t.runners.insert(
                    id,
                    RegisteredRunner {
                        id,
                        name,
                        labels: serde_json::from_str(&labels).unwrap_or_default(),
                        ephemeral: ephemeral != 0,
                        public_key: public_key.clone(),
                        runner_group_id: group_id,
                        runner_group_name: group_name,
                    },
                );
                t.runner_registered_at
                    .insert(id, codec::us_to_system(registered_at));
                if let Some(client_id) = client_id {
                    t.runner_client_ids.insert(client_id, id);
                }
                if let Some(pem) = public_key
                    && let Ok(key) = AgentRsaPublicKey::parse(&pem)
                {
                    t.runner_rsa_public_keys.insert(id, key);
                }
                let _ = rsa_key;
                if pool_proven != 0 {
                    t.pool_proven_runners.insert(id);
                }
            }
        }
        {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT session_id, runner_id, protocol, verified, last_seen_at \
                     FROM runner_sessions",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (session_id, runner_id, protocol, verified, last_seen) = row.map_err(db)?;
                if let Some(runner_id) = runner_id {
                    t.broker_session_runners
                        .insert(session_id.clone(), runner_id);
                }
                t.session_last_seen
                    .insert(session_id.clone(), codec::us_to_system(last_seen));
                if protocol == "azdo" {
                    t.azdo_sessions.insert(session_id.clone());
                }
                if verified != 0 {
                    t.verified_sessions.insert(session_id.clone());
                }
                t.sessions.insert(
                    session_id.clone(),
                    RunnerSession {
                        session_id: SessionId(codec::uuid(&session_id)),
                        runner_id: runner_id.unwrap_or(0),
                    },
                );
            }
        }
        {
            let mut stmt = tx
                .prepare_cached(
                    "SELECT message_id, session_id, message_type, body \
                     FROM session_messages ORDER BY message_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (message_id, session_id, message_type, body) = row.map_err(db)?;
                let body = body.unwrap_or_else(|| "{}".to_owned());
                let msg =
                    serde_json::from_str::<azdo::TaskAgentMessage>(&body).unwrap_or_else(|_| {
                        azdo::TaskAgentMessage {
                            message_id,
                            message_type: message_type.clone(),
                            body: body.clone(),
                            iv: None,
                        }
                    });
                t.inflight_messages
                    .entry(session_id)
                    .or_default()
                    .insert(message_id, msg);
            }
        }

        // ── Steps ────────────────────────────────────────────────────
        {
            let mut stmt = tx
                .prepare_cached(&format!(
                    "SELECT {STEP_COLUMNS} FROM job_steps ORDER BY agent_job_id, position"
                ))
                .map_err(db)?;
            let rows = stmt.query_map([], step_row).map_err(db)?;
            for row in rows {
                let (agent, record) = row.map_err(db)?;
                t.job_steps.entry(agent).or_default().push(record);
            }
        }

        // ── Concurrency / jobsets ────────────────────────────────────
        {
            let mut jobset_ids: BTreeMap<i64, JobSetId> = BTreeMap::new();
            let mut stmt = tx
                .prepare_cached("SELECT jobset_id, run_id, job_ids, state FROM jobsets")
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (id, run, ids, state) = row.map_err(db)?;
                let job_ids: BTreeSet<JobId> = serde_json::from_str::<Vec<String>>(&ids)
                    .unwrap_or_default()
                    .into_iter()
                    .map(codec::job_id)
                    .collect();
                let jid = JobSetId {
                    run_id: codec::run_id(&run),
                    job_ids,
                };
                if state == "ready" {
                    t.jobset_ready.insert(jid.clone());
                } else {
                    // `jobset_admissions` mirrors the old in-flight map: a
                    // fully-admitted set holds its concurrency rows but is
                    // no longer a pending admission.
                    t.jobset_admissions.insert(
                        jid.clone(),
                        JobSetAdmission {
                            gates: Vec::new(),
                            acquired_keys: BTreeSet::new(),
                        },
                    );
                }
                jobset_ids.insert(id, jid);
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT jobset_id, gate_index, repository, group_name, \
                            display_name, cancel_in_progress, queue_mode, acquired \
                     FROM jobset_gates ORDER BY jobset_id, gate_index",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (set_id, _gate_index, repo, group, display, cancel_ip, mode, acquired) =
                    row.map_err(db)?;
                let Some(jid) = jobset_ids.get(&set_id) else {
                    continue;
                };
                let gate = JobSetGate {
                    key: (repo.to_lowercase(), group.to_lowercase()),
                    display_name: display,
                    cancel_in_progress: cancel_ip != 0,
                    queue: if mode == "max" {
                        preloop_gha_parser::ConcurrencyQueue::Max
                    } else {
                        preloop_gha_parser::ConcurrencyQueue::Single
                    },
                };
                if let Some(adm) = t.jobset_admissions.get_mut(jid) {
                    if acquired != 0 {
                        adm.acquired_keys.insert(gate.key.clone());
                    }
                    adm.gates.push(gate);
                }
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT namespace_id, repository, group_name, display_name, \
                            holder_kind, holder_run_id, holder_job_id, holder_jobset_id \
                     FROM concurrency_holds",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (ns, repo, group, display, kind, run, job, set_id) = row.map_err(db)?;
                let holder = match kind.as_str() {
                    "run" => Some(concurrency::Holder::Run(codec::run_id(&run))),
                    "job" => job.map(|j| concurrency::Holder::Job {
                        run_id: codec::run_id(&run),
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
                let key = (repo.to_lowercase(), group.to_lowercase());
                t.holder_keys
                    .entry(codec::run_id(&run))
                    .or_default()
                    .push(key.clone());
                let entry = t.concurrency_groups.entry(key).or_default();
                entry.display_name = display;
                entry.running = holder;
                let _ = ns;
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT repository, group_name, holder_kind, holder_run_id, \
                            holder_job_id, holder_jobset_id \
                     FROM concurrency_waits ORDER BY wait_id",
                )
                .map_err(db)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                })
                .map_err(db)?;
            for row in rows {
                let (repo, group, kind, run, job, set_id) = row.map_err(db)?;
                let holder = match kind.as_str() {
                    "run" => Some(concurrency::Holder::Run(codec::run_id(&run))),
                    "job" => job.map(|j| concurrency::Holder::Job {
                        run_id: codec::run_id(&run),
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
        }

        // ── Assignments / provisioning / cancellations ───────────────
        for row in tx
            .prepare_cached(
                "SELECT run_id, job_id, runner_id, assigned_at, first_assigned_at \
                 FROM job_assignments",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(db)?
        {
            let (run, job, runner, at, first) = row.map_err(db)?;
            t.job_assignments.insert(
                (codec::run_id(&run), codec::job_id(job)),
                AssignmentRecord {
                    runner_id: runner,
                    at: codec::us_to_system(at),
                    first_at: codec::us_to_system(first),
                },
            );
        }
        for row in tx
            .prepare_cached("SELECT run_id, job_id, requested_at FROM provision_requests")
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(db)?
        {
            let (run, job, at) = row.map_err(db)?;
            t.pool_pending.insert(
                (codec::run_id(&run), codec::job_id(job)),
                codec::us_to_system(at),
            );
        }
        for row in tx
            .prepare_cached(
                "SELECT c.requested_at, q.run_id, q.job_id, q.agent_job_id \
                 FROM job_cancellations c \
                 JOIN job_requests q ON q.request_id = c.request_id \
                 WHERE c.delivered_at IS NULL ORDER BY c.cancellation_id",
            )
            .map_err(db)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .map_err(db)?
        {
            let (_at, run, job, agent) = row.map_err(db)?;
            t.cancellation_queue.push_back(QueuedCancellation {
                run_id: codec::run_id(&run),
                job_id: codec::job_id(job),
                agent_job_id: codec::uuid(&agent),
            });
        }

        // ── Counters ─────────────────────────────────────────────────
        t.next_request_id = tx
            .query_row(
                "SELECT COALESCE(MAX(request_id), 0) + 1 FROM job_requests",
                [],
                |row| row.get(0),
            )
            .map_err(db)?;
        t.next_message_id = tx
            .query_row(
                "SELECT COALESCE(MAX(message_id), 0) + 1 FROM session_messages",
                [],
                |row| row.get(0),
            )
            .map_err(db)?;
        t.next_runner_id = tx
            .query_row(
                "SELECT COALESCE(MAX(runner_id), 0) + 1 FROM runners",
                [],
                |row| row.get(0),
            )
            .map_err(db)?;
        Ok(t)
    }
    /// Test-only write escape hatch: run `f` inside a writer transaction.
    /// Seeds that no `ControlBackend` command expresses go through this; the
    /// assertions afterwards always read back through `test_working_set` or
    /// a real API — there is no in-memory mirror to drift.
    pub(crate) fn test_db_mutate<R>(
        &self,
        f: impl FnOnce(&TestDb<'_>) -> R,
    ) -> Result<R, ControlError> {
        let mut conn = self.writer.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;
        let out = f(&TestDb(&tx));
        tx.commit().map_err(db)?;
        Ok(out)
    }
}

fn status_of(s: &str) -> ExecutionStatus {
    match s {
        "success" => ExecutionStatus::Success,
        "failure" => ExecutionStatus::Failure,
        "cancelled" => ExecutionStatus::Cancelled,
        "skipped" => ExecutionStatus::Skipped,
        "timed_out" => ExecutionStatus::Failure,
        "in_progress" => ExecutionStatus::InProgress,
        _ => ExecutionStatus::Queued,
    }
}

/// Typed seed/mutation helpers for tests, over a writer transaction. Every
/// helper maps one old `TxState` field write to its relational row(s); they
/// exist so pre-cutover tests keep short bodies instead of raw SQL.
pub(crate) struct TestDb<'a>(pub &'a Transaction<'a>);

impl TestDb<'_> {
    fn db_err(e: rusqlite::Error) -> ControlError {
        ControlError::backend(anyhow::anyhow!(e))
    }

    /// `runs.status`/`conclusion`/`completed_at` for a forced-terminal or
    /// status override (what old tests did through `tx.runs[..]`).
    pub(crate) fn set_run_status(
        &self,
        run_id: RunId,
        status: &str,
        conclusion: Option<&str>,
    ) -> Result<(), ControlError> {
        let now = if conclusion.is_some() {
            Some(codec::system_to_us(std::time::SystemTime::now()))
        } else {
            None
        };
        self.0
            .execute(
                "UPDATE runs SET status = ?2, conclusion = ?3, \
                 completed_at = COALESCE(?4, completed_at) WHERE run_id = ?1",
                rusqlite::params![codec::run_key(run_id), status, conclusion, now],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `job_check_run_ids[job] = id`.
    pub(crate) fn set_job_check_run(
        &self,
        run_id: RunId,
        job_id: &JobId,
        check_run_id: i64,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE jobs SET check_run_id = ?3 WHERE run_id = ?1 AND job_id = ?2",
                rusqlite::params![codec::run_key(run_id), job_id.0, check_run_id],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// Rewrite one key inside `run_submissions.submission` (e.g.
    /// `trust_tier`, `workflow_path`).
    pub(crate) fn set_submission_json(
        &self,
        run_id: RunId,
        json_path: &str,
        value: serde_json::Value,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE run_submissions \
                 SET submission = json_set(submission, ?2, json(?3)) \
                 WHERE run_id = ?1",
                rusqlite::params![
                    codec::run_key(run_id),
                    format!("$.{json_path}"),
                    value.to_string()
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `queued_at`/`enqueued_at` backdating. `us` is microseconds.
    pub(crate) fn set_job_enqueued_us(
        &self,
        run_id: RunId,
        job_id: &JobId,
        enqueued_at_us: Option<i64>,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE jobs SET enqueued_at = ?3 WHERE run_id = ?1 AND job_id = ?2",
                rusqlite::params![codec::run_key(run_id), job_id.0, enqueued_at_us],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `runner_sessions.last_seen_at` backdating.
    pub(crate) fn set_session_seen(
        &self,
        session_id: &str,
        last_seen_us: i64,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE runner_sessions SET last_seen_at = ?2 WHERE session_id = ?1",
                rusqlite::params![
                    crate::control::logic::session_uuid(session_id).to_string(),
                    last_seen_us
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `runners.registered_at` backdating.
    pub(crate) fn set_runner_registered_at(
        &self,
        runner_id: i64,
        registered_at_us: i64,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE runners SET registered_at = ?2 WHERE runner_id = ?1",
                rusqlite::params![runner_id, registered_at_us],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// Request-row fields: `started_at`, `owner_runner_id`, `result`,
    /// `claimed_at`, `session_id`, `runner_id`.
    pub(crate) fn update_request(
        &self,
        request_id: i64,
        started_at_us: Option<i64>,
        owner_runner_id: Option<i64>,
        result: Option<&str>,
        session_id: Option<&str>,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "UPDATE job_requests SET \
                 started_at = COALESCE(?2, started_at), \
                 runner_id = COALESCE(?3, runner_id), \
                 result = COALESCE(?4, result), \
                 session_id = COALESCE(?5, session_id) \
                 WHERE request_id = ?1",
                rusqlite::params![
                    request_id,
                    started_at_us,
                    owner_runner_id,
                    result,
                    session_id.map(|sid| crate::control::logic::session_uuid(sid).to_string())
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `job_leases.renewed_at` backdating (creates the lease row if absent —
    /// only claimed requests have one).
    pub(crate) fn set_lease_renewed(
        &self,
        request_id: i64,
        runner_id: i64,
        renewed_at_us: i64,
        expires_at_us: i64,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at) \
                 VALUES (?1, ?2, ?4, ?3) \
                 ON CONFLICT (request_id) DO UPDATE SET renewed_at = ?3, expires_at = ?4",
                rusqlite::params![request_id, runner_id, renewed_at_us, expires_at_us],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// Escape hatch for one-off seed statements the typed helpers do not
    /// cover.
    pub(crate) fn execute(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> rusqlite::Result<usize> {
        self.0.execute(sql, params)
    }

    /// `job_assignments` staleness: `assigned_at`/`first_assigned_at`.
    pub(crate) fn set_assignment_times(
        &self,
        run_id: RunId,
        job_id: &JobId,
        runner_id: Option<i64>,
        assigned_at_us: Option<i64>,
        first_assigned_at_us: Option<i64>,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "INSERT INTO job_assignments \
                 (run_id, job_id, runner_id, assigned_at, first_assigned_at) \
                 VALUES (?1, ?2, ?5, \
                         COALESCE(?3, CAST(unixepoch('subsec') * 1000000 AS INTEGER)), \
                         COALESCE(?4, CAST(unixepoch('subsec') * 1000000 AS INTEGER))) \
                 ON CONFLICT (run_id, job_id) DO UPDATE SET \
                 runner_id = COALESCE(?5, runner_id), \
                 assigned_at = COALESCE(?3, assigned_at), \
                 first_assigned_at = COALESCE(?4, first_assigned_at)",
                rusqlite::params![
                    codec::run_key(run_id),
                    job_id.0,
                    assigned_at_us,
                    first_assigned_at_us,
                    runner_id
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `pool_pending` row + timestamp (`provision_requests`): inserts the
    /// pending-pairing row when absent, else just updates `requested_at`.
    /// `labels` feed the provision lookup; tests usually want the job's own
    /// labels, e.g. `["ubuntu-latest"]`.
    pub(crate) fn set_provision_requested_us(
        &self,
        run_id: RunId,
        job_id: &JobId,
        labels: &[String],
        requested_at_us: i64,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "INSERT INTO provision_requests \
                 (run_id, job_id, namespace_id, pool_key, labels, requested_at) \
                 VALUES (?1, ?2, 'default', '', ?3, ?4) \
                 ON CONFLICT (run_id, job_id) DO UPDATE SET requested_at = ?4",
                rusqlite::params![
                    codec::run_key(run_id),
                    job_id.0,
                    serde_json::to_string(labels).unwrap_or_else(|_| "[]".to_owned()),
                    requested_at_us
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// A verified broker session row (`broker_session_runners.insert`).
    /// Inserts the runner too when absent (FK).
    pub(crate) fn insert_session(
        &self,
        session_id: &str,
        runner_id: i64,
        protocol: &str,
        verified: bool,
    ) -> Result<(), ControlError> {
        let now = codec::now_us();
        let session_uuid = crate::control::logic::session_uuid(session_id).to_string();
        self.0
            .execute(
                "INSERT OR IGNORE INTO runners \
                 (runner_id, namespace_id, name, labels, ephemeral, client_id, \
                  pool_proven, registered_at) \
                 VALUES (?1, 'default', ?2, '[]', 0, NULL, 1, ?3)",
                rusqlite::params![runner_id, format!("runner-{runner_id}"), now],
            )
            .map_err(Self::db_err)?;
        self.0
            .execute(
                "INSERT OR REPLACE INTO runner_sessions \
                 (session_id, runner_id, protocol, verified, created_at, last_seen_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
                rusqlite::params![session_uuid, runner_id, protocol, verified as i64, now],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// A parked session message (`inflight_messages` insert).
    pub(crate) fn insert_session_message(
        &self,
        session_id: &str,
        message_id: i64,
        message_type: &str,
        request_id: Option<i64>,
        body_json: &str,
    ) -> Result<(), ControlError> {
        let session_uuid = crate::control::logic::session_uuid(session_id).to_string();
        self.0
            .execute(
                "INSERT INTO session_messages \
                 (message_id, session_id, message_type, request_id, body, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    message_id,
                    session_uuid,
                    message_type,
                    request_id,
                    body_json,
                    codec::now_us()
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// A runner row (`tx.runners.insert` + derived `runner_client_ids` /
    /// `pool_proven_runners`).
    pub(crate) fn insert_runner(
        &self,
        runner_id: i64,
        name: &str,
        labels: &[String],
        ephemeral: bool,
        client_id: Option<&str>,
        public_key: Option<&str>,
        pool_proven: bool,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "INSERT OR REPLACE INTO runners \
                 (runner_id, namespace_id, name, labels, ephemeral, client_id, \
                  public_key, pool_proven, registered_at) \
                 VALUES (?1, 'default', ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    runner_id,
                    name,
                    serde_json::to_string(labels).unwrap(),
                    ephemeral as i64,
                    client_id,
                    public_key,
                    pool_proven as i64,
                    codec::now_us()
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// Claimed-but-undelivered state for restart-persistence tests:
    /// dequeue the job (`claimed`), bind the request to the session, park the
    /// message. `runner_id`/`session_id` must already exist (`insert_session`).
    pub(crate) fn mark_claimed_for_session(
        &self,
        run_id: RunId,
        job_id: &JobId,
        request_id: i64,
        session_id: &str,
        runner_id: i64,
        message_json: &str,
        message_id: i64,
    ) -> Result<(), ControlError> {
        let now = codec::now_us();
        self.0
            .execute(
                "UPDATE jobs SET queue_state = 'claimed', \
                 claimed_by_runner_id = ?3, claimed_at = ?4 \
                 WHERE run_id = ?1 AND job_id = ?2",
                rusqlite::params![codec::run_key(run_id), job_id.0, runner_id, now],
            )
            .map_err(Self::db_err)?;
        self.0
            .execute(
                "UPDATE job_requests SET runner_id = ?2, session_id = ?3, \
                 claimed_at = ?4 WHERE request_id = ?1",
                rusqlite::params![
                    request_id,
                    runner_id,
                    crate::control::logic::session_uuid(session_id).to_string(),
                    now
                ],
            )
            .map_err(Self::db_err)?;
        self.insert_session_message(
            session_id,
            message_id,
            "PipelineAgentJobRequest",
            Some(request_id),
            message_json,
        )?;
        Ok(())
    }

    /// New attempt for a `(run, job)`: fresh `request_id` + `agent_job_id`,
    /// same timeline. The derived `agent_job_requests`/`timeline_requests`
    /// maps follow automatically at the next `test_working_set`.
    pub(crate) fn insert_retry_request(
        &self,
        run_id: RunId,
        job_id: &JobId,
        timeline_id: uuid::Uuid,
    ) -> Result<(i64, uuid::Uuid), ControlError> {
        // The agreed schema allows one inflight request per job and unique
        // timeline ids, so a re-dispatch must settle the prior attempt first
        // (exactly what a real retry does) and mint its own timeline.
        self.0
            .execute(
                "UPDATE job_requests SET result = 'cancelled', finished_at = ?3 \
                 WHERE run_id = ?1 AND job_id = ?2 AND result IS NULL",
                rusqlite::params![codec::run_key(run_id), job_id.0, codec::now_us()],
            )
            .map_err(Self::db_err)?;
        let request_id: i64 = self
            .0
            .query_row(
                "SELECT COALESCE(MAX(request_id), 0) + 1 FROM job_requests",
                [],
                |row| row.get(0),
            )
            .map_err(Self::db_err)?;
        let agent_job_id = uuid::Uuid::new_v4();
        let fresh_timeline = uuid::Uuid::new_v4();
        let _ = timeline_id; // every attempt owns a fresh timeline row
        self.0
            .execute(
                "INSERT INTO job_requests \
                 (request_id, run_id, job_id, namespace_id, agent_job_id, timeline_id) \
                 VALUES (?1, ?2, ?3, 'default', ?4, ?5)",
                rusqlite::params![
                    request_id,
                    codec::run_key(run_id),
                    job_id.0,
                    agent_job_id.to_string(),
                    fresh_timeline.to_string()
                ],
            )
            .map_err(Self::db_err)?;
        Ok((request_id, agent_job_id))
    }

    /// One workflow step row (`job_steps` insert for an attempt).
    pub(crate) fn insert_step(
        &self,
        agent_job_id: uuid::Uuid,
        step_id: &str,
        position: i64,
        workflow_index: Option<i64>,
        context_name: Option<&str>,
        name: &str,
        conclusion: &str,
    ) -> Result<(), ControlError> {
        self.0
            .execute(
                "INSERT INTO job_steps \
                 (agent_job_id, step_id, position, kind, workflow_index, \
                  context_name, name, conclusion) \
                 VALUES (?1, ?2, ?3, 'workflow', ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    agent_job_id.to_string(),
                    step_id,
                    position,
                    workflow_index,
                    context_name,
                    name,
                    conclusion
                ],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// `inflight`/`plan`/`agent` request lookups, then `DELETE` the request
    /// row (cascades leases/token requests/messages).
    pub(crate) fn remove_request(&self, request_id: i64) -> Result<(), ControlError> {
        self.0
            .execute(
                "DELETE FROM job_requests WHERE request_id = ?1",
                rusqlite::params![request_id],
            )
            .map_err(Self::db_err)?;
        Ok(())
    }

    /// Look up a request id by `(run, job)` — replaces iterating
    /// `inner.job_requests`.
    pub(crate) fn request_id_for(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<i64>, ControlError> {
        let out = self
            .0
            .query_row(
                "SELECT request_id FROM job_requests \
                 WHERE run_id = ?1 AND job_id = ?2 ORDER BY request_id DESC LIMIT 1",
                rusqlite::params![codec::run_key(run_id), job_id.0],
                |row| row.get(0),
            )
            .optional()
            .map_err(Self::db_err)?;
        Ok(out)
    }

    /// Look up `(request_id, agent_job_id, timeline_id)` of the newest
    /// request of a `(run, job)`.
    pub(crate) fn request_key_for(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<(i64, uuid::Uuid, uuid::Uuid)>, ControlError> {
        let out = self
            .0
            .query_row(
                "SELECT request_id, agent_job_id, timeline_id FROM job_requests \
                 WHERE run_id = ?1 AND job_id = ?2 ORDER BY request_id DESC LIMIT 1",
                rusqlite::params![codec::run_key(run_id), job_id.0],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(Self::db_err)?;
        out.map(|(id, agent, timeline)| {
            Ok::<_, ControlError>((id, codec::uuid(&agent), codec::uuid(&timeline)))
        })
        .transpose()
    }
}
