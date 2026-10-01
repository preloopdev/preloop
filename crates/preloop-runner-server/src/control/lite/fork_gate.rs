//! Fork-PR approval holds and environment protection gates on SQLite.
//!
//! A run the fork policy holds, and a job whose `[environment_rules]` gate is
//! not satisfied, are parked at submit (`queue_state = 'held'`, status
//! `pending`, no concurrency wait of their own). Nothing in the ordinary
//! promotion path moves them: `promote_run` only ever admits `blocked` rows.
//! [`LiteBackend::promote_ready_jobs`] is the one way out — it re-evaluates
//! every parked job's gate, fails closed the ones that resolved to failure,
//! hands the rest back as `blocked`, and then runs the ordinary promotion
//! sweep. The reaper drives it on wall-clock time (wait timers, approval
//! windows); the approve endpoints drive it per run.

use super::codec;
use super::promote;
use super::{LiteBackend, db, jobs};
use crate::control::backend::{EnvironmentGateRead, PromoteOutcome};
use crate::control::types::{ControlError, EnvironmentDeploymentRow, PendingEnvironmentApproval};
use crate::models::EnvironmentGateState;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};
use rusqlite::{OptionalExtension, Transaction, params};

impl LiteBackend {
    /// `promote_ready_jobs`: see the trait contract. `None` sweeps every run
    /// parking a gate-armed job (the reaper's wall-clock sweep); a run with
    /// no parked job is a no-op.
    pub(crate) async fn promote_ready_jobs(
        &self,
        run: Option<RunId>,
    ) -> Result<PromoteOutcome, ControlError> {
        self.write(move |tx| promote_ready_jobs_tx(tx, self, run))
    }

    /// `environment_gate`: one job's stored environment, armed gate and
    /// status. `None` when the run or job does not exist.
    pub(crate) async fn environment_gate(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentGateRead>, ControlError> {
        let job_id = job_id.clone();
        self.read(move |tx| {
            let Some(job) = jobs::job(tx, run_id, &job_id)? else {
                return Ok(None);
            };
            let spec = jobs::load_spec(tx, run_id, &job_id)?;
            Ok(Some(EnvironmentGateRead {
                environment: spec.and_then(|spec| spec.environment),
                gate: job.environment_gate,
                status: job.status,
            }))
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
        self.read(move |tx| pending_environment_approvals_tx(tx, run_id, false))
    }

    /// One held approval-gated job by its GitHub check run id — the
    /// `check_run.requested_action` webhook's join key.
    pub(crate) async fn pending_environment_approval_for_check_run(
        &self,
        check_run_id: u64,
    ) -> Result<Option<PendingEnvironmentApproval>, ControlError> {
        self.read(move |tx| {
            pending_environment_approvals_tx(tx, None, true)?
                .into_iter()
                .find(|row| row.check_run_id == Some(check_run_id))
                .map(Some)
                .map(Ok)
                .unwrap_or(Ok(None))
        })
    }

    /// Stamp `approval_announced` on the job's gate — the announce PATCH was
    /// delivered (or the job reports no checks, so nothing would show).
    /// In-place JSON update: approvals recorded concurrently are preserved.
    pub(crate) async fn mark_environment_approval_announced(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<(), ControlError> {
        let job_id = job_id.clone();
        self.write(move |tx| {
            tx.prepare_cached(
                "UPDATE jobs SET environment_gate = \
                     json_set(environment_gate, '$.approval_announced', json('true')) \
                 WHERE run_id = ?1 AND job_id = ?2 AND environment_gate IS NOT NULL",
            )
            .map_err(db)?
            .execute(params![codec::run_key(run_id), job_id.0])
            .map_err(db)?;
            Ok(())
        })
    }

    /// The job's GitHub deployment id, when one was created for it.
    pub(crate) async fn job_deployment_id(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<u64>, ControlError> {
        let job_id = job_id.clone();
        self.read(move |tx| {
            tx.prepare_cached("SELECT deployment_id FROM jobs WHERE run_id = ?1 AND job_id = ?2")
                .map_err(db)?
                .query_row(params![codec::run_key(run_id), job_id.0], |row| {
                    row.get::<_, Option<i64>>(0)
                })
                .optional()
                .map_err(db)
                .map(|id| id.flatten().map(|id| id as u64))
        })
    }

    /// Record the job's GitHub deployment id (created by the reporting path).
    pub(crate) async fn set_job_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
        deployment_id: u64,
    ) -> Result<(), ControlError> {
        let job_id = job_id.clone();
        self.write(move |tx| {
            tx.prepare_cached(
                "UPDATE jobs SET deployment_id = ?3 WHERE run_id = ?1 AND job_id = ?2",
            )
            .map_err(db)?
            .execute(params![
                codec::run_key(run_id),
                job_id.0,
                deployment_id as i64
            ])
            .map_err(db)?;
            Ok(())
        })
    }

    /// `environment_deployment`: check run + deployment + resolved
    /// environment for one job, assembled from `jobs`, `runs`, `job_specs`
    /// and `job_messages`. Works on any job state (the terminal deployment
    /// status posts after the row leaves `held`). `None` when the job is
    /// missing or no environment name resolves.
    pub(crate) async fn environment_deployment(
        &self,
        run_id: RunId,
        job_id: &JobId,
    ) -> Result<Option<EnvironmentDeploymentRow>, ControlError> {
        let job_id = job_id.clone();
        self.read(move |tx| {
            let row = tx
                .prepare_cached(
                    "SELECT r.repository, r.head_sha, j.environment_gate, \
                            j.check_run_id, j.deployment_id, \
                            s.environment, m.message_template \
                     FROM jobs j \
                     JOIN runs r ON r.run_id = j.run_id \
                     LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
                     LEFT JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
                     WHERE j.run_id = ?1 AND j.job_id = ?2",
                )
                .map_err(db)?
                .query_row(params![codec::run_key(run_id), job_id.0], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                    ))
                })
                .optional()
                .map_err(db)?;
            let Some((
                repository,
                head_sha,
                gate_json,
                check_run_id,
                deployment_id,
                spec_env,
                template,
            )) = row
            else {
                return Ok(None);
            };
            let Some((environment, environment_url)) = resolve_environment_parts(
                gate_json.as_deref(),
                template.as_deref(),
                spec_env.as_deref(),
            ) else {
                return Ok(None);
            };
            Ok(Some(EnvironmentDeploymentRow {
                repository,
                head_sha,
                environment,
                environment_url,
                check_run_id: check_run_id.map(|id| id as u64),
                deployment_id: deployment_id.map(|id| id as u64),
            }))
        })
    }
}

/// Resolve `(environment_name, environment_url)` for one job from its three
/// sources: the armed gate blob (post-hydration stamp), the stored runner
/// message (`environment` / `actionsEnvironment` — deferred names and
/// expression URLs resolve into it), and the spec literal. `None` when no
/// source yields a name — the job isn't an environment job.
fn resolve_environment_parts(
    gate_json: Option<&str>,
    template: Option<&str>,
    spec_env: Option<&str>,
) -> Option<(String, Option<String>)> {
    let gate: Option<EnvironmentGateState> =
        gate_json.and_then(|json| serde_json::from_str(json).ok());
    let message_environment = template.and_then(|t| {
        serde_json::from_str::<serde_json::Value>(t)
            .ok()
            .and_then(|message| {
                message
                    .get("environment")
                    .or_else(|| message.get("actionsEnvironment"))
                    .cloned()
            })
    });
    let spec = spec_env.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let environment = gate
        .and_then(|gate| gate.environment_name)
        .or_else(|| {
            message_environment
                .as_ref()
                .and_then(|env| env.get("name"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            spec.as_ref().and_then(|value| {
                crate::runtime_scheduling::environment_gate_name_of(Some(value)).map(str::to_owned)
            })
        })?;
    // The hydrated message URL wins: `environment.url` expressions resolve
    // into it before dispatch, while the spec literal may still hold `${{}}`.
    let environment_url = message_environment
        .as_ref()
        .and_then(|env| env.get("url"))
        .and_then(serde_json::Value::as_str)
        .filter(|url| !url.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            spec.as_ref()
                .and_then(|value| value.get("url"))
                .and_then(serde_json::Value::as_str)
                .filter(|url| !url.is_empty())
                .map(str::to_owned)
        });
    Some((environment, environment_url))
}

/// `pending_environment_approvals` over one (or every) run. The gate blob
/// carries the armed-at name; `environment.url` comes from the job spec's
/// `environment:` object (expression URLs resolve into the runner message,
/// which the announce path reads from `job_messages` when present).
fn pending_environment_approvals_tx(
    tx: &Transaction<'_>,
    run_id: Option<RunId>,
    include_announced: bool,
) -> Result<Vec<PendingEnvironmentApproval>, ControlError> {
    let (where_run, run_param): (&str, Vec<rusqlite::types::Value>) = match run_id {
        Some(run_id) => ("AND j.run_id = ?1", vec![codec::run_key(run_id).into()]),
        None => ("", Vec::new()),
    };
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT j.run_id, j.job_id, r.repository, j.environment_gate, \
                    j.check_run_id, j.deployment_id, \
                    s.environment, m.message_template \
             FROM jobs j \
             JOIN runs r ON r.run_id = j.run_id \
             LEFT JOIN job_specs s ON s.run_id = j.run_id AND s.job_id = j.job_id \
             LEFT JOIN job_messages m ON m.run_id = j.run_id AND m.job_id = j.job_id \
             WHERE j.queue_state = 'held' AND j.status = 'pending' \
               AND j.environment_gate IS NOT NULL {where_run} \
             ORDER BY j.run_id, j.job_order"
        ))
        .map_err(db)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(run_param), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })
        .map_err(db)?;
    let mut out = Vec::new();
    for row in rows {
        let (run, job_id, repository, gate_json, check_run_id, deployment_id, spec_env, template) =
            row.map_err(db)?;
        let gate: EnvironmentGateState = serde_json::from_str(&gate_json).unwrap_or_default();
        // Only armed approval gates are reportable: the pending row exists
        // so the announce loop PATCHes the check run once, not every tick.
        if gate.approval_requested_at_unix_nanos.is_none()
            || (!include_announced && gate.approval_announced)
        {
            continue;
        }
        let Some((environment_name, environment_url)) =
            resolve_environment_parts(Some(&gate_json), template.as_deref(), spec_env.as_deref())
        else {
            continue;
        };
        out.push(PendingEnvironmentApproval {
            run_id: codec::run_id(&run),
            job_id: JobId(job_id),
            repository,
            environment_name,
            environment_url,
            check_run_id: check_run_id.map(|id| id as u64),
            deployment_id: deployment_id.map(|id| id as u64),
        });
    }
    Ok(out)
}

fn promote_ready_jobs_tx(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run: Option<RunId>,
) -> Result<PromoteOutcome, ControlError> {
    let now_unix_nanos = crate::models::now_unix_nanos();
    let mut outcome = PromoteOutcome::default();
    for run_id in sweep_runs(tx, run)? {
        release_parked_jobs(tx, backend, run_id, now_unix_nanos, &mut outcome)?;
        let mut scheduling = crate::runtime_scheduling::SchedulingOutcome::default();
        promote::promote_run(tx, backend, run_id, &mut scheduling)?;
        outcome.promoted += scheduling.promoted;
        outcome.failed += scheduling.failed.len();
        outcome
            .failed_jobs
            .extend(scheduling.failed.iter().map(|(_, job)| job.clone()));
    }
    outcome.next_runs_on = jobs::next_ready_labels(tx)?;
    Ok(outcome)
}

/// The runs this pass must visit: the named one, or — for the reaper's
/// wall-clock sweep — every run currently parking a gate-armed job. A run
/// parked only by the fork hold is never reached here: it has no clock of its
/// own (its window is the expiry sweep's business), and the approve path
/// names it explicitly.
fn sweep_runs(tx: &Transaction<'_>, run: Option<RunId>) -> Result<Vec<RunId>, ControlError> {
    if let Some(run_id) = run {
        return Ok(vec![run_id]);
    }
    let mut stmt = tx
        .prepare_cached(
            "SELECT DISTINCT run_id FROM jobs \
             WHERE queue_state = 'held' AND status = 'pending' \
               AND environment_gate IS NOT NULL ORDER BY run_id",
        )
        .map_err(db)?;
    let rows = stmt
        .query_map(params![], |row| row.get::<_, String>(0))
        .map_err(db)?;
    Ok(rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(db)?
        .iter()
        .map(|run| codec::run_id(run))
        .collect())
}

/// Re-evaluate every job `run_id` parked on a hold and release the ones whose
/// hold lifted. A run still awaiting fork approval admits nothing at all —
/// that is the whole point of the hold.
///
/// Gate satisfaction is stamped, never cleared: a released job hands its
/// gate row to the promotion sweep, which re-evaluates it against the same
/// rules — a still-running wait timer re-arms instead of losing its
/// deadline. Only a rule that resolves to "no protection" clears the row
/// (inside `evaluate_environment_gate`).
fn release_parked_jobs(
    tx: &Transaction<'_>,
    backend: &LiteBackend,
    run_id: RunId,
    now_unix_nanos: i64,
    outcome: &mut PromoteOutcome,
) -> Result<(), ControlError> {
    let run = codec::run_key(run_id);
    let Some((repository, git_ref, fork_pending)) = tx
        .prepare_cached("SELECT repository, ref, fork_approval_pending FROM runs WHERE run_id = ?1")
        .map_err(db)?
        .query_row([&run], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
            ))
        })
        .optional()
        .map_err(db)?
    else {
        return Ok(());
    };
    if fork_pending {
        return Ok(());
    }
    let resolver = backend.environment_resolver();
    for job in parked_jobs(tx, run_id)? {
        let spec = jobs::load_spec(tx, run_id, &job.job_id)?;
        let mut gate = job.environment_gate.clone();
        // The resolved name wins: a `Pending` arm records the post-hydration
        // name so the sweep re-evaluates against the environment GitHub
        // knows, not the expression text.
        let env_name = gate
            .as_ref()
            .and_then(|gate| gate.environment_name.clone())
            .or_else(|| {
                crate::runtime_scheduling::environment_gate_name_of(
                    spec.as_ref().and_then(|spec| spec.environment.as_ref()),
                )
                .map(str::to_owned)
            });
        let Some(env_name) = env_name else {
            continue;
        };
        let lookup = resolver.lookup_sync(&repository, &env_name);
        let verdict = crate::runtime_scheduling::evaluate_environment_gate(
            &lookup,
            &git_ref,
            run_id,
            &job.job_id,
            &env_name,
            &mut gate,
            now_unix_nanos,
        );
        match verdict {
            crate::runtime_scheduling::EnvironmentGateOutcome::Proceed => {
                // The gate is satisfied: keep the stamps (the promotion
                // sweep re-evaluates them — losing them here would re-arm
                // a satisfied wait timer as a fresh gate) and hand the job
                // back to the ordinary promotion path.
                write_gate(tx, run_id, &job.job_id, gate)?;
                tx.prepare_cached(
                    "UPDATE jobs SET queue_state = 'blocked', status = 'queued' \
                     WHERE run_id = ?1 AND job_id = ?2 AND queue_state = 'held'",
                )
                .map_err(db)?
                .execute(params![run, job.job_id.0])
                .map_err(db)?;
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Wait => {
                // Still gated: keep the progress stamps (the wait deadline
                // the timer armed, the approval request time) so the next
                // sweep resumes rather than restarts the gate.
                write_gate(tx, run_id, &job.job_id, gate)?;
            }
            crate::runtime_scheduling::EnvironmentGateOutcome::Failed => {
                // Fail closed: the job never dispatches, and its dependents
                // settle through the ordinary sweep.
                write_gate(tx, run_id, &job.job_id, gate)?;
                super::settle::settle_node(
                    tx,
                    backend,
                    run_id,
                    &job.job_id,
                    ExecutionStatus::Failure,
                )?;
                outcome.failed += 1;
                outcome.failed_jobs.push(job.job_id.clone());
            }
        }
    }
    Ok(())
}

/// The jobs `run_id` parked at submit: `held` and `pending` with no
/// concurrency wait of their own (`concurrency_waits` is what re-arms a
/// gate-parked job, so a row naming this run, job or jobset means the job is
/// waiting on a concurrency slot, not on a hold).
fn parked_jobs(tx: &Transaction<'_>, run_id: RunId) -> Result<Vec<jobs::JobRow>, ControlError> {
    let run = codec::run_key(run_id);
    let mut stmt = tx
        .prepare_cached(&format!(
            "SELECT {} FROM jobs j WHERE j.run_id = ?1 \
               AND j.queue_state = 'held' AND j.status = 'pending' \
               AND NOT EXISTS (SELECT 1 FROM concurrency_waits w \
                     WHERE w.holder_run_id = j.run_id \
                       AND (w.holder_kind = 'run' \
                            OR (w.holder_kind = 'job' AND w.holder_job_id = j.job_id) \
                            OR (w.holder_kind = 'jobset' AND EXISTS ( \
                                SELECT 1 FROM jobsets s \
                                JOIN json_each(s.job_ids) je ON je.value = j.job_id \
                                WHERE s.jobset_id = w.holder_jobset_id)))) \
             ORDER BY j.job_order",
            jobs::JOB_COLUMNS
        ))
        .map_err(db)?;
    let rows = stmt.query_map([run], jobs::job_row).map_err(db)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(db)
}

/// Persist one job's gate progress (`None` clears it).
pub(super) fn write_gate(
    tx: &Transaction<'_>,
    run_id: RunId,
    job_id: &JobId,
    gate: Option<EnvironmentGateState>,
) -> Result<(), ControlError> {
    let json = gate
        .map(|gate| serde_json::to_string(&gate).map_err(ControlError::backend))
        .transpose()?;
    tx.prepare_cached("UPDATE jobs SET environment_gate = ?3 WHERE run_id = ?1 AND job_id = ?2")
        .map_err(db)?
        .execute(params![codec::run_key(run_id), job_id.0, json])
        .map_err(db)?;
    Ok(())
}
