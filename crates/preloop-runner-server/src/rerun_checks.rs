//! Check-run publication for re-run attempts (new module, kept separate
//! from `github.rs` so the check-run outbox rework can land beside it).
//!
//! [`report_rerun_check_runs`] is the rerun analogue of
//! [`crate::github::report_check_runs_for_run`]: it wakes the queued (and,
//! for jobs concluded during re-admission, completed) check-run transition
//! for the jobs a rerun reset. The difference is the `selected` filter —
//! a failed-only or single-job rerun leaves carried-forward jobs' existing
//! check runs alone, exactly like GitHub keeps the prior attempt's checks
//! for jobs it did not re-run.
//!
//! The actual GitHub work stays in the existing reporters
//! ([`crate::github::report_check_run_queued`],
//! [`crate::github::report_existing_check_run_queued`],
//! [`crate::github::report_check_run_completed`]); this module only
//! selects which jobs they should act on, so it does not need to know how
//! the check-run queue drains.

use std::collections::BTreeSet;
use std::sync::Arc;

use preloop_gha_protocol::JobId;
use tracing::warn;

use crate::control::backend::ControlBackend;
use crate::SharedState;

/// Publish queued/completed checks for a rerun attempt. `selected` is the
/// set of jobs the rerun reset; `None` means every job (a full rerun) and
/// falls through to [`crate::github::report_check_runs_for_run`], preserving
/// the pre-rerun-mode behavior byte for byte.
pub async fn report_rerun_check_runs(
    shared: &Arc<SharedState>,
    run_id: preloop_gha_protocol::RunId,
    reused_check_run: Option<(JobId, u64)>,
    selected: Option<&BTreeSet<JobId>>,
) {
    let Some(selected) = selected else {
        crate::github::report_check_runs_for_run(shared, run_id, reused_check_run).await;
        return;
    };
    // The rerun reports checks even when every job is an expandable
    // placeholder — those mint nothing here, but their materialized legs
    // report later and need the flag.
    if let Err(error) = shared
        .state
        .backend
        .set_reports_check_runs(run_id, true)
        .await
    {
        warn!(%run_id, ?error, "failed to stamp reports_check_runs for rerun");
    }
    let outcome = shared
        .state
        .backend
        .run_dispatch_info(run_id)
        .await
        .map_err(crate::ApiError::from)
        .ok()
        .flatten();
    let Some(info) = outcome else {
        return;
    };
    let (repository, sha) = (info.repository, info.sha);
    // Expandable nodes (deferred matrices, reusable callers) are
    // placeholders: expansion replaces them, and their materialized legs
    // mint their own checks. A `queued` check minted here would strand on
    // GitHub (no delete API). Jobs outside the reset set keep their prior
    // check runs.
    let jobs = info
        .jobs
        .into_iter()
        .filter(|job| !job.placeholder && selected.contains(&job.job_id))
        .map(|job| (job.job_id, job.status))
        .collect::<Vec<_>>();

    for (job_id, status) in jobs {
        match &reused_check_run {
            Some((reused_job_id, check_run_id)) if reused_job_id == &job_id => {
                if let Err(error) = crate::github::report_existing_check_run_queued(
                    shared,
                    &repository,
                    &job_id,
                    run_id,
                    *check_run_id,
                )
                .await
                {
                    warn!(%run_id, %job_id, ?error, "failed to requeue GitHub check run");
                }
            }
            _ => {
                if let Err(error) =
                    crate::github::report_check_run_queued(shared, &repository, &sha, &job_id, run_id)
                        .await
                {
                    warn!(%run_id, %job_id, ?error, "failed to report queued GitHub check run");
                }
            }
        }

        if status.is_terminal() {
            crate::github::report_check_run_completed(shared, run_id, &job_id, status).await;
        }
    }
}
