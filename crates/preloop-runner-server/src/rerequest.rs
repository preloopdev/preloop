//! Webhook-side re-run triggers that are not check-run reporting.
//!
//! A `check_suite.rerequested` delivery re-runs every member run of the
//! suite as a new attempt — this is the path GitHub's Checks-page "Re-run
//! all jobs" button and the `…/check-suites/:id/rerequest` App API take.
//! The webhook delivery handler calls into [`process_check_suite_rerequest`]
//! and then still feeds the delivery to the `check_suite` event adapter.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::{info, warn};

use crate::RunId;
use crate::SharedState;
use crate::control::backend::ControlBackend;

/// Handle a `check_suite` delivery whose action is `rerequested`: map the
/// suite to its runs by the SHA the suite's check runs reported on — the
/// same coordinate [`crate::github::check_run_report_coords`] uses — and
/// re-run **every** terminal match as a new attempt. preloop groups all of
/// a commit's reported check runs under one suite (it does not persist
/// per-run suite ids), so a suite rerequest covers every workflow run whose
/// checks landed on `check_suite.head_sha`.
///
/// The caller still feeds the delivery to the `check_suite` adapter
/// afterwards (GitHub also triggers `on: check_suite [rerequested]`
/// workflows off the same delivery), so this returns `()` rather than a
/// response.
pub(crate) async fn process_check_suite_rerequest(
    shared: &Arc<SharedState>,
    payload: &Value,
) -> Result<(), StatusCode> {
    if payload.get("action").and_then(Value::as_str) != Some("rerequested") {
        return Ok(());
    }
    let Some(suite) = payload.get("check_suite") else {
        warn!("check_suite rerequest is missing check_suite payload");
        return Ok(());
    };
    let repository = payload
        .get("repository")
        .and_then(|repository| repository.get("full_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(head_sha) = suite
        .get("head_sha")
        .and_then(Value::as_str)
        .filter(|sha| !sha.is_empty())
    else {
        warn!("check_suite rerequest is missing check_suite.head_sha");
        return Ok(());
    };

    // When the payload names an app and a multi-App registry exists, only a
    // suite of a registered App resolves here — a foreign App's suite must
    // not restart our runs. With no registry there is nothing to attribute
    // the suite to, so the SHA match below is all we have.
    if let Some(app_id) = suite
        .get("app")
        .and_then(|app| app.get("id"))
        .and_then(Value::as_u64)
        && let Some(registry) = shared.state.github_apps.as_ref()
        && registry.app_by_id(&app_id.to_string()).is_none()
    {
        info!(
            app_id,
            head_sha, "check_suite rerequest names an unregistered App; ignored"
        );
        return Ok(());
    }

    // Every terminal run whose checks reported on the suite's head SHA is a
    // member. `check_run_report_coords` is the projection's own coordinate
    // (`status_check_sha` for pull_request heads, a synced push's effective
    // SHA, else the submitted SHA), so pull-request runs whose checkout SHA
    // differs from the reported SHA still match. Live matches race us: the
    // backend re-guards and we skip them.
    let mut runs = shared
        .state
        .backend
        .runs_for_repository(repository)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    runs.sort_by_key(|run| run.created_at);
    let targets: Vec<RunId> = runs
        .iter()
        .filter(|run| {
            run.status.is_terminal()
                && crate::github::check_run_report_coords(run)
                    .is_some_and(|(_, sha)| sha == head_sha)
        })
        .map(|run| run.run_id)
        .collect();
    if targets.is_empty() {
        warn!(
            repository,
            head_sha, "check_suite rerequest does not match a known terminal run"
        );
        return Ok(());
    }

    let rerequest_sender = payload
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(Value::as_str);

    let mut reran = 0usize;
    let mut failures = Vec::new();
    for run_id in targets {
        // Re-read the record: an earlier rerun in this loop does not affect
        // siblings, but the protection gate needs this run's own trigger.
        let run = match shared.state.backend.run_record(run_id).await {
            Ok(run) => run,
            Err(error) => {
                failures.push((run_id, error.to_string()));
                continue;
            }
        };
        // Same execution-protection rule the check_run rerequest enforces:
        // the original trigger and the rerequest sender both re-clear
        // policy.
        let denied = [Some(run.submission.actor.as_str()), rerequest_sender]
            .into_iter()
            .flatten()
            .find_map(|protection_actor| {
                crate::execution_protection::denies_submission(
                    &shared.state.execution_protection,
                    &run.submission.event,
                    Some(protection_actor),
                    run.submission.workflow_file.as_deref(),
                )
            });
        if let Some(hit) = denied {
            match shared.state.execution_protection.mode {
                crate::config::ProtectionMode::Enforce => {
                    info!(
                        %run_id,
                        event = %run.submission.event,
                        rule = %hit.describe(),
                        "execution protection denied check_suite rerequest"
                    );
                    continue;
                }
                crate::config::ProtectionMode::Evaluate => {
                    info!(
                        %run_id,
                        event = %run.submission.event,
                        rule = %hit.describe(),
                        "execution protection would deny check_suite rerequest (evaluate mode)"
                    );
                }
            }
        }

        match crate::runs::rerun_run_with_mode(
            shared,
            run_id,
            crate::control::types::RerunMode::All,
            None,
            rerequest_sender.map(str::to_owned),
        )
        .await
        {
            Ok(accepted) => {
                reran += 1;
                info!(
                    %run_id,
                    rerun_run_id = %accepted.run_id,
                    head_sha,
                    "resubmitted check_suite rerequest"
                );
            }
            Err(error) => {
                let status = error.into_response().status();
                if status == StatusCode::CONFLICT {
                    // Lost the terminal race: another attempt started or the
                    // run is being cancelled — a retry finds nothing new, so
                    // this is not a failure worth redelivering for.
                    info!(
                        %run_id,
                        head_sha, "check_suite rerequest target is no longer terminal; skipped"
                    );
                    continue;
                }
                tracing::error!(
                    %run_id,
                    %status,
                    "failed to resubmit check_suite rerequest"
                );
                failures.push((run_id, status.to_string()));
            }
        }
    }

    // A delivery retry re-runs only the members that did not move (the
    // backend refuses a live run with 409, which we skip), so surfacing the
    // failure for redelivery converges instead of duplicating.
    if reran == 0
        && let Some((run_id, error)) = failures.first()
    {
        tracing::error!(
            %run_id,
            %error,
            "check_suite rerequest failed for every matching run"
        );
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    Ok(())
}
