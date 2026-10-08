//! Webhook-side re-run triggers that are not check-run reporting.
//!
//! A `check_suite.rerequested` delivery re-runs the suite's original run as
//! a new attempt — this is the path GitHub's Checks-page "Re-run all jobs"
//! button and the `…/check-suites/:id/rerequest` App API take. The webhook
//! delivery handler calls into [`process_check_suite_rerequest`] and then
//! still feeds the delivery to the `check_suite` event adapter.

use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::Value;
use tracing::{info, warn};

use crate::SharedState;
use crate::control::backend::ControlBackend;

/// Handle a `check_suite` delivery whose action is `rerequested`: map the
/// suite back to its run by `(repository, head_sha)` — the newest completed
/// run wins — apply the same execution-protection gate a `check_run`
/// rerequest applies, and re-run every job as a new attempt.
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

    // Newest completed run of the repo at this head SHA — a rerun must see
    // a terminal run (the backend re-guards, so an in-flight match errors
    // out harmlessly rather than corrupting a live attempt).
    let mut runs = shared
        .state
        .backend
        .runs_for_repository(repository)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    runs.sort_by_key(|run| std::cmp::Reverse(run.created_at));
    let target = runs
        .iter()
        .find(|run| run.head_sha == head_sha && run.status.is_terminal());
    let Some(run) = target else {
        warn!(
            repository,
            head_sha, "check_suite rerequest does not match a known terminal run"
        );
        return Ok(());
    };
    let run_id = run.run_id;

    // Same execution-protection rule the check_run rerequest enforces: the
    // original trigger and the rerequest sender both re-clear policy.
    let event = run.submission.event.clone();
    let actor = run.submission.actor.clone();
    let workflow_file = run.submission.workflow_file.clone();
    let rerequest_sender = payload
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(Value::as_str);
    let denied = [Some(actor.as_str()), rerequest_sender]
        .into_iter()
        .flatten()
        .find_map(|protection_actor| {
            crate::execution_protection::denies_submission(
                &shared.state.execution_protection,
                &event,
                Some(protection_actor),
                workflow_file.as_deref(),
            )
        });
    if let Some(hit) = denied {
        match shared.state.execution_protection.mode {
            crate::config::ProtectionMode::Enforce => {
                info!(
                    %run_id,
                    event = %event,
                    rule = %hit.describe(),
                    "execution protection denied check_suite rerequest"
                );
                return Ok(());
            }
            crate::config::ProtectionMode::Evaluate => {
                info!(
                    %run_id,
                    event = %event,
                    rule = %hit.describe(),
                    "execution protection would deny check_suite rerequest (evaluate mode)"
                );
            }
        }
    }

    let accepted = crate::runs::rerun_run_with_mode(
        shared,
        run_id,
        crate::control::types::RerunMode::All,
        None,
    )
    .await
    .map_err(|error| {
        tracing::error!(
            %run_id,
            ?error,
            "failed to resubmit check_suite rerequest"
        );
        error.into_response().status()
    })?;
    info!(
        %run_id,
        rerun_run_id = %accepted.run_id,
        head_sha,
        "resubmitted check_suite rerequest"
    );
    Ok(())
}
