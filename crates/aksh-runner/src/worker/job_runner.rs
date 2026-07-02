//! Job execution — the core worker loop.
//!
//! Receives an `AgentJobRequestMessage`, sets up the execution context,
//! runs steps, and reports results back to the server.
//!
//! ## Reporting pipeline (F018+F019+F020+F025)
//!
//! Golden 06 wire flow:
//!   acquirejob → renewjob (background) → WorkflowStepsUpdate (step transitions)
//!   → GetStepLogsSignedBlobURL + PUT (per-step) → GetJobLogsSignedBlobURL + PUT
//!   → completejob
//!
//! This module wires `ServerQueue`, `ResultsClient`, and `RunServiceClient` to
//! implement the full reporting lifecycle.

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info, warn};

use super::execution_context::Annotation;
use super::server_queue::ServerQueue;
use super::steps_runner::{Step, StepType};
use crate::cli::ProtocolPath;
use crate::client::http::HttpClient;
use crate::client::results::ResultsClient;
use crate::client::run_service::RunServiceClient;

/// Shared reporting context for step updates and log uploads.
pub struct ReportingContext {
    pub results: ResultsClient,
    pub run_service: RunServiceClient,
    pub access_token: String,
    pub plan_id: String,
    pub job_id: String,
}

/// Execute a job from the deserialized message.
pub async fn run_job(
    job_message: serde_json::Value,
    via: ProtocolPath,
    cancel_rx: watch::Receiver<bool>,
) -> Result<()> {
    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let job_name = job_message
        .get("jobDisplayName")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    info!("Starting job: {job_name} ({job_id})");

    let steps = job_message
        .get("steps")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let variables = job_message
        .get("variables")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let context_data = job_message
        .get("contextData")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    // Build execution context
    let mut job_ctx = super::contexts::JobContext::new(
        job_id.to_string(),
        job_name.to_string(),
        variables,
        context_data,
    );

    // Initialize workspace
    let workspace = super::job_extension::setup_workspace(&job_message)?;
    job_ctx.workspace = Some(workspace.clone());

    // Inject GITHUB_* environment variables
    super::job_extension::inject_github_env(&mut job_ctx, &job_message);

    // Build step list (F023: includes pre/post from already-downloaded manifests)
    let main_steps = super::job_extension::build_step_list(&steps, &job_message);
    let ordered_steps =
        super::job_extension::build_step_list_with_lifecycle(main_steps, &workspace);

    // Extract plan ID
    let plan_id = job_message
        .get("plan")
        .and_then(|p| p.get("planId"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Set up reporting context (F018/F019/F020)
    let reporting = if let Some((service_url, access_token)) =
        extract_service_endpoint(&job_message)
    {
        let http = HttpClient::new(None)?;
        let results_url = extract_results_url(&job_message).unwrap_or_else(|| service_url.clone());
        Some(Arc::new(ReportingContext {
            results: ResultsClient::new(http.clone(), results_url),
            run_service: RunServiceClient::new(http, service_url),
            access_token,
            plan_id: plan_id.clone(),
            job_id: job_id.to_string(),
        }))
    } else {
        warn!("No SystemVssConnection endpoint — reporting disabled");
        None
    };

    // F018: Spawn renew loop
    let renew_handle = if let Some(ref rpt) = reporting {
        Some(spawn_renew_loop(rpt.clone(), cancel_rx.clone()))
    } else {
        None
    };

    // Create the server queue for step status tracking
    let queue = Arc::new(Mutex::new(ServerQueue::new(
        job_id.to_string(),
        plan_id.clone(),
    )));

    // Execute steps with reporting
    let job_result = super::steps_runner::run_steps(
        &ordered_steps,
        &mut job_ctx,
        &workspace,
        cancel_rx,
        queue.clone(),
        reporting.as_deref(),
    )
    .await;

    // F018: Stop renew loop
    if let Some(handle) = renew_handle {
        handle.abort();
    }

    let (result_str, conclusion) = match &job_result {
        Ok(conclusion) => {
            info!("Job {job_name} completed: {conclusion}");
            (conclusion.clone(), conclusion.clone())
        }
        Err(e) => {
            error!("Job {job_name} failed: {e:#}");
            ("Failed".to_string(), "Failed".to_string())
        }
    };

    // F019: Send final WorkflowStepsUpdate with all steps completed
    if let Some(ref rpt) = reporting {
        let mut q = queue.lock().await;
        if let Some(body) = q.take_steps_update_body() {
            let body_json = serde_json::to_value(&body).unwrap_or_default();
            match rpt
                .results
                .update_workflow_steps(&rpt.access_token, &body_json)
                .await
            {
                Ok(_) => info!(
                    "Final WorkflowStepsUpdate sent ({} steps)",
                    body.steps.len()
                ),
                Err(e) => warn!("WorkflowStepsUpdate failed (non-fatal): {e:#}"),
            }
        }
    }

    // F020: Upload job log (concatenation of all step logs)
    if let Some(ref rpt) = reporting {
        let q = queue.lock().await;
        let all_logs = q.all_step_log_content();
        drop(q);
        if !all_logs.is_empty() {
            upload_job_log(rpt, &all_logs).await;
        }
    }

    // Report job completion — actually POST to the server
    if let Err(e) = report_completion(
        &job_message,
        &result_str,
        &job_ctx,
        &ordered_steps,
        via,
        reporting.as_deref(),
    )
    .await
    {
        error!("Failed to report job completion: {e:#}");
        return Err(e);
    }

    info!("Job {job_name} finished with result: {conclusion}");
    Ok(())
}

// ── Renew loop (F018) ────────────────────────────────────────────────

/// Spawn a background task that renews the job lock every 60 seconds.
///
/// Golden 06 flow 24: POST {run-service}/renewjob with `{planId, jobId}`.
/// Response contains `{lockedUntil: "..."}`. Interval is lock_duration/2
/// in the official runner; we use 60s as a safe default.
fn spawn_renew_loop(
    rpt: Arc<ReportingContext>,
    cancel_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await; // skip immediate first tick

        loop {
            interval.tick().await;

            if *cancel_rx.borrow() {
                info!("Renew loop: job cancelled, stopping");
                break;
            }

            let body = serde_json::json!({
                "planId": rpt.plan_id,
                "jobId": rpt.job_id,
            });

            match rpt.run_service.renew_job(&rpt.access_token, &body).await {
                Ok(resp) => {
                    let locked_until = resp
                        .get("lockedUntil")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    info!("Job lock renewed, lockedUntil={locked_until}");
                }
                Err(e) => {
                    warn!("renewjob failed: {e:#}");
                    // Don't abort on transient failure — keep trying
                }
            }
        }
    })
}

// ── Log upload (F020) ────────────────────────────────────────────────

/// Upload a single step's log content via signed blob URL.
///
/// Golden 06 flow 28-36: POST GetStepLogsSignedBlobURL → PUT blob.
pub async fn upload_step_log(rpt: &ReportingContext, step_id: &str, content: &str) {
    if content.is_empty() {
        return;
    }

    let body = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
        "step_backend_id": step_id,
    });

    let signed_url = match rpt
        .results
        .get_step_logs_signed_url(&rpt.access_token, &body)
        .await
    {
        Ok(resp) => resp
            .get("logs_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        Err(e) => {
            warn!("GetStepLogsSignedBlobURL failed for step {step_id}: {e:#}");
            return;
        }
    };

    if signed_url.is_empty() {
        warn!("Empty signed URL for step {step_id}");
        return;
    }

    match rpt
        .results
        .upload_log_blob(&signed_url, content.as_bytes().to_vec())
        .await
    {
        Ok(()) => info!("Uploaded log for step {step_id} ({} bytes)", content.len()),
        Err(e) => warn!("Log upload failed for step {step_id}: {e:#}"),
    }
}

/// Upload the full job log (concatenation of all step logs).
///
/// Golden 06 flow 37: POST GetJobLogsSignedBlobURL → PUT blob.
async fn upload_job_log(rpt: &ReportingContext, content: &str) {
    let body = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
    });

    let signed_url = match rpt
        .results
        .get_job_logs_signed_url(&rpt.access_token, &body)
        .await
    {
        Ok(resp) => resp
            .get("logs_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        Err(e) => {
            warn!("GetJobLogsSignedBlobURL failed: {e:#}");
            return;
        }
    };

    if signed_url.is_empty() {
        warn!("Empty signed URL for job log");
        return;
    }

    match rpt
        .results
        .upload_log_blob(&signed_url, content.as_bytes().to_vec())
        .await
    {
        Ok(()) => info!("Uploaded job log ({} bytes)", content.len()),
        Err(e) => warn!("Job log upload failed: {e:#}"),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Extract the run-service base URL and access token from the job message.
///
/// The job message's `resources.endpoints` contains a `SystemVssConnection`
/// endpoint with the URL and OAuth AccessToken for the run-service.
fn extract_service_endpoint(job_message: &serde_json::Value) -> Option<(String, String)> {
    let endpoints = job_message
        .get("resources")
        .and_then(|r| r.get("endpoints"))
        .and_then(|e| e.as_array())?;

    for ep in endpoints {
        let name = ep.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name == "SystemVssConnection" {
            let url = ep.get("url").and_then(|v| v.as_str())?.to_string();
            let token = ep
                .get("authorization")
                .and_then(|a| a.get("parameters"))
                .and_then(|p| p.get("AccessToken"))
                .and_then(|v| v.as_str())?
                .to_string();
            return Some((url.trim_end_matches('/').to_string(), token));
        }
    }
    None
}

/// Extract the results service URL from job message variables.
///
/// Golden 06: `system.github.results_endpoint` = `https://results-receiver.actions.githubusercontent.com/`
fn extract_results_url(job_message: &serde_json::Value) -> Option<String> {
    let vars = job_message.get("variables")?.as_object()?;
    let url = vars
        .get("system.github.results_endpoint")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())?;
    Some(url.trim_end_matches('/').to_string())
}

/// Build step results for the completejob body, including annotations (F025).
///
/// Golden 06 flow 41: each stepResult has `{external_id, number, name,
/// action_name, type, status, conclusion, started_at, completed_at, annotations}`.
/// Golden 14: annotations array has `{level, message, title, startLine, endLine, stepNumber}`.
fn build_completejob_step_results(
    ordered_steps: &[Step],
    job_ctx: &super::contexts::JobContext,
    step_annotations: &std::collections::HashMap<String, Vec<Annotation>>,
) -> Vec<serde_json::Value> {
    let now = chrono_now();
    let mut results = Vec::with_capacity(ordered_steps.len() + 2);

    // "Set up job" wrapper step
    results.push(serde_json::json!({
        "external_id": uuid::Uuid::new_v4().to_string(),
        "number": 1,
        "name": "Set up job",
        "action_name": "setup_job",
        "type": "runner",
        "status": "completed",
        "conclusion": "succeeded",
        "started_at": &now,
        "completed_at": &now,
        "annotations": [],
    }));

    for (idx, step) in ordered_steps.iter().enumerate() {
        let conclusion = job_ctx
            .steps
            .get(&step.id)
            .map(|result| runner_conclusion(&result.conclusion))
            .unwrap_or("skipped");

        let (step_type, action_name) = completejob_type_and_action(step);

        // F025: Include annotations for this step
        let step_number = (idx + 2) as u32;
        let annotations: Vec<serde_json::Value> = step_annotations
            .get(&step.id)
            .map(|anns| {
                anns.iter()
                    .map(|a| annotation_to_json(a, step_number))
                    .collect()
            })
            .unwrap_or_default();

        results.push(serde_json::json!({
            "external_id": step.id,
            "number": step_number,
            "name": step.display_name,
            "action_name": action_name,
            "type": step_type,
            "status": "completed",
            "conclusion": conclusion,
            "started_at": &now,
            "completed_at": &now,
            "annotations": annotations,
        }));
    }

    // "Complete job" wrapper step
    results.push(serde_json::json!({
        "external_id": uuid::Uuid::new_v4().to_string(),
        "number": ordered_steps.len() + 2,
        "name": "Complete job",
        "action_name": "complete_job",
        "type": "runner",
        "status": "completed",
        "conclusion": job_status_conclusion(job_ctx.job_status),
        "started_at": &now,
        "completed_at": &now,
        "annotations": [],
    }));

    results
}

/// Convert an Annotation to the golden 14 JSON shape.
fn annotation_to_json(ann: &Annotation, step_number: u32) -> serde_json::Value {
    use super::execution_context::AnnotationLevel;
    let level = match ann.level {
        AnnotationLevel::Notice => "notice",
        AnnotationLevel::Warning => "warning",
        AnnotationLevel::Error => "failure",
    };

    let mut obj = serde_json::json!({
        "level": level,
        "message": ann.message,
        "stepNumber": step_number,
    });

    if let Some(ref title) = ann.title {
        obj["title"] = serde_json::json!(title);
    }
    if let Some(line) = ann.line {
        obj["startLine"] = serde_json::json!(line);
        obj["endLine"] = serde_json::json!(ann.end_line.unwrap_or(line));
    }
    if let Some(col) = ann.col {
        obj["startColumn"] = serde_json::json!(col);
    }
    if let Some(end_col) = ann.end_column {
        obj["endColumn"] = serde_json::json!(end_col);
    }

    obj
}

fn completejob_type_and_action(step: &Step) -> (&'static str, String) {
    match &step.step_type {
        StepType::Script { shell, .. } => (
            "run",
            shell
                .as_deref()
                .and_then(|shell| shell.split_whitespace().next())
                .and_then(|shell| std::path::Path::new(shell).file_stem())
                .and_then(|stem| stem.to_str())
                .unwrap_or("sh")
                .to_string(),
        ),
        StepType::Action { uses, .. } => ("action", uses.clone()),
    }
}

fn runner_conclusion(conclusion: &str) -> &'static str {
    match conclusion.to_ascii_lowercase().as_str() {
        "success" | "succeeded" => "succeeded",
        "failure" | "failed" => "failed",
        "cancelled" | "canceled" => "cancelled",
        "skipped" => "skipped",
        _ => "failed",
    }
}

fn job_status_conclusion(status: super::contexts::JobStatus) -> &'static str {
    match status {
        super::contexts::JobStatus::Success => "succeeded",
        super::contexts::JobStatus::Failure => "failed",
        super::contexts::JobStatus::Cancelled => "cancelled",
    }
}

/// ISO 8601 timestamp for step timing (public so steps_runner can call it).
pub fn iso_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    time_to_iso8601(secs, millis)
}

/// ISO 8601 timestamp for step timing (private alias kept for local callers).
fn chrono_now() -> String {
    iso_now()
}

/// Convert unix timestamp to ISO 8601 string (UTC).
fn time_to_iso8601(secs: u64, millis: u32) -> String {
    // Simple UTC ISO 8601 formatter without chrono dependency
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Days since epoch to y/m/d (civil_from_days algorithm)
    let (y, m, d) = civil_from_days(days as i64);

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Report job completion to the server.
///
/// F013: Full completejob body matching golden flow 25/41:
/// `{planId, jobId, conclusion, outputs, stepResults, annotations, telemetry, billingOwnerId}`
async fn report_completion(
    job_message: &serde_json::Value,
    result: &str,
    job_ctx: &super::contexts::JobContext,
    ordered_steps: &[Step],
    via: ProtocolPath,
    reporting: Option<&ReportingContext>,
) -> Result<()> {
    let plan_id = job_message
        .get("plan")
        .and_then(|p| p.get("planId"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let billing_owner_id = job_message
        .get("billingOwnerId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Collect annotations from step contexts stored in the job context
    let step_annotations = job_ctx.step_annotations.clone();

    let step_results = build_completejob_step_results(ordered_steps, job_ctx, &step_annotations);

    let mut outputs = serde_json::Map::new();
    for (_, step) in &job_ctx.steps {
        for (k, v) in &step.outputs {
            outputs.insert(k.clone(), serde_json::json!(v));
        }
    }

    let completion_body = serde_json::json!({
        "planId": plan_id,
        "jobId": job_id,
        "conclusion": result.to_lowercase(),
        "outputs": outputs,
        "stepResults": step_results,
        "annotations": [],
        "telemetry": [],
        "billingOwnerId": billing_owner_id,
    });

    // Use reporting context if available, otherwise fall back to creating a new client
    if let Some(rpt) = reporting {
        match via {
            ProtocolPath::Broker => {
                let url = format!("{}/completejob", rpt.run_service.base_url());
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(
                        &url,
                        &completion_body,
                        &rpt.access_token,
                    )
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                let url = format!(
                    "{}/_apis/v1/plans/{plan_id}/events",
                    rpt.run_service.base_url()
                );
                let event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(&url, &event, &rpt.access_token)
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("FinishJob POST failed (non-fatal): {e:#}"),
                }
            }
        }
    } else if let Some((service_url, access_token)) = extract_service_endpoint(job_message) {
        let http = HttpClient::new(None)?;
        match via {
            ProtocolPath::Broker => {
                let url = format!("{service_url}/completejob");
                info!("Reporting completion to {url}");
                match http
                    .post_json_bearer::<serde_json::Value>(&url, &completion_body, &access_token)
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                let url = format!("{service_url}/_apis/v1/plans/{plan_id}/events");
                let event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                info!("Reporting completion to {url}");
                match http
                    .post_json_bearer::<serde_json::Value>(&url, &event, &access_token)
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("FinishJob POST failed (non-fatal): {e:#}"),
                }
            }
        }
    } else {
        warn!("No SystemVssConnection endpoint — cannot report completion");
        info!(
            "Job completion (unreported): planId={plan_id}, jobId={job_id}, result={result}, steps={}",
            step_results.len()
        );
    }

    Ok(())
}
