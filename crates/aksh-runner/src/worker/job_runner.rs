//! Job execution — the core worker loop.
//!
//! Receives an `AgentJobRequestMessage`, sets up the execution context,
//! runs steps, and reports results back to the server.

use anyhow::Result;
use tokio::sync::watch;
use tracing::{error, info, warn};

use super::steps_runner::{Step, StepType};
use crate::cli::ProtocolPath;
use crate::client::http::HttpClient;

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

    // Build step list
    let ordered_steps = super::job_extension::build_step_list(&steps, &job_message);

    // Execute steps with cancellation support
    let job_result =
        super::steps_runner::run_steps(&ordered_steps, &mut job_ctx, &workspace, cancel_rx).await;

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

    // Report job completion — actually POST to the server
    if let Err(e) =
        report_completion(&job_message, &result_str, &job_ctx, &ordered_steps, via).await
    {
        error!("Failed to report job completion: {e:#}");
        return Err(e);
    }

    info!("Job {job_name} finished with result: {conclusion}");
    Ok(())
}

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

fn build_completejob_step_results(
    ordered_steps: &[Step],
    job_ctx: &super::contexts::JobContext,
) -> Vec<serde_json::Value> {
    let now = "1970-01-01T00:00:00Z";
    let mut results = Vec::with_capacity(ordered_steps.len() + 2);

    results.push(serde_json::json!({
        "external_id": uuid::Uuid::new_v4().to_string(),
        "number": 1,
        "name": "Set up job",
        "action_name": "setup_job",
        "type": "runner",
        "status": "completed",
        "conclusion": "succeeded",
        "started_at": now,
        "completed_at": now,
        "annotations": [],
    }));

    for (idx, step) in ordered_steps.iter().enumerate() {
        let conclusion = job_ctx
            .steps
            .get(&step.id)
            .map(|result| runner_conclusion(&result.conclusion))
            .unwrap_or("skipped");

        let (step_type, action_name) = completejob_type_and_action(step);
        results.push(serde_json::json!({
            "external_id": step.id,
            "number": idx + 2,
            "name": step.display_name,
            "action_name": action_name,
            "type": step_type,
            "status": "completed",
            "conclusion": conclusion,
            "started_at": now,
            "completed_at": now,
            "annotations": [],
        }));
    }

    results.push(serde_json::json!({
        "external_id": uuid::Uuid::new_v4().to_string(),
        "number": ordered_steps.len() + 2,
        "name": "Complete job",
        "action_name": "complete_job",
        "type": "runner",
        "status": "completed",
        "conclusion": job_status_conclusion(job_ctx.job_status),
        "started_at": now,
        "completed_at": now,
        "annotations": [],
    }));

    results
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

/// Report job completion to the server.
///
/// F013: Full completejob body matching golden flow 25:
/// `{planId, jobId, conclusion, outputs, stepResults, annotations, telemetry, billingOwnerId}`
async fn report_completion(
    job_message: &serde_json::Value,
    result: &str,
    job_ctx: &super::contexts::JobContext,
    ordered_steps: &[Step],
    via: ProtocolPath,
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

    let step_results = build_completejob_step_results(ordered_steps, job_ctx);

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

    // Extract the run-service endpoint and token from the job message
    if let Some((service_url, access_token)) = extract_service_endpoint(job_message) {
        let http = HttpClient::new(None)?;

        match via {
            ProtocolPath::Broker => {
                // POST to {run-service}/completejob (golden flow 25)
                let url = format!("{service_url}/completejob");
                info!("Reporting completion to {url}");
                let resp = http
                    .post_json_bearer::<serde_json::Value>(&url, &completion_body, &access_token)
                    .await;
                match resp {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                // POST to FinishJob endpoint (legacy path)
                let url = format!("{service_url}/_apis/v1/plans/{plan_id}/events");
                let event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                info!("Reporting completion to {url}");
                let resp = http
                    .post_json_bearer::<serde_json::Value>(&url, &event, &access_token)
                    .await;
                match resp {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("FinishJob POST failed (non-fatal): {e:#}"),
                }
            }
        }
    } else {
        warn!("No SystemVssConnection endpoint in job message — cannot report completion");
        info!(
            "Job completion (unreported): planId={plan_id}, jobId={job_id}, result={result}, steps={}",
            step_results.len()
        );
    }

    Ok(())
}
