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
use tracing::{debug, error, info, warn};

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

    // Phase 2: Parse container/service specs from job message
    let raw_container = job_message.get("jobContainer");
    let raw_services = job_message.get("jobServiceContainers");
    info!(
        "Container fields: jobContainer={}, jobServiceContainers={}",
        raw_container
            .map(|v| v.to_string())
            .unwrap_or_else(|| "absent".to_string()),
        raw_services
            .map(|v| v.to_string())
            .unwrap_or_else(|| "absent".to_string()),
    );

    let job_container_spec = raw_container.and_then(super::container_ops::parse_container_spec);
    let service_specs = raw_services
        .map(super::container_ops::parse_service_specs)
        .unwrap_or_default();

    let has_containers = job_container_spec.is_some() || !service_specs.is_empty();
    if has_containers {
        info!(
            "Container job: container={}, services={}",
            job_container_spec.is_some(),
            service_specs.len()
        );
    }

    // Extract plan ID
    let plan_id = job_message
        .get("plan")
        .and_then(|p| p.get("planId"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let main_steps = super::job_extension::build_step_list(&steps, &job_message);

    // F022/F023: download remote actions before lifecycle discovery so pre/post
    // manifests are available and action execution uses SHA-pinned directories.
    let action_paths =
        prepare_remote_actions(&job_message, &workspace, &main_steps, &plan_id).await?;
    job_ctx.action_paths = action_paths.clone();

    // Build step list (F023: includes pre/post from downloaded manifests)
    let ordered_steps =
        super::job_extension::build_step_list_with_lifecycle(main_steps, &workspace, &action_paths);

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
    let renew_handle = reporting
        .as_ref()
        .map(|rpt| spawn_renew_loop(rpt.clone(), cancel_rx.clone()));

    let live_logs = if let Some(feed_url) = super::live_logs::extract_feed_stream_url(&job_message)
    {
        let token = reporting
            .as_ref()
            .map(|rpt| rpt.access_token.clone())
            .unwrap_or_default();
        Some(super::live_logs::LiveLogQueue::connect(feed_url, token).await)
    } else {
        None
    };
    let live_log_handle = live_logs.as_ref().map(|queue| queue.spawn_drain());
    job_ctx.live_logs = live_logs.clone();

    // Create the server queue for step status tracking
    let queue = Arc::new(Mutex::new(ServerQueue::new(
        job_id.to_string(),
        plan_id.clone(),
    )));

    // F031/P1.5: Job-level timeout enforcement.
    // For self-hosted runners on github.com, the server enforces `timeout-minutes`
    // and sends a cancellation message. The local timer is a safety net matching
    // the official runner's 360-minute default. If the job message ever carries
    // the timeout (e.g. `jobTimeout` or `plan.jobTimeoutInMinutes`), we'll use it.
    let job_timeout_minutes: u64 = job_message
        .get("jobTimeout")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            job_message
                .get("plan")
                .and_then(|p| p.get("jobTimeoutInMinutes"))
                .and_then(|v| v.as_u64())
        })
        .unwrap_or(360);

    info!("Job timeout: {job_timeout_minutes} minutes");

    // Instead of wrapping run_steps in tokio::time::timeout (which would drop
    // the future and orphan child processes — the bug F015 fixed), we create a
    // derived cancel channel that merges the parent cancel with a timeout timer.
    // When the timer fires, cancel_tx trips and process::invoke kills the
    // process group, then run_steps unwinds through normal cancel semantics.
    let (job_cancel_tx, job_cancel_rx) = watch::channel(false);
    let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Forward parent cancel → job cancel
    let fwd_tx = job_cancel_tx.clone();
    let mut fwd_rx = cancel_rx.clone();
    let forward_handle = tokio::spawn(async move {
        while fwd_rx.changed().await.is_ok() {
            if *fwd_rx.borrow() {
                let _ = fwd_tx.send(true);
                return;
            }
        }
    });

    // Spawn job-timeout timer that trips cancel and sets the timed_out flag
    let timeout_tx = job_cancel_tx.clone();
    let timeout_flag = timed_out.clone();
    let timeout_handle = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(job_timeout_minutes * 60)).await;
        warn!("Job timeout ({job_timeout_minutes} minutes) reached — cancelling");
        timeout_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = timeout_tx.send(true);
    });

    // Execute steps with the derived cancel channel
    let job_result = super::steps_runner::run_steps(
        &ordered_steps,
        &mut job_ctx,
        &workspace,
        job_cancel_rx,
        queue.clone(),
        reporting.as_deref(),
        job_container_spec.as_ref(),
        &service_specs,
    )
    .await;

    if let (Some(queue), Some(handle)) = (live_logs.as_ref(), live_log_handle) {
        queue.shutdown_and_wait(handle).await;
    }

    // Check if we timed out (must check before aborting the timer)
    let was_timeout = timed_out.load(std::sync::atomic::Ordering::SeqCst);

    // Clean up timer/forward tasks
    timeout_handle.abort();
    forward_handle.abort();

    // If the job timed out, override status to Failure with timeout message
    if was_timeout {
        let msg = format!(
            "Job {job_name} exceeded the maximum execution time of {job_timeout_minutes} minutes"
        );
        error!("{msg}");
        job_ctx.job_status = super::contexts::JobStatus::Failure;
        // F048: Add job-level annotation for timeout
        job_ctx.add_job_annotation(super::execution_context::Annotation {
            level: super::execution_context::AnnotationLevel::Error,
            message: msg,
            title: None,
            file: None,
            line: None,
            end_line: None,
            col: None,
            end_column: None,
        });
    }

    // F018: Stop renew loop
    if let Some(handle) = renew_handle {
        handle.abort();
    }

    let (result_str, conclusion) = if was_timeout {
        ("Failed".to_string(), "Failed".to_string())
    } else {
        match &job_result {
            Ok(conclusion) => {
                info!("Job {job_name} completed: {conclusion}");
                (conclusion.clone(), conclusion.clone())
            }
            Err(e) => {
                let msg = format!("Job {job_name} failed: {e:#}");
                error!("{msg}");
                // F048: Add job-level annotation for infrastructure failure
                job_ctx.add_job_annotation(super::execution_context::Annotation {
                    level: super::execution_context::AnnotationLevel::Error,
                    message: msg,
                    title: None,
                    file: None,
                    line: None,
                    end_line: None,
                    col: None,
                    end_column: None,
                });
                ("Failed".to_string(), "Failed".to_string())
            }
        }
    };

    // F019: Flush any final WorkflowStepsUpdate entries.
    if let Some(ref rpt) = reporting {
        flush_step_updates(rpt, &queue).await;
    }

    // F020: Upload job log (concatenation of all step logs)
    if let Some(ref rpt) = reporting {
        let mut q = queue.lock().await;
        let all_logs = q.all_step_log_content();
        drop(q);
        if !all_logs.is_empty() {
            upload_job_log(rpt, &all_logs).await;
        }
    }

    // F054: Upload diagnostic logs from _diag/ directory
    if let Some(ref rpt) = reporting {
        // Derive runner root from workspace: workspace is <root>/_work/<repo>/<dir>
        // so runner root is workspace/../../..
        let runner_root = std::path::Path::new(&workspace)
            .ancestors()
            .nth(3)
            .unwrap_or(std::path::Path::new("."));
        upload_diagnostic_logs(rpt, runner_root, job_name, &plan_id, job_id).await;
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

/// Spawn a background task that renews the job lock.
///
/// Golden 10 renews immediately after acquire and then continues before the
/// lease expires. We renew once immediately for parity, then every 60 seconds as
/// the fallback interval until `lockedUntil` parsing is made exact.
fn spawn_renew_loop(
    rpt: Arc<ReportingContext>,
    cancel_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cancel_rx = cancel_rx;
        let mut first_renew = true;
        loop {
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
                }
            }

            // Official runner probes service health after the first renewjob
            if first_renew {
                first_renew = false;
                let http = rpt.results.http();
                // Fire-and-forget health probes — matching official runner lifecycle
                let broker_health = format!(
                    "https://broker.actions.githubusercontent.com/health"
                );
                let run_health = format!(
                    "https://run.actions.githubusercontent.com/health"
                );
                let results_ws = format!(
                    "https://results-receiver.actions.githubusercontent.com/_ws/ingest.sock"
                );
                let token_ready = format!(
                    "https://token.actions.githubusercontent.com/ready"
                );
                // Probe in parallel, non-blocking
                let inner = http.inner_client();
                let _ = tokio::join!(
                    async { let _ = http.get_json::<serde_json::Value>(&broker_health).await; },
                    async { let _ = http.get_json::<serde_json::Value>(&run_health).await; },
                    async {
                        // WebSocket upgrade probe — official gets 101 Switching Protocols
                        let _ = inner.get(&results_ws)
                            .header("Upgrade", "websocket")
                            .header("Connection", "Upgrade")
                            .header("Sec-WebSocket-Version", "13")
                            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
                            .send()
                            .await;
                    },
                    async { let _ = http.get_json::<serde_json::Value>(&token_ready).await; },
                );
                info!("Service health probes completed");
            }

            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                changed = cancel_rx.changed() => {
                    if changed.is_err() || *cancel_rx.borrow() {
                        info!("Renew loop: cancellation channel closed/signaled, stopping");
                        break;
                    }
                }
            }
        }
    })
}

/// Flush queued WorkflowStepsUpdate entries without holding the queue lock across I/O.
pub async fn flush_step_updates(rpt: &ReportingContext, queue: &Arc<Mutex<ServerQueue>>) {
    let body = {
        let mut q = queue.lock().await;
        q.take_steps_update_body()
    };

    if let Some(body) = body {
        let body_json = serde_json::to_value(&body).unwrap_or_default();
        match rpt
            .results
            .update_workflow_steps(&rpt.access_token, &body_json)
            .await
        {
            Ok(_) => info!(
                "WorkflowStepsUpdate sent ({} steps, change_order={})",
                body.steps.len(),
                body.change_order
            ),
            Err(e) => warn!("WorkflowStepsUpdate failed (non-fatal): {e:#}"),
        }
    }
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
        Ok(()) => {
            info!("Uploaded log for step {step_id} ({} bytes)", content.len());
            // Official runner calls CreateStepLogsMetadata after each step log upload
            let metadata = serde_json::json!({
                "workflow_run_backend_id": rpt.plan_id,
                "workflow_job_run_backend_id": rpt.job_id,
                "step_backend_id": step_id,
                "uploaded_at": iso_now(),
                "line_count": content.lines().count(),
            });
            match rpt
                .results
                .create_step_logs_metadata(&rpt.access_token, &metadata)
                .await
            {
                Ok(_) => info!("CreateStepLogsMetadata succeeded for step {step_id}"),
                Err(e) => warn!("CreateStepLogsMetadata failed for step {step_id}: {e:#}"),
            }
        }
        Err(e) => warn!("Log upload failed for step {step_id}: {e:#}"),
    }
}

/// F035: Upload step summary content to the results service.
///
/// GetStepSummarySignedBlobURL → PUT blob → CreateStepSummaryMetadata.
/// Official runner rejects oversized summaries rather than truncating; we warn
/// and skip if content exceeds 1 MiB.
pub async fn upload_step_summary(rpt: &ReportingContext, step_id: &str, content: &str) {
    if content.is_empty() {
        return;
    }

    if content.len() > 1_048_576 {
        warn!("Step summary exceeds 1MiB limit for step {step_id}, skipping upload");
        return;
    }

    let body = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
        "step_backend_id": step_id,
    });

    let signed_url = match rpt
        .results
        .get_step_summary_signed_url(&rpt.access_token, &body)
        .await
    {
        Ok(resp) => resp
            .get("summary_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        Err(e) => {
            warn!("GetStepSummarySignedBlobURL failed for step {step_id}: {e:#}");
            return;
        }
    };

    if signed_url.is_empty() {
        warn!("Empty signed URL for step summary {step_id}");
        return;
    }

    let byte_count = content.len();
    match rpt
        .results
        .upload_log_blob(&signed_url, content.as_bytes().to_vec())
        .await
    {
        Ok(()) => info!("Uploaded summary for step {step_id} ({byte_count} bytes)"),
        Err(e) => {
            warn!("Summary upload failed for step {step_id}: {e:#}");
            return;
        }
    }

    // Finalize: CreateStepSummaryMetadata
    let metadata = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
        "step_backend_id": step_id,
        "size": byte_count,
        "uploaded_at": iso_now(),
    });
    match rpt
        .results
        .create_step_summary_metadata(&rpt.access_token, &metadata)
        .await
    {
        Ok(_) => info!("CreateStepSummaryMetadata succeeded for step {step_id}"),
        Err(e) => warn!("CreateStepSummaryMetadata failed for step {step_id}: {e:#}"),
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
        Ok(()) => {
            info!("Uploaded job log ({} bytes)", content.len());
            // Official runner calls CreateJobLogsMetadata after job log upload
            let metadata = serde_json::json!({
                "workflow_run_backend_id": rpt.plan_id,
                "workflow_job_run_backend_id": rpt.job_id,
                "uploaded_at": iso_now(),
                "line_count": content.lines().count(),
            });
            match rpt
                .results
                .create_job_logs_metadata(&rpt.access_token, &metadata)
                .await
            {
                Ok(_) => info!("CreateJobLogsMetadata succeeded"),
                Err(e) => warn!("CreateJobLogsMetadata failed: {e:#}"),
            }
        }
        Err(e) => warn!("Job log upload failed: {e:#}"),
    }
}

/// F054: Upload diagnostic logs from the _diag/ directory (if present).
///
/// Matches official `DiagnosticLogManager.UploadDiagnosticLogs()`:
/// - Collects log files from the runner's _diag/ directory
/// - Creates a zip archive with metadata
/// - Uploads via the results service
async fn upload_diagnostic_logs(
    rpt: &ReportingContext,
    runner_root: &std::path::Path,
    job_name: &str,
    plan_id: &str,
    job_id: &str,
) {
    let diag_dir = runner_root.join("_diag");
    if !diag_dir.is_dir() {
        debug!(
            "No _diag/ directory found at {} — skipping diagnostic log upload",
            diag_dir.display()
        );
        return;
    }

    // Collect log files
    let log_files: Vec<_> = match std::fs::read_dir(&diag_dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "log" || ext == "txt")
            })
            .collect(),
        Err(e) => {
            debug!("Failed to read _diag/ directory: {e}");
            return;
        }
    };

    if log_files.is_empty() {
        debug!("No log files in _diag/ — skipping diagnostic upload");
        return;
    }

    // Create a simple zip of all log files
    let zip_path = runner_root
        .join("_work")
        .join("_temp")
        .join(format!("{job_name}-diagnostics.zip"));
    if let Some(parent) = zip_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let zip_result = (|| -> Result<Vec<u8>> {
        use std::io::Write;
        let mut buf = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);

            for entry in &log_files {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(content) = std::fs::read(&entry.path()) {
                    zip.start_file(&name, options)?;
                    zip.write_all(&content)?;
                }
            }

            // Add metadata JSON
            let metadata = serde_json::json!({
                "jobName": job_name,
                "planId": plan_id,
                "jobId": job_id,
                "fileCount": log_files.len(),
            });
            zip.start_file("diagnostics-metadata.json", options)?;
            zip.write_all(serde_json::to_string_pretty(&metadata)?.as_bytes())?;
            zip.finish()?;
        }
        Ok(buf)
    })();

    let zip_content = match zip_result {
        Ok(content) => content,
        Err(e) => {
            warn!("Failed to create diagnostic zip: {e:#}");
            return;
        }
    };

    // Upload via results service
    let body = serde_json::json!({
        "workflow_run_backend_id": plan_id,
        "workflow_job_run_backend_id": job_id,
    });

    let signed_url = match rpt
        .results
        .get_diagnostic_logs_signed_url(&rpt.access_token, &body)
        .await
    {
        Ok(resp) => resp
            .get("blob_url")
            .or_else(|| resp.get("url"))
            .or_else(|| resp.get("logs_url"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        Err(e) => {
            debug!("Diagnostic log signed URL request failed (non-fatal): {e:#}");
            return;
        }
    };

    if signed_url.is_empty() {
        debug!("Empty signed URL for diagnostic logs — server may not support this feature");
        return;
    }

    match rpt.results.upload_log_blob(&signed_url, zip_content).await {
        Ok(()) => info!(
            "Uploaded diagnostic logs ({} files, {} bytes)",
            log_files.len(),
            zip_path.display()
        ),
        Err(e) => warn!("Diagnostic log upload failed: {e:#}"),
    }
}

async fn prepare_remote_actions(
    job_message: &serde_json::Value,
    workspace: &str,
    steps: &[Step],
    plan_id: &str,
) -> Result<std::collections::HashMap<String, String>> {
    let mut refs = Vec::new();
    for step in steps {
        let StepType::Action { uses, .. } = &step.step_type else {
            continue;
        };
        if uses.starts_with("./") || uses.starts_with("../") || uses.starts_with("docker://") {
            continue;
        }
        if let Some(parsed) = parse_remote_uses(uses) {
            refs.push((uses.clone(), parsed));
        } else {
            warn!("Cannot parse remote action ref (missing @version?): {uses:?}");
        }
    }

    if refs.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let access_token = extract_service_endpoint(job_message)
        .map(|(_, token)| token)
        .unwrap_or_default();
    let launch_url =
        message_variable(job_message, "system.github.launch_endpoint").map(str::to_string);

    let http = HttpClient::new(None)?;
    let resolver = crate::client::actions_download::ActionsResolveClient::new(http, launch_url);
    let action_pairs: Vec<(String, String)> = refs
        .iter()
        .map(|(_, parsed)| (parsed.action_name.clone(), parsed.git_ref.clone()))
        .collect();
    let action_pair_refs: Vec<(&str, &str)> = action_pairs
        .iter()
        .map(|(action, version)| (action.as_str(), version.as_str()))
        .collect();
    let resolved = if !access_token.is_empty() {
        resolver
            .resolve_batch(&access_token, plan_id, job_id, &action_pair_refs)
            .await?
    } else {
        std::collections::HashMap::new()
    };

    let actions_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_actions");
    let mut action_paths = std::collections::HashMap::new();

    for (uses, parsed) in refs {
        let key = format!("{}@{}", parsed.action_name, parsed.git_ref);
        let meta = resolved.get(&key);
        let dir_ref = meta
            .map(|m| m.resolved_sha.as_str())
            .filter(|sha| !sha.is_empty())
            .unwrap_or(parsed.git_ref.as_str());
        let download_url = meta
            .map(|m| m.tar_url.as_str())
            .filter(|url| !url.is_empty());
        let auth_token = meta.and_then(|m| m.auth_token.as_deref());

        let action_root = super::actions::manager::download_action(
            &parsed.owner,
            &parsed.repo,
            dir_ref,
            &actions_dir,
            download_url,
            auth_token,
        )
        .await?;

        let action_dir = if parsed.subpath.is_empty() {
            action_root
        } else {
            action_root.join(&parsed.subpath)
        };
        action_paths.insert(uses, action_dir.to_string_lossy().to_string());
    }

    Ok(action_paths)
}

struct ParsedUses {
    owner: String,
    repo: String,
    subpath: String,
    git_ref: String,
    action_name: String,
}

fn parse_remote_uses(uses: &str) -> Option<ParsedUses> {
    let (repo_part, git_ref) = uses.split_once('@')?;
    let mut parts = repo_part.split('/');
    let owner = parts.next()?.to_string();
    let repo = parts.next()?.to_string();
    let rest: Vec<&str> = parts.collect();
    let subpath = rest.join("/");
    Some(ParsedUses {
        owner: owner.clone(),
        repo: repo.clone(),
        subpath,
        git_ref: git_ref.to_string(),
        action_name: repo_part.to_string(),
    })
}

fn message_variable<'a>(job_message: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    job_message
        .get("variables")
        .and_then(|v| v.get(key))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
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

/// Extract the results service URL from endpoint data or job message variables.
///
/// Golden 06: `system.github.results_endpoint` = `https://results-receiver.actions.githubusercontent.com/`.
/// Current acquire payloads can also carry `resources.endpoints[].data.ResultsServiceUrl`.
fn extract_results_url(job_message: &serde_json::Value) -> Option<String> {
    if let Some(endpoints) = job_message
        .get("resources")
        .and_then(|r| r.get("endpoints"))
        .and_then(|e| e.as_array())
    {
        for ep in endpoints {
            let name = ep.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.eq_ignore_ascii_case("SystemVssConnection") {
                if let Some(url) = ep
                    .get("data")
                    .and_then(|d| d.get("ResultsServiceUrl"))
                    .and_then(|v| v.as_str())
                    .filter(|url| !url.is_empty())
                {
                    return Some(url.trim_end_matches('/').to_string());
                }
            }
        }
    }

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
        "external_id": job_ctx.setup_step_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
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
            .get(&step.context_name)
            .map(|result| runner_conclusion(&result.conclusion))
            .unwrap_or("skipped");

        let (step_type, action_name) = completejob_type_and_action(step);

        // F025: Include annotations for this step
        let step_number = (idx + 2) as u32;
        let annotations: Vec<serde_json::Value> = step_annotations
            .get(&step.context_name)
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
        "external_id": job_ctx.complete_step_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        "number": ordered_steps.len() + 2,
        "name": "Complete job",
        "action_name": "complete_job",
        "type": "runner",
        "status": "completed",
        "conclusion": "succeeded",
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

    // Golden 14 always includes startLine/endLine; default to 1 when the
    // annotation carries no source-file line info.
    let start_line = ann.line.unwrap_or(1);
    let end_line = ann.end_line.unwrap_or(start_line);

    let mut obj = serde_json::json!({
        "level": level,
        "message": ann.message,
        "stepNumber": step_number,
        "startLine": start_line,
        "endLine": end_line,
    });

    if let Some(title) = &ann.title {
        obj["title"] = serde_json::json!(title);
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

    // Official runner sends empty outputs in completejob — step outputs are
    // already available to downstream jobs via the results service.
    let outputs = serde_json::Map::new();

    // F048: Collect job-level annotations for completejob body.
    // These are infrastructure-level issues (container failures, action download errors)
    // not tied to a specific step. Step annotations are already in stepResults (F025).
    let job_annotations: Vec<serde_json::Value> = job_ctx
        .job_annotations
        .iter()
        .map(|a| annotation_to_json(a, 0))
        .collect();

    let completion_body = serde_json::json!({
        "planId": plan_id,
        "jobId": job_id,
        "conclusion": result.to_lowercase(),
        "outputs": outputs,
        "stepResults": step_results,
        "annotations": job_annotations,
        "telemetry": [{
            "type": "task",
            "message": format!("{{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"{}\"}}", result.to_lowercase()),
        }],
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::sync::watch;

    #[tokio::test]
    async fn test_run_job_executes_successfully() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-1",
            "jobDisplayName": "Mock Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Step One",
                    "run": "echo step-one-executed",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let (_tx, cancel_rx) = watch::channel(false);
        let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
        assert!(res.is_ok(), "Expected run_job to succeed, got: {:?}", res);
    }

    #[test]
    fn results_url_prefers_system_vss_endpoint_data() {
        let msg = serde_json::json!({
            "resources": {
                "endpoints": [{
                    "name": "SystemVssConnection",
                    "url": "http://127.0.0.1:9191/broker/1",
                    "data": {
                        "ResultsServiceUrl": "http://127.0.0.1:9191/"
                    }
                }]
            },
            "variables": {
                "system.github.results_endpoint": { "value": "http://wrong.example/" }
            }
        });

        assert_eq!(
            extract_results_url(&msg).as_deref(),
            Some("http://127.0.0.1:9191")
        );
    }

    #[tokio::test]
    async fn test_run_job_propagates_step_failure() {
        // When a step fails, run_job still returns Ok(()) because the failure
        // is propagated in the completion report, not the function return.
        // The worker process exits 0 and the server sees the Failed result.
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-fail",
            "jobDisplayName": "Failing Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Failing Step",
                    "run": "exit 1",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let (_tx, cancel_rx) = watch::channel(false);
        let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
        // run_job returns Ok even when steps fail — the failure result is
        // reported to the server via report_completion, not the return value.
        assert!(
            res.is_ok(),
            "Expected run_job to return Ok even with failing step, got: {:?}",
            res
        );
    }

    // --- JobRunnerL0 gap coverage ---

    #[tokio::test]
    async fn test_run_job_handles_cancelled() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-cancel",
            "jobDisplayName": "Cancel Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Long Step",
                    "run": "sleep 30",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let (cancel_tx, cancel_rx) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let _ = cancel_tx.send(true);
        });

        let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
        // run_job returns Ok — cancellation is reported via completion, not
        // the function return value.
        assert!(
            res.is_ok(),
            "Expected run_job to handle cancel gracefully, got: {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_run_job_with_timeout() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        // jobTimeout of 0 means the timeout fires immediately (0 * 60 = 0s),
        // triggering the cancel channel before the step can finish.
        let payload = serde_json::json!({
            "jobId": "job-timeout",
            "jobDisplayName": "Timeout Job",
            "plan": {"jobTimeoutInMinutes": 0},
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Long Step",
                    "run": "sleep 30",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let (_tx, cancel_rx) = watch::channel(false);
        let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
        assert!(
            res.is_ok(),
            "Expected run_job to handle timeout gracefully, got: {:?}",
            res
        );
    }
}
