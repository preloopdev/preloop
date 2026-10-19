//! Step, log, and diagnostic reporting helpers.

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::job_runner::ReportingContext;
use super::server_queue::ServerQueue;
use crate::client::results::UploadStage;
use crate::worker::helpers::iso_now;

/// Flush queued step-status updates to the server.
///
/// Broker path: WorkflowStepsUpdate Twirp call.
/// AzDO path: PATCH timeline records via `update_timeline`.
pub async fn flush_step_updates(rpt: &ReportingContext, queue: &Arc<Mutex<ServerQueue>>) {
    let pending = {
        let mut q = queue.lock().await;
        q.take_steps_update_body()
    };

    let Some((body, generation)) = pending else {
        return;
    };

    let published = if let Some(azdo) = &rpt.azdo {
        // translate StepUpdate → AzDO TimelineRecord and PATCH.
        let records: Vec<serde_json::Value> = body
            .steps
            .iter()
            .map(azdo_timeline_record_from_step_update)
            .collect();
        let count = records.len();
        let payload = serde_json::json!({ "count": count, "value": records });
        match azdo
            .client
            .update_timeline(&rpt.token(), &rpt.plan_id, &azdo.timeline_id, &payload)
            .await
        {
            Ok(_) => {
                info!(
                    "AzDO timeline updated ({} steps, change_order={})",
                    body.steps.len(),
                    body.change_order
                );
                true
            }
            Err(e) => {
                warn!("AzDO timeline update failed (non-fatal): {e:#}");
                false
            }
        }
    } else {
        let body_json = serde_json::to_value(&body).unwrap_or_default();
        match rpt
            .results
            .update_workflow_steps(&rpt.token(), &body_json)
            .await
        {
            Ok(_) => {
                info!(
                    "WorkflowStepsUpdate sent ({} steps, change_order={})",
                    body.steps.len(),
                    body.change_order
                );
                true
            }
            Err(e) => {
                warn!("WorkflowStepsUpdate failed (non-fatal): {e:#}");
                false
            }
        }
    };

    if published {
        queue.lock().await.mark_steps_published(generation);
    } else {
        // MC-S5: take_steps_update_body cleared dirty_keys, so a failed
        // publish would otherwise lose these transitions forever — a
        // terminal step never receives another queue_update. Re-dirty the
        // taken steps so the next flush retries them.
        queue.lock().await.requeue_steps(&body.steps);
    }
}

/// Convert a [`StepUpdate`] (Twirp-oriented) to an AzDO timeline record JSON value.
///
/// AzDO `TimelineRecordState`: 0=Pending, 1=InProgress, 2=Completed.
/// AzDO `TaskResult`: 0=Succeeded, 1=SucceededWithIssues, 2=Failed,
/// 3=Canceled, 4=Skipped, 5=Abandoned.
fn azdo_timeline_record_from_step_update(s: &super::server_queue::StepUpdate) -> serde_json::Value {
    use super::server_queue::{step_conclusion, step_status};

    // AzDO TimelineRecordState strings (TimelineRecordState.cs): "pending", "inProgress", "completed"
    let state_str = if s.status == step_status::COMPLETED {
        "completed"
    } else {
        "inProgress"
    };

    let mut record = serde_json::json!({
        "id":    s.external_id,
        "name":  s.name,
        "type":  "step",
        "order": s.number,
        "state": state_str,
        "percentComplete": if s.status == step_status::COMPLETED { 100_u32 } else { 0_u32 },
    });

    if let Some(ts) = &s.started_at {
        record["startTime"] = serde_json::json!(ts);
    }

    if s.status == step_status::COMPLETED {
        // AzDO TaskResult strings (TaskResult.cs): "succeeded", "succeededWithIssues",
        // "failed", "canceled", "skipped", "abandoned".
        let result_str = match s.conclusion {
            c if c == step_conclusion::SUCCEEDED => "succeeded",
            c if c == step_conclusion::FAILED => "failed",
            c if c == step_conclusion::SKIPPED => "skipped",
            _ => "failed",
        };
        record["result"] = serde_json::json!(result_str);
        if let Some(ts) = &s.completed_at {
            record["finishTime"] = serde_json::json!(ts);
        }
    }

    record
}

// ── Log upload ────────────────────────────────────────────────

/// Upload a single step's log content.
///
/// Broker path: `ResultsClient::upload_step_logs` (mint signed URL → PUT blob
/// → refresh expired URL and retry; no URL escapes that call).
/// AzDO path: POST create_log → PUT append_log → PATCH timeline log ref.
pub async fn upload_step_log(rpt: &ReportingContext, step_id: &str, content: &str) {
    if content.is_empty() {
        return;
    }

    if let Some(azdo) = &rpt.azdo {
        // AzDO: create a log entry, append content, then set the log ref on the
        // timeline record so GitHub can link the step record to its log.
        let log_body = serde_json::json!({
            "path": format!("logs/{step_id}"),
            "lineCount": content.lines().count(),
        });
        let log_id = match azdo
            .client
            .create_log(&rpt.token(), &rpt.plan_id, &log_body)
            .await
        {
            Ok(resp) => match resp.get("id").and_then(|v| v.as_i64()) {
                Some(id) => id,
                None => {
                    warn!("AzDO create_log response missing id for step {step_id}");
                    return;
                }
            },
            Err(e) => {
                warn!("AzDO create_log failed for step {step_id}: {e:#}");
                return;
            }
        };

        match azdo
            .client
            .append_log(
                &rpt.token(),
                &rpt.plan_id,
                log_id,
                content.as_bytes().to_vec(),
            )
            .await
        {
            Ok(()) => info!(
                "AzDO: uploaded log for step {step_id} (log_id={log_id}, {} bytes)",
                content.len()
            ),
            Err(e) => warn!("AzDO append_log failed for step {step_id}: {e:#}"),
        }

        // Patch the timeline record to attach the log reference.
        let log_ref_patch = serde_json::json!({
            "count": 1,
            "value": [{ "id": step_id, "log": { "id": log_id } }]
        });
        match azdo
            .client
            .update_timeline(
                &rpt.token(),
                &rpt.plan_id,
                &azdo.timeline_id,
                &log_ref_patch,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => warn!("AzDO timeline log-ref patch failed for step {step_id}: {e:#}"),
        }
        return;
    }

    // Broker path: content-in/receipt-out blob upload. Mint, PUT, and the
    // expired-URL refresh all stay inside the client — no signed URL is ever
    // held or rendered here.
    let body = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
        "step_backend_id": step_id,
    });

    match rpt
        .results
        .upload_step_logs(&rpt.token(), &body, content.as_bytes().to_vec())
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
                .create_step_logs_metadata(&rpt.token(), &metadata)
                .await
            {
                Ok(_) => info!("CreateStepLogsMetadata succeeded for step {step_id}"),
                Err(e) => warn!("CreateStepLogsMetadata failed for step {step_id}: {e:#}"),
            }
        }
        Err(e) => warn!("Log upload failed for step {step_id}: {e}"),
    }
}

/// Upload step summary content to the results service.
///
/// AzDO path: no-op — the AzDO protocol has no step summary equivalent.
/// Broker path: `ResultsClient::upload_step_summary` (mint → PUT → refresh
/// expired URL and retry) → CreateStepSummaryMetadata.
pub async fn upload_step_summary(rpt: &ReportingContext, step_id: &str, content: &str) {
    if content.is_empty() {
        return;
    }

    if rpt.azdo.is_some() {
        debug!("AzDO path: step summaries not supported, skipping for step {step_id}");
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

    let byte_count = content.len();
    match rpt
        .results
        .upload_step_summary(&rpt.token(), &body, content.as_bytes().to_vec())
        .await
    {
        Ok(()) => info!("Uploaded summary for step {step_id} ({byte_count} bytes)"),
        Err(e) => {
            warn!("Summary upload failed for step {step_id}: {e}");
            return;
        }
    }

    let metadata = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
        "step_backend_id": step_id,
        "size": byte_count,
        "uploaded_at": iso_now(),
    });
    match rpt
        .results
        .create_step_summary_metadata(&rpt.token(), &metadata)
        .await
    {
        Ok(_) => info!("CreateStepSummaryMetadata succeeded for step {step_id}"),
        Err(e) => warn!("CreateStepSummaryMetadata failed for step {step_id}: {e:#}"),
    }
}

/// Upload the full job log (concatenation of all step logs).
///
/// AzDO path: no-op — individual step logs are already uploaded via
/// `upload_step_log`; there is no separate job-log endpoint in the AzDO path.
/// Broker path: `ResultsClient::upload_job_logs` (mint → PUT → refresh
/// expired URL and retry) → CreateJobLogsMetadata.
pub(crate) async fn upload_job_log(rpt: &ReportingContext, content: &str) {
    if rpt.azdo.is_some() {
        debug!("AzDO path: skipping job log upload (step logs already uploaded individually)");
        return;
    }

    let body = serde_json::json!({
        "workflow_job_run_backend_id": rpt.job_id,
        "workflow_run_backend_id": rpt.plan_id,
    });

    match rpt
        .results
        .upload_job_logs(&rpt.token(), &body, content.as_bytes().to_vec())
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
                .create_job_logs_metadata(&rpt.token(), &metadata)
                .await
            {
                Ok(_) => info!("CreateJobLogsMetadata succeeded"),
                Err(e) => warn!("CreateJobLogsMetadata failed: {e:#}"),
            }
        }
        Err(e) => warn!("Job log upload failed: {e}"),
    }
}

/// Upload diagnostic logs from the _diag/ directory (if present).
///
/// Matches official `DiagnosticLogManager.UploadDiagnosticLogs()`:
/// - Collects log files from the runner's _diag/ directory
/// - Creates a zip archive with metadata
/// - Uploads via the results service
pub(crate) async fn upload_diagnostic_logs(
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
                if let Ok(content) = std::fs::read(entry.path()) {
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

    match rpt
        .results
        .upload_diag_logs(&rpt.token(), &body, zip_content)
        .await
    {
        Ok(()) => info!(
            "Uploaded diagnostic logs ({} files, {} bytes)",
            log_files.len(),
            zip_path.display()
        ),
        Err(e) if e.stage == UploadStage::Mint => {
            // The server may simply not support diagnostic log uploads.
            debug!("Diagnostic log signed URL request failed (non-fatal): {e}");
        }
        Err(e) => warn!("Diagnostic log upload failed: {e}"),
    }
}
