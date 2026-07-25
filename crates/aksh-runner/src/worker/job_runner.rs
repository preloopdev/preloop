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

use super::action_preparation::prepare_remote_actions;
use super::completion::{make_hook_step, report_completion};
use super::helpers::{extract_results_url, extract_service_endpoint, iso_now};
use super::reporting::{flush_step_updates, upload_diagnostic_logs, upload_job_log};
use super::server_queue::ServerQueue;
use crate::cli::ProtocolPath;
use crate::client::azdo::AzdoClient;
use crate::client::http::{HttpClient, HttpError};
use crate::client::results::ResultsClient;
use crate::client::run_service::RunServiceClient;
use anyhow::Result;
use chrono::{DateTime, TimeDelta, Utc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tracing::{error, info, warn};

/// AzDO-specific reporting state threaded into [`ReportingContext`] when
/// `--via azdo` is active.  Contains only what the timeline and log endpoints
/// need; `pool_id` is intentionally absent (not required for those URLs).
pub struct AzdoReportingContext {
    pub client: AzdoClient,
    pub timeline_id: String,
}

/// Shared reporting context for step updates and log uploads.
pub struct ReportingContext {
    pub results: ResultsClient,
    pub run_service: RunServiceClient,
    pub access_token: String,
    pub plan_id: String,
    pub job_id: String,
    /// Populated when running via the AzDO (legacy) protocol path.
    /// `None` on the broker path.
    pub azdo: Option<AzdoReportingContext>,
    /// Connectivity checks performed after the first lease renewal. The
    /// official runner includes these in completejob telemetry.
    pub connectivity_telemetry: Arc<Mutex<Vec<serde_json::Value>>>,
}

fn create_reporting_context(
    job_message: &serde_json::Value,
    via: ProtocolPath,
    plan_id: &str,
    job_id: &str,
) -> Result<Option<Arc<ReportingContext>>> {
    let Some((service_url, access_token)) = extract_service_endpoint(job_message) else {
        warn!("No SystemVssConnection endpoint — reporting disabled");
        return Ok(None);
    };
    let http = HttpClient::new(None)?;
    let results_url = extract_results_url(job_message).unwrap_or_else(|| service_url.clone());
    let azdo = if via == ProtocolPath::Azdo {
        let timeline_id = job_message
            .get("timeline")
            .and_then(|timeline| timeline.get("id"))
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_owned();
        if timeline_id.is_empty() {
            warn!("No timeline.id in AzDO job message — timeline reporting disabled");
            None
        } else {
            Some(AzdoReportingContext {
                client: AzdoClient::new(http.clone(), service_url.clone(), 0),
                timeline_id,
            })
        }
    } else {
        None
    };
    Ok(Some(Arc::new(ReportingContext {
        results: ResultsClient::new(http.clone(), results_url),
        run_service: RunServiceClient::new(http, service_url),
        access_token,
        plan_id: plan_id.to_owned(),
        job_id: job_id.to_owned(),
        azdo,
        connectivity_telemetry: Arc::new(Mutex::new(Vec::new())),
    })))
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
    let mut job_ctx = super::contexts::JobContext::new(
        job_id.to_string(),
        job_name.to_string(),
        variables,
        context_data,
    );
    let workspace = super::job_extension::setup_workspace(&job_message)?;
    job_ctx.workspace = Some(workspace.clone());
    super::job_extension::inject_github_env(&mut job_ctx, &job_message);
    // v2.336.0 (#4546/#4550): Announce locked dependencies in Setup Job log
    if let Some(deps) = job_message
        .get("actionsDependencies")
        .and_then(|v| v.as_array())
    {
        if !deps.is_empty() {
            info!("Using locked actions versions from the workflow's lockfile");
        }
    }
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
    let plan_id = job_message
        .get("plan")
        .and_then(|p| p.get("planId"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let reporting = create_reporting_context(&job_message, via, &plan_id, job_id)?;
    let main_steps = super::job_extension::build_step_list(&steps, &job_message);
    let action_paths =
        match prepare_remote_actions(&job_message, &workspace, &main_steps, &plan_id).await {
            Ok(paths) => paths,
            Err(error) => {
                error!("Set up job failed while preparing actions: {error:#}");
                job_ctx.job_status = super::contexts::JobStatus::Failure;
                report_completion(
                    &job_message,
                    "Failure",
                    &job_ctx,
                    &[],
                    via,
                    reporting.as_deref(),
                )
                .await?;
                return Ok(());
            }
        };
    job_ctx.action_paths = action_paths.clone();
    let mut ordered_steps =
        super::job_extension::build_step_list_with_lifecycle(main_steps, &workspace, &action_paths);
    if let Ok(hook) = std::env::var("ACTIONS_RUNNER_HOOK_JOB_STARTED") {
        if !hook.is_empty() {
            info!("Injecting ACTIONS_RUNNER_HOOK_JOB_STARTED: {hook}");
            ordered_steps.insert(
                0,
                make_hook_step("__hook_job_started", "__hook_job_started", &hook),
            );
        }
    }
    if let Ok(hook) = std::env::var("ACTIONS_RUNNER_HOOK_JOB_COMPLETED") {
        if !hook.is_empty() {
            info!("Injecting ACTIONS_RUNNER_HOOK_JOB_COMPLETED: {hook}");
            ordered_steps.push(make_hook_step(
                "__hook_job_completed",
                "__hook_job_completed",
                &hook,
            ));
        }
    }
    {
        let enable_debugger = job_message
            .get("enableDebugger")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if enable_debugger {
            let debugger_tunnel_json = job_message.get("debuggerTunnel").cloned();
            let debugger_welcome = job_message
                .get("debuggerWelcomeMessage")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let debugger_transport = aksh_dap::DebuggerTransportMode::from_wire(
                job_message
                    .get("akshDebugTransport")
                    .and_then(|v| v.as_str()),
            );
            let override_welcome = job_message
                .get("variables")
                .and_then(|v| v.get("actions_runner_override_debugger_welcome_message"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if let Some(tunnel_json) = debugger_tunnel_json {
                if let Ok(tunnel) =
                    serde_json::from_value::<aksh_gha_protocol::DebuggerTunnelInfo>(tunnel_json)
                {
                    let cfg = aksh_dap::DebuggerConfig::new_with_transport(
                        true,
                        Some(aksh_dap::DebuggerTunnelInfo {
                            tunnel_id: tunnel.tunnel_id,
                            cluster_id: tunnel.cluster_id,
                            host_token: tunnel.host_token,
                            port: tunnel.port,
                        }),
                        override_welcome,
                        debugger_welcome,
                        debugger_transport,
                    );
                    if cfg.is_runnable() {
                        let dbg = std::sync::Arc::new(aksh_dap::DapDebugger::new(cfg));
                        job_ctx.dap_debugger =
                            Some(dbg.clone() as std::sync::Arc<dyn aksh_dap::IDapDebugger>);
                    } else {
                        warn!(
                            "Debugger enabled but tunnel config is invalid \
                             — skipping DAP startup"
                        );
                    }
                }
            }
        }
    }
    if let Some(dbg) = job_ctx.dap_debugger.as_ref() {
        let entries: Vec<aksh_dap::SourceEntry> = ordered_steps
            .iter()
            .map(|s| {
                let is_pre = s.id.starts_with("__pre_")
                    || s.raw
                        .get("isPre")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                let is_post = s.id.starts_with("__post_")
                    || s.raw
                        .get("isPost")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                aksh_dap::SourceEntry {
                    display_name: s.display_name.clone(),
                    is_pre,
                    is_post,
                }
            })
            .collect();
        let post = vec![aksh_dap::SourceEntry {
            display_name: "Complete job".into(),
            is_pre: false,
            is_post: true,
        }];
        let predicted = vec![aksh_dap::PredictedPostStep {
            display_name: "Complete job".into(),
            frame_id: 1,
        }];
        dbg.on_job_steps_initialized(&entries, &post, &predicted)
            .await;
    }
    if let Some(rpt) = &reporting {
        if let Some(azdo) = &rpt.azdo {
            let job_record = serde_json::json!({
                "count": 1,
                "value": [{
                    "id": job_id,
                    "type": "job",
                    "name": job_name,
                    "order": 1,
                    "state": "inProgress",
                    "startTime": iso_now(),
                    "percentComplete": 0_u32,
                }]
            });
            match azdo
                .client
                .update_timeline(&rpt.access_token, &plan_id, &azdo.timeline_id, &job_record)
                .await
            {
                Ok(_) => info!("AzDO: job timeline record set to InProgress"),
                Err(e) => warn!("AzDO: job timeline InProgress failed (non-fatal): {e:#}"),
            }
        }
    }
    let live_logs = if let Some(feed_url) = super::live_logs::extract_feed_stream_url(&job_message)
    {
        let token = reporting
            .as_ref()
            .map(|rpt| rpt.access_token.clone())
            .unwrap_or_default();
        Some(super::live_logs::LiveLogQueue::connect(feed_url, token))
    } else {
        None
    };
    let live_log_handle = live_logs.as_ref().map(|queue| queue.spawn_drain());
    job_ctx.live_logs = live_logs.clone();
    let queue = Arc::new(Mutex::new(ServerQueue::new(
        job_id.to_string(),
        plan_id.clone(),
    )));
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
    // v2.336.0 (#4538): log effective cache mode when present
    if let Some(cache_mode) = job_ctx.env.get("ACTIONS_CACHE_MODE") {
        if !cache_mode.is_empty() {
            info!("Effective cache mode: {cache_mode}");
        }
    }
    let (job_cancel_tx, job_cancel_rx) = watch::channel(false);
    // Spawn periodic step-status drain (matches official runner's 500ms JobServerQueue interval)
    let drain_handle = reporting.as_ref().map(|rpt| {
        let drain_rpt = rpt.clone();
        let drain_queue = queue.clone();
        let mut drain_cancel = job_cancel_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let should_flush = drain_queue.lock().await.has_step_updates();
                        if should_flush {
                            flush_step_updates(&drain_rpt, &drain_queue).await;
                        }
                    }
                    changed = drain_cancel.changed() => {
                        if changed.is_err() || *drain_cancel.borrow() {
                            break;
                        }
                    }
                }
            }
        })
    });
    let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let lease_lost = Arc::new(AtomicBool::new(false));
    let renew_handle = reporting.as_ref().map(|rpt| {
        spawn_renew_loop(
            rpt.clone(),
            cancel_rx.clone(),
            job_cancel_tx.clone(),
            lease_lost.clone(),
        )
    });
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
    let mut debugger_result = Ok(());
    if let Some(dbg) = job_ctx.dap_debugger.clone() {
        info!("Starting debugger…");
        if let Err(e) = dbg.start(job_id, &[]).await {
            error!("DAP debugger failed to start: {e}");
            job_ctx.debugger_telemetry.push("Failed".to_string());
            debugger_result = Err(anyhow::anyhow!(
                "The debugger failed to start or no debugger client connected in time."
            ));
        } else {
            // Register the bound local port with the server
            if let Some(run_id_str) = job_message.get("akshDebugRunId").and_then(|v| v.as_str()) {
                if let Some((svc_url, token)) = extract_service_endpoint(&job_message) {
                    let port = dbg.local_port().unwrap_or(aksh_dap::DAP_TUNNEL_PORT);
                    let url = format!("{svc_url}/api/v1/runs/{run_id_str}/debug");
                    if let Ok(http) = HttpClient::new(None) {
                        let body = serde_json::json!({ "port": port, "job_id": job_id });
                        if let Err(e) = http
                            .post_json_bearer::<serde_json::Value>(&url, &body, &token)
                            .await
                        {
                            warn!("Failed to register DAP port with server: {e}");
                        }
                    }
                }
            }
            // Wait for client connection
            info!("Waiting for debugger client to connect…");
            let mut job_cancel = cancel_rx.clone();
            let wait_ready = dbg.wait_until_ready();
            tokio::select! {
                r = wait_ready => {
                    if let Err(e) = r {
                        error!("DAP debugger failed to connect: {e}");
                        job_ctx.debugger_telemetry.push("Failed".to_string());
                        let _ = dbg.stop().await;
                        debugger_result = Err(anyhow::anyhow!("The debugger failed to start or no debugger client connected in time."));
                    } else {
                        info!("Debugger connected.");
                        job_ctx.debugger_telemetry.push("Connected".to_string());
                    }
                }
                _ = job_cancel.changed() => {
                    if *job_cancel.borrow() {
                        error!("Job was cancelled before debugger client connected.");
                        job_ctx.debugger_telemetry.push("Canceled".to_string());
                        let _ = dbg.stop().await;
                        debugger_result = Err(anyhow::anyhow!("Job was cancelled before debugger client connected."));
                    }
                }
            }
        }
    }

    let job_result = if let Err(e) = debugger_result {
        Err(e)
    } else {
        super::steps_runner::run_steps(
            &ordered_steps,
            &mut job_ctx,
            &workspace,
            job_cancel_rx,
            queue.clone(),
            reporting.as_deref(),
            job_container_spec.as_ref(),
            &service_specs,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    };

    // Once execution has finished, completion wins over a concurrent renewal failure.
    if let Some(handle) = renew_handle {
        handle.abort();
    }

    if let (Some(queue), Some(handle)) = (live_logs.as_ref(), live_log_handle) {
        queue.shutdown_and_wait(handle).await;
    }

    // Kill any orphan child processes left over from the job steps.
    // This mirrors the official runner's FinalizeJob orphan-process cleanup.
    if let Some(tracking_id) = job_ctx.env.get("RUNNER_TRACKING_ID").cloned() {
        super::job_extension::kill_orphan_processes(&tracking_id);
    }

    // Check terminal causes before stopping their supporting tasks.
    let was_timeout = timed_out.load(Ordering::SeqCst);
    let was_lease_lost = lease_lost.load(Ordering::SeqCst);

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

    if was_lease_lost && !was_timeout {
        let msg = "Runner lost the server job lease".to_owned();
        error!("{msg}");
        job_ctx.job_status = super::contexts::JobStatus::Failure;
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

    let (result_str, conclusion) = if was_timeout || was_lease_lost {
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
    if let Some(handle) = drain_handle {
        handle.abort();
    }

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

    // DAP: OnJobCompleted — pause for debugger inspection.
    // Mirrors `JobExtension.cs` FinalizeJob block.
    if let Some(dbg) = job_ctx.dap_debugger.as_ref() {
        info!("Job completed — pausing for debugger inspection. Press continue to finish.");
        if let Err(e) = dbg.on_job_completed().await {
            warn!("DAP OnJobCompleted failed: {e}");
        }
        if let Err(e) = dbg.stop().await {
            warn!("DAP debugger stop failed: {e}");
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

    // Preloop: signal the orchestrator to hold this VM open for debugging.
    // Gated on the per-run opt-in carried in the job message, so preservation
    // is a property of the run rather than of the engine that happens to be up.
    // Cancellation is not a failure — preserving it would pin a pool slot on
    // every Ctrl-C.
    let preserve_requested = job_message
        .get("preloopPreserveOnFailure")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if preserve_requested && conclusion.eq_ignore_ascii_case("failed") {
        if let Some(path) = std::env::var_os("PRELOOP_FAILURE_MARKER") {
            let path = std::path::PathBuf::from(path);
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(error) = std::fs::write(&path, &conclusion) {
                warn!(path = %path.display(), %error, "failed to write Preloop failure marker");
            }
        }
    }

    info!("Job {job_name} finished with result: {conclusion}");
    Ok(())
}
fn spawn_renew_loop(
    rpt: Arc<ReportingContext>,
    cancel_rx: watch::Receiver<bool>,
    job_cancel_tx: watch::Sender<bool>,
    lease_lost: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut cancel_rx = cancel_rx;
        let mut first_renew = true;
        let mut lease_deadline = None;
        let mut failures = 0;
        loop {
            if *cancel_rx.borrow() {
                info!("Renew loop: job cancelled, stopping");
                break;
            }

            let body = serde_json::json!({
                "planId": rpt.plan_id,
                "jobId": rpt.job_id,
            });

            let delay = match rpt.run_service.renew_job(&rpt.access_token, &body).await {
                Ok(resp) => {
                    lease_deadline = resp
                        .get("lockedUntil")
                        .and_then(|value| value.as_str())
                        .and_then(parse_lease_deadline)
                        .or(lease_deadline);
                    failures = 0;
                    info!(locked_until = ?lease_deadline, "Job lock renewed");
                    Duration::from_secs(60)
                }
                Err(error) if is_job_not_found(&error) => {
                    error!("Job lease lost (404); stopping renewal");
                    lease_lost.store(true, Ordering::SeqCst);
                    let _ = job_cancel_tx.send(true);
                    break;
                }
                Err(error) => {
                    failures += 1;
                    warn!(failures, "renewjob failed: {error:#}");
                    let exhausted = lease_deadline
                        .is_some_and(|deadline| lease_expired(deadline, Utc::now()))
                        || (lease_deadline.is_none() && failures >= 5);
                    if exhausted {
                        error!("Job lease renewal retry window exhausted");
                        lease_lost.store(true, Ordering::SeqCst);
                        let _ = job_cancel_tx.send(true);
                        break;
                    }
                    renew_backoff(failures)
                }
            };

            // Official runner probes service health after the first renewjob
            if first_renew {
                first_renew = false;
                let http = rpt.results.http();
                // Fire-and-forget health probes — matching official runner lifecycle
                let broker_health =
                    "https://broker.actions.githubusercontent.com/health".to_string();
                let run_health = "https://run.actions.githubusercontent.com/health".to_string();
                let results_ws =
                    "https://results-receiver.actions.githubusercontent.com/_ws/ingest.sock"
                        .to_string();
                let token_ready = "https://token.actions.githubusercontent.com/ready".to_string();
                // Probe in parallel, non-blocking. The resulting status text
                // is also sent in completejob telemetry by the official
                // runner (the probes themselves are not step output).
                let (broker_result, run_result, ws_result, token_result) = tokio::join!(
                    async {
                        http.client_for(&broker_health)
                            .get(&broker_health)
                            .send()
                            .await
                    },
                    async { http.client_for(&run_health).get(&run_health).send().await },
                    async {
                        // WebSocket upgrade probe — matching official runner headers exactly.
                        // Official sends: Authorization, Connection: Upgrade, Upgrade: websocket,
                        // Sec-WebSocket-Key (random), Sec-WebSocket-Version: 13
                        use base64::Engine;
                        let mut nonce = [0u8; 16];
                        rand::Rng::fill(&mut rand::thread_rng(), &mut nonce);
                        let ws_key = base64::engine::general_purpose::STANDARD.encode(nonce);
                        http.client_for(&results_ws)
                            .get(&results_ws)
                            .header("Authorization", format!("Bearer {}", rpt.access_token))
                            .header("Connection", "Upgrade")
                            .header("Upgrade", "websocket")
                            .header("Sec-WebSocket-Version", "13")
                            .header("Sec-WebSocket-Key", ws_key)
                            .send()
                            .await
                    },
                    async { http.client_for(&token_ready).get(&token_ready).send().await },
                );
                let status_text = |result: &Result<reqwest::Response, reqwest::Error>| match result
                {
                    Ok(response) if response.status().as_u16() == 204 => "NoContent".to_owned(),
                    Ok(response) if response.status().is_success() => "OK".to_owned(),
                    Ok(response) => response.status().to_string(),
                    Err(_) => "Error".to_owned(),
                };
                let mut telemetry = rpt.connectivity_telemetry.lock().await;
                telemetry.extend([
                    serde_json::json!({
                        "type": "ConnectivityCheck",
                        "message": format!("{broker_health}: {}", status_text(&broker_result)),
                    }),
                    serde_json::json!({
                        "type": "ConnectivityCheck",
                        "message": format!("{token_ready}: {}", status_text(&token_result)),
                    }),
                    serde_json::json!({
                        "type": "ConnectivityCheck",
                        "message": format!("{run_health}: {}", status_text(&run_result)),
                    }),
                ]);
                drop(telemetry);
                let _ = ws_result;
                info!("Service health probes completed");
            }

            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
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

fn renew_backoff(attempt: u32) -> Duration {
    const DELAYS: [u64; 4] = [5, 10, 20, 30];
    let index = attempt.saturating_sub(1).min(DELAYS.len() as u32 - 1) as usize;
    Duration::from_secs(DELAYS[index])
}

fn parse_lease_deadline(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|deadline| deadline.with_timezone(&Utc))
}

fn lease_expired(locked_until: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    now > locked_until + TimeDelta::minutes(5)
}

fn is_job_not_found(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<HttpError>(),
        Some(HttpError::Status { status, .. }) if *status == reqwest::StatusCode::NOT_FOUND
    )
}
#[cfg(test)]
#[path = "job_runner_tests.rs"]
mod tests;
