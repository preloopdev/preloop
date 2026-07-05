//! Sequential step execution with condition evaluation.
//!
//! Each step is tracked through its lifecycle:
//!   InProgress → queue update → execute → Completed → queue update → upload log
//!
//! The `ReportingContext` from `job_runner` is threaded through so log upload
//! can happen right after each step completes (F019 + F020).

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{info, warn};

use super::contexts::{JobContext, JobStatus, StepResult};
use super::execution_context::StepContext;
use super::server_queue::{step_conclusion, step_status, ServerQueue, StepUpdate};

/// A step to execute, with its metadata.
///
/// F029: `id` is the wire GUID (used as `external_id` in step updates, `step_backend_id` in
/// log uploads). `context_name` is the human-readable key (e.g. `__run`, `__run_2`,
/// `__actions_checkout`, or the user's explicit `id:`) used in the `steps.<name>.outputs`
/// expression context, state, file commands, and `__pre_`/`__post_` naming.
#[derive(Debug, Clone)]
pub struct Step {
    /// Wire ID — GUID on live GitHub, context name on aksh-native payloads.
    pub id: String,
    /// Expression context key — `__run`, `__run_2`, user `id:`, etc.
    pub context_name: String,
    pub display_name: String,
    pub step_type: StepType,
    pub condition: Option<String>,
    pub continue_on_error: bool,
    pub timeout_minutes: Option<u64>,
    pub env: std::collections::HashMap<String, String>,
    pub raw: serde_json::Value,
}

/// What kind of step this is.
#[derive(Debug, Clone)]
pub enum StepType {
    /// Inline script (`run:`)
    Script {
        script: String,
        shell: Option<String>,
        working_directory: Option<String>,
    },
    /// Action reference (`uses:`)
    Action {
        uses: String,
        with: serde_json::Value,
    },
}

/// Run all steps sequentially, returning the job conclusion.
///
/// Watches `cancel_rx` — when it becomes `true`, the current step is abandoned
/// and remaining steps evaluate under `cancelled()` semantics.
///
/// F019: Queues WorkflowStepsUpdate for each step transition (InProgress + Completed).
/// F020: Uploads step logs right after each step completes.
pub async fn run_steps(
    steps: &[Step],
    job: &mut JobContext,
    workspace: &str,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    queue: Arc<Mutex<ServerQueue>>,
    reporting: Option<&crate::worker::job_runner::ReportingContext>,
    container_spec: Option<&super::container_ops::ContainerSpec>,
    service_specs: &[super::container_ops::ServiceSpec],
) -> Result<String> {
    let has_containers = container_spec.is_some() || !service_specs.is_empty();
    let mut any_failed = false;
    let mut cancelled = false;
    let now = crate::worker::job_runner::iso_now();

    // F019: Queue initial "Set up job" step as completed (number 1, official convention)
    {
        let mut q = queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: uuid::Uuid::new_v4().to_string(),
            number: 1,
            name: "Set up job".to_string(),
            status: step_status::COMPLETED,
            started_at: Some(now.clone()),
            completed_at: Some(now.clone()),
            conclusion: step_conclusion::SUCCEEDED,
        });
    }
    if let Some(rpt) = reporting {
        crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
    }

    // Phase 2: Initialize containers step (step 2 when containers present)
    let step_offset: u32 = if has_containers {
        let init_start = crate::worker::job_runner::iso_now();
        let init_step_id = uuid::Uuid::new_v4().to_string();
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: init_step_id.clone(),
                number: 2,
                name: "Initialize containers".to_string(),
                status: step_status::IN_PROGRESS,
                started_at: Some(init_start.clone()),
                completed_at: None,
                conclusion: 0,
            });
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
        }

        let init_result =
            initialize_containers(container_spec, service_specs, workspace, job).await;

        // Extract logs from the result (Ok holds logs, Err means init failed)
        let init_logs = match &init_result {
            Ok(logs) => logs.clone(),
            Err(_) => Vec::new(),
        };

        let init_end = crate::worker::job_runner::iso_now();
        let init_conclusion = if init_result.is_ok() {
            step_conclusion::SUCCEEDED
        } else {
            any_failed = true;
            job.job_status = JobStatus::Failure;
            step_conclusion::FAILED
        };
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: init_step_id.clone(),
                number: 2,
                name: "Initialize containers".to_string(),
                status: step_status::COMPLETED,
                started_at: Some(init_start),
                completed_at: Some(init_end),
                conclusion: init_conclusion,
            });
            // Attach init logs to synthetic step
            if !init_logs.is_empty() {
                q.record_step_logs(&init_step_id, init_logs.clone());
            }
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
            // Upload init container logs
            if !init_logs.is_empty() {
                let content = init_logs.join("\n");
                crate::worker::job_runner::upload_step_log(rpt, &init_step_id, &content).await;
            }
        }

        if init_result.is_err() {
            warn!("Container initialization failed: {:?}", init_result.err());
        }

        // User steps start at 3 (after Set up job + Initialize containers)
        3
    } else {
        // User steps start at 2 (after Set up job)
        2
    };

    for (idx, step) in steps.iter().enumerate() {
        let step_number = (idx as u32) + step_offset;

        let expr_ctx = job.build_expression_context();
        let resolved_display_name =
            crate::worker::template::evaluate_template(&step.display_name, &expr_ctx)
                .unwrap_or_else(|_| step.display_name.clone());
        // Check for cancellation
        if *cancel_rx.borrow() && !cancelled {
            cancelled = true;
            job.job_status = JobStatus::Cancelled;
            info!("Job cancelled — evaluating remaining steps under cancelled() semantics");
        }

        // Evaluate the step condition. Official runner treats expression
        // evaluation errors as step failures, not skips.
        match should_run_step(step, job) {
            Ok(true) => {}
            Ok(false) => {
                info!(
                    "Skipping step '{}' (condition not met)",
                    resolved_display_name
                );
                job.steps.insert(
                    step.context_name.clone(),
                    StepResult {
                        outcome: "Skipped".to_string(),
                        conclusion: "Skipped".to_string(),
                        outputs: std::collections::HashMap::new(),
                    },
                );
                // F019: Queue skipped step
                let ts = crate::worker::job_runner::iso_now();
                {
                    let mut q = queue.lock().await;
                    q.queue_update(StepUpdate {
                        external_id: step.id.clone(),
                        number: step_number,
                        name: resolved_display_name.clone(),
                        status: step_status::COMPLETED,
                        started_at: Some(ts.clone()),
                        completed_at: Some(ts),
                        conclusion: step_conclusion::SKIPPED,
                    });
                }
                if let Some(rpt) = reporting {
                    crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
                }
                continue;
            }
            Err(e) => {
                warn!(
                    "Condition evaluation failed for step '{}': {e:#}",
                    resolved_display_name
                );
                any_failed = true;
                job.job_status = JobStatus::Failure;
                job.steps.insert(
                    step.context_name.clone(),
                    StepResult {
                        outcome: "Failure".to_string(),
                        conclusion: "Failure".to_string(),
                        outputs: std::collections::HashMap::new(),
                    },
                );
                let ts = crate::worker::job_runner::iso_now();
                {
                    let mut q = queue.lock().await;
                    q.queue_update(StepUpdate {
                        external_id: step.id.clone(),
                        number: step_number,
                        name: resolved_display_name.clone(),
                        status: step_status::COMPLETED,
                        started_at: Some(ts.clone()),
                        completed_at: Some(ts),
                        conclusion: step_conclusion::FAILED,
                    });
                }
                if let Some(rpt) = reporting {
                    crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
                }
                continue;
            }
        }

        info!("Running step: {}", resolved_display_name);
        let step_start = crate::worker::job_runner::iso_now();

        // F019: Queue InProgress update
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: step.id.clone(),
                number: step_number,
                name: resolved_display_name.clone(),
                status: step_status::IN_PROGRESS,
                started_at: Some(step_start.clone()),
                completed_at: None,
                conclusion: 0,
            });
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
        }

        let mut step_ctx = StepContext::new(
            job,
            step.context_name.clone(),
            resolved_display_name.clone(),
        );
        {
            let expr_ctx = step_ctx.job.build_expression_context();
            for (k, v) in &step.env {
                let evaluated = crate::worker::template::evaluate_template(v, &expr_ctx)
                    .unwrap_or_else(|_| v.clone());
                step_ctx.env.insert(k.clone(), evaluated);
            }
        }

        let file_command_paths = {
            let temp_dir = std::path::Path::new(workspace)
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join("_temp");
            let paths = super::file_commands::create_file_commands(&temp_dir)?;
            for (k, v) in super::file_commands::file_command_env(&paths) {
                step_ctx.env.insert(k, v);
            }
            paths
        };
        step_ctx.update_debug_flag();

        // P2.2: Emit debug logs for condition evaluation
        let condition = step.condition.as_deref().unwrap_or("success()");
        step_ctx.debug(&format!(
            "Evaluating condition for step: '{}'",
            resolved_display_name
        ));
        step_ctx.debug(&format!("Evaluating: {condition}"));
        step_ctx.debug("Result: true");

        // P2.2: Emit debug logs for environment variables
        if step_ctx.debug {
            step_ctx.debug("Env:");
            let env = step_ctx.build_env();
            let mut keys: Vec<&String> = env.keys().collect();
            keys.sort();
            for k in keys {
                let v = env.get(k).unwrap();
                let masked_v = step_ctx.job.mask_secrets(v);
                step_ctx.debug(&format!("  {}: {}", k, masked_v));
            }
        }

        // P1.4: When we're in cancel-unwind mode (cancelled=true), steps that
        // still run (always/cancelled conditions) must NOT be immediately killed
        // by the still-active cancel channel. Give them a fresh cancel receiver
        // bounded by a grace budget (5 minutes, matching upstream's default
        // cancel timeout) so a hung always() step can still be killed.
        let step_cancel_rx = if cancelled {
            let (grace_tx, grace_rx) = watch::channel(false);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(300)).await;
                let _ = grace_tx.send(true);
            });
            grace_rx
        } else {
            cancel_rx.clone()
        };

        // Execute step — cancel_rx is threaded into process::invoke which
        // kills the process group on cancel. Step timeout is implemented by
        // signalling cancellation rather than wrapping the future in
        // tokio::time::timeout, which would drop the future and can orphan
        // child processes.
        let timed_out = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut timeout_handle = None;
        let exec_cancel_rx = if let Some(timeout_min) = step.timeout_minutes {
            let (timeout_tx, timeout_rx) = watch::channel(false);
            let mut base_rx = step_cancel_rx.clone();
            let timeout_flag = timed_out.clone();
            timeout_handle = Some(tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_min * 60)) => {
                        timeout_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        let _ = timeout_tx.send(true);
                    }
                    changed = base_rx.changed() => {
                        if changed.is_ok() && *base_rx.borrow() {
                            let _ = timeout_tx.send(true);
                        }
                    }
                }
            }));
            timeout_rx
        } else {
            step_cancel_rx
        };

        let mut outcome =
            execute_step(&step.step_type, &mut step_ctx, workspace, exec_cancel_rx).await;
        if let Some(handle) = timeout_handle {
            handle.abort();
        }
        if timed_out.load(std::sync::atomic::Ordering::SeqCst) {
            warn!(
                "Step '{}' timed out after {} minutes",
                resolved_display_name,
                step.timeout_minutes.unwrap_or_default()
            );
            step_ctx.log(&format!(
                "##[error]The step '{}' timed out",
                resolved_display_name
            ));
            outcome = Err(anyhow::anyhow!("step timed out"));
        }

        // Determine outcome and conclusion
        let (outcome_str, conclusion_str) = match &outcome {
            Ok(()) => ("Success".to_string(), "Success".to_string()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("cancelled") {
                    ("Cancelled".to_string(), "Cancelled".to_string())
                } else {
                    step_ctx.log(&format!("##[error]{e:#}"));
                    if step.continue_on_error {
                        warn!(
                            "Step '{}' failed but continue-on-error is set: {e:#}",
                            resolved_display_name
                        );
                        ("Failure".to_string(), "Success".to_string())
                    } else {
                        warn!("Step '{}' failed: {e:#}", resolved_display_name);
                        ("Failure".to_string(), "Failure".to_string())
                    }
                }
            }
        };

        let step_end = crate::worker::job_runner::iso_now();
        let conclusion_proto = ServerQueue::conclusion_to_proto(&conclusion_str);

        // Collect annotations before consuming step_ctx
        let annotations = step_ctx.annotations.clone();
        let log_lines = step_ctx.log_lines.clone();

        // F025: Store annotations in job context
        if !annotations.is_empty() {
            step_ctx
                .job
                .step_annotations
                .insert(step.context_name.clone(), annotations);
        }

        // Record a step result before applying file commands so GITHUB_OUTPUT can
        // attach outputs to this step.
        step_ctx.job.steps.insert(
            step.context_name.clone(),
            StepResult {
                outcome: outcome_str.clone(),
                conclusion: conclusion_str.clone(),
                outputs: std::collections::HashMap::new(),
            },
        );

        if let Err(e) = super::file_commands::apply_file_commands(
            &file_command_paths,
            &step.context_name,
            step_ctx.job,
        ) {
            warn!(
                "Applying file commands for step '{}' failed: {e:#}",
                resolved_display_name
            );
        }
        // F035: Read and scrub step summary content before cleanup deletes the file.
        let summary_content = std::fs::read_to_string(&file_command_paths.summary_file)
            .map(|content| step_ctx.job.mask_secrets(&content))
            .unwrap_or_default();
        super::file_commands::cleanup_file_commands(&file_command_paths);

        if conclusion_str == "Failure" {
            any_failed = true;
            step_ctx.job.job_status = JobStatus::Failure;
        } else if conclusion_str == "Cancelled" {
            cancelled = true;
            step_ctx.job.job_status = JobStatus::Cancelled;
        }

        // F019: Queue Completed update
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: step.id.clone(),
                number: step_number,
                name: resolved_display_name.clone(),
                status: step_status::COMPLETED,
                started_at: Some(step_start.clone()),
                completed_at: Some(step_end.clone()),
                conclusion: conclusion_proto,
            });
            // Record logs for job log assembly
            if !log_lines.is_empty() {
                q.record_step_logs(&step.id, log_lines.clone());
            }
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
        }

        // F020: Upload step log immediately after completion
        if let Some(rpt) = reporting {
            if !log_lines.is_empty() {
                let content = log_lines.join("\n");
                crate::worker::job_runner::upload_step_log(rpt, &step.id, &content).await;
            }
            // F035: Upload step summary if non-empty
            if !summary_content.is_empty() {
                crate::worker::job_runner::upload_step_summary(rpt, &step.id, &summary_content)
                    .await;
            }
        }
    }

    // Phase 2: Stop containers step (always runs, like post-job)
    let mut extra_steps = 0u32;
    if has_containers {
        extra_steps += 1;
        let stop_start = crate::worker::job_runner::iso_now();
        let stop_step_number = step_offset + steps.len() as u32;
        let stop_step_id = uuid::Uuid::new_v4().to_string();
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: stop_step_id.clone(),
                number: stop_step_number,
                name: "Stop containers".to_string(),
                status: step_status::IN_PROGRESS,
                started_at: Some(stop_start.clone()),
                completed_at: None,
                conclusion: 0,
            });
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
        }

        // Run cleanup
        let mut cleanup_log = Vec::new();
        if let Some(state) = &job.container_state {
            if let Err(e) = super::container_ops::cleanup_containers(state, &mut cleanup_log).await
            {
                warn!("Container cleanup failed: {e:#}");
            }
            for line in &cleanup_log {
                info!("{line}");
            }
        }

        let stop_end = crate::worker::job_runner::iso_now();
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: stop_step_id.clone(),
                number: stop_step_number,
                name: "Stop containers".to_string(),
                status: step_status::COMPLETED,
                started_at: Some(stop_start),
                completed_at: Some(stop_end),
                conclusion: step_conclusion::SUCCEEDED,
            });
            // Attach cleanup logs to synthetic step
            if !cleanup_log.is_empty() {
                q.record_step_logs(&stop_step_id, cleanup_log.clone());
            }
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
            // Upload cleanup logs
            if !cleanup_log.is_empty() {
                let content = cleanup_log.join("\n");
                crate::worker::job_runner::upload_step_log(rpt, &stop_step_id, &content).await;
            }
        }
    }

    // F019: Queue "Complete job" step
    let ts = crate::worker::job_runner::iso_now();
    // Step number: step_offset + user_steps + extra_steps (stop containers) + 1
    let complete_step_number = step_offset + steps.len() as u32 + extra_steps;
    let final_conclusion = if cancelled || any_failed {
        step_conclusion::FAILED
    } else {
        step_conclusion::SUCCEEDED
    };
    {
        let mut q = queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: uuid::Uuid::new_v4().to_string(),
            number: complete_step_number,
            name: "Complete job".to_string(),
            status: step_status::COMPLETED,
            started_at: Some(ts.clone()),
            completed_at: Some(ts),
            conclusion: final_conclusion,
        });
    }
    if let Some(rpt) = reporting {
        crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
    }

    Ok(if cancelled {
        "Cancelled".to_string()
    } else if any_failed {
        "Failed".to_string()
    } else {
        "Succeeded".to_string()
    })
}

fn should_run_step(step: &Step, job: &JobContext) -> Result<bool> {
    let condition = match &step.condition {
        Some(c) if !c.is_empty() => c.as_str(),
        _ => "success()",
    };

    let ctx = job.build_expression_context();
    match aksh_gha_expressions::eval_bool(condition, &ctx) {
        Ok(result) => Ok(result),
        Err(original) => {
            // Try stripping ${{ }} markers (conditions sometimes come pre-wrapped)
            let stripped = aksh_gha_expressions::trim_expression_markers(condition);
            if stripped == condition {
                Err(original.into())
            } else {
                aksh_gha_expressions::eval_bool(stripped, &ctx).map_err(Into::into)
            }
        }
    }
}

/// Execute a single step, threading cancel_rx to the process invoker.
///
/// When a job container is active, script steps are routed through `docker exec`.
async fn execute_step(
    step_type: &StepType,
    ctx: &mut StepContext<'_>,
    workspace: &str,
    cancel_rx: watch::Receiver<bool>,
) -> Result<()> {
    match step_type {
        StepType::Script {
            script,
            shell,
            working_directory,
        } => {
            // Evaluate ${{ }} expressions in the script body
            let expr_ctx = ctx.job.build_expression_context();
            let evaluated_script = crate::worker::template::evaluate_template(script, &expr_ctx)
                .unwrap_or_else(|_| script.clone());
            let step_working_directory = working_directory.as_deref().unwrap_or(workspace);

            // Phase 2: Route through docker exec when job container is active
            let container_id = ctx
                .job
                .container_state
                .as_ref()
                .and_then(|s| s.job_container_id.clone());
            if let Some(cid) = container_id {
                return super::handlers::script::run_script_in_container(
                    &evaluated_script,
                    shell.as_deref(),
                    step_working_directory,
                    &cid,
                    ctx,
                    Some(cancel_rx),
                )
                .await;
            }

            super::handlers::script::run_script(
                &evaluated_script,
                shell.as_deref(),
                step_working_directory,
                ctx,
                Some(cancel_rx),
            )
            .await
        }
        StepType::Action { uses, with } => {
            super::handlers::action::run_action(uses, with, workspace, ctx, cancel_rx).await
        }
    }
}

/// Initialize containers for a container job.
///
/// Matches golden trace sequence: check docker → cleanup stale → create network →
/// start job container → start service containers → wait for health checks.
async fn initialize_containers(
    container_spec: Option<&super::container_ops::ContainerSpec>,
    service_specs: &[super::container_ops::ServiceSpec],
    workspace: &str,
    job: &mut JobContext,
) -> Result<Vec<String>> {
    use super::container_ops::*;

    let mut log = Vec::new();

    // Check Docker availability
    if !check_docker(&mut log).await? {
        anyhow::bail!("Docker is not available");
    }

    let label = generate_label();
    let network = generate_network_name();

    // Clean up stale containers from previous runs
    cleanup_stale(&label, &mut log).await?;

    // Create job network
    create_network(&network, &label, &mut log).await?;

    // Derive workspace paths
    let runner_work = std::path::Path::new(workspace)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| workspace.to_string());
    let runner_temp = format!(
        "{}/_temp",
        std::path::Path::new(workspace)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_string_lossy()
    );
    let runner_externals = format!("{runner_work}/../externals");
    let runner_actions = format!(
        "{}/_actions",
        std::path::Path::new(workspace)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_string_lossy()
    );
    // Toolcache: use /opt/hostedtoolcache on Linux, or a local fallback
    let toolcache = if std::path::Path::new("/opt/hostedtoolcache").exists() {
        "/opt/hostedtoolcache".to_string()
    } else {
        format!("{runner_work}/_tool")
    };

    let mut job_container_id = None;
    let mut job_container_name = None;

    // Start job container if specified
    if let Some(spec) = container_spec {
        let name = container_name(&spec.image, &label);
        let id = start_job_container(
            spec,
            &name,
            &label,
            &network,
            workspace,
            &runner_work,
            &runner_temp,
            &runner_externals,
            &runner_actions,
            &toolcache,
            &mut log,
        )
        .await?;
        job_container_id = Some(id);
        job_container_name = Some(name);
    }

    // Start service containers
    let mut service_containers = Vec::new();
    for service in service_specs {
        let name = container_name(&service.image, &label);
        let id = start_service_container(service, &name, &label, &network, &mut log).await?;
        service_containers.push((service.alias.clone(), id, name));
    }

    // Wait for health checks
    if !service_containers.is_empty() {
        wait_for_services_healthy(&service_containers, &mut log).await?;
    }

    // Store container state in job context
    job.container_state = Some(ContainerState {
        label,
        network,
        job_container_id,
        job_container_name,
        service_containers,
    });

    // Populate job.container and job.services in context_data so
    // build_expression_context() can resolve ${{ job.container.id }},
    // ${{ job.services.<alias>.id }}, ${{ job.services.<alias>.ports['N'] }}, etc.
    if let Some(state) = &job.container_state {
        let mut job_ctx = job
            .context_data
            .get("job")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let job_obj = job_ctx.as_object_mut().unwrap();

        // job.container context
        if let Some(id) = &state.job_container_id {
            job_obj.insert(
                "container".to_string(),
                serde_json::json!({
                    "id": id,
                    "network": state.network,
                }),
            );
            job.env.insert("JOB_CONTAINER_ID".to_string(), id.clone());
            job.env
                .insert("JOB_CONTAINER_NETWORK".to_string(), state.network.clone());
        }

        // job.services context — each alias gets id, network, ports
        let mut services_ctx = serde_json::Map::new();
        for (alias, container_id, _) in &state.service_containers {
            let port_mappings = get_port_mappings(container_id).await;
            let mut ports_obj = serde_json::Map::new();
            for (container_port, host_port) in &port_mappings {
                ports_obj.insert(container_port.clone(), serde_json::json!(host_port));
            }
            services_ctx.insert(
                alias.clone(),
                serde_json::json!({
                    "id": container_id,
                    "network": state.network,
                    "ports": serde_json::Value::Object(ports_obj),
                }),
            );
        }
        if !services_ctx.is_empty() {
            job_obj.insert(
                "services".to_string(),
                serde_json::Value::Object(services_ctx),
            );
        }

        // Write back to context_data
        if let Some(cd) = job.context_data.as_object_mut() {
            cd.insert("job".to_string(), job_ctx);
        }
    }

    for line in &log {
        info!("{line}");
    }

    Ok(log)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::contexts::JobContext;
    use crate::worker::server_queue::ServerQueue;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::{watch, Mutex};

    fn test_step(name: &str, condition: Option<&str>) -> Step {
        Step {
            id: uuid::Uuid::new_v4().to_string(),
            context_name: name.to_string(),
            display_name: name.to_string(),
            step_type: StepType::Script {
                script: "echo should-not-run".to_string(),
                shell: Some("bash".to_string()),
                working_directory: None,
            },
            condition: condition.map(str::to_string),
            continue_on_error: false,
            timeout_minutes: None,
            env: std::collections::HashMap::new(),
            raw: serde_json::json!({}),
        }
    }

    #[test]
    fn condition_error_is_not_treated_as_skip() {
        let job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let step = test_step("broken", Some("${{"));

        let err = should_run_step(&step, &job).unwrap_err();

        assert!(!err.to_string().is_empty());
    }

    #[tokio::test]
    async fn run_steps_marks_condition_error_as_failure() {
        let dir = TempDir::new().unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let step = test_step("broken", Some("${{"));

        let result = run_steps(
            &[step],
            &mut job,
            dir.path().to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Failed");
        let step_result = job.steps.get("broken").unwrap();
        assert_eq!(step_result.outcome, "Failure");
        assert_eq!(step_result.conclusion, "Failure");
    }

    #[tokio::test]
    async fn run_steps_continue_on_error_sets_failure_outcome_success_conclusion() {
        let dir = TempDir::new().unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let mut step = test_step("soft_fail", None);
        step.step_type = StepType::Script {
            script: "exit 1".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
        step.continue_on_error = true;

        let result = run_steps(
            &[step],
            &mut job,
            dir.path().to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Succeeded");
        let step_result = job.steps.get("soft_fail").unwrap();
        assert_eq!(step_result.outcome, "Failure");
        assert_eq!(step_result.conclusion, "Success");
    }

    #[tokio::test]
    async fn run_steps_conditions_reflect_prior_failure() {
        let dir = TempDir::new().unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let mut fail = test_step("fail", None);
        fail.step_type = StepType::Script {
            script: "exit 1".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
        let success_after_failure = test_step("success_after_failure", Some("success()"));
        let mut failure_after_failure = test_step("failure_after_failure", Some("failure()"));
        failure_after_failure.step_type = StepType::Script {
            script: "echo failure-ran".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
        let mut always_after_failure = test_step("always_after_failure", Some("always()"));
        always_after_failure.step_type = StepType::Script {
            script: "echo always-ran".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

        let result = run_steps(
            &[
                fail,
                success_after_failure,
                failure_after_failure,
                always_after_failure,
            ],
            &mut job,
            dir.path().to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Failed");
        assert_eq!(
            job.steps.get("success_after_failure").unwrap().conclusion,
            "Skipped"
        );
        assert_eq!(
            job.steps.get("failure_after_failure").unwrap().conclusion,
            "Success"
        );
        assert_eq!(
            job.steps.get("always_after_failure").unwrap().conclusion,
            "Success"
        );
    }

    #[test]
    fn step_summary_content_uses_job_secret_masking() {
        let job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({"SECRET_TOKEN": {"value": "top-secret", "isSecret": true}}),
            serde_json::json!({}),
        );

        assert_eq!(job.mask_secrets("summary top-secret"), "summary ***");
    }

    #[tokio::test]
    async fn run_steps_outputs_are_visible_to_later_step_expressions() {
        let dir = TempDir::new().unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let mut produce = test_step("produce", None);
        produce.step_type = StepType::Script {
            script: "echo value=from-output >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
        let mut consume = test_step("consume", None);
        consume.step_type = StepType::Script {
            script: "echo seen=${{ steps.produce.outputs.value }}".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

        let result = run_steps(
            &[produce, consume],
            &mut job,
            dir.path().to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Succeeded");
        assert_eq!(
            job.steps
                .get("produce")
                .and_then(|step| step.outputs.get("value"))
                .map(String::as_str),
            Some("from-output")
        );
    }

    #[tokio::test]
    async fn run_steps_github_env_is_visible_to_later_steps() {
        let dir = TempDir::new().unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let mut set_env = test_step("set_env", None);
        set_env.step_type = StepType::Script {
            script: "echo FOO=bar >> \"$GITHUB_ENV\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
        let mut use_env = test_step("use_env", None);
        use_env.step_type = StepType::Script {
            script: "echo FOO=$FOO".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

        let result = run_steps(
            &[set_env, use_env],
            &mut job,
            dir.path().to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Succeeded");
        assert_eq!(job.env.get("FOO").map(String::as_str), Some("bar"));
    }

    #[tokio::test]
    async fn run_steps_honors_script_working_directory() {
        let dir = TempDir::new().unwrap();
        let workspace = dir.path().join("workspace");
        let subdir = workspace.join("subdir");
        std::fs::create_dir_all(&subdir).unwrap();
        let mut job = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
        let (_tx, cancel_rx) = watch::channel(false);
        let mut step = test_step("pwd", None);
        step.step_type = StepType::Script {
            script: "echo pwd=$(pwd) >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: Some(subdir.to_string_lossy().to_string()),
        };

        let result = run_steps(
            &[step],
            &mut job,
            workspace.to_str().unwrap(),
            cancel_rx,
            queue,
            None,
            None,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(result, "Succeeded");
        let actual = job
            .steps
            .get("pwd")
            .and_then(|step| step.outputs.get("pwd"))
            .map(std::path::PathBuf::from)
            .and_then(|path| path.canonicalize().ok());
        assert_eq!(
            actual.as_deref(),
            Some(subdir.canonicalize().unwrap().as_path())
        );
    }
}
