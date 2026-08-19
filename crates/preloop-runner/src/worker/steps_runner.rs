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
    /// Background steps run without DAP step-pauses, matching the official runner.
    pub is_background: bool,
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

/// Owns background step tasks until the main step loop reaches its implicit
/// wait-all boundary.  Background actions are deliberately detached from the
/// foreground step's mutable context, but their observable result is merged
/// back in one place after all tasks have been joined.
struct BackgroundStepCoordinator {
    semaphore: Arc<tokio::sync::Semaphore>,
    tasks: Vec<tokio::task::JoinHandle<BackgroundStepResult>>,
}

struct BackgroundStepResult {
    context_name: String,
    result: StepResult,
    step_id: String,
    logs: String,
    annotations: Vec<crate::worker::execution_types::Annotation>,
}

struct BackgroundStepStart {
    step: Step,
    workspace: String,
    cancel_rx: watch::Receiver<bool>,
    queue: Arc<Mutex<ServerQueue>>,
    step_number: u32,
    display_name: String,
}

impl BackgroundStepCoordinator {
    fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1))),
            tasks: Vec::new(),
        }
    }

    fn start(&mut self, job: &JobContext, start: BackgroundStepStart) {
        let permit = self.semaphore.clone();
        let mut bg_job = job.clone();
        self.tasks.push(tokio::spawn(async move {
            let _permit = permit.acquire_owned().await.expect("background semaphore");
            run_background_step(
                start.step,
                &mut bg_job,
                &start.workspace,
                start.cancel_rx,
                start.queue,
                start.step_number,
                start.display_name,
            )
            .await
        }));
    }

    async fn wait_all(&mut self) -> Vec<BackgroundStepResult> {
        let tasks = std::mem::take(&mut self.tasks);
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(result) => results.push(result),
                Err(error) => warn!("background step task terminated: {error}"),
            }
        }
        results
    }
}

/// Attempts a single step may be retried from a debug session before the
/// runner stops offering.
///
/// The attempt journal is cloned into every pause and retained by the control
/// plane, so an unbounded loop grows both sides without converging.
const MAX_DEBUG_ATTEMPTS: u32 = 25;

/// Run all steps sequentially, returning the job conclusion.
///
/// Watches `cancel_rx` — when it becomes `true`, the current step is abandoned
/// and remaining steps evaluate under `cancelled()` semantics.
///
/// F019: Queues WorkflowStepsUpdate for each step transition (InProgress + Completed).
/// F020: Uploads step logs right after each step completes.
#[allow(clippy::too_many_arguments)]
pub async fn run_steps(
    steps: &[Step],
    job: &mut JobContext,
    workspace: &str,
    cancel_rx: tokio::sync::watch::Receiver<bool>,
    queue: Arc<Mutex<ServerQueue>>,
    reporting: Option<&crate::worker::job_runner::ReportingContext>,
    container_spec: Option<&super::container_ops::ContainerSpec>,
    service_specs: &[super::container_ops::ServiceSpec],
    debug_client: Option<&super::debug_pause::DebugPauseClient>,
    snapshot_commit: Option<&str>,
) -> Result<String> {
    let mut steps = steps.to_vec();
    let total_steps = steps.len();
    let has_containers = container_spec.is_some() || !service_specs.is_empty();
    let mut any_failed = false;
    let mut init_failed = false;
    let mut cancelled = false;
    let mut background = BackgroundStepCoordinator::new(10);
    let now = crate::worker::helpers::iso_now();

    // F019: Queue initial "Set up job" step as completed (number 1, official convention)
    let setup_step_id = uuid::Uuid::new_v4().to_string();
    job.setup_step_id = Some(setup_step_id.clone());
    {
        let mut q = queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: setup_step_id.clone(),
            number: 1,
            name: "Set up job".to_string(),
            status: step_status::COMPLETED,
            started_at: Some(now.clone()),
            completed_at: Some(now.clone()),
            conclusion: step_conclusion::SUCCEEDED,
        });
    }

    // Build "Set up job" log content matching official runner
    {
        let runner_name = job.env.get("RUNNER_NAME").cloned().unwrap_or_else(|| {
            crate::settings::RunnerConfig::load(std::path::Path::new("."))
                .ok()
                .map(|c| c.settings.agent_name)
                .unwrap_or_else(|| "preloop-runner".to_string())
        });
        let machine_name = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());
        let ts = crate::worker::helpers::iso_now();

        let mut setup_lines = Vec::new();
        setup_lines.push(format!(
            "{ts} Current runner version: '{}'",
            crate::PROTOCOL_COMPAT_VERSION
        ));
        setup_lines.push(format!("{ts} Runner name: '{runner_name}'"));
        setup_lines.push(format!("{ts} Runner group name: 'Default'"));
        setup_lines.push(format!("{ts} Machine name: '{machine_name}'"));

        // GITHUB_TOKEN permissions are present in the GitHub context on local
        // submissions, while the GitHub control plane supplies the same data
        // through system.github.token.permissions.
        let token_permissions = job.github_context_value("token_permissions").or_else(|| {
            job.get_variable("system.github.token.permissions")
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        });
        if let Some(token_perms) = token_permissions {
            if let Some(perms) = token_perms.as_object() {
                setup_lines.push(format!("{ts} ##[group]GITHUB_TOKEN Permissions"));
                for (perm, level) in perms {
                    if let Some(level_str) = level.as_str() {
                        setup_lines.push(format!("{ts} {perm}: {level_str}"));
                    }
                }
                setup_lines.push(format!("{ts} ##[endgroup]"));
            }
        }

        setup_lines.push(format!("{ts} Secret source: Actions"));
        // The official runner records configured HTTP proxies in the setup
        // step. Reqwest reads these same environment variables for transport.
        for (scheme, names) in [
            ("HTTP", ["HTTP_PROXY", "http_proxy"]),
            ("HTTPS", ["HTTPS_PROXY", "https_proxy"]),
        ] {
            if let Some(proxy) = names.iter().find_map(|name| std::env::var(name).ok()) {
                setup_lines.push(format!(
                    "{ts} Runner is running behind proxy server '{proxy}' for all {scheme} requests."
                ));
            }
        }
        setup_lines.push(format!("{ts} Prepare workflow directory"));
        setup_lines.push(format!("{ts} Prepare all required actions"));
        setup_lines.push(format!("{ts} Complete job name: {}", job.job_name));

        let setup_content = setup_lines.join("\n");
        {
            let mut q = queue.lock().await;
            q.record_step_logs(&setup_step_id, &setup_content);
        }
        if let Some(rpt) = reporting {
            crate::worker::reporting::upload_step_log(rpt, &setup_step_id, &setup_content).await;
        }
    }

    // Phase 2: Initialize containers step (step 2 when containers present)
    let step_offset: u32 = if has_containers {
        let init_start = crate::worker::helpers::iso_now();
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

        // Logs are accumulated in place so a failure keeps everything up to
        // the point it broke. Returning them only on success left the user
        // with a red job, no step output, and the reason in a `warn!` that
        // never leaves the guest.
        let mut init_logs = Vec::new();
        let init_result = initialize_containers(
            container_spec,
            service_specs,
            workspace,
            job,
            &mut init_logs,
        )
        .await;
        if let Err(error) = &init_result {
            init_logs.push(format!("##[error]{error:#}"));
        }

        let init_end = crate::worker::helpers::iso_now();
        let init_conclusion = if init_result.is_ok() {
            step_conclusion::SUCCEEDED
        } else {
            any_failed = true;
            init_failed = true;
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
                q.record_step_logs(&init_step_id, &init_logs.join("\n"));
            }
        }
        if let Some(rpt) = reporting {
            // Upload init container logs
            if !init_logs.is_empty() {
                let content = init_logs.join("\n");
                crate::worker::reporting::upload_step_log(rpt, &init_step_id, &content).await;
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

    // Build compact step summaries so the debug session can present
    // `:retry --from` with human-readable names.
    let job_step_summaries: Vec<preloop_gha_protocol::debug_session::StepSummary> =
        if debug_client.is_some() {
            steps
                .iter()
                .enumerate()
                .map(|(i, s)| preloop_gha_protocol::debug_session::StepSummary {
                    index: i,
                    context_name: s.context_name.clone(),
                    display_name: s.display_name.clone(),
                })
                .collect()
        } else {
            Vec::new()
        };

    // Set once a controller aborts or the retry ceiling is hit: the job stops
    // offering to pause, so a later `always()` failure cannot re-trap a user
    // who already walked away.
    let mut debugging_declined = false;
    let mut step_idx = 0usize;
    let step_count = steps.len();
    // Per-step snapshots for `retry --from`: when replaying from an earlier
    // step, we need that step's pre-execution state, not the current step's.
    let mut step_snapshots: std::collections::HashMap<
        usize,
        super::debug_pause::StepStateSnapshot,
    > = std::collections::HashMap::new();
    // Persist attempt metadata across range replays so the retry ceiling and
    // journal survive a jump back to an earlier step.
    let mut step_attempt_state: std::collections::HashMap<
        usize,
        (
            u32,
            Vec<preloop_gha_protocol::debug_session::AttemptRecord>,
            String,
        ),
    > = std::collections::HashMap::new();
    'step_loop: while step_idx < steps.len() {
        let idx = step_idx;
        let step = &mut steps[step_idx];
        step_idx += 1;
        let step_number = (idx as u32) + step_offset;

        let expr_ctx = job.build_expression_context();
        let mut resolved_display_name = {
            let evaluated =
                crate::worker::template::evaluate_template(&step.display_name, &expr_ctx)
                    .unwrap_or_else(|_| step.display_name.clone());
            // The auto-generated display name for a script step is "Run <first-line-of-script>".
            // When the script contains ${{ }} expressions GHA encodes the entire script as a
            // single format() token, so the first line is a truncated format() expression that
            // evaluate_template cannot resolve. Detect that case and regenerate from the
            // fully-evaluated script content instead.
            if evaluated.contains("${{") {
                if let StepType::Script { script, .. } = &step.step_type {
                    let evaluated_script =
                        crate::worker::template::evaluate_template(script, &expr_ctx)
                            .unwrap_or_else(|_| script.clone());
                    crate::worker::job_extension::display_name_for_step(
                        &step.id,
                        &StepType::Script {
                            script: evaluated_script,
                            shell: None,
                            working_directory: None,
                        },
                    )
                } else {
                    evaluated
                }
            } else {
                evaluated
            }
        };
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
                let ts = crate::worker::helpers::iso_now();
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
                let ts = crate::worker::helpers::iso_now();
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
                continue;
            }
        }

        if step.is_background {
            info!("Starting background step: {}", resolved_display_name);
            let step_start = crate::worker::helpers::iso_now();
            {
                let mut q = queue.lock().await;
                q.queue_update(StepUpdate {
                    external_id: step.id.clone(),
                    number: step_number,
                    name: resolved_display_name.clone(),
                    status: step_status::IN_PROGRESS,
                    started_at: Some(step_start),
                    completed_at: None,
                    conclusion: 0,
                });
            }
            background.start(
                job,
                BackgroundStepStart {
                    step: step.clone(),
                    workspace: workspace.to_owned(),
                    cancel_rx: cancel_rx.clone(),
                    queue: queue.clone(),
                    step_number,
                    display_name: resolved_display_name,
                },
            );
            continue;
        }

        info!("Running step: {}", resolved_display_name);
        let step_start = crate::worker::helpers::iso_now();

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

        // Capture the DAP debugger reference before `StepContext::new`
        // takes a `&mut JobContext` borrow.
        let dap_debugger = job.dap_debugger.clone();

        let mut step_ctx = StepContext::new(
            job,
            step.context_name.clone(),
            resolved_display_name.clone(),
        );
        // The official runner fails the step when a step-env expression
        // cannot be evaluated (AssertString throws; StepsRunner marks the
        // step failed). Never silently keep a literal `${{ }}` in the
        // environment.
        let mut step_env_error: Option<anyhow::Error> = None;
        {
            let expr_ctx = step_ctx.job.build_expression_context();
            for (k, v) in &step.env {
                match crate::worker::template::evaluate_template_strict(v, &expr_ctx) {
                    Ok(evaluated) => {
                        step_ctx.env.insert(k.clone(), evaluated);
                    }
                    Err(error) => {
                        step_env_error
                            .get_or_insert_with(|| anyhow::anyhow!("step env '{k}' {error:#}"));
                    }
                }
            }
            // Workflow/job-level `env:` is pre-resolved by the server except
            // for runtime-only context keys (`github.workspace` — the server
            // has no runner work directory, and its resolver would zero the
            // property to ""). Evaluate any leftover templates now against
            // the full runtime context, like the official runner's
            // step-time env evaluation. Step env still overrides.
            for (k, v) in &step_ctx.job.env {
                if v.contains("${{") {
                    match crate::worker::template::evaluate_template_strict(v, &expr_ctx) {
                        Ok(evaluated) => {
                            step_ctx.env.entry(k.clone()).or_insert(evaluated);
                        }
                        Err(error) => {
                            step_env_error
                                .get_or_insert_with(|| anyhow::anyhow!("job env '{k}' {error:#}"));
                        }
                    }
                }
            }
        }

        // Snapshot the runner-managed state this step is allowed to mutate, so
        // a retry starts from the same logical position instead of appending to
        // a half-applied attempt. Captured before any file command runs.
        let step_state_snapshot = {
            let snap =
                super::debug_pause::StepStateSnapshot::capture(step_ctx.job, &step.context_name);
            if debug_client.is_some() {
                step_snapshots.insert(idx, snap.clone());
            }
            snap
        };
        // Restore persisted attempt state if we've been here before (range replay).
        let (mut attempt, mut attempt_journal, mut source_revision) = step_attempt_state
            .remove(&idx)
            .unwrap_or_else(|| (1, Vec::new(), "original".to_owned()));
        // Applied after the step is reported, not from inside the retry loop:
        // jumping straight out would skip the completion update, the log
        // upload and the file-command cleanup below, leaving the server with a
        // step stuck in progress.
        let mut jump_to: Option<usize> = None;
        // Snapshot checkout credential a retry verdict carried. Applied to the
        // current step before the replay and to every step of a jumped range
        // once the step borrow is released.
        let mut pending_snapshot_token: Option<String> = None;

        // Retry loop. Exactly one pass unless a debug controller says `retry`.
        let (conclusion_str, file_command_paths) = loop {
            let attempt_started = std::time::Instant::now();
            // Track where this attempt's log output starts so diagnostics
            // and exit-code extraction use only the current attempt's slice,
            // not cumulative output from prior retries.
            let attempt_log_offset = step_ctx.log_content().len();
            let attempt_annotation_offset = step_ctx.annotations.len();

            // Baseline for attributing workspace changes to *this* attempt.
            // Refreshed on every attempt so controller edits between retries
            // are not misattributed to the next attempt.
            let workspace_baseline = match (debug_client, snapshot_commit, step.is_background) {
                (Some(_), Some(commit), false) => {
                    match super::workspace_diff::diff_workspace_async(
                        std::path::PathBuf::from(workspace),
                        commit.to_owned(),
                    )
                    .await
                    {
                        Ok(diff) => Some(diff),
                        Err(error) => {
                            warn!(%error, "workspace baseline unavailable — retry will not offer a revert");
                            None
                        }
                    }
                }
                _ => None,
            };

            let (file_command_paths, file_command_init_error) = {
                let temp_dir = std::path::Path::new(workspace)
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join("_temp");
                // Official FileCommandManager.InitializeFiles + ArtifactsList PopulateInitialContents.
                match super::file_commands::create_file_commands_with_job(
                    &temp_dir,
                    Some(step_ctx.job),
                ) {
                    Ok(paths) => {
                        for (k, v) in super::file_commands::file_command_env(&paths) {
                            step_ctx.env.insert(k, v);
                        }
                        (paths, None)
                    }
                    Err(e) => {
                        // Route through normal failure path so DAP completion,
                        // log upload, and container cleanup still run.
                        step_ctx.log(&format!("##[error]File command setup failed: {e:#}"));
                        warn!(
                            "File command init failed for step '{}': {e:#}",
                            resolved_display_name
                        );
                        // Create minimal paths so cleanup is a no-op.
                        let temp_dir = std::path::Path::new(workspace)
                            .parent()
                            .unwrap_or(std::path::Path::new("."))
                            .join("_temp")
                            .join("_broken");
                        (
                            super::file_commands::FileCommandPaths {
                                env_file: temp_dir.join("e"),
                                path_file: temp_dir.join("p"),
                                output_file: temp_dir.join("o"),
                                state_file: temp_dir.join("s"),
                                summary_file: temp_dir.join("sm"),
                                artifacts_file: temp_dir.join("a"),
                                artifacts_list_file: temp_dir.join("al"),
                            },
                            Some(e),
                        )
                    }
                }
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

            // DAP: OnStepStarting — pause for debugger before step execution.
            // Mirrors StepsRunner.cs: `await dapDebugger?.OnStepStartingAsync(step);`
            if !step.is_background {
                if let Some(dbg) = dap_debugger.as_ref() {
                    let context_val = step_ctx.job.context_data.clone();
                    let masks: std::collections::HashSet<String> = step_ctx.job.masks.clone();
                    dbg.update_context(context_val, masks);
                    let is_pre = step.id.starts_with("__pre_")
                        || step
                            .raw
                            .get("isPre")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                    let is_post = step.id.starts_with("__post_")
                        || step
                            .raw
                            .get("isPost")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                    let source_entry = preloop_dap::SourceEntry {
                        display_name: resolved_display_name.clone(),
                        is_pre,
                        is_post,
                    };
                    if let Err(e) = dbg.on_step_starting(&source_entry).await {
                        warn!("DAP OnStepStarting failed: {e}");
                    }
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

            let mut outcome = match (file_command_init_error, step_env_error.take()) {
                (_, Some(error)) => Err(error),
                (Some(error), _) => Err(error),
                (None, None) => {
                    execute_step(&step.step_type, &mut step_ctx, workspace, exec_cancel_rx).await
                }
            };
            // F029: If the display name still contains unresolved expressions
            // after the pre-execution evaluation (e.g. `${{ needs.*.result }}`
            // or `${{ format(...) }}`), try to fix it now that the step has
            // executed and contexts may have been populated.
            if resolved_display_name.contains("${{") {
                if let Some(group_line) = step_ctx
                    .log_content()
                    .lines()
                    .find_map(|line| line.split_once("##[group]Run ").map(|(_, name)| name))
                {
                    resolved_display_name = format!("Run {group_line}");
                } else if let StepType::Script { script, .. } = &step.step_type {
                    let expr_ctx = step_ctx.job.build_expression_context();
                    if let Ok(evaluated_script) =
                        crate::worker::template::evaluate_template(script, &expr_ctx)
                    {
                        resolved_display_name = crate::worker::job_extension::display_name_for_step(
                            &step.id,
                            &StepType::Script {
                                script: evaluated_script,
                                shell: None,
                                working_directory: None,
                            },
                        );
                    }
                }
            }
            step.display_name = resolved_display_name.clone();
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

            // Determine initial outcome and conclusion from step execution.
            // If the step errored and the cancel channel fired mid-step, treat as
            // cancelled even when the process surface error is a non-zero exit from
            // SIGINT/SIGTERM. A successful step (Ok) is always "Success" so that
            // cleanup steps with `if: cancelled()` or `if: always()` are not
            // retroactively marked cancelled by a stale channel signal.
            let cancel_signaled = *cancel_rx.borrow();
            let (mut outcome_str, mut conclusion_str) = match &outcome {
                Ok(()) => ("Success".to_string(), "Success".to_string()),
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("cancelled") || msg.contains("canceled") || cancel_signaled {
                        // Match GitHub Actions hosted-runner step log wording exactly
                        // (American spelling "canceled").
                        step_ctx.log("##[error]The operation was canceled.");
                        cancelled = true;
                        step_ctx.job.job_status = JobStatus::Cancelled;
                        ("Cancelled".to_string(), "Cancelled".to_string())
                    } else {
                        // Script/container handlers already emit the official
                        // process-failure command before returning their error.
                        // Do not mirror that same `process exit code` as a second
                        // ##[error] line from the generic step wrapper.
                        if !msg.contains("process exit code") {
                            step_ctx.log(&format!("##[error]{e:#}"));
                        }
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

            // Record a step result before applying file commands so GITHUB_OUTPUT can
            // attach outputs to this step.  If file-command parsing fails, official
            // runner behavior is to mark the step failed after process execution.
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
                step_ctx.log("##[error]Unable to process file command successfully.");
                step_ctx.log(&format!("##[error]{e:#}"));
                if step.continue_on_error {
                    warn!(
                        "File commands for step '{}' failed but continue-on-error is set: {e:#}",
                        resolved_display_name
                    );
                    outcome_str = "Failure".to_string();
                    conclusion_str = "Success".to_string();
                } else {
                    warn!(
                        "Applying file commands for step '{}' failed: {e:#}",
                        resolved_display_name
                    );
                    outcome_str = "Failure".to_string();
                    conclusion_str = "Failure".to_string();
                }
                if let Some(step_result) = step_ctx.job.steps.get_mut(&step.context_name) {
                    step_result.outcome = outcome_str.clone();
                    step_result.conclusion = conclusion_str.clone();
                }
            }

            // Pause on failure. The worker stays alive and blocks here, which is
            // what keeps the microVM — and every service, package, and warm cache
            // inside it — available to whoever attaches.
            //
            // Only genuine failures pause. A cancelled step means someone already
            // decided the job is over, and background steps have no interactive
            // surface. `continue-on-error` steps pause too: the author tolerated
            // the failure, but the VM state at the moment it died is exactly
            // what debugging wants — the `continue` verdict preserves the
            // official outcome=Failure / conclusion=Success semantics with a
            // checkpoint in the middle.
            if should_pause_on_failure(
                &outcome_str,
                cancelled,
                step.is_background,
                debugging_declined,
            ) {
                if let Some(client) = debug_client.as_ref() {
                    let elapsed_ms = attempt_started.elapsed().as_millis() as u64;
                    // Use only this attempt's log/annotation slice so a retry
                    // does not report the prior attempt's exit code or errors.
                    let full_log = step_ctx.log_content();
                    let attempt_log = &full_log[attempt_log_offset..];
                    let attempt_annotations = &step_ctx.annotations[attempt_annotation_offset..];
                    let diagnostics =
                        super::debug_pause::diagnostics_from_annotations(attempt_annotations, 10);
                    let log_excerpt = if diagnostics.is_empty() {
                        super::debug_pause::log_excerpt(attempt_log, 20)
                    } else {
                        None
                    };
                    let exit_code = super::debug_pause::exit_code_from_log(attempt_log);

                    attempt_journal.push(preloop_gha_protocol::debug_session::AttemptRecord {
                        attempt,
                        outcome: outcome_str.clone(),
                        exit_code,
                        elapsed_ms,
                        source_revision: source_revision.clone(),
                    });

                    let failed_step = preloop_gha_protocol::debug_session::FailedStep {
                        index: idx,
                        total: total_steps,
                        context_name: step.context_name.clone(),
                        display_name: resolved_display_name.clone(),
                        command: match &step.step_type {
                            StepType::Script { script, .. } => Some(script.clone()),
                            StepType::Action { uses, .. } => Some(format!("uses: {uses}")),
                        },
                        working_directory: Some(workspace.to_owned()),
                        exit_code,
                        elapsed_ms,
                        diagnostics,
                        log_excerpt,
                    };

                    // What this attempt changed, as distinct from pre-existing dirt.
                    // Requires a baseline: without one there is no way to tell the
                    // two apart, and offering a revert anyway risks deleting work
                    // the step never produced.
                    let attempt_changes = match (&workspace_baseline, snapshot_commit) {
                        (Some(baseline), Some(commit)) => {
                            match super::workspace_diff::diff_workspace_async(
                                std::path::PathBuf::from(workspace),
                                commit.to_owned(),
                            )
                            .await
                            {
                                Ok(now) => super::workspace_diff::changes_since(baseline, &now),
                                Err(error) => {
                                    warn!(%error, "could not diff the workspace — no revert offered");
                                    Vec::new()
                                }
                            }
                        }
                        _ => Vec::new(),
                    };

                    // P1-3: Race the blocking pause against cancellation so a
                    // cancel arriving while the worker waits for a verdict
                    // does not hang the job/VM indefinitely.
                    let decision = {
                        let pause_fut = client.pause(
                            failed_step,
                            attempt_journal.clone(),
                            attempt_changes.clone(),
                            job_step_summaries.clone(),
                        );
                        let mut cancel_watch = cancel_rx.clone();
                        tokio::select! {
                            d = pause_fut => d,
                            _ = cancel_watch.changed() => {
                                if *cancel_watch.borrow() {
                                    warn!("Job cancelled while paused for debug verdict");
                                    step_ctx.log("##[error]The operation was canceled.");
                                    cancelled = true;
                                    step_ctx.job.job_status = JobStatus::Cancelled;
                                    break ("Cancelled".to_string(), file_command_paths);
                                }
                                // Spurious wake — pause already returned None
                                None
                            }
                        }
                    };

                    match decision.as_ref().map(|d| d.verdict) {
                        Some(preloop_gha_protocol::debug_session::Verdict::Retry)
                            if attempt >= MAX_DEBUG_ATTEMPTS =>
                        {
                            // The journal is cloned into every pause and retained
                            // server-side, so an unbounded retry loop grows both
                            // sides without ever converging. Stop offering.
                            warn!(
                                "Step '{}' has been retried {MAX_DEBUG_ATTEMPTS} times — \
                                 failing it and ending the debug session",
                                resolved_display_name
                            );
                            step_ctx.log(&format!(
                                "##[error]Retry limit ({MAX_DEBUG_ATTEMPTS}) reached for this step."
                            ));
                            debugging_declined = true;
                            break (conclusion_str, file_command_paths);
                        }
                        Some(preloop_gha_protocol::debug_session::Verdict::Retry) => {
                            // `retry_from_step` is a 0-based index into the user
                            // step list; absent means retry this step alone.
                            let target = decision.as_ref().and_then(|d| d.retry_from_step);

                            info!(
                                "Retrying step '{}' (attempt {})",
                                resolved_display_name,
                                attempt + 1
                            );
                            // Undo what the controller approved, before anything
                            // else. A leftover `build/` from the failed attempt
                            // makes the retry fail for a different reason than the
                            // original, which is worse than not retrying at all.
                            if let (Some(decision), Some(commit)) = (&decision, snapshot_commit) {
                                let selected = super::workspace_diff::select_for_policy(
                                    &attempt_changes,
                                    decision.revert,
                                );
                                if !selected.is_empty() {
                                    match super::workspace_diff::revert_paths_async(
                                        std::path::PathBuf::from(workspace),
                                        commit.to_owned(),
                                        selected,
                                    )
                                    .await
                                    {
                                        Ok(count) => {
                                            info!(
                                                "Reverted {count} path(s) from the failed attempt"
                                            );
                                            step_ctx.log(&format!(
                                                "##[group]Reverted {count} path(s) left by the failed attempt\n##[endgroup]"
                                            ));
                                        }
                                        Err(error) => {
                                            warn!(%error, "revert failed — retrying without it");
                                            step_ctx
                                                .log(&format!("##[warning]Revert failed: {error}"));
                                        }
                                    }
                                }
                            }
                            step_ctx.log(&format!(
                                "##[group]Retry attempt {} — {}",
                                attempt + 1,
                                resolved_display_name
                            ));
                            attempt += 1;
                            source_revision = decision
                                .as_ref()
                                .and_then(|d| d.source_revision.clone())
                                .unwrap_or_else(|| client.current_revision());
                            // The snapshot checkout token pinned at submission
                            // may be expired by now; the verdict carried a
                            // fresh one. Swap it in before the replay so the
                            // re-run does not fail with a git 401.
                            if let Some(token) =
                                decision.as_ref().and_then(|d| d.snapshot_token.as_deref())
                            {
                                client.refresh_snapshot_tokens(std::slice::from_mut(step), token);
                                pending_snapshot_token = Some(token.to_owned());
                            }

                            match target {
                                Some(target) if target <= idx && target < step_count => {
                                    // Report this attempt first, then replay the
                                    // range. Applied below, once the step's
                                    // completion has been recorded.
                                    jump_to = Some(target);
                                    // Persist attempt state so we pick up
                                    // where we left off after the jump.
                                    step_attempt_state.insert(
                                        idx,
                                        (attempt, attempt_journal.clone(), source_revision.clone()),
                                    );
                                    break (conclusion_str, file_command_paths);
                                }
                                Some(target) => {
                                    warn!(
                                        "retry_from_step {target} is not at or before current step {idx}, retrying current step"
                                    );
                                }
                                None => {}
                            }
                            // Retry just the current step.
                            super::file_commands::cleanup_file_commands(&file_command_paths);
                            step_state_snapshot.restore(step_ctx.job, &step.context_name);
                            continue;
                        }
                        Some(preloop_gha_protocol::debug_session::Verdict::Continue) => {
                            // The step still failed; the controller accepted it.
                            // Mirrors runtime `continue-on-error`, and is reported
                            // as such rather than laundered into a success.
                            warn!(
                                "Step '{}' failed but was continued interactively",
                                resolved_display_name
                            );
                            step_ctx.log("##[warning]Step failed but was continued interactively.");
                            if let Some(step_result) =
                                step_ctx.job.steps.get_mut(&step.context_name)
                            {
                                step_result.conclusion = "Success".to_string();
                            }
                            break ("Success".to_string(), file_command_paths);
                        }
                        Some(preloop_gha_protocol::debug_session::Verdict::Abort) => {
                            // The step keeps its failure and the job unwinds
                            // normally, so `always()` cleanup still runs and
                            // containers still stop. What abort adds is a promise
                            // not to ask again: without it a later `always()`
                            // step failing would pause a job the user already
                            // walked away from.
                            info!("Debug session aborted — failing the job without pausing again");
                            step_ctx.log(
                                "##[error]Debugging aborted. The job fails from here; \
                                 cleanup steps still run.",
                            );
                            debugging_declined = true;
                            // An aborted `continue-on-error` step keeps its
                            // failure: the verdict promises "the job fails
                            // from here", and the tolerated conclusion would
                            // leave `any_failed` false and the job green.
                            // Flip the recorded conclusion so the aggregation
                            // and the step report agree.
                            if outcome_str == "Failure" {
                                if let Some(step_result) =
                                    step_ctx.job.steps.get_mut(&step.context_name)
                                {
                                    step_result.conclusion = "Failure".to_string();
                                }
                                break ("Failure".to_string(), file_command_paths);
                            }
                            break (conclusion_str, file_command_paths);
                        }
                        // No session, or the session vanished. Neither is a
                        // decision: fall through and fail normally.
                        None => break (conclusion_str, file_command_paths),
                    }
                }
            }

            break (conclusion_str, file_command_paths);
        };

        // DAP: OnStepCompleted — emit `continued` if we paused.
        // Mirrors StepsRunner.cs: `dapDebugger?.OnStepCompleted(step);`
        if !step.is_background {
            if let Some(dbg) = dap_debugger.as_ref() {
                let is_pre = step.id.starts_with("__pre_")
                    || step
                        .raw
                        .get("isPre")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                let is_post = step.id.starts_with("__post_")
                    || step
                        .raw
                        .get("isPost")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                let source_entry = preloop_dap::SourceEntry {
                    display_name: resolved_display_name.clone(),
                    is_pre,
                    is_post,
                };
                dbg.on_step_completed(&source_entry);
            }
        }

        let step_end = crate::worker::helpers::iso_now();
        let conclusion_proto = ServerQueue::conclusion_to_proto(&conclusion_str);
        // F035: Read and scrub step summary content before cleanup deletes the file.
        let summary_content = if let Ok(metadata) =
            std::fs::metadata(&file_command_paths.summary_file)
        {
            let file_size = metadata.len();
            if file_size == 0 {
                "".to_string()
            } else if file_size > 1_048_576 {
                let limit_k = 1024;
                let size_k = file_size / 1024;
                let msg = format!(
                    "$GITHUB_STEP_SUMMARY upload aborted, supports content up to a size of {}k, got {}k. For more information see: https://docs.github.com/actions/using-workflows/workflow-commands-for-github-actions#adding-a-markdown-summary",
                    limit_k, size_k
                );
                step_ctx.annotate(crate::worker::execution_context::Annotation {
                    level: crate::worker::execution_context::AnnotationLevel::Error,
                    message: msg.clone(),
                    title: None,
                    file: None,
                    line: None,
                    end_line: None,
                    col: None,
                    end_column: None,
                });
                warn!("{msg}");
                "".to_string()
            } else {
                std::fs::read_to_string(&file_command_paths.summary_file)
                    .map(|content| step_ctx.job.mask_secrets(&content))
                    .unwrap_or_default()
            }
        } else {
            "".to_string()
        };
        super::file_commands::cleanup_file_commands(&file_command_paths);
        // Collect annotations after all processing (including step summary validation)
        let annotations = step_ctx.annotations.clone();
        let log_content = step_ctx.log_content();

        // F025: Store annotations in job context
        if !annotations.is_empty() {
            step_ctx.job.add_step_annotations_to_job(&annotations);
            step_ctx
                .job
                .step_annotations
                .insert(step.context_name.clone(), annotations);
        }

        // Official StepsRunner only folds Failed from the main loop into the job
        // result. Background steps run off-loop and are aggregated later; Cancelled
        // from a background step must not flip the job to Cancelled on its own
        // (#4482: only Failed from bg steps merges; explicit-cancel Canceled is
        // excluded). Without a BackgroundStepCoordinator, treat is_background
        // Cancelled as non-influencing (job cancel already sets `cancelled` via
        // cancel_rx). Failures always count.
        if conclusion_str == "Failure" {
            any_failed = true;
            step_ctx.job.job_status = JobStatus::Failure;
        } else if conclusion_str == "Cancelled" && !step.is_background {
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
            if !log_content.is_empty() {
                q.record_step_logs(&step.id, &log_content);
            }
        }

        // F020: Upload step log immediately after completion
        if let Some(rpt) = reporting {
            if !log_content.is_empty() {
                crate::worker::reporting::upload_step_log(rpt, &step.id, &log_content).await;
            }
            // F035: Upload step summary if non-empty
            if !summary_content.is_empty() {
                crate::worker::reporting::upload_step_summary(rpt, &step.id, &summary_content)
                    .await;
            }
        }

        // Replay an earlier range, now that this attempt has been reported.
        if let Some(target) = jump_to {
            let context_name = step.context_name.clone();
            // A jumped range may include other pinned checkout steps whose
            // submission-time credential expired while the job waited. Swap
            // in the verdict's replacement for all of them before the replay.
            if let Some(token) = pending_snapshot_token.as_deref() {
                if let Some(client) = debug_client.as_ref() {
                    client.refresh_snapshot_tokens(&mut steps, token);
                }
            }
            // Clear every runner-managed per-step value for the range about to
            // re-run. Restoring only the target snapshot leaves saveState and
            // annotations from later steps visible during their second pass.
            clear_replayed_step_state(step_ctx.job, &job_step_summaries, target, idx);
            // Restore the snapshot captured before the *target* step, not
            // the current step. Without this, the target step's prior
            // GITHUB_ENV/GITHUB_PATH/state writes remain and are doubled.
            if let Some(target_snapshot) = step_snapshots.get(&target) {
                target_snapshot.restore(step_ctx.job, &steps[target].context_name);
            } else {
                step_state_snapshot.restore(step_ctx.job, &context_name);
            }
            // Recompute from surviving step results. init_failed is tracked
            // separately so container-init failures are never lost.
            any_failed = init_failed
                || step_ctx
                    .job
                    .steps
                    .values()
                    .any(|result| result.conclusion == "Failure");
            step_ctx.job.job_status = if any_failed {
                JobStatus::Failure
            } else {
                JobStatus::Success
            };
            info!(
                "Jumping back to step {} ('{}')",
                target + 1,
                steps[target].display_name
            );
            step_idx = target;
            continue 'step_loop;
        }
    }

    // The official runner waits for every background action before post-job
    // actions and before publishing the terminal job result.  Joining here is
    // also the shutdown guarantee: no process task survives run_steps.
    for result in background.wait_all().await {
        if result.result.conclusion == "Failure" {
            any_failed = true;
            job.job_status = JobStatus::Failure;
        }
        if result.result.conclusion == "Cancelled" && *cancel_rx.borrow() {
            cancelled = true;
            job.job_status = JobStatus::Cancelled;
        }
        job.steps
            .insert(result.context_name.clone(), result.result.clone());
        if !result.annotations.is_empty() {
            job.step_annotations
                .insert(result.context_name.clone(), result.annotations.clone());
        }
        if let Some(rpt) = reporting {
            if !result.logs.is_empty() {
                crate::worker::reporting::upload_step_log(rpt, &result.step_id, &result.logs).await;
            }
        }
    }

    // Phase 2: Stop containers step (always runs, like post-job)
    let mut extra_steps = 0u32;
    if has_containers {
        extra_steps += 1;
        let stop_start = crate::worker::helpers::iso_now();
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

        let stop_end = crate::worker::helpers::iso_now();
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
                q.record_step_logs(&stop_step_id, &cleanup_log.join("\n"));
            }
        }
        if let Some(rpt) = reporting {
            // Upload cleanup logs
            if !cleanup_log.is_empty() {
                let content = cleanup_log.join("\n");
                crate::worker::reporting::upload_step_log(rpt, &stop_step_id, &content).await;
            }
        }
    }

    // F019: Queue "Complete job" step
    let ts = crate::worker::helpers::iso_now();
    // Step number: step_offset + user_steps + extra_steps (stop containers) + 1
    let complete_step_number = step_offset + steps.len() as u32 + extra_steps;
    // "Complete job" is runner bookkeeping, not a workflow step. The
    // official runner reports it successful even when the job result is
    // failure or cancellation; the job result is carried separately in
    // completejob.
    let final_conclusion = step_conclusion::SUCCEEDED;
    let complete_step_id = uuid::Uuid::new_v4().to_string();
    job.complete_step_id = Some(complete_step_id.clone());
    {
        let mut q = queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: complete_step_id.clone(),
            number: complete_step_number,
            name: "Complete job".to_string(),
            status: step_status::COMPLETED,
            started_at: Some(ts.clone()),
            completed_at: Some(ts.clone()),
            conclusion: final_conclusion,
        });
    }

    // Upload "Complete job" log matching official runner
    {
        let complete_content = format!("{ts} Cleaning up orphan processes");
        {
            let mut q = queue.lock().await;
            q.record_step_logs(&complete_step_id, &complete_content);
        }
        if let Some(rpt) = reporting {
            crate::worker::reporting::upload_step_log(rpt, &complete_step_id, &complete_content)
                .await;
        }
    }

    Ok(if cancelled {
        "Cancelled".to_string()
    } else if any_failed {
        "Failed".to_string()
    } else {
        "Succeeded".to_string()
    })
}

fn clear_replayed_step_state(
    job: &mut JobContext,
    summaries: &[preloop_gha_protocol::debug_session::StepSummary],
    target: usize,
    end: usize,
) {
    for summary in summaries.iter().take(end + 1).skip(target) {
        job.steps.shift_remove(&summary.context_name);
        job.state.remove(&summary.context_name);
        job.step_annotations.remove(&summary.context_name);
    }
}

fn should_run_step(step: &Step, job: &JobContext) -> Result<bool> {
    use super::step_conditions::{contains_status_check_function, effective_condition};

    let raw = step.condition.as_deref();
    let effective = effective_condition(raw);
    let ctx = job.build_expression_context();
    match preloop_gha_expressions::eval_bool(&effective, &ctx) {
        Ok(result) => Ok(result),
        Err(effective_error) => {
            // Fallback: retry with untrimmed markers if the strip path failed.
            let condition = match raw {
                Some(c) if !c.is_empty() => c,
                _ => return Err(effective_error.into()),
            };
            let stripped = preloop_gha_expressions::trim_expression_markers(condition);
            if stripped == condition {
                Err(effective_error.into())
            } else {
                let fallback = if contains_status_check_function(condition) {
                    condition.to_string()
                } else {
                    format!("success() && ({condition})")
                };
                preloop_gha_expressions::eval_bool(&fallback, &ctx).map_err(Into::into)
            }
        }
    }
}

/// Whether a failed step merits a debug pause.
///
/// Genuine execution failures pause; cancelled steps and background steps
/// never do, and one declined verdict silences later failures so a trailing
/// `always()` step cannot re-trap a user who walked away. `continue-on-error`
/// steps pause despite the tolerated conclusion — see the gate's comment:
/// the pause is a checkpoint, and the `continue` verdict keeps the official
/// outcome/conclusion pair intact.
fn should_pause_on_failure(
    outcome: &str,
    cancelled: bool,
    is_background: bool,
    declined: bool,
) -> bool {
    outcome == "Failure" && !cancelled && !is_background && !declined
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
            let mut expr_ctx = ctx.job.build_expression_context();
            // Step-level `env:` entries are part of the `env` context while
            // evaluating the step's own script (official runner behavior —
            // `run: ${{ env.CMD }}` with a step-level `env: CMD: ...` must
            // resolve; the dump workflow relies on it). The process
            // environment gets them separately at command launch.
            if !ctx.env.is_empty() {
                let overlay = ctx
                    .env
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                    .collect();
                expr_ctx.merge_root("env", serde_json::Value::Object(overlay));
            }
            let evaluated_script = crate::worker::template::evaluate_template(script, &expr_ctx)
                .unwrap_or_else(|_| script.clone());
            // Use step-level working-directory if set, otherwise job workspace
            let effective_dir = working_directory
                .as_ref()
                .map(|wd| {
                    // Resolve relative paths against the workspace
                    let p = std::path::Path::new(wd);
                    if p.is_absolute() {
                        wd.clone()
                    } else {
                        std::path::Path::new(workspace)
                            .join(wd)
                            .to_string_lossy()
                            .into_owned()
                    }
                })
                .unwrap_or_else(|| workspace.to_owned());

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
                    &effective_dir,
                    &cid,
                    ctx,
                    Some(cancel_rx),
                )
                .await;
            }

            super::handlers::script::run_script(
                &evaluated_script,
                shell.as_deref(),
                &effective_dir,
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

async fn run_background_step(
    step: Step,
    job: &mut JobContext,
    workspace: &str,
    cancel_rx: watch::Receiver<bool>,
    queue: Arc<Mutex<ServerQueue>>,
    step_number: u32,
    display_name: String,
) -> BackgroundStepResult {
    let started_at = crate::worker::helpers::iso_now();
    let mut ctx = StepContext::new(job, step.context_name.clone(), display_name.clone());
    {
        let expr_ctx = ctx.job.build_expression_context();
        for (key, value) in &step.env {
            ctx.env.insert(
                key.clone(),
                crate::worker::template::evaluate_template(value, &expr_ctx)
                    .unwrap_or_else(|_| value.clone()),
            );
        }
    }

    let temp_dir = std::path::Path::new(workspace)
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("_temp");
    let paths = match super::file_commands::create_file_commands_with_job(&temp_dir, Some(ctx.job))
    {
        Ok(paths) => {
            for (key, value) in super::file_commands::file_command_env(&paths) {
                ctx.env.insert(key, value);
            }
            Some(paths)
        }
        Err(error) => {
            ctx.log(&format!("##[error]File command setup failed: {error:#}"));
            None
        }
    };

    let outcome = if paths.is_some() {
        execute_step(&step.step_type, &mut ctx, workspace, cancel_rx.clone()).await
    } else {
        Err(anyhow::anyhow!("file command setup failed"))
    };
    let cancelled = outcome
        .as_ref()
        .err()
        .is_some_and(|error| error.to_string().contains("cancel"));
    let (outcome_name, conclusion) = match outcome {
        Ok(()) => ("Success".to_string(), "Success".to_string()),
        Err(_error) if cancelled || *cancel_rx.borrow() => {
            ctx.log("##[error]The operation was canceled.");
            ("Cancelled".to_string(), "Cancelled".to_string())
        }
        Err(error) => {
            if !error.to_string().contains("process exit code") {
                ctx.log(&format!("##[error]{error:#}"));
            }
            if step.continue_on_error {
                ("Failure".to_string(), "Success".to_string())
            } else {
                ("Failure".to_string(), "Failure".to_string())
            }
        }
    };

    let mut result = StepResult {
        outcome: outcome_name,
        conclusion: conclusion.clone(),
        outputs: std::collections::HashMap::new(),
    };
    ctx.job
        .steps
        .insert(step.context_name.clone(), result.clone());
    if let Some(paths) = &paths {
        if let Err(error) =
            super::file_commands::apply_file_commands(paths, &step.context_name, ctx.job)
        {
            ctx.log(&format!("##[error]{error:#}"));
            result.outcome = "Failure".to_string();
            result.conclusion = if step.continue_on_error {
                "Success".to_string()
            } else {
                "Failure".to_string()
            };
        }
        super::file_commands::cleanup_file_commands(paths);
    }
    if let Some(updated) = ctx.job.steps.get(&step.context_name) {
        result.outputs = updated.outputs.clone();
    }

    let completed_at = crate::worker::helpers::iso_now();
    let conclusion_proto = ServerQueue::conclusion_to_proto(&result.conclusion);
    let logs = ctx.log_content();
    {
        let mut q = queue.lock().await;
        let external_id = step.id.clone();
        q.queue_update(StepUpdate {
            external_id: external_id.clone(),
            number: step_number,
            name: display_name,
            status: step_status::COMPLETED,
            started_at: Some(started_at),
            completed_at: Some(completed_at),
            conclusion: conclusion_proto,
        });
        if !logs.is_empty() {
            q.record_step_logs(&external_id, &logs);
        }
    }
    BackgroundStepResult {
        context_name: step.context_name,
        result,
        step_id: step.id,
        logs,
        annotations: ctx.annotations,
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
    log: &mut Vec<String>,
) -> Result<()> {
    use super::container_ops::*;

    // Check Docker availability
    if !check_docker(log).await? {
        anyhow::bail!("Docker is not available");
    }

    let label = generate_label();
    let network = generate_network_name();

    // Clean up stale containers from previous runs
    cleanup_stale(&label, log).await?;

    // Create job network
    create_network(&network, &label, log).await?;

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
            log,
        )
        .await?;
        job_container_id = Some(id);
        job_container_name = Some(name);
    }

    // Start service containers
    let mut service_containers = Vec::new();
    for service in service_specs {
        let name = container_name(&service.image, &label);
        let id = start_service_container(service, &name, &label, &network, log).await?;
        service_containers.push((service.alias.clone(), id, name));
    }

    // Wait for health checks
    if !service_containers.is_empty() {
        wait_for_services_healthy(&service_containers, log).await?;
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

    for line in log.iter() {
        info!("{line}");
    }

    Ok(())
}

#[cfg(test)]
#[path = "steps_runner_tests.rs"]
mod tests;
