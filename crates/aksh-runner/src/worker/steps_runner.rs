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
) -> Result<String> {
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

    for (idx, step) in steps.iter().enumerate() {
        let step_number = (idx + 2) as u32; // 1-based, starting at 2 (after "Set up job")

        // Check for cancellation
        if *cancel_rx.borrow() && !cancelled {
            cancelled = true;
            job.job_status = JobStatus::Cancelled;
            info!("Job cancelled — evaluating remaining steps under cancelled() semantics");
        }

        // Evaluate the step condition
        if !should_run_step(step, job) {
            info!("Skipping step '{}' (condition not met)", step.display_name);
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
                    name: step.display_name.clone(),
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

        info!("Running step: {}", step.display_name);
        let step_start = crate::worker::job_runner::iso_now();

        // F019: Queue InProgress update
        {
            let mut q = queue.lock().await;
            q.queue_update(StepUpdate {
                external_id: step.id.clone(),
                number: step_number,
                name: step.display_name.clone(),
                status: step_status::IN_PROGRESS,
                started_at: Some(step_start.clone()),
                completed_at: None,
                conclusion: 0,
            });
        }
        if let Some(rpt) = reporting {
            crate::worker::job_runner::flush_step_updates(rpt, &queue).await;
        }

        let mut step_ctx =
            StepContext::new(job, step.context_name.clone(), step.display_name.clone());
        for (k, v) in &step.env {
            step_ctx.env.insert(k.clone(), v.clone());
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

        // Execute step — cancel_rx is threaded into process::invoke which
        // kills the process group on cancel. No outer select! that drops futures.
        let outcome = if let Some(timeout_min) = step.timeout_minutes {
            let duration = std::time::Duration::from_secs(timeout_min * 60);
            match tokio::time::timeout(
                duration,
                execute_step(&step.step_type, &mut step_ctx, workspace, cancel_rx.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        "Step '{}' timed out after {timeout_min} minutes",
                        step.display_name
                    );
                    step_ctx.log(&format!(
                        "##[error]The step '{}' timed out",
                        step.display_name
                    ));
                    Err(anyhow::anyhow!("step timed out"))
                }
            }
        } else {
            execute_step(&step.step_type, &mut step_ctx, workspace, cancel_rx.clone()).await
        };

        // Determine outcome and conclusion
        let (outcome_str, conclusion_str) = match &outcome {
            Ok(()) => ("Success".to_string(), "Success".to_string()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("cancelled") {
                    ("Cancelled".to_string(), "Cancelled".to_string())
                } else if step.continue_on_error {
                    warn!(
                        "Step '{}' failed but continue-on-error is set: {e:#}",
                        step.display_name
                    );
                    ("Failure".to_string(), "Success".to_string())
                } else {
                    ("Failure".to_string(), "Failure".to_string())
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
                step.display_name
            );
        }
        // F035: Read step summary content before cleanup deletes the file
        let summary_content =
            std::fs::read_to_string(&file_command_paths.summary_file).unwrap_or_default();
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
                name: step.display_name.clone(),
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
        }
    }

    // F019: Queue "Complete job" step
    let ts = crate::worker::job_runner::iso_now();
    let total_steps = steps.len() + 2;
    let final_conclusion = if cancelled || any_failed {
        step_conclusion::FAILED
    } else {
        step_conclusion::SUCCEEDED
    };
    {
        let mut q = queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: uuid::Uuid::new_v4().to_string(),
            number: total_steps as u32,
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

/// Evaluate whether a step should run based on its condition.
fn should_run_step(step: &Step, job: &JobContext) -> bool {
    let condition = match &step.condition {
        Some(c) if !c.is_empty() => c.as_str(),
        _ => "success()",
    };

    let ctx = job.build_expression_context();
    match aksh_gha_expressions::eval_bool(condition, &ctx) {
        Ok(result) => result,
        Err(_) => {
            // Try stripping ${{ }} markers (conditions sometimes come pre-wrapped)
            let stripped = aksh_gha_expressions::trim_expression_markers(condition);
            aksh_gha_expressions::eval_bool(stripped, &ctx).unwrap_or(false)
        }
    }
}

/// Execute a single step, threading cancel_rx to the process invoker.
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
            working_directory: _,
        } => {
            // Evaluate ${{ }} expressions in the script body
            let expr_ctx = ctx.job.build_expression_context();
            let evaluated_script =
                crate::worker::template::evaluate_template(script, &expr_ctx)
                    .unwrap_or_else(|_| script.clone());
            super::handlers::script::run_script(
                &evaluated_script,
                shell.as_deref(),
                workspace,
                ctx,
                Some(cancel_rx),
            )
            .await
        }
        StepType::Action { uses, with } => {
            super::handlers::action::run_action(uses, with, workspace, ctx).await
        }
    }
}
