//! Sequential step execution with condition evaluation.

use anyhow::Result;
use tokio::sync::watch;
use tracing::{info, warn};

use super::contexts::{JobContext, JobStatus, StepResult};
use super::execution_context::StepContext;
use super::handlers;

/// A step to execute, with its metadata.
#[derive(Debug, Clone)]
pub struct Step {
    pub id: String,
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
/// and remaining steps evaluate under `cancelled()` semantics. Post/always()
/// steps still run per official behavior.
pub async fn run_steps(
    steps: &[Step],
    job: &mut JobContext,
    workspace: &str,
    mut cancel_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<String> {
    let mut any_failed = false;
    let mut cancelled = false;

    for step in steps {
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
                step.id.clone(),
                StepResult {
                    outcome: "Skipped".to_string(),
                    conclusion: "Skipped".to_string(),
                    outputs: std::collections::HashMap::new(),
                },
            );
            continue;
        }

        info!("Running step: {}", step.display_name);

        let mut step_ctx = StepContext::new(job, step.id.clone(), step.display_name.clone());
        for (k, v) in &step.env {
            step_ctx.env.insert(k.clone(), v.clone());
        }

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

        // Record step result
        let step_outputs = std::mem::take(&mut step_ctx.env)
            .into_iter()
            .filter(|(k, _)| k.starts_with("OUTPUT_"))
            .map(|(k, v)| (k.strip_prefix("OUTPUT_").unwrap_or(&k).to_string(), v))
            .collect();

        step_ctx.job.steps.insert(
            step.id.clone(),
            StepResult {
                outcome: outcome_str.clone(),
                conclusion: conclusion_str.clone(),
                outputs: step_outputs,
            },
        );

        if conclusion_str == "Failure" {
            any_failed = true;
            step_ctx.job.job_status = JobStatus::Failure;
        } else if conclusion_str == "Cancelled" {
            cancelled = true;
            step_ctx.job.job_status = JobStatus::Cancelled;
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

/// Evaluate whether a step should run based on its condition.
fn should_run_step(step: &Step, job: &JobContext) -> bool {
    let condition = match &step.condition {
        Some(c) => c.clone(),
        None => "success()".to_string(),
    };

    let ctx = job.build_expression_context();

    match aksh_gha_expressions::eval_bool(&condition, &ctx) {
        Ok(result) => result,
        Err(e) => {
            warn!(
                "Failed to evaluate condition for step '{}': {e:#}. Defaulting to skip.",
                step.display_name
            );
            false
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
            working_directory,
        } => {
            handlers::script::run_script(
                script,
                shell.as_deref(),
                working_directory.as_deref().unwrap_or(workspace),
                ctx,
                Some(cancel_rx),
            )
            .await
        }
        StepType::Action { uses, with } => {
            handlers::action::run_action(uses, with, workspace, ctx).await
        }
    }
}
