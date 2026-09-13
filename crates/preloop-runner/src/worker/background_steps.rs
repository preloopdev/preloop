//! Background step coordination — a port of the official runner's
//! `BackgroundStepCoordinator` (v2.336.0, PRs #4476 / #4482).
//!
//! The coordinator owns every background step's async task, its per-step
//! cancellation channel, and the deferred merge of step state into the job.
//! The main step loop never awaits a background step inline: it hands the
//! step to [`BackgroundStepCoordinator::start_background_step`], which spawns
//! a task that acquires a concurrency slot (official default 10,
//! `system.runner.maxbackgroundsteps`), queues the InProgress timeline update
//! (official `ExecutionContext.Start()` is deferred until the slot is
//! acquired), executes the step against a *private snapshot* of the job, and
//! returns an outcome.
//!
//! State written by the step — GITHUB_OUTPUT, GITHUB_ENV, GITHUB_PATH,
//! GITHUB_STATE, annotations, masks, matchers, github context — is deferred,
//! mirroring the official `DeferredOutputs` / `DeferredEnvironmentVariables` /
//! `DeferOutcomeConclusion` machinery, and only folded into the job at a
//! `wait` / `wait-all` / `cancel` control step or the post-job safety net
//! ([`BackgroundStepCoordinator::complete_waited_steps`]). Canceled results
//! from explicitly cancelled steps never influence the job result (#4482).
//!
//! Cancellation propagates like the official linked token: a job cancellation
//! forwards into every background step's process-cancel channel, and a
//! `cancel` control step cancels its targets with a 7.5 s grace period before
//! force-marking stragglers as canceled.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{watch, Mutex, Semaphore};
use tracing::{info, warn};

use super::contexts::{JobContext, SharedSteps, StepResult};
use super::execution_context::StepContext;
use super::job_runner::ReportingContext;
use super::server_queue::{step_status, ServerQueue, StepUpdate};
use super::steps_runner::Step;

/// Default max concurrent background steps (official `DefaultMaxBackgroundSteps`).
pub(crate) const DEFAULT_MAX_BACKGROUND_STEPS: usize = 10;

/// Grace period for cancelled background steps to terminate
/// (official `CancelWithGracePeriodAsync`, 7.5 s).
const CANCEL_GRACE_SECONDS: f64 = 7.5;

/// A running (or finished) background step, keyed by its context name.
struct BackgroundEntry {
    /// Wire/external step ID (used for timeline updates and log uploads).
    id: String,
    context_name: String,
    display_name: String,
    /// 1-based timeline ordinal, assigned by the main loop.
    step_number: u32,
    /// Sends into the process-cancel channel watched by the step's invoker.
    cancel_tx: watch::Sender<bool>,
    /// Marks an *explicit* cancel (a `cancel` control step). Unlike the
    /// job-cancel path, an explicit cancel makes the step conclude Cancelled
    /// even when a step timeout fired at the same instant (official catch
    /// order: the linked token wins over the step token).
    explicit_cancel_tx: watch::Sender<bool>,
    /// `None` once the task has been joined (or aborted by the final drain).
    handle: Option<tokio::task::JoinHandle<BackgroundStepOutcome>>,
    /// The step's result. Set once — first writer wins, so a grace-period
    /// force-mark is never overwritten by the task finishing late.
    outcome: Option<BackgroundStepOutcome>,
    /// Whether the step tolerates failure (official `continue-on-error`),
    /// needed by the panic fallback to report the right conclusion.
    continue_on_error: bool,
}

/// Everything a background step task needs to run off-loop.
struct BackgroundStepLaunch {
    step: Step,
    /// Private working copy of the job the step mutates.
    working_job: JobContext,
    /// Snapshot at dispatch time; the flush diffs `working_job` against it to
    /// recover exactly what this step wrote.
    base_job: JobContext,
    /// Step env, evaluated by the main loop (official evaluates `step.env`
    /// on the main thread before `StartBackgroundStep`).
    env: HashMap<String, String>,
    display_name: String,
    step_number: u32,
    /// Sender half of the process-cancel channel — the timeout timer and the
    /// job-cancel forwarder send through it.
    cancel_tx: watch::Sender<bool>,
    cancel_rx: watch::Receiver<bool>,
    explicit_cancel_rx: watch::Receiver<bool>,
    job_cancel_rx: watch::Receiver<bool>,
    slots: Arc<Semaphore>,
    queue: Arc<Mutex<ServerQueue>>,
    reporting: Option<Arc<ReportingContext>>,
    workspace: String,
}

/// The terminal state of a background step, reported to the coordinator.
pub(crate) struct BackgroundStepOutcome {
    /// False when the step never acquired a slot (job cancelled while
    /// waiting). Such a step queues no timeline updates and merges nothing,
    /// matching the official faulted-task behavior.
    pub started: bool,
    pub working_job: JobContext,
    pub base_job: JobContext,
    pub context_name: String,
    /// Step-style strings: "Success" | "Failure" | "Cancelled".
    #[allow(dead_code)] // read by tests; conclusion drives all coordinator logic
    pub outcome: String,
    pub conclusion: String,
    #[allow(dead_code)] // asserted by tests; upload happens inside the task
    pub log_content: String,
    #[allow(dead_code)] // asserted by tests; upload happens inside the task
    pub summary_content: String,
    /// Complete step result for synthetic outcomes (forced cancellation,
    /// task panic) that have no job snapshot to diff. `flush_step` writes
    /// this directly into `job.steps`.
    pub direct_result: Option<StepResult>,
}

impl BackgroundStepOutcome {
    fn cancelled_before_start(
        working_job: JobContext,
        base_job: JobContext,
        context_name: String,
    ) -> Self {
        Self {
            started: false,
            working_job,
            base_job,
            context_name,
            outcome: "Cancelled".to_string(),
            conclusion: "Cancelled".to_string(),
            log_content: String::new(),
            summary_content: String::new(),
            direct_result: None,
        }
    }

    /// Fallback for a task that panicked or was aborted by the runtime:
    /// official's generic catch-all marks the step Failed.
    fn failed(context_name: String, conclusion: String) -> Self {
        Self {
            started: true,
            working_job: JobContext::new(
                String::new(),
                String::new(),
                serde_json::json!({}),
                serde_json::json!({}),
            ),
            base_job: JobContext::new(
                String::new(),
                String::new(),
                serde_json::json!({}),
                serde_json::json!({}),
            ),
            context_name,
            outcome: "Failure".to_string(),
            conclusion: conclusion.clone(),
            log_content: String::new(),
            summary_content: String::new(),
            direct_result: Some(StepResult {
                outcome: "Failure".to_string(),
                conclusion,
                outputs: std::collections::HashMap::new(),
            }),
        }
    }

    /// Force-mark applied when a cancelled step ignores its kill signal past
    /// the grace period. The task itself never completes (its finally block
    /// never runs), so this is the only result it will ever have — official
    /// `CancelWithGracePeriodAsync`'s force-mark.
    fn forced_cancel(context_name: String) -> Self {
        Self {
            started: true,
            working_job: JobContext::new(
                String::new(),
                String::new(),
                serde_json::json!({}),
                serde_json::json!({}),
            ),
            base_job: JobContext::new(
                String::new(),
                String::new(),
                serde_json::json!({}),
                serde_json::json!({}),
            ),
            context_name,
            outcome: "Cancelled".to_string(),
            conclusion: "Cancelled".to_string(),
            log_content: String::new(),
            summary_content: String::new(),
            direct_result: Some(StepResult {
                outcome: "Cancelled".to_string(),
                conclusion: "Cancelled".to_string(),
                outputs: std::collections::HashMap::new(),
            }),
        }
    }
}

/// Coordinates background step execution, waiting, cancellation, and deferred
/// state. All methods run on the main step loop; the background tasks only
/// touch their private job snapshots and the shared reporting queue, so no
/// locking is needed beyond those.
pub(crate) struct BackgroundStepCoordinator {
    /// context_name → entry. Keys are context names, matching the official
    /// `_backgroundSteps` dictionary (`step.ExecutionContext.ContextName`)
    /// and the `stepIds` a control step carries on the wire.
    entries: HashMap<String, BackgroundEntry>,
    /// Background steps already waited on or cancelled — never waited on or
    /// flushed twice.
    completed: HashSet<String>,
    /// Steps a `cancel` control step explicitly targeted. Their Canceled
    /// result must not merge into the job result (#4482).
    explicitly_canceled: HashSet<String>,
    slots: Arc<Semaphore>,
    queue: Arc<Mutex<ServerQueue>>,
    reporting: Option<Arc<ReportingContext>>,
    workspace: String,
    job_cancel_rx: watch::Receiver<bool>,
    live_steps: SharedSteps,
}

impl BackgroundStepCoordinator {
    /// Create a coordinator for one job. `max_concurrent` is
    /// `system.runner.maxbackgroundsteps` (default 10), matching the official
    /// `InitializeCoordinator`.
    pub(crate) fn new(
        queue: Arc<Mutex<ServerQueue>>,
        reporting: Option<Arc<ReportingContext>>,
        workspace: String,
        job_cancel_rx: watch::Receiver<bool>,
        max_concurrent: usize,
        initial_steps: indexmap::IndexMap<String, StepResult>,
    ) -> Self {
        Self {
            entries: HashMap::new(),
            completed: HashSet::new(),
            explicitly_canceled: HashSet::new(),
            slots: Arc::new(Semaphore::new(max_concurrent)),
            queue,
            reporting,
            workspace,
            job_cancel_rx,
            live_steps: Arc::new(std::sync::RwLock::new(initial_steps)),
        }
    }

    pub(crate) fn publish_step(&self, job: &JobContext, context_name: &str) {
        let Some(result) = job.steps.get(context_name) else {
            return;
        };
        self.live_steps
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(context_name.to_string(), result.clone());
    }

    pub(crate) fn publish_all_steps(&self, job: &JobContext) {
        *self
            .live_steps
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = job.steps.clone();
    }

    /// Prepare and launch a background step. Does not block the caller —
    /// official `StartBackgroundStep`.
    pub(crate) fn start_background_step(
        &mut self,
        step: Step,
        base_job: JobContext,
        env: HashMap<String, String>,
        display_name: String,
        step_number: u32,
    ) {
        let context_name = step.context_name.clone();
        let id = step.id.clone();
        let continue_on_error = step.continue_on_error;
        info!("Background step '{context_name}' queued (slot acquired asynchronously)");

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (explicit_cancel_tx, explicit_cancel_rx) = watch::channel(false);

        let mut working_job = base_job.clone();
        working_job.attach_live_steps(Arc::clone(&self.live_steps));
        working_job.steps.clear();

        let launch = BackgroundStepLaunch {
            step,
            working_job,
            base_job,
            env,
            display_name: display_name.clone(),
            step_number,
            cancel_tx: cancel_tx.clone(),
            cancel_rx,
            explicit_cancel_rx,
            job_cancel_rx: self.job_cancel_rx.clone(),
            slots: Arc::clone(&self.slots),
            queue: Arc::clone(&self.queue),
            reporting: self.reporting.clone(),
            workspace: self.workspace.clone(),
        };

        let handle = tokio::spawn(execute_background_step(launch));

        self.entries.insert(
            context_name.clone(),
            BackgroundEntry {
                id,
                context_name,
                display_name,
                step_number,
                cancel_tx,
                explicit_cancel_tx,
                handle: Some(handle),
                outcome: None,
                continue_on_error,
            },
        );
    }

    /// Execute a control-flow step (`wait`, `wait-all`, `cancel`) and return
    /// its (outcome, conclusion). Runs on the main loop with the control
    /// step's own execution context, matching official
    /// `RunControlFlowAsync(stepContext, data)`.
    pub(crate) async fn run_control_flow(
        &mut self,
        step_ctx: &mut StepContext<'_>,
        control_type: &str,
        step_ids: &[String],
    ) -> (String, String) {
        match control_type {
            "wait" => {
                step_ctx.log(&format!(
                    "Waiting for background step(s) to complete: {}",
                    self.describe_steps(step_ids)
                ));
                self.wait_for_step_tasks(step_ids).await;
                let merged = self.complete_waited_steps(step_ids, step_ctx.job);
                self.report_completed_steps(
                    step_ctx,
                    "Finished waiting for background step(s).",
                    step_ids,
                );
                conclusion_pair(&merged)
            }
            "wait-all" => {
                let remaining: Vec<String> = self
                    .entries
                    .keys()
                    .filter(|id| !self.completed.contains(*id))
                    .cloned()
                    .collect();
                if remaining.is_empty() {
                    step_ctx.log("No background steps remaining to wait for.");
                } else {
                    step_ctx.log(&format!(
                        "Waiting for all background step(s) to complete: {}",
                        self.describe_steps(&remaining)
                    ));
                }
                self.wait_for_step_tasks(&remaining).await;
                let merged = self.complete_waited_steps(&remaining, step_ctx.job);
                self.report_completed_steps(
                    step_ctx,
                    "Finished waiting for all background step(s).",
                    &remaining,
                );
                conclusion_pair(&merged)
            }
            "cancel" => {
                step_ctx.log(&format!(
                    "Cancelling background step(s): {}",
                    self.describe_steps(step_ids)
                ));
                self.cancel_steps(step_ids, step_ctx.job).await;
                self.report_completed_steps(
                    step_ctx,
                    "Finished cancelling background step(s).",
                    step_ids,
                );
                ("Success".to_string(), "Success".to_string())
            }
            other => {
                warn!("Unknown background step control type '{other}'");
                step_ctx.log(&format!(
                    "##[error]Unknown background step control type '{other}'."
                ));
                ("Failure".to_string(), "Failure".to_string())
            }
        }
    }

    /// Safety net: drain any background steps not already waited on by a
    /// control step, then merge the final results of *all* background steps
    /// into one conclusion for the caller to fold into the job result.
    ///
    /// Official `WaitForUnwaitedStepsAsync` — runs at the main→post-job
    /// boundary and at job end. The returned conclusion is "Success" /
    /// "Failure" / "Cancelled" in step style.
    pub(crate) async fn wait_for_unwaited_steps(&mut self, job: &mut JobContext) -> String {
        let unwaited: Vec<String> = self
            .entries
            .keys()
            .filter(|id| !self.completed.contains(*id))
            .cloned()
            .collect();
        if !unwaited.is_empty() {
            info!(
                "Safety net: {} unwaited background step(s) at the post-job boundary",
                unwaited.len()
            );
            self.wait_for_step_tasks(&unwaited).await;
            self.complete_waited_steps(&unwaited, job);
        }

        let mut merged = "Success".to_string();
        for (id, entry) in &self.entries {
            let Some(outcome) = &entry.outcome else {
                continue;
            };
            // A step that never started has no result to merge (official:
            // the faulted slot-wait task carries none).
            if !outcome.started {
                continue;
            }
            // A step explicitly canceled via a `cancel` control step is
            // expected to be canceled; its Canceled result must not influence
            // the job result. A failure before the cancel took effect still
            // counts (#4482).
            if outcome.conclusion == "Cancelled" && self.explicitly_canceled.contains(id) {
                continue;
            }
            merged = merge_conclusions(&merged, &outcome.conclusion);
        }
        merged
    }

    /// Abort any background task still running after the safety net
    /// (grace-period survivors whose processes ignored cancellation).
    /// Nothing may outlive `run_steps`.
    pub(crate) async fn drain(&mut self) {
        for entry in self.entries.values_mut() {
            if let Some(handle) = entry.handle.take() {
                if !handle.is_finished() {
                    warn!(
                        "Aborting background step '{}' that never completed",
                        entry.context_name
                    );
                    handle.abort();
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Control-flow helpers
    // ------------------------------------------------------------------

    /// Wait for the given steps' tasks, unless the job is cancelled — in
    /// which case cancel them with a grace period (official
    /// `WaitForStepTasksAsync`: `Task.WhenAll(tasks).WaitAsync(token)`).
    async fn wait_for_step_tasks(&mut self, step_ids: &[String]) {
        let mut pending: Vec<String> = Vec::new();
        for id in step_ids {
            if let Some(entry) = self.entries.get(id) {
                if entry.outcome.is_none() && entry.handle.is_some() {
                    pending.push(id.clone());
                }
            } else {
                info!("Wait references unknown background step: {id}");
            }
        }
        if pending.is_empty() {
            return;
        }

        let mut job_cancel_rx = self.job_cancel_rx.clone();
        tokio::select! {
            _ = async {
                for id in &pending {
                    self.join_entry(id).await;
                }
            } => {}
            changed = job_cancel_rx.changed() => {
                if changed.is_err() || *job_cancel_rx.borrow() {
                    info!("Wait interrupted by job cancellation — cancelling background steps");
                    self.cancel_with_grace(&pending).await;
                }
            }
        }
    }

    /// Explicit cancel: mark the steps as expected-to-be-canceled, cancel
    /// their processes with a grace period, then flush deferred state and
    /// mark them completed. Official `CancelStepsAsync`.
    async fn cancel_steps(&mut self, step_ids: &[String], job: &mut JobContext) {
        if step_ids.is_empty() {
            return;
        }
        for id in step_ids {
            self.explicitly_canceled.insert(id.clone());
        }

        let mut to_cancel: Vec<String> = Vec::new();
        for id in step_ids {
            if let Some(entry) = self.entries.get_mut(id) {
                if entry.outcome.is_none()
                    && entry.handle.as_ref().is_some_and(|h| !h.is_finished())
                {
                    let _ = entry.explicit_cancel_tx.send(true);
                    to_cancel.push(id.clone());
                }
            }
        }
        if !to_cancel.is_empty() {
            self.cancel_with_grace(&to_cancel).await;
        }

        // Harvest finished-but-unjoined tasks: a step that completed before
        // the cancel control ran still carries its outcome in the join
        // handle. Joining here (instant for finished handles) ensures its
        // result and deferred state are flushed instead of being dropped as
        // completed-but-never-merged. Handles still running after the grace
        // period were force-marked above, so this loop cannot block.
        for id in step_ids {
            if self.entries.get(id).is_some_and(|e| e.outcome.is_none()) {
                self.join_entry(id).await;
            }
        }

        // Flush deferred state and mark canceled steps as completed.
        self.complete_waited_steps(step_ids, job);
    }

    /// Cancel the given steps' processes and wait up to the grace period for
    /// them to terminate; survivors are force-marked canceled. Official
    /// `CancelWithGracePeriodAsync`.
    async fn cancel_with_grace(&mut self, step_ids: &[String]) {
        let mut to_cancel: Vec<String> = Vec::new();
        for id in step_ids {
            if let Some(entry) = self.entries.get_mut(id) {
                if entry.outcome.is_none()
                    && entry.handle.as_ref().is_some_and(|h| !h.is_finished())
                {
                    info!("Cancelling background step '{id}'");
                    let _ = entry.cancel_tx.send(true);
                    to_cancel.push(id.clone());
                }
            }
        }
        if to_cancel.is_empty() {
            return;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(CANCEL_GRACE_SECONDS);
        for id in &to_cancel {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::select! {
                _ = self.join_entry(id) => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }

        // The tasks above never completed, so their outcome was never set and
        // their Completed update never queued. Force-mark them so the
        // abandoned steps still report a terminal result.
        for id in &to_cancel {
            if let Some(entry) = self.entries.get_mut(id) {
                if entry.outcome.is_none() {
                    info!(
                        "Background step '{id}' did not terminate within the \
                         {CANCEL_GRACE_SECONDS}s grace period; marking as canceled"
                    );
                    // The task is aborted by the final drain and will never
                    // queue its own terminal update — emit it here so the
                    // server timeline does not stay InProgress forever.
                    let external_id = entry.id.clone();
                    let number = entry.step_number;
                    let name = entry.display_name.clone();
                    let ts = crate::worker::helpers::iso_now();
                    let mut q = self.queue.lock().await;
                    q.queue_update(StepUpdate {
                        external_id,
                        number,
                        name,
                        status: step_status::COMPLETED,
                        started_at: Some(ts.clone()),
                        completed_at: Some(ts),
                        conclusion: ServerQueue::conclusion_to_proto("Cancelled"),
                    });
                    entry.outcome = Some(BackgroundStepOutcome::forced_cancel(id.clone()));
                }
            }
        }
    }

    /// Mark the steps completed, flush their deferred state into the job, and
    /// return the merged conclusion ("Success" / "Failure" / "Cancelled").
    /// Official `CompleteWaitedSteps` — including explicit-canceled Canceled
    /// results: the exclusion only applies to the safety-net merge, and the
    /// main loop folds only genuine failures anyway.
    fn complete_waited_steps(&mut self, step_ids: &[String], job: &mut JobContext) -> String {
        let mut merged = "Success".to_string();
        for id in step_ids {
            if self.completed.contains(id) {
                // Already flushed by an earlier wait/cancel. Re-merge the
                // stored conclusion like official (which re-reads the
                // ExecutionContext result) but do not re-flush — that would
                // duplicate append-only state such as job-level annotations.
                // Never-started steps carry no result (official: null), so
                // they contribute nothing.
                if let Some(outcome) = self.entries.get(id).and_then(|e| e.outcome.as_ref()) {
                    if outcome.started {
                        merged = merge_conclusions(&merged, &outcome.conclusion);
                    }
                }
                continue;
            }
            self.completed.insert(id.clone());
            if let Some(entry) = self.entries.get(id) {
                if let Some(outcome) = &entry.outcome {
                    if outcome.started {
                        flush_step(job, outcome);
                    }
                    merged = merge_conclusions(&merged, &outcome.conclusion);
                }
            }
        }
        self.publish_all_steps(job);
        merged
    }

    /// Await one step's task and store its outcome. Does nothing if the
    /// outcome was already set (force-marked): the forced value wins and the
    /// still-running handle is left for the final drain.
    ///
    /// The handle is awaited through `as_mut` and stays in the entry: the
    /// caller may drop this future mid-await (a job cancellation winning a
    /// `tokio::select!` against the wait), and the cancel path must still be
    /// able to find and join — or force-mark — the step.
    async fn join_entry(&mut self, id: &str) {
        let Some(entry) = self.entries.get_mut(id) else {
            return;
        };
        if entry.outcome.is_some() {
            return;
        }
        let Some(handle) = entry.handle.as_mut() else {
            return;
        };
        match handle.await {
            Ok(outcome) => entry.outcome = Some(outcome),
            Err(_) => {
                // The task panicked or was aborted — official's generic
                // catch-all fails the step.
                warn!("Background step '{id}' task failed unexpectedly");
                let (_, conclusion_str) = if entry.continue_on_error {
                    ("Failure".to_string(), "Success".to_string())
                } else {
                    ("Failure".to_string(), "Failure".to_string())
                };
                entry.outcome = Some(BackgroundStepOutcome::failed(
                    id.to_string(),
                    conclusion_str.clone(),
                ));
                let ts = crate::worker::helpers::iso_now();
                let mut q = self.queue.lock().await;
                q.queue_update(StepUpdate {
                    external_id: entry.id.clone(),
                    number: entry.step_number,
                    name: entry.display_name.clone(),
                    status: step_status::COMPLETED,
                    started_at: Some(ts.clone()),
                    completed_at: Some(ts),
                    conclusion: ServerQueue::conclusion_to_proto(&conclusion_str),
                });
            }
        }
    }

    /// Resolve step IDs to display names for customer-facing output
    /// (official `DescribeSteps`; "(none)" when empty).
    fn describe_steps(&self, step_ids: &[String]) -> String {
        let names: Vec<String> = step_ids
            .iter()
            .map(|id| {
                self.entries
                    .get(id)
                    .map(|entry| entry.display_name.clone())
                    .unwrap_or_else(|| id.clone())
            })
            .collect();
        if names.is_empty() {
            "(none)".to_string()
        } else {
            names.join(", ")
        }
    }

    /// Emit the completion summary plus the final result of each affected
    /// step (official `ReportCompletedSteps`).
    fn report_completed_steps(
        &self,
        step_ctx: &mut StepContext<'_>,
        summary: &str,
        step_ids: &[String],
    ) {
        step_ctx.log(summary);
        for id in step_ids {
            if let Some(entry) = self.entries.get(id) {
                let result = entry
                    .outcome
                    .as_ref()
                    .map(|outcome| match outcome.conclusion.as_str() {
                        "Success" => "Succeeded",
                        "Failure" => "Failed",
                        "Cancelled" => "Canceled",
                        _ => "Unknown",
                    })
                    .unwrap_or("Unknown");
                step_ctx.log(&format!("  {}: {result}", entry.display_name));
            }
        }
    }
}

/// Run one background step: acquire a slot, queue the InProgress update,
/// execute against a private job snapshot, queue the Completed update, and
/// report the outcome to the coordinator. Official
/// `ExecuteBackgroundStepCoreAsync`.
async fn execute_background_step(mut launch: BackgroundStepLaunch) -> BackgroundStepOutcome {
    let context_name = launch.step.context_name.clone();
    let display_name = launch.display_name.clone();
    let step_id = launch.step.id.clone();
    let step_number = launch.step_number;
    let timeout_minutes = launch.step.timeout_minutes;

    // Slot acquisition — official `WaitAsync(bgCts.Token)`. A job or
    // explicit cancellation while waiting abandons the step before it
    // starts; it queues no timeline update and merges nothing (official:
    // `Start()` is deferred until the slot is acquired, and the faulted task
    // leaves no record). Official links the per-step token into this wait,
    // so an explicit `cancel` aborts a queued step too — not just job
    // cancellation.
    let _permit = loop {
        tokio::select! {
            permit = Arc::clone(&launch.slots).acquire_owned() => break permit,
            changed = launch.job_cancel_rx.changed() => {
                if changed.is_err() || *launch.job_cancel_rx.borrow() {
                    info!("Background step '{context_name}' abandoned before start (job cancelled)");
                    return BackgroundStepOutcome::cancelled_before_start(
                        launch.working_job,
                        launch.base_job,
                        context_name,
                    );
                }
            }
            changed = launch.explicit_cancel_rx.changed() => {
                if changed.is_err() || *launch.explicit_cancel_rx.borrow() {
                    info!(
                        "Background step '{context_name}' abandoned before start (explicitly cancelled)"
                    );
                    return BackgroundStepOutcome::cancelled_before_start(
                        launch.working_job,
                        launch.base_job,
                        context_name,
                    );
                }
            }
        }
    };

    // F019: InProgress update — deferred to slot acquisition
    // (official `ExecutionContext.Start()`).
    let start_ts = crate::worker::helpers::iso_now();
    {
        let mut q = launch.queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: step_id.clone(),
            number: step_number,
            name: display_name.clone(),
            status: step_status::IN_PROGRESS,
            started_at: Some(start_ts.clone()),
            completed_at: None,
            conclusion: 0,
        });
    }

    let mut step_ctx = StepContext::new(
        &mut launch.working_job,
        context_name.clone(),
        display_name.clone(),
    );
    for (k, v) in &launch.env {
        step_ctx.env.insert(k.clone(), v.clone());
    }

    // File command setup — official FileCommandManager.InitializeFiles.
    let (file_command_paths, file_command_init_error) = {
        let temp_dir = std::path::Path::new(&launch.workspace)
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("_temp");
        match super::file_commands::create_file_commands_with_job(&temp_dir, Some(step_ctx.job)) {
            Ok(paths) => {
                for (k, v) in super::file_commands::file_command_env(&paths) {
                    step_ctx.env.insert(k, v);
                }
                (paths, None)
            }
            Err(e) => {
                step_ctx.log(&format!("##[error]File command setup failed: {e:#}"));
                warn!("File command init failed for background step '{context_name}': {e:#}");
                let temp_dir = std::path::Path::new(&launch.workspace)
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

    // Step timeout — official `SetTimeout` (CancelAfter on the step's own
    // token). The timer and the job-cancel forwarder below both kill the
    // process through the same channel the invoker watches.
    let timed_out = Arc::new(std::sync::atomic::AtomicBool::new(false));
    if let Some(minutes) = timeout_minutes {
        let flag = Arc::clone(&timed_out);
        let cancel_tx = launch.cancel_tx.clone();
        let mut job_cancel_rx = launch.job_cancel_rx.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(minutes.saturating_mul(60))) => {
                    flag.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _ = cancel_tx.send(true);
                }
                changed = job_cancel_rx.changed() => {
                    if changed.is_ok() && *job_cancel_rx.borrow() {
                        let _ = cancel_tx.send(true);
                    }
                }
            }
        });
    }

    // Job-cancel forwarder — official linked-token registration calling
    // `step.ExecutionContext.CancelToken()`.
    {
        let cancel_tx = launch.cancel_tx.clone();
        let mut job_cancel_rx = launch.job_cancel_rx.clone();
        tokio::spawn(async move {
            loop {
                if job_cancel_rx.changed().await.is_err() {
                    return;
                }
                if *job_cancel_rx.borrow() {
                    let _ = cancel_tx.send(true);
                    return;
                }
            }
        });
    }

    let execute_result = match file_command_init_error {
        Some(error) => Err(error),
        None => {
            super::steps_runner::execute_step(
                &launch.step.step_type,
                &mut step_ctx,
                &launch.workspace,
                launch.cancel_rx.clone(),
            )
            .await
        }
    };

    // Outcome decision — official catch order: job/explicit cancel (bgCts)
    // wins over a step timeout (step token); everything else is a failure
    // (continue-on-error still applies).
    let cancel_signaled = *launch.job_cancel_rx.borrow() || *launch.explicit_cancel_rx.borrow();
    let timed_out_now = timed_out.load(std::sync::atomic::Ordering::SeqCst);
    let (mut outcome_str, mut conclusion_str) = bg_outcome_decision(
        &execute_result,
        timed_out_now,
        cancel_signaled,
        launch.step.continue_on_error,
    );

    if timed_out_now && !cancel_signaled {
        step_ctx.log(&format!(
            "##[error]The background step '{display_name}' has timed out after {} minutes.",
            timeout_minutes.unwrap_or(0)
        ));
    } else if cancel_signaled && conclusion_str == "Cancelled" {
        step_ctx.log("##[error]The operation was canceled.");
    } else if let Err(error) = &execute_result {
        let msg = error.to_string();
        if !cancel_signaled
            && !msg.contains("cancelled")
            && !msg.contains("canceled")
            && !msg.contains("process exit code")
        {
            step_ctx.log(&format!("##[error]{error:#}"));
        }
    }

    // Record a step result before applying file commands so GITHUB_OUTPUT can
    // attach outputs to this step.
    step_ctx.job.steps.insert(
        context_name.clone(),
        StepResult {
            outcome: outcome_str.clone(),
            conclusion: conclusion_str.clone(),
            outputs: std::collections::HashMap::new(),
        },
    );

    if let Err(e) =
        super::file_commands::apply_file_commands(&file_command_paths, &context_name, step_ctx.job)
    {
        step_ctx.log("##[error]Unable to process file command successfully.");
        step_ctx.log(&format!("##[error]{e:#}"));
        // Update the outer decision too: the Completed update and the merged
        // result must reflect the file-command failure, not just the
        // in-snapshot StepResult.
        if launch.step.continue_on_error {
            outcome_str = "Failure".to_string();
            conclusion_str = "Success".to_string();
        } else {
            outcome_str = "Failure".to_string();
            conclusion_str = "Failure".to_string();
        }
        if let Some(step_result) = step_ctx.job.steps.get_mut(&context_name) {
            step_result.outcome = outcome_str.clone();
            step_result.conclusion = conclusion_str.clone();
        }
    }

    // F035: Read and scrub step summary content before cleanup deletes the file.
    let summary_content = if let Ok(metadata) = std::fs::metadata(&file_command_paths.summary_file)
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
            step_ctx.annotate(super::execution_context::Annotation {
                level: super::execution_context::AnnotationLevel::Error,
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

    // Annotations — collect after all processing (including summary checks).
    let annotations = step_ctx.annotations.clone();
    let log_content = step_ctx.log_content();
    if !annotations.is_empty() {
        step_ctx.job.add_step_annotations_to_job(&annotations);
        step_ctx
            .job
            .step_annotations
            .insert(context_name.clone(), annotations);
    }

    // F019: Queue Completed update; F020: upload logs and summary. The
    // steps-context merge (outputs/env/state) stays deferred until the
    // coordinator flushes the outcome.
    let end_ts = crate::worker::helpers::iso_now();
    let conclusion_proto = ServerQueue::conclusion_to_proto(&conclusion_str);
    {
        let mut q = launch.queue.lock().await;
        q.queue_update(StepUpdate {
            external_id: step_id.clone(),
            number: step_number,
            name: display_name.clone(),
            status: step_status::COMPLETED,
            started_at: Some(start_ts),
            completed_at: Some(end_ts),
            conclusion: conclusion_proto,
        });
        if !log_content.is_empty() {
            q.record_step_logs(&step_id, &log_content);
        }
    }
    if let Some(rpt) = &launch.reporting {
        if !log_content.is_empty() {
            super::reporting::upload_step_log(rpt, &step_id, &log_content).await;
        }
        if !summary_content.is_empty() {
            super::reporting::upload_step_summary(rpt, &step_id, &summary_content).await;
        }
    }

    info!("Background step '{context_name}' completed with result: {conclusion_str}");
    BackgroundStepOutcome {
        started: true,
        working_job: launch.working_job,
        base_job: launch.base_job,
        context_name,
        outcome: outcome_str,
        conclusion: conclusion_str,
        log_content,
        summary_content,
        direct_result: None,
    }
}

/// Decide a background step's (outcome, conclusion) pair, mirroring the
/// official catch ordering: the linked token (job/explicit cancel) wins over
/// the step token (timeout); a cancelled process concludes Cancelled; any
/// other error is a failure, with continue-on-error tolerated.
fn bg_outcome_decision(
    execute_result: &Result<()>,
    timed_out: bool,
    cancel_signaled: bool,
    continue_on_error: bool,
) -> (String, String) {
    if timed_out && !cancel_signaled {
        // Official: OCE on the step's own token → timed out → Failed.
        if continue_on_error {
            ("Failure".to_string(), "Success".to_string())
        } else {
            ("Failure".to_string(), "Failure".to_string())
        }
    } else if let Err(error) = execute_result {
        let msg = error.to_string();
        if msg.contains("cancelled") || msg.contains("canceled") || cancel_signaled {
            ("Cancelled".to_string(), "Cancelled".to_string())
        } else if continue_on_error {
            ("Failure".to_string(), "Success".to_string())
        } else {
            ("Failure".to_string(), "Failure".to_string())
        }
    } else {
        ("Success".to_string(), "Success".to_string())
    }
}

/// Fold a merged conclusion into a step (outcome, conclusion) pair.
fn conclusion_pair(merged: &str) -> (String, String) {
    match merged {
        "Failure" => ("Failure".to_string(), "Failure".to_string()),
        "Cancelled" => ("Cancelled".to_string(), "Cancelled".to_string()),
        _ => ("Success".to_string(), "Success".to_string()),
    }
}

/// Merge two step-style conclusions; the worst result wins, matching
/// `TaskResultUtil.MergeTaskResults` (Failed > Cancelled > Succeeded).
fn merge_conclusions(a: &str, b: &str) -> String {
    if a == "Failure" || b == "Failure" {
        "Failure".to_string()
    } else if a == "Cancelled" || b == "Cancelled" {
        "Cancelled".to_string()
    } else {
        "Success".to_string()
    }
}

/// Fold a background step's deferred state into the job — official
/// `FlushDeferredOutputs` / `FlushDeferredEnvironment` /
/// `FlushDeferredOutcomeConclusion` plus the preloop state surfaces
/// (annotations, masks, matchers, artifact subjects, github context).
fn flush_step(job: &mut JobContext, outcome: &BackgroundStepOutcome) {
    let private = &outcome.working_job;
    let base = &outcome.base_job;

    // Synthetic outcomes (forced cancellation, task panic) carry a complete
    // StepResult instead of a job snapshot to diff — install it directly so
    // `steps.<id>` reflects the terminal state.
    if let Some(result) = &outcome.direct_result {
        job.steps
            .insert(outcome.context_name.clone(), result.clone());
    }

    // GITHUB_ENV — merge only keys the step wrote (value differs from the
    // dispatch-time snapshot), so concurrent foreground writes survive.
    for (k, v) in &private.env {
        if base.env.get(k) != Some(v) {
            job.env.insert(k.clone(), v.clone());
        }
    }

    // GITHUB_PATH — only entries absent at dispatch time, applied newest
    // first exactly like the inline `apply_file_commands` path. Dedup first
    // (official FlushDeferredEnvironment removes then re-adds), so a step
    // waited on twice does not duplicate entries.
    for p in private.extra_path.iter().rev() {
        if !base.extra_path.iter().any(|q| q == p) {
            job.extra_path.retain(|q| q != p);
            job.extra_path.insert(0, p.clone());
        }
    }

    // Step results (outcome/conclusion/outputs) — deferred until the flush
    // (official `DeferOutcomeConclusion`). Composite steps may have written
    // nested results too; merge every entry the snapshot did not have.
    for (name, result) in &private.steps {
        if base.steps.get(name) != Some(result) {
            job.steps.insert(name.clone(), result.clone());
        }
    }

    // GITHUB_STATE — keyed by step id with the same __pre_/__post_ aliasing
    // the inline path uses. Like env, only keys the step changed (value
    // differs from the dispatch-time snapshot) are merged, so a background
    // step's stale snapshot cannot clobber a newer foreground write.
    for (step_id, state) in &private.state {
        let effective = step_id
            .strip_prefix("__pre_")
            .or_else(|| step_id.strip_prefix("__post_"))
            .unwrap_or(step_id);
        let base_state = base
            .state
            .get(step_id)
            .or_else(|| base.state.get(effective));
        for (k, v) in state {
            if base_state.and_then(|m| m.get(k)) != Some(v) {
                job.state
                    .entry(effective.to_string())
                    .or_default()
                    .insert(k.clone(), v.clone());
            }
        }
    }

    // Masks (::add-mask::). Live log masking already saw them through the
    // shared `live_masks`; this makes them stick for later steps.
    for m in &private.masks {
        if !base.masks.contains(m) {
            job.add_mask(m);
        }
    }

    // Problem matchers (::add-matcher:: / ::remove-matcher::).
    job.matchers.merge_delta(&base.matchers, &private.matchers);

    // Artifact subjects ($GITHUB_ARTIFACTS). Conflicts with declarations made
    // concurrently by other steps keep the first declaration.
    for (name, subject) in &private.artifact_subjects {
        if base.artifact_subjects.get(name) == Some(subject) {
            continue;
        }
        match job.artifact_subjects.get(name) {
            Some(existing) if existing.digest == subject.digest => {}
            Some(_) => warn!(
                "Background step artifact '{name}' conflicts with an existing \
                 declaration — keeping the existing digest"
            ),
            None => {
                job.artifact_subjects.insert(name.clone(), subject.clone());
            }
        }
    }

    // GitHub context changes (set-output / set-env on the github context).
    let private_github = private
        .context_data
        .get("github")
        .map(super::job_extension::decode_typed_value);
    let base_github = base
        .context_data
        .get("github")
        .map(super::job_extension::decode_typed_value);
    let mut keys: Vec<String> = Vec::new();
    for key in private_github
        .as_ref()
        .and_then(|v| v.as_object())
        .into_iter()
        .flat_map(|obj| obj.keys())
    {
        keys.push(key.clone());
    }
    for key in base_github
        .as_ref()
        .and_then(|v| v.as_object())
        .into_iter()
        .flat_map(|obj| obj.keys())
    {
        if !keys.contains(key) {
            keys.push(key.clone());
        }
    }
    for key in keys {
        let private_value = private_github.as_ref().and_then(|v| v.get(&key)).cloned();
        let base_value = base_github.as_ref().and_then(|v| v.get(&key)).cloned();
        if private_value != base_value {
            job.set_github_context_value(&key, private_value);
        }
    }

    // Annotations (F025).
    if let Some(annotations) = private.step_annotations.get(&outcome.context_name) {
        job.step_annotations
            .insert(outcome.context_name.clone(), annotations.clone());
        job.add_step_annotations_to_job(annotations);
    }

    // Node-version telemetry.
    for name in &private.upgraded_node24_actions {
        if !base.upgraded_node24_actions.contains(name) {
            job.record_upgraded_node24_action(name);
        }
    }
    for name in &private.deprecated_node20_actions {
        if !base.deprecated_node20_actions.contains(name) {
            job.record_deprecated_node20_action(name);
        }
    }
}

#[cfg(test)]
#[path = "background_steps_tests.rs"]
mod tests;
