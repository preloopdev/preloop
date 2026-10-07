//! Tests for the background step coordinator: concurrency with the main
//! loop, wait/wait-all/cancel control steps, cancellation propagation,
//! deferred state merging, failure folding, and the post-job safety net.

use super::*;
use crate::worker::contexts::JobContext;
use crate::worker::server_queue::{ServerQueue, step_status};
use crate::worker::steps_runner::{StepType, run_steps};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::{Mutex, watch};

fn script_step(name: &str, script: &str, is_background: bool) -> Step {
    Step {
        id: uuid::Uuid::new_v4().to_string(),
        context_name: name.to_string(),
        display_name: name.to_string(),
        step_type: StepType::Script {
            script: script.to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        },
        condition: Some("success()".to_string()),
        continue_on_error: false,
        timeout_minutes: None,
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background,
    }
}

fn control_step(name: &str, control_type: &str, step_ids: &[&str]) -> Step {
    Step {
        id: uuid::Uuid::new_v4().to_string(),
        context_name: name.to_string(),
        display_name: name.to_string(),
        step_type: StepType::ControlFlow {
            control_type: control_type.to_string(),
            step_ids: step_ids.iter().map(|s| s.to_string()).collect(),
        },
        condition: Some("always()".to_string()),
        continue_on_error: false,
        timeout_minutes: None,
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background: false,
    }
}

fn test_job(dir: &TempDir) -> JobContext {
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.workspace = Some(dir.path().to_string_lossy().into_owned());
    job
}

async fn run(
    steps: &[Step],
    job: &mut JobContext,
    dir: &TempDir,
    cancel_rx: watch::Receiver<bool>,
) -> (String, Arc<Mutex<ServerQueue>>) {
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let result = run_steps(
        steps,
        job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue.clone(),
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();
    (result, queue)
}

// ---------------------------------------------------------------------
// Outcome decision — official catch order: linked token (job/explicit
// cancel) wins over the step token (timeout); continue-on-error applies.
// ---------------------------------------------------------------------

#[test]
fn bg_outcome_decision_matrix() {
    let ok = Ok(());
    let err = Err(anyhow::anyhow!("boom"));
    let cancel_err = Err(anyhow::anyhow!("process cancelled"));
    let timeout_err = Err(anyhow::anyhow!("process killed by timeout signal"));

    // Success
    assert_eq!(
        bg_outcome_decision(&ok, false, false, false),
        ("Success".to_string(), "Success".to_string())
    );
    // Plain failure, with and without continue-on-error
    assert_eq!(
        bg_outcome_decision(&err, false, false, false),
        ("Failure".to_string(), "Failure".to_string())
    );
    assert_eq!(
        bg_outcome_decision(&err, false, false, true),
        ("Failure".to_string(), "Success".to_string())
    );
    // A cancel-killed process concludes Cancelled (invoke reports
    // "process cancelled")
    assert_eq!(
        bg_outcome_decision(&cancel_err, false, false, false),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    // The explicit/job cancel flag wins even when the error text is opaque
    assert_eq!(
        bg_outcome_decision(&err, false, true, false),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    assert_eq!(
        bg_outcome_decision(&cancel_err, false, true, false),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    // Timeout (step token) → Failed, with the official timeout wording
    assert_eq!(
        bg_outcome_decision(&timeout_err, true, false, false),
        ("Failure".to_string(), "Failure".to_string())
    );
    assert_eq!(
        bg_outcome_decision(&timeout_err, true, false, true),
        ("Failure".to_string(), "Success".to_string())
    );
    // Job/explicit cancel wins over a simultaneously-fired timeout
    assert_eq!(
        bg_outcome_decision(&timeout_err, true, true, false),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    // Timeout fired but the process exited cleanly → still Failed
    assert_eq!(
        bg_outcome_decision(&ok, true, false, false),
        ("Failure".to_string(), "Failure".to_string())
    );
}

#[test]
fn merge_conclusions_worst_wins() {
    assert_eq!(merge_conclusions("Success", "Success"), "Success");
    assert_eq!(merge_conclusions("Success", "Cancelled"), "Cancelled");
    assert_eq!(merge_conclusions("Cancelled", "Cancelled"), "Cancelled");
    assert_eq!(merge_conclusions("Success", "Failure"), "Failure");
    assert_eq!(merge_conclusions("Cancelled", "Failure"), "Failure");
}

// ---------------------------------------------------------------------
// Concurrency: the background step runs off-loop.
// ---------------------------------------------------------------------

#[tokio::test]
async fn background_step_runs_concurrently_with_foreground() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // The background step takes a second and then touches a marker. The
    // foreground step asserts the marker is NOT there yet: under sequential
    // execution the background step would finish first and the assertion
    // would fail.
    let bg = script_step("db", "sleep 1 && touch bg-done", true);
    let fg = script_step("fg", "test ! -f bg-done", false);

    let (result, queue) = run(&[bg.clone(), fg.clone()], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("db").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    assert_eq!(
        job.steps.get("fg").map(|s| s.conclusion.as_str()),
        Some("Success")
    );

    // Timeline ordering proves the overlap: the foreground step completed
    // while the background step was still in progress.
    let updates = { queue.lock().await.all_queued_updates().to_vec() };
    let bg_completed = updates
        .iter()
        .position(|u| u.external_id == bg.id && u.status == step_status::COMPLETED)
        .expect("background step completed update");
    let fg_completed = updates
        .iter()
        .position(|u| u.external_id == fg.id && u.status == step_status::COMPLETED)
        .expect("foreground step completed update");
    assert!(
        fg_completed < bg_completed,
        "foreground step must complete while the background step still runs"
    );
}

// ---------------------------------------------------------------------
// wait / wait-all / cancel control steps.
// ---------------------------------------------------------------------

#[tokio::test]
async fn wait_control_step_blocks_until_background_completes() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("db", "echo ready", true);
    let wait = control_step("wait-db", "wait", &["db"]);

    let (result, queue) = run(&[bg, wait], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("db").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    assert_eq!(
        job.steps.get("wait-db").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    // The wait step reports what it waited for and each step's result
    // (official RunControlFlowAsync output).
    let logs = queue.lock().await.all_step_log_content();
    assert!(
        logs.contains("Waiting for background step(s) to complete: db"),
        "wait step must name the target: {logs}"
    );
    assert!(logs.contains("Finished waiting for background step(s)."));
    assert!(
        logs.contains("  db: Succeeded"),
        "per-step result line: {logs}"
    );
}

#[tokio::test]
async fn background_step_failure_propagates_at_wait() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("flaky", "exit 1", true);
    let wait = control_step("wait-flaky", "wait", &["flaky"]);

    let (result, _queue) = run(&[bg, wait], &mut job, &dir, cancel_rx).await;

    // The failure folds through the wait step into the job result.
    assert_eq!(result, "Failed");
    assert_eq!(
        job.steps.get("flaky").map(|s| s.conclusion.as_str()),
        Some("Failure")
    );
    assert_eq!(
        job.steps.get("wait-flaky").map(|s| s.conclusion.as_str()),
        Some("Failure")
    );
}

#[tokio::test]
async fn wait_all_waits_for_all_background_steps() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg1 = script_step("svc1", "sleep 0.3", true);
    let bg2 = script_step("svc2", "sleep 0.3", true);
    let wait_all = control_step("wait-all", "wait-all", &[]);

    let (result, queue) = run(&[bg1, bg2, wait_all], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("svc1").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    assert_eq!(
        job.steps.get("svc2").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    let logs = queue.lock().await.all_step_log_content();
    let waiting_line = logs
        .lines()
        .find(|line| line.contains("Waiting for all background step(s) to complete:"))
        .unwrap_or_else(|| panic!("wait-all step must report waiting: {logs}"));
    assert!(
        waiting_line.contains("svc1") && waiting_line.contains("svc2"),
        "wait-all step must name both remaining steps: {waiting_line}"
    );
}

#[tokio::test]
async fn cancel_of_queued_step_abandons_before_start_and_job_stays_succeeded() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // A long-running step that only cancellation can stop.
    let bg = script_step("server", "sleep 60", true);
    let cancel = control_step("cancel-server", "cancel", &["server"]);

    let (result, queue) = run(&[bg, cancel], &mut job, &dir, cancel_rx).await;

    // The cancel control races the spawned task's slot acquisition. Either
    // outcome is official behavior: if the cancel wins, the step is abandoned
    // *before start* (`WaitAsync(bgCts.Token)` faults the task, `Start()`
    // never runs — no result, no timeline record); if the step started first,
    // it concludes Cancelled. It must never fail the job either way.
    assert_eq!(result, "Succeeded");
    match job.steps.get("server") {
        None => {} // abandoned before start — no result, official semantics
        Some(result) => assert_eq!(
            result.conclusion, "Cancelled",
            "a started step must conclude Cancelled, never Failure"
        ),
    }
    assert_eq!(
        job.steps
            .get("cancel-server")
            .map(|s| s.conclusion.as_str()),
        Some("Success")
    );
    let logs = queue.lock().await.all_step_log_content();
    assert!(logs.contains("Cancelling background step(s): server"));
    assert!(logs.contains("Finished cancelling background step(s)."));
    assert!(
        logs.contains("  server: Canceled"),
        "per-step result line: {logs}"
    );
}

#[tokio::test]
async fn wait_after_cancel_does_not_cancel_job() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("server", "sleep 60", true);
    let cancel = control_step("cancel-server", "cancel", &["server"]);
    // A later wait over the already-cancelled step merges a Canceled result
    // into the control step — that must not cancel the job.
    let wait = control_step("wait-server", "wait", &["server"]);

    let (result, _queue) = run(&[bg, cancel, wait], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    // The step was either abandoned before start (no result to merge —
    // official: null ExecutionContext result → wait concludes Success) or
    // started and cancelled (its Cancelled result re-merges into the wait).
    // Neither may cancel the job.
    match job.steps.get("server") {
        None => assert_eq!(
            job.steps.get("wait-server").map(|s| s.conclusion.as_str()),
            Some("Success")
        ),
        Some(result) => {
            assert_eq!(result.conclusion, "Cancelled");
            assert_eq!(
                job.steps.get("wait-server").map(|s| s.conclusion.as_str()),
                Some("Cancelled")
            );
        }
    }
}

#[tokio::test]
async fn repeated_waits_do_not_duplicate_path_entries() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("db", "echo \"$PWD/extra\" >> \"$GITHUB_PATH\"", true);
    // Waiting twice must flush the deferred state twice without duplicating
    // the GITHUB_PATH entry (official FlushDeferredEnvironment removes then
    // re-adds).
    let wait1 = control_step("wait-db-1", "wait", &["db"]);
    let wait2 = control_step("wait-db-2", "wait", &["db"]);

    let (result, _queue) = run(&[bg, wait1, wait2], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    let matches = job
        .extra_path
        .iter()
        .filter(|p| p.ends_with("/extra"))
        .count();
    assert_eq!(
        matches, 1,
        "GITHUB_PATH entry must not duplicate: {:?}",
        job.extra_path
    );
}

// ---------------------------------------------------------------------
// Safety net at the post-job boundary.
// ---------------------------------------------------------------------

#[tokio::test]
async fn safety_net_merges_background_failure_without_control_steps() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // No wait/cancel control step at all: the safety net at the end of the
    // main steps must wait for the background step and fold its failure.
    let bg = script_step("flaky", "exit 1", true);

    let (result, _queue) = run(&[bg], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Failed");
    assert_eq!(
        job.steps.get("flaky").map(|s| s.conclusion.as_str()),
        Some("Failure")
    );
}

#[tokio::test]
async fn safety_net_runs_before_post_steps() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("db", "touch bg-marker", true);
    // A synthetic post step (same shape job_extension materializes) that
    // observes the background step's file: it must see it because the safety
    // net waits for background steps before post steps run.
    let mut post = script_step("__post_check", "test -f bg-marker", false);
    post.id = format!("__post_{}", post.id);
    post.raw = serde_json::json!({ "__post": true });

    let (result, _queue) = run(&[bg, post], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("__post_check").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
}

// ---------------------------------------------------------------------
// Cancellation propagation.
// ---------------------------------------------------------------------

#[tokio::test]
async fn already_cancelled_job_does_not_start_background_step() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    cancel_tx.send(true).unwrap();

    // `always()` keeps the step eligible during cancellation unwind. The
    // linked job token must still prevent it from acquiring a background
    // slot and starting, matching CancellationToken's immediate callback.
    let mut bg = script_step("server", "touch should-not-run", true);
    bg.condition = Some("always()".to_string());

    let (result, _queue) = run(&[bg], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Cancelled");
    assert!(
        !dir.path().join("should-not-run").exists(),
        "a background step must not start after job cancellation"
    );
}

#[tokio::test]
async fn job_cancel_propagates_to_background_steps() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("server", "sleep 60", true);
    let fg = script_step("fg", "echo never", false);

    // Cancel the job while the background step is still starting up.
    let cancel_tx = cancel_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let _ = cancel_tx.send(true);
    });

    let (result, _queue) = run(&[bg, fg], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Cancelled");
    // The background step was killed by the propagated cancellation and
    // reports Canceled. The foreground step completed before the cancel
    // fired (400 ms in), so it ran normally — the cancellation propagates to
    // the still-running background step and the safety net folds the
    // Canceled result into the job conclusion.
    assert_eq!(
        job.steps.get("server").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
    assert!(job.steps.contains_key("fg"));
}

// ---------------------------------------------------------------------
// Deferred state merging (GITHUB_OUTPUT / GITHUB_ENV / annotations).
// ---------------------------------------------------------------------

#[tokio::test]
async fn queued_background_step_sees_later_foreground_step_outputs() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    job.variables = serde_json::json!({
        "system.runner.maxbackgroundsteps": { "value": "1" }
    });
    let (_tx, cancel_rx) = watch::channel(false);

    // Hold the only background slot until the foreground producer exits and
    // its GITHUB_OUTPUT file has been applied. The queued consumer was
    // dispatched before that output existed, but official StepsContext is a
    // live global scope and must expose it when the consumer actually runs.
    let blocker = script_step(
        "blocker",
        "while [ ! -f producer-done ]; do sleep 0.01; done; sleep 0.2",
        true,
    );
    let consumer = script_step(
        "consumer",
        "test \"${{ steps.producer.outputs.value }}\" = \"yes\"",
        true,
    );
    let producer = script_step(
        "producer",
        "echo \"value=yes\" >> \"$GITHUB_OUTPUT\"; touch producer-done",
        false,
    );
    let wait = control_step("wait-all", "wait-all", &[]);

    let (result, _queue) = run(
        &[blocker, consumer, producer, wait],
        &mut job,
        &dir,
        cancel_rx,
    )
    .await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps
            .get("consumer")
            .map(|step| step.conclusion.as_str()),
        Some("Success")
    );
}

#[tokio::test]
async fn background_add_mask_applies_to_concurrent_foreground_logs() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // Keep the background step running so its deferred state cannot flush
    // before the foreground step logs the newly registered secret.
    let bg = script_step(
        "masker",
        "echo \"::add-mask::supersecret\"; sleep 0.2; touch mask-ready; \
         while [ ! -f mask-seen ]; do sleep 0.01; done",
        true,
    );
    let fg = script_step(
        "logger",
        "while [ ! -f mask-ready ]; do sleep 0.01; done; \
         echo supersecret; touch mask-seen",
        false,
    );
    let wait = control_step("wait-masker", "wait", &["masker"]);

    let (result, queue) = run(&[bg, fg, wait], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    let logs = queue.lock().await.all_step_log_content();
    assert!(
        !logs.lines().any(|line| line.ends_with(" supersecret")),
        "foreground output leaked the secret: {logs}"
    );
    assert!(
        logs.lines().any(|line| line.ends_with(" ***")),
        "masked foreground output missing: {logs}"
    );
}

#[tokio::test]
async fn background_step_state_merges_at_wait_and_reaches_later_steps() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step(
        "producer",
        "echo \"k=v\" >> \"$GITHUB_OUTPUT\" && \
         echo \"BG_VAR=1\" >> \"$GITHUB_ENV\" && \
         echo \"::error::bg problem\"",
        true,
    );
    let wait = control_step("wait-producer", "wait", &["producer"]);
    // A later foreground step must see the background step's env.
    let fg = script_step("consumer", "test \"$BG_VAR\" = \"1\"", false);

    let (result, _queue) = run(&[bg, wait, fg], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps
            .get("producer")
            .map(|s| s.outputs.get("k").cloned()),
        Some(Some("v".to_string()))
    );
    assert_eq!(job.env.get("BG_VAR").map(String::as_str), Some("1"));
    let annotations = job.step_annotations.get("producer");
    assert!(
        annotations.is_some_and(|anns| anns.iter().any(|a| a.message.contains("bg problem"))),
        "background step annotations must merge: {annotations:?}"
    );
}

// ---------------------------------------------------------------------
// Control-step parsing and implicit wait-all (job_extension).
// ---------------------------------------------------------------------

#[test]
fn control_step_parsed_from_wire() {
    use crate::worker::job_extension::build_step_list;
    let step = serde_json::json!({
        "controlType": "wait",
        "stepIds": ["db"],
        "displayName": "Wait for database",
        "contextName": "wait-db",
    });
    let parsed = build_step_list(&[step], &serde_json::json!({}));
    assert_eq!(parsed.len(), 1);
    let parsed = &parsed[0];
    assert_eq!(parsed.display_name, "Wait for database");
    assert_eq!(parsed.condition.as_deref(), Some("always()"));
    assert!(!parsed.is_background);
    match &parsed.step_type {
        StepType::ControlFlow {
            control_type,
            step_ids,
        } => {
            assert_eq!(control_type, "wait");
            assert_eq!(step_ids, &["db".to_string()]);
        }
        other => panic!("expected control step, got {other:?}"),
    }
}

#[test]
fn implicit_wait_all_added_for_uncovered_background_steps() {
    use crate::worker::job_extension::build_step_list_with_lifecycle;

    let mut bg = script_step("db", "echo hi", true);
    bg.step_type = StepType::Action {
        uses: "does-not-matter".to_string(),
        with: serde_json::json!({}),
    };
    let fg = script_step("fg", "echo hi", false);

    let result = build_step_list_with_lifecycle(
        vec![bg, fg],
        "/tmp/workspace",
        &std::collections::HashMap::new(),
    );

    let wait_all = result
        .iter()
        .find(|step| step.context_name == "__implicit_wait_all")
        .expect("implicit wait-all must be injected");
    assert_eq!(wait_all.display_name, "Wait for all background steps");
    assert_eq!(wait_all.condition.as_deref(), Some("always()"));
    match &wait_all.step_type {
        StepType::ControlFlow {
            control_type,
            step_ids,
        } => {
            // The implicit control step uses the official
            // `Pipelines.BackgroundControlTypes.WaitAll` spelling, the same
            // string the timeline carries as `backgroundControlType`.
            assert_eq!(control_type, "waitAll");
            assert_eq!(step_ids, &["db".to_string()]);
        }
        other => panic!("expected waitAll control step, got {other:?}"),
    }
    // The wait-all sits after the main steps (and before any post steps).
    let position = result
        .iter()
        .position(|step| step.context_name == "__implicit_wait_all")
        .unwrap();
    assert_eq!(result[position - 1].context_name, "fg");
}

#[test]
fn explicit_wait_covers_background_steps_and_suppresses_implicit_wait_all() {
    use crate::worker::job_extension::build_step_list_with_lifecycle;

    let mut bg = script_step("db", "echo hi", true);
    bg.step_type = StepType::Action {
        uses: "does-not-matter".to_string(),
        with: serde_json::json!({}),
    };
    let wait = control_step("wait-db", "wait", &["db"]);

    let result = build_step_list_with_lifecycle(
        vec![bg, wait],
        "/tmp/workspace",
        &std::collections::HashMap::new(),
    );

    assert!(
        !result
            .iter()
            .any(|step| step.context_name == "__implicit_wait_all"),
        "an explicit wait covering every background step suppresses the implicit wait-all"
    );
}

#[test]
fn implicit_wait_all_covers_only_uncovered_steps() {
    use crate::worker::job_extension::build_step_list_with_lifecycle;

    let mut covered = script_step("covered", "echo hi", true);
    covered.step_type = StepType::Action {
        uses: "does-not-matter".to_string(),
        with: serde_json::json!({}),
    };
    let mut uncovered = script_step("uncovered", "echo hi", true);
    uncovered.step_type = StepType::Action {
        uses: "does-not-matter".to_string(),
        with: serde_json::json!({}),
    };
    let wait = control_step("wait-covered", "wait", &["covered"]);

    let result = build_step_list_with_lifecycle(
        vec![covered, uncovered, wait],
        "/tmp/workspace",
        &std::collections::HashMap::new(),
    );

    let wait_all = result
        .iter()
        .find(|step| step.context_name == "__implicit_wait_all")
        .expect("uncovered background step must trigger the implicit wait-all");
    match &wait_all.step_type {
        StepType::ControlFlow { step_ids, .. } => {
            assert_eq!(step_ids, &["uncovered".to_string()]);
        }
        other => panic!("expected wait-all control step, got {other:?}"),
    }
}

#[test]
fn wait_all_control_step_covers_every_background_step() {
    use crate::worker::job_extension::build_step_list_with_lifecycle;

    let mut bg = script_step("db", "echo hi", true);
    bg.step_type = StepType::Action {
        uses: "does-not-matter".to_string(),
        with: serde_json::json!({}),
    };
    let wait_all = control_step("wait-all", "wait-all", &[]);

    let result = build_step_list_with_lifecycle(
        vec![bg, wait_all],
        "/tmp/workspace",
        &std::collections::HashMap::new(),
    );

    assert!(
        !result
            .iter()
            .any(|step| step.context_name == "__implicit_wait_all"),
        "an explicit wait-all covers every background step"
    );
}

#[tokio::test]
async fn cancel_of_started_background_step_concludes_cancelled() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("server", "sleep 60", true);
    // A foreground step that yields the runtime so the background task
    // acquires its slot and starts before the cancel control runs.
    let gap = script_step("gap", "sleep 0.2", false);
    let cancel = control_step("cancel-server", "cancel", &["server"]);

    let (result, _queue) = run(&[bg, gap, cancel], &mut job, &dir, cancel_rx).await;

    // #4482: an explicitly cancelled background step must not flip the job.
    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("server").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
}

#[tokio::test]
async fn cancel_harvests_finished_but_unjoined_step() {
    let dir = TempDir::new().unwrap();
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);
    let mut coordinator = BackgroundStepCoordinator::new(
        queue.clone(),
        None,
        dir.path().to_string_lossy().into_owned(),
        cancel_rx,
        10,
        indexmap::IndexMap::new(),
    );
    let mut job = test_job(&dir);
    let bg = script_step("fast", "echo done", true);
    coordinator.start_background_step(
        bg,
        job.clone(),
        std::collections::HashMap::new(),
        "fast".to_string(),
        1,
        None,
    );
    // Wait until the task has finished but has NOT been joined yet
    // (outcome still None) — the exact state a fast step is in when its
    // cancel control runs.
    loop {
        let finished_unjoined = coordinator
            .entries
            .get("fast")
            .map(|e| e.outcome.is_none() && e.handle.as_ref().is_some_and(|h| h.is_finished()))
            .unwrap_or(false);
        if finished_unjoined {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let mut step_ctx = StepContext::new(
        &mut job,
        "cancel-fast".to_string(),
        "cancel-fast".to_string(),
    );
    let (outcome_str, _conclusion_str) = coordinator
        .run_control_flow(&mut step_ctx, "cancel", &["fast".to_string()])
        .await;

    // The finished-but-unjoined step must be harvested and flushed, not
    // dropped as completed-but-never-merged.
    assert_eq!(outcome_str, "Success");
    assert_eq!(
        job.steps.get("fast").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
}

#[tokio::test]
async fn cancel_abandons_step_queued_behind_saturated_slots() {
    let dir = TempDir::new().unwrap();
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);
    let mut coordinator = BackgroundStepCoordinator::new(
        queue.clone(),
        None,
        dir.path().to_string_lossy().into_owned(),
        cancel_rx,
        1,
        indexmap::IndexMap::new(),
    );
    let mut job = test_job(&dir);
    let blocker = script_step("blocker", "sleep 5", true);
    let queued = script_step("queued", "echo should-not-run", true);
    coordinator.start_background_step(
        blocker,
        job.clone(),
        std::collections::HashMap::new(),
        "blocker".to_string(),
        1,
        None,
    );
    // Let the blocker take the only slot.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    coordinator.start_background_step(
        queued,
        job.clone(),
        std::collections::HashMap::new(),
        "queued".to_string(),
        2,
        None,
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut step_ctx = StepContext::new(
        &mut job,
        "cancel-queued".to_string(),
        "cancel-queued".to_string(),
    );
    coordinator
        .run_control_flow(&mut step_ctx, "cancel", &["queued".to_string()])
        .await;

    // The queued step is abandoned before start — official faults the
    // slot-wait task via the per-step token; no IN_PROGRESS, no side effects.
    let outcome = coordinator
        .entries
        .get("queued")
        .and_then(|e| e.outcome.as_ref())
        .expect("outcome set");
    assert!(
        !outcome.started,
        "queued step must be abandoned before start"
    );
    let queued_updates = {
        let q = queue.lock().await;
        q.all_queued_updates()
            .iter()
            .filter(|u| u.name == "queued")
            .count()
    };
    assert_eq!(
        queued_updates, 0,
        "abandoned step must not emit timeline updates"
    );

    // Clean up the still-running blocker.
    coordinator.drain().await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

#[tokio::test]
async fn forced_cancel_queues_terminal_update_and_result() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // Ignores SIGINT/SIGTERM; only the invoker's SIGKILL escalation can stop
    // it — well past the coordinator's grace period.
    let bg = script_step(
        "stubborn",
        "trap '' INT TERM; while true; do sleep 0.2; done",
        true,
    );
    let gap = script_step("gap", "sleep 0.3", false);
    let cancel = control_step("cancel-stubborn", "cancel", &["stubborn"]);

    let (result, queue) = run(&[bg, gap, cancel], &mut job, &dir, cancel_rx).await;

    assert_eq!(
        result, "Succeeded",
        "explicitly cancelled step must not fail the job"
    );
    assert_eq!(
        job.steps.get("stubborn").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
    let terminal_updates = {
        let q = queue.lock().await;
        q.all_queued_updates()
            .iter()
            .filter(|u| u.name == "stubborn" && u.status == step_status::COMPLETED)
            .count()
    };
    assert!(
        terminal_updates >= 1,
        "force-cancelled step must receive a terminal COMPLETED update"
    );
}

#[tokio::test]
async fn repeated_wait_does_not_duplicate_job_level_annotations() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    job.variables = serde_json::json!({
        "actions_send_job_level_annotations": { "value": "true" }
    });
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("bg", "echo '::error title=boom::kaboom'", true);
    let wait1 = control_step("wait-1", "wait", &["bg"]);
    let wait2 = control_step("wait-2", "wait", &["bg"]);

    let (result, _queue) = run(&[bg, wait1, wait2], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.job_annotations.len(),
        1,
        "repeated wait must not duplicate job-level annotations"
    );
}

#[tokio::test]
async fn wait_control_failure_respects_continue_on_error() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    let bg = script_step("fail", "exit 1", true);
    let mut wait = control_step("wait-fail", "wait", &["fail"]);
    wait.continue_on_error = true;

    let (result, _queue) = run(&[bg, wait], &mut job, &dir, cancel_rx).await;

    // The failed background step still folds into the job result via the
    // safety net (official merges every background result in
    // WaitForUnwaitedStepsAsync) — continue_on_error only softens the wait
    // control step's own recorded conclusion.
    assert_eq!(result, "Failed");
    assert_eq!(
        job.steps.get("wait-fail").map(|s| s.conclusion.as_str()),
        Some("Success")
    );
}

/// A background step that wrote no state must leave a newer foreground write
/// alone — the flush replays recorded file-command writes, so a *differing
/// private snapshot* alone (the background job's own view, which a foreground
/// write cannot update) never counts as a write.
#[test]
fn flush_step_does_not_clobber_newer_foreground_state() {
    let mut base = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    base.state.insert(
        "svc".to_string(),
        [("k".to_string(), "a".to_string())].into_iter().collect(),
    );
    // The background step never wrote state. Its private snapshot still
    // differs from the base: the value was materialized after dispatch.
    let mut private = base.clone();
    private
        .state
        .get_mut("svc")
        .unwrap()
        .insert("k".to_string(), "c".to_string());
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.state.insert(
        "svc".to_string(),
        [("k".to_string(), "b".to_string())].into_iter().collect(),
    );

    let outcome = BackgroundStepOutcome {
        started: true,
        working_job: private,
        base_job: base,
        context_name: "bg".to_string(),
        outcome: "Success".to_string(),
        conclusion: "Success".to_string(),
        log_content: String::new(),
        summary_content: String::new(),
        direct_result: None,
        deferred: crate::worker::file_commands::FileCommandWrites::default(),
    };
    flush_step(&mut job, &outcome);
    assert_eq!(
        job.state
            .get("svc")
            .and_then(|m| m.get("k"))
            .map(String::as_str),
        Some("b"),
        "newer foreground state write must survive an unchanged snapshot"
    );
}

/// A recorded state write is applied even when it restores the dispatch-time
/// value: the official deferred flush replays the step's writes, and a
/// foreground step that wrote the same key while the background step ran must
/// not win by comparison accident.
#[test]
fn flush_step_replays_state_writes_that_equal_the_dispatch_value() {
    let mut base = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    base.state.insert(
        "svc".to_string(),
        [("k".to_string(), "a".to_string())].into_iter().collect(),
    );
    // The step's own snapshot still reads the dispatch-time value.
    let private = base.clone();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    // A foreground step wrote the same key while the background step ran.
    job.state.insert(
        "svc".to_string(),
        [("k".to_string(), "foreground".to_string())]
            .into_iter()
            .collect(),
    );

    let outcome = BackgroundStepOutcome {
        started: true,
        working_job: private,
        base_job: base,
        context_name: "bg".to_string(),
        outcome: "Success".to_string(),
        conclusion: "Success".to_string(),
        log_content: String::new(),
        summary_content: String::new(),
        direct_result: None,
        deferred: crate::worker::file_commands::FileCommandWrites {
            state: vec![("svc".to_string(), "k".to_string(), "a".to_string())],
            ..Default::default()
        },
    };
    flush_step(&mut job, &outcome);
    assert_eq!(
        job.state
            .get("svc")
            .and_then(|m| m.get("k"))
            .map(String::as_str),
        Some("a"),
        "a recorded write must be replayed even when it equals the dispatch-time value"
    );
}

// ---------------------------------------------------------------------
// Regressions: harvesting unjoined handles, cancellation-preserving
// file-command failures, control-target normalisation, and the replayed
// deferred writes.
// ---------------------------------------------------------------------

/// A `wait` whose `select!` loses to a job cancellation, or a `cancel` control
/// whose target finished between the grace-period check and the flush, leaves
/// a finished-but-unjoined handle behind. `complete_waited_steps` must join it
/// before marking the step completed, or its deferred state and result are
/// dropped and the safety net (which only looks at unwaited ids) skips it.
#[tokio::test]
async fn complete_waited_steps_harvests_a_finished_but_unjoined_step() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let mut coordinator = BackgroundStepCoordinator::new(
        queue.clone(),
        None,
        dir.path().to_string_lossy().into_owned(),
        cancel_rx,
        10,
        job.steps.clone(),
    );

    let step = script_step(
        "bg",
        "echo \"BG_ENV=from-background\" >> \"$GITHUB_ENV\"; sleep 0.3",
        true,
    );
    coordinator.start_background_step(
        step,
        job.clone(),
        std::collections::HashMap::new(),
        "bg".to_string(),
        1,
        None,
    );

    // Nobody joins the task: this is the state a job-cancel-interrupted wait
    // leaves behind once the step has finished.
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(
        coordinator
            .entries
            .get("bg")
            .is_some_and(|entry| entry.outcome.is_none() && entry.handle.is_some()),
        "the entry must still be unjoined before the flush"
    );

    let merged = coordinator
        .complete_waited_steps(&["bg".to_string()], &mut job)
        .await;

    assert_eq!(merged, "Success");
    assert_eq!(
        job.env.get("BG_ENV").map(String::as_str),
        Some("from-background"),
        "the harvested step's deferred environment must reach the job"
    );
    assert!(
        job.steps.contains_key("bg"),
        "the harvested step's result must reach the job"
    );
}

/// A `cancel` control step whose target already finished must still flush that
/// target's result — including a failure — instead of reporting it Unknown
/// and letting the job succeed.
#[tokio::test]
async fn cancel_control_flushes_a_completed_failed_step() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);
    let bg = script_step("bg", "exit 7", true);
    let cancel = control_step("kill", "cancel", &["bg"]);
    // Give the background step a head start so it finishes before the cancel
    // control runs; the control must still observe and merge its failure.
    let fg = script_step("settle", "sleep 0.4", false);

    let (result, _queue) = run(&[bg, fg, cancel], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Failed", "a completed failed step must not vanish");
    let step_result = job.steps.get("bg").expect("bg result must be merged");
    assert_eq!(step_result.conclusion, "Failure");
}

/// A cancelled background step whose `$GITHUB_OUTPUT` is incomplete (the usual
/// shape after a kill) keeps its `Cancelled` conclusion: the file-command
/// failure must not turn it into a job-failing `Failure` (#4482).
#[test]
fn file_command_failure_preserves_cancellation() {
    assert_eq!(
        file_command_failure_conclusion("Cancelled", false),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    assert_eq!(
        file_command_failure_conclusion("Cancelled", true),
        ("Cancelled".to_string(), "Cancelled".to_string())
    );
    assert_eq!(
        file_command_failure_conclusion("Success", false),
        ("Failure".to_string(), "Failure".to_string())
    );
    assert_eq!(
        file_command_failure_conclusion("Success", true),
        ("Failure".to_string(), "Success".to_string())
    );
}

/// End-to-end: an explicitly cancelled step that leaves a broken
/// `$GITHUB_OUTPUT` behind does not fail the job.
#[tokio::test]
async fn cancelled_step_with_broken_output_file_does_not_fail_the_job() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);
    // Write an unterminated heredoc to $GITHUB_OUTPUT, prove the write landed,
    // then stay alive long enough for the cancel control to kill the process.
    let bg = script_step(
        "bg",
        "printf 'K<<EOF\\nv' >> \"$GITHUB_OUTPUT\"; touch bg_wrote_output; sleep 30",
        true,
    );
    // Bounded wait for the background write, so the cancel cannot win the race.
    let fg = script_step(
        "settle",
        "for _ in $(seq 1 200); do [ -f bg_wrote_output ] && exit 0; sleep 0.05; done; exit 1",
        false,
    );
    let cancel = control_step("kill", "cancel", &["bg"]);

    let (result, _queue) = run(&[bg, fg, cancel], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    let step_result = job.steps.get("bg").expect("bg result must be merged");
    assert_eq!(step_result.conclusion, "Cancelled");
}

/// A write that restores the dispatch-time value is still replayed at flush
/// time — the first half of the official deferred-environment contract (the
/// second half, that an unwritten key leaves the foreground value alone, is
/// pinned by `flush_step_does_not_clobber_newer_foreground_state`).
#[test]
fn flush_step_replays_recorded_writes_equal_to_the_dispatch_value() {
    let mut base = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    base.env.insert("FOO".to_string(), "dispatch".to_string());
    base.extra_path.push("/base/bin".to_string());
    let private = base.clone();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    // A foreground step wrote both while the background step ran.
    job.env.insert("FOO".to_string(), "foreground".to_string());
    job.extra_path.push("/foreground/bin".to_string());

    let outcome = BackgroundStepOutcome {
        started: true,
        working_job: private,
        base_job: base,
        context_name: "bg".to_string(),
        outcome: "Success".to_string(),
        conclusion: "Success".to_string(),
        log_content: String::new(),
        summary_content: String::new(),
        direct_result: None,
        deferred: crate::worker::file_commands::FileCommandWrites {
            env: vec![("FOO".to_string(), "dispatch".to_string())],
            path: vec!["/bg/bin".to_string()],
            ..Default::default()
        },
    };
    flush_step(&mut job, &outcome);

    assert_eq!(
        job.env.get("FOO").map(String::as_str),
        Some("dispatch"),
        "a recorded write equal to the dispatch-time value must still be replayed"
    );
    assert_eq!(
        job.extra_path.first().map(String::as_str),
        Some("/bg/bin"),
        "the step's path entry must be prepended, newest batch first"
    );
    assert!(
        job.extra_path
            .iter()
            .any(|entry| entry == "/foreground/bin"),
        "the foreground path entry must survive"
    );
}

/// A step that wrote nothing leaves the foreground values alone, even when its
/// private snapshot differs (a foreground write cannot reach the private job).
#[test]
fn flush_step_without_recorded_writes_keeps_foreground_env_and_path() {
    let mut base = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    base.env.insert("FOO".to_string(), "dispatch".to_string());
    let mut private = base.clone();
    private
        .env
        .insert("FOO".to_string(), "snapshot-only".to_string());
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.env.insert("FOO".to_string(), "foreground".to_string());
    job.extra_path.push("/foreground/bin".to_string());

    let outcome = BackgroundStepOutcome {
        started: true,
        working_job: private,
        base_job: base,
        context_name: "bg".to_string(),
        outcome: "Success".to_string(),
        conclusion: "Success".to_string(),
        log_content: String::new(),
        summary_content: String::new(),
        direct_result: None,
        deferred: crate::worker::file_commands::FileCommandWrites::default(),
    };
    flush_step(&mut job, &outcome);

    assert_eq!(job.env.get("FOO").map(String::as_str), Some("foreground"));
    assert_eq!(job.extra_path, vec!["/foreground/bin".to_string()]);
}

/// The official `Pipelines.BackgroundControlTypes.WaitAll` spelling works
/// end-to-end, alongside the tolerated hyphenated form.
#[tokio::test]
async fn wait_all_accepts_the_official_control_type_spelling() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);
    let bg = script_step("bg", "echo hi", true);
    let wait_all = control_step("wait", "waitAll", &[]);

    let (result, _queue) = run(&[bg, wait_all], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("bg").map(|result| result.conclusion.as_str()),
        Some("Success")
    );
}

/// Background updates carry the official timeline metadata: a background step
/// reports `isBackground` plus the wire's `parallelGroupId`; a control step
/// reports `backgroundControlType` and the *external ids* of its targets.
#[tokio::test]
async fn timeline_updates_carry_background_metadata() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);
    let mut bg = script_step("bg", "echo hi", true);
    bg.raw = serde_json::json!({ "parallelGroupId": "group-1" });
    let bg_id = bg.id.clone();
    let mut wait = control_step("wait", "waitAll", &["bg"]);
    wait.raw = serde_json::json!({ "parallelGroupId": "group-2" });
    let wait_id = wait.id.clone();

    let (result, queue) = run(&[bg, wait], &mut job, &dir, cancel_rx).await;
    assert_eq!(result, "Succeeded");

    let (body, _) = queue
        .lock()
        .await
        .take_steps_update_body()
        .expect("step updates must be queued");
    let background = body
        .steps
        .iter()
        .find(|update| update.external_id == bg_id)
        .expect("the background step must be reported");
    assert_eq!(background.is_background, Some(true));
    assert_eq!(background.parallel_group_id.as_deref(), Some("group-1"));

    let control = body
        .steps
        .iter()
        .find(|update| update.external_id == wait_id)
        .expect("the control step must be reported");
    assert_eq!(control.background_control_type.as_deref(), Some("waitAll"));
    assert_eq!(control.background_control_step_ids, vec![bg_id.clone()]);
    assert_eq!(control.parallel_group_id.as_deref(), Some("group-2"));

    let serialized = serde_json::to_value(&body).unwrap();
    let background_json = serialized["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["external_id"] == serde_json::json!(background.external_id))
        .unwrap();
    // The Twirp body keeps the snake_case spelling of its sibling fields; the
    // AzDO timeline record (see `worker::reporting` tests) carries the
    // official camelCase names.
    assert_eq!(background_json["is_background"], serde_json::json!(true));
    assert_eq!(
        background_json["parallel_group_id"],
        serde_json::json!("group-1")
    );
    let control_json = serialized["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["external_id"] == serde_json::json!(control.external_id))
        .unwrap();
    assert_eq!(
        control_json["background_control_type"],
        serde_json::json!("waitAll")
    );
    assert_eq!(
        control_json["background_control_step_ids"],
        serde_json::json!([bg_id])
    );
    // An ordinary step carries none of the metadata keys.
    let ordinary = serialized["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == serde_json::json!("Set up job"))
        .unwrap();
    assert!(ordinary.get("is_background").is_none());
    assert!(ordinary.get("background_control_type").is_none());
    assert!(ordinary.get("background_control_step_ids").is_none());
    assert!(ordinary.get("parallel_group_id").is_none());
}

/// Job-level `env:` templates are evaluated for background steps exactly like
/// foreground steps: the server pre-resolves most of them, but runtime-only
/// keys keep their `${{ }}` and must never reach the background process
/// unresolved. Step env still wins over an evaluated job value.
#[tokio::test]
async fn background_step_evaluates_job_level_env_templates() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    job.env.insert(
        "JOB_TEMPLATE".to_string(),
        "${{ 'from-template' }}".to_string(),
    );

    let (_tx, cancel_rx) = watch::channel(false);
    let mut bg = script_step(
        "bg",
        "[ \"$JOB_TEMPLATE\" = 'from-template' ] && echo \"STEP_WINS=job\" >> \"$GITHUB_ENV\"",
        true,
    );
    // The step's own value overrides the evaluated job value for the same key.
    bg.step_type = StepType::Script {
        script: "[ \"$JOB_TEMPLATE\" = 'from-step' ]".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    bg.env
        .insert("JOB_TEMPLATE".to_string(), "${{ 'from-step' }}".to_string());

    let (result, _queue) = run(&[bg], &mut job, &dir, cancel_rx).await;

    assert_eq!(result, "Succeeded");
}
