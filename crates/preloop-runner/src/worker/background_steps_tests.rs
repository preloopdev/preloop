//! Tests for the background step coordinator: concurrency with the main
//! loop, wait/wait-all/cancel control steps, cancellation propagation,
//! deferred state merging, failure folding, and the post-job safety net.

use super::*;
use crate::worker::contexts::JobContext;
use crate::worker::server_queue::ServerQueue;
use crate::worker::steps_runner::{run_steps, StepType};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::{watch, Mutex};

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
async fn cancel_control_step_terminates_background_step_and_job_stays_succeeded() {
    let dir = TempDir::new().unwrap();
    let mut job = test_job(&dir);
    let (_tx, cancel_rx) = watch::channel(false);

    // A long-running step that only cancellation can stop.
    let bg = script_step("server", "sleep 60", true);
    let cancel = control_step("cancel-server", "cancel", &["server"]);

    let (result, queue) = run(&[bg, cancel], &mut job, &dir, cancel_rx).await;

    // #4482: an explicitly cancelled background step must not flip the job.
    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps.get("server").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
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
    assert_eq!(
        job.steps.get("server").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
    assert_eq!(
        job.steps.get("wait-server").map(|s| s.conclusion.as_str()),
        Some("Cancelled")
    );
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
            assert_eq!(control_type, "wait-all");
            assert_eq!(step_ids, &["db".to_string()]);
        }
        other => panic!("expected wait-all control step, got {other:?}"),
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
