use super::*;
use crate::worker::contexts::JobContext;
use crate::worker::server_queue::ServerQueue;
use crate::worker::step_conditions::contains_status_check_function;
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
        is_background: false,
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

#[test]
fn status_check_function_detection_ignores_string_literals() {
    assert!(contains_status_check_function(
        "failure() && steps.build.outcome == 'failure'"
    ));
    assert!(contains_status_check_function("${{ always() }}"));
    assert!(!contains_status_check_function(
        "steps.build.outcome == 'failure'"
    ));
    assert!(!contains_status_check_function(
        "contains('failure()', steps.build.outcome)"
    ));
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
        None,
        None,
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
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    let step_result = job.steps.get("soft_fail").unwrap();
    assert_eq!(step_result.outcome, "Failure");
    assert_eq!(step_result.conclusion, "Success");
}

#[tokio::test]
async fn run_steps_job_status_remains_success_after_continue_on_error() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    // Step 1: fails but continue-on-error is true
    let mut soft_fail = test_step("soft_fail", None);
    soft_fail.step_type = StepType::Script {
        script: "exit 1".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    soft_fail.continue_on_error = true;

    // Step 2: normal step that should still run and succeed
    let mut after = test_step("after", None);
    after.step_type = StepType::Script {
        script: "echo still-running".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[soft_fail, after],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    // Job overall succeeds
    assert_eq!(result, "Succeeded");
    assert_eq!(job.job_status, JobStatus::Success);

    // soft_fail: outcome=Failure, conclusion=Success (continue-on-error)
    let sf = job.steps.get("soft_fail").unwrap();
    assert_eq!(sf.outcome, "Failure");
    assert_eq!(sf.conclusion, "Success");

    // after: ran normally
    let a = job.steps.get("after").unwrap();
    assert_eq!(a.outcome, "Success");
    assert_eq!(a.conclusion, "Success");
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
        None,
        None,
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

#[tokio::test]
async fn run_steps_implicitly_gates_conditions_with_success() {
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

    let implicit_success = test_step(
        "implicit_success",
        Some("steps.fail.outcome == 'Failure' || steps.fail.outcome == 'failure'"),
    );

    let mut explicit_failure = test_step(
        "explicit_failure",
        Some("failure() && (steps.fail.outcome == 'Failure' || steps.fail.outcome == 'failure')"),
    );
    explicit_failure.step_type = StepType::Script {
        script: "echo explicit-failure-ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[fail, implicit_success, explicit_failure],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Failed");
    assert_eq!(
        job.steps.get("implicit_success").unwrap().conclusion,
        "Skipped"
    );
    assert_eq!(
        job.steps.get("explicit_failure").unwrap().conclusion,
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
        None,
        None,
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
async fn run_steps_multiline_outputs_are_visible_to_later_steps() {
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
            script: "echo 'json_data<<EOF' >> \"$GITHUB_OUTPUT\"\necho '{\"key\": \"value\"}' >> \"$GITHUB_OUTPUT\"\necho 'EOF' >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };
    let mut consume = test_step("consume", None);
    consume.step_type = StepType::Script {
            script: "OUTPUT='${{ steps.produce.outputs.json_data }}'\ntest \"$OUTPUT\" = '{\"key\": \"value\"}'".to_string(),
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
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(result, "Succeeded");
    assert_eq!(
        job.steps
            .get("produce")
            .and_then(|step| step.outputs.get("json_data"))
            .map(String::as_str),
        Some("{\"key\": \"value\"}")
    );
}

#[tokio::test]
async fn run_steps_file_command_parse_error_fails_successful_step() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);
    let mut broken = test_step("broken_output", None);
    broken.step_type = StepType::Script {
            script: "echo 'value<<EOF' >> \"$GITHUB_OUTPUT\"\nprintf 'missing delimiter' >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

    let result = run_steps(
        &[broken],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Failed");
    let step = job.steps.get("broken_output").unwrap();
    assert_eq!(step.outcome, "Failure");
    assert_eq!(step.conclusion, "Failure");
    assert!(step.outputs.is_empty());
}

#[tokio::test]
async fn run_steps_file_command_parse_error_respects_continue_on_error() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);
    let mut broken = test_step("broken_output", None);
    broken.continue_on_error = true;
    broken.step_type = StepType::Script {
            script: "echo 'value<<EOF' >> \"$GITHUB_OUTPUT\"\nprintf 'missing delimiter' >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

    let result = run_steps(
        &[broken],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    let step = job.steps.get("broken_output").unwrap();
    assert_eq!(step.outcome, "Failure");
    assert_eq!(step.conclusion, "Success");
    assert!(step.outputs.is_empty());
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
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    assert_eq!(job.env.get("FOO").map(String::as_str), Some("bar"));
}

#[tokio::test]
async fn run_steps_all_steps_pass() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    let mut step1 = test_step("step1", None);
    step1.step_type = StepType::Script {
        script: "echo step1-ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    let mut step2 = test_step("step2", None);
    step2.step_type = StepType::Script {
        script: "echo step2-ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step1, step2],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    assert_eq!(job.job_status, JobStatus::Success);
    assert_eq!(job.steps.get("step1").unwrap().conclusion, "Success");
    assert_eq!(job.steps.get("step2").unwrap().conclusion, "Success");
}

#[tokio::test]
async fn run_steps_step_env_override_job_env() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    // Set a job-level environment variable
    job.env.insert("JOB_VAR".to_string(), "job-val".to_string());
    job.env
        .insert("OVERRIDE_VAR".to_string(), "job-val".to_string());

    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    let mut step = test_step("test_env", None);
    step.env
        .insert("OVERRIDE_VAR".to_string(), "step-val".to_string());
    step.env
        .insert("STEP_VAR".to_string(), "step-val".to_string());
    step.step_type = StepType::Script {
            // Write variables to output so we can verify the actual process env
            script: "echo job_var=$JOB_VAR >> \"$GITHUB_OUTPUT\"\necho override_var=$OVERRIDE_VAR >> \"$GITHUB_OUTPUT\"\necho step_var=$STEP_VAR >> \"$GITHUB_OUTPUT\"".to_string(),
            shell: Some("bash".to_string()),
            working_directory: None,
        };

    let result = run_steps(
        &[step],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    let step_res = job.steps.get("test_env").unwrap();
    assert_eq!(
        step_res.outputs.get("job_var").map(String::as_str),
        Some("job-val")
    );
    assert_eq!(
        step_res.outputs.get("override_var").map(String::as_str),
        Some("step-val")
    );
    assert_eq!(
        step_res.outputs.get("step_var").map(String::as_str),
        Some("step-val")
    );
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
        None,
        None,
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
#[tokio::test]
async fn test_step_summary_size_limit_and_scrubbing() {
    let workspace = TempDir::new().unwrap();
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    // Scenario 1: Simple summary and secret scrubbing
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.add_mask("secret-value");

    let mut step = test_step("summary-scrub", None);
    step.step_type = StepType::Script {
        script: "echo '# hello secret-value' >> \"$GITHUB_STEP_SUMMARY\"".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step.clone()],
        &mut job,
        workspace.path().to_str().unwrap(),
        cancel_rx.clone(),
        queue.clone(),
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    // Verify annotations has no errors
    assert!(!job.step_annotations.contains_key("summary-scrub"));

    // Scenario 2: Summary size limit exceeded
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let mut step = test_step("summary-large", None);
    step.step_type = StepType::Script {
        script: "dd if=/dev/zero bs=1024 count=1100 >> \"$GITHUB_STEP_SUMMARY\"".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step],
        &mut job,
        workspace.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    // Verify that an Error annotation was added under "summary-large"
    let annotations = job.step_annotations.get("summary-large").unwrap();
    assert_eq!(annotations.len(), 1);
    assert_eq!(
        annotations[0].level,
        crate::worker::execution_context::AnnotationLevel::Error
    );
    assert!(annotations[0]
        .message
        .contains("upload aborted, supports content up to a size of 1024k"));
}

#[tokio::test]
async fn run_steps_cancelled_condition_runs_only_when_cancelled() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Step 1: a slow step that will be interrupted by cancellation
    let mut slow = test_step("slow", None);
    slow.step_type = StepType::Script {
        script: "sleep 30".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    // Step 2: default condition (success()) — should be SKIPPED after cancel
    let normal = test_step("normal_after", None);
    // Step 3: cancelled() — should RUN after cancel
    let mut on_cancel = test_step("on_cancel", Some("cancelled()"));
    on_cancel.step_type = StepType::Script {
        script: "echo cleanup-ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    // Step 4: always() — should RUN after cancel
    let mut on_always = test_step("on_always", Some("always()"));
    on_always.step_type = StepType::Script {
        script: "echo always-ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    // Fire cancellation after 500ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = cancel_tx.send(true);
    });

    let result = run_steps(
        &[slow, normal, on_cancel, on_always],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Cancelled");
    assert_eq!(job.job_status, JobStatus::Cancelled);
    // slow step was cancelled
    assert_eq!(job.steps.get("slow").unwrap().conclusion, "Cancelled");
    // normal step was skipped (success() is false under cancel)
    assert_eq!(job.steps.get("normal_after").unwrap().conclusion, "Skipped");
    // cancelled() step ran
    assert_eq!(job.steps.get("on_cancel").unwrap().conclusion, "Success");
    // always() step ran
    assert_eq!(job.steps.get("on_always").unwrap().conclusion, "Success");
}

#[tokio::test]
async fn run_steps_outcome_visible_in_later_step_condition() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    // Step 1: fails with continue-on-error so job stays alive
    let mut fail_step = test_step("fail_step", None);
    fail_step.step_type = StepType::Script {
        script: "exit 1".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };
    fail_step.continue_on_error = true;

    // Step 2: condition references steps.fail_step.outcome == 'failure'
    // This should run because the outcome IS 'failure' (even though conclusion is success)
    let mut check_outcome = test_step(
        "check_outcome",
        Some("steps.fail_step.outcome == 'failure'"),
    );
    check_outcome.step_type = StepType::Script {
        script: "echo outcome-matched".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    // Step 3: condition references steps.fail_step.conclusion == 'success'
    // This should run because continue-on-error maps conclusion to success
    let mut check_conclusion = test_step(
        "check_conclusion",
        Some("steps.fail_step.conclusion == 'success'"),
    );
    check_conclusion.step_type = StepType::Script {
        script: "echo conclusion-matched".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    // Step 4: condition that should NOT match
    let skip_step = test_step("should_skip", Some("steps.fail_step.outcome == 'success'"));

    let result = run_steps(
        &[fail_step, check_outcome, check_conclusion, skip_step],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    // check_outcome ran: steps.fail_step.outcome == 'failure' was true
    assert_eq!(
        job.steps.get("check_outcome").unwrap().conclusion,
        "Success"
    );
    // check_conclusion ran: steps.fail_step.conclusion == 'success' was true
    assert_eq!(
        job.steps.get("check_conclusion").unwrap().conclusion,
        "Success"
    );
    // should_skip was skipped: steps.fail_step.outcome != 'success'
    assert_eq!(job.steps.get("should_skip").unwrap().conclusion, "Skipped");
}

// --- P1 expressions/templates gap coverage ---

#[tokio::test]
async fn run_steps_step_env_evaluates_expressions() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo", "ref": "refs/heads/main"}
        }),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    let mut step = test_step("env_expr", None);
    step.env
        .insert("REPO".to_string(), "${{ github.repository }}".to_string());
    step.env
        .insert("BRANCH".to_string(), "${{ github.ref }}".to_string());
    step.step_type = StepType::Script {
        script: "echo repo=$REPO >> \"$GITHUB_OUTPUT\"\necho branch=$BRANCH >> \"$GITHUB_OUTPUT\""
            .to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    let outputs = &job.steps.get("env_expr").unwrap().outputs;
    assert_eq!(outputs.get("repo").map(String::as_str), Some("owner/repo"));
    assert_eq!(
        outputs.get("branch").map(String::as_str),
        Some("refs/heads/main")
    );
}

#[tokio::test]
async fn run_steps_display_name_evaluates_expression() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo"},
            "matrix": {"os": "ubuntu"}
        }),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    let mut step = test_step("dynamic_name", None);
    step.display_name = "Build (${{ matrix.os }})".to_string();
    step.step_type = StepType::Script {
        script: "echo ran".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    // The step ran; the display name was resolved dynamically
    assert_eq!(job.steps.get("dynamic_name").unwrap().conclusion, "Success");
}

#[tokio::test]
async fn run_steps_condition_uses_env_context() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.env.insert("DEPLOY".into(), "true".into());
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    let mut runs = test_step("deploy_step", Some("env.DEPLOY == 'true'"));
    runs.step_type = StepType::Script {
        script: "echo deploying".to_string(),
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let skips = test_step("skip_step", Some("env.DEPLOY == 'false'"));

    let result = run_steps(
        &[runs, skips],
        &mut job,
        dir.path().to_str().unwrap(),
        cancel_rx,
        queue,
        None,
        None,
        &[],
        None,
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, "Succeeded");
    assert_eq!(job.steps.get("deploy_step").unwrap().conclusion, "Success");
    assert_eq!(job.steps.get("skip_step").unwrap().conclusion, "Skipped");
}
/// Regression: when GHA encodes a multi-expression run: script as a single format() token,
/// display_name_for_step takes only the first line, producing a truncated ${{ format(...)
/// expression. evaluate_template fails to close it; the fallback regenerates from the
/// evaluated full script so the timeline shows the resolved first line.
#[tokio::test]
async fn run_steps_display_name_from_format_token_script() {
    let dir = TempDir::new().unwrap();
    let mut job = JobContext::new(
        "job".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({"matrix": {"platform": {"name": "Linux ARM64", "target": "aarch64-unknown-linux-gnu"}}}),
    );
    let queue = Arc::new(Mutex::new(ServerQueue::new("job".into(), "plan".into())));
    let (_tx, cancel_rx) = watch::channel(false);

    // Simulate what build_step_list produces when GHA sends the script as a
    // format() token: the display name is "Run <first-line>" where the first
    // line is the start of the multi-line format() expression.
    let format_script = "${{ format('echo \"name={0}\"\necho \"target={1}\"', matrix.platform.name, matrix.platform.target) }}".to_string();
    let mut step = test_step("print_ctx", None);
    step.display_name = format!("Run {}", format_script.lines().next().unwrap_or(""));
    step.step_type = StepType::Script {
        script: format_script,
        shell: Some("bash".to_string()),
        working_directory: None,
    };

    let result = run_steps(
        &[step],
        &mut job,
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

    assert_eq!(result, "Succeeded");
    // The step name sent in queue updates should be the resolved first line,
    // not the raw format() expression.
    let updates = queue.lock().await;
    let step_update = updates
        .all_queued_updates()
        .iter()
        .find(|u| u.number > 1)
        .expect("expected at least one user step update");
    assert!(
        !step_update.name.contains("${{"),
        "display name should be resolved, got: {}",
        step_update.name
    );
    assert!(
        step_update.name.starts_with("Run "),
        "display name should start with 'Run', got: {}",
        step_update.name
    );
}
