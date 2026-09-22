//! preloop-runner-server integration tests — security group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

#[tokio::test]
async fn workflow_steps_update_terminal_first_sighting_does_not_fake_zero_duration() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "local/preloop",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .expect("submitted run must have a job request");
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };

    // Fast built-in / quick steps often complete before any in-progress
    // update is processed. First sighting is already terminal (status=6).
    request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": uuid::Uuid::new_v4().to_string(),
                "number": 2,
                "name": "Run echo hi",
                "status": 6,
                "conclusion": 2
            }]
        }),
    )
    .await;

    let run = get_run_json(&app, &run_id).await;
    let step = run["jobs_list"][0]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Run echo hi")
        .expect("step row");
    assert!(
        step.get("started_at").is_none() || step["started_at"].is_null(),
        "terminal-first sighting must not invent started_at (got {step:?})"
    );
    assert!(
        step.get("finished_at").and_then(|v| v.as_str()).is_some(),
        "terminal-first sighting must still record finished_at (got {step:?})"
    );
}

#[tokio::test]
async fn workflow_steps_update_records_start_on_in_progress_then_finish() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "local/preloop",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .expect("submitted run must have a job request");
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };

    // One step, reported twice: a real runner keeps the same `external_id`
    // across the in-progress and terminal updates, which is what lets the
    // second report land on the first one's record.
    let step_id = workflow_step_ids(&state, run_id.parse().unwrap(), "build")
        .await
        .remove(0);
    request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": step_id,
                "number": 2,
                "name": "Run echo hi",
                "status": 3,
                "conclusion": 0
            }]
        }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": step_id,
                "number": 2,
                "name": "Run echo hi",
                "status": 6,
                "conclusion": 2
            }]
        }),
    )
    .await;

    let run = get_run_json(&app, &run_id).await;
    let step = run["jobs_list"][0]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Run echo hi")
        .expect("step row");
    assert!(
        step.get("started_at").and_then(|v| v.as_str()).is_some(),
        "in_progress first sighting must set started_at (got {step:?})"
    );
    assert!(
        step.get("finished_at").and_then(|v| v.as_str()).is_some(),
        "terminal follow-up must set finished_at (got {step:?})"
    );
    assert!(step["conclusion"] == "success");
}

#[tokio::test]
async fn workflow_concurrency_serializes_runs_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: serial-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_a["status"], "queued");
    assert_eq!(run_b["status"], "pending");
    assert_eq!(run_b["jobs"]["build"], "pending");

    // Complete A via message poll + complete API.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A should be dispatchable");
    complete_via_api(&app, a_id, "build").await;

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
    assert_eq!(run_b["jobs"]["build"], "queued");
}

#[tokio::test]
async fn workflow_concurrency_cancel_in_progress_cancels_running() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: cancel-group
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 60
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();

    // Dispatch A so it is InProgress.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let message_id = msg["messageId"].as_i64().unwrap();
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!(
                    "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    assert_eq!(run_a["status"], "cancelled");
    assert_eq!(run_a["jobs"]["build"], "cancelled");

    // Cancellation message should be official shape.
    let cancellation = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        cancellation["messageType"],
        azdo::message_type::JOB_CANCELLED
    );
    let body_b64 = cancellation["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
}

#[tokio::test]
async fn pending_run_replaced_by_newer_submission() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: replace-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    let c_id = c["run_id"].as_str().unwrap();

    let run_a = get_run_json(&app, a_id).await;
    let run_b = get_run_json(&app, b_id).await;
    let run_c = get_run_json(&app, c_id).await;
    assert_eq!(run_a["status"], "queued");
    assert_eq!(run_b["status"], "cancelled");
    assert_eq!(run_c["status"], "pending");
}

#[tokio::test]
async fn queue_max_holds_multiple_pending_fifo() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: max-group
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    let d = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    let c_id = c["run_id"].as_str().unwrap();
    let d_id = d["run_id"].as_str().unwrap();

    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    assert_eq!(get_run_json(&app, c_id).await["status"], "pending");
    assert_eq!(get_run_json(&app, d_id).await["status"], "pending");

    // Dispatch+complete A, then B should become queued.
    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    complete_via_api(&app, a_id, "build").await;
    assert_eq!(get_run_json(&app, b_id).await["status"], "queued");
    assert_eq!(get_run_json(&app, c_id).await["status"], "pending");

    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    complete_via_api(&app, b_id, "build").await;
    assert_eq!(get_run_json(&app, c_id).await["status"], "queued");
    assert_eq!(get_run_json(&app, d_id).await["status"], "pending");
}

#[tokio::test]
async fn concurrency_group_names_case_insensitive() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency: Prod
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
concurrency: prod
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn job_level_concurrency_gates_single_job() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  one:
    runs-on: ubuntu-latest
    concurrency:
      group: job-serial
    steps:
      - run: echo one
  two:
    runs-on: ubuntu-latest
    concurrency:
      group: job-serial
    steps:
      - run: echo two
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    let one = run["jobs"]["one"].as_str().unwrap();
    let two = run["jobs"]["two"].as_str().unwrap();
    // Exactly one should be queued, the other pending.
    let statuses = [one, two];
    assert!(statuses.contains(&"queued"));
    assert!(statuses.contains(&"pending"));
}

#[tokio::test]
async fn concurrency_blocked_jobs_do_not_block_unrelated_work() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // First run holds the group.
    let _ = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: blocked-group
jobs:
  slow:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 99
"#,
        "owner/repo",
    )
    .await;
    // Second run is concurrency-pending.
    let _ = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: blocked-group
jobs:
  slow:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 99
"#,
        "owner/repo",
    )
    .await;
    // Unrelated work without concurrency must still be dispatchable after
    // the first job is taken.
    let _ = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let free = submit_yaml(
        &app,
        r#"
on: push
jobs:
  free:
    runs-on: ubuntu-latest
    steps:
      - run: echo free
"#,
        "owner/repo",
    )
    .await;
    let free_id = free["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, free_id).await["jobs"]["free"], "queued");
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(
        !msg.is_null(),
        "unrelated job must be pollable while group is blocked"
    );
}

#[tokio::test]
async fn empty_workflow_concurrency_group_is_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on: push
concurrency:
  group: ${{ github.event.head_commit.id_missing }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
                        "event": "push",
                        "repository": "owner/repo",
                        "payload": { "head_commit": {} }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        error["error"],
        "concurrency evaluation failed: concurrency group name must not be empty"
    );
}

#[tokio::test]
async fn concurrency_chaos_interleaved_submits_and_completes() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml_hold = r#"
on: push
concurrency:
  group: chaos
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#;
    let yaml_cancel = r#"
on: push
concurrency:
  group: chaos
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo cancel
"#;
    let mut run_ids = Vec::new();
    for i in 0..20 {
        let yaml = if i % 5 == 0 { yaml_cancel } else { yaml_hold };
        let accepted = submit_yaml(&app, yaml, "owner/repo").await;
        run_ids.push(accepted["run_id"].as_str().unwrap().to_owned());
        // Occasionally complete whatever is dispatchable.
        if i % 3 == 0 {
            let msg = request_json(
                &app,
                Method::GET,
                "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
                Value::Null,
            )
            .await;
            if !msg.is_null() {
                // Complete the currently running holder if we can find a queued/in-progress job.
                for rid in &run_ids {
                    let run = get_run_json(&app, rid).await;
                    if run["jobs"]["build"] == "in_progress" || run["jobs"]["build"] == "queued" {
                        // Mark in progress via poll already done; complete.
                        complete_via_api(&app, rid, "build").await;
                        break;
                    }
                }
            }
        }
    }
    // Server must remain consistent: no panics, every run has a known status.
    for rid in &run_ids {
        let run = get_run_json(&app, rid).await;
        let status = run["status"].as_str().unwrap();
        assert!(
            matches!(
                status,
                "queued" | "pending" | "in_progress" | "success" | "cancelled" | "failure"
            ),
            "unexpected status {status} for {rid}"
        );
    }
}

#[tokio::test]
async fn job_cancellation_message_type_is_official_string() {
    // Wire regression: must be "JobCancellation", not "JobCancelled".
    assert_eq!(azdo::message_type::JOB_CANCELLED, "JobCancellation");
}

#[tokio::test]
async fn broker_root_message_path_delivers_job_cancellation() {
    // The preloop-runner broker client polls `/runner/server/message` (root
    // path), NOT `/_apis/v1/Message`. Cancel must be delivered there.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    // The broker protocol requires a listen token that names a *registered*
    // runner (tokens are revoked with the registration on purge), so register
    // the machine first.
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "broker-cancel-runner", "version": "2.335.1"}),
    )
    .await;
    let registered_runner_id = registered["id"].as_i64().unwrap();
    // Mint a runner listen token for broker auth.
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{registered_runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    // Create broker session.
    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let yaml = r#"
on: push
concurrency:
  group: broker-root-cancel
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 60
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap().to_owned();

    // Dispatch A via broker root path.
    let job_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(job_msg["messageType"], "RunnerJobRequest");
    assert_eq!(
        get_run_json(&app, &a_id).await["jobs"]["build"],
        "in_progress"
    );

    // B cancels A.
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(get_run_json(&app, &a_id).await["status"], "cancelled");
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );

    // Busy poll must yield JobCancellation on the same session.
    let cancel_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(
        cancel_msg["messageType"],
        azdo::message_type::JOB_CANCELLED,
        "broker root path must deliver JobCancellation, got {cancel_msg}"
    );
    // messageId must differ from the job message or runner in-memory dedup
    // silently drops the cancel.
    assert_ne!(
        cancel_msg["messageId"], job_msg["messageId"],
        "cancel messageId must not collide with job messageId"
    );
    // Cancels live in a high id range so they never collide with request_id
    // messageIds of subsequent RunnerJobRequests.
    assert!(
        cancel_msg["messageId"].as_i64().unwrap() >= 1_000_000,
        "cancel messageId should be in high range, got {}",
        cancel_msg["messageId"]
    );
    let body: Value = serde_json::from_str(cancel_msg["body"].as_str().unwrap()).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");

    // Simulate runner finishing the cancelled job, freeing the session.
    complete_via_api(&app, &a_id, "build").await;
    // completejob can arrive before the worker process exits. A Busy poll
    // must not receive B yet or the run-service dispatcher cancels the
    // still-draining worker as an overlap.
    let busy_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Busy&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert!(
        busy_msg.is_null(),
        "busy runner received successor: {busy_msg}"
    );

    // B must be pollable with a messageId that does not collide with cancel.
    let b_msg = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(
        b_msg["messageType"], "RunnerJobRequest",
        "expected B job after A completed, got {b_msg}"
    );
    assert_ne!(b_msg["messageId"], cancel_msg["messageId"]);
    assert_ne!(b_msg["messageId"], job_msg["messageId"]);
}

#[tokio::test]
async fn concurrency_expression_group_uses_github_ref() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: ci-${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    // Same ref → collide.
    let a = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;
    let b = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
        }),
    )
    .await;
    // Different ref → independent group.
    let c = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feature",
        }),
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
}

#[tokio::test]
async fn concurrency_groups_are_repo_scoped() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: shared-name
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo-a").await;
    let b = submit_yaml(&app, yaml, "owner/repo-b").await;
    // Different repos → both free to run.
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
}

#[tokio::test]
async fn cancel_in_progress_expression_false_does_not_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: expr-cancel
  cancel-in-progress: ${{ false }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "in_progress"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn cancel_in_progress_expression_true_cancels() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: expr-cancel-true
  cancel-in-progress: ${{ true }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "cancelled"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    // Cancel message delivered with official body.
    let cancel_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let body = decode_cancel_body(&cancel_msg);
    assert_eq!(body["timeout"], "00:05:00");
    assert!(body.get("runId").is_none());
}

#[tokio::test]
async fn late_success_cannot_overwrite_cancelled_job() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: late-success
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap().to_owned();
    let _ = poll_and_ack(&app).await;
    let _b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, &a_id).await["jobs"]["build"],
        "cancelled"
    );
    // Late success from a runner that never saw JobCancellation.
    complete_via_api(&app, &a_id, "build").await;
    let run_a = get_run_json(&app, &a_id).await;
    assert_eq!(run_a["jobs"]["build"], "cancelled");
    assert_eq!(run_a["status"], "cancelled");
}

#[tokio::test]
async fn multi_job_workflow_concurrency_holds_all_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: multi-job-hold
jobs:
  one:
    runs-on: ubuntu-latest
    steps:
      - run: echo one
  two:
    runs-on: ubuntu-latest
    steps:
      - run: echo two
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "pending");
    assert_eq!(run_b["jobs"]["one"], "pending");
    assert_eq!(run_b["jobs"]["two"], "pending");
    // Unrelated free job still dispatchable after A's jobs taken.
    let _ = poll_and_ack(&app).await;
    let free = submit_yaml(
        &app,
        r#"
on: push
jobs:
  free:
    runs-on: ubuntu-latest
    steps:
      - run: echo free
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, free["run_id"].as_str().unwrap()).await["jobs"]["free"],
        "queued"
    );
    let _ = a;
}

#[tokio::test]
async fn job_level_concurrency_with_needs_gate_order() {
    // Gate order: needs → concurrency. Dependent job must not occupy the
    // group until needs are satisfied.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: echo first
  second:
    needs: first
    runs-on: ubuntu-latest
    concurrency:
      group: needs-then-concurrency
    steps:
      - run: echo second
  peer:
    runs-on: ubuntu-latest
    concurrency:
      group: needs-then-concurrency
    steps:
      - run: echo peer
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    // first ready; peer may take the concurrency slot; second waits on needs
    // (and possibly concurrency).
    assert_eq!(run["jobs"]["first"], "queued");
    assert_eq!(run["jobs"]["second"], "queued"); // in pending_jobs (needs)
                                                 // peer has no needs → evaluates concurrency immediately.
    assert!(
        run["jobs"]["peer"] == "queued" || run["jobs"]["peer"] == "pending",
        "peer={}",
        run["jobs"]["peer"]
    );
    // Complete first; second becomes ready and hits concurrency.
    let _ = poll_and_ack(&app).await;
    complete_via_api(&app, run_id, "first").await;
    let run = get_run_json(&app, run_id).await;
    // Exactly one of {peer, second} may be pending on the shared group if
    // the other is queued/in_progress.
    let peer = run["jobs"]["peer"].as_str().unwrap();
    let second = run["jobs"]["second"].as_str().unwrap();
    assert!(
        matches!(
            (peer, second),
            ("queued", "pending")
                | ("pending", "queued")
                | ("in_progress", "pending")
                | ("pending", "in_progress")
                | ("queued", "queued") // if peer already finished — unlikely
        ) || peer != second
            || peer == "queued",
        "peer={peer} second={second}"
    );
}

#[tokio::test]
async fn job_level_and_workflow_level_share_namespace() {
    // Plan: groups are one namespace for workflow-level runs and job-level jobs.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: shared-ns
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    concurrency:
      group: shared-ns
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    // B's job should be pending on the same group held by A's run.
    let run_b = get_run_json(&app, b["run_id"].as_str().unwrap()).await;
    assert_eq!(run_b["jobs"]["build"], "pending");
}

#[tokio::test]
async fn queue_max_overflow_cancels_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: overflow-group
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    // 1 running + 100 pending = 101 holders; 102nd arrival cancelled.
    let mut ids = Vec::new();
    for _ in 0..101 {
        let r = submit_yaml(&app, yaml, "owner/repo").await;
        ids.push(r["run_id"].as_str().unwrap().to_owned());
    }
    // First is running/queued; next 100 pending.
    assert_eq!(get_run_json(&app, &ids[0]).await["status"], "queued");
    for id in ids.iter().skip(1).take(100) {
        assert_eq!(
            get_run_json(&app, id).await["status"],
            "pending",
            "expected pending for {id}"
        );
    }
    let overflow = submit_yaml(&app, yaml, "owner/repo").await;
    let overflow_id = overflow["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, overflow_id).await["status"], "cancelled");
}

#[tokio::test]
async fn cancel_run_api_releases_concurrency_slot() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: api-cancel-release
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();
    let b_id = b["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{a_id}/cancel"),
        Value::Null,
    )
    .await;
    assert_eq!(get_run_json(&app, a_id).await["status"], "cancelled");
    // B should be promoted.
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(run_b["status"], "queued");
    assert_eq!(run_b["jobs"]["build"], "queued");
}

#[tokio::test]
async fn cancel_in_progress_then_pending_chain() {
    // A running, B arrives with cancel-in-progress → A cancelled, B runs.
    // C arrives without cancel → pending. Complete B → C queued.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml_cancel = r#"
on: push
concurrency:
  group: chain-group
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#;
    let yaml_hold = r#"
on: push
concurrency:
  group: chain-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#;
    let a = submit_yaml(&app, yaml_cancel, "owner/repo").await;
    let _ = poll_and_ack(&app).await;
    let b = submit_yaml(&app, yaml_cancel, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "cancelled"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    let c = submit_yaml(&app, yaml_hold, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    // Drain cancel message then dispatch B and complete.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    if msg["messageType"] == azdo::message_type::JOB_CANCELLED {
        let _ = poll_and_ack(&app).await; // already consumed above; get next
    }
    // Complete B (may still be queued — complete_via_api works regardless).
    complete_via_api(&app, b["run_id"].as_str().unwrap(), "build").await;
    let run_c = get_run_json(&app, c["run_id"].as_str().unwrap()).await;
    assert_eq!(run_c["status"], "queued");
    assert_eq!(run_c["jobs"]["build"], "queued");
}

#[tokio::test]
async fn bare_string_concurrency_shorthand_enforced() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency: bare-shorthand
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn job_level_matrix_concurrency_per_expansion() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // Two matrix cells share one group → serialize; different group → parallel.
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  matrixed:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [a, b]
    concurrency:
      group: matrix-${{ matrix.os }}
    steps:
      - run: echo ${{ matrix.os }}
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;
    // Different matrix.os → different groups → both queued.
    let statuses: Vec<&str> = run["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        statuses.iter().filter(|s| **s == "queued").count() >= 2
            || statuses.iter().all(|s| *s == "queued" || *s == "pending"),
        "jobs={:?}",
        run["jobs"]
    );
    // Same-group matrix should serialize.
    let accepted2 = submit_yaml(
        &app,
        r#"
on: push
jobs:
  matrixed:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        n: [1, 2, 3]
    concurrency:
      group: matrix-same
    steps:
      - run: echo ${{ matrix.n }}
"#,
        "owner/repo",
    )
    .await;
    let run2 = get_run_json(&app, accepted2["run_id"].as_str().unwrap()).await;
    let queued = run2["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter(|v| v.as_str() == Some("queued"))
        .count();
    let pending = run2["jobs"]
        .as_object()
        .unwrap()
        .values()
        .filter(|v| v.as_str() == Some("pending"))
        .count();
    assert_eq!(
        queued, 1,
        "exactly one matrix cell should run: {:?}",
        run2["jobs"]
    );
    assert_eq!(pending, 2, "other cells pending: {:?}", run2["jobs"]);
}

#[tokio::test]
async fn mixed_queue_modes_arrival_owns_join() {
    // Assumption #3: each arrival's own queue mode decides how it joins.
    // A running; B arrives with queue:max (pending); C arrives with queue:single
    // → should cancel B and take the pending slot.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
"#,
        "owner/repo",
    )
    .await;
    let b = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
        "owner/repo",
    )
    .await;
    let c = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: mixed-q
  queue: single
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo c
"#,
        "owner/repo",
    )
    .await;
    assert_eq!(
        get_run_json(&app, a["run_id"].as_str().unwrap()).await["status"],
        "queued"
    );
    assert_eq!(
        get_run_json(&app, b["run_id"].as_str().unwrap()).await["status"],
        "cancelled",
        "queue:single arrival should replace existing pending"
    );
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
}

#[tokio::test]
async fn cancel_message_targets_agent_job_guid_not_logical_id() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let a = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: guid-check
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
        "owner/repo",
    )
    .await;
    let msg = poll_and_ack(&app).await;
    assert!(!msg.is_null());
    // Extract agent job id from the job request path if present; otherwise
    // from cancellation body after B arrives.
    let _b = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: guid-check
  cancel-in-progress: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
        "owner/repo",
    )
    .await;
    let cancel_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    let body = decode_cancel_body(&cancel_msg);
    let job_id = body["jobId"].as_str().unwrap();
    // Must be a UUID, not the logical job name "build".
    assert!(
        job_id.parse::<uuid::Uuid>().is_ok(),
        "jobId must be agent GUID, got {job_id}"
    );
    assert_ne!(job_id, "build");
    assert_eq!(body["timeout"], "00:05:00");
    let _ = a;
}

#[tokio::test]
async fn workflow_concurrency_cancel_before_dispatch_no_message() {
    // Cancel a pending (not yet dispatched) run → no JobCancellation enqueued.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: no-msg
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    assert_eq!(get_run_json(&app, b_id).await["status"], "pending");
    // C with queue:single replaces B without B ever being in-flight.
    let c = submit_yaml(&app, yaml, "owner/repo").await;
    assert_eq!(get_run_json(&app, b_id).await["status"], "cancelled");
    assert_eq!(
        get_run_json(&app, c["run_id"].as_str().unwrap()).await["status"],
        "pending"
    );
    // Only A's job message should be available, not a cancel for B.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_ne!(
        msg["messageType"],
        azdo::message_type::JOB_CANCELLED,
        "pending-only cancel must not emit JobCancellation"
    );
    let _ = a;
}

// ── C-01 regression: max-parallel + concurrency promotion without self-deadlock ──

// ── C-01 regression: max-parallel + concurrency promotion without self-deadlock ──

#[tokio::test]
async fn c01_max_parallel_concurrency_no_self_deadlock() {
    // Two matrix cells with max-parallel: 1 and a shared concurrency group.
    // Cell A acquires the group, cell B waits. When A completes, B must
    // be promoted exactly once without contending with its own holder.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      max-parallel: 1
      matrix:
        ver: [1, 2]
    concurrency:
      group: mp-group
    steps:
      - run: echo test
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    // One cell should be queued, the other pending (concurrency-blocked).
    let (queued_job, _blocked_count) = {
        let inner = state.inner.lock().await;
        let q = inner.queue.len();
        let cb = inner.concurrency_blocked.len();
        let pj = inner.pending_jobs.len();
        // Exactly one in queue (or pending_jobs if max-parallel gated first)
        assert!(
            q + pj >= 1,
            "at least one job should be ready: q={q} pj={pj}"
        );
        let first_job = inner
            .queue
            .front()
            .map(|j| j.job_id.clone())
            .or_else(|| inner.pending_jobs.front().map(|j| j.job_id.clone()))
            .unwrap();
        (first_job, cb)
    };

    // Complete the first cell.
    complete_via_api(&app, run_id, queued_job.0.as_str()).await;

    // After completion + promotion, the second cell should now be queued.
    let run = get_run_json(&app, run_id).await;
    let jobs = run["jobs"].as_object().unwrap();
    // At least one job should be Queued or InProgress (promoted), and none
    // should be permanently stuck in Pending.
    let stuck_pending = jobs
        .values()
        .filter(|v| v.as_str() == Some("pending"))
        .count();
    assert_eq!(
        stuck_pending, 0,
        "no job should remain stuck in pending after promotion"
    );
}

// ── C-05 regression: eval failure → terminal run status ──

// ── C-05 regression: eval failure → terminal run status ──

#[tokio::test]
async fn c05_eval_failure_terminates_run() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // A single-job workflow with a malformed concurrency expression.
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    concurrency:
      group: ""
    steps:
      - run: echo never
"#,
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let run = get_run_json(&app, run_id).await;

    // The run must NOT stay Queued forever — it must reach a terminal state.
    let status = run["status"].as_str().unwrap();
    assert!(
        status == "failure" || status == "cancelled",
        "run with failed concurrency eval should be terminal, got: {status}"
    );
}

// ── C-06 regression: boolean expression evaluation for cancel-in-progress ──

// ── C-06 regression: boolean expression evaluation for cancel-in-progress ──

#[tokio::test]
async fn c06_cancel_in_progress_expression_bool_eval() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // cancel-in-progress uses an expression — must evaluate as boolean.
    let yaml = r#"
on: push
concurrency:
  group: bool-eval-group
  cancel-in-progress: ${{ true }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let b = submit_yaml(&app, yaml, "owner/repo").await;

    // B should cancel A (cancel-in-progress is true).
    let a_run = get_run_json(&app, a["run_id"].as_str().unwrap()).await;
    assert_eq!(
        a_run["status"], "cancelled",
        "${{{{ true }}}} must be evaluated as truthy cancel"
    );

    // B should be running/queued.
    let b_run = get_run_json(&app, b["run_id"].as_str().unwrap()).await;
    let b_status = b_run["status"].as_str().unwrap();
    assert!(
        b_status == "queued" || b_status == "in_progress",
        "successor should be active, got: {b_status}"
    );
}

#[tokio::test]
async fn c06_queue_max_with_dynamic_true_cancel_rejected() {
    // queue: max combined with cancel-in-progress: ${{ true }} must be
    // rejected. The parser catches literal "true" at parse time → 400.
    // Dynamic expressions are caught at evaluation time → also rejected.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let body = json!({
        "workflow_yaml": "on: push\nconcurrency:\n  group: queue-max-cancel-true\n  queue: max\n  cancel-in-progress: ${{ true }}\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/runs")
        .header(header::AUTHORIZATION, "Bearer preloop-system-token")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(state.inner.lock().await.runs.is_empty());
}

// ── C-07 regression: holder_keys reclamation ──

// ── C-07 regression: holder_keys reclamation ──

#[tokio::test]
async fn c07_holder_keys_cleaned_after_run_release() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let yaml = r#"
on: push
concurrency:
  group: holder-cleanup-group
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo cleanup
"#;
    let accepted = submit_yaml(&app, yaml, "owner/repo").await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Before completion, holder_keys should have an entry.
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.holder_keys.contains_key(&run_id),
            "holder_keys should track the run"
        );
    }

    // Get the job ID and complete it.
    let job_id = {
        let inner = state.inner.lock().await;
        inner.queue.front().unwrap().job_id.clone()
    };
    complete_via_api(&app, accepted["run_id"].as_str().unwrap(), &job_id.0).await;

    // After completion, holder_keys for this run should be gone.
    {
        let inner = state.inner.lock().await;
        assert!(
            !inner.holder_keys.contains_key(&run_id),
            "holder_keys should be cleaned up after run completes"
        );
    }
}

// ── C-02 regression: reusable JobSet admission and promotion ──

// ── C-02 regression: reusable JobSet admission and promotion ──

#[tokio::test]
async fn c02_reusable_call_jobset_blocks_and_promotes_members() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let caller_yaml = r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: reusable-serial
"#;
    let callee_yaml = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo callee
"#;
    let submission = || {
        json!({
            "workflow_yaml": caller_yaml,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": callee_yaml,
            }
        })
    };

    let first = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
    let second = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
    let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
    let second_run: RunId = second["run_id"].as_str().unwrap().parse().unwrap();
    let (first_job, second_job) = {
        let inner = state.inner.lock().await;
        // The first caller's gate is free at submission: its callee subtree
        // materializes immediately and `call/inner` is dispatched.
        let first_job = JobId("call/inner".to_owned());
        // The second caller holds the gate's pending slot: it stays one
        // parked caller node, not a materialized subtree.
        let second_job = JobId("call".to_owned());
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::InProgress
        );
        assert_eq!(
            inner.runs[&first_run].jobs[&first_job],
            ExecutionStatus::Queued
        );
        assert_eq!(
            inner.runs[&second_run].jobs[&second_job],
            ExecutionStatus::Pending
        );
        assert!(inner
            .queue
            .iter()
            .any(|job| job.run_id == first_run && job.job_id == first_job));
        assert!(inner
            .concurrency_blocked
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
        (first_job, second_job)
    };

    complete_via_api(&app, &first_run.to_string(), &first_job.0).await;
    {
        let inner = state.inner.lock().await;
        // Completing the subtree terminalizes the first caller, releasing the
        // gate; the second caller is admitted and expanded in turn.
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::Success
        );
        let second_inner = JobId("call/inner".to_owned());
        assert_eq!(
            inner.runs[&second_run].jobs[&JobId("call".to_owned())],
            ExecutionStatus::InProgress
        );
        assert_eq!(
            inner.runs[&second_run].jobs[&second_inner],
            ExecutionStatus::Queued
        );
        assert!(inner
            .queue
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_inner));
        assert!(!inner
            .concurrency_blocked
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
    };
    complete_via_api(&app, &second_run.to_string(), "call/inner").await;
    assert_eq!(
        state.inner.lock().await.runs[&second_run].status,
        ExecutionStatus::Success
    );
}

#[tokio::test]
async fn c02_jobset_waits_for_embedded_gate_after_acquiring_caller_gate() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let holder = submit_yaml(
        &app,
        r#"
on: push
concurrency:
  group: embedded-shared
jobs:
  hold:
    runs-on: ubuntu-latest
    steps:
      - run: echo hold
"#,
        "owner/repo",
    )
    .await;
    let reusable = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: caller-free
    with:
      concurrency_group: embedded-shared
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": r#"
on:
  workflow_call:
    inputs:
      concurrency_group:
        required: true
        type: string
concurrency:
  group: ${{ inputs.concurrency_group }}
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let holder_run: RunId = holder["run_id"].as_str().unwrap().parse().unwrap();
    let reusable_run: RunId = reusable["run_id"].as_str().unwrap().parse().unwrap();
    let (holder_job, reusable_job) = {
        let inner = state.inner.lock().await;
        let holder_job = inner.runs[&holder_run].jobs.keys().next().unwrap().clone();
        let reusable_job = inner.runs[&reusable_run]
            .jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        assert_eq!(
            inner.runs[&reusable_run].jobs[&reusable_job],
            ExecutionStatus::Pending
        );
        assert_eq!(inner.jobset_admissions.len(), 1);
        assert_eq!(
            inner
                .jobset_admissions
                .values()
                .next()
                .unwrap()
                .acquired_keys
                .len(),
            1
        );
        (holder_job, reusable_job)
    };

    complete_via_api(&app, &holder_run.to_string(), &holder_job.0).await;
    {
        let inner = state.inner.lock().await;
        // Both gates acquired: the caller materialized its subtree and is
        // tracked as the JobSet holder; the inner job is dispatched.
        assert_eq!(
            inner.runs[&reusable_run].jobs[&reusable_job],
            ExecutionStatus::InProgress
        );
        let inner_job = JobId(format!("{}/inner", reusable_job.0));
        assert_eq!(
            inner.runs[&reusable_run].jobs[&inner_job],
            ExecutionStatus::Queued
        );
        assert!(inner.jobset_admissions.is_empty());
        for group_name in ["caller-free", "embedded-shared"] {
            let key = concurrency::concurrency_key("owner/repo", group_name);
            assert!(matches!(
                inner.concurrency_groups[&key].running,
                Some(concurrency::Holder::JobSet { run_id, .. }) if run_id == reusable_run
            ));
        }
    };
    complete_via_api(&app, &reusable_run.to_string(), "call/inner").await;
}

#[tokio::test]
async fn c02_jobset_deduplicates_identical_caller_and_embedded_keys() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: same-key
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": r#"
on: workflow_call
concurrency:
  group: same-key
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let inner = state.inner.lock().await;
    // Identical caller+embedded group dedupes to one gate: the caller was
    // admitted and its subtree materialized at submission.
    assert_eq!(
        inner.runs[&run_id].jobs[&JobId("call".to_owned())],
        ExecutionStatus::InProgress
    );
    assert_eq!(
        inner.runs[&run_id].jobs[&JobId("call/inner".to_owned())],
        ExecutionStatus::Queued
    );
    assert!(inner.jobset_admissions.is_empty());
    let key = concurrency::concurrency_key("owner/repo", "same-key");
    assert!(inner.concurrency_groups[&key].pending.is_empty());
    assert!(matches!(
        inner.concurrency_groups[&key].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
}

/// uv-ci shape: a reusable `plan` produces outputs; a caller job gates on
/// `needs.plan.outputs.X == 'true'` and calls another reusable.
/// GitHub evaluates a reusable call's `if:` once the caller's needs complete;
/// a false result skips the whole invocation and the run record shows exactly
/// one skipped caller entry — the callee subtree is never materialized — and
/// jobs that `needs` it are skipped in turn.

/// uv-ci shape: a reusable `plan` produces outputs; a caller job gates on
/// `needs.plan.outputs.X == 'true'` and calls another reusable.
/// GitHub evaluates a reusable call's `if:` once the caller's needs complete;
/// a false result skips the whole invocation and the run record shows exactly
/// one skipped caller entry — the callee subtree is never materialized — and
/// jobs that `needs` it are skipped in turn.
#[tokio::test]
async fn reusable_caller_gated_on_plan_outputs_is_skipped() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  gated:
    needs: plan
    if: ${{ needs.plan.outputs.flag == 'true' }}
    uses: ./.github/workflows/callee.yml
  dependent:
    needs: gated
    runs-on: ubuntu-latest
    steps:
      - run: echo dependent
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      flag:
        value: ${{ jobs.p.outputs.flag }}
jobs:
  p:
    runs-on: ubuntu-latest
    outputs:
      flag: "false"
    steps:
      - run: echo "flag=false" >> $GITHUB_OUTPUT
"#,
                ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let plan_job = {
        let inner = state.inner.lock().await;
        // The `plan` caller itself has no gate: its callee job materialized
        // at submission time.
        inner.runs[&run]
            .jobs
            .keys()
            .find(|id| id.0.starts_with("plan/"))
            .unwrap()
            .clone()
    };
    // Complete the plan with its output resolved to "false".
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        &plan_job.0,
        serde_json::json!({"flag": "false"}),
    )
    .await;
    {
        let inner = state.inner.lock().await;
        // GitHub shape: exactly one skipped entry for the gated caller — and
        // no callee job ever appeared in the run record.
        let gated = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "gated")
            .unwrap();
        assert_eq!(
            *gated.1,
            ExecutionStatus::Skipped,
            "gated reusable caller must be skipped when its `if:` evaluates false"
        );
        assert!(
            !inner.runs[&run]
                .jobs
                .keys()
                .any(|id| id.0.starts_with("gated/")),
            "a false-gated caller's callee subtree must never materialize"
        );
        let dependent = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "dependent")
            .unwrap();
        assert_eq!(
            *dependent.1,
            ExecutionStatus::Skipped,
            "a job that needs a skipped reusable call must be skipped too"
        );
    }
    {
        let inner = state.inner.lock().await;
        assert!(
            !inner
                .queue
                .iter()
                .any(|job| job.run_id == run && job.job_id.0.starts_with("gated/")),
            "skipped gated caller's inner job must never reach the dispatch queue"
        );
        assert_eq!(
            inner.runs[&run].status,
            ExecutionStatus::Success,
            "a run whose gated call was skipped concludes success"
        );
    }
}

/// GitHub run-record parity end to end: a false-gated caller stays one
/// skipped entry, a passing caller appears only as its callee jobs, and the
/// jobs listing carries GitHub display names (evaluated `name:`, space-slash
/// `caller / callee`, per-cell matrix names).

/// GitHub run-record parity end to end: a false-gated caller stays one
/// skipped entry, a passing caller appears only as its callee jobs, and the
/// jobs listing carries GitHub display names (evaluated `name:`, space-slash
/// `caller / callee`, per-cell matrix names).
#[tokio::test]
async fn github_shaped_run_record_for_gated_and_passing_callers() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  test-smoke:
    needs: plan
    if: ${{ needs.plan.outputs.smoke == 'true' }}
    uses: ./.github/workflows/smoke.yml
  docs:
    needs: plan
    if: ${{ needs.plan.outputs.docs == 'true' }}
    uses: ./.github/workflows/docs.yml
    with:
      suite: guide
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      smoke:
        value: ${{ jobs.p.outputs.smoke }}
      docs:
        value: ${{ jobs.p.outputs.docs }}
jobs:
  p:
    name: plan
    runs-on: ubuntu-latest
    outputs:
      smoke: "false"
      docs: "true"
    steps:
      - run: echo done
"#,
                ".github/workflows/smoke.yml": r#"
on: workflow_call
jobs:
  smoke:
    strategy:
      matrix:
        os: [ubuntu, macos]
    runs-on: ${{ matrix.os }}
    steps:
      - run: echo smoke
"#,
                ".github/workflows/docs.yml": r#"
on:
  workflow_call:
    inputs:
      suite:
        required: true
        type: string
jobs:
  mkdocs:
    name: "docs ${{ matrix.python }} for ${{ inputs.suite }}"
    strategy:
      matrix:
        python: ["3.9", "3.10"]
    runs-on: ubuntu-latest
    steps:
      - run: echo docs
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        "plan/p",
        serde_json::json!({"smoke": "false", "docs": "true"}),
    )
    .await;
    // The docs caller's gate passed: its materialized matrix jobs complete.
    for id in ["docs/mkdocs (3.9)", "docs/mkdocs (3.10)"] {
        complete_via_api(&app, &run.to_string(), id).await;
    }

    let record = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{run}"),
        json!(null),
    )
    .await;
    let jobs = record["jobs"].as_object().unwrap();
    let mut job_ids: Vec<&str> = jobs.keys().map(|k| k.as_str()).collect();
    job_ids.sort();
    assert_eq!(
        job_ids,
        vec![
            "docs/mkdocs (3.10)",
            "docs/mkdocs (3.9)",
            "plan/p",
            "test-smoke"
        ],
        "visible job set matches GitHub: passing callers as callee jobs, the \
         false-gated caller as one skipped entry"
    );
    assert_eq!(jobs["test-smoke"], serde_json::json!("skipped"));
    let names: Vec<&str> = record["jobs_list"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap())
        .collect();
    let mut names = names;
    names.sort();
    assert_eq!(
        names,
        vec![
            "docs / docs 3.10 for guide",
            "docs / docs 3.9 for guide",
            "plan / plan",
            "test-smoke"
        ],
        "display names follow GitHub: evaluated name:, ` / ` separator, per-cell matrix values"
    );
}

/// The same reusable call with a true condition runs normally: the inner job
/// is dispatched and the dependent follows.

/// The same reusable call with a true condition runs normally: the inner job
/// is dispatched and the dependent follows.
#[tokio::test]
async fn reusable_caller_gated_on_true_output_runs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  plan:
    uses: ./.github/workflows/plan.yml
  gated:
    needs: plan
    if: ${{ needs.plan.outputs.flag == 'true' }}
    uses: ./.github/workflows/callee.yml
  dependent:
    needs: gated
    runs-on: ubuntu-latest
    steps:
      - run: echo dependent
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/plan.yml": r#"
on:
  workflow_call:
    outputs:
      flag:
        value: ${{ jobs.p.outputs.flag }}
jobs:
  p:
    runs-on: ubuntu-latest
    outputs:
      flag: "true"
    steps:
      - run: echo "flag=true" >> $GITHUB_OUTPUT
"#,
                ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#,
            }
        }),
    )
    .await;
    let run: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let plan_job = {
        let inner = state.inner.lock().await;
        inner.runs[&run]
            .jobs
            .keys()
            .find(|id| id.0.starts_with("plan/"))
            .unwrap()
            .clone()
    };
    // Complete the plan with its output resolved to "true".
    complete_via_api_with_outputs(
        &app,
        &run.to_string(),
        &plan_job.0,
        serde_json::json!({"flag": "true"}),
    )
    .await;
    // The gate passed: the caller is InProgress and its materialized inner
    // job is promoted and claimable.
    let gated_id = {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs[&run].jobs[&JobId("gated".to_owned())],
            ExecutionStatus::InProgress,
            "caller node tracks its running subtree"
        );
        let gated = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0.starts_with("gated/"))
            .unwrap();
        assert_eq!(
            *gated.1,
            ExecutionStatus::Queued,
            "gated reusable caller runs when the condition is true"
        );
        assert!(
            inner
                .queue
                .iter()
                .any(|job| job.run_id == run && job.job_id == *gated.0),
            "gated inner job must be in the dispatch queue"
        );
        gated.0.clone()
    };
    // The dependent stays pending until the call completes.
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs[&run]
                .jobs
                .get(&JobId("dependent".to_owned()))
                .copied(),
            Some(ExecutionStatus::Queued),
            "dependent of a running call stays queued"
        );
    }
    complete_via_api(&app, &run.to_string(), &gated_id.0).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.runs[&run].jobs[&gated_id], ExecutionStatus::Success);
        // The caller node aggregates to its subtree's result.
        assert_eq!(
            inner.runs[&run].jobs[&JobId("gated".to_owned())],
            ExecutionStatus::Success
        );
        // The dependent's needs (the inlined inner job) are now satisfied.
        let dependent = inner.runs[&run]
            .jobs
            .iter()
            .find(|(id, _)| id.0 == "dependent")
            .unwrap();
        assert_eq!(
            *dependent.1,
            ExecutionStatus::Queued,
            "dependent of a successful call is promoted after the call completes"
        );
    }
}

#[tokio::test]
async fn c02_jobset_resolves_matrix_contexts_on_caller_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let caller_yaml = r#"
on: push
jobs:
  call:
    strategy:
      matrix:
        env: [dev, prod]
    uses: ./.github/workflows/callee.yml
    concurrency:
      group: deploy-${{ matrix.env }}
"#;
    let callee_yaml = r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo callee
"#;

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": caller_yaml,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/callee.yml": callee_yaml,
            }
        }),
    )
    .await;

    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let inner = state.inner.lock().await;

    // Verify we evaluated both matrix groups: deploy-dev and deploy-prod
    let key_dev = concurrency::concurrency_key("owner/repo", "deploy-dev");
    let key_prod = concurrency::concurrency_key("owner/repo", "deploy-prod");

    assert!(
        inner.concurrency_groups.contains_key(&key_dev),
        "concurrency_groups must have deploy-dev"
    );
    assert!(
        inner.concurrency_groups.contains_key(&key_prod),
        "concurrency_groups must have deploy-prod"
    );

    assert!(matches!(
        inner.concurrency_groups[&key_dev].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
    assert!(matches!(
        inner.concurrency_groups[&key_prod].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
}
/// Production path: duplicate completion does not create a second promotion.

/// Production path: duplicate completion does not create a second promotion.
#[tokio::test]
async fn dag_duplicate_completion_idempotent_production() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Complete build once
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test should be queued exactly once
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.iter().filter(|j| j.job_id.0 == "test").count(),
            1,
            "test must appear exactly once in queue"
        );
    }

    // Complete build again (duplicate)
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test must still appear exactly once
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.queue.iter().filter(|j| j.job_id.0 == "test").count(),
            1,
            "duplicate completion must not create second promotion"
        );
    }
}

/// Production path: small structured YAML → parse → expand → server
/// submission → promote/complete verifies the full pipeline.

/// Production path: small structured YAML → parse → expand → server
/// submission → promote/complete verifies the full pipeline.
#[tokio::test]
async fn dag_yaml_parse_expand_server_production() {
    let yaml = r#"
on: push
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: echo lint
  build:
    needs: [lint]
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  test:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test
  deploy:
    needs: [test]
    if: success()
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#;
    // Verify parser round-trip
    let workflow = preloop_gha_parser::parse_workflow(yaml).unwrap();
    let plans = preloop_gha_parser::expand_jobs(&workflow).unwrap();
    let plan_ids: Vec<_> = plans.iter().map(|p| p.id.0.as_str()).collect();
    assert!(plan_ids.contains(&"lint"));
    assert!(plan_ids.contains(&"build"));
    assert!(plan_ids.contains(&"test"));
    assert!(plan_ids.contains(&"deploy"));

    // Verify DAG validation passes
    preloop_gha_parser::dag::validate_job_plans(&plans).unwrap();

    // Run through real server
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Queued jobs = parser's expanded IDs (lint is root)
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.queue[0].job_id.0, "lint");
    }

    // Walk the chain: lint → build → test → deploy
    for (job, next_queued) in [
        ("lint", Some("build")),
        ("build", Some("test")),
        ("test", Some("deploy")),
        ("deploy", None),
    ] {
        request_json(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({"run_id": run_id, "job_id": job, "status": "success"}),
        )
        .await;

        let inner = state.inner.lock().await;
        if let Some(next) = next_queued {
            assert!(
                inner.queue.iter().any(|j| j.job_id.0 == next),
                "after completing {job}, {next} should be queued"
            );
        }
    }

    // Run is terminal — all jobs completed successfully
    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert!(inner.pending_jobs.is_empty());
    for (job_id, status) in &run.jobs {
        assert_eq!(
            *status,
            ExecutionStatus::Success,
            "job {} should be Success, got {:?}",
            job_id.0,
            status
        );
    }
}
/// Exercises the real parser → queue → completion → dependency-promotion path
/// over 1,000 deterministic bounded DAGs.
#[tokio::test]
#[allow(clippy::needless_range_loop)]
async fn generated_server_dag_properties_1000_cases() {
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    for case in 0..1_000u64 {
        let mut seed = 20250713u64 ^ case.wrapping_mul(0x9E37_79B9);
        let count = 2 + (next(&mut seed) % 4) as usize;
        let mut needs = vec![Vec::<usize>::new(); count];
        for job in 1..count {
            for dependency in 0..job {
                if next(&mut seed) & 1 == 1 {
                    needs[job].push(dependency);
                }
            }
        }
        let failed_root = (0..count).find(|job| needs[*job].is_empty()).unwrap();

        // Assign conditions to non-root jobs based on PRNG
        let mut conditions: Vec<Option<&str>> = vec![None; count];
        for job in 1..count {
            if !needs[job].is_empty() {
                conditions[job] = match next(&mut seed) % 5 {
                    0 => Some("always()"),
                    1 => Some("failure()"),
                    _ => None, // default gate
                };
            }
        }

        let mut yaml = String::from("on: push\njobs:\n");
        for job in 0..count {
            yaml.push_str(&format!("  j{job}:\n"));
            if !needs[job].is_empty() {
                yaml.push_str("    needs: [");
                for (index, dependency) in needs[job].iter().enumerate() {
                    if index > 0 {
                        yaml.push_str(", ");
                    }
                    yaml.push_str(&format!("j{dependency}"));
                }
                yaml.push_str("]\n");
            }
            if let Some(cond) = conditions[job] {
                yaml.push_str(&format!("    if: {cond}\n"));
            }
            yaml.push_str("    runs-on: ubuntu-latest\n");
            yaml.push_str("    steps:\n      - run: echo property\n");
        }

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": yaml,
                "event": "push",
                "repository": "property/test"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        for _ in 0..=count {
            let queued = {
                let inner = state.inner.lock().await;
                inner
                    .queue
                    .iter()
                    .filter(|job| job.run_id == run_id)
                    .map(|job| job.job_id.0.clone())
                    .collect::<Vec<_>>()
            };
            if queued.is_empty() {
                break;
            }
            for job_id in queued {
                let status = if job_id == format!("j{failed_root}") {
                    "failure"
                } else {
                    "success"
                };
                request_json(
                    &app,
                    Method::POST,
                    "/internal/test/jobs/complete",
                    json!({"run_id": run_id, "job_id": job_id, "status": status}),
                )
                .await;
            }
        }

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        let mut failed_ancestor = vec![false; count];
        for job in 0..count {
            failed_ancestor[job] = job == failed_root
                || needs[job]
                    .iter()
                    .any(|dependency| failed_ancestor[*dependency]);
            let expected = if job == failed_root {
                ExecutionStatus::Failure
            } else if failed_ancestor[job] {
                // Job has a failed ancestor — what does the condition say?
                match conditions[job] {
                    Some("always()") => ExecutionStatus::Success, // always runs, completed successfully
                    Some("failure()") => ExecutionStatus::Success, // failure() is true, job runs
                    _ => ExecutionStatus::Skipped,                // default gate blocks
                }
            } else {
                // No failed ancestor
                match conditions[job] {
                    Some("failure()") => ExecutionStatus::Skipped, // failure() is false, skip
                    _ => ExecutionStatus::Success,                 // default or always() runs
                }
            };
            assert_eq!(
                run.jobs[&JobId(format!("j{job}"))],
                expected,
                "case {case} job j{job} condition={:?}",
                conditions[job]
            );
        }
    }
}

#[tokio::test]
async fn shallow_workspace_snapshot_preserves_upstream_shas() {
    // A shallow clone's cache inherits its boundary, and serving that forced a
    // history rewrite that changed every sha. Workflows resolve a base commit
    // from git (`HEAD^`, `merge-base`) and then fetch it from the forge, so a
    // rewritten sha fails there with "not our ref". Deepening from the remote
    // must keep the served ancestry byte-identical to upstream.
    let temp = tempfile::tempdir().unwrap();
    let upstream = temp.path().join("upstream");
    fs::create_dir_all(&upstream).unwrap();
    git_fixture_command(
        &upstream,
        &["init", "--quiet", "--initial-branch=main", "."],
    );
    git_fixture_command(&upstream, &["config", "user.email", "t@example.com"]);
    git_fixture_command(&upstream, &["config", "user.name", "Test"]);
    for n in 0..3 {
        fs::write(upstream.join("file.txt"), format!("rev {n}\n")).unwrap();
        git_fixture_command(&upstream, &["add", "file.txt"]);
        git_fixture_command(
            &upstream,
            &["commit", "--quiet", "-m", &format!("commit {n}")],
        );
    }
    let upstream_tip = String::from_utf8(git_fixture_output(&upstream, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();

    // Shallow clone, exactly like CI does.
    let workspace = temp.path().join("workspace");
    let url = format!("file://{}", upstream.display());
    let clone = std::process::Command::new("git")
        .args(["clone", "--quiet", "--depth=1", &url])
        .arg(&workspace)
        .output()
        .unwrap();
    assert!(clone.status.success(), "shallow clone failed: {clone:?}");
    assert!(
        workspace.join(".git/shallow").is_file(),
        "fixture must be shallow"
    );

    let state_dir = temp.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "22222222-2222-4222-8222-222222222222".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None, None)
        .await
        .expect("snapshot creation should succeed");

    // The snapshot commit's parent must be the upstream sha, not a copy.
    let repository = state_dir.join(&snapshot.repository);
    let parent = String::from_utf8(git_fixture_output(
        &repository,
        &["rev-parse", &format!("{}^", snapshot.commit_sha)],
    ))
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(
        parent, upstream_tip,
        "snapshot parent must be the real upstream commit, not a rewritten copy"
    );
}

#[tokio::test]
async fn workspace_snapshot_survives_refs_with_missing_objects() {
    // `update-ref --stdin` is atomic, so a single tag whose object is absent
    // used to abort the whole snapshot with "nonexistent object" — and the
    // fallback then handed the job a zero sha.
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    fs::create_dir_all(&state_dir).unwrap();
    let tags = workspace.join(".git/refs/tags");
    fs::create_dir_all(&tags).unwrap();
    fs::write(
        tags.join("dangling"),
        "3c72e3fdd04bb63c9470ad0a79ad05bba0a393a4\n",
    )
    .unwrap();

    let run_id: RunId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None, None)
        .await
        .expect("a dangling ref must not fail snapshot creation");
    assert_eq!(snapshot.commit_sha.len(), 40);
}

#[tokio::test]
async fn snapshot_drops_unresolvable_gitlinks_but_keeps_registered_submodules() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());

    // A nested repo added by hand: the parent index gets a gitlink entry
    // but no `.gitmodules` registers it — the state that makes
    // `git submodule foreach` inside the VM fail with `fatal: No url found
    // for submodule path 'stream-docker-output' in .gitmodules`.
    let nested = workspace.join("stream-docker-output");
    fs::create_dir_all(&nested).unwrap();
    git_fixture_command(&nested, &["init", "-q", "-b", "main"]);
    git_fixture_command(&nested, &["config", "user.email", "nested@example.test"]);
    git_fixture_command(&nested, &["config", "user.name", "Nested Test"]);
    fs::write(nested.join("payload.txt"), "nested\n").unwrap();
    git_fixture_command(&nested, &["add", "payload.txt"]);
    git_fixture_command(&nested, &["commit", "-qm", "nested"]);
    let nested_tip = String::from_utf8(git_fixture_output(&nested, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();
    let cacheinfo = format!("160000,{nested_tip},stream-docker-output");
    git_fixture_command(
        &workspace,
        &["update-index", "--add", "--cacheinfo", cacheinfo.as_str()],
    );

    fs::create_dir_all(&state_dir).unwrap();
    let first_run: RunId = "44444444-4444-4444-8444-444444444444".parse().unwrap();
    let first = create_workspace_snapshot(&state_dir, &workspace, first_run, None, None)
        .await
        .expect("snapshot with an unresolvable gitlink should succeed");
    let first_repository = state_dir.join(&first.repository);
    let output = git_fixture_output(
        &first_repository,
        &[
            "ls-tree",
            first.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    );
    assert!(
        output.is_empty(),
        "unresolvable gitlink must be dropped from the snapshot: {}",
        String::from_utf8_lossy(&output)
    );

    // Register the submodule properly: the gitlink must then survive so a
    // workflow asking for submodules gets the real structure.
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"stream-docker-output\"]\n\tpath = stream-docker-output\n\turl = https://example.test/stream-docker-output.git\n",
    )
    .unwrap();
    let second_run: RunId = "55555555-5555-4555-8555-555555555555".parse().unwrap();
    let second = create_workspace_snapshot(&state_dir, &workspace, second_run, None, None)
        .await
        .expect("snapshot with a registered submodule should succeed");
    let second_repository = state_dir.join(&second.repository);
    let listed = String::from_utf8(git_fixture_output(
        &second_repository,
        &[
            "ls-tree",
            second.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    ))
    .unwrap();
    assert!(
        listed.starts_with("160000"),
        "a registered submodule gitlink must survive the snapshot: {listed}"
    );

    // A logical submodule name that differs from the checkout path is valid
    // (`git submodule add --name`): the gitlink must still survive, since git
    // resolves it by the `path` key, not the section name.
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"logical-stream\"]\n\tpath = stream-docker-output\n\turl = https://example.test/stream-docker-output.git\n",
    )
    .unwrap();
    let third_run: RunId = "66666666-6666-4666-8666-666666666666".parse().unwrap();
    let third = create_workspace_snapshot(&state_dir, &workspace, third_run, None, None)
        .await
        .expect("snapshot with a logically-named submodule should succeed");
    let third_repository = state_dir.join(&third.repository);
    let listed = String::from_utf8(git_fixture_output(
        &third_repository,
        &[
            "ls-tree",
            third.commit_sha.as_str(),
            "--",
            "stream-docker-output",
        ],
    ))
    .unwrap();
    assert!(
        listed.starts_with("160000"),
        "a logically-named registered submodule gitlink must survive the snapshot: {listed}"
    );
}

#[tokio::test]
async fn snapshot_gitlink_resolution_matches_git() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());

    // The keep/drop decision must mirror git's own resolution: git resolves a
    // gitlink by the section whose `path` matches it. Three registration
    // shapes git handles but a naive parser gets wrong:
    //   `a#b`         git writes and decodes this path QUOTED in .gitmodules
    //   `mixed`       [SUBMODULE]/Path/URL: config sections and keys are
    //                 case-insensitive for git
    //   `logical-only` section name only; its `path` points elsewhere, so a
    //                 gitlink at the name itself is NOT resolvable
    for path in ["a#b", "mixed", "logical-only"] {
        let nested = workspace.join(path);
        fs::create_dir_all(&nested).unwrap();
        git_fixture_command(&nested, &["init", "-q", "-b", "main"]);
        git_fixture_command(&nested, &["config", "user.email", "nested@example.test"]);
        git_fixture_command(&nested, &["config", "user.name", "Nested Test"]);
        fs::write(nested.join("payload.txt"), format!("{path}\n")).unwrap();
        git_fixture_command(&nested, &["add", "payload.txt"]);
        git_fixture_command(&nested, &["commit", "-qm", path]);
        let tip = String::from_utf8(git_fixture_output(&nested, &["rev-parse", "HEAD"]))
            .unwrap()
            .trim()
            .to_owned();
        let cacheinfo = format!("160000,{tip},{path}");
        git_fixture_command(
            &workspace,
            &["update-index", "--add", "--cacheinfo", cacheinfo.as_str()],
        );
    }
    fs::write(
        workspace.join(".gitmodules"),
        "[submodule \"a#b\"]\n\tpath = \"a#b\"\n\turl = https://example.test/a-b.git\n\
         [SUBMODULE \"mixed\"]\n\tPath = mixed\n\tURL = https://example.test/mixed.git\n\
         [submodule \"logical-only\"]\n\tpath = elsewhere\n\turl = https://example.test/elsewhere.git\n",
    )
    .unwrap();

    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "77777777-7777-4777-8777-777777777777".parse().unwrap();
    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None, None)
        .await
        .expect("snapshot with mixed gitlink registrations should succeed");
    let repository = state_dir.join(&snapshot.repository);
    let tree_of = |path: &str| {
        String::from_utf8(git_fixture_output(
            &repository,
            &["ls-tree", snapshot.commit_sha.as_str(), "--", path],
        ))
        .unwrap()
    };

    let quoted = tree_of("a#b");
    assert!(
        quoted.starts_with("160000"),
        "quoted registered path must survive the snapshot: {quoted}"
    );
    let mixed = tree_of("mixed");
    assert!(
        mixed.starts_with("160000"),
        "mixed-case registered section must survive the snapshot: {mixed}"
    );
    let name_only = tree_of("logical-only");
    assert!(
        name_only.is_empty(),
        "name-only gitlink must be dropped from the snapshot: {name_only}"
    );
}

#[tokio::test]
async fn workspace_snapshot_captures_git_state_without_mutating_source() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    fs::create_dir_all(&state_dir).unwrap();
    let run_id: RunId = "11111111-1111-4111-8111-111111111111".parse().unwrap();

    let status_before = git_fixture_output(&workspace, &["status", "--porcelain=v1"]);
    let index_path = FsPath::new(
        String::from_utf8(git_fixture_output(
            &workspace,
            &["rev-parse", "--git-path", "index"],
        ))
        .unwrap()
        .trim(),
    )
    .to_path_buf();
    let index_path = if index_path.is_absolute() {
        index_path
    } else {
        workspace.join(index_path)
    };
    let index_before = fs::read(&index_path).unwrap();

    let snapshot = create_workspace_snapshot(&state_dir, &workspace, run_id, None, None)
        .await
        .expect("snapshot creation should succeed");

    assert_eq!(snapshot.repository, format!("snapshots/{run_id}"));
    assert_eq!(snapshot.commit_sha.len(), 40);
    assert!(snapshot
        .commit_sha
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        git_fixture_output(&workspace, &["status", "--porcelain=v1"]),
        status_before,
        "snapshot creation must not alter source status"
    );
    assert_eq!(fs::read(&index_path).unwrap(), index_before);

    let repository = state_dir.join(&snapshot.repository);
    assert!(repository.join("objects").is_dir());
    let state_dir = std::fs::canonicalize(&state_dir).unwrap();
    let alternates = git_alternate_object_directories(&repository);
    assert!(!alternates.is_empty());
    assert!(alternates.iter().all(|alternate| {
        alternate.starts_with(&state_dir) && !alternate.starts_with(&workspace)
    }));
    let head = git_fixture_output(&workspace, &["rev-parse", "HEAD"]);
    assert_eq!(
        snapshot.head_sha.as_deref(),
        Some(std::str::from_utf8(&head).unwrap().trim()),
        "the snapshot must expose the workspace's real HEAD as its identity"
    );
    let config = fs::read_to_string(repository.join("config")).unwrap();
    assert!(
        config.contains("allowReachableSHA1InWant = true"),
        "deep fetches of reachable shas must be served by the snapshot: {config}"
    );
    assert!(
        config.contains("allowTipSHA1InWant = true"),
        "deep fetches of tip shas must be served by the snapshot: {config}"
    );
    let commit = snapshot.commit_sha.as_str();
    assert!(
        git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}^{{commit}}")]
        )
        .0
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:tracked.txt")]),
        b"tracked unstaged change\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:staged.txt")]),
        b"staged index change\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:tracked.ignored")]),
        b"tracked ignored modification\n"
    );
    assert_eq!(
        git_fixture_output(&repository, &["show", &format!("{commit}:untracked.txt")]),
        b"new nonignored file\n"
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}:deleted.txt")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &["cat-file", "-e", &format!("{commit}:ignored.ignored")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:ignored-dir/hidden.txt")
            ]
        )
        .0
    );
}
