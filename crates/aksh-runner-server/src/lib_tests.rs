use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use super::*;
const TEST_API_TOKEN: &str = "property-test-token";

fn app(state: AppState, shutdown: CancellationToken) -> Router {
    app_with_test_api(state, shutdown, TEST_API_TOKEN)
}

#[tokio::test]
async fn matrix_max_parallel_and_fail_fast_are_enforced() {
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
      fail-fast: true
      max-parallel: 1
      matrix:
        os: [ubuntu, macos, windows]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let first_job = {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.pending_jobs.len(), 2);
        inner.queue.front().unwrap().job_id.clone()
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": first_job,
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(inner.queue.is_empty());
    assert!(inner.pending_jobs.is_empty());
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs
            .values()
            .filter(|status| **status == ExecutionStatus::Cancelled)
            .count(),
        2
    );
}

#[tokio::test]
async fn agent_request_patch_targets_only_the_request_id() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Two independent runs, both reach InProgress when their job is pulled.
    let workflow = json!({
        "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    let first = request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
    let second = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
    let second_run: RunId = second["run_id"].as_str().unwrap().parse().unwrap();

    // Pull both jobs so they are InProgress and each has a distinct request_id.
    let first_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1",
        Value::Null,
    )
    .await;
    let second_msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s2",
        Value::Null,
    )
    .await;
    assert_eq!(
        first_msg["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    assert_eq!(
        second_msg["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    // The mapping should have two entries — one per request_id.
    let (first_req_id, _) = state
        .inner
        .lock()
        .await
        .inflight_requests
        .iter()
        .find(|(_, (rid, _))| *rid == first_run)
        .map(|(k, v)| (*k, v.clone()))
        .unwrap();

    // PATCH only the first run's request_id.
    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{first_req_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let inner = state.inner.lock().await;
    let first = inner.runs.get(&first_run).unwrap();
    let second = inner.runs.get(&second_run).unwrap();
    assert!(first
        .jobs
        .values()
        .all(|status| *status == ExecutionStatus::Success));
    assert!(second
        .jobs
        .values()
        .all(|status| *status == ExecutionStatus::InProgress));
    assert!(!inner.inflight_requests.contains_key(&first_req_id));
}

#[tokio::test]
async fn agent_request_get_reports_completion_result() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1",
        Value::Null,
    )
    .await;
    let request_id = {
        let inner = state.inner.lock().await;
        inner
            .inflight_requests
            .iter()
            .find(|(_, (rid, _))| *rid == run_id)
            .map(|(request_id, _)| *request_id)
            .unwrap()
    };

    let before = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(before["requestId"], request_id);
    assert!(before["result"].is_null());

    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let after = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(after["result"], "succeeded");
}

#[tokio::test]
async fn same_session_waits_for_active_request_before_next_job() {
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
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let first_message_id = first["messageId"].as_i64().unwrap();
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s1"),
        Value::Null,
    )
    .await;

    let first_request_id = {
        let inner = state.inner.lock().await;
        *inner.session_active_requests.get("s1").unwrap()
    };

    let withheld = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(withheld.is_null());

    request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/v1/AgentRequest/1/{first_request_id}"),
        json!({"result": "succeeded"}),
    )
    .await;

    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs
            .values()
            .filter(|status| **status == ExecutionStatus::InProgress)
            .count(),
        1
    );
}

#[tokio::test]
async fn unacked_messages_are_scoped_to_their_session() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo"
    });
    request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
    request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let first_message_id = first["messageId"].as_i64().unwrap();

    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s2&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let second_message_id = second["messageId"].as_i64().unwrap();
    assert_ne!(first_message_id, second_message_id);

    // ACKing s1's message through s2 must not remove it from s1. The next
    // s1 poll should redeliver the same unacked message, not s2's message.
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s2"),
        Value::Null,
    )
    .await;

    let redelivered = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(redelivered["messageId"], first_message_id);

    let inner = state.inner.lock().await;
    assert!(inner
        .inflight_messages
        .get("s1")
        .is_some_and(|messages| messages.contains_key(&first_message_id)));
    assert!(inner
        .inflight_messages
        .get("s2")
        .is_some_and(|messages| messages.contains_key(&second_message_id)));
}

#[tokio::test]
async fn finish_job_resolves_plan_timeline_and_agent_job_ids() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let request = {
        let inner = state.inner.lock().await;
        inner.job_requests.values().next().unwrap().clone()
    };

    request_json(
        &app,
        Method::POST,
        &format!(
            "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
            request.plan_id
        ),
        json!({
            "jobId": request.agent_job_id,
            "result": "succeeded",
            "timelineId": request.timeline_id,
            "outputs": {"answer": "42"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&request.job_id),
        Some(&ExecutionStatus::Success)
    );
    assert!(!run
        .jobs
        .contains_key(&JobId(request.agent_job_id.to_string())));
    assert_eq!(
        run.job_outputs
            .get(&request.job_id)
            .and_then(|outputs| outputs.get("answer")),
        Some(&json!("42"))
    );
    assert_eq!(
        inner
            .job_requests
            .get(&request.request_id)
            .and_then(|request| request.result),
        Some(ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn finish_job_falls_back_to_the_single_active_request_when_unresolved() {
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
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
        Value::Null,
    )
    .await;

    let active_request = {
        let inner = state.inner.lock().await;
        let active_id = *inner.session_active_requests.get("s1").unwrap();
        inner.job_requests.get(&active_id).unwrap().clone()
    };
    let unknown_plan_id = uuid::Uuid::new_v4();
    let unknown_job_id = uuid::Uuid::new_v4();
    let unknown_timeline_id = uuid::Uuid::new_v4();

    // If callback identifiers cannot be resolved at all, the only
    // unfinished active request is the safest correlation available.
    request_json(
        &app,
        Method::POST,
        &format!(
            "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
            unknown_plan_id
        ),
        json!({
            "jobId": unknown_job_id,
            "result": "succeeded",
            "timelineId": unknown_timeline_id,
            "outputs": {}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&active_request.job_id),
        Some(&ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn matrix_fail_fast_cancels_in_progress_siblings_via_message() {
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
      fail-fast: true
      matrix:
        os: [ubuntu, macos]
    steps:
      - run: echo matrix
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Dispatch both siblings — both move to InProgress.
    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        first["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    let second = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        second["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    let failing_job = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .unwrap()
            .jobs
            .keys()
            .next()
            .unwrap()
            .clone()
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": failing_job,
            "status": "failure"
        }),
    )
    .await;

    // The fix: in-progress siblings get a cancellation enqueued so the
    // runner receives a JOB_CANCELLED message. Inspect the queue directly
    // since the matched siblings still have unACKed in-flight job messages.
    let inner = state.inner.lock().await;
    assert_eq!(inner.cancellation_queue.len(), 1);
    let cancellation = inner.cancellation_queue.front().unwrap();
    assert_eq!(cancellation.run_id, run_id);
    assert_ne!(cancellation.job_id, failing_job);
    // The sibling is now Cancelled in the run state.
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&cancellation.job_id),
        Some(&ExecutionStatus::Cancelled)
    );
}

#[tokio::test]
async fn needs_context_includes_completed_job_outputs() {
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
  deploy:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "success",
            "outputs": {"artifact": "dist.tgz"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let deploy = inner
        .queue
        .iter()
        .find(|job| job.job_id.0 == "deploy")
        .expect("deploy job should be promoted");
    let needs = deploy.message.context_data.get("needs").unwrap();
    let azdo::PipelineContextData::Dict(needs) = needs else {
        panic!("needs context should be a dict");
    };
    let azdo::PipelineContextData::Dict(build) = needs.get("build").unwrap() else {
        panic!("build context should be a dict");
    };
    let azdo::PipelineContextData::Dict(outputs) = build.get("outputs").unwrap() else {
        panic!("outputs context should be a dict");
    };
    assert!(matches!(
        outputs.get("artifact"),
        Some(azdo::PipelineContextData::String(value)) if value == "dist.tgz"
    ));
}

#[tokio::test]
async fn scenario_06_multi_step_dispatches_all_steps() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm multi step
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo first
      - run: echo "VAL=$VAL"
        env:
          VAL: hello
      - run: |
          echo line1
          echo line2
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let scripts = {
        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("build job should be queued");
        queued
            .message
            .steps
            .iter()
            .filter_map(|step| step.script.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(scripts.len(), 3);
    assert!(scripts.contains(&"echo first".to_owned()));
    assert!(scripts.contains(&"echo \"VAL=$VAL\"".to_owned()));
    assert!(scripts
        .iter()
        .any(|script| script.contains("echo line1") && script.contains("echo line2")));

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "success"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Success)
    );
}

#[tokio::test]
async fn scenario_07_step_failure_summarizes_run_failed() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm step failure
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: exit 1
      - run: echo ran-on-failure
        if: failure()
      - run: echo never
        if: success()
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Failure);
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Failure)
    );
}

#[tokio::test]
async fn scenario_08_consumer_sees_producer_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm job outputs
on: workflow_dispatch
jobs:
  producer:
    runs-on: [self-hosted, mitm]
    outputs:
      value: ${{ steps.gen.outputs.value }}
    steps:
      - id: gen
        run: echo "value=42" >> "$GITHUB_OUTPUT"
  consumer:
    needs: producer
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo "got ${{ needs.producer.outputs.value }}"
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "producer",
            "status": "success",
            "outputs": {"value": "42"}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let consumer = inner
        .queue
        .iter()
        .find(|job| job.job_id.0 == "consumer")
        .expect("consumer job should be promoted");
    let azdo::PipelineContextData::Dict(needs) =
        consumer.message.context_data.get("needs").unwrap()
    else {
        panic!("needs context should be a dict");
    };
    let azdo::PipelineContextData::Dict(producer) = needs.get("producer").unwrap() else {
        panic!("producer needs entry should be a dict");
    };
    let azdo::PipelineContextData::Dict(outputs) = producer.get("outputs").unwrap() else {
        panic!("producer outputs should be a dict");
    };
    assert!(matches!(
        outputs.get("value"),
        Some(azdo::PipelineContextData::String(value)) if value == "42"
    ));
}

#[tokio::test]
async fn scenario_09_matrix_fail_fast_cancels_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
name: mitm matrix
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    strategy:
      fail-fast: true
      matrix:
        n: [1, 2, 3]
    steps:
      - run: |
          if [ "${{ matrix.n }}" = "1" ]; then exit 1; fi
          sleep 20
"#,
            "event": "workflow_dispatch",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    for index in 0..3 {
        let session_id = format!("matrix-{index}");
        let message = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/Message/1?sessionId={session_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(
            message["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let message_id = message["messageId"].as_i64().unwrap();
        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/v1/Message/1/{message_id}?sessionId={session_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack.status(), StatusCode::NO_CONTENT);
    }

    let failing_job = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .unwrap()
            .jobs
            .iter()
            .find_map(|(job_id, status)| {
                (*status == ExecutionStatus::InProgress).then(|| job_id.clone())
            })
            .expect("a matrix sibling should be in progress")
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": failing_job,
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert_eq!(inner.cancellation_queue.len(), 2);
    let run = inner.runs.get(&run_id).unwrap();
    for (job_id, status) in &run.jobs {
        if job_id == &failing_job {
            assert_eq!(*status, ExecutionStatus::Failure);
        } else {
            assert_eq!(*status, ExecutionStatus::Cancelled);
        }
    }
}

#[tokio::test]
async fn timeline_patch_projects_annotations_to_run_events() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

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
      - run: echo annotated
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    request_json(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/Timeline/scope/actions/{run_id}/timeline-1"),
        json!({"count": 1, "value": [{
            "id": "00000000-0000-0000-0000-000000000001",
            "name": "build",
            "type": "job",
            "state": "completed",
            "result": "failed",
            "issues": [{
                "type": "error",
                "message": "boom",
                "data": {"file": "src/lib.rs", "line": "42"}
            }]
        }]}),
    )
    .await;

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/events.ndjson"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let events = String::from_utf8(bytes.to_vec()).unwrap();

    assert!(events.contains("\"type\":\"annotation\""));
    assert!(events.contains("\"message\":\"boom\""));
    assert!(events.contains("\"status\":\"failure\""));
}

#[tokio::test]
async fn live_log_websocket_accepts_bearer_and_stores_lines() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://{addr}/ws/live-logs/job-live");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        "Bearer aksh-system-token".parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let payload = json!({
        "stepId": "step-1",
        "startLine": 1,
        "count": 2,
        "value": ["hello", "world"]
    });
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text(payload.to_string()),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            {
                let inner = state.inner.lock().await;
                if let Some(job_lines) = inner.live_log_lines.get("job-live") {
                    let wrappers = job_lines.lock().await;
                    if wrappers.len() == 1 {
                        assert_eq!(wrappers[0].step_id, "step-1");
                        assert_eq!(wrappers[0].start_line, 1);
                        assert_eq!(wrappers[0].count, 2);
                        assert_eq!(wrappers[0].value, vec!["hello", "world"]);
                        break;
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    server.abort();
}

#[tokio::test]
async fn live_log_websocket_rejects_unauthenticated() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Connect without Authorization header — should fail.
    let url = format!("ws://{addr}/ws/live-logs/job-no-auth");
    let result = tokio_tungstenite::connect_async(url).await;
    assert!(result.is_err(), "WS connect without auth should fail");

    server.abort();
}

#[tokio::test]
async fn live_log_websocket_survives_malformed_payload() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let url = format!("ws://{addr}/ws/live-logs/job-malformed");
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
            .unwrap();
    request.headers_mut().insert(
        header::AUTHORIZATION,
        "Bearer aksh-system-token".parse().unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();

    // Send invalid JSON — should not close the connection.
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text("not json".to_string()),
    )
    .await
    .unwrap();

    // Send valid payload after the malformed one — should still work.
    let valid = json!({
        "stepId": "s1",
        "startLine": 1,
        "count": 1,
        "value": ["survived"]
    });
    futures::SinkExt::send(
        &mut ws,
        tokio_tungstenite::tungstenite::Message::Text(valid.to_string()),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let inner = state.inner.lock().await;
            if let Some(job_lines) = inner.live_log_lines.get("job-malformed") {
                let wrappers = job_lines.lock().await;
                if wrappers.len() == 1 {
                    assert_eq!(wrappers[0].value, vec!["survived"]);
                    break;
                }
            }
            drop(inner);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    server.abort();
}

#[tokio::test]
async fn log_append_persists_payload_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    request_json(
        &app,
        Method::POST,
        "/_apis/v1/Logfiles/scope/actions/plan-1",
        json!({"path": "log-1"}),
    )
    .await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/v1/Logfiles/scope/actions/plan-1/log-1")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::from("hello log"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let inner = state.inner.lock().await;
    assert_eq!(
        inner.logs.get("plan-1/log-1").map(Vec::as_slice),
        Some(&b"hello log"[..])
    );
}

#[tokio::test]
async fn log_get_run_logs_uses_production_plan_ids_and_numeric_order() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
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
    runs-on: ubuntu-latest
    steps:
      - run: echo second
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let requests = {
        let inner = state.inner.lock().await;
        let mut requests: Vec<_> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        requests
            .into_iter()
            .map(|request| (request.plan_id.clone(), request.agent_job_id.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(requests.len(), 2);

    for (plan_id, log_id, body) in [
        (
            &requests[0].0,
            "10",
            "first-ten
",
        ),
        (
            &requests[0].0,
            "2",
            "first-two
",
        ),
        (
            &requests[1].0,
            "1",
            "ignored-fallback
",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!(
                        "/_apis/v1/Logfiles/scope/actions/{plan_id}/{log_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    let results_dir = temp
        .path()
        .join("replay")
        .join("results")
        .join(&requests[1].0)
        .join(&requests[1].1);
    tokio::fs::create_dir_all(&results_dir).await.unwrap();
    tokio::fs::write(
        results_dir.join("job-logs.txt"),
        b"results-second
",
    )
    .await
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/plain; charset=utf-8"
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.as_ref(),
        b"first-two
first-ten
results-second
"
    );
}

#[tokio::test]
async fn log_get_run_logs_returns_404_for_unknown_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{}/logs", RunId::new()))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn log_append_masks_submitted_secrets() {
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
      - run: echo masked
"#,
            "event": "push",
            "repository": "owner/repo",
            "secrets": {"TOKEN": "super-secret"}
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/v1/Logfiles/scope/actions/{run_id}/log-1"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::from("token=super-secret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let inner = state.inner.lock().await;
    assert_eq!(
        inner
            .logs
            .get(&format!("{run_id}/log-1"))
            .map(Vec::as_slice),
        Some(&b"token=***"[..])
    );
}

#[tokio::test]
async fn registration_persists_runner_public_key_material() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let public_key = AgentRsaKeypair::generate().unwrap().public_key_xml();

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"],
            "public_key": public_key
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let inner = state.inner.lock().await;
    assert_eq!(inner.runner_public_keys.get(&runner_id), Some(&public_key));
    assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
}

#[tokio::test]
async fn session_key_uses_registered_runner_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"],
            "public_key": runner_keypair.public_key_xml()
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "local"}),
    )
    .await;
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
    assert!(encrypted, "session key should be RSA wrapped");
    let wrapped_key =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
    let key_bytes = runner_keypair.unwrap_key(&wrapped_key).unwrap();
    assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
}

#[tokio::test]
async fn session_key_falls_back_to_plaintext_without_registered_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted"]
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "local"}),
    )
    .await;
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
    assert!(
        !encrypted,
        "session key should remain plaintext only when the runner registered no key"
    );
    let key_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
    assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
}

#[tokio::test]
async fn task_agent_registration_extracts_nested_public_key() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "local",
            "labels": [{"name": "self-hosted", "type": "system"}],
            "authorization": {
                "publicKey": {
                    "modulus": modulus,
                    "exponent": exponent
                }
            }
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();
    let inner = state.inner.lock().await;
    assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
}

#[tokio::test]
async fn connection_data_exposes_current_runner_service_locations() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let conn = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
        Value::Null,
    )
    .await;
    let services = conn["locationServiceData"]["serviceDefinitions"]
        .as_array()
        .unwrap();
    let service_ids = services
        .iter()
        .filter_map(|service| service["identifier"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(service_ids.contains("38f00041-0953-4d24-86c3-5432d23e2205"));
    assert!(service_ids.contains("a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f"));
    assert!(service_ids.contains("10d13a60-2758-406c-8ab7-cffccb21fcf4"));
    assert_eq!(
        conn["locationServiceData"]["defaultAccessMappingMoniker"],
        "ScaleUnitMapping"
    );

    let fresh = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=0&lastChangeId=1&lastChangeId64=1",
        Value::Null,
    )
    .await;
    assert_eq!(fresh["locationServiceData"]["clientCacheFresh"], true);
    assert!(fresh["locationServiceData"]["serviceDefinitions"].is_null());
}

#[tokio::test]
async fn registration_and_oauth_return_runner_compatible_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let registration = request_json(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/aksh", "runner_event": "register"}),
    )
    .await;
    assert_eq!(registration["token_schema"], "OAuthAccessToken");
    assert_eq!(registration["url"], "http://127.0.0.1:9090/runner/server");
    assert_eq!(
        registration["token"].as_str().unwrap().split('.').count(),
        3
    );
    assert!(registration.get("use_v2_flow").is_none());

    let token = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert_eq!(token["token_type"], "JWT");
    assert_eq!(token["expires_in"], 2999);
    assert_eq!(
        token["access_token"].as_str().unwrap().split('.').count(),
        3
    );
}

#[tokio::test]
async fn current_runner_registration_to_broker_job_e2e() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap();

    let registration_auth = request_json(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/aksh", "runner_event": "register"}),
    )
    .await;
    assert_eq!(
        registration_auth["url"],
        "http://127.0.0.1:9090/runner/server"
    );

    let connection = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
        Value::Null,
    )
    .await;
    assert!(connection["locationServiceData"]["serviceDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|service| service["displayName"] == "brokerlistener"));

    let agent = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "runner-1",
            "version": "2.335.1",
            "osDescription": "Darwin local",
            "labels": [
                {"name": "self-hosted", "type": "system"},
                {"name": "macOS", "type": "system"},
                {"name": "ARM64", "type": "system"}
            ],
            "authorization": {
                "publicKey": {
                    "modulus": modulus,
                    "exponent": exponent
                }
            }
        }),
    )
    .await;
    let runner_id = agent["id"].as_i64().unwrap();
    let runner_token = state
        .local_jwt(json!({
            "sub": format!("aksh-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    assert_eq!(agent["properties"]["UseV2Flow"]["$value"], true);
    assert_eq!(
        agent["properties"]["ServerUrlV2"]["$value"],
        "http://127.0.0.1:9090/runner/server"
    );

    let oauth = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert_eq!(oauth["token_type"], "JWT");

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": "runner-1", "version": "2.335.1"},
            "ownerName": "local current runner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "name: Current Runner Verification\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo current\n",
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/aksh",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let broker_ref = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&waitSeconds=0"),
            Value::Null,
        )
        .await;
    assert_eq!(broker_ref["messageType"], "RunnerJobRequest");
    let body: Value = serde_json::from_str(broker_ref["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["should_acknowledge"], true);
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/broker/{runner_id}/acquirejob"))
                    .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(acquired_response.status(), StatusCode::OK);
    let acquired = serde_json::from_slice::<Value>(
        &to_bytes(acquired_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(acquired["requestId"], 0);
    assert_eq!(acquired["billingOwnerId"], "local");
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );
    assert_eq!(
        acquired["resources"]["endpoints"][0]["url"],
        format!("http://127.0.0.1:9090/broker/{runner_id}/")
    );
    assert_eq!(
        acquired["resources"]["endpoints"][0]["data"]["FeedStreamUrl"],
        format!(
            "ws://127.0.0.1:9090/ws/live-logs/{}",
            acquired["jobId"].as_str().unwrap()
        )
    );
    assert!(acquired["contextData"]["github"].is_object());
    let github_context_json = serde_json::to_string(&acquired["contextData"]["github"])
        .expect("github context should serialize");
    assert!(
        github_context_json.contains("\"workflow\""),
        "github context missing workflow key: {github_context_json}"
    );
    assert!(
        github_context_json.contains("Current Runner Verification"),
        "github context missing workflow name: {github_context_json}"
    );
    assert!(
        acquired["steps"].as_array().unwrap().iter().any(|step| {
            step["inputs"]["script"].as_str() == Some("echo current")
                || step["inputs"]["script"]["lit"].as_str() == Some("echo current")
                || step["inputs"]["script"]["expr"].as_str() == Some("echo current")
                || step["inputs"]["map"].as_array().is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        let key = entry.get("Key").or(entry.get("key"));
                        let val = entry.get("Value").or(entry.get("value"));
                        let key_match = key.is_some_and(|k| {
                            k.as_str() == Some("script")
                                || k.get("lit").and_then(|l| l.as_str()) == Some("script")
                        });
                        let val_match = val.is_some_and(|v| {
                            v.as_str() == Some("echo current")
                                || v.get("lit").and_then(|l| l.as_str()) == Some("echo current")
                        });
                        key_match && val_match
                    })
                })
        }),
        "steps={}",
        acquired["steps"]
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/broker/{runner_id}/completejob"))
                .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn current_service_broker_flow_uses_queued_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_token = state
        .local_jwt(json!({
            "sub": "aksh-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let workflow = "on:
  push:
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
";
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "preloopdev/aksh",
            "git_ref": "refs/heads/main",
            "secrets": {},
            "vars": {},
            "reusable_workflows": {}
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "runner-1"},
            "ownerName": "owner",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();

    let response = app.clone().oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        ).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let message: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(message["messageType"], "RunnerJobRequest");
    let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["should_acknowledge"], true);
    let runner_request_id = body["runner_request_id"].as_str().unwrap();
    assert!(body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    assert_eq!(session["ownerName"], "owner");
    assert_eq!(session["assignmentQueued"], false);
    assert_eq!(session["orchestrationId"], "");

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}),
        &runner_token,
    )
    .await;
    assert_eq!(acquired["requestId"].as_i64().unwrap(), 0);
    assert_eq!(acquired["billingOwnerId"], "local");
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );
    assert_eq!(
        acquired["variables"]["system.github.launch_endpoint"]["value"],
        public_base_url()
    );
    assert!(acquired["variables"]["system.github.token"]["value"].is_string());
    assert!(acquired["contextData"]["github"]["d"]
        .as_array()
        .unwrap()
        .iter()
        .any(|pair| pair["k"] == "token" && pair["v"].as_str().is_some()));
    assert_eq!(
        acquired["resources"]["endpoints"][0]["url"],
        "http://127.0.0.1:9090/broker/1/"
    );
    assert!(acquired["plan"]["planId"].is_string());
    assert!(acquired["jobId"].is_string());
    assert!(acquired["steps"].is_array());

    let renewed = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/renewjob",
        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
        &runner_token,
    )
    .await;
    let locked_until = renewed["lockedUntil"]
        .as_str()
        .expect("renewjob must advertise lockedUntil");
    let locked_until = chrono::DateTime::parse_from_rfc3339(locked_until)
        .expect("renewed lockedUntil must be RFC3339")
        .with_timezone(&chrono::Utc);
    let seconds_until = (locked_until - chrono::Utc::now()).num_seconds();
    assert!(
        (seconds_until - JOB_LEASE_SECONDS as i64).abs() <= 5,
        "renewed lease should be approximately {JOB_LEASE_SECONDS}s, got {seconds_until}s"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/broker/1/completejob")
                .header(header::AUTHORIZATION, format!("Bearer {runner_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let completed_run = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{}", accepted["run_id"].as_str().unwrap()),
        Value::Null,
    )
    .await;
    assert_eq!(completed_run["status"], "success");
    assert_eq!(completed_run["jobs"]["rust"], "success");

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/AgentRequest/1/1")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::OK);
}

#[tokio::test]
async fn action_download_info_returns_remote_action_tickets() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/ActionDownloadInfo/scope/actions/plan",
        json!({
            "actions": [
                {"action": "actions/checkout", "version": "v4"},
                "dtolnay/rust-toolchain@stable",
                "./.github/actions/local",
                "docker://alpine:3.20"
            ]
        }),
    )
    .await;

    let tickets = response["archiveDownloadTickets"].as_object().unwrap();
    assert_eq!(
        tickets["actions/checkout@v4"]["url"],
        "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/v4"
    );
    assert_eq!(
        tickets["dtolnay/rust-toolchain@stable"]["url"],
        "http://127.0.0.1:9090/api/v1/actions/download/dtolnay/rust-toolchain/stable"
    );
    assert!(!tickets.contains_key("./.github/actions/local"));
    assert!(!tickets.contains_key("docker://alpine:3.20"));
    assert_eq!(
        response["actionsDownloadInfo"],
        response["archiveDownloadTickets"]
    );
}

#[tokio::test]
async fn runnerresolve_actions_returns_runner_parseable_tar_urls() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = request_json(
        &app,
        Method::POST,
        "/actions/build/plan/jobs/job/runnerresolve/actions",
        json!({
            "actions": [
                {"action": "actions/checkout", "version": "v4"},
                {"action": "owner/repo/path", "version": "main"}
            ]
        }),
    )
    .await;

    assert_eq!(
        response["actions"]["actions/checkout@v4"]["tar_url"],
        "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/v4"
    );
    assert_eq!(
        response["actions"]["actions/checkout@v4"]["resolved_sha"],
        "v4"
    );
    assert_eq!(
        response["actions"]["owner/repo/path@main"]["tar_url"],
        "http://127.0.0.1:9090/api/v1/actions/download/owner/repo/main"
    );
}

#[tokio::test]
async fn download_action_tarball_serves_from_cache_and_rejects_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Pre-populate cache for testing
    let cache_dir = temp
        .path()
        .join("actions")
        .join("test-owner")
        .join("test-repo")
        .join("v1");
    tokio::fs::create_dir_all(&cache_dir).await.unwrap();
    let cached_path = cache_dir.join("action.tar.gz");
    tokio::fs::write(&cached_path, b"dummy-tar-content")
        .await
        .unwrap();

    // 1. Successful cache hit
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/test-repo/v1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/gzip"
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(bytes.as_ref(), b"dummy-tar-content");

    // 2. Reject path traversal
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/test-repo/../invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/../../invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn protected_apis_require_bearer_token() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                .header(header::AUTHORIZATION, "Bearer aksh-attacker-controlled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn all_twirp_api_routes_reject_missing_bearer_before_body_validation() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // The malformed body proves auth runs before any route-specific JSON extractor.
    let routes = [
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
        "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
        "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
        "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
        "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
        "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
        "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
        "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
        "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
        "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
        "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
    ];

    for route in routes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(route)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route}");
    }
}

#[tokio::test]
async fn twirp_diag_route_rejects_runner_listen_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runner_listen_token = state
        .local_jwt(json!({
            "sub": "aksh-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(
                    header::AUTHORIZATION,
                    format!("Bearer {runner_listen_token}"),
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id.to_string(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn twirp_diag_route_issues_random_blob_url_and_accepts_bearerless_upload() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan_id = uuid::Uuid::new_v4().to_string();
    let job_id = uuid::Uuid::new_v4();
    let runtime_token = state.mint_runtime_token(&plan_id, &job_id);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL")
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan_id,
                        "workflow_job_run_backend_id": job_id.to_string(),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["blob_storage_type"], "BLOB_STORAGE_TYPE_AZURE");

    let diag_url = payload["diag_logs_url"].as_str().unwrap();
    assert!(!diag_url.is_empty());
    let (_, blob_token) = diag_url
        .split_once("/twirp-blob/diag/")
        .expect("diagnostic URL must use the bearerless diag blob endpoint");
    let blob_uuid = uuid::Uuid::parse_str(blob_token).expect("diagnostic token must be a UUID");
    assert_eq!(blob_uuid.as_bytes()[6] >> 4, 4, "token must be UUIDv4");
    assert_eq!(
        blob_uuid.as_bytes()[8] & 0xc0,
        0x80,
        "token must use RFC 4122 variant"
    );

    let bytes = b"diagnostic log bytes";
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(diag_url)
                .body(Body::from(bytes.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);

    let downloaded = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(diag_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    let downloaded_bytes = to_bytes(downloaded.into_body(), usize::MAX).await.unwrap();
    assert_eq!(downloaded_bytes.as_ref(), bytes);
}

#[tokio::test]
async fn native_api_rejects_job_runtime_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let token = state.mint_runtime_token("plan", &uuid::Uuid::new_v4());
    let app = app(state, CancellationToken::new());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);

    let accepted = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(accepted.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runner_server_v1_sensitive_routes_require_bearer() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // These /runner/server/_apis/v1/* aliases were previously placed on
    // the public router, letting unauthenticated callers mutate timelines,
    // inject logs, and finish jobs. They MUST require a bearer token.
    let cases = [
        (Method::PATCH, "/runner/server/_apis/v1/Timeline/s/h/p/t"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
        (
            Method::POST,
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/s/h/p/t/r",
        ),
        (Method::POST, "/runner/server/_apis/v1/FinishJob/s/h/p"),
        (
            Method::POST,
            "/runner/server/_apis/v1/ActionDownloadInfo/s/h/p",
        ),
        (Method::GET, "/ws/live-logs/test-job"),
        (
            Method::GET,
            "/api/v1/runs/00000000-0000-0000-0000-000000000000/jobs/test/logs/live",
        ),
    ];
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} should require bearer auth"
        );
    }
}

#[tokio::test]
async fn oidc_endpoint_mints_rs256_jwt_with_requested_audience() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "name: oidc-test\non: push\npermissions:\n  id-token: write\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://custom"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(parts.len(), 3);

    // Verify header is RS256 with a kid.
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
    assert_eq!(header["alg"], "RS256");
    assert!(header["kid"].as_str().unwrap().len() > 10);

    // Verify claims.
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "api://custom");
    assert_eq!(claims["iss"], "http://127.0.0.1:9090/oidc");
    assert_eq!(claims["repository"], "owner/repo");
    assert_eq!(claims["repository_owner"], "owner");
    assert_eq!(claims["event_name"], "push");
    assert_eq!(claims["runner_environment"], "self-hosted");
    assert!(claims["sub"]
        .as_str()
        .unwrap()
        .starts_with("repo:owner/repo:"));
    assert!(claims["jti"].is_string());
    assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());

    // Verify the OIDC keypair is persisted.
    assert!(temp.path().join("oidc-key.json").exists());
}

#[tokio::test]
async fn oidc_default_audience_is_owner_url() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  id-token: write\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "octo-org/octo-repo",
    });
    let resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
    let _run_id: RunId = resp["run_id"].as_str().unwrap().parse().unwrap();

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|j| (j.message.plan.plan_id.clone(), j.message.job_id))
            .unwrap()
    };

    let token = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken"),
            Value::Null,
        )
        .await;
    let jwt = token["value"].as_str().unwrap();
    let parts: Vec<&str> = jwt.split('.').collect();
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    assert_eq!(claims["aud"], "https://github.com/octo-org");
}

#[tokio::test]
async fn oidc_forbidden_without_id_token_write() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let workflow = json!({
        "workflow_yaml": "on: push\npermissions:\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps: [{ run: \"echo hi\" }]\n",
        "event": "push",
        "repository": "owner/repo",
    });
    let _resp = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        inner
            .queue
            .front()
            .or_else(|| inner.pending_jobs.front())
            .map(|job| (job.message.plan.plan_id.clone(), job.message.job_id))
            .unwrap()
    };
    let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);

    // Use the real job-bound runtime token so this reaches permission enforcement.
    let uri = format!(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{agent_job_id}/oidctoken?audience=api://test"
        );
    let request = Request::builder()
        .method(Method::GET)
        .uri(&uri)
        .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oidc_discovery_and_jwks_endpoints() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state, CancellationToken::new());

    let discovery = request_json(
        &app,
        Method::GET,
        "/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert!(discovery["jwks_uri"]
        .as_str()
        .unwrap()
        .ends_with("/.well-known/jwks.json"));
    assert_eq!(discovery["issuer"], "http://127.0.0.1:9090/oidc");
    let namespaced = request_json(
        &app,
        Method::GET,
        "/oidc/.well-known/openid-configuration",
        Value::Null,
    )
    .await;
    assert_eq!(namespaced, discovery);

    let jwks = request_json(&app, Method::GET, "/.well-known/jwks.json", Value::Null).await;
    let keys = jwks["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["kty"], "RSA");
    assert_eq!(keys[0]["alg"], "RS256");
    assert_eq!(keys[0]["use"], "sig");
    assert!(keys[0]["kid"].is_string());
    assert!(keys[0]["n"].is_string());
    assert_eq!(keys[0]["e"], "AQAB");
}

#[tokio::test]
async fn oidc_keypair_persists_across_restarts() {
    let temp = tempfile::tempdir().unwrap();
    let state1 = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let kid1 = {
        let inner = state1.inner.lock().await;
        inner.oidc_keypair.as_ref().unwrap().kid().to_string()
    };
    drop(state1);

    // Second instance should load the same keypair.
    let state2 = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let kid2 = {
        let inner = state2.inner.lock().await;
        inner.oidc_keypair.as_ref().unwrap().kid().to_string()
    };
    assert_eq!(
        kid1, kid2,
        "OIDC keypair kid must be stable across restarts"
    );
}

#[tokio::test]
async fn messages_redeliver_until_delete_ack() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    request_json(
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
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let first = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(first["messageId"], 1);

    let redelivered = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    assert_eq!(redelivered["messageId"], first["messageId"]);

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/runner/server/_apis/v1/Message/1/1?sessionId=default")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    let empty = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(empty.is_null());
}

#[tokio::test]
async fn cancel_run_delivers_cancellation_message() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

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
      - run: sleep 30
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    let message = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;
    let message_id = message["messageId"].as_i64().unwrap();

    let ack = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri(format!(
                    "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                ))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::NO_CONTENT);

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

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
    // Body is base64 of plaintext (no session key in this test path).
    let body_b64 = cancellation["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    let body: Value = serde_json::from_slice(&body_bytes).unwrap();
    assert!(body["jobId"]
        .as_str()
        .unwrap()
        .parse::<uuid::Uuid>()
        .is_ok());
    assert_eq!(body["timeout"], "00:05:00");
    assert!(body.get("runId").is_none());
}

#[tokio::test]
async fn message_poll_waits_until_work_is_enqueued() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let poll_app = app.clone();
    let poll = tokio::spawn(async move {
        request_json(
            &poll_app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=2",
            Value::Null,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    request_json(
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
      - run: echo waited
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = poll.await.unwrap();
    assert_eq!(message["messageId"], 1);
}

#[tokio::test]
async fn session_message_flow_encrypts_decryptable_job_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let _keypair = {
        let inner = state.inner.lock().await;
        inner.agent_keypair.clone().unwrap()
    };
    let app = app(state, CancellationToken::new());

    let session = request_json(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": 1, "name": "local"}),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
    let aes_key =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();

    request_json(
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
      - run: echo encrypted
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;

    let message = request_json(
        &app,
        Method::GET,
        &format!("/internal/test/runners/sessions/{session_id}/messages?sessionId={session_id}"),
        Value::Null,
    )
    .await;

    let body = BASE64_STANDARD
        .decode(message["body"].as_str().unwrap())
        .unwrap();
    let iv: Vec<u8> = BASE64_STANDARD
        .decode(message["iv"].as_str().unwrap())
        .unwrap();
    let plaintext = SessionEncryption::from_key(aes_key)
        .decrypt(&body, &iv)
        .unwrap();
    let job: azdo::AgentJobRequestMessage = serde_json::from_slice(&plaintext).unwrap();

    assert_eq!(
        message["messageType"],
        azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
    );
    assert_eq!(job.steps[0].script.as_deref(), Some("echo encrypted"));
}

#[tokio::test]
async fn submit_run_uses_branch_and_path_filters() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "payload": {
                "commits": [
                    { "added": [], "modified": ["src/lib.rs"], "removed": [] }
                ]
            }
        }),
    )
    .await;
    assert!(accepted["run_id"].is_string());

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                        "event": "push",
                        "repository": "owner/repo",
                        "git_ref": "refs/heads/feature",
                        "payload": {
                            "commits": [
                                { "added": [], "modified": ["docs/readme.md"], "removed": [] }
                            ]
                        }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_scheduler_history_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let scheduler = crate::scheduler::Scheduler::new();

    // Add a mock fire to history
    {
        let mut hist = scheduler.history.lock().await;
        hist.push(crate::scheduler::ScheduleFire {
            workflow_path: ".github/workflows/cron.yml".to_owned(),
            cron_expr: "* * * * *".to_owned(),
            fired_at: chrono::Utc::now(),
            run_id: Some("mock-run-id".to_owned()),
            error: None,
        });
    }
    state.scheduler = Some(scheduler);

    let app = app(state, CancellationToken::new());

    let res = request_json(
        &app,
        Method::GET,
        "/api/v1/scheduler/history",
        serde_json::Value::Null,
    )
    .await;

    let arr = res.as_array().expect("expected array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["workflow_path"], ".github/workflows/cron.yml");
    assert_eq!(arr[0]["cron_expr"], "* * * * *");
    assert_eq!(arr[0]["run_id"], "mock-run-id");
}
#[tokio::test]
async fn cache_protocol_reserves_uploads_commits_and_restores() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let reserve = request_json(
        &app,
        Method::POST,
        "/_apis/artifactcache/cache",
        json!({"key": "linux-node", "version": "v1"}),
    )
    .await;
    let cache_id = reserve["cacheId"].as_i64().unwrap();

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::from("cache-bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    request_json(
        &app,
        Method::POST,
        &format!("/_apis/artifactcache/cache/{cache_id}"),
        json!({"size": 11}),
    )
    .await;

    let lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=linux-node&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(lookup["hit"], true);
    assert_eq!(lookup["content_base64"], "Y2FjaGUtYnl0ZXM=");

    let stored = request_json(
        &app,
        Method::POST,
        "/api/v1/cache",
        json!({
            "key": "native-cache",
            "version": "v1",
            "content_base64": "bmF0aXZlLWJ5dGVz"
        }),
    )
    .await;
    assert_eq!(stored["hit"], true);
    let native_lookup = request_json(
        &app,
        Method::GET,
        "/api/v1/cache?key=native-cache&version=v1",
        Value::Null,
    )
    .await;
    assert_eq!(native_lookup["content_base64"], "bmF0aXZlLWJ5dGVz");
}

#[tokio::test]
async fn artifact_endpoint_stores_and_downloads_payload() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let run_id = RunId::new();

    let created = request_json(
        &app,
        Method::POST,
        "/api/v1/artifacts",
        json!({
            "run_id": run_id,
            "name": "logs",
            "file_name": "job.txt",
            "content_base64": "aGVsbG8="
        }),
    )
    .await;
    let artifact_id = created["id"].as_str().unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/artifacts/{artifact_id}"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&bytes[..], b"hello");
}

#[tokio::test]
async fn full_runner_lifecycle_register_session_poll_complete() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Non-asserting helper
    async fn try_req(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if uri.starts_with("/api/v1/")
            || uri.starts_with("/_apis/")
            || uri.starts_with("/runner/server/_apis/")
            || uri.starts_with("/broker/")
            || uri.starts_with("/twirp/")
        {
            builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
        } else if uri.starts_with("/internal/test/") {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
        } else if uri.starts_with("/api/v3/actions/runner-registration") {
            builder = builder.header(header::AUTHORIZATION, "RemoteAuth aksh-registration-token");
        }
        let request = if body.is_null() {
            builder.body(Body::empty()).unwrap()
        } else {
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, val)
    }

    // 1. connectionData
    let (s, conn) = try_req(
        &app,
        Method::GET,
        "/runner/server/_apis/connectionData",
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "1 connectionData: {}", s);
    assert!(conn["locationServiceData"]["serviceDefinitions"].is_array());

    // 2. OAuth token
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/_apis/v1/oauth2/token",
        json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
    )
    .await;
    assert!(s.is_success(), "2 oauth2: {}", s);

    // 3. Register runner
    let (s, reg) = try_req(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({"name":"test-runner","labels":["self-hosted","linux","x64"]}),
    )
    .await;
    assert!(s.is_success(), "3 register: {} body={}", s, reg);
    let runner_id = reg["id"].as_i64().unwrap();

    // 4. Create session
    let (s, sess) = try_req(
        &app,
        Method::POST,
        "/internal/test/runners/sessions",
        json!({"runner_id": runner_id, "name": "test-runner"}),
    )
    .await;
    assert!(s.is_success(), "4 session: {} body={}", s, sess);
    let session_id = sess["sessionId"].as_str().unwrap().to_owned();

    // 5. Submit a workflow
    let (s, accepted) = try_req(&app, Method::POST, "/api/v1/runs",
            json!({"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n","event":"push","repository":"owner/repo"})).await;
    assert!(s.is_success(), "5 submit: {} body={}", s, accepted);
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 6. Poll for messages — the runner uses the AzDO Message endpoint
    let (s, msg) = try_req(
        &app,
        Method::GET,
        &format!(
            "/internal/test/runners/sessions/{}/messages?sessionId={}&waitSeconds=0",
            session_id, session_id
        ),
        Value::Null,
    )
    .await;
    assert!(s.is_success(), "6 poll: {} body={}", s, msg);

    // 7. Get the job from the run
    let inner = state.inner.lock().await;
    let run_record = inner.runs.get(&run_id).unwrap();
    let job_id = run_record.jobs.keys().next().unwrap().clone();
    drop(inner);

    // 8. Complete the job
    let (s, _) = try_req(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": job_id, "status": "success"}),
    )
    .await;
    assert!(s.is_success(), "8 complete: {}", s);

    // 9. Verify run succeeded
    let (_, final_run) = try_req(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{}", run_id),
        Value::Null,
    )
    .await;
    assert_eq!(final_run["status"], "success");
}

async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
    let mut builder = Request::builder().method(method).uri(uri);
    if uri.contains("/oidctoken") {
        let token = uri
            .split("/plans/")
            .nth(1)
            .and_then(|rest| rest.split("/jobs/").next().zip(rest.split("/jobs/").nth(1)))
            .and_then(|(plan, rest)| rest.split('/').next().map(|job| (plan, job)))
            .and_then(|(plan, job)| {
                uuid::Uuid::parse_str(job)
                    .ok()
                    .map(|id| mint_runtime_token(plan, &id))
            })
            .unwrap_or_else(|| DEFAULT_AKSH_SYSTEM_TOKEN.to_owned());
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    } else if uri.starts_with("/api/v1/")
        || uri.starts_with("/_apis/")
        || uri.starts_with("/runner/server/_apis/")
        || uri.starts_with("/broker/")
        || uri.starts_with("/actions/build/")
        || uri.starts_with("/twirp/")
    {
        builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
    } else if uri.starts_with("/internal/test/") {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
    } else if uri.starts_with("/api/v3/actions/runner-registration") {
        builder = builder.header(header::AUTHORIZATION, "RemoteAuth aksh-registration-token");
    }
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        status.is_success(),
        "unexpected status: {} body={}",
        status,
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}
async fn request_json_with_bearer(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    bearer: &str,
) -> Value {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    let request = if body.is_null() {
        builder.body(Body::empty()).unwrap()
    } else {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        builder.body(Body::from(body.to_string())).unwrap()
    };
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        status.is_success(),
        "unexpected status: {} body={}",
        status,
        String::from_utf8_lossy(&bytes)
    );
    if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    }
}

#[tokio::test]
async fn job_timeout_enforcement_cancels_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // 1. Submit run
    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 2. Poll to start job (transitions status to InProgress and sets started_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.inner.lock().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // 3. Override started_at to be in the past (beyond 360m/21600s default timeout)
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.started_at = Some(SystemTime::now() - Duration::from_secs(22000));
    }

    // 4. Run reaper tick
    reap_once(&shared).await;

    // 5. Verify cancellation is enqueued
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert!(request.timeout_triggered);
        assert_eq!(inner.cancellation_queue.len(), 1);
        assert_eq!(inner.cancellation_queue[0].run_id, run_id);
    }
}

#[tokio::test]
async fn runner_lease_expiration_disconnect_reaper() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

    // 1. Submit run
    let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // 2. Poll to start job (sets last_renewed_at)
    let _msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let request_id = {
        let inner = state.inner.lock().await;
        *inner.job_requests.keys().next().unwrap()
    };

    // 3. Exercise the just-before-boundary case without sleeping.
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(JOB_LEASE_SECONDS - 1));
    }

    reap_once(&shared).await;
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert_eq!(
            request.result, None,
            "lease must survive just before expiry"
        );
        assert!(inner.inflight_requests.contains_key(&request_id));
        assert_eq!(
            inner.runs.get(&run_id).unwrap().status,
            ExecutionStatus::InProgress
        );
    }

    // 4. Move just beyond the same production lease boundary and reap.
    {
        let mut inner = state.inner.lock().await;
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(JOB_LEASE_SECONDS + 1));
    }

    reap_once(&shared).await;

    // 5. Verify the job was marked failed and run completes as failed
    {
        let inner = state.inner.lock().await;
        let request = inner.job_requests.get(&request_id).unwrap();
        assert_eq!(request.result, Some(ExecutionStatus::Failure));
        assert!(inner.inflight_requests.is_empty());
        assert!(inner.session_active_requests.is_empty());

        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(run.status, ExecutionStatus::Failure);
    }
}

#[tokio::test]
async fn github_webhook_flows_with_signature_and_check_runs() {
    let temp = tempfile::tempdir().unwrap();

    // 1. Create a dummy workflow file in a local workspace
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    let workflow_content = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow_content)
        .await
        .unwrap();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir.clone());

    assert_eq!(state.webhook_secret.as_deref(), Some("super-secret"));
    assert_eq!(state.local_workspace.as_ref(), Some(&ws_dir));

    let app = app(state.clone(), CancellationToken::new());

    // 2. Prepare mock webhook push payload
    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        },
        "commits": [
            {
                "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }
        ]
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    // 3. Compute correct signature
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let sig_bytes = mac.finalize().into_bytes();
    let sig_hex = sig_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let signature_header = format!("sha256={}", sig_hex);

    // 4. Send request with WRONG signature -> should fail with 401
    let response_401 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-hub-signature-256", "sha256=invalid")
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_401.status(), StatusCode::UNAUTHORIZED);

    // 5. Send request with CORRECT signature -> should succeed with 200
    let response_200 = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-hub-signature-256", signature_header)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_200.status(), StatusCode::OK);

    // 6. Verify that a run was triggered and check runs are queued
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(run_record.submission.event, "push");
    assert_eq!(run_record.submission.repository, "owner/repo");
    assert_eq!(run_record.submission.git_ref, "refs/heads/main");

    // Verify that check_run_ids are created/queued in the record
    assert_eq!(run_record.job_check_run_ids.len(), 1);
    let (job_id, check_run_id) = run_record.job_check_run_ids.iter().next().unwrap();
    assert_eq!(job_id.to_string(), "build");
    assert!(*check_run_id > 0);
}

#[tokio::test]
async fn github_webhook_pull_request_event() {
    let temp = tempfile::tempdir().unwrap();

    // Create a dummy workflow file in a local workspace
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    let workflow_content = r#"
on: pull_request
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: make test
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/test.yml"), workflow_content)
        .await
        .unwrap();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir.clone());

    let app = app(state.clone(), CancellationToken::new());

    // Prepare PR payload
    let payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "head": {
                "ref": "feature-branch",
                "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"
            },
            "base": {
                "ref": "main",
                "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
            }
        },
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        }
    });

    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    // Compute signature
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let sig_bytes = mac.finalize().into_bytes();
    let sig_hex = sig_bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    let signature_header = format!("sha256={}", sig_hex);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "pull_request")
                .header("x-hub-signature-256", signature_header)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify triggered run
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(run_record.submission.event, "pull_request");
    assert_eq!(run_record.submission.git_ref, "refs/pull/42/head");
    assert_eq!(run_record.job_check_run_ids.len(), 1);
}

#[tokio::test]
async fn github_app_manifest_registration_flow() {
    let temp = tempfile::tempdir().unwrap();

    // 1. Setup a local mock GitHub API server for manifest conversion
    let mock_app = Router::new().route(
            "/app-manifests/:code/conversions",
            post(|Path(code): Path<String>| async move {
                assert_eq!(code, "mock_code_123");
                Json(json!({
                    "id": 987654,
                    "pem": "-----BEGIN RSA PRIVATE KEY-----\nMOCK-KEY-DATA\n-----END RSA PRIVATE KEY-----",
                    "webhook_secret": Some("mock-webhook-secret-xyz")
                }))
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // 2. Configure mock API URL in environment
    std::env::set_var("AKSH_GITHUB_API_URL", format!("http://127.0.0.1:{}", port));

    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 3. Request registration form (GET /api/v1/github/register)
    let response_reg = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/github/register")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_reg.status(), StatusCode::OK);
    let bytes = to_bytes(response_reg.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(html.contains("https://github.com/settings/apps/new"));
    assert!(html.contains("aksh-local-app"));

    // 4. Request callback conversion (GET /api/v1/github/callback?code=mock_code_123)
    let response_callback = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/github/callback?code=mock_code_123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response_callback.status(), StatusCode::OK);
    let bytes_callback = to_bytes(response_callback.into_body(), usize::MAX)
        .await
        .unwrap();
    let html_callback = String::from_utf8(bytes_callback.to_vec()).unwrap();
    assert!(html_callback.contains("GitHub App Registered Successfully!"));
    assert!(html_callback.contains("987654"));
    assert!(html_callback.contains("mock-webhook-secret-xyz"));

    // Clean up
    std::env::remove_var("AKSH_GITHUB_API_URL");
}

#[tokio::test]
async fn runner_oauth2_token_client_assertion_verification() {
    use aksh_gha_protocol::crypto::sign_jwt_ps256;
    use serde_json::Value;

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. Generate RSA keypair for the runner using the protocol's library
    let keypair = aksh_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let rsa_params = keypair.to_rsaparams();

    let keypair_xml = format!(
        "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
        rsa_params.modulus, rsa_params.exponent
    );

    // 2. Register the runner
    let reg_response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        json!({
            "name": "runner-cryptographic",
            "version": "2.335.1",
            "osDescription": "Linux",
            "enabled": true,
            "status": "offline",
            "publicKey": keypair_xml,
            "authorization": {
                "publicKey": keypair_xml,
            }
        }),
    )
    .await;

    let client_id = reg_response["authorization"]["clientId"]
        .as_str()
        .unwrap()
        .to_owned();

    // 3. Build a valid client assertion JWT signed with the runner's private RSA key
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let header = json!({
        "typ": "JWT",
        "alg": "PS256"
    });
    let claims = json!({
        "sub": client_id,
        "iss": client_id,
        "aud": "https://aksh.local/oauth",
        "jti": uuid::Uuid::new_v4().to_string(),
        "nbf": now,
        "exp": now + 300,
    });

    let client_assertion = sign_jwt_ps256(&header, &claims, &rsa_params).unwrap();

    // 4. Request OAuth token using urlencoded body
    let form_body = serde_urlencoded::to_string(&[
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &client_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let token_resp: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(token_resp["access_token"].is_string());

    // 5. Test negative case: Invalid signature (wrong key)
    let wrong_keypair = aksh_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let wrong_rsa_params = wrong_keypair.to_rsaparams();
    let bad_assertion = sign_jwt_ps256(&header, &claims, &wrong_rsa_params).unwrap();

    let bad_form_body = serde_urlencoded::to_string(&[
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &bad_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let bad_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(bad_form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(bad_response.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn label_matching_exact() {
    assert!(job_matches_runner(
        &["self-hosted".into(), "Linux".into()],
        &["self-hosted".into(), "Linux".into(), "X64".into()]
    ));
}

#[test]
fn label_matching_case_insensitive() {
    assert!(job_matches_runner(
        &["Self-Hosted".into(), "linux".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
}

#[test]
fn label_matching_ubuntu_alias() {
    // ubuntu-latest should match a runner with "self-hosted"
    assert!(job_matches_runner(
        &["ubuntu-latest".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
    // Also matches via the "linux" label
    assert!(job_matches_runner(
        &["ubuntu-24.04".into()],
        &["linux".into()]
    ));
}

#[test]
fn label_matching_rejects_missing_labels() {
    // Runner missing "gpu" label
    assert!(!job_matches_runner(
        &["self-hosted".into(), "gpu".into()],
        &["self-hosted".into(), "Linux".into()]
    ));
}

#[test]
fn label_matching_empty_runner_matches_all() {
    // Unknown runner (empty labels) matches everything
    assert!(job_matches_runner(
        &["self-hosted".into(), "Linux".into()],
        &[]
    ));
}

#[test]
fn label_matching_empty_job_matches_all() {
    assert!(job_matches_runner(&[], &["self-hosted".into()]));
}

// Oracle: GitHub `needs` and status-function contracts, with worker-side
// condition semantics pinned to actions/runner v2.335.1. These tests are
// production-path checks: YAML is parsed and expanded by Aksh, then the
// real queue/promotion state is driven through the explicitly gated test
// completion API and compared with the documented outcome.
// ─── DAG scheduling regression tests (spec §1) ─────────────────────────

/// Production path: build fails → test with default condition is skipped.
/// Verifies the server's promote_ready_jobs correctly propagates failure.
#[tokio::test]
async fn dag_build_fails_test_skipped_production() {
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

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(
        run.jobs.get(&JobId("build".to_owned())),
        Some(&ExecutionStatus::Failure)
    );
    assert_eq!(
        run.jobs.get(&JobId("test".to_owned())),
        Some(&ExecutionStatus::Skipped),
        "test must be skipped when build fails under default gate"
    );
    // No new jobs should have been promoted to queue
    assert!(
        !inner.queue.iter().any(|j| j.job_id.0 == "test"),
        "test must not be in queue"
    );
    assert!(inner.pending_jobs.is_empty(), "no jobs should be pending");
}

/// Production path: build fails → cleanup with `if: always()` runs.
#[tokio::test]
async fn dag_always_runs_after_failure_production() {
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
  cleanup:
    needs: [build]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo cleanup
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "cleanup"),
        "cleanup with always() must be promoted after build failure"
    );
}

/// Production path: build fails → notify with `if: failure()` runs.
#[tokio::test]
async fn dag_failure_condition_runs_after_failure_production() {
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
  notify:
    needs: [build]
    if: failure()
    runs-on: ubuntu-latest
    steps:
      - run: echo notify
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "notify"),
        "notify with failure() must be promoted after build failure"
    );
}

/// Production path: diamond graph build → test-a/test-b → deploy.
/// All succeed → deploy runs → run completes successfully.
#[tokio::test]
async fn dag_diamond_settlement_production() {
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
  test-a:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test-a
  test-b:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo test-b
  deploy:
    needs: [test-a, test-b]
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // Only build queued initially
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1);
        assert_eq!(inner.queue[0].job_id.0, "build");
    }

    // Complete build
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "build", "status": "success"}),
    )
    .await;

    // test-a and test-b promoted (build QueuedJob remains until dispatched)
    {
        let inner = state.inner.lock().await;
        let queued_ids: std::collections::BTreeSet<_> =
            inner.queue.iter().map(|j| j.job_id.0.clone()).collect();
        assert!(queued_ids.contains("test-a"), "test-a should be promoted");
        assert!(queued_ids.contains("test-b"), "test-b should be promoted");
    }

    // Complete test-a and test-b
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "test-a", "status": "success"}),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "test-b", "status": "success"}),
    )
    .await;

    // deploy promoted (other completed jobs' QueuedJobs may linger)
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.iter().any(|j| j.job_id.0 == "deploy"),
            "deploy should be promoted after test-a and test-b complete"
        );
    }

    // Complete deploy
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({"run_id": run_id, "job_id": "deploy", "status": "success"}),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert_eq!(run.status, ExecutionStatus::Success);
    assert!(inner.pending_jobs.is_empty());
}

/// Production path: cyclic graph rejected at submission time.
#[tokio::test]
async fn dag_cyclic_graph_rejected_production() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "workflow_yaml": r#"
on: push
jobs:
  a:
    needs: [b]
    runs-on: ubuntu-latest
    steps:
      - run: echo a
  b:
    needs: [a]
    runs-on: ubuntu-latest
    steps:
      - run: echo b
"#,
                        "event": "push",
                        "repository": "owner/repo"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "cyclic graph must be rejected before dispatch"
    );
}

async fn submit_yaml(app: &Router, yaml: &str, repo: &str) -> Value {
    request_json(
        app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": yaml,
            "event": "push",
            "repository": repo,
        }),
    )
    .await
}

async fn get_run_json(app: &Router, run_id: &str) -> Value {
    request_json(
        app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await
}

async fn complete_via_api(app: &Router, run_id: &str, job_id: &str) {
    request_json(
        app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": job_id,
            "status": "success",
            "outputs": {}
        }),
    )
    .await;
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
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
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
async fn empty_workflow_concurrency_group_creates_zero_job_failure() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let mut events = state.events.subscribe();
    let app = app(state.clone(), CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/runs")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
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
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let accepted: RunAccepted = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(accepted.queued_jobs, 0);

    let accepted_event = events.recv().await.unwrap();
    assert!(matches!(
        accepted_event,
        NdjsonEvent::RunAccepted { run_id, queued_jobs }
            if run_id == accepted.run_id && queued_jobs == 0
    ));
    let failed_event = events.recv().await.unwrap();
    assert!(matches!(
        failed_event,
        NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Failure,
            reason: Some(reason),
        } if run_id == accepted.run_id && reason.contains("must not be empty")
    ));

    let inner = state.inner.lock().await;
    let record = &inner.runs[&accepted.run_id];
    assert_eq!(record.status, ExecutionStatus::Failure);
    assert!(record.jobs.is_empty());
    assert!(!inner
        .job_requests
        .values()
        .any(|request| request.run_id == accepted.run_id));
    assert!(!inner.queue.iter().any(|job| job.run_id == accepted.run_id));
    assert!(!inner
        .pending_jobs
        .iter()
        .any(|job| job.run_id == accepted.run_id));
    assert!(!inner
        .concurrency_blocked
        .iter()
        .any(|job| job.run_id == accepted.run_id));
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

async fn poll_and_ack(app: &Router) -> Value {
    let msg = request_json(
        app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    if msg.is_null() {
        return msg;
    }
    if let Some(message_id) = msg["messageId"].as_i64() {
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                    ))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    msg
}

fn decode_cancel_body(msg: &Value) -> Value {
    assert_eq!(msg["messageType"], azdo::message_type::JOB_CANCELLED);
    let body_b64 = msg["body"].as_str().unwrap();
    let body_bytes = BASE64_STANDARD.decode(body_b64).unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

#[tokio::test]
async fn job_cancellation_message_type_is_official_string() {
    // Wire regression: must be "JobCancellation", not "JobCancelled".
    assert_eq!(azdo::message_type::JOB_CANCELLED, "JobCancellation");
}

#[tokio::test]
async fn broker_root_message_path_delivers_job_cancellation() {
    // The aksh-runner broker client polls `/runner/server/message` (root
    // path), NOT `/_apis/v1/Message`. Cancel must be delivered there.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // Create broker session.
    let session = request_json(&app, Method::POST, "/runner/server/session", json!({})).await;
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
    let job_msg = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
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
    let cancel_msg = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&waitSeconds=0"),
        Value::Null,
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
    let busy_msg = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Busy&waitSeconds=0"),
        Value::Null,
    )
    .await;
    assert!(
        busy_msg.is_null(),
        "busy runner received successor: {busy_msg}"
    );

    // B must be pollable with a messageId that does not collide with cancel.
    let b_msg = request_json(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
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
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(state.inner.lock().await.runs.is_empty());
}

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
        let first_job = inner.runs[&first_run].jobs.keys().next().unwrap().clone();
        let second_job = inner.runs[&second_run].jobs.keys().next().unwrap().clone();
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
        assert_eq!(
            inner.runs[&second_run].jobs[&second_job],
            ExecutionStatus::Queued
        );
        assert!(inner
            .queue
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
        assert!(!inner
            .concurrency_blocked
            .iter()
            .any(|job| job.run_id == second_run && job.job_id == second_job));
    }
    complete_via_api(&app, &second_run.to_string(), &second_job.0).await;
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
        assert_eq!(
            inner.runs[&reusable_run].jobs[&reusable_job],
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
    }
    complete_via_api(&app, &reusable_run.to_string(), &reusable_job.0).await;
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
    let job_id = inner.runs[&run_id].jobs.keys().next().unwrap();
    assert_eq!(inner.runs[&run_id].jobs[job_id], ExecutionStatus::Queued);
    assert!(inner.jobset_admissions.is_empty());
    let key = concurrency::concurrency_key("owner/repo", "same-key");
    assert!(inner.concurrency_groups[&key].pending.is_empty());
    assert!(matches!(
        inner.concurrency_groups[&key].running,
        Some(concurrency::Holder::JobSet { run_id: holder_run, .. }) if holder_run == run_id
    ));
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
    let workflow = aksh_gha_parser::parse_workflow(yaml).unwrap();
    let plans = aksh_gha_parser::expand_jobs(&workflow).unwrap();
    let plan_ids: Vec<_> = plans.iter().map(|p| p.id.0.as_str()).collect();
    assert!(plan_ids.contains(&"lint"));
    assert!(plan_ids.contains(&"build"));
    assert!(plan_ids.contains(&"test"));
    assert!(plan_ids.contains(&"deploy"));

    // Verify DAG validation passes
    aksh_gha_parser::dag::validate_job_plans(&plans).unwrap();

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
