//! preloop-runner-server integration tests — runs_api group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

/// The list endpoint projects steps like the single-run endpoint.
///
/// Step records live in the attempt manifest, not in the stored run, so a
/// handler that clones `inner.runs` directly returns empty step arrays even
/// though `GET /api/v1/runs/{id}` has the current ones.
#[tokio::test]
async fn list_runs_projects_step_records() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;

    let response = request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": jobs[0].1,
            "workflow_job_run_backend_id": jobs[0].2,
            "steps": [{
                "external_id": ids[0],
                "number": 2,
                "name": "Run echo one",
                "status": 6,
                "conclusion": 2
            }]
        }),
    )
    .await;
    assert_eq!(response["ok"], true);

    let listed = request_json(&app, Method::GET, "/api/v1/runs", json!(null)).await;
    let run = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["run_id"] == run_id.to_string())
        .expect("the submitted run must be listed");
    let steps = run["jobs_list"][0]["steps"].as_array().unwrap();
    assert_eq!(
        steps.len(),
        3,
        "the list endpoint must carry the declared steps: {steps:?}"
    );
    assert_eq!(steps[0]["conclusion"], "success");
    assert_eq!(steps[0]["name"], "Run echo one");
}

#[tokio::test]
async fn list_runs_puts_active_work_before_newer_terminal_runs() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let first: RunId = submit_simple_run(&app).await["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let second: RunId = submit_simple_run(&app).await["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let (active, terminal) = if first < second {
        (first, second)
    } else {
        (second, first)
    };
    {
        let mut inner = state.inner.lock().await;
        let completed = inner.runs.get_mut(&terminal).unwrap();
        completed.status = ExecutionStatus::Success;
        completed.completed_at = Some(chrono::Utc::now());
    }

    let listed = request_json(&app, Method::GET, "/api/v1/runs?limit=2", json!(null)).await;
    let runs = listed.as_array().unwrap();
    assert_eq!(runs[0]["run_id"], active.to_string());
    assert_eq!(runs[0]["status"], "queued");
    assert_eq!(runs[1]["run_id"], terminal.to_string());
}

/// A runner report persists the attempt, so a restart keeps step state.
///
/// Step records deliberately do not ride in `runs.record_blob` (which reseals
/// the workflow YAML and event payload on every run event) nor in
/// `MetaSnapshot` (every field of which is sealed on each `store_meta_only`),
/// and the run-event projection no longer carries them at all. The only thing
/// that persists them is `store_job_steps`, called from the reconciliation
/// paths — so this drives a real `WorkflowStepsUpdate` rather than forcing a
/// snapshot, which is what makes it a regression test for losing step
/// conclusions across a restart.

/// A runner report persists the attempt, so a restart keeps step state.
///
/// Step records deliberately do not ride in `runs.record_blob` (which reseals
/// the workflow YAML and event payload on every run event) nor in
/// `MetaSnapshot` (every field of which is sealed on each `store_meta_only`),
/// and the run-event projection no longer carries them at all. The only thing
/// that persists them is `store_job_steps`, called from the reconciliation
/// paths — so this drives a real `WorkflowStepsUpdate` rather than forcing a
/// snapshot, which is what makes it a regression test for losing step
/// conclusions across a restart.
#[tokio::test]
async fn step_manifests_survive_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let (run_id, plan_id, agent_job_id, ids) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
        let ids = workflow_step_ids(&state, run_id, "build").await;
        assert_eq!(ids.len(), 3);
        let (plan_id, agent_job_id) = (jobs[0].1.clone(), jobs[0].2.clone());

        let response = request_json(
            &app,
            Method::POST,
            "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            json!({
                "workflow_run_backend_id": plan_id,
                "workflow_job_run_backend_id": agent_job_id,
                "steps": [{
                    "external_id": ids[1],
                    "number": 3,
                    "name": "Run echo two",
                    "status": 6,
                    "conclusion": 2
                }]
            }),
        )
        .await;
        assert_eq!(response["ok"], true);
        (run_id, plan_id, agent_job_id, ids)
    };

    // A fresh AppState over the same state dir is the restart.
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let restored = workflow_step_ids(&state, run_id, "build").await;
    assert_eq!(
        restored, ids,
        "declared step ids and their order must come back intact"
    );

    // The reported conclusion came back too, not just the identities.
    {
        let inner = state.inner.lock().await;
        let records = &inner.job_steps[&agent_job_id.parse::<uuid::Uuid>().unwrap()];
        let reported = records
            .iter()
            .find(|step| step.id == ids[1])
            .expect("the reported step must be restored");
        assert_eq!(reported.conclusion, "success");
        assert_eq!(reported.runner_number, Some(3));
    }

    write_step_job_logs(
        &temp,
        &plan_id,
        &agent_job_id,
        &[(ids[1].as_str(), "second step\n")],
    )
    .await;
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body, b"second step\n",
        "`--step` must still resolve after a restart"
    );
}

/// A step report made after a restart is still persisted.
///
/// The out-of-order write guard compares an in-memory revision against the
/// persisted one, so the counter has to resume above what is on disk. Left at
/// zero it hands every post-restart write a revision the stored rows already
/// exceed, and the upsert discards them — invisibly, because memory stays
/// authoritative until the next restart drops the conclusion.

/// A step report made after a restart is still persisted.
///
/// The out-of-order write guard compares an in-memory revision against the
/// persisted one, so the counter has to resume above what is on disk. Left at
/// zero it hands every post-restart write a revision the stored rows already
/// exceed, and the upsert discards them — invisibly, because memory stays
/// authoritative until the next restart drops the conclusion.
#[tokio::test]
async fn step_reports_after_a_restart_are_persisted() {
    let temp = tempfile::tempdir().unwrap();
    let report = |app: axum::Router, plan_id: String, agent_job_id: String, id: String, n: i64| async move {
        let response = request_json(
            &app,
            Method::POST,
            "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            json!({
                "workflow_run_backend_id": plan_id,
                "workflow_job_run_backend_id": agent_job_id,
                "steps": [{
                    "external_id": id,
                    "number": n,
                    "name": "Run echo one",
                    "status": 6,
                    "conclusion": 2
                }]
            }),
        )
        .await;
        assert_eq!(response["ok"], true);
    };

    let (plan_id, agent_job_id, ids) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
        let ids = workflow_step_ids(&state, run_id, "build").await;
        let (plan_id, agent_job_id) = (jobs[0].1.clone(), jobs[0].2.clone());
        // Two reports before the restart, so the persisted revision is above
        // the value a fresh counter would hand out first.
        report(
            app.clone(),
            plan_id.clone(),
            agent_job_id.clone(),
            ids[0].clone(),
            2,
        )
        .await;
        report(
            app.clone(),
            plan_id.clone(),
            agent_job_id.clone(),
            ids[1].clone(),
            3,
        )
        .await;
        (plan_id, agent_job_id, ids)
    };

    {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        report(
            app,
            plan_id.clone(),
            agent_job_id.clone(),
            ids[2].clone(),
            4,
        )
        .await;
    }

    // Only a second restart can tell whether that write reached the store.
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = state.inner.lock().await;
    let records = &inner.job_steps[&agent_job_id.parse::<uuid::Uuid>().unwrap()];
    let reported = records
        .iter()
        .find(|step| step.id == ids[2])
        .expect("the post-restart step must be restored");
    assert_eq!(
        reported.conclusion, "success",
        "a report made after a restart must survive the next one"
    );
    assert_eq!(reported.runner_number, Some(4));
}

/// Every surface showing a whole step list uses execution order.
///
/// The stored manifest is seeded with declared steps and then appends
/// synthetic ones as the runner reports them, so its raw order puts
/// `Set up job` last despite it running first. A step id is a v4 UUID, so it
/// cannot supply the order either.

/// Every surface showing a whole step list uses execution order.
///
/// The stored manifest is seeded with declared steps and then appends
/// synthetic ones as the runner reports them, so its raw order puts
/// `Set up job` last despite it running first. A step id is a v4 UUID, so it
/// cannot supply the order either.
#[tokio::test]
async fn step_lists_and_job_logs_follow_execution_order() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;
    let (plan_id, agent_job_id) = (jobs[0].1.clone(), jobs[0].2.clone());

    // The runner reports `Set up job` first, then the declared steps at the
    // offset positions the golden capture shows (declared step 1 is number 2).
    let setup_id = uuid::Uuid::new_v4().to_string();
    let report = |external_id: &str, number: u64, name: &str| {
        serde_json::json!({
            "external_id": external_id,
            "number": number,
            "name": name,
            "status": 6,
            "conclusion": 2
        })
    };
    let response = request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [
                report(&setup_id, 1, "Set up job"),
                report(&ids[0], 2, "Run echo one"),
                report(&ids[1], 3, "Run echo two"),
                report(&ids[2], 4, "Run echo three"),
            ]
        }),
    )
    .await;
    assert_eq!(response["ok"], true);

    // The run record lists the synthetic setup step first.
    let run = get_run_json(&app, &run_id.to_string()).await;
    let names: Vec<&str> = run["jobs_list"][0]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["name"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        names,
        vec![
            "Set up job",
            "Run echo one",
            "Run echo two",
            "Run echo three"
        ],
        "the run record must show execution order, not seeded-then-appended"
    );

    // The whole-job log concatenates in the same order, even though the ids
    // sort differently.
    write_step_job_logs(
        &temp,
        &plan_id,
        &agent_job_id,
        &[
            (ids[2].as_str(), "three\n"),
            (ids[0].as_str(), "one\n"),
            (setup_id.as_str(), "setup\n"),
            (ids[1].as_str(), "two\n"),
        ],
    )
    .await;
    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        String::from_utf8_lossy(&body),
        "setup\none\ntwo\nthree\n",
        "whole-job output must follow execution order"
    );

    // `--step` still counts declared steps only, so setup takes no slot.
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"one\n");
}

/// A completion reconciles the attempt that actually reported.
///
/// `job_requests` is keyed by monotonic request id, so picking the first match
/// for `(run_id, job_id)` selects the *oldest* dispatch. A re-dispatched job
/// then applied the new attempt's `external_id`s to the previous attempt's
/// manifest — matching nothing — and terminalized that older attempt's steps
/// while the run view still projected the newer one as in-flight.

/// A completion reconciles the attempt that actually reported.
///
/// `job_requests` is keyed by monotonic request id, so picking the first match
/// for `(run_id, job_id)` selects the *oldest* dispatch. A re-dispatched job
/// then applied the new attempt's `external_id`s to the previous attempt's
/// manifest — matching nothing — and terminalized that older attempt's steps
/// while the run view still projected the newer one as in-flight.
#[tokio::test]
async fn completion_reconciles_the_reporting_attempt_not_the_oldest() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _) = two_job_run_for_log_filters(&app, &state).await;
    let first_ids = workflow_step_ids(&state, run_id, "build").await;
    let first_step_id = first_ids[0].clone();

    // Re-dispatch: a newer request, its own agent job id, its own step ids.
    let (first_agent_job_id, second_agent_job_id, second_step_id) = {
        let mut inner = state.inner.lock().await;
        let mut record = inner
            .job_requests
            .values()
            .find(|request| request.run_id == run_id && request.job_id.0 == "build")
            .cloned()
            .expect("first attempt must exist");
        let first_agent_job_id = record.agent_job_id;
        let request_id = inner.job_requests.keys().copied().max().unwrap_or(0) + 1;
        let agent_job_id = uuid::Uuid::new_v4();
        record.request_id = request_id;
        record.agent_job_id = agent_job_id;
        record.plan_id = uuid::Uuid::new_v4().to_string();
        record.result = None;
        let step_id = uuid::Uuid::new_v4().to_string();
        inner.job_steps.insert(
            agent_job_id,
            vec![crate::models::StepRecord::workflow(
                step_id.clone(),
                0,
                "Run echo build".to_owned(),
                Some("__run".to_owned()),
            )],
        );
        inner.agent_job_requests.insert(agent_job_id, request_id);
        inner.job_requests.insert(request_id, record);
        (first_agent_job_id, agent_job_id, step_id)
    };

    let _ = crate::distributed_task::complete_job_inner(
        state.shared(),
        preloop_gha_protocol::JobCompletion {
            run_id,
            job_id: preloop_gha_protocol::JobId("build".to_owned()),
            agent_job_id: Some(second_agent_job_id),
            status: ExecutionStatus::Success,
            outputs: Default::default(),
            annotations: Vec::new(),
            step_results: vec![preloop_gha_protocol::CompletionStepResult {
                external_id: Some(second_step_id.clone()),
                number: Some(2),
                name: Some("Run echo build".to_owned()),
                status: Some(serde_json::json!("completed")),
                conclusion: Some(serde_json::json!("skipped")),
            }],
        },
    )
    .await;

    let inner = state.inner.lock().await;
    let reporting = &inner.job_steps[&second_agent_job_id];
    assert_eq!(
        reporting[0].conclusion, "skipped",
        "the reporting attempt takes the completion's step conclusion"
    );
    let earlier = &inner.job_steps[&first_agent_job_id];
    assert_eq!(
        earlier[0].id, first_step_id,
        "the earlier attempt keeps its own step identity"
    );
    assert_eq!(
        earlier[0].conclusion, "pending",
        "a completion for one attempt must not terminalize another's steps"
    );
}

/// A second dispatch of the same job gets its own manifest.
///
/// `build_task_step` mints a fresh `TaskStep` id per build, so a job-scoped
/// manifest would overwrite the mapping the first attempt's `step-<id>.txt`
/// blobs are named after, and that attempt's logs would become unreachable.
/// Keying by `agent_job_id` keeps both attempts resolvable.

/// A second dispatch of the same job gets its own manifest.
///
/// `build_task_step` mints a fresh `TaskStep` id per build, so a job-scoped
/// manifest would overwrite the mapping the first attempt's `step-<id>.txt`
/// blobs are named after, and that attempt's logs would become unreachable.
/// Keying by `agent_job_id` keeps both attempts resolvable.
#[tokio::test]
async fn step_manifests_are_scoped_per_job_attempt() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let first_ids = workflow_step_ids(&state, run_id, "build").await;
    assert_eq!(first_ids.len(), 1);

    // Simulate a re-dispatch: a new attempt with a new agent job id and new
    // step ids, exactly as a fresh `build_job_artifacts` would produce.
    let (second_plan_id, second_agent_job_id, second_step_id) = {
        let mut inner = state.inner.lock().await;
        let mut record = inner
            .job_requests
            .values()
            .find(|request| request.run_id == run_id && request.job_id.0 == "build")
            .cloned()
            .expect("first attempt must exist");
        let request_id = inner.job_requests.keys().copied().max().unwrap_or(0) + 1;
        let agent_job_id = uuid::Uuid::new_v4();
        record.request_id = request_id;
        record.agent_job_id = agent_job_id;
        record.plan_id = uuid::Uuid::new_v4().to_string();
        let step_id = uuid::Uuid::new_v4().to_string();
        inner.job_steps.insert(
            agent_job_id,
            vec![crate::models::StepRecord::workflow(
                step_id.clone(),
                0,
                "Run echo build".to_owned(),
                Some("__run".to_owned()),
            )],
        );
        let plan_id = record.plan_id.clone();
        inner.agent_job_requests.insert(agent_job_id, request_id);
        inner.job_requests.insert(request_id, record);
        (plan_id, agent_job_id.to_string(), step_id)
    };

    // The run API — not an internal helper — must show the newest attempt's
    // steps. Asserting through `workflow_step_ids` alone could not detect a
    // stale projection, because it reads the same map the projection reads.
    let run = get_run_json(&app, &run_id.to_string()).await;
    let projected: Vec<&str> = run["jobs_list"]
        .as_array()
        .unwrap()
        .iter()
        .find(|detail| detail["job_id"] == "build")
        .expect("build must be in the run record")["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|step| step["id"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(
        projected,
        vec![second_step_id.as_str()],
        "the run record must project the newest attempt, not the first"
    );

    // The earlier attempt keeps its own mapping, which is what makes its
    // already-uploaded `step-<id>.txt` blobs still reachable.
    {
        let inner = state.inner.lock().await;
        let first_agent_job_id: uuid::Uuid = jobs[0].2.parse().unwrap();
        let retained = inner
            .job_steps
            .get(&first_agent_job_id)
            .expect("the earlier attempt's manifest must survive the re-dispatch");
        assert_eq!(retained[0].id, first_ids[0]);
    }

    // Both attempts' logs resolve, each through its own manifest.
    write_step_job_logs(
        &temp,
        &jobs[0].1,
        &jobs[0].2,
        &[(first_ids[0].as_str(), "first attempt\n")],
    )
    .await;
    write_step_job_logs(
        &temp,
        &second_plan_id,
        &second_agent_job_id,
        &[(second_step_id.as_str(), "second attempt\n")],
    )
    .await;
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=1")).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("first attempt") && text.contains("second attempt"),
        "each attempt resolves step 1 through its own manifest: {text}"
    );
}

#[tokio::test]
async fn log_run_logs_step_filter_selects_one_step() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;
    assert_eq!(ids.len(), 3, "manifest must carry the three declared steps");
    write_step_job_logs(
        &temp,
        &jobs[0].1,
        &jobs[0].2,
        &[
            (ids[0].as_str(), "step one\n"),
            (ids[1].as_str(), "step two\n"),
            (ids[2].as_str(), "step three\n"),
        ],
    )
    .await;

    for (step, expected) in [(1, "step one\n"), (2, "step two\n"), (3, "step three\n")] {
        let (status, body) = get_logs(
            &app,
            format!("/api/v1/runs/{run_id}/logs?job=build&step={step}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "step {step}");
        assert_eq!(body, expected.as_bytes(), "step {step} content");
    }
}

#[tokio::test]
async fn log_run_logs_step_filter_uses_workflow_step_ids() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;
    let workflow_step_id = ids.first().expect("manifest must carry the declared step");

    // A synthetic runner step ("Set up job") uploads a blob too, and sorts
    // ahead of the user step by both name and upload time. `step=1` must still
    // resolve through the manifest and pick the declared step.
    write_step_job_logs(
        &temp,
        &jobs[0].1,
        &jobs[0].2,
        &[
            ("00000000-0000-0000-0000-000000000000", "setup output\n"),
            (workflow_step_id.as_str(), "user step output\n"),
        ],
    )
    .await;

    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"user step output\n");

    // The declared step is the only one `--step` can address; the synthetic
    // blob never occupies a slot of its own.
    let (status, _) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=2")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a synthetic upload must not become step 2"
    );
}

#[tokio::test]
async fn log_run_logs_step_out_of_range_is_404() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = three_step_run_for_log_filters(&app, &state).await;
    let ids = workflow_step_ids(&state, run_id, "build").await;
    write_step_job_logs(
        &temp,
        &jobs[0].1,
        &jobs[0].2,
        &[
            (ids[0].as_str(), "step one\n"),
            (ids[1].as_str(), "step two\n"),
        ],
    )
    .await;

    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=9")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let message = String::from_utf8_lossy(&body);
    assert!(
        message.contains('3'),
        "error should report how many declared steps exist: {message}"
    );
}

#[tokio::test]
async fn log_run_logs_step_zero_is_rejected_as_one_based() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    write_step_job_logs(&temp, &jobs[0].1, &jobs[0].2, &[("a", "step one\n")]).await;

    let (status, _) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=0")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn log_run_logs_step_on_merged_upload_is_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    // A merged job log carries no step boundaries.
    write_merged_job_log(&temp, &jobs[0].1, &jobs[0].2, "everything\n").await;

    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=1")).await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "must refuse rather than pass the whole job off as one step"
    );
    assert!(
        !String::from_utf8_lossy(&body).contains("everything"),
        "conflict body must not return the unsplit log"
    );
}

#[tokio::test]
async fn log_run_logs_step_without_job_in_multi_job_run_is_ambiguous() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    write_step_job_logs(&temp, &jobs[0].1, &jobs[0].2, &[("a", "step one\n")]).await;

    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?step=1")).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "step numbering restarts per job, so this names two things"
    );
    let message = String::from_utf8_lossy(&body);
    assert!(
        message.contains("build") && message.contains("test"),
        "error should name the candidate jobs: {message}"
    );
}

#[tokio::test]
async fn log_run_logs_step_filter_refuses_live_console_blocks() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;

    // Nothing on disk yet: a job still in flight streams console blocks keyed
    // by the runner's numeric log id. Those ids count every record the runner
    // opened, `Set up job` among them, so they are not declared-step
    // positions — block "2" is not step 1.
    for (log_id, body) in [("2", "live block two\n"), ("10", "live block ten\n")] {
        let plan = &jobs[0].1;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/_apis/v1/Logfiles/scope/actions/{plan}/{log_id}"))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    // Refusing beats guessing: indexing these blocks is the same numbering
    // error `--step` was fixed to remove for durable blobs.
    let (status, body) =
        get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build&step=1")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let message = String::from_utf8_lossy(&body);
    assert!(
        message.contains("has not uploaded per-step logs yet"),
        "the error must say why the step cannot be identified: {message}"
    );

    // The whole-job read still serves the streamed output, in numeric console
    // order rather than lexicographic: 2 precedes 10.
    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs?job=build")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"live block two\nlive block ten\n");
}

#[tokio::test]
async fn live_run_logs_native_route_requires_job_when_ambiguous() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _jobs) = two_job_run_for_log_filters(&app, &state).await;

    let (status, body) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs/live")).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "two jobs cannot be followed on one stream"
    );
    let message = String::from_utf8_lossy(&body);
    assert!(
        message.contains("build") && message.contains("test"),
        "error should name the candidate jobs: {message}"
    );
}

#[tokio::test]
async fn live_run_logs_native_route_rejects_unknown_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _jobs) = two_job_run_for_log_filters(&app, &state).await;

    let (status, _) = get_logs(&app, format!("/api/v1/runs/{run_id}/logs/live?job=nope")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn live_run_logs_native_route_rejects_uuid_from_other_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_foreign_run_id, foreign_jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (run_id, _jobs) = two_job_run_for_log_filters(&app, &state).await;
    let foreign_job_uuid = foreign_jobs[0].2.clone();

    // Leave a globally keyed buffer behind, then ask for that UUID through a
    // different run. A live-log key is not a sufficient authorization to read
    // another run's output.
    crate::live_logs::record_live_log_wrapper(
        &state.shared(),
        &foreign_job_uuid,
        preloop_gha_protocol::LiveLogFeedLinesWrapper {
            step_id: "foreign".into(),
            start_line: 1,
            count: 1,
            value: vec!["must stay private".into()],
        },
    )
    .await;

    let (status, body) = get_logs(
        &app,
        format!("/api/v1/runs/{run_id}/logs/live?job={foreign_job_uuid}"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        !String::from_utf8_lossy(&body).contains("must stay private"),
        "cross-run UUID lookup must not expose the foreign buffer"
    );
}

#[tokio::test]
async fn live_run_logs_native_route_defaults_single_job_run() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  only:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // One job means no ambiguity, so omitting `job` must stream rather than 400.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs/live"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
}

#[tokio::test]
async fn live_run_logs_native_route_rejects_unauthenticated() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _jobs) = two_job_run_for_log_filters(&app, &state).await;

    // The runner-protocol twin of this route is unreachable with a native
    // bearer; this one must still refuse an anonymous caller.
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs/live?job=build"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Drive one job of a run to a terminal status through the same path every
/// completion funnels through (`complete_job_inner`).
#[tokio::test]
async fn live_log_stream_ends_when_job_completes() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let build_uuid = jobs[0].2.clone();

    // Emit a line so a follower has something, then open the stream.
    crate::live_logs::record_live_log_wrapper(
        &state.shared(),
        &build_uuid,
        preloop_gha_protocol::LiveLogFeedLinesWrapper {
            step_id: "s1".into(),
            start_line: 1,
            count: 1,
            value: vec!["building...".into()],
        },
    )
    .await;

    let response = open_live(&app, format!("/api/v1/runs/{run_id}/logs/live?job=build")).await;
    assert_eq!(response.status(), StatusCode::OK);

    // Complete the job on another task; the open stream must then end on its
    // own, carrying the line that was already buffered.
    let completer = {
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            complete_job(&state, run_id, "build", ExecutionStatus::Success).await;
        })
    };
    let body = read_sse_to_end(response).await;
    completer.await.unwrap();
    assert!(
        body.contains("building..."),
        "the buffered line must survive the close: {body}"
    );
}

#[tokio::test]
async fn live_log_stream_for_already_completed_job_serves_snapshot_and_ends() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let build_uuid = jobs[0].2.clone();

    crate::live_logs::record_live_log_wrapper(
        &state.shared(),
        &build_uuid,
        preloop_gha_protocol::LiveLogFeedLinesWrapper {
            step_id: "s1".into(),
            start_line: 1,
            count: 1,
            value: vec!["already done".into()],
        },
    )
    .await;
    // Complete BEFORE anyone connects — the late follower must still get the
    // retained snapshot and then a clean end, never a hang.
    complete_job(&state, run_id, "build", ExecutionStatus::Success).await;

    let response = open_live(&app, format!("/api/v1/runs/{run_id}/logs/live?job=build")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = read_sse_to_end(response).await;
    assert!(
        body.contains("already done"),
        "a late follower still gets the snapshot: {body}"
    );
}

#[tokio::test]
async fn live_log_stream_ends_on_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, _jobs) = two_job_run_for_log_filters(&app, &state).await;

    let response = open_live(&app, format!("/api/v1/runs/{run_id}/logs/live?job=build")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let completer = {
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            // Cancellation is a terminal status and must close the feed too.
            complete_job(&state, run_id, "build", ExecutionStatus::Cancelled).await;
        })
    };
    // Completes only because the cancel closed the stream.
    let _ = read_sse_to_end(response).await;
    completer.await.unwrap();
}

#[tokio::test]
async fn live_log_reopens_when_a_completed_job_streams_again() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let build_uuid = jobs[0].2.clone();

    complete_job(&state, run_id, "build", ExecutionStatus::Failure).await;
    // A retry reuses the agent job and streams fresh lines: the feed must
    // reopen so a follower attaching now subscribes instead of ending early.
    crate::live_logs::record_live_log_wrapper(
        &state.shared(),
        &build_uuid,
        preloop_gha_protocol::LiveLogFeedLinesWrapper {
            step_id: "retry".into(),
            start_line: 1,
            count: 1,
            value: vec!["second attempt".into()],
        },
    )
    .await;

    {
        let inner = state.inner.lock().await;
        assert!(
            !inner.live_log_closed.contains(&build_uuid),
            "fresh ingest must clear the closed mark"
        );
    }
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
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
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
async fn native_runner_list_reports_registered_runners() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // No runners yet: the CLI must be able to distinguish this from a
    // healthy pool before it prints `still waiting` forever.
    let empty = request_json(&app, Method::GET, "/api/v1/runners", Value::Null).await;
    assert_eq!(empty["count"].as_u64(), Some(0));
    assert_eq!(empty["runners"].as_array().map(Vec::len), Some(0));

    let runner = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "local",
            "labels": ["self-hosted", "linux", "x64"]
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    let listed = request_json(&app, Method::GET, "/api/v1/runners", Value::Null).await;
    assert_eq!(listed["count"].as_u64(), Some(1));
    let runners = listed["runners"].as_array().unwrap();
    assert_eq!(runners.len(), 1);
    assert_eq!(runners[0]["id"].as_i64(), Some(runner_id));
    assert_eq!(runners[0]["name"].as_str(), Some("local"));
    let labels: Vec<&str> = runners[0]["labels"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|label| label.as_str())
        .collect();
    assert_eq!(labels, vec!["self-hosted", "linux", "x64"]);

    // The endpoint is native-bearer gated.
    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/runners")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn native_runner_list_run_scoped_claimable() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // A Linux pool runner is registered, so the raw runner count is positive.
    // The CLI's dead-pool warning must key off runners that can actually
    // claim the queued work, not the count.
    request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({"name": "pool-linux", "labels": ["self-hosted", "linux"]}),
    )
    .await;

    // Claimable: a `runs-on: linux` job matches the registered runner.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: linux\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let listed = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runners?run_id={run_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(listed["count"].as_u64(), Some(1));
    assert_eq!(listed["queued"].as_u64(), Some(1));
    assert_eq!(listed["claimable"].as_u64(), Some(1));

    // Unclaimable: a job whose `runs-on` labels no registered runner carries
    // (here a custom label) stays queued — the scheduler only fails jobs with
    // no OS runner — so the raw count alone would hide the dead end.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: custom-runs-on\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let listed = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runners?run_id={run_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(listed["count"].as_u64(), Some(1));
    assert_eq!(listed["queued"].as_u64(), Some(1));
    assert_eq!(listed["claimable"].as_u64(), Some(0));

    // Unclaimable: a job requiring a specific runner group when the registered
    // runner belongs to a different group (here, default group) is unclaimable.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on:\n      group: specialized-group\n      labels: [linux]\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let listed = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runners?run_id={run_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(listed["count"].as_u64(), Some(1));
    assert_eq!(listed["queued"].as_u64(), Some(1));
    assert_eq!(listed["claimable"].as_u64(), Some(0));
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

/// The official `actions/runner` sends a stock label set that includes
/// `self-hosted` as both a system label and a user label (the default
/// `config.sh` prompt suggests it). The strict `(runner_id, label)` primary
/// key on `runner_labels` must not reject this; the runner server collapses
/// the duplicate at handler entry. The store layer dedupes again as a
/// backstop, so the round-trip through the database preserves the collapse.

/// The official `actions/runner` sends a stock label set that includes
/// `self-hosted` as both a system label and a user label (the default
/// `config.sh` prompt suggests it). The strict `(runner_id, label)` primary
/// key on `runner_labels` must not reject this; the runner server collapses
/// the duplicate at handler entry. The store layer dedupes again as a
/// backstop, so the round-trip through the database preserves the collapse.
#[tokio::test]
async fn register_runner_dedupes_official_label_set() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Generate a real RSA keypair so the registration request's publicKey
    // passes base64 + exponent/modulus validation. The label logic is what
    // we're exercising here, not the cryptography.
    let runner_keypair = AgentRsaKeypair::generate().unwrap();
    let public_xml = runner_keypair.public_key_xml();
    let modulus = public_xml
        .split("<Modulus>")
        .nth(1)
        .unwrap()
        .split("</Modulus>")
        .next()
        .unwrap()
        .to_owned();
    let exponent = public_xml
        .split("<Exponent>")
        .nth(1)
        .unwrap()
        .split("</Exponent>")
        .next()
        .unwrap()
        .to_owned();

    // Mirrors the label set captured in
    // .runner-watch/golden/v2.336.0/01-register-and-idle/flows.jsonl:
    // self-hosted appears as both system and user; Linux and linux coexist
    // (case-different today; collapsed under the same dedup rules).
    let body = json!({
        "name": "official-shape-runner",
        "labels": [
            {"name": "self-hosted", "type": "system"},
            {"name": "Linux",       "type": "system"},
            {"name": "ARM64",       "type": "system"},
            {"name": "self-hosted", "type": "user"},
            {"name": "mitm",        "type": "user"},
            {"name": "linux",       "type": "user"},
            {"name": "x64",         "type": "user"},
        ],
        "authorization": {
            "publicKey": {
                "exponent": exponent,
                "modulus": modulus,
            }
        }
    });

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/agents",
        body,
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();

    // In-memory labels must be deduped case-insensitively while preserving
    // the first occurrence of each canonical form.
    let inner = state.inner.lock().await;
    let stored = &inner.runners.get(&runner_id).unwrap().labels;
    let lowered: std::collections::BTreeSet<String> =
        stored.iter().map(|l| l.to_lowercase()).collect();
    assert_eq!(
        lowered.len(),
        stored.len(),
        "duplicates leaked into memory: {stored:?}"
    );
    assert!(lowered.contains("self-hosted"));
    assert!(lowered.contains("linux"));
    assert!(lowered.contains("arm64"));
    assert!(lowered.contains("mitm"));
    assert!(lowered.contains("x64"));
    // Case-folding must keep the first casing seen (the system one).
    assert!(stored.iter().any(|l| l == "self-hosted"));
    assert!(stored.iter().any(|l| l == "Linux"));

    // And the second registration (e.g. session creation) must still succeed —
    // if the dedup had only happened at handler entry and the database kept
    // duplicates, a second store_inner would 500 again.
    drop(inner);
    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({"ownerName": "official-shape-runner", "agent": {"id": runner_id}}),
    )
    .await;
    assert!(session.get("sessionId").is_some());
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

    let registration = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
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

/// The registration mint hands out a RunnerManage JWT. Strict mode requires
/// the system credential on both TCP and the mounted control socket; the
/// conformance golden replays a real GitHub registration token that this
/// control plane cannot verify, so those runs opt into `Permissive` explicitly.

/// The registration mint hands out a RunnerManage JWT. Strict mode requires
/// the system credential on both TCP and the mounted control socket; the
/// conformance golden replays a real GitHub registration token that this
/// control plane cannot verify, so those runs opt into `Permissive` explicitly.
#[tokio::test]
async fn registration_mint_credential_rules_are_strict_by_default() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let body = json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"});

    // No credential: refused on every surface.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // Strict-by-default: a made-up credential must NOT mint. This is the
    // registration hole: anyone able to reach the port could previously mint
    // a RunnerManage JWT and register a rogue runner that receives job
    // messages carrying a minted installation token plus job secrets.
    let forged = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(header::AUTHORIZATION, "RemoteAuth totally-made-up-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        forged.status(),
        StatusCode::UNAUTHORIZED,
        "strict policy must refuse an unrecognized credential"
    );

    // The system credential is the one thing strict accepts.
    let minted = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        body,
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(minted["token_schema"], "OAuthAccessToken");
}

#[tokio::test]
async fn registration_mint_permissive_only_under_an_explicit_env_opt_in() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.registration_policy = RegistrationPolicy::Permissive;
    let app = app(state, CancellationToken::new());
    let minted = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        "any-non-empty-credential",
    )
    .await;
    assert_eq!(
        minted["token_schema"], "OAuthAccessToken",
        "permissive policy is the conformance-harness opt-in"
    );
    // The conformance escape remains fail-closed on the mounted socket:
    // workflow code must never mint another runner identity from inside a VM.
    let socket_app = app
        .clone()
        .layer(middleware::from_fn(crate::auth::runner_surface_only));
    assert_eq!(
        request_status_with_bearer(
            &socket_app,
            Method::POST,
            "/api/v3/actions/runner-registration",
            json!({"url": "http://socket-workflow"}),
            "any-non-empty-credential",
        )
        .await,
        StatusCode::UNAUTHORIZED,
        "permissive registration must remain strict on the guest socket"
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

    let registration_auth = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/preloopdev/preloop", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
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
            // A Linux runner for a `runs-on: ubuntu-latest` job: the scheduler
            // will not hand a hosted-image label to a runner of another OS.
            "osDescription": "Linux local",
            "labels": [
                {"name": "self-hosted", "type": "system"},
                {"name": "Linux", "type": "system"},
                {"name": "X64", "type": "system"}
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
            "sub": format!("preloop-runner-listen-{runner_id}"),
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
                "repository": "preloopdev/preloop",
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
    // A Busy runner must not receive the same request again or claim a
    // successor while its worker is still draining.
    let busy_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&status=Busy&waitSeconds=0"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(busy_response.status(), StatusCode::ACCEPTED);
    let busy_body: Value = serde_json::from_slice(
        &to_bytes(busy_response.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(busy_body, Value::Null);

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

    let runtime_job_id: uuid::Uuid = runner_request_id.parse().unwrap();
    let runtime_token = state.mint_runtime_token(
        acquired["plan"]["planId"].as_str().unwrap(),
        &runtime_job_id,
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/broker/{runner_id}/completejob"))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
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

/// GitHub's dispatcher injects the job token through the lower-case
/// `github_token` variable. The runner exposes that built-in value to
/// `${{ secrets.GITHUB_TOKEN }}`; the wire must not add a second, non-official
/// uppercase variable.

/// GitHub's dispatcher injects the job token through the lower-case
/// `github_token` variable. The runner exposes that built-in value to
/// `${{ secrets.GITHUB_TOKEN }}`; the wire must not add a second, non-official
/// uppercase variable.
#[tokio::test]
async fn job_message_carries_the_official_github_token_variable() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_runner_id, runner_token) =
        register_runner_with_token(&app, "runner-1", &["self-hosted"], None).await;

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  rust:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "preloopdev/preloop",
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

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
                ))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let message: Value = serde_json::from_slice(&bytes).unwrap();
    let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;
    assert_eq!(
        acquired["messageType"],
        azdo::message_type::RUNNER_JOB_REQUEST
    );

    let token_secret = &acquired["variables"]["github_token"];
    assert_eq!(
        token_secret["isSecret"], true,
        "github_token must be marked secret so the runner masks it: {acquired}"
    );
    assert_eq!(
        token_secret["value"], acquired["variables"]["system.github.token"]["value"],
        "github_token must be the job token the engine minted"
    );
    assert!(
        acquired["variables"].get("GITHUB_TOKEN").is_none(),
        "uppercase GITHUB_TOKEN is not part of the official acquire schema"
    );
}

/// GitHub's environment-secret tier: a job whose `environment:` resolves to
/// a stored environment sees that tier, with environment > repo > global
/// precedence per name — and only that job does. Submission-provided values
/// still win per name over every stored tier.

/// GitHub's environment-secret tier: a job whose `environment:` resolves to
/// a stored environment sees that tier, with environment > repo > global
/// precedence per name — and only that job does. Submission-provided values
/// still win per name over every stored tier.
#[tokio::test]
async fn environment_secrets_override_repo_and_global_per_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (_runner_id, runner_token) =
        register_runner_with_token(&app, "runner-1", &["self-hosted"], None).await;

    // Seed the three stored tiers with the same name so precedence is
    // observable, plus a name that exists only in the environment tier.
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "global-shared" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "repo-shared", "repo": "owner/repo" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/SHARED",
        json!({ "value": "env-shared", "repo": "owner/repo", "env": "prod" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/ENV_ONLY",
        json!({ "value": "env-only", "repo": "owner/repo", "env": "prod" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/REPO_GLOBAL",
        json!({ "value": "global-v" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/REPO_GLOBAL",
        json!({ "value": "repo-v", "repo": "owner/repo" }),
    )
    .await;
    // A name present in every tier and NOT overridden by the submission: the
    // environment tier must win for the prod job, the repo tier for plain.
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-global" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-repo", "repo": "owner/repo" }),
    )
    .await;
    request_json(
        &app,
        Method::PUT,
        "/api/v1/secrets/TIERED",
        json!({ "value": "tier-env", "repo": "owner/repo", "env": "prod" }),
    )
    .await;

    // Two jobs: one in environment prod, one with no environment. The caller
    // also supplies SHARED, which must beat the environment tier.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  prod:\n    runs-on: ubuntu-latest\n    environment: prod\n    steps:\n      - run: echo hi\n  plain:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "payload": {"ref": "refs/heads/main", "commits": []},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "secrets": {"SHARED": "sub-shared"},
            "vars": {},
            "reusable_workflows": {}
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 2);
    let run_id = accepted["run_id"].as_str().unwrap();

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

    let mut variables_by_job = BTreeMap::new();
    for _ in 0..2 {
        let message = poll_message(&app, "preloop-system-token", session_id).await;
        let message_id = message["messageId"]
            .as_i64()
            .expect("polled message has an id");
        let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
        let runner_request_id = body["runner_request_id"].as_str().unwrap();
        let acquired = request_json_with_bearer(
            &app,
            Method::POST,
            "/broker/1/acquirejob",
            json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
            &runner_token,
        )
        .await;
        assert_eq!(
            acquired["messageType"],
            azdo::message_type::RUNNER_JOB_REQUEST
        );
        // Ack the delivered message so the next poll does not redeliver it.
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/distributedtask/pools/1/messages/{message_id}?sessionId={session_id}"
                    ))
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let job_name = acquired["jobName"].as_str().unwrap().to_owned();
        variables_by_job.insert(job_name.clone(), acquired["variables"].clone());
        // One session holds one active job until it completes; free the slot
        // so the next poll delivers the other job's message.
        request_json(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({"run_id": run_id, "job_id": job_name, "status": "success"}),
        )
        .await;
    }

    let prod = &variables_by_job["prod"];
    assert_eq!(
        prod["SHARED"]["value"], "sub-shared",
        "submission-provided secrets win per name over every stored tier"
    );
    assert_eq!(prod["SHARED"]["isSecret"], true);
    assert_eq!(
        prod["ENV_ONLY"]["value"], "env-only",
        "environment secrets reach jobs in that environment"
    );
    assert_eq!(prod["ENV_ONLY"]["isSecret"], true);
    assert_eq!(
        prod["TIERED"]["value"], "tier-env",
        "environment tier beats repo and global tiers for jobs in the environment"
    );
    assert_eq!(prod["TIERED"]["isSecret"], true);

    let plain = &variables_by_job["plain"];
    assert_eq!(
        plain["SHARED"]["value"], "sub-shared",
        "submission-provided secrets win per name even without an environment"
    );
    assert_eq!(
        plain["REPO_GLOBAL"]["value"], "repo-v",
        "without an environment the repo tier wins over the global tier"
    );
    assert_eq!(
        plain["TIERED"]["value"], "tier-repo",
        "without an environment the repo tier wins over the global tier"
    );
    assert!(
        plain.get("ENV_ONLY").is_none(),
        "environment secrets never reach jobs outside the environment"
    );
    assert_eq!(
        prod["REPO_GLOBAL"]["value"], "repo-v",
        "repo tier still applies to jobs in an environment that has no env-tier override"
    );
}

#[tokio::test]
async fn current_service_broker_flow_uses_queued_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner_token = state
        .local_jwt(json!({
            "sub": "preloop-runner-listen-1",
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    let _runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-1", "version": "2.335.1"}),
    )
    .await;

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
            "repository": "preloopdev/preloop",
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
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
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
    assert_eq!(
        acquired["variables"]["actions_runner_allow_artifacts_file"]["value"],
        "false"
    );
    assert_eq!(
        acquired["variables"]["actions_self_repository"]["value"],
        "true"
    );
    assert!(acquired.get("runnerSettings").is_none());
    assert_eq!(
        acquired["resources"]["endpoints"][0]["url"],
        "http://127.0.0.1:9090/broker/1/"
    );
    assert!(acquired["resources"]["endpoints"][0]["data"]["ConnectivityAndDNSChecks"].is_string());
    assert!(acquired["plan"]["planId"].is_string());
    assert!(acquired["jobId"].is_string());
    assert!(acquired["steps"].is_array());

    let runtime_job_id: uuid::Uuid = runner_request_id.parse().unwrap();
    let runtime_token = state.mint_runtime_token(
        acquired["plan"]["planId"].as_str().unwrap(),
        &runtime_job_id,
    );

    let renewed = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/renewjob",
        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
        &runtime_token,
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
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
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
    let duplicate_completion = status_with_bearer(
        &app,
        &runtime_token,
        Method::POST,
        "/broker/1/completejob",
        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
    )
    .await;
    assert_eq!(
        duplicate_completion,
        StatusCode::NO_CONTENT,
        "broker completion retries are idempotent"
    );
    let renew_after_completion = status_with_bearer(
        &app,
        &runtime_token,
        Method::POST,
        "/broker/1/renewjob",
        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
    )
    .await;
    assert_eq!(
        renew_after_completion,
        StatusCode::CONFLICT,
        "completed broker requests cannot be renewed"
    );
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
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.status(), StatusCode::OK);
}

#[tokio::test]
async fn broker_job_refs_use_session_runner_id_for_pool_and_root_polls() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Registration IDs are allocated monotonically. Register a predecessor so
    // this test exercises the replacement runner path instead of the first
    // runner's special-looking ID 1.
    let _ = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-before-replacement", "version": "2.335.1"}),
    )
    .await;

    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({
            "name": "runner-replacement",
            "version": "2.335.1",
            "labels": [
                {"name": "self-hosted", "type": "system"},
                {"name": "ubuntu-latest", "type": "system"}
            ]
        }),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();
    assert!(runner_id > 1, "replacement runner must have an ID above 1");

    let runner_token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    // The pool-path session is explicitly tied to runner 2 by its agent body.
    // The root broker session is tied to the same runner by its listen token.
    let pool_session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_id, "name": "runner-replacement"},
            "ownerName": "replacement pool session",
            "useFipsEncryption": false
        }),
        &runner_token,
    )
    .await;
    let pool_session_id = pool_session["sessionId"].as_str().unwrap();

    let root_session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let root_session_id = root_session["sessionId"].as_str().unwrap();

    let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo broker-id\n";
    for _ in 0..2 {
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/preloop",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
        assert_eq!(accepted["queued_jobs"], 1);
    }

    let pool_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={pool_session_id}&status=Online&runnerVersion=2.335.1&waitSeconds=0"
        ),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(pool_ref["messageType"], "RunnerJobRequest");
    let pool_body: Value = serde_json::from_str(pool_ref["body"].as_str().unwrap()).unwrap();
    let expected_run_service_url = format!("{}/broker/{runner_id}/", public_base_url());
    assert_eq!(pool_body["run_service_url"], expected_run_service_url);
    assert!(!pool_body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    let pool_request_id = pool_body["runner_request_id"].as_str().unwrap();

    let root_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!(
            "/runner/server/message?sessionId={root_session_id}&status=Online&runnerVersion=2.335.1&waitSeconds=0"
        ),
        Value::Null,
        &runner_token,
    )
    .await;
    assert_eq!(root_ref["messageType"], "RunnerJobRequest");
    let root_body: Value = serde_json::from_str(root_ref["body"].as_str().unwrap()).unwrap();
    assert_eq!(root_body["run_service_url"], expected_run_service_url);
    assert!(!root_body["run_service_url"]
        .as_str()
        .unwrap()
        .contains("/broker/1/"));
    let root_request_id = root_body["runner_request_id"].as_str().unwrap();

    // The runner-2 token must also authorize acquisition on the URL advertised
    // by both message paths; a hard-coded /broker/1 URL would fail this flow.
    for request_id in [pool_request_id, root_request_id] {
        let acquired = request_json_with_bearer(
            &app,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            json!({
                "jobMessageId": request_id,
                "billingOwnerId": "local",
                "runnerOS": "Linux"
            }),
            &runner_token,
        )
        .await;
        assert_eq!(
            acquired["resources"]["endpoints"][0]["url"],
            expected_run_service_url
        );
        let runtime_job_id: uuid::Uuid = request_id.parse().unwrap();
        let runtime_token = state.mint_runtime_token(
            acquired["plan"]["planId"].as_str().unwrap(),
            &runtime_job_id,
        );
        let _ = request_json_with_bearer(
            &app,
            Method::POST,
            &format!("/broker/{runner_id}/completejob"),
            json!({
                "jobId": request_id,
                "planId": acquired["plan"]["planId"]
            }),
            &runtime_token,
        )
        .await;
    }
}

#[tokio::test]
async fn action_download_info_returns_batch_download_collection() {
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "mock-bearer-token");

    // Hermetic ref→SHA resolution: a mock GitHub API answers `commits/{ref}`
    // with a fixed SHA so the batch handler never touches real GitHub.
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({"sha": "abc123def456abc123def456abc123def456abc1"}))
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // Official runner batch shape: `ActionReferenceList` of
    // `{nameWithOwner, ref, path}`. Local (`./`) and docker refs are dropped.
    let response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/ActionDownloadInfo/scope/actions/plan",
        json!({
            "actions": [
                {"nameWithOwner": "actions/checkout", "ref": "v4", "path": ""},
                {"nameWithOwner": "owner/repo", "ref": "main", "path": "sub/dir"},
                {"nameWithOwner": "actions/setup-node.js", "ref": "v4", "path": ""},
                {"nameWithOwner": "./.github/actions/local", "ref": ""},
                {"nameWithOwner": "docker://alpine:3.20", "ref": ""}
            ]
        }),
    )
    .await;

    // Reply is an `ActionDownloadInfoCollection`: `actions` keyed by
    // `nameWithOwner@ref`, each entry an `ActionDownloadInfo`.
    let actions = response["actions"].as_object().unwrap();

    let checkout = &actions["actions/checkout@v4"];
    assert_eq!(checkout["nameWithOwner"], "actions/checkout");
    assert_eq!(checkout["ref"], "v4");
    // The ref is pinned to the SHA the mock API resolves.
    assert_eq!(
        checkout["resolvedSha"],
        "abc123def456abc123def456abc123def456abc1"
    );
    // The runner reads `tarballUrl`; it carries a signed, expiring ticket
    // pinned to the resolved SHA (the bearerless route treats the URL as the
    // capability).
    let tarball = checkout["tarballUrl"].as_str().unwrap();
    let (base, query) = tarball.split_once('?').expect("ticket query");
    assert_eq!(
        base,
        "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/abc123def456abc123def456abc123def456abc1"
    );
    assert!(query.contains("exp=") && query.contains("sig="), "{query}");
    // Preloop's own download capability route is HMAC signed and bearerless; operator PAT is never leaked.
    assert!(checkout["authentication"].is_null());

    // Repositories with dots in their names (e.g. actions/setup-node.js) resolve cleanly.
    let node = &actions["actions/setup-node.js@v4"];
    assert_eq!(node["nameWithOwner"], "actions/setup-node.js");
    assert!(node["tarballUrl"]
        .as_str()
        .unwrap()
        .contains("actions/setup-node.js/abc123def456abc123def456abc123def456abc1"));

    // Subpath actions key on `nameWithOwner@ref` (path excluded, matching the
    // runner's `GetDownloadInfoLookupKey`).
    let sub = &actions["owner/repo@main"];
    assert_eq!(sub["nameWithOwner"], "owner/repo");
    assert!(
        sub["tarballUrl"].as_str().unwrap().starts_with(
            "http://127.0.0.1:9090/api/v1/actions/download/owner/repo/abc123def456abc123def456abc123def456abc1?"
        ),
        "{}",
        sub["tarballUrl"]
    );

    // Local and docker refs are never resolvable to a download.
    assert!(!actions.contains_key("./.github/actions/local@"));
    assert!(!actions.contains_key("docker://alpine:3.20@"));
    assert_eq!(actions.len(), 3);
}

#[tokio::test]
async fn action_download_info_returns_null_auth_when_token_unset() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");

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
                {"nameWithOwner": "actions/checkout", "ref": "v4", "path": ""}
            ]
        }),
    )
    .await;

    let actions = response["actions"].as_object().unwrap();
    let checkout = &actions["actions/checkout@v4"];
    assert!(checkout["authentication"].is_null());
}

#[tokio::test]
async fn action_download_info_discards_malformed_or_abbreviated_sha() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    // Mock API returns an abbreviated 7-char SHA rather than full 40-char SHA
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(|| async { axum::Json(serde_json::json!({"sha": "abc1234"})) }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

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
                {"nameWithOwner": "actions/checkout", "ref": "v4", "path": ""}
            ]
        }),
    )
    .await;

    let actions = response["actions"].as_object().unwrap();
    let checkout = &actions["actions/checkout@v4"];
    // Short SHA was discarded; resolvedSha is null and tarball URL falls back to ref
    assert!(checkout["resolvedSha"].is_null());
    assert!(checkout["tarballUrl"]
        .as_str()
        .unwrap()
        .contains("actions/checkout/v4?"));
}

#[tokio::test]
async fn action_download_info_rejects_all_zero_sha() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    // The all-zero sentinel is a valid SHA shape but names no commit. The
    // resolver must not short-circuit it as "already resolved"; it goes
    // through lookup like any other ref. The mock answers 404 for it (as
    // GitHub would), so resolvedSha stays null instead of echoing the zero SHA.
    let zero = "0000000000000000000000000000000000000000".to_owned();
    let zero_for_mock = zero.clone();
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(
            move |axum::extract::Path(git_ref): axum::extract::Path<String>| async move {
                if git_ref == zero_for_mock {
                    return axum::response::IntoResponse::into_response(
                        axum::http::StatusCode::NOT_FOUND,
                    );
                }
                axum::response::IntoResponse::into_response(axum::Json(serde_json::json!({
                    "sha": "abc123def456abc123def456abc123def456abc1"
                })))
            },
        ),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

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
                {"nameWithOwner": "actions/checkout", "ref": zero, "path": ""}
            ]
        }),
    )
    .await;

    let actions = response["actions"].as_object().unwrap();
    let checkout = &actions[&format!("actions/checkout@{zero}")];
    // Zero SHA was not accepted as a pinned commit; resolution failed closed.
    assert!(checkout["resolvedSha"].is_null());
    assert_ne!(
        checkout["resolvedSha"].as_str().unwrap_or_default(),
        zero,
        "all-zero sentinel must never be emitted as a resolved commit SHA"
    );
}

#[tokio::test]
async fn remote_workflow_content_is_fetched_at_resolved_sha() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    // The mutable ref `main` resolves to a fixed SHA. The contents request
    // must carry `?ref=<sha>` — not `?ref=main` — so a branch retargeted
    // between the two requests cannot mix content from commit A with the
    // recorded identity of commit B.
    let sha = "abc123def456abc123def456abc123def456abc1".to_owned();
    let seen_ref: std::sync::Arc<std::sync::Mutex<Option<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(None));
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new()
        .route(
            "/repos/:owner/:repo/commits/:git_ref",
            axum::routing::get({
                let sha = sha.clone();
                move || async move { axum::Json(serde_json::json!({"sha": sha})) }
            }),
        )
        .route(
            "/repos/:owner/:repo/contents/*path",
            axum::routing::get({
                let seen_ref = seen_ref.clone();
                move |axum::extract::Query(
                    params,
                ): axum::extract::Query<std::collections::HashMap<String, String>>| async move {
                    *seen_ref.lock().unwrap() = params.get("ref").cloned();
                    // base64("on: push\njobs:\n  callee:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"echo hi\"\n")
                    axum::Json(serde_json::json!({
                        "content": "b246IHB1c2gKam9iczoKICBjYWxsZWU6CiAgICBydW5zLW9uOiB1YnVudHUtbGF0ZXN0CiAgICBzdGVwczoKICAgICAgLSBydW46ICJlY2hvIGhpIgo=",
                        "encoding": "base64",
                    }))
                }
            }),
        );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let root_yaml = "on: push\njobs:\n  caller:\n    uses: someowner/somerepo/.github/workflows/callee.yml@main\n";
    let workflow = preloop_gha_parser::parse_workflow(root_yaml).unwrap();
    let reference = "someowner/somerepo/.github/workflows/callee.yml@main";
    let mut submission = preloop_gha_protocol::WorkflowSubmission {
        workflow_yaml: root_yaml.to_owned(),
        event: "push".to_owned(),
        repository: "owner/repo".to_owned(),
        ..Default::default()
    };
    crate::remote_workflows::resolve_remote_workflows(&mut submission, &workflow, None)
        .await
        .unwrap();

    // Content and identity are bound to the same validated revision.
    assert_eq!(
        seen_ref.lock().unwrap().as_deref(),
        Some(sha.as_str()),
        "contents request must pin ?ref= to the resolved commit SHA"
    );
    assert_eq!(
        submission
            .reusable_workflow_shas
            .get(reference)
            .map(String::as_str),
        Some(sha.as_str())
    );
    assert!(
        submission.reusable_workflows[reference].contains("runs-on: ubuntu-latest"),
        "fetched workflow content must be stored under the original reference"
    );
}

#[tokio::test]
async fn resolve_ref_to_sha_omits_pat_over_http() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "secret-pat");

    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(|headers: axum::http::HeaderMap| async move {
            assert!(
                !headers.contains_key(axum::http::header::AUTHORIZATION),
                "PAT must never be transmitted over plain unencrypted HTTP"
            );
            axum::Json(serde_json::json!({"sha": "abc123def456abc123def456abc123def456abc1"}))
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    let response = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/ActionDownloadInfo/scope/actions/plan",
        json!({ "actions": [{"nameWithOwner": "actions/checkout", "ref": "v4"}] }),
    )
    .await;
    assert_eq!(
        response["actions"]["actions/checkout@v4"]["resolvedSha"],
        "abc123def456abc123def456abc123def456abc1"
    );
}

#[tokio::test]
async fn runnerresolve_actions_returns_runner_parseable_tar_urls() {
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    // Hermetic ref→SHA resolution: point PRELOOP_GITHUB_API_URL at a mock that
    // answers `commits/{ref}` with a fixed SHA, so the test never touches the
    // real GitHub API (and pins the new SHA-pinning behavior deterministically).
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/commits/:git_ref",
        axum::routing::get(|| async {
            axum::Json(serde_json::json!({"sha": "abc123def456abc123def456abc123def456abc1"}))
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

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

    // Signed, expiring ticket — the bearerless download route treats the URL
    // itself as the capability.
    assert!(
        response["actions"]["actions/checkout@v4"]["tar_url"]
            .as_str()
            .unwrap()
            .starts_with(
                "http://127.0.0.1:9090/api/v1/actions/download/actions/checkout/abc123def456abc123def456abc123def456abc1?exp="
            ),
        "{}",
        response["actions"]["actions/checkout@v4"]["tar_url"]
    );
    assert_eq!(
        response["actions"]["actions/checkout@v4"]["resolved_sha"],
        "abc123def456abc123def456abc123def456abc1"
    );
    assert!(
        response["actions"]["owner/repo/path@main"]["tar_url"]
            .as_str()
            .unwrap()
            .starts_with(
                "http://127.0.0.1:9090/api/v1/actions/download/owner/repo/abc123def456abc123def456abc123def456abc1?exp="
            ),
        "{}",
        response["actions"]["owner/repo/path@main"]["tar_url"]
    );
}

#[tokio::test]
async fn action_download_requires_a_ticket_for_the_action_it_serves() {
    // The route is bearerless and reachable from inside every runner VM, so
    // the URL is the capability. Without this, workflow code could make the
    // engine fetch any repository with the engine's own GitHub credential.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    for (owner, repo, git_ref) in [("acme", "public-action", "v1"), ("acme", "private", "v1")] {
        let dir = temp
            .path()
            .join("actions")
            .join(owner)
            .join(repo)
            .join(git_ref);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("action.tar.gz"), b"tar")
            .await
            .unwrap();
    }

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let good = state.sign_action_ticket("acme", "public-action", "v1", future);

    let get = |uri: String| {
        let app = app.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
        }
    };

    // No ticket at all.
    assert_eq!(
        get("/api/v1/actions/download/acme/public-action/v1".to_owned()).await,
        StatusCode::NOT_FOUND,
        "an unsigned request must not be served"
    );

    // A ticket minted for one action, replayed against another — the
    // exfiltration path: ask for a repo the workflow was never granted.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/private/v1?exp={future}&sig={good}"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "a ticket must not authorise a different action"
    );

    // Right action, forged signature.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={future}&sig=AAAA"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "a forged signature must not be served"
    );

    // Right signature, but expired.
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1;
    let stale = state.sign_action_ticket("acme", "public-action", "v1", past);
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={past}&sig={stale}"
        ))
        .await,
        StatusCode::NOT_FOUND,
        "an expired ticket must not be served"
    );

    // The ticket it was actually minted for.
    assert_eq!(
        get(format!(
            "/api/v1/actions/download/acme/public-action/v1?exp={future}&sig={good}"
        ))
        .await,
        StatusCode::OK,
        "the minted ticket must still work"
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

    // Cache entry for repo with dots in its name
    let dotted_dir = temp
        .path()
        .join("actions")
        .join("test.owner")
        .join("test.repo.js")
        .join("v1#tag");
    tokio::fs::create_dir_all(&dotted_dir).await.unwrap();
    tokio::fs::write(dotted_dir.join("action.tar.gz"), b"dotted-tar-content")
        .await
        .unwrap();

    // 1. Successful cache hit, with the signed ticket the server mints
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let signature = state.sign_action_ticket("test-owner", "test-repo", "v1", expires_at);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/v1/actions/download/test-owner/test-repo/v1?exp={expires_at}&sig={signature}"
                ))
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

    // 1b. Successful cache hit for dotted repo name and special character in tag
    let dotted_sig = state.sign_action_ticket("test.owner", "test.repo.js", "v1#tag", expires_at);
    let enc_ref = percent_encode_path_segment("v1#tag");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/v1/actions/download/test.owner/test.repo.js/{enc_ref}?exp={expires_at}&sig={dotted_sig}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"dotted-tar-content"
    );

    // 1c. Ref containing literal % (e.g. v1%tag)
    let pct_dir = temp
        .path()
        .join("actions")
        .join("test-owner")
        .join("test-repo")
        .join("v1%tag");
    tokio::fs::create_dir_all(&pct_dir).await.unwrap();
    tokio::fs::write(pct_dir.join("action.tar.gz"), b"pct-tar-content")
        .await
        .unwrap();
    let pct_sig = state.sign_action_ticket("test-owner", "test-repo", "v1%tag", expires_at);
    let enc_pct_ref = percent_encode_path_segment("v1%tag"); // "v1%25tag"
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/v1/actions/download/test-owner/test-repo/{enc_pct_ref}?exp={expires_at}&sig={pct_sig}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 1d. Cache miss with special ref (v1#beta) correctly percent-encodes outbound fetch
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/repos/:owner/:repo/tarball/:git_ref",
        axum::routing::get(
            |axum::extract::Path((_owner, _repo, git_ref)): axum::extract::Path<(
                String,
                String,
                String,
            )>| async move {
                assert_eq!(
                    git_ref, "v1#beta",
                    "outbound fetch must preserve exact tag without fragment stripping"
                );
                axum::response::Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("fetched-tar-content"))
                    .unwrap()
            },
        ),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let miss_temp = tempfile::tempdir().unwrap();
    let miss_state = AppState::new(miss_temp.path().to_path_buf()).await.unwrap();
    let miss_app = crate::routes::app(miss_state.clone(), CancellationToken::new());
    let miss_sig = miss_state.sign_action_ticket("test-owner", "test-repo", "v1#beta", expires_at);
    let enc_hash_ref = percent_encode_path_segment("v1#beta");
    let response = miss_app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/api/v1/actions/download/test-owner/test-repo/{enc_hash_ref}?exp={expires_at}&sig={miss_sig}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"fetched-tar-content"
    );

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
        .clone()
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

    // 2b. Reject absolute paths and leading slashes in ref
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/actions/download/test-owner/test-repo/%2Ftmp%2Fescape")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // action_download_ticket returns None for absolute refs
    assert!(crate::actions::action_download_ticket(
        &state,
        "test-owner/test-repo@/tmp/escape",
        None
    )
    .is_none());
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
                .header(header::AUTHORIZATION, "Bearer preloop-attacker-controlled")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runner_protocol_errors_use_official_envelopes_without_changing_native_api() {
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );

    // Auth middleware failures on _apis routes must be VSS/AzDO JSON, not the
    // native {"error": ...} response used by local APIs.
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
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/json; charset=utf-8"
    );
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["$type"], "Microsoft.VisualStudio.Services.Common.VssException, Microsoft.VisualStudio.Services.Common");
    assert_eq!(body["message"], "runner or job protocol token required");
    assert_eq!(body["typeKey"], "UnauthorizedRequestException");
    assert!(body["typeName"].as_str().is_some());

    // Router-level 404s on the runner-facing surface use the same envelope.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/_apis/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["typeKey"], "ResourceNotFoundException");
    assert_eq!(body["message"], "Not Found");

    // JSON extractor failures on a protected _apis route are 400 VSS errors.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/distributedtask/pools/1/sessions")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["typeKey"], "VssInvalidRequestException");
    assert!(body["message"].as_str().unwrap().contains("JSON"));

    // Twirp has its own canonical error envelope rather than the VSS object.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["code"], "invalid_argument");
    assert!(body["msg"].as_str().is_some());

    // Native callers keep the existing local API error contract.
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/runs/00000000-0000-0000-0000-000000000000")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body["error"], "run not found");
    assert!(body.get("typeName").is_none());
}

/// Issue #286: dispatch inputs must not erase a reusable caller's evaluated
/// `with:` values. The submit path used to overwrite the caller
/// placeholder's whole input map with the dispatch inputs, so the callee
/// saw an empty `mode` instead of the evaluated "plan".
#[tokio::test]
async fn dispatch_inputs_do_not_erase_reusable_caller_with_values() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let caller_yaml = "on:\n  workflow_dispatch:\n    inputs:\n      dry_run: { type: boolean, default: false }\njobs:\n  infra:\n    uses: ./.github/workflows/callee.yml\n    with:\n      mode: ${{ inputs.dry_run && 'plan' || 'apply' }}\n";
    let callee_yaml = "on:\n  workflow_call:\n    inputs:\n      mode: { type: string, required: true }\njobs:\n  callee:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo \"${{ inputs.mode }}\"\n";

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": caller_yaml,
            "event": "workflow_dispatch",
            "payload": {"inputs": {"dry_run": true}},
            "repository": "owner/repo",
            "git_ref": "refs/heads/main",
            "reusable_workflows": {".github/workflows/callee.yml": callee_yaml},
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).expect("the run must be recorded");
    let caller_plan = run
        .caller_plans
        .values()
        .find(|plan| plan.reusable_call.is_some())
        .expect("the caller placeholder must be recorded");
    // The expression was evaluated in the caller context at parse time...
    assert_eq!(
        caller_plan.inputs.get("mode").and_then(|v| v.as_str()),
        Some("plan"),
        "the callee input must keep its evaluated value, not be erased"
    );
    // ...and the dispatch inputs stay available to the caller's own gates.
    assert_eq!(
        caller_plan.inputs.get("dry_run"),
        Some(&json!(true)),
        "dispatch inputs must still reach the caller's own if:/name: context"
    );
}
