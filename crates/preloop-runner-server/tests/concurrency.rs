//! preloop-runner-server integration tests — concurrency group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

#[tokio::test]
async fn listen_tokens_are_revoked_when_the_runner_identity_is_purged() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Registration precedes token issuance in every real flow: register the
    // runner, then mint its listen token.
    let registered = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({
            "name": "machine-a",
            "version": "2.335.1",
            "labels": [{"name": "self-hosted", "type": "system"}]
        }),
    )
    .await;
    let runner_id = registered["id"].as_i64().unwrap();
    let token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();

    // Before purge the token is a live runner credential: it clears
    // require_runner_bearer and reaches the handler (400 = the handler asked
    // for a sessionId, i.e. it ran past the auth layer).
    let poll = |app: &Router| {
        let app = app.clone();
        let token = token.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/runner/message")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    assert_eq!(poll(&app).await.status(), StatusCode::BAD_REQUEST);

    // The verified identity also enforces the session binding: a body
    // claiming a *different* agent is refused while the token names a live
    // registered runner.
    let session_body = json!({
        "agent": {"id": runner_id + 100, "name": "somebody-else"},
        "ownerName": "owner",
        "preloopAzdo": true,
        "useFipsEncryption": false
    });
    let create_session = |app: &Router| {
        let app = app.clone();
        let token = token.clone();
        let session_body = session_body.clone();
        async move {
            app.oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/runner/server/_apis/distributedtask/pools/1/sessions")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(session_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
        }
    };
    let before_session = create_session(&app).await;
    assert_eq!(
        before_session.status(),
        StatusCode::FORBIDDEN,
        "verified listen token must not let the session body claim a different agent"
    );

    // Deregister the runner: purge removes the registration, which is the
    // revocation of every listen token that runner was issued.
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_id}"),
        Value::Null,
    )
    .await;
    {
        let inner = state.inner.lock().await;
        assert!(!inner.runners.contains_key(&runner_id));
    }

    // The same token is now refused at the auth layer.
    assert_eq!(poll(&app).await.status(), StatusCode::UNAUTHORIZED);

    // The protected session route also rejects the revoked token; a stale
    // bearer must not fall back to an unverified body-controlled identity.
    let after_session = create_session(&app).await;
    assert_eq!(
        after_session.status(),
        StatusCode::UNAUTHORIZED,
        "purged listen token cannot create a session"
    );
}

#[tokio::test]
async fn stale_assignment_is_taken_over_by_the_next_verified_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    // Backdate the pairing past the pre-claim window, simulating a machine
    // whose runner died between registration and its first poll.
    {
        let mut inner = state.inner.lock().await;
        for record in inner.job_assignments.values_mut() {
            record.at = std::time::SystemTime::now()
                - crate::runtime_scheduling::CLAIM_BINDING_TTL
                - std::time::Duration::from_secs(5);
        }
    }

    // The dead owner's overdue pairing must not serve it; a new verified
    // runner takes over (as a replacement machine's registration would).
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .job_assignments
                .values()
                .next()
                .and_then(|r| r.runner_id),
            Some(runner_b),
            "replacement machine takes over the stale pairing"
        );
    }
    let _ = token_a;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "takeover runner receives the rescued job: {delivered}"
    );
    // And the stale owner no longer does.
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, &token_a, session_a_id).await;
    assert!(stolen.is_null(), "stale owner lost the job: {stolen}");
}

#[tokio::test]
async fn purge_requeues_claimed_unfinished_job_to_another_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Machine A registers with a provision token and claims the job.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_a, &session_a_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "machine A claimed the job: {delivered}"
    );
    {
        let inner = state.inner.lock().await;
        assert!(inner.queue.is_empty());
        assert_eq!(inner.claimed_jobs.len(), 1, "claim is stashed");
    }

    // The pool tears machine A down mid-job: purge by machine name, then the
    // job must be back on the queue for somebody else.
    let purge = request_json(
        &app,
        Method::POST,
        "/api/v1/runners/purge",
        json!({ "name": "machine-a" }),
    )
    .await;
    assert_eq!(purge["purged"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.queue.len(), 1, "unfinished job requeued");
        assert!(inner.claimed_jobs.is_empty(), "stash consumed by requeue");
        assert!(!inner.runners.contains_key(&runner_a));
    }

    // A fresh machine registers and picks the job up.
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_b, &session_b_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "machine B receives the requeued job: {delivered}"
    );
}

#[tokio::test]
async fn startup_purge_removes_only_restored_ephemeral_runners() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let ephemeral = request_json(
        &app,
        Method::POST,
        "/api/v1/runners",
        json!({
            "name": "pool-machine",
            "labels": ["self-hosted"],
            "ephemeral": true
        }),
    )
    .await;
    let ephemeral_id = ephemeral["id"].as_i64().unwrap();
    let (external_id, _) =
        register_runner_with_token(&app, "external-machine", &["self-hosted"], None).await;
    {
        let inner = state.inner.lock().await;
        assert!(inner.runners[&ephemeral_id].ephemeral);
        assert!(!inner.runners[&external_id].ephemeral);
    }
    drop(app);
    drop(state);

    let restored_state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let inner = restored_state.inner.lock().await;
        assert!(inner.runners[&ephemeral_id].ephemeral);
        assert!(inner.runners.contains_key(&external_id));
    }
    let restored_shared = Arc::new(SharedState {
        state: restored_state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::runner_lifecycle::purge_restored_ephemeral_runners(&restored_shared).await;
    drop(restored_shared);
    drop(restored_state);

    let final_state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = final_state.inner.lock().await;
    assert!(!inner.runners.contains_key(&ephemeral_id));
    assert!(inner.runners.contains_key(&external_id));
}

#[tokio::test]
async fn purge_of_finished_runner_does_not_requeue() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap().to_owned();
    let delivered = poll_message(&app, &token_a, &session_a_id).await;
    assert!(delivered["messageType"].as_str().is_some());

    // Complete the job through the broker compat completion handler, then purge.
    let (run_id, job_id) = {
        let inner = state.inner.lock().await;
        inner.claimed_jobs.keys().next().unwrap().clone()
    };
    let _ = request_json(
        &app,
        Method::PATCH,
        &format!("/runner/server/_apis/distributedtask/hubs/actions/plans/{run_id}/jobs/{job_id}"),
        json!({ "runId": run_id, "jobId": job_id, "result": "succeeded" }),
    )
    .await;

    request_json(
        &app,
        Method::POST,
        "/api/v1/runners/purge",
        json!({ "name": "machine-a" }),
    )
    .await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.queue.is_empty(),
            "finished job must not come back: {:?}",
            inner.queue
        );
    }
}

#[tokio::test]
async fn workflow_gate_released_when_run_ends_via_dependency_skip() {
    // MC-S2: a workflow-level Holder::Run must be released when the run
    // concludes through the dependency-skip arm of promote_ready_jobs
    // (previously the slot leaked forever and same-group successors without
    // cancel-in-progress parked permanently).
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    let yaml = r#"
on: push
concurrency:
  group: skip-group
jobs:
  dep:
    runs-on: ubuntu-latest
    steps:
      - run: echo dep
  main:
    runs-on: ubuntu-latest
    needs: [dep]
    if: false
    steps:
      - run: echo main
"#;
    let a = submit_yaml(&app, yaml, "owner/repo").await;
    let a_id = a["run_id"].as_str().unwrap();

    // Dispatch and complete `dep`; `main` then evaluates `if: false` and is
    // skipped, concluding run A through the skip arm.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A `dep` should be dispatchable");
    complete_via_api(&app, a_id, "dep").await;

    let run_a = get_run_json(&app, a_id).await;
    assert_eq!(run_a["jobs"]["main"], "skipped", "main must be skipped");
    assert!(
        run_a["status"].as_str().unwrap() == "success"
            || run_a["status"].as_str().unwrap() == "completed",
        "run A must be terminal after the skip, got {}",
        run_a["status"]
    );

    // A successor in the same group must now acquire the slot instead of
    // parking behind the leaked holder.
    let b = submit_yaml(&app, yaml, "owner/repo").await;
    let b_id = b["run_id"].as_str().unwrap();
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["status"], "queued",
        "run B must acquire the freed workflow gate (MC-S2), got {}",
        run_b["status"]
    );
    assert_eq!(run_b["jobs"]["dep"], "queued");
}

#[tokio::test]
async fn needs_gated_job_concurrency_acquired_at_promote_time() {
    // MC-S3: job-level concurrency must gate needs-gated jobs at promote
    // time. Previously the gate was evaluated only at submit for needs-empty
    // jobs, so a needs-gated job with a busy group was dispatched anyway.
    let temp = tempfile::tempdir().unwrap();
    let app = app(
        AppState::new(temp.path().to_path_buf()).await.unwrap(),
        CancellationToken::new(),
    );
    // Run A: `one` holds job group g (needs-empty → gated at submit).
    let a = submit_yaml(
        &app,
        r#"
on: push
jobs:
  one:
    runs-on: ubuntu-latest
    concurrency:
      group: shared-gate
    steps:
      - run: echo one
"#,
        "owner/repo",
    )
    .await;
    let a_id = a["run_id"].as_str().unwrap();
    // Claim `one` so it is InProgress and keeps holding the group.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run A `one` should be dispatchable");

    // Run B: `dep` (no gate) + `two` (needs [dep], same group g).
    let b = submit_yaml(
        &app,
        r#"
on: push
jobs:
  dep:
    runs-on: ubuntu-latest
    steps:
      - run: echo dep
  two:
    runs-on: ubuntu-latest
    needs: [dep]
    concurrency:
      group: shared-gate
    steps:
      - run: echo two
"#,
        "owner/repo",
    )
    .await;
    let b_id = b["run_id"].as_str().unwrap();

    // Dispatch and complete `dep`; `two` becomes ready and must evaluate its
    // gate — the group is busy, so it parks instead of dispatching.
    let msg = request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
        Value::Null,
    )
    .await;
    assert!(!msg.is_null(), "run B `dep` should be dispatchable");
    complete_via_api(&app, b_id, "dep").await;

    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["jobs"]["two"], "pending",
        "needs-gated job must park while its group is held (MC-S3), got {}",
        run_b["jobs"]["two"]
    );

    // Completing `one` releases the group; `two` must then dispatch.
    complete_via_api(&app, a_id, "one").await;
    let run_b = get_run_json(&app, b_id).await;
    assert_eq!(
        run_b["jobs"]["two"], "queued",
        "parked gated job must dispatch once the group frees, got {}",
        run_b["jobs"]["two"]
    );
}

#[tokio::test]
async fn expanded_matrix_placeholder_does_not_leak_request_correlation() {
    // MC-2: a deferred-matrix node is non-caller, so submit mints its full
    // request correlation, but the node is routed to expansion and never
    // dispatched to a runner. Expansion deletes it from the run and no
    // completion path ever fires for it, so without explicit retirement its
    // request stays inflight for the life of the process, still resolvable to
    // a job that no longer exists.
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
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    // The placeholder holds a real, inflight request record before expansion.
    let (request_id, plan_id, agent_job_id, timeline_id) = {
        let inner = state.inner.lock().await;
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request");
        let ids = (
            record.request_id,
            record.plan_id.clone(),
            record.agent_job_id,
            record.timeline_id,
        );
        assert!(
            inner.inflight_requests.contains_key(&ids.0),
            "placeholder request must start out inflight"
        );
        ids
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "success",
            "outputs": {"matrix": r#"{"include": [{"os": "ubuntu-latest"}, {"os": "macos-latest"}]}"#}
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    assert!(
        !run.jobs.contains_key(&placeholder),
        "expansion must replace the placeholder with its combinations"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-2: placeholder request leaked in inflight_requests after expansion"
    );
    assert!(
        !inner.job_requests.contains_key(&request_id),
        "MC-2: placeholder job_request record leaked after expansion"
    );
    assert_ne!(
        inner.plan_requests.get(&plan_id),
        Some(&request_id),
        "MC-2: plan_requests still resolves to the deleted placeholder"
    );
    assert_ne!(
        inner.agent_job_requests.get(&agent_job_id),
        Some(&request_id),
        "MC-2: agent_job_requests still resolves to the deleted placeholder"
    );
    assert_ne!(
        inner.timeline_requests.get(&timeline_id),
        Some(&request_id),
        "MC-2: timeline_requests still resolves to the deleted placeholder"
    );

    // The fan-out jobs that replaced it keep their own correlation intact.
    for id in ["downstream (ubuntu-latest)", "downstream (macos-latest)"] {
        let job_id = JobId(id.to_string());
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == job_id)
            .unwrap_or_else(|| panic!("fan-out job {id} must keep its request record"));
        assert!(
            inner.inflight_requests.contains_key(&record.request_id),
            "fan-out job {id} must still be inflight"
        );
    }
}

#[tokio::test]
async fn cancelled_deferred_matrix_node_settles_submit_requests() {
    // MC-3: a needs-deferred matrix node cancelled before its expansion never
    // dispatches, so no completion, result patch or disconnect ever settles
    // the submit-time request correlation minted for it. The run-cancel path
    // must settle those records (result Cancelled, out of inflight, out of
    // every session) exactly as completion would — the state the
    // reusable-caller path has from submit, since callers mint nothing.
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
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    let (request_id, plan_id, agent_job_id, timeline_id) = {
        let inner = state.inner.lock().await;
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request");
        assert!(
            inner.inflight_requests.contains_key(&record.request_id),
            "placeholder request must start out inflight"
        );
        (
            record.request_id,
            record.plan_id.clone(),
            record.agent_job_id,
            record.timeline_id,
        )
    };

    let cancelled = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;
    assert_eq!(cancelled["status"], "cancelled");

    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .get(&request_id)
        .expect("settled placeholder keeps its record, like a completed job");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Cancelled),
        "MC-3: cancelled placeholder request must be settled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-3: cancelled placeholder must leave inflight_requests"
    );
    assert!(
        !inner
            .session_active_requests
            .values()
            .any(|&rid| rid == request_id),
        "MC-3: cancelled placeholder must leave session_active_requests"
    );
    // The correlation indexes keep resolving to the settled record, exactly
    // as they do for a job a runner completed.
    assert_eq!(
        inner.plan_requests.get(&plan_id),
        Some(&request_id),
        "plan_requests must keep resolving to the settled placeholder"
    );
    assert_eq!(
        inner.agent_job_requests.get(&agent_job_id),
        Some(&request_id),
        "agent_job_requests must keep resolving to the settled placeholder"
    );
    assert_eq!(
        inner.timeline_requests.get(&timeline_id),
        Some(&request_id),
        "timeline_requests must keep resolving to the settled placeholder"
    );
    // RenewJob correlation end-state: the broker refuses to renew a request
    // no session owns, so a cancelled placeholder can neither be renewed nor
    // resurrected.
    assert!(
        crate::broker::ensure_broker_request_owner(&inner, request_id, 1).is_err(),
        "MC-3: no runner may renew the cancelled placeholder"
    );
    // Completion-equivalent grant semantics, verified rather than assumed:
    // nothing outside the Purge arm ever removes these maps, for any job, so
    // a settled placeholder keeps its entries exactly like a completed job.
    assert!(
        inner
            .id_token_grants
            .contains_key(&(run_id, placeholder.clone())),
        "settled placeholder keeps its id-token grant like a completed job"
    );
    assert!(
        inner
            .oidc_job_contexts
            .contains_key(&(run_id, placeholder.clone())),
        "settled placeholder keeps its OIDC context like a completed job"
    );
    drop(inner);

    let run = get_run_json(&app, &run_id.to_string()).await;
    assert_eq!(run["jobs"]["downstream"], "cancelled");
}

#[tokio::test]
async fn cancelled_deferred_matrix_node_job_cancel_settles_requests() {
    // MC-3: the job-level cancel path (job-level concurrency cancel-in-
    // progress, holder cancellation) hits the same leak as a run cancel: a
    // parked deferred-matrix node's submit-time records stay active forever.
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
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    let request_id = {
        let inner = state.inner.lock().await;
        inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request")
            .request_id
    };

    {
        let mut inner = state.inner.lock().await;
        crate::runtime_scheduling::cancel_job_inner(&mut inner, run_id, &placeholder);
    }

    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .get(&request_id)
        .expect("settled placeholder keeps its record, like a completed job");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Cancelled),
        "MC-3: job-cancelled placeholder request must be settled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-3: job-cancelled placeholder must leave inflight_requests"
    );
    assert_eq!(
        inner.runs[&run_id].jobs.get(&placeholder),
        Some(&ExecutionStatus::Cancelled),
        "job cancel must still mark the node cancelled in the run"
    );
}

#[tokio::test]
async fn overflowed_run_settles_deferred_matrix_node_requests() {
    // MC-3: a run cancelled at submit by a workflow-concurrency queue
    // overflow never dispatches anything, yet the deferred-matrix node's
    // submit-time request records were minted before the gate check. They
    // must be settled like any other cancellation instead of leaking as
    // active forever.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let yaml = r#"
on: push
concurrency:
  group: overflow-group
  queue: max
jobs:
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#;
    // 1 running + 100 pending = 101 holders; 102nd arrival cancelled.
    let mut ids = Vec::new();
    for _ in 0..101 {
        let r = submit_yaml(&app, yaml, "owner/repo").await;
        ids.push(r["run_id"].as_str().unwrap().to_owned());
    }
    let overflow_id = submit_yaml(&app, yaml, "owner/repo").await;
    let overflow_id = overflow_id["run_id"].as_str().unwrap().to_owned();
    assert_eq!(
        get_run_json(&app, &overflow_id).await["status"],
        "cancelled"
    );

    let run_id: RunId = overflow_id.parse().unwrap();
    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .values()
        .find(|r| r.run_id == run_id && r.job_id == JobId("downstream".to_string()))
        .expect("overflowed run must still have minted the placeholder request");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Cancelled),
        "MC-3: overflowed run placeholder request must be settled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&record.request_id),
        "MC-3: overflowed run placeholder must leave inflight_requests"
    );
}

#[tokio::test]
async fn dependency_skipped_deferred_matrix_node_settles_requests() {
    // MC-3: a needs-deferred matrix node whose dependency fails is concluded
    // as Skipped by the dependency-decision arm of the promote sweep — never
    // dispatched, so no completion path settles its submit-time request
    // correlation. The skip arm must settle it like any other terminal
    // conclusion.
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
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: exit 1
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    let request_id = {
        let inner = state.inner.lock().await;
        inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request")
            .request_id
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "failure"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .get(&request_id)
        .expect("skipped placeholder keeps its record, like a completed job");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Skipped),
        "MC-3: dependency-skipped placeholder request must be settled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-3: dependency-skipped placeholder must leave inflight_requests"
    );
    assert!(
        !inner
            .session_active_requests
            .values()
            .any(|&rid| rid == request_id),
        "MC-3: dependency-skipped placeholder must leave session_active_requests"
    );
    assert_eq!(
        inner.runs[&run_id].jobs.get(&placeholder),
        Some(&ExecutionStatus::Skipped),
        "the node itself must be concluded Skipped in the run"
    );
}

#[tokio::test]
async fn dependency_error_deferred_matrix_node_settles_requests() {
    // MC-3: a needs-deferred matrix node whose `if:` expression fails to
    // evaluate is concluded as Failure by the dependency-decision arm of the
    // promote sweep. Its submit-time request correlation must be settled the
    // same way.
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
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    # Parse-valid but a runtime evaluation error: `format` cannot resolve the
    # placeholder with no arguments, a genuine condition error rather than a
    # false value, so the node concludes Failure rather than Skipped.
    if: ${{ format('{}') }}
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let placeholder = JobId("downstream".to_string());

    let request_id = {
        let inner = state.inner.lock().await;
        inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == placeholder)
            .expect("deferred-matrix placeholder must have a submit-time request")
            .request_id
    };

    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "generator",
            "status": "success"
        }),
    )
    .await;

    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .get(&request_id)
        .expect("errored placeholder keeps its record, like a completed job");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Failure),
        "MC-3: condition-error placeholder request must be settled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&request_id),
        "MC-3: condition-error placeholder must leave inflight_requests"
    );
    assert_eq!(
        inner.runs[&run_id].jobs.get(&placeholder),
        Some(&ExecutionStatus::Failure),
        "the node itself must be concluded Failure in the run"
    );
}

#[tokio::test]
async fn cancel_preserves_completed_reusable_caller_result() {
    // MC-3 review follow-up: a nested reusable caller that finished Success
    // while the run stayed active still sits in `run.caller_plans` with an
    // unsettled request record (`propagate_reusable_outputs` retires none).
    // The run-cancel sweep settles every expandable node, so it must settle
    // this one with its real verdict — Success — not clobber it to Cancelled.
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
  outer:
    uses: ./.github/workflows/outer.yml
  keepalive:
    runs-on: ubuntu-latest
    steps:
      - run: echo keepalive
"#,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {
                ".github/workflows/outer.yml": r#"
on: workflow_call
jobs:
  nested:
    uses: ./.github/workflows/inner.yml
"#,
                ".github/workflows/inner.yml": r#"
on: workflow_call
jobs:
  work:
    runs-on: ubuntu-latest
    steps:
      - run: echo work
"#,
            }
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let nested_caller = JobId("outer/nested".to_string());

    // The gate-free nested call materializes its whole subtree at submit.
    let leaf = {
        let inner = state.inner.lock().await;
        inner.runs[&run_id]
            .jobs
            .keys()
            .find(|id| id.0.starts_with("outer/nested/"))
            .expect("nested callee leaf must materialize at submit")
            .clone()
    };

    // Drive the nested caller to Success by completing its only leaf, while
    // `keepalive` stays queued so the run does not conclude.
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": leaf.0,
            "status": "success"
        }),
    )
    .await;

    let caller_request_id = {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.runs[&run_id].jobs.get(&nested_caller),
            Some(&ExecutionStatus::Success),
            "nested caller must aggregate to Success once its leaf completes"
        );
        assert!(
            inner.runs[&run_id]
                .caller_plans
                .contains_key(&nested_caller),
            "completed nested caller stays in caller_plans"
        );
        let record = inner
            .job_requests
            .values()
            .find(|r| r.run_id == run_id && r.job_id == nested_caller)
            .expect("nested caller minted a request record at expansion");
        // The completion path never settles a caller's own record: this is the
        // pre-existing unsettled state the cancel sweep must not corrupt.
        assert_eq!(
            record.result, None,
            "nested caller's record is unsettled before cancel"
        );
        record.request_id
    };

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/cancel"),
        Value::Null,
    )
    .await;

    let inner = state.inner.lock().await;
    let record = inner
        .job_requests
        .get(&caller_request_id)
        .expect("cancel keeps the settled caller record");
    assert_eq!(
        record.result,
        Some(ExecutionStatus::Success),
        "MC-3: cancel must settle a completed caller with Success, not Cancelled"
    );
    assert!(
        !inner.inflight_requests.contains_key(&caller_request_id),
        "MC-3: settled caller record must leave inflight_requests"
    );
    assert_eq!(
        inner.runs[&run_id].jobs.get(&nested_caller),
        Some(&ExecutionStatus::Success),
        "the completed caller keeps its Success status through cancellation"
    );
    assert_eq!(
        inner.runs[&run_id]
            .jobs
            .get(&JobId("keepalive".to_string())),
        Some(&ExecutionStatus::Cancelled),
        "the still-queued keepalive job is cancelled by the run cancel"
    );
}

// ---------------------------------------------------------------------------
// Durable-store restart contracts.
//
// Each test below writes state, drops the store, reopens it, and asserts on
// the recovered `InnerState`. Before the fixes they accompany, every one of
// them failed while `just test-ci` stayed green — the gate had no restart
// dimension at all.
// ---------------------------------------------------------------------------

/// Secrets must come back as themselves. `SecretString::Serialize` emits the
/// literal `"<redacted>"`, so any persistence path that does not go through
/// `WorkflowSubmission::to_request_json` silently substitutes the redaction
/// marker for every secret and the resumed run authenticates with garbage.

// ---------------------------------------------------------------------------
// Durable-store restart contracts.
//
// Each test below writes state, drops the store, reopens it, and asserts on
// the recovered `InnerState`. Before the fixes they accompany, every one of
// them failed while `just test-ci` stayed green — the gate had no restart
// dimension at all.
// ---------------------------------------------------------------------------

/// Secrets must come back as themselves. `SecretString::Serialize` emits the
/// literal `"<redacted>"`, so any persistence path that does not go through
/// `WorkflowSubmission::to_request_json` silently substitutes the redaction
/// marker for every secret and the resumed run authenticates with garbage.
#[tokio::test]
async fn store_recovery_preserves_run_secrets() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let run_id = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo",
                "secrets": {"MY_TOKEN": "s3cr3t-value", "OTHER": "second-value"}
            }),
        )
        .await;
        accepted["run_id"]
            .as_str()
            .unwrap()
            .parse::<RunId>()
            .unwrap()
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let secrets = &inner
        .runs
        .get(&run_id)
        .expect("run survives restart")
        .submission
        .secrets;
    assert_eq!(
        secrets.get("MY_TOKEN").map(|s| s.expose()),
        Some("s3cr3t-value")
    );
    assert_eq!(
        secrets.get("OTHER").map(|s| s.expose()),
        Some("second-value")
    );
}

/// The ready queue is FIFO across runs, not just within one. `store_run_event`
/// rewrites a single run's rows, so its `queue_position` values have to stay on
/// the same global scale as every other writer's; numbering from zero per run
/// gave every run a `position = 0` job and interleaved them on restore.

/// The ready queue is FIFO across runs, not just within one. `store_run_event`
/// rewrites a single run's rows, so its `queue_position` values have to stay on
/// the same global scale as every other writer's; numbering from zero per run
/// gave every run a `position = 0` job and interleaved them on restore.
#[tokio::test]
async fn store_recovery_preserves_cross_run_queue_order() {
    let temp = tempfile::tempdir().unwrap();
    let workflow = "on: push\njobs:\n  one:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  two:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let before: Vec<String> = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        for repo in ["owner/alpha", "owner/beta", "owner/gamma"] {
            request_json(
                &app,
                Method::POST,
                "/api/v1/runs",
                json!({"workflow_yaml": workflow, "event": "push", "repository": repo}),
            )
            .await;
        }
        let inner = state.inner.lock().await;
        inner
            .queue
            .iter()
            .map(|job| format!("{}:{}", job.run_id, job.job_id.0))
            .collect()
    };
    assert_eq!(before.len(), 6, "three runs of two jobs must all be queued");

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let after: Vec<String> = inner
        .queue
        .iter()
        .map(|job| format!("{}:{}", job.run_id, job.job_id.0))
        .collect();
    assert_eq!(before, after, "ready-queue FIFO order must survive restart");
}

/// A job message that was dequeued but not yet delivered has to be re-delivered
/// after a restart, otherwise the runner polls forever for an assignment the
/// server believes it already handed out.

/// A job message that was dequeued but not yet delivered has to be re-delivered
/// after a restart, otherwise the runner polls forever for an assignment the
/// server believes it already handed out.
#[tokio::test]
async fn store_recovery_preserves_broker_and_inflight_messages() {
    let temp = tempfile::tempdir().unwrap();
    {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let mut inner = state.inner.lock().await;
        inner
            .inflight_messages
            .entry("sess-1".to_owned())
            .or_default()
            .insert(
                7,
                azdo::TaskAgentMessage {
                    message_id: 7,
                    message_type: "PipelineAgentJobRequest".to_owned(),
                    body: "e30=".to_owned(),
                    iv: None,
                },
            );
        state
            .store
            .store_inner(&crate::store::StoreSnapshot::from_inner(&inner))
            .await
            .unwrap();
    }

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let session = inner
        .inflight_messages
        .get("sess-1")
        .expect("undelivered broker message must survive restart");
    let message = session.get(&7).expect("message id must be preserved");
    assert_eq!(message.message_type, "PipelineAgentJobRequest");
    assert_eq!(message.body, "e30=");
}

/// The in-flight cache payload must never enter the runtime snapshot. It is a
/// `Vec<u8>` holding the whole upload, and the snapshot is cloned, serialized
/// and AES-sealed on every `store_meta_only` — putting it there made
/// `cache_upload` quadratic in cache size with the global state lock held.

/// The in-flight cache payload must never enter the runtime snapshot. It is a
/// `Vec<u8>` holding the whole upload, and the snapshot is cloned, serialized
/// and AES-sealed on every `store_meta_only` — putting it there made
/// `cache_upload` quadratic in cache size with the global state lock held.
#[tokio::test]
async fn cache_upload_payload_stays_out_of_the_runtime_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let reserve = request_json(
        &app,
        Method::POST,
        "/_apis/artifactcache/cache",
        json!({"key": "big", "version": "v1"}),
    )
    .await;
    let cache_id = reserve["cacheId"].as_i64().unwrap();

    let payload = vec![b'x'; 1 << 20]; // 1 MiB (under the default body limit)
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(payload.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    // Force a snapshot with the upload still buffered in memory.
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.pending_caches.get(&cache_id).map(|c| c.bytes.len()),
            Some(payload.len()),
            "the upload is buffered in memory"
        );
        state
            .store
            .store_meta_only(&crate::store::build_meta_snapshot(&inner))
            .await
            .unwrap();
    }

    let db = temp.path().join("preloop.db");
    let connection = rusqlite::Connection::open(&db).unwrap();
    let blob_len: i64 = connection
        .query_row(
            "SELECT length(meta_blob) FROM runtime_snapshots WHERE snapshot_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        blob_len < 64 * 1024,
        "runtime snapshot is {blob_len} bytes after a {} byte upload — the cache \
         payload leaked into the meta blob",
        payload.len()
    );
}

/// Two servers booting against one Postgres database must both start.
/// `CREATE TABLE IF NOT EXISTS` is not race-safe in Postgres: the existence
/// check and the `pg_type` insert are separate, so an unguarded migration makes
/// the loser fail with a `pg_type_typname_nsp_index` unique violation.

/// Two servers booting against one Postgres database must both start.
/// `CREATE TABLE IF NOT EXISTS` is not race-safe in Postgres: the existence
/// check and the `pg_type` insert are separate, so an unguarded migration makes
/// the loser fail with a `pg_type_typname_nsp_index` unique violation.
#[tokio::test]
async fn postgres_concurrent_open_serializes_migrations() {
    let Ok(base) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if base.trim().is_empty() {
        return;
    }
    let dbname = format!("preloop_race_{}", uuid::Uuid::new_v4().simple());
    {
        let connect_url = crate::store_pg::connect_url(&base);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute(&format!("CREATE DATABASE {dbname}"), &[])
            .await
            .unwrap();
    }
    let fresh = base
        .rsplit_once('/')
        .map(|(host, _)| format!("{host}/{dbname}"))
        .unwrap();

    let key = b"concurrent-open-root-key";
    let dir = std::path::Path::new("/tmp");
    let (first, second) = tokio::join!(
        crate::store::open_store(Some(&fresh), dir, key),
        crate::store::open_store(Some(&fresh), dir, key),
    );
    assert!(first.is_ok(), "first opener failed: {:?}", first.err());
    assert!(second.is_ok(), "second opener failed: {:?}", second.err());
}

/// Postgres twin of `store_recovery_preserves_run_secrets`. The redaction bug
/// lived in the shared serialization path, so both backends have to prove it.

/// Postgres twin of `store_recovery_preserves_run_secrets`. The redaction bug
/// lived in the shared serialization path, so both backends have to prove it.
#[tokio::test]
async fn postgres_recovery_preserves_run_secrets() {
    let Ok(pg_url) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if pg_url.trim().is_empty() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let config_path = crate::config::config_path();
    // Start from a known state: the shared test database may hold rows left by
    // earlier Postgres tests (they restore into the queue on load). A
    // brand-new database has no tables yet; only clean a schema that exists.
    {
        let connect_url = crate::store_pg::connect_url(&pg_url);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_schema: bool = client
            .query_one(
                "SELECT to_regclass('public.workflow_run_counters') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if has_schema {
            client
                .batch_execute(
                    "TRUNCATE workflow_run_counters, runs, runners, runner_labels,
                             runner_sessions, jobs, job_dependencies, job_requests, control_events,
                             session_active_requests, broker_messages, job_request_messages,
                             log_files, log_chunks, runtime_snapshots RESTART IDENTITY CASCADE",
                )
                .await
                .unwrap();
        }
    }
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let run_id = {
        let state = AppState::new_with_store(
            temp.path().to_path_buf(),
            config_path.clone(),
            Some(&pg_url),
        )
        .await
        .unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/pg-secrets",
                "secrets": {"MY_TOKEN": "s3cr3t-value"}
            }),
        )
        .await;
        accepted["run_id"]
            .as_str()
            .unwrap()
            .parse::<RunId>()
            .unwrap()
    };

    let recovered = AppState::new_with_store(temp.path().to_path_buf(), config_path, Some(&pg_url))
        .await
        .unwrap();
    let inner = recovered.inner.lock().await;
    assert_eq!(
        inner
            .runs
            .get(&run_id)
            .expect("run survives restart")
            .submission
            .secrets
            .get("MY_TOKEN")
            .map(|s| s.expose()),
        Some("s3cr3t-value")
    );
}

// ---------------------------------------------------------------------------
// Regression tests for the cubic.dev review blockers on PR #27.
// ---------------------------------------------------------------------------

/// Restart while a reusable-caller node is parked (its concurrency gate is
/// held by an earlier run) must keep the caller plan and the expansion-only
/// fields (`github`, `head_sha`, `workflow_ref`) that the scheduler needs to
/// materialize the callee subtree later. They were `#[serde(skip)]` on
/// `RunRecord`, so a restart reset them to defaults and the deferred
/// expansion failed or misbuilt.

// ---------------------------------------------------------------------------
// Regression tests for the cubic.dev review blockers on PR #27.
// ---------------------------------------------------------------------------

/// Restart while a reusable-caller node is parked (its concurrency gate is
/// held by an earlier run) must keep the caller plan and the expansion-only
/// fields (`github`, `head_sha`, `workflow_ref`) that the scheduler needs to
/// materialize the callee subtree later. They were `#[serde(skip)]` on
/// `RunRecord`, so a restart reset them to defaults and the deferred
/// expansion failed or misbuilt.
#[tokio::test]
async fn store_recovery_preserves_deferred_caller_plan_and_expansion_fields() {
    let temp = tempfile::tempdir().unwrap();
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
            "reusable_workflows": { ".github/workflows/callee.yml": callee_yaml },
        })
    };

    let (parked_run, github, head_sha, workflow_ref) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let first = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
        let second = request_json(&app, Method::POST, "/api/v1/runs", submission()).await;
        let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
        let parked: RunId = second["run_id"].as_str().unwrap().parse().unwrap();
        let inner = state.inner.lock().await;
        // First caller's gate is free: subtree materialized immediately.
        assert_eq!(
            inner.runs[&first_run].jobs[&JobId("call/inner".to_owned())],
            ExecutionStatus::Queued
        );
        // Second caller is parked behind the gate with its plan in the run.
        let run = &inner.runs[&parked];
        assert_eq!(
            run.jobs[&JobId("call".to_owned())],
            ExecutionStatus::Pending
        );
        assert!(
            run.caller_plans.contains_key(&JobId("call".to_owned())),
            "parked caller must keep its plan pre-restart"
        );
        assert!(
            !run.github.is_null() && !run.head_sha.is_empty() && !run.workflow_ref.is_empty(),
            "expansion fields must be populated pre-restart"
        );
        (
            parked,
            run.github.clone(),
            run.head_sha.clone(),
            run.workflow_ref.clone(),
        )
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let run = inner
        .runs
        .get(&parked_run)
        .expect("parked run survives restart");
    assert!(
        run.caller_plans.contains_key(&JobId("call".to_owned())),
        "deferred caller plan must survive restart"
    );
    assert_eq!(run.github, github, "github context must survive restart");
    assert_eq!(run.head_sha, head_sha, "head_sha must survive restart");
    assert_eq!(
        run.workflow_ref, workflow_ref,
        "workflow_ref must survive restart"
    );
}

/// A job claimed (dequeued, broker message handed to a session) but not yet
/// acked is re-delivered after a restart, even when the only write between
/// the claim and the crash was a `store_run_event` for another status change.

/// A job claimed (dequeued, broker message handed to a session) but not yet
/// acked is re-delivered after a restart, even when the only write between
/// the claim and the crash was a `store_run_event` for another status change.
#[tokio::test]
async fn store_recovery_preserves_claim_state_across_run_events() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let (claimed_job, other_job, request_id) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // Simulate the broker claiming `build`: dequeued, message parked in
        // the per-session and per-request maps, session claim recorded.
        let (claimed_job, other_job, request_id) = {
            let mut inner = state.inner.lock().await;
            let claimed = inner
                .queue
                .iter()
                .find(|job| job.job_id.0 == "build")
                .cloned()
                .expect("build job queued");
            inner.queue.retain(|job| job.job_id.0 != "build");
            let request = inner
                .job_requests
                .values()
                .find(|record| record.job_id.0 == "build")
                .cloned()
                .expect("build request");
            inner
                .session_active_requests
                .insert("sess-1".to_owned(), request.request_id);
            inner
                .inflight_messages
                .entry("sess-1".to_owned())
                .or_default()
                .insert(
                    99,
                    azdo::TaskAgentMessage {
                        message_id: 99,
                        message_type: "PipelineAgentJobRequest".to_owned(),
                        body: "e30=".to_owned(),
                        iv: None,
                    },
                );
            inner
                .broker_messages
                .insert(request.request_id, claimed.message.clone());
            (
                claimed.job_id.clone(),
                JobId("test".to_owned()),
                request.request_id,
            )
        };

        // The only store write after the claim: a status event for the OTHER
        // job of the same run (store_run_event).
        state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id: other_job.clone(),
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;
        (claimed_job, other_job, request_id)
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    assert!(
        inner
            .inflight_messages
            .get("sess-1")
            .and_then(|messages| messages.get(&99))
            .is_some(),
        "undelivered broker message must survive a store_run_event restart"
    );
    assert!(
        inner.broker_messages.contains_key(&request_id),
        "per-request job message must survive a store_run_event restart"
    );
    assert_eq!(
        inner.session_active_requests.get("sess-1"),
        Some(&request_id),
        "session claim must survive a store_run_event restart"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id == other_job)
            && !inner.queue.iter().any(|job| job.job_id == claimed_job),
        "claimed job stays dequeued; the unclaimed job stays queued"
    );
}

/// Pool pairing state — one-time provision proof, strict job assignments and
/// pending pairings — plus the OAuth `client_id` map must survive a restart.

/// Pool pairing state — one-time provision proof, strict job assignments and
/// pending pairings — plus the OAuth `client_id` map must survive a restart.
#[tokio::test]
async fn store_recovery_preserves_pool_pairing_and_oauth_client_ids() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let (run_id, now) = {
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
        // The store persists SystemTime as microseconds
        // (`system_time_us`/`system_time_from_us`), so a nanosecond-precision
        // `now` can never equal the recovered value on Linux hosts (where
        // SystemTime has ns resolution). Round to the store's precision —
        // the same fix `5f96d0dd` applied to the sibling assertions here.
        let now = std::time::SystemTime::now();
        let now = std::time::UNIX_EPOCH
            + std::time::Duration::from_micros(
                now.duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros()
                    .min(u64::MAX as u128) as u64,
            );
        {
            let mut inner = state.inner.lock().await;
            inner.runner_client_ids.insert("client-abc".to_owned(), 42);
            inner.pool_proven_runners.insert(7);
            inner.job_assignments.insert(
                (run_id, JobId("build".to_owned())),
                AssignmentRecord {
                    runner_id: Some(7),
                    at: now,
                    first_at: now,
                },
            );
            inner
                .pool_pending
                .insert((run_id, JobId("build".to_owned())), now);
            state
                .store
                .store_inner(&crate::store::StoreSnapshot::from_inner(&inner))
                .await
                .unwrap();
        }
        (run_id, now)
    };

    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    assert_eq!(
        inner.runner_client_ids.get("client-abc"),
        Some(&42),
        "OAuth client id must survive restart"
    );
    assert!(
        inner.pool_proven_runners.contains(&7),
        "provision-token proof must survive restart"
    );
    let assignment = inner
        .job_assignments
        .get(&(run_id, JobId("build".to_owned())))
        .expect("job assignment must survive restart");
    assert_eq!(assignment.runner_id, Some(7));
    assert_eq!(assignment.at, now);
    assert_eq!(assignment.first_at, now);
    assert!(
        inner
            .pool_pending
            .contains_key(&(run_id, JobId("build".to_owned()))),
        "pending pairing must survive restart"
    );
}
/// `ServerConfig`'s Debug output must never print a Postgres password.

/// `ServerConfig`'s Debug output must never print a Postgres password.
#[test]
fn server_config_debug_redacts_store_url_password() {
    let config = ServerConfig {
        listen: "127.0.0.1:9090".parse().unwrap(),
        systemd_socket_activation: false,
        unix_socket: None,
        state_dir: std::path::PathBuf::from(".preloop"),
        store_url: Some(
            "postgres://preloop:hunter2-secret@db.example:5432/preloop?sslmode=verify-full"
                .to_owned(),
        ),
        record_flows: None,
        tls: TlsMode::None,
        queue_depth: None,
        next_job_runs_on: None,
        pool_preparing: None,
        enable_test_api: false,
        test_api_token: Some("super-secret-token".to_owned()),
        oidc_issuer: None,
        enable_scheduler: false,
        pending_registrations: None,
        pool_status: None,
        observability: None,
        require_job_assignments: false,
    };
    let debug = format!("{config:?}");
    assert!(
        !debug.contains("hunter2-secret"),
        "Debug must not expose the Postgres password: {debug}"
    );
    assert!(
        !debug.contains("super-secret-token"),
        "Debug must not expose the test API token: {debug}"
    );
    assert!(
        debug.contains("preloop:***@db.example"),
        "Debug should keep the masked URL shape"
    );
}

/// Postgres twin of `store_recovery_preserves_claim_state_across_run_events`:
/// the `job_request_messages` table and the claim rewrite inside
/// `store_run_event` are backend-specific SQL, so the round-trip has to be
/// proven against a live database too.

/// Postgres twin of `store_recovery_preserves_claim_state_across_run_events`:
/// the `job_request_messages` table and the claim rewrite inside
/// `store_run_event` are backend-specific SQL, so the round-trip has to be
/// proven against a live database too.
#[tokio::test]
async fn postgres_recovery_preserves_claim_state_across_run_events() {
    let Ok(pg_url) = std::env::var("PRELOOP_TEST_PG_URL") else {
        eprintln!("skipping: set PRELOOP_TEST_PG_URL to a disposable Postgres URL");
        return;
    };
    if pg_url.trim().is_empty() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let config_path = crate::config::config_path();
    // Isolate from earlier Postgres tests sharing this database: their rows
    // restore into the queue on load. Only clean a schema that already exists.
    {
        let connect_url = crate::store_pg::connect_url(&pg_url);
        let (client, connection) = tokio_postgres::connect(&connect_url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_schema: bool = client
            .query_one(
                "SELECT to_regclass('public.workflow_run_counters') IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if has_schema {
            client
                .batch_execute(
                    "TRUNCATE workflow_run_counters, runs, runners, runner_labels,
                             runner_sessions, jobs, job_dependencies, job_requests, control_events,
                             session_active_requests, broker_messages, job_request_messages,
                             log_files, log_chunks, runtime_snapshots RESTART IDENTITY CASCADE",
                )
                .await
                .unwrap();
        }
    }
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let (claimed_job, other_job, request_id) = {
        let state = AppState::new_with_store(
            temp.path().to_path_buf(),
            config_path.clone(),
            Some(&pg_url),
        )
        .await
        .unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let (claimed_job, other_job, request_id) = {
            let mut inner = state.inner.lock().await;
            let claimed = inner
                .queue
                .iter()
                .find(|job| job.job_id.0 == "build")
                .cloned()
                .expect("build job queued");
            inner.queue.retain(|job| job.job_id.0 != "build");
            let request = inner
                .job_requests
                .values()
                .find(|record| record.job_id.0 == "build")
                .cloned()
                .expect("build request");
            inner
                .session_active_requests
                .insert("sess-pg".to_owned(), request.request_id);
            inner
                .inflight_messages
                .entry("sess-pg".to_owned())
                .or_default()
                .insert(
                    99,
                    azdo::TaskAgentMessage {
                        message_id: 99,
                        message_type: "PipelineAgentJobRequest".to_owned(),
                        body: "e30=".to_owned(),
                        iv: None,
                    },
                );
            inner
                .broker_messages
                .insert(request.request_id, claimed.message.clone());
            (
                claimed.job_id.clone(),
                JobId("test".to_owned()),
                request.request_id,
            )
        };
        state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id: other_job.clone(),
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;
        (claimed_job, other_job, request_id)
    };

    let recovered = AppState::new_with_store(temp.path().to_path_buf(), config_path, Some(&pg_url))
        .await
        .unwrap();
    let inner = recovered.inner.lock().await;
    assert!(
        inner
            .inflight_messages
            .get("sess-pg")
            .and_then(|messages| messages.get(&99))
            .is_some(),
        "undelivered broker message must survive a store_run_event restart (PG)"
    );
    assert!(
        inner.broker_messages.contains_key(&request_id),
        "per-request job message must survive a store_run_event restart (PG)"
    );
    assert_eq!(
        inner.session_active_requests.get("sess-pg"),
        Some(&request_id),
        "session claim must survive a store_run_event restart (PG)"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id == other_job)
            && !inner.queue.iter().any(|job| job.job_id == claimed_job),
        "claimed job stays dequeued; the unclaimed job stays queued (PG)"
    );
}

/// A restart destroys every pool machine but persists its claim, so the
/// request returns pinned to a session that will never poll again. Nothing
/// can complete it and nothing can re-claim it: the run — and the GitHub
/// check run it created — would sit queued forever while the pool idles.
/// Startup reconciliation must settle those claims, and only those.

/// A restart destroys every pool machine but persists its claim, so the
/// request returns pinned to a session that will never poll again. Nothing
/// can complete it and nothing can re-claim it: the run — and the GitHub
/// check run it created — would sit queued forever while the pool idles.
/// Startup reconciliation must settle those claims, and only those.
#[tokio::test]
async fn startup_fails_claims_orphaned_by_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let workflow =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 1\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo 2\n";
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({"workflow_yaml": workflow, "event": "push", "repository": "owner/repo"}),
    )
    .await;
    let _run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // A pool machine claimed `build`, then the control plane restarted: the
    // pin survives, its session does not.
    let (claimed_request, queued_request) = {
        let mut inner = state.inner.lock().await;
        let claimed = inner
            .queue
            .iter()
            .find(|job| job.job_id.0 == "build")
            .cloned()
            .expect("build job queued");
        inner.queue.retain(|job| job.job_id.0 != "build");
        let claimed_request = inner
            .job_requests
            .values()
            .find(|record| record.job_id.0 == "build")
            .map(|record| record.request_id)
            .expect("build request");
        let queued_request = inner
            .job_requests
            .values()
            .find(|record| record.job_id.0 == "test")
            .map(|record| record.request_id)
            .expect("test request");
        inner
            .session_active_requests
            .insert("dead-session".to_owned(), claimed_request);
        inner
            .broker_messages
            .insert(claimed_request, claimed.message.clone());
        assert!(
            inner.sessions.is_empty(),
            "no session survives the restart in this scenario"
        );
        (claimed_request, queued_request)
    };

    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let settled = crate::broker::reconcile_orphaned_claims(&shared).await;
    assert_eq!(settled, 1, "exactly the orphaned claim is settled");

    let inner = state.inner.lock().await;
    assert_eq!(
        inner
            .job_requests
            .get(&claimed_request)
            .and_then(|record| record.result),
        Some(ExecutionStatus::Failure),
        "an unclaimable job must be reported failed, not left queued forever"
    );
    assert!(
        !inner
            .session_active_requests
            .values()
            .any(|request_id| *request_id == claimed_request),
        "the dead session's claim must be released"
    );
    assert_eq!(
        inner
            .job_requests
            .get(&queued_request)
            .and_then(|record| record.result),
        None,
        "a job that was never claimed stays runnable"
    );
    assert!(
        inner.queue.iter().any(|job| job.job_id.0 == "test"),
        "the unclaimed job stays in the queue for a fresh machine"
    );
}

/// Versions before the runner-purge fix put the logical job back on the queue
/// but left its request owned by a dead runner and detached from every session.
/// Startup must release that persisted correlation for a replacement runner.

/// Versions before the runner-purge fix put the logical job back on the queue
/// but left its request owned by a dead runner and detached from every session.
/// Startup must release that persisted correlation for a replacement runner.
#[tokio::test]
async fn startup_releases_an_orphaned_request_whose_job_was_requeued() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_simple_run(&app).await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let job_id = JobId("build".to_owned());

    let request_id = {
        let mut inner = state.inner.lock().await;
        let request_id = inner
            .job_requests
            .values()
            .find(|record| record.run_id == run_id && record.job_id == job_id)
            .map(|record| record.request_id)
            .expect("queued job request");
        let record = inner.job_requests.get_mut(&request_id).unwrap();
        record.owner_runner_id = Some(99);
        record.started_at = Some(SystemTime::now() - Duration::from_secs(300));
        request_id
    };

    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let settled = crate::broker::reconcile_orphaned_claims(&shared).await;
    assert_eq!(settled, 1);

    {
        let inner = state.inner.lock().await;
        let request = &inner.job_requests[&request_id];
        assert_eq!(
            request.result, None,
            "the queued request must remain completable"
        );
        assert_eq!(
            request.owner_runner_id, None,
            "startup must release the dead runner owner"
        );
        assert_eq!(request.started_at, None);
        assert_eq!(request.last_renewed_at, None);
        assert!(
            inner.inflight_requests.contains_key(&request_id),
            "the request correlation must remain inflight"
        );
        assert!(
            inner
                .queue
                .iter()
                .any(|job| job.run_id == run_id && job.job_id == job_id),
            "the replacement attempt remains queued"
        );
        assert_eq!(
            inner.runs[&run_id].jobs[&job_id],
            ExecutionStatus::Queued,
            "releasing the old owner must not conclude its logical job"
        );
        assert!(crate::runtime_scheduling::live_runner_assignments(
            &inner.job_requests,
            &inner.session_active_requests,
            SystemTime::now(),
        )
        .is_empty());
    }

    let (runner_id, token) =
        register_runner_with_token(&app, "startup-replacement", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token, session_id).await;
    assert!(
        !delivered.is_null(),
        "replacement must receive restored retry"
    );
    request_json_with_bearer(
        &app,
        Method::PATCH,
        &format!("/_apis/v1/AgentRequest/1/{request_id}"),
        json!({"result": "Succeeded"}),
        &token,
    )
    .await;
    let inner = state.inner.lock().await;
    assert_eq!(
        inner.runs[&run_id].jobs[&job_id],
        ExecutionStatus::Success,
        "restored retry must complete through the retained correlation"
    );
}

// ---------------------------------------------------------------------------
// Submit-driven CI push-back (`--push`): the server verifies the tested tree,
// creates the draft PR, and stays idempotent across replays.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_driven_push_publishes_pr_and_checks_idempotently() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let pr_creates = Arc::new(AtomicUsize::new(0));
    let pr_bodies = Arc::new(Mutex::new(Vec::<Value>::new()));
    let check_completions = Arc::new(Mutex::new(Vec::<Value>::new()));

    let mock_app = Router::new()
        .route(
            "/repos/owner/repo",
            get(|| async { Json(json!({"default_branch": "main"})) }),
        )
        .route(
            "/repos/owner/repo/commits/:sha",
            get(|Path(_sha): Path<String>| async move {
                Json(json!({"commit": {"tree": {"sha": TREE}}}))
            }),
        )
        .route(
            "/repos/owner/repo/pulls",
            get(|| async { Json(json!([])) }).post({
                let pr_creates = pr_creates.clone();
                let pr_bodies = pr_bodies.clone();
                move |body: axum::extract::Json<Value>| {
                    let pr_creates = pr_creates.clone();
                    let pr_bodies = pr_bodies.clone();
                    async move {
                        pr_creates.fetch_add(1, Ordering::SeqCst);
                        pr_bodies.lock().unwrap().push(body.0);
                        Json(json!({"number": 42}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs",
            post(|| async { Json(json!({"id": 7})) }),
        )
        .route(
            "/repos/owner/repo/check-runs/:id",
            axum::routing::patch({
                let check_completions = check_completions.clone();
                move |Path(id): Path<u64>, body: axum::extract::Json<Value>| {
                    let check_completions = check_completions.clone();
                    async move {
                        check_completions.lock().unwrap().push(body.0);
                        Json(json!({"id": id}))
                    }
                }
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    // Held for the whole test: the GitHub env vars are process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _api_url =
        crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "sync-test-token");

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. A --push submission reports queued check runs at accept time and
    //    starts in `pending`.
    let accepted = submit_push_run(&app, SHA, TREE).await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.job_check_run_ids.len(), 1, "queued check run at submit");
        assert_eq!(
            *run.job_check_run_ids.values().next().unwrap(),
            7,
            "check run id comes from the (mock) GitHub API"
        );
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Pending);
    }

    // 2. Sync before the run is terminal is refused.
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 3. Terminal run: the sync verifies the tree, creates the draft PR,
    //    and marks the run pushed.
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
    }
    let pushed = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(pushed["status"], "pushed");
    assert_eq!(pushed["pr_number"], 42);
    assert!(pushed["pr_url"].as_str().unwrap().ends_with("/pull/42"));

    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Synced);
        assert_eq!(run.push_state.as_ref().unwrap().pr_number, Some(42));
    }
    let pr_body = pr_bodies.lock().unwrap().first().unwrap().clone();
    assert_eq!(pr_body["head"], "feat/x");
    assert_eq!(pr_body["base"], "main");
    assert_eq!(pr_body["draft"], true, "new PRs are drafts by default");
    assert!(pr_body["body"].as_str().unwrap().contains(SHA));
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1);
    assert!(
        check_completions.lock().unwrap().is_empty(),
        "jobs with a check run at submit are not re-reported by the sync"
    );

    // 4. Replay is a no-op: no second PR, same response.
    let again = request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(again["pr_number"], 42);
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1, "idempotent replay");

    // 5. A pushed tree that differs from the tested tree blocks the sync.
    let accepted = submit_push_run(&app, SHA, "cccccccccccccccccccccccccccccccccccccccc").await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
    }
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Blocked);
        assert!(run
            .push_state
            .as_ref()
            .unwrap()
            .error
            .as_deref()
            .unwrap()
            .contains("does not match"));
    }

    // 6. A run submitted without --push can never be pushed.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (status, _) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 7. Push-back lands the commit on GitHub, which answers with a push
    //    webhook for that same commit. The workflow that was already tested
    //    and published must not run a second time, while a workflow the user
    //    never submitted still has to.
    const PUBLISHED_WORKFLOW: &str = ".github/workflows/ci.yml";
    let accepted = submit_push_run(&app, SHA, TREE).await;
    let published_id = accepted["run_id"]
        .as_str()
        .unwrap()
        .parse::<RunId>()
        .unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&published_id).unwrap();
        run.conclusion = Some("success".to_owned());
        let mut submission = (*run.submission).clone();
        submission.workflow_path = Some(PUBLISHED_WORKFLOW.to_owned());
        run.submission = Arc::new(submission);
    }
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    assert_eq!(
        crate::github_push::already_published(&shared, "owner/repo", SHA, PUBLISHED_WORKFLOW).await,
        Some(published_id),
        "the echo of our own push must be recognised"
    );
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            SHA,
            ".github/workflows/other.yml"
        )
        .await,
        None,
        "a workflow that was never submitted is new work and must still run"
    );
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            "dddddddddddddddddddddddddddddddddddddddd",
            PUBLISHED_WORKFLOW
        )
        .await,
        None,
        "a different commit is different work"
    );

    // A dirty-tree run's submission sha is the *base* commit; the webhook
    // echo carries the materialized commit recorded in push_state.
    // already_published must recognise that commit too, or every dirty-tree
    // push would re-run CI.
    const BASE_SHA: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    const MATERIALIZED_SHA: &str = "ffffffffffffffffffffffffffffffffffffffff";
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feat/y",
            "sha": BASE_SHA,
            "push": {"create_pr": true, "draft_pr": true, "dirty": true},
            "push_tree": TREE,
            "workflow_path": PUBLISHED_WORKFLOW,
        }),
    )
    .await;
    let dirty_id = accepted["run_id"]
        .as_str()
        .unwrap()
        .parse::<RunId>()
        .unwrap();
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&dirty_id).unwrap();
        run.conclusion = Some("success".to_owned());
        run.push_state = Some(crate::models::PushState {
            status: crate::models::PushStatus::Synced,
            error: None,
            pr_number: Some(7),
            effective_sha: Some(MATERIALIZED_SHA.to_owned()),
        });
    }
    assert_eq!(
        crate::github_push::already_published(
            &shared,
            "owner/repo",
            MATERIALIZED_SHA,
            PUBLISHED_WORKFLOW
        )
        .await,
        Some(dirty_id),
        "the webhook echo of a materialized dirty-tree commit must be recognised"
    );
    assert_eq!(
        crate::github_push::already_published(&shared, "owner/repo", BASE_SHA, PUBLISHED_WORKFLOW)
            .await,
        Some(dirty_id),
        "the recorded submission sha (the base commit) still matches, as for any push-back run"
    );
}

#[tokio::test]
async fn dirty_push_sync_verifies_the_branch_head_and_reports_checks_on_the_materialized_commit() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    const BASE_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const MATERIALIZED: &str = "cccccccccccccccccccccccccccccccccccccccc";

    // A dirty-tree submission records the base commit as `sha`; the tested
    // commit is materialized after the run and pushed to the branch head, so
    // the sync must verify the BRANCH (not the base sha) and report checks
    // against the materialized head.
    let pr_creates = Arc::new(AtomicUsize::new(0));
    let check_creates = Arc::new(parking_lot::Mutex::new(Vec::<Value>::new()));
    let mock_app = Router::new()
        .route(
            "/repos/owner/repo",
            get(|| async { Json(json!({"default_branch": "main"})) }),
        )
        .route(
            // Branch names may contain slashes; GitHub's commits/{ref}
            // endpoint matches the whole remaining path.
            "/repos/owner/repo/commits/*ref",
            get(|Path(r#ref): Path<String>| async move {
                assert_eq!(r#ref, "feat/x", "dirty sync must verify the branch head");
                Json(json!({
                    "sha": MATERIALIZED,
                    "commit": {"tree": {"sha": TREE}},
                }))
            }),
        )
        .route(
            "/repos/owner/repo/pulls",
            get(|| async { Json(json!([])) }).post({
                let pr_creates = pr_creates.clone();
                move |_body: axum::extract::Json<Value>| {
                    let pr_creates = pr_creates.clone();
                    async move {
                        pr_creates.fetch_add(1, Ordering::SeqCst);
                        Json(json!({"number": 42}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs",
            post({
                let check_creates = check_creates.clone();
                move |body: axum::extract::Json<Value>| {
                    let check_creates = check_creates.clone();
                    async move {
                        check_creates.lock().push(body.0);
                        Json(json!({"id": 7}))
                    }
                }
            }),
        )
        .route(
            "/repos/owner/repo/check-runs/:id",
            axum::routing::patch(|| async { Json(json!({"id": 7})) }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, mock_app).await.unwrap();
    });

    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    std::env::set_var("PRELOOP_GITHUB_API_URL", format!("http://127.0.0.1:{port}"));
    std::env::set_var("PRELOOP_GITHUB_TOKEN", "sync-test-token");

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
            "repository": "owner/repo",
            "git_ref": "refs/heads/feat/x",
            "sha": BASE_SHA,
            "push_tree": TREE,
            "push": {"create_pr": true, "draft_pr": true, "dirty": true},
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        assert_eq!(
            run.job_check_run_ids.len(),
            0,
            "dirty pushes get no submit-time check runs (the head is unknown)"
        );
        assert_eq!(run.push_state.as_ref().unwrap().status, PushStatus::Pending);
    }
    {
        let mut inner = state.inner.lock().await;
        let run = inner
            .runs
            .get_mut(&run_id.parse::<RunId>().unwrap())
            .unwrap();
        run.conclusion = Some("success".to_owned());
        run.jobs
            .insert(JobId("build".to_owned()), ExecutionStatus::Success);
    }

    let (status, body) = request_json_status(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/push"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "dirty sync must succeed: {body}");
    assert_eq!(pr_creates.load(Ordering::SeqCst), 1, "PR created");
    let check = check_creates
        .lock()
        .first()
        .cloned()
        .expect("queued check run");
    assert_eq!(
        check["head_sha"], MATERIALIZED,
        "checks attach to the materialized head commit, not the base"
    );
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id.parse::<RunId>().unwrap()).unwrap();
        let push_state = run.push_state.as_ref().unwrap();
        assert_eq!(push_state.status, PushStatus::Synced);
        assert_eq!(
            push_state.effective_sha.as_deref(),
            Some(MATERIALIZED),
            "the published commit is recorded for webhook dedup"
        );
    }

    std::env::remove_var("PRELOOP_GITHUB_TOKEN");
    std::env::remove_var("PRELOOP_GITHUB_API_URL");
}

#[tokio::test]
async fn broker_hybrid_poll_rejects_a_foreign_live_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let runner_a = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-a", "version": "2.335.1"}),
    )
    .await;
    let runner_b = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-b", "version": "2.335.1"}),
    )
    .await;
    let runner_a_id = runner_a["id"].as_i64().unwrap();
    let runner_b_id = runner_b["id"].as_i64().unwrap();
    let token_a = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_a_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    let token_b = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_b_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": runner_a_id, "name": "runner-a"},
            "ownerName": "runner-a",
            "useFipsEncryption": false,
        }),
        &token_a,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let before = {
        let inner = state.inner.lock().await;
        (inner.queue.len(), inner.session_active_requests.clone())
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {token_b}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let after = {
        let inner = state.inner.lock().await;
        (inner.queue.len(), inner.session_active_requests.clone())
    };
    assert_eq!(
        after, before,
        "a foreign poll must not consume queue work or bind an active request"
    );
}

#[tokio::test]
async fn purged_runner_listen_token_cannot_open_a_broker_session() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let runner = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/v1/Agent/1/0",
        json!({"name": "runner-revocation", "version": "2.335.1"}),
    )
    .await;
    let runner_id = runner["id"].as_i64().unwrap();
    let token = state
        .local_jwt(json!({
            "sub": format!("preloop-runner-listen-{runner_id}"),
            "scp": "ActionsRuntime.RunnerListen",
        }))
        .unwrap();
    request_json(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_id}"),
        Value::Null,
    )
    .await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/session")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn reporting_rejects_a_runtime_token_for_a_different_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let workflow = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo report\n";
    for _ in 0..2 {
        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "repository": "owner/repo",
            }),
        )
        .await;
    }
    let mut requests: Vec<_> = state
        .inner
        .lock()
        .await
        .job_requests
        .values()
        .cloned()
        .collect();
    requests.sort_by_key(|request| request.request_id);
    assert!(requests.len() >= 2);
    let target = &requests[1];
    let foreign_token = state.mint_runtime_token(&requests[0].plan_id, &requests[0].agent_job_id);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!(
                    "/runner/server/_apis/v1/Timeline/scope/hub/{}/{}",
                    target.plan_id, target.timeline_id
                ))
                .header(header::AUTHORIZATION, format!("Bearer {foreign_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"count":0,"value":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn flow_recording_redacts_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let flow_path = temp.path().join("flows.ndjson");
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&flow_path)
        .unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let system_token = state.system_token.clone();
    state.inner.lock().await.flows_file = Some(file);
    let app = app(state, CancellationToken::new());
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/healthz")
                .header(header::AUTHORIZATION, format!("Bearer {system_token}"))
                .header("x-preloop-provision-token", "provision-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "status: {}",
        response.status()
    );
    let flow = fs::read_to_string(flow_path).unwrap();
    assert!(!flow.contains(&system_token));
    assert!(!flow.contains("system-secret"));
    assert!(!flow.contains("provision-secret"));
    assert!(flow.matches("[REDACTED]").count() >= 2);
}

// ─── R1-2: /twirp-blob/:kind/:token authentication & path validation ───

// ─── R1-2: /twirp-blob/:kind/:token authentication & path validation ───

#[tokio::test]
async fn r1_2_blob_rejects_unregistered_token() {
    // The finding's repro: an unauthenticated PUT to an arbitrary token must
    // not create a blob. The gate returns 404 (not 401) so unregistered
    // tokens are indistinguishable from missing blobs.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let put = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/twirp-blob/artifact/attacker-chosen-token")
                .body(Body::from(vec![b'x'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn r1_2_blob_rejects_wrong_job_write() {
    // A job's bearer must not write another job's blob token.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let owner_job = uuid::Uuid::new_v4();
    let other_job = uuid::Uuid::new_v4();
    let other_token = state.mint_runtime_token("plan-blob", &other_job);
    // Both jobs are live so the ownership mismatch is the only rejection.
    r1_10_register_live_job(&state, owner_job, "plan-blob").await;
    r1_10_register_live_job(&state, other_job, "plan-blob").await;
    let (blob_jwt, jti) = mint_blob_jwt(&state, "artifact", &owner_job.to_string());
    {
        let mut inner = state.inner.lock().await;
        inner.artifact_v2_pending.insert(
            jti,
            crate::models::ArtifactV2Pending {
                registry_key: format!("run/{owner_job}/owned"),
                job_backend_id: owner_job.to_string(),
                created_unix: 0,
            },
        );
    }

    let put = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("/twirp-blob/artifact/{blob_jwt}"))
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {other_token}"),
                )
                .body(Body::from(vec![b'x'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn r1_2_bearerless_put_requires_live_owner() {
    // The bearerless Azure-SDK flow: a minted upload URL works while the
    // owning job is live and stops the moment the job settles — the stale
    // replay window R1-10 closes everywhere else.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let job_id = uuid::Uuid::new_v4();
    r1_10_register_live_job(&state, job_id, "plan-blob").await;
    let (blob_jwt, _jti) = mint_blob_jwt(&state, "artifact", &job_id.to_string());
    let uri = format!("/twirp-blob/artifact/{blob_jwt}");

    // Live owner: bearerless PUT succeeds.
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&uri)
                .body(Body::from(vec![b'x'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::CREATED);

    // Settled owner: the same URL is rejected before touching the disk.
    r1_10_complete_job(&state, job_id).await;
    let stale = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&uri)
                .body(Body::from(vec![b'y'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn r1_2_bearerless_put_rejects_unsigned_token() {
    // A bearerless PUT to a token that is not a server-signed blob JWT is
    // indistinguishable from a missing blob.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let put = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/twirp-blob/cache/not-a-jwt-token")
                .body(Body::from(vec![b'x'; 16]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn r1_2_blob_rejects_path_traversal() {
    // Raw and percent-encoded separators / traversal must be rejected with
    // 400 before touching the filesystem.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    for uri in [
        "/twirp-blob/artifact/..%2F..%2Fsecret",
        "/twirp-blob/artifact/%2e%2e%2fsecret",
        "/twirp-blob/evil-kind/some-token",
    ] {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(uri)
                    .body(Body::from(vec![b'x'; 8]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "uri: {uri}");
    }
    // An empty token doesn't match the route's :token segment at all, so the
    // router 404s before the gate runs — also safe.
    let res = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/twirp-blob/artifact/")
                .body(Body::from(vec![b'x'; 8]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[test]
fn r1_2_parse_blob_path_allowlist() {
    use crate::blob_store::{is_valid_blob_token, parse_blob_path};

    // Valid paths parse.
    assert_eq!(
        parse_blob_path("/twirp-blob/artifact/abc123"),
        Some(("artifact".to_owned(), "abc123".to_owned()))
    );
    assert_eq!(
        parse_blob_path("/twirp-blob/cache/deadbeef-1234"),
        Some(("cache".to_owned(), "deadbeef-1234".to_owned()))
    );
    // Artifact .zip suffix is accepted and stripped.
    assert_eq!(
        parse_blob_path("/twirp-blob/artifact/abc123.zip"),
        Some(("artifact".to_owned(), "abc123".to_owned()))
    );

    // Unknown kinds, traversal, and malformed tokens are rejected.
    assert_eq!(parse_blob_path("/twirp-blob/evil/abc123"), None);
    assert_eq!(parse_blob_path("/twirp-blob/artifact/..%2Fsecret"), None);
    assert_eq!(
        parse_blob_path("/twirp-blob/artifact/%2e%2e%2fsecret"),
        None
    );
    assert_eq!(parse_blob_path("/twirp-blob/artifact/"), None);
    assert_eq!(parse_blob_path("/twirp-blob/artifact"), None);

    // Token charset: alphanumerics, dash, underscore, dot only.
    assert!(is_valid_blob_token("abcXYZ-123_.9"));
    assert!(!is_valid_blob_token(""));
    assert!(!is_valid_blob_token("has space"));
    assert!(!is_valid_blob_token("has/slash"));
    assert!(!is_valid_blob_token("../traversal"));
}

// ─── R1-10: job token lifecycle (jti uniqueness + liveness enforcement) ───

// ─── R1-10: job token lifecycle (jti uniqueness + liveness enforcement) ───

#[test]
fn r1_10_local_jwts_have_unique_jti() {
    // Two tokens minted in the same second must not be byte-identical: the
    // random jti ensures each minted token is unique.
    let temp = tempfile::tempdir().unwrap();
    // AppState::new is async; use a runtime for this sync test.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let state = rt
        .block_on(AppState::new(temp.path().to_path_buf()))
        .unwrap();
    let job_id = uuid::Uuid::new_v4();
    let t1 = state.mint_runtime_token("plan-jti", &job_id);
    let t2 = state.mint_runtime_token("plan-jti", &job_id);
    assert_ne!(
        t1, t2,
        "tokens minted in the same second must differ via jti"
    );
}

// Helper: register a minimal live job record so R1-10 liveness checks pass.

#[tokio::test]
async fn r1_10_legacy_cache_rejects_stale_job_token() {
    // Repro: a job token minted at job start keeps working on the legacy
    // cache write path after the job completes. Reserve + upload while live,
    // complete the job, then every further write with the stale token must
    // be rejected.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-cache", &job_id);
    r1_10_register_live_job_with_run(&state, job_id, "plan-cache").await;
    let bearer = format!("Bearer {token}");

    // Live: reserve succeeds.
    let reserve = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/artifactcache/cache")
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"key": "k", "version": "v1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reserve.status(), StatusCode::OK);
    let body = to_bytes(reserve.into_body(), usize::MAX).await.unwrap();
    let cache_id: i64 = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["cacheId"]
        .as_i64()
        .unwrap();

    // Live: upload succeeds.
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, bearer.clone())
                .body(Body::from("bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    // The job completes; the token is now stale but still cryptographically
    // valid (signature + expiry pass).
    r1_10_complete_job(&state, job_id).await;

    // Stale: new reservations are rejected.
    let stale_reserve = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/artifactcache/cache")
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"key": "k2", "version": "v1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale_reserve.status(), StatusCode::FORBIDDEN);

    // Stale: uploading to the still-open reservation is rejected.
    let stale_upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, bearer.clone())
                .body(Body::from("more-bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale_upload.status(), StatusCode::FORBIDDEN);

    // Stale: committing the reservation is rejected.
    let stale_commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::json!({"size": 5}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale_commit.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn r1_10_legacy_cache_allows_live_job_writes() {
    // The liveness gate must not break the normal flow: a live job can
    // reserve, upload, and commit through the legacy cache path.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-cache", &job_id);
    r1_10_register_live_job_with_run(&state, job_id, "plan-cache").await;
    let bearer = format!("Bearer {token}");

    let reserve = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/_apis/artifactcache/cache")
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"key": "k", "version": "v1"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reserve.status(), StatusCode::OK);
    let body = to_bytes(reserve.into_body(), usize::MAX).await.unwrap();
    let cache_id: i64 = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["cacheId"]
        .as_i64()
        .unwrap();

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, bearer.clone())
                .body(Body::from("bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::ACCEPTED);

    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(serde_json::json!({"size": 5}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::OK);
}

#[tokio::test]
async fn r1_10_legacy_artifact_create_rejects_stale_job_token() {
    // Repro: the legacy artifact-create route accepts any valid local JWT
    // and never checked job liveness, so a stale job token could keep
    // creating artifacts after its job completed.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let job_id = uuid::Uuid::new_v4();
    let token = state.mint_runtime_token("plan-artifact", &job_id);
    r1_10_register_live_job_with_run(&state, job_id, "plan-artifact").await;
    let bearer = format!("Bearer {token}");
    let run_id = uuid::Uuid::new_v4();

    // Live: artifact creation succeeds.
    let live = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/pipelines/workflows/{run_id}/artifacts"))
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"name": "a", "file_name": "a.bin"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(live.status(), StatusCode::OK);

    // The job completes; the token is now stale.
    r1_10_complete_job(&state, job_id).await;

    // Stale: artifact creation is rejected.
    let stale = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/_apis/pipelines/workflows/{run_id}/artifacts"))
                .header(header::AUTHORIZATION, bearer.clone())
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"name": "b", "file_name": "b.bin"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::FORBIDDEN);
}
