//! preloop-runner-server integration tests — broker group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

/// A worker that stops renewing while its session keeps polling is hung, not
/// disconnected: the live session proves the guest is reachable, so the stale
/// lease can only mean the renew task died. The reaper must fail the job on
/// the worker's own cadence (HUNG_WORKER_LEASE_SECONDS) rather than holding
/// the runner slot for the full JOB_LEASE_SECONDS disconnect timeout.
#[tokio::test]
async fn hung_worker_reaped_while_session_still_polls() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

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

    // Poll to claim the job; this marks the session live and sets the lease.
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

    // Worker stops renewing (lease stale past the hung threshold) but the
    // session stays fresh — the listener is alive, the worker is not.
    {
        let mut inner = state.inner.lock().await;
        inner
            .session_last_seen
            .insert("default".to_owned(), std::time::Instant::now());
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(HUNG_WORKER_LEASE_SECONDS + 1));
    }

    reap_once(&shared).await;

    let inner = state.inner.lock().await;
    let request = inner.job_requests.get(&request_id).unwrap();
    assert_eq!(
        request.result,
        Some(ExecutionStatus::Failure),
        "a hung worker must be reaped on its own cadence, not the full lease"
    );
    assert!(inner.session_active_requests.is_empty());
    assert_eq!(
        inner.runs.get(&run_id).unwrap().status,
        ExecutionStatus::Failure
    );
}

/// The mirror image: a stale lease with a *dead* session is a disconnect, not
/// a hung worker. It must wait out the full JOB_LEASE_SECONDS boundary — the
/// guest may be partitioned and could still come back.

/// The mirror image: a stale lease with a *dead* session is a disconnect, not
/// a hung worker. It must wait out the full JOB_LEASE_SECONDS boundary — the
/// guest may be partitioned and could still come back.
#[tokio::test]
async fn dead_session_stale_lease_waits_for_full_lease() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let shutdown = CancellationToken::new();
    let app = app(state.clone(), shutdown.clone());
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown,
    });

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
    let _run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

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

    // Lease stale past the hung threshold but the session is dead — this is a
    // disconnect, so the job must NOT be reaped at the hung-worker cadence.
    {
        let mut inner = state.inner.lock().await;
        let stale_seen =
            std::time::Instant::now() - inner.runner_liveness_timeout - Duration::from_secs(1);
        inner
            .session_last_seen
            .insert("default".to_owned(), stale_seen);
        let request = inner.job_requests.get_mut(&request_id).unwrap();
        request.last_renewed_at =
            Some(SystemTime::now() - Duration::from_secs(HUNG_WORKER_LEASE_SECONDS + 1));
    }

    reap_once(&shared).await;

    let inner = state.inner.lock().await;
    let request = inner.job_requests.get(&request_id).unwrap();
    assert_eq!(
        request.result, None,
        "a dead session is a disconnect; it must wait out the full lease"
    );
}

#[tokio::test]
async fn github_webhook_flows_with_signature_and_check_runs() {
    // This test asserts the mock check-run path (`check_run_id > 0` with no
    // GitHub server involved). A co-scheduled test's `PRELOOP_GITHUB_TOKEN`
    // would flip it to a live API call that fails, leaving zero check runs.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

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

    let event_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

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
        "after": event_sha.clone(),
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        },
        "commits": [
            {
                "id": event_sha,
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
    assert_eq!(response_200.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    assert_eq!(
        state.queue_depth.load(std::sync::atomic::Ordering::Acquire),
        1
    );
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
async fn github_webhook_fetches_workflow_from_event_sha_not_current_branch() {
    // This is a local-workspace reproduction of the same race as the remote
    // App path: the webhook names an older commit while the branch has already
    // advanced to a different workflow definition.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();

    let old_workflow = r#"
name: old
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo old-workflow
"#;
    let new_workflow = r#"
name: new
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo new-workflow
"#;
    std::fs::write(ws_dir.join(".github/workflows/build.yml"), old_workflow).unwrap();

    let git = |args: &[&str]| -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(&ws_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let event_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

    std::fs::write(ws_dir.join(".github/workflows/build.yml"), new_workflow).unwrap();
    git(&["add", ".github/workflows/build.yml"]);
    git(&["commit", "-m", "new workflow"]);
    let branch_sha = git(&["rev-parse", "HEAD"]);
    assert_ne!(event_sha, branch_sha);

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir);
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": event_sha,
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        },
        "commits": [{
            "id": event_sha,
            "added": [".github/workflows/build.yml"],
            "modified": [],
            "removed": []
        }]
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "sha-pinned-workflow")
                .header("x-hub-signature-256", format!("sha256={signature}"))
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();
    let inner = state.inner.lock().await;
    let run = inner.runs.values().next().expect("webhook created a run");
    assert_eq!(
        run.submission.resolved_sha.as_deref(),
        Some(event_sha.as_str())
    );
    assert!(
        run.submission.workflow_yaml.contains("old-workflow"),
        "workflow must be loaded from the webhook commit, not current main"
    );
    assert!(!run.submission.workflow_yaml.contains("new-workflow"));
}

#[tokio::test]
async fn github_webhook_rejects_missing_event_sha_without_workspace_fallback() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
    fs::write(
        ws_dir.join(".github/workflows/build.yml"),
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo current-worktree
"#,
    )
    .unwrap();

    commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

    let missing_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir);
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": missing_sha,
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        },
        "commits": [{
            "id": missing_sha,
            "added": [".github/workflows/build.yml"],
            "modified": [],
            "removed": []
        }]
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "missing-event-sha")
                .header("x-hub-signature-256", format!("sha256={signature}"))
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();
    let inner = state.inner.lock().await;
    assert!(
        inner.runs.is_empty(),
        "missing event SHA must not execute current-worktree YAML"
    );
}

#[tokio::test]
async fn github_webhook_rejects_pull_request_target_without_base_sha() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
    fs::write(
        ws_dir.join(".github/workflows/build.yml"),
        r#"
on: pull_request_target
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo base-workflow
"#,
    )
    .unwrap();

    commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir);
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "number": 42,
            "base": { "ref": "main" },
            "head": {
                "ref": "feature/fork",
                "sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "repo": { "fork": true }
            }
        },
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();

    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "pull_request_target")
                .header("x-github-delivery", "missing-base-sha")
                .header("x-hub-signature-256", format!("sha256={signature}"))
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();
    let inner = state.inner.lock().await;
    assert!(
        inner.runs.is_empty(),
        "pull_request_target must not execute head-controlled YAML"
    );
}

/// Check-run ids must survive a restart even when no job status event ever
/// fired — a long queue can sit between check-run creation and the job's
/// first status event, and a deploy in that window used to restore the run
/// with an empty mapping, orphaning the GitHub check in "queued" forever.

/// Check-run ids must survive a restart even when no job status event ever
/// fired — a long queue can sit between check-run creation and the job's
/// first status event, and a deploy in that window used to restore the run
/// with an empty mapping, orphaning the GitHub check in "queued" forever.
#[tokio::test]
async fn check_run_ids_survive_a_restart_before_any_job_event() {
    // Held for the whole test: the GitHub env vars are process-global, and a
    // parallel test's token would flip the check-run path from mock to a real
    // GitHub API call. The mock path is the contract under test.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _no_api_url = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

    let temp = tempfile::tempdir().unwrap();
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

    let event_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);

    let run_id = {
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());
        let app = app(state.clone(), CancellationToken::new());

        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": event_sha.clone(),
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "commits": [
                {
                    "id": event_sha,
                    "added": ["src/main.rs"],
                    "modified": [],
                    "removed": []
                }
            ]
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
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

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/github/webhooks")
                    .header("x-github-event", "push")
                    .header("x-hub-signature-256", format!("sha256={sig_hex}"))
                    .header("content-type", "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        crate::github::drain_webhook_queue(&shared).await.unwrap();

        let inner = state.inner.lock().await;
        let (run_id, run) = inner.runs.iter().next().expect("webhook created a run");
        assert_eq!(
            run.job_check_run_ids.len(),
            1,
            "mock check run id recorded at submission"
        );
        *run_id
    };

    // Restart with no job event in between: the mapping must come back.
    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let inner = recovered.inner.lock().await;
    let run = inner.runs.get(&run_id).expect("run must survive restart");
    assert_eq!(
        run.job_check_run_ids.len(),
        1,
        "check run id must survive a restart before the job's first status event"
    );
}

#[tokio::test]
async fn github_check_run_rerequest_resubmits_the_owning_run() {
    let temp = tempfile::tempdir().unwrap();
    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
            "event": "push",
            "repository": "owner/repo",
            "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
        }),
    )
    .await;
    let original_run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let original_check_run_id = 1234;
    {
        let mut inner = state.inner.lock().await;
        let run = inner.runs.get_mut(&original_run_id).unwrap();
        run.jobs
            .insert(JobId("build".to_owned()), ExecutionStatus::Failure);
        run.status = ExecutionStatus::Failure;
        run.conclusion = Some("failure".to_owned());
        run.job_check_run_ids
            .insert(JobId("build".to_owned()), original_check_run_id);
    }

    let payload = serde_json::json!({
        "action": "rerequested",
        "repository": {"full_name": "owner/repo"},
        "check_run": {
            "id": original_check_run_id,
            "name": "build"
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = format!(
        "sha256={}",
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "check_run")
                .header("x-github-delivery", "rerun-delivery")
                .header("x-hub-signature-256", signature)
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();

    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 2);
    let rerun = inner
        .runs
        .values()
        .find(|run| run.run_id != original_run_id)
        .expect("rerequest should create a new run");
    assert_eq!(rerun.status, ExecutionStatus::Queued);
    assert_eq!(
        rerun.job_check_run_ids.get(&JobId("build".to_owned())),
        Some(&original_check_run_id),
        "the rerequest must continue reporting through the requested check run"
    );
}

/// Scaffolding shared by the webhook delivery dedup tests: a workspace holding
/// one push-triggered workflow, a server with a webhook secret, and the signed
/// push payload GitHub would deliver.
#[tokio::test]
async fn github_webhook_same_delivery_is_deduped_but_new_delivery_creates_run() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // The same delivery id twice (GitHub redelivery / double-fire) must not
    // create a duplicate run; a genuinely new delivery creates another.
    assert_eq!(
        fixture.post("delivery-dup-1", Some("push")).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        fixture.post("delivery-dup-1", Some("push")).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        fixture.post("delivery-dup-2", Some("push")).await,
        StatusCode::ACCEPTED
    );
    fixture.drain().await;
    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        2,
        "a redelivered webhook must not create a duplicate run"
    );
}

#[tokio::test]
async fn github_webhook_missing_event_does_not_poison_delivery_id() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // The first request is rejected before durable enqueue because its event
    // header is missing.
    assert_eq!(
        fixture.post("delivery-retry", None).await,
        StatusCode::BAD_REQUEST
    );
    {
        let inner = fixture.state.inner.lock().await;
        assert!(
            inner.runs.is_empty(),
            "a rejected request must not create a run"
        );
    }

    // A later valid request with the same delivery id must still be accepted
    // and processed rather than being mistaken for an active duplicate.
    assert_eq!(
        fixture.post("delivery-retry", Some("push")).await,
        StatusCode::ACCEPTED
    );
    fixture.drain().await;
    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        1,
        "a retry after pre-enqueue rejection must create the run"
    );
}

#[tokio::test]
async fn github_webhook_concurrent_duplicate_delivery_creates_one_run() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    // Two copies of one delivery in flight at once: the in-flight reservation
    // makes the loser a no-op instead of a second run.
    let (first, second) = tokio::join!(
        fixture.post("delivery-concurrent", Some("push")),
        fixture.post("delivery-concurrent", Some("push"))
    );
    assert_eq!(first, StatusCode::ACCEPTED);
    assert_eq!(second, StatusCode::ACCEPTED);
    fixture.drain().await;
    let inner = fixture.state.inner.lock().await;
    assert_eq!(
        inner.runs.len(),
        1,
        "concurrent copies of one delivery must produce exactly one run"
    );
}

#[tokio::test]
async fn github_webhook_dedup_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;

    assert_eq!(
        fixture.post("delivery-restart", Some("push")).await,
        StatusCode::ACCEPTED
    );
    fixture.drain().await;
    let original_run_id = {
        let inner = fixture.state.inner.lock().await;
        assert_eq!(inner.runs.len(), 1);
        *inner.runs.keys().next().unwrap()
    };

    // Restart the server: new state instance on the same persisted database.
    let mut restarted_state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    restarted_state.webhook_secret = Some("super-secret".to_owned());
    restarted_state.local_workspace = Some(temp.path().join("ws"));
    let restarted_app = app(restarted_state.clone(), CancellationToken::new());

    // Redelivery arriving after restart must be deduplicated by the table.
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/github/webhooks")
        .header("x-github-delivery", "delivery-restart")
        .header("x-hub-signature-256", &fixture.signature_header)
        .header("x-github-event", "push")
        .header("content-type", "application/json");
    let response = restarted_app
        .oneshot(
            request
                .body(Body::from(fixture.payload_bytes.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response_body =
        serde_json::from_slice::<Value>(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(
        response_body["status"], "duplicate",
        "restart redelivery should report the retained delivery as duplicate"
    );

    let shared = Arc::new(SharedState {
        state: restarted_state.clone(),
        shutdown: CancellationToken::new(),
    });
    let claimed = restarted_state
        .store
        .claim_webhook_deliveries(1, 60)
        .await
        .unwrap();
    assert!(
        claimed.is_empty(),
        "a completed delivery must not be claimed after restart"
    );
    crate::github::drain_webhook_queue(&shared).await.unwrap();

    let inner = restarted_state.inner.lock().await;
    assert!(
        inner.runs.contains_key(&original_run_id),
        "restart redelivery must preserve the original run identity"
    );
    assert_eq!(
        inner.runs.len(),
        1,
        "a redelivery arriving after restart must be deduped rather than creating a second run"
    );
}

#[tokio::test]
async fn github_webhook_run_reservation_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;
    let payload: Value = serde_json::from_slice(&fixture.payload_bytes).unwrap();
    let event_sha = payload["after"].as_str().unwrap().to_owned();
    let workflow_yaml =
        fs::read_to_string(temp.path().join("ws/.github/workflows/build.yml")).unwrap();
    let signature_header = fixture.signature_header.clone();
    let payload_bytes = fixture.payload_bytes.clone();
    let shared = Arc::new(SharedState {
        state: fixture.state.clone(),
        shutdown: CancellationToken::new(),
    });

    assert_eq!(
        fixture
            .post("delivery-reservation-restart", Some("push"))
            .await,
        StatusCode::ACCEPTED
    );
    let accepted = crate::runs::submit_run_inner_with_webhook_delivery(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml,
            event: "push".to_owned(),
            payload,
            repository: "owner/repo".to_owned(),
            git_ref: "refs/heads/main".to_owned(),
            workflow_path: Some(".github/workflows/build.yml".to_owned()),
            workflow_file: Some("build.yml".to_owned()),
            sha: event_sha.clone(),
            resolved_sha: Some(event_sha),
            changed_paths: vec!["src/main.rs".to_owned()],
            changed_paths_known: true,
            ..Default::default()
        },
        Some("delivery-reservation-restart"),
    )
    .await
    .unwrap();
    let original_run_id = accepted.run_id;
    let claimed = fixture
        .state
        .store
        .claim_webhook_deliveries(1, 0)
        .await
        .unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "the simulated crash must leave the delivery in processing"
    );
    drop(shared);
    drop(fixture);

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let mut restarted_state = AppState::new(temp.path().join("state").to_path_buf())
        .await
        .unwrap();
    restarted_state.webhook_secret = Some("super-secret".to_owned());
    restarted_state.local_workspace = Some(temp.path().join("ws"));
    assert_eq!(
        restarted_state
            .store
            .recover_webhook_deliveries()
            .await
            .unwrap(),
        1,
        "restart must release the uncompleted delivery lease"
    );
    let restarted_app = app(restarted_state.clone(), CancellationToken::new());
    let response = restarted_app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-delivery", "delivery-reservation-restart")
                .header("x-hub-signature-256", signature_header)
                .header("x-github-event", "push")
                .header("content-type", "application/json")
                .body(Body::from(payload_bytes))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let response_body =
        serde_json::from_slice::<Value>(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(response_body["status"], "duplicate");

    let restarted_shared = Arc::new(SharedState {
        state: restarted_state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&restarted_shared)
        .await
        .unwrap();
    let inner = restarted_state.inner.lock().await;
    assert!(
        inner.runs.contains_key(&original_run_id),
        "replayed processing must reuse the run restored from the reservation"
    );
    assert_eq!(
        inner.runs.len(),
        1,
        "a replay after restart must not create a second run"
    );
}

#[tokio::test]
async fn github_webhook_pull_request_event() {
    // The webhook path reads `PRELOOP_GITHUB_TOKEN` / `PRELOOP_GITHUB_API_URL`
    // live for check-run reporting. Hold the env lock so a concurrent
    // env-mutating test cannot point those at a foreign credential or stub
    // and turn this 200 into a 502.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
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

    let git = |args: &[&str]| -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(&ws_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let base_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/test.yml"]);
    git(&["commit", "--allow-empty", "-m", "head commit"]);
    let head_sha = git(&["rev-parse", "HEAD"]);

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
                "sha": head_sha
            },
            "base": {
                "ref": "main",
                "sha": base_sha.clone()
            },
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
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    crate::github::drain_webhook_queue(&shared).await.unwrap();
    // Verify triggered run
    let inner = state.inner.lock().await;
    assert_eq!(inner.runs.len(), 1);
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(run_record.submission.event, "pull_request");
    assert_eq!(run_record.submission.git_ref, "refs/pull/42/head");
    assert_eq!(run_record.job_check_run_ids.len(), 1);
    // A pull_request payload has no `after`; the head sha must still reach
    // the job. Falling through to all-zeros makes every checkout ask the
    // server for `0000…` and fail as "not our ref".
    assert_eq!(
        run_record.head_sha, head_sha,
        "pull_request head sha must drive github.sha"
    );
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
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    std::env::set_var(
        "PRELOOP_GITHUB_API_URL",
        format!("http://127.0.0.1:{}", port),
    );

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
    assert!(html.contains("preloop-local-app"));

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
    std::env::remove_var("PRELOOP_GITHUB_API_URL");
}

#[tokio::test]
async fn runner_oauth2_token_client_assertion_verification() {
    use preloop_gha_protocol::crypto::{sign_jwt_ps256, sign_jwt_rs256};
    use serde_json::Value;

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // 1. Generate RSA keypair for the runner using the protocol's library
    let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
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
        // R1-9: aud must identify this server (the called token endpoint).
        "aud": "http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token",
        "jti": uuid::Uuid::new_v4().to_string(),
        "nbf": now,
        "exp": now + 300,
    });

    let client_assertion = sign_jwt_ps256(&header, &claims, &rsa_params).unwrap();

    // 4. Request OAuth token using urlencoded body
    let form_body = serde_urlencoded::to_string([
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

    // 4b. Test RS256 algorithm verification
    let rs256_header = json!({
        "typ": "JWT",
        "alg": "RS256"
    });
    let rs256_client_assertion = sign_jwt_rs256(&rs256_header, &claims, &rsa_params).unwrap();
    let rs256_form_body = serde_urlencoded::to_string([
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", &rs256_client_assertion),
        ("grant_type", "client_credentials"),
    ])
    .unwrap();

    let rs256_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/runner/server/_apis/v1/oauth2/token")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(rs256_form_body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rs256_response.status(), StatusCode::OK);
    let rs256_bytes = axum::body::to_bytes(rs256_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let rs256_token_resp: Value = serde_json::from_slice(&rs256_bytes).unwrap();
    assert!(rs256_token_resp["access_token"].is_string());

    // 5. Test negative case: Invalid signature (wrong key)
    let wrong_keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let wrong_rsa_params = wrong_keypair.to_rsaparams();
    let bad_assertion = sign_jwt_ps256(&header, &claims, &wrong_rsa_params).unwrap();

    let bad_form_body = serde_urlencoded::to_string([
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

// ─── R1-9: client_assertion expiry / audience validation ───

#[test]
fn r1_9_accepts_valid_assertion_claims() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = r1_9_test_uri();
    // Exact endpoint URL.
    let claims = r1_9_claims(
        now,
        serde_json::json!("http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token"),
    );
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
    // Base URL alone is also accepted.
    let claims = r1_9_claims(now, serde_json::json!("http://127.0.0.1:9090"));
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
    // Array form.
    let claims = r1_9_claims(
        now,
        serde_json::json!(["https://other.example", "http://127.0.0.1:9090"]),
    );
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
}

#[test]
fn r1_9_rejects_expired_assertion() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = r1_9_test_uri();
    let claims = serde_json::json!({
        "sub": "test-client",
        "aud": "http://127.0.0.1:9090",
        "nbf": now - 600,
        "exp": now - 1,
    });
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
}

#[test]
fn r1_9_rejects_wrong_audience() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = r1_9_test_uri();
    // Assertion addressed to a different server must not validate here.
    let claims = r1_9_claims(now, serde_json::json!("https://preloop.local/oauth"));
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
    // Missing aud is also rejected.
    let claims = serde_json::json!({"sub": "test-client", "nbf": now, "exp": now + 300});
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
}

#[test]
fn r1_9_rejects_missing_exp_and_excessive_lifetime() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = r1_9_test_uri();
    // Missing exp.
    let claims = serde_json::json!({
        "sub": "test-client",
        "aud": "http://127.0.0.1:9090",
        "nbf": now,
    });
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
    // Lifetime over the 600s cap.
    let claims = serde_json::json!({
        "sub": "test-client",
        "aud": "http://127.0.0.1:9090",
        "nbf": now,
        "exp": now + 3600,
    });
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
    // Future-dated nbf beyond skew.
    let claims = serde_json::json!({
        "sub": "test-client",
        "aud": "http://127.0.0.1:9090",
        "nbf": now + 3600,
        "exp": now + 3900,
    });
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
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

/// A hosted image label names an OS, and a self-hosted runner may only stand
/// in for one it actually runs. A macOS host claiming `ubuntu-latest` fails
/// the job deep inside a step (Linux-only crate features, `/home/runner`
/// paths, apt) instead of waiting for a Linux runner.

/// A hosted image label names an OS, and a self-hosted runner may only stand
/// in for one it actually runs. A macOS host claiming `ubuntu-latest` fails
/// the job deep inside a step (Linux-only crate features, `/home/runner`
/// paths, apt) instead of waiting for a Linux runner.
#[test]
fn label_matching_never_crosses_operating_systems() {
    let mac = [
        "self-hosted".to_owned(),
        "macOS".to_owned(),
        "ARM64".to_owned(),
    ];
    assert!(!job_matches_runner(&["ubuntu-latest".into()], &mac));
    assert!(!job_matches_runner(&["windows-latest".into()], &mac));
    assert!(job_matches_runner(&["macos-15".into()], &mac));

    let linux = [
        "self-hosted".to_owned(),
        "Linux".to_owned(),
        "X64".to_owned(),
        "ubuntu-24.04".to_owned(),
        "ubuntu-latest".to_owned(),
    ];
    // A pool advertising 24.04 still serves a 22.04 job: same OS, and the
    // alternative is a job that never runs.
    assert!(job_matches_runner(&["ubuntu-22.04".into()], &linux));
    assert!(!job_matches_runner(&["macos-14".into()], &linux));
}

/// A runner that declares no OS label has told us nothing to contradict, so
/// it stays eligible for every hosted label.

/// A runner that declares no OS label has told us nothing to contradict, so
/// it stays eligible for every hosted label.
#[test]
fn label_matching_os_less_runner_stays_eligible() {
    let unlabelled = ["self-hosted".to_owned(), "gpu".to_owned()];
    assert!(job_matches_runner(&["ubuntu-latest".into()], &unlabelled));
    assert!(job_matches_runner(&["windows-2022".into()], &unlabelled));
    assert!(!job_matches_runner(&["nvidia".into()], &unlabelled));
}

/// A 24.04 machine may stand in for an `ubuntu-22.04` job, but it must not
/// take one while a job it exactly matches is claimable: the pool is usually
/// already building the 22.04 machine that job asked for, and the stand-in
/// would hand it a different base image for no reason.

/// A 24.04 machine may stand in for an `ubuntu-22.04` job, but it must not
/// take one while a job it exactly matches is claimable: the pool is usually
/// already building the 22.04 machine that job asked for, and the stand-in
/// would hand it a different base image for no reason.
#[tokio::test]
async fn claims_prefer_a_job_the_runner_exactly_matches() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // `pinned` is first in the queue, so only the preference can reorder it.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  pinned:\n    runs-on: ubuntu-22.04\n    steps:\n      - run: echo pinned\n  wide:\n    runs-on: self-hosted\n    steps:\n      - run: echo wide\n",
            "event": "push",
            "repository": "owner/repo"
        }),
    )
    .await;
    assert!(
        accepted["run_id"].is_string(),
        "the run was accepted: {accepted}"
    );

    let machine = RunnerCapabilities {
        known: true,
        labels: vec![
            "self-hosted".to_owned(),
            "Linux".to_owned(),
            "X64".to_owned(),
            "ubuntu-24.04".to_owned(),
            "ubuntu-latest".to_owned(),
        ],
        runner_group_id: None,
        runner_group_name: None,
    };

    let mut inner = state.inner.lock().await;
    let first = crate::runtime_scheduling::take_matching_job(&mut inner, &machine, Some(1))
        .expect("a claimable job");
    assert_eq!(
        first.job_id.0, "wide",
        "the exact `self-hosted` match must win over the 22.04 stand-in"
    );

    // And the stand-in still happens rather than starving the pinned job.
    let second = crate::runtime_scheduling::take_matching_job(&mut inner, &machine, Some(1))
        .expect("the pinned job is still claimable");
    assert_eq!(second.job_id.0, "pinned");
}

/// A job for a platform with no runner host can never be claimed. Queuing it
/// forever means a run that never finishes and a check that never reports, so
/// it is skipped — but only when nothing is registered that could serve it.

/// A job for a platform with no runner host can never be claimed. Queuing it
/// forever means a run that never finishes and a check that never reports, so
/// it is skipped — but only when nothing is registered that could serve it.
#[test]
fn jobs_are_skipped_only_for_platforms_nothing_can_host() {
    let linux_pool = || ["linux", "linux"].into_iter();

    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["windows-latest".to_owned()],
            linux_pool()
        ),
        Some("windows")
    );
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(&["macos-15".to_owned()], linux_pool()),
        Some("macos")
    );

    // A registered Mac host makes macOS a supported deployment, not a gap.
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["macos-latest".to_owned()],
            ["linux", "macos"].into_iter()
        ),
        None
    );

    // Linux is never skipped: the pool provisions it on demand, and an
    // ephemeral pool is routinely between runners.
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["ubuntu-22.04".to_owned()],
            std::iter::empty()
        ),
        None
    );
    assert_eq!(
        crate::runtime_scheduling::unhostable_platform(
            &["self-hosted".to_owned(), "gpu".to_owned()],
            std::iter::empty()
        ),
        None
    );
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
// production-path checks: YAML is parsed and expanded by Preloop, then the
// real queue/promotion state is driven through the explicitly gated test
// completion API and compared with the documented outcome.
// ─── DAG scheduling regression tests (spec §1) ─────────────────────────

/// Production path: build fails → test with default condition is skipped.
/// Verifies the server's promote_ready_jobs correctly propagates failure.

// Oracle: GitHub `needs` and status-function contracts, with worker-side
// condition semantics pinned to actions/runner v2.335.1. These tests are
// production-path checks: YAML is parsed and expanded by Preloop, then the
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
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
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

#[tokio::test]
async fn stored_secrets_are_injected_into_native_submissions() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state
        .secrets
        .write()
        .global
        .insert("E2E_TEST_SECRET".to_owned(), "stored-value".to_owned());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $SECRET\n        env:\n          SECRET: ${{ secrets.E2E_TEST_SECRET }}\n",
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();

    // The job message must carry the stored secret as a secret variable so
    // the worker republishes it into the `secrets.*` context.
    let inner = state.inner.lock().await;
    let run = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .unwrap();
    let message = inner
        .queue
        .iter()
        .find(|job| job.run_id == run.run_id)
        .or_else(|| {
            inner
                .pending_jobs
                .iter()
                .find(|job| job.run_id == run.run_id)
        })
        .expect("queued job exists")
        .message
        .clone();
    let secret_var = message
        .variables
        .values()
        .find(|value| value.value.as_deref() == Some("stored-value"))
        .expect("stored secret present in job message variables");
    assert_eq!(secret_var.is_secret, Some(true));
}

/// Extract the queued job message for a run, wherever it currently sits.

/// `preloop setup github --via pat` stores the credential as `github.pat` and
/// configures no App. That PAT must reach jobs as their `GITHUB_TOKEN`:
/// previously only `PRELOOP_GITHUB_TOKEN` was consulted, so setup reported
/// success while every job silently ran on the local runtime token instead.
#[tokio::test]
async fn pat_only_config_supplies_job_github_token() {
    // Same env-lock discipline: `PRELOOP_GITHUB_TOKEN` writers serialize on
    // it, and a leaked value would win env-then-config and break the assert.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    // H3: keep PAT scope introspection hermetic while still letting the PAT be
    // verified: the stub reports read-only scopes, so the configured PAT is
    // embedded rather than withheld.
    let _live_api = live_pat_scope_api("read:org, read:user").await;
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"github_pat_testvalue\"\n").unwrap();
    // Point this engine at the temp config directly. Mutating `PRELOOP_CONFIG`
    // would race every other test that builds an `AppState` concurrently.
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert!(
        state.github_app.is_none(),
        "config declares no app id or pem"
    );
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();

    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, &run_id);
    let token = message
        .variables
        .get("system.github.token")
        .expect("job message carries a GitHub token variable");
    assert_eq!(token.value.as_deref(), Some("github_pat_testvalue"));
    assert_eq!(token.is_secret, Some(true));
}

/// H3: a PAT whose OAuth scopes cannot be introspected must not be embedded.
/// The job keeps the job-scoped runtime token, so a step that needs GitHub
/// fails at the point of use instead of running with authority nobody could
/// bound, and the wire variable discloses the withholding.
#[tokio::test]
async fn unverifiable_pat_scopes_withhold_the_pat_from_jobs() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let _dead_api = dead_pat_scope_api();
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    // A distinct PAT value, because the scope cache is process-global and keyed
    // by the token hash: reusing another test's PAT could read a verified answer
    // cached there and never exercise the withheld path.
    std::fs::write(
        &config_path,
        "[github]\npat = \"github_pat_unverifiable_scopes\"\n",
    )
    .unwrap();
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });

    let yaml =
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let accepted = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("unverifiable scopes withhold the PAT, they do not refuse the run");
    let run_id = accepted.run_id.to_string();

    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, &run_id);
    // Compare the token's job identity, not its bytes: local JWTs carry a
    // randomized `jti`, so two mints of identical claims never match.
    let token = variable_value(&message, "system.github.token")
        .expect("the job message carries a GitHub token variable");
    assert_eq!(
        jwt_sub(token).as_deref(),
        Some(format!("preloop-job-{}", message.job_id).as_str()),
        "an unverifiable PAT is withheld in favour of the job-scoped runtime token"
    );
    assert_ne!(
        variable_value(&message, "system.github.token"),
        Some("github_pat_unverifiable_scopes"),
        "the PAT must never reach a job whose bounds could not be verified"
    );
    let authority = variable_value(&message, "system.github.token.pat_scopes")
        .expect("the withheld state is published for the runner to print");
    assert!(
        authority.contains("withheld"),
        "the wire variable must disclose the withheld PAT, got: {authority}"
    );
}

/// H3: scope-mismatch matrix for the static-PAT permission check. A classic
/// PAT carrying write authority must never back a job whose effective
/// `permissions:` are read-only (or empty); a PAT no broader than declared
/// passes. Unknown classic scopes count as write-capable — the safe direction
/// for a security check.

/// H3: scope-mismatch matrix for the static-PAT permission check. A classic
/// PAT carrying write authority must never back a job whose effective
/// `permissions:` are read-only (or empty); a PAT no broader than declared
/// passes. Unknown classic scopes count as write-capable — the safe direction
/// for a security check.
#[test]
fn pat_exceeds_declared_scope_matrix() {
    use std::collections::BTreeMap;
    fn declared(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(scope, level)| ((*scope).to_owned(), (*level).to_owned()))
            .collect()
    }
    fn scopes(list: &[&str]) -> Vec<String> {
        list.iter().map(|scope| (*scope).to_owned()).collect()
    }
    let read_only = || declared(&[("contents", "read"), ("metadata", "read")]);

    // Full `repo` scope against read-only and empty declarations: exceeds.
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["repo"]),
        &read_only()
    ));
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["repo"]),
        &BTreeMap::new()
    ));
    // Terse classic scopes that hide write grants: exceed a read-only job.
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["public_repo", "read:org"]),
        &read_only()
    ));
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["repo:status"]),
        &read_only()
    ));
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["gist"]),
        &read_only()
    ));
    // Any PAT authority at all against `permissions: {}`: exceeds.
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["read:org"]),
        &BTreeMap::new()
    ));
    // Provably read-only scopes against a read-only job: no mismatch.
    assert!(!crate::runs::pat_exceeds_declared(
        &scopes(&["read:org", "read:user", "user:email"]),
        &read_only()
    ));
    // A read-only PAT never exceeds, even against a write-declaring job.
    assert!(!crate::runs::pat_exceeds_declared(
        &scopes(&["read:org"]),
        &declared(&[("contents", "write")])
    ));
    // Declared `write` absorbs a write-capable PAT.
    assert!(!crate::runs::pat_exceeds_declared(
        &scopes(&["repo"]),
        &declared(&[("contents", "write")])
    ));
    // `id-token`/`models` are platform-granted, not token authority: they
    // don't absorb a write PAT the way a real `write` grant does.
    assert!(crate::runs::pat_exceeds_declared(
        &scopes(&["repo"]),
        &declared(&[("id-token", "write")])
    ));
    // A scopeless PAT exceeds nothing.
    assert!(!crate::runs::pat_exceeds_declared(
        &scopes(&[]),
        &read_only()
    ));
}

/// H3 end-to-end: a static PAT whose OAuth scopes exceed the workflow's
/// declared `permissions:` refuses the run instead of silently embedding the
/// broader PAT as the job's `GITHUB_TOKEN`.

/// H3 end-to-end: a static PAT whose OAuth scopes exceed the workflow's
/// declared `permissions:` refuses the run instead of silently embedding the
/// broader PAT as the job's `GITHUB_TOKEN`.
#[tokio::test]
async fn static_pat_broader_than_declared_permissions_rejects_run() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    // Distinct PAT string per H3 test: introspected scopes are cached by PAT
    // hash process-wide, so sharing one value across tests would leak cached
    // scopes between them.
    std::fs::write(&config_path, "[github]\npat = \"h3-broad-pat\"\n").unwrap();
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert!(
        state.github_app.is_none(),
        "config declares no app id or pem"
    );

    // Hermetic scope introspection: the mock API root answers the PAT probe
    // with broad classic scopes.
    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/",
        axum::routing::get(|| async {
            (
                [("X-OAuth-Scopes", "repo, workflow")],
                axum::Json(serde_json::json!({})),
            )
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let yaml = "on: push\npermissions:\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let error = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect_err("a PAT broader than declared permissions must refuse the run");
    let message = error.message().to_owned();
    assert!(
        message.contains("refusing run") && message.contains("OAuth scopes"),
        "rejection explains itself, got: {message}"
    );
}

/// H3: a static PAT whose OAuth scopes are no broader than the workflow's
/// declared `permissions:` still reaches the job — but
/// `system.github.token.permissions` must advertise the PAT's actual scopes,
/// not the declared set the token does not honor.

/// H3: a static PAT whose OAuth scopes are no broader than the workflow's
/// declared `permissions:` still reaches the job — but
/// `system.github.token.permissions` must advertise the PAT's actual scopes,
/// not the declared set the token does not honor.
#[tokio::test]
async fn static_pat_matching_declared_permissions_keeps_honest_wire_variable() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"h3-narrow-pat\"\n").unwrap();
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert!(
        state.github_app.is_none(),
        "config declares no app id or pem"
    );

    let api_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_base = format!("http://{}", api_listener.local_addr().unwrap());
    let mock = axum::Router::new().route(
        "/",
        axum::routing::get(|| async {
            (
                [("X-OAuth-Scopes", "read:org, read:user")],
                axum::Json(serde_json::json!({})),
            )
        }),
    );
    tokio::spawn(async move {
        axum::serve(api_listener, mock).await.unwrap();
    });
    let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    // No declared permissions: the read-only default applies, which the
    // read-only PAT does not exceed.
    let yaml =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    let accepted = crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("a PAT no broader than declared permissions is accepted");
    let run_id = accepted.run_id.to_string();

    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, &run_id);
    assert_eq!(
        variable_value(&message, "system.github.token"),
        Some("h3-narrow-pat"),
        "the narrow PAT still reaches the job"
    );
    // `system.github.token.permissions` keeps its documented map shape: a
    // consumer parsing it as `{"<Permission>": "<level>"}` must not meet a
    // non-permission key whose value is prose.
    let wire = variable_value(&message, "system.github.token.permissions")
        .expect("PAT mode keeps the permissions wire variable");
    assert!(
        serde_json::from_str::<serde_json::Value>(wire).is_ok_and(|value| value.is_object()),
        "the permissions wire variable must stay a JSON object, got: {wire}"
    );
    // The token's real authority is published separately, so the runner can
    // state that the declared set above is not enforced in PAT mode.
    let pat_scopes = variable_value(&message, "system.github.token.pat_scopes")
        .expect("PAT mode publishes the token's real authority");
    assert!(
        pat_scopes.contains("static PAT OAuth scopes") && pat_scopes.contains("read:org"),
        "pat_scopes must carry the introspected OAuth scopes, got: {pat_scopes}"
    );
}

/// The App-manifest setup flow receives the webhook secret from GitHub and
/// stores it in the config file. Before that key existed the secret lived
/// only in `PRELOOP_WEBHOOK_SECRET`, so a configured engine still rejected
/// every signed delivery until the operator re-exported it by hand.

/// The App-manifest setup flow receives the webhook secret from GitHub and
/// stores it in the config file. Before that key existed the secret lived
/// only in `PRELOOP_WEBHOOK_SECRET`, so a configured engine still rejected
/// every signed delivery until the operator re-exported it by hand.
#[tokio::test]
async fn config_webhook_secret_verifies_signed_deliveries() {
    let temp = tempfile::tempdir().unwrap();
    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    tokio::fs::write(
        ws_dir.join(".github/workflows/build.yml"),
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
    )
    .await
    .unwrap();

    let event_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);
    let config_path = temp.path().join("config.toml");
    std::fs::write(
        &config_path,
        "[github]\nwebhook_secret = \"from-config-file\"\n",
    )
    .unwrap();

    // Explicit config path rather than `PRELOOP_CONFIG`, which would race
    // every other test building an `AppState`.
    let mut state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    assert_eq!(
        state.webhook_secret.as_deref(),
        Some("from-config-file"),
        "the config file is a valid source for the webhook secret"
    );
    state.local_workspace = Some(ws_dir);
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "ref": "refs/heads/main",
        "before": "0000000000000000000000000000000000000000",
        "after": event_sha.clone(),
        "repository": {"full_name": "owner/repo", "default_branch": "main"},
        "commits": [{
            "id": event_sha,
            "added": ["src/main.rs"],
            "modified": [],
            "removed": []
        }],
    });
    let body = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"from-config-file").unwrap();
    mac.update(&body);
    let signature = format!(
        "sha256={}",
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "config-secret-delivery")
                .header("x-hub-signature-256", &signature)
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::ACCEPTED,
        "a correctly signed delivery is accepted with only the config file configured"
    );

    // The same body under a different secret must still be rejected —
    // otherwise the check is decorative.
    let forged = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "push")
                .header("x-github-delivery", "forged-delivery")
                .header("x-hub-signature-256", "sha256=deadbeef")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn repo_scoped_secrets_override_global_and_stay_scoped() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let mut secrets = state.secrets.write();
        secrets
            .global
            .insert("GLOBAL_TOKEN".to_owned(), "global-value".to_owned());
        secrets.repo.insert(
            "owner/repo".to_owned(),
            BTreeMap::from([
                ("REPO_TOKEN".to_owned(), "repo-value".to_owned()),
                ("GLOBAL_TOKEN".to_owned(), "repo-wins".to_owned()),
            ]),
        );
    }
    let app = app(state.clone(), CancellationToken::new());
    let workflow =
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $SECRET\n";

    // owner/repo: the per-repo tier overrides the global tier per name and
    // contributes its own names.
    let accepted = submit_yaml(&app, workflow, "owner/repo").await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("repo-wins"),
        "per-repo secret overrides the global tier"
    );
    assert_eq!(
        variable_value(&message, "REPO_TOKEN"),
        Some("repo-value"),
        "per-repo secret is injected"
    );
    drop(inner);

    // other/repo: only the global tier applies — repo secrets stay scoped.
    let accepted = submit_yaml(&app, workflow, "other/repo").await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("global-value"),
        "unscoped repo still gets the global tier"
    );
    assert_eq!(
        variable_value(&message, "REPO_TOKEN"),
        None,
        "repo-scoped secret must not leak into another repository"
    );
    drop(inner);

    // Submission-provided secrets still win over both tiers.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": workflow,
            "event": "push",
            "repository": "owner/repo",
            "secrets": { "GLOBAL_TOKEN": "submitted-value" }
        }),
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.inner.lock().await;
    let message = queued_message_for(&inner, run_id);
    assert_eq!(
        variable_value(&message, "GLOBAL_TOKEN"),
        Some("submitted-value"),
        "submission-provided secrets outrank both stored tiers"
    );
}

/// Send a request carrying a specific bearer token and return just the status
/// code — the shared counterpart the cache-gating tests use, so a
/// request-shape change lands in one place.
#[tokio::test]
async fn live_secrets_api_round_trips_and_persists() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/REPO_ONLY",
            json!({ "value": "v1", "repo": "owner/repo" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/GLOBAL_ONLY",
            json!({ "value": "g1" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/OTHER",
            json!({ "value": "x", "repo": "other/repo" }),
        )
        .await;

        // Full listing carries both tiers; scoped listing only its repo.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let entries: Vec<(String, Option<String>)> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["name"].as_str().unwrap().to_owned(),
                    entry["repo"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert!(entries.contains(&("REPO_ONLY".to_owned(), Some("owner/repo".to_owned()))));
        assert!(entries.contains(&("GLOBAL_ONLY".to_owned(), None)));
        assert!(entries.contains(&("OTHER".to_owned(), Some("other/repo".to_owned()))));

        let scoped = request_json(
            &app,
            Method::GET,
            "/api/v1/secrets?repo=owner/repo",
            Value::Null,
        )
        .await;
        let scoped_names: Vec<&str> = scoped["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(scoped_names, vec!["REPO_ONLY"]);

        // Deletion, then 404 on a second attempt.
        request_json(
            &app,
            Method::DELETE,
            "/api/v1/secrets/REPO_ONLY?repo=owner/repo",
            Value::Null,
        )
        .await;
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/REPO_ONLY?repo=owner/repo",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Validation: lowercase names and empty values are rejected.
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/lowercase",
            json!({ "value": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/BAD",
            json!({ "value": "", "repo": "owner/repo" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // The in-memory store and the persisted file both reflect the API.
        let store = state.secrets.read();
        assert!(store.global.contains_key("GLOBAL_ONLY"));
        assert!(!store.repo.contains_key("owner/repo"));
        assert!(store.repo["other/repo"].contains_key("OTHER"));
        drop(store);
        let text = std::fs::read_to_string(&config_path).unwrap();
        assert!(text.contains("GLOBAL_ONLY"), "config persists the secret");
        assert!(text.contains("other/repo"), "config persists the scope");
    }
    .await;
}

#[tokio::test]
async fn live_secrets_api_env_scope_round_trips() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/DEPLOY_KEY",
            json!({ "value": "k1", "repo": "owner/repo", "env": "prod" }),
        )
        .await;
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/SHARED",
            json!({ "value": "repo-only", "repo": "owner/repo" }),
        )
        .await;

        // Full listing carries the environment scope on the env entry.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let entries: Vec<(String, Option<String>, Option<String>)> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    entry["name"].as_str().unwrap().to_owned(),
                    entry["repo"].as_str().map(str::to_owned),
                    entry["env"].as_str().map(str::to_owned),
                )
            })
            .collect();
        assert!(entries.contains(&(
            "DEPLOY_KEY".to_owned(),
            Some("owner/repo".to_owned()),
            Some("prod".to_owned())
        )));
        assert!(entries.contains(&("SHARED".to_owned(), Some("owner/repo".to_owned()), None)));

        // Env-scoped listing returns only that environment's names.
        let scoped = request_json(
            &app,
            Method::GET,
            "/api/v1/secrets?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        let scoped_names: Vec<&str> = scoped["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(scoped_names, vec!["DEPLOY_KEY"]);

        // The in-memory store and the persisted file both reflect the env tier.
        {
            let store = state.secrets.read();
            assert_eq!(store.env["owner/repo"]["prod"]["DEPLOY_KEY"], "k1");
        }
        // Reload, don't grep: the assertion must prove the value round-trips
        // through the serializer, not merely that the literal appears in the
        // file (which a malformed or misplaced table could satisfy).
        let persisted = crate::config::load_config_from(&config_path).unwrap();
        assert_eq!(
            persisted.env_secrets["owner/repo"]["prod"]["DEPLOY_KEY"], "k1",
            "config persists the env secret"
        );

        // Validation: env without repo and malformed env names are rejected.
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/NO_REPO",
            json!({ "value": "x", "env": "prod" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = request_json_status(
            &app,
            Method::PUT,
            "/api/v1/secrets/BAD_ENV",
            json!({ "value": "x", "repo": "owner/repo", "env": "-dash" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) =
            request_json_status(&app, Method::GET, "/api/v1/secrets?env=prod", Value::Null).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Deletion, then 404 on a second attempt.
        request_json(
            &app,
            Method::DELETE,
            "/api/v1/secrets/DEPLOY_KEY?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/DEPLOY_KEY?repo=owner/repo&env=prod",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // The env map is pruned when its last name goes.
        {
            let store = state.secrets.read();
            assert!(
                !store.env.contains_key("owner/repo"),
                "empty environment maps are pruned"
            );
        }
    }
    .await;
}

/// `secrets_store = "memory"` keeps values out of the config file entirely:
/// the live API mutates the in-memory store only, so a restart loses the
/// secret and the file never carries it. The in-memory store must still
/// serve it for the current process lifetime.

/// `secrets_store = "memory"` keeps values out of the config file entirely:
/// the live API mutates the in-memory store only, so a restart loses the
/// secret and the file never carries it. The in-memory store must still
/// serve it for the current process lifetime.
#[tokio::test]
async fn memory_secrets_store_never_writes_the_config_file() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "secrets_store = \"memory\"\n").unwrap();
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/GLOBAL_ONLY",
            json!({ "value": "g1" }),
        )
        .await;

        // Live and visible in the store.
        let listed = request_json(&app, Method::GET, "/api/v1/secrets", Value::Null).await;
        let names: Vec<&str> = listed["secrets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&"GLOBAL_ONLY"),
            "live store serves the secret"
        );
        {
            let store = state.secrets.read();
            assert!(store.global.contains_key("GLOBAL_ONLY"));
        }

        // Never persisted: the file holds the store-mode key and nothing else.
        let persisted = crate::config::load_config_from(&config_path).unwrap();
        assert!(persisted.secrets.is_empty(), "memory mode must not persist");
        assert!(persisted.repo_secrets.is_empty());
        assert_eq!(persisted.secrets_store.as_deref(), Some("memory"));

        // Deletion must use the runtime store as the source of truth: the
        // config-driven lookup would 404 on a secret that never reached the
        // file, leaving it live in memory.
        let (status, _) = request_json_status(
            &app,
            Method::DELETE,
            "/api/v1/secrets/GLOBAL_ONLY",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        {
            let store = state.secrets.read();
            assert!(
                !store.global.contains_key("GLOBAL_ONLY"),
                "memory-mode delete must remove from the runtime store"
            );
        }
    }
    .await;
}

/// Concurrent secret mutations must not lose writes. Each handler loads the
/// whole config file, changes one entry and writes it back; without the
/// `secret_mutation` lock the requests read the same base config and the
/// last rename wins, so the file loses secrets the in-memory store still
/// reports. Remove the lock and this test fails.

/// Concurrent secret mutations must not lose writes. Each handler loads the
/// whole config file, changes one entry and writes it back; without the
/// `secret_mutation` lock the requests read the same base config and the
/// last rename wins, so the file loses secrets the in-memory store still
/// reports. Remove the lock and this test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_secret_mutations_keep_store_and_file_in_agreement() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    async {
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path.clone())
            .await
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        // Seeded so the concurrent burst has something to delete.
        request_json(
            &app,
            Method::PUT,
            "/api/v1/secrets/DOOMED",
            json!({ "value": "gone" }),
        )
        .await;

        const WRITERS: usize = 12;
        let mut tasks = Vec::with_capacity(WRITERS + 1);
        for index in 0..WRITERS {
            let app = app.clone();
            tasks.push(tokio::spawn(async move {
                request_json(
                    &app,
                    Method::PUT,
                    &format!("/api/v1/secrets/CONCURRENT_{index}"),
                    json!({ "value": format!("value-{index}") }),
                )
                .await;
            }));
        }
        let delete_app = app.clone();
        tasks.push(tokio::spawn(async move {
            request_json(
                &delete_app,
                Method::DELETE,
                "/api/v1/secrets/DOOMED",
                Value::Null,
            )
            .await;
        }));
        for task in tasks {
            task.await.unwrap();
        }

        let expected: Vec<String> = (0..WRITERS)
            .map(|index| format!("CONCURRENT_{index}"))
            .collect();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();

        let store = state.secrets.read();
        assert!(
            !store.global.contains_key("DOOMED"),
            "deleted secret survived in the in-memory store"
        );
        let store_names: Vec<String> = store.global.keys().cloned().collect();
        drop(store);

        let persisted = crate::config::load_config_from(&config_path).unwrap();
        for name in &expected {
            assert!(
                persisted.secrets.contains_key(name),
                "{name} lost from the persisted config: {:?}",
                persisted.secrets.keys().collect::<Vec<_>>()
            );
            let index = name.trim_start_matches("CONCURRENT_");
            assert_eq!(
                persisted.secrets[name],
                format!("value-{index}"),
                "{name} persisted with the wrong value"
            );
        }
        assert!(
            !persisted.secrets.contains_key("DOOMED"),
            "deleted secret was resurrected by a concurrent write"
        );

        // Neither side may carry a name the other does not, and nothing
        // unexpected may survive on either side.
        let persisted_names: Vec<String> = persisted.secrets.keys().cloned().collect();
        assert_eq!(store_names, expected_sorted);
        assert_eq!(persisted_names, expected_sorted);
    }
    .await;
}

/// The secret store holds plaintext values, so its `Debug` must never print
/// them — one `debug!(?store)` would otherwise dump every stored secret.

/// The secret store holds plaintext values, so its `Debug` must never print
/// them — one `debug!(?store)` would otherwise dump every stored secret.
#[test]
fn secret_store_debug_redacts_values() {
    let mut store = crate::state::SecretStore::default();
    store
        .global
        .insert("GLOBAL_NAME".to_owned(), "global-plaintext".to_owned());
    store.repo.insert(
        "owner/repo".to_owned(),
        [("REPO_NAME".to_owned(), "repo-plaintext".to_owned())]
            .into_iter()
            .collect(),
    );

    let rendered = format!("{store:?}");
    assert!(rendered.contains("GLOBAL_NAME"), "{rendered}");
    assert!(rendered.contains("REPO_NAME"), "{rendered}");
    assert!(rendered.contains("owner/repo"), "{rendered}");
    assert!(!rendered.contains("global-plaintext"), "{rendered}");
    assert!(!rendered.contains("repo-plaintext"), "{rendered}");
    // Alternate formatting must be redacted too.
    let pretty = format!("{store:#?}");
    assert!(!pretty.contains("global-plaintext"), "{pretty}");
    assert!(!pretty.contains("repo-plaintext"), "{pretty}");
}

#[tokio::test]
async fn completion_step_results_are_authoritative_over_inference() {
    // The official runner carries final step conclusions in
    // CompleteJob.stepResults (TaskResult strings). A step the worker reports
    // as "skipped" must not be blanket-terminalized to the job's failure
    // status: the completion's own stepResults win.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n",
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
            .unwrap();
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };
    // The step update and the completion report describe the same step, so
    // both carry the manifest's id — that shared identity is what makes
    // `stepResults` authoritative over the job-status inference.
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
                "name": "Test",
                "status": 3,
                "conclusion": 0
            }]
        }),
    )
    .await;
    request_json(
        &app,
        Method::POST,
        "/internal/test/jobs/complete",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "status": "failure",
            "outputs": {},
            "step_results": [{
                "external_id": step_id,
                "number": 2,
                "name": "Test",
                "status": "completed",
                "conclusion": "skipped"
            }]
        }),
    )
    .await;

    let run = get_run_json(&app, &run_id).await;
    assert_eq!(run["status"], "failure");
    assert_eq!(
        run["jobs_list"][0]["steps"][0]["conclusion"], "skipped",
        "the completion's stepResults must override job-status inference"
    );
}

#[tokio::test]
async fn workflow_steps_update_prefers_runner_reported_step_names() {
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

    // The server leaves the broker-message display name empty for steps
    // without an explicit `name:`; the runner reports the rendered name
    // ("Run echo hi") in WorkflowStepsUpdate and that must win, not the
    // empty lookup result.
    let (plan_id, agent_job_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .expect("submitted run must have a job request");
        (request.plan_id.clone(), request.agent_job_id.to_string())
    };

    let response = request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [{
                "external_id": workflow_step_ids(&state, run_id.parse().unwrap(), "build")
                    .await
                    .remove(0),
                "number": 2,
                "name": "Run echo hi",
                "status": 6,
                "conclusion": 2
            }]
        }),
    )
    .await;
    assert_eq!(response["ok"], true);

    let run = get_run_json(&app, &run_id).await;
    let steps = run["jobs_list"][0]["steps"].as_array().unwrap();
    assert!(
        steps.iter().any(|step| step["name"] == "Run echo hi"),
        "runner-reported step name must appear in the run record: {steps:?}"
    );
    assert!(
        steps
            .iter()
            .all(|step| !step["name"].as_str().unwrap_or("").is_empty()),
        "no step may have an empty name in the run record: {steps:?}"
    );
}

#[tokio::test]
async fn workflow_steps_update_preserves_duplicate_names_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - name: Test\n        run: cargo test --lib\n      - name: Test\n        run: cargo test --integration\n",
        "local/preloop",
    )
    .await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (plan_id, agent_job_id, request_id) = {
        let inner = state.inner.lock().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id.0.to_string() == run_id)
            .expect("submitted run must have a job request");
        (
            request.plan_id.clone(),
            request.agent_job_id.to_string(),
            request.request_id,
        )
    };
    // The runner echoes the request message's step ids back as `external_id`
    // (verified in `.runner-watch/golden/v2.336.0/06-multi-step`), so the two
    // same-named steps are distinguished by identity, not by their names.
    let ids = workflow_step_ids(&state, run_id.parse().unwrap(), "build").await;
    assert_eq!(ids.len(), 2, "both declared steps must be in the manifest");
    let first_id = ids[0].clone();
    let second_id = ids[1].clone();

    let response = request_json(
        &app,
        Method::POST,
        "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
        json!({
            "workflow_run_backend_id": plan_id,
            "workflow_job_run_backend_id": agent_job_id,
            "steps": [
                {
                    "external_id": first_id,
                    "number": 1,
                    "name": "Test",
                    "status": 6,
                    "conclusion": 2
                },
                {
                    "external_id": second_id,
                    "number": 2,
                    "name": "Test",
                    "status": 6,
                    "conclusion": 2
                }
            ]
        }),
    )
    .await;
    assert_eq!(response["ok"], true);

    {
        let mut inner = state.inner.lock().await;
        inner.broker_messages.remove(&request_id);
    }
    let run = get_run_json(&app, &run_id).await;
    let steps = run["jobs_list"][0]["steps"].as_array().unwrap();
    assert_eq!(
        steps.len(),
        2,
        "duplicate names must remain separate: {steps:?}"
    );
    assert_eq!(steps[0]["id"], first_id);
    assert_eq!(steps[1]["id"], second_id);

    write_step_job_logs(
        &temp,
        &plan_id,
        &agent_job_id,
        &[
            (&first_id, "first duplicate\n"),
            (&second_id, "second duplicate\n"),
        ],
    )
    .await;
    for (step, expected) in [(1, "first duplicate\n"), (2, "second duplicate\n")] {
        let (status, body) = get_logs(
            &app,
            format!("/api/v1/runs/{run_id}/logs?job=build&step={step}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, expected.as_bytes());
    }
}
