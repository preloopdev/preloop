// SAFETY: edition-2024 env mutation; each test serializes via GITHUB_ENV_LOCK.
#![allow(unsafe_code)]

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
        let tx = state.test_tx().await;
        *tx.job_requests.keys().next().unwrap()
    };

    // Worker stops renewing (lease stale past the hung threshold) but the
    // session stays fresh — the listener is alive, the worker is not.
    let now_micros = crate::store::now_us();
    state
        .test_db_mutate(|tx| {
            tx.set_session_seen("default", now_micros).unwrap();
            tx.set_lease_renewed(
                request_id,
                1,
                now_micros - (HUNG_WORKER_LEASE_SECONDS as i64 + 1) * 1_000_000,
                now_micros + JOB_LEASE_SECONDS as i64 * 1_000_000,
            )
            .unwrap();
        })
        .await;

    reap_once(&shared).await;

    let tx = state.test_tx().await;
    let request = tx.job_requests.get(&request_id).unwrap();
    assert_eq!(
        request.result,
        Some(ExecutionStatus::Failure),
        "a hung worker must be reaped on its own cadence, not the full lease"
    );
    assert!(tx.session_active_requests.is_empty());
    assert_eq!(
        tx.runs.get(&run_id).unwrap().status,
        ExecutionStatus::Failure
    );
}

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
        let tx = state.test_tx().await;
        *tx.job_requests.keys().next().unwrap()
    };

    // Lease stale past the hung threshold but the session is dead — this is a
    // disconnect, so the job must NOT be reaped at the hung-worker cadence.
    let now_micros = crate::store::now_us();
    let liveness = state.test_tx().await.runner_liveness_timeout;
    state
        .test_db_mutate(|tx| {
            tx.set_session_seen(
                "default",
                now_micros - liveness.as_micros() as i64 - 1_000_000,
            )
            .unwrap();
            tx.set_lease_renewed(
                request_id,
                1,
                now_micros - (HUNG_WORKER_LEASE_SECONDS as i64 + 1) * 1_000_000,
                now_micros + JOB_LEASE_SECONDS as i64 * 1_000_000,
            )
            .unwrap();
        })
        .await;

    reap_once(&shared).await;

    let tx = state.test_tx().await;
    let request = tx.job_requests.get(&request_id).unwrap();
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

    // Create a dummy workflow file in a local workspace
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

    // Prepare mock webhook push payload
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

    // Compute correct signature
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

    // Send request with WRONG signature -> should fail with 401
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

    // Send request with CORRECT signature -> should succeed with 200
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
    let inner = state.test_tx().await;
    assert_eq!(inner.runs.len(), 1);
    // Queue depth is sampled by the 5s sampler, not updated per operation;
    // run one tick inline (the test harness spawns no sampler task).
    state.test_sample_state_once().await;
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
            .arg("-c")
            .arg("commit.gpgsign=false")
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
    let inner = state.test_tx().await;
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
    let inner = state.test_tx().await;
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
    let inner = state.test_tx().await;
    assert!(
        inner.runs.is_empty(),
        "pull_request_target must not execute head-controlled YAML"
    );
}

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

        let inner = state.test_tx().await;
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
    let inner = recovered.test_tx().await;
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
    state
        .test_db_mutate(|tx| {
            tx.set_run_status(original_run_id, "completed", Some("failure"))
                .unwrap();
            tx.0
                .execute(
                    "UPDATE jobs SET status = 'failure', queue_state = 'none'                      WHERE run_id = ?1 AND job_id = 'build'",
                    [original_run_id.to_string()],
                )
                .unwrap();
            tx.set_job_check_run(
                original_run_id,
                &JobId("build".to_owned()),
                original_check_run_id as i64,
            )
            .unwrap();
        })
        .await;

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

    let inner = state.test_tx().await;
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
    let inner = fixture.state.test_tx().await;
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
        let inner = fixture.state.test_tx().await;
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
    let inner = fixture.state.test_tx().await;
    assert_eq!(
        inner.runs.len(),
        1,
        "a retry after pre-enqueue rejection must create the run"
    );
}

/// A failed dedup read must not fall through to submitting: that
/// re-runs the workflow push-back already tested and published, which is
/// exactly the duplicate this gate exists to prevent.
#[tokio::test]
async fn push_webhook_refuses_to_submit_when_the_dedup_read_fails() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = WebhookDedupFixture::new(&temp).await;
    assert_eq!(
        fixture.post("delivery-dedup-fail", Some("push")).await,
        StatusCode::ACCEPTED
    );
    fixture
        .state
        .test_db_mutate(|tx| {
            tx.0.execute("DROP TABLE run_push_states", [])
                .expect("drop run_push_states");
        })
        .await;

    fixture.drain().await;
    let inner = fixture.state.test_tx().await;
    assert!(
        inner.runs.is_empty(),
        "a failed dedup read must not submit a possibly-duplicate run"
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
    let inner = fixture.state.test_tx().await;
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
        let inner = fixture.state.test_tx().await;
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
    let claimed = restarted_state.test_claim_webhook_deliveries(1, 60).await;
    assert_eq!(
        claimed, 0,
        "a completed delivery must not be claimed after restart"
    );
    crate::github::drain_webhook_queue(&shared).await.unwrap();

    let inner = restarted_state.test_tx().await;
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
    let claimed = fixture.state.test_claim_webhook_deliveries(1, 0).await;
    assert_eq!(
        claimed, 1,
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
        restarted_state.test_recover_webhook_deliveries().await,
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
    let inner = restarted_state.test_tx().await;
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
            .arg("-c")
            .arg("commit.gpgsign=false")
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
    let inner = state.test_tx().await;
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

/// A fork-PR run under `require_approval` must hold even needs-empty jobs in
/// `Pending`: the submit-time fast path used to enqueue them straight to the
/// ready queue, letting a runner claim them before the operator approved.
#[tokio::test]
async fn fork_pr_needs_empty_job_held_until_approval() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();

    let ws_dir = temp.path().join("workspace");
    tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
        .await
        .unwrap();
    // Single job with no `needs:` — the exact shape that took the fast path.
    let workflow_content = r#"
on: pull_request
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
    tokio::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow_content)
        .await
        .unwrap();

    let head_sha = commit_workflow_fixture(&ws_dir, &[".github/workflows/build.yml"]);
    // The adapter also emits a pull_request_target event (base trust tier);
    // it needs a base SHA or it aborts the batch before our pull_request
    // event is processed.
    let base_sha = head_sha.clone();

    let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.webhook_secret = Some("super-secret".to_owned());
    state.local_workspace = Some(ws_dir.clone());
    state.fork_policy.require_approval = true;
    let system_token = state.system_token.clone();
    let app = app(state.clone(), CancellationToken::new());

    let payload = serde_json::json!({
        "action": "opened",
        "number": 7,
        "pull_request": {
            "head": {
                "ref": "feature",
                "sha": head_sha,
                "repo": { "fork": true }
            },
            "base": { "ref": "main", "sha": base_sha },
        },
        "repository": {
            "full_name": "owner/repo",
            "default_branch": "main"
        }
    });
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"super-secret").unwrap();
    mac.update(&payload_bytes);
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-event", "pull_request")
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

    let runs = request_json(&app, Method::GET, "/api/v1/runs", Value::Null).await;
    let runs = runs.as_array().expect("run list is an array");
    assert_eq!(runs.len(), 1, "fork PR must create a run");
    let run = &runs[0];
    assert_eq!(
        run["fork_approval_pending"],
        Value::Bool(true),
        "fork PR run must wait for approval"
    );
    let jobs = run["jobs"].as_object().expect("job statuses");
    assert_eq!(jobs.len(), 1);
    // The needs-empty job parks in `pending`: `queued` would mean it reached
    // the ready queue and a runner could claim it before the approval.
    assert_eq!(
        jobs.values().next().unwrap(),
        "pending",
        "needs-empty fork job must hold in Pending"
    );
    let run_id = run["run_id"].as_str().unwrap().to_owned();

    // Approving releases the hold; an empty JSON body must be accepted.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/api/v1/runs/{run_id}/approve-fork"))
                .header("authorization", format!("Bearer {system_token}"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let run = request_json(
        &app,
        Method::GET,
        &format!("/api/v1/runs/{run_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(
        run["fork_approval_pending"],
        Value::Bool(false),
        "approval must clear the hold"
    );
    assert_eq!(
        run["jobs"]["build"], "queued",
        "the released job reaches the ready queue"
    );
}

#[tokio::test]
async fn github_app_manifest_registration_flow() {
    let temp = tempfile::tempdir().unwrap();

    // Setup a local mock GitHub API server for manifest conversion
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

    // Configure mock API URL in environment
    // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    unsafe {
        std::env::set_var(
            "PRELOOP_GITHUB_API_URL",
            format!("http://127.0.0.1:{}", port),
        )
    };

    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Request registration form (GET /api/v1/github/register)
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

    // Request callback conversion (GET /api/v1/github/callback?code=mock_code_123)
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
    unsafe { std::env::remove_var("PRELOOP_GITHUB_API_URL") };
}

#[tokio::test]
async fn runner_oauth2_token_client_assertion_verification() {
    use preloop_gha_protocol::crypto::{sign_jwt_ps256, sign_jwt_rs256};
    use serde_json::Value;

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    // Generate RSA keypair for the runner using the protocol's library
    let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
    let rsa_params = keypair.to_rsaparams();

    let keypair_xml = format!(
        "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
        rsa_params.modulus, rsa_params.exponent
    );

    // Register the runner
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

    // Build a valid client assertion JWT signed with the runner's private RSA key
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
        // aud must identify this server (the called token endpoint).
        "aud": "http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token",
        "jti": uuid::Uuid::new_v4().to_string(),
        "nbf": now,
        "exp": now + 300,
    });

    let client_assertion = sign_jwt_ps256(&header, &claims, &rsa_params).unwrap();

    // Request OAuth token using urlencoded body
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

    // Test negative case: Invalid signature (wrong key)
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

// ─── client_assertion expiry / audience validation ───

#[test]
fn accepts_valid_assertion_claims() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = oauth2_token_uri();
    // Exact endpoint URL.
    let claims = assertion_claims(
        now,
        serde_json::json!("http://127.0.0.1:9090/runner/server/_apis/v1/oauth2/token"),
    );
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
    // Base URL alone is also accepted.
    let claims = assertion_claims(now, serde_json::json!("http://127.0.0.1:9090"));
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
    // Array form.
    let claims = assertion_claims(
        now,
        serde_json::json!(["https://other.example", "http://127.0.0.1:9090"]),
    );
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_ok());
}

#[test]
fn rejects_expired_assertion() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = oauth2_token_uri();
    let claims = serde_json::json!({
        "sub": "test-client",
        "aud": "http://127.0.0.1:9090",
        "nbf": now - 600,
        "exp": now - 1,
    });
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
}

#[test]
fn rejects_wrong_audience() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = oauth2_token_uri();
    // Assertion addressed to a different server must not validate here.
    let claims = assertion_claims(now, serde_json::json!("https://preloop.local/oauth"));
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
    // Missing aud is also rejected.
    let claims = serde_json::json!({"sub": "test-client", "nbf": now, "exp": now + 300});
    assert!(crate::oauth::validate_client_assertion_claims(&claims, &uri).is_err());
}

#[test]
fn rejects_missing_exp_and_excessive_lifetime() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let uri = oauth2_token_uri();
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
#[test]
fn label_matching_os_less_runner_stays_eligible() {
    let unlabelled = ["self-hosted".to_owned(), "gpu".to_owned()];
    assert!(job_matches_runner(&["ubuntu-latest".into()], &unlabelled));
    assert!(job_matches_runner(&["windows-2022".into()], &unlabelled));
    assert!(!job_matches_runner(&["nvidia".into()], &unlabelled));
}

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

    let inner = state.test_tx().await;
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
        !inner.ready().any(|j| j.job_id.0 == "test"),
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

    let inner = state.test_tx().await;
    assert!(
        inner.ready().any(|job| job.job_id.0 == "cleanup"),
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

    let inner = state.test_tx().await;
    assert!(
        inner.ready().any(|job| job.job_id.0 == "notify"),
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
        let inner = state.test_tx().await;
        assert_eq!(inner.ready().count(), 1);
        assert_eq!(inner.ready().next().unwrap().job_id.0, "build");
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
        let inner = state.test_tx().await;
        let queued_ids: std::collections::BTreeSet<_> =
            inner.ready().map(|j| j.job_id.0.clone()).collect();
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
        let inner = state.test_tx().await;
        assert!(
            inner.ready().any(|j| j.job_id.0 == "deploy"),
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

    let inner = state.test_tx().await;
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

    submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $SECRET\n        env:\n          SECRET: ${{ secrets.E2E_TEST_SECRET }}\n",
        "owner/repo",
    )
    .await;

    // The stored message is a secret-free template; the secret arrives at
    // acquire, resolved through the SecretProvider, as a secret variable —
    // the runner's `secrets.*` context source.
    let acquired = acquire_queued_job(&app, "stored-secret-runner").await;
    let variables = &acquired["variables"];
    let var = variables
        .as_object()
        .and_then(|map| map.get("E2E_TEST_SECRET"))
        .expect("filled message carries the stored secret variable");
    assert_eq!(var["value"].as_str(), Some("stored-value"));
    assert_eq!(var["isSecret"].as_bool(), Some(true));
}

/// The acquire fill injects only the stored secrets the job's own expressions
/// reference. Every `isSecret` variable lands in the runner's `secrets`
/// context — an unreferenced name would ride the wire for nothing, so
/// `spec.names` carries the referenced subset and nothing else.
#[tokio::test]
async fn unreferenced_stored_secrets_stay_out_of_the_job_message() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let mut secrets = state.secrets.write();
        secrets
            .global
            .insert("USED_SECRET".to_owned(), "used-value".to_owned());
        secrets
            .global
            .insert("UNUSED_SECRET".to_owned(), "must-not-ship".to_owned());
    }
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    env:\n      USED: ${{ secrets.USED_SECRET }}\n    steps:\n      - run: echo $USED\n",
        "owner/repo",
    )
    .await;

    let acquired = acquire_queued_job(&app, "referenced-only-runner").await;
    let variables = acquired["variables"].as_object().unwrap();
    assert_eq!(
        variables
            .get("USED_SECRET")
            .and_then(|v| v["value"].as_str()),
        Some("used-value"),
        "the referenced secret is injected: {variables:?}"
    );
    assert!(
        !variables.contains_key("UNUSED_SECRET"),
        "unreferenced stored secrets must not reach the job: {variables:?}"
    );
}

/// A job whose `secrets` reads cannot be enumerated — `secrets[matrix.pick]`,
/// an object filter, a bare `secrets` argument — keeps the whole in-scope
/// set: dropping it would silently turn a real read into an empty string.
#[tokio::test]
async fn dynamic_secret_reads_keep_the_full_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    {
        let mut secrets = state.secrets.write();
        secrets
            .global
            .insert("PIVOTED".to_owned(), "pivot-value".to_owned());
        secrets
            .global
            .insert("ALSO_STORED".to_owned(), "also-value".to_owned());
    }
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ${{ toJSON(secrets) }}\n",
        "owner/repo",
    )
    .await;

    let acquired = acquire_queued_job(&app, "dynamic-secrets-runner").await;
    let variables = acquired["variables"].as_object().unwrap();
    for name in ["PIVOTED", "ALSO_STORED"] {
        assert!(
            variables.contains_key(name),
            "a bare secrets-context read must keep every scoped name: {variables:?}"
        );
    }
}

/// A submission's run-tier secrets must outlive the run finishing and being
/// archived: a re-run of an archived run re-resolves them. Dropping the tier
/// at archive silently ran the re-run without secrets.
#[tokio::test]
async fn archived_run_keeps_run_tier_secrets_for_rerun() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo $TOKEN\n        env:\n          TOKEN: ${{ secrets.MY_TOKEN }}\n",
            "event": "push",
            "repository": "owner/repo",
            "secrets": {"MY_TOKEN": "s3cr3t-value"}
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    complete_via_api(&app, accepted["run_id"].as_str().unwrap(), "build").await;

    // Push the settled run past the archive grace window, then archive it.
    state
        .test_db_mutate(|tx| {
            let old = (chrono::Utc::now() - chrono::Duration::seconds(120)).timestamp_micros();
            tx.set_run_completed_at_us(run_id, old).unwrap();
        })
        .await;
    let archived = state.test_archive_finished_runs_once().await;
    assert!(archived >= 1, "the settled run must archive");
    let resolved = state.test_resolve_run_secrets("owner/repo", run_id);
    assert_eq!(
        resolved.get("MY_TOKEN").map(|secret| secret.expose()),
        Some("s3cr3t-value"),
        "archiving a run must not drop its run tier"
    );

    // Re-run the archived run: the new run's jobs resolve the same value.
    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/runs/{run_id}/rerun"),
        Value::Null,
    )
    .await;
    let acquired = acquire_queued_job(&app, "rerun-secret-runner").await;
    let var = acquired["variables"]
        .as_object()
        .and_then(|map| map.get("MY_TOKEN"))
        .expect("re-run acquires the submission secret");
    assert_eq!(var["value"].as_str(), Some("s3cr3t-value"));
    assert_eq!(var["isSecret"].as_bool(), Some(true));
}

/// A reusable-workflow callee that declares no `secrets:` receives none of
/// the caller's secrets — only `secrets: inherit` or an explicit map does.
#[tokio::test]
async fn reusable_callee_without_secrets_receives_none() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.secrets.write().repo.insert(
        "owner/repo".to_owned(),
        std::collections::BTreeMap::from([("OTHER".to_owned(), "leak".to_owned())]),
    );
    let app = app(state.clone(), CancellationToken::new());

    let callee_yaml = "on: workflow_call\njobs:\n  inner:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo inner\n";
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  call:\n    uses: ./.github/workflows/callee.yml\n",
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {".github/workflows/callee.yml": callee_yaml},
        }),
    )
    .await;

    let acquired = acquire_queued_job(&app, "callee-no-secrets").await;
    let variables = acquired["variables"].as_object().unwrap();
    assert!(
        !variables.contains_key("OTHER"),
        "a callee without `secrets:` must not receive caller secrets: {variables:?}"
    );
}

/// A reusable-call `secrets: {T: ${{ secrets.X }}}` mapping resolves against
/// the caller scope and delivers only the mapped name.
#[tokio::test]
async fn reusable_callee_secrets_map_resolves_against_caller_scope() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state.secrets.write().repo.insert(
        "owner/repo".to_owned(),
        std::collections::BTreeMap::from([("OTHER".to_owned(), "leak".to_owned())]),
    );
    let app = app(state.clone(), CancellationToken::new());

    let callee_yaml = "on: workflow_call\njobs:\n  inner:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo inner\n";
    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  call:\n    uses: ./.github/workflows/callee.yml\n    secrets:\n      T: ${{ secrets.OTHER }}\n",
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": {".github/workflows/callee.yml": callee_yaml},
        }),
    )
    .await;

    let acquired = acquire_queued_job(&app, "callee-mapped").await;
    let variables = acquired["variables"].as_object().unwrap();
    assert_eq!(
        variables.get("T").and_then(|var| var["value"].as_str()),
        Some("leak"),
        "the mapped name resolves against the caller scope: {variables:?}"
    );
    assert!(
        !variables.contains_key("OTHER"),
        "only the mapped name may reach the callee: {variables:?}"
    );
}

/// Job-level `env: ${{ secrets.NAME }}` ships as an expression token in the
/// stored template (values are absent by design). fill_template must resolve
/// it into a literal on `environmentVariables` — the surface the runner
/// materializes into the step environment — since the runner has no secrets
/// context to evaluate the token itself.
#[tokio::test]
async fn job_level_env_secret_is_filled_into_environment_variables() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    state
        .secrets
        .write()
        .global
        .insert("E2E_ENV_SECRET".to_owned(), "env-stored-value".to_owned());
    let app = app(state.clone(), CancellationToken::new());
    let (runner_id, runner_token) =
        register_runner_with_token(&app, "env-secret-runner", &["self-hosted"], None).await;

    request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    env:\n      X: ${{ secrets.E2E_ENV_SECRET }}\n    steps:\n      - run: echo $X\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;

    let session = request_json_with_bearer(
        &app,
        Method::POST,
        "/runner/server/session",
        json!({}),
        &runner_token,
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let job_ref = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/runner/server/message?sessionId={session_id}&status=Online&waitSeconds=0"),
        Value::Null,
        &runner_token,
    )
    .await;
    let body: Value = serde_json::from_str(job_ref["body"].as_str().unwrap()).unwrap();
    let runner_request_id = body["runner_request_id"].as_str().unwrap();

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        &format!("/broker/{runner_id}/acquirejob"),
        json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "Linux"}),
        &runner_token,
    )
    .await;

    let value = acquired["environmentVariables"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry["map"].as_array()?.first())
        .find(|pair| pair["Key"]["lit"].as_str() == Some("X"))
        .map(|pair| pair["Value"].clone())
        .expect("X must reach the filled environmentVariables");
    assert_eq!(
        value["lit"].as_str(),
        Some("env-stored-value"),
        "the fill resolves secrets.* env tokens to literals: {value}"
    );
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
    // Keep PAT scope introspection hermetic while still letting the PAT be
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

    submit_yaml(
        &app,
        "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "owner/repo",
    )
    .await;

    // The stored template carries no token; the broker path mints/selects it
    // at claim — with no App the configured PAT fills `system.github.token`.
    let acquired = acquire_queued_job(&app, "pat-runner").await;
    assert_eq!(
        wire_variable(&acquired, "system.github.token"),
        Some("github_pat_testvalue"),
        "the configured PAT reaches the job as GITHUB_TOKEN"
    );
}

/// A PAT whose OAuth scopes cannot be introspected must not be embedded.
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

    // The stored message is a secret-free template, so the token surface these
    // assertions want is the *acquired* message: `acquirejob` mints the job's
    // credential (runtime token when the PAT is withheld).
    let tx = state.test_tx().await;
    let message = queued_message_for(&tx, &run_id);
    let app = app(state.clone(), CancellationToken::new());
    let acquired = acquire_queued_job(&app, "unverifiable-pat-runner").await;
    // Minted tokens carry a random `jti`, so compare claims rather than bytes:
    // the wire variable must be a valid local JWT scoped to this job.
    let wire_token = wire_variable(&acquired, "system.github.token")
        .expect("the job message carries a GitHub token variable");
    let claims = state
        .verify_local_jwt_claims(wire_token)
        .expect("an unverifiable PAT is withheld in favour of a job-scoped runtime token");
    assert_eq!(
        claims.get("sub").and_then(|value| value.as_str()),
        Some(format!("preloop-job-{}", message.job_id).as_str())
    );
    assert_eq!(
        claims.get("scp").and_then(|value| value.as_str()),
        Some(
            format!(
                "Actions.Results:{}:{}",
                message.plan.plan_id, message.job_id
            )
            .as_str()
        )
    );
    assert_ne!(
        wire_variable(&acquired, "system.github.token"),
        Some("github_pat_unverifiable_scopes"),
        "the PAT must never reach a job whose bounds could not be verified"
    );
    let authority = wire_variable(&acquired, "system.github.token.pat_scopes")
        .expect("the withheld state is published for the runner to print");
    assert!(
        authority.contains("withheld"),
        "the wire variable must disclose the withheld PAT, got: {authority}"
    );
}

/// scope-mismatch matrix for the static-PAT permission check. A classic
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

/// a static PAT whose OAuth scopes exceed the workflow's
/// declared `permissions:` refuses the run instead of silently embedding the
/// broader PAT as the job's `GITHUB_TOKEN`.
#[tokio::test]
async fn static_pat_broader_than_declared_permissions_rejects_run() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    // Distinct PAT string per test: introspected scopes are cached by PAT
    // hash process-wide, so sharing one value across tests would leak cached
    // scopes between them.
    std::fs::write(&config_path, "[github]\npat = \"broad-pat\"\n").unwrap();
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

/// a static PAT whose OAuth scopes are no broader than the workflow's
/// declared `permissions:` still reaches the job — but
/// `system.github.token.permissions` must advertise the PAT's actual scopes,
/// not the declared set the token does not honor.
#[tokio::test]
async fn static_pat_matching_declared_permissions_keeps_honest_wire_variable() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"narrow-pat\"\n").unwrap();
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
    let _accepted = crate::submit_run_inner(
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

    // The stored message is a secret-free template, so the token surface these
    // assertions want is the *acquired* message: `acquirejob` embeds the PAT
    // once its scopes were verified at submit.
    let app = app(state.clone(), CancellationToken::new());
    let acquired = acquire_queued_job(&app, "narrow-pat-runner").await;
    assert_eq!(
        wire_variable(&acquired, "system.github.token"),
        Some("narrow-pat"),
        "the narrow PAT still reaches the job"
    );
    // `system.github.token.permissions` keeps its documented map shape: a
    // consumer parsing it as `{"<Permission>": "<level>"}` must not meet a
    // non-permission key whose value is prose.
    let wire = wire_variable(&acquired, "system.github.token.permissions")
        .expect("PAT mode keeps the permissions wire variable");
    assert!(
        serde_json::from_str::<serde_json::Value>(wire).is_ok_and(|value| value.is_object()),
        "the permissions wire variable must stay a JSON object, got: {wire}"
    );
    // The token's real authority is published separately, so the runner can
    // state that the declared set above is not enforced in PAT mode.
    let pat_scopes = wire_variable(&acquired, "system.github.token.pat_scopes")
        .expect("PAT mode publishes the token's real authority");
    assert!(
        pat_scopes.contains("static PAT OAuth scopes") && pat_scopes.contains("read:org"),
        "pat_scopes must carry the introspected OAuth scopes, got: {pat_scopes}"
    );
}

/// A job claimed after the PAT scope cache expired must still receive the
/// PAT. Only submits refresh the cache (TTL 300 s), so a job that queued
/// longer used to be handed the runtime token instead — and the checkout the
/// submit-time routing sent straight to github.com then failed with
/// "could not read Username" (grafana's detect-changes, claimed 8.6 min after
/// submit). Acquire re-verifies the scopes instead of withholding.
#[tokio::test]
async fn static_pat_reaches_a_job_claimed_after_the_scope_cache_expired() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.toml");
    std::fs::write(&config_path, "[github]\npat = \"queued-pat\"\n").unwrap();
    let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
        .await
        .unwrap();
    let _api_url = live_pat_scope_api("").await;
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: CancellationToken::new(),
    });
    let yaml =
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";
    crate::submit_run_inner(
        &shared,
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: yaml.to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        },
    )
    .await
    .expect("a scope-less PAT is accepted");

    crate::runs::expire_pat_scope_cache();
    let app = app(state.clone(), CancellationToken::new());
    let acquired = acquire_queued_job(&app, "queued-pat-runner").await;
    assert_eq!(
        wire_variable(&acquired, "system.github.token"),
        Some("queued-pat"),
        "a job claimed after the cache expired still gets the PAT"
    );
}

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
    let workflow = "on: push\njobs:\n  probe:\n    runs-on: ubuntu-latest\n    env:\n      GLOBAL_TOKEN: ${{ secrets.GLOBAL_TOKEN }}\n      REPO_TOKEN: ${{ secrets.REPO_TOKEN }}\n    steps:\n      - run: echo $SECRET\n";

    // Secrets resolve at acquire through the SecretProvider; the stored
    // template only names them. Acquire each queued job in submit order.
    submit_yaml(&app, workflow, "owner/repo").await;
    let acquired = acquire_queued_job(&app, "scope-runner-a").await;
    assert_eq!(
        wire_variable(&acquired, "GLOBAL_TOKEN"),
        Some("repo-wins"),
        "per-repo secret overrides the global tier"
    );
    assert_eq!(
        wire_variable(&acquired, "REPO_TOKEN"),
        Some("repo-value"),
        "per-repo secret is injected"
    );

    // other/repo: only the global tier applies — repo secrets stay scoped.
    submit_yaml(&app, workflow, "other/repo").await;
    let acquired = acquire_queued_job(&app, "scope-runner-b").await;
    assert_eq!(
        wire_variable(&acquired, "GLOBAL_TOKEN"),
        Some("global-value"),
        "unscoped repo still gets the global tier"
    );
    assert_eq!(
        wire_variable(&acquired, "REPO_TOKEN"),
        None,
        "repo-scoped secret must not leak into another repository"
    );

    // Submission-provided secrets still win over both tiers.
    request_json(
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
    let acquired = acquire_queued_job(&app, "scope-runner-c").await;
    assert_eq!(
        wire_variable(&acquired, "GLOBAL_TOKEN"),
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
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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
        let inner = state.test_tx().await;
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

    state
        .test_db_mutate(|tx| {
            tx.0.execute(
                "DELETE FROM session_messages WHERE request_id = ?1",
                [request_id],
            )
            .unwrap();
        })
        .await;
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
