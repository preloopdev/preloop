use super::super::server_queue::StepUpdate;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn serve_diagnostic_signed_url() -> (String, oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (request_tx, request_rx) = oneshot::channel();
    let body = r#"{"diag_logs_url":"https://blob.example/diagnostics.zip"}"#;

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0_u8; 8192];
        let n = socket.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        let _ = request_tx.send(request);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    (format!("http://{addr}"), request_rx)
}

use super::super::action_preparation::parse_remote_uses;
use super::super::reporting::diagnostic_logs_url;
use super::*;
use tokio::sync::watch;

#[test]
fn any_step_failed_counts_continue_on_error_failures() {
    use crate::worker::contexts::StepResult;
    use indexmap::IndexMap;
    use std::collections::HashMap;

    let mut steps = IndexMap::new();
    steps.insert(
        "ok".to_string(),
        StepResult {
            outcome: "Success".to_string(),
            conclusion: "Success".to_string(),
            outputs: HashMap::new(),
        },
    );
    assert!(!any_step_failed(&steps));

    // A tolerated failure (continue-on-error) still counts: its outcome stays
    // Failure while the job conclusion is green — the VM must be preserved so
    // the failure can be inspected.
    steps.insert(
        "tolerated".to_string(),
        StepResult {
            outcome: "Failure".to_string(),
            conclusion: "Success".to_string(),
            outputs: HashMap::new(),
        },
    );
    assert!(any_step_failed(&steps));

    steps.clear();
    steps.insert(
        "skipped".to_string(),
        StepResult {
            outcome: "Skipped".to_string(),
            conclusion: "Skipped".to_string(),
            outputs: HashMap::new(),
        },
    );
    assert!(!any_step_failed(&steps));
}

#[tokio::test]
async fn test_run_job_executes_successfully() {
    let (_ws, workspace_dir) = contained_workspace();
    let payload = serde_json::json!({
        "jobId": "job-1",
        "jobDisplayName": "Mock Job",
        "steps": [
            {
                "id": "step-1",
                "contextName": "step1",
                "displayName": "Step One",
                "run": "echo step-one-executed",
                "shell": "bash"
            }
        ],
        "fileTable": {
            "workDirectory": workspace_dir
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    assert!(res.is_ok(), "Expected run_job to succeed, got: {:?}", res);
}

#[tokio::test]
async fn periodic_drain_flushes_queued_step_updates() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (request_tx, request_rx) = oneshot::channel::<String>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0_u8; 8192];
        let n = socket.read(&mut buf).await.unwrap();
        let _ = request_tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
        socket
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{}")
            .await
            .unwrap();
    });

    let http = crate::client::http::HttpClient::new(None).unwrap();
    let base_url = format!("http://{addr}");
    let reporting = Arc::new(ReportingContext {
        results: crate::client::results::ResultsClient::new(http.clone(), base_url.clone()),
        run_service: crate::client::run_service::RunServiceClient::new(http, base_url),
        access_token: super::LiveToken::new("test-token".to_string()),
        plan_id: "plan-1".to_string(),
        job_id: "job-1".to_string(),
        azdo: None,
        connectivity_telemetry: Arc::new(Mutex::new(Vec::new())),
    });
    let queue = Arc::new(Mutex::new(ServerQueue::new(
        "job-1".to_string(),
        "plan-1".to_string(),
    )));
    queue.lock().await.queue_update(StepUpdate {
        external_id: "step-1".to_string(),
        number: 1,
        name: "Step One".to_string(),
        status: super::super::server_queue::step_status::IN_PROGRESS,
        started_at: Some("2026-01-01T00:00:00Z".to_string()),
        completed_at: None,
        conclusion: 0,
    });

    let (cancel_tx, mut cancel_rx) = watch::channel(false);
    let drain_rpt = reporting.clone();
    let drain_queue = queue.clone();
    let drain_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    flush_step_updates(&drain_rpt, &drain_queue).await;
                }
                changed = cancel_rx.changed() => {
                    if changed.is_err() || *cancel_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    let request = tokio::time::timeout(Duration::from_secs(1), request_rx)
        .await
        .expect("periodic drain did not flush within one second")
        .expect("periodic drain request sender dropped");
    assert!(request.contains("WorkflowStepsUpdate"));
    assert!(request.contains("step-1"));

    cancel_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), drain_handle)
        .await
        .expect("periodic drain did not stop after cancellation")
        .expect("periodic drain task panicked");
}

#[test]
fn results_url_prefers_system_vss_endpoint_data() {
    let msg = serde_json::json!({
        "resources": {
            "endpoints": [{
                "name": "SystemVssConnection",
                "url": "http://127.0.0.1:9191/broker/1",
                "data": {
                    "ResultsServiceUrl": "http://127.0.0.1:9191/"
                }
            }]
        },
        "variables": {
            "system.github.results_endpoint": { "value": "http://wrong.example/" }
        }
    });

    assert_eq!(
        extract_results_url(&msg).as_deref(),
        Some("http://127.0.0.1:9191")
    );
}

#[tokio::test]
async fn test_run_job_propagates_step_failure() {
    // When a step fails, run_job still returns Ok(()) because the failure
    // is propagated in the completion report, not the function return.
    // The worker process exits 0 and the server sees the Failed result.
    let (_ws, workspace_dir) = contained_workspace();
    let payload = serde_json::json!({
        "jobId": "job-fail",
        "jobDisplayName": "Failing Job",
        "steps": [
            {
                "id": "step-1",
                "contextName": "step1",
                "displayName": "Failing Step",
                "run": "exit 1",
                "shell": "bash"
            }
        ],
        "fileTable": {
            "workDirectory": workspace_dir
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    // run_job returns Ok even when steps fail — the failure result is
    // reported to the server via report_completion, not the return value.
    assert!(
        res.is_ok(),
        "Expected run_job to return Ok even with failing step, got: {:?}",
        res
    );
}

// --- JobRunnerL0 gap coverage ---

#[tokio::test]
async fn test_run_job_handles_cancelled() {
    let (_ws, workspace_dir) = contained_workspace();
    let payload = serde_json::json!({
        "jobId": "job-cancel",
        "jobDisplayName": "Cancel Job",
        "steps": [
            {
                "id": "step-1",
                "contextName": "step1",
                "displayName": "Long Step",
                "run": "sleep 30",
                "shell": "bash"
            }
        ],
        "fileTable": {
            "workDirectory": workspace_dir
        }
    });

    let (cancel_tx, cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = cancel_tx.send(true);
    });

    // run_job returns Ok — cancellation is reported via completion, not
    // the function return value.
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    assert!(
        res.is_ok(),
        "Expected run_job to handle cancel gracefully, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_run_job_with_timeout() {
    let (_ws, workspace_dir) = contained_workspace();
    // jobTimeout of 0 means the timeout fires immediately (0 * 60 = 0s),
    // triggering the cancel channel before the step can finish.
    let payload = serde_json::json!({
        "jobId": "job-timeout",
        "jobDisplayName": "Timeout Job",
        "plan": {"jobTimeoutInMinutes": 0},
        "steps": [
            {
                "id": "step-1",
                "contextName": "step1",
                "displayName": "Long Step",
                "run": "sleep 30",
                "shell": "bash"
            }
        ],
        "fileTable": {
            "workDirectory": workspace_dir
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    assert!(
        res.is_ok(),
        "Expected run_job to handle timeout gracefully, got: {:?}",
        res
    );
}
#[test]
fn action_resolution_key_excludes_subpath() {
    let parsed = parse_remote_uses("actions/cache/restore@v4").expect("valid action ref");
    assert_eq!(parsed.action_name, "actions/cache");
    assert_eq!(parsed.subpath, "restore");
    assert_eq!(parsed.git_ref, "v4");
}

#[test]
fn renew_backoff_stays_within_official_bands() {
    // Official JobDispatcher.RenewJobRequestAsync: random 5–15 s for the
    // first five consecutive errors, random 15–30 s afterwards.
    for error in 1..=5_u32 {
        for _ in 0..50 {
            let delay = renew_backoff(error).as_secs();
            assert!(
                (5..=15).contains(&delay),
                "error {error}: delay {delay}s outside 5–15s band"
            );
        }
    }
    for error in 6..=10_u32 {
        for _ in 0..50 {
            let delay = renew_backoff(error).as_secs();
            assert!(
                (15..=30).contains(&delay),
                "error {error}: delay {delay}s outside 15–30s band"
            );
        }
    }
}

#[test]
fn lease_deadline_parser_accepts_rfc3339_and_rejects_garbage() {
    assert!(parse_lease_deadline("2026-07-13T12:00:00Z").is_some());
    assert!(parse_lease_deadline("not-a-timestamp").is_none());
}

#[test]
fn lease_deadline_gives_up_only_after_five_minute_grace() {
    let locked_until = parse_lease_deadline("2026-07-13T12:00:00Z").unwrap();
    let grace_boundary = parse_lease_deadline("2026-07-13T12:05:00Z").unwrap();
    let after_grace = parse_lease_deadline("2026-07-13T12:05:01Z").unwrap();

    assert!(!lease_expired(locked_until, grace_boundary));
    assert!(lease_expired(locked_until, after_grace));
}

#[test]
fn only_typed_http_404_is_classified_as_job_not_found() {
    let not_found = anyhow::Error::new(crate::client::http::HttpError::Status {
        status: reqwest::StatusCode::NOT_FOUND,
        body: "missing lease".to_owned(),
    });
    let conflict = anyhow::Error::new(crate::client::http::HttpError::Status {
        status: reqwest::StatusCode::CONFLICT,
        body: "temporary failure".to_owned(),
    });
    let text_only = anyhow::anyhow!("HTTP status 404: missing lease");

    assert!(is_job_not_found(&not_found));
    assert!(!is_job_not_found(&conflict));
    assert!(!is_job_not_found(&text_only));
}

#[test]
fn diagnostic_signed_url_reads_only_official_response_field() {
    let cases = [
        (
            "official diag_logs_url",
            serde_json::json!({"diag_logs_url": "https://blob.example/diagnostics.zip"}),
            Some("https://blob.example/diagnostics.zip"),
        ),
        (
            "legacy blob_url is ignored",
            serde_json::json!({"blob_url": "https://blob.example/legacy.zip"}),
            None,
        ),
        (
            "legacy url is ignored",
            serde_json::json!({"url": "https://blob.example/legacy.zip"}),
            None,
        ),
        (
            "legacy logs_url is ignored",
            serde_json::json!({"logs_url": "https://blob.example/legacy.zip"}),
            None,
        ),
        (
            "official field wins over legacy aliases",
            serde_json::json!({
                "diag_logs_url": "https://blob.example/diagnostics.zip",
                "blob_url": "https://blob.example/legacy-blob.zip",
                "url": "https://blob.example/legacy-url.zip",
                "logs_url": "https://blob.example/legacy-logs.zip"
            }),
            Some("https://blob.example/diagnostics.zip"),
        ),
        (
            "non-string official field is rejected",
            serde_json::json!({"diag_logs_url": 42}),
            None,
        ),
    ];

    for (name, response, expected) in cases {
        assert_eq!(diagnostic_logs_url(&response), expected, "{name}");
    }
}

#[tokio::test]
async fn diagnostic_signed_url_uses_official_receiver_endpoint() {
    let (base_url, request_rx) = serve_diagnostic_signed_url().await;
    let http = crate::client::http::HttpClient::new(None).unwrap();
    let client = crate::client::results::ResultsClient::new(http, base_url);

    let response = client
        .get_diagnostic_logs_signed_url(
            "test-token",
            &serde_json::json!({
                "workflow_run_backend_id": "plan-1",
                "workflow_job_run_backend_id": "job-1"
            }),
        )
        .await
        .unwrap();

    assert_eq!(
        diagnostic_logs_url(&response),
        Some("https://blob.example/diagnostics.zip")
    );
    let request = request_rx.await.unwrap();
    assert!(request.starts_with(
        "POST /twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL HTTP/1.1"
    ));
}

// --- Lease parity: first-renew gate + TaskResult.Abandoned (JobDispatcher) ---

/// Scriptable mock control server.
///
/// POST `/renewjob` responds with `renew_statuses` in order (the last entry
/// repeats); every other request gets 200/204 with `{}`. Records
/// `(request-line, path, body)` for every request so tests can assert on the
/// completejob wire body and on which requests ever happened.
async fn serve_scripted_control(
    renew_statuses: Vec<u16>,
) -> (String, Arc<Mutex<Vec<(String, String, String)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let renew_statuses = Arc::new(Mutex::new(renew_statuses));
    let requests: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let requests_w = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let renew_statuses = renew_statuses.clone();
            let requests = requests_w.clone();
            tokio::spawn(async move {
                let mut buf = vec![0_u8; 16384];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                let request_line = raw.lines().next().unwrap_or("").to_string();
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                requests
                    .lock()
                    .await
                    .push((request_line, path.clone(), body));
                let status: u16 = if path == "/renewjob" {
                    let mut statuses = renew_statuses.lock().await;
                    if statuses.is_empty() {
                        200
                    } else if statuses.len() == 1 {
                        statuses[0]
                    } else {
                        statuses.remove(0)
                    }
                } else {
                    200
                };
                let reason = if status == 204 { "No Content" } else { "OK" };
                let body = if status == 204 { "" } else { "{}" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), requests)
}

fn control_payload(addr: &str, work_dir: &str, steps: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jobId": "job-lease-1",
        "jobDisplayName": "Lease Job",
        "plan": {"planId": "plan-lease-1"},
        "resources": {
            "endpoints": [{
                "name": "SystemVssConnection",
                "url": addr,
                "authorization": {"parameters": {"AccessToken": "test-token"}}
            }]
        },
        "steps": steps,
        "fileTable": {
            "workDirectory": work_dir
        }
    })
}

fn fast_timing() -> LeaseTiming {
    LeaseTiming {
        first_renew_backoff: RenewBackoff::Fixed(Duration::from_millis(1)),
        renew_interval: Duration::from_millis(10),
    }
}

/// Create a workspace directory strictly inside the test process's cwd — the
/// "runner root" for in-process `run_job` tests — so `setup_workspace`'s
/// containment check accepts it (job_extension rejects payloads whose
/// workDirectory escapes the runner root). Returns the TempDir (kept alive
/// for the test so the directory is auto-cleaned) and the workDirectory
/// string for the job payload.
fn contained_workspace() -> (tempfile::TempDir, String) {
    let dir = tempfile::Builder::new()
        .prefix("preloop-test-ws-")
        .tempdir_in(std::env::current_dir().expect("test cwd"))
        .expect("tempdir under cwd");
    // Two components below the tempdir so setup_workspace's derived _temp,
    // _actions and _tool dirs also land inside the tempdir.
    let work = dir.path().join("_work").join("work");
    (dir, work.to_string_lossy().into_owned())
}

fn recorded_completion(requests: &[(String, String, String)]) -> Option<&(String, String, String)> {
    requests
        .iter()
        .find(|(_, path, _)| path.ends_with("/completejob"))
}

#[tokio::test]
async fn first_renew_gate_404_abandons_without_running_steps() {
    let (_ws, work_dir) = contained_workspace();
    let (addr, requests) = serve_scripted_control(vec![404]).await;
    let payload = control_payload(
        &addr,
        &work_dir,
        serde_json::json!([{
            "id": "step-1",
            "contextName": "step1",
            "displayName": "Step One",
            "run": "echo step-one-executed",
            "shell": "bash"
        }]),
    );
    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    assert!(res.is_ok(), "run_job failed: {res:?}");

    let reqs = requests.lock().await.clone();
    // One renew attempt (404 is terminal — no retries), then the job is
    // completed as Abandoned with zero steps executed.
    let renews = reqs
        .iter()
        .filter(|(_, path, _)| path.ends_with("/renewjob"))
        .count();
    assert_eq!(renews, 1, "404 must not be retried");
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"abandoned\""),
        "completejob body: {}",
        complete.2
    );
    assert!(
        !complete.2.contains("step-1"),
        "no user step may run before the first renewal: {}",
        complete.2
    );
}

#[tokio::test]
async fn first_renew_gate_abandons_after_retry_budget() {
    let (_ws, work_dir) = contained_workspace();
    // Six failures = initial attempt + official `firstRenewRetryLimit` (5)
    // retries of ~10 s each; the sixth exhausts the budget.
    let (addr, requests) = serve_scripted_control(vec![500; 6]).await;
    let payload = control_payload(
        &addr,
        &work_dir,
        serde_json::json!([{
            "id": "step-1",
            "contextName": "step1",
            "displayName": "Step One",
            "run": "echo step-one-executed",
            "shell": "bash"
        }]),
    );
    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(payload, ProtocolPath::Broker, cancel_rx, fast_timing()).await;
    assert!(res.is_ok(), "run_job failed: {res:?}");

    let reqs = requests.lock().await.clone();
    let renews = reqs
        .iter()
        .filter(|(_, path, _)| path.ends_with("/renewjob"))
        .count();
    // Six logical gate failures (initial attempt + the official
    // firstRenewRetryLimit of 5); each 500 is retried 3× on the wire by the
    // P1.7 client policy (2 s + 4 s backoff), so 6 × 3 = 18 POSTs.
    assert_eq!(
        renews, 18,
        "retry budget is 5 retries after the first attempt"
    );
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"abandoned\""),
        "completejob body: {}",
        complete.2
    );
    assert!(
        !complete.2.contains("step-1"),
        "no user step may run before the first renewal: {}",
        complete.2
    );
}

#[tokio::test]
async fn first_renew_gate_cancellation_completes_canceled() {
    let (_ws, work_dir) = contained_workspace();
    let (addr, requests) = serve_scripted_control(vec![500]).await;
    let payload = control_payload(
        &addr,
        &work_dir,
        serde_json::json!([{
            "id": "step-1",
            "contextName": "step1",
            "displayName": "Step One",
            "run": "echo step-one-executed",
            "shell": "bash"
        }]),
    );
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let handle = tokio::spawn(run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        fast_timing(),
    ));
    // Let the gate start failing, then cancel — the official completes a job
    // cancelled before the first renewal as Canceled, steps never run.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = cancel_tx.send(true);
    handle.await.unwrap().unwrap();

    let reqs = requests.lock().await.clone();
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"canceled\""),
        "completejob body: {}",
        complete.2
    );
    assert!(
        !complete.2.contains("step-1"),
        "no user step may run before the first renewal: {}",
        complete.2
    );
}

#[tokio::test]
async fn mid_job_lease_loss_reports_abandoned() {
    let (_ws, work_dir) = contained_workspace();
    // Gate renew succeeds (200), the loop's first renew succeeds (200), then
    // the server forgets the job (404) mid-step → the worker must cancel the
    // steps and complete as Abandoned, not Failed.
    let (addr, requests) = serve_scripted_control(vec![200, 200, 404]).await;
    let payload = control_payload(
        &addr,
        &work_dir,
        serde_json::json!([{
            "id": "step-1",
            "contextName": "step1",
            "displayName": "Long Step",
            "run": "sleep 30",
            "shell": "bash"
        }]),
    );
    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(payload, ProtocolPath::Broker, cancel_rx, fast_timing()).await;
    assert!(res.is_ok(), "run_job failed: {res:?}");

    let reqs = requests.lock().await.clone();
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"abandoned\""),
        "mid-job lease loss must complete as abandoned: {}",
        complete.2
    );
    // Unlike the gate path, the job had started — the abandoned completion
    // carries the step's (cancelled) record.
    assert!(
        complete.2.contains("step-1"),
        "started steps are still recorded: {}",
        complete.2
    );
}

/// Serve a control server whose `/renewjob` endpoint accepts connections but
/// never responds (a stalled first-renew HTTP request — the server is
/// unreachable, not failing), while every other request completes normally
/// and is recorded. Records `(request-line, path, body)` like
/// [`serve_scripted_control`].
async fn serve_stalling_renew() -> (String, Arc<Mutex<Vec<(String, String, String)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests: Arc<Mutex<Vec<(String, String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let requests_w = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let requests = requests_w.clone();
            tokio::spawn(async move {
                let mut buf = vec![0_u8; 16384];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let raw = String::from_utf8_lossy(&buf[..n]).to_string();
                let request_line = raw.lines().next().unwrap_or("").to_string();
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("")
                    .to_string();
                let body = raw.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
                requests
                    .lock()
                    .await
                    .push((request_line, path.clone(), body));
                if path == "/renewjob" {
                    // Stall: hold the connection open without responding so
                    // the renew request hangs instead of failing fast.
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    return;
                }
                let response = "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n";
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (format!("http://{addr}"), requests)
}

#[tokio::test]
async fn first_renew_cancel_preempts_stalled_renew_http() {
    // A cancel arriving while the first renewjob request hangs (server
    // unreachable — the request never completes) must win the race: the
    // worker completes the job as Canceled without waiting out the stalled
    // HTTP call. On the broken code the gate awaits renew_job to completion
    // before looking at cancel_rx, so the job hangs far past the 45s kill
    // window and the worker is killed without ever reporting.
    let (_ws, work_dir) = contained_workspace();
    let (addr, requests) = serve_stalling_renew().await;
    let payload = control_payload(
        &addr,
        &work_dir,
        serde_json::json!([{
            "id": "step-1",
            "contextName": "step1",
            "displayName": "Step One",
            "run": "echo step-one-executed",
            "shell": "bash"
        }]),
    );
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let handle = tokio::spawn(run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        fast_timing(),
    ));
    // Let the gate's renew request reach the stalled server, then cancel.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = cancel_tx.send(true);

    tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("cancel must preempt the stalled first-renew request")
        .expect("run_job panicked")
        .expect("run_job failed");

    let reqs = requests.lock().await.clone();
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"canceled\""),
        "a job cancelled during the first-renew gate completes as canceled: {}",
        complete.2
    );
    assert!(
        !complete.2.contains("step-1"),
        "no user step may run before the first renewal: {}",
        complete.2
    );
}

#[tokio::test]
async fn first_renew_gate_failure_does_not_clear_existing_workspace() {
    // The first-renew gate must run BEFORE any workspace setup: a job whose
    // lease is invalid (renew 404 → abandoned) must not wipe an existing
    // checkout that the runner does not own yet. On the broken code
    // setup_workspace (remove_dir_all) runs first, destroying the marker.
    let (_ws, work_dir) = contained_workspace();
    let marker = std::path::Path::new(&work_dir).join("marker.txt");
    std::fs::create_dir_all(std::path::Path::new(&work_dir)).unwrap();
    std::fs::write(&marker, "fresh checkout").unwrap();

    let (addr, requests) = serve_scripted_control(vec![404]).await;
    let payload = control_payload(&addr, &work_dir, serde_json::json!([]));
    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(
        payload,
        ProtocolPath::Broker,
        cancel_rx,
        LeaseTiming::default(),
    )
    .await;
    assert!(res.is_ok(), "run_job failed: {res:?}");

    let reqs = requests.lock().await.clone();
    let complete = recorded_completion(&reqs).expect("completejob was reported");
    assert!(
        complete.2.contains("\"conclusion\":\"abandoned\""),
        "invalid lease must abandon the job: {}",
        complete.2
    );
    assert!(
        marker.exists(),
        "workspace must not be cleared before the lease is validated"
    );
}

#[test]
fn live_token_starts_with_the_initial_value() {
    let token = super::LiveToken::new("initial".to_string());
    assert_eq!(token.get(), "initial");
    assert!(!token.due_for_refresh(), "no deadline means never due");
}

#[test]
fn live_token_update_publishes_for_all_readers() {
    let token = super::LiveToken::new("initial".to_string());
    let clone = token.clone();
    token.update(
        "fresh".to_string(),
        Some(std::time::Instant::now() + std::time::Duration::from_secs(60)),
    );
    assert_eq!(
        clone.get(),
        "fresh",
        "update must be visible through clones"
    );
    assert!(!clone.due_for_refresh(), "fresh token is not due yet");
}

#[test]
fn live_token_is_due_after_the_refresh_deadline() {
    let token = super::LiveToken::new("initial".to_string());
    token.update(
        "fresh".to_string(),
        Some(std::time::Instant::now() - std::time::Duration::from_secs(1)),
    );
    assert!(token.due_for_refresh(), "past deadline must be due");
}

#[test]
fn unauthorized_error_is_detected_for_token_expiry() {
    let error = anyhow::anyhow!(crate::client::http::HttpError::Status {
        status: reqwest::StatusCode::UNAUTHORIZED,
        body: "runner or job protocol token required".to_string(),
    });
    assert!(super::is_unauthorized(&error));

    let not_found = anyhow::anyhow!(crate::client::http::HttpError::Status {
        status: reqwest::StatusCode::NOT_FOUND,
        body: "gone".to_string(),
    });
    assert!(!super::is_unauthorized(&not_found));
}
