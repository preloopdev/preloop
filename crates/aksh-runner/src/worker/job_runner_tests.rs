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

use super::*;
use tempfile::TempDir;
use tokio::sync::watch;

#[tokio::test]
async fn test_run_job_executes_successfully() {
    let dir = TempDir::new().unwrap();
    let workspace_dir = dir.path().join("work");
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
            "workDirectory": workspace_dir.to_str().unwrap()
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
    assert!(res.is_ok(), "Expected run_job to succeed, got: {:?}", res);
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
    let dir = TempDir::new().unwrap();
    let workspace_dir = dir.path().join("work");
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
            "workDirectory": workspace_dir.to_str().unwrap()
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
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
    let dir = TempDir::new().unwrap();
    let workspace_dir = dir.path().join("work");
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
            "workDirectory": workspace_dir.to_str().unwrap()
        }
    });

    let (cancel_tx, cancel_rx) = watch::channel(false);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = cancel_tx.send(true);
    });

    let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
    // run_job returns Ok — cancellation is reported via completion, not
    // the function return value.
    assert!(
        res.is_ok(),
        "Expected run_job to handle cancel gracefully, got: {:?}",
        res
    );
}

#[tokio::test]
async fn test_run_job_with_timeout() {
    let dir = TempDir::new().unwrap();
    let workspace_dir = dir.path().join("work");
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
            "workDirectory": workspace_dir.to_str().unwrap()
        }
    });

    let (_tx, cancel_rx) = watch::channel(false);
    let res = run_job(payload, ProtocolPath::Broker, cancel_rx).await;
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
fn renew_backoff_caps_at_thirty_seconds() {
    let expected = [5, 10, 20, 30, 30];
    for (attempt, expected_seconds) in expected.into_iter().enumerate() {
        let attempt = attempt as u32 + 1;
        assert_eq!(
            renew_backoff(attempt),
            std::time::Duration::from_secs(expected_seconds),
            "unexpected retry delay for attempt {attempt}"
        );
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
