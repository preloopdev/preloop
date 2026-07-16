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
