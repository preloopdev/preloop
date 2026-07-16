//! Job dispatcher — spawns worker processes for incoming jobs.
//!
//! Mirrors `JobDispatcher.cs` from the official runner: one job at a time,
//! spawned as `aksh-runner worker` child process communicating via stdin NDJSON.
//!
//! The listener keeps polling for messages while the worker runs (with status=Busy),
//! so JobCancellation can arrive mid-job and be forwarded to the worker.

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Child;
use tracing::{error, info, warn};

use crate::cli::ProtocolPath;

/// IPC message types sent from listener to worker via stdin.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "t")]
enum WorkerMessage {
    /// A job to execute.
    #[serde(rename = "job")]
    Job { body: serde_json::Value },
    /// Cancel the currently running job.
    #[serde(rename = "cancel")]
    Cancel { timeout_secs: u64 },
    /// Shut down the worker process.
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Handle to a running worker process.
pub struct RunningJob {
    /// The worker child process.
    child: Child,
    /// Stdin handle — kept open so we can write cancel messages.
    stdin: Option<tokio::process::ChildStdin>,
    /// The job/request ID for matching cancellation messages.
    pub request_id: String,
}

impl RunningJob {
    /// Check if the worker has finished (non-blocking).
    pub fn try_wait(&mut self) -> Result<Option<bool>> {
        match self.child.try_wait() {
            Ok(Some(status)) => Ok(Some(status.success())),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Wait for the worker to finish.
    pub async fn wait(&mut self) -> Result<bool> {
        let status = self.child.wait().await.context("waiting for worker")?;
        Ok(status.success())
    }

    /// Send a cancel message to the worker via stdin.
    /// The worker's stdin reader task picks this up and signals cancellation.
    pub async fn cancel(&mut self, timeout_secs: u64) {
        if let Some(stdin) = &mut self.stdin {
            let msg = WorkerMessage::Cancel { timeout_secs };
            if let Ok(line) = serde_json::to_string(&msg) {
                let _ = stdin.write_all(line.as_bytes()).await;
                let _ = stdin.write_all(b"\n").await;
                let _ = stdin.flush().await;
            }
        }
    }

    /// Hard-kill the worker process group (after cancel timeout expires).
    pub async fn kill(&mut self) {
        // Drop stdin first to unblock the worker's reader
        self.stdin.take();
        if let Err(e) = self.child.kill().await {
            warn!("Failed to kill worker: {e}");
        }
    }
}

/// Spawn a worker process for a job. Returns immediately without waiting.
pub async fn spawn_job(
    job_message: serde_json::Value,
    runner_root: &Path,
    via: ProtocolPath,
) -> Result<RunningJob> {
    let request_id = job_message
        .get("jobId")
        .or_else(|| job_message.get("requestId"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    info!("Dispatching job {request_id} to worker");

    #[cfg(test)]
    let mut command = {
        static WORKER_BIN: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        let binary = WORKER_BIN.get_or_init(|| {
            let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
            let status = std::process::Command::new("cargo")
                .args(["build", "--quiet", "--manifest-path"])
                .arg(&manifest)
                .args(["--bin", "aksh-runner"])
                .status()
                .expect("build aksh-runner test worker");
            assert!(status.success(), "building aksh-runner test worker failed");
            if let Ok(target_dir_env) = std::env::var("CARGO_TARGET_DIR") {
                std::path::PathBuf::from(target_dir_env).join("debug/aksh-runner")
            } else {
                manifest
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join("target/debug/aksh-runner")
            }
        });
        let mut command = tokio::process::Command::new(binary);
        command.arg("worker");
        command
    };
    #[cfg(not(test))]
    let mut command = {
        let raw_exe = std::env::current_exe().context("finding current executable")?;
        let current_exe = if let Ok(bin) = std::env::var("CARGO_BIN_EXE_aksh-runner") {
            let p = std::path::PathBuf::from(bin);
            if p.exists() && p.file_name().unwrap() == "aksh-runner" {
                p
            } else {
                raw_exe
            }
        } else if let Ok(bin) = std::env::var("AKSH_RUNNER_BIN") {
            std::path::PathBuf::from(bin)
        } else {
            let target_dir = raw_exe.parent().unwrap();
            let aksh_bin = if target_dir.file_name().unwrap() == "deps" {
                target_dir.parent().unwrap().join("aksh-runner")
            } else {
                target_dir.join("aksh-runner")
            };
            if aksh_bin.exists() {
                aksh_bin
            } else {
                raw_exe
            }
        };
        let mut command = tokio::process::Command::new(current_exe);
        command.arg("worker");
        command
    };
    let via_str = match via {
        ProtocolPath::Broker => "broker",
        ProtocolPath::Azdo => "azdo",
    };
    let mut child = command
        .arg("--via")
        .arg(via_str)
        .current_dir(runner_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning worker process")?;

    // Send job message via stdin — but keep stdin open for cancel messages
    let mut stdin = child.stdin.take();
    if let Some(s) = &mut stdin {
        let msg = WorkerMessage::Job { body: job_message };
        let line = serde_json::to_string(&msg)?;
        s.write_all(line.as_bytes()).await?;
        s.write_all(b"\n").await?;
        s.flush().await?;
        // Do NOT drop stdin — worker needs it open for cancel messages
    }

    Ok(RunningJob {
        child,
        stdin,
        request_id,
    })
}

/// Blocking dispatch — spawns and waits. Used by the AzDO message listener
/// which doesn't need concurrent polling.
pub async fn dispatch_job(
    job_message: serde_json::Value,
    runner_root: &Path,
    via: ProtocolPath,
) -> Result<()> {
    let mut job = spawn_job(job_message, runner_root, via).await?;
    let request_id = job.request_id.clone();

    let success = job.wait().await?;
    if success {
        info!("Worker completed job {request_id} successfully");
    } else {
        error!("Worker failed for job {request_id}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_worker_dispatch_run_new_job() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-worker-1",
            "jobDisplayName": "Worker Job",
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

        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();

        let success = running.wait().await.unwrap();
        assert!(success);
    }

    #[tokio::test]
    async fn test_worker_dispatch_cancellation() {
        let dir = TempDir::new().unwrap();
        let workspace_dir = dir.path().join("work");
        let payload = serde_json::json!({
            "jobId": "job-worker-cancel",
            "jobDisplayName": "Cancel Job",
            "steps": [
                {
                    "id": "step-1",
                    "contextName": "step1",
                    "displayName": "Step One",
                    "run": "sleep 60",
                    "shell": "bash"
                }
            ],
            "fileTable": {
                "workDirectory": workspace_dir.to_str().unwrap()
            }
        });

        let start = Instant::now();
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();

        // Let it start up
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Cancel the job
        running.cancel(5).await;

        let success = running.wait().await.unwrap();
        let elapsed = start.elapsed();

        // The job should succeed/exit (with Ok status since cancellation is handled gracefully)
        assert!(success);
        // The worker intentionally gives the child process group SIGINT then
        // SIGTERM before SIGKILL; allow both test grace periods plus startup
        // scheduling overhead while still rejecting the ten-second payload.
        assert!(
            elapsed.as_secs() < 20,
            "Expected cancellation to exit within the grace budget, took {:?}",
            elapsed
        );
    }

    // --- P1 job dispatcher gap coverage ---

    #[test]
    fn worker_message_job_serialization() {
        let msg = WorkerMessage::Job {
            body: serde_json::json!({"jobId": "test-1", "steps": []}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"job\""));
        assert!(json.contains("\"body\""));
        assert!(json.contains("test-1"));
    }

    #[test]
    fn worker_message_cancel_serialization() {
        let msg = WorkerMessage::Cancel { timeout_secs: 300 };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"cancel\""));
        assert!(json.contains("300"));
    }

    #[test]
    fn worker_message_shutdown_serialization() {
        let msg = WorkerMessage::Shutdown;
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"t\":\"shutdown\""));
    }

    #[test]
    fn worker_message_roundtrip_all_types() {
        // Job
        let job = WorkerMessage::Job {
            body: serde_json::json!({"k": "v"}),
        };
        let j: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
        assert_eq!(j["t"], "job");
        assert_eq!(j["body"]["k"], "v");

        // Cancel
        let cancel = WorkerMessage::Cancel { timeout_secs: 60 };
        let c: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&cancel).unwrap()).unwrap();
        assert_eq!(c["t"], "cancel");
        assert_eq!(c["timeout_secs"], 60);

        // Shutdown
        let shutdown = WorkerMessage::Shutdown;
        let s: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&shutdown).unwrap()).unwrap();
        assert_eq!(s["t"], "shutdown");
    }

    #[tokio::test]
    async fn spawn_job_extracts_request_id_from_job_id() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "jobId": "my-unique-job-42",
            "jobDisplayName": "Test",
            "steps": [],
            "fileTable": {
                "workDirectory": dir.path().join("work").to_str().unwrap()
            }
        });
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        assert_eq!(running.request_id, "my-unique-job-42");
        running.kill().await;
    }

    #[tokio::test]
    async fn spawn_job_extracts_request_id_from_fallback() {
        let dir = TempDir::new().unwrap();
        let payload = serde_json::json!({
            "requestId": "fallback-id",
            "jobDisplayName": "Test",
            "steps": [],
            "fileTable": {
                "workDirectory": dir.path().join("work").to_str().unwrap()
            }
        });
        let mut running = spawn_job(payload, dir.path(), ProtocolPath::Broker)
            .await
            .unwrap();
        assert_eq!(running.request_id, "fallback-id");
        running.kill().await;
    }
}
