//! Worker — executes a single job received from the listener.
//!
//! Reads the first stdin line (a job message), then runs the job while
//! concurrently reading stdin for cancel messages. This mirrors Worker.cs:
//! the listener can send `{"t":"cancel"}` at any time during execution.

pub mod action_preparation;
pub mod actions;
pub mod commands;
pub mod completion;
pub mod container_ops;
pub mod contexts;
pub mod execution_context;
pub mod execution_types;
pub mod file_commands;
pub mod handlers;
pub mod helpers;
pub mod job_extension;
pub mod job_runner;
pub mod live_logs;
pub mod matchers;
pub mod official_oracles;
pub mod reporting;
pub mod server_queue;
pub mod step_conditions;
pub mod step_records;
pub mod steps_runner;
pub mod template;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::cli::WorkerArgs;

/// IPC message from listener.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "t")]
pub enum ListenerMessage {
    /// A job to execute.
    #[serde(rename = "job")]
    Job {
        /// The full job message payload.
        body: serde_json::Value,
    },
    /// Cancel the currently running job.
    #[serde(rename = "cancel")]
    Cancel {
        /// Seconds before hard-kill.
        timeout_secs: u64,
    },
    /// Shut down the worker process.
    #[serde(rename = "shutdown")]
    Shutdown,
}

/// Entry point for the hidden `worker` subcommand.
pub async fn run_worker(args: WorkerArgs) -> Result<()> {
    // Read the first line synchronously — must be a job message.
    let mut first_line = String::new();
    std::io::stdin()
        .read_line(&mut first_line)
        .context("reading job message from stdin")?;

    let msg: ListenerMessage =
        serde_json::from_str(first_line.trim()).context("parsing listener message")?;

    let job_body = match msg {
        ListenerMessage::Job { body } => body,
        ListenerMessage::Shutdown => {
            info!("Worker received shutdown");
            return Ok(());
        }
        ListenerMessage::Cancel { .. } => {
            info!("Worker received cancel before any job — exiting");
            return Ok(());
        }
    };

    info!("Worker received job");

    // Create a cancellation signal channel.
    // The stdin reader sends `true` when a cancel/shutdown message arrives.
    let (cancel_tx, cancel_rx) = watch::channel(false);

    // Spawn a plain blocking thread that reads stdin for cancel/shutdown messages.
    // Tokio stdin reads are not cleanly cancellable on all platforms and can
    // keep the worker runtime alive after the job is done.
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        loop {
            let mut line = String::new();
            match stdin.read_line(&mut line) {
                Ok(0) => return,
                Ok(_) => {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<ListenerMessage>(&line) {
                        Ok(ListenerMessage::Cancel { timeout_secs }) => {
                            info!("Worker received cancel (timeout: {timeout_secs}s)");
                            let _ = cancel_tx.send(true);
                            return;
                        }
                        Ok(ListenerMessage::Shutdown) => {
                            info!("Worker received shutdown");
                            let _ = cancel_tx.send(true);
                            return;
                        }
                        Ok(ListenerMessage::Job { .. }) => {
                            warn!("Worker received second job message — ignoring");
                        }
                        Err(e) => {
                            warn!("Worker received unparseable stdin line: {e}");
                        }
                    }
                }
                Err(e) => {
                    warn!("Worker stdin reader failed: {e}");
                    return;
                }
            }
        }
    });

    // Run the job with cancellation support.
    job_runner::run_job(job_body, args.via, cancel_rx).await
}
