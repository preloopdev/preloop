//! Listener — polls for jobs and dispatches them to worker processes.

pub mod broker_listener;
pub mod job_dispatcher;
pub mod message_listener;
pub mod oauth;

use anyhow::{Context, Result};
use tracing::info;

use crate::cli::{GlobalArgs, ProtocolPath, RunArgs};
use crate::client::http::HttpClient;
use crate::control_bridge;
use crate::settings::RunnerConfig;

/// Run the listener (main loop of `aksh-runner run`).
pub async fn run_listener(args: RunArgs, global: &GlobalArgs) -> Result<()> {
    let root = global.runner_root();
    let config = RunnerConfig::load(&root)
        .context("loading runner configuration — run `aksh-runner configure` first")?;

    info!(
        "Starting runner '{}' (agent {}, pool {})",
        config.settings.agent_name, config.settings.agent_id, config.settings.pool_id
    );

    // Held for the lifetime of the listener so job subprocesses — `git` inside
    // `actions/checkout`, Node actions, `curl` in a step — can reach the
    // control plane at the origin it advertises.
    let _control_bridge = control_bridge::spawn_from_env().await;

    let http = HttpClient::new(global.ca_bundle.as_deref())?;

    // Get OAuth token (with expiry for proactive refresh in the broker loop)
    let (token, expires_at) = oauth::get_oauth_token(&http, &config).await?;

    // Create session and start listening
    match args.via {
        ProtocolPath::Broker => {
            broker_listener::run_broker_loop(&http, &config, &token, expires_at, args.once, &root)
                .await
        }
        ProtocolPath::Azdo => {
            message_listener::run_message_loop(&http, &config, &token, args.once, &root).await
        }
    }
}
