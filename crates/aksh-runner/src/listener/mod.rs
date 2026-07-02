//! Listener — polls for jobs and dispatches them to worker processes.

pub mod broker_listener;
pub mod job_dispatcher;
pub mod message_listener;
pub mod oauth;

use anyhow::{Context, Result};
use tracing::info;

use crate::cli::{GlobalArgs, ProtocolPath, RunArgs};
use crate::client::http::HttpClient;
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

    let http = HttpClient::new(global.ca_bundle.as_deref())?;

    // Get OAuth token
    let token = oauth::get_oauth_token(&http, &config).await?;

    // Create session and start listening
    match args.via {
        ProtocolPath::Broker => {
            broker_listener::run_broker_loop(&http, &config, &token, args.once, &root).await
        }
        ProtocolPath::Azdo => {
            message_listener::run_message_loop(&http, &config, &token, args.once, &root).await
        }
    }
}
