//! Broker message listener (GitHub-current path).
//!
//! Keeps polling for messages even while a job runs (with status=Busy),
//! matching the official runner's JobDispatcher.cs behavior. This allows
//! JobCancellation to arrive mid-job.

use anyhow::{Context, Result};
use base64::Engine;
use tracing::{debug, info, warn};

use crate::client::broker::BrokerClient;
use crate::client::http::HttpClient;
use crate::listener::job_dispatcher::{self, RunningJob};
use crate::settings::RunnerConfig;

/// Run the broker message polling loop.
pub async fn run_broker_loop(
    http: &HttpClient,
    config: &RunnerConfig,
    initial_token: &str,
    once: bool,
    runner_root: &std::path::Path,
) -> Result<()> {
    // P1.1: Derive broker URL from settings.server_url_v2 (extracted from agent
    // response properties.ServerUrlV2 at configure time). This is
    // "https://broker.actions.githubusercontent.com/" for github.com, and the
    // server's own URL for local aksh / self-hosted instances.
    // Fall back to server_url if server_url_v2 is absent (pre-P1.1 configs).
    let broker_url = config
        .settings
        .server_url_v2
        .clone()
        .unwrap_or_else(|| config.settings.server_url.clone())
        .trim_end_matches('/')
        .to_string();
    let client = BrokerClient::new(http.clone(), broker_url);

    let mut token = initial_token.to_string();
    let mut session_id = String::new();
    let mut session_key: Option<Vec<u8>> = None;

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut processed_message_ids: std::collections::HashSet<i64> =
        std::collections::HashSet::new();
    let mut consecutive_errors: u32 = 0;
    let mut active_job: Option<RunningJob> = None;

    // We start in a "need session" state.
    let mut need_session = true;

    loop {
        // Check if active job has finished (non-blocking)
        if let Some(job) = &mut active_job {
            match job.try_wait() {
                Ok(Some(success)) => {
                    let id = &job.request_id;
                    if success {
                        info!("Worker completed job {id} successfully");
                    } else {
                        warn!("Worker failed for job {id}");
                    }
                    if once || config.settings.ephemeral {
                        info!("exiting after first job");
                        if config.settings.ephemeral {
                            ephemeral_unregister(http, config, &token).await;
                        }
                        if !session_id.is_empty() {
                            let _ = client.delete_session(&token, &session_id).await;
                        }
                        return Ok(());
                    }
                    active_job = None;
                }
                Ok(None) => {} // still running
                Err(e) => {
                    warn!("Error checking worker status: {e:#}");
                    active_job = None;
                }
            }
        }

        // If we need a new session, establish it here
        if need_session {
            info!("Establishing broker session...");
            let session_body = serde_json::json!({
                "sessionId": uuid::Uuid::new_v4().to_string(),
                "ownerName": format!("{} (PID: {})", hostname::get()
                    .ok()
                    .and_then(|h| h.into_string().ok())
                    .unwrap_or_else(|| "aksh-runner".to_string()),
                    std::process::id()),
                "agent": {
                    "id": config.settings.agent_id,
                    "name": config.settings.agent_name,
                },
                "useFipsEncryption": false,
            });

            match client.create_session(&token, &session_body).await {
                Ok(session_resp) => {
                    if let Some(sid) = session_resp.get("sessionId").and_then(|v| v.as_str()) {
                        session_id = sid.to_string();
                        session_key = extract_session_key_if_present(&session_resp, config);
                        info!("Broker session created: {session_id}");
                        need_session = false;
                        consecutive_errors = 0;
                    } else {
                        warn!("Session response missing sessionId");
                        consecutive_errors += 1;
                        let delay = std::cmp::min(consecutive_errors * 5, 60);
                        tokio::time::sleep(std::time::Duration::from_secs(delay as u64)).await;
                    }
                }
                Err(e) => {
                    if is_unauthorized(&e) {
                        info!("OAuth token expired during session creation. Re-acquiring token...");
                        match crate::listener::oauth::get_oauth_token(http, config).await {
                            Ok(t) => {
                                token = t;
                                consecutive_errors = 0;
                            }
                            Err(oe) => {
                                warn!("Failed to re-acquire OAuth token: {oe:#}");
                                consecutive_errors += 1;
                                let delay = std::cmp::min(consecutive_errors * 5, 60);
                                tokio::time::sleep(std::time::Duration::from_secs(delay as u64))
                                    .await;
                            }
                        }
                    } else {
                        consecutive_errors += 1;
                        let delay = std::cmp::min(consecutive_errors * 5, 60);
                        warn!("Failed to create broker session: {e:#}. Retrying in {delay}s");
                        tokio::time::sleep(std::time::Duration::from_secs(delay as u64)).await;
                    }
                    continue;
                }
            }
        }

        let busy = active_job.is_some();

        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received");
                if let Some(mut job) = active_job.take() {
                    info!("Killing active worker");
                    job.kill().await;
                }
                if once && config.settings.ephemeral {
                    ephemeral_unregister(http, config, &token).await;
                }
                if !session_id.is_empty() {
                    let _ = client.delete_session(&token, &session_id).await;
                }
                return Ok(());
            }
            result = client.get_message(&token, &session_id, busy) => {
                match result {
                    Ok(Some(msg)) => {
                        consecutive_errors = 0;
                        let message_id = msg.get("messageId").and_then(|v| v.as_i64()).unwrap_or(0);

                        // In-memory dedup: skip already-processed messages
                        // (GitHub dequeues on acknowledge; aksh may re-deliver)
                        if !processed_message_ids.insert(message_id) {
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            continue;
                        }

                        let message_type = msg.get("messageType")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();

                        info!("Received broker message {message_id}: {message_type}");

                        // Parse body — decrypt if key present, else plaintext (F011)
                        let body = match parse_message_body(&msg, session_key.as_deref()) {
                            Ok(b) => b,
                            Err(e) => {
                                warn!("Failed to parse message body: {e:#}");
                                continue;
                            }
                        };

                        // Extract runner_request_id for acknowledge.
                        // Golden flow 12: message body uses snake_case `runner_request_id`
                        // Golden flow 13: acknowledge POST body uses camelCase `runnerRequestId`
                        let runner_request_id = body
                            .get("runner_request_id")
                            .or_else(|| body.get("runnerRequestId"))
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();

                        if !runner_request_id.is_empty() {
                            let _ = client.acknowledge(&token, &session_id, &runner_request_id).await;
                        }

                        match message_type.as_str() {
                            "RunnerJobRequest" => {
                                if active_job.is_some() {
                                    warn!("Received job while another is running — ignoring");
                                    continue;
                                }
                                let job = acquire_job_from_ref(&body, http, &token).await?;
                                if let Some(job_msg) = job {
                                    let running = job_dispatcher::spawn_job(
                                        job_msg,
                                        runner_root,
                                        crate::cli::ProtocolPath::Broker,
                                    ).await?;
                                    active_job = Some(running);
                                }
                            }
                            "PipelineAgentJobRequest" => {
                                if active_job.is_some() {
                                    warn!("Received job while another is running — ignoring");
                                    continue;
                                }
                                let running = job_dispatcher::spawn_job(
                                    body,
                                    runner_root,
                                    crate::cli::ProtocolPath::Azdo,
                                ).await?;
                                active_job = Some(running);
                            }
                            "JobCancellation" => {
                                if let Some(job) = &mut active_job {
                                    info!("Cancelling active job {}", job.request_id);
                                    // F031/P1.4: Graceful cancel first — sends IPC cancel
                                    // message so worker can run always()/post steps and
                                    // flush reporting. Hard kill after 5-minute grace period
                                    // (matching upstream's default cancel timeout).
                                    job.cancel(300).await;
                                    let grace = std::time::Duration::from_secs(300);
                                    match tokio::time::timeout(grace, job.wait()).await {
                                        Ok(_) => {
                                            info!("Worker exited gracefully after cancel");
                                        }
                                        Err(_) => {
                                            warn!("Worker did not exit within grace period — killing");
                                            job.kill().await;
                                        }
                                    }
                                    if once || config.settings.ephemeral {
                                        info!("exiting after cancelled job");
                                        if config.settings.ephemeral {
                                            ephemeral_unregister(http, config, &token).await;
                                        }
                                        let _ = client.delete_session(&token, &session_id).await;
                                        return Ok(());
                                    }
                                    active_job = None;
                                } else {
                                    debug!("Received cancellation but no active job");
                                }
                            }
                            "AgentRefresh" => {
                                info!("Self-update requested; aksh-runner does not self-update");
                            }
                            "BrokerMigration" => {
                                warn!("Broker migration requested — not yet implemented");
                            }
                            other => {
                                warn!("Unknown broker message type: {other}");
                            }
                        }
                    }
                    Ok(None) => {
                        consecutive_errors = 0;
                        debug!("Broker poll returned no message (idle cycle)");
                    }
                    Err(e) => {
                        if is_unauthorized(&e) {
                            info!("OAuth token expired during message poll. Re-acquiring token...");
                            match crate::listener::oauth::get_oauth_token(http, config).await {
                                Ok(t) => {
                                    token = t;
                                    consecutive_errors = 0;
                                    // Try to recreate the session with the new token
                                    need_session = true;
                                }
                                Err(oe) => {
                                    warn!("Failed to re-acquire OAuth token: {oe:#}");
                                    consecutive_errors += 1;
                                    let delay = std::cmp::min(consecutive_errors * 5, 60);
                                    tokio::time::sleep(std::time::Duration::from_secs(delay as u64)).await;
                                }
                            }
                        } else if is_session_expired(&e) {
                            warn!("Broker session expired or invalid. Re-creating session...");
                            need_session = true;
                        } else {
                            consecutive_errors += 1;
                            let delay = std::cmp::min(consecutive_errors * 5, 60);
                            warn!("Broker poll error ({consecutive_errors}): {e:#}. Retrying in {delay}s");
                            tokio::time::sleep(std::time::Duration::from_secs(delay as u64)).await;
                        }
                    }
                }
            }
        }
    }
}

fn is_unauthorized(err: &anyhow::Error) -> bool {
    if let Some(http_err) = err.downcast_ref::<crate::client::http::HttpError>() {
        match http_err {
            crate::client::http::HttpError::Status { status, .. } => {
                *status == reqwest::StatusCode::UNAUTHORIZED
            }
        }
    } else {
        false
    }
}

fn is_session_expired(err: &anyhow::Error) -> bool {
    if let Some(http_err) = err.downcast_ref::<crate::client::http::HttpError>() {
        match http_err {
            crate::client::http::HttpError::Status { status, .. } => {
                *status == reqwest::StatusCode::NOT_FOUND
                    || *status == reqwest::StatusCode::BAD_REQUEST
            }
        }
    } else {
        false
    }
}

/// P1.8: Unregister the agent on ephemeral (--once) exit.
async fn ephemeral_unregister(http: &HttpClient, config: &RunnerConfig, token: &str) {
    let delete_url = format!(
        "{}/_apis/distributedtask/pools/{}/agents/{}?api-version=6.0-preview",
        config.settings.server_url, config.settings.pool_id, config.settings.agent_id
    );
    if let Err(e) = http.delete_with_token(&delete_url, token).await {
        warn!("Failed to unregister agent: {e:#}");
    } else {
        info!("Agent unregistered (ephemeral --once cleanup)");
    }
}

/// F011: Extract session key only if present.
fn extract_session_key_if_present(
    session: &serde_json::Value,
    config: &RunnerConfig,
) -> Option<Vec<u8>> {
    let enc_key = session.get("encryptionKey")?;
    let value = enc_key.get("value").and_then(|v| v.as_str())?;
    if value.is_empty() {
        return None;
    }
    let key_bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    let encrypted = enc_key
        .get("encrypted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if encrypted {
        let keypair =
            aksh_gha_protocol::crypto::AgentRsaKeypair::from_rsaparams(&config.rsa_params).ok()?;
        keypair.unwrap_key(&key_bytes).ok()
    } else {
        Some(key_bytes)
    }
}

/// Parse message body — decrypt if session key present, else plaintext JSON.
fn parse_message_body(
    msg: &serde_json::Value,
    session_key: Option<&[u8]>,
) -> Result<serde_json::Value> {
    let body_val = msg.get("body");

    // If body is already a JSON object (plaintext broker path), return directly
    if let Some(body) = body_val {
        if body.is_object() || body.is_array() {
            return Ok(body.clone());
        }
    }

    let body_str = body_val.and_then(|v| v.as_str()).unwrap_or("{}");
    let iv_str = msg.get("iv").and_then(|v| v.as_str());

    // Decrypt if we have key + IV
    if let (Some(key), Some(iv)) = (session_key, iv_str) {
        if !iv.is_empty() {
            let body_bytes = base64::engine::general_purpose::STANDARD
                .decode(body_str)
                .context("base64 decode body")?;
            let iv_bytes = base64::engine::general_purpose::STANDARD
                .decode(iv)
                .context("base64 decode IV")?;
            let enc = aksh_gha_protocol::crypto::SessionEncryption::from_key(key.to_vec());
            let plain = enc
                .decrypt(&body_bytes, &iv_bytes)
                .map_err(|e| anyhow::anyhow!("decrypting: {e}"))?;
            return serde_json::from_str(&String::from_utf8(plain)?)
                .context("parsing decrypted body");
        }
    }

    serde_json::from_str(body_str).context("parsing plaintext body")
}

/// Acquire a full job from a RunnerJobRequest reference via run-service.
///
/// Golden flow 12: message body fields are snake_case:
///   `runner_request_id`, `run_service_url`, `billing_owner_id`, `should_acknowledge`
/// Golden flow 15: POST /{id}/acquirejob returns the full camelCase job payload.
async fn acquire_job_from_ref(
    job_ref: &serde_json::Value,
    http: &HttpClient,
    token: &str,
) -> Result<Option<serde_json::Value>> {
    // Snake_case per golden flow 12; fall back to camelCase for compatibility
    let run_service_url = job_ref
        .get("run_service_url")
        .or_else(|| job_ref.get("runServiceUrl"))
        .and_then(|v| v.as_str());

    let runner_request_id = job_ref
        .get("runner_request_id")
        .or_else(|| job_ref.get("runnerRequestId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    if let Some(rs_url) = run_service_url {
        let rs_client =
            crate::client::run_service::RunServiceClient::new(http.clone(), rs_url.to_string());
        let acquire_body = serde_json::json!({
            "jobMessageId": runner_request_id,
            "runnerOS": if cfg!(target_os = "macos") { "macOS" } else { "Linux" },
            "billingOwnerId": job_ref.get("billing_owner_id")
                .or_else(|| job_ref.get("billingOwnerId")),
        });
        let job = rs_client.acquire_job(token, &acquire_body).await?;
        info!("Job acquired via run-service");
        Ok(Some(job))
    } else {
        // No run-service URL → this must be the full payload already (e.g. local aksh)
        info!("Job message is full payload (no run-service URL)");
        Ok(Some(job_ref.clone()))
    }
}
