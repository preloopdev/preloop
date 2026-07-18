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
use crate::listener::job_dispatcher::{self, cancellation_timing, parse_timespan_secs, RunningJob};
use crate::settings::RunnerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrokerMessageKind {
    RunnerJobRequest,
    PipelineAgentJobRequest,
    JobCancellation,
    AgentRefresh,
    BrokerMigration,
    ForceTokenRefresh,
    RunnerShutdown,
    RunnerRefresh,
    RunnerRefreshConfig,
    Unknown,
}

fn classify_message(message_type: &str) -> BrokerMessageKind {
    match message_type {
        "RunnerJobRequest" => BrokerMessageKind::RunnerJobRequest,
        "PipelineAgentJobRequest" => BrokerMessageKind::PipelineAgentJobRequest,
        "JobCancellation" => BrokerMessageKind::JobCancellation,
        "AgentRefresh" => BrokerMessageKind::AgentRefresh,
        "BrokerMigration" => BrokerMessageKind::BrokerMigration,
        "ForceTokenRefresh" => BrokerMessageKind::ForceTokenRefresh,
        "RunnerShutdown" => BrokerMessageKind::RunnerShutdown,
        "RunnerRefresh" => BrokerMessageKind::RunnerRefresh,
        "RunnerRefreshConfig" => BrokerMessageKind::RunnerRefreshConfig,
        _ => BrokerMessageKind::Unknown,
    }
}

/// Run the broker message polling loop.
pub async fn run_broker_loop(
    http: &HttpClient,
    config: &RunnerConfig,
    initial_token: &str,
    initial_expires_at: Option<std::time::Instant>,
    once: bool,
    runner_root: &std::path::Path,
) -> Result<()> {
    let mut config = config.clone();
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
    let mut client = BrokerClient::new(http.clone(), broker_url);

    let mut token = initial_token.to_string();
    let mut token_expires_at: Option<std::time::Instant> = initial_expires_at;
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
        // Proactive OAuth token refresh — renew 5 minutes before expiry so the
        // next poll cycle always uses a live token (RLIS-02).
        if let Some(exp) = token_expires_at {
            if std::time::Instant::now() >= exp {
                info!("OAuth token expiring soon, proactively refreshing...");
                match crate::listener::oauth::get_oauth_token(http, &config).await {
                    Ok((t, ea)) => {
                        token = t;
                        token_expires_at = ea;
                        consecutive_errors = 0;
                    }
                    Err(e) => {
                        warn!("Proactive OAuth token refresh failed: {e:#}");
                    }
                }
            }
        }

        // Check if active job has finished (non-blocking) — covers the case
        // where the job completed between loop iterations without going through select!.
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
                        if once {
                            info!("exiting after first job (--once)");
                        } else {
                            info!("exiting after first job (--ephemeral)");
                        }
                        if !session_id.is_empty() {
                            let _ = client.delete_session(&token, &session_id).await;
                        }
                        return Ok(());
                    }
                    active_job = None;
                }
                Ok(None) => {} // still running — wait() branch in select! will catch it
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
                    "version": crate::PROTOCOL_COMPAT_VERSION,
                    "osDescription": crate::os_description(),
                    "ephemeral": serde_json::Value::Null,
                    "status": 0,
                    "provisioningState": serde_json::Value::Null,
                },
                "useFipsEncryption": false,
            });

            match client.create_session(&token, &session_body).await {
                Ok(session_resp) => {
                    if let Some(sid) = session_resp.get("sessionId").and_then(|v| v.as_str()) {
                        session_id = sid.to_string();
                        session_key = extract_session_key_if_present(&session_resp, &config);
                        info!("Broker session created: {session_id}");
                        need_session = false;
                        consecutive_errors = 0;
                        // Scope dedup set to this session — old IDs from a previous
                        // session must not block re-delivered messages on the new one.
                        processed_message_ids.clear();
                        debug!("Cleared message dedup set for new session {session_id}");
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
                        match crate::listener::oauth::get_oauth_token(http, &config).await {
                            Ok((new_token, new_expires)) => {
                                token = new_token;
                                token_expires_at = new_expires;
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
        let kill_at = active_job.as_ref().and_then(|j| j.kill_at);

        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received");
                if let Some(mut job) = active_job.take() {
                    info!("Killing active worker");
                    job.kill().await;
                }
                if !session_id.is_empty() {
                    let _ = client.delete_session(&token, &session_id).await;
                }
                return Ok(());
            }
            // When a job is active, race between job completion and broker
            // message polling.  The broker poll uses a short ~3s timeout when
            // busy (matching the official runner's ~3s cancel-detection cadence)
            // so cancellation messages are detected promptly.
            result = async { active_job.as_mut().unwrap().wait().await }, if busy => {
                match result {
                    Ok(success) => {
                        let id = &active_job.as_ref().unwrap().request_id;
                        if success {
                            info!("Worker completed job {id} successfully");
                        } else {
                            warn!("Worker failed for job {id}");
                        }
                    }
                    Err(e) => warn!("Worker wait error: {e:#}"),
                }
                if once || config.settings.ephemeral {
                    if once {
                        info!("exiting after first job (--once)");
                    } else {
                        info!("exiting after first job (--ephemeral)");
                    }
                    if !session_id.is_empty() {
                        let _ = client.delete_session(&token, &session_id).await;
                    }
                    return Ok(());
                }
                active_job = None;
                continue;
            }
            // Hard-kill after cancel grace (official: kill_at = timeout − 15s).
            _ = tokio::time::sleep_until(kill_at.unwrap_or_else(tokio::time::Instant::now)),
                if kill_at.is_some() => {
                if let Some(job) = active_job.as_mut() {
                    warn!("Cancel grace expired — hard-killing worker {}", job.request_id);
                    job.kill().await;
                    job.kill_at = None;
                }
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

                        match classify_message(&message_type) {
                            BrokerMessageKind::RunnerJobRequest => {
                                if let Some(mut prev) = active_job.take() {
                                    // C-04: Official run-service dispatcher cancels
                                    // the previous worker immediately on overlap
                                    // rather than blocking 45s. The successor is
                                    // NEVER dropped after acknowledge.
                                    info!(
                                        "RunnerJobRequest while busy — cancelling previous job {}",
                                        prev.request_id
                                    );
                                    let timing = cancellation_timing(60);
                                    prev.cancel(timing.effective_timeout_secs).await;
                                    if tokio::time::timeout(
                                        std::time::Duration::from_secs(timing.kill_after_secs),
                                        prev.wait(),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        prev.kill().await;
                                    }
                                    // Check once/ephemeral AFTER the old job drains
                                    if once || config.settings.ephemeral {
                                        info!("exiting after first job finished during overlap (run-service)");
                                        let _ = client.delete_session(&token, &session_id).await;
                                        return Ok(());
                                    }
                                }
                                match crate::listener::oauth::get_oauth_token(http, &config).await {
                                    Ok((new_token, new_expires)) => { token = new_token; token_expires_at = new_expires; }
                                    Err(e) => warn!("OAuth token renewal before job failed: {e:#}"),
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
                            BrokerMessageKind::PipelineAgentJobRequest => {
                                if let Some(mut prev) = active_job.take() {
                                    // Same cancel-immediately pattern for AzDO path
                                    info!(
                                        "PipelineAgentJobRequest while busy — cancelling previous job {}",
                                        prev.request_id
                                    );
                                    let timing = cancellation_timing(60);
                                    prev.cancel(timing.effective_timeout_secs).await;
                                    if tokio::time::timeout(
                                        std::time::Duration::from_secs(timing.kill_after_secs),
                                        prev.wait(),
                                    )
                                    .await
                                    .is_err()
                                    {
                                        prev.kill().await;
                                    }
                                    if once || config.settings.ephemeral {
                                        let _ = client.delete_session(&token, &session_id).await;
                                        return Ok(());
                                    }
                                }
                                let running = job_dispatcher::spawn_job(
                                    body,
                                    runner_root,
                                    crate::cli::ProtocolPath::Azdo,
                                ).await?;
                                active_job = Some(running);
                            }
                            BrokerMessageKind::JobCancellation => {
                                // Official shape: { jobId: Guid, timeout: TimeSpan }
                                let cancel_job_id = body
                                    .get("jobId")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| uuid::Uuid::parse_str(s).ok());
                                let timeout_secs = body
                                    .get("timeout")
                                    .and_then(|v| v.as_str())
                                    .and_then(parse_timespan_secs)
                                    .unwrap_or(300);
                                let timing = cancellation_timing(timeout_secs);

                                // Official runner deserializes jobId as Guid;
                                // a missing/malformed value never reaches Cancel().
                                let Some(msg_id) = cancel_job_id else {
                                    debug!("JobCancellation has no valid jobId — ignoring");
                                    continue;
                                };
                                if let Some(job) = active_job.as_mut() {
                                    if let Some(active_id) = job.job_id {
                                        if msg_id != active_id {
                                            debug!(
                                                "JobCancellation jobId {msg_id} does not match active {active_id} — ignoring"
                                            );
                                            continue;
                                        }
                                    }
                                    info!(
                                        "Cancelling active job {} (timeout={}s, kill_after={}s)",
                                        job.request_id,
                                        timing.effective_timeout_secs,
                                        timing.kill_after_secs,
                                    );
                                    // Graceful cancel is delivered once. Every matching
                                    // repeat resets the forced-kill deadline, like
                                    // CancellationTokenSource.CancelAfter in the official runner.
                                    job.cancel(timing.effective_timeout_secs).await;
                                    job.kill_at = Some(
                                        tokio::time::Instant::now()
                                            + std::time::Duration::from_secs(timing.kill_after_secs),
                                    );
                                } else {
                                    debug!("Received cancellation but no active job");
                                }
                            }
                            BrokerMessageKind::AgentRefresh => {
                                info!("Self-update requested; aksh-runner does not self-update");
                            }
                            BrokerMessageKind::ForceTokenRefresh => {
                                info!("Received ForceTokenRefresh; refreshing listener token");
                                match crate::listener::oauth::get_oauth_token(http, &config).await {
                                    Ok((new_token, new_expires)) => {
                                        token = new_token;
                                        token_expires_at = new_expires;
                                        consecutive_errors = 0;
                                    }
                                    Err(error) => warn!("Forced OAuth token refresh failed: {error:#}"),
                                }
                            }
                            BrokerMessageKind::RunnerShutdown => {
                                let reason = body
                                    .get("reason")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or("unspecified");
                                info!(%reason, "Service requested runner shutdown");
                                return Ok(());
                            }
                            BrokerMessageKind::RunnerRefresh => {
                                info!("Self-update requested (RunnerRefresh); aksh-runner does not self-update");
                            }
                            BrokerMessageKind::RunnerRefreshConfig => {
                                let old_broker_url = config
                                    .settings
                                    .server_url_v2
                                    .clone()
                                    .unwrap_or_else(|| config.settings.server_url.clone())
                                    .trim_end_matches('/')
                                    .to_string();
                                match apply_runner_refresh_config(
                                    http,
                                    &mut config,
                                    runner_root,
                                    &body,
                                    &token,
                                )
                                .await
                                {
                                    Ok(true) => {
                                        info!("Runner configuration refresh applied");
                                        let new_broker_url = config
                                            .settings
                                            .server_url_v2
                                            .clone()
                                            .unwrap_or_else(|| config.settings.server_url.clone())
                                            .trim_end_matches('/')
                                            .to_string();
                                        if new_broker_url != old_broker_url {
                                            if !session_id.is_empty() {
                                                let _ = client.delete_session(&token, &session_id).await;
                                                session_id.clear();
                                            }
                                            session_key = None;
                                            need_session = true;
                                            client = BrokerClient::new(http.clone(), new_broker_url);
                                        }
                                    }
                                    Ok(false) => {
                                        info!("Runner configuration refresh acknowledged without supported changes");
                                    }
                                    Err(error) => {
                                        warn!("Runner configuration refresh failed (non-fatal): {error:#}");
                                    }
                                }
                            }
                            BrokerMessageKind::BrokerMigration => {
                                info!("Broker migration requested — re-resolving broker URL...");
                                if !session_id.is_empty() {
                                    let _ = client.delete_session(&token, &session_id).await;
                                    session_id = String::new();
                                }
                                need_session = true;
                                if let Some(new_url) = re_resolve_broker_url(http, &config.settings.server_url).await {
                                    info!("New broker URL after migration: {new_url}");
                                    client = BrokerClient::new(http.clone(), new_url.trim_end_matches('/').to_string());
                                }
                            }
                            BrokerMessageKind::Unknown => {
                                warn!(%message_type, "unhandled broker message type");
                            }
                        }
                    }
                    Ok(None) => {
                        consecutive_errors = 0;
                        debug!("Broker poll returned no message");
                    }
                    Err(e) => {
                        if is_unauthorized(&e) {
                            info!("OAuth token expired during message poll. Re-acquiring token...");
                            match crate::listener::oauth::get_oauth_token(http, &config).await {
                                Ok((new_token, new_expires)) => {
                                    token = new_token;
                                    token_expires_at = new_expires;
                                    consecutive_errors = 0;
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
                            // F052: Respect skip_session_recover setting
                            if config.settings.skip_session_recover {
                                warn!("Broker session expired. SkipSessionRecover is set — exiting.");
                                return Err(e);
                            }
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
/// Apply the official RunnerRefreshConfig protocol. The message identifies a
/// config refresh operation; the refreshed runner settings are returned by the
/// service as a base64-encoded `.runner` JSON document.
async fn apply_runner_refresh_config(
    http: &HttpClient,
    config: &mut RunnerConfig,
    runner_root: &std::path::Path,
    body: &serde_json::Value,
    token: &str,
) -> Result<bool> {
    // Local servers may include the settings object directly. Try this first,
    // while still enforcing the immutable runner identity in settings.rs.
    if config.apply_runner_settings_refresh(body, runner_root)? {
        return Ok(true);
    }

    let Some(object) = body.as_object() else {
        return Ok(false);
    };
    let config_type = object
        .get("configType")
        .or_else(|| object.get("config_type"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if !config_type.eq_ignore_ascii_case("runner") {
        // Credentials and future config types are acknowledged but deliberately
        // left untouched; this runner cannot safely rotate its auth files here.
        return Ok(false);
    }

    if let Some(qualified_id) = object
        .get("runnerQualifiedId")
        .or_else(|| object.get("runner_qualified_id"))
        .and_then(|value| value.as_str())
    {
        let parts: Vec<_> = qualified_id
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        if parts.len() != 4 || parts[3] != config.settings.agent_id.to_string() {
            return Ok(false);
        }
    }

    let service_type = object
        .get("serviceType")
        .or_else(|| object.get("service_type"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    if !service_type.eq_ignore_ascii_case("pipelines") {
        return Ok(false);
    }
    let refresh_url = object
        .get("configRefreshURL")
        .or_else(|| object.get("configRefreshUrl"))
        .or_else(|| object.get("config_refresh_url"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    let Some(refresh_url) = refresh_url else {
        return Ok(false);
    };

    let runner_file = runner_root.join(".runner");
    let current = std::fs::read(&runner_file)
        .with_context(|| format!("reading {} for refresh", runner_file.display()))?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(current);
    let refreshed: serde_json::Value = http
        .post_json_bearer(refresh_url, &serde_json::json!(encoded), token)
        .await
        .context("refreshing runner settings")?;
    if refreshed.is_null() || refreshed.as_str().is_some_and(str::is_empty) {
        return Ok(false);
    }
    config.apply_runner_settings_refresh(&refreshed, runner_root)
}

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
async fn re_resolve_broker_url(http: &HttpClient, server_url: &str) -> Option<String> {
    let url = format!("{}/_apis/connectionData?connectOptions=1", server_url);
    if let Ok(resp) = http.get_json::<serde_json::Value>(&url).await {
        if let Some(broker_url) = resp.get("brokerUrl").and_then(|v| v.as_str()) {
            return Some(broker_url.to_string());
        }
        // Fall back to locationServiceData properties if brokerUrl not directly on root
        if let Some(properties) = resp
            .get("locationServiceData")
            .and_then(|l| l.get("properties"))
        {
            if let Some(broker_url) = properties.get("ServerUrlV2").and_then(|v| v.as_str()) {
                return Some(broker_url.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- P1 broker listener gap coverage ---

    #[test]
    fn classify_message_maps_official_broker_types() {
        let cases = [
            ("RunnerJobRequest", BrokerMessageKind::RunnerJobRequest),
            (
                "PipelineAgentJobRequest",
                BrokerMessageKind::PipelineAgentJobRequest,
            ),
            ("JobCancellation", BrokerMessageKind::JobCancellation),
            ("AgentRefresh", BrokerMessageKind::AgentRefresh),
            ("BrokerMigration", BrokerMessageKind::BrokerMigration),
            ("ForceTokenRefresh", BrokerMessageKind::ForceTokenRefresh),
            ("RunnerShutdown", BrokerMessageKind::RunnerShutdown),
            ("RunnerRefresh", BrokerMessageKind::RunnerRefresh),
            (
                "RunnerRefreshConfig",
                BrokerMessageKind::RunnerRefreshConfig,
            ),
            // The official SDK wire constant is RunnerShutdown; this legacy
            // name must remain unknown rather than being accepted as an alias.
            ("HostedRunnerShutdown", BrokerMessageKind::Unknown),
            ("FutureBrokerMessage", BrokerMessageKind::Unknown),
        ];

        for (wire_type, expected) in cases {
            assert_eq!(
                classify_message(wire_type),
                expected,
                "unexpected classification for broker type {wire_type}"
            );
        }
    }

    #[test]
    fn is_unauthorized_detects_401() {
        let err = anyhow::Error::new(crate::client::http::HttpError::Status {
            status: reqwest::StatusCode::UNAUTHORIZED,
            body: "Unauthorized".to_string(),
        });
        assert!(is_unauthorized(&err));
    }

    #[test]
    fn is_unauthorized_rejects_other_status() {
        let err = anyhow::Error::new(crate::client::http::HttpError::Status {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            body: "error".to_string(),
        });
        assert!(!is_unauthorized(&err));
    }

    #[test]
    fn is_unauthorized_rejects_non_http_error() {
        let err = anyhow::anyhow!("network timeout");
        assert!(!is_unauthorized(&err));
    }

    #[test]
    fn is_session_expired_detects_404() {
        let err = anyhow::Error::new(crate::client::http::HttpError::Status {
            status: reqwest::StatusCode::NOT_FOUND,
            body: "session not found".to_string(),
        });
        assert!(is_session_expired(&err));
    }

    #[test]
    fn is_session_expired_detects_400() {
        let err = anyhow::Error::new(crate::client::http::HttpError::Status {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "bad session".to_string(),
        });
        assert!(is_session_expired(&err));
    }

    #[test]
    fn is_session_expired_rejects_200() {
        let err = anyhow::Error::new(crate::client::http::HttpError::Status {
            status: reqwest::StatusCode::OK,
            body: "ok".to_string(),
        });
        assert!(!is_session_expired(&err));
    }

    #[test]
    fn parse_message_body_plaintext_object() {
        let msg = serde_json::json!({
            "body": {"runner_request_id": "abc-123", "run_service_url": "https://example.com"}
        });
        let body = parse_message_body(&msg, None).unwrap();
        assert_eq!(
            body.get("runner_request_id").unwrap().as_str().unwrap(),
            "abc-123"
        );
    }

    #[test]
    fn parse_message_body_plaintext_string() {
        let msg = serde_json::json!({
            "body": "{\"key\": \"value\"}"
        });
        let body = parse_message_body(&msg, None).unwrap();
        assert_eq!(body.get("key").unwrap().as_str().unwrap(), "value");
    }

    #[test]
    fn parse_message_body_empty_is_error() {
        let msg = serde_json::json!({"body": ""});
        // Empty string is not valid JSON — parse_message_body should fail
        assert!(parse_message_body(&msg, None).is_err());
    }

    #[test]
    fn parse_message_body_no_body_field() {
        let msg = serde_json::json!({"messageType": "unknown"});
        let body = parse_message_body(&msg, None).unwrap();
        assert!(body.is_object() || body.is_null());
    }

    #[test]
    fn extract_session_key_no_encryption_key() {
        let session = serde_json::json!({"sessionId": "abc"});
        let config = test_config();
        assert!(extract_session_key_if_present(&session, &config).is_none());
    }

    #[test]
    fn extract_session_key_empty_value() {
        let session = serde_json::json!({
            "sessionId": "abc",
            "encryptionKey": {"value": "", "encrypted": false}
        });
        let config = test_config();
        assert!(extract_session_key_if_present(&session, &config).is_none());
    }

    #[test]
    fn extract_session_key_unencrypted_returns_raw() {
        use base64::Engine;
        let raw_key = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw_key);
        let session = serde_json::json!({
            "sessionId": "abc",
            "encryptionKey": {"value": b64, "encrypted": false}
        });
        let config = test_config();
        let key = extract_session_key_if_present(&session, &config).unwrap();
        assert_eq!(key, raw_key);
    }

    /// Helper: build a minimal RunnerConfig for tests
    fn test_config() -> RunnerConfig {
        RunnerConfig {
            settings: crate::settings::RunnerSettings {
                agent_id: 1,
                agent_name: "test".to_string(),
                pool_id: 1,
                pool_name: "Default".to_string(),
                server_url: "https://example.com".to_string(),
                git_hub_url: "https://github.com/test/repo".to_string(),
                work_folder: "_work".to_string(),
                is_hosted: false,
                runner_group_id: None,
                runner_group_name: None,
                ephemeral: false,
                is_hosted_server: false,
                use_v2_flow: true,
                server_url_v2: None,
                disable_update: false,
                skip_session_recover: false,
                monitor_socket_address: None,
                use_runner_admin_flow: false,
            },
            credentials: crate::settings::CredentialData {
                scheme: "OAuth".to_string(),
                data: {
                    let mut m = serde_json::Map::new();
                    m.insert("clientId".into(), serde_json::json!("test-id"));
                    m.insert(
                        "authorizationUrl".into(),
                        serde_json::json!("https://vstoken.example.com"),
                    );
                    m
                },
            },
            rsa_params: crate::settings::RsaParameters {
                d: "d".to_string(),
                dp: "dp".to_string(),
                dq: "dq".to_string(),
                exponent: "AQAB".to_string(),
                inverse_q: "iq".to_string(),
                modulus: "mod".to_string(),
                p: "p".to_string(),
                q: "q".to_string(),
            },
        }
    }
}
