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
    let mut client = BrokerClient::new(http.clone(), broker_url);

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
                        info!("exiting after first job");
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
                if !session_id.is_empty() {
                    let _ = client.delete_session(&token, &session_id).await;
                }
                return Ok(());
            }
            // When a job is active, await its exit directly instead of
            // polling try_wait() every 200ms.  This avoids generating a
            // new GET /message?status=Busy every 200ms — the official
            // runner issues only ONE busy poll per job.
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
                    info!("exiting after first job");
                    if !session_id.is_empty() {
                        let _ = client.delete_session(&token, &session_id).await;
                    }
                    return Ok(());
                }
                active_job = None;
                continue;
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
                                // Renew OAuth token before job acquisition (official runner refreshes here)
                                match crate::listener::oauth::get_oauth_token(http, config).await {
                                    Ok(new_token) => token = new_token,
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
    let url = format!("{}/_apis/connectionData?connectOptions=0", server_url);
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
