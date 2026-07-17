//! Legacy AzDO message listener (distributedtask long-poll).
//!
//! Creates a session, polls for encrypted messages, decrypts them,
//! and dispatches to the job dispatcher.

use anyhow::{Context, Result};
use base64::Engine;
use tracing::{debug, error, info, warn};

use crate::client::azdo::AzdoClient;
use crate::client::http::HttpClient;
use crate::listener::job_dispatcher;
use crate::settings::RunnerConfig;

/// Run the legacy AzDO message polling loop.
pub async fn run_message_loop(
    http: &HttpClient,
    config: &RunnerConfig,
    token: &str,
    once: bool,
    runner_root: &std::path::Path,
) -> Result<()> {
    let client = AzdoClient::new(
        http.clone(),
        config.settings.server_url.clone(),
        config.settings.pool_id,
    );

    let session_body = serde_json::json!({
        "ownerName": config.settings.agent_name,
        "agent": {
            "id": config.settings.agent_id,
            "name": config.settings.agent_name,
        },
        "useFipsEncryption": false,
        // F030: opt in to full AzDO message format (PipelineAgentJobRequest) with
        // encryption. AKSH-specific field; ignored by real GHES/GitHub servers.
        "akshAzdo": true,
    });

    // Retry session creation on 409 conflict: a prior runner instance may still
    // hold an active session.  GitHub typically frees it within ~30 s after the
    // TCP connection drops.  Mirrors MessageListener.cs behaviour.
    let (session_resp, session_id) = loop {
        match client.create_session(token, &session_body).await {
            Ok(resp) => {
                let id = resp
                    .get("sessionId")
                    .and_then(|v| v.as_str())
                    .context("missing sessionId in response")?
                    .to_string();
                break (resp, id);
            }
            Err(e) if e.to_string().contains("409") || e.to_string().contains("session") => {
                warn!("Session conflict (409) — waiting 30 s before retry");
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
            Err(e) => return Err(e.context("creating session")),
        }
    };

    // Extract session key if present (optional — not all servers send one)
    let keypair =
        aksh_gha_protocol::crypto::AgentRsaKeypair::from_rsaparams(&config.rsa_params).ok();
    let session_key = extract_session_key_optional(&session_resp, keypair.as_ref());
    if session_key.is_some() {
        debug!("Session encryption key present");
    } else {
        debug!("No session encryption key — messages are plaintext");
    }

    info!("Session created: {session_id}");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut last_message_id: Option<i64> = None;
    let mut consecutive_errors: u32 = 0;

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received, deleting session");
                let _ = client.delete_session(token, &session_id).await;
                return Ok(());
            }
            result = client.get_message(token, &session_id, last_message_id) => {
                match result {
                    Ok(Some(msg)) => {
                        consecutive_errors = 0;
                        let message_id = msg.get("messageId").and_then(|v| v.as_i64()).unwrap_or(0);
                        last_message_id = Some(message_id);

                        let dispatch = process_message(&msg, session_key.as_deref());

                        // Acknowledge
                        let _ = client.delete_message(token, &session_id, message_id).await;

                        match dispatch {
                            Ok(Some(job_msg)) => {
                                // F030: signal to GitHub that we accepted the job.
                                // Mirrors JobDispatcher.cs PatchAgentRequestAsync(startTime).
                                let request_id = job_msg
                                    .get("requestId")
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(0);
                                if request_id > 0 {
                                    let patch = serde_json::json!({
                                        "requestId": request_id,
                                        "agentId": config.settings.agent_id,
                                        "startTime": crate::worker::helpers::iso_now(),
                                    });
                                    match client.patch_agent_request(token, request_id, &patch).await {
                                        Ok(_) => info!("Patched agent request {request_id} (started)"),
                                        Err(e) => warn!("patch_agent_request start failed (non-fatal): {e:#}"),
                                    }
                                }

                                job_dispatcher::dispatch_job(
                                    job_msg,
                                    runner_root,
                                    crate::cli::ProtocolPath::Azdo,
                                ).await?;

                                // F030: signal job completed.
                                if request_id > 0 {
                                    let patch = serde_json::json!({
                                        "requestId": request_id,
                                        "agentId": config.settings.agent_id,
                                        "finishTime": crate::worker::helpers::iso_now(),
                                    });
                                    match client.patch_agent_request(token, request_id, &patch).await {
                                        Ok(_) => info!("Patched agent request {request_id} (completed)"),
                                        Err(e) => warn!("patch_agent_request complete failed (non-fatal): {e:#}"),
                                    }
                                }

                                if once {
                                    info!("--once: exiting after first job");
                                    let _ = client.delete_session(token, &session_id).await;
                                    return Ok(());
                                }
                            }
                            Ok(None) => {} // Non-job message, already handled
                            Err(e) => {
                                error!("Failed to process message: {e:#}");
                            }
                        }
                    }
                    Ok(None) => {
                        consecutive_errors = 0;
                        info!("Polling: no message received (long-poll timeout)");
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        let delay = std::cmp::min(consecutive_errors * 5, 60);
                        warn!("Message poll error ({consecutive_errors}): {e:#}. Retrying in {delay}s");
                        tokio::time::sleep(std::time::Duration::from_secs(delay as u64)).await;
                    }
                }
            }
        }
    }
}

/// Extract session key if present. Returns None if no encryptionKey in response.
fn extract_session_key_optional(
    session: &serde_json::Value,
    keypair: Option<&aksh_gha_protocol::crypto::AgentRsaKeypair>,
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
        keypair?.unwrap_key(&key_bytes).ok()
    } else {
        Some(key_bytes)
    }
}

/// Parse and dispatch a message. Returns Some(job) for job messages, None for others.
fn process_message(
    msg: &serde_json::Value,
    session_key: Option<&[u8]>,
) -> Result<Option<serde_json::Value>> {
    let message_type = msg
        .get("messageType")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let body_str = msg.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let iv_str = msg.get("iv").and_then(|v| v.as_str());

    // Decrypt or parse plaintext
    let decrypted = if !body_str.is_empty() {
        if let (Some(key), Some(iv)) = (session_key, iv_str) {
            if !iv.is_empty() {
                let body_bytes = base64::engine::general_purpose::STANDARD.decode(body_str)?;
                let iv_bytes = base64::engine::general_purpose::STANDARD.decode(iv)?;
                let enc = aksh_gha_protocol::crypto::SessionEncryption::from_key(key.to_vec());
                let plain = enc
                    .decrypt(&body_bytes, &iv_bytes)
                    .map_err(|e| anyhow::anyhow!("decrypting message: {e}"))?;
                String::from_utf8(plain)?
            } else {
                body_str.to_string()
            }
        } else {
            body_str.to_string()
        }
    } else {
        String::new()
    };

    match message_type {
        "PipelineAgentJobRequest" | "RunnerJobRequest" => {
            let job_msg: serde_json::Value =
                serde_json::from_str(&decrypted).context("parsing job message body")?;
            info!("Received job request (type: {message_type})");
            Ok(Some(job_msg))
        }
        "JobCancellation" => {
            info!("Received job cancellation");
            Ok(None)
        }
        "AgentRefresh" => {
            info!("Self-update requested by server; aksh-runner does not self-update");
            Ok(None)
        }
        other => {
            warn!("Unknown message type: {other}");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- P1 message listener gap coverage ---

    #[test]
    fn process_message_job_request() {
        let msg = serde_json::json!({
            "messageType": "PipelineAgentJobRequest",
            "body": "{\"jobId\": \"test-job-1\", \"steps\": []}"
        });
        let result = process_message(&msg, None).unwrap();
        let job = result.expect("should return job");
        assert_eq!(job.get("jobId").unwrap().as_str().unwrap(), "test-job-1");
    }

    #[test]
    fn process_message_runner_job_request() {
        let msg = serde_json::json!({
            "messageType": "RunnerJobRequest",
            "body": "{\"jobId\": \"broker-job-1\"}"
        });
        let result = process_message(&msg, None).unwrap();
        let job = result.expect("should return job");
        assert_eq!(job.get("jobId").unwrap().as_str().unwrap(), "broker-job-1");
    }

    #[test]
    fn process_message_cancellation_returns_none() {
        let msg = serde_json::json!({
            "messageType": "JobCancellation",
            "body": "{}"
        });
        let result = process_message(&msg, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn process_message_agent_refresh_returns_none() {
        let msg = serde_json::json!({
            "messageType": "AgentRefresh",
            "body": ""
        });
        let result = process_message(&msg, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn process_message_unknown_type_returns_none() {
        let msg = serde_json::json!({
            "messageType": "SomeFutureMessageType",
            "body": ""
        });
        let result = process_message(&msg, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn process_message_missing_type_returns_none() {
        let msg = serde_json::json!({"body": ""});
        let result = process_message(&msg, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_session_key_optional_no_key() {
        let session = serde_json::json!({"sessionId": "test"});
        assert!(extract_session_key_optional(&session, None).is_none());
    }

    #[test]
    fn extract_session_key_optional_empty_value() {
        let session = serde_json::json!({
            "encryptionKey": {"value": "", "encrypted": false}
        });
        assert!(extract_session_key_optional(&session, None).is_none());
    }

    #[test]
    fn extract_session_key_optional_unencrypted() {
        use base64::Engine;
        let key_bytes = vec![10u8, 20, 30, 40, 50, 60, 70, 80];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&key_bytes);
        let session = serde_json::json!({
            "encryptionKey": {"value": b64, "encrypted": false}
        });
        let result = extract_session_key_optional(&session, None).unwrap();
        assert_eq!(result, key_bytes);
    }

    #[test]
    fn extract_session_key_optional_encrypted_without_keypair() {
        use base64::Engine;
        let key_bytes = vec![1u8, 2, 3, 4];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&key_bytes);
        let session = serde_json::json!({
            "encryptionKey": {"value": b64, "encrypted": true}
        });
        // No keypair → cannot decrypt → None
        assert!(extract_session_key_optional(&session, None).is_none());
    }
}
