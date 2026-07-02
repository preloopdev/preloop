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
    });

    let session_resp = client.create_session(token, &session_body).await?;
    let session_id = session_resp
        .get("sessionId")
        .and_then(|v| v.as_str())
        .context("missing sessionId in response")?
        .to_string();

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
                                job_dispatcher::dispatch_job(
                                    job_msg,
                                    runner_root,
                                    crate::cli::ProtocolPath::Azdo,
                                ).await?;
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
                        debug!("No message (long-poll timeout)");
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
