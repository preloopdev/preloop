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
    token: &str,
    once: bool,
    runner_root: &std::path::Path,
) -> Result<()> {
    // GitHub broker endpoints live on a dedicated host. The official runner gets this
    // from service-location state; for github.com use the observed public broker host.
    // Local aksh keeps broker routes on the configured server URL.
    let broker_url = if config
        .settings
        .server_url
        .contains(".actions.githubusercontent.com")
        || config.settings.git_hub_url.contains("github.com")
    {
        "https://broker.actions.githubusercontent.com".to_string()
    } else {
        config.settings.server_url.clone()
    };
    let client = BrokerClient::new(http.clone(), broker_url);

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

    let session_resp = client.create_session(token, &session_body).await?;
    let session_id = session_resp
        .get("sessionId")
        .and_then(|v| v.as_str())
        .context("missing sessionId in broker response")?
        .to_string();

    // F011: session key is optional — GitHub broker doesn't send one.
    let session_key = extract_session_key_if_present(&session_resp, config);

    info!("Broker session created: {session_id}");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut processed_message_ids: std::collections::HashSet<i64> =
        std::collections::HashSet::new();
    let mut consecutive_errors: u32 = 0;
    let mut active_job: Option<RunningJob> = None;

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
                    if once {
                        info!("--once: exiting after first job");
                        let _ = client.delete_session(token, &session_id).await;
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

        let busy = active_job.is_some();

        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received");
                if let Some(mut job) = active_job.take() {
                    info!("Killing active worker");
                    job.kill().await;
                }
                let _ = client.delete_session(token, &session_id).await;
                return Ok(());
            }
            result = client.get_message(token, &session_id, busy) => {
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
                            let _ = client.acknowledge(token, &session_id, &runner_request_id).await;
                        }

                        match message_type.as_str() {
                            "RunnerJobRequest" => {
                                if active_job.is_some() {
                                    warn!("Received job while another is running — ignoring");
                                    continue;
                                }
                                let job = acquire_job_from_ref(&body, http, token).await?;
                                if let Some(job_msg) = job {
                                    let mut running = job_dispatcher::spawn_job(
                                        job_msg,
                                        runner_root,
                                        crate::cli::ProtocolPath::Broker,
                                    ).await?;
                                    if once {
                                        // --once: wait for the job, then exit
                                        let success = running.wait().await.unwrap_or(false);
                                        if success {
                                            info!("Worker completed job {} successfully", running.request_id);
                                        } else {
                                            warn!("Worker failed for job {}", running.request_id);
                                        }
                                        info!("--once: exiting after first job");
                                        let _ = client.delete_session(token, &session_id).await;
                                        return Ok(());
                                    }
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
                                    job.kill().await;
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
