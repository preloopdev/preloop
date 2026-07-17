//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod concurrency;
pub mod events;
pub mod github;
pub mod scheduler;
mod errors;
pub use errors::ApiError;

/// Pure job-graph scheduler model and property tests.
pub mod scheduling;

#[cfg(test)]
mod concurrency_http_properties;
#[cfg(test)]
mod concurrency_properties;
/// GitHub-compatible OIDC id-token provider.
pub mod oidc;

use axum_server::{tls_rustls::RustlsConfig, Handle};
use rcgen::generate_simple_self_signed;

use aksh_artifacts::{validate_artifact_name, ArtifactStore};
use aksh_cache::CacheStore;
use aksh_gha_parser::eval::build_context;
use aksh_gha_parser::{expand_jobs_with_reusables, parse_workflow};
use aksh_gha_protocol::{
    azdo,
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson, AnnotationLevel, ExecutionStatus, JobCompletion, JobId, NdjsonEvent,
    RegisteredRunner, RunAccepted, RunId, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use futures::{stream, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

/// Default local token used when `AKSH_SYSTEM_TOKEN` is not configured.
const DEFAULT_AKSH_SYSTEM_TOKEN: &str = "aksh-system-token";
#[cfg(test)]
const TEST_LOCAL_JWT_KEY: &[u8] = b"aksh-test-local-jwt-signing-key";

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind.
    pub listen: SocketAddr,
    /// State directory for cache/artifacts and future durable state.
    pub state_dir: PathBuf,
    /// Optional file path to write recorded flows to (NDJSON format).
    pub record_flows: Option<PathBuf>,
    /// TLS mode (default: no TLS).
    pub tls: TlsMode,
    /// Enable privileged local/CI simulation endpoints.
    pub enable_test_api: bool,
    /// Bearer token required by privileged simulation endpoints.
    pub test_api_token: Option<String>,
    /// OIDC issuer URL. Defaults to `{public_base_url}/oidc`.
    ///
    /// This must identify an issuer controlled by the aksh deployment. Setting
    /// GitHub's hosted issuer does not make locally signed tokens GitHub-trusted.
    pub oidc_issuer: Option<String>,
    /// Enable the cron scheduler for schedule-triggered workflows.
    pub enable_scheduler: bool,
}

/// TLS configuration.
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// Plain HTTP (default).
    None,
    /// Generate an ephemeral self-signed cert at startup.
    SelfSigned,
    /// Load cert and key from PEM files.
    PemFiles { cert: PathBuf, key: PathBuf },
}

/// A self-signed TLS certificate + private key in PEM format.
pub struct SelfSignedCert {
    /// PEM-encoded certificate.
    pub cert: String,
    /// PEM-encoded private key.
    pub key: String,
}

/// Generate an ephemeral self-signed TLS certificate valid for localhost.
pub fn generate_self_signed_cert() -> anyhow::Result<SelfSignedCert> {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let rcgen::CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names)
        .map_err(|e| anyhow::anyhow!("self-signed cert generation failed: {e}"))?;
    Ok(SelfSignedCert {
        cert: cert.pem(),
        key: key_pair.serialize_pem(),
    })
}

async fn reap_once(shared: &Arc<SharedState>) {
    let mut inner = shared.state.inner.lock().await;
    let now = SystemTime::now();
    let mut cancellations = Vec::new();
    let mut disconnected_completions = Vec::new();

    let mut active_reqs = Vec::new();
    for (request_id, request) in &inner.job_requests {
        if request.result.is_none() {
            active_reqs.push((
                *request_id,
                request.run_id,
                request.job_id.clone(),
                request.started_at,
                request.last_renewed_at,
                request.timeout_triggered,
            ));
        }
    }

    for (request_id, run_id, job_id, started_at, last_renewed_at, timeout_triggered) in active_reqs
    {
        // 1. Check Timeout Enforcement
        if let Some(started_at) = started_at {
            if !timeout_triggered {
                let elapsed = now.duration_since(started_at).unwrap_or_default();
                let job_timeout = inner
                    .broker_messages
                    .get(&request_id)
                    .and_then(|msg| msg.job_timeout)
                    .unwrap_or(21600); // 360 minutes in seconds

                if elapsed >= Duration::from_secs(job_timeout as u64) {
                    info!(
                        %run_id,
                        %job_id,
                        request_id,
                        "Job timed out after {}s",
                        job_timeout
                    );
                    if let Some(req) = inner.job_requests.get_mut(&request_id) {
                        req.timeout_triggered = true;
                    }
                    if let Some(agent_job_id) = agent_job_id_for(&inner, run_id, &job_id) {
                        cancellations.push(QueuedCancellation {
                            run_id,
                            job_id: job_id.clone(),
                            agent_job_id,
                        });
                    }
                }
            }
        }

        // 2. Check Lease Expiration / Disconnect Reaper
        if let Some(last_renewed_at) = last_renewed_at {
            let elapsed = now.duration_since(last_renewed_at).unwrap_or_default();
            // 120 seconds disconnect threshold
            if elapsed >= Duration::from_secs(120) {
                info!(
                    %run_id,
                    %job_id,
                    request_id,
                    "Runner lease expired (last renewed {}s ago). Marking job as failed.",
                    elapsed.as_secs()
                );
                if let Some(req) = inner.job_requests.get_mut(&request_id) {
                    req.result = Some(ExecutionStatus::Failure);
                }
                disconnected_completions.push((
                    request_id,
                    JobCompletion {
                        run_id,
                        job_id: job_id.clone(),
                        status: ExecutionStatus::Failure,
                        outputs: Default::default(),
                    },
                ));
            }
        }
    }

    // Cleanup session and inflight maps for disconnected runners
    for (request_id, _) in &disconnected_completions {
        inner.inflight_requests.remove(request_id);
        inner
            .session_active_requests
            .retain(|_, &mut v| v != *request_id);
    }

    // Apply cancellations
    let cancellation_count = cancellations.len();
    if cancellation_count > 0 {
        inner.cancellation_queue.extend(cancellations);
    }

    drop(inner);

    // Notify if cancellations occurred
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
    }

    // Process completions for disconnected runners
    for (_, completion) in disconnected_completions {
        let _ = complete_job_inner(shared.clone(), completion).await;
    }
}

async fn run_background_reaper(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    // Skip the first tick
    interval.tick().await;

    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                reap_once(&shared).await;
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

/// Start the server and block until shutdown.
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let mut state = AppState::new(config.state_dir.clone()).await?;
    if !config.listen.ip().is_loopback() && state.system_token == DEFAULT_AKSH_SYSTEM_TOKEN {
        anyhow::bail!(
            "AKSH_SYSTEM_TOKEN must be explicitly configured when listening beyond loopback"
        );
    }
    let oidc_issuer = normalize_oidc_issuer(
        config
            .oidc_issuer
            .unwrap_or_else(|| format!("{}/oidc", public_base_url())),
    )?;
    {
        let mut inner = state.inner.lock().await;
        inner.oidc_issuer = oidc_issuer;
    }
    let shutdown = CancellationToken::new();
    if config.enable_scheduler {
        let scheduler = crate::scheduler::Scheduler::new();
        state.scheduler = Some(scheduler.clone());
        let shared_for_scan = Arc::new(SharedState {
            state: state.clone(),
            shutdown: shutdown.clone(),
        });
        let scheduler_clone = scheduler.clone();
        if let Some(workspace) = state.local_workspace.clone() {
            tokio::spawn(async move {
                scheduler_clone
                    .scan_workspace(&workspace, shared_for_scan)
                    .await;
            });
        } else {
            tokio::spawn(async move {
                scheduler_clone.scan_remote(shared_for_scan).await;
            });
        }
    }
    if let Some(path) = &config.record_flows {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut inner = state.inner.lock().await;
        inner.flows_file = Some(file);
    }
    let test_api_token = if config.enable_test_api {
        if !config.listen.ip().is_loopback() {
            anyhow::bail!("the test API may only be enabled on a loopback listener");
        }
        let token = config
            .test_api_token
            .clone()
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("--enable-test-api requires --test-api-token"))?;
        warn!(
            listen = %config.listen,
            "PRIVILEGED TEST API ENABLED; simulated sessions and completions are accepted"
        );
        Some(token)
    } else {
        if config.test_api_token.is_some() {
            anyhow::bail!("--test-api-token requires --enable-test-api");
        }
        None
    };
    let router = build_app(state.clone(), shutdown.clone(), test_api_token);

    let shared = Arc::new(SharedState {
        state,
        shutdown: shutdown.clone(),
    });

    let checker_shared = shared.clone();
    tokio::spawn(async move {
        run_background_reaper(checker_shared).await;
    });

    match config.tls {
        TlsMode::None => {
            let listener = TcpListener::bind(config.listen).await?;
            info!(listen = %config.listen, scheme = "http", "aksh runner server listening");
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal(shutdown))
                .await?;
        }
        TlsMode::SelfSigned => {
            let cert = generate_self_signed_cert()?;
            let tls_config =
                RustlsConfig::from_pem(cert.cert.into_bytes(), cert.key.into_bytes()).await?;
            info!(listen = %config.listen, scheme = "https", self_signed = true, "aksh runner server listening");
            warn!("self-signed cert -- runner needs --ss-skip-tls-verify or GITHUB_ACTIONS_RUNNER_SKIP_TLS_VERIFY=1");
            let handle = Handle::new();
            tokio::spawn({
                let handle = handle.clone();
                async move {
                    if let Err(e) = axum_server::bind_rustls(config.listen, tls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                    {
                        warn!(%e, "TLS server error");
                    }
                }
            });
            shutdown_signal(shutdown).await;
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
        TlsMode::PemFiles { cert, key } => {
            let tls_config = RustlsConfig::from_pem_file(&cert, &key).await?;
            info!(listen = %config.listen, scheme = "https", cert = %cert.display(), "aksh runner server listening");
            let handle = Handle::new();
            tokio::spawn({
                let handle = handle.clone();
                async move {
                    if let Err(e) = axum_server::bind_rustls(config.listen, tls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                    {
                        warn!(%e, "TLS server error");
                    }
                }
            });
            shutdown_signal(shutdown).await;
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
    }
    Ok(())
}

async fn shutdown_signal(shutdown: CancellationToken) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install ctrl-c handler");
        }
    };
    ctrl_c.await;
    shutdown.cancel();
}

/// Build the production server router without simulation endpoints.
pub fn app(state: AppState, shutdown: CancellationToken) -> Router {
    build_app(state, shutdown, None)
}

/// Build an in-process router with privileged local/CI simulation endpoints.
///
/// Network servers should use [`serve`], which additionally enforces a
/// loopback-only listener when this API is enabled.
pub fn app_with_test_api(
    state: AppState,
    shutdown: CancellationToken,
    token: impl Into<String>,
) -> Router {
    build_app(state, shutdown, Some(token.into()))
}

fn build_app(
    state: AppState,
    shutdown: CancellationToken,
    test_api_token: Option<String>,
) -> Router {
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: shutdown.clone(),
    });
    let protected_apis = Router::new()
        .route("/_apis/artifactcache/cache", post(cache_reserve))
        .route("/_apis/artifactcache/cache", get(cache_lookup))
        .route("/_apis/artifactcache/cache/:cache_id", patch(cache_upload))
        .route("/_apis/artifactcache/cache/:cache_id", post(cache_commit))
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts",
            post(artifact_create),
        )
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts",
            get(artifact_list),
        )
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts/:artifact_id",
            get(artifact_get_compat),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools",
            get(runner_pools),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/agents",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/agents/:agent_id",
            delete(delete_agent),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/agents/:agent_id",
            delete(delete_agent),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions",
            post(create_session_disttask).delete(delete_sessions_for_pool),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/sessions",
            post(create_session_disttask).delete(delete_sessions_for_pool),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions/:session_id",
            delete(delete_session),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/sessions/:session_id",
            delete(delete_session),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages",
            get(next_message_disttask),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:run_id/jobs/:job_id",
            patch(complete_job_compat),
        )
        .route("/ws/live-logs/:job_id", get(ws_live_logs))
        .route("/broker/:runner_id/acquirejob", post(broker_acquire_job))
        .route("/broker/:runner_id/renewjob", post(broker_renew_job))
        .route("/broker/:runner_id/completejob", post(broker_complete_job))
        .route(
            "/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log),
        )
        .route(
            "/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log),
        )
        .route(
            "/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:plan_id/jobs/:job_id/oidctoken",
            get(oidc_token),
        )
        .route(
            "/:orchestration_id//idtoken/:plan_id/:job_id",
            get(oidc_token_run_service),
        )
        .route(
            "/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
        )
        .route(
            "/actions/build/:orchestration_id/jobs/:job_id/runnerresolve/actions",
            post(runnerresolve_actions),
        )
        .route(
            "/runner/server/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log),
        )
        .route(
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log),
        )
        .route(
            "/runner/server/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job),
        )
        .route(
            "/runner/server/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
        )
        .route(
            "/api/v1/runs/:run_id/jobs/:job_id/logs/live",
            get(live_logs_sse),
        )
        // F030: standard AzDO API URL pattern used by the aksh-runner AzDO client.
        // These alias the scope/hub-prefixed handlers above so both URL forms work.
        .route(
            "/_apis/v1/plans/:plan_id/timelines/:timeline_id/records",
            patch(patch_timeline_records_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/logs",
            post(create_log_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/logs/:log_id",
            put(append_log_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/events",
            post(finish_job_plan),
        )
        // F030: /runner/server/ aliases — runner uses the SystemVssConnection URL
        // which is http://…/runner/server so all plan-level AzDO calls land here.
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/timelines/:timeline_id/records",
            patch(patch_timeline_records_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/logs",
            post(create_log_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/logs/:log_id",
            put(append_log_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/events",
            post(finish_job_plan),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_protocol_bearer,
        ));

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route("/.well-known/jwks.json", get(oidc_jwks))
        .route(
            "/oidc/.well-known/openid-configuration",
            get(oidc_discovery),
        )
        .route("/oidc/.well-known/jwks.json", get(oidc_jwks))
        // GHES-style org-prefixed routes
        .route("/:org/_apis/connectionData", get(connection_data))
        .route("/:org/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/:org/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/:org/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id_org).post(register_runner_compat_org_2),
        )
        .route(
            "/:org/_apis/v1/Agent/:pool_id",
            get(agent_lookup_org).post(register_runner_compat_org),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat_org),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_org_pool_only),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session_org),
        )
        .route(
            "/:org/_apis/v1/Message/:pool_id",
            get(next_message_compat_org),
        )
        .route(
            "/:org/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message_org),
        )
        .route(
            "/:org/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get_org)
                .post(agent_request_ack_org)
                .patch(agent_request_patch_org),
        )
        .route(
            "/:org/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records_org),
        )
        .route(
            "/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log_org),
        )
        .route(
            "/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log_org),
        )
        .route(
            "/:org/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log_org),
        )
        .route(
            "/:org/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job_org),
        )
        .route(
            "/:org/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info_org),
        )
        .route("/_apis/v1/oauth2/token", post(oauth2_token))
        .route(
            "/api/v3/actions/runner-registration",
            post(github_registration_token),
        )
        .route(
            "/api/v3/orgs/:org/actions/runners/registration-token",
            post(github_registration_token),
        )
        .route(
            "/api/v3/repos/:owner/:repo/actions/runners/registration-token",
            post(github_registration_token),
        )
        .route("/runner/server/_apis/connectionData", get(connection_data))
        .route("/runner/server/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/runner/server/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/runner/server/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id)
                .post(register_runner_compat)
                .put(register_runner_compat),
        )
        .route(
            "/runner/server/_apis/v1/Agent/:pool_id",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session),
        )
        .route(
            "/runner/server/_apis/v1/Message/:pool_id",
            get(next_message_compat),
        )
        .route(
            "/runner/server/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/runner/server/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch),
        )
        .route("/_apis/connectionData", get(connection_data))
        .route(
            "/api/v1/runs",
            post(submit_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route("/api/v1/scheduler/history", get(get_scheduler_history))
        .route(
            "/api/v1/github/webhooks",
            post(github::handle_github_webhook),
        )
        .route("/api/v1/github/register", get(github::github_register))
        .route("/api/v1/github/callback", get(github::github_callback))
        .route(
            "/api/v1/runs/:run_id",
            get(get_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/logs",
            get(get_run_logs).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/cancel",
            post(cancel_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/rerun",
            post(rerun_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/events.ndjson",
            get(run_events).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/debug",
            get(ws_dap_debug).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/debug",
            post(register_dap_port).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        // Archive tickets are bearerless in the official runner protocol.
        .route(
            "/api/v1/actions/download/:owner/:repo/*git_ref",
            get(download_action_tarball),
        )
        .route(
            "/api/v1/runners",
            post(register_runner).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/runner/acknowledge", post(broker_acknowledge_root))
        .route(
            "/runner/server/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/server/runner/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/server/runner/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route(
            "/runner/server/runner/acknowledge",
            post(broker_acknowledge_root),
        )
        .route(
            "/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/acknowledge", post(broker_acknowledge_root))
        .route(
            "/runner/server/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/server/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/server/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/runner/server/acknowledge", post(broker_acknowledge_root))
        .route(
            "/api/v1/cache",
            get(cache_get)
                .post(cache_put)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route(
            "/api/v1/artifacts",
            post(artifact_put).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/artifacts/:artifact_id",
            get(artifact_get).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        // Runner lifecycle endpoints — public before the runner receives its token.
        .route("/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id).post(register_runner_compat),
        )
        .route(
            "/_apis/v1/Agent/:pool_id",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_pool_only),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session),
        )
        .route("/_apis/v1/Message/:pool_id", get(next_message_compat))
        .route(
            "/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch),
        )
        // P1.10: Accept blob uploads at the signed-URL paths minted by the Twirp handlers.
        // The runner PUTs logs/summaries here; we store them in the state directory.
        .route("/replay/results/*path", put(replay_results_put))
        // Twirp results-service routes — outside require_bearer so the runner's
        // job token (which uses a different signing key) is accepted.
        .route(
            "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            post(twirp_workflow_steps_update),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            post(twirp_get_job_logs_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
            post(twirp_get_step_logs_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
            post(twirp_get_step_summary_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            post(twirp_create_step_summary_metadata),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            post(twirp_create_step_logs_metadata),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            post(twirp_create_job_logs_metadata),
        )
        // Cache v2 Twirp (CacheService) — used by actions/cache@v4 when ACTIONS_CACHE_SERVICE_V2=true.
        // Auth: bearer from job runtime token (verified by having correct scp in the JWT).
        // These routes are outside require_bearer because the job JWT uses its own signing context.
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            post(twirp_cache_v2_create),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
            post(twirp_cache_v2_finalize),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
            post(twirp_cache_v2_get_dl_url),
        )
        // Artifact v2 Twirp (ArtifactService) — used by actions/upload-artifact@v4 and download-artifact@v4.
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
            post(twirp_artifact_v2_create),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
            post(twirp_artifact_v2_finalize),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
            post(twirp_artifact_v2_list),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
            post(twirp_artifact_v2_get_signed_url),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
            post(twirp_artifact_v2_delete),
        )
        // Azure Block Blob compat blob store — upload (PUT) and download (GET).
        // Cache: /twirp-blob/cache/{token}
        // Artifact: /twirp-blob/artifact/{token}  (download URL appends .zip for content-type detection)
        .route("/twirp-blob/:kind/:token", put(blob_put).get(blob_get))
        .merge(protected_apis)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_flows_middleware,
        ))
        .with_state(shared.clone());

    match test_api_token {
        Some(token) => router.merge(
            Router::new()
                .route(
                    "/internal/test/runners/sessions/:session_id/messages",
                    get(next_message),
                )
                .route(
                    "/internal/test/runners/sessions/:session_id/messages/:message_id",
                    delete(delete_session_message),
                )
                .route("/internal/test/runners/sessions", post(create_session))
                .route("/internal/test/jobs/complete", post(complete_job))
                .route_layer(middleware::from_fn_with_state(
                    Arc::<str>::from(token),
                    require_test_api_token,
                ))
                .with_state(shared),
        ),
        None => router,
    }
}

async fn require_protocol_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = bearer_token(&request).is_some_and(|token| {
        token == shared.state.system_token || shared.state.verify_local_jwt_claims(token).is_some()
    });
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "runner or job protocol token required",
        ))
    }
}

async fn require_test_api_token(
    State(expected): State<Arc<str>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == expected.as_ref());
    if !authorized {
        warn!(path = %request.uri().path(), "rejected privileged test API request");
        return Err(ApiError::unauthorized("missing or invalid test API token"));
    }
    warn!(path = %request.uri().path(), "privileged test API request");
    Ok(next.run(request).await)
}

async fn require_native_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = bearer_token(&request).is_some_and(|token| token == shared.state.system_token);
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized(
            "missing or invalid native API token",
        ))
    }
}

async fn require_runner_bearer(
    State(shared): State<Arc<SharedState>>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let authorized = bearer_token(&request)
        .and_then(|token| shared.state.runner_id_from_token(token))
        .is_some();
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("runner listen token required"))
    }
}

fn bearer_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

impl AppState {
    fn local_jwt(&self, mut claims: serde_json::Value) -> Result<String, ApiError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
            .as_secs();
        let claims = claims
            .as_object_mut()
            .ok_or_else(|| ApiError::bad_request("JWT claims must be an object"))?;
        claims.insert("iss".to_owned(), json!("https://aksh.local"));
        claims.insert("iat".to_owned(), json!(now));
        claims.insert("nbf".to_owned(), json!(now));
        claims.insert("exp".to_owned(), json!(now + 2999));
        let header = json!({
            "alg": "HS256",
            "typ": "JWT",
            "kid": "aksh-local"
        });
        let signing_input = format!(
            "{}.{}",
            base64_url_json(&header)?,
            base64_url_json(&serde_json::Value::Object(claims.clone()))?
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key)
            .map_err(|error| ApiError::bad_request(format!("invalid signing key: {error}")))?;
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{signing_input}.{signature}"))
    }

    fn verify_local_jwt_claims(&self, token: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = token.splitn(3, '.').collect();
        if parts.len() != 3 {
            return None;
        }
        let header_bytes = URL_SAFE_NO_PAD.decode(parts[0]).ok()?;
        let header: serde_json::Value = serde_json::from_slice(&header_bytes).ok()?;
        if header.get("alg").and_then(|value| value.as_str()) != Some("HS256") {
            return None;
        }
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let exp = payload.get("exp").and_then(|value| value.as_u64())?;
        let nbf = payload.get("nbf").and_then(|value| value.as_u64());
        if exp <= now || nbf.is_some_and(|value| value > now + 30) {
            return None;
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key).ok()?;
        mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).ok()?;
        mac.verify_slice(&signature).ok()?;
        Some(payload)
    }

    fn verify_local_jwt_scope(&self, token: &str, expected_scope: &str) -> bool {
        self.verify_local_jwt_claims(token)
            .and_then(|payload| {
                payload
                    .get("scp")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            })
            .as_deref()
            == Some(expected_scope)
    }

    fn runner_id_from_token(&self, token: &str) -> Option<i64> {
        let payload = self.verify_local_jwt_claims(token)?;
        let scope = payload.get("scp")?.as_str()?;
        if !scope
            .split_whitespace()
            .any(|value| value == "ActionsRuntime.RunnerListen")
        {
            return None;
        }
        payload
            .get("sub")?
            .as_str()?
            .strip_prefix("aksh-runner-listen-")?
            .parse()
            .ok()
    }

    fn mint_runtime_token(&self, plan_id: &str, job_id: &uuid::Uuid) -> String {
        self.local_jwt(json!({
            "sub": format!("aksh-job-{job_id}"),
            "scp": format!("Actions.Results:{plan_id}:{job_id}"),
        }))
        .expect("fixed local JWT claims must serialize")
    }
}

#[derive(Clone)]
pub struct SharedState {
    /// Inner application state.
    pub state: AppState,
    /// Cancellation token for graceful shutdown.
    pub shutdown: CancellationToken,
}

/// Application state.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Mutex<InnerState>>,
    events: broadcast::Sender<NdjsonEvent>,
    message_notify: Arc<Notify>,
    cache: CacheStore,
    artifacts: ArtifactStore,
    /// Optional GitHub App Webhook Secret for signature verification.
    pub webhook_secret: Option<String>,
    /// Optional local workspace path to load workflows from.
    pub local_workspace: Option<PathBuf>,
    /// State directory for replay/log storage.
    pub state_dir: PathBuf,
    /// Native API administrator credential for this server instance.
    system_token: String,
    /// Per-instance HMAC key for runner and job JWTs.
    local_jwt_key: Vec<u8>,
    /// Optional cron scheduler (active when `--enable-scheduler` is set).
    pub scheduler: Option<Arc<crate::scheduler::Scheduler>>,
}

#[derive(Debug, Clone)]
struct OidcJobContext {
    environment: Option<String>,
    job_workflow_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct JobSetId {
    run_id: RunId,
    job_ids: BTreeSet<JobId>,
}

impl JobSetId {
    fn holder(&self) -> concurrency::Holder {
        concurrency::Holder::JobSet {
            run_id: self.run_id,
            job_ids: self.job_ids.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct JobSetGate {
    key: (String, String),
    display_name: String,
    cancel_in_progress: bool,
    queue: aksh_gha_parser::ConcurrencyQueue,
}

#[derive(Debug, Clone)]
struct JobSetAdmission {
    gates: Vec<JobSetGate>,
    acquired_keys: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobSetAdmissionResult {
    Ready,
    Blocked,
}

impl AppState {
    pub async fn new(state_dir: PathBuf) -> anyhow::Result<Self> {
        let cache = CacheStore::new(state_dir.join("cache")).await?;
        let artifacts = ArtifactStore::new(state_dir.join("artifacts")).await?;
        let (events, _) = broadcast::channel(1024);
        let keypair = AgentRsaKeypair::generate()
            .map_err(|e| anyhow::anyhow!("Failed to generate RSA keypair: {}", e))?;
        let oidc_keypair = load_or_generate_oidc_keypair(&state_dir)?;
        let system_token =
            env::var("AKSH_SYSTEM_TOKEN").unwrap_or_else(|_| DEFAULT_AKSH_SYSTEM_TOKEN.to_owned());
        #[cfg(test)]
        let local_jwt_key = TEST_LOCAL_JWT_KEY.to_vec();
        #[cfg(not(test))]
        let local_jwt_key = load_or_generate_hmac_key(&state_dir)?;
        let registry_path = state_dir.join("artifact_v2_registry.json");
        let (registry, next_id) = if registry_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&registry_path) {
                if let Ok(map) = serde_json::from_str::<BTreeMap<String, ArtifactV2Entry>>(&content)
                {
                    let max_id = map.values().map(|e| e.id).max().unwrap_or(0);
                    (map, max_id)
                } else {
                    (BTreeMap::new(), 0)
                }
            } else {
                (BTreeMap::new(), 0)
            }
        } else {
            (BTreeMap::new(), 0)
        };
        let inner = InnerState {
            agent_keypair: Some(keypair),
            artifact_v2_registry: registry,
            next_artifact_v2_id: next_id,
            oidc_keypair: Some(oidc_keypair),
            ..Default::default()
        };
        let webhook_secret = std::env::var("AKSH_WEBHOOK_SECRET").ok();
        let local_workspace = std::env::var("AKSH_LOCAL_WORKSPACE")
            .ok()
            .map(PathBuf::from);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            events,
            message_notify: Arc::new(Notify::new()),
            cache,
            artifacts,
            webhook_secret,
            local_workspace,
            state_dir,
            system_token,
            local_jwt_key,
            scheduler: None,
        })
    }

    async fn emit(&self, event: NdjsonEvent) {
        let _ = self.events.send(event);
    }
}
#[cfg(test)]
fn mint_runtime_token(plan_id: &str, job_id: &uuid::Uuid) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after epoch")
        .as_secs();
    let header = json!({"alg": "HS256", "typ": "JWT", "kid": "aksh-local"});
    let claims = json!({
        "iss": "https://aksh.local",
        "iat": now,
        "nbf": now,
        "exp": now + 2999,
        "sub": format!("aksh-job-{job_id}"),
        "scp": format!("Actions.Results:{plan_id}:{job_id}"),
    });
    let signing_input = format!(
        "{}.{}",
        base64_url_json(&header).expect("test header serializes"),
        base64_url_json(&claims).expect("test claims serialize"),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(TEST_LOCAL_JWT_KEY).expect("test key is valid");
    mac.update(signing_input.as_bytes());
    format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

/// Load the OIDC signing keypair from `<state_dir>/oidc-key.json`, or generate
/// a new one and persist it for reuse across restarts.
fn load_or_generate_oidc_keypair(state_dir: &std::path::Path) -> anyhow::Result<oidc::OidcKeypair> {
    let key_path = state_dir.join("oidc-key.json");
    if key_path.exists() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            let mode = std::fs::metadata(&key_path)?.mode();
            if mode & 0o077 != 0 {
                std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
            }
        }
        let content = std::fs::read_to_string(&key_path).map_err(|error| {
            anyhow::anyhow!("failed to read OIDC key {}: {error}", key_path.display())
        })?;
        let params =
            serde_json::from_str::<aksh_gha_protocol::crypto::RsaParametersExport>(&content)
                .map_err(|error| {
                    anyhow::anyhow!("invalid OIDC key {}: {error}", key_path.display())
                })?;
        return oidc::OidcKeypair::from_params(&params);
    }

    let kp = oidc::OidcKeypair::generate()?;
    let json = serde_json::to_vec(&kp.params())?;
    std::fs::create_dir_all(state_dir)?;
    let temp_path = state_dir.join(format!("oidc-key.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp_path)?;
    use std::io::Write;
    file.write_all(&json)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp_path, &key_path).map_err(|error| {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::anyhow!("failed to persist OIDC key {}: {error}", key_path.display())
    })?;
    Ok(kp)
}

/// Load or generate a 32-byte HMAC key for local JWT signing.
///
/// Persisted to `<state_dir>/hmac-key.bin` so runtime tokens survive restarts.
fn load_or_generate_hmac_key(state_dir: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let key_path = state_dir.join("hmac-key.bin");
    if key_path.exists() {
        let key = std::fs::read(&key_path).map_err(|error| {
            anyhow::anyhow!("failed to read HMAC key {}: {error}", key_path.display())
        })?;
        if key.len() == 32 {
            return Ok(key);
        }
        // Wrong size — regenerate.
    }
    let mut key = vec![0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut key);
    std::fs::create_dir_all(state_dir)?;
    let temp_path = state_dir.join(format!("hmac-key.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp_path)?;
    use std::io::Write;
    file.write_all(&key)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp_path, &key_path).map_err(|error| {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::anyhow!("failed to persist HMAC key {}: {error}", key_path.display())
    })?;
    Ok(key)
}

impl InnerState {
    /// Look up the labels for the runner that owns a given session.
    fn runner_labels_for_session(&self, session_id: &str) -> Vec<String> {
        let runner_id = self
            .broker_session_runners
            .get(session_id)
            .copied()
            .or_else(|| {
                self.sessions
                    .get(session_id)
                    .map(|session| session.runner_id)
            });
        runner_id
            .and_then(|runner_id| self.runners.get(&runner_id))
            .map(|runner| runner.labels.clone())
            .unwrap_or_default()
    }
}

#[derive(Default)]
#[allow(dead_code)]
struct InnerState {
    runs: BTreeMap<RunId, RunRecord>,
    queue: VecDeque<QueuedJob>,
    pending_jobs: VecDeque<QueuedJob>,
    runners: BTreeMap<i64, RegisteredRunner>,
    sessions: BTreeMap<String, RunnerSession>,
    session_keys: BTreeMap<String, SessionEncryption>,
    agent_keypair: Option<AgentRsaKeypair>,
    runner_public_keys: BTreeMap<i64, String>,
    runner_rsa_public_keys: BTreeMap<i64, AgentRsaPublicKey>,
    inflight_messages: BTreeMap<String, BTreeMap<i64, azdo::TaskAgentMessage>>,
    broker_messages: BTreeMap<i64, azdo::AgentJobRequestMessage>,
    runner_client_ids: BTreeMap<String, i64>,
    cancellation_queue: VecDeque<QueuedCancellation>,
    pending_caches: BTreeMap<i64, PendingCache>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    logs: BTreeMap<String, Vec<u8>>,
    timeline_events: BTreeMap<RunId, Vec<NdjsonEvent>>,
    live_log_lines: BTreeMap<String, Arc<tokio::sync::Mutex<Vec<LiveLogFeedLinesWrapper>>>>,
    live_log_tx: BTreeMap<String, broadcast::Sender<LiveLogFeedLinesWrapper>>,
    inflight_requests: BTreeMap<i64, (RunId, JobId)>,
    job_requests: BTreeMap<i64, TaskAgentJobRequestRecord>,
    plan_requests: BTreeMap<String, i64>,
    agent_job_requests: BTreeMap<uuid::Uuid, i64>,
    timeline_requests: BTreeMap<uuid::Uuid, i64>,
    session_active_requests: BTreeMap<String, i64>,
    /// Modern broker session owner, derived from the runner-listen JWT.
    broker_session_runners: BTreeMap<String, i64>,
    next_runner_id: i64,
    next_cache_id: i64,
    next_message_id: i64,
    next_log_id: usize,
    next_request_id: i64,
    flows_file: Option<std::fs::File>,
    next_flow_index: usize,
    /// Sessions created via the AzDO distributedtask path (full encrypted message format).
    /// Sessions NOT in this set use the broker-ref (RunnerJobRequest) format.
    azdo_sessions: std::collections::HashSet<String>,
    /// Cache v2 Twirp pending uploads: upload_token → (key, version).
    cache_v2_pending: BTreeMap<String, CacheV2Pending>,
    /// Cache v2 download tokens: dl_token → (key, version).
    cache_v2_dl_tokens: BTreeMap<String, (String, String)>,
    /// Artifact v2 Twirp pending uploads: upload_token → registry_key.
    artifact_v2_pending: BTreeMap<String, ArtifactV2Pending>,
    /// Artifact v2 finalized registry: registry_key → metadata.
    artifact_v2_registry: BTreeMap<String, ArtifactV2Entry>,
    /// Monotonic artifact v2 ID counter.
    next_artifact_v2_id: u64,
    /// Per-job resolved OIDC execution context.
    oidc_job_contexts: BTreeMap<(RunId, JobId), OidcJobContext>,
    /// OIDC issuer URL used in the `iss` claim and discovery document.
    oidc_issuer: String,
    dap_ports: BTreeMap<RunId, DapPortRegistration>,
    /// OIDC signing keypair (RS256) for id-token minting.
    oidc_keypair: Option<oidc::OidcKeypair>,
    /// Per-job `id-token: write` grant, keyed by (run_id, job_id).
    id_token_grants: BTreeMap<(RunId, JobId), bool>,
    /// Concurrency groups keyed by (lowercased repo, lowercased group name).
    concurrency_groups: BTreeMap<(String, String), concurrency::ConcurrencyGroup>,
    /// Workflow-level pending runs: run_id → jobs held out of the ready queue.
    held_runs: BTreeMap<RunId, Vec<QueuedJob>>,
    /// Job-level concurrency-blocked jobs (FIFO).
    concurrency_blocked: VecDeque<QueuedJob>,
    /// Multi-key admission state for reusable workflow invocations.
    jobset_admissions: BTreeMap<JobSetId, JobSetAdmission>,
    /// Evaluated workflow-level concurrency raw config per run (for release/debug).
    run_concurrency: BTreeMap<RunId, aksh_gha_parser::Concurrency>,
    /// Which concurrency key a holder currently occupies (for release).
    holder_keys: BTreeMap<RunId, Vec<(String, String)>>,
}

#[derive(Debug, Clone)]
struct DapPortRegistration {
    port: u16,
    job_id: JobId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StepRecord {
    pub(crate) name: String,
    pub(crate) conclusion: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobDetail {
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) steps: Vec<StepRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunRecord {
    pub(crate) run_id: RunId,
    pub(crate) submission: WorkflowSubmission,
    pub(crate) jobs: BTreeMap<JobId, ExecutionStatus>,
    pub(crate) status: ExecutionStatus,
    pub(crate) job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    pub(crate) job_base_ids: BTreeMap<JobId, String>,
    #[serde(skip)]
    pub(crate) job_needs: BTreeMap<JobId, Vec<JobId>>,
    pub(crate) job_fail_fast: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) job_check_run_ids: BTreeMap<JobId, u64>,
    #[serde(default)]
    pub(crate) reusable_calls: BTreeMap<String, aksh_gha_parser::ReusableCallMetadata>,
    #[serde(default)]
    pub(crate) jobs_list: Vec<JobDetail>,
}

#[derive(Debug, Clone)]
struct TaskAgentJobRequestRecord {
    request_id: i64,
    run_id: RunId,
    job_id: JobId,
    agent_job_id: uuid::Uuid,
    plan_id: String,
    plan_type: String,
    timeline_id: uuid::Uuid,
    result: Option<ExecutionStatus>,
    locked_until: String,
    started_at: Option<std::time::SystemTime>,
    last_renewed_at: Option<std::time::SystemTime>,
    timeout_triggered: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct QueuedJob {
    run_id: RunId,
    job_id: JobId,
    base_id: String,
    needs: Vec<JobId>,
    if_condition: Option<String>,
    condition_context: aksh_gha_expressions::Context,
    fail_fast: bool,
    max_parallel: Option<u64>,
    /// Required runner labels from `runs-on`.
    runs_on: Vec<String>,
    message: azdo::AgentJobRequestMessage,
    /// Raw job-level concurrency (evaluated when the job becomes ready).
    concurrency: Option<aksh_gha_parser::Concurrency>,
    /// Matrix values for this expansion (for concurrency expression eval).
    matrix: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
struct QueuedCancellation {
    run_id: RunId,
    job_id: JobId,
    /// Agent job GUID from the job message (`jobId`), required for official JobCancelMessage.
    agent_job_id: uuid::Uuid,
}

#[derive(Debug, Clone)]
struct PendingCache {
    key: String,
    version: String,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactRecord {
    id: String,
    run_id: RunId,
    name: String,
    file_name: String,
    path: String,
    size: u64,
}

/// Pending cache v2 upload (Twirp CacheService).
#[derive(Debug)]
struct CacheV2Pending {
    key: String,
    version: String,
}

/// Pending artifact v2 upload (Twirp ArtifactService).
#[derive(Debug)]
struct ArtifactV2Pending {
    /// Registry key = "{run_backend_id}/{job_backend_id}/{name}".
    registry_key: String,
}

/// Finalized artifact v2 entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactV2Entry {
    id: u64,
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    size: u64,
    created_at: String,
    digest: Option<String>,
    /// Upload token used to find the assembled blob on disk.
    blob_token: String,
}

// Re-export from protocol crate — shared wire type with the runner.
use aksh_gha_protocol::LiveLogFeedLinesWrapper;

#[derive(Debug, Deserialize)]
struct CachePutRequest {
    key: String,
    version: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
struct CacheQuery {
    key: Option<String>,
    keys: Option<String>,
    version: String,
}

#[derive(Debug, Serialize)]
struct CacheLookupResponse {
    hit: bool,
    key: Option<String>,
    version: Option<String>,
    size: Option<u64>,
    content_base64: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheReserveRequest {
    key: String,
    version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CacheReserveResponse {
    cache_id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheCommitRequest {
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ArtifactPutRequest {
    run_id: RunId,
    name: String,
    file_name: String,
    #[serde(default)]
    content_base64: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArtifactCreateRequest {
    name: String,
    #[serde(default = "default_artifact_file_name")]
    file_name: String,
}

fn default_artifact_file_name() -> String {
    "artifact.bin".to_owned()
}

async fn healthz(State(shared): State<Arc<SharedState>>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shared.shutdown.is_cancelled(),
    }))
}

pub(crate) async fn submit_run_inner(
    shared: &Arc<SharedState>,
    mut submission: WorkflowSubmission,
) -> Result<RunAccepted, ApiError> {
    let workflow = parse_workflow(&submission.workflow_yaml)?;
    if submission.event == "workflow_dispatch" {
        workflow.apply_workflow_dispatch_inputs(&mut submission.payload)?;
        if submission.dispatch_inputs.is_empty() {
            submission.dispatch_inputs = submission
                .payload
                .get("inputs")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
        }
        if submission.dispatch_inputs_stringified.is_empty() {
            submission.dispatch_inputs_stringified = submission
                .dispatch_inputs
                .iter()
                .map(|(name, value)| (name.clone(), value_to_input_string(value)))
                .collect();
        }
        if let Some(object) = submission.payload.as_object_mut() {
            object.insert(
                "inputs".to_owned(),
                serde_json::to_value(&submission.dispatch_inputs_stringified).unwrap_or_default(),
            );
        }
    }
    if let Some(tier) = submission.trust_tier.as_deref().and_then(|value| {
        serde_json::from_value::<crate::events::trust_tier::TrustTier>(json!(value)).ok()
    }) {
        if !tier.allows_secrets() {
            submission.secrets.clear();
        }
    }
    let (branch, tag) = {
        let (default_branch, default_tag) = git_ref_context(&submission.git_ref);
        let filter_branch = submission.filter_branch.clone().or_else(|| {
            if matches!(
                submission.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else if submission.event == "workflow_run" {
                submission
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else {
                None
            }
        });
        if filter_branch.is_some() {
            (filter_branch, None)
        } else {
            (default_branch, default_tag)
        }
    };
    let payload_has_paths =
        submission.payload.get("paths").is_some() || submission.payload.get("commits").is_some();
    let changed_paths_known = submission.changed_paths_known || payload_has_paths;
    let changed_paths = if submission.changed_paths_known {
        submission.changed_paths.clone()
    } else {
        changed_paths_from_payload(&submission.payload)
    };
    if !changed_paths_known && workflow.on.has_path_filters(&submission.event) {
        return Err(ApiError::bad_request(
            "workflow path filters require a complete changed-file list".to_owned(),
        ));
    }
    // Activity type from explicit field (set by dispatcher) or payload.action fallback.
    let activity_owned: Option<String> = submission.activity_type.clone().or_else(|| {
        submission
            .payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    });
    let activity_type = activity_owned.as_deref();
    if !workflow.on.matches_with_context(
        &submission.event,
        branch.as_deref(),
        tag.as_deref(),
        &changed_paths,
        activity_type,
        &submission.workflow_run_upstream_names,
    ) {
        return Err(ApiError::bad_request(format!(
            "workflow does not match event `{}`",
            submission.event
        )));
    }
    let expanded = expand_jobs_with_reusables(&workflow, &submission.reusable_workflows)?;
    let mut jobs = expanded.jobs;
    if !submission.dispatch_inputs.is_empty() {
        for job in &mut jobs {
            job.inputs = submission.dispatch_inputs.clone();
        }
    }
    let reusable_calls = expanded.reusable_calls;
    let run_id = RunId::new();
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();
    let sha = submission
        .resolved_sha
        .clone()
        .or_else(|| {
            submission
                .payload
                .get("after")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            if submission.git_ref.len() == 40
                && submission
                    .git_ref
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
            {
                submission.git_ref.clone()
            } else {
                "0000000000000000000000000000000000000000".to_owned()
            }
        })
        .to_string();
    let workflow_path = submission
        .workflow_path
        .clone()
        .unwrap_or_else(|| ".github/workflows/workflow.yml".to_owned());
    let workflow_ref = format!(
        "{}/{}@{}",
        submission.repository, workflow_path, submission.git_ref
    );

    let ref_name = submission
        .git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| submission.git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(&submission.git_ref)
        .to_owned();
    let ref_type = if submission.git_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        "branch"
    };

    let github = json!({
        "ref": submission.git_ref,
        "sha": sha,
        "repository": submission.repository,
        "repository_owner": repository_owner,
        "repository_owner_id": "0",
        "repositoryUrl": format!("git://github.com/{}.git", submission.repository),
        "run_id": run_id.to_string(),
        "run_number": "1",
        "retention_days": "90",
        "run_attempt": "1",
        "artifact_cache_size_limit": "10",
        "repository_visibility": "private",
        "actor_id": "0",
        "actor": "aksh-system",
        "workflow": workflow.name.clone().unwrap_or_default(),
        "head_ref": "",
        "base_ref": "",
        "event_name": submission.event,
        "server_url": "https://github.com",
        "api_url": "https://api.github.com",
        "graphql_url": "https://api.github.com/graphql",
        "ref_name": ref_name,
        "ref_protected": false,
        "ref_type": ref_type,
        "secret_source": "Actions",
        "event": submission.payload,
        "workflow_ref": workflow_ref,
        "workflow_sha": sha,
        "repository_id": "0",
        "triggering_actor": "aksh-system"
    });

    // Evaluate workflow-level concurrency before locking (pure).
    let workflow_concurrency = workflow.concurrency.clone();
    let mut empty_workflow_concurrency_group = false;
    let workflow_concurrency_eval = if let Some(raw) = &workflow_concurrency {
        let eval_ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Workflow,
            github: &github,
            vars: &submission.vars,
            inputs: &submission.inputs,
            matrix: None,
            strategy: None,
            needs: None,
        };
        let (group, cancel, queue) =
            concurrency::evaluate_concurrency(raw, &eval_ctx).map_err(|error| {
                ApiError::bad_request(format!("concurrency evaluation failed: {error}"))
            })?;
        if group.trim().is_empty() {
            empty_workflow_concurrency_group = true;
            None
        } else {
            Some((group, cancel, queue, raw.clone()))
        }
    } else {
        None
    };

    {
        let mut inner = shared.state.inner.lock().await;
        let mut statuses = BTreeMap::new();
        let mut ready_jobs = 0usize;
        let mut job_base_ids = BTreeMap::new();
        let mut job_needs = BTreeMap::new();
        let mut job_fail_fast = BTreeMap::new();
        let mut ready_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let mut initially_skipped = Vec::new();
        let mut built_jobs: Vec<QueuedJob> = Vec::new();
        if empty_workflow_concurrency_group {
            let queued_jobs = 0;
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    submission,
                    jobs: BTreeMap::new(),
                    job_outputs: BTreeMap::new(),
                    job_base_ids: BTreeMap::new(),
                    job_needs: BTreeMap::new(),
                    job_fail_fast: BTreeMap::new(),
                    status: ExecutionStatus::Failure,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Failure,
                    reason: Some("concurrency group name must not be empty".to_owned()),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                queued_jobs,
            });
        }
        for job in jobs {
            job_base_ids.insert(job.id.clone(), job.base_id.clone());
            job_needs.insert(job.id.clone(), job.needs.clone());
            job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
            statuses.insert(job.id.clone(), ExecutionStatus::Queued);
            let condition_context = build_context(
                &github,
                &BTreeMap::new(),
                &submission.vars,
                &indexmap::IndexMap::new(),
                &serde_json::json!({}),
                &BTreeMap::new(),
                &job.inputs,
            );
            if job.needs.is_empty() {
                let condition =
                    aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
                let should_run = aksh_gha_expressions::eval_bool(&condition, &condition_context)
                    .map_err(|error| {
                        ApiError::bad_request(format!(
                            "failed to evaluate condition for job `{}`: {error}",
                            job.id
                        ))
                    })?;
                if !should_run {
                    statuses.insert(job.id.clone(), ExecutionStatus::Skipped);
                    initially_skipped.push((run_id, job.id.clone()));
                    continue;
                }
            }
            let mut agent_msg = aksh_gha_parser::job_builder::build_agent_job_message(
                &job,
                &github,
                &job.env,
                &submission
                    .secrets
                    .iter()
                    .map(|(k, v)| (k.clone(), v.expose().to_owned()))
                    .collect(),
                &submission.vars,
            )
            .map_err(|e| ApiError::bad_request(format!("failed to build job message: {e}")))?;

            let id_token_granted = job.oidc_id_token_granted;
            inner
                .id_token_grants
                .insert((run_id, job.id.clone()), id_token_granted);
            inner.oidc_job_contexts.insert(
                (run_id, job.id.clone()),
                OidcJobContext {
                    environment: job.oidc_environment.clone(),
                    job_workflow_ref: job.oidc_job_workflow_ref.clone(),
                },
            );
            inner.next_request_id += 1;
            let request_id = inner.next_request_id;
            agent_msg.request_id = request_id;
            if id_token_granted {
                let oidc_url = format!(
                    "{}/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?api-version=2.0",
                    public_base_url(),
                    agent_msg.plan.plan_id,
                    agent_msg.job_id,
                );
                for endpoint in &mut agent_msg.resources.endpoints {
                    if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                        endpoint
                            .data
                            .insert("GenerateIdTokenUrl".to_owned(), oidc_url.clone());
                    }
                }
            }
            // Mint a dynamic JWT for the job and inject it as GITHUB_TOKEN.
            let token = shared
                .state
                .mint_runtime_token(&agent_msg.plan.plan_id, &agent_msg.job_id);
            agent_msg.variables.insert(
                "system.github.token".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            agent_msg.variables.insert(
                "github_token".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            agent_msg.variables.insert(
                "system.github.launch_endpoint".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(public_base_url()),
            );
            agent_msg.variables.insert(
                "system.github.results_endpoint".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(public_base_url()),
            );
            agent_msg.variables.insert(
                "system.orchestrationId".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(format!(
                    "{}.{}.{}",
                    agent_msg.plan.plan_id, job.base_id, agent_msg.job_name
                )),
            );
            if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(github_dict)) =
                &mut agent_msg.context_data.get_mut("github")
            {
                github_dict.insert(
                    "token".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(token),
                );
                let mut perms = std::collections::BTreeMap::new();
                for perm in &[
                    "actions",
                    "contents",
                    "issues",
                    "metadata",
                    "pull-requests",
                    "statuses",
                ] {
                    perms.insert(
                        perm.to_string(),
                        aksh_gha_protocol::azdo::PipelineContextData::String("write".to_string()),
                    );
                }
                github_dict.insert(
                    "token_permissions".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::Dict(perms),
                );
            }

            agent_msg.file_table = vec![workflow_path.clone()];
            if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(job_dict)) =
                agent_msg.context_data.get_mut("job")
            {
                job_dict.insert(
                    "check_run_id".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::Number(0.0),
                );
                job_dict.insert(
                    "workflow_ref".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(workflow_ref.clone()),
                );
                job_dict.insert(
                    "workflow_sha".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(sha.clone()),
                );
                job_dict.insert(
                    "workflow_repository".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(
                        submission.repository.clone(),
                    ),
                );
                job_dict.insert(
                    "workflow_file_path".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(workflow_path.clone()),
                );
            }

            agent_msg.enable_debugger = submission.enable_debugger;
            agent_msg.debugger_welcome_message = submission.debugger_welcome_message.clone();
            if submission.enable_debugger {
                agent_msg.aksh_debug_run_id = Some(run_id.to_string());
                agent_msg.aksh_debug_transport = Some("local".to_string());
            }
            inner
                .inflight_requests
                .insert(request_id, (run_id, job.id.clone()));
            let job_request = TaskAgentJobRequestRecord {
                request_id,
                run_id,
                job_id: job.id.clone(),
                agent_job_id: agent_msg.job_id,
                plan_id: agent_msg.plan.plan_id.clone(),
                plan_type: agent_msg.plan.plan_type.clone(),
                timeline_id: agent_msg.timeline.id,
                result: None,
                locked_until: agent_request_locked_until(),
                started_at: None,
                last_renewed_at: None,
                timeout_triggered: false,
            };
            inner
                .plan_requests
                .insert(job_request.plan_id.clone(), request_id);
            inner
                .agent_job_requests
                .insert(job_request.agent_job_id, request_id);
            inner
                .timeline_requests
                .insert(job_request.timeline_id, request_id);
            inner.job_requests.insert(request_id, job_request);

            let queued_job = QueuedJob {
                run_id,
                job_id: job.id.clone(),
                base_id: job.base_id.clone(),
                needs: job.needs.clone(),
                if_condition: job.if_condition.clone(),
                condition_context,
                fail_fast: job.fail_fast,
                max_parallel: job.max_parallel,
                runs_on: job.runs_on.clone(),
                message: agent_msg,
                concurrency: concurrency::concurrency_from_plan_fields(
                    job.concurrency_group.as_deref(),
                    job.concurrency_cancel_in_progress.as_deref(),
                    job.concurrency_queue.as_deref(),
                ),
                matrix: job
                    .matrix
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            job_base_ids.insert(job.id.clone(), job.base_id.clone());
            job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
            built_jobs.push(queued_job);
        }

        // Workflow-level concurrency gate.
        let mut hold_entire_run = false;
        if let Some((group, cancel, queue, raw)) = &workflow_concurrency_eval {
            let key = concurrency::concurrency_key(&submission.repository, group);
            match try_acquire_concurrency(
                &mut inner,
                key,
                group.clone(),
                concurrency::Holder::Run(run_id),
                *cancel,
                *queue,
            ) {
                Ok(true) => {
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Ok(false) => {
                    hold_entire_run = true;
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Err(e) if e == "concurrency_queue_overflow" => {
                    // Cancel this run immediately — all jobs Cancelled.
                    for job in &built_jobs {
                        statuses.insert(job.job_id.clone(), ExecutionStatus::Cancelled);
                    }
                    let queued_jobs = statuses.len();
                    inner.runs.insert(
                        run_id,
                        RunRecord {
                            run_id,
                            submission,
                            jobs: statuses,
                            job_outputs: BTreeMap::new(),
                            job_base_ids,
                            job_needs,
                            job_fail_fast,
                            status: ExecutionStatus::Cancelled,
                            job_check_run_ids: BTreeMap::new(),
                            reusable_calls,
                            jobs_list: Vec::new(),
                        },
                    );
                    drop(inner);
                    shared
                        .state
                        .emit(NdjsonEvent::RunAccepted {
                            run_id,
                            queued_jobs,
                        })
                        .await;
                    shared
                        .state
                        .emit(NdjsonEvent::RunStatus {
                            run_id,
                            status: ExecutionStatus::Cancelled,
                            reason: concurrency::cancelled_reason(),
                        })
                        .await;
                    return Ok(RunAccepted {
                        run_id,
                        queued_jobs,
                    });
                }
                Err(e) => {
                    return Err(ApiError::bad_request(e));
                }
            }
        }

        if hold_entire_run {
            for job in &built_jobs {
                statuses.insert(job.job_id.clone(), ExecutionStatus::Pending);
            }
            inner.held_runs.insert(run_id, built_jobs);
            let queued_jobs = statuses.len();
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    submission,
                    jobs: statuses,
                    job_outputs: BTreeMap::new(),
                    job_base_ids,
                    job_needs,
                    job_fail_fast,
                    status: ExecutionStatus::Pending,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Pending,
                    reason: concurrency::pending_reason(),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                queued_jobs,
            });
        }
        // Install a provisional run before evaluating per-job and JobSet gates.
        // Multiple holders from this same submission can cancel each other;
        // cancellation helpers need the run to exist so they can persist the
        // affected job conclusion instead of silently becoming no-ops.
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                submission: submission.clone(),
                jobs: statuses.clone(),
                job_outputs: BTreeMap::new(),
                job_base_ids: job_base_ids.clone(),
                job_needs: job_needs.clone(),
                job_fail_fast: job_fail_fast.clone(),
                status: ExecutionStatus::Queued,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls: reusable_calls.clone(),
                jobs_list: Vec::new(),
            },
        );

        // Reusable workflow invocations must acquire caller and embedded
        // concurrency gates as one ordered, deduplicated admission set. A
        // partially admitted JobSet keeps its earlier keys while waiting on
        // the next key, preventing it from bypassing either scope.
        let mut jobset_blocked: std::collections::HashSet<JobId> = std::collections::HashSet::new();
        for call in reusable_calls.values() {
            let member_ids: BTreeSet<JobId> = call
                .inner_job_ids
                .iter()
                .map(|id| JobId(id.clone()))
                .collect();
            let id = JobSetId {
                run_id,
                job_ids: member_ids.clone(),
            };
            let mut gates = Vec::new();
            let mut evaluation_failed = false;

            for (raw, scope, label, inputs) in [
                (
                    call.caller_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Job,
                    "caller concurrency (JobSet)",
                    &submission.inputs,
                ),
                (
                    call.embedded_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Workflow,
                    "embedded concurrency (JobSet)",
                    &call.inputs,
                ),
            ] {
                let Some(raw) = raw else { continue };
                let eval_ctx = concurrency::ConcurrencyContext {
                    scope,
                    github: &github,
                    vars: &submission.vars,
                    inputs,
                    matrix: Some(&call.matrix),
                    strategy: None,
                    needs: None,
                };
                match concurrency::evaluate_concurrency(raw, &eval_ctx) {
                    Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                        merge_jobset_gate(
                            &mut gates,
                            JobSetGate {
                                key: concurrency::concurrency_key(&submission.repository, &group),
                                display_name: group,
                                cancel_in_progress,
                                queue,
                            },
                        );
                    }
                    Ok((_, _, _)) => {
                        evaluation_failed = true;
                    }
                    Err(error) => {
                        concurrency::log_eval_error(label, &error);
                        evaluation_failed = true;
                    }
                }
            }

            if evaluation_failed {
                for member_id in &member_ids {
                    statuses.insert(member_id.clone(), ExecutionStatus::Failure);
                }
                jobset_blocked.extend(member_ids);
                continue;
            }
            if gates.is_empty() {
                continue;
            }

            inner.jobset_admissions.insert(
                id.clone(),
                JobSetAdmission {
                    gates,
                    acquired_keys: BTreeSet::new(),
                },
            );
            match advance_jobset_admission(&mut inner, &id, None) {
                Ok(JobSetAdmissionResult::Ready) => {}
                Ok(JobSetAdmissionResult::Blocked) => {
                    jobset_blocked.extend(member_ids);
                }
                Err(error) => {
                    let status = if error == "concurrency_queue_overflow" {
                        ExecutionStatus::Cancelled
                    } else {
                        ExecutionStatus::Failure
                    };
                    for member_id in &member_ids {
                        statuses.insert(member_id.clone(), status);
                    }
                    jobset_blocked.extend(member_ids);
                }
            }
        }

        // Enqueue jobs (workflow concurrency free / acquired).
        for queued_job in built_jobs {
            let job_id = queued_job.job_id.clone();
            let base_id = queued_job.base_id.clone();

            // A blocked JobSet member must remain durably parked until every
            // required key is acquired. Terminal members are not parked.
            if jobset_blocked.contains(&job_id) {
                let status = statuses.get(&job_id).copied();
                if status.is_some_and(concurrency::is_awaiting_execution) {
                    statuses.insert(job_id, ExecutionStatus::Pending);
                    inner.concurrency_blocked.push_back(queued_job);
                }
                continue;
            }

            let needs_empty = queued_job.needs.is_empty();
            let max_parallel = queued_job.max_parallel;
            let under_mp = max_parallel
                .is_none_or(|max| ready_by_base.get(&base_id).copied().unwrap_or(0) < max);

            if needs_empty && under_mp {
                // Job-level concurrency gate (needs/max_parallel already satisfied).
                match try_enqueue_with_job_concurrency(
                    &mut inner,
                    &github,
                    &submission,
                    queued_job,
                    &mut statuses,
                ) {
                    Ok(true) => {
                        *ready_by_base.entry(base_id).or_default() += 1;
                        ready_jobs += 1;
                    }
                    Ok(false) => {
                        // parked pending
                    }
                    Err(_) => {
                        // cancelled by queue overflow or eval failure already marked
                    }
                }
            } else {
                statuses.insert(job_id, ExecutionStatus::Queued);
                inner.pending_jobs.push_back(queued_job);
            }
        }

        // Preserve terminal conclusions written through cancel_job_inner while
        // gates were evaluated. Non-terminal scheduling state remains owned by
        // the local status map and is installed below with the final record.
        if let Some(provisional) = inner.runs.get(&run_id) {
            for (job_id, status) in &provisional.jobs {
                if concurrency::is_terminal(*status) {
                    statuses.insert(job_id.clone(), *status);
                }
            }
        }

        let queued_jobs = statuses.len();
        // C-05: derive the initial run status from job statuses so that eval
        // failures (Failure) are reflected immediately rather than leaving the
        // run permanently Queued. summarize_run returns InProgress for any mix
        // of Queued/Pending jobs; map that to Queued since no job has started.
        let initial_status = {
            let s = summarize_run(statuses.values().copied());
            if s == ExecutionStatus::InProgress {
                ExecutionStatus::Queued
            } else {
                s
            }
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                submission,
                jobs: statuses,
                job_outputs: BTreeMap::new(),
                job_base_ids,
                job_needs,
                job_fail_fast,
                status: initial_status,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls,
                jobs_list: Vec::new(),
            },
        );
        let cancel_count = inner.cancellation_queue.len();
        drop(inner);
        if ready_jobs > 0 || cancel_count > 0 {
            shared.state.message_notify.notify_waiters();
        }
        for (event_run_id, job_id) in initially_skipped {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id: event_run_id,
                    job_id,
                    status: ExecutionStatus::Skipped,
                    reason: None,
                })
                .await;
        }
        shared
            .state
            .emit(NdjsonEvent::RunAccepted {
                run_id,
                queued_jobs,
            })
            .await;
        Ok(RunAccepted {
            run_id,
            queued_jobs,
        })
    }
}

/// Enqueue a ready job, applying job-level concurrency if present.
/// Returns Ok(true) if pushed to ready queue, Ok(false) if parked, Err if cancelled.
fn try_enqueue_with_job_concurrency(
    inner: &mut InnerState,
    github: &serde_json::Value,
    submission: &WorkflowSubmission,
    queued_job: QueuedJob,
    statuses: &mut BTreeMap<JobId, ExecutionStatus>,
) -> Result<bool, ()> {
    let Some(raw) = queued_job.concurrency.clone() else {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
        inner.queue.push_back(queued_job);
        return Ok(true);
    };

    let strategy = queued_job
        .message
        .context_data
        .get("strategy")
        .map(azdo::PipelineContextData::to_json)
        .unwrap_or_else(|| json!({}));
    let eval_ctx = concurrency::ConcurrencyContext {
        scope: concurrency::ConcurrencyScope::Job,
        github,
        vars: &submission.vars,
        inputs: &submission.inputs,
        matrix: Some(&queued_job.matrix),
        strategy: Some(&strategy),
        needs: None,
    };
    let eval = concurrency::evaluate_concurrency(&raw, &eval_ctx);
    let (group, cancel, queue) = match eval {
        Ok(v) => v,
        Err(e) => {
            concurrency::log_eval_error("job concurrency", &e);
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            return Err(());
        }
    };
    if group.trim().is_empty() {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
        return Err(());
    }

    let key = concurrency::concurrency_key(&submission.repository, &group);
    let holder = concurrency::Holder::Job {
        run_id: queued_job.run_id,
        job_id: queued_job.job_id.clone(),
    };
    match try_acquire_concurrency(inner, key, group, holder, cancel, queue) {
        Ok(true) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
            inner.queue.push_back(queued_job);
            Ok(true)
        }
        Ok(false) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Pending);
            inner.concurrency_blocked.push_back(queued_job);
            Ok(false)
        }
        Err(e) if e == "concurrency_queue_overflow" => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Cancelled);
            let _ = queued_job;
            Err(())
        }
        Err(_) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            Err(())
        }
    }
}
async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    Json(submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    submit_run_inner(&shared, submission).await.map(Json)
}

async fn get_scheduler_history(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<Vec<crate::scheduler::ScheduleFire>>, ApiError> {
    if let Some(scheduler) = &shared.state.scheduler {
        let history = scheduler.history.lock().await.clone();
        Ok(Json(history))
    } else {
        Ok(Json(vec![]))
    }
}

fn value_to_input_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => value.to_string(),
    }
}

fn git_ref_context(git_ref: &str) -> (Option<String>, Option<String>) {
    if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
        (Some(branch.to_owned()), None)
    } else if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
        (None, Some(tag.to_owned()))
    } else {
        (None, None)
    }
}

fn changed_paths_from_payload(payload: &serde_json::Value) -> Vec<String> {
    let mut paths = Vec::new();

    if let Some(values) = payload.get("paths").and_then(|value| value.as_array()) {
        collect_string_array(values, &mut paths);
    }

    if let Some(commits) = payload.get("commits").and_then(|value| value.as_array()) {
        for commit in commits {
            for field in ["added", "modified", "removed"] {
                if let Some(values) = commit.get(field).and_then(|value| value.as_array()) {
                    collect_string_array(values, &mut paths);
                }
            }
        }
    }

    paths.sort();
    paths.dedup();
    paths
}

fn collect_string_array(values: &[serde_json::Value], out: &mut Vec<String>) {
    out.extend(
        values
            .iter()
            .filter_map(|value| value.as_str())
            .map(str::to_owned),
    );
}

async fn get_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let mut run = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    // Results/timeline updates only create details for dispatched jobs. Keep
    // the native jobs_list a complete projection by adding jobs that were
    // cancelled or failed before dispatch with an empty step list.
    for (job_id, status) in &run.jobs {
        if !run.jobs_list.iter().any(|detail| detail.name == job_id.0) {
            run.jobs_list.push(JobDetail {
                name: job_id.0.clone(),
                conclusion: status_string(*status),
                steps: Vec::new(),
            });
        }
    }

    for job_detail in &mut run.jobs_list {
        if let Some(status) = run.jobs.get(&JobId(job_detail.name.clone())) {
            job_detail.conclusion = status_string(*status);
        }
    }

    Ok(Json(run))
}

async fn get_run_logs(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    let (state_dir, sources) = {
        let inner = shared.state.inner.lock().await;
        if !inner.runs.contains_key(&run_id) {
            return Err(ApiError::not_found("run not found"));
        }

        let mut requests: Vec<&TaskAgentJobRequestRecord> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        let sources = requests
            .into_iter()
            .map(|request| {
                let prefix = format!("{}/", request.plan_id);
                let mut blocks: Vec<(&str, &[u8])> = inner
                    .logs
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .map(|log_id| (log_id, value.as_slice()))
                    })
                    .collect();
                blocks.sort_by(|(left, _), (right, _)| {
                    match (left.parse::<u64>(), right.parse::<u64>()) {
                        (Ok(left), Ok(right)) => left.cmp(&right),
                        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                        (Err(_), Err(_)) => left.cmp(right),
                    }
                });
                (
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                    blocks
                        .into_iter()
                        .map(|(_, block)| block.to_vec())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        (shared.state.state_dir.clone(), sources)
    };

    let mut merged = Vec::new();
    for (plan_id, agent_job_id, fallback_blocks) in sources {
        let results_log = state_dir
            .join("replay")
            .join("results")
            .join(plan_id)
            .join(agent_job_id)
            .join("job-logs.txt");
        match tokio::fs::read(&results_log).await {
            Ok(contents) => merged.extend_from_slice(&contents),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for block in fallback_blocks {
                    merged.extend_from_slice(&block);
                }
            }
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "failed to read run log `{}`: {error}",
                    results_log.display()
                )));
            }
        }
    }

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(merged))
        .expect("static run log response"))
}

async fn cancel_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    if !inner.runs.contains_key(&run_id) {
        return Err(ApiError::not_found("run not found"));
    }
    let cancellation_count =
        cancel_run_inner(&mut inner, run_id, None /* no concurrency reason */);
    let record = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    drop(inner);
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Cancelled,
            reason: None,
        })
        .await;
    Ok(Json(record))
}

/// Resolve the agent job GUID for an in-flight job, if any.
fn agent_job_id_for(inner: &InnerState, run_id: RunId, job_id: &JobId) -> Option<uuid::Uuid> {
    inner
        .job_requests
        .values()
        .find(|r| r.run_id == run_id && r.job_id == *job_id && r.result.is_none())
        .map(|r| r.agent_job_id)
        .or_else(|| {
            // Also check via inflight_requests if result already set but still relevant.
            inner
                .job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == *job_id)
                .map(|r| r.agent_job_id)
        })
}

/// Cancel a run: mark non-terminal jobs Cancelled, enqueue JobCancellation for
/// in-flight jobs, remove from queues/held/blocked, and release concurrency.
/// Returns the number of cancellation messages enqueued.
fn cancel_run_inner(inner: &mut InnerState, run_id: RunId, reason: Option<&str>) -> usize {
    let mut in_progress: Vec<JobId> = Vec::new();
    {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        record.status = ExecutionStatus::Cancelled;
        for (job_id, status) in &mut record.jobs {
            if matches!(*status, ExecutionStatus::InProgress) {
                in_progress.push(job_id.clone());
            }
            if matches!(
                *status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            ) {
                *status = ExecutionStatus::Cancelled;
            }
        }
    }

    let mut cancellations = Vec::new();
    for job_id in in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, &job_id) {
            cancellations.push(QueuedCancellation {
                run_id,
                job_id,
                agent_job_id,
            });
        }
    }
    let count = cancellations.len();
    inner.cancellation_queue.extend(cancellations);

    inner.queue.retain(|job| job.run_id != run_id);
    inner.pending_jobs.retain(|job| job.run_id != run_id);
    inner.held_runs.remove(&run_id);
    inner.concurrency_blocked.retain(|job| job.run_id != run_id);
    inner.dap_ports.remove(&run_id);

    // Release any concurrency holders belonging to this run and promote next.
    release_concurrency_for_run(inner, run_id);
    inner.jobset_admissions.retain(|id, _| id.run_id != run_id);

    let _ = reason; // events emitted by caller when needed
    count
}

/// Cancel a single job (job-level concurrency / fail-fast style).
fn cancel_job_inner(inner: &mut InnerState, run_id: RunId, job_id: &JobId) -> usize {
    let was_in_progress = {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        let Some(status) = record.jobs.get_mut(job_id) else {
            return 0;
        };
        let in_progress = matches!(*status, ExecutionStatus::InProgress);
        if matches!(
            *status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        ) {
            *status = ExecutionStatus::Cancelled;
        }
        record.status = summarize_run(record.jobs.values().copied());
        in_progress
    };

    let mut count = 0;
    if was_in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, job_id) {
            inner.cancellation_queue.push_back(QueuedCancellation {
                run_id,
                job_id: job_id.clone(),
                agent_job_id,
            });
            count = 1;
        }
    }
    inner
        .queue
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .pending_jobs
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .concurrency_blocked
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    if let Some(held) = inner.held_runs.get_mut(&run_id) {
        held.retain(|j| j.job_id != *job_id);
    }

    release_concurrency_for_job(inner, run_id, job_id);
    count
}

fn release_concurrency_for_run(inner: &mut InnerState, run_id: RunId) {
    let keys: Vec<(String, String)> = inner.holder_keys.get(&run_id).cloned().unwrap_or_default();
    for key in keys {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            let running_match = group
                .running
                .as_ref()
                .is_some_and(|h| h.is_run_holder(run_id) || h.run_id() == run_id);
            if running_match {
                let done = group.running.take();
                if let Some(done) = done {
                    // Only release if all jobs terminal OR this was a cancel of the whole run.
                    promote_next_from_group(inner, &key, done);
                }
            } else {
                // Remove from pending queue.
                if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                    group.pending.retain(|h| h.run_id() != run_id);
                    if group.running.is_none() && group.pending.is_empty() {
                        inner.concurrency_groups.remove(&key);
                    }
                }
            }
        }
    }
    // C-07: discard all key tracking for this run now that every group has been released.
    inner.holder_keys.remove(&run_id);
}

fn release_concurrency_for_job(inner: &mut InnerState, run_id: RunId, job_id: &JobId) {
    let keys: Vec<(String, String)> = inner.concurrency_groups.keys().cloned().collect();
    for key in keys {
        let should_release = {
            let Some(group) = inner.concurrency_groups.get(&key) else {
                continue;
            };
            match &group.running {
                Some(h) if h.contains_job(run_id, job_id) => {
                    // Job holders release immediately; Run/JobSet when all terminal.
                    match h {
                        concurrency::Holder::Job { .. } => true,
                        concurrency::Holder::Run(_) | concurrency::Holder::JobSet { .. } => inner
                            .runs
                            .get(&run_id)
                            .is_some_and(|r| concurrency::holder_is_terminal(h, &r.jobs)),
                    }
                }
                _ => false,
            }
        };
        // Also drop pending entries for this job.
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| !h.contains_job(run_id, job_id));
        }
        if should_release {
            if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                if let Some(done) = group.running.take() {
                    promote_next_from_group(inner, &key, done);
                }
            }
        } else if let Some(group) = inner.concurrency_groups.get(&key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(&key);
            }
        }
        // C-07: prune this key from holder_keys when the run has no remaining
        // presence in the group (neither running nor pending).
        let run_still_present = inner.concurrency_groups.get(&key).is_some_and(|g| {
            g.running.as_ref().is_some_and(|h| h.run_id() == run_id)
                || g.pending.iter().any(|h| h.run_id() == run_id)
        });
        if !run_still_present {
            if let Some(rkeys) = inner.holder_keys.get_mut(&run_id) {
                rkeys.retain(|k| k != &key);
                if rkeys.is_empty() {
                    inner.holder_keys.remove(&run_id);
                }
            }
        }
    }
}

/// Release a single concurrency key acquired by a JobSet whose members all
/// became terminal before any could dispatch (e.g. embedded gate overflow).
/// Removes the running holder from the group and promotes the next pending.
fn merge_jobset_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
    if let Some(existing) = gates.iter_mut().find(|existing| existing.key == gate.key) {
        existing.cancel_in_progress |= gate.cancel_in_progress;
        if gate.queue == aksh_gha_parser::ConcurrencyQueue::Single {
            existing.queue = aksh_gha_parser::ConcurrencyQueue::Single;
        }
        return;
    }
    gate.display_name = gate.display_name.trim().to_owned();
    gates.push(gate);
    gates.sort_by(|left, right| left.key.cmp(&right.key));
}

fn release_holder_key(
    inner: &mut InnerState,
    key: &(String, String),
    holder: &concurrency::Holder,
) {
    let mut promote = None;
    if let Some(group) = inner.concurrency_groups.get_mut(key) {
        if group.running.as_ref() == Some(holder) {
            promote = group.running.take();
        } else {
            group.pending.retain(|pending| pending != holder);
        }
    }
    if let Some(done) = promote {
        promote_next_from_group(inner, key, done);
    }
    if inner
        .concurrency_groups
        .get(key)
        .is_some_and(|group| group.running.is_none() && group.pending.is_empty())
    {
        inner.concurrency_groups.remove(key);
    }

    let run_id = holder.run_id();
    let run_still_present = inner.concurrency_groups.get(key).is_some_and(|group| {
        group
            .running
            .as_ref()
            .is_some_and(|candidate| candidate.run_id() == run_id)
            || group
                .pending
                .iter()
                .any(|candidate| candidate.run_id() == run_id)
    });
    if !run_still_present {
        if let Some(keys) = inner.holder_keys.get_mut(&run_id) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                inner.holder_keys.remove(&run_id);
            }
        }
    }
}

fn release_jobset_admission(inner: &mut InnerState, id: &JobSetId) {
    let Some(admission) = inner.jobset_admissions.remove(id) else {
        return;
    };
    let holder = id.holder();
    for key in admission.acquired_keys {
        release_holder_key(inner, &key, &holder);
    }
}

fn advance_jobset_admission(
    inner: &mut InnerState,
    id: &JobSetId,
    promoted_key: Option<&(String, String)>,
) -> Result<JobSetAdmissionResult, String> {
    if let Some(key) = promoted_key {
        if let Some(admission) = inner.jobset_admissions.get_mut(id) {
            admission.acquired_keys.insert(key.clone());
        }
    }

    loop {
        let next_gate = {
            let Some(admission) = inner.jobset_admissions.get(id) else {
                return Ok(JobSetAdmissionResult::Ready);
            };
            admission
                .gates
                .iter()
                .find(|gate| !admission.acquired_keys.contains(&gate.key))
                .cloned()
        };
        let Some(gate) = next_gate else {
            inner.jobset_admissions.remove(id);
            return Ok(JobSetAdmissionResult::Ready);
        };

        let holder = id.holder();
        match try_acquire_concurrency(
            inner,
            gate.key.clone(),
            gate.display_name,
            holder,
            gate.cancel_in_progress,
            gate.queue,
        ) {
            Ok(true) => {
                if let Some(admission) = inner.jobset_admissions.get_mut(id) {
                    admission.acquired_keys.insert(gate.key);
                }
            }
            Ok(false) => return Ok(JobSetAdmissionResult::Blocked),
            Err(error) => {
                release_jobset_admission(inner, id);
                return Err(error);
            }
        }
    }
}

/// After a holder finishes, promote the next pending holder for the group.
fn promote_next_from_group(
    inner: &mut InnerState,
    key: &(String, String),
    _done: concurrency::Holder,
) {
    let next = {
        let Some(group) = inner.concurrency_groups.get_mut(key) else {
            return;
        };
        group.pending.pop_front()
    };

    let Some(next) = next else {
        if let Some(group) = inner.concurrency_groups.get(key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(key);
            }
        }
        return;
    };

    // Install as running immediately for Run and JobSet; for Holder::Job, defer
    // until max-parallel is confirmed free so the job cannot contend with its
    // own pending holder (C-01).
    if !matches!(&next, concurrency::Holder::Job { .. }) {
        if let Some(group) = inner.concurrency_groups.get_mut(key) {
            group.running = Some(next.clone());
        }
    }

    match next {
        concurrency::Holder::Run(run_id) => {
            if let Some(jobs) = inner.held_runs.remove(&run_id) {
                for mut job in jobs {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    // Re-check needs/max_parallel before queueing.
                    let needs_ok = inner.runs.get(&run_id).is_some_and(|run| {
                        job.needs
                            .iter()
                            .all(|n| scheduling::need_satisfied(&run.jobs, n))
                    });
                    if needs_ok && under_max_parallel(inner, &job) {
                        if let Some(run) = inner.runs.get(&run_id) {
                            hydrate_needs_context(&mut job, run);
                        }
                        inner.queue.push_back(job);
                    } else {
                        if let Some(run) = inner.runs.get_mut(&run_id) {
                            // keep Queued status in pending_jobs path
                            run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        }
                        inner.pending_jobs.push_back(job);
                    }
                }
                if let Some(run) = inner.runs.get_mut(&run_id) {
                    if run.status == ExecutionStatus::Pending {
                        run.status = ExecutionStatus::Queued;
                    }
                }
            }
        }
        concurrency::Holder::Job { run_id, job_id } => {
            let pos = inner
                .concurrency_blocked
                .iter()
                .position(|j| j.run_id == run_id && j.job_id == job_id);
            let Some(pos) = pos else { return };
            // Remove the job temporarily so we can call under_max_parallel
            // without a mutable/immutable borrow conflict on inner.
            let mut job = inner.concurrency_blocked.remove(pos).unwrap();
            if !under_max_parallel(inner, &job) {
                // max-parallel still full: restore the holder at the front of
                // the pending queue and put the job back where it was so the
                // next release event or promote_ready_jobs sweep can retry.
                inner.concurrency_blocked.insert(pos, job);
                if let Some(group) = inner.concurrency_groups.get_mut(key) {
                    group
                        .pending
                        .push_front(concurrency::Holder::Job { run_id, job_id });
                }
                return;
            }
            // Both gates clear: atomically install as running and dispatch.
            if let Some(group) = inner.concurrency_groups.get_mut(key) {
                group.running = Some(concurrency::Holder::Job { run_id, job_id });
            }
            if let Some(run) = inner.runs.get_mut(&run_id) {
                run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                hydrate_needs_context(&mut job, run);
            }
            inner.queue.push_back(job);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            let id = JobSetId {
                run_id,
                job_ids: job_ids.clone(),
            };
            match advance_jobset_admission(inner, &id, Some(key)) {
                Ok(JobSetAdmissionResult::Blocked) => return,
                Err(_) => {
                    cancel_holder(
                        inner,
                        &concurrency::Holder::JobSet { run_id, job_ids },
                        concurrency::cancelled_reason().as_deref(),
                    );
                    return;
                }
                Ok(JobSetAdmissionResult::Ready) => {}
            }

            let mut to_queue = Vec::new();
            inner.concurrency_blocked.retain(|job| {
                if job.run_id == run_id && job_ids.contains(&job.job_id) {
                    to_queue.push(job.clone());
                    false
                } else {
                    true
                }
            });
            for mut job in to_queue {
                if under_max_parallel(inner, &job) {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        hydrate_needs_context(&mut job, run);
                    }
                    inner.queue.push_back(job);
                } else {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    inner.pending_jobs.push_back(job);
                }
            }
        }
    }
}

/// Try to acquire a concurrency slot for a holder. Returns:
/// - `Ok(true)` if the holder may proceed (slot acquired / free)
/// - `Ok(false)` if parked as pending
/// - `Err("cancelled")` if the arrival itself was cancelled (queue max overflow)
/// - `Err(msg)` for evaluation / empty-group errors
fn try_acquire_concurrency(
    inner: &mut InnerState,
    key: (String, String),
    display_name: String,
    holder: concurrency::Holder,
    cancel_in_progress: bool,
    queue: aksh_gha_parser::ConcurrencyQueue,
) -> Result<bool, String> {
    let group = inner
        .concurrency_groups
        .entry(key.clone())
        .or_insert_with(|| concurrency::ConcurrencyGroup {
            display_name: display_name.clone(),
            running: None,
            pending: VecDeque::new(),
        });
    if group.display_name.is_empty() {
        group.display_name = display_name;
    }

    if group.running.is_none() {
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key);
        return Ok(true);
    }

    if cancel_in_progress {
        let prev = group.running.take();
        // Docs: "any existing pending job or workflow in the same concurrency
        // group will be canceled" — drain all pending holders too.
        let stale_pending: Vec<concurrency::Holder> = group.pending.drain(..).collect();
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key.clone());
        if let Some(prev) = prev {
            cancel_holder(inner, &prev, concurrency::cancelled_reason().as_deref());
        }
        for pending in stale_pending {
            cancel_holder(inner, &pending, concurrency::cancelled_reason().as_deref());
        }
        return Ok(true);
    }
    let _ = group;

    // Contended — apply queue mode for this arrival.
    let join = {
        let group = inner.concurrency_groups.get(&key).unwrap();
        concurrency::apply_queue_mode(queue, &group.pending)
    };

    for pending_holder in join.cancel_pending {
        if pending_holder.run_id() == holder.run_id() {
            continue;
        }
        cancel_holder(
            inner,
            &pending_holder,
            concurrency::cancelled_reason().as_deref(),
        );
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| h != &pending_holder);
        }
    }

    if join.cancel_arrival {
        return Err("concurrency_queue_overflow".to_owned());
    }

    if join.park_arrival {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            // After single-mode clears, re-push.
            group.pending.push_back(holder.clone());
        }
        track_holder_key(inner, &holder, key);
        return Ok(false);
    }

    Ok(true)
}

fn track_holder_key(inner: &mut InnerState, holder: &concurrency::Holder, key: (String, String)) {
    let run_id = holder.run_id();
    let keys = inner.holder_keys.entry(run_id).or_default();
    if !keys.contains(&key) {
        keys.push(key);
    }
}

fn cancel_holder(inner: &mut InnerState, holder: &concurrency::Holder, _reason: Option<&str>) {
    match holder {
        concurrency::Holder::Run(run_id) => {
            cancel_run_inner(inner, *run_id, Some("concurrency_cancelled"));
        }
        concurrency::Holder::Job { run_id, job_id } => {
            cancel_job_inner(inner, *run_id, job_id);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            inner.jobset_admissions.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            for job_id in job_ids {
                cancel_job_inner(inner, *run_id, job_id);
            }
            // If all jobs cancelled, mark run cancelled when appropriate.
            if let Some(run) = inner.runs.get_mut(run_id) {
                if run.jobs.values().all(|s| concurrency::is_terminal(*s)) {
                    run.status = summarize_run(run.jobs.values().copied());
                }
            }
        }
    }
}

async fn rerun_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunAccepted>, ApiError> {
    let submission = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .map(|run| run.submission.clone())
            .ok_or_else(|| ApiError::not_found("run not found"))?
    };
    submit_run(State(shared), Json(submission)).await
}

async fn run_events(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    let inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let mut out = event_to_ndjson(&NdjsonEvent::RunStatus {
        run_id,
        status: run.status,
        reason: None,
    })?;
    for (job_id, status) in &run.jobs {
        out.push_str(&event_to_ndjson(&NdjsonEvent::JobStatus {
            run_id,
            job_id: job_id.clone(),
            status: *status,
            reason: None,
        })?);
    }
    if let Some(events) = inner.timeline_events.get(&run_id) {
        for event in events {
            out.push_str(&event_to_ndjson(event)?);
        }
    }
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from(out))
        .expect("static response builder"))
}

async fn live_logs_sse(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, job_id)): Path<(RunId, String)>,
) -> Result<Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    // Grab per-job handles under the global lock, then drop it immediately.
    let (job_lines, rx) = {
        let mut inner = shared.state.inner.lock().await;
        let key = live_log_key_for_job(&inner, run_id, &job_id)
            .ok_or_else(|| ApiError::not_found("job not found"))?;
        let lines_arc = inner.live_log_lines.entry(key.clone()).or_default().clone();
        let tx = live_log_sender(&mut inner, &key);
        (lines_arc, tx.subscribe())
    };
    // Snapshot under per-job lock only — does not block global state.
    let snapshot = job_lines.lock().await.clone();

    let snapshot_stream = stream::iter(
        snapshot
            .into_iter()
            .map(|wrapper| Ok(live_log_sse_event(&wrapper))),
    );
    let live_stream = stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(wrapper) => {
                    let event = live_log_sse_event(&wrapper);
                    return Some((Ok(event), rx));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(snapshot_stream.chain(live_stream)).keep_alive(KeepAlive::default()))
}

fn live_log_sse_event(wrapper: &LiveLogFeedLinesWrapper) -> Event {
    let data = serde_json::to_string(wrapper).unwrap_or_else(|_| "{}".to_string());
    Event::default().event("live-log").data(data)
}

fn live_log_key_for_job(inner: &InnerState, run_id: RunId, job_id: &str) -> Option<String> {
    inner.runs.get(&run_id)?;
    inner
        .job_requests
        .values()
        .find(|record| {
            record.run_id == run_id
                && (record.job_id.0 == job_id || record.agent_job_id.to_string() == job_id)
        })
        .map(|record| record.agent_job_id.to_string())
        .or_else(|| Some(job_id.to_string()).filter(|key| inner.live_log_lines.contains_key(key)))
}

fn live_log_sender(
    inner: &mut InnerState,
    key: &str,
) -> broadcast::Sender<LiveLogFeedLinesWrapper> {
    inner
        .live_log_tx
        .entry(key.to_string())
        .or_insert_with(|| {
            let (tx, _) = broadcast::channel(1024);
            tx
        })
        .clone()
}

async fn ws_live_logs(
    State(shared): State<Arc<SharedState>>,
    Path(job_id): Path<String>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_live_log_socket(socket, job_id, shared))
}

async fn handle_live_log_socket(mut socket: WebSocket, job_id: String, shared: Arc<SharedState>) {
    const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
    loop {
        let message = match tokio::time::timeout(IDLE_TIMEOUT, socket.next()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break, // stream ended
            Err(_) => {
                debug!(%job_id, "live log websocket idle for 5m, closing");
                break;
            }
        };
        match message {
            Ok(WsMessage::Text(text)) => {
                match serde_json::from_str::<LiveLogFeedLinesWrapper>(&text) {
                    Ok(mut wrapper) => {
                        wrapper.count = wrapper.value.len();
                        record_live_log_wrapper(&shared, &job_id, wrapper).await;
                    }
                    Err(error) => warn!(%error, %job_id, "invalid live log websocket payload"),
                }
            }
            Ok(WsMessage::Ping(data)) => {
                if socket.send(WsMessage::Pong(data)).await.is_err() {
                    break;
                }
            }
            Ok(WsMessage::Binary(_)) | Ok(WsMessage::Pong(_)) => {}
            Ok(WsMessage::Close(_)) => break,
            Err(error) => {
                warn!(%error, %job_id, "live log websocket receive failed");
                break;
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct RegisterDapPortRequest {
    port: u16,
    job_id: JobId,
}

async fn register_dap_port(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Json(payload): Json<RegisterDapPortRequest>,
) -> Result<StatusCode, ApiError> {
    if payload.port < 1024 {
        return Err(ApiError::bad_request(
            "DAP port must be an unprivileged local port",
        ));
    }
    let mut inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let status = run
        .jobs
        .get(&payload.job_id)
        .copied()
        .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
    if !matches!(status, ExecutionStatus::InProgress) {
        return Err(ApiError::bad_request(
            "DAP port can only be registered for an in-progress job",
        ));
    }
    inner.dap_ports.insert(
        run_id,
        DapPortRegistration {
            port: payload.port,
            job_id: payload.job_id.clone(),
        },
    );
    info!(%run_id, job_id = %payload.job_id, port = payload.port, "Registered DAP port");
    Ok(StatusCode::OK)
}

async fn ws_dap_debug(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    ws: WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    ws.on_upgrade(move |socket| handle_dap_debug_socket(socket, run_id, shared))
}

async fn handle_dap_debug_socket(socket: WebSocket, run_id: RunId, shared: Arc<SharedState>) {
    let registration = {
        let inner = shared.state.inner.lock().await;
        inner.dap_ports.get(&run_id).cloned()
    };
    let (port, job_id_str) = match registration {
        Some(reg) => (reg.port, reg.job_id.to_string()),
        None => {
            info!(%run_id, "No DAP port registered; falling back to default port 4711");
            (4711, "official".to_string())
        }
    };

    info!(%run_id, job_id = %job_id_str, port, "Starting DAP websocket proxy to runner");
    if let Err(e) = pump_axum_ws_to_dap(socket, port).await {
        warn!(%run_id, job_id = %job_id_str, port, "DAP websocket proxy ended with error: {e}");
    }
}

async fn pump_axum_ws_to_dap(ws: WebSocket, target_port: u16) -> Result<(), anyhow::Error> {
    use futures::{SinkExt, StreamExt};

    let url = format!("ws://127.0.0.1:{target_port}");
    let mut target_ws = None;
    for _ in 0..50 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((stream, _)) => {
                target_ws = Some(stream);
                break;
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    let target_ws = target_ws
        .ok_or_else(|| anyhow::anyhow!("failed to connect to runner DAP bridge after retries"))?;

    let (mut target_sink, mut target_stream) = target_ws.split();
    let (mut ws_sink, mut ws_stream) = ws.split();

    let to_target = async {
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(WsMessage::Text(text)) => {
                    target_sink
                        .send(tokio_tungstenite::tungstenite::Message::Text(text))
                        .await
                        .map_err(|e| anyhow::anyhow!("target ws send: {e}"))?;
                }
                Ok(WsMessage::Binary(_)) => {
                    return Err(anyhow::anyhow!(
                        "binary WebSocket frames are not allowed on the DAP bridge"
                    ));
                }
                Ok(WsMessage::Close(_)) | Err(_) => break,
                Ok(WsMessage::Ping(_)) | Ok(WsMessage::Pong(_)) => continue,
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    let from_target = async {
        while let Some(msg) = target_stream.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    ws_sink
                        .send(WsMessage::Text(text))
                        .await
                        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    tokio::select! {
        a = to_target => a?,
        b = from_target => b?,
    }
    Ok(())
}

async fn record_live_log_wrapper(
    shared: &Arc<SharedState>,
    job_id: &str,
    wrapper: LiveLogFeedLinesWrapper,
) {
    // Grab per-job Arc and broadcast sender under the global lock, then release it.
    let (job_lines, tx) = {
        let mut inner = shared.state.inner.lock().await;
        let lines_arc = inner
            .live_log_lines
            .entry(job_id.to_string())
            .or_default()
            .clone();
        let tx = live_log_sender(&mut inner, job_id);
        (lines_arc, tx)
    };
    // Push and broadcast under per-job lock only.
    job_lines.lock().await.push(wrapper.clone());
    let _ = tx.send(wrapper);
}

async fn register_runner(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Result<Json<RegisteredRunner>, ApiError> {
    let parsed_public_key = request
        .public_key
        .as_deref()
        .map(AgentRsaPublicKey::parse)
        .transpose()
        .map_err(ApiError::from)?;
    let mut inner = shared.state.inner.lock().await;
    inner.next_runner_id += 1;
    let runner_id = inner.next_runner_id;
    let public_key = request.public_key.clone();
    let runner = RegisteredRunner {
        id: runner_id,
        name: request.name,
        labels: request.labels,
        ephemeral: request.ephemeral,
        public_key,
    };
    if let Some(public_key) = &runner.public_key {
        inner
            .runner_public_keys
            .insert(runner_id, public_key.clone());
    }
    if let Some(public_key) = parsed_public_key {
        inner.runner_rsa_public_keys.insert(runner_id, public_key);
    }
    inner.runners.insert(runner.id, runner.clone());
    Ok(Json(runner))
}

async fn create_session(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerSessionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let session_id = uuid::Uuid::new_v4();

    // Generate AES session key
    let session_enc = SessionEncryption::generate();

    let runner_public_key = {
        let inner = shared.state.inner.lock().await;
        inner
            .runner_rsa_public_keys
            .get(&request.runner_id)
            .cloned()
    };
    let (key_bytes, encrypted) = if let Some(public_key) = runner_public_key {
        (public_key.wrap_key(&session_enc.key)?, true)
    } else {
        (session_enc.key.clone(), false)
    };
    let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, key_bytes);

    // Store the session key for later message decryption
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.to_string(), session_enc);
    }

    info!(%session_id, runner_id = request.runner_id, encrypted, "session created with AES key");

    Ok(Json(json!({
        "sessionId": session_id.to_string(),
        "encryptionKey": {
            "value": key_b64,
            "encrypted": encrypted
        }
    })))
}

async fn create_session_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    // For the AzDO message path, generate an unencrypted session key directly.
    // RSA-wrapped keys are only needed for real internet-facing GHES; for local
    // use the runner's from_rsaparams may not reconstruct the keypair correctly.
    let session_id = uuid::Uuid::new_v4();
    let session_enc = SessionEncryption::generate();
    let key_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        session_enc.key.clone(),
    );

    let runner_id = body
        .pointer("/agent/id")
        .and_then(serde_json::Value::as_i64);

    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.to_string(), session_enc);
        if let Some(runner_id) = runner_id {
            inner
                .broker_session_runners
                .insert(session_id.to_string(), runner_id);
        }
        // Only mark as AzDO if the client explicitly opts in.
        // This preserves backward compat: test and broker-hybrid sessions do NOT
        // include `akshAzdo: true` and continue to receive broker-ref messages.
        if body
            .get("akshAzdo")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            inner.azdo_sessions.insert(session_id.to_string());
        }
    }

    let owner_name = body
        .get("ownerName")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    info!(%session_id, "AzDO session created (unencrypted key)");

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id.to_string(),
            "ownerName": owner_name,
            "assignmentQueued": false,
            "orchestrationId": "",
            "encryptionKey": {
                "value": key_b64,
                "encrypted": false,
            },
        })),
    ))
}

async fn delete_session(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    inner.sessions.remove(&session_id);
    inner.broker_session_runners.remove(&session_id);
    StatusCode::NO_CONTENT
}

/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/agents/:agent_id
/// Idempotent agent deregistration — the runner calls this on clean exit.
/// aksh keeps no persistent agent registry so always succeeds.
/// Returns null response body in JSON to match official.
async fn delete_agent(
    Path((_pool_id, _agent_id)): Path<(i64, i64)>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null))
}

/// DELETE /runner/server/_apis/distributedtask/pools/:pool_id/sessions (no session_id)
/// Broker-side session teardown: the runner deletes the session-less path on the broker host.
/// Return 204 unconditionally; the concrete session was already cleaned up individually.
/// Returns null response body in JSON to match official.
async fn delete_sessions_for_pool(
    Path(_pool_id): Path<i64>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NO_CONTENT, Json(serde_json::Value::Null))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerAcquireJobRequest {
    job_message_id: uuid::Uuid,
    #[allow(dead_code)]
    billing_owner_id: Option<String>,
    #[allow(dead_code)]
    runner_os: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerRenewJobRequest {
    job_id: uuid::Uuid,
    #[serde(rename = "planId")]
    _plan_id: String,
    conclusion: Option<String>,
    #[serde(default)]
    outputs: BTreeMap<String, serde_json::Value>,
}

fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
    match result.to_ascii_lowercase().as_str() {
        "success" | "succeeded" | "succeededwithissues" => Some(ExecutionStatus::Success),
        "failure" | "failed" => Some(ExecutionStatus::Failure),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
        "skipped" => Some(ExecutionStatus::Skipped),
        _ => None,
    }
}

fn broker_run_service_url(runner_id: i64) -> String {
    format!("{}/broker/{runner_id}/", public_base_url())
}

fn public_base_url() -> String {
    std::env::var("AKSH_PUBLIC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
        .trim_end_matches('/')
        .to_owned()
}

fn format_reusable_workflow_ref(repository: &str, workflow_ref: &str, caller_ref: &str) -> String {
    if let Some(path) = workflow_ref.strip_prefix("./") {
        let (path, git_ref) = path.split_once('@').unwrap_or((path, caller_ref));
        return format!("{repository}/{path}@{git_ref}");
    }
    workflow_ref.to_owned()
}

fn normalize_oidc_issuer(value: String) -> anyhow::Result<String> {
    let issuer = value.trim_end_matches('/').to_owned();
    if issuer.is_empty()
        || !(issuer.starts_with("https://") || issuer.starts_with("http://"))
        || issuer.contains('?')
        || issuer.contains('#')
    {
        anyhow::bail!("OIDC issuer must be an absolute HTTP(S) URL without query or fragment");
    }
    Ok(issuer)
}

/// Return the effective OIDC issuer URL, falling back to
/// `{public_base_url}/oidc` when not explicitly configured.
fn oidc_issuer_url(inner: &InnerState) -> String {
    if inner.oidc_issuer.is_empty() {
        format!("{}/oidc", public_base_url())
    } else {
        inner.oidc_issuer.clone()
    }
}

fn websocket_base_url() -> String {
    let base = public_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

fn runner_server_url() -> String {
    format!("{}/runner/server", public_base_url())
}

fn broker_job_ref(request: &TaskAgentJobRequestRecord, runner_id: i64) -> serde_json::Value {
    json!({
        "messageId": request.agent_job_id.to_string(),
        "messageType": "RunnerJobRequest",
        "body": serde_json::to_string(&json!({
            "runner_request_id": request.agent_job_id.to_string(),
            "run_service_url": broker_run_service_url(runner_id),
            "billing_owner_id": "local",
            "should_acknowledge": true
        })).unwrap()
    })
}

fn broker_job_ref_root(request: &TaskAgentJobRequestRecord, runner_id: i64) -> serde_json::Value {
    // messageId must be unique across job + cancel messages on a session.
    // Using request_id alone collides with cancel messages that also allocate
    // from the same integer space (runner in-memory dedup then drops the job).
    json!({
        "messageId": request.request_id,
        "messageType": "RunnerJobRequest",
        "body": serde_json::to_string(&json!({
            "runner_request_id": request.agent_job_id.to_string(),
            "run_service_url": broker_run_service_url(runner_id),
            "billing_owner_id": "local",
            "should_acknowledge": true
        })).unwrap()
    })
}

/// Allocate a session-unique broker message id that cannot collide with
/// `request_id` values used as RunnerJobRequest messageIds.
fn next_broker_message_id(inner: &mut InnerState) -> i64 {
    // request_ids start at 1 and increase; keep message ids in a separate high
    // range so cancels never reuse a past/future request_id.
    const MESSAGE_ID_BASE: i64 = 1_000_000;
    if inner.next_message_id < MESSAGE_ID_BASE {
        inner.next_message_id = MESSAGE_ID_BASE;
    }
    inner.next_message_id += 1;
    inner.next_message_id
}
async fn next_message_broker_ref(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());

    let wait_seconds = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);

    loop {
        let mut inner = shared.state.inner.lock().await;
        if let Some(message) = inner
            .inflight_messages
            .get(&session_id)
            .and_then(|messages| messages.values().next().cloned())
        {
            return Ok(Json(message).into_response());
        }

        if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
            if let Some(request) = inner.job_requests.get(&request_id) {
                if let Some(pos) = inner
                    .cancellation_queue
                    .iter()
                    .position(|c| c.run_id == request.run_id && c.job_id == request.job_id)
                {
                    let cancellation = inner.cancellation_queue.remove(pos).unwrap();
                    let message = build_broker_plaintext_message(
                        &mut inner,
                        &session_id,
                        azdo::message_type::JOB_CANCELLED,
                        concurrency::job_cancel_body(cancellation.agent_job_id),
                    );
                    return Ok(Json(message).into_response());
                }

                if request.result.is_none() {
                    return Ok(Json(broker_job_ref(request, pool_id)).into_response());
                }
            }
            inner.session_active_requests.remove(&session_id);
        }

        let runner_labels = inner.runner_labels_for_session(&session_id);
        let Some(queued) = take_matching_job(&mut inner.queue, &runner_labels) else {
            drop(inner);
            if wait_seconds == 0 {
                return Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response());
            }
            if tokio::time::timeout(
                Duration::from_secs(wait_seconds),
                shared.state.message_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response());
            }
            continue;
        };

        if let Some(run) = inner.runs.get_mut(&queued.run_id) {
            run.status = ExecutionStatus::InProgress;
            run.jobs
                .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        let request_id = queued.message.request_id;
        inner
            .session_active_requests
            .insert(session_id.clone(), request_id);
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.started_at = Some(std::time::SystemTime::now());
            request.last_renewed_at = Some(std::time::SystemTime::now());
        }
        inner
            .broker_messages
            .insert(request_id, queued.message.clone());
        let request = inner
            .job_requests
            .get(&request_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("agent request not found"))?;

        let run_id = queued.run_id;
        let job_id = queued.job_id.clone();
        drop(inner);

        github::report_check_run_in_progress(&shared, run_id, &job_id).await;

        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;

        return Ok(Json(broker_job_ref(&request, pool_id)).into_response());
    }
}

/// GET `/_apis/distributedtask/pools/:pool_id/messages` dispatcher.
///
/// Sessions created via the AzDO path (`create_session_disttask`) are marked
/// in `azdo_sessions` and receive the full encrypted `PipelineAgentJobRequest`
/// message via `next_message_compat`.  All other sessions (broker-hybrid tests,
/// legacy broker flow) get the lightweight `RunnerJobRequest` broker ref.
async fn next_message_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());
    let is_azdo = {
        let inner = shared.state.inner.lock().await;
        inner.azdo_sessions.contains(&session_id)
    };
    if is_azdo {
        next_message_compat(State(shared), Path(pool_id), Query(params))
            .await
            .map(|r| r.into_response())
    } else {
        next_message_broker_ref(State(shared), Path(pool_id), Query(params)).await
    }
}

async fn broker_session_root(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    let session_id = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.clone(), SessionEncryption::generate());
        inner
            .broker_session_runners
            .insert(session_id.clone(), runner_id);
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id,
            "ownerName": "aksh-runner",
            "assignmentQueued": false,
            "orchestrationId": ""
        })),
    ))
}

async fn broker_delete_session_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    if let Some(session_id) = params.get("sessionId") {
        remove_broker_session(&shared, session_id, runner_id).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn broker_delete_session_by_path(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    remove_broker_session(&shared, &session_id, runner_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_broker_session(
    shared: &Arc<SharedState>,
    session_id: &str,
    runner_id: i64,
) -> Result<(), ApiError> {
    let mut inner = shared.state.inner.lock().await;
    match inner.broker_session_runners.get(session_id).copied() {
        Some(owner) if owner == runner_id => {
            inner.broker_session_runners.remove(session_id);
            inner.session_keys.remove(session_id);
            inner.session_active_requests.remove(session_id);
            Ok(())
        }
        Some(_) => Err(ApiError::forbidden(
            "broker session belongs to another runner",
        )),
        None => Err(ApiError::not_found("broker session not found")),
    }
}
fn authenticated_runner_id(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: Option<i64>,
) -> Result<i64, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    let runner_id = shared
        .state
        .runner_id_from_token(bearer)
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    if expected_runner_id.is_some_and(|expected| expected != runner_id) {
        return Err(ApiError::forbidden(
            "runner token does not match broker path",
        ));
    }
    Ok(runner_id)
}

fn ensure_broker_request_owner(
    inner: &InnerState,
    request_id: i64,
    runner_id: i64,
) -> Result<(), ApiError> {
    let owner = inner
        .session_active_requests
        .iter()
        .find_map(|(session_id, active_request_id)| {
            (*active_request_id == request_id).then_some(session_id)
        })
        .and_then(|session_id| inner.broker_session_runners.get(session_id).copied());
    match owner {
        Some(owner) if owner == runner_id => Ok(()),
        Some(_) => Err(ApiError::forbidden(
            "broker request belongs to another runner",
        )),
        None => Err(ApiError::not_found(
            "broker request is not assigned to a session",
        )),
    }
}

async fn next_message_broker_ref_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    let session_id = params
        .get("sessionId")
        .cloned()
        .ok_or_else(|| ApiError::bad_request("broker sessionId is required"))?;
    {
        let inner = shared.state.inner.lock().await;
        if inner.broker_session_runners.get(&session_id) != Some(&runner_id) {
            return Err(ApiError::forbidden(
                "broker session belongs to another runner",
            ));
        }
    }

    // Default to 50s long-poll (golden flows show ~50s waits between jobs)
    let wait = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);
    // The runner may report completion before its worker process has fully
    // exited. GitHub keeps polling with status=Busy during that drain window;
    // never dispatch a successor until the runner reports Online again.
    let runner_busy = params
        .get("status")
        .is_some_and(|status| status.eq_ignore_ascii_case("busy"));

    let deadline = std::time::Instant::now() + Duration::from_secs(wait);

    loop {
        let maybe = {
            let mut inner = shared.state.inner.lock().await;
            // Prefer delivering JobCancellation for the active request (official
            // cancel path). Without this, concurrency cancel-in-progress never
            // reaches broker-path runners.
            if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
                if let Some(request) = inner.job_requests.get(&request_id).cloned() {
                    if let Some(pos) = inner
                        .cancellation_queue
                        .iter()
                        .position(|c| c.run_id == request.run_id && c.job_id == request.job_id)
                    {
                        let cancellation = inner.cancellation_queue.remove(pos).unwrap();
                        let message_id = next_broker_message_id(&mut inner);
                        Some(json!({
                            "messageId": message_id,
                            "messageType": azdo::message_type::JOB_CANCELLED,
                            "body": concurrency::job_cancel_body(cancellation.agent_job_id),
                        }))
                    } else if request.result.is_none() {
                        // Still running — long-poll for cancel rather than
                        // redelivering the same RunnerJobRequest (runner dedups it).
                        None
                    } else {
                        inner.session_active_requests.remove(&session_id);
                        None
                    }
                } else {
                    inner.session_active_requests.remove(&session_id);
                    None
                }
            } else if runner_busy {
                None
            } else {
                let labels = inner.runner_labels_for_session(&session_id);
                if let Some(queued) = take_matching_job(&mut inner.queue, &labels) {
                    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
                        run.status = ExecutionStatus::InProgress;
                        run.jobs
                            .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
                    }
                    let request_id = queued.message.request_id;
                    if let Some(request) = inner.job_requests.get_mut(&request_id) {
                        request.started_at = Some(std::time::SystemTime::now());
                        request.last_renewed_at = Some(std::time::SystemTime::now());
                    }
                    // Job messageId = request_id (low range). Cancels use 1_000_000+.
                    inner
                        .session_active_requests
                        .insert(session_id.clone(), request_id);
                    inner
                        .broker_messages
                        .insert(request_id, queued.message.clone());
                    let request = inner
                        .job_requests
                        .get(&request_id)
                        .expect("queued request must exist");
                    Some(broker_job_ref_root(request, 1))
                } else {
                    None
                }
            }
        };

        if let Some(message) = maybe {
            return Ok(Json(message));
        }
        if wait == 0 || std::time::Instant::now() >= deadline {
            return Ok(Json(serde_json::Value::Null));
        }
        // Wake promptly on cancel/enqueue rather than fixed 250ms sleep.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let slice = remaining.min(Duration::from_secs(3));
        let _ = tokio::time::timeout(slice, shared.state.message_notify.notified()).await;
    }
}

async fn broker_acknowledge_root(
    State(_shared): State<Arc<SharedState>>,
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    // Acknowledge receipt of the message. Do NOT clear session_active_requests
    // here — the runner is still working on the job. The session's active
    // request is cleared when completejob sets the result and the next poll
    // sees result.is_some() at line 2190.
    StatusCode::OK
}

async fn broker_acquire_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerAcquireJobRequest>,
) -> Result<Json<azdo::AgentJobRequestMessage>, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_message_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("broker job message not found"))?;
    ensure_broker_request_owner(&inner, request_id, runner_id)?;
    let mut message = inner
        .broker_messages
        .get(&request_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("broker job payload not found"))?;
    message.message_type = Some(azdo::message_type::RUNNER_JOB_REQUEST.to_owned());
    let run_service_url = broker_run_service_url(runner_id);
    for endpoint in &mut message.resources.endpoints {
        if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
            endpoint.url = Some(run_service_url.clone());
            endpoint.authorization.parameters.insert(
                "AccessToken".to_owned(),
                shared
                    .state
                    .mint_runtime_token(&message.plan.plan_id, &message.job_id),
            );
            endpoint
                .data
                .insert("ResultsServiceUrl".to_owned(), public_base_url());
            endpoint
                .data
                .insert("PipelinesServiceUrl".to_owned(), runner_server_url());
            endpoint
                .data
                .insert("CacheServerUrl".to_owned(), public_base_url());
            endpoint.data.insert(
                "FeedStreamUrl".to_owned(),
                format!("{}/ws/live-logs/{}", websocket_base_url(), message.job_id),
            );
        }
    }
    message.billing_owner_id = request.billing_owner_id;
    // Run-service payloads use the DTO default; internal request IDs remain in
    // `job_requests` and broker lookup maps for renew/complete bookkeeping.
    message.request_id = 0;
    Ok(Json(message))
}

async fn broker_renew_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let mut inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("broker renew request not found"))?;
    ensure_broker_request_owner(&inner, request_id, runner_id)?;
    let record = inner
        .job_requests
        .get_mut(&request_id)
        .ok_or_else(|| ApiError::not_found("agent request not found"))?;
    record.locked_until = agent_request_locked_until();
    record.last_renewed_at = Some(std::time::SystemTime::now());
    Ok(Json(json!({"lockedUntil": record.locked_until})))
}

async fn broker_complete_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<StatusCode, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let status = match request.conclusion.as_deref() {
        Some(conclusion) => execution_status_from_runner_result(conclusion).ok_or_else(|| {
            ApiError::bad_request(format!("unknown broker conclusion `{conclusion}`"))
        })?,
        // Older broker clients omit this field on successful completion.
        None => ExecutionStatus::Success,
    };

    // Extract outputs from the completejob body.
    // Runner sends: { "outputName": {"value": "theValue"} }
    // Server stores: { "outputName": "theValue" }
    let mut outputs = aksh_gha_protocol::OutputMap::new();
    for (key, val) in &request.outputs {
        if let Some(v) = val.get("value").and_then(|v| v.as_str()) {
            outputs.insert(key.clone(), serde_json::Value::String(v.to_owned()));
        } else if let Some(v) = val.get("value") {
            outputs.insert(key.clone(), v.clone());
        } else if let Some(s) = val.as_str() {
            outputs.insert(key.clone(), serde_json::Value::String(s.to_owned()));
        } else {
            outputs.insert(key.clone(), val.clone());
        }
    }

    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let request_id = inner
            .agent_job_requests
            .get(&request.job_id)
            .copied()
            .ok_or_else(|| ApiError::not_found("broker complete request not found"))?;
        ensure_broker_request_owner(&inner, request_id, runner_id)?;
        debug!(request_id, job_id = %request.job_id, "broker complete: found request");
        if let Some(record) = inner.job_requests.get_mut(&request_id) {
            record.result = Some(status);
            record.locked_until = agent_request_locked_until();
        }
        // Free the session so the next broker poll can take a new job immediately
        // (otherwise the poll arm waits until it observes result.is_some()).
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != request_id);
        let run_job = inner.inflight_requests.remove(&request_id).or_else(|| {
            job_request_tuple(&inner, request_id).map(|(_, run_id, job_id)| (run_id, job_id))
        });
        match run_job {
            Some((run_id, job_id)) => {
                info!(%run_id, %job_id, "broker complete: completing job");
                Some(JobCompletion {
                    run_id,
                    job_id,
                    status,
                    outputs,
                })
            }
            None => {
                warn!(
                    request_id,
                    "broker complete: no inflight_requests entry found"
                );
                None
            }
        }
    };
    if let Some(completion) = completion {
        let _ = complete_job_inner(shared.clone(), completion).await?;
    }
    // Wake long-polling runners so a queued successor job is delivered promptly
    // after cancel/complete (concurrency release path).
    shared.state.message_notify.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct JobLogsSignedBlobUrlRequest {
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

#[derive(Debug, Deserialize)]
struct StepLogsSignedBlobUrlRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

async fn twirp_workflow_steps_update(
    State(shared): State<Arc<SharedState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut inner = shared.state.inner.lock().await;

    let plan_id = payload["workflow_run_backend_id"].as_str().unwrap_or("");
    let agent_job_id_str = payload["workflow_job_run_backend_id"]
        .as_str()
        .unwrap_or("");

    if let (Some(plan_uuid), Some(job_uuid)) = (
        uuid::Uuid::parse_str(plan_id).ok(),
        uuid::Uuid::parse_str(agent_job_id_str).ok(),
    ) {
        if let Some((_, run_id, job_id)) =
            resolve_callback_job(&inner, &plan_uuid.to_string(), None, Some(job_uuid))
        {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }
                if let Some(steps) = payload["steps"].as_array() {
                    for step in steps {
                        let name = step["name"].as_str().unwrap_or("").to_owned();
                        let conclusion_num = step["conclusion"].as_u64().unwrap_or(0);
                        let status_num = step["status"].as_u64().unwrap_or(0);

                        let job_status = run.jobs.get(&job_id).copied();
                        let conclusion_str = if status_num == 6 {
                            match conclusion_num {
                                2 => "success",
                                3 => {
                                    if job_status == Some(ExecutionStatus::Cancelled) {
                                        "cancelled"
                                    } else {
                                        "failure"
                                    }
                                }
                                7 => "skipped",
                                _ => "success",
                            }
                        } else {
                            "in_progress"
                        };

                        if let Some(pos) = job_detail.steps.iter().position(|s| s.name == name) {
                            job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                        } else {
                            job_detail.steps.push(StepRecord {
                                name,
                                conclusion: conclusion_str.to_owned(),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(Json(json!({"ok": true})))
}

async fn twirp_get_job_logs_signed_blob_url(
    Json(request): Json<JobLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/job-logs.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id
        )
    }))
}

async fn twirp_get_step_logs_signed_blob_url(
    Json(request): Json<StepLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/step-{}.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
struct StepSummarySignedBlobUrlRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

async fn twirp_get_step_summary_signed_blob_url(
    Json(request): Json<StepSummarySignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "summary_url": format!(
            "{}/replay/results/{}/{}/step-{}-summary.md",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StepSummaryMetadataRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
    size: Option<u64>,
    uploaded_at: Option<String>,
}

async fn twirp_create_step_summary_metadata(
    Json(_request): Json<StepSummaryMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StepLogsMetadataRequest {
    step_backend_id: Option<String>,
    workflow_job_run_backend_id: Option<String>,
    workflow_run_backend_id: Option<String>,
    upload_url: Option<String>,
    line_count: Option<u64>,
}

/// POST CreateStepLogsMetadata — runner calls this after uploading step logs.
async fn twirp_create_step_logs_metadata(
    Json(_request): Json<StepLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JobLogsMetadataRequest {
    workflow_job_run_backend_id: Option<String>,
    workflow_run_backend_id: Option<String>,
    upload_url: Option<String>,
    line_count: Option<u64>,
}

/// POST CreateJobLogsMetadata — runner calls this after uploading job logs.
async fn twirp_create_job_logs_metadata(
    Json(_request): Json<JobLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

// ─── Cache v2 Twirp (github.actions.results.api.v1.CacheService) ─────────────

fn scoped_cache_key(key: &str, scope: Option<&str>, repository: Option<&str>) -> String {
    format!(
        "{}:{}\0{key}",
        repository.unwrap_or("default"),
        scope.unwrap_or("default")
    )
}

#[derive(Debug, Deserialize)]
struct CacheV2CreateRequest {
    key: String,
    version: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CacheV2FinalizeRequest {
    key: String,
    version: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CacheV2GetDlUrlRequest {
    key: String,
    version: String,
    #[serde(default)]
    restore_keys: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

async fn twirp_cache_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    if shared
        .state
        .cache
        .get(&storage_key, &request.version, &[])
        .await
        .map_err(|error| ApiError::internal(format!("cache lookup error: {error}")))?
        .is_some()
    {
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache already exists"
        })));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create cache stage dir: {e}")))?;
    let already_reserved = {
        let mut inner = shared.state.inner.lock().await;
        if inner
            .cache_v2_pending
            .values()
            .any(|pending| pending.key == storage_key && pending.version == request.version)
        {
            true
        } else {
            inner.cache_v2_pending.insert(
                token.clone(),
                CacheV2Pending {
                    key: storage_key,
                    version: request.version,
                },
            );
            false
        }
    };
    if already_reserved {
        let _ = tokio::fs::remove_dir_all(&stage_dir).await;
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache upload already reserved"
        })));
    }
    let upload_url = format!("{}/twirp-blob/cache/{token}", public_base_url());
    info!(token, "cache v2 create entry");
    Ok(Json(
        json!({ "ok": true, "signed_upload_url": upload_url, "message": "" }),
    ))
}

async fn twirp_cache_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    // Find the pending upload token matching key+version.
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .cache_v2_pending
            .iter()
            .find(|(_, p)| p.key == storage_key && p.version == request.version)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending cache upload for key+version"))?;

    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token)
        .join("data");
    let bytes = tokio::fs::read(&blob_path).await.map_err(|e| {
        ApiError::not_found(format!("cache blob not found (not yet uploaded?): {e}"))
    })?;

    let (key, version) = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .cache_v2_pending
            .get(&token)
            .ok_or_else(|| ApiError::internal("pending entry vanished"))?;
        (pending.key.clone(), pending.version.clone())
    };

    shared
        .state
        .cache
        .put(&key, &version, &bytes)
        .await
        .map_err(|e| ApiError::internal(format!("cache store error: {e}")))?;

    {
        let mut inner = shared.state.inner.lock().await;
        inner.cache_v2_pending.remove(&token);
    }

    // Clean up staging directory.
    let _ = tokio::fs::remove_dir_all(
        shared
            .state
            .state_dir
            .join("blobs")
            .join("cache")
            .join(&token),
    )
    .await;

    info!(key, version, size = bytes.len(), "cache v2 finalized");
    Ok(Json(json!({ "ok": true, "entry_id": "1", "message": "" })))
}

async fn twirp_cache_v2_get_dl_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2GetDlUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    let storage_restore_keys = request
        .restore_keys
        .iter()
        .map(|key| scoped_cache_key(key, request.scope.as_deref(), request.repository.as_deref()))
        .collect::<Vec<_>>();
    let result = shared
        .state
        .cache
        .get(&storage_key, &request.version, &storage_restore_keys)
        .await
        .map_err(|e| ApiError::internal(format!("cache lookup error: {e}")))?;

    let (entry, _bytes) = match result {
        Some(r) => r,
        None => {
            return Ok(Json(
                json!({ "ok": false, "signed_download_url": "", "matched_key": "" }),
            ))
        }
    };

    let dl_token = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .cache_v2_dl_tokens
            .insert(dl_token.clone(), (entry.key.clone(), entry.version.clone()));
    }
    let download_url = format!("{}/twirp-blob/cache/{dl_token}", public_base_url());
    let matched_key = entry
        .key
        .split_once('\0')
        .map(|(_, key)| key.to_owned())
        .unwrap_or_else(|| entry.key.clone());
    info!(key = %matched_key, "cache v2 download URL issued");
    Ok(Json(json!({
        "ok": true,
        "signed_download_url": download_url,
        "matched_key": matched_key
    })))
}

// ─── Artifact v2 Twirp (github.actions.results.api.v1.ArtifactService) ────────

#[derive(Debug, Deserialize)]
struct ArtifactV2CreateRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactV2FinalizeRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    #[serde(default)]
    size: serde_json::Value, // proto3 JSON: int64 as string
    #[serde(default)]
    hash: Option<serde_json::Value>, // StringValue: plain string or wrapped object
}

#[derive(Debug, Deserialize)]
struct ArtifactV2ListRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    #[serde(default)]
    name_filter: Option<serde_json::Value>, // StringValue: plain string in proto3 JSON
    #[serde(default)]
    id_filter: Option<serde_json::Value>, // Int64Value: string in proto3 JSON
}

#[derive(Debug, Deserialize)]
struct ArtifactV2GetSignedUrlRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactV2DeleteRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

fn artifact_v2_registry_key(run_id: &str, job_id: &str, name: &str) -> String {
    format!("{run_id}/{job_id}/{name}")
}

async fn save_artifact_v2_registry(shared: &Arc<SharedState>) -> Result<(), std::io::Error> {
    let registry_path = shared.state.state_dir.join("artifact_v2_registry.json");
    let serialized = {
        let inner = shared.state.inner.lock().await;
        serde_json::to_string(&inner.artifact_v2_registry)?
    };
    tokio::fs::write(&registry_path, serialized.as_bytes()).await?;
    Ok(())
}
async fn twirp_artifact_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_artifact_name(&request.name)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let token = uuid::Uuid::new_v4().to_string();
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create artifact stage dir: {e}")))?;
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .insert(token.clone(), ArtifactV2Pending { registry_key });
    }
    let upload_url = format!("{}/twirp-blob/artifact/{token}", public_base_url());
    info!(token, name = request.name, "artifact v2 create");
    Ok(Json(json!({ "ok": true, "signed_upload_url": upload_url })))
}

async fn twirp_artifact_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .iter()
            .find(|(_, p)| p.registry_key == registry_key)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending artifact upload for this name/run/job"))?;

    // Measure actual blob size.
    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token)
        .join("data");
    let size = tokio::fs::metadata(&blob_path)
        .await
        .map(|m| m.len())
        .unwrap_or_else(|_| match &request.size {
            serde_json::Value::String(s) => s.parse::<u64>().unwrap_or(0),
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        });

    let artifact_id;
    {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_pending.remove(&token);
        inner.next_artifact_v2_id += 1;
        artifact_id = inner.next_artifact_v2_id;
        let digest = request.hash.and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Object(ref obj) => obj
                .get("value")
                .and_then(|val| val.as_str().map(|s| s.to_owned())),
            _ => None,
        });
        inner.artifact_v2_registry.insert(
            registry_key,
            ArtifactV2Entry {
                id: artifact_id,
                workflow_run_backend_id: request.workflow_run_backend_id,
                workflow_job_run_backend_id: request.workflow_job_run_backend_id,
                name: request.name.clone(),
                size,
                created_at: server_iso_now(),
                digest,
                blob_token: token,
            },
        );
    }
    let _ = save_artifact_v2_registry(&shared).await;
    info!(
        artifact_id,
        name = request.name,
        size,
        "artifact v2 finalized"
    );
    Ok(Json(
        json!({ "ok": true, "artifact_id": artifact_id.to_string() }),
    ))
}

async fn twirp_artifact_v2_list(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2ListRequest>,
) -> Json<serde_json::Value> {
    let inner = shared.state.inner.lock().await;

    let name_filter: Option<String> = request.name_filter.and_then(|v| match v {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Object(ref obj) => obj
            .get("value")
            .and_then(|val| val.as_str().map(|s| s.to_owned())),
        _ => None,
    });
    let id_filter: Option<u64> = request.id_filter.and_then(|v| match v {
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::Object(ref obj) => obj.get("value").and_then(|val| match val {
            serde_json::Value::String(s) => s.parse::<u64>().ok(),
            serde_json::Value::Number(n) => n.as_u64(),
            _ => None,
        }),
        _ => None,
    });

    let artifacts: Vec<serde_json::Value> = inner
        .artifact_v2_registry
        .values()
        .filter(|e| {
            e.workflow_run_backend_id == request.workflow_run_backend_id
                && e.workflow_job_run_backend_id == request.workflow_job_run_backend_id
        })
        .filter(|e| name_filter.as_deref().map(|f| e.name == f).unwrap_or(true))
        .filter(|e| id_filter.map(|id| e.id == id).unwrap_or(true))
        .map(|e| {
            json!({
                "workflow_run_backend_id": e.workflow_run_backend_id,
                "workflow_job_run_backend_id": e.workflow_job_run_backend_id,
                "database_id": e.id.to_string(),
                "name": e.name,
                "size": e.size.to_string(),
                "created_at": e.created_at,
                "digest": e.digest.as_deref().unwrap_or("")
            })
        })
        .collect();
    Json(json!({ "artifacts": artifacts }))
}

async fn twirp_artifact_v2_get_signed_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2GetSignedUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let blob_token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_registry
            .get(&registry_key)
            .map(|e| e.blob_token.clone())
    }
    .ok_or_else(|| ApiError::not_found("artifact not found"))?;

    // URL must end in .zip so the toolkit's streamExtract detects it as a zip.
    let signed_url = format!("{}/twirp-blob/artifact/{blob_token}.zip", public_base_url());
    Ok(Json(json!({ "signed_url": signed_url })))
}

async fn twirp_artifact_v2_delete(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2DeleteRequest>,
) -> Json<serde_json::Value> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let removed = {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_registry.remove(&registry_key)
    };
    if let Some(e) = removed {
        let _ = save_artifact_v2_registry(&shared).await;
        let blob_dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(&e.blob_token);
        let _ = tokio::fs::remove_dir_all(blob_dir).await;
        Json(json!({ "ok": true, "artifact_id": e.id.to_string() }))
    } else {
        Json(json!({ "ok": false, "artifact_id": "0" }))
    }
}

// ─── Azure Block Blob compat blob store ───────────────────────────────────────
//
// Both actions/cache@v4 and actions/upload-artifact@v4 upload via the Azure SDK
// (BlockBlobClient).  The protocol is:
//   • Single-shot: PUT /twirp-blob/{kind}/{token}                  → 201
//   • Stage block: PUT /twirp-blob/{kind}/{token}?comp=block&blockid={b64} → 201
//   • Commit list: PUT /twirp-blob/{kind}/{token}?comp=blocklist   → 201
// Downloads (cache + artifact) use a plain GET.

#[derive(Debug, Deserialize)]
struct BlobPutQuery {
    comp: Option<String>,
    blockid: Option<String>,
}

/// Convert a base64 block ID to a filesystem-safe name.
fn blockid_to_filename(blockid: &str) -> String {
    blockid.replace('+', "-").replace('/', "_").replace('=', "")
}

/// Parse an Azure Block Blob blocklist XML body and return block IDs in order.
fn parse_blocklist_xml(body: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut pos = 0;
    while let Some(start_off) = body[pos..].find("<Latest>") {
        let content_start = pos + start_off + 8; // len("<Latest>") == 8
        if let Some(end_off) = body[content_start..].find("</Latest>") {
            let id = body[content_start..content_start + end_off]
                .trim()
                .to_owned();
            if !id.is_empty() {
                ids.push(id);
            }
            pos = content_start + end_off + 9; // len("</Latest>") == 9
        } else {
            break;
        }
    }
    ids
}

async fn blob_put(
    State(shared): State<Arc<SharedState>>,
    Path((kind, token)): Path<(String, String)>,
    Query(query): Query<BlobPutQuery>,
    body: axum::body::Bytes,
) -> StatusCode {
    let blob_root = shared
        .state
        .state_dir
        .join("blobs")
        .join(&kind)
        .join(&token);

    match query.comp.as_deref() {
        Some("block") => {
            let block_id = query.blockid.unwrap_or_default();
            let safe_id = blockid_to_filename(&block_id);
            let blocks_dir = blob_root.join("blocks");
            if let Err(e) = tokio::fs::create_dir_all(&blocks_dir).await {
                warn!(kind, token, "failed to create blocks dir: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            match tokio::fs::write(blocks_dir.join(&safe_id), &body).await {
                Ok(()) => {
                    debug!(
                        kind,
                        token,
                        block = safe_id,
                        bytes = body.len(),
                        "blob block staged"
                    );
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write block {safe_id}: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Some("blocklist") => {
            let body_str = String::from_utf8_lossy(&body);
            let block_ids = parse_blocklist_xml(&body_str);
            let blocks_dir = blob_root.join("blocks");
            let data_path = blob_root.join("data");

            let mut assembled: Vec<u8> = Vec::new();
            for bid in &block_ids {
                let safe_id = blockid_to_filename(bid);
                match tokio::fs::read(blocks_dir.join(&safe_id)).await {
                    Ok(bytes) => assembled.extend_from_slice(&bytes),
                    Err(e) => {
                        warn!(kind, token, "failed to read block {safe_id}: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            }
            match tokio::fs::write(&data_path, &assembled).await {
                Ok(()) => {
                    let _ = tokio::fs::remove_dir_all(&blocks_dir).await;
                    info!(
                        kind,
                        token,
                        size = assembled.len(),
                        blocks = block_ids.len(),
                        "blob assembled from blocks"
                    );
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write assembled blob: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        _ => {
            // Single-shot upload.
            let data_path = blob_root.join("data");
            match tokio::fs::write(&data_path, &body).await {
                Ok(()) => {
                    info!(kind, token, size = body.len(), "blob single-shot upload");
                    StatusCode::CREATED
                }
                Err(e) => {
                    warn!(kind, token, "failed to write single-shot blob: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
    }
}

async fn blob_get(
    State(shared): State<Arc<SharedState>>,
    Path((kind, mut token)): Path<(String, String)>,
) -> Response {
    // Artifact download URLs end in .zip for toolkit zip-detection.
    if kind == "artifact" && token.ends_with(".zip") {
        token.truncate(token.len() - 4);
    }

    if kind == "cache" {
        // Token is a download token → look up (key, version) in state.
        let kv = {
            let inner = shared.state.inner.lock().await;
            inner.cache_v2_dl_tokens.get(&token).cloned()
        };
        if let Some((key, version)) = kv {
            let empty: Vec<String> = Vec::new();
            return match shared.state.cache.get(&key, &version, &empty).await {
                Ok(Some((_entry, bytes))) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    bytes,
                )
                    .into_response(),
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(e) => {
                    warn!(key, version, "cache read error: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            };
        }
    }

    // Artifact (or cache fallback): serve from blob staging dir.
    let data_path = shared
        .state
        .state_dir
        .join("blobs")
        .join(&kind)
        .join(&token)
        .join("data");
    match tokio::fs::read(&data_path).await {
        Ok(bytes) => {
            if kind == "artifact" {
                let name = {
                    let inner = shared.state.inner.lock().await;
                    inner
                        .artifact_v2_registry
                        .values()
                        .find(|e| e.blob_token == token)
                        .map(|e| e.name.clone())
                };
                let filename = name.unwrap_or_else(|| "artifact".to_owned());
                let content_disposition = format!("attachment; filename=\"{filename}.zip\"");
                (
                    StatusCode::OK,
                    [
                        (header::CONTENT_TYPE, "application/zip"),
                        (header::CONTENT_DISPOSITION, &content_disposition),
                    ],
                    bytes,
                )
                    .into_response()
            } else {
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/octet-stream")],
                    bytes,
                )
                    .into_response()
            }
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Accept blob uploads (logs, summaries) at signed-URL paths.
/// Stores them in a local replay directory for conformance inspection.
async fn replay_results_put(
    State(shared): State<Arc<SharedState>>,
    axum::extract::Path(path): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> StatusCode {
    // Reject path traversal attempts
    if path.contains("..")
        || std::path::Path::new(&path)
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        tracing::warn!("Rejected path traversal attempt: {path}");
        return StatusCode::BAD_REQUEST;
    }

    let dest = shared
        .state
        .state_dir
        .join("replay")
        .join("results")
        .join(&path);
    if let Some(parent) = dest.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match std::fs::write(&dest, &body) {
        Ok(()) => {
            tracing::info!("Stored {} bytes at replay/results/{path}", body.len());
            StatusCode::CREATED
        }
        Err(e) => {
            tracing::warn!("Failed to store replay/results/{path}: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn next_message(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Option<azdo::TaskAgentMessage>>, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());

    let wait_seconds = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);

    loop {
        let mut inner = shared.state.inner.lock().await;
        if let Some(message) = inner
            .inflight_messages
            .get(&session_id)
            .and_then(|messages| messages.values().next().cloned())
        {
            return Ok(Json(Some(message)));
        }

        if let Some(cancellation) = inner.cancellation_queue.pop_front() {
            let body_json = concurrency::job_cancel_body(cancellation.agent_job_id);
            let message = build_task_agent_message(
                &mut inner,
                &session_id,
                azdo::message_type::JOB_CANCELLED,
                body_json,
            )?;
            return Ok(Json(Some(message)));
        }

        if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
            let request_finished = inner
                .job_requests
                .get(&request_id)
                .is_none_or(|request| request.result.is_some());
            if request_finished {
                inner.session_active_requests.remove(&session_id);
            } else {
                drop(inner);
                if wait_seconds == 0 {
                    return Ok(Json(None));
                }
                if tokio::time::timeout(
                    Duration::from_secs(wait_seconds),
                    shared.state.message_notify.notified(),
                )
                .await
                .is_err()
                {
                    return Ok(Json(None));
                }
                continue;
            }
        }

        let runner_labels = inner.runner_labels_for_session(&session_id);
        let Some(queued) = take_matching_job(&mut inner.queue, &runner_labels) else {
            drop(inner);
            if wait_seconds == 0 {
                return Ok(Json(None));
            }
            if tokio::time::timeout(
                Duration::from_secs(wait_seconds),
                shared.state.message_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok(Json(None));
            }
            continue;
        };

        // Update run status
        if let Some(run) = inner.runs.get_mut(&queued.run_id) {
            run.status = ExecutionStatus::InProgress;
            run.jobs
                .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        // F030: inject SystemVssConnection so the worker's AzDO reporting context
        // has a server URL, access token, and ResultsServiceUrl — same as broker_acquire_job.
        let mut msg = queued.message.clone();
        for endpoint in &mut msg.resources.endpoints {
            if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                endpoint.url = Some(runner_server_url());
                endpoint.authorization.parameters.insert(
                    "AccessToken".to_owned(),
                    shared
                        .state
                        .mint_runtime_token(&msg.plan.plan_id, &msg.job_id),
                );
                endpoint
                    .data
                    .insert("ResultsServiceUrl".to_owned(), public_base_url());
                endpoint
                    .data
                    .insert("PipelinesServiceUrl".to_owned(), runner_server_url());
                endpoint
                    .data
                    .insert("CacheServerUrl".to_owned(), public_base_url());
            }
        }
        debug!(
            endpoint_count = msg.resources.endpoints.len(),
            "F030: injected SystemVssConnection into AzDO job message"
        );
        let body_json = serde_json::to_string(&msg)
            .map_err(|e| ApiError::bad_request(format!("failed to serialize job message: {e}")))?;
        let request_id = queued.message.request_id;
        inner
            .session_active_requests
            .insert(session_id.clone(), request_id);
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.started_at = Some(std::time::SystemTime::now());
        }
        let message = build_task_agent_message(
            &mut inner,
            &session_id,
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
            body_json,
        )?;

        let run_id = queued.run_id;
        let job_id = queued.job_id.clone();
        drop(inner);

        github::report_check_run_in_progress(&shared, run_id, &job_id).await;

        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;

        return Ok(Json(Some(message)));
    }
}

async fn delete_session_message(
    State(shared): State<Arc<SharedState>>,
    Path((session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, &session_id, message_id).await
}

fn build_task_agent_message(
    inner: &mut InnerState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> Result<azdo::TaskAgentMessage, ApiError> {
    let session_key = inner
        .session_keys
        .get(session_id)
        .map(|s| s.key.clone())
        .unwrap_or_default();
    let (encrypted_body, iv) = if !session_key.is_empty() {
        let enc = SessionEncryption::from_key(session_key);
        enc.encrypt(body_json.as_bytes())
            .map_err(|e| ApiError::bad_request(format!("encryption failed: {e}")))?
    } else {
        (body_json.into_bytes(), vec![0u8; 16])
    };

    inner.next_message_id += 1;
    let message_id = inner.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: BASE64_STANDARD.encode(&encrypted_body),
        iv: Some(BASE64_STANDARD.encode(&iv)),
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    Ok(message)
}

fn build_broker_plaintext_message(
    inner: &mut InnerState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> azdo::TaskAgentMessage {
    inner.next_message_id += 1;
    let message_id = inner.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: body_json,
        iv: None,
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    message
}

async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    ack_message(shared, session_id, message_id).await
}

async fn ack_message(shared: Arc<SharedState>, session_id: &str, message_id: i64) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    if let Some(messages) = inner.inflight_messages.get_mut(session_id) {
        messages.remove(&message_id);
        if messages.is_empty() {
            inner.inflight_messages.remove(session_id);
        }
    }
    StatusCode::NO_CONTENT
}

async fn complete_job(
    State(shared): State<Arc<SharedState>>,
    Json(completion): Json<JobCompletion>,
) -> Result<Json<RunRecord>, ApiError> {
    complete_job_inner(shared, completion).await
}

async fn complete_job_compat(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, job_id)): Path<(RunId, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<RunRecord>, ApiError> {
    let status = match body.get("status").and_then(|value| value.as_str()) {
        Some("success" | "succeeded" | "completed") => ExecutionStatus::Success,
        Some("cancelled" | "canceled") => ExecutionStatus::Cancelled,
        Some("skipped") => ExecutionStatus::Skipped,
        _ => ExecutionStatus::Failure,
    };
    complete_job_inner(
        shared,
        JobCompletion {
            run_id,
            job_id: JobId(job_id),
            status,
            outputs: Default::default(),
        },
    )
    .await
}

/// GET /_apis/v1/AgentRequest/:pool_id/:request_id — query a job request lease/result.
///
/// The official listener calls this when another job arrives while the previous
/// worker process may still be unwinding. Returning a completed `result` lets it
/// safely move on; 404/405 makes it cancel the worker and can poison matrix runs.
async fn agent_request_get(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let request = inner
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("agent request not found"))?;
    Ok(Json(agent_request_json(pool_id, request)))
}

/// POST /_apis/v1/AgentRequest/:pool_id/:request_id — best-effort request ack.
async fn agent_request_ack(Path((_pool_id, _request_id)): Path<(i64, i64)>) -> StatusCode {
    StatusCode::OK
}

/// PATCH /_apis/v1/AgentRequest/:pool_id/:request_id — renew or complete job request.
/// The runner sends this to renew the job lock or report completion.
async fn agent_request_patch(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    info!(?body, "agent_request_patch received");
    // If this is a completion (has result), delegate to complete_job_inner
    // so summarize_run, promote_ready_jobs, and notify_waiters all fire.
    // The result field is only present on the final PATCH; renewals have no result.
    if let Some(result) = body.get("result").and_then(|v| v.as_str()) {
        let new_status = match execution_status_from_runner_result(result) {
            Some(status) => status,
            None => {
                info!(request_id, %result, "unknown agent_request_patch result; skipping completion");
                return Json(
                    json!({ "requestId": request_id, "lockedUntil": "2099-12-31T23:59:59Z" }),
                );
            }
        };
        // Look up (run_id, job_id) under the inner lock, then drop it before calling
        // complete_job_inner which acquires the lock itself.
        let completion = {
            let mut inner = shared.state.inner.lock().await;
            let mut already_completed = false;
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                already_completed = request.result.is_some();
                request.result = Some(new_status);
                request.locked_until = agent_request_locked_until();
            }
            if already_completed {
                inner.inflight_requests.remove(&request_id);
                info!(
                    request_id,
                    result, "agent request already completed; refreshing result only"
                );
                None
            } else if let Some((run_id, job_id)) = inner.inflight_requests.remove(&request_id) {
                info!(%run_id, %job_id, result, "job completed via agent_request_patch");
                Some(JobCompletion {
                    run_id,
                    job_id,
                    status: new_status,
                    outputs: Default::default(),
                })
            } else {
                info!(
                    request_id,
                    "no inflight job for request_id; ignoring result"
                );
                None
            }
        };
        if let Some(c) = completion {
            let _ = complete_job_inner(shared.clone(), c).await;
        }
        return Json(agent_request_response(&shared, pool_id, request_id).await);
    }
    // Renewal — runner is still working; just extend the lock.
    {
        let mut inner = shared.state.inner.lock().await;
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.locked_until = agent_request_locked_until();
            request.last_renewed_at = Some(std::time::SystemTime::now());
        }
    }
    Json(agent_request_response(&shared, pool_id, request_id).await)
}

async fn agent_request_response(
    shared: &Arc<SharedState>,
    pool_id: i64,
    request_id: i64,
) -> serde_json::Value {
    let inner = shared.state.inner.lock().await;
    inner
        .job_requests
        .get(&request_id)
        .map(|request| agent_request_json(pool_id, request))
        .unwrap_or_else(|| {
            json!({
                "requestId": request_id,
                "poolId": pool_id,
                "lockedUntil": agent_request_locked_until(),
            })
        })
}

fn agent_request_json(pool_id: i64, request: &TaskAgentJobRequestRecord) -> serde_json::Value {
    json!({
        "requestId": request.request_id,
        "poolId": pool_id,
        "jobId": request.agent_job_id,
        "jobName": request.job_id.to_string(),
        "planId": request.plan_id,
        "planType": request.plan_type,
        "lockedUntil": request.locked_until,
        "result": request.result.map(agent_request_result),
    })
}

fn agent_request_result(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Success => "succeeded",
        ExecutionStatus::Failure => "failed",
        ExecutionStatus::Cancelled => "canceled",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress => {
            "pending"
        }
    }
}

fn agent_request_locked_until() -> String {
    "2099-12-31T23:59:59Z".to_owned()
}

fn task_result_status(result: azdo::TaskResult) -> ExecutionStatus {
    match result {
        azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues => {
            ExecutionStatus::Success
        }
        azdo::TaskResult::Failed => ExecutionStatus::Failure,
        azdo::TaskResult::Cancelled => ExecutionStatus::Cancelled,
        azdo::TaskResult::Skipped => ExecutionStatus::Skipped,
    }
}

fn resolve_callback_job(
    inner: &InnerState,
    plan_id: &str,
    timeline_id: Option<uuid::Uuid>,
    agent_job_id: Option<uuid::Uuid>,
) -> Option<(i64, RunId, JobId)> {
    let request_id = inner
        .plan_requests
        .get(plan_id)
        .copied()
        .or_else(|| timeline_id.and_then(|id| inner.timeline_requests.get(&id).copied()))
        .or_else(|| agent_job_id.and_then(|id| inner.agent_job_requests.get(&id).copied()))?;
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

fn sole_active_unfinished_request(inner: &InnerState) -> Option<i64> {
    let mut active = inner
        .session_active_requests
        .values()
        .copied()
        .filter(|request_id| {
            inner
                .job_requests
                .get(request_id)
                .is_some_and(|request| request.result.is_none())
        });
    let request_id = active.next()?;
    if active.next().is_none() {
        return Some(request_id);
    }
    None
}
fn job_request_tuple(inner: &InnerState, request_id: i64) -> Option<(i64, RunId, JobId)> {
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    if !is_terminal_status(completion.status) {
        return Err(ApiError::bad_request(
            "job completion status must be terminal",
        ));
    }
    let mut inner = shared.state.inner.lock().await;
    {
        let run = inner
            .runs
            .get_mut(&completion.run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let prior = run
            .jobs
            .get(&completion.job_id)
            .copied()
            .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
        if is_terminal_status(prior) && prior != ExecutionStatus::Cancelled {
            return Ok(Json(run.clone()));
        }
        let effective = match (prior, completion.status) {
            (ExecutionStatus::Cancelled, ExecutionStatus::Success)
            | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => ExecutionStatus::Cancelled,
            _ => completion.status,
        };
        run.jobs.insert(completion.job_id.clone(), effective);
        let job_name = completion.job_id.0.clone();
        if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
            run.jobs_list[pos].conclusion = format!("{:?}", effective).to_lowercase();
        } else {
            run.jobs_list.push(JobDetail {
                name: job_name,
                conclusion: format!("{:?}", effective).to_lowercase(),
                steps: Vec::new(),
            });
        }
        run.job_outputs.insert(
            completion.job_id.clone(),
            completion
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        propagate_reusable_outputs(run);
        run.status = summarize_run(run.jobs.values().copied());
    }
    // Use the status actually stored (may differ from completion if terminal-locked).
    let effective_status = inner
        .runs
        .get(&completion.run_id)
        .and_then(|r| r.jobs.get(&completion.job_id).copied())
        .unwrap_or(completion.status);
    let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
        apply_matrix_fail_fast(&mut inner, completion.run_id, &completion.job_id)
    } else {
        Vec::new()
    };
    // A terminal job must not remain dispatchable, including completion via
    // the native/internal API before a runner acquires it.
    inner
        .queue
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .concurrency_blocked
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    if let Some(held) = inner.held_runs.get_mut(&completion.run_id) {
        held.retain(|job| job.job_id != completion.job_id);
        if held.is_empty() {
            inner.held_runs.remove(&completion.run_id);
        }
    }
    // Release concurrency for the completed job / run, which may promote held work.
    release_concurrency_for_job(&mut inner, completion.run_id, &completion.job_id);
    let scheduling = promote_ready_jobs(&mut inner);
    let record = inner
        .runs
        .get(&completion.run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    // Mark agent request finished and free the broker session slot so the
    // runner can immediately poll the next job (including concurrency successors).
    let finished_request_ids: Vec<i64> = inner
        .job_requests
        .iter()
        .filter(|(_, r)| r.run_id == completion.run_id && r.job_id == completion.job_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in &finished_request_ids {
        if let Some(req) = inner.job_requests.get_mut(request_id) {
            if req.result.is_none() {
                req.result = Some(effective_status);
            }
        }
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != *request_id);
        inner.inflight_requests.remove(request_id);
    }
    // Evict live-log state for this job to prevent unbounded memory growth.
    // The durable step-log blob has already been uploaded by the runner.
    if let Some(agent_key) = inner
        .job_requests
        .values()
        .find(|r| r.run_id == completion.run_id && r.job_id == completion.job_id)
        .map(|r| r.agent_job_id.to_string())
    {
        inner.live_log_lines.remove(&agent_key);
        inner.live_log_tx.remove(&agent_key);
    }
    inner.dap_ports.remove(&completion.run_id);
    let queue_nonempty = !inner.queue.is_empty() || !inner.cancellation_queue.is_empty();
    drop(inner);

    github::report_check_run_completed(
        &shared,
        completion.run_id,
        &completion.job_id,
        effective_status,
    )
    .await;

    if scheduling.promoted > 0 || !cancelled_siblings.is_empty() || queue_nonempty {
        shared.state.message_notify.notify_waiters();
    }

    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id: completion.run_id,
            job_id: completion.job_id,
            status: effective_status,
            reason: None,
        })
        .await;
    for job_id in cancelled_siblings {
        github::report_check_run_completed(
            &shared,
            completion.run_id,
            &job_id,
            ExecutionStatus::Cancelled,
        )
        .await;
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id: completion.run_id,
                job_id,
                status: ExecutionStatus::Cancelled,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.skipped {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Skipped,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.failed {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Failure,
                reason: None,
            })
            .await;
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id: completion.run_id,
            status: record.status,
            reason: None,
        })
        .await;
    Ok(Json(record))
}

#[derive(Default)]
struct SchedulingOutcome {
    promoted: usize,
    skipped: Vec<(RunId, JobId)>,
    failed: Vec<(RunId, JobId)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyDecision {
    Wait,
    Run,
    Skip,
    Error,
}

/// Promote or skip pending jobs once every declared dependency is terminal.
fn promote_ready_jobs(inner: &mut InnerState) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        let mut promoted_by_base: BTreeMap<(RunId, String), u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut settled = false;

        while let Some(mut job) = inner.pending_jobs.pop_front() {
            let decision = inner
                .runs
                .get(&job.run_id)
                .map(|run| dependency_decision(run, &job))
                .unwrap_or(DependencyDecision::Wait);
            match decision {
                DependencyDecision::Run
                    if under_max_parallel(inner, &job)
                        && promoted_by_base
                            .get(&(job.run_id, job.base_id.clone()))
                            .copied()
                            .unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    if let Some(run) = inner.runs.get(&job.run_id) {
                        hydrate_needs_context(&mut job, run);
                    }
                    *promoted_by_base
                        .entry((job.run_id, job.base_id.clone()))
                        .or_default() += 1;
                    promoted.push(job);
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    if let Some(run) = inner.runs.get_mut(&job.run_id) {
                        let status = if decision == DependencyDecision::Skip {
                            ExecutionStatus::Skipped
                        } else {
                            ExecutionStatus::Failure
                        };
                        run.jobs.insert(job.job_id.clone(), status);
                        run.status = summarize_run(run.jobs.values().copied());
                    }
                    if decision == DependencyDecision::Skip {
                        outcome.skipped.push((job.run_id, job.job_id));
                    } else {
                        outcome.failed.push((job.run_id, job.job_id));
                    }
                    settled = true;
                }
                DependencyDecision::Wait | DependencyDecision::Run => remaining.push_back(job),
            }
        }

        outcome.promoted += promoted.len();
        inner.pending_jobs = remaining;
        inner.queue.extend(promoted);
        if !settled {
            return outcome;
        }
    }
}

fn dependency_decision(run: &RunRecord, job: &QueuedJob) -> DependencyDecision {
    if job.needs.is_empty() {
        return DependencyDecision::Run;
    }
    let direct_statuses = job
        .needs
        .iter()
        .flat_map(|need| matching_need_statuses(run, need))
        .collect::<Vec<_>>();
    if direct_statuses.is_empty()
        || direct_statuses
            .iter()
            .any(|status| !is_terminal_status(*status))
    {
        return DependencyDecision::Wait;
    }
    let statuses = ancestor_statuses(run, job);
    let aggregate = aggregate_need_status(&statuses).unwrap_or(ExecutionStatus::Skipped);
    let context = job.condition_context.clone().with_status(
        aggregate == ExecutionStatus::Success,
        aggregate == ExecutionStatus::Failure,
        aggregate == ExecutionStatus::Cancelled,
    );
    let mut context = context;
    context.insert("needs", needs_json_context(run, &job.needs));
    let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
    match aksh_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) => DependencyDecision::Skip,
        Err(_) => DependencyDecision::Error,
    }
}

fn matching_need_ids(run: &RunRecord, need: &JobId) -> Vec<JobId> {
    run.jobs
        .keys()
        .filter(|job_id| {
            *job_id == need
                || run
                    .job_base_ids
                    .get(*job_id)
                    .is_some_and(|base| base == &need.0)
        })
        .cloned()
        .collect()
}

fn matching_need_statuses(run: &RunRecord, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(run, need)
        .iter()
        .filter_map(|job_id| run.jobs.get(job_id).copied())
        .collect()
}

fn ancestor_statuses(run: &RunRecord, job: &QueuedJob) -> Vec<ExecutionStatus> {
    let mut pending = job
        .needs
        .iter()
        .flat_map(|need| matching_need_ids(run, need))
        .collect::<Vec<_>>();
    let mut visited = std::collections::BTreeSet::new();
    let mut statuses = Vec::new();

    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        if let Some(status) = run.jobs.get(&job_id) {
            statuses.push(*status);
        }
        if let Some(needs) = run.job_needs.get(&job_id) {
            pending.extend(needs.iter().flat_map(|need| matching_need_ids(run, need)));
        }
    }
    statuses
}

fn is_terminal_status(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled
    )
}

/// Check if a job's `runs-on` labels match a runner's registered labels.
///
/// A job matches when every label in the job's `runs-on` is present in the
/// runner's label set (case-insensitive). GitHub-hosted runner labels like
/// `ubuntu-latest` are treated as aliases for common self-hosted labels.
fn job_matches_runner(job_labels: &[String], runner_labels: &[String]) -> bool {
    // Empty runs-on matches any runner (shouldn't happen, but be safe)
    if job_labels.is_empty() {
        return true;
    }
    // Unknown runner (no session→runner mapping) matches any job.
    // This preserves backward compat for tests and legacy session paths.
    if runner_labels.is_empty() {
        return true;
    }
    let runner_set: std::collections::HashSet<String> =
        runner_labels.iter().map(|l| l.to_lowercase()).collect();
    job_labels.iter().all(|required| {
        let req = required.to_lowercase();
        // Direct match
        if runner_set.contains(&req) {
            return true;
        }
        // GitHub-hosted aliases: treat `ubuntu-latest`, `ubuntu-24.04`, etc.
        // as matching a runner with "linux" label; `macos-latest` matches "macos";
        // `windows-latest` matches "windows".
        if req.starts_with("ubuntu") && runner_set.contains("linux") {
            return true;
        }
        if req.starts_with("macos") && runner_set.contains("macos") {
            return true;
        }
        if req.starts_with("windows") && runner_set.contains("windows") {
            return true;
        }
        // Broad fallback: if the runner has "self-hosted" and the job only
        // specifies a GitHub-hosted label (e.g. "ubuntu-latest"), match it.
        // This lets single-runner local setups work without label gymnastics.
        if runner_set.contains("self-hosted")
            && (req.starts_with("ubuntu") || req.starts_with("macos") || req.starts_with("windows"))
        {
            return true;
        }
        false
    })
}

/// Find and remove the first job in the queue that matches the given runner's labels.
/// Returns `None` if no matching job is found.
fn take_matching_job(
    queue: &mut VecDeque<QueuedJob>,
    runner_labels: &[String],
) -> Option<QueuedJob> {
    let pos = queue
        .iter()
        .position(|job| job_matches_runner(&job.runs_on, runner_labels))?;
    queue.remove(pos)
}

fn under_max_parallel(inner: &InnerState, job: &QueuedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = inner
        .queue
        .iter()
        .filter(|queued| queued.run_id == job.run_id && queued.base_id == job.base_id)
        .count() as u64;
    let active_running = inner
        .runs
        .get(&job.run_id)
        .map(|run| {
            run.jobs
                .iter()
                .filter(|(job_id, status)| {
                    run.job_base_ids.get(*job_id) == Some(&job.base_id)
                        && matches!(status, ExecutionStatus::InProgress)
                })
                .count() as u64
        })
        .unwrap_or(0);

    active_in_queue + active_running < max_parallel
}

fn apply_matrix_fail_fast(inner: &mut InnerState, run_id: RunId, failed_job: &JobId) -> Vec<JobId> {
    let Some(run) = inner.runs.get_mut(&run_id) else {
        return Vec::new();
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return Vec::new();
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return Vec::new();
    }

    // Track in-progress siblings: they need a JOB_CANCELLED message so the
    // runner aborts the worker. Queued siblings only need their state flipped
    // — they were never dispatched.
    let mut cancelled_jobs = Vec::new();
    let mut cancellations = Vec::new();
    for (job_id, status) in &mut run.jobs {
        if job_id != failed_job
            && run.job_base_ids.get(job_id) == Some(&base_id)
            && matches!(
                status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            )
        {
            if matches!(status, ExecutionStatus::InProgress) {
                // Resolve agent_job_id after loop (borrow checker).
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id: uuid::Uuid::nil(), // filled below
                });
            }
            cancelled_jobs.push(job_id.clone());
            *status = ExecutionStatus::Cancelled;
        }
    }
    run.status = summarize_run(run.jobs.values().copied());
    inner
        .queue
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    // Fill real agent_job_ids; drop cancellations for jobs not in flight.
    cancellations.retain_mut(|c| {
        if let Some(id) = agent_job_id_for(inner, c.run_id, &c.job_id) {
            c.agent_job_id = id;
            true
        } else {
            false
        }
    });
    inner.cancellation_queue.extend(cancellations);
    cancelled_jobs
}

fn hydrate_needs_context(job: &mut QueuedJob, run: &RunRecord) {
    let needs = job
        .needs
        .iter()
        .filter_map(|need| need_context(run, need).map(|context| (need.0.clone(), context)))
        .collect();
    job.message
        .context_data
        .insert("needs".to_owned(), azdo::PipelineContextData::Dict(needs));
}
fn needs_json_context(run: &RunRecord, needs: &[JobId]) -> serde_json::Value {
    let values = needs
        .iter()
        .filter_map(|need| {
            let statuses = matching_need_statuses(run, need);
            let result = aggregate_need_status(&statuses)?;
            let matching_ids = matching_need_ids(run, need);
            let mut outputs = serde_json::Map::new();
            for job_id in matching_ids {
                if let Some(job_outputs) = run.job_outputs.get(&job_id) {
                    outputs.extend(job_outputs.clone());
                }
            }
            Some((
                need.0.clone(),
                json!({
                    "result": status_string(result),
                    "outputs": outputs,
                }),
            ))
        })
        .collect();
    serde_json::Value::Object(values)
}

fn aggregate_need_status(statuses: &[ExecutionStatus]) -> Option<ExecutionStatus> {
    if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        Some(ExecutionStatus::Failure)
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        Some(ExecutionStatus::Cancelled)
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Skipped)
    {
        Some(ExecutionStatus::Skipped)
    } else if !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Success)
    {
        Some(ExecutionStatus::Success)
    } else {
        None
    }
}

fn need_context(run: &RunRecord, need: &JobId) -> Option<azdo::PipelineContextData> {
    let statuses = matching_need_statuses(run, need);
    let result = aggregate_need_status(&statuses)?;
    let mut outputs = BTreeMap::new();
    for job_id in matching_need_ids(run, need) {
        if let Some(job_outputs) = run.job_outputs.get(&job_id) {
            for (key, value) in job_outputs {
                outputs.insert(key.clone(), azdo::PipelineContextData::from_json(value));
            }
        }
    }

    let mut context = BTreeMap::new();
    context.insert(
        "result".to_owned(),
        azdo::PipelineContextData::String(status_string(result)),
    );
    context.insert(
        "outputs".to_owned(),
        azdo::PipelineContextData::Dict(outputs),
    );
    Some(azdo::PipelineContextData::Dict(context))
}

fn status_string(status: ExecutionStatus) -> String {
    match status {
        ExecutionStatus::Queued
        | ExecutionStatus::Pending
        | ExecutionStatus::InProgress
        | ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
    .to_owned()
}

// ─── Phase E: Timeline, logs, completion ────────────────────────────────────

/// PATCH timeline records — runner updates step/job state.
async fn patch_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    let records = wrapper.value;
    let count = records.len();
    let callback_job = {
        let inner = shared.state.inner.lock().await;
        resolve_callback_job(&inner, &plan_id, timeline_id.parse().ok(), None)
    };
    let run_id = callback_job
        .as_ref()
        .map(|(_, run_id, _)| *run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let logical_job_id = callback_job.as_ref().map(|(_, _, job_id)| job_id.clone());
    let mut projected = Vec::new();
    for record in &records {
        if let Some(state) = &record.state {
            info!(
                timeline_id = %timeline_id,
                record_id = %record.id,
                name = record.display_name.as_deref().unwrap_or(""),
                state = ?state,
                "timeline record update"
            );
        }
        if let (Some(run_id), Some(status)) = (run_id, timeline_status(record)) {
            projected.push(NdjsonEvent::JobStatus {
                run_id,
                job_id: logical_job_id
                    .clone()
                    .unwrap_or_else(|| JobId(record.id.to_string())),
                status,
                reason: None,
            });
        }
        if let Some(run_id) = run_id {
            for issue in &record.issues {
                projected.push(NdjsonEvent::Annotation {
                    run_id,
                    job_id: logical_job_id
                        .clone()
                        .unwrap_or_else(|| JobId(record.id.to_string())),
                    level: issue_level(issue.issue_type),
                    message: issue.message.clone().unwrap_or_default(),
                    file: issue.data.get("file").cloned(),
                    line: issue.data.get("line").and_then(|line| line.parse().ok()),
                });
            }
        }
    }
    if let Some(run_id) = run_id {
        let mut inner = shared.state.inner.lock().await;
        inner
            .timeline_events
            .entry(run_id)
            .or_default()
            .extend(projected.clone());

        if let Some(job_id) = logical_job_id {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }

                for record in &records {
                    let Some(name) = &record.display_name else {
                        continue;
                    };
                    if record.id.to_string() == job_id.0 {
                        continue;
                    }

                    let conclusion_str = match record.result {
                        Some(
                            azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues,
                        ) => "success",
                        Some(azdo::TaskResult::Failed) => {
                            if run.jobs.get(&job_id) == Some(&ExecutionStatus::Cancelled) {
                                "cancelled"
                            } else {
                                "failure"
                            }
                        }
                        Some(azdo::TaskResult::Cancelled) => "cancelled",
                        Some(azdo::TaskResult::Skipped) => "skipped",
                        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
                            "in_progress"
                        }
                        _ => "success",
                    };

                    if let Some(pos) = job_detail.steps.iter().position(|s| s.name == *name) {
                        job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                    } else {
                        job_detail.steps.push(StepRecord {
                            name: name.clone(),
                            conclusion: conclusion_str.to_owned(),
                        });
                    }
                }
            }
        }
    }
    for event in projected {
        shared.state.emit(event).await;
    }
    Json(json!({ "count": count, "value": records }))
}
fn timeline_status(record: &azdo::TimelineRecord) -> Option<ExecutionStatus> {
    match record.result {
        Some(azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues) => {
            Some(ExecutionStatus::Success)
        }
        Some(azdo::TaskResult::Failed) => Some(ExecutionStatus::Failure),
        Some(azdo::TaskResult::Cancelled) => Some(ExecutionStatus::Cancelled),
        Some(azdo::TaskResult::Skipped) => Some(ExecutionStatus::Skipped),
        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
            Some(ExecutionStatus::InProgress)
        }
        _ => None,
    }
}

fn issue_level(issue_type: azdo::IssueType) -> AnnotationLevel {
    match issue_type {
        azdo::IssueType::Error => AnnotationLevel::Error,
        azdo::IssueType::Warning => AnnotationLevel::Warning,
        azdo::IssueType::Info => AnnotationLevel::Notice,
    }
}

/// POST create log file — runner creates a log container.
async fn create_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(mut log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    let mut inner = shared.state.inner.lock().await;
    let next_id = inner.next_log_id;
    inner.next_log_id = next_id.wrapping_add(1);
    log.id = next_id as i64;
    let key = format!("{}/{}", plan_id, next_id);
    inner.logs.entry(key).or_default();
    Json(serde_json::to_value(&log).unwrap_or(json!({ "ok": true })))
}

/// POST append log — runner appends lines to a log file.
async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    let mut inner = shared.state.inner.lock().await;
    let masked = mask_log_bytes(&inner, &plan_id, &body);
    inner
        .logs
        .entry(key)
        .or_default()
        .extend_from_slice(&masked);
    StatusCode::ACCEPTED
}

fn log_key(plan_id: &str, log_id: &str) -> String {
    format!("{plan_id}/{log_id}")
}

fn mask_log_bytes(inner: &InnerState, plan_id: &str, body: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(body);
    let resolved_run_id = resolve_callback_job(inner, plan_id, None, None)
        .map(|(_, run_id, _)| run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let run_secrets: Vec<String> = resolved_run_id
        .and_then(|run_id| inner.runs.get(&run_id))
        .map(|run| {
            run.submission
                .secrets
                .values()
                .map(|s| s.expose().to_owned())
                .collect()
        })
        .unwrap_or_else(|| {
            inner
                .runs
                .values()
                .flat_map(|run| run.submission.secrets.values())
                .map(|s| s.expose().to_owned())
                .collect()
        });

    aksh_gha_protocol::masking::mask_secrets(&text, run_secrets.iter().map(String::as_str), &[])
        .into_bytes()
}

/// POST console log — runner streams live console output.
async fn console_log(
    State(_shared): State<Arc<SharedState>>,
    Path((_scope, _hub, _plan_id, _timeline_id, _record_id)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    _body: Bytes,
) -> StatusCode {
    StatusCode::OK
}

/// POST finish job — runner reports final result + outputs.
async fn finish_job(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Json<serde_json::Value> {
    let status = task_result_status(event.result);
    let outputs = event
        .outputs
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect();
    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let callback_resolved = resolve_callback_job(
            &inner,
            &plan_id,
            Some(event.timeline_id),
            Some(event.job_id),
        );
        let active_resolved =
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id));
        let resolved = callback_resolved.or(active_resolved).or_else(|| {
            plan_id
                .parse::<RunId>()
                .ok()
                .map(|run_id| (0, run_id, JobId(event.job_id.to_string())))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
            })
        } else {
            None
        }
    };

    info!(
        job_id = %event.job_id,
        result = ?event.result,
        outputs = ?event.outputs,
        "job completed"
    );

    if let Some(completion) = completion {
        let _ = complete_job_inner(shared, completion).await;
    } else {
        warn!(
            plan_id,
            job_id = %event.job_id,
            timeline_id = %event.timeline_id,
            "finish_job could not resolve callback to a run/job"
        );
    }

    Json(json!({ "ok": true }))
}

// ── F030: standard AzDO `/_apis/v1/plans/` route handlers ────────────────────
// These use the URL pattern our AzDO client sends (`plans/{planId}/...`) rather
// than the scoped pattern (`Timeline/{scope}/{hub}/{planId}/{timelineId}`).
// The logic is identical to the existing handlers above.

/// PATCH `/_apis/v1/plans/:plan_id/timelines/:timeline_id/records`
async fn patch_timeline_records_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    patch_timeline_records(
        State(shared),
        Path((String::new(), String::new(), plan_id, timeline_id)),
        Json(wrapper),
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/logs`
async fn create_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    create_log(
        State(shared),
        Path((String::new(), String::new(), plan_id)),
        Json(log),
    )
    .await
}

/// PUT `/_apis/v1/plans/:plan_id/logs/:log_id`
async fn append_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, log_id)): Path<(String, String)>,
    body: Bytes,
) -> StatusCode {
    append_log(
        State(shared),
        Path((String::new(), String::new(), plan_id, log_id)),
        body,
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/events`
///
/// Handles the `JobCompleted` event sent by the runner's AzDO reporting path.
/// The body shape is `{name, jobId, requestId, result, outputs}` — slightly
/// different from the scoped `finish_job` path which uses `JobCompletedEvent`.
async fn finish_job_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(event): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let result_str = event
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("failed");
    let status =
        execution_status_from_runner_result(result_str).unwrap_or(ExecutionStatus::Failure);
    let job_id_str = event.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
    let outputs = event
        .get("outputs")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();
    let outputs: aksh_gha_protocol::OutputMap = outputs
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    info!(
        plan_id,
        job_id = job_id_str,
        result = result_str,
        "finish_job_plan"
    );

    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let resolved = resolve_callback_job(&inner, &plan_id, None, None).or_else(|| {
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
            })
        } else {
            warn!(plan_id, "finish_job_plan: could not resolve run/job");
            None
        }
    };
    if let Some(c) = completion {
        let _ = complete_job_inner(shared, c).await;
    }
    Json(json!({ "ok": true }))
}

/// POST action download info — resolve action references to download URLs.
async fn action_download_info(
    State(_shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut tickets = serde_json::Map::new();
    collect_action_download_refs(&request, &mut tickets);

    Json(json!({
        "archiveDownloadTickets": tickets.clone(),
        // Some runner/protocol paths call the same payload an actionsDownloadInfo
        // map. Return both names so legacy and batch clients can consume the same
        // local fallback without a second resolution path.
        "actionsDownloadInfo": tickets,
    }))
}

async fn runnerresolve_actions(
    State(_shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let mut actions = serde_json::Map::new();
    collect_runnerresolve_refs(&request, &mut actions);

    Json(json!({ "actions": actions }))
}

async fn download_action_tarball(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, git_ref)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    // 1. Sanitize parameters to avoid directory traversal
    if owner.contains('.')
        || owner.contains('/')
        || owner.contains('\\')
        || repo.contains('.')
        || repo.contains('/')
        || repo.contains('\\')
        || git_ref.contains("..")
        || git_ref.contains('\\')
    {
        return Err(ApiError::bad_request("invalid owner, repo, or git_ref"));
    }

    let cache_dir = shared
        .state
        .state_dir
        .join("actions")
        .join(&owner)
        .join(&repo)
        .join(&git_ref);
    let cached_path = cache_dir.join("action.tar.gz");

    if cached_path.exists() {
        let file = tokio::fs::File::open(&cached_path)
            .await
            .map_err(|e| ApiError::internal(format!("failed to open cached action: {e}")))?;
        let stream = tokio_util::io::ReaderStream::new(file);
        let body = Body::from_stream(stream);

        let res = Response::builder()
            .header(header::CONTENT_TYPE, "application/gzip")
            .header(
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{repo}-{git_ref}.tar.gz\""),
            )
            .body(body)
            .map_err(|e| ApiError::internal(format!("failed to build response: {e}")))?;
        return Ok(res);
    }

    // Cache Miss: Download from GitHub
    tokio::fs::create_dir_all(&cache_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create action cache dir: {e}")))?;

    let temp_path = cache_dir.join("action.tar.gz.tmp");
    let github_url = format!("https://api.github.com/repos/{owner}/{repo}/tarball/{git_ref}");

    info!(
        owner,
        repo, git_ref, github_url, "Downloading action to server cache"
    );

    let client = reqwest::Client::builder()
        .user_agent("aksh-runner-server")
        .build()
        .map_err(|e| ApiError::internal(format!("failed to build reqwest client: {e}")))?;

    let response = client.get(&github_url).send().await.map_err(|e| {
        ApiError::internal(format!("failed to send download request to GitHub: {e}"))
    })?;

    if !response.status().is_success() {
        return Err(ApiError::not_found(format!(
            "GitHub returned status {} for {}",
            response.status(),
            github_url
        )));
    }

    let mut temp_file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create temporary action file: {e}")))?;

    let mut stream = response.bytes_stream();
    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            ApiError::internal(format!("failed to read chunk from GitHub response: {e}"))
        })?;
        tokio::io::copy(&mut &chunk[..], &mut temp_file)
            .await
            .map_err(|e| {
                ApiError::internal(format!("failed to write chunk to temporary file: {e}"))
            })?;
    }

    // Atomically rename to final target path
    tokio::fs::rename(&temp_path, &cached_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to rename cached action file: {e}")))?;

    info!(cached_path = ?cached_path, "Action cached successfully on server");

    let file = tokio::fs::File::open(&cached_path)
        .await
        .map_err(|e| ApiError::internal(format!("failed to open newly cached action: {e}")))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let res = Response::builder()
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{repo}-{git_ref}.tar.gz\""),
        )
        .body(body)
        .map_err(|e| ApiError::internal(format!("failed to build response: {e}")))?;
    Ok(res)
}

fn collect_action_download_refs(
    value: &serde_json::Value,
    tickets: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Some((key, ticket)) = action_download_ticket(raw, None) {
                tickets.entry(key).or_insert(ticket);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_action_download_refs(item, tickets);
            }
        }
        serde_json::Value::Object(map) => {
            let action = map
                .get("action")
                .or_else(|| map.get("name"))
                .or_else(|| map.get("nameWithOwner"))
                .or_else(|| map.get("repository"))
                .and_then(|v| v.as_str());
            let version = map
                .get("version")
                .or_else(|| map.get("ref"))
                .or_else(|| map.get("reference"))
                .and_then(|v| v.as_str());
            if let Some(action) = action {
                if let Some((key, ticket)) = action_download_ticket(action, version) {
                    tickets.entry(key).or_insert(ticket);
                }
            }

            for nested in map.values() {
                collect_action_download_refs(nested, tickets);
            }
        }
        _ => {}
    }
}

fn action_download_ticket(
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    if action.starts_with("./") || action.starts_with("../") || action.starts_with("docker://") {
        return None;
    }

    let (repo_part, git_ref) = if let Some(version) = version_override {
        (action, version)
    } else {
        action.split_once('@')?
    };
    if git_ref.is_empty() {
        return None;
    }

    let mut parts = repo_part.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if owner.is_empty() || repo.is_empty() {
        return None;
    }

    let key = format!("{repo_part}@{git_ref}");
    let public_url = public_base_url();
    let url = format!("{public_url}/api/v1/actions/download/{owner}/{repo}/{git_ref}");
    Some((
        key,
        json!({
            "type": "Archive",
            "url": url,
            "authentication": null,
            "auth": null,
        }),
    ))
}

fn collect_runnerresolve_refs(
    value: &serde_json::Value,
    actions: &mut serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(raw) => {
            if let Some((key, action)) = runnerresolve_action(raw, None) {
                actions.entry(key).or_insert(action);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_runnerresolve_refs(item, actions);
            }
        }
        serde_json::Value::Object(map) => {
            let action = map
                .get("action")
                .or_else(|| map.get("name"))
                .or_else(|| map.get("nameWithOwner"))
                .or_else(|| map.get("repository"))
                .and_then(|v| v.as_str());
            let version = map
                .get("version")
                .or_else(|| map.get("ref"))
                .or_else(|| map.get("reference"))
                .and_then(|v| v.as_str());
            if let Some(action) = action {
                if let Some((key, value)) = runnerresolve_action(action, version) {
                    actions.entry(key).or_insert(value);
                }
            }

            for nested in map.values() {
                collect_runnerresolve_refs(nested, actions);
            }
        }
        _ => {}
    }
}

fn runnerresolve_action(
    action: &str,
    version_override: Option<&str>,
) -> Option<(String, serde_json::Value)> {
    let (key, ticket) = action_download_ticket(action, version_override)?;
    let (name, version) = key.split_once('@')?;
    let name = name.to_string();
    let version = version.to_string();
    let tar_url = ticket.get("url")?.as_str()?.to_string();
    Some((
        key,
        json!({
            "name": name,
            "version": version,
            // Local aksh does not pin refs yet; use the requested ref as the
            // extraction directory until a GitHub API lookup is added.
            "resolved_sha": version,
            "tar_url": tar_url,
            "authentication": null,
        }),
    ))
}

fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if statuses.iter().any(|status| {
        matches!(
            status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        )
    }) {
        ExecutionStatus::InProgress
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else {
        ExecutionStatus::Success
    }
}

async fn connection_data(
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if params.get("connectOptions").map(String::as_str) == Some("0")
        && params
            .get("lastChangeId")
            .is_some_and(|last_change_id| last_change_id != "-1")
    {
        return axum::response::Json(json!({
            "deploymentId": "00000000-0000-0000-0000-000000000000",
            "deploymentType": "hosted",
            "instanceId": uuid::Uuid::new_v4().to_string(),
            "locationServiceData": {
                "clientCacheFresh": true,
                "defaultAccessMappingMoniker": "ScaleUnitMapping",
                "lastChangeId": 1,
                "lastChangeId64": 1
            }
        }))
        .into_response();
    }

    let service_root = public_base_url();
    let runner_root = runner_server_url();
    let body = serde_json::json!({
        "deploymentId": "00000000-0000-0000-0000-000000000000",
        "deploymentType": "hosted",
        "instanceId": uuid::Uuid::new_v4().to_string(),
        "serverUrlV2": runner_root,
        "brokerUrl": public_base_url(),
        "resultsServiceUrl": runner_root,
        "locationServiceData": {
            "lastChangeId": 1,
            "lastChangeId64": 1,
            "clientCacheFresh": true,
            "serviceOwner": "00000000-0000-0000-0000-000000000000",
            "serviceDefinitions": [
                area_svc("Location Service", "9f1fe989-7d0d-4a9b-a9bf-11330ab257c1", "LocationService2", "Framework", &service_root),
                area_svc("distributedtask", "a85b8835-c1a1-4aac-ae97-1c3d0ba72dbd", "LocationService2", "Framework", &runner_root),
                area_svc("pipelines", "2e0bf237-8973-4ec9-a581-9c3d679d1776", "LocationService2", "Framework", &service_root),
                area_svc("oauth2", "a7b3b527-4f4f-4dac-8e84-f144fa6d554b", "LocationService2", "Framework", &runner_root),
                svc("AgentPools", "a8c47e17-4d56-4a56-92bb-de7ea7dc65be", "/_apis/v1/AgentPools"),
                svc("Agent", "e298ef32-5878-4cab-993c-043836571f42", "/_apis/v1/Agent/{poolId}/{agentId}"),
                svc("AgentSession", "134e239e-2df3-4794-a6f6-24f1f19ec8dc", "/_apis/v1/AgentSession/{poolId}/{sessionId}"),
                svc("Message", "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7", "/_apis/v1/Message/{poolId}/{messageId}"),
                svc("AgentRequest", "fc825784-c92a-4299-9221-998a02d1b54f", "/_apis/v1/AgentRequest/{poolId}/{requestId}"),
                svc("ActionDownloadInfo", "27d7f831-88c1-4719-8ca1-6a061dad90eb", "/_apis/v1/ActionDownloadInfo/{scopeIdentifier}/{hubName}/{planId}"),
                svc("TimeLineWebConsoleLog", "858983e4-19bd-4c5e-864c-507b59b58b12", "/_apis/v1/TimeLineWebConsoleLog/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/{recordId}"),
                svc("TimelineRecords", "8893bc5b-35b2-4be7-83cb-99e683551db4", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}"),
                svc("Logfiles", "46f5667d-263a-4684-91b1-dff7fdcf64e2", "/_apis/v1/Logfiles/{scopeIdentifier}/{hubName}/{planId}/{logId}"),
                svc("FinishJob", "557624af-b29e-4c20-8ab0-0399d2204f3f", "/_apis/v1/FinishJob/{scopeIdentifier}/{hubName}/{planId}"),
                svc("Artifact", "85023071-bd5e-4438-89b0-2a5bf362a19d", "/_apis/pipelines/workflows/{runId}/artifacts"),
                svc("ArtifactFileContainer", "e4f5c81e-e250-447b-9fef-bd48471bea5e", "/_apis/pipelines/workflows/container/{containerId}"),
                svc("TimelineAttachments", "7898f959-9cdf-4096-b29e-7f293031629e", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/attachments/{recordId}/{type}/{name}"),
                svc("Timeline", "83597576-cc2c-453c-bea6-2882ae6a1653", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/timeline/{timelineId}"),
                svc("CustomerIntelligence", "b5cc35c2-ff2b-491d-a085-24b6e9f396fd", "/_apis/v1/tasks"),
                svc("Tasks", "60aac929-f0cd-4bc8-9ce4-6b30e8f1b1bd", "/_apis/v1/tasks/{taskId}/{versionString}"),
                svc("Cache", "a7c78d38-31a8-417e-ba6b-7e58b352f304", "_apis/artifactcache"),
                svc("BuildArtifacts", "1db06c96-014e-44e1-ac91-90b2d4b3e984", "_apis/pipelines/workflows/{buildId}/artifacts"),
                resource_svc("brokerlistener", "38f00041-0953-4d24-86c3-5432d23e2205", "distributedtask", "_apis/{area}/{resource}"),
                resource_svc("createdsession", "a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f", "distributedtask", "_apis/{area}/brokerlistener/{resource}"),
                resource_svc("runnermessages", "25adab70-1379-4186-be8e-b643061ebe3a", "distributedtask", "_apis/{area}/{resource}/{messageId}"),
                resource_svc("runnerconfigrefresh", "13b5d709-74aa-470b-a8e9-bf9f3ded3f18", "distributedtask", "_apis/{area}/agents/{agentId}/{resource}/{configType}"),
                resource_svc("token", "10d13a60-2758-406c-8ab7-cffccb21fcf4", "oauth2", "_apis/{area}/{resource}"),
                resource_svc("steps", "99ea91b7-bbe9-4bd3-a924-874f13205b21", "pipelines", "_apis/{area}/plans/{planId}/jobs/{jobId}/{resource}"),
                resource_svc("jobs", "4818972d-29fa-4b86-92c1-de5ae7ef33f5", "pipelines", "_apis/{area}/plans/{planId}/{resource}/{jobId}"),
                resource_svc("logs", "fb1b6d27-3957-43d5-a14b-a2d70403e545", "pipelines", "{project}/_apis/{area}/{pipelineId}/runs/{runId}/{resource}/{logId}"),
            ],
            "accessMappings": [
                {
                    "moniker": "PublicAccessMapping",
                    "displayName": "Public Access Mapping",
                    "accessPoint": service_root,
                    "serviceOwner": "00000000-0000-0000-0000-000000000000",
                    "virtualDirectory": ""
                },
                {
                    "moniker": "ScaleUnitMapping",
                    "displayName": "Scale Unit Access Mapping",
                    "accessPoint": runner_root,
                    "serviceOwner": "00000000-0000-0000-0000-000000000000",
                    "virtualDirectory": ""
                }
            ],
            "defaultAccessMappingMoniker": "ScaleUnitMapping",
            "clientCacheFresh": true,
            "serviceOwner": "00000000-0000-0000-0000-000000000000"
        }
    });
    axum::response::Json(body).into_response()
}

fn area_svc(
    display_name: &str,
    id: &str,
    service_type: &str,
    tool_id: &str,
    location: &str,
) -> serde_json::Value {
    serde_json::json!({
        "serviceType": service_type,
        "identifier": id,
        "displayName": display_name,
        "description": display_name,
        "toolId": tool_id,
        "relativeToSetting": "fullyQualified",
        "locationMappings": [
            {"accessMappingMoniker": "PublicAccessMapping", "location": location},
            {"accessMappingMoniker": "ScaleUnitMapping", "location": location}
        ],
        "serviceOwner": "00000000-0000-0000-0000-000000000000",
        "properties": {}
    })
}

fn resource_svc(name: &str, id: &str, area: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "serviceType": area,
        "identifier": id,
        "displayName": name,
        "relativePath": location,
        "description": name,
        "toolId": area,
        "locationMappings": [],
        "serviceOwner": "00000000-0000-0000-0000-000000000000",
        "resourceVersion": 1,
        "minVersion": "1.0",
        "maxVersion": "6.0",
        "releasedVersion": "0.0",
        "status": 1,
        "properties": {}
    })
}

fn svc(name: &str, id: &str, location: &str) -> serde_json::Value {
    serde_json::json!({
        "serviceType": name,
        "identifier": id,
        "displayName": name,
        "relativePath": location,
        "relativeToSetting": 2,
        "description": name,
        "toolId": name,
        "locationMappings": [
            {"accessMappingMoniker": "ScaleUnitMapping", "location": runner_server_url()},
            {"accessMappingMoniker": "PublicAccessMapping", "location": public_base_url()}
        ],
        "serviceOwner": "00000000-0000-0000-0000-000000000000",
        "resourceVersion": 6,
        "minVersion": "1.0",
        "maxVersion": "12.0",
        "status": 1,
        "properties": {}
    })
}

fn rsa_public_key_xml_from_value(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    let modulus = value.get("modulus").and_then(|v| v.as_str())?;
    let exponent = value.get("exponent").and_then(|v| v.as_str())?;
    Some(format!(
        "<RSAKeyValue><Modulus>{modulus}</Modulus><Exponent>{exponent}</Exponent></RSAKeyValue>"
    ))
}

fn task_agent_public_key(request: &serde_json::Value) -> Option<String> {
    request
        .get("authorization")
        .and_then(|authorization| authorization.get("publicKey"))
        .and_then(rsa_public_key_xml_from_value)
        .or_else(|| {
            request
                .get("publicKey")
                .and_then(rsa_public_key_xml_from_value)
        })
}

/// GET /_apis/v1/Agent/:pool_id — look up runner by agentName query param.
/// Returns 200 with the agent if found, or 200 with an empty array if not found.
/// The runner treats a non-empty result as "agent exists" and empty as "needs registration".
async fn agent_lookup(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let agent_name = params.get("agentName").cloned().unwrap_or_default();
    let inner = shared.state.inner.lock().await;
    for runner in inner.runners.values() {
        if runner.name == agent_name {
            return Json(json!({"count": 1, "value": [{
                "id": runner.id,
                "name": runner.name,
                "version": "2.322.0",
                "osDescription": "Linux",
                "enabled": true,
                "status": "online",
                "labels": runner.labels.iter().map(|l| json!({"name": l, "type": "user"})).collect::<Vec<_>>()
            }]}));
        }
    }
    // Return empty collection (not 404) — runner expects VssJsonCollectionWrapper format
    Json(json!({"count": 0, "value": []}))
}

/// GET /_apis/v1/Agent/:pool_id/:agent_id — look up runner by agentId in path.
/// The runner constructs URLs from the service definition template `{poolId}/{agentId}`.
/// For lookups it uses agentId=0; for registration it POSTs.
async fn agent_lookup_by_id(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup(State(shared), Path(_pool_id), Query(params)).await
}

async fn runner_pools() -> Json<serde_json::Value> {
    Json(json!({
        "count": 1,
        "value": [{"id": 1, "name": "Default", "isHosted": false, "poolType": 1}]
    }))
}

/// Compat handler: register runner via AzDO Agent path.
async fn register_runner_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _agent_id)): Path<(i64, String)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends a TaskAgent-style body; extract what we need.
    let name = request
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("runner")
        .to_owned();
    let labels: Vec<String> = request
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    v.as_str()
                        .or_else(|| v.get("name").and_then(|name| name.as_str()))
                        .map(str::to_owned)
                })
                .collect()
        })
        .unwrap_or_default();
    let ephemeral = request
        .get("ephemeral")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let public_key_xml = task_agent_public_key(&request);
    let public_key_object = request
        .get("authorization")
        .and_then(|authorization| authorization.get("publicKey"))
        .cloned()
        .or_else(|| request.get("publicKey").cloned())
        .unwrap_or_else(|| {
            json!({
                "exponent": "AQAB",
                "modulus": ""
            })
        });
    let reg_request = RunnerRegistrationRequest {
        name: name.clone(),
        labels,
        ephemeral,
        public_key: public_key_xml,
    };
    let result = register_runner(State(shared.clone()), Json(reg_request)).await?;
    let client_id = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .runner_client_ids
            .insert(client_id.clone(), result.0.id);
    }
    Ok(Json(json!({
        "id": result.0.id,
        "name": result.0.name,
        "version": request.get("version").and_then(|v| v.as_str()).unwrap_or("2.335.1"),
        "osDescription": request.get("osDescription").and_then(|v| v.as_str()).unwrap_or("Linux"),
        "enabled": true,
        "status": "offline",
        "ephemeral": ephemeral,
        "maxParallelism": 1,
        "currentParallelism": 0,
        "disableUpdate": false,
        "isElastic": false,
        "isVirtual": false,
        "provisioningState": "Provisioned",
        "queueName": format!("taskagent-{}", result.0.id),
        "runnerGroupId": 1,
        "runnerGroupName": null,
        "labels": result.0.labels.iter().map(|l| json!({"name": l, "type": "user"})).collect::<Vec<_>>(),
        "authorization": {
            "authorizationUrl": format!("{}/_apis/v1/oauth2/token", runner_server_url()),
            "clientId": client_id,
            "publicKey": public_key_object
        },
        "properties": {
            "RequireFipsCryptography": {"$type": "System.Boolean", "$value": true},
            "ServerUrl": {"$type": "System.String", "$value": runner_server_url()},
            "ServerUrlV2": {"$type": "System.String", "$value": runner_server_url()},
            "UseV2Flow": {"$type": "System.Boolean", "$value": true}
        }
    })))
}

/// Compat handler: register runner via `/_apis/v1/Agent/:pool_id` (no agent_id in path).
async fn register_runner_compat_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat(
        State(shared),
        Path((_pool_id, "0".to_owned())),
        Json(request),
    )
    .await
}

/// Compat handler: create session via AzDO AgentSession path.
async fn create_session_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _session_id)): Path<(i64, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = body
        .get("agent")
        .and_then(|a| a.get("id"))
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    let name = body
        .get("agent")
        .and_then(|a| a.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("runner")
        .to_owned();
    let result = create_session(
        State(shared),
        Json(RunnerSessionRequest { runner_id, name }),
    )
    .await?;
    Ok(result)
}

/// Compat handler: next message via AzDO Message path.
async fn next_message_compat(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Option<azdo::TaskAgentMessage>>, ApiError> {
    next_message(State(shared), Query(params)).await
}

// ─── GHES org-prefixed wrapper handlers ─────────────────────────────────────
// These extract the extra `:org` path parameter and delegate to the real handlers.

async fn agent_lookup_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup(State(shared), Path(pool_id), Query(params)).await
}

async fn agent_lookup_by_id_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, agent_id)): Path<(String, i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup_by_id(State(shared), Path((pool_id, agent_id)), Query(params)).await
}

async fn register_runner_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat_pool_only(State(shared), Path(pool_id), Json(request)).await
}

async fn register_runner_compat_org_2(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, agent_id)): Path<(String, i64, String)>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat(State(shared), Path((pool_id, agent_id)), Json(request)).await
}

async fn create_session_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, session_id)): Path<(String, i64, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    create_session_compat(State(shared), Path((pool_id, session_id)), Json(body)).await
}

/// Session creation with only pool_id in path (no session_id — server generates it).
async fn create_session_compat_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Generate a session_id since the runner doesn't provide one
    let session_id = uuid::Uuid::new_v4().to_string();
    create_session_compat(State(shared), Path((_pool_id, session_id)), Json(body)).await
}

/// Org-prefixed session creation with only pool_id in path.
async fn create_session_compat_org_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    create_session_compat_pool_only(State(shared), Path(pool_id), Json(body)).await
}
async fn delete_session_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, session_id)): Path<(String, i64, String)>,
) -> StatusCode {
    delete_session(State(shared), Path((pool_id, session_id))).await
}

async fn next_message_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Option<azdo::TaskAgentMessage>>, ApiError> {
    next_message_compat(State(shared), Path(pool_id), Query(params)).await
}

async fn delete_pool_message_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, message_id)): Path<(String, i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    delete_pool_message(State(shared), Path((pool_id, message_id)), Query(params)).await
}

async fn agent_request_get_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    agent_request_get(State(shared), Path((pool_id, request_id))).await
}

async fn agent_request_ack_org(
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
) -> StatusCode {
    agent_request_ack(Path((pool_id, request_id))).await
}

#[allow(dead_code)]
async fn complete_job_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, run_id, job_id)): Path<(String, RunId, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<RunRecord>, ApiError> {
    complete_job_compat(State(shared), Path((run_id, job_id)), Json(body)).await
}

async fn agent_request_patch_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    agent_request_patch(State(shared), Path((pool_id, request_id)), Json(body)).await
}

async fn patch_timeline_records_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id)): Path<(String, String, String, String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    patch_timeline_records(
        State(shared),
        Path((scope, hub, plan_id, timeline_id)),
        Json(wrapper),
    )
    .await
}

async fn create_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id)): Path<(String, String, String, String)>,
    Json(log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    create_log(State(shared), Path((scope, hub, plan_id)), Json(log)).await
}

async fn append_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, log_id)): Path<(String, String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    append_log(State(shared), Path((scope, hub, plan_id, log_id)), body).await
}

async fn console_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id, record_id)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
    body: Bytes,
) -> StatusCode {
    console_log(
        State(shared),
        Path((scope, hub, plan_id, timeline_id, record_id)),
        body,
    )
    .await
}

async fn finish_job_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id)): Path<(String, String, String, String)>,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Json<serde_json::Value> {
    finish_job(State(shared), Path((scope, hub, plan_id)), Json(event)).await
}

async fn action_download_info_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, _scope, _hub, _plan_id)): Path<(String, String, String, String)>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    action_download_info(State(shared), Json(request)).await
}

/// GitHub-compatible runner registration token endpoint.
/// The official `actions/runner` config.sh calls this to get a registration token.
/// Matches the ChristopherHX/runner.server format: `GitHubAuthResult` with
/// `token`, `token_schema`, and `tenant_url`.
async fn github_registration_token(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends `Authorization: RemoteAuth <token>`
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !auth.starts_with("RemoteAuth ") && !auth.starts_with("Bearer ") {
        return Err(ApiError::unauthorized("missing Authorization header"));
    }

    let token = shared.state.local_jwt(json!({
        "sub": "aksh-runner-registration",
        "scp": "ActionsRuntime.RunnerManage Framework.GenericRead Identity.ReadRefs LocationService.Connect",
        "jti": uuid::Uuid::new_v4().to_string()
    }))?;
    let _requested_url = payload
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1")
        .to_owned();
    Ok(Json(json!({
        "token": token,
        "token_schema": "OAuthAccessToken",
        "url": runner_server_url()
    })))
}

#[derive(Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct FormOAuth2Request {
    client_assertion_type: Option<String>,
    client_assertion: Option<String>,
    grant_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JsonOAuth2Request {
    grant_type: String,
    client_id: String,
    client_secret: String,
}

fn decode_jwt_segment(segment: &str) -> Option<serde_json::Value> {
    let bytes = BASE64_STANDARD
        .decode(segment.as_bytes())
        .or_else(|_| URL_SAFE_NO_PAD.decode(segment.as_bytes()))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Token TTL in seconds. Override with AKSH_TOKEN_TTL_SECS for testing
/// short-lived tokens (e.g. =1 triggers RLIS-02 proactive refresh immediately).
fn token_ttl_secs() -> u64 {
    std::env::var("AKSH_TOKEN_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2999)
}

async fn oauth2_token(
    State(shared): State<Arc<SharedState>>,
    _headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> Result<Json<TokenResponse>, ApiError> {
    // Try JSON first (mock flow from existing tests)
    if let Ok(req) = serde_json::from_slice::<JsonOAuth2Request>(&body) {
        let token = shared.state.local_jwt(json!({
            "sub": format!("aksh-runner-listen-mock-{}", req.client_id),
            "scp": "ActionsRuntime.RunnerListen Framework.GenericRead Identity.ReadRefs LocationService.Connect",
            "jti": uuid::Uuid::new_v4().to_string()
        }))?;
        return Ok(Json(TokenResponse {
            access_token: token,
            token_type: "JWT".to_owned(),
            expires_in: token_ttl_secs(),
        }));
    }

    // Try urlencoded form (production runner flow with client assertion)
    let form: FormOAuth2Request = serde_urlencoded::from_bytes(&body)
        .map_err(|e| ApiError::bad_request(format!("invalid urlencoded OAuth body: {e}")))?;

    let assertion = form
        .client_assertion
        .ok_or_else(|| ApiError::bad_request("missing client_assertion in OAuth request"))?;

    // Parse the client_assertion JWT (header.payload.signature)
    let parts: Vec<&str> = assertion.split('.').collect();
    if parts.len() != 3 {
        return Err(ApiError::bad_request(
            "invalid JWT format in client_assertion",
        ));
    }

    let _header_val = decode_jwt_segment(parts[0])
        .ok_or_else(|| ApiError::bad_request("failed to decode JWT header"))?;
    let _claims_val = decode_jwt_segment(parts[1])
        .ok_or_else(|| ApiError::bad_request("failed to decode JWT claims"))?;

    let client_id = _claims_val
        .get("sub")
        .or_else(|| _claims_val.get("iss"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::bad_request("client_assertion claims missing sub/iss"))?;

    let signature = URL_SAFE_NO_PAD
        .decode(parts[2].as_bytes())
        .map_err(|e| ApiError::bad_request(format!("invalid JWT signature encoding: {e}")))?;

    let signing_input = format!("{}.{}", parts[0], parts[1]);

    // Look up the runner and its public key
    let (runner_id, pubkey) = {
        let inner = shared.state.inner.lock().await;
        let id = inner
            .runner_client_ids
            .get(client_id)
            .copied()
            .ok_or_else(|| {
                ApiError::unauthorized(format!("client ID not registered: {client_id}"))
            })?;
        let pubkey = inner
            .runner_rsa_public_keys
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                ApiError::unauthorized(format!("runner {id} missing registered public key"))
            })?;
        (id, pubkey)
    };

    // Verify signature
    pubkey
        .verify_signature_ps256(signing_input.as_bytes(), &signature)
        .map_err(|e| ApiError::unauthorized(format!("JWT signature verification failed: {e}")))?;

    let token = shared.state.local_jwt(json!({
        "sub": format!("aksh-runner-listen-{runner_id}"),
        "scp": "ActionsRuntime.RunnerListen Framework.GenericRead Identity.ReadRefs LocationService.Connect",
        "jti": uuid::Uuid::new_v4().to_string()
    }))?;

    Ok(Json(TokenResponse {
        access_token: token,
        token_type: "JWT".to_owned(),
        expires_in: token_ttl_secs(),
    }))
}

#[derive(Debug, Deserialize)]
struct OidcTokenQuery {
    audience: Option<String>,
    #[serde(rename = "api-version")]
    _api_version: Option<String>,
}

#[derive(Debug, Serialize)]
struct OidcTokenResponse {
    value: String,
}

/// `GET /runner/server/_apis/distributedtask/hubs/actions/plans/:plan_id/jobs/:job_id/oidctoken`
///
/// Mints a GitHub-compatible RS256-signed OIDC id-token JWT. Looks up the
/// originating workflow run to populate claims, and enforces `id-token: write`.
async fn oidc_token_run_service(
    State(shared): State<Arc<SharedState>>,
    Path((_orchestration_id, plan_id, job_id)): Path<(String, String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    oidc_token(
        State(shared),
        Path((plan_id, job_id)),
        Query(query),
        headers,
    )
    .await
}

async fn oidc_token(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, job_id)): Path<(String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("OIDC bearer token required"))?;
    let expected_scope = format!("Actions.Results:{plan_id}:{job_id}");
    if !shared.state.verify_local_jwt_scope(bearer, &expected_scope) {
        return Err(ApiError::forbidden(
            "OIDC runtime token is not bound to this job",
        ));
    }
    let inner = shared.state.inner.lock().await;
    let request_id = inner
        .plan_requests
        .get(&plan_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("OIDC: plan not found"))?;
    let request = inner
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("OIDC: job request not found"))?;
    if request.agent_job_id.to_string() != job_id {
        return Err(ApiError::not_found("OIDC: plan and job do not match"));
    }
    let run_id = request.run_id;
    let resolved_job_id = request.job_id.clone();

    // Permission enforcement: id-token:write must be granted.
    let granted = inner
        .id_token_grants
        .get(&(run_id, resolved_job_id.clone()))
        .copied()
        .unwrap_or(false);
    if !granted {
        return Err(ApiError::forbidden(
            "id-token: write permission is required to request an OIDC token",
        ));
    }

    // Get the OIDC signing keypair.
    let oidc_kp = inner
        .oidc_keypair
        .as_ref()
        .ok_or_else(|| ApiError::internal("OIDC signing keypair not available"))?
        .clone();

    let oidc_context = inner
        .oidc_job_contexts
        .get(&(run_id, resolved_job_id.clone()))
        .cloned()
        .ok_or_else(|| ApiError::internal("OIDC context missing for dispatched job"))?;

    // Build claims from the run's submission and parser-resolved job context.
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("OIDC: run not found"))?;
    let submission = &run.submission;
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();

    // Use sha from submission first-class field, fallback to payload extraction.
    let sha = if submission.sha != "0000000000000000000000000000000000000000" {
        submission.sha.clone()
    } else {
        submission
            .payload
            .get("after")
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("head"))
                    .and_then(|h| h.get("sha"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("0000000000000000000000000000000000000000")
            .to_string()
    };

    // Use actor from submission first-class field, fallback to payload extraction.
    let actor = if submission.actor != "aksh-system" {
        submission.actor.clone()
    } else {
        submission
            .payload
            .get("pusher")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("sender")
                    .and_then(|s| s.get("login"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("aksh-system")
            .to_string()
    };

    // Extract actor_id from payload if available.
    let actor_id = submission
        .payload
        .get("sender")
        .and_then(|s| s.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();

    // Extract repository_id and repository_owner_id from payload.
    let repository_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_owner_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("owner"))
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_visibility = submission
        .payload
        .get("repository")
        .and_then(|repository| repository.get("visibility"))
        .and_then(|value| value.as_str())
        .unwrap_or("private")
        .to_owned();

    let workflow_name = parse_workflow(&submission.workflow_yaml)
        .ok()
        .and_then(|w| w.name)
        .unwrap_or_default();

    // Derive the workflow filename: explicit > parsed from YAML > "workflow.yml"
    let workflow_file = submission
        .workflow_file
        .clone()
        .unwrap_or_else(|| "workflow.yml".to_owned());

    let head_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("head"))
        .and_then(|h| h.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let base_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|b| b.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let check_run_id = run.job_check_run_ids.get(&resolved_job_id).copied();
    let workflow_ref = format!(
        "{}/.github/workflows/{}@{}",
        submission.repository, workflow_file, submission.git_ref
    );

    let job_workflow_ref = oidc_context.job_workflow_ref.as_deref().map(|reference| {
        format_reusable_workflow_ref(&submission.repository, reference, &submission.git_ref)
    });
    let job_workflow_sha = job_workflow_ref
        .as_ref()
        .and_then(|reference| {
            reference
                .rsplit_once('@')
                .map(|(_, git_ref)| git_ref)
                .filter(|git_ref| {
                    git_ref.len() == 40
                        && git_ref
                            .chars()
                            .all(|character| character.is_ascii_hexdigit())
                })
                .map(str::to_owned)
        })
        .or_else(|| job_workflow_ref.as_ref().map(|_| sha.clone()));

    let claims_input = oidc::OidcClaimsInput {
        repository: submission.repository.clone(),
        repository_owner,
        git_ref: submission.git_ref.clone(),
        event_name: submission.event.clone(),
        sha: sha.clone(),
        actor,
        actor_id,
        workflow: workflow_name,
        run_id: run_id.to_string(),
        run_number: "1".to_string(),
        run_attempt: "1".to_string(),
        head_ref,
        base_ref,
        environment: oidc_context.environment,
        repository_visibility,
        repository_id,
        repository_owner_id,
        workflow_ref: Some(workflow_ref),
        workflow_sha: Some(sha),
        job_workflow_ref,
        job_workflow_sha,
    };

    let audience = query
        .audience
        .unwrap_or_else(|| oidc::default_audience(&claims_input.repository_owner));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
        .as_secs();

    let issuer = oidc_issuer_url(&inner);
    drop(inner);
    let mut claims = oidc::build_claims(&claims_input, &audience, &issuer, now);
    if let Some(check_run_id) = check_run_id {
        claims["check_run_id"] = json!(check_run_id.to_string());
    }

    let jwt = oidc_kp
        .sign_jwt(&claims)
        .map_err(|e| ApiError::internal(format!("OIDC token signing failed: {e}")))?;

    Ok(Json(OidcTokenResponse { value: jwt }))
}

/// `GET /.well-known/openid-configuration` — OIDC discovery document.
async fn oidc_discovery(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let issuer = oidc_issuer_url(&inner);
    let jwks_uri = format!("{issuer}/.well-known/jwks.json");
    Ok(Json(oidc::discovery_document(&issuer, &jwks_uri)))
}

/// `GET /.well-known/jwks.json` — JSON Web Key Set for OIDC token verification.
async fn oidc_jwks(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let kp = inner
        .oidc_keypair
        .as_ref()
        .ok_or_else(|| ApiError::internal("OIDC keypair not available"))?;
    Ok(Json(kp.jwks()))
}

fn base64_url_json(value: &serde_json::Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ApiError::bad_request(format!("failed to encode jwt json: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

async fn cache_put(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CachePutRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let bytes = decode_base64(&request.content_base64)?;
    let entry = shared
        .state
        .cache
        .put(&request.key, &request.version, &bytes)
        .await?;
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: None,
    }))
}

async fn cache_get(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<CacheQuery>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let key = query.key.unwrap_or_default();
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    let Some((entry, bytes)) = shared
        .state
        .cache
        .get(&key, &query.version, &restore_keys)
        .await?
    else {
        return Ok(Json(CacheLookupResponse {
            hit: false,
            key: None,
            version: None,
            size: None,
            content_base64: None,
        }));
    };
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: Some(BASE64_STANDARD.encode(bytes)),
    }))
}

async fn cache_reserve(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheReserveRequest>,
) -> Json<CacheReserveResponse> {
    let mut inner = shared.state.inner.lock().await;
    inner.next_cache_id += 1;
    let cache_id = inner.next_cache_id;
    inner.pending_caches.insert(
        cache_id,
        PendingCache {
            key: request.key,
            version: request.version,
            bytes: Vec::new(),
        },
    );
    Json(CacheReserveResponse { cache_id })
}

async fn cache_upload(
    State(shared): State<Arc<SharedState>>,
    Path(cache_id): Path<i64>,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let pending = inner
        .pending_caches
        .get_mut(&cache_id)
        .ok_or_else(|| ApiError::not_found("cache reservation not found"))?;
    pending.bytes.extend_from_slice(&bytes);
    Ok(StatusCode::ACCEPTED)
}

async fn cache_commit(
    State(shared): State<Arc<SharedState>>,
    Path(cache_id): Path<i64>,
    Json(request): Json<CacheCommitRequest>,
) -> Result<Json<CacheLookupResponse>, ApiError> {
    let pending = {
        let mut inner = shared.state.inner.lock().await;
        inner
            .pending_caches
            .remove(&cache_id)
            .ok_or_else(|| ApiError::not_found("cache reservation not found"))?
    };
    if let Some(size) = request.size {
        let actual = pending.bytes.len() as u64;
        if size != actual {
            return Err(ApiError::bad_request(format!(
                "cache size mismatch: expected {size}, got {actual}"
            )));
        }
    }
    let entry = shared
        .state
        .cache
        .put(&pending.key, &pending.version, &pending.bytes)
        .await?;
    Ok(Json(CacheLookupResponse {
        hit: true,
        key: Some(entry.key),
        version: Some(entry.version),
        size: Some(entry.size),
        content_base64: None,
    }))
}

async fn cache_lookup(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<CacheQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let key = query.key.unwrap_or_default();
    let restore_keys = parse_restore_keys(query.keys.as_deref());
    let response = shared
        .state
        .cache
        .get(&key, &query.version, &restore_keys)
        .await?;
    if let Some((entry, _bytes)) = response {
        Ok(Json(json!({
            "cacheKey": entry.key,
            "scope": "aksh",
            "archiveLocation": format!("/api/v1/cache?key={}&version={}", key, query.version),
        })))
    } else {
        Ok(Json(json!({})))
    }
}

async fn artifact_put(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactPutRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    let bytes = decode_base64(&request.content_base64)?;
    put_artifact(
        shared,
        request.run_id,
        request.name,
        request.file_name,
        bytes,
    )
    .await
}

async fn artifact_create(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Json(request): Json<ArtifactCreateRequest>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    put_artifact(shared, run_id, request.name, request.file_name, Vec::new()).await
}

async fn put_artifact(
    shared: Arc<SharedState>,
    run_id: RunId,
    name: String,
    file_name: String,
    bytes: Vec<u8>,
) -> Result<Json<ArtifactRecord>, ApiError> {
    let artifact = shared
        .state
        .artifacts
        .put(run_id, &name, &file_name, &bytes)
        .await?;
    let record = ArtifactRecord {
        id: artifact.id.to_string(),
        run_id,
        name,
        file_name,
        path: artifact.path.to_string_lossy().into_owned(),
        size: artifact.size,
    };
    let mut inner = shared.state.inner.lock().await;
    inner.artifacts.insert(record.id.clone(), record.clone());
    Ok(Json(record))
}

async fn artifact_get(
    State(shared): State<Arc<SharedState>>,
    Path(artifact_id): Path<String>,
) -> Result<Response, ApiError> {
    read_artifact(shared, artifact_id).await
}

async fn artifact_get_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_run_id, artifact_id)): Path<(RunId, String)>,
) -> Result<Response, ApiError> {
    read_artifact(shared, artifact_id).await
}

async fn read_artifact(
    shared: Arc<SharedState>,
    artifact_id: String,
) -> Result<Response, ApiError> {
    let record = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifacts
            .get(&artifact_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("artifact not found"))?
    };
    let bytes = tokio::fs::read(&record.path).await?;
    Ok(Response::builder()
        .header("content-type", "application/octet-stream")
        .body(Body::from(bytes))
        .expect("static response builder"))
}

async fn artifact_list(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Json<serde_json::Value> {
    let inner = shared.state.inner.lock().await;
    let value = inner
        .artifacts
        .values()
        .filter(|artifact| artifact.run_id == run_id)
        .collect::<Vec<_>>();
    Json(json!({
        "count": value.len(),
        "value": value,
    }))
}

fn parse_restore_keys(keys: Option<&str>) -> Vec<String> {
    keys.unwrap_or_default()
        .split(',')
        .filter(|key| !key.is_empty())
        .map(str::to_owned)
        .collect()
}

fn decode_base64(value: &str) -> Result<Vec<u8>, ApiError> {
    BASE64_STANDARD
        .decode(value)
        .map_err(|error| ApiError::bad_request(format!("invalid base64 content: {error}")))
}

async fn record_flows_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let has_file = {
        let inner = state.inner.lock().await;
        inner.flows_file.is_some()
    };

    if !has_file {
        return next.run(request).await;
    }

    let method = request.method().to_string();
    let uri = request.uri();
    let path = uri
        .path_and_query()
        .map(|pq| pq.to_string())
        .unwrap_or_else(|| uri.path().to_string());
    let scheme = uri.scheme_str().unwrap_or("http").to_string();
    let host = request
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost")
        .to_string();

    let mut request_headers = Vec::new();
    for (name, value) in request.headers() {
        if let Ok(val_str) = value.to_str() {
            request_headers.push(vec![name.to_string(), val_str.to_string()]);
        }
    }

    let ts_request = server_iso_now();
    let start_time = std::time::Instant::now();

    let (parts, body) = request.into_parts();
    let req_bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => Bytes::new(),
    };
    let request_body_b64 = BASE64_STANDARD.encode(&req_bytes);
    let request = Request::from_parts(parts, Body::from(req_bytes));

    let response = next.run(request).await;

    let duration_ms = start_time.elapsed().as_millis() as u64;
    let ts_response = server_iso_now();
    let status = response.status().as_u16();

    let mut response_headers = Vec::new();
    for (name, value) in response.headers() {
        if let Ok(val_str) = value.to_str() {
            response_headers.push(vec![name.to_string(), val_str.to_string()]);
        }
    }

    let (parts, body) = response.into_parts();
    let res_bytes = match to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => Bytes::new(),
    };
    let response_body_b64 = BASE64_STANDARD.encode(&res_bytes);
    let response = Response::from_parts(parts, Body::from(res_bytes));

    let mut inner = state.inner.lock().await;
    let mut file_opt = inner.flows_file.take();
    if let Some(file) = &mut file_opt {
        inner.next_flow_index += 1;
        let flow_index = inner.next_flow_index;
        let flow_record = json!({
            "flow_index": flow_index,
            "ts_request": ts_request,
            "ts_response": ts_response,
            "duration_ms": duration_ms,
            "method": method,
            "scheme": scheme,
            "host": host,
            "path": path,
            "request_headers": request_headers,
            "request_body_b64": request_body_b64,
            "status": status,
            "response_headers": response_headers,
            "response_body_b64": response_body_b64,
        });
        if let Ok(line) = serde_json::to_string(&flow_record) {
            use std::io::Write;
            let _ = writeln!(file, "{}", line);
            let _ = file.flush();
        }
    }
    inner.flows_file = file_opt;

    response
}

fn server_iso_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();

    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Fetch a remote reusable workflow YAML from GitHub.
/// `uses` format: `owner/repo/path/.github/workflows/workflow.yml@ref`
async fn fetch_remote_workflow(uses: &str) -> Result<String, anyhow::Error> {
    let parts: Vec<&str> = uses.split('@').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("invalid uses format: {uses}"));
    }
    let path_part = parts[0];
    let git_ref = parts[1];
    let segments: Vec<&str> = path_part.splitn(3, '/').collect();
    if segments.len() < 3 {
        return Err(anyhow::anyhow!("invalid uses path: {uses}"));
    }
    let owner = segments[0];
    let repo = segments[1];
    let path = segments[2];
    let url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        owner, repo, git_ref, path
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

/// Resolve a git ref (branch/tag) to a commit SHA via the GitHub API.
async fn resolve_remote_sha(owner: &str, repo: &str, git_ref: &str) -> Option<String> {
    let url = format!(
        "https://api.github.com/repos/{}/{}/git/ref/{}",
        owner, repo, git_ref
    );
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "aksh-runner-server")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        // Try tags endpoint if heads fails
        let url = format!(
            "https://api.github.com/repos/{}/{}/git/ref/tags/{}",
            owner, repo, git_ref
        );
        let resp = client
            .get(&url)
            .header("User-Agent", "aksh-runner-server")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        return json
            .get("object")
            .and_then(|o| o.get("sha"))
            .and_then(|s| s.as_str())
            .map(String::from);
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("object")
        .and_then(|o| o.get("sha"))
        .and_then(|s| s.as_str())
        .map(String::from)
}

async fn resolve_all_reusable_workflows(
    workflow: &aksh_gha_parser::Workflow,
    reusable_workflows: &mut BTreeMap<String, String>,
    reusable_shas: &mut BTreeMap<String, String>,
    depth: usize,
) -> Result<(), ApiError> {
    if depth >= 4 {
        return Ok(());
    }
    for job in workflow.jobs.values() {
        if let Some(uses) = &job.uses {
            if !uses.starts_with("./") && !uses.starts_with(".github/") {
                if !reusable_workflows.contains_key(uses) {
                    let text = fetch_remote_workflow(uses).await.map_err(|e| {
                        ApiError::bad_request(format!(
                            "failed to fetch remote workflow `{}`: {}",
                            uses, e
                        ))
                    })?;
                    reusable_workflows.insert(uses.clone(), text.clone());
                    if let Ok(called) = parse_workflow(&text) {
                        Box::pin(resolve_all_reusable_workflows(
                            &called,
                            reusable_workflows,
                            reusable_shas,
                            depth + 1,
                        ))
                        .await?;
                    }
                }
                if !reusable_shas.contains_key(uses) {
                    let parts: Vec<&str> = uses.split('@').collect();
                    if parts.len() == 2 {
                        let path_part = parts[0];
                        let git_ref = parts[1];
                        let path_segments: Vec<&str> = path_part.splitn(3, '/').collect();
                        if path_segments.len() == 3 {
                            let owner = path_segments[0];
                            let repo = path_segments[1];
                            if let Some(sha) = resolve_remote_sha(owner, repo, git_ref).await {
                                reusable_shas.insert(uses.clone(), sha);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn propagate_reusable_outputs(run: &mut RunRecord) {
    let mut outputs_to_add = Vec::new();
    for (caller_job_id, call) in &run.reusable_calls {
        let caller_job_id_typed = JobId(caller_job_id.clone());
        if run.job_outputs.contains_key(&caller_job_id_typed) {
            continue;
        }

        // Check if all inner jobs are complete
        let all_complete = !call.inner_job_ids.is_empty()
            && call.inner_job_ids.iter().all(|id| {
                run.jobs.get(&JobId(id.clone())).is_some_and(|status| {
                    matches!(
                        status,
                        ExecutionStatus::Success
                            | ExecutionStatus::Failure
                            | ExecutionStatus::Skipped
                            | ExecutionStatus::Cancelled
                    )
                })
            });

        if all_complete {
            // Build expression context
            let mut jobs_map = serde_json::Map::new();
            for inner_id in &call.inner_job_ids {
                let prefix = format!("{}/", caller_job_id);
                let inner_id_without_prefix = if inner_id.starts_with(&prefix) {
                    &inner_id[prefix.len()..]
                } else {
                    inner_id
                };

                let mut job_outputs_map = serde_json::Map::new();
                if let Some(outputs) = run.job_outputs.get(&JobId(inner_id.clone())) {
                    for (k, v) in outputs {
                        job_outputs_map.insert(k.clone(), v.clone());
                    }
                }

                let mut job_record = serde_json::Map::new();
                job_record.insert(
                    "outputs".to_owned(),
                    serde_json::Value::Object(job_outputs_map),
                );
                jobs_map.insert(
                    inner_id_without_prefix.to_owned(),
                    serde_json::Value::Object(job_record),
                );
            }

            let mut context = aksh_gha_expressions::Context::default();
            context.insert("jobs", serde_json::Value::Object(jobs_map));

            let mut inputs_map = serde_json::Map::new();
            for (k, v) in &call.inputs {
                inputs_map.insert(k.clone(), v.clone());
            }
            context.insert("inputs", serde_json::Value::Object(inputs_map));

            let mut caller_outputs = BTreeMap::new();
            for (name, expr) in &call.output_definitions {
                let resolved = aksh_gha_parser::eval::resolve_string(expr, &context)
                    .unwrap_or_else(|_| expr.clone());
                let val =
                    serde_json::from_str(&resolved).unwrap_or(serde_json::Value::String(resolved));
                caller_outputs.insert(name.clone(), val);
            }

            outputs_to_add.push((caller_job_id_typed, caller_outputs));
        }
    }

    for (job_id, outputs) in outputs_to_add {
        run.job_outputs.insert(job_id, outputs);
    }
}

#[cfg(test)]
/// Production-path DAG/workflow properties.
///
/// Oracle sources:
/// - `needs`, skipped dependencies, and job-level conditions:
///   <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
/// - status functions: <https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions>.
/// - runner v2.335.1: `src/Runner.Worker/StepsRunner.cs` and
///   `src/Runner.Worker/Expressions/{Success,Failure,Cancelled,Always}Function.cs`.
///
/// These tests submit YAML through the real parser/router and use only the
/// explicitly gated internal test API to simulate worker completions. The
/// oracle compares observable job/run state; it does not copy scheduler code.
#[path = "lib_tests.rs"]
mod tests;
