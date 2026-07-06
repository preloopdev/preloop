//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod github;

use aksh_artifacts::ArtifactStore;
use aksh_cache::CacheStore;
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
use axum::http::{header, StatusCode};
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

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind.
    pub listen: SocketAddr,
    /// State directory for cache/artifacts and future durable state.
    pub state_dir: PathBuf,
    /// Optional file path to write recorded flows to (NDJSON format).
    pub record_flows: Option<PathBuf>,
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
                    cancellations.push(QueuedCancellation {
                        run_id,
                        job_id: job_id.clone(),
                    });
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
    let state = AppState::new(config.state_dir).await?;
    if let Some(path) = &config.record_flows {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut inner = state.inner.lock().await;
        inner.flows_file = Some(file);
    }
    let shutdown = CancellationToken::new();
    let router = app(state.clone(), shutdown.clone());
    let listener = TcpListener::bind(config.listen).await?;

    let shared = Arc::new(SharedState {
        state,
        shutdown: shutdown.clone(),
    });

    let checker_shared = shared.clone();
    tokio::spawn(async move {
        run_background_reaper(checker_shared).await;
    });

    info!(listen = %config.listen, "aksh runner server listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await?;
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

/// Build the server router.
pub fn app(state: AppState, shutdown: CancellationToken) -> Router {
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
            get(next_message_broker_ref),
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
            "/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
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
        .route_layer(middleware::from_fn(require_bearer));

    Router::new()
        .route("/healthz", get(healthz))
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
            "/_apis/",
            axum::routing::options(|| async { StatusCode::OK }),
        )
        .route("/api/v1/runs", post(submit_run))
        .route(
            "/api/v1/github/webhooks",
            post(github::handle_github_webhook),
        )
        .route("/api/v1/github/register", get(github::github_register))
        .route("/api/v1/github/callback", get(github::github_callback))
        .route("/api/v1/runs/:run_id", get(get_run))
        .route("/api/v1/runs/:run_id/cancel", post(cancel_run))
        .route("/api/v1/runs/:run_id/rerun", post(rerun_run))
        .route("/api/v1/runs/:run_id/events.ndjson", get(run_events))
        .route("/api/v1/runners", post(register_runner))
        .route("/api/v1/runners/sessions", post(create_session))
        .route(
            "/api/v1/runners/sessions/:session_id/messages",
            get(next_message),
        )
        .route(
            "/api/v1/runners/sessions/:session_id/messages/:message_id",
            delete(delete_session_message),
        )
        .route(
            "/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route("/runner/message", get(next_message_broker_ref_root))
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
            get(next_message_broker_ref_root),
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
        .route("/message", get(next_message_broker_ref_root))
        .route("/acknowledge", post(broker_acknowledge_root))
        .route(
            "/runner/server/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/server/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route("/runner/server/message", get(next_message_broker_ref_root))
        .route("/runner/server/acknowledge", post(broker_acknowledge_root))
        .route("/api/v1/jobs/complete", post(complete_job))
        .route("/api/v1/cache", post(cache_put))
        .route("/api/v1/cache", get(cache_get))
        .route("/api/v1/artifacts", post(artifact_put))
        .route("/api/v1/artifacts/:artifact_id", get(artifact_get))
        // Runner lifecycle endpoints — public (runner may not have auth token yet)
        .route("/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id).post(register_runner_compat),
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
        .merge(protected_apis)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_flows_middleware,
        ))
        .with_state(shared)
}

/// HMAC key used for local JWT signing/verification.
const LOCAL_JWT_KEY: &[u8] = b"aksh-local-runner-signing-key";

async fn require_bearer(request: Request, next: Next) -> Result<Response, ApiError> {
    if request.uri().path().starts_with("/broker/") {
        return Ok(next.run(request).await);
    }
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| {
            token == "aksh-system-token" || token.starts_with("aksh-") || verify_local_jwt(token)
        });
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("missing or invalid bearer token"))
    }
}

/// Verify an HS256 JWT issued by this server's `local_jwt()`.
fn verify_local_jwt(token: &str) -> bool {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return false;
    }
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    let mut mac = match Hmac::<Sha256>::new_from_slice(LOCAL_JWT_KEY) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(signing_input.as_bytes());
    let expected = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    expected == parts[2]
}

#[derive(Clone)]
struct SharedState {
    state: AppState,
    shutdown: CancellationToken,
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
}

impl AppState {
    /// Build state rooted in a state directory.
    pub async fn new(state_dir: PathBuf) -> anyhow::Result<Self> {
        let cache = CacheStore::new(state_dir.join("cache")).await?;
        let artifacts = ArtifactStore::new(state_dir.join("artifacts")).await?;
        let (events, _) = broadcast::channel(1024);
        let keypair = AgentRsaKeypair::generate()
            .map_err(|e| anyhow::anyhow!("Failed to generate RSA keypair: {}", e))?;
        let inner = InnerState {
            agent_keypair: Some(keypair),
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
        })
    }

    async fn emit(&self, event: NdjsonEvent) {
        let _ = self.events.send(event);
    }
}

impl InnerState {
    /// Look up the labels for the runner that owns a given session.
    fn runner_labels_for_session(&self, session_id: &str) -> Vec<String> {
        self.sessions
            .get(session_id)
            .and_then(|s| self.runners.get(&s.runner_id))
            .map(|r| r.labels.clone())
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
    next_runner_id: i64,
    next_cache_id: i64,
    next_message_id: i64,
    next_log_id: usize,
    next_request_id: i64,
    flows_file: Option<std::fs::File>,
    next_flow_index: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RunRecord {
    pub(crate) run_id: RunId,
    pub(crate) submission: WorkflowSubmission,
    pub(crate) jobs: BTreeMap<JobId, ExecutionStatus>,
    pub(crate) status: ExecutionStatus,
    pub(crate) job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    pub(crate) job_base_ids: BTreeMap<JobId, String>,
    pub(crate) job_fail_fast: BTreeMap<String, bool>,
    #[serde(default)]
    pub(crate) job_check_run_ids: BTreeMap<JobId, u64>,
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
    fail_fast: bool,
    max_parallel: Option<u64>,
    /// Required runner labels from `runs-on`.
    runs_on: Vec<String>,
    message: azdo::AgentJobRequestMessage,
}

#[derive(Debug, Clone)]
struct QueuedCancellation {
    run_id: RunId,
    job_id: JobId,
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
    submission: WorkflowSubmission,
) -> Result<RunAccepted, ApiError> {
    let workflow = parse_workflow(&submission.workflow_yaml)?;
    let (branch, tag) = git_ref_context(&submission.git_ref);
    let changed_paths = changed_paths_from_payload(&submission.payload);
    let activity_type = submission
        .payload
        .get("action")
        .and_then(|value| value.as_str());
    if !workflow.on.matches_with_context(
        &submission.event,
        branch.as_deref(),
        tag.as_deref(),
        &changed_paths,
        activity_type,
    ) {
        return Err(ApiError::bad_request(format!(
            "workflow does not match event `{}`",
            submission.event
        )));
    }
    let jobs = expand_jobs_with_reusables(&workflow, &submission.reusable_workflows)?;
    let run_id = RunId::new();
    let github = json!({
        "event_name": submission.event,
        "event": submission.payload,
        "repository": submission.repository,
        "ref": submission.git_ref,
        "run_id": run_id.to_string(),
        "workflow": workflow.name.clone().unwrap_or_default(),
        "server_url": "http://localhost"
    });

    {
        let mut inner = shared.state.inner.lock().await;
        let mut statuses = BTreeMap::new();
        let mut ready_jobs = 0usize;
        let mut job_base_ids = BTreeMap::new();
        let mut job_fail_fast = BTreeMap::new();
        let mut ready_by_base: BTreeMap<String, u64> = BTreeMap::new();
        for job in jobs {
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

            // Give every dispatched job a unique requestId so PATCH
            // /AgentRequest/:request_id can target exactly one job.
            inner.next_request_id += 1;
            let request_id = inner.next_request_id;
            agent_msg.request_id = request_id;
            inner
                .inflight_requests
                .insert(request_id, (run_id, job.id.clone()));
            let job_request = TaskAgentJobRequestRecord {
                request_id,
                run_id,
                job_id: job.id.clone(),
                agent_job_id: agent_msg.job_id,
                plan_id: agent_msg.plan.plan_id.clone(),
                plan_type: agent_msg
                    .plan
                    .plan_type
                    .clone()
                    .unwrap_or_else(|| "Job".to_owned()),
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
                fail_fast: job.fail_fast,
                max_parallel: job.max_parallel,
                runs_on: job.runs_on.clone(),
                message: agent_msg,
            };
            job_base_ids.insert(job.id.clone(), job.base_id.clone());
            job_fail_fast.insert(job.base_id.clone(), job.fail_fast);

            // Check if dependencies are met (no needs = ready immediately)
            if job.needs.is_empty()
                && job
                    .max_parallel
                    .is_none_or(|max| ready_by_base.get(&job.base_id).copied().unwrap_or(0) < max)
            {
                statuses.insert(job.id.clone(), ExecutionStatus::Queued);
                inner.queue.push_back(queued_job);
                *ready_by_base.entry(job.base_id.clone()).or_default() += 1;
                ready_jobs += 1;
            } else {
                // Job has dependencies — queue it as pending
                statuses.insert(job.id.clone(), ExecutionStatus::Queued);
                inner.pending_jobs.push_back(queued_job);
            }
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
                job_fail_fast,
                status: ExecutionStatus::Queued,
                job_check_run_ids: BTreeMap::new(),
            },
        );
        drop(inner);
        if ready_jobs > 0 {
            shared.state.message_notify.notify_waiters();
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

async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    Json(submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    submit_run_inner(&shared, submission).await.map(Json)
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
    inner
        .runs
        .get(&run_id)
        .cloned()
        .map(Json)
        .ok_or_else(|| ApiError::not_found("run not found"))
}

async fn cancel_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let mut cancellations = Vec::new();
    {
        let record = inner
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        record.status = ExecutionStatus::Cancelled;
        for (job_id, status) in &mut record.jobs {
            if matches!(*status, ExecutionStatus::InProgress) {
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                });
            }
            if matches!(
                *status,
                ExecutionStatus::Queued | ExecutionStatus::InProgress
            ) {
                *status = ExecutionStatus::Cancelled;
            }
        }
    }
    inner.queue.retain(|job| job.run_id != run_id);
    inner.pending_jobs.retain(|job| job.run_id != run_id);
    let cancellation_count = cancellations.len();
    inner.cancellation_queue.extend(cancellations);
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
        })
        .await;
    Ok(Json(record))
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
    })?;
    for (job_id, status) in &run.jobs {
        out.push_str(&event_to_ndjson(&NdjsonEvent::JobStatus {
            run_id,
            job_id: job_id.clone(),
            status: *status,
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
        let lines_arc = inner
            .live_log_lines
            .entry(key.clone())
            .or_default()
            .clone();
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
    let response = create_session(
        State(shared),
        Json(RunnerSessionRequest { runner_id, name }),
    )
    .await?;
    let session_id = response["sessionId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let owner_name = body
        .get("ownerName")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id,
            "ownerName": owner_name,
            "assignmentQueued": false,
            "orchestrationId": ""
        })),
    ))
}

async fn delete_session(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    inner.sessions.remove(&session_id);
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
    plan_id: String,
    conclusion: Option<String>,
}

fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
    match result {
        "success" | "succeeded" | "succeededWithIssues" => Some(ExecutionStatus::Success),
        "failure" | "failed" => Some(ExecutionStatus::Failure),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
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
                        json!({
                            "runId": cancellation.run_id.to_string(),
                            "jobId": cancellation.job_id.to_string(),
                        })
                        .to_string(),
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
            })
            .await;

        return Ok(Json(broker_job_ref(&request, pool_id)).into_response());
    }
}

async fn broker_session_root(
    State(shared): State<Arc<SharedState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let session_id = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.clone(), SessionEncryption::generate());
    }
    (
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id,
            "ownerName": "aksh-runner",
            "assignmentQueued": false,
            "orchestrationId": ""
        })),
    )
}

async fn broker_delete_session_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    if let Some(session_id) = params.get("sessionId") {
        let mut inner = shared.state.inner.lock().await;
        inner.session_keys.remove(session_id);
        inner.session_active_requests.remove(session_id);
    }
    StatusCode::NO_CONTENT
}

async fn broker_delete_session_by_path(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    inner.session_keys.remove(&session_id);
    inner.session_active_requests.remove(&session_id);
    StatusCode::NO_CONTENT
}

async fn next_message_broker_ref_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());

    // Default to 50s long-poll (golden flows show ~50s waits between jobs)
    let wait = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);
    let deadline = std::time::Instant::now() + Duration::from_secs(wait);

    loop {
        let maybe = {
            let mut inner = shared.state.inner.lock().await;
            if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
                if let Some(request) = inner.job_requests.get(&request_id) {
                    if request.result.is_none() {
                        Some(broker_job_ref_root(request, 1))
                    } else {
                        inner.session_active_requests.remove(&session_id);
                        None
                    }
                } else {
                    inner.session_active_requests.remove(&session_id);
                    None
                }
            } else {
                let labels = inner.runner_labels_for_session(&session_id);
                if let Some(queued) = take_matching_job(&mut inner.queue, &labels) {
                    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
                        run.status = ExecutionStatus::InProgress;
                        run.jobs
                            .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
                    }
                    let request_id = queued.message.request_id;
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
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn broker_acknowledge_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    if !session_id.is_empty() {
        let mut inner = shared.state.inner.lock().await;
        inner.session_active_requests.remove(session_id);
    }
    StatusCode::OK
}

async fn broker_acquire_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    Json(request): Json<BrokerAcquireJobRequest>,
) -> Result<Json<azdo::AgentJobRequestMessage>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_message_id)
        .copied()
        .or_else(|| sole_active_unfinished_request(&inner))
        .ok_or_else(|| ApiError::not_found("broker job message not found"))?;
    let mut message = inner
        .broker_messages
        .get(&request_id)
        .cloned()
        .or_else(|| {
            inner.job_requests.get(&request_id).and_then(|record| {
                inner
                    .agent_job_requests
                    .get(&record.agent_job_id)
                    .and_then(|_| {
                        inner
                            .queue
                            .iter()
                            .find(|queued| queued.message.request_id == request_id)
                            .map(|queued| queued.message.clone())
                    })
            })
        })
        .ok_or_else(|| ApiError::not_found("broker job payload not found"))?;
    message.message_type = Some(azdo::message_type::RUNNER_JOB_REQUEST.to_owned());
    let run_service_url = broker_run_service_url(runner_id);
    for endpoint in &mut message.resources.endpoints {
        if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
            endpoint.url = Some(run_service_url.clone());
            endpoint
                .authorization
                .parameters
                .insert("AccessToken".to_owned(), "aksh-system-token".to_owned());
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
    Ok(Json(message))
}

async fn broker_renew_job(
    State(shared): State<Arc<SharedState>>,
    Path(_runner_id): Path<i64>,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_id)
        .copied()
        .or_else(|| inner.plan_requests.get(&request.plan_id).copied())
        .or_else(|| sole_active_unfinished_request(&inner))
        .ok_or_else(|| ApiError::not_found("broker renew request not found"))?;
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
    Path(_runner_id): Path<i64>,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<StatusCode, ApiError> {
    let status = request
        .conclusion
        .as_deref()
        .and_then(execution_status_from_runner_result)
        .unwrap_or(ExecutionStatus::Success);
    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let request_id = inner
            .agent_job_requests
            .get(&request.job_id)
            .copied()
            .or_else(|| inner.plan_requests.get(&request.plan_id).copied())
            .or_else(|| sole_active_unfinished_request(&inner))
            .ok_or_else(|| ApiError::not_found("broker complete request not found"))?;
        if let Some(record) = inner.job_requests.get_mut(&request_id) {
            record.result = Some(status);
            record.locked_until = agent_request_locked_until();
        }
        inner
            .inflight_requests
            .remove(&request_id)
            .or_else(|| {
                job_request_tuple(&inner, request_id).map(|(_, run_id, job_id)| (run_id, job_id))
            })
            .map(|(run_id, job_id)| JobCompletion {
                run_id,
                job_id,
                status,
                outputs: Default::default(),
            })
    };
    if let Some(completion) = completion {
        let _ = complete_job_inner(shared, completion).await?;
    }
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
    Json(_request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
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
            let body_json = json!({
                "runId": cancellation.run_id.to_string(),
                "jobId": cancellation.job_id.to_string(),
            })
            .to_string();
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

        let body_json = serde_json::to_string(&queued.message)
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
        iv: Some(iv),
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
        ExecutionStatus::Queued | ExecutionStatus::InProgress => "pending",
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
        Some(request_id)
    } else {
        None
    }
}

fn job_request_tuple(inner: &InnerState, request_id: i64) -> Option<(i64, RunId, JobId)> {
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    {
        let run = inner
            .runs
            .get_mut(&completion.run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        run.jobs
            .insert(completion.job_id.clone(), completion.status);
        run.job_outputs.insert(
            completion.job_id.clone(),
            completion
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        run.status = summarize_run(run.jobs.values().copied());
    }
    let cancelled_siblings = if completion.status == ExecutionStatus::Failure {
        apply_matrix_fail_fast(&mut inner, completion.run_id, &completion.job_id)
    } else {
        0
    };
    let promoted_jobs = promote_ready_jobs(&mut inner);
    let record = inner
        .runs
        .get(&completion.run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
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
    drop(inner);

    github::report_check_run_completed(
        &shared,
        completion.run_id,
        &completion.job_id,
        completion.status,
    )
    .await;

    if promoted_jobs > 0 || cancelled_siblings > 0 {
        shared.state.message_notify.notify_waiters();
    }

    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id: completion.run_id,
            job_id: completion.job_id,
            status: completion.status,
        })
        .await;
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id: completion.run_id,
            status: record.status,
        })
        .await;
    Ok(Json(record))
}

/// Check if pending jobs can be dispatched and promote them to the queue.
fn promote_ready_jobs(inner: &mut InnerState) -> usize {
    let mut promoted = Vec::new();
    let mut remaining = VecDeque::new();

    while let Some(mut job) = inner.pending_jobs.pop_front() {
        let needs_satisfied = inner
            .runs
            .get(&job.run_id)
            .is_some_and(|run| job.needs.iter().all(|need| need_satisfied(run, need)))
            && under_max_parallel(inner, &job);

        if needs_satisfied {
            if let Some(run) = inner.runs.get(&job.run_id) {
                hydrate_needs_context(&mut job, run);
            }
            promoted.push(job);
        } else {
            remaining.push_back(job);
        }
    }

    let promoted_count = promoted.len();
    inner.pending_jobs = remaining;
    for job in promoted {
        inner.queue.push_back(job);
    }
    promoted_count
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

fn apply_matrix_fail_fast(inner: &mut InnerState, run_id: RunId, failed_job: &JobId) -> usize {
    let Some(run) = inner.runs.get_mut(&run_id) else {
        return 0;
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return 0;
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return 0;
    }

    // Track in-progress siblings: they need a JOB_CANCELLED message so the
    // runner aborts the worker. Queued siblings only need their state flipped
    // — they were never dispatched.
    let mut cancellations = Vec::new();
    for (job_id, status) in &mut run.jobs {
        if job_id != failed_job
            && run.job_base_ids.get(job_id) == Some(&base_id)
            && matches!(
                status,
                ExecutionStatus::Queued | ExecutionStatus::InProgress
            )
        {
            if matches!(status, ExecutionStatus::InProgress) {
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                });
            }
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
    let count = cancellations.len();
    inner.cancellation_queue.extend(cancellations);
    count
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

fn need_context(run: &RunRecord, need: &JobId) -> Option<azdo::PipelineContextData> {
    let mut result = None;
    let mut outputs = BTreeMap::new();
    let matrix_prefix = format!("{} (", need.0);

    for (job_id, status) in &run.jobs {
        if job_id == need || job_id.0.starts_with(&matrix_prefix) {
            result = Some(status_string(*status));
            if let Some(job_outputs) = run.job_outputs.get(job_id) {
                for (key, value) in job_outputs {
                    outputs.insert(key.clone(), json_to_context_data(value));
                }
            }
        }
    }

    let mut context = BTreeMap::new();
    context.insert(
        "result".to_owned(),
        azdo::PipelineContextData::String(result?),
    );
    context.insert(
        "outputs".to_owned(),
        azdo::PipelineContextData::Dict(outputs),
    );
    Some(azdo::PipelineContextData::Dict(context))
}

fn status_string(status: ExecutionStatus) -> String {
    match status {
        ExecutionStatus::Queued | ExecutionStatus::InProgress | ExecutionStatus::Success => {
            "success"
        }
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
    .to_owned()
}

fn json_to_context_data(value: &serde_json::Value) -> azdo::PipelineContextData {
    match value {
        serde_json::Value::String(value) => azdo::PipelineContextData::String(value.clone()),
        serde_json::Value::Bool(value) => azdo::PipelineContextData::Bool(*value),
        serde_json::Value::Number(value) => {
            azdo::PipelineContextData::Number(value.as_f64().unwrap_or_default())
        }
        serde_json::Value::Array(values) => {
            azdo::PipelineContextData::Array(values.iter().map(json_to_context_data).collect())
        }
        serde_json::Value::Object(values) => azdo::PipelineContextData::Dict(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_context_data(value)))
                .collect(),
        ),
        serde_json::Value::Null => azdo::PipelineContextData::String(String::new()),
    }
}

fn need_satisfied(run: &RunRecord, need: &JobId) -> bool {
    let matrix_prefix = format!("{} (", need.0);
    let mut matched = false;

    for (job_id, status) in &run.jobs {
        if job_id == need || job_id.0.starts_with(&matrix_prefix) {
            matched = true;
            if !matches!(status, ExecutionStatus::Success | ExecutionStatus::Skipped) {
                return false;
            }
        }
    }

    matched
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
    let mut text = String::from_utf8_lossy(body).into_owned();
    let resolved_run_id = resolve_callback_job(inner, plan_id, None, None)
        .map(|(_, run_id, _)| run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let run_secrets = resolved_run_id
        .and_then(|run_id| inner.runs.get(&run_id))
        .map(|run| run.submission.secrets.values().collect::<Vec<_>>())
        .unwrap_or_else(|| {
            inner
                .runs
                .values()
                .flat_map(|run| run.submission.secrets.values())
                .collect()
        });

    for secret in run_secrets {
        let exposed = secret.expose();
        if !exposed.is_empty() {
            text = text.replace(exposed, "***");
        }
    }

    text.into_bytes()
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

/// POST action download info — resolve action references to download URLs.
async fn action_download_info(
    State(_shared): State<Arc<SharedState>>,
    Json(_request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    // For now, return empty info — actions will be downloaded from GitHub
    Json(json!({ "archiveDownloadTickets": {} }))
}

fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else if statuses
        .iter()
        .all(|status| matches!(status, ExecutionStatus::Success | ExecutionStatus::Skipped))
    {
        ExecutionStatus::Success
    } else {
        ExecutionStatus::InProgress
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
    let result = register_runner(State(shared), Json(reg_request)).await?;
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
            "clientId": uuid::Uuid::new_v4().to_string(),
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

    let token = local_jwt(json!({
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

async fn oauth2_token(
    _headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> Result<Json<TokenResponse>, ApiError> {
    let _ = body;
    let token = local_jwt(json!({
        "sub": "aksh-runner-listen",
        "scp": "ActionsRuntime.RunnerListen Framework.GenericRead Identity.ReadRefs LocationService.Connect",
        "jti": uuid::Uuid::new_v4().to_string()
    }))?;
    Ok(Json(TokenResponse {
        access_token: token,
        token_type: "JWT".to_owned(),
        expires_in: 2999,
    }))
}

fn local_jwt(mut claims: serde_json::Value) -> Result<String, ApiError> {
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
    let mut mac = Hmac::<Sha256>::new_from_slice(LOCAL_JWT_KEY)
        .map_err(|error| ApiError::bad_request(format!("invalid signing key: {error}")))?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{signature}"))
}

#[derive(Debug, Deserialize)]
struct OidcTokenQuery {
    audience: Option<String>,
}

#[derive(Debug, Serialize)]
struct OidcTokenResponse {
    value: String,
}

async fn oidc_token(
    Path((plan_id, job_id)): Path<(String, String)>,
    Query(query): Query<OidcTokenQuery>,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
        .as_secs();
    let audience = query.audience.unwrap_or_else(|| "api://aksh".to_owned());
    let header = json!({
        "alg": "HS256",
        "typ": "JWT",
        "kid": "aksh-local"
    });
    let claims = json!({
        "iss": "https://aksh.local",
        "sub": format!("repo:local:job:{job_id}"),
        "aud": audience,
        "iat": now,
        "nbf": now,
        "exp": now + 600,
        "jti": uuid::Uuid::new_v4().to_string(),
        "job_id": job_id,
        "plan_id": plan_id,
    });

    let signing_input = format!(
        "{}.{}",
        base64_url_json(&header)?,
        base64_url_json(&claims)?
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(b"aksh-local-oidc-signing-key")
        .map_err(|error| ApiError::bad_request(format!("invalid signing key: {error}")))?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());

    Ok(Json(OidcTokenResponse {
        value: format!("{signing_input}.{signature}"),
    }))
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

/// API error.
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<aksh_gha_parser::ParserError> for ApiError {
    fn from(value: aksh_gha_parser::ParserError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_gha_protocol::ProtocolError> for ApiError {
    fn from(value: aksh_gha_protocol::ProtocolError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_gha_protocol::crypto::CryptoError> for ApiError {
    fn from(value: aksh_gha_protocol::crypto::CryptoError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_cache::CacheError> for ApiError {
    fn from(value: aksh_cache::CacheError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<aksh_artifacts::ArtifactError> for ApiError {
    fn from(value: aksh_artifacts::ArtifactError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<std::io::Error> for ApiError {
    fn from(value: std::io::Error) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
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

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use serde_json::Value;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn matrix_max_parallel_and_fail_fast_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      max-parallel: 1
      matrix:
        os: [ubuntu, macos, windows]
    steps:
      - run: echo matrix
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
        let first_job = {
            let inner = state.inner.lock().await;
            assert_eq!(inner.queue.len(), 1);
            assert_eq!(inner.pending_jobs.len(), 2);
            inner.queue.front().unwrap().job_id.clone()
        };

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": first_job,
                "status": "failure"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        assert!(inner.queue.is_empty());
        assert!(inner.pending_jobs.is_empty());
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(
            run.jobs
                .values()
                .filter(|status| **status == ExecutionStatus::Cancelled)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn agent_request_patch_targets_only_the_request_id() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        // Two independent runs, both reach InProgress when their job is pulled.
        let workflow = json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        });
        let first = request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
        let second = request_json(&app, Method::POST, "/api/v1/runs", workflow).await;
        let first_run: RunId = first["run_id"].as_str().unwrap().parse().unwrap();
        let second_run: RunId = second["run_id"].as_str().unwrap().parse().unwrap();

        // Pull both jobs so they are InProgress and each has a distinct request_id.
        let first_msg = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1",
            Value::Null,
        )
        .await;
        let second_msg = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s2",
            Value::Null,
        )
        .await;
        assert_eq!(
            first_msg["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        assert_eq!(
            second_msg["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        // The mapping should have two entries — one per request_id.
        let (first_req_id, _) = state
            .inner
            .lock()
            .await
            .inflight_requests
            .iter()
            .find(|(_, (rid, _))| *rid == first_run)
            .map(|(k, v)| (*k, v.clone()))
            .unwrap();

        // PATCH only the first run's request_id.
        request_json(
            &app,
            Method::PATCH,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{first_req_id}"),
            json!({"result": "succeeded"}),
        )
        .await;

        let inner = state.inner.lock().await;
        let first = inner.runs.get(&first_run).unwrap();
        let second = inner.runs.get(&second_run).unwrap();
        assert!(first
            .jobs
            .values()
            .all(|status| *status == ExecutionStatus::Success));
        assert!(second
            .jobs
            .values()
            .all(|status| *status == ExecutionStatus::InProgress));
        assert!(!inner.inflight_requests.contains_key(&first_req_id));
    }

    #[tokio::test]
    async fn agent_request_get_reports_completion_result() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let _msg = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1",
            Value::Null,
        )
        .await;
        let request_id = {
            let inner = state.inner.lock().await;
            inner
                .inflight_requests
                .iter()
                .find(|(_, (rid, _))| *rid == run_id)
                .map(|(request_id, _)| *request_id)
                .unwrap()
        };

        let before = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(before["requestId"], request_id);
        assert!(before["result"].is_null());

        request_json(
            &app,
            Method::PATCH,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
            json!({"result": "succeeded"}),
        )
        .await;

        let after = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{request_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(after["result"], "succeeded");
    }

    #[tokio::test]
    async fn same_session_waits_for_active_request_before_next_job() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let first = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            first["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let first_message_id = first["messageId"].as_i64().unwrap();
        request_json(
            &app,
            Method::DELETE,
            &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s1"),
            Value::Null,
        )
        .await;

        let first_request_id = {
            let inner = state.inner.lock().await;
            *inner.session_active_requests.get("s1").unwrap()
        };

        let withheld = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert!(withheld.is_null());

        request_json(
            &app,
            Method::PATCH,
            &format!("/runner/server/_apis/v1/AgentRequest/1/{first_request_id}"),
            json!({"result": "succeeded"}),
        )
        .await;

        let second = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            second["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(
            run.jobs
                .values()
                .filter(|status| **status == ExecutionStatus::InProgress)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn unacked_messages_are_scoped_to_their_session() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let workflow = json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo"
        });
        request_json(&app, Method::POST, "/api/v1/runs", workflow.clone()).await;
        request_json(&app, Method::POST, "/api/v1/runs", workflow).await;

        let first = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            first["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let first_message_id = first["messageId"].as_i64().unwrap();

        let second = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s2&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            second["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let second_message_id = second["messageId"].as_i64().unwrap();
        assert_ne!(first_message_id, second_message_id);

        // ACKing s1's message through s2 must not remove it from s1. The next
        // s1 poll should redeliver the same unacked message, not s2's message.
        request_json(
            &app,
            Method::DELETE,
            &format!("/runner/server/_apis/v1/Message/1/{first_message_id}?sessionId=s2"),
            Value::Null,
        )
        .await;

        let redelivered = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(redelivered["messageId"], first_message_id);

        let inner = state.inner.lock().await;
        assert!(inner
            .inflight_messages
            .get("s1")
            .is_some_and(|messages| messages.contains_key(&first_message_id)));
        assert!(inner
            .inflight_messages
            .get("s2")
            .is_some_and(|messages| messages.contains_key(&second_message_id)));
    }

    #[tokio::test]
    async fn finish_job_resolves_plan_timeline_and_agent_job_ids() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let first = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            first["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        let request = {
            let inner = state.inner.lock().await;
            inner.job_requests.values().next().unwrap().clone()
        };

        request_json(
            &app,
            Method::POST,
            &format!(
                "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
                request.plan_id
            ),
            json!({
                "jobId": request.agent_job_id,
                "result": "succeeded",
                "timelineId": request.timeline_id,
                "outputs": {"answer": "42"}
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(
            run.jobs.get(&request.job_id),
            Some(&ExecutionStatus::Success)
        );
        assert!(!run
            .jobs
            .contains_key(&JobId(request.agent_job_id.to_string())));
        assert_eq!(
            run.job_outputs
                .get(&request.job_id)
                .and_then(|outputs| outputs.get("answer")),
            Some(&json!("42"))
        );
        assert_eq!(
            inner
                .job_requests
                .get(&request.request_id)
                .and_then(|request| request.result),
            Some(ExecutionStatus::Success)
        );
    }

    #[tokio::test]
    async fn finish_job_falls_back_to_the_single_active_request_when_unresolved() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        n: [1, 2]
    steps:
      - run: echo matrix
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=s1&waitSeconds=0",
            Value::Null,
        )
        .await;

        let active_request = {
            let inner = state.inner.lock().await;
            let active_id = *inner.session_active_requests.get("s1").unwrap();
            inner.job_requests.get(&active_id).unwrap().clone()
        };
        let unknown_plan_id = uuid::Uuid::new_v4();
        let unknown_job_id = uuid::Uuid::new_v4();
        let unknown_timeline_id = uuid::Uuid::new_v4();

        // If callback identifiers cannot be resolved at all, the only
        // unfinished active request is the safest correlation available.
        request_json(
            &app,
            Method::POST,
            &format!(
                "/runner/server/_apis/v1/FinishJob/00000000-0000-0000-0000-000000000000/Job/{}",
                unknown_plan_id
            ),
            json!({
                "jobId": unknown_job_id,
                "result": "succeeded",
                "timelineId": unknown_timeline_id,
                "outputs": {}
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(
            run.jobs.get(&active_request.job_id),
            Some(&ExecutionStatus::Success)
        );
    }

    #[tokio::test]
    async fn matrix_fail_fast_cancels_in_progress_siblings_via_message() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      matrix:
        os: [ubuntu, macos]
    steps:
      - run: echo matrix
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // Dispatch both siblings — both move to InProgress.
        let first = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(
            first["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        let second = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(
            second["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        let failing_job = {
            let inner = state.inner.lock().await;
            inner
                .runs
                .get(&run_id)
                .unwrap()
                .jobs
                .keys()
                .next()
                .unwrap()
                .clone()
        };

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": failing_job,
                "status": "failure"
            }),
        )
        .await;

        // The fix: in-progress siblings get a cancellation enqueued so the
        // runner receives a JOB_CANCELLED message. Inspect the queue directly
        // since the matched siblings still have unACKed in-flight job messages.
        let inner = state.inner.lock().await;
        assert_eq!(inner.cancellation_queue.len(), 1);
        let cancellation = inner.cancellation_queue.front().unwrap();
        assert_eq!(cancellation.run_id, run_id);
        assert_ne!(cancellation.job_id, failing_job);
        // The sibling is now Cancelled in the run state.
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(
            run.jobs.get(&cancellation.job_id),
            Some(&ExecutionStatus::Cancelled)
        );
    }

    #[tokio::test]
    async fn needs_context_includes_completed_job_outputs() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  deploy:
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "build",
                "status": "success",
                "outputs": {"artifact": "dist.tgz"}
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let deploy = inner
            .queue
            .iter()
            .find(|job| job.job_id.0 == "deploy")
            .expect("deploy job should be promoted");
        let needs = deploy.message.context_data.get("needs").unwrap();
        let azdo::PipelineContextData::Dict(needs) = needs else {
            panic!("needs context should be a dict");
        };
        let azdo::PipelineContextData::Dict(build) = needs.get("build").unwrap() else {
            panic!("build context should be a dict");
        };
        let azdo::PipelineContextData::Dict(outputs) = build.get("outputs").unwrap() else {
            panic!("outputs context should be a dict");
        };
        assert!(matches!(
            outputs.get("artifact"),
            Some(azdo::PipelineContextData::String(value)) if value == "dist.tgz"
        ));
    }

    #[tokio::test]
    async fn scenario_06_multi_step_dispatches_all_steps() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
name: mitm multi step
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo first
      - run: echo "VAL=$VAL"
        env:
          VAL: hello
      - run: |
          echo line1
          echo line2
"#,
                "event": "workflow_dispatch",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let scripts = {
            let inner = state.inner.lock().await;
            let queued = inner.queue.front().expect("build job should be queued");
            queued
                .message
                .steps
                .iter()
                .filter_map(|step| step.script.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(scripts.len(), 3);
        assert!(scripts.contains(&"echo first".to_owned()));
        assert!(scripts.contains(&"echo \"VAL=$VAL\"".to_owned()));
        assert!(scripts
            .iter()
            .any(|script| script.contains("echo line1") && script.contains("echo line2")));

        let message = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(
            message["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "build",
                "status": "success"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(run.status, ExecutionStatus::Success);
        assert_eq!(
            run.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Success)
        );
    }

    #[tokio::test]
    async fn scenario_07_step_failure_summarizes_run_failed() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
name: mitm step failure
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    steps:
      - run: exit 1
      - run: echo ran-on-failure
        if: failure()
      - run: echo never
        if: success()
"#,
                "event": "workflow_dispatch",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        let message = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(
            message["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "build",
                "status": "failure"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        assert_eq!(run.status, ExecutionStatus::Failure);
        assert_eq!(
            run.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Failure)
        );
    }

    #[tokio::test]
    async fn scenario_08_consumer_sees_producer_outputs() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
name: mitm job outputs
on: workflow_dispatch
jobs:
  producer:
    runs-on: [self-hosted, mitm]
    outputs:
      value: ${{ steps.gen.outputs.value }}
    steps:
      - id: gen
        run: echo "value=42" >> "$GITHUB_OUTPUT"
  consumer:
    needs: producer
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo "got ${{ needs.producer.outputs.value }}"
"#,
                "event": "workflow_dispatch",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "producer",
                "status": "success",
                "outputs": {"value": "42"}
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        let consumer = inner
            .queue
            .iter()
            .find(|job| job.job_id.0 == "consumer")
            .expect("consumer job should be promoted");
        let azdo::PipelineContextData::Dict(needs) =
            consumer.message.context_data.get("needs").unwrap()
        else {
            panic!("needs context should be a dict");
        };
        let azdo::PipelineContextData::Dict(producer) = needs.get("producer").unwrap() else {
            panic!("producer needs entry should be a dict");
        };
        let azdo::PipelineContextData::Dict(outputs) = producer.get("outputs").unwrap() else {
            panic!("producer outputs should be a dict");
        };
        assert!(matches!(
            outputs.get("value"),
            Some(azdo::PipelineContextData::String(value)) if value == "42"
        ));
    }

    #[tokio::test]
    async fn scenario_09_matrix_fail_fast_cancels_siblings() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
name: mitm matrix
on: workflow_dispatch
jobs:
  build:
    runs-on: [self-hosted, mitm]
    strategy:
      fail-fast: true
      matrix:
        n: [1, 2, 3]
    steps:
      - run: |
          if [ "${{ matrix.n }}" = "1" ]; then exit 1; fi
          sleep 20
"#,
                "event": "workflow_dispatch",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        for index in 0..3 {
            let session_id = format!("matrix-{index}");
            let message = request_json(
                &app,
                Method::GET,
                &format!("/runner/server/_apis/v1/Message/1?sessionId={session_id}"),
                Value::Null,
            )
            .await;
            assert_eq!(
                message["messageType"],
                azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
            );
            let message_id = message["messageId"].as_i64().unwrap();
            let ack = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::DELETE)
                        .uri(format!(
                            "/runner/server/_apis/v1/Message/1/{message_id}?sessionId={session_id}"
                        ))
                        .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(ack.status(), StatusCode::NO_CONTENT);
        }

        let failing_job = {
            let inner = state.inner.lock().await;
            inner
                .runs
                .get(&run_id)
                .unwrap()
                .jobs
                .iter()
                .find_map(|(job_id, status)| {
                    (*status == ExecutionStatus::InProgress).then(|| job_id.clone())
                })
                .expect("a matrix sibling should be in progress")
        };

        request_json(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": failing_job,
                "status": "failure"
            }),
        )
        .await;

        let inner = state.inner.lock().await;
        assert_eq!(inner.cancellation_queue.len(), 2);
        let run = inner.runs.get(&run_id).unwrap();
        for (job_id, status) in &run.jobs {
            if job_id == &failing_job {
                assert_eq!(*status, ExecutionStatus::Failure);
            } else {
                assert_eq!(*status, ExecutionStatus::Cancelled);
            }
        }
    }

    #[tokio::test]
    async fn timeline_patch_projects_annotations_to_run_events() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo annotated
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap();
        request_json(
            &app,
            Method::PATCH,
            &format!("/_apis/v1/Timeline/scope/actions/{run_id}/timeline-1"),
            json!({"count": 1, "value": [{
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "build",
                "type": "job",
                "state": "completed",
                "result": "failed",
                "issues": [{
                    "type": "error",
                    "message": "boom",
                    "data": {"file": "src/lib.rs", "line": "42"}
                }]
            }]}),
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/api/v1/runs/{run_id}/events.ndjson"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let events = String::from_utf8(bytes.to_vec()).unwrap();

        assert!(events.contains("\"type\":\"annotation\""));
        assert!(events.contains("\"message\":\"boom\""));
        assert!(events.contains("\"status\":\"failure\""));
    }

    #[tokio::test]
    async fn live_log_websocket_accepts_bearer_and_stores_lines() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("ws://{addr}/ws/live-logs/job-live");
        let mut request =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(url)
                .unwrap();
        request.headers_mut().insert(
            header::AUTHORIZATION,
            "Bearer aksh-system-token".parse().unwrap(),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let payload = json!({
            "stepId": "step-1",
            "startLine": 1,
            "count": 2,
            "value": ["hello", "world"]
        });
        futures::SinkExt::send(
            &mut ws,
            tokio_tungstenite::tungstenite::Message::Text(payload.to_string()),
        )
        .await
        .unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                {
                    let inner = state.inner.lock().await;
                    if let Some(job_lines) = inner.live_log_lines.get("job-live") {
                        let wrappers = job_lines.lock().await;
                        if wrappers.len() == 1 {
                            assert_eq!(wrappers[0].step_id, "step-1");
                            assert_eq!(wrappers[0].start_line, 1);
                            assert_eq!(wrappers[0].count, 2);
                            assert_eq!(wrappers[0].value, vec!["hello", "world"]);
                            break;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        server.abort();
    }

    #[tokio::test]
    async fn log_append_persists_payload_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::POST,
            "/_apis/v1/Logfiles/scope/actions/plan-1",
            json!({"path": "log-1"}),
        )
        .await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/_apis/v1/Logfiles/scope/actions/plan-1/log-1")
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::from("hello log"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let inner = state.inner.lock().await;
        assert_eq!(
            inner.logs.get("plan-1/log-1").map(Vec::as_slice),
            Some(&b"hello log"[..])
        );
    }

    #[tokio::test]
    async fn log_append_masks_submitted_secrets() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo masked
"#,
                "event": "push",
                "repository": "owner/repo",
                "secrets": {"TOKEN": "super-secret"}
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/_apis/v1/Logfiles/scope/actions/{run_id}/log-1"))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::from("token=super-secret"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .logs
                .get(&format!("{run_id}/log-1"))
                .map(Vec::as_slice),
            Some(&b"token=***"[..])
        );
    }

    #[tokio::test]
    async fn registration_persists_runner_public_key_material() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        let public_key = AgentRsaKeypair::generate().unwrap().public_key_xml();

        let runner = request_json(
            &app,
            Method::POST,
            "/api/v1/runners",
            json!({
                "name": "local",
                "labels": ["self-hosted"],
                "public_key": public_key
            }),
        )
        .await;
        let runner_id = runner["id"].as_i64().unwrap();

        let inner = state.inner.lock().await;
        assert_eq!(inner.runner_public_keys.get(&runner_id), Some(&public_key));
        assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
    }

    #[tokio::test]
    async fn session_key_uses_registered_runner_public_key() {
        let temp = tempfile::tempdir().unwrap();
        let runner_keypair = AgentRsaKeypair::generate().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let runner = request_json(
            &app,
            Method::POST,
            "/api/v1/runners",
            json!({
                "name": "local",
                "labels": ["self-hosted"],
                "public_key": runner_keypair.public_key_xml()
            }),
        )
        .await;
        let runner_id = runner["id"].as_i64().unwrap();

        let session = request_json(
            &app,
            Method::POST,
            "/api/v1/runners/sessions",
            json!({"runner_id": runner_id, "name": "local"}),
        )
        .await;
        let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
        let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
        assert!(encrypted, "session key should be RSA wrapped");
        let wrapped_key =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
        let key_bytes = runner_keypair.unwrap_key(&wrapped_key).unwrap();
        assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
    }

    #[tokio::test]
    async fn session_key_falls_back_to_plaintext_without_registered_public_key() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let runner = request_json(
            &app,
            Method::POST,
            "/api/v1/runners",
            json!({
                "name": "local",
                "labels": ["self-hosted"]
            }),
        )
        .await;
        let runner_id = runner["id"].as_i64().unwrap();

        let session = request_json(
            &app,
            Method::POST,
            "/api/v1/runners/sessions",
            json!({"runner_id": runner_id, "name": "local"}),
        )
        .await;
        let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
        let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
        assert!(
            !encrypted,
            "session key should remain plaintext only when the runner registered no key"
        );
        let key_bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
        assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
    }

    #[tokio::test]
    async fn task_agent_registration_extracts_nested_public_key() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let runner_keypair = AgentRsaKeypair::generate().unwrap();
        let public_xml = runner_keypair.public_key_xml();
        let modulus = public_xml
            .split("<Modulus>")
            .nth(1)
            .unwrap()
            .split("</Modulus>")
            .next()
            .unwrap();
        let exponent = public_xml
            .split("<Exponent>")
            .nth(1)
            .unwrap()
            .split("</Exponent>")
            .next()
            .unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let runner = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/agents",
            json!({
                "name": "local",
                "labels": [{"name": "self-hosted", "type": "system"}],
                "authorization": {
                    "publicKey": {
                        "modulus": modulus,
                        "exponent": exponent
                    }
                }
            }),
        )
        .await;
        let runner_id = runner["id"].as_i64().unwrap();
        let inner = state.inner.lock().await;
        assert!(inner.runner_rsa_public_keys.contains_key(&runner_id));
    }

    #[tokio::test]
    async fn connection_data_exposes_current_runner_service_locations() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let conn = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
            Value::Null,
        )
        .await;
        let services = conn["locationServiceData"]["serviceDefinitions"]
            .as_array()
            .unwrap();
        let service_ids = services
            .iter()
            .filter_map(|service| service["identifier"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(service_ids.contains("38f00041-0953-4d24-86c3-5432d23e2205"));
        assert!(service_ids.contains("a4e1f2b5-0c3d-4e8a-9f6d-7b5c1a0e2d3f"));
        assert!(service_ids.contains("10d13a60-2758-406c-8ab7-cffccb21fcf4"));
        assert_eq!(
            conn["locationServiceData"]["defaultAccessMappingMoniker"],
            "ScaleUnitMapping"
        );

        let fresh = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/connectionData?connectOptions=0&lastChangeId=1&lastChangeId64=1",
            Value::Null,
        )
        .await;
        assert_eq!(fresh["locationServiceData"]["clientCacheFresh"], true);
        assert!(fresh["locationServiceData"]["serviceDefinitions"].is_null());
    }

    #[tokio::test]
    async fn registration_and_oauth_return_runner_compatible_tokens() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let registration = request_json(
            &app,
            Method::POST,
            "/api/v3/actions/runner-registration",
            json!({"url": "https://github.com/preloopdev/aksh", "runner_event": "register"}),
        )
        .await;
        assert_eq!(registration["token_schema"], "OAuthAccessToken");
        assert_eq!(registration["url"], "http://127.0.0.1:9090/runner/server");
        assert_eq!(
            registration["token"].as_str().unwrap().split('.').count(),
            3
        );
        assert!(registration.get("use_v2_flow").is_none());

        let token = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/v1/oauth2/token",
            json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
        )
        .await;
        assert_eq!(token["token_type"], "JWT");
        assert_eq!(token["expires_in"], 2999);
        assert_eq!(
            token["access_token"].as_str().unwrap().split('.').count(),
            3
        );
    }

    #[tokio::test]
    async fn current_runner_registration_to_broker_job_e2e() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state, CancellationToken::new());
        let runner_keypair = AgentRsaKeypair::generate().unwrap();
        let public_xml = runner_keypair.public_key_xml();
        let modulus = public_xml
            .split("<Modulus>")
            .nth(1)
            .unwrap()
            .split("</Modulus>")
            .next()
            .unwrap();
        let exponent = public_xml
            .split("<Exponent>")
            .nth(1)
            .unwrap()
            .split("</Exponent>")
            .next()
            .unwrap();

        let registration_auth = request_json(
            &app,
            Method::POST,
            "/api/v3/actions/runner-registration",
            json!({"url": "https://github.com/preloopdev/aksh", "runner_event": "register"}),
        )
        .await;
        assert_eq!(
            registration_auth["url"],
            "http://127.0.0.1:9090/runner/server"
        );

        let connection = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/connectionData?connectOptions=1&lastChangeId=-1&lastChangeId64=-1",
            Value::Null,
        )
        .await;
        assert!(connection["locationServiceData"]["serviceDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|service| service["displayName"] == "brokerlistener"));

        let agent = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/agents",
            json!({
                "name": "runner-1",
                "version": "2.335.1",
                "osDescription": "Darwin local",
                "labels": [
                    {"name": "self-hosted", "type": "system"},
                    {"name": "macOS", "type": "system"},
                    {"name": "ARM64", "type": "system"}
                ],
                "authorization": {
                    "publicKey": {
                        "modulus": modulus,
                        "exponent": exponent
                    }
                }
            }),
        )
        .await;
        let runner_id = agent["id"].as_i64().unwrap();
        assert_eq!(agent["properties"]["UseV2Flow"]["$value"], true);
        assert_eq!(
            agent["properties"]["ServerUrlV2"]["$value"],
            "http://127.0.0.1:9090/runner/server"
        );

        let oauth = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/v1/oauth2/token",
            json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
        )
        .await;
        assert_eq!(oauth["token_type"], "JWT");

        let session = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({
                "agent": {"id": runner_id, "name": "runner-1", "version": "2.335.1"},
                "ownerName": "local current runner",
                "sessionId": "00000000-0000-0000-0000-000000000000",
                "useFipsEncryption": false
            }),
        )
        .await;
        let session_id = session["sessionId"].as_str().unwrap();

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "name: Current Runner Verification\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo current\n",
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/aksh",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
        assert_eq!(accepted["queued_jobs"], 1);

        let broker_ref = request_json(
            &app,
            Method::GET,
            &format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&status=Online&runnerVersion=2.335.1&os=macOS&architecture=ARM64&waitSeconds=0"),
            Value::Null,
        )
        .await;
        assert_eq!(broker_ref["messageType"], "RunnerJobRequest");
        let body: Value = serde_json::from_str(broker_ref["body"].as_str().unwrap()).unwrap();
        assert_eq!(body["should_acknowledge"], true);
        let runner_request_id = body["runner_request_id"].as_str().unwrap();

        let acquired = request_json(
            &app,
            Method::POST,
            &format!("/broker/{runner_id}/acquirejob"),
            json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}),
        )
        .await;
        assert_eq!(acquired["requestId"], 1);
        assert_eq!(
            acquired["messageType"],
            azdo::message_type::RUNNER_JOB_REQUEST
        );
        assert_eq!(
            acquired["resources"]["endpoints"][0]["url"],
            format!("http://127.0.0.1:9090/broker/{runner_id}/")
        );
        assert_eq!(
            acquired["resources"]["endpoints"][0]["data"]["FeedStreamUrl"],
            format!(
                "ws://127.0.0.1:9090/ws/live-logs/{}",
                acquired["jobId"].as_str().unwrap()
            )
        );
        assert!(acquired["contextData"]["github"].is_object());
        let github_context_json = serde_json::to_string(&acquired["contextData"]["github"])
            .expect("github context should serialize");
        assert!(
            github_context_json.contains("\"workflow\""),
            "github context missing workflow key: {github_context_json}"
        );
        assert!(
            github_context_json.contains("Current Runner Verification"),
            "github context missing workflow name: {github_context_json}"
        );
        assert!(
            acquired["steps"].as_array().unwrap().iter().any(|step| {
                step["inputs"]["script"].as_str() == Some("echo current")
                    || step["inputs"]["script"]["expr"].as_str() == Some("echo current")
                    || step["inputs"]["map"].as_array().is_some_and(|entries| {
                        entries.iter().any(|entry| {
                            entry["key"].as_str() == Some("script")
                                && entry["value"].as_str() == Some("echo current")
                        })
                    })
            }),
            "steps={}",
            acquired["steps"]
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/broker/{runner_id}/completejob"))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn current_service_broker_flow_uses_queued_job() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        let workflow = "on:
  push:
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
";
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": workflow,
                "event": "push",
                "payload": {"ref": "refs/heads/main", "commits": []},
                "repository": "preloopdev/aksh",
                "git_ref": "refs/heads/main",
                "secrets": {},
                "vars": {},
                "reusable_workflows": {}
            }),
        )
        .await;
        assert_eq!(accepted["queued_jobs"], 1);

        let session = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({
                "agent": {"id": 1, "name": "runner-1"},
                "ownerName": "owner",
                "sessionId": "00000000-0000-0000-0000-000000000000",
                "useFipsEncryption": false
            }),
        )
        .await;
        let session_id = session["sessionId"].as_str().unwrap();

        let response = app.clone().oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"))
                .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                .body(Body::empty())
                .unwrap(),
        ).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let message: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(message["messageType"], "RunnerJobRequest");
        let body: Value = serde_json::from_str(message["body"].as_str().unwrap()).unwrap();
        assert_eq!(body["should_acknowledge"], true);
        let runner_request_id = body["runner_request_id"].as_str().unwrap();
        assert!(body["run_service_url"]
            .as_str()
            .unwrap()
            .contains("/broker/1/"));
        assert_eq!(session["ownerName"], "owner");
        assert_eq!(session["assignmentQueued"], false);
        assert_eq!(session["orchestrationId"], "");

        let acquired = request_json(
            &app,
            Method::POST,
            "/broker/1/acquirejob",
            json!({"jobMessageId": runner_request_id, "billingOwnerId": "local", "runnerOS": "macOS"}),
        )
        .await;
        assert_eq!(acquired["requestId"].as_i64().unwrap(), 1);
        assert_eq!(
            acquired["messageType"],
            azdo::message_type::RUNNER_JOB_REQUEST
        );
        assert_eq!(
            acquired["resources"]["endpoints"][0]["url"],
            "http://127.0.0.1:9090/broker/1/"
        );
        assert!(acquired["plan"]["planId"].is_string());
        assert!(acquired["jobId"].is_string());
        assert!(acquired["steps"].is_array());

        let renewed = request_json(
            &app,
            Method::POST,
            "/broker/1/renewjob",
            json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]}),
        )
        .await;
        assert!(renewed["lockedUntil"].as_str().unwrap().contains('T'));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/broker/1/completejob")
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"jobId": runner_request_id, "planId": acquired["plan"]["planId"]})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let completed_run = request_json(
            &app,
            Method::GET,
            &format!("/api/v1/runs/{}", accepted["run_id"].as_str().unwrap()),
            Value::Null,
        )
        .await;
        assert_eq!(completed_run["status"], "success");
        assert_eq!(completed_run["jobs"]["rust"], "success");

        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/runner/server/_apis/v1/AgentRequest/1/1")
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_apis_require_bearer_token() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/_apis/artifactcache/cache?keys=x&version=v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn runner_server_v1_sensitive_routes_require_bearer() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        // These /runner/server/_apis/v1/* aliases were previously placed on
        // the public router, letting unauthenticated callers mutate timelines,
        // inject logs, and finish jobs. They MUST require a bearer token.
        let cases = [
            (Method::PATCH, "/runner/server/_apis/v1/Timeline/s/h/p/t"),
            (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
            (Method::POST, "/runner/server/_apis/v1/Logfiles/s/h/p/l"),
            (
                Method::POST,
                "/runner/server/_apis/v1/TimeLineWebConsoleLog/s/h/p/t/r",
            ),
            (Method::POST, "/runner/server/_apis/v1/FinishJob/s/h/p"),
            (
                Method::POST,
                "/runner/server/_apis/v1/ActionDownloadInfo/s/h/p",
            ),
        ];
        for (method, uri) in cases {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from("{}"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri} should require bearer auth"
            );
        }
    }

    #[tokio::test]
    async fn oidc_endpoint_mints_jwt_with_requested_audience() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let token = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/distributedtask/hubs/actions/plans/plan-1/jobs/job-1/oidctoken?audience=api://custom",
            Value::Null,
        )
        .await;
        let jwt = token["value"].as_str().unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let claims: Value = serde_json::from_slice(&claims).unwrap();

        assert_eq!(claims["aud"], "api://custom");
        assert_eq!(claims["job_id"], "job-1");
        assert_eq!(claims["plan_id"], "plan-1");
    }

    #[tokio::test]
    async fn scenario_15_oidc_token_carries_requested_audience() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let token = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/distributedtask/hubs/actions/plans/plan-15/jobs/job-15/oidctoken?audience=api://aksh",
            Value::Null,
        )
        .await;
        let jwt = token["value"].as_str().unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        let claims = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let claims: Value = serde_json::from_slice(&claims).unwrap();

        assert_eq!(claims["aud"], "api://aksh");
        assert_eq!(claims["job_id"], "job-15");
        assert_eq!(claims["plan_id"], "plan-15");
    }

    #[tokio::test]
    async fn messages_redeliver_until_delete_ack() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;

        let first = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(first["messageId"], 1);

        let redelivered = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(redelivered["messageId"], first["messageId"]);

        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/runner/server/_apis/v1/Message/1/1?sessionId=default")
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack.status(), StatusCode::NO_CONTENT);

        let empty = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert!(empty.is_null());
    }

    #[tokio::test]
    async fn cancel_run_delivers_cancellation_message() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: sleep 30
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap();

        let message = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;
        let message_id = message["messageId"].as_i64().unwrap();

        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/v1/Message/1/{message_id}?sessionId=default"
                    ))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ack.status(), StatusCode::NO_CONTENT);

        request_json(
            &app,
            Method::POST,
            &format!("/api/v1/runs/{run_id}/cancel"),
            Value::Null,
        )
        .await;

        let cancellation = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=0",
            Value::Null,
        )
        .await;
        assert_eq!(
            cancellation["messageType"],
            azdo::message_type::JOB_CANCELLED
        );
    }

    #[tokio::test]
    async fn message_poll_waits_until_work_is_enqueued() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );
        let poll_app = app.clone();
        let poll = tokio::spawn(async move {
            request_json(
                &poll_app,
                Method::GET,
                "/runner/server/_apis/v1/Message/1?sessionId=default&waitSeconds=2",
                Value::Null,
            )
            .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo waited
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;

        let message = poll.await.unwrap();
        assert_eq!(message["messageId"], 1);
    }

    #[tokio::test]
    async fn session_message_flow_encrypts_decryptable_job_body() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let _keypair = {
            let inner = state.inner.lock().await;
            inner.agent_keypair.clone().unwrap()
        };
        let app = app(state, CancellationToken::new());

        let session = request_json(
            &app,
            Method::POST,
            "/api/v1/runners/sessions",
            json!({"runner_id": 1, "name": "local"}),
        )
        .await;
        let session_id = session["sessionId"].as_str().unwrap();
        let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
        let aes_key =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();

        request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo encrypted
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;

        let message = request_json(
            &app,
            Method::GET,
            &format!("/api/v1/runners/sessions/{session_id}/messages?sessionId={session_id}"),
            Value::Null,
        )
        .await;

        let body = BASE64_STANDARD
            .decode(message["body"].as_str().unwrap())
            .unwrap();
        let iv: Vec<u8> = message["iv"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap() as u8)
            .collect();
        let plaintext = SessionEncryption::from_key(aes_key)
            .decrypt(&body, &iv)
            .unwrap();
        let job: azdo::AgentJobRequestMessage = serde_json::from_slice(&plaintext).unwrap();

        assert_eq!(
            message["messageType"],
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST
        );
        assert_eq!(job.steps[0].script.as_deref(), Some("echo encrypted"));
    }

    #[tokio::test]
    async fn submit_run_uses_branch_and_path_filters() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                "event": "push",
                "repository": "owner/repo",
                "git_ref": "refs/heads/main",
                "payload": {
                    "commits": [
                        { "added": [], "modified": ["src/lib.rs"], "removed": [] }
                    ]
                }
            }),
        )
        .await;
        assert!(accepted["run_id"].is_string());

        let rejected = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "workflow_yaml": r#"
on:
  push:
    branches: [main]
    paths: ["src/**"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
                            "event": "push",
                            "repository": "owner/repo",
                            "git_ref": "refs/heads/feature",
                            "payload": {
                                "commits": [
                                    { "added": [], "modified": ["docs/readme.md"], "removed": [] }
                                ]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cache_protocol_reserves_uploads_commits_and_restores() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );

        let reserve = request_json(
            &app,
            Method::POST,
            "/_apis/artifactcache/cache",
            json!({"key": "linux-node", "version": "v1"}),
        )
        .await;
        let cache_id = reserve["cacheId"].as_i64().unwrap();

        let upload = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/_apis/artifactcache/cache/{cache_id}"))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::from("cache-bytes"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upload.status(), StatusCode::ACCEPTED);

        request_json(
            &app,
            Method::POST,
            &format!("/_apis/artifactcache/cache/{cache_id}"),
            json!({"size": 11}),
        )
        .await;

        let lookup = request_json(
            &app,
            Method::GET,
            "/api/v1/cache?key=linux-node&version=v1",
            Value::Null,
        )
        .await;
        assert_eq!(lookup["hit"], true);
        assert_eq!(lookup["content_base64"], "Y2FjaGUtYnl0ZXM=");
    }

    #[tokio::test]
    async fn artifact_endpoint_stores_and_downloads_payload() {
        let temp = tempfile::tempdir().unwrap();
        let app = app(
            AppState::new(temp.path().to_path_buf()).await.unwrap(),
            CancellationToken::new(),
        );
        let run_id = RunId::new();

        let created = request_json(
            &app,
            Method::POST,
            "/api/v1/artifacts",
            json!({
                "run_id": run_id,
                "name": "logs",
                "file_name": "job.txt",
                "content_base64": "aGVsbG8="
            }),
        )
        .await;
        let artifact_id = created["id"].as_str().unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(format!("/api/v1/artifacts/{artifact_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"hello");
    }

    #[tokio::test]
    async fn full_runner_lifecycle_register_session_poll_complete() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        // Non-asserting helper
        async fn try_req(
            app: &Router,
            method: Method,
            uri: &str,
            body: Value,
        ) -> (StatusCode, Value) {
            let mut builder = Request::builder().method(method).uri(uri);
            if uri.starts_with("/_apis/")
                || uri.starts_with("/runner/server/_apis/")
                || uri.starts_with("/broker/")
                || uri.starts_with("/twirp/")
            {
                builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
            } else if uri.starts_with("/api/v3/actions/runner-registration") {
                builder =
                    builder.header(header::AUTHORIZATION, "RemoteAuth aksh-registration-token");
            }
            let request = if body.is_null() {
                builder.body(Body::empty()).unwrap()
            } else {
                builder
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap()
            };
            let response = app.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (status, val)
        }

        // 1. connectionData
        let (s, conn) = try_req(
            &app,
            Method::GET,
            "/runner/server/_apis/connectionData",
            Value::Null,
        )
        .await;
        assert!(s.is_success(), "1 connectionData: {}", s);
        assert!(conn["locationServiceData"]["serviceDefinitions"].is_array());

        // 2. OAuth token
        let (s, _) = try_req(
            &app,
            Method::POST,
            "/_apis/v1/oauth2/token",
            json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"}),
        )
        .await;
        assert!(s.is_success(), "2 oauth2: {}", s);

        // 3. Register runner
        let (s, reg) = try_req(
            &app,
            Method::POST,
            "/api/v1/runners",
            json!({"name":"test-runner","labels":["self-hosted","linux","x64"]}),
        )
        .await;
        assert!(s.is_success(), "3 register: {} body={}", s, reg);
        let runner_id = reg["id"].as_i64().unwrap();

        // 4. Create session
        let (s, sess) = try_req(
            &app,
            Method::POST,
            "/api/v1/runners/sessions",
            json!({"runner_id": runner_id, "name": "test-runner"}),
        )
        .await;
        assert!(s.is_success(), "4 session: {} body={}", s, sess);
        let session_id = sess["sessionId"].as_str().unwrap().to_owned();

        // 5. Submit a workflow
        let (s, accepted) = try_req(&app, Method::POST, "/api/v1/runs",
            json!({"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n","event":"push","repository":"owner/repo"})).await;
        assert!(s.is_success(), "5 submit: {} body={}", s, accepted);
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // 6. Poll for messages — the runner uses the AzDO Message endpoint
        let (s, msg) = try_req(
            &app,
            Method::GET,
            &format!(
                "/api/v1/runners/sessions/{}/messages?sessionId={}&waitSeconds=0",
                session_id, session_id
            ),
            Value::Null,
        )
        .await;
        assert!(s.is_success(), "6 poll: {} body={}", s, msg);

        // 7. Get the job from the run
        let inner = state.inner.lock().await;
        let run_record = inner.runs.get(&run_id).unwrap();
        let job_id = run_record.jobs.keys().next().unwrap().clone();
        drop(inner);

        // 8. Complete the job
        let (s, _) = try_req(
            &app,
            Method::POST,
            "/api/v1/jobs/complete",
            json!({"run_id": run_id, "job_id": job_id, "status": "success"}),
        )
        .await;
        assert!(s.is_success(), "8 complete: {}", s);

        // 9. Verify run succeeded
        let (_, final_run) = try_req(
            &app,
            Method::GET,
            &format!("/api/v1/runs/{}", run_id),
            Value::Null,
        )
        .await;
        assert_eq!(final_run["status"], "success");
    }

    async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
        let mut builder = Request::builder().method(method).uri(uri);
        if uri.starts_with("/_apis/")
            || uri.starts_with("/runner/server/_apis/")
            || uri.starts_with("/broker/")
            || uri.starts_with("/twirp/")
        {
            builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
        } else if uri.starts_with("/api/v3/actions/runner-registration") {
            builder = builder.header(header::AUTHORIZATION, "RemoteAuth aksh-registration-token");
        }
        let request = if body.is_null() {
            builder.body(Body::empty()).unwrap()
        } else {
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = app.clone().oneshot(request).await.unwrap();
        assert!(
            response.status().is_success(),
            "unexpected status: {}",
            response.status()
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        }
    }

    #[tokio::test]
    async fn job_timeout_enforcement_cancels_job() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shutdown = CancellationToken::new();
        let app = app(state.clone(), shutdown.clone());
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown,
        });

        // 1. Submit run
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // 2. Poll to start job (transitions status to InProgress and sets started_at)
        let _msg = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;

        let request_id = {
            let inner = state.inner.lock().await;
            *inner.job_requests.keys().next().unwrap()
        };

        // 3. Override started_at to be in the past (beyond 360m/21600s default timeout)
        {
            let mut inner = state.inner.lock().await;
            let request = inner.job_requests.get_mut(&request_id).unwrap();
            request.started_at = Some(SystemTime::now() - Duration::from_secs(22000));
        }

        // 4. Run reaper tick
        reap_once(&shared).await;

        // 5. Verify cancellation is enqueued
        {
            let inner = state.inner.lock().await;
            let request = inner.job_requests.get(&request_id).unwrap();
            assert!(request.timeout_triggered);
            assert_eq!(inner.cancellation_queue.len(), 1);
            assert_eq!(inner.cancellation_queue[0].run_id, run_id);
        }
    }

    #[tokio::test]
    async fn runner_lease_expiration_disconnect_reaper() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shutdown = CancellationToken::new();
        let app = app(state.clone(), shutdown.clone());
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown,
        });

        // 1. Submit run
        let accepted = request_json(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: sleep 10\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // 2. Poll to start job (sets last_renewed_at)
        let _msg = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/v1/Message/1?sessionId=default",
            Value::Null,
        )
        .await;

        let request_id = {
            let inner = state.inner.lock().await;
            *inner.job_requests.keys().next().unwrap()
        };

        // 3. Override last_renewed_at to be in the past (beyond 120s threshold)
        {
            let mut inner = state.inner.lock().await;
            let request = inner.job_requests.get_mut(&request_id).unwrap();
            request.last_renewed_at = Some(SystemTime::now() - Duration::from_secs(130));
        }

        // 4. Run reaper tick
        reap_once(&shared).await;

        // 5. Verify the job was marked failed and run completes as failed
        {
            let inner = state.inner.lock().await;
            let request = inner.job_requests.get(&request_id).unwrap();
            assert_eq!(request.result, Some(ExecutionStatus::Failure));
            assert!(inner.inflight_requests.is_empty());
            assert!(inner.session_active_requests.is_empty());

            let run = inner.runs.get(&run_id).unwrap();
            assert_eq!(run.status, ExecutionStatus::Failure);
        }
    }

    #[tokio::test]
    async fn github_webhook_flows_with_signature_and_check_runs() {
        let temp = tempfile::tempdir().unwrap();

        // 1. Create a dummy workflow file in a local workspace
        let ws_dir = temp.path().join("workspace");
        tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
            .await
            .unwrap();
        let workflow_content = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
        tokio::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow_content)
            .await
            .unwrap();

        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());

        assert_eq!(state.webhook_secret.as_deref(), Some("super-secret"));
        assert_eq!(state.local_workspace.as_ref(), Some(&ws_dir));

        let app = app(state.clone(), CancellationToken::new());

        // 2. Prepare mock webhook push payload
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "commits": [
                {
                    "id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                    "added": ["src/main.rs"],
                    "modified": [],
                    "removed": []
                }
            ]
        });

        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        // 3. Compute correct signature
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(&payload_bytes);
        let sig_bytes = mac.finalize().into_bytes();
        let sig_hex = sig_bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let signature_header = format!("sha256={}", sig_hex);

        // 4. Send request with WRONG signature -> should fail with 401
        let response_401 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/github/webhooks")
                    .header("x-github-event", "push")
                    .header("x-hub-signature-256", "sha256=invalid")
                    .header("content-type", "application/json")
                    .body(Body::from(payload_bytes.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_401.status(), StatusCode::UNAUTHORIZED);

        // 5. Send request with CORRECT signature -> should succeed with 200
        let response_200 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/github/webhooks")
                    .header("x-github-event", "push")
                    .header("x-hub-signature-256", signature_header)
                    .header("content-type", "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_200.status(), StatusCode::OK);

        // 6. Verify that a run was triggered and check runs are queued
        let inner = state.inner.lock().await;
        assert_eq!(inner.runs.len(), 1);
        let (_, run_record) = inner.runs.iter().next().unwrap();
        assert_eq!(run_record.submission.event, "push");
        assert_eq!(run_record.submission.repository, "owner/repo");
        assert_eq!(run_record.submission.git_ref, "refs/heads/main");

        // Verify that check_run_ids are created/queued in the record
        assert_eq!(run_record.job_check_run_ids.len(), 1);
        let (job_id, check_run_id) = run_record.job_check_run_ids.iter().next().unwrap();
        assert_eq!(job_id.to_string(), "build");
        assert!(*check_run_id > 0);
    }

    #[tokio::test]
    async fn github_webhook_pull_request_event() {
        let temp = tempfile::tempdir().unwrap();

        // Create a dummy workflow file in a local workspace
        let ws_dir = temp.path().join("workspace");
        tokio::fs::create_dir_all(ws_dir.join(".github/workflows"))
            .await
            .unwrap();
        let workflow_content = r#"
on: pull_request
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: make test
"#;
        tokio::fs::write(ws_dir.join(".github/workflows/test.yml"), workflow_content)
            .await
            .unwrap();

        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.webhook_secret = Some("super-secret".to_owned());
        state.local_workspace = Some(ws_dir.clone());

        let app = app(state.clone(), CancellationToken::new());

        // Prepare PR payload
        let payload = serde_json::json!({
            "action": "opened",
            "number": 42,
            "pull_request": {
                "head": {
                    "ref": "feature-branch",
                    "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3"
                },
                "base": {
                    "ref": "main",
                    "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2"
                }
            },
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            }
        });

        let payload_bytes = serde_json::to_vec(&payload).unwrap();

        // Compute signature
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"super-secret").unwrap();
        mac.update(&payload_bytes);
        let sig_bytes = mac.finalize().into_bytes();
        let sig_hex = sig_bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let signature_header = format!("sha256={}", sig_hex);

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/github/webhooks")
                    .header("x-github-event", "pull_request")
                    .header("x-hub-signature-256", signature_header)
                    .header("content-type", "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify triggered run
        let inner = state.inner.lock().await;
        assert_eq!(inner.runs.len(), 1);
        let (_, run_record) = inner.runs.iter().next().unwrap();
        assert_eq!(run_record.submission.event, "pull_request");
        assert_eq!(run_record.submission.git_ref, "refs/pull/42/merge");
        assert_eq!(run_record.job_check_run_ids.len(), 1);
    }

    #[tokio::test]
    async fn github_app_manifest_registration_flow() {
        let temp = tempfile::tempdir().unwrap();

        // 1. Setup a local mock GitHub API server for manifest conversion
        let mock_app = Router::new().route(
            "/app-manifests/:code/conversions",
            post(|Path(code): Path<String>| async move {
                assert_eq!(code, "mock_code_123");
                Json(json!({
                    "id": 987654,
                    "pem": "-----BEGIN RSA PRIVATE KEY-----\nMOCK-KEY-DATA\n-----END RSA PRIVATE KEY-----",
                    "webhook_secret": Some("mock-webhook-secret-xyz")
                }))
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, mock_app).await.unwrap();
        });

        // 2. Configure mock API URL in environment
        std::env::set_var("AKSH_GITHUB_API_URL", format!("http://127.0.0.1:{}", port));

        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        // 3. Request registration form (GET /api/v1/github/register)
        let response_reg = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/v1/github/register")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_reg.status(), StatusCode::OK);
        let bytes = to_bytes(response_reg.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(html.contains("https://github.com/settings/apps/new"));
        assert!(html.contains("aksh-local-app"));

        // 4. Request callback conversion (GET /api/v1/github/callback?code=mock_code_123)
        let response_callback = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/api/v1/github/callback?code=mock_code_123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response_callback.status(), StatusCode::OK);
        let bytes_callback = to_bytes(response_callback.into_body(), usize::MAX)
            .await
            .unwrap();
        let html_callback = String::from_utf8(bytes_callback.to_vec()).unwrap();
        assert!(html_callback.contains("GitHub App Registered Successfully!"));
        assert!(html_callback.contains("987654"));
        assert!(html_callback.contains("mock-webhook-secret-xyz"));

        // Clean up
        std::env::remove_var("AKSH_GITHUB_API_URL");
    }

    #[test]
    fn label_matching_exact() {
        assert!(job_matches_runner(
            &["self-hosted".into(), "Linux".into()],
            &["self-hosted".into(), "Linux".into(), "X64".into()]
        ));
    }

    #[test]
    fn label_matching_case_insensitive() {
        assert!(job_matches_runner(
            &["Self-Hosted".into(), "linux".into()],
            &["self-hosted".into(), "Linux".into()]
        ));
    }

    #[test]
    fn label_matching_ubuntu_alias() {
        // ubuntu-latest should match a runner with "self-hosted"
        assert!(job_matches_runner(
            &["ubuntu-latest".into()],
            &["self-hosted".into(), "Linux".into()]
        ));
        // Also matches via the "linux" label
        assert!(job_matches_runner(
            &["ubuntu-24.04".into()],
            &["linux".into()]
        ));
    }

    #[test]
    fn label_matching_rejects_missing_labels() {
        // Runner missing "gpu" label
        assert!(!job_matches_runner(
            &["self-hosted".into(), "gpu".into()],
            &["self-hosted".into(), "Linux".into()]
        ));
    }

    #[test]
    fn label_matching_empty_runner_matches_all() {
        // Unknown runner (empty labels) matches everything
        assert!(job_matches_runner(
            &["self-hosted".into(), "Linux".into()],
            &[]
        ));
    }

    #[test]
    fn label_matching_empty_job_matches_all() {
        assert!(job_matches_runner(&[], &["self-hosted".into()]));
    }
}
