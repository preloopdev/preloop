//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use aksh_artifacts::ArtifactStore;
use aksh_cache::CacheStore;
use aksh_gha_parser::{expand_jobs_with_reusables, parse_workflow};
use aksh_gha_protocol::{
    self as protocol,
    azdo::{
        self, ConnectionData, EncryptionKey as AzdoEncryptionKey, LocationServiceData,
        ServiceDefinition, TaskAgentSession as AzdoSession,
    },
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson, AnnotationLevel, ExecutionStatus, JobCompletion, JobId, NdjsonEvent,
    RegisteredRunner, RunAccepted, RunId, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind.
    pub listen: SocketAddr,
    /// State directory for cache/artifacts and future durable state.
    pub state_dir: PathBuf,
}

/// Start the server and block until shutdown.
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let state = AppState::new(config.state_dir).await?;
    let shutdown = CancellationToken::new();
    let router = app(state, shutdown.clone());
    let listener = TcpListener::bind(config.listen).await?;

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
            post(register_runner),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions",
            post(create_session),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions/:session_id",
            delete(delete_session),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages",
            get(next_message),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:run_id/jobs/:job_id",
            patch(complete_job_compat),
        )
        .route(
            "/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(create_log),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id/:log_id2",
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
        .route_layer(middleware::from_fn(require_bearer));

    Router::new()
        .route("/healthz", get(healthz))
        // GHES-style org-prefixed routes
        .route("/:org/_apis/connectionData", get(connection_data))
        .route("/:org/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/:org/_apis/v1/AgentPools", get(runner_pools))
        .route("/:org/_apis/v1/Agent/:pool_id/:agent_id", get(agent_lookup_by_id_org).post(register_runner_compat_org_2))
        .route("/:org/_apis/v1/Agent/:pool_id", get(agent_lookup_org).post(register_runner_compat_org))
        .route("/:org/_apis/v1/AgentSession/:pool_id/:session_id", post(create_session_compat_org))
        .route("/:org/_apis/v1/AgentSession/:pool_id", post(create_session_compat_org_pool_only))
        .route("/:org/_apis/v1/AgentSession/:pool_id/:session_id", delete(delete_session_org))
        .route("/:org/_apis/v1/Message/:pool_id", get(next_message_compat_org))
        .route("/:org/_apis/v1/Message/:pool_id/:message_id", delete(delete_pool_message_org))
        .route("/:org/_apis/v1/AgentRequest/:pool_id/:request_id", patch(agent_request_patch_org))
        .route("/:org/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id", patch(patch_timeline_records_org))
        .route("/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id", post(create_log_org))
        .route("/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id/:log_id2", post(append_log_org))
        .route("/:org/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id", post(console_log_org))
        .route("/:org/_apis/v1/FinishJob/:scope/:hub/:plan_id", post(finish_job_org))
        .route("/:org/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id", post(action_download_info_org))
        .route("/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/api/v3/actions/runner-registration", post(github_registration_token))
        .route("/api/v3/orgs/:org/actions/runners/registration-token", post(github_registration_token))
        .route("/api/v3/repos/:owner/:repo/actions/runners/registration-token", post(github_registration_token))
        .route("/runner/server/_apis/connectionData", get(connection_data))
        .route("/_apis/connectionData", get(connection_data))
        .route("/_apis/", axum::routing::options(|| async { StatusCode::OK }))
        .route("/api/v1/runs", post(submit_run))
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
        .route("/api/v1/jobs/complete", post(complete_job))
        .route("/api/v1/cache", post(cache_put))
        .route("/api/v1/cache", get(cache_get))
        .route("/api/v1/artifacts", post(artifact_put))
        .route("/api/v1/artifacts/:artifact_id", get(artifact_get))
        // Runner lifecycle endpoints — public (runner may not have auth token yet)
        .route("/_apis/v1/AgentPools", get(runner_pools))
        .route("/_apis/v1/Agent/:pool_id/:agent_id", get(agent_lookup_by_id).post(register_runner_compat))
        .route("/_apis/v1/AgentSession/:pool_id/:session_id", post(create_session_compat))
        .route("/_apis/v1/AgentSession/:pool_id", post(create_session_compat_pool_only))
        .route("/_apis/v1/AgentSession/:pool_id/:session_id", delete(delete_session))
        .route("/_apis/v1/Message/:pool_id", get(next_message_compat))
        .route("/_apis/v1/Message/:pool_id/:message_id", delete(delete_pool_message))
        .route("/_apis/v1/AgentRequest/:pool_id/:request_id", patch(agent_request_patch))
        .merge(protected_apis)
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(SharedState { state, shutdown }))
}

async fn require_bearer(request: Request, next: Next) -> Result<Response, ApiError> {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| token == "aksh-system-token" || token.starts_with("aksh-"));
    if authorized {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::unauthorized("missing or invalid bearer token"))
    }
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
    #[allow(dead_code)]
    cache: CacheStore,
    #[allow(dead_code)]
    artifacts: ArtifactStore,
}

impl AppState {
    /// Build state rooted in a state directory.
    pub async fn new(state_dir: PathBuf) -> anyhow::Result<Self> {
        let cache = CacheStore::new(state_dir.join("cache")).await?;
        let artifacts = ArtifactStore::new(state_dir.join("artifacts")).await?;
        let (events, _) = broadcast::channel(1024);
        let keypair = AgentRsaKeypair::generate()
            .map_err(|e| anyhow::anyhow!("Failed to generate RSA keypair: {}", e))?;
        let mut inner = InnerState::default();
        inner.agent_keypair = Some(keypair);
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            events,
            message_notify: Arc::new(Notify::new()),
            cache,
            artifacts,
        })
    }

    async fn emit(&self, event: NdjsonEvent) {
        let _ = self.events.send(event);
    }
}

#[derive(Default)]
struct InnerState {
    runs: BTreeMap<RunId, RunRecord>,
    queue: VecDeque<QueuedJob>,
    /// Jobs waiting for their `needs` dependencies to complete.
    pending_jobs: VecDeque<QueuedJob>,
    runners: BTreeMap<i64, RegisteredRunner>,
    sessions: BTreeMap<String, RunnerSession>,
    session_keys: BTreeMap<String, SessionEncryption>,
    agent_keypair: Option<AgentRsaKeypair>,
    runner_public_keys: BTreeMap<i64, String>,
    runner_rsa_public_keys: BTreeMap<i64, AgentRsaPublicKey>,
    inflight_messages: BTreeMap<i64, azdo::TaskAgentMessage>,
    cancellation_queue: VecDeque<QueuedCancellation>,
    pending_caches: BTreeMap<i64, PendingCache>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    logs: BTreeMap<String, Vec<u8>>,
    timeline_events: BTreeMap<RunId, Vec<NdjsonEvent>>,
    next_runner_id: i64,
    next_cache_id: i64,
    next_message_id: i64,
}

#[derive(Debug, Clone, Serialize)]
struct RunRecord {
    run_id: RunId,
    submission: WorkflowSubmission,
    jobs: BTreeMap<JobId, ExecutionStatus>,
    status: ExecutionStatus,
    job_outputs: BTreeMap<JobId, BTreeMap<String, serde_json::Value>>,
    job_base_ids: BTreeMap<JobId, String>,
    job_fail_fast: BTreeMap<String, bool>,
}

#[derive(Debug, Clone)]
struct QueuedJob {
    run_id: RunId,
    job_id: JobId,
    base_id: String,
    needs: Vec<JobId>,
    fail_fast: bool,
    max_parallel: Option<u64>,
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

async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    Json(submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    let workflow = parse_workflow(&submission.workflow_yaml)?;
    let (branch, tag) = git_ref_context(&submission.git_ref);
    let changed_paths = changed_paths_from_payload(&submission.payload);
    let activity_type = submission.payload.get("action").and_then(|value| value.as_str());
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
            let agent_msg = aksh_gha_parser::job_builder::build_agent_job_message(
                &job,
                &github,
                &job.env,
                &submission.secrets.iter().map(|(k, v)| (k.clone(), v.expose().to_owned())).collect(),
                &submission.vars,
            ).map_err(|e| ApiError::bad_request(format!("failed to build job message: {e}")))?;

            let queued_job = QueuedJob {
                run_id,
                job_id: job.id.clone(),
                base_id: job.base_id.clone(),
                needs: job.needs.clone(),
                fail_fast: job.fail_fast,
                max_parallel: job.max_parallel,
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
        Ok(Json(RunAccepted {
            run_id,
            queued_jobs,
        }))
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
        inner.runner_public_keys.insert(runner_id, public_key.clone());
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
) -> Json<serde_json::Value> {
    let session_id = uuid::Uuid::new_v4();

    // Generate AES session key
    let session_enc = SessionEncryption::generate();

    // Send the AES key as base64 without RSA wrapping.
    // The runner's RSA public key parsing from registration is complex (XML format);
    // for now we send the key unencrypted and let the runner use it directly.
    let key_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &session_enc.key);

    // Store the session key for later message decryption
    {
        let mut inner = shared.state.inner.lock().await;
        inner.session_keys.insert(session_id.to_string(), session_enc);
    }

    info!(%session_id, "session created with AES key (unencrypted)");

    Json(json!({
        "sessionId": session_id.to_string(),
        "encryptionKey": {
            "value": key_b64,
            "encrypted": false
        }
    }))
}

async fn delete_session(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    inner.sessions.remove(&session_id);
    StatusCode::NO_CONTENT
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
        if let Some(message) = inner.inflight_messages.values().next().cloned() {
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

        let Some(queued) = inner.queue.pop_front() else {
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
            run.jobs.insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        let body_json = serde_json::to_string(&queued.message)
            .map_err(|e| ApiError::bad_request(format!("failed to serialize job message: {e}")))?;
        let message = build_task_agent_message(
            &mut inner,
            &session_id,
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
            body_json,
        )?;

        let run_id = queued.run_id;
        let job_id = queued.job_id.clone();
        drop(inner);

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
    Path((_session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, message_id).await
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
    inner.inflight_messages.insert(message_id, message.clone());
    Ok(message)
}

async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
) -> StatusCode {
    ack_message(shared, message_id).await
}

async fn ack_message(shared: Arc<SharedState>, message_id: i64) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    inner.inflight_messages.remove(&message_id);
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

/// PATCH /_apis/v1/AgentRequest/:pool_id/:request_id — renew or complete job request.
/// The runner sends this to renew the job lock or report completion.
async fn agent_request_patch(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, _request_id)): Path<(i64, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    info!(?body, "agent_request_patch received");
    // Check if this is a completion event
    if let Some(result) = body.get("result").and_then(|v| v.as_str()) {
        info!(result, "job request completed");
        let mut inner = shared.state.inner.lock().await;
        for run in inner.runs.values_mut() {
            for (job_id, status) in run.jobs.iter_mut() {
                if *status == ExecutionStatus::InProgress {
                    *status = match result {
                        "succeeded" => ExecutionStatus::Success,
                        "failed" => ExecutionStatus::Failure,
                        "cancelled" => ExecutionStatus::Cancelled,
                        _ => ExecutionStatus::Success,
                    };
                    info!(job_id = %job_id, result, "updated job status");
                }
            }
        }
    }
    // Return a valid TaskAgentJobRequest response
    Json(json!({
        "requestId": _request_id,
        "lockedUntil": "2099-12-31T23:59:59Z",
        "result": body.get("result"),
    }))
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
            completion.outputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        );
        run.status = summarize_run(run.jobs.values().copied());
    }
    if completion.status == ExecutionStatus::Failure {
        apply_matrix_fail_fast(&mut inner, completion.run_id, &completion.job_id);
    }
    let promoted_jobs = promote_ready_jobs(&mut inner);
    let record = inner
        .runs
        .get(&completion.run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    drop(inner);
    if promoted_jobs > 0 {
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

fn apply_matrix_fail_fast(inner: &mut InnerState, run_id: RunId, failed_job: &JobId) {
    let Some(run) = inner.runs.get_mut(&run_id) else {
        return;
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return;
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return;
    }

    for (job_id, status) in &mut run.jobs {
        if job_id != failed_job
            && run.job_base_ids.get(job_id) == Some(&base_id)
            && matches!(status, ExecutionStatus::Queued | ExecutionStatus::InProgress)
        {
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
    Json(records): Json<Vec<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    let run_id = plan_id.parse::<RunId>().ok();
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
                job_id: JobId(record.id.to_string()),
                status,
            });
        }
        if let Some(run_id) = run_id {
            for issue in &record.issues {
                projected.push(NdjsonEvent::Annotation {
                    run_id,
                    job_id: JobId(record.id.to_string()),
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
    Json(json!({ "ok": true }))
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
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
) -> Json<serde_json::Value> {
    let key = log_key(&plan_id, &log_id);
    let mut inner = shared.state.inner.lock().await;
    inner.logs.entry(key).or_default();
    Json(json!({ "ok": true }))
}

/// POST append log — runner appends lines to a log file.
async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id, _log_id2)): Path<(String, String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    let mut inner = shared.state.inner.lock().await;
    let masked = mask_log_bytes(&inner, &plan_id, &body);
    inner.logs.entry(key).or_default().extend_from_slice(&masked);
    StatusCode::ACCEPTED
}

fn log_key(plan_id: &str, log_id: &str) -> String {
    format!("{plan_id}/{log_id}")
}

fn mask_log_bytes(inner: &InnerState, plan_id: &str, body: &[u8]) -> Vec<u8> {
    let mut text = String::from_utf8_lossy(body).into_owned();
    let run_secrets = plan_id
        .parse::<RunId>()
        .ok()
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
    Path((_scope, _hub, _plan_id, _timeline_id, _record_id)): Path<(String, String, String, String, String)>,
    _body: Bytes,
) -> StatusCode {
    StatusCode::ACCEPTED
}

/// POST finish job — runner reports final result + outputs.
async fn finish_job(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Json<serde_json::Value> {
    let mut inner = shared.state.inner.lock().await;

    let status = match event.result {
        azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues => ExecutionStatus::Success,
        azdo::TaskResult::Failed => ExecutionStatus::Failure,
        azdo::TaskResult::Cancelled => ExecutionStatus::Cancelled,
        azdo::TaskResult::Skipped => ExecutionStatus::Skipped,
    };

    // Find the run and update job status
    let run_id = plan_id.parse::<RunId>().ok();
    let actual_run_id = if let Some(rid) = run_id {
        if let Some(run) = inner.runs.get_mut(&rid) {
            run.jobs.insert(JobId(event.job_id.to_string()), status);
            run.job_outputs.insert(
                JobId(event.job_id.to_string()),
                event
                    .outputs
                    .iter()
                    .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
                    .collect(),
            );
            run.status = summarize_run(run.jobs.values().copied());
            rid
        } else {
            RunId::new()
        }
    } else {
        RunId::new()
    };

    // Promote pending jobs whose dependencies are now met
    let promoted_jobs = promote_ready_jobs(&mut inner);

    info!(
        job_id = %event.job_id,
        result = ?event.result,
        outputs = ?event.outputs,
        "job completed"
    );

    drop(inner);
    if promoted_jobs > 0 {
        shared.state.message_notify.notify_waiters();
    }
    shared
        .state
        .emit(NdjsonEvent::JobCompleted {
            run_id: actual_run_id,
            job_id: JobId(event.job_id.to_string()),
            status,
            outputs: event.outputs,
        })
        .await;

    Json(json!({ "ok": true }))
}

/// POST action download info — resolve action references to download URLs.
async fn action_download_info(
    State(_shared): State<Arc<SharedState>>,
    Json(request): Json<serde_json::Value>,
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

async fn connection_data() -> axum::response::Response {
    let body = serde_json::json!({
        "instanceId": uuid::Uuid::new_v4().to_string(),
        "locationServiceData": {
            "serviceDefinitions": [
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
            ],
            "accessMappings": [{
                "moniker": "PublicAccessMapping",
                "displayName": "Default Access Mapping",
                "accessPoint": "http://127.0.0.1:9090"
            }],
            "defaultAccessMappingMoniker": "PublicAccessMapping"
        }
    });
    axum::response::Json(body).into_response()
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
        "locationMappings": [],
        "serviceOwner": "00000000-0000-0000-0000-000000000000",
        "resourceVersion": 6,
        "minVersion": "1.0",
        "maxVersion": "12.0",
        "status": 1,
        "properties": {}
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
    Path((_pool_id, agent_id)): Path<(i64, String)>,
    Json(mut request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // The runner sends a TaskAgent-style body; extract what we need.
    let name = request.get("name").and_then(|v| v.as_str()).unwrap_or("runner").to_owned();
    let labels: Vec<String> = request
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
        .unwrap_or_default();
    let ephemeral = request.get("ephemeral").and_then(|v| v.as_bool()).unwrap_or(false);
    let reg_request = RunnerRegistrationRequest {
        name: name.clone(),
        labels,
        ephemeral,
        public_key: request.get("publicKey").and_then(|v| v.as_str()).map(str::to_owned),
    };
    let result = register_runner(State(shared), Json(reg_request)).await?;
    Ok(Json(json!({
        "id": result.0.id,
        "name": result.0.name,
        "version": "2.322.0",
        "osDescription": "Linux",
        "enabled": true,
        "status": "online",
        "ephemeral": ephemeral,
        "labels": result.0.labels.iter().map(|l| json!({"name": l, "type": "user"})).collect::<Vec<_>>(),
        "authorization": {
            "authorizationUrl": "http://127.0.0.1:9090",
            "clientId": uuid::Uuid::new_v4().to_string(),
            "publicKey": {
                "exponent": "AQAB",
                "modulus": "x9DRhIzTYGvMcPEZDjc7cKrIyb+EBMNtB8riHXxElnskMQuMYNRe7Ya2WsS/dctBUSeqhegDZGKcuDM6aab8bsiJoua/hNLNKdxBSz33nsuKdZYXah8r4Z1UIQf4oan8Mo4ePqqDXXFXdTG0peWyVPqjL4VU9n/EG3JoaGcwOoLrcbT/jT2Pz2v6AquPEzaFjty0OWGQ2gRKahHS1UUAI7VKfKMvvUT1ANn6YPIZ7Jdl6YSFMDI2AFwKOwOVQB6E5bIY8W6jwANqt0vlyMbeqii58pSuto9aAEoLsdLxGGrFFxvxGScPG+scVYSkXyj4mrdS0qSm4Z/UOhtnese7OQ==",
                "keyId": uuid::Uuid::new_v4().to_string()
            }
        }
    })))
}

/// Compat handler: register runner via `/_apis/v1/Agent/:pool_id` (no agent_id in path).
async fn register_runner_compat_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat(State(shared), Path((_pool_id, "0".to_owned())), Json(request)).await
}

/// Compat handler: create session via AzDO AgentSession path.
async fn create_session_compat(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, session_id)): Path<(i64, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = body.get("agent").and_then(|a| a.get("id")).and_then(|v| v.as_i64()).unwrap_or(1);
    let name = body.get("agent").and_then(|a| a.get("name")).and_then(|v| v.as_str()).unwrap_or("runner").to_owned();
    let result = create_session(State(shared), Json(RunnerSessionRequest { runner_id, name })).await;
    Ok(result)
}

/// Compat handler: next message via AzDO Message path.
async fn next_message_compat(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    Query(mut params): Query<std::collections::HashMap<String, String>>,
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
) -> StatusCode {
    delete_pool_message(State(shared), Path((pool_id, message_id))).await
}

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
    Json(records): Json<Vec<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    patch_timeline_records(State(shared), Path((scope, hub, plan_id, timeline_id)), Json(records)).await
}

async fn create_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, log_id)): Path<(String, String, String, String, String)>,
) -> Json<serde_json::Value> {
    create_log(State(shared), Path((scope, hub, plan_id, log_id))).await
}

async fn append_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, log_id, log_id2)): Path<(String, String, String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    append_log(State(shared), Path((scope, hub, plan_id, log_id, log_id2)), body).await
}

async fn console_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id, record_id)): Path<(String, String, String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    console_log(State(shared), Path((scope, hub, plan_id, timeline_id, record_id)), body).await
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

    let token = format!("aksh-jwt-{}", uuid::Uuid::new_v4());
    let url_str = payload
        .get("url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1")
        .to_owned();
    Ok(Json(json!({
        "token": token,
        "token_schema": "OAuthAccessToken",
        "url": url_str,
        "use_v2_flow": false
    })))
}

#[derive(Deserialize)]
struct TokenRequest {
    #[allow(dead_code)]
    grant_type: String,
    #[allow(dead_code)]
    client_id: Option<String>,
    #[allow(dead_code)]
    client_secret: Option<String>,
}

#[derive(Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

async fn oauth2_token(Json(_req): Json<TokenRequest>) -> Json<TokenResponse> {
    let token = format!("aksh-{}", uuid::Uuid::new_v4());
    Json(TokenResponse {
        access_token: token,
        token_type: "bearer".to_owned(),
        expires_in: 3600,
    })
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
    let audience = query
        .audience
        .unwrap_or_else(|| "api://aksh".to_owned());
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
            json!([{
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
            }]),
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
    async fn log_append_persists_payload_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());

        request_json(
            &app,
            Method::POST,
            "/_apis/v1/Logfiles/scope/actions/plan-1/log-1",
            Value::Null,
        )
        .await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/_apis/v1/Logfiles/scope/actions/plan-1/log-1/append")
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
                    .uri(format!("/_apis/v1/Logfiles/scope/actions/{run_id}/log-1/append"))
                    .header(header::AUTHORIZATION, "Bearer aksh-system-token")
                    .body(Body::from("token=super-secret"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let inner = state.inner.lock().await;
        assert_eq!(
            inner.logs.get(&format!("{run_id}/log-1")).map(Vec::as_slice),
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
        assert_eq!(
            inner.runner_public_keys.get(&runner_id),
            Some(&public_key)
        );
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
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({"runner_id": runner_id, "name": "local"}),
        )
        .await;
        let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
        let encrypted = session["encryptionKey"]["encrypted"].as_bool().unwrap();
        assert!(!encrypted, "session key should be sent unencrypted for now");
        let key_bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();
        assert_eq!(key_bytes.len(), 32, "AES-256 key should be 32 bytes");
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
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(first["messageId"], 1);

        let redelivered = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default",
            Value::Null,
        )
        .await;
        assert_eq!(redelivered["messageId"], first["messageId"]);

        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri("/runner/server/_apis/distributedtask/pools/1/messages/1")
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
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default&waitSeconds=0",
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

        let job_message = request_json(
            &app,
            Method::GET,
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default",
            Value::Null,
        )
        .await;
        let message_id = job_message["messageId"].as_i64().unwrap();

        let ack = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::DELETE)
                    .uri(format!(
                        "/runner/server/_apis/distributedtask/pools/1/messages/{message_id}"
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
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default&waitSeconds=0",
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
                "/runner/server/_apis/distributedtask/pools/1/messages?sessionId=default&waitSeconds=2",
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
        let keypair = {
            let inner = state.inner.lock().await;
            inner.agent_keypair.clone().unwrap()
        };
        let app = app(state, CancellationToken::new());

        let session = request_json(
            &app,
            Method::POST,
            "/runner/server/_apis/distributedtask/pools/1/sessions",
            json!({"runner_id": 1, "name": "local"}),
        )
        .await;
        let session_id = session["sessionId"].as_str().unwrap();
        let key_b64 = session["encryptionKey"]["value"].as_str().unwrap();
        let aes_key = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, key_b64).unwrap();

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
            &format!("/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}"),
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
        let plaintext = SessionEncryption::from_key(aes_key).decrypt(&body, &iv).unwrap();
        let job: azdo::AgentJobRequestMessage = serde_json::from_slice(&plaintext).unwrap();

        assert_eq!(message["messageType"], azdo::message_type::PIPELINE_AGENT_JOB_REQUEST);
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
                    .body(Body::from(json!({
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
                    }).to_string()))
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
        async fn try_req(app: &Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
            let mut builder = Request::builder().method(method).uri(uri);
            if uri.starts_with("/_apis/") || uri.starts_with("/runner/server/_apis/") {
                builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
            }
            let request = if body.is_null() {
                builder.body(Body::empty()).unwrap()
            } else {
                builder.header("content-type", "application/json").body(Body::from(body.to_string())).unwrap()
            };
            let response = app.clone().oneshot(request).await.unwrap();
            let status = response.status();
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let val = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (status, val)
        }

        // 1. connectionData
        let (s, conn) = try_req(&app, Method::GET, "/runner/server/_apis/connectionData", Value::Null).await;
        assert!(s.is_success(), "1 connectionData: {}", s);
        assert!(conn["locationServiceData"]["serviceDefinitions"].is_array());

        // 2. OAuth token
        let (s, _) = try_req(&app, Method::POST, "/_apis/v1/oauth2/token",
            json!({"grant_type":"client_credentials","client_id":"t","client_secret":"t"})).await;
        assert!(s.is_success(), "2 oauth2: {}", s);

        // 3. Register runner
        let (s, reg) = try_req(&app, Method::POST, "/api/v1/runners",
            json!({"name":"test-runner","labels":["self-hosted","linux","x64"]})).await;
        assert!(s.is_success(), "3 register: {} body={}", s, reg);
        let runner_id = reg["id"].as_i64().unwrap();

        // 4. Create session
        let (s, sess) = try_req(&app, Method::POST, "/api/v1/runners/sessions",
            json!({"runner_id": runner_id, "name": "test-runner"})).await;
        assert!(s.is_success(), "4 session: {} body={}", s, sess);
        let session_id = sess["sessionId"].as_str().unwrap().to_owned();

        // 5. Submit a workflow
        let (s, accepted) = try_req(&app, Method::POST, "/api/v1/runs",
            json!({"workflow_yaml":"on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n","event":"push","repository":"owner/repo"})).await;
        assert!(s.is_success(), "5 submit: {} body={}", s, accepted);
        let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

        // 6. Poll for messages — the runner uses the AzDO Message endpoint
        let (s, msg) = try_req(&app, Method::GET,
            &format!("/api/v1/runners/sessions/{}/messages?sessionId={}&waitSeconds=0", session_id, session_id),
            Value::Null).await;
        assert!(s.is_success(), "6 poll: {} body={}", s, msg);

        // 7. Get the job from the run
        let inner = state.inner.lock().await;
        let run_record = inner.runs.get(&run_id).unwrap();
        let job_id = run_record.jobs.keys().next().unwrap().clone();
        drop(inner);

        // 8. Complete the job
        let (s, _) = try_req(&app, Method::POST, "/api/v1/jobs/complete",
            json!({"run_id": run_id, "job_id": job_id, "status": "success"})).await;
        assert!(s.is_success(), "8 complete: {}", s);

        // 9. Verify run succeeded
        let (_, final_run) = try_req(&app, Method::GET, &format!("/api/v1/runs/{}", run_id), Value::Null).await;
        assert_eq!(final_run["status"], "success");
    }

    async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
        let mut builder = Request::builder().method(method).uri(uri);
        if uri.starts_with("/_apis/") || uri.starts_with("/runner/server/_apis/") {
            builder = builder.header(header::AUTHORIZATION, "Bearer aksh-system-token");
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
}
