//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use aksh_artifacts::ArtifactStore;
use aksh_artifacts::ArtifactStore;
use aksh_cache::CacheStore;
use aksh_cache::CacheStore;
use aksh_gha_parser::{expand_jobs_with_reusables, parse_workflow};
use aksh_gha_parser::{expand_jobs_with_reusables, parse_workflow};
use aksh_gha_protocol::{
    self as protocol,
    azdo::{
        self, ConnectionData, EncryptionKey as AzdoEncryptionKey, LocationServiceData,
        ServiceDefinition, TaskAgentSession as AzdoSession,
    },
    crypto::{AgentRsaKeypair, SessionEncryption},
    event_to_ndjson, ExecutionStatus, JobCompletion, JobId, NdjsonEvent, RegisteredRunner,
    RunAccepted, RunId, RunnerRegistrationRequest, RunnerSession, RunnerSessionRequest,
    WorkflowSubmission, PROTOCOL_VERSION,
};
use aksh_gha_protocol::{
    event_to_ndjson, ExecutionStatus, JobCompletion, JobId, NdjsonEvent, RegisteredRunner,
    RunAccepted, RunId, RunnerJobMessage, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
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
    Router::new()
        .route("/healthz", get(healthz))
        .route("/_apis/v1/oauth2/token", post(oauth2_token))
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
        .route("/api/v1/jobs/complete", post(complete_job))
        .route("/api/v1/cache", post(cache_put))
        .route("/api/v1/cache", get(cache_get))
        .route("/api/v1/artifacts", post(artifact_put))
        .route("/api/v1/artifacts/:artifact_id", get(artifact_get))
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
        .route("/runner/server/_apis/connectionData", get(connection_data))
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
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:run_id/jobs/:job_id",
            patch(complete_job_compat),
        )
        // Phase E: Timeline, logs, completion
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
        .route("/_apis/v1/FinishJob/:scope/:hub/:plan_id", post(finish_job))
        // Phase H: Action download info
        .route(
            "/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(SharedState { state, shutdown }))
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
    pending_caches: BTreeMap<i64, PendingCache>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    next_runner_id: i64,
    next_cache_id: i64,
}

#[derive(Debug, Clone, Serialize)]
struct RunRecord {
    run_id: RunId,
    submission: WorkflowSubmission,
    jobs: BTreeMap<JobId, ExecutionStatus>,
    status: ExecutionStatus,
}

#[derive(Debug, Clone)]
struct QueuedJob {
    run_id: RunId,
    message: azdo::AgentJobRequestMessage,
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
    if !workflow.on.matches(&submission.event) {
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
        for job in jobs {
            let agent_msg = aksh_gha_parser::job_builder::build_agent_job_message(
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

            let queued_job = QueuedJob {
                run_id,
                message: agent_msg,
            };

            // Check if dependencies are met (no needs = ready immediately)
            if job.needs.is_empty() {
                statuses.insert(job.id.clone(), ExecutionStatus::Queued);
                inner.queue.push_back(queued_job);
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
                status: ExecutionStatus::Queued,
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
        Ok(Json(RunAccepted {
            run_id,
            queued_jobs,
        }))
    }
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
    {
        let record = inner
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        record.status = ExecutionStatus::Cancelled;
        for status in record.jobs.values_mut() {
            if matches!(
                *status,
                ExecutionStatus::Queued | ExecutionStatus::InProgress
            ) {
                *status = ExecutionStatus::Cancelled;
            }
        }
    }
    inner.queue.retain(|job| job.run_id != run_id);
    let record = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    drop(inner);
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
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from(out))
        .expect("static response builder"))
}

async fn register_runner(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<RunnerRegistrationRequest>,
) -> Json<RegisteredRunner> {
    let mut inner = shared.state.inner.lock().await;
    inner.next_runner_id += 1;
    let runner = RegisteredRunner {
        id: inner.next_runner_id,
        name: request.name,
        labels: request.labels,
        ephemeral: request.ephemeral,
    };
    inner.runners.insert(runner.id, runner.clone());
    Json(runner)
}

async fn create_session(
    State(shared): State<Arc<SharedState>>,
    Json(_request): Json<RunnerSessionRequest>,
) -> Json<AzdoSession> {
    let session_id = uuid::Uuid::new_v4();

    // Generate AES session key
    let session_enc = SessionEncryption::generate();

    // RSA-wrap the AES key with the agent's public key
    let wrapped_key = {
        let inner = shared.state.inner.lock().await;
        let keypair = inner
            .agent_keypair
            .as_ref()
            .expect("RSA keypair must be initialized");
        keypair
            .wrap_key(&session_enc.key)
            .expect("RSA wrap should not fail for valid key")
    };

    // Store the session key for later message decryption
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.to_string(), session_enc);
    }

    info!(%session_id, "session created with encrypted AES key");

    Json(AzdoSession {
        session_id,
        encryption_key: AzdoEncryptionKey {
            value: wrapped_key,
            encrypted: true,
        },
    })
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

    let mut inner = shared.state.inner.lock().await;
    let Some(queued) = inner.queue.pop_front() else {
        return Ok(Json(None));
    };

    // Update run status
    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
        run.status = ExecutionStatus::InProgress;
        run.jobs.insert(
            JobId(queued.message.job_id.to_string()),
            ExecutionStatus::InProgress,
        );
    }

    // Get the session's AES key for encryption
    let session_key = inner
        .session_keys
        .get(&session_id)
        .map(|s| s.key.clone())
        .unwrap_or_default();

    // Serialize the job message to JSON
    let body_json = serde_json::to_string(&queued.message)
        .map_err(|e| ApiError::bad_request(format!("failed to serialize job message: {e}")))?;

    // Encrypt with session AES key
    let (encrypted_body, iv) = if !session_key.is_empty() {
        let enc = SessionEncryption::from_key(session_key);
        enc.encrypt(body_json.as_bytes())
            .map_err(|e| ApiError::bad_request(format!("encryption failed: {e}")))?
    } else {
        // No encryption key — send plaintext (for testing)
        (body_json.into_bytes(), vec![0u8; 16])
    };

    drop(inner);

    let run_id = queued.run_id;
    let job_id = JobId(queued.message.job_id.to_string());

    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id,
            job_id,
            status: ExecutionStatus::InProgress,
        })
        .await;

    Ok(Json(Some(azdo::TaskAgentMessage {
        message_id: 1,
        message_type: "PipelineAgentJobRequest".to_owned(),
        body: base64::engine::general_purpose::STANDARD.encode(&encrypted_body),
        iv: Some(iv),
    })))
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

async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get_mut(&completion.run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    run.jobs
        .insert(completion.job_id.clone(), completion.status);
    run.status = summarize_run(run.jobs.values().copied());
    let record = run.clone();
    drop(inner);

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
fn promote_ready_jobs(inner: &mut InnerState) {
    let mut promoted = Vec::new();
    let mut remaining = VecDeque::new();

    while let Some(job) = inner.pending_jobs.pop_front() {
        // Get the job's needs from the message
        let needs_satisfied = {
            let run = inner.runs.get(&job.run_id);
            let job_id_str = job.message.job_id.to_string();
            // Find the base job id from the expanded id
            let base_id = job_id_str.split('[').next().unwrap_or(&job_id_str);

            if let Some(run) = run {
                // Check all needs are complete (success or skipped)
                run.jobs.iter().any(|(dep_id, status)| {
                    dep_id.0.starts_with(base_id)
                        && matches!(status, ExecutionStatus::Success | ExecutionStatus::Skipped)
                }) || run
                    .jobs
                    .values()
                    .all(|s| matches!(s, ExecutionStatus::Success | ExecutionStatus::Skipped))
            } else {
                false
            }
        };

        if needs_satisfied {
            promoted.push(job);
        } else {
            remaining.push_back(job);
        }
    }

    inner.pending_jobs = remaining;
    for job in promoted {
        inner.queue.push_back(job);
    }
}

// ─── Phase E: Timeline, logs, completion ────────────────────────────────────

/// PATCH timeline records — runner updates step/job state.
async fn patch_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, _plan_id, _timeline_id)): Path<(String, String, String, String)>,
    Json(records): Json<Vec<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    for record in &records {
        if let Some(state) = &record.state {
            info!(
                timeline_id = %_timeline_id,
                record_id = %record.id,
                name = record.display_name.as_deref().unwrap_or(""),
                state = ?state,
                "timeline record update"
            );
        }
    }
    Json(json!({ "ok": true }))
}

/// POST create log file — runner creates a log container.
async fn create_log(
    State(_shared): State<Arc<SharedState>>,
    Path((_scope, _hub, _plan_id, _log_id)): Path<(String, String, String, String)>,
) -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

/// POST append log — runner appends lines to a log file.
async fn append_log(
    State(_shared): State<Arc<SharedState>>,
    Path((_scope, _hub, _plan_id, _log_id, _log_id2)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    _body: Bytes,
) -> StatusCode {
    StatusCode::ACCEPTED
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
        azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues => {
            ExecutionStatus::Success
        }
        azdo::TaskResult::Failed => ExecutionStatus::Failure,
        azdo::TaskResult::Cancelled => ExecutionStatus::Cancelled,
        azdo::TaskResult::Skipped => ExecutionStatus::Skipped,
    };

    // Find the run and update job status
    let run_id = plan_id.parse::<RunId>().ok();
    let actual_run_id = if let Some(rid) = run_id {
        if let Some(run) = inner.runs.get_mut(&rid) {
            run.jobs.insert(JobId(event.job_id.to_string()), status);
            run.status = summarize_run(run.jobs.values().copied());
            rid
        } else {
            RunId::new()
        }
    } else {
        RunId::new()
    };

    // Promote pending jobs whose dependencies are now met
    promote_ready_jobs(&mut inner);

    info!(
        job_id = %event.job_id,
        result = ?event.result,
        outputs = ?event.outputs,
        "job completed"
    );

    drop(inner);
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

async fn connection_data() -> Json<ConnectionData> {
    Json(real_connection_data())
}

fn real_connection_data() -> ConnectionData {
    ConnectionData {
        location_service_data: Some(LocationServiceData {
            service_definitions: vec![
                svc_def("AgentPools", "a8c47e17-4d56-4a56-92bb-de7ea7dc65be", "/_apis/v1/AgentPools"),
                svc_def("Agent", "e298ef32-5878-4cab-993c-043836571f42", "/_apis/v1/Agent/{poolId}/{agentId}"),
                svc_def("AgentSession", "134e239e-2df3-4794-a6f6-24f1f19ec8dc", "/_apis/v1/AgentSession/{poolId}/{sessionId}"),
                svc_def("Message", "c3a054f6-7a8a-49c0-944e-3a8e5d7adfd7", "/_apis/v1/Message/{poolId}/{messageId}"),
                svc_def("AgentRequest", "fc825784-c92a-4299-9221-998a02d1b54f", "/_apis/v1/AgentRequest/{poolId}/{requestId}"),
                svc_def("ActionDownloadInfo", "27d7f831-88c1-4719-8ca1-6a061dad90eb", "/_apis/v1/ActionDownloadInfo/{scopeIdentifier}/{hubName}/{planId}"),
                svc_def("TimeLineWebConsoleLog", "858983e4-19bd-4c5e-864c-507b59b58b12", "/_apis/v1/TimeLineWebConsoleLog/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/{recordId}"),
                svc_def("TimelineRecords", "8893bc5b-35b2-4be7-83cb-99e683551db4", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}"),
                svc_def("Logfiles", "46f5667d-263a-4684-91b1-dff7fdcf64e2", "/_apis/v1/Logfiles/{scopeIdentifier}/{hubName}/{planId}/{logId}"),
                svc_def("FinishJob", "557624af-b29e-4c20-8ab0-0399d2204f3f", "/_apis/v1/FinishJob/{scopeIdentifier}/{hubName}/{planId}"),
                svc_def("Artifact", "85023071-bd5e-4438-89b0-2a5bf362a19d", "/_apis/pipelines/workflows/{runId}/artifacts"),
                svc_def("ArtifactFileContainer", "e4f5c81e-e250-447b-9fef-bd48471bea5e", "/_apis/pipelines/workflows/container/{containerId}"),
                svc_def("TimelineAttachments", "7898f959-9cdf-4096-b29e-7f293031629e", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/{timelineId}/attachments/{recordId}/{type}/{name}"),
                svc_def("Timeline", "83597576-cc2c-453c-bea6-2882ae6a1653", "/_apis/v1/Timeline/{scopeIdentifier}/{hubName}/{planId}/timeline/{timelineId}"),
                svc_def("CustomerIntelligence", "b5cc35c2-ff2b-491d-a085-24b6e9f396fd", "/_apis/v1/tasks"),
                svc_def("Tasks", "60aac929-f0cd-4bc8-9ce4-6b30e8f1b1bd", "/_apis/v1/tasks/{taskId}/{versionString}"),
                svc_def("Cache", "a7c78d38-31a8-417e-ba6b-7e58b352f304", "_apis/artifactcache"),
                svc_def("BuildArtifacts", "1db06c96-014e-44e1-ac91-90b2d4b3e984", "_apis/pipelines/workflows/{buildId}/artifacts"),
            ],
        }),
    }
}

fn svc_def(name: &str, id: &str, location: &str) -> ServiceDefinition {
    ServiceDefinition {
        identifier: Some(id.to_owned()),
        location_mapping: Some(BTreeMap::from([("".to_owned(), location.to_owned())])),
        display_name: Some(name.to_owned()),
    }
}

async fn runner_pools() -> Json<serde_json::Value> {
    Json(json!({
        "count": 1,
        "value": [{"id": 1, "name": "Default", "isHosted": false}]
    }))
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

    async fn request_json(app: &Router, method: Method, uri: &str, body: Value) -> Value {
        let request = if method == Method::GET {
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap()
        } else {
            Request::builder()
                .method(method)
                .uri(uri)
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
