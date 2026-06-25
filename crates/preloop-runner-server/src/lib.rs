//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post};
use axum::{Json, Router};
use preloop_artifacts::ArtifactStore;
use preloop_cache::CacheStore;
use preloop_gha_parser::{expand_jobs, parse_workflow};
use preloop_gha_protocol::{
    event_to_ndjson, ExecutionStatus, JobCompletion, JobId, NdjsonEvent, RegisteredRunner,
    RunAccepted, RunId, RunnerJobMessage, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
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

    info!(listen = %config.listen, "preloop runner server listening");
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
        .route("/api/v1/runs", post(submit_run))
        .route("/api/v1/runs/:run_id", get(get_run))
        .route("/api/v1/runs/:run_id/cancel", post(cancel_run))
        .route("/api/v1/runs/:run_id/rerun", post(rerun_run))
        .route("/api/v1/runs/:run_id/events.ndjson", get(run_events))
        .route("/api/v1/runners", post(register_runner))
        .route("/api/v1/runners/sessions", post(create_session))
        .route("/api/v1/runners/sessions/:session_id/messages", get(next_message))
        .route("/api/v1/jobs/complete", post(complete_job))
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
        Ok(Self {
            inner: Arc::new(Mutex::new(InnerState::default())),
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
    runners: BTreeMap<i64, RegisteredRunner>,
    sessions: BTreeMap<String, RunnerSession>,
    next_runner_id: i64,
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
    message: RunnerJobMessage,
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
    let jobs = expand_jobs(&workflow)?;
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
            statuses.insert(job.id.clone(), ExecutionStatus::Queued);
            inner.queue.push_back(QueuedJob {
                run_id,
                message: RunnerJobMessage::new(run_id, job, github.clone()),
            });
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
            .emit(NdjsonEvent::RunAccepted { run_id, queued_jobs })
            .await;
        Ok(Json(RunAccepted { run_id, queued_jobs }))
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
            if matches!(*status, ExecutionStatus::Queued | ExecutionStatus::InProgress) {
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
    Json(request): Json<RunnerSessionRequest>,
) -> Json<RunnerSession> {
    let session = RunnerSession {
        session_id: preloop_gha_protocol::SessionId::new(),
        runner_id: request.runner_id,
    };
    let mut inner = shared.state.inner.lock().await;
    inner
        .sessions
        .insert(session.session_id.0.to_string(), session.clone());
    Json(session)
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
) -> Result<Json<Option<RunnerJobMessage>>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let Some(queued) = inner.queue.pop_front() else {
        return Ok(Json(None));
    };
    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
        run.status = ExecutionStatus::InProgress;
        run.jobs
            .insert(queued.message.job.id.clone(), ExecutionStatus::InProgress);
    }
    drop(inner);
    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id: queued.run_id,
            job_id: queued.message.job.id.clone(),
            status: ExecutionStatus::InProgress,
        })
        .await;
    Ok(Json(Some(queued.message)))
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

fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if statuses.iter().any(|status| *status == ExecutionStatus::Failure) {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else if statuses.iter().all(|status| {
        matches!(
            status,
            ExecutionStatus::Success | ExecutionStatus::Skipped
        )
    }) {
        ExecutionStatus::Success
    } else {
        ExecutionStatus::InProgress
    }
}

async fn connection_data() -> Json<serde_json::Value> {
    Json(json!({
        "locationServiceData": {},
        "authenticatedUser": {"providerDisplayName": "preloop"},
        "deploymentType": "preloop"
    }))
}

async fn runner_pools() -> Json<serde_json::Value> {
    Json(json!({
        "count": 1,
        "value": [{"id": 1, "name": "Default", "isHosted": false}]
    }))
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

impl From<preloop_gha_parser::ParserError> for ApiError {
    fn from(value: preloop_gha_parser::ParserError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl From<preloop_gha_protocol::ProtocolError> for ApiError {
    fn from(value: preloop_gha_protocol::ProtocolError) -> Self {
        Self::bad_request(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
