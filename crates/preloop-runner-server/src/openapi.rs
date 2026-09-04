use utoipa::{
    openapi::{
        security::{Http, HttpAuthScheme, SecurityScheme},
        Components,
    },
    Modify, OpenApi, ToSchema,
};

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

/// Native API error envelope.
#[derive(Debug, ToSchema)]
pub(crate) struct ApiErrorResponse {
    /// Human-readable error message.
    pub error: String,
}

/// Result returned after accepting a workflow run.
#[derive(Debug, ToSchema)]
pub(crate) struct RunAcceptedResponse {
    /// UUID of the created run.
    pub run_id: String,
    /// Monotonic run number for this workflow path.
    pub run_number: u64,
    /// Number of expanded jobs queued for runners.
    pub queued_jobs: usize,
}

/// Workflow submission request.
///
/// Only `workflow_yaml`, `event`, and `repository` are required.
/// All other fields have sensible defaults.
#[derive(Debug, ToSchema)]
pub(crate) struct WorkflowSubmissionRequest {
    /// Raw YAML workflow contents.
    pub workflow_yaml: String,
    /// GitHub event name (e.g. `push`, `workflow_dispatch`).
    pub event: String,
    /// Repository slug (e.g. `owner/repo`).
    pub repository: String,
    /// Git ref for the run. Defaults to `refs/heads/main`.
    #[schema(default = "refs/heads/main")]
    pub git_ref: Option<String>,
    /// Event payload JSON.
    pub payload: Option<JsonValue>,
    /// Repository-relative workflow file path.
    pub workflow_path: Option<String>,
    /// Caller-provided variables.
    pub vars: Option<std::collections::BTreeMap<String, String>>,
    /// Workflow dispatch or call inputs.
    pub inputs: Option<std::collections::BTreeMap<String, JsonValue>>,
    /// Caller-provided secrets (write-only; values are redacted in responses).
    pub secrets: Option<std::collections::BTreeMap<String, String>>,
    /// Local reusable workflow YAML keyed by repository-relative path.
    pub reusable_workflows: Option<std::collections::BTreeMap<String, String>>,
    /// Resolved commit SHA for each remote reusable workflow reference.
    pub reusable_workflow_shas: Option<std::collections::BTreeMap<String, String>>,
    /// Enable DAP debugger for the run's jobs.
    pub enable_debugger: Option<bool>,
    /// Welcome message shown when the debugger attaches.
    pub debugger_welcome_message: Option<String>,
    /// Commit SHA for the run. Defaults to zeroes.
    pub sha: Option<String>,
    /// Actor (user) who initiated the run. Defaults to `aksh-system`.
    pub actor: Option<String>,
    /// Deployment environment name (for OIDC `sub` claim).
    pub environment: Option<String>,
    /// Run only these jobs (by YAML key) and their transitive `needs:` deps.
    pub selected_jobs: Option<Vec<String>>,
    /// Explicit base ref (populates `github.base_ref`).
    pub base_ref: Option<String>,
    /// Keep the failed job VM alive for interactive debugging.
    pub preserve_on_failure: Option<bool>,
}

/// Run status projection returned by GET/cancel/list endpoints.
///
/// The `submission` field contains the original submission with secret
/// values redacted to `<redacted>`.
#[derive(Debug, ToSchema)]
pub(crate) struct RunResponse {
    /// Run UUID.
    pub run_id: String,
    /// Evaluated run name (from `run-name:` in the workflow).
    pub run_name: Option<String>,
    /// Original submission (secrets redacted).
    pub submission: JsonValue,
    /// Map of job id → execution status string.
    pub jobs: std::collections::BTreeMap<String, String>,
    /// Aggregate run status.
    pub status: String,
    /// Per-job outputs.
    pub job_outputs:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, JsonValue>>,
    /// Map of expanded job id → base YAML job id.
    pub job_base_ids: std::collections::BTreeMap<String, String>,
    /// Per-base-job fail-fast flag.
    pub job_fail_fast: std::collections::BTreeMap<String, bool>,
    /// Per-job continue-on-error flag.
    pub job_continue_on_error: std::collections::BTreeMap<String, bool>,
    /// GitHub Check Run IDs (when GitHub App integration is active).
    pub job_check_run_ids: std::collections::BTreeMap<String, u64>,
    /// Reusable workflow call metadata.
    pub reusable_calls: JsonValue,
    /// Ordered list of job details.
    pub jobs_list: Vec<JsonValue>,
    /// When the run was created (ISO 8601).
    pub created_at: String,
    /// When the first job started (ISO 8601).
    pub started_at: Option<String>,
    /// When the run completed (ISO 8601).
    pub completed_at: Option<String>,
    /// Monotonic run number.
    pub run_number: u64,
    /// Run attempt (starts at 1, increments on rerun).
    pub run_attempt: u64,
    /// Workflow file path.
    pub workflow_path_str: String,
    /// Event that triggered the run.
    pub event: String,
    /// Final conclusion (success, failure, cancelled, skipped).
    pub conclusion: Option<String>,
}

// ---------------------------------------------------------------------------
// OpenAPI document
// ---------------------------------------------------------------------------

/// Native API document.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "preloop Native API",
        version = "0.2.0",
        description = "Native control-plane API for workflow submission, inspection, artifacts, cache, and debugging. Runner-compatible protocol routes (`/_apis/…`, `/broker/…`, `/twirp/…`) are intentionally excluded — they are governed by the official actions/runner protocol."
    ),
    tags(
        (name = "Health",         description = "Server health"),
        (name = "Runs",           description = "Workflow run lifecycle"),
        (name = "Debug Sessions", description = "Interactive debug session lifecycle (worker + controller)"),
        (name = "Agent Debug",    description = "Structured agent debugging surface"),
        (name = "Cache",          description = "Native cache API"),
        (name = "Artifacts",      description = "Native artifact API"),
        (name = "GitHub",         description = "GitHub App and webhook integration")
    ),
    paths(
        healthz,
        submit_run,
        list_runs,
        get_run,
        get_run_logs,
        live_run_logs,
        cancel_run,
        rerun_run,
        run_events,
        register_dap_port,
        dap_debug,
        worker_token,
        list_debug_sessions,
        open_debug_session,
        get_debug_session,
        poll_debug_verdict,
        post_debug_verdict,
        close_debug_session,
        acquire_debug_lease,
        release_debug_lease,
        debug_events,
        debug_operation,
        debug_audit,
        cache_get,
        cache_post,
        artifacts,
        artifact,
        github_webhook,
        workflow_dispatch_trigger,
        repository_dispatch_trigger,
        list_dispatch_workflows,
        list_dispatch_runs,
        github_register,
        github_callback,
        list_runners,
        readyz,
        status,
        metrics
    ),
    components(
        schemas(
            ApiErrorResponse,
            RunAcceptedResponse,
            WorkflowSubmissionRequest,
            RunResponse
        )
    ),
    modifiers(&SecuritySchemes)
)]
pub(crate) struct ApiDoc;

struct SecuritySchemes;

impl Modify for SecuritySchemes {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Components::new);
        components.add_security_scheme(
            "native_bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "System token (`PRELOOP_SYSTEM_TOKEN`, or a per-engine token generated \
                         and stored in the OS credential store or private engine.token file). \
                         Used by CLI clients, agents, and operators.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "github_dispatch_bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "GitHub-compatible dispatch credential: a GitHub App installation \
                         token (own App offline via mint ledger, any App online via GitHub \
                         round-trip), an own-App JWT, a PAT (`PRELOOP_GITHUB_TOKEN`), or the \
                         system token. Dispatches require the endpoint's write permission \
                         on the repository (`actions: write` for workflow dispatch, \
                         `contents: write` for repository dispatch); the read endpoints \
                         require repository read access.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "runner_bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Runner listen token. Issued at registration; used by the runner \
                         for broker and DAP operations.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "job_runtime_bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Job runtime token (`ACTIONS_RUNTIME_TOKEN`). Scoped to one job; \
                         exchanged for a debug-worker credential.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "debug_worker_bearer",
            SecurityScheme::Http(
                Http::builder()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "Debug-worker token. Obtained via POST /api/v1/debug/worker-token; \
                         authorizes worker-facing debug session operations.",
                    ))
                    .build(),
            ),
        );
    }
}

type JsonValue = serde_json::Value;

// ---------------------------------------------------------------------------
// Path stubs — utoipa generates OpenAPI metadata; actual handlers live
// in their respective modules.
// ---------------------------------------------------------------------------

// ── Health ──────────────────────────────────────────────────────────────────

/// Server health check.
#[utoipa::path(
    get, path = "/healthz", tag = "Health",
    responses((status = 200, description = "Server is healthy", body = JsonValue))
)]
fn healthz() {}

/// Server readiness check (public, reason codes on 503).
#[utoipa::path(
    get, path = "/readyz", tag = "Health",
    responses(
        (status = 200, description = "Ready", body = JsonValue),
        (status = 503, description = "Not ready", body = JsonValue)
    )
)]
fn readyz() {}

/// Operational status snapshot (native bearer required).
#[utoipa::path(
    get, path = "/api/v1/status", tag = "Health",
    responses(
        (status = 200, description = "Operational snapshot", body = JsonValue),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn status() {}

/// Prometheus metrics (native bearer required).
#[utoipa::path(
    get, path = "/metrics", tag = "Health",
    responses(
        (status = 200, description = "Prometheus text", content_type = "text/plain", body = String),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn metrics() {}

// ── Runs ────────────────────────────────────────────────────────────────────

/// Submit a workflow run.
#[utoipa::path(
    post, path = "/api/v1/runs", tag = "Runs",
    request_body = WorkflowSubmissionRequest,
    responses(
        (status = 202, description = "Run accepted", body = RunAcceptedResponse),
        (status = 400, description = "Invalid submission", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn submit_run() {}

/// List workflow runs.
#[utoipa::path(
    get, path = "/api/v1/runs", tag = "Runs",
    params(
        ("limit" = Option<usize>, Query, description = "Maximum runs to return"),
        ("status" = Option<String>, Query, description = "Filter by status")
    ),
    responses(
        (status = 200, description = "Array of runs", body = [RunResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn list_runs() {}

/// Get a single workflow run.
#[utoipa::path(
    get, path = "/api/v1/runs/{run_id}", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    responses(
        (status = 200, description = "Run details", body = RunResponse),
        (status = 404, description = "Run not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn get_run() {}

/// Get run logs as plain text, optionally narrowed to one job or step.
#[utoipa::path(
    get, path = "/api/v1/runs/{run_id}/logs", tag = "Runs",
    params(
        ("run_id" = String, Path, description = "Run UUID"),
        ("job" = Option<String>, Query, description = "Workflow job key or agent job UUID; omit for every job"),
        ("step" = Option<usize>, Query, description = "1-based step index within the job, in execution order")
    ),
    responses(
        (status = 200, content_type = "text/plain", description = "Merged log text", body = String),
        (status = 400, description = "`step` given without `job` in a multi-job run", body = ApiErrorResponse),
        (status = 404, description = "Run, job, or step not found", body = ApiErrorResponse),
        (status = 409, description = "Job reported one merged log, which has no step boundaries", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]

fn get_run_logs() {}

/// Follow one job's live console output as server-sent events.
#[utoipa::path(
    get, path = "/api/v1/runs/{run_id}/logs/live", tag = "Runs",
    params(
        ("run_id" = String, Path, description = "Run UUID"),
        ("job" = Option<String>, Query, description = "Workflow job key or agent job UUID; required when the run has multiple jobs")
    ),
    responses(
        (status = 200, content_type = "text/event-stream", description = "Live log events; the stream closes when the selected job is terminal", body = String),
        (status = 400, description = "Job is required when the run has multiple jobs", body = ApiErrorResponse),
        (status = 404, description = "Run or job not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn live_run_logs() {}

/// Cancel a running workflow.
#[utoipa::path(
    post, path = "/api/v1/runs/{run_id}/cancel", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    responses(
        (status = 200, description = "Run cancelled", body = RunResponse),
        (status = 404, description = "Run not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn cancel_run() {}

/// Rerun a completed workflow.
#[utoipa::path(
    post, path = "/api/v1/runs/{run_id}/rerun", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    responses(
        (status = 202, description = "Rerun accepted", body = RunAcceptedResponse),
        (status = 404, description = "Run not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn rerun_run() {}

/// Stream run events (NDJSON).
///
/// Returns a long-lived stream of newline-delimited JSON events.
/// The stream closes when the run reaches a terminal status.
/// Events include `run_accepted`, `job_status`, `run_status`, `log_line`,
/// `annotation`, and `job_completed`.
#[utoipa::path(
    get, path = "/api/v1/runs/{run_id}/events.ndjson", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    responses(
        (status = 200, content_type = "application/x-ndjson", description = "NDJSON event stream", body = String),
        (status = 404, description = "Run not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn run_events() {}

/// Register a DAP debug port (runner-facing).
///
/// Called by the runner to announce its locally-bound DAP port.
#[utoipa::path(
    post, path = "/api/v1/runs/{run_id}/debug", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    request_body = JsonValue,
    responses((status = 204, description = "Port registered")),
    security(("runner_bearer" = []))
)]
fn register_dap_port() {}

/// DAP debug WebSocket proxy.
///
/// Upgrades to a WebSocket that proxies DAP frames between an editor
/// and the runner's debug adapter.
#[utoipa::path(
    get, path = "/api/v1/runs/{run_id}/debug", tag = "Runs",
    params(("run_id" = String, Path, description = "Run UUID")),
    responses((status = 101, description = "WebSocket upgrade for DAP")),
    security(("native_bearer" = []))
)]
fn dap_debug() {}

// ── Debug Sessions ──────────────────────────────────────────────────────────

/// Exchange a job runtime token for a debug-worker token.
#[utoipa::path(
    post, path = "/api/v1/debug/worker-token", tag = "Debug Sessions",
    request_body = JsonValue,
    responses(
        (status = 200, description = "Debug-worker token", body = JsonValue),
        (status = 403, description = "Token already issued or job not eligible", body = ApiErrorResponse)
    ),
    security(("job_runtime_bearer" = []))
)]
fn worker_token() {}

/// List open debug sessions.
#[utoipa::path(
    get, path = "/api/v1/debug/sessions", tag = "Debug Sessions",
    responses((status = 200, description = "Array of sessions", body = [JsonValue])),
    security(("native_bearer" = []))
)]
fn list_debug_sessions() {}

/// Open a new debug session (worker-facing).
///
/// Called by the worker process when a job pauses on failure.
#[utoipa::path(
    post, path = "/api/v1/debug/sessions", tag = "Debug Sessions",
    request_body = JsonValue,
    responses((status = 201, description = "Session created", body = JsonValue)),
    security(("debug_worker_bearer" = []))
)]
fn open_debug_session() {}

/// Get a debug session by id.
#[utoipa::path(
    get, path = "/api/v1/debug/sessions/{session_id}", tag = "Debug Sessions",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses(
        (status = 200, description = "Session details", body = JsonValue),
        (status = 404, description = "Session not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn get_debug_session() {}

/// Poll for a controller verdict (worker-facing).
///
/// The worker long-polls this endpoint until a verdict (retry, abort, etc.)
/// is posted by the controller.
#[utoipa::path(
    get, path = "/api/v1/debug/sessions/{session_id}/verdict", tag = "Debug Sessions",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses((status = 200, description = "Verdict (or empty if pending)", body = JsonValue)),
    security(("debug_worker_bearer" = []))
)]
fn poll_debug_verdict() {}

/// Post a verdict to a debug session (controller-facing).
///
/// Possible verdicts: `retry`, `retry_from`, `abort`.
#[utoipa::path(
    post, path = "/api/v1/debug/sessions/{session_id}/verdict", tag = "Debug Sessions",
    params(("session_id" = String, Path, description = "Session identifier")),
    request_body = JsonValue,
    responses((status = 200, description = "Verdict accepted", body = JsonValue)),
    security(("native_bearer" = []))
)]
fn post_debug_verdict() {}

/// Close a debug session (worker-facing).
#[utoipa::path(
    post, path = "/api/v1/debug/sessions/{session_id}/close", tag = "Debug Sessions",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses((status = 204, description = "Session closed")),
    security(("debug_worker_bearer" = []))
)]
fn close_debug_session() {}

// ── Agent Debug ─────────────────────────────────────────────────────────────

/// Acquire a controller lease on a debug session.
///
/// At most one controller may hold a lease at a time.
#[utoipa::path(
    post, path = "/api/v1/agent/debug/sessions/{session_id}/lease", tag = "Agent Debug",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses(
        (status = 200, description = "Lease acquired", body = JsonValue),
        (status = 409, description = "Lease already held", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn acquire_debug_lease() {}

/// Release the controller lease.
#[utoipa::path(
    delete, path = "/api/v1/agent/debug/sessions/{session_id}/lease", tag = "Agent Debug",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses((status = 204, description = "Lease released")),
    security(("native_bearer" = []))
)]
fn release_debug_lease() {}

/// Read structured debug events.
///
/// Returns events after the given `after` event id (query param).
#[utoipa::path(
    get, path = "/api/v1/agent/debug/sessions/{session_id}/events", tag = "Agent Debug",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses((status = 200, description = "Structured events", body = [JsonValue])),
    security(("native_bearer" = []))
)]
fn debug_events() {}

/// Issue a debug operation (retry, retry_from, abort, etc.).
///
/// Requires an active controller lease.
#[utoipa::path(
    post, path = "/api/v1/agent/debug/sessions/{session_id}/operations", tag = "Agent Debug",
    params(("session_id" = String, Path, description = "Session identifier")),
    request_body = JsonValue,
    responses((status = 200, description = "Operation result", body = JsonValue)),
    security(("native_bearer" = []))
)]
fn debug_operation() {}

/// Read the retained audit trail.
#[utoipa::path(
    get, path = "/api/v1/agent/debug/sessions/{session_id}/audit", tag = "Agent Debug",
    params(("session_id" = String, Path, description = "Session identifier")),
    responses((status = 200, description = "Audit entries", body = [JsonValue])),
    security(("native_bearer" = []))
)]
fn debug_audit() {}

// ── Cache ───────────────────────────────────────────────────────────────────

/// Look up a cache entry by key.
#[utoipa::path(
    get, path = "/api/v1/cache", tag = "Cache",
    params(
        ("key" = Option<String>, Query, description = "Cache key"),
        ("keys" = Option<String>, Query, description = "Comma-separated restore keys"),
        ("version" = Option<String>, Query, description = "Cache version")
    ),
    responses((status = 200, description = "Cache lookup result", body = JsonValue)),
    security(("native_bearer" = []))
)]
fn cache_get() {}

/// Store a cache entry.
#[utoipa::path(
    post, path = "/api/v1/cache", tag = "Cache",
    request_body = JsonValue,
    responses((status = 200, description = "Cache entry stored", body = JsonValue)),
    security(("native_bearer" = []))
)]
fn cache_post() {}

// ── Artifacts ───────────────────────────────────────────────────────────────

/// Upload an artifact.
#[utoipa::path(
    post, path = "/api/v1/artifacts", tag = "Artifacts",
    request_body = JsonValue,
    responses((status = 201, description = "Artifact created", body = JsonValue)),
    security(("native_bearer" = []))
)]
fn artifacts() {}

/// Download an artifact by id.
#[utoipa::path(
    get, path = "/api/v1/artifacts/{artifact_id}", tag = "Artifacts",
    params(("artifact_id" = String, Path, description = "Artifact identifier")),
    responses(
        (status = 200, content_type = "application/octet-stream", description = "Artifact content", body = String),
        (status = 404, description = "Artifact not found", body = ApiErrorResponse)
    ),
    security(("native_bearer" = []))
)]
fn artifact() {}

// ── GitHub ──────────────────────────────────────────────────────────────────

/// Trigger a `workflow_dispatch` run (github.com-compatible).
///
/// `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches` —
/// authenticated through the dispatch credential chain and validated against
/// the workflow's declared triggers and inputs before any run is created.
/// `workflow_id` is the workflow file name (`ci.yml`, `ci`, or
/// `.github/workflows/ci.yml`); preloop does not track numeric workflow ids.
#[utoipa::path(
    post, path = "/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches",
    tag = "GitHub",
    security(("github_dispatch_bearer" = [])),
    params(
        ("owner" = String, Path, description = "Repository owner"),
        ("repo" = String, Path, description = "Repository name"),
        ("workflow_id" = String, Path, description = "Workflow file name (e.g. `ci.yml`)")
    ),
    request_body = JsonValue,
    responses(
        (status = 204, description = "Run dispatched"),
        (status = 400, description = "Malformed JSON body", body = ApiErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ApiErrorResponse),
        (status = 403, description = "No `actions: write` on the repository", body = ApiErrorResponse),
        (status = 404, description = "Unknown repository or workflow", body = ApiErrorResponse),
        (status = 409, description = "Workflow is not `workflow_dispatch`-triggered", body = ApiErrorResponse),
        (status = 422, description = "Input validation failed", body = ApiErrorResponse),
        (status = 502, description = "Failed to fetch workflows or resolve the ref", body = ApiErrorResponse)
    )
)]
fn workflow_dispatch_trigger() {}

/// Trigger a `repository_dispatch` event (github.com-compatible).
///
/// `POST /repos/{owner}/{repo}/dispatches` — a broadcast: every workflow whose
/// `on.repository_dispatch.types` matches `event_type` runs (an absent `types`
/// matches every event type). Returns `204` even when no workflow matches.
#[utoipa::path(
    post, path = "/repos/{owner}/{repo}/dispatches",
    tag = "GitHub",
    security(("github_dispatch_bearer" = [])),
    params(
        ("owner" = String, Path, description = "Repository owner"),
        ("repo" = String, Path, description = "Repository name")
    ),
    request_body = JsonValue,
    responses(
        (status = 204, description = "Event dispatched"),
        (status = 400, description = "Malformed JSON body", body = ApiErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ApiErrorResponse),
        (status = 403, description = "No dispatch permission on the repository", body = ApiErrorResponse),
        (status = 404, description = "Unknown repository", body = ApiErrorResponse),
        (status = 422, description = "Missing or invalid `event_type`", body = ApiErrorResponse),
        (status = 502, description = "Failed to fetch workflows or resolve the ref", body = ApiErrorResponse)
    )
)]
fn repository_dispatch_trigger() {}

/// List workflows for a repository (github.com-compatible shape).
///
/// `GET /repos/{owner}/{repo}/actions/workflows` — convenience for Apps that
/// poll. `id` is a deterministic hash of the workflow path (preloop does not
/// track github.com numeric workflow ids).
#[utoipa::path(
    get, path = "/repos/{owner}/{repo}/actions/workflows", tag = "GitHub",
    security(("github_dispatch_bearer" = [])),
    params(
        ("owner" = String, Path, description = "Repository owner"),
        ("repo" = String, Path, description = "Repository name")
    ),
    responses(
        (status = 200, description = "Workflow list", body = JsonValue),
        (status = 401, description = "Missing or invalid credential", body = ApiErrorResponse),
        (status = 403, description = "No repository access", body = ApiErrorResponse),
        (status = 404, description = "Unknown repository", body = ApiErrorResponse),
        (status = 502, description = "Failed to fetch workflows or resolve the ref", body = ApiErrorResponse)
    )
)]
fn list_dispatch_workflows() {}

/// List recent runs for a repository (github.com-compatible shape).
///
/// `GET /repos/{owner}/{repo}/actions/runs` — convenience for Apps that poll.
/// `id` is a deterministic hash of the preloop run UUID; the native `run_id`
/// field is included for preloop-native consumers.
#[utoipa::path(
    get, path = "/repos/{owner}/{repo}/actions/runs", tag = "GitHub",
    security(("github_dispatch_bearer" = [])),
    params(
        ("owner" = String, Path, description = "Repository owner"),
        ("repo" = String, Path, description = "Repository name")
    ),
    responses(
        (status = 200, description = "Run list", body = JsonValue),
        (status = 401, description = "Missing or invalid credential", body = ApiErrorResponse),
        (status = 403, description = "No repository access", body = ApiErrorResponse),
        (status = 404, description = "Unknown repository", body = ApiErrorResponse),
        (status = 502, description = "Failed to fetch workflows or resolve the ref", body = ApiErrorResponse)
    )
)]
fn list_dispatch_runs() {}

/// Receive a GitHub webhook event.
///
/// Validates the `X-Hub-Signature-256` header, extracts the event type
/// from `X-GitHub-Event`, and queues matching workflow runs.
#[utoipa::path(
    post, path = "/api/v1/github/webhooks", tag = "GitHub",
    request_body = JsonValue,
    responses(
        (status = 202, description = "Event accepted"),
        (status = 401, description = "Invalid signature", body = ApiErrorResponse)
    )
)]
fn github_webhook() {}

/// GitHub App manifest registration page (browser-facing).
#[utoipa::path(
    get, path = "/api/v1/github/register", tag = "GitHub",
    responses((status = 200, content_type = "text/html", description = "Registration form", body = String))
)]
fn github_register() {}

/// GitHub App manifest callback (browser-facing).
///
/// Exchanges the one-time code from GitHub for App credentials.
#[utoipa::path(
    get, path = "/api/v1/github/callback", tag = "GitHub",
    params(("code" = String, Query, description = "GitHub manifest conversion code")),
    responses((status = 200, content_type = "text/html", description = "App credentials page", body = String))
)]
fn github_callback() {}

/// Registered runners (read-only; used by the CLI to detect a dead pool).
#[utoipa::path(
    get, path = "/api/v1/runners", tag = "Runners",
    params(
        ("run_id" = Option<String>, Query, description = "Run UUID; when present, `queued` is the number of the run's jobs awaiting a runner and `claimable` is the number of registered runners matching at least one of those queued jobs")
    ),
    responses(
        (status = 200, description = "Registered runners with labels", body = JsonValue)
    ),
    security(("native_bearer" = []))
)]
fn list_runners() {}
