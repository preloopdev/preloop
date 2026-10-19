use super::*;
use std::pin::Pin;
use std::task::Poll;
use utoipa::OpenApi;

// The worker-facing debug surface is fail-closed in two places: the route
// registration here and `auth::is_worker_debug_route`, which decides what the
// guest control socket may reach. Shared constants keep the two from
// drifting when a route is added or renamed.
pub const DEBUG_WORKER_TOKEN_PATH: &str = "/api/v1/debug/worker-token";
pub const DEBUG_SESSIONS_PATH: &str = "/api/v1/debug/sessions";
pub const DEBUG_SESSION_VERDICT_SUFFIX: &str = "/verdict";
pub const DEBUG_SESSION_CLOSE_SUFFIX: &str = "/close";

/// How many webhook bodies may be buffered at once.
///
/// The body extractor reads the whole payload (up to the 32 MiB route limit)
/// before the handler verifies the HMAC, so without a cap an unauthenticated
/// caller turns the ingress into a memory amplifier: `N` concurrent uploads
/// each hold a full body while the signature is still unchecked. Eight slots
/// bound the transient cost at ~256 MiB; requests over the cap are shed with
/// 503 instead of being queued, which would only move the same pressure
/// behind an unbounded wait list. Shedding is the last-resort bound: GitHub
/// does not retry a non-2xx on its own, and a shed request never reaches the
/// store, so there is no local payload copy to replay — the delivery exists
/// only at GitHub and is recovered from GitHub's redelivery log.
const MAX_CONCURRENT_WEBHOOK_UPLOADS: usize = 8;

/// How long an admitted webhook request may spend uploading its body.
///
/// The slot cap alone would let eight stalled uploads — headers received, body
/// never finished — pin every permit and answer legitimate deliveries 503, a
/// self-DoS behind the gate. This budget releases the slot instead: a body
/// still arriving when it fires is cut off with 408. It covers the upload
/// only. The handler's acknowledgement budget (`WEBHOOK_ACK_BUDGET` in
/// `github.rs`) starts once the body is in, so a slow but valid upload cannot
/// spend the budget the durable write needs: a delivery that takes 4 s to
/// arrive still gets the store its full 8 s, where a deadline over the whole
/// request would have cancelled it mid-write and lost it. A 408 therefore
/// means the body never arrived and nothing was recorded — like a shed
/// request, that delivery must be redelivered from GitHub.
const WEBHOOK_INGEST_BUDGET: Duration = Duration::from_secs(10);

/// Admission cap and ingest budget for the webhook route.
///
/// One value because `limit_webhook_uploads` enforces both: `slots` bounds how
/// many bodies are buffered at once, `ingest_budget` how long any one of them
/// may spend arriving. Tests build a gate with their own budget rather than
/// waiting out the production one.
struct WebhookUploadGate {
    slots: Arc<tokio::sync::Semaphore>,
    ingest_budget: Duration,
}

/// Try to take a webhook upload slot, or shed the request.
///
/// Load-shedding is deliberate: a bounded queue only moves the memory
/// pressure to a slower moment. A shed delivery is recoverable rather than
/// lost — it never reached the store, so it is redelivered from GitHub's
/// delivery log, because GitHub does not retry an arbitrary non-2xx itself.
fn reserve_webhook_upload_slot(
    slots: &Arc<tokio::sync::Semaphore>,
) -> Result<tokio::sync::OwnedSemaphorePermit, StatusCode> {
    slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Wrap `body` so `complete` fires once the body has been read to its end, or
/// has failed, or has been dropped.
///
/// The middleware has to know when an upload has finished, but the handler's
/// extractor is what reads the body, so the signal has to travel with it. A
/// body dropped before it is read — a payload rejected as oversized — signals
/// too: that request already has its answer and must not be timed out on the
/// way out. Only data frames pass through; a webhook delivery carries no
/// trailers, and the `Bytes` extractor ignores them in any case.
fn signal_body_complete(body: Body, complete: tokio::sync::oneshot::Sender<()>) -> Body {
    use futures::Stream as _;

    let mut body = body.into_data_stream();
    let mut complete = Some(complete);
    Body::from_stream(stream::poll_fn(move |cx| {
        let next = Pin::new(&mut body).poll_next(cx);
        if matches!(next, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            && let Some(complete) = complete.take()
        {
            let _ = complete.send(());
        }
        next
    }))
}

/// Resolve when an admitted request is still uploading after its ingest
/// budget, and never once the body is in.
///
/// `body_complete` resolves when the body is exhausted, fails, or is dropped,
/// so an Ok result means the upload is over — whatever it cost — and the
/// deadline must stop counting; waiting on a future that never resolves keeps
/// the middleware off the handler's clock.
async fn upload_deadline(budget: Duration, body_complete: tokio::sync::oneshot::Receiver<()>) {
    if tokio::time::timeout(budget, body_complete).await.is_ok() {
        std::future::pending::<()>().await
    }
}

/// Gate webhook uploads.
///
/// The permit is held until the response is produced — as long as the buffered
/// body, and the record built from it, are alive — while the ingest budget
/// covers only the upload. Once the body is in, the handler runs to its own
/// budgets: the deadline is disarmed rather than left ticking, because a
/// delivery that has finished arriving must not be cancelled while it is still
/// inside the acknowledgement budget it is allowed (`WEBHOOK_ACK_BUDGET`).
async fn limit_webhook_uploads(
    State(gate): State<Arc<WebhookUploadGate>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let _permit = reserve_webhook_upload_slot(&gate.slots)?;
    let (parts, body) = request.into_parts();
    let (body_complete_tx, body_complete_rx) = tokio::sync::oneshot::channel();
    let handler = next.run(Request::from_parts(
        parts,
        signal_body_complete(body, body_complete_tx),
    ));
    tokio::pin!(handler);
    let deadline = upload_deadline(gate.ingest_budget, body_complete_rx);
    tokio::pin!(deadline);
    tokio::select! {
        response = &mut handler => Ok(response),
        () = &mut deadline => Err(StatusCode::REQUEST_TIMEOUT),
    }
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

pub fn build_app(
    state: AppState,
    shutdown: CancellationToken,
    test_api_token: Option<String>,
) -> Router {
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: shutdown.clone(),
    });
    let protected_protocol_apis = Router::new()
        .route("/_apis/artifactcache/cache", post(cache_reserve))
        .route("/_apis/artifactcache/cache", get(cache_lookup))
        .route(
            "/_apis/artifactcache/cache/{cache_id}",
            // `cache_upload` streams `Body` and enforces this cap while reading.
            // Keep the named route layer as defense-in-depth if its extractor
            // ever changes back to a buffered type.
            patch(cache_upload).layer(DefaultBodyLimit::max(MAX_CACHE_UPLOAD_BYTES as usize)),
        )
        .route("/_apis/artifactcache/cache/{cache_id}", post(cache_commit))
        .route(
            "/_apis/pipelines/workflows/{run_id}/artifacts",
            post(artifact_create),
        )
        .route(
            "/_apis/pipelines/workflows/{run_id}/artifacts",
            get(artifact_list),
        )
        .route(
            "/_apis/pipelines/workflows/{run_id}/artifacts/{artifact_id}",
            get(artifact_get_compat),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools",
            get(runner_pools),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/sessions",
            post(create_session_disttask).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/_apis/distributedtask/pools/{pool_id}/sessions",
            post(create_session_disttask).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/messages",
            get(next_message_disttask).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        // Paired with the GET above: a client that polls messages on the
        // distributedtask prefix deletes them on the same prefix. Job-facing
        // runner traffic, so it stays under the protocol guard — the
        // system-only router below is for runner/session/agent administration.
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/messages/{message_id}",
            delete(delete_pool_message).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{run_id}/jobs/{job_id}",
            patch(complete_job_compat_authenticated),
        )
        .route("/ws/live-logs/{job_id}", get(ws_live_logs))
        .route("/broker/{runner_id}/acquirejob", post(broker_acquire_job))
        .route("/broker/{runner_id}/renewjob", post(broker_renew_job))
        .route("/broker/{runner_id}/completejob", post(broker_complete_job))
        .route(
            "/_apis/v1/Timeline/{scope}/{hub}/{plan_id}/{timeline_id}",
            patch(patch_timeline_records_authenticated).get(get_timeline_records_authenticated),
        )
        .route(
            "/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}",
            post(create_log_authenticated),
        )
        .route(
            "/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}/{log_id}",
            // Log and console appends extract the whole body as `Bytes`. Every
            // log-append and console mount is capped at the per-log retained
            // window, `MAX_LOG_BYTES_PER_KEY` (memory_caps.rs).
            post(append_log_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/_apis/v1/TimeLineWebConsoleLog/{scope}/{hub}/{plan_id}/{timeline_id}/{record_id}",
            post(console_log_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/_apis/v1/FinishJob/{scope}/{hub}/{plan_id}",
            post(finish_job_authenticated),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/{plan_id}/jobs/{job_id}/oidctoken",
            get(oidc_token),
        )
        .route(
            "/{orchestration_id}//idtoken/{plan_id}/{job_id}",
            get(oidc_token_run_service),
        )
        .route(
            "/_apis/v1/ActionDownloadInfo/{scope}/{hub}/{plan_id}",
            post(action_download_info).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_results_protocol_bearer,
            )),
        )
        .route(
            "/actions/build/{orchestration_id}/jobs/{job_id}/runnerresolve/actions",
            post(runnerresolve_actions).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_results_protocol_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/v1/Timeline/{scope}/{hub}/{plan_id}/{timeline_id}",
            patch(patch_timeline_records_authenticated).get(get_timeline_records_authenticated),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}",
            post(create_log_authenticated),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}/{log_id}",
            post(append_log_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/{scope}/{hub}/{plan_id}/{timeline_id}/{record_id}",
            post(console_log_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/runner/server/_apis/v1/FinishJob/{scope}/{hub}/{plan_id}",
            post(finish_job_authenticated),
        )
        .route(
            "/runner/server/_apis/v1/ActionDownloadInfo/{scope}/{hub}/{plan_id}",
            post(action_download_info).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_results_protocol_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/jobs/{job_id}/logs/live",
            get(live_logs_sse),
        )
        // standard AzDO API URL pattern used by the preloop-runner AzDO client.
        // These alias the scope/hub-prefixed handlers above so both URL forms work.
        .route(
            "/_apis/v1/plans/{plan_id}/timelines/{timeline_id}/records",
            patch(patch_timeline_records_plan_authenticated).get(get_timeline_records_plan_authenticated),
        )
        .route(
            "/_apis/v1/plans/{plan_id}/logs",
            post(create_log_plan_authenticated),
        )
        .route(
            "/_apis/v1/plans/{plan_id}/logs/{log_id}",
            put(append_log_plan_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/_apis/v1/plans/{plan_id}/events",
            post(finish_job_plan_authenticated),
        )
        // /runner/server/ aliases — runner uses the SystemVssConnection URL
        // which is http://…/runner/server so all plan-level AzDO calls land here.
        .route(
            "/runner/server/_apis/v1/plans/{plan_id}/timelines/{timeline_id}/records",
            patch(patch_timeline_records_plan_authenticated).get(get_timeline_records_plan_authenticated),
        )
        .route(
            "/runner/server/_apis/v1/plans/{plan_id}/logs",
            post(create_log_plan_authenticated),
        )
        .route(
            "/runner/server/_apis/v1/plans/{plan_id}/logs/{log_id}",
            put(append_log_plan_authenticated).layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/runner/server/_apis/v1/plans/{plan_id}/events",
            post(finish_job_plan_authenticated),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_protocol_bearer,
        ));
    // Agent registration has a stricter credential boundary than the other
    // protected APIs: it may use the one-time pool provision token without a
    // bearer, or a trusted system/RunnerManage credential.
    let distributedtask_registration = Router::new()
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/agents",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_runner_registration_bearer,
        ));

    // Runner/session/agent administration. Split out of the protocol router
    // because `require_protocol_bearer` accepts any valid local JWT — a job's
    // own `ACTIONS_RUNTIME_TOKEN` included — which let workflow code
    // deregister runners and delete other runners' sessions. The
    // guard here admits only the system token or a registered runner's listen
    // token, and each handler additionally requires self-ownership: the
    // official runner deletes its own session and deregisters its own agent
    // through these routes, so a system-token-only guard would 401 the
    // runner's shutdown path.
    // Pool-wide session deletion has no caller-owned target. Keep it
    // administrator-only; runner listen tokens may delete only one of their
    // own sessions through the per-session routes below.
    let protected_system_apis = Router::new()
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/sessions",
            delete(delete_sessions_for_pool),
        )
        .route(
            "/_apis/distributedtask/pools/{pool_id}/sessions",
            delete(delete_sessions_for_pool),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_system_bearer,
        ));

    let protected_admin_apis = Router::new()
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/agents/{agent_id}",
            delete(delete_agent).put(update_agent),
        )
        .route(
            "/_apis/distributedtask/pools/{pool_id}/agents/{agent_id}",
            delete(delete_agent).put(update_agent),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/{pool_id}/sessions/{session_id}",
            delete(delete_session),
        )
        .route(
            "/_apis/distributedtask/pools/{pool_id}/sessions/{session_id}",
            delete(delete_session),
        )
        .route(
            "/{org}/_apis/v1/AgentSession/{pool_id}/{session_id}",
            delete(delete_session_org),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/{pool_id}/{session_id}",
            delete(delete_session),
        )
        .route(
            "/_apis/v1/AgentSession/{pool_id}/{session_id}",
            delete(delete_session),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_runner_admin_bearer,
        ));

    let results_metadata = Router::new()
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
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_results_bearer,
        ))
        .with_state(shared.clone());

    // GitHub-compatible dispatch API (surface 2): authenticated through the
    // dispatch auth chain (system bearer, PAT, own-App JWT, installation tokens) — see
    // `dispatch_auth.rs`. Auth is mandatory; github.com returns 401 without a
    // token. One `route_layer` on the sub-router keeps the auth boundary in a
    // single place instead of repeating it on every route.
    let dispatch_api = Router::new()
        .route(
            "/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches",
            post(crate::dispatch::workflow_dispatch),
        )
        .route(
            "/repos/{owner}/{repo}/dispatches",
            post(crate::dispatch::repository_dispatch),
        )
        .route(
            "/repos/{owner}/{repo}/actions/workflows",
            get(crate::dispatch::list_workflows),
        )
        .route(
            "/repos/{owner}/{repo}/actions/runs",
            get(crate::dispatch::list_actions_runs),
        )
        .route(
            "/repos/{owner}/{repo}/actions/runs/{run_id}/rerun",
            post(crate::dispatch::rerun_actions_run),
        )
        .route(
            "/repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs",
            post(crate::dispatch::rerun_actions_run_failed),
        )
        .route(
            "/repos/{owner}/{repo}/actions/runs/{run_id}/cancel",
            post(crate::dispatch::cancel_actions_run),
        )
        .route(
            "/repos/{owner}/{repo}/actions/jobs/{job_id}/rerun",
            post(crate::dispatch::rerun_actions_job),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            crate::dispatch_auth::require_dispatch_auth,
        ));

    let observability_routes = Router::new()
        .route("/api/v1/status", get(status))
        .route(
            "/api/v1/config/checkout-cache",
            get(crate::runs::checkout_cache_config),
        )
        .route(
            "/api/v1/debug/txn-stats",
            get(|| async { axum::Json(crate::control::txn_stats::snapshot()) }),
        )
        .route("/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_native_bearer,
        ));

    // Shared by the webhook route's gate: see `limit_webhook_uploads`.
    let webhook_upload_gate = Arc::new(WebhookUploadGate {
        slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_WEBHOOK_UPLOADS)),
        ingest_budget: WEBHOOK_INGEST_BUDGET,
    });

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .merge(observability_routes)
        .route("/runs/{run_id}", get(get_public_run))
        .route(
            "/openapi.json",
            get(|| async { axum::Json(crate::openapi::ApiDoc::openapi()) }),
        )
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route("/.well-known/jwks", get(oidc_jwks))
        .route("/.well-known/jwks.json", get(oidc_jwks))
        .route(
            "/oidc/.well-known/openid-configuration",
            get(oidc_discovery),
        )
        .route("/oidc/.well-known/jwks", get(oidc_jwks))
        .route("/oidc/.well-known/jwks.json", get(oidc_jwks))
        // GHES-style org-prefixed routes
        .route("/{org}/_apis/connectionData", get(connection_data))
        .route("/{org}/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/{org}/_apis/v1/AgentPools", get(runner_pools))
        .route("/{org}/_apis/v1/settings/runner", get(runner_settings))
        .route(
            "/{org}/_apis/v1/Agent/{pool_id}/{agent_id}",
            get(agent_lookup_by_id_org)
                .post(register_runner_compat_org_2)
                .put(replace_runner_compat_org_2)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/{org}/_apis/v1/Agent/{pool_id}",
            get(agent_lookup_org)
                .post(register_runner_compat_org)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/{org}/_apis/v1/AgentSession/{pool_id}/{session_id}",
            post(create_session_compat_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/AgentSession/{pool_id}",
            post(create_session_compat_org_pool_only).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/Message/{pool_id}",
            get(next_message_compat_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/Message/{pool_id}/{message_id}",
            delete(delete_pool_message_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/AgentRequest/{pool_id}/{request_id}",
            get(agent_request_get_org)
                .post(agent_request_ack_org)
                .patch(agent_request_patch_org)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_legacy_runner_bearer,
                )),
        )
        .route(
            "/{org}/_apis/v1/Timeline/{scope}/{hub}/{plan_id}/{timeline_id}",
            patch(patch_timeline_records_org)
                .get(get_timeline_records_org)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_protocol_bearer,
                )),
        )
        .route(
            "/{org}/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}",
            post(create_log_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_protocol_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/Logfiles/{scope}/{hub}/{plan_id}/{log_id}",
            post(append_log_org)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_protocol_bearer,
                ))
                .layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/{org}/_apis/v1/TimeLineWebConsoleLog/{scope}/{hub}/{plan_id}/{timeline_id}/{record_id}",
            post(console_log_org)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_protocol_bearer,
                ))
                .layer(DefaultBodyLimit::max(MAX_LOG_BYTES_PER_KEY)),
        )
        .route(
            "/{org}/_apis/v1/FinishJob/{scope}/{hub}/{plan_id}",
            post(finish_job_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_protocol_bearer,
            )),
        )
        .route(
            "/{org}/_apis/v1/ActionDownloadInfo/{scope}/{hub}/{plan_id}",
            post(action_download_info_org).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_results_protocol_bearer,
            )),
        )
        .route("/_apis/v1/oauth2/token", post(oauth2_token))
        .route(
            "/api/v3/actions/runner-registration",
            post(github_registration_token),
        )
        .route(
            "/api/v3/orgs/{org}/actions/runners/registration-token",
            post(github_registration_token),
        )
        .route(
            "/api/v3/repos/{owner}/{repo}/actions/runners/registration-token",
            post(github_registration_token),
        )
        // Runner version end-of-life schedule (GitHub's September 2026
        // changelog): same three-level scoping as the registration routes
        // above — control plane, org, repo. The org/repo segments are
        // accepted and ignored; preloop is single-tenant, so the schedule
        // is global. Credential posture matches the neighbors: no
        // routing-layer middleware, the handler requires the system token.
        .route(
            "/api/v3/actions/runners/deprecations/{version}",
            get(runner_deprecation_lookup),
        )
        .route(
            "/api/v3/orgs/{org}/actions/runners/deprecations/{version}",
            get(runner_deprecation_lookup_org),
        )
        .route(
            "/api/v3/repos/{owner}/{repo}/actions/runners/deprecations/{version}",
            get(runner_deprecation_lookup_repo),
        )
        // GitHub-compatible dispatch API (surface 2): merged sub-router with
        // a single dispatch-auth layer — see `dispatch_api` above.
        .merge(dispatch_api)
        .route("/runner/server/_apis/connectionData", get(connection_data))
        .route("/runner/server/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/runner/server/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/runner/server/_apis/v1/Agent/{pool_id}/{agent_id}",
            get(agent_lookup_by_id)
                .post(register_runner_compat)
                .put(replace_runner_compat)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/runner/server/_apis/v1/Agent/{pool_id}",
            get(agent_lookup)
                .post(register_runner_compat_pool_only)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/{pool_id}/{session_id}",
            post(create_session_compat).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/{pool_id}",
            post(create_session_compat_pool_only).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/v1/Message/{pool_id}",
            get(next_message_compat).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/v1/Message/{pool_id}/{message_id}",
            delete(delete_pool_message).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/runner/server/_apis/v1/AgentRequest/{pool_id}/{request_id}",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_legacy_runner_bearer,
                )),
        )
        .route("/_apis/connectionData", get(connection_data))
        .route(
            "/api/v1/runs",
            post(submit_run)
                .get(list_runs)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                ))
                // The CLI inlines every local workflow file as a reusable
                // workflow; monorepos with hundreds of workflow files push
                // the submission past axum's default 2 MiB body limit.
                .layer(DefaultBodyLimit::max(64 * 1024 * 1024)),
        )
        .route(
            "/api/v1/bundles",
            post(crate::bundles::upload_bundle)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                ))
                .layer(DefaultBodyLimit::max(512 * 1024 * 1024)),
        )
        .route(
            "/api/v1/secrets",
            get(list_secrets).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/secrets/{name}",
            put(set_secret)
                .delete(delete_secret)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        // Workflow execution protections: operator rule management. Rules
        // are server-config-owned (the config file is the policy store), so
        // these routes read and persist `[execution_protection]` in the
        // operator's config file; changes take effect on engine restart.
        // System token only: workflow authors must never reach policy
        // management.
        .route(
            "/api/v1/execution-protection",
            get(get_execution_protection_policy).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_system_bearer,
            )),
        )
        .route(
            "/api/v1/execution-protection/mode",
            put(set_execution_protection_mode).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_system_bearer,
            )),
        )
        .route(
            "/api/v1/execution-protection/rules",
            get(list_execution_protection_rules)
                .post(create_execution_protection_rule)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_system_bearer,
                )),
        )
        .route(
            "/api/v1/execution-protection/rules/{id}",
            get(get_execution_protection_rule)
                .put(update_execution_protection_rule)
                .delete(delete_execution_protection_rule)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_system_bearer,
                )),
        )
        .route("/api/v1/scheduler/history", get(get_scheduler_history))
        .route(
            "/api/v1/github/webhooks",
            post(github::handle_github_webhook)
                // GitHub's own webhook cap is 25 MiB; without an explicit
                // limit axum's 2 MiB default aborts the connection mid-body,
                // which the sender sees as a reset/broken pipe and retries
                // blindly. Set the limit above GitHub's ceiling so real
                // deliveries pass and oversized input gets a deterministic
                // 413 instead of a severed connection.
                .layer(DefaultBodyLimit::max(32 * 1024 * 1024))
                // …and bound how many of those bodies are buffered at once
                // before the HMAC check, so the unauthenticated ingress
                // cannot be used as a memory amplifier. Over-cap requests are
                // shed with 503 — redelivered from GitHub, not retried on
                // their own — and an upload still arriving after
                // `WEBHOOK_INGEST_BUDGET` is cut off with 408, so a stalled
                // peer cannot pin a slot while legitimate deliveries queue up
                // behind it. The budget covers the upload alone: the handler
                // keeps its own acknowledgement budget once the body is in.
                .layer(middleware::from_fn_with_state(
                    webhook_upload_gate.clone(),
                    limit_webhook_uploads,
                )),
        )
        .route("/api/v1/github/register", get(github::github_register))
        .route("/api/v1/github/callback", get(github::github_callback))
        // Engine-hosted GitHub App setup (`preloop setup github --via app`
        // with a public URL). The API pair is administrator-only; the two
        // browser routes answer 404 unless a setup session awaits them, and
        // each carries its own refusal budget, so a burst of guesses against
        // one cannot gate the other.
        .route(
            "/api/v1/github/setup-sessions",
            post(crate::github_app_setup::session::open_session).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_system_bearer),
            ),
        )
        .route(
            "/api/v1/github/setup-sessions/{session_id}",
            get(crate::github_app_setup::session::poll_session).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_system_bearer),
            ),
        )
        .route(
            "/setup/github",
            get(crate::github_app_setup::session::setup_form),
        )
        .route(
            "/setup/github/callback",
            get(crate::github_app_setup::session::setup_callback),
        )
        // Operator surface over the durable queue: inspect it, replay from
        // the retained local payload (which outlives GitHub's three-day
        // redelivery window), and read one repair-health document.
        .route(
            "/api/v1/webhooks/deliveries",
            get(crate::webhook_api::list_webhook_deliveries).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_native_bearer),
            ),
        )
        .route(
            "/api/v1/webhooks/deliveries/{delivery_id}/replay",
            post(crate::webhook_api::replay_webhook_delivery).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_native_bearer),
            ),
        )
        .route(
            "/api/v1/webhooks/health",
            get(crate::webhook_api::webhook_health).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}",
            get(get_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/logs",
            get(get_run_logs).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/logs/live",
            get(crate::live_logs::live_run_logs_sse).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/cancel",
            post(cancel_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/jobs/{job_id}/approve",
            post(approve_job).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/rerun",
            post(rerun_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/approve-fork",
            post(approve_fork).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/push",
            post(crate::github_push::push_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/events.ndjson",
            get(run_events).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/debug",
            get(ws_dap_debug).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/{run_id}/debug",
            post(register_dap_port).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        // Live debug sessions. Worker-facing routes are runner-authenticated;
        // controller-facing routes are native-authenticated. Both live on the
        // native surface so `/_apis/...` stays byte-identical.
        //
        // Worker access to these four routes through the mounted control
        // socket is mirrored by `auth::is_worker_debug_route`. Keep that
        // fail-closed allowlist in sync when changing this route group.
        .route(
            DEBUG_WORKER_TOKEN_PATH,
            post(crate::debug_sessions::issue_worker_token).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_job_runtime_bearer),
            ),
        )
        .route(
            DEBUG_SESSIONS_PATH,
            post(crate::debug_sessions::open_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}{DEBUG_SESSION_VERDICT_SUFFIX}"),
            get(crate::debug_sessions::poll_verdict).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}{DEBUG_SESSION_CLOSE_SUFFIX}"),
            post(crate::debug_sessions::close_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            DEBUG_SESSIONS_PATH,
            get(crate::debug_sessions::list_sessions).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}"),
            get(crate::debug_sessions::get_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        // Unified controller surface: every controller (human CLI, agent, or
        // DAP) drives a paused session through the same lease-gated,
        // idempotent, version-checked operations.
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}/lease"),
            post(crate::debug_sessions::acquire_lease)
                .delete(crate::debug_sessions::release_lease)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}/events"),
            get(crate::debug_sessions::session_events).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}/operations"),
            post(crate::debug_sessions::session_operation).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_native_bearer),
            ),
        )
        .route(
            &format!("{DEBUG_SESSIONS_PATH}/{{session_id}}/audit"),
            get(crate::debug_sessions::session_audit).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        // Archive tickets are bearerless in the official runner protocol.
        .route(
            "/api/v1/actions/download/{owner}/{repo}/{*git_ref}",
            get(download_action_tarball),
        )
        // Read-only Git smart HTTP for immutable local-workspace snapshots.
        .route(
            "/snapshots/{run_id}/{*path}",
            get(snapshot_git_http).post(snapshot_git_http),
        )
        // Anonymous forge relay for checkouts the snapshot cannot serve
        // (deep history, cross-repo): git smart-HTTP + the single
        // `/api/v3/repos/{o}/{r}` lookup checkout issues for the default
        // branch. Handlers authenticate the pinned job runtime token.
        .route(
            "/{org}/{repo}/info/refs",
            get(crate::snapshots::forge_git_http),
        )
        .route(
            "/{org}/{repo}/git-upload-pack",
            post(crate::snapshots::forge_git_http),
        )
        .route(
            "/{org}/{repo}/info/lfs/objects/batch",
            post(crate::snapshots::forge_git_http),
        )
        .route(
            "/api/v3/repos/{owner}/{repo}",
            get(crate::snapshots::forge_api_repo),
        )
        // The archive half of the same surface: `actions/checkout` without a
        // `git` binary (minimal container images) downloads
        // `/repos/{o}/{r}/tarball/{ref}` from its `{server}/api/v3` base, for
        // the run's snapshot and for relayed forge repositories alike.
        .route(
            "/api/v3/repos/{owner}/{repo}/tarball/{*git_ref}",
            get(crate::snapshots::forge_api_repo_tarball),
        )
        .route(
            "/api/v1/runners",
            post(register_runner_native)
                .get(list_runners_native)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route(
            "/api/v1/runners/purge",
            post(purge_runners_by_name).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/session/{session_id}",
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
            "/runner/server/runner/session/{session_id}",
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
            "/session/{session_id}",
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
            "/runner/server/session/{session_id}",
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
            "/api/v1/artifacts/{artifact_id}",
            get(artifact_get).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route("/_apis/v1/settings/runner", get(runner_settings))
        // Runner lifecycle endpoints — registration is engine-authorized, and
        // session/message traffic requires a registered runner credential.
        .route("/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/_apis/v1/Agent/{pool_id}/{agent_id}",
            get(agent_lookup_by_id)
                .post(register_runner_compat)
                .put(replace_runner_compat)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/_apis/v1/Agent/{pool_id}",
            get(agent_lookup)
                .post(register_runner_compat_pool_only)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_runner_registration_bearer,
                )),
        )
        .route(
            "/_apis/v1/AgentSession/{pool_id}/{session_id}",
            post(create_session_compat).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/_apis/v1/AgentSession/{pool_id}",
            post(create_session_compat_pool_only).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/_apis/v1/Message/{pool_id}",
            get(next_message_compat).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/_apis/v1/Message/{pool_id}/{message_id}",
            delete(delete_pool_message).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_legacy_runner_bearer,
            )),
        )
        .route(
            "/_apis/v1/AgentRequest/{pool_id}/{request_id}",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_legacy_runner_bearer,
                )),
        )
        // Accept blob uploads at the signed-URL paths minted by the Twirp handlers.
        // The runner PUTs logs/summaries here; we store them in the state directory.
        .route(
            "/replay/results/{*path}",
            // Bytes extraction otherwise inherits Axum's 2 MiB default. Keep this
            // runner-facing upload bounded while allowing real step logs through.
            put(replay_results_put).layer(DefaultBodyLimit::max(MAX_REPLAY_RESULT_BYTES)),
        )
        // Twirp APIs accept only the system token or a locally signed Actions.Results job token.
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
            "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
            post(twirp_get_job_diag_logs_signed_blob_url),
        )
        // Cache v2 Twirp (CacheService) — used by actions/cache@v4 when ACTIONS_CACHE_SERVICE_V2=true.
        // The shared Twirp middleware below validates the job token before body extraction.
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
        // Refresh an expired blob upload URL: the runner POSTs the old
        // (expired) upload JWT with its live job bearer and gets a fresh
        // upload JWT for the same pending staging dir. The job must be live;
        // a settled job's refresh is refused so uploads never extend past
        // job death.
        .route(
            "/twirp/github.actions.results.api.v1.BlobService/RefreshUploadURL",
            post(twirp_refresh_blob_upload_url),
        )
        // Azure Block Blob compat blob store — upload (PUT) and download (GET).
        // Cache: /twirp-blob/cache/{token}
        // Artifact: /twirp-blob/artifact/{token} (download URL appends.zip for content-type detection)
        .route(
            "/twirp-blob/{kind}/{token}",
            put(blob_put)
                .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
                .get(blob_get),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_results_bearer,
        ))
        .merge(distributedtask_registration)
        .merge(protected_protocol_apis)
        .merge(protected_admin_apis)
        .merge(protected_system_apis)
        .with_state(shared.clone())
        .merge(results_metadata)
        .fallback(errors::protocol_not_found)
        .layer(middleware::from_fn_with_state(
            shared.clone(),
            resolve_runner_identity,
        ))
        .layer(middleware::from_fn_with_state(
            shared.clone(),
            crate::http_metrics::http_metrics_middleware,
        ))
        .layer(middleware::from_fn(errors::protocol_error_envelope))
        .layer(middleware::from_fn(errors::github_error_envelope))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_flows_middleware,
        ));

    match test_api_token {
        Some(token) => router.merge(
            Router::new()
                .route(
                    "/internal/test/runners/sessions/{session_id}/messages",
                    get(next_message),
                )
                .route(
                    "/internal/test/runners/sessions/{session_id}/messages/{message_id}",
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    /// axum 0.8 panics while registering a legacy `:param` or `*wildcard`
    /// segment, so constructing the production router is the regression
    /// check: any route still written in the 0.7 syntax fails right here.
    #[tokio::test]
    async fn production_router_builds_and_serves_healthz() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let router = app(state, CancellationToken::new());
        let response = router
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The production admission cap over a budget the test picks, so no test
    /// here waits out (or races) the real ten seconds.
    fn webhook_gate(ingest_budget: Duration) -> Arc<WebhookUploadGate> {
        Arc::new(WebhookUploadGate {
            slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_WEBHOOK_UPLOADS)),
            ingest_budget,
        })
    }

    /// The upload gate over a stub handler that reads its body, so admission
    /// and the ingest deadline are driven without the webhook handler's
    /// signature check or store.
    fn gate_router(gate: Arc<WebhookUploadGate>) -> Router {
        Router::new()
            .route("/webhooks", post(|_body: Bytes| async { StatusCode::OK }))
            .layer(middleware::from_fn_with_state(gate, limit_webhook_uploads))
    }

    /// Uploads past the slot cap are shed (503) rather than queued: the body
    /// is buffered before the HMAC is checked, so an unbounded admission is
    /// the memory amplifier this gate exists to prevent. The test holds the
    /// permits itself, so passing does not depend on how long a staged upload
    /// takes to be cut off — the timing the earlier version of this test
    /// shared with the ingest deadline.
    #[tokio::test]
    async fn webhook_uploads_are_shed_past_the_slot_cap() {
        let gate = webhook_gate(Duration::from_secs(60));
        let held: Vec<_> = (0..MAX_CONCURRENT_WEBHOOK_UPLOADS)
            .map(|_| reserve_webhook_upload_slot(&gate.slots).expect("slot"))
            .collect();
        let router = gate_router(gate.clone());

        let shed = router
            .clone()
            .oneshot(Request::post("/webhooks").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(shed.status(), StatusCode::SERVICE_UNAVAILABLE);

        drop(held);
        let admitted = router
            .oneshot(Request::post("/webhooks").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(admitted.status(), StatusCode::OK);
    }

    /// A body still arriving when the ingest budget is spent is cut off with
    /// 408 and gives its slot back, so a stalled peer cannot pin the ingress.
    #[tokio::test]
    async fn stalled_upload_is_cut_off_at_the_ingest_budget() {
        let gate = webhook_gate(Duration::from_millis(50));
        let router = gate_router(gate.clone());

        let stalled = Body::from_stream(stream::pending::<Result<Bytes, std::io::Error>>());
        let response = router
            .oneshot(Request::post("/webhooks").body(stalled).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert!(
            reserve_webhook_upload_slot(&gate.slots).is_ok(),
            "the cut-off upload must release its slot"
        );
    }

    /// The ingest budget covers the upload, not the handler: a body that
    /// arrives inside the budget leaves the handler to finish its own work,
    /// even when that work outlasts the budget. The deadline this replaced
    /// wrapped the whole request, so it cancelled stores that were still
    /// inside `WEBHOOK_ACK_BUDGET` — and a delivery cancelled mid-write is
    /// unrecorded and has to be redelivered from GitHub.
    #[tokio::test]
    async fn handler_work_outliving_the_ingest_budget_completes() {
        let gate = webhook_gate(Duration::from_millis(50));
        let router = Router::new()
            .route(
                "/webhooks",
                post(|_body: Bytes| async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    StatusCode::OK
                }),
            )
            .layer(middleware::from_fn_with_state(gate, limit_webhook_uploads));

        // The body is itself slow — arriving just inside the budget — so the
        // slow upload the budget exists for is covered alongside the handler's
        // longer-running work.
        let body = Body::from_stream(stream::once(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, std::io::Error>(Bytes::from_static(b"payload"))
        }));
        let response = router
            .oneshot(Request::post("/webhooks").body(body).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The gate is wired onto the real webhook route: with a free slot the
    /// request reaches the handler instead of being shed, and the unsigned
    /// body is rejected there with 401.
    #[tokio::test]
    async fn webhook_route_reaches_the_handler_through_the_upload_gate() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let router = app(state, CancellationToken::new());

        let response = router
            .oneshot(
                Request::post("/api/v1/github/webhooks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn webhook_upload_slots_refill_after_release() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let first = reserve_webhook_upload_slot(&slots).expect("first slot");
        assert_eq!(
            reserve_webhook_upload_slot(&slots).unwrap_err(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(first);
        assert!(reserve_webhook_upload_slot(&slots).is_ok());
    }

    /// `--record-flows` must not buffer the webhook body ahead of the route's
    /// own admission: the recorder reads whole bodies before any route layer
    /// runs, so recording this path would undo the upload gate. Other routes
    /// stay recorded.
    #[tokio::test]
    async fn flow_recording_skips_webhook_uploads() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let flows_path = temp.path().join("flows.jsonl");
        state.inner.lock().await.flows_file = Some(std::fs::File::create(&flows_path).unwrap());

        // `build_app` applies the recorder as its outermost layer; this router
        // reproduces that wiring over stub handlers.
        let router = Router::new()
            .route("/api/v1/github/webhooks", post(|| async { "ignored" }))
            .route("/healthz", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                record_flows_middleware,
            ));

        let webhook = Request::post("/api/v1/github/webhooks")
            .body(Body::empty())
            .unwrap();
        let healthz = Request::get("/healthz").body(Body::empty()).unwrap();
        for request in [webhook, healthz] {
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let recorded = std::fs::read_to_string(&flows_path).unwrap();
        assert_eq!(recorded.lines().count(), 1, "recorded: {recorded}");
        assert!(recorded.contains("\"/healthz\""), "recorded: {recorded}");
    }
}
