use super::*;

// ─── GHES org-prefixed wrapper handlers ─────────────────────────────────────
// These extract the extra `:org` path parameter and delegate to the real handlers.

pub(crate) async fn agent_lookup_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup(State(shared), Path(pool_id), Query(params)).await
}

pub(crate) async fn agent_lookup_by_id_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, agent_id)): Path<(String, i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    agent_lookup_by_id(State(shared), Path((pool_id, agent_id)), Query(params)).await
}

pub(crate) async fn register_runner_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat_pool_only(State(shared), Path(pool_id), headers, Json(request)).await
}

pub(crate) async fn register_runner_compat_org_2(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, agent_id)): Path<(String, i64, String)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    register_runner_compat(
        State(shared),
        Path((pool_id, agent_id)),
        headers,
        Json(request),
    )
    .await
}
pub(crate) async fn replace_runner_compat_org_2(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, agent_id)): Path<(String, i64, String)>,
    headers: axum::http::HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    replace_runner_compat(
        State(shared),
        Path((pool_id, agent_id)),
        headers,
        Json(request),
    )
    .await
}

pub(crate) async fn create_session_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, session_id)): Path<(String, i64, String)>,
    headers: axum::http::HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    create_session_compat(
        State(shared),
        Path((pool_id, session_id)),
        headers,
        identity,
        Json(body),
    )
    .await
}

/// Session creation with only pool_id in path (no session_id — server generates it).
pub(crate) async fn create_session_compat_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    headers: axum::http::HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Generate a session_id since the runner doesn't provide one
    let session_id = uuid::Uuid::new_v4().to_string();
    create_session_compat(
        State(shared),
        Path((pool_id, session_id)),
        headers,
        identity,
        Json(body),
    )
    .await
}

/// Org-prefixed session creation with only pool_id in path.
pub(crate) async fn create_session_compat_org_pool_only(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    headers: axum::http::HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    create_session_compat_pool_only(State(shared), Path(pool_id), headers, identity, Json(body))
        .await
}
pub(crate) async fn delete_session_org(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Path((_org, pool_id, session_id)): Path<(String, i64, String)>,
) -> Result<StatusCode, ApiError> {
    delete_session(
        State(shared),
        headers,
        identity,
        Path((pool_id, session_id)),
    )
    .await
}

pub(crate) async fn next_message_compat_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id)): Path<(String, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Option<azdo::TaskAgentMessage>>) {
    next_message_compat(State(shared), Path(pool_id), identity, Query(params)).await
}

pub(crate) async fn delete_pool_message_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, message_id)): Path<(String, i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    delete_pool_message(
        State(shared),
        Path((pool_id, message_id)),
        identity,
        Query(params),
    )
    .await
}

pub(crate) async fn agent_request_get_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    agent_request_get(State(shared), Path((pool_id, request_id)), identity).await
}

pub(crate) async fn agent_request_ack_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
) -> Result<StatusCode, ApiError> {
    agent_request_ack(State(shared), Path((pool_id, request_id)), identity).await
}

pub(crate) async fn agent_request_patch_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, pool_id, request_id)): Path<(String, i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    agent_request_patch(
        State(shared),
        Path((pool_id, request_id)),
        identity,
        Json(body),
    )
    .await
}

pub(crate) async fn patch_timeline_records_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id)): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_uuid = timeline_id.parse().ok();
    crate::timeline_logs::authorize_reporting_callback(
        &shared,
        &headers,
        &plan_id,
        timeline_uuid,
        None,
    )
    .await?;
    Ok(patch_timeline_records(
        State(shared),
        Path((scope, hub, plan_id, timeline_id)),
        Json(wrapper),
    )
    .await)
}

pub(crate) async fn get_timeline_records_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id)): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<crate::timeline_logs::TimelineQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_uuid = timeline_id.parse().ok();
    crate::timeline_logs::authorize_reporting_callback(
        &shared,
        &headers,
        &plan_id,
        timeline_uuid,
        None,
    )
    .await?;
    Ok(get_timeline_records(
        State(shared),
        Path((scope, hub, plan_id, timeline_id)),
        Query(query),
    )
    .await)
}

pub(crate) async fn create_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Json(log): Json<azdo::TaskLog>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::timeline_logs::authorize_reporting_callback(&shared, &headers, &plan_id, None, None)
        .await?;
    Ok(create_log(State(shared), Path((scope, hub, plan_id)), Json(log)).await)
}

pub(crate) async fn append_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, log_id)): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    crate::timeline_logs::authorize_reporting_callback(&shared, &headers, &plan_id, None, None)
        .await?;
    Ok(append_log(State(shared), Path((scope, hub, plan_id, log_id)), body).await)
}

pub(crate) async fn console_log_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id, timeline_id, record_id)): Path<(
        String,
        String,
        String,
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let timeline_uuid = timeline_id.parse().ok();
    crate::timeline_logs::authorize_reporting_callback(
        &shared,
        &headers,
        &plan_id,
        timeline_uuid,
        None,
    )
    .await?;
    Ok(console_log(
        State(shared),
        Path((scope, hub, plan_id, timeline_id, record_id)),
        body,
    )
    .await)
}

pub(crate) async fn finish_job_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, scope, hub, plan_id)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Result<Json<serde_json::Value>, ApiError> {
    crate::timeline_logs::authorize_reporting_callback(
        &shared,
        &headers,
        &plan_id,
        None,
        Some(event.job_id),
    )
    .await?;
    Ok(finish_job(State(shared), Path((scope, hub, plan_id)), Json(event)).await)
}

pub(crate) async fn action_download_info_org(
    State(shared): State<Arc<SharedState>>,
    Path((_org, _scope, _hub, _plan_id)): Path<(String, String, String, String)>,
    Json(request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    action_download_info(State(shared), Json(request)).await
}
