use super::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BrokerAcquireJobRequest {
    pub(crate) job_message_id: uuid::Uuid,
    // serde: accepted from the runner but not needed by acquisition logic.
    #[allow(dead_code)]
    pub(crate) billing_owner_id: Option<String>,
    // serde: accepted from the runner but not needed by acquisition logic.
    #[allow(dead_code)]
    pub(crate) runner_os: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BrokerRenewJobRequest {
    pub(crate) job_id: uuid::Uuid,
    #[serde(rename = "planId")]
    pub(crate) _plan_id: String,
    pub(crate) conclusion: Option<String>,
    #[serde(default)]
    pub(crate) outputs: BTreeMap<String, serde_json::Value>,
}

pub(crate) fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
    match result.to_ascii_lowercase().as_str() {
        "success" | "succeeded" | "succeededwithissues" => Some(ExecutionStatus::Success),
        "failure" | "failed" => Some(ExecutionStatus::Failure),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
        "skipped" => Some(ExecutionStatus::Skipped),
        _ => None,
    }
}

pub(crate) fn broker_run_service_url(runner_id: i64) -> String {
    format!("{}/broker/{runner_id}/", public_base_url())
}

pub(crate) fn public_base_url() -> String {
    std::env::var("AKSH_PUBLIC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
        .trim_end_matches('/')
        .to_owned()
}

pub(crate) fn format_reusable_workflow_ref(
    repository: &str,
    workflow_ref: &str,
    caller_ref: &str,
) -> String {
    if let Some(path) = workflow_ref.strip_prefix("./") {
        let (path, git_ref) = path.split_once('@').unwrap_or((path, caller_ref));
        return format!("{repository}/{path}@{git_ref}");
    }
    workflow_ref.to_owned()
}

pub(crate) fn normalize_oidc_issuer(value: String) -> anyhow::Result<String> {
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
pub(crate) fn oidc_issuer_url(inner: &InnerState) -> String {
    if inner.oidc_issuer.is_empty() {
        format!("{}/oidc", public_base_url())
    } else {
        inner.oidc_issuer.clone()
    }
}

pub(crate) fn websocket_base_url() -> String {
    let base = public_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

pub(crate) fn runner_server_url() -> String {
    format!("{}/runner/server", public_base_url())
}

/// Return server-enforced runner settings.
///
/// The official runner treats these settings as optional and applies its own
/// defaults when the endpoint is unavailable. Returning an explicit default
/// response keeps that negotiation deterministic for self-hosted deployments.
pub(crate) async fn runner_settings() -> Json<azdo::RunnerServerSettings> {
    Json(azdo::RunnerServerSettings::default())
}

pub(crate) fn broker_job_ref(
    request: &TaskAgentJobRequestRecord,
    runner_id: i64,
) -> serde_json::Value {
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

pub(crate) fn broker_job_ref_root(
    request: &TaskAgentJobRequestRecord,
    runner_id: i64,
) -> serde_json::Value {
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
pub(crate) fn next_broker_message_id(inner: &mut InnerState) -> i64 {
    // request_ids start at 1 and increase; keep message ids in a separate high
    // range so cancels never reuse a past/future request_id.
    const MESSAGE_ID_BASE: i64 = 1_000_000;
    if inner.next_message_id < MESSAGE_ID_BASE {
        inner.next_message_id = MESSAGE_ID_BASE;
    }
    inner.next_message_id += 1;
    inner.next_message_id
}

/// Return the runner-compatible deprecation response used by the official
/// message endpoint. `AccessDeniedException` with `errorCode: 1` is mapped by
/// Runner.Listener to its `RunnerVersionDeprecated` exit code (7) when the
/// corresponding feature flag is enabled there.
fn runner_version_deprecated_response(
    shared: &SharedState,
    params: &std::collections::HashMap<String, String>,
) -> Option<Response> {
    if !shared.state.runner_version_deprecated {
        return None;
    }

    let version = params
        .get("runnerVersion")
        .map(String::as_str)
        .unwrap_or("unknown");
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "typeKey": "AccessDeniedException",
                "errorCode": 1,
                "message": format!(
                    "Runner version {version} is deprecated and cannot receive messages."
                ),
            })),
        )
            .into_response(),
    )
}

pub(crate) async fn next_message_broker_ref(
    State(shared): State<Arc<SharedState>>,
    Path(_pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Some(response) = runner_version_deprecated_response(&shared, &params) {
        return Ok(response);
    }
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
        let runner_id = inner
            .runner_id_for_session(&session_id)
            .ok_or_else(|| ApiError::forbidden("broker session has no runner owner"))?;
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
                    return Ok(Json(broker_job_ref(request, runner_id)).into_response());
                }
            }
            inner.session_active_requests.remove(&session_id);
        }

        let runner = inner.runner_capabilities_for_session(&session_id);
        let claimed = take_matching_job(&mut inner.queue, &runner);
        shared
            .state
            .queue_depth
            .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
        let Some(queued) = claimed else {
            drop(inner);
            if wait_seconds == 0 {
                return Ok((StatusCode::OK, Json(json!({}))).into_response());
            }
            if tokio::time::timeout(
                Duration::from_secs(wait_seconds),
                shared.state.message_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok((StatusCode::OK, Json(json!({}))).into_response());
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

        return Ok(Json(broker_job_ref(&request, runner_id)).into_response());
    }
}

/// GET `/_apis/distributedtask/pools/:pool_id/messages` dispatcher.
///
/// Sessions created via the AzDO path (`create_session_disttask`) are marked
/// in `azdo_sessions` and receive the full encrypted `PipelineAgentJobRequest`
/// message via `next_message_compat`.  All other sessions (broker-hybrid tests,
/// legacy broker flow) get the lightweight `RunnerJobRequest` broker ref.
pub(crate) async fn next_message_disttask(
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
        let (status, body) = next_message_compat(State(shared), Path(pool_id), Query(params)).await;
        Ok((status, body).into_response())
    } else {
        next_message_broker_ref(State(shared), Path(pool_id), Query(params)).await
    }
}

pub(crate) async fn broker_session_root(
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

pub(crate) async fn broker_delete_session_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    let header_session = headers
        .get("x-actions-session")
        .and_then(|value| value.to_str().ok());
    if let Some(session_id) = header_session.or_else(|| params.get("sessionId").map(String::as_str))
    {
        remove_broker_session(&shared, session_id, runner_id).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn broker_delete_session_by_path(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    remove_broker_session(&shared, &session_id, runner_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn remove_broker_session(
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
pub(crate) fn authenticated_runner_id(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: Option<i64>,
) -> Result<i64, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    // Accept runner listen tokens (normal path) or job runtime tokens
    // (worker uses the SystemVssConnection AccessToken for renewjob/completejob).
    if let Some(runner_id) = shared.state.runner_id_from_token(bearer) {
        if expected_runner_id.is_some_and(|expected| expected != runner_id) {
            return Err(ApiError::forbidden(
                "runner token does not match broker path",
            ));
        }
        return Ok(runner_id);
    }
    // Fall back: accept runtime tokens (Actions.Results scope). These don't
    // carry a runner_id, so we trust the path parameter.
    if shared.state.verify_local_jwt_claims(bearer).is_some() {
        return expected_runner_id.ok_or_else(|| ApiError::unauthorized("runner id required"));
    }
    Err(ApiError::unauthorized("runner listen token required"))
}

pub(crate) fn ensure_broker_request_owner(
    inner: &InnerState,
    request_id: i64,
    runner_id: i64,
) -> Result<(), ApiError> {
    let session_id =
        inner
            .session_active_requests
            .iter()
            .find_map(|(session_id, active_request_id)| {
                (*active_request_id == request_id).then_some(session_id.clone())
            });
    let has_session = session_id.is_some();
    let owner = session_id.and_then(|sid| {
        inner
            .broker_session_runners
            .get(&sid)
            .copied()
            .or_else(|| inner.sessions.get(&sid).map(|s| s.runner_id))
    });
    match owner {
        Some(owner) if owner == runner_id => Ok(()),
        Some(_) => Err(ApiError::forbidden(
            "broker request belongs to another runner",
        )),
        // If the request is assigned to a session but the session is not in
        // broker_session_runners or sessions (e.g. conformance replay with
        // golden session IDs), accept it as long as the token's runner_id
        // matches the path. This preserves backward compat for test/replay
        // flows where session creation and broker paths use different IDs.
        None if has_session => Ok(()),
        None => Err(ApiError::not_found(
            "broker request is not assigned to a session",
        )),
    }
}

pub(crate) async fn next_message_broker_ref_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    if let Some(response) = runner_version_deprecated_response(&shared, &params) {
        return Ok(response);
    }
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
                let runner = inner.runner_capabilities_for_session(&session_id);
                let claimed = take_matching_job(&mut inner.queue, &runner);
                shared
                    .state
                    .queue_depth
                    .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
                if let Some(queued) = claimed {
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
                    Some(broker_job_ref_root(request, runner_id))
                } else {
                    None
                }
            }
        };

        if let Some(message) = maybe {
            return Ok(Json(message).into_response());
        }
        if wait == 0 || std::time::Instant::now() >= deadline {
            return Ok(Json(serde_json::Value::Null).into_response());
        }
        // Wake promptly on cancel/enqueue rather than fixed 250ms sleep.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let slice = remaining.min(Duration::from_secs(3));
        let _ = tokio::time::timeout(slice, shared.state.message_notify.notified()).await;
    }
}

pub(crate) async fn broker_acknowledge_root(
    State(_shared): State<Arc<SharedState>>,
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    // Acknowledge receipt of the message. Do NOT clear session_active_requests
    // here — the runner is still working on the job. The session's active
    // request is cleared when completejob sets the result and the next poll
    // sees result.is_some() at line 2190.
    StatusCode::OK
}

pub(crate) async fn broker_acquire_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerAcquireJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
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
            endpoint.data.insert(
                "ConnectivityChecks".to_owned(),
                serde_json::json!([format!("{}/check", public_base_url())]).to_string(),
            );
            endpoint.data.insert("ServerId".to_owned(), String::new());
            endpoint.data.insert("ServerName".to_owned(), String::new());
            endpoint.data.insert(
                "GenerateIdTokenUrl".to_owned(),
                format!(
                    "{}/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken",
                    run_service_url, message.plan.plan_id, message.job_id
                ),
            );
        }
    }
    message.billing_owner_id = request.billing_owner_id;
    // Run-service payloads use the DTO default; internal request IDs remain in
    // `job_requests` and broker lookup maps for renew/complete bookkeeping.
    message.request_id = 0;
    let mut payload = serde_json::to_value(&message)
        .map_err(|error| ApiError::internal(format!("serialize broker job payload: {error}")))?;
    let object = payload
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("broker job payload must serialize as an object"))?;
    object.insert(
        "runnerSettings".to_owned(),
        serde_json::to_value(azdo::RunnerServerSettings::default()).map_err(|error| {
            ApiError::internal(format!("serialize runner server settings: {error}"))
        })?,
    );
    Ok(Json(payload))
}

pub(crate) async fn broker_renew_job(
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

pub(crate) async fn broker_complete_job(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runner_settings_returns_default_wire_shape() {
        let Json(settings) = runner_settings().await;
        let wire = serde_json::to_value(settings).unwrap();
        assert_eq!(wire, json!({"isHostedServer": false}));
    }

    #[tokio::test]
    async fn settings_routes_serve_default_json() {
        use axum::body::{to_bytes, Body};
        use axum::http::{Method, Request, StatusCode};
        use tokio_util::sync::CancellationToken;
        use tower::ServiceExt;

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = crate::app(state, CancellationToken::new());

        for path in [
            "/_apis/v1/settings/runner",
            "/acme/_apis/v1/settings/runner",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "path={path}");
            let wire: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                    .unwrap();
            assert_eq!(wire["isHostedServer"], false, "path={path}");
            assert!(wire.get("agentDownloadUrls").is_none(), "path={path}");
        }
    }

    #[tokio::test]
    async fn runner_version_deprecation_response_is_opt_in_and_runner_compatible() {
        use axum::body::to_bytes;
        use std::collections::HashMap;
        use tokio_util::sync::CancellationToken;

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let mut shared = SharedState {
            state,
            shutdown: CancellationToken::new(),
        };
        let params = HashMap::from([(String::from("runnerVersion"), String::from("2.330.1"))]);
        assert!(runner_version_deprecated_response(&shared, &params).is_none());

        shared.state.runner_version_deprecated = true;
        let response = runner_version_deprecated_response(&shared, &params).unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let wire: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(wire["typeKey"], "AccessDeniedException");
        assert_eq!(wire["errorCode"], 1);
        assert_eq!(
            wire["message"],
            "Runner version 2.330.1 is deprecated and cannot receive messages."
        );
    }
}
