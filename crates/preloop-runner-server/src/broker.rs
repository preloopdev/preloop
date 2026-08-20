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
    #[serde(default)]
    pub(crate) annotations: Vec<serde_json::Value>,
    #[serde(default)]
    pub(crate) step_results: Vec<preloop_gha_protocol::CompletionStepResult>,
}

pub(crate) fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
    match result.to_ascii_lowercase().as_str() {
        "success" | "succeeded" | "succeededwithissues" => Some(ExecutionStatus::Success),
        "failure" | "failed" => Some(ExecutionStatus::Failure),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
        "skipped" => Some(ExecutionStatus::Skipped),
        // Official TaskResult.Abandoned: the runner reports it when the job
        // never finished on it (lease lost, first renew failed). GitHub
        // concludes such jobs as failed — no retry for self-hosted runners.
        "abandoned" => Some(ExecutionStatus::Failure),
        _ => None,
    }
}

pub(crate) fn broker_run_service_url(runner_id: i64) -> String {
    format!("{}/broker/{runner_id}/", runner_base_url())
}

/// Runner-facing base URL: the origin embedded in connectionData, broker
/// endpoint data, Twirp signed URLs, and job-message variables. Defaults to
/// `PRELOOP_RUNNER_URL`, falling back to `PRELOOP_PUBLIC_URL` for standalone
/// `preloop-runner-server` deployments. `preloop serve` pins this to the loopback
/// listen origin so in-VM runners and their jobs reach the host exclusively
/// via the mounted control socket and in-guest loopback bridge, never the
/// public tunnel.
pub(crate) fn runner_base_url() -> String {
    std::env::var("PRELOOP_RUNNER_URL")
        .unwrap_or_else(|_| public_base_url())
        .trim_end_matches('/')
        .to_owned()
}

pub(crate) fn public_base_url() -> String {
    std::env::var("PRELOOP_PUBLIC_URL")
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
        format!("{}/oidc", runner_base_url())
    } else {
        inner.oidc_issuer.clone()
    }
}

pub(crate) fn websocket_base_url() -> String {
    let base = runner_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

pub(crate) fn runner_server_url() -> String {
    format!("{}/runner/server", runner_base_url())
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
    identity: Option<axum::Extension<RunnerIdentity>>,
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
        inner.mark_session_seen(&session_id);
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
        let verified = effective_claim_runner(
            identity.as_ref().map(|axum::Extension(id)| id),
            Some(runner_id),
        );
        let claimed = take_matching_job(&mut inner, &runner, verified);
        shared
            .state
            .queue_depth
            .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
        runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
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
            run.started_at.get_or_insert_with(chrono::Utc::now);
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
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());
    {
        let mut inner = shared.state.inner.lock().await;
        inner.mark_session_seen(&session_id);
    }
    let is_azdo = {
        let inner = shared.state.inner.lock().await;
        inner.azdo_sessions.contains(&session_id)
    };
    if is_azdo {
        let (status, body) =
            next_message_compat(State(shared), Path(pool_id), identity, Query(params)).await;
        Ok((status, body).into_response())
    } else {
        next_message_broker_ref(State(shared), Path(pool_id), identity, Query(params)).await
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
            "ownerName": "preloop-runner",
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
        let mut inner = shared.state.inner.lock().await;
        inner.mark_session_seen(&session_id);
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
                let claimed = take_matching_job(&mut inner, &runner, Some(runner_id));
                shared
                    .state
                    .queue_depth
                    .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
                runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
                if let Some(queued) = claimed {
                    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
                        run.status = ExecutionStatus::InProgress;
                        run.started_at.get_or_insert_with(chrono::Utc::now);
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
    let (request_id, mut message, github_token_request, id_token_granted) = {
        let inner = shared.state.inner.lock().await;
        let request_id = inner
            .agent_job_requests
            .get(&request.job_message_id)
            .copied()
            .ok_or_else(|| ApiError::not_found("broker job message not found"))?;
        ensure_broker_request_owner(&inner, request_id, runner_id)?;
        let message = inner
            .broker_messages
            .get(&request_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("broker job payload not found"))?;
        let id_token_granted = inner
            .job_requests
            .get(&request_id)
            .and_then(|record| {
                inner
                    .id_token_grants
                    .get(&(record.run_id, record.job_id.clone()))
                    .copied()
            })
            .unwrap_or(false);
        (
            request_id,
            message,
            inner.github_token_requests.get(&request_id).cloned(),
            id_token_granted,
        )
    };
    // The token request is registered at build time and kept until the job
    // is terminal, so a re-claim after a runner disconnect re-mints under the
    // *same* conditions the job was built with — the original permission set
    // (fork profile included) and its fallback restrictions. Rebuilding from
    // the message would lose both: the default permission set is wider than
    // many jobs' declared set, and the untrusted flag cannot be recovered.
    let token_request = github_token_request;
    if let Some(token_request) = token_request {
        tracing::info!(
            request_id,
            repository = %token_request.repository,
            "broker acquire: dispatch token request present"
        );
        // The polling path has already dequeued this job, marked the run
        // `InProgress` and pinned the request to this session, so bubbling the
        // mint refusal out as a 502 would leave nothing holding the claim: the
        // runner re-acquires, fails identically, and the run sits `InProgress`
        // until the 600s disconnect reaper notices. A refusal under the `error`
        // policy is a configuration fault that no retry can clear, so the claim
        // is failed terminally instead of being returned to the queue.
        let minted = match mint_dispatch_github_token(&shared, &token_request).await {
            Ok(minted) => minted,
            Err(error) => {
                fail_unclaimable_request(&shared, request_id).await;
                return Err(error);
            }
        };
        if let Some(minted) = minted {
            let token = minted.token;
            tracing::info!(
                token_len = token.len(),
                "minted dispatch GitHub token at claim"
            );
            message.variables.insert(
                "system.github.token".to_owned(),
                preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            message.variables.insert(
                "github_token".to_owned(),
                preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            // The build-time message also injects the token as `GITHUB_TOKEN`
            // (the `${{ secrets.GITHUB_TOKEN }}` alias). It must follow the
            // minted token too, or a fork job's hostile step code could read
            // the stale local runtime token from `secrets.GITHUB_TOKEN`
            // while `github.token` already carries the scoped mint.
            message.variables.insert(
                "GITHUB_TOKEN".to_owned(),
                preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            // Restate what the token carries when the installation could not
            // grant everything. The message was built with the requested set,
            // and leaving it would print authority the token does not have in
            // the runner's `GITHUB_TOKEN Permissions` group — sending anyone
            // debugging the resulting 403 to the wrong place. The narrowed
            // grant replaces only App-scoped entries: Actions-only metadata
            // (`IdToken: write` for a trusted job whose OIDC grant is still
            // live) is preserved from the build-time wire set, and a
            // fork-restricted job's wire set has no IdToken to preserve.
            if let Some(effective) = minted.effective_permissions {
                let merged = merge_narrowed_wire_permissions(
                    message
                        .variables
                        .get("system.github.token.permissions")
                        .and_then(|variable| variable.value.as_deref()),
                    &effective,
                );
                message.variables.insert(
                    "system.github.token.permissions".to_owned(),
                    preloop_gha_protocol::azdo::VariableValue::new(
                        preloop_gha_parser::job_builder::token_permissions_wire_json(&merged),
                    ),
                );
            }
            // The workflow's `github` context is built at submission time,
            // before the App token can exist, so `${{ github.token }}`
            // inputs (actions/checkout's token, the persist-credentials
            // config, the non-persist temp-config include) resolve empty
            // and every git fetch prompts for a username. Patch the minted
            // token into the context at claim so checkout authenticates
            // exactly like it does on GitHub-hosted runners — no runner-side
            // env header needed (an env `extraheader` would duplicate the
            // one checkout persists itself: "Duplicate header: Authorization",
            // HTTP 400).
            match message.context_data.get_mut("github") {
                Some(preloop_gha_protocol::azdo::PipelineContextData::Dict(github)) => {
                    github.insert(
                        "token".to_owned(),
                        preloop_gha_protocol::azdo::PipelineContextData::String(token),
                    );
                    tracing::info!("patched minted token into github context");
                }
                other => tracing::warn!(
                    github_context = %match other { Some(_) => "non-dict", None => "missing" },
                    "could not patch github context token"
                ),
            }
        }
        // The token request stays registered for the job's lifetime so a
        // re-claim re-mints under the build-time conditions (permission set
        // and fallback restrictions). `fail_unclaimable_request`,
        // `complete_job_inner` (claimed-job completion paths funnel there)
        // and `retire_node_requests` remove it once the job is terminal;
        // see the `distributed_task.rs` completion path for the known gap.
        let mut inner = shared.state.inner.lock().await;
        inner.broker_messages.insert(request_id, message.clone());
    } else {
        // A token request registered at build time can be lost when the
        // process dies before the next store snapshot flush (jobs enqueued
        // since the last snapshot restore with `github_token_requests`
        // missing). The claim then reaches here with no request to mint
        // from, the checkout keeps the local runtime JWT, and every git
        // fetch fails on auth. Re-derive the request from the run's
        // submission and the job's declared permissions — the same inputs
        // `build_job_artifacts` used — and mint under that policy.
        let derived = if shared.state.github_app.is_none() {
            None
        } else {
            let inner = shared.state.inner.lock().await;
            let record = inner.job_requests.get(&request_id);
            let run = record.and_then(|record| inner.runs.get(&record.run_id));
            match (record, run) {
                (Some(record), Some(run)) => {
                    // The submission stores the tier as a plain kebab-case
                    // string (e.g. "untrusted-fork-pull-request"), not JSON.
                    // `from_str` expects JSON and would reject the bare
                    // string, yielding `None` — which `job_authorization`
                    // treats as trusted, silently un-restricting a fork
                    // job's token. Parse via a JSON string value so the
                    // kebab-case variant decodes.
                    let tier = run.submission.trust_tier.as_deref().and_then(|tier| {
                        serde_json::from_value(serde_json::Value::String(tier.to_owned())).ok()
                    });
                    // The job's resolved permission set lives in the
                    // persisted message's `system.github.token.permissions`
                    // variable (PascalCase wire spelling) — the same
                    // variable the build path wrote from `JobPlan`
                    // permissions. The event payload's `workflow_job` key is
                    // absent for push/PR/dispatch events, so reading it there
                    // would fall back to the broad default and grant scopes
                    // the workflow withheld. Recover from the message
                    // instead, converting the wire spelling back to
                    // kebab-case for the token request.
                    let wire_permissions = message
                        .variables
                        .get("system.github.token.permissions")
                        .and_then(|variable| variable.value.as_deref())
                        .and_then(|json| {
                            serde_json::from_str::<BTreeMap<String, String>>(json).ok()
                        });
                    // `system.github.token.permissions` carries the effective
                    // set (defaults substituted when nothing was declared).
                    // Passing it as the declared set is faithful: for a
                    // declared job it is exactly the job's set, and for an
                    // undeclared job `job_authorization` treats a set equal
                    // to the default identically to `None`. The fork case is
                    // safe too — the wire variable was restated to the fork
                    // profile at build, and clamping it again is idempotent.
                    let declared = wire_permissions.clone();
                    let policy = crate::events::trust_tier::job_authorization(
                        tier,
                        declared.as_ref(),
                        false,
                    );
                    Some((
                        crate::models::GitHubTokenRequest {
                            repository: run.submission.repository.clone(),
                            permissions: policy.app_permissions,
                            declared: declared.is_some(),
                            untrusted: policy.fork_restricted,
                        },
                        record.request_id,
                    ))
                }
                _ => None,
            }
        };
        if let Some((token_request, derived_request_id)) = derived {
            // Register the derived request so a re-claim after a disconnect
            // re-mints under the same derived policy, then mint.
            {
                let mut inner = shared.state.inner.lock().await;
                inner
                    .github_token_requests
                    .insert(derived_request_id, token_request.clone());
            }
            tracing::info!(
                request_id,
                repository = %token_request.repository,
                "broker acquire: re-derived missing dispatch token request at claim"
            );
            let minted = match mint_dispatch_github_token(&shared, &token_request).await {
                Ok(minted) => minted,
                Err(error) => {
                    fail_unclaimable_request(&shared, request_id).await;
                    return Err(error);
                }
            };
            if let Some(minted) = minted {
                let token = minted.token;
                tracing::info!(
                    token_len = token.len(),
                    "minted re-derived dispatch GitHub token at claim"
                );
                message.variables.insert(
                    "system.github.token".to_owned(),
                    preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
                );
                message.variables.insert(
                    "github_token".to_owned(),
                    preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
                );
                message.variables.insert(
                    "GITHUB_TOKEN".to_owned(),
                    preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
                );
                // Restate what the token carries when the installation could
                // not grant everything, mirroring the normal mint path: the
                // recovered message's wire set is the requested set, and
                // leaving it would print authority the token does not have.
                if let Some(effective) = minted.effective_permissions {
                    let merged = merge_narrowed_wire_permissions(
                        message
                            .variables
                            .get("system.github.token.permissions")
                            .and_then(|variable| variable.value.as_deref()),
                        &effective,
                    );
                    message.variables.insert(
                        "system.github.token.permissions".to_owned(),
                        preloop_gha_protocol::azdo::VariableValue::new(
                            preloop_gha_parser::job_builder::token_permissions_wire_json(&merged),
                        ),
                    );
                }
                match message.context_data.get_mut("github") {
                    Some(preloop_gha_protocol::azdo::PipelineContextData::Dict(github)) => {
                        github.insert(
                            "token".to_owned(),
                            preloop_gha_protocol::azdo::PipelineContextData::String(token),
                        );
                    }
                    other => tracing::warn!(
                        github_context = %match other { Some(_) => "non-dict", None => "missing" },
                        "could not patch github context token for re-derived request"
                    ),
                }
                let mut inner = shared.state.inner.lock().await;
                inner.broker_messages.insert(request_id, message.clone());
            }
        }
    }
    // The snapshot checkout token is pinned onto the step at submission,
    // but a job can sit queued well past its ~50-minute lifetime. The
    // checkout would then be answered with a git 401 that the step can
    // never recover from — it replays whatever the message carries. Re-mint
    // the pinned inputs at claim so the token is fresh exactly when the job
    // first runs.
    let re_minted = re_mint_snapshot_tokens(&mut message, &shared.state);
    if re_minted > 0 {
        tracing::info!(
            request_id,
            steps = re_minted,
            "re-minted snapshot checkout tokens at claim"
        );
    }
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
            endpoint.data.insert(
                "ResultsServiceUrl".to_owned(),
                format!("{}/", runner_base_url()),
            );
            endpoint
                .data
                .insert("PipelinesServiceUrl".to_owned(), runner_server_url());
            endpoint.data.insert(
                "CacheServerUrl".to_owned(),
                format!("{}/", runner_base_url()),
            );
            endpoint.data.insert(
                "FeedStreamUrl".to_owned(),
                format!("{}/ws/live-logs/{}", websocket_base_url(), message.job_id),
            );
            endpoint.data.insert(
                "ConnectivityChecks".to_owned(),
                serde_json::json!([format!("{}/check", runner_base_url())]).to_string(),
            );
            endpoint.data.insert(
                "ConnectivityAndDNSChecks".to_owned(),
                serde_json::json!([format!("{}/check", runner_base_url())]).to_string(),
            );
            endpoint.data.insert("ServerId".to_owned(), String::new());
            endpoint.data.insert("ServerName".to_owned(), String::new());
            // The runner copies GenerateIdTokenUrl into the step environment
            // as `ACTIONS_ID_TOKEN_REQUEST_URL`; emitting it for a job
            // without an `id-token: write` grant (fork-restricted jobs
            // never have one) would invite a token request the endpoint then
            // refuses. Match the build-time message: URL only when granted.
            if id_token_granted {
                endpoint.data.insert(
                    "GenerateIdTokenUrl".to_owned(),
                    format!(
                        "{}/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken",
                        run_service_url, message.plan.plan_id, message.job_id
                    ),
                );
            }
        }
    }
    message.billing_owner_id = request.billing_owner_id;
    // Run-service payloads use the DTO default; internal request IDs remain in
    // `job_requests` and broker lookup maps for renew/complete bookkeeping.
    message.request_id = 0;
    let payload = serde_json::to_value(&message)
        .map_err(|error| ApiError::internal(format!("serialize broker job payload: {error}")))?;
    Ok(Json(payload))
}

/// Replace every snapshot checkout credential pinned at submission with a
/// freshly minted runtime token.
///
/// Returns the number of steps refreshed. The pinned ids travel on the
/// message ([`azdo::AgentJobRequestMessage::preloop_snapshot_token_steps`]),
/// so this deliberately matches by step id rather than by token shape.
pub(crate) fn re_mint_snapshot_tokens(
    message: &mut preloop_gha_protocol::azdo::AgentJobRequestMessage,
    state: &AppState,
) -> usize {
    let Some(pinned) = message.preloop_snapshot_token_steps.as_ref() else {
        return 0;
    };
    let pinned: std::collections::HashSet<uuid::Uuid> = pinned
        .iter()
        .filter_map(|id| uuid::Uuid::parse_str(id).ok())
        .collect();
    if pinned.is_empty() {
        return 0;
    }
    let fresh = state.mint_runtime_token(&message.plan.plan_id, &message.job_id);
    let mut re_minted = 0;
    for step in &mut message.steps {
        if pinned.contains(&step.id) {
            step.inputs.insert("token".to_owned(), fresh.clone());
            re_minted += 1;
        }
    }
    re_minted
}

/// A dispatched job's `GITHUB_TOKEN` and, when the App installation could not
/// grant everything requested, the set the token actually carries.
#[derive(Debug)]
pub(crate) struct MintedGitHubToken {
    pub(crate) token: String,
    pub(crate) effective_permissions: Option<BTreeMap<String, String>>,
}

/// Release a claimed request that can never be dispatched, using the same
/// bookkeeping `broker_complete_job` performs so the run summary, the session
/// slot and the concurrency release all behave as they do for a
/// runner-reported failure.
async fn fail_unclaimable_request(shared: &Arc<SharedState>, request_id: i64) {
    let run_job = {
        let mut inner = shared.state.inner.lock().await;
        // Nothing will consume the deferred token request now, and leaving it
        // behind keeps the job's requested permissions alive for a request that
        // is already terminal.
        inner.github_token_requests.remove(&request_id);
        if let Some(record) = inner.job_requests.get_mut(&request_id) {
            record.result = Some(ExecutionStatus::Failure);
            record.locked_until = agent_request_locked_until();
        }
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != request_id);
        inner.inflight_requests.remove(&request_id).or_else(|| {
            job_request_tuple(&inner, request_id).map(|(_, run_id, job_id)| (run_id, job_id))
        })
    };
    if let Some((run_id, job_id)) = run_job {
        let completion = JobCompletion {
            run_id,
            job_id,
            status: ExecutionStatus::Failure,
            outputs: preloop_gha_protocol::OutputMap::new(),
            annotations: Vec::new(),
            step_results: Vec::new(),
        };
        // The caller is already returning the mint failure to the runner, so a
        // secondary bookkeeping error must not mask it.
        if let Err(error) = complete_job_inner(shared.clone(), completion).await {
            warn!(
                request_id,
                status = %error.into_response().status(),
                "failing an undispatchable job did not complete its run"
            );
        }
    }
    // Let a long-polling runner pick up a successor immediately rather than
    // waiting out its poll window behind a job that will never run.
    shared.state.message_notify.notify_waiters();
}

/// Fail job claims pinned to sessions that did not survive a restart.
///
/// Pool machines are ephemeral: a control-plane restart destroys their VMs,
/// but `session_active_requests` is persisted. Those claims come back pinned
/// to sessions that will never poll again, so no fresh machine can take the
/// job — the run, and the GitHub check run it created, sit queued forever
/// while the pool idles. Failing them once at startup makes the reported
/// state honest and releases the concurrency slot.
///
/// Only claims whose session is gone are touched. A queued job that was never
/// claimed still has its queue row and is dispatched normally, and a runner
/// that outlived the control plane keeps a live session, so its job is left
/// to the ordinary disconnect reaper.
pub(crate) async fn reconcile_orphaned_claims(shared: &Arc<SharedState>) -> usize {
    let orphaned: Vec<i64> = {
        let inner = shared.state.inner.lock().await;
        inner
            .session_active_requests
            .iter()
            .filter(|(session_id, _)| !inner.sessions.contains_key(*session_id))
            .map(|(_, request_id)| *request_id)
            .filter(|request_id| {
                inner
                    .job_requests
                    .get(request_id)
                    .is_some_and(|record| record.result.is_none())
            })
            .collect()
    };
    for request_id in &orphaned {
        fail_unclaimable_request(shared, *request_id).await;
    }
    if !orphaned.is_empty() {
        warn!(
            count = orphaned.len(),
            "failed job claims orphaned by a control-plane restart"
        );
    }
    orphaned.len()
}

pub(crate) async fn mint_dispatch_github_token(
    shared: &Arc<SharedState>,
    request: &GitHubTokenRequest,
) -> Result<Option<MintedGitHubToken>, ApiError> {
    let started = std::time::Instant::now();
    let Some(app) = crate::github_app::select_app_for_repo(shared, &request.repository).await
    else {
        // No registered GitHub App covers this repository. The legacy
        // single-App path always had an App, so mint failures flowed through
        // the configured mint-failure policy; apply the default App's policy
        // rather than silently bypassing it. An untrusted fork job must never
        // fall back to the PAT (it is repository-unscoped and ignores
        // `permissions:`), so it keeps the local runtime token instead.
        if request.untrusted {
            return Ok(None);
        }
        let Some(default_app) = &shared.state.github_app else {
            return Ok(None);
        };
        warn!(
            repository = %request.repository,
            "No registered GitHub App covers the repository; applying the default App's mint-failure policy"
        );
        let fallback = crate::github_app::fallback_token(
            default_app.mint_failure,
            default_app.pat_fallback.clone(),
        )
        .map_err(|refusal| {
            ApiError::bad_gateway(format!(
                "GitHub App token minting failed for {}: {refusal}",
                request.repository
            ))
        })?;
        return Ok(match fallback {
            Some(token) => {
                info!(
                    repository = %request.repository,
                    duration_ms = started.elapsed().as_millis() as u64,
                    "GitHub token minted at claim (fallback path)"
                );
                warn!(
                    repository = %request.repository,
                    "No registered GitHub App covers the repository; using configured PAT fallback"
                );
                Some(MintedGitHubToken {
                    token,
                    effective_permissions: None,
                })
            }
            None => {
                warn!(
                    repository = %request.repository,
                    "No registered GitHub App covers the repository; job retains local runtime token"
                );
                None
            }
        });
    };
    let minted = match crate::github_app::get_or_mint_token_declared(
        &app,
        &request.repository,
        &request.permissions,
        request.declared,
    )
    .await
    {
        Ok((token, effective_permissions)) => Some(MintedGitHubToken {
            token,
            effective_permissions,
        }),
        Err(error) => {
            // An untrusted fork job must never fall back to the PAT: the
            // PAT is repository-unscoped and ignores `permissions:`, so
            // handing it to fork PR code would grant authority GitHub's
            // read-only fork profile (and this job's downgraded request)
            // never allowed. The job keeps the local runtime token, which
            // authenticates only against this control plane.
            if request.untrusted {
                warn!(
                    repository = %request.repository,
                    "GitHub App token minting failed for an untrusted job; \
                     refusing the PAT fallback, job retains the local runtime token: {error:#}"
                );
                return Ok(None);
            }
            let fallback =
                crate::github_app::fallback_token(app.mint_failure, app.pat_fallback.clone())
                    .map_err(|refusal| {
                        ApiError::bad_gateway(format!(
                            "GitHub App token minting failed for {}: {error:#} ({refusal})",
                            request.repository
                        ))
                    })?;
            info!(
                    repository = %request.repository,
                duration_ms = started.elapsed().as_millis() as u64,
                "GitHub token minted at claim (fallback path)"
            );
            if fallback.is_some() {
                warn!(
                    repository = %request.repository,
                    "GitHub App token minting failed; using configured PAT fallback: {error:#}"
                );
            } else {
                warn!(
                    repository = %request.repository,
                    "GitHub App token minting failed; job retains local runtime token: {error:#}"
                );
            }
            fallback.map(|token| MintedGitHubToken {
                token,
                effective_permissions: None,
            })
        }
    };
    info!(
        repository = %request.repository,
        duration_ms = started.elapsed().as_millis() as u64,
        "GitHub token minted at claim"
    );
    Ok(minted)
}

/// Merge a minted token's effective (App-scoped) permission set into the
/// runner-visible wire permissions.
///
/// `original_wire` is the `system.github.token.permissions` variable the
/// message carried at claim time — the policy set built in
/// `build_job_artifacts`. The effective grant is authoritative for App
/// repository scopes (a scope the installation dropped disappears from the
/// wire, so the runner's `GITHUB_TOKEN Permissions` group never overstates
/// the token), but Actions-only scopes (`id-token`, `models`) never appear
/// in an installation grant, so their build-time metadata is preserved:
/// a trusted job declared `id-token: write` keeps `IdToken: write` while its
/// OIDC grant is live, and a fork-restricted job's wire set has no IdToken
/// entry to preserve. Never reconstructs from broader defaults: a missing or
/// unparseable original wire set degrades to the effective grant alone.
fn merge_narrowed_wire_permissions(
    original_wire: Option<&str>,
    effective: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut merged: BTreeMap<String, String> = effective.clone();
    let Some(original_wire) = original_wire else {
        return merged;
    };
    let Ok(original) = serde_json::from_str::<BTreeMap<String, String>>(original_wire) else {
        return merged;
    };
    for (scope, level) in original {
        let scope = wire_scope_to_kebab(&scope);
        if crate::github_app::ACTIONS_ONLY_SCOPES.contains(&scope.as_str()) {
            // Keyed kebab-case so the merged map stays consistent with the
            // effective grant, whose keys come from the requested set;
            // `token_permissions_wire_json` pascal-cases them for the wire.
            merged.insert(scope, level);
        }
    }
    merged
}

/// `Checks` → `checks`, `PullRequests` → `pull-requests`: the workflow
/// (kebab-case) spelling of a PascalCase wire permission scope.
///
/// Not snake_case: the installation-token API spells the same scopes with
/// underscores (`pull_requests`), and mixing the two would silently drop
/// entries — see `github_app::clamp_to_grants`, which bridges kebab to
/// underscore explicitly.
fn wire_scope_to_kebab(scope: &str) -> String {
    let mut out = String::with_capacity(scope.len());
    for ch in scope.chars() {
        if ch.is_ascii_uppercase() {
            if !out.is_empty() {
                out.push('-');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
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
    let mut outputs = preloop_gha_protocol::OutputMap::new();
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
                    annotations: request.annotations.clone(),
                    step_results: request.step_results.clone(),
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

    #[test]
    fn abandoned_runner_result_concludes_failure() {
        // Official TaskResult.Abandoned: the job never finished on its runner
        // (lease lost / first renew failed). GitHub concludes abandoned
        // self-hosted jobs as failed — no retry, dependents skip.
        assert_eq!(
            execution_status_from_runner_result("abandoned"),
            Some(ExecutionStatus::Failure)
        );
        assert_eq!(
            execution_status_from_runner_result("Abandoned"),
            Some(ExecutionStatus::Failure)
        );
    }

    #[test]
    fn narrowed_grants_preserve_trusted_actions_only_metadata() {
        // Trusted job declared `checks: write` + `id-token: write`; the
        // installation granted only `pull-requests: read`. The effective
        // grant replaces App-scoped entries (checks disappears — the token
        // does not carry it) while `IdToken: write` metadata survives.
        let merged = merge_narrowed_wire_permissions(
            Some(r#"{"Checks":"write","IdToken":"write"}"#),
            &BTreeMap::from([("pull-requests".to_owned(), "read".to_owned())]),
        );
        assert_eq!(
            merged,
            BTreeMap::from([
                ("pull-requests".to_owned(), "read".to_owned()),
                ("id-token".to_owned(), "write".to_owned()),
            ]),
            "App scopes come from the effective grant; Actions-only metadata is preserved"
        );
    }

    #[test]
    fn narrowed_grants_never_add_actions_metadata_to_a_fork() {
        // Fork job's build-time wire set has no IdToken; the merge must not
        // invent one. The installation lacks `checks`, so the wire loses it.
        let merged = merge_narrowed_wire_permissions(
            Some(r#"{"Checks":"read","PullRequests":"read"}"#),
            &BTreeMap::from([("pull-requests".to_owned(), "read".to_owned())]),
        );
        assert_eq!(
            merged,
            BTreeMap::from([("pull-requests".to_owned(), "read".to_owned())]),
            "fork wire keeps no IdToken and drops ungranted App scopes"
        );
    }

    #[test]
    fn narrowed_grants_lower_declared_writes_to_the_granted_level() {
        let merged = merge_narrowed_wire_permissions(
            Some(r#"{"Contents":"write","IdToken":"write"}"#),
            &BTreeMap::from([
                ("contents".to_owned(), "read".to_owned()),
                ("metadata".to_owned(), "read".to_owned()),
            ]),
        );
        assert_eq!(
            merged,
            BTreeMap::from([
                ("contents".to_owned(), "read".to_owned()),
                ("metadata".to_owned(), "read".to_owned()),
                ("id-token".to_owned(), "write".to_owned()),
            ]),
            "declared write is lowered to the granted read; metadata survives"
        );
    }

    #[test]
    fn missing_or_unparseable_original_wire_degrades_to_the_effective_grant() {
        let effective = BTreeMap::from([("metadata".to_owned(), "read".to_owned())]);
        assert_eq!(
            merge_narrowed_wire_permissions(None, &effective),
            effective,
            "no original wire set: nothing to preserve"
        );
        assert_eq!(
            merge_narrowed_wire_permissions(Some("not json"), &effective),
            effective,
            "unparseable wire set must not reconstruct from broader defaults"
        );
    }

    #[test]
    fn wire_scope_to_kebab_round_trips_pascal_scopes() {
        assert_eq!(wire_scope_to_kebab("Checks"), "checks");
        assert_eq!(wire_scope_to_kebab("PullRequests"), "pull-requests");
        assert_eq!(wire_scope_to_kebab("IdToken"), "id-token");
        assert_eq!(wire_scope_to_kebab("Contents"), "contents");
    }
}
