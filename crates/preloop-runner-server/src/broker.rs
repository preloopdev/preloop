use super::*;
use crate::control::backend::ControlBackend;
use crate::control::types::ControlError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunnerAuthSource {
    RunnerListenToken,
    RuntimeJwt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedRunnerId {
    pub runner_id: i64,
    pub auth_source: RunnerAuthSource,
}
///
/// The two are deliberately separate counters. `Lifecycle` records a
/// rejected bare-listener-token attempt; official runner renew/complete calls
/// must use the job runtime token, so this counter is the compatibility gate
/// and must stay at zero in dogfood. `Acquire` records the Listener's own
/// `acquirejob`, where the listen token is the only credential it has. Folding
/// these into one counter would make that zero-gate unreachable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListenerTokenProbe {
    Lifecycle,
    Acquire,
}

pub fn record_listener_token_use(state: &AppState, probe: ListenerTokenProbe, route: &'static str) {
    use std::sync::atomic::Ordering::Relaxed;
    match probe {
        ListenerTokenProbe::Lifecycle => {
            let count = state.listener_token_lifecycle_calls.fetch_add(1, Relaxed) + 1;
            tracing::warn!(
                count,
                route,
                auth = "runner_listen_token",
                "job lifecycle call used the bare listener token; Plan 004's fencing carrier \
                 cannot ride the job runtime token"
            );
        }
        ListenerTokenProbe::Acquire => {
            let count = state.listener_token_acquire_calls.fetch_add(1, Relaxed) + 1;
            tracing::debug!(
                count,
                route,
                auth = "runner_listen_token",
                "acquirejob used the listener token (expected: the Listener holds no job token)"
            );
        }
    }
}

pub async fn authenticated_runner_id_with_source(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: Option<i64>,
) -> Result<AuthenticatedRunnerId, ApiError> {
    let bearer = crate::auth::bearer_from_headers(headers)
        .ok_or_else(|| ApiError::unauthorized("runner or job runtime token required"))?;
    if let Some(runner_id) = crate::auth::registered_runner_id(shared, bearer).await {
        if expected_runner_id.is_some_and(|expected| expected != runner_id) {
            return Err(ApiError::forbidden(
                "runner token does not match broker path",
            ));
        }
        return Ok(AuthenticatedRunnerId {
            runner_id,
            auth_source: RunnerAuthSource::RunnerListenToken,
        });
    }
    if shared.state.job_uuid_from_token(bearer).is_some() {
        let runner_id =
            expected_runner_id.ok_or_else(|| ApiError::unauthorized("runner id required"))?;
        return Ok(AuthenticatedRunnerId {
            runner_id,
            auth_source: RunnerAuthSource::RuntimeJwt,
        });
    }
    Err(ApiError::unauthorized(
        "runner or job runtime token required",
    ))
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerAcquireJobRequest {
    pub job_message_id: uuid::Uuid,
    // serde: accepted from the runner but not needed by acquisition logic.
    #[allow(dead_code)]
    pub billing_owner_id: Option<String>,
    // serde: accepted from the runner but not needed by acquisition logic.
    #[allow(dead_code)]
    pub runner_os: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerRenewJobRequest {
    pub job_id: uuid::Uuid,
    #[serde(rename = "planId")]
    pub _plan_id: String,
    pub conclusion: Option<String>,
    #[serde(default)]
    pub outputs: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub annotations: Vec<serde_json::Value>,
    #[serde(default)]
    pub step_results: Vec<preloop_gha_protocol::CompletionStepResult>,
}

pub fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
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

pub fn broker_run_service_url(runner_id: i64) -> String {
    format!("{}/broker/{runner_id}/", runner_base_url())
}

/// Runner-facing base URL: the origin embedded in connectionData, broker
/// endpoint data, Twirp signed URLs, and job-message variables. Defaults to
/// `PRELOOP_RUNNER_URL`, falling back to `PRELOOP_PUBLIC_URL` for standalone
/// `preloop-runner-server` deployments. `preloop serve` pins this to the loopback
/// listen origin so in-VM runners and their jobs reach the host exclusively
/// via the mounted control socket and in-guest loopback bridge, never the
/// public tunnel.
pub fn runner_base_url() -> String {
    std::env::var("PRELOOP_RUNNER_URL")
        .unwrap_or_else(|_| public_base_url())
        .trim_end_matches('/')
        .to_owned()
}

pub fn public_base_url() -> String {
    std::env::var("PRELOOP_PUBLIC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
        .trim_end_matches('/')
        .to_owned()
}

pub fn format_reusable_workflow_ref(
    repository: &str,
    workflow_ref: &str,
    caller_ref: &str,
) -> String {
    let local_path = workflow_ref
        .strip_prefix("./")
        .or_else(|| workflow_ref.strip_prefix("$/"));
    if let Some(path) = local_path {
        let (path, git_ref) = path.split_once('@').unwrap_or((path, caller_ref));
        return format!("{repository}/{path}@{git_ref}");
    }
    workflow_ref.to_owned()
}

pub fn normalize_oidc_issuer(value: String) -> anyhow::Result<String> {
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
pub fn oidc_issuer_url(inner: &InnerState) -> String {
    if inner.oidc_issuer.is_empty() {
        format!("{}/oidc", runner_base_url())
    } else {
        inner.oidc_issuer.clone()
    }
}

pub fn websocket_base_url() -> String {
    let base = runner_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

pub fn runner_server_url() -> String {
    format!("{}/runner/server", runner_base_url())
}

/// Return server-enforced runner settings.
///
/// The official runner treats these settings as optional and applies its own
/// defaults when the endpoint is unavailable. Returning an explicit default
/// response keeps that negotiation deterministic for self-hosted deployments.
pub async fn runner_settings() -> Json<azdo::RunnerServerSettings> {
    Json(azdo::RunnerServerSettings::default())
}

pub fn broker_job_ref(request: &TaskAgentJobRequestRecord, runner_id: i64) -> serde_json::Value {
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

pub fn broker_job_ref_root(
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
pub fn next_broker_message_id(tx: &mut crate::control::txstate::TxState) -> i64 {
    // request_ids start at 1 and increase; keep message ids in a separate high
    // range so cancels never reuse a past/future request_id.
    const MESSAGE_ID_BASE: i64 = 1_000_000;
    if tx.next_message_id < MESSAGE_ID_BASE {
        tx.next_message_id = MESSAGE_ID_BASE;
    }
    tx.next_message_id += 1;
    tx.next_message_id
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

pub async fn next_message_broker_ref(
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
    let runner_busy = params
        .get("status")
        .is_some_and(|status| status.eq_ignore_ascii_case("busy"));

    // Resolve the session's runner identity + capabilities once — they don't
    // change across long-poll iterations. The auth check (session has an
    // owner; a verified identity can't belong to another runner) rides the
    // same read.
    let (runner_id, runner) = shared
        .state
        .backend
        .session_owner(&session_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::forbidden("broker session has no runner owner"))?;
    if identity
        .as_ref()
        .and_then(|axum::Extension(identity)| identity.runner_id)
        .is_some_and(|identity_runner| identity_runner != runner_id)
    {
        return Err(ApiError::forbidden(
            "broker session belongs to another runner",
        ));
    }
    let verified = effective_claim_runner(
        identity.as_ref().map(|axum::Extension(id)| id),
        Some(runner_id),
    );
    // One window per request: a wake that loses the claim race must not
    // restart it, or a busy queue holds a poll open indefinitely.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);

    loop {
        let outcome = shared
            .state
            .backend
            .poll_session(crate::control::backend::PollRequest {
                session_id: session_id.clone(),
                verified_runner_id: verified,
                runner: runner.clone(),
                busy: runner_busy,
                wait_ms: 0,
            })
            .await
            .map_err(ApiError::from)?;

        match outcome {
            crate::control::types::PollOutcome::Inflight(message)
            | crate::control::types::PollOutcome::Cancel(message) => {
                return Ok(Json(message).into_response());
            }
            crate::control::types::PollOutcome::ActiveRequest { request, runner_id }
                if !runner_busy =>
            {
                return Ok(Json(broker_job_ref(&request, runner_id)).into_response());
            }
            crate::control::types::PollOutcome::Claimed(claimed) => {
                let crate::control::types::ClaimedJob {
                    queued,
                    request,
                    runner_id,
                    queue_depth,
                    next_runs_on,
                } = *claimed;
                shared
                    .state
                    .queue_depth
                    .store(queue_depth, std::sync::atomic::Ordering::Release);
                *shared.state.next_job_runs_on.write().unwrap() = next_runs_on;
                record_claim_queue_wait(&shared, &Some(queued.clone()));
                let run_id = queued.run_id;
                let job_id = queued.job_id.clone();

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
            // ActiveRequest while busy, or Empty: nothing to deliver — wait.
            _ => {}
        }

        // Long-poll: hold the request until work appears or the window ends.
        if wait_seconds == 0 {
            let status = if runner_busy {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            let body = if runner_busy {
                serde_json::Value::Null
            } else {
                json!({})
            };
            return Ok((status, Json(body)).into_response());
        }
        if tokio::time::timeout_at(deadline, shared.state.message_notify.notified())
            .await
            .is_err()
        {
            let status = if runner_busy {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            let body = if runner_busy {
                serde_json::Value::Null
            } else {
                json!({})
            };
            return Ok((status, Json(body)).into_response());
        }
    }
}

/// GET `/_apis/distributedtask/pools/:pool_id/messages` dispatcher.
///
/// Sessions created via the AzDO path (`create_session_disttask`) are marked
/// in `azdo_sessions` and receive the full encrypted `PipelineAgentJobRequest`
/// message via `next_message_compat`.  All other sessions (broker-hybrid tests,
/// legacy broker flow) get the lightweight `RunnerJobRequest` broker ref.
pub async fn next_message_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());
    // Heartbeat and protocol lookup in one statement — every poll runs this,
    // so it must not load the working set or wait on the writer lock.
    let is_azdo = matches!(
        shared
            .state
            .backend
            .touch_session(&session_id)
            .await
            .map_err(ApiError::from)?,
        Some(crate::control::types::SessionProtocol::Azdo)
    );
    if is_azdo {
        let (status, body) =
            next_message_compat(State(shared), Path(pool_id), identity, Query(params)).await;
        Ok((status, body).into_response())
    } else {
        next_message_broker_ref(State(shared), Path(pool_id), identity, Query(params)).await
    }
}

pub async fn broker_session_root(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None).await?;
    let session_id = uuid::Uuid::new_v4().to_string();
    {
        // Authentication and insertion must share a final registration check:
        // the liveness sweep may have purged this runner after token
        // validation but before this transaction was acquired — so the
        // contains_key check and the inserts run in ONE backend transaction.
        let sid = session_id.clone();
        shared
            .state
            .backend
            .transact(move |tx| {
                if !tx.runners.contains_key(&runner_id) {
                    return Err(ControlError::Forbidden(
                        "runner registration is no longer active".to_owned(),
                    ));
                }
                tx.session_keys
                    .insert(sid.clone(), SessionEncryption::generate());
                tx.broker_session_runners.insert(sid.clone(), runner_id);
                Ok(())
            })
            .await
            .map_err(|e| match e {
                ControlError::Forbidden(m) => ApiError::unauthorized(m),
                other => ApiError::from(other),
            })?;
    }
    shared
        .state
        .observability
        .metrics()
        .lifecycle
        .record_session_transition("create", "ok");
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

/// Record how long a claimed job sat in the ready queue. `enqueued_at` is
/// stamped when the job enters the queue and survives requeues, so a job
/// that bounced off a purged runner still measures total queue time. Jobs
/// restored from a snapshot without the field (`0`) are not recorded.
fn record_claim_queue_wait(shared: &Arc<SharedState>, claimed: &Option<QueuedJob>) {
    let Some(queued) = claimed else { return };
    if queued.enqueued_at_unix_nanos <= 0 {
        return;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    // Compute the elapsed difference first and only record a positive wait:
    // a backwards clock or a stamp raced by a requeue must not cast a
    // negative difference into a huge u64 duration.
    let elapsed_nanos = now.saturating_sub(queued.enqueued_at_unix_nanos);
    if elapsed_nanos <= 0 {
        return;
    }
    let elapsed = std::time::Duration::from_nanos(elapsed_nanos as u64);
    shared
        .state
        .observability
        .metrics()
        .lifecycle
        .record_queue_wait("claimed", elapsed);
}

pub async fn broker_delete_session_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None).await?;
    let header_session = headers
        .get("x-actions-session")
        .and_then(|value| value.to_str().ok());
    if let Some(session_id) = header_session.or_else(|| params.get("sessionId").map(String::as_str))
    {
        // `true` = a live session was removed; `false` = already gone. Only a
        // real delete counts as a transition — an idempotent 204 no-op does
        // not (the official control plane returns 204 either way).
        if remove_broker_session(&shared, session_id, runner_id).await? {
            shared
                .state
                .observability
                .metrics()
                .lifecycle
                .record_session_transition("delete", "ok");
        }
    }
    // No session id present: nothing was deleted, so no transition is
    // recorded — a 204 with no-op must not count as a successful delete.
    Ok(StatusCode::NO_CONTENT)
}

pub async fn broker_delete_session_by_path(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None).await?;
    if remove_broker_session(&shared, &session_id, runner_id).await? {
        shared
            .state
            .observability
            .metrics()
            .lifecycle
            .record_session_transition("delete", "ok");
    }
    Ok(StatusCode::NO_CONTENT)
}

pub async fn remove_broker_session(
    shared: &Arc<SharedState>,
    session_id: &str,
    runner_id: i64,
) -> Result<bool, ApiError> {
    let session_id = session_id.to_owned();
    shared
        .state
        .backend
        .transact(
            move |tx| match tx.broker_session_runners.get(&session_id).copied() {
                Some(owner) if owner == runner_id => {
                    tx.broker_session_runners.remove(&session_id);
                    tx.session_keys.remove(&session_id);
                    tx.session_active_requests.remove(&session_id);
                    Ok(true)
                }
                Some(_) => Err(ControlError::Forbidden(
                    "broker session belongs to another runner".to_owned(),
                )),
                // Unknown session: the official control plane answers 204 for
                // a session-less delete regardless — the runner cannot
                // distinguish "already gone" from "never existed", and
                // returning 404 breaks its delete-then-recreate cycle.
                None => Ok(false),
            },
        )
        .await
        .map_err(ApiError::from)
}
pub async fn authenticated_runner_id(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: Option<i64>,
) -> Result<i64, ApiError> {
    let bearer = crate::auth::bearer_from_headers(headers)
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    let runner_id = crate::auth::registered_runner_id(shared, bearer)
        .await
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    if expected_runner_id.is_some_and(|expected| expected != runner_id) {
        return Err(ApiError::forbidden(
            "runner token does not match broker path",
        ));
    }
    Ok(runner_id)
}

/// Authenticate a broker renew/complete call with the runtime token for the
/// exact agent job in the body. The runner listen token is deliberately
/// observed and rejected: acquirejob is its only broker lifecycle call.
pub async fn authenticated_runner_id_for_job(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: i64,
    job_id: uuid::Uuid,
    route: &'static str,
) -> Result<i64, ApiError> {
    let bearer = crate::auth::bearer_from_headers(headers)
        .ok_or_else(|| ApiError::unauthorized("runner or job runtime token required"))?;
    let auth =
        authenticated_runner_id_with_source(shared, headers, Some(expected_runner_id)).await?;
    if auth.auth_source == RunnerAuthSource::RunnerListenToken {
        record_listener_token_use(&shared.state, ListenerTokenProbe::Lifecycle, route);
        return Err(ApiError::forbidden(
            "job lifecycle requires the job runtime token",
        ));
    }
    if shared.state.job_uuid_from_token(bearer) != Some(job_id) {
        return Err(ApiError::forbidden(
            "job runtime token does not match broker job",
        ));
    }
    let run_id = shared
        .state
        .backend
        .run_for_attempt(job_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("broker job request not found"))?;
    let request =
        shared
            .state
            .backend
            .read_scoped(&crate::control::txstate::TxScope::run(run_id), move |tx| {
                if !tx.runners.contains_key(&expected_runner_id) {
                    return Err(ControlError::Forbidden(
                        "runner registration no longer exists".to_owned(),
                    ));
                }
                let request_id = tx.agent_job_requests.get(&job_id).copied().ok_or_else(|| {
                    ControlError::NotFound("broker job request not found".to_owned())
                })?;
                tx.job_requests.get(&request_id).cloned().ok_or_else(|| {
                    ControlError::NotFound("broker job request not found".to_owned())
                })
            })
            .await
            .map_err(|e| match e {
                ControlError::Forbidden(m) => ApiError::unauthorized(m),
                other => ApiError::from(other),
            })?;
    let exact_scope = format!("Actions.Results:{}:{}", request.plan_id, job_id);
    let exact_runtime_scope = shared
        .state
        .verify_local_jwt_claims(bearer)
        .and_then(|claims| {
            claims
                .get("scp")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .is_some_and(|scope| scope == exact_scope);
    if request.owner_runner_id != Some(expected_runner_id) || !exact_runtime_scope {
        return Err(ApiError::forbidden(
            "job runtime token does not own broker request",
        ));
    }
    Ok(expected_runner_id)
}

pub fn ensure_broker_request_owner(
    tx: &crate::control::txstate::TxState,
    request_id: i64,
    runner_id: i64,
) -> Result<(), ControlError> {
    // Prefer the immutable owner recorded when the request was claimed. This
    // survives session teardown/rebind and keeps late broker retries bound to
    // the runner that actually received the job.
    if let Some(owner) = tx
        .job_requests
        .get(&request_id)
        .and_then(|request| request.owner_runner_id)
    {
        return if owner == runner_id {
            Ok(())
        } else {
            Err(ControlError::Forbidden(
                "broker request belongs to another runner".to_owned(),
            ))
        };
    }
    let session_id =
        tx.session_active_requests
            .iter()
            .find_map(|(session_id, active_request_id)| {
                (*active_request_id == request_id).then_some(session_id.clone())
            });
    let has_session = session_id.is_some();
    let owner = session_id.and_then(|sid| {
        tx.broker_session_runners
            .get(&sid)
            .copied()
            .or_else(|| tx.sessions.get(&sid).map(|s| s.runner_id))
    });
    match owner {
        Some(owner) if owner == runner_id => Ok(()),
        Some(_) => Err(ControlError::Forbidden(
            "broker request belongs to another runner".to_owned(),
        )),
        // If the request is assigned to a session but the session is not in
        // broker_session_runners or sessions (e.g. conformance replay with
        // golden session IDs), accept it as long as the token's runner_id
        // matches the path. This preserves backward compat for test/replay
        // flows where session creation and broker paths use different IDs.
        None if has_session => Ok(()),
        None => Err(ControlError::NotFound(
            "broker request is not assigned to a session".to_owned(),
        )),
    }
}

pub async fn next_message_broker_ref_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None).await?;
    if let Some(response) = runner_version_deprecated_response(&shared, &params) {
        return Ok(response);
    }
    let session_id = params
        .get("sessionId")
        .cloned()
        .ok_or_else(|| ApiError::bad_request("broker sessionId is required"))?;
    // Heartbeat, then ownership + capabilities — two single statements, no
    // working-set load and no writer lock on the per-poll path.
    shared
        .state
        .backend
        .touch_session(&session_id)
        .await
        .map_err(ApiError::from)?;
    let runner = match shared
        .state
        .backend
        .session_owner(&session_id)
        .await
        .map_err(ApiError::from)?
    {
        Some((owner, capabilities)) if owner == runner_id => capabilities,
        _ => {
            return Err(ApiError::forbidden(
                "broker session belongs to another runner",
            ))
        }
    };

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
        let outcome = shared
            .state
            .backend
            .poll_session(crate::control::backend::PollRequest {
                session_id: session_id.clone(),
                verified_runner_id: Some(runner_id),
                runner: runner.clone(),
                busy: runner_busy,
                wait_ms: 0,
            })
            .await
            .map_err(ApiError::from)?;

        let maybe = match outcome {
            // Cancellation or redelivered inflight message — serialize the
            // TaskAgentMessage (messageId/messageType/body) directly.
            crate::control::types::PollOutcome::Cancel(message)
            | crate::control::types::PollOutcome::Inflight(message) => {
                Some(serde_json::to_value(&message).unwrap_or(serde_json::Value::Null))
            }
            // Active request still running — long-poll for cancel rather than
            // redelivering the same RunnerJobRequest (runner dedups it).
            crate::control::types::PollOutcome::ActiveRequest { .. } => None,
            crate::control::types::PollOutcome::Claimed(claimed) => {
                let crate::control::types::ClaimedJob {
                    queued,
                    request,
                    runner_id,
                    queue_depth,
                    next_runs_on,
                } = *claimed;
                shared
                    .state
                    .queue_depth
                    .store(queue_depth, std::sync::atomic::Ordering::Release);
                *shared.state.next_job_runs_on.write().unwrap() = next_runs_on;
                record_claim_queue_wait(&shared, &Some(queued));
                Some(broker_job_ref_root(&request, runner_id))
            }
            crate::control::types::PollOutcome::Empty => None,
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

pub async fn broker_acknowledge_root(
    State(_shared): State<Arc<SharedState>>,
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    // Acknowledge receipt of the message. Do NOT clear session_active_requests
    // here — the runner is still working on the job. The session's active
    // request is cleared when completejob sets the result and the next poll
    // sees result.is_some() at line 2190.
    StatusCode::OK
}

pub async fn broker_acquire_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerAcquireJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let auth = authenticated_runner_id_with_source(&shared, &headers, Some(runner_id)).await?;
    if auth.auth_source == RunnerAuthSource::RuntimeJwt {
        return Err(ApiError::forbidden(
            "acquirejob requires the runner listen token",
        ));
    }
    if auth.auth_source == RunnerAuthSource::RunnerListenToken {
        record_listener_token_use(
            &shared.state,
            ListenerTokenProbe::Acquire,
            "broker.acquirejob",
        );
    }
    let job_message_id = request.job_message_id;
    // One indexed join resolves the attempt plus its message, mint request,
    // grant and owner — the whole acquire read, no working set.
    let Some((request_id, run_id)) = shared
        .state
        .backend
        .find_request_by_agent_job_id(job_message_id)
        .await
        .map_err(ApiError::from)?
    else {
        return Err(ApiError::not_found("broker job message not found"));
    };
    let ctx = shared
        .state
        .backend
        .acquire_for_runner(request_id, runner_id)
        .await
        .map_err(ApiError::from)?;
    let mut message = ctx.message;
    let github_token_request = ctx.token_request;
    let id_token_granted = ctx.id_token_granted;
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
            tracing::info!(
                token_len = minted.token.len(),
                "minted dispatch GitHub token at claim"
            );
            apply_minted_token_to_message(&mut message, &minted, false);
        }
        // The token request stays registered for the job's lifetime so a
        // re-claim re-mints under the build-time conditions (permission set
        // and fallback restrictions). `fail_unclaimable_request`,
        // `complete_job_inner` (claimed-job completion paths funnel there)
        // and `retire_node_requests` remove it once the job is terminal;
        // see the `distributed_task.rs` completion path for the known gap.
        shared
            .state
            .backend
            .store_request_message(run_id, request_id, Some(&message), None)
            .await
            .map_err(ApiError::from)?;
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
            // The submission stores the tier as a plain kebab-case string
            // (e.g. "untrusted-fork-pull-request"), not JSON. `from_str`
            // expects JSON and would reject the bare string, yielding `None`
            // — which `job_authorization` treats as trusted, silently
            // un-restricting a fork job's token. Parse via a JSON string
            // value so the kebab-case variant decodes.
            let tier = ctx.trust_tier.as_deref().and_then(|tier| {
                serde_json::from_value(serde_json::Value::String(tier.to_owned())).ok()
            });
            // The job's resolved permission set lives in the persisted
            // message's `system.github.token.permissions` variable
            // (PascalCase wire spelling) — the same variable the build path
            // wrote from `JobPlan` permissions. The event payload's
            // `workflow_job` key is absent for push/PR/dispatch events, so
            // reading it there would fall back to the broad default and
            // grant scopes the workflow withheld. Recover from the message
            // instead, converting the wire spelling back to kebab-case for
            // the token request.
            let wire_permissions = message
                .variables
                .get("system.github.token.permissions")
                .and_then(|variable| variable.value.as_deref())
                .and_then(|json| serde_json::from_str::<BTreeMap<String, String>>(json).ok());
            // The wire variable spells scopes PascalCase ("PullRequests");
            // the installation-token request and `job_authorization` expect
            // the workflow's kebab-case identities ("pull-requests").
            // Minting with PascalCase keys fails (or falls back to the broad
            // PAT), so convert every key before building the request.
            let wire_permissions = wire_permissions.map(|permissions| {
                permissions
                    .into_iter()
                    .map(|(scope, level)| (wire_scope_to_kebab(&scope), level))
                    .collect::<BTreeMap<_, _>>()
            });
            // The wire variable carries repository-token scopes only; the
            // OIDC grant is persisted per job. Fall back to the old wire
            // marker so jobs queued before this renderer change can still
            // recover.
            let id_token_granted = ctx.id_token_granted.unwrap_or_else(|| {
                wire_permissions
                    .as_ref()
                    .and_then(|permissions| permissions.get("id-token"))
                    .is_some_and(|level| level == "write")
                    || message.resources.endpoints.iter().any(|endpoint| {
                        endpoint
                            .data
                            .get("GenerateIdTokenUrl")
                            .is_some_and(|url| !url.is_empty())
                    })
            });
            let declared = wire_permissions.clone();
            let policy = crate::events::trust_tier::job_authorization(
                tier,
                declared.as_ref(),
                id_token_granted,
            );
            Some(crate::models::GitHubTokenRequest {
                repository: ctx.repository.clone(),
                permissions: policy.app_permissions,
                declared: declared.is_some(),
                untrusted: policy.fork_restricted,
            })
        };
        if let Some(token_request) = derived {
            // Register the derived request so a re-claim after a disconnect
            // re-mints under the same derived policy, then mint.
            shared
                .state
                .backend
                .store_request_message(run_id, request_id, None, Some(&token_request))
                .await
                .map_err(ApiError::from)?;
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
                tracing::info!(
                    token_len = minted.token.len(),
                    "minted re-derived dispatch GitHub token at claim"
                );
                apply_minted_token_to_message(&mut message, &minted, true);
                shared
                    .state
                    .backend
                    .store_request_message(run_id, request_id, Some(&message), None)
                    .await
                    .map_err(ApiError::from)?;
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
            if id_token_granted.unwrap_or(false) {
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
    // Broker poll outcome — bounded, exactly one per successful claim. Queue
    // wait is recorded at the claim sites in `next_message_broker_ref` /
    // `next_message_disttask`, where the enqueue timestamp is still on the
    // job; by the time the acquire payload is built the queue position has
    // been lost.
    shared
        .state
        .observability
        .metrics()
        .lifecycle
        .record_broker_poll("job");
    Ok(Json(payload))
}

/// Replace every pinned checkout credential — snapshot-served or rerouted
/// onto the forge relay — with a freshly minted runtime token.
///
/// Returns the number of steps refreshed. The pinned ids travel on the
/// message ([`azdo::AgentJobRequestMessage::preloop_snapshot_token_steps`]),
/// so this deliberately matches by step id rather than by token shape.
pub fn re_mint_snapshot_tokens(
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
pub struct MintedGitHubToken {
    pub token: String,
    pub effective_permissions: Option<BTreeMap<String, String>>,
}

/// Apply a freshly minted dispatch token to the job message: inject the two
/// secret variables (`system.github.token`, `github_token`), restate the
/// narrowed permission set, and patch the minted token into the `github`
/// context so `${{ github.token }}` inputs (checkout's token,
/// persist-credentials config) authenticate. Shared by the normal mint path
/// and the re-derived-request fallback, which had already diverged (the
/// fallback lost the success log). `re_derived` only tailors the log wording:
/// the derived path historically logged no success line.
fn apply_minted_token_to_message(
    message: &mut azdo::AgentJobRequestMessage,
    minted: &MintedGitHubToken,
    re_derived: bool,
) {
    let token = &minted.token;
    message.variables.insert(
        "system.github.token".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
    );
    message.variables.insert(
        "github_token".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::secret(token.clone()),
    );
    // Restate only repository-token scopes when an installation narrowed the
    // mint. OIDC and other Actions-only capabilities have dedicated protocol
    // fields and never belong in `GITHUB_TOKEN Permissions`.
    if let Some(effective) = &minted.effective_permissions {
        let perms_json = preloop_gha_parser::job_builder::token_permissions_wire_json(effective);
        message.variables.insert(
            "system.github.token.permissions".to_owned(),
            preloop_gha_protocol::azdo::VariableValue::new(perms_json),
        );
    }
    // The workflow's `github` context is built at submission time, before
    // the App token can exist, so `${{ github.token }}` inputs resolve
    // empty and every git fetch prompts for a username. Patch the minted
    // token into the context at claim so checkout authenticates exactly
    // like it does on GitHub-hosted runners — no runner-side env header
    // needed (an env `extraheader` would duplicate the one checkout
    // persists itself: "Duplicate header: Authorization", HTTP 400).
    match message.context_data.get_mut("github") {
        Some(preloop_gha_protocol::azdo::PipelineContextData::Dict(github)) => {
            github.insert(
                "token".to_owned(),
                preloop_gha_protocol::azdo::PipelineContextData::String(token.clone()),
            );
            if !re_derived {
                tracing::info!("patched minted token into github context");
            }
        }
        other => tracing::warn!(
            github_context = %match other { Some(_) => "non-dict", None => "missing" },
            "could not patch github context token{}",
            if re_derived { " for re-derived request" } else { "" }
        ),
    }
}

/// Release a claimed request that can never be dispatched, using the same
/// bookkeeping `broker_complete_job` performs so the run summary, the session
/// slot and the concurrency release all behave as they do for a
/// runner-reported failure.
async fn fail_unclaimable_request(shared: &Arc<SharedState>, request_id: i64) {
    let run_job = shared
        .state
        .backend
        .transact(move |tx| {
            // Nothing will consume the deferred token request now, and
            // leaving it behind keeps the job's requested permissions
            // alive for a request that is already terminal.
            tx.github_token_requests.remove(&request_id);
            if let Some(record) = tx.job_requests.get_mut(&request_id) {
                record.result = Some(ExecutionStatus::Failure);
                record.locked_until = agent_request_locked_until();
            }
            tx.session_active_requests
                .retain(|_, &mut rid| rid != request_id);
            let run_job = tx.inflight_requests.remove(&request_id).or_else(|| {
                tx.job_requests
                    .get(&request_id)
                    .map(|record| (record.run_id, record.job_id.clone()))
            });
            let agent_job_id = tx
                .job_requests
                .get(&request_id)
                .map(|record| record.agent_job_id);
            Ok(run_job.map(|(run_id, job_id)| (run_id, job_id, agent_job_id)))
        })
        .await
        .unwrap_or_default();
    if let Some((run_id, job_id, agent_job_id)) = run_job {
        let completion = JobCompletion {
            run_id,
            job_id,
            agent_job_id,
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

/// Reconcile job claims whose runner session did not survive a restart.
///
/// A claim still absent from the ready queue is irrecoverable and fails as
/// before. A request whose logical job was already requeued by runner purge is
/// released for redelivery without concluding the retry. The latter also
/// migrates snapshots written by versions that requeued the job but left its
/// old runner ownership live.
pub async fn reconcile_orphaned_claims(shared: &Arc<SharedState>) -> usize {
    let (recovered, unclaimable) = match shared
        .state
        .backend
        .transact(|tx| {
            let live_requests: std::collections::BTreeSet<i64> = tx
                .session_active_requests
                .iter()
                .filter(|(session_id, _)| tx.sessions.contains_key(*session_id))
                .map(|(_, request_id)| *request_id)
                .collect();
            let claimed_requests: Vec<(i64, RunId, JobId)> = tx
                .job_requests
                .iter()
                .filter(|(request_id, record)| {
                    record.result.is_none()
                        && !live_requests.contains(request_id)
                        && (record.owner_runner_id.is_some()
                            || tx
                                .session_active_requests
                                .values()
                                .any(|active| active == *request_id))
                })
                .map(|(request_id, record)| (*request_id, record.run_id, record.job_id.clone()))
                .collect();

            // The global ready queue is `ready_index` (persisted ready jobs),
            // NOT `queue` (only newly enqueued this tx) — a claimed job that
            // was requeued by runner purge lives in `ready_index`.
            let queued_jobs: std::collections::BTreeSet<(RunId, JobId)> = tx
                .ready_index
                .iter()
                .map(|job| (job.run_id, job.job_id.clone()))
                .collect();
            let mut recovered = 0usize;
            let mut unclaimable = Vec::new();
            for (request_id, run_id, job_id) in claimed_requests {
                let queued = queued_jobs.contains(&(run_id, job_id.clone()));
                let terminal_status = tx
                    .runs
                    .get(&run_id)
                    .and_then(|run| run.jobs.get(&job_id).copied())
                    .filter(|status| {
                        matches!(
                            status,
                            ExecutionStatus::Success
                                | ExecutionStatus::Failure
                                | ExecutionStatus::Cancelled
                                | ExecutionStatus::Skipped
                        )
                    });
                if queued {
                    crate::control::sched::release_request_for_retry(tx, request_id);
                    recovered += 1;
                } else if let Some(status) = terminal_status {
                    crate::control::sched::settle_request(tx, request_id, status);
                    recovered += 1;
                } else {
                    unclaimable.push(request_id);
                }
            }
            Ok((recovered, unclaimable))
        })
        .await
    {
        Ok(v) => v,
        Err(error) => {
            warn!(?error, "reconcile_orphaned_claims transaction failed");
            return 0;
        }
    };

    if recovered > 0 {
        warn!(
            recovered,
            "released orphaned retry attempts from dead runner ownership"
        );
    }

    for request_id in &unclaimable {
        fail_unclaimable_request(shared, *request_id).await;
    }
    if !unclaimable.is_empty() {
        warn!(
            count = unclaimable.len(),
            "failed job claims orphaned by a control-plane restart"
        );
    }
    recovered + unclaimable.len()
}

pub async fn mint_dispatch_github_token(
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
    // Operator ceiling on GITHUB_TOKEN permissions: the permission map
    // requested from GitHub is the minimum of the workflow-declared set
    // (already downgraded to the fork-restricted profile for untrusted jobs)
    // and the ceiling. The App installation's grants clamp it further at
    // mint time. The PAT fallback below is the operator's own credential
    // and ignores `permissions:` by design, so the ceiling does not apply
    // to it.
    let ceiling = crate::token_ceiling::apply_ceiling(
        shared.state.token_permissions_ceiling.as_ref(),
        &request.permissions,
    );
    for detail in &ceiling.clamped {
        warn!(
            repository = %request.repository,
            scope = %detail.scope,
            requested = %detail.requested,
            granted = %detail.granted,
            "token permissions ceiling clamped GITHUB_TOKEN scope"
        );
    }
    let ceiling_clamped = !ceiling.clamped.is_empty();
    let minted = match crate::github_app::get_or_mint_token_declared(
        &app,
        &request.repository,
        &ceiling.permissions,
        request.declared,
    )
    .await
    {
        Ok((token, narrowed)) => {
            // The token carries the clamped set when the ceiling reduced the
            // request (or when the installation narrowed it): restate it so
            // the wire `system.github.token.permissions` variable reports
            // what the token actually carries.
            let effective_permissions =
                narrowed.or_else(|| ceiling_clamped.then(|| ceiling.permissions.clone()));
            Some(MintedGitHubToken {
                token,
                effective_permissions,
            })
        }
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

pub async fn broker_renew_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticated_runner_id_for_job(
        &shared,
        &headers,
        runner_id,
        request.job_id,
        "broker.renewjob",
    )
    .await?;

    let job_id = request.job_id;
    // Fast path: one conditional UPDATE, no working-set load, no writer
    // lock. Falls back to the transactional path only for attempts without
    // a recorded owner.
    let locked_until = agent_request_locked_until();
    if shared
        .state
        .backend
        .renew_lease(job_id, runner_id, &locked_until)
        .await
        .map_err(ApiError::from)?
    {
        return Ok(Json(json!({"lockedUntil": locked_until})));
    }
    let record = shared
        .state
        .backend
        .transact(move |tx| {
            let request_id = tx.agent_job_requests.get(&job_id).copied().ok_or_else(|| {
                ControlError::NotFound("broker renew request not found".to_owned())
            })?;
            ensure_broker_request_owner(tx, request_id, runner_id)?;
            if tx
                .job_requests
                .get(&request_id)
                .is_some_and(|record| record.result.is_some())
            {
                return Err(ControlError::Conflict(
                    "broker request already completed".to_owned(),
                ));
            }
            let record = tx
                .job_requests
                .get_mut(&request_id)
                .ok_or_else(|| ControlError::NotFound("agent request not found".to_owned()))?;
            record.locked_until = agent_request_locked_until();
            record.last_renewed_at = Some(std::time::SystemTime::now());
            Ok(record.clone())
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({"lockedUntil": record.locked_until})))
}

pub async fn broker_complete_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<StatusCode, ApiError> {
    authenticated_runner_id_for_job(
        &shared,
        &headers,
        runner_id,
        request.job_id,
        "broker.completejob",
    )
    .await?;

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

    // One transaction: settle the attempt this runner owns and complete its
    // job (see `complete_job_settling`).
    let (run_id, job_id) = shared
        .state
        .backend
        .attempt_job(request.job_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("broker complete request not found"))?;
    let _ = crate::distributed_task::complete_job_settling(
        shared.clone(),
        JobCompletion {
            run_id,
            job_id,
            agent_job_id: Some(request.job_id),
            status,
            outputs,
            annotations: request.annotations.clone(),
            step_results: request.step_results.clone(),
        },
        Some(crate::distributed_task::AttemptSettle {
            agent_job_id: request.job_id,
            runner_id,
        }),
    )
    .await?;
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
        use axum::body::{Body, to_bytes};
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
    fn wire_scope_to_kebab_round_trips_pascal_scopes() {
        assert_eq!(wire_scope_to_kebab("Checks"), "checks");
        assert_eq!(wire_scope_to_kebab("PullRequests"), "pull-requests");
        assert_eq!(wire_scope_to_kebab("IdToken"), "id-token");
        assert_eq!(wire_scope_to_kebab("Contents"), "contents");
    }
}
