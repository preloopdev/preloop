use super::*;

pub async fn next_message(
    State(shared): State<Arc<SharedState>>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Option<azdo::TaskAgentMessage>>) {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());
    let verified = identity
        .as_ref()
        .and_then(|axum::Extension(id)| id.runner_id);

    let wait_seconds = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);

    enum NextPoll {
        Forbidden,
        Deliver(azdo::TaskAgentMessage, StatusCode),
        /// A terminal empty response (e.g. message build failed).
        Empty(StatusCode),
        Wait,
        /// Claimed a job: message built, run marked InProgress, emit events.
        Claimed {
            message: azdo::TaskAgentMessage,
            run_id: RunId,
            job_id: JobId,
        },
    }

    loop {
        let sid = session_id.clone();
        let ident = identity.clone();
        let shared_for_mint = shared.clone();
        let step = shared
            .state
            .backend
            .transact(move |tx| {
                if let Some(runner_id) = verified {
                    if tx.runner_id_for_session(&sid) != Some(runner_id) {
                        return Ok(NextPoll::Forbidden);
                    }
                }
                tx.mark_session_seen(&sid);
                if let Some(message) = tx
                    .inflight_messages
                    .get(&sid)
                    .and_then(|messages| messages.values().next().cloned())
                {
                    return Ok(NextPoll::Deliver(message, StatusCode::ACCEPTED));
                }

                if let Some(request_id) = tx.session_active_requests.get(&sid).copied() {
                    let request_finished = tx
                        .job_requests
                        .get(&request_id)
                        .is_none_or(|request| request.result.is_some());
                    if request_finished {
                        tx.session_active_requests.remove(&sid);
                    } else {
                        let cancellation_pos =
                            tx.job_requests.get(&request_id).and_then(|request| {
                                tx.cancellation_queue.iter().position(|cancellation| {
                                    cancellation.run_id == request.run_id
                                        && cancellation.job_id == request.job_id
                                })
                            });
                        if let Some(pos) = cancellation_pos {
                            let cancellation = tx
                                .cancellation_queue
                                .remove(pos)
                                .expect("cancellation position was found in the queue");
                            let body_json = concurrency::job_cancel_body(cancellation.agent_job_id);
                            return Ok(
                                match build_task_agent_message(
                                    tx,
                                    &sid,
                                    azdo::message_type::JOB_CANCELLED,
                                    body_json,
                                ) {
                                    Ok(message) => NextPoll::Deliver(message, StatusCode::OK),
                                    Err(_) => NextPoll::Empty(StatusCode::ACCEPTED),
                                },
                            );
                        }
                        return Ok(NextPoll::Wait);
                    }
                }

                let runner = tx.runner_capabilities_for_session(&sid);
                let verified_claim = effective_claim_runner(
                    ident.as_ref().map(|axum::Extension(id)| id),
                    tx.runner_id_for_session(&sid),
                );
                let claimed =
                    crate::control::sched::choose_claim_position(tx, &runner, verified_claim)
                        .and_then(|pos| crate::control::sched::apply_claim(tx, pos));
                let Some(queued) = claimed else {
                    return Ok(NextPoll::Wait);
                };

                let claimed_at = std::time::SystemTime::now();
                if let Some(run) = tx.runs.get_mut(&queued.run_id) {
                    run.status = ExecutionStatus::InProgress;
                    run.jobs
                        .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
                }

                // F030: inject SystemVssConnection so the worker's AzDO
                // reporting context has a server URL, access token, and
                // ResultsServiceUrl — same as broker_acquire_job.
                let mut msg = queued.message.clone();
                for endpoint in &mut msg.resources.endpoints {
                    if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                        endpoint.url = Some(runner_server_url());
                        endpoint.authorization.parameters.insert(
                            "AccessToken".to_owned(),
                            // node-local crypto on SharedState — safe inside
                            // the tx (does not touch `inner`).
                            shared_for_mint
                                .state
                                .mint_runtime_token(&msg.plan.plan_id, &msg.job_id),
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
                    }
                }
                let body_json = serde_json::to_string(&msg).map_err(|e| {
                    crate::control::ControlError::Backend(anyhow::anyhow!(
                        "serialize job message: {e}"
                    ))
                })?;
                let request_id = queued.message.request_id;
                let owner_runner_id = verified_claim.or_else(|| tx.runner_id_for_session(&sid));
                tx.session_active_requests.insert(sid.clone(), request_id);
                if let Some(request) = tx.job_requests.get_mut(&request_id) {
                    request.owner_runner_id = owner_runner_id;
                    request.claimed_at = Some(claimed_at);
                    request.started_at = Some(claimed_at);
                    request.last_renewed_at = Some(claimed_at);
                }
                let message = build_task_agent_message(
                    tx,
                    &sid,
                    azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
                    body_json,
                )
                .map_err(|_| {
                    crate::control::ControlError::Backend(anyhow::anyhow!("build message failed"))
                })?;
                Ok(NextPoll::Claimed {
                    message,
                    run_id: queued.run_id,
                    job_id: queued.job_id.clone(),
                })
            })
            .await;

        let step = match step {
            Ok(s) => s,
            Err(_) => return (StatusCode::ACCEPTED, Json(None)),
        };
        match step {
            NextPoll::Forbidden => return (StatusCode::FORBIDDEN, Json(None)),
            NextPoll::Deliver(message, status) => return (status, Json(Some(message))),
            NextPoll::Empty(status) => return (status, Json(None)),
            NextPoll::Claimed {
                message,
                run_id,
                job_id,
            } => {
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
                return (StatusCode::ACCEPTED, Json(Some(message)));
            }
            NextPoll::Wait => {
                if wait_seconds == 0 {
                    return (StatusCode::OK, Json(None));
                }
                if tokio::time::timeout(
                    Duration::from_secs(wait_seconds),
                    shared.state.message_notify.notified(),
                )
                .await
                .is_err()
                {
                    return (StatusCode::OK, Json(None));
                }
                continue;
            }
        }
    }
}

pub async fn delete_session_message(
    State(shared): State<Arc<SharedState>>,
    Path((session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, &session_id, message_id).await
}

pub fn build_task_agent_message(
    tx: &mut crate::control::txstate::TxState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> Result<azdo::TaskAgentMessage, ApiError> {
    let session_key = tx
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

    tx.next_message_id += 1;
    let message_id = tx.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: BASE64_STANDARD.encode(&encrypted_body),
        iv: Some(BASE64_STANDARD.encode(&iv)),
    };
    tx.inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    Ok(message)
}

pub fn build_broker_plaintext_message(
    tx: &mut crate::control::txstate::TxState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> azdo::TaskAgentMessage {
    tx.next_message_id += 1;
    let message_id = tx.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: body_json,
        iv: None,
    };
    tx.inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    message
}

pub async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    if let Some(runner_id) = identity.and_then(|axum::Extension(id)| id.runner_id) {
        let sid = session_id.to_owned();
        let owns = shared
            .state
            .backend
            .read(move |tx| Ok(tx.runner_id_for_session(&sid) == Some(runner_id)))
            .await
            .unwrap_or(false);
        if !owns {
            return StatusCode::FORBIDDEN;
        }
    }
    ack_message(shared, session_id, message_id).await
}
pub async fn ack_message(
    shared: Arc<SharedState>,
    session_id: &str,
    message_id: i64,
) -> StatusCode {
    let sid = session_id.to_owned();
    let _ = shared
        .state
        .backend
        .transact(move |tx| {
            if let Some(messages) = tx.inflight_messages.get_mut(&sid) {
                messages.remove(&message_id);
                if messages.is_empty() {
                    tx.inflight_messages.remove(&sid);
                }
            }
            Ok(())
        })
        .await;
    StatusCode::NO_CONTENT
}

pub async fn complete_job(
    State(shared): State<Arc<SharedState>>,
    Json(completion): Json<JobCompletion>,
) -> Result<Json<RunRecord>, ApiError> {
    complete_job_inner(shared, completion).await
}

pub async fn complete_job_compat(
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
            // The compat route is addressed by logical job only, so the
            // server resolves the newest attempt itself.
            agent_job_id: None,
            status,
            outputs: Default::default(),
            annotations: Vec::new(),
            step_results: Vec::new(),
        },
    )
    .await
}
pub async fn complete_job_compat_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(RunId, String)>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<RunRecord>, ApiError> {
    let target = {
        let run_id = path.0;
        let job_key = path.1.clone();
        shared
            .state
            .backend
            .read(move |tx| {
                Ok(tx
                    .job_requests
                    .values()
                    .filter(|request| request.run_id == run_id && request.job_id.0 == job_key)
                    .max_by_key(|request| request.request_id)
                    .cloned())
            })
            .await
            .map_err(ApiError::from)?
    };
    crate::auth::authorize_reporting_request(&shared.state, &headers, target.as_ref())?;
    complete_job_compat(State(shared), Path(path), Json(body)).await
}

fn runner_owns_agent_request(
    tx: &crate::control::txstate::TxState,
    request_id: i64,
    runner_id: i64,
) -> bool {
    if let Some(owner) = tx
        .job_requests
        .get(&request_id)
        .and_then(|request| request.owner_runner_id)
    {
        return owner == runner_id;
    }
    tx.session_active_requests
        .iter()
        .any(|(session_id, active_id)| {
            *active_id == request_id && tx.runner_id_for_session(session_id) == Some(runner_id)
        })
}

/// GET /_apis/v1/AgentRequest/:pool_id/:request_id to query a job request lease/result.
///
/// The official listener calls this when another job arrives while the previous
/// worker process may still be unwinding. Returning a completed `result` lets it
/// safely move on; the request's retained owner keeps this post-completion read
/// bound to the runner that handled the attempt. 404/405 makes it cancel the
/// worker and can poison matrix runs.
pub async fn agent_request_get(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = identity.and_then(|axum::Extension(id)| id.runner_id);
    let request = shared
        .state
        .backend
        .read(move |tx| {
            let request = tx.job_requests.get(&request_id).ok_or_else(|| {
                crate::control::ControlError::NotFound("agent request not found".to_owned())
            })?;
            if let Some(runner_id) = runner_id {
                if !runner_owns_agent_request(tx, request_id, runner_id) {
                    return Err(crate::control::ControlError::Forbidden(
                        "agent request belongs to another runner".to_owned(),
                    ));
                }
            }
            Ok(request.clone())
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(agent_request_json(pool_id, &request)))
}

/// POST /_apis/v1/AgentRequest/:pool_id/:request_id — best-effort request ack.
pub async fn agent_request_ack(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, request_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
) -> Result<StatusCode, ApiError> {
    if let Some(runner_id) = identity.and_then(|axum::Extension(id)| id.runner_id) {
        shared
            .state
            .backend
            .read(move |tx| {
                if tx.job_requests.contains_key(&request_id)
                    && !runner_owns_agent_request(tx, request_id, runner_id)
                {
                    return Err(crate::control::ControlError::Forbidden(
                        "agent request belongs to another runner".to_owned(),
                    ));
                }
                Ok(())
            })
            .await
            .map_err(ApiError::from)?;
    }
    Ok(StatusCode::OK)
}

/// PATCH /_apis/v1/AgentRequest/:pool_id/:request_id — renew or complete job request.
/// The runner sends this to renew the job lock or report completion.
pub async fn agent_request_patch(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // `result` is untyped request content; never log it raw. Derive a fixed
    // label from the status mapping instead ("success"/"failure"/…, or
    // "unknown"/"renew").
    let has_result = body.get("result").is_some();
    let result_hint = body
        .get("result")
        .and_then(|v| v.as_str())
        .and_then(execution_status_from_runner_result)
        .map(|status| format!("{status:?}").to_ascii_lowercase())
        .unwrap_or_else(|| {
            if has_result {
                "unknown".to_owned()
            } else {
                "renew".to_owned()
            }
        });
    info!(
        pool_id,
        request_id,
        result = %result_hint,
        has_result,
        "agent_request_patch received"
    );
    // If this is a completion (has result), delegate to complete_job_inner
    // so summarize_run, promote_ready_jobs, and notify_waiters all fire.
    // The result field is only present on the final PATCH; renewals have no result.
    let verified_runner_id = identity.and_then(|axum::Extension(id)| id.runner_id);
    if let Some(result) = body.get("result").and_then(|v| v.as_str()) {
        let new_status = match execution_status_from_runner_result(result) {
            Some(status) => status,
            None => {
                info!(
                        request_id,
                        result = %result_hint,
                        "unknown agent_request_patch result; skipping completion"
                );
                return Ok(Json(
                    json!({ "requestId": request_id, "lockedUntil": agent_request_locked_until() }),
                ));
            }
        };
        // Look up (run_id, job_id) under the inner lock, then drop it before calling
        // complete_job_inner which acquires the lock itself.
        let completion = shared
            .state
            .backend
            .transact(move |tx| {
                if let Some(runner_id) = verified_runner_id {
                    if tx.job_requests.contains_key(&request_id)
                        && !runner_owns_agent_request(tx, request_id, runner_id)
                    {
                        return Err(crate::control::ControlError::Forbidden(
                            "agent request belongs to another runner".to_owned(),
                        ));
                    }
                }
                let already_completed = tx
                    .job_requests
                    .get(&request_id)
                    .is_some_and(|request| request.result.is_some());
                if already_completed {
                    info!(
                        request_id,
                        result = %result_hint,
                        "agent request already completed; ignoring duplicate completion"
                    );
                    return Ok(None);
                }
                if let Some(request) = tx.job_requests.get_mut(&request_id) {
                    request.result = Some(new_status);
                    request.locked_until = agent_request_locked_until();
                }
                if let Some((run_id, job_id)) = tx.inflight_requests.remove(&request_id) {
                    info!(
                        %run_id,
                        %job_id,
                        result = %result_hint,
                        "job completed via agent_request_patch"
                    );
                    Ok(Some(JobCompletion {
                        run_id,
                        job_id,
                        // The patched request is the attempt that reported.
                        agent_job_id: tx
                            .job_requests
                            .get(&request_id)
                            .map(|record| record.agent_job_id),
                        status: new_status,
                        outputs: Default::default(),
                        annotations: Vec::new(),
                        step_results: Vec::new(),
                    }))
                } else {
                    info!(
                        request_id,
                        "no inflight job for request_id; ignoring result"
                    );
                    Ok(None)
                }
            })
            .await
            .map_err(ApiError::from)?;
        if let Some(c) = completion {
            let _ = complete_job_inner(shared.clone(), c).await;
        }
        return Ok(Json(
            agent_request_response(&shared, pool_id, request_id).await,
        ));
    }
    // Renewal — runner is still working; just extend the lock. Completed
    // requests are immutable, so a late duplicate cannot rewrite their timing.
    shared
        .state
        .backend
        .transact(move |tx| {
            if let Some(runner_id) = verified_runner_id {
                if tx.job_requests.contains_key(&request_id)
                    && !runner_owns_agent_request(tx, request_id, runner_id)
                {
                    return Err(crate::control::ControlError::Forbidden(
                        "agent request belongs to another runner".to_owned(),
                    ));
                }
            }
            let request_active = tx
                .job_requests
                .get(&request_id)
                .is_some_and(|request| request.result.is_none());
            if request_active {
                if let Some(request) = tx.job_requests.get_mut(&request_id) {
                    request.locked_until = agent_request_locked_until();
                    request.last_renewed_at = Some(std::time::SystemTime::now());
                }
            }
            Ok(())
        })
        .await
        .map_err(ApiError::from)?;
    Ok(Json(
        agent_request_response(&shared, pool_id, request_id).await,
    ))
}

pub async fn agent_request_response(
    shared: &Arc<SharedState>,
    pool_id: i64,
    request_id: i64,
) -> serde_json::Value {
    shared
        .state
        .backend
        .read(move |tx| {
            Ok(tx
                .job_requests
                .get(&request_id)
                .map(|request| agent_request_json(pool_id, request))
                .unwrap_or_else(|| {
                    json!({
                        "requestId": request_id,
                        "poolId": pool_id,
                        "lockedUntil": agent_request_locked_until(),
                    })
                }))
        })
        .await
        .unwrap_or_else(|_| {
            json!({
                "requestId": request_id,
                "poolId": pool_id,
                "lockedUntil": agent_request_locked_until(),
            })
        })
}

pub fn agent_request_json(pool_id: i64, request: &TaskAgentJobRequestRecord) -> serde_json::Value {
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

pub fn agent_request_result(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Success => "succeeded",
        ExecutionStatus::Failure => "failed",
        ExecutionStatus::Cancelled => "canceled",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress => {
            "pending"
        }
    }
}

/// Job lock duration (and silent-runner-death reaper window).
///
/// Measured on GitHub-hosted runners: a job whose runner dies without
/// reporting (killed `Runner.Listener` mid-job) concluded as `failure`
/// exactly 45 minutes after start (run 30768824742,
/// `Bnjoroge1/preloop-conformance-sample`, lease-expiry experiment), with no
/// automatic retry (`run_attempt` stayed 1). 2700 s matches that window;
/// the runner-side renew loop still gives up at LockedUntil + 5 min grace,
/// mirroring the official dispatcher.
pub const JOB_LEASE_SECONDS: u64 = 2700;

/// How stale a job lease may grow while its session still polls before the
/// worker is declared hung. The runner renews every ~60s; three missed
/// renewals is a wedged renew task, not a slow one. Far below
/// [`JOB_LEASE_SECONDS`] because the live session already proves the guest is
/// reachable — the lease is only stale because the worker died.
pub const HUNG_WORKER_LEASE_SECONDS: u64 = 180;

pub fn agent_request_locked_until() -> String {
    server_iso_at(SystemTime::now() + Duration::from_secs(JOB_LEASE_SECONDS))
}

pub fn task_result_status(result: azdo::TaskResult) -> ExecutionStatus {
    match result {
        azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues => {
            ExecutionStatus::Success
        }
        azdo::TaskResult::Failed => ExecutionStatus::Failure,
        azdo::TaskResult::Cancelled => ExecutionStatus::Cancelled,
        azdo::TaskResult::Skipped => ExecutionStatus::Skipped,
        // Official TaskResult.Abandoned — the job never finished on its
        // runner; GitHub concludes abandoned self-hosted jobs as failed.
        azdo::TaskResult::Abandoned => ExecutionStatus::Failure,
    }
}

pub fn resolve_callback_job(
    tx: &crate::control::txstate::TxState,
    plan_id: &str,
    timeline_id: Option<uuid::Uuid>,
    agent_job_id: Option<uuid::Uuid>,
) -> Option<(i64, RunId, JobId)> {
    let request_id = tx
        .plan_requests
        .get(plan_id)
        .copied()
        .or_else(|| timeline_id.and_then(|id| tx.timeline_requests.get(&id).copied()))
        .or_else(|| agent_job_id.and_then(|id| tx.agent_job_requests.get(&id).copied()))?;
    let request = tx.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

pub fn sole_active_unfinished_request(tx: &crate::control::txstate::TxState) -> Option<i64> {
    let mut active = tx
        .session_active_requests
        .values()
        .copied()
        .filter(|request_id| {
            tx.job_requests
                .get(request_id)
                .is_some_and(|request| request.result.is_none())
        });
    let request_id = active.next()?;
    if active.next().is_none() {
        return Some(request_id);
    }
    None
}
pub fn job_request_tuple(
    tx: &crate::control::txstate::TxState,
    request_id: i64,
) -> Option<(i64, RunId, JobId)> {
    let request = tx.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

/// Mask job-completion annotations with the run's canonical secret masker
/// before persisting them. Crash annotations (the official runner's
/// worker-crash detail from `ForceFailJob`) embed worker stdout/stderr, which
/// can contain secret values; the raw `JobCompletion` is the protocol boundary
/// and is not safe to store or return as-is.
fn mask_completion_annotations(
    run: &RunRecord,
    completion: &JobCompletion,
) -> Vec<serde_json::Value> {
    preloop_gha_protocol::mask_annotations(
        completion.annotations.clone(),
        preloop_gha_protocol::masking::expose_values(run.submission.secrets.values())
            .iter()
            .map(String::as_str),
    )
}

/// Map a `completejob` stepResult's status + conclusion to the run record's
/// step-conclusion string, when the step is terminally reported.
///
/// Status is the official TimelineRecordState (`completed` or 2); only a
/// terminal status makes the conclusion authoritative — in-progress/pending
/// steps stay for the reconciliation pass. Conclusion is the official
/// TaskResult (`succeeded`/`succeededwithissues`/`failed`/`canceled`/
/// `skipped`/`abandoned`, or the numeric 0..5 forms).
fn completion_step_conclusion(wire: &preloop_gha_protocol::CompletionStepResult) -> Option<String> {
    let terminal = match wire.status.as_ref()?.as_str() {
        Some("completed") => true,
        Some(_) => false,
        None => matches!(wire.status.as_ref()?.as_u64(), Some(2 | 3)),
    };
    if !terminal {
        return None;
    }
    let conclusion = match wire.conclusion.as_ref()?.as_str() {
        Some(text) => match text.to_ascii_lowercase().as_str() {
            "succeeded" | "succeededwithissues" => "success",
            "failed" | "abandoned" => "failure",
            "canceled" | "cancelled" => "cancelled",
            "skipped" => "skipped",
            _ => return None,
        },
        None => match wire.conclusion.as_ref()?.as_u64() {
            Some(0 | 1) => "success",
            Some(2 | 5) => "failure",
            Some(3) => "cancelled",
            Some(4) => "skipped",
            _ => return None,
        },
    };
    Some(conclusion.to_owned())
}

pub async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    if !completion.status.is_terminal() {
        return Err(ApiError::bad_request(
            "job completion status must be terminal",
        ));
    }
    // Everything the post-commit side-effects need, produced inside one
    // scheduling transaction.
    struct CompletionTx {
        early: Option<RunRecord>,
        effective_status: ExecutionStatus,
        cancelled_siblings: Vec<JobId>,
        scheduling: runtime_scheduling::SchedulingOutcome,
        queue_nonempty: bool,
        newly_terminal_success: bool,
        finalized_callers: Vec<JobId>,
        live_log_key: String,
        completed_attempt: Option<(uuid::Uuid, Vec<StepRecord>)>,
        completion_revision: u64,
    }

    let comp = completion.clone();
    // `runs: {run_id}` + `runs_referenced` — the completion mutates its own
    // run plus the queued jobs' runs it promotes (`promote_ready_jobs`
    // summarizes them). `concurrency: true` widens `runs` with every holder's
    // run (a released gate can cancel or promote a different run).
    // `sessions: {owner}` — `settle_request` drops the moot cancellation from
    // the owner session's inflight messages; the owner is resolved before
    // the transaction.
    let sessions = match comp.agent_job_id {
        Some(id) => shared
            .state
            .backend
            .find_session_by_agent_job_id(id)
            .await
            .map_err(ApiError::from)?
            .into_iter()
            .collect(),
        // Test/internal completions carry no `agent_job_id`; resolve every
        // session owning a request of this run so `settle_request` can still
        // drop the moot cancellation from each owner session's inflight.
        None => shared
            .state
            .backend
            .find_sessions_by_run(comp.run_id)
            .await
            .map_err(ApiError::from)?,
    };
    let scope = crate::control::txstate::TxScope {
        runs: Some(std::collections::BTreeSet::from([comp.run_id])),
        // `ready_queue`/`blocked_jobs` stay unloaded: `complete_job`
        // promotes `pending_jobs`, and `needs:` never cross runs, so this
        // run's own pending rows already load via the `runs` clause.
        // Loading every queued/blocked `payload_blob` is O(#queued) per
        // completion for no benefit. `queue_nonempty`/`next_runs_on` read
        // the O(1) `ready_index`/`next_queue_labels` snapshots.
        ready_queue: false,
        blocked_jobs: false,
        sessions: Some(sessions),
        concurrency: true,
        runs_referenced: true,
        job_requests_all: false,
        pending_expansions: false,
        runs_via_requests: false,
    };
    let tx_out = shared
        .state
        .backend
        .transact_scoped(&scope, move |tx| {
            let mut newly_terminal_success = false;
            tx.claimed_jobs.remove(&(comp.run_id, comp.job_id.clone()));
            let finalized_callers: Vec<JobId>;
            {
                let run = tx.runs.get_mut(&comp.run_id).ok_or_else(|| {
                    crate::control::ControlError::NotFound("run not found".to_owned())
                })?;
                let prior = run.jobs.get(&comp.job_id).copied().ok_or_else(|| {
                    crate::control::ControlError::Backend(anyhow::anyhow!(
                        "job does not belong to run"
                    ))
                })?;
                if prior.is_terminal() && prior != ExecutionStatus::Cancelled {
                    return Ok(CompletionTx {
                        early: Some(run.clone()),
                        effective_status: prior,
                        cancelled_siblings: Vec::new(),
                        scheduling: runtime_scheduling::SchedulingOutcome::default(),
                        queue_nonempty: false,
                        newly_terminal_success: false,
                        finalized_callers: Vec::new(),
                        live_log_key: String::new(),
                        completed_attempt: None,
                        completion_revision: 0,
                    });
                }
                let tolerated = run
                    .job_continue_on_error
                    .get(&comp.job_id.to_string())
                    .copied()
                    .unwrap_or(false);
                let reported_status = if tolerated && comp.status == ExecutionStatus::Failure {
                    ExecutionStatus::Success
                } else {
                    comp.status
                };
                let effective = match (prior, reported_status) {
                    (ExecutionStatus::Cancelled, ExecutionStatus::Success)
                    | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => {
                        ExecutionStatus::Cancelled
                    }
                    _ => reported_status,
                };
                run.jobs.insert(comp.job_id.clone(), effective);
                let job_name = comp.job_id.0.clone();
                let annotations = mask_completion_annotations(run, &comp);
                if let Some(detail) = JobDetail::find(&mut run.jobs_list, &job_name) {
                    detail.conclusion = format!("{:?}", effective).to_lowercase();
                    if !comp.annotations.is_empty() {
                        detail.annotations = annotations;
                    }
                } else {
                    run.jobs_list.push(JobDetail {
                        job_id: job_name.clone(),
                        name: job_name,
                        conclusion: format!("{:?}", effective).to_lowercase(),
                        steps: Vec::new(),
                        annotations,
                    });
                }
                run.job_outputs.insert(
                    comp.job_id.clone(),
                    comp.outputs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                );
                finalized_callers = propagate_reusable_outputs(run);
                run.status = summarize_run(run.jobs.values().copied());
                if run.started_at.is_none() {
                    run.started_at = Some(chrono::Utc::now());
                }
                if matches!(
                    run.status,
                    ExecutionStatus::Success
                        | ExecutionStatus::Failure
                        | ExecutionStatus::Cancelled
                        | ExecutionStatus::Skipped
                ) && run.completed_at.is_none()
                {
                    run.completed_at = Some(chrono::Utc::now());
                    run.conclusion = Some(status_string(run.status));
                    newly_terminal_success = run.status == ExecutionStatus::Success;
                }
            }
            let live_log_key = comp
                .agent_job_id
                .or_else(|| {
                    tx.job_requests
                        .values()
                        .filter(|record| {
                            record.run_id == comp.run_id && record.job_id == comp.job_id
                        })
                        .max_by_key(|record| record.request_id)
                        .map(|record| record.agent_job_id)
                })
                .map(|agent_job_id| agent_job_id.to_string())
                .unwrap_or_else(|| comp.job_id.0.clone());
            let effective_status = tx
                .runs
                .get(&comp.run_id)
                .and_then(|r| r.jobs.get(&comp.job_id).copied())
                .unwrap_or(comp.status);
            let mut completed_attempt: Option<(uuid::Uuid, Vec<StepRecord>)> = None;
            let mut completion_revision = 0_u64;
            if let Some(agent_job_id) = comp.agent_job_id.or_else(|| {
                tx.job_requests
                    .values()
                    .filter(|record| record.run_id == comp.run_id && record.job_id == comp.job_id)
                    .max_by_key(|record| record.request_id)
                    .map(|record| record.agent_job_id)
            }) {
                if let Some(manifest) = tx.job_steps.get_mut(&agent_job_id) {
                    for wire in &comp.step_results {
                        let Some(external_id) = wire.external_id.as_deref() else {
                            continue;
                        };
                        let Some(pos) = StepRecord::find_by_id(manifest, external_id) else {
                            continue;
                        };
                        if let Some(conclusion) = completion_step_conclusion(wire) {
                            manifest[pos].conclusion = conclusion;
                        }
                        if let Some(number) = wire.number.and_then(|n| u32::try_from(n).ok()) {
                            manifest[pos].runner_number = Some(number);
                        }
                    }
                    let orphan_conclusion = status_string(effective_status);
                    for step in manifest.iter_mut() {
                        if step.conclusion == "in_progress" {
                            step.conclusion = orphan_conclusion.clone();
                            step.finished_at = step.finished_at.or(Some(chrono::Utc::now()));
                        }
                    }
                    completed_attempt = Some((agent_job_id, manifest.clone()));
                    let counter = tx.job_steps_revision.entry(agent_job_id).or_insert(0);
                    *counter += 1;
                    completion_revision = *counter;
                }
            }
            let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
                crate::control::sched::apply_matrix_fail_fast(tx, comp.run_id, &comp.job_id)
            } else {
                Vec::new()
            };
            tx.retain_ready(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
            tx.pending_jobs
                .retain(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
            tx.concurrency_blocked
                .retain(|job| !(job.run_id == comp.run_id && job.job_id == comp.job_id));
            if let Some(held) = tx.held_runs.get_mut(&comp.run_id) {
                held.retain(|job| job.job_id != comp.job_id);
                if held.is_empty() {
                    tx.held_runs.remove(&comp.run_id);
                }
            }
            crate::control::sched::release_concurrency_for_job(tx, comp.run_id, &comp.job_id);
            for caller_id in &finalized_callers {
                crate::control::sched::release_concurrency_for_job(tx, comp.run_id, caller_id);
            }
            let scheduling = crate::control::sched::promote_ready_jobs(tx);
            let finished_request_ids: Vec<i64> = tx
                .job_requests
                .iter()
                .filter(|(_, r)| r.run_id == comp.run_id && r.job_id == comp.job_id)
                .map(|(id, _)| *id)
                .collect();
            for request_id in &finished_request_ids {
                // Shared settlement also clears the session's moot
                // `JobCancellation` inflight message so it is not redelivered
                // on the post-completion busy poll.
                crate::control::sched::settle_request(tx, *request_id, effective_status);
            }
            // `ready_count` is the O(1) pre-tx global count; `ready_index`
            // holds this tx's promotions under the narrow scope.
            let queue_nonempty = tx.ready_count > 0
                || !tx.ready_index.is_empty()
                || !tx.cancellation_queue.is_empty();
            Ok(CompletionTx {
                early: None,
                effective_status,
                cancelled_siblings,
                scheduling,
                queue_nonempty,
                newly_terminal_success,
                finalized_callers,
                live_log_key,
                completed_attempt,
                completion_revision,
            })
        })
        .await
        .map_err(ApiError::from)?;

    if let Some(run) = tx_out.early {
        return Ok(Json(run));
    }
    let CompletionTx {
        effective_status,
        cancelled_siblings,
        mut scheduling,
        queue_nonempty,
        newly_terminal_success,
        finalized_callers: _,
        live_log_key,
        ..
    } = tx_out;

    // Node-local bookkeeping that does not belong to the scheduling tx:
    // close the live-log feed and drop the run's DAP port registration.
    {
        let mut inner = shared.state.inner.lock().await;
        crate::live_logs::close_live_log(&mut inner, &live_log_key);
        inner.dap_ports.remove(&completion.run_id);
    }
    // Refresh the on-demand pool wake atomic and the next-job labels from the
    // committed scheduling state.
    let (queue_len, next_labels) = shared
        .state
        .backend
        .read_scoped(
            // No queue families: `ready_count`/`next_queue_labels` are O(1)
            // SQL snapshots — loading `ready_queue` would parse every queued
            // `payload_blob` per completion.
            &crate::control::txstate::TxScope::runs(Default::default()),
            |tx| {
                Ok((
                    tx.ready_count.max(0) as usize,
                    crate::control::sched::next_job_labels(tx),
                ))
            },
        )
        .await
        .unwrap_or((0, Vec::new()));
    shared
        .state
        .queue_depth
        .store(queue_len, std::sync::atomic::Ordering::Release);
    *shared.state.next_job_runs_on.write().unwrap() = next_labels;

    // Any reusable-caller or dynamic-matrix node the sweep above unblocked was
    // deferred rather than expanded under the lock. Build those subtrees now
    // that the guard is released, and fold the result into the outcome the
    // notify and event fan-out below reports on.
    scheduling.merge(drain_expansions(&shared).await);

    // Deferred expansion runs after the snapshot point above and can mutate
    // the run record: an empty dynamic matrix concludes its node as Skipped, a
    // failed build as Failure, and a successful one materializes the subtree.
    // Re-read so the emitted RunStatus, the terminal workspace cleanup, and
    // the returned record reflect the post-expansion state — publishing the
    // pre-expansion snapshot would report a failed/empty expansion as still
    // in progress and skip terminal workspace cleanup.
    let record = {
        let run_id = completion.run_id;
        shared
            .state
            .backend
            .read_scoped(&crate::control::txstate::TxScope::run(run_id), move |tx| {
                tx.runs.get(&run_id).cloned().ok_or_else(|| {
                    crate::control::ControlError::NotFound("run not found".to_owned())
                })
            })
            .await
            .map_err(ApiError::from)?
    };

    // Webhook-driven auto-PR: a successful push run may open a PR per
    // policy. Fires only after deferred expansions have settled: an
    // expansion can add work or conclude a subtree, and the pre-expansion
    // "success" snapshot is not the final truth (the record above is
    // re-read post-expansion). Best-effort and detached — a GitHub outage
    // must never affect the run's own result.
    if newly_terminal_success
        && record.status == ExecutionStatus::Success
        && record.conclusion.as_deref() == Some("success")
    {
        let shared = shared.clone();
        let run_id = completion.run_id;
        tokio::spawn(async move {
            crate::github_pr::maybe_open_pr(shared, run_id).await;
        });
    }

    // Off the completion path on purpose: reporting check runs to GitHub is one
    // or more network PATCH requests per job. Awaiting it here stalls the
    // runner's HTTP finish request and delays pool slot turnover.
    let mut check_reports = Vec::new();
    check_reports.push((
        completion.run_id,
        completion.job_id.clone(),
        effective_status,
    ));
    for job_id in &cancelled_siblings {
        check_reports.push((
            completion.run_id,
            job_id.clone(),
            ExecutionStatus::Cancelled,
        ));
    }
    for (run_id, job_id) in &scheduling.skipped {
        check_reports.push((*run_id, job_id.clone(), ExecutionStatus::Skipped));
    }
    for (run_id, job_id) in &scheduling.failed {
        check_reports.push((*run_id, job_id.clone(), ExecutionStatus::Failure));
    }
    let report_shared = Arc::clone(&shared);
    tokio::spawn(async move {
        for (run_id, job_id, status) in check_reports {
            github::report_check_run_completed(&report_shared, run_id, &job_id, status).await;
        }
    });

    if scheduling.promoted > 0 || !cancelled_siblings.is_empty() || queue_nonempty {
        shared.state.message_notify.notify_waiters();
    }

    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id: completion.run_id,
            job_id: completion.job_id,
            status: effective_status,
            reason: None,
        })
        .await;
    for job_id in cancelled_siblings {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id: completion.run_id,
                job_id,
                status: ExecutionStatus::Cancelled,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.skipped {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Skipped,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.failed {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Failure,
                reason: None,
            })
            .await;
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id: completion.run_id,
            status: record.status,
            reason: None,
        })
        .await;
    if record.status.is_terminal() {
        let state_dir = shared.state.state_dir.clone();
        // Keep the immutable Git snapshot briefly after terminal status. A
        // runner can be slow to claim a job (or reconnect after a control
        // plane restart), and deleting the repository immediately makes the
        // otherwise-valid checkout fail with "workspace snapshot not found".
        // The retention is bounded housekeeping; it does not change run
        // semantics and keeps object-cache reuse intact. Zero discards at
        // once — tests set it so completion is observable synchronously.
        let retention = shared.state.snapshot_retention_seconds;
        if retention == 0 {
            discard_workspace_snapshot(&state_dir, completion.run_id).await;
        } else {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(retention)).await;
                discard_workspace_snapshot(&state_dir, completion.run_id).await;
            });
        }
        // A remote checkout cache is released rather than deleted with the
        // run: the configured retention window is what a rerun or a late
        // retry fetches from, and the sweep below is the only collector.
        let released = {
            let inner = shared.state.inner.lock().await;
            inner
                .runs
                .get(&completion.run_id)
                .and_then(|run| run.workspace_snapshot.clone())
        };
        if let Some(snapshot) = released {
            release_remote_checkout_snapshot(&shared.state.state_dir, &snapshot, completion.run_id)
                .await;
        }
        if shared.state.checkout_cache.mode != crate::config::CheckoutCacheMode::Off {
            let state_dir = shared.state.state_dir.clone();
            let checkout_cache = shared.state.checkout_cache.clone();
            tokio::spawn(async move { prune_checkout_cache(&state_dir, &checkout_cache).await });
        }
        // Off the completion path on purpose: this is housekeeping, and the
        // runner is waiting on this response before its slot can turn over.
        let state_dir = shared.state.state_dir.clone();
        let active_plans = shared
            .state
            .backend
            .read_scoped(&crate::control::txstate::TxScope::requests_only(), |tx| {
                Ok(tx
                    .job_requests
                    .values()
                    .filter(|r| r.result.is_none())
                    .map(|r| r.plan_id.clone())
                    .collect::<std::collections::BTreeSet<_>>())
            })
            .await
            .unwrap_or_default();
        let log_segments = shared.state.log_segments.clone();
        let plans_clone = active_plans.clone();
        tokio::spawn(async move {
            prune_replay_results(&state_dir, &active_plans).await;
            log_segments.prune_inactive_plans(&plans_clone).await;
        });
    }
    Ok(Json(record))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Method, Request};
    use serde_json::Value;
    use tower::ServiceExt;

    const TEST_API_TOKEN: &str = "cluster-g-test-token";

    fn test_app(state: AppState, shutdown: CancellationToken) -> Router {
        app_with_test_api(state, shutdown, TEST_API_TOKEN)
    }

    async fn test_request(app: &Router, method: Method, uri: &str, body: Value) -> Value {
        let mut builder = Request::builder().method(method).uri(uri);
        if uri.starts_with("/api/v1/") {
            builder = builder.header(header::AUTHORIZATION, "Bearer preloop-system-token");
        } else if uri.starts_with("/internal/test/") {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"));
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
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    /// Completing the last real job of a run with a deferred dynamic matrix
    /// runs the expansion inside the SAME completion call (`drain_expansions`).
    /// When the expansion fails, the completion response used to carry the
    /// pre-expansion snapshot: the run was still reported in progress and the
    /// terminal workspace cleanup was skipped.
    #[tokio::test]
    async fn completion_response_reflects_post_expansion_failure() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = test_app(state.clone(), CancellationToken::new());

        let accepted = test_request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  generator:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  downstream:
    needs: [generator]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.generator.outputs.matrix) }}
    steps:
      - run: echo dynamic
"#,
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        // `42` parses as JSON but is not a matrix: the deferred expansion
        // fails, concluding the downstream node (and the run) as Failure.
        let response = test_request(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "generator",
                "status": "success",
                "outputs": {"matrix": "42"}
            }),
        )
        .await;

        assert_eq!(
            response["status"], "failure",
            "completion must publish the post-expansion run status, got {}",
            response["status"]
        );
        assert_eq!(
            response["jobs"]["downstream"], "failure",
            "completion must publish the failed expansion node"
        );
    }

    /// Crash annotations embed worker stdout/stderr, which can contain secret
    /// values. Both the persisted run and the completion response must carry
    /// the masked form, never the raw completion payload.
    #[tokio::test]
    async fn completion_annotations_are_masked_before_storage_and_response() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state
            .secrets
            .write()
            .global
            .insert("CRASH_SECRET".to_owned(), "super-secret-value".to_owned());
        let app = test_app(state.clone(), CancellationToken::new());

        let accepted = test_request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": "owner/repo"
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        let response = test_request(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "build",
                "status": "failure",
                "outputs": {},
                "annotations": [
                    {"message": "worker crashed: super-secret-value leaked", "level": "failure"}
                ]
            }),
        )
        .await;

        let response_message = response["jobs_list"][0]["annotations"][0]["message"]
            .as_str()
            .unwrap();
        assert!(
            !response_message.contains("super-secret-value"),
            "completion response must not carry the raw secret: {response_message}"
        );

        let stored = test_request(
            &app,
            Method::GET,
            &format!("/api/v1/runs/{run_id}"),
            Value::Null,
        )
        .await;
        let stored_message = stored["jobs_list"][0]["annotations"][0]["message"]
            .as_str()
            .unwrap();
        assert!(
            !stored_message.contains("super-secret-value"),
            "persisted run must not carry the raw secret: {stored_message}"
        );
    }

    /// A reusable caller whose strategy matrix reads `needs` is deferred at
    /// parse time (its matrix cannot be resolved until the needs outputs
    /// exist). At runtime the matrix must be resolved against the completed
    /// outputs and the callee materialized once per cell, exactly like a
    /// static-matrix caller.
    #[tokio::test]
    async fn deferred_reusable_caller_with_needs_matrix_fans_out_per_cell() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = test_app(state.clone(), CancellationToken::new());

        let accepted = test_request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  gen:
    runs-on: ubuntu-latest
    steps:
      - run: echo gen
  call:
    needs: [gen]
    uses: ./.github/workflows/callee.yml
    strategy:
      matrix: ${{ fromJson(needs.gen.outputs.matrix) }}
"#,
                "event": "push",
                "repository": "owner/repo",
                "reusable_workflows": {
                    ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  inner:
    runs-on: ubuntu-latest
    steps:
      - run: echo inner
"#
                }
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        test_request(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "gen",
                "status": "success",
                "outputs": {"matrix": "{\"include\": [{\"os\": \"linux\"}, {\"os\": \"macos\"}]}"}
            }),
        )
        .await;

        let legs: Vec<JobId> = {
            let inner = state.test_tx().await;
            let run = inner.runs.get(&RunId(run_id.parse().unwrap())).unwrap();
            run.jobs
                .keys()
                .filter(|id| id.0.starts_with("call ("))
                .cloned()
                .collect()
        };
        assert_eq!(
            legs.len(),
            2,
            "one materialized callee leg per resolved matrix cell"
        );
        for leg in &legs {
            assert!(
                leg.0.ends_with("/inner"),
                "leg id must be the caller-cell-prefixed inner job: {leg}"
            );
        }

        // Completing every leg concludes the caller (and the run).
        for leg in &legs {
            test_request(
                &app,
                Method::POST,
                "/internal/test/jobs/complete",
                json!({
                    "run_id": run_id,
                    "job_id": leg.0,
                    "status": "success",
                    "outputs": {}
                }),
            )
            .await;
        }
        let inner = state.test_tx().await;
        assert_eq!(
            inner.runs[&RunId(run_id.parse().unwrap())].status,
            ExecutionStatus::Success,
            "run must conclude once every deferred-matrix leg completes"
        );
    }

    /// A deferred matrix that lives inside a reusable workflow is promoted
    /// with a caller-prefixed runtime id that does not exist in the root
    /// workflow; the runtime must expand it against the callee workflow that
    /// actually contains the job.
    #[tokio::test]
    async fn deferred_matrix_inside_reusable_expands_against_the_callee() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = test_app(state.clone(), CancellationToken::new());

        let accepted = test_request(
            &app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/callee.yml
"#,
                "event": "push",
                "repository": "owner/repo",
                "reusable_workflows": {
                    ".github/workflows/callee.yml": r#"
on: workflow_call
jobs:
  setup:
    runs-on: ubuntu-latest
    steps:
      - run: echo setup
  build:
    needs: [setup]
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJson(needs.setup.outputs.matrix) }}
    steps:
      - run: echo build
"#
                }
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();

        test_request(
            &app,
            Method::POST,
            "/internal/test/jobs/complete",
            json!({
                "run_id": run_id,
                "job_id": "call/setup",
                "status": "success",
                "outputs": {"matrix": "{\"include\": [{\"node\": \"1\"}, {\"node\": \"2\"}]}"}
            }),
        )
        .await;

        let legs: Vec<JobId> = {
            let inner = state.test_tx().await;
            let run = inner.runs.get(&RunId(run_id.parse().unwrap())).unwrap();
            run.jobs
                .keys()
                .filter(|id| id.0.starts_with("call/build ("))
                .cloned()
                .collect()
        };
        assert_eq!(
            legs.len(),
            2,
            "callee-local deferred matrix must fan out: {:?}",
            legs
        );

        for leg in &legs {
            test_request(
                &app,
                Method::POST,
                "/internal/test/jobs/complete",
                json!({
                    "run_id": run_id,
                    "job_id": leg.0,
                    "status": "success",
                    "outputs": {}
                }),
            )
            .await;
        }
        let inner = state.test_tx().await;
        assert_eq!(
            inner.runs[&RunId(run_id.parse().unwrap())].status,
            ExecutionStatus::Success,
            "run must conclude once the callee-local matrix legs complete"
        );
    }
}
