use super::*;

pub(crate) async fn next_message(
    State(shared): State<Arc<SharedState>>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> (StatusCode, Json<Option<azdo::TaskAgentMessage>>) {
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
        if let Some(message) = inner
            .inflight_messages
            .get(&session_id)
            .and_then(|messages| messages.values().next().cloned())
        {
            return (StatusCode::ACCEPTED, Json(Some(message)));
        }

        if let Some(cancellation) = inner.cancellation_queue.pop_front() {
            let body_json = concurrency::job_cancel_body(cancellation.agent_job_id);
            match build_task_agent_message(
                &mut inner,
                &session_id,
                azdo::message_type::JOB_CANCELLED,
                body_json,
            ) {
                Ok(message) => return (StatusCode::OK, Json(Some(message))),
                Err(_) => return (StatusCode::ACCEPTED, Json(None)),
            }
        }

        if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
            let request_finished = inner
                .job_requests
                .get(&request_id)
                .is_none_or(|request| request.result.is_some());
            if request_finished {
                inner.session_active_requests.remove(&session_id);
            } else {
                drop(inner);
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

        let runner = inner.runner_capabilities_for_session(&session_id);
        let verified = effective_claim_runner(
            identity.as_ref().map(|axum::Extension(id)| id),
            inner.runner_id_for_session(&session_id),
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
        };

        // Update run status
        if let Some(run) = inner.runs.get_mut(&queued.run_id) {
            run.status = ExecutionStatus::InProgress;
            run.jobs
                .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        // F030: inject SystemVssConnection so the worker's AzDO reporting context
        // has a server URL, access token, and ResultsServiceUrl — same as broker_acquire_job.
        let mut msg = queued.message.clone();
        for endpoint in &mut msg.resources.endpoints {
            if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                endpoint.url = Some(runner_server_url());
                endpoint.authorization.parameters.insert(
                    "AccessToken".to_owned(),
                    shared
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
        debug!(
            endpoint_count = msg.resources.endpoints.len(),
            "F030: injected SystemVssConnection into AzDO job message"
        );
        let body_json = serde_json::to_string(&msg)
            .map_err(|e| ApiError::bad_request(format!("failed to serialize job message: {e}")));
        let body_json = match body_json {
            Ok(b) => b,
            Err(_) => return (StatusCode::ACCEPTED, Json(None)),
        };
        let request_id = queued.message.request_id;
        inner
            .session_active_requests
            .insert(session_id.clone(), request_id);
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.started_at = Some(std::time::SystemTime::now());
        }
        let message = build_task_agent_message(
            &mut inner,
            &session_id,
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
            body_json,
        );

        let message = match message {
            Ok(m) => m,
            Err(_) => return (StatusCode::ACCEPTED, Json(None)),
        };

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

        return (StatusCode::ACCEPTED, Json(Some(message)));
    }
}

pub(crate) async fn delete_session_message(
    State(shared): State<Arc<SharedState>>,
    Path((session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, &session_id, message_id).await
}

pub(crate) fn build_task_agent_message(
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
        iv: Some(BASE64_STANDARD.encode(&iv)),
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    Ok(message)
}

pub(crate) fn build_broker_plaintext_message(
    inner: &mut InnerState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> azdo::TaskAgentMessage {
    inner.next_message_id += 1;
    let message_id = inner.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: body_json,
        iv: None,
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    message
}

pub(crate) async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    ack_message(shared, session_id, message_id).await
}

pub(crate) async fn ack_message(
    shared: Arc<SharedState>,
    session_id: &str,
    message_id: i64,
) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    if let Some(messages) = inner.inflight_messages.get_mut(session_id) {
        messages.remove(&message_id);
        if messages.is_empty() {
            inner.inflight_messages.remove(session_id);
        }
    }
    StatusCode::NO_CONTENT
}

pub(crate) async fn complete_job(
    State(shared): State<Arc<SharedState>>,
    Json(completion): Json<JobCompletion>,
) -> Result<Json<RunRecord>, ApiError> {
    complete_job_inner(shared, completion).await
}

pub(crate) async fn complete_job_compat(
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
            annotations: Vec::new(),
            step_results: Vec::new(),
        },
    )
    .await
}

/// GET /_apis/v1/AgentRequest/:pool_id/:request_id to query a job request lease/result.
///
/// The official listener calls this when another job arrives while the previous
/// worker process may still be unwinding. Returning a completed `result` lets it
/// safely move on; 404/405 makes it cancel the worker and can poison matrix runs.
pub(crate) async fn agent_request_get(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let request = inner
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("agent request not found"))?;
    Ok(Json(agent_request_json(pool_id, request)))
}

/// POST /_apis/v1/AgentRequest/:pool_id/:request_id — best-effort request ack.
pub(crate) async fn agent_request_ack(
    Path((_pool_id, _request_id)): Path<(i64, i64)>,
) -> StatusCode {
    StatusCode::OK
}

/// PATCH /_apis/v1/AgentRequest/:pool_id/:request_id — renew or complete job request.
/// The runner sends this to renew the job lock or report completion.
pub(crate) async fn agent_request_patch(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
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
    if let Some(result) = body.get("result").and_then(|v| v.as_str()) {
        let new_status = match execution_status_from_runner_result(result) {
            Some(status) => status,
            None => {
                info!(
                        request_id,
                        result = %result_hint,
                        "unknown agent_request_patch result; skipping completion"
                );
                return Json(
                    json!({ "requestId": request_id, "lockedUntil": agent_request_locked_until() }),
                );
            }
        };
        // Look up (run_id, job_id) under the inner lock, then drop it before calling
        // complete_job_inner which acquires the lock itself.
        let completion = {
            let mut inner = shared.state.inner.lock().await;
            let mut already_completed = false;
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                already_completed = request.result.is_some();
                request.result = Some(new_status);
                request.locked_until = agent_request_locked_until();
            }
            if already_completed {
                inner.inflight_requests.remove(&request_id);
                info!(
                    request_id,
                    result = %result_hint,
                    "agent request already completed; refreshing result only"
                );
                None
            } else if let Some((run_id, job_id)) = inner.inflight_requests.remove(&request_id) {
                info!(
                    %run_id,
                    %job_id,
                    result = %result_hint,
                    "job completed via agent_request_patch"
                );
                Some(JobCompletion {
                    run_id,
                    job_id,
                    status: new_status,
                    outputs: Default::default(),
                    annotations: Vec::new(),
                    step_results: Vec::new(),
                })
            } else {
                info!(
                    request_id,
                    "no inflight job for request_id; ignoring result"
                );
                None
            }
        };
        if let Some(c) = completion {
            let _ = complete_job_inner(shared.clone(), c).await;
        }
        return Json(agent_request_response(&shared, pool_id, request_id).await);
    }
    // Renewal — runner is still working; just extend the lock.
    {
        let mut inner = shared.state.inner.lock().await;
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.locked_until = agent_request_locked_until();
            request.last_renewed_at = Some(std::time::SystemTime::now());
        }
    }
    Json(agent_request_response(&shared, pool_id, request_id).await)
}

pub(crate) async fn agent_request_response(
    shared: &Arc<SharedState>,
    pool_id: i64,
    request_id: i64,
) -> serde_json::Value {
    let inner = shared.state.inner.lock().await;
    inner
        .job_requests
        .get(&request_id)
        .map(|request| agent_request_json(pool_id, request))
        .unwrap_or_else(|| {
            json!({
                "requestId": request_id,
                "poolId": pool_id,
                "lockedUntil": agent_request_locked_until(),
            })
        })
}

pub(crate) fn agent_request_json(
    pool_id: i64,
    request: &TaskAgentJobRequestRecord,
) -> serde_json::Value {
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

pub(crate) fn agent_request_result(status: ExecutionStatus) -> &'static str {
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
pub(crate) const JOB_LEASE_SECONDS: u64 = 2700;

pub(crate) fn agent_request_locked_until() -> String {
    server_iso_at(SystemTime::now() + Duration::from_secs(JOB_LEASE_SECONDS))
}

pub(crate) fn task_result_status(result: azdo::TaskResult) -> ExecutionStatus {
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

pub(crate) fn resolve_callback_job(
    inner: &InnerState,
    plan_id: &str,
    timeline_id: Option<uuid::Uuid>,
    agent_job_id: Option<uuid::Uuid>,
) -> Option<(i64, RunId, JobId)> {
    let request_id = inner
        .plan_requests
        .get(plan_id)
        .copied()
        .or_else(|| timeline_id.and_then(|id| inner.timeline_requests.get(&id).copied()))
        .or_else(|| agent_job_id.and_then(|id| inner.agent_job_requests.get(&id).copied()))?;
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

pub(crate) fn sole_active_unfinished_request(inner: &InnerState) -> Option<i64> {
    let mut active = inner
        .session_active_requests
        .values()
        .copied()
        .filter(|request_id| {
            inner
                .job_requests
                .get(request_id)
                .is_some_and(|request| request.result.is_none())
        });
    let request_id = active.next()?;
    if active.next().is_none() {
        return Some(request_id);
    }
    None
}
pub(crate) fn job_request_tuple(
    inner: &InnerState,
    request_id: i64,
) -> Option<(i64, RunId, JobId)> {
    let request = inner.job_requests.get(&request_id)?;
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

pub(crate) async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    if !completion.status.is_terminal() {
        return Err(ApiError::bad_request(
            "job completion status must be terminal",
        ));
    }
    let mut inner = shared.state.inner.lock().await;
    let finalized_callers: Vec<JobId>;
    // Set when this completion finalizes the whole run with a success
    // conclusion — the auto-PR trigger fires exactly once per run because
    // the block below runs only while `completed_at` is still `None`.
    let mut newly_terminal_success = false;
    inner
        .claimed_jobs
        .remove(&(completion.run_id, completion.job_id.clone()));
    {
        let run = inner
            .runs
            .get_mut(&completion.run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let prior = run
            .jobs
            .get(&completion.job_id)
            .copied()
            .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
        if prior.is_terminal() && prior != ExecutionStatus::Cancelled {
            return Ok(Json(run.clone()));
        }
        let tolerated = run
            .job_continue_on_error
            .get(&completion.job_id.to_string())
            .copied()
            .unwrap_or(false);
        let reported_status = if tolerated && completion.status == ExecutionStatus::Failure {
            ExecutionStatus::Success
        } else {
            completion.status
        };
        let effective = match (prior, reported_status) {
            (ExecutionStatus::Cancelled, ExecutionStatus::Success)
            | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => ExecutionStatus::Cancelled,
            _ => reported_status,
        };
        run.jobs.insert(completion.job_id.clone(), effective);
        let job_name = completion.job_id.0.clone();
        if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
            run.jobs_list[pos].conclusion = format!("{:?}", effective).to_lowercase();
            // A worker can terminate through ForceFailJob before it sends the
            // final WorkflowStepsUpdate. Do not leave the last reported step
            // in_progress after its job is terminal.
            //
            // The official runner carries the authoritative per-step
            // conclusions in CompleteJob.stepResults (status=TimelineRecordState,
            // conclusion=TaskResult); apply them first. A crashed worker sends
            // none, and any step still in_progress after that is reconciled to
            // the job's effective status — the same view GitHub's server
            // presents for orphaned steps.
            for step in &mut run.jobs_list[pos].steps {
                let Some(wire) = completion
                    .step_results
                    .iter()
                    .find(|result| result.name.as_deref() == Some(step.name.as_str()))
                else {
                    continue;
                };
                if let Some(conclusion) = completion_step_conclusion(wire) {
                    step.conclusion = conclusion;
                }
            }
            let step_conclusion = status_string(effective);
            for step in &mut run.jobs_list[pos].steps {
                if step.conclusion == "in_progress" {
                    step.conclusion = step_conclusion.clone();
                    step.finished_at = step.finished_at.or(Some(chrono::Utc::now()));
                }
            }
            if !completion.annotations.is_empty() {
                run.jobs_list[pos].annotations = mask_completion_annotations(run, &completion);
            }
        } else {
            run.jobs_list.push(JobDetail {
                name: job_name,
                conclusion: format!("{:?}", effective).to_lowercase(),
                steps: Vec::new(),
                annotations: mask_completion_annotations(run, &completion),
            });
        }
        run.job_outputs.insert(
            completion.job_id.clone(),
            completion
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        finalized_callers = propagate_reusable_outputs(run);
        run.status = summarize_run(run.jobs.values().copied());
        // A completed job proves that the run has started. Keep the first
        // observed completion as a conservative fallback when the runner did
        // not report an earlier start transition.
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
    // Close only after the run and job were validated and the completion was
    // projected. Invalid callbacks must not terminate another job's feed.
    let live_log_key = inner
        .job_requests
        .values()
        .find(|record| record.run_id == completion.run_id && record.job_id == completion.job_id)
        .map(|record| record.agent_job_id.to_string())
        .unwrap_or_else(|| completion.job_id.0.clone());
    crate::live_logs::close_live_log(&mut inner, &live_log_key);
    // Use the status actually stored (may differ from completion if terminal-locked).
    let effective_status = inner
        .runs
        .get(&completion.run_id)
        .and_then(|r| r.jobs.get(&completion.job_id).copied())
        .unwrap_or(completion.status);
    let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
        apply_matrix_fail_fast(&mut inner, completion.run_id, &completion.job_id)
    } else {
        Vec::new()
    };
    // A terminal job must not remain dispatchable, including completion via
    // the native/internal API before a runner acquires it.
    inner
        .queue
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .concurrency_blocked
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    if let Some(held) = inner.held_runs.get_mut(&completion.run_id) {
        held.retain(|job| job.job_id != completion.job_id);
        if held.is_empty() {
            inner.held_runs.remove(&completion.run_id);
        }
    }
    // Release concurrency for the completed job / run, which may promote held work.
    release_concurrency_for_job(&mut inner, completion.run_id, &completion.job_id);
    for caller_id in &finalized_callers {
        release_concurrency_for_job(&mut inner, completion.run_id, caller_id);
    }
    let mut scheduling = promote_ready_jobs(&mut inner);
    // The on-demand runner supervisor wakes on this atomic, and a completion
    // can promote fresh work into the queue (a `needs:` chain or a released
    // concurrency successor). Without refreshing it here the pool stays
    // asleep on the last claim-time value and the promoted job queues
    // forever — the observed "run stuck at queued" stall. Mirrors the store
    // done at submit and at claim.
    shared
        .state
        .queue_depth
        .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    // Mark agent request finished and free the broker session slot so the
    // runner can immediately poll the next job (including concurrency successors).
    let finished_request_ids: Vec<i64> = inner
        .job_requests
        .iter()
        .filter(|(_, r)| r.run_id == completion.run_id && r.job_id == completion.job_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in &finished_request_ids {
        if let Some(req) = inner.job_requests.get_mut(request_id) {
            if req.result.is_none() {
                req.result = Some(effective_status);
            }
        }
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != *request_id);
        inner.inflight_requests.remove(request_id);
        // The job is terminal, so its deferred App-token request must not
        // outlive it: a re-claim re-mints from this record. Claimed-job
        // completions funnel through here (broker `completejob`, the legacy
        // `/_apis` finish endpoints, `agent_request_patch`, and the
        // lease-expiry reaper), and the scheduler's node retirement covers
        // cancelled, skipped, and expansion-failed nodes. Known gap: the
        // starvation sweep and the cancel of a never-claimed queued job turn
        // terminal without reaching either site; the record leaks but is
        // unreachable — the job is out of every dispatchable collection.
        inner.github_token_requests.remove(request_id);
    }
    inner.dap_ports.remove(&completion.run_id);
    let queue_nonempty = !inner.queue.is_empty() || !inner.cancellation_queue.is_empty();
    drop(inner);

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
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&completion.run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("run not found"))?
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

    github::report_check_run_completed(
        &shared,
        completion.run_id,
        &completion.job_id,
        effective_status,
    )
    .await;

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
        github::report_check_run_completed(
            &shared,
            completion.run_id,
            &job_id,
            ExecutionStatus::Cancelled,
        )
        .await;
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
        github::report_check_run_completed(&shared, run_id, &job_id, ExecutionStatus::Skipped)
            .await;
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
        github::report_check_run_completed(&shared, run_id, &job_id, ExecutionStatus::Failure)
            .await;
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
        discard_workspace_snapshot(&shared.state.state_dir, completion.run_id).await;
        // Off the completion path on purpose: this is housekeeping, and the
        // runner is waiting on this response before its slot can turn over.
        let state_dir = shared.state.state_dir.clone();
        let active_plans = {
            let inner = shared.state.inner.lock().await;
            inner
                .job_requests
                .values()
                .filter(|request| request.result.is_none())
                .map(|request| request.plan_id.clone())
                .collect()
        };
        tokio::spawn(async move { prune_replay_results(&state_dir, &active_plans).await });
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
            let inner = state.inner.lock().await;
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
        let run = state.inner.lock().await;
        assert_eq!(
            run.runs[&RunId(run_id.parse().unwrap())].status,
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
            let inner = state.inner.lock().await;
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
        let run = state.inner.lock().await;
        assert_eq!(
            run.runs[&RunId(run_id.parse().unwrap())].status,
            ExecutionStatus::Success,
            "run must conclude once the callee-local matrix legs complete"
        );
    }
}
