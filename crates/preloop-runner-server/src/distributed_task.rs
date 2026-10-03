use super::*;

pub async fn next_message(
    State(shared): State<Arc<SharedState>>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<(StatusCode, Json<Option<azdo::TaskAgentMessage>>), ApiError> {
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
    // One window per request (see `broker::next_message_broker_ref`).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(wait_seconds);

    loop {
        let outcome = shared
            .state
            .backend
            .poll_azdo_session(crate::control::types::AzdoPoll {
                session_id: session_id.clone(),
                verified_runner_id: verified,
            })
            .await
            // A control-DB failure must not look like "nothing to deliver":
            // the runner would long-poll forever against an outage. Surface
            // the mapped error (5xx) so it retries the way it does for any
            // other server fault.
            .map_err(|error| {
                tracing::warn!(%error, %session_id, "disttask poll failed");
                ApiError::from(error)
            })?;

        match outcome {
            crate::control::types::AzdoPollOutcome::Forbidden => {
                return Ok((StatusCode::FORBIDDEN, Json(None)));
            }
            crate::control::types::AzdoPollOutcome::Redeliver(message) => {
                let rendered = render_session_message(&shared, &session_id, message).await?;
                return Ok((StatusCode::ACCEPTED, Json(Some(rendered))));
            }
            crate::control::types::AzdoPollOutcome::Cancel(message) => {
                let rendered = render_session_message(&shared, &session_id, message).await?;
                return Ok((StatusCode::OK, Json(Some(rendered))));
            }
            crate::control::types::AzdoPollOutcome::Claimed {
                message,
                run_id,
                job_id,
                queue_depth,
                next_runs_on,
            } => {
                // Refresh the on-demand pool gauges from the committed
                // claim, exactly like the broker claim path: the ready
                // queue just shrank.
                shared
                    .state
                    .queue_depth
                    .store(queue_depth, std::sync::atomic::Ordering::Release);
                *shared.state.next_job_runs_on.write().unwrap() = next_runs_on;
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
                let rendered = render_session_message(&shared, &session_id, message).await?;
                return Ok((StatusCode::ACCEPTED, Json(Some(rendered))));
            }
            crate::control::types::AzdoPollOutcome::Wait => {
                if wait_seconds == 0 {
                    return Ok((StatusCode::OK, Json(None)));
                }
                if tokio::time::timeout_at(deadline, shared.state.message_notify.notified())
                    .await
                    .is_err()
                {
                    return Ok((StatusCode::OK, Json(None)));
                }
                continue;
            }
        }
    }
}

/// Render a queued [`crate::control::types::SessionMessage`] into the wire
/// `TaskAgentMessage` at delivery time.
///
/// Job assignments carry only `request_id`: the body is the stored job-message
/// template (`acquire_context`) with the `SystemVssConnection` endpoint filled
/// in and a fresh runtime token minted now — so no token or secret sits in the
/// queued row. Control messages (`JobCancellation`, …) carry their small
/// plaintext `body` directly.
///
/// Key decision: sessions that completed the key exchange (`broker`/`azdo`
/// rows) are session-AES encrypted with the key derived from `session_id`
/// (never stored). Messages queued while the session had no key yet
/// (`plaintext`, the implicit `default`/compat session) pass through as
/// base64-plaintext + zero IV: without a key exchange the runner cannot
/// decrypt, so encrypting would make cancellations undecodable.
async fn render_session_message(
    shared: &Arc<SharedState>,
    session_id: &str,
    message: crate::control::types::SessionMessage,
) -> Result<azdo::TaskAgentMessage, ApiError> {
    // Control messages (JobCancellation and friends) carry their own
    // `body`; a request_id on them is correlation, not a job payload. Only
    // the assignment itself is rendered from the stored template.
    let body_json = if let Some(request_id) = message
        .request_id
        .filter(|_| message.message_type == azdo::message_type::PIPELINE_AGENT_JOB_REQUEST)
    {
        let ctx = shared
            .state
            .backend
            .acquire_context(request_id)
            .await
            .map_err(ApiError::from)?;
        let mut msg = ctx.message;
        // The stored message is a secret-free template: resolve secrets back
        // in from the SecretProvider, then fill the token slots — the AzDO
        // path has no App-mint, so the PAT (or the job-scoped runtime token
        // for fork-restricted tiers) is the credential. Never written back.
        let filled = crate::message_template::fill_template(
            &mut msg,
            shared.state.secret_provider.as_ref(),
            &ctx.repository,
            ctx.request.run_id,
        )
        .map_err(|error| ApiError::internal(format!("failed to fill job message: {error}")))?;
        // Merge the freshly resolved values (env-tier secrets included) into
        // the node masker entry seeded at submit.
        if !filled.values.is_empty() {
            let plan_id = msg.plan.plan_id.clone();
            let mut inner = shared.state.inner.lock().await;
            let mut merged: Vec<String> = inner
                .plan_secret_masker
                .get(&plan_id)
                .map(|v| (**v).clone())
                .unwrap_or_default();
            merged.extend(filled.values.values().cloned());
            merged.sort();
            merged.dedup();
            inner.plan_secret_masker.insert(plan_id, Arc::new(merged));
        }
        let tier = ctx.trust_tier.as_deref().and_then(|tier| {
            serde_json::from_value(serde_json::Value::String(tier.to_owned())).ok()
        });
        let fork_restricted = crate::events::trust_tier::job_authorization(
            tier,
            None,
            ctx.id_token_granted.unwrap_or(false),
        )
        .fork_restricted;
        let runtime = shared
            .state
            .mint_runtime_token(&msg.plan.plan_id, &msg.job_id);
        let token = if fork_restricted {
            runtime
        } else {
            // The PAT is embedded only when its OAuth scopes were verified
            // (cached by the submit-time introspection); unverifiable
            // authority stays withheld and the job keeps the runtime token.
            match shared.state.static_github_pat() {
                Some(pat) => match crate::runs::cached_pat_scopes(&pat) {
                    Some(scopes) => {
                        msg.variables.insert(
                            "system.github.token.pat_scopes".to_owned(),
                            preloop_gha_protocol::azdo::VariableValue::new(
                                crate::runs::pat_scopes_wire_value(&scopes),
                            ),
                        );
                        pat
                    }
                    None => {
                        msg.variables.insert(
                            "system.github.token.pat_scopes".to_owned(),
                            preloop_gha_protocol::azdo::VariableValue::new(
                                crate::runs::PAT_WITHHELD_WIRE_VALUE,
                            ),
                        );
                        runtime
                    }
                },
                None => runtime,
            }
        };
        crate::broker::apply_minted_token_to_message(
            &mut msg,
            &crate::broker::MintedGitHubToken {
                token,
                effective_permissions: None,
            },
            false,
        );
        // The pinned snapshot checkout token and the origin-rewrite header
        // are minted at submission and blanked in the stored template; this
        // path delivers the message directly, so it must re-mint both from a
        // fresh job-scoped token exactly like `broker_acquire_job`.
        crate::broker::re_mint_snapshot_credentials(&mut msg, &shared.state);
        // inject SystemVssConnection so the worker's AzDO reporting
        // context has a server URL, access token, and ResultsServiceUrl —
        // same as broker_acquire_job.
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
        serde_json::to_string(&msg)
            .map_err(|error| ApiError::internal(format!("failed to encode job message: {error}")))?
    } else {
        message.body.clone().unwrap_or_default()
    };

    // Keyed sessions encrypt with the derived key; compat/default sessions
    // (no key exchange) stay base64-plaintext so the runner can decode them.
    let (encrypted_body, iv) = if message.plaintext {
        (body_json.into_bytes(), vec![0u8; 16])
    } else {
        let session_enc = shared.state.session_encryption(session_id);
        if session_enc.key.is_empty() {
            (body_json.into_bytes(), vec![0u8; 16])
        } else {
            session_enc.encrypt(body_json.as_bytes()).map_err(|error| {
                ApiError::internal(format!("failed to encrypt job message: {error}"))
            })?
        }
    };
    Ok(azdo::TaskAgentMessage {
        message_id: message.message_id,
        message_type: message.message_type,
        body: BASE64_STANDARD.encode(&encrypted_body),
        iv: Some(BASE64_STANDARD.encode(&iv)),
    })
}

pub async fn delete_session_message(
    State(shared): State<Arc<SharedState>>,
    Path((session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, &session_id, message_id).await
}

pub async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    if let Some(runner_id) = identity.and_then(|axum::Extension(id)| id.runner_id) {
        let owns = shared
            .state
            .backend
            .session_owner(session_id)
            .await
            .map(|owner| owner.is_some_and(|(id, _)| id == runner_id))
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
    let _ = shared
        .state
        .backend
        .delete_inflight(session_id, message_id)
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
    let target = shared
        .state
        .backend
        .request(crate::control::backend::RequestKey::Job(
            path.0,
            JobId(path.1.clone()),
        ))
        .await
        .ok();
    crate::auth::authorize_reporting_request(&shared.state, &headers, target.as_ref())?;
    complete_job_compat(State(shared), Path(path), Json(body)).await
}

/// `Some(owned)` when the request exists — `owned` = the runner owns it via
/// the recorded owner or the owner session's runner, or the request is
/// session-bound but unowned (replay-compat). `Err(Forbidden)` when an owner
/// exists and differs, and `Ok(Some(false))` for a request that was never
/// assigned: a released attempt (ready again, no owner and no session) must
/// not be readable or settleable by any registered runner.
async fn agent_request_owned_by(
    backend: &crate::control::backend::Backend,
    request_id: i64,
    runner_id: i64,
) -> Result<Option<bool>, crate::control::ControlError> {
    let Some((owner, session_runner, session_bound)) = backend.request_owner(request_id).await?
    else {
        return Ok(None);
    };
    match owner.or(session_runner) {
        Some(owner) if owner != runner_id => Err(crate::control::ControlError::Forbidden(
            "agent request belongs to another runner".to_owned(),
        )),
        Some(_) => Ok(Some(true)),
        None if session_bound => Ok(Some(true)),
        None => Ok(Some(false)),
    }
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
        .request(crate::control::backend::RequestKey::Id(request_id))
        .await
        .map_err(ApiError::from)?;
    if let Some(runner_id) = runner_id
        && let Some(false) = agent_request_owned_by(&shared.state.backend, request_id, runner_id)
            .await
            .map_err(ApiError::from)?
    {
        return Err(ApiError::from(crate::control::ControlError::Forbidden(
            "agent request belongs to another runner".to_owned(),
        )));
    }
    Ok(Json(agent_request_json(pool_id, &request)))
}

/// POST /_apis/v1/AgentRequest/:pool_id/:request_id — best-effort request ack.
pub async fn agent_request_ack(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, request_id)): Path<(i64, i64)>,
    identity: Option<axum::Extension<RunnerIdentity>>,
) -> Result<StatusCode, ApiError> {
    if let Some(runner_id) = identity.and_then(|axum::Extension(id)| id.runner_id)
        && let Some(false) = agent_request_owned_by(&shared.state.backend, request_id, runner_id)
            .await
            .map_err(ApiError::from)?
    {
        return Err(ApiError::from(crate::control::ControlError::Forbidden(
            "agent request belongs to another runner".to_owned(),
        )));
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
        if let Some(runner_id) = verified_runner_id
            && let Some(false) =
                agent_request_owned_by(&shared.state.backend, request_id, runner_id)
                    .await
                    .map_err(ApiError::from)?
        {
            return Err(ApiError::from(crate::control::ControlError::Forbidden(
                "agent request belongs to another runner".to_owned(),
            )));
        }
        // One guarded UPDATE settles a live request; `None` means the row
        // was already completed (duplicate PATCH) or never existed.
        let settled = shared
            .state
            .backend
            .settle_agent_request(request_id, new_status, &agent_request_locked_until())
            .await
            .map_err(ApiError::from)?;
        if settled.is_none() {
            info!(
                request_id,
                result = %result_hint,
                "agent request already completed or unknown; ignoring result"
            );
        }
        let completion = settled.map(|(run_id, job_id, agent_job_id)| {
            info!(
                %run_id,
                %job_id,
                result = %result_hint,
                "job completed via agent_request_patch"
            );
            JobCompletion {
                run_id,
                job_id,
                // The patched request is the attempt that reported.
                agent_job_id: Some(agent_job_id),
                status: new_status,
                outputs: Default::default(),
                annotations: Vec::new(),
                step_results: Vec::new(),
            }
        });
        if let Some(c) = completion {
            let _ = complete_job_inner(shared.clone(), c).await;
        }
        return Ok(Json(
            agent_request_response(&shared, pool_id, request_id).await,
        ));
    }
    // Renewal — runner is still working; just extend the lock. Completed
    // requests are immutable, so a late duplicate cannot rewrite their
    // timing: the guarded UPDATE skips settled/unknown requests.
    if let Some(runner_id) = verified_runner_id
        && let Some(false) = agent_request_owned_by(&shared.state.backend, request_id, runner_id)
            .await
            .map_err(ApiError::from)?
    {
        return Err(ApiError::from(crate::control::ControlError::Forbidden(
            "agent request belongs to another runner".to_owned(),
        )));
    }
    shared
        .state
        .backend
        .renew_agent_request(request_id, &agent_request_locked_until())
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
        .request(crate::control::backend::RequestKey::Id(request_id))
        .await
        .map(|request| agent_request_json(pool_id, &request))
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

/// How stale a job lease may grow once its session is no longer live before
/// the reaper fails the attempt. Live means the session row exists and has
/// polled within the runner liveness timeout
/// (`PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS`, default 30 min): a machine that
/// just died still counts as live, so its attempt fails at
/// [`HUNG_WORKER_LEASE_SECONDS`]. This bound covers attempts whose session
/// was closed, purged, or silent past that timeout — ten minutes at most,
/// where waiting out [`JOB_LEASE_SECONDS`] (still the lock the runner is
/// told about, and what its renew loop honours) held the job for 45.
pub const DEAD_SESSION_LEASE_SECONDS: u64 = 600;

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

/// Mask job-completion annotations before they are persisted or returned.
/// Crash annotations (the official runner's worker-crash detail from
/// `ForceFailJob`) embed worker stdout/stderr, which can contain secret
/// values; the raw `JobCompletion` is the protocol boundary and is not safe
/// to store or return as-is. Values come from the SecretProvider: every
/// stored tier (over-masking another repository's value is harmless) plus
/// this run's submission-supplied tier.
pub(crate) fn mask_completion_annotations(
    shared: &SharedState,
    completion: &JobCompletion,
) -> Result<Vec<serde_json::Value>, ApiError> {
    let provider = shared.state.secret_provider.as_ref();
    let provider_error = |error: anyhow::Error| {
        ApiError::internal(format!(
            "secret provider `{}` failed: {error}",
            provider.name()
        ))
    };
    let mut values = provider.resolve_all().map_err(provider_error)?;
    values.extend(preloop_gha_protocol::masking::expose_values(
        provider
            .run_tier(completion.run_id)
            .map_err(provider_error)?
            .values(),
    ));
    Ok(preloop_gha_protocol::mask_annotations(
        completion.annotations.clone(),
        values.iter().map(String::as_str),
    ))
}

/// Map a `completejob` stepResult's status + conclusion to the run record's
/// step-conclusion string, when the step is terminally reported.
///
/// Status is the official TimelineRecordState (`completed` or 2); only a
/// terminal status makes the conclusion authoritative — in-progress/pending
/// steps stay for the reconciliation pass. Conclusion is the official
/// TaskResult (`succeeded`/`succeededwithissues`/`failed`/`canceled`/
/// `skipped`/`abandoned`, or the numeric 0..5 forms).
pub(crate) fn completion_step_conclusion(
    wire: &preloop_gha_protocol::CompletionStepResult,
) -> Option<String> {
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
    complete_job_settling(shared, completion, None).await
}

/// A runner's own completion report: settle the attempt it owns inside the
/// completion transaction instead of a separate one.
pub(crate) use crate::control::types::AttemptSettle;

/// Complete a job. With `settle`, the reporting runner's attempt is verified,
/// marked finished and released from its session in the same transaction; a
/// duplicate or already-released report returns the run unchanged.
///
/// The whole scheduling transition — status flip, concurrency release,
/// dependent promotion, workflow-group widen — lives in
/// `ControlBackend::settle_job` (one targeted transaction per attempt, widening
/// internally when a completion finishes a workflow-gated run). This handler
/// only runs the post-commit side-effects the backend deliberately does not
/// own: live-log close, pool-wake gauges, deferred expansion, check-run and
/// event fan-out, terminal workspace cleanup.
pub(crate) async fn complete_job_settling(
    shared: Arc<SharedState>,
    mut completion: JobCompletion,
    settle: Option<AttemptSettle>,
) -> Result<Json<RunRecord>, ApiError> {
    if !completion.status.is_terminal() {
        return Err(ApiError::bad_request(
            "job completion status must be terminal",
        ));
    }
    completion.annotations = mask_completion_annotations(&shared, &completion)?;
    let outcome = shared
        .state
        .backend
        .settle_job(crate::control::types::SettleJob {
            completion: completion.clone(),
            settle,
        })
        .await
        .map_err(ApiError::from)?;

    let settled = match outcome {
        // Duplicate / already-released attempt, or a job already holding a
        // terminal verdict: report the current run unchanged.
        crate::control::types::SettleJobOutcome::Unchanged(run) => {
            return Ok(Json(*run));
        }
        crate::control::types::SettleJobOutcome::Settled(settled) => *settled,
    };
    let crate::control::types::JobSettled {
        effective_status,
        cancelled_siblings,
        mut scheduling,
        queue_nonempty,
        newly_terminal_success,
        live_log_key,
        queue_len: tx_queue_len,
        next_runs_on: tx_next_labels,
    } = settled;

    // Node-local bookkeeping that does not belong to the scheduling tx:
    // close the live-log feed and drop the run's DAP port registration.
    {
        let mut inner = shared.state.inner.lock().await;
        crate::live_logs::close_live_log(&mut inner, &live_log_key);
        inner.dap_ports.remove(&completion.run_id);
    }
    // Refresh the on-demand pool wake atomic and the next-job labels from the
    // committed scheduling state.
    let (queue_len, next_labels) = (tx_queue_len, tx_next_labels);
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
    let record = shared
        .state
        .backend
        .run_record(completion.run_id)
        .await
        .map_err(ApiError::from)?;

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

    // Cancelled siblings must reach their runners (broadcast); promotions
    // wake only as many waiters as jobs became claimable.
    if scheduling.promoted > 0 || !cancelled_siblings.is_empty() || queue_nonempty {
        crate::state::wake_waiters(
            &shared.state.message_notify,
            scheduling.promoted.max(usize::from(queue_nonempty)),
            !cancelled_siblings.is_empty(),
        );
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
        let released = shared
            .state
            .backend
            .run_record(completion.run_id)
            .await
            .ok()
            .and_then(|run| run.workspace_snapshot.clone());
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
            .active_plan_ids()
            .await
            .unwrap_or_default();
        let log_segments = shared.state.log_segments.clone();
        let plans_clone = active_plans.clone();
        tokio::spawn(async move {
            prune_replay_results(&state_dir, &active_plans).await;
            if let Err(error) = log_segments.prune_inactive_plans(&plans_clone).await {
                tracing::warn!(%error, "failed to prune live-log segments");
            }
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
