use super::*;

// Timeline, logs, completion

/// Whether a timeline record describes a step rather than a container.
///
/// Shared by the annotation projection and the manifest reconciliation below,
/// which disagreed: one keyed on `Step`-or-parent and the other whitelisted
/// `Task`, so a typed `Task` with no parent was stored as a step while its
/// issues were reported against the job.
///
/// Excluding containers rather than whitelisting a step type is deliberate.
/// `type` is optional on the wire and the official runner sends `Task` where
/// preloop's sends `Step`, so an allow-list silently drops real conclusions.
fn is_step_record(record: &azdo::TimelineRecord) -> bool {
    match record.record_type {
        Some(azdo::TimelineRecordType::Task | azdo::TimelineRecordType::Step) => true,
        Some(
            azdo::TimelineRecordType::Job
            | azdo::TimelineRecordType::Phase
            | azdo::TimelineRecordType::Stage,
        ) => false,
        // Untyped: a step always hangs off its job record.
        None => record.parent_id.is_some(),
    }
}

/// PATCH timeline records — runner updates step/job state.
pub async fn patch_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    let records = wrapper.value;
    let timeline_key = format!("{}/{}", plan_id, timeline_id);
    // Resolve the callback (plan/timeline → attempt → run/job + current job
    // status) with one indexed query; node-local `timeline_*` state is read
    // under `inner` below.
    let callback = shared
        .state
        .backend
        .callback_job(&plan_id, timeline_id.parse().ok())
        .await
        .unwrap_or(None);
    let run_id = callback
        .as_ref()
        .map(|cb| cb.run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let logical_job_id = callback.as_ref().map(|cb| cb.job_id.clone());
    let agent_job_id = callback.as_ref().map(|cb| cb.agent_job_id);
    let job_status_for_run = callback.as_ref().and_then(|cb| cb.job_status);
    let mut projected = Vec::new();
    for record in &records {
        if let Some(state) = &record.state {
            info!(
                timeline_id = %timeline_id,
                record_id = %record.id,
                name = record.display_name.as_deref().unwrap_or(""),
                state = ?state,
                "timeline record update"
            );
        }
        if let (Some(run_id), Some(status)) = (run_id, timeline_status(record)) {
            projected.push(NdjsonEvent::JobStatus {
                run_id,
                job_id: logical_job_id
                    .clone()
                    .unwrap_or_else(|| JobId(record.id.to_string())),
                status,
                reason: None,
            });
        }
        if let Some(run_id) = run_id {
            for issue in &record.issues {
                let step_id = is_step_record(record).then(|| record.id.to_string());
                projected.push(NdjsonEvent::Annotation {
                    run_id,
                    job_id: logical_job_id
                        .clone()
                        .unwrap_or_else(|| JobId(record.id.to_string())),
                    level: issue_level(issue.issue_type),
                    message: issue.message.clone().unwrap_or_default(),
                    file: issue.data.get("file").cloned(),
                    line: issue.data.get("line").and_then(|line| line.parse().ok()),
                    end_line: issue
                        .data
                        .get("endLine")
                        .or_else(|| issue.data.get("endline"))
                        .and_then(|line| line.parse().ok()),
                    col: issue
                        .data
                        .get("col")
                        .or_else(|| issue.data.get("startColumn"))
                        .and_then(|column| column.parse().ok()),
                    end_column: issue
                        .data
                        .get("endColumn")
                        .or_else(|| issue.data.get("endcolumn"))
                        .and_then(|column| column.parse().ok()),
                    title: issue.data.get("title").cloned(),
                    step_id,
                });
            }
        }
    }

    // Node-local: merge the projected events into the per-run feed.
    {
        let mut inner = shared.state.inner.lock().await;
        let events = inner
            .timeline_events
            .entry(run_id.unwrap_or_else(|| RunId(uuid::Uuid::nil())))
            .or_default();
        for event in &projected {
            if !events.contains(event) {
                events.push(event.clone());
            }
        }
        trim_timeline_events(
            &mut inner,
            run_id.unwrap_or_else(|| RunId(uuid::Uuid::nil())),
        );
    }

    // Backend: reconcile the attempt's step rows and, when needed, the run's
    // `jobs_list` detail. Steps are direct keyed upserts (no run lock); the
    // run transaction runs only to create the job's detail entry or record a
    // non-running conclusion on it.
    if let (Some(run_id), Some(job_id)) = (run_id, logical_job_id.clone()) {
        let job_status = job_status_for_run;
        let observed_us = chrono::Utc::now().timestamp_micros();
        let patches: Vec<crate::control::types::StepPatch> = records
            .iter()
            .filter_map(|record| step_patch(record, job_status, observed_us))
            .collect();
        let needs_detail = job_status.is_some_and(|s| s != ExecutionStatus::InProgress)
            || shared
                .state
                .backend
                .job_detail_missing(run_id, &job_id)
                .await
                .unwrap_or(true);
        if needs_detail {
            shared
                .state
                .backend
                .transact_scoped(&crate::control::txstate::TxScope::run(run_id), move |tx| {
                    if let Some(run) = tx.runs.get_mut(&run_id) {
                        let detail = match JobDetail::find(&mut run.jobs_list, &job_id.0) {
                            Some(detail) => detail,
                            None => {
                                run.jobs_list.push(JobDetail {
                                    job_id: job_id.0.clone(),
                                    name: job_id.0.clone(),
                                    // A timeline update means the job started;
                                    // the run record's final conclusion comes
                                    // from the job status map. Default to the
                                    // truthful in-flight state.
                                    conclusion: "in_progress".to_owned(),
                                    steps: Vec::new(),
                                    annotations: Vec::new(),
                                });
                                run.jobs_list.last_mut().expect("just pushed")
                            }
                        };
                        // The status map is authoritative for terminal states
                        // only; an in-flight projection would lie.
                        if let Some(status) = job_status {
                            if status != ExecutionStatus::InProgress {
                                detail.conclusion = format!("{:?}", status).to_lowercase();
                            }
                        }
                    }
                    if let Some(agent_job_id) = agent_job_id {
                        let manifest = tx.job_steps.entry(agent_job_id).or_default();
                        for patch in &patches {
                            apply_step_patch(manifest, patch);
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(crate::ApiError::from)
                .unwrap_or(());
        } else if let Some(agent_job_id) = agent_job_id {
            if let Err(error) = shared
                .state
                .backend
                .patch_steps(agent_job_id, patches)
                .await
            {
                warn!(?error, "failed to persist timeline steps");
            }
        }
    }
    for event in projected {
        shared.state.emit(event).await;
    }
    // Persist the records (one shared row each; the change id is bumped in
    // the same transaction) and return the full stored set. No node-local
    // copy: a PATCH on one node and a GET on another must agree.
    match shared
        .state
        .backend
        .patch_timeline(&timeline_key, records)
        .await
    {
        Ok((_change_id, stored)) => Json(json!({ "count": stored.len(), "value": stored })),
        Err(error) => {
            warn!(?error, "failed to persist timeline records");
            Json(json!({ "count": 0, "value": [] }))
        }
    }
}
pub fn timeline_status(record: &azdo::TimelineRecord) -> Option<ExecutionStatus> {
    match record.result {
        Some(azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues) => {
            Some(ExecutionStatus::Success)
        }
        Some(azdo::TaskResult::Failed) => Some(ExecutionStatus::Failure),
        Some(azdo::TaskResult::Cancelled) => Some(ExecutionStatus::Cancelled),
        Some(azdo::TaskResult::Skipped) => Some(ExecutionStatus::Skipped),
        Some(azdo::TaskResult::Abandoned) => Some(ExecutionStatus::Failure),
        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
            Some(ExecutionStatus::InProgress)
        }
        _ => None,
    }
}

pub fn issue_level(issue_type: azdo::IssueType) -> AnnotationLevel {
    match issue_type {
        azdo::IssueType::Error => AnnotationLevel::Error,
        azdo::IssueType::Warning => AnnotationLevel::Warning,
        azdo::IssueType::Info => AnnotationLevel::Notice,
    }
}

/// POST create log file — runner creates a log container.
pub async fn create_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(mut log): Json<azdo::TaskLog>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let next_id = shared
        .state
        .backend
        .create_log(&plan_id)
        .await
        .map_err(ApiError::from)?;
    log.id = next_id;
    let key = format!("{plan_id}/{next_id}");
    let evicted = {
        let mut inner = shared.state.inner.lock().await;
        inner.logs.entry(key.clone()).or_default();
        inner.log_metadata.entry(key.clone()).or_default();
        if !inner.log_order.iter().any(|existing| existing == &key) {
            inner.log_order.push_back(key);
        }
        trim_plan_logs(&mut inner, &plan_id)
    };
    // Eviction bounds the node-local preview only. Published segments remain
    // available until retained-plan cleanup or final-log publication.
    let _ = evicted;
    Ok(Json(
        serde_json::to_value(&log).unwrap_or(json!({ "ok": true })),
    ))
}

/// POST append log — runner appends lines to a log file.
pub async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    // Hot path: mutate and capture the chunk under the lock, then persist
    // after releasing it.
    // Mask the body against the run's secrets. Run secrets are immutable, so
    // the resolved masker is cached node-locally per plan_id — the hot append
    // path does not touch the backend after the first chunk. On a backend
    // error we MUST NOT store the raw body — drop the append rather than
    // persist unmasked secrets.
    let masked = match mask_log_bytes_cached(&shared, &plan_id, &body).await {
        Ok(masked) => masked,
        Err(error) => {
            warn!(?error, key = %key, "failed to mask log body; dropping append");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    if let Err(error) = shared
        .state
        .log_segments
        .append(&plan_id, &log_id, &masked)
        .await
    {
        warn!(?error, key = %key, "failed to buffer/publish live log");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    let evicted = {
        let mut inner = shared.state.inner.lock().await;
        let is_new = !inner.logs.contains_key(&key);
        let byte_count = masked.len();
        let line_count = masked.iter().filter(|&&b| b == b'\n').count();
        inner
            .logs
            .entry(key.clone())
            .or_default()
            .extend_from_slice(&masked);
        inner.log_bytes_total = inner.log_bytes_total.saturating_add(byte_count);
        if is_new && !inner.log_order.iter().any(|k| k == &key) {
            inner.log_order.push_back(key.clone());
        }
        // Keep only the newest bytes in the node-local preview. Published
        // segments and the runner's complete uploaded logs are independent.
        if let Some(retained) = inner.logs.get_mut(&key) {
            // Only trim once the buffer grows a full slack window past the cap,
            // then drop back down to the cap — amortizes the O(n) front-shift
            // to O(1) per byte instead of shifting on every append.
            if retained.len() > MAX_LOG_BYTES_PER_KEY + LOG_KEY_TRIM_SLACK {
                let excess = retained.len() - MAX_LOG_BYTES_PER_KEY;
                retained.drain(0..excess);
                inner.log_bytes_total = inner.log_bytes_total.saturating_sub(excess);
            }
        }
        // Update metadata before trimming so the newest log isn't miscounted,
        // but release the borrow before calling `trim_plan_logs`.
        {
            let meta = inner.log_metadata.entry(key.clone()).or_default();
            meta.byte_count += byte_count;
            meta.line_count += line_count;
        }
        trim_plan_logs(&mut inner, &plan_id)
    };
    // Evict only the in-memory preview; the file-backed segments are
    // independent and remain readable after a restart.
    let _ = evicted;
    StatusCode::ACCEPTED
}

pub fn log_key(plan_id: &str, log_id: &str) -> String {
    format!("{plan_id}/{log_id}")
}

/// Mask `body` against the run's secrets, resolving and caching the masker
/// node-locally per `plan_id`. Run secrets are immutable for the life of the
/// run, so after the first append for a plan the hot path never touches the
/// backend.
///
/// A cache miss does one scoped read (`job_requests` to resolve the run,
/// `runs` for its secrets — no queues, sessions, or concurrency families).
/// The resolved masker is cached permanently. An *unresolved* plan (log chunk
/// arrived before the run row exists) masks against every run's secrets but
/// is NOT cached — caching the fallback would permanently mask against a set
/// that lacks this run's secrets and leak them into the log. Instead a short
/// negative-cache TTL bounds how often the unresolved plan re-probes the
/// backend.
pub async fn mask_log_bytes_cached(
    shared: &Arc<SharedState>,
    plan_id: &str,
    body: &[u8],
) -> Result<Vec<u8>, crate::control::ControlError> {
    /// How long an unresolved plan_id waits before re-probing the backend for
    /// its run's secrets. Short enough that a run registered mid-stream picks
    /// up its real masker quickly; long enough to keep a chunk-per-append
    /// storm from hammering the reader pool.
    const MASKER_NEG_CACHE_TTL: std::time::Duration = std::time::Duration::from_millis(250);

    // Fast path: masker already resolved for this plan, or a cached union
    // fallback still within its re-probe window.
    {
        let inner = shared.state.inner.lock().await;
        let cached: Option<Arc<Vec<String>>> =
            inner.plan_secret_masker.get(plan_id).cloned().or_else(|| {
                inner
                    .plan_secret_masker_pending
                    .get(plan_id)
                    .and_then(|(secrets, deadline)| {
                        (std::time::Instant::now() < *deadline).then(|| secrets.clone())
                    })
            });
        if let Some(secrets) = cached {
            drop(inner);
            let text = String::from_utf8_lossy(body);
            return Ok(preloop_gha_protocol::masking::mask_secrets(
                &text,
                secrets.iter().map(String::as_str),
                &[],
            )
            .into_bytes());
        }
    }

    // Slow path: resolve plan_id → run_id → secrets under a narrow scope.
    // Returns (secrets, resolved) — `resolved` is true only when the plan
    // mapped to a concrete run row, so the fallback union is never cached as
    // if it were the run's real masker.
    let plan_id_owned = plan_id.to_owned();
    let (secrets, resolved) = shared
        .state
        .backend
        .read_scoped(
            &crate::control::txstate::TxScope::request_correlation(),
            move |tx| {
                let resolved_run_id = resolve_callback_job(tx, &plan_id_owned, None, None)
                    .map(|(_, run_id, _)| run_id)
                    .or_else(|| plan_id_owned.parse::<RunId>().ok());
                match resolved_run_id.and_then(|run_id| tx.runs.get(&run_id)) {
                    Some(run) => Ok((
                        preloop_gha_protocol::masking::expose_values(
                            run.submission.secrets.values(),
                        ),
                        true,
                    )),
                    None => Ok((
                        preloop_gha_protocol::masking::expose_values(
                            tx.runs
                                .values()
                                .flat_map(|run| run.submission.secrets.values()),
                        ),
                        false,
                    )),
                }
            },
        )
        .await?;

    {
        let mut inner = shared.state.inner.lock().await;
        let secrets = Arc::new(secrets);
        if resolved {
            // Permanent cache: run secrets are immutable for the run's life.
            inner
                .plan_secret_masker
                .insert(plan_id.to_owned(), secrets.clone());
            inner.plan_secret_masker_pending.remove(plan_id);
        } else {
            // Negative cache: mask against this union and re-probe after the
            // TTL, not on every chunk.
            inner.plan_secret_masker_pending.insert(
                plan_id.to_owned(),
                (
                    secrets.clone(),
                    std::time::Instant::now() + MASKER_NEG_CACHE_TTL,
                ),
            );
        }
        let text = String::from_utf8_lossy(body);
        Ok(preloop_gha_protocol::masking::mask_secrets(
            &text,
            secrets.iter().map(String::as_str),
            &[],
        )
        .into_bytes())
    }
}

/// POST console log — runner streams live console output.
pub async fn console_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, _timeline_id, _record_id)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    body: Bytes,
) -> StatusCode {
    // Resolve the callback to the run-scoped agent-job key. Falling back to
    // the plan id preserves compatibility for callbacks that arrive before a
    // request record exists.
    if let Ok(wrapper) = serde_json::from_slice::<LiveLogFeedLinesWrapper>(&body) {
        // Narrow scope: `job_requests` (for the plan→run/job lookup) only —
        // no queues, sessions, or concurrency families on this hot path.
        let resolved = shared
            .state
            .backend
            .read_scoped(&crate::control::txstate::TxScope::requests_only(), {
                let plan_id = plan_id.clone();
                move |tx| {
                    Ok(resolve_callback_job(tx, &plan_id, None, None)
                        .map(|(_, run_id, job_id)| (run_id, job_id.0)))
                }
            })
            .await
            .unwrap_or(None);
        match resolved {
            Some((run_id, job_id)) => {
                crate::live_logs::record_live_log_wrapper_for_run(
                    &shared, run_id, &job_id, wrapper,
                )
                .await;
            }
            None => crate::live_logs::record_live_log_wrapper(&shared, &plan_id, wrapper).await,
        }
    }
    StatusCode::OK
}

/// POST finish job — runner reports final result + outputs.
pub async fn finish_job(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Json<serde_json::Value> {
    let status = task_result_status(event.result);
    let outputs = event
        .outputs
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect();
    let completion = {
        let plan_id = plan_id.clone();
        let event = event.clone();
        shared
            .state
            .backend
            .transact(move |tx| {
                let callback_resolved =
                    resolve_callback_job(tx, &plan_id, Some(event.timeline_id), Some(event.job_id));
                let active_resolved =
                    sole_active_unfinished_request(tx).and_then(|id| job_request_tuple(tx, id));
                let resolved = callback_resolved.or(active_resolved).or_else(|| {
                    plan_id
                        .parse::<RunId>()
                        .ok()
                        .map(|run_id| (0, run_id, JobId(event.job_id.to_string())))
                });
                if let Some((request_id, run_id, job_id)) = resolved {
                    if let Some(request) = tx.job_requests.get_mut(&request_id) {
                        request.result = Some(status);
                        request.locked_until = agent_request_locked_until();
                    }
                    Ok(Some(JobCompletion {
                        run_id,
                        job_id,
                        // Resolved from the callback's own request record.
                        agent_job_id: tx
                            .job_requests
                            .get(&request_id)
                            .map(|record| record.agent_job_id),
                        status,
                        outputs,
                        annotations: Vec::new(),
                        step_results: Vec::new(),
                    }))
                } else {
                    Ok(None)
                }
            })
            .await
            .map_err(crate::ApiError::from)
            .unwrap_or(None)
    };

    info!(
        job_id = %event.job_id,
        result = ?event.result,
        outputs = ?event.outputs,
        "job completed"
    );

    if let Some(completion) = completion {
        let _ = complete_job_inner(shared, completion).await;
    } else {
        warn!(
            plan_id,
            job_id = %event.job_id,
            timeline_id = %event.timeline_id,
            "finish_job could not resolve callback to a run/job"
        );
    }

    Json(serde_json::Value::Null)
}

// ── F030: standard AzDO `/_apis/v1/plans/` route handlers ────────────────────
// These use the URL pattern our AzDO client sends (`plans/{planId}/...`) rather
// than the scoped pattern (`Timeline/{scope}/{hub}/{planId}/{timelineId}`).
// The logic is identical to the existing handlers above.

/// PATCH `/_apis/v1/plans/:plan_id/timelines/:timeline_id/records`
pub async fn patch_timeline_records_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    patch_timeline_records(
        State(shared),
        Path((String::new(), String::new(), plan_id, timeline_id)),
        Json(wrapper),
    )
    .await
}

/// F6 — pagination controls for timeline GET. `top` is clamped to
/// [`MAX_TOP_RECORDS`] server-side; `skip` pages further.
#[derive(Debug, Deserialize)]
pub struct TimelineQuery {
    #[serde(default)]
    pub top: Option<usize>,
    #[serde(default)]
    pub skip: Option<usize>,
}

/// GET `/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id` — read back the timeline.
pub async fn get_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Query(query): Query<TimelineQuery>,
) -> Json<serde_json::Value> {
    let timeline_key = format!("{}/{}", plan_id, timeline_id);
    // When `top` is absent the official runner expects the full timeline
    // (it does not paginate); storage is capped at MAX_TIMELINE_RECORDS, so
    // returning all is bounded. When `top` is present we clamp it.
    let (top, skip) = match query.top {
        Some(t) => (t.min(MAX_TOP_RECORDS), query.skip.unwrap_or(0)),
        None => (usize::MAX, query.skip.unwrap_or(0)),
    };
    let (change_id, records) = shared
        .state
        .backend
        .get_timeline(&timeline_key, skip, top)
        .await
        .unwrap_or_else(|error| {
            warn!(?error, "failed to read timeline records");
            (0, Vec::new())
        });
    Json(json!({
        "id": timeline_id,
        "changeId": change_id,
        "lastChangedBy": uuid::Uuid::nil(),
        "lastChangedOn": "0001-01-01T00:00:00",
        "records": records
    }))
}

/// GET `/_apis/v1/plans/:plan_id/timelines/:timeline_id/records`
pub async fn get_timeline_records_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    Query(query): Query<TimelineQuery>,
) -> Json<serde_json::Value> {
    get_timeline_records(
        State(shared),
        Path((String::new(), String::new(), plan_id, timeline_id)),
        Query(query),
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/logs`
pub async fn create_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(log): Json<azdo::TaskLog>,
) -> Result<Json<serde_json::Value>, ApiError> {
    create_log(
        State(shared),
        Path((String::new(), String::new(), plan_id)),
        Json(log),
    )
    .await
}

/// PUT `/_apis/v1/plans/:plan_id/logs/:log_id`
pub async fn append_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, log_id)): Path<(String, String)>,
    body: Bytes,
) -> StatusCode {
    append_log(
        State(shared),
        Path((String::new(), String::new(), plan_id, log_id)),
        body,
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/events`
///
/// Handles the `JobCompleted` event sent by the runner's AzDO reporting path.
/// The body shape is `{name, jobId, requestId, result, outputs}` — slightly
/// different from the scoped `finish_job` path which uses `JobCompletedEvent`.
pub async fn finish_job_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(event): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let result_str = event
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("failed");
    let status =
        execution_status_from_runner_result(result_str).unwrap_or(ExecutionStatus::Failure);
    let job_id_str = event.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
    let outputs = event
        .get("outputs")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();
    let outputs: preloop_gha_protocol::OutputMap = outputs
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    info!(
        plan_id,
        job_id = job_id_str,
        result = result_str,
        "finish_job_plan"
    );

    let completion = shared
        .state
        .backend
        .transact(move |tx| {
            let resolved = resolve_callback_job(tx, &plan_id, None, None).or_else(|| {
                sole_active_unfinished_request(tx).and_then(|id| job_request_tuple(tx, id))
            });
            if let Some((request_id, run_id, job_id)) = resolved {
                if let Some(request) = tx.job_requests.get_mut(&request_id) {
                    request.result = Some(status);
                    request.locked_until = agent_request_locked_until();
                }
                Ok(Some(JobCompletion {
                    run_id,
                    job_id,
                    // Resolved from the callback's own request record.
                    agent_job_id: tx
                        .job_requests
                        .get(&request_id)
                        .map(|record| record.agent_job_id),
                    status,
                    outputs,
                    annotations: Vec::new(),
                    step_results: Vec::new(),
                }))
            } else {
                warn!(plan_id, "finish_job_plan: could not resolve run/job");
                Ok(None)
            }
        })
        .await
        .map_err(crate::ApiError::from)
        .unwrap_or(None);
    if let Some(c) = completion {
        let _ = complete_job_inner(shared, c).await;
    }
    Json(serde_json::Value::Null)
}

pub async fn authorize_reporting_callback(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    plan_id: &str,
    timeline_id: Option<uuid::Uuid>,
    agent_job_id: Option<uuid::Uuid>,
) -> Result<(), ApiError> {
    let request = shared
        .state
        .backend
        .read_scoped(&crate::control::txstate::TxScope::requests_only(), {
            let plan_id = plan_id.to_owned();
            move |tx| {
                let request_id = agent_job_id
                    .and_then(|id| tx.agent_job_requests.get(&id).copied())
                    .or_else(|| timeline_id.and_then(|id| tx.timeline_requests.get(&id).copied()))
                    .or_else(|| tx.plan_requests.get(&plan_id).copied());
                Ok(request_id
                    .and_then(|id| tx.job_requests.get(&id).cloned())
                    .filter(|request| request.plan_id == plan_id))
            }
        })
        .await
        .map_err(ApiError::from)?;
    crate::auth::authorize_reporting_request(&shared.state, headers, request.as_ref())
}

pub async fn patch_timeline_records_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_id = path.3.parse().ok();
    authorize_reporting_callback(&shared, &headers, &path.2, timeline_id, None).await?;
    Ok(patch_timeline_records(State(shared), Path(path), Json(wrapper)).await)
}

pub async fn get_timeline_records_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_id = path.3.parse().ok();
    authorize_reporting_callback(&shared, &headers, &path.2, timeline_id, None).await?;
    Ok(get_timeline_records(State(shared), Path(path), Query(query)).await)
}

pub async fn create_log_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(log): Json<azdo::TaskLog>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_reporting_callback(&shared, &headers, &path.2, None, None).await?;
    create_log(State(shared), Path(path), Json(log)).await
}

pub async fn append_log_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_reporting_callback(&shared, &headers, &path.2, None, None).await?;
    Ok(append_log(State(shared), Path(path), body).await)
}

pub async fn console_log_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let timeline_id = path.3.parse().ok();
    authorize_reporting_callback(&shared, &headers, &path.2, timeline_id, None).await?;
    Ok(console_log(State(shared), Path(path), body).await)
}

pub async fn finish_job_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(path): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_reporting_callback(&shared, &headers, &path.2, None, Some(event.job_id)).await?;
    Ok(finish_job(State(shared), Path(path), Json(event)).await)
}

pub async fn finish_job_plan_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    headers: HeaderMap,
    Json(event): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let agent_job_id = event
        .get("jobId")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse().ok());
    authorize_reporting_callback(&shared, &headers, &plan_id, None, agent_job_id).await?;
    Ok(finish_job_plan(State(shared), Path(plan_id), Json(event)).await)
}

pub async fn patch_timeline_records_plan_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_uuid = timeline_id.parse().ok();
    authorize_reporting_callback(&shared, &headers, &plan_id, timeline_uuid, None).await?;
    Ok(
        patch_timeline_records_plan(State(shared), Path((plan_id, timeline_id)), Json(wrapper))
            .await,
    )
}

pub async fn get_timeline_records_plan_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let timeline_uuid = timeline_id.parse().ok();
    authorize_reporting_callback(&shared, &headers, &plan_id, timeline_uuid, None).await?;
    Ok(get_timeline_records_plan(State(shared), Path((plan_id, timeline_id)), Query(query)).await)
}

pub async fn create_log_plan_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    headers: HeaderMap,
    Json(log): Json<azdo::TaskLog>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_reporting_callback(&shared, &headers, &plan_id, None, None).await?;
    create_log_plan(State(shared), Path(plan_id), Json(log)).await
}

pub async fn append_log_plan_authenticated(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, log_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    authorize_reporting_callback(&shared, &headers, &plan_id, None, None).await?;
    Ok(append_log_plan(State(shared), Path((plan_id, log_id)), body).await)
}

/// The step update a timeline record carries, or `None` for records that are
/// not steps (runner bookkeeping, unnamed records).
fn step_patch(
    record: &azdo::TimelineRecord,
    job_status: Option<ExecutionStatus>,
    observed_us: i64,
) -> Option<crate::control::types::StepPatch> {
    let name = record.display_name.clone()?;
    if !is_step_record(record) {
        return None;
    }
    let conclusion = match record.result {
        Some(azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues) => "success",
        Some(azdo::TaskResult::Failed) => {
            if job_status == Some(ExecutionStatus::Cancelled) {
                "cancelled"
            } else {
                "failure"
            }
        }
        Some(azdo::TaskResult::Cancelled) => "cancelled",
        Some(azdo::TaskResult::Skipped) => "skipped",
        Some(azdo::TaskResult::Abandoned) => "failed",
        None if record.state == Some(azdo::TimelineRecordState::InProgress) => "in_progress",
        _ => "success",
    };
    let micros = |time: Option<&str>| {
        time.and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| t.timestamp_micros())
    };
    Some(crate::control::types::StepPatch {
        id: record.id.to_string(),
        name,
        conclusion: conclusion.to_owned(),
        started_at_us: micros(record.start_time.as_deref()),
        finished_at_us: micros(record.finish_time.as_deref()),
        observed_us,
    })
}

/// Apply a step patch to an in-memory manifest with the same rules the
/// direct upsert uses.
fn apply_step_patch(manifest: &mut Vec<StepRecord>, patch: &crate::control::types::StepPatch) {
    let time = |us: Option<i64>| us.and_then(chrono::DateTime::from_timestamp_micros);
    match StepRecord::find_by_id(manifest, &patch.id) {
        Some(pos) => {
            let step = &mut manifest[pos];
            step.conclusion = patch.conclusion.clone();
            if let Some(started) = time(patch.started_at_us) {
                step.started_at = Some(started);
            }
            if let Some(finished) = time(patch.finished_at_us) {
                step.finished_at = Some(finished);
            }
            step.name = patch.name.clone();
        }
        // A timeline record with no manifest entry is runner bookkeeping,
        // not a workflow step; `runner_number` stays unset on this path.
        None => manifest.push(StepRecord {
            id: patch.id.clone(),
            kind: StepKind::Synthetic,
            workflow_index: None,
            runner_number: None,
            context_name: None,
            name: patch.name.clone(),
            conclusion: patch.conclusion.clone(),
            started_at: time(patch.started_at_us).or(time(Some(patch.observed_us))),
            finished_at: time(patch.finished_at_us),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{RunRecord, TaskAgentJobRequestRecord};
    use preloop_gha_protocol::{ExecutionStatus, WorkflowSubmission};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Seed a single in-flight run with one job, its plan→request mapping, and
    /// timeline id. Returns the owned `TempDir` (keeps the store file alive for
    /// the test's lifetime) alongside the handles tests assert against.
    async fn seed_inflight_run() -> (
        tempfile::TempDir,
        Arc<SharedState>,
        RunId,
        JobId,
        String,
        uuid::Uuid,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .expect("app state");
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        let plan_id = run_id.to_string();
        let timeline_id = uuid::Uuid::new_v4();
        let request_id = 7_i64;
        // Seed the authoritative backend (runs/job_requests are TxState, not
        // node-local InnerState) so the handler's `read_scoped` sees them.
        {
            let plan_id = plan_id.clone();
            let job_id = job_id.clone();
            state
                .backend
                .transact(move |tx| {
                    tx.runs.insert(
                        run_id,
                        RunRecord {
                            run_id,
                            webhook_delivery_id: None,
                            run_name: Some("timeline-conclusion-test".to_owned()),
                            submission: Arc::new(WorkflowSubmission {
                                workflow_yaml: "on: push\njobs: {}\n".to_owned(),
                                event: "push".to_owned(),
                                repository: "test/repo".to_owned(),
                                git_ref: "refs/heads/main".to_owned(),
                                ..Default::default()
                            }),
                            jobs: BTreeMap::from([(job_id.clone(), ExecutionStatus::InProgress)]),
                            status: ExecutionStatus::InProgress,
                            job_outputs: BTreeMap::new(),
                            job_base_ids: BTreeMap::new(),
                            job_needs: BTreeMap::new(),
                            caller_plans: BTreeMap::new(),
                            job_names: BTreeMap::from([(job_id.clone(), "build".to_owned())]),
                            github: serde_json::json!({}),
                            head_sha: String::new(),
                            workflow_ref: String::new(),
                            workspace_snapshot: None,
                            job_fail_fast: BTreeMap::new(),
                            job_continue_on_error: BTreeMap::new(),
                            job_check_run_ids: BTreeMap::new(),
                            reusable_calls: BTreeMap::new(),
                            jobs_list: Vec::new(),
                            created_at: chrono::Utc::now(),
                            started_at: None,
                            completed_at: None,
                            run_number: 1,
                            run_attempt: 1,
                            workflow_path_str: ".github/workflows/ci.yml".to_owned(),
                            event: "push".to_owned(),
                            conclusion: None,
                            push_state: None,
                            snapshot_timing: None,
                        },
                    );
                    tx.plan_requests.insert(plan_id.clone(), request_id);
                    tx.job_requests.insert(
                        request_id,
                        TaskAgentJobRequestRecord {
                            request_id,
                            run_id,
                            job_id: job_id.clone(),
                            agent_job_id: uuid::Uuid::new_v4(),
                            plan_id: plan_id.clone(),
                            plan_type: "Build".to_owned(),
                            timeline_id,
                            result: None,
                            locked_until: String::new(),
                            claimed_at: None,
                            owner_runner_id: None,
                            started_at: None,
                            last_renewed_at: None,
                            timeout_triggered: false,
                            debug_token_issued: false,
                        },
                    );
                    Ok(())
                })
                .await
                .expect("seed backend");
        }
        (temp, shared, run_id, job_id, plan_id, timeline_id)
    }

    /// A timeline PATCH for an in-flight job must keep the truthful
    /// "in_progress" conclusion. The raw Debug spelling ("inprogress") and
    /// the run-level status_string projection ("success") both lie about a
    /// job that is still running.
    #[tokio::test]
    async fn timeline_patch_keeps_in_progress_conclusion_for_in_flight_job() {
        let (_temp, shared, run_id, _job_id, plan_id, timeline_id) = seed_inflight_run().await;
        let state = shared.state.clone();

        let _ = patch_timeline_records(
            State(shared),
            Path((
                "scope".to_owned(),
                "hub".to_owned(),
                plan_id,
                timeline_id.to_string(),
            )),
            Json(azdo::VssJsonCollectionWrapper {
                count: 0,
                value: Vec::new(),
            }),
        )
        .await;

        // `runs` is authoritative backend state — read it via the backend,
        // not node-local `inner`.
        let detail = state
            .backend
            .read(move |tx| {
                Ok(tx.runs.get(&run_id).and_then(|run| {
                    run.jobs_list
                        .iter()
                        .find(|detail| detail.name == "build")
                        .map(|d| d.conclusion.clone())
                }))
            })
            .await
            .expect("read run")
            .expect("timeline PATCH created the job detail");
        assert_eq!(
            detail, "in_progress",
            "an in-flight job must not read as 'success' or 'inprogress'"
        );
    }

    /// A job-level Node.js 20 deprecation warning arrives from the runner as a
    /// `Warning` issue on the job timeline record. The server must preserve it
    /// on read-back and project it as a job-level annotation (no `step_id`).
    /// Preloop never synthesizes this warning itself — it only surfaces what
    /// the runner sends.
    #[tokio::test]
    async fn timeline_patch_preserves_node20_deprecation_warning() {
        let (_temp, shared, run_id, _job_id, plan_id, timeline_id) = seed_inflight_run().await;
        let state = shared.state.clone();

        let node20_message = "Node.js 20 actions are deprecated. The following actions are \
             running on Node.js 20 and may not work as expected: actions/checkout@v3, \
             actions/setup-node@v3."
            .to_owned();

        let job_record_id = uuid::Uuid::new_v4();
        let child_task_id = uuid::Uuid::new_v4();

        // Construct raw JSON wire payload with upstream "Task" and "Job" types to exercise serde wire decoding.
        let raw_json_payload = serde_json::json!({
                "count": 2,
                "value": [
                    {
                        "id": job_record_id.to_string(),
                        "parentId": null,
                        "name": "build",
                        "displayName": "build",
                        "type": "Job",
                        "state": "inProgress",
                        "issues": [
                            {
                                "type": "warning",
                                "message": node20_message
        }
                        ],
                        "warningCount": 1
                    },
                    {
                        "id": child_task_id.to_string(),
                        "parentId": job_record_id.to_string(),
                        "name": "Complete job",
                        "displayName": "Complete job",
                        "type": "Task",
                        "state": "completed",
                        "result": "succeededWithIssues",
                        "issues": [
                            {
                                "type": "warning",
                                "message": node20_message
        }
                        ]
        }
                ]
            });

        // Verify wire decoding from raw JSON
        let wrapper: azdo::VssJsonCollectionWrapper<azdo::TimelineRecord> =
            serde_json::from_value(raw_json_payload.clone()).expect("valid wire JSON payload");
        assert_eq!(wrapper.value.len(), 2);
        assert_eq!(
            wrapper.value[0].record_type,
            Some(azdo::TimelineRecordType::Job)
        );
        assert_eq!(
            wrapper.value[1].record_type,
            Some(azdo::TimelineRecordType::Task)
        );

        // PATCH timeline records (first call)
        let _ = patch_timeline_records(
            State(shared.clone()),
            Path((
                "scope".to_owned(),
                "hub".to_owned(),
                plan_id.clone(),
                timeline_id.to_string(),
            )),
            Json(wrapper),
        )
        .await;

        // Preserved on read-back.
        let response = get_timeline_records(
            State(shared.clone()),
            Path((
                "scope".to_owned(),
                "hub".to_owned(),
                plan_id.clone(),
                timeline_id.to_string(),
            )),
            axum::extract::Query(TimelineQuery {
                top: None,
                skip: None,
            }),
        )
        .await;
        let records = response
            .0
            .get("records")
            .and_then(|v| v.as_array())
            .expect("records array");
        let stored_job = records
            .iter()
            .find(|r| r["id"] == job_record_id.to_string())
            .expect("job record persisted");
        let job_issues = stored_job["issues"].as_array().expect("issues preserved");
        assert_eq!(
            job_issues.len(),
            1,
            "the Node 20 warning issue must survive on job record"
        );
        assert_eq!(job_issues[0]["type"], "warning");
        assert_eq!(job_issues[0]["message"], node20_message);

        let stored_task = records
            .iter()
            .find(|r| r["id"] == child_task_id.to_string())
            .expect("child task record persisted");
        let task_issues = stored_task["issues"].as_array().expect("issues preserved");
        assert_eq!(
            task_issues.len(),
            1,
            "the Node 20 warning issue must survive on task record"
        );

        // Replaying/retrying the exact same PATCH call (Finding 3: deduplication check)
        let wrapper_retry: azdo::VssJsonCollectionWrapper<azdo::TimelineRecord> =
            serde_json::from_value(raw_json_payload).expect("valid wire JSON payload");
        let _ = patch_timeline_records(
            State(shared.clone()),
            Path((
                "scope".to_owned(),
                "hub".to_owned(),
                plan_id,
                timeline_id.to_string(),
            )),
            Json(wrapper_retry),
        )
        .await;

        let inner = state.inner.lock().await;
        let events = inner
            .timeline_events
            .get(&run_id)
            .expect("timeline events recorded for the run");

        // Count projected annotations for the Node 20 message
        let matching_annotations: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NdjsonEvent::Annotation {
                    level,
                    message,
                    step_id,
                    ..
                } if message == &node20_message => Some((*level, step_id.clone())),
                _ => None,
            })
            .collect();

        // Deduplication check: even after retrying PATCH, there should be exactly one annotation
        // for the job record (step_id == None) and one for the child task record (step_id == Some(child_task_id)).
        assert_eq!(
            matching_annotations.len(),
            2,
            "annotations must be deduplicated across repeated PATCH calls"
        );

        let job_ann = matching_annotations
            .iter()
            .find(|(_, step_id)| step_id.is_none())
            .expect("parentless job record produces step_id == None annotation");
        assert!(matches!(job_ann.0, AnnotationLevel::Warning));

        let task_ann = matching_annotations
            .iter()
            .find(|(_, step_id)| step_id.as_deref() == Some(&child_task_id.to_string()))
            .expect("child task record produces step_id == Some(task_id) annotation");
        assert!(matches!(task_ann.0, AnnotationLevel::Warning));
    }
}
