use super::*;

// Timeline, logs, completion

/// PATCH timeline records — runner updates step/job state.
pub(crate) async fn patch_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    let mut records = wrapper.value;
    let timeline_key = format!("{}/{}", plan_id, timeline_id);
    let callback_job = {
        let inner = shared.state.inner.lock().await;
        resolve_callback_job(&inner, &plan_id, timeline_id.parse().ok(), None)
    };
    let run_id = callback_job
        .as_ref()
        .map(|(_, run_id, _)| *run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let logical_job_id = callback_job.as_ref().map(|(_, _, job_id)| job_id.clone());
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
                let step_id = if record.record_type == Some(azdo::TimelineRecordType::Step)
                    || record.parent_id.is_some()
                {
                    Some(record.id.to_string())
                } else {
                    None
                };
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

    let new_change_id = {
        let mut inner = shared.state.inner.lock().await;
        let current = inner
            .timeline_change_ids
            .entry(timeline_key.clone())
            .or_insert(0);
        *current += 1;
        let new_id = *current;

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

        if let (Some(run_id), Some(job_id)) = (run_id, &logical_job_id) {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            // A timeline update means the job started; the run
                            // record's final conclusion comes from the job
                            // status map (projected in the runs GET). Default
                            // to the truthful in-flight state, never "success".
                            conclusion: "in_progress".to_owned(),
                            steps: Vec::new(),
                            annotations: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(job_id) {
                    // The status map is authoritative for terminal states
                    // only. Its in-flight projection ("inprogress" from the
                    // raw Debug spelling, or "success" from the run-level
                    // status_string) lies about a job that is still running —
                    // keep the truthful "in_progress" default set above.
                    if *status != ExecutionStatus::InProgress {
                        job_detail.conclusion = format!("{:?}", status).to_lowercase();
                    }
                }

                for record in &records {
                    let Some(name) = &record.display_name else {
                        continue;
                    };
                    if record.id.to_string() == job_id.0 {
                        continue;
                    }

                    let conclusion_str = match record.result {
                        Some(
                            azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues,
                        ) => "success",
                        Some(azdo::TaskResult::Failed) => {
                            if run.jobs.get(job_id) == Some(&ExecutionStatus::Cancelled) {
                                "cancelled"
                            } else {
                                "failure"
                            }
                        }
                        Some(azdo::TaskResult::Cancelled) => "cancelled",
                        Some(azdo::TaskResult::Skipped) => "skipped",
                        Some(azdo::TaskResult::Abandoned) => "failed",
                        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
                            "in_progress"
                        }
                        _ => "success",
                    };
                    let started_at = record
                        .start_time
                        .as_deref()
                        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                        .map(|t| t.with_timezone(&chrono::Utc));
                    let finished_at = record
                        .finish_time
                        .as_deref()
                        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                        .map(|t| t.with_timezone(&chrono::Utc));
                    let observed = chrono::Utc::now();

                    if let Some(pos) = job_detail.steps.iter().position(|s| s.name == *name) {
                        job_detail.steps[pos].id = Some(record.id.to_string());
                        job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                        if let Some(started_at) = started_at {
                            job_detail.steps[pos].started_at = Some(started_at);
                        }
                        if let Some(finished_at) = finished_at {
                            job_detail.steps[pos].finished_at = Some(finished_at);
                        }
                    } else {
                        job_detail.steps.push(StepRecord {
                            id: Some(record.id.to_string()),
                            name: name.clone(),
                            conclusion: conclusion_str.to_owned(),
                            started_at: started_at.or(Some(observed)),
                            finished_at,
                        });
                    }
                }
            }
        }
        new_id
    };
    for event in projected {
        shared.state.emit(event).await;
    }
    // Stamp each record with server-computed fields.
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    for record in &mut records {
        record.change_id = Some(new_change_id);
        record.last_modified = Some(now.clone());
    }

    // Persist records (upsert by record ID) and return the full stored set.
    let (response_records, meta) = {
        let mut inner = shared.state.inner.lock().await;
        let stored = inner
            .timeline_records
            .entry(timeline_key.clone())
            .or_default();
        // Ids just upserted by this PATCH — protect them from eviction so a
        // low-sorting UUID isn't dropped out of the response/timeline.
        let patched_ids: Vec<uuid::Uuid> = records.iter().map(|r| r.id).collect();
        for record in records {
            stored.insert(record.id, record);
        }
        trim_timeline_after_patch(&mut inner, &timeline_key, &patched_ids);
        let vals: Vec<_> = inner
            .timeline_records
            .get(&timeline_key)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        (vals, crate::store::build_meta_snapshot(&inner))
    };
    // Persist after the lock is released so a slow backend does not serialize
    // the control plane behind the snapshot write.
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        warn!(?error, "failed to persist timeline records");
    }

    Json(json!({ "count": response_records.len(), "value": response_records }))
}
pub(crate) fn timeline_status(record: &azdo::TimelineRecord) -> Option<ExecutionStatus> {
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

pub(crate) fn issue_level(issue_type: azdo::IssueType) -> AnnotationLevel {
    match issue_type {
        azdo::IssueType::Error => AnnotationLevel::Error,
        azdo::IssueType::Warning => AnnotationLevel::Warning,
        azdo::IssueType::Info => AnnotationLevel::Notice,
    }
}

/// POST create log file — runner creates a log container.
pub(crate) async fn create_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(mut log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    let (meta, evicted) = {
        let mut inner = shared.state.inner.lock().await;
        let next_id = inner.next_log_id;
        inner.next_log_id = next_id.wrapping_add(1);
        log.id = next_id as i64;
        let key = format!("{}/{}", plan_id, next_id);
        inner.logs.entry(key.clone()).or_default();
        inner.log_metadata.entry(key.clone()).or_default();
        if !inner.log_order.iter().any(|k| k == &key) {
            inner.log_order.push_back(key.clone());
        }
        let evicted = trim_plan_logs(&mut inner, &plan_id);
        (crate::store::build_meta_snapshot(&inner), evicted)
    };
    // Delete durably any logs the caps just evicted from memory, so the
    // on-disk store never outgrows the in-memory retention (D2).
    for key in &evicted {
        if let Err(error) = shared.state.store.delete_log(key).await {
            warn!(?error, key, "failed to delete evicted log from store");
        }
    }
    if let Err(error) = shared.state.store.store_meta_only(&meta).await {
        warn!(?error, "failed to persist created log");
    }
    Json(serde_json::to_value(&log).unwrap_or(json!({ "ok": true })))
}

/// POST append log — runner appends lines to a log file.
pub(crate) async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    // Hot path: mutate and capture the chunk under the lock, then persist
    // after releasing it.
    let (masked, chunk_index, byte_count, line_count, evicted) = {
        let mut inner = shared.state.inner.lock().await;
        let is_new = !inner.logs.contains_key(&key);
        let masked = mask_log_bytes(&inner, &plan_id, &body);
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
        // F1: keep only the newest bytes in memory. `log_chunks` is the live
        // console recovery buffer (read only by restart to refill this map),
        // bounded to the SAME per-key budget in `store_log_chunk` — see D2.
        // The complete, permanent logs are the step/job-log blobs the runner
        // uploads separately, so trimming this tail never drops real log data.
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
        let evicted = trim_plan_logs(&mut inner, &plan_id);
        // Hot path: write the chunk to `log_chunks` and UPSERT the per-log
        // counter, instead of rewriting the entire meta snapshot for every
        // append. The counter is small and idempotent; the chunk is the
        // append-only event stream. We use the new byte count as the chunk
        // index so each append maps to a unique `(log_key, chunk_index)` row.
        let meta = inner.log_metadata.get(&key).cloned().unwrap_or_default();
        (
            masked,
            meta.byte_count as i64,
            meta.byte_count as i64,
            meta.line_count as i64,
            evicted,
        )
    };
    // Delete durably any logs the caps just evicted from memory (D2). The just
    // appended key is never in this list — it is the newest.
    for key in &evicted {
        if let Err(error) = shared.state.store.delete_log(key).await {
            warn!(?error, key, "failed to delete evicted log from store");
        }
    }
    if let Err(error) = shared
        .state
        .store
        .store_log_chunk(&key, chunk_index, &masked, byte_count, line_count)
        .await
    {
        warn!(?error, "failed to persist appended log chunk");
    }
    StatusCode::ACCEPTED
}

pub(crate) fn log_key(plan_id: &str, log_id: &str) -> String {
    format!("{plan_id}/{log_id}")
}

pub(crate) fn mask_log_bytes(inner: &InnerState, plan_id: &str, body: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(body);
    let resolved_run_id = resolve_callback_job(inner, plan_id, None, None)
        .map(|(_, run_id, _)| run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let run_secrets: Vec<String> = resolved_run_id
        .and_then(|run_id| inner.runs.get(&run_id))
        .map(|run| preloop_gha_protocol::masking::expose_values(run.submission.secrets.values()))
        .unwrap_or_else(|| {
            preloop_gha_protocol::masking::expose_values(
                inner
                    .runs
                    .values()
                    .flat_map(|run| run.submission.secrets.values()),
            )
        });

    preloop_gha_protocol::masking::mask_secrets(&text, run_secrets.iter().map(String::as_str), &[])
        .into_bytes()
}

/// POST console log — runner streams live console output.
pub(crate) async fn console_log(
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
        let resolved = {
            let inner = shared.state.inner.lock().await;
            resolve_callback_job(&inner, &plan_id, None, None)
                .map(|(_, run_id, job_id)| (run_id, job_id.0))
        };
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
pub(crate) async fn finish_job(
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
        let mut inner = shared.state.inner.lock().await;
        let callback_resolved = resolve_callback_job(
            &inner,
            &plan_id,
            Some(event.timeline_id),
            Some(event.job_id),
        );
        let active_resolved =
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id));
        let resolved = callback_resolved.or(active_resolved).or_else(|| {
            plan_id
                .parse::<RunId>()
                .ok()
                .map(|run_id| (0, run_id, JobId(event.job_id.to_string())))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
                annotations: Vec::new(),
                step_results: Vec::new(),
            })
        } else {
            None
        }
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
pub(crate) async fn patch_timeline_records_plan(
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
pub(crate) struct TimelineQuery {
    #[serde(default)]
    pub(crate) top: Option<usize>,
    #[serde(default)]
    pub(crate) skip: Option<usize>,
}

/// GET `/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id` — read back the timeline.
pub(crate) async fn get_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Query(query): Query<TimelineQuery>,
) -> Json<serde_json::Value> {
    let timeline_key = format!("{}/{}", plan_id, timeline_id);
    let inner = shared.state.inner.lock().await;
    let change_id = inner
        .timeline_change_ids
        .get(&timeline_key)
        .copied()
        .unwrap_or(0);
    // When `top` is absent the official runner expects the full timeline
    // (it does not paginate). Storage itself is already capped at
    // MAX_TIMELINE_RECORDS=1024, so returning all is bounded. When `top`
    // is present we clamp to MAX_TOP_RECORDS.
    let (top, skip) = match query.top {
        Some(t) => (t.min(MAX_TOP_RECORDS), query.skip.unwrap_or(0)),
        None => (usize::MAX, query.skip.unwrap_or(0)),
    };
    let records: Vec<_> = inner
        .timeline_records
        .get(&timeline_key)
        .map(|m| m.values().skip(skip).take(top).cloned().collect())
        .unwrap_or_default();
    Json(json!({
        "id": timeline_id,
        "changeId": change_id,
        "lastChangedBy": uuid::Uuid::nil(),
        "lastChangedOn": "0001-01-01T00:00:00",
        "records": records
    }))
}

/// GET `/_apis/v1/plans/:plan_id/timelines/:timeline_id/records`
pub(crate) async fn get_timeline_records_plan(
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
pub(crate) async fn create_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    create_log(
        State(shared),
        Path((String::new(), String::new(), plan_id)),
        Json(log),
    )
    .await
}

/// PUT `/_apis/v1/plans/:plan_id/logs/:log_id`
pub(crate) async fn append_log_plan(
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
pub(crate) async fn finish_job_plan(
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

    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let resolved = resolve_callback_job(&inner, &plan_id, None, None).or_else(|| {
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
                annotations: Vec::new(),
                step_results: Vec::new(),
            })
        } else {
            warn!(plan_id, "finish_job_plan: could not resolve run/job");
            None
        }
    };
    if let Some(c) = completion {
        let _ = complete_job_inner(shared, c).await;
    }
    Json(serde_json::Value::Null)
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
        {
            let mut inner = state.inner.lock().await;
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
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
            inner.plan_requests.insert(plan_id.clone(), request_id);
            inner.job_requests.insert(
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
                    started_at: None,
                    last_renewed_at: None,
                    timeout_triggered: false,
                    debug_token_issued: false,
                },
            );
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

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).expect("run still present");
        let detail = run
            .jobs_list
            .iter()
            .find(|detail| detail.name == "build")
            .expect("timeline PATCH created the job detail");
        assert_eq!(
            detail.conclusion, "in_progress",
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
