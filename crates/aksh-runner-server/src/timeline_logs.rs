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

        inner
            .timeline_events
            .entry(run_id.unwrap_or_else(|| RunId(uuid::Uuid::nil())))
            .or_default()
            .extend(projected.clone());

        if let (Some(run_id), Some(job_id)) = (run_id, &logical_job_id) {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
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
                        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
                            "in_progress"
                        }
                        _ => "success",
                    };

                    if let Some(pos) = job_detail.steps.iter().position(|s| s.name == *name) {
                        job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                    } else {
                        job_detail.steps.push(StepRecord {
                            name: name.clone(),
                            conclusion: conclusion_str.to_owned(),
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
    let response_records = {
        let mut inner = shared.state.inner.lock().await;
        let stored = inner.timeline_records.entry(timeline_key).or_default();
        for record in records {
            stored.insert(record.id, record);
        }
        let vals: Vec<_> = stored.values().cloned().collect();
        vals
    };

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
    let mut inner = shared.state.inner.lock().await;
    let next_id = inner.next_log_id;
    inner.next_log_id = next_id.wrapping_add(1);
    log.id = next_id as i64;
    let key = format!("{}/{}", plan_id, next_id);
    inner.logs.entry(key.clone()).or_default();
    inner.log_metadata.entry(key).or_default();
    Json(serde_json::to_value(&log).unwrap_or(json!({ "ok": true })))
}

/// POST append log — runner appends lines to a log file.
pub(crate) async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    let mut inner = shared.state.inner.lock().await;
    let masked = mask_log_bytes(&inner, &plan_id, &body);
    let byte_count = masked.len();
    let line_count = masked.iter().filter(|&&b| b == b'\n').count();
    inner
        .logs
        .entry(key.clone())
        .or_default()
        .extend_from_slice(&masked);
    let meta = inner.log_metadata.entry(key).or_default();
    meta.byte_count += byte_count;
    meta.line_count += line_count;
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
        .map(|run| {
            run.submission
                .secrets
                .values()
                .map(|s| s.expose().to_owned())
                .collect()
        })
        .unwrap_or_else(|| {
            inner
                .runs
                .values()
                .flat_map(|run| run.submission.secrets.values())
                .map(|s| s.expose().to_owned())
                .collect()
        });

    aksh_gha_protocol::masking::mask_secrets(&text, run_secrets.iter().map(String::as_str), &[])
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
    // Parse the body as a LiveLogFeedLinesWrapper and store/broadcast it.
    if let Ok(wrapper) = serde_json::from_slice::<LiveLogFeedLinesWrapper>(&body) {
        let job_id = {
            let inner = shared.state.inner.lock().await;
            resolve_callback_job(&inner, &plan_id, None, None)
                .map(|(_, _, job_id)| job_id.0.clone())
                .unwrap_or_else(|| plan_id.clone())
        };
        record_live_log_wrapper(&shared, &job_id, wrapper).await;
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

/// GET `/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id` — read back full timeline.
pub(crate) async fn get_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
) -> Json<serde_json::Value> {
    let timeline_key = format!("{}/{}", plan_id, timeline_id);
    let inner = shared.state.inner.lock().await;
    let change_id = inner
        .timeline_change_ids
        .get(&timeline_key)
        .copied()
        .unwrap_or(0);
    let records: Vec<_> = inner
        .timeline_records
        .get(&timeline_key)
        .map(|m| m.values().cloned().collect())
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
) -> Json<serde_json::Value> {
    get_timeline_records(
        State(shared),
        Path((String::new(), String::new(), plan_id, timeline_id)),
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
    let outputs: aksh_gha_protocol::OutputMap = outputs
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
