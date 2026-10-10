//! Completejob payload construction and completion reporting.

use anyhow::{Context, Result};
use tracing::{info, warn};

use super::execution_types::Annotation;
use super::helpers::{extract_service_endpoint, iso_now};
use super::job_runner::ReportingContext;
use super::steps_runner::{Step, StepType};
use crate::cli::ProtocolPath;
use crate::client::http::HttpClient;

/// Build step results for the completejob body, including annotations.
///
/// Golden 06 flow 41: each stepResult has `{external_id, number, name,
/// action_name, type, status, conclusion, started_at, completed_at, annotations}`.
/// Golden 14: annotations array has `{level, message, title, startLine, endLine, stepNumber}`.
pub(crate) fn build_completejob_step_results(
    ordered_steps: &[Step],
    job_ctx: &super::contexts::JobContext,
    step_annotations: &std::collections::HashMap<String, Vec<Annotation>>,
) -> Vec<serde_json::Value> {
    let now = iso_now();
    let mut results = Vec::with_capacity(ordered_steps.len() + 2);

    // "Set up job" wrapper step
    results.push(serde_json::json!({
        "external_id": job_ctx.setup_step_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        "number": 1,
        "name": "Set up job",
        "action_name": "setup_job",
        "type": "runner",
        "status": "completed",
        "conclusion": "succeeded",
        "started_at": &now,
        "completed_at": &now,
        "annotations": [],
    }));

    for (idx, step) in ordered_steps.iter().enumerate() {
        let execution_result = job_ctx.steps.get(&step.context_name);
        let conclusion = execution_result
            .map(|result| runner_conclusion(&result.conclusion))
            .unwrap_or("skipped");

        // Include annotations for this step
        let step_number = (idx + 2) as u32;
        let annotations: Vec<serde_json::Value> = step_annotations
            .get(&step.context_name)
            .map(|anns| {
                anns.iter()
                    .map(|a| annotation_to_json(a, step_number))
                    .collect()
            })
            .unwrap_or_default();

        let mut result = serde_json::json!({
            "external_id": step.id,
            "number": step_number,
            "name": step.display_name,
            "status": "completed",
            "conclusion": conclusion,
            "started_at": &now,
            "completed_at": &now,
            "annotations": annotations,
        });
        // Official Runner.Worker adds action_name/type only for execution
        // contexts that became Task records. Unexecuted/skipped steps retain
        // the timeline identity and conclusion without invented task fields.
        if execution_result.is_some_and(|result| runner_conclusion(&result.conclusion) != "skipped")
        {
            let (step_type, action_name) = completejob_type_and_action(step);
            result["action_name"] = serde_json::json!(action_name);
            result["type"] = serde_json::json!(step_type);
        }
        results.push(result);
    }

    // "Complete job" wrapper step
    let complete_annotations: Vec<serde_json::Value> = job_ctx
        .job_annotations
        .iter()
        .map(|annotation| annotation_to_json(annotation, (ordered_steps.len() + 2) as u32))
        .collect();
    results.push(serde_json::json!({
        "external_id": job_ctx.complete_step_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        "number": ordered_steps.len() + 2,
        "name": "Complete job",
        "action_name": "complete_job",
        "type": "runner",
        "status": "completed",
        "conclusion": "succeeded",
        "started_at": &now,
        "completed_at": &now,
        "annotations": complete_annotations,
    }));

    results
}

/// Convert an Annotation to the golden 14 JSON shape.
pub(crate) fn annotation_to_json(ann: &Annotation, step_number: u32) -> serde_json::Value {
    use super::execution_context::AnnotationLevel;
    let level = match ann.level {
        AnnotationLevel::Notice => "notice",
        AnnotationLevel::Warning => "warning",
        AnnotationLevel::Error => "failure",
    };

    // Golden 14 always includes startLine/endLine; default to 1 when the
    // annotation carries no source-file line info.
    let start_line = ann.line.unwrap_or(1);
    let end_line = ann.end_line.unwrap_or(start_line);

    let mut obj = serde_json::json!({
        "level": level,
        "message": ann.message,
        "stepNumber": step_number,
        "startLine": start_line,
        "endLine": end_line,
    });

    if let Some(file) = &ann.file {
        obj["file"] = serde_json::json!(file);
    }

    if let Some(title) = &ann.title {
        obj["title"] = serde_json::json!(title);
    }
    if let Some(col) = ann.col {
        obj["startColumn"] = serde_json::json!(col);
    }
    if let Some(end_col) = ann.end_column {
        obj["endColumn"] = serde_json::json!(end_col);
    }

    obj
}

/// Convert a job annotation to an AzDO timeline issue payload.
fn annotation_to_timeline_issue(annotation: &Annotation) -> serde_json::Value {
    use super::execution_context::AnnotationLevel;
    let issue_type = match annotation.level {
        AnnotationLevel::Notice => "info",
        AnnotationLevel::Warning => "warning",
        AnnotationLevel::Error => "error",
    };
    let mut data = serde_json::Map::new();
    if let Some(file) = &annotation.file {
        data.insert("file".to_owned(), serde_json::json!(file));
    }
    if let Some(line) = annotation.line {
        data.insert("line".to_owned(), serde_json::json!(line.to_string()));
    }
    if let Some(end_line) = annotation.end_line {
        data.insert(
            "endLine".to_owned(),
            serde_json::json!(end_line.to_string()),
        );
    }
    if let Some(col) = annotation.col {
        data.insert("col".to_owned(), serde_json::json!(col.to_string()));
    }
    if let Some(end_column) = annotation.end_column {
        data.insert(
            "endColumn".to_owned(),
            serde_json::json!(end_column.to_string()),
        );
    }
    if let Some(title) = &annotation.title {
        data.insert("title".to_owned(), serde_json::json!(title));
    }
    serde_json::json!({
        "type": issue_type,
        "message": annotation.message,
        "data": data,
    })
}

pub(crate) fn completejob_type_and_action(step: &Step) -> (&'static str, String) {
    match &step.step_type {
        StepType::Script { shell, .. } => (
            "run",
            shell
                .as_deref()
                .and_then(|shell| shell.split_whitespace().next())
                .and_then(|shell| std::path::Path::new(shell).file_stem())
                .and_then(|stem| stem.to_str())
                .unwrap_or("sh")
                .to_string(),
        ),
        StepType::Action { uses, .. } => ("action", uses.clone()),
    }
}

pub(crate) fn runner_conclusion(conclusion: &str) -> &'static str {
    match conclusion.to_ascii_lowercase().as_str() {
        "success" | "succeeded" => "succeeded",
        "failure" | "failed" => "failed",
        "cancelled" | "canceled" => "canceled",
        "skipped" => "skipped",
        // Official TaskResult.Abandoned: the job never finished on this runner
        // (lease lost / first renew failed). Kept distinct on the wire so the
        // server can tell "runner vanished" from "steps failed".
        "abandoned" => "abandoned",
        _ => "failed",
    }
}

/// Evaluate the job's `environment.url` after every step ran, mirroring the
/// official runner.
///
/// `actionsEnvironment.url` ships in the job message as an unevaluated
/// TemplateToken: a workflow may reference `steps.<id>.outputs.*`, which only
/// exist once the step produced them, so the official runner resolves the
/// token in `JobExtension.FinalizeJob` (with the full job context, `steps`
/// included) and `JobRunner.CompleteJobAsync` reports the resulting string to
/// the run service as `CompleteJobRequest.environmentUrl`. preloop reproduces
/// that path: this function is called from the worker's completion flow, and
/// the value rides `completejob` as `environmentUrl`.
///
/// Error semantics follow the same code: a token whose expression cannot be
/// evaluated fails the job, and a value that would expose a secret is dropped
/// with a warning. `Ok(None)` means "no environment URL to report" — the
/// deployment status then carries no `environment_url`, as on GitHub.
pub(crate) fn evaluate_environment_url(
    job_message: &serde_json::Value,
    job_ctx: &super::contexts::JobContext,
) -> Result<Option<String>> {
    let Some(url_token) = job_message
        .get("actionsEnvironment")
        .and_then(|env| env.get("url"))
        .filter(|url| !url.is_null())
    else {
        return Ok(None);
    };
    // Token shapes (the parser's `template_token`): `{type:0, lit}` for a
    // literal, `{type:3, expr}` for an expression or a `format(…)`-folded
    // interpolation. A plain string is accepted too: a hydrated message may
    // already carry the evaluated value.
    let value = if let Some(raw) = url_token.as_str() {
        raw.to_owned()
    } else {
        match url_token.get("type").and_then(serde_json::Value::as_u64) {
            Some(0) => url_token
                .get("lit")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            Some(3) => {
                let expr = url_token
                    .get("expr")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let wrapped = format!("${{{{ {expr} }}}}");
                crate::worker::template::evaluate_template_strict(
                    &wrapped,
                    &job_ctx.build_expression_context(),
                )
                .with_context(|| {
                    format!("failed to evaluate environment url expression `{expr}`")
                })?
            }
            // An unknown token shape must never surface as the raw template.
            _ => return Ok(None),
        }
    };
    if value.is_empty() {
        return Ok(None);
    }
    if job_ctx
        .masks
        .iter()
        .any(|mask| !mask.is_empty() && value.contains(mask.as_str()))
    {
        // Official `JobExtension.FinalizeJob`: a URL that would disclose a
        // secret is skipped, never reported.
        warn!("Skip setting environment url as it may contain secret");
        return Ok(None);
    }
    Ok(Some(value))
}

/// Add the `actionsEnvironment` member to an AzDO-path `JobCompleted` event.
///
/// The official runner raises `JobCompletedEvent` carrying the job's
/// `ActionsEnvironment` (name plus the evaluated `url`) on the non-run-service
/// path; the server turns that member into the job's `environmentUrl`. Omitted
/// for jobs without an environment URL — the member is optional on the wire.
fn apply_azdo_actions_environment(
    event: &mut serde_json::Value,
    job_message: &serde_json::Value,
    environment_url: Option<&str>,
) {
    let Some(url) = environment_url else {
        return;
    };
    let name = job_message
        .get("actionsEnvironment")
        .and_then(|environment| environment.get("name"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    event["actionsEnvironment"] = serde_json::json!({ "name": name, "url": url });
}

/// Report job completion to the server.
///
/// Full completejob body matching golden flow 25/41:
/// `{planId, jobId, conclusion, outputs, stepResults, annotations, telemetry, billingOwnerId}`
pub(crate) async fn report_completion(
    job_message: &serde_json::Value,
    result: &str,
    job_ctx: &super::contexts::JobContext,
    ordered_steps: &[Step],
    via: ProtocolPath,
    reporting: Option<&ReportingContext>,
    environment_url: Option<&str>,
) -> Result<()> {
    let plan_id = job_message
        .get("plan")
        .and_then(|p| p.get("planId"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let job_id = job_message
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let billing_owner_id = job_message
        .get("billingOwnerId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Collect annotations from step contexts stored in the job context
    let step_annotations = job_ctx.step_annotations.clone();

    let step_results = build_completejob_step_results(ordered_steps, job_ctx, &step_annotations);

    // Evaluate job-level output expressions (e.g. `outputs: z: ${{ steps.step1.outputs.out1 }}`)
    // and include them in the completejob body so the server can propagate
    // them to downstream jobs and reusable workflow callers.
    let outputs = {
        let mut map = serde_json::Map::new();
        if let Some(output_decls) = job_message.get("jobOutputs") {
            let expr_ctx = job_ctx.build_expression_context();
            if let Some(obj) = output_decls.as_object() {
                if obj.contains_key("type") {
                    // Format 2: TemplateToken mapping
                    if let Some(map_arr) = obj.get("map").and_then(|m| m.as_array()) {
                        for item in map_arr {
                            if let Some(item_obj) = item.as_object() {
                                let key_lit = item_obj
                                    .get("Key")
                                    .and_then(|k| k.get("lit"))
                                    .and_then(|l| l.as_str());
                                let val_expr = item_obj
                                    .get("Value")
                                    .and_then(|v| v.get("expr"))
                                    .and_then(|e| e.as_str());
                                let val_lit = item_obj
                                    .get("Value")
                                    .and_then(|v| v.get("lit"))
                                    .and_then(|l| l.as_str());

                                if let Some(name) = key_lit {
                                    if let Some(expr) = val_expr {
                                        let expr_wrapped = format!("${{{{ {expr} }}}}");
                                        match crate::worker::template::evaluate_template(
                                            &expr_wrapped,
                                            &expr_ctx,
                                        ) {
                                            Ok(val) => {
                                                map.insert(
                                                    name.to_string(),
                                                    serde_json::json!({ "value": val }),
                                                );
                                            }
                                            Err(e) => {
                                                tracing::warn!(
                                                    "job output '{name}' expression failed: {e}"
                                                );
                                            }
                                        }
                                    } else if let Some(lit) = val_lit {
                                        map.insert(
                                            name.to_string(),
                                            serde_json::json!({ "value": lit }),
                                        );
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Format 1: Simple JSON map (string -> string)
                    for (name, expr_val) in obj {
                        if let Some(expr) = expr_val.as_str() {
                            match crate::worker::template::evaluate_template(expr, &expr_ctx) {
                                Ok(val) => {
                                    map.insert(name.clone(), serde_json::json!({ "value": val }));
                                }
                                Err(e) => {
                                    tracing::warn!("job output '{name}' expression failed: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }
        map
    };

    // Collect job-level annotations for completejob body.
    // These are infrastructure-level issues (container failures, action download errors)
    // not tied to a specific step. Step annotations are already in stepResults.
    let job_annotations: Vec<serde_json::Value> = job_ctx
        .job_annotations
        .iter()
        .map(|a| annotation_to_json(a, 0))
        .collect();

    let mut telemetry = Vec::new();
    telemetry.extend(job_ctx.debugger_telemetry.iter().map(|dbg_result| {
        serde_json::json!({
            "type": "task",
            "message": format!("{{\"ClassType\":\"DapDebugger\",\"DebuggerConnectionResult\":\"{}\"}}", dbg_result),
        })
    }));
    if let Some(rpt) = reporting {
        telemetry.extend(rpt.connectivity_telemetry.lock().await.iter().cloned());
    }

    let mut completion_body = serde_json::json!({
        "planId": plan_id,
        "jobId": job_id,
        "conclusion": runner_conclusion(result),
        "outputs": outputs,
        "stepResults": step_results,
        "annotations": job_annotations,
        "telemetry": telemetry,
        "billingOwnerId": billing_owner_id,
    });
    // Official `CompleteJobRequest.EnvironmentUrl` (`EmitDefaultValue=false`):
    // present only when the job declared an environment URL that resolved.
    if let Some(url) = environment_url {
        completion_body["environmentUrl"] = serde_json::json!(url);
    }

    // Use reporting context if available, otherwise fall back to creating a new client
    if let Some(rpt) = reporting {
        // The token may have expired during the final long step. The renew
        // loop keeps it fresh while running, but it is aborted before
        // completion — refresh once so the terminal completejob is not
        // rejected with 401.
        if rpt.access_token.due_for_refresh()
            && let Some((fresh, refresh_at)) = super::job_runner::refresh_worker_oauth_token().await
        {
            rpt.access_token.update(fresh, refresh_at);
            info!("OAuth token refreshed before completion");
        }
        match via {
            ProtocolPath::Broker => {
                let url = format!("{}/completejob", rpt.run_service.base_url());
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(&url, &completion_body, &rpt.token())
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                // mark the job timeline record as Completed before posting the event.
                if let Some(azdo) = &rpt.azdo {
                    let azdo_result_str = match result.to_ascii_lowercase().as_str() {
                        "success" | "succeeded" => "succeeded",
                        "cancelled" | "canceled" => "canceled",
                        "abandoned" => "abandoned",
                        _ => "failed",
                    };
                    let issues: Vec<serde_json::Value> = job_ctx
                        .job_annotations
                        .iter()
                        .map(annotation_to_timeline_issue)
                        .collect();
                    let job_record = serde_json::json!({
                        "count": 1,
                        "value": [{
                            "id": job_id,
                            "type": "job",
                            "state": "completed",
                            "result": azdo_result_str,
                            "finishTime": iso_now(),
                            "percentComplete": 100_u32,
                            "issues": issues,
                        }]
                    });
                    match azdo
                        .client
                        .update_timeline(&rpt.token(), plan_id, &azdo.timeline_id, &job_record)
                        .await
                    {
                        Ok(_) => info!("AzDO: job timeline record set to Completed"),
                        Err(e) => warn!("AzDO: job timeline Completed failed (non-fatal): {e:#}"),
                    }
                }

                let url = format!(
                    "{}/_apis/v1/plans/{plan_id}/events",
                    rpt.run_service.base_url()
                );
                let mut event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                apply_azdo_actions_environment(&mut event, job_message, environment_url);
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(&url, &event, &rpt.token())
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("FinishJob POST failed (non-fatal): {e:#}"),
                }
            }
        }
    } else if let Some((service_url, access_token)) = extract_service_endpoint(job_message) {
        let http = HttpClient::new(None)?;
        match via {
            ProtocolPath::Broker => {
                let url = format!("{service_url}/completejob");
                info!("Reporting completion to {url}");
                match http
                    .post_json_bearer::<serde_json::Value>(&url, &completion_body, &access_token)
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                let url = format!("{service_url}/_apis/v1/plans/{plan_id}/events");
                let mut event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                apply_azdo_actions_environment(&mut event, job_message, environment_url);
                info!("Reporting completion to {url}");
                match http
                    .post_json_bearer::<serde_json::Value>(&url, &event, &access_token)
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("FinishJob POST failed (non-fatal): {e:#}"),
                }
            }
        }
    } else {
        warn!("No SystemVssConnection endpoint — cannot report completion");
        info!(
            "Job completion (unreported): planId={plan_id}, jobId={job_id}, result={result}, steps={}",
            step_results.len()
        );
    }

    Ok(())
}

/// Build a synthetic script Step for a job hook (ACTIONS_RUNNER_HOOK_JOB_STARTED
/// / ACTIONS_RUNNER_HOOK_JOB_COMPLETED). The hook variable carries a path on the
/// runner host. Upstream `HostContext.GetDefaultShellForScript` routes `.js`
/// hook scripts through `GetInternalNodeVersion()` (node24 by default,
/// overridable via `ACTIONS_RUNNER_FORCED_INTERNAL_NODE_VERSION`) and
/// everything else through the default shell — mirrored here by emitting a
/// POSIX wrapper for `.js` paths that execs the bundled internal node (or the
/// `/__e` container mount inside container jobs), and leaving the path string
/// for the default shell otherwise.
pub(crate) fn make_hook_step(
    id: &str,
    context_name: &str,
    script_path: &str,
    workspace: &str,
) -> super::steps_runner::Step {
    let mut script = script_path.to_string();
    let shell: Option<String> = None;
    if script_path.to_ascii_lowercase().ends_with(".js") {
        script = js_hook_wrapper(script_path, std::path::Path::new(workspace));
    }
    super::steps_runner::Step {
        id: id.to_string(),
        context_name: context_name.to_string(),
        display_name: context_name.replace('_', " ").trim().to_string(),
        step_type: super::steps_runner::StepType::Script {
            script,
            shell,
            working_directory: None,
        },
        condition: Some("always()".to_string()),
        continue_on_error: true,
        timeout_minutes: Some(10),
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background: false,
    }
}

/// POSIX wrapper emitted for a `.js` job hook: pick the internal Node
/// appropriate to where the step runs, then `require()` the hook file so a
/// missing/unreadable path fails through node's own error instead of
/// executing the path string as source. Mirrors upstream
/// `HostContext.GetDefaultShellForScript`'s `.js` branch, which runs the
/// script under `GetInternalNodeVersion()`.
fn js_hook_wrapper(script_path: &str, workspace: &std::path::Path) -> String {
    let version = internal_node_version();
    let host_node = bundled_node_path(workspace, version).unwrap_or_else(|| "node".to_owned());
    let escaped = script_path.replace('\\', "\\\\").replace('\'', "\\'");
    // Container jobs mount the runner's externals at /__e (the same path the
    // action handler uses); use that node when the mount is visible. Host
    // jobs exec the bundled binary directly, falling back to whatever `node`
    // is on PATH (--no-externals installs).
    format!(
        "if [ -x /__e/{version}/bin/node ]; then \\\n\
         \x20 exec /__e/{version}/bin/node -e \"require('{escaped}')\" \\\n\
         \x20 else \\\n\
         \x20 exec {host_node} -e \"require('{escaped}')\" \\\n\
         \x20 fi\n"
    )
}

/// `GetInternalNodeVersion` selection: node24 by default in v2.338.0,
/// `ACTIONS_RUNNER_FORCED_INTERNAL_NODE_VERSION` forces a built-in version
/// when it names one.
fn internal_node_version() -> &'static str {
    match std::env::var("ACTIONS_RUNNER_FORCED_INTERNAL_NODE_VERSION") {
        Ok(v) if v == "node20" => "node20",
        _ => "node24",
    }
}

/// The bundled node binary under the runner root found by walking up from
/// the workspace, if present.
fn bundled_node_path(workspace: &std::path::Path, version: &str) -> Option<String> {
    let runner_root = workspace
        .ancestors()
        .find(|dir| dir.join("externals").is_dir())?;
    let bundled = if cfg!(target_os = "windows") {
        runner_root.join("externals").join(version).join("node.exe")
    } else {
        runner_root
            .join("externals")
            .join(version)
            .join("bin")
            .join("node")
    };
    bundled
        .is_file()
        .then(|| bundled.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::execution_context::AnnotationLevel;

    #[test]
    fn annotation_serialization_preserves_job_fields() {
        let annotation = Annotation {
            level: AnnotationLevel::Error,
            message: "failed".into(),
            title: Some("Build".into()),
            file: Some("src/main.rs".into()),
            line: Some(11),
            end_line: Some(12),
            col: Some(2),
            end_column: Some(8),
        };

        let json = annotation_to_json(&annotation, 0);
        assert_eq!(json["level"], "failure");
        assert_eq!(json["file"], "src/main.rs");
        assert_eq!(json["startLine"], 11);
        assert_eq!(json["endLine"], 12);
        assert_eq!(json["startColumn"], 2);
        assert_eq!(json["endColumn"], 8);
        assert_eq!(json["title"], "Build");
        assert_eq!(json["stepNumber"], 0);

        let timeline = annotation_to_timeline_issue(&annotation);
        assert_eq!(timeline["type"], "error");
        assert_eq!(timeline["data"]["file"], "src/main.rs");
        assert_eq!(timeline["data"]["line"], "11");
        assert_eq!(timeline["data"]["endLine"], "12");
        assert_eq!(timeline["data"]["col"], "2");
        assert_eq!(timeline["data"]["endColumn"], "8");
        assert_eq!(timeline["data"]["title"], "Build");
    }

    #[test]
    fn runner_conclusion_uses_service_spelling_for_cancellation() {
        assert_eq!(runner_conclusion("Cancelled"), "canceled");
        assert_eq!(runner_conclusion("canceled"), "canceled");
    }

    #[test]
    fn skipped_step_result_omits_task_only_fields() {
        let step = Step {
            id: "step-id".into(),
            context_name: "skipped".into(),
            display_name: "Skipped step".into(),
            step_type: StepType::Script {
                script: "echo skipped".into(),
                shell: Some("bash".into()),
                working_directory: None,
            },
            condition: Some("false".into()),
            continue_on_error: false,
            timeout_minutes: None,
            env: std::collections::HashMap::new(),
            raw: serde_json::json!({}),
            is_background: false,
        };
        let mut job = super::super::contexts::JobContext::new(
            "job".into(),
            "job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.steps.insert(
            "skipped".into(),
            super::super::contexts::StepResult {
                outcome: "Skipped".into(),
                conclusion: "Skipped".into(),
                outputs: std::collections::HashMap::new(),
            },
        );

        let results =
            build_completejob_step_results(&[step], &job, &std::collections::HashMap::new());
        let skipped = &results[1];
        assert_eq!(skipped["conclusion"], "skipped");
        assert!(skipped.get("action_name").is_none());
        assert!(skipped.get("type").is_none());
    }

    fn environment_job() -> super::super::contexts::JobContext {
        super::super::contexts::JobContext::new(
            "deploy".into(),
            "deploy".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        )
    }

    /// `environment.url` may read `steps.<id>.outputs` (the `actions/deploy-pages`
    /// shape): the token is resolved after the steps ran, with the job's own
    /// `steps` context — the value the server posts as the deployment status's
    /// `environment_url`.
    #[test]
    fn environment_url_resolves_step_outputs() {
        let mut job = environment_job();
        job.steps.insert(
            "deploy".into(),
            super::super::contexts::StepResult {
                outcome: "Success".into(),
                conclusion: "Success".into(),
                outputs: std::collections::HashMap::from([(
                    "page_url".to_owned(),
                    "https://example.test/".to_owned(),
                )]),
            },
        );
        let message = serde_json::json!({
            "actionsEnvironment": {
                "name": "github-pages",
                "url": {"type": 3, "expr": "steps.deploy.outputs.page_url"},
            }
        });
        assert_eq!(
            evaluate_environment_url(&message, &job).unwrap().as_deref(),
            Some("https://example.test/")
        );
    }

    /// Literal tokens, an already-evaluated string, `null`, and no environment
    /// at all: only a real value is reported, and a raw `${{ }}` template is
    /// never returned.
    #[test]
    fn environment_url_reports_only_resolved_values() {
        let job = environment_job();
        let literal = serde_json::json!({
            "actionsEnvironment": {"name": "prod", "url": {"type": 0, "lit": "https://x.test"}}
        });
        assert_eq!(
            evaluate_environment_url(&literal, &job).unwrap().as_deref(),
            Some("https://x.test")
        );
        let hydrated = serde_json::json!({
            "actionsEnvironment": {"name": "prod", "url": "https://hydrated.test"}
        });
        assert_eq!(
            evaluate_environment_url(&hydrated, &job)
                .unwrap()
                .as_deref(),
            Some("https://hydrated.test")
        );
        assert!(
            evaluate_environment_url(&serde_json::json!({}), &job)
                .unwrap()
                .is_none(),
            "a job with no environment reports no url"
        );
        assert!(
            evaluate_environment_url(
                &serde_json::json!({"actionsEnvironment": {"name": "prod", "url": null}}),
                &job
            )
            .unwrap()
            .is_none(),
            "an environment without a url reports none"
        );
    }

    /// The official runner fails the job when the URL expression cannot be
    /// evaluated (`JobExtension.FinalizeJob`); the caller folds this error into
    /// the job result and no URL reaches the deployment.
    #[test]
    fn environment_url_fails_when_the_expression_cannot_evaluate() {
        let job = environment_job();
        let message = serde_json::json!({
            "actionsEnvironment": {
                "name": "prod",
                "url": {"type": 3, "expr": "nosuchfunction('x')"},
            }
        });
        assert!(evaluate_environment_url(&message, &job).is_err());
    }

    /// A URL that would disclose a secret is skipped, never reported — the
    /// official runner's `MaskSecrets` guard.
    #[test]
    fn environment_url_skips_values_containing_secrets() {
        let mut job = environment_job();
        job.masks.insert("s3cret".to_owned());
        let message = serde_json::json!({
            "actionsEnvironment": {
                "name": "prod",
                "url": {"type": 0, "lit": "https://x.test/?token=s3cret"},
            }
        });
        assert!(
            evaluate_environment_url(&message, &job).unwrap().is_none(),
            "a secret-bearing url must not be reported"
        );
    }
}
