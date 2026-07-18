//! Completejob payload construction and completion reporting.

use anyhow::Result;
use tracing::{info, warn};

use super::execution_types::Annotation;
use super::helpers::{extract_service_endpoint, iso_now};
use super::job_runner::ReportingContext;
use super::steps_runner::{Step, StepType};
use crate::cli::ProtocolPath;
use crate::client::http::HttpClient;

/// Build step results for the completejob body, including annotations (F025).
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
        let conclusion = job_ctx
            .steps
            .get(&step.context_name)
            .map(|result| runner_conclusion(&result.conclusion))
            .unwrap_or("skipped");

        let (step_type, action_name) = completejob_type_and_action(step);

        // F025: Include annotations for this step
        let step_number = (idx + 2) as u32;
        let annotations: Vec<serde_json::Value> = step_annotations
            .get(&step.context_name)
            .map(|anns| {
                anns.iter()
                    .map(|a| annotation_to_json(a, step_number))
                    .collect()
            })
            .unwrap_or_default();

        results.push(serde_json::json!({
            "external_id": step.id,
            "number": step_number,
            "name": step.display_name,
            "action_name": action_name,
            "type": step_type,
            "status": "completed",
            "conclusion": conclusion,
            "started_at": &now,
            "completed_at": &now,
            "annotations": annotations,
        }));
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
        _ => "failed",
    }
}
/// Report job completion to the server.
///
/// F013: Full completejob body matching golden flow 25/41:
/// `{planId, jobId, conclusion, outputs, stepResults, annotations, telemetry, billingOwnerId}`
pub(crate) async fn report_completion(
    job_message: &serde_json::Value,
    result: &str,
    job_ctx: &super::contexts::JobContext,
    ordered_steps: &[Step],
    via: ProtocolPath,
    reporting: Option<&ReportingContext>,
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

    // F048: Collect job-level annotations for completejob body.
    // These are infrastructure-level issues (container failures, action download errors)
    // not tied to a specific step. Step annotations are already in stepResults (F025).
    let job_annotations: Vec<serde_json::Value> = job_ctx
        .job_annotations
        .iter()
        .map(|a| annotation_to_json(a, 0))
        .collect();

    let mut telemetry = vec![serde_json::json!({
        "type": "task",
        "message": format!("{{\"ClassType\":\"StepsRunner\",\"FinishResult\":\"{}\"}}", result.to_lowercase()),
    })];
    telemetry.extend(job_ctx.debugger_telemetry.iter().map(|dbg_result| {
        serde_json::json!({
            "type": "task",
            "message": format!("{{\"ClassType\":\"DapDebugger\",\"DebuggerConnectionResult\":\"{}\"}}", dbg_result),
        })
    }));

    let completion_body = serde_json::json!({
        "planId": plan_id,
        "jobId": job_id,
        "conclusion": result.to_lowercase(),
        "outputs": outputs,
        "stepResults": step_results,
        "annotations": job_annotations,
        "telemetry": telemetry,
        "billingOwnerId": billing_owner_id,
    });

    // Use reporting context if available, otherwise fall back to creating a new client
    if let Some(rpt) = reporting {
        match via {
            ProtocolPath::Broker => {
                let url = format!("{}/completejob", rpt.run_service.base_url());
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(
                        &url,
                        &completion_body,
                        &rpt.access_token,
                    )
                    .await
                {
                    Ok(_) => info!("Job completion reported successfully"),
                    Err(e) => warn!("completejob POST failed (non-fatal): {e:#}"),
                }
            }
            ProtocolPath::Azdo => {
                // F030: mark the job timeline record as Completed before posting the event.
                if let Some(azdo) = &rpt.azdo {
                    let azdo_result_str = match result.to_ascii_lowercase().as_str() {
                        "success" | "succeeded" => "succeeded",
                        "cancelled" | "canceled" => "canceled",
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
                        .update_timeline(&rpt.access_token, plan_id, &azdo.timeline_id, &job_record)
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
                let event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
                info!("Reporting completion to {url}");
                match rpt
                    .results
                    .http()
                    .post_json_bearer::<serde_json::Value>(&url, &event, &rpt.access_token)
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
                let event = serde_json::json!({
                    "name": "JobCompleted",
                    "jobId": job_id,
                    "requestId": job_message.get("requestId").and_then(|v| v.as_i64()).unwrap_or(0),
                    "result": result.to_lowercase(),
                    "outputs": outputs,
                });
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
/// / ACTIONS_RUNNER_HOOK_JOB_COMPLETED). The hook path is a shell script on the
/// runner host, executed with the default shell exactly like a `run:` step.
pub(crate) fn make_hook_step(
    id: &str,
    context_name: &str,
    script_path: &str,
) -> super::steps_runner::Step {
    super::steps_runner::Step {
        id: id.to_string(),
        context_name: context_name.to_string(),
        display_name: context_name.replace('_', " ").trim().to_string(),
        step_type: super::steps_runner::StepType::Script {
            script: script_path.to_string(),
            shell: None,
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
}
