use super::*;

/// Fetch a remote reusable workflow YAML from GitHub.
/// `uses` format: `owner/repo/path/.github/workflows/workflow.yml@ref`
pub(crate) async fn fetch_remote_workflow(uses: &str) -> Result<String, anyhow::Error> {
    let parts: Vec<&str> = uses.split('@').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("invalid uses format: {uses}"));
    }
    let path_part = parts[0];
    let git_ref = parts[1];
    let segments: Vec<&str> = path_part.splitn(3, '/').collect();
    if segments.len() < 3 {
        return Err(anyhow::anyhow!("invalid uses path: {uses}"));
    }
    let owner = segments[0];
    let repo = segments[1];
    let path = segments[2];
    let url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        owner, repo, git_ref, path
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

/// Resolve a git ref (branch/tag) to a commit SHA via the GitHub API.
pub(crate) async fn resolve_remote_sha(owner: &str, repo: &str, git_ref: &str) -> Option<String> {
    let url = format!(
        "https://api.github.com/repos/{}/{}/git/ref/{}",
        owner, repo, git_ref
    );
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "aksh-runner-server")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        // Try tags endpoint if heads fails
        let url = format!(
            "https://api.github.com/repos/{}/{}/git/ref/tags/{}",
            owner, repo, git_ref
        );
        let resp = client
            .get(&url)
            .header("User-Agent", "aksh-runner-server")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        return json
            .get("object")
            .and_then(|o| o.get("sha"))
            .and_then(|s| s.as_str())
            .map(String::from);
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("object")
        .and_then(|o| o.get("sha"))
        .and_then(|s| s.as_str())
        .map(String::from)
}

pub(crate) async fn resolve_all_reusable_workflows(
    workflow: &aksh_gha_parser::Workflow,
    reusable_workflows: &mut BTreeMap<String, String>,
    reusable_shas: &mut BTreeMap<String, String>,
    depth: usize,
) -> Result<(), ApiError> {
    if depth >= 4 {
        return Ok(());
    }
    for job in workflow.jobs.values() {
        if let Some(uses) = &job.uses {
            if !uses.starts_with("./") && !uses.starts_with(".github/") {
                if !reusable_workflows.contains_key(uses) {
                    let text = fetch_remote_workflow(uses).await.map_err(|e| {
                        ApiError::bad_request(format!(
                            "failed to fetch remote workflow `{}`: {}",
                            uses, e
                        ))
                    })?;
                    reusable_workflows.insert(uses.clone(), text.clone());
                    if let Ok(called) = parse_workflow(&text) {
                        Box::pin(resolve_all_reusable_workflows(
                            &called,
                            reusable_workflows,
                            reusable_shas,
                            depth + 1,
                        ))
                        .await?;
                    }
                }
                if !reusable_shas.contains_key(uses) {
                    let parts: Vec<&str> = uses.split('@').collect();
                    if parts.len() == 2 {
                        let path_part = parts[0];
                        let git_ref = parts[1];
                        let path_segments: Vec<&str> = path_part.splitn(3, '/').collect();
                        if path_segments.len() == 3 {
                            let owner = path_segments[0];
                            let repo = path_segments[1];
                            if let Some(sha) = resolve_remote_sha(owner, repo, git_ref).await {
                                reusable_shas.insert(uses.clone(), sha);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn propagate_reusable_outputs(run: &mut RunRecord) {
    let mut outputs_to_add = Vec::new();
    for (caller_job_id, call) in &run.reusable_calls {
        let caller_job_id_typed = JobId(caller_job_id.clone());
        if run.job_outputs.contains_key(&caller_job_id_typed) {
            continue;
        }

        // Check if all inner jobs are complete
        let all_complete = !call.inner_job_ids.is_empty()
            && call.inner_job_ids.iter().all(|id| {
                run.jobs.get(&JobId(id.clone())).is_some_and(|status| {
                    matches!(
                        status,
                        ExecutionStatus::Success
                            | ExecutionStatus::Failure
                            | ExecutionStatus::Skipped
                            | ExecutionStatus::Cancelled
                    )
                })
            });

        if all_complete {
            // Build expression context
            let mut jobs_map = serde_json::Map::new();
            for inner_id in &call.inner_job_ids {
                let prefix = format!("{}/", caller_job_id);
                let inner_id_without_prefix = if inner_id.starts_with(&prefix) {
                    &inner_id[prefix.len()..]
                } else {
                    inner_id
                };

                let mut job_outputs_map = serde_json::Map::new();
                if let Some(outputs) = run.job_outputs.get(&JobId(inner_id.clone())) {
                    for (k, v) in outputs {
                        job_outputs_map.insert(k.clone(), v.clone());
                    }
                }

                let mut job_record = serde_json::Map::new();
                job_record.insert(
                    "outputs".to_owned(),
                    serde_json::Value::Object(job_outputs_map),
                );
                jobs_map.insert(
                    inner_id_without_prefix.to_owned(),
                    serde_json::Value::Object(job_record),
                );
            }

            let mut context = aksh_gha_expressions::Context::default();
            context.insert("jobs", serde_json::Value::Object(jobs_map));

            let mut inputs_map = serde_json::Map::new();
            for (k, v) in &call.inputs {
                inputs_map.insert(k.clone(), v.clone());
            }
            context.insert("inputs", serde_json::Value::Object(inputs_map));

            let mut caller_outputs = BTreeMap::new();
            for (name, expr) in &call.output_definitions {
                let resolved = aksh_gha_parser::eval::resolve_string(expr, &context)
                    .unwrap_or_else(|_| expr.clone());
                let val =
                    serde_json::from_str(&resolved).unwrap_or(serde_json::Value::String(resolved));
                caller_outputs.insert(name.clone(), val);
            }

            outputs_to_add.push((caller_job_id_typed, caller_outputs));
        }
    }

    for (job_id, outputs) in outputs_to_add {
        run.job_outputs.insert(job_id, outputs);
    }
}
