use super::*;
use std::collections::BTreeSet;

pub(crate) async fn healthz(State(shared): State<Arc<SharedState>>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shared.shutdown.is_cancelled(),
    }))
}

/// GitHub's `system.orchestrationId`: `{planId}.{jobId}.{suffix}` where the
/// suffix is the 1-based matrix cell index (`build._1`) or `__default` for
/// plain jobs. The official runner emits the value as a User-Agent product
/// token, so it must not contain spaces or other invalid token characters —
/// the job display name ("Run tests with system wide configuration") would
/// crash the worker with `FormatException`.
fn orchestration_id(plan_id: &str, job_id: &str, matrix_index: Option<usize>) -> String {
    match matrix_index {
        Some(index) => format!("{plan_id}.{job_id}._{index}"),
        None => format!("{plan_id}.{job_id}.__default"),
    }
}

/// Interpolate `${{ ... }}` expressions in a workflow run name.
///
/// A malformed expression is left untouched, matching GitHub's behavior of
/// retaining the configured run-name rather than rejecting the run.
fn evaluate_run_name(
    raw: &str,
    github: &serde_json::Value,
    inputs: &BTreeMap<String, serde_json::Value>,
    vars: &BTreeMap<String, String>,
) -> String {
    let mut context = aksh_gha_expressions::Context::default();
    context.insert("github", github.clone());
    context.insert(
        "inputs",
        serde_json::Value::Object(inputs.clone().into_iter().collect()),
    );
    context.insert(
        "vars",
        serde_json::Value::Object(
            vars.iter()
                .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
                .collect(),
        ),
    );

    let Some(_) = raw.find("${{") else {
        return raw.to_owned();
    };
    let mut result = String::with_capacity(raw.len());
    let mut cursor = 0;
    loop {
        let Some(relative_start) = raw[cursor..].find("${{") else {
            result.push_str(&raw[cursor..]);
            break;
        };
        let start = cursor + relative_start;
        result.push_str(&raw[cursor..start]);
        let expression_start = start + 3;
        let Some(relative_end) = raw[expression_start..].find("}}") else {
            return raw.to_owned();
        };
        let expression_end = expression_start + relative_end;
        let value = match aksh_gha_expressions::eval_expression(
            &raw[expression_start..expression_end],
            &context,
        ) {
            Ok(value) => value,
            Err(_) => return raw.to_owned(),
        };
        match value {
            serde_json::Value::String(value) => result.push_str(&value),
            serde_json::Value::Null => {}
            serde_json::Value::Bool(value) => result.push_str(if value { "true" } else { "false" }),
            serde_json::Value::Number(value) => result.push_str(&value.to_string()),
            value => result.push_str(&serde_json::to_string(&value).unwrap_or_default()),
        }
        cursor = expression_end + 2;
    }
    result
}

pub(crate) async fn submit_run_inner(
    shared: &Arc<SharedState>,
    mut submission: WorkflowSubmission,
) -> Result<RunAccepted, ApiError> {
    let workflow = parse_workflow(&submission.workflow_yaml)?;
    crate::remote_workflows::resolve_remote_workflows(&mut submission, &workflow).await?;
    if submission.event == "workflow_dispatch" {
        workflow.apply_workflow_dispatch_inputs(&mut submission.payload)?;
        if submission.dispatch_inputs.is_empty() {
            submission.dispatch_inputs = submission
                .payload
                .get("inputs")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
        }
        if submission.dispatch_inputs_stringified.is_empty() {
            submission.dispatch_inputs_stringified = submission
                .dispatch_inputs
                .iter()
                .map(|(name, value)| (name.clone(), value_to_input_string(value)))
                .collect();
        }
        if let Some(object) = submission.payload.as_object_mut() {
            let inputs_value = if submission.dispatch_inputs_stringified.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::to_value(&submission.dispatch_inputs_stringified).unwrap_or_default()
            };
            object.insert("inputs".to_owned(), inputs_value);
        }
    }
    // Native submissions carry no trust tier (the webhook path sets it) and
    // pass secrets through unmodified — None is therefore trusted. Mirror
    // GitHub repo secrets: stored secrets are available to every trusted
    // job, with submission-provided values winning per name.
    let allow_secrets = submission
        .trust_tier
        .as_deref()
        .and_then(|value| {
            serde_json::from_value::<crate::events::trust_tier::TrustTier>(json!(value)).ok()
        })
        .map(|tier| tier.allows_secrets())
        .unwrap_or(true);
    if !allow_secrets {
        submission.secrets.clear();
    } else {
        // Global secrets first, then per-repository secrets for the
        // submitting repository. Precedence: submission-provided secrets
        // (already in the map) > per-repo tier > global tier — mirroring
        // GitHub, where repo secrets override org secrets of the same name.
        let secret_store = shared.state.secrets.read();
        let submission_names: BTreeSet<String> = submission.secrets.keys().cloned().collect();
        for (name, value) in &secret_store.global {
            submission
                .secrets
                .entry(name.clone())
                .or_insert_with(|| aksh_gha_protocol::SecretString::new(value.clone()));
        }
        if let Some(repo_secrets) = secret_store.repo.get(&submission.repository) {
            for (name, value) in repo_secrets {
                if !submission_names.contains(name) {
                    submission.secrets.insert(
                        name.clone(),
                        aksh_gha_protocol::SecretString::new(value.clone()),
                    );
                }
            }
        }
    }
    let (branch, tag) = {
        let (default_branch, default_tag) = git_ref_context(&submission.git_ref);
        let filter_branch = submission.filter_branch.clone().or_else(|| {
            if matches!(
                submission.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else if submission.event == "workflow_run" {
                submission
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else {
                None
            }
        });
        if filter_branch.is_some() {
            (filter_branch, None)
        } else {
            (default_branch, default_tag)
        }
    };
    let payload_has_paths =
        submission.payload.get("paths").is_some() || submission.payload.get("commits").is_some();
    let changed_paths_known = submission.changed_paths_known || payload_has_paths;
    let changed_paths = if submission.changed_paths_known {
        submission.changed_paths.clone()
    } else {
        changed_paths_from_payload(&submission.payload)
    };
    if !changed_paths_known && workflow.on.has_path_filters(&submission.event) {
        return Err(ApiError::bad_request(
            "workflow path filters require a complete changed-file list".to_owned(),
        ));
    }
    // Activity type from explicit field (set by dispatcher) or payload.action fallback.
    let activity_owned: Option<String> = submission.activity_type.clone().or_else(|| {
        submission
            .payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    });
    let activity_type = activity_owned.as_deref();
    if !workflow.on.matches_with_context(
        &submission.event,
        branch.as_deref(),
        tag.as_deref(),
        &changed_paths,
        activity_type,
        &submission.workflow_run_upstream_names,
    ) {
        return Err(ApiError::bad_request(format!(
            "workflow does not match event `{}`",
            submission.event
        )));
    }
    let expanded = aksh_gha_parser::expand_jobs_with_reusables_and_shas(
        &workflow,
        &submission.reusable_workflows,
        &submission.reusable_workflow_shas,
    )?;
    let mut jobs = expanded.jobs;
    let reusable_calls = expanded.reusable_calls;
    if !submission.dispatch_inputs.is_empty() {
        for job in &mut jobs {
            job.inputs = submission.dispatch_inputs.clone();
        }
    }

    // Filter to selected jobs and their transitive needs: closure.
    if !submission.selected_jobs.is_empty() {
        let pairs: Vec<(String, Vec<String>)> = jobs
            .iter()
            .map(|job| {
                (
                    job.base_id.clone(),
                    job.needs.iter().map(|n| n.0.clone()).collect(),
                )
            })
            .collect();
        // Reject the whole selection if any id is unknown. Silently dropping a
        // typo would run a subset of the requested jobs and report success.
        let mut selected = std::collections::BTreeSet::new();
        let mut known: std::collections::BTreeSet<&str> =
            pairs.iter().map(|(id, _)| id.as_str()).collect();
        for requested in &submission.selected_jobs {
            // A reusable caller is selected by its own (possibly
            // matrix-suffixed) node: its callee subtree is not part of the
            // plan anymore — it materializes at runtime after the gate passes.
            let matched_reusable = reusable_calls.keys().any(|caller| {
                caller.as_str() == requested
                    || caller
                        .strip_prefix(requested)
                        .is_some_and(|suffix| suffix.starts_with(" ("))
            });
            if matched_reusable {
                known.insert(requested);
            }
            selected.insert(requested.clone());
        }
        let unknown: Vec<&str> = submission
            .selected_jobs
            .iter()
            .map(String::as_str)
            .filter(|id| !known.contains(id))
            .collect();
        if !unknown.is_empty() {
            return Err(ApiError::bad_request(format!(
                "unknown job(s) in selected_jobs: {}. available jobs: {}",
                unknown.join(", "),
                known.into_iter().collect::<Vec<_>>().join(", ")
            )));
        }
        let graph = aksh_gha_parser::dag::needs_graph_from_pairs(&pairs);
        let closure = aksh_gha_parser::dag::dependency_closure(
            &graph,
            &selected.into_iter().collect::<Vec<_>>(),
        );
        let before = jobs.len();
        jobs.retain(|job| closure.contains(&job.base_id));
        tracing::info!(
            selected = ?submission.selected_jobs,
            before,
            after = jobs.len(),
            "filtered jobs to dependency closure"
        );
    }

    let run_id = RunId::new();
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();
    let sha = submission
        .resolved_sha
        .clone()
        .or_else(|| {
            submission
                .payload
                .get("after")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            if submission.git_ref.len() == 40
                && submission
                    .git_ref
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
            {
                submission.git_ref.clone()
            } else {
                "0000000000000000000000000000000000000000".to_owned()
            }
        })
        .to_string();
    let workflow_path = submission
        .workflow_path
        .clone()
        .unwrap_or_else(|| ".github/workflows/workflow.yml".to_owned());
    let workflow_ref = format!(
        "{}/{}@{}",
        submission.repository, workflow_path, submission.git_ref
    );

    let ref_name = submission
        .git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| submission.git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(&submission.git_ref)
        .to_owned();
    let ref_type = if submission.git_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        "branch"
    };
    // A pull_request submission is a synthetic PR: GitHub presents the
    // event with `github.ref = refs/pull/<number>/merge`, not the base
    // branch ref. Workflows gate on this (e.g. uv's plan computes
    // `on_main_branch` from `github.ref == refs/heads/main`); leaking the
    // base branch ref makes them treat the PR as a main-branch push and
    // enable every main-only gate.
    let github_ref = if submission.event == "pull_request" {
        let number = submission
            .payload
            .get("number")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("number"))
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or(1);
        format!("refs/pull/{number}/merge")
    } else {
        submission.git_ref.clone()
    };
    let (pr_head_ref, pr_base_ref) = if submission.event == "pull_request" {
        let pr = submission.payload.get("pull_request");
        let head = pr
            .and_then(|pr| pr.get("head"))
            .and_then(|head| head.get("ref"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let base = pr
            .and_then(|pr| pr.get("base"))
            .and_then(|base| base.get("ref"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        (head.to_owned(), base.to_owned())
    } else {
        (
            String::new(),
            submission.base_ref.clone().unwrap_or_default(),
        )
    };

    let mut github = json!({
        "ref": github_ref,
        "sha": sha,
        "repository": submission.repository,
        "repository_owner": repository_owner,
        "repository_owner_id": "0",
        "repositoryUrl": format!("git://github.com/{}.git", submission.repository),
        "run_id": run_id.to_string(),
        "run_number": "1",
        "retention_days": "90",
        "run_attempt": "1",
        "artifact_cache_size_limit": "10",
        "repository_visibility": "private",
        "actor_id": "0",
        "actor": "aksh-system",
        "workflow": workflow.name.clone().unwrap_or_default(),
        "head_ref": pr_head_ref,
        "base_ref": pr_base_ref,
        "event_name": submission.event,
        "server_url": "https://github.com",
        "api_url": "https://api.github.com",
        "graphql_url": "https://api.github.com/graphql",
        "ref_name": ref_name,
        "ref_protected": false,
        "ref_type": ref_type,
        "secret_source": "Actions",
        "event": submission.payload,
        "workflow_ref": workflow_ref,
        "workflow_sha": sha,
        "repository_id": "0",
        "triggering_actor": "aksh-system"
    });

    let run_name = workflow.run_name.as_deref().map(|raw| {
        let inputs = if submission.dispatch_inputs.is_empty() {
            &submission.inputs
        } else {
            &submission.dispatch_inputs
        };
        evaluate_run_name(raw, &github, inputs, &submission.vars)
    });

    // Evaluate workflow-level concurrency before locking (pure).
    let workflow_concurrency = workflow.concurrency.clone();
    let mut empty_workflow_concurrency_group = false;
    let workflow_concurrency_eval = if let Some(raw) = &workflow_concurrency {
        let eval_ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Workflow,
            github: &github,
            vars: &submission.vars,
            inputs: &submission.inputs,
            matrix: None,
            strategy: None,
            needs: None,
        };
        let (group, cancel, queue) =
            concurrency::evaluate_concurrency(raw, &eval_ctx).map_err(|error| {
                ApiError::bad_request(format!("concurrency evaluation failed: {error}"))
            })?;
        if group.trim().is_empty() {
            empty_workflow_concurrency_group = true;
            None
        } else {
            Some((group, cancel, queue, raw.clone()))
        }
    } else {
        None
    };

    // Capture the workspace once per run, before any job is queued. Every
    // redirected checkout then fetches the same immutable local tree.
    let local_workspace = submission
        .local_workspace
        .as_deref()
        .map(std::path::Path::new)
        .or(shared.state.local_workspace.as_deref());
    let workspace_snapshot = if let Some(workspace) = local_workspace {
        match create_workspace_snapshot(&shared.state.state_dir, workspace, run_id).await {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                warn!(%run_id, error = ?error, "Failed to create workspace snapshot — falling back to normal checkout");
                None
            }
        }
    } else {
        None
    };

    // A local submission is a synthetic push/PR against the snapshot. Present
    // the same event shape GitHub would: changed-file actions
    // (`dorny/paths-filter`, `tj-actions/changed-files`) and `actions/checkout`
    // read `payload.repository.default_branch` and `payload.before` to pick
    // their diff base; without them they abort and gate the whole DAG closed.
    if let Some(snapshot) = &workspace_snapshot {
        if let Some(payload) = submission.payload.as_object_mut() {
            let (owner, name) = submission
                .repository
                .split_once('/')
                .map(|(owner, name)| (owner.to_owned(), name.to_owned()))
                .unwrap_or_else(|| ("local".to_owned(), submission.repository.clone()));
            payload.insert(
                "repository".to_owned(),
                serde_json::json!({
                    "name": name,
                    "full_name": submission.repository,
                    "owner": { "login": owner },
                    "default_branch": snapshot.default_branch.clone().unwrap_or_else(|| {
                        submission
                            .git_ref
                            .strip_prefix("refs/heads/")
                            .unwrap_or("main")
                            .to_owned()
                    }),
                }),
            );
            if submission.event == "push" {
                // `after` is the snapshot commit the runner checks out;
                // `before` is the base its changes are measured against (the
                // workspace HEAD when the tree is dirty, HEAD^ when clean — see
                // `WorkspaceSnapshot::before_sha`). An absent base (unborn or
                // initial-commit clean tree) is the null SHA, which GitHub
                // reports as an "initial push" and actions treat as
                // "everything changed".
                payload.insert(
                    "before".to_owned(),
                    snapshot
                        .before_sha
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or_else(|| {
                            serde_json::Value::String(
                                "0000000000000000000000000000000000000000".to_owned(),
                            )
                        }),
                );
                payload.insert("after".to_owned(), serde_json::json!(snapshot.commit_sha));
                payload.insert("ref".to_owned(), serde_json::json!(submission.git_ref));
            } else if submission.event == "pull_request" {
                // Same synthetic-push shape for PR-family events: the head
                // commit the runner checks out is the snapshot commit, and
                // `base.sha` is the base its changes are measured against
                // (the workspace HEAD when the tree is dirty, HEAD^ when
                // clean — see `WorkspaceSnapshot::before_sha`). Changed-file
                // actions (`dorny/paths-filter`, `tj-actions/changed-files`)
                // diff these two SHAs; without the refresh they would diff
                // the caller-supplied head against itself and see nothing.
                let base_sha = snapshot
                    .before_sha
                    .clone()
                    .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
                if let Some(pr) = payload
                    .get_mut("pull_request")
                    .and_then(|v| v.as_object_mut())
                {
                    if let Some(base) = pr.get_mut("base").and_then(|v| v.as_object_mut()) {
                        base.insert("sha".to_owned(), serde_json::json!(base_sha));
                    }
                    if let Some(head) = pr.get_mut("head").and_then(|v| v.as_object_mut()) {
                        head.insert(
                            "sha".to_owned(),
                            serde_json::json!(snapshot
                                .head_sha
                                .clone()
                                .unwrap_or_else(|| snapshot.commit_sha.clone())),
                        );
                    }
                }
            }
        }
        // The github context was built before the snapshot existed; refresh
        // the pieces that now describe the local tree.
        if let Some(object) = github.as_object_mut() {
            object.insert("event".to_owned(), submission.payload.clone());
            // `github.sha` is the workspace's real HEAD commit, not the
            // synthetic snapshot commit: the snapshot commit exists only in
            // this engine's store, so a workflow step that fetches
            // `${{ github.sha }}` from the real remote (custom checkouts)
            // would be answered "not our ref". The workspace HEAD is the
            // identity the run is really based on.
            object.insert(
                "sha".to_owned(),
                serde_json::json!(snapshot
                    .head_sha
                    .clone()
                    .unwrap_or_else(|| snapshot.commit_sha.clone())),
            );
        }
    }

    // PATs are static and can be embedded now. GitHub App installation tokens
    // are deliberately minted later, when the broker dispatches each job, so
    // downstream jobs cannot sit in the queue until a short-lived token
    // expires.
    let mut github_tokens: BTreeMap<JobId, String> = BTreeMap::new();
    if shared.state.github_app.is_none() {
        if let Some(pat) = shared.state.static_github_pat() {
            info!(%run_id, "Using AKSH_GITHUB_TOKEN PAT for run jobs");
            github_tokens.extend(jobs.iter().map(|job| (job.id.clone(), pat.clone())));
        }
    }

    // Reserve the workflow run number before pre-building messages. The
    // message builder consumes the GitHub context, so delaying this counter
    // update until after pre-building would expose "1" for every run.
    let run_number = {
        let mut inner = shared.state.inner.lock().await;
        let counter = inner
            .workflow_run_counters
            .entry(workflow_path.clone())
            .or_insert(0);
        *counter += 1;
        *counter
    };
    if let Some(object) = github.as_object_mut() {
        object.insert(
            "run_number".to_owned(),
            serde_json::json!(run_number.to_string()),
        );
    }

    // ── Pre-build job messages outside the lock ─────────────────────────
    //
    // Condition evaluation, build_agent_job_message, token minting, and
    // snapshot redirect are all pure computations.  Moving them here
    // shrinks the critical section from O(jobs × build_cost) to
    // O(jobs × map_insert).
    let base_url = runner_base_url();
    let normalized_github = aksh_gha_parser::job_builder::normalize_github_context(&github);
    let secrets_exposed: BTreeMap<String, String> =
        aksh_gha_protocol::masking::expose_all(&submission.secrets);

    struct PrebuiltJob {
        job: aksh_gha_protocol::JobPlan,
        agent_msg: Option<aksh_gha_protocol::azdo::AgentJobRequestMessage>,
        request_id: i64,
        condition_context: aksh_gha_expressions::Context,
        skipped: bool,
        caller: bool,
        id_token_granted: bool,
        oidc_ctx: OidcJobContext,
        job_request: Option<TaskAgentJobRequestRecord>,
        github_token_request: Option<GitHubTokenRequest>,
    }

    let mut prebuilt: Vec<PrebuiltJob> = Vec::with_capacity(jobs.len());
    let mut pre_statuses: BTreeMap<JobId, ExecutionStatus> = BTreeMap::new();
    let mut pre_job_base_ids: BTreeMap<JobId, String> = BTreeMap::new();
    let mut pre_job_needs: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
    let mut pre_job_fail_fast: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_job_continue_on_error: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_initially_skipped: Vec<(RunId, JobId)> = Vec::new();
    let mut pre_caller_plans: BTreeMap<JobId, aksh_gha_protocol::JobPlan> = BTreeMap::new();
    let mut pre_job_names: BTreeMap<JobId, String> = BTreeMap::new();

    for job in jobs {
        pre_job_base_ids.insert(job.id.clone(), job.base_id.clone());
        pre_job_needs.insert(job.id.clone(), job.needs.clone());
        pre_job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
        pre_job_continue_on_error.insert(job.id.to_string(), job.continue_on_error);
        pre_statuses.insert(job.id.clone(), ExecutionStatus::Queued);
        pre_job_names.insert(job.id.clone(), job.name.clone());
        if job.reusable_call.is_some() {
            pre_caller_plans.insert(job.id.clone(), job.clone());
        }

        let condition_context = build_context(
            &github,
            &BTreeMap::new(),
            &submission.vars,
            &indexmap::IndexMap::new(),
            &serde_json::json!({}),
            &BTreeMap::new(),
            &job.inputs,
        );

        // Evaluate condition for root jobs (no needs) outside the lock.
        let mut skipped = false;
        if job.needs.is_empty() {
            let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
            let should_run = aksh_gha_expressions::eval_bool(&condition, &condition_context)
                .map_err(|error| {
                    ApiError::bad_request(format!(
                        "failed to evaluate condition for job `{}`: {error}",
                        job.id
                    ))
                })?;
            if !should_run {
                pre_statuses.insert(job.id.clone(), ExecutionStatus::Skipped);
                pre_initially_skipped.push((run_id, job.id.clone()));
                skipped = true;
            }
        }

        if skipped {
            // Still need to record the prebuilt entry so the index stays
            // aligned, but we skip the expensive message build.
            prebuilt.push(PrebuiltJob {
                job,
                agent_msg: None,
                request_id: 0,
                condition_context,
                skipped: true,
                caller: false,
                id_token_granted: false,
                oidc_ctx: OidcJobContext {
                    environment: None,
                    job_workflow_ref: None,
                    job_workflow_sha: None,
                },
                job_request: None,
                github_token_request: None,
            });
            continue;
        }

        let artifacts = build_job_artifacts(
            shared,
            &submission,
            run_id,
            &workflow_path,
            &workflow_ref,
            &sha,
            &normalized_github,
            &secrets_exposed,
            &base_url,
            workspace_snapshot.as_ref(),
            &job,
            github_tokens.remove(&job.id),
        )?;

        prebuilt.push(PrebuiltJob {
            caller: job.reusable_call.is_some(),
            job,
            agent_msg: Some(artifacts.agent_msg),
            request_id: artifacts.request_id,
            condition_context,
            skipped: false,
            id_token_granted: artifacts.id_token_granted,
            oidc_ctx: artifacts.oidc_ctx,
            job_request: Some(artifacts.job_request),
            github_token_request: artifacts.github_token_request,
        });
    }

    {
        let mut inner = shared.state.inner.lock().await;
        let created_at = chrono::Utc::now();
        let event = submission.event.clone();
        let github = github;
        let mut statuses = pre_statuses;
        let caller_plans = pre_caller_plans;
        let job_names = pre_job_names;
        let mut ready_jobs = 0usize;
        let job_base_ids = pre_job_base_ids;
        let job_needs = pre_job_needs;
        let job_fail_fast = pre_job_fail_fast;
        let job_continue_on_error = pre_job_continue_on_error;
        let mut ready_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let initially_skipped = pre_initially_skipped;
        let mut built_jobs: Vec<QueuedJob> = Vec::new();
        if empty_workflow_concurrency_group {
            let queued_jobs = 0;
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    run_name,
                    submission: Arc::new(submission),
                    jobs: BTreeMap::new(),
                    job_outputs: BTreeMap::new(),
                    job_base_ids: BTreeMap::new(),
                    job_needs: BTreeMap::new(),
                    caller_plans: BTreeMap::new(),
                    job_names: BTreeMap::new(),
                    github: serde_json::Value::Null,
                    head_sha: String::new(),
                    workflow_ref: String::new(),
                    workspace_snapshot: None,
                    job_fail_fast: BTreeMap::new(),
                    job_continue_on_error: BTreeMap::new(),
                    status: ExecutionStatus::Failure,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                    created_at,
                    started_at: None,
                    completed_at: Some(created_at),
                    run_number,
                    run_attempt: 1,
                    workflow_path_str: workflow_path.clone(),
                    event: event.clone(),
                    conclusion: Some("failure".to_owned()),
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Failure,
                    reason: Some("concurrency group name must not be empty".to_owned()),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                run_number,
                queued_jobs,
            });
        }
        // ── Install pre-built jobs under the lock (map inserts only) ────
        for pb in prebuilt {
            if pb.skipped {
                continue;
            }
            let job = &pb.job;
            let agent_msg = pb.agent_msg.expect("non-skipped job must have agent_msg");

            if !pb.caller {
                // Caller placeholders are scheduling-only: no runner ever
                // acquires them, so no request correlation records exist.
                let job_request = pb
                    .job_request
                    .expect("non-skipped job must have job_request");

                inner
                    .id_token_grants
                    .insert((run_id, job.id.clone()), pb.id_token_granted);
                inner
                    .oidc_job_contexts
                    .insert((run_id, job.id.clone()), pb.oidc_ctx);

                inner
                    .inflight_requests
                    .insert(job_request.request_id, (run_id, job.id.clone()));
                inner
                    .plan_requests
                    .insert(job_request.plan_id.clone(), pb.request_id);
                inner
                    .agent_job_requests
                    .insert(job_request.agent_job_id, pb.request_id);
                inner
                    .timeline_requests
                    .insert(job_request.timeline_id, pb.request_id);
                inner.job_requests.insert(pb.request_id, job_request);
                if let Some(request) = pb.github_token_request {
                    inner.github_token_requests.insert(pb.request_id, request);
                }
            }

            let queued_job = QueuedJob {
                run_id,
                job_id: job.id.clone(),
                base_id: job.base_id.clone(),
                needs: job.needs.clone(),
                if_condition: job.if_condition.clone(),
                condition_context: pb.condition_context,
                max_parallel: job.max_parallel,
                runs_on: job.runs_on.clone(),
                runner_group: job.runner_group.clone(),
                message: agent_msg,
                concurrency: concurrency::concurrency_from_plan_fields(
                    job.concurrency_group.as_deref(),
                    job.concurrency_cancel_in_progress.as_deref(),
                    job.concurrency_queue.as_deref(),
                ),
                matrix: job
                    .matrix
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                deferred_matrix: job.deferred_matrix.clone(),
                reusable_call: job.reusable_call.clone(),
            };
            built_jobs.push(queued_job);
        }

        // Workflow-level concurrency gate.
        let mut hold_entire_run = false;
        if let Some((group, cancel, queue, raw)) = &workflow_concurrency_eval {
            let key = concurrency::concurrency_key(&submission.repository, group);
            match try_acquire_concurrency(
                &mut inner,
                key,
                group.clone(),
                concurrency::Holder::Run(run_id),
                *cancel,
                *queue,
            ) {
                Ok(true) => {
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Ok(false) => {
                    hold_entire_run = true;
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Err(e) if e == "concurrency_queue_overflow" => {
                    // Cancel this run immediately — all jobs Cancelled.
                    for job in &built_jobs {
                        statuses.insert(job.job_id.clone(), ExecutionStatus::Cancelled);
                    }
                    let queued_jobs = statuses.len();
                    inner.runs.insert(
                        run_id,
                        RunRecord {
                            run_id,
                            run_name,
                            submission: Arc::new(submission),
                            jobs: statuses,
                            job_outputs: BTreeMap::new(),
                            job_base_ids,
                            job_needs,
                            caller_plans: caller_plans.clone(),
                            job_names: job_names.clone(),
                            github: github.clone(),
                            head_sha: sha.clone(),
                            workflow_ref: workflow_ref.clone(),
                            workspace_snapshot: workspace_snapshot.clone(),
                            job_fail_fast,
                            job_continue_on_error,
                            status: ExecutionStatus::Cancelled,
                            job_check_run_ids: BTreeMap::new(),
                            reusable_calls,
                            jobs_list: Vec::new(),
                            created_at,
                            started_at: None,
                            completed_at: Some(created_at),
                            run_number,
                            run_attempt: 1,
                            workflow_path_str: workflow_path.clone(),
                            event: event.clone(),
                            conclusion: Some("cancelled".to_owned()),
                        },
                    );
                    drop(inner);
                    shared
                        .state
                        .emit(NdjsonEvent::RunAccepted {
                            run_id,
                            queued_jobs,
                        })
                        .await;
                    shared
                        .state
                        .emit(NdjsonEvent::RunStatus {
                            run_id,
                            status: ExecutionStatus::Cancelled,
                            reason: concurrency::cancelled_reason(),
                        })
                        .await;
                    return Ok(RunAccepted {
                        run_id,
                        run_number,
                        queued_jobs,
                    });
                }
                Err(e) => {
                    return Err(ApiError::bad_request(e));
                }
            }
        }

        if hold_entire_run {
            for job in &built_jobs {
                statuses.insert(job.job_id.clone(), ExecutionStatus::Pending);
            }
            inner.held_runs.insert(run_id, built_jobs);
            let queued_jobs = statuses.len();
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    run_name,
                    submission: Arc::new(submission),
                    jobs: statuses,
                    job_outputs: BTreeMap::new(),
                    job_base_ids,
                    job_needs,
                    caller_plans: caller_plans.clone(),
                    job_names: job_names.clone(),
                    github: github.clone(),
                    head_sha: sha.clone(),
                    workflow_ref: workflow_ref.clone(),
                    workspace_snapshot: workspace_snapshot.clone(),
                    job_fail_fast,
                    job_continue_on_error,
                    status: ExecutionStatus::Pending,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                    created_at,
                    started_at: None,
                    completed_at: None,
                    run_number,
                    run_attempt: 1,
                    workflow_path_str: workflow_path.clone(),
                    event: event.clone(),
                    conclusion: None,
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Pending,
                    reason: concurrency::pending_reason(),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                run_number,
                queued_jobs,
            });
        }
        // Install a provisional run before evaluating per-job and JobSet gates.
        // Multiple holders from this same submission can cancel each other;
        // cancellation helpers need the run to exist so they can persist the
        // affected job conclusion instead of silently becoming no-ops.
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                run_name: run_name.clone(),
                submission: Arc::new(submission.clone()),
                jobs: statuses.clone(),
                job_outputs: BTreeMap::new(),
                job_base_ids: job_base_ids.clone(),
                job_needs: job_needs.clone(),
                caller_plans: caller_plans.clone(),
                job_names: job_names.clone(),
                github: github.clone(),
                head_sha: sha.clone(),
                workflow_ref: workflow_ref.clone(),
                workspace_snapshot: workspace_snapshot.clone(),
                job_fail_fast: job_fail_fast.clone(),
                job_continue_on_error: job_continue_on_error.clone(),
                status: ExecutionStatus::Queued,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls: reusable_calls.clone(),
                jobs_list: Vec::new(),
                created_at,
                started_at: None,
                completed_at: None,
                run_number,
                run_attempt: 1,
                workflow_path_str: workflow_path.clone(),
                event: event.clone(),
                conclusion: None,
            },
        );

        // Enqueue jobs (workflow concurrency free / acquired).
        for queued_job in built_jobs {
            let job_id = queued_job.job_id.clone();
            let base_id = queued_job.base_id.clone();

            // Deferred reusable-caller nodes are scheduling-only: they wait in
            // pending_jobs until their `if:` gate passes, when the scheduler
            // acquires caller/embedded JobSet concurrency gates and expands
            // the callee subtree (mirroring GitHub, which evaluates caller
            // concurrency when the caller job starts).
            if queued_job.reusable_call.is_some() {
                statuses.insert(job_id, ExecutionStatus::Pending);
                inner.pending_jobs.push_back(queued_job);
                continue;
            }

            let needs_empty = queued_job.needs.is_empty();
            let max_parallel = queued_job.max_parallel;
            let under_mp = max_parallel
                .is_none_or(|max| ready_by_base.get(&base_id).copied().unwrap_or(0) < max);

            if needs_empty && under_mp {
                // Job-level concurrency gate (needs/max_parallel already satisfied).
                match try_enqueue_with_job_concurrency(
                    &mut inner,
                    &github,
                    &submission,
                    queued_job,
                    &mut statuses,
                ) {
                    Ok(true) => {
                        *ready_by_base.entry(base_id).or_default() += 1;
                        ready_jobs += 1;
                    }
                    Ok(false) => {
                        // parked pending
                    }
                    Err(_) => {
                        // cancelled by queue overflow or eval failure already marked
                    }
                }
            } else {
                statuses.insert(job_id, ExecutionStatus::Queued);
                inner.pending_jobs.push_back(queued_job);
            }
        }

        // Preserve terminal conclusions written through cancel_job_inner while
        // gates were evaluated. Non-terminal scheduling state remains owned by
        // the local status map and is installed below with the final record.
        if let Some(provisional) = inner.runs.get(&run_id) {
            for (job_id, status) in &provisional.jobs {
                if status.is_terminal() {
                    statuses.insert(job_id.clone(), *status);
                }
            }
        }

        let queued_jobs = statuses.len();
        // C-05: derive the initial run status from job statuses so that eval
        // failures (Failure) are reflected immediately rather than leaving the
        // run permanently Queued. summarize_run returns InProgress for any mix
        // of Queued/Pending jobs; map that to Queued since no job has started.
        let initial_status = {
            let s = summarize_run(statuses.values().copied());
            if s == ExecutionStatus::InProgress {
                ExecutionStatus::Queued
            } else {
                s
            }
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                run_name,
                submission: Arc::new(submission),
                jobs: statuses,
                job_outputs: BTreeMap::new(),
                job_base_ids,
                job_needs,
                caller_plans,
                job_names,
                github,
                head_sha: sha,
                workflow_ref,
                workspace_snapshot,
                job_fail_fast,
                job_continue_on_error,
                status: initial_status,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls,
                jobs_list: Vec::new(),
                created_at,
                started_at: None,
                completed_at: None,
                run_number,
                run_attempt: 1,
                workflow_path_str: workflow_path.clone(),
                event: event.clone(),
                conclusion: None,
            },
        );
        // Deferred reusable-caller nodes whose needs are already satisfied
        // (typically none) are reified by a first promote sweep: needs-free
        // callers acquire their JobSet gates and materialize their callee
        // subtree immediately.
        promote_ready_jobs(&mut inner);
        // The on-demand runner supervisor uses this atomic as its wake-up
        // signal. Refresh it when submission makes work runnable; updating it
        // only after a runner claims a job leaves a size-zero pool asleep
        // forever on the first webhook-created run.
        shared
            .state
            .queue_depth
            .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
        let cancel_count = inner.cancellation_queue.len();
        drop(inner);
        // The sweep above only recorded the intent to expand; the subtree build
        // runs here with the lock released.
        let expansion = drain_expansions(shared).await;
        if ready_jobs > 0 || cancel_count > 0 || expansion.promoted > 0 {
            shared.state.message_notify.notify_waiters();
        }
        for (event_run_id, job_id) in initially_skipped {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id: event_run_id,
                    job_id,
                    status: ExecutionStatus::Skipped,
                    reason: None,
                })
                .await;
        }
        shared
            .state
            .emit(NdjsonEvent::RunAccepted {
                run_id,
                queued_jobs,
            })
            .await;
        Ok(RunAccepted {
            run_id,
            run_number,
            queued_jobs,
        })
    }
}
pub(crate) async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(mut submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    if let Some(encoded) = headers
        .get("x-preloop-local-workspace")
        .and_then(|value| value.to_str().ok())
    {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ApiError::bad_request("invalid local workspace header"))?;
        submission.local_workspace = Some(
            String::from_utf8(bytes)
                .map_err(|_| ApiError::bad_request("local workspace path is not UTF-8"))?,
        );
    }
    submit_run_inner(&shared, submission).await.map(Json)
}

pub(crate) async fn get_scheduler_history(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<Vec<crate::scheduler::ScheduleFire>>, ApiError> {
    if let Some(scheduler) = &shared.state.scheduler {
        let history = scheduler.history.lock().await.clone();
        Ok(Json(history))
    } else {
        Ok(Json(vec![]))
    }
}

pub(crate) fn value_to_input_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => value.to_string(),
    }
}

pub(crate) fn git_ref_context(git_ref: &str) -> (Option<String>, Option<String>) {
    if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
        (Some(branch.to_owned()), None)
    } else if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
        (None, Some(tag.to_owned()))
    } else {
        (None, None)
    }
}

pub(crate) fn changed_paths_from_payload(payload: &serde_json::Value) -> Vec<String> {
    let mut paths = Vec::new();

    if let Some(values) = payload.get("paths").and_then(|value| value.as_array()) {
        collect_string_array(values, &mut paths);
    }

    if let Some(commits) = payload.get("commits").and_then(|value| value.as_array()) {
        for commit in commits {
            for field in ["added", "modified", "removed"] {
                if let Some(values) = commit.get(field).and_then(|value| value.as_array()) {
                    collect_string_array(values, &mut paths);
                }
            }
        }
    }

    paths.sort();
    paths.dedup();
    paths
}

pub(crate) fn collect_string_array(values: &[serde_json::Value], out: &mut Vec<String>) {
    out.extend(
        values
            .iter()
            .filter_map(|value| value.as_str())
            .map(str::to_owned),
    );
}

/// Per-job runner artifacts: agent message plus the correlation records the
/// broker and results/timeline services use to track the delivered request.
pub(crate) struct BuiltJobArtifacts {
    pub(crate) agent_msg: azdo::AgentJobRequestMessage,
    pub(crate) request_id: i64,
    pub(crate) job_request: TaskAgentJobRequestRecord,
    pub(crate) id_token_granted: bool,
    pub(crate) oidc_ctx: OidcJobContext,
    pub(crate) github_token_request: Option<GitHubTokenRequest>,
}

/// Build one job's runner message and correlation records.
///
/// Pure computation shared by the submission prebuild and the scheduler's
/// runtime expansion of reusable-workflow callee subtrees (which cannot be
/// built at submission: they exist only after the caller's `if:` gate passes).
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_job_artifacts(
    shared: &SharedState,
    submission: &WorkflowSubmission,
    run_id: RunId,
    workflow_path: &str,
    workflow_ref: &str,
    sha: &str,
    normalized_github: &serde_json::Value,
    secrets_exposed: &BTreeMap<String, String>,
    base_url: &str,
    workspace_snapshot: Option<&WorkspaceSnapshot>,
    job: &aksh_gha_protocol::JobPlan,
    github_token_override: Option<String>,
) -> Result<BuiltJobArtifacts, ApiError> {
    let mut agent_msg =
        aksh_gha_parser::job_builder::build_agent_job_message_with_normalized_context(
            job,
            normalized_github,
            &job.env,
            secrets_exposed,
            &submission.vars,
        )
        .map_err(|e| ApiError::bad_request(format!("failed to build job message: {e}")))?;

    agent_msg.preloop_preserve_on_failure = submission.preserve_on_failure.then_some(true);

    // Pre-allocate request ID atomically (no lock needed).
    let request_id = shared
        .state
        .next_request_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    agent_msg.request_id = request_id;

    // Mint tokens outside the lock (HMAC computation).
    let runtime_token = shared
        .state
        .mint_runtime_token(&agent_msg.plan.plan_id, &agent_msg.job_id);

    if let Some(snapshot) = workspace_snapshot {
        let redirected =
            redirect_primary_checkout(&mut agent_msg, snapshot, base_url, &runtime_token);
        if redirected > 0 {
            info!(
                %run_id,
                job = %job.id,
                %redirected,
                commit = %snapshot.commit_sha,
                "Redirected primary checkout to local workspace snapshot"
            );
            agent_msg.aksh_snapshot_commit = Some(snapshot.commit_sha.clone());
        }
    }

    let id_token_granted = job.oidc_id_token_granted;
    if id_token_granted {
        let oidc_url = format!(
            "{}/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?api-version=2.0",
            base_url,
            agent_msg.plan.plan_id,
            agent_msg.job_id,
        );
        for endpoint in &mut agent_msg.resources.endpoints {
            if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                endpoint
                    .data
                    .insert("GenerateIdTokenUrl".to_owned(), oidc_url.clone());
            }
        }
    }

    let github_token = github_token_override.unwrap_or_else(|| runtime_token.clone());
    agent_msg.variables.insert(
        "system.github.token".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::secret(github_token.clone()),
    );
    agent_msg.variables.insert(
        "github_token".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::secret(github_token),
    );
    agent_msg.variables.insert(
        "actions_runner_allow_artifacts_file".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::new("false"),
    );
    agent_msg.variables.insert(
        "actions_self_repository".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::new("true"),
    );
    // The debug-worker token is deliberately not a job variable. An
    // official runner copies every `isSecret` variable into the `secrets`
    // context, so shipping it here would publish it to workflow YAML as
    // `${{ secrets['system.preloop.debug_worker_token'] }}`. The worker
    // acquires it instead over `POST /api/v1/debug/worker-token`, which
    // authenticates against this job's runtime token.
    agent_msg.variables.insert(
        "system.github.launch_endpoint".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::new(base_url),
    );
    agent_msg.variables.insert(
        "system.github.results_endpoint".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::new(base_url),
    );
    agent_msg.variables.insert(
        "system.orchestrationId".to_owned(),
        aksh_gha_protocol::azdo::VariableValue::new(orchestration_id(
            &agent_msg.plan.plan_id,
            &job.base_id,
            job.matrix_index,
        )),
    );
    agent_msg.file_table = vec![workflow_path.to_owned()];
    if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(job_dict)) =
        agent_msg.context_data.get_mut("job")
    {
        job_dict.insert(
            "check_run_id".to_owned(),
            aksh_gha_protocol::azdo::PipelineContextData::Number(0.0),
        );
        job_dict.insert(
            "workflow_ref".to_owned(),
            aksh_gha_protocol::azdo::PipelineContextData::String(workflow_ref.to_owned()),
        );
        job_dict.insert(
            "workflow_sha".to_owned(),
            aksh_gha_protocol::azdo::PipelineContextData::String(sha.to_owned()),
        );
        job_dict.insert(
            "workflow_repository".to_owned(),
            aksh_gha_protocol::azdo::PipelineContextData::String(submission.repository.clone()),
        );
        job_dict.insert(
            "workflow_file_path".to_owned(),
            aksh_gha_protocol::azdo::PipelineContextData::String(workflow_path.to_owned()),
        );
    }
    agent_msg.enable_debugger = submission.enable_debugger;
    agent_msg.debugger_welcome_message = submission.debugger_welcome_message.clone();
    if submission.enable_debugger || submission.preserve_on_failure {
        agent_msg.aksh_debug_run_id = Some(run_id.to_string());
        agent_msg.aksh_debug_transport = Some("local".to_string());
    }

    let oidc_ctx = OidcJobContext {
        environment: job.oidc_environment.clone(),
        job_workflow_ref: job.oidc_job_workflow_ref.clone(),
        job_workflow_sha: job.workflow_sha.clone(),
    };

    let job_request = TaskAgentJobRequestRecord {
        request_id,
        run_id,
        job_id: job.id.clone(),
        agent_job_id: agent_msg.job_id,
        plan_id: agent_msg.plan.plan_id.clone(),
        plan_type: agent_msg.plan.plan_type.clone(),
        timeline_id: agent_msg.timeline.id,
        result: None,
        locked_until: agent_request_locked_until(),
        started_at: None,
        last_renewed_at: None,
        timeout_triggered: false,
        debug_token_issued: false,
    };
    let github_token_request = shared
        .state
        .github_app
        .as_ref()
        .map(|_| GitHubTokenRequest {
            repository: submission.repository.clone(),
            permissions: aksh_gha_parser::effective_token_permissions(job.permissions.as_ref())
                .into_owned(),
            declared: job.permissions.is_some(),
        });

    Ok(BuiltJobArtifacts {
        agent_msg,
        request_id,
        job_request,
        id_token_granted,
        oidc_ctx,
        github_token_request,
    })
}

pub(crate) async fn get_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let mut run = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    // GitHub's run record shows a gate-passed reusable caller only as its
    // callee jobs: once the subtree is materialized, the caller entry leaves
    // the visible job set. Gate-failed callers never materialize and stay as
    // exactly one (skipped) entry.
    let expanded_callers: std::collections::BTreeSet<&str> = run
        .reusable_calls
        .iter()
        .filter(|(_, call)| !call.inner_job_ids.is_empty())
        .map(|(caller_id, _)| caller_id.as_str())
        .collect();
    run.jobs
        .retain(|job_id, _| !expanded_callers.contains(job_id.0.as_str()));

    // Project with GitHub display names (evaluated `name:`, `caller / callee`
    // separator). Results/timeline updates only create details for dispatched
    // jobs; jobs skipped or cancelled before dispatch get an empty step list.
    let existing = std::mem::take(&mut run.jobs_list);
    run.jobs_list = run
        .jobs
        .iter()
        .map(|(job_id, status)| {
            let name = run
                .job_names
                .get(job_id)
                .cloned()
                .unwrap_or_else(|| job_id.0.clone());
            let mut detail = existing
                .iter()
                .find(|detail| detail.name == job_id.0 || detail.name == name)
                .cloned()
                .unwrap_or(JobDetail {
                    name: name.clone(),
                    conclusion: status_string(*status),
                    steps: Vec::new(),
                    annotations: Vec::new(),
                });
            detail.name = name;
            if let Some(stored) = run.jobs.get(job_id) {
                detail.conclusion = status_string(*stored);
            }
            detail
        })
        .collect();

    Ok(Json(run))
}

/// Browser-safe status page linked from GitHub Check Runs.
///
/// This deliberately projects only execution metadata. The native run response
/// remains bearer-protected because it contains the submitted event payload and
/// secret names.
pub(crate) async fn get_public_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<axum::response::Html<String>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    let jobs = run
        .jobs
        .iter()
        .map(|(job, status)| {
            format!(
                "<li><code>{}</code> <strong>{}</strong></li>",
                escape_html(&job.0),
                escape_html(&status_string(*status))
            )
        })
        .collect::<String>();
    let status = escape_html(&status_string(run.status));
    let workflow = escape_html(&run.workflow_path_str);
    let id = escape_html(&run.run_id.to_string());
    let html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <meta name=\"robots\" content=\"noindex,nofollow\">\
         <title>Preloop run {id}</title></head><body>\
         <main><h1>Preloop run</h1><p><code>{id}</code></p>\
         <p>Workflow: <code>{workflow}</code></p>\
         <p>Status: <strong>{status}</strong></p><h2>Jobs</h2><ul>{jobs}</ul>\
         </main></body></html>"
    );
    Ok(axum::response::Html(html))
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListRunsQuery {
    #[serde(default)]
    workflow: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default, deserialize_with = "deserialize_limit")]
    limit: Option<usize>,
}

fn deserialize_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    Option::<usize>::deserialize(deserializer)
}

pub(crate) async fn list_runs(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let limit = query.limit.unwrap_or(50).min(200);

    let runs: Vec<RunRecord> = inner
        .runs
        .values()
        .rev()
        .filter(|run| {
            if let Some(workflow) = &query.workflow {
                if !run.workflow_path_str.contains(workflow) {
                    return false;
                }
            }
            if let Some(status) = &query.status {
                let run_status = serde_json::to_value(run.status)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default();
                if run_status != *status {
                    return false;
                }
            }
            if let Some(event) = &query.event {
                if run.event != *event {
                    return false;
                }
            }
            true
        })
        .take(limit)
        .cloned()
        .collect();

    Ok(Json(runs))
}

pub(crate) async fn get_run_logs(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    let (state_dir, sources) = {
        let inner = shared.state.inner.lock().await;
        if !inner.runs.contains_key(&run_id) {
            return Err(ApiError::not_found("run not found"));
        }

        let mut requests: Vec<&TaskAgentJobRequestRecord> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        let sources = requests
            .into_iter()
            .map(|request| {
                let prefix = format!("{}/", request.plan_id);
                let mut blocks: Vec<(&str, &[u8])> = inner
                    .logs
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .map(|log_id| (log_id, value.as_slice()))
                    })
                    .collect();
                blocks.sort_by(|(left, _), (right, _)| {
                    match (left.parse::<u64>(), right.parse::<u64>()) {
                        (Ok(left), Ok(right)) => left.cmp(&right),
                        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                        (Err(_), Err(_)) => left.cmp(right),
                    }
                });
                (
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                    blocks
                        .into_iter()
                        .map(|(_, block)| block.to_vec())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        (shared.state.state_dir.clone(), sources)
    };

    let mut merged = Vec::new();
    for (plan_id, agent_job_id, fallback_blocks) in sources {
        let results_log = state_dir
            .join("replay")
            .join("results")
            .join(plan_id)
            .join(agent_job_id)
            .join("job-logs.txt");
        match tokio::fs::read(&results_log).await {
            Ok(contents) => merged.extend_from_slice(&contents),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for block in fallback_blocks {
                    merged.extend_from_slice(&block);
                }
            }
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "failed to read run log `{}`: {error}",
                    results_log.display()
                )));
            }
        }
    }

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(merged))
        .expect("static run log response"))
}

pub(crate) async fn cancel_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    if !inner.runs.contains_key(&run_id) {
        return Err(ApiError::not_found("run not found"));
    }
    let cancellation_count =
        cancel_run_inner(&mut inner, run_id, None /* no concurrency reason */);
    let record = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    drop(inner);
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Cancelled,
            reason: None,
        })
        .await;
    Ok(Json(record))
}
pub(crate) async fn rerun_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunAccepted>, ApiError> {
    let submission = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .map(|run| (*run.submission).clone())
            .ok_or_else(|| ApiError::not_found("run not found"))?
    };
    submit_run_inner(&shared, submission).await.map(Json)
}

/// Upper bound on how long an event stream waits for the next event.
///
/// A run that stalls must not pin a connection forever; clients reconnect and
/// re-receive the snapshot, so closing here costs only a round trip.
const EVENT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// NDJSON event feed for a run: a snapshot of everything so far, then live
/// events until the run reaches a terminal status.
///
/// Holding the response open is what keeps `preloop run` off a poll timer —
/// snapshot-and-close forced clients to re-request on an interval, which added
/// that interval to every run's wall clock.
pub(crate) async fn run_events(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    // Subscribe before snapshotting so nothing emitted in between is lost. The
    // overlap can replay a line the snapshot already carried; clients
    // de-duplicate, and applying a status twice is idempotent.
    let receiver = shared.state.events.subscribe();

    let (snapshot, settled) = {
        let inner = shared.state.inner.lock().await;
        let run = inner
            .runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let mut out = event_to_ndjson(&NdjsonEvent::RunStatus {
            run_id,
            status: run.status,
            reason: None,
        })?;
        for (job_id, status) in &run.jobs {
            out.push_str(&event_to_ndjson(&NdjsonEvent::JobStatus {
                run_id,
                job_id: job_id.clone(),
                status: *status,
                reason: None,
            })?);
        }
        if let Some(events) = inner.timeline_events.get(&run_id) {
            for event in events {
                out.push_str(&event_to_ndjson(event)?);
            }
        }
        (out, run.status.is_terminal())
    };

    let body = if settled {
        Body::from(snapshot)
    } else {
        let head = stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(snapshot)) });
        Body::from_stream(head.chain(live_run_events(run_id, receiver)))
    };

    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(body)
        .expect("static response builder"))
}

/// Live tail of a run's events, ending once the run status turns terminal.
fn live_run_events(
    run_id: RunId,
    receiver: broadcast::Receiver<NdjsonEvent>,
) -> impl stream::Stream<Item = Result<Bytes, std::io::Error>> {
    stream::unfold(
        (receiver, false),
        move |(mut receiver, finished)| async move {
            if finished {
                return None;
            }
            // One deadline for the whole filtering loop, not one per `recv`.
            // The broadcast channel carries every run's events, so a per-`recv`
            // timeout is refreshed by traffic belonging to other runs and then
            // discarded by the run-id check below; on a busy server a stalled
            // run would hold its connection open forever. Anchoring the
            // deadline before the loop keeps "idle" meaning "nothing delivered
            // to *this* client", which is what the bound is for, and it reads
            // more directly than tracking whether the last event matched.
            let deadline = tokio::time::Instant::now() + EVENT_STREAM_IDLE_TIMEOUT;
            loop {
                let event = match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Ok(Ok(event)) => event,
                    // A lagging consumer has an incomplete stream. End it
                    // so the client reconnects and receives a fresh,
                    // authoritative snapshot before tailing again.
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => return None,
                    Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => return None,
                };
                if event.run_id() != run_id {
                    continue;
                }
                let Ok(line) = event_to_ndjson(&event) else {
                    continue;
                };
                let finished = event.terminal_run_status().is_some();
                return Some((Ok(Bytes::from(line)), (receiver, finished)));
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn orchestration_id_matches_github_format() {
        // Plain job: `{planId}.{jobId}.__default` (golden:
        // 49f720db-...hello.__default)
        assert_eq!(
            orchestration_id("49f720db-d368-4a3a-8b97-adbc8733aa79", "hello", None),
            "49f720db-d368-4a3a-8b97-adbc8733aa79.hello.__default"
        );
        // Matrix cells: 1-based index suffix (golden: build._1/_2/_3)
        assert_eq!(
            orchestration_id("37e6d806-40ab-4d76-92bd-7f6b0c91c002", "build", Some(2)),
            "37e6d806-40ab-4d76-92bd-7f6b0c91c002.build._2"
        );
        // The value must be a valid User-Agent product token: the official
        // runner inserts it into `ProductInfoHeaderValue`, which throws
        // FormatException on spaces (e.g. a display name like
        // "Run tests with system wide configuration").
        assert!(!orchestration_id("p", "j", None).contains(' '));
        assert!(!orchestration_id("p", "j", Some(1)).contains(' '));
    }

    /// The broadcast channel fans out every run's events, so a stalled run's
    /// stream sees — and discards — traffic it must not treat as liveness.
    /// Before the deadline was hoisted out of the filtering loop, that traffic
    /// refreshed the idle bound and the connection leaked for as long as the
    /// server stayed busy.
    #[tokio::test(start_paused = true)]
    async fn stalled_run_event_stream_ends_on_its_idle_deadline_despite_other_run_traffic() {
        let stalled = RunId::new();
        let noisy = RunId::new();
        let (sender, receiver) = broadcast::channel(64);
        let stream = live_run_events(stalled, receiver);
        tokio::pin!(stream);

        let started = tokio::time::Instant::now();
        // Emit for the other run more often than the idle bound while the clock
        // walks past it, which is the busy-server shape that hid the leak.
        let mut ended = false;
        for _ in 0..30 {
            sender
                .send(NdjsonEvent::RunStatus {
                    run_id: noisy,
                    status: ExecutionStatus::InProgress,
                    reason: None,
                })
                .expect("stream holds the receiver alive");
            // Polled rather than awaited on purpose: awaiting would let the
            // paused clock auto-advance to the next timer, which would end the
            // stream even under the per-event timeout this test rules out.
            match stream.next().now_or_never() {
                None => {}
                Some(None) => {
                    ended = true;
                    break;
                }
                Some(Some(_)) => panic!("another run's event must not be yielded to this stream"),
            }
            tokio::time::advance(EVENT_STREAM_IDLE_TIMEOUT / 3).await;
        }

        assert!(
            ended,
            "stalled stream stayed open while other runs kept the channel busy"
        );
        assert!(
            started.elapsed() < EVENT_STREAM_IDLE_TIMEOUT * 2,
            "stream outlived its idle deadline by more than a full bound"
        );
    }
}
