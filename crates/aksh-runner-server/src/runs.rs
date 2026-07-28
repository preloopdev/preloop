use super::*;

pub(crate) async fn healthz(State(shared): State<Arc<SharedState>>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shared.shutdown.is_cancelled(),
    }))
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
    crate::remote_workflows::resolve_remote_workflows(&mut submission).await?;
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
    if let Some(tier) = submission.trust_tier.as_deref().and_then(|value| {
        serde_json::from_value::<crate::events::trust_tier::TrustTier>(json!(value)).ok()
    }) {
        if !tier.allows_secrets() {
            submission.secrets.clear();
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
        let known: std::collections::BTreeSet<&str> =
            pairs.iter().map(|(id, _)| id.as_str()).collect();
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
        let closure = aksh_gha_parser::dag::dependency_closure(&graph, &submission.selected_jobs);
        let before = jobs.len();
        jobs.retain(|job| closure.contains(&job.base_id));
        tracing::info!(
            selected = ?submission.selected_jobs,
            before,
            after = jobs.len(),
            "filtered jobs to dependency closure"
        );
    }

    let reusable_calls = expanded.reusable_calls;
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

    let mut github = json!({
        "ref": submission.git_ref,
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
        "head_ref": "",
        "base_ref": submission.base_ref.as_deref().unwrap_or(""),
        "event_name": submission.event,
        "server_url": "https://github.com",
        "api_url": "https://api.github.com",
        "graphql_url": "https://api.github.com/graphql",
        "ref_name": ref_name,
        "ref_protected": false,
        "ref_type": ref_type,
        "secret_source": "Actions",
        // Public-repository workflow_dispatch defaults observed from the
        // official runner setup log. The worker uses this context to emit the
        // same GITHUB_TOKEN Permissions group before user steps.
        "token_permissions": {
            "contents": "read",
            "metadata": "read",
            "packages": "read"
        },
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

    // Resolve the real GitHub credential for each job *before* taking the
    // dispatch lock: an installation-token round-trip must never stall every
    // other run behind `state.inner`. A job with no entry here falls back to
    // the local HMAC JWT minted inside the loop, so the effective order stays
    // GitHub App > PAT > local JWT.
    let mut github_tokens: BTreeMap<JobId, String> = BTreeMap::new();
    if let Some(app) = &shared.state.github_app {
        for job in &jobs {
            match crate::github_app::get_or_mint_token(
                app,
                &repository_owner,
                job.permissions.as_ref(),
            )
            .await
            {
                Ok(token) => {
                    info!(
                        %run_id,
                        job = %job.id,
                        owner = %repository_owner,
                        "Minted GitHub App installation token for job"
                    );
                    github_tokens.insert(job.id.clone(), token);
                }
                Err(error) => {
                    warn!(
                        %run_id,
                        job = %job.id,
                        owner = %repository_owner,
                        "GitHub App token minting failed, falling back: {error:#}"
                    );
                    if let Ok(pat) = std::env::var("AKSH_GITHUB_TOKEN") {
                        github_tokens.insert(job.id.clone(), pat);
                    }
                }
            }
        }
    } else if let Ok(pat) = std::env::var("AKSH_GITHUB_TOKEN") {
        info!(%run_id, "Using AKSH_GITHUB_TOKEN PAT for run jobs");
        github_tokens.extend(jobs.iter().map(|job| (job.id.clone(), pat.clone())));
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
    let base_url = public_base_url();
    let secrets_exposed: BTreeMap<String, String> = submission
        .secrets
        .iter()
        .map(|(k, v)| (k.clone(), v.expose().to_owned()))
        .collect();

    struct PrebuiltJob {
        job: aksh_gha_protocol::JobPlan,
        agent_msg: Option<aksh_gha_protocol::azdo::AgentJobRequestMessage>,
        request_id: i64,
        condition_context: aksh_gha_expressions::Context,
        skipped: bool,
        id_token_granted: bool,
        oidc_ctx: OidcJobContext,
        job_request: Option<TaskAgentJobRequestRecord>,
    }

    let mut prebuilt: Vec<PrebuiltJob> = Vec::with_capacity(jobs.len());
    let mut pre_statuses: BTreeMap<JobId, ExecutionStatus> = BTreeMap::new();
    let mut pre_job_base_ids: BTreeMap<JobId, String> = BTreeMap::new();
    let mut pre_job_needs: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
    let mut pre_job_fail_fast: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_job_continue_on_error: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_initially_skipped: Vec<(RunId, JobId)> = Vec::new();

    for job in jobs {
        pre_job_base_ids.insert(job.id.clone(), job.base_id.clone());
        pre_job_needs.insert(job.id.clone(), job.needs.clone());
        pre_job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
        pre_job_continue_on_error.insert(job.id.to_string(), job.continue_on_error);
        pre_statuses.insert(job.id.clone(), ExecutionStatus::Queued);

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
                id_token_granted: false,
                oidc_ctx: OidcJobContext {
                    environment: None,
                    job_workflow_ref: None,
                    job_workflow_sha: None,
                },
                job_request: None,
            });
            continue;
        }

        // Heavy work: build the agent message outside the lock.
        let mut agent_msg = aksh_gha_parser::job_builder::build_agent_job_message(
            &job,
            &github,
            &job.env,
            &secrets_exposed,
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

        if let Some(snapshot) = workspace_snapshot.as_ref() {
            let redirected =
                redirect_primary_checkout(&mut agent_msg, snapshot, &base_url, &runtime_token);
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

        let github_token = github_tokens
            .remove(&job.id)
            .unwrap_or_else(|| runtime_token.clone());
        agent_msg.variables.insert(
            "system.github.token".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::secret(github_token.clone()),
        );
        agent_msg.variables.insert(
            "github_token".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::secret(github_token),
        );
        let debug_token = shared
            .state
            .mint_debug_worker_token(&agent_msg.plan.plan_id, &agent_msg.job_id);
        agent_msg.variables.insert(
            "system.preloop.debug_worker_token".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::secret(debug_token),
        );
        agent_msg.variables.insert(
            "system.github.launch_endpoint".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::new(base_url.clone()),
        );
        agent_msg.variables.insert(
            "system.github.results_endpoint".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::new(base_url.clone()),
        );
        agent_msg.variables.insert(
            "system.orchestrationId".to_owned(),
            aksh_gha_protocol::azdo::VariableValue::new(format!(
                "{}.{}.{}",
                agent_msg.plan.plan_id, job.base_id, agent_msg.job_name
            )),
        );
        agent_msg.file_table = vec![workflow_path.clone()];
        if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(job_dict)) =
            agent_msg.context_data.get_mut("job")
        {
            job_dict.insert(
                "check_run_id".to_owned(),
                aksh_gha_protocol::azdo::PipelineContextData::Number(0.0),
            );
            job_dict.insert(
                "workflow_ref".to_owned(),
                aksh_gha_protocol::azdo::PipelineContextData::String(workflow_ref.clone()),
            );
            job_dict.insert(
                "workflow_sha".to_owned(),
                aksh_gha_protocol::azdo::PipelineContextData::String(sha.clone()),
            );
            job_dict.insert(
                "workflow_repository".to_owned(),
                aksh_gha_protocol::azdo::PipelineContextData::String(submission.repository.clone()),
            );
            job_dict.insert(
                "workflow_file_path".to_owned(),
                aksh_gha_protocol::azdo::PipelineContextData::String(workflow_path.clone()),
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
        };

        prebuilt.push(PrebuiltJob {
            job,
            agent_msg: Some(agent_msg),
            request_id,
            condition_context,
            skipped: false,
            id_token_granted,
            oidc_ctx,
            job_request: Some(job_request),
        });
    }

    {
        let mut inner = shared.state.inner.lock().await;
        let created_at = chrono::Utc::now();
        let event = submission.event.clone();
        let github = github;
        let mut statuses = pre_statuses;
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
                    submission,
                    jobs: BTreeMap::new(),
                    job_outputs: BTreeMap::new(),
                    job_base_ids: BTreeMap::new(),
                    job_needs: BTreeMap::new(),
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
                            submission,
                            jobs: statuses,
                            job_outputs: BTreeMap::new(),
                            job_base_ids,
                            job_needs,
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
                    submission,
                    jobs: statuses,
                    job_outputs: BTreeMap::new(),
                    job_base_ids,
                    job_needs,
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
                submission: submission.clone(),
                jobs: statuses.clone(),
                job_outputs: BTreeMap::new(),
                job_base_ids: job_base_ids.clone(),
                job_needs: job_needs.clone(),
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

        // Reusable workflow invocations must acquire caller and embedded
        // concurrency gates as one ordered, deduplicated admission set. A
        // partially admitted JobSet keeps its earlier keys while waiting on
        // the next key, preventing it from bypassing either scope.
        let mut jobset_blocked: std::collections::HashSet<JobId> = std::collections::HashSet::new();
        for call in reusable_calls.values() {
            let member_ids: BTreeSet<JobId> = call
                .inner_job_ids
                .iter()
                .map(|id| JobId(id.clone()))
                .collect();
            let id = JobSetId {
                run_id,
                job_ids: member_ids.clone(),
            };
            let mut gates = Vec::new();
            let mut evaluation_failed = false;

            for (raw, scope, label, inputs) in [
                (
                    call.caller_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Job,
                    "caller concurrency (JobSet)",
                    &submission.inputs,
                ),
                (
                    call.embedded_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Workflow,
                    "embedded concurrency (JobSet)",
                    &call.inputs,
                ),
            ] {
                let Some(raw) = raw else { continue };
                let eval_ctx = concurrency::ConcurrencyContext {
                    scope,
                    github: &github,
                    vars: &submission.vars,
                    inputs,
                    matrix: Some(&call.matrix),
                    strategy: None,
                    needs: None,
                };
                match concurrency::evaluate_concurrency(raw, &eval_ctx) {
                    Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                        merge_jobset_gate(
                            &mut gates,
                            JobSetGate {
                                key: concurrency::concurrency_key(&submission.repository, &group),
                                display_name: group,
                                cancel_in_progress,
                                queue,
                            },
                        );
                    }
                    Ok((_, _, _)) => {
                        evaluation_failed = true;
                    }
                    Err(error) => {
                        concurrency::log_eval_error(label, &error);
                        evaluation_failed = true;
                    }
                }
            }

            if evaluation_failed {
                for member_id in &member_ids {
                    statuses.insert(member_id.clone(), ExecutionStatus::Failure);
                }
                jobset_blocked.extend(member_ids);
                continue;
            }
            if gates.is_empty() {
                continue;
            }

            inner.jobset_admissions.insert(
                id.clone(),
                JobSetAdmission {
                    gates,
                    acquired_keys: BTreeSet::new(),
                },
            );
            match advance_jobset_admission(&mut inner, &id, None) {
                Ok(JobSetAdmissionResult::Ready) => {}
                Ok(JobSetAdmissionResult::Blocked) => {
                    jobset_blocked.extend(member_ids);
                }
                Err(error) => {
                    let status = if error == "concurrency_queue_overflow" {
                        ExecutionStatus::Cancelled
                    } else {
                        ExecutionStatus::Failure
                    };
                    for member_id in &member_ids {
                        statuses.insert(member_id.clone(), status);
                    }
                    jobset_blocked.extend(member_ids);
                }
            }
        }

        // Enqueue jobs (workflow concurrency free / acquired).
        for queued_job in built_jobs {
            let job_id = queued_job.job_id.clone();
            let base_id = queued_job.base_id.clone();

            // A blocked JobSet member must remain durably parked until every
            // required key is acquired. Terminal members are not parked.
            if jobset_blocked.contains(&job_id) {
                let status = statuses.get(&job_id).copied();
                if status.is_some_and(concurrency::is_awaiting_execution) {
                    statuses.insert(job_id, ExecutionStatus::Pending);
                    inner.concurrency_blocked.push_back(queued_job);
                }
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
                submission,
                jobs: statuses,
                job_outputs: BTreeMap::new(),
                job_base_ids,
                job_needs,
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
        let cancel_count = inner.cancellation_queue.len();
        drop(inner);
        if ready_jobs > 0 || cancel_count > 0 {
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

    // Results/timeline updates only create details for dispatched jobs. Keep
    // the native jobs_list a complete projection by adding jobs that were
    // cancelled or failed before dispatch with an empty step list.
    for (job_id, status) in &run.jobs {
        if !run.jobs_list.iter().any(|detail| detail.name == job_id.0) {
            run.jobs_list.push(JobDetail {
                name: job_id.0.clone(),
                conclusion: status_string(*status),
                steps: Vec::new(),
            });
        }
    }

    for job_detail in &mut run.jobs_list {
        if let Some(status) = run.jobs.get(&JobId(job_detail.name.clone())) {
            job_detail.conclusion = status_string(*status);
        }
    }

    Ok(Json(run))
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
            .map(|run| run.submission.clone())
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
            loop {
                let event =
                    match tokio::time::timeout(EVENT_STREAM_IDLE_TIMEOUT, receiver.recv()).await {
                        Ok(Ok(event)) => event,
                        // A lagging consumer missed events; the snapshot it gets on
                        // reconnect is authoritative, so keep tailing from here.
                        Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
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
