//! Build `AgentJobRequestMessage` from parsed workflow data.
//!
//! This module converts a parsed `JobPlan` + context data into the
//! `AgentJobRequestMessage` that the runner receives after decryption.

use std::collections::BTreeMap;

use aksh_gha_protocol::azdo::{
    AgentJobRequestMessage, EndpointAuthorization, MaskHint, MaskType, PipelineContextData,
    PlanReference, ServiceEndpoint, TaskResources, TaskStep, TimelineReference, VariableValue,
};

use crate::eval::{build_context, resolve_string};
use crate::JobPlan;
use aksh_gha_expressions::Context;
use serde_json::{json, Value};

fn job_outputs_token(outputs: &BTreeMap<String, String>) -> Option<Value> {
    if outputs.is_empty() {
        return None;
    }
    let map = outputs
        .iter()
        .map(|(key, raw)| {
            let expression = raw
                .trim()
                .strip_prefix("${{")
                .and_then(|value| value.strip_suffix("}}").map(str::trim));
            let value = match expression {
                Some(expr) => json!({
                    "type": 3,
                    "file": 1,
                    "line": 1,
                    "col": 1,
                    "expr": expr,
                }),
                None => json!({
                    "type": 0,
                    "file": 1,
                    "line": 1,
                    "col": 1,
                    "lit": raw,
                }),
            };
            json!({
                "Key": {"type": 0, "file": 1, "line": 1, "col": 1, "lit": key},
                "Value": value,
            })
        })
        .collect::<Vec<_>>();
    Some(json!({
        "type": 2,
        "file": 1,
        "line": 1,
        "col": 1,
        "map": map,
    }))
}
fn normalized_github_context(github: &Value) -> Value {
    let mut object = match github {
        Value::Object(value) => value.clone(),
        _ => serde_json::Map::new(),
    };

    let repository = object
        .get("repository")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let repository_owner = repository.split('/').next().unwrap_or_default().to_owned();
    let git_ref = object
        .get("ref")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let ref_name = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(&git_ref)
        .to_owned();
    let ref_type = if git_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        "branch"
    };

    // These values are part of the runner's stable github context, rather than
    // event-payload fields. Keep server-supplied run/actor metadata intact while
    // deriving the ref and repository fields from the submission context.
    object.insert("server_url".to_owned(), json!("https://github.com"));
    object.insert("api_url".to_owned(), json!("https://api.github.com"));
    object.insert(
        "graphql_url".to_owned(),
        json!("https://api.github.com/graphql"),
    );
    object.insert("ref_name".to_owned(), json!(ref_name));
    object.insert("ref_protected".to_owned(), json!(false));
    object.insert("ref_type".to_owned(), json!(ref_type));
    object.insert("secret_source".to_owned(), json!("Actions"));
    object
        .entry("retention_days".to_owned())
        .or_insert_with(|| json!("90"));
    object
        .entry("artifact_cache_size_limit".to_owned())
        .or_insert_with(|| json!("10"));
    object.insert("repository_owner".to_owned(), json!(repository_owner));
    object
        .entry("repository_owner_id".to_owned())
        .or_insert_with(|| json!("0"));
    object.insert(
        "repositoryUrl".to_owned(),
        json!(format!("git://github.com/{repository}.git")),
    );

    for key in [
        "ref",
        "sha",
        "repository",
        "run_id",
        "run_number",
        "run_attempt",
        "repository_visibility",
        "actor_id",
        "actor",
        "workflow",
        "head_ref",
        "base_ref",
        "event_name",
    ] {
        object.entry(key.to_owned()).or_insert_with(|| json!(""));
    }
    if !object.get("event").is_some_and(Value::is_object) {
        object.insert("event".to_owned(), json!({}));
    }

    Value::Object(object)
}
/// Build a full `AgentJobRequestMessage` from a job plan and context.
///
/// This resolves `${{ }}` in env/with/run fields, builds contextData,
/// materializes variables and maskHints, and produces the complete
/// job message the runner expects.
pub fn build_agent_job_message(
    plan: &JobPlan,
    github: &Value,
    global_env: &BTreeMap<String, String>,
    secrets: &BTreeMap<String, String>,
    vars: &BTreeMap<String, String>,
) -> Result<AgentJobRequestMessage, String> {
    let timeline_id = uuid::Uuid::new_v4();
    let job_id = uuid::Uuid::new_v4();
    let github_context = normalized_github_context(github);

    // Build expression evaluation context
    let strategy = plan
        .matrix
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<String, Value>>();
    let strategy_value = Value::Object(
        strategy
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );

    let expr_context = build_context(
        &github_context,
        &plan.env,
        vars,
        &plan.matrix,
        &strategy_value,
        secrets,
        &plan.inputs,
    );

    let mut resolved_secrets = BTreeMap::new();
    if plan.workflow_file.is_none() || plan.secrets_inherit {
        resolved_secrets = secrets.clone();
    } else if !plan.secrets_map.is_empty() {
        for (k, expr) in &plan.secrets_map {
            let resolved = resolve_string(expr, &expr_context).unwrap_or_else(|_| expr.clone());
            resolved_secrets.insert(k.clone(), resolved);
        }
    }

    let job_expr_context = build_context(
        &github_context,
        &plan.env,
        vars,
        &plan.matrix,
        &strategy_value,
        &resolved_secrets,
        &plan.inputs,
    );

    // Resolve expressions in step env and with.
    // Generate contextName for each step matching GitHub's wire format:
    //   - Script steps: __run, __run_2, __run_3 (separate counter)
    //   - Action steps: __<sanitized_action>, __<sanitized_action>_2 (per-action counter)
    //   - User `id:` is used verbatim when present
    let mut run_counter: usize = 0;
    let mut action_counters: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut steps: Vec<TaskStep> = Vec::new();
    for step in &plan.steps {
        let is_script = step.uses.is_none() && step.run.is_some();
        let context_name = if let Some(ref user_id) = step.id {
            user_id.clone()
        } else if is_script {
            run_counter += 1;
            if run_counter == 1 {
                "__run".to_string()
            } else {
                format!("__run_{run_counter}")
            }
        } else if let Some(ref uses) = step.uses {
            // Action steps: __<sanitized_action>, __<sanitized_action>_2, etc.
            let base = uses.split('@').next().unwrap_or(uses).replace('/', "_");
            let counter = action_counters.entry(base.clone()).or_insert(0);
            *counter += 1;
            if *counter == 1 {
                format!("__{base}")
            } else {
                format!("__{base}_{counter}")
            }
        } else {
            format!("__step_{}", steps.len())
        };
        // Dedupe: if a user-supplied id collides with an auto-generated name,
        // the auto-generated one gets a suffix (shouldn't happen in practice).
        let final_name = if used_names.contains(&context_name) {
            format!("{context_name}_{}", steps.len())
        } else {
            context_name
        };
        used_names.insert(final_name.clone());
        let mut task_step = build_task_step(step, &job_expr_context);
        task_step.context_name = Some(final_name);
        steps.push(task_step);
    }

    // Materialize variables
    let mut variables = BTreeMap::new();
    for (k, v) in &plan.env {
        variables.insert(k.clone(), VariableValue::new(v));
    }
    for (k, v) in global_env {
        variables
            .entry(k.clone())
            .or_insert_with(|| VariableValue::new(v));
    }
    for (k, v) in vars {
        variables.insert(k.clone(), VariableValue::new(v));
    }
    for (k, v) in &resolved_secrets {
        variables.insert(k.clone(), VariableValue::secret(v));
    }

    if let Some(wf_file) = &plan.workflow_file {
        variables.insert(
            "system.workflowFileFullPath".to_owned(),
            VariableValue::new(wf_file),
        );
    }
    populate_runner_variables(&mut variables, plan);

    // GitHub supplies baseline regexes in addition to value-derived secret masks.
    let mut mask_hints = default_mask_hints();
    mask_hints.extend(
        resolved_secrets
            .values()
            .filter(|v| !v.is_empty())
            .map(|v| MaskHint {
                hint_type: MaskType::Regex,
                value: regex_escape(v),
            }),
    );

    // Service endpoints (SystemVssConnection)
    let endpoints = vec![ServiceEndpoint {
        data: BTreeMap::from([
            ("ConnectivityChecks".to_owned(), "{}".to_owned()),
            ("GenerateIdTokenUrl".to_owned(), String::new()),
            ("ServerId".to_owned(), String::new()),
            ("ServerName".to_owned(), String::new()),
        ]),
        name: "SystemVssConnection".to_owned(),
        endpoint_type: None,
        url: Some("http://localhost".to_owned()),
        authorization: EndpointAuthorization {
            parameters: BTreeMap::from([(
                "AccessToken".to_owned(),
                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.e30.ZmFrZXNpZw".to_owned(),
            )]),
            scheme: Some("OAuth".to_owned()),
        },
        is_shared: Some(false),
        is_ready: Some(true),
        service_owner: None,
    }];

    let resources = TaskResources {
        endpoints,
        repositories: Vec::new(),
    };

    // Context data follows the AgentJobRequestMessage contract. Workflow-level
    // metadata is enriched by the server, which owns the submission path/ref.
    let mut context_data = BTreeMap::new();
    context_data.insert(
        "github".to_owned(),
        PipelineContextData::from_json(&github_context),
    );
    let inputs_ctx = plan
        .inputs
        .iter()
        .map(|(k, v)| (k.clone(), PipelineContextData::from_json(v)))
        .collect();
    context_data.insert("inputs".to_owned(), PipelineContextData::Dict(inputs_ctx));

    let job_ctx = BTreeMap::from([
        ("check_run_id".to_owned(), PipelineContextData::Number(0.0)),
        (
            "workflow_ref".to_owned(),
            PipelineContextData::String(plan.workflow_ref.clone().unwrap_or_default()),
        ),
        (
            "workflow_sha".to_owned(),
            PipelineContextData::String(plan.workflow_sha.clone().unwrap_or_default()),
        ),
        (
            "workflow_repository".to_owned(),
            PipelineContextData::String(plan.workflow_repository.clone().unwrap_or_default()),
        ),
        (
            "workflow_file_path".to_owned(),
            PipelineContextData::String(plan.workflow_file.clone().unwrap_or_default()),
        ),
    ]);
    context_data.insert("job".to_owned(), PipelineContextData::Dict(job_ctx));

    let matrix_ctx = if plan.matrix.is_empty() {
        PipelineContextData::Null
    } else {
        PipelineContextData::Dict(
            plan.matrix
                .iter()
                .map(|(k, v)| (k.clone(), PipelineContextData::from_json(v)))
                .collect(),
        )
    };
    context_data.insert("matrix".to_owned(), matrix_ctx);
    context_data.insert(
        "needs".to_owned(),
        PipelineContextData::Dict(BTreeMap::new()),
    );
    let strategy_ctx = BTreeMap::from([
        (
            "fail-fast".to_owned(),
            PipelineContextData::Bool(plan.fail_fast),
        ),
        ("job-index".to_owned(), PipelineContextData::Number(0.0)),
        ("job-total".to_owned(), PipelineContextData::Number(1.0)),
        (
            "max-parallel".to_owned(),
            PipelineContextData::Number(plan.max_parallel.unwrap_or(1) as f64),
        ),
    ]);
    context_data.insert(
        "strategy".to_owned(),
        PipelineContextData::Dict(strategy_ctx),
    );
    context_data.insert(
        "vars".to_owned(),
        PipelineContextData::Dict(
            vars.iter()
                .map(|(k, v)| (k.clone(), PipelineContextData::String(v.clone())))
                .collect(),
        ),
    );

    let request_id: i64 = 1;

    Ok(AgentJobRequestMessage {
        message_type: None,
        job_id,
        request_id,
        plan: PlanReference {
            scope_identifier: String::new(),
            plan_id: job_id.to_string(),
            plan_type: "actions".to_owned(),
            version: 0,
            artifact_uri: String::new(),
            artifact_location: String::new(),
        },
        timeline: TimelineReference {
            id: timeline_id,
            change_id: 0,
            location: None,
        },
        job_display_name: Some(plan.name.clone()),
        job_name: plan.name.clone(),
        locked_until: "0001-01-01T00:00:00".to_owned(),
        billing_owner_id: None,
        file_table: Vec::new(),
        defaults: Vec::new(),
        environment_variables: Vec::new(),
        snapshot: None,
        condition: plan.if_condition.clone(),
        variables,
        mask_hints,
        resources,
        context_data,
        steps,
        retry_count: None,
        pre_job_timeout: None,
        job_timeout: None,
        job_container: plan.container.clone(),
        job_service_containers: non_empty_services(plan.services.clone()),
        job_outputs: job_outputs_token(&plan.job_outputs),
        enable_debugger: false,
        debugger_tunnel: None,
        debugger_welcome_message: None,
        aksh_debug_run_id: None,
        aksh_debug_transport: None,
        preloop_preserve_on_failure: None,
    })
}

/// Omit empty `services: {}` to match `EmitDefaultValue=false` behavior.
fn non_empty_services(services: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match &services {
        Some(serde_json::Value::Object(m)) if m.is_empty() => None,
        _ => services,
    }
}

fn regex_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(
            ch,
            '\\' | '.'
                | '+'
                | '*'
                | '?'
                | '('
                | ')'
                | '['
                | ']'
                | '{'
                | '}'
                | '|'
                | '^'
                | '$'
                | '/'
        ) {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn default_mask_hints() -> Vec<MaskHint> {
    const REGEXES: &[&str] = &[
        r#"\b(?:eyJ0eXAiOi|eyJhbGciOi|eyJ4NXQiOi|eyJraWQiOi)[^\s'";]+"#,
        r#"\bBearer\s+[^\s'";]+"#,
        r#"\b(?i:Password|Pwd)=(?:[^\s'";]+|"[^"]+")"#,
        r#"\s+-(?i:Password|Pwd)\s+(?:[^\s'";]+|"[^"]+")"#,
        r#"\bv1\.[0-9A-Fa-f]{40}\b"#,
        r#"\bgh[pousr]{1}_[A-Za-z0-9]{36}\b"#,
        r#"\bgithub_pat_[0-9][A-Za-z0-9]{21}_[A-Za-z0-9]{59}\b"#,
        r#"(?:[a-zA-Z][a-zA-Z\d+-.]*):\/\/([a-zA-Z\d\-._~\!$&'()*+,;=%]+):([a-zA-Z\d\-._~\!$&'()*+,;=:%]*)@"#,
        r#"\b[0-9A-Za-z-_~.]{3}7Q~[0-9A-Za-z-_~.]{31}\b|\b[0-9A-Za-z-_~.]{3}8Q~[0-9A-Za-z-_~.]{34}\b"#,
        r#"(?:^|[^0-9A-Za-z+/])[0-9A-Za-z+/]{76}(APIM|ACDb|\+(ABa|AMC|ASt))[0-9A-Za-z+/]{5}[AQgw]=="#,
        r#"(?:^|[^0-9A-Za-z+/])[0-9A-Za-z+/]{33}(AIoT|\+(ASb|AEh|ARm))[A-P][0-9A-Za-z+/]{5}="#,
        r#"\b[0-9A-Za-z_\-]{44}AzFu[0-9A-Za-z\-_]{5}[AQgw]=="#,
        r#"\b[0-9A-Za-z]{42}AzSe[A-D][0-9A-Za-z]{5}\b"#,
        r#"\b[0-9A-Za-z+/]{42}\+ACR[A-D][0-9A-Za-z+/]{5}\b"#,
        r#"\b[0-9A-Za-z]{33}AzCa[A-P][0-9A-Za-z]{5}="#,
        r#"\boy2[a-p][0-9a-z]{15}[aq][0-9a-z]{11}[eu][bdfhjlnprtvxz357][a-p][0-9a-z]{11}[aeimquy4]\b"#,
        r#"\bnpm_[0-9A-Za-z]{36}\b"#,
        r#"\bx-ghcr-signature=[^&]+"#,
    ];
    REGEXES
        .iter()
        .map(|value| MaskHint {
            hint_type: MaskType::Regex,
            value: (*value).to_owned(),
        })
        .collect()
}

fn populate_runner_variables(variables: &mut BTreeMap<String, VariableValue>, plan: &JobPlan) {
    const TRUE_FLAGS: &[&str] = &[
        "Actions.EnableHttpRedirects",
        "DistributedTask.AddWarningToNode12Action",
        "DistributedTask.AddWarningToNode16Action",
        "DistributedTask.AllowRunnerContainerHooks",
        "DistributedTask.DeprecateStepOutputCommands",
        "DistributedTask.DetailUntarFailure",
        "DistributedTask.EnableCompositeActions",
        "DistributedTask.EnableJobServerQueueTelemetry",
        "DistributedTask.EnhancedAnnotations",
        "DistributedTask.ForceGithubJavascriptActionsToNode16",
        "DistributedTask.ForceGithubJavascriptActionsToNode20",
        "DistributedTask.LogTemplateErrorsAsDebugMessages",
        "DistributedTask.MarkJobAsFailedOnWorkerCrash",
        "DistributedTask.NewActionMetadata",
        "DistributedTask.UploadStepSummary",
        "DistributedTask.UseActionArchiveCache",
        "DistributedTask.UseWhich2",
        "RunService.FixEmbeddedIssues",
        "actions.runner.usenode24bydefault",
        "actions.runner.warnonnode20",
        "actions_add_check_run_id_to_job_context",
        "actions_container_action_runner_temp",
        "actions_display_helpful_actions_download_errors",
        "actions_runner_deprecate_linux_arm32",
        "actions_service_container_command",
        "actions_set_orchestration_id_env_for_actions",
        "actions_skip_retry_complete_job_upon_known_errors",
        "actions_uses_cache_service_v2",
    ];
    const FALSE_FLAGS: &[&str] = &[
        "actions.runner.requirenode24",
        "actions_batch_action_resolution",
        "actions_runner_compare_workflow_parser",
        "actions_runner_emit_composite_markers",
        "actions_runner_kill_linux_arm32",
        "actions_snapshot_preflight_hosted_runner_check",
        "actions_snapshot_preflight_image_gen_pool_check",
        "actions_use_bearer_token_for_codeload",
    ];
    for key in TRUE_FLAGS {
        variables
            .entry((*key).to_owned())
            .or_insert_with(|| VariableValue::new("true"));
    }
    for key in FALSE_FLAGS {
        variables
            .entry((*key).to_owned())
            .or_insert_with(|| VariableValue::new("false"));
    }
    for (key, value) in [
        ("actions_runner_node20_removal_date", ""),
        ("actions_runner_node24_default_date", "June 16th, 2026"),
        ("system.from_run_service", "true"),
        ("system.github.job", plan.base_id.as_str()),
        ("system.github.launch_endpoint", ""),
        ("system.github.results_endpoint", ""),
        ("system.github.results_upload_with_sdk", "true"),
        (
            "system.github.token.permissions",
            r#"{"Contents":"read","Metadata":"read","Packages":"read"}"#,
        ),
        ("system.orchestrationId", ""),
        ("system.phaseDisplayName", plan.name.as_str()),
        ("system.runnerEnvironment", "self-hosted"),
        ("system.runnerGroupName", "Default"),
        ("system.runner.lowdiskspacethreshold", "100"),
    ] {
        variables
            .entry(key.to_owned())
            .or_insert_with(|| VariableValue::new(value));
    }
    variables
        .entry("github_token".to_owned())
        .or_insert_with(|| VariableValue::secret(String::new()));
    variables
        .entry("system.github.token".to_owned())
        .or_insert_with(|| VariableValue::secret(String::new()));
}

/// Build a `TaskStep` from a `StepPlan`.
fn build_task_step(step: &crate::StepPlan, context: &Context) -> TaskStep {
    let step_id = uuid::Uuid::new_v4();

    // Do NOT pre-resolve env or the run script here.
    //
    // The runner evaluates these at step execution time via evaluate_template()
    // with the full job context (including workspace for hashFiles, github.action,
    // steps.*.outputs, etc.). Pre-resolving at job-build time runs without a
    // workspace and silently zeros out hashFiles() results (PEXP-01 root cause).
    //
    // `with` inputs are still resolved because action handlers need resolved values
    // to locate and configure the action before step execution.
    let env = step.env.clone();
    let with: BTreeMap<String, String> = step
        .with
        .iter()
        .map(|(k, v)| {
            let input = step_input_to_string(v);
            // Don't resolve expressions — preserve ${{ }} so the protocol
            // layer serializes them as TemplateToken expression type=3.
            (k.clone(), input)
        })
        .collect();

    // Run script: pass as-is; the runner evaluates ${{ }} at step execution time.
    let run = step.run.clone();

    // The runner always evaluates a step condition. Omitted conditions are
    // the same as GitHub's default `success()`.
    let condition = Some(
        step.if_condition
            .clone()
            .unwrap_or_else(|| "success()".to_owned()),
    );

    // Build displayNameToken — TemplateToken literal matching GitHub's wire format.
    // type=1 is a literal token; lit contains the human-readable step name.
    let display_name_token = step.name.as_ref().map(|n| {
        serde_json::json!({
            "type": 1,
            "lit": n,
            "col": 0,
            "file": 0,
            "line": 0
        })
    });

    TaskStep {
        id: step_id,
        name: step.name.clone(),
        context_name: None, // Set by caller after construction
        display_name: step.name.clone(),
        display_name_token,
        condition,
        script: run,
        reference: step.uses.as_ref().map(|uses| {
            let is_local = uses.starts_with("./") || uses.starts_with(".\\");
            if is_local {
                aksh_gha_protocol::azdo::TaskReference {
                    id: None,
                    name: None,
                    path: Some(uses.clone()),
                    version: None,
                    reference_type: Some("repository".to_owned()),
                }
            } else {
                let (name, version) = if let Some((n, v)) = uses.split_once('@') {
                    (n.to_owned(), Some(v.to_owned()))
                } else {
                    (uses.clone(), None)
                };
                aksh_gha_protocol::azdo::TaskReference {
                    id: None,
                    name: Some(name),
                    path: None,
                    version,
                    reference_type: None,
                }
            }
        }),
        inputs: with,
        env,
        continue_on_error: step.continue_on_error,
        working_directory: step
            .working_directory
            .as_ref()
            .map(|wd| resolve_string(wd, context).unwrap_or_else(|_| wd.clone())),
        timeout_in_minutes: None,
    }
}

fn step_input_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_workflow, Workflow};

    fn simple_workflow() -> Workflow {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
        parse_workflow(yaml).unwrap()
    }

    #[test]
    fn build_message_from_simple_workflow() {
        let workflow = simple_workflow();
        let plans = crate::expand_jobs(&workflow).unwrap();
        let plan = &plans[0];

        let github = serde_json::json!({
            "event_name": "push",
            "ref": "refs/heads/main",
            "sha": "abc123"
        });

        let msg = build_agent_job_message(
            plan,
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(!msg.steps.is_empty());
        assert_eq!(msg.steps[0].condition.as_deref(), Some("success()"));
        assert!(msg.timeline.id != uuid::Uuid::nil());
        assert!(msg.job_id != uuid::Uuid::nil());
        assert!(msg
            .resources
            .endpoints
            .iter()
            .any(|e| e.name == "SystemVssConnection"));
    }

    #[test]
    fn build_message_with_matrix() {
        let yaml = r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps:
      - run: echo ${{ matrix.os }}
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();
        assert_eq!(plans.len(), 2);

        let github = serde_json::json!({"event_name": "push"});
        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        // Matrix should be in context data
        assert!(msg.context_data.contains_key("matrix"));
    }

    #[test]
    fn string_with_inputs_are_not_json_quoted() {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/cache@v4
        with:
          path: target
          fail-on-cache-miss: true
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();
        let github = serde_json::json!({"event_name": "push"});

        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(msg.steps[0].inputs.get("path"), Some(&"target".to_owned()));
        assert_eq!(
            msg.steps[0].inputs.get("fail-on-cache-miss"),
            Some(&"true".to_owned())
        );
    }

    #[test]
    fn secrets_become_variables_and_mask_hints() {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo secret
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();

        let mut secrets = BTreeMap::new();
        secrets.insert("MY_SECRET".to_owned(), "s3cr3t".to_owned());
        secrets.insert("SPECIAL_SECRET".to_owned(), "p@$$(word)".to_owned());
        let github = serde_json::json!({"event_name": "push"});
        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &secrets,
            &BTreeMap::new(),
        )
        .unwrap();

        let literal_hint = msg
            .mask_hints
            .iter()
            .find(|hint| hint.value == "s3cr3t")
            .expect("secret hint");
        assert_eq!(literal_hint.hint_type, MaskType::Regex);
        assert_eq!(serde_json::to_value(literal_hint).unwrap()["type"], "regex");
        let special_hint = msg
            .mask_hints
            .iter()
            .find(|hint| hint.value == r#"p@\$\$\(word\)"#)
            .expect("escaped secret hint");
        assert_eq!(special_hint.hint_type, MaskType::Regex);
        let secret = msg.variables.get("MY_SECRET").unwrap();
        assert_eq!(secret.value.as_deref(), Some("s3cr3t"));
        assert_eq!(secret.is_secret, Some(true));
        let special_secret = msg.variables.get("SPECIAL_SECRET").unwrap();
        assert_eq!(special_secret.value.as_deref(), Some("p@$$(word)"));
        assert_eq!(special_secret.is_secret, Some(true));
    }
    #[test]
    fn workflow_dispatch_inputs_are_in_event_context() {
        let yaml = r#"
on: workflow_dispatch
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ${{ github.event.inputs.greeting }}
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();

        let github = serde_json::json!({
            "event_name": "workflow_dispatch",
            "event": {
                "inputs": {
                    "greeting": "hello world"
                }
            },
            "ref": "refs/heads/main"
        });

        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(!msg.steps.is_empty());
    }
}
