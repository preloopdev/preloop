//! Job extension — workspace setup, GITHUB_* env injection, step ordering.

use std::collections::HashMap;
use std::path::Path;
use tracing::info;

use super::contexts::JobContext;
use super::steps_runner::{Step, StepType};

/// Set up the workspace directory and return its path.
pub fn setup_workspace(job_message: &serde_json::Value) -> anyhow::Result<String> {
    // Extract workspace info from the job message
    let work_dir = job_message
        .get("fileTable")
        .and_then(|ft| ft.get("workDirectory"))
        .and_then(|v| v.as_str())
        .or_else(|| {
            job_message
                .get("contextData")
                .and_then(|cd| cd.get("github"))
                .and_then(|gh| gh.get("workspace"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("_work/default/default");

    let work_path = Path::new(work_dir);
    let work_path = if work_path.is_absolute() {
        work_path.to_path_buf()
    } else {
        std::env::current_dir()?.join(work_path)
    };

    std::fs::create_dir_all(&work_path)?;
    info!("Workspace: {}", work_path.display());

    // Also create temp and actions dirs
    if let Some(parent) = work_path.parent().and_then(|p| p.parent()) {
        let temp_dir = parent.join("_temp");
        let actions_dir = parent.join("_actions");
        let tool_dir = parent.join("_tool");
        std::fs::create_dir_all(&temp_dir)?;
        std::fs::create_dir_all(&actions_dir)?;
        std::fs::create_dir_all(&tool_dir)?;
    }

    Ok(work_path.to_string_lossy().into_owned())
}

/// Inject GITHUB_* and RUNNER_* environment variables into the job context.
pub fn inject_github_env(job: &mut JobContext, msg: &serde_json::Value) {
    let raw_github = msg
        .get("contextData")
        .and_then(|cd| cd.get("github"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    // GitHub sends contextData in Azure DevOps typed-dictionary format:
    // {"t": 2, "d": [{"k": "key", "v": value}, ...]}
    // Decode to a flat JSON object.
    let github = decode_typed_value(&raw_github);

    let workspace = job.workspace.as_deref().unwrap_or("_work/default/default");

    // Core GITHUB_* variables
    let vars: Vec<(&str, String)> = vec![
        ("CI", "true".to_string()),
        ("GITHUB_ACTIONS", "true".to_string()),
        ("GITHUB_WORKSPACE", workspace.to_string()),
        ("GITHUB_REPOSITORY", str_from_json(&github, "repository")),
        (
            "GITHUB_REPOSITORY_OWNER",
            str_from_json(&github, "repository_owner"),
        ),
        ("GITHUB_SHA", str_from_json(&github, "sha")),
        ("GITHUB_REF", str_from_json(&github, "ref")),
        ("GITHUB_REF_NAME", str_from_json(&github, "ref_name")),
        ("GITHUB_REF_TYPE", str_from_json(&github, "ref_type")),
        ("GITHUB_HEAD_REF", str_from_json(&github, "head_ref")),
        ("GITHUB_BASE_REF", str_from_json(&github, "base_ref")),
        ("GITHUB_EVENT_NAME", str_from_json(&github, "event_name")),
        ("GITHUB_RUN_ID", str_from_json(&github, "run_id")),
        ("GITHUB_RUN_NUMBER", str_from_json(&github, "run_number")),
        ("GITHUB_RUN_ATTEMPT", str_from_json(&github, "run_attempt")),
        ("GITHUB_ACTOR", str_from_json(&github, "actor")),
        ("GITHUB_WORKFLOW", str_from_json(&github, "workflow")),
        ("GITHUB_JOB", str_from_json(&github, "job")),
        (
            "GITHUB_SERVER_URL",
            str_from_json_or(&github, "server_url", "https://github.com"),
        ),
        (
            "GITHUB_API_URL",
            str_from_json_or(&github, "api_url", "https://api.github.com"),
        ),
        (
            "GITHUB_GRAPHQL_URL",
            str_from_json_or(&github, "graphql_url", "https://api.github.com/graphql"),
        ),
        ("GITHUB_ACTION", str_from_json(&github, "action")),
        ("GITHUB_TOKEN", {
            let token = str_from_json(&github, "token");
            if token.is_empty() {
                // Fall back to variables.system.github.token
                msg.get("variables")
                    .and_then(|v| v.get("system.github.token"))
                    .and_then(|v| v.get("value"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            } else {
                token
            }
        }),
        // Runner variables — P1.12: runner name from .runner settings, not job name
        ("RUNNER_NAME", {
            // Read from .runner settings file (CWD = runner root from spawn_job)
            crate::settings::RunnerConfig::load(std::path::Path::new("."))
                .ok()
                .map(|c| c.settings.agent_name)
                .unwrap_or_else(|| job.job_name.clone())
        }),
        ("RUNNER_OS", runner_os().to_string()),
        ("RUNNER_ARCH", runner_arch().to_string()),
        (
            "RUNNER_TEMP",
            Path::new(workspace)
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.join("_temp").to_string_lossy().to_string())
                .unwrap_or_else(|| "/tmp".to_string()),
        ),
        (
            "RUNNER_TOOL_CACHE",
            Path::new(workspace)
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.join("_tool").to_string_lossy().to_string())
                .unwrap_or_else(|| "/tmp".to_string()),
        ),
        // P1.9: Missing GITHUB_*/RUNNER_* env vars (F034)
        (
            "GITHUB_REF_PROTECTED",
            str_from_json_or(&github, "ref_protected", "false"),
        ),
        (
            "GITHUB_REPOSITORY_ID",
            str_from_json(&github, "repository_id"),
        ),
        (
            "GITHUB_REPOSITORY_OWNER_ID",
            str_from_json(&github, "repository_owner_id"),
        ),
        (
            "GITHUB_TRIGGERING_ACTOR",
            str_from_json(&github, "triggering_actor"),
        ),
        (
            "GITHUB_WORKFLOW_REF",
            str_from_json(&github, "workflow_ref"),
        ),
        (
            "GITHUB_WORKFLOW_SHA",
            str_from_json(&github, "workflow_sha"),
        ),
        (
            "GITHUB_RETENTION_DAYS",
            str_from_json_or(&github, "retention_days", "90"),
        ),
        ("RUNNER_ENVIRONMENT", "self-hosted".to_string()),
        ("RUNNER_PERFLOG", String::new()),
        // Generate a unique tracking ID per job, matching the official runner's
        // `github_{Guid.NewGuid()}` pattern. Used to identify orphan child
        // processes after the job finishes (any process still carrying this env
        // var was spawned by this job and not cleaned up).
        (
            "RUNNER_TRACKING_ID",
            format!("github_{}", uuid::Uuid::new_v4()),
        ),
    ];

    for (key, value) in vars {
        if !value.is_empty() {
            job.env.insert(key.to_string(), value);
        }
    }

    // P1.9: RUNNER_DEBUG from system.debug variable
    let runner_debug = msg
        .get("variables")
        .and_then(|v| v.get("system.debug"))
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())
        .map(|v| if v == "true" { "1" } else { "0" })
        .unwrap_or("0");
    job.env
        .insert("RUNNER_DEBUG".to_string(), runner_debug.to_string());

    // Write event payload to GITHUB_EVENT_PATH
    if let Some(event) = github.get("event") {
        let temp_dir = Path::new(workspace)
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("_temp"))
            .unwrap_or_else(|| Path::new("/tmp").to_path_buf());
        let event_path = temp_dir.join("event.json");
        if let Ok(event_json) = serde_json::to_string_pretty(event) {
            let _ = std::fs::write(&event_path, &event_json);
            job.env.insert(
                "GITHUB_EVENT_PATH".to_string(),
                event_path.to_string_lossy().to_string(),
            );
        }
    }

    // Inject variables from the job message (non-secret ones)
    if let Some(vars) = msg.get("variables").and_then(|v| v.as_object()) {
        for (key, val) in vars {
            let is_secret = val
                .get("isSecret")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !is_secret {
                if let Some(value) = val.get("value").and_then(|v| v.as_str()) {
                    // Only inject if not already set (step env > job vars)
                    if !job.env.contains_key(key) {
                        job.env.insert(key.clone(), value.to_string());
                    }
                }
            }
        }
    }

    // Official runner evaluates job/workflow-level env from
    // AgentJobRequestMessage.EnvironmentVariables into the global environment
    // before any step starts. GitHub sends this as a list of typed template maps.
    if let Some(env_tokens) = msg.get("environmentVariables").and_then(|v| v.as_array()) {
        for token in env_tokens {
            for (key, value) in extract_template_map(token) {
                job.env.insert(key, value);
            }
        }
    }

    // F021: ACTIONS_* runtime env plumbing.
    // SystemVssConnection carries both the run-service URL/token and data URLs
    // used by cache, artifact, results, and OIDC actions.
    if let Some(endpoints) = msg
        .get("resources")
        .and_then(|r| r.get("endpoints"))
        .and_then(|e| e.as_array())
    {
        for ep in endpoints {
            if ep.get("name").and_then(|v| v.as_str()) == Some("SystemVssConnection") {
                if let Some(url) = ep.get("url").and_then(|v| v.as_str()) {
                    job.env
                        .insert("ACTIONS_RUNTIME_URL".to_string(), url.to_string());
                }
                if let Some(token) = ep
                    .get("authorization")
                    .and_then(|a| a.get("parameters"))
                    .and_then(|p| p.get("AccessToken"))
                    .and_then(|v| v.as_str())
                {
                    job.env
                        .insert("ACTIONS_RUNTIME_TOKEN".to_string(), token.to_string());
                    job.env.insert(
                        "ACTIONS_ID_TOKEN_REQUEST_TOKEN".to_string(),
                        token.to_string(),
                    );
                    job.add_mask(token);
                }

                if let Some(data) = ep.get("data").and_then(|v| v.as_object()) {
                    if let Some(url) = data.get("ResultsServiceUrl").and_then(|v| v.as_str()) {
                        job.env
                            .insert("ACTIONS_RESULTS_URL".to_string(), url.to_string());
                    }
                    if let Some(url) = data.get("CacheServerUrl").and_then(|v| v.as_str()) {
                        job.env.insert(
                            "ACTIONS_CACHE_URL".to_string(),
                            url.trim_end_matches('/').to_string(),
                        );
                        job.env
                            .entry("ACTIONS_CACHE_SERVICE_V2".to_string())
                            .or_insert_with(|| "true".to_string());
                    }
                    if let Some(url) = data.get("GenerateIdTokenUrl").and_then(|v| v.as_str()) {
                        if !url.is_empty() {
                            job.env.insert(
                                "ACTIONS_ID_TOKEN_REQUEST_URL".to_string(),
                                url.to_string(),
                            );
                        }
                    }
                }
                break;
            }
        }
    }

    // Some services also arrive as system.github.* variables. Let those fill
    // gaps without overwriting endpoint data.
    if let Some(vars) = msg.get("variables").and_then(|v| v.as_object()) {
        let mappings: &[(&str, &str)] = &[
            ("system.github.results_endpoint", "ACTIONS_RESULTS_URL"),
            ("system.github.cache_service_v2", "ACTIONS_CACHE_SERVICE_V2"),
            (
                "system.github.id_token_request_url",
                "ACTIONS_ID_TOKEN_REQUEST_URL",
            ),
            // v2.336.0 (#4538): effective cache mode surfaced to steps
            ("actions_cache_mode", "ACTIONS_CACHE_MODE"),
        ];
        for (var_key, env_key) in mappings {
            if job.env.contains_key(*env_key) {
                continue;
            }
            if let Some(value) = vars
                .get(*var_key)
                .and_then(|v| v.get("value"))
                .and_then(|v| v.as_str())
            {
                if !value.is_empty() {
                    job.env.insert(env_key.to_string(), value.to_string());
                }
            }
        }
    }
}

/// Decode an Azure DevOps typed-dictionary value.
///
/// GitHub/AzDO contextData uses `{"t": TYPE, "d": DATA}` encoding:
/// - `t=1` (string): `{"t": 1, "d": "value"}` → `"value"`
/// - `t=2` (dictionary): `{"t": 2, "d": [{"k": "key", "v": VALUE}, ...]}` → `{"key": decoded(VALUE), ...}`
/// - `t=3` (array): `{"t": 3, "d": [VALUE, ...]}` → `[decoded(VALUE), ...]`
/// - `t=4` (bool): `{"t": 4, "d": true/false}` → `true/false`
/// - `t=5` (number): `{"t": 5, "d": N}` → `N`
///
/// If the value is already a plain JSON object (e.g. from local aksh), it is returned as-is.
pub(crate) fn decode_typed_value(val: &serde_json::Value) -> serde_json::Value {
    match val.get("t").and_then(|t| t.as_u64()) {
        Some(1) => {
            // String
            val.get("d").cloned().unwrap_or(serde_json::Value::Null)
        }
        Some(2) => {
            // Dictionary
            let mut obj = serde_json::Map::new();
            if let Some(entries) = val.get("d").and_then(|d| d.as_array()) {
                for entry in entries {
                    if let Some(key) = entry.get("k").and_then(|k| k.as_str()) {
                        let value = entry
                            .get("v")
                            .map(decode_typed_value)
                            .unwrap_or(serde_json::Value::Null);
                        obj.insert(key.to_string(), value);
                    }
                }
            }
            serde_json::Value::Object(obj)
        }
        Some(3) => {
            // Array
            if let Some(items) = val.get("d").and_then(|d| d.as_array()) {
                serde_json::Value::Array(items.iter().map(decode_typed_value).collect())
            } else {
                serde_json::Value::Array(vec![])
            }
        }
        Some(4) => {
            // Boolean
            val.get("d").cloned().unwrap_or(serde_json::Value::Null)
        }
        Some(5) => {
            // Number
            val.get("d").cloned().unwrap_or(serde_json::Value::Null)
        }
        _ => {
            // Not typed — return as-is (plain JSON from aksh)
            val.clone()
        }
    }
}

fn bool_from_template_token(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::String(s) => s.eq_ignore_ascii_case("true"),
        serde_json::Value::Object(map) => {
            if let Some(b) = map.get("bool").and_then(|v| v.as_bool()) {
                return b;
            }
            if let Some(b) = map.get("boolean").and_then(|v| v.as_bool()) {
                return b;
            }
            if let Some(lit) = map.get("lit").and_then(|v| v.as_str()) {
                return lit.eq_ignore_ascii_case("true");
            }
            if let Some(expr) = map.get("expr").and_then(|v| v.as_str()) {
                return expr.trim().eq_ignore_ascii_case("true");
            }
            false
        }
        _ => false,
    }
}

/// Build the ordered step list from the job message steps.
pub fn build_step_list(steps: &[serde_json::Value], job_message: &serde_json::Value) -> Vec<Step> {
    let mut result = Vec::new();
    let mut run_counter: usize = 0; // F029: counts id-less script steps for __run / __run_N

    // Parse defaults.run from job message (working-directory, shell)
    let (default_working_dir, default_shell) = parse_job_defaults(job_message);

    for (i, step) in steps.iter().enumerate() {
        let reference = step.get("reference");
        let inputs = extract_template_map(step.get("inputs").unwrap_or(&serde_json::Value::Null));
        let is_script = match reference {
            Some(ref_val) => {
                let ref_type = ref_val.get("type").and_then(|v| v.as_str()).unwrap_or("");
                matches!(ref_type, "script" | "Script")
            }
            None => step
                .get("run")
                .and_then(|v| v.as_str())
                .is_some_and(|r| !r.is_empty()),
        };

        // F029: Split wire ID (GUID) from context name (human-readable key).
        // Live GitHub sends both `id` (GUID) and `contextName` (__run, __run_2, etc.).
        // aksh-native payloads may only have `id` (which IS the context name).
        let raw_context_name = step.get("contextName").and_then(|v| v.as_str());
        let raw_id = step.get("id").and_then(|v| v.as_str());

        // Context name: prefer contextName, then auto-generate __run/__run_N for scripts
        let context_name = if let Some(cn) = raw_context_name {
            cn.to_string()
        } else if let Some(eid) = raw_id {
            // aksh-native: id IS the context name
            eid.to_string()
        } else if is_script {
            run_counter += 1;
            if run_counter == 1 {
                "__run".to_string()
            } else {
                format!("__run_{run_counter}")
            }
        } else {
            format!("step_{i}")
        };

        // Wire ID: prefer id (GUID on live GitHub), fall back to context_name
        let id = raw_id.unwrap_or(&context_name).to_string();

        let display_name_override =
            step.get("displayName")
                .and_then(template_scalar)
                .or_else(|| {
                    // Live GitHub payloads use displayNameToken.lit instead of displayName
                    step.get("displayNameToken")
                        .and_then(|t| t.get("lit"))
                        .and_then(|v| v.as_str())
                        .map(String::from)
                });

        let condition = step
            .get("condition")
            .and_then(|v| v.as_str())
            .map(String::from);

        let continue_on_error = step
            .get("continueOnError")
            .map(bool_from_template_token)
            .unwrap_or(false);

        let timeout_minutes = step.get("timeoutInMinutes").and_then(|v| v.as_u64());

        // Official ActionStep.Background (DTPipelines) — wire `background: true`.
        let is_background = step
            .get("background")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let env = extract_step_env(step);

        // Determine step type (reuse `reference` from above)
        let step_type = if let Some(ref_val) = reference {
            let ref_type = ref_val.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match ref_type {
                "script" | "Script" => {
                    let script = inputs.get("script").cloned().unwrap_or_default();
                    let shell = inputs
                        .get("shell")
                        .cloned()
                        .or_else(|| default_shell.clone());
                    let working_dir = inputs
                        .get("workingDirectory")
                        .cloned()
                        .or_else(|| default_working_dir.clone());
                    StepType::Script {
                        script,
                        shell,
                        working_directory: working_dir,
                    }
                }
                _ => {
                    // Action reference — check for local (self) vs remote
                    let repo_type = ref_val
                        .get("repositoryType")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let uses = if repo_type == "self" {
                        // Local action: uses the `path` field (e.g. "./.github/actions/greet")
                        ref_val
                            .get("path")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string()
                    } else if repo_type.eq_ignore_ascii_case("selfRepository") {
                        // v2.336.0 $/ self-repository action reference
                        // (PipelineConstants.SelfRepositoryAlias).
                        let path = ref_val
                            .get("path")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .trim_start_matches('/');
                        format!("$/{path}")
                    } else {
                        // Remote action: combine name + /path + @ref
                        let name = ref_val.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let path = ref_val.get("path").and_then(|v| v.as_str()).unwrap_or("");
                        let action_ref = ref_val.get("ref").and_then(|v| v.as_str());
                        let full_name = if path.is_empty() {
                            name.to_string()
                        } else {
                            format!("{name}/{path}")
                        };
                        match action_ref {
                            Some(version) if !full_name.contains('@') => {
                                format!("{full_name}@{version}")
                            }
                            _ => full_name,
                        }
                    };
                    let with =
                        serde_json::to_value(&inputs).unwrap_or_else(|_| serde_json::json!({}));
                    StepType::Action { uses, with }
                }
            }
        } else {
            // No reference — might be a raw script step from aksh
            let run = step
                .get("run")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !run.is_empty() {
                let shell = step.get("shell").and_then(|v| v.as_str()).map(String::from);
                StepType::Script {
                    script: run,
                    shell,
                    working_directory: step
                        .get("working-directory")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                }
            } else {
                StepType::Script {
                    script: String::new(),
                    shell: None,
                    working_directory: None,
                }
            }
        };

        let display_name = display_name_override
            .unwrap_or_else(|| display_name_for_step(&context_name, &step_type));

        result.push(Step {
            id,
            context_name,
            display_name,
            step_type,
            condition,
            continue_on_error,
            timeout_minutes,
            env,
            raw: step.clone(),
            is_background,
        });
    }

    result
}

/// Discover pre/post steps from action manifests and return the full ordered list.
///
/// F023: Official runner builds three lists:
///   - pre steps: declared order, `pre-if` defaults to `always()`
///   - main steps: the workflow steps
///   - post steps: LIFO (reverse of main-step order), `post-if` defaults to `always()`
///
/// State context (`GITHUB_STATE` file) is wired so pre can communicate to post
/// via `save-state`; `StepContext::build_env()` exposes those values as
/// `STATE_*` for `__post_<step-id>` steps.
///
/// Remote actions must be downloaded before this runs; `action_paths` maps the
/// original `uses:` ref to the resolved manifest directory.
pub fn build_step_list_with_lifecycle(
    main_steps: Vec<Step>,
    workspace: &str,
    action_paths: &std::collections::HashMap<String, String>,
) -> Vec<Step> {
    let mut pre_steps: Vec<Step> = Vec::new();
    let mut post_steps: Vec<Step> = Vec::new();

    for step in &main_steps {
        let StepType::Action { ref uses, ref with } = step.step_type else {
            continue;
        };
        let is_local_action = uses.starts_with("./") || uses.starts_with("../");

        // Resolve the action directory. Prefer the SHA-pinned path discovered
        // during the setup/download phase; fall back to local action paths.
        let action_dir = if let Some(path) = action_paths.get(uses) {
            std::path::PathBuf::from(path)
        } else if is_local_action {
            std::path::Path::new(workspace).join(uses)
        } else {
            continue;
        };

        let manifest = match super::handlers::factory::load_action_manifest(&action_dir) {
            Ok(m) => m,
            Err(_) => continue, // action not yet on disk — skip pre/post
        };
        if !action_supports_lifecycle(&manifest) {
            continue;
        }

        // ActionRunner.RunAsync warns that pre is unsupported for local self actions;
        // post cleanup is still registered. Keep this aligned with the pinned source
        // (ActionRunner.cs, `RunAsync`, lines 105-110).
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionRunner.cs#L105-L110
        if !is_local_action {
            // Pre step
            if let Some(pre_main) = &manifest.runs_pre {
                let pre_if = manifest.runs_pre_if.as_deref().unwrap_or("always()");
                let pre_context = format!("__pre_{}", step.context_name);
                let pre_id = format!("__pre_{}", step.id);
                pre_steps.push(Step {
                    id: pre_id,
                    context_name: pre_context,
                    display_name: format!("Pre {}", step.display_name),
                    step_type: StepType::Action {
                        uses: uses.clone(),
                        with: with_internal_entry(with, pre_main),
                    },
                    condition: Some(pre_if.to_string()),
                    continue_on_error: step.continue_on_error,
                    timeout_minutes: step.timeout_minutes,
                    env: step.env.clone(),
                    raw: serde_json::json!({
                        "__pre": true,
                        "__pre_main": pre_main,
                        "uses": uses,
                    }),
                    is_background: false,
                });
            }
        }

        // Post step (will be reversed into LIFO)
        if let Some(post_main) = &manifest.runs_post {
            // The official runner keys post registration by Action.Id. Each
            // workflow step is therefore a distinct invocation, including
            // repeated `uses:` references with separate saved state.
            let post_if = manifest.runs_post_if.as_deref().unwrap_or("always()");
            let post_context = format!("__post_{}", step.context_name);
            let post_id = format!("__post_{}", step.id);
            post_steps.push(Step {
                id: post_id,
                context_name: post_context,
                display_name: format!("Post {}", step.display_name),
                step_type: StepType::Action {
                    uses: uses.clone(),
                    with: with_internal_entry(with, post_main),
                },
                condition: Some(post_if.to_string()),
                continue_on_error: true, // post steps shouldn't block other posts
                timeout_minutes: step.timeout_minutes,
                env: step.env.clone(),
                raw: serde_json::json!({
                    "__post": true,
                    "__post_main": post_main,
                    "uses": uses,
                }),
                is_background: false,
            });
        }
    }

    // LIFO for post: reverse post_steps
    post_steps.reverse();

    // Assemble: pre → main → post
    let mut result = pre_steps;
    result.extend(main_steps);
    result.extend(post_steps);
    result
}

fn with_internal_entry(with: &serde_json::Value, entry: &str) -> serde_json::Value {
    let mut obj = with.as_object().cloned().unwrap_or_default();
    obj.insert(
        "__aksh_entry".to_string(),
        serde_json::Value::String(entry.to_string()),
    );
    serde_json::Value::Object(obj)
}

fn action_supports_lifecycle(manifest: &super::handlers::factory::ActionManifest) -> bool {
    matches!(
        manifest.runs_using.as_str(),
        "node12" | "node16" | "node20" | "node24" | "docker" | "composite"
    )
}

/// Parse `defaults.run` from the job message.
///
/// GitHub sends `defaults` as an array of AzDO typed-dict entries:
/// ```json
/// [{"type":2,"map":[{"Key":{"lit":"run"},"Value":{"type":2,"map":[
///   {"Key":{"lit":"working-directory"},"Value":{"lit":"subdir"}},
///   {"Key":{"lit":"shell"},"Value":{"lit":"bash"}}
/// ]}}]}]
/// ```
///
/// Returns `(working_directory, shell)` as optional strings.
fn parse_job_defaults(job_message: &serde_json::Value) -> (Option<String>, Option<String>) {
    let defaults = match job_message.get("defaults") {
        Some(v) => v,
        None => return (None, None),
    };

    // defaults can be an array of typed-dict entries or a plain object
    let run_value = if let Some(arr) = defaults.as_array() {
        // Walk the array looking for a "run" key in each typed-dict map
        arr.iter().find_map(|entry| {
            let map = entry.get("map").and_then(|v| v.as_array())?;
            map.iter().find_map(|kv| {
                let key = kv
                    .get("Key")
                    .or_else(|| kv.get("key"))
                    .and_then(template_scalar)?;
                if key == "run" {
                    kv.get("Value").or_else(|| kv.get("value")).cloned()
                } else {
                    None
                }
            })
        })
    } else if let Some(obj) = defaults.as_object() {
        obj.get("run").cloned()
    } else {
        None
    };

    let run_value = match run_value {
        Some(v) => v,
        None => return (None, None),
    };

    // Extract working-directory and shell from the run value
    let run_map = extract_template_map(&run_value);
    let working_dir = run_map.get("working-directory").cloned();
    let shell = run_map.get("shell").cloned();
    (working_dir, shell)
}

fn extract_step_env(step: &serde_json::Value) -> HashMap<String, String> {
    step.get("environment")
        .map(extract_template_map)
        .unwrap_or_default()
}

fn extract_template_map(value: &serde_json::Value) -> HashMap<String, String> {
    let mut out = HashMap::new();

    if let Some(obj) = value.as_object() {
        for (key, value) in obj {
            if key == "map" {
                continue;
            }
            if let Some(string) = template_scalar(value) {
                out.insert(key.clone(), string);
            }
        }
    }

    if let Some(entries) = value.get("map").and_then(|v| v.as_array()) {
        for entry in entries {
            let Some(key) = entry
                .get("Key")
                .or_else(|| entry.get("key"))
                .and_then(template_scalar)
            else {
                continue;
            };
            let Some(value) = entry
                .get("Value")
                .or_else(|| entry.get("value"))
                .and_then(template_scalar)
            else {
                continue;
            };
            out.insert(key, value);
        }
    }

    out
}

fn template_scalar(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(String::from)
        .or_else(|| value.get("lit").and_then(|v| v.as_str()).map(String::from))
        .or_else(|| {
            // type=3 is an expression: {"type": 3, "expr": "..."}
            // Return the expression wrapped in ${{ }} for later evaluation
            value
                .get("expr")
                .and_then(|v| v.as_str())
                .map(|expr| format!("${{{{ {expr} }}}}"))
        })
}

/// F029: Generate display names matching official runner conventions.
/// Script steps: "Run {first_line}" truncated to 80 chars.
/// Action steps: "Run {uses}" (e.g. "Run actions/checkout@v4").
pub(crate) fn display_name_for_step(id: &str, step_type: &StepType) -> String {
    match step_type {
        StepType::Script { script, .. } if !script.trim().is_empty() => {
            let first_line = script.lines().next().unwrap_or("").trim();
            // Official runner truncates display names; 80 chars is the practical limit
            if first_line.chars().count() > 80 {
                let truncated: String = first_line.chars().take(80).collect();
                format!("Run {truncated}…")
            } else {
                format!("Run {first_line}")
            }
        }
        StepType::Action { uses, .. } if !uses.is_empty() => format!("Run {uses}"),
        _ => id.to_string(),
    }
}

fn str_from_json(val: &serde_json::Value, key: &str) -> String {
    val.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn str_from_json_or(val: &serde_json::Value, key: &str, default: &str) -> String {
    val.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or(default)
        .to_string()
}

fn runner_os() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else {
        "Unknown"
    }
}

fn runner_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else {
        "X64"
    }
}

/// Kill any child processes still carrying a `RUNNER_TRACKING_ID` env var
/// matching `tracking_id`. This mirrors the official runner's orphan-process
/// cleanup in `JobExtension.cs` (`FinalizeJob`).
///
/// Best-effort: errors reading individual process environments are silently
/// skipped — we never want cleanup failures to fail the job.
pub fn kill_orphan_processes(tracking_id: &str) {
    let needle = format!("RUNNER_TRACKING_ID={tracking_id}");
    for pid in orphan_pids_with_tracking_id(&needle) {
        tracing::warn!(pid, %tracking_id, "killing orphan process");
        // Use the shell `kill -9` — avoids unsafe libc calls while still
        // matching the official runner's SIGKILL semantics.
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
    }
}

/// Enumerate PIDs whose environment contains `needle`.
///
/// On Linux reads `/proc/<pid>/environ` (NUL-delimited).
/// On macOS uses `ps -Ewwx -o pid=,command=` which prints the env inline.
fn orphan_pids_with_tracking_id(needle: &str) -> Vec<u32> {
    let mut pids = Vec::new();
    #[cfg(target_os = "linux")]
    {
        if let Ok(proc_dir) = std::fs::read_dir("/proc") {
            for entry in proc_dir.flatten() {
                let name = entry.file_name();
                let pid_str = name.to_string_lossy();
                if let Ok(pid) = pid_str.parse::<u32>() {
                    let env_path = format!("/proc/{pid}/environ");
                    if let Ok(data) = std::fs::read(&env_path) {
                        let has = data.split(|&b| b == 0).any(|kv| kv == needle.as_bytes());
                        if has {
                            pids.push(pid);
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        // `ps -Ewwx` prints each process's env vars inline after its command.
        if let Ok(out) = std::process::Command::new("ps")
            .args(["-Ewwx", "-o", "pid=,command="])
            .output()
        {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                if line.contains(needle) {
                    if let Some(pid) = line.split_whitespace().next().and_then(|s| s.parse().ok()) {
                        pids.push(pid);
                    }
                }
            }
        }
    }
    pids
}

#[cfg(test)]
#[path = "job_extension_tests.rs"]
mod tests;
