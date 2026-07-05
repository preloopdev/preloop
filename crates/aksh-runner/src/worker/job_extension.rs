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
        ("RUNNER_TRACKING_ID", str_from_json(&github, "tracking_id")),
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

/// Build the ordered step list from the job message steps.
pub fn build_step_list(steps: &[serde_json::Value], _job_message: &serde_json::Value) -> Vec<Step> {
    let mut result = Vec::new();
    let mut run_counter: usize = 0; // F029: counts id-less script steps for __run / __run_N

    for (i, step) in steps.iter().enumerate() {
        // Determine step type first so we know if it's a script step (needed for auto-ID)
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

        let display_name_override = step
            .get("displayName")
            .and_then(|v| v.as_str())
            .map(String::from);

        let condition = step
            .get("condition")
            .and_then(|v| v.as_str())
            .map(String::from);

        let continue_on_error = step
            .get("continueOnError")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let timeout_minutes = step.get("timeoutInMinutes").and_then(|v| v.as_u64());

        let env = extract_step_env(step);

        // Determine step type (reuse `reference` from above)
        let step_type = if let Some(ref_val) = reference {
            let ref_type = ref_val.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match ref_type {
                "script" | "Script" => {
                    let script = inputs.get("script").cloned().unwrap_or_default();
                    let shell = inputs.get("shell").cloned();
                    let working_dir = inputs.get("workingDirectory").cloned();
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
                        if full_name.contains('@') || action_ref.is_none() {
                            full_name
                        } else {
                            format!("{full_name}@{}", action_ref.unwrap())
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

        // Resolve the action directory. Prefer the SHA-pinned path discovered
        // during the setup/download phase; fall back to local action paths.
        let action_dir = if let Some(path) = action_paths.get(uses) {
            std::path::PathBuf::from(path)
        } else if uses.starts_with("./") || uses.starts_with("../") {
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
            });
        }

        // Post step (will be reversed into LIFO)
        if let Some(post_main) = &manifest.runs_post {
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
/// Action steps: the full `uses` ref (e.g. "actions/checkout@v4").
fn display_name_for_step(id: &str, step_type: &StepType) -> String {
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
        StepType::Action { uses, .. } if !uses.is_empty() => uses.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_github_env_sets_core_vars() {
        let mut job = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {
                    "repository": "owner/repo",
                    "sha": "abc123",
                    "ref": "refs/heads/main",
                    "actor": "user1"
                }
            }),
        );
        job.workspace = Some("_work/repo/repo".into());

        let msg = serde_json::json!({
            "contextData": {
                "github": {
                    "repository": "owner/repo",
                    "sha": "abc123",
                    "ref": "refs/heads/main",
                    "actor": "user1"
                }
            }
        });

        inject_github_env(&mut job, &msg);

        assert_eq!(job.env.get("CI").unwrap(), "true");
        assert_eq!(job.env.get("GITHUB_ACTIONS").unwrap(), "true");
        assert_eq!(job.env.get("GITHUB_REPOSITORY").unwrap(), "owner/repo");
        assert_eq!(job.env.get("GITHUB_SHA").unwrap(), "abc123");
        assert_eq!(job.env.get("GITHUB_REF").unwrap(), "refs/heads/main");
        assert_eq!(job.env.get("GITHUB_ACTOR").unwrap(), "user1");
        assert!(job.env.contains_key("RUNNER_OS"));
        assert!(job.env.contains_key("RUNNER_ARCH"));
    }

    #[test]
    fn build_step_list_parses_script_reference() {
        let steps = vec![serde_json::json!({
            "id": "step1",
            "displayName": "Run echo",
            "reference": {
                "type": "script"
            },
            "inputs": {
                "script": "echo hello"
            },
            "condition": "success()"
        })];

        let msg = serde_json::json!({});
        let result = build_step_list(&steps, &msg);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "step1");
        assert_eq!(result[0].display_name, "Run echo");
        assert!(
            matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo hello")
        );
    }

    #[test]
    fn build_step_list_parses_action_reference() {
        let steps = vec![serde_json::json!({
            "id": "checkout",
            "displayName": "Checkout",
            "reference": {
                "type": "repository",
                "name": "actions/checkout@v4"
            },
            "inputs": {
                "fetch-depth": "1"
            }
        })];

        let msg = serde_json::json!({});
        let result = build_step_list(&steps, &msg);

        assert_eq!(result.len(), 1);
        assert!(
            matches!(&result[0].step_type, StepType::Action { uses, .. } if uses == "actions/checkout@v4")
        );
    }

    #[test]
    fn build_step_list_parses_github_template_token_maps() {
        let steps = vec![serde_json::json!({
            "id": "step1",
            "reference": {
                "type": "script"
            },
            "inputs": {
                "type": 2,
                "map": [
                    {
                        "Key": { "type": 0, "lit": "script" },
                        "Value": { "type": 0, "lit": "echo first" }
                    }
                ]
            },
            "environment": {
                "type": 2,
                "map": [
                    {
                        "Key": { "type": 0, "lit": "VAL" },
                        "Value": { "type": 0, "lit": "hello" }
                    }
                ]
            }
        })];

        let result = build_step_list(&steps, &serde_json::json!({}));

        assert_eq!(result[0].display_name, "Run echo first");
        assert_eq!(result[0].env.get("VAL").map(String::as_str), Some("hello"));
        assert!(
            matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo first")
        );
    }

    #[test]
    fn build_step_list_parses_aksh_template_string_maps() {
        let steps = vec![serde_json::json!({
            "id": "step1",
            "reference": {
                "type": "script"
            },
            "inputs": {
                "type": 2,
                "map": [
                    {
                        "key": "script",
                        "value": "echo line1\necho line2\n"
                    }
                ]
            },
            "environment": {
                "type": 2,
                "map": [
                    {
                        "key": "VAL",
                        "value": "hello"
                    }
                ]
            }
        })];

        let result = build_step_list(&steps, &serde_json::json!({}));

        assert_eq!(result[0].display_name, "Run echo line1");
        assert_eq!(result[0].env.get("VAL").map(String::as_str), Some("hello"));
        assert!(
            matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo line1\necho line2\n")
        );
    }

    #[test]
    fn build_step_list_handles_continue_on_error() {
        let steps = vec![serde_json::json!({
            "id": "risky",
            "displayName": "Risky step",
            "continueOnError": true,
            "run": "exit 1"
        })];

        let result = build_step_list(&steps, &serde_json::json!({}));
        assert!(result[0].continue_on_error);
    }

    #[test]
    fn inject_actions_env_from_system_vss_endpoint_data() {
        let mut job = JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.workspace = Some("_work/repo/repo".into());

        let msg = serde_json::json!({
            "resources": {
                "endpoints": [{
                    "name": "SystemVssConnection",
                    "url": "https://run-actions.example/45/",
                    "authorization": {
                        "parameters": {
                            "AccessToken": "runtime-token"
                        }
                    },
                    "data": {
                        "ResultsServiceUrl": "https://results.example/",
                        "CacheServerUrl": "https://cache.example/",
                        "GenerateIdTokenUrl": "https://run-actions.example/idtoken"
                    }
                }]
            }
        });

        inject_github_env(&mut job, &msg);

        assert_eq!(
            job.env.get("ACTIONS_RUNTIME_URL").map(String::as_str),
            Some("https://run-actions.example/45/")
        );
        assert_eq!(
            job.env.get("ACTIONS_RUNTIME_TOKEN").map(String::as_str),
            Some("runtime-token")
        );
        assert_eq!(
            job.env
                .get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
                .map(String::as_str),
            Some("runtime-token")
        );
        assert_eq!(
            job.env.get("ACTIONS_RESULTS_URL").map(String::as_str),
            Some("https://results.example/")
        );
        assert_eq!(
            job.env.get("ACTIONS_CACHE_URL").map(String::as_str),
            Some("https://cache.example")
        );
        assert_eq!(
            job.env.get("ACTIONS_CACHE_SERVICE_V2").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            job.env
                .get("ACTIONS_ID_TOKEN_REQUEST_URL")
                .map(String::as_str),
            Some("https://run-actions.example/idtoken")
        );
        assert!(job.mask_secrets("runtime-token").contains("***"));
    }

    #[test]
    fn injects_job_environment_variables_from_acquire_payload() {
        let mut job = JobContext::new(
            "j1".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.workspace = Some("_work/repo/repo".into());

        let msg = serde_json::json!({
            "environmentVariables": [{
                "type": 2,
                "map": [{
                    "Key": { "type": 0, "lit": "MEGA_GLOBAL_ENV" },
                    "Value": { "type": 0, "lit": "global-env-ok" }
                }]
            }]
        });

        inject_github_env(&mut job, &msg);

        assert_eq!(
            job.env.get("MEGA_GLOBAL_ENV").map(String::as_str),
            Some("global-env-ok")
        );
    }
    #[test]
    fn lifecycle_uses_resolved_action_path_and_entry_overrides() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = temp.path().join("_work/repo/repo");
        let action_dir = temp
            .path()
            .join("_work/_actions/actions/example/0123456789abcdef");
        std::fs::create_dir_all(&action_dir).unwrap();
        std::fs::write(
            action_dir.join("action.yml"),
            r#"
name: example
runs:
  using: node20
  main: main.js
  pre: pre.js
  post: cleanup.js
"#,
        )
        .unwrap();

        let main_steps = vec![Step {
            id: "main-action".into(),
            context_name: "main-action".into(),
            display_name: "Example".into(),
            step_type: StepType::Action {
                uses: "actions/example@v1".into(),
                with: serde_json::json!({"token": "x"}),
            },
            condition: Some("success()".into()),
            continue_on_error: false,
            timeout_minutes: None,
            env: std::collections::HashMap::new(),
            raw: serde_json::json!({}),
        }];
        let mut action_paths = std::collections::HashMap::new();
        action_paths.insert(
            "actions/example@v1".to_string(),
            action_dir.to_string_lossy().to_string(),
        );

        let ordered =
            build_step_list_with_lifecycle(main_steps, workspace.to_str().unwrap(), &action_paths);

        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].id, "__pre_main-action");
        assert_eq!(ordered[1].id, "main-action");
        assert_eq!(ordered[2].id, "__post_main-action");
        assert!(matches!(
            &ordered[0].step_type,
            StepType::Action { with, .. }
                if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("pre.js")
        ));
        assert!(matches!(
            &ordered[2].step_type,
            StepType::Action { with, .. }
                if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("cleanup.js")
        ));
        assert_eq!(ordered[2].condition.as_deref(), Some("always()"));
    }

    #[test]
    fn lifecycle_registers_docker_action_pre_and_post() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = temp.path().join("_work/repo/repo");
        let action_dir = temp
            .path()
            .join("_work/_actions/actions/docker-action/0123456789abcdef");
        std::fs::create_dir_all(&action_dir).unwrap();
        std::fs::write(
            action_dir.join("action.yml"),
            r#"
name: docker action
runs:
  using: docker
  image: Dockerfile
  pre-entrypoint: pre-entrypoint.sh
  post-entrypoint: post-entrypoint.sh
  post-if: always()
"#,
        )
        .unwrap();

        let main_steps = vec![Step {
            id: "docker-action".into(),
            context_name: "docker-action".into(),
            display_name: "Docker Action".into(),
            step_type: StepType::Action {
                uses: "actions/docker-action@v1".into(),
                with: serde_json::json!({}),
            },
            condition: Some("success()".into()),
            continue_on_error: false,
            timeout_minutes: None,
            env: std::collections::HashMap::new(),
            raw: serde_json::json!({}),
        }];
        let mut action_paths = std::collections::HashMap::new();
        action_paths.insert(
            "actions/docker-action@v1".to_string(),
            action_dir.to_string_lossy().to_string(),
        );

        let ordered =
            build_step_list_with_lifecycle(main_steps, workspace.to_str().unwrap(), &action_paths);

        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered[0].id, "__pre_docker-action");
        assert_eq!(ordered[1].id, "docker-action");
        assert_eq!(ordered[2].id, "__post_docker-action");
        assert!(matches!(
            &ordered[0].step_type,
            StepType::Action { with, .. }
                if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("pre-entrypoint.sh")
        ));
        assert!(matches!(
            &ordered[2].step_type,
            StepType::Action { with, .. }
                if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("post-entrypoint.sh")
        ));
        assert_eq!(ordered[2].condition.as_deref(), Some("always()"));
    }

    #[test]
    fn test_golden_acquirejob_payloads_parsing() {
        let scenarios = &[
            "06-multi-step",
            "08-job-outputs-needs",
            "10-uses-checkout",
            "11-cache-roundtrip",
            "12-artifact",
            "13-composite-action",
            "14-annotations",
            "15-oidc-id-token",
        ];

        for scenario in scenarios {
            let msg = load_golden_acquirejob(scenario)
                .unwrap_or_else(|| panic!("failed to load golden acquirejob for {scenario}"));

            // 1. Build step list from raw steps
            let steps = msg
                .get("steps")
                .and_then(|v| v.as_array())
                .expect("missing steps in golden");
            let parsed_steps = build_step_list(steps, &msg);
            assert!(
                !parsed_steps.is_empty(),
                "parsed steps must not be empty for {scenario}"
            );

            // 2. Inject environment and verify GITHUB_REPOSITORY is parsed
            let mut job = JobContext::new(
                "job1".into(),
                "test-job".into(),
                serde_json::json!({}),
                serde_json::json!({}),
            );
            job.workspace = Some("_work/repo/repo".into());
            inject_github_env(&mut job, &msg);

            // GITHUB_REPOSITORY must be set and not empty (from contextData.github.repository)
            let repo = job.env.get("GITHUB_REPOSITORY").map(|s| s.as_str());
            assert_eq!(
                repo,
                Some("preloopdev/aksh-conformance-sample"),
                "mismatched GITHUB_REPOSITORY in {scenario}"
            );

            // GITHUB_TOKEN must be set and not empty
            let token = job.env.get("GITHUB_TOKEN").map(|s| s.as_str());
            assert!(
                token.is_some() && !token.unwrap().is_empty(),
                "GITHUB_TOKEN must not be empty in {scenario}"
            );

            // 3. Scenario-specific checks
            if *scenario == "10-uses-checkout" {
                // Verify actions/checkout has @v4 ref combined
                let checkout_step = parsed_steps
                    .iter()
                    .find(|s| match &s.step_type {
                        StepType::Action { uses, .. } => uses.starts_with("actions/checkout"),
                        _ => false,
                    })
                    .expect("missing checkout step");
                if let StepType::Action { uses, .. } = &checkout_step.step_type {
                    assert_eq!(uses, "actions/checkout@v4");
                }
            } else if *scenario == "13-composite-action" {
                // Verify local action has repositoryType=self path
                let composite_step = parsed_steps
                    .iter()
                    .find(|s| match &s.step_type {
                        StepType::Action { uses, .. } => uses.starts_with("./"),
                        _ => false,
                    })
                    .expect("missing composite step");
                if let StepType::Action { uses, .. } = &composite_step.step_type {
                    assert_eq!(uses, "./.github/actions/greet");
                }
            }
        }
    }

    fn load_golden_acquirejob(scenario: &str) -> Option<serde_json::Value> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};
        let path = format!("../../.runner-watch/golden/v2.335.1/{scenario}/flows.jsonl");
        let file = File::open(path).ok()?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line.ok()?;
            let d: serde_json::Value = serde_json::from_str(&line).ok()?;
            if d.get("path")
                .and_then(|v| v.as_str())
                .map(|s| s.contains("acquirejob"))
                .unwrap_or(false)
            {
                return d.get("response_body_json").cloned();
            }
        }
        None
    }
}
