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
    let github = msg
        .get("contextData")
        .and_then(|cd| cd.get("github"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

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
        ("GITHUB_TOKEN", str_from_json(&github, "token")),
        // Runner variables
        ("RUNNER_NAME", job.job_name.clone()),
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
    ];

    for (key, value) in vars {
        if !value.is_empty() {
            job.env.insert(key.to_string(), value);
        }
    }

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

    // F021: ACTIONS_* runtime env plumbing
    // Extract SystemVssConnection endpoint for ACTIONS_RUNTIME_URL / TOKEN
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
                    // Mask the runtime token (it's a short-lived access token)
                    job.add_mask(token);
                }
                break;
            }
        }
    }

    // Extract results/cache/OIDC URLs from system.* variables
    // Golden 06: system.github.results_endpoint = https://results-receiver.actions.githubusercontent.com/
    if let Some(vars) = msg.get("variables").and_then(|v| v.as_object()) {
        let mappings: &[(&str, &str)] = &[
            ("system.github.results_endpoint", "ACTIONS_RESULTS_URL"),
            ("system.github.results_endpoint", "ACTIONS_CACHE_URL"),
            (
                "system.github.cache_service_v2",
                "ACTIONS_CACHE_SERVICE_V2",
            ),
            (
                "system.github.id_token_request_url",
                "ACTIONS_ID_TOKEN_REQUEST_URL",
            ),
        ];
        for (var_key, env_key) in mappings {
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

/// Build the ordered step list from the job message steps.
pub fn build_step_list(steps: &[serde_json::Value], _job_message: &serde_json::Value) -> Vec<Step> {
    let mut result = Vec::new();

    for (i, step) in steps.iter().enumerate() {
        let id = step
            .get("id")
            .or_else(|| step.get("contextName"))
            .and_then(|v| v.as_str())
            .unwrap_or(&format!("step_{i}"))
            .to_string();

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

        // Extract step env
        let inputs = extract_template_map(step.get("inputs").unwrap_or(&serde_json::Value::Null));

        let env = extract_step_env(step);

        // Determine step type
        let reference = step.get("reference");
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
                    // Action reference
                    let uses = ref_val
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
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

        let display_name =
            display_name_override.unwrap_or_else(|| display_name_for_step(&id, &step_type));

        result.push(Step {
            id,
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
}

fn display_name_for_step(id: &str, step_type: &StepType) -> String {
    match step_type {
        StepType::Script { script, .. } if !script.trim().is_empty() => {
            let first_line = script.lines().next().unwrap_or("").trim();
            format!("Run {first_line}")
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
}
