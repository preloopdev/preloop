//! Job execution contexts (github, runner, job, steps, env, secrets).

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::worker::execution_context::Annotation;
use crate::worker::matchers::MatcherRegistry;

/// The top-level job context holding all sub-contexts and accumulated state.
#[derive(Debug, Clone)]
pub struct JobContext {
    pub job_id: String,
    pub job_name: String,
    pub variables: serde_json::Value,
    pub context_data: serde_json::Value,
    pub workspace: Option<String>,

    /// Accumulated environment from GITHUB_ENV file commands.
    pub env: HashMap<String, String>,
    /// Accumulated PATH additions from GITHUB_PATH.
    pub extra_path: Vec<String>,
    /// Per-step results: step_id → StepResult.
    pub steps: IndexMap<String, StepResult>,
    /// Secret values to mask in logs.
    pub masks: HashSet<String>,
    /// Shared read-view of masks for live-log callbacks that need to see
    /// `::add-mask::` additions mid-step without rebuilding the callback.
    pub live_masks: Arc<RwLock<HashSet<String>>>,
    /// Job-level outputs collected from steps.
    pub outputs: HashMap<String, String>,
    /// Job status for status functions (success/failure/cancelled).
    pub job_status: JobStatus,
    /// State values saved by steps (for post-steps).
    pub state: HashMap<String, HashMap<String, String>>,
    /// Annotations collected per step_id (F025).
    pub step_annotations: HashMap<String, Vec<Annotation>>,
    /// Job-level annotations for completejob (F048).
    /// These are infrastructure-level issues (container failures, action download errors)
    /// that are not tied to a specific step.
    pub job_annotations: Vec<Annotation>,
    /// Resolved action directories keyed by the original `uses:` reference.
    pub action_paths: HashMap<String, String>,
    /// P1.6: Active problem matchers (cross-step, registered by actions like setup-node).
    pub matchers: MatcherRegistry,
    /// Container state for job/service containers (Phase 2).
    pub container_state: Option<super::container_ops::ContainerState>,
    /// Live log queue for WebSocket streaming (None when not connected).
    pub live_logs: Option<std::sync::Arc<crate::worker::live_logs::LiveLogQueue>>,
}

/// Result of a completed step.
#[derive(Debug, Clone)]
pub struct StepResult {
    pub outcome: String,
    pub conclusion: String,
    pub outputs: HashMap<String, String>,
}

/// Job execution status for condition evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Success,
    Failure,
    Cancelled,
}

impl JobContext {
    /// Create a new job context from the job message fields.
    pub fn new(
        job_id: String,
        job_name: String,
        variables: serde_json::Value,
        context_data: serde_json::Value,
    ) -> Self {
        // Extract mask hints from variables
        let mut masks = HashSet::new();
        if let Some(vars) = variables.as_object() {
            for (k, v) in vars {
                if k.is_empty() {
                    continue;
                }
                let is_secret = v.get("isSecret").and_then(|s| s.as_bool()).unwrap_or(false);
                if is_secret {
                    if let Some(val) = v.get("value").and_then(|s| s.as_str()) {
                        if !val.is_empty() {
                            masks.insert(val.to_string());
                            // Also mask trimmed variant and base64-encoded form (F028)
                            let trimmed = val.trim();
                            if trimmed != val {
                                masks.insert(trimmed.to_string());
                            }
                            use base64::engine::Engine as _;
                            masks.insert(base64::engine::general_purpose::STANDARD.encode(val));
                            masks.insert(
                                base64::engine::general_purpose::STANDARD_NO_PAD.encode(val),
                            );
                            masks.insert(base64::engine::general_purpose::URL_SAFE.encode(val));
                            masks.insert(
                                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(val),
                            );
                        }
                    }
                }
            }
        }

        Self {
            job_id,
            job_name,
            variables,
            context_data,
            workspace: None,
            env: HashMap::new(),
            extra_path: Vec::new(),
            steps: IndexMap::new(),
            masks: masks.clone(),
            live_masks: Arc::new(RwLock::new(masks)),
            outputs: HashMap::new(),
            job_status: JobStatus::Success,
            step_annotations: HashMap::new(),
            job_annotations: Vec::new(),
            state: HashMap::new(),
            action_paths: HashMap::new(),
            matchers: MatcherRegistry::new(),
            container_state: None,
            live_logs: None,
        }
    }

    /// Get the value of a variable by key. Supports case-insensitive lookup.
    /// If key is found but value is null, returns empty string `""` (C# parity).
    pub fn get_variable(&self, key: &str) -> Option<&str> {
        let key_lower = key.to_lowercase();
        if let Some(obj) = self.variables.as_object() {
            for (k, v) in obj {
                if k.is_empty() {
                    continue;
                }
                if k.to_lowercase() == key_lower {
                    if let Some(val_node) = v.get("value") {
                        if val_node.is_null() {
                            return Some("");
                        }
                        return Some(val_node.as_str().unwrap_or(""));
                    }
                    return Some("");
                }
            }
        }
        None
    }

    /// Get a variable parsed as boolean. Does not throw if not found or null.
    pub fn get_variable_bool(&self, key: &str) -> bool {
        self.get_variable(key)
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    /// Add a mask value (secrets, add-mask command).
    pub fn add_mask(&mut self, value: &str) {
        if !value.is_empty() {
            self.masks.insert(value.to_string());
            if let Ok(mut live) = self.live_masks.write() {
                live.insert(value.to_string());
            }
        }
    }

    /// Mask secret values in a string (longest secrets first to prevent partial matches).
    pub fn mask_secrets(&self, input: &str) -> String {
        let mut result = input.to_string();
        // Sort by length descending so longer secrets are replaced before
        // shorter ones that might be a subset (e.g. trimmed variant).
        let mut secrets: Vec<&String> = self.masks.iter().filter(|s| !s.is_empty()).collect();
        secrets.sort_by_key(|b| std::cmp::Reverse(b.len()));
        for secret in secrets {
            result = result.replace(secret.as_str(), "***");
        }
        result
    }

    /// Read one key from the mutable GitHub context.
    pub fn github_context_value(&self, key: &str) -> Option<serde_json::Value> {
        self.context_data
            .get("github")
            .map(super::job_extension::decode_typed_value)
            .and_then(|github| github.get(key).cloned())
    }

    /// Set or remove one key in the mutable GitHub context.
    ///
    /// The official runner's `SetGitHubContext` also affects exported
    /// `GITHUB_*` environment variables for allow-listed string/bool values, so
    /// keep `job.env` in sync for action-scoped fields.
    pub fn set_github_context_value(&mut self, key: &str, value: Option<serde_json::Value>) {
        let mut github = self
            .context_data
            .get("github")
            .map(super::job_extension::decode_typed_value)
            .unwrap_or_else(|| serde_json::json!({}));
        if !github.is_object() {
            github = serde_json::json!({});
        }

        if let Some(obj) = github.as_object_mut() {
            match value {
                Some(value) => {
                    sync_github_env(&mut self.env, key, &value);
                    obj.insert(key.to_string(), value);
                }
                None => {
                    remove_github_env(&mut self.env, key);
                    obj.remove(key);
                }
            }
        }

        if !self.context_data.is_object() {
            self.context_data = serde_json::json!({});
        }
        if let Some(obj) = self.context_data.as_object_mut() {
            obj.insert("github".to_string(), github);
        }
    }

    /// F048: Add a job-level annotation (infrastructure issue).
    pub fn add_job_annotation(&mut self, annotation: Annotation) {
        self.job_annotations.push(annotation);
    }

    /// Build the expression evaluation context for condition evaluation.
    pub fn build_expression_context(&self) -> aksh_gha_expressions::Context {
        let mut ctx = aksh_gha_expressions::Context::new();

        // github context from contextData (may be typed-dict encoded)
        if let Some(github) = self.context_data.get("github") {
            let mut gh = super::job_extension::decode_typed_value(github);
            // Token is often in variables, not contextData; inject it if missing
            if gh
                .get("token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
            {
                if let Some(token) = self.env.get("GITHUB_TOKEN") {
                    if let Some(obj) = gh.as_object_mut() {
                        obj.insert("token".to_string(), serde_json::json!(token));
                    }
                }
            }
            ctx.insert("github", gh);
        }

        // runner context — P1.12: add tool_cache and workspace
        let tool_cache = std::env::var("RUNNER_TOOL_CACHE").unwrap_or_else(|_| {
            // Default: runner root / _work / _tool, matching inject_github_env.
            self.workspace
                .as_deref()
                .and_then(|w| std::path::Path::new(w).parent().and_then(|p| p.parent()))
                .map(|p| p.join("_tool").to_string_lossy().to_string())
                .unwrap_or_default()
        });
        let runner_workspace = self.workspace.clone().unwrap_or_default();
        let runner_name = self.env.get("RUNNER_NAME").cloned().unwrap_or_else(|| {
            crate::settings::RunnerConfig::load(std::path::Path::new("."))
                .ok()
                .map(|c| c.settings.agent_name)
                .unwrap_or_else(|| self.job_name.clone())
        });
        let runner_ctx = serde_json::json!({
            "name": runner_name,
            "os": current_os(),
            "arch": current_arch(),
            "temp": std::env::temp_dir().to_string_lossy().to_string(),
            "tool_cache": tool_cache,
            "workspace": runner_workspace,
        });
        ctx.insert("runner", runner_ctx);

        // steps context
        let mut steps_map = serde_json::Map::new();
        for (id, result) in &self.steps {
            let mut step_val = serde_json::Map::new();
            step_val.insert("outcome".to_string(), serde_json::json!(result.outcome));
            step_val.insert(
                "conclusion".to_string(),
                serde_json::json!(result.conclusion),
            );
            let mut outputs_map = serde_json::Map::new();
            for (k, v) in &result.outputs {
                outputs_map.insert(k.clone(), serde_json::json!(v));
            }
            step_val.insert(
                "outputs".to_string(),
                serde_json::Value::Object(outputs_map),
            );
            steps_map.insert(id.clone(), serde_json::Value::Object(step_val));
        }
        ctx.insert("steps", serde_json::Value::Object(steps_map));

        // job context — P1.12: add container and services (empty objects when not containerized)
        let job_container = self
            .context_data
            .get("job")
            .and_then(|j| j.get("container"))
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let job_services = self
            .context_data
            .get("job")
            .and_then(|j| j.get("services"))
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let job_ctx = serde_json::json!({
            "status": match self.job_status {
                JobStatus::Success => "success",
                JobStatus::Failure => "failure",
                JobStatus::Cancelled => "cancelled",
            },
            "container": job_container,
            "services": job_services,
        });
        ctx.insert("job", job_ctx);

        // env context
        let env_map: serde_json::Value = self
            .env
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect();
        ctx.insert("env", env_map);

        // secrets context — from isSecret variables (F028)
        if let Some(vars) = self.variables.as_object() {
            let mut secrets_map = serde_json::Map::new();
            for (key, val) in vars {
                let is_secret = val
                    .get("isSecret")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_secret {
                    if let Some(value) = val.get("value").and_then(|v| v.as_str()) {
                        secrets_map.insert(key.clone(), serde_json::json!(value));
                    }
                }
            }
            ctx.insert("secrets", serde_json::Value::Object(secrets_map));
        }

        // matrix/needs/strategy/vars/inputs from contextData
        // GitHub sends these in Azure DevOps typed-dictionary format;
        // decode before inserting so expression evaluation sees flat objects.
        for key in ["matrix", "needs", "strategy", "vars", "inputs"] {
            if let Some(val) = self.context_data.get(key) {
                ctx.insert(key, super::job_extension::decode_typed_value(val));
            }
        }

        // Set status function values, and pass workspace for hashFiles() (F027)
        let mut ctx = ctx.with_status(
            self.job_status == JobStatus::Success,
            self.job_status == JobStatus::Failure,
            self.job_status == JobStatus::Cancelled,
        );
        if let Some(ws) = &self.workspace {
            ctx = ctx.with_workspace(ws.clone());
        }

        ctx
    }
}

fn sync_github_env(env: &mut HashMap<String, String>, key: &str, value: &serde_json::Value) {
    let Some(env_key) = github_env_key(key) else {
        return;
    };
    match value {
        serde_json::Value::String(s) => {
            env.insert(env_key, s.clone());
        }
        serde_json::Value::Bool(b) => {
            env.insert(env_key, b.to_string());
        }
        _ => {
            env.remove(&env_key);
        }
    }
}

fn remove_github_env(env: &mut HashMap<String, String>, key: &str) {
    if let Some(env_key) = github_env_key(key) {
        env.remove(&env_key);
    }
}

fn github_env_key(key: &str) -> Option<String> {
    // Official GitHubContext.GetRuntimeEnvironmentVariables allowlist.
    const ALLOWLIST: &[&str] = &[
        "action_path",
        "action_ref",
        "action_repository",
        "action",
        "actor",
        "actor_id",
        "api_url",
        "base_ref",
        "env",
        "event_name",
        "event_path",
        "graphql_url",
        "head_ref",
        "job",
        "output",
        "path",
        "ref_name",
        "ref_protected",
        "ref_type",
        "ref",
        "repository",
        "repository_id",
        "repository_owner",
        "repository_owner_id",
        "retention_days",
        "run_attempt",
        "run_id",
        "run_number",
        "server_url",
        "sha",
        "state",
        "step_summary",
        "triggering_actor",
        "workflow",
        "workflow_ref",
        "workflow_sha",
        "workspace",
    ];

    ALLOWLIST
        .contains(&key)
        .then(|| format!("GITHUB_{}", key.to_ascii_uppercase()))
}

fn current_os() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else {
        "Unknown"
    }
}

fn current_arch() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else {
        "X64"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_variables() -> serde_json::Value {
        serde_json::json!({
            "system.github.token": {
                "value": "ghp_secret123",
                "isSecret": true
            },
            "ACTIONS_RUNTIME_URL": {
                "value": "https://results.actions.githubusercontent.com",
                "isSecret": false
            }
        })
    }

    #[test]
    fn new_extracts_masks_from_secret_variables() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test Job".into(),
            make_variables(),
            serde_json::json!({}),
        );
        assert!(ctx.masks.contains("ghp_secret123"));
        assert!(!ctx
            .masks
            .contains("https://results.actions.githubusercontent.com"));
    }

    #[test]
    fn mask_secrets_replaces_with_stars() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test Job".into(),
            make_variables(),
            serde_json::json!({}),
        );
        let masked = ctx.mask_secrets("Token is ghp_secret123 here");
        assert_eq!(masked, "Token is *** here");
        assert!(!masked.contains("ghp_secret123"));
    }

    #[test]
    fn add_mask_adds_new_secret() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.add_mask("my-password");
        assert!(ctx.masks.contains("my-password"));
        assert_eq!(ctx.mask_secrets("my-password is set"), "*** is set");
    }

    #[test]
    fn add_mask_ignores_empty() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.add_mask("");
        assert!(ctx.masks.is_empty());
    }

    #[test]
    fn get_variable_returns_value() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            make_variables(),
            serde_json::json!({}),
        );
        assert_eq!(
            ctx.get_variable("ACTIONS_RUNTIME_URL"),
            Some("https://results.actions.githubusercontent.com")
        );
        assert_eq!(ctx.get_variable("nonexistent"), None);
    }

    #[test]
    fn build_expression_context_has_required_roots() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test Job".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {
                    "repository": "test/repo",
                    "ref": "refs/heads/main"
                }
            }),
        );
        ctx.env.insert("MY_VAR".into(), "hello".into());
        ctx.steps.insert(
            "step1".into(),
            StepResult {
                outcome: "Success".into(),
                conclusion: "Success".into(),
                outputs: HashMap::from([("result".into(), "42".into())]),
            },
        );

        let expr_ctx = ctx.build_expression_context();
        // Verify github context resolves
        let val = aksh_gha_expressions::eval_expression("github.repository", &expr_ctx);
        assert!(val.is_ok());

        // Verify steps context resolves
        let steps_val = aksh_gha_expressions::eval_expression("steps.step1.conclusion", &expr_ctx);
        assert!(steps_val.is_ok());

        // Verify success() evaluates correctly
        let success = aksh_gha_expressions::eval_bool("success()", &expr_ctx);
        assert!(success.unwrap());
    }

    #[test]
    fn job_status_failure_reflects_in_context() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.job_status = JobStatus::Failure;

        let expr_ctx = ctx.build_expression_context();
        let success = aksh_gha_expressions::eval_bool("success()", &expr_ctx).unwrap();
        assert!(!success);
        let failure = aksh_gha_expressions::eval_bool("failure()", &expr_ctx).unwrap();
        assert!(failure);
    }

    #[test]
    fn set_github_context_value_updates_context_and_env() {
        let mut job = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"repository": "owner/repo"}}),
        );

        job.set_github_context_value(
            "action_repository",
            Some(serde_json::json!("actions/checkout")),
        );
        job.set_github_context_value("action_ref", Some(serde_json::json!("v4")));

        let expr = job.build_expression_context();
        assert_eq!(
            expr.resolve(&["github".to_string(), "action_repository".to_string()])
                .as_str(),
            Some("actions/checkout")
        );
        assert_eq!(
            expr.resolve(&["github".to_string(), "action_ref".to_string()])
                .as_str(),
            Some("v4")
        );
        assert_eq!(
            job.env.get("GITHUB_ACTION_REPOSITORY").map(String::as_str),
            Some("actions/checkout")
        );
        assert_eq!(
            job.env.get("GITHUB_ACTION_REF").map(String::as_str),
            Some("v4")
        );

        job.set_github_context_value("action_repository", Some(serde_json::Value::Null));
        assert!(!job.env.contains_key("GITHUB_ACTION_REPOSITORY"));
    }

    #[test]
    fn vars_context_decodes_typed_dict_format() {
        // GitHub sends contextData.vars in Azure DevOps typed-dictionary format:
        // {"t": 2, "d": [{"k": "AKSH_REPO_ROOT", "v": {"t": 1, "d": "/workspace"}}]}
        let ctx = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {"repository": "owner/repo"},
                "vars": {
                    "t": 2,
                    "d": [
                        {"k": "AKSH_REPO_ROOT", "v": {"t": 1, "d": "/workspace"}},
                        {"k": "OTHER_VAR", "v": {"t": 1, "d": "hello"}}
                    ]
                }
            }),
        );

        let expr = ctx.build_expression_context();
        let val = aksh_gha_expressions::eval_expression("vars.AKSH_REPO_ROOT", &expr);
        assert_eq!(val.unwrap().as_str(), Some("/workspace"));
        let val2 = aksh_gha_expressions::eval_expression("vars.OTHER_VAR", &expr);
        assert_eq!(val2.unwrap().as_str(), Some("hello"));
    }

    #[test]
    fn variables_case_insensitive_and_edge_cases() {
        let variables = serde_json::json!({
            "MY_VAR": {"value": "hello", "isSecret": false},
            "secret_var": {"value": "secret123", "isSecret": true},
            "NULL_VAR": {"value": null, "isSecret": false},
            "": {"value": "skipped", "isSecret": false}
        });

        let ctx = JobContext::new(
            "job1".into(),
            "Test Job".into(),
            variables,
            serde_json::json!({}),
        );

        // Case-insensitive lookup
        assert_eq!(ctx.get_variable("MY_VAR"), Some("hello"));
        assert_eq!(ctx.get_variable("my_var"), Some("hello"));
        assert_eq!(ctx.get_variable("My_Var"), Some("hello"));

        // Null variable sets null/empty as empty string ""
        assert_eq!(ctx.get_variable("NULL_VAR"), Some(""));
        assert_eq!(ctx.get_variable("null_var"), Some(""));

        // Empty name is ignored/skipped (or at least cannot be looked up)
        assert_eq!(ctx.get_variable(""), None);

        // Missing returns None
        assert_eq!(ctx.get_variable("MISSING_VAR"), None);
    }

    #[test]
    fn variables_get_boolean_does_not_throw_when_null() {
        let variables = serde_json::json!({
            "TRUE_VAR": {"value": "true", "isSecret": false},
            "FALSE_VAR": {"value": "false", "isSecret": false},
            "NULL_VAR": {"value": null, "isSecret": false}
        });

        let ctx = JobContext::new(
            "job1".into(),
            "Test Job".into(),
            variables,
            serde_json::json!({}),
        );

        assert!(ctx.get_variable_bool("TRUE_VAR"));
        assert!(ctx.get_variable_bool("true_var"));
        assert!(!ctx.get_variable_bool("FALSE_VAR"));
        assert!(!ctx.get_variable_bool("NULL_VAR"));
        assert!(!ctx.get_variable_bool("MISSING_VAR"));
    }

    // --- JobContextL0 gap coverage ---

    #[test]
    fn set_github_context_value_clears_on_none() {
        let mut job = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"repository": "owner/repo"}}),
        );

        // Set workflow_ref
        job.set_github_context_value(
            "workflow_ref",
            Some(serde_json::json!(
                "owner/repo/.github/workflows/ci.yml@refs/heads/main"
            )),
        );
        assert_eq!(
            job.github_context_value("workflow_ref")
                .and_then(|v| v.as_str().map(String::from)),
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_string())
        );
        assert_eq!(
            job.env.get("GITHUB_WORKFLOW_REF").map(String::as_str),
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main")
        );

        // Clear it
        job.set_github_context_value("workflow_ref", None);
        assert!(job.github_context_value("workflow_ref").is_none());
        assert!(!job.env.contains_key("GITHUB_WORKFLOW_REF"));
    }

    #[test]
    fn set_github_context_value_workflow_identity_fields() {
        let mut job = JobContext::new(
            "j1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({"github": {"repository": "owner/repo"}}),
        );

        // Set all workflow identity fields
        job.set_github_context_value(
            "workflow_ref",
            Some(serde_json::json!(
                "owner/repo/.github/workflows/ci.yml@refs/heads/main"
            )),
        );
        job.set_github_context_value("workflow_sha", Some(serde_json::json!("abc123def456")));
        job.set_github_context_value("workflow", Some(serde_json::json!("CI")));

        // Verify all set
        assert_eq!(
            job.github_context_value("workflow_ref")
                .and_then(|v| v.as_str().map(String::from)),
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_string())
        );
        assert_eq!(
            job.github_context_value("workflow_sha")
                .and_then(|v| v.as_str().map(String::from)),
            Some("abc123def456".to_string())
        );
        assert_eq!(
            job.github_context_value("workflow")
                .and_then(|v| v.as_str().map(String::from)),
            Some("CI".to_string())
        );

        // Verify env synced
        assert_eq!(
            job.env.get("GITHUB_WORKFLOW_REF").map(String::as_str),
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main")
        );
        assert_eq!(
            job.env.get("GITHUB_WORKFLOW_SHA").map(String::as_str),
            Some("abc123def456")
        );
        assert_eq!(
            job.env.get("GITHUB_WORKFLOW").map(String::as_str),
            Some("CI")
        );

        // Clear all
        job.set_github_context_value("workflow_ref", None);
        job.set_github_context_value("workflow_sha", None);
        job.set_github_context_value("workflow", None);

        assert!(job.github_context_value("workflow_ref").is_none());
        assert!(job.github_context_value("workflow_sha").is_none());
        assert!(job.github_context_value("workflow").is_none());
        assert!(!job.env.contains_key("GITHUB_WORKFLOW_REF"));
        assert!(!job.env.contains_key("GITHUB_WORKFLOW_SHA"));
        assert!(!job.env.contains_key("GITHUB_WORKFLOW"));
    }

    #[test]
    fn cancelled_status_reflects_in_context() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.job_status = JobStatus::Cancelled;

        let expr_ctx = ctx.build_expression_context();
        assert!(!aksh_gha_expressions::eval_bool("success()", &expr_ctx).unwrap());
        assert!(!aksh_gha_expressions::eval_bool("failure()", &expr_ctx).unwrap());
        assert!(aksh_gha_expressions::eval_bool("cancelled()", &expr_ctx).unwrap());
        assert!(aksh_gha_expressions::eval_bool("always()", &expr_ctx).unwrap());
    }

    // --- P1 expressions/templates gap coverage ---

    #[test]
    fn matrix_context_resolves_in_expressions() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {"repository": "owner/repo"},
                "matrix": {"os": "ubuntu-latest", "node": "20"}
            }),
        );

        let expr_ctx = ctx.build_expression_context();
        assert_eq!(
            aksh_gha_expressions::eval_expression("matrix.os", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("ubuntu-latest")
        );
        assert_eq!(
            aksh_gha_expressions::eval_expression("matrix.node", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("20")
        );
    }

    #[test]
    fn needs_context_resolves_in_expressions() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {"repository": "owner/repo"},
                "needs": {
                    "build": {
                        "result": "success",
                        "outputs": {"sha": "abc123", "version": "1.2.3"}
                    }
                }
            }),
        );

        let expr_ctx = ctx.build_expression_context();
        assert_eq!(
            aksh_gha_expressions::eval_expression("needs.build.result", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("success")
        );
        assert_eq!(
            aksh_gha_expressions::eval_expression("needs.build.outputs.sha", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("abc123")
        );
    }

    #[test]
    fn strategy_context_resolves_in_expressions() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({
                "github": {"repository": "owner/repo"},
                "strategy": {"fail-fast": true, "max-parallel": 2}
            }),
        );

        let expr_ctx = ctx.build_expression_context();
        assert!(aksh_gha_expressions::eval_bool("strategy.fail-fast", &expr_ctx).unwrap());
    }

    #[test]
    fn env_context_resolves_in_expressions() {
        let mut ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.env.insert("MY_VAR".into(), "hello".into());
        ctx.env.insert("OTHER".into(), "world".into());

        let expr_ctx = ctx.build_expression_context();
        assert_eq!(
            aksh_gha_expressions::eval_expression("env.MY_VAR", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("hello")
        );
        assert_eq!(
            aksh_gha_expressions::eval_expression("env.OTHER", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("world")
        );
    }

    #[test]
    fn secrets_context_resolves_in_expressions() {
        let ctx = JobContext::new(
            "job1".into(),
            "Test".into(),
            serde_json::json!({
                "system.github.token": {"value": "ghp_tok", "isSecret": true},
                "MY_SECRET": {"value": "s3cr3t", "isSecret": true}
            }),
            serde_json::json!({}),
        );

        let expr_ctx = ctx.build_expression_context();
        assert_eq!(
            aksh_gha_expressions::eval_expression("secrets.MY_SECRET", &expr_ctx)
                .unwrap()
                .as_str(),
            Some("s3cr3t")
        );
    }
}
