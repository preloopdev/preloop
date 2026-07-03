//! Job execution contexts (github, runner, job, steps, env, secrets).

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

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
    /// Job-level outputs collected from steps.
    pub outputs: HashMap<String, String>,
    /// Job status for status functions (success/failure/cancelled).
    pub job_status: JobStatus,
    /// State values saved by steps (for post-steps).
    pub state: HashMap<String, HashMap<String, String>>,
    /// Annotations collected per step_id (F025).
    pub step_annotations: HashMap<String, Vec<Annotation>>,
    /// Resolved action directories keyed by the original `uses:` reference.
    pub action_paths: HashMap<String, String>,
    /// P1.6: Active problem matchers (cross-step, registered by actions like setup-node).
    pub matchers: MatcherRegistry,
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
            for (_, v) in vars {
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
            masks,
            outputs: HashMap::new(),
            job_status: JobStatus::Success,
            state: HashMap::new(),
            step_annotations: HashMap::new(),
            action_paths: HashMap::new(),
            matchers: MatcherRegistry::new(),
        }
    }

    /// Get the value of a variable by key.
    pub fn get_variable(&self, key: &str) -> Option<&str> {
        self.variables
            .get(key)
            .and_then(|v| v.get("value"))
            .and_then(|v| v.as_str())
    }

    /// Add a mask value (secrets, add-mask command).
    pub fn add_mask(&mut self, value: &str) {
        if !value.is_empty() {
            self.masks.insert(value.to_string());
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
        for key in ["matrix", "needs", "strategy", "vars", "inputs"] {
            if let Some(val) = self.context_data.get(key) {
                ctx.insert(key, val.clone());
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
}
