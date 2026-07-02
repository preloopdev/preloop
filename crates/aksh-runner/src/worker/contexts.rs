//! Job execution contexts (github, runner, job, steps, env, secrets).

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};

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

    /// Mask secret values in a string.
    pub fn mask_secrets(&self, input: &str) -> String {
        let mut result = input.to_string();
        for secret in &self.masks {
            if !secret.is_empty() {
                result = result.replace(secret, "***");
            }
        }
        result
    }

    /// Build the expression evaluation context for condition evaluation.
    pub fn build_expression_context(&self) -> aksh_gha_expressions::Context {
        let mut ctx = aksh_gha_expressions::Context::new();

        // github context from contextData
        if let Some(github) = self.context_data.get("github") {
            ctx.insert("github", github.clone());
        }

        // runner context
        let runner_ctx = serde_json::json!({
            "name": self.job_name,
            "os": current_os(),
            "arch": current_arch(),
            "temp": std::env::temp_dir().to_string_lossy().to_string(),
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

        // job context
        let job_ctx = serde_json::json!({
            "status": match self.job_status {
                JobStatus::Success => "success",
                JobStatus::Failure => "failure",
                JobStatus::Cancelled => "cancelled",
            },
        });
        ctx.insert("job", job_ctx);

        // env context
        let env_map: serde_json::Value = self
            .env
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v)))
            .collect();
        ctx.insert("env", env_map);

        // matrix/needs/strategy/vars/inputs from contextData
        for key in ["matrix", "needs", "strategy", "vars", "inputs"] {
            if let Some(val) = self.context_data.get(key) {
                ctx.insert(key, val.clone());
            }
        }

        // Set status function values
        let ctx = ctx.with_status(
            self.job_status == JobStatus::Success,
            self.job_status == JobStatus::Failure,
            self.job_status == JobStatus::Cancelled,
        );

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
        assert_eq!(success.unwrap(), true);
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
