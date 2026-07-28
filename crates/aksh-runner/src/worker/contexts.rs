//! Job execution contexts (github, runner, job, steps, env, secrets).

use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::worker::execution_types::Annotation;
use crate::worker::matchers::MatcherRegistry;

/// The top-level job context holding all sub-contexts and accumulated state.
#[derive(Clone)]
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
    /// Synthetic step IDs for "Set up job" and "Complete job" (generated in steps_runner, read in job_runner).
    pub setup_step_id: Option<String>,
    pub complete_step_id: Option<String>,
    /// DAP debugger for this job. `None` unless the acquire response set
    /// `enableDebugger=true` and provided a valid `DebuggerTunnelInfo`.
    /// Mirrors `GlobalContext.Debugger` in `actions/runner` v2.335.0+.
    pub dap_debugger: Option<Arc<dyn aksh_dap::IDapDebugger>>,
    /// Debugger connection telemetry entries for completejob.
    pub debugger_telemetry: Vec<String>,
    /// Actions upgraded from node20 to node24 by migration policy.
    pub upgraded_node24_actions: Vec<String>,
    /// Actions still using deprecated node20 (for warning).
    pub deprecated_node20_actions: Vec<String>,
    /// v2.336.0 (#4527): Job-scoped artifact subjects from $GITHUB_ARTIFACTS.
    /// Keyed by canonical subject name; value is (digest, kind).
    pub artifact_subjects: IndexMap<String, ArtifactSubject>,
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("job_id", &self.job_id)
            .field("job_name", &self.job_name)
            .field("job_status", &self.job_status)
            .field("dap_debugger", &self.dap_debugger.is_some())
            .finish_non_exhaustive()
    }
}

/// v2.336.0 (#4527): Artifact subject declared via $GITHUB_ARTIFACTS.
#[derive(Debug, Clone)]
pub struct ArtifactSubject {
    pub name: String,
    pub digest: String,
    pub kind: String,
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
            setup_step_id: None,
            complete_step_id: None,
            dap_debugger: None,
            debugger_telemetry: Vec::new(),
            upgraded_node24_actions: Vec::new(),
            deprecated_node20_actions: Vec::new(),
            artifact_subjects: IndexMap::new(),
        }
    }

    /// Record an action that was upgraded from node20 to node24 by migration policy.
    pub fn record_upgraded_node24_action(&mut self, name: &str) {
        if !self.upgraded_node24_actions.iter().any(|n| n == name) {
            self.upgraded_node24_actions.push(name.to_string());
        }
    }

    /// Record an action still using deprecated node20.
    pub fn record_deprecated_node20_action(&mut self, name: &str) {
        if !self.deprecated_node20_actions.iter().any(|n| n == name) {
            self.deprecated_node20_actions.push(name.to_string());
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
        // actions/runner v2.335.1 AddMaskCommandExtension.ProcessCommand registers
        // Pinned upstream contract (actions/runner v2.335.1, AddMaskCommandExtension):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionCommandManager.cs#L419-L448
        // the raw command data and each non-empty, trimmed CR/LF-delimited line.
        if value.trim().is_empty() {
            return;
        }
        self.masks.insert(value.to_string());
        if let Ok(mut live) = self.live_masks.write() {
            live.insert(value.to_string());
        }
        for line in value
            .split(['\r', '\n'])
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            self.masks.insert(line.to_string());
            if let Ok(mut live) = self.live_masks.write() {
                live.insert(line.to_string());
            }
        }
    }

    /// Mask secret values in a string (longest secrets first to prevent partial matches).
    pub fn mask_secrets(&self, input: &str) -> String {
        let exclude: &[&str] = if self.dap_debugger.is_some() {
            aksh_dap::DAP_PROTOCOL_KEYWORDS
        } else {
            &[]
        };
        aksh_gha_protocol::masking::mask_secrets(
            input,
            self.masks.iter().map(String::as_str),
            exclude,
        )
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
    /// Whether the runner should mirror step issues onto the job annotation list.
    ///
    /// This is the v2.335.1 `actions_send_job_level_annotations` feature flag.
    pub fn send_job_level_annotations_enabled(&self) -> bool {
        self.get_variable_bool("actions_send_job_level_annotations")
    }

    /// Aggregate annotations emitted while executing a step into the job projection.
    /// Step annotations remain stored in `step_annotations` as well.
    pub fn add_step_annotations_to_job(&mut self, annotations: &[Annotation]) {
        if self.send_job_level_annotations_enabled() {
            self.job_annotations.extend(annotations.iter().cloned());
        }
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
            // `github.workspace` is supplied by the runner at execution time,
            // not by the server's job message. Actions use it in input
            // defaults such as `working-directory: ${{ github.workspace }}`.
            // Fill it in only when `context_data` has nothing usable, so a
            // value written through `set_github_context_value` — the mutable
            // source of truth — is not silently overwritten on every rebuild.
            if let Some(workspace) = &self.workspace {
                if let Some(obj) = gh.as_object_mut() {
                    let current = obj.get("workspace").and_then(|v| v.as_str()).unwrap_or("");
                    if current.is_empty() {
                        obj.insert("workspace".to_string(), serde_json::json!(workspace));
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
            // GitHub exposes these context fields as lowercase strings even
            // though the runner's internal result model uses title case.
            step_val.insert(
                "outcome".to_string(),
                serde_json::json!(result.outcome.to_ascii_lowercase()),
            );
            step_val.insert(
                "conclusion".to_string(),
                serde_json::json!(result.conclusion.to_ascii_lowercase()),
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
        let job_decoded = self
            .context_data
            .get("job")
            .map(super::job_extension::decode_typed_value)
            .unwrap_or_else(|| serde_json::json!({}));
        let job_container = job_decoded
            .get("container")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let job_services = job_decoded
            .get("services")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let mut job_ctx_obj = serde_json::json!({
            "status": match self.job_status {
                JobStatus::Success => "success",
                JobStatus::Failure => "failure",
                JobStatus::Cancelled => "cancelled",
            },
            "container": job_container,
            "services": job_services,
        });
        if let Some(obj) = job_ctx_obj.as_object_mut() {
            if let Some(wref) = job_decoded.get("workflow_ref").cloned() {
                obj.insert("workflow_ref".to_string(), wref);
            }
            if let Some(wsha) = job_decoded.get("workflow_sha").cloned() {
                obj.insert("workflow_sha".to_string(), wsha);
            }
            if let Some(wrepo) = job_decoded.get("workflow_repository").cloned() {
                obj.insert("workflow_repository".to_string(), wrepo);
            }
        }
        ctx.insert("job", job_ctx_obj);

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
                // `system.*` variables are runner/server plumbing — the job
                // runtime token, the debug-worker token — not workflow
                // secrets. Surfacing them here would hand untrusted workflow
                // YAML the very credentials it is meant to be fenced off
                // from, via `${{ secrets['system.preloop.debug_worker_token'] }}`.
                // Real GitHub Actions does not expose them either. Every
                // legitimate consumer reads these straight out of the raw
                // `variables` map by exact key, so nothing is starved; log
                // masking is applied independently in `new`, so they stay
                // redacted regardless.
                //
                // Prefix-matched rather than allowlisted deliberately, so a
                // future `system.*` credential is fenced off by default
                // instead of leaking until someone remembers to list it. The
                // cost is that a user secret named `system.foo` would be
                // dropped here: user secret names are not validated
                // (aksh-gha-parser job_builder.rs:257). That fails closed
                // rather than open, the value is still masked, and GitHub
                // secret names cannot contain `.`, so nothing valid collides.
                if key.starts_with("system.") {
                    continue;
                }
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
#[path = "contexts_tests.rs"]
mod tests;
