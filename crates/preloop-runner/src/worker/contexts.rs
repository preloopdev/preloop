//! Job execution contexts (github, runner, job, steps, env, secrets).

use indexmap::IndexMap;
use preloop_gha_protocol::azdo::{MaskHint, MaskType};
use preloop_gha_protocol::masking::PatternMasker;
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
    /// Compiled `MaskHint` pattern maskers (`MaskType::Regex`) registered for
    /// this job, shared with the live-log drain path through
    /// [`JobContext::live_mask_set`]. Matches are redacted alongside value
    /// masks; see [`JobContext::apply_mask_hints`].
    pub pattern_masks: Arc<RwLock<Vec<Arc<dyn PatternMasker>>>>,
    /// Job-level outputs collected from steps.
    pub outputs: HashMap<String, String>,
    /// Job status for status functions (success/failure/cancelled).
    pub job_status: JobStatus,
    /// State values saved by steps (for post-steps).
    pub state: HashMap<String, HashMap<String, String>>,
    /// Annotations collected per step_id.
    pub step_annotations: HashMap<String, Vec<Annotation>>,
    /// Job-level annotations for completejob.
    /// These are infrastructure-level issues (container failures, action download errors)
    /// that are not tied to a specific step.
    pub job_annotations: Vec<Annotation>,
    /// Resolved action directories keyed by the original `uses:` reference.
    pub action_paths: HashMap<String, String>,
    /// Active problem matchers (cross-step, registered by actions like setup-node).
    pub matchers: MatcherRegistry,
    /// Container state for job/service containers (Phase 2).
    pub container_state: Option<super::container_ops::ContainerState>,
    /// Container → engine reachability for this job, created with the job
    /// network and used to rewrite engine URLs handed to container steps.
    /// See `container_ops::ContainerEngineAccess`.
    pub container_engine: Option<Arc<super::container_ops::ContainerEngineAccess>>,
    /// Live log queue for WebSocket streaming (None when not connected).
    pub live_logs: Option<std::sync::Arc<crate::worker::live_logs::LiveLogQueue>>,
    /// Synthetic step IDs for "Set up job" and "Complete job" (generated in steps_runner, read in job_runner).
    pub setup_step_id: Option<String>,
    pub complete_step_id: Option<String>,
    /// Ids of the steps the job request message declared.
    ///
    /// The authoritative list of what the workflow asked for, so steps the
    /// runner synthesizes during setup (`Pre`/`Post` hooks discovered from
    /// action manifests, container lifecycle, host hooks) can be told apart by
    /// absence from it rather than by guessing at name prefixes. Empty when the
    /// job carried no steps.
    pub declared_step_ids: std::collections::HashSet<String>,
    /// DAP debugger for this job. `None` unless the acquire response set
    /// `enableDebugger=true` and provided a valid `DebuggerTunnelInfo`.
    /// Mirrors `GlobalContext.Debugger` in `actions/runner` v2.335.0+.
    pub dap_debugger: Option<Arc<dyn preloop_dap::IDapDebugger>>,
    /// Debugger connection telemetry entries for completejob.
    pub debugger_telemetry: Vec<String>,
    /// Actions upgraded from node20 to node24 by migration policy.
    pub upgraded_node24_actions: Vec<String>,
    /// Actions still using deprecated node20 (for warning).
    pub deprecated_node20_actions: Vec<String>,
    /// Whether the Node 20 deprecation warning has already been emitted for this job.
    /// Guard for one-time per-job warning via the node handler; not per step.
    pub node20_warning_emitted: bool,
    /// v2.336.0 (#4527): Job-scoped artifact subjects from $GITHUB_ARTIFACTS.
    /// Keyed by canonical subject name; value is (digest, kind).
    pub artifact_subjects: IndexMap<String, ArtifactSubject>,
    /// Control plane this runner registered against (`.runner` `controlPlane`),
    /// set by `run_job` from the registration the worker validated and
    /// reasserted by `inject_github_env`. Decides whether the
    /// `system.github.token` variable is a usable GitHub credential
    /// (github.com) or the claim's GitHub credential slot, which must stay
    /// off the step surface (engine, the default).
    pub control_plane: crate::settings::ControlPlane,
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

/// The job's mask sources as seen by live-log consumers.
///
/// Value masks are a `HashSet<String>` and cannot express a
/// `MaskType::Regex` hint, so the live-log queue keeps both shared handles
/// and drain-path masking redacts pattern matches alongside values. Built
/// with [`JobContext::live_mask_set`].
#[derive(Clone, Default)]
pub struct LiveMaskSet {
    /// Value masks; the same handle as [`JobContext::live_masks`].
    pub values: Arc<RwLock<HashSet<String>>>,
    /// Compiled pattern maskers; the same handle as
    /// [`JobContext::pattern_masks`].
    pub patterns: Arc<RwLock<Vec<Arc<dyn PatternMasker>>>>,
}

/// A `regex::Regex` registered as a [`PatternMasker`] from a job `MaskHint`
/// of `MaskType::Regex` — the compiled form the official worker builds in
/// `Worker.cs` `InitializeSecretMasker`.
pub(crate) struct RegexMasker(regex::Regex);

impl RegexMasker {
    /// Compile `pattern`. The caller decides how an invalid pattern is
    /// reported; nothing here panics.
    pub(crate) fn compile(pattern: &str) -> Result<Self, regex::Error> {
        regex::Regex::new(pattern).map(Self)
    }
}

impl PatternMasker for RegexMasker {
    fn find_spans(&self, haystack: &str, spans: &mut Vec<(usize, usize)>) {
        spans.extend(self.0.find_iter(haystack).map(|m| (m.start(), m.end())));
    }
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
                if is_secret
                    && let Some(val) = v.get("value").and_then(|s| s.as_str())
                    && !val.is_empty()
                {
                    masks.insert(val.to_string());
                    // Also mask trimmed variant and base64-encoded form.
                    let trimmed = val.trim();
                    if trimmed != val {
                        masks.insert(trimmed.to_string());
                    }
                    use base64::engine::Engine as _;
                    masks.insert(base64::engine::general_purpose::STANDARD.encode(val));
                    masks.insert(base64::engine::general_purpose::STANDARD_NO_PAD.encode(val));
                    masks.insert(base64::engine::general_purpose::URL_SAFE.encode(val));
                    masks.insert(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(val));
                    // actions/runner `ExecutionContext.InitializeJob` also
                    // registers `base64("x-access-token:{token}")` for the
                    // job's GitHub token: that is the git extraheader
                    // actions/checkout persists, and it would otherwise leak
                    // verbatim in a log that echoes remote/auth config.
                    // Resolution accepts `system.github.token` and its
                    // `github_token` alias case-insensitively, so the derived
                    // mask has to cover every spelling it can export.
                    if k.eq_ignore_ascii_case("system.github.token")
                        || k.eq_ignore_ascii_case("github_token")
                    {
                        masks.insert(
                            base64::engine::general_purpose::STANDARD
                                .encode(format!("x-access-token:{val}")),
                        );
                    }
                    // Register each non-empty trimmed CR/LF-delimited
                    // line too. Log masking runs per assembled line, so
                    // a multiline initial secret (PEM key, JSON blob)
                    // would otherwise never match the whole value and
                    // would leak line by line. Mirrors add_mask.
                    for line in val
                        .split(['\r', '\n'])
                        .map(str::trim)
                        .filter(|l| !l.is_empty())
                    {
                        masks.insert(line.to_string());
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
            pattern_masks: Arc::new(RwLock::new(Vec::new())),
            outputs: HashMap::new(),
            job_status: JobStatus::Success,
            step_annotations: HashMap::new(),
            job_annotations: Vec::new(),
            state: HashMap::new(),
            action_paths: HashMap::new(),
            matchers: MatcherRegistry::new(),
            container_state: None,
            container_engine: None,
            live_logs: None,
            setup_step_id: None,
            complete_step_id: None,
            declared_step_ids: std::collections::HashSet::new(),
            dap_debugger: None,
            debugger_telemetry: Vec::new(),
            upgraded_node24_actions: Vec::new(),
            deprecated_node20_actions: Vec::new(),
            node20_warning_emitted: false,
            artifact_subjects: IndexMap::new(),
            control_plane: crate::settings::ControlPlane::default(),
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

    /// Emit the one-time per-job Node 20 deprecation warning if not already emitted.
    /// Returns true if a warning was emitted.
    pub fn emit_node20_deprecation_warning(&mut self) -> bool {
        if self.node20_warning_emitted {
            return false;
        }
        self.node20_warning_emitted = true;
        true
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

    /// The first non-empty value among `keys` (case-insensitive), if any.
    ///
    /// "Present but empty" is not a credential: the job builder writes empty
    /// `isSecret` slots for `github_token` and `system.github.token` before the
    /// claim fills them, so treating such a slot as authoritative would shadow
    /// the token a sibling alias carries in the same message.
    pub fn first_nonempty_variable(&self, keys: &[&str]) -> Option<&str> {
        keys.iter()
            .find_map(|key| self.get_variable(key).filter(|value| !value.is_empty()))
    }

    /// Get a variable parsed as boolean. Does not throw if not found or null.
    pub fn get_variable_bool(&self, key: &str) -> bool {
        self.get_variable(key)
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false)
    }

    /// Add a mask value (secrets, add-mask command), returning the raw value
    /// and every trimmed CR/LF-delimited value that was registered.
    pub fn add_mask(&mut self, value: &str) -> Vec<String> {
        // actions/runner v2.335.1 AddMaskCommandExtension.ProcessCommand registers
        // Pinned upstream contract (actions/runner v2.335.1, AddMaskCommandExtension):
        // https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionCommandManager.cs#L419-L448
        // the raw command data and each non-empty, trimmed CR/LF-delimited line.
        if value.trim().is_empty() {
            return Vec::new();
        }
        let mut added = vec![value.to_string()];
        self.masks.insert(value.to_string());
        if let Ok(mut live) = self.live_masks.write() {
            live.insert(value.to_string());
        }
        for line in value
            .split(['\r', '\n'])
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let line = line.to_string();
            self.masks.insert(line.clone());
            if let Ok(mut live) = self.live_masks.write() {
                live.insert(line.clone());
            }
            added.push(line);
        }
        added
    }

    /// Register the job message's `MaskHint`s, mirroring the official
    /// worker's `Worker.cs` `InitializeSecretMasker`:
    ///
    /// * [`MaskType::Regex`] — the pattern is compiled once and registered
    ///   as a pattern masker, so secret-shaped spans are redacted even when
    ///   no exact value was registered. A pattern that does not compile is
    ///   skipped with a warning; a hint must never fail or panic the job.
    /// * [`MaskType::Variable`] — the named variable's current value is
    ///   masked through [`Self::add_mask`]. Unknown or empty variables are
    ///   skipped.
    pub fn apply_mask_hints(&mut self, hints: &[MaskHint]) {
        for hint in hints {
            match hint.hint_type {
                MaskType::Regex => match RegexMasker::compile(&hint.value) {
                    Ok(masker) => {
                        // Recover a poisoned guard: dropping the masker would
                        // leak every span the pattern matches.
                        self.pattern_masks
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .push(Arc::new(masker));
                    }
                    Err(error) => tracing::warn!(
                        pattern = %hint.value,
                        %error,
                        "skipping mask hint: pattern does not compile"
                    ),
                },
                MaskType::Variable => {
                    if let Some(value) = self.get_variable(&hint.value).map(str::to_owned) {
                        self.add_mask(&value);
                    }
                }
            }
        }
    }

    /// The shared mask handles for live-log consumers
    /// ([`crate::worker::live_logs::LiveLogQueue`] and
    /// `process_line_callback`), so drain-path masking sees both the value
    /// masks and the compiled `MaskType::Regex` hints.
    pub fn live_mask_set(&self) -> LiveMaskSet {
        LiveMaskSet {
            values: Arc::clone(&self.live_masks),
            patterns: Arc::clone(&self.pattern_masks),
        }
    }

    /// Like [`Self::mask_secrets`], but a secret spanning N lines is
    /// replaced with one `***` per line instead of a single marker, so the
    /// physical record count of the output is unchanged.
    ///
    /// Retroactive masking rewrites the durable log after line checkpoints
    /// were captured; collapsing records would invalidate them (see
    /// `StepContext::log_content_since`).
    pub fn mask_secrets_preserving_lines_with(&self, input: &str, secrets: &[String]) -> String {
        let exclude: &[&str] = if self.dap_debugger.is_some() {
            preloop_dap::DAP_PROTOCOL_KEYWORDS
        } else {
            &[]
        };
        let guard = self.pattern_masks.read();
        let empty: &[Arc<dyn PatternMasker>] = &[];
        // A poisoned lock degrades to value-only masking rather than
        // skipping redaction entirely.
        let patterns = match &guard {
            Ok(patterns) => patterns.as_slice(),
            Err(_) => empty,
        };
        preloop_gha_protocol::masking::mask_secrets_preserving_lines_transform_aware_with_patterns(
            input,
            secrets.iter().map(String::as_str),
            patterns.iter().map(|pattern| &**pattern),
            exclude,
        )
    }

    /// Does any registered `MaskType::Regex` hint match `text`?
    ///
    /// The environment-URL guard asks the same question the mask scan asks —
    /// would this text disclose something the job's hints call secret — without
    /// rewriting the text (the official `JobExtension.FinalizeJob` drops a URL
    /// whose masked form differs from the raw value). Value masks cover only
    /// registered values, so a URL matching only a pattern hint (the engine's
    /// `scheme://user:password@host` shape, say) would otherwise be published
    /// while the same span is redacted from the logs.
    pub fn matches_pattern_mask(&self, text: &str) -> bool {
        let guard = self.pattern_masks.read();
        let patterns = match &guard {
            Ok(patterns) => patterns.as_slice(),
            // A poisoned lock degrades to value-only masking, like
            // [`Self::mask_secrets`].
            Err(_) => return false,
        };
        let mut spans = Vec::new();
        for pattern in patterns {
            spans.clear();
            pattern.find_spans(text, &mut spans);
            if !spans.is_empty() {
                return true;
            }
        }
        false
    }

    /// Mask secret values in a string (longest secrets first to prevent partial matches).
    ///
    /// Transform-aware: every secret is also matched by its encoded variants
    /// (base64 standard/URL-safe, percent-encoding, hex) with ASCII
    /// case-insensitive comparison, so `::add-mask::` secrets cannot leak
    /// through trivial encodings.
    pub fn mask_secrets(&self, input: &str) -> String {
        let exclude: &[&str] = if self.dap_debugger.is_some() {
            preloop_dap::DAP_PROTOCOL_KEYWORDS
        } else {
            &[]
        };
        let guard = self.pattern_masks.read();
        let empty: &[Arc<dyn PatternMasker>] = &[];
        // A poisoned lock degrades to value-only masking rather than
        // skipping redaction entirely.
        let patterns = match &guard {
            Ok(patterns) => patterns.as_slice(),
            Err(_) => empty,
        };
        preloop_gha_protocol::masking::mask_secrets_transform_aware_with_patterns(
            input,
            self.masks.iter().map(String::as_str),
            patterns.iter().map(|pattern| &**pattern),
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

    /// Add a job-level annotation (infrastructure issue).
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
    pub fn build_expression_context(&self) -> preloop_gha_expressions::Context {
        let mut ctx = preloop_gha_expressions::Context::new();

        // github context from contextData (may be typed-dict encoded)
        if let Some(github) = self.context_data.get("github") {
            let mut gh = super::job_extension::decode_typed_value(github);
            let context_token_len = gh
                .get("token")
                .and_then(|v| v.as_str())
                .map(|t| t.len())
                .unwrap_or(0);
            // Token is often in variables, not contextData; inject it if missing
            if gh
                .get("token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
                && let Some(obj) = gh.as_object_mut()
            {
                // The official runner fills `github.token` from the
                // `system.github.token` variable before overlaying
                // `contextData.github` (`ExecutionContext.InitializeJob`).
                // That variable is only a GitHub credential when the control
                // plane is GitHub; on a preloop engine it is the control
                // plane's own slot, which the claim fills with the credential
                // it minted (and patches into the context read above) or
                // leaves empty.
                //
                // `job.env` is deliberately not consulted: a workflow- or
                // job-level `env: GITHUB_TOKEN` (and a `GITHUB_ENV` write)
                // replaces the variable the step process sees, but it is not
                // the job's GitHub credential, and upstream never reads it
                // back into the context.
                let variable_token = if self.control_plane == crate::settings::ControlPlane::GitHub
                {
                    self.first_nonempty_variable(&["system.github.token", "github_token"])
                        .unwrap_or("")
                } else {
                    ""
                };
                if !variable_token.is_empty() {
                    obj.insert("token".to_string(), serde_json::json!(variable_token));
                } else if !obj.contains_key("token") {
                    // `StringContextData(null)` in the official runner
                    // renders as the empty string; keep the key present when
                    // no credential exists instead of exposing a null.
                    obj.insert("token".to_string(), serde_json::json!(""));
                }
            }
            tracing::info!(
                context_token_len,
                env_token = self.env.get("GITHUB_TOKEN").map(|t| t.len()).unwrap_or(0),
                "github context token resolution"
            );
            // `github.workspace` is supplied by the runner at execution time,
            // not by the server's job message. Actions use it in input
            // defaults such as `working-directory: ${{ github.workspace }}`.
            // Fill it in only when `context_data` has nothing usable, so a
            // value written through `set_github_context_value` — the mutable
            // source of truth — is not silently overwritten on every rebuild.
            if let Some(workspace) = &self.workspace
                && let Some(obj) = gh.as_object_mut()
            {
                let current = obj.get("workspace").and_then(|v| v.as_str()).unwrap_or("");
                if current.is_empty() {
                    obj.insert("workspace".to_string(), serde_json::json!(workspace));
                }
            }
            ctx.insert("github", gh);
        }

        // runner context — add tool_cache and workspace
        let tool_cache = std::env::var("RUNNER_TOOL_CACHE").unwrap_or_else(|_| {
            // Default: runner root / _work / _tool, matching inject_github_env.
            self.workspace
                .as_deref()
                .and_then(|w| std::path::Path::new(w).parent().and_then(|p| p.parent()))
                .map(|p| p.join("_tool").to_string_lossy().to_string())
                .unwrap_or_default()
        });
        let runner_workspace = self.workspace.clone().unwrap_or_default();
        let runner_name = self
            .env
            .get("RUNNER_NAME")
            .cloned()
            .unwrap_or_else(|| self.job_name.clone());
        let runner_ctx = serde_json::json!({
            "name": runner_name,
            "os": current_os(),
            "arch": current_arch(),
            "temp": std::env::temp_dir().to_string_lossy().to_string(),
            "tool_cache": tool_cache,
            "workspace": runner_workspace,
            // Official runner exposes `runner.environment` as
            // "github-hosted" or "self-hosted"; preloop jobs always run on
            // self-hosted infrastructure.
            "environment": "self-hosted",
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

        // job context — add container and services (empty objects when not containerized)
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
            if let Some(wpath) = job_decoded.get("workflow_file_path").cloned() {
                obj.insert("workflow_file_path".to_string(), wpath);
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

        // secrets context — from isSecret variables
        if let Some(vars) = self.variables.as_object() {
            let mut secrets_map = serde_json::Map::new();
            for (key, val) in vars {
                // `system.*` variables are runner/server plumbing — the job
                // token, the endpoint URLs — not workflow secrets. Surfacing
                // them here would hand untrusted workflow YAML the very
                // credentials it is meant to be fenced off from, via
                // `${{ secrets['system.github.token'] }}`.
                // Real GitHub Actions does not expose them either. Every
                // legitimate consumer reads these straight out of the raw
                // `variables` map by exact key, while the built-in
                // `github_token` variable is mapped to the canonical
                // `secrets.GITHUB_TOKEN` name below; log masking is applied
                // independently in `new`, so they stay redacted regardless.
                //
                // This is defence in depth, not the only defence: a credential
                // that must not reach workflow code is not shipped as a job
                // variable at all, because the official runner has no such
                // filter. The debug-worker token is fetched over
                // `POST /api/v1/debug/worker-token` for exactly that reason.
                //
                // Prefix-matched rather than allowlisted deliberately, so a
                // future `system.*` credential is fenced off by default
                // instead of leaking until someone remembers to list it. The
                // cost is that a user secret named `system.foo` would be
                // dropped here: user secret names are not validated
                // (preloop-gha-parser job_builder.rs:257). That fails closed
                // rather than open, the value is still masked, and GitHub
                // secret names cannot contain `.`, so nothing valid collides.
                if key.starts_with("system.") {
                    continue;
                }
                let is_secret = val
                    .get("isSecret")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_secret && let Some(value) = val.get("value").and_then(|v| v.as_str()) {
                    secrets_map.insert(key.clone(), serde_json::json!(value));
                    // The wire uses the official runner's lower-case
                    // `github_token` variable, while workflow expressions
                    // address the built-in secret as `GITHUB_TOKEN`.
                    if key == "github_token" {
                        secrets_map.insert("GITHUB_TOKEN".to_owned(), serde_json::json!(value));
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

        // Set status function values, and pass workspace for hashFiles()
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
    } else if cfg!(target_os = "windows") {
        "Windows"
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

#[cfg(test)]
mod mask_hint_tests {
    use super::*;

    fn job(variables: serde_json::Value) -> JobContext {
        JobContext::new(
            "job1".to_string(),
            "Test".to_string(),
            variables,
            serde_json::json!({}),
        )
    }

    fn regex_hint(pattern: &str) -> MaskHint {
        MaskHint {
            hint_type: MaskType::Regex,
            value: pattern.to_string(),
        }
    }

    #[test]
    fn regex_mask_hint_masks_span_value_masking_misses() {
        let mut ctx = job(serde_json::json!({}));
        let input = "login user:hunter2@host ok";
        // Without the hint the span survives: `hunter2` is not a registered
        // value, and even registering it only masks the interior word.
        assert_eq!(ctx.mask_secrets(input), input);
        ctx.add_mask("hunter2");
        assert_eq!(ctx.mask_secrets(input), "login user:***@host ok");

        ctx.apply_mask_hints(&[regex_hint(r"user:[^@\s]+@host")]);
        assert_eq!(ctx.mask_secrets(input), "login *** ok");
    }

    #[test]
    fn variable_mask_hint_masks_named_variable_value() {
        let mut ctx = job(serde_json::json!({
            "MY_SECRET": { "value": "tok-9f3k2", "isSecret": false },
        }));
        assert_eq!(ctx.mask_secrets("val=tok-9f3k2"), "val=tok-9f3k2");
        ctx.apply_mask_hints(&[
            MaskHint {
                hint_type: MaskType::Variable,
                value: "MY_SECRET".to_string(),
            },
            // An unknown variable is skipped without effect.
            MaskHint {
                hint_type: MaskType::Variable,
                value: "NOT_A_VARIABLE".to_string(),
            },
        ]);
        assert_eq!(ctx.mask_secrets("val=tok-9f3k2"), "val=***");
    }

    #[test]
    fn invalid_regex_mask_hint_is_skipped_and_masking_continues() {
        let mut ctx = job(serde_json::json!({
            "PLAIN": { "value": "value-secret", "isSecret": true },
        }));
        // Must not panic: the broken pattern is skipped with a warning.
        ctx.apply_mask_hints(&[regex_hint("("), regex_hint(r"user:[^@\s]+@host")]);
        assert_eq!(ctx.mask_secrets("value-secret"), "***");
        assert_eq!(ctx.mask_secrets("see user:hunter2@host"), "see ***");
    }

    #[test]
    fn regex_mask_hint_masks_across_lines_preserving_records() {
        let mut ctx = job(serde_json::json!({}));
        ctx.apply_mask_hints(&[regex_hint(r"(?s)BEGIN.*END")]);
        assert_eq!(
            ctx.mask_secrets_preserving_lines_with("l1\nBEGIN\nsecret\nEND\nl2", &[]),
            "l1\n***\n***\n***\nl2"
        );
    }

    #[test]
    fn no_mask_hints_leave_masking_unchanged() {
        let mut ctx = job(serde_json::json!({}));
        ctx.apply_mask_hints(&[]);
        let input = "plain user:hunter2@host output";
        assert_eq!(ctx.mask_secrets(input), input);
    }
}
