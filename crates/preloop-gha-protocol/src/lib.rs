//! Shared domain and wire models for preloop's GitHub Actions control plane.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// Azure DevOps wire-format DTOs for the official runner protocol.
pub mod azdo;
pub use azdo::DebuggerTunnelInfo;

/// RSA/AES session encryption for the runner protocol.
pub mod crypto;

/// Shared secret-masking logic (longest-first, exclusion-aware).
pub mod masking;

/// Live debug-session DTOs for the native `/api/v1/debug/...` surface.
pub mod debug_session;

/// Protocol version exposed by this crate's runner-compatible DTOs.
pub const PROTOCOL_VERSION: &str = "2026-06-25.preloop.v1";

/// Line a runner prints on stdout the moment it accepts a job.
///
/// An ephemeral runner is single-use, so the orchestrator supervising it can
/// start building its replacement as soon as the current one is spoken for
/// rather than waiting for the job to end. This is a private channel between
/// our runner and our orchestrator; it never crosses the GitHub wire protocol.
pub const RUNNER_BUSY_SENTINEL: &str = "::preloop-runner::job-acquired";

/// Stable identifier for a workflow run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub Uuid);

impl RunId {
    /// Create a fresh run id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Stable identifier for an expanded job.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub String);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable identifier for a runner session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(pub Uuid);

impl SessionId {
    /// Create a fresh runner session id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Secret value that redacts itself in logs and serialized output.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap a secret value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the secret value for explicit use at the protocol boundary.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(<redacted>)")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self)
    }
}

/// Redaction-safe map of secret names to values.
pub type SecretMap = BTreeMap<String, SecretString>;

/// Push-back requested for a run: after the run reaches a terminal state the
/// tested commit is pushed to GitHub and the server creates or updates the
/// pull request and reports check runs. The push itself is performed by the
/// submitting client (its own git credentials); this record only carries the
/// user's intent so the server can gate check reporting and the push
/// endpoint on it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushRequest {
    /// Create a pull request when the branch has no open PR yet.
    pub create_pr: bool,
    /// Create newly-created pull requests as drafts so reviewers are not
    /// notified until the author marks them ready.
    pub draft_pr: bool,
}

/// Complete workflow request submitted to the control plane.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowSubmission {
    /// YAML workflow contents.
    pub workflow_yaml: String,
    /// GitHub event name such as `push` or `workflow_dispatch`.
    pub event: String,
    /// Event payload JSON.
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Repository slug or local identifier.
    pub repository: String,
    #[serde(default = "default_ref")]
    /// Git ref for the run.
    pub git_ref: String,
    /// Repository-relative path of the submitted workflow file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_path: Option<String>,
    /// Canonical host workspace used only by a trusted local control plane.
    ///
    /// This is accepted on input but never returned from run APIs, so host
    /// filesystem layout does not leak into run metadata.
    #[serde(default, skip_serializing)]
    pub local_workspace: Option<String>,
    /// Caller-provided variables.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// Workflow dispatch or call inputs.
    #[serde(default)]
    pub inputs: BTreeMap<String, serde_json::Value>,
    /// Caller-provided secrets.
    #[serde(default)]
    pub secrets: SecretMap,
    /// Names the caller provided before stored-secret merging, so per-job
    /// environment overlays can keep submission-provided values winning per
    /// name. Populated server-side after deserialization; skipped on the
    /// wire and lost if a run's submission is ever rebuilt from persisted
    /// state (in that case stored environment secrets win by name).
    #[serde(default, skip)]
    pub submission_names: BTreeSet<String>,
    /// Local reusable workflow YAML keyed by repository-relative path.
    #[serde(default)]
    pub reusable_workflows: BTreeMap<String, String>,
    /// Resolved commit SHA for each remote reusable workflow reference.
    #[serde(default)]
    pub reusable_workflow_shas: BTreeMap<String, String>,
    /// Enable DAP debugger for the run's jobs.
    #[serde(default)]
    pub enable_debugger: bool,
    /// Welcome message to show when debugger attaches.
    #[serde(default)]
    pub debugger_welcome_message: Option<String>,
    /// Commit SHA for the run. Defaults to zeroes if not supplied.
    #[serde(default = "default_sha")]
    pub sha: String,
    /// The actor (user) who initiated the run. Defaults to `"preloop-system"`.
    #[serde(default = "default_actor")]
    pub actor: String,
    /// Deployment environment name (for OIDC `sub` claim formatting).
    #[serde(default)]
    pub environment: Option<String>,
    /// Workflow filename (e.g. `"ci.yml"`). Derived from YAML or overridden.
    #[serde(default)]
    pub workflow_file: Option<String>,
    /// Trust tier assigned by the webhook dispatcher. The server enforces its
    /// repository-secret policy before building a job; it does not grant an
    /// untrusted payload permission to select a more trusted tier.
    #[serde(default)]
    pub trust_tier: Option<String>,
    /// Upstream workflow display names for `on.workflow_run.workflows:` filter
    /// enforcement. Populated from `workflow_run.name` by the adapter.
    #[serde(default)]
    pub workflow_run_upstream_names: Vec<String>,
    /// Activity type for the event (for example `opened`, `synchronize`, or
    /// `submitted`). Set by the dispatcher so submission does not reinterpret
    /// event-specific payload fields.
    #[serde(default)]
    pub activity_type: Option<String>,
    /// Resolved SHA for the run's `github.sha` context. A webhook adapter owns
    /// this value because it differs from payload `after` for PR-family events.
    #[serde(default)]
    pub resolved_sha: Option<String>,
    /// Explicitly resolved changed paths. An empty list is meaningful only
    /// when `changed_paths_known` is true.
    #[serde(default)]
    pub changed_paths: Vec<String>,
    /// Whether `changed_paths` represents a complete change set.
    #[serde(default)]
    pub changed_paths_known: bool,
    /// Branch used for trigger filtering, independent of `git_ref`.
    #[serde(default)]
    pub filter_branch: Option<String>,
    #[serde(default)]
    /// Typed workflow_dispatch inputs.
    pub dispatch_inputs: BTreeMap<String, serde_json::Value>,
    /// String-valued workflow_dispatch inputs for `github.event.inputs`.
    #[serde(default)]
    pub dispatch_inputs_stringified: BTreeMap<String, String>,
    /// Run only these jobs (by YAML key) and their transitive `needs:`
    /// dependencies. An empty list means run all jobs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub selected_jobs: Vec<String>,
    /// Explicit base ref for the run (populates `github.base_ref`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Keep the failed job VM alive for interactive debugging.
    #[serde(default)]
    pub preserve_on_failure: bool,
    /// Push-back requested after the run completes. Absent means the run is
    /// a plain local submission with no GitHub interaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<PushRequest>,
    /// `git rev-parse HEAD^{tree}` of the tree the workspace snapshot was
    /// taken from. The push endpoint refuses to report checks unless the
    /// pushed commit's tree matches this, so a run can never vouch for a
    /// commit it did not test.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_tree: Option<String>,
}

impl WorkflowSubmission {
    /// Serialize for transmission to the control plane, exposing secret values.
    ///
    /// [`SecretString`] redacts on `Serialize` so that a submission embedded in
    /// server state (for example `RunRecord`) can never leak secrets through an
    /// API response. Sending secrets is the one legitimate exception, so it is
    /// opt-in at the call site rather than a property of the type.
    pub fn to_request_json(&self) -> Result<serde_json::Value, serde_json::Error> {
        let mut value = serde_json::to_value(self)?;
        if self.secrets.is_empty() {
            return Ok(value);
        }
        // Resolve the whole map once through the sanctioned masking boundary,
        // then wrap the already-plaintext values for JSON.
        let exposed = masking::expose_all(&self.secrets)
            .into_iter()
            .map(|(name, value)| (name, serde_json::Value::String(value)))
            .collect();
        if let Some(object) = value.as_object_mut() {
            object.insert("secrets".to_owned(), serde_json::Value::Object(exposed));
        }
        Ok(value)
    }
}

fn default_ref() -> String {
    "refs/heads/main".to_owned()
}

fn default_sha() -> String {
    "0000000000000000000000000000000000000000".to_owned()
}

fn default_actor() -> String {
    "preloop-system".to_owned()
}

/// Result returned after accepting a workflow run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunAccepted {
    /// Run this status refers to.
    pub run_id: RunId,
    /// Monotonic run number for this workflow path.
    pub run_number: u64,
    /// Number of expanded jobs queued for runners.
    pub queued_jobs: usize,
}

/// Status of a workflow run or job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// Accepted and waiting for a runner.
    Queued,
    /// Object is waiting on a concurrency group (not runnable yet).
    Pending,
    /// A runner has picked the job up.
    InProgress,
    /// Finished successfully.
    Success,
    /// Finished with a failure.
    Failure,
    /// Object was skipped by condition or dependency.
    Skipped,
    /// Cancelled before completion.
    Cancelled,
}

impl ExecutionStatus {
    /// Whether the run or job has reached a final state and will not change.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Success | Self::Failure | Self::Skipped | Self::Cancelled
        )
    }
}

/// A parsed and expanded job ready to send to a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPlan {
    /// Stable job identifier within the run.
    pub id: JobId,
    /// Original workflow job id before matrix suffixing.
    pub base_id: String,
    /// Display name.
    pub name: String,
    /// Display name of the required runner group from object-valued `runs-on`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_group: Option<String>,
    /// Required runner labels.
    pub runs_on: Vec<String>,
    /// Dependency job ids.
    #[serde(default)]
    pub needs: Vec<JobId>,
    /// Matrix values for this expansion.
    #[serde(default)]
    pub matrix: IndexMap<String, serde_json::Value>,
    /// 1-based index of this matrix cell within its base job, when the job
    /// expanded from a matrix. GitHub encodes it in `system.orchestrationId`
    /// (`build._1`, `build._2`, …) which the official runner emits as a
    /// User-Agent product token; `__default` is used when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix_index: Option<usize>,
    /// Total number of matrix cells for the base job, when the job expanded
    /// from a matrix with more than one combination. Feeds the
    /// `strategy.job-total` context (1-based total, like GitHub).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix_total: Option<usize>,
    /// Deferred expression for runtime dynamic matrix expansion, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferred_matrix: Option<String>,
    /// Job-level environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Ordered step plan.
    #[serde(default)]
    pub steps: Vec<StepPlan>,
    /// Optional `if` expression.
    #[serde(default)]
    pub if_condition: Option<String>,
    /// Whether sibling matrix jobs should be cancelled after a failure.
    #[serde(default = "default_fail_fast")]
    pub fail_fast: bool,
    /// Whether a failed job is allowed to complete as success.
    #[serde(default)]
    pub continue_on_error: bool,
    /// Maximum concurrent matrix jobs for this base job.
    #[serde(default)]
    pub max_parallel: Option<u64>,
    /// Whether this job inherits all parent secrets (reusable workflow `secrets: inherit`).
    #[serde(default)]
    pub secrets_inherit: bool,
    /// Raw `container:` value, string or mapping, un-evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<serde_json::Value>,
    /// Raw `services:` mapping, un-evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub services: Option<serde_json::Value>,
    /// Inputs passed to a reusable workflow.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub inputs: BTreeMap<String, serde_json::Value>,
    /// Path of the called workflow file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_file: Option<String>,
    /// Resolved workflow ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Resolved workflow commit SHA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_sha: Option<String>,
    /// Resolved workflow repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_repository: Option<String>,
    /// Map of called secret name to the expression/value passed by the caller.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets_map: BTreeMap<String, String>,
    /// Job-level output declarations: output name → value expression.
    /// The runner evaluates these after step execution and includes results in completejob.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub job_outputs: BTreeMap<String, String>,
    /// Effective `id-token: write` permission after reusable-workflow reduction.
    #[serde(default)]
    pub oidc_id_token_granted: bool,
    /// Effective job permissions (job-level overrides workflow-level).
    /// Keys are GitHub permission names (`contents`, `issues`, `pull-requests`, …),
    /// values are `read`, `write`, or `none`. `None` means default permissions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<BTreeMap<String, String>>,
    /// Resolved deployment environment used for OIDC claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc_environment: Option<String>,
    /// Executing reusable workflow reference, when this job came from one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oidc_job_workflow_ref: Option<String>,
    /// Raw job-level concurrency group expression/string (server-evaluated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_group: Option<String>,
    /// Raw job-level `cancel-in-progress` value: "true"/"false"/expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_cancel_in_progress: Option<String>,
    /// Job-level concurrency queue mode: `"single"` or `"max"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_queue: Option<String>,
    /// Pending reusable-workflow invocation.
    ///
    /// Present on caller placeholder nodes: the callee subtree is not part of
    /// the plan. The server expands it at runtime only when the caller's
    /// `if:` gate passes, and otherwise records this single node as skipped —
    /// matching GitHub, which never materializes a false-gated caller's subtree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reusable_call: Option<ReusableCallPlan>,
}

/// Everything needed to expand a reusable-workflow caller node into its
/// callee job subtree once the caller's `if:` gate passes at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReusableCallPlan {
    /// The raw `uses:` reference (`owner/repo/path@ref` or `./path`).
    pub uses: String,
    /// Normalized path of the called workflow file.
    pub workflow_file: String,
    /// Resolved called-workflow commit SHA.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_sha: Option<String>,
    /// Resolved called-workflow repository.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_repository: Option<String>,
    /// Reusable-call nesting depth of this caller (1 = called from the root
    /// workflow). GitHub caps nesting at 4.
    #[serde(default = "default_reusable_depth")]
    pub depth: usize,
}

fn default_reusable_depth() -> usize {
    1
}

fn default_fail_fast() -> bool {
    true
}

/// Pull one OCI image reference out of a raw `container:`/service value.
///
/// Both accept either a bare string (`container: node:20`) or a mapping with an
/// `image:` key. Values are un-evaluated workflow source, so anything still
/// holding a `${{ }}` expression is skipped: its value is not known until the
/// job runs, and pulling a guess wastes the work it was meant to save.
pub fn oci_image_ref(value: &serde_json::Value) -> Option<String> {
    let raw = match value {
        serde_json::Value::String(image) => image.as_str(),
        serde_json::Value::Object(map) => map.get("image")?.as_str()?,
        _ => return None,
    }
    .trim();
    if raw.is_empty() || raw.contains("${{") {
        return None;
    }
    Some(raw.to_owned())
}

impl JobPlan {
    /// Every OCI image this job needs, from `container:` and `services:`.
    ///
    /// Sorted and deduplicated so the result is stable across declaration order.
    pub fn container_images(&self) -> Vec<String> {
        let mut images = BTreeSet::new();
        if let Some(image) = self.container.as_ref().and_then(oci_image_ref) {
            images.insert(image);
        }
        if let Some(serde_json::Value::Object(services)) = &self.services {
            images.extend(services.values().filter_map(oci_image_ref));
        }
        images.into_iter().collect()
    }
}

/// A workflow step after normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepPlan {
    /// Stable step id if provided.
    #[serde(default)]
    pub id: Option<String>,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Shell script body.
    #[serde(default)]
    pub run: Option<String>,
    /// Action reference from `uses`.
    #[serde(default)]
    pub uses: Option<String>,
    /// Step environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Step inputs.
    #[serde(default)]
    pub with: BTreeMap<String, serde_json::Value>,
    /// Optional `if` expression.
    pub if_condition: Option<String>,
    /// Working directory for `run` steps.
    #[serde(default)]
    pub working_directory: Option<String>,
    /// Shell override for `run` steps.
    #[serde(default)]
    pub shell: Option<String>,
    /// Whether to continue on error.
    #[serde(default)]
    pub continue_on_error: Option<bool>,
}

/// Context material sent to a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerJobMessage {
    /// Protocol version.
    pub protocol_version: String,
    /// Run id.
    pub run_id: RunId,
    /// Job plan.
    pub job: JobPlan,
    /// GitHub context.
    pub github: serde_json::Value,
    /// Variables context.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// Environment context.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl RunnerJobMessage {
    /// Build a message with the current protocol version.
    pub fn new(run_id: RunId, job: JobPlan, github: serde_json::Value) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_owned(),
            run_id,
            job,
            github,
            vars: BTreeMap::new(),
            env: BTreeMap::new(),
        }
    }
}

/// Runner registration request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerRegistrationRequest {
    /// Runner display name.
    pub name: String,
    #[serde(default)]
    /// Labels this runner advertises for `runs-on` matching.
    pub labels: Vec<String>,
    #[serde(default)]
    /// Whether the runner retires after a single job.
    pub ephemeral: bool,
    /// Runner RSA public key material (XML/JWK/PEM depending on client).
    #[serde(default)]
    pub public_key: Option<String>,
    /// Numeric runner group identifier. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupId",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_id: Option<i64>,
    /// Runner group display name. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupName",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A runner registered with the control plane.
pub struct RegisteredRunner {
    /// Numeric id used by GitHub runner APIs.
    pub id: i64,
    /// Runner name.
    pub name: String,
    /// Runner labels.
    pub labels: Vec<String>,
    /// Ephemeral runner flag.
    pub ephemeral: bool,
    /// Runner RSA public key material, if supplied at registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<String>,
    /// Numeric runner group identifier. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupId",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_id: Option<i64>,
    /// Runner group display name. Missing means the default group.
    #[serde(
        default,
        alias = "runnerGroupName",
        skip_serializing_if = "Option::is_none"
    )]
    pub runner_group_name: Option<String>,
}

/// Runner session creation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerSessionRequest {
    /// Runner id.
    pub runner_id: i64,
    /// Runner name.
    pub name: String,
}

/// Runner session response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerSession {
    /// Session id.
    pub session_id: SessionId,
    /// Runner id.
    pub runner_id: i64,
}

/// Step or job outputs.
pub type OutputMap = IndexMap<String, serde_json::Value>;

/// Job completion request from a runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobCompletion {
    /// Run id.
    pub run_id: RunId,
    /// Job id.
    pub job_id: JobId,
    /// Final status.
    pub status: ExecutionStatus,
    /// Outputs captured by the runner.
    #[serde(default)]
    pub outputs: OutputMap,
    /// Job-level annotations reported with the completion (e.g. the
    /// official runner's worker-crash detail from `ForceFailJob`).
    #[serde(default)]
    pub annotations: Vec<serde_json::Value>,
    /// Authoritative per-step conclusions carried by `completejob`.
    ///
    /// The official runner always sends these (`CompleteJobRequest.stepResults`):
    /// each entry's `status` is the TimelineRecordState (`completed`) and
    /// `conclusion` the TaskResult (`succeeded`/`failed`/`skipped`/…). The
    /// server applies them in preference to its own inference; a crashed
    /// worker (ForceFailJob) sends none, and the server reconciles instead.
    #[serde(default)]
    pub step_results: Vec<CompletionStepResult>,
}

/// One entry of the `completejob` `stepResults` array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionStepResult {
    /// Step timeline record id.
    #[serde(default)]
    pub external_id: Option<String>,
    /// 1-based step position within the job.
    #[serde(default)]
    pub number: Option<u64>,
    /// Step name as reported by the runner (matches the run record's step
    /// name for the same step).
    #[serde(default)]
    pub name: Option<String>,
    /// TimelineRecordState: `"completed"`/`"inprogress"`/`"pending"` or the
    /// numeric 0..3 form. Only a terminal status makes the conclusion
    /// authoritative.
    #[serde(default)]
    pub status: Option<serde_json::Value>,
    /// TaskResult: `"succeeded"`/`"failed"`/`"canceled"`/`"skipped"`/
    /// `"abandoned"` or the numeric 0..5 form.
    #[serde(default)]
    pub conclusion: Option<serde_json::Value>,
}

/// Mask every string value in job-completion annotations with the run's
/// canonical secret masker.
///
/// Crash annotations (e.g. the official runner's worker-crash detail from
/// `ForceFailJob`) embed worker stdout/stderr, which can contain secret
/// values. The server must run annotations through this before persisting or
/// returning them — the raw `JobCompletion` is the protocol boundary and is
/// not safe to store as-is. Object keys are preserved so the annotation
/// schema survives masking; only string values are rewritten.
///
/// `secrets` matches [`masking::mask_secrets`]'s iterator shape, so server
/// callers pass the same plaintext collection they use for log masking (e.g.
/// `masking::expose_values(run.submission.secrets.values()).iter().map(String::as_str)`).
pub fn mask_annotations<'a, I>(
    annotations: Vec<serde_json::Value>,
    secrets: I,
) -> Vec<serde_json::Value>
where
    I: IntoIterator<Item = &'a str>,
{
    let secrets: Vec<&'a str> = secrets.into_iter().collect();
    annotations
        .into_iter()
        .map(|value| mask_annotation_strings(value, &secrets))
        .collect()
}

fn mask_annotation_strings(value: serde_json::Value, secrets: &[&str]) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            serde_json::Value::String(masking::mask_secrets(&text, secrets.iter().copied(), &[]))
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(|item| mask_annotation_strings(item, secrets))
                .collect(),
        ),
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, mask_annotation_strings(value, secrets)))
                .collect(),
        ),
        other => other,
    }
}

/// Machine-readable event emitted as NDJSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NdjsonEvent {
    /// Run was accepted.
    RunAccepted {
        /// Run the event refers to.
        run_id: RunId,
        /// Jobs still queued for this run.
        queued_jobs: usize,
    },
    /// A job changed execution status.
    JobStatus {
        /// Run the event refers to.
        run_id: RunId,
        /// Job the event refers to.
        job_id: JobId,
        /// New status.
        status: ExecutionStatus,
        /// Optional status reason (`concurrency_pending`, `concurrency_cancelled`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Log line was appended.
    Log {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// Raw log line.
        line: String,
    },
    /// Annotation was emitted.
    Annotation {
        /// Run the annotation belongs to.
        run_id: RunId,
        /// Job the annotation belongs to.
        job_id: JobId,
        /// Annotation severity.
        level: AnnotationLevel,
        /// Human-readable annotation text.
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Source file the annotation points at, when known.
        file: Option<String>,
        /// Optional start line number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        line: Option<u64>,
        /// Optional end line number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_line: Option<u64>,
        /// Optional start column number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        col: Option<u64>,
        /// Optional end column number.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_column: Option<u64>,
        /// Optional annotation title.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// Optional step/record ID.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_id: Option<String>,
    },
    /// Run-level status changed.
    RunStatus {
        /// Run id.
        run_id: RunId,
        /// New status.
        status: ExecutionStatus,
        /// Optional status reason (`concurrency_pending`, `concurrency_cancelled`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// Job completed with result and outputs.
    JobCompleted {
        /// Run id.
        run_id: RunId,
        /// Job id.
        job_id: JobId,
        /// Final status.
        status: ExecutionStatus,
        /// Job outputs.
        #[serde(default)]
        outputs: BTreeMap<String, String>,
    },
    /// GitHub check runs were created for this run's jobs.
    ///
    /// Carries no job data: it exists so the run record — which holds the
    /// `job_check_run_ids` mapping — is persisted the moment the ids are
    /// known, not on the next job status event. A restart between check-run
    /// creation and the job's first status event used to lose the mapping,
    /// leaving GitHub checks permanently "queued" while the jobs ran.
    CheckRunCreated {
        /// Run the check runs belong to.
        run_id: RunId,
    },
}

impl NdjsonEvent {
    /// Run this event belongs to.
    pub fn run_id(&self) -> RunId {
        match self {
            Self::RunAccepted { run_id, .. }
            | Self::JobStatus { run_id, .. }
            | Self::Log { run_id, .. }
            | Self::Annotation { run_id, .. }
            | Self::RunStatus { run_id, .. }
            | Self::JobCompleted { run_id, .. }
            | Self::CheckRunCreated { run_id } => *run_id,
        }
    }

    /// Final run-level status carried by this event, if any.
    ///
    /// Only a terminal `RunStatus` closes an event stream: job-level terminals
    /// still leave sibling jobs running.
    pub fn terminal_run_status(&self) -> Option<ExecutionStatus> {
        match self {
            Self::RunStatus { status, .. } if status.is_terminal() => Some(*status),
            _ => None,
        }
    }
}

/// Annotation severity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnotationLevel {
    /// Notice.
    Notice,
    /// Warning.
    Warning,
    /// Error.
    Error,
}

/// Protocol crate error.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Serialize an event as a single NDJSON line.
pub fn event_to_ndjson(event: &NdjsonEvent) -> Result<String, ProtocolError> {
    let mut line = serde_json::to_string(event)?;
    line.push('\n');
    Ok(line)
}

/// JSON payload exchanged between the runner and server over the live console
/// feed WebSocket. Matches the official runner's `TimelineRecordFeedLinesWrapper`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveLogFeedLinesWrapper {
    /// Step/timeline record GUID.
    pub step_id: String,
    /// First line number in this batch, 1-indexed within the step.
    pub start_line: u64,
    /// Number of lines in `value`.
    pub count: usize,
    /// Console lines for this step.
    pub value: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_display_and_json_are_redacted() {
        let secret = SecretString::new("super-secret");

        assert_eq!(format!("{secret:?}"), "SecretString(<redacted>)");
        assert_eq!(secret.to_string(), "<redacted>");
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"<redacted>\"");
        assert_eq!(secret.expose(), "super-secret");
    }

    #[test]
    fn workflow_submission_redacts_secrets_on_plain_serialization() {
        let submission = submission_with_secrets();

        // Anything that embeds a submission in server state (RunRecord) and
        // serializes it must never emit the real values.
        let json = serde_json::to_string(&submission).unwrap();

        assert!(
            !json.contains("actual-value") && !json.contains("another-secret"),
            "plain serialization leaked a secret value: {json}"
        );
        assert!(json.contains("<redacted>"), "expected redaction marker");

        #[derive(Serialize)]
        struct Nested {
            submission: WorkflowSubmission,
        }
        let nested = serde_json::to_string(&Nested { submission }).unwrap();
        assert!(
            !nested.contains("actual-value") && !nested.contains("another-secret"),
            "nested serialization leaked a secret value: {nested}"
        );
    }

    #[test]
    fn workflow_submission_request_json_exposes_secrets_for_the_wire() {
        let submission = submission_with_secrets();

        let value = submission.to_request_json().unwrap();

        assert_eq!(value["secrets"]["TOKEN"], "actual-value");
        assert_eq!(value["secrets"]["KEY"], "another-secret");
        assert!(
            !value.to_string().contains("<redacted>"),
            "request JSON still contains a redaction marker"
        );

        // The server recovers the real values from the request body.
        let deserialized: WorkflowSubmission = serde_json::from_value(value).unwrap();
        assert_eq!(deserialized.secrets["TOKEN"].expose(), "actual-value");
        assert_eq!(deserialized.secrets["KEY"].expose(), "another-secret");
    }

    #[test]
    fn workflow_submission_request_json_matches_plain_shape_without_secrets() {
        let submission = WorkflowSubmission::default();

        assert_eq!(
            submission.to_request_json().unwrap(),
            serde_json::to_value(&submission).unwrap()
        );
    }

    #[test]
    fn mask_annotations_redacts_secrets_in_nested_strings() {
        let annotations = vec![
            serde_json::json!({
                "level": "failure",
                "message": "worker crashed: token ghp_secret123 in output",
                "stack": ["stdout ghp_secret123", "stderr ghp_secret123"],
            }),
            serde_json::json!({ "message": "clean annotation" }),
        ];

        let masked = mask_annotations(annotations, ["ghp_secret123"].iter().copied());

        let serialized = serde_json::to_string(&masked).unwrap();
        assert!(
            !serialized.contains("ghp_secret123"),
            "annotation leaked a secret: {serialized}"
        );
        assert_eq!(masked[0]["message"], "worker crashed: token *** in output");
        assert_eq!(masked[0]["stack"][0], "stdout ***");
        assert_eq!(masked[0]["stack"][1], "stderr ***");
        assert_eq!(
            masked[0]["level"], "failure",
            "annotation schema keys must survive masking"
        );
        assert_eq!(masked[1]["message"], "clean annotation");

        // Masking is idempotent — re-masking the already-masked output is a
        // no-op, so a secret can never surface on a second pass.
        let twice = mask_annotations(masked.clone(), ["ghp_secret123"].iter().copied());
        assert_eq!(twice, masked);
    }

    fn submission_with_secrets() -> WorkflowSubmission {
        let mut submission = WorkflowSubmission::default();
        submission
            .secrets
            .insert("TOKEN".to_owned(), SecretString::new("actual-value"));
        submission
            .secrets
            .insert("KEY".to_owned(), SecretString::new("another-secret"));
        submission
    }

    #[test]
    fn annotation_event_serializes_optional_source_fields() {
        let event = NdjsonEvent::Annotation {
            run_id: RunId(uuid::Uuid::nil()),
            job_id: JobId("job".into()),
            level: AnnotationLevel::Warning,
            message: "warning".into(),
            file: Some("src/lib.rs".into()),
            line: Some(7),
            end_line: Some(8),
            col: Some(3),
            end_column: Some(9),
            title: Some("Compiler".into()),
            step_id: None,
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "annotation");
        assert_eq!(value["file"], "src/lib.rs");
        assert_eq!(value["line"], 7);
        assert_eq!(value["end_line"], 8);
        assert_eq!(value["col"], 3);
        assert_eq!(value["end_column"], 9);
        assert_eq!(value["title"], "Compiler");
        assert!(value.get("step_id").is_none());
    }
}
