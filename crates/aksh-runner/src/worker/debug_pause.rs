//! Pause-on-failure: hold a failed step open and await a controller verdict.
//!
//! The worker does not exit when a step fails under debugging. It registers a
//! session with the control plane and long-polls for a decision. Because the
//! worker process stays alive, the microVM stays alive with it — the whole
//! environment (installed packages, running services, warm build caches) is
//! still there when a human or agent attaches.
//!
//! Two invariants this module exists to protect:
//!
//! 1. **A failed poll is never a decision.** Network trouble must not read as
//!    `Abort`. Only an explicit verdict resumes the worker; everything else
//!    re-polls.
//! 2. **A retry must not double-apply state.** [`StepStateSnapshot`] captures
//!    the runner-managed context a step attempt mutates so a retry starts from
//!    the same logical state, rather than appending to it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aksh_gha_protocol::debug_session::{
    AttemptRecord, Diagnostic, FailedStep, OpenSessionRequest, OpenSessionResponse, RevertPolicy,
    Verdict, VerdictResponse, WorkerTokenRequest, WorkerTokenResponse, WorkspaceChange,
};
use aksh_gha_protocol::{JobId, RunId};
use tracing::{info, warn};

use super::contexts::{JobContext, JobStatus, StepResult};
use crate::client::http::HttpClient;

/// How long to wait before re-polling after a transport error.
const POLL_BACKOFF: Duration = Duration::from_secs(2);

/// Reduce a service endpoint to its origin.
///
/// The runner protocol hands out endpoints with path prefixes; the native debug
/// surface is rooted at the origin. Keeping only scheme/host/port also keeps
/// the URL matching `PRELOOP_CONTROL_ORIGIN`, which is what routes the request
/// over the guest's mounted control socket.
fn origin_of(service_url: &str) -> anyhow::Result<String> {
    let parsed = reqwest::Url::parse(service_url)
        .map_err(|error| anyhow::anyhow!("invalid service endpoint `{service_url}`: {error}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("service endpoint `{service_url}` has no host"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

/// Trade the job runtime token for this job's debug-worker credential.
///
/// The credential is fetched rather than read out of the job message because
/// the job message is shared ground with the official runner, which copies
/// every secret variable into the `secrets` context — a workflow could then
/// read the debug credential straight out of its own YAML.
///
/// The server issues at most once per job request and only for a run that
/// enabled pause-on-failure, so this must be called during job setup, before
/// any step runs.
async fn exchange_worker_token(
    http: &HttpClient,
    base_url: &str,
    runtime_token: &str,
    agent_job_id: uuid::Uuid,
) -> anyhow::Result<String> {
    let url = format!("{base_url}/api/v1/debug/worker-token");
    let response = http
        .client_for(&url)
        .post(&url)
        .bearer_auth(runtime_token)
        .json(&WorkerTokenRequest { agent_job_id })
        .send()
        .await?
        .error_for_status()?
        .json::<WorkerTokenResponse>()
        .await?;
    Ok(response.token)
}

/// Server-side long-poll ceiling. Requests use this so the connection cycles
/// often enough for the server to observe worker liveness.
const POLL_WAIT_SECS: u64 = 25;

/// A controller's decision, plus what it asked to be undone first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    /// What to do with the failed step.
    pub verdict: Verdict,
    /// How much of the attempt's workspace debris to undo before retrying.
    pub revert: RevertPolicy,
    /// Source revision the next attempt runs against.
    pub source_revision: Option<String>,
    /// When set, re-execute from this step index instead of only the failed
    /// step. The worker must jump the outer step loop back to this index.
    pub retry_from_step: Option<usize>,
}

/// Everything needed to talk to the control plane about one job's sessions.
#[derive(Clone)]
pub struct DebugPauseClient {
    /// Routes control-plane requests over the mounted Unix socket when the
    /// runner is inside a microVM. A bare `reqwest::Client` would try plain TCP
    /// to an origin the guest cannot reach, and the pause would silently never
    /// register.
    http: Arc<HttpClient>,
    base_url: String,
    token: String,
    run_id: RunId,
    job_id: JobId,
    agent_job_id: uuid::Uuid,
    job_name: String,
    machine: Option<String>,
    workspace: Option<String>,
    snapshot_commit: Option<String>,
    /// Bumped each time a controller supplies a new source revision, so the
    /// attempt journal records what each attempt actually ran against.
    revision: Arc<AtomicU32>,
    /// True while a step is blocked awaiting a verdict.
    ///
    /// The runner's own job-timeout timer reads it. The server suspends its
    /// copy of the clock independently; without this the two disagree and the
    /// runner cancels a job the server is still holding open — the failure
    /// looks like a spontaneous timeout mid-debug-session.
    paused: Arc<AtomicBool>,
    /// True only after a session received a controller verdict.
    resolved: Arc<AtomicBool>,
}

impl DebugPauseClient {
    /// Build a client, acquiring the debug-worker credential from the server.
    ///
    /// `service_url` is the `SystemVssConnection` endpoint, which carries a
    /// path prefix (`http://host:9090/broker/4`). The native debug surface is
    /// origin-rooted, so only scheme/host/port are kept — appending to the full
    /// endpoint produces `/broker/4/api/v1/...` and a 404.
    ///
    /// `runtime_token` is that endpoint's `AccessToken`. It is spent solely to
    /// authenticate the exchange; the session routes themselves reject it.
    pub async fn acquire(
        service_url: &str,
        runtime_token: &str,
        run_id: RunId,
        job_id: JobId,
        agent_job_id: uuid::Uuid,
        job_name: String,
    ) -> anyhow::Result<Self> {
        Self::acquire_with_http(
            Arc::new(HttpClient::new(None)?),
            service_url,
            runtime_token,
            run_id,
            job_id,
            agent_job_id,
            job_name,
        )
        .await
    }

    /// [`Self::acquire`] with an explicit transport. Test seam.
    pub async fn acquire_with_http(
        http: Arc<HttpClient>,
        service_url: &str,
        runtime_token: &str,
        run_id: RunId,
        job_id: JobId,
        agent_job_id: uuid::Uuid,
        job_name: String,
    ) -> anyhow::Result<Self> {
        let base_url = origin_of(service_url)?;
        let token = exchange_worker_token(&http, &base_url, runtime_token, agent_job_id).await?;
        Ok(Self {
            http,
            base_url,
            token,
            run_id,
            job_id,
            agent_job_id,
            job_name,
            machine: std::env::var("PRELOOP_MACHINE_NAME").ok(),
            workspace: None,
            snapshot_commit: None,
            revision: Arc::new(AtomicU32::new(0)),
            paused: Arc::new(AtomicBool::new(false)),
            resolved: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Build a client around an already-acquired credential, with an explicit
    /// transport. Test seam for the session protocol itself.
    pub fn with_http(
        http: Arc<HttpClient>,
        service_url: &str,
        token: String,
        run_id: RunId,
        job_id: JobId,
        agent_job_id: uuid::Uuid,
        job_name: String,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http,
            base_url: origin_of(service_url)?,
            token,
            run_id,
            job_id,
            agent_job_id,
            job_name,
            machine: std::env::var("PRELOOP_MACHINE_NAME").ok(),
            workspace: None,
            snapshot_commit: None,
            revision: Arc::new(AtomicU32::new(0)),
            paused: Arc::new(AtomicBool::new(false)),
            resolved: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Share the flag the job-timeout timer watches.
    pub fn with_pause_flag(mut self, paused: Arc<AtomicBool>) -> Self {
        self.paused = paused;
        self
    }

    /// Whether a live session opened and received an explicit controller
    /// verdict. Failed opens and abandoned polls must still preserve the VM.
    pub fn resolved_session(&self) -> bool {
        self.resolved.load(Ordering::SeqCst)
    }

    /// Attach workspace provenance so a controller can diff against the
    /// pristine snapshot.
    pub fn with_workspace(
        mut self,
        workspace: Option<String>,
        snapshot_commit: Option<String>,
    ) -> Self {
        self.workspace = workspace;
        self.snapshot_commit = snapshot_commit;
        self
    }

    /// Label for the source revision the next attempt will run against.
    pub fn current_revision(&self) -> String {
        match self.revision.load(Ordering::SeqCst) {
            0 => "original".to_owned(),
            n => format!("repair-{n}"),
        }
    }

    /// Register a paused step and block until a controller decides.
    ///
    /// Returns `None` when the session cannot be opened at all, which the
    /// caller must treat as "debugging unavailable, fail normally" rather than
    /// as any particular verdict.
    pub async fn pause(
        &self,
        step: FailedStep,
        attempts: Vec<AttemptRecord>,
        attempt_changes: Vec<WorkspaceChange>,
        job_steps: Vec<aksh_gha_protocol::debug_session::StepSummary>,
    ) -> Option<Decision> {
        let session_id = match self.open(step, attempts, attempt_changes, job_steps).await {
            Ok(id) => id,
            Err(error) => {
                warn!(%error, "could not open debug session — failing the step normally");
                return None;
            }
        };

        info!(
            session = %session_id,
            "step failed — paused for debugging. Attach with: preloop debug {session_id}"
        );

        // Held across the whole wait, including reconnect backoff: every
        // second here is debugging, not execution.
        self.paused.store(true, Ordering::SeqCst);
        let decision = self.await_verdict(&session_id).await;
        self.paused.store(false, Ordering::SeqCst);
        if decision.is_some() {
            self.resolved.store(true, Ordering::SeqCst);
        }

        if decision.as_ref().map(|d| d.verdict) == Some(Verdict::Retry) {
            self.revision.fetch_add(1, Ordering::SeqCst);
        }

        let state = match decision.as_ref().map(|d| d.verdict) {
            Some(Verdict::Abort) => "aborted",
            _ => "resumed",
        };
        if let Err(error) = self.close(&session_id, state).await {
            warn!(%error, session = %session_id, "failed to close debug session");
        }
        decision
    }

    async fn open(
        &self,
        step: FailedStep,
        attempts: Vec<AttemptRecord>,
        attempt_changes: Vec<WorkspaceChange>,
        job_steps: Vec<aksh_gha_protocol::debug_session::StepSummary>,
    ) -> anyhow::Result<String> {
        let body = OpenSessionRequest {
            run_id: self.run_id,
            job_id: self.job_id.clone(),
            agent_job_id: self.agent_job_id,
            job_name: self.job_name.clone(),
            step,
            machine: self.machine.clone(),
            workspace: self.workspace.clone(),
            snapshot_commit: self.snapshot_commit.clone(),
            attempts,
            attempt_changes,
            job_steps,
        };
        let url = format!("{}/api/v1/debug/sessions", self.base_url);
        let response = self
            .http
            .client_for(&url)
            .post(&url)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<OpenSessionResponse>()
            .await?;
        Ok(response.session_id)
    }

    /// Long-poll until a verdict arrives.
    ///
    /// Loops indefinitely on purpose: while a session is open the server has
    /// suspended this job's timeout, and a human reading code is not idle. The
    /// exits are an explicit verdict, or the server dropping the session
    /// (worker declared abandoned), which surfaces as a 404.
    async fn await_verdict(&self, session_id: &str) -> Option<Decision> {
        loop {
            match self.poll_once(session_id).await {
                Ok(Some(decision)) => return Some(decision),
                Ok(None) => continue,
                Err(error) => {
                    if error
                        .downcast_ref::<reqwest::Error>()
                        .and_then(|e| e.status())
                        .is_some_and(|s| s == reqwest::StatusCode::NOT_FOUND)
                    {
                        warn!(session = %session_id, "debug session no longer exists — resuming");
                        return None;
                    }
                    warn!(%error, session = %session_id, "verdict poll failed — retrying");
                    tokio::time::sleep(POLL_BACKOFF).await;
                }
            }
        }
    }

    async fn poll_once(&self, session_id: &str) -> anyhow::Result<Option<Decision>> {
        let url = format!(
            "{}/api/v1/debug/sessions/{session_id}/verdict?wait={POLL_WAIT_SECS}",
            self.base_url
        );
        let response = self
            .http
            .client_for(&url)
            .get(&url)
            .bearer_auth(&self.token)
            .timeout(Duration::from_secs(POLL_WAIT_SECS + 15))
            .send()
            .await?
            .error_for_status()?
            .json::<VerdictResponse>()
            .await?;
        Ok(response.verdict.map(|verdict| Decision {
            verdict,
            revert: response.revert,
            source_revision: response.source_revision,
            retry_from_step: response.retry_from_step,
        }))
    }

    async fn close(&self, session_id: &str, state: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/v1/debug/sessions/{session_id}/close", self.base_url);
        self.http
            .client_for(&url)
            .post(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "state": state }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

/// Runner-managed state a single step attempt can mutate.
///
/// Restored before a retry so the second attempt starts from the same logical
/// position as the first. This does **not** rewind the guest filesystem or any
/// external side effect — see `docs/debug-sessions.md` §4. It exists to stop a
/// failed attempt's `$GITHUB_ENV` / `$GITHUB_PATH` / `saveState` writes from
/// being applied twice.
#[derive(Debug, Clone)]
pub struct StepStateSnapshot {
    env: HashMap<String, String>,
    extra_path: Vec<String>,
    step_result: Option<StepResult>,
    step_state: Option<HashMap<String, String>>,
    job_status: JobStatus,
    job_annotation_count: usize,
}

impl StepStateSnapshot {
    /// Capture the state a step attempt is allowed to mutate.
    ///
    /// Secret masks are deliberately excluded. A step that calls `::add-mask::`
    /// and then fails has already had that value pass through the log
    /// pipeline; dropping the mask on retry would unmask it in subsequent
    /// output. Masks only ever accumulate.
    pub fn capture(job: &JobContext, context_name: &str) -> Self {
        Self {
            env: job.env.clone(),
            extra_path: job.extra_path.clone(),
            step_result: job.steps.get(context_name).cloned(),
            step_state: job.state.get(context_name).cloned(),
            job_status: job.job_status,
            job_annotation_count: job.job_annotations.len(),
        }
    }

    /// Roll the captured state back onto the context.
    pub fn restore(&self, job: &mut JobContext, context_name: &str) {
        job.env = self.env.clone();
        job.extra_path = self.extra_path.clone();
        match &self.step_result {
            Some(result) => {
                job.steps.insert(context_name.to_owned(), result.clone());
            }
            None => {
                job.steps.shift_remove(context_name);
            }
        }
        match &self.step_state {
            Some(state) => {
                job.state.insert(context_name.to_owned(), state.clone());
            }
            None => {
                job.state.remove(context_name);
            }
        }
        job.step_annotations.remove(context_name);
        job.job_annotations.truncate(self.job_annotation_count);
        job.job_status = self.job_status;
    }
}

/// Convert the step's collected annotations into session diagnostics.
///
/// The runner's workflow-command processor and problem matchers have already
/// parsed `::error file=…,line=…::message` into structured annotations, so the
/// file and line are available without re-parsing the log — by the time text
/// reaches the log those properties have been stripped into
/// `##[error]message`, and any attempt to recover them from there loses them.
///
/// Errors only: warnings and notices are noise at a failure banner.
pub fn diagnostics_from_annotations(
    annotations: &[crate::worker::execution_types::Annotation],
    limit: usize,
) -> Vec<Diagnostic> {
    use crate::worker::execution_types::AnnotationLevel;
    annotations
        .iter()
        .filter(|annotation| annotation.level == AnnotationLevel::Error)
        .filter(|annotation| !annotation.message.trim().is_empty())
        .take(limit)
        .map(|annotation| Diagnostic {
            level: "error".to_owned(),
            file: annotation.file.clone(),
            line: annotation.line.map(u64::from),
            column: annotation.col.map(u64::from),
            message: annotation.message.trim().to_owned(),
        })
        .collect()
}

/// Trailing log excerpt, used only when no structured diagnostic was found.
pub fn log_excerpt(log: &str, lines: usize) -> Option<String> {
    let collected: Vec<&str> = log.lines().filter(|line| !line.trim().is_empty()).collect();
    if collected.is_empty() {
        return None;
    }
    let start = collected.len().saturating_sub(lines);
    Some(collected[start..].join("\n"))
}

/// Recover the process exit code from the runner's own failure line.
///
/// The script and container handlers emit `Process completed with exit code N.`
/// before returning their error, so the code is already in the log rather than
/// needing to be threaded back through the handler signatures.
pub fn exit_code_from_log(log: &str) -> Option<i32> {
    log.lines().rev().find_map(|line| {
        let (_, rest) = line.split_once("Process completed with exit code ")?;
        rest.trim().trim_end_matches('.').parse().ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_with(context_name: &str) -> JobContext {
        let mut job = JobContext::new(
            "job-1".to_owned(),
            "build".to_owned(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.env.insert("BASE".into(), "1".into());
        job.steps.insert(
            context_name.to_owned(),
            StepResult {
                outcome: "Success".into(),
                conclusion: "Success".into(),
                outputs: HashMap::new(),
            },
        );
        job
    }

    #[test]
    fn restore_undoes_file_command_effects() {
        let mut job = job_with("other");
        let snapshot = StepStateSnapshot::capture(&job, "__run");

        // Simulate a failed attempt that wrote $GITHUB_ENV and $GITHUB_PATH
        // before dying, and recorded its own failure.
        job.env.insert("FROM_STEP".into(), "leaked".into());
        job.extra_path.push("/opt/leaked/bin".into());
        job.steps.insert(
            "__run".to_owned(),
            StepResult {
                outcome: "Failure".into(),
                conclusion: "Failure".into(),
                outputs: HashMap::new(),
            },
        );
        job.job_status = JobStatus::Failure;

        snapshot.restore(&mut job, "__run");

        assert!(!job.env.contains_key("FROM_STEP"), "env must not leak");
        assert!(job.extra_path.is_empty(), "PATH must not leak");
        assert!(
            !job.steps.contains_key("__run"),
            "the failed attempt's result must be removed, not overwritten in place"
        );
        assert!(job.steps.contains_key("other"), "other steps untouched");
        assert_eq!(job.job_status, JobStatus::Success);
        assert_eq!(job.env.get("BASE").map(String::as_str), Some("1"));
    }

    #[test]
    fn restore_preserves_a_prior_result_for_the_same_step() {
        // A step retried twice: the snapshot was taken when attempt 1's result
        // already existed, so restore must put that back rather than delete.
        let mut job = job_with("__run");
        let snapshot = StepStateSnapshot::capture(&job, "__run");
        job.steps.insert(
            "__run".to_owned(),
            StepResult {
                outcome: "Failure".into(),
                conclusion: "Failure".into(),
                outputs: HashMap::new(),
            },
        );
        snapshot.restore(&mut job, "__run");
        assert_eq!(job.steps.get("__run").unwrap().outcome, "Success");
    }

    #[test]
    fn masks_are_never_rolled_back() {
        let mut job = job_with("__run");
        let snapshot = StepStateSnapshot::capture(&job, "__run");
        job.masks.insert("s3cret".to_owned());
        snapshot.restore(&mut job, "__run");
        assert!(
            job.masks.contains("s3cret"),
            "dropping a mask on retry would unmask a secret in later logs"
        );
    }

    #[test]
    fn diagnostics_come_from_structured_annotations() {
        use crate::worker::execution_types::{Annotation, AnnotationLevel};
        let annotations = vec![
            Annotation {
                level: AnnotationLevel::Warning,
                message: "deprecated".into(),
                title: None,
                file: None,
                line: None,
                end_line: None,
                col: None,
                end_column: None,
            },
            Annotation {
                level: AnnotationLevel::Error,
                message: "expected `Completed`, found `Pending`".into(),
                title: None,
                file: Some("src/lib.rs".into()),
                line: Some(42),
                end_line: None,
                col: Some(9),
                end_column: None,
            },
        ];
        let diagnostics = diagnostics_from_annotations(&annotations, 10);
        assert_eq!(
            diagnostics.len(),
            1,
            "warnings are noise at a failure banner"
        );
        assert_eq!(diagnostics[0].file.as_deref(), Some("src/lib.rs"));
        assert_eq!(diagnostics[0].line, Some(42));
        assert_eq!(diagnostics[0].column, Some(9));
        assert_eq!(
            diagnostics[0].message,
            "expected `Completed`, found `Pending`"
        );
    }

    #[test]
    fn excerpt_skips_blank_lines_and_keeps_the_tail() {
        let log = "one\n\ntwo\n\n\nthree\n";
        assert_eq!(log_excerpt(log, 2).unwrap(), "two\nthree");
        assert_eq!(log_excerpt("   \n\n", 5), None);
    }

    #[test]
    fn exit_code_comes_from_the_last_failure_line() {
        let log = "\
##[error]Process completed with exit code 1.
retrying
##[error]Process completed with exit code 101.";
        assert_eq!(exit_code_from_log(log), Some(101));
        assert_eq!(exit_code_from_log("nothing here"), None);
    }

    #[test]
    fn service_endpoints_reduce_to_their_origin() {
        // The regression that made end-to-end silently fall back to the old
        // post-mortem path: appending to the full endpoint yields
        // `/broker/4/api/v1/debug/sessions`, which 404s.
        assert_eq!(
            origin_of("http://127.0.0.1:9090/broker/4").unwrap(),
            "http://127.0.0.1:9090"
        );
        assert_eq!(
            origin_of("http://127.0.0.1:9090/").unwrap(),
            "http://127.0.0.1:9090"
        );
        assert_eq!(
            origin_of("https://example.com/a/b").unwrap(),
            "https://example.com"
        );
        assert!(origin_of("not a url").is_err());
    }

    #[test]
    fn revision_label_tracks_retries() {
        let client = DebugPauseClient::with_http(
            Arc::new(crate::client::http::HttpClient::with_control(None, None).unwrap()),
            "http://localhost:9090/broker/1",
            "token".to_owned(),
            RunId::new(),
            JobId("build".to_owned()),
            uuid::Uuid::new_v4(),
            "build".to_owned(),
        )
        .unwrap();
        assert_eq!(client.current_revision(), "original");
        client.revision.fetch_add(1, Ordering::SeqCst);
        assert_eq!(client.current_revision(), "repair-1");
    }
}
