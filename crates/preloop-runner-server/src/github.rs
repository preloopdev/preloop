//! GitHub App Webhook Integration.

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
use crate::{
    ControlBackend, ExecutionStatus, SharedState, changed_paths_from_payload,
    submit_run_inner_with_webhook_delivery,
};
use preloop_gha_protocol::{AnnotationLevel, JobId, NdjsonEvent, RunId, WorkflowSubmission};

/// Comma-separated workflow filenames or `.github/workflows/...` paths that
/// GitHub, rather than Preloop, owns. This keeps release and artifact-publish
/// workflows out of the local webhook dispatcher while leaving the default
/// generic forges-only behavior unchanged.
pub const GITHUB_OWNED_WORKFLOWS_ENV: &str = "PRELOOP_GITHUB_SKIP_WORKFLOWS";

pub fn configured_github_owned_workflows() -> BTreeSet<String> {
    std::env::var(GITHUB_OWNED_WORKFLOWS_ENV)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(|entry| entry.trim_start_matches("./").to_owned())
                .collect()
        })
        .unwrap_or_default()
}

pub fn is_github_owned_workflow(filename: &str, configured: &BTreeSet<String>) -> bool {
    let path = format!(".github/workflows/{filename}");
    configured
        .iter()
        .any(|entry| entry == filename || entry == &path)
}

/// Webhook push event payload.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PushEvent {
    /// Git reference for the push event.
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// Previous commit SHA.
    pub before: String,
    /// Current commit SHA.
    pub after: String,
    /// Repository info.
    pub repository: RepositoryInfo,
    /// Commits in this push.
    pub commits: Vec<CommitInfo>,
}

/// Repository info.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RepositoryInfo {
    /// Full repository name (e.g. owner/repo).
    pub full_name: String,
    /// Default branch (e.g. main).
    pub default_branch: Option<String>,
}

/// Commit info.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CommitInfo {
    /// Commit ID.
    pub id: String,
    /// Added files.
    pub added: Vec<String>,
    /// Modified files.
    pub modified: Vec<String>,
    /// Removed files.
    pub removed: Vec<String>,
}

/// Webhook pull request event payload.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PullRequestEvent {
    /// Webhook action type.
    pub action: String,
    /// PR number.
    pub number: u64,
    /// PR details.
    pub pull_request: PullRequestDetails,
    /// Repository info.
    pub repository: RepositoryInfo,
}

/// Pull request details.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PullRequestDetails {
    /// Head reference.
    pub head: GitReference,
    /// Base reference.
    pub base: GitReference,
}

/// Git reference.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GitReference {
    /// Git reference name.
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// Commit SHA.
    pub sha: String,
}

/// Verify X-Hub-Signature-256 webhook signature.
pub fn verify_signature(secret: &str, payload: &[u8], signature_header: &str) -> bool {
    let signature_hex = match signature_header.strip_prefix("sha256=") {
        Some(hex) => hex,
        None => return false,
    };
    let signature_bytes = match decode_hex(signature_hex) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(payload);
    mac.verify_slice(&signature_bytes).is_ok()
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, &'static str> {
    if !hex.len().is_multiple_of(2) {
        return Err("Odd length");
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| "Invalid hex character")?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// GitHub REST root, overridable for GHES and for tests that point the server
/// at a stub API.
pub fn github_api_base() -> String {
    std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .map(|base| base.trim_end_matches('/').to_owned())
        .filter(|base| !base.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

fn record_check_reporting(shared: &Arc<SharedState>, success: bool) {
    let mut snapshot = shared.state.status_snapshot.write();
    if success {
        snapshot.github.last_check_success_at = Some(chrono::Utc::now());
    } else {
        snapshot.github.last_check_failure_at = Some(chrono::Utc::now());
    }
}

async fn resolve_check_run_token(shared: &Arc<SharedState>, repo: &str) -> Option<String> {
    let app_creds = crate::github_app::select_app_for_repo(shared, repo).await;
    if let Some(app_creds) = app_creds.as_ref() {
        let mut permissions = std::collections::BTreeMap::new();
        permissions.insert("checks".to_owned(), "write".to_owned());
        // The App mint intermittently 422s while the installation grants are
        // being read; a single retry keeps a transient rejection from
        // stranding the check run in `queued`.
        for attempt in 0..2 {
            match crate::github_app::get_or_mint_token(app_creds, repo, &permissions).await {
                Ok(token) => return Some(token),
                Err(error) if attempt == 0 => {
                    tracing::warn!(
                        %repo,
                        %error,
                        "check run token mint failed; retrying once"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => {
                    tracing::warn!(%repo, %error, "check run token mint failed");
                    break;
                }
            }
        }
    }
    let fallback = std::env::var("PRELOOP_GITHUB_TOKEN").ok();
    if fallback.is_none() && app_creds.is_some() {
        record_check_reporting(shared, false);
    }
    fallback
}

async fn send_github_check_request(
    shared: &Arc<SharedState>,
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    method: reqwest::Method,
    path: &str,
    body: &Value,
) -> anyhow::Result<Value> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!("{}/repos/{}/{}", github_api_base(), repo, path);
    let res = crate::github_breaker::send_observed(
        breaker,
        client
            .request(method, &url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {}", token))
            .header("Accept", "application/vnd.github+json")
            .json(body),
    )
    .await?;

    // Honour the primary budget advertised on the response: once remaining
    // hits zero, wait for the reset instead of spending the next call on a
    // guaranteed 403.
    breaker.observe_rate_budget(res.headers());

    if !res.status().is_success() {
        let status = res.status();
        let retry_after = res
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let header = |name: &str| {
            res.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned()
        };
        // GitHub sends `x-ratelimit-reset` on every response, so the reset is
        // only meaningful together with `remaining`: `check_retry_delay`
        // waits for the reset only when the budget is actually spent.
        let reset = header("x-ratelimit-reset");
        let remaining = header("x-ratelimit-remaining");
        let err_text = res.text().await.unwrap_or_default();
        record_check_reporting(shared, false);
        return Err(anyhow::anyhow!(
            "GitHub Check API failed with status {}: {}; retry_after={retry_after}; rate_remaining={remaining}; rate_reset={reset}",
            status,
            err_text
        ));
    }

    record_check_reporting(shared, true);
    let val = res.json().await.unwrap_or(Value::Null);
    Ok(val)
}

pub fn run_details_url(run_id: RunId) -> Option<String> {
    std::env::var("PRELOOP_PUBLIC_URL")
        .ok()
        .map(|base| format!("{}/runs/{run_id}", base.trim_end_matches('/')))
}

/// Report a queued check run to GitHub or simulate it locally.
///
/// Also stamps `reports_check_runs` on the run: every intake path that
/// reports checks (webhook, dispatch, push, rerun) funnels through here, so
/// the flag is the persistent answer to "should jobs materialized later get
/// check runs too".
pub async fn report_check_run_queued(
    shared: &Arc<SharedState>,
    _repo: &str,
    _sha: &str,
    job_id: &JobId,
    run_id: RunId,
) -> anyhow::Result<Option<u64>> {
    shared
        .state
        .backend
        .set_reports_check_runs(run_id, true)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    // The state transition already normally emitted a durable event. Intake
    // paths such as rerun/push can report a queued check without changing the
    // scheduler row, and a job already terminal when the flag lands would
    // stamp a `JobStatus` event `Stale`; append an unconditional projection
    // wake instead.
    shared
        .state
        .backend
        .append_check_run_projection(run_id, Some(job_id))
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    shared.state.events_dirty.notify_one();
    shared
        .state
        .backend
        .job_check_run_id(run_id, job_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// The repository + SHA check runs for this run attach to, when the run
/// reports checks at all. `status_check_sha` wins so late-minted checks land
/// on the same commit as intake-time checks (PR head vs. base/merge); a
/// synced push run overrides with the published commit.
fn check_run_report_coords(run: &crate::models::RunRecord) -> Option<(String, String)> {
    if !run.reports_check_runs {
        return None;
    }
    let sha = run
        .push_state
        .as_ref()
        .and_then(|state| state.effective_sha.clone())
        .or_else(|| run.submission.status_check_sha.clone())
        .unwrap_or_else(|| run.submission.sha.clone());
    Some((run.submission.repository.clone(), sha))
}

/// Return an already persisted job→check-run mapping, if any.
///
/// The sender is solely responsible for creating a missing GitHub check run;
/// request handlers never mint one. Callers that need a check for a
/// late-materialized job call [`report_check_run_queued`], which stamps
/// `reports_check_runs` and appends a durable projection wake.
pub async fn ensure_check_run_mapped(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) -> Option<u64> {
    shared
        .state
        .backend
        .job_check_run_id(run_id, job_id)
        .await
        .ok()
        .flatten()
}

fn is_check_run_not_found(error: &anyhow::Error) -> bool {
    error.to_string().contains("status 404")
}

/// Record a rerequest's desired queued state; the sender performs the PATCH.
pub async fn report_existing_check_run_queued(
    shared: &Arc<SharedState>,
    _repo: &str,
    job_id: &JobId,
    run_id: RunId,
    check_run_id: u64,
) -> anyhow::Result<()> {
    shared
        .state
        .backend
        .set_job_check_run(run_id, job_id, check_run_id)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    shared
        .state
        .backend
        .append_event(&NdjsonEvent::JobStatus {
            run_id,
            job_id: job_id.clone(),
            status: ExecutionStatus::Queued,
            reason: None,
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    // The status event above is dropped when the job row settled differently;
    // the wake still delivers whatever state the row holds.
    shared
        .state
        .backend
        .append_check_run_projection(run_id, Some(job_id))
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    shared.state.events_dirty.notify_one();
    Ok(())
}
/// The `external_id` this engine stamps on every check run it creates. It
/// identifies the queue row's own check, so crash-after-POST reconciliation
/// never adopts another workflow's (or another app's) same-named check.
///
/// Job checks key on `{run_id}:{job_id}`. A workflow-evaluation failure gets a
/// fresh `RunId` on every redelivery but a stable
/// `__workflow_failure__:{name}` job id, so the job id alone is its key.
fn check_run_external_id(update: &crate::control::types::CheckRunUpdate) -> String {
    if update.payload.get("kind").and_then(Value::as_str) == Some("workflow_failure") {
        update.job_id.0.clone()
    } else {
        format!("{}:{}", update.run_id, update.job_id)
    }
}

/// Find the check run this queue row already created on `sha`, by its
/// `external_id`. Pages through every check run on the commit (100 per page,
/// bounded) rather than trusting GitHub's default first page of 30, and asks
/// for `filter=all` so a newer same-named check cannot hide ours.
async fn find_existing_check_run(
    shared: &Arc<SharedState>,
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    sha: &str,
    external_id: &str,
) -> anyhow::Result<Option<u64>> {
    const PER_PAGE: u64 = 100;
    const MAX_PAGES: u64 = 10;
    for page in 1..=MAX_PAGES {
        let payload = match send_github_check_request(
            shared,
            breaker,
            token,
            repo,
            reqwest::Method::GET,
            &format!("commits/{sha}/check-runs?filter=all&per_page={PER_PAGE}&page={page}"),
            &Value::Null,
        )
        .await
        {
            Ok(payload) => payload,
            Err(error) if is_check_run_not_found(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let check_runs = payload
            .get("check_runs")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let found = check_runs.iter().find_map(|check_run| {
            (check_run.get("external_id").and_then(Value::as_str) == Some(external_id)
                && check_run.get("head_sha").and_then(Value::as_str) == Some(sha))
            .then(|| check_run.get("id").and_then(Value::as_u64))
            .flatten()
        });
        if found.is_some() {
            return Ok(found);
        }
        let total = payload
            .get("total_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if (check_runs.len() as u64) < PER_PAGE || page * PER_PAGE >= total {
            return Ok(None);
        }
    }
    Ok(None)
}

/// Per-installation rate budget key.
///
/// Installation ids come from [`crate::github_app::installation_for_repo`];
/// the PAT fallback shares one bucket because it is one token. `0` is never a
/// valid GitHub installation id, so it is never used as a key.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum CheckRunRateKey {
    Installation(u64),
    Pat,
}

/// Ceiling on how long a rate limit may park a row; a bogus header must not
/// strand a check run for a day.
const MAX_DEFER: std::time::Duration = std::time::Duration::from_secs(3_600);

/// How far each renewal extends a row's lease. Every write to GitHub renews
/// first, and this covers one request at the shared client's 30 s timeout
/// with margin, so a row cannot expire under an in-flight POST and be
/// re-leased by another sender that would POST a second check run.
const CHECK_RUN_LEASE_RENEWAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Error marker for a row another sender re-leased mid-delivery.
const LOST_CHECK_RUN_LEASE: &str = "check-run lease lost to another sender";

/// Attempts during which a PATCH 404 keeps the stored check-run id. GitHub
/// can answer 404 while a just-created check run replicates; only a 404 that
/// outlives these backoff retries means the check is really gone, and only
/// then is the id dropped so the next attempt reconciles or re-creates it.
const CHECK_RUN_NOT_FOUND_RETRIES: i32 = 3;

/// The single background sender for the durable check-run queue.
///
/// The serve bootstrap spawns one per node; tests build their own and drive
/// [`CheckRunSender::drain_once`]. Rate budgets are per installation, so a
/// throttled installation parks only its own rows and the rest keep flowing.
pub(crate) struct CheckRunSender {
    owner: String,
    budget: parking_lot::Mutex<
        std::collections::HashMap<CheckRunRateKey, Arc<crate::github_breaker::GithubBreaker>>,
    >,
}

impl Default for CheckRunSender {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckRunSender {
    pub(crate) fn new() -> Self {
        Self {
            owner: uuid::Uuid::new_v4().to_string(),
            budget: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn breaker(&self, key: &CheckRunRateKey) -> Arc<crate::github_breaker::GithubBreaker> {
        let mut budget = self.budget.lock();
        budget
            .entry(key.clone())
            .or_insert_with(|| Arc::new(crate::github_breaker::GithubBreaker::default()))
            .clone()
    }

    /// Lease and process one batch. Returns how many rows were leased; the
    /// caller keeps looping while batches arrive and sleeps when none do.
    pub(crate) async fn drain_once(&self, shared: &Arc<SharedState>) -> usize {
        const BATCH: usize = 32;
        const LEASE: std::time::Duration = std::time::Duration::from_secs(30);
        let updates = match shared
            .state
            .backend
            .lease_check_run_updates(&self.owner, LEASE, BATCH)
            .await
        {
            Ok(updates) => updates,
            Err(error) => {
                warn!(?error, "check-run sender lease failed");
                return 0;
            }
        };
        let leased = updates.len();
        for update in updates {
            self.send_one(shared, &update).await;
        }
        leased
    }

    async fn send_one(
        &self,
        shared: &Arc<SharedState>,
        update: &crate::control::types::CheckRunUpdate,
    ) {
        let repo = update
            .payload
            .get("repository")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // Resolve the real installation before budgeting or sending: the
        // queue row's `installation_id` is projection metadata (0 = unknown),
        // never an authority.
        let key = resolve_check_run_rate_key(shared, &repo).await;
        let breaker = self.breaker(&key);
        if let Some(remaining) = breaker.retry_after() {
            // Waiting out a rate limit is not a delivery failure: release the
            // row due at GitHub's advertised resume time without burning an
            // attempt, so a long throttle cannot exhaust the permanent-drop
            // budget.
            if let Err(error) = shared
                .state
                .backend
                .defer_check_run_update(
                    &self.owner,
                    update.run_id,
                    &update.job_id,
                    remaining.min(MAX_DEFER),
                )
                .await
            {
                warn!(run_id=%update.run_id, job_id=%update.job_id, ?error, "failed to defer rate-limited check-run row");
            }
            return;
        }
        let token = resolve_check_run_token(shared, &repo).await;
        match send_queued_check_run(shared, &self.owner, &breaker, token.as_deref(), update).await {
            Ok(()) => {}
            Err(error) if error.to_string().contains(LOST_CHECK_RUN_LEASE) => {
                // Another sender owns the row now; it delivers the update.
                // Touching the row here would overwrite its lease state.
                debug!(run_id=%update.run_id, job_id=%update.job_id, "check-run lease taken over by another sender; skipping");
            }
            Err(error) if error.to_string().contains("circuit breaker is open") => {
                // A breaker that opened between the budget check and the call
                // is still a deferral, not a failure.
                let remaining = breaker
                    .retry_after()
                    .unwrap_or(std::time::Duration::from_secs(30));
                let _ = shared
                    .state
                    .backend
                    .defer_check_run_update(
                        &self.owner,
                        update.run_id,
                        &update.job_id,
                        remaining.min(MAX_DEFER),
                    )
                    .await;
            }
            Err(error) => {
                let permanent = update.attempts >= 7 || error.to_string().contains("status 422");
                let delay = check_retry_delay(&error, update.attempts);
                warn!(run_id=%update.run_id, job_id=%update.job_id, attempts=update.attempts, permanent, ?error, "check-run sender attempt failed");
                if let Err(store_error) = shared
                    .state
                    .backend
                    .retry_check_run_update(
                        &self.owner,
                        update.run_id,
                        &update.job_id,
                        delay,
                        permanent,
                    )
                    .await
                {
                    warn!(run_id=%update.run_id, job_id=%update.job_id, ?store_error, "failed to persist check-run retry state");
                }
            }
        }
    }
}

/// The installation (or PAT) whose budget governs this repository's check
/// runs. Resolution failure falls back to the PAT bucket; it never invents an
/// installation id.
async fn resolve_check_run_rate_key(shared: &Arc<SharedState>, repo: &str) -> CheckRunRateKey {
    match crate::github_app::select_app_for_repo(shared, repo).await {
        Some(app) => match crate::github_app::installation_for_repo(&app, repo).await {
            Ok(installation) if installation != 0 => CheckRunRateKey::Installation(installation),
            _ => CheckRunRateKey::Pat,
        },
        None => CheckRunRateKey::Pat,
    }
}

/// Drain the durable check-run queue. This is the only path that invokes the
/// GitHub Checks API; request handlers only append desired-state events.
pub(crate) async fn run_check_run_sender(shared: Arc<SharedState>) {
    let sender = CheckRunSender::new();
    while !shared.shutdown.is_cancelled() {
        let leased = sender.drain_once(&shared).await;
        if leased == 0 {
            tokio::select! {
                _ = shared.shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {},
            }
        }
    }
}

/// Test-only single pass of the check-run sender, mirroring the bootstrap
/// loop. Tests that assert a check-run mapping was recorded must drain the
/// sender: the sender is the only path that writes one, so a webhook alone
/// leaves `job_check_run_ids` empty.
#[cfg(any(test, feature = "test-support"))]
pub async fn drain_check_run_sender(shared: &Arc<SharedState>) -> usize {
    CheckRunSender::new().drain_once(shared).await
}

fn check_retry_delay(error: &anyhow::Error, attempts: i32) -> std::time::Duration {
    let text = error.to_string();
    if let Some(value) = text
        .split("retry_after=")
        .nth(1)
        .and_then(|v| v.split(';').next())
        && let Ok(seconds) = value.trim().parse::<u64>()
        && seconds > 0
    {
        return std::time::Duration::from_secs(seconds.min(86_400));
    }
    // `rate_reset` is `x-ratelimit-reset`: an absolute unix timestamp. GitHub
    // sends it on every response (404, 422, 5xx included), so it only governs
    // the retry when the primary budget is actually spent; otherwise a
    // transient failure would wait out the rest of the rate window.
    let budget_spent = text
        .split("rate_remaining=")
        .nth(1)
        .and_then(|v| v.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|v| v.parse::<u64>().ok())
        == Some(0);
    if budget_spent
        && let Some(value) = text
            .split("rate_reset=")
            .nth(1)
            .and_then(|v| v.split(|c: char| !c.is_ascii_digit()).next())
        && let Ok(reset) = value.parse::<u64>()
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_secs())
            .unwrap_or(0);
        if reset > now {
            return std::time::Duration::from_secs((reset - now).min(86_400));
        }
    }
    if text.contains("secondary rate limit") || text.contains("abuse detection") {
        return std::time::Duration::from_secs(60);
    }
    let base = 250_u64.saturating_mul(1_u64 << (attempts.clamp(0, 8) as u32));
    std::time::Duration::from_millis(base + rand::random::<u64>() % 250)
}

/// Render and deliver one queued update. `breaker` is the resolved
/// installation's rate budget; every request this function makes goes
/// through it.
async fn send_queued_check_run(
    shared: &Arc<SharedState>,
    owner: &str,
    breaker: &crate::github_breaker::GithubBreaker,
    token: Option<&str>,
    update: &crate::control::types::CheckRunUpdate,
) -> anyhow::Result<()> {
    let repo = update
        .payload
        .get("repository")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("check-run queue row has no repository"))?;
    let sha = update
        .payload
        .get("sha")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("check-run queue row has no head sha"))?;
    let name = update
        .payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(update.job_id.0.as_str());
    let status = update
        .payload
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("queued");
    let Some(token) = token else {
        info!(run_id=%update.run_id, job_id=%update.job_id, status, "GitHub token not configured; check-run update simulated locally");
        // A token-less engine still records a local check-run id. The base
        // minted exactly this mock at submit, before the sender took over
        // minting; the mapping is what `job_check_run_ids` serves and what
        // must survive a restart, so dropping it would silently erase the
        // mock path (`check_run_ids_survive_a_restart_before_any_job_event`).
        let existing = shared
            .state
            .backend
            .job_check_run_id(update.run_id, &update.job_id)
            .await
            .ok()
            .flatten();
        if existing.is_none() {
            let mock = rand::random::<u32>() as u64;
            let mapping_changed = shared
                .state
                .backend
                .set_job_check_run(update.run_id, &update.job_id, mock)
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            if mapping_changed {
                shared
                    .state
                    .emit_persisted(preloop_gha_protocol::NdjsonEvent::CheckRunCreated {
                        run_id: update.run_id,
                    })
                    .await;
            }
        }
        shared
            .state
            .backend
            .finish_check_run_update(owner, update.run_id, &update.job_id, update.version)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        return Ok(());
    };
    let mut check_run_id = update.check_run_id;
    if check_run_id.is_none() {
        // The persisted job mapping is the first dedup source: a previous
        // attempt may have POSTed and saved the mapping while failing to save
        // the queue row.
        check_run_id = shared
            .state
            .backend
            .job_check_run_id(update.run_id, &update.job_id)
            .await
            .ok()
            .flatten();
        if let Some(id) = check_run_id {
            persist_check_run_id(shared, owner, update, id).await?;
        }
    }
    if check_run_id.is_none() {
        // Crash-after-POST reconciliation: adopt the check run this row
        // already created (matched by its `external_id`), never a same-named
        // check from another workflow or app.
        let external_id = check_run_external_id(update);
        check_run_id =
            find_existing_check_run(shared, breaker, token, repo, sha, &external_id).await?;
        if let Some(id) = check_run_id {
            persist_check_run_id(shared, owner, update, id).await?;
        }
    }

    if update.payload.get("kind").and_then(Value::as_str) == Some("workflow_failure") {
        // Synthetic workflow-evaluation failures have no run row; keep the
        // body byte-identical to the pre-queue reporter.
        let body = serde_json::json!({
            "name": name,
            "head_sha": sha,
            "status": "completed",
            "conclusion": "failure",
            "completed_at": chrono::Utc::now().to_rfc3339(),
            "output": {
                "title": "Workflow evaluation failed",
                "summary": update.payload.get("summary").and_then(Value::as_str).unwrap_or(""),
            }
        });
        send_check_run_body(
            shared,
            owner,
            breaker,
            token,
            repo,
            sha,
            name,
            update,
            check_run_id,
            body,
        )
        .await?;
        finish_check_run_update(shared, owner, update).await?;
        return Ok(());
    }

    match status {
        // `pending` is a held job (needs/approval); it reports as a queued
        // check exactly as intake did. Only terminal statuses conclude one.
        "queued" | "pending" | "in_progress" => {
            let mut body = if status == "in_progress" {
                serde_json::json!({"status":"in_progress","started_at":chrono::Utc::now().to_rfc3339(),"output":{"title":name,"summary":"Running in preloop."}})
            } else {
                serde_json::json!({"name":name,"head_sha":sha,"status":"queued","output":{"title":name,"summary":format!("Waiting for a preloop runner.\n\njob_id: `{}`",update.job_id.0)}})
            };
            if let Some(url) = run_details_url(update.run_id) {
                body["details_url"] = Value::String(url);
            }
            send_check_run_body(
                shared,
                owner,
                breaker,
                token,
                repo,
                sha,
                name,
                update,
                check_run_id,
                body,
            )
            .await?;
        }
        _ => {
            let info = shared
                .state
                .backend
                .run_dispatch_info(update.run_id)
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            let conclusion = match status {
                "success" => "success",
                "failure" => "failure",
                "cancelled" => "cancelled",
                "skipped" => "skipped",
                _ => "failure",
            };
            let (title, summary, started_at, completed_at, annotations) = if let Some(info) = info {
                let job = info.jobs.iter().find(|job| job.job_id == update.job_id);
                let steps = job.map(|j| j.steps.clone()).unwrap_or_default();
                let (annotations, mut global_issues) =
                    timeline_annotations(shared, update.run_id, &update.job_id).await;
                if let Some(detail) = job.and_then(|j| j.detail.as_ref()) {
                    for annotation in &detail.annotations {
                        let message = annotation
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| annotation.to_string());
                        global_issues.push(format!("- {}", markdown_cell(&message)));
                    }
                }
                let summary = check_summary(conclusion, &steps, &global_issues, &update.job_id);
                let title = job
                    .and_then(|j| j.display_name.as_deref())
                    .unwrap_or(name)
                    .to_owned();
                let started_at = steps
                    .iter()
                    .filter_map(|step| step.started_at)
                    .min()
                    .or_else(|| info.started_at.map(chrono::DateTime::<chrono::Utc>::from));
                let completed_at = steps
                    .iter()
                    .filter_map(|step| step.finished_at)
                    .max()
                    .or_else(|| info.completed_at.map(chrono::DateTime::<chrono::Utc>::from))
                    .unwrap_or_else(chrono::Utc::now);
                (title, summary, started_at, completed_at, annotations)
            } else {
                (
                    name.to_owned(),
                    update
                        .payload
                        .get("summary")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    None,
                    chrono::Utc::now(),
                    Vec::new(),
                )
            };
            // Annotations travel 50 per request, exactly as before; only the
            // final request concludes the check run.
            let chunks: Vec<&[Value]> = if annotations.is_empty() {
                vec![&[]]
            } else {
                annotations.chunks(50).collect()
            };
            let mut id = check_run_id;
            for (index, chunk) in chunks.iter().enumerate() {
                let last = index + 1 == chunks.len();
                let mut body = serde_json::json!({
                    "output": {"title": title, "summary": summary, "annotations": chunk}
                });
                if last {
                    body["status"] = Value::String("completed".to_owned());
                    body["conclusion"] = Value::String(conclusion.to_owned());
                    body["completed_at"] = Value::String(completed_at.to_rfc3339());
                    if let Some(started_at) = started_at {
                        body["started_at"] = Value::String(started_at.to_rfc3339());
                    }
                    if let Some(url) = run_details_url(update.run_id) {
                        body["details_url"] = Value::String(url);
                    }
                } else if id.is_none() {
                    // Creating a check run requires a status; annotations
                    // arrive over the follow-up PATCHes.
                    body["status"] = Value::String("queued".to_owned());
                }
                id = send_check_run_body(
                    shared, owner, breaker, token, repo, sha, &title, update, id, body,
                )
                .await?;
            }
        }
    }
    finish_check_run_update(shared, owner, update).await?;
    Ok(())
}

/// The in-memory annotation stream for one job, split into GitHub annotation
/// payloads and free-text failure details (annotations without a file).
async fn timeline_annotations(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) -> (Vec<Value>, Vec<String>) {
    let inner = shared.state.inner.lock().await;
    let mut annotations = Vec::new();
    let mut global_issues = Vec::new();
    if let Some(events) = inner.timeline_events.get(&run_id) {
        for event in events {
            if let NdjsonEvent::Annotation {
                job_id: event_job_id,
                level,
                message,
                file,
                line,
                ..
            } = event
                && event_job_id == job_id
            {
                let level_str = match level {
                    AnnotationLevel::Notice => "notice",
                    AnnotationLevel::Warning => "warning",
                    AnnotationLevel::Error => "failure",
                };
                if let Some(file_path) = file {
                    let line_num = line.unwrap_or(1);
                    annotations.push(serde_json::json!({
                        "path": file_path,
                        "start_line": line_num,
                        "end_line": line_num,
                        "annotation_level": level_str,
                        "message": message,
                    }));
                } else {
                    global_issues.push(format!("**{}**: {}", level_str.to_uppercase(), message));
                }
            }
        }
    }
    (annotations, global_issues)
}

/// Send one Checks API request for `update`, creating the check run on POST.
/// Returns the check-run id (persisted alongside the queue row).
#[allow(clippy::too_many_arguments)]
async fn send_check_run_body(
    shared: &Arc<SharedState>,
    owner: &str,
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    sha: &str,
    name: &str,
    update: &crate::control::types::CheckRunUpdate,
    check_run_id: Option<u64>,
    mut body: Value,
) -> anyhow::Result<Option<u64>> {
    let (method, path) = match check_run_id {
        Some(id) => (reqwest::Method::PATCH, format!("check-runs/{id}")),
        None => {
            // Creation requires the check name and commit; `external_id` is
            // what reconciliation matches on after a crash.
            body["name"] = Value::String(name.to_owned());
            body["head_sha"] = Value::String(sha.to_owned());
            body["external_id"] = Value::String(check_run_external_id(update));
            (reqwest::Method::POST, "check-runs".to_owned())
        }
    };
    // Hold the row through the request: if it expired and another sender
    // re-leased it, that sender owns delivery, and a POST from here would
    // create a second check run.
    let renewed = shared
        .state
        .backend
        .renew_check_run_update(
            owner,
            update.run_id,
            &update.job_id,
            CHECK_RUN_LEASE_RENEWAL,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    if !renewed {
        anyhow::bail!(LOST_CHECK_RUN_LEASE);
    }
    let response =
        match send_github_check_request(shared, breaker, token, repo, method, &path, &body).await {
            Ok(response) => response,
            Err(error) => {
                // An early 404 is usually a just-created check still
                // replicating: keep the id and let the backoff retry the
                // PATCH. Only a persistent 404 drops it.
                if let Some(stale) = check_run_id.filter(|_| {
                    is_check_run_not_found(&error) && update.attempts >= CHECK_RUN_NOT_FOUND_RETRIES
                }) {
                    let _ = shared
                        .state
                        .backend
                        .clear_check_run_update_id(owner, update.run_id, &update.job_id, stale)
                        .await;
                    let _ = shared
                        .state
                        .backend
                        .clear_job_check_run(update.run_id, &update.job_id, stale)
                        .await;
                }
                return Err(error);
            }
        };
    let Some(id) = check_run_id else {
        let id = match response.get("id").and_then(Value::as_u64) {
            Some(id) => id,
            None => {
                let external_id = check_run_external_id(update);
                find_existing_check_run(shared, breaker, token, repo, sha, &external_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("GitHub check-run POST returned no id"))?
            }
        };
        persist_check_run_id(shared, owner, update, id).await?;
        return Ok(Some(id));
    };
    Ok(Some(id))
}

/// Record a GitHub check-run id on both the queue row (version-guarded) and
/// the job mapping.
async fn persist_check_run_id(
    shared: &Arc<SharedState>,
    owner: &str,
    update: &crate::control::types::CheckRunUpdate,
    id: u64,
) -> anyhow::Result<()> {
    shared
        .state
        .backend
        .set_check_run_update_id(owner, update.run_id, &update.job_id, update.version, id)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    shared
        .state
        .backend
        .set_job_check_run(update.run_id, &update.job_id, id)
        .await
        .map_err(|e| anyhow::anyhow!("saved check-run id {id} but job mapping failed: {e:?}"))?;
    Ok(())
}

async fn finish_check_run_update(
    shared: &Arc<SharedState>,
    owner: &str,
    update: &crate::control::types::CheckRunUpdate,
) -> anyhow::Result<()> {
    shared
        .state
        .backend
        .finish_check_run_update(owner, update.run_id, &update.job_id, update.version)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// Report a permanent failure check run to GitHub (e.g. invalid workflow YAML, expression failure).
///
/// The check name and head SHA are the idempotency key. A retry after a crash
/// first finds the existing check and patches it, avoiding duplicate failed
/// checks in GitHub's UI.
pub async fn report_check_run_permanent_failure(
    shared: &Arc<SharedState>,
    repo: &str,
    sha: &str,
    name: &str,
    summary: &str,
) -> anyhow::Result<()> {
    let run_id = RunId::new();
    let update = crate::control::types::CheckRunUpdateInput {
        run_id,
        job_id: JobId(format!("__workflow_failure__:{name}")),
        installation_id: 0,
        check_run_id: None,
        version: 0,
        payload: serde_json::json!({
            "kind": "workflow_failure",
            "repository": repo,
            "sha": sha,
            "name": name,
            "status": "completed",
            "summary": summary,
        }),
    };
    shared
        .state
        .backend
        .enqueue_check_run_update(update)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    shared.state.events_dirty.notify_one();
    Ok(())
}

/// Publish queued/completed checks for a native rerun.
pub async fn report_check_runs_for_run(
    shared: &Arc<SharedState>,
    run_id: RunId,
    reused_check_run: Option<(JobId, u64)>,
) {
    if let Err(error) = shared
        .state
        .backend
        .set_reports_check_runs(run_id, true)
        .await
    {
        warn!(%run_id, ?error, "failed to stamp reports_check_runs for rerun");
        return;
    }
    if let Some((job_id, check_run_id)) = reused_check_run
        && let Err(error) = shared
            .state
            .backend
            .set_job_check_run(run_id, &job_id, check_run_id)
            .await
    {
        warn!(%run_id, %job_id, ?error, "failed to persist reused check-run id");
    }
    if let Ok(Some(info)) = shared.state.backend.run_dispatch_info(run_id).await {
        for job in info.jobs.into_iter().filter(|job| !job.placeholder) {
            if let Err(error) = shared
                .state
                .backend
                .append_check_run_projection(run_id, Some(&job.job_id))
                .await
            {
                warn!(%run_id, job_id=%job.job_id, ?error, "failed to append rerun check projection wake");
            }
        }
        shared.state.events_dirty.notify_one();
    }
}

/// Report check run status to in_progress on GitHub or simulate it locally.
/// The acquire transition writes `job.started.v1` in its own transaction; the
/// durable projector observes it and the sender performs the PATCH
/// asynchronously. This wake covers a projector that consumed that event
/// before the run's `reports_check_runs` flag landed.
pub async fn report_check_run_in_progress(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) {
    if let Err(error) = shared
        .state
        .backend
        .append_check_run_projection(run_id, Some(job_id))
        .await
    {
        warn!(%run_id, %job_id, ?error, "failed to append in-progress check projection wake");
    } else {
        shared.state.events_dirty.notify_one();
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], " ")
}

fn duration_text(
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
) -> String {
    match (started_at, finished_at) {
        (Some(start), Some(finish)) => {
            let duration = finish
                .signed_duration_since(start)
                .num_milliseconds()
                .max(0);
            format!("{:.1}s", duration as f64 / 1000.0)
        }
        (Some(_), None) => "running".to_owned(),
        _ => "-".to_owned(),
    }
}

fn check_summary(
    conclusion: &str,
    steps: &[crate::models::StepRecord],
    global_issues: &[String],
    job_id: &JobId,
) -> String {
    let mut summary = format!("Job completed with status: **{conclusion}**");
    if !steps.is_empty() {
        summary.push_str("\n\n| Step | Conclusion | Duration |\n|---|---:|---:|");
        for step in steps {
            summary.push_str(&format!(
                "\n| {} | {} | {} |",
                markdown_cell(&step.name),
                markdown_cell(&step.conclusion),
                duration_text(step.started_at, step.finished_at)
            ));
        }
        if let Some(failed) = steps.iter().find(|step| step.conclusion == "failure") {
            summary.push_str(&format!(
                "\n\n**Failed step:** `{}`",
                markdown_cell(&failed.name)
            ));
        }
    }
    if !global_issues.is_empty() {
        summary.push_str("\n\n### Failure details\n");
        summary.push_str(&global_issues.join("\n"));
    }
    // Map the display name back to the workflow job id: required status
    // checks match on this exact display string, so a rename that breaks
    // the ruleset is diagnosable from the check page itself instead of
    // presenting as "Expected" forever with no explanation.
    summary.push_str(&format!("\n\njob_id: `{}`", job_id.0));
    summary
}
/// Report check run status to completed on GitHub or simulate it locally.
///
/// The terminal transition already wrote a durable event; this wake covers a
/// projector that consumed it before the run's `reports_check_runs` flag
/// landed (a job concluded during submit) — the sender renders the row's
/// final state.
pub async fn report_check_run_completed(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    _status: ExecutionStatus,
) {
    if let Err(error) = shared
        .state
        .backend
        .append_check_run_projection(run_id, Some(job_id))
        .await
    {
        warn!(%run_id, %job_id, ?error, "failed to append completed check projection wake");
    } else {
        shared.state.events_dirty.notify_one();
    }
}

/// Fetch workflows helper.
pub async fn fetch_workflows(
    shared: &Arc<SharedState>,
    repo: &str,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let api_base = github_api_base();
    fetch_workflows_at(shared, repo, git_ref, &api_base).await
}

pub async fn fetch_workflows_at(
    shared: &Arc<SharedState>,
    repo: &str,
    git_ref: &str,
    api_base: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    if let Some(base_path) = &shared.state.local_workspace {
        let mut workflows = BTreeMap::new();
        // A dispatch for a branch other than the checked-out tree must read
        // the workflow definitions from that ref, not from the working tree.
        if !git_ref.is_empty() {
            if git_ref.starts_with('-') {
                anyhow::bail!("workflow revision {git_ref:?} is not a valid Git ref");
            }
            let commitish = format!("{git_ref}^{{commit}}");
            let resolved = tokio::process::Command::new("git")
                .arg("-C")
                .arg(base_path)
                .args(["rev-parse", "--verify", &commitish])
                .output()
                .await?;
            if !resolved.status.success() {
                anyhow::bail!(
                    "workflow revision {git_ref:?} is unavailable in local workspace {}: {}",
                    base_path.display(),
                    String::from_utf8_lossy(&resolved.stderr),
                );
            }
            let listing = tokio::process::Command::new("git")
                .arg("-C")
                .arg(base_path)
                .args([
                    "ls-tree",
                    "-r",
                    "--name-only",
                    git_ref,
                    "--",
                    ".github/workflows",
                ])
                .output()
                .await?;
            if !listing.status.success() {
                anyhow::bail!(
                    "git ls-tree for ref {git_ref:?} in {} failed: {}",
                    base_path.display(),
                    String::from_utf8_lossy(&listing.stderr),
                );
            }
            for line in String::from_utf8_lossy(&listing.stdout).lines() {
                let path = line.trim();
                if path.is_empty() {
                    continue;
                }
                let name = path.rsplit('/').next().unwrap_or(path);
                if name.ends_with(".yml") || name.ends_with(".yaml") {
                    let path_ref = format!("{git_ref}:{path}");
                    let content = tokio::process::Command::new("git")
                        .arg("-C")
                        .arg(base_path)
                        .args(["show", &path_ref])
                        .output()
                        .await?;
                    if !content.status.success() {
                        anyhow::bail!(
                            "git show {path_ref:?} in {} failed: {}",
                            base_path.display(),
                            String::from_utf8_lossy(&content.stderr),
                        );
                    }
                    workflows.insert(
                        name.to_owned(),
                        String::from_utf8_lossy(&content.stdout).into_owned(),
                    );
                }
            }
            return Ok(workflows);
        }
        let workflows_dir = base_path.join(".github/workflows");
        if workflows_dir.exists() {
            let mut dir = tokio::fs::read_dir(workflows_dir).await?;
            while let Some(entry) = dir.next_entry().await? {
                let path = entry.path();
                if path.is_file()
                    && let Some(ext) = path.extension()
                    && (ext == "yml" || ext == "yaml")
                    && let Some(name) = path.file_name().and_then(|n| n.to_str())
                {
                    let content = tokio::fs::read_to_string(&path).await?;
                    workflows.insert(name.to_owned(), content);
                }
            }
        }
        Ok(workflows)
    } else {
        let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repo).await {
            let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
            Some(crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await?)
        } else {
            shared.state.static_github_pat()
        };
        if let Some(token) = &token {
            fetch_remote_workflows(&shared.state.github_breaker, token, repo, git_ref, api_base)
                .await
        } else if !git_ref.is_empty() {
            anyhow::bail!(
                "cannot fetch workflow revision {git_ref:?} without a local workspace or GitHub credentials"
            );
        } else {
            let workflows_dir = PathBuf::from(".").join(".github/workflows");
            let mut workflows = BTreeMap::new();
            if workflows_dir.exists() {
                let mut dir = tokio::fs::read_dir(workflows_dir).await?;
                while let Some(entry) = dir.next_entry().await? {
                    let path = entry.path();
                    if path.is_file()
                        && let Some(ext) = path.extension()
                        && (ext == "yml" || ext == "yaml")
                        && let Some(name) = path.file_name().and_then(|n| n.to_str())
                    {
                        let content = tokio::fs::read_to_string(&path).await?;
                        workflows.insert(name.to_owned(), content);
                    }
                }
            }
            Ok(workflows)
        }
    }
}

async fn fetch_remote_workflows(
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    git_ref: &str,
    api_base: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!(
        "{}/repos/{}/contents/.github/workflows?ref={}",
        api_base.trim_end_matches('/'),
        repo,
        git_ref
    );
    let response = crate::github_breaker::send_observed(
        breaker,
        client
            .get(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {}", token))
            .header("Accept", "application/vnd.github+json"),
    )
    .await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "GitHub API returned status: {}",
            response.status()
        ));
    }

    #[derive(Deserialize)]
    struct GitHubContentItem {
        name: String,
        r#type: String,
        download_url: Option<String>,
    }

    let items: Vec<GitHubContentItem> = response.json().await?;
    let mut workflows = BTreeMap::new();

    for item in &items {
        if item.r#type == "file"
            && (item.name.ends_with(".yml") || item.name.ends_with(".yaml"))
            && let Some(download_url) = &item.download_url
        {
            let file_res = crate::github_breaker::send_observed(
                breaker,
                client
                    .get(download_url)
                    .header("User-Agent", "preloop")
                    .header("Authorization", format!("Bearer {}", token)),
            )
            .await?;
            if !file_res.status().is_success() {
                let status = file_res.status();
                let err_text = file_res.text().await.unwrap_or_default();
                anyhow::bail!(
                    "failed to download workflow file {}: GitHub returned {} ({})",
                    item.name,
                    status,
                    err_text
                );
            }
            let content = file_res.text().await?;
            workflows.insert(item.name.clone(), content);
        }
    }
    Ok(workflows)
}

/// Resolve `git_ref` to a commit SHA, using the same credential ladder as
/// [`fetch_workflows`]: a local workspace is answered by `git rev-parse`,
/// otherwise the PAT or an App-minted `contents: read` installation token
/// queries the GitHub commits API. Returns `Ok(None)` when the ref does not
/// resolve (unknown ref, offline without a workspace) — the caller decides
/// what that means.
pub async fn resolve_ref_sha(
    shared: &Arc<SharedState>,
    repository: &str,
    git_ref: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(workspace) = &shared.state.local_workspace {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["rev-parse", git_ref])
            .output()
            .await?;
        if output.status.success() {
            return Ok(String::from_utf8(output.stdout)
                .ok()
                .map(|sha| sha.trim().to_owned())
                .filter(|sha| preloop_gha_protocol::git_ref::is_commit_sha(sha)));
        }
        return Ok(None);
    }
    let api_base = github_api_base();
    let Some(token) = contents_read_token(shared, repository, &api_base).await? else {
        return Ok(None);
    };
    let commit_ref = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(git_ref);
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .get(format!(
                "{api_base}/repos/{repository}/commits/{commit_ref}"
            ))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let commit: Value = response.json().await?;
    Ok(commit.get("sha").and_then(Value::as_str).map(str::to_owned))
}

/// The credential [`resolve_ref_sha`] and [`resolve_ref_protected`] query
/// GitHub with: an App-minted `contents: read` installation token for the
/// repository, else the static PAT. `None` when neither exists.
async fn contents_read_token(
    shared: &Arc<SharedState>,
    repository: &str,
    api_base: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await {
        let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        return Ok(Some(
            crate::github_app::get_or_mint_token_at(api_base, &app, repository, &permissions)
                .await?,
        ));
    }
    Ok(std::env::var("PRELOOP_GITHUB_TOKEN").ok())
}

/// GitHub's `github.ref_protected` for `git_ref`: whether branch protection
/// rules or rulesets apply to it. Only `refs/heads/*` can be protected; tags,
/// pull-request refs, a local workspace, and any lookup that fails resolve
/// to `false`. That is the unprivileged answer — consumers grant more to a
/// protected ref (cache writers publish only from one, OIDC trust policies
/// key on the claim) — so an unknown ref must never read as protected.
pub async fn resolve_ref_protected(
    shared: &Arc<SharedState>,
    repository: &str,
    git_ref: &str,
) -> bool {
    let Some(branch) = git_ref.strip_prefix("refs/heads/") else {
        return false;
    };
    if shared.state.local_workspace.is_some() {
        return false;
    }
    match fetch_branch_protected(shared, repository, branch).await {
        Ok(protected) => protected,
        Err(error) => {
            warn!(
                repository,
                branch,
                ?error,
                "could not resolve branch protection; reporting github.ref_protected = false"
            );
            false
        }
    }
}

async fn fetch_branch_protected(
    shared: &Arc<SharedState>,
    repository: &str,
    branch: &str,
) -> anyhow::Result<bool> {
    let api_base = github_api_base();
    let Some(token) = contents_read_token(shared, repository, &api_base).await? else {
        return Ok(false);
    };
    // Branch names are URL-significant: `git check-ref-format` permits `#`, `%`
    // and `/`, so a raw `release#test` is transmitted as `release` (the rest is
    // a fragment) and a lookup for an unprotected branch would answer with a
    // *different* branch's protection. Encode the name as one path segment, the
    // way the action-tarball URLs already do.
    let encoded = crate::actions::percent_encode_path_segment(branch);
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .get(format!("{api_base}/repos/{repository}/branches/{encoded}"))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await?;
    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(false);
    }
    if !status.is_success() {
        anyhow::bail!("GitHub returned {status} for branch {branch:?}");
    }
    let body: Value = response.json().await?;
    // The answer must be about the branch that was asked for: a server (or
    // intermediary) that folds or drops part of the encoded name — or resolves
    // it to another ref — would otherwise lend that branch's protection to
    // this run. Anything but an exact match is unresolvable, and unresolvable
    // is unprivileged.
    match body.get("name").and_then(Value::as_str) {
        Some(name) if name == branch => {}
        other => {
            warn!(
                repository,
                branch,
                resolved = ?other,
                "branch protection lookup answered for a different branch; \
                 reporting github.ref_protected = false"
            );
            return Ok(false);
        }
    }
    Ok(body
        .get("protected")
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

async fn get_pr_changed_files(
    breaker: &crate::github_breaker::GithubBreaker,
    token: &str,
    repo: &str,
    pr_number: u64,
    api_base: &str,
) -> anyhow::Result<Vec<String>> {
    let client = crate::shared_http::CLIENT.clone();
    let mut page = 1;
    let mut all_files = Vec::new();

    #[derive(Deserialize)]
    struct GitHubFileItem {
        filename: String,
    }

    loop {
        let url = format!(
            "{}/repos/{}/pulls/{}/files?per_page=100&page={}",
            api_base.trim_end_matches('/'),
            repo,
            pr_number,
            page
        );
        let response = crate::github_breaker::send_observed(
            breaker,
            client
                .get(&url)
                .header("User-Agent", "preloop")
                .header("Authorization", format!("Bearer {}", token))
                .header("Accept", "application/vnd.github+json"),
        )
        .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "GitHub API returned status: {}",
                response.status()
            ));
        }

        let files: Vec<GitHubFileItem> = response.json().await?;
        if files.is_empty() {
            break;
        }

        all_files.extend(files.into_iter().map(|f| f.filename));
        page += 1;
    }

    Ok(all_files)
}

/// Changed files for a pull request, or `None` when nothing can authenticate
/// the lookup.
///
/// A webhook payload never carries the full file list, so `paths:` and
/// `paths-ignore:` can only be evaluated against this call. Consulting
/// `PRELOOP_GITHUB_TOKEN` alone would leave an App-only deployment — the
/// documented way to run this server — permanently unable to answer, and every
/// path-filtered workflow would be rejected as unevaluable rather than queued.
/// So the App is tried first, exactly as the workflow inventory does.
///
/// The workflow-inventory token is not reused: it is scoped to
/// `contents: read`, and listing pull request files needs `pull_requests`.
pub async fn resolve_pr_changed_files_at(
    shared: &Arc<SharedState>,
    repo: &str,
    pr_number: u64,
    api_base: &str,
) -> anyhow::Result<Option<Vec<String>>> {
    let token = if let Some(app) = crate::github_app::select_app_for_repo(shared, repo).await {
        let permissions = BTreeMap::from([("pull_requests".to_owned(), "read".to_owned())]);
        Some(crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await?)
    } else {
        std::env::var("PRELOOP_GITHUB_TOKEN").ok()
    };
    let Some(token) = token else {
        return Ok(None);
    };
    get_pr_changed_files(
        &shared.state.github_breaker,
        &token,
        repo,
        pr_number,
        api_base,
    )
    .await
    .map(Some)
}

const WEBHOOK_ACK_BUDGET: Duration = Duration::from_secs(8);
const WEBHOOK_ENQUEUE_ATTEMPTS: usize = 2;
const WEBHOOK_LEASE_DURATION_SECS: u64 = 60;
const WEBHOOK_LEASE_RENEW_INTERVAL_SECS: u64 = 20;
const WEBHOOK_DELIVERY_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const WEBHOOK_MAX_ATTEMPTS: u32 = 6;
const WEBHOOK_PRUNE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const WEBHOOK_PRUNE_BATCH_SIZE: usize = 256;
/// How stale the published queue counters may get while the worker has nothing
/// to do. The worker refreshes them whenever it moves or prunes a row; this
/// only covers changes it did not make — another engine on a shared store, or
/// an operator replay through the native API.
const WEBHOOK_STATS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Backoff ladder for transient webhook delivery failures, indexed by the
/// delivery's attempt count (the final entry repeats).
///
/// The two cheap leading retries cover what this path actually sees: a
/// workflow file momentarily unreadable, or a snapshot not yet visible. The
/// rest stretches so a broken dependency is not hammered, and the attempt cap
/// dead-letters a delivery long before the ladder could become a hot loop.
pub const WEBHOOK_RETRY_BACKOFF: &[Duration] = &[
    Duration::from_secs(1),
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(15),
    Duration::from_secs(30),
];

/// Delay before the next attempt at a delivery that has now failed `attempts`
/// times.
///
/// Takes the ladder as an argument rather than reading the constant so the
/// retry path is driven by state: tests shorten it instead of sleeping through
/// real tiers. An empty ladder falls back to the first tier, never to zero —
/// "retry immediately" is the one answer this must not invent.
fn webhook_retry_backoff(ladder: &[Duration], attempts: u32) -> Duration {
    ladder
        .get(attempts as usize)
        .or_else(|| ladder.last())
        .copied()
        .unwrap_or_else(|| WEBHOOK_RETRY_BACKOFF[0])
}

/// Publish the queue counters the operational snapshot reads.
///
/// The sampler used to query the store on every tick to report these. The
/// queue worker both mutates the queue and already talks to the store, so it
/// hands over what it read and the snapshot reads memory instead of taking the
/// store's single connection for a query that usually reports "unchanged".
pub async fn refresh_webhook_queue_stats(state: &crate::state::AppState) -> bool {
    match state.backend.webhook_queue_stats().await {
        Ok(stats) => {
            state.webhook_status.set_queue_stats(stats);
            true
        }
        Err(error) => {
            debug!(?error, "failed to refresh webhook queue counters");
            false
        }
    }
}

/// Commit the delivery row within the webhook acknowledgement budget.
///
/// The 202 response is the durable boundary. GitHub does not automatically
/// redeliver a delivery after a non-2xx response, so retry quick store errors
/// here, but never wait indefinitely behind a contended store connection.
async fn enqueue_webhook_delivery_with_budget(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) -> anyhow::Result<bool> {
    let deadline = Instant::now() + WEBHOOK_ACK_BUDGET;
    let mut last_error = None;

    for attempt in 0..WEBHOOK_ENQUEUE_ATTEMPTS {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt_budget = remaining.min(Duration::from_secs(4));
        match tokio::time::timeout(
            attempt_budget,
            shared.state.backend.enqueue_webhook_delivery(delivery),
        )
        .await
        {
            Ok(Ok(inserted)) => return Ok(inserted),
            Ok(Err(error)) => {
                last_error = Some(error.to_string());
                if attempt + 1 < WEBHOOK_ENQUEUE_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "timed out persisting webhook delivery after {}s",
                    WEBHOOK_ACK_BUDGET.as_secs()
                ));
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to persist webhook delivery after {WEBHOOK_ENQUEUE_ATTEMPTS} attempts: {}",
        last_error.unwrap_or_else(|| "acknowledgement budget exhausted".to_owned())
    ))
}

/// Route handler for GitHub App Webhooks.
///
/// Verifies the signature, atomically enqueues the delivery to the durable
/// store, and acknowledges with HTTP 202 Accepted. Background workers drain
/// the queue asynchronously.
/// The `repository.full_name` a webhook payload claims, if any.
///
/// Read before any event processing so the signer's installation coverage
/// can be bound to the claimed repository. A payload without a repository
/// (e.g. `ping`) has nothing to bind.
fn claimed_repository(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("repository")?
        .get("full_name")?
        .as_str()
        .map(str::to_owned)
}

pub async fn handle_github_webhook(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<impl IntoResponse, StatusCode> {
    // Verify Signature
    let sig_header = headers
        .get("x-hub-signature-256")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    // Every registered App's secret is a candidate : a payload signed by
    // any App preloop fronts is accepted, one signed by none is rejected.
    // Identify WHICH credential verified the payload — the signer
    // binds the claimed repository below.
    let signers: Vec<(String, crate::github_app::WebhookSigner)> = match &shared.state.github_apps {
        Some(apps) => apps.webhook_signers(shared.state.webhook_secret.as_deref()),
        None => shared
            .state
            .webhook_secret
            .clone()
            .filter(|secret| !secret.is_empty())
            .map(|secret| (secret, crate::github_app::WebhookSigner::Legacy))
            .into_iter()
            .collect(),
    };
    if signers.is_empty() {
        warn!("No webhook secret is configured on the server, rejecting request");
        return Err(StatusCode::UNAUTHORIZED);
    }
    let signer = signers
        .iter()
        .find(|(secret, _)| verify_signature(secret, &body, sig_header))
        .map(|(_, signer)| signer.clone());
    let Some(signer) = signer else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    // Bind the claimed repository to the signer's installation
    // coverage BEFORE any event processing (adapters and check_run
    // rerequests alike). The signature only proves *some* registered
    // credential sent the payload — without binding, a payload signed by
    // App B's secret could claim App A's repository and trigger runs
    // there under App A's credentials. Coverage is verified at exact
    // repository granularity: an owner-granularity check would pass a
    // selected-repository installation for a sibling repo under the same
    // owner that the installation does not cover.
    if let crate::github_app::WebhookSigner::App(app_id) = &signer
        && let Some(claimed) = claimed_repository(&body)
    {
        // Resolve the App's credentials here rather than in the signer:
        // the signer carries only the id, so a delivery never clones App
        // key material.
        let covers = match shared
            .state
            .github_apps
            .as_ref()
            .and_then(|apps| apps.app_by_id(app_id))
        {
            Some(app) => crate::github_app::app_covers_repository(app, &claimed).await,
            // The payload's secret matched a registered App's secret, so
            // its id must resolve. Fail closed rather than skip binding.
            None => false,
        };
        if !covers {
            warn!(
                app_id = %app_id,
                repository = %claimed,
                "webhook signer is not installed on the claimed repository; rejecting"
            );
            return Err(StatusCode::FORBIDDEN);
        }
    }
    // `WebhookSigner::Legacy`: the single shared secret is the only trust
    // anchor — with no registry there is no cross-App confusion to bind
    // against.

    let event_name = headers
        .get("x-github-event")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    let delivery_id = headers
        .get("x-github-delivery")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let record = WebhookDeliveryRecord {
        delivery_id: delivery_id.clone(),
        event: event_name.to_owned(),
        payload: body.to_vec(),
        received_at_us: crate::store::now_us(),
        state: WebhookDeliveryStatus::Received,
        attempts: 0,
        lease_until_us: None,
        lease_token: None,
        last_error: None,
    };

    // A failed durable commit MUST NOT be acknowledged. GitHub does not
    // automatically redeliver non-2xx responses, so the delivery remains
    // available for an operator/API redelivery after the store recovers.
    match enqueue_webhook_delivery_with_budget(&shared, &record).await {
        Ok(true) => {
            info!(delivery = %delivery_id, event = %event_name, "GitHub webhook delivery queued");
            // Keep one permit when the worker is between drains; `notify_waiters`
            // can lose this wakeup and defer processing to the polling tick.
            shared.state.webhook_queue_notify.notify_one();
            Ok((
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "delivery_id": delivery_id, "status": "accepted" })),
            ))
        }
        Ok(false) => {
            info!(
                delivery = %delivery_id,
                event = %event_name,
                "Duplicate GitHub webhook delivery — already enqueued"
            );
            Ok((
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "delivery_id": delivery_id, "status": "duplicate" })),
            ))
        }
        Err(error) => {
            error!(
                delivery = %delivery_id,
                ?error,
                "Failed to commit webhook delivery row — returning 500; redelivery is manual"
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Handle GitHub's Checks API rerequest action by resubmitting the native run
/// that owns the requested check. GitHub sends this as a `check_run` webhook,
/// not as a workflow trigger event.
async fn process_check_run_rerequest(
    shared: &Arc<SharedState>,
    payload: &Value,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    if payload.get("action").and_then(Value::as_str) != Some("rerequested") {
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    }

    let Some(check_run) = payload.get("check_run") else {
        warn!("check_run rerequest is missing check_run payload");
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };
    let Some(check_run_id) = check_run.get("id").and_then(Value::as_u64) else {
        warn!("check_run rerequest is missing check_run.id");
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };
    let repository = payload
        .get("repository")
        .and_then(|repository| repository.get("full_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let head_sha = check_run
        .get("head_sha")
        .and_then(Value::as_str)
        .filter(|sha| !sha.is_empty());
    let job_name = check_run.get("name").and_then(Value::as_str);
    let details_run_id = check_run
        .get("details_url")
        .and_then(Value::as_str)
        .and_then(|url| {
            url.trim_end_matches('/')
                .rsplit('/')
                .next()
                .and_then(|value| value.parse::<RunId>().ok())
        });

    let target = shared
        .state
        .backend
        .check_run_target(check_run_id, repository, head_sha, job_name, details_run_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Some((run_id, job_id)) = target else {
        warn!(
            repository,
            check_run_id, "check_run rerequest does not match a known terminal run"
        );
        return Ok((StatusCode::OK, Json(serde_json::json!([]))));
    };

    // The backend target carries only the (run, job) identity; the
    // execution-protection gate below also needs the original trigger's
    // event, actor and workflow file, so reread the run it resolved.
    let run = shared
        .state
        .backend
        .run_record(run_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let event = run.submission.event.clone();
    let actor = run.submission.actor.clone();
    let workflow_file = run.submission.workflow_file.clone();

    // Workflow execution protections: a rerequest re-triggers the original
    // run, so the original trigger must still pass policy — and so must the
    // user who sent the rerequest. Actor rules match the webhook sender's
    // login, so a blocked actor could otherwise re-run someone else's
    // terminal run by clicking "Re-run". Deny if either matches; a denial
    // is not transient and must not retry.
    let rerequest_sender = payload
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(Value::as_str);
    let denied = [Some(actor.as_str()), rerequest_sender]
        .into_iter()
        .flatten()
        .find_map(|protection_actor| {
            crate::execution_protection::denies_submission(
                &shared.state.execution_protection,
                &event,
                Some(protection_actor),
                workflow_file.as_deref(),
            )
        });
    if let Some(hit) = denied {
        match shared.state.execution_protection.mode {
            crate::config::ProtectionMode::Enforce => {
                info!(
                    %run_id,
                    %job_id,
                    event = %event,
                    rule = %hit.describe(),
                    "execution protection denied check_run rerequest"
                );
                return Ok((StatusCode::OK, Json(serde_json::json!([]))));
            }
            crate::config::ProtectionMode::Evaluate => {
                info!(
                    %run_id,
                    %job_id,
                    event = %event,
                    rule = %hit.describe(),
                    "execution protection would deny check_run rerequest (evaluate mode)"
                );
            }
        }
    }

    let accepted = crate::rerun_run_inner(shared, run_id, Some((job_id.clone(), check_run_id)))
        .await
        .map_err(|error| {
            error!(
                %run_id,
                %job_id,
                check_run_id,
                ?error,
                "failed to resubmit check_run rerequest"
            );
            error.into_response().status()
        })?;
    info!(
        %run_id,
        rerun_run_id = %accepted.run_id,
        %job_id,
        check_run_id,
        "resubmitted check_run rerequest"
    );
    Ok((StatusCode::OK, Json(serde_json::json!([accepted]))))
}

/// Outcome of processing a webhook delivery payload.
#[derive(Debug)]
enum WebhookOutcome {
    Success,
    TransientError(String),
    PermanentErrors {
        repo: String,
        failures: Vec<WebhookFailure>,
    },
    Unreportable(String),
    /// GitHub itself is unreachable. Distinct from `TransientError` because
    /// the delivery did nothing wrong: it is returned to the queue with its
    /// attempt refunded and retried after the breaker's window, instead of
    /// spending one of six attempts on an outage it cannot influence.
    DependencyUnavailable {
        error: String,
        retry_after_secs: u64,
    },
}
#[derive(Debug)]
struct WebhookFailure {
    sha: Option<String>,
    check_name: String,
    error: String,
}

/// Renew a claimed delivery while its workflow evaluation is in flight.
///
/// A worker may spend longer than the initial lease fetching workflow files or
/// creating checks. Without renewal, another worker can reclaim the same row
/// and submit duplicate runs before the first worker finishes. The fencing
/// token prevents a stale worker from renewing the replacement lease.
async fn run_webhook_lease_heartbeat(
    shared: Arc<SharedState>,
    delivery_id: String,
    lease_token: String,
    lease_until_us: i64,
    lease_lost: tokio_util::sync::CancellationToken,
) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(WEBHOOK_LEASE_RENEW_INTERVAL_SECS));
    let remaining_us = lease_until_us.saturating_sub(crate::store::now_us()).max(0) as u64;
    let mut lease_deadline = Instant::now() + Duration::from_micros(remaining_us);
    let mut deadline = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
        lease_deadline,
    )));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => {
                lease_lost.cancel();
                return;
            }
            _ = &mut deadline => {
                warn!(
                    delivery = %delivery_id,
                    "webhook delivery lease expired while renewal was unavailable"
                );
                lease_lost.cancel();
                return;
            }
            _ = interval.tick() => {
                let renewal_timeout = lease_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(WEBHOOK_LEASE_RENEW_INTERVAL_SECS));
                if renewal_timeout.is_zero() {
                    lease_lost.cancel();
                    return;
                }
                let renewal = tokio::time::timeout(
                    renewal_timeout,
                    shared
                        .state
                        .backend
                        .renew_webhook_delivery(
                            &delivery_id,
                            &lease_token,
                            WEBHOOK_LEASE_DURATION_SECS,
                        ),
                )
                .await;
                match renewal {
                    Ok(Ok(true)) => {
                        debug!(delivery = %delivery_id, "renewed webhook delivery lease");
                        lease_deadline =
                            Instant::now() + Duration::from_secs(WEBHOOK_LEASE_DURATION_SECS);
                        deadline
                            .as_mut()
                            .reset(tokio::time::Instant::from_std(lease_deadline));
                    }
                    Ok(Ok(false)) => {
                        warn!(
                            delivery = %delivery_id,
                            "webhook delivery lease was fenced while processing"
                        );
                        lease_lost.cancel();
                        return;
                    }
                    Ok(Err(error)) => {
                        warn!(
                            delivery = %delivery_id,
                            ?error,
                            "failed to renew webhook delivery lease"
                        );
                        if Instant::now() >= lease_deadline {
                            lease_lost.cancel();
                            return;
                        }
                    }
                    Err(_) => {
                        warn!(
                            delivery = %delivery_id,
                            "timed out renewing webhook delivery lease"
                        );
                        if Instant::now() >= lease_deadline {
                            lease_lost.cancel();
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Background task that continuously drains the durable webhook queue.
pub async fn run_webhook_queue_worker(
    shared: Arc<SharedState>,
    heartbeat: preloop_observability::HeartbeatHandle,
) {
    // Registering happens before the task is spawned so a panic during
    // startup remains visible to readiness checks. Beat before recovery and
    // again on every loop; a stuck queue worker must not look healthy.
    heartbeat.beat();
    // Crash recovery: on boot, reset processing rows whose lease has expired back to received.
    if let Err(error) = shared.state.backend.recover_webhook_deliveries().await {
        warn!(
            ?error,
            "failed to recover stale webhook deliveries on startup"
        );
    }
    let mut last_prune = Instant::now();
    let mut last_stats_refresh = Instant::now();
    // Publish once before the first drain: the boot snapshot is built while
    // this task is still starting, and every later snapshot reads the cache.
    if !refresh_webhook_queue_stats(&shared.state).await {
        // Startup refresh failed; keep it due immediately on the next loop
        // rather than treating the unpopulated cache as fresh for 60 seconds.
        last_stats_refresh = Instant::now() - WEBHOOK_STATS_REFRESH_INTERVAL;
    }

    loop {
        heartbeat.beat();
        if shared.shutdown.is_cancelled() {
            break;
        }
        let processed = match drain_webhook_queue_with_heartbeat(&shared, &heartbeat).await {
            Ok(processed) => processed,
            Err(error) => {
                if !shared.shutdown.is_cancelled() {
                    warn!(?error, "error draining webhook delivery queue");
                }
                0
            }
        };
        let mut pruned = 0u64;
        if last_prune.elapsed() >= WEBHOOK_PRUNE_INTERVAL {
            last_prune = Instant::now();
            let cutoff = crate::store::now_us()
                .saturating_sub(WEBHOOK_DELIVERY_RETENTION_SECS.saturating_mul(1_000_000));
            match shared
                .state
                .backend
                .prune_webhook_deliveries(cutoff, WEBHOOK_PRUNE_BATCH_SIZE)
                .await
            {
                Ok(count) => {
                    if count > 0 {
                        info!(pruned = count, "pruned terminal webhook deliveries");
                    }
                    pruned = count;
                }
                Err(error) => warn!(?error, "failed to prune terminal webhook deliveries"),
            }
        }
        // Every row this worker moved or pruned changed the counters, so the
        // cache is refreshed then. The interval is the fallback for changes
        // made elsewhere (a second engine on a shared store, an operator
        // replay), and keeps a stuck queue visible instead of frozen at the
        // last busy moment.
        let counters_changed = processed > 0 || pruned > 0;
        let refresh_due = last_stats_refresh.elapsed() >= WEBHOOK_STATS_REFRESH_INTERVAL;
        if (counters_changed || refresh_due) && refresh_webhook_queue_stats(&shared.state).await {
            last_stats_refresh = Instant::now();
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = shared.state.webhook_queue_notify.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
        }
    }
}

/// Concurrent webhook deliveries processed per node
/// (`PRELOOP_WEBHOOK_WORKERS`, default 8). Deliveries are independent:
/// cross-delivery ordering is decided by GitHub event timestamps
/// (`concurrency::EventOrder`), not by processing order, and other nodes
/// already drain the same queue in parallel.
fn webhook_workers() -> usize {
    std::env::var("PRELOOP_WEBHOOK_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(8)
        .max(1)
}

/// Drain pending webhook deliveries (oldest received first) with
/// [`webhook_workers`] concurrent claim loops.
pub async fn drain_webhook_queue(shared: &Arc<SharedState>) -> anyhow::Result<usize> {
    let loops = (0..webhook_workers()).map(|_| drain_webhook_queue_loop(shared));
    let mut total = 0;
    for result in futures::future::join_all(loops).await {
        total += result?;
    }
    Ok(total)
}

async fn drain_webhook_queue_loop(shared: &Arc<SharedState>) -> anyhow::Result<usize> {
    let mut total_processed = 0;
    loop {
        if shared.shutdown.is_cancelled() {
            break;
        }
        // A known GitHub outage means every claim here would charge an
        // attempt and then fail on the same dependency. Not claiming is the
        // difference between riding out an incident and dead-lettering every
        // push that arrived during it.
        if let Some(retry_after) = shared.state.github_breaker.retry_after() {
            debug!(
                retry_in_secs = retry_after.as_secs(),
                "GitHub breaker open; deferring webhook queue drain"
            );
            break;
        }
        // One row per claim so every processing lease is heartbeated; a
        // claimed batch could let later rows expire while earlier ones run.
        let deliveries = shared
            .state
            .backend
            .claim_webhook_deliveries(1, WEBHOOK_LEASE_DURATION_SECS)
            .await?;
        let Some(delivery) = deliveries.first() else {
            break;
        };
        process_one_delivery(shared, delivery).await;
        total_processed += 1;
    }
    Ok(total_processed)
}

async fn drain_webhook_queue_with_heartbeat(
    shared: &Arc<SharedState>,
    heartbeat: &preloop_observability::HeartbeatHandle,
) -> anyhow::Result<usize> {
    let mut drain = Box::pin(drain_webhook_queue(shared));
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            result = &mut drain => return result,
            _ = ticker.tick() => heartbeat.beat(),
        }
    }
}

/// Process one claimed delivery and update its state according to the failure taxonomy.
pub async fn process_one_delivery(shared: &Arc<SharedState>, delivery: &WebhookDeliveryRecord) {
    let Some(lease_token) = delivery.lease_token.as_deref() else {
        error!(
            delivery_id = %delivery.delivery_id,
            "claimed webhook delivery has no lease token"
        );
        return;
    };
    let Some(lease_until_us) = delivery.lease_until_us else {
        error!(
            delivery_id = %delivery.delivery_id,
            "claimed webhook delivery has no lease expiry"
        );
        return;
    };
    let lease_lost = tokio_util::sync::CancellationToken::new();
    let heartbeat = tokio::spawn(run_webhook_lease_heartbeat(
        shared.clone(),
        delivery.delivery_id.clone(),
        lease_token.to_owned(),
        lease_until_us,
        lease_lost.clone(),
    ));
    // Isolate payload processing so a panic cannot strand the heartbeat task
    // or leave the queue row in `processing` forever. A fenced or expired
    // lease cancels this task before it can report external side effects.
    let processing_shared = shared.clone();
    let processing_delivery = delivery.clone();
    let processing_lease_lost = lease_lost.clone();
    let mut processing = tokio::spawn(async move {
        process_delivery_payload_with_lease(
            &processing_shared,
            &processing_delivery,
            &processing_lease_lost,
        )
        .await
    });
    let outcome = tokio::select! {
        _ = lease_lost.cancelled() => {
            processing.abort();
            let _ = processing.await;
            None
        }
        result = &mut processing => Some(match result {
            Ok(outcome) => outcome,
            Err(error) => {
                WebhookOutcome::Unreportable(format!("webhook processing task failed: {error}"))
            }
        }),
    };
    heartbeat.abort();
    let _ = heartbeat.await;
    let Some(outcome) = outcome else {
        warn!(
            delivery_id = %delivery.delivery_id,
            "abandoned webhook processing after losing its lease"
        );
        return;
    };
    // The payload may have completed at the same instant the heartbeat
    // observed fencing. Do not report checks or mutate the queue row after
    // that point; the replacement worker owns the retry.
    if lease_lost.is_cancelled() {
        warn!(
            delivery_id = %delivery.delivery_id,
            "discarding webhook outcome after losing its lease"
        );
        return;
    }
    // Outage reclassification happens BEFORE the attempt cap: a delivery
    // must never be dead-lettered for a dependency that was down. The
    // breaker, not the delivery, is the authority on whether GitHub is the
    // reason a step failed.
    let outcome = match outcome {
        WebhookOutcome::TransientError(error) => {
            match shared.state.github_breaker.retry_after() {
                Some(retry_after) => WebhookOutcome::DependencyUnavailable {
                    error,
                    // Bounded: a long breaker window still gets re-examined
                    // periodically, and a short one does not become a spin.
                    retry_after_secs: retry_after.as_secs().clamp(5, 300),
                },
                None if delivery.attempts >= WEBHOOK_MAX_ATTEMPTS => {
                    WebhookOutcome::Unreportable(format!(
                        "transient webhook failure exceeded {WEBHOOK_MAX_ATTEMPTS} attempts: {error}"
                    ))
                }
                None => WebhookOutcome::TransientError(error),
            }
        }
        outcome => outcome,
    };
    match outcome {
        WebhookOutcome::Success => {
            match shared
                .state
                .backend
                .complete_webhook_delivery(&delivery.delivery_id, lease_token)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before marking completed"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to mark webhook delivery completed"
                ),
            }
        }
        WebhookOutcome::DependencyUnavailable {
            error,
            retry_after_secs,
        } => {
            warn!(
                delivery_id = %delivery.delivery_id,
                attempts = delivery.attempts,
                retry_after_secs,
                error = %error,
                "GitHub is unavailable; parking webhook delivery without spending an attempt"
            );
            match shared
                .state
                .backend
                .park_webhook_delivery(&delivery.delivery_id, lease_token, &error, retry_after_secs)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before parking it"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to park webhook delivery for a GitHub outage"
                ),
            }
        }
        WebhookOutcome::TransientError(err) => {
            let backoff =
                webhook_retry_backoff(&shared.state.webhook_retry_backoff, delivery.attempts);
            warn!(
                delivery_id = %delivery.delivery_id,
                attempts = delivery.attempts,
                backoff_ms = backoff.as_millis() as u64,
                error = %err,
                "transient error processing webhook delivery; retrying internally"
            );
            match shared
                .state
                .backend
                .fail_webhook_delivery(
                    &delivery.delivery_id,
                    lease_token,
                    &err,
                    false,
                    Some(backoff),
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before scheduling retry"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to update webhook delivery for retry"
                ),
            }
        }
        WebhookOutcome::PermanentErrors { repo, failures } => {
            let error = failures
                .iter()
                .map(|failure| format!("{}: {}", failure.check_name, failure.error))
                .collect::<Vec<_>>()
                .join("; ");
            error!(
                delivery_id = %delivery.delivery_id,
                %repo,
                error = %error,
                failures = failures.len(),
                "permanent errors processing webhook delivery; reporting failure check runs"
            );
            for failure in &failures {
                if lease_lost.is_cancelled() {
                    warn!(
                        delivery_id = %delivery.delivery_id,
                        "stopped reporting webhook failures after losing lease"
                    );
                    return;
                }
                if let Some(sha) = &failure.sha {
                    let report_result = tokio::select! {
                        _ = lease_lost.cancelled() => return,
                        result = report_check_run_permanent_failure(
                            shared,
                            &repo,
                            sha,
                            &failure.check_name,
                            &failure.error,
                        ) => result,
                    };
                    if let Err(report_error) = report_result {
                        let retry_error = format!(
                            "failed to report permanent check run: {report_error}; \
                             original workflow error: {error}"
                        );
                        let backoff = webhook_retry_backoff(
                            &shared.state.webhook_retry_backoff,
                            delivery.attempts,
                        );
                        warn!(
                            delivery_id = %delivery.delivery_id,
                            %report_error,
                            "check-run failure reporting failed; retrying webhook delivery"
                        );
                        if let Err(store_error) = shared
                            .state
                            .backend
                            .fail_webhook_delivery(
                                &delivery.delivery_id,
                                lease_token,
                                &retry_error,
                                false,
                                Some(backoff),
                            )
                            .await
                        {
                            warn!(
                                delivery_id = %delivery.delivery_id,
                                ?store_error,
                                "failed to schedule webhook retry after check-run reporting failure"
                            );
                        }
                        return;
                    }
                }
            }
            if lease_lost.is_cancelled() {
                return;
            }
            match shared
                .state
                .backend
                .fail_webhook_delivery(&delivery.delivery_id, lease_token, &error, true, None)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before marking permanently failed"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to mark webhook delivery permanently failed"
                ),
            }
        }
        WebhookOutcome::Unreportable(err) => {
            error!(
                delivery_id = %delivery.delivery_id,
                error = %err,
                "unreportable error processing webhook delivery; dead-lettering row"
            );
            match shared
                .state
                .backend
                .fail_webhook_delivery(&delivery.delivery_id, lease_token, &err, true, None)
                .await
            {
                Ok(true) => {}
                Ok(false) => warn!(
                    delivery_id = %delivery.delivery_id,
                    "lost webhook delivery lease before dead-lettering row"
                ),
                Err(error) => warn!(
                    delivery_id = %delivery.delivery_id,
                    ?error,
                    "failed to dead-letter webhook delivery row"
                ),
            }
        }
    }
}

async fn process_delivery_payload(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) -> WebhookOutcome {
    let lease_lost = tokio_util::sync::CancellationToken::new();
    process_delivery_payload_with_lease(shared, delivery, &lease_lost).await
}

async fn process_delivery_payload_with_lease(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
    lease_lost: &tokio_util::sync::CancellationToken,
) -> WebhookOutcome {
    let payload_val: Value = match serde_json::from_slice(&delivery.payload) {
        Ok(v) => v,
        Err(e) => {
            return WebhookOutcome::Unreportable(format!("malformed JSON payload: {e}"));
        }
    };
    let workflow_dispatch_target = if delivery.event == "workflow_dispatch" {
        let Some(target) = payload_val.get("workflow").and_then(Value::as_str) else {
            return WebhookOutcome::Unreportable(
                "workflow_dispatch payload missing workflow".to_owned(),
            );
        };
        Some(
            target
                .strip_prefix(".github/workflows/")
                .unwrap_or(target)
                .to_owned(),
        )
    } else {
        None
    };

    if lease_lost.is_cancelled() {
        return WebhookOutcome::Success;
    }

    if delivery.event == "check_run" {
        let rerequest = tokio::select! {
            _ = lease_lost.cancelled() => return WebhookOutcome::Success,
            result = process_check_run_rerequest(shared, &payload_val) => result,
        };
        match rerequest {
            Ok(_) => return WebhookOutcome::Success,
            Err(status) if status.is_server_error() => {
                return WebhookOutcome::TransientError(format!(
                    "check run rerequest failed with status {status}"
                ));
            }
            Err(status) => {
                info!(%status, "check run rerequest ignored");
                return WebhookOutcome::Success;
            }
        }
    }

    let adapter = match crate::events::adapter_for(&delivery.event) {
        Some(a) => a,
        None => {
            info!("No adapter for event: {}", delivery.event);
            return WebhookOutcome::Success;
        }
    };

    let effective_events = adapter.project(&payload_val);
    if effective_events.is_empty() {
        info!(
            "Event {} produced no effective events (e.g. [skip ci] or fork-gated)",
            delivery.event
        );
        return WebhookOutcome::Success;
    }

    // Workflow execution protections (unscoped): admin-level deny rules on
    // the event/actor are evaluated here, before the PR changed-files lookup
    // below — a denied delivery must not burn a GitHub API call (and retries)
    // on a lookup whose result can never be used. Scoped per-workflow rules
    // are still evaluated during workflow matching further down.
    let protection_actor = payload_val
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(|login| login.as_str());
    let mut live_events = Vec::with_capacity(effective_events.len());
    for effective in effective_events {
        if effective.skip {
            live_events.push(effective);
            continue;
        }
        match crate::execution_protection::denies_event(
            &shared.state.execution_protection,
            &effective.event,
            protection_actor,
        ) {
            Some(hit) => match shared.state.execution_protection.mode {
                crate::config::ProtectionMode::Enforce => {
                    info!(
                        event = %effective.event,
                        rule = %hit.describe(),
                        "execution protection denied event"
                    );
                }
                crate::config::ProtectionMode::Evaluate => {
                    info!(
                        event = %effective.event,
                        rule = %hit.describe(),
                        "execution protection would deny event (evaluate mode)"
                    );
                    live_events.push(effective);
                }
            },
            None => live_events.push(effective),
        }
    }
    // Every remaining event is skip-flagged, or every live event was denied
    // in enforce mode: nothing downstream can use the PR lookup.
    if live_events.iter().all(|effective| effective.skip) {
        return WebhookOutcome::Success;
    }
    let effective_events = live_events;

    let repo_full_name = match payload_val
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|v| v.as_str())
    {
        Some(name) => name.to_owned(),
        None => {
            return WebhookOutcome::Unreportable(
                "webhook payload missing repository.full_name".to_owned(),
            );
        }
    };

    let (changed_paths, changed_paths_known) = if matches!(
        delivery.event.as_str(),
        "pull_request" | "pull_request_target" | "pull_request_review"
    ) {
        let pr_number = payload_val
            .get("number")
            .or_else(|| {
                payload_val
                    .get("pull_request")
                    .and_then(|pr| pr.get("number"))
            })
            .and_then(|value| value.as_u64());
        let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_owned());
        let fetched = match pr_number {
            Some(number) => {
                match resolve_pr_changed_files_at(shared, &repo_full_name, number, &api_base).await
                {
                    Ok(files) => files,
                    Err(error) => {
                        error!(?error, "failed to resolve pull request changed files");
                        return WebhookOutcome::TransientError(format!(
                            "failed to resolve pull request changed files: {error:?}"
                        ));
                    }
                }
            }
            None => None,
        };
        match fetched {
            Some(files) => (files, true),
            None => (
                changed_paths_from_payload(&payload_val),
                payload_val.get("paths").is_some() || payload_val.get("commits").is_some(),
            ),
        }
    } else {
        (changed_paths_from_payload(&payload_val), true)
    };

    let github_owned_workflows = configured_github_owned_workflows();
    let mut unmatched_workflows: Vec<String> = Vec::new();
    let mut triggered_count = 0usize;
    let mut permanent_failures: Vec<WebhookFailure> = Vec::new();
    let mut permanent_failure_keys: BTreeSet<(Option<String>, String)> = BTreeSet::new();

    for effective in &effective_events {
        if lease_lost.is_cancelled() {
            return WebhookOutcome::Success;
        }
        if effective.skip {
            info!("Skipping event {} (skip flag set)", effective.event);
            continue;
        }

        // Fork-PR workflow policy: the operator kill switch for fork
        // pull-request workflows. Skips the event before any workflow is
        // fetched or matched. `pull_request_target` is unaffected — it runs
        // with base-repo trust (see execution protections for that knob).
        if effective.trust_tier
            == Some(crate::events::trust_tier::TrustTier::UntrustedForkPullRequest)
            && !shared.state.fork_policy.run_fork_workflows
        {
            info!(
                event = %effective.event,
                "fork policy skipped fork pull-request event (run_fork_workflows = false)"
            );
            continue;
        }

        // Unscoped execution-protection rules were already applied to every
        // effective event before the PR changed-files lookup above; scoped
        // per-workflow rules are evaluated during workflow matching below.
        let protection_actor = payload_val
            .get("sender")
            .and_then(|sender| sender.get("login"))
            .and_then(|login| login.as_str());

        let default_branch = payload_val
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");
        let ref_default = format!("refs/heads/{default_branch}");

        let workflow_ref = if effective.event == "workflow_run" {
            &ref_default
        } else {
            &effective.git_ref
        };

        let resolved_sha = match &effective.sha {
            Some(sha) => sha.clone(),
            None if effective.event == "pull_request_target" => {
                error!(
                    event = %effective.event,
                    ref_name = %workflow_ref,
                    "pull_request_target has no base commit SHA"
                );
                return WebhookOutcome::TransientError(
                    "pull_request_target has no base commit SHA".to_owned(),
                );
            }
            None => match resolve_ref_sha(shared, &repo_full_name, workflow_ref).await {
                Ok(Some(sha)) => sha,
                Ok(None) => {
                    error!(
                        ref_name = %workflow_ref,
                        "webhook workflow ref has no resolvable commit SHA"
                    );
                    return WebhookOutcome::TransientError(
                        "webhook workflow ref has no resolvable commit SHA".to_owned(),
                    );
                }
                Err(error) => {
                    error!(
                        ?error,
                        ref_name = %workflow_ref,
                        "failed to resolve webhook workflow ref SHA"
                    );
                    return WebhookOutcome::TransientError(format!(
                        "failed to resolve webhook workflow ref SHA: {error:?}"
                    ));
                }
            },
        };

        let workflows = match fetch_workflows(shared, &repo_full_name, &resolved_sha).await {
            Ok(w) => w,
            Err(error) => {
                error!(
                    event = %effective.event,
                    sha = %resolved_sha,
                    source_ref = %workflow_ref,
                    ?error,
                    "Failed to fetch workflows at the event commit"
                );
                return WebhookOutcome::TransientError(format!(
                    "Failed to fetch workflows at event commit: {error:?}"
                ));
            }
        };

        if effective.event == "push"
            && effective.git_ref == ref_default
            && let Some(scheduler) = &shared.state.scheduler
        {
            let scheduler_source = match resolve_ref_sha(shared, &repo_full_name, &ref_default)
                .await
            {
                Ok(Some(scheduler_sha)) => {
                    let scheduler_workflows = if scheduler_sha == resolved_sha {
                        Some(workflows.clone())
                    } else {
                        match fetch_workflows(shared, &repo_full_name, &scheduler_sha).await {
                            Ok(workflows) => Some(workflows),
                            Err(error) => {
                                warn!(
                                    sha = %scheduler_sha,
                                    ?error,
                                    "failed to fetch current default-branch workflows — skipping cron reconciliation"
                                );
                                None
                            }
                        }
                    };
                    scheduler_workflows.map(|workflows| (scheduler_sha, workflows))
                }
                Ok(None) => {
                    warn!(
                        ref_name = %ref_default,
                        "current default branch has no resolvable commit SHA — skipping cron reconciliation"
                    );
                    None
                }
                Err(error) => {
                    warn!(
                        ?error,
                        ref_name = %ref_default,
                        "failed to resolve current default branch SHA — skipping cron reconciliation"
                    );
                    None
                }
            };
            if let Some((scheduler_sha, scheduler_workflows)) = scheduler_source {
                let mut scheduler_payload = payload_val.clone();
                if let Some(object) = scheduler_payload.as_object_mut() {
                    object.insert("after".to_owned(), Value::String(scheduler_sha));
                }
                scheduler
                    .reconcile_all(&scheduler_workflows, scheduler_payload, shared.clone())
                    .await;
            }
        }

        // GitHub's `github.ref_protected` for this event's ref. Resolved once
        // per effective event, not per workflow: every workflow one event
        // fans out to sees the same ref.
        let ref_protected =
            resolve_ref_protected(shared, &repo_full_name, &effective.git_ref).await;

        for (filename, content) in workflows {
            if lease_lost.is_cancelled() {
                return WebhookOutcome::Success;
            }
            if workflow_dispatch_target
                .as_deref()
                .is_some_and(|target| target != filename)
            {
                continue;
            }

            if is_github_owned_workflow(&filename, &github_owned_workflows) {
                info!(
                    workflow = %filename,
                    event = %effective.event,
                    "Skipping workflow owned by GitHub"
                );
                continue;
            }

            // Per-file execution protections: scoped deny rules for this
            // event/actor/workflow file.
            if let Some(hit) = crate::execution_protection::denies_workflow(
                &shared.state.execution_protection,
                &effective.event,
                protection_actor,
                &filename,
            ) {
                match shared.state.execution_protection.mode {
                    crate::config::ProtectionMode::Enforce => {
                        info!(
                            workflow = %filename,
                            event = %effective.event,
                            rule = %hit.describe(),
                            "execution protection denied workflow"
                        );
                        continue;
                    }
                    crate::config::ProtectionMode::Evaluate => {
                        info!(
                            workflow = %filename,
                            event = %effective.event,
                            rule = %hit.describe(),
                            "execution protection would deny workflow (evaluate mode)"
                        );
                    }
                }
            }

            match preloop_gha_parser::parse_workflow(&content) {
                Ok(parsed) => {
                    if let Err(e) = parsed.on.validate_filters(&effective.event) {
                        warn!("Filter validation warning for {filename}: {e}");
                    }
                    if let Err(e) = parsed.on.check_conflicting_filters(&effective.event) {
                        warn!("Conflicting filters in {filename}: {e}");
                        continue;
                    }
                }
                Err(e) => {
                    let error_msg = format!("Failed to parse workflow file {filename}: {e:?}");
                    warn!("{error_msg}");
                    let sha = effective
                        .status_check_sha
                        .clone()
                        .or_else(|| Some(resolved_sha.clone()));
                    if permanent_failure_keys.insert((sha.clone(), filename.clone())) {
                        permanent_failures.push(WebhookFailure {
                            sha,
                            check_name: filename.clone(),
                            error: error_msg,
                        });
                    }
                    // A malformed file must not prevent other workflows at
                    // the same commit from being evaluated and queued.
                    continue;
                }
            }

            let filter_branch = if effective.event == "workflow_run" {
                effective
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else if matches!(
                effective.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                effective
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            };

            let submission = WorkflowSubmission {
                workflow_yaml: content,
                event: effective.event.clone(),
                payload: effective.payload.clone(),
                repository: repo_full_name.clone(),
                git_ref: effective.git_ref.clone(),
                workflow_path: Some(format!(".github/workflows/{filename}")),
                local_workspace: None,
                vars: BTreeMap::new(),
                secrets: BTreeMap::new(),
                run_secret_names: BTreeSet::new(),
                reusable_workflows: BTreeMap::new(),
                reusable_workflow_shas: BTreeMap::new(),
                enable_debugger: false,
                debugger_welcome_message: None,
                sha: resolved_sha.clone(),
                actor: payload_val
                    .get("sender")
                    .and_then(|s| s.get("login"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("preloop-system")
                    .to_owned(),
                environment: None,
                workflow_file: Some(filename.clone()),
                inputs: BTreeMap::new(),
                trust_tier: effective.trust_tier.as_ref().and_then(|tier| {
                    serde_json::to_value(tier)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                }),
                workflow_run_upstream_names: effective.upstream_workflow_names.clone(),
                activity_type: effective.activity_type.clone(),
                changed_paths: changed_paths.clone(),
                changed_paths_known,
                resolved_sha: Some(resolved_sha.clone()),
                status_check_sha: effective
                    .status_check_sha
                    .clone()
                    .or_else(|| Some(resolved_sha.clone())),
                filter_branch,
                ref_protected,
                dispatch_inputs: BTreeMap::new(),
                dispatch_inputs_stringified: BTreeMap::new(),
                selected_jobs: vec![],
                base_ref: None,
                preserve_on_failure: false,
                debug_on_failure: false,
                push: None,
                push_tree: None,
            };

            // The dedup gate decides whether a run may be submitted at all.
            // A failed read must abort the delivery (GitHub redelivers), not
            // fall through to submitting: `None` would re-run CI on the exact
            // commit push-back already tested and published.
            let tested_by = match crate::github_push::already_published(
                shared,
                &repo_full_name,
                &submission.sha,
                submission.workflow_path.as_deref().unwrap_or_default(),
            )
            .await
            {
                Ok(tested_by) => tested_by,
                Err(error) => {
                    error!(
                        ?error,
                        sha = %submission.sha,
                        "failed to read push-back publication state; refusing to submit a possible duplicate"
                    );
                    return WebhookOutcome::TransientError(format!(
                        "failed to read push-back publication state: {error:?}"
                    ));
                }
            };
            if let Some(tested_by) = tested_by {
                info!(
                    workflow = %filename,
                    sha = %submission.sha,
                    run_id = %tested_by,
                    "skipping webhook run: this commit was already tested and published by push-back"
                );
                continue;
            }

            let submission_result = tokio::select! {
                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                result = submit_run_inner_with_webhook_delivery(
                    shared,
                    submission,
                    Some(&delivery.delivery_id),
                ) => result,
            };
            match submission_result {
                Ok(accepted) => {
                    if lease_lost.is_cancelled() {
                        return WebhookOutcome::Success;
                    }
                    let run_id = accepted.run_id;
                    // Stamped even when every job is an expandable node — the
                    // dispatch list may be empty, so nothing downstream calls
                    // report_check_run_queued, but legs still materialize later
                    // and must report.
                    if let Err(error) = shared
                        .state
                        .backend
                        .set_reports_check_runs(run_id, true)
                        .await
                    {
                        warn!(%run_id, ?error, "failed to stamp reports_check_runs for webhook run");
                    }
                    let sha = effective
                        .status_check_sha
                        .clone()
                        .unwrap_or_else(|| resolved_sha.clone());
                    let info = shared
                        .state
                        .backend
                        .run_dispatch_info(run_id)
                        .await
                        .ok()
                        .flatten();
                    if let Some(info) = info {
                        // Expandable nodes (deferred matrices, reusable
                        // callers) are placeholders: expansion replaces them
                        // and their materialized legs mint their own checks.
                        // Minting a `queued` check here would strand it on
                        // GitHub (there is no delete API) for a dissolved
                        // placeholder that never dispatches.
                        for job in info.jobs.iter().filter(|job| !job.placeholder) {
                            let job_id = job.job_id.clone();
                            tokio::select! {
                                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                                res = report_check_run_queued(
                                    shared,
                                    &repo_full_name,
                                    &sha,
                                    &job_id,
                                    run_id,
                                ) => {
                                    if let Err(error) = res {
                                        return WebhookOutcome::TransientError(format!(
                                            "failed to report check run for {job_id:?}: {error:?}"
                                        ));
                                    }
                                }
                            };
                            if job.status.is_terminal() {
                                tokio::select! {
                                    _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                                    _ = report_check_run_completed(shared, run_id, &job_id, job.status) => {}
                                }
                            }
                        }
                    }
                    triggered_count += 1;
                }
                Err(e) => {
                    let detail = format!("{e:?}");
                    let status = e.into_response().status();
                    if status.is_client_error() {
                        debug!(
                            workflow = %filename,
                            event = %effective.event,
                            "workflow not triggered by this event ({detail}) — not a delivery failure"
                        );
                        unmatched_workflows.push(filename.clone());
                        continue;
                    }
                    error!("Failed to submit run for {filename}: {detail}");
                    return WebhookOutcome::TransientError(format!(
                        "Failed to submit run for {filename}: {detail}"
                    ));
                }
            }
        }
    }
    if !unmatched_workflows.is_empty() {
        info!(
            event = %delivery.event,
            unmatched = unmatched_workflows.len(),
            triggered = triggered_count,
            workflows = %unmatched_workflows.join(", "),
            "workflows evaluated but not triggered by this event"
        );
    }
    if !permanent_failures.is_empty() {
        return WebhookOutcome::PermanentErrors {
            repo: repo_full_name,
            failures: permanent_failures,
        };
    }

    WebhookOutcome::Success
}

/// Webhook events the App-manifest flow asks GitHub to subscribe a new App to.
///
/// Defaults to the minimal CI event set (`push`, `pull_request`). GitHub
/// cannot change an App's event subscriptions through its API after creation,
/// so operators who need additional triggers must add them manually in the
/// App settings UI. Operators who want a different creation-time set can
/// override it with `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` (comma-separated).
pub fn manifest_default_events() -> Vec<String> {
    if let Some(raw) = std::env::var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        let events: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|event| !event.is_empty())
            .map(str::to_owned)
            .collect();
        if !events.is_empty() {
            return events;
        }
    }
    vec!["push".to_owned(), "pull_request".to_owned()]
}

/// Serve registration page for GitHub App Manifest flow.
pub async fn github_register(headers: HeaderMap) -> impl IntoResponse {
    let host = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:9090");

    let scheme = if host.contains("localhost") || host.contains("127.0.0.1") {
        "http"
    } else {
        "https"
    };

    let base_url = format!("{}://{}", scheme, host);
    let is_local = host.contains("localhost") || host.contains("127.0.0.1");

    let mut manifest_json = serde_json::json!({
        "name": "preloop-local-app",
        "url": base_url,
        "redirect_url": format!("{}/api/v1/github/callback", base_url),
        "public": false,
        "default_permissions": {
            "checks": "write",
            "contents": "read",
            "metadata": "read",
            "pull_requests": "read"
        }
    });

    if !is_local {
        manifest_json["hook_attributes"] = serde_json::json!({
                "url": format!("{}/api/v1/github/webhooks", base_url)
        });
        manifest_json["default_events"] = serde_json::json!(manifest_default_events());
    }

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>Register GitHub App</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: auto;">
    <h1>Register GitHub App for preloop</h1>
    <p>Click the button below to register a local GitHub App on your GitHub account automatically.</p>
    <form action="https://github.com/settings/apps/new" method="post">
        <input type="hidden" name="manifest" value='{}'>
        <button type="submit" style="font-size: 16px; padding: 10px 20px; cursor: pointer; background: #2da44e; color: white; border: none; border-radius: 6px; font-weight: bold;">Register App on GitHub</button>
    </form>
    <p style="color: #57606a; font-size: 14px;">
        If you want to run CI before creating a pull request
        (<code>preloop run --push --create-pr</code>), grant the App
        <code>pull_requests: write</code>: GitHub App settings &rarr;
        Permissions &rarr; Pull requests &rarr; Read and write. Check-run
        reporting works with just <code>checks: write</code>.
    </p>
</body>
</html>"#,
        manifest_json
    );

    axum::response::Html(html)
}

/// Query parameters for GitHub callback.
#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    code: String,
}

/// Callback endpoint for GitHub App Manifest conversion.
pub async fn github_callback(
    // The App credentials are handed to the operator through the one-time HTML
    // response below; `AppState` is immutable behind an `Arc`, so nothing here
    // can persist them into the running server.
    State(_shared): State<Arc<SharedState>>,
    Query(params): Query<CallbackQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let client = crate::shared_http::CLIENT.clone();
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .unwrap_or_else(|_| "https://api.github.com".to_owned());
    let url = format!("{}/app-manifests/{}/conversions", api_base, params.code);
    let res = client
        .post(&url)
        .header("User-Agent", "preloop")
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if !res.status().is_success() {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[derive(Deserialize)]
    struct AppManifestConversion {
        id: u64,
        #[serde(default)]
        slug: Option<String>,
        pem: String,
        webhook_secret: Option<String>,
    }

    let credentials: AppManifestConversion = res
        .json()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    info!("Successfully registered GitHub App ID: {}", credentials.id);

    // The webhook secret is deliberately never logged: it is the only thing
    // standing between this server and a forged `push` event. It reaches the
    // operator through the one-time HTML handoff below, the same way the
    // private key does, and is configured from there.
    info!(
        has_webhook_secret = credentials.webhook_secret.is_some(),
        "GitHub App credentials received"
    );

    let install = credentials
        .slug
        .as_ref()
        .map(|slug| format!("https://github.com/apps/{slug}/installations/new"));
    let credentials_html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>GitHub App Registered</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 800px; margin: auto;">
    <h1 style="color: #2da44e;">GitHub App Registered Successfully!</h1>
    <p><strong>App ID:</strong> {}</p>
    <p><strong>Webhook Secret:</strong> {}</p>
    <p><strong>Private Key PEM:</strong></p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px; overflow-x: auto;">{}</pre>
    <p>Save the key to a file, then hand both to the engine and restart it:</p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px;">
preloop setup github --via app --app-id {} --pem-file ./app.pem
export PRELOOP_WEBHOOK_SECRET="{}"
    </pre>
    <p>{}</p>
    <p>On the machine running the engine, <code>preloop setup github --via app</code>
       does all of this without copying anything out of a browser.</p>
</body>
</html>"#,
        credentials.id,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        credentials.pem,
        credentials.id,
        credentials.webhook_secret.as_deref().unwrap_or("none"),
        install
            .as_ref()
            .map(|url| format!(
                r#"Then install it on your repositories: <a href="{url}">{url}</a>"#
            ))
            .unwrap_or_else(|| {
                "Then install it on your repositories from the App's settings page.".to_owned()
            }),
    );

    Ok(axum::response::Html(credentials_html))
}

#[cfg(test)]
#[allow(unsafe_code)] // SAFETY: env writes confined to serialized tests.
mod tests {
    use super::*;
    use crate::AppState;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    /// Sign `payload` the way GitHub does: HMAC-SHA256 over the raw body with
    /// the webhook secret, hex-encoded and prefixed with `sha256=`.
    fn sign(payload: &[u8], secret: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        let sig = mac.finalize().into_bytes();
        let hex = sig.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!("sha256={hex}")
    }

    fn git_output(workspace: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(workspace)
            // Test fixtures must not depend on the developer's signing
            // setup — a broken or locked signing agent (e.g. 1Password)
            // would otherwise fail every commit fixture.
            .arg("-c")
            .arg("commit.gpgsign=false")
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn commit_workspace(workspace: &std::path::Path) -> String {
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["config", "user.email", "github-tests@example.invalid"][..],
            &["config", "user.name", "GitHub Tests"][..],
        ] {
            git_output(workspace, args);
        }
        git_output(workspace, &["add", "-A"]);
        git_output(workspace, &["commit", "-qm", "test workflow"]);
        git_output(workspace, &["rev-parse", "HEAD"])
    }

    #[test]
    fn github_owned_workflow_filter_matches_filename_or_path() {
        let configured =
            "release.yml, .github/workflows/release-golden.yml, ./release-linux-runner.yml"
                .split(',')
                .map(str::trim)
                .map(|entry| entry.trim_start_matches("./").to_owned())
                .collect();

        assert!(is_github_owned_workflow("release.yml", &configured));
        assert!(is_github_owned_workflow("release-golden.yml", &configured));
        assert!(is_github_owned_workflow(
            "release-linux-runner.yml",
            &configured
        ));
        assert!(!is_github_owned_workflow("ci.yml", &configured));
    }

    #[test]
    fn completed_check_summary_names_the_failed_step_and_durations() {
        let start = chrono::Utc::now();
        let mut setup = crate::models::StepRecord::workflow(
            "setup".to_owned(),
            0,
            "Set up | tools".to_owned(),
            None,
        );
        setup.conclusion = "success".to_owned();
        setup.started_at = Some(start);
        setup.finished_at = Some(start + chrono::Duration::milliseconds(1250));
        let mut test =
            crate::models::StepRecord::workflow("test".to_owned(), 1, "Run tests".to_owned(), None);
        test.conclusion = "failure".to_owned();
        test.started_at = setup.finished_at;
        test.finished_at = Some(start + chrono::Duration::milliseconds(3250));

        let summary = check_summary(
            "failure",
            &[setup, test],
            &["- exit code 1".to_owned()],
            &preloop_gha_protocol::JobId("test".to_owned()),
        );
        assert!(summary.contains("| Set up \\| tools | success | 1.2s |"));
        assert!(summary.contains("| Run tests | failure | 2.0s |"));
        assert!(summary.contains("**Failed step:** `Run tests`"));
        assert!(summary.contains("exit code 1"));
        assert!(summary.contains("job_id: `test`"));
    }

    #[tokio::test]
    async fn manifest_default_events_use_minimal_ci_defaults() {
        // Held for the whole test: `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` is
        // process-global and other tests build apps concurrently; the
        // fallback must be asserted with the override absent.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        unsafe { std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS") };
        let defaults = manifest_default_events();
        assert_eq!(defaults, vec!["push", "pull_request"]);
    }

    #[tokio::test]
    async fn manifest_default_events_override_is_used_when_set() {
        // Held for the whole test: `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` is
        // process-global and other tests build apps concurrently.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        unsafe { std::env::set_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS", "push, pull_request") };
        let events = manifest_default_events();
        assert_eq!(events, vec!["push".to_owned(), "pull_request".to_owned()]);
        unsafe { std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS") };

        // A blank override falls back to the minimal default.
        unsafe { std::env::set_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS", "  ") };
        assert_eq!(manifest_default_events(), vec!["push", "pull_request"]);
        unsafe { std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS") };
    }

    /// The signed push payload GitHub would deliver for `owner/repo`.
    fn signed_push_payload(after: &str) -> (Vec<u8>, String) {
        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": after,
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": after,
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }],
        });
        let bytes = serde_json::to_vec(&payload).unwrap();
        let signature = sign(&bytes, "super-secret");
        (bytes, signature)
    }

    /// In-process server fixture mirroring the crate's `WebhookDedupFixture`:
    /// a local workspace (with one push-triggered workflow by default), a
    /// webhook secret, and the signed push payload.
    struct WebhookFixture {
        state: AppState,
        app: axum::Router,
        workspace: std::path::PathBuf,
        payload_bytes: Vec<u8>,
        signature_header: String,
    }

    impl WebhookFixture {
        /// Standard fixture: the workspace holds one push-triggered workflow.
        async fn new(temp: &tempfile::TempDir) -> Self {
            let ws_dir = temp.path().join("ws");
            std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
            std::fs::write(
                ws_dir.join(".github/workflows/build.yml"),
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
            )
            .unwrap();
            Self::with_workspace(temp, ws_dir).await
        }

        async fn with_workspace(temp: &tempfile::TempDir, ws_dir: std::path::PathBuf) -> Self {
            let event_sha = if ws_dir.join(".github/workflows").is_dir() {
                commit_workspace(&ws_dir)
            } else {
                "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned()
            };
            let mut state = AppState::new(temp.path().join("state").to_path_buf())
                .await
                .unwrap();
            state.webhook_secret = Some("super-secret".to_owned());
            state.local_workspace = Some(ws_dir.clone());
            let app =
                crate::app_with_test_api(state.clone(), CancellationToken::new(), "test-token");
            let (payload_bytes, signature_header) = signed_push_payload(&event_sha);
            Self {
                state,
                app,
                workspace: ws_dir,
                payload_bytes,
                signature_header,
            }
        }

        fn refresh_payload_from_head(&mut self) {
            let event_sha = git_output(&self.workspace, &["rev-parse", "HEAD"]);
            let (payload_bytes, signature_header) = signed_push_payload(&event_sha);
            self.payload_bytes = payload_bytes;
            self.signature_header = signature_header;
        }

        /// Deliver the signed standard payload under `delivery`.
        async fn post(&self, delivery: &str, event: Option<&str>) -> StatusCode {
            self.post_body(delivery, event, &self.payload_bytes).await
        }

        /// Deliver an arbitrary signed payload under `delivery`.
        async fn post_body(
            &self,
            delivery: &str,
            event: Option<&str>,
            payload: &[u8],
        ) -> StatusCode {
            let app = self.app.clone();
            let signature = sign(payload, "super-secret");
            let mut request = Request::builder()
                .method(Method::POST)
                .uri("/api/v1/github/webhooks")
                .header("x-github-delivery", delivery)
                .header("x-hub-signature-256", signature)
                .header("content-type", "application/json");
            if let Some(event) = event {
                request = request.header("x-github-event", event);
            }
            app.oneshot(request.body(Body::from(payload.to_vec())).unwrap())
                .await
                .unwrap()
                .status()
        }
        async fn drain(&self) -> usize {
            let shared = Arc::new(SharedState {
                state: self.state.clone(),
                shutdown: CancellationToken::new(),
            });
            drain_webhook_queue(&shared).await.unwrap()
        }

        /// Replace the retry ladder for this fixture.
        ///
        /// The real ladder opens at one second, so a test that exercises a
        /// retry would otherwise have to sleep through it. The delay still has
        /// to be non-zero: a zero-delay retry is immediately claimable, and
        /// one drain pass would burn the whole attempt budget in a hot loop.
        fn with_retry_backoff(mut self, ladder: Vec<Duration>) -> Self {
            self.state.webhook_retry_backoff = ladder;
            self
        }
    }

    /// A delivery whose workflow inventory cannot be fetched fails internally
    /// and is retried with backoff rather than dropped.
    #[tokio::test]
    async fn webhook_workflow_fetch_failure_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone())
            .await
            // One 40ms tier: the retry is still a real scheduled retry (the row
            // is not claimable while its backoff is pending), but the test does
            // not sleep through the production ladder.
            .with_retry_backoff(vec![Duration::from_millis(40)]);

        // Hide workspace temporarily so workflow fetching fails transiently.
        let hidden_ws = temp.path().join("hidden_ws");
        std::fs::rename(&ws_dir, &hidden_ws).unwrap();

        let status = fixture.post("delivery-fetch-fail", Some("push")).await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "durable enqueue must be acknowledged with 202"
        );
        fixture.drain().await;
        {
            let record = fixture
                .state
                .backend
                .get_webhook_delivery("delivery-fetch-fail")
                .await
                .unwrap()
                .expect("delivery row must exist");
            assert_eq!(
                record.state,
                WebhookDeliveryStatus::Received,
                "transient fetch failure must keep the row claimable for internal retry"
            );
            assert_eq!(record.attempts, 1);
            assert!(record.last_error.is_some());
            let inner = fixture.state.test_tx().await;
            assert!(
                inner.runs.is_empty(),
                "a failed delivery must not create a run"
            );
        }

        // Restore the workspace: internal retry drains the queue and creates the run.
        std::fs::rename(&hidden_ws, &ws_dir).unwrap();
        // Past the injected tier, so the retry is due rather than early.
        tokio::time::sleep(Duration::from_millis(80)).await;
        fixture.drain().await;
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-fetch-fail")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(record.state, WebhookDeliveryStatus::Done);
        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "internal retry must successfully create the run"
        );
    }

    /// Trip the shared breaker the way three consecutive 5xx responses would.
    fn open_breaker(state: &AppState) {
        for _ in 0..3 {
            state.github_breaker.record_failure(
                &crate::github_breaker::GithubFailureKind::Unavailable,
                "GitHub responded 503",
            );
        }
        assert!(state.github_breaker.is_open(), "breaker must be open");
    }

    /// A known GitHub outage must stop the queue from claiming: every claim
    /// charges an attempt and would fail on the same dependency.
    #[tokio::test]
    async fn webhook_queue_stops_claiming_while_github_is_unavailable() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        assert_eq!(
            fixture.post("delivery-outage-gate", Some("push")).await,
            StatusCode::ACCEPTED
        );
        open_breaker(&fixture.state);

        assert_eq!(
            fixture.drain().await,
            0,
            "no delivery may be claimed while GitHub is circuit-broken"
        );

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-outage-gate")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(record.state, WebhookDeliveryStatus::Received);
        assert_eq!(
            record.attempts, 0,
            "an outage must not spend the delivery's retry budget"
        );
    }

    /// A delivery that fails because GitHub is down is parked with its
    /// attempt refunded — never dead-lettered, even past the attempt cap.
    #[tokio::test]
    async fn github_outage_parks_a_delivery_instead_of_dead_lettering_it() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        // Make workflow evaluation fail the way an unreachable dependency
        // would, and start from an attempt count already past the cap.
        let hidden_ws = temp.path().join("hidden_ws");
        std::fs::rename(&ws_dir, &hidden_ws).unwrap();
        fixture
            .state
            .backend
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: "delivery-outage-park".to_owned(),
                event: "push".to_owned(),
                payload: fixture.payload_bytes.clone(),
                received_at_us: crate::store::now_us(),
                state: WebhookDeliveryStatus::Received,
                attempts: WEBHOOK_MAX_ATTEMPTS,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            })
            .await
            .unwrap();
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = shared
            .state
            .backend
            .claim_webhook_deliveries(1, WEBHOOK_LEASE_DURATION_SECS)
            .await
            .unwrap();
        let delivery = claimed.into_iter().next().expect("claimed the delivery");
        assert_eq!(delivery.attempts, WEBHOOK_MAX_ATTEMPTS + 1);

        open_breaker(&fixture.state);
        process_one_delivery(&shared, &delivery).await;

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-outage-park")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "a dependency outage must never dead-letter a delivery"
        );
        assert_eq!(
            record.attempts, WEBHOOK_MAX_ATTEMPTS,
            "parking refunds the attempt the claim charged"
        );
        assert!(
            record
                .lease_until_us
                .is_some_and(|lease_until| lease_until > crate::store::now_us())
        );
    }

    /// A delivery whose ref SHA cannot be resolved is retried internally rather than dropped.
    #[tokio::test]
    async fn webhook_unresolvable_sha_is_redelivered() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        let payload = serde_json::json!({
            "ref": "refs/heads/main",
            "before": "0000000000000000000000000000000000000000",
            "after": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "commits": [{
                "id": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                "added": ["src/main.rs"],
                "modified": [],
                "removed": []
            }],
        });
        let bytes = serde_json::to_vec(&payload).unwrap();

        let status = fixture
            .post_body("delivery-no-sha", Some("push"), &bytes)
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        fixture.drain().await;

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-no-sha")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "transient unresolvable SHA must keep the row claimable for retry"
        );
        assert_eq!(record.attempts, 1);
        assert!(record.last_error.is_some());
        let inner = fixture.state.test_tx().await;
        assert!(
            inner.runs.is_empty(),
            "a failed delivery must not create a run"
        );
    }
    /// GitHub's workflow_dispatch webhook names the one selected workflow.
    /// Treating it as a repository-wide broadcast multiplies every manual
    /// dispatch across all workflows that declare the trigger.
    #[tokio::test]
    async fn workflow_dispatch_webhook_submits_only_the_named_workflow() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        for name in ["first.yml", "selected.yml"] {
            std::fs::write(
                ws_dir.join(".github/workflows").join(name),
                "on: workflow_dispatch\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo selected\n",
            )
            .unwrap();
        }
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = serde_json::to_vec(&serde_json::json!({
            "ref": "main",
            "workflow": ".github/workflows/selected.yml",
            "inputs": {},
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "sender": {"login": "octocat"},
        }))
        .unwrap();

        assert_eq!(
            fixture
                .post_body(
                    "delivery-targeted-dispatch",
                    Some("workflow_dispatch"),
                    &payload,
                )
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let runs = fixture.state.test_tx().await.runs;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs.values().next().unwrap().workflow_path_str,
            ".github/workflows/selected.yml"
        );
    }

    /// GitHub stringifies workflow_dispatch inputs in the webhook payload.
    /// The typed `inputs` context must still expose a boolean so `"false"`
    /// does not run a truthy-gated job.
    #[tokio::test]
    async fn workflow_dispatch_webhook_coerces_boolean_inputs_before_job_gates() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/dispatch.yml"),
            r#"
on:
  workflow_dispatch:
    inputs:
      reuse:
        type: boolean
        default: false
jobs:
  reuse:
    if: inputs.reuse
    runs-on: ubuntu-latest
    steps:
      - run: echo reuse
  build:
    if: ${{ !inputs.reuse }}
    runs-on: ubuntu-latest
    steps:
      - run: echo build
"#,
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = serde_json::to_vec(&serde_json::json!({
            "ref": "main",
            "workflow": ".github/workflows/dispatch.yml",
            "inputs": {"reuse": "false"},
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "sender": {"login": "octocat"},
        }))
        .unwrap();

        assert_eq!(
            fixture
                .post_body(
                    "delivery-stringified-boolean",
                    Some("workflow_dispatch"),
                    &payload,
                )
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        let run = inner.runs.values().next().unwrap();
        assert_eq!(
            run.submission.dispatch_inputs.get("reuse"),
            Some(&serde_json::json!(false))
        );
        assert_eq!(
            run.jobs.get(&JobId("reuse".to_owned())),
            Some(&ExecutionStatus::Skipped)
        );
        assert_eq!(
            run.jobs.get(&JobId("build".to_owned())),
            Some(&ExecutionStatus::Queued)
        );
    }

    /// A malformed workflow is reported permanently without preventing valid
    /// workflows from the same webhook commit from being submitted.
    #[tokio::test]
    async fn malformed_workflow_does_not_abort_other_workflows() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/z-valid.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo valid\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-malformed", Some("push")).await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-malformed")
            .await
            .unwrap()
            .expect("delivery row exists");
        assert_eq!(record.state, WebhookDeliveryStatus::Failed);
        assert!(
            record
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("a-malformed.yml")),
            "permanent delivery error must identify malformed workflow"
        );
        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "valid workflow must still be submitted after malformed workflow"
        );
        assert_eq!(
            inner.runs.values().next().unwrap().workflow_path_str,
            ".github/workflows/z-valid.yml"
        );
    }

    #[tokio::test]
    async fn malformed_pull_request_workflow_reports_head_sha() {
        // See `malformed_pull_request_workflow_failure_is_deduplicated`: the
        // PR-files lookup must not hit another test's GitHub stub.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let base_sha = git_output(&fixture.workspace, &["rev-parse", "HEAD"]);
        let payload = serde_json::json!({
            "action": "opened",
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "pull_request": {
                "base": { "ref": "main", "sha": base_sha },
                "head": { "ref": "feature", "sha": "head-sha-456" }
            }
        });
        let payload = serde_json::to_vec(&payload).unwrap();
        assert_eq!(
            fixture
                .post_body(
                    "delivery-pr-failure-sha",
                    Some("pull_request_target"),
                    &payload
                )
                .await,
            StatusCode::ACCEPTED
        );

        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let outcome = process_delivery_payload(&shared, &claimed[0]).await;
        match outcome {
            WebhookOutcome::PermanentErrors { failures, .. } => {
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].sha.as_deref(), Some("head-sha-456"));
            }
            other => panic!("malformed pull request workflow must be permanent: {other:?}"),
        }
    }

    #[tokio::test]
    async fn malformed_pull_request_workflow_failure_is_deduplicated() {
        // `PRELOOP_GITHUB_API_URL` is process-global; this scenario expects no
        // stub to answer the PR-files lookup, so it must not race a test that
        // installs one.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/a-malformed.yml"),
            "on: [push\njobs:\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let commit_sha = git_output(&fixture.workspace, &["rev-parse", "HEAD"]);
        let payload = serde_json::json!({
            "action": "opened",
            "number": 7,
            "repository": {
                "full_name": "owner/repo",
                "default_branch": "main"
            },
            "pull_request": {
                "number": 7,
                "base": { "ref": "main", "sha": commit_sha },
                "head": {
                    "ref": "feature",
                    "sha": "head-sha-456",
                    "repo": { "fork": false }
                },
                "merge_commit_sha": commit_sha
            }
        });
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-pr-duplicate-failure".to_owned(),
            event: "pull_request".to_owned(),
            payload: serde_json::to_vec(&payload).unwrap(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Processing,
            attempts: 1,
            lease_until_us: Some(crate::store::now_us() + 60_000_000),
            lease_token: Some("test-lease".to_owned()),
            last_error: None,
        };

        match process_delivery_payload(&shared, &delivery).await {
            WebhookOutcome::PermanentErrors { failures, .. } => {
                assert_eq!(
                    failures.len(),
                    1,
                    "pull_request target and pull_request projections share one failure"
                );
                assert_eq!(failures[0].sha.as_deref(), Some("head-sha-456"));
            }
            other => panic!("malformed pull request workflow must be permanent: {other:?}"),
        }
    }

    /// Regression guard: a workflow that simply is not triggered by the event
    /// is a *completed* delivery, not a failure.
    #[tokio::test]
    async fn webhook_untriggered_workflow_still_completes_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-no-match", Some("push")).await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-no-match")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "a delivery that triggered no workflow is still completed successfully"
        );
        let inner = fixture.state.test_tx().await;
        assert!(inner.runs.is_empty());
    }

    /// A deployment can leave release and artifact workflows to GitHub while
    /// Preloop owns the ordinary CI workflows in the same repository.
    #[tokio::test]
    async fn webhook_skips_github_owned_workflows() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        unsafe { std::env::set_var(GITHUB_OWNED_WORKFLOWS_ENV, "release.yml") };

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/release.yml"),
            "on: push\njobs:\n  release:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo release\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-github-owned", Some("push")).await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-github-owned")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "skipped github-owned workflow completes delivery"
        );
        let inner = fixture.state.test_tx().await;
        assert!(inner.runs.is_empty());

        unsafe { std::env::remove_var(GITHUB_OWNED_WORKFLOWS_ENV) };
    }

    /// Crash recovery: processing that died mid-flight has its expired lease
    /// recovered on boot and drains to completion.
    #[tokio::test]
    async fn webhook_cancelled_processing_releases_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        let record = WebhookDeliveryRecord {
            delivery_id: "delivery-cancel".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us() - 100_000_000,
            state: WebhookDeliveryStatus::Processing,
            attempts: 1,
            lease_until_us: Some(crate::store::now_us() - 1_000_000), // expired lease
            lease_token: Some("expired-token".to_owned()),
            last_error: None,
        };
        fixture
            .state
            .backend
            .enqueue_webhook_delivery(&record)
            .await
            .unwrap();

        let recovered = fixture
            .state
            .backend
            .recover_webhook_deliveries()
            .await
            .unwrap();
        assert_eq!(recovered, 1, "expired processing lease must be recovered");

        let rec = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-cancel")
            .await
            .unwrap()
            .expect("delivery row exists");
        assert_eq!(rec.state, WebhookDeliveryStatus::Received);

        fixture.drain().await;
        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "recovered delivery must be processed to create run"
        );
    }
    #[tokio::test]
    async fn webhook_replay_reuses_existing_run() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;

        assert_eq!(
            fixture.post("delivery-replay", Some("push")).await,
            StatusCode::ACCEPTED
        );
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let outcome = process_delivery_payload(&shared, &claimed[0]).await;
        assert!(matches!(outcome, WebhookOutcome::Success));
        let (original_run_id, original_check_run_ids) = {
            let inner = fixture.state.test_tx().await;
            assert_eq!(inner.runs.len(), 1);
            let run = inner.runs.values().next().unwrap();
            assert_eq!(run.webhook_delivery_id.as_deref(), Some("delivery-replay"));
            (run.run_id, run.job_check_run_ids.clone())
        };

        // Simulate a worker crash after run creation but before marking the
        // delivery done. The replay must reuse that persisted run.
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(
            fixture
                .state
                .backend
                .fail_webhook_delivery(
                    "delivery-replay",
                    lease_token,
                    "simulated crash",
                    false,
                    Some(Duration::ZERO),
                )
                .await
                .unwrap()
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "replaying one delivery must not create a second run"
        );
        let run = inner
            .runs
            .get(&original_run_id)
            .expect("original run survives replay");
        assert_eq!(
            run.job_check_run_ids, original_check_run_ids,
            "replaying a persisted run must reuse its GitHub check-run mapping"
        );
    }
    #[tokio::test]
    async fn failed_webhook_delivery_can_be_reopened_for_redelivery() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-redelivery".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        assert!(
            fixture
                .state
                .backend
                .enqueue_webhook_delivery(&delivery)
                .await
                .unwrap()
        );
        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(
            fixture
                .state
                .backend
                .fail_webhook_delivery(
                    &delivery.delivery_id,
                    lease_token,
                    "permanent test failure",
                    true,
                    None,
                )
                .await
                .unwrap()
        );

        let failed = fixture
            .state
            .backend
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .expect("failed delivery row exists");
        assert_eq!(failed.state, WebhookDeliveryStatus::Failed);

        let mut redelivery = delivery.clone();
        redelivery.received_at_us = crate::store::now_us();
        assert!(
            fixture
                .state
                .backend
                .enqueue_webhook_delivery(&redelivery)
                .await
                .unwrap(),
            "GitHub redelivery must reopen a retained failed row"
        );
        let reopened = fixture
            .state
            .backend
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .expect("reopened delivery row exists");
        assert_eq!(reopened.state, WebhookDeliveryStatus::Received);
        assert_eq!(reopened.attempts, 0);
        assert!(reopened.lease_token.is_none());
        assert!(reopened.last_error.is_none());
        assert!(
            !fixture
                .state
                .backend
                .enqueue_webhook_delivery(&redelivery)
                .await
                .unwrap(),
            "an active redelivery remains deduplicated"
        );
    }

    #[tokio::test]
    async fn corrupt_webhook_payload_is_dead_lettered_without_wedging_queue() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let corrupt = WebhookDeliveryRecord {
            delivery_id: "delivery-corrupt".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us() - 1,
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        let valid = WebhookDeliveryRecord {
            delivery_id: "delivery-after-corrupt".to_owned(),
            received_at_us: crate::store::now_us(),
            ..corrupt.clone()
        };
        assert!(
            fixture
                .state
                .backend
                .enqueue_webhook_delivery(&corrupt)
                .await
                .unwrap()
        );
        assert!(
            fixture
                .state
                .backend
                .enqueue_webhook_delivery(&valid)
                .await
                .unwrap()
        );

        let db_path = temp.path().join("state").join("preloop.db");
        let connection = rusqlite::Connection::open(db_path).unwrap();
        connection
            .execute(
                "UPDATE webhook_deliveries SET payload = ?1 WHERE delivery_id = ?2",
                rusqlite::params![vec![0_u8, 1, 2], corrupt.delivery_id],
            )
            .unwrap();
        drop(connection);

        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "corrupt payload is dead-lettered instead of returned to the worker"
        );
        assert_eq!(
            fixture
                .state
                .backend
                .count_dead_letter_webhook_deliveries()
                .await
                .unwrap(),
            1
        );

        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "a corrupt FIFO row must not wedge later valid deliveries"
        );
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(
            fixture
                .state
                .backend
                .complete_webhook_delivery(&valid.delivery_id, lease_token)
                .await
                .unwrap()
        );
    }
    #[tokio::test]
    async fn webhook_processing_lease_can_be_renewed() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-lease".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        fixture
            .state
            .backend
            .enqueue_webhook_delivery(&delivery)
            .await
            .unwrap();
        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 1)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        assert!(
            !fixture
                .state
                .backend
                .renew_webhook_delivery("delivery-lease", "stale-token", 60)
                .await
                .unwrap(),
            "a stale worker must not renew a reclaimed lease"
        );
        assert!(
            fixture
                .state
                .backend
                .renew_webhook_delivery("delivery-lease", lease_token, 60)
                .await
                .unwrap()
        );
        assert!(
            !fixture
                .state
                .backend
                .complete_webhook_delivery("delivery-lease", "stale-token")
                .await
                .unwrap(),
            "a stale worker must not finalize a reclaimed lease"
        );
        let renewed = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-lease")
            .await
            .unwrap()
            .unwrap();
        assert!(
            renewed.lease_until_us.unwrap() > crate::store::now_us() + 50_000_000,
            "renewal must move the lease beyond the original one-second claim"
        );
    }
    #[tokio::test]
    async fn expired_webhook_lease_rejects_all_mutations() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let delivery = WebhookDeliveryRecord {
            delivery_id: "delivery-expired-lease".to_owned(),
            event: "push".to_owned(),
            payload: fixture.payload_bytes.clone(),
            received_at_us: crate::store::now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        fixture
            .state
            .backend
            .enqueue_webhook_delivery(&delivery)
            .await
            .unwrap();
        let claimed = fixture
            .state
            .backend
            .claim_webhook_deliveries(1, 0)
            .await
            .unwrap();
        let lease_token = claimed[0].lease_token.as_deref().unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;

        assert!(
            !fixture
                .state
                .backend
                .renew_webhook_delivery(&delivery.delivery_id, lease_token, 60)
                .await
                .unwrap(),
            "an expired lease must not be renewed by its old owner"
        );
        assert!(
            !fixture
                .state
                .backend
                .complete_webhook_delivery(&delivery.delivery_id, lease_token)
                .await
                .unwrap(),
            "an expired lease must not be completed by its old owner"
        );
        assert!(
            !fixture
                .state
                .backend
                .fail_webhook_delivery(
                    &delivery.delivery_id,
                    lease_token,
                    "stale failure",
                    false,
                    Some(Duration::ZERO),
                )
                .await
                .unwrap(),
            "an expired lease must not be failed by its old owner"
        );
        let retained = fixture
            .state
            .backend
            .get_webhook_delivery(&delivery.delivery_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retained.state, WebhookDeliveryStatus::Processing);
        assert_eq!(retained.lease_token.as_deref(), Some(lease_token));
    }

    #[tokio::test]
    async fn webhook_terminal_rows_are_pruned_after_retention() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let now = crate::store::now_us();
        for (delivery_id, received_at_us) in
            [("delivery-old", now - 2_000_000), ("delivery-fresh", now)]
        {
            fixture
                .state
                .backend
                .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                    delivery_id: delivery_id.to_owned(),
                    event: "push".to_owned(),
                    payload: fixture.payload_bytes.clone(),
                    received_at_us,
                    state: WebhookDeliveryStatus::Done,
                    attempts: 1,
                    lease_until_us: None,
                    lease_token: None,
                    last_error: None,
                })
                .await
                .unwrap();
        }

        let pruned = fixture
            .state
            .backend
            .prune_webhook_deliveries(now - 1_000_000, 10)
            .await
            .unwrap();
        assert_eq!(pruned, 1);
        assert!(
            fixture
                .state
                .backend
                .get_webhook_delivery("delivery-old")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .state
                .backend
                .get_webhook_delivery("delivery-fresh")
                .await
                .unwrap()
                .is_some()
        );
    }

    /// A check_run rerequest is a new trigger by the webhook sender, so both
    /// the original run's actor and the sender must pass actor rules. Here
    /// the original actor (alice) is clean but the sender (mallory) is
    /// denied: the rerequest must not resubmit.
    async fn rerequest_fixture() -> (tempfile::TempDir, std::sync::Arc<crate::SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.execution_protection = crate::config::ExecutionProtectionConfig {
            mode: crate::config::ProtectionMode::Enforce,
            event_rules: vec![],
            actor_rules: vec![crate::config::ActorRule {
                actor: "mallory".to_owned(),
                workflows: None,
                action: crate::config::PolicyRuleAction::Deny,
            }],
        };
        let shared = state.shared();
        // Seed a terminal run owned by alice with a known check run id.
        let submission = WorkflowSubmission {
            workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(".github/workflows/ci.yml".to_owned()),
            workflow_file: Some("ci.yml".to_owned()),
            actor: "alice".to_owned(),
            sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
            resolved_sha: Some("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned()),
            ..Default::default()
        };
        let accepted = crate::submit_run_inner(&shared, submission).await.unwrap();
        // Force the run terminal and plant the known check-run mapping. Both
        // go through the control database now — there is no in-memory mirror.
        shared
            .state
            .test_db_mutate(|db| db.set_run_status(accepted.run_id, "completed", Some("success")))
            .await
            .unwrap();
        shared
            .state
            .backend
            .set_job_check_run(accepted.run_id, &JobId("build".to_owned()), 12345)
            .await
            .unwrap();
        // Keep the TempDir alive for the state's lifetime.
        (temp, shared)
    }

    fn rerequest_payload(sender: &str) -> serde_json::Value {
        serde_json::json!({
            "action": "rerequested",
            "check_run": {
                "id": 12345,
                "name": "build",
                "head_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            },
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": sender},
        })
    }

    #[tokio::test]
    async fn check_run_rerequest_denies_blocked_sender() {
        let (_temp, shared) = rerequest_fixture().await;
        let (status, body) = process_check_run_rerequest(&shared, &rerequest_payload("mallory"))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0, serde_json::json!([]));
        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 1, "blocked sender must not resubmit the run");
    }

    #[tokio::test]
    async fn check_run_rerequest_allows_clean_sender() {
        let (_temp, shared) = rerequest_fixture().await;
        let (status, body) = process_check_run_rerequest(&shared, &rerequest_payload("alice"))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_ne!(body.0, serde_json::json!([]), "clean sender must resubmit");
        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 2, "clean sender's rerequest must create a run");
    }

    /// Seed one plain run, reported or not. `submit_run_inner` never mints
    /// check-run ids itself — intake loops do — so this yields a run with an
    /// empty `job_check_run_ids`, the shape a not-yet-materialized job has.
    async fn mint_fixture(
        reports_check_runs: bool,
        status_check_sha: Option<&str>,
    ) -> (tempfile::TempDir, std::sync::Arc<crate::SharedState>, RunId) {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();
        let submission = WorkflowSubmission {
            workflow_yaml:
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                    .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(".github/workflows/ci.yml".to_owned()),
            sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
            status_check_sha: status_check_sha.map(str::to_owned),
            resolved_sha: Some("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned()),
            ..Default::default()
        };
        let accepted = crate::submit_run_inner(&shared, submission).await.unwrap();
        shared
            .state
            .backend
            .set_reports_check_runs(accepted.run_id, reports_check_runs)
            .await
            .unwrap();
        (temp, shared, accepted.run_id)
    }

    #[tokio::test]
    async fn materialized_job_mints_check_run_when_intake_reported() {
        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job = JobId("build".to_owned());

        // Handlers never mint inline any more: there is no GitHub call and no
        // persisted id before the sender runs.
        assert_eq!(
            ensure_check_run_mapped(&shared, run_id, &job).await,
            None,
            "a request handler must not mint a check run"
        );

        // The durable queued report stamps the run and appends a projection
        // wake; the projector materializes the desired row, and the sender
        // (exercised by the sender tests) creates the GitHub check from it.
        report_check_run_queued(
            &shared,
            "owner/repo",
            "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            &job,
            run_id,
        )
        .await
        .unwrap();
        let row = wait_for_check_run_row(&shared, run_id, &job).await;
        assert_eq!(row.payload["status"], "queued");
        assert_eq!(row.payload["name"], "build");
        assert_eq!(row.payload["repository"], "owner/repo");
        assert_eq!(
            row.payload["sha"], "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            "the projected row carries the run's check sha"
        );
        let runs = shared.state.test_tx().await.runs;
        assert!(
            runs.get(&run_id).unwrap().reports_check_runs,
            "the queued report must stamp the run so later jobs report too"
        );
    }

    #[tokio::test]
    async fn materialized_job_gets_no_check_run_when_intake_never_reported() {
        let (_temp, shared, run_id) = mint_fixture(false, None).await;

        let leg = JobId("build (linux)".to_owned());
        assert_eq!(
            ensure_check_run_mapped(&shared, run_id, &leg).await,
            None,
            "plain local runs mint no check runs, matching intake"
        );
        let runs = shared.state.test_tx().await.runs;
        assert!(runs.get(&run_id).unwrap().job_check_run_ids.is_empty());
    }

    #[tokio::test]
    async fn terminal_report_mints_missing_check_run_for_late_job() {
        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job = JobId("build".to_owned());

        // A matrix leg that skips at promotion never dispatches; the only
        // report it ever gets is the completion one. It must reach the queue
        // even though the job settled before the reporting flag landed (a
        // `JobStatus` event would be stamped stale; the projection wake is
        // not).
        shared
            .state
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE jobs SET status = 'skipped' WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run_id.to_string(), "build"],
                )
            })
            .await
            .unwrap();
        report_check_run_completed(&shared, run_id, &job, ExecutionStatus::Skipped).await;

        let row = wait_for_check_run_row(&shared, run_id, &job).await;
        assert_eq!(
            row.payload["status"], "skipped",
            "a terminal report must queue its final state for the sender"
        );
    }

    /// Wait for the durable projector (which owns the consumer lease in the
    /// background) to materialize a queue row for `run_id`/`job_id`.
    async fn wait_for_check_run_row(
        shared: &std::sync::Arc<crate::SharedState>,
        run_id: RunId,
        job_id: &JobId,
    ) -> crate::control::types::CheckRunUpdate {
        for _ in 0..120 {
            if let Ok(rows) = shared
                .state
                .backend
                .lease_check_run_updates("test-probe", std::time::Duration::from_millis(1), 100)
                .await
                && let Some(row) = rows
                    .into_iter()
                    .find(|row| row.run_id == run_id && row.job_id == *job_id)
            {
                return row;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("check-run row for {run_id}/{job_id} was never projected");
    }

    // ── Sender acceptance tests ────────────────────────────────────────

    type StubRequests = std::sync::Arc<parking_lot::Mutex<Vec<(String, String, Value)>>>;

    /// Fake GitHub API: records every request and answers from
    /// `handler(method, path, body)`. Returns the base URL to install as
    /// `PRELOOP_GITHUB_API_URL`.
    async fn start_github_stub(
        handler: impl Fn(&str, &str, &Value) -> (u16, Vec<(&'static str, String)>, Value)
        + Send
        + Sync
        + 'static,
    ) -> (String, StubRequests) {
        let requests: StubRequests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let handler = std::sync::Arc::new(handler);
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let recorded = recorded.clone();
            let handler = handler.clone();
            async move {
                let method = request.method().to_string();
                let path = request.uri().path().to_owned();
                let bytes = axum::body::to_bytes(request.into_body(), 1 << 20)
                    .await
                    .unwrap_or_default();
                let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                recorded
                    .lock()
                    .push((method.clone(), path.clone(), body.clone()));
                let (status, headers, response_body) = handler(&method, &path, &body);
                let mut response = axum::response::Response::new(axum::body::Body::from(
                    response_body.to_string(),
                ));
                *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
                for (name, value) in headers {
                    response.headers_mut().insert(
                        axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        axum::http::HeaderValue::from_str(&value).unwrap(),
                    );
                }
                response
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), requests)
    }

    fn check_run_writes(requests: &StubRequests) -> Vec<(String, String, Value)> {
        requests
            .lock()
            .iter()
            .filter(|(method, _, _)| method == "POST" || method == "PATCH")
            .cloned()
            .collect()
    }

    async fn seed_check_run_update(
        shared: &std::sync::Arc<crate::SharedState>,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        status: &str,
        check_run_id: Option<u64>,
    ) {
        shared
            .state
            .backend
            .enqueue_check_run_update(crate::control::types::CheckRunUpdateInput {
                run_id,
                job_id: job_id.clone(),
                installation_id: 0,
                check_run_id,
                version,
                payload: serde_json::json!({
                    "repository": "owner/repo",
                    "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                    "job_id": job_id.0,
                    "status": status,
                    "name": "build",
                }),
            })
            .await
            .unwrap();
    }

    async fn drain_sender(shared: &std::sync::Arc<crate::SharedState>) -> usize {
        CheckRunSender::new().drain_once(shared).await
    }

    /// (b) queued → in_progress → completed committed before the sender runs
    /// coalesces to ONE Checks API write carrying the final state.
    #[tokio::test]
    async fn sender_coalesces_versions_into_one_final_request() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api, requests) = start_github_stub(|method, _path, _body| match method {
            "GET" => (200, vec![], serde_json::json!({"check_runs": []})),
            _ => (201, vec![], serde_json::json!({"id": 4242})),
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");
        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job = JobId("build".to_owned());
        for (version, status) in [(1, "queued"), (2, "in_progress"), (3, "success")] {
            seed_check_run_update(&shared, run_id, &job, version, status, None).await;
        }

        assert_eq!(drain_sender(&shared).await, 1);
        let writes = check_run_writes(&requests);
        assert_eq!(writes.len(), 1, "coalesced to one write: {writes:?}");
        assert_eq!(writes[0].0, "POST");
        assert_eq!(writes[0].2["status"], "completed");
        assert_eq!(writes[0].2["conclusion"], "success");
        assert!(
            shared
                .state
                .backend
                .lease_check_run_updates("probe", std::time::Duration::from_millis(1), 10)
                .await
                .unwrap()
                .is_empty(),
            "the sent row is deleted"
        );
    }

    /// (d) a 429 with Retry-After parks the row for the advertised delay and
    /// the next attempt succeeds.
    #[tokio::test]
    async fn sender_retries_after_retry_after_then_succeeds() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_for_stub = attempts.clone();
        let (api, requests) = start_github_stub(move |method, _path, _body| {
            if method == "GET" {
                return (200, vec![], serde_json::json!({"check_runs": []}));
            }
            if attempts_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                (
                    429,
                    vec![("retry-after", "1".to_owned())],
                    serde_json::json!({"message": "You have exceeded a secondary rate limit"}),
                )
            } else {
                (201, vec![], serde_json::json!({"id": 777}))
            }
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");
        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job = JobId("build".to_owned());
        seed_check_run_update(&shared, run_id, &job, 1, "queued", None).await;

        assert_eq!(drain_sender(&shared).await, 1);
        assert_eq!(
            check_run_writes(&requests).len(),
            1,
            "first attempt is throttled"
        );
        assert_eq!(
            drain_sender(&shared).await,
            0,
            "the row is not due until Retry-After elapses"
        );
        assert_eq!(check_run_writes(&requests).len(), 1);
        tokio::time::sleep(std::time::Duration::from_millis(1_150)).await;
        assert_eq!(drain_sender(&shared).await, 1);
        assert_eq!(
            check_run_writes(&requests).len(),
            2,
            "retried and succeeded"
        );
        assert!(
            shared
                .state
                .backend
                .lease_check_run_updates("probe", std::time::Duration::from_millis(1), 10)
                .await
                .unwrap()
                .is_empty(),
            "the successful retry deletes the row"
        );
    }

    /// (e) crash after POST before the id was saved: the next attempt finds
    /// the existing check run (or the persisted job mapping) and PATCHes it;
    /// it never POSTs a duplicate.
    #[tokio::test]
    async fn sender_reconciles_instead_of_second_post() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        // The row's own check is listed after a same-named check another
        // workflow created on the same commit; only the `external_id` match
        // may be adopted. The id is filled in once the fixture exists.
        let own_external_id = std::sync::Arc::new(parking_lot::Mutex::new(String::new()));
        let stub_external_id = own_external_id.clone();
        let (api, requests) = start_github_stub(move |method, path, _body| {
            if method == "GET" {
                let existing = if path.contains("commits/") {
                    serde_json::json!({"total_count": 2, "check_runs": [
                        {
                            "id": 6001,
                            "name": "build",
                            "external_id": "another-workflow-run:build",
                            "head_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                        },
                        {
                            "id": 7001,
                            "name": "build",
                            "external_id": *stub_external_id.lock(),
                            "head_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                        },
                    ]})
                } else {
                    serde_json::json!({})
                };
                return (200, vec![], existing);
            }
            (200, vec![], serde_json::json!({"id": 7001}))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");
        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job = JobId("build".to_owned());
        *own_external_id.lock() = format!("{run_id}:{}", job.0);

        // The check run already exists on GitHub but the row has no id.
        seed_check_run_update(&shared, run_id, &job, 1, "queued", None).await;
        assert_eq!(drain_sender(&shared).await, 1);
        let writes = check_run_writes(&requests);
        assert_eq!(writes.len(), 1, "one write only: {writes:?}");
        assert_eq!(writes[0].0, "PATCH");
        assert!(writes[0].1.ends_with("/check-runs/7001"));
        assert_eq!(
            shared
                .state
                .backend
                .job_check_run_id(run_id, &job)
                .await
                .unwrap(),
            Some(7001),
            "the reconciled id is persisted"
        );

        // A persisted job mapping alone (no GitHub lookup) also PATCHes.
        let (_temp2, shared2, run_id2) = mint_fixture(true, None).await;
        shared2
            .state
            .backend
            .set_job_check_run(run_id2, &job, 7002)
            .await
            .unwrap();
        seed_check_run_update(&shared2, run_id2, &job, 1, "queued", None).await;
        let before = requests.lock().len();
        assert_eq!(drain_sender(&shared2).await, 1);
        let writes: Vec<_> = requests
            .lock()
            .iter()
            .skip(before)
            .filter(|(method, _, _)| method == "POST" || method == "PATCH")
            .cloned()
            .collect();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, "PATCH");
        assert!(writes[0].1.ends_with("/check-runs/7002"));
        assert!(
            !requests
                .lock()
                .iter()
                .skip(before)
                .any(|(method, _, _)| method == "POST"),
            "no duplicate POST"
        );
    }

    /// (h) a throttled installation parks only its own rows: the other
    /// installation's check run succeeds in the same batch.
    #[tokio::test]
    async fn throttled_installation_does_not_block_another() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api, requests) = start_github_stub(|method, path, _body| {
            if path.contains("/access_tokens") {
                let token = if path.contains("/101/") {
                    "ghs-a"
                } else {
                    "ghs-b"
                };
                return (
                    201,
                    vec![],
                    serde_json::json!({"token": token, "expires_at": "2099-01-01T00:00:00Z"}),
                );
            }
            if method == "GET" && path.starts_with("/app/installations/") {
                let login = if path.ends_with("/101") {
                    "owner-a"
                } else {
                    "owner-b"
                };
                return (
                    200,
                    vec![],
                    serde_json::json!({"account": {"login": login}}),
                );
            }
            if method == "GET" {
                return (200, vec![], serde_json::json!({"check_runs": []}));
            }
            if path.starts_with("/repos/owner-a/") {
                let reset = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 60)
                    .to_string();
                return (
                    403,
                    vec![
                        ("x-ratelimit-remaining", "0".to_owned()),
                        ("x-ratelimit-reset", reset),
                    ],
                    serde_json::json!({"message": "API rate limit exceeded"}),
                );
            }
            (201, vec![], serde_json::json!({"id": 900}))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
        let mut app_a = crate::github_app::GitHubAppCredentials::for_tests(
            "424",
            key.clone(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        );
        app_a.installation_id = Some(101);
        let mut app_b = crate::github_app::GitHubAppCredentials::for_tests(
            "425",
            key,
            crate::github_app::MintFailurePolicy::LocalJwt,
        );
        app_b.installation_id = Some(202);
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![app_a.clone(), app_b],
            default_index: 0,
        });
        state.github_app = Some(app_a);
        let shared = state.shared();
        let job = JobId("build".to_owned());
        let mut run_ids = Vec::new();
        for repo in ["owner-a/repo", "owner-b/repo"] {
            let submission = WorkflowSubmission {
                workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n".to_owned(),
                event: "push".to_owned(),
                repository: repo.to_owned(),
                workflow_path: Some(".github/workflows/ci.yml".to_owned()),
                sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
                resolved_sha: Some("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned()),
                ..Default::default()
            };
            let accepted = crate::submit_run_inner(&shared, submission).await.unwrap();
            shared
                .state
                .backend
                .set_reports_check_runs(accepted.run_id, true)
                .await
                .unwrap();
            let payload = serde_json::json!({
                "repository": repo,
                "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
                "job_id": "build",
                "status": "queued",
                "name": "build",
            });
            shared
                .state
                .backend
                .enqueue_check_run_update(crate::control::types::CheckRunUpdateInput {
                    run_id: accepted.run_id,
                    job_id: job.clone(),
                    installation_id: 0,
                    check_run_id: None,
                    version: 1,
                    payload,
                })
                .await
                .unwrap();
            run_ids.push(accepted.run_id);
        }

        assert_eq!(drain_sender(&shared).await, 2, "both rows are leased");
        let writes: Vec<_> = check_run_writes(&requests)
            .into_iter()
            .filter(|(_, path, _)| path.contains("/check-runs"))
            .collect();
        assert_eq!(
            writes.len(),
            2,
            "one write per installation: all={:?}",
            check_run_writes(&requests)
        );
        let a_writes = writes
            .iter()
            .filter(|(_, path, _)| path.starts_with("/repos/owner-a/"))
            .count();
        let b_writes = writes
            .iter()
            .filter(|(_, path, _)| path.starts_with("/repos/owner-b/"))
            .count();
        assert_eq!((a_writes, b_writes), (1, 1));
        let writes_before = writes.len();
        assert_eq!(
            drain_sender(&shared).await,
            0,
            "the throttled installation's row is deferred, not retried immediately"
        );
        assert_eq!(
            check_run_writes(&requests)
                .iter()
                .filter(|(_, path, _)| path.contains("/check-runs"))
                .count(),
            writes_before
        );
        // B's row is gone; A's is parked until the reset (not due).
        assert!(
            shared
                .state
                .backend
                .lease_check_run_updates("probe", std::time::Duration::from_millis(1), 10)
                .await
                .unwrap()
                .is_empty(),
            "the successful installation's row is deleted and the throttled one is not due"
        );
    }

    /// (j) a run that vanished before the sender ran still delivers its final
    /// state from the payload; permanent errors drop the row.
    #[tokio::test]
    async fn sender_delivers_vanished_run_and_drops_permanent_failures() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let statuses = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<u16>::new()));
        let statuses_for_stub = statuses.clone();
        let (api, requests) = start_github_stub(move |method, _path, _body| {
            if method == "GET" {
                return (200, vec![], serde_json::json!({"check_runs": []}));
            }
            let status = statuses_for_stub.lock().pop().unwrap_or(201);
            (status, vec![], serde_json::json!({"id": 555}))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();
        let job = JobId("build".to_owned());

        // No `runs` row at all: an archived/deleted run, or the synthetic
        // workflow-failure shape.
        seed_check_run_update(&shared, RunId::new(), &job, 1, "success", None).await;
        assert_eq!(drain_sender(&shared).await, 1);
        let writes = check_run_writes(&requests);
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].2["status"], "completed");
        assert_eq!(writes[0].2["conclusion"], "success");

        // A permanent 422 drops the row after one attempt.
        statuses.lock().push(422);
        seed_check_run_update(&shared, RunId::new(), &job, 1, "queued", None).await;
        assert_eq!(drain_sender(&shared).await, 1);
        assert!(
            shared
                .state
                .backend
                .lease_check_run_updates("probe", std::time::Duration::from_millis(1), 10)
                .await
                .unwrap()
                .is_empty(),
            "a 422 must drop the row instead of retrying forever"
        );
    }

    /// (a) a webhook push fanning out to many jobs returns without any inline
    /// GitHub Checks API call; the durable queue carries the reports instead.
    #[tokio::test]
    async fn webhook_fanout_makes_zero_inline_check_calls() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api, requests) =
            start_github_stub(|_method, _path, _body| (200, vec![], serde_json::json!({}))).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        let mut workflow = "on: push\njobs:\n".to_owned();
        for index in 0..8 {
            workflow.push_str(&format!(
                "  job{index}:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo {index}\n"
            ));
        }
        std::fs::write(ws_dir.join(".github/workflows/build.yml"), workflow).unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        assert_eq!(
            fixture.post("delivery-check-fanout", Some("push")).await,
            StatusCode::ACCEPTED
        );
        assert!(
            requests
                .lock()
                .iter()
                .all(|(_, path, _)| !path.contains("check-runs")),
            "the webhook handler must not call the Checks API inline: {:?}",
            requests.lock()
        );
        fixture.drain().await;
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-check-fanout")
            .await
            .unwrap()
            .expect("delivery row");
        assert_eq!(record.state, WebhookDeliveryStatus::Done);
        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        assert_eq!(
            inner.runs.values().next().unwrap().jobs.len(),
            8,
            "all eight jobs fan out"
        );
        assert!(
            requests
                .lock()
                .iter()
                .all(|(_, path, _)| !path.contains("check-runs")),
            "delivery must not call the Checks API inline either"
        );
    }

    #[tokio::test]
    async fn terminal_check_retries_not_found_after_creation() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let creates = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mock_app = axum::Router::new()
            .route(
                "/repos/owner/repo/check-runs/:id",
                axum::routing::patch({
                    let attempts = attempts.clone();
                    move || {
                        let attempts = attempts.clone();
                        async move {
                            let attempt =
                                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            if attempt == 0 {
                                (
                                    StatusCode::NOT_FOUND,
                                    Json(serde_json::json!({"message": "Not Found"})),
                                )
                                    .into_response()
                            } else {
                                Json(serde_json::json!({"id": 7})).into_response()
                            }
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs",
                axum::routing::post({
                    let creates = creates.clone();
                    move || {
                        let creates = creates.clone();
                        async move {
                            creates.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Json(serde_json::json!({"id": 99}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(listener, mock_app).await.unwrap();
        });

        let (_temp, shared, run_id) = mint_fixture(true, None).await;
        let job_id = JobId("build".to_owned());
        shared
            .state
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE jobs SET status = 'skipped', check_run_id = ?3 \
                     WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run_id.0.to_string(), job_id.0, 7i64],
                )
            })
            .await
            .unwrap();
        let _api_url = crate::state::TestEnvVar::set(
            "PRELOOP_GITHUB_API_URL",
            format!("http://127.0.0.1:{port}"),
        );
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "check-retry-token");

        report_check_run_completed(&shared, run_id, &job_id, ExecutionStatus::Skipped).await;
        // The projector queues the update in the background; drain the
        // sender until the PATCH is retried past its first 404.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while attempts.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                drain_sender(&shared).await;
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the PATCH is retried after an immediate 404");

        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "an immediate 404 must be retried instead of stranding the check"
        );
        assert_eq!(
            creates.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "an immediate 404 must not abandon the check and POST a duplicate"
        );
        assert_eq!(
            shared
                .state
                .backend
                .job_check_run_id(run_id, &job_id)
                .await
                .unwrap(),
            Some(7),
            "the check-run mapping survives the replication-race 404"
        );
        server.abort();
    }

    /// GitHub sends `x-ratelimit-reset` on every response, so a failure with
    /// budget left keeps the short exponential backoff; only a spent budget
    /// waits for the reset.
    #[test]
    fn check_retry_delay_waits_for_reset_only_when_budget_is_spent() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1_800;
        let failure = |status: u16, remaining: &str| {
            anyhow::anyhow!(
                "GitHub Check API failed with status {status} Internal Server Error: {{}}; \
                 retry_after=; rate_remaining={remaining}; rate_reset={reset}"
            )
        };

        for (status, remaining) in [(500, "4321"), (404, "4999"), (422, "12"), (502, "")] {
            let delay = check_retry_delay(&failure(status, remaining), 0);
            assert!(
                delay < std::time::Duration::from_secs(1),
                "status {status} with remaining={remaining:?} must back off briefly, got {delay:?}"
            );
        }

        let delay = check_retry_delay(&failure(403, "0"), 0);
        assert!(
            delay > std::time::Duration::from_secs(1_700),
            "a spent budget waits for the advertised reset, got {delay:?}"
        );
    }

    /// `github.ref_protected` comes from the forge: branch protection (or a
    /// ruleset) answers `protected`, a branch GitHub does not know answers
    /// false, and non-branch refs are never looked up at all.
    #[tokio::test]
    async fn ref_protected_reads_branch_protection_from_the_forge() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        let seen = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let branch_stub = axum::routing::get({
            let seen = seen.clone();
            move |axum::extract::Path(branch): axum::extract::Path<String>| {
                let seen = seen.clone();
                async move {
                    seen.lock().push(branch.clone());
                    let protected = matches!(branch.as_str(), "main" | "release");
                    if protected || branch == "feature" {
                        axum::Json(serde_json::json!({"name": branch, "protected": protected}))
                            .into_response()
                    } else {
                        (
                            StatusCode::NOT_FOUND,
                            axum::Json(serde_json::json!({"message": "Branch not found"})),
                        )
                            .into_response()
                    }
                }
            }
        });
        // A server that answers about a *different* branch must never lend
        // that branch's protection to this run.
        let mismatch_stub = axum::routing::get(|| async {
            axum::Json(serde_json::json!({"name": "release", "protected": true}))
        });
        let stub = axum::Router::new()
            .route("/repos/owner/repo/branches/:branch", branch_stub)
            .route("/repos/owner/mismatch/branches/:branch", mismatch_stub);
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _api_url = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", api_base);
        let _token =
            crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "branch-protection-token");

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();

        assert!(resolve_ref_protected(&shared, "owner/repo", "refs/heads/main").await);
        assert!(!resolve_ref_protected(&shared, "owner/repo", "refs/heads/feature").await);
        assert!(resolve_ref_protected(&shared, "owner/repo", "refs/heads/release").await);
        // `#`, `%` and `/` are all legal in a branch name, so the lookup must
        // transmit the whole name as one encoded segment: sent raw, the
        // fragment cut `release#test` down to `release` and the lookup
        // answered with *that* branch's protection.
        assert!(
            !resolve_ref_protected(&shared, "owner/repo", "refs/heads/release#test").await,
            "release#test must not inherit release's protection"
        );
        assert!(
            !resolve_ref_protected(&shared, "owner/repo", "refs/heads/release/test").await,
            "release/test must not inherit release's protection"
        );
        // A 200 naming another branch is not an answer about the one asked
        // for: an exact name match is what makes the answer trustworthy.
        assert!(
            !resolve_ref_protected(&shared, "owner/mismatch", "refs/heads/release#test").await,
            "a lookup answered with a different branch name must stay unprivileged"
        );
        // Tags and pull-request refs cannot be protected; no lookup is made.
        assert!(!resolve_ref_protected(&shared, "owner/repo", "refs/tags/v1.0.0").await);
        assert!(!resolve_ref_protected(&shared, "owner/repo", "refs/pull/7/merge").await);
        assert_eq!(
            seen.lock().clone(),
            vec![
                "main".to_owned(),
                "feature".to_owned(),
                "release".to_owned(),
                "release#test".to_owned(),
                "release/test".to_owned(),
            ],
            "each lookup must carry the branch name verbatim, as one path segment"
        );
    }

    #[test]
    fn report_coords_prefer_status_check_sha() {
        // Late mints must land on the same commit as intake checks: for PR
        // runs the check sha is the head sha, not `submission.sha` (base).
        let mut submission = WorkflowSubmission {
            repository: "owner/repo".to_owned(),
            sha: "base".to_owned(),
            status_check_sha: Some("head".to_owned()),
            ..Default::default()
        };
        let run = crate::models::RunRecord {
            run_id: RunId::new(),
            webhook_delivery_id: None,
            run_name: None,
            submission: std::sync::Arc::new(submission.clone()),
            jobs: BTreeMap::new(),
            status: ExecutionStatus::InProgress,
            job_outputs: BTreeMap::new(),
            job_base_ids: BTreeMap::new(),
            job_needs: BTreeMap::new(),
            caller_plans: BTreeMap::new(),
            job_names: BTreeMap::new(),
            github: serde_json::Value::Null,
            head_sha: String::new(),
            workflow_ref: String::new(),
            workspace_snapshot: None,
            job_fail_fast: BTreeMap::new(),
            job_continue_on_error: BTreeMap::new(),
            job_check_run_ids: BTreeMap::new(),
            reports_check_runs: true,
            reusable_calls: BTreeMap::new(),
            jobs_list: Vec::new(),
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            run_number: 1,
            run_attempt: 1,
            workflow_path_str: String::new(),
            event: "pull_request".to_owned(),
            conclusion: None,
            push_state: None,
            snapshot_timing: None,
            fork_approval_pending: false,
            fork_approval_requested_at_unix_nanos: None,
            fork_approved_at_unix_nanos: None,
            fork_approval_note: None,
        };
        assert_eq!(
            check_run_report_coords(&run),
            Some(("owner/repo".to_owned(), "head".to_owned()))
        );
        // A synced push overrides: checks must attach to the published
        // commit, not the submission's local base sha.
        let mut pushed = run.clone();
        pushed.push_state = Some(crate::models::PushState {
            status: crate::models::PushStatus::Synced,
            error: None,
            pr_number: None,
            effective_sha: Some("published".to_owned()),
        });
        assert_eq!(
            check_run_report_coords(&pushed),
            Some(("owner/repo".to_owned(), "published".to_owned()))
        );
        // Unreported runs yield no coordinates.
        submission.status_check_sha = None;
        let mut quiet = run.clone();
        quiet.submission = std::sync::Arc::new(submission);
        quiet.reports_check_runs = false;
        assert_eq!(check_run_report_coords(&quiet), None);
    }

    /// GitHub does not deliver webhooks in order, and retries/watchdog
    /// redeliveries land late by design. A push for an OLDER commit processed
    /// after a newer one must not use `cancel-in-progress` to cancel the newer
    /// commit's run: the ref's newest head is what the group should keep.
    #[tokio::test]
    async fn late_older_push_does_not_cancel_newer_run() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\nconcurrency:\n  group: ci-${{ github.ref }}\n  cancel-in-progress: true\n\
             jobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        // The fixture commits the workspace: that is the older commit.
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        let older = git_output(&ws_dir, &["rev-parse", "HEAD"]);
        std::fs::write(ws_dir.join("change.txt"), "newer").unwrap();
        git_output(&ws_dir, &["add", "-A"]);
        git_output(&ws_dir, &["commit", "-qm", "newer"]);
        let newer = git_output(&ws_dir, &["rev-parse", "HEAD"]);

        // `repository.pushed_at` is GitHub's record of when each push happened.
        let push = |before: &str, after: &str, pushed_at: i64| {
            serde_json::to_vec(&serde_json::json!({
                "ref": "refs/heads/main",
                "before": before,
                "after": after,
                "repository": {
                    "full_name": "owner/repo",
                    "default_branch": "main",
                    "pushed_at": pushed_at
                },
                "commits": [{"id": after, "added": [], "modified": ["change.txt"], "removed": []}],
            }))
            .unwrap()
        };

        // Newer push arrives (and is processed) first…
        let status = fixture
            .post_body(
                "delivery-newer",
                Some("push"),
                &push(&older, &newer, 1_700_000_200),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        fixture.drain().await;
        // …then the older push shows up late.
        let status = fixture
            .post_body(
                "delivery-older",
                Some("push"),
                &push(
                    "0000000000000000000000000000000000000000",
                    &older,
                    1_700_000_100,
                ),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        let status_for = |sha: &str| {
            inner
                .runs
                .values()
                .find(|run| run.submission.sha == sha)
                .map(|run| run.status)
        };
        let newer_status = status_for(&newer).expect("the newer push must have a run");
        assert_ne!(
            newer_status,
            ExecutionStatus::Cancelled,
            "a late push for an older commit cancelled the newer commit's run"
        );
        assert_eq!(
            status_for(&older),
            Some(ExecutionStatus::Cancelled),
            "the stale push is the one superseded, as it would have been in GitHub's order"
        );
    }

    /// ChatOps shape: a per-PR `cancel-in-progress` group fed by comments.
    /// A comment delivered late must not cancel the run of a newer comment.
    #[tokio::test]
    async fn late_older_pr_comment_does_not_cancel_newer_run() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/chatops.yml"),
            "on: issue_comment\nconcurrency:\n  group: pr-${{ github.event.issue.number }}\n  \
             cancel-in-progress: true\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    \
             steps:\n      - run: echo deploy\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;

        let comment = |id: u64, created_at: &str| {
            serde_json::to_vec(&serde_json::json!({
                "action": "created",
                "issue": {"number": 42, "pull_request": {"url": "https://example.invalid/pr/42"}},
                "comment": {"id": id, "body": "/deploy", "created_at": created_at},
                "repository": {"full_name": "owner/repo", "default_branch": "main"},
                "sender": {"login": "octocat"},
            }))
            .unwrap()
        };

        // The newer comment is processed first, the older one arrives late.
        for (delivery, id, created_at) in [
            ("delivery-comment-newer", 2, "2026-01-01T00:05:00Z"),
            ("delivery-comment-older", 1, "2026-01-01T00:00:00Z"),
        ] {
            let status = fixture
                .post_body(delivery, Some("issue_comment"), &comment(id, created_at))
                .await;
            assert_eq!(status, StatusCode::ACCEPTED);
            fixture.drain().await;
        }

        let inner = fixture.state.test_tx().await;
        let status_for = |id: u64| {
            inner
                .runs
                .values()
                .find(|run| run.submission.payload["comment"]["id"] == id)
                .map(|run| run.status)
        };
        let newer = status_for(2).expect("the newer comment must have a run");
        assert_ne!(
            newer,
            ExecutionStatus::Cancelled,
            "a late comment cancelled the newer comment's run"
        );
        assert_eq!(status_for(1), Some(ExecutionStatus::Cancelled));
    }
}
