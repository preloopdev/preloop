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

use crate::events::EffectiveEvent;
use crate::github_breaker::{BreakerOpen, GithubCredentialBucket};
use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
use crate::{
    ControlBackend, ExecutionStatus, SharedState, changed_paths_from_payload,
    submit_run_inner_with_webhook_delivery,
};
use preloop_gha_protocol::{
    AnnotationLevel, JobId, NdjsonEvent, PrebuiltMerge, RunId, WorkflowSubmission,
};

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

/// Mint the token one GitHub side-channel needs: an App installation token
/// carrying exactly `permissions` (clamped to the installation's grants), or
/// `PRELOOP_GITHUB_TOKEN` when no App covers the repo. `None` in local mode.
async fn resolve_repo_token(
    shared: &Arc<SharedState>,
    repo: &str,
    permissions: &std::collections::BTreeMap<String, String>,
) -> Option<String> {
    let app_creds = crate::github_app::select_app_for_repo(shared, repo).await;
    if let Some(app_creds) = app_creds.as_ref() {
        // The App mint intermittently 422s while the installation grants are
        // being read; a single retry keeps a transient rejection from
        // stranding the report.
        for attempt in 0..2 {
            match crate::github_app::get_or_mint_token(app_creds, repo, permissions).await {
                Ok(token) => return Some(token),
                Err(error) if attempt == 0 => {
                    tracing::warn!(
                        %repo,
                        %error,
                        "GitHub token mint failed; retrying once"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => {
                    tracing::warn!(%repo, %error, "GitHub token mint failed");
                    break;
                }
            }
        }
    }
    let fallback = shared.state.static_github_pat();
    if fallback.is_none() && app_creds.is_some() {
        record_check_reporting(shared, false);
    }
    fallback
}

async fn resolve_check_run_token(shared: &Arc<SharedState>, repo: &str) -> Option<String> {
    let mut permissions = std::collections::BTreeMap::new();
    permissions.insert("checks".to_owned(), "write".to_owned());
    resolve_repo_token(shared, repo, &permissions).await
}

async fn send_github_check_request(
    shared: &Arc<SharedState>,
    bucket: crate::github_breaker::GithubCredentialBucket,
    token: &str,
    repo: &str,
    method: reqwest::Method,
    path: &str,
    body: &Value,
) -> anyhow::Result<Value> {
    let client = crate::shared_http::CLIENT.clone();
    let url = format!("{}/repos/{}/{}", github_api_base(), repo, path);
    let res = crate::github_breaker::send_observed_labeled(
        &shared.state.github_breakers,
        bucket,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::CheckRuns,
        client
            .request(method, &url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {}", token))
            .header("Accept", "application/vnd.github+json")
            .json(body),
    )
    .await?;
    // The labeled sender already honours the primary budget advertised on
    // the response (`x-ratelimit-remaining: 0` + `x-ratelimit-reset`), so
    // the explicit `observe_rate_budget` call it replaces is gone.
    shared
        .state
        .github_consumption
        .record_advertised_bytes(crate::github_breaker::GithubSubsystem::CheckRuns, &res);

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
pub(crate) fn check_run_report_coords(run: &crate::models::RunRecord) -> Option<(String, String)> {
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
    bucket: crate::github_breaker::GithubCredentialBucket,
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
            bucket,
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

impl CheckRunRateKey {
    /// The breaker bucket for this rate key: installation budgets are
    /// per-installation, the PAT fallback shares the one PAT bucket.
    fn bucket(&self) -> crate::github_breaker::GithubCredentialBucket {
        match self {
            CheckRunRateKey::Installation(id) => {
                crate::github_breaker::GithubCredentialBucket::Installation(*id)
            }
            CheckRunRateKey::Pat => crate::github_breaker::GithubCredentialBucket::Pat,
        }
    }
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
/// Breaker state lives in the shared [`crate::github_breaker::GithubBreakerRegistry`]:
/// PAT-backed check runs share the PAT bucket with every other PAT caller
/// (action downloads, ref resolution), so a PAT 429 parks them too.
pub(crate) struct CheckRunSender {
    owner: String,
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
        }
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
        // never an authority. The bucket comes from the shared registry, so
        // PAT-backed check runs share the PAT bucket with action downloads
        // and every other PAT caller.
        let bucket = resolve_check_run_rate_key(shared, &repo).await.bucket();
        let breaker = shared.state.github_breakers.breaker_for(bucket);
        if let Some(remaining) = breaker.retry_after() {
            // Waiting out a rate limit is not a delivery failure: release the
            // row due at GitHub's advertised resume time without burning an
            // attempt, so a long throttle cannot exhaust the permanent-drop
            // budget.
            shared
                .state
                .github_consumption
                .record_breaker_blocked(crate::github_breaker::GithubSubsystem::CheckRuns);
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
        match send_queued_check_run(shared, &self.owner, bucket, token.as_deref(), update).await {
            Ok(()) => {}
            Err(error) if error.to_string().contains(LOST_CHECK_RUN_LEASE) => {
                // Another sender owns the row now; it delivers the update.
                // Touching the row here would overwrite its lease state.
                debug!(run_id=%update.run_id, job_id=%update.job_id, "check-run lease taken over by another sender; skipping");
            }
            Err(error) if error.downcast_ref::<BreakerOpen>().is_some() => {
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

/// The breaker bucket for a check-run/deployment request carrying `token`.
///
/// When the resolved token is the static PAT, the PAT bucket governs no
/// matter what the installation lookup said: the installation mint failed
/// and the PAT budget is what is actually spent. Otherwise the resolved
/// installation's bucket applies.
async fn check_request_bucket(
    shared: &Arc<SharedState>,
    repo: &str,
    token: Option<&str>,
) -> crate::github_breaker::GithubCredentialBucket {
    if let Some(token) = token
        && Some(token) == shared.state.static_github_pat().as_deref()
    {
        return crate::github_breaker::GithubCredentialBucket::Pat;
    }
    resolve_check_run_rate_key(shared, repo).await.bucket()
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

/// Render and deliver one queued update. `bucket` is the resolved
/// installation's rate budget; every request this function makes goes
/// through it.
async fn send_queued_check_run(
    shared: &Arc<SharedState>,
    owner: &str,
    bucket: crate::github_breaker::GithubCredentialBucket,
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
            find_existing_check_run(shared, bucket, token, repo, sha, &external_id).await?;
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
            bucket,
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
                bucket,
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
                    shared, owner, bucket, token, repo, sha, &title, update, id, body,
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
    bucket: crate::github_breaker::GithubCredentialBucket,
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
        match send_github_check_request(shared, bucket, token, repo, method, &path, &body).await {
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
                find_existing_check_run(shared, bucket, token, repo, sha, &external_id)
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

    // Environment side-channel: the deployment flips to `in_progress` once
    // the job actually starts executing (GitHub's DeploymentStatus states).
    // This post is asynchronous — a fast job can already be concluded by the
    // time it runs, and a late `in_progress` after the terminal status leaves
    // GitHub showing a finished deployment as running. Fence on the persisted
    // job state at read time and again right before posting (the terminal
    // commit may land while the token mints).
    let Some(row) = environment_deployment_row(shared, run_id, job_id).await else {
        return;
    };
    if row.job_status.is_terminal() {
        return;
    }
    let Some(token) = resolve_environment_token(shared, &row.repository).await else {
        return;
    };
    let Some(deployment_id) = ensure_environment_deployment(shared, run_id, job_id, &row).await
    else {
        return;
    };
    if environment_job_terminal(shared, run_id, job_id).await {
        return;
    }
    if let Err(error) = post_deployment_status(
        shared,
        &token,
        &row.repository,
        run_id,
        deployment_id,
        "in_progress",
        row.environment_url.as_deref(),
    )
    .await
    {
        tracing::warn!(
            %run_id, %job_id, %error,
            "deployment in_progress status failed"
        );
    }
}

/// The fence [`report_check_run_in_progress`] re-checks before posting the
/// asynchronous `in_progress` deployment status: the job's persisted status.
/// An unreadable or missing row answers `true` — skipping a real `in_progress`
/// costs one stale `queued` badge, while posting one over a concluded job
/// leaves a finished deployment showing as running forever.
async fn environment_job_terminal(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) -> bool {
    match shared.state.backend.environment_gate(run_id, job_id).await {
        Ok(Some(read)) => read.status.is_terminal(),
        _ => true,
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
    status: ExecutionStatus,
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

    // Environment side-channel: a concluded `environment:` job posts its
    // deployment's terminal status (`success`/`failure`). No-op for jobs
    // without one.
    report_environment_concluded(shared, run_id, job_id, status).await;
}

// ── Environment deployments + review announcements ────────────────────────
//
// GitHub pairs every `environment:` job with a Deployment whose statuses the
// UI rolls up (deployment statuses reference the deployment, which belongs to
// the environment — they never carry `environment` themselves). While the
// job waits on reviewers GitHub additionally marks the check run
// `action_required` with Approve/Reject actions; each `requested_action`
// webhook is one review decision.

/// Token for the environment side-channels: rules reads, deployment writes
/// and the check-run PATCHes the review surface needs. `deployments:write`
/// is the gate; `checks:write` covers `patch_check_run_actions` (the
/// `action_required` Approve/Reject surface lives on the check run);
/// `contents:read` keeps private repositories' environment endpoints
/// reachable.
async fn resolve_environment_token(shared: &Arc<SharedState>, repo: &str) -> Option<String> {
    let mut permissions = std::collections::BTreeMap::new();
    permissions.insert("deployments".to_owned(), "write".to_owned());
    permissions.insert("contents".to_owned(), "read".to_owned());
    permissions.insert("checks".to_owned(), "write".to_owned());
    resolve_repo_token(shared, repo, &permissions).await
}

/// Look up one job's deployment row: check run, stamped gate, hydrated
/// environment name/url, and the run's head sha. `None` for jobs with no
/// `environment:` or no reachable message template.
async fn environment_deployment_row(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
) -> Option<crate::control::types::EnvironmentDeploymentRow> {
    shared
        .state
        .backend
        .environment_deployment(run_id, job_id)
        .await
        .map_err(|error| {
            tracing::warn!(%run_id, %job_id, %error, "environment deployment row read failed");
            error
        })
        .ok()
        .flatten()
}

/// Lazily create + stamp the GitHub Deployment for an environment job.
/// GitHub auto-creates the environment itself on first deploy, so the create
/// call is `POST /repos/{o}/{r}/deployments` with the run's head sha and the
/// resolved environment name/url. Returns the deployment id.
async fn ensure_environment_deployment(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    row: &crate::control::types::EnvironmentDeploymentRow,
) -> Option<u64> {
    if let Some(id) = row.deployment_id {
        return Some(id);
    }
    let token = resolve_environment_token(shared, &row.repository).await?;
    let mut body =
        crate::control::logic::deployment_create_payload(&row.environment, &row.head_sha);
    if let Some(object) = body.as_object_mut() {
        object.insert(
            "description".to_owned(),
            format!("preloop run {run_id} job {}", job_id.0).into(),
        );
    }
    let bucket = check_request_bucket(shared, &row.repository, Some(&token)).await;
    let res = send_github_check_request(
        shared,
        bucket,
        &token,
        &row.repository,
        reqwest::Method::POST,
        "deployments",
        &body,
    )
    .await;
    match res {
        Ok(value) => match value.get("id").and_then(Value::as_u64) {
            Some(id) => {
                if let Err(error) = shared
                    .state
                    .backend
                    .set_job_deployment(run_id, job_id, id)
                    .await
                {
                    tracing::warn!(
                        %run_id, %job_id, %error,
                        "stamping deployment id failed"
                    );
                }
                Some(id)
            }
            None => {
                tracing::warn!(%run_id, %job_id, "deployment create returned no id");
                None
            }
        },
        Err(error) => {
            tracing::warn!(%run_id, %job_id, %error, "GitHub deployment create failed");
            None
        }
    }
}

/// `POST /repos/{o}/{r}/deployments/{id}/statuses`.
async fn post_deployment_status(
    shared: &Arc<SharedState>,
    token: &str,
    repo: &str,
    run_id: RunId,
    deployment_id: u64,
    state: &str,
    environment_url: Option<&str>,
) -> anyhow::Result<()> {
    let mut body = serde_json::json!({
        "state": state,
        "log_url": run_details_url(run_id).unwrap_or_default(),
    });
    if let Some(url) = environment_url {
        body["environment_url"] = serde_json::json!(url);
    }
    let bucket = check_request_bucket(shared, repo, Some(token)).await;
    send_github_check_request(
        shared,
        bucket,
        token,
        repo,
        reqwest::Method::POST,
        &format!("deployments/{deployment_id}/statuses"),
        &body,
    )
    .await
    .map(|_| ())
}

/// PATCH a check run for the environment review surface.
///
/// `action_required` is a check-run *conclusion*, never a status: the writable
/// statuses are `queued`, `in_progress` and `completed`, and GitHub rejects a
/// PATCH whose status is not one of them (`422`). `actions` — the
/// Approve/Reject buttons whose click arrives as
/// `check_run.requested_action` — are accepted only on a completed run whose
/// conclusion is `action_required`. `conclusion: None` sends `in_progress`,
/// clearing the review surface on the approving transition.
async fn patch_check_run_actions(
    shared: &Arc<SharedState>,
    token: &str,
    repo: &str,
    check_run_id: u64,
    conclusion: Option<&str>,
    actions: Option<Value>,
    summary: Option<&str>,
) -> anyhow::Result<()> {
    let mut body = match conclusion {
        Some(conclusion) => serde_json::json!({
            "status": "completed",
            "conclusion": conclusion,
        }),
        None => serde_json::json!({ "status": "in_progress" }),
    };
    if let Some(actions) = actions {
        body["actions"] = actions;
    }
    if let Some(summary) = summary {
        body["output"] = serde_json::json!({ "title": "Deployment review", "summary": summary });
    }
    let bucket = check_request_bucket(shared, repo, Some(token)).await;
    send_github_check_request(
        shared,
        bucket,
        token,
        repo,
        reqwest::Method::PATCH,
        &format!("check-runs/{check_run_id}"),
        &body,
    )
    .await
    .map(|_| ())
}

/// The Approve/Reject action pair GitHub renders on a `action_required`
/// check run while an environment gate waits. `identifier`s are what a
/// `check_run.requested_action` webhook echoes back.
fn environment_review_actions() -> Value {
    serde_json::json!([
        {
            "label": "Approve",
            "description": "Approve the deployment",
            "identifier": "approve",
        },
        {
            "label": "Reject",
            "description": "Reject the deployment",
            "identifier": "reject",
        },
    ])
}

/// Scan armed gates that need their GitHub review surface announced: PATCH
/// the check run to `action_required` with Approve/Reject and post the
/// deployment's `pending` status. Stamps `approval_announced` only after
/// both succeed — a retry sweep picks the row back up on failure.
///
/// Called post-commit from the gate sweep paths (`environment_sweep`,
/// reaper ticks) and best-effort: every failure logs and retries next tick.
pub async fn announce_environment_gates(shared: &Arc<SharedState>, run: Option<RunId>) {
    let pending = match shared
        .state
        .backend
        .pending_environment_approvals(run)
        .await
    {
        Ok(pending) => pending,
        Err(error) => {
            tracing::warn!(%error, "pending environment approvals scan failed");
            return;
        }
    };
    for row in pending {
        let Some(token) = resolve_environment_token(shared, &row.repository).await else {
            continue;
        };
        let deployment_id = if let Some(id) = row.deployment_id {
            Some(id)
        } else if let Some(full) = environment_deployment_row(shared, row.run_id, &row.job_id).await
        {
            ensure_environment_deployment(shared, row.run_id, &row.job_id, &full).await
        } else {
            None
        };
        // The check run PATCH and the deployment status are independent
        // surfaces: a job reporting no checks still gets its deployment row.
        // But a reporting run whose check run id has not landed yet (the mint
        // is a separate spawned task), and a deployment create that failed,
        // are *expected* surfaces that are missing — leave the gate
        // unannounced so the reaper sweep retries instead of dropping them.
        let mut announced = true;
        if row.reports_check_runs && row.check_run_id.is_none() {
            announced = false;
        }
        if let Some(check_run_id) = row.check_run_id
            && let Err(error) = patch_check_run_actions(
                shared,
                &token,
                &row.repository,
                check_run_id,
                Some("action_required"),
                Some(environment_review_actions()),
                Some(&format!(
                    "Deployment to environment `{}` is awaiting review",
                    row.environment_name
                )),
            )
            .await
        {
            tracing::warn!(run_id = %row.run_id, %error, "check run action_required patch failed");
            announced = false;
        }
        match deployment_id {
            Some(deployment_id) => {
                if let Err(error) = post_deployment_status(
                    shared,
                    &token,
                    &row.repository,
                    row.run_id,
                    deployment_id,
                    "pending",
                    row.environment_url.as_deref(),
                )
                .await
                {
                    tracing::warn!(run_id = %row.run_id, %error, "deployment pending status failed");
                    announced = false;
                }
            }
            None => announced = false,
        }
        if announced
            && let Err(error) = shared
                .state
                .backend
                .mark_environment_approval_announced(row.run_id, &row.job_id)
                .await
        {
            tracing::warn!(run_id = %row.run_id, %error, "announced stamp failed; will retry");
        }
    }
}

/// Settle a gate's GitHub side-channel after a review *decision*: PATCH the
/// check run off `action_required` and post the deployment's
/// `queued`/`failure` status. Call only once the gate has a verdict —
/// `approved` means "the gate released the deployment" (`true`) or "the
/// gate rejected it" (`false`). An approval that was recorded but does not
/// yet satisfy the required count is not a decision: GitHub keeps the
/// review pending, and reporting one would show a rejection that never
/// happened — callers must skip it.
pub async fn report_environment_review(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    approved: bool,
    reviewer: Option<&str>,
    note: Option<&str>,
) {
    let Some(row) = environment_deployment_row(shared, run_id, job_id).await else {
        return;
    };
    let Some(token) = resolve_environment_token(shared, &row.repository).await else {
        return;
    };
    if let Some(check_run_id) = row.check_run_id {
        // Approved: hand the check run back to `in_progress` so the job's own
        // lifecycle updates continue; the gate is already released. Rejected:
        // leave the run `action_required` — the terminal PATCH from
        // `report_check_run_completed` lands on top anyway.
        let (conclusion, summary) = if approved {
            let who = reviewer.unwrap_or("a reviewer");
            (None, format!("Deployment approved by {who}"))
        } else {
            let who = reviewer.unwrap_or("a reviewer");
            (
                Some("action_required"),
                format!("Deployment rejected by {who}"),
            )
        };
        let summary = match note {
            Some(note) if !note.is_empty() => format!("{summary}: {note}"),
            _ => summary,
        };
        if let Err(error) = patch_check_run_actions(
            shared,
            &token,
            &row.repository,
            check_run_id,
            conclusion,
            if approved {
                None
            } else {
                Some(environment_review_actions())
            },
            Some(&summary),
        )
        .await
        {
            tracing::warn!(%run_id, %job_id, %error, "check run review patch failed");
        }
    }
    if let Some(deployment_id) = ensure_environment_deployment(shared, run_id, job_id, &row).await {
        let state = if approved { "queued" } else { "failure" };
        if let Err(error) = post_deployment_status(
            shared,
            &token,
            &row.repository,
            run_id,
            deployment_id,
            state,
            row.environment_url.as_deref(),
        )
        .await
        {
            tracing::warn!(%run_id, %job_id, %error, "deployment review status failed");
        }
    }
}

/// Post the terminal deployment status for a finished environment job:
/// `success` for `Success`, `failure` for everything else (cancelled,
/// rejected, timed-out — GitHub folds non-success conclusions into
/// `failure`). No-op for jobs without `environment:` or check reporting.
pub async fn report_environment_concluded(
    shared: &Arc<SharedState>,
    run_id: RunId,
    job_id: &JobId,
    status: ExecutionStatus,
) {
    let Some(row) = environment_deployment_row(shared, run_id, job_id).await else {
        return;
    };
    // GitHub creates a Deployment only once the job starts or its gate enters
    // review — a job skipped by `if:` (or otherwise concluded before either)
    // has none. Never mint one at conclusion time for a job GitHub never
    // tracked.
    if row.deployment_id.is_none() && !row.deployment_started {
        return;
    }
    let Some(token) = resolve_environment_token(shared, &row.repository).await else {
        return;
    };
    let Some(deployment_id) = ensure_environment_deployment(shared, run_id, job_id, &row).await
    else {
        return;
    };
    let state = if status == ExecutionStatus::Success {
        "success"
    } else {
        "failure"
    };
    if let Err(error) = post_deployment_status(
        shared,
        &token,
        &row.repository,
        run_id,
        deployment_id,
        state,
        row.environment_url.as_deref(),
    )
    .await
    {
        tracing::warn!(%run_id, %job_id, %error, "deployment terminal status failed");
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

/// The workflow files `git_ref` holds under `.github/workflows`, read with `git`
/// from the repository at `repo_dir`: a local worktree, or the engine's mirror
/// that holds a self-built merge.
async fn workflows_at_git_ref(
    repo_dir: &std::path::Path,
    git_ref: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    if git_ref.starts_with('-') {
        anyhow::bail!("workflow revision {git_ref:?} is not a valid Git ref");
    }
    let commitish = format!("{git_ref}^{{commit}}");
    let resolved = tokio::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["rev-parse", "--verify", &commitish])
        .output()
        .await?;
    if !resolved.status.success() {
        anyhow::bail!(
            "workflow revision {git_ref:?} is unavailable in {}: {}",
            repo_dir.display(),
            String::from_utf8_lossy(&resolved.stderr),
        );
    }
    let listing = tokio::process::Command::new("git")
        .arg("-C")
        .arg(repo_dir)
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
            repo_dir.display(),
            String::from_utf8_lossy(&listing.stderr),
        );
    }
    let mut workflows = BTreeMap::new();
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
                .arg(repo_dir)
                .args(["show", &path_ref])
                .output()
                .await?;
            if !content.status.success() {
                anyhow::bail!(
                    "git show {path_ref:?} in {} failed: {}",
                    repo_dir.display(),
                    String::from_utf8_lossy(&content.stderr),
                );
            }
            workflows.insert(
                name.to_owned(),
                String::from_utf8_lossy(&content.stdout).into_owned(),
            );
        }
    }
    Ok(workflows)
}

/// The workflows of a self-built pull-request merge, read from the merge commit
/// in the engine's mirror where it was built. GitHub has no copy of that commit,
/// and its own `pull_request` runs read their workflows from the merge, so this
/// is the workflow set the run executes.
async fn fetch_merge_workflows(
    shared: &Arc<SharedState>,
    merge: &PrebuiltMerge,
) -> anyhow::Result<BTreeMap<String, String>> {
    let mirror = shared.state.state_dir.join(&merge.mirror_repository);
    workflows_at_git_ref(&mirror, &merge.sha).await
}

/// The workflows one effective event's runs are submitted from.
///
/// A pull-request run on an engine-built merge takes its workflows from that
/// merge, the tree it checks out. Only a failed mirror read falls back to the
/// pull request head's workflows, with a warning.
async fn event_workflows(
    shared: &Arc<SharedState>,
    repo: &str,
    effective: &EffectiveEvent,
    engine_built_merge: Option<&PrebuiltMerge>,
    resolved_sha: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    match (engine_built_merge, is_pull_request_run_event(effective)) {
        (Some(merge), true) => match fetch_merge_workflows(shared, merge).await {
            Ok(workflows) => Ok(workflows),
            Err(error) => {
                let head = effective
                    .status_check_sha
                    .as_deref()
                    .unwrap_or(resolved_sha);
                warn!(
                    repository = %repo,
                    merge = %merge.sha,
                    head = %head,
                    ?error,
                    "could not read the workflows at the engine-built merge; using the pull request head's"
                );
                fetch_workflows(shared, repo, head).await
            }
        },
        _ => fetch_workflows(shared, repo, resolved_sha).await,
    }
}

pub async fn fetch_workflows_at(
    shared: &Arc<SharedState>,
    repo: &str,
    git_ref: &str,
    api_base: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    if let Some(base_path) = &shared.state.local_workspace {
        // A dispatch for a branch other than the checked-out tree must read
        // the workflow definitions from that ref, not from the working tree.
        if !git_ref.is_empty() {
            return workflows_at_git_ref(base_path, git_ref).await;
        }
        let mut workflows = BTreeMap::new();
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
        let (token, bucket) = match crate::github_app::select_app_for_repo(shared, repo).await {
            Some(app) => {
                let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
                let token =
                    crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions)
                        .await?;
                // The installation id is the rate-limit identity; never
                // invent one (same convention as check runs).
                let bucket = match crate::github_app::installation_for_repo(&app, repo).await {
                    Ok(installation) if installation != 0 => {
                        crate::github_breaker::GithubCredentialBucket::Installation(installation)
                    }
                    _ => crate::github_breaker::GithubCredentialBucket::Pat,
                };
                (Some(token), bucket)
            }
            None => (
                shared.state.static_github_pat(),
                crate::github_breaker::GithubCredentialBucket::Pat,
            ),
        };
        if let Some(token) = &token {
            fetch_remote_workflows(shared, bucket, token, repo, git_ref, api_base).await
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
    shared: &Arc<SharedState>,
    bucket: crate::github_breaker::GithubCredentialBucket,
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
    let response = crate::github_breaker::send_observed_labeled(
        &shared.state.github_breakers,
        bucket,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::Workflows,
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
            let file_res = crate::github_breaker::send_observed_labeled(
                &shared.state.github_breakers,
                bucket,
                &shared.state.github_consumption,
                crate::github_breaker::GithubSubsystem::Workflows,
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
    let Some((token, bucket)) = contents_read_token(shared, repository, &api_base).await? else {
        return Ok(None);
    };
    let commit_ref = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(git_ref);
    let response = crate::github_breaker::send_observed_labeled(
        &shared.state.github_breakers,
        bucket,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::Workflows,
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
) -> anyhow::Result<Option<(String, crate::github_breaker::GithubCredentialBucket)>> {
    if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await {
        let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        let token =
            crate::github_app::get_or_mint_token_at(api_base, &app, repository, &permissions)
                .await?;
        // The installation id is the rate-limit identity; never invent one
        // (same convention as check runs).
        let bucket = match crate::github_app::installation_for_repo(&app, repository).await {
            Ok(installation) if installation != 0 => {
                crate::github_breaker::GithubCredentialBucket::Installation(installation)
            }
            _ => crate::github_breaker::GithubCredentialBucket::Pat,
        };
        return Ok(Some((token, bucket)));
    }
    // The env PAT here mirrors the pre-existing credential ladder; the
    // bucket is the PAT's either way.
    Ok(std::env::var("PRELOOP_GITHUB_TOKEN")
        .ok()
        .map(|token| (token, crate::github_breaker::GithubCredentialBucket::Pat)))
}

/// GitHub's `github.ref_protected` for `git_ref`: whether branch protection
/// rules or rulesets apply to it. Only `refs/heads/*` is resolved here; tags
/// (which a tag ruleset can protect), pull-request refs, a local workspace,
/// and any lookup that fails resolve to `false`. That is the unprivileged
/// answer — consumers grant more to a protected ref (cache writers publish
/// only from one, OIDC trust policies key on the claim) — so an unknown ref
/// must never read as protected.
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
    let Some((token, bucket)) = contents_read_token(shared, repository, &api_base).await? else {
        return Ok(false);
    };
    // Branch names are URL-significant: `git check-ref-format` permits `#`, `%`
    // and `/`, so a raw `release#test` is transmitted as `release` (the rest is
    // a fragment) and a lookup for an unprotected branch would answer with a
    // *different* branch's protection. Encode the name as one path segment, the
    // way the action-tarball URLs already do.
    let encoded = crate::actions::percent_encode_path_segment(branch);
    let response = crate::github_breaker::send_observed_labeled(
        &shared.state.github_breakers,
        bucket,
        &shared.state.github_consumption,
        crate::github_breaker::GithubSubsystem::RefResolve,
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
    shared: &Arc<SharedState>,
    bucket: crate::github_breaker::GithubCredentialBucket,
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
        let response = crate::github_breaker::send_observed_labeled(
            &shared.state.github_breakers,
            bucket,
            &shared.state.github_consumption,
            crate::github_breaker::GithubSubsystem::Workflows,
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
    let (token, bucket) = match crate::github_app::select_app_for_repo(shared, repo).await {
        Some(app) => {
            let permissions = BTreeMap::from([("pull_requests".to_owned(), "read".to_owned())]);
            let token =
                crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await?;
            let bucket = match crate::github_app::installation_for_repo(&app, repo).await {
                Ok(installation) if installation != 0 => {
                    crate::github_breaker::GithubCredentialBucket::Installation(installation)
                }
                _ => crate::github_breaker::GithubCredentialBucket::Pat,
            };
            (Some(token), bucket)
        }
        None => (
            std::env::var("PRELOOP_GITHUB_TOKEN").ok(),
            crate::github_breaker::GithubCredentialBucket::Pat,
        ),
    };
    let Some(token) = token else {
        return Ok(None);
    };
    get_pr_changed_files(shared, bucket, &token, repo, pr_number, api_base)
        .await
        .map(Some)
}

/// Bounded poll schedule for resolving a pull request's live test-merge
/// commit through the GitHub API before a webhook delivery creates runs.
///
/// State rather than constants so tests can drive the poll without waiting
/// out the production budget.
#[derive(Debug, Clone)]
pub struct FreshMergePoll {
    /// Maximum time to wait before building the merge locally.
    pub budget: Duration,
    /// Delay between GitHub API probes.
    pub poll_interval: Duration,
}

impl Default for FreshMergePoll {
    fn default() -> Self {
        Self {
            // Keep the wait bounded and abortable with the delivery lease.
            budget: Duration::from_secs(30),
            poll_interval: Duration::from_secs(2),
        }
    }
}

/// Where a `pull_request`/`pull_request_review` delivery's runs check out.
#[derive(Debug)]
enum FreshMergePlacement {
    /// Keep the adapter's projection (not a GitHub-backed delivery, or the
    /// payload carries nothing to resolve against).
    Projected,
    /// `refs/pull/{n}/merge` at GitHub's live test merge of the payload head.
    Merge(String),
    /// GitHub reports a merge conflict. The docs are explicit: workflows do
    /// not run on pull_request activity for a conflicted pull request, so
    /// this delivery creates no pull_request runs.
    Conflict,
    /// GitHub's own test merge was out of reach (head race, probe failure, the
    /// poll budget ran out, or no probe permit or poll slot came free). The runs check out
    /// `refs/pull/{n}/merge` at a merge this engine built from the current base
    /// tip and the payload head, served from the engine's mirror — GitHub does
    /// not have the commit. Never the bare `refs/pull/{n}/head`: a run there
    /// tests a tree the pull request does not have.
    SelfMerge(Box<PrebuiltMerge>),
}

/// The pull request number a GitHub PR payload refers to.
fn webhook_pull_request_number(payload: &Value) -> Option<u64> {
    payload.get("number").and_then(Value::as_u64).or_else(|| {
        payload
            .get("pull_request")
            .and_then(|pr| pr.get("number"))
            .and_then(Value::as_u64)
    })
}

/// Whether an effective event is one GitHub places on the PR merge ref.
fn is_pull_request_run_event(event: &EffectiveEvent) -> bool {
    matches!(event.event.as_str(), "pull_request" | "pull_request_review")
}

/// The fork-workflow kill switch: whether this event's run is dropped before
/// any workflow is fetched or matched.
fn fork_policy_skips(policy: &crate::config::ForkPolicyConfig, event: &EffectiveEvent) -> bool {
    !policy.run_fork_workflows
        && event.trust_tier == Some(crate::events::trust_tier::TrustTier::UntrustedForkPullRequest)
}

/// GitHub's live view of a pull request's merge state.
#[derive(Debug, Deserialize)]
struct PullRequestMergeState {
    /// `null` (absent) while GitHub is still computing mergeability.
    mergeable: Option<bool>,
    /// The current test merge of the PR head into its base, when one exists.
    merge_commit_sha: Option<String>,
    /// The head GitHub used to compute `mergeable` and `merge_commit_sha`.
    head: Option<PullRequestRef>,
    /// The base tip GitHub used to compute `mergeable` and `merge_commit_sha`.
    base: Option<PullRequestRef>,
}

#[derive(Debug, Deserialize)]
struct PullRequestRef {
    sha: Option<String>,
}

/// Why a merge-state probe has not produced an answer yet.
#[derive(Debug)]
enum MergeProbeFailure {
    /// Re-polling within the budget may answer differently.
    Retry(String),
    /// Re-polling cannot help (auth, scope, missing pull request).
    Fatal(String),
}

impl MergeProbeFailure {
    fn reason(&self) -> &str {
        match self {
            Self::Retry(reason) | Self::Fatal(reason) => reason,
        }
    }
}

/// Classify a non-success GitHub status for the merge-state poll.
///
/// Everything but a client-side refusal is transient by nature. A 4xx (rate
/// limiting aside) is the one answer a retry cannot change, and spending the
/// whole budget on it would tax every delivery of a mis-scoped deployment.
fn merge_probe_status(status: reqwest::StatusCode) -> MergeProbeFailure {
    if status.is_client_error() && status != reqwest::StatusCode::TOO_MANY_REQUESTS {
        MergeProbeFailure::Fatal(format!("GitHub API returned {status}"))
    } else {
        MergeProbeFailure::Retry(format!("GitHub API returned {status}"))
    }
}

/// `GET /repos/{repo}/pulls/{number}`.
async fn fetch_pull_request_merge_state(
    shared: &Arc<SharedState>,
    api_base: &str,
    bucket: crate::github_breaker::GithubCredentialBucket,
    token: &str,
    repo: &str,
    number: u64,
) -> Result<PullRequestMergeState, MergeProbeFailure> {
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breakers,
        bucket,
        crate::shared_http::CLIENT
            .clone()
            .get(format!(
                "{}/repos/{repo}/pulls/{number}",
                api_base.trim_end_matches('/')
            ))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await
    .map_err(|error| MergeProbeFailure::Retry(format!("request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(merge_probe_status(response.status()));
    }
    response
        .json()
        .await
        .map_err(|error| MergeProbeFailure::Retry(format!("response was not JSON: {error}")))
}

/// How the merge commit GitHub offers lines up with this delivery's head and
/// with the base tip the API currently reports.
enum MergeParentage {
    /// The commit is the test merge of the payload head into the API's
    /// current base tip.
    Current,
    /// The commit is not a merge of the payload head into the current base.
    OtherHead,
}

/// Resolve how `merge_sha`'s parents line up with the payload `head` and the
/// API's `base`.
///
/// Never accept a merge commit without checking both parents: the pointer
/// lags exactly one push through a rapid sequence, and its first parent must
/// be the base tip observed in the same pull-request API response.
async fn merge_parentage(
    shared: &Arc<SharedState>,
    api_base: &str,
    bucket: crate::github_breaker::GithubCredentialBucket,
    token: &str,
    repo: &str,
    merge_sha: &str,
    base: &str,
    head: &str,
) -> Result<MergeParentage, MergeProbeFailure> {
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breakers,
        bucket,
        crate::shared_http::CLIENT
            .clone()
            .get(format!(
                "{}/repos/{repo}/commits/{merge_sha}",
                api_base.trim_end_matches('/')
            ))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await
    .map_err(|error| MergeProbeFailure::Retry(format!("request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(merge_probe_status(response.status()));
    }
    let commit: Value = response
        .json()
        .await
        .map_err(|error| MergeProbeFailure::Retry(format!("response was not JSON: {error}")))?;
    let parents = commit.get("parents").and_then(Value::as_array);
    let first_parent = parents
        .and_then(|parents| parents.first())
        .and_then(|parent| parent.get("sha"))
        .and_then(Value::as_str);
    let second_parent = parents
        .and_then(|parents| parents.get(1))
        .and_then(|parent| parent.get("sha"))
        .and_then(Value::as_str);
    let Some(second_parent) = second_parent else {
        return Ok(MergeParentage::OtherHead);
    };
    if !second_parent.eq_ignore_ascii_case(head) {
        return Ok(MergeParentage::OtherHead);
    }
    if first_parent.is_some_and(|sha| sha.eq_ignore_ascii_case(base)) {
        Ok(MergeParentage::Current)
    } else {
        Ok(MergeParentage::OtherHead)
    }
}

/// The pull request a delivery's merge-state resolution works on.
#[derive(Debug, Clone)]
struct FreshMergeTarget {
    /// Repository full name (`owner/repo`).
    repo: String,
    /// Pull request number; names `refs/pull/{number}/merge` and `/head`.
    number: u64,
    /// The payload's head sha. Every accepted merge is a merge of exactly
    /// this commit, and a merge this engine builds pins it as second parent.
    head: String,
    /// The payload's base branch (`pull_request.base.ref`).
    base_branch: String,
}

impl FreshMergeTarget {
    /// Read the target out of a webhook payload.
    ///
    /// `None` when the payload names no pull request (or no base branch to
    /// merge into): the adapter's own projection stays in charge.
    fn from_payload(payload: &Value) -> Option<Self> {
        let repo = payload
            .get("repository")
            .and_then(|repository| repository.get("full_name"))
            .and_then(Value::as_str)?;
        let number = webhook_pull_request_number(payload)?;
        let pull_request = payload.get("pull_request")?;
        let head = pull_request.get("head")?.get("sha")?.as_str()?;
        let base_branch = pull_request
            .get("base")
            .and_then(|base| base.get("ref"))
            .and_then(Value::as_str)?;
        Some(Self {
            repo: repo.to_owned(),
            number,
            head: head.to_owned(),
            base_branch: base_branch.to_owned(),
        })
    }
}

/// What one probe of GitHub's live merge state decided.
enum MergeProbe {
    /// A verifiable answer from GitHub.
    Resolved(FreshMergePlacement),
    /// No verifiable merge yet; a later probe may answer.
    Waiting(String),
    /// The probe could not answer, and re-probing will not change that (auth,
    /// scope, missing pull request).
    Unavailable(String),
}

/// Ask GitHub where this delivery's pull-request runs check out.
///
/// The payload's merge sha can lag the push. Accept only a merge whose first
/// parent matches the API's current base and whose second parent is the payload
/// head.
async fn probe_fresh_merge(shared: &Arc<SharedState>, target: &FreshMergeTarget) -> MergeProbe {
    let api_base = github_api_base();
    let (token, bucket) = match crate::github_app::select_app_for_repo(shared, &target.repo).await {
        Some(app) => {
            let permissions = BTreeMap::from([
                ("contents".to_owned(), "read".to_owned()),
                ("pull_requests".to_owned(), "read".to_owned()),
            ]);
            match crate::github_app::get_or_mint_token_at(
                &api_base,
                &app,
                &target.repo,
                &permissions,
            )
            .await
            {
                Ok(token) => {
                    // The installation id is the rate-limit identity; never
                    // invent one (same convention as check runs).
                    let bucket =
                        match crate::github_app::installation_for_repo(&app, &target.repo).await {
                            Ok(installation) if installation != 0 => {
                                crate::github_breaker::GithubCredentialBucket::Installation(
                                    installation,
                                )
                            }
                            _ => crate::github_breaker::GithubCredentialBucket::Pat,
                        };
                    (Some(token), bucket)
                }
                Err(error) => {
                    return MergeProbe::Unavailable(format!(
                        "failed to mint a pull-request read token: {error:#}"
                    ));
                }
            }
        }
        // The same PAT ladder as the workflow inventory.
        None => (
            shared.state.static_github_pat(),
            crate::github_breaker::GithubCredentialBucket::Pat,
        ),
    };
    // No GitHub credentials at all (local-workspace development, native
    // webhook replays): there is no API to ask, so the projection stands.
    let Some(token) = token else {
        return MergeProbe::Resolved(FreshMergePlacement::Projected);
    };

    let state = match fetch_pull_request_merge_state(
        shared,
        &api_base,
        bucket,
        &token,
        &target.repo,
        target.number,
    )
    .await
    {
        Ok(state) => state,
        Err(MergeProbeFailure::Fatal(reason)) => return MergeProbe::Unavailable(reason),
        Err(MergeProbeFailure::Retry(reason)) => return MergeProbe::Waiting(reason),
    };
    let api_head = state
        .head
        .as_ref()
        .and_then(|reference| reference.sha.as_deref())
        .map(str::to_owned);
    let response_base = state
        .base
        .as_ref()
        .and_then(|reference| reference.sha.as_deref())
        .map(str::to_owned);

    // A candidate merge is checked before anything the API says about the
    // head: through a rapid push sequence the pointer can report a newer head
    // while `merge_commit_sha` still names this delivery's own head's merge,
    // which is exactly what this delivery wants.
    if let Some(merge) = state.merge_commit_sha.as_deref() {
        let Some(base) = response_base.as_deref() else {
            return MergeProbe::Waiting(
                "GitHub merge response omitted the pull request's base".to_owned(),
            );
        };
        match merge_parentage(
            shared,
            &api_base,
            bucket,
            &token,
            &target.repo,
            merge,
            base,
            &target.head,
        )
        .await
        {
            Ok(MergeParentage::Current) => {
                return MergeProbe::Resolved(FreshMergePlacement::Merge(merge.to_owned()));
            }
            Ok(MergeParentage::OtherHead) => {}
            Err(MergeProbeFailure::Fatal(reason)) => return MergeProbe::Unavailable(reason),
            Err(MergeProbeFailure::Retry(reason)) => return MergeProbe::Waiting(reason),
        }
    }
    let Some(api_head) = api_head else {
        return MergeProbe::Waiting(
            "GitHub merge response omitted the pull request's head".to_owned(),
        );
    };
    if !api_head.eq_ignore_ascii_case(&target.head) {
        // The API's head is not this delivery's head. The pointer lags a push
        // by a second or two, so this may simply be "not caught up yet", and
        // only the budget can tell that apart from a superseded head whose
        // merge GitHub will never surface. Keep polling; the timeout builds
        // the engine's own merge when nothing appears.
        return MergeProbe::Waiting(format!(
            "the API reports head {api_head}, not this delivery's {}",
            target.head
        ));
    }
    if state.mergeable == Some(false) {
        info!(
            repository = %target.repo,
            pull_request = target.number,
            "pull request has a merge conflict; skipping pull_request runs for this delivery"
        );
        return MergeProbe::Resolved(FreshMergePlacement::Conflict);
    }
    match state.mergeable {
        Some(true) => MergeProbe::Waiting(format!(
            "the API's merge pointer does not (yet) name a merge of {}",
            target.head
        )),
        other => MergeProbe::Waiting(format!("GitHub reports mergeable={other:?}")),
    }
}

/// Why a delivery could not be placed on the pull request's merge.
struct FreshMergeFailure {
    reason: String,
    /// True when retrying the delivery could still place it: the mirror fetch
    /// failed. A head or base the mirror can never hold is permanent, and the
    /// delivery is dead-lettered instead of retried.
    transient: bool,
}

impl FreshMergeFailure {
    fn transient(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            transient: true,
        }
    }

    fn permanent(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            transient: false,
        }
    }
}

/// Resolve where this delivery's pull-request runs check out.
///
/// Every probe of GitHub's merge state takes one of the node's probe permits
/// ([`take_probe_permit`]), so a bounded number of webhook workers can be inside
/// GitHub's API at once. A merge GitHub already offers is used as soon as a
/// probe sees it. A delivery that must wait takes one of the node's poll slots
/// ([`fresh_merge_poll_capacity`]) and re-probes every
/// [`FreshMergePoll::poll_interval`] until the budget is spent. Waiting parks a
/// webhook worker for the whole budget, so a delivery that cannot get a slot
/// stops waiting and builds its own merge at once. Every path that cannot use
/// GitHub's merge ends at the engine's own merge of the current base tip and
/// the payload head — never at the bare `refs/pull/{n}/head`, which is not the
/// pull request's tree. `Err` means no placement could be produced at all; the
/// delivery is retried or dead-lettered.
async fn resolve_fresh_merge(
    shared: &Arc<SharedState>,
    target: &FreshMergeTarget,
) -> Result<FreshMergePlacement, FreshMergeFailure> {
    match poll_fresh_merge(shared, target).await {
        Some(placement) => Ok(placement),
        None => self_merge(shared, target).await,
    }
}

/// How long a delivery waits for one of the node's probe permits before it
/// gives up on GitHub's merge state and builds its own merge.
const FRESH_MERGE_PROBE_WAIT: Duration = Duration::from_secs(2);

/// Take one of the node's probe permits for a single merge-state probe.
///
/// A probe can take as long as the client's own timeout, and it runs on a
/// webhook worker. Without this bound, slow GitHub answers could hold every
/// worker inside the API. `None` when no permit came free within
/// [`FRESH_MERGE_PROBE_WAIT`].
async fn take_probe_permit(shared: &Arc<SharedState>) -> Option<tokio::sync::OwnedSemaphorePermit> {
    let permits = shared.state.fresh_merge_probe_permits.clone();
    match tokio::time::timeout(FRESH_MERGE_PROBE_WAIT, permits.acquire_owned()).await {
        Ok(Ok(permit)) => Some(permit),
        // The wait ran out, or the semaphore closed (it never is).
        _ => None,
    }
}

/// Poll GitHub's merge state for `target` until it answers or the budget ends.
///
/// `None` means GitHub's merge cannot be used, and the caller builds the merge
/// itself: the merge state is unavailable, the budget ran out, or no probe
/// permit or poll slot came free. Both are RAII permits: each probe holds its
/// permit only while it runs, and the poll slot is held across the whole wait.
/// Every exit drops them, including the processing task being aborted when the
/// delivery's lease is lost, so neither can leak.
async fn poll_fresh_merge(
    shared: &Arc<SharedState>,
    target: &FreshMergeTarget,
) -> Option<FreshMergePlacement> {
    let poll = &shared.state.fresh_merge_poll;
    let deadline = Instant::now() + poll.budget;
    let mut slot = None;
    loop {
        let probe = {
            let Some(_permit) = take_probe_permit(shared).await else {
                info!(
                    repository = %target.repo,
                    pull_request = target.number,
                    "every merge-state probe permit is busy; building the test merge ourselves"
                );
                return None;
            };
            probe_fresh_merge(shared, target).await
        };
        match probe {
            MergeProbe::Resolved(placement) => return Some(placement),
            MergeProbe::Unavailable(reason) => {
                warn!(
                    repository = %target.repo,
                    pull_request = target.number,
                    reason = %reason,
                    "pull request merge state is unavailable; building the test merge ourselves"
                );
                return None;
            }
            MergeProbe::Waiting(reason) => {
                debug!(
                    repository = %target.repo,
                    pull_request = target.number,
                    reason = %reason,
                    "GitHub has not produced the pull request's test merge yet; re-polling"
                );
            }
        }
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        if slot.is_none() {
            match shared
                .state
                .fresh_merge_poll_slots
                .clone()
                .try_acquire_owned()
            {
                Ok(permit) => slot = Some(permit),
                Err(_) => {
                    info!(
                        repository = %target.repo,
                        pull_request = target.number,
                        "every merge-state poll slot is busy; building the test merge ourselves"
                    );
                    return None;
                }
            }
        }
        tokio::time::sleep(poll.poll_interval.min(deadline - now)).await;
    }
    warn!(
        repository = %target.repo,
        pull_request = target.number,
        budget_secs = poll.budget.as_secs(),
        "GitHub did not surface the pull request's test merge within the budget; \
         building the test merge ourselves"
    );
    None
}

/// Build this engine's own test merge of the payload head into the current
/// base tip: the merge GitHub would have produced.
///
/// Used whenever GitHub's live merge cannot be used: its pointer moved past
/// this delivery's head, the API failed, or the poll budget ran out. The
/// engine's mirror holds the merge afterward, and the run is served from the
/// engine — GitHub has no such commit. A conflict found here is treated
/// exactly like GitHub's: no `pull_request` runs for this delivery.
async fn self_merge(
    shared: &Arc<SharedState>,
    target: &FreshMergeTarget,
) -> Result<FreshMergePlacement, FreshMergeFailure> {
    let source = crate::merge_builder::webhook_merge_source(shared, &target.repo)
        .await
        .map_err(|error| {
            FreshMergeFailure::transient(format!(
                "cannot open the merge mirror for {}: {error}",
                target.repo
            ))
        })?;
    let request = crate::merge_builder::MergeRequest {
        repository: target.repo.clone(),
        pull_request_number: Some(target.number),
        base_branch: target.base_branch.clone(),
        // The payload head, pinned: `refs/pull/{n}/head` may already point at
        // a newer commit, and this delivery must test the head it was sent.
        head: crate::merge_builder::MergeHead::Commit(target.head.clone()),
    };
    match crate::merge_builder::build_merge(&source, &request, &source.mirror).await {
        Ok(crate::merge_builder::MergeOutcome::Merged(merge)) => {
            let Some(prebuilt) = crate::merge_builder::prebuilt_merge_record(
                shared,
                &source.mirror,
                &merge,
                Some(target.number),
            ) else {
                return Err(FreshMergeFailure::permanent(format!(
                    "merge mirror {} is outside the state directory",
                    source.mirror.display()
                )));
            };
            Ok(FreshMergePlacement::SelfMerge(Box::new(prebuilt)))
        }
        Ok(crate::merge_builder::MergeOutcome::Conflict { paths, .. }) => {
            info!(
                repository = %target.repo,
                pull_request = target.number,
                conflicted_paths = paths.len(),
                "the engine's own test merge conflicts; skipping pull_request runs for this delivery"
            );
            Ok(FreshMergePlacement::Conflict)
        }
        Err(error) => {
            let reason = format!(
                "failed to build a test merge of {} into {}: {error}",
                target.head, target.base_branch
            );
            if error.is_transient() {
                Err(FreshMergeFailure::transient(reason))
            } else {
                Err(FreshMergeFailure::permanent(reason))
            }
        }
    }
}

/// A delivery's events after merge-state resolution, plus the engine-built
/// merge their run submissions must carry when the placement is one.
struct FreshenedEvents {
    events: Vec<EffectiveEvent>,
    /// Set when the pull-request events check out a merge this engine built.
    self_merge: Option<PrebuiltMerge>,
}

/// Apply a resolved placement to a delivery's projected events.
fn apply_fresh_merge_placement(
    events: Vec<EffectiveEvent>,
    placement: FreshMergePlacement,
    number: u64,
) -> FreshenedEvents {
    let place = |mut event: EffectiveEvent, git_ref: String, sha: &str| {
        event.git_ref = git_ref;
        event.sha = Some(sha.to_owned());
        event
    };
    let place_merge = |events: Vec<EffectiveEvent>, sha: &str| {
        events
            .into_iter()
            .map(|event| {
                if is_pull_request_run_event(&event) {
                    place(event, format!("refs/pull/{number}/merge"), sha)
                } else {
                    event
                }
            })
            .collect()
    };
    match placement {
        FreshMergePlacement::Projected => FreshenedEvents {
            events,
            self_merge: None,
        },
        FreshMergePlacement::Conflict => FreshenedEvents {
            events: events
                .into_iter()
                .filter(|event| !is_pull_request_run_event(event))
                .collect(),
            self_merge: None,
        },
        FreshMergePlacement::Merge(merge) => FreshenedEvents {
            events: place_merge(events, &merge),
            self_merge: None,
        },
        FreshMergePlacement::SelfMerge(merge) => FreshenedEvents {
            events: place_merge(events, &merge.sha),
            self_merge: Some(*merge),
        },
    }
}

/// Re-place a GitHub delivery's `pull_request`/`pull_request_review` events on
/// the pull request's *current* test merge — GitHub's live one, or a merge
/// this engine builds when GitHub's is out of reach.
///
/// Everything else — other events, deployments without GitHub credentials —
/// passes through with the adapter's projection. `Err` means the delivery
/// could not be placed at all (the engine's own test merge could not be
/// built) and must be retried or dead-lettered.
async fn freshen_pull_request_events(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
    payload: &Value,
    events: Vec<EffectiveEvent>,
) -> Result<FreshenedEvents, FreshMergeFailure> {
    let untouched = || FreshenedEvents {
        events: events.clone(),
        self_merge: None,
    };
    if !matches!(
        delivery.event.as_str(),
        "pull_request" | "pull_request_review"
    ) {
        return Ok(untouched());
    }
    if !events
        .iter()
        .any(|event| !event.skip && is_pull_request_run_event(event))
    {
        return Ok(untouched());
    }
    // The fork-policy kill switch drops fork-pull-request events further down,
    // after this probe. A delivery whose every pull-request event will be
    // dropped must not spend GitHub API calls on a merge nothing will run.
    if events
        .iter()
        .filter(|event| !event.skip && is_pull_request_run_event(event))
        .all(|event| fork_policy_skips(&shared.state.fork_policy, event))
    {
        info!("fork workflows are disabled; skipping merge-state resolution for this delivery");
        return Ok(untouched());
    }
    let Some(target) = FreshMergeTarget::from_payload(payload) else {
        return Ok(untouched());
    };
    let placement = resolve_fresh_merge(shared, &target).await?;
    Ok(apply_fresh_merge_placement(
        events,
        placement,
        target.number,
    ))
}

/// Whether a pull-request delivery's head commit carries a `[skip ci]` label.
///
/// The pull-request webhook payload carries only `head.sha`, never the commit
/// message, so the head commit must be fetched here — this gate lives in the
/// async dispatcher, not the pure `project()` adapters (which cannot make
/// network calls).
///
/// Fork PRs are never eligible: a fork's commit message is attacker-controlled,
/// so untrusted code must not be able to opt itself out of scrutiny. Every
/// failure to read the commit (auth, network, missing data) fails open and
/// runs CI; skipping is never inferred from an error.
async fn pr_head_commit_skips_ci(
    shared: &Arc<SharedState>,
    repo: &str,
    payload: &Value,
    api_base: &str,
) -> bool {
    let pr_head = payload.get("pull_request").and_then(|pr| pr.get("head"));

    // Fork PRs always run. `head.repo.fork` missing fails closed to fork.
    let is_fork = pr_head
        .and_then(|head| head.get("repo"))
        .and_then(|repo| repo.get("fork"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if is_fork {
        return false;
    }

    let Some(head_sha) = pr_head
        .and_then(|head| head.get("sha"))
        .and_then(Value::as_str)
    else {
        return false;
    };

    let (token, bucket) = match crate::github_app::select_app_for_repo(shared, repo).await {
        Some(app) => {
            let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
            match crate::github_app::get_or_mint_token_at(api_base, &app, repo, &permissions).await
            {
                Ok(token) => {
                    let bucket = match crate::github_app::installation_for_repo(&app, repo).await {
                        Ok(installation) if installation != 0 => {
                            crate::github_breaker::GithubCredentialBucket::Installation(
                                installation,
                            )
                        }
                        _ => crate::github_breaker::GithubCredentialBucket::Pat,
                    };
                    (Some(token), bucket)
                }
                Err(error) => {
                    warn!(
                        ?error,
                        "failed to mint token for [skip ci] check; running CI"
                    );
                    return false;
                }
            }
        }
        None => (
            std::env::var("PRELOOP_GITHUB_TOKEN").ok(),
            crate::github_breaker::GithubCredentialBucket::Pat,
        ),
    };
    let Some(token) = token else {
        return false;
    };

    let response = match crate::github_breaker::send_observed(
        &shared.state.github_breakers,
        bucket,
        crate::shared_http::CLIENT
            .clone()
            .get(format!("{api_base}/repos/{repo}/commits/{head_sha}"))
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json"),
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            warn!(
                ?error,
                "failed to fetch PR head commit for [skip ci] check; running CI"
            );
            return false;
        }
    };
    if !response.status().is_success() {
        return false;
    }
    let Ok(commit) = response.json::<Value>().await else {
        return false;
    };

    commit
        .get("commit")
        .and_then(|c| c.get("message"))
        .and_then(Value::as_str)
        .is_some_and(crate::events::has_skip_label)
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

    let rerequest_sender = payload
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let accepted = crate::rerun_run_with_mode(
        shared,
        run_id,
        crate::control::types::RerunMode::Job(job_id.clone()),
        Some((job_id.clone(), check_run_id)),
        rerequest_sender,
        None,
    )
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

/// `check_run` `requested_action`: one environment review decision.
///
/// GitHub renders the Approve/Reject pair we PATCH onto the `action_required`
/// check run; a click delivers this webhook with
/// `requested_action.identifier`. We authorize `sender` against the
/// environment's reviewer list (teams expanded through the resolver) and
/// `prevent_self_review` against the run's actor, then record the decision —
/// the backend transaction that writes it also re-runs the promotion pass,
/// so a satisfied gate releases its job before we answer GitHub.
async fn process_check_run_requested_action(
    shared: &Arc<SharedState>,
    payload: &Value,
) -> Result<(), StatusCode> {
    if payload.get("action").and_then(Value::as_str) != Some("requested_action") {
        return Ok(());
    }
    let Some(check_run_id) = payload
        .get("check_run")
        .and_then(|check_run| check_run.get("id"))
        .and_then(Value::as_u64)
    else {
        return Ok(());
    };
    let decision = match payload
        .get("requested_action")
        .and_then(|action| action.get("identifier"))
        .and_then(Value::as_str)
    {
        Some("approve") => crate::control::types::EnvironmentDecision::Approve,
        Some("reject") => crate::control::types::EnvironmentDecision::Reject,
        other => {
            info!(
                ?other,
                check_run_id, "unknown check_run requested_action ignored"
            );
            return Ok(());
        }
    };
    // Not every requested_action is ours: only a check run still parked on
    // an armed reviewer gate maps to a pending approval.
    let Some(held) = shared
        .state
        .backend
        .pending_environment_approval_for_check_run(check_run_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        info!(
            check_run_id,
            "check_run requested_action matches no held environment gate"
        );
        return Ok(());
    };
    // The held job's repository must match the payload's: a delivery naming a
    // different repository never authorizes a review on this one. Missing or
    // mismatched `repository.full_name` fails closed. GitHub repository names
    // are case-insensitive, so compare case-insensitively.
    let payload_repository = payload
        .get("repository")
        .and_then(|repo| repo.get("full_name"))
        .and_then(Value::as_str);
    if !payload_repository.is_some_and(|repo| repo.eq_ignore_ascii_case(&held.repository)) {
        warn!(
            run_id = %held.run_id,
            job_id = %held.job_id.0,
            held_repository = %held.repository,
            ?payload_repository,
            check_run_id,
            "check_run requested_action names a different repository; ignored"
        );
        return Ok(());
    }
    let sender = payload
        .get("sender")
        .and_then(|sender| sender.get("login"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    // Fresh rules for the authorization check: the reviewer list may have
    // changed since the gate armed. A failed refresh falls back to the
    // cached entry; a never-resolved repo answers `false` (fail closed).
    let _ = shared
        .state
        .environment_resolver
        .resolve(shared, &held.repository, &held.environment_name)
        .await;
    let run_actor = shared
        .state
        .backend
        .run_record(held.run_id)
        .await
        .map(|record| record.submission.actor.clone())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let authorized = shared
        .state
        .environment_resolver
        .reviewer_authorized(
            shared,
            &held.repository,
            &held.environment_name,
            &sender,
            &run_actor,
        )
        .await
        .unwrap_or(false);
    if !authorized {
        warn!(
            run_id = %held.run_id,
            job_id = %held.job_id.0,
            %sender,
            "environment review from an unauthorized sender ignored"
        );
        return Ok(());
    }

    let outcome = shared
        .state
        .backend
        .record_environment_approval(crate::control::types::EnvironmentApproval {
            run_id: held.run_id,
            job_id: held.job_id.clone(),
            decision,
            actor: Some(sender.clone()),
            admin_override: false,
            note: None,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Ok(mut guard) = shared.state.next_job_runs_on.write() {
        *guard = outcome.next_runs_on;
    }
    shared.state.message_notify.notify_waiters();
    match outcome.result {
        crate::control::types::EnvironmentApprovalResult::Recorded { satisfied, .. } => {
            info!(
                run_id = %held.run_id,
                job_id = %held.job_id.0,
                %sender,
                satisfied,
                "environment approval recorded via check_run requested_action"
            );
            if satisfied {
                report_environment_review(
                    shared,
                    held.run_id,
                    &held.job_id,
                    true,
                    Some(&sender),
                    None,
                )
                .await;
            }
            // An approval recorded but not yet satisfying the required count
            // leaves the gate open on GitHub too: the check run stays
            // `action_required` and the deployment `pending`, so there is no
            // side-channel update to report. Reporting it would stamp the
            // review as a rejection (and the deployment as `failure`).
        }
        crate::control::types::EnvironmentApprovalResult::Rejected => {
            info!(
                run_id = %held.run_id,
                job_id = %held.job_id.0,
                %sender,
                "environment rejection recorded via check_run requested_action"
            );
            report_environment_review(
                shared,
                held.run_id,
                &held.job_id,
                false,
                Some(&sender),
                None,
            )
            .await;
        }
        crate::control::types::EnvironmentApprovalResult::AlreadyTerminal
        | crate::control::types::EnvironmentApprovalResult::NotAwaiting
        | crate::control::types::EnvironmentApprovalResult::Expired => {
            info!(
                run_id = %held.run_id,
                job_id = %held.job_id.0,
                result = ?outcome.result,
                "check_run requested_action arrived after the gate settled"
            );
        }
    }
    Ok(())
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
    /// A dependency the delivery cannot influence is down: GitHub itself is
    /// unreachable, or the delivery's own credential is throttled. Distinct
    /// from `TransientError` because the delivery did nothing wrong: it is
    /// returned to the queue with its attempt refunded and retried after the
    /// breaker's window, instead of spending one of six attempts on it.
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

/// Deliveries that may wait on GitHub's test merge at once, and merge-state
/// probes that may be in flight at once, per node: a quarter of the webhook
/// workers, and at least one. A waiting delivery holds its worker for up to the
/// whole poll budget, so most workers stay free for every other delivery; the
/// rest build the engine's own merge without waiting.
pub fn fresh_merge_poll_capacity() -> usize {
    (webhook_workers() / 4).max(1)
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
        // push that arrived during it. Only unreachability gates claiming:
        // a per-credential rate limit parks that credential's bucket, while
        // other credentials' deliveries keep flowing.
        if let Some(retry_after) = shared.state.github_breakers.unavailable_retry_after() {
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

/// The credential bucket a delivery's GitHub calls are charged to.
///
/// The payload's `installation.id` is only what the sender claims; it does
/// not choose the credential. Calls resolve it the way every repo-scoped
/// GitHub call does — the App covering `repository.full_name` (its real
/// installation id), else the PAT — so a delivery parks on the bucket its
/// requests actually spend against, not the one the webhook happened to
/// name.
async fn webhook_delivery_bucket(
    shared: &Arc<SharedState>,
    delivery: &WebhookDeliveryRecord,
) -> GithubCredentialBucket {
    let repo = serde_json::from_slice::<Value>(&delivery.payload)
        .ok()
        .and_then(|payload| {
            payload
                .get("repository")
                .and_then(|repository| repository.get("full_name"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let Some(repo) = repo else {
        return GithubCredentialBucket::Pat;
    };
    let Some(app) = crate::github_app::select_app_for_repo(shared, &repo).await else {
        return GithubCredentialBucket::Pat;
    };
    match crate::github_app::installation_for_repo(&app, &repo).await {
        Ok(installation) if installation != 0 => GithubCredentialBucket::Installation(installation),
        _ => GithubCredentialBucket::Pat,
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
            // Park only when a breaker this delivery depends on is open: a
            // GitHub-wide outage, or the credential the delivery's GitHub
            // calls actually resolve to (payload `installation.id` is not
            // that credential — the repo's App/PAT selection is). An
            // unrelated throttled installation must not turn every transient
            // failure into a park, which would bypass the attempt cap. When
            // both block, wait for the longer of the two.
            let breakers = &shared.state.github_breakers;
            let own_bucket = webhook_delivery_bucket(shared, delivery).await;
            let blocked_for = breakers
                .unavailable_retry_after()
                .max(breakers.retry_after_for(own_bucket));
            match blocked_for {
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

/// Whether a webhook can be the echo of push-back publishing a tested commit:
/// the branch `push` itself, or the `pull_request` that opening, reopening or
/// updating its PR emits. Everything else at the same head is a new request
/// and must run: a `pull_request_target` run executes the *base* branch's
/// workflow, which a published head run never exercised, and a manual
/// dispatch, a label or a comment is the user asking again.
fn is_publish_echo(event: &str, activity_type: Option<&str>) -> bool {
    match event {
        "push" => true,
        "pull_request" => matches!(activity_type, Some("opened" | "reopened" | "synchronize")),
        _ => false,
    }
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
        let action = payload_val.get("action").and_then(Value::as_str);
        if action == Some("requested_action") {
            let handled = tokio::select! {
                _ = lease_lost.cancelled() => return WebhookOutcome::Success,
                result = process_check_run_requested_action(shared, &payload_val) => result,
            };
            return match handled {
                Ok(()) => WebhookOutcome::Success,
                Err(status) if status.is_server_error() => WebhookOutcome::TransientError(format!(
                    "check run requested_action failed with status {status}"
                )),
                Err(_) => WebhookOutcome::Success,
            };
        }
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

    if delivery.event == "check_suite" {
        // A rerequest re-runs the suite's run in place; the delivery still
        // falls through to the `check_suite` adapter below so
        // `on: check_suite` workflows see it.
        let rerequest = tokio::select! {
            _ = lease_lost.cancelled() => return WebhookOutcome::Success,
            result = crate::rerequest::process_check_suite_rerequest(shared, &payload_val) => result,
        };
        match rerequest {
            Ok(()) => {}
            Err(status) if status.is_server_error() => {
                return WebhookOutcome::TransientError(format!(
                    "check suite rerequest failed with status {status}"
                ));
            }
            Err(status) => {
                info!(%status, "check suite rerequest ignored");
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
    // GitHub computes a pull request's test-merge commit asynchronously, so
    // the payload's `merge_commit_sha` is often the previous merge or null.
    // Resolve the live placement before anything else reads `sha`/`git_ref`
    // (workflow inventory, changed-files filtering, run creation). A merge
    // conflict means GitHub creates no pull_request runs for this delivery;
    // an unresolvable placement fails the delivery for a retry.
    let freshened =
        match freshen_pull_request_events(shared, delivery, &payload_val, live_events).await {
            Ok(freshened) => freshened,
            // A placement that cannot be produced is fatal to this attempt: a
            // transient cause (mirror fetch) retries the delivery, a
            // permanent one (a head or base the mirror can never hold) is
            // dead-lettered rather than retried into the same wall.
            Err(failure) if failure.transient => {
                return WebhookOutcome::TransientError(failure.reason);
            }
            Err(failure) => return WebhookOutcome::Unreportable(failure.reason),
        };
    // The run submissions' `prebuilt_merge`: set only when the pull-request
    // events check out a merge this engine built.
    let engine_built_merge = freshened.self_merge;
    let effective_events = freshened.events;
    if effective_events.is_empty() {
        info!(
            "Event {} produced no runnable events after merge-state resolution",
            delivery.event
        );
        return WebhookOutcome::Success;
    }

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

    // [skip ci] on an internal PR's head commit suppresses the whole delivery
    // (both the pull_request_target and pull_request fan-out). Fork PRs are
    // never eligible — untrusted code cannot opt itself out of scrutiny. This
    // runs before the changed-files lookup so a skipped PR burns no API call.
    if matches!(
        delivery.event.as_str(),
        "pull_request" | "pull_request_target"
    ) {
        let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_owned());
        if pr_head_commit_skips_ci(shared, &repo_full_name, &payload_val, &api_base).await {
            info!("[skip ci] on internal pull-request head commit — suppressing delivery");
            return WebhookOutcome::Success;
        }
    }

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
        if fork_policy_skips(&shared.state.fork_policy, effective) {
            info!(
                event = %effective.event,
                "fork policy skipped fork pull-request event (run_fork_workflows = false)"
            );
            continue;
        }

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

        // The workflows are those at the commit the run checks out; see `event_workflows`.
        let workflows = match event_workflows(
            shared,
            &repo_full_name,
            effective,
            engine_built_merge.as_ref(),
            &resolved_sha,
        )
        .await
        {
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
                triggering_actor: None,
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
                // The delivery already decided this run's tree — GitHub's
                // live merge, the engine's own merge, or the adapter's
                // projection when there is no API to ask — so the submit
                // path must not build a second merge from a local workspace:
                // that merge would need an `origin` the workspace may not
                // have, and would replace the resolved tree.
                no_merge: effective.event == "pull_request",
                // A run whose placement is the engine's own merge must be
                // served from the engine: GitHub has no such commit.
                prebuilt_merge: if is_pull_request_run_event(effective) {
                    engine_built_merge.clone()
                } else {
                    None
                },
                git_bundle_id: None,
            };

            // Pull-request workflows check out the merge commit but GitHub
            // delivers the echo for the head commit. Match the status-check
            // SHA first, so a publication that tested that merge suppresses
            // the PR echo too; one that tested the branch alone does not (see
            // `already_published`). Only what publishing itself causes is an
            // echo (see `is_publish_echo`); any other event at this head is a
            // new request and always runs.
            let echo_sha = submission
                .status_check_sha
                .as_deref()
                .unwrap_or(&submission.sha);
            let tested_by = if is_publish_echo(&effective.event, effective.activity_type.as_deref())
            {
                match crate::github_push::already_published(
                    shared,
                    &repo_full_name,
                    echo_sha,
                    submission.workflow_path.as_deref().unwrap_or_default(),
                    &effective.event,
                )
                .await
                {
                    Ok(tested_by) => tested_by,
                    Err(error) => {
                        error!(
                            ?error,
                            sha = %echo_sha,
                            "failed to read push-back publication state; refusing to submit a possible duplicate"
                        );
                        return WebhookOutcome::TransientError(format!(
                            "failed to read push-back publication state: {error:?}"
                        ));
                    }
                }
            } else {
                None
            };
            if let Some(tested_by) = tested_by {
                info!(
                    workflow = %filename,
                    sha = %echo_sha,
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
/// Defaults to the minimal CI event set (`push`, `pull_request`, `check_run`
/// — the environment review buttons arrive as `check_run.requested_action`,
/// and GitHub only delivers it to Apps subscribed to `check_run`). GitHub
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
    vec![
        "push".to_owned(),
        "pull_request".to_owned(),
        "check_run".to_owned(),
    ]
}

/// Name of the cookie that binds an App-manifest registration to the browser that
/// opened the registration page.
const APP_MANIFEST_STATE_COOKIE: &str = "preloop_app_manifest_state";
/// Path the registration cookie is scoped to: the registration and callback routes.
const APP_MANIFEST_COOKIE_PATH: &str = "/api/v1/github";
/// Lifetime of one registration: the cookie's `Max-Age` and the signed expiry.
const APP_MANIFEST_STATE_TTL_SECS: u64 = 3600;

/// Whether the App-manifest routes are served. Off unless
/// `PRELOOP_ENABLE_APP_REGISTRATION` is `1`, `true`, or `on` (case-insensitive).
fn app_registration_enabled() -> bool {
    let Ok(value) = std::env::var("PRELOOP_ENABLE_APP_REGISTRATION") else {
        return false;
    };
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on"
    )
}

/// The `Host` the browser addressed; the manifest's URLs are built from it.
fn manifest_host(headers: &HeaderMap) -> &str {
    headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost:9090")
}

/// Loopback hosts are served over plain HTTP and every other host over HTTPS. The
/// manifest's scheme and the cookie's `Secure` flag both follow this.
fn manifest_is_https(host: &str) -> bool {
    !(host.contains("localhost") || host.contains("127.0.0.1"))
}

/// Fresh registration nonce: 256 random bits, base64url-encoded.
fn app_manifest_nonce() -> String {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use rand::Rng as _;
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

/// The registration token, `nonce.expires_at.sig`. `sig` is HMAC-SHA256 under the
/// engine's `local_jwt_key` over a domain-separated payload, so the token is not
/// valid as an action ticket, a replay ticket, or a job JWT.
fn app_manifest_state_token(key: &[u8], nonce: &str, expires_at: u64) -> String {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(format!("app-manifest-state:{nonce}.{expires_at}").as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    format!("{nonce}.{expires_at}.{signature}")
}

/// Whether `token` is one this engine minted and has not expired. The signature is
/// checked in constant time.
fn app_manifest_state_is_valid(key: &[u8], token: &str, now: u64) -> bool {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut parts = token.split('.');
    let (Some(nonce), Some(expires_raw), Some(tag_b64), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Ok(expires_at) = expires_raw.parse::<u64>() else {
        return false;
    };
    if now >= expires_at {
        return false;
    }
    let Ok(tag) = URL_SAFE_NO_PAD.decode(tag_b64) else {
        return false;
    };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(key) else {
        return false;
    };
    mac.update(format!("app-manifest-state:{nonce}.{expires_raw}").as_bytes());
    mac.verify_slice(&tag).is_ok()
}

/// The registration cookie's value. A cookie of that name sent more than once is
/// ambiguous, so it counts as absent.
fn app_manifest_cookie_value(headers: &HeaderMap) -> Option<&str> {
    let mut value = None;
    for header in headers.get_all("cookie") {
        for pair in header.to_str().ok()?.split(';') {
            let Some((name, candidate)) = pair.trim().split_once('=') else {
                continue;
            };
            if name == APP_MANIFEST_STATE_COOKIE {
                if value.is_some() {
                    return None;
                }
                value = Some(candidate);
            }
        }
    }
    value
}

/// Equality that compares every byte of equal-length inputs before answering.
fn constant_time_str_eq(left: &str, right: &str) -> bool {
    let diff = left
        .bytes()
        .zip(right.bytes())
        .fold(0_u8, |acc, (a, b)| acc | (a ^ b));
    left.len() == right.len() && diff == 0
}

/// `Set-Cookie` for the registration binding. An empty value with `max_age` 0
/// clears it.
fn app_manifest_cookie(
    value: &str,
    max_age: u64,
    secure: bool,
) -> Result<axum::http::HeaderValue, StatusCode> {
    let secure = if secure { "; Secure" } else { "" };
    axum::http::HeaderValue::from_str(&format!(
        "{APP_MANIFEST_STATE_COOKIE}={value}; HttpOnly; SameSite=Lax; \
         Path={APP_MANIFEST_COOKIE_PATH}; Max-Age={max_age}{secure}"
    ))
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// An HTML response that is never cached and never leaks its URL through `Referer`:
/// the callback URL carries a one-time manifest code.
fn manifest_page(
    status: StatusCode,
    body: String,
    set_cookie: Option<axum::http::HeaderValue>,
) -> axum::response::Response {
    let mut response = (status, axum::response::Html(body)).into_response();
    let headers = response.headers_mut();
    headers.insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    headers.insert(
        "referrer-policy",
        axum::http::HeaderValue::from_static("no-referrer"),
    );
    if let Some(cookie) = set_cookie {
        headers.append("set-cookie", cookie);
    }
    response
}

/// Escape `value` for HTML text and for single- or double-quoted attributes.
fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// POSIX single-quote `value`, so a command pasted from the page passes it through
/// unchanged.
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// Serve the registration page for the GitHub App manifest flow.
///
/// Disabled unless `PRELOOP_ENABLE_APP_REGISTRATION` is set. The page sets a signed
/// `state` cookie and gives GitHub the same token, so the callback can require that
/// the two match.
pub async fn github_register(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
) -> Result<axum::response::Response, StatusCode> {
    if !app_registration_enabled() {
        return Err(StatusCode::NOT_FOUND);
    }
    let host = manifest_host(&headers);
    let secure = manifest_is_https(host);
    let scheme = if secure { "https" } else { "http" };
    let base_url = format!("{}://{}", scheme, host);
    let is_local = !secure;

    let mut manifest_json = serde_json::json!({
        "name": "preloop-local-app",
        "url": base_url,
        "redirect_url": format!("{}/api/v1/github/callback", base_url),
        "public": false,
        "default_permissions": {
            "actions": "read",
            "checks": "write",
            "contents": "read",
            "deployments": "write",
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

    let expires_at = unix_now_secs() + APP_MANIFEST_STATE_TTL_SECS;
    let token = app_manifest_state_token(
        &shared.state.local_jwt_key,
        &app_manifest_nonce(),
        expires_at,
    );
    let cookie = app_manifest_cookie(&token, APP_MANIFEST_STATE_TTL_SECS, secure)?;
    let action_state = crate::actions::percent_encode_path_segment(&token);
    let manifest = html_escape(&manifest_json.to_string());

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>Register GitHub App</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: auto;">
    <h1>Register GitHub App for preloop</h1>
    <p>Click the button below to register a local GitHub App on your GitHub account automatically.</p>
    <form action="https://github.com/settings/apps/new?state={action_state}" method="post">
        <input type="hidden" name="manifest" value='{manifest}'>
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
</html>"#
    );

    Ok(manifest_page(StatusCode::OK, html, Some(cookie)))
}

/// Query parameters for GitHub callback. Both fields are optional on the wire
/// and are parsed after the feature gate, so any request — including one that
/// cannot parse — gets the disabled route's 404 or the generic refusal, never
/// an extractor rejection.
#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    code: Option<String>,
    state: Option<String>,
}

/// Generic refusal for any failed binding check. It names no check, so it tells a
/// prober nothing about which one failed.
const MANIFEST_REJECTED_PAGE: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>Registration not accepted</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: auto;">
    <h1>Registration not accepted</h1>
    <p>This GitHub App registration link is invalid or has expired. Start again from the registration page on this engine.</p>
</body>
</html>"#;

/// Generic page for a failed code exchange. The browser URL still carries the
/// one-time code, so the response is as protected as the handoff page.
const MANIFEST_EXCHANGE_FAILED_PAGE: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>Registration could not be completed</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: auto;">
    <h1>Registration could not be completed</h1>
    <p>GitHub did not return the App credentials. Start again from the registration page on this engine.</p>
</body>
</html>"#;

/// Callback endpoint for GitHub App Manifest conversion.
///
/// The one-time `code` is exchanged only when the registration cookie and the
/// `state` parameter are both present, equal, signed by this engine, and unexpired.
/// Any other request is refused before GitHub is contacted; a failed exchange
/// answers with the same protected page as any refusal.
pub async fn github_callback(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    uri: axum::http::Uri,
) -> Result<axum::response::Response, StatusCode> {
    // The gate runs before query extraction: a request that cannot parse must
    // still see the disabled route's 404, not an extractor rejection.
    if !app_registration_enabled() {
        return Err(StatusCode::NOT_FOUND);
    }
    let params = match Query::<CallbackQuery>::try_from_uri(&uri) {
        Ok(Query(params)) => params,
        Err(_) => {
            warn!("rejected GitHub App manifest callback: malformed query");
            return Ok(manifest_page(
                StatusCode::BAD_REQUEST,
                MANIFEST_REJECTED_PAGE.to_owned(),
                None,
            ));
        }
    };
    let now = unix_now_secs();
    let bound = match (app_manifest_cookie_value(&headers), params.state.as_deref()) {
        (Some(cookie), Some(state)) => {
            constant_time_str_eq(cookie, state)
                && app_manifest_state_is_valid(&shared.state.local_jwt_key, state, now)
        }
        _ => false,
    };
    let code = params.code.as_deref().filter(|code| !code.is_empty());
    let Some(code) = code.filter(|_| bound) else {
        warn!("rejected GitHub App manifest callback: state missing, mismatched, or expired");
        return Ok(manifest_page(
            StatusCode::BAD_REQUEST,
            MANIFEST_REJECTED_PAGE.to_owned(),
            None,
        ));
    };

    let credentials = match exchange_manifest_code(code).await {
        Ok(credentials) => credentials,
        Err(status) => {
            warn!("GitHub App manifest code exchange failed");
            return Ok(manifest_page(
                status,
                MANIFEST_EXCHANGE_FAILED_PAGE.to_owned(),
                None,
            ));
        }
    };
    info!("Successfully registered GitHub App ID: {}", credentials.id);

    // The webhook secret is deliberately never logged: it is the only thing
    // standing between this server and a forged `push` event. It reaches the
    // operator through the one-time HTML handoff below, the same way the
    // private key does, and is configured from there.
    info!(
        has_webhook_secret = credentials.webhook_secret.is_some(),
        "GitHub App credentials received"
    );

    // The registration is complete, so the binding cookie goes too.
    let cleared = app_manifest_cookie("", 0, manifest_is_https(manifest_host(&headers)))?;
    Ok(manifest_page(
        StatusCode::OK,
        app_credentials_page(&credentials),
        Some(cleared),
    ))
}

/// Exchange the one-time manifest `code` for the App's credentials. The code is
/// caller-supplied, so it is encoded as a single path segment.
async fn exchange_manifest_code(code: &str) -> Result<AppManifestConversion, StatusCode> {
    let client = crate::shared_http::CLIENT.clone();
    let api_base = std::env::var("PRELOOP_GITHUB_API_URL")
        .unwrap_or_else(|_| "https://api.github.com".to_owned());
    let url = format!(
        "{}/app-manifests/{}/conversions",
        api_base,
        crate::actions::percent_encode_path_segment(code)
    );
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

    res.json::<AppManifestConversion>()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// The fields of GitHub's manifest-conversion response that the handoff page uses.
#[derive(Deserialize)]
struct AppManifestConversion {
    id: u64,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    owner: Option<AppManifestOwner>,
    pem: String,
    webhook_secret: Option<String>,
}

#[derive(Deserialize)]
struct AppManifestOwner {
    login: String,
}

/// The one-time handoff page: the App's credentials, the owning account to confirm,
/// and the CLI command that binds the credentials to this App. Every value GitHub
/// supplies is HTML-escaped.
fn app_credentials_page(credentials: &AppManifestConversion) -> String {
    let app_id = html_escape(&credentials.id.to_string());
    let owner = match &credentials.owner {
        Some(owner) => format!(
            "<p>This App is owned by <strong>{}</strong>. Confirm that it is the account you \
             registered it with before you install it.</p>",
            html_escape(&owner.login)
        ),
        None => "<p>GitHub did not report the owning account. Check the App's settings page \
                 before you install it.</p>"
            .to_owned(),
    };
    let secret = html_escape(credentials.webhook_secret.as_deref().unwrap_or("none"));
    let command = match credentials.webhook_secret.as_deref() {
        Some(webhook_secret) => format!(
            "preloop setup github --via app --app-id {} --pem-file ./app.pem --webhook-secret {}",
            credentials.id,
            shell_single_quote(webhook_secret)
        ),
        None => format!(
            "preloop setup github --via app --app-id {} --pem-file ./app.pem",
            credentials.id
        ),
    };
    let command = html_escape(&command);
    let pem = html_escape(&credentials.pem);
    let install = match credentials.slug.as_deref() {
        Some(slug) => {
            let url = html_escape(&format!(
                "https://github.com/apps/{}/installations/new",
                crate::actions::percent_encode_path_segment(slug)
            ));
            format!(r#"Then install it on your repositories: <a href="{url}">{url}</a>"#)
        }
        None => "Then install it on your repositories from the App's settings page.".to_owned(),
    };
    format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>GitHub App Registered</title>
</head>
<body style="font-family: sans-serif; padding: 40px; max-width: 800px; margin: auto;">
    <h1 style="color: #2da44e;">GitHub App Registered Successfully!</h1>
    {owner}
    <p><strong>App ID:</strong> {app_id}</p>
    <p><strong>Webhook Secret:</strong> {secret}</p>
    <p><strong>Private Key PEM:</strong></p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px; overflow-x: auto;">{pem}</pre>
    <p>Save the key to <code>./app.pem</code>, then run this on the machine running the engine and restart it:</p>
    <pre style="background: #f6f8fa; padding: 16px; border-radius: 6px; overflow-x: auto;">{command}</pre>
    <p><strong>Do not</strong> set <code>PRELOOP_WEBHOOK_SECRET</code> for this App. The per-App secret above is bound to this App's installations. The variable is the single default-App slot and cannot address a second App.</p>
    <p>{install}</p>
    <p>On the machine running the engine, <code>preloop setup github --via app</code>
       does all of this without copying anything out of a browser.</p>
</body>
</html>"#
    )
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
        assert_eq!(defaults, vec!["push", "pull_request", "check_run"]);
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
        assert_eq!(
            manifest_default_events(),
            vec!["push", "pull_request", "check_run"]
        );
        unsafe { std::env::remove_var("PRELOOP_GITHUB_APP_DEFAULT_EVENTS") };
    }

    /// App-manifest registration: the feature flag, the cookie-bound `state`, the
    /// one-time handoff page, and the conversion request sent upstream.
    mod manifest_registration {
        use super::*;

        const FLAG: &str = "PRELOOP_ENABLE_APP_REGISTRATION";

        /// GitHub's manifest-conversion response for an App owned by `owner`.
        fn conversion_response(owner: &str, webhook_secret: &str) -> Value {
            serde_json::json!({
                "id": 987654,
                "slug": "preloop-local-app",
                "owner": { "login": owner },
                "pem": "-----BEGIN RSA PRIVATE KEY-----\nMOCK-KEY\n-----END RSA PRIVATE KEY-----",
                "webhook_secret": webhook_secret,
            })
        }

        /// A stub GitHub, the production router, and the environment guards that keep
        /// both in place for one test. Fields drop in declaration order, so the
        /// environment is restored before the lock is released.
        struct Harness {
            app: axum::Router,
            state: AppState,
            requests: StubRequests,
            _vars: Vec<crate::state::TestEnvVar>,
            _env: tokio::sync::MutexGuard<'static, ()>,
            _temp: tempfile::TempDir,
        }

        async fn harness(response: Value, registration: Option<&str>) -> Harness {
            harness_at(response, 201, registration).await
        }

        /// [`harness`] with control over the conversion endpoint's status.
        async fn harness_at(
            response: Value,
            upstream_status: u16,
            registration: Option<&str>,
        ) -> Harness {
            let env = crate::state::GITHUB_ENV_LOCK.lock().await;
            let (api, requests) =
                start_github_stub(move |_, _, _| (upstream_status, vec![], response.clone())).await;
            let flag = match registration {
                Some(value) => crate::state::TestEnvVar::set(FLAG, value),
                None => crate::state::TestEnvVar::unset(FLAG),
            };
            let vars = vec![
                crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api),
                flag,
            ];
            let temp = tempfile::tempdir().unwrap();
            let state = AppState::new(temp.path().join("state")).await.unwrap();
            let app = crate::routes::app(state.clone(), CancellationToken::new());
            Harness {
                app,
                state,
                requests,
                _vars: vars,
                _env: env,
                _temp: temp,
            }
        }

        /// One GET through the production router, with an optional `Cookie` and `Host`.
        async fn get(
            app: &axum::Router,
            uri: &str,
            cookie: Option<&str>,
            host: Option<&str>,
        ) -> axum::response::Response {
            let mut request = Request::builder().method(Method::GET).uri(uri);
            if let Some(cookie) = cookie {
                request = request.header("cookie", cookie);
            }
            if let Some(host) = host {
                request = request.header("host", host);
            }
            app.clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap()
        }

        async fn text(response: axum::response::Response) -> String {
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        }

        fn set_cookie_of(response: &axum::response::Response) -> String {
            let value = &response.headers()["set-cookie"];
            value.to_str().unwrap().to_owned()
        }

        /// The `Cookie` header and the `state` token of a binding this engine would
        /// issue under `nonce`, valid for ten minutes.
        fn binding(key: &[u8], nonce: &str) -> (String, String) {
            let token = app_manifest_state_token(key, nonce, unix_now_secs() + 600);
            (format!("preloop_app_manifest_state={token}"), token)
        }

        fn upstream(requests: &StubRequests) -> Vec<(String, String, Value)> {
            requests.lock().iter().cloned().collect()
        }

        #[tokio::test]
        async fn flag_accepts_only_the_documented_on_values() {
            let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
            for value in ["1", "true", "TRUE", " on ", "On"] {
                let _flag = crate::state::TestEnvVar::set(FLAG, value);
                assert!(
                    app_registration_enabled(),
                    "{value:?} should enable registration"
                );
            }
            for value in ["", "0", "false", "off", "yes"] {
                let _flag = crate::state::TestEnvVar::set(FLAG, value);
                assert!(
                    !app_registration_enabled(),
                    "{value:?} should not enable registration"
                );
            }
            let _unset = crate::state::TestEnvVar::unset(FLAG);
            assert!(!app_registration_enabled());
        }

        #[tokio::test]
        async fn routes_are_not_found_while_registration_is_disabled() {
            let h = harness(conversion_response("acme", "s"), None).await;

            let register = get(&h.app, "/api/v1/github/register", None, None).await;
            assert_eq!(register.status(), StatusCode::NOT_FOUND);
            assert!(register.headers().get("set-cookie").is_none());

            let (cookie, token) = binding(&h.state.local_jwt_key, "nonce-disabled");
            let uri = format!("/api/v1/github/callback?code=c1&state={token}");
            let callback = get(&h.app, &uri, Some(cookie.as_str()), None).await;
            assert_eq!(callback.status(), StatusCode::NOT_FOUND);
            // A query that cannot parse must still see the disabled route's
            // 404, not an extractor rejection.
            let malformed = get(
                &h.app,
                "/api/v1/github/callback?code=c1&code=c2&state=x",
                None,
                None,
            )
            .await;
            assert_eq!(malformed.status(), StatusCode::NOT_FOUND);
            assert!(
                upstream(&h.requests).is_empty(),
                "a disabled route must not reach GitHub"
            );
        }

        #[tokio::test]
        async fn callback_without_a_matching_state_never_reaches_github() {
            let h = harness(conversion_response("acme", "s"), Some("1")).await;
            let key = &h.state.local_jwt_key;
            let (cookie, token) = binding(key, "nonce-good");
            let (_, other) = binding(key, "nonce-other");
            let expiry = unix_now_secs() + 600;
            let forged = app_manifest_state_token(b"not-this-engine", "nonce-good", expiry);
            let refused: [(String, Option<String>); 6] = [
                ("/api/v1/github/callback?code=c1".to_owned(), None),
                (
                    format!("/api/v1/github/callback?code=c1&state={token}"),
                    None,
                ),
                (
                    "/api/v1/github/callback?code=c1".to_owned(),
                    Some(cookie.clone()),
                ),
                (
                    format!("/api/v1/github/callback?code=c1&state={other}"),
                    Some(cookie.clone()),
                ),
                (
                    format!("/api/v1/github/callback?code=c1&state={forged}"),
                    Some(format!("preloop_app_manifest_state={forged}")),
                ),
                (
                    format!("/api/v1/github/callback?code=c1&state={token}"),
                    Some(format!("{cookie}; {cookie}")),
                ),
            ];
            for (uri, cookie_header) in &refused {
                let response = get(&h.app, uri, cookie_header.as_deref(), None).await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            }
            assert!(
                upstream(&h.requests).is_empty(),
                "no refused callback may reach GitHub"
            );
        }

        #[tokio::test]
        async fn expired_state_is_refused_even_when_signed_and_echoed() {
            let h = harness(conversion_response("acme", "s"), Some("1")).await;
            let expired = app_manifest_state_token(
                &h.state.local_jwt_key,
                "nonce-old",
                unix_now_secs().saturating_sub(1),
            );
            let cookie = format!("preloop_app_manifest_state={expired}");
            let uri = format!("/api/v1/github/callback?code=c1&state={expired}");
            let response = get(&h.app, &uri, Some(cookie.as_str()), None).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(
                upstream(&h.requests).is_empty(),
                "an expired state must not reach GitHub"
            );
        }

        /// A malformed query gets the generic refusal — never an extractor
        /// rejection — and never reaches GitHub.
        #[tokio::test]
        async fn malformed_query_gets_the_generic_refusal() {
            let h = harness(conversion_response("acme", "s"), Some("1")).await;
            let response = get(
                &h.app,
                "/api/v1/github/callback?code=c1&code=c2&state=x",
                None,
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                response.headers()["cache-control"].to_str().unwrap(),
                "no-store"
            );
            assert!(text(response).await.contains("Registration not accepted"));
            assert!(
                upstream(&h.requests).is_empty(),
                "a malformed query must not reach GitHub"
            );
        }

        /// A failed code exchange answers with the protected page: the browser
        /// URL still carries the one-time code, so the response is never
        /// cacheable and never leaks through `Referer`.
        #[tokio::test]
        async fn exchange_failure_renders_a_protected_page() {
            let h = harness_at(conversion_response("acme", "s"), 500, Some("1")).await;
            let (cookie, token) = binding(&h.state.local_jwt_key, "nonce-exchange");
            let uri = format!("/api/v1/github/callback?code=the-code&state={token}");
            let response = get(&h.app, &uri, Some(cookie.as_str()), None).await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                response.headers()["cache-control"].to_str().unwrap(),
                "no-store"
            );
            assert_eq!(
                response.headers()["referrer-policy"].to_str().unwrap(),
                "no-referrer"
            );
            let page = text(response).await;
            assert!(page.contains("Registration could not be completed"));
            assert_eq!(
                upstream(&h.requests).len(),
                1,
                "the bound exchange is attempted exactly once"
            );
        }

        #[tokio::test]
        async fn round_trip_hands_off_a_cli_command_and_clears_the_binding() {
            let response = conversion_response("acme-admin", "mock-webhook-secret-xyz");
            let h = harness(response, Some("1")).await;

            // Loopback hosts are plain HTTP, so their cookie is not `Secure`.
            let local = get(&h.app, "/api/v1/github/register", None, None).await;
            assert!(!set_cookie_of(&local).contains("Secure"));

            // Over HTTPS the cookie is HttpOnly, Lax, scoped to the GitHub routes, and
            // lives for an hour.
            let register = get(
                &h.app,
                "/api/v1/github/register",
                None,
                Some("preloop.example.com"),
            )
            .await;
            assert_eq!(register.status(), StatusCode::OK);
            assert_eq!(
                register.headers()["cache-control"].to_str().unwrap(),
                "no-store"
            );
            let set_cookie = set_cookie_of(&register);
            for attribute in [
                "HttpOnly",
                "SameSite=Lax",
                "Path=/api/v1/github",
                "Max-Age=3600",
                "Secure",
            ] {
                assert!(
                    set_cookie.contains(attribute),
                    "missing {attribute}: {set_cookie}"
                );
            }
            let cookie = set_cookie.split(';').next().unwrap().to_owned();
            let token = cookie
                .strip_prefix("preloop_app_manifest_state=")
                .unwrap()
                .to_owned();
            let page = text(register).await;
            assert!(
                page.contains(&format!(
                    "https://github.com/settings/apps/new?state={token}"
                )),
                "the form must send the token the cookie carries"
            );

            // GitHub redirects the same browser back, echoing the state.
            let uri = format!("/api/v1/github/callback?code=the-code&state={token}");
            let callback = get(
                &h.app,
                &uri,
                Some(cookie.as_str()),
                Some("preloop.example.com"),
            )
            .await;
            assert_eq!(callback.status(), StatusCode::OK);
            assert_eq!(
                callback.headers()["cache-control"].to_str().unwrap(),
                "no-store"
            );
            assert_eq!(
                callback.headers()["referrer-policy"].to_str().unwrap(),
                "no-referrer"
            );
            assert!(
                set_cookie_of(&callback).contains("Max-Age=0"),
                "the binding is cleared"
            );
            let page = text(callback).await;
            assert!(page.contains("GitHub App Registered Successfully!"));
            assert!(page.contains("987654"));
            assert!(
                page.contains("acme-admin"),
                "the owner is shown for confirmation"
            );
            assert!(
                page.contains("--webhook-secret &#39;mock-webhook-secret-xyz&#39;"),
                "the handoff must pass the secret on the CLI"
            );
            assert!(!page.contains("export PRELOOP_WEBHOOK_SECRET"));

            let calls = upstream(&h.requests);
            assert_eq!(
                calls.len(),
                1,
                "one registration makes one conversion request"
            );
            assert_eq!(calls[0].0, "POST");
            assert_eq!(calls[0].1, "/app-manifests/the-code/conversions");
        }

        #[tokio::test]
        async fn code_stays_one_percent_encoded_upstream() {
            let h = harness(conversion_response("acme", "s"), Some("1")).await;
            let (cookie, token) = binding(&h.state.local_jwt_key, "nonce-code");
            // The code decodes to `a/b?c&d`; none of those characters may split the path.
            let uri = format!("/api/v1/github/callback?code=a%2Fb%3Fc%26d&state={token}");
            let response = get(&h.app, &uri, Some(cookie.as_str()), None).await;
            assert_eq!(response.status(), StatusCode::OK);
            let calls = upstream(&h.requests);
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].1, "/app-manifests/a%2Fb%3Fc%26d/conversions");
        }

        #[tokio::test]
        async fn handoff_page_escapes_every_value_github_supplies() {
            let hostile = serde_json::json!({
                "id": 42,
                "slug": "<img src=x>",
                "owner": { "login": "<b>acme</b>" },
                "pem": "<script>alert(1)</script>",
                "webhook_secret": "s3cr'et<i>",
            });
            let h = harness(hostile, Some("1")).await;
            let (cookie, token) = binding(&h.state.local_jwt_key, "nonce-escape");
            let uri = format!("/api/v1/github/callback?code=c1&state={token}");
            let response = get(&h.app, &uri, Some(cookie.as_str()), None).await;
            assert_eq!(response.status(), StatusCode::OK);
            let page = text(response).await;
            for raw in ["<script>", "<img", "<b>acme", "<i>"] {
                assert!(!page.contains(raw), "{raw} reached the page unescaped");
            }
            assert!(page.contains("&lt;b&gt;acme&lt;/b&gt;"));
            assert!(page.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
            assert!(page.contains("s3cr&#39;et&lt;i&gt;"));
            // The slug only reaches the install link percent-encoded.
            assert!(page.contains("%3Cimg%20src%3Dx%3E"));
        }

        #[test]
        fn state_token_is_bound_to_nonce_expiry_and_key() {
            let key: &[u8] = b"engine-local-jwt-key";
            let now: u64 = 1_800_000_000;
            let token = app_manifest_state_token(key, "nonce", now + 600);
            assert!(app_manifest_state_is_valid(key, &token, now));
            // The nonce, the expiry, and the engine key are all covered by the signature.
            assert!(!app_manifest_state_is_valid(
                key,
                &token.replacen("nonce", "other", 1),
                now
            ));
            let longer = (now + 6_000).to_string();
            let extended = token.replacen(&(now + 600).to_string(), &longer, 1);
            assert!(!app_manifest_state_is_valid(key, &extended, now));
            assert!(!app_manifest_state_is_valid(
                b"another-engine-key",
                &token,
                now
            ));
            // Expired from the expiry second onward.
            assert!(!app_manifest_state_is_valid(key, &token, now + 600));
        }
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

    // ── Fresh merge resolution ─────────────────────────────────────────
    //
    // GitHub's merge sha can lag a push. Accepted merges pin the payload head
    // as second parent; a matching-head conflict skips PR runs; fallback
    // builds the engine's own merge.

    /// A workspace holding a single workflow, ready to be committed into a
    /// pull-request graph.
    fn pr_workspace(temp: &tempfile::TempDir, workflow: &str) -> std::path::PathBuf {
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(ws_dir.join(".github/workflows/ci.yml"), workflow).unwrap();
        ws_dir
    }

    /// Build a pull-request-shaped git graph in `ws_dir`, returning
    /// `(head, stale_merge, current_merge, current_base)`.
    ///
    /// `head` is the payload's `pull_request.head.sha`; `stale_merge` is the
    /// previous head's test merge — exactly what the payload still carries on
    /// `synchronize`; `current_merge` is the live merge of `head` into the
    /// current base tip, and `current_base` is that first parent. All four
    /// values are real commits, so the local workspace can resolve each of them.
    fn build_pull_request_graph(ws_dir: &std::path::Path) -> (String, String, String, String) {
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["config", "user.email", "github-tests@example.invalid"][..],
            &["config", "user.name", "GitHub Tests"][..],
        ] {
            git_output(ws_dir, args);
        }
        git_output(ws_dir, &["add", "-A"]);
        git_output(ws_dir, &["commit", "-qm", "base"]);
        git_output(ws_dir, &["checkout", "-q", "-b", "feature"]);
        git_output(ws_dir, &["commit", "-q", "--allow-empty", "-m", "head 1"]);
        git_output(ws_dir, &["checkout", "-q", "main"]);
        git_output(
            ws_dir,
            &[
                "merge",
                "-q",
                "--no-ff",
                "-m",
                "previous test merge",
                "feature",
            ],
        );
        let stale_merge = git_output(ws_dir, &["rev-parse", "HEAD"]);
        git_output(ws_dir, &["checkout", "-q", "feature"]);
        git_output(ws_dir, &["commit", "-q", "--allow-empty", "-m", "head 2"]);
        let head = git_output(ws_dir, &["rev-parse", "HEAD"]);
        git_output(ws_dir, &["checkout", "-q", "main"]);
        git_output(
            ws_dir,
            &[
                "merge",
                "-q",
                "--no-ff",
                "-m",
                "current test merge",
                "feature",
            ],
        );
        let current_merge = git_output(ws_dir, &["rev-parse", "HEAD"]);
        let current_base = git_output(ws_dir, &["rev-parse", "HEAD^1"]);
        // `WebhookFixture` commits the workspace itself; leave it something to
        // commit so that fixture step cannot fail on an empty tree.
        std::fs::write(ws_dir.join(".fixture-marker"), "marker\n").unwrap();
        (head, stale_merge, current_merge, current_base)
    }

    /// Publish the workspace's graph as the engine's own git upstream for
    /// `owner/repo`, so a self-built merge fetches from it instead of GitHub.
    ///
    /// `webhook_merge_source` fetches `{server_url}/{owner}/{repo}.git` from
    /// a live engine; here the server URL is the returned repository's
    /// grandparent (`<state>/test-upstream`), a `file://` clone of the same
    /// graph. Returns the bare repository's path.
    fn publish_merge_upstream(state: &AppState, ws_dir: &std::path::Path) -> std::path::PathBuf {
        let root = state.state_dir.join("test-upstream");
        let upstream = root.join("owner/repo.git");
        std::fs::create_dir_all(upstream.parent().unwrap()).unwrap();
        git_output(
            ws_dir,
            &["clone", "--bare", "-q", ".", upstream.to_str().unwrap()],
        );
        upstream
    }

    /// Point the engine at `upstream_root` (`file://`) as its git server.
    fn serve_merges_from(state: &mut AppState, upstream_root: &std::path::Path) {
        state.github_urls.server_url = format!("file://{}", upstream_root.display());
    }

    /// Assert `sha` exists in `mirror` and merges exactly `(base, head)`.
    fn assert_engine_merge(mirror: &std::path::Path, sha: &str, base: &str, head: &str) {
        let parents = git_output(mirror, &["log", "-1", "--format=%P", sha]);
        assert_eq!(
            parents,
            format!("{base} {head}"),
            "run sha {sha} must be the engine's merge of {head} into {base}"
        );
    }

    /// The delivery mirror `webhook_merge_source` created for `owner/repo`.
    fn merge_mirror(state: &AppState) -> std::path::PathBuf {
        let root = state.state_dir.join("checkout-cache/repositories");
        let mirrors: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("merge-"))
            })
            .collect();
        assert_eq!(mirrors.len(), 1, "exactly one delivery merge mirror");
        mirrors.into_iter().next().unwrap()
    }

    /// A `pull_request` webhook payload whose carried merge sha is the
    /// previous head's test merge (what GitHub sends on `synchronize`).
    fn stale_pull_request_payload(head: &str, stale_merge: &str, base_sha: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "action": "synchronize",
            "number": 42,
            "pull_request": {
                "number": 42,
                "head": { "ref": "feature", "sha": head, "repo": { "fork": false } },
                "base": { "ref": "main", "sha": base_sha },
                "merge_commit_sha": stale_merge,
            },
            "repository": { "full_name": "owner/repo", "default_branch": "main" },
        }))
        .unwrap()
    }

    /// The stub's pull-request-files answer: these fixtures declare no
    /// `paths:` filters, so an empty list is always enough.
    fn pr_files_answer() -> Value {
        serde_json::json!([])
    }

    /// The payload's stale merge sha must be replaced by GitHub's live test
    /// merge, and the stale sha must never be consulted.
    #[tokio::test]
    async fn stale_payload_merge_sha_is_replaced_with_the_live_test_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let merge_state = serde_json::json!({
            "mergeable": true,
            "merge_commit_sha": current_merge.clone(),
            "head": { "sha": head.clone() },
            "base": { "sha": current_base.clone() },
        });
        let merge_commit = serde_json::json!({
            "sha": current_merge.clone(),
            "parents": [{ "sha": current_base.clone() }, { "sha": head.clone() }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{current_merge}");
        let (api, requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (200, vec![], merge_state.clone());
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-stale-merge", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1, "exactly the pull_request run");
        let run = inner.runs.values().next().unwrap();
        assert_eq!(
            run.submission.sha, current_merge,
            "the run must check out GitHub's live test merge, not the payload's stale sha"
        );
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        assert_eq!(
            run.submission.status_check_sha.as_deref(),
            Some(head.as_str()),
            "check runs still attach to the PR head"
        );
        drop(inner);
        assert!(
            !requests
                .lock()
                .iter()
                .any(|(_, path, _)| path.contains(&stale_merge)),
            "the stale payload merge sha must never be trusted — not even inspected"
        );
    }

    /// A transient probe failure and GitHub's "still computing" answer are
    /// both polled through, not treated as final.
    #[tokio::test]
    async fn mergeability_unknown_is_polled_until_the_test_merge_is_ready() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let merge_state = serde_json::json!({
            "mergeable": true,
            "merge_commit_sha": current_merge.clone(),
            "head": { "sha": head.clone() },
            "base": { "sha": current_base.clone() },
        });
        let merge_commit = serde_json::json!({
            "sha": current_merge.clone(),
            "parents": [{ "sha": current_base.clone() }, { "sha": head.clone() }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{current_merge}");
        let api_head = head.clone();
        let api_base = current_base.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return match polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    // A transient 5xx must not decide anything…
                    0 => (500, vec![], serde_json::json!({ "message": "boom" })),
                    // …nor must GitHub's "still computing" answer.
                    1 => (
                        200,
                        vec![],
                        serde_json::json!({
                            "mergeable": null,
                            "merge_commit_sha": null,
                            "head": { "sha": api_head.clone() },
                            "base": { "sha": api_base.clone() },
                        }),
                    ),
                    _ => (200, vec![], merge_state.clone()),
                };
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
        };
        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-poll", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert_eq!(
            polls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the probe must be retried past the transient failure and the unknown state"
        );
        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        assert_eq!(
            inner.runs.values().next().unwrap().submission.sha,
            current_merge
        );
    }

    /// GitHub creates no pull_request runs for a conflicted PR; the
    /// pull_request_target projection still runs on the base.
    #[tokio::test]
    async fn conflicted_pull_request_creates_no_pr_runs_but_still_runs_target() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        std::fs::write(
            ws_dir.join(".github/workflows/pre.yml"),
            "on: pull_request_target\njobs:\n  base:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo base\n",
        )
        .unwrap();
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);
        let api_head = head.clone();
        let api_base = current_base.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": false,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": api_base.clone() },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-conflict", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "a conflicted PR creates no pull_request runs"
        );
        let run = inner.runs.values().next().unwrap();
        assert_eq!(
            run.submission.event, "pull_request_target",
            "pull_request_target is unaffected by the conflict"
        );
        drop(inner);
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-pr-conflict")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "skipping on a conflict is a completed delivery"
        );
    }
    /// A pointer that moved to a newer head must not suppress the delivery
    /// for the older head: GitHub will never build that head's merge, so the
    /// engine builds merge(current base tip, payload head) itself and runs
    /// it. Never the bare head ref, and never the newer head's tree.
    #[tokio::test]
    async fn head_race_builds_and_runs_the_engines_own_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        // GitHub's pointer moved on before this delivery landed: it reports a
        // newer head and offers no merge this payload could use.
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let head_for_stub = head.clone();
        let (api, requests) = start_github_stub(move |_method, path, _body| {
            if path == format!("/repos/owner/repo/commits/{head_for_stub}") {
                return (
                    200,
                    vec![],
                    serde_json::json!({"commit": {"message": "regular change"}}),
                );
            }
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": false,
                        "merge_commit_sha": null,
                        "head": { "sha": "newer-head" },
                        "base": { "sha": "newer-base" },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
        };
        // The engine fetches the current base tip from its own git upstream.
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-newer-conflict", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1, "the older payload head's run");
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.event, "pull_request");
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        assert_eq!(
            run.submission.status_check_sha.as_deref(),
            Some(head.as_str()),
            "check runs still attach to the PR head"
        );
        let merge_sha = run.submission.sha.clone();
        drop(inner);
        assert_ne!(merge_sha, head, "never the bare head ref");
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "a head the API never offers a merge for is polled through the budget, not decided on first sight"
        );
        let commit_paths: Vec<String> = requests
            .lock()
            .iter()
            .filter(|(_, path, _)| path.contains("/commits/"))
            .map(|(_, path, _)| path.clone())
            .collect();
        assert_eq!(
            commit_paths,
            vec![format!("/repos/owner/repo/commits/{head}")],
            "only the head commit is fetched for [skip ci] evaluation"
        );
    }

    /// A merge-state API that keeps failing cannot block the delivery: after
    /// the bounded wait the engine builds and runs its own merge of the
    /// current base tip and the payload head — never the stale payload sha,
    /// never the bare head ref.
    #[tokio::test]
    async fn api_failure_within_the_budget_builds_and_runs_the_engines_own_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (500, vec![], serde_json::json!({ "message": "boom" }));
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        // A persistent 5xx would trip the shared breaker; this test is about
        // the resolver's own fallback, so keep the breaker out of the way.
        fixture.state.github_breakers =
            Arc::new(crate::github_breaker::GithubBreakerRegistry::new(
                crate::github_breaker::BreakerConfig {
                    enabled: false,
                    ..Default::default()
                },
            ));
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-probe-failure", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "the probe must be retried within the budget before falling back"
        );
        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        assert_eq!(
            run.submission.status_check_sha.as_deref(),
            Some(head.as_str()),
            "check runs still attach to the PR head"
        );
        let merge_sha = run.submission.sha.clone();
        drop(inner);
        assert_ne!(merge_sha, stale_merge, "never the stale payload sha");
        assert_ne!(merge_sha, head, "never the bare head ref");
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-pr-probe-failure")
            .await
            .unwrap()
            .expect("delivery row must exist");
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "the delivery settles once the engine's merge runs"
        );
    }

    /// A merge commit that is not the merge of the payload head is never
    /// used, even when the API still points at it (the push-race guard): the
    /// delivery polls through the budget and the engine builds its own merge.
    #[tokio::test]
    async fn a_merge_commit_from_another_head_is_never_used() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);
        // The previous head: the second parent of the merge the API offers.
        let previous_head = git_output(&ws_dir, &["rev-parse", "feature~1"]);

        let merge_state = serde_json::json!({
            "mergeable": true,
            "merge_commit_sha": stale_merge.clone(),
            "head": { "sha": head.clone() },
            "base": { "sha": current_base.clone() },
        });
        let merge_commit = serde_json::json!({
            "sha": stale_merge.clone(),
            "parents": [{ "sha": current_base.clone() }, { "sha": previous_head }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{stale_merge}");
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (200, vec![], merge_state.clone());
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_millis(100),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-other-head", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        let merge_sha = run.submission.sha.clone();
        let git_ref = run.submission.git_ref.clone();
        drop(inner);
        assert_ne!(
            merge_sha, stale_merge,
            "a merge of another head must never be checked out"
        );
        assert_eq!(git_ref, "refs/pull/42/merge");
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
    }

    /// A merge with the payload head but an older first parent is not a match
    /// for the API's current base. Keep polling, then build the delivery's
    /// exact head against the target branch's current tip in the engine.
    #[tokio::test]
    async fn a_merge_against_an_older_base_is_rejected_and_rebuilt() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, old_base) = build_pull_request_graph(&ws_dir);
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let merge_state = serde_json::json!({
            "mergeable": true,
            "merge_commit_sha": stale_merge.clone(),
            "head": { "sha": head.clone() },
            "base": { "sha": "new-base" },
        });
        let merge_commit = serde_json::json!({
            "sha": stale_merge.clone(),
            "parents": [{ "sha": old_base }, { "sha": head.clone() }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{stale_merge}");
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (200, vec![], merge_state.clone());
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let current_base = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-obsolete-base", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "the stale-base candidate must be rejected and polled through the budget"
        );
        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        assert_eq!(
            run.submission.status_check_sha.as_deref(),
            Some(head.as_str()),
            "check runs remain pinned to the webhook head"
        );
        let merge_sha = run.submission.sha.clone();
        let served_repository = run
            .workspace_snapshot
            .as_ref()
            .expect("the engine-built merge is attached to the run")
            .repository
            .clone();
        drop(inner);
        assert_ne!(
            merge_sha, stale_merge,
            "the old-base GitHub merge is rejected"
        );
        assert_ne!(merge_sha, head, "never fall back to the bare PR head");
        assert_engine_merge(
            &merge_mirror(&fixture.state),
            &merge_sha,
            &current_base,
            &head,
        );
        let checkout = temp.path().join("engine-served-checkout");
        std::fs::create_dir(&checkout).unwrap();
        git_output(&checkout, &["init", "-q"]);
        let served_path = fixture.state.state_dir.join(served_repository);
        git_output(
            &checkout,
            &["fetch", served_path.to_str().unwrap(), "refs/pull/42/merge"],
        );
        git_output(&checkout, &["checkout", "--detach", "FETCH_HEAD"]);
        assert_eq!(git_output(&checkout, &["rev-parse", "HEAD"]), merge_sha);
        assert_engine_merge(&checkout, &merge_sha, &current_base, &head);
    }

    /// The measured rapid-push race: the API reports a newer head while
    /// `merge_commit_sha` still names *this* delivery's head's merge. The
    /// candidate's parents are inspected first, so the valid merge is kept —
    /// not discarded for a head race that no longer concerns this delivery.
    #[tokio::test]
    async fn head_race_keeps_this_heads_merge_when_the_pointer_still_names_it() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let merge_state = serde_json::json!({
            "mergeable": null,
            "merge_commit_sha": current_merge.clone(),
            "head": { "sha": "newer-head" },
            "base": { "sha": current_base.clone() },
        });
        let merge_commit = serde_json::json!({
            "sha": current_merge.clone(),
            "parents": [{ "sha": current_base.clone() }, { "sha": head.clone() }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{current_merge}");
        let (api, requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (200, vec![], merge_state.clone());
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-race-keep", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        assert_eq!(
            run.submission.sha, current_merge,
            "this head's live merge is used even though the API's head already moved"
        );
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        drop(inner);
        assert_eq!(
            requests
                .lock()
                .iter()
                .filter(|(_, path, _)| path == "/repos/owner/repo/pulls/42")
                .count(),
            1,
            "the first probe already knows the answer; nothing waits"
        );
    }

    /// GitHub never surfaces the merge for this head: the budget expires and
    /// the delivery runs on the engine's own merge of the current base tip.
    #[tokio::test]
    async fn budget_expiry_builds_and_runs_the_engines_own_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        // GitHub is mid-recompute for this very head: it names the head but
        // has no merge to offer, forever.
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let api_head = head.clone();
        let api_base = current_base.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": api_base.clone() },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_secs(5),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-budget", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "the delivery polls before giving up on GitHub"
        );
        let inner = fixture.state.test_tx().await;
        let runs = inner.runs.len();
        drop(inner);
        if runs != 1 {
            let record = fixture
                .state
                .backend
                .get_webhook_delivery("delivery-pr-budget")
                .await
                .unwrap()
                .expect("delivery row must exist");
            panic!(
                "expected one run, got {runs}: delivery state={:?} last_error={:?}",
                record.state, record.last_error
            );
        }
        let inner = fixture.state.test_tx().await;
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        let merge_sha = run.submission.sha.clone();
        drop(inner);
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-pr-budget")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.state, WebhookDeliveryStatus::Done);
    }

    /// The engine's own merge can conflict. When it does, the delivery gets
    /// GitHub's conflict treatment: no pull_request runs, while
    /// pull_request_target still runs on the base.
    #[tokio::test]
    async fn engine_merge_conflict_skips_pull_request_runs() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        std::fs::write(
            ws_dir.join(".github/workflows/pre.yml"),
            "on: pull_request_target\njobs:\n  base:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo base\n",
        )
        .unwrap();
        let (_head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);
        // The head and the base tip both touch the same file: the engine's
        // merge of the two conflicts.
        git_output(&ws_dir, &["checkout", "-q", "feature"]);
        std::fs::write(ws_dir.join("conflict.txt"), "feature side\n").unwrap();
        git_output(&ws_dir, &["add", "-A"]);
        git_output(&ws_dir, &["commit", "-qm", "feature side"]);
        let head = git_output(&ws_dir, &["rev-parse", "HEAD"]);
        git_output(&ws_dir, &["checkout", "-q", "main"]);
        std::fs::write(ws_dir.join("conflict.txt"), "base side\n").unwrap();
        git_output(&ws_dir, &["add", "-A"]);
        git_output(&ws_dir, &["commit", "-qm", "base side"]);
        // `WebhookFixture` commits the workspace itself; leave it something to
        // commit so that fixture step cannot fail on an empty tree.
        std::fs::write(ws_dir.join(".fixture-marker"), "marker\n").unwrap();

        let api_head = head.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": "some-base" },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_millis(60),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-self-conflict", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(
            inner.runs.len(),
            1,
            "a conflicting engine merge creates no pull_request runs"
        );
        assert_eq!(
            inner.runs.values().next().unwrap().submission.event,
            "pull_request_target"
        );
        drop(inner);
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-pr-self-conflict")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Done,
            "the conflict gate is a completed delivery"
        );
    }

    /// A permanent mirror failure (the payload's base branch does not exist)
    /// dead-letters the delivery instead of retrying it into the same wall.
    #[tokio::test]
    async fn unfetchable_base_branch_dead_letters_the_delivery() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let api_head = head.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": "some-base" },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_millis(60),
            poll_interval: Duration::from_millis(20),
        };
        // An upstream with no `main` at all.
        let root = fixture.state.state_dir.join("test-upstream");
        let upstream = root.join("owner/repo.git");
        std::fs::create_dir_all(upstream.parent().unwrap()).unwrap();
        git_output(
            &ws_dir,
            &["init", "--bare", "-q", upstream.to_str().unwrap()],
        );
        serve_merges_from(&mut fixture.state, &root);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-no-base", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert!(
            inner.runs.is_empty(),
            "a merge that cannot be built never places a run"
        );
        drop(inner);
        let record = fixture
            .state
            .backend
            .get_webhook_delivery("delivery-pr-no-base")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Failed,
            "a base the mirror can never hold dead-letters instead of retrying"
        );
    }

    /// `pull_request_review` uses the same merge-ref placement and gets the
    /// same fresh resolution.
    #[tokio::test]
    async fn pull_request_review_uses_the_live_test_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request_review\njobs:\n  review:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo reviewed\n",
        );
        let (head, stale_merge, current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let merge_state = serde_json::json!({
            "mergeable": true,
            "merge_commit_sha": current_merge.clone(),
            "head": { "sha": head.clone() },
            "base": { "sha": current_base.clone() },
        });
        let merge_commit = serde_json::json!({
            "sha": current_merge.clone(),
            "parents": [{ "sha": current_base.clone() }, { "sha": head.clone() }],
        });
        let commits_path = format!("/repos/owner/repo/commits/{current_merge}");
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (200, vec![], merge_state.clone());
            }
            if path == commits_path.as_str() {
                return (200, vec![], merge_commit.clone());
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = serde_json::to_vec(&serde_json::json!({
            "action": "submitted",
            "pull_request": {
                "number": 42,
                "head": { "ref": "feature", "sha": head, "repo": { "fork": false } },
                "base": { "ref": "main", "sha": current_base },
                "merge_commit_sha": stale_merge,
            },
            "review": { "state": "approved" },
            "repository": { "full_name": "owner/repo", "default_branch": "main" },
        }))
        .unwrap();
        assert_eq!(
            fixture
                .post_body(
                    "delivery-review-stale-merge",
                    Some("pull_request_review"),
                    &payload
                )
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.event, "pull_request_review");
        assert_eq!(
            run.submission.sha, current_merge,
            "the review run must check out GitHub's live test merge"
        );
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
    }

    /// Without GitHub credentials (local-workspace development, native
    /// webhook replays) there is no API to ask, so the adapter's projection
    /// stands — the historical behavior stays available for non-GitHub use.
    #[tokio::test]
    async fn pull_request_delivery_without_github_credentials_keeps_the_payload_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _no_token = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_TOKEN");
        let _no_api = crate::state::TestEnvVar::unset("PRELOOP_GITHUB_API_URL");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-no-creds", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1);
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.sha, stale_merge);
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
    }

    /// With fork workflows disabled, a fork pull-request delivery creates no
    /// pull_request runs — and must not spend a GitHub probe looking for a
    /// merge nothing will run.
    #[tokio::test]
    async fn disabled_fork_workflows_skip_the_merge_state_poll() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        std::fs::write(
            ws_dir.join(".github/workflows/pre.yml"),
            "on: pull_request_target\njobs:\n  base:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo base\n",
        )
        .unwrap();
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        let (api, requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            // A probe landing here is the bug: no run can use the answer.
            (
                200,
                vec![],
                serde_json::json!({ "mergeable": true, "merge_commit_sha": null }),
            )
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        fixture.state.fork_policy.run_fork_workflows = false;
        // A regression would poll to this budget before failing; keep the
        // failure fast.
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_millis(200),
            poll_interval: Duration::from_millis(20),
        };

        let payload = serde_json::to_vec(&serde_json::json!({
            "action": "synchronize",
            "number": 42,
            "pull_request": {
                "number": 42,
                "head": { "ref": "feature", "sha": head, "repo": { "fork": true } },
                "base": { "ref": "main", "sha": current_base },
                "merge_commit_sha": stale_merge,
            },
            "repository": { "full_name": "owner/repo", "default_branch": "main" },
        }))
        .unwrap();
        assert_eq!(
            fixture
                .post_body("delivery-pr-fork-off", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert!(
            inner
                .runs
                .values()
                .all(|run| run.submission.event != "pull_request"),
            "the fork policy still gates the fork pull_request run"
        );
        assert!(
            inner
                .runs
                .values()
                .any(|run| run.submission.event == "pull_request_target"),
            "pull_request_target is unaffected by the fork policy"
        );
        drop(inner);
        let probes: Vec<String> = requests
            .lock()
            .iter()
            .map(|(_, path, _)| path.clone())
            .collect();
        assert!(
            !probes.iter().any(|path| {
                // The merge-state probe or its merge-commit verification; the
                // changed-files lookup (`/pulls/42/files`) still runs for the
                // pull_request_target event.
                path == "/repos/owner/repo/pulls/42" || path.contains("/commits/")
            }),
            "a policy-skipped fork delivery must not probe the merge state at all: {probes:?}"
        );
    }

    /// One-workflow file whose only step echoes `marker`, so a test can tell
    /// which revision of the file a run executed.
    fn marked_pull_request_workflow(marker: &str) -> String {
        format!(
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo {marker}\n"
        )
    }

    /// A pull-request graph in which the base changed `.github/workflows/ci.yml`
    /// after the pull request branched, and the head never touched that file.
    /// Returns the head sha. Once the fixture commits the workspace, the base
    /// tip is `main`.
    fn build_base_advanced_workflow_graph(ws_dir: &std::path::Path) -> String {
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/ci.yml"),
            marked_pull_request_workflow("head-version"),
        )
        .unwrap();
        for args in [
            &["init", "-q", "-b", "main"][..],
            &["config", "user.email", "github-tests@example.invalid"][..],
            &["config", "user.name", "GitHub Tests"][..],
        ] {
            git_output(ws_dir, args);
        }
        git_output(ws_dir, &["add", "-A"]);
        git_output(ws_dir, &["commit", "-qm", "base"]);
        git_output(ws_dir, &["checkout", "-q", "-b", "feature"]);
        std::fs::write(ws_dir.join("feature.txt"), "feature\n").unwrap();
        git_output(ws_dir, &["add", "-A"]);
        git_output(ws_dir, &["commit", "-qm", "head"]);
        let head = git_output(ws_dir, &["rev-parse", "HEAD"]);
        git_output(ws_dir, &["checkout", "-q", "main"]);
        std::fs::write(
            ws_dir.join(".github/workflows/ci.yml"),
            marked_pull_request_workflow("merge-version"),
        )
        .unwrap();
        git_output(ws_dir, &["add", "-A"]);
        git_output(ws_dir, &["commit", "-qm", "base advances the workflow"]);
        // `WebhookFixture` commits the workspace itself; leave it something to commit.
        std::fs::write(ws_dir.join(".fixture-marker"), "marker\n").unwrap();
        head
    }

    /// A self-built merge runs the workflow revision at that merge. The base
    /// changed a workflow file the head never touched; GitHub's own
    /// `pull_request` run would execute the base's copy, so the engine's run
    /// must too, not the head's older one.
    #[tokio::test]
    async fn self_built_merge_runs_the_workflow_revision_at_the_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        let head = build_base_advanced_workflow_graph(&ws_dir);

        // GitHub never offers a merge for this head, so the engine builds its own.
        let api_head = head.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": null },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget: Duration::from_millis(300),
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        // The fixture commits the workspace, so the base tip is read after it.
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        let payload = stale_pull_request_payload(&head, &"1".repeat(40), &base_tip);
        assert_eq!(
            fixture
                .post_body("delivery-pr-base-workflow", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1, "exactly the pull_request run");
        let run = inner.runs.values().next().unwrap();
        assert!(
            run.submission.workflow_yaml.contains("merge-version"),
            "the run must execute the workflow revision at the merge, got:\n{}",
            run.submission.workflow_yaml
        );
        let merge_sha = run.submission.sha.clone();
        drop(inner);
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
    }

    /// When the engine's mirror cannot serve the merge's workflows, they come
    /// from the pull request head instead, so the delivery is not lost.
    #[tokio::test]
    async fn unreadable_merge_mirror_falls_back_to_the_head_workflows() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        let head = build_base_advanced_workflow_graph(&ws_dir);
        let fixture = WebhookFixture::with_workspace(&temp, ws_dir).await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        let merge = PrebuiltMerge {
            sha: "2".repeat(40),
            tree: "3".repeat(40),
            base_sha: "4".repeat(40),
            head_sha: head.clone(),
            repository: "owner/repo".to_owned(),
            // Never created, so the merge cannot be read from this mirror.
            mirror_repository: "checkout-cache/repositories/missing.git".to_owned(),
            pull_request_number: Some(42),
        };
        let effective = EffectiveEvent {
            event: "pull_request".to_owned(),
            git_ref: "refs/pull/42/merge".to_owned(),
            sha: Some(merge.sha.clone()),
            status_check_sha: Some(head.clone()),
            activity_type: Some("synchronize".to_owned()),
            trust_tier: None,
            skip: false,
            payload: Value::Null,
            upstream_workflow_names: Vec::new(),
        };

        let workflows =
            event_workflows(&shared, "owner/repo", &effective, Some(&merge), &merge.sha)
                .await
                .unwrap();
        let ci = workflows
            .get("ci.yml")
            .expect("ci.yml at the pull request head");
        assert!(
            ci.contains("head-version"),
            "a failed mirror read must fall back to the head's workflows, got:\n{ci}"
        );
    }

    /// Engine state for driving [`poll_fresh_merge`] directly: a poll schedule
    /// and the environment's stubbed API, with no repository or merge needed.
    async fn poll_only_shared(temp: &tempfile::TempDir, budget: Duration) -> Arc<SharedState> {
        let mut state = AppState::new(temp.path().join("state")).await.unwrap();
        state.fresh_merge_poll = FreshMergePoll {
            budget,
            poll_interval: Duration::from_millis(20),
        };
        // Some probes here fail on purpose; keep the breaker out of the way.
        state.github_breakers = Arc::new(crate::github_breaker::GithubBreakerRegistry::new(
            crate::github_breaker::BreakerConfig {
                enabled: false,
                ..Default::default()
            },
        ));
        Arc::new(SharedState {
            state,
            shutdown: CancellationToken::new(),
        })
    }

    /// The poll target for the pull request at `head`.
    fn poll_target(head: &str) -> FreshMergeTarget {
        FreshMergeTarget {
            repo: "owner/repo".to_owned(),
            number: 42,
            head: head.to_owned(),
            base_branch: "main".to_owned(),
        }
    }

    /// With every poll slot held by other deliveries, a delivery whose GitHub
    /// merge is not ready does not park a webhook worker for the poll budget:
    /// it builds the engine's own merge at once, and the held slots stay held.
    #[tokio::test]
    async fn saturated_poll_slots_build_the_merge_without_waiting_for_the_budget() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let ws_dir = pr_workspace(
            &temp,
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        );
        let (head, stale_merge, _current_merge, current_base) = build_pull_request_graph(&ws_dir);

        // GitHub's merge is never ready for this head.
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let api_head = head.clone();
        let api_base = current_base.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": api_base.clone() },
                    }),
                );
            }
            if path == "/repos/owner/repo/pulls/42/files" {
                return (200, vec![], pr_files_answer());
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        let budget = Duration::from_secs(10);
        fixture.state.fresh_merge_poll = FreshMergePoll {
            budget,
            poll_interval: Duration::from_millis(20),
        };
        let upstream = publish_merge_upstream(&fixture.state, &ws_dir);
        serve_merges_from(
            &mut fixture.state,
            upstream.parent().unwrap().parent().unwrap(),
        );
        let base_tip = git_output(&ws_dir, &["rev-parse", "main"]);

        // Other deliveries hold every poll slot.
        let slots = fixture.state.fresh_merge_poll_slots.clone();
        let total = slots.available_permits();
        let held: Vec<_> = (0..total)
            .map(|_| slots.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(slots.available_permits(), 0);

        let payload = stale_pull_request_payload(&head, &stale_merge, &current_base);
        assert_eq!(
            fixture
                .post_body("delivery-pr-saturated", Some("pull_request"), &payload)
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;
        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) <= 1,
            "a delivery that cannot take a poll slot must not poll"
        );

        let inner = fixture.state.test_tx().await;
        assert_eq!(inner.runs.len(), 1, "exactly the pull_request run");
        let run = inner.runs.values().next().unwrap();
        assert_eq!(run.submission.git_ref, "refs/pull/42/merge");
        let merge_sha = run.submission.sha.clone();
        drop(inner);
        assert_engine_merge(&merge_mirror(&fixture.state), &merge_sha, &base_tip, &head);
        assert_eq!(
            slots.available_permits(),
            0,
            "the held slots stay held by their owners"
        );
        drop(held);
        assert_eq!(slots.available_permits(), total);
    }

    /// A poll that resolves to GitHub's merge gives its slot back.
    #[tokio::test]
    async fn poll_slot_is_released_once_github_offers_the_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let (head, base, merge) = ("a".repeat(40), "b".repeat(40), "c".repeat(40));
        let commits_path = format!("/repos/owner/repo/commits/{merge}");
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let (api_head, api_base, api_merge) = (head.clone(), base.clone(), merge.clone());
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                // Not computed on the first probe; offered on the second.
                let first = polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                let answer = if first {
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": api_base.clone() },
                    })
                } else {
                    serde_json::json!({
                        "mergeable": true,
                        "merge_commit_sha": api_merge.clone(),
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": api_base.clone() },
                    })
                };
                return (200, vec![], answer);
            }
            if path == commits_path.as_str() {
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "sha": api_merge.clone(),
                        "parents": [{ "sha": api_base.clone() }, { "sha": api_head.clone() }],
                    }),
                );
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let shared = poll_only_shared(&temp, Duration::from_secs(5)).await;
        let slots = shared.state.fresh_merge_poll_slots.clone();
        let total = slots.available_permits();
        match poll_fresh_merge(&shared, &poll_target(&head)).await {
            Some(FreshMergePlacement::Merge(sha)) => assert_eq!(sha, merge),
            other => panic!("GitHub's merge must be used once offered, got {other:?}"),
        }
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            slots.available_permits(),
            total,
            "a poll that resolves releases its slot"
        );
    }

    /// A poll whose probe turns fatal after waiting gives its slot back.
    #[tokio::test]
    async fn poll_slot_is_released_when_the_probe_turns_fatal() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let head = "a".repeat(40);
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let api_head = head.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                return match polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 => (
                        200,
                        vec![],
                        serde_json::json!({
                            "mergeable": null,
                            "merge_commit_sha": null,
                            "head": { "sha": api_head.clone() },
                            "base": { "sha": null },
                        }),
                    ),
                    _ => (403, vec![], serde_json::json!({ "message": "forbidden" })),
                };
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let shared = poll_only_shared(&temp, Duration::from_secs(5)).await;
        let slots = shared.state.fresh_merge_poll_slots.clone();
        let total = slots.available_permits();
        assert!(
            poll_fresh_merge(&shared, &poll_target(&head))
                .await
                .is_none(),
            "an unavailable merge state is not waited out"
        );
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(
            slots.available_permits(),
            total,
            "the slot is released when the poll fails"
        );
    }

    /// A poll that runs out of budget gives its slot back too.
    #[tokio::test]
    async fn poll_slot_is_released_when_the_budget_runs_out() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let head = "a".repeat(40);
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (500, vec![], serde_json::json!({ "message": "boom" }));
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        // Leave wall time for the stub request when nextest is CPU-contended;
        // the 20ms poll interval still exercises the retry budget.
        let shared = poll_only_shared(&temp, Duration::from_secs(5)).await;
        let slots = shared.state.fresh_merge_poll_slots.clone();
        let total = slots.available_permits();
        assert!(
            poll_fresh_merge(&shared, &poll_target(&head))
                .await
                .is_none()
        );
        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "the probe is retried within the budget"
        );
        assert_eq!(
            slots.available_permits(),
            total,
            "the slot is released when the budget runs out"
        );
    }

    /// Cancelling a waiting delivery (its processing task aborted when the
    /// lease is lost) drops the slot it holds.
    #[tokio::test]
    async fn poll_slot_is_released_when_the_waiting_delivery_is_cancelled() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let head = "a".repeat(40);
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let api_head = head.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": api_head.clone() },
                        "base": { "sha": null },
                    }),
                );
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let shared = poll_only_shared(&temp, Duration::from_secs(30)).await;
        let slots = shared.state.fresh_merge_poll_slots.clone();
        let total = slots.available_permits();
        let target = poll_target(&head);
        let waiting = tokio::spawn({
            let shared = shared.clone();
            async move { poll_fresh_merge(&shared, &target).await }
        });
        for _ in 0..500 {
            if polls.load(std::sync::atomic::Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            polls.load(std::sync::atomic::Ordering::SeqCst) >= 2,
            "the delivery must be polling before it is cancelled"
        );
        assert_eq!(
            slots.available_permits(),
            total - 1,
            "a waiting delivery holds one slot"
        );
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(
            slots.available_permits(),
            total,
            "cancelling the wait releases the slot"
        );
    }

    /// However many deliveries poll at once, merge-state probes in flight never
    /// exceed the node's probe capacity, and every probe permit comes back. The
    /// stub answers slowly so probes overlap, and it counts how many run at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merge_state_probes_in_flight_stay_within_the_probe_capacity() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (in_flight_for_stub, peak_for_stub) = (in_flight, peak.clone());
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                let now = in_flight_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                peak_for_stub.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(150));
                in_flight_for_stub.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": "a".repeat(40) },
                        "base": { "sha": null },
                    }),
                );
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let shared = poll_only_shared(&temp, Duration::from_millis(600)).await;
        let permits = shared.state.fresh_merge_probe_permits.clone();
        let total = permits.available_permits();
        let head = "a".repeat(40);
        let polls: Vec<_> = (0..fresh_merge_poll_capacity() * 4)
            .map(|_| {
                let shared = shared.clone();
                let target = poll_target(&head);
                tokio::spawn(async move { poll_fresh_merge(&shared, &target).await })
            })
            .collect();
        for poll in polls {
            poll.await.unwrap();
        }

        let peak = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert!(peak >= 1, "the polls must reach GitHub");
        assert!(
            peak <= fresh_merge_poll_capacity(),
            "{peak} merge-state probes were in flight at once"
        );
        assert_eq!(
            permits.available_permits(),
            total,
            "every probe permit is returned"
        );
    }

    /// With every probe permit held by other probes, a delivery does not ask
    /// GitHub at all. It gives up after the probe wait, well inside the budget,
    /// and builds its own merge.
    #[tokio::test]
    async fn saturated_probe_permits_skip_the_probe_and_build_the_merge() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "test-token");

        let temp = tempfile::tempdir().unwrap();
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let polls_for_stub = polls.clone();
        let (api, _requests) = start_github_stub(move |_method, path, _body| {
            if path == "/repos/owner/repo/pulls/42" {
                polls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return (
                    200,
                    vec![],
                    serde_json::json!({
                        "mergeable": null,
                        "merge_commit_sha": null,
                        "head": { "sha": "a".repeat(40) },
                        "base": { "sha": null },
                    }),
                );
            }
            (404, vec![], serde_json::json!({ "message": "not found" }))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);

        let budget = Duration::from_secs(10);
        let shared = poll_only_shared(&temp, budget).await;
        let permits = shared.state.fresh_merge_probe_permits.clone();
        let total = permits.available_permits();
        let held: Vec<_> = (0..total)
            .map(|_| permits.clone().try_acquire_owned().unwrap())
            .collect();

        let started = Instant::now();
        let placement = poll_fresh_merge(&shared, &poll_target(&"a".repeat(40))).await;
        assert!(
            placement.is_none(),
            "without a probe permit the merge is built locally"
        );
        assert!(
            started.elapsed() < budget,
            "the delivery must give up at the probe wait, not the poll budget"
        );
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a delivery without a probe permit must not ask GitHub"
        );
        drop(held);
        assert_eq!(permits.available_permits(), total);
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

    /// Trip a breaker bucket the way three consecutive 5xx responses would.
    fn open_breaker(state: &AppState) {
        let breaker = state
            .github_breakers
            .breaker_for(crate::github_breaker::GithubCredentialBucket::Pat);
        for _ in 0..3 {
            breaker.record_failure(
                &crate::github_breaker::GithubFailureKind::Unavailable,
                "GitHub responded 503",
            );
        }
        assert!(breaker.is_open(), "breaker must be open");
    }

    /// `payload` with `installation.id` set, the way a GitHub App delivery
    /// names the installation whose token it runs under.
    fn with_installation(payload: &[u8], installation_id: u64) -> Vec<u8> {
        let mut value: Value = serde_json::from_slice(payload).unwrap();
        value["installation"] = serde_json::json!({ "id": installation_id });
        serde_json::to_vec(&value).unwrap()
    }

    /// Register a test App whose installation on `owner` is
    /// `installation_id`, stubbing the ownership verification the selection
    /// ladder makes. Returns the env guard holding `PRELOOP_GITHUB_API_URL`
    /// and a cloned `AppState` carrying the registry — `github_app`/`github_apps`
    /// are plain fields, so the returned state is the one the delivery
    /// processing must run against.
    async fn configure_app_installation(
        state: &AppState,
        installation_id: u64,
        owner: &'static str,
    ) -> (crate::state::TestEnvVar, AppState) {
        let (api, _requests) = start_github_stub(move |method, path, _body| {
            if method == "GET" && path == format!("/app/installations/{installation_id}") {
                return (
                    200,
                    vec![],
                    serde_json::json!({"account": {"login": owner}}),
                );
            }
            (404, vec![], serde_json::json!({"message": "not found"}))
        })
        .await;
        let api_env = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let mut app = crate::github_app::GitHubAppCredentials::for_tests(
            "474",
            rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        );
        app.installation_id = Some(installation_id);
        let mut state = state.clone();
        state.github_app = Some(app.clone());
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![app],
            default_index: 0,
        });
        (api_env, state)
    }

    /// A fixture whose workspace is hidden, so workflow evaluation fails
    /// transiently the way an unreachable dependency would.
    async fn hidden_workspace_fixture(temp: &tempfile::TempDir) -> WebhookFixture {
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n",
        )
        .unwrap();
        let fixture = WebhookFixture::with_workspace(temp, ws_dir.clone()).await;
        std::fs::rename(&ws_dir, temp.path().join("hidden_ws")).unwrap();
        fixture
    }

    /// Queue `payload` as a delivery that has already spent its whole attempt
    /// budget, then claim it the way the drain loop would.
    async fn claim_exhausted_delivery(
        state: &AppState,
        delivery_id: &str,
        payload: Vec<u8>,
    ) -> WebhookDeliveryRecord {
        state
            .backend
            .enqueue_webhook_delivery(&WebhookDeliveryRecord {
                delivery_id: delivery_id.to_owned(),
                event: "push".to_owned(),
                payload,
                received_at_us: crate::store::now_us(),
                state: WebhookDeliveryStatus::Received,
                attempts: WEBHOOK_MAX_ATTEMPTS,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            })
            .await
            .unwrap();
        state
            .backend
            .claim_webhook_deliveries(1, WEBHOOK_LEASE_DURATION_SECS)
            .await
            .unwrap()
            .into_iter()
            .next()
            .expect("claimed the delivery")
    }

    async fn webhook_record(state: &AppState, delivery_id: &str) -> WebhookDeliveryRecord {
        state
            .backend
            .get_webhook_delivery(delivery_id)
            .await
            .unwrap()
            .expect("delivery row must exist")
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
        // Make workflow evaluation fail the way an unreachable dependency
        // would, and start from an attempt count already past the cap.
        let fixture = hidden_workspace_fixture(&temp).await;
        let delivery = claim_exhausted_delivery(
            &fixture.state,
            "delivery-outage-park",
            fixture.payload_bytes.clone(),
        )
        .await;
        open_breaker(&fixture.state);
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        process_one_delivery(&shared, &delivery).await;

        let record = webhook_record(&fixture.state, "delivery-outage-park").await;
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

    /// A throttled installation must not make another installation's
    /// transient failure look like an outage. It takes normal attempt
    /// accounting, so a delivery already past the cap is dead-lettered.
    #[tokio::test]
    async fn throttled_unrelated_installation_does_not_park_transient_failures() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = hidden_workspace_fixture(&temp).await;
        fixture
            .state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Installation(2))
            .record_failure(
                &crate::github_breaker::GithubFailureKind::RateLimited {
                    retry_after: Some(Duration::from_secs(120)),
                },
                "429",
            );
        let delivery = claim_exhausted_delivery(
            &fixture.state,
            "delivery-unrelated",
            with_installation(&fixture.payload_bytes, 9),
        )
        .await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        process_one_delivery(&shared, &delivery).await;

        let record = webhook_record(&fixture.state, "delivery-unrelated").await;
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Failed,
            "another installation's throttle must not park a failure past the cap"
        );
    }

    /// A throttled installation parks its own deliveries' transient failures
    /// even past the attempt cap: the throttle belongs to the credential, not
    /// to the delivery. "Own" means the App installation the repo actually
    /// selects, which the payload's `installation.id` happens to name here.
    #[tokio::test]
    async fn throttled_own_installation_parks_transient_failures() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let fixture = hidden_workspace_fixture(&temp).await;
        // The repo's real credential is this App's installation 9: selection,
        // not the payload's `installation.id`, decides the bucket.
        let (_api, app_state) = configure_app_installation(&fixture.state, 9, "owner").await;
        fixture
            .state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Installation(9))
            .record_failure(
                &crate::github_breaker::GithubFailureKind::RateLimited {
                    retry_after: Some(Duration::from_secs(120)),
                },
                "429",
            );
        let delivery = claim_exhausted_delivery(
            &fixture.state,
            "delivery-own",
            with_installation(&fixture.payload_bytes, 9),
        )
        .await;
        let shared = Arc::new(SharedState {
            state: app_state,
            shutdown: CancellationToken::new(),
        });
        process_one_delivery(&shared, &delivery).await;

        let record = webhook_record(&fixture.state, "delivery-own").await;
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "the own credential's throttle must park the delivery"
        );
        assert_eq!(
            record.attempts, WEBHOOK_MAX_ATTEMPTS,
            "parking refunds the attempt the claim charged"
        );
        assert!(
            record
                .lease_until_us
                .is_some_and(|retry_at| retry_at > crate::store::now_us())
        );
    }

    /// The bucket the delivery's calls resolve to — not the payload's
    /// `installation.id` — governs parking. No App covers `owner/repo`, so
    /// the calls spend the PAT even though the payload names installation 9:
    /// a PAT throttle must park the delivery instead of dead-lettering it
    /// while installation 9's budget sits unused.
    #[tokio::test]
    async fn throttled_pat_parks_delivery_naming_a_foreign_installation() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let fixture = hidden_workspace_fixture(&temp).await;
        fixture
            .state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Pat)
            .record_failure(
                &crate::github_breaker::GithubFailureKind::RateLimited {
                    retry_after: Some(Duration::from_secs(120)),
                },
                "429",
            );
        let delivery = claim_exhausted_delivery(
            &fixture.state,
            "delivery-foreign",
            with_installation(&fixture.payload_bytes, 9),
        )
        .await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        process_one_delivery(&shared, &delivery).await;

        let record = webhook_record(&fixture.state, "delivery-foreign").await;
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "the PAT bucket is what the delivery's calls spend: its throttle must park"
        );
        assert_eq!(
            record.attempts, WEBHOOK_MAX_ATTEMPTS,
            "parking refunds the attempt the claim charged"
        );
    }

    /// A GitHub-wide outage parks every installation's transient failures:
    /// the outage is nobody's delivery's fault, whichever credential it uses.
    #[tokio::test]
    async fn github_wide_outage_parks_any_installations_transient_failures() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = hidden_workspace_fixture(&temp).await;
        open_breaker(&fixture.state);
        let delivery = claim_exhausted_delivery(
            &fixture.state,
            "delivery-wide",
            with_installation(&fixture.payload_bytes, 9),
        )
        .await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        process_one_delivery(&shared, &delivery).await;

        let record = webhook_record(&fixture.state, "delivery-wide").await;
        assert_eq!(
            record.state,
            WebhookDeliveryStatus::Received,
            "a GitHub-wide outage must park, never dead-letter, a delivery"
        );
        assert_eq!(
            record.attempts, WEBHOOK_MAX_ATTEMPTS,
            "parking refunds the attempt the claim charged"
        );
    }

    /// The delivery is charged to the credential its GitHub calls resolve
    /// to — the App covering `repository.full_name`, else the PAT — never to
    /// the installation id the payload happens to name.
    #[tokio::test]
    async fn webhook_delivery_bucket_follows_the_selected_credential() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = Arc::new(SharedState {
            state,
            shutdown: CancellationToken::new(),
        });
        let bucket = |payload: &str| {
            let shared = shared.clone();
            let delivery = WebhookDeliveryRecord {
                delivery_id: "delivery".to_owned(),
                event: "push".to_owned(),
                payload: payload.as_bytes().to_vec(),
                received_at_us: 0,
                state: WebhookDeliveryStatus::Received,
                attempts: 0,
                lease_until_us: None,
                lease_token: None,
                last_error: None,
            };
            async move { webhook_delivery_bucket(&shared, &delivery).await }
        };
        // No App configured: every delivery's calls use the PAT, whatever
        // `installation.id` the payload claims.
        assert_eq!(
            bucket(r#"{"installation":{"id":9},"repository":{"full_name":"owner/repo"}}"#).await,
            GithubCredentialBucket::Pat
        );
        assert_eq!(
            bucket(r#"{"ref":"refs/heads/main"}"#).await,
            GithubCredentialBucket::Pat
        );
        assert_eq!(bucket("not json").await, GithubCredentialBucket::Pat);
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
        // `reports_check_runs` marks the run as a check reporter, which is
        // what makes it a check-suite member.
        shared
            .state
            .backend
            .set_reports_check_runs(accepted.run_id, true)
            .await
            .unwrap();
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
        assert_eq!(
            runs.len(),
            1,
            "a clean sender's rerequest re-runs the run in place"
        );
        let run = runs.values().next().unwrap();
        assert_eq!(run.run_attempt, 2, "the rerequest starts attempt 2");
        assert!(
            !run.status.is_terminal(),
            "attempt 2 is live, got {:?}",
            run.status
        );
        assert_eq!(
            run.job_check_run_ids.get(&JobId("build".to_owned())),
            Some(&12345),
            "the new attempt keeps reporting through the requested check run"
        );
    }

    fn check_suite_payload(action: &str, sender: &str) -> serde_json::Value {
        serde_json::json!({
            "action": action,
            "check_suite": {
                "head_sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
            },
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": sender},
        })
    }

    /// A `check_suite.rerequested` delivery re-runs the suite's terminal run
    /// in place — same run id, attempt 2 — not as a new run.
    #[tokio::test]
    async fn check_suite_rerequest_reruns_the_suite_run_in_place() {
        let (_temp, shared) = rerequest_fixture().await;
        assert_eq!(shared.state.test_tx().await.runs.len(), 1);
        crate::rerequest::process_check_suite_rerequest(
            &shared,
            &check_suite_payload("rerequested", "alice"),
        )
        .await
        .unwrap();
        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 1, "the suite rerequest must not mint a run");
        let run = runs.values().next().unwrap();
        assert_eq!(run.run_attempt, 2, "the suite rerequest starts attempt 2");
        assert!(
            !run.status.is_terminal(),
            "attempt 2 is live, got {:?}",
            run.status
        );
    }

    /// A non-`rerequested` suite action is not a re-run trigger.
    #[tokio::test]
    async fn check_suite_completed_does_not_rerun() {
        let (_temp, shared) = rerequest_fixture().await;
        crate::rerequest::process_check_suite_rerequest(
            &shared,
            &check_suite_payload("completed", "alice"),
        )
        .await
        .unwrap();
        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs.values().next().unwrap().run_attempt,
            1,
            "a completed suite delivery leaves the run alone"
        );
    }

    /// A suite rerequest from a denied actor is ignored like the check-run
    /// path: execution protection applies to both.
    #[tokio::test]
    async fn check_suite_rerequest_denies_blocked_sender() {
        let (_temp, shared) = rerequest_fixture().await;
        crate::rerequest::process_check_suite_rerequest(
            &shared,
            &check_suite_payload("rerequested", "mallory"),
        )
        .await
        .unwrap();
        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs.values().next().unwrap().run_attempt,
            1,
            "a blocked sender must not restart the run"
        );
    }

    /// A check-run rerequest records the webhook sender as the rerun's
    /// `github.triggering_actor` while the run keeps its original actor.
    #[tokio::test]
    async fn check_run_rerequest_stamps_triggering_actor() {
        let (_temp, shared) = rerequest_fixture().await;
        let (status, _) = process_check_run_rerequest(&shared, &rerequest_payload("bob"))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        let runs = shared.state.test_tx().await.runs;
        let run = runs.values().next().unwrap();
        assert_eq!(run.run_attempt, 2);
        assert_eq!(
            run.github["triggering_actor"].as_str(),
            Some("bob"),
            "the webhook sender becomes github.triggering_actor: {}",
            run.github
        );
        assert_eq!(
            run.submission.actor, "alice",
            "the original submission actor is unchanged"
        );
    }

    /// Seed a second terminal workflow run on `shared`, reporting its checks
    /// at `reported_sha` (the checkout SHA may differ, as on pull_request).
    /// Returns the new run's id.
    async fn seed_suite_run(
        shared: &std::sync::Arc<crate::SharedState>,
        workflow_file: &str,
        checkout_sha: &str,
        reported_sha: &str,
        reports: bool,
    ) -> RunId {
        let submission = WorkflowSubmission {
            workflow_yaml:
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                    .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(format!(".github/workflows/{workflow_file}")),
            workflow_file: Some(workflow_file.to_owned()),
            actor: "alice".to_owned(),
            sha: checkout_sha.to_owned(),
            status_check_sha: (reported_sha != checkout_sha).then(|| reported_sha.to_owned()),
            resolved_sha: Some(checkout_sha.to_owned()),
            ..Default::default()
        };
        let accepted = crate::submit_run_inner(shared, submission).await.unwrap();
        shared
            .state
            .backend
            .set_reports_check_runs(accepted.run_id, reports)
            .await
            .unwrap();
        shared
            .state
            .test_db_mutate(|db| db.set_run_status(accepted.run_id, "completed", Some("success")))
            .await
            .unwrap();
        accepted.run_id
    }

    /// Every check-reporting run whose checks landed on the suite's head SHA
    /// is a member: a rerequest re-runs all of them, not just the newest.
    /// Workflows that never reported checks cannot belong to the suite.
    #[tokio::test]
    async fn check_suite_rerequest_reruns_every_member_run() {
        let (_temp, shared) = rerequest_fixture().await;
        let head = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let second = seed_suite_run(&shared, "deploy.yml", head, head, true).await;
        let quiet = seed_suite_run(&shared, "docs.yml", head, head, false).await;

        crate::rerequest::process_check_suite_rerequest(
            &shared,
            &check_suite_payload("rerequested", "alice"),
        )
        .await
        .unwrap();

        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 3, "in-place reruns mint no new runs");
        let mut attempts: Vec<u64> = runs.values().map(|run| run.run_attempt).collect();
        attempts.sort();
        assert_eq!(
            attempts,
            vec![1, 2, 2],
            "both reporting runs restart; the non-reporter stays at attempt 1"
        );
        assert_eq!(runs.get(&quiet).unwrap().run_attempt, 1);
        assert_eq!(runs.get(&second).unwrap().run_attempt, 2);
    }

    /// A pull_request run checks out the merge commit but reports checks on
    /// the PR head (`status_check_sha`); the suite rerequest names the
    /// reported SHA, so the run matches on its report coordinate — not
    /// `run.head_sha`.
    #[tokio::test]
    async fn check_suite_rerequest_matches_the_reported_sha() {
        let (_temp, shared) = rerequest_fixture().await;
        let pr_head = "bbbb2222cccc3333dddd4444eeee5555ffff6666";
        let checkout = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let pr_run = seed_suite_run(&shared, "pr.yml", checkout, pr_head, true).await;

        let mut payload = check_suite_payload("rerequested", "alice");
        payload["check_suite"]["head_sha"] = serde_json::json!(pr_head);
        crate::rerequest::process_check_suite_rerequest(&shared, &payload)
            .await
            .unwrap();

        let runs = shared.state.test_tx().await.runs;
        assert_eq!(runs.len(), 2);
        assert_eq!(
            runs.get(&pr_run).unwrap().run_attempt,
            2,
            "the run reporting at the suite head_sha must rerun"
        );
        let fixture_run = runs.values().find(|run| run.run_id != pr_run).unwrap();
        assert_eq!(
            fixture_run.run_attempt, 1,
            "the run reporting on another commit is untouched"
        );
    }

    /// A suite rerequest that names another App's id does not restart the
    /// registered App's runs — suite membership is per reporting app.
    #[tokio::test]
    async fn check_suite_rerequest_ignores_a_foreign_app_suite() {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![crate::github_app::GitHubAppCredentials::for_tests(
                "424",
                rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap(),
                crate::github_app::MintFailurePolicy::LocalJwt,
            )],
            default_index: 0,
        });
        let shared = state.shared();
        let head = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let run_id = seed_suite_run(&shared, "ci.yml", head, head, true).await;

        // 15368 is github.com's own "GitHub Actions" app id — a suite ours
        // never reported.
        let mut payload = check_suite_payload("rerequested", "alice");
        payload["check_suite"]["app"] = serde_json::json!({"id": 15368});
        crate::rerequest::process_check_suite_rerequest(&shared, &payload)
            .await
            .unwrap();
        assert_eq!(shared.state.test_tx().await.runs[&run_id].run_attempt, 1);

        // Naming the registered app id takes the normal path.
        payload["check_suite"]["app"] = serde_json::json!({"id": 424});
        crate::rerequest::process_check_suite_rerequest(&shared, &payload)
            .await
            .unwrap();
        assert_eq!(shared.state.test_tx().await.runs[&run_id].run_attempt, 2);
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

    /// A PAT 429 observed on the actions path parks PAT-backed check runs:
    /// the sender shares the PAT bucket from the registry instead of a
    /// private breaker, so the throttled budget is not spent twice.
    #[tokio::test]
    async fn pat_throttle_on_actions_parks_check_run_sender() {
        use crate::github_breaker::{GithubCredentialBucket, GithubSubsystem};
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let (api, requests) = start_github_stub(move |_, _, _| {
            (
                429,
                vec![("x-ratelimit-reset", reset.to_string())],
                serde_json::json!({"message": "API rate limit exceeded"}),
            )
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "pat-for-test");
        let (_temp, shared, run_id) = mint_fixture(true, None).await;

        // Trip the PAT bucket the way an action-tarball 429 would: the 429
        // response itself is returned, and the bucket opens at once.
        let response = crate::github_breaker::send_observed_labeled(
            &shared.state.github_breakers,
            GithubCredentialBucket::Pat,
            &shared.state.github_consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{api}/repos/o/r/tarball/main")),
        )
        .await
        .expect("the 429 response itself is returned, not an error");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            shared
                .state
                .github_breakers
                .breaker_for(GithubCredentialBucket::Pat)
                .is_open(),
            "the PAT bucket must be parked"
        );

        // A check run for a PAT-backed repo must not reach GitHub now: the
        // sender consults the same shared bucket and defers the row.
        let job = JobId("build".to_owned());
        seed_check_run_update(&shared, run_id, &job, 1, "in_progress", None).await;
        assert_eq!(drain_sender(&shared).await, 1);
        assert!(
            check_run_writes(&requests).is_empty(),
            "no check-run write may reach GitHub while the PAT bucket is parked"
        );
        let usage: std::collections::HashMap<&str, crate::github_breaker::GithubSubsystemUsage> =
            shared
                .state
                .github_consumption
                .snapshot()
                .into_iter()
                .map(|usage| (usage.subsystem, usage))
                .collect();
        assert_eq!(usage["check-runs"].breaker_blocked, 1);
        assert_eq!(
            usage["check-runs"].requests, 0,
            "a refused attempt is not a GitHub request"
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

        let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
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
                "/repos/owner/repo/check-runs/{id}",
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

        // The PAT and API base are part of `AppState` construction
        // (`state.github_pat` snapshots `PRELOOP_GITHUB_TOKEN` — env wins,
        // then the config file), so they must be set before `mint_fixture`.
        let _api_url = crate::state::TestEnvVar::set(
            "PRELOOP_GITHUB_API_URL",
            format!("http://127.0.0.1:{port}"),
        );
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "check-retry-token");
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
            .route("/repos/owner/repo/branches/{branch}", branch_stub)
            .route("/repos/owner/mismatch/branches/{branch}", mismatch_stub);
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
    /// The fork kill switch (`fork_policy.run_fork_workflows = false`) skips a
    /// fork `pull_request` event before any workflow is fetched or matched.
    /// The first arm proves the payload really is a fork event (with the
    /// switch on the same delivery produces a run) — a regression here let a
    /// disabled operator setting run fork workflows anyway.
    #[tokio::test]
    async fn fork_pull_request_kill_switch_skips_before_matching() {
        let temp = tempfile::tempdir().unwrap();
        let ws_dir = temp.path().join("ws");
        std::fs::create_dir_all(ws_dir.join(".github/workflows")).unwrap();
        std::fs::write(
            ws_dir.join(".github/workflows/pr.yml"),
            "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        )
        .unwrap();
        let mut fixture = WebhookFixture::with_workspace(&temp, ws_dir.clone()).await;
        // Workflow fetching reads the event's ref out of the local clone;
        // materialize the PR ref GitHub would have resolved.
        assert!(
            std::process::Command::new("git")
                .args(["-C", ws_dir.to_str().unwrap()])
                .args(["update-ref", "refs/pull/99/merge", "HEAD"])
                .status()
                .unwrap()
                .success(),
            "the PR ref must exist for the control arm to match a workflow"
        );
        let payload = serde_json::json!({
            "action": "opened",
            "number": 99,
            "pull_request": {
                "number": 99,
                "base": {"ref": "main", "sha": "base-sha"},
                "head": {"ref": "feature", "sha": "fork-head-sha", "repo": {"fork": true}},
                "merge_commit_sha": "merge-sha",
            },
            "repository": {"full_name": "owner/repo", "default_branch": "main"},
            "sender": {"login": "octocat"},
        })
        .to_string();

        let payload_value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        // The payload really is a fork event — the projection must tier it
        // `UntrustedForkPullRequest`, or the skip assertion below would be
        // vacuous (nothing to skip).
        let projected = {
            use crate::events::EventAdapter;
            crate::events::pull_request::Adapter.project(&payload_value)
        };
        assert!(
            projected.iter().any(|event| event.trust_tier
                == Some(crate::events::trust_tier::TrustTier::UntrustedForkPullRequest)),
            "the delivery must project an untrusted fork pull request: {projected:?}"
        );
        assert!(
            projected.iter().any(|event| event.event == "pull_request"),
            "the pull_request event must be projected: {projected:?}"
        );

        // Switch off: the delivery is skipped before any workflow is fetched
        // or matched, and no run is created.
        fixture.state.fork_policy.run_fork_workflows = false;
        assert_eq!(
            fixture
                .post_body(
                    "delivery-fork-off",
                    Some("pull_request"),
                    payload.as_bytes()
                )
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;
        assert!(
            fixture.state.test_tx().await.runs.is_empty(),
            "run_fork_workflows = false must skip the fork event before matching"
        );
    }

    /// The deployment-status bodies the stub recorded for one deployment, in
    /// request order: `(state, environment_url)`.
    fn deployment_statuses(
        requests: &[String],
        deployment_id: u64,
    ) -> Vec<(String, Option<String>)> {
        requests
            .iter()
            .filter_map(|request| {
                request.strip_prefix(&format!("deployments/{deployment_id}/statuses:"))
            })
            .map(|body| {
                let value: Value = serde_json::from_str(body).expect("status body is JSON");
                (
                    value["state"].as_str().unwrap_or_default().to_owned(),
                    value
                        .get("environment_url")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                )
            })
            .collect()
    }

    /// A `check_run.requested_action` delivery from an authorized reviewer
    /// approves the held environment job: the gate records the reviewer's
    /// identity and the promotion sweep releases the job to the queue.
    #[tokio::test]
    async fn check_run_requested_action_approves_held_environment_job() {
        use axum::routing::{get, patch, post};
        // PAT-mode stub: environment rules with octocat as the reviewer, plus
        // the deployment + check-run surfaces the review posts to.
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = axum::Router::new()
            .route(
                "/repos/owner/repo/environments/prod",
                get({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("environment".to_owned());
                            Json(serde_json::json!({
                                "protection_rules": [{
                                    "type": "required_reviewers",
                                    "prevent_self_review": false,
                                    "reviewers": [
                                        {"type": "User", "reviewer": {"login": "octocat"}},
                                    ],
                                }],
                            }))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment_protection_rules",
                get(|| async {
                    Json(serde_json::json!({"custom_deployment_protection_rules": []}))
                }),
            )
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move |Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("deployments:{body}"));
                            Json(serde_json::json!({"id": 555}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("check-runs/{id}:{body}"));
                            Json(serde_json::json!({"id": id}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        // Restored when the guards drop (including on panic): a leaked stub
        // base or token breaks every later env-sensitive test in the binary.
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        // The backend shares AppState's resolver (bootstrap's contract);
        // PAT mode resolves against the stub above.
        fixture.state.environment_resolver.set_github_configured();
        fixture
            .state
            .backend
            .set_environment_resolver(fixture.state.environment_resolver.clone());
        fixture
            .state
            .environment_resolver
            .resolve_at(&api_base, &shared, "owner/repo", "prod")
            .await
            .expect("environment rules resolve");

        // Park a job on the approval gate: an armed (pre-arm) gate holds the
        // job at submit, then the sweep stamps the pending approval. The
        // declared `environment.url` is the pre-completion literal the
        // deployment statuses carry.
        let run_id = RunId::new();
        let job_id = preloop_gha_protocol::JobId("deploy".to_owned());
        let mut submit = crate::control::tests::submit_run(
            run_id,
            vec![crate::control::tests::submit_job(run_id, "deploy", 1)],
        );
        submit.jobs[0].queued.environment = Some(serde_json::json!({
            "name": "prod",
            "url": "https://staging.example.com",
        }));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        fixture.state.backend.submit_run(submit).await.unwrap();
        fixture
            .state
            .backend
            .promote_ready_jobs(Some(run_id))
            .await
            .unwrap();
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();
        assert_eq!(
            fixture
                .state
                .backend
                .job_check_run_id(run_id, &job_id)
                .await
                .unwrap(),
            Some(99)
        );
        let pending = fixture
            .state
            .backend
            .pending_environment_approvals(Some(run_id))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].check_run_id, Some(99));
        crate::github::announce_environment_gates(&shared, Some(run_id)).await;
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("approval gate armed");
        assert!(
            gate.approval_requested_at_unix_nanos.is_some(),
            "the required-reviewer gate must stamp the wait"
        );
        // The review surface is a *completed* check run whose conclusion is
        // `action_required`: `action_required` is not a writable status, and
        // GitHub rejects the PATCH (422) when it is sent as one.
        let announce_calls = requests.lock().clone();
        assert!(
            announce_calls
                .iter()
                .any(|request| request.contains("check-runs/99")
                    && request.contains("\"status\":\"completed\"")
                    && request.contains("\"conclusion\":\"action_required\"")
                    && request.contains("\"actions\"")
                    && request.contains("\"identifier\":\"approve\"")
                    && request.contains("\"identifier\":\"reject\"")),
            "the announce PATCHes a completed action_required check run with the \
             Approve/Reject actions: {announce_calls:?}"
        );

        // The Approve click delivers `check_run.requested_action` under the
        // reviewer's login.
        let payload = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "approve"},
            "check_run": {"id": 99, "head_sha": "abc123"},
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": "octocat"},
        })
        .to_string();
        assert_eq!(
            fixture
                .post_body("del-approve", Some("check_run"), payload.as_bytes())
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert_eq!(
            fixture
                .state
                .backend
                .job_queue_state(run_id, &job_id)
                .await
                .unwrap(),
            Some(("ready".to_owned(), "queued".to_owned())),
            "the satisfied environment gate releases the job to the ready queue"
        );
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("gate records the review");
        assert_eq!(gate.approvals.len(), 1);
        assert_eq!(gate.approvals[0].actor.as_deref(), Some("octocat"));
        assert!(!gate.approvals[0].admin_override);

        // GitHub surfaces: the check run PATCH back to in_progress and the
        // deployment create + `queued` status.
        let calls = requests.lock().clone();
        assert!(
            calls
                .iter()
                .any(|request| request.contains("check-runs/99")
                    && request.contains("in_progress")),
            "the check run resumes after approval: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|request| request.starts_with("deployments:")),
            "the deployment is created for the environment job: {calls:?}"
        );
        assert!(
            calls
                .iter()
                .any(|request| request.contains("deployments/555/statuses")
                    && request.contains("queued")),
            "the deployment status reports the release: {calls:?}"
        );
        // The deployment tracked the job from the moment it parked.
        let statuses = deployment_statuses(&calls, 555);
        assert_eq!(
            statuses
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["pending", "queued"],
            "the announce posts `pending`, the approval releases as `queued`: {calls:?}"
        );

        // The job starts: the deployment flips to `in_progress` (and the
        // terminal completion posts `success`) with the declared
        // `environment.url` on every status.
        crate::github::report_check_run_in_progress(&shared, run_id, &job_id).await;
        crate::github::report_check_run_completed(
            &shared,
            run_id,
            &job_id,
            preloop_gha_protocol::ExecutionStatus::Success,
        )
        .await;
        let requests = requests.lock().clone();
        let statuses = deployment_statuses(&requests, 555);
        assert_eq!(
            statuses
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["pending", "queued", "in_progress", "success"],
            "the deployment statuses follow the job lifecycle: {requests:?}"
        );
        assert_eq!(
            statuses.last().and_then(|(_, url)| url.as_deref()),
            Some("https://staging.example.com"),
            "the terminal status carries the evaluated environment url: {requests:?}"
        );
        assert!(
            statuses
                .iter()
                .all(|(_, url)| url.as_deref() == Some("https://staging.example.com")),
            "every status carries the environment url: {requests:?}"
        );
    }

    /// A transient App-installation lookup failure is not "no credential
    /// covers this repository". Treating it as the latter resolved the
    /// repository to the local TOML table — empty by default — so the gate
    /// read "no protection" and the job ran with the protected environment's
    /// secrets and OIDC claim. The key stays pending (fail closed) instead.
    #[tokio::test]
    async fn transient_app_lookup_holds_environment_rules_fail_closed() {
        use axum::routing::get;
        // `true` → 500 on the installations endpoint (a GitHub outage of the
        // kind that also fails 5xx/timeouts); `false` → a definitive "no
        // installation on this owner".
        let fail = std::sync::Arc::new(parking_lot::Mutex::new(true));
        let stub = axum::Router::new().route(
            "/app/installations",
            get({
                let fail = fail.clone();
                move || {
                    let fail = *fail.lock();
                    async move {
                        if fail {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "{\"message\":\"server error\"}",
                            )
                        } else {
                            (StatusCode::OK, "[]")
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _pat = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let mut state = fixture.state.clone();
        // Two Apps: the single-App fallback (a lone App is still the only App
        // minting can try) deliberately does not apply.
        state.github_app = None;
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![
                crate::github_app::GitHubAppCredentials::for_tests(
                    "111111",
                    rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap(),
                    crate::github_app::MintFailurePolicy::LocalJwt,
                ),
                crate::github_app::GitHubAppCredentials::for_tests(
                    "222222",
                    rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap(),
                    crate::github_app::MintFailurePolicy::LocalJwt,
                ),
            ],
            default_index: 0,
        });
        let shared = Arc::new(SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        state.environment_resolver.set_github_configured();
        assert!(matches!(
            state.environment_resolver.lookup_sync("owner/repo", "prod"),
            crate::environment_resolver::EnvironmentLookup::Pending
        ));

        let lookup = crate::github_app::candidate_apps_for_repo_inner(&shared, "owner/repo").await;
        assert!(
            matches!(lookup, Err(crate::github_app::AppLookupError::Transient(_))),
            "a 5xx installation lookup is transient, not a definitive miss"
        );

        let error = state
            .environment_resolver
            .resolve_at(&api_base, &shared, "owner/repo", "prod")
            .await
            .expect_err("a transient lookup failure must not resolve rules");
        assert!(
            error.to_string().contains("installation lookup failed"),
            "the failure names the App lookup: {error}"
        );
        assert!(
            matches!(
                state.environment_resolver.lookup_sync("owner/repo", "prod"),
                crate::environment_resolver::EnvironmentLookup::Pending
            ),
            "the key stays pending so the gate holds instead of proceeding unprotected"
        );

        // A definitive "no installation" answer is an answer: the repository
        // is uncovered and the local (empty) rule set applies.
        *fail.lock() = false;
        let lookup = crate::github_app::candidate_apps_for_repo_inner(&shared, "owner/repo").await;
        assert!(
            matches!(&lookup, Ok(candidates) if candidates.is_empty()),
            "the repository owner has no App installation"
        );
    }

    /// A `requested_action` reject from an authorized reviewer fails the held
    /// environment job closed: the gate records the rejecting identity, the
    /// job never dispatches, and the deployment's status is `failure` — the
    /// terminal conclusion of the failed job re-posts `failure`, never
    /// `success`.
    #[tokio::test]
    async fn check_run_requested_action_reject_fails_held_environment_job() {
        use axum::routing::{get, patch, post};
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = axum::Router::new()
            .route(
                "/repos/owner/repo/environments/prod",
                get(|| async {
                    Json(serde_json::json!({
                        "protection_rules": [{
                            "type": "required_reviewers",
                            "prevent_self_review": false,
                            "reviewers": [
                                {"type": "User", "reviewer": {"login": "octocat"}},
                            ],
                        }],
                    }))
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment_protection_rules",
                get(|| async {
                    Json(serde_json::json!({"custom_deployment_protection_rules": []}))
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("check-runs/{id}:{body}"));
                            Json(serde_json::json!({"id": id}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        fixture.state.environment_resolver.set_github_configured();
        fixture
            .state
            .backend
            .set_environment_resolver(fixture.state.environment_resolver.clone());
        fixture
            .state
            .environment_resolver
            .resolve_at(&api_base, &shared, "owner/repo", "prod")
            .await
            .expect("environment rules resolve");

        let run_id = RunId::new();
        let job_id = preloop_gha_protocol::JobId("deploy".to_owned());
        let mut submit = crate::control::tests::submit_run(
            run_id,
            vec![crate::control::tests::submit_job(run_id, "deploy", 1)],
        );
        submit.jobs[0].queued.environment = Some(serde_json::json!({
            "name": "prod",
            "url": "https://staging.example.com",
        }));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        fixture.state.backend.submit_run(submit).await.unwrap();
        fixture
            .state
            .backend
            .promote_ready_jobs(Some(run_id))
            .await
            .unwrap();
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();
        fixture
            .state
            .backend
            .set_job_deployment(run_id, &job_id, 556)
            .await
            .unwrap();
        crate::github::announce_environment_gates(&shared, Some(run_id)).await;

        let payload = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "reject"},
            "check_run": {"id": 99, "head_sha": "abc123"},
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": "octocat"},
        })
        .to_string();
        assert_eq!(
            fixture
                .post_body("del-reject", Some("check_run"), payload.as_bytes())
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        assert_eq!(
            fixture
                .state
                .backend
                .job_queue_state(run_id, &job_id)
                .await
                .unwrap(),
            Some(("none".to_owned(), "failure".to_owned())),
            "a rejected deployment never dispatches"
        );
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("the gate records the rejection");
        assert_eq!(gate.rejected_by.as_deref(), Some("octocat"));
        let audit = fixture
            .state
            .backend
            .environment_approvals(run_id, &job_id)
            .await
            .unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].decision, "rejected");
        assert_eq!(audit[0].actor.as_deref(), Some("octocat"));

        // The terminal conclusion of the failed job re-posts `failure`; the
        // deployment never reports `success`.
        crate::github::report_check_run_completed(
            &shared,
            run_id,
            &job_id,
            preloop_gha_protocol::ExecutionStatus::Failure,
        )
        .await;
        let requests = requests.lock().clone();
        let statuses = deployment_statuses(&requests, 556);
        assert_eq!(
            statuses.first().map(|(state, _)| state.as_str()),
            Some("pending"),
            "the announce posts `pending`: {requests:?}"
        );
        assert_eq!(
            statuses.get(1).map(|(state, _)| state.as_str()),
            Some("failure"),
            "the reject click reports the deployment's failure: {requests:?}"
        );
        assert!(
            statuses.iter().all(|(state, _)| state != "success"),
            "a rejected deployment never reports success: {requests:?}"
        );
        assert_eq!(
            statuses.last().map(|(state, _)| state.as_str()),
            Some("failure"),
            "the failed job's terminal status is `failure`: {requests:?}"
        );
        assert_eq!(
            statuses.last().and_then(|(_, url)| url.as_deref()),
            Some("https://staging.example.com"),
            "the failure status carries the environment url: {requests:?}"
        );
    }

    /// A `requested_action` from a login outside the reviewer set is ignored:
    /// the gate stays armed and the job stays held (fail closed).
    #[tokio::test]
    async fn check_run_requested_action_denies_unauthorized_sender() {
        use axum::routing::{get, patch, post};
        let stub = axum::Router::new()
            .route(
                "/repos/owner/repo/environments/prod",
                get(|| async {
                    Json(serde_json::json!({
                        "protection_rules": [{
                            "type": "required_reviewers",
                            "prevent_self_review": false,
                            "reviewers": [
                                {"type": "User", "reviewer": {"login": "octocat"}},
                            ],
                        }],
                    }))
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment_protection_rules",
                get(|| async {
                    Json(serde_json::json!({"custom_deployment_protection_rules": []}))
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post(|| async { Json(serde_json::json!({"id": 1})) }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch(|| async { Json(serde_json::json!({"id": 99})) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        // Restored when the guards drop (including on panic): a leaked stub
        // base or token breaks every later env-sensitive test in the binary.
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let fixture = WebhookFixture::new(&temp).await;
        let shared = Arc::new(SharedState {
            state: fixture.state.clone(),
            shutdown: CancellationToken::new(),
        });
        fixture.state.environment_resolver.set_github_configured();
        fixture
            .state
            .backend
            .set_environment_resolver(fixture.state.environment_resolver.clone());
        fixture
            .state
            .environment_resolver
            .resolve_at(&api_base, &shared, "owner/repo", "prod")
            .await
            .unwrap();

        let run_id = RunId::new();
        let job_id = preloop_gha_protocol::JobId("deploy".to_owned());
        let mut submit = crate::control::tests::submit_run(
            run_id,
            vec![crate::control::tests::submit_job(run_id, "deploy", 1)],
        );
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        fixture.state.backend.submit_run(submit).await.unwrap();
        fixture
            .state
            .backend
            .promote_ready_jobs(Some(run_id))
            .await
            .unwrap();
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();
        fixture
            .state
            .backend
            .set_job_deployment(run_id, &job_id, 555)
            .await
            .unwrap();
        crate::github::announce_environment_gates(&shared, Some(run_id)).await;

        let payload = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "approve"},
            "check_run": {"id": 99, "head_sha": "abc123"},
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": "intruder"},
        })
        .to_string();
        assert_eq!(
            fixture
                .post_body("del-deny", Some("check_run"), payload.as_bytes())
                .await,
            StatusCode::ACCEPTED
        );
        fixture.drain().await;

        let read = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists");
        let gate = read.gate.expect("the gate stays armed");
        assert!(
            gate.approvals.is_empty(),
            "an unauthorized click records nothing"
        );
        assert_eq!(
            fixture
                .state
                .backend
                .job_queue_state(run_id, &job_id)
                .await
                .unwrap(),
            Some(("blocked".to_owned(), "pending".to_owned())),
            "the job stays parked on the reviewer gate"
        );
    }

    // ── Environment side-channel regressions ───────────────────────────

    /// The environment side-channels PATCH check runs (the `action_required`
    /// review surface) in addition to writing deployments, so the App token
    /// they mint must ask for `checks: write` — not only `deployments:write`
    /// and `contents:read`.
    #[tokio::test]
    async fn environment_token_requests_checks_write() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (api, requests) = start_github_stub(|method, path, _body| {
            if method == "GET" && path == "/app/installations/101" {
                return (
                    200,
                    vec![],
                    serde_json::json!({"account": {"login": "owner"}}),
                );
            }
            if path.contains("/access_tokens") {
                return (
                    201,
                    vec![],
                    serde_json::json!({"token": "ghs-env", "expires_at": "2099-01-01T00:00:00Z"}),
                );
            }
            (404, vec![], serde_json::json!({"message": "not found"}))
        })
        .await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api);
        // No PAT: the App path must mint.
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "");

        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let mut app = crate::github_app::GitHubAppCredentials::for_tests(
            "424",
            rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap(),
            crate::github_app::MintFailurePolicy::LocalJwt,
        );
        app.installation_id = Some(101);
        state.github_app = Some(app.clone());
        state.github_apps = Some(crate::github_app::GitHubApps {
            apps: vec![app],
            default_index: 0,
        });
        let shared = state.shared();

        assert_eq!(
            resolve_environment_token(&shared, "owner/repo")
                .await
                .as_deref(),
            Some("ghs-env")
        );
        let mint = requests
            .lock()
            .iter()
            .find(|(method, path, _)| method == "POST" && path.contains("/access_tokens"))
            .map(|(_, _, body)| body.clone())
            .expect("the side-channel mints an installation token");
        let permissions = mint["permissions"].as_object().expect("permissions object");
        assert_eq!(
            permissions.get("checks").and_then(Value::as_str),
            Some("write"),
            "patch_check_run_actions needs checks:write: {mint}"
        );
        assert_eq!(
            permissions.get("deployments").and_then(Value::as_str),
            Some("write"),
            "deployment create/statuses need deployments:write: {mint}"
        );
        assert_eq!(
            permissions.get("contents").and_then(Value::as_str),
            Some("read"),
            "private environment reads need contents:read: {mint}"
        );
    }

    /// A config-file PAT (`github.pat`, stored on `state.github_pat`) covers
    /// the deployment/review side-channels exactly like
    /// `PRELOOP_GITHUB_TOKEN`; an empty env value counts as unset, never as
    /// an empty bearer.
    #[tokio::test]
    async fn repo_token_falls_back_to_config_pat() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "");

        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        state.github_pat = Some(preloop_gha_protocol::SecretString::new(
            "config_pat".to_owned(),
        ));
        let shared = state.shared();

        assert_eq!(
            resolve_environment_token(&shared, "owner/repo")
                .await
                .as_deref(),
            Some("config_pat"),
            "the config-file PAT answers when no App covers the repository"
        );

        let temp2 = tempfile::tempdir().unwrap();
        let bare = AppState::new(temp2.path().to_path_buf()).await.unwrap();
        let shared = bare.shared();
        assert_eq!(
            resolve_environment_token(&shared, "owner/repo").await,
            None,
            "an empty PRELOOP_GITHUB_TOKEN is unset, not an empty bearer"
        );
    }

    /// Submit one `environment: prod` job and arm its required-reviewer gate:
    /// rules resolve from `api_base` first so the sweep stamps the pending
    /// approval. Returns the fixture + shared handle + ids, with the job
    /// still held.
    async fn held_environment_fixture(
        temp: &tempfile::TempDir,
        api_base: &str,
    ) -> (WebhookFixture, std::sync::Arc<SharedState>, RunId, JobId) {
        let fixture = WebhookFixture::new(temp).await;
        let shared = fixture.state.shared();
        fixture.state.environment_resolver.set_github_configured();
        fixture
            .state
            .backend
            .set_environment_resolver(fixture.state.environment_resolver.clone());
        fixture
            .state
            .environment_resolver
            .resolve_at(api_base, &shared, "owner/repo", "prod")
            .await
            .expect("environment rules resolve");
        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = crate::control::tests::submit_run(
            run_id,
            vec![crate::control::tests::submit_job(run_id, "deploy", 1)],
        );
        submit.jobs[0].queued.environment = Some(serde_json::json!({
            "name": "prod",
            "url": "https://staging.example.com",
        }));
        submit.jobs[0].queued.environment_gate =
            Some(crate::models::EnvironmentGateState::default());
        fixture.state.backend.submit_run(submit).await.unwrap();
        fixture
            .state
            .backend
            .promote_ready_jobs(Some(run_id))
            .await
            .unwrap();
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("approval gate armed");
        assert!(
            gate.approval_requested_at_unix_nanos.is_some(),
            "the required-reviewer gate must stamp the wait"
        );
        assert!(
            !gate.approval_announced,
            "the gate starts unannounced: the announce loop stamps it"
        );
        (fixture, shared, run_id, job_id)
    }

    /// The environment-rules endpoints the resolver reads for `prod`:
    /// `octocat` is the only required reviewer.
    fn environment_rules_routes() -> axum::Router {
        use axum::routing::get;
        axum::Router::new()
            .route(
                "/repos/owner/repo/environments/prod",
                get(|| async {
                    Json(serde_json::json!({
                        "protection_rules": [{
                            "type": "required_reviewers",
                            "prevent_self_review": false,
                            "reviewers": [
                                {"type": "User", "reviewer": {"login": "octocat"}},
                            ],
                        }],
                    }))
                }),
            )
            .route(
                "/repos/owner/repo/environments/prod/deployment_protection_rules",
                get(|| async {
                    Json(serde_json::json!({"custom_deployment_protection_rules": []}))
                }),
            )
    }

    /// The announce sweep's expected surfaces: the check-run PATCH the moment
    /// the run's check run id lands, and the deployment's `pending` status.
    /// A gate scanned before its check run id is persisted (creation is a
    /// separate spawned task) or whose deployment create failed must stay
    /// unannounced so the reaper retries — stamping it would permanently drop
    /// the Approve/Reject buttons or the pending status.
    #[tokio::test]
    async fn announce_retries_when_expected_surfaces_are_missing() {
        use axum::routing::{patch, post};
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = environment_rules_routes()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 555}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("check-runs/{id}:{body}"));
                            Json(serde_json::json!({"id": id}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let (fixture, shared, run_id, job_id) = held_environment_fixture(&temp, &api_base).await;
        // The run reports check runs, but the check-run creation task has not
        // stamped the job's id yet — exactly the submit-time split between the
        // check-run and announce tasks.
        fixture
            .state
            .backend
            .set_reports_check_runs(run_id, true)
            .await
            .unwrap();

        crate::github::announce_environment_gates(&shared, Some(run_id)).await;
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("gate stays armed");
        assert!(
            !gate.approval_announced,
            "a missing check run on a reporting run must not stamp announced"
        );
        assert!(
            requests
                .lock()
                .iter()
                .all(|request| !request.contains("check-runs/")),
            "no check-run PATCH without an id: {:?}",
            requests.lock()
        );
        assert!(
            requests
                .lock()
                .iter()
                .any(|request| request.contains("deployments/555/statuses")),
            "the deployment surface is independent and still posted: {:?}",
            requests.lock()
        );

        // The mint lands; the next sweep PATCHes the check run and stamps the
        // gate.
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();
        crate::github::announce_environment_gates(&shared, Some(run_id)).await;
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("gate stays armed");
        assert!(
            gate.approval_announced,
            "the retry announces once the check run id exists"
        );
        assert!(
            requests
                .lock()
                .iter()
                .any(|request| request.contains("check-runs/99")
                    && request.contains("\"conclusion\":\"action_required\"")),
            "the retry PATCHes the Approve/Reject surface: {:?}",
            requests.lock()
        );
    }

    /// Same retry contract for the deployment surface: a failed create leaves
    /// the gate unannounced, and the next sweep creates it.
    #[tokio::test]
    async fn announce_retries_when_deployment_create_fails() {
        use axum::routing::post;
        let fail = std::sync::Arc::new(parking_lot::Mutex::new(true));
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = environment_rules_routes()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    let fail = fail.clone();
                    move || {
                        let seen = seen.clone();
                        let fail = fail.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            if *fail.lock() {
                                return (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(serde_json::json!({"message": "boom"})),
                                );
                            }
                            (StatusCode::OK, Json(serde_json::json!({"id": 777})))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let (fixture, shared, run_id, job_id) = held_environment_fixture(&temp, &api_base).await;

        crate::github::announce_environment_gates(&shared, Some(run_id)).await;
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("gate stays armed");
        assert!(
            !gate.approval_announced,
            "a failed deployment create must not stamp announced"
        );
        assert!(
            requests
                .lock()
                .iter()
                .all(|request| !request.contains("statuses")),
            "no status posts without a deployment: {:?}",
            requests.lock()
        );

        *fail.lock() = false;
        crate::github::announce_environment_gates(&shared, Some(run_id)).await;
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("gate stays armed");
        assert!(
            gate.approval_announced,
            "the retry creates the deployment and announces"
        );
        let requests = requests.lock().clone();
        assert_eq!(
            deployment_statuses(&requests, 777)
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["pending"],
            "the retried announce posts the pending status: {requests:?}"
        );
    }

    /// The `in_progress` deployment status is asynchronous: a job that
    /// finished before it lands must never be reported running again. The
    /// persisted terminal state fences the post.
    #[tokio::test]
    async fn late_in_progress_report_never_overtakes_terminal() {
        use axum::routing::post;
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = axum::Router::new()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 42}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();

        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut submit = crate::control::tests::submit_run(
            run_id,
            vec![crate::control::tests::submit_job(run_id, "deploy", 1)],
        );
        submit.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        fixture_submit(&state, submit).await;

        // Started, then terminal before the in_progress wake ran.
        report_check_run_in_progress(&shared, run_id, &job_id).await;
        state
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE jobs SET status = 'failure', queue_state = 'none' \
                     WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run_id.to_string(), "deploy"],
                )
                .unwrap();
                db.execute(
                    "UPDATE job_requests SET started_at = 1, finished_at = 2 \
                     WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run_id.to_string(), "deploy"],
                )
                .unwrap();
            })
            .await;
        report_check_run_completed(
            &shared,
            run_id,
            &job_id,
            preloop_gha_protocol::ExecutionStatus::Failure,
        )
        .await;
        report_check_run_in_progress(&shared, run_id, &job_id).await;

        let requests = requests.lock().clone();
        let statuses = deployment_statuses(&requests, 42);
        assert_eq!(
            statuses
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["in_progress", "failure"],
            "the terminal status is the last word — no late in_progress: {requests:?}"
        );
    }

    /// `submit_run` through `state` (a bare `AppState` instead of the webhook
    /// fixture) — kept tiny so each test reads linearly.
    async fn fixture_submit(state: &AppState, submit: crate::control::types::SubmitRun) {
        state.backend.submit_run(submit).await.unwrap();
    }

    /// A job skipped by `if:` never reaches a runner, so GitHub creates no
    /// Deployment for it. Concluding it must not mint one — nor post a
    /// failure status for a deployment GitHub never made.
    #[tokio::test]
    async fn skipped_environment_job_creates_no_deployment() {
        use axum::routing::post;
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = axum::Router::new()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 42}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();

        let run_id = RunId::new();
        let job_id = JobId("deploy".to_owned());
        let mut job = crate::control::tests::submit_job(run_id, "deploy", 1);
        job.queued.environment = Some(serde_json::json!("prod"));
        job.initially_skipped = true;
        let submit = crate::control::tests::submit_run(run_id, vec![job]);
        fixture_submit(&state, submit).await;

        report_check_run_completed(
            &shared,
            run_id,
            &job_id,
            preloop_gha_protocol::ExecutionStatus::Skipped,
        )
        .await;

        {
            let requests = requests.lock().clone();
            assert!(
                requests
                    .iter()
                    .all(|request| !request.contains("deployments")),
                "a skipped job never creates a deployment or posts a status: {requests:?}"
            );
        }

        // Contrast: a job the runner actually started does carry a
        // deployment — its conclusion posts `success` against it.
        let run2 = RunId::new();
        let job2 = JobId("deploy".to_owned());
        let mut submit2 = crate::control::tests::submit_run(
            run2,
            vec![crate::control::tests::submit_job(run2, "deploy", 1)],
        );
        submit2.jobs[0].queued.environment = Some(serde_json::json!("prod"));
        fixture_submit(&state, submit2).await;
        state
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE job_requests SET started_at = 1 WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run2.to_string(), "deploy"],
                )
                .unwrap();
            })
            .await;
        report_check_run_completed(
            &shared,
            run2,
            &job2,
            preloop_gha_protocol::ExecutionStatus::Success,
        )
        .await;
        let requests = requests.lock().clone();
        assert_eq!(
            deployment_statuses(&requests, 42)
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["success"],
            "a job that ran reports its terminal status: {requests:?}"
        );
    }

    /// A gate-engaged job that never dispatched (rejected by a reviewer)
    /// still owns a GitHub deployment — GitHub creates it when the review
    /// wait starts — so its conclusion posts `failure`, creating the
    /// deployment on the spot when the announce path's create was lost.
    #[tokio::test]
    async fn rejected_environment_job_concludes_with_deployment() {
        use axum::routing::post;
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = environment_rules_routes()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 88}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let (fixture, shared, run_id, job_id) = held_environment_fixture(&temp, &api_base).await;
        // No announce ran and no deployment is stamped: the rejection lands
        // before any side-channel pass succeeded.
        fixture
            .state
            .backend
            .record_environment_approval(crate::control::types::EnvironmentApproval {
                run_id,
                job_id: job_id.clone(),
                decision: crate::control::types::EnvironmentDecision::Reject,
                actor: Some("octocat".to_owned()),
                admin_override: false,
                note: None,
            })
            .await
            .unwrap();
        report_check_run_completed(
            &shared,
            run_id,
            &job_id,
            preloop_gha_protocol::ExecutionStatus::Failure,
        )
        .await;

        let requests = requests.lock().clone();
        assert!(
            requests.iter().any(|request| request == "deployments"),
            "a gate-engaged job keeps its deployment even though it never ran: {requests:?}"
        );
        assert_eq!(
            deployment_statuses(&requests, 88)
                .iter()
                .map(|(state, _)| state.as_str())
                .collect::<Vec<_>>(),
            vec!["failure"],
            "the rejected job's deployment concludes failure: {requests:?}"
        );
    }

    /// A `requested_action` whose `repository.full_name` names a different
    /// repository than the held job's never authorizes a review — the gate
    /// records nothing (fail closed). Matching is case-insensitive, matching
    /// GitHub's repository-name semantics.
    #[tokio::test]
    async fn requested_action_must_match_the_held_jobs_repository() {
        use axum::routing::{patch, post};
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = environment_rules_routes()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 555}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("check-runs/{id}:{body}"));
                            Json(serde_json::json!({"id": id}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let (fixture, shared, run_id, job_id) = held_environment_fixture(&temp, &api_base).await;
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();

        // A foreign repository's click records nothing.
        let foreign = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "approve"},
            "check_run": {"id": 99},
            "repository": {"full_name": "evil/repo"},
            "sender": {"login": "octocat"},
        });
        process_check_run_requested_action(&shared, &foreign)
            .await
            .expect("the delivery answers Ok, it just ignores it");
        let read = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists");
        assert!(
            read.gate.expect("gate stays armed").approvals.is_empty(),
            "a delivery naming another repository records no approval"
        );

        // The same click under the right repository (case-insensitive) lands.
        let ours = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "approve"},
            "check_run": {"id": 99},
            "repository": {"full_name": "OWNER/Repo"},
            "sender": {"login": "octocat"},
        });
        process_check_run_requested_action(&shared, &ours)
            .await
            .expect("the matching delivery processes");
        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("the approval recorded");
        assert_eq!(gate.approvals.len(), 1);
        assert_eq!(gate.approvals[0].actor.as_deref(), Some("octocat"));
    }

    /// A recorded approval that does not satisfy the required reviewer count
    /// is not a decision: GitHub keeps the gate pending, so nothing may be
    /// PATCHed or posted — reporting it would read as a rejection.
    #[tokio::test]
    async fn unsatisfied_approval_reports_nothing_to_github() {
        use axum::routing::{patch, post};
        let requests = std::sync::Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let seen = requests.clone();
        let stub = environment_rules_routes()
            .route(
                "/repos/owner/repo/deployments",
                post({
                    let seen = seen.clone();
                    move || {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push("deployments".to_owned());
                            Json(serde_json::json!({"id": 555}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/deployments/{id}/statuses",
                post({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock()
                                .push(format!("deployments/{id}/statuses:{body}"));
                            Json(serde_json::json!({"id": 1}))
                        }
                    }
                }),
            )
            .route(
                "/repos/owner/repo/check-runs/{id}",
                patch({
                    let seen = seen.clone();
                    move |axum::extract::Path(id): axum::extract::Path<u64>,
                          Json(body): Json<Value>| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().push(format!("check-runs/{id}:{body}"));
                            Json(serde_json::json!({"id": id}))
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, stub).await.unwrap() });

        let _env_lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "ghp_test_token");
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);

        let temp = tempfile::tempdir().unwrap();
        let (fixture, shared, run_id, job_id) = held_environment_fixture(&temp, &api_base).await;
        fixture
            .state
            .backend
            .set_job_check_run(run_id, &job_id, 99)
            .await
            .unwrap();
        // Raise the quorum past one approval (config validation caps
        // `required_reviewers` at 1 today; the gate blob carries the stamped
        // requirement so the reporting path stays honest when a quorum is
        // possible).
        fixture
            .state
            .test_db_mutate(|db| {
                db.execute(
                    "UPDATE jobs SET environment_gate = \
                         json_set(environment_gate, '$.approvals_required', 2) \
                     WHERE run_id = ?1 AND job_id = ?2",
                    rusqlite::params![run_id.to_string(), "deploy"],
                )
                .unwrap();
            })
            .await;

        let payload = serde_json::json!({
            "action": "requested_action",
            "requested_action": {"identifier": "approve"},
            "check_run": {"id": 99},
            "repository": {"full_name": "owner/repo"},
            "sender": {"login": "octocat"},
        });
        process_check_run_requested_action(&shared, &payload)
            .await
            .expect("the delivery processes");

        let gate = fixture
            .state
            .backend
            .environment_gate(run_id, &job_id)
            .await
            .unwrap()
            .expect("job exists")
            .gate
            .expect("the approval recorded");
        assert_eq!(gate.approvals.len(), 1);
        assert!(
            !fixture
                .state
                .backend
                .pending_environment_approvals(Some(run_id))
                .await
                .unwrap()
                .is_empty(),
            "one of two approvals keeps the job gated"
        );
        let requests = requests.lock().clone();
        assert!(
            requests
                .iter()
                .all(|request| !request.contains("check-runs/") && !request.contains("deployments")),
            "an unsatisfied approval touches no GitHub surface: {requests:?}"
        );
    }
}
