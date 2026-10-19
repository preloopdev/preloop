//! Lifecycle of GitHub App installation tokens minted for dispatched jobs.
//!
//! Unlike an App JWT, a job's installation token must be sent to the job (the
//! workflow's `GITHUB_TOKEN`), so it is the one credential a compromised guest
//! can exfiltrate and replay. Preloop therefore:
//!
//! 1. records the token (sealed with the cluster key, see [`crate::store::Envelope`])
//!    together with the owning attempt and its expiry *before* the token enters
//!    the job message;
//! 2. refuses the token for GitHub-compatible dispatch authentication once its
//!    attempt is terminal or the token is settled, including after a restart
//!    and ahead of the short-TTL online-validation cache;
//! 3. revokes it with `DELETE /installation/token` authenticated by the token
//!    itself on every terminal path — normal completion, cancellation, timeout,
//!    dead-runner reaping and attempt retries — and retries transient GitHub
//!    failures with capped backoff until the token's own expiry.
//!
//! Engine side-channel mints (check runs, snapshots, remote actions) are not
//! job-owned and must not be tracked or revoked here: their lifetime is the
//! engine's own. Official runners talking directly to github.com never receive
//! an engine-minted token, so the engine has no revocation authority there.
//!
//! Limits, stated plainly: the raw installation token has to exist in the
//! database, sealed, for the revoke call; anyone who can read both the control
//! database and the cluster key can unseal it. If GitHub is unreachable when a
//! job ends, the token stays usable until GitHub accepts the revoke, the
//! engine exhausts nothing (rows are retried until expiry), or the token's
//! own expiry passes. The engine cannot revoke a static PAT (the operator
//! chose to keep PAT support) or an OIDC JWT/downstream STS credential, which
//! are issued outside its authority.

use std::time::{Duration, SystemTime};

use preloop_gha_protocol::crypto::sha256_hex;
use preloop_gha_protocol::{JobId, RunId, SecretString};
use tracing::{debug, error, info, warn};

use crate::control::backend::ControlBackend;
use crate::state::SharedState;
use crate::store::Envelope;

/// How long a reconciler node holds a claimed row before another pass may
/// retry it. It must exceed one pass's worst case
/// ([`RECONCILE_BATCH`] × [`REVOCATION_HTTP_TIMEOUT`]) so a sibling node does
/// not race an in-flight revoke.
const CLAIM_LEASE: Duration = Duration::from_secs(300);

/// Hard deadline for one `DELETE /installation/token` call. A hung GitHub
/// socket must not pin a claimed row (or the lifecycle worker) indefinitely.
const REVOCATION_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Delay between lifecycle passes.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// First retry delay after a transient GitHub failure; doubles per attempt.
const BASE_RETRY: Duration = Duration::from_secs(10);

/// Ceiling for retry backoff. Rows are kept until the token expires, so an
/// outage delays revocation but never forgets it.
const MAX_RETRY: Duration = Duration::from_secs(30 * 60);

/// Tombstones outlive the token by this much so a dispatch-token cache entry
/// minted just before expiry (60s TTL) cannot outlive the tombstone.
const TOMBSTONE_GRACE: Duration = Duration::from_secs(5 * 60);

/// Rows one reconciler pass may attempt. Bounded so a large crash backlog is
/// drained over several worker passes; the pass runs on its own task, never
/// on the job-death reaper.
const RECONCILE_BATCH: usize = 8;

/// Identity of the attempt a job token was minted for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobTokenProvenance {
    pub request_id: i64,
    pub run_id: RunId,
    pub job_id: JobId,
    pub agent_job_id: uuid::Uuid,
}

/// Counters from one reconciliation pass, for logs, status and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileReport {
    pub claimed: usize,
    pub revoked: usize,
    pub spent: usize,
    pub retried: usize,
    pub cleanup_failed: usize,
}

/// Durably record a freshly minted job token before it enters the job message
/// and associate it with its attempt in the in-memory dispatch ledger.
///
/// Any failure here refuses the dispatch: the caller must not deliver a token
/// whose revocation is untracked. An inline best-effort revoke is attempted
/// first, because the freshly minted credential would otherwise live at GitHub
/// until its natural expiry.
pub(crate) async fn track_job_token(
    shared: &SharedState,
    provenance: JobTokenProvenance,
    token: &str,
    expires_at: SystemTime,
    app_id: &str,
    installation_id: u64,
    repository: &str,
) -> Result<String, String> {
    let token_hash = sha256_hex(token);
    let envelope = Envelope::new(&shared.state.local_jwt_key);
    let sealed_token = match envelope
        .seal_with_associated_data(token.as_bytes(), token_hash.as_bytes())
    {
        Ok(sealed) => sealed,
        Err(error) => {
            // The token must not stay dispatchable in the ledger after we
            // refused delivery; try an inline revoke and forget it.
            forget_ledger_token(shared, &token_hash);
            let inline =
                revoke_installation_token_at(&crate::github::github_api_base(), token).await;
            return Err(format!(
                "refusing to dispatch: could not seal the installation token for durable revocation: {error:#} (inline revoke: {inline:?})"
            ));
        }
    };
    let now = SystemTime::now();
    let now_us = system_to_us(now);
    let expires_at_us = system_to_us(expires_at);
    let record = crate::control::types::NewGitHubTokenRevocation {
        token_hash: token_hash.clone(),
        request_id: Some(provenance.request_id),
        run_id: provenance.run_id,
        job_id: provenance.job_id,
        agent_job_id: provenance.agent_job_id,
        app_id: app_id.to_owned(),
        installation_id,
        repository: repository.to_owned(),
        sealed_token,
        expires_at_us,
        next_attempt_at_us: now_us,
        created_at_us: now_us,
    };
    if let Err(error) = shared
        .state
        .backend
        .record_github_token_revocation(record)
        .await
    {
        forget_ledger_token(shared, &token_hash);
        let inline = revoke_installation_token_at(&crate::github::github_api_base(), token).await;
        match &inline {
            RevocationAttempt::Revoked | RevocationAttempt::Spent => warn!(
                token_hash = %token_hash,
                request_id = provenance.request_id,
                "recorded revocation failed; revoked the fresh installation token inline before refusing dispatch"
            ),
            _ => warn!(
                token_hash = %token_hash,
                request_id = provenance.request_id,
                ?inline,
                "recorded revocation failed and the inline revoke did not confirm; the undelivered token will expire at GitHub without engine tracking"
            ),
        }
        return Err(format!(
            "refusing to dispatch: could not durably record the GitHub App token's revocation: {error}"
        ));
    }
    let live_in_ledger =
        ledger_for_app(shared, app_id).is_some_and(|ledger| ledger.lookup(token).is_some());
    if !live_in_ledger {
        // The mint ledger no longer holds the token (expired during a slow
        // mint, or the ledger was rebuilt). Never deliver it: the durable row
        // will revoke it when the failed attempt reaches its terminal state.
        forget_ledger_token(shared, &token_hash);
        return Err(
            "refusing to dispatch: the minted GitHub App token is no longer live in the mint ledger"
                .to_owned(),
        );
    }
    debug!(
        token_hash = %token_hash,
        request_id = provenance.request_id,
        app_id,
        installation_id,
        expires_at_us,
        "recorded job installation token for terminal revocation"
    );
    Ok(token_hash)
}

/// Shutdown-aware lifecycle worker. Runs its own bounded pass every
/// [`RECONCILE_INTERVAL`], never on the job-death reaper: a hung GitHub socket
/// delays token cleanup, not timeout/lease reaping.
pub async fn run(shared: std::sync::Arc<SharedState>) {
    let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if shared.shutdown.is_cancelled() {
            break;
        }
        tokio::select! {
            _ = reconcile(&shared) => {}
            _ = shared.shutdown.cancelled() => break,
        }
        tokio::select! {
            _ = interval.tick() => {}
            _ = shared.shutdown.cancelled() => break,
        }
    }
}

/// One reconciliation pass. Called by [`run`] and by tests with an explicit
/// clock/API base.
pub(crate) async fn reconcile(shared: &SharedState) {
    let report = reconcile_at(shared, SystemTime::now(), &crate::github::github_api_base()).await;
    if report.claimed > 0 {
        if report.retried > 0 {
            warn!(
                claimed = report.claimed,
                revoked = report.revoked,
                spent = report.spent,
                retried = report.retried,
                "GitHub App token revocation pass left credentials pending; will retry"
            );
        } else {
            info!(
                claimed = report.claimed,
                revoked = report.revoked,
                spent = report.spent,
                "revoked terminal job GitHub App tokens"
            );
        }
    }
}

pub(crate) async fn reconcile_at(
    shared: &SharedState,
    now: SystemTime,
    api_base: &str,
) -> ReconcileReport {
    let mut report = ReconcileReport::default();
    let now_us = system_to_us(now);
    // Expired tombstones are no longer needed: the credential itself is dead.
    // Keep the grace window so the dispatch cache cannot outlive the tombstone.
    let cleanup_before = now_us.saturating_sub(duration_us(TOMBSTONE_GRACE));
    match shared
        .state
        .backend
        .expire_github_token_revocations(cleanup_before)
        .await
    {
        Ok(_) => {}
        Err(error) => {
            report.cleanup_failed += 1;
            warn!(
                ?error,
                "GitHub App token revocation tombstone cleanup failed"
            );
        }
    }
    let due = match shared
        .state
        .backend
        .due_github_token_revocations(now_us, RECONCILE_BATCH, duration_us(CLAIM_LEASE))
        .await
    {
        Ok(due) => due,
        Err(error) => {
            warn!(?error, "GitHub App token revocation claim failed");
            return report;
        }
    };
    report.claimed = due.len();
    let envelope = Envelope::new(&shared.state.local_jwt_key);
    for row in due {
        let token = match envelope
            .unseal_with_associated_data(row.sealed_token(), row.token_hash.as_bytes())
        {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(token) => SecretString::new(token),
                Err(_) => {
                    // Corrupt ciphertext: cannot call GitHub. Keep the row and
                    // retry after the normal backoff; expiry still bounds it.
                    let message = "sealed token is not valid UTF-8";
                    error!(
                        token_hash = %row.token_hash,
                        request_id = ?row.request_id,
                        "cannot unseal GitHub App token for revocation: {message}"
                    );
                    schedule_retry(shared, &row, message, None, now, &mut report).await;
                    continue;
                }
            },
            Err(error) => {
                let message = format!("cannot unseal sealed token: {error:#}");
                error!(
                    token_hash = %row.token_hash,
                    request_id = ?row.request_id,
                    "cannot unseal GitHub App token for revocation: {message}"
                );
                schedule_retry(shared, &row, &message, None, now, &mut report).await;
                continue;
            }
        };
        let attempt = if row.expires_at_us <= now_us {
            // The credential has already expired at GitHub; no revoke call can
            // succeed. Tombstone it so cached validation cannot accept it.
            RevocationAttempt::Spent
        } else {
            revoke_installation_token_at(api_base, token.expose()).await
        };
        match attempt {
            RevocationAttempt::Revoked => {
                settle(
                    shared,
                    &row,
                    crate::control::types::TokenRevocationResolution::Revoked,
                    "revoked",
                )
                .await;
                report.revoked += 1;
            }
            RevocationAttempt::Spent => {
                settle(
                    shared,
                    &row,
                    crate::control::types::TokenRevocationResolution::Spent,
                    "already invalid",
                )
                .await;
                report.spent += 1;
            }
            RevocationAttempt::Retry { error, retry_after }
            | RevocationAttempt::Refused { error, retry_after } => {
                schedule_retry(shared, &row, &error, retry_after, now, &mut report).await;
            }
        }
    }
    report
}

/// Whether a token may authenticate a GitHub-compatible dispatch right now.
///
/// A tracked job token is rejected as soon as its attempt is terminal (or the
/// attempt row is gone), even while remote revocation is still pending and
/// even after a restart has emptied the in-memory mint ledger. `None` means
/// the hash is not a tracked job token, so the existing ledger/online paths
/// apply unchanged.
pub(crate) async fn dispatchable_token(
    shared: &SharedState,
    token_hash: &str,
    now_us: i64,
) -> Result<Option<bool>, crate::control::types::ControlError> {
    shared
        .state
        .backend
        .job_token_dispatchable(token_hash, now_us)
        .await
}

/// `DELETE /installation/token`, authenticated with the token being revoked.
///
/// This is the only GitHub endpoint that can revoke an installation token
/// without the App private key. It returns `204` on success; `401` means the
/// credential is already invalid. Other statuses are classified as transient
/// (network, rate limit, server fault) or explicit refusals; neither forgets
/// the row.
async fn revoke_installation_token_at(api_base: &str, token: &str) -> RevocationAttempt {
    let url = format!("{api_base}/installation/token");
    let response = crate::shared_http::CLIENT
        .delete(&url)
        .header("User-Agent", "preloop")
        .header("Authorization", format!("token {token}"))
        .header("Accept", "application/vnd.github+json")
        .timeout(REVOCATION_HTTP_TIMEOUT)
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return RevocationAttempt::Retry {
                // Never format the request: reqwest errors can include headers.
                error: format!("DELETE {url} transport failure: {error}"),
                retry_after: None,
            };
        }
    };
    let status = response.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return RevocationAttempt::Revoked;
    }
    if status.is_success() {
        // GitHub documents `204 No Content`. Any other 2xx is not proof the
        // credential was revoked; retain the obligation and retry rather
        // than reporting a success the API did not state.
        return RevocationAttempt::Refused {
            error: format!("DELETE {url} returned unexpected success status {status}"),
            retry_after: None,
        };
    }
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs);
    let detail = format!("DELETE {url} returned {status}");
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return RevocationAttempt::Spent;
    }
    let rate_limited = status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || (status == reqwest::StatusCode::FORBIDDEN && retry_after.is_some());
    if rate_limited || status.is_server_error() {
        RevocationAttempt::Retry {
            error: detail,
            retry_after,
        }
    } else {
        // Explicit refusal (400/403 without rate-limit hints/404/422/…).
        // Keep the obligation: do not claim the token was revoked, and retry
        // with a long backoff until it expires.
        RevocationAttempt::Refused {
            error: detail,
            retry_after,
        }
    }
}

async fn settle(
    shared: &SharedState,
    row: &crate::control::types::GitHubTokenRevocation,
    resolution: crate::control::types::TokenRevocationResolution,
    verb: &str,
) {
    match shared
        .state
        .backend
        .settle_github_token_revocation(&row.token_hash, resolution)
        .await
    {
        Ok(()) => {
            forget_ledger_token(shared, &row.token_hash);
            debug!(
                token_hash = %row.token_hash,
                request_id = ?row.request_id,
                state = ?resolution,
                "GitHub App job token {verb}; tombstone retained until expiry"
            );
        }
        Err(error) => {
            // The remote credential is gone or invalid, so the in-memory
            // ledger must stop accepting it regardless of the write failure.
            forget_ledger_token(shared, &row.token_hash);
            warn!(
                token_hash = %row.token_hash,
                ?error,
                "could not settle GitHub App token revocation row; it will be retried"
            );
        }
    }
}

async fn schedule_retry(
    shared: &SharedState,
    row: &crate::control::types::GitHubTokenRevocation,
    message: &str,
    retry_after: Option<Duration>,
    now: SystemTime,
    report: &mut ReconcileReport,
) {
    let delay = retry_after
        .unwrap_or_default()
        .max(backoff(row.attempts))
        .min(MAX_RETRY);
    let next = system_to_us(now).saturating_add(duration_us(delay));
    match shared
        .state
        .backend
        .retry_github_token_revocation(&row.token_hash, message, next)
        .await
    {
        Ok(()) => {
            report.retried += 1;
            warn!(
                token_hash = %row.token_hash,
                request_id = ?row.request_id,
                attempts = row.attempts + 1,
                retry_in_secs = delay.as_secs(),
                error = %message,
                "GitHub App token revocation failed; retained and rescheduled"
            );
        }
        Err(error) => {
            report.retried += 1;
            warn!(
                token_hash = %row.token_hash,
                ?error,
                "could not reschedule GitHub App token revocation; the next pass retries it"
            );
        }
    }
}

/// Exponential backoff capped at [`MAX_RETRY`], derived from failed attempts.
///
/// The multiplier saturates instead of being clamped to `2^7`; clamping the
/// shift made the cap unreachable (1280s < the documented 1800s).
fn backoff(attempts: u32) -> Duration {
    let factor = 1u32.checked_shl(attempts.min(31)).unwrap_or(u32::MAX);
    BASE_RETRY.saturating_mul(factor).min(MAX_RETRY)
}

/// The mint ledger of the App that minted `app_id`, when configured.
fn ledger_for_app<'a>(
    shared: &'a SharedState,
    app_id: &str,
) -> Option<&'a crate::github_app::MintLedger> {
    if let Some(app) = &shared.state.github_app
        && app.app_id == app_id
    {
        return Some(&app.mint_ledger);
    }
    shared
        .state
        .github_apps
        .as_ref()
        .and_then(|registry| registry.app_by_id(app_id))
        .map(|app| app.mint_ledger.as_ref())
}

/// Stop offline acceptance for this token hash in every configured ledger.
/// Hash collision across Apps is not a concern: SHA-256 over a fresh token.
fn forget_ledger_token(shared: &SharedState, token_hash: &str) {
    if let Some(apps) = &shared.state.github_apps {
        for app in &apps.apps {
            app.mint_ledger.forget(token_hash);
        }
    }
    if let Some(app) = &shared.state.github_app {
        app.mint_ledger.forget(token_hash);
    }
}

fn duration_us(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

pub(crate) fn system_to_us(time: SystemTime) -> i64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Test-support entry points for the isolated behavioral tests. They expose
/// only outcomes (counts, booleans), never sealed material. Integration-test
/// crates build the library with `test-support`.
#[cfg(any(test, feature = "test-support"))]
pub async fn reconcile_at_for_tests(
    shared: &SharedState,
    now: SystemTime,
    api_base: &str,
) -> ReconcileReport {
    reconcile_at(shared, now, api_base).await
}

#[cfg(any(test, feature = "test-support"))]
pub async fn token_dispatchable_for_tests(shared: &SharedState, token: &str) -> Option<bool> {
    token_dispatchable_at_for_tests(shared, token, SystemTime::now()).await
}

#[cfg(any(test, feature = "test-support"))]
pub async fn token_dispatchable_at_for_tests(
    shared: &SharedState,
    token: &str,
    now: SystemTime,
) -> Option<bool> {
    dispatchable_token(shared, &sha256_hex(token), system_to_us(now))
        .await
        .ok()
        .flatten()
}

/// What one `DELETE /installation/token` call proved.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RevocationAttempt {
    Revoked,
    Spent,
    Retry {
        error: String,
        retry_after: Option<Duration>,
    },
    Refused {
        error: String,
        retry_after: Option<Duration>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0), BASE_RETRY);
        assert_eq!(backoff(1), BASE_RETRY * 2);
        assert_eq!(backoff(2), BASE_RETRY * 4);
        assert_eq!(backoff(7), BASE_RETRY * 128);
        assert_eq!(backoff(8), MAX_RETRY);
        assert_eq!(backoff(20), MAX_RETRY);
    }
}
