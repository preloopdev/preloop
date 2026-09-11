//! Source-state reconciler: the net for events that never existed.
//!
//! The delivery watchdog can repair anything GitHub *recorded*. It cannot
//! help when GitHub never generated a delivery at all — an event the App is
//! not subscribed to, a push that exceeded GitHub's tag/branch fan-out
//! limits, an outage on GitHub's own dispatch side. Nothing is in the
//! delivery history, so nothing can be redelivered.
//!
//! What is still true in that case is the repository's *current state*: a
//! branch head, an open pull request head. Comparing those against the runs
//! preloop created finds the gap, and synthesizing one webhook-shaped
//! payload per gap closes it through the exact same durable queue, event
//! adapters, trigger evaluation and check reporting a real delivery uses.
//! There is no second code path for "reconciled" runs.
//!
//! What this deliberately cannot do:
//!
//! * **Replay history.** A push of five commits is reconstructed as its head
//!   commit. `paths:` filters that depended on an intermediate commit will
//!   evaluate differently. The delivery payload stays authoritative; this is
//!   an approximation of the present, not a recording of the past.
//! * **Reproduce PR activity types.** A PR is synthesized as `synchronize`,
//!   the action that means "the head moved", because that is the only claim
//!   current state supports. A workflow filtering on `opened` or `labeled`
//!   will not fire.
//! * **Run by default.** Every synthesis is a CI run GitHub never asked for.
//!   The reconciler scans only repositories named in
//!   `PRELOOP_WEBHOOK_RECONCILE_REPOS`; with none listed it does nothing and
//!   makes no HTTP calls.
//!
//! The race it must not lose: a merely-late real webhook arriving after the
//! reconciler already synthesized would produce two runs for one commit,
//! under two different delivery ids, and delivery-id dedup cannot catch it.
//! Two defences: a grace window far longer than any plausible delivery
//! delay, and a durable reservation keyed by
//! `repository | event | ref | head sha` that survives restarts even when
//! in-memory run state does not.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};
use crate::state::SharedState;
use crate::webhook_status::now_us;

const DEFAULT_INTERVAL_SECS: u64 = 900;
const MIN_INTERVAL_SECS: u64 = 60;
const DEFAULT_GRACE_SECS: i64 = 900;

/// What one reconciliation pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ReconcileOutcome {
    pub(crate) repositories: u64,
    /// Heads considered after the grace window.
    pub(crate) candidates: u64,
    /// Deliveries actually enqueued.
    pub(crate) synthesized: u64,
    /// Heads that already had a run or a reservation.
    pub(crate) skipped_existing: u64,
}

/// A repository head that may need a run.
struct Candidate {
    event: &'static str,
    git_ref: String,
    head_sha: String,
    /// Age gate input: the commit/PR timestamp, not our observation time.
    updated_at: chrono::DateTime<chrono::Utc>,
    payload: Value,
}

/// Repositories the reconciler is allowed to scan.
///
/// Empty means disabled. This is opt-in on purpose: an unconfigured
/// reconciler that started creating runs across every installed repository
/// would be a far worse failure than the one it repairs.
pub(crate) fn reconciler_repositories() -> Vec<String> {
    std::env::var("PRELOOP_WEBHOOK_RECONCILE_REPOS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|repo| !repo.is_empty() && repo.contains('/'))
        .map(str::to_owned)
        .collect()
}

fn reconcile_interval() -> Duration {
    Duration::from_secs(
        std::env::var("PRELOOP_WEBHOOK_RECONCILE_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_INTERVAL_SECS)
            .max(MIN_INTERVAL_SECS),
    )
}

fn grace_us() -> i64 {
    std::env::var("PRELOOP_WEBHOOK_RECONCILE_GRACE_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(DEFAULT_GRACE_SECS)
        .max(0)
        * 1_000_000
}

/// Canonical idempotency key for a synthesized event.
///
/// Includes the head sha so a new commit is new work, and the ref so the
/// same commit reached from a branch and from a PR are still one decision
/// each.
fn idempotency_key(repository: &str, event: &str, git_ref: &str, head_sha: &str) -> String {
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    sha2::Digest::update(&mut hasher, repository.as_bytes());
    sha2::Digest::update(&mut hasher, b"|");
    sha2::Digest::update(&mut hasher, event.as_bytes());
    sha2::Digest::update(&mut hasher, b"|");
    sha2::Digest::update(&mut hasher, git_ref.as_bytes());
    sha2::Digest::update(&mut hasher, b"|");
    sha2::Digest::update(&mut hasher, head_sha.as_bytes());
    let digest = sha2::Digest::finalize(hasher);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Background loop; disabled cheaply when no repositories are configured.
pub(crate) async fn run_webhook_reconciler(shared: Arc<SharedState>) {
    let interval = reconcile_interval();
    loop {
        if shared.shutdown.is_cancelled() {
            break;
        }
        match reconcile_once(&shared).await {
            Ok(outcome) if outcome.synthesized > 0 => tracing::warn!(
                repositories = outcome.repositories,
                candidates = outcome.candidates,
                synthesized = outcome.synthesized,
                "source-state reconciler synthesized webhook deliveries GitHub never sent"
            ),
            Ok(outcome) => tracing::debug!(
                repositories = outcome.repositories,
                candidates = outcome.candidates,
                skipped_existing = outcome.skipped_existing,
                "source-state reconciler pass complete"
            ),
            Err(error) => tracing::warn!(?error, "source-state reconciler pass failed"),
        }
        tokio::select! {
            _ = shared.shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

/// One reconciliation pass over the configured repositories.
pub(crate) async fn reconcile_once(shared: &Arc<SharedState>) -> anyhow::Result<ReconcileOutcome> {
    let repositories = reconciler_repositories();
    if repositories.is_empty() {
        shared.state.webhook_status.update_reconciler(|status| {
            status.enabled = false;
        });
        return Ok(ReconcileOutcome::default());
    }
    shared.state.webhook_status.update_reconciler(|status| {
        status.enabled = true;
        status.last_run_at_us = Some(now_us());
    });
    if let Some(retry_after) = shared.state.github_breaker.retry_after() {
        tracing::debug!(
            retry_in_secs = retry_after.as_secs(),
            "GitHub breaker open; skipping source-state reconciliation"
        );
        return Ok(ReconcileOutcome::default());
    }

    let api_base = crate::github::github_api_base();
    let mut outcome = ReconcileOutcome::default();
    let mut errors: Vec<String> = Vec::new();
    for repository in &repositories {
        outcome.repositories += 1;
        match reconcile_repository(shared, &api_base, repository).await {
            Ok(repo_outcome) => {
                outcome.candidates += repo_outcome.candidates;
                outcome.synthesized += repo_outcome.synthesized;
                outcome.skipped_existing += repo_outcome.skipped_existing;
            }
            Err(error) => {
                tracing::warn!(%repository, ?error, "reconciling repository failed");
                errors.push(format!("{repository}: {error}"));
            }
        }
    }

    let synthesized = outcome.synthesized;
    let repositories_scanned = outcome.repositories;
    let succeeded = errors.is_empty();
    shared.state.webhook_status.update_reconciler(|status| {
        status.enabled = true;
        status.repositories_scanned = repositories_scanned;
        status.synthesized = status.synthesized.saturating_add(synthesized);
        if succeeded {
            status.last_success_at_us = Some(now_us());
            status.last_error = None;
        } else {
            status.last_error = Some(errors.join("; "));
        }
    });
    if !errors.is_empty() {
        anyhow::bail!("reconciliation failed: {}", errors.join("; "));
    }
    Ok(outcome)
}

async fn reconcile_repository(
    shared: &Arc<SharedState>,
    api_base: &str,
    repository: &str,
) -> anyhow::Result<ReconcileOutcome> {
    let Some(token) = repository_token(shared, api_base, repository).await? else {
        anyhow::bail!("no GitHub credential can read {repository}");
    };
    let mut outcome = ReconcileOutcome::default();
    let grace_boundary = chrono::Utc::now()
        - chrono::Duration::microseconds(grace_us().max(0)).max(chrono::Duration::zero());

    let repo_meta = get_json(shared, api_base, &token, &format!("/repos/{repository}")).await?;
    let default_branch = repo_meta
        .get("default_branch")
        .and_then(Value::as_str)
        .unwrap_or("main")
        .to_owned();

    let mut candidates = Vec::new();
    if let Some(candidate) =
        default_branch_candidate(shared, api_base, &token, repository, &default_branch).await?
    {
        candidates.push(candidate);
    }
    candidates.extend(
        open_pull_request_candidates(shared, api_base, &token, repository, &default_branch).await?,
    );

    for candidate in candidates {
        if candidate.updated_at > grace_boundary {
            // Still inside the window where a real delivery may yet arrive.
            continue;
        }
        outcome.candidates += 1;
        if run_exists(
            shared,
            repository,
            candidate.event,
            &candidate.git_ref,
            &candidate.head_sha,
        )
        .await
        {
            outcome.skipped_existing += 1;
            continue;
        }
        let key = idempotency_key(
            repository,
            candidate.event,
            &candidate.git_ref,
            &candidate.head_sha,
        );
        let reserved = shared
            .state
            .store
            .reserve_synthetic_webhook_event(
                &key,
                repository,
                candidate.event,
                &candidate.git_ref,
                &candidate.head_sha,
                now_us(),
            )
            .await?;
        if !reserved {
            outcome.skipped_existing += 1;
            continue;
        }
        let record = WebhookDeliveryRecord {
            delivery_id: format!("synthetic-{key}"),
            event: candidate.event.to_owned(),
            payload: serde_json::to_vec(&candidate.payload)?,
            received_at_us: now_us(),
            state: WebhookDeliveryStatus::Received,
            attempts: 0,
            lease_until_us: None,
            lease_token: None,
            last_error: None,
        };
        match shared.state.store.enqueue_webhook_delivery(&record).await {
            Ok(true) => {
                outcome.synthesized += 1;
                shared.state.webhook_queue_notify.notify_one();
                tracing::warn!(
                    %repository,
                    event = candidate.event,
                    git_ref = %candidate.git_ref,
                    sha = %candidate.head_sha,
                    "synthesized a webhook delivery from repository state"
                );
            }
            Ok(false) => {
                outcome.skipped_existing += 1;
            }
            Err(error) => {
                let _ = shared
                    .state
                    .store
                    .clear_synthetic_webhook_reservation(&key)
                    .await;
                return Err(error);
            }
        }
    }
    Ok(outcome)
}

/// Is there already a run for this head?
///
/// A fast path only: `RunRecord::head_sha` is not persisted, so after a
/// restart this answers "no" for runs that do exist. The durable
/// reservation, not this check, is what actually guarantees at-most-once.
async fn run_exists(
    shared: &Arc<SharedState>,
    repository: &str,
    event: &str,
    git_ref: &str,
    head_sha: &str,
) -> bool {
    let inner = shared.state.inner.lock().await;
    inner.runs.values().any(|run| {
        run.submission.repository == repository
            && run.submission.event == event
            && run.submission.git_ref == git_ref
            && run.head_sha == head_sha
    })
}

/// Credential ladder identical to the webhook path: App installation token
/// first (so an App-only deployment works), static PAT second.
async fn repository_token(
    shared: &Arc<SharedState>,
    api_base: &str,
    repository: &str,
) -> anyhow::Result<Option<String>> {
    if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await {
        let permissions = BTreeMap::from([
            ("contents".to_owned(), "read".to_owned()),
            ("pull_requests".to_owned(), "read".to_owned()),
        ]);
        return Ok(Some(
            crate::github_app::get_or_mint_token_at(api_base, &app, repository, &permissions)
                .await?,
        ));
    }
    Ok(shared.state.static_github_pat())
}

async fn default_branch_candidate(
    shared: &Arc<SharedState>,
    api_base: &str,
    token: &str,
    repository: &str,
    default_branch: &str,
) -> anyhow::Result<Option<Candidate>> {
    let commit = get_json(
        shared,
        api_base,
        token,
        &format!("/repos/{repository}/commits/{default_branch}"),
    )
    .await?;
    let Some(sha) = commit.get("sha").and_then(Value::as_str) else {
        return Ok(None);
    };
    let message = commit
        .get("commit")
        .and_then(|commit| commit.get("message"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let committed_at = commit
        .get("commit")
        .and_then(|commit| commit.get("committer"))
        .and_then(|committer| committer.get("date"))
        .and_then(Value::as_str)
        .and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok())
        .map(|date| date.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);
    let git_ref = format!("refs/heads/{default_branch}");
    // `commits` carries the head commit only: REST cannot tell us which
    // commits arrived in one push, and inventing a range would make
    // `paths:` filters lie rather than merely approximate.
    let payload = json!({
        "ref": git_ref,
        "before": "0000000000000000000000000000000000000000",
        "after": sha,
        "created": false,
        "deleted": false,
        "forced": false,
        "repository": {
            "full_name": repository,
            "default_branch": default_branch,
        },
        "head_commit": { "id": sha, "message": message },
        "commits": [{
            "id": sha,
            "message": message,
            "added": [],
            "modified": [],
            "removed": [],
        }],
        "preloop_synthetic": true,
    });
    Ok(Some(Candidate {
        event: "push",
        git_ref,
        head_sha: sha.to_owned(),
        updated_at: committed_at,
        payload,
    }))
}

async fn open_pull_request_candidates(
    shared: &Arc<SharedState>,
    api_base: &str,
    token: &str,
    repository: &str,
    default_branch: &str,
) -> anyhow::Result<Vec<Candidate>> {
    let pulls = get_json(
        shared,
        api_base,
        token,
        &format!("/repos/{repository}/pulls?state=open&per_page=100"),
    )
    .await?;
    let Some(pulls) = pulls.as_array() else {
        return Ok(Vec::new());
    };
    let mut candidates = Vec::new();
    for pull in pulls {
        let (Some(number), Some(head_sha), Some(head_ref), Some(base_ref)) = (
            pull.get("number").and_then(Value::as_u64),
            pull.get("head")
                .and_then(|head| head.get("sha"))
                .and_then(Value::as_str),
            pull.get("head")
                .and_then(|head| head.get("ref"))
                .and_then(Value::as_str),
            pull.get("base")
                .and_then(|base| base.get("ref"))
                .and_then(Value::as_str),
        ) else {
            continue;
        };
        let updated_at = pull
            .get("updated_at")
            .and_then(Value::as_str)
            .and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok())
            .map(|date| date.with_timezone(&chrono::Utc))
            .unwrap_or_else(chrono::Utc::now);
        let base_sha = pull
            .get("base")
            .and_then(|base| base.get("sha"))
            .and_then(Value::as_str)
            .unwrap_or(head_sha);
        let git_ref = format!("refs/pull/{number}/head");
        // `synchronize` is the only action current state can honestly
        // claim: it means "the head is at this sha", which is exactly what
        // was observed. Claiming `opened` would fabricate history.
        let payload = json!({
            "action": "synchronize",
            "number": number,
            "pull_request": {
                "number": number,
                "draft": pull.get("draft").and_then(Value::as_bool).unwrap_or(false),
                "title": pull.get("title").and_then(Value::as_str).unwrap_or_default(),
                "merge_commit_sha": pull.get("merge_commit_sha").cloned().unwrap_or(Value::Null),
                "labels": pull.get("labels").cloned().unwrap_or_else(|| json!([])),
                "head": {
                    "ref": head_ref,
                    "sha": head_sha,
                    "repo": pull
                        .get("head")
                        .and_then(|head| head.get("repo"))
                        .cloned()
                        .unwrap_or_else(|| json!({"full_name": "unknown/unknown", "fork": true})),
                },
                "base": { "ref": base_ref, "sha": base_sha },
            },
            "repository": {
                "full_name": repository,
                "default_branch": default_branch,
            },
            "preloop_synthetic": true,
        });
        candidates.push(Candidate {
            event: "pull_request",
            git_ref,
            head_sha: head_sha.to_owned(),
            updated_at,
            payload,
        });
    }
    Ok(candidates)
}

/// GET a GitHub JSON resource through the breaker.
async fn get_json(
    shared: &Arc<SharedState>,
    api_base: &str,
    token: &str,
    path: &str,
) -> anyhow::Result<Value> {
    let url = format!("{}{}", api_base.trim_end_matches('/'), path);
    let response = crate::github_breaker::send_observed(
        &shared.state.github_breaker,
        crate::shared_http::CLIENT
            .clone()
            .get(&url)
            .header("User-Agent", "preloop")
            .header("Authorization", format!("Bearer {token}"))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28"),
    )
    .await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GET {path} returned {status}: {body}");
    }
    Ok(response.json().await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn commit_json(sha: &str, age_secs: i64) -> Value {
        let date = chrono::Utc::now() - chrono::Duration::seconds(age_secs);
        json!({
            "sha": sha,
            "commit": {
                "message": "reconciled head",
                "committer": { "date": date.to_rfc3339() },
            },
        })
    }

    async fn stub_github(head: Value, calls: Arc<AtomicUsize>) -> String {
        let router = Router::new()
            .route(
                "/repos/:owner/:repo",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { Json(json!({"default_branch": "main", "full_name": "acme/app"})) }
                    }
                }),
            )
            .route(
                "/repos/:owner/:repo/commits/:branch",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let head = head.clone();
                        async move { Json(head) }
                    }
                }),
            )
            .route(
                "/repos/:owner/:repo/pulls",
                get({
                    let calls = calls.clone();
                    move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { Json(json!([])) }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }

    async fn shared_state() -> Arc<SharedState> {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.keep();
        let state = crate::AppState::new(path).await.unwrap();
        Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    #[tokio::test]
    async fn synthesizes_a_push_for_an_unrun_default_branch_head() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let api_base = stub_github(commit_json("a".repeat(40).as_str(), 3600), calls).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "reconcile-token");
        let _repos = crate::state::TestEnvVar::set("PRELOOP_WEBHOOK_RECONCILE_REPOS", "acme/app");
        let shared = shared_state().await;

        let outcome = reconcile_once(&shared).await.unwrap();

        assert_eq!(outcome.synthesized, 1, "{outcome:?}");
        let claimed = shared
            .state
            .store
            .claim_webhook_deliveries(1, 60)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].event, "push");
        let push: crate::github::PushEvent = serde_json::from_slice(&claimed[0].payload)
            .expect("synthesized payload must satisfy the push adapter's shape");
        assert_eq!(push.after, "a".repeat(40));
        assert_eq!(push.git_ref, "refs/heads/main");
        assert_eq!(push.repository.full_name, "acme/app");
    }

    #[tokio::test]
    async fn second_pass_does_not_synthesize_the_same_head_twice() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let api_base = stub_github(commit_json("b".repeat(40).as_str(), 3600), calls).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "reconcile-token");
        let _repos = crate::state::TestEnvVar::set("PRELOOP_WEBHOOK_RECONCILE_REPOS", "acme/app");
        let shared = shared_state().await;

        let first = reconcile_once(&shared).await.unwrap();
        let second = reconcile_once(&shared).await.unwrap();

        assert_eq!(first.synthesized, 1);
        assert_eq!(
            second.synthesized, 0,
            "the durable reservation must hold across passes"
        );
        assert_eq!(second.skipped_existing, 1);
    }

    #[tokio::test]
    async fn head_inside_the_grace_window_is_left_for_the_real_webhook() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let api_base = stub_github(commit_json("c".repeat(40).as_str(), 10), calls).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _token = crate::state::TestEnvVar::set("PRELOOP_GITHUB_TOKEN", "reconcile-token");
        let _repos = crate::state::TestEnvVar::set("PRELOOP_WEBHOOK_RECONCILE_REPOS", "acme/app");
        let shared = shared_state().await;

        let outcome = reconcile_once(&shared).await.unwrap();

        assert_eq!(outcome.candidates, 0, "{outcome:?}");
        assert_eq!(outcome.synthesized, 0);
    }

    #[tokio::test]
    async fn disabled_reconciler_makes_no_requests() {
        let _lock = crate::state::GITHUB_ENV_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let api_base = stub_github(commit_json("d".repeat(40).as_str(), 3600), calls.clone()).await;
        let _api = crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", &api_base);
        let _repos = crate::state::TestEnvVar::unset("PRELOOP_WEBHOOK_RECONCILE_REPOS");
        let shared = shared_state().await;

        let outcome = reconcile_once(&shared).await.unwrap();

        assert_eq!(outcome, ReconcileOutcome::default());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!shared.state.webhook_status.reconciler().enabled);
    }

    #[test]
    fn idempotency_key_separates_shas_refs_and_events() {
        let base = idempotency_key("acme/app", "push", "refs/heads/main", "abc");
        assert_ne!(
            base,
            idempotency_key("acme/app", "push", "refs/heads/main", "def")
        );
        assert_ne!(
            base,
            idempotency_key("acme/app", "pull_request", "refs/heads/main", "abc")
        );
        assert_ne!(
            base,
            idempotency_key("acme/other", "push", "refs/heads/main", "abc")
        );
    }
}
