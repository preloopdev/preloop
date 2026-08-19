//! Webhook-driven auto-PR: when a push-triggered run succeeds, open a pull
//! request for its branch per policy.
//!
//! This is the "seamless" flow: developers push to GitHub normally, CI runs
//! on preloop, and the server opens the PR — no `preloop run` step, no hook.
//! It deliberately does **not** touch push-back runs (`submission.push` set):
//! those are client-managed by `preloop run --push` / `github_push.rs`, which
//! own their own PR creation.
//!
//! Policy (`config::PrConfig`): open for feature branches by default (any
//! branch that is not the repository's default branch, with no existing open
//! PR, and not excluded by pattern); never open for the default branch or
//! tags; dedup against an existing open PR for the head. Head-commit labels
//! override: `[no-pr]` skips, `[draft]` opens as draft, `[pr]` forces an
//! open even under `auto = never`.

use std::sync::Arc;

use crate::config::PrAuto;
use crate::{RunId, SharedState};
use serde_json::Value;

/// Best-effort entry point. Runs detached from the completion path so a
/// GitHub outage never affects the run's own result; failures are logged,
/// never returned.
pub(crate) async fn maybe_open_pr(shared: Arc<SharedState>, run_id: RunId) {
    if let Err(error) = maybe_open_pr_inner(&shared, run_id).await {
        tracing::warn!(%run_id, ?error, "auto-PR: not opened");
    }
}

/// Whether the run qualifies and, if so, the PR it opened.
///
/// Quiet no-ops (policy skip, run not applicable) return `Ok(false)`; only
/// real failures (missing credentials, GitHub refusing) propagate.
async fn maybe_open_pr_inner(shared: &Arc<SharedState>, run_id: RunId) -> anyhow::Result<bool> {
    // Snapshot the fields we need under the lock, then drop it before any
    // network I/O.
    let (repository, git_ref, payload) = {
        let inner = shared.state.inner.lock().await;
        let run = inner
            .runs
            .get(&run_id)
            .ok_or_else(|| anyhow::anyhow!("run {run_id} not found"))?;
        if run.conclusion.as_deref() != Some("success") {
            return Ok(false);
        }
        if run.event != "push" {
            return Ok(false);
        }
        // Only webhook-delivered runs carry a trust tier (the dispatcher
        // stamps it). A native `/api/v1/runs` caller setting `event = "push"`
        // is a local submission, not a GitHub push, and must not trigger
        // auto-PR.
        if crate::events::trust_tier::tier_of(&run.submission).is_none() {
            return Ok(false);
        }
        // Push-back runs are client-managed: `github_push.rs` owns their PR.
        if run.submission.push.is_some() {
            return Ok(false);
        }
        // A local-only submission (no real `owner/repo` slug) can never have
        // a PR opened for it.
        let Some((owner, repo)) = run.submission.repository.split_once('/') else {
            return Ok(false);
        };
        if owner.is_empty() || repo.is_empty() || repo.contains('/') {
            return Ok(false);
        }
        (
            run.submission.repository.clone(),
            run.submission.git_ref.clone(),
            run.submission.payload.clone(),
        )
    };

    let Some(branch) = git_ref.strip_prefix("refs/heads/") else {
        return Ok(false);
    };
    if branch.is_empty() || branch == "refs/heads/" {
        return Ok(false);
    }

    let config = &shared.state.pr_config;
    let labels = pr_labels_from_payload(&payload);
    if labels.no_pr {
        return Ok(false);
    }
    if config.auto == PrAuto::Never && !labels.force {
        return Ok(false);
    }
    let draft = labels.draft.unwrap_or(config.draft);

    // Never PR the default branch, and honor the exclusion patterns.
    let default_branch = payload
        .get("repository")
        .and_then(|repo| repo.get("default_branch"))
        .and_then(Value::as_str)
        .unwrap_or("main");
    if branch == default_branch {
        return Ok(false);
    }
    if config
        .exclude
        .iter()
        .any(|pattern| branch_matches(pattern, branch))
    {
        return Ok(false);
    }

    let token = crate::github_push::push_token(shared, &repository)
        .await
        .ok_or_else(|| anyhow::anyhow!("no GitHub credentials configured"))?;

    // Dedup: reuse an existing open PR for the head instead of creating a
    // second one.
    let (owner, _) = repository.split_once('/').expect("checked above");
    let head = format!("{owner}:{branch}");
    let query = serde_urlencoded::to_string([("head", head.as_str()), ("state", "open")])
        .expect("url-encoding cannot fail for str pairs");
    let existing = crate::github_push::github_json(
        &token,
        &repository,
        "GET",
        &format!("pulls?{query}"),
        None,
    )
    .await?;
    if existing.as_array().is_some_and(|pulls| !pulls.is_empty()) {
        return Ok(false);
    }

    let body = format!(
        "CI run `{run_id}` completed with `success`.\n\n\
         - Head: `{branch}`\n\
         - Details: {}",
        crate::github::run_details_url(run_id)
            .unwrap_or_else(|| "local server (no public URL configured)".to_owned())
    );
    let pr = crate::github_push::github_json(
        &token,
        &repository,
        "POST",
        "pulls",
        Some(serde_json::json!({
            "title": branch,
            "head": branch,
            "base": default_branch,
            "body": body,
            "draft": draft,
        })),
    )
    .await
    .map_err(|error| {
        let mut message = format!("could not create pull request: {error}");
        if format!("{error}").contains("status 403") {
            message.push_str(
                ". To auto-open pull requests, grant the App `pull_requests: write` \
                 (App settings → Permissions → Pull requests → Read and write)",
            );
        }
        anyhow::anyhow!(message)
    })?;
    let number = pr.get("number").and_then(Value::as_u64).unwrap_or_default();
    tracing::info!(%run_id, %branch, %default_branch, draft, number, "auto-PR opened");
    Ok(true)
}

/// Head-commit labels parsed from the push payload's head commit message.
#[derive(Debug, Clone, Copy, Default)]
struct PrLabels {
    no_pr: bool,
    draft: Option<bool>,
    force: bool,
}

/// Scan the push payload's **head** commit message for `[no-pr]`, `[draft]`,
/// and `[pr]` labels. GitHub push payloads carry the head commit explicitly
/// and also as the last element of `commits`; labels in *older* commits of
/// the same push must not override the head's intent (an earlier `[no-pr]`
/// must not suppress the PR for the current head).
fn pr_labels_from_payload(payload: &Value) -> PrLabels {
    let mut labels = PrLabels::default();
    let message = payload
        .get("head_commit")
        .and_then(|c| c.get("message"))
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .get("commits")
                .and_then(Value::as_array)
                .and_then(|commits| commits.last())
                .and_then(|commit| commit.get("message"))
                .and_then(Value::as_str)
        });
    if let Some(message) = message {
        let lower = message.to_ascii_lowercase();
        if lower.contains("[no-pr]") {
            labels.no_pr = true;
        }
        if lower.contains("[draft]") {
            labels.draft = Some(true);
        }
        if lower.contains("[pr]") {
            labels.force = true;
        }
    }
    labels
}

/// gitignore-style pattern match for branch names: `*` matches any run of
/// characters (including `/`), and a trailing `/` matches everything below
/// that prefix. No pattern is an exact match.
fn branch_matches(pattern: &str, branch: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    if let Some(prefix) = pattern.strip_suffix('/') {
        return branch.starts_with(prefix);
    }
    if pattern == "*" {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == branch;
    }
    // Anchor the first literal as a prefix and the last as a suffix; scan
    // the middle literals in order between them. Matching the last literal
    // at its first occurrence (as the old code did) let trailing bytes make
    // `x*y` miss `xyy` and `*-wip` miss `feat-wip-wip`.
    let first = parts[0];
    if !first.is_empty() && !branch.starts_with(first) {
        return false;
    }
    let last = parts[parts.len() - 1];
    if !last.is_empty() && !branch.ends_with(last) {
        return false;
    }
    let mut rest = if first.is_empty() {
        branch
    } else {
        &branch[first.len()..]
    };
    if !last.is_empty() {
        // `ends_with` matched at the end, so this is a byte boundary.
        if rest.len() < last.len() {
            return false;
        }
        rest = &rest[..rest.len() - last.len()];
    }
    for part in &parts[1..parts.len() - 1] {
        if part.is_empty() {
            continue;
        }
        let Some(pos) = rest.find(part) else {
            return false;
        };
        rest = &rest[pos + part.len()..];
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PrConfig;

    fn payload_with_messages(messages: &[&str]) -> Value {
        serde_json::json!({
            "commits": messages.iter().map(|m| serde_json::json!({"message": m})).collect::<Vec<_>>(),
            "repository": { "default_branch": "main" },
        })
    }

    #[test]
    fn labels_from_head_commit_message() {
        let labels = pr_labels_from_payload(&payload_with_messages(&["fix: thing\n\n[draft]"]));
        assert!(labels.draft == Some(true));
        assert!(!labels.no_pr);
        assert!(!labels.force);

        let labels = pr_labels_from_payload(&payload_with_messages(&["chore: x [no-pr]"]));
        assert!(labels.no_pr);

        let labels = pr_labels_from_payload(&payload_with_messages(&["feat: y [pr]"]));
        assert!(labels.force);
    }

    #[test]
    fn labels_only_read_the_head_commit() {
        // GitHub lists the most recent commit last; an older `[no-pr]` in the
        // same push must not suppress the PR for the current head.
        let labels =
            pr_labels_from_payload(&payload_with_messages(&["chore: a [no-pr]", "feat: b"]));
        assert!(!labels.no_pr, "an older [no-pr] must not apply to the head");

        // An explicit `head_commit` wins over the `commits` array.
        let mut payload = payload_with_messages(&["chore: c [no-pr]"]);
        payload["head_commit"] = serde_json::json!({"message": "feat: d [pr]"});
        let labels = pr_labels_from_payload(&payload);
        assert!(labels.force);
        assert!(!labels.no_pr);
    }

    #[test]
    fn labels_fallback_to_head_commit_when_commits_absent() {
        let payload = serde_json::json!({
            "head_commit": { "message": "fix: z [draft]" },
            "repository": { "default_branch": "main" },
        });
        let labels = pr_labels_from_payload(&payload);
        assert!(labels.draft == Some(true));
    }

    #[test]
    fn branch_pattern_matching() {
        assert!(branch_matches("release/*", "release/v1"));
        assert!(branch_matches("release/", "release/v1"));
        assert!(!branch_matches("release/", "hotfix/x"));
        assert!(branch_matches("gh-pages", "gh-pages"));
        assert!(!branch_matches("gh-pages", "gh-pages-foo"));
        assert!(branch_matches("*-experiment", "foo-experiment"));
        assert!(!branch_matches("*-experiment", "foo-experimental"));
        assert!(branch_matches("*", "anything"));
        // The final literal is a suffix, not a first occurrence: trailing
        // bytes must not make a matching pattern miss.
        assert!(branch_matches("*-wip", "feat-wip-wip"));
        assert!(branch_matches("*ab", "abab"));
        assert!(!branch_matches("*-wip", "feat-wip-x"));
        assert!(branch_matches("foo*bar", "foobar"));
        assert!(branch_matches("foo*bar*", "foobar"));
        assert!(!branch_matches("foo*bar", "foobarbaz"));
        assert!(!branch_matches("foo*foo", "foo"));
        assert!(branch_matches("a*a", "aa"));
    }

    #[test]
    fn pr_config_defaults() {
        let config = PrConfig::default();
        assert_eq!(config.auto, PrAuto::Feature);
        assert!(
            config.draft,
            "draft must default true (draft PRs are safer)"
        );
        assert!(config.exclude.is_empty());
    }

    #[tokio::test]
    async fn auto_pr_opens_draft_and_respects_labels_and_dedup() {
        use axum::body::{to_bytes, Body};
        use axum::http::{header, Method, Request, StatusCode};
        use axum::{routing::get, routing::post, Json as AxumJson, Router};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;
        use tower::ServiceExt;

        // Stub api.github.com. `existing` flips the pulls list (no open PR →
        // one open PR); `created_count`/`created_body` record PR-create calls.
        // One stub + one env var + one sequential test: the PRELOOP_GITHUB_API_URL
        // env var is process-wide, so two concurrent tests would race each other.
        let existing = Arc::new(AtomicBool::new(false));
        let created_count = Arc::new(AtomicUsize::new(0));
        let created_body = Arc::new(parking_lot::Mutex::new(None));
        let (existing_for_stub, count_for_stub, body_for_stub) = (
            existing.clone(),
            created_count.clone(),
            created_body.clone(),
        );
        let stub = Router::new()
            .route(
                "/repos/acme/web/pulls",
                get(move || {
                    let existing = existing_for_stub.clone();
                    async move {
                        if existing.load(Ordering::SeqCst) {
                            AxumJson(serde_json::json!([
                                {"number": 7, "head": {"ref": "feature/x"}}
                            ]))
                        } else {
                            AxumJson(serde_json::json!([]))
                        }
                    }
                }),
            )
            .route(
                "/repos/acme/web/pulls",
                post(move |AxumJson(body): AxumJson<serde_json::Value>| {
                    count_for_stub.fetch_add(1, Ordering::SeqCst);
                    *body_for_stub.lock() = Some(body);
                    async { AxumJson(serde_json::json!({"number": 42})) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, stub).await.unwrap();
        });
        // The GitHub env vars are process-global; serialize against every
        // other test that pins them (see the push-back test pattern).
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let _api_url =
            crate::state::TestEnvVar::set("PRELOOP_GITHUB_API_URL", format!("http://{addr}"));

        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        state.github_pat = Some(preloop_gha_protocol::SecretString::new(String::from(
            "test-pat",
        )));

        // Submit a webhook-style push run for a feature branch and mark the
        // run successful (the real trigger lives in complete_job_inner; here
        // we drive maybe_open_pr_inner directly).
        let submit_successful = |state: &crate::AppState, message: &str| {
            let state = state.clone();
            let message = message.to_owned();
            async move {
                let app = crate::app(state.clone(), CancellationToken::new());
                let body = serde_json::json!({
                    "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
                    "event": "push",
                    "repository": "acme/web",
                    "git_ref": "refs/heads/feature/x",
                    "payload": {
                        "repository": { "default_branch": "main" },
                        "commits": [{"message": message}],
                    },
                });
                let response = app
                    .oneshot(
                        Request::builder()
                            .method(Method::POST)
                            .uri("/api/v1/runs")
                            .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(Body::from(body.to_string()))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let accepted: serde_json::Value = serde_json::from_slice(
                    &to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                )
                .unwrap();
                let run_id = crate::RunId(
                    uuid::Uuid::parse_str(accepted["run_id"].as_str().unwrap()).unwrap(),
                );
                {
                    let mut inner = state.inner.lock().await;
                    let run = inner.runs.get_mut(&run_id).expect("run recorded");
                    run.conclusion = Some("success".to_owned());
                    // The webhook dispatcher stamps the trust tier; native
                    // submissions carry none and are never auto-PR'd.
                    Arc::make_mut(&mut run.submission).trust_tier = Some("internal".to_owned());
                }
                run_id
            }
        };
        let shared = Arc::new(crate::SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });

        // 1. Successful feature-branch push with no label → draft PR opens.
        let run_id = submit_successful(&state, "feat: x").await;
        let opened = maybe_open_pr_inner(&shared, run_id).await.unwrap();
        assert!(
            opened,
            "policy must open a PR for a successful feature-branch push"
        );
        assert_eq!(created_count.load(Ordering::SeqCst), 1);
        let created = created_body.lock().clone().expect("PR create called");
        assert_eq!(created["head"], "feature/x");
        assert_eq!(created["base"], "main");
        assert_eq!(created["draft"], serde_json::Value::Bool(true));

        // 2. [no-pr] label skips under the default policy.
        let run_id = submit_successful(&state, "chore: y [no-pr]").await;
        let opened = maybe_open_pr_inner(&shared, run_id).await.unwrap();
        assert!(!opened, "[no-pr] must suppress the PR");
        assert_eq!(created_count.load(Ordering::SeqCst), 1, "no new PR create");

        // 3. Existing open PR for the head → dedup, no create.
        existing.store(true, Ordering::SeqCst);
        let run_id = submit_successful(&state, "feat: z [pr]").await;
        let opened = maybe_open_pr_inner(&shared, run_id).await.unwrap();
        assert!(
            !opened,
            "an existing open PR must be reused, not duplicated"
        );
        assert_eq!(created_count.load(Ordering::SeqCst), 1, "no new PR create");
    }

    #[tokio::test]
    async fn native_push_submission_never_auto_opens_a_pr() {
        use axum::body::{to_bytes, Body};
        use axum::http::{header, Method, Request, StatusCode};
        use tokio_util::sync::CancellationToken;
        use tower::ServiceExt;

        // A native `/api/v1/runs` caller may set `event = "push"`; without a
        // webhook-stamped trust tier the run must never auto-open a PR.
        let temp = tempfile::tempdir().unwrap();
        let mut state = crate::AppState::new(temp.path().to_path_buf())
            .await
            .unwrap();
        state.github_pat = Some(preloop_gha_protocol::SecretString::new(String::from(
            "test-pat",
        )));
        let app = crate::app(state.clone(), CancellationToken::new());
        let body = serde_json::json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "acme/web",
            "git_ref": "refs/heads/feature/x",
            "trust_tier": "internal",
            "payload": { "repository": { "default_branch": "main" } },
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/runs")
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let accepted: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let run_id =
            crate::RunId(uuid::Uuid::parse_str(accepted["run_id"].as_str().unwrap()).unwrap());
        {
            let mut inner = state.inner.lock().await;
            let run = inner.runs.get_mut(&run_id).expect("run recorded");
            run.conclusion = Some("success".to_owned());
        }
        let shared = Arc::new(crate::SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        let opened = maybe_open_pr_inner(&shared, run_id).await.unwrap();
        assert!(
            !opened,
            "a native submission must never auto-open a PR (no webhook provenance)"
        );
    }

    #[tokio::test]
    async fn dirty_push_submission_records_snapshot_tree() {
        use axum::body::{to_bytes, Body};
        use axum::http::{header, Method, Request, StatusCode};
        use base64::Engine as _;
        use std::process::Command;
        use tokio_util::sync::CancellationToken;
        use tower::ServiceExt;

        // A git repo with one commit, then a dirty modification.
        let temp = tempfile::tempdir().unwrap();
        let ws = temp.path().join("ws");
        std::fs::create_dir_all(ws.join(".github/workflows")).unwrap();
        std::fs::write(
            ws.join(".github/workflows/build.yml"),
            "on: push\njobs:\n  b:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        )
        .unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .current_dir(&ws)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        };
        git(&["init", "-b", "main"]);
        git(&["config", "user.name", "Test"]);
        git(&["config", "user.email", "t@example.com"]);
        std::fs::write(ws.join("file.txt"), "v1\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-m", "base"]);
        let head = git(&["rev-parse", "HEAD"]);
        let head_tree = git(&["rev-parse", "HEAD^{tree}"]);
        std::fs::write(ws.join("file.txt"), "v2\n").unwrap();

        let state = crate::AppState::new(temp.path().join("state").to_path_buf())
            .await
            .unwrap();
        let app = crate::app(state.clone(), CancellationToken::new());
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(ws.as_os_str().to_string_lossy().as_bytes());
        let body = serde_json::json!({
            "workflow_yaml": "on: push\njobs:\n  b:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "acme/web",
            "git_ref": "refs/heads/feature/x",
            "sha": head,
            "push": { "create_pr": true, "draft_pr": true, "dirty": true },
            "payload": { "repository": { "default_branch": "main" } },
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/runs")
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-preloop-local-workspace", encoded)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        if response.status() != StatusCode::OK {
            let text =
                String::from_utf8_lossy(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                    .to_string();
            panic!("submit failed: {text}");
        }
        let accepted: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let run_id =
            crate::RunId(uuid::Uuid::parse_str(accepted["run_id"].as_str().unwrap()).unwrap());

        // The server must have recorded the snapshot's tree — the exact tree
        // CI tested — even though the submission carried no push_tree.
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).expect("run recorded");
        let push_tree = run
            .submission
            .push_tree
            .as_deref()
            .expect("snapshot tree recorded");
        assert_eq!(push_tree.len(), 40);
        assert!(push_tree.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(
            push_tree, head_tree,
            "the tested tree must be the dirty snapshot tree, not the base commit's"
        );
    }
}
