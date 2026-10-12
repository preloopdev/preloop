//! Integration tests for self-built pull-request test merges.
//!
//! A local `pull_request` run tests the merge of the current base tip into the
//! head — the tree GitHub's `refs/pull/<n>/merge` holds — built by the engine
//! when GitHub's own merge is not available. These tests pin the behaviour end
//! to end: the run's commit, its parents, the payload labelling, the served
//! snapshot, conflict and unreachable-upstream failures, the `--no-merge`
//! escape, and the `prebuilt_merge` contract the hosted/webhook flows stack on.

mod common;
use common::*;

use preloop_runner_server::merge_builder::{
    MergeError, MergeHead, MergeOutcome, MergeRequest, MergeSource, attach_prebuilt_merge,
    build_merge, fetch_inputs, prebuilt_merge_record, webhook_merge_source,
};
use std::path::{Path, PathBuf};

/// `git <args>` in `cwd`, with a fixed identity so fixtures never depend on
/// the machine's git config. Panics on failure.
fn git(cwd: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(cwd)
        .arg("-c")
        .arg("commit.gpgsign=false")
        .env("GIT_AUTHOR_NAME", "Merge Test")
        .env("GIT_AUTHOR_EMAIL", "merge@example.test")
        .env("GIT_COMMITTER_NAME", "Merge Test")
        .env("GIT_COMMITTER_EMAIL", "merge@example.test")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("git {args:?}: {error}"));
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A workspace on `feature` whose `main` moved on `origin` after the branch
/// was cut, so the merge must use the *current* base tip, not the branch point.
struct MergeFixture {
    temp: tempfile::TempDir,
    state_dir: PathBuf,
    workspace: PathBuf,
    origin: PathBuf,
    head: String,
    base: String,
}

impl MergeFixture {
    /// Clean merge: the head adds a file, the base adds another.
    fn clean() -> Self {
        Self::new(false)
    }

    /// Conflicting merge: both sides edit `shared.txt` differently.
    fn conflicted() -> Self {
        Self::new(true)
    }

    fn new(conflict: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let origin = temp.path().join("origin.git");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        git(&workspace, &["init", "-q", "-b", "main"]);
        std::fs::write(workspace.join("base-only.txt"), "base\n").unwrap();
        std::fs::write(workspace.join("shared.txt"), "shared base\n").unwrap();
        git(&workspace, &["add", "-A"]);
        git(&workspace, &["commit", "-qm", "base"]);
        git(
            &workspace,
            &["init", "-q", "--bare", origin.to_str().unwrap()],
        );
        git(
            &workspace,
            &["remote", "add", "origin", origin.to_str().unwrap()],
        );
        git(&workspace, &["push", "-q", "origin", "main"]);

        git(&workspace, &["checkout", "-q", "-b", "feature"]);
        if conflict {
            std::fs::write(workspace.join("shared.txt"), "feature side\n").unwrap();
        } else {
            std::fs::write(workspace.join("head-only.txt"), "head\n").unwrap();
        }
        git(&workspace, &["add", "-A"]);
        git(&workspace, &["commit", "-qm", "head"]);

        // The base moves on the forge after the branch was cut.
        git(&workspace, &["checkout", "-q", "main"]);
        if conflict {
            std::fs::write(workspace.join("shared.txt"), "main side\n").unwrap();
        } else {
            std::fs::write(workspace.join("base-new.txt"), "new base\n").unwrap();
        }
        git(&workspace, &["add", "-A"]);
        git(&workspace, &["commit", "-qm", "base moved"]);
        git(&workspace, &["push", "-q", "origin", "main"]);
        git(&workspace, &["push", "-q", "origin", "feature"]);
        let base = git(&workspace, &["rev-parse", "main"]);
        git(&workspace, &["checkout", "-q", "feature"]);
        let head = git(&workspace, &["rev-parse", "HEAD"]);
        let state_dir = temp.path().join("state");
        Self {
            temp,
            state_dir,
            workspace,
            origin,
            head,
            base,
        }
    }

    fn submission(&self, no_merge: bool) -> serde_json::Value {
        serde_json::json!({
            "workflow_yaml": "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo merge\n",
            "event": "pull_request",
            "repository": "owner/repo",
            "git_ref": "refs/heads/feature",
            "workflow_path": ".github/workflows/ci.yml",
            "base_ref": "main",
            "filter_branch": "main",
            "no_merge": no_merge,
            "payload": {
                "number": 7,
                "pull_request": {
                    "number": 7,
                    "base": { "ref": "main", "sha": "0".repeat(40) },
                    "head": { "ref": "feature", "sha": self.head },
                },
            },
        })
    }

    async fn app(&self) -> (AppState, Router) {
        std::fs::create_dir_all(&self.state_dir).unwrap();
        let mut state = AppState::new(self.state_dir.clone()).await.unwrap();
        state.local_workspace = Some(self.workspace.clone());
        let app = app(state.clone(), CancellationToken::new());
        (state, app)
    }
}

/// The run tests the merge of the current base tip into the head, the payload
/// keeps GitHub's shape, and the engine serves the merge to the run's jobs.
#[tokio::test]
async fn local_pull_request_run_checks_out_the_merge_of_the_current_base_tip() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let mut submission = fixture.submission(false);
    submission["payload"]["pull_request"]
        .as_object_mut()
        .unwrap()
        .remove("base");
    let accepted = request_json(&app, Method::POST, "/api/v1/runs", submission).await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let (merge_sha, message) = {
        let inner = state.test_tx().await;
        let record = inner
            .runs
            .values()
            .find(|run| run.run_id.to_string() == run_id)
            .expect("run record");
        let snapshot = record
            .workspace_snapshot
            .as_ref()
            .expect("run must record its snapshot");
        let merge = snapshot.merge.as_ref().expect("merge record");
        assert_eq!(snapshot.commit_sha, merge.sha);
        assert_eq!(merge.base_sha, fixture.base);
        assert_eq!(merge.head_sha, fixture.head);
        assert_ne!(
            merge.sha, fixture.head,
            "the run must test the merge, not the branch tip"
        );
        assert_eq!(record.github["sha"], serde_json::json!(merge.sha));
        assert_eq!(record.github["base_ref"], "main");
        assert_eq!(
            record.github["event"]["pull_request"]["base"]["ref"],
            "main"
        );
        assert_eq!(record.github["ref"], "refs/pull/7/merge");
        // GitHub's shape: base.sha is the CURRENT base tip, head.sha the head.
        assert_eq!(
            record.github["event"]["pull_request"]["base"]["sha"],
            serde_json::json!(fixture.base)
        );
        assert_eq!(
            record.github["event"]["pull_request"]["head"]["sha"],
            serde_json::json!(fixture.head)
        );
        (merge.sha.clone(), queued_message_for(&inner, &run_id))
    };

    // The merge is a real two-parent commit: the fetched base tip first, the
    // head second (never the stale branch point).
    let served = fixture.state_dir.join("snapshots").join(&run_id);
    assert_eq!(git(&served, &["cat-file", "-t", &merge_sha]), "commit");
    assert_eq!(
        git(&served, &["log", "--format=%P", "-1", &merge_sha]),
        format!("{} {}", fixture.base, fixture.head)
    );
    let files = git(&served, &["ls-tree", "--name-only", &merge_sha]);
    for path in [
        "base-only.txt",
        "base-new.txt",
        "head-only.txt",
        "shared.txt",
    ] {
        assert!(
            files.contains(path),
            "merge tree must contain {path}: {files}"
        );
    }
    // A job-like clone of the engine-served repository resolves the merge,
    // so the run's checkout can fetch it without GitHub.
    let clone = fixture.temp.path().join("job-clone");
    git(
        fixture.temp.path(),
        &[
            "clone",
            "-q",
            served.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(git(&clone, &["cat-file", "-t", &merge_sha]), "commit");

    // The job message pins the merge as the checked-out commit.
    assert_eq!(
        message.preloop_snapshot_commit.as_deref(),
        Some(merge_sha.as_str())
    );
}

/// A real Git client can fetch and check out the merge through the authenticated
/// snapshot smart-HTTP endpoint, not just from the backing repository path.
#[tokio::test]
async fn local_pull_request_merge_is_fetchable_over_snapshot_http() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let mut submission = fixture.submission(false);
    submission["workflow_yaml"] = serde_json::json!(
        "on: pull_request\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo merge\n"
    );
    let accepted = request_json(&app, Method::POST, "/api/v1/runs", submission).await;
    let run_id = accepted["run_id"].as_str().unwrap().to_owned();
    let merge_sha = {
        let inner = state.test_tx().await;
        inner
            .runs
            .values()
            .find(|run| run.run_id.to_string() == run_id)
            .unwrap()
            .workspace_snapshot
            .as_ref()
            .unwrap()
            .merge
            .as_ref()
            .unwrap()
            .sha
            .clone()
    };
    let acquired = acquire_queued_job(&app, "merge-http-smoke").await;
    let runtime_token = service_endpoint_token(&acquired).unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_app = app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, server_app).await.unwrap();
    });

    let authorization = format!("http.extraHeader=AUTHORIZATION: Bearer {runtime_token}");
    let snapshot_url = format!("http://{address}/snapshots/{run_id}");
    let clone = fixture.temp.path().join("http-job-clone");
    let clone_output = tokio::process::Command::new("git")
        .arg("-c")
        .arg(&authorization)
        .args(["clone", "--quiet", "--no-checkout"])
        .arg(&snapshot_url)
        .arg(&clone)
        .output()
        .await
        .unwrap();
    assert!(
        clone_output.status.success(),
        "git clone over snapshot HTTP failed: {}",
        String::from_utf8_lossy(&clone_output.stderr)
    );

    let fetch_output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(&clone)
        .arg("-c")
        .arg(&authorization)
        .args(["fetch", "--quiet", "--no-tags", "--depth=1", "origin"])
        .arg(&merge_sha)
        .output()
        .await
        .unwrap();
    assert!(
        fetch_output.status.success(),
        "git fetch of the merge commit over snapshot HTTP failed: {}",
        String::from_utf8_lossy(&fetch_output.stderr)
    );
    git(&clone, &["checkout", "--quiet", "--detach", &merge_sha]);

    assert_eq!(git(&clone, &["rev-parse", "HEAD"]), merge_sha);
    let commit_object = git(&clone, &["cat-file", "-p", "HEAD"]);
    let parents = commit_object
        .lines()
        .filter_map(|line| line.strip_prefix("parent "))
        .collect::<Vec<_>>();
    assert_eq!(parents, vec![fixture.base.as_str(), fixture.head.as_str()]);
    for (path, expected) in [
        ("base-only.txt", "base\n"),
        ("base-new.txt", "new base\n"),
        ("head-only.txt", "head\n"),
        ("shared.txt", "shared base\n"),
    ] {
        assert_eq!(std::fs::read_to_string(clone.join(path)).unwrap(), expected);
    }
    server.abort();
}

/// A conflicted pull request fails before any job is queued, naming the
/// conflicted files — GitHub runs no pull_request workflows on a conflict.
#[tokio::test]
async fn conflicted_local_pull_request_fails_fast_and_creates_no_run() {
    let fixture = MergeFixture::conflicted();
    let (state, app) = fixture.app().await;
    let (status, body) = request_json_status(
        &app,
        Method::POST,
        "/api/v1/runs",
        fixture.submission(false),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let message = body["error"].as_str().unwrap_or_default();
    assert!(
        message.contains("shared.txt"),
        "the conflicted path must be listed: {message}"
    );
    assert!(
        message.contains("--no-merge"),
        "the escape hatch must be offered: {message}"
    );
    assert!(
        state.test_tx().await.runs.is_empty(),
        "a conflicted pull request must create no run"
    );
}

/// An unreachable origin fails the submission loudly; `--no-merge` is the
/// explicit escape, and nothing silently tests a different tree.
#[tokio::test]
async fn unreachable_upstream_fails_clearly() {
    let fixture = MergeFixture::clean();
    git(
        &fixture.workspace,
        &[
            "remote",
            "set-url",
            "origin",
            "/nonexistent/preloop-origin.git",
        ],
    );
    let (state, app) = fixture.app().await;
    let (status, body) = request_json_status(
        &app,
        Method::POST,
        "/api/v1/runs",
        fixture.submission(false),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = body["error"].as_str().unwrap_or_default();
    assert!(
        message.contains("--no-merge"),
        "the failure must name the escape hatch: {message}"
    );
    assert!(
        state.test_tx().await.runs.is_empty(),
        "an unfetchable base must create no run"
    );
}

/// A local pull-request merge requires an origin; `--no-merge` is the named
/// escape hatch when the workspace is intentionally offline.
#[tokio::test]
async fn originless_pull_request_names_no_merge_escape() {
    let fixture = MergeFixture::clean();
    git(&fixture.workspace, &["remote", "remove", "origin"]);
    let (state, app) = fixture.app().await;
    let (status, body) = request_json_status(
        &app,
        Method::POST,
        "/api/v1/runs",
        fixture.submission(false),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = body["error"].as_str().unwrap_or_default();
    assert!(message.contains("no `origin` remote"), "error: {message}");
    assert!(message.contains("--no-merge"), "error: {message}");
    assert!(state.test_tx().await.runs.is_empty());
}

/// `--no-merge` keeps today's behaviour: the branch alone, no merge record.
#[tokio::test]
async fn no_merge_tests_the_branch_alone() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let accepted = request_json(&app, Method::POST, "/api/v1/runs", fixture.submission(true)).await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.test_tx().await;
    let record = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .expect("run record");
    assert_eq!(
        record.github["sha"],
        serde_json::json!(fixture.head),
        "--no-merge must test the branch tip"
    );
    assert!(record.workspace_snapshot.as_ref().unwrap().merge.is_none());
}

/// `push` events test the commit itself; they never grow a merge.
#[tokio::test]
async fn push_events_never_merge() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let mut submission = fixture.submission(false);
    submission["event"] = serde_json::json!("push");
    submission["workflow_yaml"] = serde_json::json!(
        "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo push\n"
    );
    let accepted = request_json(&app, Method::POST, "/api/v1/runs", submission).await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.test_tx().await;
    let record = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .expect("run record");
    assert_eq!(record.github["sha"], serde_json::json!(fixture.head));
    assert!(record.workspace_snapshot.as_ref().unwrap().merge.is_none());
}
/// The bundle the CLI uploads: the submitted commit and the base tip under the
/// submit namespace, and nothing else. `legacy_main` also bundles a stale local
/// `main`, as an old `--all` bundle would, to prove the engine never reads it.
fn submit_bundle(fixture: &MergeFixture, head: &str, legacy_main: bool) -> PathBuf {
    let workspace = &fixture.workspace;
    git(
        workspace,
        &["update-ref", "refs/preloop/submit/test/head", head],
    );
    git(
        workspace,
        &[
            "update-ref",
            "refs/preloop/submit/test/base/main",
            &fixture.base,
        ],
    );
    let mut refs = vec![
        "refs/preloop/submit/test/head",
        "refs/preloop/submit/test/base/main",
    ];
    if legacy_main {
        // What a checkout that never fetched still calls `main`: the point
        // the branch was cut from, not the tip origin has now.
        let branch_point = git(workspace, &["merge-base", &fixture.base, &fixture.head]);
        git(workspace, &["branch", "-f", "main", &branch_point]);
        refs.push("refs/heads/main");
    }
    let path = fixture.temp.path().join("hosted.bundle");
    let mut args = vec!["bundle", "create", path.to_str().unwrap()];
    args.extend(refs);
    git(workspace, &args);
    path
}

/// The dirty commit the CLI creates on top of the feature head.
fn dirty_commit(fixture: &MergeFixture) -> String {
    std::fs::write(fixture.workspace.join("hosted-dirty.txt"), "uploaded\n").unwrap();
    git(&fixture.workspace, &["add", "-A"]);
    let tree = git(&fixture.workspace, &["write-tree"]);
    git(
        &fixture.workspace,
        &[
            "commit-tree",
            &tree,
            "-p",
            &fixture.head,
            "-m",
            "hosted submit",
        ],
    )
}

async fn upload_bundle(app: &Router, bundle: &Path) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/bundles")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(std::fs::read(bundle).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// Submit a run, optionally with one extra request header.
async fn post_run(
    app: &Router,
    submission: Value,
    extra_header: Option<(&str, &str)>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/runs")
        .header(header::AUTHORIZATION, "Bearer preloop-system-token")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some((name, value)) = extra_header {
        request = request.header(name, value);
    }
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(serde_json::to_vec(&submission).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

/// An uploaded commit bundle supplies both the dirty head and the base branch
/// used to build a hosted pull-request test merge. The base is the explicit
/// base tip in the bundle, never a stale local `main` that rode along.
#[tokio::test]
async fn hosted_bundle_submission_merges_the_uploaded_dirty_commit() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let head = dirty_commit(&fixture);
    let bundle = submit_bundle(&fixture, &head, true);
    let (status, accepted) = upload_bundle(&app, &bundle).await;
    assert_eq!(status, StatusCode::OK);

    let mut submission = fixture.submission(false);
    submission["sha"] = serde_json::json!(head);
    submission["git_bundle_id"] = accepted["bundle_id"].clone();
    let accepted_run = request_json(&app, Method::POST, "/api/v1/runs", submission).await;
    let run_id = accepted_run["run_id"].as_str().unwrap();
    let inner = state.test_tx().await;
    let record = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .expect("run record");
    let snapshot = record.workspace_snapshot.as_ref().expect("bundle snapshot");
    let merge = snapshot.merge.as_ref().expect("hosted PR merge");
    assert_eq!(
        merge.base_sha, fixture.base,
        "the merge must use the bundled base tip, not the stale `main` in the bundle"
    );
    assert_eq!(merge.head_sha, head);
    assert_eq!(record.github["sha"], serde_json::json!(merge.sha));
    assert!(
        queued_message_for(&inner, run_id)
            .preloop_snapshot_origin_rewrite
            .is_some()
    );
    let served = fixture.state_dir.join("snapshots").join(run_id);
    assert_eq!(
        git(&served, &["log", "--format=%P", "-1", &merge.sha]),
        format!("{} {head}", fixture.base)
    );
    assert_eq!(
        git(
            &served,
            &["show", &format!("{}:hosted-dirty.txt", merge.sha)]
        ),
        "uploaded"
    );
}

/// A bundle is single-use, and a submission leaves nothing of it behind: not
/// the upload, not its bare repository, not its workspace.
#[tokio::test]
async fn hosted_bundle_is_single_use_and_reclaimed_after_the_submission() {
    let fixture = MergeFixture::clean();
    let (_state, app) = fixture.app().await;
    let head = dirty_commit(&fixture);
    let (status, accepted) = upload_bundle(&app, &submit_bundle(&fixture, &head, false)).await;
    assert_eq!(status, StatusCode::OK);
    let bundle_id = accepted["bundle_id"].as_str().unwrap().to_owned();

    let mut submission = fixture.submission(false);
    submission["sha"] = serde_json::json!(head);
    submission["git_bundle_id"] = serde_json::json!(bundle_id);
    let (status, _) = post_run(&app, submission.clone(), None).await;
    assert_eq!(status, StatusCode::OK);

    let store = fixture.state_dir.join("bundles");
    assert!(
        !store.join(format!("{bundle_id}.bundle")).exists(),
        "the upload is consumed by the claim"
    );
    // The scratch directory is removed on a spawned task.
    let scratch = store.join(format!("{bundle_id}.work"));
    for _ in 0..100 {
        if !scratch.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        !scratch.exists(),
        "the unpacked bundle must be reclaimed once the submission is done"
    );

    let (status, body) = post_run(&app, submission, None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("already used"),
        "a replayed bundle id is refused: {body}"
    );
}

/// The bundle is the workspace under test: a workspace header would repoint the
/// engine at another tree, so the combination is refused without spending the
/// bundle.
#[tokio::test]
async fn hosted_bundle_refuses_a_workspace_header_and_stays_usable() {
    use base64::Engine as _;
    let fixture = MergeFixture::clean();
    let (_state, app) = fixture.app().await;
    let head = dirty_commit(&fixture);
    let (_, accepted) = upload_bundle(&app, &submit_bundle(&fixture, &head, false)).await;

    let mut submission = fixture.submission(false);
    submission["sha"] = serde_json::json!(head);
    submission["git_bundle_id"] = accepted["bundle_id"].clone();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(fixture.workspace.to_string_lossy().as_bytes());
    let (status, body) = post_run(
        &app,
        submission.clone(),
        Some(("x-preloop-local-workspace", &encoded)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.to_string().contains("git_bundle_id"), "{body}");

    let (status, body) = post_run(&app, submission, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the refused attempt must not have consumed the bundle: {body}"
    );
}

/// GitHub cannot hold SHA-256 repositories, so only a full SHA-1 commit id is a
/// publishable submission; anything else is refused before the bundle is spent.
#[tokio::test]
async fn hosted_bundle_requires_a_full_sha1_commit_id() {
    let fixture = MergeFixture::clean();
    let (_state, app) = fixture.app().await;
    let head = dirty_commit(&fixture);
    let (_, accepted) = upload_bundle(&app, &submit_bundle(&fixture, &head, false)).await;

    for bad in ["a".repeat(64), head[..12].to_owned(), "z".repeat(40)] {
        let mut submission = fixture.submission(false);
        submission["sha"] = serde_json::json!(bad);
        submission["git_bundle_id"] = accepted["bundle_id"].clone();
        let (status, body) = post_run(&app, submission, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        assert!(body.to_string().contains("invalid commit SHA"), "{body}");
    }
}

/// The push publishes the head tree, even though CI checked out the merge tree.
#[tokio::test]
async fn push_after_local_pr_merge_records_head_tree_not_merge_tree() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    let mut submission = fixture.submission(false);
    submission["sha"] = serde_json::json!(fixture.head);
    submission["push"] = serde_json::json!({
        "create_pr": true,
        "draft_pr": true,
        "dirty": true
    });

    let accepted = request_json(&app, Method::POST, "/api/v1/runs", submission).await;
    let run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.test_tx().await;
    let record = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == run_id)
        .expect("push run record");
    let snapshot = record.workspace_snapshot.as_ref().unwrap();
    let merge = snapshot.merge.as_ref().unwrap();
    let head_tree_rev = format!("{}^{{tree}}", fixture.head);
    let merge_tree_rev = format!("{}^{{tree}}", merge.sha);
    let head_tree = git(&fixture.workspace, &["rev-parse", &head_tree_rev]);
    let served = fixture.state_dir.join("snapshots").join(run_id);
    let merge_tree = git(&served, &["rev-parse", &merge_tree_rev]);

    assert_ne!(head_tree, merge_tree, "fixture must distinguish H from M");
    assert_eq!(snapshot.tree_sha, head_tree);
    assert_eq!(
        record.submission.push_tree.as_deref(),
        Some(head_tree.as_str()),
        "push-back must verify the head tree, not the merge tree"
    );
}

/// The shared builder contract: fetch + merge + serve, deterministic, and the
/// `prebuilt_merge` record is validated before anything is served.
#[tokio::test]
async fn prebuilt_merge_is_built_served_and_validated() {
    let fixture = MergeFixture::clean();
    std::fs::create_dir_all(&fixture.state_dir).unwrap();
    let mut state = AppState::new(fixture.state_dir.clone()).await.unwrap();
    state.local_workspace = Some(fixture.workspace.clone());
    let shared = state.shared();

    // Build the merge in an engine mirror. The fixture's bare origin is a
    // local path, so no credential and no network are involved.
    let mut source = webhook_merge_source(&shared, "owner/repo").await.unwrap();
    let mirror = source.mirror.clone();
    source.fetch_url = Some(fixture.origin.to_string_lossy().to_string());
    let request = MergeRequest {
        repository: "owner/repo".to_owned(),
        pull_request_number: Some(7),
        base_branch: "main".to_owned(),
        head: MergeHead::Commit(fixture.head.clone()),
    };
    let outcome = build_merge(&source, &request, &mirror).await.unwrap();
    let merge = match outcome {
        MergeOutcome::Merged(merge) => merge,
        other => panic!("expected a clean merge: {other:?}"),
    };
    assert_eq!(merge.base_sha, fixture.base);
    assert_eq!(merge.head_sha, fixture.head);

    // Deterministic: rebuilding the same merge yields the same commit.
    let again = build_merge(&source, &request, &mirror).await.unwrap();
    match again {
        MergeOutcome::Merged(second) => assert_eq!(second.sha, merge.sha),
        other => panic!("expected a clean merge: {other:?}"),
    }

    // The record points at the state-relative mirror, and attaching it serves
    // the merge from the engine.
    let record = prebuilt_merge_record(&shared, &mirror, &merge, Some(7))
        .expect("a mirror under the state directory has a relative path");
    assert_eq!(record.repository, "owner/repo");
    assert_eq!(
        record.mirror_repository,
        mirror
            .strip_prefix(&fixture.state_dir)
            .unwrap()
            .to_string_lossy()
    );
    let run_id = RunId::new();
    let snapshot = attach_prebuilt_merge(&shared, run_id, &record)
        .await
        .unwrap();
    assert_eq!(snapshot.commit_sha, merge.sha);
    assert_eq!(snapshot.source, SnapshotSource::SelfBuiltMerge);
    assert_eq!(
        snapshot
            .merge
            .as_ref()
            .unwrap()
            .mirror_repository
            .as_deref(),
        Some(record.mirror_repository.as_str())
    );
    let served = fixture.state_dir.join("snapshots").join(run_id.to_string());
    assert_eq!(git(&served, &["cat-file", "-t", &merge.sha]), "commit");
    let clone = fixture.temp.path().join("prebuilt-clone");
    git(
        fixture.temp.path(),
        &[
            "clone",
            "-q",
            served.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(git(&clone, &["cat-file", "-t", &merge.sha]), "commit");
    let run_app = app(state.clone(), CancellationToken::new());
    let mut submission = fixture.submission(false);
    submission["prebuilt_merge"] = serde_json::to_value(&record).unwrap();
    submission["base_ref"] = serde_json::Value::Null;
    submission["filter_branch"] = serde_json::Value::Null;
    submission["payload"]["pull_request"]
        .as_object_mut()
        .unwrap()
        .remove("base");
    let accepted = request_json(&run_app, Method::POST, "/api/v1/runs", submission).await;
    let submitted_run_id = accepted["run_id"].as_str().unwrap();
    let inner = state.test_tx().await;
    let submitted = inner
        .runs
        .values()
        .find(|run| run.run_id.to_string() == submitted_run_id)
        .expect("prebuilt run record");
    assert_eq!(submitted.github["sha"], serde_json::json!(merge.sha));
    assert!(
        queued_message_for(&inner, submitted_run_id)
            .preloop_snapshot_origin_rewrite
            .is_some()
    );

    let mut mismatched_submission = fixture.submission(false);
    mismatched_submission["repository"] = serde_json::json!("owner/other");
    mismatched_submission["prebuilt_merge"] = serde_json::to_value(&record).unwrap();
    let (status, _) = request_json_status(
        &run_app,
        Method::POST,
        "/api/v1/runs",
        mismatched_submission,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Nothing in a submission is trusted: wrong parents and escaping paths are
    // refused before a served repository is created.
    let mut tampered = record.clone();
    tampered.head_sha = "0".repeat(40);
    let error = attach_prebuilt_merge(&shared, RunId::new(), &tampered)
        .await
        .unwrap_err();
    assert!(matches!(error, MergeError::InvalidPrebuilt(_)), "{error:?}");
    let mut cross_repository = record.clone();
    cross_repository.repository = "owner/other".to_owned();
    let error = attach_prebuilt_merge(&shared, RunId::new(), &cross_repository)
        .await
        .unwrap_err();
    assert!(matches!(error, MergeError::InvalidPrebuilt(_)), "{error:?}");
    let mut escaping = record.clone();
    escaping.mirror_repository = "../escape.git".to_owned();
    let error = attach_prebuilt_merge(&shared, RunId::new(), &escaping)
        .await
        .unwrap_err();
    assert!(matches!(error, MergeError::InvalidPrebuilt(_)), "{error:?}");
}

/// A malformed caller-supplied SHA is rejected before it reaches `git fetch`.
#[tokio::test]
async fn fetch_inputs_rejects_a_non_object_id_head() {
    let fixture = MergeFixture::clean();
    let source = MergeSource {
        mirror: fixture.state_dir.join("sha-validation.git"),
        fetch_url: Some(fixture.origin.to_string_lossy().into_owned()),
        token: None,
        forge_host: None,
    };
    let request = MergeRequest {
        repository: "owner/repo".to_owned(),
        pull_request_number: None,
        base_branch: "main".to_owned(),
        head: MergeHead::Commit("not-a-full-object-id".to_owned()),
    };

    let error = fetch_inputs(&source, &request).await.unwrap_err();
    assert!(
        matches!(
            error,
            MergeError::MissingCommit { ref sha, .. } if sha == "not-a-full-object-id"
        ),
        "malformed head must fail as a missing commit, got {error:?}"
    );
}
/// The webhook fallback's mirror is prepared on demand and reused.
#[tokio::test]
async fn webhook_merge_source_prepares_a_reusable_mirror() {
    let fixture = MergeFixture::clean();
    std::fs::create_dir_all(&fixture.state_dir).unwrap();
    let state = AppState::new(fixture.state_dir.clone()).await.unwrap();
    let shared = state.shared();
    let source = webhook_merge_source(&shared, "owner/repo").await.unwrap();
    assert!(source.mirror.starts_with(&fixture.state_dir));
    assert!(
        source.mirror.join("HEAD").is_file(),
        "mirror must be a git dir"
    );
    assert!(
        source
            .fetch_url
            .as_deref()
            .unwrap()
            .ends_with("/owner/repo.git")
    );
    let again = webhook_merge_source(&shared, "owner/repo").await.unwrap();
    assert_eq!(again.mirror, source.mirror, "the mirror is reused");
}
