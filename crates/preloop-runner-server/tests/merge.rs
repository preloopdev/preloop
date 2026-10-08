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
    build_merge, prebuilt_merge_record, webhook_merge_source,
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
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        fixture.submission(false),
    )
    .await;
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
/// An uploaded commit bundle supplies both the dirty head and the base branch
/// used to build a hosted pull-request test merge.
#[tokio::test]
async fn hosted_bundle_submission_merges_the_uploaded_dirty_commit() {
    let fixture = MergeFixture::clean();
    let (state, app) = fixture.app().await;
    std::fs::write(fixture.workspace.join("hosted-dirty.txt"), "uploaded\n").unwrap();
    git(&fixture.workspace, &["add", "-A"]);
    let tree = git(&fixture.workspace, &["write-tree"]);
    let head = git(
        &fixture.workspace,
        &[
            "commit-tree",
            &tree,
            "-p",
            &fixture.head,
            "-m",
            "hosted submit",
        ],
    );
    git(
        &fixture.workspace,
        &["update-ref", "refs/preloop/submit/test", &head],
    );
    let bundle_path = fixture.temp.path().join("hosted.bundle");
    git(
        &fixture.workspace,
        &["bundle", "create", bundle_path.to_str().unwrap(), "--all"],
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/bundles")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::from(std::fs::read(bundle_path).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let accepted: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();

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
    assert_eq!(merge.base_sha, fixture.base);
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


/// The shared builder contract: fetch + merge + serve, deterministic, and the
/// `prebuilt_merge` record is validated before anything is served.
#[tokio::test]
async fn prebuilt_merge_is_built_served_and_validated() {
    let fixture = MergeFixture::clean();
    std::fs::create_dir_all(&fixture.state_dir).unwrap();
    let state = AppState::new(fixture.state_dir.clone()).await.unwrap();
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
