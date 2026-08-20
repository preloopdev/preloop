//! Immutable local-workspace snapshots exposed as Git repositories.
//!
//! A local submission is captured as a synthetic root commit. The server then
//! exposes the resulting bare repository over Git smart HTTP so an unmodified
//! `actions/checkout` step can fetch the exact local tree through supported
//! checkout inputs. No runner-specific job-message extension is required.

use super::*;
use axum::body::{to_bytes, Body};
use axum::extract::{Path, Request};
use axum::http::{header, HeaderName, HeaderValue, Response, StatusCode};
use base64::Engine;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_util::io::ReaderStream;

const SNAPSHOT_REF: &str = "refs/heads/snapshot";
const MAX_GIT_REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// The checkout coordinates for one immutable workspace snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspaceSnapshot {
    pub(crate) commit_sha: String,
    /// Tree of the snapshot commit — the exact tree the run tests. A
    /// push-back client materializes a real commit from this tree so the
    /// pushed commit is byte-identical to what CI validated.
    /// Empty for snapshots persisted before this field existed, so a restart
    /// can still load old run records.
    #[serde(default)]
    pub(crate) tree_sha: String,
    /// The workspace's real HEAD commit (the commit the submission is based
    /// on), when the workspace has one. This is the identity a workflow sees
    /// as `github.sha`: it is what a custom checkout that fetches from the
    /// real remote can actually resolve. The synthetic [`Self::commit_sha`]
    /// exists only in this engine's snapshot store and would be rejected as
    /// `not our ref` by the upstream host.
    pub(crate) head_sha: Option<String>,
    pub(crate) repository: String,
    /// Current branch of the source workspace (`master`, `main`, …), when
    /// resolvable. Mirrored into the event payload as
    /// `repository.default_branch` so changed-file actions can pick a base.
    pub(crate) default_branch: Option<String>,
    /// Base commit the synthetic push measures against, mirrored into push
    /// event payloads as `before`. When the working tree carries uncommitted
    /// edits this is the workspace `HEAD` (so `before..after` is exactly the
    /// local delta); when the tree is clean it is `HEAD^` (so the range still
    /// covers the last commit the user wants tested). `None` on an unborn or
    /// initial-commit clean tree yields the null-SHA "initial push" base.
    pub(crate) before_sha: Option<String>,
    /// Server-side cost of capturing this snapshot; present on snapshots
    /// created after the timing instrumentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot_timing: Option<crate::models::SnapshotTiming>,
}

/// Capture `workspace` as an immutable cache-backed bare repository for `run_id`.
///
/// A private index and a temporary bare object database keep the user's index,
/// refs, and working tree untouched. Committed objects are incrementally fetched
/// into a state-directory cache; each run stores only its synthetic dirty-tree
/// objects and references that private cache as an alternate. No snapshot keeps
/// a path to the user's source repository.
pub(crate) async fn create_workspace_snapshot(
    state_dir: &FsPath,
    workspace: &FsPath,
    run_id: RunId,
    github_pat: Option<&str>,
) -> Result<WorkspaceSnapshot, ApiError> {
    let started = std::time::Instant::now();
    let workspace = std::fs::canonicalize(workspace).map_err(|error| {
        ApiError::bad_request(format!(
            "failed to resolve local workspace {}: {error}",
            workspace.display()
        ))
    })?;
    let snapshots_dir = state_dir.join("snapshots");
    tokio::fs::create_dir_all(&snapshots_dir)
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to create snapshot directory {}: {error}",
                snapshots_dir.display()
            ))
        })?;

    let repository = format!("snapshots/{run_id}");
    let final_repository = state_dir.join(&repository);
    if final_repository.exists() {
        return Err(ApiError::internal(format!(
            "snapshot repository already exists for run {run_id}"
        )));
    }

    // Keep the staging repository outside the source worktree. Otherwise a
    // state directory that is not ignored could recursively snapshot itself.
    let staging_root = std::env::temp_dir().join(format!(
        "preloop-workspace-snapshot-{run_id}-{}",
        uuid::Uuid::new_v4()
    ));
    let staging_repository = staging_root.join("repository.git");
    let staging_index = staging_root.join("index");
    tokio::fs::create_dir_all(&staging_root)
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to create snapshot staging directory {}: {error}",
                staging_root.display()
            ))
        })?;

    let result = create_workspace_snapshot_inner(
        state_dir,
        &workspace,
        &staging_repository,
        &staging_index,
        &final_repository,
        run_id,
        github_pat,
    )
    .await;
    if let Err(error) = tokio::fs::remove_dir_all(&staging_root).await {
        if staging_root.exists() {
            warn!(
                path = %staging_root.display(),
                %error,
                "Failed to remove snapshot staging directory"
            );
        }
    }

    let SnapshotResult {
        commit_sha,
        tree_sha,
        head_sha,
        default_branch,
        before_sha,
    } = result?;
    let timing = match snapshot_repo_timing(&final_repository) {
        Ok(mut stats) => {
            stats.duration_ms = started.elapsed().as_millis() as u64;
            Some(stats)
        }
        Err(error) => {
            warn!(
                %run_id,
                %error,
                "Failed to collect snapshot repository stats"
            );
            None
        }
    };
    info!(
        %run_id,
        %commit_sha,
        duration_ms = timing.map(|t| t.duration_ms).unwrap_or_default(),
        object_count = timing.map(|t| t.object_count).unwrap_or_default(),
        pack_bytes = timing.map(|t| t.pack_bytes).unwrap_or_default(),
        repository = %final_repository.display(),
        "Created immutable workspace snapshot"
    );
    Ok(WorkspaceSnapshot {
        commit_sha,
        tree_sha,
        head_sha,
        repository,
        default_branch,
        before_sha,
        snapshot_timing: timing,
    })
}

/// Object-count and stored-size statistics for a snapshot repository.
///
/// The snapshot's own objects live in a pack inside the repo directory, but
/// the bulk of a real tree is shared through the alternate object cache —
/// `git count-objects -v` never sees alternates. The number a checkout's
/// fetch would transfer is the reachable set, so count it with
/// `rev-list --objects --all` (includes alternates). Stored bytes are the
/// run-owned repository directory (`du`), the incremental storage the run
/// added to the state directory.
fn snapshot_repo_timing(repository: &FsPath) -> anyhow::Result<crate::models::SnapshotTiming> {
    use std::process::Command;
    let count = Command::new("git")
        .arg("--git-dir")
        .arg(repository)
        .args(["rev-list", "--objects", "--all"])
        .output()?;
    if !count.status.success() {
        anyhow::bail!(
            "git rev-list failed: {}",
            String::from_utf8_lossy(&count.stderr).trim()
        );
    }
    let object_count = count.stdout.iter().filter(|byte| **byte == b'\n').count() as u64;
    let du = Command::new("du").arg("-sk").arg(repository).output()?;
    let size_kib = if du.status.success() {
        String::from_utf8_lossy(&du.stdout)
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    } else {
        0
    };
    Ok(crate::models::SnapshotTiming {
        duration_ms: 0, // filled by the caller
        object_count,
        pack_bytes: size_kib.saturating_mul(1024),
    })
}

/// Count objects that `rev-list` explicitly reports as missing. With
/// `--missing=print`, missing objects are prefixed with `?`; ordinary commit
/// lines are bare object IDs, while tree/blob lines may include a path.
async fn missing_snapshot_objects(repository: &FsPath) -> Result<u64, ApiError> {
    let mut verify = Command::new("git");
    verify
        .env("GIT_DIR", repository)
        .args(["rev-list", "--objects", "--all", "--missing=print"]);
    let output = run_git(&mut verify, "verify snapshot object cache completeness").await?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| line.starts_with('?'))
        .count() as u64)
}

async fn create_workspace_snapshot_inner(
    state_dir: &FsPath,
    workspace: &FsPath,
    staging_repository: &FsPath,
    staging_index: &FsPath,
    final_repository: &FsPath,
    run_id: RunId,
    github_pat: Option<&str>,
) -> Result<SnapshotResult, ApiError> {
    // Creating the staging repository does not depend on anything we learn
    // from the workspace, so pay for both spawns at once. Every millisecond
    // here sits directly in `POST /api/v1/runs`.
    //
    // `--template=` skips copying the sample hooks and description into a
    // repository that only ever serves one fetch.
    let mut init_command = Command::new("git");
    init_command
        .args(["init", "--bare", "--quiet", "--template="])
        .arg(staging_repository);
    let (init, probe) = tokio::join!(
        run_git(&mut init_command, "initialize snapshot repository"),
        probe_workspace(workspace),
    );
    init?;
    let WorkspaceRevision {
        common_dir,
        source_head,
        default_branch,
        parent_sha,
        source_tree,
    } = probe?;
    let source_objects = common_dir.join("objects");
    if !source_objects.is_dir() {
        return Err(ApiError::bad_request(format!(
            "source Git object directory does not exist: {}",
            source_objects.display()
        )));
    }
    let cache = ensure_object_cache(
        state_dir,
        workspace,
        &common_dir,
        source_head.as_deref(),
        github_pat,
    )
    .await?;
    let ObjectCache {
        objects: cached_objects,
        index: cache_index,
        refreshed: cache_refreshed,
        ancestry_complete,
    } = cache;

    // A shallow workspace ends its history at the commits listed in its
    // `shallow` file; commits behind those roots were never fetched, so
    // their objects do not exist anywhere local. The staging repository must
    // serve a *complete* graph: a shallow-mirrored repo advertises refs
    // whose ancestry crosses the boundary, and a plain full fetch from it is
    // rejected by git ("shallow roots are not allowed to be updated"),
    // leaving the client with a broken object store and unusable
    // merge-base/diff walks. Instead, graft every shallow root to a
    // parentless copy (`git replace --graft`), which pack-objects honors
    // when serving. Clients then receive the boundary commit with no parents
    // and never see a shallow edge; `HEAD^` and the snapshot parent chain
    // stay resolvable for changed-file diffing.
    // A shallow workspace ends its history at the commits listed in its
    // `shallow` file; commits behind those roots were never fetched, so
    // their objects do not exist anywhere local. The staging repository must
    // serve a *complete* graph: advertising refs whose ancestry crosses the
    // boundary makes git reject the client's full fetch ("shallow roots are
    // not allowed to be updated") and leaves the client's object store
    // broken for merge-base/diff walks. `git replace` does not help — the
    // server-side pack generation deliberately ignores replace refs. So
    // rewrite the reachable history: every shallow root becomes a
    // parentless copy and every descendant is re-created with rewritten
    // parent links, producing a self-contained, fsck-clean repository.
    // Clients receive the boundary commits with no parents and never see a
    // shallow edge; `HEAD^` and the snapshot parent chain stay resolvable
    // for changed-file diffing. Returns the original→rewritten mapping for
    // the shas the submission exposes (snapshot parent, before/after base).
    let mut history_rewrite: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    // Only when the real ancestry could not be recovered: rewriting is what
    // makes the served SHAs diverge from the forge's, so it is the fallback,
    // not the default.
    if source_head.is_some() && !ancestry_complete {
        let source_shallow = common_dir.join("shallow");
        if source_shallow.is_file() {
            let shallow_roots = std::fs::read_to_string(&source_shallow).map_err(|error| {
                ApiError::internal(format!("failed to read workspace shallow file: {error}"))
            })?;
            let shallow_roots = shallow_roots
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect::<Vec<_>>();
            if !shallow_roots.is_empty() {
                // The shallow file stops `rev-list` at the boundary; the
                // walk fails on the missing objects without it.
                let staging_shallow = staging_repository.join("shallow");
                tokio::fs::copy(&source_shallow, &staging_shallow)
                    .await
                    .map_err(|error| {
                        ApiError::internal(format!(
                            "failed to stage workspace shallow file: {error}"
                        ))
                    })?;
                let rev_list = run_snapshot_git(
                    workspace,
                    staging_repository,
                    staging_index,
                    &cached_objects,
                    [
                        "rev-list",
                        "--reverse",
                        source_head.as_deref().expect("checked above"),
                    ],
                    "list shallow history",
                )
                .await?;
                for sha in output_text(&rev_list, "list shallow history")?.lines() {
                    let sha = sha.trim();
                    if sha.is_empty() {
                        continue;
                    }
                    let raw = {
                        let mut cat = snapshot_git_command(
                            workspace,
                            staging_repository,
                            staging_index,
                            &cached_objects,
                        );
                        cat.args(["cat-file", "commit", sha]);
                        let output = cat.output().await.map_err(|error| {
                            ApiError::internal(format!("read commit {sha}: {error}"))
                        })?;
                        if !output.status.success() {
                            return Err(ApiError::internal(format!(
                                "failed to read commit {sha}: {}",
                                String::from_utf8_lossy(&output.stderr).trim()
                            )));
                        }
                        String::from_utf8_lossy(&output.stdout).into_owned()
                    };
                    let (header, body) = raw.split_once("\n\n").unwrap_or((raw.as_str(), ""));
                    let mut parents = Vec::new();
                    let mut author: Option<(String, String, String)> = None;
                    let mut committer: Option<(String, String, String)> = None;
                    let mut tree: Option<String> = None;
                    let mut header_lines = header.lines().peekable();
                    while let Some(line) = header_lines.next() {
                        if let Some(value) = line.strip_prefix("tree ") {
                            tree = Some(value.trim().to_owned());
                        } else if let Some(value) = line.strip_prefix("parent ") {
                            let parent = value.trim();
                            if let Some(rewritten) = history_rewrite.get(parent) {
                                parents.push(rewritten.clone());
                            }
                            // An unmapped parent is behind the shallow
                            // boundary: drop it (this commit is a shallow
                            // root and becomes parentless).
                        } else if let Some(value) = line.strip_prefix("author ") {
                            author = Some(parse_ident(value));
                        } else if let Some(value) = line.strip_prefix("committer ") {
                            committer = Some(parse_ident(value));
                        } else if line.starts_with("gpgsig") {
                            // Skip the signature and its indented
                            // continuation lines.
                            while header_lines
                                .peek()
                                .is_some_and(|next| next.starts_with(' '))
                            {
                                header_lines.next();
                            }
                        }
                        // Other headers (e.g. `encoding`) are kept implicitly
                        // by regenerating the commit from the tree, parents,
                        // and ident below; they are not re-emitted.
                    }
                    let tree = tree
                        .ok_or_else(|| ApiError::internal(format!("commit {sha} has no tree")))?;
                    let mut commit_tree = snapshot_git_command(
                        workspace,
                        staging_repository,
                        staging_index,
                        &cached_objects,
                    );
                    if let Some((name, email, date)) = author {
                        commit_tree
                            .env("GIT_AUTHOR_NAME", name)
                            .env("GIT_AUTHOR_EMAIL", email)
                            .env("GIT_AUTHOR_DATE", date);
                    }
                    if let Some((name, email, date)) = committer {
                        commit_tree
                            .env("GIT_COMMITTER_NAME", name)
                            .env("GIT_COMMITTER_EMAIL", email)
                            .env("GIT_COMMITTER_DATE", date);
                    }
                    let mut args = vec!["commit-tree".to_owned(), tree];
                    for parent in &parents {
                        args.push("-p".to_owned());
                        args.push(parent.clone());
                    }
                    commit_tree.args(args);
                    let mut child = commit_tree
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::piped())
                        .spawn()
                        .map_err(|error| {
                            ApiError::internal(format!("spawn commit-tree: {error}"))
                        })?;
                    AsyncWriteExt::write_all(
                        child.stdin.as_mut().expect("stdin is piped"),
                        body.as_bytes(),
                    )
                    .await
                    .map_err(|error| ApiError::internal(format!("write commit body: {error}")))?;
                    let output = child
                        .wait_with_output()
                        .await
                        .map_err(|error| ApiError::internal(format!("run commit-tree: {error}")))?;
                    if !output.status.success() {
                        return Err(ApiError::internal(format!(
                            "failed to rewrite commit {sha}: {}",
                            String::from_utf8_lossy(&output.stderr).trim()
                        )));
                    }
                    let rewritten = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                    history_rewrite.insert(sha.to_owned(), rewritten);
                }
                let _ = tokio::fs::remove_file(&staging_shallow).await;
            }
        }
    }

    // `git add --all` has to decide, for every path, whether the working tree
    // still matches the index. With a cold index it re-hashes the whole tree;
    // with the previous run's stat data it only re-hashes what changed. On a
    // 6000-file workspace that is 156 ms versus 16 ms.
    //
    // The reuse is safe because the index is reset to HEAD immediately after:
    // `--reset` takes every entry's object id from the tree (so every blob it
    // names is reachable from HEAD and lives in the object cache) and keeps
    // stat data only where the path is unchanged. The persisted index
    // contributes cached stat information and nothing else. Plain `read-tree`
    // would drop that stat data and put us back on the slow path.
    if let Some(head) = source_head.as_deref() {
        if cache_index.is_file() {
            let _ = tokio::fs::copy(&cache_index, staging_index).await;
        }
        run_snapshot_git(
            workspace,
            staging_repository,
            staging_index,
            &cached_objects,
            ["read-tree", "--reset", head],
            "seed snapshot index",
        )
        .await?;
    }

    let mut add = snapshot_git_command(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
    );
    add.args(["add", "--all"]);
    // A positive pathspec that matches a gitignored path makes `git add` fail
    // the whole invocation ("the following paths are ignored by one of your
    // .gitignore files"), and an exclude pathspec does not suppress it. The
    // normal setup gitignores the state directory (`/.preloop/`), so the old
    // `-- :/ :(exclude,top){state}/**` form failed on every repository that
    // followed the documented convention, and each snapshot silently degraded
    // to a plain checkout.
    //
    // When git already ignores the state directory a bare `--all` excludes it
    // for free. Only a *tracked or otherwise visible* state directory needs an
    // explicit exclusion, and naming one cannot trip the ignore error.
    match state_dir_exclusion(state_dir, workspace)? {
        Some(excluded_state) if !path_is_ignored(workspace, &excluded_state).await => {
            add.arg("--");
            add.arg(":/");
            add.arg(format!(":(exclude,top){excluded_state}"));
            add.arg(format!(":(exclude,top){excluded_state}/**"));
        }
        _ => {}
    }
    run_git(&mut add, "stage local workspace state").await?;

    // A local workspace can carry gitlink entries for submodules that were
    // never registered in `.gitmodules` — a nested clone added by hand, or a
    // half-removed submodule. Served faithfully, the gitlink makes
    // `git submodule` operations in the VM (which actions/checkout runs when
    // a workflow asks for submodules) die with `fatal: No url found for
    // submodule path '…' in .gitmodules` even though the repository itself
    // is intact. GitHub-hosted workspaces cannot have this state; local ones
    // routinely do. Drop gitlinks no `.gitmodules` url resolves so the
    // checkout behaves as if the path were an ordinary directory.
    let staged = run_snapshot_git(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
        ["ls-files", "--stage", "-z"],
        "list staged paths",
    )
    .await?;
    let gitlinks = gitlink_paths(&staged.stdout);
    if !gitlinks.is_empty() {
        let configured = configured_submodule_urls(workspace).await;
        let unresolvable: Vec<&str> = gitlinks
            .iter()
            .filter(|path| !configured.contains(*path))
            .map(String::as_str)
            .collect();
        if !unresolvable.is_empty() {
            let mut remove = snapshot_git_command(
                workspace,
                staging_repository,
                staging_index,
                &cached_objects,
            );
            remove.args(["update-index", "--force-remove", "--"]);
            remove.args(&unresolvable);
            run_git(&mut remove, "drop unresolvable submodule gitlinks").await?;
        }
    }

    let tree_output = run_snapshot_git(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
        ["write-tree"],
        "write snapshot tree",
    )
    .await?;
    let tree = output_text(&tree_output, "write snapshot tree")?;

    // Choose the base the synthetic push measures against. The snapshot commit
    // `S` sits on top of the workspace `HEAD`, so `before..after` should span
    // exactly the change under test:
    //   * dirty tree (snapshot tree ≠ HEAD tree) → base is `HEAD`, so the range
    //     is only the uncommitted local edits;
    //   * clean tree (trees equal) → base is `HEAD^`, so the range still covers
    //     the last commit the user just made and wants tested (an equal-tree
    //     `HEAD..S` would be empty and changed-file actions would see nothing).
    // `None` (unborn HEAD, or an initial commit with a clean tree) falls
    // through to the null-SHA "initial push" base downstream.
    let rewrite = |sha: &Option<String>| {
        sha.as_deref()
            .and_then(|value| history_rewrite.get(value).cloned())
            .or_else(|| sha.clone())
    };
    let before_sha = if Some(tree.as_str()) != source_tree.as_deref() {
        rewrite(&source_head)
    } else {
        rewrite(&parent_sha)
    };

    // Best effort: the index is a cache, so a failed hand-off only costs the
    // next submission its stat data.
    persist_snapshot_index(staging_index, &cache_index).await;

    let mut commit = snapshot_git_command(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
    );
    commit
        .env("GIT_AUTHOR_NAME", "preloop")
        .env("GIT_AUTHOR_EMAIL", "snapshot.local")
        .env("GIT_AUTHOR_DATE", "1970-01-01T00:00:00Z")
        .env("GIT_COMMITTER_NAME", "preloop")
        .env("GIT_COMMITTER_EMAIL", "snapshot.local")
        .env("GIT_COMMITTER_DATE", "1970-01-01T00:00:00Z");
    // Link the snapshot commit to the workspace HEAD so the snapshot clone
    // carries the workspace's real history. Changed-file actions
    // (`dorny/paths-filter`, `tj-actions/changed-files`) diff against a base
    // ref; an orphan root commit leaves them nothing to diff and they fail.
    // The parent object is always present: the object cache holds every
    // committed object from the workspace (see `ensure_object_cache`).
    if let Some(head) = rewrite(&source_head) {
        commit.args([
            "commit-tree",
            tree.as_str(),
            "-p",
            head.as_str(),
            "-m",
            "preloop workspace snapshot",
        ]);
    } else {
        commit.args([
            "commit-tree",
            tree.as_str(),
            "-m",
            "preloop workspace snapshot",
        ]);
    }
    let commit_output = run_git(&mut commit, "create snapshot commit").await?;
    let commit_sha = output_text(&commit_output, "create snapshot commit")?;
    if commit_sha.len() != 40 || !commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::internal(format!(
            "git returned invalid snapshot commit id `{commit_sha}`"
        )));
    }

    let mut update_refs = snapshot_git_command(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
    );
    update_refs.args(["update-ref", "--stdin"]);
    let update_input = format!("update {SNAPSHOT_REF} {commit_sha}\n");
    run_git_with_stdin(
        &mut update_refs,
        update_input.as_bytes(),
        "publish snapshot ref",
    )
    .await?;
    let mut update_head = snapshot_git_command(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
    );
    update_head.args(["symbolic-ref", "HEAD", SNAPSHOT_REF]);
    run_git(&mut update_head, "publish snapshot HEAD").await?;

    let alternate_file = staging_repository.join("objects/info/alternates");
    tokio::fs::create_dir_all(alternate_file.parent().expect("alternates parent"))
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to create snapshot alternates directory: {error}"
            ))
        })?;
    tokio::fs::write(&alternate_file, format!("{}\n", cached_objects.display()))
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to publish snapshot object alternate: {error}"
            ))
        })?;

    // Prove that the synthetic commit is fully connected through the persisted
    // cache, without decompressing and re-hashing every historical blob. Git
    // clone/fetch validate incoming objects; connectivity-only catches an
    // alternate that is missing an object the new tree needs.
    //
    // Only a clone or fetch can change what the alternate holds, and the
    // objects this run wrote live in the staging repository itself, so a run
    // that reused the cache untouched is already covered by the check that ran
    // when those objects landed. Re-verifying every submission cost ~30 % of
    // snapshot time for a result that cannot have changed.
    if cache_refreshed {
        let mut fsck = Command::new("git");
        fsck.env("GIT_DIR", staging_repository).args([
            "fsck",
            "--connectivity-only",
            "--no-dangling",
        ]);
        run_git(&mut fsck, "verify incremental snapshot repository").await?;
    }

    // Advertise the workspace's own branches and tags so changed-file actions
    // (`dorny/paths-filter`, `tj-actions/changed-files`) and `actions/checkout`
    // with `fetch-depth: 0` can fetch a base ref (`origin/main`, tags, …).
    // Published after fsck on purpose: fsck walks every ref, and these refs
    // point into the cache alternate whose connectivity was already validated
    // when the cache was cloned or fetched.
    //
    // The default branch ref points at the snapshot commit, not the workspace
    // tip: the runner checks out the snapshot, so a change-file action that
    // resolves its `head` via `origin/<branch>` must land on the same commit
    // the runner has checked out — otherwise the diff (base..head) silently
    // excludes the local changes the submission is meant to represent.
    let workspace_refs = Command::new("git")
        .env("GIT_DIR", &common_dir)
        .args([
            "for-each-ref",
            "--format=%(refname) %(objectname)",
            "refs/heads",
            "refs/tags",
        ])
        .output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to list workspace refs: {error}")))?;
    if workspace_refs.status.success() {
        let refs = String::from_utf8_lossy(&workspace_refs.stdout);
        // `update-ref --stdin` is atomic: one ref whose target is missing
        // aborts the whole transaction and fails the snapshot. Workspaces
        // collect such refs routinely — a tag fetched without its object, a
        // filtered clone, a `--unshallow` that dropped an old tag's target.
        // Ask once which objects actually exist and publish only those.
        // Resolve every ref to the sha we would publish first, so the
        // existence check below covers rewritten objects too.
        let mut planned: Vec<(String, String)> = Vec::new();
        for line in refs.lines() {
            let Some((name, object)) = line.split_once(' ') else {
                continue;
            };
            if object.len() != 40 {
                continue;
            }
            let is_default_branch =
                default_branch.as_deref() == Some(name.strip_prefix("refs/heads/").unwrap_or(name));
            let published = if is_default_branch {
                commit_sha.clone()
            } else {
                history_rewrite
                    .get(object)
                    .cloned()
                    .unwrap_or_else(|| object.to_owned())
            };
            planned.push((name.to_owned(), published));
        }
        // `update-ref --stdin` is atomic: one ref whose target object is
        // missing aborts the whole transaction and fails the snapshot.
        // Workspaces collect such refs routinely — a tag fetched without its
        // target, a filtered clone, an `--unshallow` that left an old tag
        // dangling. Ask once which objects exist and skip the rest.
        let candidates: std::collections::BTreeSet<String> =
            planned.iter().map(|(_, sha)| sha.clone()).collect();
        let present = present_objects(
            workspace,
            staging_repository,
            staging_index,
            &cached_objects,
            &candidates,
        )
        .await?;
        let mut publish = snapshot_git_command(
            workspace,
            staging_repository,
            staging_index,
            &cached_objects,
        );
        publish.args(["update-ref", "--stdin"]);
        let mut publish_input = String::new();
        let mut skipped = 0usize;
        for (name, published) in planned {
            if !present.contains(&published) {
                skipped += 1;
                continue;
            }
            publish_input.push_str(&format!("update {name} {published}\n"));
        }
        if skipped > 0 {
            warn!(
                skipped,
                "skipped workspace refs whose target objects are absent"
            );
        }
        if !publish_input.is_empty() {
            run_git_with_stdin(
                &mut publish,
                publish_input.as_bytes(),
                "publish workspace refs",
            )
            .await?;
        }
    }

    // Allow clients to fetch arbitrary commits that exist in the snapshot's
    // object store, not just advertised ref tips: workflows that deep-fetch a
    // concrete SHA (`git fetch origin <sha>`, `fetch-depth: 0` checkouts of a
    // base ref) hit exactly this path, and the official host serves it via
    // `uploadpack.allowReachableSHA1InWant`. Without it upload-pack answers
    // "not our ref" for any want that is not a tip.
    let mut uploadpack_reachable = Command::new("git");
    uploadpack_reachable
        .env("GIT_DIR", staging_repository)
        .args(["config", "uploadpack.allowReachableSHA1InWant", "true"]);
    run_git(
        &mut uploadpack_reachable,
        "allow reachable sha wants in snapshot upload-pack",
    )
    .await?;
    let mut uploadpack_tip = Command::new("git");
    uploadpack_tip.env("GIT_DIR", staging_repository).args([
        "config",
        "uploadpack.allowTipSHA1InWant",
        "true",
    ]);
    run_git(
        &mut uploadpack_tip,
        "allow tip sha wants in snapshot upload-pack",
    )
    .await?;

    tokio::fs::rename(staging_repository, final_repository)
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to publish snapshot repository for run {run_id}: {error}"
            ))
        })?;
    Ok(SnapshotResult {
        commit_sha,
        // The snapshot commit's tree — the exact staged dirty tree CI tests,
        // not the workspace HEAD's tree. Push-back clients materialize their
        // commit from this so pushed == tested.
        tree_sha: tree.clone(),
        head_sha: source_head,
        default_branch,
        before_sha,
    })
}

/// The immutable snapshot commit plus the workspace facts needed to present
/// the submission as a coherent GitHub event to changed-file actions.
struct SnapshotResult {
    commit_sha: String,
    tree_sha: String,
    head_sha: Option<String>,
    default_branch: Option<String>,
    before_sha: Option<String>,
}

/// What one `git rev-parse` tells us about the source workspace.
struct WorkspaceRevision {
    /// Canonical `.git` common directory backing the worktree.
    common_dir: PathBuf,
    /// Current `HEAD` commit, absent when the branch is unborn.
    source_head: Option<String>,
    /// Tree object of the current `HEAD`, absent when `HEAD` is unborn. Used
    /// to decide whether the working tree carries uncommitted edits.
    source_tree: Option<String>,
    /// Current branch name, absent when `HEAD` is detached or unborn.
    default_branch: Option<String>,
    /// Parent of the current `HEAD`, absent on the first commit or in a
    /// depth-1 shallow clone.
    parent_sha: Option<String>,
}

/// Validate the workspace and resolve its common directory and `HEAD` in a
/// single `git` invocation.
///
/// Process spawns dominate snapshot creation, so the three questions the
/// snapshot needs are asked together rather than one process each.
async fn probe_workspace(workspace: &FsPath) -> Result<WorkspaceRevision, ApiError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args([
            "rev-parse",
            "--is-inside-work-tree",
            "--git-common-dir",
            "HEAD",
            "HEAD^{tree}",
        ])
        .output()
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to inspect local Git workspace: {error}"))
        })?;
    // An unborn HEAD makes `rev-parse` exit non-zero after it has already
    // printed the answers that do resolve, so the lines are parsed either way.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines().map(str::trim);

    if lines.next() != Some("true") {
        return Err(ApiError::bad_request(format!(
            "local workspace is not a Git worktree: {}",
            workspace.display()
        )));
    }
    let common_dir = lines
        .next()
        .filter(|line| !line.is_empty())
        .ok_or_else(|| {
            ApiError::internal(format!(
                "git produced no Git common directory for {}: {}",
                workspace.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        })?;
    let common_dir = output_path_text(common_dir, workspace)?;
    let source_head = if output.status.success() {
        let head = lines
            .next()
            .filter(|line| !line.is_empty())
            .ok_or_else(|| {
                ApiError::internal("git produced no source HEAD for the local workspace")
            })?;
        Some(head.to_owned())
    } else {
        None
    };
    // Printed on the same `rev-parse` line after `HEAD`; only meaningful when
    // the combined call succeeded (an unborn HEAD fails and prints neither).
    let source_tree = if output.status.success() {
        lines
            .next()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
    } else {
        None
    };
    // Best-effort probes: a detached or unborn HEAD, or a depth-1 shallow
    // clone, legitimately lacks these and the snapshot works without them.
    let default_branch = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["symbolic-ref", "--short", "HEAD"])
        .output()
        .await
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|branch| !branch.is_empty() && branch != "HEAD");
    let parent_sha = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "HEAD^"])
        .output()
        .await
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .filter(|sha| sha.len() == 40);
    Ok(WorkspaceRevision {
        common_dir,
        source_head,
        source_tree,
        default_branch,
        parent_sha,
    })
}

fn snapshot_git_command(
    workspace: &FsPath,
    repository: &FsPath,
    index: &FsPath,
    source_objects: &FsPath,
) -> Command {
    let mut command = Command::new("git");
    command
        .current_dir(workspace)
        .env("GIT_DIR", repository)
        .env("GIT_WORK_TREE", workspace)
        .env("GIT_INDEX_FILE", index)
        .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", source_objects);
    command
}

/// Paths of gitlink (mode `160000`) entries in a `git ls-files --stage -z`
/// listing.
///
/// Records are NUL-terminated with a TAB between the `mode sha stage` header
/// and the path; `-z` leaves paths unquoted.
fn gitlink_paths(staged: &[u8]) -> Vec<String> {
    let mut paths = Vec::new();
    for record in staged.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
            continue;
        };
        if record[..tab].starts_with(b"160000 ") {
            if let Ok(path) = std::str::from_utf8(&record[tab + 1..]) {
                paths.push(path.to_owned());
            }
        }
    }
    paths
}

/// Paths `git submodule` can resolve in the workspace's `.gitmodules`: the
/// `path` value of every section that also carries a non-empty `url`.
///
/// Parsing is delegated to git itself (`git config -f .gitmodules -z
/// --get-regexp '^submodule\..*\.(path|url)$'`) so quoting, escapes, line
/// continuations, and case-insensitive section/key names are decoded exactly
/// as git's submodule machinery decodes them. Git resolves a gitlink by
/// path: it finds the section whose `path` matches and uses that section's
/// `url`. A gitlink whose path only equals a section *name* is not
/// resolvable — it dies with `fatal: No url found for submodule path '…' in
/// .gitmodules` (verified against git 2.x) — so section names are
/// deliberately excluded from the set. Empty set when the file is absent or
/// git cannot parse it, matching git's own treatment of such a file.
async fn configured_submodule_urls(workspace: &FsPath) -> BTreeSet<String> {
    let output = Command::new("git")
        .current_dir(workspace)
        .args([
            "config",
            "-f",
            ".gitmodules",
            "-z",
            "--get-regexp",
            "^submodule\\..*\\.(path|url)$",
        ])
        .output()
        .await;
    let Ok(output) = output else {
        return BTreeSet::new();
    };
    if !output.status.success() {
        return BTreeSet::new();
    }
    // `-z` output is NUL-terminated records of the form `key\nvalue`. Keys
    // are `submodule.<name>.path|url`; the name itself may contain dots and
    // spaces, so only the trailing `.path`/`.url` suffix is stripped.
    let mut path_by_name: std::collections::HashMap<&[u8], &[u8]> =
        std::collections::HashMap::new();
    let mut url_names: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    for record in output.stdout.split(|byte| *byte == 0) {
        let Some(nl) = record.iter().position(|byte| *byte == b'\n') else {
            continue;
        };
        let key = &record[..nl];
        let value = &record[nl + 1..];
        let is_url = key.ends_with(b".url");
        let suffix_len = if is_url {
            b".url".len()
        } else {
            b".path".len()
        };
        if !is_url && !key.ends_with(b".path") {
            continue;
        }
        let Some(name) = key.strip_prefix(b"submodule.") else {
            continue;
        };
        let name = &name[..name.len() - suffix_len];
        if value.is_empty() {
            continue;
        }
        if is_url {
            url_names.insert(name);
        } else {
            path_by_name.insert(name, value);
        }
    }
    let mut configured = BTreeSet::new();
    for (name, path) in path_by_name {
        if url_names.contains(name) {
            if let Ok(path) = std::str::from_utf8(path) {
                configured.insert(path.to_owned());
            }
        }
    }
    configured
}

/// Whether the workspace's own `.gitignore` rules already exclude `relative`.
///
/// Uses the workspace repository rather than the staging one so the answer
/// reflects the rules the user actually wrote. `check-ignore` exits 0 when the
/// path is ignored, 1 when it is not, and >1 on error; anything other than a
/// clean "ignored" answer is treated as not ignored, which keeps the explicit
/// exclusion in place and is the safe direction.
async fn path_is_ignored(workspace: &FsPath, relative: &str) -> bool {
    Command::new("git")
        .current_dir(workspace)
        .args(["check-ignore", "--quiet", "--", relative])
        .status()
        .await
        .map(|status| status.code() == Some(0))
        .unwrap_or(false)
}

async fn run_snapshot_git<const N: usize>(
    workspace: &FsPath,
    repository: &FsPath,
    index: &FsPath,
    source_objects: &FsPath,
    args: [&str; N],
    operation: &str,
) -> Result<std::process::Output, ApiError> {
    let mut command = snapshot_git_command(workspace, repository, index, source_objects);
    command.args(args);
    run_git(&mut command, operation).await
}

async fn run_git(command: &mut Command, operation: &str) -> Result<std::process::Output, ApiError> {
    let output = command
        .output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to {operation}: {error}")))?;
    if !output.status.success() {
        return Err(ApiError::internal(format!(
            "failed to {operation}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

fn output_text(output: &std::process::Output, operation: &str) -> Result<String, ApiError> {
    let value = std::str::from_utf8(&output.stdout)
        .map_err(|error| {
            ApiError::internal(format!(
                "invalid UTF-8 while trying to {operation}: {error}"
            ))
        })?
        .trim();
    if value.is_empty() {
        return Err(ApiError::internal(format!(
            "git produced no output while trying to {operation}"
        )));
    }
    Ok(value.to_owned())
}

fn output_path_text(value: &str, workspace: &FsPath) -> Result<PathBuf, ApiError> {
    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    };
    std::fs::canonicalize(&path).map_err(|error| {
        ApiError::internal(format!(
            "failed to resolve Git common directory {}: {error}",
            path.display()
        ))
    })
}

async fn run_git_with_stdin(
    command: &mut Command,
    input: &[u8],
    operation: &str,
) -> Result<std::process::Output, ApiError> {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| ApiError::internal(format!("failed to {operation}: {error}")))?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        ApiError::internal(format!("failed to {operation}: Git stdin was not piped"))
    })?;
    stdin
        .write_all(input)
        .await
        .map_err(|error| ApiError::internal(format!("failed to {operation}: {error}")))?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to {operation}: {error}")))?;
    if !output.status.success() {
        return Err(ApiError::internal(format!(
            "failed to {operation}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

/// Result of pointing a snapshot at the persistent object cache.
struct ObjectCache {
    /// Object directory to expose as the snapshot's alternate.
    objects: PathBuf,
    /// Persisted index carrying this workspace's cached stat data.
    index: PathBuf,
    /// Whether this call added objects to the cache.
    refreshed: bool,
    /// Whether the cache holds the full ancestry behind the workspace's
    /// shallow boundary. False means the snapshot must rewrite history to
    /// serve a complete graph, at the cost of changing every sha.
    ancestry_complete: bool,
}

async fn ensure_object_cache(
    state_dir: &FsPath,
    workspace: &FsPath,
    common_dir: &FsPath,
    source_head: Option<&str>,
    github_pat: Option<&str>,
) -> Result<ObjectCache, ApiError> {
    use sha2::Digest;

    let identity = common_dir.to_string_lossy();
    let key = format!("{:x}", sha2::Sha256::digest(identity.as_bytes()));
    let root = state_dir.join("snapshot-object-cache");
    let repository = root.join(format!("{key}.git"));
    let lock = root.join(format!("{key}.lock"));
    tokio::fs::create_dir_all(&root).await.map_err(|error| {
        ApiError::internal(format!("failed to create snapshot object cache: {error}"))
    })?;
    let _guard = acquire_cache_lock(&lock).await?;
    let mut last_head = repository.as_os_str().to_os_string();
    last_head.push(".last-head");
    let last_head = PathBuf::from(last_head);

    let mut cloned = false;
    if !repository.is_dir() {
        let staging = root.join(format!("{key}.{}.tmp", uuid::Uuid::new_v4()));
        let mut clone = Command::new("git");
        clone
            .args(["clone", "--bare", "--local", "--quiet"])
            .arg(workspace)
            .arg(&staging);
        if let Err(error) = run_git(&mut clone, "initialize snapshot object cache").await {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            return Err(error);
        }
        let mut disable_gc = Command::new("git");
        disable_gc
            .env("GIT_DIR", &staging)
            .args(["config", "gc.auto", "0"]);
        run_git(&mut disable_gc, "disable snapshot cache auto-gc").await?;
        match tokio::fs::rename(&staging, &repository).await {
            Ok(()) => cloned = true,
            Err(error) if repository.is_dir() => {
                let _ = tokio::fs::remove_dir_all(&staging).await;
                let _ = error;
            }
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&staging).await;
                return Err(ApiError::internal(format!(
                    "failed to publish snapshot object cache: {error}"
                )));
            }
        }
    }

    let mut refreshed = cloned;
    if cloned {
        record_cache_head(&last_head, source_head).await?;
    } else {
        // Fetch only adds immutable objects and atomically updates refs. Auto
        // GC is disabled, so active run alternates cannot lose base objects.
        let cached_head = match tokio::fs::read_to_string(&last_head).await {
            Ok(value) => Some(value.trim().to_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "failed to read snapshot object cache HEAD: {error}"
                )));
            }
        };
        // An unborn HEAD gives nothing to compare, so always refresh.
        if source_head.is_none() || cached_head.as_deref() != source_head {
            let mut fetch = Command::new("git");
            fetch
                .env("GIT_DIR", &repository)
                .args(["fetch", "--quiet", "--force", "--prune"])
                .arg(workspace)
                .args(["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]);
            run_git(&mut fetch, "refresh snapshot object cache").await?;
            record_cache_head(&last_head, source_head).await?;
            refreshed = true;
        }
    }

    let objects = std::fs::canonicalize(repository.join("objects")).map_err(|error| {
        ApiError::internal(format!("failed to resolve snapshot object cache: {error}"))
    })?;
    // The cache is what the snapshot actually serves, so its boundary is the
    // one that matters — a workspace that has since been deepened does not
    // help if the cache was built while it was shallow. Deepen from the
    // remote while the lock is held; once complete it stays complete and
    // later runs skip the fetch entirely.
    let mut ancestry_complete = !repository.join("shallow").is_file();
    if ancestry_complete {
        // A cache cloned from a `--filter=blob:none` workspace inherits the
        // promisor pack contents (commits and trees but no blobs) without
        // carrying the partial-clone markers, so the shallow-file check
        // above cannot see the hole. `rev-list --missing=print` enumerates
        // every object reachable from the refs and prints the absent ones
        // as bare SHAs (present objects print "sha path"). Any hole means
        // the cache advertises refs it cannot serve — every workflow
        // `git fetch --unshallow origin` against the snapshot then dies
        // with `Could not read <sha>` / "revision walk setup failed" — so
        // recover the full ancestry from the workspace's remote, the same
        // deepen the shallow path uses.
        let missing = missing_snapshot_objects(&repository).await?;
        if missing > 0 {
            warn!(
                cache = %repository.display(),
                %missing,
                "snapshot object cache is missing objects (partial-clone workspace?); deepening from the remote"
            );
            ancestry_complete = false;
        }
    }
    if !ancestry_complete {
        ancestry_complete =
            deepen_object_cache_from_remote(&repository, workspace, github_pat).await;
        if ancestry_complete {
            ancestry_complete = missing_snapshot_objects(&repository).await? == 0;
        }
        refreshed = refreshed || ancestry_complete;
    }
    let mut index = repository.as_os_str().to_owned();
    index.push(".index");
    Ok(ObjectCache {
        objects,
        index: PathBuf::from(index),
        refreshed,
        ancestry_complete,
    })
}

/// Which of `shas` exist in the snapshot's object view.
///
/// One `cat-file --batch-check` instead of a spawn per ref: a workspace can
/// carry thousands of tags, and this runs inside `POST /api/v1/runs`.
async fn present_objects(
    workspace: &FsPath,
    staging_repository: &FsPath,
    staging_index: &FsPath,
    cached_objects: &FsPath,
    shas: &std::collections::BTreeSet<String>,
) -> Result<std::collections::BTreeSet<String>, ApiError> {
    if shas.is_empty() {
        return Ok(std::collections::BTreeSet::new());
    }
    let mut query = String::with_capacity(shas.len() * 41);
    for sha in shas {
        query.push_str(sha);
        query.push('\n');
    }
    let mut command =
        snapshot_git_command(workspace, staging_repository, staging_index, cached_objects);
    command.args(["cat-file", "--batch-check=%(objectname) %(objecttype)"]);
    let output =
        run_git_with_stdin(&mut command, query.as_bytes(), "check snapshot objects").await?;
    let text = output_text(&output, "check snapshot objects")?;
    let mut present = std::collections::BTreeSet::new();
    for line in text.lines() {
        // Present: "<sha> commit". Missing: "<sha> missing".
        let mut parts = line.split_whitespace();
        let (Some(sha), Some(kind)) = (parts.next(), parts.next()) else {
            continue;
        };
        if kind != "missing" {
            present.insert(sha.to_owned());
        }
    }
    Ok(present)
}

/// Restore the ancestry a shallow workspace never downloaded.
///
/// A shallow clone stops at the commits listed in its `shallow` file, and the
/// object cache inherits that boundary. Serving a shallow graph is impossible,
/// so the snapshot otherwise rewrites history: shallow roots become
/// parentless and every descendant is re-created, which changes the tip's
/// sha. Workflows that resolve a base commit from git (`rev-list --parents`,
/// `HEAD^`, `merge-base`) and then fetch it from the forge then ask for a sha
/// that exists nowhere upstream and fail with "not our ref".
///
/// Fetching the real ancestry from the workspace's own remote removes the
/// boundary and keeps every sha identical to the forge's.
///
/// This fetches objects in full. `--filter=blob:none` would be enough for what
/// diff-base resolution needs — the commit and tree graph — and far cheaper,
/// but it leaves the cache a promisor whose missing blobs fail the snapshot's
/// `fsck` and cannot be hydrated through the alternate. Making that work needs
/// real partial-clone plumbing (`extensions.partialClone` on the staging repo
/// plus a promisor remote it can reach); until then, correctness first.
///
/// Best effort by design. No remote, no network, or a private repository
/// without credentials simply leaves the cache shallow and the caller falls
/// back to rewriting.
async fn deepen_object_cache_from_remote(
    repository: &FsPath,
    workspace: &FsPath,
    github_pat: Option<&str>,
) -> bool {
    let remote = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["config", "--get", "remote.origin.url"])
        .output()
        .await;
    let Ok(remote) = remote else {
        return false;
    };
    if !remote.status.success() {
        return false;
    }
    let url = String::from_utf8_lossy(&remote.stdout).trim().to_owned();
    if url.is_empty() {
        return false;
    }
    let shallow_marker = repository.join("shallow");
    let mut fetch = Command::new("git");
    fetch
        .env("GIT_DIR", repository)
        .args(["fetch", "--quiet", "--force", "--no-tags"]);
    if shallow_marker.is_file() {
        fetch.arg("--unshallow");
    } else {
        // A filtered clone can have no shallow marker while omitting blobs.
        // Refetch asks Git to transfer those promisor objects too.
        fetch.arg("--refetch");
    }
    if let Some((key, value)) = github_pat.and_then(|pat| github_auth_header_for_remote(&url, pat))
    {
        // A private remote rejects the anonymous unshallow; the engine's own
        // GitHub credential (already used for action downloads) closes it.
        // Scoped by `github_auth_header_for_remote` so the PAT is never sent
        // to a host the operator did not configure it for.
        fetch
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", key)
            .env("GIT_CONFIG_VALUE_0", value);
    }
    fetch
        .arg(&url)
        .arg("+refs/heads/*:refs/remotes/preloop-upstream/*");
    match run_git(&mut fetch, "deepen snapshot object cache").await {
        Ok(_) => !shallow_marker.is_file(),
        Err(error) => {
            // The remote URL can embed a credential in its userinfo, and
            // git's stderr echoes the URL it failed on verbatim. Scrub both
            // before anything reaches the log.
            let sanitized_url = sanitize_remote_url(&url);
            let sanitized_error = format!("{error:?}").replace(&url, &sanitized_url);
            warn!(
                error = %sanitized_error,
                url = %sanitized_url,
                "could not deepen snapshot object cache from remote; \
                 snapshot will rewrite history and expose synthetic SHAs"
            );
            false
        }
    }
}

/// Strip userinfo credentials from a remote URL so a log never embeds them.
///
/// `https://user:token@github.com/owner/repo` becomes
/// `https://github.com/owner/repo`. Schemeless (SSH) remotes carry no
/// userinfo and are returned untouched.
fn sanitize_remote_url(remote_url: &str) -> String {
    let Some((scheme, rest)) = remote_url.split_once("://") else {
        return remote_url.to_owned();
    };
    let (authority, suffix) = match rest.find('/') {
        Some(index) => rest.split_at(index),
        None => (rest, ""),
    };
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}://{authority}{suffix}")
}

/// Extra-header config that authenticates a deepen fetch against the engine's
/// GitHub credential, or `None` when the remote is not GitHub.
///
/// The static PAT is a GitHub credential; attaching it to any other host would
/// leak it to a remote the operator never vouched for. Scope strictly: only
/// `https://github.com/...` remotes get the header, matching what
/// `actions/checkout` configures for its own auth.
fn github_auth_header_for_remote(remote_url: &str, pat: &str) -> Option<(&'static str, String)> {
    // The credential rides only over https to github.com: never over a
    // plaintext remote, and never over an SSH remote (no scheme at all —
    // SSH has its own key auth).
    let (scheme, rest) = remote_url.split_once("://")?;
    if scheme != "https" {
        return None;
    }
    // Strip userinfo (`https://user:pass@github.com/...`) before extracting
    // the host; the port separator `:` must not truncate a host either.
    let authority = rest.rsplit('@').next().unwrap_or(rest);
    let host = authority.split(['/', ':']).next().unwrap_or("");
    if host != "github.com" {
        return None;
    }
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(format!("x-access-token:{pat}").as_bytes());
    Some((
        "http.https://github.com/.extraheader",
        format!("AUTHORIZATION: basic {encoded}"),
    ))
}
/// Persist the workspace HEAD the cache was last synced to.
///
/// An unborn HEAD has nothing to record; clearing the marker keeps the next
/// call on the always-fetch path.
async fn record_cache_head(marker: &FsPath, source_head: Option<&str>) -> Result<(), ApiError> {
    let result = match source_head {
        Some(head) => tokio::fs::write(marker, format!("{head}\n")).await,
        None => match tokio::fs::remove_file(marker).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        },
    };
    result.map_err(|error| {
        ApiError::internal(format!(
            "failed to record snapshot object cache HEAD: {error}"
        ))
    })
}

/// Hand this run's index to the next one, atomically.
///
/// Concurrent submissions for the same workspace simply race to publish; the
/// index holds only stat data, so either winner is correct.
async fn persist_snapshot_index(staging_index: &FsPath, destination: &FsPath) {
    let Some(parent) = destination.parent() else {
        return;
    };
    let staged = parent.join(format!("{}.tmp", uuid::Uuid::new_v4()));
    if tokio::fs::copy(staging_index, &staged).await.is_err()
        || tokio::fs::rename(&staged, destination).await.is_err()
    {
        let _ = tokio::fs::remove_file(&staged).await;
    }
}

struct CacheLock(PathBuf);

/// Drop a finished run's snapshot repository.
///
/// The repository exists so the run's checkouts can fetch the workspace; once
/// every job is terminal nothing can ask for it again, and a re-run captures a
/// fresh snapshot. Keeping them made the state directory grow without bound —
/// enough matrix runs filled the disk and the engine began failing blob writes
/// with HTTP 500. The persistent object cache is untouched: it is shared and
/// is what makes the next snapshot cheap.
pub(crate) async fn discard_workspace_snapshot(state_dir: &FsPath, run_id: RunId) {
    let started = std::time::Instant::now();
    let repository = state_dir.join("snapshots").join(run_id.to_string());
    match tokio::fs::remove_dir_all(&repository).await {
        Ok(()) => debug!(
            %run_id,
            duration_ms = started.elapsed().as_millis() as u64,
            "Discarded finished run's workspace snapshot"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            %run_id,
            path = %repository.display(),
            %error,
            "Failed to discard workspace snapshot"
        ),
    }
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.0);
    }
}

async fn acquire_cache_lock(path: &FsPath) -> Result<CacheLock, ApiError> {
    let started = std::time::Instant::now();
    loop {
        match std::fs::create_dir(path) {
            Ok(()) => return Ok(CacheLock(path.to_owned())),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = std::fs::metadata(path)
                    .and_then(|metadata| metadata.modified())
                    .ok()
                    .and_then(|modified| modified.elapsed().ok())
                    .is_some_and(|age| age > std::time::Duration::from_secs(60));
                if stale {
                    let _ = std::fs::remove_dir(path);
                    continue;
                }
                if started.elapsed() > std::time::Duration::from_secs(10) {
                    return Err(ApiError::internal(
                        "timed out waiting for snapshot object cache",
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "failed to lock snapshot object cache: {error}"
                )));
            }
        }
    }
}

/// Split a git ident header (`Name <email> 1234567890 +0000`) into the
/// (name, email, date) parts `commit-tree` expects through its env vars.
fn parse_ident(value: &str) -> (String, String, String) {
    let value = value.trim();
    let name;
    let email;
    let date;
    match value.rfind('<') {
        Some(angle) => {
            name = value[..angle].trim().to_owned();
            let tail = &value[angle + 1..];
            match tail.find('>') {
                Some(end) => {
                    email = format!("<{}>", &tail[..end]);
                    date = tail[end + 1..].trim().to_owned();
                }
                None => {
                    email = String::new();
                    date = tail.trim().to_owned();
                }
            }
        }
        None => {
            // Unusual ident without angle brackets; take the last token as
            // the date and everything before as the name.
            let mut parts = value.rsplitn(2, ' ');
            date = parts.next().unwrap_or("").to_owned();
            name = parts.next().unwrap_or("").to_owned();
            email = String::new();
        }
    }
    (name, email, date)
}

fn state_dir_exclusion(state_dir: &FsPath, workspace: &FsPath) -> Result<Option<String>, ApiError> {
    let state_dir = std::fs::canonicalize(state_dir).map_err(|error| {
        ApiError::internal(format!(
            "failed to resolve state directory {}: {error}",
            state_dir.display()
        ))
    })?;
    let Ok(relative) = state_dir.strip_prefix(workspace) else {
        return Ok(None);
    };
    if relative.as_os_str().is_empty() {
        return Err(ApiError::bad_request(
            "AKSH state directory cannot be the local workspace root",
        ));
    }
    let relative = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    Ok(Some(relative))
}

/// Rewrite default primary checkout steps to fetch the local snapshot.
///
/// `runtime_token` is pinned onto the step so the checkout authenticates to
/// [`snapshot_git_http`] with the local HMAC job JWT it expects. Without it the
/// step would fall back to `${{ github.token }}`, which carries a GitHub App
/// installation token or PAT whenever one is configured — neither of which
/// [`authorize_snapshot_token`] can verify.
pub(crate) fn redirect_primary_checkout(
    message: &mut preloop_gha_protocol::azdo::AgentJobRequestMessage,
    snapshot: &WorkspaceSnapshot,
    github_server_url: &str,
    runtime_token: &str,
) -> usize {
    let mut redirected = 0;
    let mut pinned = Vec::new();
    for step in &mut message.steps {
        let is_checkout = step
            .reference
            .as_ref()
            .and_then(|reference| reference.name.as_deref())
            .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"));
        // A checkout whose `repository`/`ref`/`github-server-url` input is
        // provably absent or provably the action's declared default
        // (`${{ github.repository }}` / `${{ github.server_url }}`) selects
        // GitHub's "default branch" semantics — the snapshot IS the local
        // default — so the redirect applies. Everything else is explicitly
        // set: a literal points at a specific remote target, and an
        // unresolved template expression (e.g. `ref: ${{ inputs.head-sha }}`)
        // selects a target the workflow controls at runtime, which the server
        // cannot prove is the default. Redirecting those hijacks the
        // workflow's intended checkout once the runner evaluates them.
        let declared_default = |name: &str| -> Option<&str> {
            match name {
                // actions/checkout's own action.yml defaults. `ref` declares
                // no default, so no expression can be provably the default
                // for it.
                "repository" => Some("${{ github.repository }}"),
                "github-server-url" => Some("${{ github.server_url }}"),
                _ => None,
            }
        };
        let explicitly_set = |name: &str| {
            step.inputs.iter().any(|(key, value)| {
                if !key.eq_ignore_ascii_case(name) {
                    return false;
                }
                let value = value.as_str().trim();
                if value.is_empty() {
                    return false;
                }
                !value.contains("${{")
                    || declared_default(name).is_none_or(|default| value != default)
            })
        };
        if !is_checkout
            || ["repository", "ref", "github-server-url"]
                .iter()
                .any(|reserved| explicitly_set(reserved))
        {
            continue;
        }
        step.inputs
            .insert("repository".to_owned(), snapshot.repository.clone());
        // The snapshot commit SHA. `actions/checkout` treats a bare SHA as a
        // commit ref (it derives `commit` from it, so the fetch lands on the
        // snapshot). A branch ref instead leaves the action's `commit` empty,
        // which skips the targeted refetch and breaks checkout entirely when
        // the all-refs fetch is rejected (shallow-mirrored snapshots).
        step.inputs
            .insert("ref".to_owned(), snapshot.commit_sha.clone());
        step.inputs
            .insert("github-server-url".to_owned(), github_server_url.to_owned());
        step.inputs
            .insert("token".to_owned(), runtime_token.to_owned());
        pinned.push(step.id.to_string());
        redirected += 1;
    }
    if !pinned.is_empty() {
        message.preloop_snapshot_token_steps = Some(pinned);
    }
    redirected
}

/// Serve a snapshot bare repository through Git's read-only smart HTTP CGI.
pub(crate) async fn snapshot_git_http(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, path)): Path<(RunId, String)>,
    request: Request,
) -> Result<Response<Body>, ApiError> {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(snapshot_authorization_token);
    let authorization = match token {
        Some(token) => authorize_snapshot_token(&shared.state, &token, run_id).await,
        None => Err(ApiError::unauthorized(
            "snapshot Git authentication required",
        )),
    };
    // A bare 401 makes git fall back to Basic semantics and prompt for a
    // username ("could not read Username ... terminal prompts disabled").
    // The Bearer challenge tells git the failure is an authentication
    // rejection, so it reports it instead of prompting.
    if let Err(error) = authorization {
        return if error.status() == StatusCode::UNAUTHORIZED {
            Ok(snapshot_unauthorized_response(error.message()))
        } else {
            Err(error)
        };
    }

    let method = request.method().clone();
    let query = request.uri().query().unwrap_or_default().to_owned();
    let valid_request = (method == axum::http::Method::GET
        && path == "info/refs"
        && query == "service=git-upload-pack")
        || (method == axum::http::Method::POST && path == "git-upload-pack");
    if !valid_request {
        return Err(ApiError::not_found("snapshot Git endpoint not found"));
    }

    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let git_protocol = request
        .headers()
        .get("git-protocol")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let request_body = to_bytes(request.into_body(), MAX_GIT_REQUEST_BYTES)
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid Git request body: {error}")))?;

    let project_root = shared.state.state_dir.join("snapshots");
    let repository = project_root.join(run_id.to_string());
    if !repository.is_dir() {
        return Err(ApiError::not_found("workspace snapshot not found"));
    }

    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", &project_root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", format!("/{run_id}/{path}"))
        .env("QUERY_STRING", query)
        .env("REMOTE_USER", "preloop-runner")
        .env("CONTENT_LENGTH", request_body.len().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(content_type) = content_type {
        command.env("CONTENT_TYPE", content_type);
    }
    if let Some(git_protocol) = git_protocol {
        command.env("HTTP_GIT_PROTOCOL", git_protocol);
    }

    let mut child = command.spawn().map_err(|error| {
        ApiError::internal(format!("failed to start git http-backend: {error}"))
    })?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| ApiError::internal("git http-backend stdin unavailable"))?;
    stdin
        .write_all(&request_body)
        .await
        .map_err(|error| ApiError::internal(format!("failed to write Git request: {error}")))?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ApiError::internal("git http-backend stdout unavailable"))?;
    let mut reader = BufReader::new(stdout);
    let mut response = Response::builder();
    let mut status = StatusCode::OK;
    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .await
            .map_err(|error| ApiError::internal(format!("failed to read Git response: {error}")))?;
        if read == 0 {
            let _ = child.kill().await;
            return Err(ApiError::internal(
                "git http-backend ended before emitting CGI headers",
            ));
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            break;
        }
        let Some(separator) = line.iter().position(|byte| *byte == b':') else {
            let _ = child.kill().await;
            return Err(ApiError::internal(
                "git http-backend emitted invalid CGI headers",
            ));
        };
        let name = std::str::from_utf8(&line[..separator])
            .map_err(|error| ApiError::internal(format!("invalid Git response header: {error}")))?;
        let value = std::str::from_utf8(&line[separator + 1..])
            .map_err(|error| ApiError::internal(format!("invalid Git response header: {error}")))?
            .trim();
        if name.eq_ignore_ascii_case("status") {
            let code = value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u16>().ok())
                .and_then(|value| StatusCode::from_u16(value).ok())
                .ok_or_else(|| ApiError::internal("git http-backend emitted invalid status"))?;
            status = code;
        } else {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
                ApiError::internal(format!("invalid Git response header name: {error}"))
            })?;
            let value = HeaderValue::from_str(value).map_err(|error| {
                ApiError::internal(format!("invalid Git response header value: {error}"))
            })?;
            response = response.header(name, value);
        }
    }

    let mut stderr = child.stderr.take();
    tokio::spawn(async move {
        let mut diagnostic = Vec::new();
        if let Some(stderr) = stderr.as_mut() {
            let _ = stderr.read_to_end(&mut diagnostic).await;
        }
        match child.wait().await {
            Ok(exit) if exit.success() => {}
            Ok(exit) => warn!(
                status = %exit,
                stderr = %String::from_utf8_lossy(&diagnostic).trim(),
                "git http-backend failed"
            ),
            Err(error) => warn!(%error, "Failed to reap git http-backend"),
        }
    });

    response
        .status(status)
        .body(Body::from_stream(ReaderStream::new(reader)))
        .map_err(|error| ApiError::internal(format!("failed to build Git response: {error}")))
}

/// 401 response for the snapshot Git surface.
///
/// Carries a `WWW-Authenticate: Bearer` challenge so git reports the
/// rejection instead of falling back to interactive Basic credential
/// prompts that cannot be answered inside a job.
fn snapshot_unauthorized_response(message: &str) -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            header::WWW_AUTHENTICATE,
            "Bearer realm=\"preloop-snapshot\"",
        )
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::json!({ "error": message }).to_string(),
        ))
        .expect("static 401 response is valid")
}

async fn authorize_snapshot_token(
    state: &AppState,
    token: &str,
    run_id: RunId,
) -> Result<(), ApiError> {
    let claims = state
        .verify_local_jwt_claims(token)
        .ok_or_else(|| ApiError::unauthorized("invalid snapshot Git token"))?;
    let job_id = claims
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .and_then(|subject| subject.strip_prefix("preloop-job-"))
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .ok_or_else(|| ApiError::unauthorized("snapshot Git token is not a job token"))?;
    let valid_scope = claims
        .get("scp")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|scope| {
            scope.starts_with("Actions.Results:") && scope.ends_with(&format!(":{job_id}"))
        });
    if !valid_scope {
        return Err(ApiError::unauthorized(
            "snapshot Git token lacks job result scope",
        ));
    }

    let inner = state.inner.lock().await;
    let belongs_to_run = inner
        .agent_job_requests
        .get(&job_id)
        .and_then(|request_id| inner.job_requests.get(request_id))
        .is_some_and(|request| request.run_id == run_id);
    if !belongs_to_run {
        return Err(ApiError::forbidden(
            "snapshot Git token does not belong to this run",
        ));
    }
    Ok(())
}

fn snapshot_authorization_token(value: &str) -> Option<String> {
    let (scheme, credentials) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        return Some(credentials.to_owned());
    }
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(credentials)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (_, token) = decoded.split_once(':')?;
    Some(token.to_owned())
}

#[cfg(test)]
mod auth_scoping_tests {
    use super::github_auth_header_for_remote;

    #[test]
    fn github_remote_gets_scoped_basic_header() {
        let (key, value) = github_auth_header_for_remote(
            "https://github.com/preloopdev/preloop-trigger-e2e-20260715.git",
            "gho_secret",
        )
        .expect("github.com remote should get the header");
        assert_eq!(key, "http.https://github.com/.extraheader");
        // base64("x-access-token:gho_secret")
        assert_eq!(
            value,
            "AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46Z2hvX3NlY3JldA=="
        );
    }

    #[test]
    fn userinfo_in_remote_url_is_stripped_before_host_matching() {
        let header = github_auth_header_for_remote(
            "https://x-access-token:gho_secret@github.com/owner/repo.git",
            "gho_secret",
        );
        assert!(header.is_some(), "userinfo must not defeat host matching");
    }

    #[test]
    fn non_github_remote_never_receives_the_pat() {
        for url in [
            "https://git.example.com/owner/repo.git",
            "https://github.example.net/owner/repo.git",
            "git@github.com:owner/repo.git", // SSH remotes have no scheme
            "http://github.com/owner/repo.git", // https-only credential
        ] {
            assert_eq!(
                github_auth_header_for_remote(url, "gho_secret"),
                None,
                "PAT must not be attached to {url}"
            );
        }
    }
}

#[cfg(test)]
mod deepen_and_redirect_tests {
    use super::*;

    /// A `Write` sink that records every byte, so a test can assert on what a
    /// tracing subscriber actually emitted.
    #[derive(Clone)]
    struct RecordingWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn git_in(cwd: &FsPath, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .status()
            .expect("git runs in tests");
        assert!(status.success(), "git {args:?} failed");
    }

    /// The deepen failure path must never write the remote's embedded
    /// userinfo credential to the server log: the remote URL itself carries
    /// it, and git's stderr echoes the URL verbatim.
    #[tokio::test]
    async fn deepen_failure_warning_never_logs_the_remote_credential() {
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || RecordingWriter(sink.clone()))
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        git_in(&workspace, &["init", "-q", "-b", "main"]);
        git_in(
            &workspace,
            &[
                "config",
                "remote.origin.url",
                "https://user:super-secret-token@127.0.0.1:1/owner/repo.git",
            ],
        );
        // A valid bare repository so the fetch is actually attempted: the
        // connect to 127.0.0.1:1 is refused instantly (loopback, nothing
        // listening), so the failure carries git's stderr, which echoes the
        // credential-bearing URL.
        let repository = temp.path().join("snapshot.git");
        let status = std::process::Command::new("git")
            .args(["init", "-q", "--bare"])
            .arg(&repository)
            .status()
            .unwrap();
        assert!(status.success());

        let deepened = deepen_object_cache_from_remote(&repository, &workspace, None).await;
        assert!(
            !deepened,
            "the deepen must fail against a refused connection"
        );

        let logs = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(
            !logs.contains("super-secret-token"),
            "remote credential leaked into the server log: {logs}"
        );
        assert!(
            !logs.contains("user:"),
            "userinfo leaked into the server log: {logs}"
        );
    }

    #[test]
    fn sanitize_remote_url_strips_userinfo_credentials() {
        assert_eq!(
            sanitize_remote_url("https://user:token@github.com/owner/repo.git"),
            "https://github.com/owner/repo.git"
        );
        assert_eq!(
            sanitize_remote_url("https://user@github.com"),
            "https://github.com"
        );
        // Ports and paths survive; only the userinfo goes.
        assert_eq!(
            sanitize_remote_url("https://user:token@github.example:8443/owner/repo"),
            "https://github.example:8443/owner/repo"
        );
        // An `@` inside the path is not userinfo.
        assert_eq!(
            sanitize_remote_url("https://user:token@github.com/owner/@releases/repo"),
            "https://github.com/owner/@releases/repo"
        );
        // Schemeless (SSH) remotes carry no userinfo and are untouched.
        assert_eq!(
            sanitize_remote_url("git@github.com:owner/repo.git"),
            "git@github.com:owner/repo.git"
        );
    }

    fn checkout_message(
        steps: serde_json::Value,
    ) -> preloop_gha_protocol::azdo::AgentJobRequestMessage {
        serde_json::from_value(serde_json::json!({
            "jobId": "00000000-0000-0000-0000-000000000001",
            "requestId": 1,
            "plan": {
                "planId": "plan",
                "planType": "build",
                "version": 1,
                "artifactUri": "",
                "artifactLocation": ""
            },
            "timeline": {
                "id": "00000000-0000-0000-0000-000000000002",
                "changeId": 0,
                "location": null
            },
            "jobName": "build",
            "lockedUntil": "",
            "resources": {"endpoints": []},
            "steps": steps,
            "snapshot": null
        }))
        .unwrap()
    }

    fn snapshot_fixture() -> WorkspaceSnapshot {
        WorkspaceSnapshot {
            head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            repository: "snapshots/11111111-1111-4111-8111-111111111111".to_owned(),
            default_branch: Some("main".to_owned()),
            before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            snapshot_timing: None,
        }
    }

    fn redirect_count(inputs: serde_json::Value) -> usize {
        let mut message = checkout_message(serde_json::json!([{
            "id": "00000000-0000-0000-0000-000000000010",
            "name": "checkout",
            "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": inputs,
            "continueOnError": false,
            "timeoutInMinutes": null
        }]));
        redirect_primary_checkout(
            &mut message,
            &snapshot_fixture(),
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        )
    }

    /// A checkout whose `ref`/`repository` input is a template expression
    /// selects a target the workflow controls at runtime; redirecting it to
    /// the snapshot hijacks that target once the runner evaluates the
    /// expression. Only inputs that are provably absent — or provably the
    /// action's declared default — may be redirected.
    #[test]
    fn redirect_primary_checkout_respects_dynamic_expression_inputs() {
        // A dynamic ref is an explicit target once evaluated.
        assert_eq!(
            redirect_count(
                serde_json::json!({"ref": "${{ inputs.head-sha }}", "fetch-depth": "0"})
            ),
            0,
            "an unresolved expression ref must count as explicitly set"
        );
        // A dynamic repository likewise.
        assert_eq!(
            redirect_count(serde_json::json!({
                "repository": "${{ fromJSON(inputs.targets)[0].repo }}",
            })),
            0,
            "an unresolved expression repository must count as explicitly set"
        );
        // A literal target is explicitly set.
        assert_eq!(
            redirect_count(
                serde_json::json!({"repository": "octo/other", "ref": "refs/heads/release"})
            ),
            0,
            "a literal remote target must not be redirected"
        );
        // The declared input default (`${{ github.repository }}`) is provably
        // the default branch the snapshot represents — redirect applies.
        assert_eq!(
            redirect_count(serde_json::json!({"repository": "${{ github.repository }}"})),
            1,
            "the declared repository default must still be redirected"
        );
        // `github-server-url`'s declared default is provably the default too.
        assert_eq!(
            redirect_count(serde_json::json!({"github-server-url": "${{ github.server_url }}"})),
            1,
            "the declared server-url default must still be redirected"
        );
        // Absent and empty inputs keep the default-branch redirect.
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-depth": "0"})),
            1,
            "an absent ref/repository is default-branch semantics"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "", "fetch-depth": "0"})),
            1,
            "an empty ref is default-branch semantics"
        );
    }
}

mod snapshot_deserialization_tests {
    use super::WorkspaceSnapshot;

    // A snapshot persisted before `tree_sha` existed has no such field. The
    // store restores old run records at startup; a missing field must load as
    // empty rather than abort the whole control plane (regression: the field
    // was added without a serde default and a restart died with
    // `missing field tree_sha` on the first pre-upgrade snapshot).
    #[test]
    fn snapshot_without_tree_sha_still_deserializes() {
        let snapshot: WorkspaceSnapshot = serde_json::from_value(serde_json::json!({
            "commit_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "head_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "repository": "preloopdev/preloop",
            "default_branch": "main",
            "before_sha": null,
        }))
        .unwrap();
        assert_eq!(snapshot.tree_sha, "");
        assert_eq!(snapshot.commit_sha, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(snapshot.head_sha.as_deref(), Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    }

    #[test]
    fn snapshot_with_tree_sha_round_trips() {
        let snapshot: WorkspaceSnapshot = serde_json::from_value(serde_json::json!({
            "commit_sha": "cccccccccccccccccccccccccccccccccccccccc",
            "tree_sha": "dddddddddddddddddddddddddddddddddddddddd",
            "head_sha": null,
            "repository": "preloopdev/preloop",
            "default_branch": null,
            "before_sha": null,
        }))
        .unwrap();
        assert_eq!(snapshot.tree_sha, "dddddddddddddddddddddddddddddddddddddddd");
    }
}
