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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceSnapshot {
    pub(crate) commit_sha: String,
    pub(crate) repository: String,
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
) -> Result<WorkspaceSnapshot, ApiError> {
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
        "aksh-workspace-snapshot-{run_id}-{}",
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

    let commit_sha = result?;
    info!(
        %run_id,
        %commit_sha,
        repository = %final_repository.display(),
        "Created immutable workspace snapshot"
    );
    Ok(WorkspaceSnapshot {
        commit_sha,
        repository,
    })
}

async fn create_workspace_snapshot_inner(
    state_dir: &FsPath,
    workspace: &FsPath,
    staging_repository: &FsPath,
    staging_index: &FsPath,
    final_repository: &FsPath,
    run_id: RunId,
) -> Result<String, ApiError> {
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
    } = probe?;
    let source_objects = common_dir.join("objects");
    if !source_objects.is_dir() {
        return Err(ApiError::bad_request(format!(
            "source Git object directory does not exist: {}",
            source_objects.display()
        )));
    }
    let cache =
        ensure_object_cache(state_dir, workspace, &common_dir, source_head.as_deref()).await?;
    let ObjectCache {
        objects: cached_objects,
        index: cache_index,
        refreshed: cache_refreshed,
    } = cache;

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
    add.args(["add", "--all", "--", ":/"]);
    if let Some(excluded_state) = state_dir_exclusion(state_dir, workspace)? {
        add.arg(format!(":(exclude,top){excluded_state}/**"));
    }
    run_git(&mut add, "stage local workspace state").await?;

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
        .env("GIT_AUTHOR_NAME", "aksh")
        .env("GIT_AUTHOR_EMAIL", "snapshot@aksh.local")
        .env("GIT_AUTHOR_DATE", "1970-01-01T00:00:00Z")
        .env("GIT_COMMITTER_NAME", "aksh")
        .env("GIT_COMMITTER_EMAIL", "snapshot@aksh.local")
        .env("GIT_COMMITTER_DATE", "1970-01-01T00:00:00Z")
        .args([
            "commit-tree",
            tree.as_str(),
            "-m",
            "aksh workspace snapshot",
        ]);
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
    let update_input =
        format!("update {SNAPSHOT_REF} {commit_sha}\nsymref-update HEAD {SNAPSHOT_REF}\n");
    run_git_with_stdin(
        &mut update_refs,
        update_input.as_bytes(),
        "publish snapshot refs",
    )
    .await?;

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

    tokio::fs::rename(staging_repository, final_repository)
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to publish snapshot repository for run {run_id}: {error}"
            ))
        })?;
    Ok(commit_sha)
}

/// What one `git rev-parse` tells us about the source workspace.
struct WorkspaceRevision {
    /// Canonical `.git` common directory backing the worktree.
    common_dir: PathBuf,
    /// Current `HEAD` commit, absent when the branch is unborn.
    source_head: Option<String>,
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
    Ok(WorkspaceRevision {
        common_dir,
        source_head,
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
}

async fn ensure_object_cache(
    state_dir: &FsPath,
    workspace: &FsPath,
    common_dir: &FsPath,
    source_head: Option<&str>,
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
    let mut index = repository.as_os_str().to_owned();
    index.push(".index");
    Ok(ObjectCache {
        objects,
        index: PathBuf::from(index),
        refreshed,
    })
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
    let repository = state_dir.join("snapshots").join(run_id.to_string());
    match tokio::fs::remove_dir_all(&repository).await {
        Ok(()) => debug!(%run_id, "Discarded finished run's workspace snapshot"),
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
    message: &mut aksh_gha_protocol::azdo::AgentJobRequestMessage,
    snapshot: &WorkspaceSnapshot,
    github_server_url: &str,
    runtime_token: &str,
) -> usize {
    let mut redirected = 0;
    for step in &mut message.steps {
        let is_checkout = step
            .reference
            .as_ref()
            .and_then(|reference| reference.name.as_deref())
            .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"));
        if !is_checkout
            || step.inputs.keys().any(|key| {
                ["repository", "ref", "token", "github-server-url"]
                    .iter()
                    .any(|reserved| key.eq_ignore_ascii_case(reserved))
            })
        {
            continue;
        }
        step.inputs
            .insert("repository".to_owned(), snapshot.repository.clone());
        step.inputs
            .insert("ref".to_owned(), snapshot.commit_sha.clone());
        step.inputs
            .insert("github-server-url".to_owned(), github_server_url.to_owned());
        step.inputs
            .insert("token".to_owned(), runtime_token.to_owned());
        redirected += 1;
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
        .and_then(snapshot_authorization_token)
        .ok_or_else(|| ApiError::unauthorized("snapshot Git authentication required"))?;
    authorize_snapshot_token(&shared.state, &token, run_id).await?;

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
        .env("REMOTE_USER", "aksh-runner")
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
        .and_then(|subject| subject.strip_prefix("aksh-job-"))
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
