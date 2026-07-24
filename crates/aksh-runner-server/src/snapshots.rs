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
    ensure_git_worktree(&workspace).await?;

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
    run_git(
        Command::new("git")
            .args(["init", "--bare", "--quiet"])
            .arg(staging_repository),
        "initialize snapshot repository",
    )
    .await?;

    let common_dir_output = run_git(
        Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["rev-parse", "--git-common-dir"]),
        "resolve source Git common directory",
    )
    .await?;
    let common_dir = output_path(&common_dir_output, workspace)?;
    let source_objects = common_dir.join("objects");
    if !source_objects.is_dir() {
        return Err(ApiError::bad_request(format!(
            "source Git object directory does not exist: {}",
            source_objects.display()
        )));
    }
    let cached_objects = ensure_object_cache(state_dir, workspace, &common_dir).await?;

    let source_head = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to resolve source HEAD: {error}")))?;

    if source_head.status.success() {
        let head = output_text(&source_head, "resolve source HEAD")?;
        run_snapshot_git(
            workspace,
            staging_repository,
            staging_index,
            &cached_objects,
            ["read-tree", head.as_str()],
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

    run_snapshot_git(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
        ["update-ref", SNAPSHOT_REF, commit_sha.as_str()],
        "publish snapshot ref",
    )
    .await?;
    run_snapshot_git(
        workspace,
        staging_repository,
        staging_index,
        &cached_objects,
        ["symbolic-ref", "HEAD", SNAPSHOT_REF],
        "set snapshot default ref",
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
    // cache without decompressing and re-hashing every historical blob on every
    // submission. Git clone/fetch validate incoming objects; connectivity-only
    // catches a missing alternate object while keeping warm snapshots bounded by
    // the current tree rather than total repository history.
    let mut fsck = Command::new("git");
    fsck.env("GIT_DIR", staging_repository)
        .args(["fsck", "--connectivity-only", "--no-dangling"]);
    run_git(&mut fsck, "verify incremental snapshot repository").await?;

    tokio::fs::rename(staging_repository, final_repository)
        .await
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to publish snapshot repository for run {run_id}: {error}"
            ))
        })?;
    Ok(commit_sha)
}

async fn ensure_git_worktree(workspace: &FsPath) -> Result<(), ApiError> {
    let output = run_git(
        Command::new("git")
            .arg("-C")
            .arg(workspace)
            .args(["rev-parse", "--is-inside-work-tree"]),
        "validate local Git workspace",
    )
    .await?;
    if output.stdout != b"true\n" && output.stdout != b"true\r\n" {
        return Err(ApiError::bad_request(format!(
            "local workspace is not a Git worktree: {}",
            workspace.display()
        )));
    }
    Ok(())
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

fn output_path(output: &std::process::Output, workspace: &FsPath) -> Result<PathBuf, ApiError> {
    let path = PathBuf::from(output_text(output, "resolve source Git common directory")?);
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

async fn ensure_object_cache(
    state_dir: &FsPath,
    workspace: &FsPath,
    common_dir: &FsPath,
) -> Result<PathBuf, ApiError> {
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
            Ok(()) => {}
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
    } else {
        // Fetch only adds immutable objects and atomically updates refs. Auto
        // GC is disabled, so active run alternates cannot lose base objects.
        let mut fetch = Command::new("git");
        fetch
            .env("GIT_DIR", &repository)
            .args(["fetch", "--quiet", "--force", "--prune"])
            .arg(workspace)
            .args(["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]);
        run_git(&mut fetch, "refresh snapshot object cache").await?;
    }

    std::fs::canonicalize(repository.join("objects")).map_err(|error| {
        ApiError::internal(format!("failed to resolve snapshot object cache: {error}"))
    })
}

struct CacheLock(PathBuf);

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
pub(crate) fn redirect_primary_checkout(
    message: &mut aksh_gha_protocol::azdo::AgentJobRequestMessage,
    snapshot: &WorkspaceSnapshot,
    github_server_url: &str,
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
