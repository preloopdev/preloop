//! Hosted dirty submits: the CLI uploads the commit it wants tested as a git
//! bundle, and the engine unpacks it into an isolated workspace so the shared
//! snapshot and merge-builder paths work without access to the submitter's disk.
//!
//! A bundle carries only refs under [`SUBMIT_REFS`]: `<id>/head` (the submitted
//! commit) and, for pull-request submissions, `<id>/base/<branch>` (the tip of
//! the base branch the merge is built against). Nothing else from the
//! submitter's repository is uploaded.

use crate::ApiError;
use axum::{Json, body::Body, extract::State};
use futures::StreamExt;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Largest bundle accepted. Enforced while the body streams to disk, so an
/// oversized upload is refused without ever being held in memory.
const MAX_BUNDLE_BYTES: u64 = 512 * 1024 * 1024;

/// Uploads nobody submitted, and scratch a crashed submission left behind, are
/// reclaimed after this long. A submission holds its scratch only for the
/// duration of `submit_run`, so the margin is wide.
const ABANDONED_AFTER: Duration = Duration::from_secs(60 * 60);

/// Ref namespace the CLI bundles: `<id>/head` and `<id>/base/<branch>`.
const SUBMIT_REFS: &str = "refs/preloop/submit/";

/// Local branch the submitted commit is pinned under so a workspace fetch
/// transfers its tree as well as its commit.
const SUBMIT_BRANCH: &str = "preloop-submit";

#[derive(Debug, Serialize)]
pub struct BundleAccepted {
    pub bundle_id: String,
}

fn bundle_root(shared: &crate::state::SharedState) -> PathBuf {
    shared.state.state_dir.join("bundles")
}

/// Store and validate an uploaded git bundle. The returned opaque id is safe to
/// put in a workflow submission; callers never send a server filesystem path.
pub async fn upload_bundle(
    State(shared): State<Arc<crate::state::SharedState>>,
    body: Body,
) -> Result<Json<BundleAccepted>, ApiError> {
    let root = bundle_root(&shared);
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|error| ApiError::internal(format!("failed to create bundle store: {error}")))?;
    sweep_abandoned(&root).await;

    let bundle_id = uuid::Uuid::new_v4().to_string();
    let path = root.join(format!("{bundle_id}.bundle"));
    let temp = root.join(format!(".{bundle_id}.tmp"));
    if let Err(error) = write_limited(&temp, body).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    // `git bundle verify` needs an object database to check prerequisites;
    // verify in an isolated temporary bare repository before publishing.
    let verify_repo = root.join(format!(".verify-{bundle_id}.git"));
    let verified = verify_bundle(&verify_repo, &temp).await;
    let _ = tokio::fs::remove_dir_all(&verify_repo).await;
    if let Err(error) = verified {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(error);
    }
    tokio::fs::rename(&temp, &path)
        .await
        .map_err(|error| ApiError::internal(format!("failed to publish git bundle: {error}")))?;
    Ok(Json(BundleAccepted { bundle_id }))
}

/// Stream the request body to `path`, refusing it as soon as it passes the cap.
async fn write_limited(path: &Path, body: Body) -> Result<(), ApiError> {
    let store_error =
        |error: std::io::Error| ApiError::internal(format!("failed to store git bundle: {error}"));
    let mut file = tokio::fs::File::create(path).await.map_err(store_error)?;
    let mut stream = body.into_data_stream();
    let mut written: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| ApiError::bad_request(format!("git bundle upload failed: {error}")))?;
        written += chunk.len() as u64;
        if written > MAX_BUNDLE_BYTES {
            return Err(ApiError::payload_too_large(format!(
                "git bundle exceeds the {} MiB upload limit",
                MAX_BUNDLE_BYTES / (1024 * 1024)
            )));
        }
        file.write_all(&chunk).await.map_err(store_error)?;
    }
    file.flush().await.map_err(store_error)?;
    if written == 0 {
        return Err(ApiError::bad_request("git bundle upload is empty"));
    }
    Ok(())
}

async fn verify_bundle(verify_repo: &Path, bundle: &Path) -> Result<(), ApiError> {
    let repo = utf8(verify_repo)?;
    git(None, &["init", "-q", "--bare", repo])
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to prepare bundle verification: {error}"))
        })?;
    git(
        None,
        &["--git-dir", repo, "bundle", "verify", utf8(bundle)?],
    )
    .await
    .map_err(|error| ApiError::bad_request(format!("invalid git bundle: {error}")))?;
    Ok(())
}

/// One submission's unpacked bundle. Dropping it reclaims the scratch
/// directory (the claimed bundle, its bare repository and the workspace), so a
/// submission leaves nothing behind whether it succeeds, fails or returns early.
pub struct BundleWorkspace {
    scratch: PathBuf,
    workspace: PathBuf,
}

impl BundleWorkspace {
    /// The checked-out workspace: a git repository whose `origin` is the bare
    /// repository holding the bundled commits.
    pub fn path(&self) -> &Path {
        &self.workspace
    }
}

impl Drop for BundleWorkspace {
    fn drop(&mut self) {
        let scratch = std::mem::take(&mut self.scratch);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let _ = tokio::fs::remove_dir_all(scratch).await;
                });
            }
            Err(_) => {
                let _ = std::fs::remove_dir_all(scratch);
            }
        }
    }
}

/// Claim one uploaded bundle and materialize it as an isolated workspace for a
/// hosted run. A bundle is single-use: the first caller wins an exclusive claim
/// and any other (a retry, a replayed id) is refused, so two submissions can
/// never share one working tree and a failed unpack never leaves state a later
/// attempt would mistake for a finished one.
///
/// The workspace has a local bare `origin`, allowing the shared local snapshot
/// and merge-builder paths to operate without access to the submitter's disk.
pub async fn materialize_bundle(
    shared: &crate::state::SharedState,
    bundle_id: &str,
    commit_sha: &str,
) -> Result<BundleWorkspace, ApiError> {
    let id = uuid::Uuid::parse_str(bundle_id)
        .map_err(|_| ApiError::bad_request("invalid git bundle id"))?
        .to_string();
    if !is_commit_sha(commit_sha) {
        return Err(ApiError::bad_request(
            "bundle submission has an invalid commit SHA",
        ));
    }
    let root = bundle_root(shared);
    let scratch = root.join(format!("{id}.work"));
    let unavailable =
        || ApiError::bad_request("git bundle has expired, was already used, or was not uploaded");
    // `mkdir` is the claim: exactly one caller creates the directory.
    match tokio::fs::create_dir(&scratch).await {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound
            ) =>
        {
            return Err(unavailable());
        }
        Err(error) => {
            return Err(ApiError::internal(format!(
                "failed to claim git bundle: {error}"
            )));
        }
    }
    let claimed = scratch.join("bundle");
    if tokio::fs::rename(root.join(format!("{id}.bundle")), &claimed)
        .await
        .is_err()
    {
        let _ = tokio::fs::remove_dir_all(&scratch).await;
        return Err(unavailable());
    }
    // From here the guard owns the scratch: any error below drops it.
    let guard = BundleWorkspace {
        workspace: scratch.join("workspace"),
        scratch,
    };
    build_workspace(
        &claimed,
        &guard.scratch.join("repo.git"),
        &guard.workspace,
        commit_sha,
    )
    .await?;
    Ok(guard)
}

async fn build_workspace(
    bundle: &Path,
    bare: &Path,
    workspace: &Path,
    commit_sha: &str,
) -> Result<(), ApiError> {
    let bare_dir = utf8(bare)?;
    git(None, &["init", "-q", "--bare", bare_dir])
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to initialize bundle repository: {error}"))
        })?;
    // Fetch exactly the submit namespace: a bundle with no branches has
    // nothing for `git clone` to import, and anything else a (hostile)
    // uploader packed is simply not brought in.
    git(
        None,
        &[
            "--git-dir",
            bare_dir,
            "fetch",
            "-q",
            utf8(bundle)?,
            "+refs/preloop/submit/*:refs/preloop/submit/*",
        ],
    )
    .await
    .map_err(|error| ApiError::bad_request(format!("cannot ingest git bundle: {error}")))?;
    git(
        None,
        &[
            "--git-dir",
            bare_dir,
            "cat-file",
            "-e",
            &format!("{commit_sha}^{{commit}}"),
        ],
    )
    .await
    .map_err(|error| {
        ApiError::bad_request(format!("bundle does not contain submitted commit: {error}"))
    })?;
    // Pin the submitted commit under a local branch so the workspace fetch
    // below transfers its tree as well as its commit.
    git(
        None,
        &[
            "--git-dir",
            bare_dir,
            "update-ref",
            &format!("refs/heads/{SUBMIT_BRANCH}"),
            commit_sha,
        ],
    )
    .await
    .map_err(|error| ApiError::bad_request(format!("cannot retain submitted commit: {error}")))?;
    // The base tip the submitter merged against becomes the `origin` branch the
    // merge builder reads, so a pull-request merge uses it rather than whatever
    // stale local branch of the same name exists in the uploader's repository.
    for (branch, sha) in bundled_base_tips(bare_dir).await? {
        git(
            None,
            &[
                "--git-dir",
                bare_dir,
                "update-ref",
                &format!("refs/heads/{branch}"),
                &sha,
            ],
        )
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("cannot retain base branch `{branch}`: {error}"))
        })?;
    }

    tokio::fs::create_dir_all(workspace)
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to create bundle workspace: {error}"))
        })?;
    git(Some(workspace), &["init", "-q", "-b", SUBMIT_BRANCH])
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to initialize bundle workspace: {error}"))
        })?;
    git(Some(workspace), &["remote", "add", "origin", bare_dir])
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to configure bundle origin: {error}"))
        })?;
    git(Some(workspace), &["fetch", "-q", "origin"])
        .await
        .map_err(|error| ApiError::bad_request(format!("cannot unpack git bundle: {error}")))?;
    git(Some(workspace), &["checkout", "-q", "--detach", commit_sha])
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("bundle does not contain submitted commit: {error}"))
        })?;
    Ok(())
}

/// `(branch, sha)` for every `<id>/base/<branch>` ref in the bundle. Branch
/// names come from the uploader, so each must be a name git itself accepts and
/// must not collide with the branch reserved for the submitted commit.
async fn bundled_base_tips(bare_dir: &str) -> Result<Vec<(String, String)>, ApiError> {
    let listing = git(
        None,
        &[
            "--git-dir",
            bare_dir,
            "for-each-ref",
            "--format=%(objectname) %(refname)",
            SUBMIT_REFS,
        ],
    )
    .await
    .map_err(|error| ApiError::bad_request(format!("cannot list bundled refs: {error}")))?;
    let mut tips = Vec::new();
    for line in listing.lines() {
        let Some((sha, refname)) = line.split_once(' ') else {
            continue;
        };
        let Some(branch) = refname
            .strip_prefix(SUBMIT_REFS)
            .and_then(|rest| rest.split_once('/'))
            .and_then(|(_, tail)| tail.strip_prefix("base/"))
        else {
            continue;
        };
        if branch == SUBMIT_BRANCH
            || git(None, &["check-ref-format", &format!("refs/heads/{branch}")])
                .await
                .is_err()
        {
            return Err(ApiError::bad_request(format!(
                "bundle names an invalid base branch `{branch}`"
            )));
        }
        tips.push((branch.to_owned(), sha.to_owned()));
    }
    Ok(tips)
}

/// Remove anything in the bundle store that has sat untouched past
/// [`ABANDONED_AFTER`]: bundles that were uploaded but never submitted, and
/// scratch directories a crashed submission left behind. Runs on every upload,
/// so the store is bounded by upload traffic and needs no extra task.
async fn sweep_abandoned(root: &Path) {
    let Ok(mut entries) = tokio::fs::read_dir(root).await else {
        return;
    };
    let now = SystemTime::now();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        let abandoned = metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age > ABANDONED_AFTER);
        if !abandoned {
            continue;
        }
        let path = entry.path();
        let _ = if metadata.is_dir() {
            tokio::fs::remove_dir_all(&path).await
        } else {
            tokio::fs::remove_file(&path).await
        };
    }
}

/// A full SHA-1 commit id. GitHub cannot hold SHA-256 repositories, so a
/// 64-character id could never be published; refusing it here is simpler than
/// initializing every repository with the matching object format.
fn is_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn utf8(path: &Path) -> Result<&str, ApiError> {
    path.to_str()
        .ok_or_else(|| ApiError::internal("bundle store path is not valid UTF-8"))
}

/// Run `git <args>` (in `cwd` when given) and return its trimmed stdout, or its
/// trimmed stderr on failure.
async fn git(cwd: Option<&Path>, args: &[&str]) -> Result<String, String> {
    let mut command = Command::new("git");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command
        .args(args)
        .output()
        .await
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}
