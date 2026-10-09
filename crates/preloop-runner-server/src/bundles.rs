use crate::ApiError;
use axum::{Json, extract::State};
use bytes::Bytes;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;

const MAX_BUNDLE_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub struct BundleAccepted {
    pub bundle_id: String,
}

/// Store and validate an uploaded git bundle. The returned opaque id is safe to
/// put in a workflow submission; callers never send a server filesystem path.
pub async fn upload_bundle(
    State(shared): State<Arc<crate::state::SharedState>>,
    body: Bytes,
) -> Result<Json<BundleAccepted>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad_request("git bundle upload is empty"));
    }
    if body.len() > MAX_BUNDLE_BYTES {
        return Err(ApiError::bad_request(format!(
            "git bundle exceeds the {} MiB upload limit",
            MAX_BUNDLE_BYTES / (1024 * 1024)
        )));
    }
    let root = shared.state.state_dir.join("bundles");
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(|error| ApiError::internal(format!("failed to create bundle store: {error}")))?;
    let bundle_id = uuid::Uuid::new_v4().to_string();
    let path = root.join(format!("{bundle_id}.bundle"));
    let temp = root.join(format!(".{bundle_id}.tmp"));
    tokio::fs::write(&temp, &body)
        .await
        .map_err(|error| ApiError::internal(format!("failed to store git bundle: {error}")))?;
    // `git bundle verify` needs an object database to check prerequisites;
    // verify in an isolated temporary bare repository before publishing.
    let verify_repo = root.join(format!(".verify-{bundle_id}.git"));
    if let Err(error) = git_command(&["init", "--bare", verify_repo.to_str().unwrap()]).await {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(ApiError::internal(format!(
            "failed to prepare bundle verification: {error}"
        )));
    }
    let verified = git_command(&[
        "--git-dir",
        verify_repo.to_str().unwrap(),
        "bundle",
        "verify",
        temp.to_str().unwrap(),
    ])
    .await;
    let _ = tokio::fs::remove_dir_all(&verify_repo).await;
    if let Err(error) = verified {
        let _ = tokio::fs::remove_file(&temp).await;
        return Err(ApiError::bad_request(format!(
            "invalid git bundle: {error}"
        )));
    }
    tokio::fs::rename(&temp, &path)
        .await
        .map_err(|error| ApiError::internal(format!("failed to publish git bundle: {error}")))?;
    Ok(Json(BundleAccepted { bundle_id }))
}

/// Materialize one uploaded bundle as an isolated workspace for a hosted run.
/// The workspace has a local bare `origin`, allowing the shared local snapshot
/// and merge-builder paths to operate without access to the submitter's disk.
pub async fn materialize_bundle(
    shared: &crate::state::SharedState,
    bundle_id: &str,
    commit_sha: &str,
) -> Result<PathBuf, ApiError> {
    let id = uuid::Uuid::parse_str(bundle_id)
        .map_err(|_| ApiError::bad_request("invalid git bundle id"))?;
    let id = id.to_string();
    let root = shared.state.state_dir.join("bundles");
    let bundle = root.join(format!("{id}.bundle"));
    if !bundle.is_file() {
        return Err(ApiError::bad_request(
            "git bundle has expired or was not uploaded",
        ));
    }
    if !is_hex_commit(commit_sha) {
        return Err(ApiError::bad_request(
            "bundle submission has an invalid commit SHA",
        ));
    }
    let bare = root.join(format!("{id}.git"));
    if !bare.is_dir() {
        git_command(&[
            "clone",
            "--bare",
            bundle.to_str().unwrap(),
            bare.to_str().unwrap(),
        ])
        .await
        .map_err(|error| ApiError::bad_request(format!("cannot ingest git bundle: {error}")))?;
    }
    // Bundle refs outside `refs/heads`/`refs/tags` are not retained by
    // `git clone --bare`, even though their objects are present in the clone.
    // Pin the caller's submitted commit under a local branch so the following
    // workspace fetch transfers its tree as well as its commit.
    git_command(&[
        "--git-dir",
        bare.to_str().unwrap(),
        "update-ref",
        "refs/heads/preloop-submit",
        commit_sha,
    ])
    .await
    .map_err(|error| ApiError::bad_request(format!("cannot retain submitted commit: {error}")))?;
    let workspace = root.join(format!("{id}.workspace"));
    if !workspace.is_dir() {
        tokio::fs::create_dir_all(&workspace)
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to create bundle workspace: {error}"))
            })?;
        git_command_in(&workspace, &["init", "-q", "-b", "preloop-submit"])
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to initialize bundle workspace: {error}"))
            })?;
        git_command_in(
            &workspace,
            &["remote", "add", "origin", bare.to_str().unwrap()],
        )
        .await
        .map_err(|error| {
            ApiError::internal(format!("failed to configure bundle origin: {error}"))
        })?;
        git_command_in(&workspace, &["fetch", "-q", "origin"])
            .await
            .map_err(|error| ApiError::bad_request(format!("cannot unpack git bundle: {error}")))?;
    }
    git_command_in(&workspace, &["checkout", "-q", "--detach", commit_sha])
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("bundle does not contain submitted commit: {error}"))
        })?;
    Ok(workspace)
}

fn is_hex_commit(value: &str) -> bool {
    (40..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn git_command(args: &[&str]) -> Result<(), String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .await
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

async fn git_command_in(cwd: &Path, args: &[&str]) -> Result<(), String> {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .await
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}
