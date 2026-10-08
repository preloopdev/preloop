//! Self-built pull-request test merges.
//!
//! GitHub computes a pull request's test merge asynchronously, so a webhook
//! payload's `merge_commit_sha` is often the *previous* head's merge (or
//! null), and a run created from it tests a tree the pull request no longer
//! has. This engine therefore builds the merge itself: fetch the current base
//! tip and the head into the engine's mirror, `git merge-tree --write-tree`
//! them, and commit the result with two parents. The merge commit exists only
//! in the engine, so any run on it is served to its jobs from the engine
//! ([`attach_merge`] / [`attach_prebuilt_merge`]), never from the forge.
//!
//! Three flows share this module:
//!
//! 1. local `--event pull_request` runs (`create_workspace_snapshot` builds the
//!    merge into the run's snapshot repository before it is published);
//! 2. hosted submits whose CLI already created the tested commit C
//!    (`WorkflowSubmission::prebuilt_merge` carries it through
//!    [`attach_prebuilt_merge`]);
//! 3. webhook `pull_request` deliveries when GitHub's own merge cannot be
//!    verified in time (the delivery worker builds one and attaches it the
//!    same way).
//!
//! Merge inputs are validated, never trusted: a prebuilt merge must exist in
//! the named mirror with exactly the claimed parents and tree.

use super::*;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;

/// Fixed identity for self-built merge commits. A fixed name and date keep
/// the merge sha deterministic for the same parents, so replays and
/// concurrent deliveries agree on one commit.
pub const MERGE_IDENTITY_NAME: &str = "preloop";
pub const MERGE_IDENTITY_EMAIL: &str = "merge.local";
const MERGE_IDENTITY_DATE: &str = "1970-01-01T00:00:00Z";

/// Where a pull request's head comes from.
#[derive(Debug, Clone)]
pub enum MergeHead {
    /// A commit that must already be present in the mirror, or (when a fetch
    /// URL is configured) is fetched by sha.
    Commit(String),
    /// A branch of the same repository: `refs/heads/<name>`.
    Branch(String),
    /// GitHub's pull ref: `refs/pull/<n>/head`. Exists for fork and
    /// same-repo pull requests alike, which makes it the reliable head
    /// source for webhook deliveries.
    PullRef(u64),
}

/// One pull-request test merge to build.
#[derive(Debug, Clone)]
pub struct MergeRequest {
    /// `owner/repo`, for fetch URLs and log context.
    pub repository: String,
    /// Pull request number when known; labels `refs/pull/<n>/merge` in the
    /// served repository.
    pub pull_request_number: Option<u64>,
    /// Base branch name (`main`); the current tip is fetched as
    /// `refs/heads/<base>` and becomes the merge's first parent.
    pub base_branch: String,
    /// The head side (merge's second parent).
    pub head: MergeHead,
}

/// A local workspace `pull_request` run's merge inputs.
///
/// The head is implicit: the workspace `HEAD` when its tree is clean, the
/// synthetic snapshot commit when it is dirty — only snapshot creation can
/// tell which, and merging the dirty synthetic commit is what makes the
/// uncommitted edits part of the tested tree. The base branch is fetched from
/// the workspace's `origin` into the snapshot's shared object cache.
#[derive(Debug, Clone)]
pub struct LocalPullRequestMerge {
    /// `owner/repo`, for fetch URLs and log context.
    pub repository: String,
    /// Pull request number when known; labels `refs/pull/<n>/merge`.
    pub pull_request_number: Option<u64>,
    /// Base branch name (`main`).
    pub base_branch: String,
}

/// The engine mirror a merge is fetched into and built from.
#[derive(Debug, Clone)]
pub struct MergeSource {
    /// Bare repository holding (and receiving) the objects.
    pub mirror: PathBuf,
    /// Remote to fetch base/head from. `None` means the objects must already
    /// exist in the mirror (tests, local heads).
    pub fetch_url: Option<String>,
    /// Engine GitHub credential. Attached only when `fetch_url` is an
    /// `https://` remote whose host is `github.com` or the configured forge
    /// host ([`Self::forge_host`]), so it never leaks to another host.
    pub token: Option<String>,
    /// Host of the configured forge (`github.com`, or a GHES hostname); the
    /// credential above may also ride to this host.
    pub forge_host: Option<String>,
}

impl Default for MergeSource {
    fn default() -> Self {
        Self {
            mirror: PathBuf::new(),
            fetch_url: None,
            token: None,
            forge_host: None,
        }
    }
}

/// Resolved parents of a merge.
#[derive(Debug, Clone)]
pub struct MergeParents {
    pub base_sha: String,
    pub head_sha: String,
}

/// A built merge commit.
#[derive(Debug, Clone)]
pub struct MergedMerge {
    /// The two-parent merge commit; exists in the served repository.
    pub sha: String,
    /// Its tree.
    pub tree: String,
    /// First parent: the base tip that was merged into.
    pub base_sha: String,
    /// Second parent: the head that was merged.
    pub head_sha: String,
}

/// Outcome of building a merge.
#[derive(Debug)]
pub enum MergeOutcome {
    Merged(MergedMerge),
    /// The head does not merge cleanly into the base tip. GitHub does not run
    /// `pull_request` workflows for a conflicted pull request either.
    Conflict {
        base_sha: String,
        head_sha: String,
        /// Conflicted paths, as reported by `git merge-tree --name-only`.
        paths: Vec<String>,
    },
}

/// Merge build failures.
#[derive(Debug)]
pub enum MergeError {
    /// A fetch failed: upstream unreachable, refused, or no matching ref.
    /// Transient by nature — a redelivery or retry may succeed.
    Fetch {
        refspec: String,
        url: String,
        message: String,
    },
    /// The base branch does not exist at the remote.
    BaseBranchMissing { branch: String, url: String },
    /// An input commit is not present where it must be.
    MissingCommit { sha: String, repository: String },
    /// A `prebuilt_merge` submission named a mirror or commit that failed
    /// validation (bad path, wrong parents, wrong tree).
    InvalidPrebuilt(String),
    /// `git` itself failed.
    Git { operation: String, message: String },
    /// Filesystem failure while preparing a served repository.
    Io { path: String, message: String },
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fetch {
                refspec,
                url,
                message,
            } => write!(formatter, "could not fetch {refspec} from {url}: {message}"),
            Self::BaseBranchMissing { branch, url } => {
                write!(formatter, "base branch `{branch}` was not found at {url}")
            }
            Self::MissingCommit { sha, repository } => {
                write!(formatter, "commit {sha} is not present in {repository}")
            }
            Self::InvalidPrebuilt(message) => {
                write!(formatter, "prebuilt merge is not valid: {message}")
            }
            Self::Git {
                operation,
                message,
            } => write!(formatter, "git {operation} failed: {message}"),
            Self::Io { path, message } => write!(formatter, "{path}: {message}"),
        }
    }
}

impl std::error::Error for MergeError {}

impl MergeError {
    /// Whether a retry (delivery redelivery, user re-run) may succeed.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Fetch { .. })
    }

    /// Map a submit-time merge failure to the HTTP error a submission gets.
    pub fn into_api_error(self) -> ApiError {
        match self {
            Self::Fetch { .. } | Self::BaseBranchMissing { .. } | Self::MissingCommit { .. } => {
                ApiError::bad_request(format!(
                    "could not build the pull-request test merge: {self}. Re-run with \
                     --no-merge to test the branch alone."
                ))
            }
            Self::InvalidPrebuilt(_) => ApiError::bad_request(format!(
                "could not build the pull-request test merge: {self}"
            )),
            Self::Git { .. } | Self::Io { .. } => ApiError::internal(format!(
                "could not build the pull-request test merge: {self}"
            )),
        }
    }
}

/// Whether `sha` is a full lowercase-or-uppercase hex object id.
fn is_object_id(sha: &str) -> bool {
    sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Whether `repository` holds `sha` as a commit.
async fn commit_present(repository: &Path, sha: &str) -> bool {
    if !is_object_id(sha) {
        return false;
    }
    Command::new("git")
        .arg("--git-dir")
        .arg(repository)
        .args(["cat-file", "-e", &format!("{sha}^{{commit}}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

/// `git rev-parse <rev>` in `repository`.
async fn rev_parse(repository: &Path, rev: &str) -> Result<String, MergeError> {
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(repository)
        .args(["rev-parse", rev])
        .output()
        .await
        .map_err(|error| MergeError::Git {
            operation: format!("rev-parse {rev}"),
            message: error.to_string(),
        })?;
    if !output.status.success() {
        return Err(MergeError::Git {
            operation: format!("rev-parse {rev}"),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Resolve a ref or revision to a commit id, or `None` when it does not
/// resolve (a missing branch is not an error here).
async fn resolve_commit(repository: &Path, rev: &str) -> Option<String> {
    rev_parse(repository, &format!("{rev}^{{commit}}"))
        .await
        .ok()
        .filter(|sha| is_object_id(sha))
}

/// Scrub a remote URL out of a git error message. Git echoes the URL it
/// failed on, and a URL can embed credentials in its userinfo.
fn sanitized_fetch_error(url: &str, message: String) -> String {
    let sanitized = crate::snapshots::sanitize_remote_url(url);
    message.replace(url, &sanitized)
}

/// Origin-scoped auth config for a fetch: the credential rides only to the
/// upstream's own origin over https, and only when that host is `github.com`
/// or the operator's configured forge. A command-wide `http.extraHeader`
/// would also be sent on redirects to hosts the operator never vouched for.
fn auth_header_for_remote(
    url: &str,
    token: &str,
    forge_host: Option<&str>,
) -> Option<(String, String)> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme != "https" {
        return None;
    }
    // Strip userinfo (`https://user:pass@host/...`) before extracting the
    // host; the port separator must not truncate it either.
    let authority = rest.rsplit('@').next().unwrap_or(rest);
    let host = authority.split(['/', ':']).next().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    let allowed = host == "github.com"
        || forge_host.is_some_and(|configured| configured.eq_ignore_ascii_case(host));
    if !allowed {
        return None;
    }
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(format!("x-access-token:{token}").as_bytes());
    Some((
        format!("http.{scheme}://{host}/.extraheader"),
        format!("AUTHORIZATION: basic {encoded}"),
    ))
}

/// Create `mirror` as a bare repository when it does not exist yet.
///
/// Idempotent and tolerant of concurrent deliveries: `git init --bare` on an
/// existing repository re-initializes it harmlessly, and a racing init that
/// loses is retried by the existence check below. Auto-GC is disabled because
/// served per-run repositories alternate to this mirror's objects, so nothing
/// may prune them while a run can still fetch.
async fn ensure_mirror(mirror: &Path) -> Result<(), MergeError> {
    if mirror.join("HEAD").is_file() {
        return Ok(());
    }
    if let Some(parent) = mirror.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| MergeError::Io {
                path: parent.display().to_string(),
                message: error.to_string(),
            })?;
    }
    let output = Command::new("git")
        .args(["init", "--bare", "--quiet", "--template="])
        .arg(mirror)
        .output()
        .await
        .map_err(|error| MergeError::Io {
            path: mirror.display().to_string(),
            message: error.to_string(),
        })?;
    if !output.status.success() && !mirror.join("HEAD").is_file() {
        return Err(MergeError::Io {
            path: mirror.display().to_string(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(mirror)
        .args(["config", "gc.auto", "0"])
        .output()
        .await
        .map_err(|error| MergeError::Io {
            path: mirror.display().to_string(),
            message: error.to_string(),
        })?;
    if !output.status.success() {
        return Err(MergeError::Git {
            operation: "config gc.auto".to_owned(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(())
}

/// Fetch one refspec into `source.mirror` and resolve `resolve_ref` to a sha.
///
/// `refspec` may be a bare sha (fetch-by-sha); `resolve_ref` is then
/// `FETCH_HEAD`.
async fn fetch_into_mirror(
    source: &MergeSource,
    url: &str,
    refspec: &str,
    resolve_ref: &str,
) -> Result<String, MergeError> {
    let mut command = Command::new("git");
    command
        .arg("--git-dir")
        .arg(&source.mirror)
        .args(["fetch", "--quiet", "--no-tags"])
        .arg(url)
        .arg(refspec)
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some((key, value)) = source
        .token
        .as_deref()
        .and_then(|token| auth_header_for_remote(url, token, source.forge_host.as_deref()))
    {
        command
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", key)
            .env("GIT_CONFIG_VALUE_0", value);
    }
    let output = command.output().await.map_err(|error| MergeError::Fetch {
        refspec: refspec.to_owned(),
        url: crate::snapshots::sanitize_remote_url(url),
        message: error.to_string(),
    })?;
    if !output.status.success() {
        let message = sanitized_fetch_error(
            url,
            String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        );
        return Err(MergeError::Fetch {
            refspec: refspec.to_owned(),
            url: crate::snapshots::sanitize_remote_url(url),
            message,
        });
    }
    rev_parse(&source.mirror, resolve_ref).await
}

/// Fetch the current base tip (and the head, when it lives on the remote)
/// into `source.mirror`, resolving both parents.
///
/// The base is always the *current* tip of `refs/heads/<base_branch>` — never
/// a payload's `base.sha`, which is the tip at webhook time and can already be
/// stale.
pub async fn fetch_inputs(
    source: &MergeSource,
    request: &MergeRequest,
) -> Result<MergeParents, MergeError> {
    ensure_mirror(&source.mirror).await?;
    let base_source_ref = format!("refs/heads/{}", request.base_branch);
    let base_local_ref = format!("refs/preloop/merge/base/{}", request.base_branch);
    let base_sha = match &source.fetch_url {
        Some(url) => {
            fetch_into_mirror(
                source,
                url,
                &format!("+{base_source_ref}:{base_local_ref}"),
                &base_local_ref,
            )
            .await
            .map_err(|error| match error {
                // "couldn't find remote ref" is a missing branch, not a
                // transient fetch failure: re-running cannot create it.
                MergeError::Fetch {
                    message,
                    url: sanitized,
                    ..
                } if message.contains("couldn't find remote ref") => {
                    MergeError::BaseBranchMissing {
                        branch: request.base_branch.clone(),
                        url: sanitized,
                    }
                }
                other => other,
            })?
        }
        None => {
            // Local-only source: the mirror's own branch first, then the
            // namespaced ref a previous fetch left behind.
            let mut resolved = None;
            for candidate in [base_source_ref.as_str(), base_local_ref.as_str()] {
                if let Some(sha) = resolve_commit(&source.mirror, candidate).await {
                    resolved = Some(sha);
                    break;
                }
            }
            resolved.ok_or_else(|| MergeError::BaseBranchMissing {
                branch: request.base_branch.clone(),
                url: source.mirror.display().to_string(),
            })?
        }
    };

    let head_sha = match &request.head {
        MergeHead::Commit(sha) => {
            if commit_present(&source.mirror, sha).await {
                sha.clone()
            } else if let Some(url) = &source.fetch_url {
                fetch_into_mirror(source, url, sha, "FETCH_HEAD").await?
            } else {
                return Err(MergeError::MissingCommit {
                    sha: sha.clone(),
                    repository: source.mirror.display().to_string(),
                });
            }
        }
        MergeHead::Branch(name) => {
            let source_ref = format!("refs/heads/{name}");
            let local_ref = format!("refs/preloop/merge/heads/{name}");
            let url = source
                .fetch_url
                .as_deref()
                .ok_or_else(|| MergeError::MissingCommit {
                    sha: source_ref.clone(),
                    repository: source.mirror.display().to_string(),
                })?;
            fetch_into_mirror(
                source,
                url,
                &format!("+{source_ref}:{local_ref}"),
                &local_ref,
            )
            .await?
        }
        MergeHead::PullRef(number) => {
            let source_ref = format!("refs/pull/{number}/head");
            let local_ref = format!("refs/preloop/merge/pull/{number}/head");
            let url = source
                .fetch_url
                .as_deref()
                .ok_or_else(|| MergeError::MissingCommit {
                    sha: source_ref.clone(),
                    repository: source.mirror.display().to_string(),
                })?;
            fetch_into_mirror(
                source,
                url,
                &format!("+{source_ref}:{local_ref}"),
                &local_ref,
            )
            .await?
        }
    };

    for (label, sha) in [("base", &base_sha), ("head", &head_sha)] {
        if !is_object_id(sha) {
            return Err(MergeError::Git {
                operation: "resolve merge parents".to_owned(),
                message: format!("{label} did not resolve to a commit id (`{sha}`)"),
            });
        }
    }
    Ok(MergeParents { base_sha, head_sha })
}

/// Parse `git merge-tree --write-tree --name-only -z` output.
///
/// The first NUL-separated token is the written tree; the following tokens are
/// conflicted paths, terminated by an empty token (git then appends
/// conflict-type records, which the caller ignores).
fn parse_merge_tree_output(stdout: &[u8]) -> Option<(String, Vec<String>)> {
    let mut parts = stdout.split(|byte| *byte == 0);
    let tree = parts.next()?;
    let tree = String::from_utf8_lossy(tree).trim().to_owned();
    if !is_object_id(&tree) {
        return None;
    }
    let mut paths = Vec::new();
    for part in parts {
        if part.is_empty() {
            break;
        }
        paths.push(String::from_utf8_lossy(part).into_owned());
    }
    Some((tree, paths))
}

/// Build the merge commit inside `served` from commits visible to it.
///
/// `served` must see both parents' objects — either it is the mirror itself,
/// or [`link_mirror`] pointed it at the mirror. The merge commit is written
/// into `served`; a conflict writes nothing.
pub async fn merge_in(served: &Path, parents: &MergeParents) -> Result<MergeOutcome, MergeError> {
    for sha in [&parents.base_sha, &parents.head_sha] {
        if !commit_present(served, sha).await {
            return Err(MergeError::MissingCommit {
                sha: sha.clone(),
                repository: served.display().to_string(),
            });
        }
    }

    let output = Command::new("git")
        .arg("--git-dir")
        .arg(served)
        .args(["merge-tree", "--write-tree", "--name-only", "-z"])
        .arg(&parents.base_sha)
        .arg(&parents.head_sha)
        .output()
        .await
        .map_err(|error| MergeError::Git {
            operation: "merge-tree".to_owned(),
            message: error.to_string(),
        })?;
    let parsed = parse_merge_tree_output(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    match (output.status.success(), output.status.code(), parsed) {
        // Clean merge: one tree, no conflicts.
        (true, _, Some((tree, _))) => return finish_merge(served, tree, parents).await,
        // A conflict exits 1 with a real tree and the conflicted paths.
        (false, Some(1), Some((_, paths))) if !paths.is_empty() => {
            let mut paths = paths;
            paths.sort();
            paths.dedup();
            Ok(MergeOutcome::Conflict {
                base_sha: parents.base_sha.clone(),
                head_sha: parents.head_sha.clone(),
                paths,
            })
        }
        // Anything else is a genuine git failure: a bad revision exits 1 with
        // no tree on stdout and the reason on stderr.
        _ => Err(MergeError::Git {
            operation: "merge-tree".to_owned(),
            message: if stderr.is_empty() {
                "no output".to_owned()
            } else {
                stderr
            },
        }),
    }
}

/// Commit a merged tree with both parents and the fixed preloop identity.
async fn finish_merge(
    served: &Path,
    tree: String,
    parents: &MergeParents,
) -> Result<MergeOutcome, MergeError> {
    let mut command = Command::new("git");
    command
        .arg("--git-dir")
        .arg(served)
        .args(["commit-tree", &tree])
        .args(["-p", &parents.base_sha, "-p", &parents.head_sha])
        .args([
            "-m",
            &format!(
                "preloop test merge of {} into {}",
                parents.head_sha, parents.base_sha
            ),
        ])
        .env("GIT_AUTHOR_NAME", MERGE_IDENTITY_NAME)
        .env("GIT_AUTHOR_EMAIL", MERGE_IDENTITY_EMAIL)
        .env("GIT_AUTHOR_DATE", MERGE_IDENTITY_DATE)
        .env("GIT_COMMITTER_NAME", MERGE_IDENTITY_NAME)
        .env("GIT_COMMITTER_EMAIL", MERGE_IDENTITY_EMAIL)
        .env("GIT_COMMITTER_DATE", MERGE_IDENTITY_DATE);
    let output = command.output().await.map_err(|error| MergeError::Git {
        operation: "commit-tree".to_owned(),
        message: error.to_string(),
    })?;
    if !output.status.success() {
        return Err(MergeError::Git {
            operation: "commit-tree".to_owned(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !is_object_id(&sha) {
        return Err(MergeError::Git {
            operation: "commit-tree".to_owned(),
            message: format!("git returned invalid commit id `{sha}`"),
        });
    }
    Ok(MergeOutcome::Merged(MergedMerge {
        sha,
        tree,
        base_sha: parents.base_sha.clone(),
        head_sha: parents.head_sha.clone(),
    }))
}

/// Point `served` at `mirror`'s object store through `objects/info/alternates`.
///
/// The merge commit and its tree live in `served`; its parents' objects (the
/// base tip's history) stay in the mirror, exactly like the local snapshot's
/// alternate to the shared object cache. Idempotent, and a no-op when both
/// paths are the same repository.
pub async fn link_mirror(served: &Path, mirror: &Path) -> Result<(), MergeError> {
    let served = tokio::fs::canonicalize(served)
        .await
        .map_err(|error| MergeError::Io {
            path: served.display().to_string(),
            message: error.to_string(),
        })?;
    let mirror = tokio::fs::canonicalize(mirror)
        .await
        .map_err(|error| MergeError::Io {
            path: mirror.display().to_string(),
            message: error.to_string(),
        })?;
    if served == mirror {
        return Ok(());
    }
    let objects = mirror.join("objects");
    let alternates_path = served.join("objects").join("info").join("alternates");
    let mut alternates: Vec<String> = match tokio::fs::read_to_string(&alternates_path).await {
        Ok(existing) => existing
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            return Err(MergeError::Io {
                path: alternates_path.display().to_string(),
                message: error.to_string(),
            });
        }
    };
    let objects_text = objects.to_string_lossy().into_owned();
    let already_present = alternates.iter().any(|line| {
        line == &objects_text
            || std::fs::canonicalize(line).is_ok_and(|canonical| canonical == objects)
    });
    if !already_present {
        alternates.push(objects_text);
    }
    if let Some(parent) = alternates_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| MergeError::Io {
                path: parent.display().to_string(),
                message: error.to_string(),
            })?;
    }
    let mut body = alternates.join("\n");
    body.push('\n');
    tokio::fs::write(&alternates_path, body)
        .await
        .map_err(|error| MergeError::Io {
            path: alternates_path.display().to_string(),
            message: error.to_string(),
        })
}

/// Publish the merge in `served` so jobs can fetch it: `refs/heads/snapshot`
/// (plus `refs/pull/<n>/merge` when the number is known), HEAD, and the
/// upload-pack permissions a shallow single-commit fetch needs.
pub async fn publish_merge(
    served: &Path,
    merge: &MergedMerge,
    pull_request_number: Option<u64>,
) -> Result<(), MergeError> {
    let mut refs: Vec<(String, String)> =
        vec![("refs/heads/snapshot".to_owned(), merge.sha.clone())];
    if let Some(number) = pull_request_number {
        refs.push((format!("refs/pull/{number}/merge"), merge.sha.clone()));
    }
    for (name, target) in refs {
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(served)
            .args(["update-ref", &name, &target])
            .output()
            .await
            .map_err(|error| MergeError::Git {
                operation: format!("update-ref {name}"),
                message: error.to_string(),
            })?;
        if !output.status.success() {
            return Err(MergeError::Git {
                operation: format!("update-ref {name}"),
                message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
    }
    for (operation, args) in [
        (
            "symbolic-ref HEAD",
            vec!["symbolic-ref", "HEAD", "refs/heads/snapshot"],
        ),
        (
            "config uploadpack.allowReachableSHA1InWant",
            vec!["config", "uploadpack.allowReachableSHA1InWant", "true"],
        ),
        (
            "config uploadpack.allowTipSHA1InWant",
            vec!["config", "uploadpack.allowTipSHA1InWant", "true"],
        ),
        (
            "config uploadpack.allowFilter",
            vec!["config", "uploadpack.allowFilter", "true"],
        ),
    ] {
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(served)
            .args(&args)
            .output()
            .await
            .map_err(|error| MergeError::Git {
                operation: operation.to_owned(),
                message: error.to_string(),
            })?;
        if !output.status.success() {
            return Err(MergeError::Git {
                operation: operation.to_owned(),
                message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
    }
    Ok(())
}

/// Fetch the merge inputs into `source.mirror`, build the merge inside
/// `served`, and publish it there.
///
/// `served` must not be the mirror unless the caller wants the merge written
/// into the mirror; when they differ, `served` is linked to the mirror's
/// objects first.
pub async fn build_merge(
    source: &MergeSource,
    request: &MergeRequest,
    served: &Path,
) -> Result<MergeOutcome, MergeError> {
    let parents = fetch_inputs(source, request).await?;
    let distinct = !same_repository(served, &source.mirror).await;
    if distinct {
        link_mirror(served, &source.mirror).await?;
    }
    let outcome = merge_in(served, &parents).await?;
    // When the merge is built straight into the mirror it stays unreferenced
    // until a run's served repository publishes it: the mirror is shared and
    // must not advertise a `snapshot` ref of its own.
    if distinct && let MergeOutcome::Merged(merge) = &outcome {
        publish_merge(served, merge, request.pull_request_number).await?;
    }
    Ok(outcome)
}

/// Whether two repository paths name the same directory.
async fn same_repository(left: &Path, right: &Path) -> bool {
    match (
        tokio::fs::canonicalize(left).await,
        tokio::fs::canonicalize(right).await,
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// Prepare the engine mirror a webhook delivery's merge is built in, together
/// with the forge coordinates to fetch from.
///
/// Mirrors are keyed per repository under the checkout cache's repositories
/// root, so the existing retention sweep bounds them. A run serving a merge
/// from a mirror publishes `refs/preloop/runs/<run_id>` there (see
/// [`attach_prebuilt_merge`]) and the release path deletes it, which keeps the
/// size sweep from evicting a mirror a live run still reads through its
/// alternate.
pub async fn webhook_merge_source(
    shared: &SharedState,
    repository: &str,
) -> Result<MergeSource, MergeError> {
    use sha2::Digest;

    let key = format!("{:x}", sha2::Sha256::digest(repository.as_bytes()));
    let mirror = shared
        .state
        .state_dir
        .join("checkout-cache")
        .join("repositories")
        .join(format!("merge-{key}.git"));
    ensure_mirror(&mirror).await?;
    let server_url = shared.state.github_urls.server_url.trim_end_matches('/');
    let forge_host = reqwest::Url::parse(server_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned));
    Ok(MergeSource {
        mirror,
        fetch_url: Some(format!("{server_url}/{repository}.git")),
        token: crate::snapshots::forge_read_token(shared, repository).await,
        forge_host,
    })
}

/// Build the submission's `prebuilt_merge` record for a merge that was built
/// in `mirror`.
///
/// The record is what a run's `submit_run_inner` needs to serve the merge
/// from the engine: the commit, its tree and parents, and the mirror's
/// state-directory-relative path. `None` when `mirror` is not under the
/// state directory (nothing could serve it).
pub fn prebuilt_merge_record(
    shared: &SharedState,
    mirror: &Path,
    merge: &MergedMerge,
    pull_request_number: Option<u64>,
) -> Option<preloop_gha_protocol::PrebuiltMerge> {
    let relative = mirror.strip_prefix(&shared.state.state_dir).ok()?;
    Some(preloop_gha_protocol::PrebuiltMerge {
        sha: merge.sha.clone(),
        tree: merge.tree.clone(),
        base_sha: merge.base_sha.clone(),
        head_sha: merge.head_sha.clone(),
        mirror_repository: relative.to_string_lossy().to_string(),
        pull_request_number,
    })
}

/// Create the run's engine-served repository (`snapshots/<run_id>`), which
/// `snapshot_git_http` serves to the run's jobs. Fails when it already exists.
async fn create_served_repository(state_dir: &Path, run_id: RunId) -> Result<PathBuf, MergeError> {
    let served = state_dir.join("snapshots").join(run_id.to_string());
    if served.exists() {
        return Err(MergeError::Io {
            path: served.display().to_string(),
            message: "served snapshot repository already exists".to_owned(),
        });
    }
    if let Some(parent) = served.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| MergeError::Io {
                path: parent.display().to_string(),
                message: error.to_string(),
            })?;
    }
    let output = Command::new("git")
        .args(["init", "--bare", "--quiet", "--template="])
        .arg(&served)
        .output()
        .await
        .map_err(|error| MergeError::Io {
            path: served.display().to_string(),
            message: error.to_string(),
        })?;
    if !output.status.success() {
        return Err(MergeError::Io {
            path: served.display().to_string(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(served)
}

/// Publish this run's ownership ref in a prunable mirror and return the
/// mirror's state-directory-relative path.
///
/// Mirrors under the checkout cache are evicted by idle age and by the size
/// sweep; the sweep treats a repository holding any `refs/preloop/runs/*` as
/// live, so the ref keeps a mirror alive while a run's served repository can
/// still fetch through its alternate. `release_remote_checkout_snapshot`
/// deletes the ref when the run terminalizes. `None` means the mirror is not
/// prunable (the workspace's object cache), so no ref is needed.
async fn protect_mirror_for_run(
    shared: &SharedState,
    run_id: RunId,
    mirror: &Path,
    merge_sha: &str,
) -> Option<String> {
    let state_dir = tokio::fs::canonicalize(&shared.state.state_dir)
        .await
        .ok()?;
    let mirror = tokio::fs::canonicalize(mirror).await.ok()?;
    let relative = mirror.strip_prefix(&state_dir).ok()?;
    if !mirror.starts_with(state_dir.join("checkout-cache")) {
        return None;
    }
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(&mirror)
        .args([
            "update-ref",
            &format!("refs/preloop/runs/{run_id}"),
            merge_sha,
        ])
        .output()
        .await;
    match output {
        Ok(output) if output.status.success() => Some(relative.to_string_lossy().to_string()),
        Ok(output) => {
            warn!(
                %run_id,
                path = %mirror.display(),
                error = %String::from_utf8_lossy(&output.stderr).trim(),
                "failed to publish the run's merge-mirror ownership ref; the mirror may be evicted early"
            );
            None
        }
        Err(error) => {
            warn!(
                %run_id,
                path = %mirror.display(),
                %error,
                "failed to publish the run's merge-mirror ownership ref; the mirror may be evicted early"
            );
            None
        }
    }
}

/// Attach a built merge to a run's workspace snapshot.
///
/// The head snapshot is carried forward (its tree stays the tree of the head
/// the run's change set is measured against) with `commit_sha` moved to the
/// merge — the commit the run actually checks out — and `merge` recorded for
/// payload labelling. A local snapshot's repository already serves the run; a
/// remote or absent one gets a fresh engine-served repository, so the merge is
/// fetchable even when the checkout cache is off.
pub async fn attach_merge(
    shared: &SharedState,
    run_id: RunId,
    mirror: &Path,
    merge: &MergedMerge,
    head_snapshot: Option<WorkspaceSnapshot>,
) -> Result<WorkspaceSnapshot, MergeError> {
    let state_dir = &shared.state.state_dir;
    let (served, mut snapshot) = match head_snapshot {
        Some(head) if head.source == SnapshotSource::LocalWorkspace => {
            let served = state_dir.join(&head.repository);
            (served, head)
        }
        Some(head) => (create_served_repository(state_dir, run_id).await?, head),
        None => {
            let served = create_served_repository(state_dir, run_id).await?;
            let snapshot = WorkspaceSnapshot {
                commit_sha: merge.sha.clone(),
                tree_sha: merge.tree.clone(),
                head_sha: Some(merge.head_sha.clone()),
                repository: format!("snapshots/{run_id}"),
                default_branch: None,
                before_sha: None,
                snapshot_timing: None,
                storage_repository: None,
                source: SnapshotSource::SelfBuiltMerge,
                cache_namespace: None,
                upstream_repository: None,
                upstream_repository_id: None,
                upstream_private: None,
                merge: None,
            };
            (served, snapshot)
        }
    };
    link_mirror(&served, mirror).await?;
    publish_merge(&served, merge, None).await?;
    let mirror_repository = protect_mirror_for_run(shared, run_id, mirror, &merge.sha).await;
    snapshot.commit_sha = merge.sha.clone();
    snapshot.merge = Some(SnapshotMerge {
        sha: merge.sha.clone(),
        base_sha: merge.base_sha.clone(),
        head_sha: merge.head_sha.clone(),
        mirror_repository,
    });
    if snapshot.source != SnapshotSource::LocalWorkspace {
        snapshot.source = SnapshotSource::SelfBuiltMerge;
        snapshot.storage_repository = None;
    }
    Ok(snapshot)
}

/// Validate a `WorkflowSubmission::prebuilt_merge` and turn it into the run's
/// served snapshot.
///
/// Nothing in the submission is trusted: the mirror path must be a plain
/// state-directory-relative path, the merge commit must exist there with
/// exactly the claimed tree and parents, and the served repository is created
/// under `snapshots/<run_id>` and linked to the mirror.
pub async fn attach_prebuilt_merge(
    shared: &SharedState,
    run_id: RunId,
    prebuilt: &preloop_gha_protocol::PrebuiltMerge,
) -> Result<WorkspaceSnapshot, MergeError> {
    let state_dir = tokio::fs::canonicalize(&shared.state.state_dir)
        .await
        .map_err(|error| MergeError::Io {
            path: shared.state.state_dir.display().to_string(),
            message: error.to_string(),
        })?;
    let relative = Path::new(&prebuilt.mirror_repository);
    let plain_relative = !relative.is_absolute()
        && !prebuilt.mirror_repository.is_empty()
        && relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !plain_relative {
        return Err(MergeError::InvalidPrebuilt(format!(
            "mirror repository `{}` is not a plain state-relative path",
            prebuilt.mirror_repository
        )));
    }
    let mirror = tokio::fs::canonicalize(state_dir.join(relative))
        .await
        .map_err(|error| {
            MergeError::InvalidPrebuilt(format!(
                "mirror repository `{}` is not readable: {error}",
                prebuilt.mirror_repository
            ))
        })?;
    if !mirror.starts_with(&state_dir) {
        return Err(MergeError::InvalidPrebuilt(format!(
            "mirror repository `{}` escapes the state directory",
            prebuilt.mirror_repository
        )));
    }
    if !commit_present(&mirror, &prebuilt.sha).await {
        return Err(MergeError::InvalidPrebuilt(format!(
            "merge commit {} is not present in `{}`",
            prebuilt.sha, prebuilt.mirror_repository
        )));
    }
    let parents = rev_parse(&mirror, &format!("{}^@", prebuilt.sha))
        .await
        .map_err(|_| {
            MergeError::InvalidPrebuilt(format!("merge commit {} has no parents", prebuilt.sha))
        })?;
    let parents: Vec<&str> = parents.split_whitespace().collect();
    if parents.len() != 2
        || !parents[0].eq_ignore_ascii_case(&prebuilt.base_sha)
        || !parents[1].eq_ignore_ascii_case(&prebuilt.head_sha)
    {
        return Err(MergeError::InvalidPrebuilt(format!(
            "merge commit {} does not have parents [{}, {}]",
            prebuilt.sha, prebuilt.base_sha, prebuilt.head_sha
        )));
    }
    let tree = rev_parse(&mirror, &format!("{}^{{tree}}", prebuilt.sha)).await?;
    if !tree.eq_ignore_ascii_case(&prebuilt.tree) {
        return Err(MergeError::InvalidPrebuilt(format!(
            "merge commit {} has tree {tree}, not {}",
            prebuilt.sha, prebuilt.tree
        )));
    }

    let merge = MergedMerge {
        sha: prebuilt.sha.clone(),
        tree: tree.clone(),
        base_sha: prebuilt.base_sha.clone(),
        head_sha: prebuilt.head_sha.clone(),
    };
    let served = create_served_repository(&state_dir, run_id).await?;
    link_mirror(&served, &mirror).await?;
    publish_merge(&served, &merge, prebuilt.pull_request_number).await?;
    let mirror_repository = protect_mirror_for_run(shared, run_id, &mirror, &merge.sha).await;
    Ok(WorkspaceSnapshot {
        commit_sha: prebuilt.sha.clone(),
        tree_sha: tree,
        head_sha: Some(prebuilt.head_sha.clone()),
        repository: format!("snapshots/{run_id}"),
        default_branch: None,
        before_sha: None,
        snapshot_timing: None,
        storage_repository: None,
        source: SnapshotSource::SelfBuiltMerge,
        cache_namespace: None,
        upstream_repository: None,
        upstream_repository_id: None,
        upstream_private: None,
        merge: Some(SnapshotMerge {
            sha: prebuilt.sha.clone(),
            base_sha: prebuilt.base_sha.clone(),
            head_sha: prebuilt.head_sha.clone(),
            mirror_repository,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(bytes: &[u8]) -> Option<(String, Vec<String>)> {
        parse_merge_tree_output(bytes)
    }

    #[test]
    fn parses_a_clean_merge_tree() {
        let tree = "a".repeat(40);
        let output = format!("{tree}\0");
        let (parsed_tree, paths) = parse(output.as_bytes()).expect("clean output parses");
        assert_eq!(parsed_tree, tree);
        assert!(paths.is_empty());
    }

    #[test]
    fn parses_conflicted_paths_and_stops_at_the_separator() {
        let tree = "b".repeat(40);
        let mut output = Vec::new();
        output.extend_from_slice(tree.as_bytes());
        output.push(0);
        output.extend_from_slice(b"src/a.rs\0src/b.rs\0");
        // Conflict-type records follow an empty token; they are not paths.
        output.extend_from_slice(b"\x001\0src/a.rs\0CONFLICT (content)\0");
        let (parsed_tree, paths) = parse(&output).expect("conflict output parses");
        assert_eq!(parsed_tree, tree);
        assert_eq!(paths, vec!["src/a.rs", "src/b.rs"]);
    }

    #[test]
    fn rejects_output_that_does_not_start_with_a_tree() {
        assert!(parse(b"merge-tree: not something we can merge\n").is_none());
        assert!(parse(b"").is_none());
    }
}
