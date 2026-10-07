//! Immutable checkout snapshots exposed as Git repositories.
//!
//! Local submissions capture a synthetic commit. Opt-in remote checkout
//! caching fetches a webhook run's immutable commit once, then exposes the
//! resulting bare repository over Git smart HTTP so each job still receives a
//! private writable checkout through the unmodified `actions/checkout`.

use super::*;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, Request};
use axum::http::{HeaderName, HeaderValue, Response, StatusCode, header};
use base64::Engine;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_util::io::ReaderStream;

const SNAPSHOT_REF: &str = "refs/heads/snapshot";
const MAX_GIT_REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// TTL for cached GitHub repository metadata (numeric ID + visibility).
/// Repo metadata changes rarely; five minutes bounds staleness for renames
/// while keeping repeated local submissions off the network.
const REPO_META_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Shorter TTL for failed lookups so an offline or rate-limited forge does
/// not stall every run creation on the full request timeout.
const REPO_META_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Storage lifetime for one snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotSource {
    #[default]
    LocalWorkspace,
    RemoteRunScoped,
    RemoteRepository,
}

/// Security namespace for remote Git objects. `tenant_id` is deliberately
/// present before first-class tenancy: adding it later creates a cold namespace
/// rather than sharing pre-tenant objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckoutCacheNamespace {
    pub tenant_id: Option<String>,
    pub provider_origin: String,
    pub credential_domain: String,
    pub repository_id: String,
}

/// The checkout coordinates for one immutable workspace snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSnapshot {
    pub commit_sha: String,
    /// Tree of the snapshot commit — the exact tree the run tests. A
    /// push-back client materializes a real commit from this tree so the
    /// pushed commit is byte-identical to what CI validated.
    pub tree_sha: String,
    /// The workspace's real HEAD commit (the commit the submission is based
    /// on), when the workspace has one. This is the identity a workflow sees
    /// as `github.sha`: it is what a custom checkout that fetches from the
    /// real remote can actually resolve. The synthetic [`Self::commit_sha`]
    /// exists only in this engine's snapshot store and would be rejected as
    /// `not our ref` by the upstream host.
    pub head_sha: Option<String>,
    pub repository: String,
    /// Current branch of the source workspace (`master`, `main`, …), when
    /// resolvable. Mirrored into the event payload as
    /// `repository.default_branch` so changed-file actions can pick a base.
    pub default_branch: Option<String>,
    /// Base commit the synthetic push measures against, mirrored into push
    /// event payloads as `before`. When the working tree carries uncommitted
    /// edits this is the workspace `HEAD` (so `before..after` is exactly the
    /// local delta); when the tree is clean it is `HEAD^` (so the range still
    /// covers the last commit the user wants tested). `None` on an unborn or
    /// initial-commit clean tree yields the null-SHA "initial push" base.
    pub before_sha: Option<String>,
    /// Server-side cost of capturing this snapshot; present on snapshots
    /// created after the timing instrumentation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_timing: Option<crate::models::SnapshotTiming>,
    /// Physical bare repository relative to the state directory. Older local
    /// snapshots derive this from `repository`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_repository: Option<String>,
    #[serde(default)]
    pub source: SnapshotSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_namespace: Option<CheckoutCacheNamespace>,
    /// Forge coordinates this snapshot was fetched from (`owner/repo`).
    /// For remote snapshots this is the cloned repository; for local
    /// snapshots it is the workspace's `origin` remote when it points at
    /// the configured forge host. Lets later requests (LFS batch) find the
    /// same upstream without the original submission. Absent on snapshots
    /// with no detectable forge upstream, which never fetch on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_repository: Option<String>,
    /// Stable numeric forge repository ID, resolved at snapshot creation.
    /// Unlike `upstream_repository`, this survives renames and owner
    /// transfers. Best-effort: absent when resolution failed or no forge
    /// coordinate was detected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_repository_id: Option<u64>,
    /// Whether that upstream is private. Unknown (legacy) reads as private so
    /// on-demand fetching fails closed instead of trying anonymous access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_private: Option<bool>,
}

/// Parse a GitHub `owner/repo` slug from a git remote URL.
///
/// `github_host` is the configured forge host (`github.com`, or a GHES
/// hostname from `PRELOOP_GITHUB_SERVER_URL`). Handles the common remote
/// forms:
/// - `https://<host>/owner/repo.git` (and without `.git`)
/// - `git@<host>:owner/repo.git` (and without `.git`)
/// - `ssh://git@<host>/owner/repo.git`
///
/// Returns `None` for other hosts, malformed URLs, or paths that are not
/// exactly `owner/repo`. Case is preserved; GitHub slugs are
/// case-insensitive but the API canonicalizes them.
fn parse_github_remote_slug(remote_url: &str, github_host: &str) -> Option<String> {
    let remote_url = remote_url.trim();
    let ssh_scp = format!("git@{github_host}:");
    let ssh_url = format!("ssh://git@{github_host}/");
    let https = format!("https://{github_host}/");
    let http = format!("http://{github_host}/");
    let path = remote_url
        .strip_prefix(&ssh_scp)
        .or_else(|| remote_url.strip_prefix(&ssh_url))
        .or_else(|| {
            remote_url
                .strip_prefix(&https)
                .or_else(|| remote_url.strip_prefix(&http))
        })?;
    let path = path.strip_suffix(".git").unwrap_or(path);
    let path = path.trim_matches('/');
    let mut parts = path.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if parts.next().is_some() || owner.is_empty() || repo.is_empty() {
        return None;
    }
    // Reject paths with characters that cannot appear in a GitHub slug.
    let valid = |s: &str| {
        s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    };
    if !valid(owner) || !valid(repo) {
        return None;
    }
    Some(format!("{owner}/{repo}"))
}

/// Resolve a GitHub `owner/repo` slug to its stable numeric repository ID and
/// visibility.
///
/// Best-effort: returns `(None, None)` on any failure (network, auth, rate
/// limit, renamed repo). The ID survives renames and owner transfers that
/// would stale the slug. Prefers a GitHub App installation token when the
/// server has one for the repository, then the provided PAT, then anonymous
/// access (which works for public repositories). Results are cached on the
/// engine state so repeated local submissions do not each pay the API
/// latency — including a short negative TTL so an offline forge does not
/// stall every run creation.
async fn resolve_github_repo_meta(
    shared: Option<&SharedState>,
    slug: &str,
    github_pat: Option<&str>,
) -> (Option<u64>, Option<bool>) {
    // The cache key includes the credential fingerprint: a PAT-scoped
    // answer (e.g. private visibility) must not leak into a later anonymous
    // lookup for the same slug.
    let pat_fp = github_pat.map(|pat| {
        use sha2::Digest;
        format!("{:x}", sha2::Sha256::digest(pat.as_bytes()))
    });
    let cache_key = (slug.to_owned(), pat_fp);
    if let Some(shared) = shared
        && let Ok(cache) = shared.state.repo_meta_cache.lock()
        && let Some((meta, at)) = cache.get(&cache_key)
    {
        let ttl = if meta.0.is_some() || meta.1.is_some() {
            REPO_META_CACHE_TTL
        } else {
            REPO_META_NEGATIVE_TTL
        };
        if at.elapsed() < ttl {
            return *meta;
        }
    }
    // Prefer the App registry: a private repository configured App-only has
    // no PAT, and the installation token is the only credential that can
    // read its metadata.
    let mut app_token: Option<String> = None;
    let mut api_url = "https://api.github.com".to_owned();
    if let Some(shared) = shared {
        api_url = shared
            .state
            .github_urls
            .api_url
            .trim_end_matches('/')
            .to_owned();
        if let Some(app) = crate::github_app::select_app_for_repo(shared, slug).await {
            let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
            app_token = crate::github_app::get_or_mint_token(&app, slug, &permissions)
                .await
                .ok();
        }
    }
    let mut request = crate::shared_http::CLIENT
        .get(format!("{api_url}/repos/{slug}"))
        .header("User-Agent", "preloop-runner-server")
        .header("Accept", "application/vnd.github+json")
        .timeout(std::time::Duration::from_secs(10));
    if let Some(token) = app_token.as_deref().or(github_pat) {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => return (None, None),
    };
    if !response.status().is_success() {
        return (None, None);
    }
    let body: serde_json::Value = match response.json().await {
        Ok(body) => body,
        Err(_) => return (None, None),
    };
    let id = body
        .get("id")
        .and_then(|id| id.as_u64())
        .filter(|id| *id > 0);
    let private = body.get("private").and_then(|private| private.as_bool());
    let meta = (id, private);
    if let Some(shared) = shared
        && let Ok(mut cache) = shared.state.repo_meta_cache.lock()
    {
        cache.retain(|_, (_, at)| at.elapsed() < REPO_META_CACHE_TTL);
        cache.insert(cache_key, (meta, std::time::Instant::now()));
    }
    meta
}

/// Pick the GitHub `owner/repo` slug from `git remote -v` output.
///
/// `github_host` is the configured forge host (`github.com`, or a GHES
/// hostname). `origin` wins when it points at the forge; otherwise the
/// first remote in configuration order that parses as a forge URL is used
/// — a workspace whose GitHub remote is named `private` or `upstream`
/// still gets on-demand LFS fetching. Only fetch URLs are considered: a
/// push-only mirror is not the upstream the objects came from.
fn github_remote_slug(remote_v: &str, github_host: &str) -> Option<String> {
    let mut first_github: Option<String> = None;
    for line in remote_v.lines() {
        let mut fields = line.split_whitespace();
        let (Some(name), Some(url)) = (fields.next(), fields.next()) else {
            continue;
        };
        // `git remote -v` appends "(fetch)"/"(push)"; accept a bare URL too
        // so the parser stays useful on trimmed input.
        if let Some(marker) = fields.next()
            && marker != "(fetch)"
        {
            continue;
        }
        let Some(slug) = parse_github_remote_slug(url, github_host) else {
            continue;
        };
        if name == "origin" {
            return Some(slug);
        }
        if first_github.is_none() {
            first_github = Some(slug);
        }
    }
    first_github
}

/// Host (with port) of the configured forge server URL, lowercased.
/// `github.com` when the URL cannot be parsed — the parser then simply
/// matches nothing rather than guessing a host.
fn configured_github_host(shared: Option<&SharedState>) -> String {
    shared
        .and_then(|shared| url_download_host(&shared.state.github_urls.server_url))
        .unwrap_or_else(|| "github.com".to_owned())
}

/// Detect the workspace's GitHub upstream from its remotes.
///
/// Returns `(slug, repository_id, private)`: the `owner/repo` slug parsed
/// from the remote URL, the stable numeric repository ID, and the
/// repository's visibility, resolved via the forge API. Each element may
/// be `None` independently — a slug without an ID still enables on-demand
/// LFS fetching; unknown visibility is treated as public so an anonymous
/// fetch is attempted (a private repo simply 404s into the same fallback).
/// Returns `None` when the workspace has no remote pointing at the
/// configured forge host.
async fn detect_workspace_upstream(
    workspace: &FsPath,
    shared: Option<&SharedState>,
    github_pat: Option<&str>,
) -> Option<(String, Option<u64>, Option<bool>)> {
    let output = tokio::process::Command::new("git")
        .args(["remote", "-v"])
        .current_dir(workspace)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let remote_v = String::from_utf8_lossy(&output.stdout);
    let slug = github_remote_slug(&remote_v, &configured_github_host(shared))?;
    let (repository_id, private) = resolve_github_repo_meta(shared, &slug, github_pat).await;
    Some((slug, repository_id, private))
}

/// Capture `workspace` as an immutable cache-backed bare repository for `run_id`.
///
/// A private index and a temporary bare object database keep the user's index,
/// refs, and working tree untouched. Committed objects are incrementally fetched
/// into a state-directory cache; each run stores only its synthetic dirty-tree
/// objects and references that private cache as an alternate. No snapshot keeps
/// a path to the user's source repository.
pub async fn create_workspace_snapshot(
    state_dir: &FsPath,
    workspace: &FsPath,
    run_id: RunId,
    shared: Option<&SharedState>,
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

    // Detect the workspace's GitHub upstream concurrently with snapshot
    // creation. Best-effort: a missing remote or unresolvable slug leaves
    // the snapshot without upstream coordinates and LFS on-demand fetching
    // stays disabled, as before.
    let (result, upstream) = tokio::join!(
        create_workspace_snapshot_inner(
            state_dir,
            &workspace,
            &staging_repository,
            &staging_index,
            &final_repository,
            run_id,
            github_pat,
        ),
        detect_workspace_upstream(&workspace, shared, github_pat),
    );
    if let Err(error) = tokio::fs::remove_dir_all(&staging_root).await
        && staging_root.exists()
    {
        warn!(
            path = %staging_root.display(),
            %error,
            "Failed to remove snapshot staging directory"
        );
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
        storage_repository: None,
        source: SnapshotSource::LocalWorkspace,
        cache_namespace: None,
        upstream_repository: upstream.as_ref().map(|(slug, _, _)| slug.clone()),
        upstream_repository_id: upstream.as_ref().and_then(|(_, id, _)| *id),
        upstream_private: upstream.as_ref().and_then(|(_, _, private)| *private),
    })
}

/// Forge credential for one cacheable repository. The token crosses only the
/// engine-to-forge leg; jobs never see it. `None` is an anonymous public
/// fetch, not a missing credential.
struct CacheUpstreamCredential {
    upstream_token: Option<String>,
    credential_domain: String,
}

/// Select the forge credential the control plane uses on a repository's
/// behalf: a contents-read App installation token first, then the static PAT,
/// then anonymous access for public repositories. `None` means unprovable
/// (private with no usable credential) and the caller falls back to direct
/// forge checkout instead of failing the run.
async fn resolve_cache_upstream_credential(
    shared: &SharedState,
    repository: &str,
    repository_private: bool,
) -> Result<Option<CacheUpstreamCredential>, ApiError> {
    use sha2::Digest;

    if let Some(app) = crate::github_app::select_app_for_repo(shared, repository).await {
        let permissions = BTreeMap::from([("contents".to_owned(), "read".to_owned())]);
        let token = crate::github_app::get_or_mint_token(&app, repository, &permissions)
            .await
            .map_err(|error| {
                ApiError::internal(format!("failed to mint checkout-cache token: {error:#}"))
            })?;
        let Some(entry) = app.mint_ledger.lookup(&token) else {
            return Ok(None);
        };
        return Ok(Some(CacheUpstreamCredential {
            upstream_token: Some(token),
            credential_domain: format!("app-{}-installation-{}", app.app_id, entry.installation_id),
        }));
    }
    if let Some(token) = shared.state.static_github_pat() {
        let fingerprint = format!("{:x}", sha2::Sha256::digest(token.as_bytes()));
        return Ok(Some(CacheUpstreamCredential {
            upstream_token: Some(token),
            credential_domain: format!("pat-{fingerprint}"),
        }));
    }
    if !repository_private {
        return Ok(Some(CacheUpstreamCredential {
            upstream_token: None,
            credential_domain: "public".to_owned(),
        }));
    }
    Ok(None)
}

/// Cap for one on-demand LFS blob. Blobs stream straight to disk, so this
/// bounds a runaway response rather than RAM: anything larger is treated as
/// uncacheable and the job falls back to the forge.
const MAX_LFS_OBJECT_BYTES: u64 = 1024 * 1024 * 1024;

/// Forge coordinates for on-demand LFS fetching, resolved from the run's own
/// remote snapshot. Only snapshots carrying an upstream repository fetch;
/// local and legacy snapshots keep the previous miss-is-404 behavior.
struct LfsFetch<'a> {
    shared: &'a SharedState,
    /// Absolute path of the cache repository the blob is stored into, so the
    /// object shares the git objects' per-run lifecycle.
    repository_dir: &'a FsPath,
    /// `owner/repo` on the forge, as recorded at snapshot creation. May be
    /// stale after a rename or owner transfer; see `upstream_repository_id`.
    upstream_repository: &'a str,
    /// Stable numeric forge repository ID. When present, the current
    /// canonical slug is resolved from it before any forge call, so renames
    /// and transfers do not break fetching for existing snapshots.
    upstream_repository_id: Option<u64>,
    upstream_private: bool,
}

/// Resolve the current canonical `owner/repo` slug for an on-demand fetch.
///
/// The stored slug goes stale when a repository is renamed or transferred;
/// the numeric ID does not. When an ID is present, ask the forge for the
/// repository's current `full_name` and use that for credential selection
/// and the LFS batch URL. Any failure (no ID, unresolvable, renamed-away)
/// falls back to the stored slug — a stale slug that still resolves is
/// better than no fetch at all.
async fn resolve_canonical_upstream_slug(
    shared: &SharedState,
    stored_slug: &str,
    repository_id: Option<u64>,
    repository_private: bool,
) -> String {
    let Some(id) = repository_id else {
        return stored_slug.to_owned();
    };
    // The stored slug may be stale, so the App lookup inside may miss; the
    // PAT and anonymous fallbacks still let us reach the ID endpoint.
    let credential: Option<CacheUpstreamCredential> =
        resolve_cache_upstream_credential(shared, stored_slug, repository_private)
            .await
            .unwrap_or_default();
    let Some(credential) = credential else {
        return stored_slug.to_owned();
    };
    let url = format!(
        "{}/repositories/{id}",
        shared.state.github_urls.server_url.trim_end_matches('/'),
    );
    let mut request = crate::shared_http::CLIENT
        .get(&url)
        .header("User-Agent", "preloop-runner-server")
        .header("Accept", "application/vnd.github+json")
        .timeout(std::time::Duration::from_secs(10));
    if let Some(token) = credential.upstream_token.as_deref() {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => return stored_slug.to_owned(),
    };
    if !response.status().is_success() {
        return stored_slug.to_owned();
    }
    let body: serde_json::Value = match response.json().await {
        Ok(body) => body,
        Err(_) => return stored_slug.to_owned(),
    };
    body.get("full_name")
        .and_then(|name| name.as_str())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| stored_slug.to_owned())
}

/// Host (with port) of an HTTP(S) URL, lowercased and without userinfo.
/// Decides whether a forge-supplied download address may receive the forge
/// credential: the batch host may, anything else never may. `None` means
/// unparseable — never equal to anything, not even another `None`.
fn url_download_host(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let authority = rest.rsplit('@').next().unwrap_or(rest);
    let host = authority.split('/').next().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
}
/// Pull one LFS blob from the forge into the run's cache on first request.
///
/// Returns `true` when the object is now servable from `repository_dir`
/// (already present or just fetched and hash-verified). Returns `false` when
/// the forge has no such object or it cannot be proven intact — the caller
/// answers per-object not-found and the job falls back to the forge. Only
/// hard failures (unusable HTTP client, unwritable store) are errors.
async fn fetch_lfs_object_into_cache(
    client: &reqwest::Client,
    shared: &SharedState,
    repository_dir: &FsPath,
    upstream_repository: &str,
    credential: &CacheUpstreamCredential,
    oid: &str,
    expected_size: u64,
) -> Result<bool, ApiError> {
    use sha2::Digest;

    let path = lfs_object_path(repository_dir, oid);
    if tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return Ok(true);
    }
    let batch_url = format!(
        "{}/{}.git/info/lfs/objects/batch",
        shared.state.github_urls.server_url.trim_end_matches('/'),
        upstream_repository
    );
    let request_body = serde_json::json!({
        "operation": "download",
        "transfers": ["basic"],
        "objects": [{ "oid": oid, "size": expected_size }],
    });
    // The batch media type, both directions: strict forges reject plain
    // `application/json` with 406/415 even though lenient ones accept it.
    let mut batch = client
        .post(&batch_url)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/vnd.git-lfs+json",
        )
        .header(reqwest::header::ACCEPT, "application/vnd.git-lfs+json");
    if let Some(token) = credential.upstream_token.as_deref() {
        batch = batch.basic_auth("x-access-token", Some(token));
    }
    let batch = batch
        .body(request_body.to_string())
        .send()
        .await
        .map_err(|error| ApiError::internal(format!("forge LFS batch request failed: {error}")))?;
    if !batch.status().is_success() {
        return Ok(false);
    }
    let batch_body: serde_json::Value = batch
        .text()
        .await
        .map_err(|error| ApiError::internal(format!("forge LFS batch body unreadable: {error}")))?
        .parse()
        .map_err(|error| ApiError::internal(format!("forge LFS batch body invalid: {error}")))?;
    let object = batch_body
        .pointer("/objects/0")
        .filter(|first| first.pointer("/oid").and_then(|value| value.as_str()) == Some(oid));
    let download = object.and_then(|first| first.pointer("/actions/download"));
    let Some(href) = download
        .and_then(|action| action.pointer("/href"))
        .and_then(|value| value.as_str())
    else {
        return Ok(false);
    };
    // Trust boundary for anything the batch response names: the forge may
    // point downloads anywhere, including an attacker host, so no
    // Authorization value — neither the forge-supplied header nor the
    // engine credential — leaves for another host. Same-host downloads
    // keep working; presigned cross-host URLs download anonymously.
    let http_scheme = href.starts_with("http://") || href.starts_with("https://");
    let same_host = match (url_download_host(href), url_download_host(&batch_url)) {
        (Some(target), Some(forge)) => target == forge,
        _ => false,
    };
    let mut download_request = client.get(href);
    if same_host && http_scheme {
        if let Some(authorization) = download
            .and_then(|action| action.pointer("/header/Authorization"))
            .and_then(|value| value.as_str())
        {
            download_request =
                download_request.header(reqwest::header::AUTHORIZATION, authorization);
        } else if let Some(token) = credential.upstream_token.as_deref() {
            download_request = download_request.basic_auth("x-access-token", Some(token));
        }
    }
    let mut response = download_request
        .send()
        .await
        .map_err(|error| ApiError::internal(format!("forge LFS download failed: {error}")))?;
    if !response.status().is_success() {
        return Ok(false);
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ApiError::internal(format!("failed to create LFS cache directory: {error}"))
        })?;
    }
    let tmp = path.with_extension("tmp");
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .map_err(|error| ApiError::internal(format!("failed to stage LFS object: {error}")))?;
    let mut hasher = sha2::Sha256::new();
    let mut written: u64 = 0;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| ApiError::internal(format!("forge LFS download interrupted: {error}")))?
    {
        written = written.saturating_add(chunk.len() as u64);
        if written > MAX_LFS_OBJECT_BYTES {
            drop(file);
            let _ = tokio::fs::remove_file(&tmp).await;
            warn!(
                %oid,
                "LFS object exceeds the per-object cache cap; leaving uncached"
            );
            return Ok(false);
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(|error| {
            ApiError::internal(format!("failed to write staged LFS object: {error}"))
        })?;
    }
    file.flush().await.map_err(|error| {
        ApiError::internal(format!("failed to flush staged LFS object: {error}"))
    })?;
    drop(file);
    if format!("{:x}", hasher.finalize()) != oid {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Ok(false);
    }
    tokio::fs::rename(&tmp, &path)
        .await
        .map_err(|error| ApiError::internal(format!("failed to publish LFS object: {error}")))?;
    Ok(true)
}

/// Fetch a webhook run's immutable commit into the configured checkout cache.
///
/// The control plane alone writes this repository. Jobs only reach it through
/// [`snapshot_git_http`], authenticated by their Actions runtime token.
pub async fn create_remote_checkout_snapshot(
    shared: &SharedState,
    submission: &preloop_gha_protocol::WorkflowSubmission,
    run_id: RunId,
    commit_sha: &str,
) -> Result<Option<WorkspaceSnapshot>, ApiError> {
    use crate::config::CheckoutCacheMode;
    use sha2::Digest;

    let mode = shared.state.checkout_cache.mode;
    if mode == CheckoutCacheMode::Off || submission.local_workspace.is_some() {
        return Ok(None);
    }
    if !(40..=64).contains(&commit_sha.len())
        || !commit_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(None);
    }
    let Some(repository_id) = submission
        .payload
        .pointer("/repository/id")
        .and_then(|value| value.as_u64())
        .filter(|id| *id > 0)
        .map(|id| id.to_string())
    else {
        return Ok(None);
    };
    let repository_private = submission
        .payload
        .pointer("/repository/private")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);

    let Some(credential) =
        resolve_cache_upstream_credential(shared, &submission.repository, repository_private)
            .await?
    else {
        return Ok(None);
    };
    let CacheUpstreamCredential {
        upstream_token,
        credential_domain,
    } = credential;

    let namespace = CheckoutCacheNamespace {
        tenant_id: None,
        provider_origin: shared.state.github_urls.server_url.clone(),
        credential_domain,
        repository_id,
    };
    let namespace_json = serde_json::to_vec(&namespace).map_err(|error| {
        ApiError::internal(format!("failed to encode cache namespace: {error}"))
    })?;
    let namespace_key = format!("{:x}", sha2::Sha256::digest(namespace_json));
    let (repository, lock) = match mode {
        CheckoutCacheMode::Off => return Ok(None),
        CheckoutCacheMode::RunScoped => {
            let root = shared.state.state_dir.join("checkout-cache/runs");
            (
                root.join(format!("{run_id}.git")),
                root.join(format!("{run_id}.lock")),
            )
        }
        CheckoutCacheMode::Repository => {
            let root = shared.state.state_dir.join("checkout-cache/repositories");
            (
                root.join(format!("{namespace_key}.git")),
                root.join(format!("{namespace_key}.lock")),
            )
        }
    };
    let parent = repository
        .parent()
        .ok_or_else(|| ApiError::internal("checkout cache repository has no parent"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| ApiError::internal(format!("failed to create checkout cache: {error}")))?;
    let _guard = acquire_cache_lock(&lock).await?;

    if !repository.is_dir() {
        let mut init = Command::new("git");
        init.arg("init").arg("--bare").arg(&repository);
        run_git(&mut init, "initialize remote checkout cache").await?;
    }

    let upstream = format!(
        "{}/{}.git",
        shared.state.github_urls.server_url.trim_end_matches('/'),
        submission.repository
    );
    let mut fetch = Command::new("git");
    fetch
        .arg("--git-dir")
        .arg(&repository)
        .arg("fetch")
        .arg("--no-tags")
        .arg("--depth=1")
        .arg(&upstream)
        .arg(commit_sha)
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some(token) = upstream_token.as_deref() {
        // Scope the credential to the upstream origin: a command-wide header
        // would also ride along on redirects to unvouched hosts.
        if let Some((key, value)) = scoped_fetch_auth_header(&upstream, token) {
            fetch
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", key)
                .env("GIT_CONFIG_VALUE_0", value);
        } else if upstream.contains("://") {
            warn!(
                %run_id,
                "checkout-cache upstream URL has no usable origin; fetching without credentials"
            );
        }
    }
    run_git(&mut fetch, "fetch remote checkout snapshot").await?;

    let snapshot_ref = format!("refs/preloop/runs/{run_id}");
    let mut update_ref = Command::new("git");
    update_ref
        .arg("--git-dir")
        .arg(&repository)
        .arg("update-ref")
        .arg(&snapshot_ref)
        .arg(commit_sha);
    run_git(&mut update_ref, "publish remote checkout snapshot").await?;
    // Serving rules differ by scope. A run-scoped repository holds exactly
    // one run's commit, so arbitrary wants cannot escape it and its refs
    // stay advertised. A shared repository retains many runs' commits: hide
    // every run ref and serve only ref tips, so a job fetches its own run
    // commit and can neither enumerate nor request any other run's objects.
    if mode == CheckoutCacheMode::Repository {
        let mut hide = Command::new("git");
        hide.arg("--git-dir")
            .arg(&repository)
            .arg("config")
            .arg("uploadpack.hideRefs")
            .arg("refs/preloop/runs");
        run_git(&mut hide, "hide cached run refs").await?;
    }
    let mut configure = Command::new("git");
    configure.arg("--git-dir").arg(&repository).arg("config");
    if mode == CheckoutCacheMode::Repository {
        configure.arg("uploadpack.allowTipSHA1InWant").arg("true");
    } else {
        configure.arg("uploadpack.allowAnySHA1InWant").arg("true");
    }
    run_git(&mut configure, "configure remote checkout cache").await?;

    let mut tree = Command::new("git");
    tree.arg("--git-dir")
        .arg(&repository)
        .arg("rev-parse")
        .arg(format!("{commit_sha}^{{tree}}"));
    let tree_sha = output_text(
        &run_git(&mut tree, "resolve remote checkout tree").await?,
        "resolve remote checkout tree",
    )?;
    let storage_repository = repository
        .strip_prefix(&shared.state.state_dir)
        .map_err(|_| ApiError::internal("checkout cache escaped state directory"))?
        .to_string_lossy()
        .to_string();
    Ok(Some(WorkspaceSnapshot {
        commit_sha: commit_sha.to_owned(),
        tree_sha,
        head_sha: Some(commit_sha.to_owned()),
        repository: format!("snapshots/{run_id}"),
        default_branch: submission
            .payload
            .pointer("/repository/default_branch")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        before_sha: None,
        snapshot_timing: None,
        storage_repository: Some(storage_repository),
        source: match mode {
            CheckoutCacheMode::RunScoped => SnapshotSource::RemoteRunScoped,
            CheckoutCacheMode::Repository => SnapshotSource::RemoteRepository,
            CheckoutCacheMode::Off => unreachable!(),
        },
        cache_namespace: Some(namespace),
        upstream_repository: Some(submission.repository.clone()),
        upstream_repository_id: submission
            .payload
            .pointer("/repository/id")
            .and_then(serde_json::Value::as_u64)
            .filter(|id| *id > 0),
        upstream_private: Some(repository_private),
    }))
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
    let mut uploadpack_filter = Command::new("git");
    uploadpack_filter.env("GIT_DIR", staging_repository).args([
        "config",
        "uploadpack.allowFilter",
        "true",
    ]);
    run_git(
        &mut uploadpack_filter,
        "allow filtered wants in snapshot upload-pack",
    )
    .await?;

    // Preserve any local Git LFS object store so `actions/checkout` with
    // `lfs: true` can download through the snapshot Git HTTP endpoint.
    copy_lfs_objects_into_snapshot(&common_dir, staging_repository).await?;

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
        .filter(|sha| preloop_gha_protocol::git_ref::is_commit_sha(sha));
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

/// Copy the workspace Git LFS object store into the staging snapshot repository.
async fn copy_lfs_objects_into_snapshot(
    source_common_dir: &FsPath,
    staging_repository: &FsPath,
) -> Result<(), ApiError> {
    let source = source_common_dir.join("lfs");
    if !source.is_dir() {
        return Ok(());
    }
    let destination = staging_repository.join("lfs");
    let source_owned = source.clone();
    let destination_owned = destination.clone();
    tokio::task::spawn_blocking(move || copy_dir_recursive(&source_owned, &destination_owned))
        .await
        .map_err(|error| ApiError::internal(format!("Git LFS copy task failed: {error}")))?
        .map_err(|error| {
            ApiError::internal(format!(
                "failed to copy Git LFS objects from {} into {}: {error}",
                source.display(),
                destination.display()
            ))
        })
}

fn copy_dir_recursive(source: &FsPath, destination: &FsPath) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else if file_type.is_file() {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

fn lfs_object_oid_from_path(path: &str) -> Option<&str> {
    let oid = path
        .strip_prefix("info/lfs/objects/")
        .or_else(|| path.strip_prefix(".git/info/lfs/objects/"))?;
    is_valid_lfs_oid(oid).then_some(oid)
}

fn is_valid_lfs_oid(oid: &str) -> bool {
    oid.len() == 64 && oid.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn lfs_object_path(repository: &FsPath, oid: &str) -> PathBuf {
    repository
        .join("lfs")
        .join("objects")
        .join(&oid[..2])
        .join(&oid[2..4])
        .join(oid)
}

#[derive(Debug, Deserialize)]
struct LfsBatchRequest {
    #[serde(default)]
    operation: Option<String>,
    #[serde(default)]
    objects: Vec<LfsBatchObject>,
}

#[derive(Debug, Deserialize)]
struct LfsBatchObject {
    oid: String,
    #[serde(default)]
    size: Option<u64>,
}

async fn lfs_batch_response(
    repository: &FsPath,
    run_id: RunId,
    authorization_header: Option<&str>,
    body: &[u8],
    fetch: Option<LfsFetch<'_>>,
) -> Result<serde_json::Value, ApiError> {
    let request: LfsBatchRequest = serde_json::from_slice(body)
        .map_err(|error| ApiError::bad_request(format!("invalid Git LFS batch body: {error}")))?;
    let operation = request.operation.as_deref().unwrap_or("download");
    let base = runner_base_url();
    if operation == "download" {
        populate_missing_lfs_objects(repository, &request.objects, fetch).await;
    }
    let mut objects = Vec::with_capacity(request.objects.len());
    for object in request.objects {
        let requested_size = object.size.unwrap_or(0);
        if !is_valid_lfs_oid(&object.oid) {
            objects.push(serde_json::json!({
                "oid": object.oid,
                "size": requested_size,
                "error": {
                    "code": 422,
                    "message": "invalid Git LFS object id"
                }
            }));
            continue;
        }
        if operation != "download" {
            objects.push(serde_json::json!({
                "oid": object.oid,
                "size": requested_size,
                "error": {
                    "code": 403,
                    "message": "snapshot Git endpoint is read-only"
                }
            }));
            continue;
        }
        let path = lfs_object_path(repository, &object.oid);
        match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => {
                let mut download = serde_json::json!({
                    "href": format!("{base}/snapshots/{run_id}/info/lfs/objects/{}", object.oid),
                });
                if let Some(authorization) = authorization_header {
                    download["header"] = serde_json::json!({
                        "Authorization": authorization
                    });
                }
                objects.push(serde_json::json!({
                    "oid": object.oid,
                    "size": metadata.len(),
                    "authenticated": authorization_header.is_some(),
                    "actions": {
                        "download": download
                    }
                }));
            }
            _ => {
                objects.push(serde_json::json!({
                    "oid": object.oid,
                    "size": requested_size,
                    "error": {
                        "code": 404,
                        "message": "Git LFS object is not available in this workspace snapshot"
                    }
                }));
            }
        }
    }
    Ok(serde_json::json!({
        "transfer": "basic",
        "objects": objects,
        "hash_algo": "sha256"
    }))
}

/// Forge HTTP client for on-demand LFS fetching. Timeouts are generous on
/// purpose: blobs stream straight to disk and can be large; the per-object
/// byte cap, not the clock, is the backstop.
static LFS_FORGE_CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    crate::shared_http::github_client_builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .expect("LFS forge client builds")
});

/// Fill a run cache's missing LFS blobs from the forge, once per object.
///
/// The per-repository lock serializes concurrent first requests (four shards
/// asking for the same golden trigger one upstream fetch, not four), and a
/// re-check under the lock absorbs races with sibling jobs. Unprovable
/// repositories and any forge-side miss are left absent: the batch answer
/// stays per-object not-found and the job falls back to the forge.
async fn populate_missing_lfs_objects(
    repository: &FsPath,
    objects: &[LfsBatchObject],
    fetch: Option<LfsFetch<'_>>,
) {
    let Some(fetch) = fetch else { return };
    let mut missing: Vec<(&str, u64)> = Vec::new();
    for object in objects {
        if !is_valid_lfs_oid(&object.oid) {
            continue;
        }
        if missing.iter().any(|(oid, _)| *oid == object.oid) {
            continue;
        }
        let path = lfs_object_path(repository, &object.oid);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            continue;
        }
        missing.push((object.oid.as_str(), object.size.unwrap_or(0)));
    }
    if missing.is_empty() {
        return;
    }
    // The ceiling is enforced by the sweep after the fact; check the
    // projection first so one batch cannot blow past it before any sweep
    // runs. Sizes are client-claimed, so this is a guard, not accounting —
    // the per-object cap bounds what actually lands on disk.
    let max_bytes = fetch.shared.state.checkout_cache.max_bytes;
    if max_bytes > 0 {
        let dir = repository.to_owned();
        let current = tokio::task::spawn_blocking(move || directory_bytes(&dir))
            .await
            .unwrap_or(0);
        let wanted: u64 = missing
            .iter()
            .map(|(_, size)| *size)
            .fold(0, u64::saturating_add);
        if current.saturating_add(wanted) > max_bytes {
            warn!(
                wanted,
                max_bytes, "LFS populate would exceed the cache ceiling; leaving uncached"
            );
            return;
        }
    }
    // Resolve the canonical slug before touching the forge: the stored slug
    // may predate a rename or owner transfer. The numeric ID is stable, so
    // an existing snapshot keeps fetching under the repository's new name.
    let canonical_slug = resolve_canonical_upstream_slug(
        fetch.shared,
        fetch.upstream_repository,
        fetch.upstream_repository_id,
        fetch.upstream_private,
    )
    .await;
    let credential = match resolve_cache_upstream_credential(
        fetch.shared,
        &canonical_slug,
        fetch.upstream_private,
    )
    .await
    {
        Ok(credential) => credential,
        Err(error) => {
            warn!(error = ?error, "forge credential mint failed; LFS objects stay uncached");
            return;
        }
    };
    let Some(credential) = credential else { return };
    let lock_path = repository.with_extension("lock");
    let _guard = match acquire_cache_lock(&lock_path).await {
        Ok(guard) => guard,
        Err(error) => {
            warn!(error = ?error, "LFS cache lock unavailable; objects stay uncached");
            return;
        }
    };
    for (oid, size) in missing {
        match fetch_lfs_object_into_cache(
            &LFS_FORGE_CLIENT,
            fetch.shared,
            repository,
            &canonical_slug,
            &credential,
            oid,
            size,
        )
        .await
        {
            Ok(_) => {}
            Err(error) => {
                warn!(%oid, error = ?error, "forge LFS fetch failed; answering not-found");
            }
        }
    }
}

async fn serve_lfs_object(repository: &FsPath, oid: &str) -> Result<Response<Body>, ApiError> {
    let path = lfs_object_path(repository, oid);
    let file = tokio::fs::File::open(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ApiError::not_found("Git LFS object not found")
        } else {
            ApiError::internal(format!("failed to open Git LFS object: {error}"))
        }
    })?;
    let metadata = file.metadata().await.map_err(|error| {
        ApiError::internal(format!("failed to read Git LFS object metadata: {error}"))
    })?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, metadata.len())
        .body(Body::from_stream(ReaderStream::new(file)))
        .unwrap())
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
        if record[..tab].starts_with(b"160000 ")
            && let Ok(path) = std::str::from_utf8(&record[tab + 1..])
        {
            paths.push(path.to_owned());
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
        if url_names.contains(name)
            && let Ok(path) = std::str::from_utf8(path)
        {
            configured.insert(path.to_owned());
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

/// Whether the snapshot object cache holds `sha` as a commit.
///
/// Any failure to ask counts as "does not hold it": the caller answers by
/// fetching, which is the safe direction.
async fn cache_holds_commit(repository: &FsPath, sha: &str) -> bool {
    Command::new("git")
        .arg("--git-dir")
        .arg(repository)
        .args(["cat-file", "-e", &format!("{sha}^{{commit}}")])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
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
        // An unborn HEAD gives nothing to compare, so always refresh. A
        // recorded head the cache does not hold is stale as well: the marker
        // alone cannot be trusted, or a cache that missed a commit once would
        // keep missing it until the workspace moved on.
        let head_missing = match source_head {
            Some(head) => !cache_holds_commit(&repository, head).await,
            None => false,
        };
        if source_head.is_none() || cached_head.as_deref() != source_head || head_missing {
            let mut fetch = Command::new("git");
            fetch
                .env("GIT_DIR", &repository)
                .args(["fetch", "--quiet", "--force", "--prune"])
                .arg(workspace)
                .args(["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]);
            // A detached HEAD (a worktree at a CI merge sha, a bisect step, a
            // review checkout) is reachable from no branch or tag, so the
            // refspecs above never carry its commit and seeding the snapshot
            // index fails with "failed to unpack tree object". A bare `HEAD`
            // source stores its objects without creating a ref in the cache.
            if source_head.is_some() {
                fetch.arg("HEAD");
            }
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

/// Origin-scoped auth header for the checkout-cache fetch: the credential
/// rides only to the upstream's own origin. A command-wide `http.extraHeader`
/// would also be sent on redirects to hosts the operator never vouched for.
/// Returns `None` for URLs without a usable HTTP(S) authority (local paths
/// in tests need no auth at all).
fn scoped_fetch_auth_header(upstream_url: &str, token: &str) -> Option<(String, String)> {
    let (scheme, rest) = upstream_url.split_once("://")?;
    if scheme != "http" && scheme != "https" {
        return None;
    }
    // Strip userinfo before extracting the host; the port separator must not
    // truncate a host, and credentials in the URL must not leak into the key.
    let authority = rest.rsplit('@').next().unwrap_or(rest);
    let host = authority.split('/').next().unwrap_or("");
    if host.is_empty() {
        return None;
    }
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(format!("x-access-token:{token}").as_bytes());
    Some((
        format!("http.{scheme}://{host}/.extraHeader"),
        format!("Authorization: basic {encoded}"),
    ))
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

/// Directory-based mutex for one cache repository. The holder refreshes a
/// heartbeat file inside the directory while it works: without it a slow
/// upstream fetch (past the 60s stale takeover) would have its lock stolen
/// mid-write and share the repository with a second writer. Takeover still
/// applies to holders that died without cleanup — their heartbeat goes stale
/// exactly like their lock did before.
struct CacheLock {
    dir: PathBuf,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

/// Liveness marker refreshed by the lock holder. Cousin of
/// [`RELEASED_MARKER`]: presence plus freshness means "owned", absence or
/// age means "abandoned".
const LOCK_HEARTBEAT: &str = "preloop-lock-heartbeat";

/// Drop a finished run's snapshot repository.
///
/// The repository exists so the run's checkouts can fetch the workspace; once
/// every job is terminal nothing can ask for it again, and a re-run captures a
/// fresh snapshot. Keeping them made the state directory grow without bound —
/// enough matrix runs filled the disk and the engine began failing blob writes
/// with HTTP 500. The persistent object cache is untouched: it is shared and
/// is what makes the next snapshot cheap.
pub async fn discard_workspace_snapshot(state_dir: &FsPath, run_id: RunId) {
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

/// Reap workspace snapshots whose deletion timer died with the last engine.
///
/// [`discard_workspace_snapshot`] is scheduled by an in-process
/// `tokio::spawn(sleep(retention))` at run completion; a restart inside the
/// retention window orphans the repository permanently because nothing else
/// ever looks at `state/snapshots/`. Called once at startup after the store
/// has restored runs:
///
/// - live (non-terminal) runs keep their snapshot — jobs may still check out;
/// - terminal runs inside the retention window get their discard timer
///   re-armed for the remaining time;
/// - terminal runs past retention, and directories whose run no longer
///   exists in memory, are deleted now;
/// - an unparseable or very young entry is left alone: a snapshot being
///   written right now must not be swept out from under its run.
pub async fn sweep_workspace_snapshots(shared: &Arc<SharedState>) {
    let retention = std::time::Duration::from_secs(shared.state.snapshot_retention_seconds);
    let runs: std::collections::BTreeMap<
        RunId,
        (ExecutionStatus, Option<chrono::DateTime<chrono::Utc>>),
    > = shared
        .state
        .backend
        .list_runs(crate::control::backend::RunListFilter {
            workflow: None,
            status: None,
            event: None,
            limit: 200,
        })
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|run| (run.run_id, (run.status, run.completed_at)))
        .collect();
    let snapshots_dir = shared.state.state_dir.join("snapshots");
    let mut entries = match tokio::fs::read_dir(&snapshots_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            warn!(
                path = %snapshots_dir.display(),
                %error,
                "Failed to list workspace snapshots for startup sweep"
            );
            return;
        }
    };
    let mut swept = 0usize;
    let mut rearmed = 0usize;
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(error) => {
                warn!(
                    path = %snapshots_dir.display(),
                    %error,
                    "Failed to read entry during workspace snapshot sweep"
                );
                break;
            }
        };
        let path = entry.path();
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let Ok(run_id) = name.parse::<RunId>() else {
            continue;
        };
        match runs.get(&run_id) {
            Some((status, _)) if !status.is_terminal() => {}
            Some((_, completed_at)) => {
                let age = match completed_at {
                    Some(at) => Some(
                        chrono::Utc::now()
                            .signed_duration_since(*at)
                            .to_std()
                            .unwrap_or_default(),
                    ),
                    None => entry_age(&path).await,
                };
                match age {
                    Some(age) if age >= retention => {
                        discard_workspace_snapshot(&shared.state.state_dir, run_id).await;
                        swept += 1;
                    }
                    Some(age) => {
                        // The completion-time discard died with the last
                        // process; re-arm it for the remaining window so the
                        // snapshot still expires on schedule.
                        let state_dir = shared.state.state_dir.clone();
                        let remaining = retention - age;
                        tokio::spawn(async move {
                            tokio::time::sleep(remaining).await;
                            discard_workspace_snapshot(&state_dir, run_id).await;
                        });
                        rearmed += 1;
                    }
                    None => {}
                }
            }
            // No run record survived the restart — the snapshot can never be
            // referenced again. Keep only entries young enough to plausibly
            // still be mid-write.
            None => match entry_age(&path).await {
                Some(age) if age >= retention => {
                    discard_workspace_snapshot(&shared.state.state_dir, run_id).await;
                    swept += 1;
                }
                Some(age) => {
                    let state_dir = shared.state.state_dir.clone();
                    let remaining = retention - age;
                    tokio::spawn(async move {
                        tokio::time::sleep(remaining).await;
                        discard_workspace_snapshot(&state_dir, run_id).await;
                    });
                    rearmed += 1;
                }
                None => {}
            },
        }
    }
    if swept > 0 || rearmed > 0 {
        info!(swept, rearmed, "Swept orphaned workspace snapshots");
    }
}

/// Marker file recording when a run-scoped cache became releasable.
const RELEASED_MARKER: &str = "preloop-released-at";

/// Release a finished run's remote checkout cache.
///
/// Run-scoped caches are marked rather than deleted: a retry window needs the
/// objects, and [`prune_checkout_cache`] collects them once the configured
/// retention elapses. Repository caches only drop the run's ref; the shared
/// objects are the whole point of that mode.
pub async fn release_remote_checkout_snapshot(
    state_dir: &FsPath,
    snapshot: &WorkspaceSnapshot,
    run_id: RunId,
) {
    let Some(relative) = snapshot.storage_repository.as_deref() else {
        return;
    };
    let repository = state_dir.join(relative);
    match snapshot.source {
        SnapshotSource::LocalWorkspace => {}
        SnapshotSource::RemoteRunScoped => {
            let marker = repository.join(RELEASED_MARKER);
            if let Err(error) = tokio::fs::write(&marker, crate::store::now_us().to_string()).await
            {
                warn!(
                    %run_id,
                    path = %marker.display(),
                    %error,
                    "Failed to mark run-scoped checkout cache releasable"
                );
            }
        }
        SnapshotSource::RemoteRepository => {
            let mut delete = Command::new("git");
            delete
                .arg("--git-dir")
                .arg(&repository)
                .arg("update-ref")
                .arg("-d")
                .arg(format!("refs/preloop/runs/{run_id}"));
            if let Err(error) = run_git(&mut delete, "release repository checkout ref").await {
                debug!(%run_id, error = ?error, "Failed to drop repository checkout ref");
            }
        }
    }
}

/// Collect expired checkout caches and enforce the configured size ceiling.
///
/// Run-scoped entries go once their release marker ages past the run
/// retention; an entry that never terminalized is still bounded by the same
/// window measured from its own mtime, so a lost completion cannot leak a
/// repository forever. Repository entries expire on idle age, then the
/// largest-first sweep brings total bytes under `max_bytes`.
pub async fn prune_checkout_cache(state_dir: &FsPath, config: &crate::config::CheckoutCacheConfig) {
    let root = state_dir.join("checkout-cache");
    let run_retention = std::time::Duration::from_secs(config.run_retention_seconds);
    let repository_retention = std::time::Duration::from_secs(config.repository_retention_seconds);
    // A run whose completion never landed has no marker; bound it by the same
    // retention applied to the repository itself, with a floor so a tiny
    // configured retention cannot delete a cache a live run is still using.
    let orphan_retention = run_retention.max(std::time::Duration::from_secs(6 * 60 * 60));

    for (directory, expiry, marker_required) in [
        (root.join("runs"), run_retention, true),
        (root.join("repositories"), repository_retention, false),
    ] {
        let mut entries = match tokio::fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                warn!(path = %directory.display(), %error, "Failed to scan checkout cache");
                continue;
            }
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("git") {
                continue;
            }
            let age = if marker_required {
                match tokio::fs::read(path.join(RELEASED_MARKER)).await {
                    Ok(_) => entry_age(&path.join(RELEASED_MARKER)).await,
                    Err(_) => entry_age(&path).await.filter(|age| *age > orphan_retention),
                }
            } else {
                entry_age(&path).await
            };
            if age.is_some_and(|age| age > expiry) {
                remove_cache_entry(&path).await;
            }
        }
    }

    if config.max_bytes == 0 {
        return;
    }
    let mut sized: Vec<(PathBuf, u64)> = Vec::new();
    let mut total: u64 = 0;
    for directory in [root.join("runs"), root.join("repositories")] {
        let Ok(mut entries) = tokio::fs::read_dir(&directory).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("git") {
                continue;
            }
            let bytes = {
                let path = path.clone();
                tokio::task::spawn_blocking(move || directory_bytes(&path))
                    .await
                    .unwrap_or(0)
            };
            total = total.saturating_add(bytes);
            sized.push((path, bytes));
        }
    }
    if total <= config.max_bytes {
        return;
    }
    // Oldest first: a cache nobody has touched recently is the cheapest one to
    // lose, and every eviction only costs the next run an upstream fetch.
    let mut ordered: Vec<(PathBuf, u64, std::time::Duration)> = Vec::new();
    for (path, bytes) in sized {
        let age = entry_age(&path).await.unwrap_or_default();
        ordered.push((path, bytes, age));
    }
    ordered.sort_by_key(|(_, _, age)| std::cmp::Reverse(*age));
    for (path, bytes, _) in ordered {
        if total <= config.max_bytes {
            break;
        }
        // A ceiling must never break a live checkout: unreleased run caches
        // and shared repositories that still publish a run ref may be serving
        // a job right now, and redirected checkouts have no forge fallback.
        if cache_entry_is_live(&path, &root).await {
            continue;
        }
        remove_cache_entry(&path).await;
        total = total.saturating_sub(bytes);
    }
}

/// Whether a cache entry may still serve a live run. Run-scoped entries are
/// live until the release marker lands; shared repositories are live while
/// any run ref is published. Anything else is retry-window weight the size
/// sweep may reclaim.
async fn cache_entry_is_live(path: &FsPath, root: &FsPath) -> bool {
    if path.starts_with(root.join("runs")) {
        return tokio::fs::try_exists(path.join(RELEASED_MARKER))
            .await
            .map(|exists| !exists)
            .unwrap_or(true);
    }
    let mut refs = Command::new("git");
    refs.arg("--git-dir")
        .arg(path)
        .arg("for-each-ref")
        .arg("--format=%(refname)")
        .arg("refs/preloop/runs");
    match run_git(&mut refs, "list cached run refs").await {
        Ok(output) => !output.stdout.is_empty(),
        // An unreadable repository serves nothing; let the sweep reclaim it.
        Err(_) => false,
    }
}

async fn entry_age(path: &FsPath) -> Option<std::time::Duration> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.elapsed().ok())
}

async fn remove_cache_entry(path: &FsPath) {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => debug!(path = %path.display(), "Evicted checkout cache entry"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            path = %path.display(),
            %error,
            "Failed to evict checkout cache entry"
        ),
    }
}

fn directory_bytes(path: &FsPath) -> u64 {
    let mut total: u64 = 0;
    let mut pending = vec![path.to_owned()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    total
}

impl CacheLock {
    fn held(path: PathBuf) -> Self {
        let heartbeat_file = path.join(LOCK_HEARTBEAT);
        let _ = std::fs::write(&heartbeat_file, "held");
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                if std::fs::write(&heartbeat_file, "held").is_err() {
                    return;
                }
            }
        });
        Self {
            dir: path,
            heartbeat: Some(heartbeat),
        }
    }
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        if let Some(heartbeat) = self.heartbeat.take() {
            heartbeat.abort();
        }
        let _ = std::fs::remove_file(self.dir.join(LOCK_HEARTBEAT));
        let _ = std::fs::remove_dir(&self.dir);
    }
}

/// Freshness of a lock directory: the holder's heartbeat when present, the
/// directory itself for locks that predate heartbeats or died mid-create.
fn lock_age(path: &FsPath) -> Option<std::time::Duration> {
    let marker = path.join(LOCK_HEARTBEAT);
    let target = if marker.is_file() { &marker } else { path };
    std::fs::metadata(target)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.elapsed().ok())
}
async fn acquire_cache_lock(path: &FsPath) -> Result<CacheLock, ApiError> {
    let started = std::time::Instant::now();
    loop {
        match std::fs::create_dir(path) {
            Ok(()) => return Ok(CacheLock::held(path.to_owned())),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // Freshness comes from the holder's heartbeat, so a slow
                // fetch is never mistaken for an abandoned lock. Only a
                // holder that died without cleanup goes stale.
                let stale =
                    lock_age(path).is_some_and(|age| age > std::time::Duration::from_secs(60));
                if stale {
                    let _ = std::fs::remove_file(path.join(LOCK_HEARTBEAT));
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

/// Evaluate a step input's `${{ }}` template against the message's own
/// context data, proving the value the runner will compute at step time.
///
/// Returns `None` when the value is unprovable: a parse/eval failure, an
/// unclosed expression, or a reference to a context the submission does not
/// carry (`env`, `secrets`, `steps`, `vars` secrets …). Missing roots are the
/// dangerous case — an empty `env.MISSING` evaluates to `""`, which would
/// look provably default while the runner sees the real env. Only inputs
/// every referenced root can prove are redirected or rerouted.
fn eval_step_input_template(
    raw: &str,
    context_data: &BTreeMap<String, preloop_gha_protocol::azdo::PipelineContextData>,
) -> Option<String> {
    if !raw.contains("${{") {
        return Some(raw.to_owned());
    }
    let mut ctx = preloop_gha_expressions::Context::new();
    for (key, value) in context_data {
        ctx.insert(key.clone(), value.to_json());
    }
    // Scan `${{ … }}` tokens; refuse the input if any expression references a
    // root we cannot prove or fails to evaluate.
    let mut rest = raw;
    let mut out = String::new();
    while let Some(start) = rest.find("${{") {
        out.push_str(&rest[..start]);
        let remaining = &rest[start + 3..];
        let end = preloop_gha_protocol::expr_scan::find_expression_end(remaining)?;
        let expr = remaining[..end].trim();
        let referenced = preloop_gha_expressions::collect_contexts(expr).ok()?;
        // `needs` is present from the start but empty: `hydrate_needs_context`
        // fills it only after the dependency completes. Evaluating against the
        // empty shell would prove a dynamic dependency output (`ref: ${{
        // needs.build.outputs.sha }}`) to be `""` and misclassify it as the
        // default checkout, permanently rewriting it to the snapshot before
        // the real value is known. Treat it as unprovable here.
        if !referenced
            .iter()
            .all(|root| root != "needs" && context_data.contains_key(root.as_str()))
        {
            return None;
        }
        let value = preloop_gha_expressions::eval_expression(expr, &ctx).ok()?;
        out.push_str(&input_value_to_string(&value));
        rest = &rest[start + 3 + end + 2..];
    }
    out.push_str(rest);
    Some(out)
}

/// Render an evaluated expression the way the runner renders step inputs:
/// whole numbers without a decimal point, strings verbatim, other values as
/// JSON.
fn input_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Number(n) => {
            if let Some(value) = n.as_i64() {
                return value.to_string();
            }
            if let Some(value) = n.as_u64() {
                return value.to_string();
            }
            match n.as_f64() {
                Some(value) if value.is_finite() && value.fract() == 0.0 => value.to_string(),
                _ => n.to_string(),
            }
        }
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// One resolved input value and whether it exists at all.
struct ResolvedInput {
    present: bool,
    /// `Some` when the input is provably a fixed value — literal or
    /// expression fully evaluable against submission context.
    value: Option<String>,
    /// The input's raw template, when the step supplied one. A failed
    /// expression still carries its source text, which lets the caller
    /// recognize the action's declared default (`${{ github.repository }}`)
    /// even when the context cannot evaluate it.
    raw: Option<String>,
}
///
/// Whitespace-empty values count as absent — the action treats them as unset.
fn resolve_step_input(
    step: &preloop_gha_protocol::azdo::TaskStep,
    name: &str,
    context_data: &BTreeMap<String, preloop_gha_protocol::azdo::PipelineContextData>,
) -> ResolvedInput {
    for (key, value) in &step.inputs {
        if !key.eq_ignore_ascii_case(name) {
            continue;
        }
        let raw = value.as_str();
        if raw.trim().is_empty() {
            return ResolvedInput {
                present: false,
                value: None,
                raw: None,
            };
        }
        return ResolvedInput {
            present: true,
            value: eval_step_input_template(raw, context_data).map(|v| v.trim().to_owned()),
            raw: Some(raw.to_owned()),
        };
    }
    ResolvedInput {
        present: false,
        value: None,
        raw: None,
    }
}

/// Normalize a checkout `repository` input to an `owner/repo` slug, or `None`
/// when it cannot name the forge the job runs against.
///
/// `actions/checkout` accepts slugs and full URLs (`https://host/o/r`). A
/// slug is only meaningful against the job's own forge — the
/// `github-server-url` input or `github.server_url` decides — so callers
/// compare the resolved slug against the run's repository.
fn normalize_checkout_repository(raw: &str) -> Option<String> {
    let raw = raw.trim().trim_matches('"').trim_matches('\'');
    if raw.is_empty() {
        return None;
    }
    if let Some(rest) = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
    {
        // URL form: strip host and optional .git; keep the slug for the
        // caller to pair with the hostname check.
        let rest = rest.trim_end_matches(".git");
        let mut parts = rest.split('/');
        parts.next()?; // host
        let owner = parts.next()?;
        let repo = parts.next()?;
        if parts.next().is_some() || owner.is_empty() || repo.is_empty() {
            return None;
        }
        return Some(format!("{owner}/{repo}"));
    }
    let slug = raw.trim_end_matches(".git");
    let mut parts = slug.split('/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    if parts.next().is_some() || owner.is_empty() || repo.is_empty() {
        return None;
    }
    Some(format!("{owner}/{repo}"))
}

/// The repository slug the run targets, from the message's github context.
fn run_repository_slug(
    context_data: &BTreeMap<String, preloop_gha_protocol::azdo::PipelineContextData>,
) -> Option<String> {
    context_data
        .get("github")
        .map(|data| data.to_json())
        .and_then(|github| {
            github
                .get("repository")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
}

/// The forge origin (SCHEME://HOST[:PORT]) the run targets, from the
/// message's github context (`github.server_url`).
fn run_forge_origin(
    context_data: &BTreeMap<String, preloop_gha_protocol::azdo::PipelineContextData>,
) -> Option<String> {
    let url = context_data
        .get("github")
        .map(|data| data.to_json())
        .and_then(|github| {
            github
                .get("server_url")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "https://github.com".to_owned());
    url_origin(&url)
}

/// True when `value` names the same origin as `reference` — the checkout
/// `github-server-url` input is default when it is the job's own forge.
fn same_origin(value: &str, reference: &str) -> bool {
    let origin = |raw: &str| -> Option<String> {
        let raw = raw.trim().trim_end_matches('/');
        let (scheme, rest) = raw.split_once("://")?;
        let host = rest.split('/').next()?;
        Some(format!(
            "{}://{}",
            scheme.to_ascii_lowercase(),
            host.to_ascii_lowercase()
        ))
    };
    match (origin(value), origin(reference)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// `scheme://host[:port]` for http(s) URLs, `None` for anything else
/// (slugs, SSH). Used to prove a `repository:` URL points at the run's forge.
fn url_origin(raw: &str) -> Option<String> {
    let raw = raw.trim().trim_end_matches('/');
    let (scheme, rest) = raw.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
        return None;
    }
    let host = rest.split('/').next()?;
    if host.is_empty() {
        return None;
    }
    Some(format!(
        "{}://{}",
        scheme.to_ascii_lowercase(),
        host.to_ascii_lowercase()
    ))
}

/// Rewrite default primary checkout steps to fetch the local snapshot.
///
/// `runtime_token` is pinned onto the step so the checkout authenticates to
/// [`snapshot_git_http`] with the local HMAC job JWT it expects. Without it the
/// step would fall back to `${{ github.token }}`, which carries a GitHub App
/// installation token or PAT whenever one is configured — neither of which
/// [`authorize_snapshot_token`] can verify.
///
/// An input counts as the action's default when it is absent, whitespace-
/// empty, the declared default expression (`${{ github.repository }}`,
/// `${{ github.server_url }}`), a literal spelling of the run's own
/// repository or forge, or an expression provably evaluating to one of those
/// against the submission context — `ref: ${{ inputs.x || '' }}` resolving
/// to the run's SHA is still the default checkout. Unprovable values
/// (eval failures, roots outside `context_data`) are left on the forge.
pub fn redirect_primary_checkout(
    message: &mut preloop_gha_protocol::azdo::AgentJobRequestMessage,
    snapshot: &WorkspaceSnapshot,
    github_server_url: &str,
    runtime_token: &str,
) -> usize {
    let context_data = message.context_data.clone();
    let run_repo = run_repository_slug(&context_data);
    let github_json = context_data
        .get("github")
        .map(|data| data.to_json())
        .unwrap_or_default();
    let forge_origin = run_forge_origin(&context_data);
    // Refs and SHAs that all mean "the commit this event tested": the run
    // SHA, the event ref, the PR head SHA/ref, the snapshot commit, the
    // workspace's real HEAD, and the default branch in both spellings.
    let mut default_ref_candidates: Vec<String> = Vec::new();
    for key in ["sha", "ref", "ref_name", "head_ref"] {
        if let Some(value) = github_json.get(key).and_then(|v| v.as_str()) {
            default_ref_candidates.push(value.to_owned());
        }
    }
    for pointer in [
        "/event/pull_request/head/sha",
        "/event/pull_request/head/ref",
    ] {
        if let Some(value) = github_json.pointer(pointer).and_then(|v| v.as_str()) {
            default_ref_candidates.push(value.to_owned());
        }
    }
    default_ref_candidates.push(snapshot.commit_sha.clone());
    if let Some(head) = snapshot.head_sha.as_deref() {
        default_ref_candidates.push(head.to_owned());
    }
    // `snapshot.default_branch` is deliberately absent: it names the
    // *repository's* default branch from the event payload, which on a cached
    // remote run of a feature branch or PR is not the branch this run tests.
    // Counting it would make an explicit `ref: main` look like the default
    // checkout and silently rewrite the step to the run's commit. Only refs
    // that identify `snapshot.commit_sha` (or the real commit it is based on)
    // are candidates — a literal branch name stays on the forge, where it
    // resolves to the branch the workflow actually asked for.
    let mut redirected = 0;
    let mut pinned = Vec::new();
    for step in &mut message.steps {
        let is_checkout = step
            .reference
            .as_ref()
            .and_then(|reference| reference.name.as_deref())
            .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"));
        if !is_checkout {
            continue;
        }
        // `repository`: default when absent, the declared-default expression,
        // or provably the run's own repository. A literal that normalizes to
        // the run's repo selects the same target as omitting the input, so
        // skipping it would strand the checkout on the forge for no reason.
        let repository = resolve_step_input(step, "repository", &context_data);
        let repository_default = match &repository.value {
            // Evaluation failure means the input still holds its template;
            // the declared-default expression is provably the run's repo.
            None => {
                !repository.present
                    || repository
                        .raw
                        .as_deref()
                        .is_some_and(|r| r.trim() == "${{ github.repository }}")
            }
            // Post-evaluation `${{ github.repository }}` has already resolved
            // to the run's slug; what remains to prove is slug equality.
            // A literal URL for a different host is still "the same repo" only
            // when its origin is the run's forge.
            Some(value) => {
                let url_is_other_forge = url_origin(value)
                    .zip(forge_origin.as_ref())
                    .is_some_and(|(host, origin)| !same_origin(&host, origin));
                !url_is_other_forge
                    && normalize_checkout_repository(value)
                        .zip(run_repo.clone())
                        .is_some_and(|(a, b)| a.eq_ignore_ascii_case(&b))
            }
        };
        // `github-server-url`: default when absent, the declared default, or
        // provably the run's forge origin.
        let server_url = resolve_step_input(step, "github-server-url", &context_data);
        let server_url_default = match &server_url.value {
            None => {
                !server_url.present
                    || server_url
                        .raw
                        .as_deref()
                        .is_some_and(|r| r.trim() == "${{ github.server_url }}")
            }
            Some(value) => {
                value == "${{ github.server_url }}"
                    || forge_origin
                        .as_ref()
                        .is_some_and(|origin| same_origin(value, origin))
            }
        };
        // `ref`: default when absent, empty after evaluation, or provably one
        // of the run's refs/SHAs — `github.sha`, `github.ref`,
        // `github.event.pull_request.head.sha`, the snapshot commit — all
        // name the position the snapshot holds.
        let git_ref = resolve_step_input(step, "ref", &context_data);
        let ref_default = match &git_ref.value {
            None => !git_ref.present,
            Some(value) => {
                value.is_empty()
                    || default_ref_candidates
                        .iter()
                        .any(|candidate| value == candidate)
            }
        };
        // The snapshot holds exactly one commit and no tags. History the
        // snapshot cannot serve must stay on the forge: an explicit depth
        // other than the action default of 1, or requested tags. Values the
        // server cannot evaluate stay on the forge too.
        let history_compatible = ["fetch-depth", "fetch-tags"].iter().all(|name| {
            let input = resolve_step_input(step, name, &context_data);
            if !input.present {
                return true;
            }
            match &input.value {
                None => false,
                Some(value) => {
                    if *name == "fetch-depth" {
                        value == "1"
                    } else {
                        value.eq_ignore_ascii_case("false") || value == "0"
                    }
                }
            }
        });
        // A step that supplies its own `token` authenticates with a
        // credential the server cannot reproduce — often a secret that is
        // unprovable here. Redirecting would overwrite it with the snapshot's
        // runtime token and strip the workflow's authority, and
        // `actions/checkout` persists that token for later Git commands. Only
        // a provably empty token is the action's default.
        let token = resolve_step_input(step, "token", &context_data);
        let token_default = !token.present
            || token
                .value
                .as_deref()
                .is_some_and(|value| value.trim().is_empty());
        if !(repository_default
            && server_url_default
            && ref_default
            && history_compatible
            && token_default)
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

/// Point every checkout the snapshot could not serve at the engine's
/// anonymous forge relay (`forge_git_http`), with the job runtime token
/// pinned as its credential and recorded for claim-time re-mint.
///
/// Without a GitHub App or PAT, `github.token` is empty and `actions/checkout`
/// still writes `AUTHORIZATION: basic x-access-token:` — every forge fetch
/// then 401s into `could not read Username` (GitHub rejects the malformed
/// credential rather than treating it as anonymous). The relay strips auth
/// before forwarding, which is exactly anonymous public access.
///
/// Steps stay on the direct forge path when a real credential exists
/// (`has_forge_credential`), when `repository`/`github-server-url` cannot be
/// proven to name the configured forge (GHES hosts cannot ride the path-only
/// relay), or when the repository value is unprovable. SSH checkouts and
/// already-redirected steps are untouched.
///
/// Rewritten ids are appended to `preloop_snapshot_token_steps` — the list
/// the broker re-mints at claim — because the pinned credential is a
/// ~50-minute JWT and a queued job outlives it; an expired token turns the
/// relay into a 401 the step can never recover from.
pub fn reroute_forge_checkouts(
    message: &mut preloop_gha_protocol::azdo::AgentJobRequestMessage,
    base_url: &str,
    runtime_token: &str,
    has_forge_credential: bool,
) -> usize {
    if has_forge_credential {
        return 0;
    }
    let context_data = message.context_data.clone();
    let forge_origin = match run_forge_origin(&context_data) {
        Some(origin) => origin,
        None => return 0,
    };
    let mut pinned: Vec<String> = message
        .preloop_snapshot_token_steps
        .clone()
        .unwrap_or_default();
    let snapshot_steps: std::collections::HashSet<String> = pinned.iter().cloned().collect();
    let mut rerouted = 0;
    for step in &mut message.steps {
        let is_checkout = step
            .reference
            .as_ref()
            .and_then(|reference| reference.name.as_deref())
            .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"));
        if !is_checkout || snapshot_steps.contains(&step.id.to_string()) {
            continue;
        }
        // `repository`: must resolve to a provable owner/repo slug — literal
        // or fully evaluable. Absent means the run's own repo, which the
        // github context names.
        let repository = resolve_step_input(step, "repository", &context_data);
        let repo_slug = if repository.present {
            match repository
                .value
                .as_deref()
                .and_then(normalize_checkout_repository)
            {
                Some(slug) => slug,
                // A literal URL for a different host cannot ride the relay;
                // an unprovable expression stays on the forge.
                None => continue,
            }
        } else {
            match run_repository_slug(&context_data) {
                Some(slug) => slug,
                None => continue,
            }
        };
        // The URL form may name another forge; only the configured one can
        // relay. Slugs are unqualified, so they always target the run's forge.
        if let Some(raw) = &repository.value
            && let Some(origin) = url_origin(raw)
            && !same_origin(&origin, &forge_origin)
        {
            continue;
        }
        // `github-server-url`: absent/default/this-forge only.
        let server_url = resolve_step_input(step, "github-server-url", &context_data);
        let server_ok = match &server_url.value {
            None => !server_url.present,
            Some(value) => value == "${{ github.server_url }}" || same_origin(value, &forge_origin),
        };
        if !server_ok {
            continue;
        }
        // A step carrying its own credential (`token: ${{ secrets.X }}` or a
        // literal) already authenticates — rerouting would strip that token
        // and strand it on anonymous access. Whatever it contains, it is not
        // ours to replace.
        let token = resolve_step_input(step, "token", &context_data);
        if token.present {
            continue;
        }
        // SSH checkouts never hit the relay; leave them on the forge. A key
        // we cannot evaluate (`${{ secrets.DEPLOY_KEY }}`) is still a key:
        // rerouting would rewrite `github-server-url` to the engine, which
        // serves no SSH, breaking a checkout that authenticated fine before.
        // Only an absent or provably empty `ssh-key` can ride the relay.
        let ssh_key = resolve_step_input(step, "ssh-key", &context_data);
        let ssh_unused = !ssh_key.present
            || ssh_key
                .value
                .as_deref()
                .is_some_and(|value| value.trim().is_empty());
        if !ssh_unused {
            continue;
        }
        step.inputs
            .insert("repository".to_owned(), repo_slug.clone());
        step.inputs
            .insert("github-server-url".to_owned(), base_url.to_owned());
        // The relay authenticates callers with the job runtime token and
        // strips it before forwarding — a real forge credential would leak
        // to the engine were it pinned here, and an empty token makes
        // checkout write a header git refuses anonymously at the relay.
        step.inputs
            .insert("token".to_owned(), runtime_token.to_owned());
        pinned.push(step.id.to_string());
        rerouted += 1;
    }
    if !pinned.is_empty() {
        message.preloop_snapshot_token_steps = Some(pinned);
    }
    rerouted
}

/// Serve a snapshot bare repository through Git's read-only smart HTTP CGI.
/// Decode a smart-HTTP request body per its `Content-Encoding`.
///
/// Git gzip-encodes large POST bodies (observed on full-history fetches:
/// thousands of want lines, ~6 KB). `git http-backend` expects decoded
/// pkt-lines on stdin — a real front-end gunzips first — so without this
/// the backend dies with `bad line length character` and the client reports
/// `fatal: expected 'packfile'`. Small bodies (ls-refs, single-want fetches)
/// arrive plain and pass through untouched.
fn decode_git_request_body(
    body: &[u8],
    content_encoding: Option<&str>,
) -> Result<Vec<u8>, ApiError> {
    let gzipped = content_encoding
        .unwrap_or_default()
        .split(',')
        .any(|value| value.trim().eq_ignore_ascii_case("gzip"));
    if !gzipped {
        return Ok(body.to_vec());
    }
    use flate2::read::GzDecoder;
    use std::io::Read as _;
    let mut decoded = Vec::new();
    // Bound decoded output, not the compressed wire size. Git already caps
    // the request at MAX_GIT_REQUEST_BYTES before this runs; without a
    // second cap a tiny gzip expands past the same budget in RAM.
    let mut decoder = GzDecoder::new(body).take(MAX_GIT_REQUEST_BYTES as u64 + 1);
    decoder.read_to_end(&mut decoded).map_err(|error| {
        ApiError::bad_request(format!("invalid gzip Git request body: {error}"))
    })?;
    if decoded.len() > MAX_GIT_REQUEST_BYTES {
        return Err(ApiError::payload_too_large(
            "gzip Git request body exceeded size limit",
        ));
    }
    Ok(decoded)
}

/// Parse the `:run_id` route segment, tolerating an optional `.git` suffix.
///
/// Git clients address the same snapshot both ways: fetch goes to
/// `/snapshots/<run>/…` while git-lfs posts its batch to
/// `/snapshots/<run>.git/info/lfs/…`. `RunId` is a bare UUID that cannot
/// parse the suffixed form, so axum rejects the request before the handler
/// runs and LFS checkouts die with an opaque client-side error. Strip one
/// trailing `.git`, then parse strictly — anything else is still a 400.
fn snapshot_route_run_id(raw: &str) -> Result<RunId, ApiError> {
    let raw = raw.strip_suffix(".git").unwrap_or(raw);
    let uuid = raw
        .parse::<uuid::Uuid>()
        .map_err(|_| ApiError::bad_request("invalid snapshot run id"))?;
    Ok(RunId(uuid))
}
#[cfg(test)]
mod route_run_id_tests {
    use super::*;

    /// The LFS batch shape (`<run>.git/info/lfs/…`) must resolve to the same
    /// run as the fetch shape (`<run>/…`); otherwise axum rejects the batch
    /// before the handler runs and LFS checkouts fail opaquely.
    #[test]
    fn git_suffixed_run_id_parses_like_bare() {
        let bare = "658fa99d-0f4e-424a-a9df-549f4dbd6092";
        let suffixed = format!("{bare}.git");
        let from_bare = snapshot_route_run_id(bare).expect("bare UUID parses");
        let from_suffixed = snapshot_route_run_id(&suffixed).expect(".git suffix parses");
        assert_eq!(from_bare, from_suffixed);
        assert!(snapshot_route_run_id("not-a-uuid").is_err());
        assert!(snapshot_route_run_id("not-a-uuid.git").is_err());
    }
}

/// Serve a snapshot bare repository through Git's read-only smart HTTP CGI.
pub async fn snapshot_git_http(
    State(shared): State<Arc<SharedState>>,
    Path((run_id_raw, path)): Path<(String, String)>,
    request: Request,
) -> Result<Response<Body>, ApiError> {
    let run_id = snapshot_route_run_id(&run_id_raw)?;
    let authorization_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let token = authorization_header
        .as_deref()
        .and_then(snapshot_authorization_token);
    let storage_repository = match token {
        Some(token) => authorize_snapshot_token(&shared.state, &token, run_id).await,
        None => Err(ApiError::unauthorized(
            "snapshot Git authentication required",
        )),
    };
    // A bare 401 makes git fall back to Basic semantics and prompt for a
    // username ("could not read Username ... terminal prompts disabled").
    // The Bearer challenge tells git the failure is an authentication
    // rejection, so it reports it instead of prompting.
    let storage_repository = match storage_repository {
        Ok(repository) => repository,
        Err(error) if error.status() == StatusCode::UNAUTHORIZED => {
            return Ok(snapshot_unauthorized_response(error.message()));
        }
        Err(error) => return Err(error),
    };
    let method = request.method().clone();
    let query = request.uri().query().unwrap_or_default().to_owned();
    let lfs_object_oid = lfs_object_oid_from_path(&path);
    let valid_request = (method == axum::http::Method::GET
        && path == "info/refs"
        && query == "service=git-upload-pack")
        || (method == axum::http::Method::POST && path == "git-upload-pack")
        || (method == axum::http::Method::POST
            && (path == "info/lfs/objects/batch" || path == ".git/info/lfs/objects/batch"))
        || (method == axum::http::Method::GET && lfs_object_oid.is_some());
    if !valid_request {
        return Err(ApiError::not_found("snapshot Git endpoint not found"));
    }

    let project_root = &shared.state.state_dir;
    let repository = project_root.join(&storage_repository);
    if !repository.is_dir() {
        return Err(ApiError::not_found("checkout snapshot not found"));
    }
    if let Some(oid) = lfs_object_oid {
        return serve_lfs_object(&repository, oid).await;
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
        .map(str::to_owned)
        .or_else(|| {
            request
                .headers()
                .get("Git-Protocol")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        });
    let content_encoding = request
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let request_body = to_bytes(request.into_body(), MAX_GIT_REQUEST_BYTES)
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid Git request body: {error}")))?;
    let request_body = decode_git_request_body(&request_body, content_encoding.as_deref())?;
    if path == "info/lfs/objects/batch" || path == ".git/info/lfs/objects/batch" {
        // Snapshots carrying forge coordinates can pull a missing blob on
        // demand. Snapshots without an upstream (local workspaces with no
        // GitHub remote, legacy snapshots) keep the previous miss-is-404
        // behavior.
        let lfs_upstream: Option<(String, Option<u64>, bool)> = shared
            .state
            .backend
            .run_record(run_id)
            .await
            .ok()
            .and_then(|run| run.workspace_snapshot)
            .filter(|snapshot| snapshot.upstream_repository.is_some())
            .and_then(|snapshot| {
                snapshot.upstream_repository.clone().map(|upstream| {
                    (
                        upstream,
                        snapshot.upstream_repository_id,
                        // Unknown visibility is treated as public so an
                        // anonymous fetch is attempted — a private repo
                        // 404s into the same fallback either way, while
                        // a public repo with no configured credential
                        // would otherwise never fetch.
                        snapshot.upstream_private.unwrap_or(false),
                    )
                })
            });
        let lfs_fetch = lfs_upstream.as_ref().map(
            |(upstream_repository, upstream_repository_id, upstream_private)| LfsFetch {
                shared: shared.as_ref(),
                repository_dir: &repository,
                upstream_repository,
                upstream_repository_id: *upstream_repository_id,
                upstream_private: *upstream_private,
            },
        );
        let body = lfs_batch_response(
            &repository,
            run_id,
            authorization_header.as_deref(),
            &request_body,
            lfs_fetch,
        )
        .await?;
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Body::from(body.to_string()))
            .unwrap());
    }
    {
        let prefix_len = std::cmp::min(200, request_body.len());
        let hex_prefix: String = request_body[..prefix_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        let text_prefix = String::from_utf8_lossy(&request_body[..prefix_len]);
        tracing::debug!(
            %run_id,
            %method,
            %path,
            %query,
            ?content_type,
            ?git_protocol,
            body_len = request_body.len(),
            %hex_prefix,
            %text_prefix,
            "snapshot http-backend request"
        );
    }

    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", project_root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("REQUEST_METHOD", method.as_str())
        .env("PATH_INFO", format!("/{storage_repository}/{path}"))
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
) -> Result<String, ApiError> {
    let identity = match crate::auth::results_identity(state, token) {
        Ok(crate::auth::ResultsIdentity::Job(identity)) => identity,
        Ok(crate::auth::ResultsIdentity::System) => {
            return Err(ApiError::unauthorized("invalid snapshot Git token"));
        }
        Err(crate::auth::ResultsIdentityError::NotJob)
        | Err(crate::auth::ResultsIdentityError::MalformedJobSubject) => {
            return Err(ApiError::unauthorized(
                "snapshot Git token is not a job token",
            ));
        }
        Err(crate::auth::ResultsIdentityError::MalformedJob)
        | Err(crate::auth::ResultsIdentityError::MalformedScope) => {
            return Err(ApiError::unauthorized(
                "snapshot Git token lacks job result scope",
            ));
        }
        Err(crate::auth::ResultsIdentityError::Invalid) => {
            return Err(ApiError::unauthorized("invalid snapshot Git token"));
        }
    };

    // Backend: agent_job → request → run membership check (`job_requests`
    // rows, scoped to this run).
    let belongs_to_run = state
        .backend
        .attempt_in_run(run_id, &identity.plan_id, identity.job_id)
        .await
        .map_err(ApiError::from)?;
    if !belongs_to_run {
        return Err(ApiError::forbidden(
            "snapshot Git token does not belong to this run",
        ));
    }
    let run = state
        .backend
        .run_record(run_id)
        .await
        .map_err(ApiError::from)?;
    let snapshot = run
        .workspace_snapshot
        .as_ref()
        .ok_or_else(|| ApiError::not_found("checkout snapshot not found"))?;
    Ok(snapshot
        .storage_repository
        .clone()
        .unwrap_or_else(|| snapshot.repository.clone()))
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

/// Validate an `owner`/`repo` segment before it is interpolated into a relay
/// URL.
///
/// Axum percent-decodes path parameters, so a decoded value can carry `/`,
/// `?`, `#`, or a whole dot segment (`%2e%2e%2f..%2fusers%2fx%3F`). The URL
/// crate normalizes `.`/`..` while parsing, which would let such a segment
/// escape the configured path prefix — `forge_api_repo` has no literal tail
/// to anchor it. Anything but a plain name is refused.
fn valid_repo_segment(segment: &str) -> bool {
    !segment.is_empty()
        && !segment.starts_with('.')
        && segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Headers the forge relay copies inbound — everything else (notably
/// `authorization`, `host`, `cookie`) is dropped so the upstream request is
/// anonymous by construction.
///
/// `accept-encoding` is deliberately absent: the workspace's `reqwest` is
/// built without compression codecs, so an encoded body would be relayed
/// verbatim while the client cannot decode it. Leaving the header off makes
/// the forge answer identity.
const FORGE_RELAY_FORWARD_HEADERS: &[&str] = &[
    "accept",
    "content-type",
    "content-encoding",
    "git-protocol",
    "user-agent",
];

/// Headers the relay copies back out of the forge's response.
///
/// `content-encoding` is carried so an upstream that encodes anyway stays
/// decodable end to end (the snapshot path has the same hazard).
const FORGE_RELAY_RESPONSE_HEADERS: &[&str] = &[
    "content-type",
    "content-encoding",
    "git-protocol",
    "cache-control",
    "expires",
    "etag",
];

/// Authorize a forge-relay request with a live job runtime token.
///
/// Same credential posture as [`snapshot_git_http`]: a Basic `x-access-token`
/// (what `actions/checkout` sends) or a Bearer value must verify as a job
/// runtime JWT. Anonymous callers get the Bearer challenge so git reports
/// the rejection instead of prompting.
fn authorize_forge_relay(state: &AppState, request: &Request) -> Result<(), Box<Response<Body>>> {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(snapshot_authorization_token);
    match token.and_then(|token| state.job_uuid_from_token(&token)) {
        Some(_) => Ok(()),
        None => Err(Box::new(snapshot_unauthorized_response(
            "forge relay requires a job runtime token",
        ))),
    }
}

/// Concurrent forge-relay streams the engine will hold open, process-wide.
///
/// The relay is authenticated per job but otherwise unmetered: without a
/// bound one workflow could hold unlimited upstream connections and make the
/// engine proxy arbitrary public-Git traffic. Requests over the bound are
/// refused with 503 rather than queued. Response *bytes* are deliberately not
/// capped — a deep-history pack (`fetch-depth: 0`) is legitimately large, and
/// truncating it would break the checkouts the relay exists to serve.
const FORGE_RELAY_MAX_CONCURRENCY: usize = 64;

static FORGE_RELAY_PERMITS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(FORGE_RELAY_MAX_CONCURRENCY));

/// Relay one request to the configured forge with credentials stripped.
///
/// `upstream` is the full URL including path and query. Returns the upstream
/// status and response with only [`FORGE_RELAY_RESPONSE_HEADERS`] carried
/// over — `www-authenticate` deliberately stays behind so an upstream 401
/// (private repo) surfaces to the job as a fetch failure, not a git
/// credential prompt.
async fn forge_relay_forward(request: Request, upstream: &str) -> Result<Response<Body>, ApiError> {
    let permit = FORGE_RELAY_PERMITS
        .try_acquire()
        .map_err(|_| ApiError::service_unavailable("forge relay is at capacity; retry shortly"))?;
    let method = request.method().clone();
    let mut builder = LFS_FORGE_CLIENT.request(method.clone(), upstream);
    for name in FORGE_RELAY_FORWARD_HEADERS {
        if let Some(value) = request.headers().get(*name) {
            builder = builder.header(*name, value);
        }
    }
    let body = to_bytes(request.into_body(), MAX_GIT_REQUEST_BYTES)
        .await
        .map_err(|error| ApiError::bad_request(format!("invalid relay request body: {error}")))?;
    builder = builder.body(body);
    let response = builder
        .send()
        .await
        .map_err(|error| ApiError::internal(format!("forge relay request failed: {error}")))?;
    let status = response.status();
    let mut out = Response::builder().status(status.as_u16());
    for name in FORGE_RELAY_RESPONSE_HEADERS {
        if let Some(value) = response.headers().get(*name) {
            out = out.header(*name, value);
        }
    }
    let stream = response.bytes_stream();
    // The permit rides the response stream: it is released when the body is
    // dropped, which is exactly when the relay stops holding the upstream
    // connection.
    let stream = futures::StreamExt::map(stream, move |chunk| {
        let _keep_alive = &permit;
        chunk
    });
    out.body(Body::from_stream(stream))
        .map_err(|error| ApiError::internal(format!("failed to build relay response: {error}")))
}

/// Anonymous git smart-HTTP relay for checkouts the snapshot cannot serve.
///
/// Registered at `/{org}/{repo}/{info/refs|git-upload-pack|info/lfs/objects/batch}`.
/// `actions/checkout` takes the job's forge by default; when no GitHub
/// credential exists the run cannot authenticate anyway, so the step is
/// rewritten ([`reroute_forge_checkouts`]) to fetch through this relay with
/// the job runtime token. The relay authenticates the caller, strips that
/// credential, and forwards to the configured forge — which is exactly the
/// anonymous public-repo access GitHub's own runners need a token for only
/// to avoid a bogus `x-access-token:` header. Private repositories still
/// fail closed: the upstream 404 is relayed verbatim.
pub async fn forge_git_http(
    State(shared): State<Arc<SharedState>>,
    Path((org, repo)): Path<(String, String)>,
    request: Request,
) -> Result<Response<Body>, ApiError> {
    if let Err(response) = authorize_forge_relay(&shared.state, &request) {
        return Ok(*response);
    }
    let method = request.method().clone();
    let repo = repo.strip_suffix(".git").unwrap_or(&repo);
    // The routes pin three literal tails; the tail is read off the URI so a
    // percent-encoded org/repo segment cannot skew the split.
    let path = request.uri().path().splitn(4, '/').nth(3).unwrap_or("");
    let valid = (method == axum::http::Method::GET && path == "info/refs")
        || (method == axum::http::Method::POST && path == "git-upload-pack")
        || (method == axum::http::Method::POST && path == "info/lfs/objects/batch");
    if !valid || org.is_empty() || repo.is_empty() {
        return Err(ApiError::not_found("forge relay path not found"));
    }
    // Repo slugs are path segments — no traversal characters allowed through
    // to the upstream URL.
    if !valid_repo_segment(&org) || !valid_repo_segment(repo) {
        return Err(ApiError::bad_request("invalid repository path"));
    }
    let query = request.uri().query().unwrap_or_default();
    let upstream = format!(
        "{}/{}/{}/{}",
        shared.state.github_urls.server_url.trim_end_matches('/'),
        org,
        repo,
        path
    );
    let upstream = if query.is_empty() {
        upstream
    } else {
        format!("{upstream}?{query}")
    };
    forge_relay_forward(request, &upstream).await
}

/// REST archive endpoint for checkouts that cannot run Git.
///
/// `actions/checkout` derives `{github-server-url}/api/v3` as its API base
/// when the engine is the run's GHES-shaped server, and without a `git`
/// binary in the image it downloads the repository through
/// `GET /repos/{owner}/{repo}/tarball/{ref}` instead of fetching over Git.
/// Host images have `git`, so only container jobs on minimal images take
/// this path — and until now the engine answered it with
/// "not available on this endpoint" (the socket surface refused `/api/v3/*`
/// and no tarball route existed), which made every `container:` job whose
/// first step was a redirected `actions/checkout` fail even after the
/// container could reach the engine.
///
/// Two upstreams, mirroring the Git relay's split:
/// - `snapshots/{run-id}` archives the run's immutable local-workspace
///   snapshot (`git archive` of the same bare repository the snapshot Git
///   endpoint serves), authenticated with the pinned job runtime token.
/// - any other repository is an anonymous forge fetch, relayed to the
///   forge's own API host with the caller's credential stripped
///   ([`authorize_forge_relay`] pins the job runtime token; private
///   repositories keep failing closed on the upstream 404). The forge
///   answers with a redirect to its archive host, which the client follows;
///   jobs that reach this endpoint have egress to the public forge.
pub async fn forge_api_repo_tarball(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, git_ref)): Path<(String, String, String)>,
    request: Request,
) -> Result<Response<Body>, ApiError> {
    if !valid_repo_segment(&owner) || !valid_repo_segment(&repo) || !valid_archive_ref(&git_ref) {
        return Err(ApiError::bad_request("invalid archive path"));
    }
    if owner == "snapshots" {
        // The request body is not `Sync`: keep only the credential, and let the
        // request go before awaiting, or the handler future is not `Send`.
        let token = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(snapshot_authorization_token);
        drop(request);
        return snapshot_repo_tarball(&shared, &repo, &git_ref, token).await;
    }
    if let Err(response) = authorize_forge_relay(&shared.state, &request) {
        return Ok(*response);
    }
    // The API base is the forge's own api host (`api.github.com` for the
    // github.com default), never the server URL — the same rule the Git
    // relay and the repository lookup follow.
    let api_base = shared
        .state
        .github_urls
        .api_url
        .trim_end_matches('/')
        .to_owned();
    let upstream = format!("{api_base}/repos/{owner}/{repo}/tarball/{git_ref}");
    forge_relay_forward(request, &upstream).await
}

/// `git archive` of a run's local-workspace snapshot, shaped like the forge's
/// own tarball so `actions/checkout`'s extractor sees a single top-level
/// `{owner}-{repo}-{sha}/` directory.
async fn snapshot_repo_tarball(
    shared: &Arc<SharedState>,
    run_id_raw: &str,
    git_ref: &str,
    token: Option<String>,
) -> Result<Response<Body>, ApiError> {
    let run_id = snapshot_route_run_id(run_id_raw)?;
    let token =
        token.ok_or_else(|| ApiError::unauthorized("snapshot archive authentication required"))?;
    let storage_repository = match authorize_snapshot_token(&shared.state, &token, run_id).await {
        Ok(repository) => repository,
        Err(error) if error.status() == StatusCode::UNAUTHORIZED => {
            return Ok(snapshot_unauthorized_response(error.message()));
        }
        Err(error) => return Err(error),
    };
    let repository = shared.state.state_dir.join(&storage_repository);
    if !repository.is_dir() {
        return Err(ApiError::not_found("checkout snapshot not found"));
    }
    let archive = build_snapshot_archive(&repository, run_id_raw, git_ref).await?;
    Ok(archive)
}

/// Build the snapshot archive response: a gzipped `git archive` written to a
/// private temp file and streamed back.
///
/// Written to disk rather than held in memory: a snapshot can be a whole
/// repository tree, and the response is streamed to the client anyway.
async fn build_snapshot_archive(
    repository: &FsPath,
    run_id_raw: &str,
    git_ref: &str,
) -> Result<Response<Body>, ApiError> {
    let sha = resolve_archive_commit(repository, git_ref).await?;
    let prefix = archive_prefix("snapshots", run_id_raw, &sha);
    let archive_path = write_snapshot_archive(repository, git_ref, &prefix).await?;
    let file = tokio::fs::File::open(&archive_path)
        .await
        .map_err(|error| ApiError::internal(format!("failed to open snapshot archive: {error}")))?;
    // The handle keeps the data; the directory entry goes away now so a
    // client that never finishes reading leaves nothing behind.
    let _ = tokio::fs::remove_file(&archive_path).await;
    let metadata = file
        .metadata()
        .await
        .map_err(|error| ApiError::internal(format!("failed to stat snapshot archive: {error}")))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-gzip")
        .header(header::CONTENT_LENGTH, metadata.len())
        .body(Body::from_stream(ReaderStream::new(file)))
        .unwrap())
}

/// Run `git archive` for a snapshot into a private file and return its path.
async fn write_snapshot_archive(
    repository: &FsPath,
    git_ref: &str,
    prefix: &str,
) -> Result<PathBuf, ApiError> {
    let archive_path = repository
        .parent()
        .map(|parent| parent.join(format!("archive-{}.tar.gz", uuid::Uuid::new_v4())))
        .ok_or_else(|| ApiError::internal("snapshot repository has no parent directory"))?;
    let args = snapshot_archive_args(git_ref, prefix);
    let output = tokio::process::Command::new("git")
        .current_dir(repository)
        .args(args.iter().map(String::as_str))
        .arg("-o")
        .arg(&archive_path)
        .output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to run git archive: {error}")))?;
    if !output.status.success() {
        let _ = tokio::fs::remove_file(&archive_path).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ApiError::not_found(format!(
            "snapshot archive ref not found: {}",
            stderr.trim().lines().last().unwrap_or("")
        )));
    }
    Ok(archive_path)
}

/// `git archive` arguments for a snapshot tarball: gzip, GitHub's single
/// top-level directory, then the requested tree-ish.
fn snapshot_archive_args(git_ref: &str, prefix: &str) -> Vec<String> {
    vec![
        "archive".to_owned(),
        "--format=tar.gz".to_owned(),
        format!("--prefix={prefix}"),
        git_ref.to_owned(),
    ]
}

/// The forge's archive directory name: `{owner}-{repo}-{sha}`.
fn archive_prefix(owner: &str, repo: &str, sha: &str) -> String {
    format!("{owner}-{repo}-{sha}/")
}

/// Resolve the archive's commit so the prefix carries a sha, and so an
/// unprovable ref never reaches `git archive` as an argument.
async fn resolve_archive_commit(repository: &FsPath, git_ref: &str) -> Result<String, ApiError> {
    let output = tokio::process::Command::new("git")
        .current_dir(repository)
        .args([
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{git_ref}^{{commit}}"),
        ])
        .output()
        .await
        .map_err(|error| ApiError::internal(format!("failed to run git rev-parse: {error}")))?;
    if !output.status.success() {
        return Err(ApiError::not_found("snapshot archive ref not found"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Archive refs arrive in a URL path segment: allow sha(1)/sha256(256) hex and
/// ordinary ref names, never anything that could read as a `git` option or a
/// traversal.
fn valid_archive_ref(git_ref: &str) -> bool {
    !git_ref.is_empty()
        && git_ref.len() <= 1024
        && !git_ref.starts_with('-')
        && git_ref.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'/' | b'+' | b'-')
        })
        && !git_ref
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
}

/// Minimal `api/v3/repos/{owner}/{repo}` passthrough for checkout's
/// default-branch lookup.
///
/// A `github-server-url` rewrite makes checkout treat the engine as GHES, so
/// `urlHelper.getServerApiUrl` resolves to `{engine}/api/v3`. Only the one
/// shape a redirected `actions/checkout` issues (`GET /repos/{o}/{r}` via
/// `@actions/github`'s `getDefaultBranch`) is exposed — deeper API surface
/// stays off the relay.
pub async fn forge_api_repo(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    request: Request,
) -> Result<Response<Body>, ApiError> {
    if let Err(response) = authorize_forge_relay(&shared.state, &request) {
        return Ok(*response);
    }
    // Same segment rules as `forge_git_http`: without them a decoded
    // `%2e%2e%2f..%2fusers%2fx%3F` reaches a URL with no literal tail to
    // anchor it, and any job token could drive arbitrary GETs on the API host.
    if !valid_repo_segment(&owner) || !valid_repo_segment(&repo) {
        return Err(ApiError::bad_request("invalid repository path"));
    }
    // The API base is the forge's own api host (`api.github.com` for the
    // github.com default), never the server URL.
    let api_base = shared
        .state
        .github_urls
        .api_url
        .trim_end_matches('/')
        .to_owned();
    let upstream = format!("{api_base}/repos/{owner}/{repo}");
    forge_relay_forward(request, &upstream).await
}

#[cfg(test)]
mod auth_scoping_tests {
    use super::*;

    /// Decoded path parameters can carry separators or dot segments;
    /// `url`/`reqwest` normalize `..` while parsing, so a segment that slips
    /// through escapes the configured path prefix (SSRF).
    #[test]
    fn relay_segments_reject_traversal() {
        for bad in ["", ".", "..", ".git", "a/b", "a?b", "a#b", "..%2fusers"] {
            assert!(!valid_repo_segment(bad), "{bad:?} must be refused");
        }
        for good in ["owner", "repo.js", "some-repo_name", "node-24.x"] {
            assert!(valid_repo_segment(good), "{good:?} must be allowed");
        }
    }

    #[test]
    fn gzipped_git_body_decodes_and_plain_passes_through() {
        // Full-history fetches arrive gzip-encoded (thousands of want
        // lines); piping them raw to http-backend died with
        // `bad line length character` and the client saw
        // `fatal: expected 'packfile'`.
        let plain = b"0014command=ls-refs\n0000".to_vec();
        assert_eq!(decode_git_request_body(&plain, None).unwrap(), plain);
        assert_eq!(
            decode_git_request_body(&plain, Some("identity")).unwrap(),
            plain
        );
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let gzipped = encoder.finish().unwrap();
        assert!(gzipped.starts_with(&[0x1f, 0x8b]));
        assert_eq!(
            decode_git_request_body(&gzipped, Some("gzip")).unwrap(),
            plain
        );
        assert!(decode_git_request_body(&gzipped, None).unwrap() != plain);
        assert!(decode_git_request_body(b"not-gzip", Some("gzip")).is_err());
    }

    #[test]
    fn gzipped_git_body_rejects_decoded_over_limit() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        let chunk = [0u8; 64 * 1024];
        let mut remaining = MAX_GIT_REQUEST_BYTES + 1;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            encoder.write_all(&chunk[..n]).unwrap();
            remaining -= n;
        }
        let gzipped = encoder.finish().unwrap();
        let error = decode_git_request_body(&gzipped, Some("gzip")).unwrap_err();
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

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

    #[tokio::test]
    async fn results_snapshot_auth_preserves_malformed_claim_errors() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let job_id = uuid::Uuid::new_v4();
        let cases = [
            (
                "preloop-job-not-a-uuid".to_owned(),
                format!("Actions.Results:plan:{job_id}"),
                "snapshot Git token is not a job token",
            ),
            (
                format!("preloop-job-{job_id}"),
                "not-results".to_owned(),
                "snapshot Git token lacks job result scope",
            ),
            (
                format!("preloop-job-{job_id}"),
                format!("Actions.Results:plan:extra:{job_id}"),
                "snapshot Git token lacks job result scope",
            ),
        ];
        let run_id = "00000000-0000-0000-0000-000000000001".parse().unwrap();

        for (subject, scope, expected_message) in cases {
            let token = state
                .local_jwt(json!({
                    "sub": subject,
                    "scp": scope,
                }))
                .unwrap();
            let error = authorize_snapshot_token(&state, &token, run_id)
                .await
                .expect_err("malformed Results claims must be rejected");
            assert_eq!(error.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(error.message(), expected_message);
        }
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
    #[test]
    fn fetch_credential_is_scoped_to_the_upstream_origin() {
        let (key, value) =
            scoped_fetch_auth_header("https://ghe.example.com/owner/repo.git", "tok").unwrap();
        assert_eq!(key, "http.https://ghe.example.com/.extraHeader");
        assert!(value.starts_with("Authorization: basic "));
        // Ports are part of the origin and survive.
        let (key, _) =
            scoped_fetch_auth_header("https://ghe.example.com:8443/owner/repo.git", "tok").unwrap();
        assert_eq!(key, "http.https://ghe.example.com:8443/.extraHeader");
        // Userinfo in the URL must not leak into the config key.
        let (key, _) =
            scoped_fetch_auth_header("https://user:pass@ghe.example.com/owner/repo.git", "tok")
                .unwrap();
        assert_eq!(key, "http.https://ghe.example.com/.extraHeader");
    }

    #[test]
    fn fetch_credential_is_withheld_without_usable_origin() {
        for url in [
            "git@ghe.example.com:owner/repo.git",
            "/srv/git/owner/repo.git",
            "https:///owner/repo.git",
        ] {
            assert_eq!(
                scoped_fetch_auth_header(url, "tok"),
                None,
                "no credential without an HTTP(S) origin: {url}"
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
            .arg("-c")
            .arg("commit.gpgsign=false")
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
            "snapshot": null,
            "contextData": {
                // PipelineContextData wire form: {t:2, d:[{k,v}]} dict with
                // tagged values ({s}, {n}, {b}). A plain-object shorthand
                // deserializes to String("") and silently empties every
                // expression the tests rely on.
                "github": {"t": 2, "d": [
                    {"k": "repository", "v": {"s": "owner/repo"}},
                    {"k": "sha", "v": {"s": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
                    {"k": "ref", "v": {"s": "refs/heads/main"}},
                    {"k": "server_url", "v": {"s": "https://github.com"}},
                    {"k": "event", "v": {"t": 2, "d": [
                        {"k": "pull_request", "v": {"t": 2, "d": [
                            {"k": "head", "v": {"t": 2, "d": [
                                {"k": "sha", "v": {"s": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
                                {"k": "ref", "v": {"s": "feature"}}
                            ]}}
                        ]}}
                    ]}}
                ]},
                // Present from the start but empty until
                // `hydrate_needs_context` runs after the dependency
                // completes — the shape that must stay unprovable.
                "needs": {"t": 2, "d": []}
            }
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
            ..Default::default()
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
            redirect_count(serde_json::json!({"fetch-depth": "1"})),
            1,
            "an absent ref/repository is default-branch semantics"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "", "fetch-depth": "1"})),
            1,
            "an empty ref is default-branch semantics"
        );
    }

    /// The snapshot holds one commit and no tags, so history it cannot
    /// serve must stay on the forge: full or deeper fetches and tag
    /// requests are left for the forge, while the action default (depth 1,
    /// no tags) redirects.
    #[test]
    fn redirect_leaves_history_requests_on_the_forge() {
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-depth": "0"})),
            0,
            "unlimited history cannot come from a single-commit snapshot"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-depth": "5"})),
            0,
            "deeper history cannot come from a single-commit snapshot"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-tags": "true"})),
            0,
            "tags are absent from the snapshot and must come from the forge"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-depth": "1"})),
            1,
            "the action default depth matches the snapshot"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-tags": "false"})),
            1,
            "explicitly declining tags matches the snapshot"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"fetch-depth": "${{ inputs.depth }}"})),
            0,
            "an unevaluatable depth is unprovable and stays on the forge"
        );
    }

    /// Workflow-supplied credentials and dependency outputs keep the step on
    /// the forge: the redirect replaces the `token` input with the snapshot
    /// runtime token and pins the commit, neither of which is right when the
    /// workflow named its own authority or a `needs` output.
    #[test]
    fn redirect_keeps_workflow_credentials_and_needs_outputs() {
        assert_eq!(
            redirect_count(serde_json::json!({"token": "${{ secrets.DEPLOY_KEY }}"})),
            0,
            "an explicit secret token is the workflow's credential, not ours to replace"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"token": "ghp_literal"})),
            0,
            "a literal token authenticates the checkout and must survive"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"token": "${{ github.token }}"})),
            1,
            "the action's own empty-token default is still the default checkout"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "${{ needs.build.outputs.sha }}"})),
            0,
            "needs is empty at submission; its outputs are unprovable here"
        );
    }

    /// A literal `repository:` that names the run's own repo is still the
    /// default checkout — checkout resolves it to the same target as an
    /// omitted input, and the redirect used to skip it onto github.com where
    /// the empty token 401'd.
    #[test]
    fn redirect_primary_checkout_accepts_same_repo_literal() {
        assert_eq!(
            redirect_count(serde_json::json!({"repository": "owner/repo"})),
            1,
            "literal same-repo slug is the default target"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"repository": "Owner/Repo"})),
            1,
            "slug match is case-insensitive like GitHub's"
        );
        assert_eq!(
            redirect_count(serde_json::json!({
                "repository": "https://github.com/owner/repo"
            })),
            1,
            "full URL form of the same repo on the same forge"
        );
        assert_eq!(
            redirect_count(serde_json::json!({
                "repository": "https://git.example.com/owner/repo"
            })),
            0,
            "same slug on a different forge is not the default"
        );
    }

    /// Expressions that resolve to the run's commit/branch are provably the
    /// default checkout even though they are not literal.
    #[test]
    fn redirect_primary_checkout_evaluates_provable_expressions() {
        assert_eq!(
            redirect_count(serde_json::json!({
                "ref": "${{ github.event.pull_request.head.sha || '' }}"
            })),
            1,
            "a PR-head ref expression that resolves empty means the default"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "${{ github.sha }}"})),
            1,
            "ref: github.sha is the snapshot commit"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "${{ github.ref }}"})),
            1,
            "ref: github.ref names the pushed branch"
        );
        assert_eq!(
            redirect_count(serde_json::json!({
                "ref": "${{ inputs.missing-root }}"
            })),
            0,
            "roots outside context_data are unprovable"
        );
        assert_eq!(
            redirect_count(serde_json::json!({"ref": "refs/heads/other"})),
            0,
            "a literal non-default ref is an explicit target"
        );
    }

    /// Without a forge credential, checkouts the snapshot cannot serve are
    /// rewritten onto the engine's anonymous relay with the runtime token
    /// pinned; with a credential they stay on the direct forge path.
    #[test]
    fn reroute_forge_checkouts_routes_non_snapshot_checkouts() {
        let make = |inputs: serde_json::Value| {
            checkout_message(serde_json::json!([{
                "id": "00000000-0000-0000-0000-000000000010",
                "name": "checkout",
                "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
                "inputs": inputs,
                "continueOnError": false,
                "timeoutInMinutes": null
            }]))
        };
        // Deep same-repo fetch: the snapshot cannot serve it, the relay can.
        let mut message = make(serde_json::json!({"fetch-depth": "0"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            1
        );
        let step = &message.steps[0];
        assert_eq!(step.inputs["github-server-url"], "http://127.0.0.1:9090");
        assert_eq!(step.inputs["repository"], "owner/repo");
        assert_eq!(step.inputs["token"], "jwt");
        assert_eq!(
            message.preloop_snapshot_token_steps,
            Some(vec!["00000000-0000-0000-0000-000000000010".to_owned()]),
            "a rerouted step's token must be re-minted at claim like a snapshot step's"
        );

        // Cross-repo literal slug reroutes too.
        let mut message = make(serde_json::json!({"repository": "microsoft/vcpkg"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            1
        );
        assert_eq!(message.steps[0].inputs["repository"], "microsoft/vcpkg");
        assert_eq!(
            message.preloop_snapshot_token_steps,
            Some(vec!["00000000-0000-0000-0000-000000000010".to_owned()])
        );

        // A forge credential keeps the direct path.
        let mut message = make(serde_json::json!({"fetch-depth": "0"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", true),
            0
        );
        assert!(!message.steps[0].inputs.contains_key("github-server-url"));
        assert_eq!(
            message.preloop_snapshot_token_steps, None,
            "an untouched message must not gain pins"
        );

        // Unprovable repository expressions stay untouched.
        let mut message = make(serde_json::json!({"repository": "${{ inputs.repo }}"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            0
        );

        // A different forge's URL cannot ride the relay.
        let mut message = make(serde_json::json!({"repository": "https://git.example.com/o/r"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            0
        );

        // An SSH checkout stays on the forge even though its key cannot be
        // evaluated here; only a provably empty key is "no SSH".
        let mut message = make(serde_json::json!({"ssh-key": "${{ secrets.DEPLOY_KEY }}"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            0
        );
        assert!(!message.steps[0].inputs.contains_key("github-server-url"));
        let mut message = make(serde_json::json!({"ssh-key": ""}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            1
        );

        // A workflow-supplied token already authenticates the fetch.
        let mut message = make(serde_json::json!({"token": "${{ secrets.PAT }}"}));
        assert_eq!(
            reroute_forge_checkouts(&mut message, "http://127.0.0.1:9090", "jwt", false),
            0
        );
    }
}

#[cfg(test)]
mod lfs_batch_tests {
    use super::*;

    #[tokio::test]
    async fn lfs_batch_returns_download_action_for_present_objects() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("snapshot.git");
        let oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let object_path = lfs_object_path(&repository, oid);
        std::fs::create_dir_all(object_path.parent().unwrap()).unwrap();
        std::fs::write(&object_path, b"lfs-bytes").unwrap();
        let run_id = "00000000-0000-0000-0000-000000000001".parse().unwrap();

        let body = serde_json::json!({
            "operation": "download",
            "objects": [{"oid": oid, "size": 9}]
        })
        .to_string();
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            body.as_bytes(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(response["transfer"], "basic");
        assert_eq!(response["objects"][0]["oid"], oid);
        assert_eq!(response["objects"][0]["size"], 9);
        assert_eq!(
            response["objects"][0]["actions"]["download"]["href"],
            format!(
                "{}/snapshots/{run_id}/info/lfs/objects/{oid}",
                runner_base_url()
            )
        );
        assert_eq!(
            response["objects"][0]["actions"]["download"]["header"]["Authorization"],
            "Bearer job-token"
        );
    }

    #[tokio::test]
    async fn lfs_batch_returns_per_object_error_when_missing() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("snapshot.git");
        std::fs::create_dir_all(&repository).unwrap();
        let oid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let run_id = "00000000-0000-0000-0000-000000000002".parse().unwrap();
        let body = serde_json::json!({
            "operation": "download",
            "objects": [{"oid": oid, "size": 42}]
        })
        .to_string();

        let response = lfs_batch_response(&repository, run_id, None, body.as_bytes(), None)
            .await
            .unwrap();
        assert_eq!(response["objects"][0]["error"]["code"], 404);
        assert!(
            response["objects"][0]["actions"].is_null()
                || response["objects"][0].get("actions").is_none()
        );
    }

    #[tokio::test]
    async fn lfs_batch_rejects_uploads_on_read_only_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("snapshot.git");
        std::fs::create_dir_all(&repository).unwrap();
        let oid = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let run_id = "00000000-0000-0000-0000-000000000003".parse().unwrap();
        let body = serde_json::json!({
            "operation": "upload",
            "objects": [{"oid": oid, "size": 1}]
        })
        .to_string();

        let response = lfs_batch_response(&repository, run_id, None, body.as_bytes(), None)
            .await
            .unwrap();
        assert_eq!(response["objects"][0]["error"]["code"], 403);
    }
}

#[cfg(test)]
mod remote_checkout_cache_tests {
    use super::*;

    fn git(directory: &FsPath, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(directory)
            .env("GIT_AUTHOR_NAME", "preloop")
            .env("GIT_AUTHOR_EMAIL", "preloop@example.com")
            .env("GIT_COMMITTER_NAME", "preloop")
            .env("GIT_COMMITTER_EMAIL", "preloop@example.com")
            .output()
            .expect("git runs");
        assert!(
            status.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

    /// A bare `owner/repo.git` under `root`, carrying one commit, addressable
    /// as `{root}/owner/repo.git` — the shape the cache builds from
    /// `github.server_url`.
    fn upstream_repository(root: &FsPath) -> String {
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-b", "main"]);
        std::fs::write(work.join("file.txt"), "cached\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "cached commit"]);
        let bare = root.join("owner/repo.git");
        std::fs::create_dir_all(bare.parent().unwrap()).unwrap();
        git(
            root,
            &[
                "clone",
                "--bare",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        let head = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&work)
            .output()
            .unwrap();
        String::from_utf8(head.stdout).unwrap().trim().to_owned()
    }

    fn submission(commit: &str) -> preloop_gha_protocol::WorkflowSubmission {
        preloop_gha_protocol::WorkflowSubmission {
            repository: "owner/repo".to_owned(),
            event: "push".to_owned(),
            sha: commit.to_owned(),
            payload: serde_json::json!({
                "repository": {
                    "id": 4242,
                    "private": false,
                    "default_branch": "main"
                }
            }),
            ..Default::default()
        }
    }

    async fn fixture(
        mode: crate::config::CheckoutCacheMode,
    ) -> (tempfile::TempDir, Arc<SharedState>, String) {
        let temp = tempfile::tempdir().unwrap();
        let upstream_root = temp.path().join("upstream");
        std::fs::create_dir_all(&upstream_root).unwrap();
        let commit = upstream_repository(&upstream_root);
        let mut state = AppState::new(temp.path().join("state")).await.unwrap();
        state.checkout_cache = crate::config::CheckoutCacheConfig {
            mode,
            ..Default::default()
        };
        state.github_urls.server_url = upstream_root.to_string_lossy().to_string();
        let shared = Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        (temp, shared, commit)
    }

    /// Off is the default and must stay a pure no-op: nothing is fetched and no
    /// repository source is written to the state directory.
    #[tokio::test]
    async fn disabled_mode_caches_nothing() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::Off).await;
        let run_id: RunId = "11111111-1111-4111-8111-111111111111".parse().unwrap();
        let snapshot =
            create_remote_checkout_snapshot(&shared, &submission(&commit), run_id, &commit)
                .await
                .expect("disabled mode is not an error");
        assert!(snapshot.is_none());
        assert!(!shared.state.state_dir.join("checkout-cache").exists());
    }

    /// One upstream fetch per run attempt yields a servable snapshot of exactly
    /// the requested commit, and every later job in the same run reuses it.
    #[tokio::test]
    async fn run_scoped_mode_caches_the_requested_commit() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::RunScoped).await;
        let run_id: RunId = "22222222-2222-4222-8222-222222222222".parse().unwrap();
        let submission = submission(&commit);

        let snapshot = create_remote_checkout_snapshot(&shared, &submission, run_id, &commit)
            .await
            .expect("cache population succeeds")
            .expect("run-scoped mode caches the commit");
        assert_eq!(snapshot.commit_sha, commit);
        assert_eq!(snapshot.source, SnapshotSource::RemoteRunScoped);
        assert_eq!(
            snapshot.repository,
            format!("snapshots/{run_id}"),
            "checkout-cache snapshots are served on the same authenticated /snapshots/<run> route as local ones"
        );
        assert_eq!(snapshot.default_branch.as_deref(), Some("main"));
        let namespace = snapshot
            .cache_namespace
            .clone()
            .expect("namespace recorded");
        assert_eq!(namespace.repository_id, "4242");
        assert!(namespace.tenant_id.is_none());

        let repository = shared
            .state
            .state_dir
            .join(snapshot.storage_repository.as_deref().unwrap());
        assert!(repository.is_dir(), "cache repository exists");
        let present = std::process::Command::new("git")
            .args([
                "--git-dir",
                repository.to_str().unwrap(),
                "cat-file",
                "-e",
                &commit,
            ])
            .status()
            .unwrap();
        assert!(present.success(), "the requested commit is in the cache");

        // A second job in the same run resolves the same repository instead of
        // fetching again.
        let reused = create_remote_checkout_snapshot(&shared, &submission, run_id, &commit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reused.storage_repository, snapshot.storage_repository);
    }

    /// Release is not deletion: the retry window keeps the objects, and the
    /// sweep collects them only once the configured retention has elapsed.
    #[tokio::test]
    async fn released_run_cache_survives_retention_then_is_pruned() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::RunScoped).await;
        let run_id: RunId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
        let snapshot =
            create_remote_checkout_snapshot(&shared, &submission(&commit), run_id, &commit)
                .await
                .unwrap()
                .unwrap();
        let repository = shared
            .state
            .state_dir
            .join(snapshot.storage_repository.as_deref().unwrap());

        release_remote_checkout_snapshot(&shared.state.state_dir, &snapshot, run_id).await;
        assert!(repository.is_dir(), "release must not delete the objects");

        let keep = crate::config::CheckoutCacheConfig {
            run_retention_seconds: 3_600,
            ..shared.state.checkout_cache.clone()
        };
        prune_checkout_cache(&shared.state.state_dir, &keep).await;
        assert!(
            repository.is_dir(),
            "a cache inside its retention window must be kept"
        );

        let expire = crate::config::CheckoutCacheConfig {
            run_retention_seconds: 0,
            ..shared.state.checkout_cache.clone()
        };
        prune_checkout_cache(&shared.state.state_dir, &expire).await;
        assert!(
            !repository.exists(),
            "an expired released cache must be collected"
        );
    }

    /// Repository mode keeps the shared objects across runs and only drops the
    /// finished run's ref.
    #[tokio::test]
    async fn repository_mode_keeps_objects_after_release() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::Repository).await;
        let run_id: RunId = "44444444-4444-4444-8444-444444444444".parse().unwrap();
        let snapshot =
            create_remote_checkout_snapshot(&shared, &submission(&commit), run_id, &commit)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(snapshot.source, SnapshotSource::RemoteRepository);
        let repository = shared
            .state
            .state_dir
            .join(snapshot.storage_repository.as_deref().unwrap());

        release_remote_checkout_snapshot(&shared.state.state_dir, &snapshot, run_id).await;
        assert!(repository.is_dir(), "repository caches persist across runs");
        let refs = std::process::Command::new("git")
            .args([
                "--git-dir",
                repository.to_str().unwrap(),
                "for-each-ref",
                "--format=%(refname)",
            ])
            .output()
            .unwrap();
        let refs = String::from_utf8(refs.stdout).unwrap();
        assert!(
            !refs.contains(&run_id.to_string()),
            "the finished run's ref is dropped: {refs}"
        );
        let present = std::process::Command::new("git")
            .args([
                "--git-dir",
                repository.to_str().unwrap(),
                "cat-file",
                "-e",
                &commit,
            ])
            .status()
            .unwrap();
        assert!(present.success(), "shared objects outlive the run");
    }

    /// A repository whose numeric identity the event never carried is not
    /// cacheable: without it the security namespace cannot be derived, so the
    /// job falls back to a direct forge checkout.
    #[tokio::test]
    async fn missing_repository_identity_is_not_cached() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::RunScoped).await;
        let run_id: RunId = "55555555-5555-4555-8555-555555555555".parse().unwrap();
        let mut submission = submission(&commit);
        submission.payload = serde_json::json!({ "repository": { "private": false } });
        assert!(
            create_remote_checkout_snapshot(&shared, &submission, run_id, &commit)
                .await
                .unwrap()
                .is_none()
        );
        // A ref that is not an immutable object id is equally ineligible.
        let identified = super::remote_checkout_cache_tests::submission(&commit);
        assert!(
            create_remote_checkout_snapshot(&shared, &identified, run_id, "main")
                .await
                .unwrap()
                .is_none()
        );
    }
    fn upload_pack_want_request(sha: &str) -> Vec<u8> {
        let body = format!("want {sha} multi_ack\n");
        let mut request = format!("{:04x}{body}", body.len() + 4).into_bytes();
        request.extend_from_slice(b"00000009done\n");
        request
    }

    fn upload_pack_verdict(repository: &FsPath, sha: &str) -> String {
        let mut child = std::process::Command::new("git")
            .args(["--git-dir", repository.to_str().unwrap(), "upload-pack"])
            .arg(repository)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        use std::io::Write as _;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&upload_pack_want_request(sha))
            .unwrap();
        let output = child.wait_with_output().unwrap();
        let mut combined = output.stdout;
        combined.extend_from_slice(&output.stderr);
        String::from_utf8_lossy(&combined).into_owned()
    }

    /// Repository mode shares one repo across runs, so serving must not
    /// expose more than the requesting run's tip: refs stay hidden and only
    /// tip wants are honored. Objects left behind by a released run are
    /// present on disk but unreachable — a want for one must be refused.
    #[tokio::test]
    async fn repository_mode_serves_only_run_tips() {
        let (_temp, shared, commit) = fixture(crate::config::CheckoutCacheMode::Repository).await;
        let run_id: RunId = "99999999-9999-4999-8999-999999999999".parse().unwrap();
        let snapshot =
            create_remote_checkout_snapshot(&shared, &submission(&commit), run_id, &commit)
                .await
                .unwrap()
                .expect("repository mode caches the commit");
        let repository = shared
            .state
            .state_dir
            .join(snapshot.storage_repository.as_deref().unwrap());
        // While the run is live its tip is fetchable.
        assert!(
            upload_pack_verdict(&repository, &commit).contains("ACK"),
            "the live run tip must stay fetchable"
        );
        // Release drops the run's ref but the objects remain on disk.
        release_remote_checkout_snapshot(&shared.state.state_dir, &snapshot, run_id).await;
        assert!(
            !upload_pack_verdict(&repository, &commit).contains("ACK"),
            "a released run's retained objects must not be fetchable"
        );
    }
    fn sized_cache_entry(root: &FsPath, relative: &str) -> PathBuf {
        let dir = root.join(relative);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("objects.bin"), vec![0u8; 100]).unwrap();
        dir
    }

    fn sweep_config() -> crate::config::CheckoutCacheConfig {
        crate::config::CheckoutCacheConfig {
            mode: crate::config::CheckoutCacheMode::RunScoped,
            run_retention_seconds: 3_600,
            repository_retention_seconds: 7 * 24 * 60 * 60,
            max_bytes: 1,
        }
    }

    /// The size ceiling must not break a live checkout: a run cache without
    /// a release marker may still be serving a job, so the sweep spares it
    /// even when the ceiling is exceeded.
    #[tokio::test]
    async fn size_sweep_spares_unreleased_run_cache() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let entry = sized_cache_entry(&state_dir, "checkout-cache/runs/live.git");
        prune_checkout_cache(&state_dir, &sweep_config()).await;
        assert!(entry.is_dir(), "a live run cache must survive the ceiling");
    }

    /// A released cache inside its retention window is still reclaimable by
    /// the ceiling: its run is terminal, so nothing can be fetching from it.
    #[tokio::test]
    async fn size_sweep_evicts_released_run_cache() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let entry = sized_cache_entry(&state_dir, "checkout-cache/runs/done.git");
        std::fs::write(entry.join(RELEASED_MARKER), "1").unwrap();
        prune_checkout_cache(&state_dir, &sweep_config()).await;
        assert!(
            !entry.exists(),
            "a released cache must yield to the ceiling"
        );
    }

    /// A shared repository that still publishes a run ref is live no matter
    /// its idle age; once the last ref is released the ceiling may take it.
    #[tokio::test]
    async fn size_sweep_spares_repo_with_live_run_ref() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let entry = state_dir.join("checkout-cache/repositories/shared.git");
        std::fs::create_dir_all(&entry).unwrap();
        let status = std::process::Command::new("git")
            .args(["init", "--bare"])
            .arg(&entry)
            .output()
            .unwrap();
        assert!(status.status.success());
        let probe = entry.join("probe.bin");
        std::fs::write(&probe, b"live").unwrap();
        let run_ref = "refs/preloop/runs/99999999-9999-4999-8999-999999999999";
        let hash = std::process::Command::new("git")
            .args(["--git-dir", entry.to_str().unwrap(), "hash-object", "-w"])
            .arg(&probe)
            .output()
            .unwrap();
        assert!(hash.status.success());
        let object = String::from_utf8(hash.stdout).unwrap().trim().to_owned();
        let status = std::process::Command::new("git")
            .args([
                "--git-dir",
                entry.to_str().unwrap(),
                "update-ref",
                run_ref,
                object.as_str(),
            ])
            .output()
            .unwrap();
        assert!(status.status.success());

        prune_checkout_cache(&state_dir, &sweep_config()).await;
        assert!(entry.is_dir(), "a repo with a live run ref must survive");

        let status = std::process::Command::new("git")
            .args([
                "--git-dir",
                entry.to_str().unwrap(),
                "update-ref",
                "-d",
                run_ref,
            ])
            .output()
            .unwrap();
        assert!(status.status.success());
        prune_checkout_cache(&state_dir, &sweep_config()).await;
        assert!(
            !entry.exists(),
            "an unreferenced repo must yield to the ceiling"
        );
    }
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockForge {
        base: String,
        batch_hits: Arc<AtomicUsize>,
        authed_downloads: Arc<AtomicUsize>,
    }

    /// A forge where the repository was renamed: `GET /repositories/{id}`
    /// reports the canonical `full_name`, and the LFS batch endpoint exists
    /// only under that canonical slug. A fetch driven by the stale stored
    /// slug alone would 404; the numeric ID must resolve the new name first.
    async fn mock_renamed_forge(repo_id: u64, canonical_slug: &str, blob: Vec<u8>) -> MockForge {
        use axum::extract::{Extension, Path};
        use axum::routing::{get, post};

        #[derive(Clone)]
        struct RepoId(u64);
        #[derive(Clone)]
        struct CanonicalSlug(String);

        let batch_hits = Arc::new(AtomicUsize::new(0));
        let authed_downloads = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let canonical = canonical_slug.to_owned();
        let router = axum::Router::new()
            .route(
                "/repositories/:id",
                get(
                    |Path(id): Path<u64>,
                     Extension(repo_id): Extension<RepoId>,
                     Extension(canonical): Extension<CanonicalSlug>| async move {
                        assert_eq!(id, repo_id.0);
                        axum::Json(serde_json::json!({
                            "id": repo_id.0,
                            "full_name": canonical.0,
                            "private": false,
                        }))
                    },
                ),
            )
            .route(
                &format!("/{canonical}.git/info/lfs/objects/batch"),
                post(
                    |Extension(base): Extension<String>,
                     Extension(hits): Extension<BatchHits>,
                     body: String| async move {
                        hits.0.fetch_add(1, Ordering::SeqCst);
                        let request: serde_json::Value = serde_json::from_str(&body).unwrap();
                        let oid = request["objects"][0]["oid"].as_str().unwrap().to_owned();
                        let size = request["objects"][0]["size"].as_u64().unwrap_or(0);
                        axum::Json(serde_json::json!({
                            "objects": [{
                                "oid": oid,
                                "size": size,
                                "actions": {
                                    "download": { "href": format!("{base}/blobs/{oid}") }
                                }
                            }]
                        }))
                    },
                ),
            )
            .route(
                "/blobs/:oid",
                get(
                    |Extension(authed): Extension<AuthedDownloads>,
                     Extension(blob): Extension<Vec<u8>>| async move {
                        authed.0.fetch_add(1, Ordering::SeqCst);
                        blob
                    },
                ),
            )
            .layer(Extension(base.clone()))
            .layer(Extension(RepoId(repo_id)))
            .layer(Extension(CanonicalSlug(canonical)))
            .layer(Extension(BatchHits(batch_hits.clone())))
            .layer(Extension(AuthedDownloads(authed_downloads.clone())))
            .layer(Extension(blob));
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        MockForge {
            base,
            batch_hits,
            authed_downloads,
        }
    }

    /// A minimal forge LFS endpoint: answers batch with a download address
    /// for the requested oid and serves `blob` there. Anonymous, like a
    /// public repository. `href_base` overrides where downloads point, so a
    /// test can aim them at a different host.
    ///
    /// Counter layers use distinct wrapper types: two `Extension<Arc<…>>`
    /// layers would collapse into one map entry and alias the counters.
    #[derive(Clone)]
    struct BatchHits(Arc<AtomicUsize>);
    #[derive(Clone)]
    struct AuthedDownloads(Arc<AtomicUsize>);

    async fn mock_forge(blob: Vec<u8>, href_base: Option<String>) -> MockForge {
        use axum::extract::Extension;
        use axum::routing::{get, post};

        let batch_hits = Arc::new(AtomicUsize::new(0));
        let authed_downloads = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = axum::Router::new()
            .route(
                "/owner/repo.git/info/lfs/objects/batch",
                post(
                    |Extension(base): Extension<String>,
                     Extension(hits): Extension<BatchHits>,
                     Extension(href_base): Extension<Option<String>>,
                     body: String| async move {
                        hits.0.fetch_add(1, Ordering::SeqCst);
                        let request: serde_json::Value = serde_json::from_str(&body).unwrap();
                        let oid = request["objects"][0]["oid"].as_str().unwrap().to_owned();
                        let size = request["objects"][0]["size"].as_u64().unwrap_or(0);
                        axum::Json(serde_json::json!({
                            "objects": [{
                                "oid": oid,
                                "size": size,
                                "actions": {
                                    "download": { "href": format!("{}/blobs/{oid}", href_base.as_deref().unwrap_or(&base)), "header": { "Authorization": "Bearer forge-issued" } }
                                }
                            }]
                        }))
                    },
                ),
            )
            .route(
                "/blobs/:oid",
                get(
                    |headers: axum::http::HeaderMap,
                     Extension(authed): Extension<AuthedDownloads>,
                     Extension(blob): Extension<Vec<u8>>| async move {
                        if headers.contains_key(axum::http::header::AUTHORIZATION) {
                            authed.0.fetch_add(1, Ordering::SeqCst);
                        }
                        blob
                    },
                ),
            )
            .layer(Extension(base.clone()))
            .layer(Extension(href_base))
            .layer(Extension(BatchHits(batch_hits.clone())))
            .layer(Extension(AuthedDownloads(authed_downloads.clone())))
            .layer(Extension(blob));
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        MockForge {
            base,
            batch_hits,
            authed_downloads,
        }
    }

    fn lfs_blob_oid(contents: &[u8]) -> String {
        use sha2::Digest;
        format!("{:x}", sha2::Sha256::digest(contents))
    }

    async fn lfs_state(
        temp: &tempfile::TempDir,
        forge_base: &str,
        max_bytes: u64,
    ) -> Arc<SharedState> {
        let mut state = AppState::new(temp.path().join("state")).await.unwrap();
        state.checkout_cache = crate::config::CheckoutCacheConfig {
            mode: crate::config::CheckoutCacheMode::RunScoped,
            max_bytes,
            ..Default::default()
        };
        state.github_urls.server_url = forge_base.to_owned();
        Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        })
    }

    fn lfs_batch_body(oid: &str, size: u64) -> Vec<u8> {
        serde_json::json!({
            "operation": "download",
            "objects": [{ "oid": oid, "size": size }]
        })
        .to_string()
        .into_bytes()
    }

    /// First request pulls the blob from the forge into the run cache and
    /// serves it; the second request is answered from the store without
    /// touching the forge again.
    #[tokio::test]
    async fn lfs_miss_populates_cache_then_serves_locally() {
        let contents = b"golden-bytes".to_vec();
        let oid = lfs_blob_oid(&contents);
        let forge = mock_forge(contents.clone(), None).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, u64::MAX).await;
        let run_id: RunId = "66666666-6666-4666-8666-666666666666".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let fetch = || LfsFetch {
            shared: shared.as_ref(),
            repository_dir: repository.as_path(),
            upstream_repository: "owner/repo",
            upstream_repository_id: None,
            upstream_private: false,
        };
        let body = lfs_batch_body(&oid, contents.len() as u64);
        let first = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(fetch()),
        )
        .await
        .unwrap();
        assert_eq!(
            first["objects"][0]["actions"]["download"]["href"],
            format!(
                "{}/snapshots/{run_id}/info/lfs/objects/{oid}",
                runner_base_url()
            )
        );
        assert!(
            forge.authed_downloads.load(Ordering::SeqCst) >= 1,
            "the forge-issued header must ride the same-host download"
        );

        let second = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(fetch()),
        )
        .await
        .unwrap();
        assert!(second["objects"][0]["actions"]["download"]["href"].is_string());
        assert_eq!(
            forge.batch_hits.load(Ordering::SeqCst),
            1,
            "the cached blob must not trigger another upstream fetch"
        );
    }

    /// Bytes that do not hash to the requested oid are rejected and never
    /// stored: the answer stays per-object not-found.
    #[tokio::test]
    async fn lfs_corrupt_upstream_is_rejected() {
        let oid = lfs_blob_oid(b"expected-bytes");
        let forge = mock_forge(b"tampered-bytes".to_vec(), None).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, u64::MAX).await;
        let run_id: RunId = "77777777-7777-4777-8777-777777777777".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let body = lfs_batch_body(&oid, 14);
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(LfsFetch {
                shared: shared.as_ref(),
                repository_dir: repository.as_path(),
                upstream_repository: "owner/repo",
                upstream_repository_id: None,
                upstream_private: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["objects"][0]["error"]["code"], 404);
        assert!(!lfs_object_path(&repository, &oid).exists());
    }

    /// A private repository with no usable credential fails closed: no
    /// upstream request is attempted and the answer is not-found.
    #[tokio::test]
    async fn lfs_private_without_credential_is_not_fetched() {
        let contents = b"private-bytes".to_vec();
        let oid = lfs_blob_oid(&contents);
        let forge = mock_forge(contents, None).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, u64::MAX).await;
        let run_id: RunId = "88888888-8888-4888-8888-888888888888".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let body = lfs_batch_body(&oid, 13);
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(LfsFetch {
                shared: shared.as_ref(),
                repository_dir: repository.as_path(),
                upstream_repository: "owner/repo",
                upstream_repository_id: None,
                upstream_private: true,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["objects"][0]["error"]["code"], 404);
        assert_eq!(forge.batch_hits.load(Ordering::SeqCst), 0);
    }

    /// A snapshot recorded before a rename still fetches: the stale stored
    /// slug (`oldowner/repo`) cannot serve the batch, but the numeric ID
    /// resolves the canonical slug (`newowner/repo`) before any forge call.
    /// The blob lands in the run cache and the answer carries a download
    /// action instead of a 404.
    #[tokio::test]
    async fn lfs_fetch_resolves_canonical_slug_from_repository_id() {
        let contents = b"renamed-bytes".to_vec();
        let oid = lfs_blob_oid(&contents);
        let forge = mock_renamed_forge(12345, "newowner/repo", contents.clone()).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, u64::MAX).await;
        let run_id: RunId = "99999999-9999-4999-8999-999999999999".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let body = lfs_batch_body(&oid, contents.len() as u64);
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(LfsFetch {
                shared: shared.as_ref(),
                repository_dir: repository.as_path(),
                upstream_repository: "oldowner/repo",
                upstream_repository_id: Some(12345),
                upstream_private: false,
            }),
        )
        .await
        .unwrap();
        assert!(
            response["objects"][0]["actions"]["download"]["href"].is_string(),
            "the renamed repository must serve the blob, not 404"
        );
        assert_eq!(
            forge.batch_hits.load(Ordering::SeqCst),
            1,
            "the batch must have been served under the canonical slug"
        );
        assert!(
            lfs_object_path(&repository, &oid).exists(),
            "the fetched blob must be cached for subsequent requests"
        );
    }
    /// A blob server on another host, answering openly like presigned
    /// storage. Records whether the caller presented any Authorization.
    async fn open_blob_server(blob: Vec<u8>) -> (String, Arc<AtomicUsize>) {
        use axum::extract::Extension;
        use axum::http::HeaderMap;
        use axum::routing::get;

        let authed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let router = axum::Router::new()
            .route(
                "/blobs/:oid",
                get(
                    |headers: HeaderMap,
                     Extension(authed): Extension<Arc<AtomicUsize>>,
                     Extension(blob): Extension<Vec<u8>>| async move {
                        if headers.contains_key(axum::http::header::AUTHORIZATION) {
                            authed.fetch_add(1, Ordering::SeqCst);
                        }
                        blob
                    },
                ),
            )
            .layer(Extension(authed.clone()))
            .layer(Extension(blob));
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (base, authed)
    }

    /// A forge-named download on another host must not receive the forge
    /// credential: presigned-style URLs carry their own authorization, and
    /// anything else must fail closed rather than leak the token.
    #[tokio::test]
    async fn lfs_cross_host_download_sends_no_credential() {
        let contents = b"external-bytes".to_vec();
        let oid = lfs_blob_oid(&contents);
        let (blob_base, authed) = open_blob_server(contents.clone()).await;
        let forge = mock_forge(vec![], Some(blob_base)).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, u64::MAX).await;
        let run_id: RunId = "99999999-9999-4999-8999-999999999999".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let body = lfs_batch_body(&oid, contents.len() as u64);
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(LfsFetch {
                shared: shared.as_ref(),
                repository_dir: repository.as_path(),
                upstream_repository: "owner/repo",
                upstream_repository_id: None,
                upstream_private: false,
            }),
        )
        .await
        .unwrap();
        assert!(response["objects"][0]["actions"]["download"]["href"].is_string());
        assert_eq!(
            std::fs::read(lfs_object_path(&repository, &oid)).unwrap(),
            contents
        );
        assert_eq!(
            authed.load(Ordering::SeqCst),
            0,
            "no Authorization header may leave the engine for another host"
        );
    }

    /// A batch whose claimed sizes already exceed the ceiling is not
    /// fetched at all: the forge is never contacted.
    #[tokio::test]
    async fn lfs_populate_respects_the_cache_ceiling() {
        let contents = b"ceiling-bytes".to_vec();
        let oid = lfs_blob_oid(&contents);
        let forge = mock_forge(contents, None).await;
        let temp = tempfile::tempdir().unwrap();
        let shared = lfs_state(&temp, &forge.base, 1).await;
        let run_id: RunId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse().unwrap();
        let repository = shared
            .state
            .state_dir
            .join(format!("checkout-cache/runs/{run_id}.git"));
        std::fs::create_dir_all(&repository).unwrap();

        let body = lfs_batch_body(&oid, 1_000_000);
        let response = lfs_batch_response(
            &repository,
            run_id,
            Some("Bearer job-token"),
            &body,
            Some(LfsFetch {
                shared: shared.as_ref(),
                repository_dir: repository.as_path(),
                upstream_repository: "owner/repo",
                upstream_repository_id: None,
                upstream_private: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(response["objects"][0]["error"]["code"], 404);
        assert_eq!(forge.batch_hits.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod snapshot_sweep_tests {
    use super::*;

    /// Seed one run row through the real submit path and return its id. The
    /// sweep reads status/completion from the control database, so a run that
    /// should look terminal is forced terminal with one direct write — the
    /// `test_db_mutate` escape hatch exists for exactly this, since no
    /// `ControlBackend` command expresses a forced-terminal run.
    async fn seed_run(
        shared: &Arc<SharedState>,
        completed_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> RunId {
        let submission = WorkflowSubmission {
            workflow_yaml:
                "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                    .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(".github/workflows/ci.yml".to_owned()),
            sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
            ..Default::default()
        };
        let accepted = crate::submit_run_inner(shared, submission).await.unwrap();
        if let Some(at) = completed_at {
            let run_key = accepted.run_id.0.to_string();
            let completed_us = at.timestamp_micros();
            shared
                .state
                .test_db_mutate(move |db| {
                    db.execute(
                        "UPDATE runs SET status = 'completed', conclusion = 'success', \
                         completed_at = ?2 WHERE run_id = ?1",
                        rusqlite::params![run_key, completed_us],
                    )
                })
                .await
                .unwrap();
        }
        accepted.run_id
    }

    async fn fixture(retention_seconds: u64) -> (tempfile::TempDir, Arc<SharedState>) {
        let temp = tempfile::tempdir().unwrap();
        let mut state = AppState::new(temp.path().join("state")).await.unwrap();
        state.snapshot_retention_seconds = retention_seconds;
        let shared = Arc::new(SharedState {
            state,
            shutdown: tokio_util::sync::CancellationToken::new(),
        });
        (temp, shared)
    }

    fn snapshot_dir(shared: &SharedState, run_id: RunId) -> PathBuf {
        let dir = shared
            .state
            .state_dir
            .join("snapshots")
            .join(run_id.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A restart inside the retention window used to orphan the snapshot
    /// forever: the discard timer is in-process. The startup sweep must
    /// collect terminal runs past retention and snapshots whose run record
    /// never restored, while sparing live runs.
    #[tokio::test]
    async fn sweep_collects_orphans_and_spares_live_runs() {
        let (_temp, shared) = fixture(60).await;

        let live = seed_run(&shared, None).await;
        let terminal_old = seed_run(
            &shared,
            Some(chrono::Utc::now() - chrono::Duration::hours(1)),
        )
        .await;
        let terminal_fresh = seed_run(&shared, Some(chrono::Utc::now())).await;
        let unknown = RunId::new();
        let live_dir = snapshot_dir(&shared, live);
        let old_dir = snapshot_dir(&shared, terminal_old);
        let fresh_dir = snapshot_dir(&shared, terminal_fresh);
        // Age the unknown snapshot past retention so the sweep can take it:
        // a young unreferenced entry might still be mid-write.
        let unknown_dir = snapshot_dir(&shared, unknown);
        let old_mtime =
            filetime::FileTime::from_unix_time(chrono::Utc::now().timestamp() - 3600, 0);
        filetime::set_file_mtime(&unknown_dir, old_mtime).unwrap();

        sweep_workspace_snapshots(&shared).await;

        assert!(live_dir.is_dir(), "live run keeps its snapshot");
        assert!(!old_dir.exists(), "terminal run past retention is swept");
        assert!(fresh_dir.is_dir(), "terminal run inside retention is kept");
        assert!(!unknown_dir.exists(), "orphaned snapshot is swept");
    }

    /// A terminal run inside its window gets the discard timer re-armed, so
    /// the snapshot still expires without waiting for another restart.
    #[tokio::test]
    async fn sweep_rearms_fresh_terminal_snapshot() {
        let (_temp, shared) = fixture(1).await;

        let run_id = seed_run(&shared, Some(chrono::Utc::now())).await;
        let dir = snapshot_dir(&shared, run_id);

        sweep_workspace_snapshots(&shared).await;
        assert!(dir.is_dir(), "inside retention at sweep time");

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        tokio::task::yield_now().await;
        assert!(!dir.exists(), "re-armed timer discards the snapshot");
    }

    /// An unreferenced snapshot (no surviving run record) inside its window
    /// gets the discard timer re-armed, so it expires after retention without
    /// waiting for another server restart.
    #[tokio::test]
    async fn sweep_rearms_fresh_unreferenced_snapshot() {
        let (_temp, shared) = fixture(1).await;

        let run_id = RunId::new();
        let dir = snapshot_dir(&shared, run_id);

        sweep_workspace_snapshots(&shared).await;
        assert!(dir.is_dir(), "inside retention at sweep time");

        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        tokio::task::yield_now().await;
        assert!(
            !dir.exists(),
            "re-armed timer discards unreferenced snapshot"
        );
    }
}

#[cfg(test)]
mod github_remote_slug_tests {
    use super::*;

    #[test]
    fn https_with_git_suffix_parses() {
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/repo.git", "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn https_without_git_suffix_parses() {
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/repo", "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn ssh_scp_form_parses() {
        assert_eq!(
            parse_github_remote_slug("git@github.com:owner/repo.git", "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn ssh_url_form_parses() {
        assert_eq!(
            parse_github_remote_slug("ssh://git@github.com/owner/repo.git", "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn ghes_host_parses() {
        // A configured GHES host is the forge: remotes on it parse, and
        // github.com URLs are rejected when the forge is enterprise.
        assert_eq!(
            parse_github_remote_slug(
                "https://ghes.example.com/owner/repo.git",
                "ghes.example.com"
            ),
            Some("owner/repo".to_owned())
        );
        assert_eq!(
            parse_github_remote_slug("git@ghes.example.com:owner/repo.git", "ghes.example.com"),
            Some("owner/repo".to_owned())
        );
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/repo.git", "ghes.example.com"),
            None
        );
    }

    #[test]
    fn non_github_hosts_rejected() {
        assert_eq!(
            parse_github_remote_slug("https://gitlab.com/owner/repo.git", "github.com"),
            None
        );
        assert_eq!(
            parse_github_remote_slug("git@gitlab.com:owner/repo.git", "github.com"),
            None
        );
        assert_eq!(
            parse_github_remote_slug("https://github.example.com/owner/repo.git", "github.com"),
            None
        );
    }

    #[test]
    fn malformed_paths_rejected() {
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner", "github.com"),
            None
        );
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/repo/extra", "github.com"),
            None
        );
        assert_eq!(
            parse_github_remote_slug("https://github.com//repo", "github.com"),
            None
        );
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/", "github.com"),
            None
        );
        assert_eq!(parse_github_remote_slug("not a url", "github.com"), None);
        assert_eq!(parse_github_remote_slug("", "github.com"), None);
    }

    #[test]
    fn whitespace_is_trimmed() {
        assert_eq!(
            parse_github_remote_slug("https://github.com/owner/repo.git\n", "github.com"),
            Some("owner/repo".to_owned())
        );
    }
}

#[cfg(test)]
mod github_remote_slug_selection_tests {
    use super::github_remote_slug;

    #[test]
    fn origin_wins_when_it_is_github() {
        let remote_v = "upstream\thttps://github.com/other/repo.git (fetch)\n\
                        origin\thttps://github.com/owner/repo.git (fetch)\n\
                        origin\thttps://github.com/owner/repo.git (push)\n";
        assert_eq!(
            github_remote_slug(remote_v, "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn first_github_remote_wins_when_origin_is_not_github() {
        let remote_v = "origin\thttps://gitlab.com/owner/repo.git (fetch)\n\
                        private\tgit@github.com:owner/repo.git (fetch)\n\
                        private\tgit@github.com:owner/repo.git (push)\n";
        assert_eq!(
            github_remote_slug(remote_v, "github.com"),
            Some("owner/repo".to_owned())
        );
    }

    #[test]
    fn ghes_remote_is_matched() {
        // With a GHES forge configured, remotes on that host parse and
        // github.com remotes do not.
        let remote_v = "origin\thttps://ghes.example.com/owner/repo.git (fetch)\n";
        assert_eq!(
            github_remote_slug(remote_v, "ghes.example.com"),
            Some("owner/repo".to_owned())
        );
        assert_eq!(github_remote_slug(remote_v, "github.com"), None);
    }

    #[test]
    fn push_only_urls_are_ignored() {
        let remote_v = "origin\thttps://github.com/owner/repo.git (push)\n";
        assert_eq!(github_remote_slug(remote_v, "github.com"), None);
    }

    #[test]
    fn no_github_remote_returns_none() {
        let remote_v = "origin\thttps://gitlab.com/owner/repo.git (fetch)\n\
                        backup\thttps://bitbucket.org/owner/repo.git (fetch)\n";
        assert_eq!(github_remote_slug(remote_v, "github.com"), None);
        assert_eq!(github_remote_slug("", "github.com"), None);
    }
}

#[cfg(test)]
mod object_cache_tests {
    use super::*;

    fn git(directory: &FsPath, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(directory)
            .env("GIT_AUTHOR_NAME", "preloop")
            .env("GIT_AUTHOR_EMAIL", "preloop@example.com")
            .env("GIT_COMMITTER_NAME", "preloop")
            .env("GIT_COMMITTER_EMAIL", "preloop@example.com")
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    fn commit(work: &FsPath, name: &str) -> String {
        std::fs::write(work.join(name), name).unwrap();
        git(work, &["add", "."]);
        git(work, &["commit", "-m", name]);
        git(work, &["rev-parse", "HEAD"])
    }

    /// Whether the cache's object database holds `sha` as a commit.
    fn cache_has(cache: &ObjectCache, sha: &str) -> bool {
        let repository = cache.objects.parent().expect("objects has a parent");
        std::process::Command::new("git")
            .arg("--git-dir")
            .arg(repository)
            .args(["cat-file", "-e", &format!("{sha}^{{commit}}")])
            .status()
            .expect("git runs")
            .success()
    }

    /// A workspace whose `main` sits at one commit and whose HEAD is detached at
    /// a later commit no branch or tag reaches, which is what a worktree at a CI
    /// merge sha, a bisect step, or a review checkout looks like.
    fn detached_workspace(root: &FsPath) -> (PathBuf, PathBuf, String, String) {
        let work = root.join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-b", "main"]);
        let on_branch = commit(&work, "a.txt");
        git(&work, &["checkout", "--detach"]);
        let detached = commit(&work, "b.txt");
        assert_ne!(on_branch, detached);
        let common_dir = std::fs::canonicalize(work.join(".git")).unwrap();
        (work, common_dir, on_branch, detached)
    }

    #[tokio::test]
    async fn first_build_holds_a_detached_head() {
        let temp = tempfile::tempdir().unwrap();
        let (work, common_dir, _, detached) = detached_workspace(temp.path());
        let state = temp.path().join("state");

        let cache = ensure_object_cache(&state, &work, &common_dir, Some(&detached), None)
            .await
            .unwrap();

        assert!(cache_has(&cache, &detached));
    }

    /// The cache is keyed by the workspace's common dir and survives across
    /// runs, so the commit a later run checks out is usually new to it. Before
    /// the refresh fetched `HEAD`, a detached commit was never copied and
    /// seeding the snapshot index died with "failed to unpack tree object".
    #[tokio::test]
    async fn refresh_copies_a_detached_head_the_cache_has_not_seen() {
        let temp = tempfile::tempdir().unwrap();
        let (work, common_dir, on_branch, _) = detached_workspace(temp.path());
        let state = temp.path().join("state");
        git(&work, &["checkout", "main"]);
        ensure_object_cache(&state, &work, &common_dir, Some(&on_branch), None)
            .await
            .unwrap();

        git(&work, &["checkout", "--detach"]);
        let later = commit(&work, "later.txt");
        let cache = ensure_object_cache(&state, &work, &common_dir, Some(&later), None)
            .await
            .unwrap();

        assert!(cache.refreshed);
        assert!(cache_has(&cache, &later));
    }

    /// A cache that already recorded a head it never received must heal: the
    /// recorded head matching the workspace used to skip the refresh forever.
    #[tokio::test]
    async fn a_recorded_head_the_cache_lacks_is_refetched() {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-b", "main"]);
        let on_branch = commit(&work, "a.txt");
        let common_dir = std::fs::canonicalize(work.join(".git")).unwrap();
        let state = temp.path().join("state");
        let seeded = ensure_object_cache(&state, &work, &common_dir, Some(&on_branch), None)
            .await
            .unwrap();

        // The state a failed refresh used to leave behind: the marker names
        // the workspace's head although the cache never received it.
        git(&work, &["checkout", "--detach"]);
        let detached = commit(&work, "b.txt");
        assert!(!cache_has(&seeded, &detached));
        let repository = seeded.objects.parent().unwrap().to_path_buf();
        let mut marker = repository.as_os_str().to_os_string();
        marker.push(".last-head");
        std::fs::write(PathBuf::from(marker), format!("{detached}\n")).unwrap();

        let healed = ensure_object_cache(&state, &work, &common_dir, Some(&detached), None)
            .await
            .unwrap();

        assert!(cache_has(&healed, &detached));
    }
}

#[cfg(test)]
mod archive_tests {
    use super::*;

    #[test]
    fn snapshot_archive_refs_are_url_safe() {
        assert!(valid_archive_ref(
            "de2d615edebdc27145960f78962b1b789ea07d33"
        ));
        assert!(valid_archive_ref("refs/heads/main"));
        assert!(valid_archive_ref("v1.2.3-rc.1"));
        assert!(!valid_archive_ref(""));
        assert!(!valid_archive_ref("--output=/tmp/x"));
        assert!(!valid_archive_ref("-x"));
        assert!(!valid_archive_ref("refs/../etc/passwd"));
        assert!(!valid_archive_ref("refs//main"));
        assert!(!valid_archive_ref("refs/heads/main "));
    }

    #[test]
    fn archive_arguments_and_prefix_match_the_forge_shape() {
        assert_eq!(
            archive_prefix(
                "snapshots",
                "7b116f3a-6973-4e4e-bcd1-213de9518ad1",
                "abc123"
            ),
            "snapshots-7b116f3a-6973-4e4e-bcd1-213de9518ad1-abc123/"
        );
        assert_eq!(
            snapshot_archive_args("abc123", "snapshots-run-abc123/"),
            vec![
                "archive".to_string(),
                "--format=tar.gz".to_string(),
                "--prefix=snapshots-run-abc123/".to_string(),
                "abc123".to_string(),
            ]
        );
    }

    /// `actions/checkout`'s no-git path extracts GitHub's archive shape: one
    /// top-level `{owner}-{repo}-{sha}/` directory. Build the real archive
    /// from a real repository and check the shape the action will see.
    #[tokio::test]
    async fn snapshot_archive_has_the_forge_top_level_directory() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("snapshot.git");
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let git = |args: &[&str], cwd: &FsPath| {
            std::process::Command::new("git")
                .current_dir(cwd)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@example.com")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@example.com")
                .args(args)
                .output()
                .unwrap()
        };
        assert!(git(&["init", "--quiet", "."], &work).status.success());
        std::fs::write(work.join("tracked.txt"), "archive me\n").unwrap();
        assert!(git(&["add", "-A"], &work).status.success());
        assert!(
            git(&["commit", "--quiet", "-m", "seed"], &work)
                .status
                .success()
        );
        let sha = String::from_utf8(
            std::process::Command::new("git")
                .current_dir(&work)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        let sha = sha.trim().to_owned();
        assert!(
            git(
                &["clone", "--quiet", "--bare", ".", repo.to_str().unwrap()],
                &work
            )
            .status
            .success()
        );

        let prefix = archive_prefix("snapshots", "run-1", &sha);
        let path = write_snapshot_archive(&repo, &sha, &prefix).await.unwrap();
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[..2], b"\x1f\x8b", "archive must be gzipped");
        // `tar` sees one top-level directory holding the tree, which is the
        // directory `actions/checkout` moves the contents out of. The raw
        // first block is git's `pax_global_header` (commit id metadata), as in
        // the forge's own tarballs; `tar` does not list it.
        let listing = std::process::Command::new("tar")
            .args(["-tzf", path.to_str().unwrap()])
            .output()
            .unwrap();
        assert!(listing.status.success());
        let listing = String::from_utf8_lossy(&listing.stdout);
        let entries: Vec<&str> = listing.lines().collect();
        assert_eq!(
            entries.first().copied(),
            Some(prefix.as_str()),
            "first listed entry must be the top-level dir: {listing}"
        );
        assert!(
            entries.iter().all(|entry| entry.starts_with(&prefix)),
            "every entry must live under {prefix}: {listing}"
        );
        assert!(
            entries.contains(&format!("{prefix}tracked.txt").as_str()),
            "the tree must be inside the prefixed directory: {listing}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
