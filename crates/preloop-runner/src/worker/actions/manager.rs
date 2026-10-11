//! Action download and extraction manager.
//!
//! Uses `ActionsResolveClient` to batch-resolve `uses:` refs to SHA-pinned
//! codeload.github.com URLs before downloading.
//!
//! Golden 10 flow 19-20: batch POST to runnerresolve → GET codeload tarball →
//! extract to `_work/_actions/{owner}/{repo}/{sha}/`.
//!
//! There is no api.github.com fallback. If the server did not resolve
//! the ref to a commit SHA with a SHA-pinned download URL, the download is
//! refused before any network access — fetching the mutable ref would
//! reintroduce the TOCTOU that SHA pinning removes.

use anyhow::{Context, Result};
use rand::Rng;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::{info, warn};

use crate::client::actions_download::ArchiveDigestPin;

/// SHA-256 hex digest of downloaded action tarball bytes.
///
/// The digest is computed over the archive bytes *before* extraction, so a
/// known pin is compared before `extract_tarball` ever runs: tampered bytes
/// fail closed without creating an executable destination tree. The bytes
/// are whatever the server-pinned URL served over TLS, so the digest also
/// pins the packaging — a re-packaged tarball of the same commit is a
/// different archive and will not match an old pin.
pub use preloop_gha_protocol::crypto::sha256_hex as archive_sha256_hex;

/// Path of the sidecar file recording which archive produced a cached
/// action tree: `<actions_dir>/<owner>/<repo>/<sha>.sha256`, containing the
/// lowercase hex SHA-256 of the tarball bytes that were extracted there.
/// Digest-sidecar path: `<actions>/<owner>/<repo>/<sha>.sha256`, the
/// lowercase hex SHA-256 of the tarball bytes a verified fresh download
/// hashed before extraction.
///
/// A cache entry's provenance is unknown (it may predate checksum support
/// or have been written by hand), so a cached tree alone can never match
/// a known pin — it is evicted and re-downloaded. The sidecar written by
/// a verified fresh download lets a later cache hit prove it came from
/// the pinned archive without re-downloading.
fn archive_digest_sidecar(actions_dir: &Path, owner: &str, repo: &str, dir_ref: &str) -> PathBuf {
    actions_dir
        .join(owner)
        .join(repo)
        .join(format!("{dir_ref}.sha256"))
}

/// Read the archive digest recorded for a cached action tree, if any.
fn read_cached_archive_digest(sidecar: &Path) -> Option<String> {
    std::fs::read_to_string(sidecar)
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
}

/// Response header in which the engine attests the SHA-256 of the action
/// tarball bytes it serves. Must match the server's
/// `ACTION_ARCHIVE_SHA256_HEADER` in `preloop-runner-server`.
const ACTION_ARCHIVE_SHA256_HEADER: &str = "x-preloop-action-archive-sha256";

/// Parse and validate the engine-attested archive digest from download
/// response headers. Returns `None` when the header is absent (older
/// engine) or malformed — a missing attestation never fails the
/// download, it just leaves Unpinned downloads unverified.
fn header_archive_digest(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let value = headers.get(ACTION_ARCHIVE_SHA256_HEADER)?;
    let digest = value.to_str().ok()?.trim().to_ascii_lowercase();
    if digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(digest)
    } else {
        tracing::warn!("ignoring malformed {ACTION_ARCHIVE_SHA256_HEADER} response header");
        None
    }
}

/// Result of a single action-archive fetch attempt: either the archive
/// bytes plus the engine's attested digest header, or a throttling signal
/// carrying the raw `Retry-After` header value to convert into a backoff.
enum ArchiveFetch {
    Downloaded(bytes::Bytes, Option<String>),
    Throttled(Option<String>),
}

/// Official `UrlUtil.GetRetryAfter` (v2.338.0, Runner.Sdk/Util/UrlUtil.cs):
/// the first `retry-after` response header value, or `None` when absent.
fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Official `BackoffTimerHelper.GetRandomBackoff`: a uniform random delay in
/// `[min, max)` measured in whole milliseconds.
fn random_backoff(min: Duration, max: Duration) -> Duration {
    Duration::from_millis(
        rand::thread_rng().gen_range(min.as_millis() as u64..max.as_millis() as u64),
    )
}

/// Official `VssNetworkHelper.ConvertRetryAfterToTimeSpan` (v2.338.0): the
/// `Retry-After` header may be delta-seconds or an HTTP-date. Dates in the
/// past are ignored (`None` → the caller's random backoff). A delay below
/// `min` yields a random backoff in `[min, min+30s)`; above `max`, a random
/// backoff in `[max, max+30s)` — the same random band the official helper
/// returns rather than a hard clamp. Unparseable values yield `None`.
fn parse_http_date(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // `DateTime::parse_from_rfc2822` accepts the IMF-fixdate-compatible
    // numeric-offset form already used by the existing tests. The HTTP
    // parser in .NET also accepts the three HTTP-date wire forms below:
    // IMF-fixdate, obsolete RFC 850, and asctime-date.
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(value) {
        return Some(date.with_timezone(&chrono::Utc));
    }
    [
        "%a, %d %b %Y %H:%M:%S GMT",
        "%A, %d-%b-%y %H:%M:%S GMT",
        "%a %b %e %H:%M:%S %Y",
    ]
    .into_iter()
    .find_map(|format| {
        chrono::NaiveDateTime::parse_from_str(value, format)
            .ok()
            .map(|date| date.and_utc())
    })
}

fn convert_retry_after_to_duration(
    retry_after: Option<&str>,
    min: Duration,
    max: Duration,
) -> Option<Duration> {
    let retry_after = retry_after?.trim();
    if retry_after.is_empty() {
        return None;
    }
    let delay = if let Ok(seconds) = retry_after.parse::<u64>() {
        Duration::from_secs(seconds)
    } else if let Some(date) = parse_http_date(retry_after) {
        // Absolute HTTP-date → delay relative to now. A date in the past is
        // ignored, matching the official guard against clock skew.
        date.signed_duration_since(chrono::Utc::now())
            .to_std()
            .ok()?
    } else {
        return None;
    };
    if delay < min {
        return Some(random_backoff(min, min + Duration::from_secs(30)));
    }
    if delay > max {
        return Some(random_backoff(max, max + Duration::from_secs(30)));
    }
    Some(delay)
}

/// One authenticated-or-anonymous archive fetch: the former
/// `download_action` request bodies. `Throttled` carries the raw
/// `Retry-After` header so the caller's retry loop can convert it; every
/// other non-success status fails exactly as before.
async fn fetch_archive(
    client: &crate::client::http::HttpClient,
    url: &str,
    auth_token: Option<&str>,
) -> Result<ArchiveFetch> {
    if let Some(token) = auth_token {
        // Authenticated download (GitHub codeload or private actions)
        let resp = client
            .client_for(url)
            .get(url)
            .header("Authorization", format!("Bearer {token}"))
            .header("User-Agent", "preloop-runner")
            .send()
            .await
            .with_context(|| format!("downloading action tarball from {url}"))?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Ok(ArchiveFetch::Throttled(retry_after_header(resp.headers())));
        }
        if !resp.status().is_success() {
            anyhow::bail!("Action download failed: {} {}", resp.status(), url);
        }
        let attested = header_archive_digest(resp.headers());
        let bytes = resp.bytes().await?;
        Ok(ArchiveFetch::Downloaded(bytes, attested))
    } else {
        let resp = client
            .client_for(url)
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Ok(ArchiveFetch::Throttled(retry_after_header(resp.headers())));
        }
        if !resp.status().is_success() {
            anyhow::bail!("GET {url} returned {}", resp.status());
        }
        let attested = header_archive_digest(resp.headers());
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("reading body of GET {url}"))?;
        Ok(ArchiveFetch::Downloaded(bytes, attested))
    }
}

/// `std::env::var` with a test-only thread-local override — the convention
/// from `main.rs` (`env_or_test`): edition 2024 makes `std::env::set_var`
/// unsafe and the workspace denies `unsafe`, so tests redirect reads.
fn env_or_test(name: &str) -> Option<String> {
    #[cfg(test)]
    if let Some(value) = TEST_ENV.with(|cell| cell.borrow().get(name).cloned()) {
        // Blank override reads as unset, matching the empty-value filters the
        // production callers apply.
        if !value.trim().is_empty() {
            return Some(value);
        }
        return None;
    }
    std::env::var(name).ok()
}

#[cfg(test)]
thread_local! {
    static TEST_ENV: std::cell::RefCell<std::collections::HashMap<&'static str, String>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Run `body` with `vars` visible to `env_or_test` on this thread only.
/// Replaces `std::env::set_var`; the override survives `await` points because
/// `#[tokio::test]` drives the whole future on one thread.
#[cfg(test)]
async fn with_test_env<T>(
    vars: &[(&'static str, Option<String>)],
    body: impl Future<Output = T>,
) -> T {
    let saved = TEST_ENV.with(|cell| cell.borrow().clone());
    TEST_ENV.with(|cell| {
        let mut map = cell.borrow_mut();
        for (name, value) in vars {
            match value {
                Some(value) => {
                    map.insert(*name, value.clone());
                }
                None => {
                    map.insert(*name, String::new());
                }
            }
        }
    });
    let result = body.await;
    TEST_ENV.with(|cell| *cell.borrow_mut() = saved);
    result
}

/// Official `_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF` kill-switch: any non-empty
/// value disables the inter-attempt sleep (the retry still happens).
fn action_download_backoff_disabled() -> bool {
    env_or_test("_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF").is_some_and(|value| !value.is_empty())
}

/// Backoff before a throttled retry: the server's `Retry-After` converted to
/// [10s, 10min] when usable, otherwise the official 10–30s random backoff —
/// prefer the Retry-After header.
fn action_download_backoff(retry_after_value: Option<&str>) -> Duration {
    convert_retry_after_to_duration(
        retry_after_value,
        Duration::from_secs(10),
        Duration::from_secs(600),
    )
    .unwrap_or_else(|| random_backoff(Duration::from_secs(10), Duration::from_secs(30)))
}

/// Remove a cached action tree and its digest sidecar. Fails closed: if the
/// eviction itself fails the caller must not run the suspect tree.
fn evict_action_cache(dest: &Path, sidecar: &Path) -> Result<()> {
    std::fs::remove_dir_all(dest)
        .with_context(|| format!("evicting action cache {}", dest.display()))?;
    if sidecar.exists() {
        std::fs::remove_file(sidecar)
            .with_context(|| format!("evicting action digest sidecar {}", sidecar.display()))?;
    }
    Ok(())
}

/// Download and extract a remote action to the _actions directory.
///
/// `git_ref` must be the server-resolved commit SHA (40 hex chars), not a
/// mutable branch/tag: callers pass `resolved_sha` from the runnerresolve
/// response, falling back to the raw `uses:` ref only when resolution
/// failed. `download_url` must be the server-supplied SHA-pinned tarball
/// URL. Both are required: if the server could not pin the ref to a
/// commit, the runner refuses to fetch the mutable ref from
/// api.github.com — downloading `tarball/{branch|tag}` reintroduces the
/// TOCTOU that SHA pinning exists to remove (the ref can move between
/// resolution and download).
///
/// `digest_pin` carries the server's archive-checksum pin for this action
/// version ([`ArchiveDigestPin`]). The SHA-256 of the downloaded archive
/// bytes is computed immediately after download and compared *before*
/// `extract_tarball` runs: a mismatch fails closed and no destination
/// tree is created, so tampered bytes are never executed. The expected
/// digest is the resolve-time pin when Pinned; otherwise the engine
/// attests the digest in the `x-preloop-action-archive-sha256` download
/// response header, so even an Unpinned download is verified against the
/// exact bytes the engine served. On a successful fresh download the
/// observed digest is returned, and a digest sidecar is written after
/// successful extraction so later cache hits can be checked against it
/// without re-downloading. Nothing is reported back to the server: pins
/// are minted by the engine when it fetches the tarball itself, never by
/// job VMs.
///
/// A cache entry whose sidecar does not match a known pin — or any cache
/// entry at all when Unpinned, since its provenance is unknown and job
/// containers mount this directory read-write — is evicted and replaced
/// by a fresh verified download, never executed.
///
/// These checks run before the cache lookup so a stale mutable-ref cache
/// entry cannot bypass them, and before any network access.
pub async fn download_action(
    owner: &str,
    repo: &str,
    git_ref: &str,
    actions_dir: &Path,
    download_url: Option<&str>,
    auth_token: Option<&str>,
    digest_pin: ArchiveDigestPin,
) -> Result<(PathBuf, Option<String>)> {
    // Only pinned commit SHAs may be downloaded; anything else means
    // server-side resolution failed. The all-zero sentinel is a valid SHA
    // shape but names no commit, so it is rejected like any unpinned ref.
    if !preloop_gha_protocol::git_ref::is_commit_sha_not_zero(git_ref) {
        anyhow::bail!(
            "refusing to download action {owner}/{repo}@{git_ref}: \
             ref was not resolved to a commit SHA"
        );
    }
    let url = download_url.filter(|url| !url.is_empty()).ok_or_else(|| {
        anyhow::anyhow!(
            "refusing to download action {owner}/{repo}@{git_ref}: \
             server supplied no SHA-pinned download URL"
        )
    })?;

    // Use the resolved SHA as directory name when available for correctness.
    let dir_ref = git_ref; // caller should pass resolved_sha here when available
    let dest = actions_dir.join(owner).join(repo).join(dir_ref);

    // Whether this run verifies digests at all. An older server that
    // predates digest support keeps the exact legacy behavior: no
    // verification, no report, no cache eviction.
    let verifying = !matches!(digest_pin, ArchiveDigestPin::Unsupported);
    let sidecar = archive_digest_sidecar(actions_dir, owner, repo, dir_ref);

    if dest.exists() {
        match &digest_pin {
            ArchiveDigestPin::Pinned(expected) => {
                if read_cached_archive_digest(&sidecar).as_deref() == Some(expected.as_str()) {
                    info!(
                        "Action {owner}/{repo}@{git_ref} already cached at {} (archive checksum verified)",
                        dest.display()
                    );
                    return Ok((dest, Some(expected.clone())));
                }
                // A cached tree whose recorded archive digest does not match
                // the pin — poisoned, stale, or predating checksum support —
                // must never be executed: evict it and fall through to a
                // fresh verified download below. A known pin is never
                // bypassed by a cache entry.
                tracing::warn!(
                    "action archive checksum mismatch for cached {owner}/{repo}@{git_ref}: \
                     evicting cache and re-downloading"
                );
                evict_action_cache(&dest, &sidecar)?;
            }
            ArchiveDigestPin::Unpinned => {
                // The engine had not pinned this version at resolve time, so
                // a cache entry of unknown provenance cannot be trusted: a
                // job container gets this directory mounted read-write
                // (`{runner_actions}:/__w/_actions`), so a previous job's
                // workflow could have modified the extracted tree. Evict it
                // and re-download; the fresh bytes are verified against the
                // engine-attested digest header before extraction.
                // The runner verifies *against* pins and attestations, it
                // never mints them.
                tracing::warn!(
                    "action {owner}/{repo}@{git_ref} cached but server has no pin yet: \
                     evicting cache of unknown provenance and re-downloading"
                );
                evict_action_cache(&dest, &sidecar)?;
            }
            ArchiveDigestPin::Unsupported => {
                info!(
                    "Action {owner}/{repo}@{git_ref} already cached at {}",
                    dest.display()
                );
                return Ok((dest, None));
            }
        }
    }

    // No api.github.com fallback. `url` is the server-supplied
    // SHA-pinned tarball URL, validated above; a missing URL fails closed.
    let url = url.to_string();

    info!("Downloading action {owner}/{repo}@{git_ref} from {url}");

    let client = crate::client::http::HttpClient::new(None)?;
    // Official `ActionManager.DownloadRepositoryArchive` (v2.338.0): an
    // action download retried on HTTP 429 at most twice, preferring the
    // server's `Retry-After` header (converted to [10s, 10min]) over the
    // 10–30s random backoff; `_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF` disables
    // the sleep. Other failures keep this runner's existing fail-fast
    // semantics — they are not retried.
    let (bytes, attested_digest) = {
        let mut throttled_attempts = 0;
        loop {
            match fetch_archive(&client, &url, auth_token).await? {
                ArchiveFetch::Downloaded(bytes, attested) => break (bytes, attested),
                ArchiveFetch::Throttled(retry_after_value) => {
                    throttled_attempts += 1;
                    if throttled_attempts > 2 {
                        anyhow::bail!("Action download failed: 429 Too Many Requests {url}");
                    }
                    if action_download_backoff_disabled() {
                        continue;
                    }
                    // We are being throttled, use the Retry-After header (if
                    // provided) to decide backoff time; otherwise the random
                    // backoff — prefer the Retry-After header.
                    // Action preparation runs before `run_steps` creates the
                    // synthetic setup-step log, and this manager also serves
                    // on-demand nested actions. Neither caller supplies an
                    // ExecutionContext/StepContext or reporting sink, so
                    // tracing is the only reachable warning channel here.
                    let back_off = action_download_backoff(retry_after_value.as_deref());
                    warn!("Back off {} seconds before retry.", back_off.as_secs_f64());
                    tokio::time::sleep(back_off).await;
                }
            }
        }
    };

    // Extract tarball, stripping top-level directory (standard GitHub tarball layout)
    // v2.336.0 (#4509): Log archive size for telemetry
    info!(
        "Action archive {owner}/{repo}@{git_ref}: {} bytes",
        bytes.len()
    );

    // Hash the downloaded archive bytes immediately, and compare against
    // the expected digest BEFORE creating any destination tree: a mismatch
    // fails closed here, so tampered bytes never reach `extract_tarball`
    // and no executable destination is left behind.
    //
    // The expected digest is the resolve-time pin when Pinned; otherwise
    // the engine-attested digest from the download response header:
    // the engine computed it over the exact bytes it serves, so even an
    // Unpinned download is verified. When neither exists (older engine),
    // the download proceeds unverified with a warning — legacy behavior.
    let expected_digest: Option<String> = match &digest_pin {
        ArchiveDigestPin::Pinned(expected) => Some(expected.clone()),
        ArchiveDigestPin::Unpinned => attested_digest.clone(),
        ArchiveDigestPin::Unsupported => None,
    };
    // A pin/attestation disagreement means the engine attested different
    // bytes than it pinned: fail closed rather than guess which to trust.
    if let (ArchiveDigestPin::Pinned(expected), Some(attested)) = (&digest_pin, &attested_digest)
        && attested != expected
    {
        anyhow::bail!(
            "action archive sha256 attestation mismatch for {owner}/{repo}@{git_ref}: \
                 pinned {expected} but engine attested {attested}; refusing to extract"
        );
    }
    let observed_digest = if verifying {
        let observed = archive_sha256_hex(&bytes);
        if let Some(expected) = &expected_digest {
            if &observed != expected {
                anyhow::bail!(
                    "action archive sha256 mismatch for {owner}/{repo}@{git_ref}: \
                     expected {expected}, observed {observed}; refusing to extract a \
                     tarball whose bytes differ from the pinned archive checksum"
                );
            }
            info!("Action {owner}/{repo}@{git_ref} archive checksum verified: {observed}");
        } else {
            tracing::warn!(
                "action {owner}/{repo}@{git_ref}: no archive digest pin or attestation \
                 available; proceeding unverified (older engine)"
            );
        }
        Some(observed)
    } else {
        None
    };

    let parent_dir = dest
        .parent()
        .context("action destination must have parent directory")?;
    std::fs::create_dir_all(parent_dir)
        .with_context(|| format!("creating parent dir {}", parent_dir.display()))?;

    let staging = tempfile::Builder::new()
        .prefix(".action_tmp_")
        .tempdir_in(parent_dir)
        .with_context(|| format!("creating staging dir in {}", parent_dir.display()))?;

    extract_tarball(&bytes, staging.path())?;

    let staging_path = staging.keep();
    if !dest.exists() {
        if let Err(err) = std::fs::rename(&staging_path, &dest) {
            let _ = std::fs::remove_dir_all(&staging_path);
            if !dest.exists() {
                return Err(err)
                    .with_context(|| format!("moving extracted action to {}", dest.display()));
            }
        }
    } else {
        let _ = std::fs::remove_dir_all(&staging_path);
    }

    // Record which archive produced this tree so later cache hits can
    // prove they came from the pinned bytes. Written only after the
    // archive extracted and moved into place successfully, so a sidecar
    // always describes a complete, extracted tree.
    if let Some(observed) = &observed_digest {
        std::fs::write(&sidecar, observed)
            .with_context(|| format!("recording action archive digest {}", sidecar.display()))?;
    }

    info!("Extracted action to {}", dest.display());
    Ok((dest, observed_digest))
}

/// Check whether a relative symlink target, resolved against the symlink's parent directory,
/// normalizes safely within the root directory (never escapes above root).
fn is_safe_relative_symlink(link_parent: Option<&Path>, link_target: &Path) -> bool {
    if link_target.is_absolute() || link_target.starts_with("/") || link_target.starts_with("\\") {
        return false;
    }

    let mut stack: Vec<&std::ffi::OsStr> = Vec::new();
    if let Some(parent) = link_parent {
        for component in parent.components() {
            match component {
                std::path::Component::Normal(c) => stack.push(c),
                std::path::Component::ParentDir => {
                    if stack.pop().is_none() {
                        return false;
                    }
                }
                std::path::Component::CurDir => {}
                _ => return false,
            }
        }
    }

    for component in link_target.components() {
        match component {
            std::path::Component::Normal(c) => stack.push(c),
            std::path::Component::ParentDir => {
                if stack.pop().is_none() {
                    return false;
                }
            }
            std::path::Component::CurDir => {}
            _ => return false,
        }
    }

    true
}

/// Extract a `.tar.gz` tarball to `dest`, stripping the top-level directory.
///
/// Uses `cap_std` capability-based filesystem sandboxing to ensure extracted entries
/// cannot escape `dest` via path traversal (`..`), absolute paths, or malicious symlinks.
pub fn extract_tarball(bytes: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest)
        .with_context(|| format!("creating action dir {}", dest.display()))?;

    let dest_dir = cap_std::fs::Dir::open_ambient_dir(dest, cap_std::ambient_authority())
        .with_context(|| format!("opening capability sandbox for {}", dest.display()))?;

    #[cfg(unix)]
    use cap_std::fs::PermissionsExt;

    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        }) {
            anyhow::bail!(
                "malicious archive entry escapes sandbox: {}",
                path.display()
            );
        }

        let stripped: PathBuf = path.components().skip(1).collect();
        if stripped.components().count() == 0 {
            continue;
        }

        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            dest_dir.create_dir_all(&stripped)?;
        } else if entry_type.is_file() {
            if let Some(parent) = stripped.parent()
                && parent.components().count() > 0
            {
                dest_dir.create_dir_all(parent)?;
            }
            let mut outfile = dest_dir.create(&stripped)?;
            std::io::copy(&mut entry, &mut outfile)?;

            #[cfg(unix)]
            if let Ok(mode) = entry.header().mode() {
                // Mask to standard rwx permissions (0o777), stripping setuid (0o4000), setgid (0o2000), and sticky (0o1000) bits
                let safe_mode = mode & 0o777;
                let perms = cap_std::fs::Permissions::from_mode(safe_mode);
                outfile
                    .set_permissions(perms)
                    .with_context(|| format!("setting permissions on {}", stripped.display()))?;
            }
        } else if entry_type.is_symlink() {
            if let Some(link_target) = entry.link_name()? {
                let parent = stripped.parent();
                if let Some(parent) = parent
                    && parent.components().count() > 0
                {
                    dest_dir.create_dir_all(parent)?;
                }

                // Resolve physical parent directory relative to dest to account for
                // preceding symlinks that shift the physical parent depth.
                let canonical_dest = dest.canonicalize()?;
                let physical_parent = if let Some(parent) = parent {
                    let parent_path = dest.join(parent);
                    if let Ok(canonical_parent) = parent_path.canonicalize() {
                        if !canonical_parent.starts_with(&canonical_dest) {
                            anyhow::bail!(
                                "symlink parent directory escapes destination root: {}",
                                parent.display()
                            );
                        }
                        canonical_parent
                            .strip_prefix(&canonical_dest)
                            .ok()
                            .map(Path::to_path_buf)
                    } else {
                        Some(parent.to_path_buf())
                    }
                } else {
                    None
                };

                if !is_safe_relative_symlink(physical_parent.as_deref(), &link_target) {
                    anyhow::bail!(
                        "symlink with escaping or absolute target rejected: {}",
                        link_target.display()
                    );
                }
                #[cfg(unix)]
                dest_dir.symlink(&link_target, &stripped)?;
                #[cfg(windows)]
                {
                    // cap_std splits symlink creation by target type on
                    // Windows; the archived target may not exist yet, so try
                    // file first and fall back to dir.
                    if dest_dir.symlink_file(&link_target, &stripped).is_err() {
                        dest_dir.symlink_dir(&link_target, &stripped)?;
                    }
                }
            }
        } else {
            anyhow::bail!(
                "unsupported or dangerous archive entry type {:?} for {}",
                entry_type,
                path.display()
            );
        }
    }

    Ok(())
}

/// Copy a local action to the actions directory.
pub fn copy_local_action(source: &Path, actions_dir: &Path, action_name: &str) -> Result<PathBuf> {
    let dest = actions_dir.join(action_name);
    if dest.exists() {
        return Ok(dest);
    }
    let parent_dir = dest
        .parent()
        .context("destination must have parent directory")?;
    std::fs::create_dir_all(parent_dir)?;
    let staging = tempfile::Builder::new()
        .prefix(".local_action_tmp_")
        .tempdir_in(parent_dir)?;
    copy_dir_recursive(source, staging.path())?;
    let staging_path = staging.keep();
    if !dest.exists() {
        if let Err(err) = std::fs::rename(&staging_path, &dest) {
            let _ = std::fs::remove_dir_all(&staging_path);
            if !dest.exists() {
                return Err(err).with_context(|| {
                    format!(
                        "moving copied local action from {} to {}",
                        staging_path.display(),
                        dest.display()
                    )
                });
            }
        }
    } else {
        let _ = std::fs::remove_dir_all(&staging_path);
    }
    Ok(dest)
}

/// Recursively copy a directory.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    copy_dir_recursive_inner(src, dst, src)
}

fn copy_dir_recursive_inner(src: &Path, dst: &Path, root_src: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest = dst.join(entry.file_name());
        if ty.is_symlink() {
            let entry_path = entry.path();
            let target = std::fs::read_link(&entry_path)
                .with_context(|| format!("reading symlink {}", entry_path.display()))?;
            let canonical_root = root_src.canonicalize()?;
            let physical_parent = if let Some(parent) = entry_path.parent() {
                if let Ok(canonical_parent) = parent.canonicalize() {
                    if !canonical_parent.starts_with(&canonical_root) {
                        anyhow::bail!(
                            "symlink parent directory escapes source root: {}",
                            parent.display()
                        );
                    }
                    canonical_parent
                        .strip_prefix(&canonical_root)
                        .ok()
                        .map(Path::to_path_buf)
                } else {
                    entry_path
                        .strip_prefix(root_src)
                        .ok()
                        .and_then(|p| p.parent())
                        .map(Path::to_path_buf)
                }
            } else {
                None
            };

            if !is_safe_relative_symlink(physical_parent.as_deref(), &target) {
                anyhow::bail!(
                    "local action contains escaping or absolute symlink: {} -> {}",
                    entry.path().display(),
                    target.display()
                );
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&target, &dest)
                .with_context(|| format!("creating symlink {}", dest.display()))?;
            #[cfg(windows)]
            {
                let is_dir = if let Some(parent) = entry_path.parent() {
                    parent.join(&target).is_dir()
                } else {
                    target.is_dir()
                };
                if is_dir {
                    std::os::windows::fs::symlink_dir(&target, &dest)?;
                } else {
                    std::os::windows::fs::symlink_file(&target, &dest)?;
                }
            }
        } else if ty.is_dir() {
            copy_dir_recursive_inner(&entry.path(), &dest, root_src)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_tarball(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            for (path, content) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_size(content.len() as u64);
                header.set_mode(0o644);
                header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
                header.set_cksum();
                tar.append(&header, *content).unwrap();
            }
            tar.finish().unwrap();
        }
        enc.finish().unwrap()
    }

    fn create_test_tarball_with_custom_entry(
        path: &str,
        entry_type: tar::EntryType,
        link_name: Option<&str>,
        content: &[u8],
    ) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(entry_type);
            header.as_mut_bytes()[..path.len()].copy_from_slice(path.as_bytes());
            if let Some(target) = link_name {
                header.set_link_name(target).unwrap();
            }
            header.set_cksum();
            tar.append(&header, content).unwrap();
            tar.finish().unwrap();
        }
        enc.finish().unwrap()
    }

    #[test]
    fn extract_tarball_unpacks_safely_inside_sandbox() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[
            ("checkout-v4/action.yml", b"name: Checkout\n"),
            ("checkout-v4/dist/index.js", b"console.log('hello');\n"),
        ]);

        extract_tarball(&tar_bytes, &dest).unwrap();
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("dist/index.js")).unwrap(),
            "console.log('hello');\n"
        );
    }

    #[test]
    fn extract_tarball_rejects_path_traversal() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[("root/../../escape.txt", b"evil")]);
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
    }

    #[test]
    fn extract_tarball_rejects_absolute_paths() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball(&[("/escape.txt", b"evil")]);
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
    }

    #[test]
    fn extract_tarball_rejects_hard_links() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball_with_custom_entry(
            "root/evil_hardlink",
            tar::EntryType::Link,
            Some("/etc/passwd"),
            b"",
        );
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported or dangerous")
        );
    }

    #[test]
    fn extract_tarball_rejects_absolute_symlinks() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        let tar_bytes = create_test_tarball_with_custom_entry(
            "root/evil_symlink",
            tar::EntryType::Symlink,
            Some("/etc/shadow"),
            b"",
        );
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("symlink with escaping or absolute target rejected")
        );
    }

    #[test]
    fn extract_tarball_rejects_escaping_symlink_traversal() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");
        let outside_file = temp.path().join("escaped_target.txt");
        std::fs::write(&outside_file, b"initial").unwrap();

        // Archive has:
        // symlink `sub/evil_link` -> `../../escaped_target.txt`
        // file `sub/evil_link` trying to overwrite through it or traverse it
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_entry_type(tar::EntryType::Symlink);
            header.as_mut_bytes()[.."root/sub/evil_link".len()]
                .copy_from_slice(b"root/sub/evil_link");
            header.set_link_name("../../escaped_target.txt").unwrap();
            header.set_cksum();
            tar.append(&header, &b""[..]).unwrap();

            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(7);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.as_mut_bytes()[.."root/sub/evil_link/pwn".len()]
                .copy_from_slice(b"root/sub/evil_link/pwn");
            file_header.set_cksum();
            tar.append(&file_header, &b"hacked!"[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        // Verify outside file was untouched
        assert_eq!(std::fs::read_to_string(&outside_file).unwrap(), "initial");
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_rejects_chained_symlink_escape() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Archive has:
        // directory `root/b`
        // symlink `root/a/deep` -> `../b`
        // symlink `root/a/deep/link` -> `../../outside` (lexically looks like depth 2 with 2 '..' = 0, but physically is depth 1 with 2 '..' = -1!)
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut dir_header = tar::Header::new_gnu();
            dir_header.set_size(0);
            dir_header.set_mode(0o755);
            dir_header.set_entry_type(tar::EntryType::Directory);
            dir_header.as_mut_bytes()[.."root/b/".len()].copy_from_slice(b"root/b/");
            dir_header.set_cksum();
            tar.append(&dir_header, &b""[..]).unwrap();

            let mut link1_header = tar::Header::new_gnu();
            link1_header.set_size(0);
            link1_header.set_mode(0o777);
            link1_header.set_entry_type(tar::EntryType::Symlink);
            link1_header.as_mut_bytes()[.."root/a/deep".len()].copy_from_slice(b"root/a/deep");
            link1_header.set_link_name("../b").unwrap();
            link1_header.set_cksum();
            tar.append(&link1_header, &b""[..]).unwrap();

            let mut link2_header = tar::Header::new_gnu();
            link2_header.set_size(0);
            link2_header.set_mode(0o777);
            link2_header.set_entry_type(tar::EntryType::Symlink);
            link2_header.as_mut_bytes()[.."root/a/deep/link".len()]
                .copy_from_slice(b"root/a/deep/link");
            link2_header.set_link_name("../../outside").unwrap();
            link2_header.set_cksum();
            tar.append(&link2_header, &b""[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        let result = extract_tarball(&tar_bytes, &dest);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("symlink with escaping or absolute target rejected")
        );
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_allows_in_root_relative_symlinks() {
        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Archive has:
        // regular file `lib/tool.js`
        // in-root relative symlink `bin/tool` -> `../lib/tool.js`
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(19);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.as_mut_bytes()[.."root/lib/tool.js".len()]
                .copy_from_slice(b"root/lib/tool.js");
            file_header.set_cksum();
            tar.append(&file_header, &b"console.log('tool')"[..])
                .unwrap();

            let mut link_header = tar::Header::new_gnu();
            link_header.set_size(0);
            link_header.set_mode(0o777);
            link_header.set_entry_type(tar::EntryType::Symlink);
            link_header.as_mut_bytes()[.."root/bin/tool".len()].copy_from_slice(b"root/bin/tool");
            link_header.set_link_name("../lib/tool.js").unwrap();
            link_header.set_cksum();
            tar.append(&link_header, &b""[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        extract_tarball(&tar_bytes, &dest).unwrap();
        assert_eq!(
            std::fs::read_to_string(dest.join("bin/tool")).unwrap(),
            "console.log('tool')"
        );
    }

    #[cfg(unix)]
    #[test]
    fn extract_tarball_masks_special_permission_bits() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().unwrap();
        let dest = temp.path().join("action_dest");

        // Entry with setuid (0o4755)
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut tar = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(5);
            header.set_mode(0o4755);
            header.set_entry_type(tar::EntryType::Regular);
            header.as_mut_bytes()[.."root/script.sh".len()].copy_from_slice(b"root/script.sh");
            header.set_cksum();
            tar.append(&header, &b"echo\n"[..]).unwrap();
            tar.finish().unwrap();
        }
        let tar_bytes = enc.finish().unwrap();
        extract_tarball(&tar_bytes, &dest).unwrap();

        let metadata = std::fs::metadata(dest.join("script.sh")).unwrap();
        let mode = metadata.permissions().mode();
        // The setuid bit (0o4000) must be stripped, leaving only rwxr-xr-x (0o755)
        assert_eq!(mode & 0o7777, 0o755);
    }

    #[tokio::test]
    async fn download_action_refuses_all_zero_sha() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // No server is started: the zero sentinel must be refused before any
        // network access or cache lookup.
        let result = download_action(
            "owner",
            "repo",
            "0000000000000000000000000000000000000000",
            &actions_dir,
            Some("http://127.0.0.1:1/tarball"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;

        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("was not resolved to a commit SHA"),
            "unexpected error: {error}"
        );
        assert!(
            !actions_dir.exists(),
            "refused download must not create the actions directory"
        );
    }

    #[tokio::test]
    async fn download_action_atomic_cleanup_on_error() {
        use axum::{Router, routing::get};
        let evil_tar = create_test_tarball(&[("root/../../escape.txt", b"evil")]);

        let app = Router::new().route("/tarball", get(|| async move { evil_tar }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let url = format!("http://{addr}/tarball");
        let result = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;

        assert!(result.is_err());
        let dest = actions_dir
            .join("owner")
            .join("repo")
            .join("0123456789abcdef0123456789abcdef01234567");
        assert!(
            !dest.exists(),
            "failed download must not leave dest directory behind"
        );
    }

    #[tokio::test]
    async fn download_action_atomic_success_and_cache_hit() {
        use axum::{Router, routing::get};
        let valid_tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);

        let app = Router::new().route("/tarball", get(|| async move { valid_tar }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let url = format!("http://{addr}/tarball");
        let (res, observed) = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await
        .unwrap();

        assert!(res.exists());
        assert!(
            observed.is_none(),
            "an unsupported pin must not compute a digest"
        );
        assert_eq!(
            std::fs::read_to_string(res.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );

        // Second call hits the cache without reaching the server
        let (cached_res, _) = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await
        .unwrap();
        assert_eq!(cached_res, res);
    }

    /// An unresolved action (no SHA-pinned download URL from the
    /// server) must be rejected before any network access — the runner
    /// must not fall back to `api.github.com/repos/{o}/{r}/tarball/{ref}`.
    #[tokio::test]
    async fn download_action_rejects_missing_resolved_url() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let sha = "0123456789abcdef0123456789abcdef01234567";

        let result = download_action(
            "owner",
            "repo",
            sha,
            &actions_dir,
            None,
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;
        let error = result.expect_err("missing resolved URL must fail closed");
        assert!(
            error.to_string().contains("no SHA-pinned download URL"),
            "unexpected error: {error:#}"
        );
        assert!(
            !actions_dir.join("owner").join("repo").join(sha).exists(),
            "rejected download must not create the destination"
        );
    }

    /// A mutable ref (branch/tag/short SHA) that the server failed to
    /// resolve must be rejected before any network access, even when a URL
    /// is supplied. The unreachable URL proves no network attempt happens:
    /// a fetch would fail with a connection error, not the refusal.
    #[tokio::test]
    async fn download_action_rejects_mutable_ref() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        for git_ref in ["v4", "main", "a5ac7e5", "not-a-sha"] {
            let result = download_action(
                "owner",
                "repo",
                git_ref,
                &actions_dir,
                Some("http://127.0.0.1:1/unreachable"),
                None,
                ArchiveDigestPin::Unsupported,
            )
            .await;
            let error = result.expect_err("mutable ref must fail closed");
            assert!(
                error.to_string().contains("not resolved to a commit SHA"),
                "ref {git_ref:?}: unexpected error: {error:#}"
            );
        }
    }

    /// The SHA/URL checks run before the cache lookup — a stale
    /// mutable-ref cache entry must not bypass fail-closed.
    #[tokio::test]
    async fn download_action_rejects_mutable_ref_despite_cache() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let stale = actions_dir.join("owner").join("repo").join("v4");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join("action.yml"), b"stale").unwrap();

        let result = download_action(
            "owner",
            "repo",
            "v4",
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;
        assert!(
            result.is_err(),
            "stale mutable-ref cache entry must not bypass fail-closed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn copy_local_action_rejects_escaping_symlinks() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source_action");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("action.yml"), "name: Local\n").unwrap();

        // Create escaping symlink pointing outside action
        let outside = temp.path().join("secret.txt");
        std::fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink("../secret.txt", source.join("escape_link")).unwrap();

        let actions_dir = temp.path().join("actions");
        let result = copy_local_action(&source, &actions_dir, "my-local-action");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("escaping or absolute symlink")
        );
        assert!(!actions_dir.join("my-local-action").exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_local_action_allows_safe_internal_symlinks() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source_action");
        std::fs::create_dir_all(source.join("dist")).unwrap();
        std::fs::create_dir_all(source.join("bin")).unwrap();
        std::fs::write(source.join("action.yml"), "name: Local\n").unwrap();
        std::fs::write(source.join("dist/index.js"), "console.log('hi');\n").unwrap();

        // Create safe internal symlink
        std::os::unix::fs::symlink("dist/index.js", source.join("main.js")).unwrap();
        // Create safe in-root relative symlink spanning subdirectories
        std::fs::write(source.join("dist/tool.js"), "tool_content").unwrap();
        std::os::unix::fs::symlink("../dist/tool.js", source.join("bin/tool")).unwrap();

        let actions_dir = temp.path().join("actions");
        let dest = copy_local_action(&source, &actions_dir, "my-local-action").unwrap();
        assert!(dest.exists());
        assert_eq!(
            std::fs::read_to_string(dest.join("main.js")).unwrap(),
            "console.log('hi');\n"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("bin/tool")).unwrap(),
            "tool_content"
        );
    }

    /// Serve `tar_bytes` on a loopback axum server; returns the tarball URL.
    /// When `digest` is `Some`, the engine attestation header is set, like
    /// `download_action_tarball` does for a pinned fetch.
    async fn serve_test_tarball_with_digest(tar_bytes: Vec<u8>, digest: Option<String>) -> String {
        use axum::{Router, http::HeaderMap, routing::get};
        let app = Router::new().route(
            "/tarball",
            get(|| async move {
                let mut headers = HeaderMap::new();
                if let Some(digest) = digest {
                    headers.insert(
                        super::ACTION_ARCHIVE_SHA256_HEADER,
                        digest.parse().expect("valid header value"),
                    );
                }
                (headers, tar_bytes)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/tarball")
    }

    /// Serve `tar_bytes` on a loopback axum server; returns the tarball URL.
    async fn serve_test_tarball(tar_bytes: Vec<u8>) -> String {
        serve_test_tarball_with_digest(tar_bytes, None).await
    }

    const TEST_SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn test_action_tarball() -> Vec<u8> {
        create_test_tarball(&[
            ("action-root/action.yml", b"name: Checkout\n"),
            ("action-root/dist/index.js", b"console.log('hi');\n"),
        ])
    }

    #[test]
    fn archive_sha256_hex_is_deterministic_and_content_sensitive() {
        let a = b"fake tarball bytes";
        let b = b"fake tarball bytes";
        let c = b"fake tarball bytes!";
        let ha = super::archive_sha256_hex(a);
        assert_eq!(ha, super::archive_sha256_hex(b));
        assert_eq!(ha.len(), 64);
        assert!(ha.bytes().all(|ch| ch.is_ascii_hexdigit()));
        assert_ne!(ha, super::archive_sha256_hex(c));
        // Known vector: sha256 of empty input.
        assert_eq!(
            super::archive_sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// A fresh download whose archive bytes hash to the pinned digest is
    /// accepted, the observed digest is returned, and the sidecar records
    /// the archive digest for later cache hits.
    #[tokio::test]
    async fn download_action_accepts_matching_archive_pin() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();

        assert!(dest.exists());
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
        let sidecar = actions_dir
            .join("owner")
            .join("repo")
            .join(format!("{TEST_SHA}.sha256"));
        assert_eq!(
            std::fs::read_to_string(&sidecar).unwrap().trim(),
            pin,
            "sidecar must record the verified archive digest"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n"
        );
    }

    /// A fresh download whose archive bytes do NOT hash to the pin fails
    /// closed before extraction: the error names the mismatch, no
    /// destination tree is created, and nothing is executed.
    #[tokio::test]
    async fn download_action_fails_closed_on_archive_mismatch() {
        let url = serve_test_tarball(test_action_tarball()).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);

        let wrong_pin = "0".repeat(64);
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(wrong_pin),
        )
        .await
        .expect_err("archive mismatch must fail closed");
        assert!(
            error.to_string().contains("sha256 mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            !dest.exists(),
            "mismatched download must leave no destination behind"
        );
    }

    /// The pin is compared before extraction: with a wrong pin, even bytes
    /// that are not a valid tarball fail with the mismatch error rather
    /// than an extraction error; with the right pin the same bytes reach
    /// extraction and fail there.
    #[tokio::test]
    async fn download_action_checks_archive_hash_before_extracting() {
        let not_a_tarball = b"this is not a tarball".to_vec();
        let honest_pin = super::archive_sha256_hex(&not_a_tarball);

        // Wrong pin: the mismatch error fires before extraction is attempted.
        let url = serve_test_tarball(not_a_tarball.clone()).await;
        let temp = TempDir::new().unwrap();
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &temp.path().join("actions"),
            Some(&url),
            None,
            ArchiveDigestPin::Pinned("f".repeat(64)),
        )
        .await
        .expect_err("wrong pin must fail closed");
        assert!(
            error.to_string().contains("sha256 mismatch"),
            "hash must be checked before extraction: {error:#}"
        );

        // Right pin for garbage bytes: the hash passes and extraction fails.
        let url = serve_test_tarball(not_a_tarball).await;
        let temp = TempDir::new().unwrap();
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &temp.path().join("actions"),
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(honest_pin),
        )
        .await
        .expect_err("garbage bytes must fail extraction");
        assert!(
            !error.to_string().contains("sha256 mismatch"),
            "matching pin must reach extraction: {error:#}"
        );
    }

    /// An unpinned fresh download returns the observed archive digest,
    /// which is also written to the digest sidecar after extraction so a
    /// later Pinned cache hit can verify against it.
    #[tokio::test]
    async fn download_action_unpinned_fresh_download_returns_observed_digest() {
        let tarball = test_action_tarball();
        let expected = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();

        assert!(dest.exists());
        assert_eq!(
            observed.as_deref(),
            Some(expected.as_str()),
            "fresh download must report the archive's SHA-256"
        );
    }

    /// A cache hit whose sidecar matches the known pin is used without
    /// re-downloading — the pin is enforced, not bypassed, by the cache.
    #[tokio::test]
    async fn download_action_cache_hit_with_matching_pin_uses_cache() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        let (first_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();
        assert_eq!(observed.as_deref(), Some(pin.as_str()));

        // Second call with the now-known pin and an unreachable URL must
        // succeed from cache — no network, no bypass.
        let (cached_dest, cached_observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(cached_dest, first_dest);
        assert_eq!(cached_observed.as_deref(), Some(pin.as_str()));
    }

    /// A poisoned cache entry (sidecar digest differs from the pin) is
    /// evicted and replaced by a fresh verified download — never executed.
    #[tokio::test]
    async fn download_action_evicts_cache_on_pin_mismatch() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();

        // Poison the cache entry and its sidecar.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::write(dest.join("dist/index.js"), b"console.log('pwned');\n").unwrap();
        let sidecar = actions_dir
            .join("owner")
            .join("repo")
            .join(format!("{TEST_SHA}.sha256"));
        std::fs::write(&sidecar, "1".repeat(64)).unwrap();

        // The poisoned entry must be evicted and replaced by a fresh
        // verified download.
        let (fresh_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("dist/index.js")).unwrap(),
            "console.log('hi');\n"
        );
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
        assert_eq!(std::fs::read_to_string(&sidecar).unwrap().trim(), pin);
    }

    /// A pre-feature cache entry (no sidecar) cannot satisfy a known pin:
    /// it is evicted and re-downloaded, so a stale cache never bypasses
    /// the pin.
    #[tokio::test]
    async fn download_action_cache_without_sidecar_cannot_satisfy_pin() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball(tarball).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Simulate a pre-feature cache entry: valid content, no sidecar.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("action.yml"), b"name: Stale\n").unwrap();

        let (fresh_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n",
            "cache of unknown provenance must be replaced by a verified download"
        );
        assert_eq!(observed.as_deref(), Some(pin.as_str()));
    }

    /// An Unpinned cache hit is NOT trusted as-is. The cache entry is
    /// of unknown provenance (job containers mount this directory
    /// read-write), so it is evicted and replaced by a fresh download
    /// verified against the engine-attested digest header — never executed
    /// unverified.
    #[tokio::test]
    async fn download_action_unpinned_cache_hit_evicts_and_redownloads() {
        let tarball = test_action_tarball();
        let digest = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball_with_digest(tarball, Some(digest.clone())).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");

        // Simulate a pre-existing cache entry of unknown provenance.
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("action.yml"), b"name: Existing\n").unwrap();

        let (fresh_dest, observed) = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .unwrap();
        assert_eq!(fresh_dest, dest);
        assert_eq!(
            std::fs::read_to_string(dest.join("action.yml")).unwrap(),
            "name: Checkout\n",
            "stale unpinned cache must be replaced by a verified download"
        );
        assert_eq!(observed.as_deref(), Some(digest.as_str()));
        // The fresh download was verified against the attestation, so its
        // sidecar is trustworthy for later Pinned cache hits.
        let sidecar = actions_dir
            .join("owner")
            .join("repo")
            .join(format!("{TEST_SHA}.sha256"));
        assert_eq!(std::fs::read_to_string(&sidecar).unwrap().trim(), digest);
    }

    /// When the Unpinned re-download fails, the stale cache is still
    /// evicted — the failure surfaces as an error rather than silently
    /// executing the unknown-provenance tree.
    #[tokio::test]
    async fn download_action_unpinned_cache_hit_evicts_even_when_download_fails() {
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);
        std::fs::create_dir_all(&dest).unwrap();
        std::fs::write(dest.join("action.yml"), b"name: Existing\n").unwrap();

        // Unreachable URL: the download must be attempted and fail; the
        // stale cache must not be used as a fallback.
        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some("http://127.0.0.1:1/unreachable"),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .expect_err("unpinned cache must not be used without verification");
        assert!(
            !dest.exists(),
            "stale cache must be evicted, not executed: {error:#}"
        );
    }

    /// A download whose bytes do not match the engine-attested digest
    /// fails closed before extraction, even when Unpinned — no tree left.
    #[tokio::test]
    async fn download_action_fails_closed_on_attestation_mismatch() {
        let url = serve_test_tarball_with_digest(test_action_tarball(), Some("0".repeat(64))).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);

        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Unpinned,
        )
        .await
        .expect_err("attestation mismatch must fail closed");
        assert!(
            error.to_string().contains("sha256 mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            !dest.exists(),
            "mismatched download must leave no destination behind"
        );
    }

    /// A Pinned download whose engine attestation disagrees with the pin
    /// fails closed: the engine attested different bytes than it pinned.
    #[tokio::test]
    async fn download_action_fails_closed_on_pin_attestation_disagreement() {
        let tarball = test_action_tarball();
        let pin = super::archive_sha256_hex(&tarball);
        let url = serve_test_tarball_with_digest(tarball, Some("f".repeat(64))).await;
        let temp = TempDir::new().unwrap();
        let actions_dir = temp.path().join("actions");
        let dest = actions_dir.join("owner").join("repo").join(TEST_SHA);

        let error = download_action(
            "owner",
            "repo",
            TEST_SHA,
            &actions_dir,
            Some(&url),
            None,
            ArchiveDigestPin::Pinned(pin),
        )
        .await
        .expect_err("pin/attestation disagreement must fail closed");
        assert!(
            error.to_string().contains("attestation mismatch"),
            "unexpected error: {error:#}"
        );
        assert!(
            !dest.exists(),
            "disagreeing download must leave no destination behind"
        );
    }

    /// The attestation header parser accepts exactly 64 lowercase hex
    /// chars, normalizing case; absent or malformed headers verify nothing.
    #[test]
    fn header_archive_digest_validates_shape() {
        use reqwest::header::{HeaderMap, HeaderValue};
        let mut headers = HeaderMap::new();
        assert_eq!(super::header_archive_digest(&headers), None);

        headers.insert(
            super::ACTION_ARCHIVE_SHA256_HEADER,
            HeaderValue::from_static("abc"),
        );
        assert_eq!(
            super::header_archive_digest(&headers),
            None,
            "too short is not an attestation"
        );

        let lower = "a".repeat(64);
        headers.insert(
            super::ACTION_ARCHIVE_SHA256_HEADER,
            HeaderValue::from_str(&lower).unwrap(),
        );
        assert_eq!(super::header_archive_digest(&headers), Some(lower));

        headers.insert(
            super::ACTION_ARCHIVE_SHA256_HEADER,
            HeaderValue::from_str(&"A".repeat(64)).unwrap(),
        );
        assert_eq!(
            super::header_archive_digest(&headers),
            Some("a".repeat(64)),
            "uppercase attestation is normalized, matching pin comparison"
        );
    }

    /// Serve a tarball endpoint that answers 429 (optionally carrying
    /// `Retry-After`) until `throttle_until` requests have arrived, then
    /// serves `tar_bytes` with 200. Any other `status` is served on every
    /// request. Returns the tarball URL and the request counter.
    async fn serve_throttled_tarball(
        tar_bytes: Vec<u8>,
        throttle_until: usize,
        retry_after: Option<String>,
        status: axum::http::StatusCode,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use axum::{Router, http::HeaderMap, routing::get};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let hits = Arc::new(AtomicUsize::new(0));
        /// (request counter, tarball bytes, hit# after which 200 is served,
        /// `Retry-After` value, failure status) for the throttled test server.
        type ThrottleState = (
            Arc<AtomicUsize>,
            Vec<u8>,
            usize,
            Option<String>,
            axum::http::StatusCode,
        );
        let state: ThrottleState = (hits.clone(), tar_bytes, throttle_until, retry_after, status);
        let app = Router::new().route(
            "/tarball",
            get(
                |axum::extract::State((hits, tar, throttle_until, retry_after, status)): axum::extract::State<
                    ThrottleState,
                >| async move {
                    let hit = hits.fetch_add(1, Ordering::SeqCst) + 1;
                    if status == axum::http::StatusCode::TOO_MANY_REQUESTS
                        && hit >= throttle_until
                    {
                        return (axum::http::StatusCode::OK, HeaderMap::new(), tar);
                    }
                    let mut headers = HeaderMap::new();
                    if let Some(value) = retry_after {
                        headers.insert(
                            "retry-after",
                            value.parse().expect("valid header value"),
                        );
                    }
                    (status, headers, tar)
                },
            )
            .with_state(state),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}/tarball"), hits)
    }

    /// `convert_retry_after_to_duration` mirrors
    /// `VssNetworkHelper.ConvertRetryAfterToTimeSpan`: an in-range
    /// delta-seconds or HTTP-date is used verbatim; out-of-range values get
    /// the official random band ([min, min+30s) / [max, max+30s), not a hard
    /// clamp); unparseable or past-dated values yield `None` → random backoff.
    #[test]
    fn convert_retry_after_to_duration_clamps_to_random_band() {
        let min = Duration::from_secs(10);
        let max = Duration::from_secs(600);

        assert_eq!(
            convert_retry_after_to_duration(Some("120"), min, max),
            Some(Duration::from_secs(120)),
            "in-range delta-seconds are used as-is"
        );
        let below = convert_retry_after_to_duration(Some("1"), min, max).unwrap();
        assert!(
            below >= min && below < min + Duration::from_secs(30),
            "sub-min delay takes the official random band [10s, 40s), got {below:?}"
        );
        let above = convert_retry_after_to_duration(Some("3600"), min, max).unwrap();
        assert!(
            above >= max && above < max + Duration::from_secs(30),
            "over-max delay takes the official random band [600s, 630s), got {above:?}"
        );
        for empty in [None, Some(""), Some("   "), Some("not-a-date")] {
            assert_eq!(
                convert_retry_after_to_duration(empty, min, max),
                None,
                "{empty:?} must fall back to the random backoff"
            );
        }
    }

    #[test]
    fn convert_retry_after_to_duration_parses_http_date_forms() {
        let min = Duration::from_secs(10);
        let max = Duration::from_secs(600);

        let future = chrono::Utc::now() + chrono::Duration::seconds(120);
        let imf_fixdate = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let rfc850 = future.format("%A, %d-%b-%y %H:%M:%S GMT").to_string();
        let asctime = future.format("%a %b %e %H:%M:%S %Y").to_string();
        for (label, value) in [
            ("IMF-fixdate", imf_fixdate.as_str()),
            ("RFC 850", rfc850.as_str()),
            ("asctime-date", asctime.as_str()),
        ] {
            let delay = convert_retry_after_to_duration(Some(value), min, max).unwrap();
            assert!(
                delay > Duration::from_secs(100) && delay <= Duration::from_secs(120),
                "{label} converts to a delay relative to now, got {delay:?}"
            );
        }

        // Keep accepting the numeric-offset form accepted by the previous
        // parser as well.
        let rfc2822 = future.to_rfc2822();
        let delay = convert_retry_after_to_duration(Some(&rfc2822), min, max).unwrap();
        assert!(
            delay > Duration::from_secs(100) && delay <= Duration::from_secs(120),
            "RFC 2822 form converts to a delay relative to now, got {delay:?}"
        );

        let past = chrono::Utc::now() - chrono::Duration::seconds(60);
        assert_eq!(
            convert_retry_after_to_duration(Some(&past.to_rfc2822()), min, max),
            None,
            "a past HTTP-date is ignored → random backoff"
        );
    }

    /// The throttled backoff prefers the server's Retry-After (converted to
    /// [10s, 10min]) over the 10–30s random range; a missing or unusable
    /// header falls back to the random backoff.
    #[test]
    fn action_download_backoff_prefers_retry_after_header() {
        assert_eq!(
            action_download_backoff(Some("45")),
            Duration::from_secs(45),
            "in-range Retry-After is used verbatim"
        );
        for missing in [None, Some("not-a-date")] {
            let backoff = action_download_backoff(missing);
            assert!(
                backoff >= Duration::from_secs(10) && backoff < Duration::from_secs(30),
                "{missing:?} must fall back to the 10–30s random backoff, got {backoff:?}"
            );
        }
    }

    /// 429 + Retry-After: the loop must retry the download. The kill-switch
    /// skips the real sleep; backoff selection is covered by the
    /// `action_download_backoff_*` / `convert_retry_after_to_duration_*`
    /// unit tests above.
    #[tokio::test]
    async fn download_action_retries_429_with_retry_after_backoff() {
        let tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);
        let (url, hits) = serve_throttled_tarball(
            tar,
            2,
            Some("45".to_string()),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let temp = TempDir::new().unwrap();

        let (res, _) = with_test_env(
            &[("_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF", Some("1".to_string()))],
            download_action(
                "owner",
                "repo",
                "0123456789abcdef0123456789abcdef01234567",
                &temp.path().join("actions"),
                Some(&url),
                None,
                ArchiveDigestPin::Unsupported,
            ),
        )
        .await
        .unwrap();

        assert!(res.exists());
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one throttled attempt, one successful retry"
        );
    }

    /// 429 without Retry-After: the loop must still retry.
    #[tokio::test]
    async fn download_action_retries_429_without_retry_after() {
        let tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);
        let (url, hits) =
            serve_throttled_tarball(tar, 2, None, axum::http::StatusCode::TOO_MANY_REQUESTS).await;
        let temp = TempDir::new().unwrap();

        let (res, _) = with_test_env(
            &[("_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF", Some("1".to_string()))],
            download_action(
                "owner",
                "repo",
                "0123456789abcdef0123456789abcdef01234567",
                &temp.path().join("actions"),
                Some(&url),
                None,
                ArchiveDigestPin::Unsupported,
            ),
        )
        .await
        .unwrap();

        assert!(res.exists());
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// `_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF` disables the sleep but keeps
    /// the retry: the retry still happens even with the switch set.
    #[tokio::test]
    async fn download_action_no_backoff_env_skips_sleep() {
        let tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);
        let (url, hits) = serve_throttled_tarball(
            tar,
            2,
            Some("45".to_string()),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let temp = TempDir::new().unwrap();

        let (res, _) = with_test_env(
            &[("_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF", Some("1".to_string()))],
            download_action(
                "owner",
                "repo",
                "0123456789abcdef0123456789abcdef01234567",
                &temp.path().join("actions"),
                Some(&url),
                None,
                ArchiveDigestPin::Unsupported,
            ),
        )
        .await
        .unwrap();

        assert!(res.exists());
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the retry still happens; only the sleep is skipped"
        );
    }

    /// 429 on every attempt: at most two retries, then the error surfaces.
    #[tokio::test]
    async fn download_action_gives_up_after_two_throttled_retries() {
        let tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);
        let (url, hits) = serve_throttled_tarball(
            tar,
            usize::MAX,
            Some("10".to_string()),
            axum::http::StatusCode::TOO_MANY_REQUESTS,
        )
        .await;
        let temp = TempDir::new().unwrap();

        let result = with_test_env(
            &[("_GITHUB_ACTION_DOWNLOAD_NO_BACKOFF", Some("1".to_string()))],
            download_action(
                "owner",
                "repo",
                "0123456789abcdef0123456789abcdef01234567",
                &temp.path().join("actions"),
                Some(&url),
                None,
                ArchiveDigestPin::Unsupported,
            ),
        )
        .await;

        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("429"),
            "exhausted throttled retries surface the status: {error}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "initial attempt plus two retries"
        );
    }

    /// Non-429 failures keep the existing semantics: a single attempt, the
    /// error propagates immediately, no backoff, no retry.
    #[tokio::test]
    async fn download_action_non_429_fails_fast_without_retry() {
        let tar = create_test_tarball(&[("checkout-v4/action.yml", b"name: Checkout\n")]);
        let (url, hits) = serve_throttled_tarball(
            tar,
            usize::MAX,
            None,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let temp = TempDir::new().unwrap();

        let result = download_action(
            "owner",
            "repo",
            "0123456789abcdef0123456789abcdef01234567",
            &temp.path().join("actions"),
            Some(&url),
            None,
            ArchiveDigestPin::Unsupported,
        )
        .await;

        let error = result.unwrap_err().to_string();
        assert!(
            error.contains("GET") && error.contains("500"),
            "non-429 errors keep the existing error surface: {error}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "non-429 responses are not retried"
        );
    }
}
