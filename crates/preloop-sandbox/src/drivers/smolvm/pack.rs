//! Template artifacts: how a driver turns one into something it can boot.
//!
//! A template is logical — `Artifact { format, arch, ref, digest }` — and each
//! backend implements [`PackFormat`] for the formats it understands. SmolVM
//! accepts a registry image itself (`oci-image`, already digest-pinned) and a
//! monolithic packed machine (`smolmachine`), which this module fetches from a
//! `file://` path or a registry blob into a content-addressed cache, verifying
//! the sha256 before anything boots.
//!
//! The cache is shared by every sandbox on the host and bounded by
//! [`CACHE_MAX_BYTES_ENV`]. Garbage collection evicts least-recently-used
//! artifacts and never one a sandbox still references (an [`ArtifactPin`]).

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::api::Artifact;
use crate::error::{Code, Error, Result};

/// Default cache budget: 50 GiB.
pub const DEFAULT_CACHE_MAX_BYTES: u64 = 50 * 1024 * 1024 * 1024;

/// Cache budget in bytes (`PRELOOP_SANDBOX_CACHE_MAX_BYTES`).
pub const CACHE_MAX_BYTES_ENV: &str = "PRELOOP_SANDBOX_CACHE_MAX_BYTES";

/// Cache directory override (`PRELOOP_SANDBOX_CACHE_DIR`).
pub const CACHE_DIR_ENV: &str = "PRELOOP_SANDBOX_CACHE_DIR";

/// What a format hands back to the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalImage {
    /// A verified file on this host.
    File(PathBuf),
    /// A reference the backend resolves itself.
    Reference(String),
}

impl LocalImage {
    /// The value the SmolVM CLI's `--from`/`--image` takes.
    pub fn into_image(self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Reference(reference) => reference,
        }
    }
}

/// One artifact format a backend supports.
#[async_trait]
pub trait PackFormat: Send + Sync {
    /// Stable format name, as templates spell it.
    fn name(&self) -> &'static str;

    /// Turn `artifact` into something the backend can boot.
    async fn materialize(&self, artifact: &Artifact, cache: &ArtifactCache) -> Result<LocalImage>;
}

/// Pick the first artifact whose format and architecture the backend supports.
///
/// Template order is preference order: a caller that publishes both a packed
/// machine and a registry image decides which one a given host runs.
pub fn select_artifact<'a>(
    artifacts: &'a [Artifact],
    formats: &'a [Arc<dyn PackFormat>],
    arch: &str,
) -> Result<(&'a Artifact, &'a dyn PackFormat)> {
    for artifact in artifacts {
        let Some(format) = formats
            .iter()
            .find(|format| format.name() == artifact.format)
        else {
            continue;
        };
        if artifact.matches_arch(arch) {
            return Ok((artifact, format.as_ref()));
        }
    }
    let offered = artifacts
        .iter()
        .map(|artifact| format!("{}/{}", artifact.format, artifact.arch))
        .collect::<Vec<_>>()
        .join(", ");
    let supported = formats
        .iter()
        .map(|format| format.name())
        .collect::<Vec<_>>()
        .join(", ");
    Err(Error::failed_precondition(format!(
        "no artifact for {arch}: template offers [{offered}], driver supports [{supported}]"
    )))
}

/// Formats the SmolVM driver understands.
pub fn default_formats() -> Vec<Arc<dyn PackFormat>> {
    vec![Arc::new(SmolMachineFormat), Arc::new(OciImageFormat)]
}

/// A monolithic `.smolmachine` pack, local or an OCI artifact blob.
pub struct SmolMachineFormat;

#[async_trait]
impl PackFormat for SmolMachineFormat {
    fn name(&self) -> &'static str {
        Artifact::SMOLMACHINE
    }

    async fn materialize(&self, artifact: &Artifact, cache: &ArtifactCache) -> Result<LocalImage> {
        artifact.validate().map_err(Error::invalid_argument)?;
        // Holding a pin while the bytes are resolved keeps a concurrent GC
        // from deleting the entry before the caller can pin it itself.
        let _pin = cache.pin(&artifact.digest);
        let target = cache.path_for(&artifact.digest);
        if target.is_file() {
            cache.touch(&artifact.digest);
            return Ok(LocalImage::File(target));
        }
        tokio::fs::create_dir_all(cache.root())
            .await
            .map_err(|error| {
                Error::internal(format!(
                    "creating artifact cache {}: {error}",
                    cache.root().display()
                ))
            })?;
        let hex = artifact
            .digest
            .strip_prefix("sha256:")
            .unwrap_or(&artifact.digest)
            .to_ascii_lowercase();
        let partial = cache
            .root()
            .join(format!(".{hex}.{}.partial", uuid::Uuid::new_v4().simple()));
        let stored = match artifact.reference().strip_prefix("file://") {
            Some(source) => copy_verified(Path::new(source), &partial, &artifact.digest).await,
            None => fetch_oci_blob(artifact.reference(), &artifact.digest, &partial).await,
        };
        if let Err(error) = stored {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(error);
        }
        if let Err(error) = tokio::fs::rename(&partial, &target).await {
            let _ = tokio::fs::remove_file(&partial).await;
            return Err(Error::internal(format!(
                "caching artifact {}: {error}",
                artifact.digest
            )));
        }
        cache.touch(&artifact.digest);
        if let Err(error) = cache.collect_garbage() {
            tracing::warn!(%error, "artifact cache GC failed");
        }
        Ok(LocalImage::File(target))
    }
}

/// A registry image SmolVM pulls itself, pinned by digest.
pub struct OciImageFormat;

#[async_trait]
impl PackFormat for OciImageFormat {
    fn name(&self) -> &'static str {
        Artifact::OCI_IMAGE
    }

    async fn materialize(&self, artifact: &Artifact, _cache: &ArtifactCache) -> Result<LocalImage> {
        artifact.validate().map_err(Error::invalid_argument)?;
        Ok(LocalImage::Reference(artifact.reference().to_owned()))
    }
}

/// Content-addressed cache of materialized artifacts.
///
/// One file per digest under `root`, named `<hex>.smolmachine`. LRU ordering is
/// the last use this process saw, falling back to the file's mtime for entries
/// left by an earlier process.
#[derive(Debug, Clone)]
pub struct ArtifactCache {
    root: PathBuf,
    max_bytes: u64,
    state: Arc<Mutex<CacheState>>,
}

#[derive(Debug, Default)]
struct CacheState {
    /// Monotonic use counter per digest; higher is more recent.
    uses: BTreeMap<String, u64>,
    /// Digests a sandbox is still using.
    pins: BTreeMap<String, u32>,
    clock: u64,
}

impl ArtifactCache {
    /// A cache rooted at `root`, evicting once it exceeds `max_bytes`.
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
            state: Arc::new(Mutex::new(CacheState::default())),
        }
    }

    /// A cache configured by the environment: [`CACHE_DIR_ENV`] overrides
    /// `default_root`, [`CACHE_MAX_BYTES_ENV`] overrides the 50 GiB budget.
    pub fn from_env(default_root: impl Into<PathBuf>) -> Self {
        let root = std::env::var_os(CACHE_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| default_root.into());
        let max_bytes = std::env::var(CACHE_MAX_BYTES_ENV)
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(DEFAULT_CACHE_MAX_BYTES);
        Self::new(root, max_bytes)
    }

    /// Cache directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Configured budget.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Where an artifact with `digest` lives once materialized.
    pub fn path_for(&self, digest: &str) -> PathBuf {
        self.root
            .join(format!("{}.smolmachine", digest_hex(digest)))
    }

    /// Whether the artifact is already materialized.
    pub fn contains(&self, digest: &str) -> bool {
        self.path_for(digest).is_file()
    }

    /// Mark the artifact as used now.
    pub fn touch(&self, digest: &str) {
        let mut state = self.state.lock();
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        state.uses.insert(digest_key(digest), clock);
    }

    /// Keep the artifact resident until the returned guard is dropped.
    pub fn pin(&self, digest: &str) -> ArtifactPin {
        let digest = digest_key(digest);
        {
            let mut state = self.state.lock();
            *state.pins.entry(digest.clone()).or_default() += 1;
            state.clock = state.clock.saturating_add(1);
            let clock = state.clock;
            state.uses.insert(digest.clone(), clock);
        }
        ArtifactPin {
            digest,
            state: self.state.clone(),
        }
    }

    /// Evict least-recently-used artifacts until the cache fits its budget.
    ///
    /// Pinned artifacts are skipped. Returns the bytes freed.
    pub fn collect_garbage(&self) -> Result<u64> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(Error::internal(format!(
                    "reading artifact cache {}: {error}",
                    self.root.display()
                )));
            }
        };
        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                Error::internal(format!("reading artifact cache entry: {error}"))
            })?;
            let path = entry.path();
            if path.extension().and_then(OsStr::to_str) != Some("smolmachine") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
                continue;
            };
            if stem.len() != 64 || !stem.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            let digest = format!("sha256:{}", stem.to_ascii_lowercase());
            let modified = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|age| age.as_secs())
                .unwrap_or(0);
            files.push(CacheFile {
                path,
                digest,
                size: metadata.len(),
                modified,
            });
        }
        let mut order = {
            let state = self.state.lock();
            files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    (
                        state.uses.get(&file.digest).copied().unwrap_or(0),
                        file.modified,
                        index,
                    )
                })
                .collect::<Vec<_>>()
        };
        order.sort_unstable();
        let mut total: u64 = files.iter().map(|file| file.size).sum();
        let mut freed = 0;
        for (_, _, index) in order {
            if total <= self.max_bytes {
                break;
            }
            let file = &files[index];
            {
                let state = self.state.lock();
                if state.pins.get(&file.digest).copied().unwrap_or(0) > 0 {
                    continue;
                }
            }
            match std::fs::remove_file(&file.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(
                        path = %file.path.display(),
                        %error,
                        "artifact cache eviction failed"
                    );
                    continue;
                }
            }
            total = total.saturating_sub(file.size);
            freed += file.size;
            self.state.lock().uses.remove(&file.digest);
        }
        Ok(freed)
    }
}

struct CacheFile {
    path: PathBuf,
    digest: String,
    size: u64,
    modified: u64,
}

/// Keeps one cached artifact from being evicted while a sandbox uses it.
#[derive(Debug)]
pub struct ArtifactPin {
    digest: String,
    state: Arc<Mutex<CacheState>>,
}

impl Drop for ArtifactPin {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        if let Some(count) = state.pins.get_mut(&self.digest) {
            *count -= 1;
        }
        if state
            .pins
            .get(&self.digest)
            .is_some_and(|count| *count == 0)
        {
            state.pins.remove(&self.digest);
        }
    }
}

fn digest_hex(digest: &str) -> String {
    digest
        .strip_prefix("sha256:")
        .unwrap_or(digest)
        .to_ascii_lowercase()
}

/// Key used by the in-memory LRU and pin maps: the full normalized digest,
/// matching [`CacheFile::digest`] derived from a cache file name.
fn digest_key(digest: &str) -> String {
    format!("sha256:{}", digest_hex(digest))
}

/// Copy a local pack into the cache, hashing as it goes.
async fn copy_verified(source: &Path, target: &Path, expected: &str) -> Result<()> {
    let mut input = tokio::fs::File::open(source).await.map_err(|error| {
        Error::failed_precondition(format!("opening pack {}: {error}", source.display()))
    })?;
    let mut output = tokio::fs::File::create(target)
        .await
        .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = input.read(&mut buffer).await.map_err(|error| {
            Error::internal(format!("reading pack {}: {error}", source.display()))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        output
            .write_all(&buffer[..read])
            .await
            .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    }
    output
        .flush()
        .await
        .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    let actual = hex::encode(hasher.finalize());
    verify_digest(expected, &actual, &source.display().to_string())
}

/// Fetch an OCI artifact blob by digest into `target`.
///
/// Plain registry v2 over HTTPS: an anonymous bearer token for public
/// registries (GHCR included), then the blob, whose sha256 must match the
/// artifact's digest.
async fn fetch_oci_blob(reference: &str, expected: &str, target: &Path) -> Result<()> {
    let location = RegistryLocation::parse(reference)?;
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(30))
        // A pack is gigabytes: bound a stalled transfer without racing a
        // slow one.
        .timeout(std::time::Duration::from_secs(2 * 60 * 60))
        .user_agent(concat!("preloop-sandbox/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| Error::internal(format!("building registry client: {error}")))?;
    let url = format!(
        "https://{}/v2/{}/blobs/{}",
        location.registry, location.repository, expected
    );
    let mut response = client
        .get(&url)
        .send()
        .await
        .map_err(|error| Error::unavailable(format!("fetching {url}: {error}")))?;
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        let challenge = response
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_challenge);
        let Some(challenge) = challenge else {
            return Err(Error::new(
                Code::PermissionDenied,
                format!("registry `{}` requires authentication", location.registry),
            ));
        };
        let token = request_token(&client, &challenge, &location.repository).await?;
        response = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| Error::unavailable(format!("fetching {url}: {error}")))?;
    }
    let status = response.status();
    if !status.is_success() {
        let code = if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
        {
            Code::PermissionDenied
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Code::FailedPrecondition
        } else {
            Code::Unavailable
        };
        return Err(Error::new(
            code,
            format!(
                "registry blob {expected} from `{}` answered {status}",
                location.registry
            ),
        ));
    }
    let mut output = tokio::fs::File::create(target)
        .await
        .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    let mut hasher = Sha256::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| Error::unavailable(format!("reading {url}: {error}")))?
    {
        hasher.update(&chunk);
        output
            .write_all(&chunk)
            .await
            .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    }
    output
        .flush()
        .await
        .map_err(|error| Error::internal(format!("writing {}: {error}", target.display())))?;
    verify_digest(expected, &hex::encode(hasher.finalize()), reference)
}

fn verify_digest(expected: &str, actual_hex: &str, subject: &str) -> Result<()> {
    let expected_hex = expected.strip_prefix("sha256:").unwrap_or(expected);
    if !expected_hex.eq_ignore_ascii_case(actual_hex) {
        return Err(Error::failed_precondition(format!(
            "{subject}: sha256 mismatch: expected sha256:{expected_hex}, got sha256:{actual_hex}"
        )));
    }
    Ok(())
}

/// Registry and repository of an image reference.
struct RegistryLocation {
    registry: String,
    repository: String,
}

impl RegistryLocation {
    fn parse(reference: &str) -> Result<Self> {
        let name = reference
            .split_once('@')
            .map_or(reference, |(name, _)| name);
        // A tag lives in the last path segment; a registry port keeps its
        // colon because only that segment is inspected.
        let name = match name.rsplit_once('/') {
            Some((prefix, last)) => match last.split_once(':') {
                Some((repository, _)) => format!("{prefix}/{repository}"),
                None => name.to_owned(),
            },
            None => match name.split_once(':') {
                Some((repository, _)) => repository.to_owned(),
                None => name.to_owned(),
            },
        };
        let (registry, repository) = match name.split_once('/') {
            Some((first, rest))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (first.to_owned(), rest.to_owned())
            }
            Some(_) => ("registry-1.docker.io".to_owned(), name.clone()),
            None => ("registry-1.docker.io".to_owned(), format!("library/{name}")),
        };
        if repository.is_empty() || registry.is_empty() {
            return Err(Error::invalid_argument(format!(
                "`{reference}` is not a registry reference"
            )));
        }
        Ok(Self {
            registry,
            repository,
        })
    }
}

struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

fn bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let parameters = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))?;
    let mut realm = None;
    let mut service = None;
    let mut scope = None;
    for (key, value) in challenge_parameters(parameters) {
        match key.as_str() {
            "realm" => realm = Some(value),
            "service" => service = Some(value),
            "scope" => scope = Some(value),
            _ => {}
        }
    }
    Some(BearerChallenge {
        realm: realm?,
        service,
        scope,
    })
}

fn challenge_parameters(raw: &str) -> Vec<(String, String)> {
    let mut parameters = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in raw.chars() {
        match character {
            '"' => {
                quoted = !quoted;
                current.push(character);
            }
            ',' if !quoted => {
                push_challenge_parameter(&mut parameters, &mut current);
            }
            _ => current.push(character),
        }
    }
    push_challenge_parameter(&mut parameters, &mut current);
    parameters
}

fn push_challenge_parameter(parameters: &mut Vec<(String, String)>, raw: &mut String) {
    if let Some((key, value)) = raw.split_once('=') {
        parameters.push((
            key.trim().to_ascii_lowercase(),
            value.trim().trim_matches('"').to_owned(),
        ));
    }
    raw.clear();
}

async fn request_token(
    client: &reqwest::Client,
    challenge: &BearerChallenge,
    repository: &str,
) -> Result<String> {
    let mut url = reqwest::Url::parse(&challenge.realm).map_err(|error| {
        Error::new(
            Code::PermissionDenied,
            format!("registry token realm `{}`: {error}", challenge.realm),
        )
    })?;
    if url.scheme() != "https" {
        return Err(Error::new(
            Code::PermissionDenied,
            format!(
                "registry token realm `{}` is not https; refusing to send a token request over \
                 plain HTTP",
                challenge.realm
            ),
        ));
    }
    let scope = challenge
        .scope
        .clone()
        .unwrap_or_else(|| format!("repository:{repository}:pull"));
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("scope", &scope);
        if let Some(service) = &challenge.service {
            query.append_pair("service", service);
        }
    }
    let response = client
        .get(url.clone())
        // The token exchange is a small metadata request; it must not inherit
        // the blob transfer's hours.
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|error| Error::unavailable(format!("requesting {url}: {error}")))?;
    if !response.status().is_success() {
        return Err(Error::new(
            Code::PermissionDenied,
            format!(
                "registry token endpoint {} answered {}",
                url,
                response.status()
            ),
        ));
    }
    let body: serde_json::Value = response
        .json()
        .await
        .map_err(|error| Error::unavailable(format!("reading {url}: {error}")))?;
    body.get("token")
        .or_else(|| body.get("access_token"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::new(
                Code::PermissionDenied,
                format!("registry token endpoint {url} handed back no token"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_for(path: &Path) -> Artifact {
        let bytes = std::fs::read(path).unwrap();
        Artifact {
            format: Artifact::SMOLMACHINE.into(),
            arch: crate::api::host_arch().into(),
            reference: format!("file://{}", path.display()),
            digest: format!("sha256:{}", hex::encode(Sha256::digest(&bytes))),
            size_bytes: bytes.len() as u64,
        }
    }

    fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[tokio::test]
    async fn a_file_pack_is_verified_and_then_served_from_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let source = write(dir.path(), "runner.smolmachine", b"packed machine");
        let cache = ArtifactCache::new(dir.path().join("cache"), DEFAULT_CACHE_MAX_BYTES);
        let artifact = artifact_for(&source);

        let image = SmolMachineFormat
            .materialize(&artifact, &cache)
            .await
            .unwrap();
        let LocalImage::File(cached) = image else {
            panic!("a file pack must materialize as a file");
        };
        assert_eq!(cached, cache.path_for(&artifact.digest));
        assert_eq!(std::fs::read(&cached).unwrap(), b"packed machine");

        // The second call must not need the source at all: a cache hit.
        std::fs::remove_file(&source).unwrap();
        let again = SmolMachineFormat
            .materialize(&artifact, &cache)
            .await
            .unwrap();
        assert_eq!(again, LocalImage::File(cached));
    }

    #[tokio::test]
    async fn a_digest_mismatch_is_rejected_and_leaves_nothing_cached() {
        let dir = tempfile::tempdir().unwrap();
        let source = write(dir.path(), "runner.smolmachine", b"packed machine");
        let cache = ArtifactCache::new(dir.path().join("cache"), DEFAULT_CACHE_MAX_BYTES);
        let mut artifact = artifact_for(&source);
        artifact.digest = format!("sha256:{}", "0".repeat(64));

        let error = SmolMachineFormat
            .materialize(&artifact, &cache)
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::FailedPrecondition);
        assert!(
            error.message.contains("sha256 mismatch"),
            "{}",
            error.message
        );
        assert!(!cache.path_for(&artifact.digest).exists());
    }

    #[tokio::test]
    async fn garbage_collection_evicts_the_least_recently_used_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let first = artifact_for(&write(dir.path(), "first.smolmachine", b"first"));
        let second = artifact_for(&write(dir.path(), "second.smolmachine", b"second"));
        let third = artifact_for(&write(dir.path(), "third.smolmachine", b"third"));
        // 5 + 6 + 5 bytes; two fit, the oldest is evicted when the third lands.
        let cache = ArtifactCache::new(dir.path().join("cache"), 11);

        SmolMachineFormat.materialize(&first, &cache).await.unwrap();
        SmolMachineFormat
            .materialize(&second, &cache)
            .await
            .unwrap();
        SmolMachineFormat.materialize(&third, &cache).await.unwrap();

        assert!(!cache.contains(&first.digest), "oldest artifact must go");
        assert!(cache.contains(&second.digest));
        assert!(cache.contains(&third.digest));
    }

    #[tokio::test]
    async fn garbage_collection_spares_an_artifact_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let first = write(dir.path(), "first.smolmachine", b"first");
        let second = write(dir.path(), "second.smolmachine", b"second");
        let third = write(dir.path(), "third.smolmachine", b"third");
        let cache = ArtifactCache::new(dir.path().join("cache"), 12);
        let first = artifact_for(&first);
        let second = artifact_for(&second);
        let third = artifact_for(&third);
        let _pin = cache.pin(&first.digest);

        for artifact in [&first, &second, &third] {
            SmolMachineFormat
                .materialize(artifact, &cache)
                .await
                .unwrap();
        }

        assert!(cache.contains(&first.digest), "pinned artifact evicted");
        assert!(
            !cache.contains(&second.digest),
            "unpinned LRU artifact kept"
        );
        assert!(cache.contains(&third.digest));
    }

    #[test]
    fn selection_prefers_template_order_then_filters_format_and_arch() {
        let formats = default_formats();
        let arm = |format: &str| Artifact {
            format: format.into(),
            arch: "arm64".into(),
            reference: format!("ghcr.io/preloop/base@sha256:{}", "a".repeat(64)),
            digest: format!("sha256:{}", "a".repeat(64)),
            size_bytes: 0,
        };
        let artifacts = vec![arm("unknown"), arm(Artifact::OCI_IMAGE)];
        let (artifact, format) = select_artifact(&artifacts, &formats, "aarch64").unwrap();
        assert_eq!(artifact.format, Artifact::OCI_IMAGE);
        assert_eq!(format.name(), Artifact::OCI_IMAGE);

        let artifacts = vec![
            Artifact {
                arch: "amd64".into(),
                ..arm(Artifact::SMOLMACHINE)
            },
            arm(Artifact::SMOLMACHINE),
        ];
        let (artifact, format) = select_artifact(&artifacts, &formats, "aarch64").unwrap();
        assert_eq!(artifact.format, Artifact::SMOLMACHINE);
        assert_eq!(format.name(), Artifact::SMOLMACHINE);

        let Err(error) = select_artifact(&[arm(Artifact::SMOLMACHINE)], &formats, "x86_64") else {
            panic!("an arm-only template must not match an x86_64 host");
        };
        assert_eq!(error.code, Code::FailedPrecondition);
    }
}
