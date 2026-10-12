//! Local cache storage compatible with GitHub Actions cache semantics.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// POSIX `NAME_MAX` for the filesystems we target (APFS, ext4, XFS, overlayfs):
/// a single path component may not exceed 255 bytes.
const MAX_NAME_BYTES: usize = 255;

/// Default total quota for finalized cache entries: 10 GiB. Matches GitHub's
/// 10 GB per-repository cache limit; this is the server-wide total across
/// every cache namespace. Operators override it with `[cache]
/// total_quota_bytes` (or `PRELOOP_CACHE_TOTAL_QUOTA_BYTES`).
pub const DEFAULT_CACHE_QUOTA_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// How long an interrupted upload may leave debris behind — a `.part` file
/// with no committed archive, or metadata with no archive — before quota
/// enforcement reaps it. Crashed writers never come back to finish those, and
/// without reaping they would wedge the entry (and the quota) forever.
const STALE_WRITE_MAX_AGE: Duration = Duration::from_secs(3600);

/// Cache storage error.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// I/O failed.
    #[error("cache io error: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid cache key.
    #[error("invalid cache key: {0}")]
    InvalidKey(String),
    /// A cache with the same key and version already exists. GitHub caches are immutable.
    #[error("cache `{key}` with version `{version}` already exists")]
    AlreadyExists {
        /// Cache key.
        key: String,
        /// Cache version.
        version: String,
    },
}

/// Cache entry metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    /// Cache key.
    pub key: String,
    /// Cache version.
    pub version: String,
    /// Archive path.
    pub path: PathBuf,
    /// Archive size in bytes.
    pub size: u64,
}

/// File-backed cache store.
#[derive(Debug, Clone)]
pub struct CacheStore {
    root: PathBuf,
    /// Total bytes of finalized entries the store keeps. When a commit
    /// pushes the total over this quota, least-recently-used entries are
    /// evicted until the total fits again.
    quota_bytes: u64,
}

impl CacheStore {
    /// Create a cache store rooted at `root`, with the default quota
    /// ([`DEFAULT_CACHE_QUOTA_BYTES`]).
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self, CacheError> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self {
            root,
            quota_bytes: DEFAULT_CACHE_QUOTA_BYTES,
        })
    }

    /// Override the total quota for finalized entries. The server sets this
    /// from `[cache] total_quota_bytes` at startup.
    pub fn set_quota_bytes(&mut self, quota_bytes: u64) {
        self.quota_bytes = quota_bytes;
    }

    /// The effective total quota for finalized entries.
    pub fn quota_bytes(&self) -> u64 {
        self.quota_bytes
    }

    /// Claim the entry directory and write the metadata sidecars. The caller
    /// places `archive.tzst` inside the returned directory, then calls
    /// [`CacheStore::finish_commit`]. A directory left without an archive
    /// (crashed writer) is reaped by quota enforcement once stale.
    async fn begin_commit(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
    ) -> Result<PathBuf, CacheError> {
        validate_key(key, "Cache")?;
        let directory = self.entry_dir_scoped(namespace, key, version);
        match fs::create_dir(&directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(CacheError::AlreadyExists {
                    key: key.to_owned(),
                    version: version.to_owned(),
                });
            }
            Err(error) => return Err(error.into()),
        }
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();
        if let Err(error) = async {
            fs::write(directory.join("namespace"), namespace).await?;
            fs::write(directory.join("key"), key).await?;
            fs::write(directory.join("version"), version).await?;
            fs::write(directory.join("created_at"), created_at).await?;
            Ok::<(), std::io::Error>(())
        }
        .await
        {
            let _ = fs::remove_dir_all(&directory).await;
            return Err(error.into());
        }
        Ok(directory)
    }

    /// Mark the archive committed: refresh its LRU timestamp and evict
    /// least-recently-used entries while the store is over quota. `protect`
    /// is the just-committed archive, which is never evicted by its own
    /// commit — a single entry larger than the quota stays until a later
    /// commit evicts it.
    async fn finish_commit(
        &self,
        directory: &Path,
        key: &str,
        version: &str,
        protect: &Path,
    ) -> Result<CacheEntry, CacheError> {
        let archive = directory.join("archive.tzst");
        let size = fs::metadata(&archive).await?.len();
        touch_file(&archive).await;
        self.enforce_quota_except(Some(protect)).await?;
        Ok(CacheEntry {
            key: key.to_owned(),
            version: version.to_owned(),
            path: archive,
            size,
        })
    }

    /// Commit an already-staged blob file as a cache archive without ever
    /// loading it into RAM: the file is renamed into the entry directory
    /// (same filesystem), falling back to a bounded-buffer streaming copy.
    /// The staging file is consumed — moved or deleted — on success.
    ///
    /// This is the finalize path for chunked uploads: chunks were streamed
    /// to `src` as they arrived, so committing a 150 MB blob costs only a
    /// rename and a directory scan, not a full-blob read.
    pub async fn commit_file_scoped(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
        src: &Path,
    ) -> Result<CacheEntry, CacheError> {
        if !fs::try_exists(src).await? {
            return Err(CacheError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("staged cache file not found: {}", src.display()),
            )));
        }
        let directory = self.begin_commit(namespace, key, version).await?;
        let archive = directory.join("archive.tzst");
        let staged = async {
            match fs::rename(src, &archive).await {
                Ok(()) => Ok(()),
                Err(_) => {
                    // Cross-filesystem staging (or a platform without an
                    // atomic rename): stream the bytes over with a small
                    // constant buffer instead of failing the commit.
                    stream_copy(src, &archive).await?;
                    fs::remove_file(src).await?;
                    Ok(())
                }
            }
        }
        .await;
        if let Err(error) = staged {
            let _ = fs::remove_dir_all(&directory).await;
            return Err(error);
        }
        self.finish_commit(&directory, key, version, &archive).await
    }

    /// Commit an already-staged blob file in the default (unscoped) namespace.
    pub async fn commit_file(
        &self,
        key: &str,
        version: &str,
        src: &Path,
    ) -> Result<CacheEntry, CacheError> {
        self.commit_file_scoped("", key, version, src).await
    }

    /// Save an immutable cache archive.
    ///
    /// Reference: GitHub Actions dependency caching documentation, “Cache key
    /// matching”, and `actions/toolkit/packages/cache/src/cache.ts::checkKey`.
    /// Keys are at most 512 Unicode scalar values, must be non-empty, and may
    /// not contain commas. The filesystem identity is a fixed SHA-256 digest;
    /// the original key/version are persisted as metadata for prefix matching.
    pub async fn put(
        &self,
        key: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<CacheEntry, CacheError> {
        self.put_scoped("", key, version, bytes).await
    }

    /// Save an immutable archive in an isolated namespace. The namespace is
    /// part of the filesystem identity, but is not counted against the
    /// Actions cache key's 512 UTF-16-unit limit.
    pub async fn put_scoped(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
        bytes: &[u8],
    ) -> Result<CacheEntry, CacheError> {
        let directory = self.begin_commit(namespace, key, version).await?;
        let archive = directory.join("archive.tzst");
        if let Err(error) = fs::write(&archive, bytes).await {
            let _ = fs::remove_dir_all(&directory).await;
            return Err(error.into());
        }
        self.finish_commit(&directory, key, version, &archive).await
    }

    /// Resolve a cache entry's metadata without reading the archive bytes.
    /// Restores that only need the entry identity (existence checks, download
    /// URL minting) use this so a 150 MB restore lookup does not pull 150 MB
    /// into RAM just to throw the bytes away. Every successful resolution
    /// refreshes the entry's LRU timestamp.
    pub async fn lookup(
        &self,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<CacheEntry>, CacheError> {
        self.lookup_scoped("", key, version, restore_keys).await
    }

    /// Resolve a cache entry's metadata within one namespace, without reading
    /// the archive bytes. See [`CacheStore::lookup`].
    pub async fn lookup_scoped(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<CacheEntry>, CacheError> {
        let entry = self
            .resolve_scoped(namespace, key, version, restore_keys)
            .await?;
        if let Some(entry) = &entry {
            touch_file(&entry.path).await;
        }
        Ok(entry)
    }

    /// Restore a cache using GitHub's lookup order: exact primary key, partial
    /// primary key, then each restore key in declaration order. Prefix matches
    /// select the newest matching immutable cache.
    pub async fn get(
        &self,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<(CacheEntry, Vec<u8>)>, CacheError> {
        self.get_scoped("", key, version, restore_keys).await
    }

    /// Restore a cache within one namespace. Exact and prefix matching never
    /// crosses namespace boundaries.
    pub async fn get_scoped(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<(CacheEntry, Vec<u8>)>, CacheError> {
        let Some(entry) = self
            .lookup_scoped(namespace, key, version, restore_keys)
            .await?
        else {
            return Ok(None);
        };
        let bytes = fs::read(&entry.path).await?;
        Ok(Some((entry, bytes)))
    }

    /// Shared entry resolution for [`CacheStore::lookup_scoped`] and
    /// [`CacheStore::get_scoped`]: key validation, exact-path probe (plus the
    /// legacy layout probe), then prefix scan. Does not touch the LRU
    /// timestamp; callers do that once they know the entry is used.
    async fn resolve_scoped(
        &self,
        namespace: &str,
        key: &str,
        version: &str,
        restore_keys: &[String],
    ) -> Result<Option<CacheEntry>, CacheError> {
        validate_key(key, "Cache")?;
        if restore_keys.len() > 10 {
            return Err(CacheError::InvalidKey(format!(
                "at most 10 restore keys are supported, got {}",
                restore_keys.len()
            )));
        }
        for restore_key in restore_keys {
            validate_key(restore_key, "Restore")?;
        }

        let exact = self.path_for_scoped(namespace, key, version);
        let exact_path = if fs::try_exists(&exact).await? {
            Some(exact)
        } else if let Some(legacy) = namespace
            .is_empty()
            .then(|| self.legacy_path_for(key, version))
            .flatten()
        {
            fs::try_exists(&legacy).await?.then_some(legacy)
        } else {
            None
        };
        if let Some(path) = exact_path {
            let metadata = fs::metadata(&path).await?;
            return Ok(Some(CacheEntry {
                key: key.to_owned(),
                version: version.to_owned(),
                path,
                size: metadata.len(),
            }));
        }

        for prefix in std::iter::once(key).chain(restore_keys.iter().map(String::as_str)) {
            if let Some(entry) = self.find_prefix_scoped(namespace, prefix, version).await? {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    fn entry_dir_scoped(&self, namespace: &str, key: &str, version: &str) -> PathBuf {
        self.root.join(entry_id_scoped(namespace, key, version))
    }

    #[cfg(test)]
    fn path_for(&self, key: &str, version: &str) -> PathBuf {
        self.path_for_scoped("", key, version)
    }

    fn path_for_scoped(&self, namespace: &str, key: &str, version: &str) -> PathBuf {
        self.entry_dir_scoped(namespace, key, version)
            .join("archive.tzst")
    }
    fn legacy_path_for(&self, key: &str, version: &str) -> Option<PathBuf> {
        let key_component = hex(key.as_bytes());
        let mut version_hasher = Sha256::new();
        version_hasher.update(version.as_bytes());
        let version_hash = hex(version_hasher.finalize());
        let version_hash = &version_hash[..16.min(version_hash.len())];
        let mut identity = Sha256::new();
        identity.update(key.as_bytes());
        identity.update(b"\0");
        identity.update(version.as_bytes());
        let component = format!(
            "{key_component}-{version_hash}-{}",
            hex(identity.finalize())
        );
        // The legacy layout used the raw hex-encoded key as part of a single
        // directory component. A component longer than NAME_MAX can never have
        // been written by that layout, and probing it fails with ENAMETOOLONG
        // instead of a clean not-found miss — which surfaced as a 500 on cache
        // restore. Skip the probe rather than let the syscall error escape.
        if component.len() > MAX_NAME_BYTES {
            return None;
        }
        Some(self.root.join(component).join("archive.tzst"))
    }

    async fn find_prefix_scoped(
        &self,
        namespace: &str,
        prefix: &str,
        version: &str,
    ) -> Result<Option<CacheEntry>, CacheError> {
        let mut directory = fs::read_dir(&self.root).await?;
        let mut newest: Option<(CacheEntry, u128)> = None;
        while let Some(candidate) = directory.next_entry().await? {
            let entry_dir = candidate.path();
            let path = entry_dir.join("archive.tzst");
            if !fs::try_exists(&path).await? {
                continue;
            }
            let stored_namespace = match fs::read_to_string(entry_dir.join("namespace")).await {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
                Err(error) => return Err(error.into()),
            };
            let (Ok(key), Ok(stored_version), Ok(created_at)) = (
                fs::read_to_string(entry_dir.join("key")).await,
                fs::read_to_string(entry_dir.join("version")).await,
                fs::read_to_string(entry_dir.join("created_at")).await,
            ) else {
                continue;
            };
            if stored_namespace != namespace
                || stored_version != version
                || !key.starts_with(prefix)
            {
                continue;
            }
            let Ok(created_at) = created_at.parse::<u128>() else {
                continue;
            };
            let metadata = fs::metadata(&path).await?;
            let entry = CacheEntry {
                key,
                version: version.to_owned(),
                path,
                size: metadata.len(),
            };
            if newest
                .as_ref()
                .map(|(_, current)| created_at > *current)
                .unwrap_or(true)
            {
                newest = Some((entry, created_at));
            }
        }
        Ok(newest.map(|(entry, _)| entry))
    }

    /// Evict least-recently-used entries until the finalized total fits the
    /// quota. Also reaps stale incomplete writes (crashed uploads) so they
    /// can never wedge an entry or the quota. Runs after every commit and at
    /// server startup.
    pub async fn enforce_quota(&self) -> Result<(), CacheError> {
        self.enforce_quota_except(None).await
    }

    async fn enforce_quota_except(&self, protect: Option<&Path>) -> Result<(), CacheError> {
        let mut entries: Vec<(PathBuf, u64, SystemTime)> = Vec::new();
        let mut read_dir = fs::read_dir(&self.root).await?;
        while let Some(child) = read_dir.next_entry().await? {
            if !child.file_type().await?.is_dir() {
                continue;
            }
            let dir = child.path();
            let archive = dir.join("archive.tzst");
            match fs::metadata(&archive).await {
                Ok(metadata) => {
                    // Entries whose mtime cannot be read sort as oldest and
                    // are evicted first — the safe direction.
                    let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
                    entries.push((archive, metadata.len(), mtime));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    self.reap_stale_incomplete(&dir).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let mut total: u64 = entries.iter().map(|(_, size, _)| size).sum();
        if total <= self.quota_bytes {
            return Ok(());
        }
        // Oldest use first.
        entries.sort_by_key(|entry| entry.2);
        for (archive, size, _) in entries {
            if total <= self.quota_bytes {
                break;
            }
            if protect.is_some_and(|protected| protected == archive.as_path()) {
                continue;
            }
            // Best-effort: a concurrent reader may hold the file; it will be
            // reaped on the next commit.
            if let Some(parent) = archive.parent()
                && fs::remove_dir_all(parent).await.is_ok()
            {
                total = total.saturating_sub(size);
            }
        }
        Ok(())
    }

    /// Remove an entry directory that holds no committed archive once it is
    /// older than [`STALE_WRITE_MAX_AGE`]: a crashed writer's debris. Fresh
    /// incomplete directories are left alone — a commit may be in flight.
    async fn reap_stale_incomplete(&self, dir: &Path) {
        let mut newest = UNIX_EPOCH;
        let mut any = false;
        if let Ok(mut read_dir) = fs::read_dir(dir).await {
            while let Ok(Some(entry)) = read_dir.next_entry().await {
                any = true;
                if let Ok(metadata) = entry.metadata().await
                    && let Ok(modified) = metadata.modified()
                {
                    newest = newest.max(modified);
                }
            }
        }
        // An empty directory is always debris: `begin_commit` writes the
        // metadata sidecars immediately after creating it.
        let stale = !any
            || SystemTime::now()
                .duration_since(newest)
                .is_ok_and(|age| age > STALE_WRITE_MAX_AGE);
        if stale {
            let _ = fs::remove_dir_all(dir).await;
        }
    }
}

/// Refresh a file's mtime to now. Best-effort: filesystems that cannot set
/// mtimes simply age entries by creation time.
async fn touch_file(path: &Path) {
    let _ = std::fs::File::options()
        .write(true)
        .open(path)
        .and_then(|file| file.set_modified(SystemTime::now()));
}

/// Copy `src` to `dst` with a small constant buffer, never loading the whole
/// file into RAM. Fallback for commits whose staging area lives on another
/// filesystem than the cache root (the fast path is an atomic rename).
async fn stream_copy(src: &Path, dst: &Path) -> Result<(), CacheError> {
    let mut reader = fs::File::open(src).await?;
    let mut writer = fs::File::create(dst).await?;
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

/// Match `actions/toolkit` JavaScript `String.length` semantics: cache keys
/// are limited to 512 UTF-16 code units, not Rust UTF-8 bytes.
fn validate_key(key: &str, kind: &str) -> Result<(), CacheError> {
    let utf16_length = key.encode_utf16().count();
    if key.is_empty() || utf16_length > 512 || key.contains(',') {
        return Err(CacheError::InvalidKey(format!(
            "{kind} key must be non-empty, at most 512 UTF-16 code units, and contain no commas"
        )));
    }
    Ok(())
}

fn entry_id(key: &str, version: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hasher.update(b"\0");
    hasher.update(version.as_bytes());
    hex(hasher.finalize())
}

fn entry_id_scoped(namespace: &str, key: &str, version: &str) -> String {
    if namespace.is_empty() {
        return entry_id(key, version);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"preloop-cache-namespace-v1\0");
    for component in [namespace, key, version] {
        hasher.update((component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    hex(hasher.finalize())
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
/// Return true if a path is inside a configured root.
pub fn is_under_root(root: &Path, path: &Path) -> bool {
    path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid_key() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("".to_owned()),
            prop::string::string_regex("[a-zA-Z0-9_-]{513,600}").unwrap(),
        ]
    }
    #[tokio::test]
    async fn stores_and_restores_exact_cache() {
        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path()).await.unwrap();

        store.put("linux-node", "v1", b"payload").await.unwrap();
        let (_entry, bytes) = store.get("linux-node", "v1", &[]).await.unwrap().unwrap();

        assert_eq!(bytes, b"payload");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_cache_key_path_safety(ref key in "\\PC*", ref version in "\\PC*") {
            let temp = tempfile::tempdir().unwrap();
            let store = tokio::runtime::Runtime::new().unwrap().block_on(async {
                CacheStore::new(temp.path()).await.unwrap()
            });
            let path = store.path_for(key, version);
            assert!(is_under_root(temp.path(), &path));
        }

        #[test]
        fn test_cache_roundtrip(
            ref key in "[a-zA-Z0-9_-]{1,32}",
            ref version in "[a-zA-Z0-9_-]{1,32}",
            ref payload in prop::collection::vec(0..=255u8, 0..1024)
        ) {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                store.put(key, version, payload).await.unwrap();
                let (entry, restored) = store.get(key, version, &[]).await.unwrap().unwrap();
                assert_eq!(entry.key, *key);
                assert_eq!(entry.version, *version);
                assert_eq!(restored, *payload);
            });
        }

        #[test]
        fn test_cache_prefix_restore(
            ref key_prefix in "[a-zA-Z0-9_-]{5,10}",
            ref key_suffix1 in "[a-zA-Z0-9_-]{1,10}",
            ref key_suffix2 in "[a-zA-Z0-9_-]{1,10}",
            ref version in "[a-zA-Z0-9_-]{1,10}",
            ref payload1 in prop::collection::vec(0..=255u8, 0..100),
            ref payload2 in prop::collection::vec(0..=255u8, 0..100)
        ) {
            if key_suffix1 == key_suffix2 {
                return Ok(());
            }
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                let key1 = format!("{}{}", key_prefix, key_suffix1);
                let key2 = format!("{}{}", key_prefix, key_suffix2);
                store.put(&key1, version, payload1).await.unwrap();
                store.put(&key2, version, payload2).await.unwrap();
                let restored = store.get("non-existent-key", version, std::slice::from_ref(key_prefix)).await.unwrap();
                let (entry, bytes) = restored.expect("prefix cache must resolve");
                assert_eq!(entry.key, key2);
                assert_eq!(bytes, *payload2);
            });
        }


        #[test]
        fn test_cache_invalid_keys(ref key in invalid_key(), ref version in "[a-zA-Z0-9_-]{1,32}") {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = CacheStore::new(temp.path()).await.unwrap();
                let res = store.put(key, version, b"payload").await;
                assert!(matches!(res, Err(CacheError::InvalidKey(_))));

                let res_get = store.get(key, version, &[]).await;
                assert!(matches!(res_get, Err(CacheError::InvalidKey(_))));

                let res_get_rk = store.get("valid-key", version, std::slice::from_ref(key)).await;
                assert!(matches!(res_get_rk, Err(CacheError::InvalidKey(_))));
            });
        }
    }

    #[test]
    fn long_keys_do_not_fail_lookup_with_name_too_long() {
        let temp = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = CacheStore::new(temp.path()).await.unwrap();
            // The legacy layout hex-encodes the key as a directory component;
            // a key this long overflows NAME_MAX and the legacy probe used
            // to surface ENAMETOOLONG as a 500 on restore.
            let long_key = "v0-check-".to_owned() + &"a".repeat(400);
            let version = "v-".to_owned() + &"b".repeat(64);
            // Put + exact restore round-trip through the hashed layout.
            store.put(&long_key, &version, b"payload").await.unwrap();
            let (entry, bytes) = store
                .get(&long_key, &version, &[])
                .await
                .expect("lookup must not fail on key length")
                .expect("entry must be found");
            assert_eq!(entry.key, long_key);
            assert_eq!(bytes, b"payload");
            // A restore-key probe for a missing entry must also succeed.
            let missing = store
                .get(&long_key, &version, std::slice::from_ref(&long_key))
                .await
                .expect("prefix probe must not fail on key length");
            assert!(missing.is_some());
            let unknown = store
                .get(&"z".repeat(300), &version, &[])
                .await
                .expect("unknown long key must not fail the lookup");
            assert!(unknown.is_none());
        });
    }

    #[test]
    fn legacy_probe_never_errors_across_name_max_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = CacheStore::new(temp.path()).await.unwrap();
            let version = "v1";
            // The legacy component is `{key_hex}-{version_hash_16}-{identity_hex_64}`,
            // i.e. `2 * key.len() + 82` bytes. 86-byte keys are the largest that
            // fit in NAME_MAX (254 bytes); 87 bytes is the first that does not
            // (256 bytes) and used to raise ENAMETOOLONG from the probe.
            for key_len in 84..=92 {
                let stored = "s".repeat(key_len);
                store.put(&stored, version, b"payload").await.unwrap();
                let hit = store
                    .get(&stored, version, &[])
                    .await
                    .unwrap_or_else(|error| panic!("get errored at len {key_len}: {error}"));
                let (entry, bytes) = hit.expect("stored entry must be found");
                assert_eq!(entry.key, stored);
                assert_eq!(bytes, b"payload");

                let unknown = "u".repeat(key_len);
                let miss = store
                    .get(&unknown, version, &[])
                    .await
                    .unwrap_or_else(|error| panic!("miss errored at len {key_len}: {error}"));
                assert!(miss.is_none(), "unknown {key_len}-byte key must miss");
            }
        });
    }

    /// Current process RSS in bytes, Linux only. Used to prove commits never
    /// buffer the full blob in RAM.
    #[cfg(target_os = "linux")]
    fn rss_bytes() -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status
                    .lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|kb| kb.parse::<u64>().ok())
            })
            .map(|kb| kb * 1024)
            .unwrap_or(0)
    }

    /// Committing a staged blob file must stream it into place: finalizing a
    /// 150 MB blob (the round-3 repro size) must not spike process RSS the
    /// way the old full-blob-in-RAM read did (+265 MB).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn commit_file_streams_large_blob_with_small_rss_delta() {
        use std::io::Write as _;

        const BLOB_MB: usize = 150;
        const RSS_DELTA_LIMIT: u64 = 64 * 1024 * 1024;

        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path().join("cache")).await.unwrap();

        // Stage a 150 MB blob on disk in 1 MiB chunks, the way chunked
        // uploads arrive. Writing it must not inflate process RSS either
        // (page cache, not process memory) — the assertion below covers the
        // whole finalize path.
        let stage = temp.path().join("stage.bin");
        let chunk = vec![0xABu8; 1024 * 1024];
        let mut file = std::fs::File::create(&stage).unwrap();
        for _ in 0..BLOB_MB {
            file.write_all(&chunk).unwrap();
        }
        drop(file);

        let before = rss_bytes();
        assert!(before > 0, "could not read process RSS");
        let entry = store
            .commit_file("big-blob", "v1", &stage)
            .await
            .expect("commit must succeed");
        let after = rss_bytes();
        let delta = after.saturating_sub(before);

        assert_eq!(entry.size, (BLOB_MB * 1024 * 1024) as u64);
        assert!(
            !stage.exists(),
            "the staging file must be consumed by the commit"
        );
        let (_restored_entry, bytes) = store
            .get("big-blob", "v1", &[])
            .await
            .unwrap()
            .expect("entry must be readable");
        assert_eq!(bytes.len(), BLOB_MB * 1024 * 1024);
        assert!(
            delta < RSS_DELTA_LIMIT,
            "finalizing a {BLOB_MB} MB blob spiked RSS by {} MiB (limit {} MiB)",
            delta / (1024 * 1024),
            RSS_DELTA_LIMIT / (1024 * 1024),
        );
    }

    /// Quota enforcement evicts least-recently-used entries first: filling
    /// past the quota drops the oldest untouched entry while a recently read
    /// (older-created) entry and the newest entries survive.
    #[tokio::test]
    async fn quota_evicts_least_recently_used() {
        const MIB: usize = 1024 * 1024;
        let temp = tempfile::tempdir().unwrap();
        let mut store = CacheStore::new(temp.path()).await.unwrap();
        store.set_quota_bytes(3 * MIB as u64);

        let payload = vec![0xCDu8; MIB];
        for key in ["k1", "k2", "k3"] {
            store.put(key, "v", &payload).await.unwrap();
            // mtime granularity insurance: commits must sort k1 < k2 < k3.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // k1 was created first, but a read refreshes its LRU timestamp, so
        // the eviction must take k2 (oldest untouched) instead.
        let _ = store.get("k1", "v", &[]).await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        store.put("k4", "v", &payload).await.unwrap();

        assert!(
            store.lookup("k2", "v", &[]).await.unwrap().is_none(),
            "k2 (least recently used) must be evicted past quota"
        );
        for key in ["k1", "k3", "k4"] {
            assert!(
                store.lookup(key, "v", &[]).await.unwrap().is_some(),
                "{key} must survive quota eviction"
            );
        }
        // Total finalized size is back under quota.
        let mut total = 0u64;
        let mut read_dir = tokio::fs::read_dir(temp.path()).await.unwrap();
        while let Some(child) = read_dir.next_entry().await.unwrap() {
            let archive = child.path().join("archive.tzst");
            if let Ok(metadata) = tokio::fs::metadata(&archive).await {
                total += metadata.len();
            }
        }
        assert!(total <= 3 * MIB as u64, "total {total} over quota");
    }

    /// A single entry larger than the quota is kept by its own commit (it is
    /// never evicted by the commit that created it); the next commit evicts
    /// it first.
    #[tokio::test]
    async fn oversized_single_entry_survives_its_own_commit() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = CacheStore::new(temp.path()).await.unwrap();
        store.set_quota_bytes(1024);

        let entry = store.put("big", "v", &vec![0u8; 2048]).await.unwrap();
        assert_eq!(entry.size, 2048);
        assert!(store.lookup("big", "v", &[]).await.unwrap().is_some());

        store.put("small", "v", b"x").await.unwrap();
        assert!(
            store.lookup("big", "v", &[]).await.unwrap().is_none(),
            "the oversized entry must be evicted by the next commit"
        );
        assert!(store.lookup("small", "v", &[]).await.unwrap().is_some());
    }

    /// `commit_file` round-trips bytes and reports AlreadyExists on a
    /// duplicate key+version, matching `put` semantics.
    #[tokio::test]
    async fn commit_file_roundtrip_and_duplicate() {
        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path()).await.unwrap();

        let stage = temp.path().join("stage.bin");
        tokio::fs::write(&stage, b"staged-payload").await.unwrap();
        let entry = store.commit_file("staged", "v9", &stage).await.unwrap();
        assert_eq!(entry.key, "staged");
        assert_eq!(entry.version, "v9");
        assert_eq!(entry.size, 14);

        let stage2 = temp.path().join("stage2.bin");
        tokio::fs::write(&stage2, b"staged-payload").await.unwrap();
        let duplicate = store.commit_file("staged", "v9", &stage2).await;
        assert!(matches!(duplicate, Err(CacheError::AlreadyExists { .. })));
        // The failed commit must not consume the caller's staging file.
        assert!(stage2.exists());
    }

    /// Metadata-only reads never pull the archive into RAM: `lookup` returns
    /// the entry identity while the bytes stay on disk.
    #[tokio::test]
    async fn lookup_returns_metadata_without_reading_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let store = CacheStore::new(temp.path()).await.unwrap();
        let payload = vec![7u8; 1024];
        store.put("meta-only", "v", &payload).await.unwrap();

        let entry = store
            .lookup("meta-only", "v", &[])
            .await
            .unwrap()
            .expect("entry must resolve");
        assert_eq!(entry.size, 1024);
        assert!(entry.path.ends_with("archive.tzst"));
    }
}
