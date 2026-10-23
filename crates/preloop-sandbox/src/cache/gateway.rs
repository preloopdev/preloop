//! Deadline-bounded, failure-tolerant cache access.
//!
//! Service read paths never talk to a [`SharedCache`] directly. They go
//! through a [`CacheGateway`], which turns every backend call into a bounded
//! operation, treats timeouts and errors as a miss, rate-limits the warning,
//! and fills in the background. A cache that is slow, unreachable, flushed or
//! replaced therefore changes latency only: reads fall back to the
//! authoritative database, and mutations never consult or wait on the cache.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;
use tracing::{debug, warn};

use super::{CacheEntry, CacheError, CacheKey, SharedCache};

/// A payload plus the authoritative row version it was read at.
///
/// Database reads return this together so a cache fill can verify it observed
/// the same version as the cache key. Payload and version must come from one
/// atomic read (or a `WHERE version = $n` query); a later "read latest"
/// result must never be presented as the version a caller selected earlier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Versioned<T> {
    /// Authoritative row version the payload came from.
    pub version: u64,
    /// The payload itself.
    pub value: T,
}

impl<T> Versioned<T> {
    /// Pair a payload with the version it was read at.
    pub fn new(version: u64, value: T) -> Self {
        Self { version, value }
    }
}

impl<T> From<(u64, T)> for Versioned<T> {
    fn from((version, value): (u64, T)) -> Self {
        Self { version, value }
    }
}

/// Cumulative cache outcomes, for diagnostics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheHealth {
    /// Entries served.
    pub hits: u64,
    /// Lookups with no usable entry (backend miss, error, timeout, expiry).
    pub misses: u64,
    /// Backend errors and timeouts.
    pub errors: u64,
    /// Expired entries a backend returned anyway (discarded).
    pub expired_returned: u64,
    /// Background fills attempted.
    pub fills_attempted: u64,
    /// Fills the backend accepted.
    pub fills_stored: u64,
    /// Fills refused by admission limits.
    pub fills_refused: u64,
    /// Outage warnings actually emitted.
    pub warnings: u64,
    /// Outage warnings suppressed by the rate limit.
    pub suppressed_warnings: u64,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    errors: AtomicU64,
    expired_returned: AtomicU64,
    fills_attempted: AtomicU64,
    fills_stored: AtomicU64,
    fills_refused: AtomicU64,
    warnings: AtomicU64,
    suppressed_warnings: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> CacheHealth {
        CacheHealth {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            expired_returned: self.expired_returned.load(Ordering::Relaxed),
            fills_attempted: self.fills_attempted.load(Ordering::Relaxed),
            fills_stored: self.fills_stored.load(Ordering::Relaxed),
            fills_refused: self.fills_refused.load(Ordering::Relaxed),
            warnings: self.warnings.load(Ordering::Relaxed),
            suppressed_warnings: self.suppressed_warnings.load(Ordering::Relaxed),
        }
    }
}

struct GatewayInner {
    backend: Arc<dyn SharedCache>,
    timeout: Duration,
    warn_interval: Duration,
    /// Largest value the gateway forwards to the backend.
    max_entry_bytes: usize,
    counters: Counters,
    last_warning: Mutex<Option<Instant>>,
}

/// Bounded, best-effort access to a [`SharedCache`].
#[derive(Clone)]
pub struct CacheGateway {
    inner: Arc<GatewayInner>,
}

impl std::fmt::Debug for CacheGateway {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CacheGateway")
            .field("backend", &self.inner.backend.name())
            .field("timeout", &self.inner.timeout)
            .field("warn_interval", &self.inner.warn_interval)
            .finish()
    }
}

impl CacheGateway {
    /// Wrap a backend.
    ///
    /// `timeout` bounds every call; `warn_interval` is the minimum gap
    /// between outage warnings. Both must be non-zero (the service builder
    /// validates its configuration before constructing a gateway).
    pub fn new(backend: Arc<dyn SharedCache>, timeout: Duration, warn_interval: Duration) -> Self {
        Self::with_admission_limit(backend, timeout, warn_interval, usize::MAX)
    }

    /// Wrap a backend with an admission limit applied before the backend is
    /// called. This gives every implementation (including a network cache
    /// that cannot know local policy) the same oversized-value rule.
    pub fn with_admission_limit(
        backend: Arc<dyn SharedCache>,
        timeout: Duration,
        warn_interval: Duration,
        max_entry_bytes: usize,
    ) -> Self {
        debug_assert!(!timeout.is_zero(), "cache timeout must be bounded");
        debug_assert!(
            !warn_interval.is_zero(),
            "cache warning interval must be non-zero"
        );
        Self {
            inner: Arc::new(GatewayInner {
                backend,
                timeout,
                warn_interval,
                max_entry_bytes,
                counters: Counters::default(),
                last_warning: Mutex::new(None),
            }),
        }
    }

    /// The wrapped backend (for diagnostics and tests, never for service
    /// logic that bypasses the deadline/fallback rules).
    pub fn backend(&self) -> &Arc<dyn SharedCache> {
        &self.inner.backend
    }

    /// Per-call deadline.
    pub fn timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// Cumulative outcomes.
    pub fn health(&self) -> CacheHealth {
        self.inner.counters.snapshot()
    }

    /// Look up an exact-version key.
    ///
    /// Returns `None` for a miss, an error, a timeout or an expired entry.
    /// This method never returns an error: a read must fall back to SQL
    /// rather than fail because acceleration is unavailable.
    pub async fn get(&self, key: &CacheKey) -> Option<CacheEntry> {
        match tokio::time::timeout(self.inner.timeout, self.inner.backend.get(key)).await {
            Ok(Ok(Some(entry))) => {
                if entry.is_expired() {
                    self.inner
                        .counters
                        .expired_returned
                        .fetch_add(1, Ordering::Relaxed);
                    self.record_failure(
                        "get",
                        &CacheError::Unavailable("backend returned an expired entry".to_owned()),
                    );
                    return None;
                }
                self.inner.counters.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            Ok(Ok(None)) => {
                self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            Ok(Err(error)) => {
                self.record_failure("get", &error);
                None
            }
            Err(_) => {
                self.record_failure("get", &CacheError::Timeout(self.inner.timeout));
                None
            }
        }
    }

    /// Populate the cache from a value whose authoritative version the
    /// caller already holds.
    ///
    /// `key` must use the version that produced `value`. This is for read
    /// paths that receive payload and version atomically (for example a
    /// metadata page followed by an exact-version get): the caller can cache
    /// the payload without a second verification read. Service mutation and
    /// placement paths do not call this; they never publish values into the
    /// cache.
    pub fn remember<T: Serialize>(&self, key: &CacheKey, value: &T, ttl: Duration) {
        match serde_json::to_vec(value) {
            Ok(bytes) => self.fill(key.clone(), CacheEntry::new(bytes, ttl)),
            Err(error) => warn!(
                cache = self.inner.backend.name(),
                key = %key,
                error = %error,
                "could not serialize an authoritative read for caching"
            ),
        }
    }

    /// Populate the cache without delaying the request that missed.
    ///
    /// The fill is spawned on the current runtime and bounded by the same
    /// deadline. Failures are warnings, never caller-visible errors.
    pub fn fill(&self, key: CacheKey, entry: CacheEntry) {
        let gateway = self.clone();
        tokio::spawn(async move {
            let _ = gateway.fill_now(&key, entry).await;
        });
    }

    /// Populate the cache and wait for the backend, bounded by the deadline.
    ///
    /// Exposed for tests and for callers that deliberately want to know the
    /// fill outcome. Service read paths use [`Self::fill`].
    pub async fn fill_now(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        self.inner
            .counters
            .fills_attempted
            .fetch_add(1, Ordering::Relaxed);
        if entry.len() > self.inner.max_entry_bytes {
            let error = CacheError::EntryTooLarge {
                size: entry.len(),
                limit: self.inner.max_entry_bytes,
            };
            self.inner
                .counters
                .fills_refused
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                cache = self.inner.backend.name(),
                key = %key,
                error = %error,
                "cache declined an oversized entry; the read was still served from the database"
            );
            return Err(error);
        }
        match tokio::time::timeout(self.inner.timeout, self.inner.backend.put(key, entry)).await {
            Ok(Ok(())) => {
                self.inner
                    .counters
                    .fills_stored
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(error @ CacheError::EntryTooLarge { .. })) => {
                self.inner
                    .counters
                    .fills_refused
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    cache = self.inner.backend.name(),
                    key = %key,
                    error = %error,
                    "cache declined an oversized entry; the read was still served from the database"
                );
                Err(error)
            }
            Ok(Err(error)) => {
                self.record_failure("put", &error);
                Err(error)
            }
            Err(_) => {
                let error = CacheError::Timeout(self.inner.timeout);
                self.record_failure("put", &error);
                Err(error)
            }
        }
    }

    /// Drop one exact-version entry, if the backend can. Advisory: an
    /// invalidation failure is a warning and the version-keyed entry ages
    /// out on its own.
    pub async fn invalidate(&self, key: &CacheKey) -> bool {
        match tokio::time::timeout(self.inner.timeout, self.inner.backend.invalidate(key)).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                self.record_failure("invalidate", &error);
                false
            }
            Err(_) => {
                self.record_failure("invalidate", &CacheError::Timeout(self.inner.timeout));
                false
            }
        }
    }

    /// Drop every entry. Advisory, like [`Self::invalidate`].
    pub async fn clear(&self) -> bool {
        match tokio::time::timeout(self.inner.timeout, self.inner.backend.clear()).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                self.record_failure("clear", &error);
                false
            }
            Err(_) => {
                self.record_failure("clear", &CacheError::Timeout(self.inner.timeout));
                false
            }
        }
    }

    /// Cache-aside read of one authoritative version.
    ///
    /// On a usable hit the cached JSON is deserialized and returned. On a
    /// miss, error, timeout or unreadable entry, `load` runs the
    /// authoritative read. Its result is `Some(Versioned<T>)` when the row
    /// still exists at the requested version, and `None` when it changed or
    /// disappeared between the metadata read and the payload read.
    ///
    /// The cache is filled only when `loaded.version == key.version()`.
    /// A payload observed at a newer version is returned to the caller as
    /// "retry with fresh metadata" (`Ok(None)`), never stored under the older
    /// key and never served as if it were the older version. A late fill of
    /// an older key cannot shadow a newer key because versions are part of
    /// the key.
    ///
    /// `key` must have been built from the authoritative row version observed
    /// before this call; callers construct a new key (and retry) when this
    /// returns `Ok(None)`.
    pub async fn load<T, E, F, Fut>(
        &self,
        key: &CacheKey,
        ttl: Duration,
        load: F,
    ) -> Result<Option<T>, E>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<Versioned<T>>, E>>,
    {
        if let Some(entry) = self.get(key).await {
            match serde_json::from_slice::<T>(entry.as_bytes()) {
                Ok(value) => return Ok(Some(value)),
                Err(error) => {
                    // A payload from an older build (or a backend bug) is not
                    // served; make the bad entry unlikely to be read again.
                    debug!(
                        cache = self.inner.backend.name(),
                        key = %key,
                        error = %error,
                        "discarding unreadable cache entry"
                    );
                    let _ = tokio::time::timeout(
                        self.inner.timeout,
                        self.inner.backend.invalidate(key),
                    )
                    .await;
                }
            }
        }
        let Some(loaded) = load().await? else {
            self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        if loaded.version != key.version() {
            // The authoritative row moved while we were reading. Returning
            // None makes the caller re-read metadata; storing this payload
            // under `key` would be the exact stale-fill race this API
            // exists to prevent.
            debug!(
                cache = self.inner.backend.name(),
                key = %key,
                observed = loaded.version,
                "authoritative version changed during a cache miss; not filling"
            );
            self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.remember(key, &loaded.value, ttl);
        Ok(Some(loaded.value))
    }

    /// Cache-aside read backed by an atomic payload+version getter.
    ///
    /// `key` carries the version a cheap metadata query selected. On a hit,
    /// the cached value is returned with that version. On a miss,
    /// `load` performs one authoritative read that returns the payload
    /// together with the version it observed. The fill goes under *that*
    /// version — never under the older selected version if the row moved in
    /// between — and the observed `Versioned<T>` is returned to the caller.
    ///
    /// This is the read shape for a backend such as
    /// `SELECT …` returning its current `version` column: the cache cannot
    /// be populated with a payload the database did not report at the same
    /// instant. A `None` from `load` means the row disappeared; the caller
    /// decides whether that is a retry or a not-found.
    pub async fn load_versioned<T, E, F, Fut>(
        &self,
        key: &CacheKey,
        ttl: Duration,
        load: F,
    ) -> Result<Option<Versioned<T>>, E>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Option<Versioned<T>>, E>>,
    {
        if let Some(entry) = self.get(key).await {
            match serde_json::from_slice::<T>(entry.as_bytes()) {
                Ok(value) => {
                    return Ok(Some(Versioned::new(key.version(), value)));
                }
                Err(error) => {
                    debug!(
                        cache = self.inner.backend.name(),
                        key = %key,
                        error = %error,
                        "discarding unreadable cache entry"
                    );
                    let _ = tokio::time::timeout(
                        self.inner.timeout,
                        self.inner.backend.invalidate(key),
                    )
                    .await;
                }
            }
        }
        let Some(loaded) = load().await? else {
            self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        let fill_key = if loaded.version == key.version() {
            key.clone()
        } else {
            debug!(
                cache = self.inner.backend.name(),
                key = %key,
                observed = loaded.version,
                "payload version moved during a miss; filling the observed version"
            );
            key.with_version(loaded.version)
        };
        self.remember(&fill_key, &loaded.value, ttl);
        Ok(Some(loaded))
    }

    fn record_failure(&self, operation: &'static str, error: &CacheError) {
        self.inner.counters.errors.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let mut last = self.inner.last_warning.lock();
        let emit = last.is_none_or(|at| now.duration_since(at) >= self.inner.warn_interval);
        if emit {
            let suppressed = self
                .inner
                .counters
                .suppressed_warnings
                .swap(0, Ordering::Relaxed);
            *last = Some(now);
            self.inner.counters.warnings.fetch_add(1, Ordering::Relaxed);
            warn!(
                cache = self.inner.backend.name(),
                operation,
                error = %error,
                suppressed,
                "sandbox state cache unavailable; falling back to the database"
            );
        } else {
            self.inner
                .counters
                .suppressed_warnings
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
