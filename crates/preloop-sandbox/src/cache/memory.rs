//! Bounded in-process cache: exact-version keys, payload-byte capacity and
//! lazy TTL expiry.
//!
//! The implementation is deliberately small and synchronous. Its lock is
//! held only for map bookkeeping — never across an await — so a cache hit
//! costs no task scheduling and a miss cannot serialize database I/O behind
//! cache maintenance.
//!
//! Capacity is charged in serialized payload bytes. Keys and per-entry
//! bookkeeping are bounded separately by the entry limit. Eviction is exact
//! LRU (a hit moves the key to the back), so the ordering is deterministic
//! for a given access sequence rather than approximate.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;

use super::config::CacheConfigError;
use super::{CacheEntry, CacheError, CacheKey, SharedCache};

/// Limits for [`InMemoryCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InMemoryConfig {
    /// Total serialized payload bytes retained.
    pub max_bytes: usize,
    /// Maximum number of entries retained.
    pub max_entries: usize,
    /// Largest single entry admitted.
    pub max_entry_bytes: usize,
}

impl Default for InMemoryConfig {
    fn default() -> Self {
        Self {
            max_bytes: super::config::DEFAULT_MAX_BYTES,
            max_entries: super::config::DEFAULT_MAX_ENTRIES,
            max_entry_bytes: super::config::DEFAULT_MAX_ENTRY_BYTES,
        }
    }
}

impl InMemoryConfig {
    fn validate(&self) -> Result<(), CacheConfigError> {
        if self.max_bytes == 0 || self.max_entries == 0 || self.max_entry_bytes == 0 {
            return Err(CacheConfigError::OutOfRange {
                name: "in-memory cache capacity",
                minimum: 1,
                value: 0,
            });
        }
        if self.max_entry_bytes > self.max_bytes {
            return Err(CacheConfigError::EntryLimitExceedsCapacity {
                max_entry_bytes: self.max_entry_bytes,
                max_bytes: self.max_bytes,
            });
        }
        Ok(())
    }
}

/// A point-in-time view of the cache's occupancy and outcomes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Entries currently retained (including not-yet-swept expired entries).
    pub entries: usize,
    /// Serialized payload bytes currently retained.
    pub bytes: usize,
    /// Hits served.
    pub hits: u64,
    /// Lookups that found nothing (or an expired entry).
    pub misses: u64,
    /// Entries removed because their TTL elapsed.
    pub expired: u64,
    /// Entries removed to respect capacity.
    pub evictions: u64,
    /// Puts rejected by the admission limit or already-expired deadline.
    pub rejected: u64,
}

struct Stored {
    entry: CacheEntry,
    /// Monotonic deadline local to this process. `SystemTime` in the entry
    /// remains the portable deadline an external backend would enforce.
    expires: Instant,
    tick: u64,
}

struct Inner {
    entries: HashMap<CacheKey, Stored>,
    /// Least-recently-used first.
    lru: BTreeMap<u64, CacheKey>,
    bytes: usize,
    tick: u64,
    stats: CacheStats,
}

/// A bounded, in-process [`SharedCache`].
pub struct InMemoryCache {
    config: InMemoryConfig,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for InMemoryCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        formatter
            .debug_struct("InMemoryCache")
            .field("max_bytes", &self.config.max_bytes)
            .field("max_entries", &self.config.max_entries)
            .field("max_entry_bytes", &self.config.max_entry_bytes)
            .field("entries", &stats.entries)
            .field("bytes", &stats.bytes)
            .finish()
    }
}

impl Default for InMemoryCache {
    fn default() -> Self {
        Self::new(InMemoryConfig::default()).expect("default in-memory cache limits are valid")
    }
}

impl InMemoryCache {
    /// Build a cache with explicit limits.
    pub fn new(config: InMemoryConfig) -> Result<Self, CacheConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                lru: BTreeMap::new(),
                bytes: 0,
                tick: 0,
                stats: CacheStats::default(),
            }),
        })
    }

    /// Current occupancy and counters.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().expect("cache lock");
        CacheStats {
            entries: inner.entries.len(),
            bytes: inner.bytes,
            ..inner.stats
        }
    }

    /// Limits this cache enforces.
    pub fn limits(&self) -> InMemoryConfig {
        self.config
    }

    fn next_tick(inner: &mut Inner) -> u64 {
        if inner.tick == u64::MAX {
            // Wrap after 2^64 accesses is theoretical; renumber instead of
            // inserting tick 0 next to the existing maximum.
            let keys: Vec<CacheKey> = inner.entries.keys().cloned().collect();
            inner.lru.clear();
            for (index, key) in keys.into_iter().enumerate() {
                let tick = index as u64 + 1;
                if let Some(stored) = inner.entries.get_mut(&key) {
                    stored.tick = tick;
                }
                inner.lru.insert(tick, key);
            }
            inner.tick = inner.entries.len() as u64 + 1;
        } else {
            inner.tick += 1;
        }
        inner.tick
    }

    fn remove_locked(inner: &mut Inner, key: &CacheKey) -> Option<Stored> {
        let stored = inner.entries.remove(key)?;
        inner.lru.remove(&stored.tick);
        inner.bytes = inner.bytes.saturating_sub(stored.entry.len());
        Some(stored)
    }

    /// Remove every expired entry. Returns how many were removed.
    ///
    /// Not required for correctness: a lookup never returns an expired entry
    /// and capacity eviction reclaims expired space. This exists for callers
    /// that want reclaim without a put and for a maintenance tick.
    pub fn prune_expired(&self) -> usize {
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("cache lock");
        let expired: Vec<CacheKey> = inner
            .entries
            .iter()
            .filter(|(_, stored)| now >= stored.expires)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &expired {
            Self::remove_locked(&mut inner, key);
        }
        inner.stats.expired = inner.stats.expired.saturating_add(expired.len() as u64);
        expired.len()
    }
}

#[async_trait]
impl SharedCache for InMemoryCache {
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        let now = Instant::now();
        let mut inner = self.inner.lock().expect("cache lock");
        let expired = inner
            .entries
            .get(key)
            .is_some_and(|stored| now >= stored.expires);
        if expired {
            Self::remove_locked(&mut inner, key);
            inner.stats.expired = inner.stats.expired.saturating_add(1);
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        }
        let Some(stored) = inner.entries.get(key) else {
            inner.stats.misses = inner.stats.misses.saturating_add(1);
            return Ok(None);
        };
        let entry = stored.entry.clone();
        let old_tick = stored.tick;
        let tick = Self::next_tick(&mut inner);
        inner.lru.remove(&old_tick);
        inner.lru.insert(tick, key.clone());
        if let Some(stored) = inner.entries.get_mut(key) {
            stored.tick = tick;
        }
        inner.stats.hits = inner.stats.hits.saturating_add(1);
        Ok(Some(entry))
    }

    async fn put(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        let size = entry.len();
        let limit = self.config.max_entry_bytes.min(self.config.max_bytes);
        let remaining = entry
            .expires_at()
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            self.inner.lock().expect("cache lock").stats.rejected += 1;
            return Ok(());
        }
        if size > limit {
            self.inner.lock().expect("cache lock").stats.rejected += 1;
            return Err(CacheError::EntryTooLarge { size, limit });
        }

        let mut inner = self.inner.lock().expect("cache lock");
        Self::remove_locked(&mut inner, key);
        while inner.entries.len() + 1 > self.config.max_entries
            || inner.bytes + size > self.config.max_bytes
        {
            let Some((_, oldest)) = inner.lru.pop_first() else {
                break;
            };
            if let Some(stored) = inner.entries.remove(&oldest) {
                inner.bytes = inner.bytes.saturating_sub(stored.entry.len());
                inner.stats.evictions = inner.stats.evictions.saturating_add(1);
            }
        }
        debug_assert_eq!(inner.lru.len(), inner.entries.len());
        let tick = Self::next_tick(&mut inner);
        let expires = Instant::now() + remaining;
        inner.bytes += size;
        inner.lru.insert(tick, key.clone());
        inner.entries.insert(
            key.clone(),
            Stored {
                entry,
                expires,
                tick,
            },
        );
        Ok(())
    }

    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        Self::remove_locked(&mut self.inner.lock().expect("cache lock"), key);
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let mut inner = self.inner.lock().expect("cache lock");
        inner.entries.clear();
        inner.lru.clear();
        inner.bytes = 0;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}
