//! Behavior suite for the replaceable cache contract.
//!
//! Every contract scenario runs against two independent implementations:
//! the shipped bounded in-process cache and a test-local map with its own
//! eviction and expiry code. Nothing here asserts source text or trivial
//! forwarding; the assertions are about values, scoping, expiry, bounded
//! occupancy, stale-fill races and failure fallback.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use preloop_sandbox::cache::{
    CacheEntry, CacheError, CacheGateway, CacheKey, CacheSettings, InMemoryCache, InMemoryConfig,
    Namespace, NoCache, ResourceKind, SharedCache, Versioned,
};
use tokio::time::sleep;

// ─── An independent implementation used only by these tests ────────────

#[derive(Debug)]
struct MapEntry {
    value: Vec<u8>,
    expires_at: SystemTime,
    used: u64,
}

#[derive(Debug)]
struct MapCache {
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
    inner: Mutex<MapInner>,
}

#[derive(Debug, Default)]
struct MapInner {
    rows: BTreeMap<CacheKey, MapEntry>,
    bytes: usize,
    clock: u64,
}

impl MapCache {
    fn new(max_entries: usize, max_bytes: usize, max_entry_bytes: usize) -> Self {
        Self {
            max_entries,
            max_bytes,
            max_entry_bytes,
            inner: Mutex::new(MapInner::default()),
        }
    }

    fn tick(inner: &mut MapInner) -> u64 {
        inner.clock += 1;
        inner.clock
    }
}

#[async_trait]
impl SharedCache for MapCache {
    async fn get(&self, key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        let mut inner = self.inner.lock().expect("map cache lock");
        let now = SystemTime::now();
        if inner
            .rows
            .get(key)
            .is_some_and(|entry| now >= entry.expires_at)
        {
            if let Some(removed) = inner.rows.remove(key) {
                inner.bytes = inner.bytes.saturating_sub(removed.value.len());
            }
            return Ok(None);
        }
        let Some(entry) = inner.rows.get(key) else {
            return Ok(None);
        };
        let expiry = entry.expires_at;
        let value = entry.value.clone();
        let used = Self::tick(&mut inner);
        if let Some(entry) = inner.rows.get_mut(key) {
            entry.used = used;
        }
        Ok(Some(CacheEntry::with_expiry(Bytes::from(value), expiry)))
    }

    async fn put(&self, key: &CacheKey, entry: CacheEntry) -> Result<(), CacheError> {
        let size = entry.len();
        if size > self.max_entry_bytes {
            return Err(CacheError::EntryTooLarge {
                size,
                limit: self.max_entry_bytes,
            });
        }
        if size > self.max_bytes || entry.is_expired() {
            return Ok(());
        }
        let mut inner = self.inner.lock().expect("map cache lock");
        if let Some(removed) = inner.rows.remove(key) {
            inner.bytes = inner.bytes.saturating_sub(removed.value.len());
        }
        while inner.rows.len() + 1 > self.max_entries || inner.bytes + size > self.max_bytes {
            let Some(oldest) = inner
                .rows
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(removed) = inner.rows.remove(&oldest) {
                inner.bytes = inner.bytes.saturating_sub(removed.value.len());
            }
        }
        let used = Self::tick(&mut inner);
        inner.bytes += size;
        inner.rows.insert(
            key.clone(),
            MapEntry {
                value: entry.as_bytes().to_vec(),
                expires_at: entry.expires_at(),
                used,
            },
        );
        Ok(())
    }

    async fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        let mut inner = self.inner.lock().expect("map cache lock");
        if let Some(removed) = inner.rows.remove(key) {
            inner.bytes = inner.bytes.saturating_sub(removed.value.len());
        }
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let mut inner = self.inner.lock().expect("map cache lock");
        inner.rows.clear();
        inner.bytes = 0;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "test-map"
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────

fn key(namespace: &str, kind: ResourceKind, id: &str, version: u64) -> CacheKey {
    CacheKey::new(Namespace::new(namespace).unwrap(), kind, id, version).unwrap()
}

fn ttl(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

fn memory_cache(
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
) -> Arc<dyn SharedCache> {
    Arc::new(
        InMemoryCache::new(InMemoryConfig {
            max_entries,
            max_bytes,
            max_entry_bytes,
        })
        .unwrap(),
    )
}

fn map_cache(max_entries: usize, max_bytes: usize, max_entry_bytes: usize) -> Arc<dyn SharedCache> {
    Arc::new(MapCache::new(max_entries, max_bytes, max_entry_bytes))
}

async fn wait_until_filled(cache: &dyn SharedCache, key: &CacheKey) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if cache.get(key).await.unwrap().is_some() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background fill did not land");
}

// ─── Scenarios, run against both implementations ───────────────────────

async fn values_survive_hit_miss_and_invalidate(cache: Arc<dyn SharedCache>) {
    let key = key("default", ResourceKind::Template, "base", 7);
    assert!(cache.get(&key).await.unwrap().is_none());
    cache
        .put(
            &key,
            CacheEntry::new(Bytes::from_static(b"payload"), ttl(60)),
        )
        .await
        .unwrap();
    let hit = cache.get(&key).await.unwrap().expect("hit");
    assert_eq!(hit.as_bytes(), b"payload");
    cache.invalidate(&key).await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_none());
    cache.invalidate(&key).await.unwrap();

    cache
        .put(&key, CacheEntry::new(Bytes::from_static(b"again"), ttl(60)))
        .await
        .unwrap();
    cache.clear().await.unwrap();
    assert!(cache.get(&key).await.unwrap().is_none());
}

async fn ttl_expires_entries(cache: Arc<dyn SharedCache>) {
    let short = key("default", ResourceKind::Pool, "short", 1);
    cache
        .put(
            &short,
            CacheEntry::new(Bytes::from_static(b"gone"), Duration::from_millis(10)),
        )
        .await
        .unwrap();
    assert_eq!(
        cache.get(&short).await.unwrap().expect("fresh").as_bytes(),
        b"gone"
    );
    sleep(Duration::from_millis(60)).await;
    assert!(
        cache.get(&short).await.unwrap().is_none(),
        "entry outlived its TTL"
    );

    let past = key("default", ResourceKind::Pool, "past", 1);
    cache
        .put(
            &past,
            CacheEntry::with_expiry(
                Bytes::from_static(b"expired"),
                SystemTime::now() - Duration::from_secs(1),
            ),
        )
        .await
        .unwrap();
    assert!(cache.get(&past).await.unwrap().is_none());
}

async fn namespace_and_version_scope_entries(cache: Arc<dyn SharedCache>) {
    let one = key("one", ResourceKind::Template, "shared-name", 1);
    let two = key("two", ResourceKind::Template, "shared-name", 1);
    let newer = key("one", ResourceKind::Template, "shared-name", 2);
    cache
        .put(
            &one,
            CacheEntry::new(Bytes::from_static(b"one-v1"), ttl(60)),
        )
        .await
        .unwrap();
    cache
        .put(
            &two,
            CacheEntry::new(Bytes::from_static(b"two-v1"), ttl(60)),
        )
        .await
        .unwrap();
    assert_eq!(
        cache.get(&one).await.unwrap().unwrap().as_bytes(),
        b"one-v1"
    );
    assert_eq!(
        cache.get(&two).await.unwrap().unwrap().as_bytes(),
        b"two-v1"
    );
    assert!(
        cache.get(&newer).await.unwrap().is_none(),
        "a newer version must not alias the older key"
    );
}

async fn entry_capacity_evicts_least_recently_used(cache: Arc<dyn SharedCache>) {
    let first = key("default", ResourceKind::Template, "first", 1);
    let second = key("default", ResourceKind::Template, "second", 1);
    let third = key("default", ResourceKind::Template, "third", 1);
    for (key, value) in [(&first, b"1111".as_slice()), (&second, b"2222".as_slice())] {
        cache
            .put(key, CacheEntry::new(Bytes::copy_from_slice(value), ttl(60)))
            .await
            .unwrap();
    }
    // Touch the first so the second is the eviction victim.
    assert!(cache.get(&first).await.unwrap().is_some());
    cache
        .put(
            &third,
            CacheEntry::new(Bytes::from_static(b"3333"), ttl(60)),
        )
        .await
        .unwrap();
    assert!(cache.get(&first).await.unwrap().is_some());
    assert!(cache.get(&second).await.unwrap().is_none());
    assert!(cache.get(&third).await.unwrap().is_some());
}

async fn byte_capacity_stays_bounded(cache: Arc<dyn SharedCache>) {
    // 16-byte cap, 8-byte entries: a touch-protected entry and two others.
    let first = key("default", ResourceKind::Pool, "a", 1);
    let second = key("default", ResourceKind::Pool, "b", 1);
    let third = key("default", ResourceKind::Pool, "c", 1);
    cache
        .put(
            &first,
            CacheEntry::new(Bytes::from_static(&[1; 8]), ttl(60)),
        )
        .await
        .unwrap();
    cache
        .put(
            &second,
            CacheEntry::new(Bytes::from_static(&[2; 8]), ttl(60)),
        )
        .await
        .unwrap();
    assert!(cache.get(&first).await.unwrap().is_some());
    cache
        .put(
            &third,
            CacheEntry::new(Bytes::from_static(&[3; 8]), ttl(60)),
        )
        .await
        .unwrap();
    assert!(cache.get(&first).await.unwrap().is_some());
    assert!(cache.get(&second).await.unwrap().is_none());
    assert!(cache.get(&third).await.unwrap().is_some());
}

async fn oversized_values_are_refused(cache: Arc<dyn SharedCache>) {
    let key = key("default", ResourceKind::Template, "big", 1);
    let error = cache
        .put(&key, CacheEntry::new(Bytes::from_static(&[9; 9]), ttl(60)))
        .await
        .unwrap_err();
    assert_eq!(error, CacheError::EntryTooLarge { size: 9, limit: 8 });
    assert!(cache.get(&key).await.unwrap().is_none());
    cache
        .put(&key, CacheEntry::new(Bytes::from_static(&[8; 8]), ttl(60)))
        .await
        .unwrap();
    assert!(cache.get(&key).await.unwrap().is_some());
}

async fn late_old_version_fill_cannot_shadow_newer(cache: Arc<dyn SharedCache>) {
    let old = key("default", ResourceKind::Template, "recreated", 1);
    let new = key("default", ResourceKind::Template, "recreated", 2);
    cache
        .put(&old, CacheEntry::new(Bytes::from_static(b"old"), ttl(60)))
        .await
        .unwrap();
    // Delete + recreate must hand out a non-reused version. A read that
    // selected version 2 never asks for the version-1 key.
    assert!(cache.get(&new).await.unwrap().is_none());
    cache
        .put(&new, CacheEntry::new(Bytes::from_static(b"new"), ttl(60)))
        .await
        .unwrap();
    // A delayed fill of the old key landing now changes nothing for v2.
    cache
        .put(&old, CacheEntry::new(Bytes::from_static(b"old"), ttl(60)))
        .await
        .unwrap();
    assert_eq!(cache.get(&new).await.unwrap().unwrap().as_bytes(), b"new");
}

async fn concurrent_readers_and_writers_never_cross_versions(cache: Arc<dyn SharedCache>) {
    let mut tasks = Vec::new();
    for worker in 0..8u64 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            for step in 0..100u64 {
                let version = (worker + step) % 8;
                let key = key("default", ResourceKind::SandboxSpec, "spec", version);
                let payload = format!("version-{version}");
                if step % 3 == 0 {
                    cache
                        .put(&key, CacheEntry::new(Bytes::from(payload.clone()), ttl(60)))
                        .await
                        .unwrap();
                }
                if let Some(entry) = cache.get(&key).await.unwrap() {
                    assert_eq!(
                        entry.as_bytes(),
                        payload.as_bytes(),
                        "key version {version} returned another version's payload"
                    );
                }
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

macro_rules! run_on_both {
    ($test_name:ident, $scenario:ident) => {
        mod $test_name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn memory() {
                $scenario(memory_cache(16, 4096, 1024)).await;
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn independent_adapter() {
                $scenario(map_cache(16, 4096, 1024)).await;
            }
        }
    };
}

run_on_both!(hit_miss_invalidate, values_survive_hit_miss_and_invalidate);
run_on_both!(ttl, ttl_expires_entries);
run_on_both!(scoping, namespace_and_version_scope_entries);
run_on_both!(
    concurrency,
    concurrent_readers_and_writers_never_cross_versions
);

mod capacities {
    use super::*;

    #[tokio::test]
    async fn memory_entry_capacity_evicts_lru() {
        entry_capacity_evicts_least_recently_used(memory_cache(2, 4096, 1024)).await;
    }

    #[tokio::test]
    async fn independent_entry_capacity_evicts_lru() {
        entry_capacity_evicts_least_recently_used(map_cache(2, 4096, 1024)).await;
    }

    #[tokio::test]
    async fn memory_byte_capacity_evicts_lru() {
        byte_capacity_stays_bounded(memory_cache(10, 16, 8)).await;
    }

    #[tokio::test]
    async fn independent_byte_capacity_evicts_lru() {
        byte_capacity_stays_bounded(map_cache(10, 16, 8)).await;
    }

    #[tokio::test]
    async fn memory_refuses_oversized_values() {
        oversized_values_are_refused(memory_cache(10, 4096, 8)).await;
    }

    #[tokio::test]
    async fn independent_refuses_oversized_values() {
        oversized_values_are_refused(map_cache(10, 4096, 8)).await;
    }

    #[tokio::test]
    async fn memory_old_version_fill_cannot_shadow() {
        late_old_version_fill_cannot_shadow_newer(memory_cache(16, 4096, 1024)).await;
    }

    #[tokio::test]
    async fn independent_old_version_fill_cannot_shadow() {
        late_old_version_fill_cannot_shadow_newer(map_cache(16, 4096, 1024)).await;
    }
}

// ─── Gateway: deadline, fallback, stale-fill and admission behavior ────

#[derive(Debug, Default)]
struct SlowCache {
    gets: AtomicUsize,
}

#[async_trait]
impl SharedCache for SlowCache {
    async fn get(&self, _key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        sleep(Duration::from_secs(30)).await;
        Ok(Some(CacheEntry::new(
            Bytes::from_static(b"too late"),
            ttl(60),
        )))
    }

    async fn put(&self, _key: &CacheKey, _entry: CacheEntry) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate(&self, _key: &CacheKey) -> Result<(), CacheError> {
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "slow-test"
    }
}

#[derive(Debug)]
struct FailingCache;

#[async_trait]
impl SharedCache for FailingCache {
    async fn get(&self, _key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    async fn put(&self, _key: &CacheKey, _entry: CacheEntry) -> Result<(), CacheError> {
        // The read path failure is what this test exercises; a background
        // fill on the same outage must not race the health snapshot.
        Ok(())
    }

    async fn invalidate(&self, _key: &CacheKey) -> Result<(), CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Err(CacheError::Unavailable("connection refused".into()))
    }

    fn name(&self) -> &'static str {
        "failing-test"
    }
}

/// A backend that violates the contract by returning an expired entry.
#[derive(Debug)]
struct ExpiredCache;

#[async_trait]
impl SharedCache for ExpiredCache {
    async fn get(&self, _key: &CacheKey) -> Result<Option<CacheEntry>, CacheError> {
        Ok(Some(CacheEntry::with_expiry(
            Bytes::from_static(b"stale"),
            SystemTime::now() - Duration::from_secs(1),
        )))
    }

    async fn put(&self, _key: &CacheKey, _entry: CacheEntry) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate(&self, _key: &CacheKey) -> Result<(), CacheError> {
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "expired-test"
    }
}

fn gateway(backend: Arc<dyn SharedCache>) -> CacheGateway {
    CacheGateway::new(
        backend,
        Duration::from_millis(20),
        Duration::from_secs(3600),
    )
}

#[tokio::test]
async fn slow_cache_times_out_then_loads_and_rate_limits_warnings() {
    let backend = Arc::new(SlowCache::default());
    let gateway = gateway(backend.clone());
    let key = key("default", ResourceKind::Template, "slow", 1);
    let started = Instant::now();
    let loaded = gateway
        .load(&key, ttl(60), || async {
            Ok::<_, String>(Some(Versioned::new(1, b"sql".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(loaded.as_deref(), Some(b"sql".as_slice()));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the slow cache should be abandoned at its deadline"
    );
    assert_eq!(backend.gets.load(Ordering::Relaxed), 1);
    let health = gateway.health();
    assert_eq!(health.errors, 1);
    assert_eq!(health.warnings, 1);
    assert_eq!(health.suppressed_warnings, 0);

    let second = gateway
        .load(&key, ttl(60), || async {
            Ok::<_, String>(Some(Versioned::new(1, b"sql-two".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(second.as_deref(), Some(b"sql-two".as_slice()));
    let health = gateway.health();
    assert_eq!(health.errors, 2);
    assert_eq!(health.warnings, 1, "outage warnings must be rate-limited");
    assert_eq!(health.suppressed_warnings, 1);
}

#[tokio::test]
async fn failing_cache_falls_back_to_the_loader() {
    let gateway = gateway(Arc::new(FailingCache));
    let key = key("default", ResourceKind::Pool, "failing", 3);
    let loaded = gateway
        .load(&key, ttl(60), || async {
            Ok::<_, String>(Some(Versioned::new(3, b"authoritative".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(loaded.as_deref(), Some(b"authoritative".as_slice()));
    assert_eq!(gateway.health().errors, 1);
}

#[tokio::test]
async fn atomic_read_fills_the_observed_version() {
    let backend = memory_cache(16, 4096, 1024);
    let gateway = gateway(backend.clone());
    let selected = key("default", ResourceKind::Template, "moved", 1);
    let first = gateway
        .load_versioned(&selected, ttl(60), || async {
            Ok::<_, String>(Some(Versioned::new(2, b"new".to_vec())))
        })
        .await
        .unwrap()
        .expect("row");
    assert_eq!(first.version, 2);
    assert_eq!(first.value, b"new");
    let observed = selected.with_version(2);
    wait_until_filled(&*backend, &observed).await;
    assert!(
        backend.get(&selected).await.unwrap().is_none(),
        "the payload must not be cached under the version metadata selected"
    );
    let cached = backend.get(&observed).await.unwrap().unwrap();
    let decoded: Vec<u8> = serde_json::from_slice(cached.as_bytes()).unwrap();
    assert_eq!(decoded, b"new");
}

#[tokio::test]
async fn late_old_version_fill_does_not_change_what_a_newer_read_serves() {
    let backend = memory_cache(16, 4096, 1024);
    let gateway = gateway(backend.clone());
    let old = key("default", ResourceKind::Pool, "recreated", 1);
    let new = key("default", ResourceKind::Pool, "recreated", 2);

    // A read selects v1, misses, and stalls before its database read.
    let release = Arc::new(tokio::sync::Notify::new());
    let slow = {
        let gateway = gateway.clone();
        let old = old.clone();
        let release = release.clone();
        tokio::spawn(async move {
            gateway
                .load_versioned(&old, ttl(60), || async {
                    release.notified().await;
                    Ok::<_, String>(Some(Versioned::new(1, b"old".to_vec())))
                })
                .await
                .unwrap()
        })
    };

    // The name is deleted and recreated while that read is stalled; the
    // authoritative database assigns a new, non-reused version.
    let fresh = gateway
        .load_versioned(&new, ttl(60), || async {
            Ok::<_, String>(Some(Versioned::new(2, b"new".to_vec())))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fresh.value, b"new");
    wait_until_filled(&*backend, &new).await;

    // Now the old fill lands. It belongs to a different key and cannot
    // shadow the new incarnation.
    release.notify_one();
    let old_result = slow.await.unwrap().unwrap();
    assert_eq!(old_result.value, b"old");
    wait_until_filled(&*backend, &old).await;
    let served = gateway
        .load_versioned(&new, ttl(60), || async {
            Err::<Option<Versioned<Vec<u8>>>, String>("v2 should hit".into())
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(served.value, b"new");
}

#[tokio::test]
async fn expired_entries_returned_by_a_backend_are_never_served() {
    let gateway = gateway(Arc::new(ExpiredCache));
    let key = key("default", ResourceKind::Template, "expired", 1);
    assert!(gateway.get(&key).await.is_none());
    let health = gateway.health();
    assert_eq!(health.expired_returned, 1);
    assert_eq!(health.errors, 1);
}

#[tokio::test]
async fn a_payload_from_a_newer_version_is_not_stored_under_the_older_key() {
    let backend = memory_cache(16, 4096, 1024);
    let gateway = gateway(backend.clone());
    let key_v1 = key("default", ResourceKind::Template, "moved", 1);
    let loaded = gateway
        .load(&key_v1, ttl(60), || async {
            // The payload query observed version 2 even though metadata said
            // version 1. Returning the value is fine; caching it under v1 is
            // not.
            Ok::<_, String>(Some(Versioned::new(2, b"v2".to_vec())))
        })
        .await
        .unwrap();
    assert!(
        loaded.is_none(),
        "a payload from another version must not be returned as the selected version"
    );
    assert!(
        backend.get(&key_v1).await.unwrap().is_none(),
        "a newer payload must not be cached as an older version"
    );
    assert_eq!(gateway.health().fills_attempted, 0);
}

#[tokio::test]
async fn removed_row_returns_none_and_fills_nothing() {
    let backend = memory_cache(16, 4096, 1024);
    let gateway = gateway(backend.clone());
    let key = key("default", ResourceKind::Pool, "deleted", 9);
    let loaded = gateway
        .load::<Vec<u8>, _, _, _>(&key, ttl(60), || async { Ok::<_, String>(None) })
        .await
        .unwrap();
    assert!(loaded.is_none());
    assert!(backend.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn hit_skips_the_loader_and_miss_fills_in_the_background() {
    let backend = memory_cache(16, 4096, 1024);
    let gateway = gateway(backend.clone());
    let key = key("default", ResourceKind::SandboxSpec, "spec", 4);
    let loads = Arc::new(AtomicUsize::new(0));
    let first = {
        let loads = loads.clone();
        gateway
            .load(&key, ttl(60), move || async move {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(Some(Versioned::new(4, b"spec-v4".to_vec())))
            })
            .await
            .unwrap()
    };
    assert_eq!(first.as_deref(), Some(b"spec-v4".as_slice()));
    wait_until_filled(&*backend, &key).await;

    let loads = Arc::new(AtomicUsize::new(0));
    let second = {
        let loads = loads.clone();
        gateway
            .load(&key, ttl(60), move || async move {
                loads.fetch_add(1, Ordering::SeqCst);
                Err::<Option<Versioned<Vec<u8>>>, String>("cache hit should not load".into())
            })
            .await
            .unwrap()
    };
    assert_eq!(second.as_deref(), Some(b"spec-v4".as_slice()));
    assert_eq!(loads.load(Ordering::SeqCst), 0);
    assert_eq!(gateway.health().hits, 1);
}

#[tokio::test]
async fn oversized_read_payload_is_served_but_not_cached() {
    let backend = memory_cache(16, 4096, 4096);
    let gateway = CacheGateway::with_admission_limit(
        backend.clone(),
        Duration::from_millis(20),
        Duration::from_secs(3600),
        8,
    );
    let key = key("default", ResourceKind::Template, "large", 1);
    let loads = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let loads = loads.clone();
        let loaded = gateway
            .load(&key, ttl(60), move || async move {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(Some(Versioned::new(1, vec![7u8; 64])))
            })
            .await
            .unwrap();
        assert_eq!(loaded.unwrap().len(), 64);
        sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert!(
        backend.get(&key).await.unwrap().is_none(),
        "oversized payload must not displace smaller entries"
    );
    assert!(gateway.health().fills_refused >= 1);
}

#[tokio::test]
async fn disabled_cache_loads_every_time() {
    let settings = CacheSettings {
        mode: preloop_sandbox::cache::CacheMode::None,
        ..CacheSettings::default()
    };
    let gateway = settings.gateway().unwrap();
    assert_eq!(gateway.backend().name(), "none");
    let key = key("default", ResourceKind::Template, "uncached", 1);
    let loads = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let loads = loads.clone();
        let loaded = gateway
            .load(&key, ttl(60), move || async move {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(Some(Versioned::new(1, b"value".to_vec())))
            })
            .await
            .unwrap();
        assert!(loaded.is_some());
    }
    assert_eq!(loads.load(Ordering::SeqCst), 2);
    assert_eq!(gateway.health().hits, 0);
}

#[tokio::test]
async fn no_cache_type_is_explicitly_empty() {
    let cache = NoCache;
    let key = key("default", ResourceKind::Template, "none", 1);
    cache
        .put(&key, CacheEntry::new(Bytes::from_static(b"x"), ttl(60)))
        .await
        .unwrap();
    assert!(cache.get(&key).await.unwrap().is_none());
}
