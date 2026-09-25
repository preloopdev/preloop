//! File-backed live log segments for real-time console tail persistence.
//!
//! Replaces database-backed `log_chunks` and `log_files` tables.
//! Masked log lines are buffered in memory and flushed periodically or when the
//! buffer exceeds a size threshold to sequential, immutable segment files under
//! `<state_dir>/live-logs/<plan_id>/<log_id>/seg-<seq:06>.log`.
//!
//! Readers (e.g. `get_run_logs`) read contiguous segments from disk and any
//! pending in-memory buffer. Completed jobs upload authoritative step logs
//! separately to BlobStore.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-log-key retained byte cap on disk (16 MiB). Oldest segments are pruned
/// when disk usage exceeds this threshold.
pub(crate) const MAX_LIVE_LOG_BYTES_PER_KEY: usize = 16 * 1024 * 1024;

/// Flush pending buffer when it reaches 1 MiB.
pub(crate) const FLUSH_BYTES_THRESHOLD: usize = 1024 * 1024;

/// Max time pending bytes can sit in memory before background flush (5 seconds).
pub(crate) const FLUSH_STALE_INTERVAL: Duration = Duration::from_secs(5);

/// Sanitize plan_id / log_id for safe filesystem paths (no directory traversal).
fn sanitize_id(id: &str) -> String {
    let sanitized: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "default".to_string()
    } else {
        sanitized
    }
}

pub(crate) struct KeyState {
    plan_id: String,
    log_id: String,
    pending: Vec<u8>,
    last_write: Instant,
    next_seq: u64,
}

#[derive(Clone)]
pub(crate) struct LiveLogSegments {
    root: PathBuf,
    entries: Arc<parking_lot::Mutex<BTreeMap<String, KeyState>>>,
}

impl LiveLogSegments {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            entries: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
        }
    }

    fn key(plan_id: &str, log_id: &str) -> String {
        format!("{plan_id}/{log_id}")
    }

    fn log_dir(&self, plan_id: &str, log_id: &str) -> PathBuf {
        self.root
            .join(sanitize_id(plan_id))
            .join(sanitize_id(log_id))
    }

    fn plan_dir(&self, plan_id: &str) -> PathBuf {
        self.root.join(sanitize_id(plan_id))
    }

    /// Append masked log bytes to the pending buffer. Flushes to disk if the
    /// buffer reaches [`FLUSH_BYTES_THRESHOLD`].
    pub(crate) async fn append(&self, plan_id: &str, log_id: &str, chunk: &[u8]) {
        let key = Self::key(plan_id, log_id);
        let flush_payload = {
            let mut guard = self.entries.lock();
            let entry = guard.entry(key.clone()).or_insert_with(|| KeyState {
                plan_id: plan_id.to_string(),
                log_id: log_id.to_string(),
                pending: Vec::new(),
                last_write: Instant::now(),
                next_seq: 0,
            });
            entry.pending.extend_from_slice(chunk);
            entry.last_write = Instant::now();
            if entry.pending.len() >= FLUSH_BYTES_THRESHOLD {
                let bytes = std::mem::take(&mut entry.pending);
                let seq = entry.next_seq;
                entry.next_seq += 1;
                Some((entry.plan_id.clone(), entry.log_id.clone(), seq, bytes))
            } else {
                None
            }
        };

        if let Some((plan, log, seq, bytes)) = flush_payload {
            let dir = self.log_dir(&plan, &log);
            let _ = tokio::task::spawn_blocking(move || {
                write_and_prune_segment(&dir, seq, &bytes);
            })
            .await;
        }
    }

    /// Flush all pending logs whose last write is older than [`FLUSH_STALE_INTERVAL`].
    pub(crate) async fn flush_stale(&self) {
        let now = Instant::now();
        let flushes: Vec<(String, String, u64, Vec<u8>)> = {
            let mut guard = self.entries.lock();
            let mut to_flush = Vec::new();
            for entry in guard.values_mut() {
                if !entry.pending.is_empty()
                    && now.duration_since(entry.last_write) >= FLUSH_STALE_INTERVAL
                {
                    let bytes = std::mem::take(&mut entry.pending);
                    let seq = entry.next_seq;
                    entry.next_seq += 1;
                    to_flush.push((entry.plan_id.clone(), entry.log_id.clone(), seq, bytes));
                }
            }
            to_flush
        };

        for (plan, log, seq, bytes) in flushes {
            let dir = self.log_dir(&plan, &log);
            let _ = tokio::task::spawn_blocking(move || {
                write_and_prune_segment(&dir, seq, &bytes);
            })
            .await;
        }
    }

    /// Flush all pending bytes across all keys immediately (used during shutdown).
    pub(crate) async fn flush_all(&self) {
        let flushes: Vec<(String, String, u64, Vec<u8>)> = {
            let mut guard = self.entries.lock();
            let mut to_flush = Vec::new();
            for entry in guard.values_mut() {
                if !entry.pending.is_empty() {
                    let bytes = std::mem::take(&mut entry.pending);
                    let seq = entry.next_seq;
                    entry.next_seq += 1;
                    to_flush.push((entry.plan_id.clone(), entry.log_id.clone(), seq, bytes));
                }
            }
            to_flush
        };

        for (plan, log, seq, bytes) in flushes {
            let dir = self.log_dir(&plan, &log);
            let _ = tokio::task::spawn_blocking(move || {
                write_and_prune_segment(&dir, seq, &bytes);
            })
            .await;
        }
    }

    /// Read all logs for a plan as sorted `(log_id, bytes)` pairs.
    /// Flushes any pending buffers for this plan first so all writes are visible.
    pub(crate) async fn read_blocks_for_plan(&self, plan_id: &str) -> Vec<(String, Vec<u8>)> {
        // Flush pending for this plan first.
        let flushes: Vec<(String, String, u64, Vec<u8>)> = {
            let mut guard = self.entries.lock();
            let mut to_flush = Vec::new();
            for entry in guard.values_mut() {
                if entry.plan_id == plan_id && !entry.pending.is_empty() {
                    let bytes = std::mem::take(&mut entry.pending);
                    let seq = entry.next_seq;
                    entry.next_seq += 1;
                    to_flush.push((entry.plan_id.clone(), entry.log_id.clone(), seq, bytes));
                }
            }
            to_flush
        };
        for (plan, log, seq, bytes) in flushes {
            let dir = self.log_dir(&plan, &log);
            let _ = tokio::task::spawn_blocking(move || {
                write_and_prune_segment(&dir, seq, &bytes);
            })
            .await;
        }

        let plan_dir = self.plan_dir(plan_id);
        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();
            let entries = match std::fs::read_dir(&plan_dir) {
                Ok(entries) => entries,
                Err(_) => return results,
            };
            for entry in entries.flatten() {
                if !entry.path().is_dir() {
                    continue;
                }
                let log_id = entry.file_name().to_string_lossy().into_owned();
                let bytes = read_all_segments_sync(&entry.path());
                results.push((log_id, bytes));
            }
            // Sort by numeric log_id ascending.
            results.sort_by(|(left, _), (right, _)| {
                match (left.parse::<u64>(), right.parse::<u64>()) {
                    (Ok(left), Ok(right)) => left.cmp(&right),
                    (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                    (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                    (Err(_), Err(_)) => left.cmp(right),
                }
            });
            results
        })
        .await
        .unwrap_or_default()
    }

    /// Read all bytes for one log key.
    pub(crate) async fn read_all(&self, plan_id: &str, log_id: &str) -> Vec<u8> {
        let key = Self::key(plan_id, log_id);
        let pending_flush = {
            let mut guard = self.entries.lock();
            guard.get_mut(&key).and_then(|entry| {
                if !entry.pending.is_empty() {
                    let bytes = std::mem::take(&mut entry.pending);
                    let seq = entry.next_seq;
                    entry.next_seq += 1;
                    Some((entry.plan_id.clone(), entry.log_id.clone(), seq, bytes))
                } else {
                    None
                }
            })
        };
        if let Some((plan, log, seq, bytes)) = pending_flush {
            let dir = self.log_dir(&plan, &log);
            let _ = tokio::task::spawn_blocking(move || {
                write_and_prune_segment(&dir, seq, &bytes);
            })
            .await;
        }

        let dir = self.log_dir(plan_id, log_id);
        tokio::task::spawn_blocking(move || read_all_segments_sync(&dir))
            .await
            .unwrap_or_default()
    }

    /// Delete all live log segments for a plan.
    pub(crate) async fn delete_plan(&self, plan_id: &str) {
        {
            let mut guard = self.entries.lock();
            guard.retain(|_, v| v.plan_id != plan_id);
        }
        let dir = self.plan_dir(plan_id);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = std::fs::remove_dir_all(&dir);
        })
        .await;
    }

    /// Delete all live log segments for one log file.
    pub(crate) async fn delete_log(&self, plan_id: &str, log_id: &str) {
        let key = Self::key(plan_id, log_id);
        {
            let mut guard = self.entries.lock();
            guard.remove(&key);
        }
        let dir = self.log_dir(plan_id, log_id);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = std::fs::remove_dir_all(&dir);
        })
        .await;
    }

    /// Prune plans not in `active_plans`, mirroring `prune_replay_results`.
    pub(crate) async fn prune_inactive_plans(
        &self,
        active_plans: &std::collections::BTreeSet<String>,
    ) {
        let root = self.root.clone();
        let active = active_plans.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let entries = match std::fs::read_dir(&root) {
                Ok(entries) => entries,
                Err(_) => return,
            };
            for entry in entries.flatten() {
                if !entry.path().is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if !active.contains(&name) {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        })
        .await;
    }
}

/// Atomically write a segment and prune oldest segments if total size exceeds budget.
fn write_and_prune_segment(dir: &Path, seq: u64, bytes: &[u8]) {
    if let Err(e) = std::fs::create_dir_all(dir) {
        tracing::warn!(?e, path = %dir.display(), "failed to create live-log segment dir");
        return;
    }
    let target = dir.join(format!("seg-{seq:06}.log"));
    let temp = dir.join(format!("seg-{seq:06}.tmp"));
    if let Err(e) = std::fs::write(&temp, bytes) {
        tracing::warn!(?e, path = %temp.display(), "failed to write live-log temp segment");
        return;
    }
    if let Err(e) = std::fs::rename(&temp, &target) {
        tracing::warn!(?e, from = %temp.display(), to = %target.display(), "failed to publish live-log segment");
        let _ = std::fs::remove_file(&temp);
        return;
    }

    // Prune oldest segments if total byte count exceeds limit.
    prune_old_segments(dir);
}

fn prune_old_segments(dir: &Path) {
    let mut segments = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("seg-") && n.ends_with(".log"))
        {
            if let Ok(meta) = entry.metadata() {
                segments.push((path, meta.len() as usize));
            }
        }
    }
    segments.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut total: usize = segments.iter().map(|(_, sz)| *sz).sum();
    for (path, sz) in segments {
        if total <= MAX_LIVE_LOG_BYTES_PER_KEY {
            break;
        }
        let _ = std::fs::remove_file(&path);
        total = total.saturating_sub(sz);
    }
}

fn read_all_segments_sync(dir: &Path) -> Vec<u8> {
    let mut segments = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("seg-") && n.ends_with(".log"))
        {
            segments.push(path);
        }
    }
    segments.sort();
    let mut out = Vec::new();
    for path in segments {
        if let Ok(bytes) = std::fs::read(&path) {
            out.extend_from_slice(&bytes);
        }
    }
    out
}
