//! Bounded, file-backed preview of live console output.
//!
//! The official runner uploads complete step logs separately. These segments
//! preserve the already-masked live preview across a server restart. A local
//! filesystem is not a shared BlobStore: another control node cannot read it.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const MAX_LIVE_LOG_BYTES_PER_KEY: usize = 16 * 1024 * 1024;
pub(crate) const FLUSH_BYTES_THRESHOLD: usize = 1024 * 1024;
pub(crate) const FLUSH_STALE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_ACTIVE_KEYS: usize = 4096;

#[derive(Default)]
struct KeyState {
    pending: Vec<u8>,
    next_seq: Option<u64>,
    last_write: Option<Instant>,
}

type KeyEntries = BTreeMap<(String, String), Arc<tokio::sync::Mutex<KeyState>>>;

#[derive(Clone)]
pub(crate) struct LiveLogSegments {
    root: PathBuf,
    entries: Arc<parking_lot::Mutex<KeyEntries>>,
}

// Encode the complete UTF-8 component injectively; replacing punctuation with
// '_' mapped different authenticated plan IDs onto the same directory.
fn component(id: &str) -> io::Result<String> {
    if id.is_empty() || id.len() > 128 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid log identity length",
        ));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(id.len() * 2);
    for byte in id.bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0xf) as usize] as char);
    }
    Ok(encoded)
}

fn decode_component(encoded: &str) -> Option<String> {
    if !encoded.len().is_multiple_of(2) {
        return None;
    }
    let bytes: Option<Vec<u8>> = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16)? as u8;
            let lo = (pair[1] as char).to_digit(16)? as u8;
            Some((hi << 4) | lo)
        })
        .collect();
    String::from_utf8(bytes?).ok()
}

fn segment_number(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("seg-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

fn segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf, u64)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Some(seq) = segment_number(&entry.path()) {
            let meta = entry.metadata()?;
            if meta.is_file() {
                out.push((seq, entry.path(), meta.len()));
            }
        }
    }
    out.sort_by_key(|(seq, _, _)| *seq);
    Ok(out)
}

fn publish(dir: &Path, seq: u64, bytes: &[u8]) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let target = dir.join(format!("seg-{seq:020}.log"));
    let temp = dir.join(format!("seg-{seq:020}-{}.tmp", uuid::Uuid::new_v4()));
    let result: io::Result<()> = (|| {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        // `rename` would overwrite an existing segment on Unix. Never do so.
        match std::fs::hard_link(&temp, &target) {
            Ok(()) => {}
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    && std::fs::read(&target)? == bytes => {}
            Err(error) => return Err(error),
        }
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&temp);
    result?;
    let mut kept = segments(dir)?;
    let mut total: u64 = kept.iter().map(|(_, _, size)| *size).sum();
    for (_, path, size) in kept.drain(..) {
        if total <= MAX_LIVE_LOG_BYTES_PER_KEY as u64 {
            break;
        }
        // The segment was already published. A failed prune must not cause
        // retrying a successful append and duplicating its bytes.
        if let Err(error) = std::fs::remove_file(&path) {
            tracing::warn!(%error, path = %path.display(), "failed to prune live log");
        } else {
            total -= size;
        }
    }
    Ok(())
}

fn read_segments(dir: &Path) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for (_, path, _) in segments(dir)? {
        out.extend_from_slice(&std::fs::read(path)?);
    }
    Ok(out)
}

impl LiveLogSegments {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            entries: Arc::new(parking_lot::Mutex::new(BTreeMap::new())),
        }
    }

    fn log_dir(&self, plan: &str, log: &str) -> io::Result<PathBuf> {
        Ok(self.root.join(component(plan)?).join(component(log)?))
    }

    fn state(&self, plan: &str, log: &str) -> io::Result<Arc<tokio::sync::Mutex<KeyState>>> {
        // Validate identities before registering a key (including before a
        // new tiny chunk could allocate a buffer).
        self.log_dir(plan, log)?;
        let mut entries = self.entries.lock();
        let key = (plan.to_owned(), log.to_owned());
        if let Some(state) = entries.get(&key) {
            return Ok(state.clone());
        }
        if entries.len() >= MAX_ACTIVE_KEYS {
            // Idle entries can be reconstructed from published segments.
            // Keep a key with pending bytes or an in-flight operation.
            entries.retain(|_, state| {
                if Arc::strong_count(state) > 1 {
                    return true;
                }
                state
                    .try_lock()
                    .map_or(true, |guard| !guard.pending.is_empty())
            });
            if entries.len() >= MAX_ACTIVE_KEYS {
                return Err(io::Error::other("too many active live log keys"));
            }
        }
        let state = Arc::new(tokio::sync::Mutex::new(KeyState::default()));
        entries.insert(key, state.clone());
        Ok(state)
    }

    async fn flush(&self, plan: &str, log: &str, state: &mut KeyState) -> io::Result<()> {
        if state.pending.is_empty() {
            return Ok(());
        }
        let dir = self.log_dir(plan, log)?;
        let seq = match state.next_seq {
            Some(seq) => seq,
            None => segments(&dir)?.last().map_or(0, |(seq, _, _)| seq + 1),
        };
        // The key mutex remains held across blocking I/O on the blocking
        // pool, serializing publication, read and pruning for this key.
        let bytes = state.pending.clone();
        tokio::task::spawn_blocking(move || publish(&dir, seq, &bytes))
            .await
            .map_err(io::Error::other)??;
        state.pending.clear();
        state.next_seq = Some(seq + 1);
        Ok(())
    }

    pub(crate) async fn append(&self, plan: &str, log: &str, bytes: &[u8]) -> io::Result<()> {
        let state = self.state(plan, log)?;
        let mut state = state.lock().await;
        // One very large runner batch must not sit unbounded in memory.
        for chunk in bytes.chunks(FLUSH_BYTES_THRESHOLD) {
            state.pending.extend_from_slice(chunk);
            state.last_write = Some(Instant::now());
            if state.pending.len() >= FLUSH_BYTES_THRESHOLD {
                self.flush(plan, log, &mut state).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_stale(&self) -> io::Result<()> {
        let entries: Vec<_> = self
            .entries
            .lock()
            .iter()
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect();
        for ((plan, log), state) in entries {
            let mut state = state.lock().await;
            if state
                .last_write
                .is_some_and(|at| at.elapsed() >= FLUSH_STALE_INTERVAL)
            {
                self.flush(&plan, &log, &mut state).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush_all(&self) -> io::Result<()> {
        let entries: Vec<_> = self
            .entries
            .lock()
            .iter()
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect();
        for ((plan, log), state) in entries {
            let mut guard = state.lock().await;
            self.flush(&plan, &log, &mut guard).await?;
        }
        Ok(())
    }

    pub(crate) async fn read_all(&self, plan: &str, log: &str) -> io::Result<Vec<u8>> {
        let state = self.state(plan, log)?;
        let mut state = state.lock().await;
        self.flush(plan, log, &mut state).await?;
        let dir = self.log_dir(plan, log)?;
        tokio::task::spawn_blocking(move || read_segments(&dir))
            .await
            .map_err(io::Error::other)?
    }

    pub(crate) async fn read_blocks_for_plan(
        &self,
        plan: &str,
    ) -> io::Result<Vec<(String, Vec<u8>)>> {
        let dir = self.root.join(component(plan)?);
        let mut keys: BTreeSet<String> = self
            .entries
            .lock()
            .keys()
            .filter(|(p, _)| p == plan)
            .map(|(_, log)| log.clone())
            .collect();
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if let Some(entries) = entries {
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    if let Some(id) = decode_component(&entry.file_name().to_string_lossy()) {
                        keys.insert(id);
                    }
                }
            }
        }
        let mut results = Vec::with_capacity(keys.len());
        for log in keys {
            results.push((log.clone(), self.read_all(plan, &log).await?));
        }
        results.sort_by(|(left, _), (right, _)| {
            match (left.parse::<u64>(), right.parse::<u64>()) {
                (Ok(l), Ok(r)) => l.cmp(&r),
                (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                _ => left.cmp(right),
            }
        });
        Ok(results)
    }

    /// Retain the newest inactive plan directories, as with completed results.
    /// Never delete a still-active plan merely because another job completed.
    pub(crate) async fn prune_inactive_plans(&self, active: &BTreeSet<String>) -> io::Result<()> {
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let mut inactive = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if decode_component(&name).is_some_and(|id| active.contains(&id)) {
                continue;
            }
            inactive.push((entry.metadata()?.modified()?, entry.path()));
        }
        inactive.sort_by_key(|(time, _)| std::cmp::Reverse(*time));
        for (_, path) in inactive
            .into_iter()
            .skip(crate::blob_store::REPLAY_PLANS_RETAINED)
        {
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn restart_appends_without_overwriting_published_segment() {
        let tmp = tempfile::tempdir().unwrap();
        let first = LiveLogSegments::new(tmp.path().to_path_buf());
        first.append("plan", "0", b"one\n").await.unwrap();
        first.flush_all().await.unwrap();
        let second = LiveLogSegments::new(tmp.path().to_path_buf());
        second.append("plan", "0", b"two\n").await.unwrap();
        second.flush_all().await.unwrap();
        assert_eq!(second.read_all("plan", "0").await.unwrap(), b"one\ntwo\n");
    }

    #[tokio::test]
    async fn path_encoding_does_not_alias_or_traverse() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LiveLogSegments::new(tmp.path().to_path_buf());
        store.append("a/b", "0", b"a").await.unwrap();
        store.append("a_b", "0", b"b").await.unwrap();
        assert_eq!(store.read_all("a/b", "0").await.unwrap(), b"a");
        assert_eq!(store.read_all("a_b", "0").await.unwrap(), b"b");
    }
    #[tokio::test]
    async fn failed_flush_preserves_pending_bytes_for_retry() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        std::fs::write(&root, b"not a directory").unwrap();
        let store = LiveLogSegments::new(root.clone());
        store.append("plan", "0", b"line\n").await.unwrap();
        assert!(store.flush_all().await.is_err());
        std::fs::remove_file(&root).unwrap();
        store.flush_all().await.unwrap();
        assert_eq!(store.read_all("plan", "0").await.unwrap(), b"line\n");
    }

    #[tokio::test]
    async fn concurrent_appends_and_flushes_do_not_duplicate_or_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LiveLogSegments::new(tmp.path().to_path_buf());
        let mut tasks = Vec::new();
        for i in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .append("plan", "0", format!("{i:02}\n").as_bytes())
                    .await
                    .unwrap();
                store.flush_all().await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let contents = String::from_utf8(store.read_all("plan", "0").await.unwrap()).unwrap();
        let mut values: Vec<_> = contents.lines().collect();
        values.sort_unstable();
        assert_eq!(
            values,
            (0..32).map(|i| format!("{i:02}")).collect::<Vec<_>>()
        );
    }
}
