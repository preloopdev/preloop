//! Directory-based mutex for one mirror repository.
//!
//! The holder refreshes a heartbeat file while it works. Without it a slow
//! upstream fetch would have its lock stolen mid-write at the stale threshold
//! and share the repository with a second writer. Takeover still applies to
//! holders that died without cleanup: their heartbeat goes stale exactly like
//! their lock did.

use std::path::{Path, PathBuf};

/// Liveness marker refreshed by the lock holder. Presence plus freshness means
/// "owned"; absence or age means "abandoned".
pub const LOCK_HEARTBEAT: &str = "preloop-lock-heartbeat";

/// How long a lock may go without a heartbeat before it is treated as
/// abandoned.
pub const LOCK_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// How often the holder refreshes its heartbeat. Comfortably inside
/// [`LOCK_STALE_AFTER`] so a live holder is never mistaken for a corpse.
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);

/// How long [`acquire`] waits for a live holder before giving up.
const ACQUIRE_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

/// An owned mirror lock. Dropping it removes the heartbeat and the directory,
/// so a panicking or cancelled writer never leaves a lock behind.
#[derive(Debug)]
pub struct MirrorLock {
    dir: PathBuf,
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl MirrorLock {
    /// Take ownership of `dir`, which the caller has already created.
    fn held(dir: PathBuf) -> Self {
        let heartbeat_file = dir.join(LOCK_HEARTBEAT);
        let _ = std::fs::write(&heartbeat_file, "held");
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                if std::fs::write(&heartbeat_file, "held").is_err() {
                    return;
                }
            }
        });
        Self {
            dir,
            heartbeat: Some(heartbeat),
        }
    }

    /// Path of the lock directory, for diagnostics.
    pub fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for MirrorLock {
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
pub fn lock_age(path: &Path) -> Option<std::time::Duration> {
    let marker = path.join(LOCK_HEARTBEAT);
    let target = if marker.is_file() { &marker } else { path };
    std::fs::metadata(target)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.elapsed().ok())
}

/// Take over a stale lock directory, if it is stale. Returns whether the lock
/// is now free for a fresh `create_dir`.
fn take_over_if_stale(path: &Path) {
    if !lock_age(path).is_some_and(|age| age > LOCK_STALE_AFTER) {
        return;
    }
    let _ = std::fs::remove_file(path.join(LOCK_HEARTBEAT));
    let _ = std::fs::remove_dir(path);
}

/// Wait for the mirror lock at `path`, taking over stale holders.
///
/// Returns once the directory is created. A live holder that never releases
/// makes this fail after [`ACQUIRE_WAIT_LIMIT`] rather than block a handler
/// indefinitely; a fetch that outlives its own deadline is the only way to
/// get there.
pub async fn acquire(path: &Path) -> Result<MirrorLock, crate::MirrorError> {
    let started = std::time::Instant::now();
    loop {
        match std::fs::create_dir(path) {
            Ok(()) => return Ok(MirrorLock::held(path.to_owned())),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                take_over_if_stale(path);
                if started.elapsed() > ACQUIRE_WAIT_LIMIT {
                    return Err(crate::MirrorError::Io(format!(
                        "timed out waiting for mirror lock {}",
                        path.display()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(error) => {
                return Err(crate::MirrorError::io("failed to lock mirror", &error));
            }
        }
    }
}

/// Take the mirror lock only if it is free, without waiting.
///
/// Eviction sweeps run beside writes, so a held lock means "this entry is busy
/// right now" and the sweep moves on. A stale lock is taken over exactly like
/// [`acquire`] does.
pub fn try_acquire(path: &Path) -> Option<MirrorLock> {
    match std::fs::create_dir(path) {
        Ok(()) => return Some(MirrorLock::held(path.to_owned())),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return None,
    }
    if !lock_age(path).is_some_and(|age| age > LOCK_STALE_AFTER) {
        return None;
    }
    let _ = std::fs::remove_file(path.join(LOCK_HEARTBEAT));
    let _ = std::fs::remove_dir(path);
    std::fs::create_dir(path)
        .ok()
        .map(|()| MirrorLock::held(path.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropping_the_lock_releases_the_directory() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mirror.lock");
        {
            let lock = acquire(&path).await.unwrap();
            assert_eq!(lock.path(), path.as_path());
            assert!(path.is_dir());
        }
        assert!(!path.exists());
        // The next taker is not blocked by the released directory.
        let second = try_acquire(&path).expect("a released lock is free");
        drop(second);
    }

    #[tokio::test]
    async fn a_live_lock_is_not_taken_over() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mirror.lock");
        let held = try_acquire(&path).expect("first taker wins");
        assert!(
            try_acquire(&path).is_none(),
            "a freshly heartbeated lock belongs to its holder"
        );
        drop(held);
    }

    #[tokio::test]
    async fn an_abandoned_lock_is_taken_over() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("mirror.lock");
        std::fs::create_dir(&path).unwrap();
        // A holder that died leaves a heartbeat that stops being refreshed.
        let heartbeat = path.join(LOCK_HEARTBEAT);
        std::fs::write(&heartbeat, "held").unwrap();
        let stale = std::time::SystemTime::now() - std::time::Duration::from_secs(300);
        std::fs::File::options()
            .write(true)
            .open(&heartbeat)
            .unwrap()
            .set_modified(stale)
            .unwrap();
        assert!(
            try_acquire(&path).is_some(),
            "a lock whose heartbeat is older than the threshold is a corpse"
        );
    }
}
