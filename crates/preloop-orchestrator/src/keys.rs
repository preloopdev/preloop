//! Ahead-of-time RSA keypairs for ephemeral runner registration.
//!
//! `preloop-runner configure` needs a fresh 2048-bit RSA keypair, and
//! generating one costs 70-180 ms. For a pool of single-use runners that lands
//! on the path between one job finishing and the slot being ready for the
//! next, and — worse during a matrix — it burns a guest vCPU next to the jobs
//! being measured.
//!
//! Nothing about the key depends on the runner it will belong to, so it can be
//! made in advance on the host. This keeps a small buffer of keys topped up in
//! the background and hands them out on demand. Every runner still gets its
//! own key; only the timing moves.
//!
//! # Handling
//!
//! A key is delivered by writing it to a private file under `PRELOOP_HOME` and
//! pointing SmolVM's `--secret-file` at it, so the private key never appears
//! in an argument vector, an environment variable of the engine, or the
//! machine record. The file is removed as soon as `configure` returns.
//!
//! This does briefly place a runner's private key on the host filesystem,
//! where it would otherwise only exist inside the guest. It authenticates one
//! ephemeral local runner to one local control plane, the file is `0600`
//! inside a `0700` directory, and it is deleted immediately after use.
//! Two rejected alternatives were worse: sharing one keypair across the pool
//! would let any job decrypt another runner's job messages, and pre-seeding a
//! key into the golden image would give every fork the same one.

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use preloop_gha_protocol::crypto::AgentRsaKeypair;
use tokio::sync::Mutex;
use tracing::warn;

/// Keys kept ready. Two covers the common case of a couple of slots turning
/// over at once without holding meaningful memory or entropy hostage.
const BUFFER: usize = 2;

/// A background-filled buffer of runner keypairs.
#[derive(Debug, Default)]
pub(crate) struct KeyPool {
    ready: Mutex<Vec<String>>,
    refilling: AtomicBool,
}

impl KeyPool {
    /// Start filling the buffer.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Take a keypair as `RSAParameters` JSON, or `None` when the buffer is
    /// empty.
    ///
    /// Returning `None` is normal under a burst; the caller falls back to
    /// in-guest generation, which is exactly the old behaviour. Waiting here
    /// would trade the latency we are trying to remove for a different one.
    pub(crate) async fn take(self: &Arc<Self>) -> Option<String> {
        let key = self.ready.lock().await.pop();
        self.spawn_refill();
        key
    }

    /// Bring the buffer back up to size without blocking the caller.
    pub(crate) fn spawn_refill(self: &Arc<Self>) {
        if !self.claim_refill() {
            return;
        }
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if pool.ready.lock().await.len() >= BUFFER {
                    pool.release_refill().await;
                    return;
                }
                let Ok(Ok(key)) = tokio::task::spawn_blocking(generate).await else {
                    warn!(
                        "pre-generating a runner keypair failed; runners will generate their own"
                    );
                    pool.refilling.store(false, Ordering::Release);
                    return;
                };
                let mut ready = pool.ready.lock().await;
                if ready.len() < BUFFER {
                    ready.push(key);
                }
            }
        });
    }

    fn claim_refill(&self) -> bool {
        self.refilling
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Relinquish the producer role without losing a concurrent `take`.
    ///
    /// A consumer may observe `refilling = true` just before this task finds a
    /// full buffer. Release first, then recheck capacity: either this task or
    /// that consumer will claim and start the replacement refill.
    async fn release_refill(self: &Arc<Self>) {
        self.refilling.store(false, Ordering::Release);
        if self.ready.lock().await.len() < BUFFER {
            self.spawn_refill();
        }
    }
}

fn generate() -> Result<String, String> {
    let keypair = AgentRsaKeypair::generate().map_err(|error| error.to_string())?;
    serde_json::to_string(&keypair.to_rsaparams()).map_err(|error| error.to_string())
}

/// A keypair staged on disk for exactly one `configure` call.
///
/// Dropping it removes the file, so no error path can leave a private key
/// behind.
pub(crate) struct StagedKey {
    path: PathBuf,
}

impl StagedKey {
    /// Write `params` to a private file under `directory`.
    pub(crate) fn write(directory: &Path, machine: &str, params: &str) -> std::io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        restrict(directory, 0o700)?;
        let path = directory.join(format!("{machine}.json"));
        std::fs::write(&path, params)?;
        restrict(&path, 0o600)?;
        Ok(Self { path })
    }

    /// Absolute path SmolVM should read the value from.
    pub(crate) fn path(&self) -> std::io::Result<PathBuf> {
        std::fs::canonicalize(&self.path)
    }
}

impl Drop for StagedKey {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                warn!(path = %self.path.display(), %error, "failed to remove staged runner key");
            }
        }
    }
}

#[cfg(unix)]
fn restrict(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn restrict(_path: &Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_key_is_private_and_removed_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let staged = StagedKey::write(directory.path(), "runner-1", "{}").unwrap();
        let path = staged.path().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        drop(staged);
        assert!(!path.exists(), "the key file must not outlive its use");
    }

    #[tokio::test]
    async fn pool_hands_out_importable_keypairs() {
        let pool = Arc::new(KeyPool::new());
        for _ in 0..64 {
            pool.spawn_refill();
        }
        // Concurrent refill requests still elect exactly one producer. Wait
        // for it to finish so an overfill cannot hide behind an early pop.
        let key = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                if !pool.refilling.load(Ordering::Acquire) {
                    let mut ready = pool.ready.lock().await;
                    if ready.len() == BUFFER {
                        return ready.pop().unwrap();
                    }
                }
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the pool produces a keypair");
        let params: preloop_gha_protocol::crypto::RsaParametersExport =
            serde_json::from_str(&key).unwrap();
        AgentRsaKeypair::from_rsaparams(&params).expect("a pre-generated key imports cleanly");
    }
}
