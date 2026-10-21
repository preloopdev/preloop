//! The embedding engine's live sandbox service, published to the runner
//! server for status and host metrics.
//!
//! Sandbox-owned telemetry (host memory, per-sandbox runtime) is measured by
//! the sandbox service's drivers, not by this control plane: the pool samples
//! its host and heartbeats it in, and `Stats` exposes it. Handing this handle
//! to the server keeps that data flow one-directional — the runner server
//! never reaches into the pool.
//!
//! `Stats` is aggregates-only (another client-token holder must not learn
//! host PIDs, process names or paths), and an unset handle or a failed call
//! is *unknown*, never an empty fleet; the status builder turns the `None`
//! into a `vm_sample_unavailable` condition.

use std::sync::Arc;

use preloop_sandbox::SandboxApi;
use preloop_sandbox::api::StatsResponse;

/// The published service plus the pool provider id whose host sample
/// describes this engine's host.
type Published = (Arc<dyn SandboxApi>, String);

/// Cloneable handle to the co-hosted sandbox service.
///
/// Unset ([`SandboxHandle::new`]/[`Default`]) is the standalone-server and
/// test case: every read degrades to "no data yet" instead of fabricating a
/// zero. `set` is called once the pool has started its service; a pool that
/// later stops leaves the last handle in place, and a failing `Stats` call
/// reports nothing rather than failing the status path.
#[derive(Clone, Default)]
pub struct SandboxHandle {
    /// The live API plus the provider id whose host sample describes *this*
    /// engine's host (the co-hosted pool runs one provider, whose sample is
    /// the host's).
    inner: Arc<parking_lot::RwLock<Option<Published>>>,
}

impl SandboxHandle {
    /// An unset handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish the service and the pool provider id whose host sample is the
    /// engine host's. Replaces any previous handle.
    pub fn set(&self, api: Arc<dyn SandboxApi>, pool_provider_id: String) {
        *self.inner.write() = Some((api, pool_provider_id));
    }

    /// The pool provider id, or an empty string when unset.
    pub fn pool_provider_id(&self) -> String {
        self.inner
            .read()
            .as_ref()
            .map(|(_, provider_id)| provider_id.clone())
            .unwrap_or_default()
    }

    /// Live sandbox statistics, or `None` when unset or the RPC fails.
    ///
    /// The lock is released before awaiting: the call is best-effort status
    /// data and must never serialize request handling on a slow service.
    pub async fn stats(&self) -> Option<StatsResponse> {
        let api = {
            let guard = self.inner.read();
            guard.as_ref().map(|(api, _)| Arc::clone(api))
        }?;
        match api.stats().await {
            Ok(stats) => Some(stats),
            Err(error) => {
                tracing::debug!(%error, "sandbox stats unavailable");
                None
            }
        }
    }
}
