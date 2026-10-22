//! Sandbox Service access for the header and overview.
//!
//! This wraps `preloop_sandbox::SandboxClient` and reads only what the header
//! and overview show. Lease ids are never copied out of the service's answers.
//! Sandbox-scoped operations need a lease the TUI holds, and that arrives with
//! the sandboxes view.

use anyhow::{Context as _, anyhow};
use preloop_sandbox::{SandboxApi, SandboxClient};

use crate::options::SandboxTarget;

/// What the header and overview read from the Sandbox Service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxSummary {
    /// Providers the service knows, in service order.
    pub providers: Vec<ProviderRow>,
    /// Sandboxes across all providers.
    pub sandboxes: usize,
    /// Host memory from the first provider that reported a sample.
    pub host_mem: Option<HostMem>,
}

/// One provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRow {
    /// Provider name.
    pub name: String,
    /// True while new work is refused to this provider.
    pub cordoned: bool,
}

/// Host memory from a provider's last sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostMem {
    /// Total memory in bytes.
    pub total_bytes: u64,
    /// Memory available in bytes.
    pub available_bytes: u64,
}

/// A connected Sandbox Service client.
pub struct SandboxHandle {
    client: SandboxClient,
}

impl SandboxHandle {
    /// Connect to `target`. A CA, when given, is the only root trusted.
    pub fn connect(target: &SandboxTarget) -> anyhow::Result<Self> {
        let token = target.token.as_ref().map(|token| token.expose().to_owned());
        let client = match target.ca_pem.as_deref() {
            Some(ca_pem) => SandboxClient::with_tls(&target.url, token, ca_pem, None),
            None => SandboxClient::new(&target.url, token),
        }
        .map_err(|error| anyhow!("{error}"))
        .with_context(|| format!("connecting to the sandbox service at {}", target.url))?;
        Ok(Self { client })
    }

    /// Providers, sandbox count and host memory.
    pub async fn summary(&self) -> anyhow::Result<SandboxSummary> {
        let providers = self
            .client
            .list_providers()
            .await
            .map_err(|error| anyhow!("{error}"))
            .context("listing sandbox providers")?;
        let stats = self
            .client
            .stats()
            .await
            .map_err(|error| anyhow!("{error}"))
            .context("reading sandbox stats")?;

        let sandboxes = stats
            .providers
            .iter()
            .map(|provider| provider.sandboxes.len())
            .sum();
        let host_mem = stats.providers.iter().find_map(|provider| {
            let host = provider.host.as_option()?;
            Some(HostMem {
                total_bytes: host.mem_total_bytes?,
                available_bytes: host.mem_available_bytes?,
            })
        });

        Ok(SandboxSummary {
            providers: providers
                .into_iter()
                .map(|provider| ProviderRow {
                    name: provider.name,
                    cordoned: provider.cordoned,
                })
                .collect(),
            sandboxes,
            host_mem,
        })
    }
}
