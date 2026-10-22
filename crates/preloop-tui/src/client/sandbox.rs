//! Sandbox Service access for the header, overview and sandboxes screen.

use anyhow::{Context as _, anyhow};
use preloop_sandbox::api::SandboxStateExt;
use preloop_sandbox::{SandboxApi, SandboxClient};

use crate::options::SandboxTarget;

/// What the header, overview and sandboxes screen read from the service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxSummary {
    /// Providers the service knows.
    pub providers: Vec<ProviderRow>,
    /// Sandboxes across all providers.
    pub sandboxes: usize,
    /// Host memory from the first provider that reported a sample.
    pub host_mem: Option<HostMem>,
    /// Warm pools the service defines.
    pub pools: Vec<PoolRow>,
    /// Templates registered with the service.
    pub templates: Vec<TemplateRow>,
    /// Sandboxes the service currently reports.
    pub sandboxes_rows: Vec<SandboxRow>,
}

/// One provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRow {
    /// Provider name.
    pub name: String,
    /// True while new work is refused to this provider.
    pub cordoned: bool,
    /// Sandboxes this provider hosts.
    pub sandboxes: usize,
}

/// One warm pool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolRow {
    /// Pool name.
    pub name: String,
    /// Template the pool provisions from.
    pub template: String,
    /// Warm sandboxes currently available.
    pub warm: u32,
    /// Maximum sandboxes the pool may hold.
    pub max: u32,
    /// Sandboxes leased from this pool.
    pub leased: usize,
    /// Lease time-to-live in seconds.
    pub lease_ttl_secs: u64,
}

/// One template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateRow {
    /// Template name.
    pub name: String,
    /// One-line description (the operating system or image reference).
    pub description: String,
}

/// One sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxRow {
    /// Sandbox id.
    pub id: String,
    /// Upper-case state name.
    pub state: String,
    /// Upper-case purpose name.
    pub purpose: String,
    /// Pool the sandbox belongs to, when any.
    pub pool: String,
    /// Last provisioning error, when any.
    pub error: String,
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
            Some(ca) => SandboxClient::with_tls(&target.url, token, ca, None),
            None => SandboxClient::new(&target.url, token),
        }
        .map_err(|e| anyhow!("{e}"))
        .with_context(|| format!("connecting to the sandbox service at {}", target.url))?;
        Ok(Self { client })
    }

    /// Providers, pools, templates, sandboxes and host memory.
    pub async fn summary(&self) -> anyhow::Result<SandboxSummary> {
        let providers = self
            .client
            .list_providers()
            .await
            .map_err(|e| anyhow!("{e}"))
            .context("listing sandbox providers")?;
        let stats = self
            .client
            .stats()
            .await
            .map_err(|e| anyhow!("{e}"))
            .context("reading sandbox stats")?;
        let pools = self
            .client
            .list_pools()
            .await
            .map_err(|e| anyhow!("{e}"))
            .context("listing sandbox pools")?;
        let templates = self
            .client
            .list_templates()
            .await
            .map_err(|e| anyhow!("{e}"))
            .context("listing sandbox templates")?;
        let rows = self
            .client
            .list()
            .await
            .map_err(|e| anyhow!("{e}"))
            .context("listing sandboxes")?;
        let sandboxes = rows.len();
        let host_mem = stats.providers.iter().find_map(|p| {
            let h = p.host.as_option()?;
            Some(HostMem {
                total_bytes: h.mem_total_bytes?,
                available_bytes: h.mem_available_bytes?,
            })
        });
        let provider_rows = providers
            .into_iter()
            .map(|p| {
                let count = stats
                    .providers
                    .iter()
                    .find(|x| x.provider_id == p.id || x.name == p.name)
                    .map_or(0, |x| x.sandboxes.len());
                ProviderRow {
                    name: p.name,
                    cordoned: p.cordoned,
                    sandboxes: count,
                }
            })
            .collect();
        let pool_rows = pools
            .into_iter()
            .map(|p| {
                let leased = rows
                    .iter()
                    .filter(|x| {
                        x.pool == p.name
                            && x.state.known() == preloop_sandbox::api::SandboxState::Leased
                    })
                    .count();
                PoolRow {
                    name: p.name,
                    template: p.template,
                    warm: p.warm,
                    max: p.max,
                    leased,
                    lease_ttl_secs: p.lease_ttl_secs,
                }
            })
            .collect();
        let template_rows = templates
            .into_iter()
            .map(|x| TemplateRow {
                name: x
                    .spec
                    .as_option()
                    .map_or_else(String::new, |s| s.name.clone()),
                description: x
                    .spec
                    .as_option()
                    .map_or_else(String::new, |s| s.os.clone()),
            })
            .collect();
        let sandbox_rows = rows
            .into_iter()
            .map(|x| SandboxRow {
                id: x.id,
                state: format!("{:?}", x.state.known()).to_ascii_uppercase(),
                purpose: format!("{:?}", x.purpose).to_ascii_uppercase(),
                pool: x.pool,
                error: x.error,
            })
            .collect();
        Ok(SandboxSummary {
            providers: provider_rows,
            sandboxes,
            host_mem,
            pools: pool_rows,
            templates: template_rows,
            sandboxes_rows: sandbox_rows,
        })
    }
}
