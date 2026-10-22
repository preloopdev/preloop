//! Sandbox Service access for the header, overview and sandboxes screen.
use crate::options::SandboxTarget;
use anyhow::{Context as _, anyhow};
use preloop_sandbox::api::SandboxStateExt;
use preloop_sandbox::{SandboxApi, SandboxClient};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxSummary {
    pub providers: Vec<ProviderRow>,
    pub sandboxes: usize,
    pub host_mem: Option<HostMem>,
    pub pools: Vec<PoolRow>,
    pub templates: Vec<TemplateRow>,
    pub sandboxes_rows: Vec<SandboxRow>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRow {
    pub name: String,
    pub cordoned: bool,
    pub sandboxes: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolRow {
    pub name: String,
    pub template: String,
    pub warm: u32,
    pub max: u32,
    pub leased: usize,
    pub lease_ttl_secs: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemplateRow {
    pub name: String,
    pub description: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxRow {
    pub id: String,
    pub state: String,
    pub purpose: String,
    pub pool: String,
    pub error: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostMem {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

pub struct SandboxHandle {
    client: SandboxClient,
}
impl SandboxHandle {
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
