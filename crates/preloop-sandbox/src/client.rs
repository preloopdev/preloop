//! The caller-side API, in-process or over the network.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::BoxStream;

use crate::api::*;
use crate::connect::{self, Error, Result};
use crate::service::{Service, collect_exec};

/// Everything a caller (CI fleet adapter, CLI, agents) can do.
#[async_trait]
pub trait SandboxApi: Send + Sync {
    /// `PutTemplate`.
    async fn put_template(&self, spec: TemplateSpec) -> Result<Template>;
    /// `ListTemplates`.
    async fn list_templates(&self) -> Result<Vec<Template>>;
    /// `DeleteTemplate`.
    async fn delete_template(&self, name: &str) -> Result<()>;
    /// `PutPool`.
    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec>;
    /// `ListPools`.
    async fn list_pools(&self) -> Result<Vec<PoolSpec>>;
    /// `DeletePool`.
    async fn delete_pool(&self, name: &str) -> Result<()>;
    /// `Acquire`.
    async fn acquire(&self, request: AcquireRequest) -> Result<AcquireResponse>;
    /// `Renew`.
    async fn renew(&self, request: RenewRequest) -> Result<Lease>;
    /// `Release`.
    async fn release(&self, request: ReleaseRequest) -> Result<()>;
    /// `Get`: one sandbox, for the lease that holds it.
    async fn get(&self, sandbox_id: &str, lease_id: &str) -> Result<Sandbox>;
    /// `List`.
    async fn list(&self) -> Result<Vec<Sandbox>>;
    /// `ListProviders`.
    async fn list_providers(&self) -> Result<Vec<Provider>>;
    /// `CordonProvider`: stop new placements on a provider.
    async fn cordon_provider(&self, name: &str) -> Result<Provider>;
    /// `UncordonProvider`: allow placements again.
    async fn uncordon_provider(&self, name: &str) -> Result<Provider>;
    /// `DrainProvider`: cordon, release idle pool members, report leases.
    async fn drain_provider(&self, name: &str) -> Result<DrainProviderResult>;
    /// `Stats`.
    async fn stats(&self) -> Result<StatsResponse>;
    /// `Exec`.
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>>;
    /// Upload a local tar, extracted at `guest_dir`.
    async fn upload(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &Path,
    ) -> Result<()>;
    /// Download a tar of `guest_path` to `dest`.
    async fn download(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_path: &str,
        dest: &Path,
    ) -> Result<()>;
    /// `Snapshot`.
    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template>;
    /// `Stop`: power off, keeping the machine.
    async fn stop(&self, sandbox_id: &str, lease_id: &str) -> Result<()>;
    /// `Start`: boot a stopped machine.
    async fn start(&self, sandbox_id: &str, lease_id: &str, forkable: bool) -> Result<()>;
    /// `Pause`.
    async fn pause(&self, sandbox_id: &str, lease_id: &str) -> Result<()>;
    /// `Resume`.
    async fn resume(&self, sandbox_id: &str, lease_id: &str) -> Result<()>;
    /// `CreateJoinToken`: mint a single-use provider enrollment token.
    async fn create_join_token(&self, ttl_secs: u64) -> Result<CreateJoinTokenResponse>;
    /// `DeleteProvider`: remove a provider and revoke its certificates.
    async fn delete_provider(&self, name: &str) -> Result<()>;

    /// Run to completion: `(exit, stdout, stderr)`.
    async fn exec_collect(&self, request: ExecRequest) -> Result<(i32, Vec<u8>, Vec<u8>)> {
        collect_exec(self.exec(request).await?).await
    }
}

/// The in-process API: a [`Service`] behind the [`SandboxApi`] trait.
#[derive(Clone)]
pub struct InProcess(pub Arc<Service>);

#[async_trait]
impl SandboxApi for InProcess {
    async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        Service::put_template(&self.0, spec).await
    }
    async fn list_templates(&self) -> Result<Vec<Template>> {
        Ok(Service::list_templates(&self.0))
    }
    async fn delete_template(&self, name: &str) -> Result<()> {
        Service::delete_template(&self.0, name).await
    }
    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        Service::put_pool(&self.0, pool).await
    }
    async fn list_pools(&self) -> Result<Vec<PoolSpec>> {
        Ok(Service::list_pools(&self.0))
    }
    async fn delete_pool(&self, name: &str) -> Result<()> {
        Service::delete_pool(&self.0, name).await
    }
    async fn acquire(&self, request: AcquireRequest) -> Result<AcquireResponse> {
        Service::acquire(&self.0, request).await
    }
    async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        Service::renew(&self.0, request).await
    }
    async fn release(&self, request: ReleaseRequest) -> Result<()> {
        Service::release(&self.0, request).await
    }
    async fn get(&self, sandbox_id: &str, lease_id: &str) -> Result<Sandbox> {
        Service::get(&self.0, sandbox_id, lease_id)
    }
    async fn list(&self) -> Result<Vec<Sandbox>> {
        Ok(Service::list(&self.0))
    }
    async fn list_providers(&self) -> Result<Vec<Provider>> {
        Ok(Service::list_providers(&self.0))
    }
    async fn cordon_provider(&self, name: &str) -> Result<Provider> {
        Service::set_provider_cordon(&self.0, name, true).await
    }
    async fn uncordon_provider(&self, name: &str) -> Result<Provider> {
        Service::set_provider_cordon(&self.0, name, false).await
    }
    async fn drain_provider(&self, name: &str) -> Result<DrainProviderResult> {
        Service::drain_provider(&self.0, name).await
    }
    async fn stats(&self) -> Result<StatsResponse> {
        Ok(Service::stats(&self.0))
    }
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        Service::exec(&self.0, request)
    }
    async fn upload(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &Path,
    ) -> Result<()> {
        Service::upload_path(&self.0, sandbox_id, lease_id, guest_dir, tar).await
    }
    async fn download(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_path: &str,
        dest: &Path,
    ) -> Result<()> {
        Service::download_path(&self.0, sandbox_id, lease_id, guest_path, dest).await
    }
    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        Service::snapshot(&self.0, request).await
    }
    async fn stop(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        Service::stop(&self.0, sandbox_id, lease_id).await
    }
    async fn start(&self, sandbox_id: &str, lease_id: &str, forkable: bool) -> Result<()> {
        Service::start(&self.0, sandbox_id, lease_id, forkable).await
    }
    async fn pause(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        Service::pause(&self.0, sandbox_id, lease_id).await
    }
    async fn resume(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        Service::resume(&self.0, sandbox_id, lease_id).await
    }
    async fn create_join_token(&self, ttl_secs: u64) -> Result<CreateJoinTokenResponse> {
        Service::create_join_token(&self.0, std::time::Duration::from_secs(ttl_secs)).await
    }
    async fn delete_provider(&self, name: &str) -> Result<()> {
        Service::delete_provider(&self.0, name).await
    }
}

const SANDBOXES: &str = "preloop.sandbox.v1.Sandboxes";

/// Network client for the `Sandboxes` service.
#[derive(Clone)]
pub struct SandboxClient {
    inner: connect::Client,
}

impl SandboxClient {
    /// `base` like `https://127.0.0.1:9191` (system trust store).
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            inner: connect::Client::new(base, token),
        }
    }

    /// `base` like `https://127.0.0.1:9191`, trusting only `ca_pem`.
    pub fn with_tls(
        base: impl Into<String>,
        token: Option<String>,
        ca_pem: &str,
        identity_pem: Option<&[u8]>,
    ) -> Result<Self> {
        Ok(Self {
            inner: connect::Client::with_tls(base, token, ca_pem, identity_pem)?,
        })
    }

    async fn call<Req: serde::Serialize + Sync, Resp: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        request: &Req,
    ) -> Result<Resp> {
        self.inner
            .unary(&format!("{SANDBOXES}/{method}"), request)
            .await
    }

    fn ticket_url(&self, ticket: &Ticket) -> String {
        if ticket.url.starts_with('/') {
            format!("{}{}", self.inner.base(), ticket.url)
        } else {
            ticket.url.clone()
        }
    }
}

#[async_trait]
impl SandboxApi for SandboxClient {
    async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        self.call("PutTemplate", &PutTemplateRequest { template: spec })
            .await
    }
    async fn list_templates(&self) -> Result<Vec<Template>> {
        let mut templates = Vec::new();
        let mut page_token = String::new();
        loop {
            let page: ListTemplatesResponse = self
                .call(
                    "ListTemplates",
                    &ListRequest {
                        page_size: crate::service::MAX_PAGE_SIZE,
                        page_token,
                    },
                )
                .await?;
            templates.extend(page.templates);
            if page.next_page_token.is_empty() {
                return Ok(templates);
            }
            page_token = page.next_page_token;
        }
    }
    async fn delete_template(&self, name: &str) -> Result<()> {
        self.call::<_, Empty>(
            "DeleteTemplate",
            &DeleteTemplateRequest {
                name: name.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        self.call("PutPool", &PutPoolRequest { pool }).await
    }
    async fn list_pools(&self) -> Result<Vec<PoolSpec>> {
        let mut pools = Vec::new();
        let mut page_token = String::new();
        loop {
            let page: ListPoolsResponse = self
                .call(
                    "ListPools",
                    &ListRequest {
                        page_size: crate::service::MAX_PAGE_SIZE,
                        page_token,
                    },
                )
                .await?;
            pools.extend(page.pools);
            if page.next_page_token.is_empty() {
                return Ok(pools);
            }
            page_token = page.next_page_token;
        }
    }
    async fn delete_pool(&self, name: &str) -> Result<()> {
        self.call::<_, Empty>(
            "DeletePool",
            &DeletePoolRequest {
                name: name.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn acquire(&self, request: AcquireRequest) -> Result<AcquireResponse> {
        self.call("Acquire", &request).await
    }
    async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        self.call("Renew", &request).await
    }
    async fn release(&self, request: ReleaseRequest) -> Result<()> {
        self.call::<_, Empty>("Release", &request).await.map(|_| ())
    }
    async fn get(&self, sandbox_id: &str, lease_id: &str) -> Result<Sandbox> {
        self.call(
            "Get",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
            },
        )
        .await
    }
    async fn list(&self) -> Result<Vec<Sandbox>> {
        let mut sandboxes = Vec::new();
        let mut page_token = String::new();
        loop {
            let page: ListSandboxesResponse = self
                .call(
                    "List",
                    &ListRequest {
                        page_size: crate::service::MAX_PAGE_SIZE,
                        page_token,
                    },
                )
                .await?;
            sandboxes.extend(page.sandboxes);
            if page.next_page_token.is_empty() {
                return Ok(sandboxes);
            }
            page_token = page.next_page_token;
        }
    }
    async fn list_providers(&self) -> Result<Vec<Provider>> {
        let mut providers = Vec::new();
        let mut page_token = String::new();
        loop {
            let page: ListProvidersResponse = self
                .call(
                    "ListProviders",
                    &ListRequest {
                        page_size: crate::service::MAX_PAGE_SIZE,
                        page_token,
                    },
                )
                .await?;
            providers.extend(page.providers);
            if page.next_page_token.is_empty() {
                return Ok(providers);
            }
            page_token = page.next_page_token;
        }
    }
    async fn cordon_provider(&self, name: &str) -> Result<Provider> {
        self.call(
            "CordonProvider",
            &ProviderControlRequest {
                name: name.to_owned(),
            },
        )
        .await
    }
    async fn uncordon_provider(&self, name: &str) -> Result<Provider> {
        self.call(
            "UncordonProvider",
            &ProviderControlRequest {
                name: name.to_owned(),
            },
        )
        .await
    }
    async fn drain_provider(&self, name: &str) -> Result<DrainProviderResult> {
        self.call(
            "DrainProvider",
            &ProviderControlRequest {
                name: name.to_owned(),
            },
        )
        .await
    }
    async fn stats(&self) -> Result<StatsResponse> {
        self.call("Stats", &ListRequest::default()).await
    }
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        Ok(self
            .inner
            .server_stream::<_, ExecEvent>(&format!("{SANDBOXES}/Exec"), &request)
            .await?
            .boxed())
    }
    async fn upload(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &Path,
    ) -> Result<()> {
        let ticket: Ticket = self
            .call(
                "OpenStream",
                &OpenStreamRequest {
                    sandbox_id: sandbox_id.to_owned(),
                    lease_id: lease_id.to_owned(),
                    kind: StreamKind::Upload,
                    path: guest_dir.to_owned(),
                },
            )
            .await?;
        let file = tokio::fs::File::open(tar)
            .await
            .map_err(|e| Error::internal(format!("upload: {e}")))?;
        let length = file.metadata().await.map(|m| m.len()).unwrap_or_default();
        let response = self
            .inner
            .http()
            .put(self.ticket_url(&ticket))
            .bearer_auth(&ticket.token)
            .header(reqwest::header::CONTENT_LENGTH, length)
            .body(reqwest::Body::wrap_stream(
                tokio_util::io::ReaderStream::new(file),
            ))
            .send()
            .await
            .map_err(|e| Error::unavailable(format!("upload: {e}")))?;
        if !response.status().is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(serde_json::from_slice::<Error>(&body).unwrap_or_else(|_| {
                Error::unavailable(String::from_utf8_lossy(&body).into_owned())
            }));
        }
        Ok(())
    }
    async fn download(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_path: &str,
        dest: &Path,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let ticket: Ticket = self
            .call(
                "OpenStream",
                &OpenStreamRequest {
                    sandbox_id: sandbox_id.to_owned(),
                    lease_id: lease_id.to_owned(),
                    kind: StreamKind::Download,
                    path: guest_path.to_owned(),
                },
            )
            .await?;
        let response = self
            .inner
            .http()
            .get(self.ticket_url(&ticket))
            .bearer_auth(&ticket.token)
            .send()
            .await
            .map_err(|e| Error::unavailable(format!("download: {e}")))?;
        if !response.status().is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(serde_json::from_slice::<Error>(&body).unwrap_or_else(|_| {
                Error::unavailable(String::from_utf8_lossy(&body).into_owned())
            }));
        }
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|e| Error::internal(format!("download: {e}")))?;
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::unavailable(format!("download: {e}")))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| Error::internal(format!("download: {e}")))?;
        }
        file.flush()
            .await
            .map_err(|e| Error::internal(format!("download: {e}")))
    }
    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        self.call("Snapshot", &request).await
    }
    async fn stop(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Stop",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn start(&self, sandbox_id: &str, lease_id: &str, forkable: bool) -> Result<()> {
        self.call::<_, Empty>(
            "Start",
            &StartRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                forkable,
            },
        )
        .await
        .map(|_| ())
    }
    async fn pause(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Pause",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn resume(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Resume",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn create_join_token(&self, ttl_secs: u64) -> Result<CreateJoinTokenResponse> {
        self.call("CreateJoinToken", &CreateJoinTokenRequest { ttl_secs })
            .await
    }
    async fn delete_provider(&self, name: &str) -> Result<()> {
        self.call::<_, Empty>(
            "DeleteProvider",
            &DeleteProviderRequest {
                name: name.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
}
