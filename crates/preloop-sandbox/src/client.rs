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
    /// `Get`.
    async fn get(&self, sandbox_id: &str) -> Result<Sandbox>;
    /// `List`.
    async fn list(&self) -> Result<Vec<Sandbox>>;
    /// `ListProviders`.
    async fn list_providers(&self) -> Result<Vec<Provider>>;
    /// `Stats`.
    async fn stats(&self) -> Result<StatsResponse>;
    /// `Exec`.
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>>;
    /// Upload a local tar, extracted at `guest_dir`.
    async fn upload(&self, sandbox_id: &str, guest_dir: &str, tar: &Path) -> Result<()>;
    /// Download a tar of `guest_path` to `dest`.
    async fn download(&self, sandbox_id: &str, guest_path: &str, dest: &Path) -> Result<()>;
    /// `Snapshot`.
    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template>;
    /// `Stop`: power off, keeping the machine.
    async fn stop(&self, sandbox_id: &str) -> Result<()>;
    /// `Start`: boot a stopped machine.
    async fn start(&self, sandbox_id: &str, forkable: bool) -> Result<()>;
    /// `Pause`.
    async fn pause(&self, sandbox_id: &str) -> Result<()>;
    /// `Resume`.
    async fn resume(&self, sandbox_id: &str) -> Result<()>;

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
    async fn get(&self, sandbox_id: &str) -> Result<Sandbox> {
        Service::get(&self.0, sandbox_id)
    }
    async fn list(&self) -> Result<Vec<Sandbox>> {
        Ok(Service::list(&self.0))
    }
    async fn list_providers(&self) -> Result<Vec<Provider>> {
        Ok(Service::list_providers(&self.0))
    }
    async fn stats(&self) -> Result<StatsResponse> {
        Ok(Service::stats(&self.0))
    }
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        Service::exec(&self.0, request)
    }
    async fn upload(&self, sandbox_id: &str, guest_dir: &str, tar: &Path) -> Result<()> {
        Service::upload_path(&self.0, sandbox_id, guest_dir, tar).await
    }
    async fn download(&self, sandbox_id: &str, guest_path: &str, dest: &Path) -> Result<()> {
        Service::download_path(&self.0, sandbox_id, guest_path, dest).await
    }
    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        Service::snapshot(&self.0, request).await
    }
    async fn stop(&self, sandbox_id: &str) -> Result<()> {
        Service::stop(&self.0, sandbox_id).await
    }
    async fn start(&self, sandbox_id: &str, forkable: bool) -> Result<()> {
        Service::start(&self.0, sandbox_id, forkable).await
    }
    async fn pause(&self, sandbox_id: &str) -> Result<()> {
        Service::pause(&self.0, sandbox_id).await
    }
    async fn resume(&self, sandbox_id: &str) -> Result<()> {
        Service::resume(&self.0, sandbox_id).await
    }
}

const SANDBOXES: &str = "preloop.sandbox.v1.Sandboxes";

/// Network client for the `Sandboxes` service.
#[derive(Clone)]
pub struct SandboxClient {
    inner: connect::Client,
}

impl SandboxClient {
    /// `base` like `http://127.0.0.1:9191`.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            inner: connect::Client::new(base, token),
        }
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
        self.call::<_, ListTemplatesResponse>("ListTemplates", &ListRequest {})
            .await
            .map(|r| r.templates)
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
        self.call::<_, ListPoolsResponse>("ListPools", &ListRequest {})
            .await
            .map(|r| r.pools)
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
    async fn get(&self, sandbox_id: &str) -> Result<Sandbox> {
        self.call(
            "Get",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
            },
        )
        .await
    }
    async fn list(&self) -> Result<Vec<Sandbox>> {
        self.call::<_, ListSandboxesResponse>("List", &ListRequest {})
            .await
            .map(|r| r.sandboxes)
    }
    async fn list_providers(&self) -> Result<Vec<Provider>> {
        self.call::<_, ListProvidersResponse>("ListProviders", &ListRequest {})
            .await
            .map(|r| r.providers)
    }
    async fn stats(&self) -> Result<StatsResponse> {
        self.call("Stats", &ListRequest {}).await
    }
    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        Ok(self
            .inner
            .server_stream::<_, ExecEvent>(&format!("{SANDBOXES}/Exec"), &request)
            .await?
            .boxed())
    }
    async fn upload(&self, sandbox_id: &str, guest_dir: &str, tar: &Path) -> Result<()> {
        let ticket: Ticket = self
            .call(
                "OpenStream",
                &OpenStreamRequest {
                    sandbox_id: sandbox_id.to_owned(),
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
    async fn download(&self, sandbox_id: &str, guest_path: &str, dest: &Path) -> Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let ticket: Ticket = self
            .call(
                "OpenStream",
                &OpenStreamRequest {
                    sandbox_id: sandbox_id.to_owned(),
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
    async fn stop(&self, sandbox_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Stop",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn start(&self, sandbox_id: &str, forkable: bool) -> Result<()> {
        self.call::<_, Empty>(
            "Start",
            &StartRequest {
                sandbox_id: sandbox_id.to_owned(),
                forkable,
            },
        )
        .await
        .map(|_| ())
    }
    async fn pause(&self, sandbox_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Pause",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
    async fn resume(&self, sandbox_id: &str) -> Result<()> {
        self.call::<_, Empty>(
            "Resume",
            &GetRequest {
                sandbox_id: sandbox_id.to_owned(),
            },
        )
        .await
        .map(|_| ())
    }
}
