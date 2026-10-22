//! The caller-side API, in-process or over the network.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::BoxStream;

use crate::api::*;
use crate::error::{Code, Error, Result};
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

/// Network client for the `Sandboxes` service.
///
/// Speaks ConnectRPC through the generated [`rpc::SandboxesClient`] (JSON
/// codec, same wire shape as before) and uses the reqwest client only for the
/// ticketed data plane, whose tar bodies are not RPC messages.
pub struct SandboxClient {
    inner: rpc::SandboxesClient<ClassifiedTransport<connectrpc::client::HttpClient>>,
    http: reqwest::Client,
    base: String,
}

/// The generated message types, re-exported for readability below.
use crate::generated::preloop::sandbox::v1 as rpc;

impl SandboxClient {
    /// `base` like `https://127.0.0.1:9191` (system trust store).
    pub fn new(base: impl Into<String>, token: Option<String>) -> Result<Self> {
        let base = base.into().trim_end_matches('/').to_owned();
        let tls = native_tls_config()?;
        Self::build(base, token, tls, reqwest::Client::new(), false)
    }

    /// `base` like `https://127.0.0.1:9191`, trusting only `ca_pem`.
    pub fn with_tls(
        base: impl Into<String>,
        token: Option<String>,
        ca_pem: &str,
        identity_pem: Option<&[u8]>,
    ) -> Result<Self> {
        let base = base.into().trim_end_matches('/').to_owned();
        let tls = crate::tls::client_config(ca_pem, identity_pem)?;
        let http = crate::tls::http_client(ca_pem, identity_pem)?;
        Self::build(base, token, tls, http, true)
    }

    /// `ca_configured` is `true` when the caller supplied an explicit
    /// service CA (`with_tls`), `false` when trusting the system store
    /// (`new`): it gates the TLS failure's re-enroll hint (see
    /// [`classify_transport`]), which only makes sense when a CA was ever
    /// configured to go stale.
    fn build(
        base: String,
        token: Option<String>,
        tls: Arc<rustls::ClientConfig>,
        http: reqwest::Client,
        ca_configured: bool,
    ) -> Result<Self> {
        let uri: axum::http::Uri = base
            .parse()
            .map_err(|e| Error::invalid_argument(format!("sandbox service URL {base:?}: {e}")))?;
        let mut config = connectrpc::client::ClientConfig::new(uri).json();
        if let Some(token) = token.as_deref() {
            let value = axum::http::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| Error::invalid_argument(format!("sandbox token: {e}")))?;
            config = config.with_default_header(axum::http::header::AUTHORIZATION, value);
        }
        let transport =
            ClassifiedTransport(connectrpc::client::HttpClient::with_tls(tls), ca_configured);
        Ok(Self {
            inner: rpc::SandboxesClient::new(transport, config),
            http,
            base,
        })
    }

    /// Base URL.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Underlying HTTP client (for data-plane tickets).
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    fn ticket_url(&self, ticket: &Ticket) -> String {
        if ticket.url.starts_with('/') {
            format!("{}{}", self.base, ticket.url)
        } else {
            ticket.url.clone()
        }
    }
}

/// Map a generated-client transport failure through the TLS identity checks.
///
/// A stale CA is an identity problem, not a network blip: an agent whose
/// `ca.pem` predates a service re-key must re-enroll instead of retrying (and
/// reporting `HTTPS request failed` forever). The built-in transport keeps
/// the hyper/rustls failure as the `ConnectError` source, so the certificate
/// cause is still reachable here.
///
/// `ca_configured` is `false` for [`SandboxClient::new`] (system trust store
/// only): without an explicit CA there is nothing that could have gone
/// stale, so the "re-enroll" hint would be actively misleading. The
/// certificate is still untrusted either way (the code stays
/// `Unauthenticated`); only the wording, and the client-certificate check
/// (which never applies without a configured identity), change.
pub(crate) fn classify_transport(error: Error, ca_configured: bool) -> Error {
    let Some(source) = error.source_arc() else {
        return error;
    };
    let hint = if crate::tls::is_untrusted_service_ca(source.as_ref()) {
        if ca_configured {
            "identity was issued by a different CA (the service certificate does not chain to \
             the stored ca.pem); re-enroll with `--join <service-url> --token <join-token>`"
        } else {
            "the service certificate is not trusted by the system root store; if this is a \
             private Sandbox Service, pass `--ca <ca.pem>` (or enroll one with `--join \
             <service-url> --token <join-token>`)"
        }
    } else if ca_configured && crate::tls::is_client_certificate_rejected(source.as_ref()) {
        "the service rejected the client certificate (the identity is unknown or was issued by \
         a different CA); re-enroll with `--join <service-url> --token <join-token>`"
    } else {
        return error;
    };
    let message = error.message.as_deref().unwrap_or("transport failure");
    Error::new(Code::Unauthenticated, format!("{message}: {hint}")).with_shared_source(source)
}

/// A generated transport that classifies TLS identity failures.
///
/// The second field is `ca_configured` (see [`classify_transport`]).
#[derive(Clone)]
pub(crate) struct ClassifiedTransport<T>(pub(crate) T, pub(crate) bool);

impl<T: connectrpc::client::ClientTransport<Error = Error>> connectrpc::client::ClientTransport
    for ClassifiedTransport<T>
{
    type ResponseBody = T::ResponseBody;
    type Error = Error;

    fn send(
        &self,
        request: axum::http::Request<connectrpc::client::ClientBody>,
    ) -> connectrpc::client::BoxFuture<
        'static,
        std::result::Result<axum::http::Response<Self::ResponseBody>, Self::Error>,
    > {
        let inner = self.0.clone();
        let ca_configured = self.1;
        Box::pin(async move {
            inner
                .send(request)
                .await
                .map_err(|e| classify_transport(e, ca_configured))
        })
    }
}

/// A rustls configuration trusting the system roots.
fn native_tls_config() -> Result<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();
    if !loaded.errors.is_empty() {
        tracing::warn!(
            errors = ?loaded.errors,
            "failed to load some native root certificates"
        );
    }
    if loaded.certs.is_empty() {
        return Err(Error::internal(format!(
            "no usable system root certificates: {:?}",
            loaded.errors
        )));
    }
    roots.add_parsable_certificates(loaded.certs);
    Ok(Arc::new(
        crate::tls::client_config_builder()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}

/// Map a ticketed data-plane HTTP failure (`upload`/`download`) to a
/// Connect code. The data plane is plain HTTP, not a Connect RPC, so its
/// errors used to all collapse to `unavailable` ("try again"); 401/403/404
/// and 413 instead reflect the request itself, not transient unavailability
/// (B10).
fn data_plane_error(status: reqwest::StatusCode, body: &[u8]) -> Error {
    let message = String::from_utf8_lossy(body).into_owned();
    match status {
        reqwest::StatusCode::UNAUTHORIZED => Error::unauthenticated(message),
        reqwest::StatusCode::FORBIDDEN => Error::permission_denied(message),
        reqwest::StatusCode::NOT_FOUND => Error::not_found(message),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE => Error::resource_exhausted(message),
        _ => Error::unavailable(message),
    }
}

#[async_trait]
impl SandboxApi for SandboxClient {
    async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        Ok(self
            .inner
            .put_template(PutTemplateRequest {
                template: spec.into(),
                ..PutTemplateRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn list_templates(&self) -> Result<Vec<Template>> {
        let mut templates = Vec::new();
        let mut page_token = String::new();
        loop {
            let page = self
                .inner
                .list_templates(ListRequest {
                    page_size: crate::service::MAX_PAGE_SIZE,
                    page_token,
                    ..ListRequest::default()
                })
                .await?
                .into_owned();
            templates.extend(page.templates);
            if page.next_page_token.is_empty() {
                return Ok(templates);
            }
            page_token = page.next_page_token;
        }
    }

    async fn delete_template(&self, name: &str) -> Result<()> {
        self.inner
            .delete_template(DeleteTemplateRequest {
                name: name.to_owned(),
                ..DeleteTemplateRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        Ok(self
            .inner
            .put_pool(PutPoolRequest {
                pool: pool.into(),
                ..PutPoolRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn list_pools(&self) -> Result<Vec<PoolSpec>> {
        let mut pools = Vec::new();
        let mut page_token = String::new();
        loop {
            let page = self
                .inner
                .list_pools(ListRequest {
                    page_size: crate::service::MAX_PAGE_SIZE,
                    page_token,
                    ..ListRequest::default()
                })
                .await?
                .into_owned();
            pools.extend(page.pools);
            if page.next_page_token.is_empty() {
                return Ok(pools);
            }
            page_token = page.next_page_token;
        }
    }

    async fn delete_pool(&self, name: &str) -> Result<()> {
        self.inner
            .delete_pool(DeletePoolRequest {
                name: name.to_owned(),
                ..DeletePoolRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn acquire(&self, request: AcquireRequest) -> Result<AcquireResponse> {
        Ok(self.inner.acquire(request).await?.into_owned())
    }

    async fn renew(&self, request: RenewRequest) -> Result<Lease> {
        Ok(self.inner.renew(request).await?.into_owned())
    }

    async fn release(&self, request: ReleaseRequest) -> Result<()> {
        self.inner.release(request).await?;
        Ok(())
    }

    async fn get(&self, sandbox_id: &str, lease_id: &str) -> Result<Sandbox> {
        Ok(self
            .inner
            .get(GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                ..GetRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn list(&self) -> Result<Vec<Sandbox>> {
        let mut sandboxes = Vec::new();
        let mut page_token = String::new();
        loop {
            let page = self
                .inner
                .list(ListRequest {
                    page_size: crate::service::MAX_PAGE_SIZE,
                    page_token,
                    ..ListRequest::default()
                })
                .await?
                .into_owned();
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
            let page = self
                .inner
                .list_providers(ListRequest {
                    page_size: crate::service::MAX_PAGE_SIZE,
                    page_token,
                    ..ListRequest::default()
                })
                .await?
                .into_owned();
            providers.extend(page.providers);
            if page.next_page_token.is_empty() {
                return Ok(providers);
            }
            page_token = page.next_page_token;
        }
    }

    async fn cordon_provider(&self, name: &str) -> Result<Provider> {
        Ok(self
            .inner
            .cordon_provider(ProviderControlRequest {
                name: name.to_owned(),
                ..ProviderControlRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn uncordon_provider(&self, name: &str) -> Result<Provider> {
        Ok(self
            .inner
            .uncordon_provider(ProviderControlRequest {
                name: name.to_owned(),
                ..ProviderControlRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn drain_provider(&self, name: &str) -> Result<DrainProviderResult> {
        Ok(self
            .inner
            .drain_provider(ProviderControlRequest {
                name: name.to_owned(),
                ..ProviderControlRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn stats(&self) -> Result<StatsResponse> {
        Ok(self.inner.stats(ListRequest::default()).await?.into_owned())
    }

    async fn exec(&self, request: ExecRequest) -> Result<BoxStream<'static, Result<ExecEvent>>> {
        let stream = self.inner.exec(request).await?;
        Ok(
            futures::stream::unfold((stream, false), |(mut stream, done)| async move {
                if done {
                    return None;
                }
                match stream.message::<ExecEvent>().await {
                    Ok(Some(message)) => Some((Ok(message.to_owned_message()), (stream, false))),
                    Ok(None) => None,
                    Err(error) => Some((Err(error), (stream, true))),
                }
            })
            .boxed(),
        )
    }

    async fn upload(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        guest_dir: &str,
        tar: &Path,
    ) -> Result<()> {
        let ticket = self
            .inner
            .open_stream(OpenStreamRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                kind: StreamKind::Upload.into(),
                path: guest_dir.to_owned(),
                ..OpenStreamRequest::default()
            })
            .await?
            .into_owned();
        let file = tokio::fs::File::open(tar)
            .await
            .map_err(|e| Error::internal(format!("upload: {e}")))?;
        let length = file.metadata().await.map(|m| m.len()).unwrap_or_default();
        let response = self
            .http
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
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            return Err(data_plane_error(status, &body));
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
        let ticket = self
            .inner
            .open_stream(OpenStreamRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                kind: StreamKind::Download.into(),
                path: guest_path.to_owned(),
                ..OpenStreamRequest::default()
            })
            .await?
            .into_owned();
        let response = self
            .http
            .get(self.ticket_url(&ticket))
            .bearer_auth(&ticket.token)
            .send()
            .await
            .map_err(|e| Error::unavailable(format!("download: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await.unwrap_or_default();
            return Err(data_plane_error(status, &body));
        }
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|e| Error::internal(format!("download: {e}")))?;
        let result: Result<()> = async {
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
        .await;
        if result.is_err() {
            // Do not leave a truncated tar behind for the caller to mistake
            // for a complete download (B10).
            drop(file);
            let _ = tokio::fs::remove_file(dest).await;
        }
        result
    }

    async fn snapshot(&self, request: SnapshotRequest) -> Result<Template> {
        Ok(self.inner.snapshot(request).await?.into_owned())
    }

    async fn stop(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.inner
            .stop(GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                ..GetRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn start(&self, sandbox_id: &str, lease_id: &str, forkable: bool) -> Result<()> {
        self.inner
            .start(StartRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                forkable,
                ..StartRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn pause(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.inner
            .pause(GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                ..GetRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn resume(&self, sandbox_id: &str, lease_id: &str) -> Result<()> {
        self.inner
            .resume(GetRequest {
                sandbox_id: sandbox_id.to_owned(),
                lease_id: lease_id.to_owned(),
                ..GetRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn create_join_token(&self, ttl_secs: u64) -> Result<CreateJoinTokenResponse> {
        Ok(self
            .inner
            .create_join_token(CreateJoinTokenRequest {
                ttl_secs,
                ..CreateJoinTokenRequest::default()
            })
            .await?
            .into_owned())
    }

    async fn delete_provider(&self, name: &str) -> Result<()> {
        self.inner
            .delete_provider(DeleteProviderRequest {
                name: name.to_owned(),
                ..DeleteProviderRequest::default()
            })
            .await?;
        Ok(())
    }
}
