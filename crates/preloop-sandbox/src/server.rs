//! HTTP surface: the generated ConnectRPC services plus the ticketed data
//! plane, served on separate caller and agent listeners.
//!
//! TLS is used by default. Callers hold the one client bearer token; capacity
//! owners authenticate with a client certificate issued by the service CA
//! (see [`crate::tls`]), except for `Enroll` which uses a single-use join token.
//!
//! The generated RPC routes ([`crate::generated`]) run under an axum fallback
//! so the ticketed data plane — plain `GET`/`PUT` routes that stream tar
//! bodies — shares the listener and the connection-scoped [`PeerIdentity`].
//! The generated dispatch hands each handler a [`connectrpc::RequestContext`]
//! whose extensions are the request's own: `ConnectInfo<PeerIdentity>`
//! inserted by axum's listener is how `Providers` sees the client
//! certificate.
//!
//! [`ListenerSurface`] decides which RPC service and which data-plane routes a
//! listener exposes: the caller listener never serves provider RPCs or agent
//! blob routes, and the agent listener never serves caller RPCs or caller
//! streams.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use connectrpc::{
    Encodable, RequestContext, Response as RpcResponse, ServiceRequest, ServiceResult,
    ServiceStream,
};
use futures::StreamExt as _;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::api::*;
use crate::driver::TransferChunk;
use crate::error::{Error, Result};
use crate::generated::preloop::sandbox::v1::{
    Empty, Providers, ProvidersExt, Sandboxes, SandboxesExt,
};
use crate::service::{ProviderIdentity, Service};
use crate::tls::{PeerIdentity, ServerTls, TlsListener};

/// Maximum request body for one Connect RPC.
pub const CONNECT_BODY_LIMIT: usize = 4 * 1024 * 1024;

/// Which part of the Sandbox Service a listener exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerSurface {
    /// Caller RPCs and caller data-plane streams.
    Client,
    /// Provider RPCs, enrollment, CA retrieval and provider blobs.
    Agent,
    /// The compatibility mode where one listener exposes both surfaces.
    Combined,
}

/// Host names accepted by the client listener's DNS-rebinding guard.
#[derive(Debug, Clone, Default)]
pub struct HostAllowlist {
    bound: Option<IpAddr>,
    patterns: Vec<HostPattern>,
    enforced: bool,
}

#[derive(Debug, Clone)]
enum HostPattern {
    Exact(String),
    Wildcard(String),
}

impl HostAllowlist {
    /// Disable Host checking.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Build a policy from the bound address and comma-separated setting.
    ///
    /// The setting is considered explicit when `raw` is `Some`, including an
    /// empty value. Loopback listeners are guarded even without the setting.
    pub fn from_config(
        bound: SocketAddr,
        raw: Option<&str>,
        extra_exact: impl IntoIterator<Item = String>,
    ) -> std::result::Result<Self, String> {
        let mut patterns = Vec::new();
        if let Some(raw) = raw {
            for value in raw.split(',').map(str::trim).filter(|v| !v.is_empty()) {
                patterns.push(parse_host_pattern(value)?);
            }
        }
        for value in extra_exact {
            patterns.push(HostPattern::Exact(normalize_host(&value)?));
        }
        Ok(Self {
            bound: (!bound.ip().is_unspecified()).then_some(bound.ip()),
            patterns,
            enforced: raw.is_some() || bound.ip().is_loopback(),
        })
    }

    /// Whether this policy accepts a Host header value.
    pub fn accepts(&self, value: &str) -> bool {
        let Some(host) = host_without_port(value.trim()).map(normalize_host_lossy) else {
            return false;
        };
        if host == "localhost"
            || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
            || self
                .bound
                .is_some_and(|ip| host.parse::<IpAddr>() == Ok(ip))
        {
            return true;
        }
        self.patterns.iter().any(|pattern| match pattern {
            HostPattern::Exact(expected) => host == *expected,
            HostPattern::Wildcard(suffix) => host
                .strip_suffix(suffix)
                .is_some_and(|prefix| !prefix.is_empty() && !prefix.contains('.')),
        })
    }

    /// Whether this policy should be applied to requests.
    pub fn enforced(&self) -> bool {
        self.enforced
    }
}

fn normalize_host(value: &str) -> std::result::Result<String, String> {
    let host = host_without_port(value.trim())
        .ok_or_else(|| format!("invalid host in PRELOOP_SANDBOX_ALLOWED_HOSTS: `{value}`"))?
        .trim_end_matches('.');
    if host.is_empty()
        || host == "*"
        || host.contains('*')
        || host.contains('/')
        || host.contains('\\')
        || host.contains(' ')
        || host.parse::<IpAddr>().is_err() && host.contains(':')
    {
        return Err(format!(
            "invalid host in PRELOOP_SANDBOX_ALLOWED_HOSTS: `{value}`"
        ));
    }
    Ok(host.to_ascii_lowercase())
}

fn normalize_host_lossy(value: &str) -> String {
    value.trim_end_matches('.').to_ascii_lowercase()
}

fn parse_host_pattern(value: &str) -> std::result::Result<HostPattern, String> {
    if let Some(suffix) = value.strip_prefix("*.") {
        let suffix = normalize_host(suffix)?;
        if suffix.is_empty() {
            return Err(format!(
                "invalid host in PRELOOP_SANDBOX_ALLOWED_HOSTS: `{value}`"
            ));
        }
        Ok(HostPattern::Wildcard(format!(".{suffix}")))
    } else {
        Ok(HostPattern::Exact(normalize_host(value)?))
    }
}

/// Bearer token for `Sandboxes` callers plus the optional loopback Host guard.
///
/// The token is always required: the listener speaks TLS even on loopback.
#[derive(Debug, Clone)]
pub struct Auth {
    /// Token callers present as `Authorization: Bearer …`.
    pub client_token: String,
    /// Reject requests whose `Host` is not loopback. Set when the listener is
    /// bound to a loopback address, so a DNS-rebound page cannot reach it
    /// through a name that resolves to `127.0.0.1`.
    pub loopback_only: bool,
}

impl Auth {
    /// Build from a token, without a Host guard.
    pub fn new(client_token: impl Into<String>) -> Self {
        Self {
            client_token: client_token.into(),
            loopback_only: false,
        }
    }

    /// A token plus the Host guard a listener bound to `addr` needs.
    pub fn for_listener(client_token: impl Into<String>, addr: std::net::SocketAddr) -> Self {
        Self {
            client_token: client_token.into(),
            loopback_only: addr.ip().is_loopback(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    service: Arc<Service>,
    auth: Auth,
    hosts: HostAllowlist,
}
fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// The host part of a `Host` header, without its port.
fn host_without_port(value: &str) -> Option<&str> {
    if let Some(rest) = value.strip_prefix('[') {
        // `[::1]` or `[::1]:9191`.
        let (host, tail) = rest.split_once(']')?;
        if !tail.is_empty() && !tail.starts_with(':') {
            return None;
        }
        return Some(host);
    }
    match value.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => Some(host),
        _ => Some(value),
    }
}

/// Accept only names that reach a listener on this machine: `localhost`, any
/// loopback address, the bound address and configured names.
fn loopback_host(value: &str) -> bool {
    let Some(host) = host_without_port(value.trim()) else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

fn check_host(
    policy: &HostAllowlist,
    legacy_loopback_only: bool,
    headers: &HeaderMap,
) -> Result<(), Error> {
    if !policy.enforced() && !legacy_loopback_only {
        return Ok(());
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Error::new(Code::PermissionDenied, "Host header is required"))?;
    let accepted = if policy.enforced() {
        policy.accepts(host)
    } else {
        loopback_host(host)
    };
    if !accepted {
        let setting = if policy.enforced() {
            "PRELOOP_SANDBOX_ALLOWED_HOSTS"
        } else {
            "the loopback Host policy"
        };
        return Err(Error::new(
            Code::PermissionDenied,
            format!("Host `{host}` is not allowed; change {setting}"),
        ));
    }
    Ok(())
}

/// Constant-time equality, so a token cannot be probed byte by byte.
fn token_eq(candidate: &str, expected: &str) -> bool {
    candidate.len() == expected.len()
        && candidate
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

/// `Sandboxes` handler: bearer token, then the service.
struct SandboxesService {
    service: Arc<Service>,
    auth: Auth,
    hosts: HostAllowlist,
}

impl SandboxesService {
    fn authorize(&self, ctx: &RequestContext) -> Result<()> {
        check_host(&self.hosts, self.auth.loopback_only, ctx.headers())?;
        let token = ctx
            .header(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        match token {
            Some(token) if token_eq(token, &self.auth.client_token) => Ok(()),
            _ => Err(Error::unauthenticated("missing or bad bearer token")),
        }
    }
}

/// `Providers` handler: certificate identity, then the service.
struct ProvidersService {
    service: Arc<Service>,
    /// Reject RPCs whose `Host` is not loopback; see [`Auth::loopback_only`].
    loopback_only: bool,
    hosts: HostAllowlist,
}

impl ProvidersService {
    /// The enrolled provider the request's client certificate authenticates,
    /// plus the fingerprint it was resolved from.
    fn identity(&self, ctx: &RequestContext) -> Result<(ProviderIdentity, String)> {
        check_host(&self.hosts, self.loopback_only, ctx.headers())?;
        let peer = ctx
            .extensions()
            .get::<ConnectInfo<PeerIdentity>>()
            .map(|info| &info.0)
            .ok_or_else(|| Error::unauthenticated("a client certificate is required"))?;
        let fingerprint = peer.fingerprint()?.to_owned();
        let identity = self.service.provider_identity(&fingerprint)?;
        Ok((identity, fingerprint))
    }

    fn bound(&self, ctx: &RequestContext, provider_id: &str) -> Result<()> {
        let (identity, _) = self.identity(ctx)?;
        require_identity(&identity, provider_id)
    }
}

fn require_identity(identity: &ProviderIdentity, provider_id: &str) -> Result<()> {
    if provider_id != identity.provider_id {
        return Err(Error::permission_denied(
            "provider id does not match the client certificate",
        ));
    }
    Ok(())
}

// The response future borrows neither the request body nor `self` beyond
// the call, so the concrete type captures less than the trait's
// `use<'a, Self>` allows. That narrowing is deliberate and local.
#[allow(refining_impl_trait)]
impl Sandboxes for SandboxesService {
    fn put_template<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, PutTemplateRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Template> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(
                self.service
                    .put_template(request.template.into_option().unwrap_or_default())
                    .await?,
            )
        }
    }

    fn list_templates<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<ListTemplatesResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.list_templates_page(&request)?)
        }
    }

    fn delete_template<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteTemplateRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            self.service.delete_template(&request.name).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn put_pool<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, PutPoolRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<PoolSpec> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(
                self.service
                    .put_pool(request.pool.into_option().unwrap_or_default())
                    .await?,
            )
        }
    }

    fn list_pools<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<ListPoolsResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.list_pools_page(&request)?)
        }
    }

    fn delete_pool<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeletePoolRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            self.service.delete_pool(&request.name).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn acquire<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, AcquireRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<AcquireResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.acquire(request).await?)
        }
    }

    fn renew<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RenewRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Lease> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.renew(request).await?)
        }
    }

    fn release<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ReleaseRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            self.service.release(request).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn get<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Sandbox> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let request = request;
            RpcResponse::ok(self.service.get(&request.sandbox_id, &request.lease_id)?)
        }
    }

    fn list<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<ListSandboxesResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.list_page(&request)?)
        }
    }

    fn exec(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ExecRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<ServiceStream<impl Encodable<ExecEvent> + Send + use<>>>,
    > + Send
    + use<> {
        let request = request.to_owned_message();
        // Authorize before the future is built: the stream is `'static`, so
        // the response borrows neither the request nor `self`.
        let allowed = self.authorize(&ctx);
        let service = self.service.clone();
        async move {
            allowed?;
            Ok(RpcResponse::stream(service.exec(request)?))
        }
    }

    fn open_stream<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, OpenStreamRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Ticket> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.open_stream(request)?)
        }
    }

    fn snapshot<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, SnapshotRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Template> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.snapshot(request).await?)
        }
    }

    fn stop<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let request = request;
            self.service
                .stop(&request.sandbox_id, &request.lease_id)
                .await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn start<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, StartRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let request = request;
            self.service
                .start(&request.sandbox_id, &request.lease_id, request.forkable)
                .await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn pause<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let request = request;
            self.service
                .pause(&request.sandbox_id, &request.lease_id)
                .await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn resume<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let request = request;
            self.service
                .resume(&request.sandbox_id, &request.lease_id)
                .await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn list_providers<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<ListProvidersResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.list_providers_page(&request)?)
        }
    }

    fn cordon_provider<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ProviderControlRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Provider> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(
                self.service
                    .set_provider_cordon(&request.name, true)
                    .await?,
            )
        }
    }

    fn uncordon_provider<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ProviderControlRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Provider> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(
                self.service
                    .set_provider_cordon(&request.name, false)
                    .await?,
            )
        }
    }

    fn drain_provider<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ProviderControlRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<DrainProviderResult> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.drain_provider(&request.name).await?)
        }
    }

    fn stats<'a>(
        &'a self,
        ctx: RequestContext,
        _request: ServiceRequest<'_, ListRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<StatsResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        async move {
            self.authorize(&ctx)?;
            RpcResponse::ok(self.service.stats())
        }
    }

    fn delete_provider<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteProviderRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            self.service.delete_provider(&request.name).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn create_join_token<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateJoinTokenRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<CreateJoinTokenResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            self.authorize(&ctx)?;
            let ttl = std::time::Duration::from_secs(request.ttl_secs);
            RpcResponse::ok(
                self.service
                    .create_join_token(ttl, &request.provider_name)
                    .await?,
            )
        }
    }
}

#[allow(refining_impl_trait)]
impl Providers for ProvidersService {
    fn enroll<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, EnrollRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<EnrollResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        let loopback_only = self.loopback_only;
        async move {
            // `Enroll` is authenticated by its join token alone; the Host
            // guard still applies.
            check_host(loopback_only, ctx.headers())?;
            RpcResponse::ok(self.service.enroll(request).await?)
        }
    }

    fn register<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RegisterRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<RegisterResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let (identity, _) = self.identity(&ctx)?;
            RpcResponse::ok(
                self.service
                    .register_enrolled(request, &identity.provider_id)
                    .await?,
            )
        }
    }

    fn heartbeat<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, HeartbeatRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let request = request;
            self.bound(&ctx, &request.provider_id)?;
            self.service.heartbeat(request)?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn poll<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, PollRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<PollResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let request = request;
            self.bound(&ctx, &request.provider_id)?;
            RpcResponse::ok(self.service.poll(request).await?)
        }
    }

    fn report<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ReportRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let request = request;
            self.bound(&ctx, &request.provider_id)?;
            self.service.report(request).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn report_exec<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ReportExecRequest>,
    ) -> impl std::future::Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>>
    + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let request = request;
            self.bound(&ctx, &request.provider_id)?;
            self.service.report_exec(request).await?;
            RpcResponse::ok(Empty::default())
        }
    }

    fn reconcile<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ReconcileRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<ReconcileResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let request = request;
            self.bound(&ctx, &request.provider_id)?;
            RpcResponse::ok(self.service.reconcile(request).await?)
        }
    }

    fn renew_certificate<'a>(
        &'a self,
        ctx: RequestContext,
        request: ServiceRequest<'_, RenewCertificateRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<RenewCertificateResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            let (_, fingerprint) = self.identity(&ctx)?;
            RpcResponse::ok(
                self.service
                    .renew_certificate(request, &fingerprint)
                    .await?,
            )
        }
    }
}

#[derive(Clone)]
struct AppState {
    service: Arc<Service>,
    auth: Auth,
}

/// Build the compatibility router exposing both surfaces.
///
/// This is the single-listener deployment mode: caller and agent routes share
/// one router, as they did before the listener split.
pub fn router(service: Arc<Service>, auth: Auth) -> Router {
    router_with_options(
        service,
        auth,
        ListenerSurface::Combined,
        HostAllowlist::disabled(),
    )
}

/// Build a router for one listener surface.
///
/// The ConnectRPC services own the fallback; the data-plane routes are plain
/// axum routes on the same listener. `Providers` reads the peer certificate
/// from `ConnectInfo<PeerIdentity>`, so serve it with [`serve_with_options`]
/// (or `Router::into_make_service_with_connect_info`).
pub fn router_with_options(
    service: Arc<Service>,
    auth: Auth,
    surface: ListenerSurface,
    hosts: HostAllowlist,
) -> Router {
    let mut rpc = connectrpc::Router::new();
    let mut router = Router::new();
    if matches!(surface, ListenerSurface::Client | ListenerSurface::Combined) {
        let sandboxes = Arc::new(SandboxesService {
            service: service.clone(),
            auth: auth.clone(),
            hosts: hosts.clone(),
        });
        rpc = sandboxes.register(rpc);
        router = router
            .route(
                "/sandbox/v1/streams/{ticket}",
                get(stream_download).put(stream_upload),
            )
            .route("/sandbox/v1/healthz", get(|| async { "ok" }));
    }
    if matches!(surface, ListenerSurface::Agent | ListenerSurface::Combined) {
        let providers = Arc::new(ProvidersService {
            service: service.clone(),
            loopback_only: auth.loopback_only,
            hosts: hosts.clone(),
        });
        rpc = providers.register(rpc);
        router = router
            .route(
                "/sandbox/v1/agent/blobs/{ticket}",
                get(agent_blob_get).put(agent_blob_put),
            )
            .route("/sandbox/v1/ca.pem", get(ca_pem));
    }
    // Bulk bytes travel over the ticketed data plane, never in RPCs. The
    // buffering middleware bounds every unary request (and the single Exec
    // envelope), and the data-plane routes below stream their own bodies.
    let rpc = axum::Router::new()
        .fallback_service(rpc.into_axum_service())
        .layer(middleware::from_fn(cap_rpc_body));
    router.fallback_service(rpc).with_state(AppState {
        service,
        auth,
        hosts,
    })
}

/// Build a router for one listener surface.
pub fn router_with_options(
    service: Arc<Service>,
    auth: Auth,
    surface: ListenerSurface,
    hosts: HostAllowlist,
) -> Router {
    let mut router = Router::new();
    if matches!(surface, ListenerSurface::Client | ListenerSurface::Combined) {
        router = router
            .route("/preloop.sandbox.v1.Sandboxes/{method}", post(sandboxes))
            .route(
                "/sandbox/v1/streams/{ticket}",
                get(stream_download).put(stream_upload),
            )
            .route("/sandbox/v1/healthz", get(|| async { "ok" }));
    }
    if matches!(surface, ListenerSurface::Agent | ListenerSurface::Combined) {
        router = router
            .route("/preloop.sandbox.v1.Providers/{method}", post(providers))
            .route(
                "/sandbox/v1/agent/blobs/{ticket}",
                get(agent_blob_get).put(agent_blob_put),
            )
            .route("/sandbox/v1/ca.pem", get(ca_pem));
    }
    router.with_state(AppState {
        service,
        auth,
        hosts,
    })
}

/// Serve the compatibility API over TLS until `shutdown`.
pub async fn serve(
    listener: tokio::net::TcpListener,
    service: Arc<Service>,
    auth: Auth,
    tls: Arc<ServerTls>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    serve_with_options(
        listener,
        service,
        auth,
        Some(tls),
        ListenerSurface::Combined,
        HostAllowlist::disabled(),
        shutdown,
    )
    .await
}

/// Serve one listener surface over TLS or plain HTTP.
pub async fn serve_with_options(
    listener: tokio::net::TcpListener,
    service: Arc<Service>,
    auth: Auth,
    tls: Option<Arc<ServerTls>>,
    surface: ListenerSurface,
    hosts: HostAllowlist,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let app = router_with_options(service, auth, surface, hosts);
    if let Some(tls) = tls {
        axum::serve(
            TlsListener::new(listener, tls.acceptor.clone()),
            app.into_make_service_with_connect_info::<PeerIdentity>(),
        )
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
    } else {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<PeerIdentity>(),
        )
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
    }
}

/// The service CA, for agents that pin it before enrolling.
async fn ca_pem(State(app): State<AppState>) -> Response {
    match app.service.ca_pem() {
        Some(pem) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-pem-file")],
            pem,
        )
            .into_response(),
        None => Error::not_found("this service has no CA").into_response(),
    }
}

/// A rejected upload body: the cap was exceeded or the transfer failed.
enum BodyWriteError {
    TooLarge(u64),
    Failed(Error),
}

impl From<Error> for BodyWriteError {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

impl IntoResponse for BodyWriteError {
    fn into_response(self) -> Response {
        match self {
            Self::TooLarge(max_bytes) => payload_too_large(max_bytes),
            Self::Failed(error) => error.into_response(),
        }
    }
}

fn payload_too_large(max_bytes: u64) -> Response {
    let error = Error::out_of_range(format!("blob exceeds the {max_bytes} byte limit"));
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        error.to_json(),
    )
        .into_response()
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// Pump a request body into a transfer's producer channel.
///
/// The cap is enforced here, on the bytes actually received, so a lying
/// `Content-Length` cannot overrun it. A body error or a vanished consumer
/// sends `Failed`, which the peer turns into a failed extraction instead of a
/// plausible-looking truncated archive.
async fn pump_upload(
    body: Body,
    tx: mpsc::Sender<TransferChunk>,
    max_bytes: u64,
) -> std::result::Result<(), BodyWriteError> {
    let mut stream = body.into_data_stream();
    let mut written = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                let reason = format!("upload: {error}");
                let _ = tx.send(TransferChunk::Failed(reason.clone())).await;
                return Err(Error::unavailable(reason).into());
            }
        };
        written = written.saturating_add(chunk.len() as u64);
        if written > max_bytes {
            let _ = tx
                .send(TransferChunk::Failed("blob exceeds the size limit".into()))
                .await;
            return Err(BodyWriteError::TooLarge(max_bytes));
        }
        if tx.send(TransferChunk::Data(chunk.to_vec())).await.is_err() {
            return Err(Error::unavailable("transfer consumer went away").into());
        }
    }
    drop(tx);
    Ok(())
}

/// Remove a finished ticket when the response body (or the handler's stream
/// state) is dropped, including when the caller disconnects mid-download.
struct TicketGuard {
    service: Arc<Service>,
    ticket: String,
}

impl Drop for TicketGuard {
    fn drop(&mut self) {
        self.service.remove_ticket(&self.ticket);
    }
}

fn chunk_item(chunk: TransferChunk) -> std::result::Result<Vec<u8>, std::io::Error> {
    match chunk {
        TransferChunk::Data(bytes) => Ok(bytes),
        TransferChunk::Failed(reason) => Err(std::io::Error::other(reason)),
    }
}

/// A response body that streams a transfer's consumer side.
fn transfer_body(
    rx: mpsc::Receiver<TransferChunk>,
    first: Option<TransferChunk>,
    guard: Option<TicketGuard>,
) -> Body {
    Body::from_stream(futures::stream::unfold(
        (rx, first, guard),
        |(mut rx, first, guard)| async move {
            if let Some(chunk) = first {
                return Some((chunk_item(chunk), (rx, None, guard)));
            }
            rx.recv()
                .await
                .map(|chunk| (chunk_item(chunk), (rx, None, guard)))
        },
    ))
}

/// Take the first transfer item, bounded so an agent that claimed but never
/// sends cannot hold the caller's headers open forever.
async fn first_item(
    service: &Service,
    rx: &mut mpsc::Receiver<TransferChunk>,
) -> Result<TransferChunk> {
    match tokio::time::timeout(service.config().work_timeout, rx.recv()).await {
        Ok(Some(chunk)) => Ok(chunk),
        Ok(None) => Err(Error::internal("transfer produced no bytes")),
        Err(_) => Err(Error::unavailable("transfer did not start")),
    }
}

async fn stream_upload(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let token = bearer(&headers).unwrap_or_default();
    let max_bytes = app.service.config().blob_max_bytes;
    if let Err(error) = check_host(&app.hosts, app.auth.loopback_only, &headers) {
        return error.into_response();
    }
    let mut claim =
        match app
            .service
            .claim_caller(&ticket, token, crate::service::TransferSide::Producer)
        {
            Ok(claim) => claim,
            Err(error) => return error.into_response(),
        };
    if content_length(&headers).is_some_and(|length| length > max_bytes) {
        app.service
            .abort_ticket(&ticket, "blob exceeds the size limit")
            .await;
        return payload_too_large(max_bytes);
    }
    let reply = match app
        .service
        .start_copy(&claim.sandbox_id, &ticket, &claim.guest_path, claim.kind)
        .await
    {
        Ok(reply) => reply,
        Err(error) => {
            app.service
                .abort_ticket(&ticket, crate::error::message(&error))
                .await;
            return error.into_response();
        }
    };
    if let Err(error) = app.service.wait_peer(&mut claim).await {
        app.service
            .abort_ticket(&ticket, crate::error::message(&error))
            .await;
        return error.into_response();
    }
    let Some(tx) = claim.take_producer() else {
        app.service.remove_ticket(&ticket);
        return Error::internal("upload ticket has no producer side").into_response();
    };
    match pump_upload(body, tx, max_bytes).await {
        Ok(()) => {
            // Close the consumer's stream first; the agent reports only after
            // its extraction sees EOF.
            app.service.finish_ticket(&ticket);
            let result = crate::service::await_copy(reply, app.service.config().work_timeout).await;
            app.service.remove_ticket(&ticket);
            match result {
                Ok(()) => StatusCode::NO_CONTENT.into_response(),
                Err(error) => error.into_response(),
            }
        }
        Err(error) => {
            app.service
                .abort_ticket(&ticket, "upload did not complete")
                .await;
            error.into_response()
        }
    }
}

async fn stream_download(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    headers: HeaderMap,
) -> Response {
    let token = bearer(&headers).unwrap_or_default();
    if let Err(error) = check_host(&app.hosts, app.auth.loopback_only, &headers) {
        return error.into_response();
    }
    let mut claim =
        match app
            .service
            .claim_caller(&ticket, token, crate::service::TransferSide::Consumer)
        {
            Ok(claim) => claim,
            Err(error) => return error.into_response(),
        };
    if let Err(error) = app
        .service
        .start_copy(&claim.sandbox_id, &ticket, &claim.guest_path, claim.kind)
        .await
    {
        app.service
            .abort_ticket(&ticket, crate::error::message(&error))
            .await;
        return error.into_response();
    }
    if let Err(error) = app.service.wait_peer(&mut claim).await {
        app.service
            .abort_ticket(&ticket, crate::error::message(&error))
            .await;
        return error.into_response();
    }
    let Some(mut rx) = claim.take_consumer() else {
        app.service.remove_ticket(&ticket);
        return Error::internal("download ticket has no consumer side").into_response();
    };
    let first = match first_item(&app.service, &mut rx).await {
        Ok(TransferChunk::Data(bytes)) => TransferChunk::Data(bytes),
        Ok(TransferChunk::Failed(reason)) => {
            app.service.remove_ticket(&ticket);
            return Error::unavailable(reason).into_response();
        }
        Err(error) => {
            app.service
                .abort_ticket(&ticket, crate::error::message(&error))
                .await;
            return error.into_response();
        }
    };
    let guard = TicketGuard {
        service: app.service.clone(),
        ticket,
    };
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-tar"),
        )],
        transfer_body(rx, Some(first), Some(guard)),
    )
        .into_response()
}

fn provider_headers(headers: &HeaderMap) -> Result<(String, u64)> {
    let id = headers
        .get("x-provider-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::invalid_argument("x-provider-id required"))?;
    let generation = headers
        .get("x-provider-generation")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Error::invalid_argument("x-provider-generation required"))?;
    Ok((id.to_owned(), generation))
}

fn missing_client_certificate() -> Error {
    Error::new(
        Code::Unauthenticated,
        "no client certificate was presented; the agent listener must be exposed as raw TCP \
         behind a proxy or tunnel, not through TLS termination (see docs/sandbox-service.md#deployment)",
    )
}

/// The agent fetches an upload ticket's bytes (consumer side).
async fn agent_blob_get(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        check_host(&app.hosts, app.auth.loopback_only, &headers)?;
        let identity = app.service.provider_identity(
            peer.fingerprint()
                .map_err(|_| missing_client_certificate())?,
        )?;
        let (id, generation) = provider_headers(&headers)?;
        require_identity(&identity, &id)?;
        let mut claim = app.service.claim_agent(
            &id,
            generation,
            &ticket,
            crate::service::TransferSide::Consumer,
        )?;
        app.service.wait_peer(&mut claim).await?;
        let Some(mut rx) = claim.take_consumer() else {
            return Err(Error::internal("upload ticket has no consumer side"));
        };
        let first = first_item(&app.service, &mut rx).await?;
        if let TransferChunk::Failed(reason) = first {
            return Err(Error::unavailable(reason));
        }
        Ok((
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-tar"),
            )],
            // The caller owns this ticket's lifetime; the agent's response
            // body must not remove it.
            transfer_body(rx, Some(first), None),
        )
            .into_response())
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

/// The agent produces a download ticket's bytes (producer side).
async fn agent_blob_put(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let result = async {
        check_host(&app.hosts, app.auth.loopback_only, &headers)?;
        let identity = app.service.provider_identity(
            peer.fingerprint()
                .map_err(|_| missing_client_certificate())?,
        )?;
        let (id, generation) = provider_headers(&headers)?;
        require_identity(&identity, &id)?;
        app.service.claim_agent(
            &id,
            generation,
            &ticket,
            crate::service::TransferSide::Producer,
        )
    }
    .await;
    let mut claim = match result {
        Ok(claim) => claim,
        Err(error) => return error.into_response(),
    };
    let max_bytes = app.service.config().blob_max_bytes;
    if content_length(&headers).is_some_and(|length| length > max_bytes) {
        app.service
            .abort_ticket(&ticket, "blob exceeds the size limit")
            .await;
        return payload_too_large(max_bytes);
    }
    if let Err(error) = app.service.wait_peer(&mut claim).await {
        app.service
            .abort_ticket(&ticket, crate::error::message(&error))
            .await;
        return error.into_response();
    }
    let Some(tx) = claim.take_producer() else {
        app.service.remove_ticket(&ticket);
        return Error::internal("download ticket has no producer side").into_response();
    };
    match pump_upload(body, tx, max_bytes).await {
        Ok(()) => {
            app.service.finish_ticket(&ticket);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => {
            app.service
                .abort_ticket(&ticket, "download did not complete")
                .await;
            error.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts_accept_local_names_and_ports() {
        for host in [
            "localhost",
            "localhost:9191",
            "LOCALHOST:1",
            "127.0.0.1",
            "127.0.0.1:9191",
            "127.42.0.9",
            "::1",
            "[::1]",
            "[::1]:9191",
        ] {
            assert!(loopback_host(host), "{host} must be accepted");
        }
    }

    #[test]
    fn non_loopback_hosts_are_rejected() {
        for host in [
            "example.com",
            "example.com:9191",
            "10.0.0.1",
            "0.0.0.0",
            "192.168.1.1:80",
            "[2001:db8::1]:9191",
            "127.0.0.1.evil.test",
            "",
        ] {
            assert!(!loopback_host(host), "{host} must be rejected");
        }
    }

    #[test]
    fn allowlist_matches_exact_wildcard_ports_and_bound_address() {
        let policy = HostAllowlist::from_config(
            "192.0.2.10:9191".parse().unwrap(),
            Some("api.example.test,*.svc.example.test,[2001:db8::10]"),
            std::iter::empty(),
        )
        .unwrap();
        for host in [
            "API.EXAMPLE.TEST:9191",
            "worker.svc.example.test.",
            "192.0.2.10:1",
            "[2001:db8::10]:9191",
            "localhost.",
            "127.0.0.42",
            "[::1]:9191",
        ] {
            assert!(policy.accepts(host), "{host} must be accepted");
        }
        for host in [
            "evil.example.test",
            "worker.deep.svc.example.test",
            "api.example.test.evil",
        ] {
            assert!(!policy.accepts(host), "{host} must be rejected");
        }
        assert!(
            HostAllowlist::from_config(
                "127.0.0.1:9191".parse().unwrap(),
                Some("*"),
                std::iter::empty(),
            )
            .is_err()
        );
    }
}
