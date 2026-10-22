//! HTTP surface: the generated ConnectRPC services plus the ticketed data
//! plane.
//!
//! Everything is served over TLS. Callers hold the one client bearer token;
//! capacity owners authenticate with a client certificate issued by the
//! service CA (see [`crate::tls`]), except for `Enroll` which is authenticated
//! by a single-use join token.
//!
//! The RPC routes ([`crate::generated`]) run under an axum fallback so the
//! ticketed data plane — plain `GET`/`PUT` routes that stream tar bodies —
//! shares the listener and the connection-scoped [`PeerIdentity`]. The
//! generated dispatch hands each handler a [`connectrpc::RequestContext`]
//! whose extensions are the request's own: `ConnectInfo<PeerIdentity>`
//! inserted by axum's listener is how `Providers` sees the client
//! certificate.

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
use tokio::io::AsyncWriteExt as _;
use tokio_util::sync::CancellationToken;

use crate::api::*;
use crate::error::{Error, Result};
use crate::generated::preloop::sandbox::v1::{
    Empty, Providers, ProvidersExt, Sandboxes, SandboxesExt,
};
use crate::service::{ProviderIdentity, Service};
use crate::tls::{PeerIdentity, ServerTls, TlsListener};

/// Maximum request body for one Connect RPC.
pub const CONNECT_BODY_LIMIT: usize = 4 * 1024 * 1024;

/// Bearer token for `Sandboxes` callers. Always required: the listener speaks
/// TLS even on loopback.
#[derive(Debug, Clone, Default)]
pub struct Auth {
    /// Token callers present as `Authorization: Bearer …`.
    pub client_token: String,
}

impl Auth {
    /// Build from a token.
    pub fn new(client_token: impl Into<String>) -> Self {
        Self {
            client_token: client_token.into(),
        }
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
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
}

impl SandboxesService {
    fn authorize(&self, ctx: &RequestContext) -> Result<()> {
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
}

impl ProvidersService {
    /// The enrolled provider the request's client certificate authenticates,
    /// plus the fingerprint it was resolved from.
    fn identity(&self, ctx: &RequestContext) -> Result<(ProviderIdentity, String)> {
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
        _ctx: RequestContext,
        request: ServiceRequest<'_, EnrollRequest>,
    ) -> impl std::future::Future<
        Output = ServiceResult<impl Encodable<EnrollResponse> + Send + use<'a>>,
    > + Send
    + 'a {
        let request = request.to_owned_message();
        async move {
            // `Enroll` is authenticated by its join token alone.
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
}

/// Build the router.
///
/// The ConnectRPC services own the fallback; the data-plane routes are plain
/// axum routes on the same listener. `Providers` reads the peer certificate
/// from `ConnectInfo<PeerIdentity>`, so serve it with [`serve`] (or
/// `Router::into_make_service_with_connect_info`).
pub fn router(service: Arc<Service>, auth: Auth) -> Router {
    let sandboxes = Arc::new(SandboxesService {
        service: service.clone(),
        auth: auth.clone(),
    });
    let providers = Arc::new(ProvidersService {
        service: service.clone(),
    });
    let rpc = sandboxes.register(providers.register(connectrpc::Router::new()));
    // Bulk bytes travel over the ticketed data plane, never in RPCs. The
    // buffering middleware bounds every unary request (and the single Exec
    // envelope), and the data-plane routes below stream their own bodies.
    let rpc = axum::Router::new()
        .fallback_service(rpc.into_axum_service())
        .layer(middleware::from_fn(cap_rpc_body));
    Router::new()
        .route(
            "/sandbox/v1/streams/{ticket}",
            get(stream_download).put(stream_upload),
        )
        .route(
            "/sandbox/v1/agent/blobs/{ticket}",
            get(agent_blob_get).put(agent_blob_put),
        )
        .route("/sandbox/v1/ca.pem", get(ca_pem))
        .route("/sandbox/v1/healthz", get(|| async { "ok" }))
        .fallback_service(rpc)
        .with_state(AppState { service })
}

/// Reject an RPC body above [`CONNECT_BODY_LIMIT`] with `413`.
///
/// The generated dispatch reads the body itself and would answer a body over
/// its own limit with a Connect `resource_exhausted`; this middleware keeps
/// the service's documented HTTP answer and bounds buffering before dispatch.
async fn cap_rpc_body(request: axum::extract::Request, next: Next) -> Response {
    let (parts, body) = request.into_parts();
    match axum::body::to_bytes(body, CONNECT_BODY_LIMIT).await {
        Ok(bytes) => {
            let request = axum::extract::Request::from_parts(parts, Body::from(bytes));
            next.run(request).await
        }
        Err(_) => payload_too_large(CONNECT_BODY_LIMIT as u64),
    }
}

/// Serve the API over TLS until `shutdown`.
///
/// Each connection completes a handshake first; the peer's certificate (when
/// presented) becomes [`ConnectInfo<PeerIdentity>`] for the handlers.
pub async fn serve(
    listener: tokio::net::TcpListener,
    service: Arc<Service>,
    auth: Auth,
    tls: Arc<ServerTls>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let app = router(service, auth);
    axum::serve(
        TlsListener::new(listener, tls.acceptor.clone()),
        app.into_make_service_with_connect_info::<PeerIdentity>(),
    )
    .with_graceful_shutdown(async move { shutdown.cancelled().await })
    .await
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

/// A rejected blob body: the cap was exceeded or the transfer failed.
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

async fn remove_partial(path: &std::path::Path) {
    let _ = tokio::fs::remove_file(path).await;
}

async fn write_body(
    path: &std::path::Path,
    body: Body,
    max_bytes: u64,
) -> std::result::Result<(), BodyWriteError> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|e| Error::internal(format!("blob write: {e}")))?;
    let mut written: u64 = 0;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                drop(file);
                remove_partial(path).await;
                return Err(Error::unavailable(format!("upload: {error}")).into());
            }
        };
        written = written.saturating_add(chunk.len() as u64);
        if written > max_bytes {
            drop(file);
            remove_partial(path).await;
            return Err(BodyWriteError::TooLarge(max_bytes));
        }
        if let Err(error) = file.write_all(&chunk).await {
            drop(file);
            remove_partial(path).await;
            return Err(Error::internal(format!("blob write: {error}")).into());
        }
    }
    if let Err(error) = file.flush().await {
        drop(file);
        remove_partial(path).await;
        return Err(Error::internal(format!("blob write: {error}")).into());
    }
    Ok(())
}

async fn file_response(path: &std::path::Path) -> Result<Response> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| Error::internal(format!("blob read: {e}")))?;
    let length = file.metadata().await.map(|m| m.len()).unwrap_or_default();
    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-tar".to_owned()),
            (header::CONTENT_LENGTH, length.to_string()),
        ],
        Body::from_stream(tokio_util::io::ReaderStream::new(file)),
    )
        .into_response())
}

async fn stream_upload(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let token = bearer(&headers).unwrap_or_default();
    let max_bytes = app.service.config().blob_max_bytes;
    let (kind, path) = match app.service.claim_ticket(&ticket, token) {
        Ok(claimed) => claimed,
        Err(error) => return error.into_response(),
    };
    if kind != StreamKind::Upload {
        return Error::invalid_argument("ticket is not an upload").into_response();
    }
    if content_length(&headers).is_some_and(|length| length > max_bytes) {
        return payload_too_large(max_bytes);
    }
    match write_body(&path, body, max_bytes).await {
        Ok(()) => match app.service.finish_upload(&ticket).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(error) => error.into_response(),
        },
        Err(error) => {
            // The ticket's bytes are incomplete: consume it so nothing can
            // extract a truncated tar.
            app.service.drop_ticket(&ticket);
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
    let result = async {
        let (kind, _) = app.service.claim_ticket(&ticket, token)?;
        if kind != StreamKind::Download {
            return Err(Error::invalid_argument("ticket is not a download"));
        }
        let path = app.service.prepare_download(&ticket).await?;
        let response = file_response(&path).await;
        // The open file handle keeps the bytes readable after unlink.
        app.service.drop_ticket(&ticket);
        response
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
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

async fn agent_blob_get(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let identity = app.service.provider_identity(peer.fingerprint()?)?;
        let (id, generation) = provider_headers(&headers)?;
        require_identity(&identity, &id)?;
        let path = app.service.agent_blob(&id, generation, &ticket)?;
        file_response(&path).await
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

async fn agent_blob_put(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let result = async {
        let identity = app.service.provider_identity(peer.fingerprint()?)?;
        let (id, generation) = provider_headers(&headers)?;
        require_identity(&identity, &id)?;
        let path = app.service.agent_blob(&id, generation, &ticket)?;
        Ok::<_, Error>(path)
    }
    .await;
    let path = match result {
        Ok(path) => path,
        Err(error) => return error.into_response(),
    };
    let max_bytes = app.service.config().blob_max_bytes;
    if content_length(&headers).is_some_and(|length| length > max_bytes) {
        return payload_too_large(max_bytes);
    }
    match write_body(&path, body, max_bytes).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}
