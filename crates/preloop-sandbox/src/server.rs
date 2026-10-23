//! HTTP surface: Connect routes for both services plus the ticketed data plane.
//!
//! Everything is served over TLS. Callers hold the one client bearer token;
//! capacity owners authenticate with a client certificate issued by the
//! service CA (see [`crate::tls`]), except for `Enroll` which is authenticated
//! by a single-use join token.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::connect_info::ConnectInfo;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::api::*;
use crate::connect::{self, Code, Error};
use crate::service::{ProviderIdentity, Service};
use crate::tls::{PeerIdentity, ServerTls, TlsListener};

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
/// `127.0.0.0/8` address, or `::1`.
fn loopback_host(value: &str) -> bool {
    let Some(host) = host_without_port(value.trim()) else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

fn check_host(loopback_only: bool, headers: &HeaderMap) -> Result<(), Error> {
    if !loopback_only {
        return Ok(());
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Error::new(Code::PermissionDenied, "Host header is required"))?;
    if !loopback_host(host) {
        return Err(Error::new(
            Code::PermissionDenied,
            format!("Host `{host}` is not a loopback address"),
        ));
    }
    Ok(())
}

fn check(expected: &str, headers: &HeaderMap) -> Result<(), Error> {
    match bearer(headers) {
        Some(token)
            if token.len() == expected.len()
                && token
                    .bytes()
                    .zip(expected.bytes())
                    .fold(0u8, |a, (x, y)| a | (x ^ y))
                    == 0 =>
        {
            Ok(())
        }
        _ => Err(Error::new(
            Code::Unauthenticated,
            "missing or bad bearer token",
        )),
    }
}

/// Build the router.
///
/// `Providers` reads the peer certificate from `ConnectInfo<PeerIdentity>`, so
/// serve it with [`serve`] (or `Router::into_make_service_with_connect_info`).
pub fn router(service: Arc<Service>, auth: Auth) -> Router {
    Router::new()
        .route("/preloop.sandbox.v1.Sandboxes/{method}", post(sandboxes))
        .route("/preloop.sandbox.v1.Providers/{method}", post(providers))
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
        .with_state(AppState { service, auth })
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

async fn sandboxes(
    State(app): State<AppState>,
    Path(method): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(error) = check_host(app.auth.loopback_only, &headers) {
        return error.into_response();
    }
    if let Err(error) = check(&app.auth.client_token, &headers) {
        return error.into_response();
    }
    let service = app.service;
    use connect::{decode, unary_response as reply};
    match method.as_str() {
        "PutTemplate" => match decode::<PutTemplateRequest>(&body) {
            Ok(r) => reply(service.put_template(r.template).await),
            Err(e) => e.into_response(),
        },
        "DeleteTemplate" => match decode::<DeleteTemplateRequest>(&body) {
            Ok(r) => reply(service.delete_template(&r.name).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "ListTemplates" => reply(Ok(ListTemplatesResponse {
            templates: service.list_templates(),
        })),
        "PutPool" => match decode::<PutPoolRequest>(&body) {
            Ok(r) => reply(service.put_pool(r.pool).await),
            Err(e) => e.into_response(),
        },
        "ListPools" => reply(Ok(ListPoolsResponse {
            pools: service.list_pools(),
        })),
        "DeletePool" => match decode::<DeletePoolRequest>(&body) {
            Ok(r) => reply(service.delete_pool(&r.name).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "Acquire" => match decode::<AcquireRequest>(&body) {
            Ok(r) => reply(service.acquire(r).await),
            Err(e) => e.into_response(),
        },
        "Renew" => match decode::<RenewRequest>(&body) {
            Ok(r) => reply(service.renew(r).await),
            Err(e) => e.into_response(),
        },
        "Release" => match decode::<ReleaseRequest>(&body) {
            Ok(r) => reply(service.release(r).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "Get" => match decode::<GetRequest>(&body) {
            Ok(r) => reply(service.get(&r.sandbox_id)),
            Err(e) => e.into_response(),
        },
        "List" => reply(Ok(ListSandboxesResponse {
            sandboxes: service.list(),
        })),
        "ListProviders" => reply(Ok(ListProvidersResponse {
            providers: service.list_providers(),
        })),
        "DeleteProvider" => match decode::<DeleteProviderRequest>(&body) {
            Ok(r) => reply(service.delete_provider(&r.name).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "CreateJoinToken" => match decode::<CreateJoinTokenRequest>(&body) {
            Ok(r) => {
                let ttl = std::time::Duration::from_secs(r.ttl_secs);
                reply(service.create_join_token(ttl).await)
            }
            Err(e) => e.into_response(),
        },
        "Exec" => match connect::decode_stream_request::<ExecRequest>(&body) {
            Ok(r) => match service.exec(r) {
                Ok(stream) => connect::stream_response(stream),
                Err(e) => connect::stream_response(futures::stream::once(async move {
                    Err::<ExecEvent, _>(e)
                })),
            },
            Err(e) => e.into_response(),
        },
        "OpenStream" => match decode::<OpenStreamRequest>(&body) {
            Ok(r) => reply(service.open_stream(r)),
            Err(e) => e.into_response(),
        },
        "Snapshot" => match decode::<SnapshotRequest>(&body) {
            Ok(r) => reply(service.snapshot(r).await),
            Err(e) => e.into_response(),
        },
        "Stop" => match decode::<GetRequest>(&body) {
            Ok(r) => reply(service.stop(&r.sandbox_id).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "Start" => match decode::<StartRequest>(&body) {
            Ok(r) => reply(
                service
                    .start(&r.sandbox_id, r.forkable)
                    .await
                    .map(|_| Empty {}),
            ),
            Err(e) => e.into_response(),
        },
        "Pause" => match decode::<GetRequest>(&body) {
            Ok(r) => reply(service.pause(&r.sandbox_id).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        "Resume" => match decode::<GetRequest>(&body) {
            Ok(r) => reply(service.resume(&r.sandbox_id).await.map(|_| Empty {})),
            Err(e) => e.into_response(),
        },
        other => Error::new(Code::Unimplemented, format!("unknown method {other}")).into_response(),
    }
}

fn require_identity(identity: &ProviderIdentity, provider_id: &str) -> Result<(), Error> {
    if provider_id != identity.provider_id {
        return Err(Error::new(
            Code::PermissionDenied,
            "provider id does not match the client certificate",
        ));
    }
    Ok(())
}

async fn providers(
    State(app): State<AppState>,
    Path(method): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Providers authenticate by client certificate; the loopback Host guard
    // still applies to the enrollment call, which carries no certificate.
    if let Err(error) = check_host(app.auth.loopback_only, &headers) {
        return error.into_response();
    }
    let service = app.service;
    use connect::{decode, unary_response as reply};
    // `Enroll` is the one RPC authenticated by its join token instead of a
    // client certificate; every other method needs an enrolled identity.
    if method == "Enroll" {
        return match decode::<EnrollRequest>(&body) {
            Ok(request) => reply(service.enroll(request).await),
            Err(error) => error.into_response(),
        };
    }
    let fingerprint = match peer.fingerprint() {
        Ok(fingerprint) => fingerprint.to_owned(),
        Err(error) => {
            warn!(method = %method, "providers: call without a client certificate");
            return error.into_response();
        }
    };
    let identity = match service.provider_identity(&fingerprint) {
        Ok(identity) => identity,
        Err(error) => {
            // The TLS layer accepted the certificate (it chains to this CA),
            // but no live provider record owns that fingerprint: an identity
            // from before a re-key, or a revoked one.
            warn!(
                method = %method,
                fingerprint = %fingerprint,
                %error,
                "providers: client certificate is not enrolled"
            );
            return error.into_response();
        }
    };
    match method.as_str() {
        "Register" => match decode::<RegisterRequest>(&body) {
            Ok(request) => reply(
                service
                    .register_enrolled(request, &identity.provider_id)
                    .await,
            ),
            Err(error) => error.into_response(),
        },
        "Heartbeat" => match decode::<HeartbeatRequest>(&body) {
            Ok(request) => match require_identity(&identity, &request.provider_id) {
                Ok(()) => reply(service.heartbeat(request).map(|_| Empty {})),
                Err(error) => error.into_response(),
            },
            Err(error) => error.into_response(),
        },
        "Poll" => match decode::<PollRequest>(&body) {
            Ok(request) => match require_identity(&identity, &request.provider_id) {
                Ok(()) => reply(service.poll(request).await),
                Err(error) => error.into_response(),
            },
            Err(error) => error.into_response(),
        },
        "Report" => match decode::<ReportRequest>(&body) {
            Ok(request) => match require_identity(&identity, &request.provider_id) {
                Ok(()) => reply(service.report(request).await.map(|_| Empty {})),
                Err(error) => error.into_response(),
            },
            Err(error) => error.into_response(),
        },
        "ReportExec" => match decode::<ReportExecRequest>(&body) {
            Ok(request) => match require_identity(&identity, &request.provider_id) {
                Ok(()) => reply(service.report_exec(request).await.map(|_| Empty {})),
                Err(error) => error.into_response(),
            },
            Err(error) => error.into_response(),
        },
        "Reconcile" => match decode::<ReconcileRequest>(&body) {
            Ok(request) => match require_identity(&identity, &request.provider_id) {
                Ok(()) => reply(service.reconcile(request).await),
                Err(error) => error.into_response(),
            },
            Err(error) => error.into_response(),
        },
        "RenewCertificate" => match decode::<RenewCertificateRequest>(&body) {
            Ok(request) => reply(service.renew_certificate(request, &fingerprint).await),
            Err(error) => error.into_response(),
        },
        other => Error::new(Code::Unimplemented, format!("unknown method {other}")).into_response(),
    }
}

async fn write_body(path: &std::path::Path, body: Body) -> Result<(), Error> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|e| Error::internal(format!("blob write: {e}")))?;
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Error::unavailable(format!("upload: {e}")))?;
        file.write_all(&chunk)
            .await
            .map_err(|e| Error::internal(format!("blob write: {e}")))?;
    }
    file.flush()
        .await
        .map_err(|e| Error::internal(format!("blob write: {e}")))
}

async fn file_response(path: &std::path::Path) -> Result<Response, Error> {
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
    let result = async {
        check_host(app.auth.loopback_only, &headers)?;
        let (kind, path) = app.service.claim_ticket(&ticket, token)?;
        if kind != StreamKind::Upload {
            return Err(Error::invalid("ticket is not an upload"));
        }
        // Write beside the destination and rename on success: a reader can
        // never observe a partially written blob.
        let staging = path.with_file_name(format!(".{ticket}.part"));
        let written = write_body(&staging, body).await;
        let uploaded = match written {
            Ok(()) => tokio::fs::rename(&staging, &path)
                .await
                .map_err(|error| Error::internal(format!("blob rename: {error}"))),
            Err(error) => Err(error),
        };
        if uploaded.is_err() {
            let _ = tokio::fs::remove_file(&staging).await;
            return uploaded;
        }
        app.service.finish_upload(&ticket).await
    }
    .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn stream_download(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    headers: HeaderMap,
) -> Response {
    let token = bearer(&headers).unwrap_or_default();
    let result = async {
        check_host(app.auth.loopback_only, &headers)?;
        let (kind, _) = app.service.claim_ticket(&ticket, token)?;
        if kind != StreamKind::Download {
            return Err(Error::invalid("ticket is not a download"));
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

fn provider_headers(headers: &HeaderMap) -> Result<(String, u64), Error> {
    let id = headers
        .get("x-provider-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Error::invalid("x-provider-id required"))?;
    let generation = headers
        .get("x-provider-generation")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Error::invalid("x-provider-generation required"))?;
    Ok((id.to_owned(), generation))
}

async fn agent_blob_get(
    State(app): State<AppState>,
    Path(ticket): Path<String>,
    ConnectInfo(peer): ConnectInfo<PeerIdentity>,
    headers: HeaderMap,
) -> Response {
    let result = async {
        check_host(app.auth.loopback_only, &headers)?;
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
        check_host(app.auth.loopback_only, &headers)?;
        let identity = app.service.provider_identity(peer.fingerprint()?)?;
        let (id, generation) = provider_headers(&headers)?;
        require_identity(&identity, &id)?;
        let path = app.service.agent_blob(&id, generation, &ticket)?;
        write_body(&path, body).await
    }
    .await;
    match result {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
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
}
