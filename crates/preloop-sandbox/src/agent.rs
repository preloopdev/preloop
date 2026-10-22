//! Host agent: registers local drivers with a Sandbox Service, long-polls
//! for work, runs it, and reports back fenced by its generation.
//!
//! The same agent runs in-process (local `preloop serve`) and on remote
//! hosts (a Mac Studio behind NAT dials out; the service never connects in).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::*;
use crate::driver::Driver;
use crate::error::{Code, Error, Result};
use crate::generated::preloop::sandbox::v1 as rpc;
use crate::service::Service;

/// Most per-sandbox runtime entries one `Heartbeat` carries. A host can run
/// more sandboxes than this; the report is ordered most-recently-active
/// first and the tail is dropped, so the heartbeat stays bounded while the
/// sandboxes that matter keep their stats.
pub const MAX_HEARTBEAT_SANDBOX_STATS: usize = 512;

/// How an agent reaches the service's `Providers` API.
#[async_trait]
pub trait ProviderLink: Send + Sync {
    /// `Register`.
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse>;
    /// `Heartbeat`.
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()>;
    /// `Poll`.
    async fn poll(&self, request: PollRequest) -> Result<PollResponse>;
    /// `Report`.
    async fn report(&self, request: ReportRequest) -> Result<()>;
    /// `ReportExec`.
    async fn report_exec(&self, request: ReportExecRequest) -> Result<()>;
    /// `Reconcile`.
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse>;
    /// Download a ticket's blob to `dest`.
    async fn fetch_blob(&self, id: &Session, ticket: &str, dest: &Path) -> Result<()>;
    /// Upload `src` as a ticket's blob.
    async fn put_blob(&self, id: &Session, ticket: &str, src: &Path) -> Result<()>;
}

/// A registered agent session.
#[derive(Debug, Clone)]
pub struct Session {
    /// Provider id.
    pub provider_id: String,
    /// Generation.
    pub generation: u64,
}

/// In-process link.
#[async_trait]
impl ProviderLink for Arc<Service> {
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        Service::register(self, request).await
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        Service::heartbeat(self, request)
    }
    async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        Service::poll(self, request).await
    }
    async fn report(&self, request: ReportRequest) -> Result<()> {
        Service::report(self, request).await
    }
    async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        Service::report_exec(self, request).await
    }
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        Service::reconcile(self, request).await
    }
    async fn fetch_blob(&self, id: &Session, ticket: &str, dest: &Path) -> Result<()> {
        let path = self.agent_blob(&id.provider_id, id.generation, ticket)?;
        tokio::fs::copy(&path, dest)
            .await
            .map(|_| ())
            .map_err(|e| Error::internal(format!("blob fetch: {e}")))
    }
    async fn put_blob(&self, id: &Session, ticket: &str, src: &Path) -> Result<()> {
        let path = self.begin_agent_write(&id.provider_id, id.generation, ticket)?;
        let result = tokio::fs::copy(src, &path)
            .await
            .map(|_| ())
            .map_err(|e| Error::internal(format!("blob put: {e}")));
        self.end_agent_write(ticket);
        result
    }
}

/// Remote link over Connect + HTTPS blobs, authenticated by the client
/// certificate issued at enrollment.
#[derive(Debug)]
pub struct RemoteLink {
    base: String,
    http: parking_lot::RwLock<reqwest::Client>,
    /// rustls configuration for the RPC transport, rebuilt with the HTTP
    /// client whenever the identity is renewed.
    tls: parking_lot::RwLock<Arc<rustls::ClientConfig>>,
    identity: parking_lot::RwLock<Option<crate::tls::AgentIdentity>>,
    renew: tokio::sync::Mutex<()>,
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

/// The agent only speaks TLS: the CA pin has no meaning behind a plaintext
/// proxy that terminates TLS.
fn normalize_base(base: &str) -> Result<String> {
    let base = base.trim_end_matches('/');
    if !base.starts_with("https://") {
        return Err(Error::invalid_argument(
            "the sandbox API is always TLS; use an https:// URL",
        ));
    }
    Ok(base.to_owned())
}

impl RemoteLink {
    /// Enroll with a join token and store the identity in `identity_dir`.
    ///
    /// The service CA is fetched first and checked against the token's pin,
    /// so the CSR only reaches a service that holds the CA the operator
    /// issued the token for.
    pub async fn enroll(
        base: impl Into<String>,
        token: &str,
        identity_dir: impl Into<PathBuf>,
        name: &str,
    ) -> Result<Self> {
        let base = base.into();
        let base = normalize_base(&base)?;
        let pin = crate::tls::parse_join_token(token)?.ca_sha256.to_owned();
        let ca_pem = crate::tls::fetch_ca(&base).await?;
        if crate::tls::pem_fingerprint(&ca_pem)? != pin {
            return Err(Error::new(
                Code::Unauthenticated,
                "sandbox service CA does not match the join token pin",
            ));
        }
        let (key_pem, csr_pem) = crate::tls::generate_csr(name)?;
        let response = enroll(&base, &ca_pem, token, name, csr_pem).await?;
        if crate::tls::pem_fingerprint(&response.ca_pem)? != pin {
            return Err(Error::new(
                Code::Unauthenticated,
                "enrollment answered with a different CA",
            ));
        }
        let identity = crate::tls::AgentIdentity {
            dir: identity_dir.into(),
            metadata: crate::tls::IdentityMetadata {
                name: name.to_owned(),
                base: base.clone(),
                ca_sha256: pin,
                issued_at: unix_now(),
                expires_at: response.expires_at,
            },
            ca_pem: response.ca_pem,
            cert_pem: response.cert_pem,
            key_pem,
        };
        identity.save()?;
        Self::from_identity(identity)
    }

    /// Load a stored identity, or enroll with `token` when there is none.
    pub async fn connect(
        identity_dir: impl Into<PathBuf>,
        base: Option<&str>,
        token: Option<&str>,
        name: impl Into<String>,
    ) -> Result<Self> {
        let identity_dir = identity_dir.into();
        if let Some(mut identity) = crate::tls::AgentIdentity::load(&identity_dir)? {
            if let Some(base) = base {
                identity.metadata.base = normalize_base(base)?;
                identity.save()?;
            }
            return Self::from_identity(identity);
        }
        let base = base.ok_or_else(|| {
            Error::invalid_argument("`--join` is required on the first run (no stored identity)")
        })?;
        let token = token.ok_or_else(|| {
            Error::new(
                Code::Unauthenticated,
                "a join token is required on the first run",
            )
        })?;
        let name = name.into();
        Self::enroll(base, token, identity_dir, &name).await
    }

    fn from_identity(identity: crate::tls::AgentIdentity) -> Result<Self> {
        let http = crate::tls::http_client(&identity.ca_pem, Some(&identity.identity_pem()))?;
        let tls = crate::tls::client_config(&identity.ca_pem, Some(&identity.identity_pem()))?;
        Ok(Self {
            base: identity.metadata.base.clone(),
            http: parking_lot::RwLock::new(http),
            tls: parking_lot::RwLock::new(tls),
            identity: parking_lot::RwLock::new(Some(identity)),
            renew: tokio::sync::Mutex::new(()),
        })
    }

    /// Base URL.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// The provider name this link's certificate is bound to.
    pub fn name(&self) -> Option<String> {
        self.identity
            .read()
            .as_ref()
            .map(|identity| identity.metadata.name.clone())
    }

    /// A generated `Providers` client over the current identity.
    fn client(
        &self,
    ) -> Result<
        rpc::ProvidersClient<crate::client::ClassifiedTransport<connectrpc::client::HttpClient>>,
    > {
        let uri: axum::http::Uri = self
            .base
            .parse()
            .map_err(|e| Error::invalid_argument(format!("sandbox API URL: {e}")))?;
        let config = connectrpc::client::ClientConfig::new(uri).json();
        Ok(rpc::ProvidersClient::new(
            crate::client::ClassifiedTransport(connectrpc::client::HttpClient::with_tls(
                self.tls.read().clone(),
            )),
            config,
        ))
    }

    /// Issue a fresh certificate and hot-reload the client identity.
    ///
    /// The service keeps the previous fingerprint valid until it expires, so
    /// a failure after the server signed (before the files land) is safe.
    pub async fn renew(&self) -> Result<()> {
        let Some(identity) = self.identity.read().clone() else {
            return Ok(());
        };
        let _guard = self.renew.lock().await;
        let (key_pem, csr_pem) = crate::tls::generate_csr(&identity.metadata.name)?;
        let response = self
            .client()?
            .renew_certificate(RenewCertificateRequest {
                csr_pem,
                ..RenewCertificateRequest::default()
            })
            .await?
            .into_owned();
        let next = identity.renewed(response.cert_pem, key_pem, unix_now(), response.expires_at);
        let http = crate::tls::http_client(&next.ca_pem, Some(&next.identity_pem()))?;
        let tls = crate::tls::client_config(&next.ca_pem, Some(&next.identity_pem()))?;
        next.save()?;
        *self.http.write() = http;
        *self.tls.write() = tls;
        *self.identity.write() = Some(next);
        Ok(())
    }

    async fn maybe_renew(&self) {
        let due = self
            .identity
            .read()
            .as_ref()
            .is_some_and(|identity| identity.needs_renewal(unix_now()));
        if !due {
            return;
        }
        if let Err(error) = self.renew().await {
            debug!(%error, "certificate renewal failed; retrying on the next heartbeat");
        }
    }

    fn blob_request(
        &self,
        method: reqwest::Method,
        id: &Session,
        ticket: &str,
    ) -> reqwest::RequestBuilder {
        self.http
            .read()
            .request(
                method,
                format!("{}/sandbox/v1/agent/blobs/{ticket}", self.base),
            )
            .header("x-provider-id", &id.provider_id)
            .header("x-provider-generation", id.generation.to_string())
    }
}

/// `Enroll`: the one RPC authenticated by the join token instead of a
/// client certificate, so it runs over a transport that presents none.
async fn enroll(
    base: &str,
    ca_pem: &str,
    token: &str,
    name: &str,
    csr_pem: String,
) -> Result<EnrollResponse> {
    let uri: axum::http::Uri = base
        .parse()
        .map_err(|e| Error::invalid_argument(format!("sandbox API URL: {e}")))?;
    let config = connectrpc::client::ClientConfig::new(uri).json();
    let transport =
        connectrpc::client::HttpClient::with_tls(crate::tls::client_config(ca_pem, None)?);
    let response = rpc::ProvidersClient::new(transport, config)
        .enroll(EnrollRequest {
            token: token.to_owned(),
            name: name.to_owned(),
            csr_pem,
            ..EnrollRequest::default()
        })
        .await?
        .into_owned();
    Ok(response)
}

#[async_trait]
impl ProviderLink for RemoteLink {
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        Ok(self.client()?.register(request).await?.into_owned())
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        // Heartbeats are the agent's slowest reliable tick: renew here so a
        // long-polling agent still rotates its certificate in time.
        self.maybe_renew().await;
        self.client()?.heartbeat(request).await?;
        Ok(())
    }
    async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        Ok(self.client()?.poll(request).await?.into_owned())
    }
    async fn report(&self, request: ReportRequest) -> Result<()> {
        self.client()?.report(request).await?;
        Ok(())
    }
    async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        self.client()?.report_exec(request).await?;
        Ok(())
    }
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        Ok(self.client()?.reconcile(request).await?.into_owned())
    }
    async fn fetch_blob(&self, id: &Session, ticket: &str, dest: &Path) -> Result<()> {
        use futures::StreamExt as _;
        use tokio::io::AsyncWriteExt as _;
        let response = self
            .blob_request(reqwest::Method::GET, id, ticket)
            .send()
            .await
            .map_err(|e| Error::unavailable(format!("blob fetch: {e}")))?;
        if !response.status().is_success() {
            return Err(Error::unavailable(format!(
                "blob fetch: HTTP {}",
                response.status()
            )));
        }
        let mut file = tokio::fs::File::create(dest)
            .await
            .map_err(|e| Error::internal(format!("blob fetch: {e}")))?;
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::unavailable(format!("blob fetch: {e}")))?;
            file.write_all(&chunk)
                .await
                .map_err(|e| Error::internal(format!("blob fetch: {e}")))?;
        }
        file.flush()
            .await
            .map_err(|e| Error::internal(format!("blob fetch: {e}")))
    }
    async fn put_blob(&self, id: &Session, ticket: &str, src: &Path) -> Result<()> {
        let file = tokio::fs::File::open(src)
            .await
            .map_err(|e| Error::internal(format!("blob put: {e}")))?;
        let length = file.metadata().await.map(|m| m.len()).unwrap_or_default();
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let response = self
            .blob_request(reqwest::Method::PUT, id, ticket)
            .header(reqwest::header::CONTENT_LENGTH, length)
            .body(body)
            .send()
            .await
            .map_err(|e| Error::unavailable(format!("blob put: {e}")))?;
        if !response.status().is_success() {
            return Err(Error::unavailable(format!(
                "blob put: HTTP {}",
                response.status()
            )));
        }
        Ok(())
    }
}

/// Host paths an agent may expose inside its guests.
///
/// The agent is the authority for this, not the service or the driver: a
/// `Create` asking for a host mount or socket outside these prefixes is
/// rejected with `FAILED_PRECONDITION` before the driver runs. Paths are
/// resolved through symlinks when checked, so `..` traversal and symlinks
/// cannot step outside an allowed tree. An empty list denies every mount of
/// that kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HostAccess {
    /// Host directory prefixes allowed for `Mount.host`.
    pub mounts: Vec<PathBuf>,
    /// Host socket path prefixes allowed for `SocketMount.host`.
    pub sockets: Vec<PathBuf>,
}

impl HostAccess {
    /// Prefixes from `PRELOOP_SANDBOX_MOUNT_ALLOW` and
    /// `PRELOOP_SANDBOX_SOCKET_ALLOW` (comma-separated host paths; unset or
    /// empty denies every mount of that kind).
    pub fn from_env() -> Self {
        fn list(variable: &str) -> Vec<PathBuf> {
            std::env::var(variable)
                .ok()
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|path| !path.is_empty())
                        .map(PathBuf::from)
                        .collect()
                })
                .unwrap_or_default()
        }
        Self {
            mounts: list("PRELOOP_SANDBOX_MOUNT_ALLOW"),
            sockets: list("PRELOOP_SANDBOX_SOCKET_ALLOW"),
        }
    }

    /// Allow more host directory prefixes.
    pub fn allow_mounts(&mut self, prefixes: impl IntoIterator<Item = PathBuf>) {
        self.mounts.extend(prefixes);
    }

    /// Allow more host socket prefixes.
    pub fn allow_sockets(&mut self, prefixes: impl IntoIterator<Item = PathBuf>) {
        self.sockets.extend(prefixes);
    }

    /// Reject a create whose host mounts or sockets this agent does not
    /// export. Forks arrive as `Create` work and take the same check.
    pub fn check_create(&self, work: &CreateWork) -> Result<()> {
        for mount in &work.mounts {
            check_host_path("mount", &mount.host, &self.mounts)?;
        }
        for socket in &work.sockets {
            check_host_path("socket", &socket.host, &self.sockets)?;
        }
        Ok(())
    }
}

/// Canonicalize `host` and require it under one of `allowed`.
fn check_host_path(kind: &str, host: &str, allowed: &[PathBuf]) -> Result<()> {
    if host.is_empty() {
        return Err(Error::failed_precondition(format!(
            "host {kind} path is empty"
        )));
    }
    if allowed.is_empty() {
        return Err(Error::failed_precondition(format!(
            "host {kind} `{host}` is not allowed: this agent exports no {kind} paths"
        )));
    }
    let resolved = std::fs::canonicalize(host).map_err(|error| {
        Error::failed_precondition(format!("host {kind} `{host}` cannot be resolved: {error}"))
    })?;
    let permitted = allowed.iter().any(|prefix| {
        std::fs::canonicalize(prefix).is_ok_and(|prefix| resolved.starts_with(prefix))
    });
    if permitted {
        Ok(())
    } else {
        Err(Error::failed_precondition(format!(
            "host {kind} `{host}` is outside this agent's allowlist"
        )))
    }
}

/// Agent settings.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Stable provider name.
    pub name: String,
    /// Scratch directory for blobs.
    pub scratch: PathBuf,
    /// Heartbeat period.
    pub heartbeat: Duration,
    /// Full inventory reconcile period.
    pub reconcile: Duration,
    /// Host mounts and sockets this agent may expose to guests.
    pub access: HostAccess,
    /// `preloop.sandbox.*` instruments for facts only the agent sees (a
    /// heartbeat trimmed to its cap). Defaults to a no-op registry.
    pub metrics: crate::metrics::SandboxMetrics,
    /// Work items run at once (`PRELOOP_SANDBOX_AGENT_CONCURRENCY`).
    pub concurrency: usize,
    /// First retry delay for failed polls and reports; doubles up to 15 s,
    /// with jitter.
    pub retry_backoff: Duration,
}

impl AgentConfig {
    /// Defaults.
    pub fn new(name: impl Into<String>, scratch: PathBuf) -> Self {
        Self {
            name: name.into(),
            scratch,
            heartbeat: Duration::from_secs(10),
            reconcile: Duration::from_secs(60),
            access: HostAccess::default(),
            metrics: crate::metrics::SandboxMetrics::default(),
            concurrency: 32,
            retry_backoff: Duration::from_millis(500),
        }
    }

    /// Set the host access allowlist.
    pub fn with_access(mut self, access: HostAccess) -> Self {
        self.access = access;
        self
    }
}

/// Most a retry delay grows to.
const BACKOFF_CAP: Duration = Duration::from_secs(15);

/// How long a completed work id is remembered so a redelivery does not run
/// it again. Longer than the service's delivery timeout.
const DONE_RETENTION: Duration = Duration::from_secs(600);

/// The result of one work item, kept so a redelivery can re-report it
/// instead of re-executing.
#[derive(Debug, Clone)]
struct WorkOutcome {
    ok: bool,
    error: String,
    native_ref: String,
}

impl WorkOutcome {
    fn from_result(result: Result<String>) -> Self {
        match result {
            Ok(native_ref) => Self {
                ok: true,
                error: String::new(),
                native_ref,
            },
            Err(error) => Self {
                ok: false,
                // Keep the code visible: an operator reading the service log
                // can tell "not supported" from "provider is down".
                error: format!("{}: {}", error.code.as_str(), crate::error::message(&error)),
                native_ref: String::new(),
            },
        }
    }
}

/// What the agent knows about a delivered work id.
enum WorkState {
    Running,
    Done(WorkOutcome),
}

/// A retried call stops for good on these: the service fenced this session
/// (a newer generation registered) or forgot the work (it completed).
fn fenced(error: &Error) -> bool {
    error.code == Code::Aborted || error.code == Code::NotFound
}

/// Uniform jitter in `[base/2, base]`.
fn jittered(base: Duration) -> Duration {
    let nanos = base.as_nanos().max(1) as u64;
    let low = nanos / 2;
    Duration::from_nanos(low + rand::random::<u64>() % (nanos - low + 1))
}

/// First delay between registration attempts.
const REGISTER_BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Ceiling for ordinary transport failures.
const REGISTER_BACKOFF_MAX: Duration = Duration::from_secs(15);
/// Certificate failures (`unauthenticated`, `permission_denied`) cannot fix
/// themselves: hold the retry cadence at this delay until an operator
/// re-enrolls the agent, instead of hammering the service.
const REGISTER_AUTH_BACKOFF: Duration = Duration::from_secs(30);

/// Delay before the next registration attempt.
fn registration_delay(backoff: Duration, error: Option<&Error>) -> Duration {
    match error {
        Some(error) if matches!(error.code, Code::Unauthenticated | Code::PermissionDenied) => {
            backoff.max(REGISTER_AUTH_BACKOFF)
        }
        _ => backoff,
    }
}

/// The host agent.
pub struct Agent {
    link: Arc<dyn ProviderLink>,
    drivers: HashMap<String, Arc<dyn Driver>>,
    config: AgentConfig,
    /// Last time this agent handled work for a driver-native sandbox, keyed
    /// by native ref. Orders the capped heartbeat report so the sandboxes
    /// being used are the ones that keep their stats.
    active: Mutex<HashMap<String, Instant>>,
    permits: Arc<Semaphore>,
    /// Delivered work by id, so redelivery is not re-executed.
    work: tokio::sync::Mutex<HashMap<String, (WorkState, Instant)>>,
}

impl Agent {
    /// Build an agent over drivers.
    pub fn new(
        link: Arc<dyn ProviderLink>,
        drivers: Vec<Arc<dyn Driver>>,
        config: AgentConfig,
    ) -> Arc<Self> {
        let permits = Arc::new(Semaphore::new(config.concurrency.max(1)));
        Arc::new(Self {
            link,
            drivers: drivers
                .into_iter()
                .map(|driver| (driver.name().to_owned(), driver))
                .collect(),
            config,
            active: Mutex::new(HashMap::new()),
            permits,
            work: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Register once (tests use this to wait for registration).
    pub async fn register(&self) -> Result<Session> {
        let response = self
            .link
            .register(RegisterRequest {
                name: self.config.name.clone(),
                drivers: self
                    .drivers
                    .values()
                    .map(|driver| DriverInfo {
                        driver: driver.name().to_owned(),
                        capabilities: driver.capabilities().into(),
                        ..DriverInfo::default()
                    })
                    .collect(),
                protocol_version: crate::PROTOCOL_VERSION,
                ..RegisterRequest::default()
            })
            .await?;
        Ok(Session {
            provider_id: response.provider_id,
            generation: response.generation,
        })
    }

    /// Run until `shutdown`, re-registering after fencing or link failures.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        std::fs::create_dir_all(&self.config.scratch).ok();
        let mut backoff = REGISTER_BACKOFF_MIN;
        while !shutdown.is_cancelled() {
            let mut failed = None;
            match self.register().await {
                Ok(session) => {
                    info!(provider = %session.provider_id, generation = session.generation, "agent registered");
                    backoff = REGISTER_BACKOFF_MIN;
                    let error = self.clone().session(session, shutdown.clone()).await;
                    if let Some(error) = error {
                        warn!(%error, "agent session ended; re-registering");
                    }
                }
                Err(error) => {
                    warn!(%error, "agent registration failed");
                    failed = Some(error);
                }
            }
            let delay = registration_delay(backoff, failed.as_ref());
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
            backoff = (backoff * 2).min(REGISTER_BACKOFF_MAX);
        }
    }

    async fn session(
        self: Arc<Self>,
        session: Session,
        shutdown: CancellationToken,
    ) -> Option<Error> {
        let stop = shutdown.child_token();
        let heartbeat = {
            let agent = self.clone();
            let session = session.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(agent.config.heartbeat);
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = tick.tick() => {
                            let (host, sandboxes) = agent.collect_host_report().await;
                            if let Err(error) = agent.link.heartbeat(HeartbeatRequest {
                                provider_id: session.provider_id.clone(),
                                generation: session.generation,
                                host: host.into(),
                                sandboxes,
                                ..HeartbeatRequest::default()
                            }).await && error.code == Code::Aborted {
                                stop.cancel();
                                return;
                            }
                        }
                    }
                }
            })
        };
        let reconcile = {
            let agent = self.clone();
            let session = session.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(agent.config.reconcile);
                loop {
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        _ = tick.tick() => agent.reconcile_once(&session).await,
                    }
                }
            })
        };
        let mut result = None;
        let mut backoff = self.config.retry_backoff;
        loop {
            let poll = self.link.poll(PollRequest {
                provider_id: session.provider_id.clone(),
                generation: session.generation,
                wait_secs: 25,
                ..PollRequest::default()
            });
            let response = tokio::select! {
                _ = stop.cancelled() => break,
                response = poll => response,
            };
            match response {
                Ok(response) => {
                    backoff = self.config.retry_backoff;
                    for item in response.work {
                        self.dispatch(session.clone(), stop.clone(), item).await;
                    }
                }
                Err(error) if fenced(&error) => {
                    result = Some(error);
                    break;
                }
                Err(error) => {
                    debug!(%error, "poll failed; backing off");
                    let delay = jittered(backoff);
                    tokio::select! {
                        _ = stop.cancelled() => break,
                        _ = tokio::time::sleep(delay) => {}
                    }
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
            }
        }
        stop.cancel();
        let _ = heartbeat.await;
        let _ = reconcile.await;
        result
    }

    /// Hand one delivered item to a worker, unless this id is already running
    /// or done (a redelivery), in which case its stored result is re-reported.
    async fn dispatch(self: &Arc<Self>, session: Session, stop: CancellationToken, item: WorkItem) {
        let work_id = item.id.clone();
        let previous = {
            let mut work = self.work.lock().await;
            prune_work(&mut work);
            match work.get(&work_id) {
                Some((WorkState::Running, _)) => Some(None),
                Some((WorkState::Done(outcome), _)) => Some(Some(outcome.clone())),
                None => {
                    work.insert(work_id.clone(), (WorkState::Running, Instant::now()));
                    None
                }
            }
        };
        match previous {
            // Already in hand; its own task will report.
            Some(None) => {}
            // Completed earlier: re-report the stored result, do not run it.
            Some(Some(outcome)) => {
                let agent = Arc::clone(self);
                let session = session.clone();
                let stop = stop.clone();
                tokio::spawn(async move {
                    agent.report_work(&session, &stop, work_id, &outcome).await;
                });
            }
            None => {
                // Backpressure: do not take more work than the agent can run.
                let permit = tokio::select! {
                    _ = stop.cancelled() => {
                        // Not started: let the next session run it.
                        self.work.lock().await.remove(&work_id);
                        return;
                    }
                    permit = self.permits.clone().acquire_owned() => permit,
                };
                let Ok(permit) = permit else {
                    self.work.lock().await.remove(&work_id);
                    return;
                };
                let agent = Arc::clone(self);
                tokio::spawn(async move {
                    let _permit = permit;
                    agent.handle(session, stop, item).await;
                });
            }
        }
    }

    /// One inventory reconcile.
    pub async fn reconcile_once(&self, session: &Session) {
        let mut inventory = Vec::new();
        for driver in self.drivers.values() {
            match driver.inventory().await {
                Ok(items) => inventory.extend(items),
                Err(error) => {
                    // A partial inventory would look like losses: skip the round.
                    warn!(driver = driver.name(), %error, "inventory failed; skipping reconcile");
                    return;
                }
            }
        }
        match self
            .link
            .reconcile(ReconcileRequest {
                provider_id: session.provider_id.clone(),
                generation: session.generation,
                sandboxes: inventory,
                ..ReconcileRequest::default()
            })
            .await
        {
            Ok(response) => {
                for target in response.destroy {
                    let Some(driver) = self.drivers.get(&target.driver) else {
                        continue;
                    };
                    info!(native = %target.native_ref, "reconcile: destroying orphan");
                    if let Err(error) = driver.destroy(&target.native_ref).await {
                        warn!(native = %target.native_ref, %error, "orphan destroy failed");
                    }
                }
            }
            Err(error) => debug!(%error, "reconcile failed"),
        }
    }

    /// Fan out `host_report` over every driver on this host: one host
    /// sample (the first driver to measure one wins; they describe the
    /// same host) plus every driver's current sandboxes, merged and capped.
    /// Never fails — a driver without the hook contributes nothing.
    ///
    /// The merged report is ordered most-recently-active first and trimmed
    /// to [`MAX_HEARTBEAT_SANDBOX_STATS`], so one heartbeat stays bounded no
    /// matter how many sandboxes the host runs. Dropped entries are counted
    /// on `preloop.sandbox.heartbeat.truncated` and logged.
    pub async fn collect_host_report(&self) -> (Option<HostSample>, Vec<SandboxRuntimeStats>) {
        let mut host = None;
        let mut sandboxes = Vec::new();
        for driver in self.drivers.values() {
            let report = driver.host_report().await;
            if host.is_none()
                && report.host.is_set()
                && report.host.as_option() != Some(&HostSample::default())
            {
                host = report.host.into_option();
            }
            sandboxes.extend(report.sandboxes);
        }
        let dropped = {
            let mut active = self.active.lock();
            cap_heartbeat_sandboxes(&mut sandboxes, &mut active)
        };
        if dropped > 0 {
            self.config
                .metrics
                .record_heartbeat_truncation(dropped as u64);
            warn!(
                dropped,
                kept = MAX_HEARTBEAT_SANDBOX_STATS,
                "heartbeat sandbox stats truncated"
            );
        }
        (host, sandboxes)
    }

    /// Remember that this agent just handled work for `native_ref`.
    fn touch(&self, native_ref: &str) {
        if native_ref.is_empty() {
            return;
        }
        self.active
            .lock()
            .insert(native_ref.to_owned(), Instant::now());
    }

    fn driver(&self, name: &str) -> Result<Arc<dyn Driver>> {
        self.drivers.get(name).cloned().ok_or_else(|| {
            Error::failed_precondition(format!("driver `{name}` is not on this host"))
        })
    }

    /// Run one work item, remember its outcome, and report it until the
    /// service acknowledges it (or fences this session).
    async fn handle(self: Arc<Self>, session: Session, stop: CancellationToken, item: WorkItem) {
        let work_id = item.id.clone();
        let outcome = WorkOutcome::from_result(self.run_work(&session, &stop, item).await);
        if !outcome.ok {
            warn!(%work_id, error = %outcome.error, "work failed");
        }
        // A successful create or snapshot returns the native ref that did
        // not exist when the work was dispatched.
        if outcome.ok {
            self.touch(&outcome.native_ref);
        }

        {
            let mut work = self.work.lock().await;
            work.insert(
                work_id.clone(),
                (WorkState::Done(outcome.clone()), Instant::now()),
            );
        }
        self.report_work(&session, &stop, work_id, &outcome).await;
    }

    /// Retry `Report` until the service acknowledges it; stop when fenced.
    async fn report_work(
        &self,
        session: &Session,
        stop: &CancellationToken,
        work_id: String,
        outcome: &WorkOutcome,
    ) {
        let mut backoff = self.config.retry_backoff;
        loop {
            match self
                .link
                .report(ReportRequest {
                    provider_id: session.provider_id.clone(),
                    generation: session.generation,
                    work_id: work_id.clone(),
                    ok: outcome.ok,
                    error: outcome.error.clone(),
                    native_ref: outcome.native_ref.clone(),
                    ..ReportRequest::default()
                })
                .await
            {
                Ok(()) => return,
                Err(error) if fenced(&error) => {
                    debug!(%work_id, %error, "report stopped");
                    return;
                }
                Err(error) => {
                    debug!(%work_id, %error, "report failed; retrying");
                    if !self.wait_backoff(stop, backoff).await {
                        return;
                    }
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
            }
        }
    }

    /// Sleep a jittered delay; false when the session is stopping.
    async fn wait_backoff(&self, stop: &CancellationToken, delay: Duration) -> bool {
        tokio::select! {
            _ = stop.cancelled() => false,
            _ = tokio::time::sleep(jittered(delay)) => true,
        }
    }

    async fn run_work(
        &self,
        session: &Session,
        stop: &CancellationToken,
        item: WorkItem,
    ) -> Result<String> {
        // Record activity before running: a slow exec is exactly the
        // sandbox whose stats a capped heartbeat must keep.
        if let Some(native_ref) = item_native_ref(&item) {
            self.touch(native_ref);
        }
        let Some(work) = item.work else {
            return Err(Error::new(Code::Unimplemented, "unknown work item"));
        };
        match work {
            work_item::Work::CancelExec(cancel) => {
                self.driver(&cancel.target.driver)?
                    .cancel_exec(&cancel.target.native_ref, &cancel.exec_id)
                    .await?;
                Ok(String::new())
            }
            work_item::Work::Create(create) => {
                let create = *create;
                // Admission before the driver: no backend sees a host path
                // this agent does not export. Fork acquires carry their mounts
                // here too, so they are checked the same way.
                self.config.access.check_create(&create)?;
                self.driver(&create.driver)?.create(&create).await
            }
            work_item::Work::Destroy(target) => {
                let driver = self.driver(&target.driver)?;
                if let Some(snapshot) = target.native_ref.strip_prefix("snapshot:") {
                    driver.drop_snapshot(snapshot).await?;
                } else {
                    driver.destroy(&target.native_ref).await?;
                }
                Ok(String::new())
            }
            work_item::Work::Exec(exec) => {
                self.run_exec(session, stop, *exec).await?;
                Ok(String::new())
            }
            work_item::Work::Copy(copy) => {
                let driver = self.driver(&copy.target.driver)?;
                let scratch = self.config.scratch.join(format!("{}.tar", copy.ticket));
                let result = match copy.kind.as_known().unwrap_or_default() {
                    StreamKind::Upload => {
                        self.link
                            .fetch_blob(session, &copy.ticket, &scratch)
                            .await?;
                        driver
                            .copy_in(&copy.target.native_ref, &scratch, &copy.path)
                            .await
                    }
                    StreamKind::Download => {
                        driver
                            .copy_out(&copy.target.native_ref, &copy.path, &scratch)
                            .await?;
                        self.link.put_blob(session, &copy.ticket, &scratch).await
                    }
                    StreamKind::StreamKindUnspecified => {
                        Err(Error::invalid_argument("copy kind unset"))
                    }
                };
                let _ = tokio::fs::remove_file(&scratch).await;
                result.map(|_| String::new())
            }
            work_item::Work::Snapshot(snapshot) => {
                self.driver(&snapshot.target.driver)?
                    .snapshot(&snapshot.target.native_ref, &snapshot.template)
                    .await
            }
            work_item::Work::Stop(target) => {
                self.driver(&target.driver)?
                    .stop(&target.native_ref)
                    .await?;
                Ok(String::new())
            }
            work_item::Work::Start(start) => {
                // `Start` boots an existing machine and carries no host
                // mounts; a fork is a `Create` and took the allowlist check.
                self.driver(&start.target.driver)?
                    .start(&start.target.native_ref, start.forkable)
                    .await?;
                Ok(String::new())
            }
            work_item::Work::Pause(target) => {
                self.driver(&target.driver)?
                    .pause(&target.native_ref)
                    .await?;
                Ok(String::new())
            }
            work_item::Work::Resume(target) => {
                self.driver(&target.driver)?
                    .resume(&target.native_ref)
                    .await?;
                Ok(String::new())
            }
        }
    }

    async fn run_exec(
        &self,
        session: &Session,
        stop: &CancellationToken,
        exec: ExecWork,
    ) -> Result<()> {
        let driver = self.driver(&exec.target.driver)?;
        let cancel_driver = driver.clone();
        let (tx, mut rx) = mpsc::channel::<ExecEvent>(256);
        let native = exec.target.native_ref.clone();
        let request = exec.request.clone();
        let exec_id = exec.exec_id.clone();
        let run_native = native.clone();
        let run_exec_id = exec_id.clone();
        let run =
            tokio::spawn(async move { driver.exec(&run_native, &run_exec_id, &request, tx).await });
        // Forward output in small batches. Sequences start at this session's
        // generation base, so output from a re-registered session is never
        // mistaken for a duplicate of the previous session's batches.
        let mut batch = Vec::new();
        let mut size = 0usize;
        let mut caller_gone = false;
        let mut sequence = session.generation << 32;
        loop {
            let next = if batch.is_empty() {
                rx.recv().await
            } else {
                match tokio::time::timeout(Duration::from_millis(40), rx.recv()).await {
                    Ok(next) => next,
                    Err(_) => {
                        caller_gone |= !self
                            .flush(session, stop, &exec_id, &mut sequence, &mut batch)
                            .await;
                        size = 0;
                        continue;
                    }
                }
            };
            let Some(event) = next else { break };
            size += match event.event.as_ref() {
                Some(exec_event::Event::Stdout(bytes)) => bytes.len(),
                Some(exec_event::Event::Stderr(bytes)) => bytes.len(),
                _ => 0,
            };
            batch.push(event);
            if size >= 64 * 1024 {
                caller_gone |= !self
                    .flush(session, stop, &exec_id, &mut sequence, &mut batch)
                    .await;
                size = 0;
            }
        }
        if caller_gone {
            // Nobody is listening any more: stop the command instead of
            // letting it run unobserved.
            let _ = cancel_driver.cancel_exec(&native, &exec_id).await;
        }
        let code = run
            .await
            .map_err(|e| Error::internal(format!("exec task: {e}")))??;
        batch.push(ExecEvent::exit(code));
        if !caller_gone {
            self.flush(session, stop, &exec_id, &mut sequence, &mut batch)
                .await;
        }
        Ok(())
    }

    /// Report one batch, retrying until it is applied or the service fences
    /// this session. Returns false once output should stop being sent.
    async fn flush(
        &self,
        session: &Session,
        stop: &CancellationToken,
        exec_id: &str,
        sequence: &mut u64,
        batch: &mut Vec<ExecEvent>,
    ) -> bool {
        if batch.is_empty() {
            return true;
        }
        *sequence += 1;
        let sequence = *sequence;
        let events = std::mem::take(batch);
        let mut backoff = self.config.retry_backoff;
        loop {
            match self
                .link
                .report_exec(ReportExecRequest {
                    provider_id: session.provider_id.clone(),
                    generation: session.generation,
                    exec_id: exec_id.to_owned(),
                    events: events.clone(),
                    sequence,
                    ..ReportExecRequest::default()
                })
                .await
            {
                Ok(()) => return true,
                Err(error) if fenced(&error) => {
                    debug!(exec_id, %error, "exec output stopped");
                    return false;
                }
                Err(error) => {
                    debug!(exec_id, %error, "exec output not accepted; retrying");
                    if !self.wait_backoff(stop, backoff).await {
                        return false;
                    }
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
            }
        }
    }
}

/// The driver-native sandbox a work item addresses, when it names one. A
/// `CreateWork` does not: the driver picks the native ref and returns it in
/// the report.
fn item_native_ref(item: &WorkItem) -> Option<&str> {
    match item.work.as_ref()? {
        work_item::Work::Create(_) => None,
        work_item::Work::Destroy(target)
        | work_item::Work::Pause(target)
        | work_item::Work::Resume(target)
        | work_item::Work::Stop(target) => Some(target.native_ref.as_str()),
        work_item::Work::CancelExec(cancel) => Some(cancel.target.native_ref.as_str()),
        work_item::Work::Exec(exec) => Some(exec.target.native_ref.as_str()),
        work_item::Work::Copy(copy) => Some(copy.target.native_ref.as_str()),
        work_item::Work::Snapshot(snapshot) => Some(snapshot.target.native_ref.as_str()),
        work_item::Work::Start(start) => Some(start.target.native_ref.as_str()),
    }
}

/// Order a heartbeat's runtime report most-recently-active first and trim it
/// to [`MAX_HEARTBEAT_SANDBOX_STATS`], returning the number of entries
/// dropped. Activity comes from the work this agent has handled; sandboxes it
/// has never touched fall back to newest start time. Activity for sandboxes
/// that no longer report is forgotten, so the map stays bounded by the host's
/// live set rather than every native ref ever seen.
fn cap_heartbeat_sandboxes(
    sandboxes: &mut Vec<SandboxRuntimeStats>,
    active: &mut HashMap<String, Instant>,
) -> usize {
    let reported: HashSet<&str> = sandboxes
        .iter()
        .map(|stats| stats.native_ref.as_str())
        .collect();
    active.retain(|native_ref, _| reported.contains(native_ref.as_str()));
    drop(reported);
    let active = &*active;
    sandboxes.sort_by(|a, b| {
        match (active.get(&a.native_ref), active.get(&b.native_ref)) {
            (Some(a_at), Some(b_at)) => b_at.cmp(a_at),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => b.start_time.cmp(&a.start_time),
        }
        .then_with(|| a.native_ref.cmp(&b.native_ref))
    });
    let dropped = sandboxes.len().saturating_sub(MAX_HEARTBEAT_SANDBOX_STATS);
    sandboxes.truncate(MAX_HEARTBEAT_SANDBOX_STATS);
    dropped
}

/// Forget completed work ids after [`DONE_RETENTION`]; running ones stay.
fn prune_work(work: &mut HashMap<String, (WorkState, Instant)>) {
    work.retain(|_, (state, at)| match state {
        WorkState::Running => true,
        WorkState::Done(_) => at.elapsed() < DONE_RETENTION,
    });
}
#[cfg(test)]
mod tests {
    use super::*;

    fn create(mounts: Vec<Mount>, sockets: Vec<SocketMount>) -> CreateWork {
        CreateWork {
            sandbox_id: "sbx-test".into(),
            driver: "fake".into(),
            mounts,
            sockets,
            ..CreateWork::default()
        }
    }

    fn mount(host: &Path) -> Mount {
        Mount {
            host: host.display().to_string(),
            guest: "/mnt".into(),
            readonly: true,
            ..Mount::default()
        }
    }

    fn socket(host: &Path) -> SocketMount {
        SocketMount {
            host: host.display().to_string(),
            guest: "/run/socket".into(),
            ..SocketMount::default()
        }
    }

    fn text(error: &Error) -> &str {
        crate::error::message(error)
    }

    fn stats(native_ref: impl Into<String>, start_time: i64) -> SandboxRuntimeStats {
        SandboxRuntimeStats {
            native_ref: native_ref.into(),
            start_time: Some(start_time),
            ..SandboxRuntimeStats::default()
        }
    }

    #[test]
    fn jitter_stays_inside_the_backoff_window() {
        for base in [
            Duration::from_millis(1),
            Duration::from_millis(500),
            Duration::from_secs(15),
        ] {
            for _ in 0..100 {
                let delay = jittered(base);
                assert!(delay >= base / 2, "{delay:?} < {:?}", base / 2);
                assert!(delay <= base, "{delay:?} > {base:?}");
            }
        }
    }

    #[test]
    fn empty_allowlist_denies_every_mount() {
        let dir = tempfile::tempdir().unwrap();
        let access = HostAccess::default();
        let error = access
            .check_create(&create(vec![mount(dir.path())], vec![]))
            .unwrap_err();
        assert_eq!(error.code, Code::FailedPrecondition);
        assert!(
            text(&error).contains("exports no mount paths"),
            "{}",
            text(&error)
        );
        let error = access
            .check_create(&create(vec![], vec![socket(dir.path())]))
            .unwrap_err();
        assert!(
            text(&error).contains("exports no socket paths"),
            "{}",
            text(&error)
        );
        // No mounts means nothing to check.
        access.check_create(&create(vec![], vec![])).unwrap();
    }

    #[test]
    fn prefixes_allow_the_tree_and_nothing_outside_it() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        let child = allowed.join("child");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&child).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let access = HostAccess {
            mounts: vec![allowed.clone()],
            sockets: vec![allowed.clone()],
        };
        access
            .check_create(&create(vec![mount(&allowed)], vec![]))
            .unwrap();
        access
            .check_create(&create(vec![mount(&child)], vec![]))
            .unwrap();
        access
            .check_create(&create(vec![], vec![socket(&allowed)]))
            .unwrap();
        let error = access
            .check_create(&create(vec![mount(&outside)], vec![]))
            .unwrap_err();
        assert_eq!(error.code, Code::FailedPrecondition);
        assert!(
            text(&error).contains("outside this agent's allowlist"),
            "{}",
            text(&error)
        );
        // `..` resolves before the prefix check, so it cannot escape.
        let escape = allowed.join("..").join("outside");
        let error = access
            .check_create(&create(vec![mount(&escape)], vec![]))
            .unwrap_err();
        assert!(
            text(&error).contains("outside this agent's allowlist"),
            "{}",
            text(&error)
        );
        // A symlink inside the tree that points out of it is denied too.
        #[cfg(unix)]
        {
            let link = allowed.join("link");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            let error = access
                .check_create(&create(vec![mount(&link)], vec![]))
                .unwrap_err();
            assert!(
                text(&error).contains("outside this agent's allowlist"),
                "{}",
                text(&error)
            );
        }
        // A path that does not exist cannot be resolved, so it is denied.
        let error = access
            .check_create(&create(vec![mount(&allowed.join("missing"))], vec![]))
            .unwrap_err();
        assert!(
            text(&error).contains("cannot be resolved"),
            "{}",
            text(&error)
        );
    }

    #[test]
    fn heartbeat_report_is_capped_and_orders_activity_first() {
        let now = Instant::now();
        let mut active = HashMap::new();
        // 600 sandboxes, all created in the same second, so start time alone
        // cannot separate them; the ten most recently used must survive.
        let mut sandboxes: Vec<SandboxRuntimeStats> = (0..600)
            .map(|i| stats(format!("sbx-{i:03}"), 1_000))
            .collect();
        for (offset, i) in (590..600).enumerate() {
            active.insert(
                format!("sbx-{i:03}"),
                now - Duration::from_secs(60 - offset as u64),
            );
        }
        // A sandbox that is gone must not keep its activity entry.
        active.insert("sbx-gone".to_owned(), now);

        let dropped = cap_heartbeat_sandboxes(&mut sandboxes, &mut active);

        assert_eq!(dropped, 600 - MAX_HEARTBEAT_SANDBOX_STATS);
        assert_eq!(sandboxes.len(), MAX_HEARTBEAT_SANDBOX_STATS);
        // Newest activity first: offset 9 (sbx-599) was touched last.
        assert_eq!(sandboxes[0].native_ref, "sbx-599");
        assert_eq!(sandboxes[9].native_ref, "sbx-590");
        // Never-touched sandboxes follow, newest start time first, then by
        // native ref for a stable report.
        assert_eq!(sandboxes[10].native_ref, "sbx-000");
        assert!(!active.contains_key("sbx-gone"));
        assert!(!active.contains_key("sbx-000"));
        assert_eq!(active.len(), 10);
    }

    #[test]
    fn heartbeat_report_keeps_everything_under_the_cap() {
        let mut active = HashMap::new();
        let mut sandboxes: Vec<SandboxRuntimeStats> = (0..MAX_HEARTBEAT_SANDBOX_STATS)
            .map(|i| stats(format!("sbx-{i:03}"), 1_000))
            .collect();
        assert_eq!(cap_heartbeat_sandboxes(&mut sandboxes, &mut active), 0);
        assert_eq!(sandboxes.len(), MAX_HEARTBEAT_SANDBOX_STATS);
    }

    #[test]
    fn work_items_name_their_native_sandbox() {
        let target = SandboxWork {
            sandbox_id: "sbx-1".into(),
            driver: "fake".into(),
            native_ref: "native-1".into(),
            ..SandboxWork::default()
        };
        let exec = WorkItem::new(
            "w1",
            work_item::Work::Exec(Box::new(ExecWork {
                exec_id: "e1".into(),
                target: target.clone().into(),
                request: ExecRequest::default().into(),
                ..ExecWork::default()
            })),
        );
        assert_eq!(item_native_ref(&exec), Some("native-1"));
        assert_eq!(item_native_ref(&WorkItem::default()), None);
        let create = WorkItem::new("w2", work_item::Work::Create(Box::default()));
        assert_eq!(item_native_ref(&create), None);
    }

    #[test]
    fn fenced_errors_end_retries() {
        assert!(fenced(&Error::aborted("stale generation")));
        assert!(fenced(&Error::not_found("exec not found")));
        assert!(!fenced(&Error::unavailable("try again")));
    }

    #[test]
    fn certificate_failures_back_off_further_than_transport_errors() {
        let unauthenticated = Error::new(Code::Unauthenticated, "different CA");
        let denied = Error::new(Code::PermissionDenied, "not enrolled");
        let unavailable = Error::new(Code::Unavailable, "connection refused");
        assert_eq!(
            registration_delay(REGISTER_BACKOFF_MIN, Some(&unauthenticated)),
            REGISTER_AUTH_BACKOFF
        );
        assert_eq!(
            registration_delay(REGISTER_BACKOFF_MIN, Some(&denied)),
            REGISTER_AUTH_BACKOFF
        );
        assert_eq!(
            registration_delay(REGISTER_BACKOFF_MIN, Some(&unavailable)),
            REGISTER_BACKOFF_MIN
        );
        // A session that ended after a successful registration is not an
        // identity problem: retry on the ordinary cadence.
        assert_eq!(
            registration_delay(REGISTER_BACKOFF_MIN, None),
            REGISTER_BACKOFF_MIN
        );
        let doubled = (REGISTER_BACKOFF_MIN * 2).min(REGISTER_BACKOFF_MAX);
        assert_eq!(registration_delay(doubled, Some(&unavailable)), doubled);
    }
}
