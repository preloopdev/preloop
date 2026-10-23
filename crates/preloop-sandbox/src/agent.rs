//! Host agent: registers local drivers with a Sandbox Service, long-polls
//! for work, runs it, and reports back fenced by its generation.
//!
//! The same agent runs in-process (local `preloop serve`) and on remote
//! hosts (a Mac Studio behind NAT dials out; the service never connects in).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::api::*;
use crate::connect::{self, Code, Error, Result};
use crate::driver::Driver;
use crate::service::Service;

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
        let path = self.agent_blob(&id.provider_id, id.generation, ticket)?;
        tokio::fs::copy(src, &path)
            .await
            .map(|_| ())
            .map_err(|e| Error::internal(format!("blob put: {e}")))
    }
}

/// Remote link over Connect + HTTPS blobs, authenticated by the client
/// certificate issued at enrollment.
#[derive(Debug)]
pub struct RemoteLink {
    base: String,
    http: parking_lot::RwLock<reqwest::Client>,
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
        return Err(Error::invalid(
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
        let http = crate::tls::http_client(&ca_pem, None)?;
        let response: EnrollResponse = connect::Client::with_http(base.clone(), None, http)
            .unary(
                &format!("{PROVIDERS}/Enroll"),
                &EnrollRequest {
                    token: token.to_owned(),
                    name: name.to_owned(),
                    csr_pem,
                },
            )
            .await?;
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
            Error::invalid("`--join` is required on the first run (no stored identity)")
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
        Ok(Self {
            base: identity.metadata.base.clone(),
            http: parking_lot::RwLock::new(http),
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

    fn client(&self) -> connect::Client {
        connect::Client::with_http(self.base.clone(), None, self.http.read().clone())
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
        let response: RenewCertificateResponse = self
            .client()
            .unary(
                &format!("{PROVIDERS}/RenewCertificate"),
                &RenewCertificateRequest { csr_pem },
            )
            .await?;
        let next = identity.renewed(response.cert_pem, key_pem, unix_now(), response.expires_at);
        let http = crate::tls::http_client(&next.ca_pem, Some(&next.identity_pem()))?;
        next.save()?;
        *self.http.write() = http;
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

const PROVIDERS: &str = "preloop.sandbox.v1.Providers";

#[async_trait]
impl ProviderLink for RemoteLink {
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        self.client()
            .unary(&format!("{PROVIDERS}/Register"), &request)
            .await
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        // Heartbeats are the agent's slowest reliable tick: renew here so a
        // long-polling agent still rotates its certificate in time.
        self.maybe_renew().await;
        self.client()
            .unary::<_, Empty>(&format!("{PROVIDERS}/Heartbeat"), &request)
            .await
            .map(|_| ())
    }
    async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        self.client()
            .unary(&format!("{PROVIDERS}/Poll"), &request)
            .await
    }
    async fn report(&self, request: ReportRequest) -> Result<()> {
        self.client()
            .unary::<_, Empty>(&format!("{PROVIDERS}/Report"), &request)
            .await
            .map(|_| ())
    }
    async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        self.client()
            .unary::<_, Empty>(&format!("{PROVIDERS}/ReportExec"), &request)
            .await
            .map(|_| ())
    }
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        self.client()
            .unary(&format!("{PROVIDERS}/Reconcile"), &request)
            .await
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
}

impl AgentConfig {
    /// Defaults.
    pub fn new(name: impl Into<String>, scratch: PathBuf) -> Self {
        Self {
            name: name.into(),
            scratch,
            heartbeat: Duration::from_secs(10),
            reconcile: Duration::from_secs(60),
        }
    }
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
}

impl Agent {
    /// Build an agent over drivers.
    pub fn new(
        link: Arc<dyn ProviderLink>,
        drivers: Vec<Arc<dyn Driver>>,
        config: AgentConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            link,
            drivers: drivers
                .into_iter()
                .map(|driver| (driver.name().to_owned(), driver))
                .collect(),
            config,
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
                        capabilities: driver.capabilities(),
                    })
                    .collect(),
            })
            .await?;
        Ok(Session {
            provider_id: response.provider_id,
            generation: response.generation,
        })
    }

    /// Run until `shutdown`, re-registering after fencing or link failures.
    ///
    /// `initial` is a session the caller already registered (so a process
    /// that needs the provider id before serving does not register twice and
    /// fence its own in-flight work). After that session ends the agent
    /// registers again.
    pub async fn run(self: Arc<Self>, initial: Option<Session>, shutdown: CancellationToken) {
        std::fs::create_dir_all(&self.config.scratch).ok();
        // A previous process may have died mid-transfer: its archives are
        // neither needed nor safe to keep.
        if let Ok(entries) = std::fs::read_dir(&self.config.scratch) {
            for entry in entries.flatten() {
                if entry.path().is_file() {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        let mut backoff = REGISTER_BACKOFF_MIN;
        let mut pending = initial;
        while !shutdown.is_cancelled() {
            let mut failed = None;
            let session = match pending.take() {
                Some(session) => Some(session),
                None => match self.register().await {
                    Ok(session) => Some(session),
                    Err(error) => {
                        warn!(%error, "agent registration failed");
                        failed = Some(error);
                        None
                    }
                },
            };
            if let Some(session) = session {
                info!(provider = %session.provider_id, generation = session.generation, "agent registered");
                backoff = REGISTER_BACKOFF_MIN;
                let error = self.clone().session(session, shutdown.clone()).await;
                if let Some(error) = error {
                    warn!(%error, "agent session ended; re-registering");
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
                            if let Err(error) = agent.link.heartbeat(HeartbeatRequest {
                                provider_id: session.provider_id.clone(),
                                generation: session.generation,
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
        loop {
            let poll = self.link.poll(PollRequest {
                provider_id: session.provider_id.clone(),
                generation: session.generation,
                wait_secs: 25,
            });
            let response = tokio::select! {
                _ = stop.cancelled() => break,
                response = poll => response,
            };
            match response {
                Ok(response) => {
                    for item in response.work {
                        let agent = self.clone();
                        let session = session.clone();
                        tokio::spawn(async move { agent.handle(session, item).await });
                    }
                }
                Err(error) if error.code == Code::Aborted || error.code == Code::NotFound => {
                    result = Some(error);
                    break;
                }
                Err(error) => {
                    debug!(%error, "poll failed; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        stop.cancel();
        let _ = heartbeat.await;
        let _ = reconcile.await;
        result
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

    fn driver(&self, name: &str) -> Result<Arc<dyn Driver>> {
        self.drivers
            .get(name)
            .cloned()
            .ok_or_else(|| Error::precondition(format!("driver `{name}` is not on this host")))
    }

    async fn handle(self: Arc<Self>, session: Session, item: WorkItem) {
        let work_id = item.id.clone();
        let result = self.run_work(&session, item).await;
        let (ok, error, native_ref) = match result {
            Ok(native_ref) => (true, String::new(), native_ref),
            // Keep the code visible: an operator reading the service log can
            // tell "not supported" from "provider is down".
            Err(error) => (
                false,
                format!(
                    "{}: {}",
                    serde_json::to_value(error.code)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_default(),
                    error.message
                ),
                String::new(),
            ),
        };
        if !ok {
            warn!(%work_id, %error, "work failed");
        }
        if let Err(report_error) = self
            .link
            .report(ReportRequest {
                provider_id: session.provider_id,
                generation: session.generation,
                work_id,
                ok,
                error,
                native_ref,
            })
            .await
        {
            debug!(%report_error, "report rejected");
        }
    }

    async fn run_work(&self, session: &Session, item: WorkItem) -> Result<String> {
        if let Some(create) = item.create {
            return self.driver(&create.driver)?.create(&create).await;
        }
        if let Some(target) = item.destroy {
            let driver = self.driver(&target.driver)?;
            if let Some(snapshot) = target.native_ref.strip_prefix("snapshot:") {
                driver.drop_snapshot(snapshot).await?;
            } else {
                driver.destroy(&target.native_ref).await?;
            }
            return Ok(String::new());
        }
        if let Some(exec) = item.exec {
            self.run_exec(session, exec).await?;
            return Ok(String::new());
        }
        if let Some(copy) = item.copy {
            let driver = self.driver(&copy.target.driver)?;
            let scratch = self.config.scratch.join(format!("{}.tar", copy.ticket));
            // Clean the scratch archive however the transfer ends; a failed
            // fetch or copy must not leave credentials or guest bytes behind.
            let result = async {
                match copy.kind {
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
                    StreamKind::StreamKindUnspecified => Err(Error::invalid("copy kind unset")),
                }
            }
            .await;
            let _ = tokio::fs::remove_file(&scratch).await;
            return result.map(|_| String::new());
        }
        if let Some(snapshot) = item.snapshot {
            return self
                .driver(&snapshot.target.driver)?
                .snapshot(&snapshot.target.native_ref, &snapshot.template)
                .await;
        }
        if let Some(target) = item.stop {
            self.driver(&target.driver)?
                .stop(&target.native_ref)
                .await?;
            return Ok(String::new());
        }
        if let Some(start) = item.start {
            self.driver(&start.target.driver)?
                .start(&start.target.native_ref, start.forkable)
                .await?;
            return Ok(String::new());
        }
        if let Some(target) = item.pause {
            self.driver(&target.driver)?
                .pause(&target.native_ref)
                .await?;
            return Ok(String::new());
        }
        if let Some(target) = item.resume {
            self.driver(&target.driver)?
                .resume(&target.native_ref)
                .await?;
            return Ok(String::new());
        }
        Err(Error::new(Code::Unimplemented, "unknown work item"))
    }

    async fn run_exec(&self, session: &Session, exec: ExecWork) -> Result<()> {
        let driver = self.driver(&exec.target.driver)?;
        let (tx, mut rx) = mpsc::channel::<ExecEvent>(256);
        let native = exec.target.native_ref.clone();
        let request = exec.request.clone();
        let run = tokio::spawn(async move { driver.exec(&native, &request, tx).await });
        // Forward output in small batches.
        let mut batch = Vec::new();
        let mut size = 0usize;
        let mut caller_gone = false;
        loop {
            let next = if batch.is_empty() {
                rx.recv().await
            } else {
                match tokio::time::timeout(Duration::from_millis(40), rx.recv()).await {
                    Ok(next) => next,
                    Err(_) => {
                        caller_gone |= !self.flush(session, &exec.exec_id, &mut batch).await;
                        size = 0;
                        continue;
                    }
                }
            };
            let Some(event) = next else { break };
            size += event.stdout.as_ref().map_or(0, Vec::len)
                + event.stderr.as_ref().map_or(0, Vec::len);
            batch.push(event);
            if size >= 64 * 1024 {
                caller_gone |= !self.flush(session, &exec.exec_id, &mut batch).await;
                size = 0;
            }
        }
        let code = run
            .await
            .map_err(|e| Error::internal(format!("exec task: {e}")))??;
        batch.push(ExecEvent::exit(code));
        if !caller_gone {
            self.flush(session, &exec.exec_id, &mut batch).await;
        }
        Ok(())
    }

    /// Returns false once the caller has gone away.
    async fn flush(&self, session: &Session, exec_id: &str, batch: &mut Vec<ExecEvent>) -> bool {
        if batch.is_empty() {
            return true;
        }
        let events = std::mem::take(batch);
        match self
            .link
            .report_exec(ReportExecRequest {
                provider_id: session.provider_id.clone(),
                generation: session.generation,
                exec_id: exec_id.to_owned(),
                events,
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                debug!(%error, "exec output dropped");
                error.code != Code::NotFound
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    use crate::api::{CopyWork, PollResponse, ReconcileResponse, ReportExecRequest, ReportRequest};
    use crate::drivers::fake::FakeDriver;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct TestLink {
        registers: AtomicUsize,
        abort_polls: AtomicBool,
        fail_fetch: bool,
    }

    #[async_trait]
    impl ProviderLink for TestLink {
        async fn register(&self, _request: RegisterRequest) -> Result<RegisterResponse> {
            let generation = self.registers.fetch_add(1, Ordering::SeqCst) as u64 + 1;
            Ok(RegisterResponse {
                provider_id: "prv-test".into(),
                generation,
            })
        }
        async fn heartbeat(&self, _request: HeartbeatRequest) -> Result<()> {
            Ok(())
        }
        async fn poll(&self, _request: PollRequest) -> Result<PollResponse> {
            if self.abort_polls.swap(false, Ordering::SeqCst) {
                return Err(Error::aborted("fenced"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(PollResponse::default())
        }
        async fn report(&self, _request: ReportRequest) -> Result<()> {
            Ok(())
        }
        async fn report_exec(&self, _request: ReportExecRequest) -> Result<()> {
            Ok(())
        }
        async fn reconcile(&self, _request: ReconcileRequest) -> Result<ReconcileResponse> {
            Ok(ReconcileResponse::default())
        }
        async fn fetch_blob(&self, _id: &Session, _ticket: &str, _dest: &Path) -> Result<()> {
            if self.fail_fetch {
                Err(Error::unavailable("fetch failed"))
            } else {
                Ok(())
            }
        }
        async fn put_blob(&self, _id: &Session, _ticket: &str, _src: &Path) -> Result<()> {
            Ok(())
        }
    }

    fn session() -> Session {
        Session {
            provider_id: "prv-test".into(),
            generation: 1,
        }
    }

    async fn agent(link: Arc<TestLink>, scratch: &Path) -> Arc<Agent> {
        Agent::new(
            link as Arc<dyn ProviderLink>,
            Vec::new(),
            AgentConfig::new("test", scratch.to_owned()),
        )
    }

    #[tokio::test]
    async fn an_initial_session_is_not_registered_twice() {
        let dir = tempfile::tempdir().unwrap();
        let link = Arc::new(TestLink::default());
        let agent = agent(link.clone(), &dir.path().join("scratch")).await;
        let initial = agent.register().await.unwrap();
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(agent.run(Some(initial), shutdown.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(link.registers.load(Ordering::SeqCst), 1);
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn a_fenced_session_registers_again() {
        let dir = tempfile::tempdir().unwrap();
        let link = Arc::new(TestLink::default());
        let agent = agent(link.clone(), &dir.path().join("scratch")).await;
        let initial = agent.register().await.unwrap();
        link.abort_polls.store(true, Ordering::SeqCst);
        let shutdown = CancellationToken::new();
        let run = tokio::spawn(agent.run(Some(initial), shutdown.clone()));
        for _ in 0..100 {
            if link.registers.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            link.registers.load(Ordering::SeqCst) >= 2,
            "the agent must re-register after a fenced session"
        );
        shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), run).await;
    }

    #[tokio::test]
    async fn failed_transfers_leave_no_scratch_archive() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        let ticket = "tkt-test";
        let archive = scratch.join(format!("{ticket}.tar"));
        std::fs::write(&archive, b"partial download").unwrap();
        let link = Arc::new(TestLink {
            fail_fetch: true,
            ..TestLink::default()
        });
        let fake = Arc::new(FakeDriver::new(
            "fake",
            dir.path().join("host"),
            crate::api::Capabilities::default(),
        ));
        let agent = Agent::new(
            link as Arc<dyn ProviderLink>,
            vec![fake as Arc<dyn Driver>],
            AgentConfig::new("test", scratch.clone()),
        );
        let error = agent
            .run_work(
                &session(),
                WorkItem {
                    id: "wrk-test".into(),
                    copy: Some(CopyWork {
                        target: crate::api::SandboxWork {
                            sandbox_id: "sbx-test".into(),
                            driver: "fake".into(),
                            native_ref: "native-test".into(),
                        },
                        ticket: ticket.into(),
                        path: "/work".into(),
                        kind: StreamKind::Upload,
                    }),
                    ..WorkItem::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::Unavailable);
        assert!(
            !archive.exists(),
            "the scratch archive outlived the failure"
        );
    }

    #[tokio::test]
    async fn startup_sweeps_stale_scratch_archives() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        let stale = scratch.join("tkt-stale.tar");
        std::fs::write(&stale, b"stale").unwrap();
        let link = Arc::new(TestLink::default());
        let agent = agent(link, &scratch).await;
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        agent.run(None, shutdown).await;
        assert!(!stale.exists(), "startup must sweep old scratch archives");
    }
}
