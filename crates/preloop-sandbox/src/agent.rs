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

/// Remote link over Connect + HTTP blobs.
pub struct RemoteLink {
    client: connect::Client,
}

impl RemoteLink {
    /// `base` like `https://sandbox.example:9191`, `token` the provider token.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            client: connect::Client::new(base, token),
        }
    }

    fn blob_request(
        &self,
        method: reqwest::Method,
        id: &Session,
        ticket: &str,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .http()
            .request(
                method,
                format!("{}/sandbox/v1/agent/blobs/{ticket}", self.client.base()),
            )
            .header("x-provider-id", &id.provider_id)
            .header("x-provider-generation", id.generation.to_string());
        if let Some(token) = self.client.token() {
            request = request.bearer_auth(token);
        }
        request
    }
}

const PROVIDERS: &str = "preloop.sandbox.v1.Providers";

#[async_trait]
impl ProviderLink for RemoteLink {
    async fn register(&self, request: RegisterRequest) -> Result<RegisterResponse> {
        self.client
            .unary(&format!("{PROVIDERS}/Register"), &request)
            .await
    }
    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        self.client
            .unary::<_, Empty>(&format!("{PROVIDERS}/Heartbeat"), &request)
            .await
            .map(|_| ())
    }
    async fn poll(&self, request: PollRequest) -> Result<PollResponse> {
        self.client
            .unary(&format!("{PROVIDERS}/Poll"), &request)
            .await
    }
    async fn report(&self, request: ReportRequest) -> Result<()> {
        self.client
            .unary::<_, Empty>(&format!("{PROVIDERS}/Report"), &request)
            .await
            .map(|_| ())
    }
    async fn report_exec(&self, request: ReportExecRequest) -> Result<()> {
        self.client
            .unary::<_, Empty>(&format!("{PROVIDERS}/ReportExec"), &request)
            .await
            .map(|_| ())
    }
    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        self.client
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
        return Err(Error::precondition(format!("host {kind} path is empty")));
    }
    if allowed.is_empty() {
        return Err(Error::precondition(format!(
            "host {kind} `{host}` is not allowed: this agent exports no {kind} paths"
        )));
    }
    let resolved = std::fs::canonicalize(host).map_err(|error| {
        Error::precondition(format!("host {kind} `{host}` cannot be resolved: {error}"))
    })?;
    let permitted = allowed.iter().any(|prefix| {
        std::fs::canonicalize(prefix).is_ok_and(|prefix| resolved.starts_with(prefix))
    });
    if permitted {
        Ok(())
    } else {
        Err(Error::precondition(format!(
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
        }
    }

    /// Set the host access allowlist.
    pub fn with_access(mut self, access: HostAccess) -> Self {
        self.access = access;
        self
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
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        std::fs::create_dir_all(&self.config.scratch).ok();
        let mut backoff = Duration::from_millis(500);
        while !shutdown.is_cancelled() {
            match self.register().await {
                Ok(session) => {
                    info!(provider = %session.provider_id, generation = session.generation, "agent registered");
                    backoff = Duration::from_millis(500);
                    let error = self.clone().session(session, shutdown.clone()).await;
                    if let Some(error) = error {
                        warn!(%error, "agent session ended; re-registering");
                    }
                }
                Err(error) => warn!(%error, "agent registration failed"),
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
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
            // Admission before the driver: no backend sees a host path this
            // agent does not export. Fork acquires carry their mounts here
            // too, so they are checked the same way.
            self.config.access.check_create(&create)?;
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
            let result = match copy.kind {
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
            };
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
            // `Start` boots an existing machine and carries no host mounts;
            // a fork is a `Create` and took the allowlist check above.
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

    fn create(mounts: Vec<Mount>, sockets: Vec<SocketMount>) -> CreateWork {
        CreateWork {
            sandbox_id: "sbx-test".into(),
            incarnation: 1,
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
        }
    }

    fn socket(host: &Path) -> SocketMount {
        SocketMount {
            host: host.display().to_string(),
            guest: "/run/socket".into(),
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
            error.message.contains("exports no mount paths"),
            "{}",
            error.message
        );
        let error = access
            .check_create(&create(vec![], vec![socket(dir.path())]))
            .unwrap_err();
        assert!(
            error.message.contains("exports no socket paths"),
            "{}",
            error.message
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
            error.message.contains("outside this agent's allowlist"),
            "{}",
            error.message
        );
        // `..` resolves before the prefix check, so it cannot escape.
        let escape = allowed.join("..").join("outside");
        let error = access
            .check_create(&create(vec![mount(&escape)], vec![]))
            .unwrap_err();
        assert!(
            error.message.contains("outside this agent's allowlist"),
            "{}",
            error.message
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
                error.message.contains("outside this agent's allowlist"),
                "{}",
                error.message
            );
        }
        // A path that does not exist cannot be resolved, so it is denied.
        let error = access
            .check_create(&create(vec![mount(&allowed.join("missing"))], vec![]))
            .unwrap_err();
        assert!(
            error.message.contains("cannot be resolved"),
            "{}",
            error.message
        );
    }
}
