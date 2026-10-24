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
        let mut backoff = Duration::from_millis(500);
        let mut pending = initial;
        while !shutdown.is_cancelled() {
            let session = match pending.take() {
                Some(session) => session,
                None => match self.register().await {
                    Ok(session) => session,
                    Err(error) => {
                        warn!(%error, "agent registration failed");
                        tokio::select! {
                            _ = shutdown.cancelled() => return,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(15));
                        continue;
                    }
                },
            };
            info!(provider = %session.provider_id, generation = session.generation, "agent registered");
            backoff = Duration::from_millis(500);
            let error = self.clone().session(session, shutdown.clone()).await;
            if let Some(error) = error {
                warn!(%error, "agent session ended; re-registering");
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
