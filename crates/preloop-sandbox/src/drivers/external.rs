//! Drivers that live in another process.
//!
//! An external driver is a local plugin process that implements
//! `preloop.sandbox.driver.v1.Driver` (see
//! `proto/preloop/sandbox/driver/v1/driver.proto`). The agent speaks Connect
//! over HTTP/2 cleartext to the plugin on a Unix socket that only the agent
//! can reach, then exposes it to the rest of the host agent through the same
//! [`Driver`] trait as an in-process backend.
//!
//! A plugin is either spawned by the agent (`name=/path/to/binary [args…]`)
//! or already listening on a socket (`name=unix:///path/to.sock`). Spawned
//! plugins are supervised: when the process exits the agent restarts it with
//! exponential backoff, and calls fail `unavailable` until it is back, so a
//! reconcile round never mistakes a restart for every sandbox being gone.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use connectrpc::client::{ClientConfig, Http2Connection, SharedHttp2Connection};
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use crate::api::{Capabilities, CreateWork, ExecEvent, ExecRequest, HostReport, Observed};
use crate::driver::{DRIVER_PROTOCOL_VERSION, Driver};
use crate::error::{Code, Error, Result};
use crate::generated::preloop::sandbox::driver::v1 as proto;

/// The HTTP/2 `:authority` the plugin sees. IPC sockets do not validate it.
const AUTHORITY: &str = "http://localhost";

/// How long [`ExternalDriver::start`] waits for the plugin's first `GetInfo`.
const GET_INFO_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay between `GetInfo` probes while the plugin is starting.
const GET_INFO_RETRY: Duration = Duration::from_millis(100);

/// First restart delay; doubles up to [`RESTART_BACKOFF_MAX`].
const RESTART_BACKOFF_START: Duration = Duration::from_secs(1);

/// Ceiling for the restart delay.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Request buffer for the shared HTTP/2 connection.
const CONNECTION_BUFFER: usize = 64;

type Client = proto::DriverClient<SharedHttp2Connection>;

/// How to reach an external driver plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalSpec {
    /// Driver name reported to the service (`acme`) and used for the socket
    /// file name in spawn mode.
    pub name: String,
    /// Where the plugin is.
    pub target: ExternalTarget,
}

/// Where an external driver plugin lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalTarget {
    /// Start this binary (with these arguments) and talk to the socket the
    /// agent appends as `--socket <path>`.
    Spawn(PathBuf, Vec<String>),
    /// Talk to a plugin that is already listening on this socket.
    Socket(PathBuf),
}

impl ExternalSpec {
    /// Parse `name=/path/to/binary [args…]` or `name=unix:///path/to.sock`.
    pub fn parse(spec: &str) -> Result<Self> {
        let invalid = |detail: &str| {
            Error::invalid_argument(format!(
                "external driver spec `{spec}` {detail}; expected \
                 `name=/path/to/plugin [args...]` or `name=unix:///path/to.sock`"
            ))
        };
        let (name, target) = spec.split_once('=').ok_or_else(|| invalid("has no `=`"))?;
        let name = name.trim();
        if !valid_name(name) {
            return Err(invalid(
                "has an invalid driver name (letters, digits, `-`, `_` and `.` only)",
            ));
        }
        let target = target.trim();
        if target.is_empty() {
            return Err(invalid("has an empty target"));
        }
        let target = match target.strip_prefix("unix://") {
            Some("") => return Err(invalid("names an empty socket path")),
            Some(path) => ExternalTarget::Socket(PathBuf::from(path)),
            None => {
                let mut words = target.split_whitespace();
                let binary = words
                    .next()
                    .ok_or_else(|| invalid("has an empty command"))?;
                ExternalTarget::Spawn(PathBuf::from(binary), words.map(str::to_owned).collect())
            }
        };
        Ok(Self {
            name: name.to_owned(),
            target,
        })
    }
}

/// Whether `name` is usable as a driver name and socket file stem.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// An in-process [`Driver`] backed by an out-of-process plugin.
pub struct ExternalDriver {
    /// Stable identity reported by `GetInfo` at start. `Driver::name` borrows
    /// it, and a driver's name must not change across a restart (it is how
    /// the service addresses this backend).
    name: String,
    shared: Arc<Shared>,
}

impl ExternalDriver {
    /// Start and verify a plugin, then expose it as a driver.
    ///
    /// Spawn-mode plugins are started with `--socket
    /// <runtime_dir>/<name>.sock`; `runtime_dir` is created with mode 0700.
    /// Socket-mode plugins are dialed where the spec says. In both modes this
    /// fails unless the plugin answers `GetInfo` with this agent's
    /// [`DRIVER_PROTOCOL_VERSION`].
    pub async fn start(spec: ExternalSpec, runtime_dir: &Path) -> Result<Arc<Self>> {
        let socket = match &spec.target {
            ExternalTarget::Spawn(..) => {
                prepare_runtime_dir(runtime_dir)?;
                runtime_dir.join(format!("{}.sock", spec.name))
            }
            ExternalTarget::Socket(path) => path.clone(),
        };
        let shared = Arc::new(Shared {
            spec_name: spec.name.clone(),
            target: spec.target,
            socket,
            state: RwLock::default(),
            connect: Mutex::new(()),
            capabilities: parking_lot::RwLock::new(Capabilities::default()),
            shutdown: CancellationToken::new(),
            reconnecting: AtomicBool::new(false),
        });
        let mut spawned = None;
        let (client, info) = match &shared.target {
            ExternalTarget::Spawn(..) => {
                let mut child = spawn_child(&shared)?;
                match wait_until_ready(&shared, Some(&mut child)).await {
                    Ok(ready) => {
                        spawned = Some(child);
                        ready
                    }
                    Err(error) => {
                        // Reap the process instead of leaving a zombie behind a
                        // refused start.
                        let _ = child.kill().await;
                        return Err(error);
                    }
                }
            }
            ExternalTarget::Socket(_) => wait_until_ready(&shared, None).await?,
        };
        shared.install(client, &info).await;
        // Supervise only once the ready client is installed, so an immediate
        // exit cannot race the first install.
        if let Some(child) = spawned {
            tokio::spawn(supervise(shared.clone(), child));
        }
        tracing::info!(
            driver = %shared.spec_name,
            socket = %shared.socket.display(),
            plugin_version = %info.plugin_version,
            "external driver ready"
        );
        Ok(Arc::new(Self {
            name: if info.name.is_empty() {
                shared.spec_name.clone()
            } else {
                info.name
            },
            shared,
        }))
    }

    /// The connected client, failing `unavailable` while the plugin is down.
    async fn client(&self) -> Result<Client> {
        if let Some(client) = self.shared.ready_client().await {
            return Ok(client);
        }
        // Only socket mode reaches here: spawned plugins are installed by
        // `start` and marked down (never silently unconnected) by the
        // supervisor.
        let _guard = self.shared.connect.lock().await;
        if let Some(client) = self.shared.ready_client().await {
            return Ok(client);
        }
        if let Some(reason) = self.shared.down_reason().await {
            return Err(Error::unavailable(reason));
        }
        let client = new_client(&self.shared.socket);
        let info = get_info(&client, &self.shared.spec_name).await?;
        self.shared.install(client.clone(), &info).await;
        Ok(client)
    }

    /// Await one RPC result, marking the plugin down after a transport loss.
    async fn call<T>(
        &self,
        call: impl Future<Output = std::result::Result<T, Error>>,
    ) -> Result<T> {
        match call.await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.note_failure(&error).await;
                Err(error)
            }
        }
    }

    /// A transport failure in socket mode means the plugin restarted (or is
    /// gone); reconnect in the background while calls fail fast.
    async fn note_failure(&self, error: &Error) {
        if error.code != Code::Unavailable {
            return;
        }
        let message = error.message.as_deref().unwrap_or("transport failure");
        self.shared
            .mark_down_and_reconnect(format!(
                "external driver `{}` unavailable: {message}",
                self.shared.spec_name
            ))
            .await;
    }
}

#[async_trait]
impl Driver for ExternalDriver {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> Capabilities {
        self.shared.capabilities.read().clone()
    }

    async fn create(&self, work: &CreateWork) -> Result<String> {
        let client = self.client().await?;
        let response = self
            .call(client.create(proto::CreateRequest {
                work: work.clone().into(),
                ..proto::CreateRequest::default()
            }))
            .await?;
        Ok(response.into_owned().native_ref)
    }

    async fn destroy(&self, native_ref: &str) -> Result<()> {
        let client = self.client().await?;
        match self
            .call(client.destroy(proto::NativeRequest {
                native_ref: native_ref.to_owned(),
                ..proto::NativeRequest::default()
            }))
            .await
        {
            Ok(_) => Ok(()),
            // Destroying something already gone succeeds, even if a plugin
            // reports it as `not_found`.
            Err(error) if error.code == Code::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32> {
        let client = self.client().await?;
        let mut stream = self
            .call(client.exec(proto::ExecRequest {
                native_ref: native_ref.to_owned(),
                exec_id: exec_id.to_owned(),
                request: request.clone().into(),
                ..proto::ExecRequest::default()
            }))
            .await?;
        let mut exit = None;
        loop {
            let message = match stream.message::<ExecEvent>().await {
                Ok(message) => message,
                Err(error) => {
                    self.note_failure(&error).await;
                    return Err(error);
                }
            };
            let Some(message) = message else {
                break;
            };
            let event = message.to_owned_message();
            if let Some(crate::api::exec_event::Event::ExitCode(code)) = event.event {
                exit = Some(code);
                continue;
            }
            // The agent cancels the command through `CancelExec` when the
            // caller goes away; keep draining so the exit status still
            // arrives.
            let _ = output.send(event).await;
        }
        exit.ok_or_else(|| {
            Error::internal(format!(
                "external driver `{}` ended exec `{exec_id}` without an exit status",
                self.shared.spec_name
            ))
        })
    }

    async fn cancel_exec(&self, native_ref: &str, exec_id: &str) -> Result<()> {
        let client = self.client().await?;
        self.call(client.cancel_exec(proto::CancelExecRequest {
            native_ref: native_ref.to_owned(),
            exec_id: exec_id.to_owned(),
            ..proto::CancelExecRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn copy_in(&self, native_ref: &str, tar: &Path, guest_dir: &str) -> Result<()> {
        let client = self.client().await?;
        self.call(client.copy_in(proto::CopyInRequest {
            native_ref: native_ref.to_owned(),
            host_tar: path_str(tar)?.to_owned(),
            guest_dir: guest_dir.to_owned(),
            ..proto::CopyInRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn copy_out(&self, native_ref: &str, guest_path: &str, tar: &Path) -> Result<()> {
        let client = self.client().await?;
        self.call(client.copy_out(proto::CopyOutRequest {
            native_ref: native_ref.to_owned(),
            guest_path: guest_path.to_owned(),
            host_tar: path_str(tar)?.to_owned(),
            ..proto::CopyOutRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn snapshot(&self, native_ref: &str, template: &str) -> Result<String> {
        let client = self.client().await?;
        let response = self
            .call(client.snapshot(proto::SnapshotRequest {
                native_ref: native_ref.to_owned(),
                template: template.to_owned(),
                ..proto::SnapshotRequest::default()
            }))
            .await?;
        Ok(response.into_owned().native_ref)
    }

    async fn drop_snapshot(&self, native_ref: &str) -> Result<()> {
        let client = self.client().await?;
        match self
            .call(client.drop_snapshot(proto::NativeRequest {
                native_ref: native_ref.to_owned(),
                ..proto::NativeRequest::default()
            }))
            .await
        {
            Ok(_)
            | Err(Error {
                code: Code::NotFound,
                ..
            }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn stop(&self, native_ref: &str) -> Result<()> {
        let client = self.client().await?;
        self.call(client.stop(proto::NativeRequest {
            native_ref: native_ref.to_owned(),
            ..proto::NativeRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn start(&self, native_ref: &str, forkable: bool) -> Result<()> {
        let client = self.client().await?;
        self.call(client.start(proto::StartRequest {
            native_ref: native_ref.to_owned(),
            forkable,
            ..proto::StartRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn pause(&self, native_ref: &str) -> Result<()> {
        let client = self.client().await?;
        self.call(client.pause(proto::NativeRequest {
            native_ref: native_ref.to_owned(),
            ..proto::NativeRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn resume(&self, native_ref: &str) -> Result<()> {
        let client = self.client().await?;
        self.call(client.resume(proto::NativeRequest {
            native_ref: native_ref.to_owned(),
            ..proto::NativeRequest::default()
        }))
        .await?;
        Ok(())
    }

    async fn inventory(&self) -> Result<Vec<Observed>> {
        let client = self.client().await?;
        let response = self
            .call(client.inventory(proto::InventoryRequest::default()))
            .await?;
        Ok(response.into_owned().sandboxes)
    }

    async fn host_report(&self) -> HostReport {
        // The trait promises this never fails; a plugin that is down or cannot
        // answer simply reports nothing.
        let Ok(client) = self.client().await else {
            return HostReport::default();
        };
        match client
            .host_report(proto::HostReportRequest::default())
            .await
        {
            Ok(response) => response.into_owned(),
            Err(error) => {
                self.note_failure(&error).await;
                HostReport::default()
            }
        }
    }
}

impl Drop for ExternalDriver {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
    }
}

/// State shared by the driver handle and its supervisor tasks.
struct Shared {
    /// Configured name, used in socket file names and log lines.
    spec_name: String,
    target: ExternalTarget,
    socket: PathBuf,
    state: RwLock<State>,
    /// Serializes connection establishment.
    connect: Mutex<()>,
    /// Last `GetInfo` capabilities; refreshed after every restart.
    capabilities: parking_lot::RwLock<Capabilities>,
    shutdown: CancellationToken,
    /// Guards against starting a second socket-mode reconnect loop.
    reconnecting: AtomicBool,
}

#[derive(Default)]
struct State {
    client: Option<Client>,
    /// Why the plugin is not usable right now; calls fail `unavailable`.
    down: Option<String>,
}

/// Plugin identity, cached from `GetInfo`.
struct Info {
    name: String,
    capabilities: Capabilities,
    plugin_version: String,
}

impl Shared {
    async fn ready_client(&self) -> Option<Client> {
        self.state.read().await.client.clone()
    }

    async fn down_reason(&self) -> Option<String> {
        self.state.read().await.down.clone()
    }

    /// Publish a connected, version-checked client.
    async fn install(&self, client: Client, info: &Info) {
        {
            let mut state = self.state.write().await;
            state.client = Some(client);
            state.down = None;
        }
        *self.capabilities.write() = info.capabilities.clone();
    }

    /// Take the plugin out of service: calls fail `unavailable` until a
    /// restart or reconnect installs a new client.
    async fn mark_down(&self, reason: String) {
        let mut state = self.state.write().await;
        state.client = None;
        state.down = Some(reason);
    }

    /// Take the plugin out of service and, for socket mode, reconnect in the
    /// background. Spawned plugins are restarted by their supervisor, which
    /// owns the process and its backoff.
    async fn mark_down_and_reconnect(self: &Arc<Self>, reason: String) {
        self.mark_down(reason).await;
        if matches!(self.target, ExternalTarget::Socket(_))
            && !self.reconnecting.swap(true, Ordering::SeqCst)
        {
            let shared = self.clone();
            tokio::spawn(async move { shared.reconnect().await });
        }
    }

    /// Dial the socket until the plugin answers `GetInfo`, then install it.
    async fn reconnect(self: Arc<Self>) {
        let client = new_client(&self.socket);
        let mut backoff = RESTART_BACKOFF_START;
        loop {
            match get_info(&client, &self.spec_name).await {
                Ok(info) => {
                    self.install(client, &info).await;
                    self.reconnecting.store(false, Ordering::SeqCst);
                    tracing::info!(driver = %self.spec_name, "external driver reconnected");
                    return;
                }
                Err(error) => {
                    tracing::debug!(driver = %self.spec_name, %error, "external driver reconnect failed");
                }
            }
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    self.reconnecting.store(false, Ordering::SeqCst);
                    return;
                }
                _ = sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
        }
    }
}

/// Create the runtime directory with mode 0700.
fn prepare_runtime_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|error| {
        Error::internal(format!(
            "creating external driver runtime dir {}: {error}",
            path.display()
        ))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                Error::internal(format!(
                    "setting mode 0700 on external driver runtime dir {}: {error}",
                    path.display()
                ))
            },
        )?;
    }
    Ok(())
}

/// Start a spawn-mode plugin and forward its stderr through [`tracing`].
fn spawn_child(shared: &Shared) -> Result<Child> {
    let ExternalTarget::Spawn(binary, args) = &shared.target else {
        return Err(Error::internal(
            "spawn_child called for a socket-mode external driver",
        ));
    };
    // A previous process may have left its socket behind; a connect must not
    // reach a dead listener while the replacement binds.
    match std::fs::remove_file(&shared.socket) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(Error::internal(format!(
                "removing stale external driver socket {}: {error}",
                shared.socket.display()
            )));
        }
    }
    let mut child = Command::new(binary)
        .args(args)
        .arg("--socket")
        .arg(&shared.socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            Error::unavailable(format!(
                "starting external driver `{}` ({}): {error}",
                shared.spec_name,
                binary.display()
            ))
        })?;
    if let Some(stderr) = child.stderr.take() {
        let spec_name = shared.spec_name.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        tracing::info!(driver = %spec_name, "driver plugin: {line}");
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::debug!(driver = %spec_name, %error, "driver plugin stderr closed");
                        break;
                    }
                }
            }
        });
    }
    Ok(child)
}

/// Retry `GetInfo` until the plugin answers or [`GET_INFO_TIMEOUT`] elapses.
async fn wait_until_ready(
    shared: &Shared,
    mut child: Option<&mut Child>,
) -> Result<(Client, Info)> {
    let client = new_client(&shared.socket);
    let deadline = Instant::now() + GET_INFO_TIMEOUT;
    loop {
        match get_info(&client, &shared.spec_name).await {
            Ok(info) => return Ok((client, info)),
            // A version mismatch (or any non-transport refusal) will not fix
            // itself; fail now instead of burning the startup deadline.
            Err(error) if error.code != Code::Unavailable => return Err(error),
            Err(error) => {
                if let Some(child) = child.as_mut() {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            return Err(Error::unavailable(format!(
                                "external driver `{}` exited before answering GetInfo ({status})",
                                shared.spec_name
                            )));
                        }
                        Ok(None) => {}
                        Err(wait) => {
                            return Err(Error::unavailable(format!(
                                "waiting for external driver `{}`: {wait}",
                                shared.spec_name
                            )));
                        }
                    }
                }
                if Instant::now() >= deadline {
                    return Err(Error::unavailable(format!(
                        "external driver `{}` did not answer GetInfo on {} within {}s: {error}",
                        shared.spec_name,
                        shared.socket.display(),
                        GET_INFO_TIMEOUT.as_secs()
                    )));
                }
                if shared.shutdown.is_cancelled() {
                    return Err(Error::unavailable(format!(
                        "external driver `{}` start cancelled",
                        shared.spec_name
                    )));
                }
                sleep(GET_INFO_RETRY).await;
            }
        }
    }
}

/// Fetch and validate the plugin's identity and capabilities.
async fn get_info(client: &Client, spec_name: &str) -> Result<Info> {
    let response = client
        .get_info(proto::GetInfoRequest {
            agent_protocol_version: DRIVER_PROTOCOL_VERSION,
            ..proto::GetInfoRequest::default()
        })
        .await?
        .into_owned();
    if response.protocol_version != DRIVER_PROTOCOL_VERSION {
        return Err(Error::failed_precondition(format!(
            "driver plugin `{spec_name}` speaks driver protocol version {}, but this agent \
             speaks version {DRIVER_PROTOCOL_VERSION}; install a matching plugin build",
            response.protocol_version
        )));
    }
    Ok(Info {
        name: response.name,
        capabilities: response.capabilities.into_option().unwrap_or_default(),
        plugin_version: response.plugin_version,
    })
}

/// Restart a spawn-mode plugin whenever it exits, with exponential backoff.
async fn supervise(shared: Arc<Shared>, mut child: Child) {
    let mut backoff = RESTART_BACKOFF_START;
    loop {
        let status = tokio::select! {
            _ = shared.shutdown.cancelled() => {
                let _ = child.kill().await;
                return;
            }
            status = child.wait() => status,
        };
        let reason = match status {
            Ok(status) => format!("external driver `{}` exited ({status})", shared.spec_name),
            Err(error) => format!(
                "waiting for external driver `{}`: {error}",
                shared.spec_name
            ),
        };
        tracing::warn!(driver = %shared.spec_name, %reason, "driver plugin stopped; restarting");
        shared.mark_down(reason).await;
        loop {
            tokio::select! {
                _ = shared.shutdown.cancelled() => return,
                _ = sleep(backoff) => {}
            }
            let mut restarted = match spawn_child(&shared) {
                Ok(child) => child,
                Err(error) => {
                    tracing::warn!(driver = %shared.spec_name, %error, "external driver restart failed");
                    backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
                    continue;
                }
            };
            match wait_until_ready(&shared, Some(&mut restarted)).await {
                Ok((client, info)) => {
                    shared.install(client, &info).await;
                    tracing::info!(driver = %shared.spec_name, "external driver restarted");
                    backoff = RESTART_BACKOFF_START;
                    child = restarted;
                    break;
                }
                Err(error) => {
                    let _ = restarted.kill().await;
                    tracing::warn!(driver = %shared.spec_name, %error, "external driver restart failed");
                    backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
                }
            }
        }
    }
}

/// A Connect client for the plugin's h2c Unix socket.
fn new_client(socket: &Path) -> Client {
    let authority: axum::http::Uri = AUTHORITY.parse().expect("static authority URI");
    let connection =
        Http2Connection::lazy_unix(socket, authority.clone()).shared(CONNECTION_BUFFER);
    Client::new(connection, ClientConfig::new(authority))
}

/// A path as the plugin's protocol spells it.
fn path_str(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| {
        Error::invalid_argument(format!("path `{}` is not valid UTF-8", path.display()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::fake::FakeDriver;
    use crate::error::message;

    #[test]
    fn parses_a_spawn_spec() {
        let spec = ExternalSpec::parse("acme=/usr/libexec/acme-driver --flag value").unwrap();
        assert_eq!(spec.name, "acme");
        assert_eq!(
            spec.target,
            ExternalTarget::Spawn(
                PathBuf::from("/usr/libexec/acme-driver"),
                vec!["--flag".to_owned(), "value".to_owned()]
            )
        );
    }

    #[test]
    fn parses_a_socket_spec() {
        let spec = ExternalSpec::parse(" acme = unix:///run/acme.sock ").unwrap();
        assert_eq!(spec.name, "acme");
        assert_eq!(
            spec.target,
            ExternalTarget::Socket(PathBuf::from("/run/acme.sock"))
        );
    }

    #[test]
    fn rejects_malformed_specs() {
        for bad in [
            "",
            "acme",
            "=/bin/driver",
            "acme=",
            "acme=unix://",
            "bad/name=/bin/driver",
            "..=/bin/driver",
        ] {
            assert!(
                ExternalSpec::parse(bad).is_err(),
                "`{bad}` should not parse"
            );
        }
    }

    #[tokio::test]
    async fn refuses_a_plugin_with_a_different_protocol_version() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("fake.sock");
        let shutdown = CancellationToken::new();
        let other_version = DRIVER_PROTOCOL_VERSION + 1;
        let fake: Arc<dyn Driver> = Arc::new(FakeDriver::new(
            "fake",
            dir.path().join("root"),
            Capabilities::default(),
        ));
        let serve_socket = socket.clone();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            crate::driver_plugin::serve_with_version(
                fake,
                &serve_socket,
                server_shutdown,
                other_version,
            )
            .await
        });
        let spec = ExternalSpec::parse(&format!("fake=unix://{}", socket.display())).unwrap();
        let error = ExternalDriver::start(spec, dir.path())
            .await
            .err()
            .expect("a version mismatch must refuse to start");
        assert_eq!(error.code, Code::FailedPrecondition);
        let text = message(&error);
        assert!(text.contains(&other_version.to_string()), "{text}");
        assert!(
            text.contains(&DRIVER_PROTOCOL_VERSION.to_string()),
            "{text}"
        );
        shutdown.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn starts_a_matching_plugin_and_caches_its_identity() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("fake.sock");
        let shutdown = CancellationToken::new();
        let fake: Arc<dyn Driver> = Arc::new(FakeDriver::new(
            "fake",
            dir.path().join("root"),
            FakeDriver::full_capabilities(),
        ));
        let serve_socket = socket.clone();
        let server_shutdown = shutdown.clone();
        let server = tokio::spawn(async move {
            crate::driver_plugin::serve(fake, &serve_socket, server_shutdown).await
        });
        let spec = ExternalSpec::parse(&format!("fake=unix://{}", socket.display())).unwrap();
        let driver = ExternalDriver::start(spec, dir.path()).await.unwrap();
        assert_eq!(driver.name(), "fake");
        assert!(driver.capabilities().pause);
        assert!(driver.capabilities().snapshot);

        let native = driver
            .create(&CreateWork {
                sandbox_id: "sbx-external-test".into(),
                ..CreateWork::default()
            })
            .await
            .unwrap();
        assert_eq!(native, "psbx-external-test");

        let (tx, mut rx) = mpsc::channel(16);
        let code = driver
            .exec(
                &native,
                "exe-external-test",
                &ExecRequest {
                    argv: vec!["/bin/sh".into(), "-c".into(), "echo hi".into()],
                    timeout_secs: 10,
                    ..ExecRequest::default()
                },
                tx,
            )
            .await
            .unwrap();
        assert_eq!(code, 0);
        let mut stdout = Vec::new();
        while let Some(event) = rx.recv().await {
            if let Some(crate::api::exec_event::Event::Stdout(bytes)) = event.event {
                stdout.extend(bytes);
            }
        }
        assert_eq!(stdout, b"hi\n");

        assert_eq!(driver.inventory().await.unwrap().len(), 1);
        driver.destroy(&native).await.unwrap();
        assert!(driver.inventory().await.unwrap().is_empty());

        shutdown.cancel();
        server.await.unwrap().unwrap();
    }
}
