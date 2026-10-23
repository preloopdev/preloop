//! Serve an in-process [`Driver`] as an out-of-process plugin.
//!
//! A driver plugin is any process that speaks
//! `preloop.sandbox.driver.v1.Driver` over Connect (HTTP/2 cleartext) on a
//! Unix domain socket the host agent chooses. This module is the Rust side of
//! that contract: it turns a [`Driver`] implementation into a server, so an
//! existing in-tree driver and a plugin differ only in how they are launched.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use connectrpc::{
    ConnectRpcService, Encodable, RequestContext, Response as RpcResponse, ServiceRequest,
    ServiceResult, ServiceStream,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnectionBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::Driver;
use crate::api::{Empty, ExecEvent, exec_event};
use crate::driver::DRIVER_PROTOCOL_VERSION;
use crate::error::Error;
use crate::generated::preloop::sandbox::driver::v1::{
    self as proto, Driver as DriverService, DriverServer,
};

/// Parse `--socket <path>` from the process arguments.
///
/// The host agent starts a spawned plugin with
/// `--socket <runtime_dir>/<name>.sock`; a plugin exits when the flag is
/// missing or has no value.
pub fn socket_arg() -> Option<PathBuf> {
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--socket" {
            return args.next().map(PathBuf::from);
        }
    }
    None
}

/// Serve `driver` on `socket` until `shutdown` is cancelled.
///
/// The server speaks the out-of-process driver protocol
/// (`preloop.sandbox.driver.v1.Driver`) over HTTP/2 cleartext. `GetInfo`
/// reports [`DRIVER_PROTOCOL_VERSION`]; use [`serve_with_version`] to emulate
/// another version.
///
/// # Turning an existing Rust driver into a plugin
///
/// Any type that implements [`Driver`] already has the right shape: wrap it
/// in an `Arc`, pick the socket the host agent will dial, and serve:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use preloop_sandbox::driver_plugin;
/// use tokio_util::sync::CancellationToken;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let socket = driver_plugin::socket_arg()
///         .expect("the host agent passes --socket <path>");
///     let driver = Arc::new(MyDriver::new());
///     let shutdown = CancellationToken::new();
///     driver_plugin::serve(driver, &socket, shutdown).await
/// }
/// ```
///
/// Register the binary with the host agent as
/// `mydriver=/path/to/binary` (spawned) or `mydriver=unix:///path/sock`
/// (externally started). The agent creates the socket's directory, starts the
/// process with `--socket <runtime_dir>/mydriver.sock`, polls `GetInfo` until
/// it answers, and sends every work item to the plugin instead of calling the
/// driver in-process.
///
/// The socket is created with mode `0600`; a stale socket at the same path is
/// removed first, so a crashed plugin cannot block the next start.
///
/// # Errors
///
/// Fails if the socket cannot be created or secured, or if the listener fails
/// while accepting connections. Connection-level failures are logged and do
/// not stop the server.
pub async fn serve(
    driver: Arc<dyn Driver>,
    socket: &Path,
    shutdown: CancellationToken,
) -> Result<()> {
    serve_with_version(driver, socket, shutdown, DRIVER_PROTOCOL_VERSION).await
}

/// [`serve`] with an explicit protocol version.
///
/// The host agent compares `GetInfo.protocol_version` with its own and refuses
/// a mismatch. Production plugins use [`serve`]; this variant exists for tests
/// that exercise the refusal path.
///
/// # Errors
///
/// Same as [`serve`].
pub async fn serve_with_version(
    driver: Arc<dyn Driver>,
    socket: &Path,
    shutdown: CancellationToken,
    protocol_version: u32,
) -> Result<()> {
    remove_stale_socket(socket)?;
    let listener = UnixListener::bind(socket)
        .with_context(|| format!("binding driver socket {}", socket.display()))?;
    set_socket_mode(socket)?;

    let service = ConnectRpcService::new(DriverServer::new(PluginDriver {
        driver,
        protocol_version,
    }));
    let builder = ConnectionBuilder::new(TokioExecutor::new()).http2_only();
    let graceful = GracefulShutdown::new();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted
                    .with_context(|| format!("accepting on driver socket {}", socket.display()))?;
                let service = service.clone();
                let builder = builder.clone();
                let watcher = graceful.watcher();
                tokio::spawn(async move {
                    let connection = builder.serve_connection(
                        TokioIo::new(stream),
                        TowerToHyperService::new(service),
                    );
                    if let Err(error) = watcher.watch(connection).await {
                        tracing::debug!(%error, "driver plugin connection ended with error");
                    }
                });
            }
        }
    }

    // Close the listener first so a late dial fails instead of hanging, then
    // send GOAWAY to live connections and wait for in-flight RPCs to finish.
    drop(listener);
    graceful.shutdown().await;
    let _ = std::fs::remove_file(socket);
    Ok(())
}

/// Remove a socket a previous plugin process left behind.
fn remove_stale_socket(socket: &Path) -> Result<()> {
    match std::fs::remove_file(socket) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("removing stale driver socket {}", socket.display()))
        }
    }
}

/// Restrict the socket to its owner before the accept loop starts.
fn set_socket_mode(socket: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("setting mode 0600 on driver socket {}", socket.display()))
}

/// The generated `Driver` service forwarding to a [`Driver`].
struct PluginDriver {
    driver: Arc<dyn Driver>,
    protocol_version: u32,
}

impl PluginDriver {
    fn ok<T>(message: T) -> ServiceResult<T> {
        RpcResponse::ok(message)
    }
}

// The response futures borrow neither the request body nor `self` beyond the
// call, so they capture less than the trait's `use<'a, Self>` allows. That
// narrowing is deliberate and local.
#[allow(refining_impl_trait)]
impl DriverService for PluginDriver {
    fn get_info<'a>(
        &'a self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, proto::GetInfoRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<proto::GetInfoResponse> + Send + use<'a>>>
    + Send
    + use<'a> {
        let response = proto::GetInfoResponse {
            name: self.driver.name().to_owned(),
            protocol_version: self.protocol_version,
            capabilities: buffa::MessageField::some(self.driver.capabilities()),
            plugin_version: env!("CARGO_PKG_VERSION").to_owned(),
            ..proto::GetInfoResponse::default()
        };
        async move { RpcResponse::ok(response) }
    }

    fn create<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::CreateRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<proto::CreateResponse> + Send + use<'a>>>
    + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            let work = request.work.into_option().unwrap_or_default();
            let native_ref = self.driver.create(&work).await?;
            Self::ok(proto::CreateResponse {
                native_ref,
                ..proto::CreateResponse::default()
            })
        }
    }

    fn destroy<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::NativeRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver.destroy(&request.native_ref).await?;
            Self::ok(Empty::default())
        }
    }

    fn exec(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::ExecRequest>,
    ) -> impl Future<Output = ServiceResult<ServiceStream<impl Encodable<ExecEvent> + Send + use<>>>>
    + Send
    + use<> {
        let request = request.to_owned_message();
        let driver = Arc::clone(&self.driver);
        async move {
            let native_ref = request.native_ref;
            let exec_id = request.exec_id;
            let exec = request.request.into_option().unwrap_or_default();
            let (events_tx, mut events_rx) = mpsc::channel::<ExecEvent>(256);
            let (frames_tx, frames_rx) = mpsc::channel::<Result<ExecEvent, Error>>(256);

            let run =
                tokio::spawn(
                    async move { driver.exec(&native_ref, &exec_id, &exec, events_tx).await },
                );

            // Forward output first, then append the single exit event the
            // protocol promises. Drivers report the exit status by return
            // value, so an exit event from the driver is dropped.
            tokio::spawn(async move {
                while let Some(event) = events_rx.recv().await {
                    if matches!(event.event.as_ref(), Some(exec_event::Event::ExitCode(_))) {
                        continue;
                    }
                    if frames_tx.send(Ok(event)).await.is_err() {
                        // The caller's stream is gone; stop the command.
                        run.abort();
                        return;
                    }
                }
                let terminal = match run.await {
                    Ok(Ok(code)) => Ok(ExecEvent::exit(code)),
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(Error::internal(format!("driver exec task failed: {error}"))),
                };
                let _ = frames_tx.send(terminal).await;
            });

            let stream = futures::stream::unfold(frames_rx, |mut frames| async move {
                frames.recv().await.map(|frame| (frame, frames))
            });
            Ok(RpcResponse::stream(stream))
        }
    }

    fn cancel_exec<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::CancelExecRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver
                .cancel_exec(&request.native_ref, &request.exec_id)
                .await?;
            Self::ok(Empty::default())
        }
    }

    fn copy_in<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::CopyInRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver
                .copy_in(
                    &request.native_ref,
                    Path::new(&request.host_tar),
                    &request.guest_dir,
                )
                .await?;
            Self::ok(Empty::default())
        }
    }

    fn copy_out<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::CopyOutRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver
                .copy_out(
                    &request.native_ref,
                    &request.guest_path,
                    Path::new(&request.host_tar),
                )
                .await?;
            Self::ok(Empty::default())
        }
    }

    fn inventory<'a>(
        &'a self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, proto::InventoryRequest>,
    ) -> impl Future<
        Output = ServiceResult<impl Encodable<proto::InventoryResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        async move {
            let sandboxes = self.driver.inventory().await?;
            Self::ok(proto::InventoryResponse {
                sandboxes,
                ..proto::InventoryResponse::default()
            })
        }
    }

    fn host_report<'a>(
        &'a self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, proto::HostReportRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<crate::api::HostReport> + Send + use<'a>>>
    + Send
    + use<'a> {
        async move { RpcResponse::ok(self.driver.host_report().await) }
    }

    fn snapshot<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::SnapshotRequest>,
    ) -> impl Future<
        Output = ServiceResult<impl Encodable<proto::SnapshotResponse> + Send + use<'a>>,
    > + Send
    + use<'a> {
        let request = request.to_owned_message();
        async move {
            let native_ref = self
                .driver
                .snapshot(&request.native_ref, &request.template)
                .await?;
            Self::ok(proto::SnapshotResponse {
                native_ref,
                ..proto::SnapshotResponse::default()
            })
        }
    }

    fn drop_snapshot<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::NativeRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver.drop_snapshot(&request.native_ref).await?;
            Self::ok(Empty::default())
        }
    }

    fn stop<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::NativeRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver.stop(&request.native_ref).await?;
            Self::ok(Empty::default())
        }
    }

    fn start<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::StartRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver
                .start(&request.native_ref, request.forkable)
                .await?;
            Self::ok(Empty::default())
        }
    }

    fn pause<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::NativeRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver.pause(&request.native_ref).await?;
            Self::ok(Empty::default())
        }
    }

    fn resume<'a>(
        &'a self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, proto::NativeRequest>,
    ) -> impl Future<Output = ServiceResult<impl Encodable<Empty> + Send + use<'a>>> + Send + use<'a>
    {
        let request = request.to_owned_message();
        async move {
            self.driver.resume(&request.native_ref).await?;
            Self::ok(Empty::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::time::Duration;

    use connectrpc::client::{ClientConfig, Http2Connection};

    use super::*;
    use crate::api::{Capabilities, CreateWork};
    use crate::drivers::fake::FakeDriver;
    use crate::generated::preloop::sandbox::driver::v1::{
        CreateRequest, DriverClient, GetInfoRequest,
    };

    #[tokio::test]
    async fn serves_get_info_and_create() {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("fake.sock");
        let driver = Arc::new(FakeDriver::new(
            "fake",
            dir.path().join("root"),
            FakeDriver::full_capabilities(),
        ));
        let shutdown = CancellationToken::new();
        let server = tokio::spawn({
            let shutdown = shutdown.clone();
            let socket = socket.clone();
            async move { serve(driver, &socket, shutdown).await }
        });

        let mode = wait_for_socket(&socket).await;
        assert_eq!(mode, 0o600, "driver socket must be owner-only");

        let client = DriverClient::new(
            Http2Connection::lazy_unix(&socket, "http://localhost".parse().unwrap()).shared(64),
            ClientConfig::new("http://localhost".parse().unwrap()),
        );

        let info = client
            .get_info(GetInfoRequest::default())
            .await
            .expect("GetInfo")
            .into_owned();
        assert_eq!(info.name, "fake");
        assert_eq!(info.protocol_version, DRIVER_PROTOCOL_VERSION);
        assert_eq!(info.plugin_version, env!("CARGO_PKG_VERSION"));
        let capabilities: &Capabilities = info
            .capabilities
            .as_option()
            .expect("capabilities are reported");
        assert_eq!(capabilities.os, "linux");

        let created = client
            .create(CreateRequest {
                work: buffa::MessageField::some(CreateWork {
                    sandbox_id: "sbx-test".into(),
                    driver: "fake".into(),
                    ..CreateWork::default()
                }),
                ..CreateRequest::default()
            })
            .await
            .expect("Create")
            .into_owned();
        assert_eq!(created.native_ref, "psbx-test");

        shutdown.cancel();
        server.await.expect("server task").expect("server shutdown");
        assert!(!socket.exists(), "the socket is removed on shutdown");
    }

    /// Wait until the server has bound the socket and restricted it to its
    /// owner, then return the mode.
    async fn wait_for_socket(socket: &Path) -> u32 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(metadata) = std::fs::metadata(socket) {
                let mode = metadata.permissions().mode() & 0o777;
                if mode == 0o600 {
                    return mode;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "driver socket {} was not created",
                socket.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
