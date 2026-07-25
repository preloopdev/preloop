//! Loopback bridge from the guest to a mounted control-plane Unix socket.
//!
//! The runner itself talks to the control plane over `PRELOOP_CONTROL_SOCKET`,
//! but a job is mostly other people's programs: `git` inside
//! `actions/checkout`, Node actions using `@actions/http-client`, `curl` in a
//! `run:` step. They only know the origin URL the server advertises
//! (`PRELOOP_CONTROL_ORIGIN`, e.g. `http://127.0.0.1:9090`) and they open a
//! TCP connection to it.
//!
//! Inside a hardware-isolated VM that connection has nowhere to go: the guest's
//! loopback is its own, and the hypervisor's egress floor deliberately refuses
//! guest → host loopback. Without a bridge, `actions/checkout` cannot fetch the
//! workspace snapshot and live console logs never connect.
//!
//! This binds the advertised address *inside the guest* and splices every
//! accepted connection onto the mounted socket. The blast radius is exactly one
//! host endpoint — the control plane the runner is already authenticated
//! against — so it buys drop-in workflow compatibility without widening guest
//! egress.

use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tracing::{debug, info, warn};

/// Environment variable naming the mounted control-plane socket.
pub const CONTROL_SOCKET_ENV: &str = "PRELOOP_CONTROL_SOCKET";
/// Environment variable naming the origin the control plane advertises.
pub const CONTROL_ORIGIN_ENV: &str = "PRELOOP_CONTROL_ORIGIN";

/// A running loopback bridge. Dropping it stops accepting new connections.
pub struct ControlBridge {
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl ControlBridge {
    /// Address the bridge listens on inside the guest.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for ControlBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start the bridge described by the environment, if one is configured.
///
/// Returns `None` when the runner is not behind a mounted control socket (the
/// normal GitHub-hosted case) or when the advertised origin is not a loopback
/// address the guest can bind.
pub async fn spawn_from_env() -> Option<ControlBridge> {
    let socket = PathBuf::from(std::env::var_os(CONTROL_SOCKET_ENV)?);
    let origin = std::env::var(CONTROL_ORIGIN_ENV).ok()?;
    let address = loopback_address(&origin)?;
    match spawn(address, socket).await {
        Ok(bridge) => {
            info!(%address, "control-plane loopback bridge listening");
            Some(bridge)
        }
        Err(error) => {
            // A job can still run: the runner's own client uses the socket
            // directly. Only third-party tools lose control-plane access.
            warn!(%address, %error, "control-plane loopback bridge unavailable");
            None
        }
    }
}

/// Parse an origin into the loopback socket address to bind inside the guest.
///
/// Only loopback literals are accepted. Binding anything else would either fail
/// or, worse, expose the control plane on a routable guest interface.
pub fn loopback_address(origin: &str) -> Option<SocketAddr> {
    let rest = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let (host, port) = match authority.rsplit_once(':') {
        // An IPv6 literal keeps its brackets, so a colon inside them is not a
        // port separator.
        Some((host, port)) if !host.ends_with(']') || port.chars().all(|c| c.is_ascii_digit()) => {
            (host, port.parse().ok()?)
        }
        _ => (
            authority,
            if origin.starts_with("https://") {
                443
            } else {
                80
            },
        ),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let ip: std::net::IpAddr = host.parse().ok()?;
    ip.is_loopback().then(|| SocketAddr::new(ip, port))
}

async fn spawn(address: SocketAddr, socket: PathBuf) -> std::io::Result<ControlBridge> {
    let listener = TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, "control bridge accept failed");
                    continue;
                }
            };
            let socket = socket.clone();
            tokio::spawn(async move {
                if let Err(error) = splice(client, &socket).await {
                    debug!(%peer, %error, "control bridge connection ended");
                }
            });
        }
    });
    Ok(ControlBridge { address, task })
}

async fn splice(client: TcpStream, socket: &std::path::Path) -> std::io::Result<()> {
    // Nagle would add up to 40ms to the small request/response pairs the
    // control plane exchanges.
    client.set_nodelay(true)?;
    let upstream = UnixStream::connect(socket).await?;
    pump(client, upstream).await
}

async fn pump<A, B>(mut a: A, mut b: B) -> std::io::Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    tokio::io::copy_bidirectional(&mut a, &mut b)
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn loopback_origins_resolve_to_bindable_addresses() {
        assert_eq!(
            loopback_address("http://127.0.0.1:9090"),
            Some("127.0.0.1:9090".parse().unwrap())
        );
        assert_eq!(
            loopback_address("http://127.0.0.1:9090/runner/server"),
            Some("127.0.0.1:9090".parse().unwrap())
        );
        assert_eq!(
            loopback_address("http://127.0.0.1"),
            Some("127.0.0.1:80".parse().unwrap())
        );
        assert_eq!(
            loopback_address("https://127.0.0.1"),
            Some("127.0.0.1:443".parse().unwrap())
        );
        assert_eq!(
            loopback_address("http://[::1]:9090"),
            Some("[::1]:9090".parse().unwrap())
        );
    }

    #[test]
    fn non_loopback_origins_are_refused() {
        // Binding a routable address would publish the control plane to
        // whatever else can reach the guest.
        assert_eq!(loopback_address("http://10.0.0.5:9090"), None);
        assert_eq!(loopback_address("http://example.com:9090"), None);
        assert_eq!(loopback_address("ftp://127.0.0.1:9090"), None);
        assert_eq!(loopback_address("127.0.0.1:9090"), None);
    }

    #[tokio::test]
    async fn bridged_tcp_connection_reaches_the_unix_socket() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("control.sock");
        let server = tokio::net::UnixListener::bind(&socket_path).unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = server.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let bridge = spawn("127.0.0.1:0".parse().unwrap(), socket_path)
            .await
            .unwrap();
        let mut client = TcpStream::connect(bridge.address()).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
    }
}
