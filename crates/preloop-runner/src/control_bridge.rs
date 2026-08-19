//! Loopback bridge from the guest to the control-plane server.
//!
//! The runner itself knows how to reach the control plane, but a job is mostly
//! other people's programs: `git` inside `actions/checkout`, Node actions using
//! `@actions/http-client`, `curl` in a `run:` step. They only know the origin
//! URL the server advertises (`PRELOOP_CONTROL_ORIGIN`, typically
//! `http://127.0.0.1:9090`) and they open a TCP connection to it.
//!
//! Inside a hardware-isolated VM that connection has nowhere to go: the guest's
//! loopback is its own, and the hypervisor's egress floor deliberately refuses
//! guest → host loopback. Without a bridge, `actions/checkout` cannot fetch the
//! workspace snapshot and live console logs never connect.
//!
//! This binds the advertised address *inside the guest* and splices every
//! accepted connection onto either:
//! 1. A mounted Unix socket (`PRELOOP_CONTROL_SOCKET`) — the preferred path
//!    when the hypervisor's vsock bridge is functional.
//! 2. A TCP upstream (`PRELOOP_CONTROL_UPSTREAM`) — fallback when the socket
//!    bridge is unavailable (e.g. broken vsock on Linux x86_64).
//!
//! The blast radius is exactly one host endpoint — the control plane the runner
//! is already authenticated against — so it buys drop-in workflow compatibility
//! without widening guest egress.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

/// Consecutive upstream connect failures before the bridge logs a warning.
///
/// The bridge never exits on upstream failure: the runner polls through it
/// with its own unbounded retry loop, so a transient outage — the guest
/// network not yet up at fork is the common one — must not deafen the runner
/// for the rest of the VM's life. Exiting after N failures did exactly that:
/// the runner kept polling a dead loopback address ("Connection refused")
/// while its job sat in_progress with no logs.
const UPSTREAM_FAILURE_WARN_THRESHOLD: u32 = 10;

/// Environment variable naming the mounted control-plane socket.
pub const CONTROL_SOCKET_ENV: &str = "PRELOOP_CONTROL_SOCKET";
/// Environment variable naming the origin the control plane advertises.
pub const CONTROL_ORIGIN_ENV: &str = "PRELOOP_CONTROL_ORIGIN";
/// Environment variable naming the TCP upstream address for the bridge.
///
/// Set when the mounted Unix socket is unavailable and the bridge should
/// forward connections over TCP instead (e.g. to a LAN-reachable host IP).
pub const CONTROL_UPSTREAM_ENV: &str = "PRELOOP_CONTROL_UPSTREAM";

/// Which upstream transport the bridge forwards accepted connections to.
#[derive(Debug, Clone)]
enum Upstream {
    /// Forward to a mounted Unix socket (vsock bridge).
    #[cfg(unix)]
    Socket(PathBuf),
    /// Forward to a TCP address (virtio-net path).
    Tcp(SocketAddr),
}

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
    let origin = std::env::var(CONTROL_ORIGIN_ENV).ok()?;
    let socket = std::env::var_os(CONTROL_SOCKET_ENV).map(PathBuf::from);
    let upstream_addr = std::env::var(CONTROL_UPSTREAM_ENV).ok();
    let upstream = match (socket, upstream_addr) {
        #[cfg(unix)]
        (Some(socket), _) => Upstream::Socket(socket),
        #[cfg(not(unix))]
        (Some(_), _) => {
            // No Unix domain sockets on this platform; a control socket env
            // cannot be honored. Prefer the TCP upstream when one is
            // configured; otherwise no bridge at all.
            match upstream_addr {
                Some(addr) => {
                    let addr = upstream_tcp_address(&addr)?;
                    Upstream::Tcp(addr)
                }
                None => return None,
            }
        }
        (None, Some(addr)) => {
            let addr = upstream_tcp_address(&addr)?;
            Upstream::Tcp(addr)
        }
        (None, None) => return None,
    };
    let address = loopback_address(&origin)?;
    match spawn(address, upstream).await {
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
    let ip: std::net::IpAddr = if host.eq_ignore_ascii_case("localhost") {
        std::net::Ipv4Addr::LOCALHOST.into()
    } else {
        host.parse().ok()?
    };
    ip.is_loopback().then(|| SocketAddr::new(ip, port))
}

/// Parse a TCP upstream address from a URL or `host:port` literal.
fn upstream_tcp_address(addr: &str) -> Option<SocketAddr> {
    // Try as a URL first (e.g. "http://10.0.0.161:9090").
    if let Some(rest) = addr
        .strip_prefix("http://")
        .or_else(|| addr.strip_prefix("https://"))
    {
        let authority = rest.split(['/', '?', '#']).next()?;
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) if !host.ends_with(']') => (host, port.parse().ok()?),
            _ => (
                authority,
                if addr.starts_with("https://") {
                    443
                } else {
                    80
                },
            ),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let ip: std::net::IpAddr = host.parse().ok()?;
        return Some(SocketAddr::new(ip, port));
    }
    // Try as bare `host:port`.
    addr.parse().ok()
}

async fn spawn(address: SocketAddr, upstream: Upstream) -> std::io::Result<ControlBridge> {
    let listener = TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let consecutive_failures = Arc::new(AtomicU32::new(0));
    let warned_at_threshold = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn(async move {
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, "control bridge accept failed");
                    continue;
                }
            };
            let upstream = upstream.clone();
            let failures = Arc::clone(&consecutive_failures);
            let warned = Arc::clone(&warned_at_threshold);
            tokio::spawn(async move {
                match splice(client, &upstream).await {
                    Ok(()) => {
                        failures.store(0, Ordering::Relaxed);
                    }
                    Err(error) => {
                        let n = failures.fetch_add(1, Ordering::Relaxed) + 1;
                        if n >= UPSTREAM_FAILURE_WARN_THRESHOLD
                            && !warned.swap(true, Ordering::Relaxed)
                        {
                            warn!(
                                %peer,
                                "control bridge upstream unreachable after {n} consecutive failures; \
                                 staying up and retrying — the runner polls through this bridge"
                            );
                        }
                        debug!(%peer, %error, "control bridge connection ended");
                    }
                }
            });
        }
    });
    Ok(ControlBridge { address, task })
}

async fn splice(client: TcpStream, upstream: &Upstream) -> std::io::Result<()> {
    // Nagle would add up to 40ms to the small request/response pairs the
    // control plane exchanges.
    client.set_nodelay(true)?;
    match upstream {
        #[cfg(unix)]
        Upstream::Socket(socket) => {
            let stream = UnixStream::connect(socket).await?;
            pump(client, stream).await
        }
        Upstream::Tcp(addr) => {
            let stream = TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            pump(client, stream).await
        }
    }
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
        assert_eq!(
            loopback_address("http://localhost:9090"),
            Some("127.0.0.1:9090".parse().unwrap())
        );
        assert_eq!(
            loopback_address("https://LOCALHOST"),
            Some("127.0.0.1:443".parse().unwrap())
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

    #[test]
    fn upstream_tcp_address_parses_urls_and_bare_addresses() {
        assert_eq!(
            upstream_tcp_address("http://10.0.0.161:9090"),
            Some("10.0.0.161:9090".parse().unwrap())
        );
        assert_eq!(
            upstream_tcp_address("http://10.0.0.161"),
            Some("10.0.0.161:80".parse().unwrap())
        );
        assert_eq!(
            upstream_tcp_address("https://10.0.0.161:8443"),
            Some("10.0.0.161:8443".parse().unwrap())
        );
        assert_eq!(
            upstream_tcp_address("10.0.0.161:9090"),
            Some("10.0.0.161:9090".parse().unwrap())
        );
        assert_eq!(upstream_tcp_address("not-a-url"), None);
    }

    #[tokio::test]
    #[cfg(unix)]
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

        let bridge = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Upstream::Socket(socket_path),
        )
        .await
        .unwrap();
        let mut client = TcpStream::connect(bridge.address()).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn bridge_survives_transient_upstream_outage() {
        // Reserve a port and free it: nothing listens there, modelling the
        // guest network not being up yet at fork. Blow far past the old
        // 10-failure exit budget, then bring the upstream up and prove the
        // bridge is still alive and splicing — the runner's poll loop
        // retries forever, so the bridge must outlast any outage.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = probe.local_addr().unwrap();
        drop(probe);

        let bridge = spawn("127.0.0.1:0".parse().unwrap(), Upstream::Tcp(upstream_addr))
            .await
            .unwrap();
        let addr = bridge.address();

        // Exhaust the old failure budget: each connection is accepted, then
        // the splice to the closed upstream fails.
        for _ in 0..UPSTREAM_FAILURE_WARN_THRESHOLD + 5 {
            if let Ok(mut c) = TcpStream::connect(addr).await {
                let _ = c.write_all(b"x").await;
                // Give the spawned splice task time to run and fail.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        assert!(
            !bridge.task.is_finished(),
            "bridge must stay up through an upstream outage"
        );

        // The upstream comes up now; a fresh connection must round-trip.
        let upstream_listener = TcpListener::bind(upstream_addr).await.unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bridged_tcp_connection_reaches_tcp_upstream() {
        // Start a TCP "server" that echoes back.
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = upstream_listener.accept().await.unwrap();
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let bridge = spawn("127.0.0.1:0".parse().unwrap(), Upstream::Tcp(upstream_addr))
            .await
            .unwrap();
        let mut client = TcpStream::connect(bridge.address()).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
    }
}
