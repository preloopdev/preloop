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
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
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
/// Environment variable overriding the maximum concurrent spliced
/// connections. Defaults to [`DEFAULT_MAX_CONNECTIONS`].
pub const BRIDGE_MAX_CONNECTIONS_ENV: &str = "PRELOOP_BRIDGE_MAX_CONNECTIONS";
/// Environment variable overriding the idle timeout for a spliced
/// connection, in seconds. Defaults to 300
/// ([`DEFAULT_IDLE_TIMEOUT_SECS`]).
pub const BRIDGE_IDLE_TIMEOUT_SECS_ENV: &str = "PRELOOP_BRIDGE_IDLE_TIMEOUT_SECS";

/// Default cap on concurrent spliced connections.
///
/// R1-13: every accepted connection spawns a task and opens an upstream
/// socket. Without a cap, a guest process can accumulate tasks and sockets
/// without bound by opening connections and leaving them idle.
const DEFAULT_MAX_CONNECTIONS: usize = 256;
/// Default idle timeout, in seconds, for a spliced connection.
///
/// R1-13: a connection that moves zero bytes in either direction for this
/// long is closed. Polling HTTP clients and keep-alive pools reconnect
/// transparently, so this only reaps truly idle splices.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

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
///
/// R1-13: concurrent connections are capped at `PRELOOP_BRIDGE_MAX_CONNECTIONS`
/// (default 256); excess connections are closed immediately. Connections idle
/// longer than `PRELOOP_BRIDGE_IDLE_TIMEOUT_SECS` (default 300) are closed.
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
    let max_connections = std::env::var(BRIDGE_MAX_CONNECTIONS_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let idle_timeout = std::env::var(BRIDGE_IDLE_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS));
    match spawn(address, upstream, max_connections, idle_timeout).await {
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

async fn spawn(
    address: SocketAddr,
    upstream: Upstream,
    max_connections: usize,
    idle_timeout: Duration,
) -> std::io::Result<ControlBridge> {
    let listener = TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    let consecutive_failures = Arc::new(AtomicU32::new(0));
    let warned_at_threshold = Arc::new(AtomicBool::new(false));
    // R1-13: bound the number of concurrent spliced connections. The permit
    // is held for the whole connection lifetime, so at most `max_connections`
    // tasks and upstream sockets can exist at once.
    let connection_semaphore = Arc::new(Semaphore::new(max_connections));
    let task = tokio::spawn(async move {
        loop {
            let (client, peer) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    warn!(%error, "control bridge accept failed");
                    continue;
                }
            };
            let permit = match connection_semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    // Over the cap: close immediately rather than queueing a
                    // task the bridge can never service. HTTP clients retry.
                    debug!(%peer, "control bridge connection cap reached; closing");
                    drop(client);
                    continue;
                }
            };
            let upstream = upstream.clone();
            let failures = Arc::clone(&consecutive_failures);
            let warned = Arc::clone(&warned_at_threshold);
            tokio::spawn(async move {
                // Hold the permit until the splice ends.
                let _permit = permit;
                match splice(client, &upstream, idle_timeout).await {
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

async fn splice(
    client: TcpStream,
    upstream: &Upstream,
    idle_timeout: Duration,
) -> std::io::Result<()> {
    // Nagle would add up to 40ms to the small request/response pairs the
    // control plane exchanges.
    client.set_nodelay(true)?;
    // The idle deadline covers establishment too: a wedged connect must not
    // retain a semaphore permit past it.
    match upstream {
        #[cfg(unix)]
        Upstream::Socket(socket) => {
            let stream = tokio::time::timeout(idle_timeout, UnixStream::connect(socket))
                .await
                .map_err(|_| idle_expired())??;
            pump(client, stream, idle_timeout).await
        }
        Upstream::Tcp(addr) => {
            let stream = tokio::time::timeout(idle_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| idle_expired())??;
            stream.set_nodelay(true)?;
            pump(client, stream, idle_timeout).await
        }
    }
}

/// Idle-deadline expiry shared by bridge connects, reads, and writes, so no
/// stalled phase can retain a semaphore permit past the deadline.
fn idle_expired() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "control bridge connection idle",
    )
}

/// Bidirectional splice with an idle timeout and independent half-close.
///
/// R1-13: `copy_bidirectional` never times out, so a guest could hold
/// connections open forever at near-zero cost. This closes a splice that
/// moves no bytes in *either* direction for `idle_timeout`. Active traffic
/// — bytes flowing either way — resets the deadline; only truly idle
/// connections are reaped.
///
/// Each direction ends independently: EOF on one side shuts down only the
/// other side's writer, and relaying continues until both sides reach EOF.
/// Returning on the first EOF would drop a response already on its way back
/// after a client half-close. Stalled writes share the deadline so a wedged
/// peer cannot pin the opposite direction.
async fn pump<A, B>(mut a: A, mut b: B, idle_timeout: Duration) -> std::io::Result<()>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    async fn read_with_timeout<R: AsyncRead + Unpin>(
        r: &mut R,
        buf: &mut [u8],
        idle_timeout: Duration,
    ) -> std::io::Result<usize> {
        tokio::time::timeout(idle_timeout, r.read(buf))
            .await
            .map_err(|_| idle_expired())?
    }

    async fn write_with_timeout<W: AsyncWrite + Unpin>(
        w: &mut W,
        buf: &[u8],
        idle_timeout: Duration,
    ) -> std::io::Result<()> {
        tokio::time::timeout(idle_timeout, w.write_all(buf))
            .await
            .map_err(|_| idle_expired())?
    }

    let mut a_buf = [0u8; 8192];
    let mut b_buf = [0u8; 8192];
    let (mut a_eof, mut b_eof) = (false, false);
    loop {
        if a_eof && b_eof {
            return Ok(());
        }
        tokio::select! {
            read = read_with_timeout(&mut a, &mut a_buf, idle_timeout), if !a_eof => {
                let n = read?;
                if n == 0 {
                    a_eof = true;
                    b.shutdown().await?;
                } else {
                    write_with_timeout(&mut b, &a_buf[..n], idle_timeout).await?;
                }
            }
            read = read_with_timeout(&mut b, &mut b_buf, idle_timeout), if !b_eof => {
                let n = read?;
                if n == 0 {
                    b_eof = true;
                    a.shutdown().await?;
                } else {
                    write_with_timeout(&mut a, &b_buf[..n], idle_timeout).await?;
                }
            }
        }
    }
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
            DEFAULT_MAX_CONNECTIONS,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
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

        let bridge = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Upstream::Tcp(upstream_addr),
            DEFAULT_MAX_CONNECTIONS,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
        )
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

        let bridge = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Upstream::Tcp(upstream_addr),
            DEFAULT_MAX_CONNECTIONS,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
        )
        .await
        .unwrap();
        let mut client = TcpStream::connect(bridge.address()).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"pong");
    }
}
