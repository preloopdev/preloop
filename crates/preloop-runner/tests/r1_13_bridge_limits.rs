//! R1-13 regression tests: the control bridge must bound concurrent
//! connections and reap truly-idle splices.
//!
//! Exercised through the public `spawn_from_env` entry point with
//! `PRELOOP_BRIDGE_MAX_CONNECTIONS` / `PRELOOP_BRIDGE_IDLE_TIMEOUT_SECS`
//! overrides. On the unfixed code (verified by stashing the fix) the
//! excess-connection scenario parks the third connection instead of closing
//! it, and the idle scenario never closes the silent splice — both
//! assertions fail.

use preloop_runner::control_bridge::{
    self, CONTROL_ORIGIN_ENV, CONTROL_SOCKET_ENV, CONTROL_UPSTREAM_ENV,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// R1-13 tuning knobs, as string literals (not the module constants) so this
// test also compiles against the unfixed code — where the bridge ignores them
// and the scenarios below fail at runtime.
const BRIDGE_MAX_CONNECTIONS_ENV: &str = "PRELOOP_BRIDGE_MAX_CONNECTIONS";
const BRIDGE_IDLE_TIMEOUT_SECS_ENV: &str = "PRELOOP_BRIDGE_IDLE_TIMEOUT_SECS";
/// Env overrides are process-global and integration tests in one binary share
/// a process: serialize scenarios that mutate them.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Fake upstream: accepts connections, drains anything forwarded, never
/// writes back — so splices only end when the bridge or the client ends them.
async fn park_upstream(listener: TcpListener) {
    loop {
        let Ok((mut conn, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                match conn.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        });
    }
}

#[tokio::test]
async fn bridge_enforces_connection_cap_and_idle_timeout() {
    let _env = lock_env();
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    tokio::spawn(park_upstream(upstream));

    // Bridge via the TCP-upstream fallback (no Unix socket in the sandbox),
    // with a 2-connection cap and a 1-second idle timeout.
    std::env::set_var(CONTROL_ORIGIN_ENV, "http://127.0.0.1:0");
    std::env::remove_var(CONTROL_SOCKET_ENV);
    std::env::set_var(CONTROL_UPSTREAM_ENV, upstream_addr.to_string());
    std::env::set_var(BRIDGE_MAX_CONNECTIONS_ENV, "2");
    std::env::set_var(BRIDGE_IDLE_TIMEOUT_SECS_ENV, "1");

    let bridge = control_bridge::spawn_from_env()
        .await
        .expect("bridge should spawn");
    let addr = bridge.address();

    // --- Scenario 1: excess connections are closed, not parked. ---
    let c1 = TcpStream::connect(addr).await.unwrap();
    let c2 = TcpStream::connect(addr).await.unwrap();
    // Let the accept loop acquire both permits before the third arrives.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut c3 = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(5), c3.read(&mut buf)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "expected excess connection to be closed promptly, got {closed:?}"
    );
    drop(c3);

    // Release the permits so the idle scenario starts clean.
    drop(c1);
    drop(c2);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // --- Scenario 2: a truly idle splice is reaped. ---
    let mut idle = TcpStream::connect(addr).await.unwrap();
    // Send nothing: after the 1s idle timeout the bridge must close it.
    let closed = tokio::time::timeout(Duration::from_secs(10), idle.read(&mut buf)).await;
    assert!(
        matches!(closed, Ok(Ok(0))),
        "expected idle connection to be reaped, got {closed:?}"
    );
    drop(idle);

    // --- Scenario 3: active traffic is not reaped. ---
    let mut active = TcpStream::connect(addr).await.unwrap();
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        // Bytes flowing client -> upstream keep resetting the idle deadline.
        active.write_all(b"heartbeat").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Still open: a read must time out (open, silent) rather than return EOF.
    let still_open = tokio::time::timeout(Duration::from_millis(300), active.read(&mut buf)).await;
    assert!(
        still_open.is_err(),
        "active connection should still be open, got {still_open:?}"
    );
    drop(active);
    drop(bridge);

    std::env::remove_var(CONTROL_ORIGIN_ENV);
    std::env::remove_var(CONTROL_UPSTREAM_ENV);
    std::env::remove_var(BRIDGE_MAX_CONNECTIONS_ENV);
    std::env::remove_var(BRIDGE_IDLE_TIMEOUT_SECS_ENV);
}

/// Half-close: a client that finishes its request and shuts down its write
/// half must still receive the upstream response. Returning on the first EOF
/// drops a reply already on its way back.
#[tokio::test]
async fn bridge_half_close_preserves_response() {
    let _env = lock_env();
    // Upstream reads the request to EOF, then answers.
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = upstream.accept().await {
            tokio::spawn(async move {
                let mut req = Vec::new();
                if s.read_to_end(&mut req).await.is_ok() {
                    let _ = s.write_all(b"reply-to:").await;
                    let _ = s.write_all(&req).await;
                }
            });
        }
    });

    std::env::set_var(CONTROL_ORIGIN_ENV, "http://127.0.0.1:0");
    std::env::remove_var(CONTROL_SOCKET_ENV);
    std::env::set_var(CONTROL_UPSTREAM_ENV, upstream_addr.to_string());
    std::env::set_var(BRIDGE_MAX_CONNECTIONS_ENV, "8");
    std::env::set_var(BRIDGE_IDLE_TIMEOUT_SECS_ENV, "10");

    let bridge = control_bridge::spawn_from_env()
        .await
        .expect("bridge should spawn");
    let addr = bridge.address();

    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    client.shutdown().await.unwrap();
    let mut reply = Vec::new();
    let got = tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut reply)).await;
    assert!(got.is_ok(), "response never arrived after half-close");
    got.unwrap().unwrap();
    assert_eq!(
        reply.as_slice(),
        b"reply-to:ping",
        "upstream response lost after half-close: {reply:?}"
    );

    std::env::remove_var(CONTROL_ORIGIN_ENV);
    std::env::remove_var(CONTROL_UPSTREAM_ENV);
    std::env::remove_var(BRIDGE_MAX_CONNECTIONS_ENV);
    std::env::remove_var(BRIDGE_IDLE_TIMEOUT_SECS_ENV);
}
