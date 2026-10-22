//! Minimal systemd socket activation support.
//!
//! `LISTEN_FDS`/`LISTEN_PID` are the socket-activation protocol itself and
//! are read here the way `argv` is read at startup. The descriptor is claimed
//! through the `listenfd` crate — the workspace forbids `unsafe`, which rules
//! out the raw `BorrowedFd::borrow_raw` conversion this used to do. The
//! listener is taken exactly once per process, and the returned
//! `TcpListener` owns its descriptor until it is dropped at shutdown.

use anyhow::{Context, bail};

/// Whether [`take_tcp_listener`] already claimed the systemd listener.
/// Enforces the take-once ownership contract: a second claim of the same raw
/// descriptor would give two owners one fd.
#[cfg(target_os = "linux")]
static TAKEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Take the single TCP listener passed by systemd, if socket activation is
/// configured for this process.
///
/// systemd reserves descriptors 0, 1, and 2 for stdio and starts listening
/// descriptors at 3. `LISTEN_FDS` must name exactly one descriptor, and a
/// `LISTEN_PID` that does not match this process is refused. The listener is
/// taken **once** per process: a second call is an error rather than a second
/// conversion of the same descriptor. Off Linux a configured activation is
/// refused — systemd socket activation is Linux-only.
pub fn take_tcp_listener() -> anyhow::Result<Option<tokio::net::TcpListener>> {
    let Some(listen_fds) = std::env::var_os("LISTEN_FDS") else {
        return Ok(None);
    };
    let listen_fds: usize = listen_fds
        .to_string_lossy()
        .parse()
        .context("LISTEN_FDS must be an integer")?;
    if listen_fds != 1 {
        bail!("expected exactly one systemd listener, got {listen_fds}");
    }
    if let Some(listen_pid) = std::env::var_os("LISTEN_PID") {
        let listen_pid = listen_pid
            .to_string_lossy()
            .parse::<u32>()
            .context("LISTEN_PID must be an integer")?;
        if listen_pid != std::process::id() {
            bail!("LISTEN_PID does not match the current process");
        }
    }

    #[cfg(target_os = "linux")]
    {
        use listenfd::ListenFd;

        if TAKEN.swap(true, std::sync::atomic::Ordering::SeqCst) {
            bail!("the systemd listener has already been taken");
        }
        let mut listen_fd = ListenFd::from_env();
        let listener = listen_fd
            .take_tcp_listener(0)
            .context("take systemd listener fd")?
            .context("LISTEN_FDS is set but the systemd listener is missing")?;
        listener
            .set_nonblocking(true)
            .context("set systemd listener nonblocking")?;
        // Trailing expression, not `return`: exactly one of these two cfg
        // blocks is compiled, so on Linux this is the function's tail and a
        // `return` here trips clippy::needless_return.
        Ok(Some(tokio::net::TcpListener::from_std(listener)?))
    }

    #[cfg(not(target_os = "linux"))]
    {
        bail!("systemd socket activation is only supported on Linux");
    }
}
