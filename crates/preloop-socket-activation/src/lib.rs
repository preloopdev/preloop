//! Minimal systemd socket activation support.

use anyhow::{bail, Context};

/// Take the single TCP listener passed by systemd, if socket activation is
/// configured for this process.
///
/// systemd reserves descriptors 0, 1, and 2 for stdio and starts listening
/// descriptors at 3. The descriptor is cloned before conversion so the
/// original descriptor remains owned by the service process until shutdown.
/// This is the only raw-descriptor boundary in the project.
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
        use std::net::TcpListener as StdTcpListener;
        use std::os::fd::BorrowedFd;

        // SAFETY: systemd guarantees LISTEN_FDS descriptors are open for this
        // process. We immediately clone descriptor 3 into an OwnedFd, so the
        // borrowed view never outlives this function.
        let borrowed = unsafe { BorrowedFd::borrow_raw(3) };
        let owned = borrowed
            .try_clone_to_owned()
            .context("clone systemd listener fd")?;
        let listener = StdTcpListener::from(owned);
        listener
            .set_nonblocking(true)
            .context("set systemd listener nonblocking")?;
        return Ok(Some(tokio::net::TcpListener::from_std(listener)?));
    }

    #[cfg(not(target_os = "linux"))]
    {
        bail!("systemd socket activation is only supported on Linux");
    }
}
