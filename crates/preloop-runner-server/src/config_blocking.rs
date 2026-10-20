//! Load and write the operator config file without parking an async worker.
//!
//! `config::load_config_from` and `config::write_config_to` are synchronous
//! and reach outside the process: the credential store for the `[secrets]`
//! sealing key (`config.rs::resolve_config_sealing_key_with_store`) and the
//! keyring for stored credential references. Those calls are bounded by
//! `credential_store`'s OS-store guard, so they cannot hang forever — but a
//! worker thread blocked on that bound (up to 10 s per call) is still a
//! worker spent, and the API handlers that call them hold
//! `secret_mutation`/`policy_mutation` while they wait.
//!
//! These wrappers move the blocking call to the blocking pool through the
//! shared [`preloop_observability::deadline::run_blocking`] runner. Lock
//! semantics are unchanged: a caller still holds its mutation lock across the
//! whole load → mutate → persist sequence, it just no longer occupies an
//! executor thread while the load or the write is in the OS store.

use std::path::Path;
use std::time::Duration;

use preloop_observability::deadline::run_blocking;

use crate::config::ConfigFile;

/// Deadline for one config load or write on the blocking pool.
///
/// The blocking body has its own bounds — every OS-store call goes through
/// `credential_store`'s 10 s guard, and that store's circuit makes later calls
/// fail immediately once one has expired — so this is deliberately far above
/// the real duration. 60 s leaves room for a load that resolves several
/// credential references, unseals the secret tables, and writes the file
/// atomically, while still letting an operator tell a wedged filesystem from a
/// slow disk. It is also why the bound is generous rather than tight:
/// blocking work cannot be cancelled, so the API must not report a failed
/// mutation that could still land.
const CONFIG_IO_DEADLINE: Duration = Duration::from_secs(60);

/// `config::load_config_from` on the blocking pool.
pub(crate) async fn load_config(path: &Path) -> anyhow::Result<ConfigFile> {
    let path = path.to_path_buf();
    run_blocking(
        CONFIG_IO_DEADLINE,
        "operator config load (config file / OS credential store)",
        move || crate::config::load_config_from(&path),
    )
    .await
    .map_err(|error| anyhow::anyhow!("{error}"))?
}

/// Run one synchronous config-file critical section on the blocking pool.
///
/// Used by the mutating API handlers: `config_mutation` (the process-wide lock
/// they share with engine-hosted App setup) must cover a whole
/// load → mutate → persist → publish sequence, and a `parking_lot` guard
/// cannot span an await — so the section is moved, guard included, onto the
/// blocking pool. The caller's async mutation lock still spans the call, and a
/// 10 s OS-credential-store wait now costs a blocking thread instead of an
/// async worker.
///
/// Returns the section's own result as the `Ok` payload; the outer error is
/// only the deadline/join failure.
pub(crate) async fn run_section<T: Send + 'static>(
    what: &str,
    section: impl FnOnce() -> T + Send + 'static,
) -> Result<T, anyhow::Error> {
    run_blocking(CONFIG_IO_DEADLINE, what, section)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
}
