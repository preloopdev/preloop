//! Errors for the Sandbox Service and its clients.
//!
//! `Error` is the ConnectRPC error type the generated server returns and the
//! generated clients produce, so one type spans the service's internals, the
//! RPC boundary and the CLI. Builders are the protocol's own
//! (`Error::invalid_argument`, `Error::failed_precondition`, …).

pub use connectrpc::{ConnectError as Error, ErrorCode as Code};

/// Result alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The error's human-readable message, or `""` when it carries none.
pub fn message(error: &Error) -> &str {
    error.message.as_deref().unwrap_or_default()
}

/// Render an error code as its snake-case wire name (`failed_precondition`).
pub fn code_label(code: Code) -> &'static str {
    code.as_str()
}
