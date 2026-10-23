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

/// Rebuild an error from a report the agent stored as `<code>: <message>`.
///
/// The host agent records a failed work item with its code visible (an
/// operator reading the log can tell "not supported" from "provider is
/// down"). When the service turns that report back into an error, the code
/// belongs in the error's `code`, not in its message: wrapping the whole
/// string as `unavailable` both loses the original code and renders as
/// `unavailable: unavailable: …`.
pub fn from_reported(text: String) -> Error {
    let mut rest = text.as_str();
    let mut code = None;
    // Older records can repeat the same label (`unavailable: unavailable:
    // …`); consume every repetition and keep the code once.
    while let Some((label, tail)) = rest.split_once(": ")
        && let Ok(parsed) = label.parse::<Code>()
        && code.is_none_or(|code| code == parsed)
    {
        code = Some(parsed);
        rest = tail;
    }
    Error::new(code.unwrap_or(Code::Unavailable), rest.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reported_errors_keep_their_code_without_repeating_it() {
        let error = from_reported("unavailable: external driver `x` exited".into());
        assert_eq!(error.code, Code::Unavailable);
        assert_eq!(message(&error), "external driver `x` exited");

        let error = from_reported("failed_precondition: an operation the driver lacks".into());
        assert_eq!(error.code, Code::FailedPrecondition);
        assert_eq!(message(&error), "an operation the driver lacks");

        let error = from_reported("unavailable: unavailable: plugin exited".into());
        assert_eq!(error.code, Code::Unavailable);
        assert_eq!(message(&error), "plugin exited");

        // Older records and plain messages keep the historic classification.
        let error = from_reported("provider lost".into());
        assert_eq!(error.code, Code::Unavailable);
        assert_eq!(message(&error), "provider lost");
    }
}
