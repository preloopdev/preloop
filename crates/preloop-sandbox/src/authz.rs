//! Authorization seam for the Sandbox Service.
//!
//! Authentication remains in the HTTP handlers: the Sandboxes routes verify
//! the client bearer token, the Providers routes resolve the client
//! certificate, and ticket routes verify their ticket token. After that
//! identity check, handlers call [`Authorizer::authorize`] exactly once.
//!
//! The route-to-action mapping is:
//!
//! * `ListTemplates`, `ListPools`, `Get`, `List`, `ListProviders`, and the
//!   health check use [`Action::Read`].
//! * Mutations to sandbox, pool, template, or lease state use
//!   [`Action::Write`].
//! * Provider administration (`DeleteProvider` and `CreateJoinToken`) uses
//!   [`Action::Admin`].
//! * `Exec` and `OpenStream` use [`Action::Exec`].
//! * Caller and agent blob transfers use [`Action::Transfer`].
//! * `Enroll` and `ca.pem` use [`Action::Enroll`].
//! * Every other `Providers` RPC uses [`Action::Provider`].

use std::env;
use std::sync::Arc;

use crate::Error;

/// The authenticated identity presented to an authorization policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The kind of credential that authenticated the request.
    pub kind: PrincipalKind,
    /// A stable name for the identity within that credential surface.
    pub name: String,
}

/// The credential surface that authenticated a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// A caller authenticated by the Sandbox Service client token or ticket.
    Client,
    /// A provider authenticated by an enrolled client certificate.
    Provider,
    /// A route that intentionally has no credential, such as enrollment.
    Anonymous,
}

/// The coarse-grained action required by a Sandbox Service route group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Read service state.
    Read,
    /// Mutate sandbox, pool, template, or lease state.
    Write,
    /// Administer providers or enrollment tokens.
    Admin,
    /// Execute commands or open a caller data stream.
    Exec,
    /// Transfer caller or provider blob data.
    Transfer,
    /// Enroll a provider or retrieve the service CA used for enrollment.
    Enroll,
    /// Use the provider protocol.
    Provider,
}

/// Policy seam called after authentication and before route work begins.
pub trait Authorizer: Send + Sync {
    /// Reject `principal` when it cannot perform `action`.
    fn authorize(&self, principal: &Principal, action: Action) -> Result<(), Error>;

    /// Stable policy name for diagnostics and future policy selection.
    fn name(&self) -> &'static str;
}

/// The built-in policy, which permits every authenticated principal today.
#[derive(Debug, Default)]
pub struct TokenAuthorizer;

impl Authorizer for TokenAuthorizer {
    fn authorize(&self, _principal: &Principal, _action: Action) -> Result<(), Error> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "builtin"
    }
}

/// Environment variable that selects the policy.
pub const AUTHORIZER_SETTING: &str = "PRELOOP_SANDBOX_AUTHORIZER";

/// Select the configured authorization policy from the environment.
///
/// `builtin` and an unset variable are the only supported configurations. The
/// error is returned before the HTTP server starts so a future policy can be
/// added without changing any route handler.
pub fn from_config() -> Result<Arc<dyn Authorizer>, String> {
    match env::var(AUTHORIZER_SETTING) {
        Ok(value) => from_setting(Some(&value)),
        Err(env::VarError::NotPresent) => from_setting(None),
        Err(error) => Err(format!("could not read {AUTHORIZER_SETTING}: {error}")),
    }
}

/// Select a policy from the setting's value (`None` when unset).
fn from_setting(value: Option<&str>) -> Result<Arc<dyn Authorizer>, String> {
    match value {
        None | Some("builtin") => Ok(Arc::new(TokenAuthorizer)),
        Some(other) => Err(format!(
            "unsupported {AUTHORIZER_SETTING} value `{other}` (only `builtin` is available)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_and_builtin_select_the_default_policy() {
        assert_eq!(from_setting(None).unwrap().name(), "builtin");
        assert_eq!(from_setting(Some("builtin")).unwrap().name(), "builtin");
    }

    #[test]
    fn unknown_values_refuse_startup_naming_the_setting() {
        for value in ["unknown", "", "Builtin", " builtin"] {
            let error = from_setting(Some(value)).err().expect("must be rejected");
            assert!(error.contains(AUTHORIZER_SETTING), "{error}");
        }
    }
}
