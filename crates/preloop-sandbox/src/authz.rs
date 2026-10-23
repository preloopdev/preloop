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

/// Select the configured authorization policy.
///
/// `builtin` and an unset variable are the only supported configurations. The
/// error is returned before the HTTP server starts so a future policy can be
/// added without changing any route handler.
pub fn from_config() -> Result<Arc<dyn Authorizer>, String> {
    const SETTING: &str = "PRELOOP_SANDBOX_AUTHORIZER";

    match env::var(SETTING) {
        Ok(value) if value == "builtin" => Ok(Arc::new(TokenAuthorizer)),
        Ok(value) => Err(format!("unsupported {SETTING} value `{value}`")),
        Err(env::VarError::NotPresent) => Ok(Arc::new(TokenAuthorizer)),
        Err(error) => Err(format!("could not read {SETTING}: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_allows_every_action() {
        let authorizer = TokenAuthorizer;
        let principals = [
            Principal {
                kind: PrincipalKind::Client,
                name: "client".into(),
            },
            Principal {
                kind: PrincipalKind::Provider,
                name: "host-a".into(),
            },
            Principal {
                kind: PrincipalKind::Anonymous,
                name: "anonymous".into(),
            },
        ];
        for principal in principals {
            for action in [
                Action::Read,
                Action::Write,
                Action::Admin,
                Action::Exec,
                Action::Transfer,
                Action::Enroll,
                Action::Provider,
            ] {
                assert!(authorizer.authorize(&principal, action).is_ok());
            }
        }
        assert_eq!(authorizer.name(), "builtin");
    }

    #[test]
    #[allow(unsafe_code)]
    fn unknown_configuration_names_the_setting() {
        const SETTING: &str = "PRELOOP_SANDBOX_AUTHORIZER";
        let previous = env::var_os(SETTING);
        unsafe { env::set_var(SETTING, "unknown") };
        let result = from_config();
        unsafe {
            match previous {
                Some(value) => env::set_var(SETTING, value),
                None => env::remove_var(SETTING),
            }
        }

        let error = match result {
            Ok(_) => panic!("unknown authorizer must refuse startup"),
            Err(error) => error,
        };
        assert!(error.contains(SETTING), "{error}");
    }
}
