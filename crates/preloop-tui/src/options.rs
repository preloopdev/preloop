//! Inputs `preloop-cli` passes to [`run`](crate::run).
//!
//! The TUI never reads credentials itself. The CLI resolves each context's
//! engine URL and token (the same way `preloop` resolves them everywhere else)
//! and hands them over here.

use std::fmt;

use crate::wizard::WizardPort;

/// A credential the TUI holds. Debug output is redacted; [`Secret::expose`] is
/// only for building protocol requests.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wrap a credential value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value. Only call this where a request needs the credential.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Where the Sandbox Service lives and how to authenticate to it.
#[derive(Debug, Clone)]
pub struct SandboxTarget {
    /// Base URL, for example `https://127.0.0.1:9191`.
    pub url: String,
    /// Bearer token. `None` means sandbox calls fail with an auth error.
    pub token: Option<Secret>,
    /// PEM of the Sandbox Service CA. `None` uses the system roots.
    pub ca_pem: Option<String>,
}

/// One engine (plus optional sandbox service) the TUI can point at.
#[derive(Debug, Clone)]
pub struct Context {
    /// Display name, shown in the header and used by `--context`.
    pub name: String,
    /// Engine base URL, for example `http://127.0.0.1:9090`.
    pub engine_url: String,
    /// Engine bearer token. `None` shows the banner and fails API calls.
    pub engine_token: Option<Secret>,
    /// Sandbox Service target, when the context has one.
    pub sandbox: Option<SandboxTarget>,
}

/// The view and selection the TUI opens on.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StartView {
    /// The overview tab.
    #[default]
    Overview,
    /// The runs tab, focused on the run with this id (or unique prefix).
    Run(String),
}

/// Everything [`run`](crate::run) needs to start.
#[derive(Debug, Clone)]
pub struct Options {
    /// Contexts to choose from. Must not be empty.
    pub contexts: Vec<Context>,
    /// Index into `contexts` of the context to open.
    pub context: usize,
    /// Initial view and selection.
    pub start: StartView,
    /// Disable every action that changes state.
    pub read_only: bool,
    /// Stack repositories and mirror configuration.
    pub stack: crate::config::StackConfig,
}

/// Hook used to run a foreground command after the TUI suspends the terminal.
pub type ShellOutHook = Box<dyn Fn(&[String]) -> anyhow::Result<i32> + Send + Sync>;
/// Actions the TUI cannot perform itself. `preloop-cli` injects them.
pub struct Hooks {
    /// Start the engine detached (`preloop serve`), logging to `$PRELOOP_HOME/engine.log`.
    pub start_engine: Box<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    /// Run the setup wizard. The wizard drives `port` and blocks on each reply.
    pub run_wizard: Box<dyn Fn(WizardPort) -> anyhow::Result<()> + Send + Sync>,
    /// Run `preloop` with `args` in the foreground and return its exit code.
    /// The TUI suspends the terminal around the call.
    pub shell_out: ShellOutHook,
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks").finish_non_exhaustive()
    }
}
