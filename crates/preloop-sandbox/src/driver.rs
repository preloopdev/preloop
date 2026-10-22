//! The host-side driver interface.
//!
//! Internal to preloop's host agent: it is not a public plugin ABI. Third
//! parties integrate through the `Providers` protocol instead, in any
//! language. Every method is intent-level; how a backend creates a machine
//! (cold boot, fork, snapshot restore) is its own business.

use std::collections::HashMap;
use std::path::Path;

use crate::api::{Capabilities, CreateWork, ExecEvent, ExecRequest, HostReport, Observed};
use crate::error::{Error, Result};
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Exit status reported when a driver killed the command at its deadline
/// (the same convention as the shell's `timeout`).
pub const EXEC_TIMED_OUT: i32 = 124;

/// Exit status reported when the caller of an exec went away and the driver
/// cancelled the command.
pub const EXEC_CANCELLED: i32 = 130;

/// Version of the out-of-process driver protocol
/// (`proto/preloop/sandbox/driver/v1/driver.proto`). The agent refuses a
/// plugin whose version differs.
pub const DRIVER_PROTOCOL_VERSION: u32 = 1;
/// A sandbox backend on one host (or one remote endpoint, for connectors).
#[async_trait]
pub trait Driver: Send + Sync {
    /// Stable driver name (`smolvm`, `fake`, or a private downstream driver).
    fn name(&self) -> &str;

    /// What this backend can do.
    fn capabilities(&self) -> Capabilities;

    /// Create and start a sandbox; returns its native reference.
    ///
    /// Drivers name native machines so [`Driver::inventory`] can recover
    /// the sandbox id (see [`native_name`]).
    ///
    /// `work.mounts` and `work.sockets` were admitted against the host
    /// agent's allowlist before this call; a driver uses those paths as
    /// given and must not resolve or widen them.
    async fn create(&self, work: &CreateWork) -> Result<String>;

    /// Destroy a sandbox. Destroying something already gone succeeds.
    async fn destroy(&self, native_ref: &str) -> Result<()>;

    /// Run command `exec_id` on the sandbox, forwarding output; returns the
    /// exit status. `request.timeout_secs` is never zero: the service fills
    /// in its default deadline, and the driver kills the command at it
    /// (reporting [`EXEC_TIMED_OUT`]).
    async fn exec(
        &self,
        native_ref: &str,
        exec_id: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32>;

    /// Kill the command of a running exec because its caller went away.
    ///
    /// A cancel may arrive before the exec starts: backends that can should
    /// remember it and refuse to start that exec id. Cancelling an exec that
    /// already finished succeeds.
    async fn cancel_exec(&self, _native_ref: &str, _exec_id: &str) -> Result<()> {
        Err(unsupported(self.name(), "cancel_exec"))
    }

    /// Extract a tar archive into `guest_dir`.
    async fn copy_in(&self, native_ref: &str, tar: &Path, guest_dir: &str) -> Result<()>;

    /// Write a tar archive of `guest_path` to `tar`.
    async fn copy_out(&self, native_ref: &str, guest_path: &str, tar: &Path) -> Result<()>;

    /// Capture a snapshot-capable sandbox as a template; returns the native
    /// template reference later passed back in `CreateWork::template`.
    async fn snapshot(&self, _native_ref: &str, _template: &str) -> Result<String> {
        Err(unsupported(self.name(), "snapshot"))
    }

    /// Release a snapshot created by [`Driver::snapshot`].
    async fn drop_snapshot(&self, _native_ref: &str) -> Result<()> {
        Ok(())
    }

    /// Power the sandbox off, keeping it and its disk for a later start.
    async fn stop(&self, _native_ref: &str) -> Result<()> {
        Err(unsupported(self.name(), "stop"))
    }

    /// Start a stopped sandbox; `forkable` asks for a branch base.
    async fn start(&self, _native_ref: &str, _forkable: bool) -> Result<()> {
        Err(unsupported(self.name(), "start"))
    }

    /// Pause with live state.
    async fn pause(&self, _native_ref: &str) -> Result<()> {
        Err(unsupported(self.name(), "pause"))
    }

    /// Resume a paused sandbox.
    async fn resume(&self, _native_ref: &str) -> Result<()> {
        Err(unsupported(self.name(), "resume"))
    }

    /// Every sandbox this driver owns on this host.
    async fn inventory(&self) -> Result<Vec<Observed>>;

    /// Best-effort host + per-sandbox runtime facts, sent on every
    /// `Heartbeat`. Never fails: a backend that cannot measure any of this
    /// returns the default (empty) report rather than an error — stats
    /// collection must not block heartbeat delivery or create false
    /// liveness negatives.
    async fn host_report(&self) -> HostReport {
        HostReport::default()
    }
}

/// The error for an operation a backend does not declare.
pub fn unsupported(driver: &str, operation: &str) -> Error {
    Error::failed_precondition(format!("{driver}: {operation} unsupported"))
}

/// Strip ANSI escape sequences from captured subprocess output.
///
/// Backends embed a child's stderr in their errors, and tools that detect a
/// terminal (or format for one unconditionally) emit colour and cursor codes.
/// Those bytes are useless in a log line or an API error, and a caller that
/// renders the message shows literal `\u{1b}[2m…` instead of plain text.
pub fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            plain.push(ch);
            continue;
        }
        match chars.peek() {
            // Control sequence: ESC `[` … one final byte in 0x40..=0x7e.
            Some('[') => {
                chars.next();
                for byte in chars.by_ref() {
                    if ('\u{40}'..='\u{7e}').contains(&byte) {
                        break;
                    }
                }
            }
            // Operating system command: ESC `]` … BEL or ESC `\`.
            Some(']') => {
                chars.next();
                while let Some(byte) = chars.next() {
                    if byte == '\u{07}' {
                        break;
                    }
                    if byte == '\u{1b}' && chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
            // Any other two-character escape.
            Some(_) => {
                chars.next();
            }
            None => {}
        }
    }
    plain
}

/// Prefix of every native machine name the sandbox layer creates.
pub const NATIVE_PREFIX: &str = "psbx";

/// Native machine name for a sandbox: `psbx-<id>`, where `<id>` is the id
/// without its `sbx-`. Sandbox ids are never reused, so the name is unique.
pub fn native_name(sandbox_id: &str) -> String {
    let bare = sandbox_id.strip_prefix("sbx-").unwrap_or(sandbox_id);
    format!("{NATIVE_PREFIX}-{bare}")
}

/// Parse a name produced by [`native_name`].
pub fn parse_native_name(name: &str) -> Option<String> {
    let bare = name.strip_prefix(NATIVE_PREFIX)?.strip_prefix('-')?;
    if bare.is_empty() {
        return None;
    }
    Some(format!("sbx-{bare}"))
}

/// Shell-quote one argument.
pub fn shell_quote(arg: &str) -> String {
    format!("'{}'", arg.replace('\'', "'\\''"))
}

/// An argv applying the request's env/workdir/stdin/user through a POSIX
/// shell, for backends whose exec primitive takes only an argv. Returns the
/// argv verbatim when there is nothing to apply.
pub fn posix_exec_argv(request: &ExecRequest) -> Vec<String> {
    use base64::Engine as _;
    let plain = request.env.is_empty()
        && request.workdir.is_empty()
        && request.stdin.is_empty()
        && (request.user.is_empty() || request.user == "root");
    if plain {
        return request.argv.clone();
    }
    let mut script = String::new();
    if !request.workdir.is_empty() {
        script.push_str(&format!(
            "cd {} || exit 127; ",
            shell_quote(&request.workdir)
        ));
    }
    for (key, value) in &request.env {
        script.push_str(&format!("export {key}={}; ", shell_quote(value)));
    }
    let command: Vec<String> = request.argv.iter().map(|a| shell_quote(a)).collect();
    let mut command = command.join(" ");
    if !request.user.is_empty() && request.user != "root" {
        // Keep the exported environment across the user switch.
        command = format!(
            "su -m -s /bin/sh {} -c {}",
            shell_quote(&request.user),
            shell_quote(&command)
        );
    }
    if request.stdin.is_empty() {
        script.push_str(&format!("exec {command}"));
    } else {
        let data = base64::engine::general_purpose::STANDARD.encode(&request.stdin);
        script.push_str(&format!(
            "printf %s {} | base64 -d | {command}",
            shell_quote(&data)
        ));
    }
    vec!["sh".into(), "-c".into(), script]
}

/// Cancellation state for a driver's running execs, keyed by exec id.
///
/// [`Driver::cancel_exec`] can arrive before [`Driver::exec`] starts — the
/// caller's stream may drop between the work being delivered and the command
/// starting — so a cancel for an unknown exec records a pre-cancelled marker
/// that the next `exec` for that id observes instead of spawning a child.
#[derive(Default)]
pub struct ExecCancels {
    tokens: parking_lot::Mutex<HashMap<String, CancellationToken>>,
}

impl ExecCancels {
    /// Register exec `exec_id`; the token is already cancelled when a cancel
    /// for it arrived first.
    pub fn start(&self, exec_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        let mut tokens = self.tokens.lock();
        if tokens
            .get(exec_id)
            .is_some_and(CancellationToken::is_cancelled)
        {
            token.cancel();
        }
        tokens.insert(exec_id.to_owned(), token.clone());
        token
    }

    /// Cancel `exec_id`, remembering it when no exec is running yet.
    pub fn cancel(&self, exec_id: &str) {
        let mut tokens = self.tokens.lock();
        match tokens.get(exec_id) {
            Some(token) => token.cancel(),
            None => {
                let token = CancellationToken::new();
                token.cancel();
                tokens.insert(exec_id.to_owned(), token);
            }
        }
        // Markers for execs that never start would otherwise accumulate.
        if tokens.len() > 4096 {
            tokens.retain(|_, token| !token.is_cancelled());
        }
    }

    /// Forget a finished exec.
    pub fn finish(&self, exec_id: &str) {
        self.tokens.lock().remove(exec_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_names_round_trip() {
        let name = native_name("sbx-3f2a9c");
        assert_eq!(name, "psbx-3f2a9c");
        assert_eq!(parse_native_name(&name), Some("sbx-3f2a9c".into()));
        assert_eq!(parse_native_name("preloop-runner-1"), None);
        assert_eq!(parse_native_name("psbx-"), None);
        // Caller-chosen names, e.g. the CI pool's, are not encoded names.
        assert_eq!(parse_native_name("preloop-runner-0-1"), None);
    }

    #[test]
    fn exec_argv_is_verbatim_when_nothing_to_apply() {
        let request = ExecRequest {
            argv: vec!["uname".into(), "-a".into()],
            ..ExecRequest::default()
        };
        assert_eq!(posix_exec_argv(&request), vec!["uname", "-a"]);
    }

    #[test]
    fn exec_argv_applies_env_user_and_stdin() {
        let mut request = ExecRequest {
            argv: vec!["cat".into()],
            user: "runner".into(),
            stdin: b"hi".to_vec(),
            ..ExecRequest::default()
        };
        request.env.insert("K".into(), "v w".into());
        let argv = posix_exec_argv(&request);
        assert_eq!(
            argv[2],
            "export K='v w'; printf %s 'aGk=' | base64 -d | su -m -s /bin/sh 'runner' -c ''\\''cat'\\'''"
        );
    }

    #[test]
    fn exec_cancels_survive_the_start_race() {
        let cancels = ExecCancels::default();
        // A cancel before the exec starts refuses to start it.
        cancels.cancel("exe-early");
        let token = cancels.start("exe-early");
        assert!(token.is_cancelled());
        cancels.finish("exe-early");
        // A cancel while running cancels the running token.
        let token = cancels.start("exe-running");
        assert!(!token.is_cancelled());
        cancels.cancel("exe-running");
        assert!(token.is_cancelled());
        cancels.finish("exe-running");
        // A cancel after it finished is harmless.
        cancels.cancel("exe-late");
    }

    #[test]
    fn strip_ansi_removes_colour_codes_and_keeps_text() {
        assert_eq!(
            strip_ansi("\u{1b}[2mError:\u{1b}[0m pull failed\n"),
            "Error: pull failed\n"
        );
        assert_eq!(strip_ansi("plain text"), "plain text");
        // OSC sequences (terminal hyperlinks) end with BEL or ESC `\`.
        assert_eq!(
            strip_ansi("\u{1b}]8;;https://example.invalid\u{07}link\u{1b}]8;;\u{1b}\\ok"),
            "linkok"
        );
    }
}
