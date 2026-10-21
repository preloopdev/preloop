//! The host-side driver interface.
//!
//! Internal to preloop's host agent: it is not a public plugin ABI. Third
//! parties integrate through the `Providers` protocol instead, in any
//! language. Every method is intent-level; how a backend creates a machine
//! (cold boot, fork, snapshot restore) is its own business.

use std::path::Path;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::api::{Capabilities, CreateWork, ExecEvent, ExecRequest, Observed};
use crate::connect::{Error, Result};

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
    /// the sandbox id and incarnation (see [`native_name`]).
    ///
    /// `work.mounts` and `work.sockets` were admitted against the host
    /// agent's allowlist before this call; a driver uses those paths as
    /// given and must not resolve or widen them.
    async fn create(&self, work: &CreateWork) -> Result<String>;

    /// Destroy a sandbox. Destroying something already gone succeeds.
    async fn destroy(&self, native_ref: &str) -> Result<()>;

    /// Run a command, forwarding output; returns the exit status.
    async fn exec(
        &self,
        native_ref: &str,
        request: &ExecRequest,
        output: mpsc::Sender<ExecEvent>,
    ) -> Result<i32>;

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
}

/// The error for an operation a backend does not declare.
pub fn unsupported(driver: &str, operation: &str) -> Error {
    Error::precondition(format!("{driver}: {operation} unsupported"))
}

/// Prefix of every native machine name the sandbox layer creates.
pub const NATIVE_PREFIX: &str = "psbx";

/// Native machine name encoding sandbox id and incarnation:
/// `psbx-<id>-i<incarnation>`, where `<id>` is the id without its `sbx-`.
pub fn native_name(sandbox_id: &str, incarnation: u64) -> String {
    let bare = sandbox_id.strip_prefix("sbx-").unwrap_or(sandbox_id);
    format!("{NATIVE_PREFIX}-{bare}-i{incarnation}")
}

/// Parse a name produced by [`native_name`].
pub fn parse_native_name(name: &str) -> Option<(String, u64)> {
    let rest = name.strip_prefix(NATIVE_PREFIX)?.strip_prefix('-')?;
    let (bare, incarnation) = rest.rsplit_once("-i")?;
    let incarnation = incarnation.parse().ok()?;
    if bare.is_empty() {
        return None;
    }
    Some((format!("sbx-{bare}"), incarnation))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_names_round_trip() {
        let name = native_name("sbx-3f2a9c", 4);
        assert_eq!(name, "psbx-3f2a9c-i4");
        assert_eq!(parse_native_name(&name), Some(("sbx-3f2a9c".into(), 4)));
        assert_eq!(parse_native_name("preloop-runner-1"), None);
        assert_eq!(parse_native_name("psbx--i1"), None);
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
}
