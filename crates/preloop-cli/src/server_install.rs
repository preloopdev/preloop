//! `preloop server install` / `preloop server uninstall`.
//!
//! Scaffolds the control plane as a supervised system service:
//!
//! - Linux: hardened systemd units (service + socket activation + optional
//!   self-update timer), mirroring `contrib/systemd/`.
//! - macOS: a LaunchDaemon plist.
//!
//! Anything that may be secret (webhook secret, GitHub App configuration) is
//! written to a 0600 environment file (systemd) or a 0600 plist (launchd) —
//! never into a world-readable unit. `PRELOOP_HOME` data is never deleted by
//! `uninstall` unless `--purge-data` is given.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};

#[cfg(any(target_os = "linux", test))]
use crate::set_private_file_permissions;
use crate::{set_private_directory_permissions, write_private_file};

/// Default state directory for the service.
pub(crate) const DEFAULT_HOME: &str = "/var/lib/preloop";

/// Root-owned configuration directory for system-scope installs.
///
/// `PRELOOP_HOME` must be writable by the service (the engine creates its DB,
/// `config.toml`, and key material there), and on Unix the *directory* write
/// bit governs unlink and rename — so any file inside it can be replaced by
/// the service no matter what that file's own owner and mode are. Install
/// artifacts the service must not be able to rewrite therefore live here
/// instead of under `PRELOOP_HOME`: the systemd `EnvironmentFile` (which
/// overrides the unit's own `Environment=`, so a writable copy would let a
/// compromised service turn its sandbox off across a restart) and the staged
/// GitHub App key.
const SYSTEM_CONFIG_DIR: &str = "/etc/preloop";

/// Root-owned parent for the bootstrapped smolvm copy.
///
/// Never under `PRELOOP_HOME`: `/usr/local/bin/smolvm` points into this prefix
/// and **root** executes that path (`preloop update` probes `smolvm` before
/// deciding to reinstall), so a service-writable prefix is a direct
/// service-user → root escalation.
#[cfg(any(target_os = "linux", test))]
const SMOLVM_PREFIX_PARENT: &str = "/usr/local/lib/preloop";

/// Dedicated system account the hardened system-scope service runs under.
///
/// A guest→VMM escape lands in the SmolVM boot subprocess, which inherits the
/// service identity: running the whole control plane as root would hand an
/// escape the host. The installer creates the account (and kvm group
/// membership for /dev/kvm) at install time; user-scope units keep the
/// installing user's identity.
#[cfg(any(target_os = "linux", test))]
const SERVICE_USER: &str = "preloop";

#[cfg(any(target_os = "macos", test))]
const LAUNCHD_LABEL: &str = "dev.preloop.server";
#[cfg(any(target_os = "macos", test))]
const LAUNCHD_PLIST: &str = "/Library/LaunchDaemons/dev.preloop.server.plist";
#[cfg(target_os = "linux")]
const SYSTEMD_DIR: &str = "/etc/systemd/system";

#[derive(Debug, Parser)]
pub(crate) struct ServerArgs {
    #[command(subcommand)]
    command: ServerCommand,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ServerCommand {
    /// Install the control plane as a supervised service (systemd / launchd).
    Install(InstallArgs),
    /// Remove the service units and configuration, keeping PRELOOP_HOME data.
    Uninstall(UninstallArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct InstallArgs {
    /// Address to bind. Defaults to the engine default (127.0.0.1:9090); on
    /// Linux the port is published through socket activation.
    #[arg(long, value_name = "ADDR")]
    listen: Option<SocketAddr>,

    /// Externally reachable base URL (GitHub webhook + Checks links). GitHub
    /// must be able to reach it: a public domain with DNS + reverse proxy for
    /// production, or a tunnel (cloudflared, ngrok, Tailscale Funnel) to try
    /// it out. See docs/setup.md "Exposing the engine to GitHub".
    #[arg(long, value_name = "URL")]
    public_url: Option<String>,

    /// GitHub App id.
    #[arg(long, value_name = "ID")]
    github_app_id: Option<String>,

    /// Path to the GitHub App private key PEM.
    #[arg(long, value_name = "PATH")]
    github_app_key: Option<PathBuf>,

    /// GitHub App installation id.
    #[arg(long, value_name = "ID")]
    github_app_installation_id: Option<u64>,

    /// Shared secret for verifying `X-Hub-Signature-256`.
    #[arg(long, value_name = "SECRET")]
    webhook_secret: Option<String>,

    /// State directory (PRELOOP_HOME for the service). Default /var/lib/preloop.
    #[arg(long, value_name = "PATH")]
    home: Option<PathBuf>,

    /// Do not install the systemd self-update timer (Linux only).
    #[arg(long)]
    no_update_timer: bool,

    /// Encrypted systemd credential to mount into the service as
    /// `LoadCredentialEncrypted=preloop-secrets:PATH`. The engine reads
    /// `[secrets]`/`[repo_secrets]` from it at startup — encrypted at rest,
    /// host-key bound, decrypted into a memfd by systemd. Create it with
    /// `systemd-creds encrypt --name=preloop-secrets secrets.toml PATH`.
    /// Linux only.
    #[arg(long, value_name = "PATH")]
    systemd_credential: Option<PathBuf>,

    /// Install for the current user instead of system-wide: systemd user
    /// units (Linux) or a LaunchAgent (macOS). No root needed; state
    /// defaults to ~/.preloop. User services stop when you log out — on
    /// Linux, `sudo loginctl enable-linger $USER` keeps them running.
    #[arg(long)]
    user: bool,

    /// Print what would be written and run, without touching the system.
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
pub(crate) struct UninstallArgs {
    /// State directory the service was installed with. Default /var/lib/preloop.
    #[arg(long, value_name = "PATH")]
    home: Option<PathBuf>,

    /// Also delete the state directory and everything in it.
    #[arg(long)]
    purge_data: bool,

    /// Uninstall the per-user service (`install --user`).
    #[arg(long)]
    user: bool,

    /// Print what would be removed, without touching the system.
    #[arg(long)]
    dry_run: bool,
}

pub(crate) fn run(args: ServerArgs) -> Result<()> {
    match args.command {
        ServerCommand::Install(args) => install(args),
        ServerCommand::Uninstall(args) => uninstall(args),
    }
}

fn install(args: InstallArgs) -> Result<()> {
    if !args.user && !args.dry_run {
        require_root()?;
    }
    let home = resolve_home(args.home.as_deref(), args.user)?;
    let exe = std::env::current_exe().context("resolve current executable path")?;
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    if exe.to_string_lossy().contains("/target/") {
        eprintln!(
            "[preloop] warning: installing a development build ({}) as a service; \
             use the release installer for production",
            exe.display()
        );
    }
    if let Some(key) = &args.github_app_key {
        // Distinguish "missing" from "not readable by *you*": a system
        // dry-run is allowed without root, so a key under /root reports
        // false from a bare `exists()` and would otherwise be reported as
        // nonexistent when it is merely unreadable from this account.
        if let Err(error) = std::fs::metadata(key) {
            match error.kind() {
                std::io::ErrorKind::NotFound => {
                    bail!("--github-app-key {} does not exist", key.display())
                }
                std::io::ErrorKind::PermissionDenied => bail!(
                    "--github-app-key {} is not readable by the current user ({error}); \
                     re-run with sudo",
                    key.display()
                ),
                _ => bail!("--github-app-key {}: {error}", key.display()),
            }
        }
    }
    // A system install runs as the dedicated `preloop` account, which cannot
    // traverse a 0700 /home/<user> or /root. Such a --home would produce an
    // install that looks fine and then fails at first start, so reject it
    // here rather than shipping a broken unit.
    #[cfg(any(target_os = "linux", test))]
    if !args.user && home_blocked_by_protect_home(&home) {
        bail!(
            "--home {} is unusable for a system install: the `{SERVICE_USER}` service \
             account cannot traverse /home, /root, or /run/user. Use {DEFAULT_HOME} \
             (the default) or another root-reachable path, or install with --user.",
            home.display()
        );
    }
    // The state dir is chowned to the service account recursively, so it must
    // be a directory dedicated to Preloop — never a shared system root.
    #[cfg(any(target_os = "linux", test))]
    if !args.user {
        if let Some(protected) = shared_system_path(&home) {
            bail!(
                "--home {} is too broad for a system install: the state directory is \
                 chowned to `{SERVICE_USER}` recursively, which would hand it {}. \
                 Use {DEFAULT_HOME} (the default) or another dedicated directory.",
                home.display(),
                protected.display()
            );
        }
    }
    // The service must be able to execute the binary the unit points at. An
    // exe under a 0700 /home/<user> or /root is unreachable for the same
    // traversal reason as the state dir, and the unit would restart-loop.
    #[cfg(any(target_os = "linux", test))]
    if !args.user && home_blocked_by_protect_home(&exe) {
        bail!(
            "the running executable {} is under a directory the `{SERVICE_USER}` service \
             account cannot traverse, so the unit could never start it. Install the \
             binary system-wide first (e.g. /usr/local/bin/preloop) and re-run, or \
             install with --user.",
            exe.display()
        );
    }
    // The state dir must exist before anything is staged into it: the very
    // first system install with --github-app-key targets a not-yet-created
    // (default or nested) directory, and staging the key copy would fail
    // without it. prepare_home is idempotent and dry-run prints only.
    prepare_home(&home, args.dry_run)?;
    // The service account and the root-owned config dir must both exist
    // before the key is staged into the latter and chowned to the former.
    #[cfg(target_os = "linux")]
    if !args.user {
        ensure_service_user(&home, args.dry_run)?;
        prepare_system_config_dir(args.dry_run)?;
    }
    let config_dir = if args.user {
        home.clone()
    } else {
        PathBuf::from(SYSTEM_CONFIG_DIR)
    };
    let staged_key = staged_app_key(&args, &config_dir)?;
    // The staged copy is root-owned 0600 until here; the service still has to
    // read it, so widen it to root:preloop 0640 now that the account exists.
    #[cfg(target_os = "linux")]
    if !args.user && !args.dry_run {
        if let Some(key) = &staged_key {
            grant_service_read(key)
                .with_context(|| format!("grant the service read access to {}", key.display()))?;
        }
    }
    let env_lines = config_env_lines(&args, staged_key.as_deref())?;
    if let Some(path) = &args.systemd_credential {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            bail!("--systemd-credential is Linux-only (systemd credentials)");
        }
        #[cfg(target_os = "linux")]
        if !path.exists() {
            bail!(
                "--systemd-credential {} does not exist — encrypt it first with \
                 `systemd-creds encrypt --name=preloop-secrets secrets.toml {}`",
                path.display(),
                path.display()
            );
        }
    }

    #[cfg(target_os = "linux")]
    return install_systemd(&args, &home, &exe, &env_lines);
    #[cfg(target_os = "macos")]
    return install_launchd(&args, &home, &exe, &env_lines);
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (&args, &home, &exe, &env_lines);
        bail!("server install is supported on Linux (systemd) and macOS (launchd) only")
    }
}

fn uninstall(args: UninstallArgs) -> Result<()> {
    if !args.user && !args.dry_run {
        require_root()?;
    }
    let home = resolve_home(args.home.as_deref(), args.user)?;

    #[cfg(target_os = "linux")]
    uninstall_systemd(&args, &home)?;
    #[cfg(target_os = "macos")]
    uninstall_launchd(&args, &home)?;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = &home;
        bail!("server uninstall is supported on Linux (systemd) and macOS (launchd) only")
    }

    let env_file = home.join("environment");
    if env_file.exists() {
        remove_path(&env_file, args.dry_run)?;
    }
    if args.purge_data {
        if home.exists() {
            if args.dry_run {
                eprintln!("[preloop] would delete state directory {}", home.display());
            } else {
                std::fs::remove_dir_all(&home)
                    .with_context(|| format!("delete {}", home.display()))?;
            }
        }
    } else if home.exists() {
        eprintln!(
            "[preloop] kept {} (data); pass --purge-data to delete it",
            home.display()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// systemd (Linux)
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn install_systemd(
    args: &InstallArgs,
    home: &Path,
    exe: &Path,
    env_lines: &[String],
) -> Result<()> {
    let dry = args.dry_run;
    let dir = systemd_unit_dir(args.user);
    let service = render_systemd_service(exe, home, args.user, args.systemd_credential.as_deref())?;
    let socket = render_systemd_socket(args.listen);
    let update_service = render_systemd_update_service(exe, home, args.user)?;
    let timer = render_systemd_update_timer();

    if !args.user {
        bootstrap_system_smolvm(dry)?;
        bootstrap_smolvm_data(home, dry)?;
        migrate_legacy_env_file(home, dry)?;
        chown_state_dir(home, dry)?;
    }
    // Written after chown_state_dir, and deliberately outside it for a system
    // install: the env file lives in the root-owned config dir, so the
    // recursive chown of the state dir never reaches it.
    write_env_file(&env_file_path(home, args.user), env_lines, dry)?;

    // A rootless install cannot assume `~/.config/systemd/user` exists on a
    // fresh machine — create the unit directory before the first write.
    if !dir.exists() {
        if dry {
            eprintln!("[preloop] would create unit directory {}", dir.display());
        } else {
            std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        }
    }
    write_unit(&dir.join("preloop.service"), &service, dry)?;
    write_unit(&dir.join("preloop.socket"), &socket, dry)?;
    if !args.no_update_timer {
        write_unit(&dir.join("preloop-update.service"), &update_service, dry)?;
        write_unit(&dir.join("preloop-update.timer"), &timer, dry)?;
    }

    run_systemctl(&systemctl_args(args.user, &["daemon-reload"]), dry)?;
    // Upgrades: `enable --now` starts an inactive unit but leaves a running
    // one alone, so an existing service would keep its old identity and an
    // unhardened environment until it happened to fail or the host rebooted.
    // `try-restart` restarts it only if it is already active, so exactly one
    // start happens either way: restart here for an upgrade, or the
    // `enable --now` below for a fresh install.
    run_ok(
        "systemctl",
        &systemctl_args(args.user, &["try-restart", "preloop.service"]),
        dry,
        "restart a running preloop.service onto the new unit",
    );
    run_systemctl(
        &systemctl_args(
            args.user,
            &["enable", "--now", "preloop.socket", "preloop.service"],
        ),
        dry,
    )?;
    if !args.no_update_timer {
        run_systemctl(
            &systemctl_args(args.user, &["enable", "--now", "preloop-update.timer"]),
            dry,
        )?;
    }
    if let Some(path) = &args.systemd_credential {
        eprintln!(
            "[preloop] credential: {} mounted as LoadCredentialEncrypted=preloop-secrets \
             (secrets from it override config.toml)",
            path.display()
        );
    }

    let identity = if args.user {
        String::new()
    } else {
        // Self-terminating block: the template supplies the leading indent,
        // this supplies the trailing one for whatever follows.
        format!(
            "identity: dedicated `{SERVICE_USER}` system account (kvm group for /dev/kvm);\n\
             \x20          state dir chowned to it — config.toml must be written as that user:\n\
             \x20          sudo -u {SERVICE_USER} env PRELOOP_HOME={} preloop setup github --save\n\
             \x20 ",
            home.display()
        )
    };
    let secrets = format!(
        "secrets:   --webhook-secret/--github-app-* flags land in {} (0600);\n\
         \x20          `setup github --save` writes config.toml (0600). Define a webhook\n\
         \x20          secret or GitHub webhook delivery is rejected.",
        env_file_path(home, args.user).display()
    );
    eprintln!(
        "[preloop] installed Preloop control plane as a {} systemd service:\n\
         \x20 units:   {}/preloop.{{service,socket}}{}\n\
         \x20 state:   {} (0700), service config {} (0600)\n\
         \x20 status:  systemctl {} status preloop\n\
         \x20 logs:    journalctl {} -u preloop -f\n\
         \x20 GitHub:  re-run with --github-app-* flags, or run\n\
         \x20          {}preloop setup github --save\n\
         \x20 {}{}{}{}",
        if args.user { "user-scope" } else { "system" },
        dir.display(),
        if args.no_update_timer {
            ""
        } else {
            " + preloop-update.{service,timer}"
        },
        home.display(),
        env_file_path(home, args.user).display(),
        if args.user { "--user" } else { "" },
        if args.user { "--user" } else { "" },
        if args.user {
            format!("PRELOOP_HOME={} ", home.display())
        } else {
            format!(
                "sudo -u {SERVICE_USER} env PRELOOP_HOME={} ",
                home.display()
            )
        },
        identity,
        secrets,
        if args.user {
            "\n\
             \x20 note:    user units stop at logout — `sudo loginctl enable-linger $USER`\n\
             \x20          keeps them running after you sign out"
        } else {
            ""
        },
        webhook_hint(args.public_url.as_deref()),
    );
    Ok(())
}

/// Whether a system account exists.
#[cfg(target_os = "linux")]
fn account_exists(name: &str) -> bool {
    Command::new("getent")
        .args(["passwd", name])
        // `.output()` not `.status()`: getent prints the matched passwd line,
        // which would land in the middle of the install plan.
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Create the dedicated service account (idempotent) and add it to the `kvm`
/// group when the host exposes `/dev/kvm` — the VMM opens it as the service
/// identity. An existing account is never modified beyond group membership.
#[cfg(target_os = "linux")]
fn ensure_service_user(home: &Path, dry_run: bool) -> Result<()> {
    if !account_exists(SERVICE_USER) {
        let home_str = home.to_string_lossy();
        let args = [
            "--system",
            "--home-dir",
            &home_str,
            "--shell",
            "/usr/sbin/nologin",
            "--comment",
            "Preloop service account",
            SERVICE_USER,
        ];
        if dry_run {
            eprintln!(
                "[preloop] would create system user {SERVICE_USER} (useradd {})",
                args.join(" ")
            );
        } else {
            match Command::new("useradd").args(args).status() {
                Ok(status) if status.success() => {}
                Ok(status) => bail!("useradd {SERVICE_USER} failed ({status})"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => bail!(
                    "`useradd` not found; create the service account manually and re-run: \
                     useradd --system --home-dir {} --shell /usr/sbin/nologin {SERVICE_USER}",
                    home.display()
                ),
                Err(error) => {
                    return Err(error).with_context(|| format!("run useradd {SERVICE_USER}"))
                }
            }
        }
    }
    // The unit pins `Group=preloop` and the chowns use `preloop:preloop`, so
    // the group must exist even when a pre-existing passwd entry made us skip
    // useradd (which creates the user's primary group by default).
    let group_exists = Command::new("getent")
        .args(["group", SERVICE_USER])
        .output()
        .is_ok_and(|output| output.status.success());
    if !group_exists {
        if dry_run {
            eprintln!("[preloop] would create system group {SERVICE_USER} (groupadd --system)");
        } else {
            let status = Command::new("groupadd")
                .args(["--system", SERVICE_USER])
                .status()
                .with_context(|| format!("create system group {SERVICE_USER}"))?;
            if !status.success() {
                bail!("groupadd {SERVICE_USER} failed ({status})");
            }
        }
    }
    add_to_kvm_group(dry_run)?;
    Ok(())
}

/// The VMM opens `/dev/kvm` (root:kvm, 0660) under the service identity.
/// Only meaningful when the host actually exposes KVM.
#[cfg(target_os = "linux")]
fn add_to_kvm_group(dry_run: bool) -> Result<()> {
    if !Path::new("/dev/kvm").exists() {
        return Ok(());
    }
    let group_exists = Command::new("getent")
        .args(["group", "kvm"])
        .output()
        .is_ok_and(|output| output.status.success());
    if !group_exists {
        eprintln!(
            "[preloop] warning: /dev/kvm exists but there is no `kvm` group; \
             the service user cannot open /dev/kvm"
        );
        return Ok(());
    }
    if dry_run {
        eprintln!(
            "[preloop] would add {SERVICE_USER} to the kvm group \
             (usermod -aG kvm {SERVICE_USER})"
        );
        return Ok(());
    }
    let status = Command::new("usermod")
        .args(["-aG", "kvm", SERVICE_USER])
        .status();
    if !status.as_ref().is_ok_and(|status| status.success()) {
        eprintln!(
            "[preloop] warning: adding {SERVICE_USER} to the kvm group failed ({status:?}); \
             the VM pool cannot open /dev/kvm"
        );
    }
    Ok(())
}

/// Ensure the service can resolve `smolvm` on its PATH.
///
/// The unit runs with the stock systemd service PATH and a HOME under the
/// state dir, so an install under `/root/.local/bin` (where `preloop update`
/// and the official installer put it) is invisible and untraversable to the
/// service account. When no system-wide smolvm exists, copy root's prefix
/// into [`SMOLVM_PREFIX_PARENT`] and link it onto the PATH: the wrapper script
/// resolves its own location, so the copy is self-contained (binary, libs,
/// bundled agent rootfs).
///
/// The copy stays **root-owned and read-only to the service** (`a+rX`, never
/// chowned), and lives outside `PRELOOP_HOME`. `/usr/local/bin/smolvm` points
/// into it and root executes that path — `preloop update` probes `smolvm` for
/// its version and `--mount-socket` support before deciding to reinstall — so
/// a prefix the service could write, or a prefix inside a directory the
/// service could unlink entries from, would hand a guest→VMM escape a root
/// shell on the next `sudo preloop update`. The service needs read and execute
/// on this tree, nothing more; its mutable data lives in `SMOLVM_DATA_DIR`
/// under the state dir.
///
/// Lifecycle: the copy is refreshed on every re-install when the source
/// install is newer (after `sudo preloop update`, re-running
/// `sudo preloop server install` picks the new version up), and the managed
/// `/usr/local/bin/smolvm` symlink is always re-pointed. An independently
/// installed system smolvm (not our symlink) is left alone — the installer
/// never shadows an operator-managed binary. Refreshes are atomic: the new
/// prefix is fully assembled (copied, pruned, made world-readable) in a
/// sibling staging directory and swapped into place with a rename, so a
/// running service never observes a half-copied prefix; running VMMs hold
/// open inodes and are unaffected. Root's machine database is never imported
/// (it describes VMs the service cannot see).
#[cfg(target_os = "linux")]
fn bootstrap_system_smolvm(dry_run: bool) -> Result<()> {
    let managed_link = Path::new("/usr/local/bin/smolvm");
    let parent = Path::new(SMOLVM_PREFIX_PARENT);
    let destination = parent.join("smolvm-prefix");
    // `smolvm` is the wrapper script; `smolvm-bin` is the real binary the
    // freshness check must compare (the copied prefix mirrors the source
    // layout exactly).
    let destination_bin = destination.join("smolvm");
    let destination_binary = destination.join("smolvm-bin");
    if independent_system_smolvm(managed_link, &destination_bin) {
        return Ok(());
    }
    let source_link = Path::new("/root/.local/bin/smolvm");
    let source_prefix = Path::new("/root/.smolvm");
    if !source_link.exists() || !source_prefix.is_dir() {
        eprintln!(
            "[preloop] note: no smolvm on the service PATH and none installed for root; \
             the VM pool needs `sudo preloop update` (or smolvm in /usr/local/bin) \
             before it can launch machines"
        );
        return Ok(());
    }
    let stale = smolvm_copy_stale(&source_prefix.join("smolvm-bin"), &destination_binary);
    if dry_run {
        if stale {
            eprintln!(
                "[preloop] would copy {} into {} (root-owned, read-only to the service)",
                source_prefix.display(),
                destination.display()
            );
        }
        eprintln!(
            "[preloop] would symlink /usr/local/bin/smolvm -> {}",
            destination_bin.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    set_world_readable_directory(parent)?;
    if stale {
        let staging = parent.join(format!("smolvm-prefix.staging-{}", std::process::id()));
        let backup = parent.join(format!("smolvm-prefix.backup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&staging);
        let _ = std::fs::remove_dir_all(&backup);
        let assembled = (|| -> Result<()> {
            std::fs::create_dir_all(&staging)
                .with_context(|| format!("create {}", staging.display()))?;
            let status = Command::new("cp")
                .arg("-a")
                .arg(format!("{}/.", source_prefix.display()))
                .arg(format!("{}/", staging.display()))
                .status()
                .with_context(|| {
                    format!(
                        "copy {} into {}",
                        source_prefix.display(),
                        staging.display()
                    )
                })?;
            if !status.success() {
                bail!(
                    "copying {} into {} failed ({status})",
                    source_prefix.display(),
                    staging.display()
                );
            }
            // The copied database describes the root user's VMs — the service
            // starts from a clean record instead of importing invisible
            // machines.
            for stale_db in ["smolvm.db", "smolvm.db-wal", "smolvm.db-shm"] {
                let _ = std::fs::remove_file(staging.join(stale_db));
            }
            // Root's ~/.smolvm is typically 0700; the service must be able to
            // read and execute the copy without being able to write it. Files
            // stay root-owned — `a+rX` adds read (and execute where already
            // executable), never write.
            let status = Command::new("chmod")
                .args(["-R", "a+rX"])
                .arg(&staging)
                .status()
                .with_context(|| format!("chmod {}", staging.display()))?;
            if !status.success() {
                bail!("chmod {} failed ({status})", staging.display());
            }
            Ok(())
        })();
        if let Err(error) = assembled {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        atomic_directory_swap(&destination, &staging, &backup)?;
    }
    // Idempotent repair: an earlier release chowned this prefix to the service
    // account, which is exactly the escalation path above. Take it back.
    let status = Command::new("chown")
        .args(["-R", "root:root"])
        .arg(&destination)
        .status()
        .with_context(|| format!("chown {}", destination.display()))?;
    if !status.success() {
        bail!("chown {} failed ({status})", destination.display());
    }
    let status = Command::new("chmod")
        .args(["-R", "a+rX"])
        .arg(&destination)
        .status()
        .with_context(|| format!("chmod {}", destination.display()))?;
    if !status.success() {
        bail!("chmod {} failed ({status})", destination.display());
    }
    // The symlink lands in the standard system bin dir; make sure it exists
    // (some minimal systems ship without it).
    std::fs::create_dir_all("/usr/local/bin").context("create /usr/local/bin")?;
    let status = Command::new("ln")
        .args(["-sfn"])
        .arg(&destination_bin)
        .arg("/usr/local/bin/smolvm")
        .status()
        .with_context(|| "symlink /usr/local/bin/smolvm".to_string())?;
    if !status.success() {
        bail!("symlinking /usr/local/bin/smolvm failed ({status})");
    }
    Ok(())
}

/// Copy the bundled SmolVM *data assets* into the service's data directory.
///
/// `preloop update` installs the immutable assets — the agent rootfs and
/// Linux's `init.krun` — into the data directory, not the prefix:
/// `update.rs` writes `agent-rootfs` and `init.krun` under
/// `~/.local/share/smolvm` (the default `data_dir`), while only the binary,
/// libs, and templates go into `~/.smolvm`. The unit pins
/// `SMOLVM_DATA_DIR` to a fresh `PRELOOP_HOME/smolvm`, so without this copy
/// the service's first VM boot would find no agent rootfs and no init and
/// fail before ever reaching the sandbox. The root user's machine database
/// stays behind, exactly as for the prefix copy.
#[cfg(target_os = "linux")]
fn bootstrap_smolvm_data(home: &Path, dry_run: bool) -> Result<()> {
    let source_dir = Path::new("/root/.local/share/smolvm");
    let destination_dir = home.join("smolvm");
    if !source_dir.is_dir() {
        // No root-side install to copy; the note for the prefix covers this.
        return Ok(());
    }
    let needs_copy = |name: &str| -> bool {
        let destination = destination_dir.join(name);
        let source = source_dir.join(name);
        !destination.exists() || newer_than(&source, &destination)
    };
    let assets: Vec<&str> = ["agent-rootfs", "init.krun"]
        .into_iter()
        .filter(|name| source_dir.join(name).exists() && needs_copy(name))
        .collect();
    if assets.is_empty() {
        return Ok(());
    }
    if dry_run {
        eprintln!(
            "[preloop] would copy smolvm data assets ({}) into {}",
            assets.join(", "),
            destination_dir.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(&destination_dir)
        .with_context(|| format!("create {}", destination_dir.display()))?;
    for name in assets {
        let source = source_dir.join(name);
        let destination = destination_dir.join(name);
        // Copy as root, then hand the tree to the service account: the data
        // dir is service-owned (the recursive chown runs after this), and the
        // agent rootfs is read-only for the VMM.
        std::fs::create_dir_all(&destination)
            .with_context(|| format!("create {}", destination.display()))?;
        let status = Command::new("cp")
            .args(["-a"])
            .arg(format!("{}/.", source.display()))
            .arg(format!("{}/", destination.display()))
            .status()
            .with_context(|| format!("copy {} into {}", source.display(), destination.display()))?;
        if !status.success() {
            bail!(
                "copying {} into {} failed ({status})",
                source.display(),
                destination.display()
            );
        }
    }
    // `init.krun` needs its executable bit for the boot path; `cp -a` keeps
    // it from the source. Re-assert it explicitly so a filesystem that drops
    // the bit cannot break the first boot.
    let init_krun = destination_dir.join("init.krun");
    if init_krun.exists() {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&init_krun)?.permissions();
        permissions.set_mode(permissions.mode() | 0o500);
        std::fs::set_permissions(&init_krun, permissions)?;
    }
    Ok(())
}

/// Atomically replace `destination` with a fully-assembled `staging`
/// directory: rename the current prefix aside, rename staging into place,
/// then drop the backup. If the swap fails the previous prefix is restored.
/// Staging and backup are always cleaned up, on success and failure alike.
#[cfg(any(target_os = "linux", test))]
fn atomic_directory_swap(destination: &Path, staging: &Path, backup: &Path) -> Result<()> {
    let had_previous = destination.exists();
    if had_previous {
        std::fs::rename(destination, backup).with_context(|| {
            format!(
                "set aside old {} to {}",
                destination.display(),
                backup.display()
            )
        })?;
    }
    if let Err(error) = std::fs::rename(staging, destination) {
        if had_previous {
            let _ = std::fs::rename(backup, destination);
        }
        let _ = std::fs::remove_dir_all(staging);
        return Err(error)
            .with_context(|| format!("swap {} into {}", staging.display(), destination.display()));
    }
    if had_previous {
        let _ = std::fs::remove_dir_all(backup);
    }
    Ok(())
}

/// Whether an independently managed smolvm already exists on the service
/// PATH. The installer's own `/usr/local/bin/smolvm` symlink into the service
/// prefix is NOT independent: it may be stale relative to a newer source
/// install and must be refreshed, never mistaken for a current binary.
#[cfg(any(target_os = "linux", test))]
fn independent_system_smolvm(managed_link: &Path, destination_bin: &Path) -> bool {
    let is_managed = std::fs::read_link(managed_link).is_ok_and(|target| target == destination_bin);
    !is_managed
        && [
            managed_link,
            Path::new("/usr/bin/smolvm"),
            Path::new("/bin/smolvm"),
        ]
        .iter()
        .any(|candidate| candidate.exists())
}

/// Whether the service copy of smolvm is stale relative to the source
/// install: missing entirely (first install), or older than the source
/// binary (refresh after an update).
#[cfg(any(target_os = "linux", test))]
fn smolvm_copy_stale(source_bin: &Path, destination_bin: &Path) -> bool {
    !destination_bin.exists() || newer_than(source_bin, destination_bin)
}

/// Whether `source`'s mtime is newer than `destination`'s; unknown metadata
/// counts as stale so the copy is made.
#[cfg(any(target_os = "linux", test))]
fn newer_than(source: &Path, destination: &Path) -> bool {
    match (
        std::fs::metadata(source).and_then(|meta| meta.modified()),
        std::fs::metadata(destination).and_then(|meta| meta.modified()),
    ) {
        (Ok(source), Ok(destination)) => source > destination,
        _ => true,
    }
}

/// Hand the state directory to the service account so the non-root service
/// can initialize and own its state (config.toml, DB, smolvm data, copied
/// smolvm prefix). Runs as root at install; a no-op for user-scope installs.
#[cfg(target_os = "linux")]
fn chown_state_dir(home: &Path, dry_run: bool) -> Result<()> {
    if dry_run {
        eprintln!(
            "[preloop] would chown -R {} to {SERVICE_USER}:{SERVICE_USER}",
            home.display()
        );
        return Ok(());
    }
    let status = Command::new("chown")
        .arg("-R")
        .arg(format!("{SERVICE_USER}:{SERVICE_USER}"))
        .arg(home)
        .status()
        .with_context(|| format!("chown {}", home.display()))?;
    if !status.success() {
        bail!("chown {} failed ({status})", home.display());
    }
    Ok(())
}

/// Resolve the GitHub App key path the environment file will reference.
///
/// Linux system scope: the service reads the key under its dedicated
/// non-root account, and a key left in the caller's tree — e.g. under
/// `/root` — is unreachable no matter how it is chowned, because the service
/// user cannot traverse the parent directory. The installer therefore stages
/// a copy in the root-owned [`SYSTEM_CONFIG_DIR`], which [`grant_service_read`]
/// then hands to the service as `root:preloop` mode `0640`: readable by the
/// service and nothing more. It deliberately does not
/// live under `PRELOOP_HOME`, because the service can unlink and recreate any
/// entry in a directory it owns, which would let it swap its own App key.
/// The caller's original is never modified. Every other scope keeps the
/// original path.
#[cfg(any(target_os = "linux", test))]
fn staged_app_key(args: &InstallArgs, config_dir: &Path) -> Result<Option<PathBuf>> {
    let Some(source) = args.github_app_key.clone() else {
        return Ok(None);
    };
    if args.user {
        return Ok(Some(source));
    }
    let destination = config_dir.join("github-app-key.pem");
    if std::fs::canonicalize(&source).is_ok_and(|resolved| resolved == destination) {
        // Already the staged file (re-install with the staged path).
        return Ok(Some(destination));
    }
    if args.dry_run {
        eprintln!(
            "[preloop] would copy {} to {} (0640 root:{SERVICE_USER}, read-only to the service)",
            source.display(),
            destination.display()
        );
        return Ok(Some(destination));
    }
    std::fs::copy(&source, &destination)
        .with_context(|| format!("copy {} to {}", source.display(), destination.display()))?;
    // Staged private by default; `grant_service_read` widens it to the service
    // group once the account is known to exist. Ownership is an install-time
    // concern, kept out of the staging logic so this stays testable without
    // root or a `preloop` account.
    set_private_file_permissions(&destination)
        .with_context(|| format!("chmod 0600 {}", destination.display()))?;
    Ok(Some(destination))
}

/// Create the root-owned system config directory: `0750 root:{SERVICE_USER}`
/// so the service can traverse in to read its key, but cannot create, rename,
/// or unlink anything inside it.
#[cfg(target_os = "linux")]
fn prepare_system_config_dir(dry_run: bool) -> Result<()> {
    let dir = Path::new(SYSTEM_CONFIG_DIR);
    if dry_run {
        eprintln!(
            "[preloop] would create {} (0750 root:{SERVICE_USER})",
            dir.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o750))
        .with_context(|| format!("chmod 0750 {}", dir.display()))?;
    let status = Command::new("chown")
        .arg(format!("root:{SERVICE_USER}"))
        .arg(dir)
        .status()
        .with_context(|| format!("chown {}", dir.display()))?;
    if !status.success() {
        bail!("chown {} failed ({status})", dir.display());
    }
    Ok(())
}

/// Hand a root-owned file to the service for reading only: `root:{SERVICE_USER}`
/// mode `0640`. Requires the service account to exist, so it runs after
/// [`ensure_service_user`].
#[cfg(target_os = "linux")]
fn grant_service_read(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o640))
        .with_context(|| format!("chmod 0640 {}", path.display()))?;
    let status = Command::new("chown")
        .arg(format!("root:{SERVICE_USER}"))
        .arg(path)
        .status()
        .with_context(|| format!("chown {}", path.display()))?;
    if !status.success() {
        bail!("chown {} failed ({status})", path.display());
    }
    Ok(())
}

/// `0755` — traversable and readable by every account, writable only by root.
#[cfg(target_os = "linux")]
fn set_world_readable_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .with_context(|| format!("chmod 0755 {}", path.display()))
}

/// Non-Linux installs keep the caller's key path (launchd runs as root, so
/// traversal is not an issue).
#[cfg(all(not(target_os = "linux"), not(test)))]
fn staged_app_key(args: &InstallArgs, _config_dir: &Path) -> Result<Option<PathBuf>> {
    Ok(args.github_app_key.clone())
}

#[cfg(target_os = "linux")]
fn uninstall_systemd(args: &UninstallArgs, home: &Path) -> Result<()> {
    let _ = home;
    let dry = args.dry_run;
    // Stop first and treat failure as fatal: deleting the unit files while a
    // service is still active would report success with the engine running
    // from a now-orphaned unit. The service and socket are always installed;
    // the update timer may never have been, so its disable stays tolerant.
    run_systemctl(
        &systemctl_args(
            args.user,
            &["disable", "--now", "preloop.socket", "preloop.service"],
        ),
        dry,
    )?;
    run_ok(
        "systemctl",
        &systemctl_args(args.user, &["disable", "--now", "preloop-update.timer"]),
        dry,
        "disable the update timer",
    );
    // Belt and braces: verify nothing is left running before removing units.
    if !dry {
        let status = Command::new("systemctl")
            .args(systemctl_args(args.user, &["is-active", "preloop.service"]))
            .status()
            .context("check preloop.service state")?;
        if status.success() {
            bail!("preloop.service is still active after disable --now — aborting uninstall");
        }
    } else {
        eprintln!(
            "[preloop] would run: systemctl {} is-active preloop.service",
            if args.user { "--user" } else { "" }
        );
    }
    let dir = systemd_unit_dir(args.user);
    for unit in [
        "preloop.service",
        "preloop.socket",
        "preloop-update.service",
        "preloop-update.timer",
    ] {
        remove_path(&dir.join(unit), dry)?;
    }
    run_ok(
        "systemctl",
        &systemctl_args(args.user, &["daemon-reload"]),
        dry,
        "reload systemd",
    );
    // Secrets outlive --purge-data otherwise: for a system install the
    // environment file and the staged App key live in the root-owned config
    // dir, not under the state dir that --purge-data removes.
    if !args.user {
        for artifact in ["environment", "github-app-key.pem"] {
            let path = Path::new(SYSTEM_CONFIG_DIR).join(artifact);
            if path.exists() || dry {
                remove_path(&path, dry)?;
            }
        }
        // Only our own managed symlink, never an operator-installed binary.
        let managed_link = Path::new("/usr/local/bin/smolvm");
        let prefix = Path::new(SMOLVM_PREFIX_PARENT).join("smolvm-prefix");
        if std::fs::read_link(managed_link).is_ok_and(|target| target.starts_with(&prefix)) {
            remove_path(managed_link, dry)?;
        }
        if prefix.is_dir() {
            if dry {
                eprintln!("[preloop] would remove {}", prefix.display());
            } else {
                std::fs::remove_dir_all(&prefix)
                    .with_context(|| format!("remove {}", prefix.display()))?;
            }
        }
    }
    eprintln!(
        "[preloop] removed Preloop {} systemd units",
        if args.user { "user" } else { "system" }
    );
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn render_systemd_service(
    exe: &Path,
    home: &Path,
    user: bool,
    credential: Option<&Path>,
) -> Result<String> {
    let exe_display = systemd_path(exe);
    let identity = if user {
        String::new()
    } else {
        // Dedicated service identity: an escape from the guest cannot reach
        // root. The account (plus kvm group membership) is created by the
        // installer; the state dir is chowned to it.
        format!("User={SERVICE_USER}\nGroup={SERVICE_USER}\n")
    };
    let data_root = if user {
        String::new()
    } else {
        // Pin SmolVM's data (machine records, VM disks, agent rootfs) under
        // the state dir so it follows --home and stays owned by the service
        // user — never the operator's or root's XDG tree.
        format!(
            "Environment=HOME={}\nEnvironment=SMOLVM_DATA_DIR={}/smolvm\n",
            systemd_path(home),
            systemd_path(home)
        )
    };
    let delegation = if user {
        String::new()
    } else {
        // Delegate this unit's cgroup subtree so `_boot-vm` can place each VM
        // in its own capped `vm-<pid>` leaf (cpu/pids/memory). The provider
        // only passes SMOLVM_CGROUP_ROOT when it observes this delegation.
        "Delegate=cpu memory pids\n".to_owned()
    };
    Ok(format!(
        r#"[Unit]
Description=Preloop self-hosted GitHub Actions control plane
Requires=preloop.socket
After=preloop.socket network-online.target

[Service]
Type=simple
ExecStart={exe_display} serve
Environment=PRELOOP_HOME={home}
EnvironmentFile=-{env_file}
{credential}{identity}{data_root}Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=read-only
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectClock=true
LockPersonality=true
RestrictRealtime=true
CapabilityBoundingSet=
{delegation}{state_directory}{readwrite}[Install]
WantedBy={wanted_by}
"#,
        home = home.display(),
        env_file = systemd_path(&env_file_path(home, user)),
        state_directory = state_directory_line(home, user),
        wanted_by = if user {
            "default.target"
        } else {
            "multi-user.target"
        },
        credential = credential
            .map(|path| {
                format!(
                    "LoadCredentialEncrypted=preloop-secrets:{}\n",
                    path.display()
                )
            })
            .unwrap_or_default(),
        readwrite = readwrite_paths(exe, home, user, false),
    ))
}

/// Directory holding the service units for the requested scope.
#[cfg(target_os = "linux")]
fn systemd_unit_dir(user: bool) -> PathBuf {
    if user {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
            .unwrap_or_else(|| PathBuf::from(".config"));
        base.join("systemd/user")
    } else {
        PathBuf::from(SYSTEMD_DIR)
    }
}

/// `systemctl` invocation for the requested scope.
#[cfg(any(target_os = "linux", test))]
fn systemctl_args(user: bool, args: &[&str]) -> Vec<String> {
    let mut out = Vec::with_capacity(args.len() + 1);
    if user {
        out.push("--user".to_owned());
    }
    out.extend(args.iter().map(|arg| arg.to_string()));
    out
}

#[cfg(any(target_os = "linux", test))]
fn render_systemd_update_service(exe: &Path, home: &Path, user: bool) -> Result<String> {
    let exe_display = systemd_path(exe);
    Ok(format!(
        r#"[Unit]
Description=Update the Preloop binary from GitHub Releases
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart={exe_display} update
Environment=PRELOOP_HOME={home}
EnvironmentFile=-{env_file}
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=read-only
{readwrite}"#,
        home = home.display(),
        env_file = systemd_path(&env_file_path(home, user)),
        readwrite = readwrite_paths(exe, home, user, true),
    ))
}

/// `ReadWritePaths=` line for a unit.
///
/// Only the update unit may rewrite the executable's directory
/// (`replaceable_exe`): the self-update timer replaces the binary under
/// `ProtectSystem=full`. The serving unit must NOT be able to write its own
/// binary — a VMM escape under the service identity planting a new
/// `ExecStart` target for the next root start to execute is an escalation
/// path. The state dir is carved out only for user scope, where
/// `ProtectHome=read-only` would otherwise block `~`; a system install under
/// one of those roots is rejected outright by [`install`], and the default
/// /var/lib/preloop needs no carve-out. Paths are quoted per
/// systemd's path-list syntax when they contain whitespace; an unquoted path
/// with a space would split the list and the whitelist would silently miss
/// the real directory.
#[cfg(any(target_os = "linux", test))]
fn readwrite_paths(exe: &Path, home: &Path, user: bool, replaceable_exe: bool) -> String {
    let mut paths = Vec::new();
    if replaceable_exe {
        if let Some(dir) = readwrite_dir(exe) {
            paths.push(systemd_path(&dir));
        }
    }
    if user {
        paths.push(systemd_path(home));
    }
    if paths.is_empty() {
        String::new()
    } else {
        format!("ReadWritePaths={}\n", paths.join(" "))
    }
}

/// `StateDirectory=` line for the default state directory: systemd then
/// creates and chowns `/var/lib/preloop` for the service identity at every
/// start. Custom `--home` paths cannot be expressed as a StateDirectory name,
/// so those rely on the installer's chown instead.
#[cfg(any(target_os = "linux", test))]
fn state_directory_line(home: &Path, user: bool) -> String {
    if !user && home == Path::new(DEFAULT_HOME) {
        "StateDirectory=preloop\nStateDirectoryMode=0700\n".to_owned()
    } else {
        String::new()
    }
}

/// Where the unit's `EnvironmentFile=` lives.
///
/// System scope puts it in the root-owned [`SYSTEM_CONFIG_DIR`] so the service
/// identity cannot rewrite (or unlink and recreate) the file systemd feeds it;
/// `EnvironmentFile=` overrides the unit's `Environment=`, so a service-owned
/// copy would let a compromised VMM persist `SMOLVM_SECCOMP=off` across the
/// next `Restart=on-failure`. User scope has no privilege boundary to defend,
/// so it keeps the file beside the rest of the state.
#[cfg(any(target_os = "linux", test))]
fn env_file_path(home: &Path, user: bool) -> PathBuf {
    if user {
        home.join("environment")
    } else {
        Path::new(SYSTEM_CONFIG_DIR).join("environment")
    }
}

/// `ProtectHome=read-only` makes these roots unwritable; a state dir under
/// one of them needs an explicit `ReadWritePaths` carve-out (user scope), and
/// is unusable altogether for a system install: the dedicated service account
/// cannot traverse a `0700` `/home/<someone>` or `/root` no matter how the
/// state dir itself is owned. [`install`] rejects those up front.
#[cfg(any(target_os = "linux", test))]
fn home_blocked_by_protect_home(home: &Path) -> bool {
    home.starts_with("/home/") || home.starts_with("/root") || home.starts_with("/run/user")
}

/// A shared system location `home` would swallow, or `None` when `home` is a
/// directory Preloop may own outright.
///
/// The installer chowns the state directory to the service account
/// *recursively*, so `--home /var/lib` or `--home /` would silently reassign
/// unrelated host data. A candidate is rejected when it *is* a protected
/// path or is an ancestor of one; `/var/lib/preloop` (the default) and
/// `/srv/preloop` are ancestors of nothing and pass.
#[cfg(any(target_os = "linux", test))]
fn shared_system_path(home: &Path) -> Option<&'static Path> {
    const PROTECTED: &[&str] = &[
        "/",
        "/bin",
        "/boot",
        "/dev",
        "/etc",
        "/home",
        "/lib",
        "/lib64",
        "/mnt",
        "/opt",
        "/proc",
        "/root",
        "/run",
        "/sbin",
        "/srv",
        "/sys",
        "/tmp",
        "/usr",
        "/var",
        "/var/lib",
        "/var/log",
        "/var/tmp",
        SYSTEM_CONFIG_DIR,
        SMOLVM_PREFIX_PARENT,
    ];
    PROTECTED
        .iter()
        .map(Path::new)
        .find(|protected| *protected == home || protected.starts_with(home))
}

#[cfg(any(target_os = "linux", test))]
fn render_systemd_socket(listen: Option<SocketAddr>) -> String {
    let addr = listen
        .map(|addr| addr.to_string())
        .unwrap_or_else(|| "127.0.0.1:9090".to_owned());
    format!(
        r#"[Unit]
Description=Preloop HTTP control-plane socket

[Socket]
ListenStream={addr}
NoDelay=true
ReusePort=false

[Install]
WantedBy=sockets.target
"#
    )
}

#[cfg(any(target_os = "linux", test))]
fn render_systemd_update_timer() -> String {
    r#"[Unit]
Description=Poll for Preloop releases

[Timer]
OnBootSec=10m
OnUnitActiveSec=1h
RandomizedDelaySec=30m
Persistent=true

[Install]
WantedBy=timers.target
"#
    .to_owned()
}

// ---------------------------------------------------------------------------
// launchd (macOS)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn install_launchd(
    args: &InstallArgs,
    home: &Path,
    exe: &Path,
    env_lines: &[String],
) -> Result<()> {
    let dry = args.dry_run;
    let (plist_path, domain) = launchd_target(args.user);
    let plist = render_launchd_plist(exe, home, env_lines)?;

    if dry {
        eprintln!(
            "[preloop] would write {} (0600; contains secrets if configured)",
            plist_path.display()
        );
    } else {
        write_private_file(&plist_path, plist.as_bytes())?;
    }

    if !dry {
        let boot = Command::new("launchctl")
            .args(["bootstrap", &domain])
            .arg(&plist_path)
            .status();
        match boot {
            Ok(status) if status.success() => {}
            _ => {
                let load = Command::new("launchctl")
                    .args(["load", "-w"])
                    .arg(&plist_path)
                    .status();
                match load {
                    Ok(status) if status.success() => {}
                    other => bail!(
                        "launchctl could not start the daemon ({other:?}); \
                         check `launchctl print system/{LAUNCHD_LABEL}`"
                    ),
                }
            }
        }
    } else {
        eprintln!(
            "[preloop] would run: launchctl bootstrap {} {}",
            domain,
            plist_path.display()
        );
    }

    let secrets = "secrets:   --webhook-secret/--github-app-* flags are embedded in the plist\n\
                   \x20          (0600); `setup github --save` writes config.toml (0600). Define\n\
                   \x20          a webhook secret or GitHub webhook delivery is rejected."
        .to_owned();
    eprintln!(
        "[preloop] installed Preloop control plane as a {}:\n\
         \x20 plist:   {} (0600)\n\
         \x20 state:   {} (0700)\n\
         \x20 status:  {} launchctl print {}/{LAUNCHD_LABEL}\n\
         \x20 logs:    {}/server.log\n\
         \x20 updates: no macOS timer — run `preloop update` from cron or a LaunchAgent\n\
         \x20 GitHub:  re-run with --github-app-* flags, or run\n\
         \x20          PRELOOP_HOME={} preloop setup github --save\n\
         \x20 {}{}{}",
        if args.user {
            "LaunchAgent (user session)"
        } else {
            "LaunchDaemon (system)"
        },
        plist_path.display(),
        home.display(),
        if args.user { "" } else { "sudo" },
        domain,
        home.display(),
        home.display(),
        secrets,
        if args.user {
            "\n\
             \x20 note:    LaunchAgents run only while you are logged in"
        } else {
            ""
        },
        webhook_hint(args.public_url.as_deref()),
    );
    Ok(())
}

/// Reminder printed when no --public-url was configured: without it GitHub
/// cannot deliver webhooks, so the service silently never triggers.
fn webhook_hint(public_url: Option<&str>) -> String {
    match public_url {
        Some(_) => String::new(),
        None => "\n\
             \x20 webhooks:  no --public-url set — GitHub can't reach the engine yet.\n\
             \x20          Re-run with --public-url (tunnel: `cloudflared tunnel --url\n\
             \x20          http://127.0.0.1:9090`; production: DNS + reverse proxy).\n\
             \x20          See docs/setup.md \"Exposing the engine to GitHub\"."
            .to_owned(),
    }
}

#[cfg(target_os = "macos")]
fn uninstall_launchd(args: &UninstallArgs, home: &Path) -> Result<()> {
    let _ = home;
    let dry = args.dry_run;
    let (plist_path, domain) = launchd_target(args.user);
    let bootout = format!("{domain}/{LAUNCHD_LABEL}");
    run_ok(
        "launchctl",
        &["bootout".to_owned(), bootout],
        dry,
        "unload the daemon",
    );
    remove_path(&plist_path, dry)?;
    eprintln!(
        "[preloop] removed Preloop {}",
        if args.user {
            "LaunchAgent"
        } else {
            "LaunchDaemon"
        }
    );
    Ok(())
}

/// Plist path and launchctl domain for the requested scope.
#[cfg(any(target_os = "macos", test))]
fn launchd_target(user: bool) -> (PathBuf, String) {
    if user {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        (
            home.join("Library/LaunchAgents/dev.preloop.server.plist"),
            format!("gui/{}", unsafe { libc::getuid() }),
        )
    } else {
        (PathBuf::from(LAUNCHD_PLIST), "system".to_owned())
    }
}

#[cfg(any(target_os = "macos", test))]
fn render_launchd_plist(exe: &Path, home: &Path, env_lines: &[String]) -> Result<String> {
    let mut env = String::new();
    env.push_str(&format!(
        "        <key>PRELOOP_HOME</key>\n        <string>{}</string>\n",
        xml_escape(&home.display().to_string())
    ));
    for line in env_lines {
        let (key, value) = line.split_once('=').expect("validated env line");
        env.push_str(&format!(
            "        <key>{}</key>\n        <string>{}</string>\n",
            xml_escape(key),
            xml_escape(value)
        ));
    }
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>serve</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
{env}    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>{home}/server.log</string>
    <key>StandardErrorPath</key>
    <string>{home}/server.log</string>
</dict>
</plist>
"#,
        label = LAUNCHD_LABEL,
        exe = xml_escape(&exe.display().to_string()),
        home = home.display(),
        env = env,
    ))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Environment-file lines for the config the operator supplied. Keys are the
/// names `preloop serve` / the server already read (`PRELOOP_LISTEN`,
/// `PRELOOP_PUBLIC_URL`, `PRELOOP_GITHUB_APP_*`, `PRELOOP_WEBHOOK_SECRET`).
/// `app_key` is the path the environment file should reference — the staged
/// service-owned copy on Linux system installs, the caller's original
/// elsewhere; `None` falls back to `args.github_app_key`.
fn config_env_lines(args: &InstallArgs, app_key: Option<&Path>) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    if let Some(listen) = args.listen {
        lines.push(env_line("PRELOOP_LISTEN", &listen.to_string())?);
    }
    if let Some(url) = &args.public_url {
        lines.push(env_line("PRELOOP_PUBLIC_URL", url)?);
    }
    if let Some(id) = &args.github_app_id {
        lines.push(env_line("PRELOOP_GITHUB_APP_ID", id)?);
    }
    if let Some(key) = app_key.or(args.github_app_key.as_deref()) {
        let key = match std::fs::canonicalize(key) {
            Ok(resolved) => resolved,
            // A --dry-run stages the key copy without creating it; the
            // absolute staged path still renders the correct environment
            // file. Real installs resolve, because `install` validated that
            // the original exists and the staged copy was just made.
            Err(_) => {
                anyhow::ensure!(
                    key.is_absolute(),
                    "--github-app-key must be an absolute path (got {})",
                    key.display()
                );
                key.to_path_buf()
            }
        };
        lines.push(env_line(
            "PRELOOP_GITHUB_APP_PEM_FILE",
            &key.display().to_string(),
        )?);
    }
    if let Some(id) = args.github_app_installation_id {
        lines.push(env_line(
            "PRELOOP_GITHUB_APP_INSTALLATION_ID",
            &id.to_string(),
        )?);
    }
    if let Some(secret) = &args.webhook_secret {
        lines.push(env_line("PRELOOP_WEBHOOK_SECRET", secret)?);
    }
    Ok(lines)
}

fn env_line(key: &str, value: &str) -> Result<String> {
    if value.contains(['\n', '\0']) {
        bail!("{key} must not contain newlines or NUL bytes");
    }
    // systemd parses EnvironmentFile values with POSIX-shell backslash-escape
    // rules, so a literal `\`, `"` or `'` in a secret must be escaped or the
    // service receives a different value (and rejects every webhook).
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\'', "\\'");
    Ok(format!("{key}={escaped}"))
}

fn resolve_home(home: Option<&Path>, user: bool) -> Result<PathBuf> {
    let default = if user {
        crate::preloop_home()
    } else {
        PathBuf::from(DEFAULT_HOME)
    };
    let home = home.map(PathBuf::from).unwrap_or(default);
    if !home.is_absolute() {
        bail!("--home must be an absolute path (got {})", home.display());
    }
    Ok(home)
}

#[cfg(unix)]
fn require_root() -> Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        bail!("server install/uninstall must run as root (sudo preloop server install)")
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_root() -> Result<()> {
    bail!("server install/uninstall is supported on Linux and macOS only")
}

fn prepare_home(home: &Path, dry_run: bool) -> Result<()> {
    if dry_run {
        eprintln!(
            "[preloop] would create state directory {} (0700)",
            home.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(home).with_context(|| format!("create {}", home.display()))?;
    set_private_directory_permissions(home)?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn write_env_file(path: &Path, env_lines: &[String], dry_run: bool) -> Result<()> {
    if env_lines.is_empty() {
        return Ok(());
    }
    if dry_run {
        eprintln!("[preloop] would write {} (0600)", path.display());
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    // write_private_file uses O_CREAT|O_TRUNC without unlinking, so a rewrite
    // preserves the existing inode — and with it the existing owner. For a
    // system install the file sits in the root-owned config dir and is never
    // reached by the state-dir chown, so it stays root-owned across
    // re-installs. (Before this was true, a second `server install` handed the
    // service write access to its own EnvironmentFile, which overrides the
    // unit's Environment= and could disable the VM sandbox on restart.)
    write_private_file(path, env_lines.join("\n").as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

/// Move a pre-existing `<home>/environment` into the root-owned config dir.
///
/// Releases before the hardened layout read the environment file from the
/// state dir. Upgrading must not strand the operator's `--github-app-*` and
/// `--webhook-secret` values: a re-install without repeating those flags
/// writes no new env file, and the new unit no longer reads the old path, so
/// the credentials would silently vanish on the first restart. Migrate once,
/// then stop looking (the old path is in the service-writable state dir and
/// must not become authoritative again).
#[cfg(target_os = "linux")]
fn migrate_legacy_env_file(home: &Path, dry_run: bool) -> Result<()> {
    let legacy = home.join("environment");
    let destination = env_file_path(home, false);
    if !legacy.is_file() || destination.exists() {
        return Ok(());
    }
    if dry_run {
        eprintln!(
            "[preloop] would migrate legacy {} -> {} (0600 root-owned)",
            legacy.display(),
            destination.display()
        );
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::rename(&legacy, &destination)
        .with_context(|| format!("migrate {} to {}", legacy.display(), destination.display()))?;
    set_private_file_permissions(&destination)
        .with_context(|| format!("chmod 0600 {}", destination.display()))?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn write_unit(path: &Path, contents: &str, dry_run: bool) -> Result<()> {
    if dry_run {
        eprintln!("[preloop] would write {}", path.display());
        return Ok(());
    }
    std::fs::write(path, contents).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn remove_path(path: &Path, dry_run: bool) -> Result<()> {
    if dry_run {
        eprintln!("[preloop] would remove {}", path.display());
        return Ok(());
    }
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}

#[cfg(target_os = "linux")]
fn run_systemctl(args: &[String], dry_run: bool) -> Result<()> {
    if dry_run {
        eprintln!("[preloop] would run: systemctl {}", args.join(" "));
        return Ok(());
    }
    let status = Command::new("systemctl")
        .args(args)
        .status()
        .with_context(|| format!("run systemctl {}", args.join(" ")))?;
    if !status.success() {
        bail!("systemctl {} failed ({status})", args.join(" "));
    }
    Ok(())
}

fn run_ok(program: &str, args: &[String], dry_run: bool, what: &str) {
    if dry_run {
        eprintln!("[preloop] would run: {program} {}", args.join(" "));
        return;
    }
    match Command::new(program).args(args).status() {
        Ok(status) if status.success() => {}
        other => eprintln!("[preloop] warning: {what} ({other:?}); continuing"),
    }
}

/// Render a path for systemd directives (`ExecStart=`, `ReadWritePaths=`):
/// quoted when it contains whitespace, since both are parsed as
/// whitespace-separated lists.
#[cfg(any(target_os = "linux", test))]
fn systemd_path(path: &Path) -> String {
    let display = path.display().to_string();
    if display.contains(char::is_whitespace) {
        format!("\"{display}\"")
    } else {
        display
    }
}

/// Directory holding the executable, for `ReadWritePaths` so the self-update
/// timer can replace the binary under `ProtectSystem=full`. Omitted when it
/// would be meaningless (`/`).
#[cfg(any(target_os = "linux", test))]
fn readwrite_dir(exe: &Path) -> Option<PathBuf> {
    let dir = exe.parent()?;
    (!dir.as_os_str().is_empty() && dir != Path::new("/")).then(|| dir.to_owned())
}

#[cfg(any(target_os = "macos", test))]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn env_lines() -> Vec<String> {
        vec![
            "PRELOOP_GITHUB_APP_ID=12345".to_owned(),
            "PRELOOP_WEBHOOK_SECRET=hunter2&<secret>".to_owned(),
        ]
    }

    #[test]
    fn systemd_service_has_exec_and_hardening() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert!(unit.contains("ExecStart=/usr/local/bin/preloop serve"));
        assert!(unit.contains("Environment=PRELOOP_HOME=/var/lib/preloop"));
        // Root-owned config dir, not the service-writable state dir.
        assert!(unit.contains("EnvironmentFile=-/etc/preloop/environment"));
        assert!(unit.contains("Requires=preloop.socket"));
        assert!(unit.contains("NoNewPrivileges=true"));
        assert!(unit.contains("ProtectSystem=full"));
        assert!(unit.contains("ProtectHome=read-only"));
        assert!(unit.contains("WantedBy=multi-user.target"));
    }

    /// The full default system-scope unit, byte for byte — the renderer is
    /// the observable output, and contrib/systemd/preloop.service mirrors it.
    #[test]
    fn systemd_service_default_system_scope_renders_exactly() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert_eq!(
            unit,
            r#"[Unit]
Description=Preloop self-hosted GitHub Actions control plane
Requires=preloop.socket
After=preloop.socket network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/preloop serve
Environment=PRELOOP_HOME=/var/lib/preloop
EnvironmentFile=-/etc/preloop/environment
User=preloop
Group=preloop
Environment=HOME=/var/lib/preloop
Environment=SMOLVM_DATA_DIR=/var/lib/preloop/smolvm
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=read-only
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectClock=true
LockPersonality=true
RestrictRealtime=true
CapabilityBoundingSet=
Delegate=cpu memory pids
StateDirectory=preloop
StateDirectoryMode=0700
[Install]
WantedBy=multi-user.target
"#
        );
    }

    /// The system-scope service must run under a dedicated non-root account:
    /// a guest→VMM escape lands in the VMM process, which inherits the
    /// service identity — root would hand the escape the host. The identity
    /// is not applied to user-scope units (those already run as the
    /// installing user, and systemd rejects User= there).
    #[test]
    fn systemd_service_runs_as_dedicated_non_root_identity() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert!(unit.contains("User=preloop\nGroup=preloop\n"));
        assert!(unit.contains("CapabilityBoundingSet=\n"));
        assert!(unit.contains("StateDirectory=preloop"));
        assert!(unit.contains("StateDirectoryMode=0700"));
        assert!(unit.contains("Delegate=cpu memory pids"));
        assert!(unit.contains("ProtectKernelModules=true"));
        assert!(unit.contains("ProtectKernelLogs=true"));
        assert!(unit.contains("ProtectClock=true"));
        assert!(unit.contains("LockPersonality=true"));
        assert!(unit.contains("RestrictRealtime=true"));
        // The serving unit must not be able to rewrite its own binary, and
        // the default state dir is owned via StateDirectory, so no
        // ReadWritePaths at all.
        assert!(!unit.contains("ReadWritePaths="));
        // SmolVM data is pinned under the state dir, not the operator's or
        // root's XDG tree, and HOME follows so the service identity never
        // resolves a foreign home.
        assert!(unit.contains("Environment=HOME=/var/lib/preloop"));
        assert!(unit.contains("Environment=SMOLVM_DATA_DIR=/var/lib/preloop/smolvm"));

        let user_unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new("/Users/me/.preloop"),
            true,
            None,
        )
        .unwrap();
        assert!(!user_unit.contains("User=preloop"));
        assert!(!user_unit.contains("StateDirectory="));
        assert!(!user_unit.contains("Delegate="));
        assert!(!user_unit.contains("SMOLVM_DATA_DIR="));
    }

    /// Only the update unit may write the executable's directory: a VMM
    /// escape (or any service-user code) planting a new binary for the next
    /// root start to execute would be an escalation path.
    #[test]
    fn serving_unit_cannot_rewrite_its_own_binary() {
        let service = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert!(!service.contains("ReadWritePaths=/usr/local/bin"));
        let update = render_systemd_update_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
        )
        .unwrap();
        assert!(update.contains("ReadWritePaths=/usr/local/bin"));
    }

    /// A system install under a `ProtectHome=read-only` root is rejected, not
    /// papered over with a `ReadWritePaths` carve-out: the `preloop` account
    /// cannot traverse a 0700 `/home/<user>` or `/root` however the state dir
    /// itself is owned, so the carve-out produced a unit that looked correct
    /// and failed at first start. A root-reachable custom home is fine and
    /// needs no carve-out at all.
    #[test]
    fn system_install_rejects_a_home_the_service_account_cannot_traverse() {
        for blocked in [
            "/home/me/preloop-data",
            "/root/preloop",
            "/run/user/1000/preloop",
        ] {
            assert!(
                home_blocked_by_protect_home(Path::new(blocked)),
                "{blocked} must be rejected for a system install"
            );
            let error = install(InstallArgs {
                home: Some(PathBuf::from(blocked)),
                user: false,
                dry_run: true,
                listen: None,
                public_url: None,
                github_app_id: None,
                github_app_key: None,
                github_app_installation_id: None,
                webhook_secret: None,
                no_update_timer: false,
                systemd_credential: None,
            })
            .unwrap_err();
            assert!(
                error.to_string().contains("unusable for a system install"),
                "{blocked}: {error}"
            );
        }
        // A home the service account CAN traverse is still rejected when it
        // is a shared system path: the recursive chown would hand unrelated
        // host data to the service account.
        for blocked in ["/", "/var/lib", "/var", "/srv", "/opt", "/etc/preloop"] {
            let error = install(InstallArgs {
                home: Some(PathBuf::from(blocked)),
                user: false,
                dry_run: true,
                listen: None,
                public_url: None,
                github_app_id: None,
                github_app_key: None,
                github_app_installation_id: None,
                webhook_secret: None,
                no_update_timer: false,
                systemd_credential: None,
            })
            .unwrap_err();
            assert!(
                error.to_string().contains("too broad for a system install"),
                "{blocked}: {error}"
            );
        }
        // Root-reachable custom home: allowed, no carve-out, no StateDirectory.
        assert!(!home_blocked_by_protect_home(Path::new("/srv/preloop")));
        assert!(shared_system_path(Path::new(DEFAULT_HOME)).is_none());
        assert!(shared_system_path(Path::new("/srv/preloop")).is_none());
        let open = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new("/srv/preloop"),
            false,
            None,
        )
        .unwrap();
        assert!(!open.contains("ReadWritePaths="));
        assert!(!open.contains("StateDirectory="));
        // User scope still carves out its home, which ProtectHome would block.
        let user_unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new("/home/me/.preloop"),
            true,
            None,
        )
        .unwrap();
        assert!(user_unit.contains("ReadWritePaths=/home/me/.preloop"));
    }

    /// The whole point of the ownership split: nothing the service must not be
    /// able to rewrite may live inside `PRELOOP_HOME`. The service owns that
    /// directory, and on Unix the directory write bit governs unlink/rename,
    /// so any entry in it can be replaced regardless of the entry's own mode.
    #[test]
    fn install_artifacts_live_outside_the_service_writable_state_dir() {
        let home = Path::new(DEFAULT_HOME);
        assert!(
            !Path::new(SYSTEM_CONFIG_DIR).starts_with(home),
            "the EnvironmentFile/App-key dir must not be under {}",
            home.display()
        );
        assert!(
            !Path::new(SMOLVM_PREFIX_PARENT).starts_with(home),
            "the smolvm prefix (which /usr/local/bin/smolvm points into, and \
             root executes) must not be under {}",
            home.display()
        );
        // System scope: env file in the root-owned config dir. User scope: no
        // privilege boundary, so it stays with the rest of the state.
        assert_eq!(
            env_file_path(home, false),
            PathBuf::from("/etc/preloop/environment")
        );
        let user_home = Path::new("/home/me/.preloop");
        assert_eq!(
            env_file_path(user_home, true),
            user_home.join("environment")
        );
    }

    #[test]
    fn systemd_service_mounts_credential_when_configured() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            Some(Path::new("/etc/preloop-secrets.enc")),
        )
        .unwrap();
        assert!(unit.contains("LoadCredentialEncrypted=preloop-secrets:/etc/preloop-secrets.enc"));
        let plain = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert!(!plain.contains("LoadCredential="));
        assert!(!plain.contains("LoadCredentialEncrypted="));
        // Exactly one Restart line: the credential slot must not emit an
        // empty duplicate line.
        assert_eq!(unit.matches("Restart=on-failure").count(), 1);
        assert_eq!(plain.matches("Restart=on-failure").count(), 1);
    }

    #[test]
    fn systemd_service_quotes_paths_with_spaces() {
        let unit = render_systemd_service(
            Path::new("/opt/pre loop/preloop"),
            Path::new("/var/lib/preloop"),
            false,
            None,
        )
        .unwrap();
        assert!(unit.contains("ExecStart=\"/opt/pre loop/preloop\" serve"));
        // The serving unit never gets the executable's directory, so no
        // quoting is needed here; the update unit does carry it.
        assert!(!unit.contains("ReadWritePaths="));
        let update = render_systemd_update_service(
            Path::new("/opt/pre loop/preloop"),
            Path::new("/var/lib/preloop"),
            false,
        )
        .unwrap();
        assert!(update.contains("ReadWritePaths=\"/opt/pre loop\""));
    }

    #[test]
    fn user_service_grants_write_to_preloop_home() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new("/Users/me/.preloop"),
            true,
            None,
        )
        .unwrap();
        // ProtectHome=read-only would block ~/.preloop state; the unit must
        // carve out a writable exception.
        assert!(unit.contains("ProtectHome=read-only"));
        assert!(unit.contains("ReadWritePaths=/Users/me/.preloop"));
        let system = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        // System scope relies on StateDirectory for the default home and
        // grants no extra paths at all.
        assert!(!system.contains("ReadWritePaths="));
    }

    #[test]
    fn readwrite_paths_quote_whitespace_dirs() {
        // Update unit: executable dir + (user-scope) home, quoted.
        let line = readwrite_paths(
            Path::new("/opt/pre loop/preloop"),
            Path::new("/h/p"),
            true,
            true,
        );
        assert_eq!(line, "ReadWritePaths=\"/opt/pre loop\" /h/p\n");
        // Serving unit: only the state dir, never the executable's.
        let serving = readwrite_paths(
            Path::new("/opt/pre loop/preloop"),
            Path::new("/h/p"),
            true,
            false,
        );
        assert_eq!(serving, "ReadWritePaths=/h/p\n");
        // System scope with the default home: nothing at all.
        let system = readwrite_paths(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            false,
        );
        assert_eq!(system, "");
    }

    #[test]
    fn env_line_escapes_shell_specials() {
        assert_eq!(env_line("A", "a\\b\"c'd").unwrap(), "A=a\\\\b\\\"c\\'d");
        assert_eq!(env_line("A", "plain").unwrap(), "A=plain");
        assert!(env_line("A", "x\ny").is_err());
        assert!(env_line("A", "x\0y").is_err());
    }

    #[test]
    fn systemd_service_never_contains_secrets() {
        let unit = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        let update = render_systemd_update_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
        )
        .unwrap();
        for secret in ["hunter2", "12345"] {
            assert!(!unit.contains(secret), "secret leaked into service unit");
            assert!(!update.contains(secret), "secret leaked into update unit");
        }
    }

    #[test]
    fn user_service_targets_default_and_system_targets_multi_user() {
        let user = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new("/Users/me/.preloop"),
            true,
            None,
        )
        .unwrap();
        let system = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        assert!(user.contains("WantedBy=default.target"));
        assert!(system.contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn user_home_defaults_to_preloop_home() {
        let unique = std::env::temp_dir().join("preloop-home-test");
        std::env::set_var("PRELOOP_HOME", &unique);
        let home = resolve_home(None, true).unwrap();
        std::env::remove_var("PRELOOP_HOME");
        assert_eq!(home, unique);
    }

    #[test]
    fn systemctl_args_prepend_user_flag() {
        assert_eq!(
            systemctl_args(false, &["enable", "--now"]),
            vec!["enable".to_owned(), "--now".to_owned()]
        );
        assert_eq!(
            systemctl_args(true, &["daemon-reload"]),
            vec!["--user".to_owned(), "daemon-reload".to_owned()]
        );
    }

    #[test]
    fn user_launchd_target_is_gui_domain_in_home() {
        let unique = std::env::temp_dir().join("preloop-home-test");
        std::env::set_var("HOME", &unique);
        let (path, domain) = launchd_target(true);
        std::env::remove_var("HOME");
        assert!(path.ends_with("Library/LaunchAgents/dev.preloop.server.plist"));
        assert!(domain.starts_with("gui/"));
        let (system_path, system_domain) = launchd_target(false);
        assert_eq!(system_path, PathBuf::from(LAUNCHD_PLIST));
        assert_eq!(system_domain, "system");
    }

    #[test]
    fn systemd_socket_defaults_to_loopback_and_honors_listen() {
        assert!(render_systemd_socket(None).contains("ListenStream=127.0.0.1:9090"));
        let custom: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let unit = render_systemd_socket(Some(custom));
        assert!(unit.contains("ListenStream=127.0.0.1:8080"));
        assert!(unit.contains("WantedBy=sockets.target"));
    }

    #[test]
    fn systemd_update_timer_matches_contrib() {
        let timer = render_systemd_update_timer();
        assert!(timer.contains("OnBootSec=10m"));
        assert!(timer.contains("OnUnitActiveSec=1h"));
        assert!(timer.contains("Persistent=true"));
        assert!(timer.contains("WantedBy=timers.target"));
    }

    #[test]
    fn launchd_plist_has_label_args_and_escaped_env() {
        let plist = render_launchd_plist(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            &env_lines(),
        )
        .unwrap();
        assert!(plist.contains("<string>dev.preloop.server</string>"));
        assert!(plist.contains("<string>/usr/local/bin/preloop</string>"));
        assert!(plist.contains("<string>serve</string>"));
        assert!(plist.contains("<key>PRELOOP_HOME</key>"));
        assert!(plist.contains("<string>/var/lib/preloop</string>"));
        assert!(plist.contains("<key>PRELOOP_WEBHOOK_SECRET</key>"));
        // XML-escaped: & → &amp;, < → &lt;
        assert!(plist.contains("hunter2&amp;&lt;secret&gt;"));
        assert!(!plist.contains("hunter2&<secret>"));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn env_lines_only_include_supplied_config() {
        let args = InstallArgs {
            listen: Some("127.0.0.1:9090".parse().unwrap()),
            public_url: Some("https://ci.example.com".to_owned()),
            github_app_id: None,
            github_app_key: None,
            github_app_installation_id: Some(7),
            webhook_secret: None,
            home: None,
            no_update_timer: false,
            systemd_credential: None,
            user: false,
            dry_run: false,
        };
        let lines = config_env_lines(&args, None).unwrap();
        assert_eq!(
            lines,
            vec![
                "PRELOOP_LISTEN=127.0.0.1:9090".to_owned(),
                "PRELOOP_PUBLIC_URL=https://ci.example.com".to_owned(),
                "PRELOOP_GITHUB_APP_INSTALLATION_ID=7".to_owned(),
            ]
        );
    }

    #[test]
    fn env_line_rejects_newlines_and_nuls() {
        assert!(env_line("A", "x\ny").is_err());
        assert!(env_line("A", "x\0y").is_err());
        assert_eq!(env_line("A", "x=y=z").unwrap(), "A=x=y=z");
    }

    #[test]
    fn resolve_home_requires_absolute() {
        assert!(resolve_home(None, false).unwrap().is_absolute());
        assert!(resolve_home(Some(Path::new("relative")), false).is_err());
    }

    #[test]
    fn xml_escape_handles_plist_specials() {
        assert_eq!(
            xml_escape("a&b<c>d\"e'f"),
            "a&amp;b&lt;c&gt;d&quot;e&apos;f"
        );
    }

    #[test]
    fn readwrite_dir_omitted_for_root_exe() {
        assert!(readwrite_dir(Path::new("/preloop")).is_none());
        assert_eq!(
            readwrite_dir(Path::new("/usr/local/bin/preloop")).unwrap(),
            PathBuf::from("/usr/local/bin")
        );
    }

    /// The GitHub App key must be copied into service-owned state, never
    /// chowned in place: the service user cannot traverse a `/root`-like
    /// parent no matter how the file itself is owned. The caller's original
    /// stays byte-for-byte and mode-for-mode untouched.
    #[test]
    fn staged_app_key_copies_into_state_without_touching_the_source() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("app.pem");
        std::fs::write(&source, b"-----BEGIN PRIVATE KEY-----\nsecret\n").unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
        let home = tempfile::tempdir().unwrap();
        let args = InstallArgs {
            listen: None,
            public_url: None,
            github_app_id: None,
            github_app_key: Some(source.clone()),
            github_app_installation_id: None,
            webhook_secret: None,
            home: None,
            no_update_timer: false,
            systemd_credential: None,
            user: false,
            dry_run: false,
        };

        let staged = staged_app_key(&args, home.path()).unwrap().unwrap();

        let expected = home.path().join("github-app-key.pem");
        assert_eq!(staged, expected);
        assert_eq!(
            std::fs::read(&expected).unwrap(),
            b"-----BEGIN PRIVATE KEY-----\nsecret\n"
        );
        assert_eq!(
            std::fs::metadata(&expected).unwrap().permissions().mode() & 0o777,
            0o600,
            "staged copy starts private; grant_service_read widens it to 0640 \
             root:preloop once the service account exists"
        );
        // The caller's original is untouched.
        assert_eq!(
            std::fs::read(&source).unwrap(),
            b"-----BEGIN PRIVATE KEY-----\nsecret\n"
        );
        assert_eq!(
            std::fs::metadata(&source).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // The environment file references the staged path, and dry-run
        // renders the same line without creating the copy.
        let env_args = InstallArgs {
            github_app_key: Some(staged.clone()),
            dry_run: false,
            ..args.clone()
        };
        let lines = config_env_lines(&env_args, None).unwrap();
        let canonical_staged = std::fs::canonicalize(&staged).unwrap();
        assert!(
            lines.iter().any(|line| line
                == &format!("PRELOOP_GITHUB_APP_PEM_FILE={}", canonical_staged.display())),
            "{lines:?}"
        );
        let dry_args = InstallArgs {
            dry_run: true,
            ..args
        };
        // A fresh home: dry-run must not create the staged copy anywhere.
        let dry_home = tempfile::tempdir().unwrap();
        let dry_staged = staged_app_key(&dry_args, dry_home.path()).unwrap().unwrap();
        let dry_expected = dry_home.path().join("github-app-key.pem");
        assert_eq!(dry_staged, dry_expected);
        assert!(
            !dry_expected.exists(),
            "dry-run must not create the staged copy"
        );
        // Dry-run renders the staged path without a canonicalize round-trip.
        let dry_lines = config_env_lines(&dry_args, Some(&dry_staged)).unwrap();
        assert!(
            dry_lines
                .iter()
                .any(|line| line
                    == &format!("PRELOOP_GITHUB_APP_PEM_FILE={}", dry_expected.display()))
        );
    }

    /// User-scope installs keep the caller's key path — the service runs as
    /// the installing user, who can already reach it.
    #[test]
    fn staged_app_key_keeps_the_original_path_for_user_scope() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("app.pem");
        std::fs::write(&source, b"key").unwrap();
        let home = tempfile::tempdir().unwrap();
        let args = InstallArgs {
            github_app_key: Some(source.clone()),
            user: true,
            dry_run: false,
            listen: None,
            public_url: None,
            github_app_id: None,
            github_app_installation_id: None,
            webhook_secret: None,
            home: None,
            no_update_timer: false,
            systemd_credential: None,
        };
        assert_eq!(
            staged_app_key(&args, home.path()).unwrap(),
            Some(source.clone())
        );
        assert!(!home.path().join("github-app-key.pem").exists());
    }

    /// The very first system install with `--github-app-key` targets a state
    /// dir that does not exist yet (default or nested). `install()` must
    /// prepare the directory before staging the key — this test pins that
    /// ordering by reproducing the exact sequence against a nonexistent
    /// nested path.
    #[test]
    fn first_install_stages_key_into_a_prepared_nested_state_dir() {
        let base = tempfile::tempdir().unwrap();
        let home = base.path().join("nested/preloop");
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("app.pem");
        std::fs::write(&source, b"-----BEGIN PRIVATE KEY-----\nsecret\n").unwrap();
        let args = InstallArgs {
            github_app_key: Some(source.clone()),
            dry_run: false,
            user: false,
            listen: None,
            public_url: None,
            github_app_id: None,
            github_app_installation_id: None,
            webhook_secret: None,
            home: None,
            no_update_timer: false,
            systemd_credential: None,
        };

        // install()'s order: state dir first, then the staged key copy.
        prepare_home(&home, false).unwrap();
        let staged = staged_app_key(&args, &home).unwrap().unwrap();

        assert_eq!(staged, home.join("github-app-key.pem"));
        assert_eq!(
            std::fs::read(&staged).unwrap(),
            b"-----BEGIN PRIVATE KEY-----\nsecret\n"
        );
        assert_eq!(
            std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777,
            0o600,
            "staged copy starts private; grant_service_read widens it to 0640 \
             root:preloop once the service account exists"
        );
        assert_eq!(
            std::fs::metadata(&home).unwrap().permissions().mode() & 0o777,
            0o700,
            "state dir must stay 0700"
        );
        // The source is never modified.
        assert_eq!(
            std::fs::read(&source).unwrap(),
            b"-----BEGIN PRIVATE KEY-----\nsecret\n"
        );
        // Without the prepared state dir the staging copy fails — the
        // ordering is the contract.
        let unprepared = base.path().join("other-nested/preloop");
        assert!(
            staged_app_key(&args, &unprepared).is_err(),
            "staging into a nonexistent state dir must fail loudly"
        );
        assert!(!unprepared.exists());
    }

    /// The full `install()` pre-flight, dry-run, against a nonexistent
    /// nested state dir with `--github-app-key`: it must succeed and leave
    /// nothing behind.
    #[test]
    fn dry_run_install_handles_nonexistent_nested_home_with_key() {
        let base = tempfile::tempdir().unwrap();
        let home = base.path().join("nested/preloop");
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("app.pem");
        std::fs::write(&source, b"key").unwrap();
        let args = InstallArgs {
            home: Some(home.clone()),
            github_app_key: Some(source),
            dry_run: true,
            user: false,
            listen: Some("127.0.0.1:9090".parse().unwrap()),
            public_url: None,
            github_app_id: None,
            github_app_installation_id: None,
            webhook_secret: None,
            no_update_timer: false,
            systemd_credential: None,
        };

        install(args).unwrap();

        assert!(!home.exists(), "dry-run must not create the state dir");
    }

    /// First install: no service copy yet, so the copy is stale and must be
    /// made; the managed symlink is not an independent binary.
    #[test]
    fn smolvm_bootstrap_first_install_needs_a_copy() {
        let home = tempfile::tempdir().unwrap();
        let destination_bin = home.path().join("smolvm-prefix/smolvm");
        let bin_dir = tempfile::tempdir().unwrap();
        let managed_link = bin_dir.path().join("smolvm");
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("smolvm-bin");
        std::fs::write(&source, b"binary").unwrap();
        assert!(smolvm_copy_stale(&source, &destination_bin));
        assert!(!independent_system_smolvm(&managed_link, &destination_bin));
    }

    /// Pin a file's mtime relative to now. `smolvm_copy_stale` compares
    /// modification times, and two consecutive `fs::write`s can land in the
    /// same filesystem timestamp tick (they do on the ext4/overlayfs used by
    /// Linux CI, though not on APFS), which made ordering-by-write-order
    /// flaky. Set the times explicitly instead.
    #[cfg(any(target_os = "linux", test))]
    fn set_mtime_secs_ago(path: &Path, secs_ago: u64) {
        let when = std::time::SystemTime::now() - std::time::Duration::from_secs(secs_ago);
        std::fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(when)
            .unwrap();
    }

    /// Refresh: after `preloop update` the source is newer and the copy must
    /// be re-made; an up-to-date copy stays put.
    #[test]
    fn smolvm_bootstrap_refreshes_a_stale_copy_and_keeps_a_current_one() {
        let source_dir = tempfile::tempdir().unwrap();
        let source_bin = source_dir.path().join("smolvm-bin");
        let destination_dir = tempfile::tempdir().unwrap();
        let destination_bin = destination_dir.path().join("smolvm-bin");
        std::fs::write(&destination_bin, b"old").unwrap();
        std::fs::write(&source_bin, b"new").unwrap();
        set_mtime_secs_ago(&destination_bin, 120);
        set_mtime_secs_ago(&source_bin, 60);
        assert!(smolvm_copy_stale(&source_bin, &destination_bin));

        set_mtime_secs_ago(&source_bin, 120);
        set_mtime_secs_ago(&destination_bin, 60);
        assert!(!smolvm_copy_stale(&source_bin, &destination_bin));

        // Identical mtimes (a same-tick copy) must NOT count as stale, or
        // every re-install would rebuild the prefix.
        let when = std::time::SystemTime::now();
        for path in [&source_bin, &destination_bin] {
            std::fs::File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_modified(when)
                .unwrap();
        }
        assert!(!smolvm_copy_stale(&source_bin, &destination_bin));
    }

    /// The freshness check compares source `smolvm-bin` against destination
    /// `smolvm-bin` — the wrapper file `smolvm` sits beside the real binary
    /// and must not be used as a directory to look under.
    #[test]
    fn smolvm_bootstrap_compares_the_real_copied_binary_path() {
        let home = tempfile::tempdir().unwrap();
        let destination = home.path().join("smolvm-prefix");
        let source_dir = tempfile::tempdir().unwrap();
        let source_prefix = source_dir.path();
        std::fs::create_dir_all(&destination).unwrap();

        // Up to date: the destination binary is newer than the source.
        std::fs::write(source_prefix.join("smolvm-bin"), b"old").unwrap();
        std::fs::write(destination.join("smolvm-bin"), b"new").unwrap();
        set_mtime_secs_ago(&source_prefix.join("smolvm-bin"), 120);
        set_mtime_secs_ago(&destination.join("smolvm-bin"), 60);
        assert!(!smolvm_copy_stale(
            &source_prefix.join("smolvm-bin"),
            &destination.join("smolvm-bin")
        ));

        // Stale: the source binary is newer — refresh needed.
        std::fs::write(source_prefix.join("smolvm-bin"), b"newest").unwrap();
        set_mtime_secs_ago(&source_prefix.join("smolvm-bin"), 10);
        assert!(smolvm_copy_stale(
            &source_prefix.join("smolvm-bin"),
            &destination.join("smolvm-bin")
        ));
    }

    #[test]
    fn atomic_swap_first_install_moves_the_staging_dir_into_place() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("smolvm-prefix");
        let staging = directory.path().join("smolvm-prefix.staging-1");
        let backup = directory.path().join("smolvm-prefix.backup-1");
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("smolvm-bin"), b"binary").unwrap();

        atomic_directory_swap(&destination, &staging, &backup).unwrap();

        assert_eq!(
            std::fs::read(destination.join("smolvm-bin")).unwrap(),
            b"binary"
        );
        assert!(!staging.exists());
        assert!(!backup.exists());
    }

    /// A refresh must atomically replace the previous prefix: no reader can
    /// observe a mix of old and new files, and no staging/backup debris
    /// remains.
    #[test]
    fn atomic_swap_refresh_replaces_the_previous_prefix_without_leftovers() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("smolvm-prefix");
        let staging = directory.path().join("smolvm-prefix.staging-2");
        let backup = directory.path().join("smolvm-prefix.backup-2");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("smolvm-bin"), b"old-binary").unwrap();
        std::fs::write(destination.join("lib"), b"old-lib").unwrap();
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("smolvm-bin"), b"new-binary").unwrap();
        std::fs::write(staging.join("lib"), b"new-lib").unwrap();

        atomic_directory_swap(&destination, &staging, &backup).unwrap();

        assert_eq!(
            std::fs::read(destination.join("smolvm-bin")).unwrap(),
            b"new-binary"
        );
        assert_eq!(std::fs::read(destination.join("lib")).unwrap(), b"new-lib");
        assert!(!staging.exists());
        assert!(!backup.exists());
    }

    /// A failed swap must restore the previous prefix and leave no staging
    /// or backup debris.
    #[test]
    fn atomic_swap_failure_restores_the_previous_prefix_and_cleans_up() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("smolvm-prefix");
        let staging = directory.path().join("smolvm-prefix.staging-3");
        let backup = directory.path().join("smolvm-prefix.backup-3");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("smolvm-bin"), b"old-binary").unwrap();
        // staging is missing: the swap rename fails.

        let error = atomic_directory_swap(&destination, &staging, &backup).unwrap_err();

        assert!(error.to_string().contains("swap"));
        assert_eq!(
            std::fs::read(destination.join("smolvm-bin")).unwrap(),
            b"old-binary",
            "the previous prefix must be restored"
        );
        assert!(!staging.exists());
        assert!(!backup.exists());
    }

    /// The installer's own symlink into the service prefix must not be
    /// mistaken for an independently current binary — that is exactly the
    /// permanently-stale-shadow failure mode. An unrelated system binary is
    /// respected and never shadowed.
    #[test]
    fn smolvm_bootstrap_managed_symlink_is_not_an_independent_binary() {
        let home = tempfile::tempdir().unwrap();
        let destination_bin = home.path().join("smolvm-prefix/smolvm");
        let bin_dir = tempfile::tempdir().unwrap();
        let managed_link = bin_dir.path().join("smolvm");

        // Managed symlink (our own): not independent.
        std::os::unix::fs::symlink(&destination_bin, &managed_link).unwrap();
        assert!(!independent_system_smolvm(&managed_link, &destination_bin));
        // A symlink to something else: independent.
        std::fs::remove_file(&managed_link).unwrap();
        let other = bin_dir.path().join("other");
        std::fs::write(&other, b"other-binary").unwrap();
        std::os::unix::fs::symlink(&other, &managed_link).unwrap();
        assert!(independent_system_smolvm(&managed_link, &destination_bin));
        // A regular file: independent.
        std::fs::remove_file(&managed_link).unwrap();
        std::fs::write(&managed_link, b"#! /bin/sh\n").unwrap();
        assert!(independent_system_smolvm(&managed_link, &destination_bin));
        // Nothing at the link: not independent (bootstrap proceeds).
        std::fs::remove_file(&managed_link).unwrap();
        assert!(!independent_system_smolvm(&managed_link, &destination_bin));
    }

    /// The rendered plist must be accepted by launchd's own parser.
    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_passes_plutil_lint() {
        let plist = render_launchd_plist(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            &env_lines(),
        )
        .unwrap();
        let path = std::env::temp_dir().join("preloop-test.plist");
        std::fs::write(&path, plist).unwrap();
        let status = std::process::Command::new("plutil")
            .args(["-lint"])
            .arg(&path)
            .status()
            .expect("plutil runs on macOS");
        let _ = std::fs::remove_file(&path);
        assert!(status.success(), "plutil -lint rejected the rendered plist");
    }
}
