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

use crate::{set_private_directory_permissions, write_private_file};

/// Default state directory for the service.
pub(crate) const DEFAULT_HOME: &str = "/var/lib/preloop";

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

#[derive(Debug, Args)]
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
    let env_lines = config_env_lines(&args)?;
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

    prepare_home(home, dry)?;
    write_env_file(home, env_lines, dry)?;

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

    let secrets = format!(
        "secrets:   --webhook-secret/--github-app-* flags land in {}/environment (0600);\n\
         \x20          `setup github --save` writes config.toml (0600). Define a webhook\n\
         \x20          secret or GitHub webhook delivery is rejected.",
        home.display()
    );
    eprintln!(
        "[preloop] installed Preloop control plane as a {} systemd service:\n\
         \x20 units:   {}/preloop.{{service,socket}}{}\n\
         \x20 state:   {} (0700), service config {}/environment (0600)\n\
         \x20 status:  systemctl {} status preloop\n\
         \x20 logs:    journalctl {} -u preloop -f\n\
         \x20 GitHub:  re-run with --github-app-* flags, or run\n\
         \x20          PRELOOP_HOME={} preloop setup github --save\n\
         \x20 {}{}{}",
        if args.user { "user-scope" } else { "system" },
        if args.no_update_timer {
            ""
        } else {
            " + preloop-update.{service,timer}"
        },
        dir.display(),
        home.display(),
        home.display(),
        if args.user { "--user" } else { "" },
        if args.user { "--user" } else { "" },
        home.display(),
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
    Ok(format!(
        r#"[Unit]
Description=Preloop self-hosted GitHub Actions control plane
Requires=preloop.socket
After=preloop.socket network-online.target

[Service]
Type=simple
ExecStart={exe_display} serve
Environment=PRELOOP_HOME={home}
EnvironmentFile=-{home}/environment
{credential}Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=read-only
{readwrite}[Install]
WantedBy={wanted_by}
"#,
        home = home.display(),
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
        readwrite = readwrite_paths(exe, home, user),
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
EnvironmentFile=-{home}/environment
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=read-only
{readwrite}"#,
        home = home.display(),
        readwrite = readwrite_paths(exe, home, user),
    ))
}

/// `ReadWritePaths=` line: the executable's directory (so the self-update
/// timer can replace the binary under `ProtectSystem=full`) and, for
/// user-scoped units, `PRELOOP_HOME` (which `ProtectHome=read-only` would
/// otherwise make unwritable — the service cannot initialize its state).
/// Paths are quoted per systemd's path-list syntax when they contain
/// whitespace; an unquoted path with a space would split the list and the
/// whitelist would silently miss the real directory.
#[cfg(any(target_os = "linux", test))]
fn readwrite_paths(exe: &Path, home: &Path, user: bool) -> String {
    let mut paths = Vec::new();
    if let Some(dir) = readwrite_dir(exe) {
        paths.push(systemd_path(&dir));
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

#[cfg(any(target_os = "macos", test))]
fn install_launchd(
    args: &InstallArgs,
    home: &Path,
    exe: &Path,
    env_lines: &[String],
) -> Result<()> {
    let dry = args.dry_run;
    let (plist_path, domain) = launchd_target(args.user);
    let plist = render_launchd_plist(exe, home, env_lines)?;

    prepare_home(home, dry)?;

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

#[cfg(any(target_os = "macos", test))]
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
fn config_env_lines(args: &InstallArgs) -> Result<Vec<String>> {
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
    if let Some(key) = &args.github_app_key {
        let key = std::fs::canonicalize(key)
            .with_context(|| format!("resolve --github-app-key {}", key.display()))?;
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
fn write_env_file(home: &Path, env_lines: &[String], dry_run: bool) -> Result<()> {
    if env_lines.is_empty() {
        return Ok(());
    }
    let path = home.join("environment");
    if dry_run {
        eprintln!("[preloop] would write {} (0600)", path.display());
        return Ok(());
    }
    write_private_file(&path, env_lines.join("\n").as_bytes())
        .with_context(|| format!("write {}", path.display()))?;
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
        assert!(unit.contains("EnvironmentFile=-/var/lib/preloop/environment"));
        assert!(unit.contains("ReadWritePaths=/usr/local/bin"));
        assert!(unit.contains("Requires=preloop.socket"));
        assert!(unit.contains("NoNewPrivileges=true"));
        assert!(unit.contains("ProtectSystem=full"));
        assert!(unit.contains("WantedBy=multi-user.target"));
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
        assert!(unit.contains("ReadWritePaths=\"/opt/pre loop\""));
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
        assert!(unit.contains("ReadWritePaths=/usr/local/bin /Users/me/.preloop"));
        let system = render_systemd_service(
            Path::new("/usr/local/bin/preloop"),
            Path::new(DEFAULT_HOME),
            false,
            None,
        )
        .unwrap();
        // System scope grants only the executable's directory, never home.
        assert!(system.contains("ReadWritePaths=/usr/local/bin\n"));
        assert!(!system.contains("/var/lib/preloop\""));
    }

    #[test]
    fn readwrite_paths_quote_whitespace_dirs() {
        let line = readwrite_paths(Path::new("/opt/pre loop/preloop"), Path::new("/h/p"), true);
        assert_eq!(line, "ReadWritePaths=\"/opt/pre loop\" /h/p\n",);
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
        let lines = config_env_lines(&args).unwrap();
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
