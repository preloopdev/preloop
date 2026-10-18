//! GitHub credential setup, verification, and the local secret store.
//!
//! `preloop setup github` configures the engine's GitHub App or fine-grained
//! PAT credential in `~/.preloop/config.toml`; `preloop doctor` verifies the
//! credential against GitHub; `preloop secret` manages the local secret
//! store the server injects into trusted jobs.

use crate::preloop_home;
use anyhow::Context;
use clap::{Parser, Subcommand};
use preloop_runner_server::config::{
    ConfigFile, config_path, load_config, store_memory, write_config,
};
use preloop_runner_server::credential_store::{
    CredentialRef, CredentialWriter, SecretString, github_reference_with_host,
};
use preloop_runner_server::secrets_api::valid_repo;
use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

/// `preloop setup` — engine configuration commands.
#[derive(Debug, Parser)]
pub struct SetupArgs {
    #[command(subcommand)]
    pub command: SetupCommand,
}

#[derive(Debug, Subcommand)]
pub enum SetupCommand {
    /// Configure GitHub credentials (App or fine-grained PAT).
    Github(GithubSetupArgs),
}

/// Configure GitHub credentials for the engine.
#[derive(Parser)]
pub struct GithubSetupArgs {
    /// Credential type: `app` (GitHub App, recommended) or `pat`
    /// (fine-grained PAT, for orgs that gate app installations).
    #[arg(long, value_enum)]
    pub via: Option<Via>,

    /// GitHub App ID (with --via app).
    #[arg(long)]
    pub app_id: Option<String>,

    /// Path to the GitHub App private key PEM (with --via app).
    #[arg(long)]
    pub pem_file: Option<PathBuf>,

    /// Create an additional App in the registry via browser (with --via app).
    #[arg(long)]
    pub add: bool,

    /// Create the App under this organization instead of your personal
    /// account (with --via app, browser flow).
    #[arg(long)]
    pub org: Option<String>,

    /// Port for the loopback listener GitHub redirects back to
    /// (with --via app, browser flow). 0 picks a free port.
    #[arg(long, default_value_t = 0)]
    pub port: u16,

    /// Public HTTPS URL of this engine. Selects the engine-hosted setup and
    /// enables webhook delivery on the created App; without it the App is
    /// created with webhooks off. Falls back to PRELOOP_PUBLIC_URL.
    #[arg(long)]
    pub public_url: Option<String>,

    /// Name of the created App (with --via app, browser flow). GitHub
    /// requires it to be globally unique.
    #[arg(long, default_value = "preloop-local")]
    pub app_name: String,

    /// Print the URL instead of opening a browser.
    #[arg(long)]
    pub no_browser: bool,

    /// Webhook secret to store. The App flow receives one from GitHub
    /// automatically; set this when you created the webhook yourself
    /// (any `--via pat` deployment that wants push triggers).
    #[arg(long)]
    pub webhook_secret: Option<String>,

    /// PAT to store (with --via pat). Falls back to PRELOOP_GITHUB_PAT,
    /// then an interactive prompt.
    #[arg(long)]
    pub token: Option<String>,

    /// Repository to verify the credential against (repeatable). On the
    /// engine-hosted flow the engine runs the checks with the stored key.
    #[arg(long = "repo")]
    pub repos: Vec<String>,

    /// Workspace whose workflows should drive the permission checklist.
    #[arg(long)]
    pub workspace: Option<PathBuf>,
}

/// Manual `Debug` — `token` is a PAT, so it is never printed. Clap's derives
/// require the trait, so redact the field rather than drop the impl.
impl std::fmt::Debug for GithubSetupArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubSetupArgs")
            .field("via", &self.via)
            .field("app_id", &self.app_id)
            .field("pem_file", &self.pem_file)
            .field("add", &self.add)
            .field("org", &self.org)
            .field("port", &self.port)
            .field("public_url", &self.public_url)
            .field("app_name", &self.app_name)
            .field("no_browser", &self.no_browser)
            .field("webhook_secret", &redacted(self.webhook_secret.is_some()))
            .field("token", &redacted(self.token.is_some()))
            .field("repos", &self.repos)
            .field("workspace", &self.workspace)
            .finish()
    }
}

/// Renders as `<redacted>` / `None` without quoting, for credential fields.
fn redacted(present: bool) -> impl std::fmt::Debug {
    struct Marker(bool);
    impl std::fmt::Debug for Marker {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(if self.0 { "<redacted>" } else { "None" })
        }
    }
    Marker(present)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Via {
    App,
    Pat,
}

/// `preloop doctor` — verify the engine's GitHub credential setup.
#[derive(Debug, Parser)]
pub struct DoctorArgs {
    /// Repository to verify the credential against (repeatable).
    #[arg(long = "repo")]
    pub repos: Vec<String>,
}

/// `preloop secret` — manage the local secret store.
#[derive(Debug, Parser)]
pub struct SecretArgs {
    #[command(subcommand)]
    pub command: SecretCommand,
}

#[derive(Subcommand)]
pub enum SecretCommand {
    /// Set a secret value.
    Set {
        /// Secret name.
        name: String,
        /// Value. Omitted → read one line from stdin (hidden when a TTY).
        #[arg(long)]
        value: Option<String>,
        /// Scope to one repository (owner/repo) instead of global.
        #[arg(long)]
        repo: Option<String>,
        /// Scope to one environment of `--repo` (requires --repo).
        /// Mirrors GitHub environment secrets.
        #[arg(long)]
        env: Option<String>,
    },
    /// List secret names (never values).
    List {
        /// Only list secrets scoped to this repository (owner/repo).
        #[arg(long)]
        repo: Option<String>,
        /// Only list secrets scoped to this environment (requires --repo).
        #[arg(long)]
        env: Option<String>,
    },
    /// Remove a secret.
    Rm {
        /// Secret name.
        name: String,
        /// Remove from this repository scope (owner/repo) instead of global.
        #[arg(long)]
        repo: Option<String>,
        /// Remove from this environment scope (requires --repo).
        #[arg(long)]
        env: Option<String>,
    },
}

/// Manual `Debug` — `Set { value }` is a secret. Clap's derives require the
/// trait, so redact the field rather than drop the impl.
impl std::fmt::Debug for SecretCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set {
                name,
                value,
                repo,
                env,
            } => f
                .debug_struct("Set")
                .field("name", name)
                .field("value", &redacted(value.is_some()))
                .field("repo", repo)
                .field("env", env)
                .finish(),
            Self::List { repo, env } => f
                .debug_struct("List")
                .field("repo", repo)
                .field("env", env)
                .finish(),
            Self::Rm { name, repo, env } => f
                .debug_struct("Rm")
                .field("name", name)
                .field("repo", repo)
                .field("env", env)
                .finish(),
        }
    }
}

pub(crate) async fn cmd_setup(args: SetupArgs) -> anyhow::Result<()> {
    let SetupCommand::Github(args) = args.command;
    cmd_setup_github(args).await
}

pub(crate) async fn cmd_setup_github(args: GithubSetupArgs) -> anyhow::Result<()> {
    let via = match args.via {
        Some(via) => via,
        None => {
            println!(
                "preloop setup github --via <app|pat>\n\
                 \n\
                 app  GitHub App is recommended. Tokens are minted per job,\n\
                 \x20     scoped to each workflow's `permissions:` block.\n\
                 pat  Fine-grained PAT — for orgs that gate app installations.\n\
                 \n\
                 `preloop setup github --via app` creates the App for you: it opens a\n\
                 local page, GitHub asks you to confirm, and the App id, private key,\n\
                 and webhook secret are written to your config automatically.\n\
                 \x20 --org <NAME>       create it under an organization\n\
                 \x20 --public-url <URL> also enable webhook delivery to this engine\n\
                 \n\
                 Already have an App? Skip the browser:\n\
                 \x20 preloop setup github --via app --app-id <ID> --pem-file <KEY>"
            );
            return Ok(());
        }
    };
    // Applied before the credential path runs, so `--via app --public-url`
    // pushes this exact secret to GitHub rather than minting a fresh one.
    // App *creation* still wins: GitHub generates the secret it will sign
    // with, and nothing local can override that.
    if let Some(secret) = args.webhook_secret.as_deref() {
        // If `--via app` is chosen with an explicit `--app-id` or `--pem-file`,
        // `setup_app` / `save_app_credentials` will bind the secret directly to
        // that target App. Otherwise, configure it here.
        if !(via == Via::App && args.app_id.is_some()) {
            let mut config = load_config()?;
            let target_app_id = args.app_id.clone().or_else(|| config.github.app_id.clone());
            if let Some(app_id) = target_app_id {
                let mut writer = CredentialWriter::for_config(
                    &config_path(),
                    config.credential_store.as_deref(),
                )?;
                let reference = github_reference_with_host(
                    "webhook",
                    config.github.server_url.as_deref(),
                    Some(app_id.as_str()),
                )?;
                persist_secret(
                    &mut writer,
                    &mut config,
                    &reference,
                    &SecretString::new(secret),
                )?;
                if let Some(existing) = config.github.apps.iter_mut().find(|a| a.app_id == app_id) {
                    existing.legacy_webhook_secret = None;
                    existing.webhook_secret_ref = Some(reference.as_str().to_owned());
                } else {
                    config.github.legacy_webhook_secret = None;
                    config.github.webhook_secret_ref = Some(reference.as_str().to_owned());
                }
                let path = write_config(&config)?;
                println!(
                    "Webhook secret stored in {} (referenced from {}).",
                    writer.destination(),
                    path.display()
                );
            } else {
                config.github.legacy_webhook_secret = Some(secret.to_owned());
                config.github.webhook_secret_ref = None;
                let path = write_config(&config)?;
                println!(
                    "Webhook secret stored in {} (moves to the credential store once an \
                     App id is configured).",
                    path.display()
                );
            }
        }
    }
    match via {
        Via::App => setup_app(&args).await,
        Via::Pat => setup_pat(&args).await,
    }
}

async fn setup_app(args: &GithubSetupArgs) -> anyhow::Result<()> {
    // Nothing supplied means "create the App for me"; either flag on its own
    // is a half-finished manual setup, and silently starting a browser flow
    // would create a second App the operator did not ask for.
    let (app_id, pem, webhook_secret) = match (args.app_id.as_deref(), args.pem_file.as_deref()) {
        (None, None) => {
            // The effective public URL, resolved once here: an App that is
            // already configured takes the webhook-update branch on the same
            // value the transport choice uses, so `PRELOOP_PUBLIC_URL` alone
            // is enough to point an existing App at a reachable address.
            let public_url = resolve_public_url(
                args.public_url.as_deref(),
                std::env::var("PRELOOP_PUBLIC_URL").ok().as_deref(),
            );
            // An App is already configured: creating a second one would leave
            // the first installed and minting tokens, and the operator almost
            // certainly meant "point the one I have at this URL".
            let existing = preloop_runner_server::config::load_config()?.github;
            let has_configured_app = (existing.app_id.is_some() && existing.app_pem().is_some())
                || !existing.apps.is_empty();
            if has_configured_app && !args.add {
                if let (Some(app_id), Some(pem)) = (existing.app_id.as_deref(), existing.app_pem())
                {
                    return match public_url.as_deref() {
                        Some(public_url) => {
                            let webhook_secret = existing.webhook_secret().map(str::to_owned);
                            enable_webhooks(app_id, pem, public_url, webhook_secret).await
                        }
                        None => {
                            let primary_id = existing.app_id.as_deref().unwrap_or("configured");
                            println!(
                                "App {primary_id} is already configured in {}.\n\
                                 \x20 --public-url <URL>  point its webhook at a reachable address\n\
                                 \x20 --add              create an additional App via browser and add it to registry\n\
                                 \x20 --app-id/--pem-file add or update an App with existing credentials\n\
                                 Delete the [github] keys from that file to start over.",
                                preloop_runner_server::config::config_path().display()
                            );
                            Ok(())
                        }
                    };
                }
                // Only registry Apps are configured — there is no legacy
                // primary App to point at a URL, and launching the browser
                // flow would create another App the operator did not ask
                // for. Say how to proceed instead.
                println!(
                    "GitHub Apps are already configured in the `github.apps` registry in {}.\n\
                     \x20 --add              create an additional App via browser and add it to registry\n\
                     \x20 --app-id/--pem-file add or update an App with existing credentials",
                    preloop_runner_server::config::config_path().display()
                );
                return Ok(());
            }
            return setup_app_via_browser(args, public_url.as_deref()).await;
        }
        (Some(app_id), Some(pem_path)) => {
            let pem = std::fs::read_to_string(pem_path)
                .with_context(|| format!("reading App private key {}", pem_path.display()))?;
            (
                app_id.to_owned(),
                pem,
                args.webhook_secret.clone().map(SecretString::new),
            )
        }
        (Some(_), None) => anyhow::bail!(
            "--pem-file is required alongside --app-id (omit both to create the App in a browser)"
        ),
        (None, Some(_)) => anyhow::bail!(
            "--app-id is required alongside --pem-file (omit both to create the App in a browser)"
        ),
    };

    let path = save_app_credentials(&app_id, &SecretString::new(pem.as_str()), webhook_secret)?;
    println!("Wrote {}", path.display());
    // Credentials alone do not deliver anything: GitHub's manifest flow sets
    // the hook up, this path cannot, so an App whose id and key were supplied
    // by hand has webhooks off until `--public-url` points them here. The
    // secret just stored (or one from an earlier run) is reused, so a working
    // secret is never rotated. A refusal is fatal and named — the secret is
    // kept locally, the hook still delivers nowhere, and `apply_app_webhook`
    // says all of that plus how to recover — because claiming success would
    // leave pushes and pull requests silently starting nothing.
    if let Some(update) = app_webhook_update(
        args.public_url.as_deref(),
        stored_webhook_secret(&load_config()?, &app_id),
    ) {
        apply_app_webhook(&app_id, &pem, &update).await?;
    }
    println!(
        "App {app_id} configured. If the App is not yet installed on your repositories:\n\
         \x20 https://github.com/apps/<slug>/installations/new\n\
         Then restart the engine (`preloop serve`) to pick up the config."
    );
    if args.repos.is_empty() {
        println!("Tip: verify with `preloop doctor --repo owner/name`.");
        return Ok(());
    }
    doctor_app(&app_id, &pem, &args.repos).await
}

/// Persist App credentials to the CLI's config file and credential store, and
/// print the notices the write produced. Supports both the primary legacy App
/// and additional Apps in `github.apps`; see
/// [`preloop_runner_server::github_app_setup::save_app_credentials_at`].
///
/// The webhook secret only exists on the manifest path — GitHub generates it
/// during creation — so it is written when present and left untouched
/// otherwise, which keeps a manual re-run from clearing a working secret.
fn save_app_credentials(
    app_id: &str,
    pem: &SecretString,
    webhook_secret: Option<SecretString>,
) -> anyhow::Result<PathBuf> {
    let path = config_path();
    let notices = preloop_runner_server::github_app_setup::save_app_credentials_at(
        &path,
        app_id,
        pem,
        webhook_secret,
    )?;
    for notice in notices {
        println!("{notice}");
    }
    Ok(path)
}

/// Write one credential through `writer`, printing the notice when the OS
/// store could not hold it and the private file store did.
fn persist_secret(
    writer: &mut CredentialWriter,
    config: &mut ConfigFile,
    reference: &CredentialRef,
    value: &SecretString,
) -> anyhow::Result<()> {
    if let Some(notice) = preloop_runner_server::github_app_setup::persist_secret(
        writer,
        config,
        reference,
        value.expose(),
    )? {
        println!("{notice}");
    }
    Ok(())
}

/// Which transport carries GitHub's redirect back to the operator.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AppSetupTransport {
    /// The running engine hosts the form and receives the redirect.
    Engine { public_url: String },
    /// A loopback listener on a fixed port, reached through an SSH port forward.
    SshForward,
    /// A loopback listener on this machine.
    Loopback,
}

/// The transport decision: a public URL wins, an SSH session comes next, and
/// everything else uses the local listener. `public_url` is the already
/// resolved value; blank counts as unset.
pub(crate) fn choose_app_setup_transport(
    public_url: Option<&str>,
    ssh_session: bool,
) -> AppSetupTransport {
    match public_url.map(str::trim).filter(|url| !url.is_empty()) {
        Some(url) => AppSetupTransport::Engine {
            public_url: url.to_owned(),
        },
        None if ssh_session => AppSetupTransport::SshForward,
        None => AppSetupTransport::Loopback,
    }
}

/// The public URL for App setup: the `--public-url` flag, else
/// `PRELOOP_PUBLIC_URL`. Blank values count as unset.
pub(crate) fn resolve_public_url(flag: Option<&str>, env: Option<&str>) -> Option<String> {
    [flag, env]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Create the App through GitHub's manifest flow and store what comes back.
///
/// The transport follows [`choose_app_setup_transport`]: the engine hosts the
/// form when a public URL is set; over SSH the listener is reached through a
/// port forward; otherwise it is local. `public_url` is the already resolved
/// value from [`resolve_public_url`].
async fn setup_app_via_browser(
    args: &GithubSetupArgs,
    public_url: Option<&str>,
) -> anyhow::Result<()> {
    let ssh = crate::app_manifest::ssh_session_from_env(|key| std::env::var(key).ok());
    match choose_app_setup_transport(public_url, ssh) {
        AppSetupTransport::Engine { public_url } => {
            crate::engine_app_setup::setup_app_via_engine(args, &public_url).await
        }
        AppSetupTransport::SshForward => {
            let target = ssh_target();
            setup_app_via_loopback(args, Some(&target)).await
        }
        AppSetupTransport::Loopback => setup_app_via_loopback(args, None).await,
    }
}

/// This session's SSH target, as the forward command needs it.
fn ssh_target() -> crate::app_manifest::SshTarget {
    let hostname = crate::app_manifest::local_hostname();
    crate::app_manifest::SshTarget::from_env(
        std::env::var("SSH_CONNECTION").ok().as_deref(),
        std::env::var("USER").ok().as_deref(),
        &hostname,
    )
}

/// The loopback flow: a single-use listener on this machine. Over SSH the
/// listener binds the forward port and prints how to reach it.
async fn setup_app_via_loopback(
    args: &GithubSetupArgs,
    ssh: Option<&crate::app_manifest::SshTarget>,
) -> anyhow::Result<()> {
    // Settle the config and the store selection before GitHub creates the App:
    // its one-time private key must have somewhere to land once it exists.
    let preflight = load_config()?;
    CredentialWriter::for_config(&config_path(), preflight.credential_store.as_deref())?;
    let (port, guidance) = match ssh {
        Some(target) => {
            let port = if args.port == 0 {
                crate::app_manifest::SSH_FORWARD_PORT
            } else {
                args.port
            };
            (
                port,
                Some(crate::app_manifest::forwarded_guidance(target, port)),
            )
        }
        None => (args.port, None),
    };
    let (registration, path) = crate::app_manifest::register_with_persist(
        &args.app_name,
        args.org.as_deref(),
        port,
        guidance.as_deref(),
        !args.no_browser,
        |credentials| {
            let app_id = credentials.id.to_string();
            save_app_credentials(
                &app_id,
                &credentials.pem,
                credentials.webhook_secret.clone(),
            )
            .with_context(|| {
                format!(
                    "GitHub created App {app_id}, but its credentials could not be saved; \
                     regenerate a private key under the App's settings to recover one"
                )
            })
        },
    )
    .await?;
    let credentials = &registration.credentials;
    let app_id = credentials.id.to_string();
    println!("Created App {app_id} and wrote {}", path.display());
    if credentials.webhook_secret.is_some() {
        println!("Webhook secret stored — signed deliveries will verify.");
    }
    match registration.installation_id {
        Some(id) => println!("Installed (installation {id})."),
        None => {
            if let Some(url) = credentials.install_url(crate::app_manifest::GITHUB_WEB) {
                println!("Install it on your repositories to finish:\n  {url}");
            }
        }
    }
    println!("Restart the engine (`preloop serve`) to pick up the config.");
    if args.repos.is_empty() {
        println!("Tip: verify with `preloop doctor --repo owner/name`.");
        return Ok(());
    }
    doctor_app(&app_id, credentials.pem.expose(), &args.repos).await
}

/// The webhook an existing App must be pointed at: the delivery URL, and the
/// secret it signs with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WebhookUpdate {
    /// The full hook URL, `/api/v1/github/webhooks` included.
    url: String,
    secret: String,
}

/// The webhook to enable for an App at `public_url`, decided without touching
/// GitHub. `None` without a URL: there is nothing to point anywhere, and the
/// App keeps whatever hook it has.
///
/// The stored secret is reused when there is one — rotating it would silently
/// break any other consumer of the same App — and a fresh one is generated
/// otherwise, since GitHub only signs deliveries when a secret is set.
fn app_webhook_update(
    public_url: Option<&str>,
    stored_secret: Option<String>,
) -> Option<WebhookUpdate> {
    let public_url = public_url?;
    let secret = match stored_secret.filter(|secret| !secret.is_empty()) {
        Some(secret) => secret,
        None => {
            let bytes: [u8; 32] = rand::random();
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }
    };
    Some(WebhookUpdate {
        url: format!(
            "{}/api/v1/github/webhooks",
            public_url.trim_end_matches('/')
        ),
        secret,
    })
}

/// The webhook secret already stored for `app_id`, whether it is the primary
/// App (`github.app_id`) or an entry in the `github.apps` registry. Reading it
/// back is what keeps a re-run from rotating a secret GitHub is already
/// signing with.
fn stored_webhook_secret(config: &ConfigFile, app_id: &str) -> Option<String> {
    if let Some(app) = config.github.apps.iter().find(|app| app.app_id == app_id) {
        return app.webhook_secret().map(str::to_owned);
    }
    if config.github.app_id.as_deref() == Some(app_id) {
        return config.github.webhook_secret().map(str::to_owned);
    }
    None
}

/// Store `secret` as the webhook secret for `app_id`, and return it as a
/// fresh load reads it back — the value the engine will verify deliveries
/// with.
///
/// This runs *before* GitHub is told which secret to sign with. The other
/// order has a failure mode with no local trace: if the credential store or
/// the config write fails (read-only or full filesystem, a refusing keychain)
/// after GitHub accepted the secret, GitHub signs every delivery with a
/// secret nobody holds, and every delivery fails verification.
fn store_webhook_secret(app_id: &str, secret: &str) -> anyhow::Result<String> {
    let mut config = load_config()?;
    let mut writer =
        CredentialWriter::for_config(&config_path(), config.credential_store.as_deref())?;
    let webhook_ref =
        github_reference_with_host("webhook", config.github.server_url.as_deref(), Some(app_id))?;
    persist_secret(&mut writer, &mut config, &webhook_ref, &SecretString::new(secret))?;
    if let Some(existing) = config.github.apps.iter_mut().find(|a| a.app_id == app_id) {
        existing.legacy_webhook_secret = None;
        existing.webhook_secret_ref = Some(webhook_ref.as_str().to_owned());
    } else {
        config.github.legacy_webhook_secret = None;
        config.github.webhook_secret_ref = Some(webhook_ref.as_str().to_owned());
    }
    write_config(&config)?;
    println!("Secret stored in {}.", writer.destination());
    stored_webhook_secret(&load_config()?, app_id)
        .filter(|stored| !stored.is_empty())
        .with_context(|| {
            format!(
                "the webhook secret for App {app_id} was written, but does not load back \
                 from the credential store"
            )
        })
}

/// Persist the secret, then let `point_hook` — the GitHub call in production —
/// aim the App's deliveries at `update.url` signing with it.
///
/// The split is the fix, not a style choice: `point_hook` runs second, and the
/// secret it receives is the one read back from the local store, so GitHub is
/// only ever told to sign with a value the engine holds. Whichever step fails,
/// the outcome is coherent — the secret is kept when GitHub refused the
/// update, and GitHub is untouched when the local writes failed — and a re-run
/// reuses the stored secret rather than rotating it.
async fn persist_secret_then_point_hook<F, Fut>(
    app_id: &str,
    update: &WebhookUpdate,
    point_hook: F,
) -> anyhow::Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let stored = store_webhook_secret(app_id, &update.secret)?;
    point_hook(stored).await.with_context(|| {
        format!(
            "App {app_id}'s webhook secret was stored locally, but GitHub refused the \
             webhook update — its webhook still delivers nowhere. Fix the credentials (the \
             App id and private key must match, and the App must be manageable with them) \
             and re-run; the stored secret is reused, never rotated. Or point it at {} by \
             hand under the App's Webhook settings.",
            update.url
        )
    })
}

/// Point an App's webhook at `update`, storing the secret it will sign with
/// first — see [`persist_secret_then_point_hook`] for why that order is the
/// whole point.
async fn apply_app_webhook(app_id: &str, pem: &str, update: &WebhookUpdate) -> anyhow::Result<()> {
    persist_secret_then_point_hook(app_id, update, |secret| async move {
        preloop_runner_server::github_app::set_app_webhook_config(app_id, pem, &update.url, &secret)
            .await
    })
    .await?;
    println!("App {app_id} webhook now delivers to {}", update.url);
    println!(
        "If deliveries do not arrive, tick **Active** under Webhook in the App's\n\
         settings — GitHub's API cannot set that flag, and an App created\n\
         without --public-url starts with it off."
    );
    println!("Restart the engine (`preloop serve`) to pick up the secret.");
    Ok(())
}

/// Point an already-configured App's webhook at a reachable URL. For callers
/// that always have a URL; see [`app_webhook_update`] for the decision and
/// [`apply_app_webhook`] for the GitHub call.
async fn enable_webhooks(
    app_id: &str,
    pem: &str,
    public_url: &str,
    stored_secret: Option<String>,
) -> anyhow::Result<()> {
    let update = app_webhook_update(Some(public_url), stored_secret)
        .expect("a public URL always yields a webhook update");
    apply_app_webhook(app_id, pem, &update).await
}

async fn setup_pat(args: &GithubSetupArgs) -> anyhow::Result<()> {
    // Permission checklist derived from the workspace's own workflows.
    let baseline = vec![
        "contents: read".to_owned(),
        "pull-requests: read".to_owned(),
    ];
    let (derived, oidc_declared) = match args.workspace.as_deref() {
        Some(workspace) => workflow_permission_checklist(workspace)?,
        None => (Vec::new(), false),
    };
    let checklist = pat_permission_checklist(&baseline, &derived);
    println!(
        "Create a fine-grained PAT for the repositories you run workflows against:\n\
         \x20 https://github.com/settings/personal-access-tokens/new\n\
         Recommended permissions for your workflows:"
    );
    for line in &checklist {
        println!("  - {line}");
    }
    println!("Metadata: Read-only is always required (GitHub enforces it).");
    // GitHub has no API for minting a fine-grained PAT — unlike Apps, which
    // have the manifest flow — so the browser is unavoidable here. Opening
    // the page is the most that can be automated.
    println!(
        "Note: check runs are App-only at GitHub's end, so a PAT-configured engine\n\
         \x20 records them locally instead of publishing them to the commit. Use\n\
         \x20 `--via app` if you want status checks on PRs."
    );
    if oidc_declared {
        println!(
            "Note: your workflows also declare `id-token: write` — OIDC token issuance. For preloop\n             \x20 the ENGINE is the OIDC issuer (it mints the token with its own key); GitHub's\n             \x20 provider applies only to hosted runs. Either way it is not a PAT permission —\n             \x20 no action needed on the PAT. To consume the token, configure your cloud\n             \x20 provider's trust to accept the engine's OIDC issuer."
        );
    }

    let token = match args.token.clone() {
        Some(token) => token,
        None => match std::env::var("PRELOOP_GITHUB_PAT")
            .ok()
            .filter(|v| !v.is_empty())
        {
            Some(token) => token,
            None => {
                // Echoing a PAT leaves it in the terminal scrollback; hide it
                // when a human is typing, but keep plain stdin for pipes so
                // automation still works. Same convention as `secret set`.
                if std::io::stdin().is_terminal() {
                    // Only for a human at a prompt: a piped token means
                    // automation, which has no use for a browser window.
                    if !args.no_browser {
                        let _ = crate::app_manifest::open_in_browser(
                            "https://github.com/settings/personal-access-tokens/new",
                        );
                    }
                    rpassword::prompt_password("Paste the PAT (hidden): ")?
                        .trim()
                        .to_owned()
                } else {
                    let mut line = String::new();
                    std::io::stdin().read_line(&mut line)?;
                    line.trim().to_owned()
                }
            }
        },
    };
    if token.is_empty() {
        anyhow::bail!("no PAT provided (--token, PRELOOP_GITHUB_PAT, or stdin)");
    }

    // GitHub token prefixes: `github_pat_` is a fine-grained PAT (scoped to
    // selected repos and permissions, expiring); `ghp_` is a classic PAT
    // (broad, long-lived — GitHub deprecated new classic-PAT use); `gho_`
    // is a device-flow OAuth token (account-wide, short-lived). The engine
    // accepts any bearer token, but the guidance is fine-grained: a classic
    // or OAuth token hands every job the account's full scope.
    if !token.starts_with("github_pat_") {
        let kind = if token.starts_with("ghp_") {
            "classic PAT"
        } else if token.starts_with("gho_") {
            "device-flow OAuth token"
        } else {
            "unrecognized token type"
        };
        println!(
            "\nwarning: this looks like a {kind}, not a fine-grained PAT (github_pat_...).\n\
             \x20 Every job this engine runs will carry its full scope — broad and long-lived.\n\
             \x20 Prefer a fine-grained PAT scoped to the repositories you actually run:\n\
             \x20   https://github.com/settings/personal-access-tokens/new\n"
        );
    }

    let mut config = load_config()?;
    let mut writer =
        CredentialWriter::for_config(&config_path(), config.credential_store.as_deref())?;
    let pat_ref = github_reference_with_host("pat", config.github.server_url.as_deref(), None)?;
    persist_secret(
        &mut writer,
        &mut config,
        &pat_ref,
        &SecretString::new(&token),
    )?;
    config.github.legacy_pat = None;
    config.github.pat_ref = Some(pat_ref.as_str().to_owned());
    println!("Stored PAT in {}.", writer.destination());
    config.github.mint_failure = Some("pat".into());
    write_config(&config)?;
    println!("Restart the engine (`preloop serve`) to pick up the config.");
    if args.repos.is_empty() {
        println!("Tip: verify with `preloop doctor --repo owner/name`.");
        return Ok(());
    }
    doctor_pat(&token, &args.repos).await
}

/// Scan a workspace's workflows and return the union of the permissions they
/// declare, e.g. `["contents: read", "pull-requests: read"]`, plus whether
/// any workflow declares `id-token` (OIDC — not a PAT permission).
fn workflow_permission_checklist(workspace: &Path) -> anyhow::Result<(Vec<String>, bool)> {
    let dir = workspace.join(".github").join("workflows");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), false));
        }
        Err(error) => return Err(error.into()),
    };
    let mut out: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&text) else {
            continue;
        };
        collect_permissions(&doc, &mut out);
    }
    let oidc_declared = out.iter().any(|line| line.starts_with("id-token:"));
    let checklist = out
        .into_iter()
        .filter(|line| !line.starts_with("id-token:"))
        .collect();
    Ok((checklist, oidc_declared))
}

/// The PAT checklist: the mandatory baseline unioned with whatever the
/// workspace's workflows explicitly declare. `contents: read` is GitHub's
/// enforced floor and `pull-requests: read` covers the engine's token
/// defaults, so the baseline must survive even when a workflow declares its
/// own permissions — dropping it would print a checklist that omits the one
/// permission GitHub always requires.
fn pat_permission_checklist(baseline: &[String], derived: &[String]) -> Vec<String> {
    let mut checklist = baseline.to_vec();
    for line in derived {
        if !checklist.contains(line) {
            checklist.push(line.clone());
        }
    }
    checklist
}

fn collect_permissions(doc: &serde_yaml::Value, out: &mut std::collections::BTreeSet<String>) {
    if let Some(permissions) = doc.get("permissions") {
        match permissions {
            serde_yaml::Value::Mapping(map) => {
                for (key, value) in map {
                    if let (Some(key), Some(access)) = (key.as_str(), value.as_str()) {
                        out.insert(format!("{key}: {access}"));
                    }
                }
            }
            serde_yaml::Value::String(access) => {
                for line in scalar_permission_lines(access) {
                    out.insert(line);
                }
            }
            _ => {}
        }
    }
    // Job-level permissions blocks.
    if let Some(jobs) = doc.get("jobs").and_then(serde_yaml::Value::as_mapping) {
        for (_, job) in jobs {
            if let Some(serde_yaml::Value::Mapping(map)) = job.get("permissions") {
                for (key, value) in map {
                    if let (Some(key), Some(access)) = (key.as_str(), value.as_str()) {
                        out.insert(format!("{key}: {access}"));
                    }
                }
            }
        }
    }
}

/// Expand a scalar `permissions:` value (`read-all`/`write-all`) over the
/// parser's complete permission-scope list. GitHub applies these to every
/// scope, so the checklist must too — `permissions: read-all` is not just
/// `contents: read`. Read-only scopes (no write level on GitHub) are capped
/// at `read`, matching the parser's expansion.
fn scalar_permission_lines(access: &str) -> Vec<String> {
    let level = match access {
        "read-all" => "read",
        "write-all" => "write",
        _ => return Vec::new(),
    };
    let read_only = preloop_gha_parser::READ_ONLY_SCOPES;
    preloop_gha_parser::PERMISSION_SCOPES
        .iter()
        .map(|scope| {
            let granted = if level == "write" && read_only.contains(scope) {
                "read"
            } else {
                level
            };
            format!("{scope}: {granted}")
        })
        .collect()
}

pub(crate) async fn doctor_app(app_id: &str, pem: &str, repos: &[String]) -> anyhow::Result<()> {
    let mut failed = false;
    for repo in repos {
        match preloop_runner_server::github_app::verify_app_config(app_id, pem, repo).await {
            Ok(granted) => {
                let names = granted
                    .iter()
                    .map(|(p, a)| format!("{p}: {a}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("✓ App {app_id} on {repo}: {names}");
            }
            Err(error) => {
                println!("✗ App {app_id} on {repo}: {error:#}");
                failed = true;
            }
        }
    }
    anyhow::ensure!(!failed, "one or more App verification checks failed");
    Ok(())
}

pub(crate) async fn doctor_pat(token: &str, repos: &[String]) -> anyhow::Result<()> {
    let mut failed = false;
    for repo in repos {
        match preloop_runner_server::github_app::verify_repo_access_with_token(token, repo).await {
            Ok(granted) => {
                let names = granted
                    .iter()
                    .map(|(p, a)| format!("{p}: {a}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("✓ PAT on {repo}: {names}");
            }
            Err(error) => {
                println!("✗ PAT on {repo}: {error:#}");
                failed = true;
            }
        }
    }
    anyhow::ensure!(!failed, "one or more PAT verification checks failed");
    Ok(())
}

pub(crate) async fn cmd_doctor(args: DoctorArgs) -> anyhow::Result<()> {
    let path = preloop_runner_server::config::config_path();
    if !path.exists() {
        anyhow::bail!(
            "no config file at {} — run `preloop setup github --via app` first \
             (creates the GitHub App in your browser)",
            path.display()
        );
    }
    println!("config: {}", path.display());
    let config = preloop_runner_server::config::load_config()?;
    let mut failed = false;
    let mut app_found = false;

    if let (Some(app_id), Some(pem)) = (config.github.app_id.as_deref(), config.github.app_pem()) {
        app_found = true;
        println!(
            "github app: configured (id {app_id}, mint failure = {})",
            config.github.mint_failure.as_deref().unwrap_or("local")
        );
        if !args.repos.is_empty() {
            if let Err(error) = doctor_app(app_id, pem, &args.repos).await {
                println!("{error:#}");
                failed = true;
            }
        } else {
            println!("  (pass --repo owner/name to verify against GitHub)");
        }
    }
    for app in &config.github.apps {
        app_found = true;
        println!(
            "github app: configured (id {}, mint failure = {})",
            app.app_id,
            config.github.mint_failure.as_deref().unwrap_or("local")
        );
        if !args.repos.is_empty() {
            if let Err(error) = doctor_app(&app.app_id, app.pem(), &args.repos).await {
                println!("{error:#}");
                failed = true;
            }
        } else if config.github.app_id.is_none() {
            println!("  (pass --repo owner/name to verify against GitHub)");
        }
    }
    if !app_found {
        println!("github app: not configured (jobs get no GitHub authority)");
    }
    if let Some(pat) = config.github.pat() {
        println!(
            "github pat: configured (mint failure = {})",
            config.github.mint_failure.as_deref().unwrap_or("local")
        );
        if !args.repos.is_empty()
            && let Err(error) = doctor_pat(pat, &args.repos).await
        {
            println!("{error:#}");
            failed = true;
        }
    } else {
        println!("github pat: not configured");
    }
    if !config.secrets.is_empty() {
        println!("secrets: {} stored", config.secrets.len());
    }
    anyhow::ensure!(!failed, "one or more checks failed");
    Ok(())
}

/// Outcome of a live-engine call: applied through the engine, or the engine
/// is unavailable/too old, in which case the caller falls back to writing
/// the config file directly (applies on next engine start).
pub(crate) enum ApiOutcome<T> {
    Applied(T),
    Unavailable,
}

fn valid_secret_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn valid_env_scope(env: &str) -> bool {
    !env.is_empty()
        && env.len() <= 255
        && !env.starts_with(['-', '_'])
        && env
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn scope_suffix(repo: Option<&str>, env: Option<&str>) -> String {
    match (repo, env) {
        (Some(repo), Some(env)) => format!(" (for {repo}, environment {env})"),
        (Some(repo), None) => format!(" (for {repo})"),
        (None, None) => String::new(),
        (None, Some(_)) => unreachable!("env requires repo, validated by callers"),
    }
}

/// Map a request to the engine into `Unavailable` when the engine is down
/// or predates the live secrets API (404), `Applied` on success, and an
/// error for anything the engine actively rejected.
async fn api_request(
    request: reqwest::RequestBuilder,
) -> Result<ApiOutcome<reqwest::Response>, anyhow::Error> {
    let request = match crate::api_token() {
        Some(token) => request.bearer_auth(token),
        None => request,
    };
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) if error.is_connect() || error.is_timeout() => {
            return Ok(ApiOutcome::Unavailable);
        }
        Err(error) => return Err(error).context("talking to the engine"),
    };
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(ApiOutcome::Unavailable);
    }
    if response.status().is_success() {
        return Ok(ApiOutcome::Applied(response));
    }
    let body = response.text().await.unwrap_or_default();
    anyhow::bail!("engine rejected the request: {body}")
}

async fn api_set_secret(
    name: &str,
    value: &str,
    repo: Option<&str>,
    env: Option<&str>,
) -> Result<ApiOutcome<()>, anyhow::Error> {
    let client = crate::build_client();
    let url = format!("{}/api/v1/secrets/{name}", crate::server_url());
    let request = client.put(&url).json(&serde_json::json!({
        "value": value,
        "repo": repo,
        "env": env,
    }));
    match api_request(request).await? {
        ApiOutcome::Applied(_) => Ok(ApiOutcome::Applied(())),
        ApiOutcome::Unavailable => Ok(ApiOutcome::Unavailable),
    }
}

async fn api_delete_secret(
    name: &str,
    repo: Option<&str>,
    env: Option<&str>,
) -> Result<ApiOutcome<()>, anyhow::Error> {
    let client = crate::build_client();
    let url = format!("{}/api/v1/secrets/{name}", crate::server_url());
    let mut request = client.delete(&url);
    if let Some(repo) = repo {
        request = request.query(&[("repo", repo)]);
    }
    if let Some(env) = env {
        request = request.query(&[("env", env)]);
    }
    match api_request(request).await? {
        ApiOutcome::Applied(_) => Ok(ApiOutcome::Applied(())),
        ApiOutcome::Unavailable => Ok(ApiOutcome::Unavailable),
    }
}

/// List stored secret names from the engine. `None` means the engine is
/// unavailable; the caller falls back to reading the config file.
pub(crate) async fn api_list_secrets(
    repo: Option<&str>,
    env: Option<&str>,
) -> Result<ApiOutcome<Vec<(String, Option<String>, Option<String>)>>, anyhow::Error> {
    let client = crate::build_client();
    let url = format!("{}/api/v1/secrets", crate::server_url());
    let mut request = client.get(&url);
    if let Some(repo) = repo {
        request = request.query(&[("repo", repo)]);
    }
    if let Some(env) = env {
        request = request.query(&[("env", env)]);
    }
    match api_request(request).await? {
        ApiOutcome::Unavailable => Ok(ApiOutcome::Unavailable),
        ApiOutcome::Applied(response) => {
            let body: serde_json::Value =
                response.json().await.context("parsing engine response")?;
            let secrets = body
                .get("secrets")
                .and_then(serde_json::Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|entry| {
                            let name = entry.get("name")?.as_str()?.to_owned();
                            let repo = entry
                                .get("repo")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned);
                            let env = entry
                                .get("env")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned);
                            Some((name, repo, env))
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(ApiOutcome::Applied(secrets))
        }
    }
}

pub(crate) async fn cmd_secret(args: SecretArgs) -> anyhow::Result<()> {
    match args.command {
        SecretCommand::Set {
            name,
            value,
            repo,
            env,
        } => {
            if !valid_secret_name(&name) {
                anyhow::bail!("secret name must be UPPER_SNAKE (letters, digits, underscore)");
            }
            if let Some(repo) = &repo
                && !valid_repo(repo)
            {
                anyhow::bail!("--repo must look like owner/repo");
            }
            if let Some(env) = &env {
                if !valid_env_scope(env) {
                    anyhow::bail!(
                        "--env must be letters, digits, hyphens, underscores (max 255, not starting with `-` or `_`)"
                    );
                }
                if repo.is_none() {
                    anyhow::bail!("--env requires --repo (owner/repo)");
                }
            }
            let value = match value {
                Some(value) => value,
                None => {
                    if std::io::stdin().is_terminal() {
                        rpassword::prompt_password("secret value (hidden): ")?
                    } else {
                        let mut line = String::new();
                        std::io::stdin().read_line(&mut line)?;
                        line.trim().to_owned()
                    }
                }
            };
            if value.is_empty() {
                anyhow::bail!("empty secret value");
            }
            match api_set_secret(&name, &value, repo.as_deref(), env.as_deref()).await? {
                ApiOutcome::Applied(()) => {
                    println!(
                        "secret {name}{} stored (live)",
                        scope_suffix(repo.as_deref(), env.as_deref())
                    );
                }
                ApiOutcome::Unavailable => {
                    let mut config = load_config()?;
                    if store_memory(&config)? {
                        anyhow::bail!(
                            "secrets store is memory-only (secrets_store = \"memory\" or \
                             PRELOOP_SECRETS_STORE=memory): nothing is written to the config \
                             file — start the engine (`preloop serve`) to set secrets live"
                        );
                    }
                    match (&repo, &env) {
                        (Some(repo), Some(env)) => {
                            config
                                .env_secrets
                                .entry(repo.clone())
                                .or_default()
                                .entry(env.clone())
                                .or_default()
                                .insert(name.clone(), value);
                        }
                        (Some(repo), None) => {
                            config
                                .repo_secrets
                                .entry(repo.clone())
                                .or_default()
                                .insert(name.clone(), value);
                        }
                        (None, None) => {
                            config.secrets.insert(name.clone(), value);
                        }
                        (None, Some(_)) => unreachable!("env requires repo, validated above"),
                    }
                    let path = write_config(&config)?;
                    println!(
                        "secret {name}{} stored in {} (engine not running or predates live secrets — applies on next start)",
                        scope_suffix(repo.as_deref(), env.as_deref()),
                        path.display()
                    );
                }
            }
        }
        SecretCommand::List { repo, env } => {
            if let Some(repo) = &repo
                && !valid_repo(repo)
            {
                anyhow::bail!("--repo must look like owner/repo");
            }
            if env.is_some() && repo.is_none() {
                anyhow::bail!("--env requires --repo (owner/repo)");
            }
            if let Some(env) = &env
                && !valid_env_scope(env)
            {
                anyhow::bail!(
                    "--env must be letters, digits, hyphens, underscores (max 255, not starting with `-` or `_`)"
                );
            }
            match api_list_secrets(repo.as_deref(), env.as_deref()).await? {
                ApiOutcome::Applied(secrets) => {
                    if secrets.is_empty() {
                        println!(
                            "no secrets stored{}",
                            scope_suffix(repo.as_deref(), env.as_deref())
                        );
                    } else {
                        for (name, scope, env) in secrets {
                            match (scope, env) {
                                (Some(repo), Some(env)) => {
                                    println!("{name} ({repo}, environment {env})")
                                }
                                (Some(repo), None) => println!("{name} ({repo})"),
                                (None, None) => println!("{name}"),
                                (None, Some(_)) => unreachable!("env requires repo on the server"),
                            }
                        }
                    }
                }
                ApiOutcome::Unavailable => {
                    let config = load_config()?;
                    let mut secrets: Vec<String> = config.secrets.keys().cloned().collect();
                    match (&repo, &env) {
                        (Some(repo), Some(env)) => {
                            if let Some(map) =
                                config.env_secrets.get(repo).and_then(|envs| envs.get(env))
                            {
                                secrets = map.keys().cloned().collect();
                            } else {
                                secrets.clear();
                            }
                        }
                        (Some(repo), None) => {
                            if let Some(map) = config.repo_secrets.get(repo) {
                                secrets = map.keys().cloned().collect();
                            } else {
                                secrets.clear();
                            }
                        }
                        (None, None) => {
                            for (scope, map) in &config.repo_secrets {
                                secrets.extend(map.keys().map(|name| format!("{name} ({scope})")));
                            }
                            for (scope, envs) in &config.env_secrets {
                                for (env, map) in envs {
                                    secrets.extend(map.keys().map(|name| {
                                        format!("{name} ({scope}, environment {env})")
                                    }));
                                }
                            }
                        }
                        (None, Some(_)) => unreachable!("env requires repo, validated above"),
                    }
                    if secrets.is_empty() {
                        println!(
                            "no secrets stored{}",
                            scope_suffix(repo.as_deref(), env.as_deref())
                        );
                    } else {
                        for entry in secrets {
                            println!("{entry}");
                        }
                    }
                }
            }
        }
        SecretCommand::Rm { name, repo, env } => {
            if let Some(repo) = &repo
                && !valid_repo(repo)
            {
                anyhow::bail!("--repo must look like owner/repo");
            }
            if let Some(env) = &env {
                if !valid_env_scope(env) {
                    anyhow::bail!(
                        "--env must be letters, digits, hyphens, underscores (max 255, not starting with `-` or `_`)"
                    );
                }
                if repo.is_none() {
                    anyhow::bail!("--env requires --repo (owner/repo)");
                }
            }
            match api_delete_secret(&name, repo.as_deref(), env.as_deref()).await? {
                ApiOutcome::Applied(()) => {
                    println!(
                        "secret {name}{} removed",
                        scope_suffix(repo.as_deref(), env.as_deref())
                    );
                }
                ApiOutcome::Unavailable => {
                    let mut config = load_config()?;
                    if store_memory(&config)? {
                        anyhow::bail!(
                            "secrets store is memory-only (secrets_store = \"memory\" or \
                             PRELOOP_SECRETS_STORE=memory): nothing is written to the config \
                             file — start the engine (`preloop serve`) to remove secrets live"
                        );
                    }
                    let removed = match (&repo, &env) {
                        (Some(repo), Some(env)) => {
                            let removed = config.env_secrets.get_mut(repo).is_some_and(|envs| {
                                envs.get_mut(env)
                                    .is_some_and(|map| map.remove(&name).is_some())
                            });
                            if removed {
                                if config.env_secrets.get(repo).is_some_and(|envs| {
                                    envs.get(env).is_some_and(BTreeMap::is_empty)
                                }) {
                                    config
                                        .env_secrets
                                        .get_mut(repo)
                                        .expect("envs exist when env map exists")
                                        .remove(env);
                                }
                                if config.env_secrets.get(repo).is_some_and(BTreeMap::is_empty) {
                                    config.env_secrets.remove(repo);
                                }
                            }
                            removed
                        }
                        (Some(repo), None) => {
                            let removed = config
                                .repo_secrets
                                .get_mut(repo)
                                .is_some_and(|map| map.remove(&name).is_some());
                            if removed
                                && config
                                    .repo_secrets
                                    .get(repo)
                                    .is_some_and(|map| map.is_empty())
                            {
                                config.repo_secrets.remove(repo);
                            }
                            removed
                        }
                        (None, None) => config.secrets.remove(&name).is_some(),
                        (None, Some(_)) => unreachable!("env requires repo, validated above"),
                    };
                    if !removed {
                        anyhow::bail!(
                            "no secret named {name}{}",
                            scope_suffix(repo.as_deref(), env.as_deref())
                        );
                    }
                    let path = write_config(&config)?;
                    println!(
                        "secret {name}{} removed from {} (engine not running or predates live secrets)",
                        scope_suffix(repo.as_deref(), env.as_deref()),
                        path.display()
                    );
                }
            }
        }
    }
    Ok(())
}

/// The config file the engine reads, pinned to the CLI's home so both agree
/// even under XDG_DATA_HOME.
pub(crate) fn config_path_for_home() -> std::path::PathBuf {
    preloop_home().join("config.toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use preloop_runner_server::credential_store::{CREDENTIAL_STORE_ENV, SecretString};
    use preloop_runner_server::github_app_setup::record_app_credentials;
    #[test]
    fn checklist_union_of_workflow_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let wf = dir.path().join(".github/workflows");
        std::fs::create_dir_all(&wf).unwrap();
        std::fs::write(
            wf.join("a.yml"),
            "on: push\npermissions: { contents: read, pull-requests: write }\njobs: {}\n",
        )
        .unwrap();
        std::fs::write(
            wf.join("b.yml"),
            "on: push\njobs:\n  x:\n    permissions: { issues: read }\n    runs-on: ubuntu-latest\n    steps: []\n",
        )
        .unwrap();
        std::fs::write(wf.join("not-a-workflow.txt"), "nope").unwrap();
        let (checklist, oidc) = workflow_permission_checklist(dir.path()).unwrap();
        assert!(!oidc);
        assert_eq!(
            checklist,
            vec![
                "contents: read".to_owned(),
                "issues: read".to_owned(),
                "pull-requests: write".to_owned()
            ]
        );
    }

    #[test]
    fn checklist_empty_without_workflows() {
        let dir = tempfile::tempdir().unwrap();
        let (checklist, oidc) = workflow_permission_checklist(dir.path()).unwrap();
        assert!(checklist.is_empty());
        assert!(!oidc);
    }

    #[test]
    fn checklist_expands_scalar_read_all_and_write_all() {
        let dir = tempfile::tempdir().unwrap();
        let wf = dir.path().join(".github/workflows");
        std::fs::create_dir_all(&wf).unwrap();
        std::fs::write(
            wf.join("read-all.yml"),
            "on: push\npermissions: read-all\njobs: {}\n",
        )
        .unwrap();
        std::fs::write(
            wf.join("write-all.yml"),
            "on: push\npermissions: write-all\njobs: {}\n",
        )
        .unwrap();
        let (checklist, oidc) = workflow_permission_checklist(dir.path()).unwrap();
        assert!(oidc, "read-all/write-all cover id-token too");
        for scope in preloop_gha_parser::PERMISSION_SCOPES {
            if scope == "id-token" {
                continue;
            }
            assert!(
                checklist.contains(&format!("{scope}: read")),
                "read-all must expand to {scope}: read, got {checklist:?}"
            );
            if preloop_gha_parser::READ_ONLY_SCOPES.contains(&scope) {
                continue;
            }
            assert!(
                checklist.contains(&format!("{scope}: write")),
                "write-all must expand to {scope}: write, got {checklist:?}"
            );
        }
    }

    #[test]
    fn pat_checklist_keeps_baseline_when_explicit_permissions_exist() {
        let baseline = vec![
            "contents: read".to_owned(),
            "pull-requests: read".to_owned(),
        ];
        let derived = vec!["issues: write".to_owned(), "contents: read".to_owned()];
        let checklist = pat_permission_checklist(&baseline, &derived);
        assert_eq!(
            checklist,
            vec![
                "contents: read".to_owned(),
                "pull-requests: read".to_owned(),
                "issues: write".to_owned()
            ],
            "the mandatory baseline (contents: read, pull-requests: read) must survive \
             any explicit workflow permissions"
        );
    }

    /// A full `GithubSetupArgs` with everything but the fields under test at
    /// the manual-App defaults, so those tests stay readable.
    fn app_args(
        app_id: Option<&str>,
        pem_file: Option<&Path>,
        public_url: Option<&str>,
    ) -> GithubSetupArgs {
        GithubSetupArgs {
            via: Some(Via::App),
            app_id: app_id.map(str::to_owned),
            pem_file: pem_file.map(Path::to_path_buf),
            add: false,
            org: None,
            port: 0,
            public_url: public_url.map(str::to_owned),
            app_name: "preloop-local".to_owned(),
            no_browser: true,
            webhook_secret: None,
            token: None,
            repos: Vec::new(),
            workspace: None,
        }
    }

    /// The manual App path (`--app-id` + `--pem-file`) saves credentials, which
    /// on its own leaves the hook inactive: the manifest flow is what enables
    /// webhooks for an App the browser created. So when a URL is given, that
    /// path must end in a webhook update at exactly the URL checked here, or
    /// pushes and pull requests never start a run.
    #[test]
    fn the_manual_app_path_enables_the_webhook_at_the_given_url() {
        let args = app_args(
            Some("4242"),
            Some(Path::new("/tmp/app.pem")),
            Some("https://ci.example.com/"),
        );
        let update =
            app_webhook_update(args.public_url.as_deref(), Some("stored-secret".to_owned()))
                .expect("--public-url must produce a webhook update for the manual App path");
        assert_eq!(update.url, "https://ci.example.com/api/v1/github/webhooks");
        assert_eq!(
            update.secret, "stored-secret",
            "the secret GitHub already signs with is reused, never rotated"
        );

        // A fresh secret is minted only when none is stored (GitHub signs
        // deliveries only once a secret is set), and it is hex, like the
        // engine's own secrets.
        let fresh = app_webhook_update(args.public_url.as_deref(), Some(String::new())).unwrap();
        assert_eq!(fresh.url, update.url);
        assert_ne!(fresh.secret, update.secret);
        assert_eq!(fresh.secret.len(), 64);
        assert!(fresh.secret.chars().all(|c| c.is_ascii_hexdigit()));

        // No URL: nothing to point anywhere. The manual path then saves the
        // credentials and touches no webhook, exactly as before.
        let no_url = app_args(Some("4242"), Some(Path::new("/tmp/app.pem")), None);
        assert_eq!(
            app_webhook_update(no_url.public_url.as_deref(), Some("s".to_owned())),
            None
        );
    }

    /// Reading the stored secret back is per App: `--app-id` may name a
    /// registry entry (`github.apps`) rather than the primary App, and picking
    /// the wrong secret would break the App that is actually delivering.
    #[test]
    fn the_stored_webhook_secret_is_found_for_the_app_in_question() {
        let mut config = ConfigFile::default();
        assert_eq!(stored_webhook_secret(&config, "4242"), None);

        config.github.app_id = Some("4242".to_owned());
        config.github.legacy_webhook_secret = Some("primary-secret".to_owned());
        assert_eq!(
            stored_webhook_secret(&config, "4242").as_deref(),
            Some("primary-secret")
        );
        assert_eq!(stored_webhook_secret(&config, "9999"), None);

        config
            .github
            .apps
            .push(preloop_runner_server::config::AppConfig {
                app_id: "9999".to_owned(),
                legacy_webhook_secret: Some("registry-secret".to_owned()),
                ..Default::default()
            });
        assert_eq!(
            stored_webhook_secret(&config, "9999").as_deref(),
            Some("registry-secret")
        );
        assert_eq!(
            stored_webhook_secret(&config, "4242").as_deref(),
            Some("primary-secret"),
            "a registry entry must not shadow the primary App's own secret"
        );
    }

    /// A throwaway private key: the webhook tests only need *some* PEM saved
    /// so the App id is bound in the config, and nothing parses it.
    const TEST_APP_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nTEST-ONLY-NOT-A-REAL-KEY\n-----END RSA PRIVATE KEY-----\n";

    /// The env keys the setup writers read, captured so a test can restore
    /// them. Paired with [`pin_setup_env`] / [`restore_setup_env`], with
    /// [`crate::TEST_ENV_LOCK`] held across the pair.
    fn capture_setup_env() -> Vec<(&'static str, Option<std::ffi::OsString>)> {
        [
            preloop_runner_server::config::CONFIG_PATH_ENV,
            "PRELOOP_HOME",
            CREDENTIAL_STORE_ENV,
        ]
        .into_iter()
        .map(|key| (key, std::env::var_os(key)))
        .collect()
    }

    /// Pin the config file and the credential store to `home`, so the setup
    /// writers touch a temp dir instead of the developer's real config, home,
    /// and keychain.
    #[allow(unsafe_code)]
    fn pin_setup_env(home: &Path) {
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access.
        unsafe {
            std::env::set_var(
                preloop_runner_server::config::CONFIG_PATH_ENV,
                home.join("config.toml"),
            );
            std::env::set_var("PRELOOP_HOME", home);
            std::env::set_var(CREDENTIAL_STORE_ENV, "file");
        }
    }

    /// Put back every value [`capture_setup_env`] read.
    #[allow(unsafe_code)]
    fn restore_setup_env(previous: Vec<(&'static str, Option<std::ffi::OsString>)>) {
        // SAFETY: TEST_ENV_LOCK was held from the capture through the restore.
        unsafe {
            for (key, value) in previous {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    /// GitHub must only be told to sign with a secret the local store already
    /// holds. The reversed order has a failure mode with no local trace: if
    /// the credential-store or config write fails after GitHub accepted the
    /// secret (read-only filesystem, refused keychain), GitHub signs every
    /// delivery with a secret nobody has and each one fails verification.
    /// `point_hook` stands in for the network call, so the order is observable
    /// offline: the state it sees is the persisted one.
    #[test]
    fn the_webhook_secret_is_stored_before_github_is_pointed_at_the_hook() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let home = tempfile::tempdir().unwrap();
        let previous = capture_setup_env();
        pin_setup_env(home.path());
        // The production order: credentials are saved first, which binds the
        // App id in the config, and the webhook secret is what follows.
        save_app_credentials("4242", &SecretString::new(TEST_APP_PEM), None)
            .expect("the credentials must save");
        let update = WebhookUpdate {
            url: "https://ci.example.com/api/v1/github/webhooks".to_owned(),
            secret: "fresh-secret".to_owned(),
        };
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                persist_secret_then_point_hook("4242", &update, |secret| async move {
                    assert_eq!(secret, "fresh-secret");
                    assert_eq!(
                        stored_webhook_secret(&load_config()?, "4242").as_deref(),
                        Some("fresh-secret"),
                        "GitHub must not be told to sign with a secret that is not stored yet"
                    );
                    Ok::<(), anyhow::Error>(())
                })
                .await
            });
        restore_setup_env(previous);
        result.expect("a stored secret must let the GitHub call run");
    }

    /// A GitHub call that fails after the secret was stored keeps the stored
    /// secret — the hook is unchanged, so the retry must reuse that secret
    /// instead of rotating a delivery the operator is about to fix — and the
    /// error says where the secret is, because an operator who cannot tell
    /// whether it survived has to guess before retrying.
    #[test]
    fn a_failed_github_call_keeps_the_stored_webhook_secret() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let home = tempfile::tempdir().unwrap();
        let previous = capture_setup_env();
        pin_setup_env(home.path());
        save_app_credentials("4242", &SecretString::new(TEST_APP_PEM), None)
            .expect("the credentials must save");
        let update = WebhookUpdate {
            url: "https://ci.example.com/api/v1/github/webhooks".to_owned(),
            secret: "fresh-secret".to_owned(),
        };
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let error = persist_secret_then_point_hook("4242", &update, |_secret| async move {
                    Err::<(), anyhow::Error>(anyhow::anyhow!(
                        "GitHub refused the webhook update (403): forbidden"
                    ))
                })
                .await
                .expect_err("a failed GitHub call must fail the command");
                let config = load_config()?;
                Ok::<_, anyhow::Error>((
                    format!("{error:#}"),
                    stored_webhook_secret(&config, "4242"),
                ))
            });
        restore_setup_env(previous);
        let (message, stored) = result.expect("the config must still load after the failure");
        assert!(
            message.contains("stored locally") && message.contains("still delivers nowhere"),
            "the error must say the secret was kept and the hook is unchanged: {message}"
        );
        assert_eq!(
            stored.as_deref(),
            Some("fresh-secret"),
            "the secret stored before the failed GitHub call must be kept"
        );
    }

    /// The failure the order exists to prevent: a secret that cannot be
    /// stored locally must never reach GitHub, or the hook would sign with a
    /// secret that exists nowhere. A plain file where the credentials
    /// directory belongs makes every store write fail, like a read-only or
    /// full filesystem.
    #[test]
    fn a_failing_local_write_stops_the_github_call() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join("credentials"), "not a directory").unwrap();
        let previous = capture_setup_env();
        pin_setup_env(home.path());
        let update = WebhookUpdate {
            url: "https://ci.example.com/api/v1/github/webhooks".to_owned(),
            secret: "fresh-secret".to_owned(),
        };
        let github_called = std::cell::Cell::new(false);
        let called = &github_called;
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                persist_secret_then_point_hook("4242", &update, |_secret| async move {
                    called.set(true);
                    Ok::<(), anyhow::Error>(())
                })
                .await
            });
        restore_setup_env(previous);
        result.expect_err("a secret that cannot be stored must fail the command");
        assert!(
            !github_called.get(),
            "GitHub must not be told to sign with a secret that was never stored"
        );
    }

    /// `--repo` on the secret commands is a *scope*, and a workspace identity
    /// is `local/<dir>` — any directory basename, not a GitHub slug. The
    /// stricter slug check, which is what a URL-interpolated request needs,
    /// refuses every such scope, so all three subcommands are exercised here.
    #[test]
    #[allow(unsafe_code)]
    fn secret_scopes_accept_a_local_workspace_repo() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let home = tempfile::tempdir().unwrap();
        let config_path = home.path().join("config.toml");
        let previous: Vec<(&str, Option<std::ffi::OsString>)> = [
            preloop_runner_server::config::CONFIG_PATH_ENV,
            "PRELOOP_HOME",
            CREDENTIAL_STORE_ENV,
            "PRELOOP_URL",
            "PRELOOP_SECRETS_STORE",
        ]
        .into_iter()
        .map(|key| (key, std::env::var_os(key)))
        .collect();
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access, and
        // every value read above is restored below. Port 1 refuses instantly,
        // so each command takes its local-config path instead of talking to an
        // engine that may be running on this host.
        unsafe {
            std::env::set_var(preloop_runner_server::config::CONFIG_PATH_ENV, &config_path);
            std::env::set_var("PRELOOP_HOME", home.path());
            std::env::set_var(CREDENTIAL_STORE_ENV, "file");
            std::env::set_var("PRELOOP_URL", "http://127.0.0.1:1");
            std::env::remove_var("PRELOOP_SECRETS_STORE");
        }
        let scope = "local/my-checkout".to_owned();
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                cmd_secret(SecretArgs {
                    command: SecretCommand::Set {
                        name: "TOKEN".to_owned(),
                        value: Some("v".to_owned()),
                        repo: Some(scope.clone()),
                        env: None,
                    },
                })
                .await?;
                let stored = preloop_runner_server::config::load_config_from(&config_path)?;
                assert!(
                    stored
                        .repo_secrets
                        .get(&scope)
                        .is_some_and(|map| map.contains_key("TOKEN")),
                    "the secret must be scoped to the local workspace identity"
                );
                cmd_secret(SecretArgs {
                    command: SecretCommand::List {
                        repo: Some(scope.clone()),
                        env: None,
                    },
                })
                .await?;
                let refused = cmd_secret(SecretArgs {
                    command: SecretCommand::Set {
                        name: "TOKEN".to_owned(),
                        value: Some("v".to_owned()),
                        repo: Some("no-slash".to_owned()),
                        env: None,
                    },
                })
                .await;
                let refused = refused.expect_err("a scope without owner/repo must be refused");
                assert!(refused.to_string().contains("owner/repo"), "{refused}");
                cmd_secret(SecretArgs {
                    command: SecretCommand::Rm {
                        name: "TOKEN".to_owned(),
                        repo: Some(scope.clone()),
                        env: None,
                    },
                })
                .await
            });
        // SAFETY: restores every value read above.
        unsafe {
            for (key, value) in previous {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
        result.expect("a local/<dir> scope must be accepted by set, list, and rm");
    }

    /// An OS store that refuses every write, like a locked keychain.
    struct RefusingOsStore;

    impl preloop_runner_server::credential_store::CredentialStore for RefusingOsStore {
        fn get(&self, _reference: &CredentialRef) -> anyhow::Result<Option<SecretString>> {
            Ok(None)
        }

        fn set(&self, _reference: &CredentialRef, _value: &SecretString) -> anyhow::Result<()> {
            anyhow::bail!("keychain refused the write")
        }

        fn delete(&self, _reference: &CredentialRef) -> anyhow::Result<()> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            "operating-system credential store"
        }
    }

    /// When the OS store refuses the App's private key, the key is still kept
    /// (in the file store), the config holds only a reference, and a normal
    /// config load returns the key byte-for-byte.
    #[test]
    #[allow(unsafe_code)]
    fn app_key_survives_an_os_store_write_failure() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        use preloop_runner_server::credential_store::{CREDENTIAL_STORE_ENV, FileCredentialStore};

        let home = tempfile::tempdir().unwrap();
        let config_path = home.path().join("config.toml");
        let pem =
            "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAKj\n-----END RSA PRIVATE KEY-----\n";
        let mut config = ConfigFile {
            credential_store: Some("os".into()),
            ..ConfigFile::default()
        };
        let mut writer = CredentialWriter::with_fallback(
            std::sync::Arc::new(RefusingOsStore),
            FileCredentialStore::new(FileCredentialStore::default_dir(home.path())),
        );
        record_app_credentials(
            &mut config,
            &mut writer,
            "4242",
            &SecretString::new(pem),
            Some(SecretString::new("hook-secret")),
        )
        .unwrap();
        preloop_runner_server::config::write_config_to(&config_path, &config).unwrap();

        let text = std::fs::read_to_string(&config_path).unwrap();
        assert!(
            !text.contains("BEGIN RSA"),
            "private key written inline: {text}"
        );
        assert!(text.contains("app_pem_ref"), "{text}");

        // The load must not be redirected by an environment override.
        let previous = std::env::var_os(CREDENTIAL_STORE_ENV);
        // SAFETY: the previous value is restored below; no test in this module reads it.
        unsafe { std::env::remove_var(CREDENTIAL_STORE_ENV) };
        let loaded = preloop_runner_server::config::load_config_from(&config_path);
        // SAFETY: restores the value read above.
        unsafe {
            match previous {
                Some(value) => std::env::set_var(CREDENTIAL_STORE_ENV, value),
                None => std::env::remove_var(CREDENTIAL_STORE_ENV),
            }
        }
        let loaded = loaded.unwrap();
        assert_eq!(loaded.github.app_pem(), Some(pem));
        assert_eq!(loaded.github.webhook_secret(), Some("hook-secret"));
        assert_eq!(loaded.credential_store.as_deref(), Some("file"));
    }
    /// The real setup writers and a subsequent normal config load agree on
    /// the credential directory when an explicit config path is outside the
    /// CLI home. The file backend keeps this test off the host keychain.
    #[test]
    #[allow(unsafe_code)]
    fn setup_round_trips_app_and_pat_beside_external_config() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let home = tempfile::tempdir().unwrap();
        let config_root = tempfile::tempdir().unwrap();
        let config_path = config_root.path().join("config.toml");
        let previous_config = std::env::var_os(preloop_runner_server::config::CONFIG_PATH_ENV);
        let previous_home = std::env::var_os("PRELOOP_HOME");
        let previous_store = std::env::var_os(CREDENTIAL_STORE_ENV);
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access
        // across tests; each value is restored below.
        unsafe {
            std::env::set_var(preloop_runner_server::config::CONFIG_PATH_ENV, &config_path);
            std::env::set_var("PRELOOP_HOME", home.path());
            std::env::set_var(CREDENTIAL_STORE_ENV, "file");
        }

        let pem = "-----BEGIN RSA PRIVATE KEY-----\nEXTERNAL-PEM\n-----END RSA PRIVATE KEY-----\n";
        let pem_path = home.path().join("app.pem");
        std::fs::write(&pem_path, pem).unwrap();
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                save_app_credentials("9876", &SecretString::new(pem), None)?;
                setup_pat(&GithubSetupArgs {
                    via: Some(Via::Pat),
                    app_id: None,
                    pem_file: None,
                    add: false,
                    org: None,
                    port: 0,
                    public_url: None,
                    app_name: "preloop-test".to_owned(),
                    no_browser: true,
                    webhook_secret: None,
                    token: Some("github_pat_external".to_owned()),
                    repos: Vec::new(),
                    workspace: None,
                })
                .await?;
                preloop_runner_server::config::load_config_from(&config_path)
            });

        // SAFETY: restores the values captured before the test.
        unsafe {
            match previous_config {
                Some(value) => {
                    std::env::set_var(preloop_runner_server::config::CONFIG_PATH_ENV, value)
                }
                None => std::env::remove_var(preloop_runner_server::config::CONFIG_PATH_ENV),
            }
            match previous_home {
                Some(value) => std::env::set_var("PRELOOP_HOME", value),
                None => std::env::remove_var("PRELOOP_HOME"),
            }
            match previous_store {
                Some(value) => std::env::set_var(CREDENTIAL_STORE_ENV, value),
                None => std::env::remove_var(CREDENTIAL_STORE_ENV),
            }
        }

        let loaded = result.unwrap();
        assert_eq!(loaded.github.app_pem(), Some(pem));
        assert_eq!(loaded.github.pat(), Some("github_pat_external"));
        assert!(config_root.path().join("credentials").is_dir());
        assert!(!home.path().join("credentials").exists());
    }

    #[test]
    fn the_transport_decision_is_public_url_then_ssh_then_loopback() {
        assert_eq!(
            choose_app_setup_transport(Some("https://ci.example.com/"), true),
            AppSetupTransport::Engine {
                public_url: "https://ci.example.com/".to_owned()
            },
            "a public URL wins even over SSH"
        );
        assert_eq!(
            choose_app_setup_transport(Some("https://ci.example.com"), false),
            AppSetupTransport::Engine {
                public_url: "https://ci.example.com".to_owned()
            }
        );
        assert_eq!(
            choose_app_setup_transport(None, true),
            AppSetupTransport::SshForward
        );
        assert_eq!(
            choose_app_setup_transport(None, false),
            AppSetupTransport::Loopback
        );
        assert_eq!(
            choose_app_setup_transport(Some("  "), true),
            AppSetupTransport::SshForward,
            "a blank URL is no URL"
        );
    }

    #[test]
    fn the_public_url_comes_from_the_flag_then_the_environment() {
        assert_eq!(
            resolve_public_url(Some(" https://flag.example "), Some("https://env.example"))
                .as_deref(),
            Some("https://flag.example")
        );
        assert_eq!(
            resolve_public_url(None, Some("https://env.example")).as_deref(),
            Some("https://env.example")
        );
        assert_eq!(
            resolve_public_url(Some(""), Some("https://env.example")).as_deref(),
            Some("https://env.example"),
            "a blank flag does not hide the environment"
        );
        assert_eq!(resolve_public_url(None, Some("  ")), None);
        assert_eq!(resolve_public_url(None, None), None);
    }
}
