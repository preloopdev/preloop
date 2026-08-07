//! GitHub credential setup, verification, and the local secret store.
//!
//! `preloop setup github` configures the engine's GitHub App or fine-grained
//! PAT credential in `~/.preloop/config.toml`; `preloop doctor` verifies the
//! credential against GitHub; `preloop secret` manages the local secret
//! store the server injects into trusted jobs.

use crate::preloop_home;
use preloop_runner_server::config::{load_config, store_memory, write_config};
use anyhow::Context;
use clap::{Parser, Subcommand};
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

    /// PAT to store (with --via pat). Falls back to PRELOOP_GITHUB_PAT,
    /// then an interactive prompt.
    #[arg(long)]
    pub token: Option<String>,

    /// Repository to verify the credential against (repeatable).
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

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
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

async fn cmd_setup_github(args: GithubSetupArgs) -> anyhow::Result<()> {
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
                 Run `preloop setup github --via app --app-id <ID> --pem-file <KEY>` after\n\
                 creating the App:\n\
                 \x20 1. github.com/settings/apps/new — name it (e.g. `dummy-preloop-app`),\n\
                 \x20    disable webhook, permissions: Contents: Read-only and\n\
                 \x20    Pull requests: Read-only (if your workflows read PRs).\n\
                 \x20 2. Generate a private key — save the PEM.\n\
                 \x20 3. Install the App on your repositories:\n\
                 \x20    https://github.com/apps/<slug>/installations/new\n\
                 \x20 4. Re-run this command with --app-id and --pem-file."
            );
            return Ok(());
        }
    };
    match via {
        Via::App => setup_app(&args).await,
        Via::Pat => setup_pat(&args).await,
    }
}

async fn setup_app(args: &GithubSetupArgs) -> anyhow::Result<()> {
    let app_id = args
        .app_id
        .as_deref()
        .context("--app-id is required with --via app")?;
    let pem_path = args
        .pem_file
        .as_deref()
        .context("--pem-file is required with --via app")?;
    let pem = std::fs::read_to_string(pem_path)
        .with_context(|| format!("reading App private key {}", pem_path.display()))?;

    let mut config = preloop_runner_server::config::load_config()?;
    config.github.app_id = Some(app_id.to_owned());
    config.github.app_pem = Some(pem.clone());
    if config.github.mint_failure.is_none() {
        config.github.mint_failure = Some("local".into());
    }
    let path = write_config(&config)?;
    println!("Wrote {}", path.display());
    println!(
        "App {app_id} configured. If the App is not yet installed on your repositories:\n\
         \x20 https://github.com/apps/<slug>/installations/new\n\
         Then restart the engine (`preloop serve`) to pick up the config."
    );
    if args.repos.is_empty() {
        println!("Tip: verify with `preloop doctor --repo owner/name`.");
        return Ok(());
    }
    doctor_app(app_id, &pem, &args.repos).await
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

    let mut config = preloop_runner_server::config::load_config()?;
    config.github.pat = Some(token.clone());
    config.github.mint_failure = Some("pat".into());
    let path = write_config(&config)?;
    println!("Wrote {}", path.display());
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
            return Ok((Vec::new(), false))
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
/// `contents: read`.
fn scalar_permission_lines(access: &str) -> Vec<String> {
    let level = match access {
        "read-all" => "read",
        "write-all" => "write",
        _ => return Vec::new(),
    };
    preloop_gha_parser::PERMISSION_SCOPES
        .iter()
        .map(|scope| format!("{scope}: {level}"))
        .collect()
}

async fn doctor_app(app_id: &str, pem: &str, repos: &[String]) -> anyhow::Result<()> {
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

async fn doctor_pat(token: &str, repos: &[String]) -> anyhow::Result<()> {
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
            "no config file at {} — run `preloop setup github` first",
            path.display()
        );
    }
    println!("config: {}", path.display());
    let config = preloop_runner_server::config::load_config()?;
    let mut failed = false;

    match (&config.github.app_id, &config.github.app_pem) {
        (Some(app_id), Some(pem)) => {
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
        _ => {
            println!("github app: not configured (jobs get no GitHub authority)");
        }
    }
    if let Some(pat) = &config.github.pat {
        println!(
            "github pat: configured (mint failure = {})",
            config.github.mint_failure.as_deref().unwrap_or("local")
        );
        if !args.repos.is_empty() {
            if let Err(error) = doctor_pat(pat, &args.repos).await {
                println!("{error:#}");
                failed = true;
            }
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
enum ApiOutcome<T> {
    Applied(T),
    Unavailable,
}

fn valid_secret_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn valid_repo_scope(repo: &str) -> bool {
    repo.split_once('/')
        .is_some_and(|(owner, name)| !owner.is_empty() && !name.is_empty())
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
            return Ok(ApiOutcome::Unavailable)
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
async fn api_list_secrets(
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
            if let Some(repo) = &repo {
                if !valid_repo_scope(repo) {
                    anyhow::bail!("--repo must look like owner/repo");
                }
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
            if let Some(repo) = &repo {
                if !valid_repo_scope(repo) {
                    anyhow::bail!("--repo must look like owner/repo");
                }
            }
            if env.is_some() && repo.is_none() {
                anyhow::bail!("--env requires --repo (owner/repo)");
            }
            if let Some(env) = &env {
                if !valid_env_scope(env) {
                    anyhow::bail!(
                        "--env must be letters, digits, hyphens, underscores (max 255, not starting with `-` or `_`)"
                    );
                }
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
            if let Some(repo) = &repo {
                if !valid_repo_scope(repo) {
                    anyhow::bail!("--repo must look like owner/repo");
                }
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
}
