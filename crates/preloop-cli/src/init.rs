//! `preloop init` — one command that configures credentials, the golden every
//! job forks from, and how the engine should run.
//!
//! Humans get a five-step wizard (only when stdin *and* stdout are TTYs);
//! agents get the same steps as flags plus `--json`, and
//! `preloop init --probe --json` reports host capabilities without touching
//! anything. Both paths resolve the same answers and write the same config:
//! the `[github]` section through the existing `preloop setup` code, and the
//! golden choice into the `[golden]` section of `$PRELOOP_CONFIG`
//! (`$PRELOOP_HOME/config.toml`). `serve` and `server install` read that file
//! through [`preloop_runner_server::config::golden_base_image`], where
//! `PRELOOP_RUNNER_BASE_IMAGE` still wins — there is no second config
//! mechanism.
//!
//! Golden work is deliberately *recorded*, not performed: the packed official
//! golden is downloaded by the engine, and a custom base is baked by the pool
//! on the next `serve`, into the same artifact path the engine already uses.
//! The one exception is the Dockerfile kind, whose `docker build` + `docker
//! save` produce a tar that *is* the base image — there is no engine path that
//! would build it later, so `init` builds it and records the tar.

use crate::github_setup::{self, GithubSetupArgs, Via};
use crate::preloop_home;
use anyhow::Context;
use clap::{Parser, ValueEnum};
use preloop_runner_server::config::{ConfigFile, GoldenConfig, load_config, write_config};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod workflows;

/// Missing or invalid flag in a non-interactive run.
pub(crate) const EXIT_USAGE: i32 = 2;
/// A preflight check failed: the host cannot run the chosen golden.
pub(crate) const EXIT_PREFLIGHT: i32 = 3;
/// Fetching, building, or resolving the chosen base image failed.
pub(crate) const EXIT_BUILD: i32 = 4;

/// A failure that carries the exit code `preloop init` promised scripts.
/// `main` prints it and exits with that code; every other error keeps the
/// process's usual exit 1.
#[derive(Debug)]
pub(crate) struct InitError {
    code: i32,
    message: String,
    /// Machine-readable detail for `--json`, when the failure has one: the
    /// preflight shortfalls, so a script can see *what* is short instead of
    /// parsing prose.
    detail: Option<Value>,
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InitError {}

fn usage_error(message: impl Into<String>) -> anyhow::Error {
    InitError {
        code: EXIT_USAGE,
        message: message.into(),
        detail: None,
    }
    .into()
}

fn preflight_error(message: impl Into<String>, shortfalls: Value) -> anyhow::Error {
    InitError {
        code: EXIT_PREFLIGHT,
        message: message.into(),
        detail: Some(shortfalls),
    }
    .into()
}

fn build_error(message: impl Into<String>) -> anyhow::Error {
    InitError {
        code: EXIT_BUILD,
        message: message.into(),
        detail: None,
    }
    .into()
}

/// The preflight shortfalls a coded failure carries, for `--json`.
fn preflight_shortfall_json(error: &anyhow::Error) -> Value {
    error
        .downcast_ref::<InitError>()
        .and_then(|error| error.detail.clone())
        .unwrap_or_else(|| json!([]))
}

/// The exit code this failure requires, when it requires a specific one.
pub(crate) fn exit_code(error: &anyhow::Error) -> Option<i32> {
    error.downcast_ref::<InitError>().map(|error| error.code)
}

use preloop_orchestrator::environment::OFFICIAL_GOLDEN;
use preloop_orchestrator::oci::{
    MANIFEST_ACCEPT, OciError, OciReference, get_manifest, host_arch_aliases, platform_matches_host,
};
use preloop_orchestrator::{
    DISK_PREFLIGHT_OVERRIDE, GIB, GOLDEN_BUILD_DISK_HEADROOM_GIB, GOLDEN_DOWNLOAD_DISK_MARGIN,
    disk_preflight_overridden, golden_builder_storage_gib,
};

/// Compressed packed-official-golden transfer, the size the engine's own
/// download budget and docs quote (~9.6 GB).
const OFFICIAL_GOLDEN_DOWNLOAD_BYTES: u64 = 9 * GIB + (6 * GIB) / 10;
/// Working set the official golden settles into: the download, the unpacked
/// golden, and per-job VM space. Quoted everywhere the official image is
/// offered; a shortfall warns rather than refusing, exactly like the engine's
/// unpack warning.
const OFFICIAL_GOLDEN_WORKING_SET_BYTES: u64 = 60 * GIB;
/// smolvm's own ceiling for a local image archive passed to `machine create
/// --image` (its `--max-image-size` help text: default 8GiB). A base image over
/// this fails at serve time with a smolvm error, so `init` says so up front.
const SMOLVM_MAX_LOCAL_IMAGE_BYTES: u64 = 8 * GIB;

#[derive(Debug, Default, Parser)]
pub(crate) struct InitArgs {
    /// Print a capability report as JSON and exit (no side effects).
    #[arg(long)]
    pub probe: bool,

    /// Credential step: `app`, `pat`, or `none` (local only).
    #[arg(long, value_enum)]
    pub auth: Option<AuthChoice>,

    /// GitHub App id (with `--auth app`).
    #[arg(long, value_name = "ID")]
    pub app_id: Option<String>,

    /// Path to the GitHub App private key PEM (with `--auth app`).
    #[arg(long, value_name = "PATH")]
    pub pem_file: Option<PathBuf>,

    /// PAT to store (with `--auth pat`). Falls back to PRELOOP_GITHUB_PAT.
    #[arg(long, value_name = "TOKEN")]
    pub token: Option<String>,

    /// Webhook secret to store alongside the credential.
    #[arg(long, value_name = "SECRET")]
    pub webhook_secret: Option<String>,

    /// Create the App under this organization (with `--auth app`, browser flow).
    #[arg(long, value_name = "NAME")]
    pub org: Option<String>,

    /// Public HTTPS URL GitHub reaches this engine at (with --auth app): enables webhooks and PR checks. Must be https and not loopback.
    #[arg(long, value_name = "URL")]
    pub public_url: Option<String>,

    /// Skip the secrets scan of the repositories' workflows. The runner-label
    /// report still runs: it reads the same files and asks nothing.
    #[arg(long)]
    pub no_secrets_scan: bool,

    /// Port for the loopback listener GitHub redirects back to (0 = free port).
    #[arg(long, default_value_t = 0)]
    pub port: u16,

    /// Print the App-creation URL instead of opening a browser.
    #[arg(long)]
    pub no_browser: bool,

    /// Repository to verify the credential against (repeatable).
    #[arg(long = "repo", value_name = "OWNER/NAME")]
    pub repos: Vec<String>,

    /// Golden step: `official`, `oci`, `dockerfile`, or `file`.
    #[arg(long, value_enum)]
    pub golden: Option<GoldenChoice>,

    /// OCI base image (with `--golden oci`).
    #[arg(long = "base-image", value_name = "REF")]
    pub base_image: Option<String>,

    /// Dockerfile to build (with `--golden dockerfile`; default ./Dockerfile).
    #[arg(long, value_name = "PATH")]
    pub dockerfile: Option<PathBuf>,

    /// Build context (with `--golden dockerfile`; default: the Dockerfile's dir).
    #[arg(long = "docker-context", value_name = "DIR")]
    pub docker_context: Option<PathBuf>,

    /// Local `.smolmachine` pack or rootfs directory (with `--golden file`).
    #[arg(long, value_name = "PATH")]
    pub path: Option<PathBuf>,

    /// Run mode: `foreground`, `service`, or `none`.
    #[arg(long, value_enum)]
    pub mode: Option<RunMode>,

    /// Assume the suggested answer for confirmation prompts.
    #[arg(long)]
    pub yes: bool,

    /// One JSON object per step, plus a final object naming the written config.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum AuthChoice {
    /// GitHub App: per-job tokens scoped to each workflow's `permissions:`.
    App,
    /// Fine-grained PAT, for orgs that gate App installations.
    Pat,
    /// Local only: write no credential.
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum GoldenChoice {
    /// The packed official GitHub runner golden the engine downloads.
    Official,
    /// Any registry reference (Docker Hub, GHCR, private).
    Oci,
    /// A Dockerfile in this repo, built and saved as a local tar.
    Dockerfile,
    /// A local `.smolmachine` pack or rootfs directory.
    File,
}

impl GoldenChoice {
    fn as_str(self) -> &'static str {
        match self {
            Self::Official => "official",
            Self::Oci => "oci",
            Self::Dockerfile => "dockerfile",
            Self::File => "file",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "official" => Some(Self::Official),
            "oci" => Some(Self::Oci),
            "dockerfile" => Some(Self::Dockerfile),
            "file" => Some(Self::File),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum RunMode {
    /// Start `preloop serve` in this terminal.
    Foreground,
    /// Install the control plane as a supervised service.
    Service,
    /// Write the config and start nothing.
    #[value(name = "none")]
    Configure,
}

impl RunMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Service => "service",
            Self::Configure => "none",
        }
    }
}

/// `preloop init`, entered from the CLI.
pub(crate) async fn run(
    args: InitArgs,
    observability: Option<preloop_observability::Observability>,
) -> anyhow::Result<()> {
    if args.probe {
        return print_probe().await;
    }
    // The wizard owns the terminal; `--json` output is one object per line, so
    // it is never mixed with prompts.
    let interactive =
        std::io::stdin().is_terminal() && std::io::stdout().is_terminal() && !args.json;
    let json = args.json;
    let session = Session {
        interactive,
        json,
        yes: args.yes,
        from_serve: false,
    };
    match session.execute(args, observability, false).await {
        // The resolved URL matters to the `serve` caller, which is already
        // running the engine the wizard configured; an `init` run starts its
        // mode itself and has nothing left to hand it to.
        Ok(_) => Ok(()),
        Err(error) => {
            // A coded failure is a *step* failing, so `--json` reports it as
            // one more object before the exit code carries it to the caller.
            if json {
                let code = exit_code(&error).unwrap_or(1);
                let shortfalls = preflight_shortfall_json(&error);
                println!(
                    "{}",
                    json!({
                        "step": "error",
                        "decision": "failed",
                        "exit_code": code,
                        "error": format!("{error}"),
                        "shortfalls": shortfalls,
                    })
                );
            }
            Err(error)
        }
    }
}

/// Run the wizard from `serve` when no golden is configured yet. The mode step
/// is fixed to "configure only": this process already *is* the foreground
/// engine, and a nested `serve` would fight it for the socket.
///
/// Returns the public URL the wizard resolved (the one the App's webhook was
/// pointed at); `None` when the wizard left the origin to `serve`'s own
/// resolution. The caller hands it to the engine it is already running — see
/// [`apply_engine_public_url`].
pub(crate) async fn run_from_serve() -> anyhow::Result<Option<String>> {
    let args = InitArgs {
        mode: Some(RunMode::Configure),
        ..InitArgs::default()
    };
    let session = Session {
        interactive: true,
        json: false,
        yes: false,
        from_serve: true,
    };
    session.execute(args, None, true).await
}

struct Session {
    /// Prompts are only safe when a human is on both ends.
    interactive: bool,
    json: bool,
    yes: bool,
    /// Entered from `serve`, which is already running this config: the
    /// "start `serve` on its next run" messages would be wrong.
    from_serve: bool,
}

/// The answers the five steps produce, before any of them is applied.
#[derive(Debug)]
struct Answers {
    auth: AuthChoice,
    /// Step 1 kept the stored credential: there is nothing to configure again.
    auth_unchanged: bool,
    golden: GoldenAnswer,
    /// Step 2 kept the stored golden: nothing to verify or build again.
    golden_unchanged: bool,
    mode: RunMode,
}

#[derive(Debug)]
struct GoldenAnswer {
    kind: GoldenChoice,
    /// Registry reference, for `--golden oci`.
    base_image: Option<String>,
    /// Dockerfile and build context, for `--golden dockerfile`.
    dockerfile: Option<PathBuf>,
    docker_context: Option<PathBuf>,
    /// Local pack or rootfs directory, for `--golden file`.
    path: Option<PathBuf>,
}

/// The golden answer after any work it required (an OCI reference resolved, a
/// Dockerfile built and saved).
struct GoldenDecision {
    kind: GoldenChoice,
    /// Persisted base image; `None` = nothing configured, i.e. the packed
    /// official golden the engine downloads and verifies itself.
    base_image: Option<String>,
    dockerfile: Option<PathBuf>,
    label: String,
    /// Bytes this choice places on the data volume (the download, the pulled
    /// image, the saved tar). `None` when there is nothing to place.
    artifact_bytes: Option<u64>,
    /// `Some(false)` when the image provably does not support this host's
    /// architecture.
    arch_ok: Option<bool>,
    /// Non-fatal findings about the chosen artifact, reported in the step.
    warnings: Vec<String>,
    detail: Value,
}

/// A local image archive over smolvm's default ceiling: it would be refused at
/// serve time unless the operator raises the limit.
fn oversized_archive_warning(bytes: u64, what: &str) -> Option<String> {
    (bytes > SMOLVM_MAX_LOCAL_IMAGE_BYTES).then(|| {
        format!(
            "{what} is {} — over smolvm's {} default limit for a local image archive; raise it              with SMOLVM_MAX_IMAGE_BYTES before `serve`, or use a registry reference instead",
            gib(bytes),
            gib(SMOLVM_MAX_LOCAL_IMAGE_BYTES),
        )
    })
}

/// The base image the engine will actually fork jobs from, in the engine's own
/// precedence (`preloop_runner_server::config::golden_base_image`): a non-blank
/// `PRELOOP_RUNNER_BASE_IMAGE` override, else the `[golden]` section, else the
/// packed official golden. The resolver itself cannot be called here — the
/// config file still holds the pre-wizard golden until the run ends — so
/// `chosen` stands in for the `[golden]` section.
fn effective_base_image(chosen: Option<&str>) -> String {
    if let Some(override_image) = base_image_override() {
        return override_image;
    }
    chosen
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(OFFICIAL_GOLDEN)
        .to_owned()
}

/// A non-blank `PRELOOP_RUNNER_BASE_IMAGE` this process carries, if any: the
/// first source of the engine's own golden precedence, and the value the
/// installer must render into a service's environment file (the service does
/// not inherit this shell).
fn base_image_override() -> Option<String> {
    std::env::var(preloop_runner_server::config::BASE_IMAGE_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// Escape control characters in human-report lines — `runs-on` labels, file
/// paths, parser messages — as visible `\x1b`-style text: a crafted workflow
/// must not be able to move the cursor, retitle the terminal, or inject an
/// ANSI sequence into `init`'s output. Mirrors `sanitize_console_text` in
/// `preloop-dap`, except the bytes are shown rather than dropped, so the
/// reader still sees what the workflow said. C0 controls, DEL, and C1
/// (ESC included) are all escaped.
fn escape_control(text: &str) -> String {
    if !text.chars().any(char::is_control) {
        return text.to_owned();
    }
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        if character.is_control() {
            let code = character as u32;
            if code <= 0xff {
                escaped.push_str(&format!("\\x{code:02x}"));
            } else {
                escaped.push_str(&format!("\\u{{{code:x}}}"));
            }
        } else {
            escaped.push(character);
        }
    }
    escaped
}

/// The scope label a successful write leaves `name` at, named by the same
/// [`workflows::Stored`] view the report looks names up in, so the two cannot
/// drift.
fn stored_scope(name: &str, repo: Option<&str>) -> Option<String> {
    workflows::Stored::from_listing(&[(name.to_owned(), repo.map(str::to_owned), None)])
        .scope_of(name, repo)
}

/// Apply one successful interactive `secret set` to the rows it satisfies: a
/// row that read `name` for this target is now configured at the scope the
/// write used. Returns whether any row changed.
fn mark_stored(rows: &mut [workflows::SecretRow], name: &str, repo: Option<&str>) -> bool {
    let scope = stored_scope(name, repo);
    let mut changed = false;
    for row in rows
        .iter_mut()
        .filter(|row| row.name == name && row.scope.is_none())
    {
        row.scope = scope.clone();
        row.stored_as = None;
        changed = true;
    }
    changed
}

impl Session {
    fn section(&self, title: &str) {
        if !self.json {
            println!("\n{}", escape_control(title));
        }
    }

    /// Emit one step object in `--json` mode. Every other mode has already
    /// reported the same decision in prose.
    fn emit(&self, value: Value) {
        if self.json {
            println!("{value}");
        }
    }

    fn note(&self, line: &str) {
        if !self.json {
            println!("    {}", escape_control(line));
        }
    }

    /// Run every step and apply the answers. Returns the public URL the run
    /// resolved for the engine it configured (the App flow's answer, or the
    /// flag/environment when the credential was kept): `serve` offering the
    /// wizard is already running that engine, so it needs the answer back to
    /// install it in-process.
    async fn execute(
        mut self,
        args: InitArgs,
        observability: Option<preloop_observability::Observability>,
        from_serve: bool,
    ) -> anyhow::Result<Option<String>> {
        let config = load_config().context("reading the engine config")?;
        let answers = self.resolve(&args, &config, from_serve)?;

        // A system service reads its own state dir, so when the operator did
        // not pin one, the credentials and the golden must be written there or
        // the installed service would start with neither.
        if answers.mode == RunMode::Service {
            self.prepare_service_home()?;
        }

        self.section("Applying your answers:");
        // The URL the App flow resolved is also the engine's own public origin:
        // Check Run detail links and the webhook drift check read
        // `PRELOOP_PUBLIC_URL`, and the run mode that starts is the only thing
        // that can carry the answer there (for the service, its environment
        // file is written now — it does not inherit this shell).
        let public_url = if answers.auth_unchanged {
            self.report_stored_credentials(&config, &args).await?;
            engine_public_url(&args, None)
        } else {
            let answer = self.configure_credentials(&args, answers.auth).await?;
            engine_public_url(&args, answer.as_ref())
        };
        let decision = if answers.golden_unchanged {
            let decision = decision_from_config(&config, &answers.golden).ok_or_else(|| {
                anyhow::anyhow!(
                    "internal: no stored golden to keep — re-run `preloop init` and pick one"
                )
            })?;
            self.section(&format!("2/5 Golden: {} (kept)", decision.kind.as_str()));
            self.note(&format!("kept: {}", decision.label));
            self.emit(json!({
                "step": "golden",
                "decision": decision.kind.as_str(),
                "result": {"unchanged": true, "base_image": decision.base_image},
            }));
            decision
        } else {
            self.configure_golden(&answers.golden).await?
        };
        self.workflow_step(&args, &decision).await?;
        self.section(&format!(
            "4/5 Run mode: {}",
            describe_mode(answers.mode, &decision.label)
        ));
        let preflight = self.preflight(&decision, answers.mode)?;

        // Re-read: the credentials step rewrote the file (or created it).
        let mut config = load_config().context("re-reading the engine config")?;
        config.golden = GoldenConfig {
            kind: Some(decision.kind.as_str().to_owned()),
            base_image: decision.base_image.clone(),
            dockerfile: decision.dockerfile.clone(),
        };
        let written = write_config(&config).context("writing the engine config")?;
        if !self.json {
            println!("\nwrote {}", written.display());
        }
        // `PRELOOP_RUNNER_BASE_IMAGE` is the first source of the engine's own
        // golden precedence, so a non-blank override is what the mode this run
        // starts forks from — and what an installed service will fork from,
        // because the installer renders this process's environment into the
        // service's environment file. Saying only the stored choice would
        // describe a golden the engine does not use.
        let env_override = base_image_override();
        let effective_golden = effective_base_image(decision.base_image.as_deref());
        if let Some(image) = &env_override {
            // `note` escapes control characters, so a crafted environment
            // value cannot move the cursor in the report.
            self.note(&format!(
                "golden override: PRELOOP_RUNNER_BASE_IMAGE={image} wins over the choice above — \
                 the engine (and any service installed from this shell) forks from it"
            ));
        }
        self.emit(json!({
            "step": "done",
            "decision": answers.mode.as_str(),
            "config_path": written,
            // What the run mode this run starts is handed. It is not written
            // to the config file: `serve` reads PRELOOP_PUBLIC_URL (or
            // --public-url), and the installer renders it into the service's
            // own environment file.
            "public_url": public_url.as_deref(),
            "golden": {
                "kind": decision.kind.as_str(),
                "base_image": decision.base_image,
                "label": decision.label,
                // Not from the config file either: the installer renders the
                // override into the service's environment file for the same
                // reason it renders the public URL.
                "env_override": env_override,
                "effective_base_image": effective_golden,
            },
            "preflight": preflight,
        }));
        self.start_mode(answers.mode, observability, &written, public_url.as_deref())
            .await?;
        Ok(public_url)
    }

    // -- step 0: answers -------------------------------------------------

    fn resolve(
        &mut self,
        args: &InitArgs,
        config: &ConfigFile,
        from_serve: bool,
    ) -> anyhow::Result<Answers> {
        if !self.interactive {
            return self.resolve_from_flags(args);
        }
        if !self.json {
            println!(
                "preloop init — configuring {}\n\
                 Five steps; press Enter to keep the current answer.",
                preloop_runner_server::config::config_path().display()
            );
        }
        let (auth, auth_unchanged) = self.ask_auth(args, config)?;
        let (golden, golden_unchanged) = self.ask_golden(args, config)?;
        let mode = if from_serve {
            self.section("4/5 Run mode: none (configure only — this engine keeps running)");
            RunMode::Configure
        } else {
            self.ask_mode(args)?
        };
        Ok(Answers {
            auth,
            auth_unchanged,
            golden,
            golden_unchanged,
            mode,
        })
    }

    fn resolve_from_flags(&self, args: &InitArgs) -> anyhow::Result<Answers> {
        let mut missing: Vec<&str> = Vec::new();
        if args.auth.is_none() {
            missing.push("--auth");
        }
        if args.golden.is_none() {
            missing.push("--golden");
        }
        if args.mode.is_none() {
            missing.push("--mode");
        }
        if !missing.is_empty() {
            return Err(usage_error(format!(
                "non-interactive `preloop init` needs {} — stdin and stdout are not both TTYs, \
                 so there is no wizard to answer. Run it in a terminal, or pass the flags and \
                 add --json for machine-readable steps (`preloop init --help` lists them).",
                missing.join(", ")
            )));
        }
        let kind = args.golden.expect("checked above");
        reject_mismatched_flags(kind, args)?;
        let golden = match kind {
            GoldenChoice::Oci => GoldenAnswer {
                base_image: Some(
                    args.base_image
                        .clone()
                        .ok_or_else(|| usage_error("--golden oci needs --base-image <ref>"))?,
                ),
                ..GoldenAnswer::of(kind)
            },
            GoldenChoice::File => GoldenAnswer {
                path: Some(
                    args.path
                        .clone()
                        .ok_or_else(|| usage_error("--golden file needs --path <file-or-dir>"))?,
                ),
                ..GoldenAnswer::of(kind)
            },
            GoldenChoice::Dockerfile => GoldenAnswer {
                dockerfile: Some(args.dockerfile.clone().unwrap_or_else(default_dockerfile)),
                docker_context: args.docker_context.clone(),
                ..GoldenAnswer::of(kind)
            },
            GoldenChoice::Official => GoldenAnswer::of(kind),
        };
        Ok(Answers {
            auth: args.auth.expect("checked above"),
            auth_unchanged: false,
            golden,
            golden_unchanged: false,
            mode: args.mode.expect("checked above"),
        })
    }

    /// Returns the step-1 answer and whether it is the stored one.
    fn ask_auth(
        &mut self,
        args: &InitArgs,
        config: &ConfigFile,
    ) -> anyhow::Result<(AuthChoice, bool)> {
        self.section("1/5 Credentials");
        if let Some(auth) = args.auth {
            self.note(&format!("using --auth {}", describe_auth(auth)));
            return Ok((auth, false));
        }
        if let Some((prior, summary)) = credential_prior(config) {
            self.note(&format!("current: {summary}"));
            if !self.ask_bool("    change? [y/N] ", false)? {
                return Ok((prior, true));
            }
        }
        let items = [
            MenuItem::enabled(
                "GitHub App",
                "recommended — tokens are minted per job and scoped to each workflow's permissions",
            ),
            MenuItem::enabled(
                "fine-grained PAT",
                "for orgs that gate App installations; check runs stay local",
            ),
            MenuItem::enabled("none", "local only — jobs get local tokens"),
        ];
        Ok((
            match self.ask_choice(&items, 0)? {
                0 => AuthChoice::App,
                1 => AuthChoice::Pat,
                _ => AuthChoice::None,
            },
            false,
        ))
    }

    /// Returns the step-2 answer and whether it is the stored one.
    fn ask_golden(
        &mut self,
        args: &InitArgs,
        config: &ConfigFile,
    ) -> anyhow::Result<(GoldenAnswer, bool)> {
        self.section("2/5 Golden — the VM image every job forks from");
        if let Some(kind) = args.golden {
            self.note(&format!("using --golden {}", kind.as_str()));
            reject_mismatched_flags(kind, args)?;
            return match kind {
                GoldenChoice::Oci => Ok((
                    GoldenAnswer {
                        base_image: Some(require_interactive_flag(
                            args.base_image.clone(),
                            "--base-image <ref>",
                            &mut |prompt| self.read_line(prompt),
                        )?),
                        ..GoldenAnswer::of(kind)
                    },
                    false,
                )),
                GoldenChoice::File => Ok((
                    GoldenAnswer {
                        path: Some(PathBuf::from(require_interactive_flag(
                            args.path.clone().map(|path| path.display().to_string()),
                            "--path <file-or-dir>",
                            &mut |prompt| self.read_line(prompt),
                        )?)),
                        ..GoldenAnswer::of(kind)
                    },
                    false,
                )),
                GoldenChoice::Dockerfile => Ok((
                    GoldenAnswer {
                        dockerfile: Some(
                            args.dockerfile.clone().unwrap_or_else(default_dockerfile),
                        ),
                        docker_context: args.docker_context.clone(),
                        ..GoldenAnswer::of(kind)
                    },
                    false,
                )),
                GoldenChoice::Official => Ok((GoldenAnswer::of(kind), false)),
            };
        }
        if let Some((kind, configured)) = golden_prior(config) {
            self.note(&format!("current: {configured}"));
            if !self.ask_bool("    change? [y/N] ", false)? {
                return Ok((
                    GoldenAnswer {
                        base_image: config.golden.base_image.clone(),
                        dockerfile: config.golden.dockerfile.clone(),
                        ..GoldenAnswer::of(kind)
                    },
                    true,
                ));
            }
        }
        let dockerfile = args.dockerfile.clone().unwrap_or_else(default_dockerfile);
        let items = [
            MenuItem::enabled(
                "Official GitHub image",
                &format!(
                    "drop-in parity with GitHub-hosted runners; ~{} on disk \
                     (≈{} download + the unpacked golden + per-job VM space)",
                    gib(OFFICIAL_GOLDEN_WORKING_SET_BYTES),
                    gib(OFFICIAL_GOLDEN_DOWNLOAD_BYTES),
                ),
            ),
            MenuItem::enabled(
                "OCI image reference",
                "any registry — Docker Hub, GHCR, private; pin @sha256: for reproducibility",
            ),
            MenuItem {
                label: "Dockerfile in this repo".to_owned(),
                description: "build it here and use the saved tar as the base image".to_owned(),
                disabled: dockerfile_unavailable_reason(&dockerfile),
            },
            MenuItem::enabled(
                "Local .smolmachine / rootfs dir",
                "a pack or rootfs directory you already have",
            ),
        ];
        match self.ask_choice(&items, 0)? {
            0 => Ok((GoldenAnswer::of(GoldenChoice::Official), false)),
            1 => {
                let reference = loop {
                    let reference =
                        self.read_line("    image reference (registry/repo:tag or @sha256:…): ")?;
                    if reference.is_empty() {
                        self.note("a registry reference is required");
                        continue;
                    }
                    break reference;
                };
                Ok((
                    GoldenAnswer {
                        base_image: Some(reference),
                        ..GoldenAnswer::of(GoldenChoice::Oci)
                    },
                    false,
                ))
            }
            2 => {
                let dockerfile = if args.dockerfile.is_some() {
                    args.dockerfile.clone().unwrap_or_else(default_dockerfile)
                } else {
                    default_dockerfile()
                };
                let context = match args.docker_context.clone() {
                    Some(context) => context,
                    None => {
                        let hint = dockerfile_context(&dockerfile);
                        let answer =
                            self.read_line(&format!("    build context [{}]: ", hint.display()))?;
                        if answer.is_empty() {
                            hint
                        } else {
                            PathBuf::from(answer)
                        }
                    }
                };
                Ok((
                    GoldenAnswer {
                        dockerfile: Some(dockerfile),
                        docker_context: Some(context),
                        ..GoldenAnswer::of(GoldenChoice::Dockerfile)
                    },
                    false,
                ))
            }
            _ => {
                let path = loop {
                    let answer =
                        self.read_line("    path to the .smolmachine pack or rootfs dir: ")?;
                    if answer.is_empty() {
                        self.note("a path is required");
                        continue;
                    }
                    break PathBuf::from(answer);
                };
                Ok((
                    GoldenAnswer {
                        path: Some(path),
                        ..GoldenAnswer::of(GoldenChoice::File)
                    },
                    false,
                ))
            }
        }
    }

    fn ask_mode(&mut self, args: &InitArgs) -> anyhow::Result<RunMode> {
        self.section("4/5 Run mode — not persisted, chosen each run");
        if let Some(mode) = args.mode {
            self.note(&format!("using --mode {}", mode.as_str()));
            return Ok(mode);
        }
        let items = [
            MenuItem::enabled("foreground now", "`preloop serve` in this terminal"),
            MenuItem::enabled(
                "install as a service",
                "systemd (Linux) / launchd (macOS); needs root",
            ),
            MenuItem::enabled("configure only", "write the config and start nothing"),
        ];
        Ok(match self.ask_choice(&items, 0)? {
            0 => RunMode::Foreground,
            1 => RunMode::Service,
            _ => RunMode::Configure,
        })
    }

    // -- step 1: credentials ---------------------------------------------

    /// Step 1 on a re-run that kept the stored credential: nothing is
    /// configured again (a PAT keep must not re-prompt for the token, and an
    /// App keep must not re-open the browser flow). The live check still runs
    /// when `--repo` was passed explicitly, because a rotated or expired
    /// credential breaks jobs silently and costs one API call to notice.
    async fn report_stored_credentials(
        &mut self,
        config: &ConfigFile,
        args: &InitArgs,
    ) -> anyhow::Result<()> {
        self.section("1/5 Credentials: (kept)");
        let summary = credential_summary(config).unwrap_or_else(|| "none — local only".to_owned());
        self.note(&format!("kept: {summary}"));
        if args.public_url.is_some() {
            self.note(
                "the kept credential is not reconfigured, but the engine this run starts will \
                 use --public-url; to point the kept App's webhook at it: \
                 preloop setup github --via app --public-url <URL>",
            );
        }
        let verify = if args.repos.is_empty() {
            json!({"skipped": "no --repo"})
        } else {
            self.verify_credentials(config, args).await?
        };
        self.emit(json!({
            "step": "credentials",
            "decision": "unchanged",
            "result": {"summary": summary, "verify": verify},
        }));
        Ok(())
    }

    /// The stable URL the App flow resolved, when it asked one: the run mode
    /// this run starts needs it too (see [`engine_public_url`]).
    async fn configure_credentials(
        &mut self,
        args: &InitArgs,
        auth: AuthChoice,
    ) -> anyhow::Result<Option<PublicUrlAnswer>> {
        self.section("1/5 Credentials:");
        match auth {
            AuthChoice::None => {
                self.note(
                    "local only — no GitHub credential written; existing ones are left alone",
                );
                self.emit(json!({"step": "credentials", "decision": "none", "result": {}}));
                return Ok(None);
            }
            AuthChoice::App => self.note("GitHub App"),
            AuthChoice::Pat => self.note("fine-grained PAT"),
        }
        // The stable URL is asked only for an App: a PAT has no webhook to feed.
        let stable_url = match auth {
            AuthChoice::App => Some(self.public_url_answer(
                args.public_url.as_deref(),
                std::env::var("PRELOOP_PUBLIC_URL").ok().as_deref(),
            )?),
            _ => None,
        };
        let public_url = match &stable_url {
            Some(answer) => answer.url.clone(),
            None => args
                .public_url
                .as_deref()
                .and_then(|raw| validate_public_url(raw).ok()),
        };
        if !self.interactive {
            // The browser manifest flow waits for a redirect GitHub sends to a
            // local listener, and the PAT prompt reads a line from stdin:
            // neither can finish without a human, so a non-interactive run
            // must be given the credential up front instead of hanging.
            match auth {
                AuthChoice::App if args.app_id.is_none() || args.pem_file.is_none() => {
                    return Err(usage_error(
                        "--auth app in a non-interactive shell needs both --app-id and \
                         --pem-file (the browser flow needs a terminal); or use --auth none",
                    ));
                }
                AuthChoice::Pat if args.token.is_none() && github_pat_env().is_none() => {
                    return Err(usage_error(
                        "--auth pat in a non-interactive shell needs --token (or \
                         PRELOOP_GITHUB_PAT)",
                    ));
                }
                _ => {}
            }
        }

        // The credential flow is `preloop setup github`, verbatim: same flags,
        // same stored config, same browser/PAT handling. No repos are passed,
        // so its own end-of-run doctor stays quiet and the live check below is
        // the one that reports.
        let setup_args = GithubSetupArgs {
            via: Some(match auth {
                AuthChoice::App => Via::App,
                _ => Via::Pat,
            }),
            app_id: args.app_id.clone(),
            pem_file: args.pem_file.clone(),
            add: false,
            org: args.org.clone(),
            port: args.port,
            public_url,
            app_name: "preloop-local".to_owned(),
            no_browser: args.no_browser,
            webhook_secret: args.webhook_secret.clone(),
            token: args.token.clone(),
            repos: Vec::new(),
            workspace: None,
        };
        // In `--json` the reused flow's human output would land between our
        // one-object-per-line steps, so it is captured and attached instead.
        let log = if self.json {
            let (result, captured) =
                capture_stdout(github_setup::cmd_setup_github(setup_args)).await;
            result?;
            captured
        } else {
            github_setup::cmd_setup_github(setup_args).await?;
            String::new()
        };
        let config = load_config().context("re-reading the engine config")?;
        let summary = credential_summary(&config).unwrap_or_else(|| "configured".to_owned());
        self.note(&format!("configured: {summary}"));
        let verified = self.verify_credentials(&config, args).await?;
        let mut result = json!({"summary": summary, "verify": verified});
        if let Some(answer) = &stable_url {
            result["public_url"] = json!({"url": answer.url, "source": answer.source});
        }
        self.emit(json!({
            "step": "credentials",
            "decision": match auth { AuthChoice::App => "app", _ => "pat" },
            "result": result,
            "log": log,
        }));
        Ok(stable_url)
    }

    /// Verify the stored credential live, the way `preloop doctor` does.
    ///
    /// A repository is required to probe anything: with none supplied, the git
    /// remote is offered in the wizard only, so an unattended run never makes
    /// a network call the caller did not ask for.
    async fn verify_credentials(
        &mut self,
        config: &ConfigFile,
        args: &InitArgs,
    ) -> anyhow::Result<Value> {
        let mut repos = args.repos.clone();
        if repos.is_empty() && self.interactive {
            let detected = crate::detect_repository();
            // "local/<dir>" is the fallback when there is no origin remote:
            // GitHub has never heard of that repository, so do not offer it.
            if !detected.starts_with("local/")
                && self.ask_bool(
                    &format!("    verify the credential against {detected}? [Y/n] "),
                    true,
                )?
            {
                repos.push(detected);
            }
        }
        if repos.is_empty() {
            self.note("credential check skipped (pass --repo owner/name to verify against GitHub)");
            return Ok(json!({"skipped": "no --repo and no detected remote"}));
        }
        let result = match (
            config.github.app_id.as_deref(),
            config.github.app_pem(),
            config.github.pat(),
        ) {
            (Some(app_id), Some(pem), _) => github_setup::doctor_app(app_id, pem, &repos).await,
            (_, _, Some(token)) => github_setup::doctor_pat(token, &repos).await,
            _ => {
                self.note("no credential is configured to verify");
                return Ok(json!({"skipped": "no credential"}));
            }
        };
        match result {
            Ok(()) => Ok(json!({"repos": repos, "ok": true})),
            Err(error) => {
                let message = format!("{error:#}");
                if !self.json {
                    println!("    credential check failed: {message}");
                }
                if self.yes || !self.interactive {
                    if self.yes {
                        self.note("continuing anyway (--yes)");
                        return Ok(json!({"repos": repos, "ok": false, "error": message}));
                    }
                    return Err(anyhow::anyhow!(
                        "credential check failed: {message}\n\
                         The credential is stored; re-run `preloop init` once GitHub can reach \
                         it, or pass --yes to configure anyway."
                    ));
                }
                if self.ask_bool("    continue anyway? [y/N] ", false)? {
                    Ok(json!({"repos": repos, "ok": false, "error": message}))
                } else {
                    Err(anyhow::anyhow!(
                        "credential check failed: {message}\n\
                         The credential is stored; fix it and re-run `preloop init`."
                    ))
                }
            }
        }
    }

    /// The stable-URL answer for an App flow: `--public-url`, else a usable
    /// `PRELOOP_PUBLIC_URL`, else one prompt on a terminal. The prompt takes a
    /// URL directly, a Funnel-published tailnet name on Enter, or `-` to skip
    /// — and an App created without a webhook never triggers on pushes or pull
    /// requests.
    fn public_url_answer(
        &mut self,
        flag: Option<&str>,
        env: Option<&str>,
    ) -> anyhow::Result<PublicUrlAnswer> {
        if let Some(raw) = flag {
            let url = validate_public_url(raw).map_err(usage_error)?;
            return Ok(PublicUrlAnswer {
                url: Some(url),
                source: "flag",
            });
        }
        if let Some(raw) = env.map(str::trim).filter(|raw| !raw.is_empty()) {
            match validate_public_url(raw) {
                Ok(url) => {
                    self.note(&format!("using PRELOOP_PUBLIC_URL={url}"));
                    return Ok(PublicUrlAnswer {
                        url: Some(url),
                        source: "env",
                    });
                }
                Err(reason) => {
                    self.note(&format!(
                        "PRELOOP_PUBLIC_URL is not used for webhooks: {reason}"
                    ));
                }
            }
        }
        if !self.interactive || self.yes {
            self.note_no_public_url();
            return Ok(PublicUrlAnswer {
                url: None,
                source: "skipped",
            });
        }
        // A Tailscale host already has a stable HTTPS name, so offer it when
        // Funnel publishes the webhook path there instead of making the
        // operator look one up. Detection is best effort: no tailscale, no
        // suggestion, and a tailnet-only name is shown as a hint Enter does not
        // accept.
        let suggestion = detected_public_url();
        for line in public_url_help(suggestion.as_ref()) {
            self.note(&line);
        }
        loop {
            let answer = self.read_line(&public_url_prompt(suggestion.as_ref()))?;
            match read_public_url(&answer, suggestion.as_ref()) {
                UrlInput::Use { url, source } => {
                    return Ok(PublicUrlAnswer {
                        url: Some(url),
                        source,
                    });
                }
                UrlInput::Skip => {
                    self.note_no_public_url();
                    return Ok(PublicUrlAnswer {
                        url: None,
                        source: "skipped",
                    });
                }
                UrlInput::Retry(reason) => self.note(&reason),
            }
        }
    }

    fn note_no_public_url(&self) {
        self.note(
            "no public URL: the App gets no webhook, so pushes and pull requests will not \
             start runs and PR checks will not report",
        );
        self.note("add one later with: preloop setup github --via app --public-url <URL>");
    }

    // -- step 2: golden ---------------------------------------------------

    async fn configure_golden(&mut self, answer: &GoldenAnswer) -> anyhow::Result<GoldenDecision> {
        self.section(&format!("2/5 Golden: {}", answer.kind.as_str()));
        let decision = match answer.kind {
            GoldenChoice::Official => Ok(GoldenDecision {
                kind: GoldenChoice::Official,
                base_image: None,
                dockerfile: None,
                label: "official GitHub runner image".to_owned(),
                artifact_bytes: Some(OFFICIAL_GOLDEN_DOWNLOAD_BYTES),
                arch_ok: None,
                warnings: Vec::new(),
                detail: json!({
                    "note": "the engine downloads the pinned packed official golden on the first \
                             serve; a failed download fails the job (no local fallback)",
                    "download_bytes": OFFICIAL_GOLDEN_DOWNLOAD_BYTES,
                    "working_set_bytes": OFFICIAL_GOLDEN_WORKING_SET_BYTES,
                }),
            }),
            GoldenChoice::Oci => {
                let reference = answer
                    .base_image
                    .clone()
                    .ok_or_else(|| usage_error("--golden oci needs --base-image <ref>"))?;
                self.record_oci(reference).await
            }
            GoldenChoice::Dockerfile => {
                let dockerfile = answer.dockerfile.clone().unwrap_or_else(default_dockerfile);
                let context = answer
                    .docker_context
                    .clone()
                    .unwrap_or_else(|| dockerfile_context(&dockerfile));
                self.record_dockerfile(&dockerfile, &context)
            }
            GoldenChoice::File => {
                let path = answer
                    .path
                    .clone()
                    .ok_or_else(|| usage_error("--golden file needs --path <file-or-dir>"))?;
                self.record_local_path(&path)
            }
        }?;
        // A configured image is baked by the pool on the next serve. The
        // official golden takes the opposite path — downloaded packed by the
        // engine, never baked — so this line must not be shown for it (and
        // cannot be: an official decision carries no base image).
        if decision.base_image.is_some() {
            self.note(
                "serve builds this golden locally on the first run (a builder VM, then a \
                 packed artifact); nothing is baked now",
            );
        }
        self.note(&format!("chosen: {}", decision.label));
        for warning in &decision.warnings {
            self.note(&format!("warning: {warning}"));
        }
        let mut detail = decision.detail.clone();
        if !decision.warnings.is_empty()
            && let Some(object) = detail.as_object_mut()
        {
            object.insert("warnings".to_owned(), json!(decision.warnings));
        }
        self.emit(json!({
            "step": "golden",
            "decision": decision.kind.as_str(),
            "result": detail,
        }));
        Ok(decision)
    }

    async fn record_oci(&mut self, reference: String) -> anyhow::Result<GoldenDecision> {
        if reference.starts_with('/')
            || reference.starts_with('.')
            || Path::new(&reference).exists()
        {
            return Err(usage_error(format!(
                "`{reference}` is a filesystem path, not a registry reference — use \
                 --golden file for a local .smolmachine/rootfs, or --golden dockerfile to \
                 build one here"
            )));
        }
        let trimmed = reference.trim();
        if trimmed.is_empty() {
            return Err(usage_error("--base-image must not be empty"));
        }
        if trimmed.contains(char::is_whitespace) {
            return Err(usage_error(format!(
                "`{trimmed}` is not a valid image reference (whitespace)"
            )));
        }
        let parsed =
            OciReference::parse(trimmed).map_err(|error| usage_error(error.to_string()))?;
        if !parsed.is_digest_pinned() {
            self.note(
                "tip: pin the reference with @sha256:… so a retag cannot change what your \
                 jobs run",
            );
        }
        let client = oci_client()?;
        let probe = resolve_manifest(&parsed, &client).await?;
        let arch_ok = probe.supports_host_arch();
        if arch_ok == Some(false) {
            self.note(&format!(
                "warning: {} publishes no image for this host ({})",
                parsed.display(),
                probe.platforms.join(", ")
            ));
        }
        let layers = probe.layers_bytes;
        self.note(&format!(
            "resolved {}{}",
            parsed.display(),
            match layers {
                Some(bytes) => format!(" ({} of layers)", gib(bytes)),
                None => String::new(),
            }
        ));
        Ok(GoldenDecision {
            kind: GoldenChoice::Oci,
            base_image: Some(reference),
            dockerfile: None,
            label: format!("OCI image {}", parsed.display()),
            artifact_bytes: layers,
            arch_ok,
            warnings: Vec::new(),
            detail: json!({
                "reference": parsed.display(),
                "registry": parsed.registry,
                "digest_pinned": parsed.is_digest_pinned(),
                "media_type": probe.media_type,
                "platforms": probe.platforms,
                "layers_bytes": layers,
            }),
        })
    }

    fn record_dockerfile(
        &mut self,
        dockerfile: &Path,
        context: &Path,
    ) -> anyhow::Result<GoldenDecision> {
        if let Some(reason) = dockerfile_unavailable_reason(dockerfile) {
            return Err(build_error(format!(
                "--golden dockerfile cannot run here: {reason}"
            )));
        }
        let engine = container_engine().expect("checked by dockerfile_unavailable_reason");
        let built = build_base_tar(engine, dockerfile, context, self.json)?;
        let arch_ok = built_image_arch(engine, &built.tag, self.json);
        let archive_warnings: Vec<String> =
            oversized_archive_warning(built.bytes, "the saved image tar")
                .into_iter()
                .collect();
        self.note(&format!(
            "built {} and saved it to {} ({})",
            built.dockerfile.display(),
            built.tar.display(),
            human_bytes(built.bytes)
        ));
        let detail = json!({
            "dockerfile": built.dockerfile.display().to_string(),
            "context": built.context.display().to_string(),
            "tag": built.tag,
            "tar": built.tar.display().to_string(),
            "tar_bytes": built.bytes,
            "engine": engine,
        });
        Ok(GoldenDecision {
            kind: GoldenChoice::Dockerfile,
            base_image: Some(built.tar.display().to_string()),
            dockerfile: Some(built.dockerfile),
            label: format!("local image tar {}", built.tar.display()),
            artifact_bytes: Some(built.bytes),
            arch_ok,
            warnings: archive_warnings,
            detail,
        })
    }

    fn record_local_path(&mut self, path: &Path) -> anyhow::Result<GoldenDecision> {
        let path = std::fs::canonicalize(path).map_err(|error| {
            usage_error(format!(
                "--path {}: {error} (give an existing .smolmachine pack or rootfs directory)",
                path.display()
            ))
        })?;
        let metadata = std::fs::metadata(&path).context("reading the --path metadata")?;
        if metadata.is_dir() {
            return Ok(GoldenDecision {
                kind: GoldenChoice::File,
                base_image: Some(path.display().to_string()),
                dockerfile: None,
                label: format!("rootfs directory {}", path.display()),
                artifact_bytes: None,
                arch_ok: None,
                warnings: Vec::new(),
                detail: json!({"kind": "rootfs", "path": path.display().to_string()}),
            });
        }
        if !metadata.is_file() {
            return Err(usage_error(format!(
                "--path {} is neither a file nor a directory",
                path.display()
            )));
        }
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if !(name.ends_with(".smolmachine") || name.ends_with(".tar")) {
            self.note(&format!(
                "note: `{name}` is neither *.smolmachine nor *.tar, and smolvm reads any other \
                 file as a pack — rename it to .tar if it is an image archive"
            ));
        }
        let path_warnings: Vec<String> = name
            .ends_with(".tar")
            .then(|| oversized_archive_warning(metadata.len(), "this image archive"))
            .flatten()
            .into_iter()
            .collect();
        Ok(GoldenDecision {
            kind: GoldenChoice::File,
            base_image: Some(path.display().to_string()),
            dockerfile: None,
            label: format!("local base image {}", path.display()),
            artifact_bytes: Some(metadata.len()),
            arch_ok: None,
            warnings: path_warnings,
            detail: json!({
                "kind": if name.ends_with(".tar") { "image tar" } else { "smolmachine pack" },
                "path": path.display().to_string(),
                "bytes": metadata.len(),
            }),
        })
    }

    // -- step 3: workflows ------------------------------------------------

    /// Step 3: the secrets and `runs-on` labels the repositories' workflows
    /// need, checked against the engine's own rules. Nothing is stored here
    /// unless the operator says so on a terminal, and a repository that cannot
    /// be read is reported and skipped: it never fails the run.
    async fn workflow_step(
        &mut self,
        args: &InitArgs,
        golden: &GoldenDecision,
    ) -> anyhow::Result<()> {
        self.section("3/5 Workflows: secrets and runner labels");
        let config = load_config().context("reading the engine config")?;
        let checkout = workflows::checkout_root();
        // A checkout with no `origin` keeps its `local/<dir>` identity: it is
        // the scope local workflow submissions store and read secrets under,
        // so dropping it would report those secrets missing and offer to
        // store them globally instead.
        let origin = checkout.as_ref().map(|_| crate::detect_repository());
        let targets = workflows::targets(&args.repos, checkout.as_deref(), origin.as_deref());
        if targets.is_empty() {
            self.note(
                "no repository in scope: run inside a git checkout, or pass --repo owner/name",
            );
            self.emit(json!({
                "step": "secrets",
                "decision": "none",
                "result": {"targets": [], "secrets": [], "missing": [], "commands": []},
            }));
            self.emit(json!({
                "step": "runner_labels",
                "decision": "none",
                "result": {"targets": [], "labels": [], "unparsed": [], "set": null},
            }));
            return Ok(());
        }
        let mut scans = Vec::new();
        for target in targets {
            scans.push(workflows::read_target(target, &config).await);
        }
        for scan in &scans {
            self.note(&workflows::describe(scan));
            for note in &scan.notes {
                self.note(&format!("note: {note}"));
            }
        }
        self.workflow_secrets(args, &config, &scans).await?;
        // The pool's golden is the image the engine will actually fork jobs
        // from: `PRELOOP_RUNNER_BASE_IMAGE`, else this run's choice, else the
        // official golden. Classifying the labels against the wizard's choice
        // alone would claim `ubuntu-22.04` runs on the official golden while
        // an override makes the engine run it on the override.
        let pool = workflows::Pool::new(
            crate::runner_pool_labels(),
            std::env::consts::ARCH,
            &effective_base_image(golden.base_image.as_deref()),
        );
        self.workflow_labels(&scans, &pool);
        Ok(())
    }

    /// The secrets the workflows read, against the names the engine stores.
    /// Missing ones are listed with the command that stores them; on a terminal
    /// each can be set now, through the same hidden prompt as `preloop secret set`.
    async fn workflow_secrets(
        &mut self,
        args: &InitArgs,
        config: &ConfigFile,
        scans: &[workflows::Scan],
    ) -> anyhow::Result<()> {
        if args.no_secrets_scan {
            self.note("secrets: not scanned (--no-secrets-scan)");
            self.emit(json!({
                "step": "secrets",
                "decision": "skipped",
                "result": {"reason": "--no-secrets-scan"},
            }));
            return Ok(());
        }
        let (stored, stored_in) = Self::stored_secret_names(config).await;
        let mut per_target: Vec<_> = scans
            .iter()
            .filter(|scan| scan.skipped.is_none())
            .map(|scan| (scan, workflows::secret_rows(scan, &stored)))
            .collect();
        self.note(&format!("secrets (checked against the {stored_in}):"));
        for (scan, rows) in &per_target {
            if rows.is_empty() {
                self.note(&format!("{}: none read", workflows::target_name(scan)));
                continue;
            }
            self.note(&format!("{}:", workflows::target_name(scan)));
            let cells: Vec<Vec<String>> = rows
                .iter()
                .map(|row| {
                    vec![
                        row.name.clone(),
                        workflows::places(&row.reads),
                        row.status(),
                    ]
                })
                .collect();
            for line in workflows::table(&["SECRET", "READ IN", "CONFIGURED"], &cells) {
                self.note(&format!("  {line}"));
            }
        }
        // A row the engine holds only in an environment tier is missing for
        // the jobs that read it, but it gets no `set` offer and no command:
        // the tier it lives in is a deliberate scope, and the repository-wide
        // write the other missing rows are offered would silently escalate it
        // to every job of the repository. What to do instead is said here.
        for (scan, rows) in &per_target {
            for row in rows.iter().filter(|row| row.environment_scope.is_some()) {
                self.note(&workflows::environment_only_advice(
                    row,
                    scan.target.repo.as_deref(),
                ));
            }
        }
        // The rows the interactive pass can fix, by position in `per_target`.
        // A row the engine holds only in an environment tier is *not* one of
        // them (see above).
        let mut missing_at: Vec<(usize, usize)> = Vec::new();
        for (target_index, (_, rows)) in per_target.iter().enumerate() {
            for (row_index, row) in rows.iter().enumerate() {
                if row.scope.is_none() && row.environment_scope.is_none() {
                    missing_at.push((target_index, row_index));
                }
            }
        }
        if self.interactive && !missing_at.is_empty() {
            for &(target_index, row_index) in &missing_at {
                let (name, repo, environment) = {
                    let (scan, rows) = &per_target[target_index];
                    let row = &rows[row_index];
                    (
                        row.name.clone(),
                        scan.target.repo.clone(),
                        row.environment_scope.clone(),
                    )
                };
                if workflows::set_command(&name, repo.as_deref(), environment.as_deref()).is_none()
                {
                    continue;
                }
                let scope = repo.as_deref().map_or_else(
                    || "globally".to_owned(),
                    |repo| format!("for {}", escape_control(repo)),
                );
                let prompt = format!("    set {} {scope} now? [y/N] ", escape_control(&name));
                if !self.ask_bool(&prompt, false)? {
                    continue;
                }
                let command = github_setup::SecretCommand::Set {
                    name: name.clone(),
                    value: None,
                    repo: repo.clone(),
                    env: None,
                };
                match github_setup::cmd_secret(github_setup::SecretArgs { command }).await {
                    Ok(()) => {
                        // The write succeeded, so the row is configured at the
                        // scope it was written to: the table, the JSON entries,
                        // and the decision below all come from that state.
                        let (_, rows) = &mut per_target[target_index];
                        mark_stored(rows, &name, repo.as_deref());
                    }
                    Err(error) => self.note(&format!("not set: {error:#}")),
                }
            }
        }
        let mut json_secrets = Vec::new();
        let mut all_rows = Vec::new();
        let mut missing: Vec<(String, Option<String>)> = Vec::new();
        for (scan, rows) in &per_target {
            json_secrets.extend(workflows::secret_rows_json(scan, rows));
            missing.extend(
                rows.iter()
                    .filter(|row| row.scope.is_none())
                    .map(|row| (row.name.clone(), scan.target.repo.clone())),
            );
            all_rows.extend(rows.iter().cloned());
        }
        let commands: Vec<String> = per_target
            .iter()
            .flat_map(|(scan, rows)| {
                rows.iter()
                    .filter(|row| row.scope.is_none())
                    .filter_map(|row| {
                        workflows::set_command(
                            &row.name,
                            scan.target.repo.as_deref(),
                            row.environment_scope.as_deref(),
                        )
                    })
            })
            .collect();
        if !commands.is_empty() {
            self.note("store the missing secrets with:");
            for command in &commands {
                self.note(&format!("  {command}"));
            }
        }
        self.emit(json!({
            "step": "secrets",
            "decision": workflows::secrets_decision(&all_rows),
            "result": {
                "stored_in": stored_in,
                "targets": workflows::targets_json(scans),
                "secrets": json_secrets,
                "missing": missing
                    .iter()
                    .map(|(name, repo)| json!({"name": name, "repo": repo}))
                    .collect::<Vec<_>>(),
                "commands": commands,
            },
        }));
        Ok(())
    }

    /// The `runs-on` labels the workflows request, each with the verdict the
    /// engine's own matching gives it on this host.
    fn workflow_labels(&mut self, scans: &[workflows::Scan], pool: &workflows::Pool) {
        let report = workflows::label_report(scans, pool);
        if report.rows.is_empty() {
            self.note("runner labels: none requested");
        } else {
            self.note(&format!("runner labels ({} distinct):", report.rows.len()));
            let cells: Vec<Vec<String>> = report
                .rows
                .iter()
                .map(|row| {
                    vec![
                        row.verdict.severity().to_owned(),
                        row.label.clone(),
                        workflows::places(&row.uses),
                        workflows::verdict_message(row.verdict, &row.label, pool),
                    ]
                })
                .collect();
            for line in workflows::table(&["STATUS", "LABEL", "USED IN", "MEANING"], &cells) {
                self.note(&format!("  {line}"));
            }
        }
        for (path, error) in &report.unparsed {
            self.note(&format!(
                "could not parse {path} ({error}); its runs-on was not checked"
            ));
        }
        let declare = workflows::declare_line(pool, &report);
        if let Some(line) = &declare {
            self.note(&format!(
                "this host needs {line} where `preloop serve` runs (it adds to the base labels)"
            ));
        }
        self.emit(json!({
            "step": "runner_labels",
            "decision": report.decision(),
            "result": {
                "pool": workflows::pool_json(pool),
                "targets": workflows::targets_json(scans),
                "labels": workflows::label_rows_json(&report, pool),
                "unparsed": report
                    .unparsed
                    .iter()
                    .map(|(path, error)| json!({"path": path, "error": error}))
                    .collect::<Vec<_>>(),
                "set": declare,
            },
        }));
    }

    /// The secret names the engine holds: the running engine's when it
    /// answers, else the config file's, as `preloop secret list` reads them.
    async fn stored_secret_names(config: &ConfigFile) -> (workflows::Stored, &'static str) {
        match tokio::time::timeout(
            Duration::from_secs(2),
            github_setup::api_list_secrets(None, None),
        )
        .await
        {
            Ok(Ok(github_setup::ApiOutcome::Applied(entries))) => {
                (workflows::Stored::from_listing(&entries), "engine")
            }
            _ => (workflows::Stored::from_config(config), "config file"),
        }
    }

    // -- step 4: preflight ------------------------------------------------

    /// Check the host against the chosen golden.
    ///
    /// The *hard* requirement is what the engine itself refuses to proceed
    /// without, so `init` never blocks something `serve` would accept and
    /// never accepts something `serve` would refuse:
    ///
    /// - the official golden's download (artifact + the engine's margin);
    /// - a local golden build's builder disk + pack staging, but only when a
    ///   mode that starts the engine was chosen: "configure only" places
    ///   nothing, so it reports that number as a warning instead.
    ///
    /// The *recommended* number is what the golden settles into (the unpacked
    /// official image and per-job VM space; the same builder rule otherwise)
    /// and only ever warns — the engine warns about the unpack too.
    fn preflight(&mut self, decision: &GoldenDecision, mode: RunMode) -> anyhow::Result<Value> {
        self.section("5/5 Preflight");
        let starting = matches!(mode, RunMode::Foreground | RunMode::Service);
        let data_root = preloop_vm::machine_data_root();
        let free = data_root.as_deref().and_then(|root| {
            match preloop_vm::filesystem_available_bytes(root) {
                Ok(free) => Some(free),
                Err(error) => {
                    // Mirror the engine: a volume `df` cannot measure is not a
                    // refusal, the check exists to fail early.
                    if !self.json {
                        println!(
                            "    disk: cannot measure {} ({error}); skipping",
                            root.display()
                        );
                    }
                    None
                }
            }
        });
        // The engine's own builder rule: builder disk + pack staging.
        let builder_gib = u64::from(golden_builder_storage_gib(crate::runner_storage_gib()));
        let bake_bytes = (builder_gib + GOLDEN_BUILD_DISK_HEADROOM_GIB) * GIB;
        let download_bytes = decision
            .artifact_bytes
            .map(|bytes| bytes + GOLDEN_DOWNLOAD_DISK_MARGIN)
            .unwrap_or(0);
        let (required, advisory) = match decision.kind {
            GoldenChoice::Official => (download_bytes, OFFICIAL_GOLDEN_WORKING_SET_BYTES),
            _ if starting => (bake_bytes, bake_bytes),
            _ => (0, bake_bytes),
        };
        let (hypervisor, hypervisor_detail) = hypervisor_state();

        let mut shortfalls: Vec<Value> = Vec::new();
        if let (Some(free), Some(root)) = (free, data_root.as_deref())
            && required > 0
            && free < required
        {
            shortfalls.push(json!({
                "resource": "disk",
                "purpose": if decision.kind == GoldenChoice::Official {
                    "the official golden download"
                } else {
                    "the local golden build the engine will run on the first serve"
                },
                "path": root.display().to_string(),
                "required_bytes": required,
                "available_bytes": free,
            }));
        }
        if decision.arch_ok == Some(false) {
            shortfalls.push(json!({
                "resource": "architecture",
                "host": std::env::consts::ARCH,
                "detail": format!("{} does not publish an image for this host", decision.label),
            }));
        }
        if !hypervisor {
            shortfalls.push(json!({
                "resource": "hypervisor",
                "host": std::env::consts::OS,
                "detail": hypervisor_detail,
            }));
        }

        if !self.json {
            match (free, data_root.as_deref()) {
                (Some(free), Some(root)) => println!(
                    "    disk: {} free on {}{}",
                    gib(free),
                    root.display(),
                    if required > 0 {
                        format!(" (needs {} to place the base image)", gib(required))
                    } else {
                        String::new()
                    }
                ),
                _ => println!("    disk: unknown (the SmolVM data volume is not resolvable)"),
            }
            println!(
                "    arch: {}{}",
                std::env::consts::ARCH,
                match decision.arch_ok {
                    Some(true) => " (image supports it)",
                    Some(false) => " (image does NOT support it)",
                    None => "",
                }
            );
            println!("    hypervisor: {hypervisor_detail}");
        }
        // Reported, never fatal: the engine enforces these itself at bake
        // time, and by then the decision is made. Saying it here is what turns
        // "the first serve refused to build a golden" into a warning the user
        // saw while configuring.
        let mut warnings: Vec<String> = Vec::new();
        if let Some(free) = free
            && free < advisory
        {
            warnings.push(match decision.kind {
                GoldenChoice::Official => format!(
                    "{} free now; the official golden wants about {} (the download, the unpacked \
                     golden, and per-job VM space)",
                    gib(free),
                    gib(advisory)
                ),
                _ => format!(
                    "{} free now; the engine's first local golden build wants about {} ({} builder \
                     disk + {} pack staging) and refuses below that — free space or lower \
                     PRELOOP_RUNNER_STORAGE_GB",
                    gib(free),
                    gib(advisory),
                    gib(builder_gib * GIB),
                    gib(GOLDEN_BUILD_DISK_HEADROOM_GIB * GIB),
                ),
            });
        }
        let report = json!({
            "data_root": data_root.as_deref().map(|root| root.display().to_string()),
            "disk_free_bytes": free,
            "disk_required_bytes": required,
            "disk_recommended_bytes": advisory,
            "arch": std::env::consts::ARCH,
            "arch_ok": decision.arch_ok,
            "hypervisor": hypervisor,
            "hypervisor_detail": hypervisor_detail,
            "shortfalls": shortfalls,
            "warnings": warnings,
        });

        if !shortfalls.is_empty() {
            let disk_only = shortfalls.iter().all(|short| short["resource"] == "disk");
            if disk_only && disk_preflight_overridden() {
                self.emit(json!({"step": "preflight", "decision": "overridden", "result": report}));
                if !self.json {
                    println!(
                        "    {DISK_PREFLIGHT_OVERRIDE} is set; continuing despite the disk shortfall"
                    );
                }
                return Ok(report);
            }
            self.emit(json!({"step": "preflight", "decision": "failed", "result": report}));
            let detail = shortfalls
                .iter()
                .map(describe_shortfall)
                .collect::<Vec<_>>()
                .join("; ");
            return Err(preflight_error(
                format!(
                    "preflight failed: {detail}. Free space, lower PRELOOP_RUNNER_STORAGE_GB, or \
                     set {DISK_PREFLIGHT_OVERRIDE}=1 to proceed anyway."
                ),
                Value::Array(shortfalls.clone()),
            ));
        }
        self.emit(json!({"step": "preflight", "decision": "ok", "result": report}));
        if !self.json {
            println!("    ok");
        }
        for warning in &warnings {
            self.note(&format!("warning: {warning}"));
        }
        Ok(report)
    }

    // -- starting the chosen mode -----------------------------------------

    /// A system service reads its own state dir; write the config there when
    /// the operator did not pin `PRELOOP_HOME` (which the service would then
    /// be told to use, because `install` gets the same value).
    fn prepare_service_home(&self) -> anyhow::Result<()> {
        ensure_service_root()?;
        let service_home = std::env::var_os("PRELOOP_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(crate::server_install::DEFAULT_HOME));
        if service_home == preloop_home() {
            return Ok(());
        }
        let target = service_home.join("config.toml");
        self.note(&format!(
            "service state dir: {} — the installed service reads this file, so the answers below \
             are written there (not {})",
            target.display(),
            preloop_runner_server::config::config_path().display(),
        ));
        // SAFETY: the CLI is single-threaded here and nothing else reads the
        // environment concurrently; `main` sets the same variable the same way.
        unsafe {
            std::env::set_var(preloop_runner_server::config::CONFIG_PATH_ENV, &target);
        }
        Ok(())
    }

    async fn start_mode(
        &self,
        mode: RunMode,
        observability: Option<preloop_observability::Observability>,
        config_path: &Path,
        public_url: Option<&str>,
    ) -> anyhow::Result<()> {
        match mode {
            RunMode::Configure => {
                // From `serve` the caller says it is continuing with the new
                // config; saying "on its next start" there would be wrong,
                // because this process picks the choice up immediately — the
                // URL included, which the caller installs into the engine it
                // is already running ([`apply_engine_public_url`]). An `init`
                // run starts nothing, so it still says both.
                if !self.json && !self.from_serve {
                    for line in configure_only_hint(config_path, public_url) {
                        println!("{line}");
                    }
                }
                Ok(())
            }
            RunMode::Foreground => {
                let observability = observability.ok_or_else(|| {
                    anyhow::anyhow!(
                        "internal: foreground mode without the engine's telemetry handle"
                    )
                })?;
                if !self.json {
                    println!("starting the engine in the foreground (Ctrl-C to stop)");
                }
                // Brand-new installs: initialize the control database so the
                // first foreground serve works with no extra step. An
                // existing database is never upgraded here — `preloop store
                // migrate` is the explicit path.
                let state_dir = crate::preloop_home().join("state");
                std::fs::create_dir_all(&state_dir)?;
                if preloop_runner_server::store_admin::prepare_brand_new_local(&state_dir)?
                    && !self.json
                {
                    println!(
                        "initialized the control database at {}",
                        state_dir.join("preloop.db").display()
                    );
                }
                crate::cmd_engine(serve_args_for(public_url), observability).await
            }
            RunMode::Service => {
                ensure_service_root()?;
                if !self.json {
                    println!("installing the control plane as a service…");
                }
                let home = std::env::var_os("PRELOOP_HOME").map(PathBuf::from);
                crate::server_install::run(crate::server_install::install_args(
                    home,
                    public_url.map(str::to_owned),
                ))
            }
        }
    }

    // -- prompts ----------------------------------------------------------

    fn read_line(&self, prompt: &str) -> anyhow::Result<String> {
        print!("{prompt}");
        std::io::stdout().flush().context("flushing stdout")?;
        let mut line = String::new();
        let read = std::io::stdin()
            .lock()
            .read_line(&mut line)
            .context("reading stdin")?;
        if read == 0 {
            anyhow::bail!("input closed before the wizard finished");
        }
        Ok(line.trim().to_owned())
    }

    fn ask_bool(&self, prompt: &str, default: bool) -> anyhow::Result<bool> {
        if self.yes {
            return Ok(default);
        }
        loop {
            let answer = self.read_line(prompt)?;
            match answer.to_ascii_lowercase().as_str() {
                "" => return Ok(default),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => println!("    answer y or n"),
            }
        }
    }

    fn ask_choice(&self, items: &[MenuItem], default: usize) -> anyhow::Result<usize> {
        loop {
            for (index, item) in items.iter().enumerate() {
                // A choice that cannot run here is shown, not hidden: the
                // reason ("no Dockerfile at ./Dockerfile", "docker is not on
                // PATH") is what tells the operator how to get it.
                match &item.disabled {
                    Some(reason) => println!(
                        "    {}) {} — {} [unavailable: {reason}]",
                        index + 1,
                        item.label,
                        item.description
                    ),
                    None => println!("    {}) {} — {}", index + 1, item.label, item.description),
                }
            }
            let answer = self.read_line(&format!("    choice [{}]: ", default + 1))?;
            let chosen = if answer.is_empty() {
                default
            } else {
                match answer.trim().parse::<usize>() {
                    Ok(value) if value >= 1 && value <= items.len() => value - 1,
                    _ => {
                        println!("    enter a number between 1 and {}", items.len());
                        continue;
                    }
                }
            };
            if let Some(reason) = &items[chosen].disabled {
                println!("    {} is unavailable: {reason}", items[chosen].label);
                continue;
            }
            return Ok(chosen);
        }
    }
}

impl GoldenAnswer {
    fn of(kind: GoldenChoice) -> Self {
        Self {
            kind,
            base_image: None,
            dockerfile: None,
            docker_context: None,
            path: None,
        }
    }
}

struct MenuItem {
    label: String,
    description: String,
    /// `Some(reason)` renders the item but refuses it, so the wizard can say
    /// *why* a choice is unavailable instead of hiding it.
    disabled: Option<String>,
}

impl MenuItem {
    fn enabled(label: &str, description: &str) -> Self {
        Self {
            label: label.to_owned(),
            description: description.to_owned(),
            disabled: None,
        }
    }
}

/// Reject a kind-specific flag that does not belong to the chosen kind:
/// silently ignoring `--base-image` with `--golden file` would configure
/// something other than what was asked for.
fn reject_mismatched_flags(kind: GoldenChoice, args: &InitArgs) -> anyhow::Result<()> {
    let mut stray: Vec<&str> = Vec::new();
    if kind != GoldenChoice::Oci && args.base_image.is_some() {
        stray.push("--base-image");
    }
    if kind != GoldenChoice::File && args.path.is_some() {
        stray.push("--path");
    }
    if kind != GoldenChoice::Dockerfile
        && (args.dockerfile.is_some() || args.docker_context.is_some())
    {
        stray.push("--dockerfile/--docker-context");
    }
    if stray.is_empty() {
        return Ok(());
    }
    Err(usage_error(format!(
        "{} do{} not apply to --golden {}; drop them or choose the matching kind",
        stray.join(", "),
        if stray.len() == 1 { "es" } else { "" },
        kind.as_str()
    )))
}

/// A flag the wizard still needs, asked for on the terminal.
fn require_interactive_flag(
    value: Option<String>,
    flag: &str,
    ask: &mut impl FnMut(&str) -> anyhow::Result<String>,
) -> anyhow::Result<String> {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        return Ok(value);
    }
    loop {
        let answer = ask(&format!("    {flag}: "))?;
        if !answer.is_empty() {
            return Ok(answer);
        }
        println!("    a value is required");
    }
}

fn describe_auth(auth: AuthChoice) -> &'static str {
    match auth {
        AuthChoice::App => "app",
        AuthChoice::Pat => "pat",
        AuthChoice::None => "none",
    }
}

fn describe_mode(mode: RunMode, golden: &str) -> String {
    match mode {
        RunMode::Foreground => format!("foreground now (serving {golden})"),
        RunMode::Service => "install as a service".to_owned(),
        RunMode::Configure => "none (configure only)".to_owned(),
    }
}

fn describe_shortfall(short: &Value) -> String {
    match short["resource"].as_str() {
        Some("disk") => format!(
            "{} needs {} free on {} but only {} is available",
            short["purpose"].as_str().unwrap_or("the golden"),
            gib(short["required_bytes"].as_u64().unwrap_or(0)),
            short["path"].as_str().unwrap_or("the data volume"),
            gib(short["available_bytes"].as_u64().unwrap_or(0)),
        ),
        Some("architecture") => format!(
            "the chosen image has no build for this host architecture ({})",
            short["host"].as_str().unwrap_or("unknown")
        ),
        Some("hypervisor") => format!(
            "no microVM hypervisor: {}",
            short["detail"].as_str().unwrap_or("unavailable")
        ),
        other => other.unwrap_or("unknown preflight failure").to_owned(),
    }
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// Sizes a human reads: GiB above a gigabyte, MiB below, so a small locally
/// built image does not print as "0.0 GiB".
fn human_bytes(bytes: u64) -> String {
    if bytes >= GIB {
        gib(bytes)
    } else {
        format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn ensure_service_root() -> anyhow::Result<()> {
    if unsafe { libc::geteuid() } == 0 {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "`install as a service` must run as root — re-run `sudo preloop init` (or pick \
         \"configure only\" and install the service later with `preloop server install`)"
    ))
}

/// The stored answer step 1 shows on a re-run, with the label to display.
///
/// A config file with no credential in it still answers the question — the
/// operator already decided this engine runs local-only — so "none" is offered
/// as the previous answer instead of asking the whole menu again. A machine
/// that has never run `init` has no file, and gets the menu (whose default is
/// the recommended App).
fn credential_prior(config: &ConfigFile) -> Option<(AuthChoice, String)> {
    if let Some(summary) = credential_summary(config) {
        let choice = if config.github.app_id.is_none()
            && config.github.apps.is_empty()
            && config.github.pat().is_some()
        {
            AuthChoice::Pat
        } else {
            AuthChoice::App
        };
        return Some((choice, summary));
    }
    preloop_runner_server::config::config_path()
        .exists()
        .then(|| (AuthChoice::None, "none — local only".to_owned()))
}

/// The credential summary a re-run shows for step 1.
fn credential_summary(config: &ConfigFile) -> Option<String> {
    if let Some(app_id) = config.github.app_id.as_deref() {
        return Some(format!("GitHub App {app_id}"));
    }
    if !config.github.apps.is_empty() {
        let ids = config
            .github
            .apps
            .iter()
            .map(|app| app.app_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Some(format!("GitHub Apps {ids}"));
    }
    config.github.pat().map(|_| "fine-grained PAT".to_owned())
}

/// The engine's public URL for webhooks, and where the answer came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicUrlAnswer {
    url: Option<String>,
    /// `flag`, `env`, `prompt`, `suggested`, or `skipped`.
    source: &'static str,
}

/// The URL the run mode this run starts (or installs) is handed, so Check Run
/// detail links and the webhook health check use the address GitHub was given
/// instead of the loopback default.
///
/// The App flow's own answer wins — it may have come from the prompt — and
/// without one (`--auth pat`, `--auth none`, or a kept credential)
/// `--public-url` and then `PRELOOP_PUBLIC_URL` are read the same way
/// `preloop serve` reads them. A value that does not validate is dropped:
/// `serve` would fall back to loopback anyway, and only the App flow, which has
/// to give GitHub a working URL, refuses the input outright.
fn engine_public_url(args: &InitArgs, answer: Option<&PublicUrlAnswer>) -> Option<String> {
    if let Some(answer) = answer {
        return answer.url.clone();
    }
    args.public_url
        .clone()
        .or_else(|| std::env::var("PRELOOP_PUBLIC_URL").ok())
        .and_then(|raw| validate_public_url(&raw).ok())
}

/// `serve` args for the foreground mode: `serve`'s own defaults plus the URL
/// resolved above. `None` keeps `serve`'s fallback (`PRELOOP_PUBLIC_URL`, then
/// loopback) untouched.
fn serve_args_for(public_url: Option<&str>) -> crate::ServeArgs {
    crate::ServeArgs {
        public_url: public_url.map(str::to_owned),
        ..crate::ServeArgs::default()
    }
}

/// Hand the URL the wizard resolved to the engine that is already running in
/// this process: `serve` offers the wizard before it starts its server, and
/// Check Run detail links and the webhook health check read
/// `PRELOOP_PUBLIC_URL` off this process's environment — a `serve` that never
/// starts cannot pick the answer up any other way. Without this the App's
/// webhook would point at the new URL while the running engine stayed on the
/// old (loopback) one.
///
/// `current` is the origin the engine resolved for itself before the wizard
/// ran (`--public-url`, `PRELOOP_PUBLIC_URL`, or the loopback default). The
/// wizard's answer replaces only a loopback default, and is a no-op when the
/// two already agree: an origin the operator configured deliberately is never
/// silently replaced. Returns the line that says which one won — `None` when
/// there was nothing to decide (no answer, or the same one).
pub(crate) fn apply_engine_public_url(current: &str, resolved: Option<&str>) -> Option<String> {
    let Some(resolved) = resolved.map(str::trim).filter(|url| !url.is_empty()) else {
        // The wizard skipped the question: the engine's own origin stands.
        return None;
    };
    let resolved = resolved.trim_end_matches('/');
    let current = current.trim().trim_end_matches('/');
    if resolved == current {
        return None;
    }
    if !is_loopback_url(current) {
        return Some(format!(
            "keeping this engine's configured public URL {}; the wizard's {} is not applied to \
             it — restart `serve` with PRELOOP_PUBLIC_URL={} to switch",
            escape_control(current),
            escape_control(resolved),
            escape_control(resolved),
        ));
    }
    // SAFETY: this runs at engine startup, before the server reads the
    // variable, and nothing else touches the environment concurrently.
    unsafe { std::env::set_var("PRELOOP_PUBLIC_URL", resolved) };
    Some(format!(
        "public URL {} — this engine now advertises it (Check Run detail links and the webhook \
         health check use it)",
        escape_control(resolved)
    ))
}

/// What a configure-only run says when it starts nothing: `serve` reads the
/// written config on its next start, and the URL this run resolved has to be
/// handed to it explicitly — the config file does not hold the URL, so without
/// this line the answer is lost the moment the wizard exits. (From `serve`
/// itself neither line is printed: that process picks the answer up now.)
fn configure_only_hint(config_path: &Path, public_url: Option<&str>) -> Vec<String> {
    let mut lines = vec![format!(
        "configure only: `preloop serve` reads {} on its next start",
        config_path.display()
    )];
    if let Some(url) = public_url {
        lines.push(format!(
            "public URL {url} — start `serve` with PRELOOP_PUBLIC_URL={url} (or --public-url \
             {url}) to keep using it"
        ));
    }
    lines
}

/// Why the stable-URL question is asked, and how to get one, in the wizard's
/// own words. Whatever fronts the engine must publish *only*
/// `/api/v1/github/webhooks`: the port also serves runner registration, which
/// no tunnel should expose (docs/self-hosting.md, "Exposure options"). The
/// Funnel wording must not promise that mounting one path keeps the rest
/// private — Funnel access applies to the whole host and port, so an earlier
/// root mount would become public with it (tailscale/tailscale#10234).
const PUBLIC_URL_HELP: [&str; 10] = [
    "PR status checks and push-triggered runs need GitHub to reach this engine at a stable",
    "HTTPS URL — only /api/v1/github/webhooks may be public, never the whole port (it also",
    "serves runner registration). Funnel access applies to the whole host and port, not just",
    "the mounted path, so use a dedicated hostname (or remove every other mount there) before",
    "enabling it, then mount the webhook path:",
    "  tailscale funnel --set-path=/api/v1/github/webhooks --bg http://127.0.0.1:9090/api/v1/github/webhooks",
    "(use your PRELOOP_LISTEN port in place of 9090; verify with `tailscale funnel status`)",
    "Alternatives: a Cloudflare *named* Tunnel (quick tunnels change on restart and break the",
    "App's webhook) behind a path-filtering proxy, or your own domain + reverse proxy — see",
    "docs/self-hosting.md, \"Exposure options\".",
];

/// A stable HTTPS address detected on this host, and what Tailscale Funnel
/// publishes at it. Only a host:port whose *only* public mount is the webhook
/// path may be accepted: Funnel access applies to the whole host and port, so
/// a name that is not Funnel-published is tailnet-only (GitHub cannot reach
/// it), and a name that also publishes other paths would expose them with it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UrlSuggestion {
    url: String,
    funnel: FunnelPublication,
}

/// What `tailscale funnel status --json` shows for this machine's HTTPS name.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FunnelPublication {
    /// The webhook path is published there and nothing else: reachable from
    /// GitHub, and nothing beside it became public.
    WebhookPathOnly,
    /// The webhook path is published *and* so is everything else mounted on
    /// that host:port — the paths, named, that the operator would be exposing.
    WebhookPathAndOthers(Vec<String>),
    /// Nothing is Funnel-published there: plain Serve only serves the tailnet,
    /// which GitHub cannot reach.
    NotPublished,
    /// A `--bg`-less `serve`/`funnel` session is up for this host:port. Its
    /// config is what is public there while it lasts, and it is gone when that
    /// command exits, so the persistent mounts cannot certify the name either
    /// way.
    ForegroundSession,
}

impl FunnelPublication {
    /// Whether Enter may accept the name: GitHub can reach it, and the Funnel
    /// turned no other mount on that host:port public with it.
    fn accepts(&self) -> bool {
        matches!(self, Self::WebhookPathOnly)
    }
}

/// What one line at the URL prompt means.
#[derive(Debug, Clone, PartialEq, Eq)]
enum UrlInput {
    /// A usable URL, and whether the operator typed it or accepted the
    /// suggestion.
    Use { url: String, source: &'static str },
    /// Webhooks stay off.
    Skip,
    /// Refused, with the reason to show before asking again.
    Retry(String),
}

/// Read one answer to the URL question. Blank accepts a suggestion only when
/// Enter on it is safe (`FunnelPublication::accepts`): a tailnet-only name, or
/// one whose Funnel publishes other mounts too, would otherwise point the
/// App's webhook at an address GitHub cannot reach, and the pre-existing
/// semantics (blank = skip) still hold for it. `-` skips either way (webhooks
/// off, said plainly).
fn read_public_url(input: &str, suggestion: Option<&UrlSuggestion>) -> UrlInput {
    if input.is_empty() {
        return match suggestion {
            Some(suggestion) if suggestion.funnel.accepts() => UrlInput::Use {
                url: suggestion.url.clone(),
                source: "suggested",
            },
            _ => UrlInput::Skip,
        };
    }
    if input == "-" {
        return UrlInput::Skip;
    }
    match validate_public_url(input) {
        Ok(url) => UrlInput::Use {
            url,
            source: "prompt",
        },
        Err(reason) => {
            let way_out = match suggestion {
                Some(suggestion) if suggestion.funnel.accepts() => {
                    "type \"-\" to leave webhooks off"
                }
                _ => "leave it blank to skip",
            };
            UrlInput::Retry(format!("{reason}; enter an https:// address, or {way_out}"))
        }
    }
}

/// The prompt for the URL question. Only a name Enter may safely accept is
/// shown as a suggestion; a detected-but-unusable one is a hint, so the prompt
/// keeps the safe default (blank = skip) and the help above says what is
/// missing. `WebhookPathAndOthers` says so in its own words: Funnel *does*
/// publish the webhook path there, it just publishes other mounts with it, and
/// "not published" would contradict both the help above and the operator's own
/// `tailscale funnel status`.
fn public_url_prompt(suggestion: Option<&UrlSuggestion>) -> String {
    match suggestion {
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::WebhookPathOnly,
        }) => format!("    public URL [{url}] (Enter accepts; \"-\" skips, webhooks off): "),
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::WebhookPathAndOthers(_),
        }) => {
            format!("    public URL (blank = skip; {url} also publishes other paths, see above): ")
        }
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::ForegroundSession,
        }) => format!(
            "    public URL (blank = skip; {url} has a foreground serve session, see above): "
        ),
        Some(UrlSuggestion { url, .. }) => {
            format!("    public URL (blank = skip; {url} is not published by Funnel for GitHub): ")
        }
        None => "    public URL (blank = skip, local only): ".to_owned(),
    }
}

/// The explanation shown before the prompt. A detected name is only ever
/// claimed as detected, and an accepted suggestion must never look like
/// something `init` invented; a name Enter may not accept says why.
fn public_url_help(suggestion: Option<&UrlSuggestion>) -> Vec<String> {
    let mut lines: Vec<String> = PUBLIC_URL_HELP
        .iter()
        .map(|line| (*line).to_owned())
        .collect();
    match suggestion {
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::WebhookPathOnly,
        }) => {
            lines.push(format!(
                "detected on this host: tailscale funnel publishes {WEBHOOK_PATH} and nothing \
                 else at {url} — Enter accepts it"
            ));
        }
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::WebhookPathAndOthers(others),
        }) => {
            lines.push(format!(
                "this machine is on the tailnet as {url}, and its funnel also publishes {} on \
                 the same host and port:",
                others.join(", ")
            ));
            lines.push(
                "Tailscale applies Funnel to the whole host and port, so the webhook path cannot \
                 be public there alone (tailscale/tailscale#10234) — give it a dedicated \
                 hostname, check with `tailscale funnel status`, then re-run this step"
                    .to_owned(),
            );
        }
        Some(UrlSuggestion {
            url,
            funnel: FunnelPublication::ForegroundSession,
        }) => {
            lines.push(format!(
                "this machine is on the tailnet as {url}, and a `tailscale serve`/`funnel` \
                 command is running there without `--bg`:"
            ));
            lines.push(
                "a foreground session is what serves this name while it runs, and it stops with \
                 that command — so GitHub cannot rely on it, and the App's webhook needs a \
                 background mount instead. Re-run the funnel command above with `--bg` (or exit \
                 the foreground one), check with `tailscale funnel status`, then re-run this step"
                    .to_owned(),
            );
        }
        Some(UrlSuggestion { url, .. }) => {
            lines.push(format!(
                "this machine is on the tailnet as {url}, but no tailscale funnel publishes \
                 {WEBHOOK_PATH} there:"
            ));
            lines.push(
                "GitHub cannot reach a tailnet-only name, so it is a hint only — run the funnel \
                 command above, or type the URL your own proxy publishes"
                    .to_owned(),
            );
        }
        None => lines.push(
            "no stable URL detected here — type the one your tunnel or proxy publishes".to_owned(),
        ),
    }
    lines
}

/// The one path GitHub delivers webhooks to, and so the only path a tunnel in
/// front of the engine may publish.
const WEBHOOK_PATH: &str = "/api/v1/github/webhooks";

/// How long a `tailscale` query may take before the child is killed and the
/// query read as "no answer". The daemon is local, so anything slower is
/// unresponsive — and `status` must never hang the wizard.
const TAILSCALE_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the child is re-checked while waiting for that deadline.
const TAILSCALE_POLL: Duration = Duration::from_millis(50);

/// A stable HTTPS URL this host probably already has — the tailnet's own name
/// for this machine — with what Funnel publishes there. Best effort and never
/// fatal: no tailscale, a stopped daemon, an unreadable answer, a daemon that
/// does not answer within [`TAILSCALE_TIMEOUT`], or an unexpected shape all
/// mean "nothing detected" rather than a wizard failure.
fn detected_public_url() -> Option<UrlSuggestion> {
    let status = tailscale_json(&["status", "--json"])?;
    // A second query: without an answer the name is still shown, as a hint
    // only — never accepted. `funnel status --json` is the daemon's own
    // answer; `serve status --json` marshals the same config, so it stands in
    // when the daemon is too old for the funnel subcommand.
    let funnel = tailscale_json(&["funnel", "status", "--json"])
        .or_else(|| tailscale_json(&["serve", "status", "--json"]));
    tailnet_suggestion(&status, funnel.as_ref())
}

/// The suggestion the two daemon answers support, or `None` when this host has
/// no tailnet name to offer.
fn tailnet_suggestion(status: &Value, funnel: Option<&Value>) -> Option<UrlSuggestion> {
    let url = tailnet_public_url(status)?;
    let host = reqwest::Url::parse(&url).ok()?.host_str()?.to_owned();
    let funnel = match funnel {
        Some(funnel) => funnel_publication(funnel, &host),
        None => FunnelPublication::NotPublished,
    };
    Some(UrlSuggestion { url, funnel })
}

/// One `tailscale` query's JSON answer: `None` when tailscale is not
/// installed, exits non-zero, does not answer within [`TAILSCALE_TIMEOUT`]
/// (the child is killed), or answers with something that is not JSON.
fn tailscale_json(args: &[&str]) -> Option<Value> {
    let tailscale = find_on_path("tailscale")?;
    let mut command = std::process::Command::new(tailscale);
    command.args(args);
    parse_tailscale_json(&bounded_stdout(&mut command, TAILSCALE_TIMEOUT)?)
}

/// The JSON a tailscale answer carries; `None` when the answer is not JSON at
/// all (a daemon's error text on stdout, a version whose flag differs).
fn parse_tailscale_json(stdout: &str) -> Option<Value> {
    serde_json::from_str(stdout).ok()
}

/// Run `command` and read its stdout, killing it after `timeout`: `tailscale
/// status` talks to a daemon, and an unresponsive one must not hang the
/// wizard. `None` when the program cannot be run, exits non-zero, or does not
/// finish in time — all of which read as "no answer".
fn bounded_stdout(command: &mut std::process::Command, timeout: Duration) -> Option<String> {
    use std::process::Stdio;
    command
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::piped());
    let mut child = command.spawn().ok()?;
    let mut stdout = child.stdout.take()?;
    // Drained on its own thread: a chatty answer fills the pipe buffer while
    // this thread watches the clock, and a full buffer would otherwise block
    // the child until the deadline killed it.
    let reader = std::thread::spawn(move || {
        use std::io::Read;
        let mut bytes = Vec::new();
        let _ = stdout.read_to_end(&mut bytes);
        bytes
    });
    let deadline = std::time::Instant::now() + timeout;
    let mut succeeded = false;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                succeeded = status.success();
                break;
            }
            // An unresponsive daemon is not worth waiting for: kill it and
            // treat the query as unanswered.
            Ok(None) if std::time::Instant::now() >= deadline => break,
            Ok(None) => std::thread::sleep(TAILSCALE_POLL),
            Err(_) => break,
        }
    }
    if !succeeded {
        let _ = child.kill();
        // Reaped either way, so the reader thread sees EOF.
        let _ = child.wait();
    }
    let bytes = reader.join().ok()?;
    succeeded.then(|| String::from_utf8_lossy(&bytes).into_owned())
}

/// `tailscale status --json`'s name for this machine as a URL
/// (`https://<machine>.<tailnet>.ts.net`). Tailscale writes it as an FQDN with
/// a trailing dot, and a node that is not up has a name GitHub could not reach
/// either, so anything that does not validate is not offered.
fn tailnet_public_url(status: &Value) -> Option<String> {
    // The daemon's own verdict on the tailnet link: "Stopped", "NeedsLogin",
    // and friends still report a DNS name.
    if status.get("BackendState")?.as_str()? != "Running" {
        return None;
    }
    let dns_name = status
        .get("Self")?
        .get("DNSName")?
        .as_str()?
        .trim_end_matches('.');
    validate_public_url(&format!("https://{dns_name}")).ok()
}

/// What `tailscale funnel status --json` publishes at `host`'s HTTPS port.
///
/// `AllowFunnel` is the daemon's own "Funnel, not just Serve, is enabled for
/// this host:port" flag and `Web` carries the mounted paths: without the first
/// nothing there is public, and any shape this does not understand (an older
/// or newer daemon) degrades to "not published" rather than a false claim.
///
/// A `--bg`-less `serve`/`funnel` session writes a *separate*, session-scoped
/// config into `Foreground`, and the daemon consults those before the
/// persistent one (`ServeConfig.FindWeb` prefers a foreground handler for the
/// same host:port, and `HasFunnelForTarget` counts a foreground `AllowFunnel`
/// entry as well). So anything `Foreground` says about this host:port is what
/// is actually public — and only while the session lives.
fn funnel_publication(funnel_status: &Value, host: &str) -> FunnelPublication {
    if foreground_touches(funnel_status, host) {
        return FunnelPublication::ForegroundSession;
    }
    let allowed = funnel_status
        .get("AllowFunnel")
        .and_then(Value::as_object)
        .and_then(|map| host_entry(map, host))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !allowed {
        return FunnelPublication::NotPublished;
    }
    let handlers = funnel_status
        .get("Web")
        .and_then(Value::as_object)
        .and_then(|map| host_entry(map, host))
        .and_then(|entry| entry.get("Handlers"))
        .and_then(Value::as_object);
    let Some(handlers) = handlers else {
        return FunnelPublication::NotPublished;
    };
    if !handlers.keys().any(|path| path == WEBHOOK_PATH) {
        return FunnelPublication::NotPublished;
    }
    // Funnel is enabled for the host and port, so every other mount on it is
    // public too: the name is only usable once the webhook path stands alone.
    let mut others: Vec<String> = handlers
        .keys()
        .filter(|path| path.as_str() != WEBHOOK_PATH)
        .cloned()
        .collect();
    if others.is_empty() {
        return FunnelPublication::WebhookPathOnly;
    }
    others.sort();
    FunnelPublication::WebhookPathAndOthers(others)
}

/// The `AllowFunnel` / `Web` entry naming `host`, whatever port suffix this
/// daemon's version writes (`builder.tail1234.ts.net:443`, or a bare host,
/// which is the same 443): another port is a different address and does not
/// make this URL public. Tailscale reports names as FQDNs, so a key carrying
/// the trailing dot is the same name.
fn host_entry<'a>(map: &'a serde_json::Map<String, Value>, host: &str) -> Option<&'a Value> {
    let names_this_host_on_443 = |key: &str| {
        let (name, port) = match key.rsplit_once(':') {
            Some((name, port)) => (name, Some(port)),
            None => (key, None),
        };
        name.trim_end_matches('.').eq_ignore_ascii_case(host) && matches!(port, None | Some("443"))
    };
    map.iter()
        .find(|(key, _)| names_this_host_on_443(key))
        .map(|(_, value)| value)
}

/// Whether a `--bg`-less serve session in `funnel_status` configures `host` on
/// 443 at all — in its mounts (`Web`) or in its `AllowFunnel` flag. Either one
/// changes what is public there (see [`funnel_publication`]) and neither is
/// durable, so nothing can be certified from the persistent config while such
/// a session is up. A session on another host:port is a different address and
/// is left alone.
fn foreground_touches(funnel_status: &Value, host: &str) -> bool {
    ["AllowFunnel", "Web"].iter().any(|key| {
        funnel_status
            .get("Foreground")
            .and_then(Value::as_object)
            .is_some_and(|sessions| {
                sessions.values().any(|session| {
                    session
                        .get(*key)
                        .and_then(Value::as_object)
                        .is_some_and(|map| host_entry(map, host).is_some())
                })
            })
    })
}

/// Check the URL GitHub will deliver webhooks to, and return it without a
/// trailing slash. Refused: anything but https, a loopback or `localhost` host
/// (GitHub cannot reach those), credentials, a query, or a fragment. The error
/// names the rule that failed.
fn validate_public_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    let url = reqwest::Url::parse(trimmed)
        .map_err(|error| format!("`{trimmed}` is not a URL ({error})"))?;
    if url.scheme() != "https" {
        return Err(format!(
            "`{trimmed}` must start with https:// (GitHub delivers webhooks over HTTPS only)"
        ));
    }
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| format!("`{trimmed}` has no host name"))?;
    if loopback_host(host) {
        return Err(format!(
            "`{trimmed}` is a loopback address, which GitHub cannot reach"
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(format!("`{trimmed}` must not carry a username or password"));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(format!(
            "`{trimmed}` must not have a query or fragment; the webhook path is added for you"
        ));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// Whether a URL's host is one GitHub cannot deliver to: `localhost`, any
/// `.localhost` name, a loopback address, or the unspecified address. Shared
/// by [`validate_public_url`], which refuses such a URL outright, and the
/// `serve` handoff, which may replace one the engine fell back to.
fn loopback_host(host: &str) -> bool {
    let bare = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    bare == "localhost"
        || bare.ends_with(".localhost")
        || bare
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback() || ip.is_unspecified())
}

/// Whether `url` points at a host GitHub cannot reach. A URL that does not
/// parse counts as not-loopback: replacing an origin the operator configured
/// is never decided by a guess.
fn is_loopback_url(url: &str) -> bool {
    reqwest::Url::parse(url.trim())
        .ok()
        .and_then(|url| url.host_str().map(loopback_host))
        .unwrap_or(false)
}

/// The stored answer step 2 shows on a re-run, with the label to display and
/// store. The official golden has no base image to name, so a `kind` alone is
/// a complete answer there.
fn golden_prior(config: &ConfigFile) -> Option<(GoldenChoice, String)> {
    let kind = configured_kind(config)?;
    let label = match (kind, config.golden.base_image.as_deref()) {
        (GoldenChoice::Official, _) => {
            "official — the packed official GitHub runner golden".to_owned()
        }
        (_, Some(base_image)) => format!("{} — {base_image}", kind.as_str()),
        (_, None) => kind.as_str().to_owned(),
    };
    Some((kind, label))
}

fn configured_kind(config: &ConfigFile) -> Option<GoldenChoice> {
    config
        .golden
        .kind
        .as_deref()
        .and_then(GoldenChoice::from_str)
}

/// The stored golden as a decision, for a re-run that keeps step 2: nothing is
/// re-verified or rebuilt, so this must not pretend to know more than the
/// config says (`arch_ok` unknown, sizes only where a local path can be
/// measured).
fn decision_from_config(config: &ConfigFile, _answer: &GoldenAnswer) -> Option<GoldenDecision> {
    let (kind, label) = golden_prior(config)?;
    let base_image = config.golden.base_image.clone();
    let artifact_bytes = base_image
        .as_deref()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len());
    Some(GoldenDecision {
        kind,
        base_image,
        dockerfile: config.golden.dockerfile.clone(),
        label,
        artifact_bytes,
        arch_ok: None,
        warnings: Vec::new(),
        detail: json!({"unchanged": true}),
    })
}

// -- host capabilities ----------------------------------------------------

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn find_on_path(binary: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(binary))
        .find(|candidate| is_executable_file(candidate))
}

/// The container engine that can build a Dockerfile, if any.
fn container_engine() -> Option<&'static str> {
    ["docker", "nerdctl"]
        .into_iter()
        .find(|candidate| find_on_path(candidate).is_some())
}

fn default_dockerfile() -> PathBuf {
    PathBuf::from("Dockerfile")
}

/// The directory the context defaults to: docker's own convention, the
/// Dockerfile's parent.
fn dockerfile_context(dockerfile: &Path) -> PathBuf {
    dockerfile
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

/// Why the Dockerfile choice is unavailable here, or `None` when it can run.
fn dockerfile_unavailable_reason(dockerfile: &Path) -> Option<String> {
    if !dockerfile.is_file() {
        return Some(format!("no Dockerfile at {}", dockerfile.display()));
    }
    if container_engine().is_none() {
        return Some("docker (or nerdctl) is not on PATH".to_owned());
    }
    None
}

/// Whether this host can boot microVMs, and what provides that.
fn hypervisor_state() -> (bool, String) {
    #[cfg(target_os = "macos")]
    {
        match std::process::Command::new("sysctl")
            .args(["-n", "kern.hv_support"])
            .output()
        {
            Ok(output) if output.status.success() => {
                let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                (
                    value == "1",
                    format!("Hypervisor.framework (kern.hv_support={value})"),
                )
            }
            Ok(output) => (
                false,
                format!("Hypervisor.framework (sysctl exited {})", output.status),
            ),
            Err(error) => (
                false,
                format!("Hypervisor.framework (sysctl unavailable: {error})"),
            ),
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Opening read-write checks the permission bit too, not just existence:
        // a /dev/kvm the user cannot open is a host that cannot boot a VM.
        return match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
        {
            Ok(_) => (true, "/dev/kvm".to_owned()),
            Err(error) => (false, format!("/dev/kvm ({error})")),
        };
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (
            false,
            format!("no microVM hypervisor on {}", std::env::consts::OS),
        )
    }
}

// -- probe ----------------------------------------------------------------

/// `preloop init --probe --json`: a side-effect-free capability report. Nothing
/// here writes, creates, or starts anything.
async fn print_probe() -> anyhow::Result<()> {
    let data_root = preloop_vm::machine_data_root();
    let disk_free_bytes = data_root
        .as_deref()
        .and_then(|root| preloop_vm::filesystem_available_bytes(root).ok());
    let (hypervisor, hypervisor_detail) = hypervisor_state();
    let (config, config_error) = match load_config() {
        Ok(config) => (config, None),
        Err(error) => (ConfigFile::default(), Some(format!("{error:#}"))),
    };
    let config_path = preloop_runner_server::config::config_path();
    let docker = container_engine();
    let dockerfile_detected = default_dockerfile().is_file();
    let base_image = preloop_runner_server::config::golden_base_image(&config);
    let report = json!({
        "arch": std::env::consts::ARCH,
        "os": std::env::consts::OS,
        "disk_free_bytes": disk_free_bytes,
        "data_root": data_root.as_deref().map(|root| root.display().to_string()),
        "hypervisor": hypervisor,
        "hypervisor_detail": hypervisor_detail,
        "docker": docker,
        "dockerfile_detected": dockerfile_detected,
        "smolvm": find_on_path("smolvm").is_some(),
        "existing_config": config_path.exists(),
        "config_path": config_path.display().to_string(),
        "credentials_configured": credential_summary(&config).is_some(),
        "ghcr_reachable": ghcr_reachable().await,
        "golden": {
            "kind": configured_kind(&config).map(GoldenChoice::as_str),
            // The base image `serve` would use right now: the environment
            // variable when set, else the persisted choice; null means
            // nothing is configured, i.e. the packed official golden.
            "base_image": base_image,
            "env_override": base_image_override(),
        },
        "config_error": config_error,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// One short request; any HTTP answer means the registry is reachable, only a
/// transport failure or timeout means it is not.
async fn ghcr_reachable() -> bool {
    let Ok(client) = probe_client() else {
        return false;
    };
    let Ok(response) = client.get("https://ghcr.io/v2/").send().await else {
        return false;
    };
    // `send` resolves once headers arrive, so the body is never read here.
    response.status().is_success() || response.status().is_client_error()
}

fn probe_client() -> anyhow::Result<reqwest::Client> {
    preloop_runner_server::shared_http::with_tls_roots(reqwest::Client::builder())
        .user_agent("preloop-init")
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()
        .context("building the HTTP client")
}

// -- OCI references -------------------------------------------------------

fn oci_client() -> anyhow::Result<reqwest::Client> {
    preloop_runner_server::shared_http::with_tls_roots(reqwest::Client::builder())
        .user_agent("preloop-init")
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()
        .context("building the registry client")
}

/// Resolve the reference anonymously and report what it holds, so a typo or a
/// private image is caught before it reaches the config.
///
/// The shared client does the anonymous handshake and one level of index
/// indirection; this function only decides how to present what came back.
async fn resolve_manifest(
    reference: &OciReference,
    client: &reqwest::Client,
) -> anyhow::Result<ManifestProbe> {
    let resolved = get_manifest(
        client,
        reference,
        MANIFEST_ACCEPT,
        Some(&platform_matches_host),
    )
    .await
    .map_err(|error| manifest_error(reference, error))?;
    let mut probe = summarize_manifest(&resolved.top);
    probe.host_manifest_digest = resolved.selected_digest;
    // A multi-platform index names the per-architecture manifest by digest;
    // following it is what turns "these platforms exist" into the real
    // download size.
    if probe.host_manifest_digest.is_some() {
        probe.layers_bytes = layer_bytes(&resolved.image);
    }
    Ok(probe)
}

/// Present a shared registry failure the way `init` promises scripts: a
/// refusal gets the SmolVM registry hint, a missing manifest names the
/// repository, and everything else keeps its own words.
fn manifest_error(reference: &OciReference, error: OciError) -> anyhow::Error {
    match error {
        OciError::Unauthorized => build_error(private_registry_message(&reference.display())),
        OciError::NotFound => build_error(format!(
            "{} was not found in {} (check the repository and tag)",
            reference.display(),
            reference.registry
        )),
        OciError::Status(status) => build_error(format!(
            "{} answered {status} for {}",
            reference.registry,
            reference.display()
        )),
        OciError::ManifestUnreadable(error) => build_error(format!(
            "{} returned a manifest this client cannot read ({error})",
            reference.registry
        )),
        OciError::Message(message) => build_error(message),
    }
}

/// A registry that hides private repositories answers 401 (sometimes 403) for
/// a name that does not exist too, so the message must offer both readings
/// instead of sending every typo to the registry-login command.
fn private_registry_message(reference: &str) -> String {
    format!(
        "{reference} refused an anonymous manifest request — it is private, or no such repository \
         exists under that name. Private registries authenticate through SmolVM: add the login with \
         `smolvm config registries edit` and retry `preloop init`. Those credentials are used for \
         the pull only and never enter the VM."
    )
}

#[derive(Debug, Default)]
struct ManifestProbe {
    media_type: Option<String>,
    /// `os/arch` of every entry in a multi-platform index, for the report.
    platforms: Vec<String>,
    /// Compressed bytes of the manifest's layers, when they are known.
    layers_bytes: Option<u64>,
    /// Digest of the index entry matching this host, when it selected one.
    host_manifest_digest: Option<String>,
}

impl ManifestProbe {
    /// `None` when the reference is a single manifest and says nothing about
    /// platforms (manifests carry no platform field).
    fn supports_host_arch(&self) -> Option<bool> {
        if self.platforms.is_empty() {
            return None;
        }
        Some(
            self.platforms
                .iter()
                .any(|platform| platform_matches_host(platform)),
        )
    }
}

/// Read the fields `init` reports from an index or single manifest. The
/// per-host selection itself is the shared client's, so the digest it chose is
/// filled in by the caller.
fn summarize_manifest(value: &Value) -> ManifestProbe {
    let media_type = value
        .get("mediaType")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut probe = ManifestProbe {
        media_type,
        layers_bytes: layer_bytes(value),
        ..ManifestProbe::default()
    };
    if let Some(entries) = value.get("manifests").and_then(Value::as_array) {
        for entry in entries {
            let Some(platform) = entry.get("platform") else {
                continue;
            };
            let (Some(os), Some(arch)) = (
                platform.get("os").and_then(Value::as_str),
                platform.get("architecture").and_then(Value::as_str),
            ) else {
                continue;
            };
            probe.platforms.push(format!("{os}/{arch}"));
        }
        probe.platforms.sort();
        probe.platforms.dedup();
        // An index carries no layers of its own: the number only becomes real
        // once the per-architecture manifest has been fetched.
        probe.layers_bytes = None;
    }
    probe
}

fn layer_bytes(value: &Value) -> Option<u64> {
    let layers = value.get("layers")?.as_array()?;
    let total: u64 = layers
        .iter()
        .filter_map(|layer| layer.get("size").and_then(Value::as_u64))
        .sum();
    (total > 0).then_some(total)
}

// -- Dockerfile -----------------------------------------------------------

/// A Dockerfile built into the local image tar that will be the base image.
struct BuiltBase {
    tar: PathBuf,
    dockerfile: PathBuf,
    context: PathBuf,
    tag: String,
    bytes: u64,
}

/// `docker build` the context, then `docker save` it to a tar under the
/// preloop home. The tar *is* the base image: smolvm image inputs are registry
/// references, docker-save tars, and packs, and only a `.tar` name routes to
/// its `--image` branch.
fn build_base_tar(
    engine: &str,
    dockerfile: &Path,
    context: &Path,
    json: bool,
) -> anyhow::Result<BuiltBase> {
    let dockerfile = std::fs::canonicalize(dockerfile)
        .with_context(|| format!("resolving the Dockerfile {}", dockerfile.display()))?;
    let context = std::fs::canonicalize(context)
        .with_context(|| format!("resolving the build context {}", context.display()))?;
    let contents =
        std::fs::read(&dockerfile).with_context(|| format!("reading {}", dockerfile.display()))?;
    // The tag is content-addressed on the Dockerfile so successive runs of the
    // same file reuse one name instead of littering the daemon; the image
    // itself is rebuilt every run, so a changed context is never stale.
    let tag = format!("preloop-init:{}", short_hash(&contents));
    let goldens = preloop_home().join("goldens");
    std::fs::create_dir_all(&goldens).with_context(|| format!("creating {}", goldens.display()))?;
    let tar = goldens.join(format!("{}.tar", tag.replace(':', "-")));

    run_container(
        std::process::Command::new(engine)
            .arg("build")
            .arg("--file")
            .arg(&dockerfile)
            .arg("--tag")
            .arg(&tag)
            .arg(&context),
        json,
        &format!("{engine} build"),
    )?;
    run_container(
        std::process::Command::new(engine)
            .arg("save")
            .arg("--output")
            .arg(&tar)
            .arg(&tag),
        json,
        &format!("{engine} save"),
    )?;
    let bytes = std::fs::metadata(&tar)
        .with_context(|| format!("reading the saved image {}", tar.display()))?
        .len();
    Ok(BuiltBase {
        tar,
        dockerfile,
        context,
        tag,
        bytes,
    })
}

/// Ask the image the engine just built what architecture it is (`inspect`
/// takes an image reference, not a saved tar). `None` means the engine could
/// not say — never a claim about the image.
fn built_image_arch(engine: &str, tag: &str, json: bool) -> Option<bool> {
    let output = std::process::Command::new(engine)
        .args(["image", "inspect", "--format", "{{.Architecture}}"])
        .arg(tag)
        .output()
        .ok()?;
    if !output.status.success() {
        if json {
            eprintln!(
                "preloop init: `{engine} image inspect {tag}` could not report the architecture"
            );
        }
        return None;
    }
    let arch = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if arch.is_empty() {
        return None;
    }
    Some(host_arch_aliases().contains(&arch.as_str()))
}

/// Run a container-engine command. In `--json` mode its output is captured and
/// echoed on stderr, so the one-object-per-line contract on stdout survives a
/// long build log.
fn run_container(
    command: &mut std::process::Command,
    json: bool,
    what: &str,
) -> anyhow::Result<()> {
    if !json {
        let status = command
            .status()
            .with_context(|| format!("running `{what}`"))?;
        if !status.success() {
            return Err(build_error(format!("`{what}` failed ({status})")));
        }
        return Ok(());
    }
    let output = command
        .output()
        .with_context(|| format!("running `{what}`"))?;
    for stream in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(stream);
        if !text.trim().is_empty() {
            eprint!("{text}");
            if !text.ends_with('\n') {
                eprintln!();
            }
        }
    }
    if !output.status.success() {
        return Err(build_error(format!("`{what}` failed ({})", output.status)));
    }
    Ok(())
}

fn short_hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn github_pat_env() -> Option<String> {
    std::env::var("PRELOOP_GITHUB_PAT")
        .ok()
        .filter(|value| !value.is_empty())
}

// -- stdout capture -------------------------------------------------------

/// Run `future` with this process's stdout redirected into a temporary file,
/// and return what it printed there. Used so the reused `preloop setup` flow's
/// human output cannot corrupt `--json`'s one-object-per-line contract.
async fn capture_stdout<F: Future>(future: F) -> (F::Output, String) {
    #[cfg(unix)]
    {
        let Ok(capture) = StdoutCapture::begin() else {
            return (future.await, String::new());
        };
        let output = future.await;
        (output, capture.finish())
    }
    #[cfg(not(unix))]
    {
        (future.await, String::new())
    }
}

/// Restores fd 1 on drop, so an unwinding future cannot leave the process
/// writing its output into the capture file.
#[cfg(unix)]
struct StdoutCapture {
    saved: libc::c_int,
    file: std::fs::File,
    active: bool,
}

#[cfg(unix)]
impl StdoutCapture {
    fn begin() -> anyhow::Result<Self> {
        use std::os::fd::AsRawFd;
        std::io::stdout().flush().context("flushing stdout")?;
        let file = tempfile::tempfile().context("creating the capture file")?;
        // SAFETY: dup/dup2 operate on this process's own descriptor table, and
        // the guard restores fd 1 when it is dropped.
        let saved = unsafe { libc::dup(1) };
        if saved < 0 {
            anyhow::bail!("dup(1) failed");
        }
        if unsafe { libc::dup2(file.as_raw_fd(), 1) } < 0 {
            unsafe { libc::close(saved) };
            anyhow::bail!("dup2(2) failed");
        }
        Ok(Self {
            saved,
            file,
            active: true,
        })
    }

    fn finish(mut self) -> String {
        self.restore();
        use std::io::{Read, Seek, SeekFrom};
        let mut text = String::new();
        if self.file.seek(SeekFrom::Start(0)).is_ok() {
            let _ = self.file.read_to_string(&mut text);
        }
        text
    }

    fn restore(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let _ = std::io::stdout().flush();
        // SAFETY: same descriptor table; `saved` is the fd dup'd in `begin`.
        unsafe {
            libc::dup2(self.saved, 1);
            libc::close(self.saved);
        }
    }
}

#[cfg(unix)]
impl Drop for StdoutCapture {
    fn drop(&mut self) {
        self.restore();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_interactive_run_requires_every_step_flag() {
        let session = Session {
            interactive: false,
            json: true,
            yes: true,
            from_serve: false,
        };
        // Nothing supplied: every step flag is named, so a script knows what to
        // add instead of guessing.
        let error = session
            .resolve_from_flags(&InitArgs::default())
            .unwrap_err();
        assert_eq!(exit_code(&error), Some(EXIT_USAGE));
        let message = error.to_string();
        for flag in ["--auth", "--golden", "--mode"] {
            assert!(message.contains(flag), "{message}");
        }
    }

    #[test]
    fn non_interactive_oci_without_a_reference_names_the_flag() {
        let session = Session {
            interactive: false,
            json: false,
            yes: false,
            from_serve: false,
        };
        let args = InitArgs {
            auth: Some(AuthChoice::None),
            golden: Some(GoldenChoice::Oci),
            mode: Some(RunMode::Configure),
            ..InitArgs::default()
        };
        let error = session.resolve_from_flags(&args).unwrap_err();
        assert_eq!(exit_code(&error), Some(EXIT_USAGE));
        assert!(error.to_string().contains("--base-image"), "{error}");
    }

    #[test]
    fn a_flag_from_another_kind_is_a_usage_error() {
        let session = Session {
            interactive: false,
            json: false,
            yes: false,
            from_serve: false,
        };
        let args = InitArgs {
            auth: Some(AuthChoice::None),
            golden: Some(GoldenChoice::Official),
            base_image: Some("ghcr.io/x/y:1".to_owned()),
            mode: Some(RunMode::Configure),
            ..InitArgs::default()
        };
        let error = session.resolve_from_flags(&args).unwrap_err();
        assert_eq!(exit_code(&error), Some(EXIT_USAGE));
        assert!(error.to_string().contains("--base-image"), "{error}");
    }

    #[test]
    fn index_platforms_report_whether_the_host_is_supported() {
        let index = json!({
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {"digest": "sha256:1", "platform": {"os": "linux", "architecture": "amd64"}},
                {"digest": "sha256:2", "platform": {"os": "linux", "architecture": "arm64"}},
                {"digest": "sha256:3", "platform": {"os": "windows", "architecture": "amd64"}},
            ]
        });
        let probe = summarize_manifest(&index);
        assert_eq!(
            probe.platforms,
            vec!["linux/amd64", "linux/arm64", "windows/amd64"]
        );
        assert_eq!(probe.supports_host_arch(), Some(true));
        // An index carries no layers of its own; the per-host digest the shared
        // client selected is what turns it into a real size.
        assert_eq!(probe.layers_bytes, None);
        assert_eq!(probe.host_manifest_digest, None);

        let foreign = json!({
            "manifests": [
                {"digest": "sha256:1", "platform": {"os": "linux", "architecture": "s390x"}}
            ]
        });
        assert_eq!(
            summarize_manifest(&foreign).supports_host_arch(),
            Some(false)
        );
    }

    #[test]
    fn a_single_manifest_says_nothing_about_architecture() {
        let manifest = json!({
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "layers": [{"size": 100}, {"size": 23}]
        });
        let probe = summarize_manifest(&manifest);
        assert_eq!(probe.layers_bytes, Some(123));
        assert_eq!(
            probe.supports_host_arch(),
            None,
            "a single manifest carries no platform field, so arch support is unknown"
        );
    }

    #[test]
    fn the_dockerfile_choice_names_why_it_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("Dockerfile");
        let reason = dockerfile_unavailable_reason(&missing).unwrap();
        assert!(reason.contains("no Dockerfile"), "{reason}");

        std::fs::write(&missing, "FROM scratch\n").unwrap();
        // The reason, when there is one, must be about the engine instead.
        match dockerfile_unavailable_reason(&missing) {
            Some(reason) => assert!(reason.contains("docker"), "{reason}"),
            None => assert!(container_engine().is_some()),
        }
    }

    #[test]
    fn mismatched_flags_are_usage_errors_not_silent_ignores() {
        let args = InitArgs {
            path: Some(PathBuf::from("/tmp/base.smolmachine")),
            ..InitArgs::default()
        };
        let error = reject_mismatched_flags(GoldenChoice::Oci, &args).unwrap_err();
        assert_eq!(exit_code(&error), Some(EXIT_USAGE));
        assert!(error.to_string().contains("--path"), "{error}");
        assert!(reject_mismatched_flags(GoldenChoice::File, &args).is_ok());
    }

    #[test]
    fn service_mode_refuses_to_install_without_root() {
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let error = ensure_service_root().unwrap_err();
        assert!(error.to_string().contains("root"), "{error}");
    }

    /// The probe's contract: a JSON object whose keys a script can rely on.
    #[test]
    fn probe_reports_the_base_image_serve_would_use() {
        // `golden_base_image` is the single resolver `serve` reads, so pinning
        // its precedence here is what makes the probe's answer trustworthy.
        let mut config = ConfigFile::default();
        assert_eq!(
            preloop_runner_server::config::golden_base_image(&config),
            None
        );
        config.golden = GoldenConfig {
            kind: Some("oci".to_owned()),
            base_image: Some("ghcr.io/x/y:1".to_owned()),
            dockerfile: None,
        };
        assert_eq!(
            preloop_runner_server::config::golden_base_image(&config).as_deref(),
            Some("ghcr.io/x/y:1")
        );
    }

    /// The official choice is `kind` alone — no base image — and that is a
    /// complete answer: a re-run keeps it without re-asking, and nothing
    /// pretends a local bake is in its future (the engine downloads the
    /// pinned packed golden; a failed download fails the job).
    #[test]
    fn the_stored_official_golden_is_a_complete_answer() {
        let config = ConfigFile {
            golden: GoldenConfig {
                kind: Some(GoldenChoice::Official.as_str().to_owned()),
                base_image: None,
                dockerfile: None,
            },
            ..ConfigFile::default()
        };
        let (kind, label) = golden_prior(&config).expect("the stored golden must be offered");
        assert_eq!(kind, GoldenChoice::Official);
        assert!(label.contains("official"), "{label}");

        let decision = decision_from_config(&config, &GoldenAnswer::of(GoldenChoice::Official))
            .expect("the stored golden must be a complete answer");
        assert_eq!(decision.kind, GoldenChoice::Official);
        assert_eq!(decision.base_image, None);
        assert_eq!(
            decision.artifact_bytes, None,
            "the official golden is downloaded, not staged from a local artifact"
        );
    }

    #[test]
    fn dockerfile_context_defaults_to_the_dockerfile_directory() {
        assert_eq!(
            dockerfile_context(Path::new("Dockerfile")),
            PathBuf::from(".")
        );
        assert_eq!(
            dockerfile_context(Path::new("ci/Dockerfile")),
            PathBuf::from("ci")
        );
    }

    #[test]
    fn an_archive_over_smolvm_local_image_limit_warns() {
        assert!(
            oversized_archive_warning(SMOLVM_MAX_LOCAL_IMAGE_BYTES, "the saved image tar")
                .is_none(),
            "the limit itself is accepted; only bytes above it are refused"
        );
        let warning =
            oversized_archive_warning(SMOLVM_MAX_LOCAL_IMAGE_BYTES + 1, "the saved image tar")
                .expect("one byte over the limit must warn");
        assert!(warning.contains("SMOLVM_MAX_IMAGE_BYTES"), "{warning}");
        assert!(warning.contains("8.0 GiB"), "{warning}");
    }

    #[test]
    fn short_hash_is_stable_and_short() {
        let a = short_hash(b"FROM scratch\n");
        assert_eq!(a, short_hash(b"FROM scratch\n"));
        assert_eq!(a.len(), 24);
        assert_ne!(a, short_hash(b"FROM other\n"));
    }

    #[test]
    fn public_url_is_https_public_and_normalized() {
        assert_eq!(
            validate_public_url("https://engine.example.com").unwrap(),
            "https://engine.example.com"
        );
        assert_eq!(
            validate_public_url(" https://engine.example.com/ ").unwrap(),
            "https://engine.example.com"
        );
        assert_eq!(
            validate_public_url("https://Engine.Example.com:443/preloop/").unwrap(),
            "https://engine.example.com/preloop"
        );
        for refused in [
            "http://engine.example.com",
            "engine.example.com",
            "https://",
            "https://localhost:9090",
            "https://api.localhost",
            "https://127.0.0.1",
            "https://0.0.0.0",
            "https://[::1]",
            "https://user:secret@engine.example.com",
            "https://engine.example.com/?token=1",
            "https://engine.example.com/#fragment",
        ] {
            assert!(
                validate_public_url(refused).is_err(),
                "{refused} must be refused"
            );
        }
    }

    #[test]
    fn the_public_url_question_never_prompts_without_a_terminal() {
        let mut session = Session {
            interactive: false,
            json: true,
            yes: false,
            from_serve: false,
        };
        assert_eq!(
            session.public_url_answer(None, None).unwrap(),
            PublicUrlAnswer {
                url: None,
                source: "skipped"
            }
        );
        // A loopback PRELOOP_PUBLIC_URL (the local-dev default) is not used.
        assert_eq!(
            session
                .public_url_answer(None, Some("http://127.0.0.1:9090"))
                .unwrap()
                .source,
            "skipped"
        );
        assert_eq!(
            session
                .public_url_answer(None, Some(" https://engine.example.com/ "))
                .unwrap(),
            PublicUrlAnswer {
                url: Some("https://engine.example.com".to_owned()),
                source: "env"
            }
        );
        assert_eq!(
            session
                .public_url_answer(
                    Some("https://flag.example.com/"),
                    Some("https://env.example.com")
                )
                .unwrap(),
            PublicUrlAnswer {
                url: Some("https://flag.example.com".to_owned()),
                source: "flag"
            },
            "the flag wins over the environment"
        );
    }

    #[test]
    fn an_invalid_public_url_flag_is_a_usage_error() {
        let error = validate_public_url("http://engine.example.com")
            .map_err(usage_error)
            .unwrap_err();
        assert_eq!(exit_code(&error), Some(EXIT_USAGE));
        assert!(error.to_string().contains("https://"), "{error}");
    }

    /// The chosen URL is not only what the App's webhook gets: the run mode
    /// this run starts must carry it too, or Check Run detail links and the
    /// webhook health check stay on the loopback default. The kept-credential
    /// path never reaches `configure_credentials`, so it resolves the URL from
    /// `--public-url`/`PRELOOP_PUBLIC_URL` there.
    #[test]
    #[allow(unsafe_code)]
    fn the_chosen_url_reaches_serve_and_the_service_install() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let mut args = InitArgs {
            public_url: Some(" https://flag.example.com/ ".to_owned()),
            ..InitArgs::default()
        };
        // No App answer (PAT, `--auth none`, or a kept credential): the flag.
        assert_eq!(
            engine_public_url(&args, None).as_deref(),
            Some("https://flag.example.com")
        );
        assert_eq!(
            serve_args_for(engine_public_url(&args, None).as_deref())
                .public_url
                .as_deref(),
            Some("https://flag.example.com"),
            "the foreground engine must start with the URL GitHub was given"
        );
        // Without one, `serve` keeps its own fallback (PRELOOP_PUBLIC_URL, then
        // loopback): nothing here may invent an address for it.
        assert_eq!(serve_args_for(None).public_url, None);

        // The App flow's answer — possibly accepted from the prompt — wins.
        let answer = PublicUrlAnswer {
            url: Some("https://tail.example.ts.net".to_owned()),
            source: "suggested",
        };
        assert_eq!(
            engine_public_url(&args, Some(&answer)).as_deref(),
            Some("https://tail.example.ts.net")
        );
        // A skipped answer is still an answer: webhooks stay off on purpose.
        let skipped = PublicUrlAnswer {
            url: None,
            source: "skipped",
        };
        assert_eq!(engine_public_url(&args, Some(&skipped)), None);

        // The kept-credential path has no App answer to hand over, so the
        // environment stands in for the flag there.
        args.public_url = None;
        let previous = std::env::var_os("PRELOOP_PUBLIC_URL");
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access; the
        // value read above is restored below.
        unsafe { std::env::set_var("PRELOOP_PUBLIC_URL", "https://env.example.com/") };
        assert_eq!(
            engine_public_url(&args, None).as_deref(),
            Some("https://env.example.com"),
            "a kept credential still needs the engine's public origin"
        );
        // The local-dev loopback default is not an origin GitHub can use.
        // SAFETY: as above.
        unsafe { std::env::set_var("PRELOOP_PUBLIC_URL", "http://127.0.0.1:9090") };
        assert_eq!(engine_public_url(&args, None), None);
        // SAFETY: restores the value read above.
        unsafe {
            match previous {
                Some(value) => std::env::set_var("PRELOOP_PUBLIC_URL", value),
                None => std::env::remove_var("PRELOOP_PUBLIC_URL"),
            }
        }
    }

    /// A suggestion is offered from evidence, never guessed:
    /// `tailscale status --json`'s own name for this machine while the tailnet
    /// is up — and, for the Enter default, `tailscale funnel status --json`
    /// publishing the webhook path there and nothing else.
    #[test]
    fn a_running_tailnet_supplies_the_suggested_url() {
        let status = json!({
            "BackendState": "Running",
            "MagicDNSSuffix": "tail1234.ts.net",
            "Self": {
                "ID": "n1234",
                "HostName": "builder",
                // Tailscale reports an FQDN, trailing dot included.
                "DNSName": "builder.tail1234.ts.net.",
                "OS": "linux",
            },
            "Peer": {},
        });
        assert_eq!(
            tailnet_public_url(&status).as_deref(),
            Some("https://builder.tail1234.ts.net")
        );
        let published = json!({
            "TCP": {"443": true},
            "Web": {
                "builder.tail1234.ts.net:443": {
                    "Handlers": {
                        "/api/v1/github/webhooks": {
                            "Proxy": "http://127.0.0.1:9090/api/v1/github/webhooks"
                        }
                    }
                }
            },
            "AllowFunnel": {"builder.tail1234.ts.net:443": true},
        });
        let suggestion = tailnet_suggestion(&status, Some(&published))
            .expect("a running tailnet supplies a suggestion");
        assert_eq!(suggestion.url, "https://builder.tail1234.ts.net");
        assert_eq!(
            suggestion.funnel,
            FunnelPublication::WebhookPathOnly,
            "Funnel publishes only the webhook path, so GitHub can reach it"
        );
        assert_eq!(
            read_public_url("", Some(&suggestion)),
            UrlInput::Use {
                url: "https://builder.tail1234.ts.net".to_owned(),
                source: "suggested"
            },
            "Enter accepts a published Funnel name"
        );
    }

    /// `{"<key>": value}` with a computed key: the Funnel status maps are keyed
    /// by `<host>:443`, which `json!` cannot write as a literal.
    fn one_key(key: String, value: Value) -> Value {
        let mut map = serde_json::Map::new();
        map.insert(key, value);
        Value::Object(map)
    }

    /// Anything short of "Funnel publishes the webhook path, and nothing else
    /// on that host:port" is a hint only: Enter must not point the App's
    /// webhook at an address GitHub cannot reach, and Funnel makes *every*
    /// mount on its host and port public, so a shared one is not safe either.
    #[test]
    fn only_a_path_scoped_funnel_lets_enter_accept_the_tailnet_name() {
        let status = json!({
            "BackendState": "Running",
            "Self": {"DNSName": "builder.tail1234.ts.net."},
        });
        let host = "builder.tail1234.ts.net";
        let handlers = json!({
            "/api/v1/github/webhooks": {"Proxy": "http://127.0.0.1:9090/api/v1/github/webhooks"}
        });
        // The shape a working `tailscale funnel --set-path=… --bg …` writes:
        // Funnel allowed for this host:port, and the webhook path mounted.
        let published = json!({
            "TCP": {"443": true},
            "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
            "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
        });
        assert_eq!(
            funnel_publication(&published, host),
            FunnelPublication::WebhookPathOnly
        );
        for (what, funnel, expected) in [
            (
                "plain Serve: the handlers exist but Funnel is not allowed for the host:port",
                json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                }),
                FunnelPublication::NotPublished,
            ),
            (
                "a Funnel for another path only",
                json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": {"/": {"Proxy": "http://127.0.0.1:3000"}}})),
                    "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                }),
                FunnelPublication::NotPublished,
            ),
            (
                "a Funnel for another host:port",
                json!({
                    "Web": one_key("other.tail1234.ts.net:443".to_owned(), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key("other.tail1234.ts.net:443".to_owned(), json!(true)),
                }),
                FunnelPublication::NotPublished,
            ),
            (
                "a port-less key, which is the same 443",
                json!({
                    "Web": one_key(host.to_owned(), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(host.to_owned(), json!(true)),
                }),
                FunnelPublication::WebhookPathOnly,
            ),
            (
                "the FQDN form of the same name and port",
                json!({
                    "Web": one_key(format!("{host}.:443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}.:443"), json!(true)),
                }),
                FunnelPublication::WebhookPathOnly,
            ),
            (
                "another port is a different address",
                json!({
                    "Web": one_key(format!("{host}:8443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}:8443"), json!(true)),
                }),
                FunnelPublication::NotPublished,
            ),
            (
                // A `--bg`-less session of another serve/funnel command. Its
                // handlers are consulted before the persistent ones for this
                // host:port, so the persistent "webhook path only" is not what
                // is public.
                "a foreground session mounts another path on this host:port",
                json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                    "Foreground": one_key("session-1".to_owned(), json!({
                        "Web": one_key(format!("{host}:443"), json!({"Handlers": {
                            "/api/v1/github/webhooks": {"Proxy": "http://127.0.0.1:9090/api/v1/github/webhooks"},
                            "/": {"Proxy": "http://127.0.0.1:3000"},
                        }})),
                        "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                    })),
                }),
                FunnelPublication::ForegroundSession,
            ),
            (
                // The foreground session carries no Funnel flag of its own,
                // but the persistent one already allows Funnel for this
                // host:port and the foreground handlers serve it: those mounts
                // are public too.
                "a foreground mount here under the persistent Funnel flag",
                json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                    "Foreground": one_key("session-1".to_owned(), json!({
                        "Web": one_key(format!("{host}:443"), json!({"Handlers": {
                            "/": {"Proxy": "http://127.0.0.1:3000"},
                        }})),
                    })),
                }),
                FunnelPublication::ForegroundSession,
            ),
            (
                // Even a foreground publication of the webhook path alone is
                // session-scoped: it is gone when that command exits, and the
                // App's webhook needs an address that outlives it.
                "a foreground Funnel publishes the webhook path by itself",
                json!({
                    "Foreground": one_key("session-1".to_owned(), json!({
                        "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                        "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                    })),
                }),
                FunnelPublication::ForegroundSession,
            ),
            (
                "a shape this version does not write",
                json!({"AllowFunnel": ["builder.tail1234.ts.net:443"]}),
                FunnelPublication::NotPublished,
            ),
            ("nothing at all", json!({}), FunnelPublication::NotPublished),
            (
                "no JSON object at all",
                json!("denied"),
                FunnelPublication::NotPublished,
            ),
            (
                // A foreground session on another host:port is a different
                // address: it changes nothing here, so the persistent verdict
                // still stands.
                "a foreground session on another host:port",
                json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                    "Foreground": one_key("session-1".to_owned(), json!({
                        "Web": one_key("other.tail1234.ts.net:443".to_owned(), json!({"Handlers": {
                            "/": {"Proxy": "http://127.0.0.1:3000"},
                        }})),
                        "AllowFunnel": one_key("other.tail1234.ts.net:443".to_owned(), json!(true)),
                    })),
                }),
                FunnelPublication::WebhookPathOnly,
            ),
        ] {
            assert_eq!(funnel_publication(&funnel, host), expected, "{what}");
        }

        // Without a Funnel answer (absent, a version without the flag, or a
        // timeout) the name is still a hint, and Enter must not accept it.
        for funnel in [None, Some(json!("denied"))] {
            let suggestion = tailnet_suggestion(&status, funnel.as_ref())
                .expect("the tailnet name is still worth showing");
            assert_eq!(suggestion.funnel, FunnelPublication::NotPublished);
            assert_eq!(
                read_public_url("", Some(&suggestion)),
                UrlInput::Skip,
                "Enter must not accept a name GitHub cannot reach"
            );
            assert!(
                !public_url_prompt(Some(&suggestion)).contains('['),
                "an unusable name must not look like a suggestion"
            );
        }

        // The webhook path *is* published, but something else shares the
        // host:port — Funnel makes that public too, so the operator is told
        // which paths would be exposed instead of being offered the name.
        let shared = json!({
            "Web": one_key(format!("{host}:443"), json!({"Handlers": {
                "/api/v1/github/webhooks": {"Proxy": "http://127.0.0.1:9090/api/v1/github/webhooks"},
                "/": {"Proxy": "http://127.0.0.1:3000"},
            }})),
            "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
        });
        let suggestion = tailnet_suggestion(&status, Some(&shared)).expect("still a hint");
        assert_eq!(
            suggestion.funnel,
            FunnelPublication::WebhookPathAndOthers(vec!["/".to_owned()])
        );
        assert_eq!(read_public_url("", Some(&suggestion)), UrlInput::Skip);
        let help = public_url_help(Some(&suggestion)).join("\n");
        assert!(help.contains("builder.tail1234.ts.net"), "{help}");
        assert!(help.contains("whole host and port"), "{help}");
        assert!(help.contains("dedicated hostname"), "{help}");
        assert!(help.contains("tailscale funnel status"), "{help}");
        assert!(
            !help.contains("Enter accepts"),
            "an unsafe name must never be announced as accepted: {help}"
        );
        let prompt = public_url_prompt(Some(&suggestion));
        assert!(prompt.contains("blank = skip"), "{prompt}");
        assert!(
            prompt.contains("also publishes other paths"),
            "Funnel does publish the webhook path here; only the other mounts \
             make it unusable: {prompt}"
        );
        assert!(
            !prompt.contains("not published by Funnel"),
            "the prompt must not contradict `tailscale funnel status`: {prompt}"
        );

        // A `--bg`-less session: Enter must not accept a publication that dies
        // with that command, and the wording must say so instead of claiming
        // nothing is published.
        let foreground = tailnet_suggestion(
            &status,
            Some(&json!({
                "Foreground": one_key("session-1".to_owned(), json!({
                    "Web": one_key(format!("{host}:443"), json!({"Handlers": handlers.clone()})),
                    "AllowFunnel": one_key(format!("{host}:443"), json!(true)),
                })),
            })),
        )
        .expect("still a hint");
        assert_eq!(foreground.funnel, FunnelPublication::ForegroundSession);
        assert_eq!(read_public_url("", Some(&foreground)), UrlInput::Skip);
        let fg_prompt = public_url_prompt(Some(&foreground));
        assert!(
            fg_prompt.contains("foreground serve session"),
            "{fg_prompt}"
        );
        assert!(
            !fg_prompt.contains("not published by Funnel"),
            "a foreground funnel does publish it: {fg_prompt}"
        );
        let fg_help = public_url_help(Some(&foreground)).join("\n");
        assert!(fg_help.contains("foreground"), "{fg_help}");
        assert!(fg_help.contains("--bg"), "{fg_help}");
        assert!(
            !fg_help.contains("no tailscale funnel publishes"),
            "the help must name what is really wrong: {fg_help}"
        );
    }

    #[test]
    fn no_usable_tailscale_answer_means_no_suggestion() {
        for (what, status) in [
            (
                "a stopped daemon still names a node",
                json!({
                    "BackendState": "Stopped",
                    "Self": {"DNSName": "builder.tail1234.ts.net."},
                }),
            ),
            (
                "a logged-out node",
                json!({
                    "BackendState": "NeedsLogin",
                    "Self": {"DNSName": "builder.tail1234.ts.net."},
                }),
            ),
            ("no Self block", json!({"BackendState": "Running"})),
            (
                "a Self without a DNS name",
                json!({"BackendState": "Running", "Self": {"HostName": "builder"}}),
            ),
            (
                "a name GitHub cannot reach",
                json!({"BackendState": "Running", "Self": {"DNSName": "localhost."}}),
            ),
            (
                "an empty name",
                json!({"BackendState": "Running", "Self": {"DNSName": "."}}),
            ),
            ("an empty answer", Value::Null),
        ] {
            assert_eq!(tailnet_public_url(&status), None, "{what}: {status}");
            assert_eq!(tailnet_suggestion(&status, None), None, "{what}: {status}");
        }
    }

    /// A daemon that does not answer is "no answer", never a failure: both the
    /// parse and the deadline have to collapse to `None`.
    #[test]
    fn a_tailscale_query_that_does_not_answer_is_no_suggestion() {
        for (what, answer) in [
            ("an error message where JSON should be", "Exit status 1"),
            ("an empty answer", ""),
            ("truncated JSON", "{\"BackendState\":"),
        ] {
            assert_eq!(parse_tailscale_json(answer), None, "{what}: {answer}");
        }
        // The deadline: a child that outlives it is killed, not awaited, and
        // the query reads as unanswered.
        let mut slow = std::process::Command::new("sh");
        slow.args(["-c", "sleep 30"]);
        let started = std::time::Instant::now();
        assert_eq!(bounded_stdout(&mut slow, Duration::from_millis(200)), None);
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(200),
            "the deadline must be waited out, took {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "an unresponsive daemon must not hang the wizard, took {elapsed:?}"
        );
    }

    /// A bounded query still returns a healthy answer, and a program that
    /// cannot answer is `None` rather than a wizard failure.
    #[test]
    fn a_bounded_query_returns_the_answer_it_can() {
        let run = |program: &str, args: &[&str]| {
            let mut command = std::process::Command::new(program);
            command.args(args);
            bounded_stdout(&mut command, Duration::from_secs(10))
        };
        assert_eq!(run("sh", &["-c", "echo hello"]).as_deref(), Some("hello\n"));
        assert_eq!(
            run("sh", &["-c", "exit 3"]),
            None,
            "a non-zero exit is not an answer"
        );
        assert_eq!(
            run("preloop-no-such-binary", &[]),
            None,
            "a program that cannot be run is not an answer"
        );
    }

    /// The question takes a typed URL, the suggestion through Enter, or `-` to
    /// skip; with no suggestion — or one Enter may not accept — the older
    /// contract (blank = skip) still holds.
    #[test]
    fn the_url_answer_takes_a_typed_url_the_suggestion_or_a_skip() {
        let suggestion = UrlSuggestion {
            url: "https://builder.tail1234.ts.net".to_owned(),
            funnel: FunnelPublication::WebhookPathOnly,
        };
        assert_eq!(
            read_public_url("", Some(&suggestion)),
            UrlInput::Use {
                url: "https://builder.tail1234.ts.net".to_owned(),
                source: "suggested"
            },
            "Enter accepts the suggestion"
        );
        assert_eq!(
            read_public_url("https://ci.example.com/", Some(&suggestion)),
            UrlInput::Use {
                url: "https://ci.example.com".to_owned(),
                source: "prompt"
            }
        );
        assert_eq!(read_public_url("-", Some(&suggestion)), UrlInput::Skip);
        assert_eq!(read_public_url("", None), UrlInput::Skip);
        assert_eq!(read_public_url("-", None), UrlInput::Skip);
        // A superset of the accepted case: a hint must never be taken by Enter.
        let hint = UrlSuggestion {
            url: "https://builder.tail1234.ts.net".to_owned(),
            funnel: FunnelPublication::NotPublished,
        };
        assert_eq!(read_public_url("", Some(&hint)), UrlInput::Skip);
        assert_eq!(read_public_url("-", Some(&hint)), UrlInput::Skip);
        // A refused answer is asked again, and the retry names the way out:
        // `-` when Enter had a way to accept, blank otherwise.
        match read_public_url("http://ci.example.com", Some(&suggestion)) {
            UrlInput::Retry(reason) => {
                assert!(reason.contains("https://"), "{reason}");
                assert!(reason.contains("webhooks off"), "{reason}");
            }
            other => panic!("a non-https URL must be refused, got {other:?}"),
        }
        match read_public_url("nonsense", Some(&hint)) {
            UrlInput::Retry(reason) => assert!(reason.contains("blank"), "{reason}"),
            other => panic!("garbage must be refused, got {other:?}"),
        }
        match read_public_url("nonsense", None) {
            UrlInput::Retry(reason) => assert!(reason.contains("blank"), "{reason}"),
            other => panic!("garbage must be refused, got {other:?}"),
        }
    }

    /// The how-to shown at the prompt: the exact path-scoped Funnel command,
    /// where the full guide is, and — when one was detected — the URL itself,
    /// so an accepted suggestion is never mistaken for a guess. The Funnel
    /// wording must not promise that mounting one path keeps the rest private
    /// (Funnel publishes the whole host and port, tailscale/tailscale#10234).
    #[test]
    fn the_url_prompt_shows_the_funnel_command_and_the_suggestion() {
        let published = UrlSuggestion {
            url: "https://builder.tail1234.ts.net".to_owned(),
            funnel: FunnelPublication::WebhookPathOnly,
        };
        let text = public_url_help(Some(&published)).join("\n");
        assert!(
            text.contains(
                "tailscale funnel --set-path=/api/v1/github/webhooks --bg http://127.0.0.1:9090/api/v1/github/webhooks"
            ),
            "{text}"
        );
        assert!(text.contains("docs/self-hosting.md"), "{text}");
        assert!(text.contains("builder.tail1234.ts.net"), "{text}");
        assert!(text.to_lowercase().contains("cloudflare"), "{text}");
        // F7: the path mount does not keep the other mounts private.
        assert!(!text.contains("mounted at just that path"), "{text}");
        assert!(text.contains("dedicated hostname"), "{text}");
        assert!(text.contains("tailscale funnel status"), "{text}");
        assert!(
            text.contains("only /api/v1/github/webhooks"),
            "the one-path rule is still stated: {text}"
        );
        assert!(
            public_url_prompt(Some(&published)).contains("https://builder.tail1234.ts.net"),
            "the suggestion must be visible at the prompt"
        );
        assert!(
            public_url_prompt(None).contains("blank"),
            "without a suggestion the prompt must still say blank skips"
        );
        // Nothing detected: no address is offered or invented.
        let plain = public_url_help(None).join("\n");
        assert!(!plain.contains("Enter accepts"), "{plain}");
        assert!(!plain.contains("https://builder"), "{plain}");
        // Detected but not Funnel-published: the name is a hint, and the
        // wording never claims Enter accepts it.
        let hint = UrlSuggestion {
            url: "https://builder.tail1234.ts.net".to_owned(),
            funnel: FunnelPublication::NotPublished,
        };
        let hinted = public_url_help(Some(&hint)).join("\n");
        assert!(hinted.contains("builder.tail1234.ts.net"), "{hinted}");
        assert!(hinted.contains("tailnet"), "{hinted}");
        assert!(
            hinted.contains("GitHub cannot reach a tailnet-only name"),
            "{hinted}"
        );
        assert!(!hinted.contains("Enter accepts"), "{hinted}");
        assert!(
            public_url_prompt(Some(&hint)).contains("blank = skip"),
            "{hint:?}"
        );
    }

    /// From `serve` the wizard runs inside the process that *is* the engine:
    /// the URL it resolved (the one the App's webhook was pointed at) has to
    /// reach that engine's own origin, because Check Run detail links and the
    /// webhook health check read `PRELOOP_PUBLIC_URL` off this environment —
    /// no `serve` starts to pick it up any other way. An origin configured
    /// deliberately is never silently replaced, and which one won is said.
    #[test]
    #[allow(unsafe_code)]
    fn the_wizard_answer_reaches_the_running_engine_or_says_who_won() {
        let _env_guard = crate::TEST_ENV_LOCK.blocking_lock();
        let previous = std::env::var_os("PRELOOP_PUBLIC_URL");
        // SAFETY: TEST_ENV_LOCK serializes this process-global env access; the
        // value read above is restored below.
        unsafe { std::env::set_var("PRELOOP_PUBLIC_URL", "http://127.0.0.1:9090") };

        let note =
            apply_engine_public_url("http://127.0.0.1:9090", Some("https://tail.example.ts.net"))
                .expect("replacing the loopback default is reported");
        assert_eq!(
            std::env::var("PRELOOP_PUBLIC_URL").unwrap(),
            "https://tail.example.ts.net"
        );
        assert!(note.contains("https://tail.example.ts.net"), "{note}");
        // The engine's own consumers read exactly this variable.
        let details =
            preloop_runner_server::github::run_details_url(preloop_gha_protocol::RunId::new())
                .expect("the engine has an origin");
        assert!(
            details.starts_with("https://tail.example.ts.net/runs/"),
            "{details}"
        );

        // A deliberate origin wins: the wizard's answer is not applied, and
        // the note names both so the difference is not silent.
        // SAFETY: as above.
        unsafe { std::env::set_var("PRELOOP_PUBLIC_URL", "https://ci.example.com") };
        let note = apply_engine_public_url(
            "https://ci.example.com",
            Some("https://tail.example.ts.net"),
        )
        .expect("a disagreement is reported");
        assert_eq!(
            std::env::var("PRELOOP_PUBLIC_URL").unwrap(),
            "https://ci.example.com"
        );
        assert!(note.contains("https://ci.example.com"), "{note}");
        assert!(note.contains("https://tail.example.ts.net"), "{note}");

        // The same answer is no change at all, and a skipped question leaves
        // the engine's own origin alone.
        assert_eq!(
            apply_engine_public_url("https://ci.example.com/", Some("https://ci.example.com")),
            None
        );
        assert_eq!(apply_engine_public_url("http://127.0.0.1:9090", None), None);
        assert_eq!(
            std::env::var("PRELOOP_PUBLIC_URL").unwrap(),
            "https://ci.example.com"
        );
        // SAFETY: restores the value read above.
        unsafe {
            match previous {
                Some(value) => std::env::set_var("PRELOOP_PUBLIC_URL", value),
                None => std::env::remove_var("PRELOOP_PUBLIC_URL"),
            }
        }
    }

    /// The configure-only hint is for the run that starts nothing: the written
    /// config is what the next `serve` reads, and the URL this run resolved
    /// has to be handed to it explicitly, because the config file does not
    /// hold it. (The `serve`-embedded wizard prints neither line — that
    /// process applies the URL itself.)
    #[test]
    fn the_configure_only_hint_speaks_for_the_next_serve() {
        let path = Path::new("/home/op/.config/preloop/config.toml");
        let with_url = configure_only_hint(path, Some("https://tail.example.ts.net")).join("\n");
        assert!(with_url.contains("`preloop serve` reads"), "{with_url}");
        assert!(
            with_url.contains("/home/op/.config/preloop/config.toml"),
            "{with_url}"
        );
        assert!(with_url.contains("on its next start"), "{with_url}");
        assert!(
            with_url.contains("PRELOOP_PUBLIC_URL=https://tail.example.ts.net"),
            "{with_url}"
        );
        assert!(
            with_url.contains("--public-url https://tail.example.ts.net"),
            "{with_url}"
        );
        let without_url = configure_only_hint(path, None);
        assert_eq!(without_url.len(), 1, "{without_url:?}");
        assert!(
            without_url[0].contains("on its next start"),
            "{without_url:?}"
        );
        assert!(
            !without_url[0].contains("PRELOOP_PUBLIC_URL"),
            "{without_url:?}"
        );
    }

    #[test]
    fn the_new_flags_parse() {
        let args = InitArgs::try_parse_from([
            "init",
            "--no-secrets-scan",
            "--public-url",
            "https://engine.example.com",
            "--repo",
            "owner/name",
        ])
        .unwrap();
        assert!(args.no_secrets_scan);
        assert_eq!(
            args.public_url.as_deref(),
            Some("https://engine.example.com")
        );
        assert_eq!(args.repos, vec!["owner/name".to_owned()]);
    }

    /// A successful interactive write must leave the report configured: the
    /// row, its JSON entry, and the decision all come from the post-write
    /// state, not the snapshot taken before the prompt.
    #[test]
    fn a_secret_stored_interactively_is_reported_configured() {
        let scan = workflows::Scan {
            target: workflows::Target {
                repo: Some("local/app".to_owned()),
                checkout: None,
            },
            source: "checkout",
            files: vec![workflows::WorkflowFile {
                path: ".github/workflows/ci.yml".to_owned(),
                text:
                    "jobs:\n  deploy:\n    steps:\n      - run: echo ${{ secrets.DEPLOY_TOKEN }}\n"
                        .to_owned(),
            }],
            skipped: None,
            notes: Vec::new(),
        };
        let mut rows = vec![workflows::SecretRow {
            name: "DEPLOY_TOKEN".to_owned(),
            reads: vec![".github/workflows/ci.yml:4".to_owned()],
            scope: None,
            environment_scope: None,
            stored_as: None,
        }];
        assert_eq!(workflows::secrets_decision(&rows), "missing");
        assert_eq!(
            workflows::secret_rows_json(&scan, &rows)[0]["command"],
            json!("preloop secret set DEPLOY_TOKEN --repo local/app")
        );

        assert!(mark_stored(
            &mut rows,
            "DEPLOY_TOKEN",
            scan.target.repo.as_deref()
        ));
        assert_eq!(rows[0].status(), "repository local/app");
        assert_eq!(workflows::secrets_decision(&rows), "ok");
        let entries = workflows::secret_rows_json(&scan, &rows);
        assert_eq!(entries[0]["configured"], json!("repository local/app"));
        assert_eq!(entries[0]["command"], Value::Null);
        // The scope a write lands at is named by the same lookup the report
        // uses: a store that already held the secret says the same thing.
        let stored = workflows::Stored::from_listing(&[(
            "DEPLOY_TOKEN".to_owned(),
            Some("local/app".to_owned()),
            None,
        )]);
        assert_eq!(
            workflows::secret_rows(&scan, &stored)[0].scope,
            rows[0].scope
        );
        // An already-configured row, and another name, are left alone.
        assert!(!mark_stored(&mut rows, "DEPLOY_TOKEN", None));
        assert!(!mark_stored(&mut rows, "OTHER_TOKEN", Some("local/app")));
    }

    /// `PRELOOP_RUNNER_BASE_IMAGE` beats the wizard's choice at serve time, so
    /// the label verdicts must be classified against the override: the engine
    /// runs `ubuntu-22.04` on the override, not on the official golden.
    #[test]
    fn label_verdicts_follow_the_effective_base_image() {
        let _guard = crate::TEST_ENV_LOCK.blocking_lock();
        let previous = std::env::var_os(preloop_runner_server::config::BASE_IMAGE_ENV);
        let pool = |base: &str| {
            workflows::Pool::new(
                vec![
                    "self-hosted".to_owned(),
                    "Linux".to_owned(),
                    "x86_64".to_owned(),
                ],
                "x86_64",
                base,
            )
        };
        unsafe { std::env::remove_var(preloop_runner_server::config::BASE_IMAGE_ENV) };
        assert_eq!(
            effective_base_image(Some("ghcr.io/acme/chosen:1")),
            "ghcr.io/acme/chosen:1"
        );
        assert_eq!(effective_base_image(None), OFFICIAL_GOLDEN);
        assert_eq!(
            workflows::classify("ubuntu-22.04", &pool(&effective_base_image(None))),
            workflows::Verdict::HostedLinux { official: true },
            "nothing configured means the packed official golden"
        );

        unsafe {
            std::env::set_var(
                preloop_runner_server::config::BASE_IMAGE_ENV,
                "ghcr.io/acme/base:1",
            )
        };
        let base = effective_base_image(Some("ghcr.io/acme/chosen:1"));
        assert_eq!(base, "ghcr.io/acme/base:1");
        assert_eq!(
            workflows::classify("ubuntu-22.04", &pool(&base)),
            workflows::Verdict::HostedLinux { official: false },
            "ubuntu-22.04 runs on the override the engine resolved"
        );
        let message = workflows::verdict_message(
            workflows::Verdict::HostedLinux { official: false },
            "ubuntu-22.04",
            &pool(&base),
        );
        assert!(message.contains("your configured golden"), "{message}");

        // An exported-but-blank override behaves like an unset one.
        unsafe { std::env::set_var(preloop_runner_server::config::BASE_IMAGE_ENV, " ") };
        assert_eq!(effective_base_image(None), OFFICIAL_GOLDEN);
        assert_eq!(
            effective_base_image(Some("ghcr.io/acme/chosen:1")),
            "ghcr.io/acme/chosen:1"
        );
        match previous {
            Some(value) => unsafe {
                std::env::set_var(preloop_runner_server::config::BASE_IMAGE_ENV, value)
            },
            None => unsafe { std::env::remove_var(preloop_runner_server::config::BASE_IMAGE_ENV) },
        }
    }

    /// A checkout without an `origin` remote keeps its `local/<dir>` identity:
    /// secrets the local submission path stored under that scope are found,
    /// and a missing one is offered at that scope, never globally.
    #[test]
    fn a_local_checkout_keeps_its_local_secret_scope() {
        let root = Path::new("/work/app");
        let targets = workflows::targets(&[], Some(root), Some("local/app"));
        assert_eq!(targets[0].repo.as_deref(), Some("local/app"));
        assert_eq!(targets[0].checkout.as_deref(), Some(root));

        let stored = workflows::Stored::from_listing(&[(
            "DEPLOY_TOKEN".to_owned(),
            Some("local/app".to_owned()),
            None,
        )]);
        let scan = workflows::Scan {
            target: workflows::Target {
                repo: Some("local/app".to_owned()),
                checkout: Some(root.to_path_buf()),
            },
            source: "checkout",
            files: vec![workflows::WorkflowFile {
                path: ".github/workflows/ci.yml".to_owned(),
                text:
                    "jobs:\n  deploy:\n    steps:\n      - run: echo ${{ secrets.DEPLOY_TOKEN }} \
                       ${{ secrets.MISSING_TOKEN }}\n"
                        .to_owned(),
            }],
            skipped: None,
            notes: Vec::new(),
        };
        let rows = workflows::secret_rows(&scan, &stored);
        let token = rows
            .iter()
            .find(|row| row.name == "DEPLOY_TOKEN")
            .expect("the stored secret is read");
        assert_eq!(token.scope.as_deref(), Some("repository local/app"));
        assert_eq!(token.status(), "repository local/app");
        let missing = rows
            .iter()
            .find(|row| row.name == "MISSING_TOKEN")
            .expect("the missing secret is read");
        assert_eq!(missing.scope, None);
        assert_eq!(
            workflows::set_command(&missing.name, scan.target.repo.as_deref(), None).as_deref(),
            Some("preloop secret set MISSING_TOKEN --repo local/app"),
            "the remediation must keep the repository scope"
        );
    }

    /// Repository text reaches the human report through `note`/`section`,
    /// which must render control characters visibly: a `runs-on` label cannot
    /// inject an ANSI sequence into the terminal.
    #[test]
    fn repository_text_prints_escaped_not_raw() {
        let escaped = escape_control("\x1b]0;pwn\x07");
        assert_eq!(escaped, "\\x1b]0;pwn\\x07");
        assert!(!escaped.chars().any(char::is_control));

        // A label carried through the scanner keeps its bytes until print time.
        let scan = workflows::Scan {
            target: workflows::Target {
                repo: Some("o/r".to_owned()),
                checkout: None,
            },
            source: "github",
            files: vec![workflows::WorkflowFile {
                path: ".github/workflows/ci.yml".to_owned(),
                text: "on: push\njobs:\n  a:\n    runs-on: \"\\e]0;pwn\\a\"\n".to_owned(),
            }],
            skipped: None,
            notes: Vec::new(),
        };
        let pool = workflows::Pool::new(vec!["self-hosted".to_owned()], "x86_64", OFFICIAL_GOLDEN);
        let report = workflows::label_report(&[scan], &pool);
        assert!(report.unparsed.is_empty(), "{:?}", report.unparsed);
        let row = report
            .rows
            .iter()
            .find(|row| row.label.contains("pwn"))
            .expect("the crafted label was scanned");
        let printed = escape_control(&row.label);
        assert!(!printed.chars().any(char::is_control), "{printed}");
        assert!(printed.contains("\\x1b"), "{printed}");

        // Ordinary text, including non-ASCII, is untouched.
        assert_eq!(escape_control("ubuntu-22.04"), "ubuntu-22.04");
        assert_eq!(
            escape_control(".github/workflows/café.yml"),
            ".github/workflows/café.yml"
        );
    }
}
