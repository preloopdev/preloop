//! `preloop init` — one command that configures credentials, the golden every
//! job forks from, and how the engine should run.
//!
//! Humans get a four-step wizard (only when stdin *and* stdout are TTYs);
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

    /// Public HTTPS URL of this engine; enables webhook delivery on the App.
    #[arg(long, value_name = "URL")]
    pub public_url: Option<String>,

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
        Ok(()) => Ok(()),
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
pub(crate) async fn run_from_serve() -> anyhow::Result<()> {
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

/// The answers the four steps produce, before any of them is applied.
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
    /// Persisted base image; `None` = the engine's stock base, i.e. the packed
    /// official golden the engine downloads itself.
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

impl Session {
    fn section(&self, title: &str) {
        if !self.json {
            println!("\n{title}");
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
            println!("    {line}");
        }
    }

    async fn execute(
        mut self,
        args: InitArgs,
        observability: Option<preloop_observability::Observability>,
        from_serve: bool,
    ) -> anyhow::Result<()> {
        let config = load_config().context("reading the engine config")?;
        let answers = self.resolve(&args, &config, from_serve)?;

        // A system service reads its own state dir, so when the operator did
        // not pin one, the credentials and the golden must be written there or
        // the installed service would start with neither.
        if answers.mode == RunMode::Service {
            self.prepare_service_home()?;
        }

        self.section("Applying your answers:");
        if answers.auth_unchanged {
            self.report_stored_credentials(&config, &args).await?;
        } else {
            self.configure_credentials(&args, answers.auth).await?;
        }
        let decision = if answers.golden_unchanged {
            let decision = decision_from_config(&config, &answers.golden).ok_or_else(|| {
                anyhow::anyhow!(
                    "internal: no stored golden to keep — re-run `preloop init` and pick one"
                )
            })?;
            self.section(&format!("2/4 Golden: {} (kept)", decision.kind.as_str()));
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
        if !self.json {
            println!(
                "    3/4 Run mode: {}",
                describe_mode(answers.mode, &decision.label)
            );
        }
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
        self.emit(json!({
            "step": "done",
            "decision": answers.mode.as_str(),
            "config_path": written,
            "golden": {
                "kind": decision.kind.as_str(),
                "base_image": decision.base_image,
                "label": decision.label,
            },
            "preflight": preflight,
        }));
        self.start_mode(answers.mode, observability, &written).await
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
                 Four steps; press Enter to keep the current answer.",
                preloop_runner_server::config::config_path().display()
            );
        }
        let (auth, auth_unchanged) = self.ask_auth(args, config)?;
        let (golden, golden_unchanged) = self.ask_golden(args, config)?;
        let mode = if from_serve {
            self.section("3/4 Run mode: none (configure only — this engine keeps running)");
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
        self.section("1/4 Credentials");
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
        self.section("2/4 Golden — the VM image every job forks from");
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
        self.section("3/4 Run mode — not persisted, chosen each run");
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
        self.section("1/4 Credentials: (kept)");
        let summary = credential_summary(config).unwrap_or_else(|| "none — local only".to_owned());
        self.note(&format!("kept: {summary}"));
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

    async fn configure_credentials(
        &mut self,
        args: &InitArgs,
        auth: AuthChoice,
    ) -> anyhow::Result<()> {
        self.section("1/4 Credentials:");
        match auth {
            AuthChoice::None => {
                self.note(
                    "local only — no GitHub credential written; existing ones are left alone",
                );
                self.emit(json!({"step": "credentials", "decision": "none", "result": {}}));
                return Ok(());
            }
            AuthChoice::App => self.note("GitHub App"),
            AuthChoice::Pat => self.note("fine-grained PAT"),
        }
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
            public_url: args.public_url.clone(),
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
        self.emit(json!({
            "step": "credentials",
            "decision": match auth { AuthChoice::App => "app", _ => "pat" },
            "result": {"summary": summary, "verify": verified},
            "log": log,
        }));
        Ok(())
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

    // -- step 2: golden ---------------------------------------------------

    async fn configure_golden(&mut self, answer: &GoldenAnswer) -> anyhow::Result<GoldenDecision> {
        self.section(&format!("2/4 Golden: {}", answer.kind.as_str()));
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
                    "note": "the engine downloads the packed official golden on the first serve",
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
        self.section("4/4 Preflight");
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
    ) -> anyhow::Result<()> {
        match mode {
            RunMode::Configure => {
                // From `serve` the caller says it is continuing with the new
                // config; saying "on its next start" there would be wrong,
                // because this process picks the choice up immediately.
                if !self.json && !self.from_serve {
                    println!(
                        "configure only: `preloop serve` reads {} on its next start",
                        config_path.display()
                    );
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
                crate::cmd_engine(crate::ServeArgs::default(), observability).await
            }
            RunMode::Service => {
                ensure_service_root()?;
                if !self.json {
                    println!("installing the control plane as a service…");
                }
                let home = std::env::var_os("PRELOOP_HOME").map(PathBuf::from);
                crate::server_install::run(crate::server_install::install_args(home))
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
            // variable when set, else the persisted choice, else the engine's
            // stock base (null).
            "base_image": base_image,
            "env_override": std::env::var(preloop_runner_server::config::BASE_IMAGE_ENV)
                .ok()
                .filter(|value| !value.trim().is_empty()),
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
    reqwest::Client::builder()
        .user_agent("preloop-init")
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .build()
        .context("building the HTTP client")
}

// -- OCI references -------------------------------------------------------

fn oci_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
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
}
