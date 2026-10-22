//! `preloop tui`: resolve contexts and credentials, then hand off to
//! `preloop-tui`.
//!
//! The TUI never reads credentials itself, so this module owns every source a
//! token or CA can come from: `tui.toml` (names and paths only), the local
//! default context, and the environment overrides for the local Sandbox
//! Service. The hooks cover the three actions that need the CLI binary.

use crate::init;
use crate::prompter::{self, ChoiceOption, NoteLevel, Prompter};
use std::io::IsTerminal as _;
use std::path::Path;

use preloop_tui::config::ContextConfig;
use preloop_tui::{Context, Hooks, Options, SandboxTarget, Secret, StartView};

/// Arguments for `preloop tui`.
#[derive(Debug, clap::Args)]
pub(crate) struct TuiArgs {
    /// Open this context from `tui.toml` instead of the first one.
    #[arg(long, value_name = "NAME")]
    pub(crate) context: Option<String>,
    /// Disable every action that changes engine or sandbox state.
    #[arg(long)]
    pub(crate) read_only: bool,
    /// Open the runs tab on this run id (or unique prefix).
    #[arg(long, value_name = "RUN_ID")]
    pub(crate) run: Option<String>,
}

/// Resolve the contexts and hooks, then run the TUI on its own thread.
///
/// The TUI builds its own tokio runtime, so it must not run on the CLI's
/// runtime thread; the hook closures are `Send`, so they move across with the
/// options.
pub(crate) fn run(args: TuiArgs) -> anyhow::Result<()> {
    if !std::io::stdout().is_terminal() {
        anyhow::bail!("preloop tui needs an interactive terminal");
    }
    let home = crate::preloop_home();
    let config = preloop_tui::config::load(&home.join("tui.toml"))?.unwrap_or_default();
    let contexts = resolve_contexts_from_config(&config, &home);
    let index = select_context(&contexts, args.context.as_deref())?;
    let options = Options {
        contexts,
        context: index,
        start: match args.run {
            Some(run_id) => StartView::Run(run_id),
            None => StartView::Overview,
        },
        read_only: args.read_only,
        stack: config.stack.clone(),
    };
    let hooks = Hooks {
        start_engine: Box::new(move || start_engine(&home)),
        run_wizard: Box::new(run_wizard),
        shell_out: Box::new(shell_out),
    };
    let handle = std::thread::Builder::new()
        .name("preloop-tui".to_owned())
        .spawn(move || preloop_tui::run(options, hooks))?;
    handle
        .join()
        .map_err(|_| anyhow::anyhow!("the TUI thread panicked"))?
}

/// Resolve contexts from the already-loaded TUI configuration.
fn resolve_contexts_from_config(
    config: &preloop_tui::config::TuiConfig,
    home: &Path,
) -> Vec<Context> {
    if !config.contexts.is_empty() {
        config.contexts.iter().map(context_from_config).collect()
    } else {
        vec![local_context(home)]
    }
}

/// One `[[context]]` table as a live context: URL resolved, secrets read.
///
/// A configured sandbox uses only the files named in its own table. It never
/// inherits the local token or CA: those belong to this host's Sandbox
/// Service and must not be sent to a remote one.
fn context_from_config(config: &ContextConfig) -> Context {
    let engine_token = match &config.engine_token_env {
        Some(name) => std::env::var(name)
            .ok()
            .and_then(non_empty)
            .map(Secret::new),
        None => config
            .engine_token_file
            .as_deref()
            .and_then(read_secret)
            .map(Secret::new),
    };
    let sandbox = config.sandbox_url.as_ref().map(|url| SandboxTarget {
        url: url.clone(),
        token: config
            .sandbox_token_file
            .as_deref()
            .and_then(read_secret)
            .map(Secret::new),
        ca_pem: config.sandbox_ca_file.as_deref().and_then(read_text),
    });
    Context {
        name: config.name.clone(),
        engine_url: config.engine_url.clone().unwrap_or_else(crate::server_url),
        engine_token,
        sandbox,
    }
}

/// The default context: whatever this CLI would use for every other command,
/// plus the local Sandbox Service defaults.
fn local_context(home: &Path) -> Context {
    Context {
        name: "local".to_owned(),
        engine_url: crate::server_url(),
        engine_token: crate::api_token().map(Secret::new),
        sandbox: Some(local_sandbox_target(home)),
    }
}

/// Local Sandbox Service target: `PRELOOP_SANDBOX_*` wins, then the files
/// under `<preloop_home>/state`.
///
/// The state-file fallbacks are loopback-only. Those files hold this host's
/// credentials and CA, so sending them anywhere else would hand a remote
/// service a secret it has no business seeing.
fn local_sandbox_target(home: &Path) -> SandboxTarget {
    let url = std::env::var("PRELOOP_SANDBOX_URL")
        .ok()
        .and_then(non_empty)
        .unwrap_or_else(|| "https://127.0.0.1:9191".to_owned());
    let loopback = url_is_loopback(&url);
    let state = home.join("state");
    let token = std::env::var("PRELOOP_SANDBOX_TOKEN")
        .ok()
        .and_then(non_empty)
        .or_else(|| {
            if loopback {
                read_secret(&state.join("sandbox-client-token"))
            } else {
                None
            }
        });
    let ca_pem = std::env::var("PRELOOP_SANDBOX_CA")
        .ok()
        .and_then(non_empty)
        .and_then(|path| read_text(Path::new(&path)))
        .or_else(|| {
            let path = state.join("sandbox-ca").join("ca.pem");
            if loopback && path.exists() {
                read_text(&path)
            } else {
                None
            }
        });
    SandboxTarget {
        url,
        token: token.map(Secret::new),
        ca_pem,
    }
}

/// Whether `url` names this host's loopback interface. Unparseable URLs are
/// not loopback, so they get no local credentials.
fn url_is_loopback(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
    )
}

/// `--context NAME` resolved to an index, defaulting to the first context.
fn select_context(contexts: &[Context], requested: Option<&str>) -> anyhow::Result<usize> {
    let Some(name) = requested else {
        return Ok(0);
    };
    if let Some(index) = contexts.iter().position(|context| context.name == name) {
        return Ok(index);
    }
    let available = contexts
        .iter()
        .map(|context| context.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::bail!("unknown context {name:?}; available contexts: {available}")
}

/// Start `preloop serve` detached, appending its output to
/// `<preloop_home>/engine.log`.
///
/// Its own process group on unix is what lets the engine outlive the TUI: the
/// child must survive the terminal session that started it.
fn start_engine(home: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(home)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(home.join("engine.log"))?;
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .arg("serve")
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        command.process_group(0);
    }
    command.spawn()?;
    Ok(())
}

struct TuiPrompter {
    port: preloop_tui::wizard::WizardPort,
}

impl TuiPrompter {
    fn send(&self, request: preloop_tui::wizard::WizardRequest) -> anyhow::Result<()> {
        self.port
            .requests
            .send(request)
            .map_err(|_| prompter::cancelled_with("setup wizard UI closed"))
    }

    fn receive(&mut self) -> anyhow::Result<preloop_tui::wizard::WizardReply> {
        self.port
            .replies
            .recv()
            .map_err(|_| prompter::cancelled_with("setup wizard UI closed"))
    }
}

impl Prompter for TuiPrompter {
    fn section(&mut self, step: usize, total: usize, title: &str) {
        let _ = self.send(preloop_tui::wizard::WizardRequest::Section {
            title: title.to_owned(),
            step,
            total,
        });
    }

    fn note(&mut self, level: NoteLevel, text: &str) {
        let level = match level {
            NoteLevel::Info => preloop_tui::wizard::NoteLevel::Info,
            NoteLevel::Success => preloop_tui::wizard::NoteLevel::Success,
            NoteLevel::Warning => preloop_tui::wizard::NoteLevel::Warning,
            NoteLevel::Error => preloop_tui::wizard::NoteLevel::Error,
        };
        let _ = self.send(preloop_tui::wizard::WizardRequest::Note {
            text: text.to_owned(),
            level,
        });
    }

    fn choice(
        &mut self,
        prompt: &str,
        options: &[ChoiceOption],
        default: usize,
    ) -> anyhow::Result<usize> {
        let options = options
            .iter()
            .map(|option| preloop_tui::wizard::WizardOption {
                label: option.label.clone(),
                detail: (!option.detail.is_empty()).then(|| option.detail.clone()),
                enabled: option.enabled,
            })
            .collect();
        self.send(preloop_tui::wizard::WizardRequest::Choice {
            prompt: prompt.to_owned(),
            options,
            default,
        })?;
        match self.receive()? {
            preloop_tui::wizard::WizardReply::Choice(index) => Ok(index),
            preloop_tui::wizard::WizardReply::Cancel => Err(prompter::cancelled()),
            _ => anyhow::bail!("setup wizard received an invalid choice reply"),
        }
    }

    fn confirm(&mut self, prompt: &str, default: bool) -> anyhow::Result<bool> {
        self.send(preloop_tui::wizard::WizardRequest::Confirm {
            prompt: prompt.to_owned(),
            default,
        })?;
        match self.receive()? {
            preloop_tui::wizard::WizardReply::Confirm(value) => Ok(value),
            preloop_tui::wizard::WizardReply::Cancel => Err(prompter::cancelled()),
            _ => anyhow::bail!("setup wizard received an invalid confirm reply"),
        }
    }

    fn text(
        &mut self,
        prompt: &str,
        default: Option<&str>,
        help: Option<&str>,
    ) -> anyhow::Result<String> {
        self.send(preloop_tui::wizard::WizardRequest::Text {
            prompt: prompt.to_owned(),
            default: default.map(str::to_owned),
            help: help.map(str::to_owned),
        })?;
        match self.receive()? {
            preloop_tui::wizard::WizardReply::Text(value) => Ok(value),
            preloop_tui::wizard::WizardReply::Cancel => Err(prompter::cancelled()),
            _ => anyhow::bail!("setup wizard received an invalid text reply"),
        }
    }

    fn secret(&mut self, prompt: &str) -> anyhow::Result<String> {
        self.send(preloop_tui::wizard::WizardRequest::Secret {
            prompt: prompt.to_owned(),
        })?;
        match self.receive()? {
            preloop_tui::wizard::WizardReply::Secret(value) => Ok(value),
            preloop_tui::wizard::WizardReply::Cancel => Err(prompter::cancelled()),
            _ => anyhow::bail!("setup wizard received an invalid secret reply"),
        }
    }

    fn progress(&mut self, label: &str) {
        let _ = self.send(preloop_tui::wizard::WizardRequest::Progress {
            label: label.to_owned(),
        });
    }
}

fn run_wizard(port: preloop_tui::wizard::WizardPort) -> anyhow::Result<()> {
    std::thread::spawn(move || {
        let mut prompter = TuiPrompter { port };
        let options = init::WizardOptions::embedded(init::InitArgs::default());
        init::run_wizard(&mut prompter, options)
    })
    .join()
    .map_err(|_| anyhow::anyhow!("setup wizard thread panicked"))??;
    Ok(())
}

/// Run this same binary with `args` in the foreground and return its exit
/// code. The TUI suspends the terminal around the call.
fn shell_out(args: &[String]) -> anyhow::Result<i32> {
    let status = std::process::Command::new(std::env::current_exe()?)
        .args(args)
        .status()?;
    Ok(status.code().unwrap_or(1))
}

/// Trimmed contents of a credential file, or `None` when it is absent,
/// unreadable, or blank.
fn read_secret(path: &Path) -> Option<String> {
    read_text(path).and_then(non_empty)
}

/// File contents, or `None` when the file cannot be read.
fn read_text(path: &Path) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// Trim `value`, keeping it only when something remains.
fn non_empty(value: String) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}
