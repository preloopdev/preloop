//! Terminal lifecycle, input, the effect executor, and the main loop.
//!
//! - The terminal is put in raw mode on the alternate screen. A guard restores
//!   it on every exit path, and a panic hook restores it before the panic
//!   message is printed.
//! - Key presses are read on a dedicated thread and sent over a tokio channel.
//!   The thread pauses while a shell-out owns the terminal.
//! - A 250 ms tick drives the clock and refresh timers. The screen is redrawn
//!   only when the app reports it dirty.
//! - Effects run on tokio. Each stream is one task; starting a stream of the
//!   same kind aborts the previous one.

use std::collections::HashMap;
use std::io::{self, IsTerminal, Stdout};
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context as _, bail};
use chrono::Utc;
use futures::StreamExt;
use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event};
use ratatui::crossterm::{cursor, terminal as term};
use ratatui::layout::{Constraint, Layout, Rect};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use crate::actions::Action;
use crate::api::FeedEvent;
use crate::app::{App, ContextInfo, Effect, Fetch, Fetched, Msg, PaletteAction, StreamKind, Subscription};
use crate::client::engine::RunsQuery;
use crate::client::{EngineClient, SandboxHandle};
use crate::options::{Hooks, Options};
use crate::theme::Theme;
use crate::views::ViewKind;
use crate::widgets::{chrome, modal, palette, toast};

/// Period of the `Msg::Tick` the app uses for its clock and refresh timers.
const TICK: Duration = Duration::from_millis(250);

/// Upper bound on the reconnect delay of a stream.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Run the terminal UI until the user quits.
///
/// Blocks the calling thread, which must not be a tokio worker: the TUI builds
/// its own multi-threaded runtime. Fails before touching the terminal when
/// stdout is not a terminal.
pub fn run(options: Options, hooks: Hooks) -> anyhow::Result<()> {
    if !io::stdout().is_terminal() {
        bail!("preloop tui needs an interactive terminal");
    }
    if options.contexts.is_empty() {
        bail!("no engine context to open");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting the TUI runtime")?;
    let result = runtime.block_on(async_main(options, hooks));
    runtime.shutdown_timeout(Duration::from_millis(200));
    result
}

type Term = Terminal<CrosstermBackend<Stdout>>;

/// Clients for one context, built once when the TUI starts.
#[derive(Clone)]
struct ContextClients {
    engine: EngineClient,
    /// The sandbox connection, or why it could not be made.
    sandbox: Result<Arc<SandboxHandle>, String>,
    /// Whether the context has a sandbox target at all.
    has_sandbox: bool,
}

async fn async_main(options: Options, hooks: Hooks) -> anyhow::Result<()> {
    let theme = Theme::from_env();
    let clients = build_clients(&options)?;
    let infos: Vec<ContextInfo> = options
        .contexts
        .iter()
        .map(|context| ContextInfo {
            name: context.name.clone(),
            engine_url: context.engine_url.clone(),
            sandbox: context.sandbox.is_some(),
        })
        .collect();

    let (tx, mut rx) = unbounded_channel::<Msg>();
    let paused = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    spawn_input_thread(tx.clone(), Arc::clone(&paused), Arc::clone(&stop));

    let mut terminal = enter_terminal()?;
    let _guard = TerminalGuard;

    let mut app = App::new(
        infos,
        options.context,
        options.start.clone(),
        options.read_only,
        theme,
        Utc::now(),
    );
    app.set_stack_config(options.stack.clone());
    let mut executor = Executor {
        tx: tx.clone(),
        clients,
        current: options.context,
        streams: HashMap::new(),
        hooks: Arc::new(hooks),
        paused: Arc::clone(&paused),
    };
    let startup = app.startup();
    let mut quit = executor.run(startup, &mut terminal);

    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !quit {
        if app.take_dirty() {
            terminal
                .draw(|frame| render(&app, frame))
                .context("drawing the screen")?;
        }
        let msg = next_msg(&mut rx, &mut ticker).await;
        let effects = app.update(msg);
        quit = executor.run(effects, &mut terminal);
    }

    stop.store(true, Ordering::Relaxed);
    for (_, handle) in executor.streams.drain() {
        handle.abort();
    }
    Ok(())
}

async fn next_msg(rx: &mut UnboundedReceiver<Msg>, ticker: &mut tokio::time::Interval) -> Msg {
    tokio::select! {
        Some(msg) = rx.recv() => msg,
        _ = ticker.tick() => Msg::Tick(Utc::now()),
    }
}

fn build_clients(options: &Options) -> anyhow::Result<Vec<ContextClients>> {
    options
        .contexts
        .iter()
        .map(|context| {
            let engine = EngineClient::new(&context.engine_url, context.engine_token.clone())
                .with_context(|| format!("context {}", context.name))?;
            let sandbox = match &context.sandbox {
                Some(target) => SandboxHandle::connect(target)
                    .map(Arc::new)
                    .map_err(|error| format!("{error:#}")),
                None => Err("no sandbox configured".to_owned()),
            };
            Ok(ContextClients {
                engine,
                sandbox,
                has_sandbox: context.sandbox.is_some(),
            })
        })
        .collect()
}

/// Read key presses on a dedicated thread. The thread polls with a timeout so
/// it can notice `stop` and `paused` without blocking forever.
fn spawn_input_thread(tx: UnboundedSender<Msg>, paused: Arc<AtomicBool>, stop: Arc<AtomicBool>) {
    let spawned = std::thread::Builder::new()
        .name("preloop-tui-input".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if paused.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                match event::poll(Duration::from_millis(100)) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(_) => break,
                }
                let msg = match event::read() {
                    Ok(Event::Key(key)) => Msg::Key(key),
                    Ok(Event::Resize(..)) => Msg::Resized,
                    Ok(_) => continue,
                    Err(_) => break,
                };
                if tx.send(msg).is_err() {
                    break;
                }
            }
        });
    if let Err(error) = spawned {
        tracing::error!(%error, "could not start the input thread");
    }
}

/// Put the terminal in raw mode on the alternate screen and install the panic hook.
fn enter_terminal() -> anyhow::Result<Term> {
    install_panic_hook();
    term::enable_raw_mode().context("enabling raw mode")?;
    let mut out = io::stdout();
    ratatui::crossterm::execute!(out, term::EnterAlternateScreen, cursor::Hide)
        .context("entering the alternate screen")?;
    Terminal::new(CrosstermBackend::new(out)).context("creating the terminal")
}

/// Undo `enter_terminal`. Best effort: errors are ignored because this runs
/// during unwinding and on the way out.
fn restore_terminal() {
    let _ = ratatui::crossterm::execute!(io::stdout(), cursor::Show, term::LeaveAlternateScreen);
    let _ = term::disable_raw_mode();
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn install_panic_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            previous(info);
        }));
    });
}

/// Hand the terminal to a foreground child process, then take it back.
fn with_terminal_suspended<T>(
    terminal: &mut Term,
    paused: &AtomicBool,
    body: impl FnOnce() -> T,
) -> anyhow::Result<T> {
    paused.store(true, Ordering::Relaxed);
    restore_terminal();
    let result = body();
    let resumed = term::enable_raw_mode()
        .and_then(|()| {
            ratatui::crossterm::execute!(io::stdout(), term::EnterAlternateScreen, cursor::Hide)
        })
        .context("returning to the TUI");
    paused.store(false, Ordering::Relaxed);
    terminal.clear().context("clearing the screen")?;
    resumed?;
    Ok(result)
}

/// Runs effects. Owns the clients, the open streams, and the hooks.
struct Executor {
    tx: UnboundedSender<Msg>,
    clients: Vec<ContextClients>,
    current: usize,
    streams: HashMap<StreamKind, JoinHandle<()>>,
    hooks: Arc<Hooks>,
    paused: Arc<AtomicBool>,
}

impl Executor {
    fn client(&self) -> &ContextClients {
        &self.clients[self.current]
    }

    /// Run effects in order. Returns true when the app asked to quit.
    fn run(&mut self, effects: Vec<Effect>, terminal: &mut Term) -> bool {
        for effect in effects {
            match effect {
                Effect::Fetch(fetch) => self.fetch(fetch),
                Effect::Subscribe(subscription) => self.subscribe(subscription),
                Effect::Unsubscribe(kind) => self.close(kind),
                Effect::Act(action) => self.act(action, terminal),
                Effect::SwitchContext(index) => {
                    self.close_all();
                    self.current = index;
                }
                Effect::Quit => return true,
            }
        }
        false
    }

    fn close(&mut self, kind: StreamKind) {
        if let Some(handle) = self.streams.remove(&kind) {
            handle.abort();
        }
    }

    fn close_all(&mut self) {
        for (_, handle) in self.streams.drain() {
            handle.abort();
        }
    }

    fn fetch(&self, fetch: Fetch) {
        let clients = self.client().clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let msg = run_fetch(fetch, clients).await;
            let _ = tx.send(msg);
        });
    }

    fn subscribe(&mut self, subscription: Subscription) {
        let client = self.client().engine.clone();
        let tx = self.tx.clone();
        match subscription {
            Subscription::RunEvents { run_id, attempt } => {
                self.close(StreamKind::RunEvents);
                let handle = tokio::spawn(run_events(client, tx, run_id, attempt));
                self.streams.insert(StreamKind::RunEvents, handle);
            }
            Subscription::LiveLog {
                run_id,
                job_id,
                attempt,
            } => {
                self.close(StreamKind::LiveLog);
                let handle = tokio::spawn(live_log(client, tx, run_id, job_id, attempt));
                self.streams.insert(StreamKind::LiveLog, handle);
            }
        }
    }

    fn act(&mut self, action: Action, terminal: &mut Term) {
        let label = action.label();
        let clients = self.client().clone();
        let tx = self.tx.clone();
        match action {
            Action::AttachDebug { session_id } => {
                let hooks = Arc::clone(&self.hooks);
                let paused = Arc::clone(&self.paused);
                let args = vec!["debug".to_owned(), session_id];
                let outcome = with_terminal_suspended(terminal, &paused, || (hooks.shell_out)(&args));
                let result = match outcome {
                    Ok(Ok(code)) => Ok(code),
                    Ok(Err(error)) | Err(error) => Err(format!("{error:#}")),
                };
                let _ = tx.send(Msg::ShellOutDone(result));
            }
            Action::StartEngine => {
                let hooks = Arc::clone(&self.hooks);
                tokio::task::spawn_blocking(move || {
                    let result = (hooks.start_engine)().map_err(|error| format!("{error:#}"));
                    let _ = tx.send(Msg::Hook { label, result });
                });
            }
            Action::RunWizard => {
                let hooks = Arc::clone(&self.hooks);
                tokio::task::spawn_blocking(move || {
                    let (port, _ui) = crate::wizard::channel();
                    let result = (hooks.run_wizard)(port).map_err(|error| format!("{error:#}"));
                    let _ = tx.send(Msg::Hook { label, result });
                });
            }
            Action::OpenUrl { url } => {
                let result = open_url(&url).map_err(|error| error.to_string());
                let _ = tx.send(Msg::ActionDone {
                    label,
                    result: result.map(|()| "opened in the browser".to_owned()),
                });
            }
            other => {
                tokio::spawn(async move {
                    let result = run_write(other, clients.engine).await;
                    let _ = tx.send(Msg::ActionDone { label, result });
                });
            }
        }
    }
}

/// Run one read and wrap its result in the matching `Fetched` variant.
async fn run_fetch(fetch: Fetch, clients: ContextClients) -> Msg {
    let engine = clients.engine;
    let text = |error: crate::client::EngineError| error.to_string();
    let fetched = match fetch {
        Fetch::Status => Fetched::Status(Box::new(engine.status().await.map_err(text))),
        Fetch::Runs => Fetched::Runs(
            engine
                .runs(&RunsQuery {
                    limit: Some(200),
                    ..Default::default()
                })
                .await
                .map_err(text),
        ),
        Fetch::RunDetail(run_id) => {
            let result = engine.run(&run_id).await.map_err(text);
            Fetched::RunDetail {
                run_id,
                result: Box::new(result),
            }
        }
        Fetch::Log(key) => {
            let result = engine
                .run_logs(&key.run_id, Some(&key.job_id), key.step)
                .await
                .map_err(text);
            Fetched::Log { key, result }
        }
        Fetch::Bindings => Fetched::Bindings(engine.pool_bindings().await.map_err(text)),
        Fetch::Webhooks => Fetched::Webhooks(engine.webhook_deliveries(None, 20).await.map_err(text)),
        Fetch::WebhookHealth => Fetched::WebhookHealth(engine.webhook_health().await.map_err(text)),
        Fetch::Debug => Fetched::Debug(engine.debug_sessions().await.map_err(text)),
        Fetch::Runners => Fetched::Runners(engine.runners(None).await.map_err(text)),
        Fetch::Stack(config) => Fetched::Stack(crate::client::github::stack(&config).await.map_err(|error| format!("{error:#}"))),
        Fetch::Sandbox => {
            if !clients.has_sandbox {
                Fetched::Sandbox(Err("no sandbox configured".to_owned()))
            } else {
                let result = match &clients.sandbox {
                    Ok(handle) => handle.summary().await.map_err(|error| format!("{error:#}")),
                    Err(error) => Err(error.clone()),
                };
                Fetched::Sandbox(result)
            }
        }
    };
    Msg::Fetched(fetched)
}

/// Perform one write against the engine and describe the outcome.
async fn run_write(action: Action, engine: EngineClient) -> Result<String, String> {
    let text = |error: crate::client::EngineError| error.to_string();
    match action {
        Action::CancelRun { run_id } => engine
            .cancel_run(&run_id)
            .await
            .map(|run| format!("run {} is {}", run.short_id(), run.status.label()))
            .map_err(text),
        Action::RerunRun { run_id } => engine
            .rerun(&run_id)
            .await
            .map(|accepted| format!("queued #{} ({} jobs)", accepted.run_number, accepted.queued_jobs))
            .map_err(text),
        Action::ApproveJob { run_id, job_id } => engine
            .approve_job(&run_id, &job_id, None)
            .await
            .map(|_| format!("approved {job_id}"))
            .map_err(text),
        Action::ApproveFork { run_id } => engine
            .approve_fork(&run_id, None)
            .await
            .map(|_| "fork run approved".to_owned())
            .map_err(text),
        Action::PurgeRunner { name } => engine
            .purge_runner(&name)
            .await
            .map(|_| format!("purged {name}"))
            .map_err(text),
        Action::ReplayDelivery { delivery_id } => engine
            .replay_delivery(&delivery_id)
            .await
            .map(|_| "requeued".to_owned())
            .map_err(text),
        other => Err(format!("{} is not a write", other.label())),
    }
}

async fn run_events(client: EngineClient, tx: UnboundedSender<Msg>, run_id: String, attempt: u32) {
    if attempt > 0 {
        tokio::time::sleep(backoff(attempt)).await;
    }
    let ended = |error: Option<String>| Msg::StreamEnded {
        kind: StreamKind::RunEvents,
        run_id: run_id.clone(),
        job_id: None,
        error,
    };
    let mut stream = match client.events(&run_id).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = tx.send(ended(Some(error.to_string())));
            return;
        }
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                if is_relevant(&event) {
                    let _ = tx.send(Msg::Feed {
                        run_id: run_id.clone(),
                        event,
                    });
                }
            }
            Err(error) => {
                let _ = tx.send(ended(Some(error.to_string())));
                return;
            }
        }
    }
    let _ = tx.send(ended(None));
}

fn is_relevant(event: &FeedEvent) -> bool {
    !matches!(event, FeedEvent::Other(_))
}

async fn live_log(client: EngineClient, tx: UnboundedSender<Msg>, run_id: String, job_id: String, attempt: u32) {
    if attempt > 0 {
        tokio::time::sleep(backoff(attempt)).await;
    }
    let ended = |error: Option<String>| Msg::StreamEnded {
        kind: StreamKind::LiveLog,
        run_id: run_id.clone(),
        job_id: Some(job_id.clone()),
        error,
    };
    let mut stream = match client.live_logs(&run_id, &job_id).await {
        Ok(stream) => stream,
        Err(error) => {
            let _ = tx.send(ended(Some(error.to_string())));
            return;
        }
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(chunk) => {
                let _ = tx.send(Msg::LiveLines {
                    run_id: run_id.clone(),
                    job_id: job_id.clone(),
                    chunk,
                });
            }
            Err(error) => {
                let _ = tx.send(ended(Some(error.to_string())));
                return;
            }
        }
    }
    let _ = tx.send(ended(None));
}

/// Reconnect delay: 1 s, 2 s, 4 s, ... up to [`MAX_BACKOFF`].
fn backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(16);
    Duration::from_secs(1u64 << exponent).min(MAX_BACKOFF)
}

/// Open `url` in the default browser.
fn open_url(url: &str) -> io::Result<()> {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    std::process::Command::new(program)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
}

/// Draw the whole screen: chrome, the active view, and overlays.
pub fn render(app: &App, frame: &mut Frame) {
    let theme = app.theme;
    let area = frame.area();
    let [header, tabs, rule_top, body, rule_bottom, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Min(0),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .areas(area);

    chrome::header(frame, header, &app.header_info(), theme);
    let tab_labels: Vec<(String, bool)> = ViewKind::TABS
        .iter()
        .filter_map(|view| {
            Some((
                format!("{} {}", view.number()?, view.title()),
                *view == app.view,
            ))
        })
        .collect();
    chrome::tabs(frame, tabs, &tab_labels, theme);
    chrome::rule(frame, rule_top, theme);

    let body = match &app.engine_down {
        Some(_) => {
            let [banner, rest] = Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(body);
            let text = format!(
                "no engine at {} — s: setup wizard, S: start engine",
                app.current().engine_url
            );
            chrome::banner(frame, banner, &text, theme);
            rest
        }
        None => body,
    };
    app.view.view().render(app, frame, body);
    draw_overlays(app, frame, body);

    chrome::rule(frame, rule_bottom, theme);
    chrome::footer(frame, footer, &app.footer_hints(), theme);
}

fn draw_overlays(app: &App, frame: &mut Frame, body: Rect) {
    let theme = app.theme;
    if app.help {
        modal::render_help(frame, body, "Help", &help_rows(app.view), theme);
    }
    if let Some(palette) = &app.palette {
        palette::render::<PaletteAction>(frame, body, palette, theme);
    }
    if let Some(confirm) = &app.confirm {
        modal::render_confirm(frame, body, confirm, theme);
    }
    let toasts: Vec<toast::Toast> = app.toasts.clone();
    toast::render_toasts(frame, body, &toasts, theme);
}

fn help_rows(view: ViewKind) -> Vec<(&'static str, &'static str)> {
    let mut rows = vec![
        ("1-7, tab", "switch view"),
        (":", "command palette"),
        ("⇧C", "switch context"),
        ("r", "refresh"),
        ("?", "this help"),
        ("q, ctrl-c", "quit"),
    ];
    match view {
        ViewKind::Runs => rows.extend([
            ("enter / esc", "move focus right / left"),
            ("↑ ↓", "move selection"),
            ("/", "filter runs, or search the log"),
            ("n / N", "next / previous match"),
            ("f", "follow the log"),
            ("c / R", "cancel / rerun the run"),
            ("A", "approve the job or fork run"),
            ("a", "attach to the debug session"),
            ("o", "open on GitHub"),
        ]),
        ViewKind::Overview => rows.extend([
            ("↑ ↓", "move through conditions"),
            ("enter", "jump to the condition's view"),
        ]),
        _ => {}
    }
    rows
}
