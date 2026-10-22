//! Application state and its update function.
//!
//! [`App::update`] is the only place state changes. It takes a [`Msg`] (a key
//! press, a timer tick, a finished fetch, a live feed event) and returns the
//! [`Effect`]s the runtime should carry out. Rendering reads the state and
//! nothing else, so every screen can be reproduced from fixtures.

use chrono::{DateTime, TimeDelta, Utc};
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::actions::{Action, Danger};
use crate::api::{
    DebugSession, ExecStatus, FeedEvent, JobDetail, LiveLogChunk, OperationalSnapshot,
    PoolBindings, RunRecord, RunnersList, StackSnapshot, StepRecord, WebhookDeliveries,
    WebhookHealth,
};
use crate::client::sandbox::SandboxSummary;
use crate::config::StackConfig;
use crate::detectors::{self, LeakedBindingTracker, fmt_age};
use crate::options::StartView;
use crate::store::{LogKey, Store};
use crate::theme::{Theme, Tone};
use crate::views::{ViewKind, overview, runs, setup, stack};
use crate::widgets::chrome::HeaderInfo;
use crate::widgets::modal::Confirm;
use crate::widgets::palette::{Outcome, Palette, PaletteEntry};
use crate::widgets::toast::Toast;
use crate::wizard::{WizardReply, WizardRequest};

/// How often the data behind the open screen is refreshed.
pub const REFRESH_EVERY: TimeDelta = TimeDelta::seconds(5);

/// How long a toast stays on screen.
const TOAST_TTL: TimeDelta = TimeDelta::seconds(4);

/// What the header and the palette need to know about a context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextInfo {
    /// Display name.
    pub name: String,
    /// Engine base URL.
    pub engine_url: String,
    /// Whether the context has a Sandbox Service configured.
    pub sandbox: bool,
}

/// Results of one-shot reads, delivered back to the app.
#[derive(Debug)]
pub enum Fetched {
    /// The operational snapshot. An error means the engine is unreachable.
    Status(Box<Result<OperationalSnapshot, String>>),
    /// The recent runs list.
    Runs(Result<Vec<RunRecord>, String>),
    /// One full run record.
    RunDetail {
        /// Run id.
        run_id: String,
        /// The record, or the error text.
        result: Box<Result<RunRecord, String>>,
    },
    /// Text of one job's or step's log.
    Log {
        /// Which log.
        key: LogKey,
        /// The text, or the error text.
        result: Result<String, String>,
    },
    /// Pool bindings.
    Bindings(Result<PoolBindings, String>),
    /// Webhook deliveries.
    Webhooks(Result<WebhookDeliveries, String>),
    /// Webhook health.
    WebhookHealth(Result<WebhookHealth, String>),
    /// Debug sessions.
    Debug(Result<Vec<DebugSession>, String>),
    /// Sandbox Service summary.
    Sandbox(Result<SandboxSummary, String>),
    /// Registered runners.
    Runners(Result<RunnersList, String>),
    /// GitHub pull requests and stack topology.
    Stack(Result<StackSnapshot, String>),
}

/// Everything that can happen to the app.
#[derive(Debug)]
pub enum Msg {
    /// A key was pressed.
    Key(KeyEvent),
    /// The terminal was resized. Redraw.
    Resized,
    /// The 250 ms tick, with the current time.
    Tick(DateTime<Utc>),
    /// A read finished.
    Fetched(Fetched),
    /// A live feed event for a run.
    Feed {
        /// Run the event belongs to.
        run_id: String,
        /// The event.
        event: FeedEvent,
    },
    /// A live stream ended, cleanly or not.
    StreamEnded {
        /// Which stream.
        kind: StreamKind,
        /// Run the stream was for.
        run_id: String,
        /// Job, for a live-log stream.
        job_id: Option<String>,
        /// The failure, when it ended with one.
        error: Option<String>,
    },
    /// Live console lines for a step of a job.
    LiveLines {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
        /// The lines.
        chunk: LiveLogChunk,
    },
    /// A write (cancel, rerun, approve, replay, purge) finished.
    ActionDone {
        /// What was attempted, for the toast.
        label: String,
        /// Success text or error text.
        result: Result<String, String>,
    },
    /// A hook (start engine, setup wizard) finished.
    Hook {
        /// What was attempted, for the toast.
        label: String,
        /// Nothing on success, the error text otherwise.
        result: Result<(), String>,
    },
    /// The debug shell-out returned.
    ShellOutDone(Result<i32, String>),
    /// A request from the setup wizard thread.
    WizardRequest(WizardRequest),
}

/// A one-shot read the runtime performs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fetch {
    /// `GET /api/v1/status`.
    Status,
    /// `GET /api/v1/runs`.
    Runs,
    /// `GET /api/v1/runs/{id}`.
    RunDetail(String),
    /// A job or step log.
    Log(LogKey),
    /// `GET /api/v1/pool/bindings`.
    Bindings,
    /// `GET /api/v1/webhooks/deliveries`.
    Webhooks,
    /// `GET /api/v1/webhooks/health`.
    WebhookHealth,
    /// `GET /api/v1/debug/sessions`.
    Debug,
    /// The Sandbox Service summary.
    Sandbox,
    /// `GET /api/v1/runners`.
    Runners,
    /// GitHub pull requests configured for the Stack view.
    Stack(StackConfig),
}

/// Which kind of live stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// `events.ndjson` for one run.
    RunEvents,
    /// SSE live-log for one job.
    LiveLog,
}

/// A live stream the runtime should hold open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscription {
    /// Feed of one run. `attempt` drives the reconnect backoff.
    RunEvents {
        /// Run id.
        run_id: String,
        /// 0 for the first connect, then 1, 2, ... after drops.
        attempt: u32,
    },
    /// Live console lines of one job.
    LiveLog {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
        /// 0 for the first connect, then 1, 2, ... after drops.
        attempt: u32,
    },
}

/// Something the runtime must do on the app's behalf.
#[derive(Debug)]
pub enum Effect {
    /// Read something once, then deliver `Msg::Fetched`.
    Fetch(Fetch),
    /// Open a stream, replacing any stream of the same kind.
    Subscribe(Subscription),
    /// Close the stream of this kind, if one is open.
    Unsubscribe(StreamKind),
    /// Perform a write or a hook.
    Act(Action),
    /// Switch the engine client to another context. Closes every stream.
    SwitchContext(usize),
    /// Leave the TUI.
    Quit,
    /// Reply to the setup wizard's current prompt.
    WizardReply(WizardReply),
}

/// What the palette can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteAction {
    /// Open a view.
    Goto(ViewKind),
    /// Perform an action (with its confirmation).
    Do(Action),
    /// Switch context by index.
    SwitchContext(usize),
    /// Select a run in the runs view.
    SelectRun(String),
    /// Refresh everything now.
    Refresh,
}

/// Which stream is open for what.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Streams {
    /// The run feed the app wants open, with its attempt count.
    pub events: Option<(String, u32)>,
    /// The live-log stream the app wants open: run, job, attempt.
    pub live: Option<(String, String, u32)>,
}

/// The whole application state.
pub struct App {
    /// Known contexts, in config order.
    pub contexts: Vec<ContextInfo>,
    /// Index into `contexts` of the active one.
    pub context: usize,
    /// Refuse every mutating action.
    pub read_only: bool,
    /// Colour mode.
    pub theme: Theme,
    /// The current time. The runtime sets it on every tick; tests set it directly.
    pub now: DateTime<Utc>,
    /// The active view.
    pub view: ViewKind,
    /// Everything loaded so far.
    pub store: Store,
    /// Overview screen state.
    pub overview: overview::State,
    /// Runs screen state.
    pub runs: runs::State,
    /// Stack screen configuration and cursor state.
    pub stack_config: StackConfig,
    pub stack: stack::State,
    /// Setup wizard state and the current request/reply form.
    pub setup: setup::State,
    /// Leaked-binding tracker, fed by every status snapshot.
    pub tracker: LeakedBindingTracker,
    /// Streams the app wants open.
    pub streams: Streams,
    /// Why the engine cannot be reached, when it cannot.
    pub engine_down: Option<String>,
    /// The command palette, when open.
    pub palette: Option<Palette<PaletteAction>>,
    /// The confirmation dialog, when open.
    pub confirm: Option<Confirm>,
    /// Whether the help overlay is open.
    pub help: bool,
    /// Transient messages.
    pub toasts: Vec<Toast>,
    /// Set when the screen needs redrawing.
    dirty: bool,
    /// When the data was last refreshed.
    pub last_refresh: Option<DateTime<Utc>>,
}

impl App {
    /// A fresh app showing `start`, with nothing loaded yet.
    pub fn new(
        contexts: Vec<ContextInfo>,
        context: usize,
        start: StartView,
        read_only: bool,
        theme: Theme,
        now: DateTime<Utc>,
    ) -> Self {
        let mut app = Self {
            contexts,
            context,
            read_only,
            theme,
            now,
            view: ViewKind::Overview,
            store: Store::default(),
            overview: overview::State::default(),
            runs: runs::State::default(),
            stack_config: StackConfig::default(),
            stack: stack::State::default(),
            setup: setup::State::default(),
            tracker: LeakedBindingTracker::default(),
            streams: Streams::default(),
            engine_down: None,
            palette: None,
            confirm: None,
            help: false,
            toasts: Vec::new(),
            dirty: true,
            last_refresh: None,
        };
        if let StartView::Run(prefix) = start {
            app.view = ViewKind::Runs;
            app.runs.pending = Some(prefix);
        }
        app
    }

    /// Install the stack configuration resolved by the CLI.
    pub fn set_stack_config(&mut self, config: StackConfig) {
        self.stack_config = config;
    }

    /// The effects that load the first screen. Call once, after `new`.
    pub fn startup(&mut self) -> Vec<Effect> {
        self.last_refresh = Some(self.now);
        self.refresh_all()
    }

    /// The active context.
    pub fn current(&self) -> &ContextInfo {
        &self.contexts[self.context.min(self.contexts.len().saturating_sub(1))]
    }

    /// Whether a redraw is due. Clears the flag.
    pub fn take_dirty(&mut self) -> bool {
        std::mem::take(&mut self.dirty)
    }

    /// The run the Runs screen has selected, when its record is loaded.
    pub fn selected_run(&self) -> Option<&RunRecord> {
        self.store.run(self.runs.run.as_deref()?)
    }

    /// Everything the header shows.
    pub fn header_info(&self) -> HeaderInfo {
        let status = self.store.status.get();
        // Version and uptime describe a live engine; a stale snapshot from
        // before the engine went away must not keep claiming an uptime.
        let live = status.filter(|_| self.engine_down.is_none());
        let health = if self.engine_down.is_some() {
            Some(("no engine".to_owned(), Tone::Error))
        } else {
            status.map(|status| (status.overall.label().to_owned(), overall_tone(status)))
        };
        let sandbox = if self.current().sandbox {
            Some(match self.store.sandbox.get() {
                Some(summary) => {
                    let tone = if summary.providers.iter().any(|provider| !provider.cordoned) {
                        Tone::Success
                    } else {
                        Tone::Warning
                    };
                    (summary.providers.len(), tone)
                }
                None => (0, Tone::Muted),
            })
        } else {
            None
        };
        HeaderInfo {
            context: self.current().name.clone(),
            health,
            version: live.map_or_else(String::new, |status| status.service.version.clone()),
            uptime: live.map_or_else(String::new, |status| {
                fmt_age(TimeDelta::seconds(status.service.uptime_seconds as i64))
            }),
            ready: status.map_or(0, |status| status.jobs.ready),
            claimable: status.map_or(0, |status| status.jobs.claimable),
            pool_idle: status.map_or(0, |status| status.pool.idle),
            pool_busy: status.map_or(0, |status| status.pool.busy),
            sandbox,
        }
    }

    /// Findings from every detector, in display order.
    pub fn findings(&self) -> Vec<detectors::Finding> {
        let mut findings = Vec::new();
        if let Some(finding) = self.tracker.finding(self.now) {
            findings.push(finding);
        }
        if let Some(sessions) = self.store.debug.get() {
            findings.extend(detectors::debug_session_open(sessions));
        }
        if let Some(summary) = self.store.sandbox.get() {
            findings.extend(detectors::cordoned_providers(summary));
        }
        if self.store.webhooks.get().is_some() {
            let last_delivery = self
                .store
                .webhooks
                .get()
                .and_then(|list| list.deliveries.iter().filter_map(|d| d.received_at).max());
            let last_run = self
                .store
                .runs
                .get()
                .and_then(|runs| runs.iter().filter_map(|run| run.created_at).max());
            findings.push(detectors::ingress(self.now, last_delivery, last_run));
        }
        findings
    }

    /// Apply one message and return the effects it causes.
    pub fn update(&mut self, msg: Msg) -> Vec<Effect> {
        match msg {
            Msg::Key(key) => self.on_key(key),
            Msg::Resized => {
                self.dirty = true;
                Vec::new()
            }
            Msg::Tick(now) => self.on_tick(now),
            Msg::Fetched(fetched) => {
                self.dirty = true;
                self.on_fetched(fetched)
            }
            Msg::Feed { run_id, event } => {
                self.dirty = true;
                self.on_feed(&run_id, event);
                Vec::new()
            }
            Msg::StreamEnded {
                kind,
                run_id,
                job_id,
                error,
            } => self.on_stream_ended(kind, run_id, job_id, error),
            Msg::LiveLines {
                run_id,
                job_id,
                chunk,
            } => {
                self.dirty = true;
                self.on_live_lines(run_id, job_id, &chunk);
                Vec::new()
            }
            Msg::ActionDone { label, result } => {
                self.dirty = true;
                match result {
                    Ok(text) => self.toast(format!("{label}: {text}"), Tone::Success),
                    Err(error) => self.toast(format!("{label} failed: {error}"), Tone::Error),
                }
                self.refresh_runs()
            }
            Msg::Hook { label, result } => {
                self.dirty = true;
                match result {
                    Ok(()) => self.toast(format!("{label} started"), Tone::Success),
                    Err(error) => self.toast(format!("{label}: {error}"), Tone::Error),
                }
                Vec::new()
            }
            Msg::WizardRequest(request) => {
                self.setup.receive(request);
                self.view = ViewKind::Setup;
                self.dirty = true;
                Vec::new()
            }
            Msg::ShellOutDone(result) => {
                self.dirty = true;
                match result {
                    Ok(0) => self.toast("debug session closed".to_owned(), Tone::Success),
                    Ok(code) => self.toast(format!("debug exited with {code}"), Tone::Warning),
                    Err(error) => self.toast(format!("debug: {error}"), Tone::Error),
                }
                self.refresh_runs()
            }
        }
    }

    fn toast(&mut self, text: String, tone: Tone) {
        self.toasts.push(Toast {
            text,
            tone,
            until: self.now + TOAST_TTL,
        });
    }

    /// Show a warning toast. Used by views for "nothing to do here" feedback.
    pub fn notify(&mut self, text: &str) {
        self.toast(text.to_owned(), Tone::Warning);
    }

    fn on_tick(&mut self, now: DateTime<Utc>) -> Vec<Effect> {
        let previous_second = self.now.timestamp();
        self.now = now;
        let before = self.toasts.len();
        self.toasts.retain(|toast| toast.until > now);
        if self.toasts.len() != before {
            self.dirty = true;
        }
        if now.timestamp() != previous_second && self.has_running_work() {
            self.dirty = true;
        }
        let due = self
            .last_refresh
            .is_none_or(|last| now - last >= REFRESH_EVERY);
        if due {
            self.last_refresh = Some(now);
            return self.refresh_all();
        }
        Vec::new()
    }

    /// Whether anything on screen shows a clock that is still running.
    fn has_running_work(&self) -> bool {
        self.selected_run()
            .is_some_and(|run| run.status.is_active() || run.jobs_list.iter().any(job_is_running))
    }

    fn on_fetched(&mut self, fetched: Fetched) -> Vec<Effect> {
        match fetched {
            Fetched::Status(result) => match *result {
                Ok(status) => {
                    let observed = status.observed_at.unwrap_or(self.now);
                    self.store.push_ready(observed, status.jobs.ready);
                    self.tracker.observe(observed, &status);
                    self.engine_down = None;
                    self.store.status.succeed(status, self.now);
                    Vec::new()
                }
                Err(error) => {
                    self.engine_down = Some(error.clone());
                    self.store.status.fail(error);
                    Vec::new()
                }
            },
            Fetched::Runs(Ok(mut runs)) => {
                runs.sort_by_key(|run| std::cmp::Reverse(run.run_number));
                self.store.runs.succeed(runs, self.now);
                self.after_selection_change()
            }
            Fetched::Runs(Err(error)) => {
                self.store.runs.fail(error);
                Vec::new()
            }
            Fetched::RunDetail { run_id, result } => {
                let slot = self.store.details.entry(run_id).or_default();
                match *result {
                    Ok(run) => slot.succeed(run, self.now),
                    Err(error) => slot.fail(error),
                }
                self.after_selection_change()
            }
            Fetched::Log { key, result } => {
                let slot = self.store.logs.entry(key).or_default();
                match result {
                    Ok(text) => slot.succeed(text, self.now),
                    Err(error) => slot.fail(error),
                }
                Vec::new()
            }
            Fetched::Bindings(result) => {
                settle(&mut self.store.bindings, result, self.now);
                Vec::new()
            }
            Fetched::Webhooks(result) => {
                settle(&mut self.store.webhooks, result, self.now);
                Vec::new()
            }
            Fetched::WebhookHealth(result) => {
                settle(&mut self.store.webhook_health, result, self.now);
                Vec::new()
            }
            Fetched::Debug(result) => {
                settle(&mut self.store.debug, result, self.now);
                Vec::new()
            }
            Fetched::Sandbox(result) => {
                settle(&mut self.store.sandbox, result, self.now);
                Vec::new()
            }
            Fetched::Runners(result) => {
                settle(&mut self.store.runners, result, self.now);
                Vec::new()
            }
            Fetched::Stack(result) => {
                settle(&mut self.store.stack, result, self.now);
                self.stack.clamp(self.store.stack.get(), &self.stack_config);
                Vec::new()
            }
        }
    }

    fn on_feed(&mut self, run_id: &str, event: FeedEvent) {
        match event {
            FeedEvent::RunStatus { status, .. } => {
                self.set_run_status(run_id, status);
                if let Some(run) = self.store.run_mut(run_id) {
                    run.status = status;
                }
            }
            FeedEvent::JobStatus { job_id, status, .. }
            | FeedEvent::JobCompleted { job_id, status, .. } => {
                if let Some(run) = self.store.run_mut(run_id) {
                    run.jobs.insert(job_id.clone(), status);
                    if let Some(job) = run.job_detail_mut(&job_id) {
                        job.conclusion = status.label().to_owned();
                    }
                }
            }
            FeedEvent::StepStatus(step) => {
                if let Some(run) = self.store.run_mut(&step.run_id) {
                    apply_step(run, &step);
                }
            }
            FeedEvent::Annotation {
                job_id, annotation, ..
            } => {
                self.store
                    .live_annotations
                    .entry((run_id.to_owned(), job_id))
                    .or_default()
                    .push(annotation);
            }
            FeedEvent::RunAccepted { .. } | FeedEvent::Other(_) => {}
        }
        if let Some(events) = self.streams.events.as_mut()
            && events.0 == run_id
        {
            events.1 = 0;
        }
    }

    fn set_run_status(&mut self, run_id: &str, status: ExecStatus) {
        if let Some(entry) = self
            .store
            .runs
            .data
            .as_mut()
            .and_then(|runs| runs.iter_mut().find(|run| run.run_id == run_id))
        {
            entry.status = status;
        }
    }

    fn on_live_lines(&mut self, run_id: String, job_id: String, chunk: &LiveLogChunk) {
        self.store
            .live
            .entry((run_id.clone(), job_id.clone()))
            .or_default()
            .entry(chunk.step_id.clone())
            .or_default()
            .apply(chunk);
        if let Some(live) = self.streams.live.as_mut()
            && live.0 == run_id
            && live.1 == job_id
        {
            live.2 = 0;
        }
    }

    fn on_stream_ended(
        &mut self,
        kind: StreamKind,
        run_id: String,
        job_id: Option<String>,
        error: Option<String>,
    ) -> Vec<Effect> {
        self.dirty = true;
        if let Some(error) = &error {
            tracing::warn!(%error, run = %run_id, "live stream ended");
        }
        match kind {
            StreamKind::RunEvents => {
                let Some((current, attempt)) = self.streams.events.clone() else {
                    return Vec::new();
                };
                if current != run_id {
                    return Vec::new();
                }
                if !self.run_is_active(&run_id) {
                    self.streams.events = None;
                    return Vec::new();
                }
                self.streams.events = Some((run_id.clone(), attempt + 1));
                vec![Effect::Subscribe(Subscription::RunEvents {
                    run_id,
                    attempt: attempt + 1,
                })]
            }
            StreamKind::LiveLog => {
                let Some((current_run, current_job, attempt)) = self.streams.live.clone() else {
                    return Vec::new();
                };
                if current_run != run_id || Some(&current_job) != job_id.as_ref() {
                    return Vec::new();
                }
                if !self.job_is_active(&run_id, &current_job) {
                    self.streams.live = None;
                    return Vec::new();
                }
                self.streams.live = Some((run_id.clone(), current_job.clone(), attempt + 1));
                vec![Effect::Subscribe(Subscription::LiveLog {
                    run_id,
                    job_id: current_job,
                    attempt: attempt + 1,
                })]
            }
        }
    }

    fn run_is_active(&self, run_id: &str) -> bool {
        match self.store.run(run_id) {
            Some(run) => run.status.is_active(),
            None => self
                .store
                .runs
                .get()
                .and_then(|runs| runs.iter().find(|run| run.run_id == run_id))
                .is_none_or(|run| run.status.is_active()),
        }
    }

    fn job_is_active(&self, run_id: &str, job_id: &str) -> bool {
        self.store
            .run(run_id)
            .is_some_and(|run| run.job_status(job_id).is_active())
    }

    /// Bring the screen's selection in line with the loaded data, then return
    /// the streams and fetches the selection needs.
    pub fn after_selection_change(&mut self) -> Vec<Effect> {
        self.normalise_selection();
        let mut effects = self.sync_streams();
        effects.extend(self.ensure_fetches());
        effects
    }

    fn normalise_selection(&mut self) {
        if let Some(prefix) = self.runs.pending.clone()
            && let Some(runs) = self.store.runs.get()
        {
            let found = runs
                .iter()
                .find(|run| run.run_id.starts_with(&prefix))
                .map(|run| run.run_id.clone());
            self.runs.pending = None;
            match found {
                Some(id) => {
                    self.runs.run = Some(id);
                    self.runs.job = None;
                    self.runs.step = None;
                }
                None => self.toast(format!("no run matches {prefix}"), Tone::Error),
            }
        }

        let visible = runs::visible_run_ids(self);
        let selected_visible = self
            .runs
            .run
            .as_ref()
            .is_some_and(|id| visible.contains(id));
        if !selected_visible {
            self.runs.run = visible.first().cloned();
            self.runs.job = None;
            self.runs.step = None;
        }

        let Some(run) = self.runs.run.as_deref().and_then(|id| self.store.run(id)) else {
            return;
        };
        let jobs = runs::leaf_job_ids(run);
        let job_known = self.runs.job.as_ref().is_some_and(|job| jobs.contains(job));
        if !job_known {
            self.runs.job = runs::default_job(run);
            self.runs.step = None;
        }
        let Some(job) = self.runs.job.clone() else {
            return;
        };
        let steps = runs::steps_of(run, &job);
        let step_known = self
            .runs
            .step
            .as_ref()
            .is_some_and(|step| steps.iter().any(|candidate| &candidate.id == step));
        if !step_known {
            self.runs.step = runs::default_step(&steps).map(|step| step.id.clone());
            self.runs.reset_log();
        }
    }

    /// Open or close the streams the selection needs. Only changes are emitted.
    fn sync_streams(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();

        let want_events = self
            .runs
            .run
            .clone()
            .filter(|run_id| self.run_is_active(run_id));
        match (&want_events, &self.streams.events) {
            (Some(run_id), Some((open, _))) if run_id == open => {}
            (Some(run_id), _) => {
                self.streams.events = Some((run_id.clone(), 0));
                effects.push(Effect::Subscribe(Subscription::RunEvents {
                    run_id: run_id.clone(),
                    attempt: 0,
                }));
            }
            (None, Some(_)) => {
                self.streams.events = None;
                effects.push(Effect::Unsubscribe(StreamKind::RunEvents));
            }
            (None, None) => {}
        }

        let want_live = match (self.runs.run.clone(), self.runs.job.clone()) {
            (Some(run_id), Some(job_id)) if self.job_is_active(&run_id, &job_id) => {
                Some((run_id, job_id))
            }
            _ => None,
        };
        match (&want_live, &self.streams.live) {
            (Some((run_id, job_id)), Some((open_run, open_job, _)))
                if run_id == open_run && job_id == open_job => {}
            (Some((run_id, job_id)), _) => {
                self.streams.live = Some((run_id.clone(), job_id.clone(), 0));
                effects.push(Effect::Subscribe(Subscription::LiveLog {
                    run_id: run_id.clone(),
                    job_id: job_id.clone(),
                    attempt: 0,
                }));
            }
            (None, Some(_)) => {
                self.streams.live = None;
                effects.push(Effect::Unsubscribe(StreamKind::LiveLog));
            }
            (None, None) => {}
        }
        effects
    }

    /// Fetches the selection needs and nothing is already loading for.
    fn ensure_fetches(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let Some(run_id) = self.runs.run.clone() else {
            return effects;
        };
        let detail_missing = self
            .store
            .details
            .get(&run_id)
            .is_none_or(|slot| slot.data.is_none() && slot.error.is_none());
        if detail_missing
            && self
                .store
                .details
                .entry(run_id.clone())
                .or_default()
                .begin()
        {
            effects.push(Effect::Fetch(Fetch::RunDetail(run_id.clone())));
        }
        if let Some(key) = runs::finished_log_key(self) {
            let slot = self.store.logs.entry(key.clone()).or_default();
            if slot.data.is_none() && slot.error.is_none() && slot.begin() {
                effects.push(Effect::Fetch(Fetch::Log(key)));
            }
        }
        effects
    }

    /// Start a refresh of every slot the open screens read.
    fn refresh_all(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.store.status.begin() {
            effects.push(Effect::Fetch(Fetch::Status));
        }
        effects.extend(self.refresh_runs());
        if self.store.bindings.begin() {
            effects.push(Effect::Fetch(Fetch::Bindings));
        }
        if self.store.webhooks.begin() {
            effects.push(Effect::Fetch(Fetch::Webhooks));
        }
        if self.store.webhook_health.begin() {
            effects.push(Effect::Fetch(Fetch::WebhookHealth));
        }
        if self.store.debug.begin() {
            effects.push(Effect::Fetch(Fetch::Debug));
        }
        if self.current().sandbox && self.store.sandbox.begin() {
            effects.push(Effect::Fetch(Fetch::Sandbox));
        }
        if self.store.runners.begin() {
            effects.push(Effect::Fetch(Fetch::Runners));
        }
        if self.store.stack.begin() {
            effects.push(Effect::Fetch(Fetch::Stack(self.stack_config.clone())));
        }
        if let Some(run_id) = self.runs.run.clone()
            && self
                .store
                .details
                .entry(run_id.clone())
                .or_default()
                .begin()
        {
            effects.push(Effect::Fetch(Fetch::RunDetail(run_id)));
        }
        effects
    }

    fn refresh_runs(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.store.runs.begin() {
            effects.push(Effect::Fetch(Fetch::Runs));
        }
        if let Some(run_id) = self.runs.run.clone()
            && self
                .store
                .details
                .entry(run_id.clone())
                .or_default()
                .begin()
        {
            effects.push(Effect::Fetch(Fetch::RunDetail(run_id)));
        }
        effects
    }

    fn on_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return Vec::new();
        }
        self.dirty = true;

        if self.palette.is_some() {
            return self.on_palette_key(key);
        }
        if self.confirm.is_some() {
            return self.on_confirm_key(key);
        }
        if self.help {
            self.help = false;
            return Vec::new();
        }
        if self.runs.filter_editing || self.runs.search_editing {
            return runs::on_text_key(self, key);
        }

        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => return vec![Effect::Quit],
            KeyCode::Char('q') if !ctrl => return vec![Effect::Quit],
            KeyCode::Char(digit @ '1'..='7') if !ctrl => {
                let number = digit as usize - '0' as usize;
                if let Some(view) = ViewKind::from_number(number) {
                    return self.show(view);
                }
            }
            KeyCode::Tab => return self.show(self.view.next_tab()),
            KeyCode::BackTab => return self.show(self.view.previous_tab()),
            KeyCode::Char(':') => {
                self.open_palette(String::new());
                return Vec::new();
            }
            KeyCode::Char('C') => {
                self.open_palette("ctx ".to_owned());
                return Vec::new();
            }
            KeyCode::Char('?') => {
                self.help = true;
                return Vec::new();
            }
            KeyCode::Char('r') if !ctrl => return self.refresh_all(),
            KeyCode::Char('s') => {
                return self.run_action(Action::RunWizard);
            }
            KeyCode::Char('S') if self.engine_down.is_some() => {
                return self.run_action(Action::StartEngine);
            }
            _ => {}
        }

        let view = self.view.view();
        view.on_key(self, key)
    }

    /// Switch to `view`, returning whatever it needs to load.
    pub fn show(&mut self, view: ViewKind) -> Vec<Effect> {
        self.view = view;
        self.dirty = true;
        let mut effects = self.refresh_all();
        effects.extend(view.view().on_show(self));
        effects
    }

    fn open_palette(&mut self, query: String) {
        let palette = Palette::new(self.palette_entries()).with_query(&query);
        self.palette = Some(palette);
    }

    fn palette_entries(&self) -> Vec<PaletteEntry<PaletteAction>> {
        let mut entries = Vec::new();
        for view in ViewKind::TABS {
            entries.push(PaletteEntry {
                label: format!("view {}", view.title().to_lowercase()),
                detail: format!("tab {}", view.number().unwrap_or(0)),
                danger: false,
                action: PaletteAction::Goto(view),
            });
        }
        entries.push(PaletteEntry {
            label: "refresh".into(),
            detail: "reload every screen".into(),
            danger: false,
            action: PaletteAction::Refresh,
        });
        for (index, context) in self.contexts.iter().enumerate() {
            entries.push(PaletteEntry {
                label: format!("ctx {}", context.name),
                detail: context.engine_url.clone(),
                danger: false,
                action: PaletteAction::SwitchContext(index),
            });
        }
        for run in self.store.runs.get().into_iter().flatten().take(50) {
            entries.push(PaletteEntry {
                label: format!(
                    "run {} #{} {}",
                    run.short_id(),
                    run.run_number,
                    run.workflow_name()
                ),
                detail: run.ref_label(),
                danger: false,
                action: PaletteAction::SelectRun(run.run_id.clone()),
            });
        }
        if let Some(run) = self.selected_run() {
            let run_id = run.run_id.clone();
            let mut actions = vec![Action::RerunRun {
                run_id: run_id.clone(),
            }];
            if run.status.is_active() {
                actions.insert(
                    0,
                    Action::CancelRun {
                        run_id: run_id.clone(),
                    },
                );
            }
            for action in actions {
                entries.push(PaletteEntry {
                    label: action.label(),
                    detail: "Runs".into(),
                    danger: action.danger() != Danger::None,
                    action: PaletteAction::Do(action),
                });
            }
        }
        entries.push(PaletteEntry {
            label: "setup wizard".into(),
            detail: "configure credentials and golden image".into(),
            danger: false,
            action: PaletteAction::Do(Action::RunWizard),
        });
        for runner in self
            .store
            .runners
            .get()
            .into_iter()
            .flat_map(|list| list.runners.iter())
        {
            entries.push(PaletteEntry {
                label: format!("purge runner {}", runner.name),
                detail: "Fleet".into(),
                danger: true,
                action: PaletteAction::Do(Action::PurgeRunner {
                    name: runner.name.clone(),
                }),
            });
        }
        for delivery in self
            .store
            .webhooks
            .get()
            .into_iter()
            .flat_map(|list| list.deliveries.iter())
            .filter(|delivery| delivery.state == "failed")
        {
            entries.push(PaletteEntry {
                label: format!("replay delivery {}", delivery.delivery_id),
                detail: format!("{} · {} attempts", delivery.event, delivery.attempts),
                danger: true,
                action: PaletteAction::Do(Action::ReplayDelivery {
                    delivery_id: delivery.delivery_id.clone(),
                }),
            });
        }
        entries.push(PaletteEntry {
            label: "setup screen".into(),
            detail: "wizard state".into(),
            danger: false,
            action: PaletteAction::Goto(ViewKind::Setup),
        });
        entries.push(PaletteEntry {
            label: "start engine".into(),
            detail: "preloop serve, detached".into(),
            danger: false,
            action: PaletteAction::Do(Action::StartEngine),
        });
        entries
    }

    fn on_palette_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let Some(palette) = self.palette.as_mut() else {
            return Vec::new();
        };
        match palette.handle_key(key) {
            Outcome::Consumed => Vec::new(),
            Outcome::Close => {
                self.palette = None;
                Vec::new()
            }
            Outcome::Run(action) => {
                self.palette = None;
                match action {
                    PaletteAction::Goto(view) => self.show(view),
                    PaletteAction::Do(action) => self.run_action(action),
                    PaletteAction::SwitchContext(index) => self.switch_context(index),
                    PaletteAction::SelectRun(run_id) => {
                        self.view = ViewKind::Runs;
                        self.runs.run = Some(run_id);
                        self.runs.job = None;
                        self.runs.step = None;
                        let mut effects = self.refresh_all();
                        effects.extend(self.after_selection_change());
                        effects
                    }
                    PaletteAction::Refresh => self.refresh_all(),
                }
            }
        }
    }

    fn switch_context(&mut self, index: usize) -> Vec<Effect> {
        if index >= self.contexts.len() || index == self.context {
            return Vec::new();
        }
        self.context = index;
        self.store = Store::default();
        self.tracker = LeakedBindingTracker::default();
        self.streams = Streams::default();
        self.engine_down = None;
        self.runs = runs::State::default();
        self.overview = overview::State::default();
        self.stack = stack::State::default();
        self.last_refresh = None;
        self.toast(format!("context {}", self.current().name), Tone::Accent);
        let mut effects = vec![Effect::SwitchContext(index)];
        effects.extend(self.refresh_all());
        effects
    }

    fn on_confirm_key(&mut self, key: KeyEvent) -> Vec<Effect> {
        let Some(confirm) = self.confirm.as_mut() else {
            return Vec::new();
        };
        match key.code {
            KeyCode::Esc => {
                self.confirm = None;
                Vec::new()
            }
            KeyCode::Enter if confirm.can_confirm() => {
                let action = confirm.action.clone();
                self.confirm = None;
                self.execute(action)
            }
            _ => {
                if confirm.required.is_some() {
                    confirm.input.handle_key(key);
                }
                Vec::new()
            }
        }
    }

    /// Run an action, asking for confirmation first when it needs it.
    pub fn run_action(&mut self, action: Action) -> Vec<Effect> {
        if action.mutates() && self.read_only {
            self.toast(
                format!("read-only: {} is disabled", action.label()),
                Tone::Warning,
            );
            return Vec::new();
        }
        match action.danger() {
            Danger::None => self.execute(action),
            Danger::Confirm => {
                self.confirm = Some(Confirm::new(
                    action.clone(),
                    confirm_lines(self, &action),
                    None,
                ));
                Vec::new()
            }
            Danger::TypeToConfirm(id) => {
                self.confirm = Some(Confirm::new(
                    action.clone(),
                    confirm_lines(self, &action),
                    Some(id),
                ));
                Vec::new()
            }
        }
    }

    fn execute(&mut self, action: Action) -> Vec<Effect> {
        if matches!(action, Action::RunWizard) {
            self.setup.begin();
            self.view = ViewKind::Setup;
        }
        vec![Effect::Act(action)]
    }

    /// The footer hints for the active view.
    pub fn footer_hints(&self) -> Vec<String> {
        self.view.view().hints(self)
    }
}

fn overall_tone(status: &OperationalSnapshot) -> Tone {
    use crate::api::Overall;
    match status.overall {
        Overall::Ok => Tone::Success,
        Overall::Degraded | Overall::ShuttingDown => Tone::Warning,
        Overall::Blocked => Tone::Error,
        Overall::Unknown => Tone::Muted,
    }
}

fn settle<T>(slot: &mut crate::store::Loadable<T>, result: Result<T, String>, now: DateTime<Utc>) {
    match result {
        Ok(data) => slot.succeed(data, now),
        Err(error) => slot.fail(error),
    }
}

/// Apply a `step_status` event to a run's job detail, creating what is missing.
fn apply_step(run: &mut RunRecord, event: &crate::api::StepStatus) {
    let job_index = match run
        .jobs_list
        .iter()
        .position(|job| job.job_id == event.job_id)
    {
        Some(index) => index,
        None => {
            run.jobs_list.push(JobDetail {
                job_id: event.job_id.clone(),
                name: run.job_label(&event.job_id).to_owned(),
                ..Default::default()
            });
            run.jobs_list.len() - 1
        }
    };
    let job = &mut run.jobs_list[job_index];
    match job.steps.iter_mut().find(|step| step.id == event.step_id) {
        Some(step) => {
            step.conclusion = event.conclusion.clone();
            step.finished_at = event.finished_at;
            if event.started_at.is_some() {
                step.started_at = event.started_at;
            }
            if !event.name.is_empty() {
                step.name = event.name.clone();
            }
        }
        None => job.steps.push(StepRecord {
            id: event.step_id.clone(),
            kind: if event.workflow_index.is_some() {
                "workflow".to_owned()
            } else {
                "synthetic".to_owned()
            },
            workflow_index: event.workflow_index,
            runner_number: event.runner_number,
            name: event.name.clone(),
            conclusion: event.conclusion.clone(),
            started_at: event.started_at,
            finished_at: event.finished_at,
            ..Default::default()
        }),
    }
}

fn job_is_running(job: &JobDetail) -> bool {
    job.steps
        .iter()
        .any(|step| step.status() == ExecStatus::InProgress)
}

fn confirm_lines(app: &App, action: &Action) -> Vec<String> {
    match action {
        Action::CancelRun { run_id } => {
            let mut lines = vec![format!("run {}", &run_id[..run_id.len().min(8)])];
            if let Some(run) = app.store.run(run_id) {
                lines.push(format!("{} · #{}", run.workflow_name(), run.run_number));
            }
            lines.push("running jobs are stopped; the run ends as cancelled".to_owned());
            lines
        }
        Action::ApproveJob { job_id, .. } => {
            vec![format!(
                "job {job_id} runs with the run's secrets once approved"
            )]
        }
        Action::ApproveFork { .. } => {
            vec!["fork code runs with secrets once approved".to_owned()]
        }
        Action::PurgeRunner { name } => {
            vec![format!(
                "{name} is removed from the engine and its VM is released"
            )]
        }
        Action::ReplayDelivery { delivery_id } => {
            vec![format!(
                "delivery {delivery_id} is queued again for processing"
            )]
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use ratatui::crossterm::event::KeyModifiers;

    use super::*;
    use crate::api::{ExecStatus, FeedEvent, StepStatus};

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 22, 12, 20, 0)
            .single()
            .expect("valid time")
    }

    fn app() -> App {
        App::new(
            vec![
                ContextInfo {
                    name: "cpane".into(),
                    engine_url: "http://127.0.0.1:9090".into(),
                    sandbox: false,
                },
                ContextInfo {
                    name: "main".into(),
                    engine_url: "http://10.0.0.2:9090".into(),
                    sandbox: false,
                },
            ],
            0,
            StartView::Overview,
            false,
            Theme::monochrome(),
            now(),
        )
    }

    fn key(code: KeyCode) -> Msg {
        Msg::Key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn run(id: &str, number: u64, status: ExecStatus) -> RunRecord {
        RunRecord {
            run_id: id.into(),
            run_number: number,
            status,
            workflow_path_str: ".github/workflows/ci.yml".into(),
            jobs: [("test".to_owned(), ExecStatus::InProgress)]
                .into_iter()
                .collect(),
            jobs_list: vec![JobDetail {
                job_id: "test".into(),
                name: "test".into(),
                steps: vec![StepRecord {
                    id: "s1".into(),
                    name: "cargo test".into(),
                    workflow_index: Some(0),
                    conclusion: "in_progress".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn feed_runs(app: &mut App, runs: Vec<RunRecord>) -> Vec<Effect> {
        app.update(Msg::Fetched(Fetched::Runs(Ok(runs))))
    }

    #[test]
    fn number_keys_switch_views_and_q_quits() {
        let mut app = app();
        let effects = app.update(key(KeyCode::Char('2')));
        assert_eq!(app.view, ViewKind::Runs);
        assert!(
            effects
                .iter()
                .any(|effect| matches!(effect, Effect::Fetch(Fetch::Runs)))
        );
        let quit = app.update(key(KeyCode::Char('q')));
        assert!(matches!(quit.as_slice(), [Effect::Quit]));
    }

    #[test]
    fn selecting_a_run_subscribes_to_its_feed_and_switching_cancels_the_old_one() {
        let mut app = app();
        app.update(key(KeyCode::Char('2')));
        let effects = feed_runs(
            &mut app,
            vec![
                run("aaaaaaaa-1", 2, ExecStatus::InProgress),
                run("bbbbbbbb-2", 1, ExecStatus::InProgress),
            ],
        );
        assert_eq!(app.runs.run.as_deref(), Some("aaaaaaaa-1"));
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Subscribe(Subscription::RunEvents { run_id, attempt: 0 }) if run_id == "aaaaaaaa-1"
        )));

        app.runs.run = Some("bbbbbbbb-2".into());
        let effects = app.after_selection_change();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Subscribe(Subscription::RunEvents { run_id, .. }) if run_id == "bbbbbbbb-2"
        )));
        assert_eq!(
            app.streams.events.as_ref().map(|s| s.0.as_str()),
            Some("bbbbbbbb-2")
        );
    }

    #[test]
    fn step_status_events_update_steps_in_place() {
        let mut app = app();
        app.update(key(KeyCode::Char('2')));
        feed_runs(&mut app, vec![run("aaaaaaaa-1", 2, ExecStatus::InProgress)]);
        app.update(Msg::Fetched(Fetched::RunDetail {
            run_id: "aaaaaaaa-1".into(),
            result: Box::new(Ok(run("aaaaaaaa-1", 2, ExecStatus::InProgress))),
        }));
        app.update(Msg::Feed {
            run_id: "aaaaaaaa-1".into(),
            event: FeedEvent::StepStatus(StepStatus {
                run_id: "aaaaaaaa-1".into(),
                job_id: "test".into(),
                step_id: "s1".into(),
                workflow_index: Some(0),
                runner_number: Some(2),
                name: "cargo test".into(),
                conclusion: "failure".into(),
                started_at: None,
                finished_at: Some(now()),
            }),
        });
        let step = &app.store.run("aaaaaaaa-1").expect("loaded").jobs_list[0].steps[0];
        assert_eq!(step.status(), ExecStatus::Failure);
        assert_eq!(step.finished_at, Some(now()));
    }

    #[test]
    fn unknown_step_is_appended_to_its_job() {
        let mut detail = run("r", 1, ExecStatus::InProgress);
        apply_step(
            &mut detail,
            &StepStatus {
                run_id: "r".into(),
                job_id: "test".into(),
                step_id: "new".into(),
                workflow_index: None,
                runner_number: Some(1),
                name: "Set up job".into(),
                conclusion: "in_progress".into(),
                started_at: None,
                finished_at: None,
            },
        );
        let steps = &detail.jobs_list[0].steps;
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[1].kind, "synthetic");
    }

    #[test]
    fn cancel_asks_for_confirmation_and_read_only_refuses() {
        let mut app = app();
        let effects = app.run_action(Action::CancelRun { run_id: "r".into() });
        assert!(effects.is_empty());
        assert!(app.confirm.is_some());
        let effects = app.update(key(KeyCode::Enter));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Act(Action::CancelRun { .. })]
        ));

        app.read_only = true;
        assert!(
            app.run_action(Action::RerunRun { run_id: "r".into() })
                .is_empty()
        );
        assert!(app.confirm.is_none());
    }

    #[test]
    fn purge_requires_typing_the_runner_name() {
        let mut app = app();
        app.run_action(Action::PurgeRunner {
            name: "plr-4".into(),
        });
        app.update(key(KeyCode::Enter));
        assert!(
            app.confirm.is_some(),
            "enter does nothing until the name matches"
        );
        for c in "plr-4".chars() {
            app.update(key(KeyCode::Char(c)));
        }
        let effects = app.update(key(KeyCode::Enter));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Act(Action::PurgeRunner { .. })]
        ));
    }

    #[test]
    fn live_stream_drop_resubscribes_with_backoff_only_while_running() {
        let mut app = app();
        app.update(key(KeyCode::Char('2')));
        feed_runs(&mut app, vec![run("aaaaaaaa-1", 2, ExecStatus::InProgress)]);
        let effects = app.update(Msg::StreamEnded {
            kind: StreamKind::RunEvents,
            run_id: "aaaaaaaa-1".into(),
            job_id: None,
            error: Some("reset".into()),
        });
        assert!(matches!(
            effects.as_slice(),
            [Effect::Subscribe(Subscription::RunEvents {
                attempt: 1,
                ..
            })]
        ));

        app.update(Msg::Feed {
            run_id: "aaaaaaaa-1".into(),
            event: FeedEvent::RunStatus {
                run_id: "aaaaaaaa-1".into(),
                status: ExecStatus::Success,
            },
        });
        let effects = app.update(Msg::StreamEnded {
            kind: StreamKind::RunEvents,
            run_id: "aaaaaaaa-1".into(),
            job_id: None,
            error: None,
        });
        assert!(effects.is_empty(), "a finished run is not reconnected");
    }

    #[test]
    fn tick_refreshes_every_five_seconds() {
        let mut app = app();
        let first = app.startup();
        assert!(
            first
                .iter()
                .any(|effect| matches!(effect, Effect::Fetch(Fetch::Status)))
        );
        app.store.status.loading = false;
        assert!(
            app.update(Msg::Tick(now() + TimeDelta::seconds(1)))
                .is_empty()
        );
        let later = app.update(Msg::Tick(now() + TimeDelta::seconds(6)));
        assert!(
            later
                .iter()
                .any(|effect| matches!(effect, Effect::Fetch(Fetch::Status)))
        );
    }

    #[test]
    fn palette_runs_a_view_switch() {
        let mut app = app();
        app.update(key(KeyCode::Char(':')));
        for c in "webhooks".chars() {
            app.update(key(KeyCode::Char(c)));
        }
        app.update(key(KeyCode::Enter));
        assert_eq!(app.view, ViewKind::Webhooks);
        assert!(app.palette.is_none());
    }

    #[test]
    fn engine_down_shows_the_setup_keys_and_s_starts_the_wizard() {
        let mut app = app();
        app.update(Msg::Fetched(Fetched::Status(Box::new(Err(
            "connection refused".into(),
        )))));
        assert!(app.engine_down.is_some());
        let effects = app.update(key(KeyCode::Char('s')));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Act(Action::RunWizard)]
        ));
    }
}
