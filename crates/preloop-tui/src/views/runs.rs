//! The runs screen: runs → jobs → steps → log, with a filter bar on top.
//!
//! Layout follows the approved mockup: a filter row, then four columns of
//! 32, 26, 22 and 40 columns. The log column carries a debug panel along its
//! bottom when the selected run has a paused debug session.
//!
//! Selection is kept as ids (run, job, step), not positions, so a refresh that
//! reorders the lists keeps the user where they were.

use chrono::{DateTime, TimeDelta, Utc};
use ratatui::Frame;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::View;
use crate::actions::Action;
use crate::api::{DebugSession, ExecStatus, JobDetail, RunRecord, StepRecord, sort_steps};
use crate::app::{App, Effect};
use crate::detectors::{fmt_age, fmt_clock};
use crate::store::LogKey;
use crate::theme::{Theme, Tone};
use crate::widgets::columns::{fixed_columns, pane, pane_open_bottom};
use crate::widgets::list::{ListItem, render_list};
use crate::widgets::log_view::{LineKind, LogLine, LogView, classify};
use crate::widgets::text_input::TextInput;

/// Columns of the four panes, from the mockup.
const COLUMN_WIDTHS: [u16; 4] = [32, 26, 22, 40];
/// Rows of the debug panel under the log, borders included.
const DEBUG_PANEL_ROWS: u16 = 5;
/// Scroll step the log assumes when a key arrives (the render clamps the rest).
const LOG_PAGE: usize = 20;

/// The four panes of the screen, left to right.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Pane {
    /// Run list.
    #[default]
    Runs,
    /// Jobs of the selected run.
    Jobs,
    /// Steps of the selected job.
    Steps,
    /// The log of the selected step.
    Log,
}

impl Pane {
    fn left(self) -> Self {
        match self {
            Self::Runs | Self::Jobs => Self::Runs,
            Self::Steps => Self::Jobs,
            Self::Log => Self::Steps,
        }
    }

    fn right(self) -> Self {
        match self {
            Self::Runs => Self::Jobs,
            Self::Jobs => Self::Steps,
            Self::Steps | Self::Log => Self::Log,
        }
    }
}

/// Runs screen state.
#[derive(Debug, Clone, Default)]
pub struct State {
    /// A run id prefix from `--run`, resolved when the runs list arrives.
    pub pending: Option<String>,
    /// Selected run id.
    pub run: Option<String>,
    /// Selected job id.
    pub job: Option<String>,
    /// Selected step id.
    pub step: Option<String>,
    /// Focused pane.
    pub focus: Pane,
    /// The filter text, like `status:failure,running workflow:ci`.
    pub filter: TextInput,
    /// True while the filter box takes keys.
    pub filter_editing: bool,
    /// The log search box.
    pub search: TextInput,
    /// True while the search box takes keys.
    pub search_editing: bool,
    /// Log scroll, follow and folding state.
    pub log: LogView,
}

impl State {
    /// Forget log position and folds, for a new step.
    pub fn reset_log(&mut self) {
        self.log = LogView::new();
    }
}

/// Parsed filter text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    /// `status:` values, lowercase.
    pub statuses: Vec<String>,
    /// `workflow:` substrings, lowercase.
    pub workflows: Vec<String>,
    /// `event:` values, lowercase.
    pub events: Vec<String>,
    /// Free words, each matched as a substring of the run's text.
    pub words: Vec<String>,
}

/// Parse filter text such as `status:failure,running workflow:ci`.
pub fn parse_filter(text: &str) -> Filter {
    let mut filter = Filter::default();
    for token in text.split_whitespace() {
        let lowered = token.to_lowercase();
        match lowered.split_once(':') {
            Some(("status", value)) => filter.statuses.extend(
                value
                    .split(',')
                    .filter(|v| !v.is_empty())
                    .map(str::to_owned),
            ),
            Some(("workflow" | "wf", value)) => filter.workflows.push(value.to_owned()),
            Some(("event", value)) => filter.events.push(value.to_owned()),
            _ => filter.words.push(lowered),
        }
    }
    filter
}

/// Whether a run passes the filter.
pub fn run_matches(run: &RunRecord, filter: &Filter) -> bool {
    let status_ok = filter.statuses.is_empty()
        || filter.statuses.iter().any(|wanted| {
            wanted == run.status.label()
                || (wanted == "in_progress" && run.status == ExecStatus::InProgress)
        });
    let workflow = run.workflow_name().to_lowercase();
    let workflow_ok = filter
        .workflows
        .iter()
        .all(|wanted| workflow.contains(wanted.as_str()));
    let event = if run.event.is_empty() {
        run.submission.event.to_lowercase()
    } else {
        run.event.to_lowercase()
    };
    let event_ok = filter.events.iter().all(|wanted| event == *wanted);
    let haystack = format!(
        "#{} {} {} {} {}",
        run.run_number,
        workflow,
        event,
        run.ref_label().to_lowercase(),
        run.short_id()
    );
    status_ok
        && workflow_ok
        && event_ok
        && filter
            .words
            .iter()
            .all(|word| haystack.contains(word.as_str()))
}

/// Ids of the runs that pass the current filter, newest first.
pub fn visible_run_ids(app: &App) -> Vec<String> {
    let filter = parse_filter(app.runs.filter.value());
    app.store
        .runs
        .get()
        .into_iter()
        .flatten()
        .filter(|run| run_matches(run, &filter))
        .map(|run| run.run_id.clone())
        .collect()
}

/// Job ids of a run in display order: the detail order, then any job only the
/// status map knows about.
pub fn ordered_job_ids(run: &RunRecord) -> Vec<String> {
    let mut ids: Vec<String> = run.jobs_list.iter().map(|job| job.job_id.clone()).collect();
    for job_id in run.jobs.keys() {
        if !ids.contains(job_id) {
            ids.push(job_id.clone());
        }
    }
    ids
}

/// Selectable job ids, the ones the jobs pane can highlight.
pub fn leaf_job_ids(run: &RunRecord) -> Vec<String> {
    ordered_job_ids(run)
}

/// Default job: the first failed one, else the first active one, else the first.
pub fn default_job(run: &RunRecord) -> Option<String> {
    let ids = ordered_job_ids(run);
    ids.iter()
        .find(|id| run.job_status(id) == ExecStatus::Failure)
        .or_else(|| ids.iter().find(|id| run.job_status(id).is_active()))
        .or_else(|| ids.first())
        .cloned()
}

/// A job's steps, in execution order.
pub fn steps_of(run: &RunRecord, job_id: &str) -> Vec<StepRecord> {
    let mut steps = run
        .job_detail(job_id)
        .map(|job| job.steps.clone())
        .unwrap_or_default();
    sort_steps(&mut steps);
    steps
}

/// Default step: the first failed one, else the running one, else the last.
pub fn default_step(steps: &[StepRecord]) -> Option<&StepRecord> {
    steps
        .iter()
        .find(|step| step.status() == ExecStatus::Failure)
        .or_else(|| {
            steps
                .iter()
                .find(|step| step.status() == ExecStatus::InProgress)
        })
        .or_else(|| steps.last())
}

/// The status of a job, as the job pane shows it: the run's map, falling back
/// to the job detail's conclusion.
fn job_status(run: &RunRecord, job_id: &str) -> ExecStatus {
    match run.jobs.get(job_id) {
        Some(status) => *status,
        None => run
            .job_detail(job_id)
            .map(|job| ExecStatus::from_conclusion(&job.conclusion))
            .unwrap_or_default(),
    }
}

/// Elapsed time of a job: first step start to last step finish, or to `now`.
pub fn job_elapsed(job: &JobDetail, now: DateTime<Utc>) -> Option<TimeDelta> {
    let start = job.steps.iter().filter_map(|step| step.started_at).min()?;
    let end = job
        .steps
        .iter()
        .map(|step| step.finished_at.unwrap_or(now))
        .max()?;
    Some((end - start).max(TimeDelta::zero()))
}

/// One row of the jobs pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRow {
    /// A matrix group heading.
    Header {
        /// The matrix base id.
        base: String,
        /// How many legs the group has.
        legs: usize,
    },
    /// A job, or one leg of a matrix.
    Job {
        /// Job id.
        job_id: String,
        /// Label shown in the row.
        label: String,
        /// Whether the row is a matrix leg.
        leg: bool,
        /// The job's status.
        status: ExecStatus,
        /// Elapsed time, when the job has started.
        elapsed: Option<TimeDelta>,
    },
}

/// Group a run's jobs the way the jobs pane shows them: matrix legs sharing a
/// base id sit under a heading.
pub fn job_rows(run: &RunRecord, now: DateTime<Utc>) -> Vec<JobRow> {
    let ids = ordered_job_ids(run);
    let base_of = |id: &str| {
        run.job_base_ids
            .get(id)
            .cloned()
            .unwrap_or_else(|| id.to_owned())
    };
    let mut rows = Vec::new();
    let mut index = 0;
    while index < ids.len() {
        let base = base_of(&ids[index]);
        let group_end = ids[index..]
            .iter()
            .position(|id| base_of(id) != base)
            .map_or(ids.len(), |offset| index + offset);
        let group = &ids[index..group_end];
        let legs = group.len();
        let is_matrix = legs > 1 || group.iter().any(|id| base_of(id) != *id);
        if is_matrix {
            rows.push(JobRow::Header {
                base: base.clone(),
                legs,
            });
        }
        for job_id in group {
            let label = if is_matrix {
                leg_label(run.job_label(job_id), &base)
            } else {
                run.job_label(job_id).to_owned()
            };
            rows.push(JobRow::Job {
                job_id: job_id.clone(),
                label,
                leg: is_matrix,
                status: job_status(run, job_id),
                elapsed: run.job_detail(job_id).and_then(|job| job_elapsed(job, now)),
            });
        }
        index = group_end;
    }
    rows
}

/// `test (runner)` under base `test` shows as `runner`.
fn leg_label(name: &str, base: &str) -> String {
    name.strip_prefix(base)
        .map(|rest| {
            rest.trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .trim()
                .to_owned()
        })
        .filter(|rest| !rest.is_empty())
        .unwrap_or_else(|| name.to_owned())
}

/// The selected run's job rows.
fn selected_job_rows(app: &App) -> Vec<JobRow> {
    app.selected_run()
        .map(|run| job_rows(run, app.now))
        .unwrap_or_default()
}

/// The log key of the selected step, when its finished log must be fetched.
/// `None` for a live step, a step with live lines, or a step without a
/// workflow index (those have no addressable log).
pub fn finished_log_key(app: &App) -> Option<LogKey> {
    let (run, job, step) = selected(app)?;
    if step.status().is_active() {
        return None;
    }
    if live_lines(app, &run.run_id, &job.job_id, &step.id).is_some() {
        return None;
    }
    let index = step.workflow_index?;
    Some(LogKey {
        run_id: run.run_id.clone(),
        job_id: job.job_id.clone(),
        step: Some(index + 1),
    })
}

/// The selected run, job and step records.
fn selected(app: &App) -> Option<(&RunRecord, &JobDetail, StepRecord)> {
    let run = app.selected_run()?;
    let job_id = app.runs.job.as_ref()?;
    let job = run.job_detail(job_id)?;
    let step_id = app.runs.step.as_ref()?;
    let step = job.steps.iter().find(|step| &step.id == step_id)?.clone();
    Some((run, job, step))
}

/// Live console lines for one step, when any have arrived.
fn live_lines<'a>(
    app: &'a App,
    run_id: &str,
    job_id: &str,
    step_id: &str,
) -> Option<&'a crate::store::StepLog> {
    app.store
        .live
        .get(&(run_id.to_owned(), job_id.to_owned()))?
        .get(step_id)
        .filter(|log| !log.is_empty())
}

/// What the log pane shows and why.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogPane {
    /// The lines, with annotations after them.
    pub lines: Vec<LogLine>,
    /// Shown instead of lines when there are none.
    pub note: Option<String>,
}

/// Build the log pane for the selected step.
pub fn log_pane(app: &App) -> LogPane {
    let Some((run, job, step)) = selected(app) else {
        return LogPane {
            lines: Vec::new(),
            note: Some("select a step".to_owned()),
        };
    };
    let mut lines: Vec<LogLine> = Vec::new();
    let mut note = None;
    if let Some(live) = live_lines(app, &run.run_id, &job.job_id, &step.id) {
        lines = live
            .numbered()
            .into_iter()
            .map(|(number, text)| LogLine { number, text })
            .collect();
    } else if let Some(key) = log_key_for(run, job, &step) {
        match app.store.logs.get(&key) {
            Some(slot) if slot.data.is_some() => {
                lines = slot
                    .data
                    .as_deref()
                    .unwrap_or_default()
                    .lines()
                    .enumerate()
                    .map(|(index, text)| LogLine {
                        number: index as u64 + 1,
                        text: text.to_owned(),
                    })
                    .collect();
            }
            Some(slot) if slot.error.is_some() => {
                note = Some(format!(
                    "log unavailable: {}",
                    slot.error.clone().unwrap_or_default()
                ));
            }
            _ => note = Some("loading log…".to_owned()),
        }
    } else if step.status().is_active() {
        note = Some("waiting for output…".to_owned());
    } else {
        note = Some("no separate log for this step".to_owned());
    }

    let annotations = annotation_rows(app, run, job, &step);
    if !annotations.is_empty() {
        let start = lines.last().map_or(1, |line| line.number + 1);
        for (number, text) in (start..).zip(annotations) {
            lines.push(LogLine { number, text });
        }
    }
    LogPane { lines, note }
}

fn log_key_for(run: &RunRecord, job: &JobDetail, step: &StepRecord) -> Option<LogKey> {
    if step.status().is_active() {
        return None;
    }
    Some(LogKey {
        run_id: run.run_id.clone(),
        job_id: job.job_id.clone(),
        step: Some(step.workflow_index? + 1),
    })
}

/// Annotations for a step, as log lines the log view colours by level.
fn annotation_rows(app: &App, run: &RunRecord, job: &JobDetail, step: &StepRecord) -> Vec<String> {
    let mut annotations = job.annotations();
    if let Some(live) = app
        .store
        .live_annotations
        .get(&(run.run_id.clone(), job.job_id.clone()))
    {
        annotations.extend(live.iter().cloned());
    }
    annotations
        .into_iter()
        .filter(|annotation| annotation.step_id.as_deref().is_none_or(|id| id == step.id))
        .flat_map(|annotation| {
            let marker = match annotation.level.as_str() {
                "error" => "##[error]",
                "warning" => "##[warning]",
                _ => "##[notice]",
            };
            let location = match (&annotation.file, annotation.line) {
                (Some(file), Some(line)) => format!("{file}:{line}"),
                (Some(file), None) => file.clone(),
                _ => String::new(),
            };
            [
                format!("{marker}⚠ annotation  {location}")
                    .trim_end()
                    .to_owned(),
                format!("{marker}  {}", annotation.message),
            ]
        })
        .collect()
}

/// Debug session of the selected run, if one exists.
fn debug_for_run<'a>(app: &'a App, run_id: &str) -> Option<&'a DebugSession> {
    app.store
        .debug
        .get()?
        .iter()
        .find(|session| session.run_id == run_id)
}

/// The runs screen.
pub struct Runs;

impl View for Runs {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let [filter_row, body] =
            Layout::vertical([Constraint::Length(1), Constraint::Min(0)]).areas(area);
        render_filter_bar(app, frame, filter_row);

        let theme = app.theme;
        let columns = fixed_columns(body, &COLUMN_WIDTHS);
        let focus = app.runs.focus;

        let visible = visible_run_ids(app);
        let selected_run = app.selected_run();

        let inner = pane(frame, columns[0], "─ Runs ", focus == Pane::Runs, theme);
        let items: Vec<ListItem> = app
            .store
            .runs
            .get()
            .into_iter()
            .flatten()
            .filter(|run| visible.contains(&run.run_id))
            .map(|run| run_item(run, app.now, theme))
            .collect();
        let cursor = visible
            .iter()
            .position(|id| Some(id) == app.runs.run.as_ref())
            .unwrap_or(0);
        render_list(frame, inner, &items, cursor, theme);

        let short = selected_run.map_or("—", RunRecord::short_id).to_owned();
        let inner_jobs = pane(
            frame,
            columns[1],
            &format!("─ Jobs · {short} "),
            focus == Pane::Jobs,
            theme,
        );
        let rows = selected_job_rows(app);
        let job_items: Vec<ListItem> = rows.iter().map(|row| job_item(row, theme)).collect();
        let job_cursor = rows
            .iter()
            .position(|row| matches!(row, JobRow::Job { job_id, .. } if Some(job_id) == app.runs.job.as_ref()))
            .unwrap_or(0);
        let [job_list, job_info] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(inner_jobs);
        render_list(frame, job_list, &job_items, job_cursor, theme);
        if let (Some(run), Some(job_id)) = (selected_run, app.runs.job.as_ref()) {
            frame.render_widget(Paragraph::new(job_info_lines(app, run, job_id)), job_info);
        }

        let inner = pane(frame, columns[2], "─ Steps ", focus == Pane::Steps, theme);
        let steps = match (selected_run, app.runs.job.as_ref()) {
            (Some(run), Some(job_id)) => steps_of(run, job_id),
            _ => Vec::new(),
        };
        let step_items: Vec<ListItem> = steps
            .iter()
            .map(|step| step_item(step, app.now, theme))
            .collect();
        let step_cursor = steps
            .iter()
            .position(|step| Some(&step.id) == app.runs.step.as_ref())
            .unwrap_or(0);
        render_list(frame, inner, &step_items, step_cursor, theme);

        let debug = selected_run.and_then(|run| debug_for_run(app, &run.run_id));
        let (log_area, debug_area) = match debug {
            Some(_) => {
                let [top, bottom] =
                    Layout::vertical([Constraint::Min(0), Constraint::Length(DEBUG_PANEL_ROWS)])
                        .areas(columns[3]);
                (top, Some(bottom))
            }
            None => (columns[3], None),
        };
        let log_title = match app
            .runs
            .step
            .as_ref()
            .and_then(|id| steps.iter().find(|s| &s.id == id))
        {
            Some(step) => {
                let follow = if app.runs.log.follow {
                    "◉ follow"
                } else {
                    "○ follow"
                };
                let search = app
                    .runs
                    .log
                    .match_status()
                    .map(|status| format!(" · {status}"))
                    .unwrap_or_default();
                format!("─ {} · {follow}{search} ", step.name)
            }
            None => "─ Log ".to_owned(),
        };
        let log_focused = focus == Pane::Log;
        let log_inner = if debug_area.is_some() {
            pane_open_bottom(frame, log_area, &log_title, log_focused, theme)
        } else {
            pane(frame, log_area, &log_title, log_focused, theme)
        };
        render_log(app, frame, log_inner);

        if let (Some(area), Some(session)) = (debug_area, debug) {
            let inner = pane(frame, area, "─ Debug ", false, theme);
            frame.render_widget(Paragraph::new(debug_lines(session, theme)), inner);
        }
    }

    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return Vec::new();
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl {
            return Vec::new();
        }
        match key.code {
            KeyCode::Up => move_selection(app, -1),
            KeyCode::Down => move_selection(app, 1),
            KeyCode::Enter => match app.runs.focus {
                Pane::Log => {
                    toggle_group_at_top(app);
                    Vec::new()
                }
                focus => {
                    app.runs.focus = focus.right();
                    Vec::new()
                }
            },
            KeyCode::Esc => {
                app.runs.focus = app.runs.focus.left();
                Vec::new()
            }
            KeyCode::Char('/') => {
                if app.runs.focus == Pane::Log {
                    app.runs.search_editing = true;
                } else {
                    app.runs.filter_editing = true;
                }
                Vec::new()
            }
            KeyCode::Char('f') => {
                app.runs.log.toggle_follow();
                Vec::new()
            }
            KeyCode::Char('g') => {
                app.runs.log.jump_top();
                Vec::new()
            }
            KeyCode::Char('G') => {
                app.runs.log.jump_bottom();
                Vec::new()
            }
            KeyCode::Char('n') => {
                let lines = log_pane(app).lines;
                app.runs.log.next_match(&lines);
                Vec::new()
            }
            KeyCode::Char('N') => {
                let lines = log_pane(app).lines;
                app.runs.log.prev_match(&lines);
                Vec::new()
            }
            KeyCode::Char('c') => match app.runs.run.clone() {
                Some(run_id) => app.run_action(Action::CancelRun { run_id }),
                None => Vec::new(),
            },
            KeyCode::Char('R') => match app.runs.run.clone() {
                Some(run_id) => app.run_action(Action::RerunRun { run_id }),
                None => Vec::new(),
            },
            KeyCode::Char('A') => approve_selected(app),
            KeyCode::Char('o') => open_github(app),
            KeyCode::Char('a') => attach_debug(app),
            _ => Vec::new(),
        }
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        [
            "enter drill",
            "esc back",
            "/ filter",
            "f follow",
            "n/N search",
            "c cancel",
            "R rerun",
            "A approve",
            "a attach",
            "o github",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    fn on_show(&self, app: &mut App) -> Vec<Effect> {
        app.after_selection_change()
    }
}

/// Keys while the filter or the log search box has focus.
pub fn on_text_key(app: &mut App, key: KeyEvent) -> Vec<Effect> {
    if app.runs.filter_editing {
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                app.runs.filter_editing = false;
                app.after_selection_change()
            }
            _ => {
                if app.runs.filter.handle_key(key) {
                    app.after_selection_change()
                } else {
                    Vec::new()
                }
            }
        }
    } else {
        match key.code {
            KeyCode::Esc => {
                app.runs.search_editing = false;
                Vec::new()
            }
            KeyCode::Enter => {
                app.runs.search_editing = false;
                let lines = log_pane(app).lines;
                let query = app.runs.search.value().to_owned();
                app.runs.log.set_query(&lines, &query);
                Vec::new()
            }
            _ => {
                app.runs.search.handle_key(key);
                Vec::new()
            }
        }
    }
}

fn move_selection(app: &mut App, delta: isize) -> Vec<Effect> {
    match app.runs.focus {
        Pane::Runs => {
            let visible = visible_run_ids(app);
            let current = visible
                .iter()
                .position(|id| Some(id) == app.runs.run.as_ref());
            let next = step_index(current, visible.len(), delta);
            app.runs.run = next.and_then(|index| visible.get(index).cloned());
            app.runs.job = None;
            app.runs.step = None;
            app.runs.reset_log();
            app.after_selection_change()
        }
        Pane::Jobs => {
            let jobs: Vec<String> = selected_job_rows(app)
                .into_iter()
                .filter_map(|row| match row {
                    JobRow::Job { job_id, .. } => Some(job_id),
                    JobRow::Header { .. } => None,
                })
                .collect();
            let current = jobs.iter().position(|id| Some(id) == app.runs.job.as_ref());
            let next = step_index(current, jobs.len(), delta);
            app.runs.job = next.and_then(|index| jobs.get(index).cloned());
            app.runs.step = None;
            app.runs.reset_log();
            app.after_selection_change()
        }
        Pane::Steps => {
            let steps = match (app.selected_run(), app.runs.job.clone()) {
                (Some(run), Some(job)) => steps_of(run, &job),
                _ => Vec::new(),
            };
            let current = steps
                .iter()
                .position(|step| Some(&step.id) == app.runs.step.as_ref());
            let next = step_index(current, steps.len(), delta);
            app.runs.step = next.and_then(|index| steps.get(index).map(|step| step.id.clone()));
            app.runs.reset_log();
            app.after_selection_change()
        }
        Pane::Log => {
            let lines = log_pane(app).lines;
            if delta < 0 {
                app.runs.log.scroll(-1, &lines, LOG_PAGE);
            } else {
                app.runs.log.scroll(1, &lines, LOG_PAGE);
            }
            Vec::new()
        }
    }
}

fn step_index(current: Option<usize>, len: usize, delta: isize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let base = current.unwrap_or(0) as isize;
    Some((base + delta).clamp(0, len as isize - 1) as usize)
}

fn toggle_group_at_top(app: &mut App) {
    let lines = log_pane(app).lines;
    let rows = app.runs.log.visible_rows(&lines);
    let top = app.runs.log.top.min(rows.len().saturating_sub(1));
    let Some(&index) = rows.get(top) else {
        return;
    };
    match classify(&lines[index].text) {
        LineKind::GroupStart => app.runs.log.toggle_group(index),
        _ => {
            if let Some(group) = LogView::enclosing_group(&lines, index) {
                app.runs.log.toggle_group(group);
            }
        }
    }
}

fn approve_selected(app: &mut App) -> Vec<Effect> {
    let Some(run) = app.selected_run() else {
        return Vec::new();
    };
    let run_id = run.run_id.clone();
    if run.fork_approval_pending {
        return app.run_action(Action::ApproveFork { run_id });
    }
    let Some(job_id) = app.runs.job.clone() else {
        return Vec::new();
    };
    if job_status(run, &job_id) != ExecStatus::Pending {
        return Vec::new();
    }
    app.run_action(Action::ApproveJob { run_id, job_id })
}

fn open_github(app: &mut App) -> Vec<Effect> {
    let Some(run) = app.selected_run() else {
        return Vec::new();
    };
    let repo = run.submission.repository.clone();
    let url = match run.pr_number() {
        Some(number) => format!("https://github.com/{repo}/pull/{number}/checks"),
        None => format!(
            "https://github.com/{repo}/commit/{}/checks",
            run.submission.sha
        ),
    };
    app.run_action(Action::OpenUrl { url })
}

fn attach_debug(app: &mut App) -> Vec<Effect> {
    let Some(run_id) = app.runs.run.clone() else {
        return Vec::new();
    };
    match debug_for_run(app, &run_id).map(|session| session.session_id.clone()) {
        Some(session_id) => app.run_action(Action::AttachDebug { session_id }),
        None => {
            app.notify("no debug session for this run");
            Vec::new()
        }
    }
}

fn render_filter_bar(app: &App, frame: &mut Frame, area: Rect) {
    let theme = app.theme;
    let visible = visible_run_ids(app).len();
    let total = app.store.runs.get().map_or(0, Vec::len);
    let mut spans = vec![Span::styled(" / ", theme.muted())];
    spans.extend(app.runs.filter.line(theme, app.runs.filter_editing).spans);
    spans.push(Span::styled(
        format!("  {visible} of {total}"),
        theme.muted(),
    ));
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn run_item(run: &RunRecord, now: DateTime<Utc>, theme: Theme) -> ListItem {
    let width: usize = 30;
    let left = format!(
        "{} #{} {}",
        run.status.glyph(),
        run.run_number,
        run.workflow_name()
            .trim_end_matches(".yml")
            .trim_end_matches(".yaml")
    );
    let age = run
        .created_at
        .map_or_else(String::new, |created| fmt_age(now - created));
    let gap = width
        .saturating_sub(left.chars().count() + age.chars().count())
        .max(1);
    let first = Line::from(vec![
        Span::styled(
            left,
            theme.tone(crate::views::overview::status_tone(run.status)),
        ),
        Span::styled(" ".repeat(gap), theme.text()),
        Span::styled(age, theme.muted()),
    ]);
    let second = Line::styled(format!("  {}", run.ref_label()), theme.muted());
    ListItem::selectable(vec![first, second, Line::raw("")])
}

fn job_item(row: &JobRow, theme: Theme) -> ListItem {
    match row {
        JobRow::Header { base, legs } => ListItem::header(vec![Line::from(vec![
            Span::styled(format!(" ▾ {base}"), theme.text()),
            Span::styled(
                format!(
                    "{:>width$}",
                    format!("matrix {legs}"),
                    width = 21usize.saturating_sub(base.chars().count())
                ),
                theme.muted(),
            ),
        ])]),
        JobRow::Job {
            label,
            leg,
            status,
            elapsed,
            ..
        } => {
            let prefix = if *leg { "   " } else { " " };
            let duration = elapsed.map_or_else(String::new, fmt_clock);
            let label_width = 24usize.saturating_sub(prefix.len() + 2 + duration.len() + 1);
            let line = Line::from(vec![
                Span::styled(
                    format!("{prefix}{} ", status.glyph()),
                    theme.tone(crate::views::overview::status_tone(*status)),
                ),
                Span::styled(format!("{label:<label_width$}"), theme.text()),
                Span::styled(format!("{duration:>5}"), theme.muted()),
            ]);
            ListItem::selectable(vec![line])
        }
    }
}

fn job_info_lines(app: &App, run: &RunRecord, job_id: &str) -> Vec<Line<'static>> {
    let theme = app.theme;
    let check = if run.job_check_run_ids.contains_key(job_id) {
        Span::styled("check run ✓ minted", theme.tone(Tone::Success))
    } else {
        Span::styled("check run —", theme.muted())
    };
    let runner = app
        .store
        .bindings
        .get()
        .and_then(|bindings| {
            bindings
                .bindings
                .iter()
                .find(|binding| binding.run_id == run.run_id && binding.job_id == job_id)
        })
        .and_then(|binding| binding.runner_name.clone());
    let runner_text = match runner {
        Some(name) => format!("runner {name}"),
        None => "runner —".to_owned(),
    };
    vec![
        Line::raw(""),
        Line::from(vec![Span::raw(" "), check]),
        Line::styled(format!(" {runner_text}"), theme.muted()),
    ]
}

fn step_item(step: &StepRecord, now: DateTime<Utc>, theme: Theme) -> ListItem {
    let status = step.status();
    let duration = step
        .elapsed(now)
        .filter(|_| step.started_at.is_some())
        .map_or_else(String::new, fmt_clock);
    let name: String = step.name.chars().take(12).collect();
    let line = Line::from(vec![
        Span::styled(
            format!(" {} ", status.glyph()),
            theme.tone(crate::views::overview::status_tone(status)),
        ),
        Span::styled(format!("{name:<12}"), theme.text()),
        Span::styled(format!("{duration:>5}"), theme.muted()),
    ]);
    ListItem::selectable(vec![line])
}

fn debug_lines(session: &DebugSession, theme: Theme) -> Vec<Line<'static>> {
    let paused = fmt_age(TimeDelta::seconds(session.paused_seconds as i64));
    vec![
        Line::from(vec![
            Span::styled(" ◆ ", theme.tone(Tone::Warning)),
            Span::styled(
                format!("session {} {} {paused}", session.session_id, session.state),
                theme.text(),
            ),
        ]),
        Line::styled(
            format!(
                " {} · a attach  r retry  x abort",
                session
                    .machine
                    .as_ref()
                    .map_or_else(|| "no VM".to_owned(), |machine| format!("VM {machine}"))
            ),
            theme.muted(),
        ),
    ]
}

fn render_log(app: &App, frame: &mut Frame, area: Rect) {
    let pane = log_pane(app);
    if pane.lines.is_empty() {
        let text = pane.note.unwrap_or_default();
        frame.render_widget(
            Paragraph::new(Line::styled(format!(" {text}"), app.theme.muted())),
            area,
        );
        return;
    }
    app.runs.log.render(frame, area, &pane.lines, app.theme);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{ExecStatus, JobDetail, StepRecord};
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 22, 12, 20, 0)
            .single()
            .expect("valid")
    }

    fn matrix_run() -> RunRecord {
        let mut run = RunRecord {
            run_id: "8f3a91c2-0000".into(),
            run_number: 412,
            status: ExecStatus::Failure,
            workflow_path_str: ".github/workflows/ci.yml".into(),
            ..Default::default()
        };
        for (id, name, status) in [
            ("fmt", "fmt", ExecStatus::Success),
            ("test (runner)", "test (runner)", ExecStatus::Success),
            ("test (sandbox)", "test (sandbox)", ExecStatus::Failure),
            ("e2e", "e2e", ExecStatus::Pending),
        ] {
            run.jobs.insert(id.into(), status);
            run.job_names.insert(id.into(), name.into());
        }
        run.job_base_ids
            .insert("test (runner)".into(), "test".into());
        run.job_base_ids
            .insert("test (sandbox)".into(), "test".into());
        run
    }

    #[test]
    fn filter_tokens_combine_with_and() {
        let filter = parse_filter("status:failure,running  workflow:ci");
        let mut failing = RunRecord {
            status: ExecStatus::Failure,
            workflow_path_str: ".github/workflows/ci.yml".into(),
            ..Default::default()
        };
        assert!(run_matches(&failing, &filter));
        failing.workflow_path_str = ".github/workflows/nightly.yml".into();
        assert!(!run_matches(&failing, &filter));
        failing.workflow_path_str = ".github/workflows/ci.yml".into();
        failing.status = ExecStatus::Success;
        assert!(!run_matches(&failing, &filter));
    }

    #[test]
    fn matrix_legs_sit_under_a_heading() {
        let rows = job_rows(&matrix_run(), now());
        let labels: Vec<String> = rows
            .iter()
            .map(|row| match row {
                JobRow::Header { base, legs } => format!("{base} x{legs}"),
                JobRow::Job { label, leg, .. } => {
                    format!("{}{label}", if *leg { "  " } else { "" })
                }
            })
            .collect();
        assert_eq!(labels, ["e2e", "fmt", "test x2", "  runner", "  sandbox"]);
    }

    #[test]
    fn default_job_prefers_the_failure() {
        assert_eq!(
            default_job(&matrix_run()).as_deref(),
            Some("test (sandbox)")
        );
    }

    #[test]
    fn default_step_prefers_the_failed_step() {
        let steps = vec![
            StepRecord {
                id: "a".into(),
                conclusion: "success".into(),
                ..Default::default()
            },
            StepRecord {
                id: "b".into(),
                conclusion: "failure".into(),
                ..Default::default()
            },
            StepRecord {
                id: "c".into(),
                conclusion: "skipped".into(),
                ..Default::default()
            },
        ];
        assert_eq!(default_step(&steps).map(|s| s.id.as_str()), Some("b"));
    }

    #[test]
    fn job_elapsed_runs_to_now_for_live_steps() {
        let start = now() - TimeDelta::seconds(180);
        let job = JobDetail {
            job_id: "test".into(),
            steps: vec![StepRecord {
                started_at: Some(start),
                finished_at: None,
                conclusion: "in_progress".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(job_elapsed(&job, now()), Some(TimeDelta::seconds(180)));
    }

    #[test]
    fn leg_labels_drop_the_base() {
        assert_eq!(leg_label("test (runner)", "test"), "runner");
        assert_eq!(leg_label("fmt", "test"), "fmt");
    }
}
