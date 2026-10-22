//! The overview: queue, pool and fleet at a glance, conditions with jump
//! links, and the most recent runs.
//!
//! Layout follows the approved mockup on a 120-column screen: three equal
//! panels on top, a full-width conditions list, then a full-width recent-runs
//! table. The top row is 10 rows tall, conditions 8, and runs take the rest.

use chrono::TimeDelta;
use ratatui::Frame;
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::{View, ViewKind};
use crate::app::{App, Effect};
use crate::detectors::{Severity, fmt_age};
use crate::theme::{Theme, Tone};
use crate::widgets::columns::{fixed_columns, pane};
use crate::widgets::gauge::{bar, ratio};
use crate::widgets::kv::{Kv, kv_lines};
use crate::widgets::list::{ListItem, render_list};
use crate::widgets::sparkline::spark;

/// Overview state: the cursor in the conditions list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    /// Index of the highlighted condition.
    pub cursor: usize,
}

/// One row of the conditions list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConditionRow {
    /// Severity glyph and tone.
    pub icon: &'static str,
    /// Tone of the glyph and code.
    pub tone: Tone,
    /// Condition code.
    pub code: String,
    /// Message.
    pub message: String,
    /// Where enter jumps, and the label shown at the right.
    pub jump: Option<(ViewKind, &'static str)>,
}

/// Widths of the conditions list columns, from the mockup.
const CODE_WIDTH: usize = 23;
const MESSAGE_WIDTH: usize = 63;

/// The overview screen.
pub struct Overview;

/// The conditions list: the engine's own conditions, then detector findings.
pub fn condition_rows(app: &App) -> Vec<ConditionRow> {
    let mut rows: Vec<ConditionRow> = Vec::new();
    if let Some(status) = app.store.status.get() {
        for condition in &status.conditions {
            let (icon, tone) = severity_look(&condition.severity);
            rows.push(ConditionRow {
                icon,
                tone,
                code: condition.code.clone(),
                message: condition.message.clone(),
                jump: jump_for(&condition.code),
            });
        }
    }
    for finding in app.findings() {
        let (icon, tone) = match finding.severity {
            Severity::Warning => ("⚠", Tone::Warning),
            Severity::Info => ("●", Tone::Success),
        };
        rows.push(ConditionRow {
            icon,
            tone,
            code: finding.code.to_owned(),
            message: finding.message.clone(),
            jump: jump_for(finding.code),
        });
    }
    rows
}

fn severity_look(severity: &str) -> (&'static str, Tone) {
    match severity {
        "error" => ("✗", Tone::Error),
        "warning" => ("⚠", Tone::Warning),
        _ => ("●", Tone::Success),
    }
}

fn jump_for(code: &str) -> Option<(ViewKind, &'static str)> {
    match code {
        "webhook_dead_letters" => Some((ViewKind::Webhooks, "5 Webhooks › replay")),
        "leaked_binding" => Some((ViewKind::Fleet, "3 Fleet › bindings")),
        "debug_session_open" => Some((ViewKind::Debug, "4 Debug › attach")),
        "sbx_provider_cordoned" => Some((ViewKind::Sandboxes, "6 Sandboxes")),
        _ => None,
    }
}

impl View for Overview {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let [top, conditions, recent] = Layout::vertical([
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Min(0),
        ])
        .areas(area);
        let theme = app.theme;
        let panels = fixed_columns(top, &[40, 40, 40]);

        let queue = pane(frame, panels[0], "─ Queue ", false, theme);
        frame.render_widget(Paragraph::new(queue_lines(app)), queue);

        let pool = pane(frame, panels[1], "─ Pool & runners ", false, theme);
        frame.render_widget(Paragraph::new(pool_lines(app)), pool);

        let fleet = pane(frame, panels[2], "─ VM fleet · Webhooks ", false, theme);
        frame.render_widget(Paragraph::new(fleet_lines(app)), fleet);

        let rows = condition_rows(app);
        let inner = pane(frame, conditions, "─ Conditions · Detectors ", true, theme);
        if rows.is_empty() {
            frame.render_widget(
                Paragraph::new(Line::styled(" no conditions", theme.muted())),
                inner,
            );
        } else {
            let items: Vec<ListItem> = rows
                .iter()
                .map(|row| ListItem::selectable(vec![condition_line(row, theme)]))
                .collect();
            render_list(frame, inner, &items, app.overview.cursor, theme);
        }

        let inner = pane(frame, recent, "─ Recent runs ", false, theme);
        frame.render_widget(Paragraph::new(recent_lines(app, inner.height as usize)), inner);
    }

    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        let count = condition_rows(app).len();
        match key.code {
            KeyCode::Up => {
                app.overview.cursor = app.overview.cursor.saturating_sub(1);
                Vec::new()
            }
            KeyCode::Down => {
                if app.overview.cursor + 1 < count {
                    app.overview.cursor += 1;
                }
                Vec::new()
            }
            KeyCode::Enter => {
                let rows = condition_rows(app);
                match rows.get(app.overview.cursor).and_then(|row| row.jump) {
                    Some((view, _)) => app.show(view),
                    None => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        [
            "tab/1-7 views",
            "enter jump to source",
            ": palette",
            "⇧C context",
            "r refresh",
            "? help",
            "q quit",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}

fn queue_lines(app: &App) -> Vec<Line<'static>> {
    let theme = app.theme;
    let status = app.store.status.get();
    let jobs = status.map(|s| s.jobs.clone()).unwrap_or_default();
    let runs = status.map(|s| s.runs.clone()).unwrap_or_default();
    let oldest = jobs
        .oldest_ready_seconds
        .map_or_else(|| "—".to_owned(), |seconds| fmt_age(TimeDelta::seconds(seconds as i64)));
    let rows = [
        Kv::text("jobs ready", jobs.ready.to_string(), theme),
        Kv::text("claimable", jobs.claimable.to_string(), theme),
        Kv::text("dependency blocked", jobs.dependency_blocked.to_string(), theme),
        Kv::text("concurrency blocked", jobs.concurrency_blocked.to_string(), theme),
        Kv::text("oldest ready", oldest, theme),
        Kv::text(
            "runs queued/active",
            format!("{} / {}", runs.queued, runs.in_progress),
            theme,
        ),
    ];
    let mut lines = kv_lines(&rows, 21, theme);
    lines.push(Line::raw(""));

    let history: Vec<u32> = app
        .store
        .ready_history
        .iter()
        .map(|(_, ready)| *ready)
        .collect();
    let keep = history.len().saturating_sub(28);
    let recent = &history[keep..];
    let ceiling = recent.iter().copied().max().unwrap_or(0).max(8);
    lines.push(Line::from(vec![
        Span::styled(" ready 15m ", theme.muted()),
        Span::styled(spark(recent, ceiling), theme.accent()),
    ]));
    lines
}

fn pool_lines(app: &App) -> Vec<Line<'static>> {
    let theme = app.theme;
    let status = app.store.status.get().cloned().unwrap_or_default();
    let pool = &status.pool;
    let rows = [
        Kv::text("mode / desired", format!("{} / {}", pool.mode, pool.desired), theme),
        Kv::text("idle / busy", format!("{} / {}", pool.idle, pool.busy), theme),
        Kv::text(
            "building · provis.",
            format!("{} · {}", pool.building, pool.provisioning),
            theme,
        ),
        Kv::text("held for debug", status.debug.active_sessions.to_string(), theme),
        Kv::text("released bindings", pool.released_bindings.to_string(), theme),
        Kv::text(
            "runners reg/stale",
            format!("{} / {}", status.runners.registered, status.runners.stale),
            theme,
        ),
    ];
    let mut lines = kv_lines(&rows, 21, theme);
    lines.push(Line::raw(""));

    let occupied = f64::from(pool.idle + pool.busy + status.debug.active_sessions);
    let capacity = f64::from(status.runners.registered.max(1));
    lines.push(Line::from(vec![
        Span::styled(" slots ", theme.muted()),
        Span::styled(bar(ratio(occupied, capacity), 12), theme.tone(Tone::Success)),
        Span::styled(" idle·busy·held", theme.muted()),
    ]));
    lines
}

fn fleet_lines(app: &App) -> Vec<Line<'static>> {
    let theme = app.theme;
    let status = app.store.status.get().cloned().unwrap_or_default();
    let host = &status.vms.host_usage;

    let mem = match (host.mem_total_bytes, host.mem_available_bytes) {
        (Some(total), Some(available)) if total > 0 => {
            let used = total.saturating_sub(available);
            let gib = |bytes: u64| bytes as f64 / 1_073_741_824.0;
            Line::from(vec![
                Span::styled(bar(ratio(used as f64, total as f64), 10), theme.tone(Tone::Success)),
                Span::styled(
                    format!(" {:.0}/{:.0}G", gib(used), gib(total)),
                    theme.text(),
                ),
            ])
        }
        _ => Line::styled("—", theme.muted()),
    };
    let cpu = host
        .cpu_cores
        .map_or_else(|| "—".to_owned(), |cores| format!("{cores:.1} cores"));

    let breaker = app
        .store
        .webhook_health
        .get()
        .map(|health| {
            if health.github_breaker.iter().any(|b| b.open) {
                ("open", Tone::Error)
            } else {
                ("closed", Tone::Success)
            }
        })
        .unwrap_or(("—", Tone::Muted));
    let stats = app
        .store
        .webhooks
        .get()
        .map(|deliveries| deliveries.stats.clone())
        .unwrap_or_default();
    let dead_tone = if stats.dead_letters > 0 { Tone::Error } else { Tone::Plain };

    let mut lines = vec![
        Line::from(vec![
            Span::styled(format!(" {:<17}", "runner / golden"), theme.muted()),
            Span::styled(
                format!("{} / {}", status.vms.count.runner, status.vms.count.golden),
                theme.text(),
            ),
        ]),
        Line::from(vec![
            Span::styled(format!(" {:<17}", "host cpu"), theme.muted()),
            Span::styled(cpu, theme.text()),
        ]),
    ];
    let mut mem_line = vec![Span::styled(format!(" {:<17}", "host mem"), theme.muted())];
    mem_line.extend(mem.spans);
    lines.push(Line::from(mem_line));
    lines.push(Line::from(vec![
        Span::styled(format!(" {:<17}", "deliveries 24h"), theme.muted()),
        Span::styled(thousands(stats.received), theme.text()),
    ]));
    lines.push(Line::from(vec![
        Span::styled(format!(" {:<17}", "dead letters"), theme.muted()),
        Span::styled(stats.dead_letters.to_string(), theme.tone(dead_tone)),
    ]));
    lines.push(Line::from(vec![
        Span::styled(format!(" {:<17}", "github breaker"), theme.muted()),
        Span::styled(breaker.0.to_owned(), theme.tone(breaker.1)),
    ]));
    lines
}

fn condition_line(row: &ConditionRow, theme: Theme) -> Line<'static> {
    let message: String = row.message.chars().take(MESSAGE_WIDTH).collect();
    let jump = row
        .jump
        .map(|(_, label)| format!("→ {label}"))
        .unwrap_or_default();
    Line::from(vec![
        Span::styled(format!(" {} ", row.icon), theme.tone(row.tone)),
        Span::styled(format!("{:<CODE_WIDTH$}", row.code), theme.tone(row.tone)),
        Span::styled(format!("{message:<MESSAGE_WIDTH$}"), theme.text()),
        Span::styled(jump, theme.accent()),
    ])
}

fn recent_lines(app: &App, height: usize) -> Vec<Line<'static>> {
    let theme = app.theme;
    let mut lines = vec![Line::styled(
        format!(
            " {:<10}{:<5}{:<13}{:<19}{:<15}{:<34}{:<11}AGE",
            "RUN", "#", "STATUS", "WORKFLOW", "EVENT", "REF / PR", "PUSH"
        ),
        theme.muted(),
    )];
    let now = app.now;
    for run in app.store.runs.get().into_iter().flatten().take(height.saturating_sub(2).min(5)) {
        let status_text = format!("{} {}", run.status.glyph(), run.status.label());
        let push = run
            .push_state
            .as_ref()
            .map_or_else(|| "—".to_owned(), |state| state.status.clone());
        let age = run
            .created_at
            .map_or_else(|| "—".to_owned(), |created| fmt_age(now - created));
        lines.push(Line::from(vec![
            Span::styled(format!(" {:<10}", run.short_id()), theme.text()),
            Span::styled(format!("{:<5}", run.run_number), theme.text()),
            Span::styled(format!("{status_text:<13}"), theme.tone(status_tone(run.status))),
            Span::styled(format!("{:<19}", run.workflow_name()), theme.text()),
            Span::styled(format!("{:<15}", run.event), theme.text()),
            Span::styled(format!("{:<34}", run.ref_label()), theme.text()),
            Span::styled(format!("{push:<11}"), theme.muted()),
            Span::styled(age, theme.muted()),
        ]));
    }
    lines
}

/// Tone for a run status in tables.
pub fn status_tone(status: crate::api::ExecStatus) -> Tone {
    use crate::api::ExecStatus;
    match status {
        ExecStatus::Success => Tone::Success,
        ExecStatus::Failure => Tone::Error,
        ExecStatus::InProgress => Tone::Accent,
        ExecStatus::Queued | ExecStatus::Pending => Tone::Muted,
        ExecStatus::Skipped | ExecStatus::Cancelled | ExecStatus::Unknown => Tone::Muted,
    }
}

/// `1284` as `1 284`.
fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(digit);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thousands_groups_by_three() {
        assert_eq!(thousands(1284), "1 284");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000_000), "1 000 000");
    }

    #[test]
    fn jump_targets_match_the_mockup_links() {
        assert_eq!(
            jump_for("webhook_dead_letters"),
            Some((ViewKind::Webhooks, "5 Webhooks › replay"))
        );
        assert_eq!(jump_for("ingress"), None);
    }
}
