//! Fleet view: registered runners, pool state, and job bindings.
use super::{View, ViewKind};
use crate::{
    actions::Action,
    app::{App, Effect},
    detectors::fmt_age,
    theme::Tone,
};
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

pub struct Fleet;

impl View for Fleet {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let status = app.store.status.get().cloned().unwrap_or_default();
        let [banner, top, bindings] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Length(11),
            Constraint::Min(0),
        ])
        .areas(area);
        let leaked = app
            .findings()
            .into_iter()
            .find(|f| f.code == "leaked_binding");
        frame.render_widget(
            Paragraph::new(leaked.map_or_else(
                || " no leaked binding detector finding".to_owned(),
                |f| format!(" ⚠ {}", f.message),
            ))
            .style(theme.tone(Tone::Warning)),
            banner,
        );
        let [runners, pool] =
            Layout::horizontal([Constraint::Percentage(65), Constraint::Percentage(35)]).areas(top);
        let mut lines = vec![Line::styled(
            " NAME                 STATE   JOB                         POLL   LEASE   LABELS",
            theme.muted(),
        )];
        if let Some(list) = app.store.runners.get() {
            for r in &list.runners {
                let assignment = status
                    .runners
                    .assignments
                    .iter()
                    .find(|a| a.runner_id == r.id);
                lines.push(Line::from(format!(
                    " {:20} {:7} {:27} {:6} {:7} {}",
                    r.name,
                    if assignment.is_some() { "busy" } else { "idle" },
                    assignment.map_or("—".to_owned(), |a| format!(
                        "{} · {}",
                        a.run_id.get(..8).unwrap_or(&a.run_id),
                        a.job_id
                    )),
                    "—",
                    assignment.map_or("—".to_owned(), |a| fmt_age(chrono::TimeDelta::seconds(
                        a.assigned_seconds_ago as i64
                    ))),
                    r.labels.join(",")
                )));
            }
        } else {
            lines.push(Line::styled(" runners unavailable", theme.muted()));
        }
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::default().title("─ Runners ─").borders(Borders::ALL)),
            runners,
        );
        let pool_lines = vec![
            Line::from(format!(" mode                 {}", status.pool.mode)),
            Line::from(format!(" desired              {}", status.pool.desired)),
            Line::from(format!(
                " idle / busy          {} / {}",
                status.pool.idle, status.pool.busy
            )),
            Line::from(format!(" held for debug       {}", status.pool.paused)),
            Line::from(format!(
                " building / provision {} / {}",
                status.pool.building, status.pool.provisioning
            )),
            Line::from(format!(
                " pending registrations {}",
                status.pool.pending_registrations
            )),
            Line::from(format!(
                " released bindings    {}",
                status.pool.released_bindings
            )),
        ];
        frame.render_widget(
            Paragraph::new(pool_lines).block(
                Block::default()
                    .title("─ Pool · OperationalSnapshot ─")
                    .borders(Borders::ALL),
            ),
            pool,
        );
        let mut blines = vec![Line::styled(
            " JOB                         STATE        RUNNER       SINCE   WAITLIST",
            theme.muted(),
        )];
        if let Some(data) = app.store.bindings.get() {
            for b in &data.bindings {
                blines.push(Line::from(format!(
                    " {:27} {:12} {:12} {:7} {}",
                    format!("{} · {}", b.run_id.get(..8).unwrap_or(&b.run_id), b.job_id),
                    b.state,
                    b.runner_name.as_deref().unwrap_or("—"),
                    b.since_seconds
                        .map_or("—".into(), |s| fmt_age(chrono::TimeDelta::seconds(
                            s as i64
                        ))),
                    b.waitlist_position.map_or("—".into(), |p| p.to_string())
                )));
            }
        } else {
            blines.push(Line::styled(
                " pool bindings unavailable",
                theme.tone(Tone::Warning),
            ));
        }
        frame.render_widget(
            Paragraph::new(blines).block(
                Block::default()
                    .title("─ Bindings · job ⇄ runner ─")
                    .borders(Borders::ALL),
            ),
            bindings,
        );
    }
    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        match key.code {
            KeyCode::Char('p') => {
                if let Some(r) = app.store.runners.get().and_then(|l| l.runners.first()) {
                    return app.run_action(Action::PurgeRunner {
                        name: r.name.clone(),
                    });
                }
                Vec::new()
            }
            KeyCode::Char('b') => Vec::new(),
            KeyCode::Enter => app.show(ViewKind::Runs),
            _ => Vec::new(),
        }
    }
    fn hints(&self, _: &App) -> Vec<String> {
        [
            "enter detail",
            "/ filter",
            "p purge runner",
            "b bindings",
            "s ssh journal",
            "? help",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}
