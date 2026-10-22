//! Live debug sessions and controller actions.
use super::View;
use crate::{
    actions::Action,
    app::{App, Effect},
    detectors::fmt_age,
};
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::Line,
    widgets::{Block, Borders, Paragraph},
};

pub struct Debug;
impl View for Debug {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let [sessions, detail] =
            Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)])
                .areas(area);
        let data = app.store.debug.get();
        let mut sl = vec![Line::styled(
            " ID       RUN       JOB              STATE      AGE",
            theme.muted(),
        )];
        if let Some(ss) = data {
            for s in ss {
                sl.push(Line::from(format!(
                    " {:8} {:8} {:16} {:10} {}",
                    s.session_id.get(..8).unwrap_or(&s.session_id),
                    s.run_id.get(..8).unwrap_or(&s.run_id),
                    s.job_name,
                    s.state,
                    fmt_age(chrono::TimeDelta::seconds(s.paused_seconds as i64))
                )));
            }
        } else {
            sl.push(Line::styled(" sessions unavailable", theme.muted()));
        }
        sl.push(Line::styled(
            " Abandoned / never-closed = finding",
            theme.muted(),
        ));
        frame.render_widget(
            Paragraph::new(sl).block(Block::default().title("─ Sessions ─").borders(Borders::ALL)),
            sessions,
        );
        let selected = data.and_then(|x| x.first());
        let mut dl = Vec::new();
        if let Some(s) = selected {
            dl.push(Line::from(format!(
                " run · job          {} · {}",
                s.run_id.get(..8).unwrap_or(&s.run_id),
                s.job_name
            )));
            dl.push(Line::from(format!(" state              {}", s.state)));
            dl.push(Line::from(format!(
                " failed step        #{} {} (exit {})",
                s.step.index + 1,
                s.step.display_name,
                s.step.exit_code.map_or("—".into(), |x| x.to_string())
            )));
            dl.push(Line::from(format!(
                " VM                 {}",
                s.machine.as_deref().unwrap_or("—")
            )));
            dl.push(Line::from(format!(
                " agent lease        {}",
                s.controller.as_deref().unwrap_or("—")
            )));
            dl.push(Line::from(format!(
                " hold               {}",
                fmt_age(chrono::TimeDelta::seconds(s.paused_seconds as i64))
            )));
            dl.push(Line::raw(""));
            dl.push(Line::from(
                " a attach  r retry  R retry --from step  c continue  x abort",
            ));
            dl.push(Line::styled(
                " audit: endpoint exists; load is unavailable in current store",
                theme.muted(),
            ));
        } else {
            dl.push(Line::styled(" no debug sessions", theme.muted()));
        }
        frame.render_widget(
            Paragraph::new(dl).block(
                Block::default()
                    .title("─ Session detail · Audit ─")
                    .borders(Borders::ALL),
            ),
            detail,
        );
    }
    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        let Some(s) = app.store.debug.get().and_then(|x| x.first()) else {
            return Vec::new();
        };
        let id = s.session_id.clone();
        match key.code {
            KeyCode::Char('a') => app.run_action(Action::AttachDebug { session_id: id }),
            KeyCode::Char('r') => app.run_action(Action::DebugRetry { session_id: id }),
            KeyCode::Char('R') => app.run_action(Action::DebugRetryFrom {
                session_id: id,
                step_index: s.step.index,
            }),
            KeyCode::Char('c') => app.run_action(Action::DebugContinue { session_id: id }),
            KeyCode::Char('x') => app.run_action(Action::DebugAbort { session_id: id }),
            _ => Vec::new(),
        }
    }
    fn hints(&self, _: &App) -> Vec<String> {
        [
            "a attach",
            "r retry",
            "R retry --from step",
            "c continue",
            "x abort",
            "L take agent lease",
            "u audit",
            "? help",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}
