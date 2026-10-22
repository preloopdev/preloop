//! Webhook queue, delivery history, and replay.
use super::View;
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

pub struct Webhooks;
impl View for Webhooks {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let [queue, body] =
            Layout::vertical([Constraint::Length(4), Constraint::Min(0)]).areas(area);
        let data = app.store.webhooks.get();
        let stats = data.map(|d| d.stats.clone()).unwrap_or_default();
        let oldest = stats.oldest_pending_age_seconds.map_or("—".into(), |s| {
            fmt_age(chrono::TimeDelta::seconds(s as i64))
        });
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(format!(
                    " received {}   processing {}   done {}   dead letters {}   oldest pending {}",
                    stats.received, stats.processing, stats.done, stats.dead_letters, oldest
                )),
                Line::styled(
                    " deliveries/h  (history unavailable in current API response)",
                    theme.muted(),
                ),
            ])
            .block(
                Block::default()
                    .title("─ Queue · last 24h ─")
                    .borders(Borders::ALL),
            ),
            queue,
        );
        let [deliveries, health] =
            Layout::horizontal([Constraint::Percentage(70), Constraint::Percentage(30)])
                .areas(body);
        let mut dl = vec![Line::styled(
            " DELIVERY       EVENT          STATE       TRY AGE     LAST ERROR",
            theme.muted(),
        )];
        if let Some(d) = data {
            for x in &d.deliveries {
                let age = x.age_seconds.map_or("—".into(), |s| {
                    fmt_age(chrono::TimeDelta::seconds(s as i64))
                });
                dl.push(Line::from(format!(
                    " {:14} {:14} {:10} {:3} {:7} {}",
                    x.delivery_id.get(..14).unwrap_or(&x.delivery_id),
                    x.event,
                    x.state,
                    x.attempts,
                    age,
                    x.last_error.as_deref().unwrap_or("")
                )));
            }
        } else {
            dl.push(Line::styled(" deliveries unavailable", theme.muted()));
        }
        frame.render_widget(
            Paragraph::new(dl).block(
                Block::default()
                    .title("─ Deliveries ─")
                    .borders(Borders::ALL),
            ),
            deliveries,
        );
        let mut hl = vec![];
        if let Some(h) = app.store.webhook_health.get() {
            let w = &h.watchdog;
            hl.push(Line::from(format!(
                " watchdog           {}",
                if w.enabled { "ok" } else { "disabled" }
            )));
            hl.push(Line::from(format!(
                " open repairs       {}",
                w.open_repairs
            )));
            if let Some(b) = h.github_breaker.first() {
                hl.push(Line::from(format!(
                    " github breaker     {}",
                    if b.open { "open" } else { "closed" }
                )));
            }
            for a in &h.apps {
                hl.push(Line::from(format!(
                    " app {}             {}",
                    a.app_id,
                    if a.healthy { "ok" } else { "error" }
                )));
            }
        } else {
            hl.push(Line::styled(
                " health unavailable",
                theme.tone(Tone::Warning),
            ));
        }
        hl.push(Line::styled(
            " ingress probe      not available (no client action)",
            theme.muted(),
        ));
        frame.render_widget(
            Paragraph::new(hl).block(Block::default().title("─ Health ─").borders(Borders::ALL)),
            health,
        );
    }
    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        if key.code == KeyCode::Char('R') {
            if let Some(d) = app
                .store
                .webhooks
                .get()
                .and_then(|x| x.deliveries.iter().find(|d| d.state == "failed"))
            {
                return app.run_action(Action::ReplayDelivery {
                    delivery_id: d.delivery_id.clone(),
                });
            }
        }
        Vec::new()
    }
    fn hints(&self, _: &App) -> Vec<String> {
        [
            "enter payload meta",
            "R replay",
            "⇧R replay all dead",
            "P probe public ingress",
            "/ filter state:dead",
            "? help",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}
