//! Sandbox providers and sandbox inventory screen.
use super::View;
use crate::app::{App, Effect};
use crate::client::sandbox::{ProviderRow, SandboxSummary};
use crate::theme::{Theme, Tone};
use ratatui::style::Modifier;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

pub struct Sandboxes;

impl View for Sandboxes {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let Some(summary) = app.store.sandbox.get() else {
            let text = app
                .store
                .sandbox
                .error
                .as_deref()
                .map_or("loading sandbox data…", |e| e);
            frame.render_widget(
                Paragraph::new(text).block(panel("─ Sandboxes ", false, theme)),
                area,
            );
            return;
        };
        let [top, bottom] =
            Layout::vertical([Constraint::Length(11), Constraint::Min(0)]).areas(area);
        let [providers, pools] =
            Layout::horizontal([Constraint::Length(40), Constraint::Min(0)]).areas(top);
        frame.render_widget(
            Paragraph::new(provider_lines(summary, theme)).block(panel(
                "─ Providers ",
                false,
                theme,
            )),
            providers,
        );
        frame.render_widget(
            Paragraph::new(pool_lines(summary, theme)).block(panel(
                "─ Pools · Templates ",
                false,
                theme,
            )),
            pools,
        );
        let [sandboxes, stats] =
            Layout::horizontal([Constraint::Length(64), Constraint::Min(0)]).areas(bottom);
        frame.render_widget(
            Paragraph::new(sandbox_lines(summary, theme)).block(panel("─ Sandboxes ", true, theme)),
            sandboxes,
        );
        frame.render_widget(
            Paragraph::new(stat_lines(summary, theme)).block(panel("─ Host stats ", false, theme)),
            stats,
        );
    }
    fn on_key(&self, _app: &mut App, _key: ratatui::crossterm::event::KeyEvent) -> Vec<Effect> {
        Vec::new()
    }
    fn hints(&self, _app: &App) -> Vec<String> {
        [
            "e exec (owned lease only)",
            "s stop/start (owned lease only)",
            "K cordon",
            "D drain",
            "r refresh",
            "? help",
            "q quit",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
}

fn panel(title: &'static str, focused: bool, theme: Theme) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(theme.border(focused))
        .title(Span::styled(
            title,
            theme.accent().add_modifier(Modifier::BOLD),
        ))
}
fn provider_lines(s: &SandboxSummary, t: Theme) -> Vec<Line<'static>> {
    if s.providers.is_empty() {
        return vec![Line::styled(" no providers", t.muted())];
    }
    s.providers
        .iter()
        .flat_map(|p| {
            let mark = if p.cordoned { "⊘" } else { "●" };
            let tone = if p.cordoned {
                Tone::Warning
            } else {
                Tone::Success
            };
            [
                Line::from(vec![
                    Span::styled(format!(" {mark} "), t.tone(tone)),
                    Span::styled(p.name.clone(), t.text().add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {} sandboxes", p.sandboxes), t.muted()),
                ]),
                Line::styled(
                    if p.cordoned {
                        "   cordoned · new placements disabled"
                    } else {
                        "   accepting placements"
                    },
                    t.muted(),
                ),
            ]
        })
        .collect()
}
fn pool_lines(s: &SandboxSummary, t: Theme) -> Vec<Line<'static>> {
    let mut out = vec![Line::styled(
        " POOL             TEMPLATE                 WARM/MAX  LEASED  TTL",
        t.muted(),
    )];
    if s.pools.is_empty() {
        out.push(Line::styled(" no pools returned by service", t.muted()));
    } else {
        for p in &s.pools {
            out.push(Line::from(vec![Span::styled(
                format!(
                    " {:<16}{:<25}{:>3}/{:<3}  {:>5}  {}",
                    p.name,
                    clip(&p.template, 24),
                    p.warm,
                    if p.max == 0 {
                        "∞".into()
                    } else {
                        p.max.to_string()
                    },
                    p.leased,
                    ttl(p.lease_ttl_secs)
                ),
                t.text(),
            )]));
        }
    }
    if !s.templates.is_empty() {
        out.push(Line::styled(" TEMPLATES", t.muted()));
        for x in &s.templates {
            out.push(Line::styled(
                format!("   {}  {}", x.name, x.description),
                t.muted(),
            ));
        }
    }
    out
}
fn sandbox_lines(s: &SandboxSummary, t: Theme) -> Vec<Line<'static>> {
    let mut out = vec![Line::styled(
        " ID              STATE          PURPOSE     POOL",
        t.muted(),
    )];
    if s.sandboxes_rows.is_empty() {
        out.push(Line::styled(
            " no sandbox rows returned by service",
            t.muted(),
        ));
        return out;
    }
    for x in &s.sandboxes_rows {
        let tone = match x.state.as_str() {
            "READY" => Tone::Success,
            "FAILED" => Tone::Error,
            "LEASED" | "PRESERVED" => Tone::Warning,
            _ => Tone::Accent,
        };
        out.push(Line::from(vec![
            Span::styled(format!(" {:<16}", clip(&x.id, 15)), t.accent()),
            Span::styled(format!("{:<15}", x.state), t.tone(tone)),
            Span::styled(format!("{:<12} {}", x.purpose, x.pool), t.muted()),
        ]));
        if !x.error.is_empty() {
            out.push(Line::styled(
                format!("   error: {}", x.error),
                t.tone(Tone::Error),
            ));
        }
    }
    out
}
fn stat_lines(s: &SandboxSummary, t: Theme) -> Vec<Line<'static>> {
    let mut out = vec![Line::styled(
        format!(" sandboxes total  {}", s.sandboxes),
        t.text(),
    )];
    if let Some(m) = s.host_mem {
        let used = m.total_bytes.saturating_sub(m.available_bytes);
        out.push(Line::styled(
            format!(" host memory     {}/{}", bytes(used), bytes(m.total_bytes)),
            t.text(),
        ));
        out.push(Line::styled(
            format!(" available       {}", bytes(m.available_bytes)),
            t.muted(),
        ));
    } else {
        out.push(Line::styled(" host memory     unavailable", t.muted()));
    }
    out
}
fn clip(v: &str, n: usize) -> String {
    if v.chars().count() <= n {
        v.to_owned()
    } else {
        format!(
            "{}…",
            v.chars().take(n.saturating_sub(1)).collect::<String>()
        )
    }
}
fn ttl(v: u64) -> String {
    if v == 0 {
        "—".into()
    } else if v % 3600 == 0 {
        format!("{}h", v / 3600)
    } else {
        format!("{}m", v / 60)
    }
}
fn bytes(v: u64) -> String {
    if v >= 1 << 30 {
        format!("{:.1}G", v as f64 / (1 << 30) as f64)
    } else {
        format!("{}M", v / (1 << 20))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clipping_is_bounded() {
        assert_eq!(clip("abcdefgh", 5), "abcd…");
    }
    #[test]
    fn ttl_formats() {
        assert_eq!(ttl(3600), "1h");
    }
}
