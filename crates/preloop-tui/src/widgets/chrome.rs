//! Header bar, tab bar, rules, footer and the engine-down banner.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::theme::{Theme, Tone};

/// Columns kept clear on the right of the header's summary, so it lines up
/// with the approved mockup.
const HEADER_RIGHT_MARGIN: usize = 5;

/// Everything the header shows, already formatted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderInfo {
    /// Context name.
    pub context: String,
    /// Health word and its tone, or None before the first status arrives.
    pub health: Option<(String, Tone)>,
    /// Engine version.
    pub version: String,
    /// Uptime, formatted (`3d04h`).
    pub uptime: String,
    /// Jobs ready to run.
    pub ready: u32,
    /// Jobs a runner can take now.
    pub claimable: u32,
    /// Idle pool slots.
    pub pool_idle: u32,
    /// Busy pool slots.
    pub pool_busy: u32,
    /// Sandbox providers and the tone of the sandbox link, when configured.
    pub sandbox: Option<(usize, Tone)>,
}

/// Draw the header bar: context and health on the left, queue and pool
/// summary on the right.
pub fn header(frame: &mut Frame, area: Rect, info: &HeaderInfo, theme: Theme) {
    let dot_tone = info.health.as_ref().map_or(Tone::Muted, |(_, tone)| *tone);
    let (health_word, _) = info
        .health
        .clone()
        .unwrap_or_else(|| ("connecting".to_owned(), Tone::Muted));

    let mut left = vec![
        Span::styled(" preloop  ctx ", theme.text()),
        Span::styled(info.context.clone(), theme.text()),
        Span::styled(" ● ", theme.tone(dot_tone)),
        Span::styled(health_word, theme.tone(dot_tone)),
    ];
    let engine = match (info.version.is_empty(), info.uptime.is_empty()) {
        (false, false) => format!("  {} · up {}", info.version, info.uptime),
        (false, true) => format!("  {}", info.version),
        (true, false) => format!("  up {}", info.uptime),
        (true, true) => String::new(),
    };
    if !engine.is_empty() {
        left.push(Span::styled(engine, theme.text()));
    }

    let left_width: usize = left.iter().map(|span| span.content.chars().count()).sum();
    let total = area.width as usize;
    let width = |spans: &[Span<'_>]| -> usize {
        spans.iter().map(|span| span.content.chars().count()).sum()
    };
    // The mockup's summary at full width; narrower terminals get the compact
    // form, and only then give up the right margin, so the sandbox summary is
    // the last thing to be clipped.
    let mut right = summary(info, theme, false);
    if left_width + 1 + width(&right) + HEADER_RIGHT_MARGIN > total {
        right = summary(info, theme, true);
    }
    let right_width = width(&right);
    let margin = if left_width + 1 + right_width + HEADER_RIGHT_MARGIN <= total {
        HEADER_RIGHT_MARGIN
    } else {
        0
    };
    let gap = total
        .saturating_sub(left_width + right_width + margin)
        .max(1);
    left.push(Span::raw(" ".repeat(gap)));
    left.append(&mut right);
    left.push(Span::raw(" ".repeat(margin)));

    frame.render_widget(Paragraph::new(Line::from(left)).style(theme.bar()), area);
}

/// The header's right-hand summary: queue, pool and sandbox. `compact` is the
/// form for terminals narrower than the mockup.
fn summary(info: &HeaderInfo, theme: Theme, compact: bool) -> Vec<Span<'static>> {
    let text = if compact {
        format!(
            "q {}/{} · pool {}/{} · ",
            info.ready, info.claimable, info.pool_idle, info.pool_busy
        )
    } else {
        format!(
            "queue {} ready · {} claimable   pool {}/{}   ",
            info.ready, info.claimable, info.pool_idle, info.pool_busy
        )
    };
    let mut spans = vec![Span::styled(text, theme.text())];
    match info.sandbox {
        Some((providers, tone)) => {
            spans.push(Span::styled("sbx ", theme.text()));
            spans.push(Span::styled("● ", theme.tone(tone)));
            spans.push(Span::styled(
                if compact {
                    providers.to_string()
                } else {
                    format!("{providers} prov")
                },
                theme.text(),
            ));
        }
        None => spans.push(Span::styled("sbx ○ off", theme.muted())),
    }
    spans
}

/// Draw the tab bar. `tabs` is `(label, active)`, labels like `2 Runs`.
pub fn tabs(frame: &mut Frame, area: Rect, tabs: &[(String, bool)], theme: Theme) {
    let mut spans = vec![Span::raw(" ")];
    for (index, (label, active)) in tabs.iter().enumerate() {
        if index > 0 {
            spans.push(Span::raw("  "));
        }
        let marker = if *active { "▌" } else { " " };
        spans.push(Span::styled(marker.to_owned(), theme.accent()));
        let style = if *active {
            theme.accent().add_modifier(ratatui::style::Modifier::BOLD)
        } else {
            theme.text()
        };
        spans.push(Span::styled(label.clone(), style));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Draw a full-width horizontal rule.
pub fn rule(frame: &mut Frame, area: Rect, theme: Theme) {
    let line = "─".repeat(area.width as usize);
    frame.render_widget(Paragraph::new(line).style(theme.muted()), area);
}

/// Draw the footer hints, separated by ` · `.
pub fn footer(frame: &mut Frame, area: Rect, hints: &[String], theme: Theme) {
    let text = format!(" {}", hints.join(" · "));
    frame.render_widget(Paragraph::new(text).style(theme.text()), area);
}

/// Draw a one-line banner in the error tone.
pub fn banner(frame: &mut Frame, area: Rect, text: &str, theme: Theme) {
    let line = Line::from(vec![
        Span::styled(" ✗ ", theme.tone(Tone::Error)),
        Span::styled(text.to_owned(), theme.tone(Tone::Error)),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn row_text(terminal: &Terminal<TestBackend>, y: u16) -> String {
        let buffer = terminal.backend().buffer();
        (0..buffer.area.width)
            .map(|x| buffer[(x, y)].symbol().to_owned())
            .collect()
    }

    #[test]
    fn header_puts_summary_at_the_mockup_column() {
        let mut terminal = Terminal::new(TestBackend::new(120, 1)).expect("test terminal");
        let info = HeaderInfo {
            context: "cpane".into(),
            health: Some(("healthy".into(), Tone::Success)),
            version: "0.21.0".into(),
            uptime: "3d04h".into(),
            ready: 2,
            claimable: 0,
            pool_idle: 3,
            pool_busy: 1,
            sandbox: Some((2, Tone::Success)),
        };
        terminal
            .draw(|frame| header(frame, frame.area(), &info, Theme::monochrome()))
            .expect("draw");
        let text = row_text(&terminal, 0);
        assert!(
            text.starts_with(" preloop  ctx cpane ● healthy  0.21.0 · up 3d04h"),
            "{text:?}"
        );
        assert!(text.ends_with("sbx ● 2 prov     "), "{text:?}");
        assert_eq!(text.chars().count(), 120);
        assert_eq!(text.chars().position(|c| c == 'q'), Some(62));
    }

    #[test]
    fn header_without_an_engine_shows_no_version_or_uptime() {
        let mut terminal = Terminal::new(TestBackend::new(80, 1)).expect("test terminal");
        let info = HeaderInfo {
            context: "local".into(),
            health: Some(("no engine".into(), Tone::Error)),
            sandbox: Some((0, Tone::Muted)),
            ..HeaderInfo::default()
        };
        terminal
            .draw(|frame| header(frame, frame.area(), &info, Theme::monochrome()))
            .expect("draw");
        let text = row_text(&terminal, 0);
        assert!(!text.contains("up"), "{text:?}");
        assert!(!text.contains('—'), "{text:?}");
        assert!(
            text.starts_with(" preloop  ctx local ● no engine "),
            "{text:?}"
        );
        // 80 columns cannot hold the mockup's summary: the compact form keeps
        // the sandbox count visible instead of clipping it.
        assert!(
            text.trim_end().ends_with("q 0/0 · pool 0/0 · sbx ● 0"),
            "{text:?}"
        );
        assert_eq!(text.chars().count(), 80);
    }

    #[test]
    fn banner_names_the_setup_keys() {
        let mut terminal = Terminal::new(TestBackend::new(60, 1)).expect("test terminal");
        terminal
            .draw(|frame| {
                banner(
                    frame,
                    frame.area(),
                    "no engine at http://x",
                    Theme::monochrome(),
                )
            })
            .expect("draw");
        assert!(row_text(&terminal, 0).contains("no engine at http://x"));
    }
}
