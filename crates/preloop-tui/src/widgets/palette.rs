//! The `:` command palette: fuzzy search over actions and store objects.
//!
//! The palette is generic over the action type `A`, so it draws and filters
//! without knowing what a selection does. The app maps the chosen `A` to effects.

use ratatui::Frame;
use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::theme::Theme;
use crate::widgets::columns;
use crate::widgets::list::fuzzy_filter;
use crate::widgets::text_input::TextInput;

/// One thing the palette can run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaletteEntry<A> {
    /// Short name, matched by the query.
    pub label: String,
    /// Secondary text, also matched.
    pub detail: String,
    /// True when the action needs confirmation, shown with `!`.
    pub danger: bool,
    /// What selecting the entry does.
    pub action: A,
}

/// What a key did to the palette.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<A> {
    /// The key was used by the palette.
    Consumed,
    /// Close the palette.
    Close,
    /// Run this action and close.
    Run(A),
}

/// Palette state: the query, the entries, and the highlighted match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Palette<A> {
    input: TextInput,
    cursor: usize,
    entries: Vec<PaletteEntry<A>>,
}

impl<A: Clone> Palette<A> {
    /// A palette over `entries`, with an empty query.
    pub fn new(entries: Vec<PaletteEntry<A>>) -> Self {
        Self {
            input: TextInput::default(),
            cursor: 0,
            entries,
        }
    }

    /// Start with `query` already typed (for example `ctx ` to pick a context).
    pub fn with_query(mut self, query: &str) -> Self {
        self.input = TextInput::new(query);
        self
    }

    /// The query box.
    pub fn input(&self) -> &TextInput {
        &self.input
    }

    /// Indices of the entries matching the query, best first.
    pub fn matches(&self) -> Vec<usize> {
        let texts: Vec<String> = self
            .entries
            .iter()
            .map(|entry| format!("{} {}", entry.label, entry.detail))
            .collect();
        fuzzy_filter(self.input.value(), &texts)
    }

    /// The highlighted entry, if any match.
    pub fn selected(&self) -> Option<&PaletteEntry<A>> {
        let matches = self.matches();
        let index = *matches.get(self.cursor.min(matches.len().checked_sub(1)?))?;
        self.entries.get(index)
    }

    /// Apply a key press.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome<A> {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return Outcome::Consumed;
        }
        match key.code {
            KeyCode::Esc => Outcome::Close,
            KeyCode::Enter => match self.selected() {
                Some(entry) => Outcome::Run(entry.action.clone()),
                None => Outcome::Consumed,
            },
            KeyCode::Up => {
                self.cursor = self.cursor.saturating_sub(1);
                Outcome::Consumed
            }
            KeyCode::Down => {
                let count = self.matches().len();
                if self.cursor + 1 < count {
                    self.cursor += 1;
                }
                Outcome::Consumed
            }
            _ => {
                if self.input.handle_key(key) {
                    self.cursor = 0;
                }
                Outcome::Consumed
            }
        }
    }
}

/// Draw the palette as a box near the top of `area`.
pub fn render<A: Clone>(frame: &mut Frame, area: Rect, palette: &Palette<A>, theme: Theme) {
    let width = area.width.min(72);
    let height = area.height.min(14);
    let rect = Rect::new(area.x + area.width.min(12), area.y + 1, width, height);
    if rect.height < 4 || rect.width < 10 {
        return;
    }
    frame.render_widget(Clear, rect);
    let inner = columns::pane(frame, rect, "─ Command ", true, theme);

    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut prompt = Line::from(Span::styled(": ", theme.accent()));
    prompt.spans.extend(palette.input().line(theme, true).spans);
    lines.push(prompt);
    lines.push(Line::from(Span::styled(
        "─".repeat(inner.width as usize),
        theme.muted(),
    )));

    let matches = palette.matches();
    let selected_row = palette.cursor.min(matches.len().saturating_sub(1));
    let room = inner.height.saturating_sub(3) as usize;
    for (row, index) in matches.iter().take(room).enumerate() {
        let entry = &palette.entries[*index];
        let marker = if entry.danger { "! " } else { "  " };
        let text = format!("{marker}{}   {}", entry.label, entry.detail);
        let line = Line::from(Span::styled(text, theme.text()));
        lines.push(if row == selected_row {
            line.style(theme.selected())
        } else {
            line
        });
    }
    if matches.is_empty() {
        lines.push(Line::from(Span::styled("  no match", theme.muted())));
    }

    let footer_row = inner.y + inner.height.saturating_sub(1);
    frame.render_widget(Paragraph::new(lines), inner);
    let footer = Rect::new(inner.x, footer_row, inner.width, 1);
    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "enter run · esc close · ! requires confirmation",
            theme.muted(),
        ))),
        footer,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::crossterm::event::KeyModifiers;

    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn entries() -> Vec<PaletteEntry<&'static str>> {
        vec![
            PaletteEntry {
                label: "rerun 8f3a91c2".into(),
                detail: "Runs".into(),
                danger: false,
                action: "rerun",
            },
            PaletteEntry {
                label: "release sbx-p91x".into(),
                detail: "Sandboxes".into(),
                danger: true,
                action: "release",
            },
        ]
    }

    #[test]
    fn typing_filters_and_enter_runs_the_best_match() {
        let mut palette = Palette::new(entries());
        for c in "rel".chars() {
            palette.handle_key(key(KeyCode::Char(c)));
        }
        assert_eq!(palette.matches(), vec![1]);
        assert_eq!(
            palette.handle_key(key(KeyCode::Enter)),
            Outcome::Run("release")
        );
    }

    #[test]
    fn escape_closes_and_arrows_stay_in_bounds() {
        let mut palette = Palette::new(entries());
        palette.handle_key(key(KeyCode::Down));
        palette.handle_key(key(KeyCode::Down));
        palette.handle_key(key(KeyCode::Down));
        assert_eq!(palette.selected().map(|entry| entry.action), Some("release"));
        assert_eq!(palette.handle_key(key(KeyCode::Esc)), Outcome::Close);
    }
}
