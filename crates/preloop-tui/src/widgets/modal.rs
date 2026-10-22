//! Centered overlays: the confirmation dialog and the help screen.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::actions::Action;
use crate::theme::Theme;
use crate::widgets::columns;
use crate::widgets::text_input::TextInput;

/// A pending action waiting for the user's confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Confirm {
    /// What will run on confirm.
    pub action: Action,
    /// Detail lines shown under the question.
    pub lines: Vec<String>,
    /// What the user has typed (type-to-confirm only).
    pub input: TextInput,
    /// The id the user must type, when the action is type-to-confirm.
    pub required: Option<String>,
}

impl Confirm {
    /// A confirmation for `action`.
    pub fn new(action: Action, lines: Vec<String>, required: Option<String>) -> Self {
        Self {
            action,
            lines,
            input: TextInput::default(),
            required,
        }
    }

    /// Whether enter may run the action now.
    pub fn can_confirm(&self) -> bool {
        match &self.required {
            None => true,
            Some(id) => self.input.value() == id,
        }
    }
}

/// A `width` x `height` rectangle centred in `area`, clamped to it.
pub fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    )
}

/// Draw the confirmation dialog centred in `area`.
pub fn render_confirm(frame: &mut Frame, area: Rect, confirm: &Confirm, theme: Theme) {
    let rect = centered(area, 64, (confirm.lines.len() as u16 + 9).max(10));
    frame.render_widget(Clear, rect);
    let inner = columns::pane(frame, rect, "─ Confirm ", true, theme);

    let mut lines: Vec<Line<'static>> = vec![Line::from(Span::styled(
        format!(" {}", confirm.action.label()),
        theme.accent(),
    ))];
    lines.extend(
        confirm
            .lines
            .iter()
            .map(|text| Line::from(Span::styled(format!(" {text}"), theme.text()))),
    );
    lines.push(Line::raw(""));
    let footer = match &confirm.required {
        Some(id) => {
            lines.push(Line::from(Span::styled(
                format!(" type {id} to confirm"),
                theme.muted(),
            )));
            let mut input = Line::from(Span::styled(" > ", theme.muted()));
            input.spans.extend(confirm.input.line(theme, true).spans);
            lines.push(input);
            if confirm.can_confirm() {
                " enter confirm · esc cancel"
            } else {
                " esc cancel"
            }
        }
        None => " enter confirm · esc cancel",
    };
    lines.push(Line::from(Span::styled(footer.to_owned(), theme.muted())));
    frame.render_widget(Paragraph::new(lines), inner);
}

/// Draw a help overlay listing `(keys, description)` rows, centred in `area`.
pub fn render_help(frame: &mut Frame, area: Rect, title: &str, rows: &[(&str, &str)], theme: Theme) {
    let rect = centered(area, 70, (rows.len() as u16 + 4).max(6));
    frame.render_widget(Clear, rect);
    let title = format!("─ {title} ");
    let inner = columns::pane(frame, rect, &title, true, theme);
    let lines: Vec<Line<'static>> = rows
        .iter()
        .map(|(keys, description)| {
            Line::from(vec![
                Span::styled(format!(" {keys:<16}"), theme.accent()),
                Span::styled((*description).to_owned(), theme.text()),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_to_confirm_requires_the_exact_id() {
        let mut confirm = Confirm::new(
            Action::PurgeRunner { name: "plr-4".into() },
            Vec::new(),
            Some("plr-4".into()),
        );
        assert!(!confirm.can_confirm());
        confirm.input = TextInput::new("plr");
        assert!(!confirm.can_confirm());
        confirm.input = TextInput::new("plr-4");
        assert!(confirm.can_confirm());
    }

    #[test]
    fn plain_confirm_needs_no_typing() {
        let confirm = Confirm::new(Action::CancelRun { run_id: "r".into() }, Vec::new(), None);
        assert!(confirm.can_confirm());
    }

    #[test]
    fn centered_clamps_to_the_area() {
        let rect = centered(Rect::new(0, 0, 40, 10), 80, 4);
        assert_eq!(rect, Rect::new(0, 3, 40, 4));
    }
}
