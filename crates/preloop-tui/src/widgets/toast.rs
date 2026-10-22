//! Transient one-line messages in the bottom-right corner.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Clear, Paragraph};

use crate::theme::{Theme, Tone};

/// How many toasts are shown at once.
const VISIBLE: usize = 3;

/// A message that disappears at `until`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Toast {
    /// The message.
    pub text: String,
    /// Colour meaning.
    pub tone: Tone,
    /// When the toast expires.
    pub until: DateTime<Utc>,
}

/// Draw the newest toasts, right-aligned, stacked upward from the bottom of `area`.
pub fn render_toasts(frame: &mut Frame, area: Rect, toasts: &[Toast], theme: Theme) {
    let shown: Vec<&Toast> = toasts.iter().rev().take(VISIBLE).collect();
    for (row, toast) in shown.iter().enumerate() {
        if row as u16 >= area.height {
            break;
        }
        let text = format!(" {} ", toast.text);
        let width = (text.chars().count() as u16).min(area.width);
        let x = area.x + area.width - width;
        let y = area.y + area.height - 1 - row as u16;
        let rect = Rect::new(x, y, width, 1);
        frame.render_widget(Clear, rect);
        frame.render_widget(
            Paragraph::new(Line::styled(text, theme.tone(toast.tone).bg(crate::theme::SELECTION)))
                .alignment(ratatui::layout::Alignment::Right),
            rect,
        );
    }
}
