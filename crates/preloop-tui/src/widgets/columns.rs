//! Bordered panes laid out side by side, with focus styling.
//!
//! A pane's title starts with `─ ` so it reads `┌─ Runs ───┐` as in the
//! approved mockups. Titles are passed in already formatted.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, BorderType, Borders};

use crate::theme::Theme;

/// Split `area` into columns of the given widths. The last column takes the
/// remaining width. Columns never extend past `area`.
pub fn fixed_columns(area: Rect, widths: &[u16]) -> Vec<Rect> {
    let right = area.x.saturating_add(area.width);
    let mut x = area.x;
    let mut out = Vec::with_capacity(widths.len());
    for (index, width) in widths.iter().enumerate() {
        let remaining = right.saturating_sub(x);
        let is_last = index + 1 == widths.len();
        let width = if is_last {
            remaining
        } else {
            (*width).min(remaining)
        };
        out.push(Rect::new(x, area.y, width, area.height));
        x = x.saturating_add(width);
    }
    out
}

/// Draw a bordered pane and return its inner area.
pub fn pane(frame: &mut Frame, area: Rect, title: &str, focused: bool, theme: Theme) -> Rect {
    let block = pane_block(title, focused, theme, Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    inner
}

/// Draw a pane with a top border only, for a pane that continues into the
/// one below it. Returns its inner area.
pub fn pane_open_bottom(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    focused: bool,
    theme: Theme,
) -> Rect {
    let block = pane_block(
        title,
        focused,
        theme,
        Borders::TOP | Borders::LEFT | Borders::RIGHT,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    inner
}

fn pane_block<'a>(title: &'a str, focused: bool, theme: Theme, borders: Borders) -> Block<'a> {
    let title_style = if focused {
        theme.accent()
    } else {
        theme.text()
    };
    Block::default()
        .borders(borders)
        .border_type(BorderType::Plain)
        .border_style(theme.border(focused))
        .title(Line::styled(title.to_owned(), title_style))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_column_takes_the_rest() {
        let columns = fixed_columns(Rect::new(0, 0, 120, 10), &[32, 26, 22, 40]);
        let spans: Vec<(u16, u16)> = columns.iter().map(|rect| (rect.x, rect.width)).collect();
        assert_eq!(spans, vec![(0, 32), (32, 26), (58, 22), (80, 40)]);
    }

    #[test]
    fn columns_stop_at_the_area_edge() {
        let columns = fixed_columns(Rect::new(0, 0, 30, 4), &[20, 20]);
        assert_eq!(columns[0].width, 20);
        assert_eq!(columns[1].width, 10);
    }
}
