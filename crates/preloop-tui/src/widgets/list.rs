//! Fuzzy filtering and a stateless, scroll-aware list renderer.
//!
//! The list holds no state of its own: the caller tracks the cursor and
//! passes it in on every draw, so [`render_list`] never has to remember how
//! it scrolled last time.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use crate::theme::Theme;

/// Per-match bonus for each query character found in the candidate.
const MATCH_BONUS: i64 = 16;
/// Bonus when a match lands at a word boundary (start of string, or right
/// after a non-alphanumeric character).
const WORD_START_BONUS: i64 = 24;
/// Bonus when a match is immediately after the previous match.
const CONTIGUOUS_BONUS: i64 = 20;
/// Bonus when the whole query is a prefix of the candidate.
const PREFIX_BONUS: i64 = 40;

/// Scores `candidate` against `query` as a case-insensitive subsequence
/// match, or returns `None` when the query's characters do not all appear
/// in `candidate`, in order. An empty query always scores `0`.
pub fn fuzzy_score(query: &str, candidate: &str) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }

    let query_chars: Vec<char> = query.chars().flat_map(char::to_lowercase).collect();
    let cand_chars: Vec<char> = candidate.chars().collect();
    let cand_lower: Vec<char> = cand_chars
        .iter()
        .copied()
        .flat_map(char::to_lowercase)
        .collect();

    let mut score: i64 = 0;
    let mut search_from = 0usize;
    let mut prev_match: Option<usize> = None;
    let mut last_match = 0usize;

    for &qc in &query_chars {
        let found = cand_lower
            .get(search_from..)?
            .iter()
            .position(|&c| c == qc)?;
        let idx = search_from + found;

        score += MATCH_BONUS;
        let at_word_start = idx == 0 || !cand_chars[idx - 1].is_alphanumeric();
        if at_word_start {
            score += WORD_START_BONUS;
        }
        if idx > 0 && prev_match == Some(idx - 1) {
            score += CONTIGUOUS_BONUS;
        }

        prev_match = Some(idx);
        last_match = idx;
        search_from = idx + 1;
    }

    let query_len = query_chars.len();
    if cand_lower.len() >= query_len && cand_lower[..query_len] == query_chars[..] {
        score += PREFIX_BONUS;
    }

    let skipped = (last_match + 1).saturating_sub(query_len);
    score -= skipped as i64;

    Some(score)
}

/// Indices of `texts` that match `query`, sorted by score descending, then
/// by index ascending. An empty query returns every index, in order.
pub fn fuzzy_filter(query: &str, texts: &[String]) -> Vec<usize> {
    let mut scored: Vec<(usize, i64)> = texts
        .iter()
        .enumerate()
        .filter_map(|(index, text)| fuzzy_score(query, text).map(|score| (index, score)))
        .collect();
    scored.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    scored.into_iter().map(|(index, _)| index).collect()
}

/// One row (or multi-line block) in a list. Header items are skipped by the
/// cursor and never highlighted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListItem {
    /// The rows this item occupies, top to bottom.
    pub lines: Vec<Line<'static>>,
    /// Whether the cursor may land on this item.
    pub selectable: bool,
}

impl ListItem {
    /// A normal, cursor-reachable item.
    pub fn selectable(lines: Vec<Line<'static>>) -> Self {
        Self {
            lines,
            selectable: true,
        }
    }

    /// A section header: drawn, but never selected.
    pub fn header(lines: Vec<Line<'static>>) -> Self {
        Self {
            lines,
            selectable: false,
        }
    }
}

/// Draws `items` into `area`, scrolling only as far as needed to keep the
/// item at `cursor` fully visible. Pure: takes no state and keeps none.
pub fn render_list(frame: &mut Frame, area: Rect, items: &[ListItem], cursor: usize, theme: Theme) {
    if items.is_empty() || area.height == 0 {
        return;
    }
    let height = area.height as usize;

    let mut starts = Vec::with_capacity(items.len());
    let mut row = 0usize;
    for item in items {
        starts.push(row);
        row += item.lines.len();
    }

    let cursor_item = cursor.min(items.len() - 1);
    let cursor_start = starts[cursor_item];
    let cursor_rows = items[cursor_item].lines.len();
    let cursor_last_row = cursor_start + cursor_rows.saturating_sub(1);

    let mut scroll_top = cursor_last_row.saturating_sub(height.saturating_sub(1));
    if scroll_top > cursor_start {
        scroll_top = cursor_start;
    }

    for (index, item) in items.iter().enumerate() {
        let item_start = starts[index];
        for (line_index, line) in item.lines.iter().enumerate() {
            let absolute_row = item_start + line_index;
            if absolute_row < scroll_top {
                continue;
            }
            let offset = absolute_row - scroll_top;
            if offset >= height {
                return;
            }
            let row_area = Rect {
                x: area.x,
                y: area.y + offset as u16,
                width: area.width,
                height: 1,
            };
            if index == cursor_item && item.selectable {
                frame.render_widget(
                    Paragraph::new(line.clone()).style(theme.selected()),
                    row_area,
                );
            } else {
                frame.render_widget(Paragraph::new(line.clone()), row_area);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn empty_query_scores_zero() {
        assert_eq!(fuzzy_score("", "anything"), Some(0));
    }

    #[test]
    fn case_insensitive_subsequence_match() {
        assert!(fuzzy_score("UBN", "urbanist").is_some());
        assert_eq!(
            fuzzy_score("ban", "urbanist"),
            fuzzy_score("BAN", "urbanist")
        );
    }

    #[test]
    fn non_subsequence_is_none() {
        assert_eq!(fuzzy_score("xyz", "urbanist"), None);
    }

    #[test]
    fn ranks_prefix_above_contiguous_above_scattered() {
        let prefix = fuzzy_score("ban", "banana").unwrap();
        let contiguous = fuzzy_score("ban", "urbanist").unwrap();
        let scattered = fuzzy_score("ban", "xbyaznx").unwrap();
        assert!(prefix > contiguous, "{prefix} should beat {contiguous}");
        assert!(
            contiguous > scattered,
            "{contiguous} should beat {scattered}"
        );
    }

    #[test]
    fn fuzzy_filter_orders_by_score_then_index() {
        let texts = vec![
            "xbyaznx".to_string(),
            "banana".to_string(),
            "urbanist".to_string(),
            "no match here".to_string(),
            "banner".to_string(),
        ];
        let result = fuzzy_filter("ban", &texts);
        assert_eq!(result, vec![1, 4, 2, 0]);
    }

    #[test]
    fn fuzzy_filter_empty_query_keeps_order() {
        let texts = vec!["c".to_string(), "a".to_string(), "b".to_string()];
        assert_eq!(fuzzy_filter("", &texts), vec![0, 1, 2]);
    }

    fn item(label: &str) -> ListItem {
        ListItem::selectable(vec![Line::from(label.to_string())])
    }

    #[test]
    fn render_list_keeps_cursor_visible_in_small_viewport() {
        let items: Vec<ListItem> = (0..10).map(|i| item(&format!("Item {i}"))).collect();
        let backend = TestBackend::new(20, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        let theme = Theme::monochrome();

        terminal
            .draw(|frame| {
                let area = frame.area();
                render_list(frame, area, &items, 9, theme);
            })
            .unwrap();

        let buffer = terminal.backend().buffer();
        let mut found = false;
        for y in 0..3 {
            let mut line = String::new();
            for x in 0..20 {
                if let Some(cell) = buffer.cell((x, y)) {
                    line.push_str(cell.symbol());
                }
            }
            if line.contains("Item 9") {
                found = true;
            }
        }
        assert!(found, "cursor item should be visible");
    }

    #[test]
    fn render_list_does_nothing_for_empty_items() {
        let items: Vec<ListItem> = Vec::new();
        let backend = TestBackend::new(10, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        let theme = Theme::monochrome();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render_list(frame, area, &items, 0, theme);
            })
            .unwrap();
    }
}
