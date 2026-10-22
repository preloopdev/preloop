//! Raw CI log rendering: ANSI colour, GitHub-style `##[group]` folding, and
//! an incremental text search.
//!
//! [`LogView`] holds only the view's own state (scroll position, open
//! groups, the active search); the log lines themselves live in the
//! caller's store and are passed in on every call.

use std::collections::BTreeSet;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::theme::{Theme, Tone};

/// One line of raw log output, as the source numbered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// Source-assigned line number.
    pub number: u64,
    /// Raw text: may contain ANSI SGR escapes and `##[...]` markers.
    pub text: String,
}

/// What a line means to the folding and highlighting logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    /// Ordinary output.
    Plain,
    /// Opens a collapsible group (`##[group]...`).
    GroupStart,
    /// Closes the innermost open group (`##[endgroup]`). Never drawn.
    GroupEnd,
    /// `##[error]...`.
    Error,
    /// `##[warning]...`.
    Warning,
    /// `##[notice]...`.
    Notice,
}

/// Classifies a raw line, skipping leading whitespace and leading ANSI SGR
/// escapes first. Matching is case-sensitive.
pub fn classify(text: &str) -> LineKind {
    let s = skip_leading(text);
    if s.starts_with("##[group]") {
        LineKind::GroupStart
    } else if s.starts_with("##[endgroup]") {
        LineKind::GroupEnd
    } else if s.starts_with("##[error]") {
        LineKind::Error
    } else if s.starts_with("##[warning]") {
        LineKind::Warning
    } else if s.starts_with("##[notice]") {
        LineKind::Notice
    } else {
        LineKind::Plain
    }
}

/// Skips leading whitespace and leading `ESC [ <digits/;>* m` sequences.
fn skip_leading(text: &str) -> &str {
    let mut s = text;
    loop {
        let trimmed = s.trim_start();
        if trimmed.len() != s.len() {
            s = trimmed;
            continue;
        }
        match skip_one_sgr(s) {
            Some(rest) => s = rest,
            None => break,
        }
    }
    s
}

/// Strips one leading SGR escape (`ESC [ <digits/;>* m`), if `s` starts with
/// one.
fn skip_one_sgr(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.len() < 3 || bytes[0] != 0x1b || bytes[1] != b'[' {
        return None;
    }
    let mut idx = 2;
    while idx < bytes.len() && (bytes[idx].is_ascii_digit() || bytes[idx] == b';') {
        idx += 1;
    }
    if idx < bytes.len() && bytes[idx] == b'm' {
        Some(&s[idx + 1..])
    } else {
        None
    }
}

/// Parses `text` for ANSI CSI escapes (`ESC [ params final-byte`). Only
/// SGR sequences (final byte `m`) change the style; every other CSI
/// sequence is removed without effect. Adjacent runs with identical
/// resulting style are merged into one span. Malformed sequences (no final
/// byte, or numbers that don't fit) are dropped without panicking.
pub fn parse_ansi(text: &str, base: Style, color: bool) -> Vec<Span<'static>> {
    let chars: Vec<char> = text.chars().collect();
    let mut spans = Vec::new();
    let mut current_text = String::new();
    let mut current_style = base;
    let mut style = base;
    let mut i = 0usize;

    while i < chars.len() {
        if chars[i] == '\u{1b}' && chars.get(i + 1) == Some(&'[') {
            let mut j = i + 2;
            while j < chars.len() && (chars[j].is_ascii_digit() || chars[j] == ';') {
                j += 1;
            }
            if j < chars.len() && chars[j].is_ascii_alphabetic() {
                if chars[j] == 'm' {
                    let params: String = chars[i + 2..j].iter().collect();
                    style = apply_sgr(style, base, &params, color);
                }
                i = j + 1;
            } else {
                // No valid final byte: drop the malformed escape attempt and
                // resume parsing right where it stalled.
                i = j;
            }
            continue;
        }

        if style != current_style {
            if !current_text.is_empty() {
                spans.push(Span::styled(
                    std::mem::take(&mut current_text),
                    current_style,
                ));
            }
            current_style = style;
        }
        current_text.push(chars[i]);
        i += 1;
    }

    if !current_text.is_empty() {
        spans.push(Span::styled(current_text, current_style));
    }

    spans
}

/// Applies one `m`-terminated SGR parameter list to `style`. Code `0`
/// resets to `base`. Colour codes are skipped entirely when `color` is
/// false, though their parameters are still consumed so trailing numbers
/// are not misread as separate codes. Unknown codes are ignored.
fn apply_sgr(style: Style, base: Style, params: &str, color: bool) -> Style {
    let codes: Vec<i64> = if params.is_empty() {
        vec![0]
    } else {
        params
            .split(';')
            .map(|p| p.parse::<i64>().unwrap_or(0))
            .collect()
    };

    let mut style = style;
    let mut idx = 0usize;
    while idx < codes.len() {
        match codes[idx] {
            0 => style = base,
            1 => style = style.add_modifier(Modifier::BOLD),
            2 => style = style.add_modifier(Modifier::DIM),
            3 => style = style.add_modifier(Modifier::ITALIC),
            4 => style = style.add_modifier(Modifier::UNDERLINED),
            7 => style = style.add_modifier(Modifier::REVERSED),
            22 => {
                style = style
                    .remove_modifier(Modifier::BOLD)
                    .remove_modifier(Modifier::DIM)
            }
            23 => style = style.remove_modifier(Modifier::ITALIC),
            24 => style = style.remove_modifier(Modifier::UNDERLINED),
            27 => style = style.remove_modifier(Modifier::REVERSED),
            code @ 30..=37 if color => style.fg = Some(standard_color((code - 30) as u8)),
            39 if color => style.fg = base.fg,
            code @ 40..=47 if color => style.bg = Some(standard_color((code - 40) as u8)),
            49 if color => style.bg = base.bg,
            code @ 90..=97 if color => style.fg = Some(bright_color((code - 90) as u8)),
            code @ 100..=107 if color => style.bg = Some(bright_color((code - 100) as u8)),
            38 => {
                if let Some((consumed, result)) = extended_color(&codes, idx, color) {
                    if let Some(c) = result {
                        style.fg = Some(c);
                    }
                    idx = consumed;
                }
            }
            48 => {
                if let Some((consumed, result)) = extended_color(&codes, idx, color) {
                    if let Some(c) = result {
                        style.bg = Some(c);
                    }
                    idx = consumed;
                }
            }
            _ => {}
        }
        idx += 1;
    }
    style
}

/// Parses the `5;n` or `2;r;g;b` tail of an extended colour code (`38` or
/// `48`) starting after `codes[idx]`. Returns the index of the last
/// consumed parameter, and the colour when `color` is on and the
/// parameters are well formed. Returns `None` when the tail is missing or
/// malformed, in which case nothing is consumed.
fn extended_color(codes: &[i64], idx: usize, color: bool) -> Option<(usize, Option<Color>)> {
    match codes.get(idx + 1).copied() {
        Some(5) => {
            let n = codes.get(idx + 2).copied()?;
            let indexed = u8::try_from(n).ok();
            Some((
                idx + 2,
                if color {
                    indexed.map(Color::Indexed)
                } else {
                    None
                },
            ))
        }
        Some(2) => {
            let r = codes.get(idx + 2).copied()?;
            let g = codes.get(idx + 3).copied()?;
            let b = codes.get(idx + 4).copied()?;
            let rgb = match (u8::try_from(r), u8::try_from(g), u8::try_from(b)) {
                (Ok(r), Ok(g), Ok(b)) => Some((r, g, b)),
                _ => None,
            };
            Some((
                idx + 4,
                if color {
                    rgb.map(|(r, g, b)| Color::Rgb(r, g, b))
                } else {
                    None
                },
            ))
        }
        _ => None,
    }
}

/// Maps SGR 30-37/40-47 offsets (0-7) to the standard ANSI colours.
fn standard_color(n: u8) -> Color {
    match n {
        0 => Color::Black,
        1 => Color::Red,
        2 => Color::Green,
        3 => Color::Yellow,
        4 => Color::Blue,
        5 => Color::Magenta,
        6 => Color::Cyan,
        _ => Color::Gray,
    }
}

/// Maps SGR 90-97/100-107 offsets (0-7) to the bright ANSI colours.
fn bright_color(n: u8) -> Color {
    match n {
        0 => Color::DarkGray,
        1 => Color::LightRed,
        2 => Color::LightGreen,
        3 => Color::LightYellow,
        4 => Color::LightBlue,
        5 => Color::LightMagenta,
        6 => Color::LightCyan,
        _ => Color::White,
    }
}

/// Scroll position, open groups and search state for one log pane.
///
/// Rendering is a pure function of this state plus the lines; nothing here
/// is mutated while drawing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogView {
    /// Scroll offset in visible rows, used when `follow` is false.
    pub top: usize,
    /// When true, the view tracks the bottom of the buffer as it grows.
    pub follow: bool,
    /// Indices (into the `lines` slice) of `GroupStart` lines the user
    /// opened. Everything else starts, and stays, collapsed.
    pub expanded: BTreeSet<usize>,
    /// The active search string, if any.
    pub query: Option<String>,
    /// Indices of lines matching `query`, in buffer order.
    pub matches: Vec<usize>,
    /// Index into `matches` of the current match.
    pub match_cursor: usize,
}

/// Rows of scroll context kept above a revealed search match.
const REVEAL_CONTEXT: usize = 3;

impl LogView {
    /// A fresh view: following the tail, nothing expanded, no search.
    pub fn new() -> Self {
        Self {
            top: 0,
            follow: true,
            expanded: BTreeSet::new(),
            query: None,
            matches: Vec::new(),
            match_cursor: 0,
        }
    }

    /// Indices of `lines` to draw, in order. Lines inside a collapsed group
    /// are omitted; `GroupEnd` lines are always omitted.
    pub fn visible_rows(&self, lines: &[LogLine]) -> Vec<usize> {
        let mut rows = Vec::new();
        let mut index = 0usize;
        while index < lines.len() {
            match classify(&lines[index].text) {
                LineKind::GroupEnd => {}
                LineKind::GroupStart => {
                    rows.push(index);
                    if self.expanded.contains(&index) {
                        index += 1;
                        continue;
                    }
                    let end = Self::group_end(lines, index);
                    index = end + 1;
                    continue;
                }
                _ => rows.push(index),
            }
            index += 1;
        }
        rows
    }

    /// Index of the `GroupEnd` matching the `GroupStart` at `start`, or
    /// `lines.len()` when the group runs to the end of the buffer.
    fn group_end(lines: &[LogLine], start: usize) -> usize {
        let mut depth = 0i32;
        let mut i = start + 1;
        while i < lines.len() {
            match classify(&lines[i].text) {
                LineKind::GroupStart => depth += 1,
                LineKind::GroupEnd => {
                    if depth == 0 {
                        return i;
                    }
                    depth -= 1;
                }
                _ => {}
            }
            i += 1;
        }
        lines.len()
    }

    /// The `GroupStart` whose group contains `index` (a `GroupStart`
    /// counts as contained by itself), or `None` outside any group.
    pub fn enclosing_group(lines: &[LogLine], index: usize) -> Option<usize> {
        if index >= lines.len() {
            return None;
        }
        let mut stack: Vec<usize> = Vec::new();
        for (i, line) in lines.iter().enumerate().take(index + 1) {
            match classify(&line.text) {
                LineKind::GroupStart => stack.push(i),
                LineKind::GroupEnd => {
                    stack.pop();
                }
                _ => {}
            }
        }
        stack.last().copied()
    }

    /// Opens or closes the group started at `line_index`.
    pub fn toggle_group(&mut self, line_index: usize) {
        if !self.expanded.remove(&line_index) {
            self.expanded.insert(line_index);
        }
    }

    /// Flips whether the view tracks the bottom of the buffer.
    pub fn toggle_follow(&mut self) {
        self.follow = !self.follow;
    }

    /// Moves the scroll offset by `delta` rows from its current effective
    /// position, clamps to the valid range, and stops following the tail.
    pub fn scroll(&mut self, delta: isize, lines: &[LogLine], height: usize) {
        let rows = self.visible_rows(lines);
        let max_top = rows.len().saturating_sub(height);
        let effective = if self.follow {
            max_top
        } else {
            self.top.min(max_top)
        };
        let next = effective.saturating_add_signed(delta).min(max_top);
        self.top = next;
        self.follow = false;
    }

    /// Jumps to the top of the buffer and stops following.
    pub fn jump_top(&mut self) {
        self.top = 0;
        self.follow = false;
    }

    /// Jumps to, and starts following, the bottom of the buffer.
    pub fn jump_bottom(&mut self) {
        self.follow = true;
    }

    /// Sets the search query. An empty query clears the search. Otherwise
    /// finds every case-insensitive match (regardless of folds), resets the
    /// match cursor to the first one, and reveals it.
    pub fn set_query(&mut self, lines: &[LogLine], query: &str) {
        if query.is_empty() {
            self.query = None;
            self.matches.clear();
            self.match_cursor = 0;
            return;
        }
        let needle = query.to_lowercase();
        self.query = Some(query.to_string());
        self.matches = lines
            .iter()
            .enumerate()
            .filter_map(|(i, line)| line.text.to_lowercase().contains(&needle).then_some(i))
            .collect();
        self.match_cursor = 0;
        if let Some(&first) = self.matches.first() {
            self.reveal(lines, first);
        }
    }

    /// Moves to the next match, wrapping around, and reveals it.
    pub fn next_match(&mut self, lines: &[LogLine]) {
        if self.matches.is_empty() {
            return;
        }
        self.match_cursor = (self.match_cursor + 1) % self.matches.len();
        let index = self.matches[self.match_cursor];
        self.reveal(lines, index);
    }

    /// Moves to the previous match, wrapping around, and reveals it.
    pub fn prev_match(&mut self, lines: &[LogLine]) {
        if self.matches.is_empty() {
            return;
        }
        self.match_cursor = if self.match_cursor == 0 {
            self.matches.len() - 1
        } else {
            self.match_cursor - 1
        };
        let index = self.matches[self.match_cursor];
        self.reveal(lines, index);
    }

    /// Expands `index`'s enclosing group if it is hidden, stops following,
    /// and scrolls so `index` sits near the top with a little context.
    /// `render` clamps the resulting `top`.
    fn reveal(&mut self, lines: &[LogLine], index: usize) {
        if let Some(group) = Self::enclosing_group(lines, index) {
            self.expanded.insert(group);
        }
        self.follow = false;
        self.top = index.saturating_sub(REVEAL_CONTEXT);
    }

    /// "n/N" (1-based) when there are matches, "no matches" when a query is
    /// set but nothing matched, `None` when no query is set.
    pub fn match_status(&self) -> Option<String> {
        self.query.as_ref()?;
        if self.matches.is_empty() {
            Some("no matches".to_string())
        } else {
            Some(format!("{}/{}", self.match_cursor + 1, self.matches.len()))
        }
    }

    /// Draws the view. Pure: takes no state and keeps none.
    pub fn render(&self, frame: &mut Frame, area: Rect, lines: &[LogLine], theme: Theme) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let rows = self.visible_rows(lines);
        if rows.is_empty() {
            return;
        }
        let height = area.height as usize;
        let max_top = rows.len().saturating_sub(height);
        let top = if self.follow {
            max_top
        } else {
            self.top.min(max_top)
        };

        let number_width = lines
            .iter()
            .map(|l| l.number.to_string().len())
            .max()
            .unwrap_or(1)
            .max(1);
        let current_match_row = self.matches.get(self.match_cursor).copied();

        for (screen_row, &row_index) in rows.iter().enumerate().skip(top).take(height) {
            let offset = screen_row - top;
            let line = &lines[row_index];
            let kind = classify(&line.text);

            let mut spans: Vec<Span<'static>> = vec![Span::styled(
                format!("{:>number_width$} ", line.number),
                theme.muted(),
            )];

            match kind {
                LineKind::GroupStart => {
                    let expanded = self.expanded.contains(&row_index);
                    spans.push(Span::styled(
                        if expanded { "▾ " } else { "▸ " },
                        theme.text(),
                    ));
                    spans.extend(parse_ansi(&line.text, theme.text(), theme.is_color()));
                    if !expanded {
                        let end = Self::group_end(lines, row_index);
                        let count = end.saturating_sub(row_index + 1);
                        spans.push(Span::styled(format!(" ({count} lines)"), theme.muted()));
                    }
                }
                LineKind::Error => {
                    spans.push(Span::styled(line.text.clone(), theme.tone(Tone::Error)))
                }
                LineKind::Warning => {
                    spans.push(Span::styled(line.text.clone(), theme.tone(Tone::Warning)));
                }
                LineKind::Notice => {
                    spans.push(Span::styled(line.text.clone(), theme.tone(Tone::Accent)))
                }
                LineKind::Plain => {
                    spans.extend(parse_ansi(&line.text, theme.text(), theme.is_color()))
                }
                LineKind::GroupEnd => {}
            }

            let row_area = Rect {
                x: area.x,
                y: area.y + offset as u16,
                width: area.width,
                height: 1,
            };
            let paragraph = if current_match_row == Some(row_index) {
                Paragraph::new(Line::from(spans)).style(theme.selected())
            } else {
                Paragraph::new(Line::from(spans))
            };
            frame.render_widget(paragraph, row_area);
        }
    }
}

impl Default for LogView {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(number: u64, text: &str) -> LogLine {
        LogLine {
            number,
            text: text.to_string(),
        }
    }

    fn span_text(spans: &[Span<'static>]) -> String {
        spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn classify_every_prefix() {
        assert_eq!(classify("plain text"), LineKind::Plain);
        assert_eq!(classify("##[group]Build"), LineKind::GroupStart);
        assert_eq!(classify("##[endgroup]"), LineKind::GroupEnd);
        assert_eq!(classify("##[error]boom"), LineKind::Error);
        assert_eq!(classify("##[warning]careful"), LineKind::Warning);
        assert_eq!(classify("##[notice]fyi"), LineKind::Notice);
        assert_eq!(classify("  \t ##[group]Indented"), LineKind::GroupStart);
        assert_eq!(classify("\u{1b}[1m##[error]coloured"), LineKind::Error);
    }

    #[test]
    fn parse_ansi_bold_red_then_reset() {
        let spans = parse_ansi("\u{1b}[1;31mHello\u{1b}[0m World", Style::default(), true);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].content.as_ref(), "Hello");
        assert!(spans[0].style.add_modifier.contains(Modifier::BOLD));
        assert_eq!(spans[0].style.fg, Some(Color::Red));
        assert_eq!(spans[1].content.as_ref(), " World");
        assert_eq!(spans[1].style, Style::default());
    }

    #[test]
    fn parse_ansi_256_colour() {
        let spans = parse_ansi("\u{1b}[38;5;202mHi\u{1b}[0m", Style::default(), true);
        assert_eq!(spans[0].style.fg, Some(Color::Indexed(202)));
    }

    #[test]
    fn parse_ansi_rgb_colour() {
        let spans = parse_ansi("\u{1b}[38;2;10;20;30mHi\u{1b}[0m", Style::default(), true);
        assert_eq!(spans[0].style.fg, Some(Color::Rgb(10, 20, 30)));
    }

    #[test]
    fn parse_ansi_monochrome_drops_colour_keeps_emphasis() {
        let spans = parse_ansi("\u{1b}[31;1mHi\u{1b}[0m", Style::default(), false);
        assert_eq!(spans[0].style.fg, None);
        assert!(spans[0].style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn parse_ansi_malformed_input_never_panics() {
        let inputs = [
            "\u{1b}[1",
            "\u{1b}[",
            "\u{1b}",
            "\u{1b}[38;5m",
            "\u{1b}[38;2;1;2m",
            "\u{1b}[9999999999999999999mtext",
            "plain \u{1b}[zm text",
        ];
        for input in inputs {
            let spans = parse_ansi(input, Style::default(), true);
            // Must not panic; text content should still be recoverable.
            assert!(span_text(&spans).len() <= input.len());
        }
    }

    fn sample_lines() -> Vec<LogLine> {
        vec![
            line(1, "start"),
            line(2, "##[group]Step A"),
            line(3, "inside A line 1"),
            line(4, "inside A line 2"),
            line(5, "##[endgroup]"),
            line(6, "after"),
        ]
    }

    #[test]
    fn visible_rows_hides_collapsed_group_body() {
        let lines = sample_lines();
        let view = LogView::new();
        assert_eq!(view.visible_rows(&lines), vec![0, 1, 5]);
    }

    #[test]
    fn visible_rows_shows_expanded_group_body_but_not_endgroup() {
        let lines = sample_lines();
        let mut view = LogView::new();
        view.toggle_group(1);
        assert_eq!(view.visible_rows(&lines), vec![0, 1, 2, 3, 5]);
    }

    #[test]
    fn search_reveals_match_inside_collapsed_group() {
        let lines = sample_lines();
        let mut view = LogView::new();
        view.set_query(&lines, "line 1");
        assert_eq!(view.matches, vec![2]);
        assert!(view.expanded.contains(&1));
        assert!(view.visible_rows(&lines).contains(&2));
        assert_eq!(view.match_status().as_deref(), Some("1/1"));
    }

    #[test]
    fn search_with_no_matches_reports_status() {
        let lines = sample_lines();
        let mut view = LogView::new();
        view.set_query(&lines, "does not appear");
        assert!(view.matches.is_empty());
        assert_eq!(view.match_status().as_deref(), Some("no matches"));
    }

    #[test]
    fn scroll_clamps_to_valid_range() {
        let lines: Vec<LogLine> = (0..20).map(|i| line(i, &format!("line {i}"))).collect();
        let mut view = LogView::new();

        view.scroll(100, &lines, 5);
        assert_eq!(view.top, 15);
        assert!(!view.follow);

        view.scroll(-1000, &lines, 5);
        assert_eq!(view.top, 0);
    }

    #[test]
    fn default_matches_new() {
        assert_eq!(LogView::default(), LogView::new());
    }
}
