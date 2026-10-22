//! Key/value rows: a muted key column with the value after it.

use ratatui::text::{Line, Span};

use crate::theme::Theme;

/// One row of a key/value pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kv {
    /// Left-hand label.
    pub key: String,
    /// Right-hand value, already styled.
    pub value: Line<'static>,
}

impl Kv {
    /// A row with a plain-text value.
    pub fn text(key: impl Into<String>, value: impl Into<String>, theme: Theme) -> Self {
        Self {
            key: key.into(),
            value: Line::styled(value.into(), theme.text()),
        }
    }
}

/// Build the lines of a key/value pane. Each line is one space, the key padded
/// to `key_width` columns, then the value.
pub fn kv_lines(rows: &[Kv], key_width: usize, theme: Theme) -> Vec<Line<'static>> {
    rows.iter()
        .map(|row| {
            let mut spans = vec![Span::styled(
                format!(" {:<width$}", row.key, width = key_width),
                theme.muted(),
            )];
            spans.extend(row.value.spans.iter().cloned());
            Line::from(spans)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_start_after_the_key_column() {
        let rows = [Kv::text("jobs ready", "2", Theme::monochrome())];
        let lines = kv_lines(&rows, 21, Theme::monochrome());
        let text: String = lines[0]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(text.find('2'), Some(22));
        assert!(text.starts_with(" jobs ready"));
    }
}
