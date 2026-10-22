//! A single-line, editable text field with a char-index cursor.
//!
//! [`TextInput`] holds no rendering state of its own; callers decide whether to
//! draw the cursor via [`TextInput::line`].

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::text::{Line, Span};

use crate::theme::Theme;

/// The cursor marker drawn when the field is being edited.
const CURSOR_MARKER: char = '▏';

/// A single-line text field. The cursor is a character index, never a byte
/// offset, so it always lands on a UTF-8 boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextInput {
    value: String,
    cursor: usize,
}

impl TextInput {
    /// Builds a field holding `value`, with the cursor at the end.
    pub fn new(value: &str) -> Self {
        let cursor = value.chars().count();
        Self {
            value: value.to_string(),
            cursor,
        }
    }

    /// The current contents, without any cursor marker.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Empties the field.
    pub fn clear(&mut self) {
        self.value.clear();
        self.cursor = 0;
    }

    /// Byte offset of the char at `char_index`, or `value.len()` if
    /// `char_index` is at or past the end.
    fn byte_offset(&self, char_index: usize) -> usize {
        self.value
            .char_indices()
            .nth(char_index)
            .map(|(byte, _)| byte)
            .unwrap_or(self.value.len())
    }

    /// Inserts `c` at the cursor and advances the cursor past it.
    pub fn insert(&mut self, c: char) {
        let at = self.byte_offset(self.cursor);
        self.value.insert(at, c);
        self.cursor += 1;
    }

    /// Deletes the character before the cursor, if any.
    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let start = self.byte_offset(self.cursor - 1);
        let end = self.byte_offset(self.cursor);
        self.value.replace_range(start..end, "");
        self.cursor -= 1;
    }

    /// Deletes the character at the cursor, if any.
    pub fn delete(&mut self) {
        let len = self.value.chars().count();
        if self.cursor >= len {
            return;
        }
        let start = self.byte_offset(self.cursor);
        let end = self.byte_offset(self.cursor + 1);
        self.value.replace_range(start..end, "");
    }

    /// Moves the cursor one character left, if possible.
    pub fn move_left(&mut self) {
        self.cursor = self.cursor.saturating_sub(1);
    }

    /// Moves the cursor one character right, if possible.
    pub fn move_right(&mut self) {
        let len = self.value.chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    /// Moves the cursor to the start of the field.
    pub fn home(&mut self) {
        self.cursor = 0;
    }

    /// Moves the cursor to the end of the field.
    pub fn end(&mut self) {
        self.cursor = self.value.chars().count();
    }

    /// Applies a key event. Returns `true` when the event was consumed.
    ///
    /// Only key-down events (`Press` or `Repeat`) act; releases are ignored.
    /// `Enter`, `Esc` and `Tab` are left for the caller and always return
    /// `false`.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return false;
        }
        match key.code {
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.clear();
                true
            }
            KeyCode::Char(c)
                if !key.modifiers.contains(KeyModifiers::CONTROL)
                    && !key.modifiers.contains(KeyModifiers::ALT) =>
            {
                self.insert(c);
                true
            }
            KeyCode::Backspace => {
                self.backspace();
                true
            }
            KeyCode::Delete => {
                self.delete();
                true
            }
            KeyCode::Left => {
                self.move_left();
                true
            }
            KeyCode::Right => {
                self.move_right();
                true
            }
            KeyCode::Home => {
                self.home();
                true
            }
            KeyCode::End => {
                self.end();
                true
            }
            _ => false,
        }
    }

    /// Renders the field as a single styled line. When `editing` is true a
    /// cursor marker is spliced in at the cursor position; it is never part
    /// of [`TextInput::value`].
    pub fn line(&self, theme: Theme, editing: bool) -> Line<'static> {
        let style = theme.text();
        if !editing {
            return Line::from(Span::styled(self.value.clone(), style));
        }
        let at = self.byte_offset(self.cursor);
        let mut text = String::with_capacity(self.value.len() + CURSOR_MARKER.len_utf8());
        text.push_str(&self.value[..at]);
        text.push(CURSOR_MARKER);
        text.push_str(&self.value[at..]);
        Line::from(Span::styled(text, style))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn release(code: KeyCode) -> KeyEvent {
        let mut key = KeyEvent::new(code, KeyModifiers::NONE);
        key.kind = KeyEventKind::Release;
        key
    }

    #[test]
    fn new_places_cursor_at_end() {
        let field = TextInput::new("héllo");
        assert_eq!(field.cursor, 5);
        assert_eq!(field.value(), "héllo");
    }

    #[test]
    fn insert_and_backspace_handle_multibyte_chars() {
        let mut field = TextInput::new("héllo");
        field.cursor = 2; // after "hé"
        field.insert('€');
        assert_eq!(field.value(), "hé€llo");
        assert_eq!(field.cursor, 3);

        field.backspace();
        assert_eq!(field.value(), "héllo");
        assert_eq!(field.cursor, 2);

        field.backspace();
        assert_eq!(field.value(), "hllo");
        assert_eq!(field.cursor, 1);
    }

    #[test]
    fn delete_removes_char_at_cursor() {
        let mut field = TextInput::new("héllo");
        field.cursor = 1; // before 'é'
        field.delete();
        assert_eq!(field.value(), "hllo");
        assert_eq!(field.cursor, 1);
    }

    #[test]
    fn cursor_moves_clamp_at_edges() {
        let mut field = TextInput::new("ab");
        field.home();
        field.move_left();
        assert_eq!(field.cursor, 0);
        field.end();
        move_right_past_end(&mut field);
        assert_eq!(field.cursor, 2);
    }

    fn move_right_past_end(field: &mut TextInput) {
        field.move_right();
    }

    #[test]
    fn handle_key_ignores_non_press_events() {
        let mut field = TextInput::new("ab");
        assert!(!field.handle_key(release(KeyCode::Char('x'))));
        assert_eq!(field.value(), "ab");
    }

    #[test]
    fn handle_key_ignores_control_and_alt_chars() {
        let mut field = TextInput::new("ab");
        let mut ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        ctrl_c.kind = KeyEventKind::Press;
        assert!(!field.handle_key(ctrl_c));
        assert_eq!(field.value(), "ab");
    }

    #[test]
    fn handle_key_ctrl_u_clears() {
        let mut field = TextInput::new("ab");
        let mut ctrl_u = KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL);
        ctrl_u.kind = KeyEventKind::Press;
        assert!(field.handle_key(ctrl_u));
        assert_eq!(field.value(), "");
    }

    #[test]
    fn handle_key_enter_esc_tab_not_consumed() {
        let mut field = TextInput::new("ab");
        assert!(!field.handle_key(press(KeyCode::Enter)));
        assert!(!field.handle_key(press(KeyCode::Esc)));
        assert!(!field.handle_key(press(KeyCode::Tab)));
    }

    #[test]
    fn line_inserts_cursor_marker_only_for_display() {
        let mut field = TextInput::new("ab");
        field.cursor = 1;
        let theme = Theme::monochrome();
        let line = field.line(theme, true);
        let text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(text, format!("a{CURSOR_MARKER}b"));
        assert_eq!(field.value(), "ab");

        let plain = field.line(theme, false);
        let plain_text: String = plain.spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(plain_text, "ab");
    }
}
