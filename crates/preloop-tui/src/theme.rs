//! Colours from the approved mockups, with a monochrome mode for `NO_COLOR`.
//!
//! Views and widgets never name a colour directly. They ask the [`Theme`] for a
//! [`Tone`] or a pane border, so monochrome mode works everywhere.

use ratatui::style::{Color, Modifier, Style};

/// Body text.
pub const FG: Color = Color::Rgb(0xdd, 0xdd, 0xdd);
/// Secondary text and dimmed hints.
pub const MUTED: Color = Color::Rgb(0x88, 0x88, 0x88);
/// Focus, links, the active tab, and key hints.
pub const ACCENT: Color = Color::Rgb(0x66, 0xcc, 0xff);
/// Unfocused pane borders.
pub const BORDER: Color = Color::Rgb(0x55, 0x55, 0x55);
/// Passing, finished, healthy.
pub const SUCCESS: Color = Color::Rgb(0x66, 0xdd, 0x66);
/// Needs attention.
pub const WARNING: Color = Color::Rgb(0xff, 0xcc, 0x55);
/// Failed, dead, broken.
pub const ERROR: Color = Color::Rgb(0xff, 0x66, 0x66);
/// Background of the selected row.
pub const SELECTION: Color = Color::Rgb(0x1d, 0x3a, 0x4a);

/// The meaning of a piece of text, independent of colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// Ordinary text.
    Plain,
    /// Secondary text.
    Muted,
    /// Emphasis and focus.
    Accent,
    /// Good news.
    Success,
    /// Needs attention.
    Warning,
    /// Something is broken.
    Error,
}

/// Whether colour is in use. Copy it freely; it is two bits of state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Theme {
    color: bool,
}

impl Theme {
    /// Full colour.
    pub const fn color() -> Self {
        Self { color: true }
    }

    /// No colour. Emphasis uses bold, selection uses reverse video.
    pub const fn monochrome() -> Self {
        Self { color: false }
    }

    /// Colour unless `NO_COLOR` is set to a non-empty value.
    pub fn from_env() -> Self {
        if std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()) {
            Self::monochrome()
        } else {
            Self::color()
        }
    }

    /// Whether colour is in use.
    pub fn is_color(self) -> bool {
        self.color
    }

    /// Style for a tone.
    pub fn tone(self, tone: Tone) -> Style {
        if !self.color {
            return match tone {
                Tone::Plain | Tone::Muted => Style::default(),
                Tone::Accent | Tone::Success | Tone::Warning | Tone::Error => {
                    Style::default().add_modifier(Modifier::BOLD)
                }
            };
        }
        let fg = match tone {
            Tone::Plain => FG,
            Tone::Muted => MUTED,
            Tone::Accent => ACCENT,
            Tone::Success => SUCCESS,
            Tone::Warning => WARNING,
            Tone::Error => ERROR,
        };
        Style::default().fg(fg)
    }

    /// Ordinary body text.
    pub fn text(self) -> Style {
        self.tone(Tone::Plain)
    }

    /// Secondary text.
    pub fn muted(self) -> Style {
        self.tone(Tone::Muted)
    }

    /// Emphasis and focus.
    pub fn accent(self) -> Style {
        self.tone(Tone::Accent)
    }

    /// Border of a pane. The focused pane takes the accent colour.
    pub fn border(self, focused: bool) -> Style {
        match (self.color, focused) {
            (false, true) => Style::default().add_modifier(Modifier::BOLD),
            (false, false) => Style::default(),
            (true, true) => Style::default().fg(ACCENT),
            (true, false) => Style::default().fg(BORDER),
        }
    }

    /// The selected row: accent background at low intensity, or reverse video.
    pub fn selected(self) -> Style {
        if self.color {
            Style::default().bg(SELECTION).fg(FG)
        } else {
            Style::default().add_modifier(Modifier::REVERSED)
        }
    }

    /// Header and footer bars.
    pub fn bar(self) -> Style {
        if self.color {
            Style::default().fg(FG)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monochrome_drops_colour_but_keeps_emphasis() {
        let theme = Theme::monochrome();
        assert_eq!(theme.tone(Tone::Error).fg, None);
        assert!(
            theme
                .tone(Tone::Error)
                .add_modifier
                .contains(Modifier::BOLD)
        );
        assert!(theme.selected().add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn colour_uses_mockup_palette() {
        let theme = Theme::color();
        assert_eq!(theme.tone(Tone::Success).fg, Some(SUCCESS));
        assert_eq!(theme.border(true).fg, Some(ACCENT));
        assert_eq!(theme.border(false).fg, Some(BORDER));
        assert_eq!(theme.selected().bg, Some(SELECTION));
    }
}
