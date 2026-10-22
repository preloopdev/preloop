//! Setup screen: the interactive init wizard driven through the wizard bridge.

use ratatui::Frame;
use ratatui::crossterm::event::{KeyCode, KeyEvent};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use super::View;
use crate::app::{App, Effect};
use crate::theme::{Theme, Tone};
use crate::widgets::columns::pane;
use crate::widgets::text_input::TextInput;
use crate::wizard::{NoteLevel, WizardReply, WizardRequest};

/// State of the setup wizard screen.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct State {
    /// The latest request (and the request currently waiting for an answer).
    pub request: Option<WizardRequest>,
    /// Lines emitted by the wizard since the current run began.
    pub notes: Vec<(NoteLevel, String)>,
    /// Selected choice or current confirm value.
    pub cursor: usize,
    /// Value being edited for a text or secret prompt.
    pub input: TextInput,
    /// Whether a wizard hook is currently running.
    pub running: bool,
}

impl State {
    /// Reset the screen before starting a new wizard.
    pub fn begin(&mut self) {
        *self = Self {
            running: true,
            ..Self::default()
        };
    }

    /// Accept one request from the wizard thread.
    pub fn receive(&mut self, request: WizardRequest) {
        match &request {
            WizardRequest::Note { text, level } => {
                self.notes.push((*level, text.clone()));
                if self.notes.len() > 100 {
                    self.notes.remove(0);
                }
            }
            WizardRequest::Choice {
                options, default, ..
            } => {
                self.cursor = default
                    .min(options.len().saturating_sub(1))
                    .min(enabled_last(options));
                self.input.clear();
            }
            WizardRequest::Confirm { default, .. } => {
                self.cursor = usize::from(*default);
                self.input.clear();
            }
            WizardRequest::Text { default, .. } => {
                self.input = TextInput::new(default.as_deref().unwrap_or_default());
            }
            WizardRequest::Secret { .. } => self.input.clear(),
            WizardRequest::Done { .. } => self.running = false,
            WizardRequest::Section { .. } | WizardRequest::Progress { .. } => {}
        }
        self.request = Some(request);
    }

    /// Answer the current request, if it needs one.
    pub fn on_key(&mut self, key: KeyEvent) -> Option<(WizardReply, bool)> {
        let request = self.request.as_ref()?;
        match request {
            WizardRequest::Choice { options, .. } => match key.code {
                KeyCode::Esc => Some((WizardReply::Cancel, true)),
                KeyCode::Up => {
                    self.cursor = previous_enabled(options, self.cursor);
                    None
                }
                KeyCode::Down => {
                    self.cursor = next_enabled(options, self.cursor);
                    None
                }
                KeyCode::Enter => Some((WizardReply::Choice(self.cursor), false)),
                _ => None,
            },
            WizardRequest::Confirm { default, .. } => match key.code {
                KeyCode::Esc => Some((WizardReply::Cancel, true)),
                KeyCode::Left | KeyCode::Right | KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.cursor = 1;
                    None
                }
                KeyCode::Char('n') | KeyCode::Char('N') => {
                    self.cursor = 0;
                    None
                }
                KeyCode::Enter => {
                    let value = if self.cursor <= 1 {
                        self.cursor == 1
                    } else {
                        *default
                    };
                    Some((WizardReply::Confirm(value), false))
                }
                _ => None,
            },
            WizardRequest::Text { .. } => {
                if key.code == KeyCode::Esc {
                    return Some((WizardReply::Cancel, true));
                }
                if key.code == KeyCode::Enter {
                    return Some((WizardReply::Text(self.input.value().to_owned()), false));
                }
                self.input.handle_key(key);
                None
            }
            WizardRequest::Secret { .. } => {
                if key.code == KeyCode::Esc {
                    return Some((WizardReply::Cancel, true));
                }
                if key.code == KeyCode::Enter {
                    return Some((WizardReply::Secret(self.input.value().to_owned()), false));
                }
                self.input.handle_key(key);
                None
            }
            WizardRequest::Section { .. }
            | WizardRequest::Note { .. }
            | WizardRequest::Progress { .. }
            | WizardRequest::Done { .. } => {
                (key.code == KeyCode::Esc).then_some((WizardReply::Cancel, true))
            }
        }
    }
}

fn enabled_last(options: &[crate::wizard::WizardOption]) -> usize {
    options
        .iter()
        .rposition(|option| option.enabled)
        .unwrap_or(0)
}

fn previous_enabled(options: &[crate::wizard::WizardOption], cursor: usize) -> usize {
    (0..cursor)
        .rev()
        .find(|index| options[*index].enabled)
        .unwrap_or(cursor)
}

fn next_enabled(options: &[crate::wizard::WizardOption], cursor: usize) -> usize {
    ((cursor + 1)..options.len())
        .find(|index| options[*index].enabled)
        .unwrap_or(cursor)
}

/// The Setup tab.
pub struct Setup;

impl View for Setup {
    fn render(&self, app: &App, frame: &mut Frame, area: Rect) {
        let theme = app.theme;
        let inner = pane(frame, area, "─ Setup wizard ", true, theme);
        let [history, prompt] =
            Layout::vertical([Constraint::Min(3), Constraint::Length(8)]).areas(inner);

        let mut lines = Vec::with_capacity(app.setup.notes.len() + 2);
        if app.setup.notes.is_empty() {
            lines.push(Line::styled(
                "No wizard output yet. Press s or choose “setup wizard” from the palette.",
                theme.muted(),
            ));
        } else {
            lines.extend(app.setup.notes.iter().map(|(level, text)| {
                Line::from(vec![
                    Span::styled(note_marker(*level), theme.tone(note_tone(*level))),
                    Span::raw(format!(" {text}")),
                ])
            }));
        }
        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), history);

        let mut prompt_lines = Vec::new();
        match app.setup.request.as_ref() {
            Some(WizardRequest::Section { title, step, total }) => {
                prompt_lines.push(Line::styled(
                    if *step == 0 {
                        title.clone()
                    } else {
                        format!("Step {step}/{total}: {title}")
                    },
                    theme.accent(),
                ));
                prompt_lines.push(Line::styled("waiting for the next question…", theme.muted()));
            }
            Some(WizardRequest::Choice {
                prompt, options, ..
            }) => {
                prompt_lines.push(Line::styled(prompt.clone(), theme.accent()));
                for (index, option) in options.iter().enumerate() {
                    let marker = if index == app.setup.cursor { "›" } else { " " };
                    let style = if !option.enabled {
                        theme.muted()
                    } else if index == app.setup.cursor {
                        theme.selected()
                    } else {
                        theme.text()
                    };
                    prompt_lines.push(Line::styled(
                        format!("{marker} {}{}", option.label, option.detail.as_deref().map_or(String::new(), |detail| format!(" — {detail}"))),
                        style,
                    ));
                }
            }
            Some(WizardRequest::Confirm { prompt, default, .. }) => {
                prompt_lines.push(Line::styled(prompt.clone(), theme.accent()));
                let selected = if app.setup.cursor == 1 { "yes" } else { "no" };
                prompt_lines.push(Line::from(vec![
                    Span::styled("  y/yes  ", theme.tone(if selected == "yes" { Tone::Accent } else { Tone::Muted })),
                    Span::styled("n/no", theme.tone(if selected == "no" { Tone::Accent } else { Tone::Muted })),
                    Span::styled(format!("  (default: {})", if *default { "yes" } else { "no" }), theme.muted()),
                ]));
            }
            Some(WizardRequest::Text { prompt, help, .. }) => {
                prompt_lines.push(Line::styled(prompt.clone(), theme.accent()));
                prompt_lines.push(app.setup.input.line(theme, true));
                if let Some(help) = help {
                    prompt_lines.push(Line::styled(help.clone(), theme.muted()));
                }
            }
            Some(WizardRequest::Secret { prompt }) => {
                prompt_lines.push(Line::styled(prompt.clone(), theme.accent()));
                let masked = "•".repeat(app.setup.input.value().chars().count());
                prompt_lines.push(Line::from(vec![
                    Span::raw(masked),
                    Span::styled("▏", theme.accent()),
                ]));
            }
            Some(WizardRequest::Progress { label }) => {
                prompt_lines.push(Line::styled(format!("… {label}"), theme.accent()));
            }
            Some(WizardRequest::Done { ok, summary }) => {
                prompt_lines.push(Line::styled(
                    summary.clone(),
                    theme.tone(if *ok { Tone::Success } else { Tone::Error }),
                ));
                prompt_lines.push(Line::styled("press s to run setup again, or choose another view", theme.muted()));
            }
            None if app.setup.running => {
                prompt_lines.push(Line::styled("starting setup wizard…", theme.accent()));
            }
            None => {
                prompt_lines.push(Line::styled("Setup is ready.", theme.accent()));
                prompt_lines.push(Line::styled("Press s or choose “setup wizard” from the palette.", theme.muted()));
            }
        }
        frame.render_widget(Paragraph::new(prompt_lines).wrap(Wrap { trim: true }), prompt);
    }

    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        if let Some((reply, cancelled)) = app.setup.on_key(key) {
            if cancelled {
                app.setup.running = false;
            }
            return vec![Effect::WizardReply(reply)];
        }
        Vec::new()
    }

    fn hints(&self, app: &App) -> Vec<String> {
        if app.setup.running {
            ["↑↓ select", "enter answer", "esc cancel", "q quit"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        } else {
            ["s run setup", "tab/1-7 views", "? help", "q quit"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        }
    }
}

fn note_marker(level: NoteLevel) -> &'static str {
    match level {
        NoteLevel::Info => "·",
        NoteLevel::Success => "✓",
        NoteLevel::Warning => "⚠",
        NoteLevel::Error => "✗",
    }
}

fn note_tone(level: NoteLevel) -> Tone {
    match level {
        NoteLevel::Info => Tone::Muted,
        NoteLevel::Success => Tone::Success,
        NoteLevel::Warning => Tone::Warning,
        NoteLevel::Error => Tone::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::crossterm::event::{KeyEventKind, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new_with_kind(code, KeyModifiers::NONE, KeyEventKind::Press)
    }

    #[test]
    fn choice_request_selects_default_and_replies() {
        let mut state = State::default();
        state.begin();
        state.receive(WizardRequest::Choice {
            prompt: "pick".into(),
            options: vec![
                crate::wizard::WizardOption {
                    label: "first".into(),
                    detail: None,
                    enabled: true,
                },
                crate::wizard::WizardOption {
                    label: "second".into(),
                    detail: None,
                    enabled: true,
                },
            ],
            default: 1,
        });
        assert_eq!(state.cursor, 1);
        assert_eq!(state.on_key(key(KeyCode::Enter)), Some((WizardReply::Choice(1), false)));
    }

    #[test]
    fn escape_is_a_cancel_reply() {
        let mut state = State::default();
        state.begin();
        state.receive(WizardRequest::Text {
            prompt: "name".into(),
            default: None,
            help: None,
        });
        assert_eq!(state.on_key(key(KeyCode::Esc)), Some((WizardReply::Cancel, true)));
    }
}
