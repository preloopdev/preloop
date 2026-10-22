//! Stack screen: Stacked pull requests. Build it in the follow-up slice.

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Stack tab. Stub: see the module docs.
pub struct Stack;

impl View for Stack {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Stack");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
