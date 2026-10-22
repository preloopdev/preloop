//! Sandboxes screen: Sandbox providers, pools and sandboxes. Build it in the follow-up slice.

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Sandboxes tab. Stub: see the module docs.
pub struct Sandboxes;

impl View for Sandboxes {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Sandboxes");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
