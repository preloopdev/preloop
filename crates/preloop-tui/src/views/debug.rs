//! Debug screen: Paused debug sessions and attach. Build it in the follow-up slice.

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Debug tab. Stub: see the module docs.
pub struct Debug;

impl View for Debug {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Debug");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
