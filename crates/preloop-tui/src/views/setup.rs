//! Setup screen: Setup wizard screen. Build it in the follow-up slice with the wizard bridge.

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Setup tab. Stub: see the module docs.
pub struct Setup;

impl View for Setup {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Setup");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
