//! Fleet screen: Runners and pool bindings. Build it in the follow-up slice (see views/mod.rs template).

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Fleet tab. Stub: see the module docs.
pub struct Fleet;

impl View for Fleet {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Fleet");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
