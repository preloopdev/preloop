//! Webhooks screen: Webhook queue, deliveries and replay. Build it in the follow-up slice.

use ratatui::Frame;
use ratatui::layout::Rect;

use super::{View, placeholder};
use crate::app::App;

/// The Webhooks tab. Stub: see the module docs.
pub struct Webhooks;

impl View for Webhooks {
    fn render(&self, _app: &App, frame: &mut Frame, area: Rect) {
        placeholder(frame, area, "Webhooks");
    }

    fn hints(&self, _app: &App) -> Vec<String> {
        ["tab/1-7 views", "? help", "q quit"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}
