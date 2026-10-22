//! Views: one per tab. Each implements [`View`].
//!
//! Template for a new view:
//!
//! 1. Add a `State` struct for the view's own selection and filter state, and
//!    a field for it on [`App`](crate::app::App).
//! 2. Put the data it needs in [`Store`](crate::store::Store) as a
//!    `Loadable<T>`. Add a [`Fetch`](crate::app::Fetch) variant, a
//!    [`Fetched`](crate::app::Fetched) variant, and the request in
//!    `App::refresh_all`.
//! 3. Implement [`View`]: `render` reads `&App` and draws, `on_key` changes the
//!    view's state and returns effects, `hints` lists the footer keys.
//! 4. Snapshot it in `snapshot_tests.rs` with fixture data.

use ratatui::Frame;
use ratatui::crossterm::event::KeyEvent;
use ratatui::layout::Rect;

use crate::app::{App, Effect};

pub mod debug;
pub mod fleet;
pub mod overview;
pub mod runs;
pub mod sandboxes;
pub mod setup;
pub mod stack;
pub mod webhooks;

/// The screens. The first seven are tabs; `Setup` opens from the wizard keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewKind {
    /// The overview: queue, pool, fleet, conditions and recent runs.
    Overview,
    /// Runs, jobs, steps and logs.
    Runs,
    /// Runners and pool bindings.
    Fleet,
    /// Debug sessions.
    Debug,
    /// Webhook queue and deliveries.
    Webhooks,
    /// Sandbox providers, pools and sandboxes.
    Sandboxes,
    /// The stacked pull request view.
    Stack,
    /// The setup wizard.
    Setup,
}

impl ViewKind {
    /// The tabs, in number order.
    pub const TABS: [ViewKind; 7] = [
        ViewKind::Overview,
        ViewKind::Runs,
        ViewKind::Fleet,
        ViewKind::Debug,
        ViewKind::Webhooks,
        ViewKind::Sandboxes,
        ViewKind::Stack,
    ];

    /// Tab label, without the number.
    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Runs => "Runs",
            Self::Fleet => "Fleet",
            Self::Debug => "Debug",
            Self::Webhooks => "Webhooks",
            Self::Sandboxes => "Sandboxes",
            Self::Stack => "Stack",
            Self::Setup => "Setup",
        }
    }

    /// The tab number (1-7), or None for views that are not tabs.
    pub fn number(self) -> Option<usize> {
        Self::TABS
            .iter()
            .position(|tab| *tab == self)
            .map(|index| index + 1)
    }

    /// The tab for a number 1-7.
    pub fn from_number(number: usize) -> Option<Self> {
        number
            .checked_sub(1)
            .and_then(|index| Self::TABS.get(index).copied())
    }

    /// The next tab, wrapping. Non-tabs go to the first tab.
    pub fn next_tab(self) -> Self {
        match self.number() {
            Some(number) => Self::TABS[number % Self::TABS.len()],
            None => Self::TABS[0],
        }
    }

    /// The previous tab, wrapping. Non-tabs go to the last tab.
    pub fn previous_tab(self) -> Self {
        match self.number() {
            Some(number) => Self::TABS[(number + Self::TABS.len() - 2) % Self::TABS.len()],
            None => Self::TABS[Self::TABS.len() - 1],
        }
    }

    /// The view that implements this screen.
    pub fn view(self) -> &'static dyn View {
        match self {
            Self::Overview => &overview::Overview,
            Self::Runs => &runs::Runs,
            Self::Fleet => &fleet::Fleet,
            Self::Debug => &debug::Debug,
            Self::Webhooks => &webhooks::Webhooks,
            Self::Sandboxes => &sandboxes::Sandboxes,
            Self::Stack => &stack::Stack,
            Self::Setup => &setup::Setup,
        }
    }
}

/// One screen. Views hold no state; their state lives on [`App`].
pub trait View: Sync {
    /// Draw the screen into `area`. Must not change state.
    fn render(&self, app: &App, frame: &mut Frame, area: Rect);

    /// Handle a key the global keys did not take. Returns effects.
    fn on_key(&self, app: &mut App, key: KeyEvent) -> Vec<Effect> {
        let _ = (app, key);
        Vec::new()
    }

    /// Footer hints for this screen, in display order.
    fn hints(&self, app: &App) -> Vec<String>;

    /// Called when the screen becomes active. Returns effects to load it.
    fn on_show(&self, app: &mut App) -> Vec<Effect> {
        let _ = app;
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tab_numbers_round_trip() {
        for (index, view) in ViewKind::TABS.iter().enumerate() {
            assert_eq!(view.number(), Some(index + 1));
            assert_eq!(ViewKind::from_number(index + 1), Some(*view));
        }
        assert_eq!(ViewKind::from_number(0), None);
        assert_eq!(ViewKind::from_number(8), None);
        assert_eq!(ViewKind::Setup.number(), None);
    }

    #[test]
    fn tab_cycling_wraps_both_ways() {
        assert_eq!(ViewKind::Stack.next_tab(), ViewKind::Overview);
        assert_eq!(ViewKind::Overview.previous_tab(), ViewKind::Stack);
        assert_eq!(ViewKind::Runs.next_tab(), ViewKind::Fleet);
    }
}
