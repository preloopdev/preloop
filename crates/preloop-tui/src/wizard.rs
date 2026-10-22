//! Bridge between the setup wizard and the TUI.
//!
//! The wizard engine runs on a blocking thread and blocks on every reply. It
//! sends one [`WizardRequest`] at a time over a `std::sync::mpsc` channel and
//! waits for the matching [`WizardReply`]. The TUI owns the other end and
//! renders each request. [`WizardPort`] is the wizard's end; [`channel`]
//! creates both ends.

use std::sync::mpsc;

/// Severity of a [`WizardRequest::Note`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoteLevel {
    /// Information.
    Info,
    /// A step succeeded.
    Success,
    /// Something to look at.
    Warning,
    /// A step failed.
    Error,
}

/// One selectable answer in a [`WizardRequest::Choice`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WizardOption {
    /// Text shown for the option.
    pub label: String,
    /// Secondary text shown under the label.
    pub detail: Option<String>,
    /// False shows the option greyed out and unselectable.
    pub enabled: bool,
}

/// What the wizard asks the TUI to show.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WizardRequest {
    /// Start of a step, for a progress header.
    Section {
        /// Step title.
        title: String,
        /// 1-based step number.
        step: usize,
        /// Total number of steps.
        total: usize,
    },
    /// Informational text. Needs no reply.
    Note {
        /// The text.
        text: String,
        /// Severity.
        level: NoteLevel,
    },
    /// Pick one option. Replies with [`WizardReply::Choice`].
    Choice {
        /// Question shown above the options.
        prompt: String,
        /// The options.
        options: Vec<WizardOption>,
        /// Index selected at first.
        default: usize,
    },
    /// Yes or no. Replies with [`WizardReply::Confirm`].
    Confirm {
        /// Question.
        prompt: String,
        /// Answer when the user just presses enter.
        default: bool,
    },
    /// Free text. Replies with [`WizardReply::Text`].
    Text {
        /// Question.
        prompt: String,
        /// Pre-filled value.
        default: Option<String>,
        /// Extra help shown under the prompt.
        help: Option<String>,
    },
    /// A hidden value. Replies with [`WizardReply::Secret`].
    Secret {
        /// Question.
        prompt: String,
    },
    /// Indeterminate progress. The next request ends it.
    Progress {
        /// What is happening.
        label: String,
    },
    /// The wizard is finished. No reply.
    Done {
        /// Whether the wizard succeeded.
        ok: bool,
        /// Summary text.
        summary: String,
    },
}

/// The user's answer to a [`WizardRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WizardReply {
    /// Index of the chosen option.
    Choice(usize),
    /// Yes or no.
    Confirm(bool),
    /// Entered text.
    Text(String),
    /// Entered secret.
    Secret(String),
    /// The user cancelled. The wizard returns an error.
    Cancel,
}

/// The wizard's end of the channel: send requests, then block on replies.
#[derive(Debug)]
pub struct WizardPort {
    /// Requests to the TUI.
    pub requests: mpsc::Sender<WizardRequest>,
    /// Replies from the TUI.
    pub replies: mpsc::Receiver<WizardReply>,
}

/// The TUI's end of the channel: receive requests, send replies.
#[derive(Debug)]
pub struct WizardUi {
    /// Requests from the wizard.
    pub requests: mpsc::Receiver<WizardRequest>,
    /// Replies to the wizard.
    pub replies: mpsc::Sender<WizardReply>,
}

/// Create a connected pair: the wizard's port and the TUI's end.
pub fn channel() -> (WizardPort, WizardUi) {
    let (request_tx, request_rx) = mpsc::channel();
    let (reply_tx, reply_rx) = mpsc::channel();
    (
        WizardPort {
            requests: request_tx,
            replies: reply_rx,
        },
        WizardUi {
            requests: request_rx,
            replies: reply_tx,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_reply_round_trip_across_threads() {
        let (port, ui) = channel();
        let wizard = std::thread::spawn(move || {
            port.requests
                .send(WizardRequest::Confirm {
                    prompt: "continue?".into(),
                    default: true,
                })
                .expect("ui is listening");
            port.replies.recv().expect("ui replies")
        });
        let request = ui.requests.recv().expect("wizard asked");
        assert!(matches!(request, WizardRequest::Confirm { default: true, .. }));
        ui.replies.send(WizardReply::Confirm(false)).expect("wizard waits");
        assert_eq!(wizard.join().expect("no panic"), WizardReply::Confirm(false));
    }
}
