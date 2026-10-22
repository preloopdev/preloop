//! Actions the TUI can perform, and how much confirmation each one needs.
//!
//! Every key, palette entry and modal refers to an [`Action`], so the
//! confirmation rules live in one place. `--read-only` refuses every action
//! whose [`Action::mutates`] is true.

/// How much the user must confirm before an action runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Danger {
    /// Runs immediately.
    None,
    /// One keypress to confirm in a modal.
    Confirm,
    /// The user must type this id to confirm.
    TypeToConfirm(String),
}

/// Something the user can ask the TUI to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Cancel a run.
    CancelRun {
        /// Run id.
        run_id: String,
    },
    /// Re-run every job of a run.
    RerunRun {
        /// Run id.
        run_id: String,
    },
    /// Approve a job waiting for approval.
    ApproveJob {
        /// Run id.
        run_id: String,
        /// Job id.
        job_id: String,
    },
    /// Release a fork run held for approval.
    ApproveFork {
        /// Run id.
        run_id: String,
    },
    /// Remove a runner from the engine.
    PurgeRunner {
        /// Runner name.
        name: String,
    },
    /// Re-queue a webhook delivery.
    ReplayDelivery {
        /// Delivery id.
        delivery_id: String,
    },
    /// Retry the failed step in a debug session.
    DebugRetry {
        /// Session id.
        session_id: String,
    },
    /// Retry a debug session from a zero-based step index.
    DebugRetryFrom {
        /// Session id.
        session_id: String,
        /// Step index.
        step_index: usize,
    },
    /// Continue a paused debug session.
    DebugContinue {
        /// Session id.
        session_id: String,
    },
    /// Abort a paused debug session.
    DebugAbort {
        /// Session id.
        session_id: String,
    },
    /// Attach to a paused debug session (`preloop debug`), suspending the TUI.
    AttachDebug {
        /// Session id.
        session_id: String,
    },
    /// Open a URL in the browser.
    OpenUrl {
        /// The URL.
        url: String,
    },
    /// Start the engine detached (`preloop serve`).
    StartEngine,
    /// Run the setup wizard.
    RunWizard,
}

impl Action {
    /// Confirmation this action needs.
    pub fn danger(&self) -> Danger {
        match self {
            Self::CancelRun { .. }
            | Self::ApproveJob { .. }
            | Self::ApproveFork { .. }
            | Self::ReplayDelivery { .. } => Danger::Confirm,
            Self::PurgeRunner { name } => Danger::TypeToConfirm(name.clone()),
            Self::RerunRun { .. }
            | Self::DebugRetry { .. }
            | Self::DebugRetryFrom { .. }
            | Self::DebugContinue { .. }
            | Self::DebugAbort { .. }
            | Self::AttachDebug { .. }
            | Self::OpenUrl { .. }
            | Self::StartEngine
            | Self::RunWizard => Danger::None,
        }
    }

    /// Whether the action changes state, and so is refused in `--read-only`.
    pub fn mutates(&self) -> bool {
        match self {
            Self::CancelRun { .. }
            | Self::RerunRun { .. }
            | Self::ApproveJob { .. }
            | Self::ApproveFork { .. }
            | Self::PurgeRunner { .. }
            | Self::ReplayDelivery { .. }
            | Self::DebugRetry { .. }
            | Self::DebugRetryFrom { .. }
            | Self::DebugContinue { .. }
            | Self::DebugAbort { .. }
            | Self::AttachDebug { .. }
            | Self::StartEngine
            | Self::RunWizard => true,
            Self::OpenUrl { .. } => false,
        }
    }

    /// Sentence shown in the palette and the confirmation modal.
    pub fn label(&self) -> String {
        match self {
            Self::CancelRun { run_id } => format!("cancel run {}", short(run_id)),
            Self::RerunRun { run_id } => format!("rerun {}", short(run_id)),
            Self::ApproveJob { run_id, job_id } => {
                format!("approve job {job_id} in {}", short(run_id))
            }
            Self::ApproveFork { run_id } => format!("approve fork run {}", short(run_id)),
            Self::PurgeRunner { name } => format!("purge runner {name}"),
            Self::ReplayDelivery { delivery_id } => {
                format!("replay delivery {}", short(delivery_id))
            }
            Self::DebugRetry { session_id }
            | Self::DebugContinue { session_id }
            | Self::DebugAbort { session_id } => format!("debug action on session {session_id}"),
            Self::DebugRetryFrom {
                session_id,
                step_index,
            } => {
                format!("retry session {session_id} from step {step_index}")
            }
            Self::AttachDebug { session_id } => format!("attach debug session {session_id}"),
            Self::OpenUrl { url } => format!("open {url}"),
            Self::StartEngine => "start engine".to_owned(),
            Self::RunWizard => "setup wizard".to_owned(),
        }
    }
}

fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destructive_actions_require_confirmation() {
        assert_eq!(
            Action::CancelRun { run_id: "r".into() }.danger(),
            Danger::Confirm
        );
        assert_eq!(
            Action::PurgeRunner {
                name: "plr-4".into()
            }
            .danger(),
            Danger::TypeToConfirm("plr-4".to_owned())
        );
        assert_eq!(
            Action::RerunRun { run_id: "r".into() }.danger(),
            Danger::None,
            "rerun is not destructive and needs no modal"
        );
    }

    #[test]
    fn read_only_refuses_mutations_only() {
        assert!(Action::ApproveFork { run_id: "r".into() }.mutates());
        assert!(
            !Action::OpenUrl {
                url: "https://x".into()
            }
            .mutates()
        );
    }

    #[test]
    fn labels_shorten_run_ids() {
        let label = Action::CancelRun {
            run_id: "8f3a91c2-0000-4000-8000-000000000000".into(),
        }
        .label();
        assert_eq!(label, "cancel run 8f3a91c2");
    }
}
