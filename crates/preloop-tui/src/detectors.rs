//! Detectors: conditions the engine does not raise itself.
//!
//! Each detector turns data the TUI already holds into a [`Finding`]. The
//! overview lists them next to the engine's own conditions, with the same
//! jump links. Detectors are pure functions of their inputs, so tests can
//! supply times directly.

use std::collections::BTreeSet;

use chrono::{DateTime, TimeDelta, Utc};

use crate::api::{DebugSession, OperationalSnapshot};
use crate::client::sandbox::SandboxSummary;

/// How long the leaked-binding condition must hold before it is reported.
pub const LEAKED_BINDING_WINDOW: TimeDelta = TimeDelta::seconds(60);

/// How long runs may arrive without any webhook delivery before ingress is suspect.
pub const INGRESS_GAP: TimeDelta = TimeDelta::seconds(300);

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Needs attention.
    Warning,
    /// Informational; the thing is working.
    Info,
}

/// One thing a detector noticed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// Stable code, such as `leaked_binding`.
    pub code: &'static str,
    /// Severity.
    pub severity: Severity,
    /// Human-readable detail.
    pub message: String,
}

/// Tracks how long the leaked-binding condition has held.
///
/// The condition is: jobs are ready to run, a runner is idle, and no new
/// assignment has appeared. Each observation either extends the window or
/// restarts it, so [`LeakedBindingTracker::finding`] can report how long the
/// condition has held without progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeakedBindingTracker {
    since: Option<DateTime<Utc>>,
    known: BTreeSet<String>,
    ready: u32,
    idle: u32,
}

impl LeakedBindingTracker {
    /// Record one status snapshot, taken at `at`.
    pub fn observe(&mut self, at: DateTime<Utc>, status: &OperationalSnapshot) {
        let current: BTreeSet<String> = status
            .runners
            .assignments
            .iter()
            .map(|assignment| {
                format!(
                    "{}/{}/{}",
                    assignment.run_id, assignment.job_id, assignment.runner_id
                )
            })
            .collect();
        let new_assignment = current.difference(&self.known).next().is_some();
        let ready = status.jobs.ready + status.jobs.claimable;
        let holding = ready > 0 && status.runners.idle > 0;

        if !holding {
            self.since = None;
        } else if self.since.is_none() || new_assignment {
            self.since = Some(at);
        }
        self.known = current;
        self.ready = status.jobs.ready;
        self.idle = status.runners.idle;
    }

    /// The finding, when the condition has held for [`LEAKED_BINDING_WINDOW`].
    pub fn finding(&self, now: DateTime<Utc>) -> Option<Finding> {
        let since = self.since?;
        let held = now - since;
        if held < LEAKED_BINDING_WINDOW {
            return None;
        }
        Some(Finding {
            code: "leaked_binding",
            severity: Severity::Warning,
            message: format!(
                "detector: {} ready ∧ {} idle ∧ no assignment {}",
                self.ready,
                self.idle,
                fmt_precise(held)
            ),
        })
    }
}

/// The first paused debug session, as a finding.
pub fn debug_session_open(sessions: &[DebugSession]) -> Option<Finding> {
    let paused = sessions.iter().find(|session| session.state == "paused")?;
    Some(Finding {
        code: "debug_session_open",
        severity: Severity::Warning,
        message: format!(
            "run {} · {} paused {}",
            short(&paused.run_id),
            paused.job_name,
            fmt_age(TimeDelta::seconds(paused.paused_seconds as i64))
        ),
    })
}

/// One finding per cordoned sandbox provider.
pub fn cordoned_providers(summary: &SandboxSummary) -> Vec<Finding> {
    summary
        .providers
        .iter()
        .filter(|provider| provider.cordoned)
        .map(|provider| Finding {
            code: "sbx_provider_cordoned",
            severity: Severity::Warning,
            message: format!("{} cordoned", provider.name),
        })
        .collect()
}

/// Ingress health: deliveries should keep up with runs.
///
/// A gap is flagged when the newest run is more than [`INGRESS_GAP`] younger
/// than the newest delivery, or when there has never been a delivery.
pub fn ingress(
    now: DateTime<Utc>,
    last_delivery: Option<DateTime<Utc>>,
    last_run: Option<DateTime<Utc>>,
) -> Finding {
    match (last_delivery, last_run) {
        (None, _) => Finding {
            code: "ingress",
            severity: Severity::Warning,
            message: "no webhook deliveries received yet".to_owned(),
        },
        (Some(delivery), Some(run)) if run - delivery > INGRESS_GAP => Finding {
            code: "ingress",
            severity: Severity::Warning,
            message: format!(
                "runs arriving without deliveries: last delivery {} ago",
                fmt_age(now - delivery)
            ),
        },
        (Some(delivery), run) => {
            let run_text = run
                .map(|run| format!(" · last run {} ago", fmt_age(now - run)))
                .unwrap_or_default();
            Finding {
                code: "ingress",
                severity: Severity::Info,
                message: format!("last delivery {} ago{run_text}", fmt_age(now - delivery)),
            }
        }
    }
}

/// Short run id for messages.
pub fn short(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

/// Compact age: `42s`, `6m`, `3h12m`, `1h`, `3d04h`.
pub fn fmt_age(duration: TimeDelta) -> String {
    let secs = duration.num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        let (hours, minutes) = (secs / 3600, (secs % 3600) / 60);
        if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h{minutes:02}m")
        }
    } else {
        let (days, hours) = (secs / 86_400, (secs % 86_400) / 3600);
        format!("{days}d{hours:02}h")
    }
}

/// Step-style duration: `0s`, `38s`, `3m02`, `1h02m`.
pub fn fmt_clock(duration: TimeDelta) -> String {
    let secs = duration.num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}", secs / 60, secs % 60)
    } else {
        format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Precise duration: `42s`, `4m12s`, `1h02m05s`.
pub fn fmt_precise(duration: TimeDelta) -> String {
    let secs = duration.num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{:02}s", secs / 60, secs % 60)
    } else {
        format!(
            "{}h{:02}m{:02}s",
            secs / 3600,
            (secs % 3600) / 60,
            secs % 60
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::api::status::{RunnerAssignment, RunnerCounts};

    fn at(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 22, hour, minute, second)
            .single()
            .expect("valid time")
    }

    fn snapshot(ready: u32, idle: u32, assignments: Vec<RunnerAssignment>) -> OperationalSnapshot {
        let mut status = OperationalSnapshot::default();
        status.jobs.ready = ready;
        status.runners = RunnerCounts {
            idle,
            assignments,
            ..Default::default()
        };
        status
    }

    #[test]
    fn leaked_binding_fires_after_the_window_without_assignments() {
        let mut tracker = LeakedBindingTracker::default();
        tracker.observe(at(12, 15, 48), &snapshot(2, 3, Vec::new()));
        tracker.observe(at(12, 20, 0), &snapshot(2, 3, Vec::new()));
        let finding = tracker.finding(at(12, 20, 0)).expect("held 4m12s");
        assert_eq!(finding.code, "leaked_binding");
        assert_eq!(
            finding.message,
            "detector: 2 ready ∧ 3 idle ∧ no assignment 4m12s"
        );
    }

    #[test]
    fn a_new_assignment_restarts_the_window() {
        let mut tracker = LeakedBindingTracker::default();
        tracker.observe(at(12, 0, 0), &snapshot(2, 3, Vec::new()));
        let bound = vec![RunnerAssignment {
            runner_id: 4,
            run_id: "r".into(),
            job_id: "j".into(),
            assigned_seconds_ago: 0.0,
        }];
        tracker.observe(at(12, 5, 0), &snapshot(1, 2, bound));
        assert_eq!(tracker.finding(at(12, 5, 30)), None);
    }

    #[test]
    fn no_finding_when_nothing_is_ready() {
        let mut tracker = LeakedBindingTracker::default();
        tracker.observe(at(12, 0, 0), &snapshot(0, 3, Vec::new()));
        assert_eq!(tracker.finding(at(13, 0, 0)), None);
    }

    #[test]
    fn ingress_reports_a_gap_when_runs_outpace_deliveries() {
        let now = at(12, 20, 0);
        let finding = ingress(now, Some(at(12, 0, 0)), Some(at(12, 10, 0)));
        assert_eq!(finding.severity, Severity::Warning);
        assert!(finding.message.starts_with("runs arriving without deliveries"));
        let healthy = ingress(now, Some(at(12, 19, 42)), Some(at(12, 18, 0)));
        assert_eq!(healthy.severity, Severity::Info);
        assert_eq!(healthy.message, "last delivery 18s ago · last run 2m ago");
    }

    #[test]
    fn age_formats_match_the_mockup_style() {
        assert_eq!(fmt_age(TimeDelta::seconds(42)), "42s");
        assert_eq!(fmt_age(TimeDelta::seconds(6 * 60)), "6m");
        assert_eq!(fmt_age(TimeDelta::seconds(3 * 3600 + 12 * 60)), "3h12m");
        assert_eq!(fmt_age(TimeDelta::seconds(3600)), "1h");
        assert_eq!(fmt_age(TimeDelta::seconds(273_600)), "3d04h");
        assert_eq!(fmt_clock(TimeDelta::seconds(182)), "3m02");
        assert_eq!(fmt_clock(TimeDelta::seconds(0)), "0s");
    }
}
