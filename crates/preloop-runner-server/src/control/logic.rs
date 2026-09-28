//! Backend-neutral scheduling decisions.
//!
//! Pure functions over small row structs: no I/O, no locks, no working set.
//! A backend loads the few rows a decision needs, calls the function here,
//! and applies the result with conditional statements. Both backends call
//! the same functions, so their scheduling semantics cannot diverge.

use std::time::{Duration, SystemTime};
/// Namespace used to deterministically encode legacy non-UUID session ids in
/// runner_sessions UUID columns.
pub(crate) const SESSION_ID_NAMESPACE: uuid::Uuid =
    uuid::uuid!("7d3c0a1c-6f4a-4d4c-a5d4-8d27b5a3c1f0");

/// Return `id` as a UUID, or a deterministic UUID v5 for legacy ids such as
/// `"default"` and `"s1"`.
pub(crate) fn session_uuid(id: &str) -> uuid::Uuid {
    id.parse()
        .unwrap_or_else(|_| uuid::Uuid::new_v5(&SESSION_ID_NAMESPACE, id.as_bytes()))
}

/// How long an unmatched ready job may wait for a matching runner before the
/// reaper fails it (outside a pool warm-up).
pub(crate) const QUEUED_JOB_GRACE: Duration = Duration::from_secs(120);

/// Absolute ceiling, measured from ready-enqueue, on how long a preparing or
/// provisioning pool may protect an unmatched job from starvation.
pub(crate) const MAX_QUEUED_GRACE: Duration = Duration::from_secs(600);

/// A ready job as the starvation sweep sees it.
#[derive(Debug, Clone)]
pub(crate) struct StarvationCandidate<'a> {
    /// The job's `runs-on` labels.
    pub(crate) runs_on: &'a [String],
    /// Ready-enqueue time (`jobs.enqueued_at`); epoch when unknown.
    pub(crate) enqueued_at: SystemTime,
    /// The reaper's first-seen mark for this job, if one is stored.
    pub(crate) first_seen: Option<SystemTime>,
    /// Some registered runner's labels match `runs_on`
    /// ([`crate::runtime_scheduling::job_matches_runner`]).
    pub(crate) any_runner_matches: bool,
}

/// What the reaper does with one ready job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StarvationVerdict {
    /// Drop the first-seen mark (a runner can take the job, or it is
    /// protected for now).
    ClearMark,
    /// Keep the job waiting; store `first_seen` as its mark when it has none
    /// (an existing mark is kept unchanged).
    Mark { first_seen: SystemTime },
    /// Fail the job with this user-facing reason and drop its mark.
    Starve { reason: String, grace: Duration },
}

/// Decide one ready job's fate in a reaper tick.
///
/// - A job some runner matches clears its mark.
/// - A job that needs an external host (`macos*`/`windows*` label) is never
///   starved here: the host may register later; its mark clears.
/// - While a pool is preparing (`pool_preparing`): within this process's
///   warm window (`warm_window_open`) or while the job is younger than
///   [`MAX_QUEUED_GRACE`] since enqueue, the mark clears; past that the job
///   starves with grace [`MAX_QUEUED_GRACE`].
/// - Otherwise the first-seen clock is the stored mark, else the enqueue
///   time (so a restart grants no fresh window): younger than
///   [`QUEUED_JOB_GRACE`] keeps waiting (`Mark`), else it starves with grace
///   [`QUEUED_JOB_GRACE`].
pub(crate) fn starvation_verdict(
    job: &StarvationCandidate<'_>,
    now: SystemTime,
    pool_preparing: bool,
    warm_window_open: bool,
) -> StarvationVerdict {
    if job.any_runner_matches {
        return StarvationVerdict::ClearMark;
    }
    let needs_external_host = job.runs_on.iter().any(|label| {
        let label = label.to_ascii_lowercase();
        label.starts_with("macos") || label.starts_with("windows")
    });
    if needs_external_host {
        return StarvationVerdict::ClearMark;
    }
    let grace = if pool_preparing {
        let enqueue_age_expired = now
            .duration_since(job.enqueued_at)
            .map(|age| age >= MAX_QUEUED_GRACE)
            .unwrap_or(true);
        if warm_window_open || !enqueue_age_expired {
            return StarvationVerdict::ClearMark;
        }
        MAX_QUEUED_GRACE
    } else {
        let first_seen = job.first_seen.unwrap_or(job.enqueued_at);
        if now
            .duration_since(first_seen)
            .map(|age| age < QUEUED_JOB_GRACE)
            .unwrap_or(false)
        {
            return StarvationVerdict::Mark { first_seen };
        }
        QUEUED_JOB_GRACE
    };
    StarvationVerdict::Starve {
        reason: format!(
            "no runner is registered for `runs-on: {}` and none appeared \
             within {}s, so the job cannot be scheduled",
            job.runs_on.join(", "),
            grace.as_secs()
        ),
        grace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }

    fn candidate<'a>(
        runs_on: &'a [String],
        enqueued_ago: Duration,
        first_seen_ago: Option<Duration>,
        now: SystemTime,
    ) -> StarvationCandidate<'a> {
        StarvationCandidate {
            runs_on,
            enqueued_at: now - enqueued_ago,
            first_seen: first_seen_ago.map(|ago| now - ago),
            any_runner_matches: false,
        }
    }

    #[test]
    fn matched_or_external_host_jobs_never_starve() {
        let now = SystemTime::now();
        let linux = labels(&["ubuntu-latest"]);
        let mut job = candidate(&linux, Duration::from_secs(10_000), None, now);
        job.any_runner_matches = true;
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::ClearMark
        );
        let mac = labels(&["macOS-14"]);
        let job = candidate(&mac, Duration::from_secs(10_000), None, now);
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::ClearMark
        );
    }

    #[test]
    fn grace_counts_from_first_seen_else_enqueue() {
        let now = SystemTime::now();
        let linux = labels(&["ubuntu-latest"]);
        // Enqueued long ago but first seen recently: keep the existing mark.
        let job = candidate(
            &linux,
            Duration::from_secs(1_000),
            Some(Duration::from_secs(30)),
            now,
        );
        assert_eq!(
            starvation_verdict(&job, now, false, false),
            StarvationVerdict::Mark {
                first_seen: now - Duration::from_secs(30)
            }
        );
        // No mark: the enqueue time seeds the clock, so a restart grants no
        // fresh window.
        let job = candidate(&linux, Duration::from_secs(121), None, now);
        let StarvationVerdict::Starve { reason, grace } =
            starvation_verdict(&job, now, false, false)
        else {
            panic!("an unmatched job past the grace window must starve");
        };
        assert_eq!(grace, QUEUED_JOB_GRACE);
        assert_eq!(
            reason,
            "no runner is registered for `runs-on: ubuntu-latest` and none appeared within \
             120s, so the job cannot be scheduled"
        );
    }

    #[test]
    fn preparing_pool_protects_until_the_enqueue_ceiling() {
        let now = SystemTime::now();
        let linux = labels(&["self-hosted", "linux"]);
        let young = candidate(&linux, Duration::from_secs(599), None, now);
        assert_eq!(
            starvation_verdict(&young, now, true, false),
            StarvationVerdict::ClearMark
        );
        let old = candidate(&linux, Duration::from_secs(600), None, now);
        assert!(matches!(
            starvation_verdict(&old, now, true, false),
            StarvationVerdict::Starve { grace, .. } if grace == MAX_QUEUED_GRACE
        ));
        // The process warm window protects even past the ceiling.
        assert_eq!(
            starvation_verdict(&old, now, true, true),
            StarvationVerdict::ClearMark
        );
    }
}
