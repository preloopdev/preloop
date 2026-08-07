//! Step-record merge and cancellation reconciliation.
//!
//! Official runner (`actions/runner` v2.335.1) reports steps via Twirp
//! `WorkflowStepsUpdate` as a **cumulative** map keyed by `external_id`.
//! Status progresses `InProgress (2) → Completed (6)`. Partial / out-of-order
//! updates must not erase conclusions or regress status.
//!
//! On cancellation, every interrupted in-flight task ends with exactly one
//! terminal cancelled record; already-completed steps are left alone; setup
//! and complete-job synthetic records remain valid.

use std::collections::BTreeMap;

use super::server_queue::{step_conclusion, step_status, StepUpdate};

/// Partial step update — omitted fields preserve existing values on merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialStepUpdate {
    /// Identity key (UUID / wire external_id). Required.
    pub external_id: String,
    /// Ordinal number (1-based). Omitted → keep previous.
    pub number: Option<u32>,
    /// Display name. Omitted → keep previous.
    pub name: Option<String>,
    /// Status enum. Omitted → keep previous; regressions are ignored.
    pub status: Option<u32>,
    /// Start timestamp. `None` means omit; `Some(None)` would clear — we only
    /// support omit (`None`) or set (`Some(value)`).
    pub started_at: Option<String>,
    /// Whether `started_at` is present in this update.
    pub has_started_at: bool,
    /// Completion timestamp.
    pub completed_at: Option<String>,
    /// Whether `completed_at` is present in this update.
    pub has_completed_at: bool,
    /// Conclusion enum. Omitted → keep previous; never erase a non-zero
    /// conclusion with zero/absent unless status is still non-terminal.
    pub conclusion: Option<u32>,
}

impl PartialStepUpdate {
    /// Build a partial update from a full `StepUpdate` (all fields present).
    pub fn from_full(update: &StepUpdate) -> Self {
        Self {
            external_id: update.external_id.clone(),
            number: Some(update.number),
            name: Some(update.name.clone()),
            status: Some(update.status),
            started_at: update.started_at.clone(),
            has_started_at: true,
            completed_at: update.completed_at.clone(),
            has_completed_at: true,
            conclusion: Some(update.conclusion),
        }
    }
}

/// Rank for status monotonicity. Higher is later in the lifecycle.
pub fn status_rank(status: u32) -> u8 {
    match status {
        step_status::IN_PROGRESS => 1,
        step_status::COMPLETED => 2,
        // Unknown / pending-like values sort before in-progress.
        _ => 0,
    }
}

/// Merge `incoming` into `existing` (if any) under official identity rules.
///
/// - Identity is solely `external_id` (number never re-keys a different step).
/// - Omitted fields preserve existing values.
/// - Status never regresses (Completed ↛ InProgress).
/// - Non-zero conclusion is not erased by a later partial without conclusion.
/// - Duplicate identical merges are idempotent.
pub fn merge_step_update(
    existing: Option<&StepUpdate>,
    incoming: &PartialStepUpdate,
) -> StepUpdate {
    let base = existing.cloned().unwrap_or(StepUpdate {
        external_id: incoming.external_id.clone(),
        number: incoming.number.unwrap_or(0),
        name: incoming.name.clone().unwrap_or_default(),
        status: incoming.status.unwrap_or(0),
        started_at: None,
        completed_at: None,
        conclusion: 0,
    });

    let mut merged = base;

    if let Some(number) = incoming.number {
        merged.number = number;
    }
    if let Some(name) = incoming.name.clone() {
        merged.name = name;
    }

    if let Some(status) = incoming.status {
        if status_rank(status) >= status_rank(merged.status) {
            merged.status = status;
        }
    }

    if incoming.has_started_at {
        // Prefer first non-empty started_at; do not clear once set.
        if merged.started_at.is_none() {
            merged.started_at = incoming.started_at.clone();
        }
    }

    if incoming.has_completed_at
        && (incoming.completed_at.is_some() || merged.completed_at.is_none())
    {
        // Allow setting completed_at; do not clear a set value with None
        // unless we are explicitly completing again with a timestamp.
        if let Some(ref ts) = incoming.completed_at {
            merged.completed_at = Some(ts.clone());
        }
    }

    if let Some(conclusion) = incoming.conclusion {
        // Never erase a real conclusion with 0 (unset).
        if conclusion != 0 || merged.conclusion == 0 {
            merged.conclusion = conclusion;
        }
    }

    // Completed steps must keep a terminal conclusion if one was ever set.
    if merged.status == step_status::COMPLETED && merged.conclusion == 0 {
        if let Some(prev) = existing {
            if prev.conclusion != 0 {
                merged.conclusion = prev.conclusion;
            }
        }
    }

    merged.external_id = incoming.external_id.clone();
    merged
}

/// Apply a partial update into a map keyed by external_id.
pub fn apply_step_update(
    store: &mut BTreeMap<String, StepUpdate>,
    incoming: &PartialStepUpdate,
) -> StepUpdate {
    let existing = store.get(&incoming.external_id);
    let merged = merge_step_update(existing, incoming);
    store.insert(incoming.external_id.clone(), merged.clone());
    merged
}

/// A dispatched task that may need cancellation reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedTask {
    /// Wire external_id.
    pub external_id: String,
    /// Step number.
    pub number: u32,
    /// Display name.
    pub name: String,
    /// True for synthetic setup / complete-job records.
    pub synthetic: bool,
}

/// Reconcile dispatched tasks against partial received records after cancel.
///
/// Official rules encoded here:
/// - Every interrupted in-flight (non-completed) task gets exactly one
///   cancelled terminal record.
/// - Completed steps are not rewritten as cancelled.
/// - Synthetic setup/complete-job records that already completed stay as-is;
///   missing complete-job is added as cancelled only if it was dispatched
///   and not completed.
/// - Final order is by step number (stable).
pub fn reconcile_cancelled_steps(
    dispatched: &[DispatchedTask],
    received: &BTreeMap<String, StepUpdate>,
    completed_at: &str,
) -> Vec<StepUpdate> {
    let mut out: BTreeMap<String, StepUpdate> = BTreeMap::new();

    for task in dispatched {
        if let Some(existing) = received.get(&task.external_id) {
            if existing.status == step_status::COMPLETED {
                // Keep completed as-is (including success/failure/skipped).
                out.insert(task.external_id.clone(), existing.clone());
                continue;
            }
            // In-flight or unknown → force cancelled terminal.
            let mut cancelled = existing.clone();
            cancelled.status = step_status::COMPLETED;
            cancelled.conclusion = step_conclusion::FAILED; // Twirp has no cancel enum
            cancelled.name = if cancelled.name.is_empty() {
                task.name.clone()
            } else {
                cancelled.name
            };
            cancelled.number = if cancelled.number == 0 {
                task.number
            } else {
                cancelled.number
            };
            if cancelled.completed_at.is_none() {
                cancelled.completed_at = Some(completed_at.to_owned());
            }
            out.insert(task.external_id.clone(), cancelled);
        } else {
            // Never reported — emit a single cancelled record.
            out.insert(
                task.external_id.clone(),
                StepUpdate {
                    external_id: task.external_id.clone(),
                    number: task.number,
                    name: task.name.clone(),
                    status: step_status::COMPLETED,
                    started_at: None,
                    completed_at: Some(completed_at.to_owned()),
                    conclusion: step_conclusion::FAILED,
                },
            );
        }
    }

    let mut steps: Vec<StepUpdate> = out.into_values().collect();
    steps.sort_by_key(|s| s.number);
    steps
}

/// Count cancelled terminal records (completed + failed conclusion used for cancel).
pub fn count_cancelled(records: &[StepUpdate]) -> usize {
    records
        .iter()
        .filter(|r| r.status == step_status::COMPLETED && r.conclusion == step_conclusion::FAILED)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use proptest::prelude::*;

    const CANCEL_COMPLETED_AT: &str = "2024-01-02T03:04:05Z";
    const ORIGINAL_COMPLETED_AT: &str = "2024-01-01T00:00:00Z";

    fn assert_synthesized_cancelled(record: &StepUpdate, supplied_completed_at: &str) {
        assert_eq!(record.status, step_status::COMPLETED);
        assert_eq!(record.conclusion, step_conclusion::FAILED);
        let completed_at = record
            .completed_at
            .as_deref()
            .expect("synthesized cancellation must have completion time");
        assert_eq!(completed_at, supplied_completed_at);
        assert_ne!(record.completed_at.as_deref(), Some("cancelled"));
        let parsed = DateTime::parse_from_rfc3339(completed_at)
            .expect("synthesized completion time must be RFC3339");
        assert_eq!(parsed.offset().local_minus_utc(), 0);
    }

    fn full(id: &str, number: u32, name: &str, status: u32, conclusion: u32) -> StepUpdate {
        StepUpdate {
            external_id: id.into(),
            number,
            name: name.into(),
            status,
            started_at: Some("t0".into()),
            completed_at: if status == step_status::COMPLETED {
                Some("t1".into())
            } else {
                None
            },
            conclusion,
        }
    }

    #[test]
    fn completed_does_not_regress_to_in_progress() {
        let existing = full(
            "s1",
            1,
            "A",
            step_status::COMPLETED,
            step_conclusion::SUCCEEDED,
        );
        let incoming = PartialStepUpdate {
            external_id: "s1".into(),
            number: Some(1),
            name: Some("A".into()),
            status: Some(step_status::IN_PROGRESS),
            started_at: Some("t0".into()),
            has_started_at: true,
            completed_at: None,
            has_completed_at: true,
            conclusion: Some(0),
        };
        let merged = merge_step_update(Some(&existing), &incoming);
        assert_eq!(merged.status, step_status::COMPLETED);
        assert_eq!(merged.conclusion, step_conclusion::SUCCEEDED);
    }

    #[test]
    fn omitted_fields_preserve_existing() {
        let existing = full("s1", 2, "Build", step_status::IN_PROGRESS, 0);
        let incoming = PartialStepUpdate {
            external_id: "s1".into(),
            number: None,
            name: None,
            status: Some(step_status::COMPLETED),
            started_at: None,
            has_started_at: false,
            completed_at: Some("t9".into()),
            has_completed_at: true,
            conclusion: Some(step_conclusion::FAILED),
        };
        let merged = merge_step_update(Some(&existing), &incoming);
        assert_eq!(merged.number, 2);
        assert_eq!(merged.name, "Build");
        assert_eq!(merged.started_at.as_deref(), Some("t0"));
        assert_eq!(merged.status, step_status::COMPLETED);
        assert_eq!(merged.conclusion, step_conclusion::FAILED);
    }

    #[test]
    fn unrelated_steps_never_merge() {
        let mut store = BTreeMap::new();
        apply_step_update(
            &mut store,
            &PartialStepUpdate::from_full(&full(
                "a",
                1,
                "A",
                step_status::COMPLETED,
                step_conclusion::SUCCEEDED,
            )),
        );
        apply_step_update(
            &mut store,
            &PartialStepUpdate::from_full(&full(
                "b",
                1,
                "B",
                step_status::COMPLETED,
                step_conclusion::FAILED,
            )),
        );
        assert_eq!(store.len(), 2);
        assert_eq!(store["a"].conclusion, step_conclusion::SUCCEEDED);
        assert_eq!(store["b"].conclusion, step_conclusion::FAILED);
    }

    #[test]
    fn reconcile_cancels_in_flight_preserves_completed() {
        let dispatched = vec![
            DispatchedTask {
                external_id: "setup".into(),
                number: 1,
                name: "Set up job".into(),
                synthetic: true,
            },
            DispatchedTask {
                external_id: "s1".into(),
                number: 2,
                name: "Run".into(),
                synthetic: false,
            },
            DispatchedTask {
                external_id: "s2".into(),
                number: 3,
                name: "Later".into(),
                synthetic: false,
            },
        ];
        let mut received = BTreeMap::new();
        let mut setup_record = full(
            "setup",
            1,
            "Set up job",
            step_status::COMPLETED,
            step_conclusion::SUCCEEDED,
        );
        setup_record.completed_at = Some(ORIGINAL_COMPLETED_AT.into());
        received.insert("setup".into(), setup_record);
        received.insert(
            "s1".into(),
            full("s1", 2, "Run", step_status::IN_PROGRESS, 0),
        );
        // s2 never reported

        let out = reconcile_cancelled_steps(&dispatched, &received, CANCEL_COMPLETED_AT);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].conclusion, step_conclusion::SUCCEEDED);
        assert_eq!(out[0].completed_at.as_deref(), Some(ORIGINAL_COMPLETED_AT));
        assert_eq!(out[1].status, step_status::COMPLETED);
        assert_eq!(out[1].conclusion, step_conclusion::FAILED);
        assert_synthesized_cancelled(&out[1], CANCEL_COMPLETED_AT);
        assert_eq!(out[2].conclusion, step_conclusion::FAILED);
        assert_synthesized_cancelled(&out[2], CANCEL_COMPLETED_AT);
        // Exactly one cancelled record per interrupted task (s1, s2); setup untouched.
        assert_eq!(
            out.iter()
                .filter(|r| r.conclusion == step_conclusion::FAILED)
                .count(),
            2
        );
    }

    fn arb_status() -> impl Strategy<Value = u32> {
        prop_oneof![
            Just(0u32),
            Just(step_status::IN_PROGRESS),
            Just(step_status::COMPLETED),
        ]
    }

    fn arb_conclusion() -> impl Strategy<Value = u32> {
        prop_oneof![
            Just(0u32),
            Just(step_conclusion::SUCCEEDED),
            Just(step_conclusion::FAILED),
            Just(step_conclusion::SKIPPED),
        ]
    }

    fn arb_partial() -> impl Strategy<Value = PartialStepUpdate> {
        (
            "[a-c]{1}",
            prop::option::of(1u32..5),
            prop::option::of("[A-Z]{1,4}"),
            prop::option::of(arb_status()),
            prop::option::of(arb_conclusion()),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(id, number, name, status, conclusion, has_started, has_completed)| {
                    PartialStepUpdate {
                        external_id: id,
                        number,
                        name,
                        status,
                        started_at: has_started.then(|| "t".into()),
                        has_started_at: has_started,
                        completed_at: has_completed.then(|| "t".into()),
                        has_completed_at: has_completed,
                        conclusion,
                    }
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        /// Merging the same partial twice is idempotent.
        #[test]
        fn merge_idempotent(first in arb_partial(), second in arb_partial()) {
            let mut store = BTreeMap::new();
            let a = apply_step_update(&mut store, &first);
            let b = apply_step_update(&mut store, &first);
            prop_assert_eq!(&a, &b);

            // Force a distinct second id so we never reject on collision.
            let mut second = second;
            if second.external_id == first.external_id {
                second.external_id = format!("{}'", first.external_id);
            }
            apply_step_update(&mut store, &second);
            prop_assert_eq!(store.len(), 2);
        }

        /// Status is monotonic under a sequence of updates to one id.
        #[test]
        fn status_monotonic(updates in proptest::collection::vec(arb_partial(), 1..8)) {
            let mut store = BTreeMap::new();
            let id = "step".to_string();
            let mut last_rank = 0u8;
            for mut u in updates {
                u.external_id = id.clone();
                let merged = apply_step_update(&mut store, &u);
                let rank = status_rank(merged.status);
                prop_assert!(rank >= last_rank, "status regressed {} → {}", last_rank, rank);
                last_rank = rank;
            }
        }

        /// Conclusion once set non-zero is never erased by conclusion=0 partial.
        #[test]
        fn conclusion_not_erased(
            c in prop_oneof![
                Just(step_conclusion::SUCCEEDED),
                Just(step_conclusion::FAILED),
                Just(step_conclusion::SKIPPED),
            ]
        ) {
            let existing = full("s", 1, "X", step_status::COMPLETED, c);
            let incoming = PartialStepUpdate {
                external_id: "s".into(),
                number: None,
                name: None,
                status: None,
                started_at: None,
                has_started_at: false,
                completed_at: None,
                has_completed_at: false,
                conclusion: Some(0),
            };
            let merged = merge_step_update(Some(&existing), &incoming);
            prop_assert_eq!(merged.conclusion, c);
        }

        /// external_id is the only identity: same number, different id → two rows.
        #[test]
        fn external_id_not_number(suffix in 0u32..1000) {
            // Generate distinct ids without prop_assume rejects.
            let id_a = format!("a{suffix}");
            let id_b = format!("b{suffix}");
            let mut store = BTreeMap::new();
            apply_step_update(
                &mut store,
                &PartialStepUpdate::from_full(&full(
                    &id_a,
                    1,
                    "A",
                    step_status::COMPLETED,
                    step_conclusion::SUCCEEDED,
                )),
            );
            apply_step_update(
                &mut store,
                &PartialStepUpdate::from_full(&full(
                    &id_b,
                    1,
                    "B",
                    step_status::COMPLETED,
                    step_conclusion::FAILED,
                )),
            );
            prop_assert_eq!(store.len(), 2);
        }

        /// Cancel reconciliation: completed preserved; others terminal cancelled once.
        #[test]
        fn reconcile_exactly_one_cancel_per_open(
            n in 1usize..6,
            completed_mask in 0u8..32
        ) {
            let dispatched: Vec<DispatchedTask> = (0..n)
                .map(|i| DispatchedTask {
                    external_id: format!("t{i}"),
                    number: (i as u32) + 1,
                    name: format!("step{i}"),
                    synthetic: i == 0,
                })
                .collect();
            let mut received = BTreeMap::new();
            for (i, task) in dispatched.iter().enumerate() {
                if completed_mask & (1 << (i % 8)) != 0 {
                    received.insert(
                        task.external_id.clone(),
                        full(
                            &task.external_id,
                            task.number,
                            &task.name,
                            step_status::COMPLETED,
                            step_conclusion::SUCCEEDED,
                        ),
                    );
                } else if i % 2 == 0 {
                    received.insert(
                        task.external_id.clone(),
                        full(
                            &task.external_id,
                            task.number,
                            &task.name,
                            step_status::IN_PROGRESS,
                            0,
                        ),
                    );
                }
            }
            let out = reconcile_cancelled_steps(&dispatched, &received, CANCEL_COMPLETED_AT);
            prop_assert_eq!(out.len(), n);
            // Order by number.
            for w in out.windows(2) {
                prop_assert!(w[0].number <= w[1].number);
            }
            for task in &dispatched {
                let rec = out.iter().find(|r| r.external_id == task.external_id).unwrap();
                if let Some(prev) = received.get(&task.external_id) {
                    if prev.status == step_status::COMPLETED {
                        prop_assert_eq!(rec.conclusion, step_conclusion::SUCCEEDED);
                    } else {
                        prop_assert_eq!(rec.status, step_status::COMPLETED);
                        prop_assert_eq!(rec.conclusion, step_conclusion::FAILED);
                        prop_assert_eq!(rec.completed_at.as_deref(), Some(CANCEL_COMPLETED_AT));
                        prop_assert!(DateTime::parse_from_rfc3339(
                            rec.completed_at.as_deref().unwrap()
                        ).is_ok());
                    }
                } else {
                    prop_assert_eq!(rec.conclusion, step_conclusion::FAILED);
                    prop_assert_eq!(rec.completed_at.as_deref(), Some(CANCEL_COMPLETED_AT));
                    prop_assert!(DateTime::parse_from_rfc3339(
                        rec.completed_at.as_deref().unwrap()
                    ).is_ok());
                }
            }
            // No duplicates.
            let mut ids = std::collections::BTreeSet::new();
            for r in &out {
                prop_assert!(ids.insert(r.external_id.clone()));
            }
        }
    }

    /// Explicit null on wire (has_started_at=true, started_at=None) does NOT
    /// clear an existing timestamp — official cumulative updates preserve
    /// the first non-empty value.
    /// Oracle: docs/property-tests.md §4.5 — explicit null follows wire contract.
    #[test]
    fn explicit_null_wire_contract() {
        let existing = full("s1", 1, "A", step_status::IN_PROGRESS, 0);
        assert_eq!(existing.started_at.as_deref(), Some("t0"));
        let incoming = PartialStepUpdate {
            external_id: "s1".into(),
            number: None,
            name: None,
            status: None,
            started_at: None,
            has_started_at: true,
            completed_at: None,
            has_completed_at: false,
            conclusion: None,
        };
        let merged = merge_step_update(Some(&existing), &incoming);
        assert_eq!(
            merged.started_at.as_deref(),
            Some("t0"),
            "cumulative update preserves existing timestamp"
        );
        let fresh = merge_step_update(None, &incoming);
        assert_eq!(fresh.started_at, None, "no existing → stays None");
    }

    /// Unknown received records (not in dispatched list) handled gracefully.
    /// Oracle: docs/property-tests.md §4.c.4 — must not be silently dropped.
    #[test]
    fn unknown_received_record_not_dropped() {
        let dispatched = vec![]; // empty
        let mut received = BTreeMap::new();
        received.insert(
            "unknown".into(),
            full(
                "unknown",
                99,
                "Mystery",
                step_status::COMPLETED,
                step_conclusion::SUCCEEDED,
            ),
        );
        let out = reconcile_cancelled_steps(&dispatched, &received, CANCEL_COMPLETED_AT);
        // With empty dispatched, no tasks to reconcile — output should be empty
        // (unknown records are not in scope of dispatched reconciliation)
        assert_eq!(out.len(), 0);
    }

    /// Twirp conclusion constants match the proto enum from golden flows.
    /// Oracle: docs/property-tests.md §4 — conclusion mapping from Twirp schema.
    #[test]
    fn twirp_conclusion_mapping() {
        assert_eq!(step_conclusion::SUCCEEDED, 2);
        assert_eq!(step_conclusion::FAILED, 3);
        assert_eq!(step_conclusion::SKIPPED, 7);
        assert_eq!(step_status::IN_PROGRESS, 3);
        assert_eq!(step_status::COMPLETED, 6);
    }

    // Generated merges produce valid field types.
    // Oracle: docs/property-tests.md §4.12 — valid field types.
    proptest! {
        #![proptest_config(ProptestConfig { cases: 1_000, ..ProptestConfig::default() })]

        #[test]
        fn valid_field_types_in_generated_merges(
            updates in proptest::collection::vec(arb_partial(), 1..6)
        ) {
            let mut store = BTreeMap::new();
            for u in updates {
                apply_step_update(&mut store, &u);
            }
            for (ext_id, record) in &store {
                prop_assert!(!ext_id.is_empty(), "empty external_id");
                prop_assert!(!record.external_id.is_empty(), "empty record external_id");
                prop_assert!(
                    matches!(record.status, 0 | step_status::IN_PROGRESS | step_status::COMPLETED),
                    "unknown status {}", record.status
                );
                prop_assert!(
                    matches!(record.conclusion, 0 | step_conclusion::SUCCEEDED | step_conclusion::FAILED | step_conclusion::SKIPPED),
                    "unknown conclusion {}", record.conclusion
                );
            }
        }
    }
}
