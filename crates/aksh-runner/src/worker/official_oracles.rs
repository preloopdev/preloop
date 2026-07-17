//! Oracles ported from official `actions/runner` v2.335.1 L0 tests and source.
//!
//! These tables are the ground truth for property tests. When aksh disagrees,
//! the property fails — we do not normalize away official behavior.
//!
//! Sources:
//! - `src/Test/L0/Worker/Expressions/ConditionFunctionsL0.cs`
//! - `src/Runner.Worker/Expressions/SuccessFunction.cs` (`jobStatus ?? Success`)
//! - `src/Sdk/WorkflowParser/Conversion/WorkflowTemplateConverter.cs`
//!   `ConvertToIfCondition` (default `success()`, else `success() && (cond)`)
//! - `src/Sdk/WorkflowParser/Conversion/MatrixBuilder.cs` documented examples
//! - Live GitHub run for `p0-failure-conditions.yml` (docs/test-coverage.md)

use super::step_conditions::{evaluate_step_condition, StatusFlags};

/// Official `ActionResult` / `JobContext.Status` values.
///
/// Official SuccessFunction:
/// `ActionResult jobStatus = executionContext.JobContext.Status ?? ActionResult.Success`
/// so **unset (null) is Success**.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfficialJobStatus {
    /// `JobContext.Status == null` is treated as Success by official runner.
    Unset,
    Success,
    Failure,
    Cancelled,
}

impl OfficialJobStatus {
    /// Map to aksh expression flags the way JobContext does for non-composite steps.
    pub fn to_flags(self) -> StatusFlags {
        match self {
            // Official: null ?? Success → success() true
            Self::Unset | Self::Success => StatusFlags::ok(),
            Self::Failure => StatusFlags::after_failure(),
            Self::Cancelled => StatusFlags::after_cancel(),
        }
    }
}

/// One row of ConditionFunctionsL0 for a status function.
#[derive(Debug, Clone, Copy)]
pub struct OfficialStatusCase {
    /// Job status under test.
    pub status: OfficialJobStatus,
    /// Expected `always()` / `success()` / `failure()` / `cancelled()`.
    pub always: bool,
    pub success: bool,
    pub failure: bool,
    pub cancelled: bool,
}

/// Exact ConditionFunctionsL0 table (non-composite).
///
/// From official AlwaysFunction / SuccessFunction / FailureFunction / CancelledFunction tests.
pub fn condition_functions_l0_table() -> &'static [OfficialStatusCase] {
    &[
        OfficialStatusCase {
            status: OfficialJobStatus::Unset,
            always: true,
            success: true, // null ?? Success
            failure: false,
            cancelled: false,
        },
        OfficialStatusCase {
            status: OfficialJobStatus::Success,
            always: true,
            success: true,
            failure: false,
            cancelled: false,
        },
        OfficialStatusCase {
            status: OfficialJobStatus::Failure,
            always: true,
            success: false,
            failure: true,
            cancelled: false,
        },
        OfficialStatusCase {
            status: OfficialJobStatus::Cancelled,
            always: true,
            success: false,
            failure: false,
            cancelled: true,
        },
    ]
}

/// p0-failure-conditions.yml expected step outcomes after intentional failure
/// (verified on GitHub Actions run 28754419325 per docs/test-coverage.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepRunOutcome {
    Run,
    Skip,
}

/// (step if-condition, expected outcome) once job is in Failure status.
pub fn p0_failure_conditions_oracle() -> &'static [(&'static str, StepRunOutcome)] {
    &[
        // After failure, default/success gated steps skip; failure/always run.
        ("success()", StepRunOutcome::Skip),
        ("failure()", StepRunOutcome::Run),
        ("always()", StepRunOutcome::Run),
    ]
}

/// Official ConvertToIfCondition rewriting rules.
pub fn official_effective_condition(raw: Option<&str>) -> String {
    match raw {
        None => "success()".to_owned(),
        Some(s) if s.trim().is_empty() => "success()".to_owned(),
        Some(s) => {
            let stripped = aksh_gha_expressions::trim_expression_markers(s);
            if super::step_conditions::contains_status_check_function(stripped) {
                stripped.to_owned()
            } else {
                format!("success() && ({stripped})")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// ConditionFunctionsL0 — exact table, no generation.
    #[test]
    fn condition_functions_l0_matches_official_runner() {
        for case in condition_functions_l0_table() {
            let flags = case.status.to_flags();
            assert_eq!(
                evaluate_step_condition(Some("always()"), flags).unwrap(),
                case.always,
                "always() for {:?}",
                case.status
            );
            assert_eq!(
                evaluate_step_condition(Some("success()"), flags).unwrap(),
                case.success,
                "success() for {:?}",
                case.status
            );
            assert_eq!(
                evaluate_step_condition(Some("failure()"), flags).unwrap(),
                case.failure,
                "failure() for {:?}",
                case.status
            );
            assert_eq!(
                evaluate_step_condition(Some("cancelled()"), flags).unwrap(),
                case.cancelled,
                "cancelled() for {:?}",
                case.status
            );
        }
    }

    /// Unset status must behave like Success (SuccessFunction null-coalesce).
    #[test]
    fn unset_job_status_is_success_like_official() {
        let unset = OfficialJobStatus::Unset.to_flags();
        let success = OfficialJobStatus::Success.to_flags();
        assert_eq!(
            evaluate_step_condition(Some("success()"), unset).unwrap(),
            evaluate_step_condition(Some("success()"), success).unwrap()
        );
        assert!(evaluate_step_condition(None, unset).unwrap());
    }

    /// p0-failure-conditions.yml oracle from live GitHub.
    #[test]
    fn p0_failure_conditions_matches_github_run() {
        let flags = OfficialJobStatus::Failure.to_flags();
        for (cond, expected) in p0_failure_conditions_oracle() {
            let runs = evaluate_step_condition(Some(cond), flags).unwrap();
            match expected {
                StepRunOutcome::Run => assert!(runs, "{cond} should run after failure"),
                StepRunOutcome::Skip => assert!(!runs, "{cond} should skip after failure"),
            }
        }
    }

    /// ConvertToIfCondition rewrite must match official formatting.
    #[test]
    fn convert_to_if_condition_matches_official() {
        assert_eq!(official_effective_condition(None), "success()");
        assert_eq!(official_effective_condition(Some("")), "success()");
        assert_eq!(
            official_effective_condition(Some("true")),
            "success() && (true)"
        );
        assert_eq!(official_effective_condition(Some("failure()")), "failure()");
        assert_eq!(
            official_effective_condition(Some("always() && 1 == 1")),
            "always() && 1 == 1"
        );
        assert_eq!(
            official_effective_condition(Some("${{ success() }}")),
            "success()"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        /// Across all official statuses, status functions stay mutually exclusive
        /// (except always, which is always true). Official JobContext.Status is a
        /// single enum, never Success+Failure simultaneously.
        #[test]
        fn official_status_enum_invariants(status in prop_oneof![
            Just(OfficialJobStatus::Unset),
            Just(OfficialJobStatus::Success),
            Just(OfficialJobStatus::Failure),
            Just(OfficialJobStatus::Cancelled),
        ]) {
            let flags = status.to_flags();
            let s = evaluate_step_condition(Some("success()"), flags).unwrap();
            let f = evaluate_step_condition(Some("failure()"), flags).unwrap();
            let c = evaluate_step_condition(Some("cancelled()"), flags).unwrap();
            let a = evaluate_step_condition(Some("always()"), flags).unwrap();
            prop_assert!(a);
            // At most one of success/failure/cancelled is true for official statuses.
            let true_count = [s, f, c].iter().filter(|x| **x).count();
            prop_assert!(
                true_count == 1,
                "expected exactly one of success/failure/cancelled, got s={s} f={f} c={c} for {status:?}"
            );
            // Unset and Success are equivalent for success().
            if matches!(status, OfficialJobStatus::Unset | OfficialJobStatus::Success) {
                prop_assert!(s && !f && !c);
            }
        }

        /// After failure, bare literals stay success-gated (ConvertToIfCondition).
        #[test]
        fn after_failure_bare_true_is_skipped(lit in prop_oneof![Just("true"), Just("1 == 1")]) {
            let flags = OfficialJobStatus::Failure.to_flags();
            let effective = official_effective_condition(Some(lit));
            prop_assert!(effective.starts_with("success() &&"));
            prop_assert!(!evaluate_step_condition(Some(lit), flags).unwrap());
        }

        /// effective_condition matches official_effective_condition for common inputs.
        #[test]
        fn effective_condition_parity(
            raw in prop::option::of(prop_oneof![
                Just("success()".to_string()),
                Just("failure()".to_string()),
                Just("always()".to_string()),
                Just("cancelled()".to_string()),
                Just("true".to_string()),
                Just("false".to_string()),
                Just("${{ always() }}".to_string()),
                Just("1 == 1".to_string()),
            ])
        ) {
            let a = super::super::step_conditions::effective_condition(raw.as_deref());
            let b = official_effective_condition(raw.as_deref());
            prop_assert_eq!(a, b);
        }
    }
}
