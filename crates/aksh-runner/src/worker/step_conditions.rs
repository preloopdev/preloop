//! Step condition evaluation matching official `actions/runner` StepsRunner.
//!
//! Reference: ConditionFunctionsL0 + StepsRunner implicit success gate.
//!
//! Rules:
//! - Default condition is `success()`.
//! - If the expression does **not** contain a status-check function
//!   (`success` / `failure` / `cancelled` / `always`), it is rewritten to
//!   `success() && (expr)`.
//! - `always()` is true regardless of prior failure/cancel.
//! - Status flags are not mutated by evaluation.
//! - Skipped is not success/failure/cancelled; default success gate fails after
//!   a prior step failure even when the job is not cancelled.

pub use aksh_gha_expressions::{contains_status_check_function, effective_condition};
use aksh_gha_expressions::{eval_bool, Context};

/// Job-level status flags used for condition evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusFlags {
    /// `success()` result.
    pub success: bool,
    /// `failure()` result.
    pub failure: bool,
    /// `cancelled()` result.
    pub cancelled: bool,
}

impl StatusFlags {
    /// Healthy job — default after no failures.
    pub fn ok() -> Self {
        Self {
            success: true,
            failure: false,
            cancelled: false,
        }
    }

    /// After a prior step failure (not cancelled).
    pub fn after_failure() -> Self {
        Self {
            success: false,
            failure: true,
            cancelled: false,
        }
    }

    /// After external cancellation.
    pub fn after_cancel() -> Self {
        Self {
            success: false,
            failure: false,
            cancelled: true,
        }
    }

    /// Build an expression context with these flags (no extra roots).
    pub fn to_context(self) -> Context {
        Context::default().with_status(self.success, self.failure, self.cancelled)
    }
}

/// Evaluate whether a step should run under the given status flags.
///
/// Returns `Ok(bool)` or an expression error. Does not mutate `flags`.
pub fn evaluate_step_condition(
    raw: Option<&str>,
    flags: StatusFlags,
) -> Result<bool, aksh_gha_expressions::ExpressionError> {
    let expr = effective_condition(raw);
    let ctx = flags.to_context();
    eval_bool(&expr, &ctx)
}

/// Expected should-run result for the canonical status × condition truth table.
///
/// Conditions: default / success / failure / cancelled / always / true / false /
/// `failure() || cancelled()` — matching ConditionFunctionsL0 + StepsRunner.
pub fn expected_should_run(condition: Option<&str>, flags: StatusFlags) -> bool {
    evaluate_step_condition(condition, flags).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn default_runs_only_on_success() {
        assert!(evaluate_step_condition(None, StatusFlags::ok()).unwrap());
        assert!(!evaluate_step_condition(None, StatusFlags::after_failure()).unwrap());
        assert!(!evaluate_step_condition(None, StatusFlags::after_cancel()).unwrap());
    }

    #[test]
    fn always_runs_regardless() {
        for flags in [
            StatusFlags::ok(),
            StatusFlags::after_failure(),
            StatusFlags::after_cancel(),
            StatusFlags {
                success: false,
                failure: false,
                cancelled: false,
            },
        ] {
            assert!(
                evaluate_step_condition(Some("always()"), flags).unwrap(),
                "always() failed for {flags:?}"
            );
        }
    }

    #[test]
    fn failure_only_after_failure() {
        assert!(!evaluate_step_condition(Some("failure()"), StatusFlags::ok()).unwrap());
        assert!(evaluate_step_condition(Some("failure()"), StatusFlags::after_failure()).unwrap());
        assert!(!evaluate_step_condition(Some("failure()"), StatusFlags::after_cancel()).unwrap());
    }

    #[test]
    fn cancelled_only_when_cancelled() {
        assert!(!evaluate_step_condition(Some("cancelled()"), StatusFlags::ok()).unwrap());
        assert!(
            !evaluate_step_condition(Some("cancelled()"), StatusFlags::after_failure()).unwrap()
        );
        assert!(evaluate_step_condition(Some("cancelled()"), StatusFlags::after_cancel()).unwrap());
    }

    #[test]
    fn bare_true_is_gated_by_success() {
        // Official: no status fn → success() && (true)
        assert!(evaluate_step_condition(Some("true"), StatusFlags::ok()).unwrap());
        assert!(!evaluate_step_condition(Some("true"), StatusFlags::after_failure()).unwrap());
    }

    #[test]
    fn status_fn_inside_string_does_not_count() {
        assert!(!contains_status_check_function("'success()'"));
        assert!(!contains_status_check_function("\"failure()\""));
        assert!(contains_status_check_function("success()"));
        assert!(contains_status_check_function("true || always()"));
    }

    #[test]
    fn evaluation_does_not_mutate_flags() {
        let flags = StatusFlags::after_failure();
        let _ = evaluate_step_condition(Some("always()"), flags).unwrap();
        assert_eq!(flags, StatusFlags::after_failure());
    }

    /// Full truth table for the four status functions × flag combinations.
    #[test]
    fn status_function_truth_table() {
        let combos = [
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (false, true, true),
            (false, false, false),
            (true, true, false), // unusual but allowed by context API
        ];
        for (s, f, c) in combos {
            let flags = StatusFlags {
                success: s,
                failure: f,
                cancelled: c,
            };
            assert_eq!(
                evaluate_step_condition(Some("success()"), flags).unwrap(),
                s
            );
            assert_eq!(
                evaluate_step_condition(Some("failure()"), flags).unwrap(),
                f
            );
            assert_eq!(
                evaluate_step_condition(Some("cancelled()"), flags).unwrap(),
                c
            );
            assert!(evaluate_step_condition(Some("always()"), flags).unwrap());
        }
    }

    fn arb_flags() -> impl Strategy<Value = StatusFlags> {
        (any::<bool>(), any::<bool>(), any::<bool>()).prop_map(|(s, f, c)| StatusFlags {
            success: s,
            failure: f,
            cancelled: c,
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn always_is_constant_true(flags in arb_flags()) {
            prop_assert!(evaluate_step_condition(Some("always()"), flags).unwrap());
            prop_assert!(evaluate_step_condition(Some("${{ always() }}"), flags).unwrap());
        }

        #[test]
        fn success_matches_flag(flags in arb_flags()) {
            prop_assert_eq!(
                evaluate_step_condition(Some("success()"), flags).unwrap(),
                flags.success
            );
        }

        #[test]
        fn failure_matches_flag(flags in arb_flags()) {
            prop_assert_eq!(
                evaluate_step_condition(Some("failure()"), flags).unwrap(),
                flags.failure
            );
        }

        #[test]
        fn cancelled_matches_flag(flags in arb_flags()) {
            prop_assert_eq!(
                evaluate_step_condition(Some("cancelled()"), flags).unwrap(),
                flags.cancelled
            );
        }

        #[test]
        fn default_equals_success_fn(flags in arb_flags()) {
            prop_assert_eq!(
                evaluate_step_condition(None, flags).unwrap(),
                evaluate_step_condition(Some("success()"), flags).unwrap()
            );
        }

        #[test]
        fn bare_literal_gated_by_success(
            flags in arb_flags(),
            lit in prop_oneof![Just("true"), Just("false"), Just("1"), Just("0")]
        ) {
            let result = evaluate_step_condition(Some(lit), flags).unwrap();
            let inner = aksh_gha_expressions::eval_bool(lit, &flags.to_context()).unwrap();
            prop_assert_eq!(result, flags.success && inner);
        }

        #[test]
        fn compound_failure_or_cancelled(flags in arb_flags()) {
            let result =
                evaluate_step_condition(Some("failure() || cancelled()"), flags).unwrap();
            prop_assert_eq!(result, flags.failure || flags.cancelled);
        }

        #[test]
        fn skipped_not_confused_with_status_fns(flags in arb_flags()) {
            // "skipped" is not a status function; bare identifier is a context path → null → false,
            // and is success-gated.
            let result = evaluate_step_condition(Some("skipped"), flags);
            // May be Ok(false) or err depending on path resolution; must not panic.
            let _ = result;
            prop_assert!(!contains_status_check_function("skipped"));
            prop_assert!(!contains_status_check_function("success")); // no '('
        }

        #[test]
        fn effective_condition_stable(raw in prop::option::of("[a-z()!&| ]{0,24}")) {
            let a = effective_condition(raw.as_deref());
            let b = effective_condition(raw.as_deref());
            prop_assert_eq!(a, b);
        }
    }
}
