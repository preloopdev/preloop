use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::*;
use crate::expand::coerce_value;
use crate::trigger::glob_match;
use aksh_gha_protocol::JobId;

#[test]
fn glob_match_handles_multiple_wildcards() {
    assert!(glob_match("feature/*/*", "feature/auth/login"));
    assert!(glob_match("release-*-rc*", "release-2026-rc1"));
    assert!(!glob_match("feature/*", "feature/auth/login"));
    assert!(glob_match("src/**", "src/bin/main.rs"));
}

#[test]
fn parses_workflow_run_name() {
    let workflow = parse_workflow(
        r#"
name: deploy
run-name: Deploy production
on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();

    assert_eq!(workflow.run_name.as_deref(), Some("Deploy production"));
}

#[test]
fn workflow_run_name_defaults_to_none() {
    let workflow = parse_workflow(
        r#"
name: ci
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo test
"#,
    )
    .unwrap();

    assert_eq!(workflow.run_name, None);
}

#[test]
fn preserves_run_name_expression_source() {
    let workflow = parse_workflow(
        r#"
run-name: "Deploy ${{ github.ref }}"
on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();

    assert_eq!(
        workflow.run_name.as_deref(),
        Some("Deploy ${{ github.ref }}")
    );
}

#[test]
fn trigger_context_matches_activity_types() {
    let workflow = parse_workflow(
        r#"
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
    )
    .unwrap();

    assert!(workflow
        .on
        .matches_with_context("pull_request", None, None, &[], Some("opened"), &[]));
    assert!(!workflow.on.matches_with_context(
        "pull_request",
        None,
        None,
        &[],
        Some("closed"),
        &[]
    ));
}

#[test]
fn parses_and_expands_opencode_test_workflow_fixture() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let fixture_path = root
        .join("fixtures")
        .join("workflows")
        .join("opencode-test.yml");
    let yaml = std::fs::read_to_string(&fixture_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", fixture_path.display()));
    let parsed = parse_workflow(&yaml).expect("parse_workflow failed for opencode-test.yml");
    let expanded = expand_jobs(&parsed).expect("expand_jobs failed for opencode-test.yml");
    assert_eq!(expanded.len(), 4);
}
#[test]
fn schedule_trigger_matches_event_name() {
    let workflow = parse_workflow(
        r#"
on:
  schedule:
    - cron: '0 0 * * *'
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
"#,
    )
    .unwrap();

    assert!(workflow.on.matches("schedule"));
    assert!(!workflow.on.matches("push"));
}

#[test]
fn parses_and_expands_matrix() {
    let workflow = parse_workflow(
        r#"
name: ci
on: [push]
env:
  GLOBAL: true
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        node: [20, 22]
        exclude:
          - os: macos-latest
            node: 20
        include:
          - os: ubuntu-latest
            node: 24
            experimental: true
    steps:
      - run: echo hi
"#,
    )
    .unwrap();

    assert!(workflow.on.matches("push"));
    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs.len(), 4);
    assert!(jobs.iter().any(|job| {
        job.matrix.get("node") == Some(&json!(24))
            && job.matrix.get("experimental") == Some(&json!(true))
    }));
    assert_eq!(jobs[0].env.get("GLOBAL"), Some(&"true".to_owned()));
}

#[test]
fn evaluates_job_continue_on_error_for_each_matrix_cell() {
    let workflow = parse_workflow(
        r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    continue-on-error: ${{ matrix.expected == 'failure' }}
    strategy:
      matrix:
        expected: [success, failure]
    steps:
      - run: echo test
"#,
    )
    .unwrap();

    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs.len(), 2);
    for job in jobs {
        let expected = job.matrix.get("expected").and_then(Value::as_str).unwrap();
        assert_eq!(job.continue_on_error, expected == "failure");
    }
}

#[test]
fn parses_object_runs_on_group_and_labels() {
    let workflow = parse_workflow(
        r#"
on: push
jobs:
  deploy:
    runs-on:
      group: release-runners
      labels: [self-hosted, linux]
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();
    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs[0].runner_group.as_deref(), Some("release-runners"));
    assert_eq!(jobs[0].runs_on, vec!["self-hosted", "linux"]);
}

#[test]
fn parses_local_action_metadata() {
    let action = parse_action_metadata(
        r#"
name: local composite
description: test action
inputs:
  who:
    required: true
    default: world
runs:
  using: composite
  steps:
    - run: echo "hello ${{ inputs.who }}"
"#,
    )
    .unwrap();

    assert_eq!(action.name, "local composite");
    assert!(matches!(action.runs, ActionRuns::Composite { .. }));
    assert!(action.inputs.get("who").is_some_and(|input| input.required));
}

#[test]
fn expands_local_reusable_workflow_call_jobs() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
"#,
    )
    .unwrap();
    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
        .to_owned(),
    );

    let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id.0, "call/test");
    assert_eq!(jobs[0].runs_on, vec!["ubuntu-latest"]);
}

#[test]
fn records_oidc_permission_and_matrix_environment() {
    let workflow = parse_workflow(
        r#"
on: push
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ matrix.environment }}
    strategy:
      matrix:
        environment: [staging, production]
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();

    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job.oidc_id_token_granted));
    assert_eq!(jobs[0].oidc_environment.as_deref(), Some("staging"));
    assert_eq!(jobs[1].oidc_environment.as_deref(), Some("production"));
}

#[test]
fn reusable_oidc_permission_requires_caller_grant() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
"#,
    )
    .unwrap();
    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production
    steps:
      - run: echo deploy
"#
        .to_owned(),
    );

    let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;
    assert_eq!(jobs.len(), 1);
    assert!(!jobs[0].oidc_id_token_granted);
    assert_eq!(jobs[0].oidc_environment.as_deref(), Some("production"));
    assert_eq!(
        jobs[0].oidc_job_workflow_ref.as_deref(),
        Some("./.github/workflows/reusable.yml")
    );
}

#[test]
fn reusable_workflow_secrets_inherit_flag() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    secrets: inherit
"#,
    )
    .unwrap();
    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
        .to_owned(),
    );

    let jobs = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].secrets_inherit);
}
#[test]
fn reusable_workflow_input_validation_and_coercion() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    with:
      name: "Alice"
      enable: "true"
      count: "42"
"#,
    )
    .unwrap();

    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
    inputs:
      name:
        type: string
        required: true
      enable:
        type: boolean
        required: false
      count:
        type: number
        required: false
        default: 100
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
        .to_owned(),
    );

    let expanded = expand_jobs_with_reusables(&caller, &reusable).unwrap();
    let jobs = expanded.jobs;
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        jobs[0].inputs.get("name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(jobs[0].inputs.get("enable"), Some(&Value::Bool(true)));
    assert_eq!(jobs[0].inputs.get("count"), Some(&Value::Number(42.into())));
}

#[test]
fn reusable_workflow_missing_required_input() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
"#,
    )
    .unwrap();

    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
    inputs:
      name:
        type: string
        required: true
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
        .to_owned(),
    );

    let res = expand_jobs_with_reusables(&caller, &reusable);
    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err(),
        ParserError::MissingRequiredInput { .. }
    ));
}

#[test]
fn reusable_workflow_undeclared_input() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
    with:
      age: 25
"#,
    )
    .unwrap();

    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
    inputs:
      name:
        type: string
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo reusable
"#
        .to_owned(),
    );

    let res = expand_jobs_with_reusables(&caller, &reusable);
    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err(),
        ParserError::UndeclaredInput { .. }
    ));
}

#[test]
fn reusable_workflow_max_depth_exceeded() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  call1:
    uses: ./.github/workflows/level1.yml
"#,
    )
    .unwrap();

    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/level1.yml".to_owned(),
        "on: { workflow_call: {} }\njobs:\n  call2:\n    uses: ./.github/workflows/level2.yml"
            .to_owned(),
    );
    reusable.insert(
        ".github/workflows/level2.yml".to_owned(),
        "on: { workflow_call: {} }\njobs:\n  call3:\n    uses: ./.github/workflows/level3.yml"
            .to_owned(),
    );
    reusable.insert(
        ".github/workflows/level3.yml".to_owned(),
        "on: { workflow_call: {} }\njobs:\n  call4:\n    uses: ./.github/workflows/level4.yml"
            .to_owned(),
    );
    reusable.insert(
        ".github/workflows/level4.yml".to_owned(),
        "on: { workflow_call: {} }\njobs:\n  call5:\n    uses: ./.github/workflows/level5.yml"
            .to_owned(),
    );
    reusable.insert(
            ".github/workflows/level5.yml".to_owned(),
            "on: { workflow_call: {} }\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo leaf".to_owned(),
        );

    let res = expand_jobs_with_reusables(&caller, &reusable);
    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err(),
        ParserError::MaxNestingDepthExceeded
    ));
}

#[test]
fn reusable_workflow_outer_needs_propagated() {
    let caller = parse_workflow(
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo build
  call:
    needs: build
    uses: ./.github/workflows/reusable.yml
"#,
    )
    .unwrap();

    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        r#"
on:
  workflow_call:
jobs:
  test1:
    runs-on: ubuntu-latest
    steps:
      - run: echo test1
  test2:
    needs: test1
    runs-on: ubuntu-latest
    steps:
      - run: echo test2
"#
        .to_owned(),
    );

    let expanded = expand_jobs_with_reusables(&caller, &reusable).unwrap();
    let jobs = expanded.jobs;
    assert_eq!(jobs.len(), 3);
    let test1 = jobs.iter().find(|j| j.id.0 == "call/test1").unwrap();
    let test2 = jobs.iter().find(|j| j.id.0 == "call/test2").unwrap();

    assert!(test1.needs.contains(&JobId("build".to_string())));
    assert!(test2.needs.contains(&JobId("call/test1".to_string())));
    assert!(test2.needs.contains(&JobId("build".to_string())));
}

mod coerce_value_properties {
    use super::coerce_value;
    use crate::InputType;
    use proptest::prelude::*;
    use serde_json::Value;

    /// Values that should be accepted for boolean coercion.
    fn arb_valid_bool_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            Just(Value::String("true".into())),
            Just(Value::String("false".into())),
            Just(Value::String("TRUE".into())),
            Just(Value::String("False".into())),
            Just(Value::String("${{ inputs.x }}".into())),
        ]
    }

    /// Values that should be accepted for number coercion.
    fn arb_valid_num_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
            Just(Value::String("42".into())),
            Just(Value::String("-7".into())),
            Just(Value::String("3.14".into())),
            Just(Value::String("${{ inputs.x }}".into())),
        ]
    }

    proptest! {
        /// Idempotence: coercing an already-valid value twice = coercing once.
        #[test]
        fn coercion_idempotent_bool(val in arb_valid_bool_value()) {
            let once = coerce_value(&val, InputType::Boolean, "test").unwrap();
            let twice = coerce_value(&once, InputType::Boolean, "test").unwrap();
            prop_assert_eq!(once, twice);
        }

        #[test]
        fn coercion_idempotent_number(val in arb_valid_num_value()) {
            let once = coerce_value(&val, InputType::Number, "test").unwrap();
            let twice = coerce_value(&once, InputType::Number, "test").unwrap();
            prop_assert_eq!(once, twice);
        }

        /// Never panics — any value + any type produces a Result (Ok or Err), never a panic.
        #[test]
        fn coercion_never_panics(val in prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
            r#"[a-zA-Z0-9_ -]{0,30}"#.prop_map(Value::String),
        ]) {
            let _ = coerce_value(&val, InputType::Boolean, "test");
            let _ = coerce_value(&val, InputType::Number, "test");
            let _ = coerce_value(&val, InputType::String, "test");
        }

        /// Boolean coercion of valid values produces only Bool (or expression passthrough).
        #[test]
        fn boolean_coercion_always_bool(val in arb_valid_bool_value()) {
            let result = coerce_value(&val, InputType::Boolean, "test").unwrap();
            // Expression passthrough stays as string
            if !val.as_str().is_some_and(|s| s.starts_with("${{")) {
                prop_assert!(result.is_boolean(),
                    "boolean coercion of {:?} produced {:?}", val, result);
            }
        }

        /// Number coercion of valid values produces only Number (or expression passthrough).
        #[test]
        fn number_coercion_always_number(val in arb_valid_num_value()) {
            let result = coerce_value(&val, InputType::Number, "test").unwrap();
            if !val.as_str().is_some_and(|s| s.starts_with("${{")) {
                prop_assert!(result.is_number(),
                    "number coercion of {:?} produced {:?}", val, result);
            }
        }

        /// String coercion always succeeds.
        #[test]
        fn string_coercion_always_ok(val in prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            (-1000i64..1000i64).prop_map(|n| Value::Number(n.into())),
            r#"[a-zA-Z0-9_ -]{0,30}"#.prop_map(Value::String),
        ]) {
            let result = coerce_value(&val, InputType::String, "test");
            prop_assert!(result.is_ok(), "string coercion should always succeed");
            prop_assert!(result.unwrap().is_string());
        }

        /// Roundtrip: bool→string→bool preserves truth value.
        #[test]
        fn bool_string_bool_roundtrip(val in arb_valid_bool_value()) {
            if let Ok(b) = coerce_value(&val, InputType::Boolean, "test") {
                let s = coerce_value(&b, InputType::String, "test").unwrap();
                if let Ok(b2) = coerce_value(&s, InputType::Boolean, "test") {
                    if b.is_boolean() {
                        prop_assert_eq!(b, b2);
                    }
                }
            }
        }
    }

    // Deterministic rejection tests matching GitHub behavior.

    #[test]
    fn rejects_arbitrary_string_as_boolean() {
        let result = coerce_value(&Value::String("0".into()), InputType::Boolean, "flag");
        assert!(result.is_err(), "\"0\" should be rejected for boolean");
        let result = coerce_value(&Value::String("sure".into()), InputType::Boolean, "flag");
        assert!(result.is_err(), "\"sure\" should be rejected for boolean");
        let result = coerce_value(&Value::String("yes".into()), InputType::Boolean, "flag");
        assert!(result.is_err(), "\"yes\" should be rejected for boolean");
    }

    #[test]
    fn rejects_number_as_boolean() {
        let result = coerce_value(&Value::Number(1.into()), InputType::Boolean, "flag");
        assert!(result.is_err(), "number 1 should be rejected for boolean");
    }

    #[test]
    fn rejects_null_as_boolean() {
        let result = coerce_value(&Value::Null, InputType::Boolean, "flag");
        assert!(result.is_err(), "null should be rejected for boolean");
    }

    #[test]
    fn rejects_arbitrary_string_as_number() {
        let result = coerce_value(&Value::String("abc".into()), InputType::Number, "count");
        assert!(result.is_err(), "\"abc\" should be rejected for number");
    }

    #[test]
    fn rejects_bool_as_number() {
        let result = coerce_value(&Value::Bool(true), InputType::Number, "count");
        assert!(result.is_err(), "bool should be rejected for number");
    }

    #[test]
    fn rejects_null_as_number() {
        let result = coerce_value(&Value::Null, InputType::Number, "count");
        assert!(result.is_err(), "null should be rejected for number");
    }

    #[test]
    fn accepts_true_false_strings_for_boolean() {
        assert_eq!(
            coerce_value(&Value::String("true".into()), InputType::Boolean, "f").unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            coerce_value(&Value::String("false".into()), InputType::Boolean, "f").unwrap(),
            Value::Bool(false)
        );
        assert_eq!(
            coerce_value(&Value::String("TRUE".into()), InputType::Boolean, "f").unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn accepts_numeric_strings_for_number() {
        assert_eq!(
            coerce_value(&Value::String("42".into()), InputType::Number, "n").unwrap(),
            Value::Number(42.into())
        );
        assert_eq!(
            coerce_value(&Value::String("-7".into()), InputType::Number, "n").unwrap(),
            Value::Number((-7).into())
        );
    }

    #[test]
    fn expression_passthrough() {
        let expr = Value::String("${{ inputs.x }}".into());
        assert_eq!(coerce_value(&expr, InputType::Boolean, "f").unwrap(), expr);
        assert_eq!(coerce_value(&expr, InputType::Number, "n").unwrap(), expr);
        assert_eq!(coerce_value(&expr, InputType::String, "s").unwrap(), expr);
    }
}
#[test]
fn preserves_job_output_expressions() {
    let workflow = parse_workflow(
        r#"jobs:
  producer:
    runs-on: self-hosted
    outputs:
      value: ${{ steps.gen.outputs.value }}
    steps:
      - id: gen
        run: echo value=42 >> "$GITHUB_OUTPUT"
"#,
    )
    .unwrap();
    let plans = expand_jobs(&workflow).unwrap();
    assert_eq!(
        plans[0].job_outputs.get("value").map(String::as_str),
        Some("${{ steps.gen.outputs.value }}")
    );
}
#[test]
fn concurrency_bare_string_shorthand() {
    let wf = parse_workflow(
        r#"
on: push
concurrency: ci-${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
    )
    .unwrap();
    let c = wf.concurrency.unwrap();
    assert_eq!(c.group, "ci-${{ github.ref }}");
    assert_eq!(c.cancel_in_progress, None);
    assert_eq!(c.queue, ConcurrencyQueue::Single);
}

#[test]
fn concurrency_mapping_form() {
    let wf = parse_workflow(
        r#"
on: push
concurrency:
  group: g
  cancel-in-progress: true
  queue: single
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
    )
    .unwrap();
    let c = wf.concurrency.unwrap();
    assert_eq!(c.group, "g");
    assert_eq!(c.cancel_in_progress.as_deref(), Some("true"));
}

#[test]
fn concurrency_preserves_expression_cancel() {
    let wf = parse_workflow(
        r#"
on: push
concurrency:
  group: g
  cancel-in-progress: ${{ github.ref == 'refs/heads/main' }}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
    )
    .unwrap();
    let c = wf.concurrency.unwrap();
    assert_eq!(
        c.cancel_in_progress.as_deref(),
        Some("${{ github.ref == 'refs/heads/main' }}")
    );
}

#[test]
fn concurrency_queue_max_with_literal_cancel_is_error() {
    let err = parse_workflow(
        r#"
on: push
concurrency:
  group: g
  cancel-in-progress: true
  queue: max
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#,
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("queue: max") && msg.contains("cancel-in-progress"),
        "unexpected error: {msg}"
    );
}

#[test]
fn job_level_concurrency_on_plan() {
    let wf = parse_workflow(
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    concurrency:
      group: jg
      cancel-in-progress: false
    steps:
      - run: echo hi
"#,
    )
    .unwrap();
    let plans = expand_jobs(&wf).unwrap();
    assert_eq!(plans[0].concurrency_group.as_deref(), Some("jg"));
    assert_eq!(
        plans[0].concurrency_cancel_in_progress.as_deref(),
        Some("false")
    );
}
