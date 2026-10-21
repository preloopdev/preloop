//! Reproductions for the OIDC provider pentest sweep (2026-10-21).
//!
//! Each test encodes the behavior GitHub / preloop's own permission model
//! requires. A failure here is the bug.

use preloop_gha_parser::{
    cap_deferred_matrix_plans, expand_deferred_matrix_job, expand_jobs_with_reusables,
    expand_reusable_call, parse_workflow,
};
use std::collections::BTreeMap;

const CALLER_NO_GRANT: &str = r#"
on: push
jobs:
  call:
    uses: ./.github/workflows/reusable.yml
"#;

const CALLEE_DEFERRED_MATRIX: &str = r#"
on:
  workflow_call:
jobs:
  setup:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.m.outputs.matrix }}
    steps:
      - id: m
        run: echo setup
  deploy:
    needs: setup
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    strategy:
      matrix: ${{ fromJSON(needs.setup.outputs.matrix) }}
    steps:
      - run: echo deploy
"#;

/// F2: a reusable callee job whose matrix is deferred to runtime
/// (`needs.*`-driven) must stay capped by the caller's grant, exactly like a
/// statically expanded callee job. The static path ANDs the caller's grant
/// into every callee plan (`expand_reusable_call`); the runtime fan-out
/// (`expand_deferred_matrix_job`, driven by `control/logic.rs`) recomputes
/// the grant from the callee's permissions alone.
#[test]
fn deferred_matrix_legs_in_reusable_callee_keep_caller_oidc_cap() {
    let caller = parse_workflow(CALLER_NO_GRANT).unwrap();
    let mut reusable = BTreeMap::new();
    reusable.insert(
        ".github/workflows/reusable.yml".to_owned(),
        CALLEE_DEFERRED_MATRIX.to_owned(),
    );

    let caller_node = expand_jobs_with_reusables(&caller, &reusable).unwrap().jobs;
    assert_eq!(caller_node.len(), 1);
    assert!(
        !caller_node[0].oidc_id_token_granted,
        "the caller never declared id-token: write"
    );

    let called = parse_workflow(CALLEE_DEFERRED_MATRIX).unwrap();
    let inner = expand_reusable_call(&called, &caller_node[0], &reusable, &BTreeMap::new())
        .unwrap()
        .jobs;
    let placeholder = inner
        .iter()
        .find(|j| j.base_id.ends_with("deploy"))
        .expect("the callee deploy job must be planned");
    assert!(
        placeholder.deferred_matrix.is_some(),
        "the needs-driven matrix job must be a deferred placeholder at static expansion"
    );
    assert!(
        !placeholder.oidc_id_token_granted,
        "the static placeholder must already carry the caller's cap"
    );

    // Runtime fan-out, exactly as control/logic.rs drives it: expand the
    // legs from the callee workflow, then cap them at the stored node plan.
    let mut setup_outputs = BTreeMap::new();
    setup_outputs.insert(
        "matrix".to_owned(),
        serde_json::json!({"include": [{"target": "a"}, {"target": "b"}]}),
    );
    let mut needs_outputs = BTreeMap::new();
    needs_outputs.insert("setup".to_owned(), setup_outputs);
    let mut legs = expand_deferred_matrix_job(
        &called,
        "call/deploy",
        "fromJSON(needs.setup.outputs.matrix)",
        &needs_outputs,
        None,
    )
    .expect("the deferred matrix must expand");
    cap_deferred_matrix_plans(&mut legs, placeholder);
    assert_eq!(legs.len(), 2, "two matrix cells must fan out");
    for leg in &legs {
        assert!(
            !leg.oidc_id_token_granted,
            "leg {} must inherit the caller's withheld id-token grant",
            leg.name
        );
    }
}
