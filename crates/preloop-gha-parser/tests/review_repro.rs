//!  reproductions for dynamic-matrix fidelity bugs.
//!
//! Each test encodes the behavior GitHub produces. A failure here is the bug.

use preloop_gha_parser::{expand_deferred_matrix_job, parse_workflow};
use std::collections::BTreeMap;

const WORKFLOW: &str = r#"
name: dyn
on: push
jobs:
  seed:
    runs-on: ubuntu-latest
    steps:
      - run: echo seed
  build:
    needs: seed
    runs-on: ubuntu-latest
    strategy:
      matrix: ${{ fromJSON(needs.seed.outputs.spec) }}
    steps:
      - run: echo build
"#;

fn expand(spec: serde_json::Value) -> Vec<String> {
    let workflow = parse_workflow(WORKFLOW).expect("workflow parses");
    let mut seed_outputs = BTreeMap::new();
    seed_outputs.insert("spec".to_owned(), spec);
    let mut needs = BTreeMap::new();
    needs.insert("seed".to_owned(), seed_outputs);
    expand_deferred_matrix_job(
        &workflow,
        "build",
        "fromJSON(needs.seed.outputs.spec)",
        &needs,
        None,
    )
    .expect("expansion succeeds")
    .into_iter()
    .map(|plan| plan.name)
    .collect()
}

/// GitHub names a matrix job by walking the axes in *declaration* order.
/// `JobNameBuilder` consumes the matrix mapping in document order, so a spec
/// declaring `os` before `arch` yields `build (ubuntu-latest, x64)`.
#[test]
fn dynamic_matrix_axis_order_follows_declaration_not_alphabet() {
    let names = expand(serde_json::json!({
        "os": ["ubuntu-latest"],
        "arch": ["x64"],
    }));
    assert_eq!(names, vec!["build (ubuntu-latest, x64)".to_owned()]);
}

/// Same spec, reversed declaration order, must reverse the rendered name.
/// If both orderings render identically the axis order is being discarded.
#[test]
fn dynamic_matrix_axis_order_is_not_canonicalized() {
    let os_first = expand(serde_json::json!({
        "os": ["ubuntu-latest"],
        "arch": ["x64"],
    }));
    let arch_first = expand(serde_json::json!({
        "arch": ["x64"],
        "os": ["ubuntu-latest"],
    }));
    assert_ne!(
        os_first, arch_first,
        "declaration order must survive into the job name"
    );
}

/// GitHub's schema requires `strategy.matrix` to be a mapping. A bare JSON
/// array is a workflow error, not an implicit `include:` list.
#[test]
fn dynamic_matrix_rejects_bare_top_level_array() {
    let workflow = parse_workflow(WORKFLOW).expect("workflow parses");
    let mut seed_outputs = BTreeMap::new();
    seed_outputs.insert(
        "spec".to_owned(),
        serde_json::json!([{"os": "ubuntu-latest"}, {"os": "windows-latest"}]),
    );
    let mut needs = BTreeMap::new();
    needs.insert("seed".to_owned(), seed_outputs);
    let result = expand_deferred_matrix_job(
        &workflow,
        "build",
        "fromJSON(needs.seed.outputs.spec)",
        &needs,
        None,
    );
    assert!(
        result.is_err(),
        "a bare array is not a valid matrix mapping, got {:?}",
        result.map(|plans| plans.into_iter().map(|plan| plan.name).collect::<Vec<_>>())
    );
}
