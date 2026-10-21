//! Reproductions for the OIDC provider pentest sweep (2026-10-21).
//!
//! Each test encodes the behavior GitHub / preloop's own permission model
//! requires. A failure here is the bug.

use preloop_gha_parser::{expand_jobs, parse_workflow};

/// F3 (parser half): an environment name that merely *contains* a matrix
/// expression must be resolved per cell — the matrix is known at expansion —
/// never recorded verbatim (before the fix, the literal string
/// `prod-${{ matrix.target }}` became the environment name in the minted
/// token's claims and `sub`).
#[test]
fn embedded_matrix_expression_in_environment_name_is_resolved() {
    let workflow = parse_workflow(
        r#"
on: push
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: prod-${{ matrix.target }}
    strategy:
      matrix:
        target: [a, b]
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();
    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs.len(), 2);
    let mut names: Vec<Option<&str>> = jobs.iter().map(|j| j.oidc_environment.as_deref()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![Some("prod-a"), Some("prod-b")],
        "each cell's environment name must have its matrix expression resolved"
    );
}

/// An environment name whose expression the parser cannot resolve (anything
/// but `matrix.*`) records no environment at expansion: the raw template
/// must never become the claimed environment name. Runtime hydration
/// resolves the name later and the recorded context follows it.
#[test]
fn unresolvable_expression_in_environment_name_records_none() {
    let workflow = parse_workflow(
        r#"
on: push
permissions:
  id-token: write
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: ${{ vars.deploy_env }}
    steps:
      - run: echo deploy
"#,
    )
    .unwrap();
    let jobs = expand_jobs(&workflow).unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].oidc_environment, None);
}
