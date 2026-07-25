//! Needs-graph validation (DAG properties) for workflow jobs.
//!
//! Oracle source: GitHub Actions workflow syntax, `jobs.<job_id>.needs`:
//! <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
//! The parser properties encode the contract that every `needs` name exists,
//! cycles are rejected during validation, and every topological layer appears
//! after all of its dependencies. The implementation algorithm is an internal
//! choice; GitHub's private control-plane algorithm is not assumed.
use aksh_gha_protocol::JobPlan;
use std::collections::BTreeMap;

use crate::ParserError;

/// A needs edge list: job id → dependency job ids (as written in YAML).
pub type NeedsGraph = BTreeMap<String, Vec<String>>;

/// Cycle detection error with a witness node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeedsCycle {
    /// A job id that participates in a cycle.
    pub witness: String,
}

/// Detect a cycle in a needs graph via three-color DFS.
pub fn detect_needs_cycle(edges: &NeedsGraph) -> Result<(), NeedsCycle> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut color: BTreeMap<&str, Color> =
        edges.keys().map(|k| (k.as_str(), Color::White)).collect();
    for deps in edges.values() {
        for d in deps {
            color.entry(d.as_str()).or_insert(Color::White);
        }
    }

    fn visit<'a>(
        node: &'a str,
        edges: &'a NeedsGraph,
        color: &mut BTreeMap<&'a str, Color>,
    ) -> Result<(), NeedsCycle> {
        color.insert(node, Color::Gray);
        if let Some(deps) = edges.get(node) {
            for dep in deps {
                match color.get(dep.as_str()).copied().unwrap_or(Color::White) {
                    Color::Gray => {
                        return Err(NeedsCycle {
                            witness: dep.clone(),
                        });
                    }
                    Color::White => visit(dep.as_str(), edges, color)?,
                    Color::Black => {}
                }
            }
        }
        color.insert(node, Color::Black);
        Ok(())
    }

    let nodes: Vec<&str> = color.keys().copied().collect();
    for node in nodes {
        if color.get(node) == Some(&Color::White) {
            visit(node, edges, &mut color)?;
        }
    }
    Ok(())
}

/// Build a needs graph from expanded job plans (base ids + needs).
pub fn needs_graph_from_pairs(jobs: &[(String, Vec<String>)]) -> NeedsGraph {
    let mut edges = NeedsGraph::new();
    for (id, needs) in jobs {
        edges.entry(id.clone()).or_default().extend(needs.clone());
    }
    edges
}
/// Validate dependency references and cycles after matrix/reusable expansion.
///
/// A declared need may name one concrete expanded job or a base job, in which
/// case it expands to every matrix sibling with that base id. Validation runs
/// before server state is mutated, so invalid workflows cannot queue jobs.
pub fn validate_job_plans(jobs: &[JobPlan]) -> Result<(), ParserError> {
    let mut graph = NeedsGraph::new();

    for job in jobs {
        if let Some(condition) = &job.if_condition {
            let effective = aksh_gha_expressions::effective_condition(Some(condition));
            aksh_gha_expressions::validate_expression(&effective).map_err(|error| {
                ParserError::InvalidJobCondition {
                    job_id: job.id.0.clone(),
                    message: error.to_string(),
                }
            })?;
        }
        let mut expanded_needs = Vec::new();
        for need in &job.needs {
            let matches = jobs
                .iter()
                .filter(|candidate| candidate.id == *need || candidate.base_id == need.0)
                .map(|candidate| candidate.id.0.clone())
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(ParserError::UnknownNeed {
                    job_id: job.id.0.clone(),
                    need: need.0.clone(),
                });
            }
            expanded_needs.extend(matches);
        }
        expanded_needs.sort();
        expanded_needs.dedup();
        graph.insert(job.id.0.clone(), expanded_needs);
    }

    detect_needs_cycle(&graph).map_err(|cycle| ParserError::NeedsCycle {
        witness: cycle.witness,
    })
}

/// Kahn topological layers: jobs whose needs are subset of already-emitted.
/// Returns `None` if a cycle prevents a full order.
pub fn topo_layers(edges: &NeedsGraph) -> Option<Vec<Vec<String>>> {
    use std::collections::BTreeSet;

    let mut remaining: BTreeMap<String, BTreeSet<String>> = edges
        .iter()
        .map(|(k, v)| (k.clone(), v.iter().cloned().collect()))
        .collect();
    // Ensure referenced deps exist as nodes.
    let deps: Vec<String> = remaining.values().flat_map(|d| d.iter().cloned()).collect();
    for d in deps {
        remaining.entry(d).or_default();
    }

    let mut layers = Vec::new();
    let mut done: BTreeSet<String> = BTreeSet::new();

    while done.len() < remaining.len() {
        let layer: Vec<String> = remaining
            .iter()
            .filter(|(id, needs)| !done.contains(*id) && needs.iter().all(|n| done.contains(n)))
            .map(|(id, _)| id.clone())
            .collect();
        if layer.is_empty() {
            return None;
        }
        for id in &layer {
            done.insert(id.clone());
        }
        layers.push(layer);
    }
    Some(layers)
}

/// Compute the transitive dependency closure for a set of root job ids.
///
/// Given a needs graph and one or more root ids (the jobs the user selected),
/// returns the set of all job ids that must run: the roots themselves plus
/// every transitive `needs:` dependency. Unknown root ids are silently
/// included (validation happens later when matching against expanded jobs).
pub fn dependency_closure(
    edges: &NeedsGraph,
    roots: &[String],
) -> std::collections::BTreeSet<String> {
    use std::collections::BTreeSet;
    let mut closed = BTreeSet::new();
    let mut stack: Vec<String> = roots.to_vec();
    while let Some(node) = stack.pop() {
        if closed.insert(node.clone()) {
            if let Some(deps) = edges.get(&node) {
                for dep in deps {
                    if !closed.contains(dep) {
                        stack.push(dep.clone());
                    }
                }
            }
        }
    }
    closed
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::RngSeed;

    /// Deterministic proptest config for DAG validation tests.
    /// Fixed seed ensures reproducibility; failure persistence and verbose
    /// reporting preserve/report the seed on failure (docs/property-tests.md §Common).
    fn dag_config(cases: u32) -> ProptestConfig {
        ProptestConfig {
            cases,
            rng_seed: RngSeed::Fixed(20250713),
            verbose: 1,
            ..ProptestConfig::default()
        }
    }

    #[test]
    fn rejects_self_loop() {
        let mut g = NeedsGraph::new();
        g.insert("a".into(), vec!["a".into()]);
        assert!(detect_needs_cycle(&g).is_err());
    }

    #[test]
    fn accepts_diamond() {
        let mut g = NeedsGraph::new();
        g.insert("a".into(), vec![]);
        g.insert("b".into(), vec!["a".into()]);
        g.insert("c".into(), vec!["a".into()]);
        g.insert("d".into(), vec!["b".into(), "c".into()]);
        assert!(detect_needs_cycle(&g).is_ok());
        let layers = topo_layers(&g).unwrap();
        assert_eq!(layers[0], vec!["a".to_string()]);
        assert!(layers.last().unwrap().contains(&"d".to_string()));
    }

    /// Generate a DAG by only allowing edges to lower indices.
    fn arb_dag_edges(n: usize) -> impl Strategy<Value = NeedsGraph> {
        proptest::collection::vec(any::<u8>(), n..=n).prop_map(move |masks| {
            (0..n)
                .map(|i| {
                    let mut needs = Vec::new();
                    if i > 0 {
                        let mut seen = std::collections::BTreeSet::new();
                        for d in 0..i {
                            if masks[i] & (1u8 << (d % 8)) != 0 && seen.insert(d) {
                                needs.push(format!("j{d}"));
                            }
                        }
                    }
                    (format!("j{i}"), needs)
                })
                .collect()
        })
    }

    // Oracle: `jobs.<job_id>.needs` workflow syntax contract. These
    // generated graphs verify acyclicity, topological dependency order,
    // and cycle rejection independently of server scheduling.
    proptest! {
        #![proptest_config(dag_config(10_000))]

        #[test]
        fn generated_dags_are_acyclic(g in arb_dag_edges(6)) {
            prop_assert!(detect_needs_cycle(&g).is_ok());
            prop_assert!(topo_layers(&g).is_some());
        }

        #[test]
        fn topo_layers_respect_needs(g in arb_dag_edges(5)) {
            let Some(layers) = topo_layers(&g) else {
                prop_assert!(false, "expected layers");
                return Ok(());
            };
            let mut rank = BTreeMap::new();
            for (i, layer) in layers.iter().enumerate() {
                for id in layer {
                    rank.insert(id.clone(), i);
                }
            }
            for (id, needs) in &g {
                for n in needs {
                    if let (Some(&ri), Some(&rn)) = (rank.get(id), rank.get(n)) {
                        prop_assert!(rn < ri, "{n} must precede {id}");
                    }
                }
            }
        }

        #[test]
        fn cycle_detector_flags_mutual_edge(a in "[a-z]{1,3}", b in "[a-z]{1,3}") {
            // Force distinct endpoints without reject-heavy prop_assume.
            let b = if a == b { format!("{b}_x") } else { b };
            let mut g = NeedsGraph::new();
            g.insert(a.clone(), vec![b.clone()]);
            g.insert(b.clone(), vec![a.clone()]);
            prop_assert!(detect_needs_cycle(&g).is_err());
            prop_assert!(topo_layers(&g).is_none());
        }
    }

    // ─── Unknown/cycle prequeue rejection (spec §1.12, §1 regression) ───────

    fn job_plan(id: &str, base: &str, needs: &[&str]) -> JobPlan {
        JobPlan {
            id: aksh_gha_protocol::JobId(id.to_owned()),
            base_id: base.to_owned(),
            name: id.to_owned(),
            runner_group: None,
            runs_on: vec!["ubuntu-latest".to_owned()],
            needs: needs
                .iter()
                .map(|n| aksh_gha_protocol::JobId(n.to_string()))
                .collect(),
            matrix: Default::default(),
            env: Default::default(),
            steps: vec![],
            if_condition: None,
            continue_on_error: false,
            fail_fast: true,
            max_parallel: None,
            secrets_inherit: false,
            container: None,
            services: None,
            inputs: Default::default(),
            workflow_file: None,
            workflow_ref: None,
            workflow_sha: None,
            workflow_repository: None,
            secrets_map: Default::default(),
            job_outputs: Default::default(),
            oidc_id_token_granted: false,
            oidc_environment: None,
            oidc_job_workflow_ref: None,
            concurrency_group: None,
            concurrency_cancel_in_progress: None,
            concurrency_queue: None,
        }
    }

    /// Unknown need is rejected before dispatch (spec §1.12).
    #[test]
    fn validate_rejects_unknown_need() {
        let plans = vec![
            job_plan("build", "build", &[]),
            job_plan("test", "test", &["nonexistent"]),
        ];
        let err = validate_job_plans(&plans).unwrap_err();
        match err {
            ParserError::UnknownNeed { job_id, need } => {
                assert_eq!(job_id, "test");
                assert_eq!(need, "nonexistent");
            }
            other => panic!("expected UnknownNeed, got {other:?}"),
        }
    }

    /// Cyclic graph is rejected before dispatch (spec §1.12).
    #[test]
    fn validate_rejects_cycle_before_dispatch() {
        let plans = vec![job_plan("a", "a", &["b"]), job_plan("b", "b", &["a"])];
        let err = validate_job_plans(&plans).unwrap_err();
        match err {
            ParserError::NeedsCycle { witness } => {
                assert!(
                    witness == "a" || witness == "b",
                    "witness should be a cycle participant, got {witness}"
                );
            }
            other => panic!("expected NeedsCycle, got {other:?}"),
        }
    }

    /// Three-node cycle: a → b → c → a.
    #[test]
    fn validate_rejects_three_node_cycle() {
        let plans = vec![
            job_plan("a", "a", &["c"]),
            job_plan("b", "b", &["a"]),
            job_plan("c", "c", &["b"]),
        ];
        assert!(matches!(
            validate_job_plans(&plans),
            Err(ParserError::NeedsCycle { .. })
        ));
    }

    /// Valid DAG passes validation.
    #[test]
    fn validate_accepts_valid_dag() {
        let plans = vec![
            job_plan("build", "build", &[]),
            job_plan("test", "test", &["build"]),
            job_plan("deploy", "deploy", &["build", "test"]),
        ];
        assert!(validate_job_plans(&plans).is_ok());
    }

    /// Base-id matching: needing "build" resolves to matrix siblings
    /// "build (1)" and "build (2)".
    #[test]
    fn validate_resolves_base_id_needs() {
        let plans = vec![
            job_plan("build (1)", "build", &[]),
            job_plan("build (2)", "build", &[]),
            job_plan("test", "test", &["build"]),
        ];
        assert!(validate_job_plans(&plans).is_ok());
    }

    /// Generate graphs with REVERSE edges (higher → lower indices) to exercise
    /// parser rejection. This is NOT acyclic by construction.
    fn arb_graph_with_reverse_edges(n: usize) -> impl Strategy<Value = NeedsGraph> {
        proptest::collection::vec(any::<u8>(), n..=n).prop_map(move |masks| {
            (0..n)
                .map(|i| {
                    let mut needs = Vec::new();
                    // Allow edges to ANY node, including higher indices
                    for d in 0..n {
                        if d != i && masks[i] & (1u8 << (d % 8)) != 0 {
                            needs.push(format!("j{d}"));
                        }
                    }
                    (format!("j{i}"), needs)
                })
                .collect()
        })
    }

    // Oracle cross-check: parser cycle detection and topological layers
    // must agree on every generated graph; this checks consistency, not
    // a claim that GitHub uses this particular algorithm.
    proptest! {
        #![proptest_config(dag_config(5_000))]

        /// Graphs with reverse edges: cycle detection and topo_layers agree.
        #[test]
        fn reverse_edge_graphs_agree(g in arb_graph_with_reverse_edges(5)) {
            let cycle_result = detect_needs_cycle(&g);
            let topo_result = topo_layers(&g);
            match (&cycle_result, &topo_result) {
                (Ok(()), Some(layers)) => {
                    // Acyclic: layers must cover all nodes
                    let total: usize = layers.iter().map(|l| l.len()).sum();
                    prop_assert_eq!(total, g.len() + g.values().flatten()
                        .filter(|d| !g.contains_key(*d)).count());
                }
                (Err(_), None) => { /* both agree it's cyclic */ }
                (Ok(()), None) => {
                    prop_assert!(false, "cycle detector says ok but topo failed");
                }
                (Err(_), Some(_)) => {
                    prop_assert!(false, "cycle detector says cycle but topo succeeded");
                }
            }
        }
    }

    #[test]
    fn dependency_closure_single_root_no_deps() {
        let g = needs_graph_from_pairs(&[
            ("lint".into(), vec![]),
            ("build".into(), vec![]),
            ("test".into(), vec!["build".into()]),
        ]);
        let closed = dependency_closure(&g, &["lint".into()]);
        assert_eq!(closed, ["lint".into()].into());
    }

    #[test]
    fn dependency_closure_transitive() {
        let g = needs_graph_from_pairs(&[
            ("lint".into(), vec![]),
            ("build".into(), vec!["lint".into()]),
            ("test".into(), vec!["build".into()]),
            ("deploy".into(), vec!["test".into()]),
        ]);
        let closed = dependency_closure(&g, &["test".into()]);
        assert_eq!(
            closed,
            ["lint", "build", "test"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    #[test]
    fn dependency_closure_diamond() {
        let g = needs_graph_from_pairs(&[
            ("a".into(), vec![]),
            ("b".into(), vec!["a".into()]),
            ("c".into(), vec!["a".into()]),
            ("d".into(), vec!["b".into(), "c".into()]),
        ]);
        let closed = dependency_closure(&g, &["d".into()]);
        assert_eq!(
            closed,
            ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect()
        );
    }

    #[test]
    fn dependency_closure_unknown_root_included() {
        let g = needs_graph_from_pairs(&[("a".into(), vec![])]);
        let closed = dependency_closure(&g, &["unknown".into()]);
        assert!(closed.contains("unknown"));
        assert!(!closed.contains("a"));
    }
}
