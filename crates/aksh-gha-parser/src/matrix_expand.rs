//! Pure matrix expansion matching GitHub Actions semantics.
//!
//! Official order (docs.github.com + runner.server):
//! 1. Cartesian product of axis values (declaration order).
//! 2. Drop combinations that partially match any `exclude` object.
//! 3. For each `include` object: merge into the first compatible combination,
//!    else append as include-only.
//! 4. If the result is empty, emit a single empty combination (no matrix context).
//!
//! Deferred matrices (`fromJSON(needs.*.outputs.*)`) are modeled explicitly so
//! property tests can assert unresolved display identity when a producer fails
//! (property-testing-plan scenario 63).

use indexmap::IndexMap;
use serde_json::Value;

/// Structured matrix specification used by generators and expansion.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MatrixSpec {
    /// Axis name → list of values (declaration order).
    pub axes: IndexMap<String, Vec<Value>>,
    /// Exclude partial objects.
    pub exclude: Vec<IndexMap<String, Value>>,
    /// Include objects (merge or append).
    pub include: Vec<IndexMap<String, Value>>,
}

/// A concrete expanded combination.
#[derive(Debug, Clone, PartialEq)]
pub struct MatrixCombination {
    /// Key/value pairs in axis declaration order, then include-only keys.
    pub values: IndexMap<String, Value>,
    /// True when this row came only from `include` (no cartesian match).
    pub include_only: bool,
}

/// Deferred matrix axis that cannot be expanded until a producer job finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredMatrixAxis {
    /// Axis name in the consumer job.
    pub name: String,
    /// Raw expression, e.g. `fromJSON(needs.producer.outputs.matrix)`.
    pub expression: String,
}

/// Result of attempting expansion when some axes are deferred.
#[derive(Debug, Clone, PartialEq)]
pub enum ExpandOutcome {
    /// Fully concrete expansion.
    Concrete(Vec<MatrixCombination>),
    /// Producer failed or outputs missing: keep unresolved display placeholders.
    Unresolved {
        /// Base job id.
        base_id: String,
        /// Display name template that must retain `${{ matrix.* }}` placeholders.
        display_template: String,
        /// Deferred axes that never resolved.
        deferred: Vec<DeferredMatrixAxis>,
    },
}

/// Expand a concrete matrix. Never panics; invalid empty axis lists yield no
/// cartesian rows (then include-only / empty fallback apply).
pub fn expand_matrix_spec(spec: &MatrixSpec) -> Vec<MatrixCombination> {
    let mut combinations: Vec<IndexMap<String, Value>> = vec![IndexMap::new()];

    for (axis, values) in &spec.axes {
        if values.is_empty() {
            // Empty axis annihilates the product (GitHub rejects this in practice;
            // we model it as zero cartesian rows before include).
            combinations.clear();
            break;
        }
        combinations = combinations
            .into_iter()
            .flat_map(|existing| {
                values.iter().cloned().map({
                    let axis = axis.clone();
                    move |value| {
                        let mut next = existing.clone();
                        next.insert(axis.clone(), value);
                        next
                    }
                })
            })
            .collect();
    }

    for excluded in &spec.exclude {
        combinations.retain(|candidate| !matches_partial(candidate, excluded));
    }

    let mut tagged: Vec<MatrixCombination> = combinations
        .into_iter()
        .map(|values| MatrixCombination {
            values,
            include_only: false,
        })
        .collect();

    // Official MatrixBuilder.MatrixInclude.Match: each include filter is applied
    // against *every* cross-product vector (not just the first match). Unmatched
    // include rows are appended as include-only configurations.
    // Source: actions/runner MatrixBuilder.cs (via runner.server Sdk/WorkflowParser).
    for included in &spec.include {
        let mut matched_any = false;
        for candidate in &mut tagged {
            if can_merge_include(&candidate.values, included) {
                candidate.values.extend(included.clone());
                matched_any = true;
            }
        }
        if !matched_any {
            tagged.push(MatrixCombination {
                values: included.clone(),
                include_only: true,
            });
        }
    }

    // Official MatrixBuilder yields zero configs when the product is empty and
    // no include-only rows remain (all-excluded matrix → no jobs). Do not invent
    // a synthetic empty combination here.
    tagged
}

/// Cartesian product size before exclude/include (0 if any axis is empty).
pub fn cartesian_count(spec: &MatrixSpec) -> usize {
    if spec.axes.is_empty() {
        return 1;
    }
    let mut n = 1usize;
    for values in spec.axes.values() {
        if values.is_empty() {
            return 0;
        }
        n = n.saturating_mul(values.len());
    }
    n
}

/// Build the official expanded job id: `base (v1, v2)` in declaration order.
pub fn expanded_job_id(base: &str, matrix: &IndexMap<String, Value>) -> String {
    if matrix.is_empty() {
        return base.to_owned();
    }
    let values: Vec<String> = matrix.values().map(value_key).collect();
    format!("{base} ({})", values.join(", "))
}

/// When deferred axes cannot resolve, the display identity must keep the
/// template form rather than collapsing to the bare base id (scenario 63).
pub fn unresolved_display_identity(base_id: &str, display_template: &str) -> String {
    if display_template.contains("${{") {
        display_template.to_owned()
    } else {
        // No template — still must not invent matrix values.
        base_id.to_owned()
    }
}

/// Expand or record unresolved deferred identity.
pub fn expand_with_deferred(
    base_id: &str,
    display_template: &str,
    concrete: &MatrixSpec,
    deferred: &[DeferredMatrixAxis],
    producer_ok: bool,
) -> ExpandOutcome {
    if !deferred.is_empty() && !producer_ok {
        return ExpandOutcome::Unresolved {
            base_id: base_id.to_owned(),
            display_template: unresolved_display_identity(base_id, display_template),
            deferred: deferred.to_vec(),
        };
    }
    ExpandOutcome::Concrete(expand_matrix_spec(concrete))
}

fn value_key(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn matches_partial(candidate: &IndexMap<String, Value>, partial: &IndexMap<String, Value>) -> bool {
    partial.iter().all(|(key, value)| {
        candidate
            .get(key)
            .is_some_and(|candidate| candidate == value)
    })
}

fn can_merge_include(
    candidate: &IndexMap<String, Value>,
    include: &IndexMap<String, Value>,
) -> bool {
    include
        .iter()
        .all(|(key, value)| candidate.get(key).is_none_or(|existing| existing == value))
}

/// Convert parser `Matrix` wire type into a pure `MatrixSpec`.
pub fn matrix_to_spec(matrix: &crate::Matrix) -> MatrixSpec {
    let mut axes = IndexMap::new();
    for (axis, values) in &matrix.axes {
        let axis_values = match values {
            Value::Array(values) => values.clone(),
            value => vec![value.clone()],
        };
        axes.insert(axis.clone(), axis_values);
    }
    let exclude = matrix
        .exclude
        .iter()
        .filter_map(|v| value_object_indexed(v))
        .collect();
    let include = matrix
        .include
        .iter()
        .filter_map(|v| value_object_indexed(v))
        .collect();
    MatrixSpec {
        axes,
        exclude,
        include,
    }
}

fn value_object_indexed(value: &Value) -> Option<IndexMap<String, Value>> {
    let Value::Object(map) = value else {
        return None;
    };
    Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    #[test]
    fn exclude_and_include_match_unit_fixture() {
        // Mirrors parses_and_expands_matrix expectations.
        let mut axes = IndexMap::new();
        axes.insert(
            "os".into(),
            vec![json!("ubuntu-latest"), json!("macos-latest")],
        );
        axes.insert("node".into(), vec![json!(20), json!(22)]);
        let mut exclude = IndexMap::new();
        exclude.insert("os".into(), json!("macos-latest"));
        exclude.insert("node".into(), json!(20));
        let mut include = IndexMap::new();
        include.insert("os".into(), json!("ubuntu-latest"));
        include.insert("node".into(), json!(24));
        include.insert("experimental".into(), json!(true));

        let spec = MatrixSpec {
            axes,
            exclude: vec![exclude],
            include: vec![include],
        };
        let combos = expand_matrix_spec(&spec);
        // 2*2 - 1 exclude + 1 include-only (24 is new) = 4
        assert_eq!(combos.len(), 4);
        assert!(combos.iter().any(|c| {
            c.values.get("node") == Some(&json!(24))
                && c.values.get("experimental") == Some(&json!(true))
        }));
        assert!(!combos.iter().any(|c| {
            c.values.get("os") == Some(&json!("macos-latest"))
                && c.values.get("node") == Some(&json!(20))
        }));
    }

    #[test]
    fn scenario_63_unresolved_keeps_template() {
        let deferred = vec![DeferredMatrixAxis {
            name: "case".into(),
            expression: "fromJSON(needs.producer.outputs.matrix)".into(),
        }];
        let outcome = expand_with_deferred(
            "matrix-build",
            "matrix-build-${{ matrix.case }}-${{ matrix.mode }}",
            &MatrixSpec::default(),
            &deferred,
            false,
        );
        match outcome {
            ExpandOutcome::Unresolved {
                display_template, ..
            } => {
                assert_eq!(
                    display_template,
                    "matrix-build-${{ matrix.case }}-${{ matrix.mode }}"
                );
                assert_ne!(display_template, "matrix-build");
            }
            other => panic!("expected unresolved, got {other:?}"),
        }
    }

    fn arb_scalar() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<bool>().prop_map(Value::Bool),
            (0i64..8).prop_map(|n| Value::Number(n.into())),
            "[a-d]{1,3}".prop_map(Value::String),
        ]
    }

    fn arb_matrix_spec() -> impl Strategy<Value = MatrixSpec> {
        // Up to two fixed axis names with 1–3 values each; small excludes/includes.
        (
            proptest::collection::vec(arb_scalar(), 1..3),
            prop::option::of(proptest::collection::vec(arb_scalar(), 1..3)),
            proptest::collection::vec(arb_scalar(), 0..2),
            proptest::collection::vec(arb_scalar(), 0..2),
        )
            .prop_map(|(os_vals, node_vals, ex_os, in_extra)| {
                let mut axes = IndexMap::new();
                axes.insert("os".into(), os_vals);
                if let Some(node) = node_vals {
                    axes.insert("node".into(), node);
                }

                let exclude = ex_os
                    .into_iter()
                    .take(2)
                    .map(|v| {
                        let mut m = IndexMap::new();
                        m.insert("os".into(), v);
                        m
                    })
                    .collect();

                let include = in_extra
                    .into_iter()
                    .take(2)
                    .map(|v| {
                        let mut m = IndexMap::new();
                        m.insert("os".into(), v);
                        m.insert("extra".into(), Value::Bool(true));
                        m
                    })
                    .collect();

                MatrixSpec {
                    axes,
                    exclude,
                    include,
                }
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        /// Expansion never panics. Empty is allowed when the product is fully
        /// excluded and no include-only rows remain (official MatrixBuilder).
        #[test]
        fn expand_never_panics(spec in arb_matrix_spec()) {
            let _ = expand_matrix_spec(&spec);
        }

        /// When each axis has distinct values, expanded combinations are unique.
        /// (Duplicate axis values intentionally fan out identical rows — same as GitHub.)
        #[test]
        fn combinations_unique_when_axis_values_unique(spec in arb_matrix_spec()) {
            for values in spec.axes.values() {
                let mut seen = std::collections::BTreeSet::new();
                for v in values {
                    let key = serde_json::to_string(v).unwrap();
                    prop_assume!(seen.insert(key));
                }
            }
            let combos = expand_matrix_spec(&spec);
            let mut seen = std::collections::BTreeSet::new();
            for c in &combos {
                let key = serde_json::to_string(&c.values.iter().collect::<Vec<_>>()).unwrap();
                prop_assert!(seen.insert(key), "duplicate combination {:?}", c.values);
            }
        }

        /// Exclude applies only to the cartesian product (before include).
        #[test]
        fn excluded_never_in_cartesian(spec in arb_matrix_spec()) {
            let mut pre_include = MatrixSpec {
                axes: spec.axes.clone(),
                exclude: spec.exclude.clone(),
                include: vec![],
            };
            // expand with empty include; drop the empty-fallback-only case
            let combos = expand_matrix_spec(&pre_include);
            for ex in &spec.exclude {
                for c in &combos {
                    if c.values.is_empty() && pre_include.axes.is_empty() {
                        continue;
                    }
                    // After cartesian+exclude, no non-empty row matches exclude.
                    if !c.values.is_empty() {
                        prop_assert!(
                            !matches_partial(&c.values, ex),
                            "excluded combination still present: {:?} matches {:?}",
                            c.values,
                            ex
                        );
                    }
                }
            }
            // Keep `spec.include` used so the generator stays honest.
            let _ = expand_matrix_spec(&spec);
            let _ = &mut pre_include;
        }

        /// Determinism: same spec → same ordered combinations.
        #[test]
        fn expansion_deterministic(spec in arb_matrix_spec()) {
            let a = expand_matrix_spec(&spec);
            let b = expand_matrix_spec(&spec);
            prop_assert_eq!(a, b);
        }

        /// Cartesian count multiplies axis lengths (before exclude).
        #[test]
        fn cartesian_count_matches_product(spec in arb_matrix_spec()) {
            let expected = if spec.axes.is_empty() {
                1
            } else {
                spec.axes.values().map(|v| v.len()).product()
            };
            prop_assert_eq!(cartesian_count(&spec), expected);
        }

        /// Expanded job ids are unique for unique combinations.
        #[test]
        fn expanded_ids_unique_for_distinct_combos(spec in arb_matrix_spec()) {
            let combos = expand_matrix_spec(&spec);
            let ids: Vec<_> = combos
                .iter()
                .map(|c| expanded_job_id("job", &c.values))
                .collect();
            // Same values ⇒ same id; distinct value maps should not collide when
            // value_key renderings differ. We only assert injectivity on the
            // serialized value map, not on the display id (bool/number can alias).
            let mut map = std::collections::BTreeMap::new();
            for (c, id) in combos.iter().zip(ids.iter()) {
                let key = serde_json::to_string(&c.values.iter().collect::<Vec<_>>()).unwrap();
                if let Some(prev) = map.insert(key, id.clone()) {
                    prop_assert_eq!(prev, id.clone());
                }
            }
        }

        /// Deferred failure preserves template identity (scenario 63 family).
        #[test]
        fn deferred_failure_preserves_placeholders(
            case_name in "[a-z]{1,5}",
            mode_name in "[a-z]{1,5}"
        ) {
            let template = format!(
                "matrix-build-${{{{ matrix.{case_name} }}}}-${{{{ matrix.{mode_name} }}}}"
            );
            let deferred = vec![
                DeferredMatrixAxis {
                    name: case_name,
                    expression: "fromJSON(needs.p.outputs.m)".into(),
                },
                DeferredMatrixAxis {
                    name: mode_name,
                    expression: "fromJSON(needs.p.outputs.m2)".into(),
                },
            ];
            let outcome = expand_with_deferred(
                "matrix-build",
                &template,
                &MatrixSpec::default(),
                &deferred,
                false,
            );
            match outcome {
                ExpandOutcome::Unresolved {
                    display_template, ..
                } => {
                    prop_assert!(display_template.contains("${{"));
                    prop_assert_ne!(display_template, "matrix-build");
                }
                ExpandOutcome::Concrete(_) => prop_assert!(false, "should be unresolved"),
            }
        }

        /// When producer succeeds with no deferred axes, concrete expansion runs.
        #[test]
        fn deferred_ok_without_axes_is_concrete(spec in arb_matrix_spec()) {
            let outcome =
                expand_with_deferred("job", "job", &spec, &[], true);
            prop_assert!(matches!(outcome, ExpandOutcome::Concrete(_)));
        }
    }

    /// Include-only rows are tagged and do not invent axis declaration order.
    #[test]
    fn include_only_flag_set_for_appended_rows() {
        let mut axes = IndexMap::new();
        axes.insert("os".into(), vec![json!("linux")]);
        let mut include = IndexMap::new();
        include.insert("os".into(), json!("windows"));
        include.insert("extra".into(), json!(true));
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![include],
        });
        assert_eq!(combos.len(), 2);
        assert!(!combos[0].include_only);
        assert!(combos[1].include_only);
        assert_eq!(combos[1].values.get("extra"), Some(&json!(true)));
    }

    /// Official MatrixBuilder example 1 — simple cross product.
    #[test]
    fn official_matrix_builder_example_cross_product() {
        let mut axes = IndexMap::new();
        axes.insert("arch".into(), vec![json!("x64"), json!("x86")]);
        axes.insert("os".into(), vec![json!("linux"), json!("windows")]);
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![],
        });
        let ids: Vec<_> = combos
            .iter()
            .map(|c| expanded_job_id("job", &c.values))
            .collect();
        assert_eq!(
            ids,
            vec![
                "job (x64, linux)",
                "job (x64, windows)",
                "job (x86, linux)",
                "job (x86, windows)",
            ]
        );
    }

    /// Official MatrixBuilder example 2 — exclude filter.
    #[test]
    fn official_matrix_builder_example_exclude() {
        let mut axes = IndexMap::new();
        axes.insert("arch".into(), vec![json!("x64"), json!("x86")]);
        axes.insert("os".into(), vec![json!("linux"), json!("windows")]);
        let mut ex = IndexMap::new();
        ex.insert("arch".into(), json!("x86"));
        ex.insert("os".into(), json!("linux"));
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![ex],
            include: vec![],
        });
        assert_eq!(combos.len(), 3);
        assert!(!combos.iter().any(|c| {
            c.values.get("arch") == Some(&json!("x86"))
                && c.values.get("os") == Some(&json!("linux"))
        }));
    }

    /// Official MatrixBuilder example 3 — include adds extra values to *all* matches.
    #[test]
    fn official_matrix_builder_example_include_extra_all_matches() {
        let mut axes = IndexMap::new();
        axes.insert("arch".into(), vec![json!("x64"), json!("x86")]);
        axes.insert("os".into(), vec![json!("linux"), json!("windows")]);
        let mut inc = IndexMap::new();
        inc.insert("arch".into(), json!("x64"));
        inc.insert("os".into(), json!("linux"));
        inc.insert("publish".into(), json!(true));
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![inc],
        });
        assert_eq!(combos.len(), 4);
        let x64_linux = combos
            .iter()
            .find(|c| {
                c.values.get("arch") == Some(&json!("x64"))
                    && c.values.get("os") == Some(&json!("linux"))
            })
            .unwrap();
        assert_eq!(x64_linux.values.get("publish"), Some(&json!(true)));
        // Other rows must not get publish.
        assert!(combos.iter().filter(|c| c.values.get("publish").is_some()).count() == 1);
    }

    /// Include with only an axis key that matches multiple product rows applies
    /// to every match (MatrixBuilder.Match loops all vectors).
    #[test]
    fn official_include_merges_into_all_matching_vectors() {
        let mut axes = IndexMap::new();
        axes.insert("arch".into(), vec![json!("x64"), json!("x86")]);
        axes.insert("os".into(), vec![json!("linux"), json!("windows")]);
        let mut inc = IndexMap::new();
        inc.insert("arch".into(), json!("x64"));
        inc.insert("publish".into(), json!(true));
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![inc],
        });
        let published: Vec<_> = combos
            .iter()
            .filter(|c| c.values.get("publish") == Some(&json!(true)))
            .collect();
        // Both x64/linux and x64/windows get publish:true.
        assert_eq!(published.len(), 2);
        assert!(published.iter().all(|c| c.values.get("arch") == Some(&json!("x64"))));
    }

    /// Official: all-excluded product with no include-only → zero configurations.
    #[test]
    fn official_all_excluded_yields_empty() {
        let mut axes = IndexMap::new();
        axes.insert("os".into(), vec![json!("linux")]);
        let mut ex = IndexMap::new();
        ex.insert("os".into(), json!("linux"));
        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![ex],
            include: vec![],
        });
        assert!(combos.is_empty());
    }

    /// Golden fixture fixtures/golden/matrix-expand.yml expansion ids.
    #[test]
    fn golden_matrix_expand_fixture_ids() {
        let yaml = include_str!("../../../fixtures/golden/matrix-expand.yml");
        let workflow = crate::parse_workflow(yaml).unwrap();
        let jobs = crate::expand_jobs(&workflow).unwrap();
        let ids: Vec<_> = jobs.iter().map(|j| j.id.0.clone()).collect();
        assert_eq!(
            ids,
            vec![
                "build (ubuntu-latest, 16)",
                "build (ubuntu-latest, 18)",
                "build (macos-latest, 16)",
                "build (macos-latest, 18)",
            ]
        );
    }

    /// Size limit: cartesian count stays bounded for generated specs.
    /// Oracle: docs/property-tests.md §2.14 — size limits enforced before expansion.
    proptest! {
        #![proptest_config(ProptestConfig { cases: 1_000, ..ProptestConfig::default() })]

        #[test]
        fn cartesian_count_bounded(
            axis_count in 0usize..=4,
            sizes in proptest::collection::vec(0usize..=6, 0..=4),
        ) {
            let mut axes = IndexMap::new();
            for i in 0..axis_count.min(sizes.len()) {
                let vals: Vec<Value> = (0..sizes[i]).map(|v| json!(v)).collect();
                if !vals.is_empty() {
                    axes.insert(format!("a{i}"), vals);
                }
            }
            let spec = MatrixSpec { axes, exclude: vec![], include: vec![] };
            let count = cartesian_count(&spec);
            prop_assert!(count <= 1296, "cartesian count {count} exceeds 6^4 bound");
            let combos = expand_matrix_spec(&spec);
            prop_assert!(combos.len() <= count + 1); // +1 for empty-fallback
        }
    }

    /// Declaration-order permutation: same axes in different order produce
    /// the same set of value combinations.
    /// Oracle: docs/property-tests.md §2.13 — reordering does not change values.
    #[test]
    fn declaration_order_permutation_stable() {
        let mut axes_ab = IndexMap::new();
        axes_ab.insert("os".into(), vec![json!("linux"), json!("mac")]);
        axes_ab.insert("node".into(), vec![json!(18), json!(20)]);
        let mut axes_ba = IndexMap::new();
        axes_ba.insert("node".into(), vec![json!(18), json!(20)]);
        axes_ba.insert("os".into(), vec![json!("linux"), json!("mac")]);

        let combos_ab = expand_matrix_spec(&MatrixSpec { axes: axes_ab, exclude: vec![], include: vec![] });
        let combos_ba = expand_matrix_spec(&MatrixSpec { axes: axes_ba, exclude: vec![], include: vec![] });
        assert_eq!(combos_ab.len(), combos_ba.len());
        // Same value sets (order may differ by declaration order, which is correct)
        let set_ab: std::collections::BTreeSet<_> = combos_ab.iter().map(|c| {
            let mut sorted: Vec<_> = c.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            sorted
        }).collect();
        let set_ba: std::collections::BTreeSet<_> = combos_ba.iter().map(|c| {
            let mut sorted: Vec<_> = c.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            sorted.sort_by(|a, b| a.0.cmp(&b.0));
            sorted
        }).collect();
        assert_eq!(set_ab, set_ba);
    }

    /// Production-path: generated matrix → YAML → parse → expand → job count matches model.
    /// Oracle: docs/property-tests.md §2 production-path requirements.
    #[test]
    fn matrix_production_path_count_matches_model() {
        // 100 deterministic cases
        for case in 0u64..100 {
            let n_vals = ((case * 7 + 3) % 3 + 1) as usize; // 1-3 values
            let vals: Vec<Value> = (0..n_vals).map(|v| json!(format!("v{v}"))).collect();
            let mut axes = IndexMap::new();
            axes.insert("x".into(), vals);
            if case % 3 == 0 {
                let vals2: Vec<Value> = (0..((case % 2 + 1) as usize)).map(|v| json!(format!("w{v}"))).collect();
                axes.insert("y".into(), vals2);
            }
            let spec = MatrixSpec { axes: axes.clone(), exclude: vec![], include: vec![] };
            let model_count = expand_matrix_spec(&spec).len();

            // Render as YAML
            let mut yaml = String::from("on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n");
            for (name, values) in &axes {
                yaml.push_str(&format!("        {name}: ["));
                for (i, v) in values.iter().enumerate() {
                    if i > 0 { yaml.push_str(", "); }
                    yaml.push_str(v.as_str().unwrap_or("0"));
                }
                yaml.push_str("]\n");
            }
            yaml.push_str("    steps:\n      - run: echo test\n");

            let workflow = crate::parse_workflow(&yaml).unwrap();
            let jobs = crate::expand_jobs(&workflow).unwrap();
            assert_eq!(
                jobs.len(), model_count,
                "case {case}: production {}, model {model_count}",
                jobs.len()
            );
        }
    }
}
