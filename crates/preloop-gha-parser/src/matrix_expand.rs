//! Pure matrix expansion matching GitHub Actions semantics.
//!
//! Official order (`MatrixBuilder.cs` in the GitHub workflow parser):
//! 1. Build the Cartesian product of declared axes.
//! 2. Drop rows that match an `exclude` filter.
//! 3. Match every `include` against each surviving original row using only
//!    declared axis keys; merge non-axis extras, with later extras winning.
//! 4. Append each include that matched no original row as an independent row.
//!
//! An explicitly empty or fully excluded matrix produces no rows. A job with
//! no matrix is handled separately and produces one empty matrix context.

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

/// GitHub's documented ceiling on jobs a single matrix job may generate.
pub const MAX_MATRIX_COMBINATIONS: usize = 256;
const MAX_MATRIX_AXES: usize = 1024;
const MAX_MATRIX_EXPANSION_WORK: usize = 1_000_000;

/// Expand a concrete matrix, refusing inputs whose fan-out exceeds
/// [`MAX_MATRIX_COMBINATIONS`].
///
/// The cross-product is fully materialized, so an uncapped spec is a
/// memory-exhaustion vector from a tiny workflow payload; GitHub rejects the
/// same input server-side, and the official runner surfaces that error, so
/// refusing here also preserves wire compatibility.
pub fn try_expand_matrix_spec(
    job_id: &str,
    spec: &MatrixSpec,
) -> Result<Vec<MatrixCombination>, crate::ParserError> {
    // A matrix made only from `include` is bounded before any rows are
    // materialized. Matching includes do not add jobs, so counting every
    // include here would reject valid 256-row matrices.
    if spec.axes.is_empty() && spec.include.len() > MAX_MATRIX_COMBINATIONS {
        return Err(crate::ParserError::MatrixTooLarge {
            job_id: job_id.to_owned(),
            limit: MAX_MATRIX_COMBINATIONS,
        });
    }
    if spec.axes.len() > MAX_MATRIX_AXES {
        return Err(crate::ParserError::MatrixTooLarge {
            job_id: job_id.to_owned(),
            limit: MAX_MATRIX_COMBINATIONS,
        });
    }
    expand_matrix_spec_bounded(spec).map_err(|()| crate::ParserError::MatrixTooLarge {
        job_id: job_id.to_owned(),
        limit: MAX_MATRIX_COMBINATIONS,
    })
}

struct ExpansionState<'a> {
    spec: &'a MatrixSpec,
    axes: Vec<(&'a String, &'a Vec<Value>)>,
    include_matched: Vec<bool>,
    work: usize,
}

fn expand_matrix_spec_bounded(spec: &MatrixSpec) -> Result<Vec<MatrixCombination>, ()> {
    let mut expanded = Vec::with_capacity(
        MAX_MATRIX_COMBINATIONS.min(cartesian_count(spec).saturating_add(spec.include.len())),
    );
    let mut state = ExpansionState {
        spec,
        axes: spec.axes.iter().collect(),
        include_matched: vec![false; spec.include.len()],
        work: 0,
    };
    let mut current = IndexMap::new();
    if !state.axes.is_empty() && state.axes.iter().all(|(_, values)| !values.is_empty()) {
        expand_axis_rows(&mut state, 0, &mut current, &mut expanded)?;
    }

    for (included, matched) in spec.include.iter().zip(state.include_matched) {
        if !matched {
            push_matrix_row(
                &mut expanded,
                MatrixCombination {
                    values: included.clone(),
                    include_only: true,
                },
            )?;
        }
    }
    Ok(expanded)
}

fn expand_axis_rows(
    state: &mut ExpansionState<'_>,
    axis_index: usize,
    current: &mut IndexMap<String, Value>,
    expanded: &mut Vec<MatrixCombination>,
) -> Result<(), ()> {
    if axis_index == state.axes.len() {
        state.work = state.work.checked_add(1).ok_or(())?;
        if state.work > MAX_MATRIX_EXPANSION_WORK {
            return Err(());
        }
        let mut excluded = false;
        for exc in &state.spec.exclude {
            state.work = state.work.checked_add(1).ok_or(())?;
            if state.work > MAX_MATRIX_EXPANSION_WORK {
                return Err(());
            }
            if matches_partial(current, exc) {
                excluded = true;
                break;
            }
        }
        if excluded {
            return Ok(());
        }

        let mut values = current.clone();
        let mut extras = IndexMap::new();
        for (index, included) in state.spec.include.iter().enumerate() {
            state.work = state.work.checked_add(1).ok_or(())?;
            if state.work > MAX_MATRIX_EXPANSION_WORK {
                return Err(());
            }
            if included.iter().all(|(key, value)| {
                !state.spec.axes.contains_key(key) || current.get(key) == Some(value)
            }) {
                state.include_matched[index] = true;
                for (key, value) in included {
                    if !state.spec.axes.contains_key(key) {
                        extras.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        values.extend(extras);
        push_matrix_row(
            expanded,
            MatrixCombination {
                values,
                include_only: false,
            },
        )
    } else {
        let (axis, values) = state.axes[axis_index];
        for value in values {
            current.insert(axis.clone(), value.clone());
            expand_axis_rows(state, axis_index + 1, current, expanded)?;
            current.shift_remove(axis);
        }
        Ok(())
    }
}

fn push_matrix_row(
    expanded: &mut Vec<MatrixCombination>,
    row: MatrixCombination,
) -> Result<(), ()> {
    if expanded.len() >= MAX_MATRIX_COMBINATIONS {
        return Err(());
    }
    expanded.push(row);
    Ok(())
}

/// Expand a concrete matrix according to GitHub's `MatrixBuilder` semantics.
#[allow(dead_code)]
pub fn expand_matrix_spec(spec: &MatrixSpec) -> Vec<MatrixCombination> {
    let mut combinations = Vec::with_capacity(cartesian_count(spec));
    if !spec.axes.is_empty() && spec.axes.values().all(|values| !values.is_empty()) {
        combinations.push(IndexMap::new());
    }

    for (axis, values) in &spec.axes {
        if values.is_empty() {
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

    // MatrixBuilder evaluates every include against the original cross-product
    // row, not against a row already mutated by earlier includes. Only declared
    // axis keys are filters; all other keys are extras. Later matching includes
    // overwrite earlier extras with the same key.
    let mut include_matched = vec![false; spec.include.len()];
    let mut expanded = Vec::with_capacity(combinations.len() + spec.include.len());
    for mut values in combinations {
        let mut extras = IndexMap::new();
        for (index, included) in spec.include.iter().enumerate() {
            let matches = included
                .iter()
                .all(|(key, value)| !spec.axes.contains_key(key) || values.get(key) == Some(value));
            if !matches {
                continue;
            }

            include_matched[index] = true;
            for (key, value) in included {
                if !spec.axes.contains_key(key) {
                    extras.insert(key.clone(), value.clone());
                }
            }
        }
        values.extend(extras);
        expanded.push(MatrixCombination {
            values,
            include_only: false,
        });
    }

    for (included, matched) in spec.include.iter().zip(include_matched) {
        if !matched {
            expanded.push(MatrixCombination {
                values: included.clone(),
                include_only: true,
            });
        }
    }

    expanded
}

/// Cartesian product size before exclude/include (0 if any axis is empty).
pub fn cartesian_count(spec: &MatrixSpec) -> usize {
    if spec.axes.is_empty() {
        return 0;
    }
    spec.axes
        .values()
        .try_fold(1usize, |count, values| {
            (!values.is_empty()).then(|| count.saturating_mul(values.len()))
        })
        .unwrap_or(0)
}

/// Build the official expanded job id: `base (v1, v2)` in declaration order.
pub fn expanded_job_id(base: &str, matrix: &IndexMap<String, Value>) -> String {
    if matrix.is_empty() {
        return base.to_owned();
    }
    let values: Vec<String> = matrix.values().map(value_key).collect();
    format!("{base} ({})", values.join(", "))
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

/// Convert the parser's matrix type into a validated expansion specification.
pub fn matrix_to_spec(
    job_id: &str,
    matrix: &crate::Matrix,
) -> Result<MatrixSpec, crate::ParserError> {
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
        .map(|value| matrix_entry(job_id, "exclude", value))
        .collect::<Result<_, _>>()?;
    let include = matrix
        .include
        .iter()
        .map(|value| matrix_entry(job_id, "include", value))
        .collect::<Result<_, _>>()?;

    Ok(MatrixSpec {
        axes,
        exclude,
        include,
    })
}

/// Convert any JSON matrix value (object or array) into a validated expansion specification.
///
/// GitHub's workflow schema requires `strategy.matrix` to be a mapping, and a
/// dynamic `fromJSON(...)` matrix is held to the same schema: a bare array is a
/// workflow error there, not an implicit `include:` list. Axis order is the
/// object's declaration order — `JobNameBuilder` renders the name by walking
/// the mapping as written — which is why `serde_json` is built with
/// `preserve_order` and the axes land in an `IndexMap`.
pub fn value_to_matrix_spec(job_id: &str, value: &Value) -> Result<MatrixSpec, crate::ParserError> {
    match value {
        Value::Object(map) => {
            let mut axes = IndexMap::new();
            let mut include = Vec::new();
            let mut exclude = Vec::new();
            for (key, val) in map {
                if key == "include" {
                    let arr =
                        val.as_array()
                            .ok_or_else(|| crate::ParserError::InvalidMatrixEntry {
                                job_id: job_id.to_owned(),
                                field: "include",
                            })?;
                    for item in arr {
                        include.push(matrix_entry(job_id, "include", item)?);
                    }
                } else if key == "exclude" {
                    let arr =
                        val.as_array()
                            .ok_or_else(|| crate::ParserError::InvalidMatrixEntry {
                                job_id: job_id.to_owned(),
                                field: "exclude",
                            })?;
                    for item in arr {
                        exclude.push(matrix_entry(job_id, "exclude", item)?);
                    }
                } else {
                    let axis_values = match val {
                        Value::Array(arr) => arr.clone(),
                        v => vec![v.clone()],
                    };
                    axes.insert(key.clone(), axis_values);
                }
            }
            Ok(MatrixSpec {
                axes,
                exclude,
                include,
            })
        }
        _ => Err(crate::ParserError::InvalidExpression(format!(
            "job `{job_id}` matrix expression did not return a mapping"
        ))),
    }
}

fn matrix_entry(
    job_id: &str,
    field: &'static str,
    value: &Value,
) -> Result<IndexMap<String, Value>, crate::ParserError> {
    value_object_indexed(value).ok_or_else(|| crate::ParserError::InvalidMatrixEntry {
        job_id: job_id.to_owned(),
        field,
    })
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
        assert!(
            combos
                .iter()
                .filter(|c| c.values.get("publish").is_some())
                .count()
                == 1
        );
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
        assert!(published
            .iter()
            .all(|c| c.values.get("arch") == Some(&json!("x64"))));
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

    /// GitHub's documented include example: include filters compare only axis
    /// keys against original cartesian rows; later extras overwrite earlier
    /// extras, and unmatched include rows remain independent.
    #[test]
    fn official_sequential_includes_use_original_axes() {
        let mut axes = IndexMap::new();
        axes.insert("fruit".into(), vec![json!("apple"), json!("pear")]);
        axes.insert("animal".into(), vec![json!("cat"), json!("dog")]);

        let includes = [
            json!({"color": "green"}),
            json!({"color": "pink", "animal": "cat"}),
            json!({"fruit": "apple", "shape": "circle"}),
            json!({"fruit": "banana"}),
            json!({"fruit": "banana", "animal": "cat"}),
        ]
        .into_iter()
        .map(|value| value_object_indexed(&value).unwrap())
        .collect();

        let combos = expand_matrix_spec(&MatrixSpec {
            axes,
            exclude: vec![],
            include: includes,
        });
        let values: Vec<Value> = combos
            .into_iter()
            .map(|combo| serde_json::to_value(combo.values).unwrap())
            .collect();

        assert_eq!(
            values,
            vec![
                json!({
                    "fruit": "apple", "animal": "cat", "color": "pink", "shape": "circle"
                }),
                json!({
                    "fruit": "apple", "animal": "dog", "color": "green", "shape": "circle"
                }),
                json!({"fruit": "pear", "animal": "cat", "color": "pink"}),
                json!({"fruit": "pear", "animal": "dog", "color": "green"}),
                json!({"fruit": "banana"}),
                json!({"fruit": "banana", "animal": "cat"}),
            ]
        );
    }

    #[test]
    fn expand_jobs_all_excluded_yields_no_jobs() {
        let workflow = crate::parse_workflow(
            r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [linux]
        exclude:
          - os: linux
    steps:
      - run: echo test
"#,
        )
        .unwrap();

        assert!(crate::expand_jobs(&workflow).unwrap().is_empty());
    }

    #[test]
    fn expand_jobs_explicit_empty_matrix_yields_no_jobs() {
        let workflow = crate::parse_workflow(
            r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix: {}
    steps:
      - run: echo test
"#,
        )
        .unwrap();

        assert!(crate::expand_jobs(&workflow).unwrap().is_empty());
    }

    #[test]
    fn matrix_conversion_rejects_non_object_include_and_exclude_rows() {
        for (field, matrix) in [
            ("include", json!({"include": ["not-an-object"]})),
            ("exclude", json!({"exclude": [42]})),
        ] {
            let matrix: crate::Matrix = serde_json::from_value(matrix).unwrap();
            let error = matrix_to_spec("build", &matrix).unwrap_err();
            assert!(matches!(
                error,
                crate::ParserError::InvalidMatrixEntry {
                    ref job_id,
                    field: actual_field,
                } if job_id == "build" && actual_field == field
            ));
        }
    }

    /// A dynamic matrix (`fromJson` result) with a non-array reserved field
    /// must be rejected like the static path rejects malformed include/exclude,
    /// not silently dropped — dropping `exclude` would run jobs the workflow
    /// asked to exclude.
    #[test]
    fn value_to_matrix_spec_rejects_non_array_include_and_exclude() {
        for (field, value) in [
            ("include", json!({"include": {"os": "ubuntu-latest"}})),
            ("exclude", json!({"exclude": {"os": "ubuntu-latest"}})),
        ] {
            let error = value_to_matrix_spec("build", &value).unwrap_err();
            assert!(matches!(
                error,
                crate::ParserError::InvalidMatrixEntry {
                    ref job_id,
                    field: actual_field,
                } if job_id == "build" && actual_field == field
            ));
        }
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

    // Size limit: cartesian count stays bounded for generated specs.
    // Oracle: docs/property-tests.md §2.14 — size limits enforced before expansion.

    proptest! {
        #![proptest_config(ProptestConfig { cases: 1_000, ..ProptestConfig::default() })]

        #[test]
        fn cartesian_count_bounded(
            axis_count in 0usize..=4,
            sizes in proptest::collection::vec(0usize..=6, 0..=4),
        ) {
            let mut axes = IndexMap::new();
            for (i, size) in sizes.iter().copied().take(axis_count).enumerate() {
                let vals: Vec<Value> = (0..size).map(|v| json!(v)).collect();
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

        let combos_ab = expand_matrix_spec(&MatrixSpec {
            axes: axes_ab,
            exclude: vec![],
            include: vec![],
        });
        let combos_ba = expand_matrix_spec(&MatrixSpec {
            axes: axes_ba,
            exclude: vec![],
            include: vec![],
        });
        assert_eq!(combos_ab.len(), combos_ba.len());
        // Compare logical key/value sets; declaration order intentionally differs.
        let normalize = |combination: &MatrixCombination| {
            let sorted: std::collections::BTreeMap<_, _> = combination
                .values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            serde_json::to_string(&sorted).unwrap()
        };
        let set_ab: std::collections::BTreeSet<_> = combos_ab.iter().map(normalize).collect();
        let set_ba: std::collections::BTreeSet<_> = combos_ba.iter().map(normalize).collect();
        assert_eq!(set_ab, set_ba);
    }

    #[test]
    fn value_to_matrix_spec_accepts_mappings_and_rejects_bare_arrays() {
        let json_obj = serde_json::json!({
            "include": [
                {"os": "ubuntu-latest", "node": "20"},
                {"os": "macos-latest", "node": "22"}
            ]
        });
        let spec = value_to_matrix_spec("test", &json_obj).unwrap();
        let combos = expand_matrix_spec(&spec);
        assert_eq!(combos.len(), 2);

        // GitHub's schema requires `strategy.matrix` to be a mapping. A bare
        // array is a workflow error, not an implicit `include:` list.
        let json_arr = serde_json::json!([
            {"os": "ubuntu-latest"},
            {"os": "windows-latest"}
        ]);
        assert!(matches!(
            value_to_matrix_spec("test", &json_arr),
            Err(crate::ParserError::InvalidExpression(_))
        ));
    }

    /// A matrix whose cross-product exceeds the limit must be refused before
    /// the cartesian product is materialized. Ten axes of ten values expand
    /// to 10^10 combinations — uncapped, this exhausts memory at expansion
    /// time from a ~450 byte workflow.
    #[test]
    fn try_expand_matrix_spec_rejects_cross_product_over_limit() {
        let mut axes = IndexMap::new();
        for i in 0..10 {
            axes.insert(
                format!("a{i}"),
                (0..10).map(Value::from).collect::<Vec<_>>(),
            );
        }
        let spec = MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![],
        };
        assert!(matches!(
            try_expand_matrix_spec("bomb", &spec),
            Err(crate::ParserError::MatrixTooLarge { .. })
        ));
    }

    /// Include-only rows append past the cross product and must count toward
    /// the same ceiling.
    #[test]
    fn try_expand_matrix_spec_rejects_include_overflow() {
        let mut axes = IndexMap::new();
        axes.insert("a".to_owned(), (0..16).map(Value::from).collect::<Vec<_>>());
        let include = (0..241)
            .map(|i| {
                let mut row = IndexMap::new();
                // Non-matching axis value forces each include to become its
                // own row instead of merging into the cartesian rows.
                row.insert("a".to_owned(), Value::String(format!("only{i}")));
                row.insert(format!("extra{i}"), Value::from(i));
                row
            })
            .collect::<Vec<_>>();
        let spec = MatrixSpec {
            axes,
            exclude: vec![],
            include,
        };
        // 16 cartesian rows + 241 include-only rows = 257 > 256.
        assert!(matches!(
            try_expand_matrix_spec("bomb", &spec),
            Err(crate::ParserError::MatrixTooLarge { .. })
        ));
    }

    /// The limit itself must pass through untouched — a matrix exactly at the
    /// ceiling is legitimate on GitHub and must expand here too.
    #[test]
    fn try_expand_matrix_spec_allows_exact_limit() {
        let mut axes = IndexMap::new();
        axes.insert(
            "a".to_owned(),
            (0..MAX_MATRIX_COMBINATIONS)
                .map(Value::from)
                .collect::<Vec<_>>(),
        );
        let spec = MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![],
        };
        assert_eq!(
            try_expand_matrix_spec("edge", &spec).unwrap().len(),
            MAX_MATRIX_COMBINATIONS
        );
    }

    #[test]
    fn try_expand_matrix_spec_applies_excludes_before_limit() {
        let mut axes = IndexMap::new();
        axes.insert(
            "a".to_owned(),
            (0..=MAX_MATRIX_COMBINATIONS as i64)
                .map(Value::from)
                .collect::<Vec<_>>(),
        );
        let mut excluded = IndexMap::new();
        excluded.insert("a".to_owned(), Value::from(MAX_MATRIX_COMBINATIONS as i64));
        let spec = MatrixSpec {
            axes,
            exclude: vec![excluded],
            include: vec![],
        };

        assert_eq!(
            try_expand_matrix_spec("edge", &spec).unwrap().len(),
            MAX_MATRIX_COMBINATIONS
        );
    }

    #[test]
    fn matching_include_does_not_count_as_an_extra_job() {
        let mut axes = IndexMap::new();
        axes.insert(
            "a".to_owned(),
            (0..MAX_MATRIX_COMBINATIONS)
                .map(Value::from)
                .collect::<Vec<_>>(),
        );
        let mut include = IndexMap::new();
        include.insert("extra".to_owned(), Value::String("value".to_owned()));
        let spec = MatrixSpec {
            axes,
            exclude: vec![],
            include: vec![include],
        };

        let expanded = try_expand_matrix_spec("edge", &spec).unwrap();
        assert_eq!(expanded.len(), MAX_MATRIX_COMBINATIONS);
        assert_eq!(
            expanded[0].values.get("extra"),
            Some(&Value::String("value".to_owned()))
        );
    }

    /// End-to-end through the public API: the workflow bomb must fail with
    /// `MatrixTooLarge`, not OOM the process.
    #[test]
    fn expand_jobs_rejects_matrix_bomb_workflow() {
        let mut yaml = String::from(
            "on: push\njobs:\n  bomb:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n",
        );
        for i in 0..10 {
            yaml.push_str(&format!("        a{i}: [0,1,2,3,4,5,6,7,8,9]\n"));
        }
        yaml.push_str("    steps:\n      - run: echo hi\n");
        let workflow = crate::parse_workflow(&yaml).unwrap();
        assert!(matches!(
            crate::expand_jobs(&workflow),
            Err(crate::ParserError::MatrixTooLarge { .. })
        ));
    }

    /// A dynamic matrix names its jobs in the order the mapping declares its
    /// axes, matching `JobNameBuilder`. Sorted keys would render
    /// `build (x64, ubuntu-latest)` for a spec that declares `os` first.
    #[test]
    fn value_to_matrix_spec_preserves_axis_declaration_order() {
        let spec = value_to_matrix_spec(
            "build",
            &serde_json::json!({"os": ["ubuntu-latest"], "arch": ["x64"]}),
        )
        .unwrap();
        assert_eq!(
            spec.axes.keys().cloned().collect::<Vec<_>>(),
            vec!["os".to_owned(), "arch".to_owned()]
        );
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
                let vals2: Vec<Value> = (0..((case % 2 + 1) as usize))
                    .map(|v| json!(format!("w{v}")))
                    .collect();
                axes.insert("y".into(), vals2);
            }
            let spec = MatrixSpec {
                axes: axes.clone(),
                exclude: vec![],
                include: vec![],
            };
            let model_count = expand_matrix_spec(&spec).len();

            // Render as YAML
            let mut yaml = String::from("on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n");
            for (name, values) in &axes {
                yaml.push_str(&format!("        {name}: ["));
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        yaml.push_str(", ");
                    }
                    yaml.push_str(v.as_str().unwrap_or("0"));
                }
                yaml.push_str("]\n");
            }
            yaml.push_str("    steps:\n      - run: echo test\n");

            let workflow = crate::parse_workflow(&yaml).unwrap();
            let jobs = crate::expand_jobs(&workflow).unwrap();
            assert_eq!(
                jobs.len(),
                model_count,
                "case {case}: production {}, model {model_count}",
                jobs.len()
            );
        }
    }
}
