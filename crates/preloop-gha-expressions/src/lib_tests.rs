use proptest::prelude::*;
use serde_json::json;

use super::*;

#[test]
fn format_output_cap_rejects_amplification_bomb() {
    // Nested format() triples its argument per level: 20 levels around a
    // 1 KB literal demands ~3 GB. Uncapped this was killed at 6.2 GB
    // resident; the cap must trip as an ordinary error instead.
    let mut expr = format!("'{}'", "A".repeat(1024));
    for _ in 0..20 {
        expr = format!("format('{{0}}{{0}}{{0}}', {expr})");
    }
    assert!(matches!(
        eval_expression(&expr, &Context::default()),
        Err(ExpressionError::FormatOutputTooLarge(_))
    ));
}

#[test]
fn format_output_cap_allows_normal_formatting() {
    assert_eq!(
        eval_expression("format('{0} {1}', 'hello', 'world')", &Context::default()).unwrap(),
        Value::String("hello world".to_owned())
    );
    // Just under the cap passes through untouched.
    let value = "A".repeat(500_000);
    assert_eq!(
        eval_expression(
            &format!("format('{{0}}{{0}}', '{value}')",),
            &Context::default()
        )
        .unwrap(),
        Value::String(format!("{value}{value}"))
    );
}

#[test]
fn format_argument_evaluation_has_an_expression_wide_budget() {
    let value = "A".repeat(500_000);
    let mut expr = "format('{0}', 'ok'".to_owned();
    for _ in 0..20 {
        expr.push_str(", '");
        expr.push_str(&value);
        expr.push('\'');
    }
    expr.push(')');

    assert!(matches!(
        eval_expression(&expr, &Context::default()),
        Err(ExpressionError::EvaluationTooLarge(_))
    ));
}

#[test]
fn evaluation_budget_counts_container_and_null_overhead() {
    let mut context = Context::default();
    context.insert("values", Value::Array(vec![Value::Null; 300_000]));

    assert!(matches!(
        eval_expression("values", &context),
        Err(ExpressionError::EvaluationTooLarge(_))
    ));
}

#[test]
fn format_rejects_large_container_before_unbounded_render() {
    let mut context = Context::default();
    context.insert(
        "values",
        Value::Array(vec![Value::String("A".repeat(2_000)); 1_000]),
    );

    assert!(matches!(
        eval_expression("format('{0}', values)", &context),
        Err(ExpressionError::FormatOutputTooLarge(_))
    ));
}

#[test]
fn format_bounds_container_serialization_by_format_capacity() {
    // Container serialization is bounded by the remaining format capacity
    // (1 MiB), not the evaluation budget: a render that exceeds the format
    // cap must report FormatOutputTooLarge even when the evaluation budget
    // is the tighter constraint. Without the format-capacity bound the
    // serialization fails against the evaluation budget instead and reports
    // EvaluationTooLarge.
    let mut context = Context::default();
    // Consume most of the evaluation budget with an unreferenced argument.
    context.insert("bomb", Value::String("A".repeat(6_000_000)));
    // JSON escaping doubles the render: 800_000 quotes render as ~1.6 MiB of
    // JSON while the charged value size stays under the remaining budget.
    context.insert(
        "container",
        Value::Array(vec![Value::String("\"".repeat(800_000))]),
    );

    assert!(matches!(
        eval_expression("format('{1}', bomb, container)", &context),
        Err(ExpressionError::FormatOutputTooLarge(_))
    ));
}

#[test]
fn evaluates_context_and_functions() {
    let mut context = Context::default();
    context.insert("github", json!({"event_name": "push"}));
    context.insert("matrix", json!({"os": "ubuntu-latest"}));

    assert_eq!(
        eval_expression("${{ github.event_name == 'PUSH' }}", &context).unwrap(),
        Value::Bool(true)
    );
    assert!(eval_bool("contains(matrix.os, 'ubuntu') && success()", &context).unwrap());
}

#[test]
fn status_functions_use_context_state() {
    let context = Context::default().with_status(false, true, false);
    assert!(!eval_bool("success()", &context).unwrap());
    assert!(eval_bool("failure()", &context).unwrap());
    assert!(!eval_bool("cancelled()", &context).unwrap());
    assert!(eval_bool("always()", &context).unwrap());
}

#[test]
fn short_circuits() {
    let context = Context::default();
    assert_eq!(
        eval_expression("false && unknown()", &context).unwrap(),
        Value::Bool(false)
    );
    assert_eq!(
        eval_expression("'left' || unknown()", &context).unwrap(),
        Value::String("left".to_owned())
    );
}

#[test]
fn undefined_context_keys_coalesce_to_empty_like_github() {
    // GitHub expressions treat an undefined context key as an empty string
    // (with a warning), so a missing `head_ref` must coalesce through `||` to
    // the present `ref_name`. Mastodon's cache `key:` uses exactly this
    // pattern; the empty-input failure was a job-message round-trip bug that
    // dropped the template, not an evaluator bug.
    let mut context = Context::default();
    context.insert(
        "github",
        json!({"ref": "refs/heads/main", "ref_name": "main"}),
    );
    assert_eq!(
        eval_expression("github.head_ref || github.ref_name", &context).unwrap(),
        Value::String("main".to_owned())
    );
    // An undefined key on its own is null (renders as the empty string).
    assert_eq!(
        eval_expression("github.head_ref", &context).unwrap(),
        Value::Null
    );
}

#[test]
fn evaluates_json_join_and_comparisons() {
    let context = Context::default();

    assert_eq!(
        eval_expression("case(true, 'first', 'second')", &context).unwrap(),
        Value::String("first".to_owned())
    );
    assert_eq!(
        eval_expression("case(false, 'first', true, 'second', 'third')", &context).unwrap(),
        Value::String("second".to_owned())
    );
    assert_eq!(
        eval_expression("case(false, 'first', 'default')", &context).unwrap(),
        Value::String("default".to_owned())
    );
    assert!(validate_expression("case(true, 'a')").is_err());
    assert!(validate_expression("case(true, 'a', false, 'b')").is_err());
    assert!(validate_expression("case(github.ref == 'refs/heads/dev', format('{0}-{1}', github.workflow, github.run_id), 'default')").is_ok());

    assert_eq!(
        eval_expression("fromJson('[\"a\",\"b\"]')", &context).unwrap(),
        json!(["a", "b"])
    );
    assert_eq!(
        eval_expression("join(fromJson('[\"a\",\"b\"]'), '-')", &context).unwrap(),
        Value::String("a-b".to_owned())
    );
    assert_eq!(
        eval_expression("'10' > 2", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("2 <= 2", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("'B' > 'a'", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("'a' < 'B'", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("'ABC' >= 'abc'", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("'abc' <= 'ABC'", &context).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        eval_expression("hashFiles('Cargo.toml')", &context).unwrap(),
        Value::String(String::new())
    );
}
#[test]
fn function_arity_is_enforced_during_validation_and_evaluation() {
    for expression in [
        "always(true)",
        "cancelled('job')",
        "contains('only-one')",
        "fromJSON()",
        "join('a', '-', 'extra')",
        "hashFiles()",
    ] {
        assert!(
            matches!(
                validate_expression(expression),
                Err(ExpressionError::InvalidFunctionArity { .. })
            ),
            "validation accepted {expression}"
        );
        assert!(
            matches!(
                eval_expression(expression, &Context::default()),
                Err(ExpressionError::InvalidFunctionArity { .. })
            ),
            "evaluation accepted {expression}"
        );
    }
}

#[test]
fn handles_escaped_single_quotes_in_literals() {
    let context = Context::default();
    assert_eq!(
        eval_expression("'It''s a string'", &context).unwrap(),
        Value::String("It's a string".to_owned())
    );
    assert_eq!(
        eval_expression("''", &context).unwrap(),
        Value::String("".to_owned())
    );
    assert_eq!(
        eval_expression("''''", &context).unwrap(),
        Value::String("'".to_owned())
    );
    assert_eq!(
        eval_expression("'a''b''c'", &context).unwrap(),
        Value::String("a'b'c".to_owned())
    );
}

#[test]
fn fromjson_wildcard_member_access() {
    let context = Context::default();
    // fromJSON('...').*.name — member access on function call result
    assert_eq!(
        eval_expression(
            r#"join(fromJSON('[{"name":"alpha"},{"name":"beta"},{"name":"gamma"}]').*.name, ',')"#,
            &context
        )
        .unwrap(),
        Value::String("alpha,beta,gamma".to_owned())
    );
    // Simple member access on fromJSON
    assert_eq!(
        eval_expression(r#"fromJSON('{"a":{"b":"deep"}}').a.b"#, &context).unwrap(),
        Value::String("deep".to_owned())
    );
    // Bracket access on fromJSON
    assert_eq!(
        eval_expression(r#"fromJSON('{"x":"val"}')['x']"#, &context).unwrap(),
        Value::String("val".to_owned())
    );
}

#[test]
fn chained_bracket_access_on_from_json() {
    let context = Context::default();
    // Chained bracket: fromJSON(...)['a']['b']['c']
    assert_eq!(
        eval_expression(
            r#"fromJSON('{"a":{"b":{"c":"deep"}}}')['a']['b']['c']"#,
            &context,
        )
        .unwrap(),
        Value::String("deep".to_owned())
    );
    // Mixed dot and bracket
    assert_eq!(
        eval_expression(r#"fromJSON('{"a":{"b":{"c":"deep"}}}').a.b.c"#, &context,).unwrap(),
        Value::String("deep".to_owned())
    );
}

#[test]
fn hashfiles_follow_symlinks_flag() {
    // F055: hashFiles('--follow-symbolic-links', 'pattern') should parse
    // the flag without treating it as a glob pattern.
    // Without a workspace_dir, hashFiles returns "" regardless, but this
    // confirms the flag parsing doesn't cause errors.
    let context = Context::default();
    assert_eq!(
        eval_expression(
            "hashFiles('--follow-symbolic-links', 'Cargo.toml')",
            &context
        )
        .unwrap(),
        Value::String(String::new())
    );
    // Without the flag — same result (no workspace)
    assert_eq!(
        eval_expression("hashFiles('Cargo.toml')", &context).unwrap(),
        Value::String(String::new())
    );
}

#[test]
fn hashfiles_binary_digest_matches_official_algorithm() {
    // PEXP-01 regression test: verify binary digest concatenation.
    // Official hashFiles.ts concatenates raw 32-byte SHA-256 digests before hashing.
    // Pre-fix aksh concatenated hex strings — wrong algorithm, different cache keys.
    use sha2::{Digest, Sha256};

    // Use a unique temp dir under /tmp
    let dir = std::path::Path::new("/tmp/preloop-hashfiles-test");
    std::fs::create_dir_all(dir).unwrap();

    // Write two known files
    let a = dir.join("a.txt");
    let b = dir.join("b.txt");
    std::fs::write(&a, b"hello preloop").unwrap();
    std::fs::write(&b, b"hello world").unwrap();

    let ctx = Context::default().with_workspace(dir.to_string_lossy().to_string());

    // Single file: hashFiles('a.txt') should equal SHA256(binary concat of SHA256(content))
    let ha = eval_expression("hashFiles('a.txt')", &ctx).unwrap();
    let expected_a: String = {
        let inner = Sha256::digest(b"hello preloop");
        let mut combined: Vec<u8> = Vec::new();
        combined.extend_from_slice(&inner);
        format!("{:x}", Sha256::digest(&combined))
    };
    assert_eq!(
        ha,
        Value::String(expected_a.clone()),
        "single-file hash mismatch"
    );

    // Must be 64 lowercase hex chars
    assert_eq!(expected_a.len(), 64);
    assert!(expected_a.chars().all(|c| c.is_ascii_hexdigit()));

    // Different files must produce different hashes
    let hb = eval_expression("hashFiles('b.txt')", &ctx).unwrap();
    assert_ne!(ha, hb, "distinct files must produce distinct hashes");

    // hashFiles with no-match pattern returns ""
    let empty = eval_expression("hashFiles('nonexistent.txt')", &ctx).unwrap();
    assert_eq!(
        empty,
        Value::String(String::new()),
        "no-match must return empty string"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(dir);
}

proptest! {
    #[test]
    fn string_equality_is_case_insensitive(value in "[A-Za-z]{1,24}") {
        let expr = format!("'{}' == '{}'", value, value.to_ascii_uppercase());
        prop_assert_eq!(eval_expression(&expr, &Context::default()).unwrap(), Value::Bool(true));
    }

    #[test]
    fn non_empty_strings_are_truthy(value in ".{1,32}") {
        prop_assert!(is_truthy(&Value::String(value)));
    }
}

// --- ConditionFunctionsL0 coverage ---

#[test]
fn condition_always_returns_true_regardless_of_status() {
    // all-success
    assert!(eval_bool(
        "always()",
        &Context::default().with_status(true, false, false)
    )
    .unwrap());
    // failure
    assert!(eval_bool(
        "always()",
        &Context::default().with_status(false, true, false)
    )
    .unwrap());
    // cancelled
    assert!(eval_bool(
        "always()",
        &Context::default().with_status(false, false, true)
    )
    .unwrap());
    // all-false (edge case: no status set)
    assert!(eval_bool(
        "always()",
        &Context::default().with_status(false, false, false)
    )
    .unwrap());
}

#[test]
fn condition_success_true_only_when_success_flag_set() {
    assert!(eval_bool(
        "success()",
        &Context::default().with_status(true, false, false)
    )
    .unwrap());
    assert!(!eval_bool(
        "success()",
        &Context::default().with_status(false, true, false)
    )
    .unwrap());
    assert!(!eval_bool(
        "success()",
        &Context::default().with_status(false, false, true)
    )
    .unwrap());
    assert!(!eval_bool(
        "success()",
        &Context::default().with_status(false, false, false)
    )
    .unwrap());
}

#[test]
fn condition_failure_true_only_when_failure_flag_set() {
    assert!(!eval_bool(
        "failure()",
        &Context::default().with_status(true, false, false)
    )
    .unwrap());
    assert!(eval_bool(
        "failure()",
        &Context::default().with_status(false, true, false)
    )
    .unwrap());
    assert!(!eval_bool(
        "failure()",
        &Context::default().with_status(false, false, true)
    )
    .unwrap());
    assert!(!eval_bool(
        "failure()",
        &Context::default().with_status(false, false, false)
    )
    .unwrap());
}

#[test]
fn condition_cancelled_true_only_when_cancelled_flag_set() {
    assert!(!eval_bool(
        "cancelled()",
        &Context::default().with_status(true, false, false)
    )
    .unwrap());
    assert!(!eval_bool(
        "cancelled()",
        &Context::default().with_status(false, true, false)
    )
    .unwrap());
    assert!(eval_bool(
        "cancelled()",
        &Context::default().with_status(false, false, true)
    )
    .unwrap());
    assert!(!eval_bool(
        "cancelled()",
        &Context::default().with_status(false, false, false)
    )
    .unwrap());
}

#[test]
fn condition_functions_combined_state() {
    // failure+cancelled: both true simultaneously
    let ctx = Context::default().with_status(false, true, true);
    assert!(!eval_bool("success()", &ctx).unwrap());
    assert!(eval_bool("failure()", &ctx).unwrap());
    assert!(eval_bool("cancelled()", &ctx).unwrap());
    assert!(eval_bool("always()", &ctx).unwrap());

    // Compound expressions
    assert!(eval_bool("failure() || cancelled()", &ctx).unwrap());
    assert!(!eval_bool("success() && !failure()", &ctx).unwrap());
}
#[test]
fn format_with_non_ascii_in_template() {
    let ctx = Context::default();
    // em dash U+2014 inside the format template literal
    let r = eval_expression(
        "format('only runs for ubuntu-latest \u{2014} os={0}', 'ubuntu-latest')",
        &ctx,
    );
    assert_eq!(
        r.unwrap(),
        serde_json::Value::String(
            "only runs for ubuntu-latest \u{2014} os=ubuntu-latest".to_string()
        )
    );
}

#[test]
fn format_with_matrix_context() {
    let mut ctx = Context::default();
    ctx.insert("matrix", serde_json::json!({"os": "ubuntu-latest"}));
    let r = eval_expression(
        "format('echo \"only runs for ubuntu-latest \u{2014} os={0}\"', matrix.os)",
        &ctx,
    );
    assert_eq!(
        r.unwrap(),
        serde_json::Value::String(
            "echo \"only runs for ubuntu-latest \u{2014} os=ubuntu-latest\"".to_string()
        )
    );
}

#[test]
fn format_with_multiline_template() {
    let mut ctx = Context::default();
    ctx.insert(
        "matrix",
        serde_json::json!({"platform": {"name": "Linux ARM64", "target": "aarch64"}}),
    );
    // Matches what GHA sends: format string with real newlines
    let r = eval_expression(
            "format('echo \"name={0}\"\necho \"target={1}\"\n', matrix.platform.name, matrix.platform.target)",
            &ctx,
        );
    assert_eq!(
        r.unwrap(),
        serde_json::Value::String(
            "echo \"name=Linux ARM64\"\necho \"target=aarch64\"\n".to_string()
        )
    );
}

/// GH-MATRIX-INT: integer matrix values must stringify as "1" not "1.0".
/// Tests the `string_value` path used by `format()` and other string functions.
#[test]
fn matrix_integer_renders_without_decimal_suffix() {
    let mut ctx = Context::default();
    // Simulate matrix.val = 1 as f64 (what serde_yaml 0.9 may produce)
    ctx.insert("matrix", serde_json::json!({"val": 1.0_f64}));

    // format() goes through string_value — must produce "1" not "1.0"
    let result = eval_expression("format('{0}', matrix.val)", &ctx).unwrap();
    assert_eq!(
        result,
        serde_json::Value::String("1".to_owned()),
        "f64(1.0) must render as '1' via format()"
    );

    // join() also goes through string_value
    ctx.insert(
        "matrix",
        serde_json::json!({"vals": [1.0_f64, 2.0_f64, 3.0_f64]}),
    );
    let joined = eval_expression("join(matrix.vals, ',')", &ctx).unwrap();
    assert_eq!(
        joined,
        serde_json::Value::String("1,2,3".to_owned()),
        "f64 array join must produce '1,2,3' not '1.0,2.0,3.0'"
    );
}

/// Genuine floats (1.5) must not be truncated.
#[test]
fn matrix_genuine_float_preserved() {
    let mut ctx = Context::default();
    ctx.insert("matrix", serde_json::json!({"val": 1.5_f64}));
    let result = eval_expression("matrix.val", &ctx).unwrap();
    let s = match &result {
        serde_json::Value::String(st) => st.clone(),
        other => other.to_string(),
    };
    assert!(s.contains('.'), "1.5 must retain decimal: got {s}");
}

mod official_semantics {
    use super::*;

    fn assert_bool(expression: &str, expected: bool) {
        let actual = eval_expression(expression, &Context::default())
            .unwrap_or_else(|error| panic!("{expression:?} should evaluate: {error}"));
        assert_eq!(actual, Value::Bool(expected), "expression: {expression}");
    }

    #[test]
    fn starts_with_and_ends_with_are_case_insensitive() {
        assert_bool("startsWith('Hello world', 'HELLO')", true);
        assert_bool("endsWith('Hello world', 'WORLD')", true);
        assert_bool("startsWith('Hello world', 'WORLD')", false);
        assert_bool("endsWith('Hello world', 'HELLO')", false);
    }

    #[test]
    fn same_kind_string_equality_is_case_insensitive() {
        assert_bool("'Alpha-Beta' == 'aLPHA-bETA'", true);
        assert_bool("'Alpha-Beta' != 'aLPHA-bETA'", false);
        assert_bool("'Alpha-Beta' == 'Alpha-Gamma'", false);
    }

    #[test]
    fn mixed_kind_coercion_uses_official_numeric_rules() {
        let cases = [
            ("0 == ''", true),
            ("null == 0", true),
            ("true == 1", true),
            ("false == 0", true),
            ("'1' == 1.0", true),
            ("null == ''", true),
            ("'  1  ' == 1", true),
            ("'0x10' == 16", true),
            ("'0o10' == 8", true),
            ("'Infinity' == 1", false),
            ("'Infinity' > 1", true),
            ("'-Infinity' < 0", true),
            ("'abc' == 0", false),
            ("'NaN' == 0", false),
            ("'NaN' != 0", true),
            ("'inf' != 0", true),
        ];

        for (expression, expected) in cases {
            assert_bool(expression, expected);
        }
    }

    #[test]
    fn arrays_and_objects_are_truthy_even_when_empty() {
        let cases = [
            ("fromJSON('[]')", true),
            ("fromJSON('[1]')", true),
            ("fromJSON('{}')", true),
            ("fromJSON('{\"key\":\"value\"}')", true),
        ];

        for (expression, expected) in cases {
            assert_eq!(
                eval_bool(expression, &Context::default()).unwrap(),
                expected,
                "expression: {expression}"
            );
        }
    }

    #[test]
    fn contains_array_uses_abstract_equality() {
        let cases = [
            ("contains(fromJSON('[1, 2]'), '1')", true),
            ("contains(fromJSON('[null]'), '')", true),
            ("contains(fromJSON('[true]'), '1')", true),
            ("contains(fromJSON('[1, 2]'), '3')", false),
        ];

        for (expression, expected) in cases {
            assert_bool(expression, expected);
        }
    }

    #[test]
    fn format_escaped_braces_are_preserved() {
        let result = eval_expression("format('{{literal}}')", &Context::default()).unwrap();
        assert_eq!(result, Value::String("{literal}".to_owned()));
    }

    #[test]
    fn format_malformed_template_errors() {
        for expression in [
            "format('{', 'value')",
            "format('value}', 'value')",
            "format('{x}', 'value')",
        ] {
            assert!(
                eval_expression(expression, &Context::default()).is_err(),
                "malformed template should fail: {expression}"
            );
        }
    }

    #[test]
    fn format_missing_index_argument_errors() {
        for expression in ["format('{1}', 'value')", "format('{256}', 'value')"] {
            assert!(
                eval_expression(expression, &Context::default()).is_err(),
                "invalid argument index should fail: {expression}"
            );
        }
    }

    #[test]
    fn format_invalid_specifier_errors() {
        assert!(
            eval_expression("format('{0:bogus}', 'value')", &Context::default()).is_err(),
            "unsupported format specifier should fail"
        );
    }

    #[test]
    fn hash_files_unknown_leading_flag_errors() {
        let workspace = std::env::temp_dir().join("preloop-official-semantics-hashfiles");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("x"), b"x").unwrap();

        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());
        let result = eval_expression("hashFiles('--bogus-flag', 'x')", &context);

        let _ = std::fs::remove_dir_all(&workspace);
        assert!(result.is_err(), "unknown hashFiles flags must fail");
    }
    /// R1-7: absolute patterns are rejected outright, never used as-is —
    /// otherwise hashFiles('/etc/passwd') is a file-content oracle for
    /// anything the runner can read. Silent remapping would hide intent.
    #[test]
    fn hash_files_absolute_pattern_rejected() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(workspace.join("sub")).unwrap();
        std::fs::write(
            workspace.join("sub").join("secret.txt"),
            b"workspace secret",
        )
        .unwrap();
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        for expr in ["hashFiles('/sub/secret.txt')", "hashFiles('/etc/hostname')"] {
            let result = eval_expression(expr, &context);
            assert!(
                matches!(result, Err(ExpressionError::HashFilesDisallowedPattern(_))),
                "absolute pattern must be rejected, got {result:?}: {expr}"
            );
        }

        // Workspace-relative patterns still work.
        let hashed = eval_expression("hashFiles('sub/secret.txt')", &context).unwrap();
        assert!(
            matches!(&hashed, Value::String(s) if s.len() == 64),
            "relative pattern should still hash, got {hashed:?}"
        );

        let _ = std::fs::remove_dir_all(&base);
    }

    /// R1-7: `../` traversal in a pattern is rejected outright, not silently
    /// skipped — the workflow author must see the failure.
    #[test]
    fn hash_files_dotdot_traversal_rejected() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17b-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(base.join("outside.txt"), b"must not be hashed").unwrap();
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        for expr in [
            "hashFiles('../outside.txt')",
            "hashFiles('sub/../../outside.txt')",
        ] {
            let result = eval_expression(expr, &context);
            assert!(
                matches!(result, Err(ExpressionError::HashFilesDisallowedPattern(_))),
                ".. traversal must be rejected, got {result:?}: {expr}"
            );
        }

        let _ = std::fs::remove_dir_all(&base);
    }
    /// R1-7: Windows-native traversal and absolute forms are rejected on
    /// Windows, where backslash is a separator and prefixes/roots escape.
    /// (On Unix these are literal filenames and correctly pass the guard.)
    #[cfg(windows)]
    #[test]
    fn hash_files_windows_traversal_rejected() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17w-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        for expr in [
            "hashFiles('..\\outside.txt')",
            "hashFiles('C:\\outside.txt')",
            "hashFiles('\\outside.txt')",
        ] {
            let result = eval_expression(expr, &context);
            assert!(
                matches!(result, Err(ExpressionError::HashFilesDisallowedPattern(_))),
                "Windows escape must be rejected, got {result:?}: {expr}"
            );
        }

        let _ = std::fs::remove_dir_all(&base);
    }

    /// R1-7: the traversal budget bounds visited entries, not just retained
    /// matches. 100k+ skipped directories would otherwise burn glob work
    /// without ever tripping the 10k retained-file cap.
    #[test]
    fn hash_files_traversal_budget_bounds_skipped_entries() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17f-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        for i in 0..100_050 {
            std::fs::create_dir(workspace.join(format!("d{i}"))).unwrap();
        }
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        let result = eval_expression("hashFiles('*')", &context);
        let _ = std::fs::remove_dir_all(&base);
        assert!(
            matches!(
                result,
                Err(ExpressionError::HashFilesTraversalLimit(100_000))
            ),
            "expected HashFilesTraversalLimit error, got {result:?}"
        );
    }

    /// R1-7: hashing more than 100 MiB of input fails with HashFilesTooLarge
    /// instead of loading it all into memory.
    #[test]
    fn hash_files_total_byte_cap_rejects_huge_input() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17d-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        // Sparse file: instant to create, reads as zeros.
        let big = std::fs::File::create(workspace.join("big.bin")).unwrap();
        big.set_len(101 * 1024 * 1024).unwrap();
        drop(big);
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        let result = eval_expression("hashFiles('big.bin')", &context);
        let _ = std::fs::remove_dir_all(&base);
        assert!(
            matches!(result, Err(ExpressionError::HashFilesTooLarge(_))),
            "expected HashFilesTooLarge, got {result:?}"
        );
    }

    /// R1-7: symlinks escaping the workspace are skipped even with
    /// --follow-symbolic-links; in-workspace symlinks are only followed
    /// when the flag is passed.
    #[test]
    fn hash_files_escaping_symlink_skipped() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17c-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("real.txt"), b"real").unwrap();
        std::os::unix::fs::symlink("/etc/hostname", workspace.join("escape-link")).unwrap();
        std::os::unix::fs::symlink(workspace.join("real.txt"), workspace.join("inner-link"))
            .unwrap();
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        // Escaping symlink is never hashed, flag or not.
        for expr in [
            "hashFiles('escape-link')",
            "hashFiles('--follow-symbolic-links', 'escape-link')",
        ] {
            let result = eval_expression(expr, &context).unwrap();
            assert_eq!(
                result,
                Value::String(String::new()),
                "escaping symlink must be skipped: {expr}"
            );
        }

        // In-workspace symlink: skipped without the flag, hashed with it.
        let no_flag = eval_expression("hashFiles('inner-link')", &context).unwrap();
        assert_eq!(no_flag, Value::String(String::new()));
        let with_flag = eval_expression(
            "hashFiles('--follow-symbolic-links', 'inner-link')",
            &context,
        )
        .unwrap();
        assert!(
            matches!(&with_flag, Value::String(s) if s.len() == 64),
            "in-workspace symlink should be followed with the flag"
        );

        let _ = std::fs::remove_dir_all(&base);
    }
    /// R1-7/Codex: symlink aliases to the same target hash per matched path
    /// like official — canonical dedup must not collapse them into one entry.
    #[cfg(unix)]
    #[test]
    fn hash_files_symlink_alias_hashes_per_match() {
        use sha2::{Digest, Sha256};
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17e-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("real.txt"), b"real").unwrap();
        std::os::unix::fs::symlink(workspace.join("real.txt"), workspace.join("inner-link"))
            .unwrap();
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        let single = eval_expression("hashFiles('real.txt')", &context).unwrap();
        let aliased = eval_expression(
            "hashFiles('--follow-symbolic-links', 'real.txt', 'inner-link')",
            &context,
        )
        .unwrap();
        let Value::String(single_hex) = single else {
            panic!("expected string hash");
        };
        let Value::String(aliased_hex) = aliased else {
            panic!("expected string hash");
        };
        assert_eq!(single_hex.len(), 64);
        assert_eq!(aliased_hex.len(), 64);
        assert_ne!(
            single_hex, aliased_hex,
            "alias + target must hash as two matches, not dedup to one"
        );
        // Both matches hold identical bytes, so order is irrelevant: the
        // outer hash must equal SHA256(digest || digest).
        let inner = Sha256::digest(b"real");
        let mut combined = Vec::new();
        combined.extend_from_slice(&inner);
        combined.extend_from_slice(&inner);
        let expected = format!("{:x}", Sha256::digest(&combined));
        assert_eq!(aliased_hex, expected);

        let _ = std::fs::remove_dir_all(&base);
    }

    /// R1-7: matching more than the file cap is a clear error, not silent
    /// truncation or unbounded hashing.
    #[test]
    fn hash_files_too_many_files_errors() {
        let base =
            std::env::temp_dir().join(format!("preloop-hashfiles-r17d-{}", std::process::id()));
        let workspace = base.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        for i in 0..10_001 {
            std::fs::write(workspace.join(format!("f{i}.txt")), b"x").unwrap();
        }
        let context = Context::default().with_workspace(workspace.to_string_lossy().into_owned());

        let result = eval_expression("hashFiles('*.txt')", &context);
        let _ = std::fs::remove_dir_all(&base);
        assert!(
            matches!(
                result,
                Err(crate::ExpressionError::HashFilesTooManyFiles(10_000))
            ),
            "expected HashFilesTooManyFiles error, got {result:?}"
        );
    }

    /// Deeply nested parens overflow the parser's stack instead of returning
    /// an error: 100k `(` from a 200 KB expression aborted the process before
    /// the depth guard existed. The guard must reject, not crash.
    #[test]
    fn deeply_nested_parens_error_instead_of_overflowing() {
        let n = 100_000;
        let expr = format!("{}1{}", "(".repeat(n), ")".repeat(n));
        assert!(matches!(
            validate_expression(&expr),
            Err(ExpressionError::TooDeep(_))
        ));
    }

    /// `!` recurses through a different parse function than parens; it needs
    /// the same ceiling.
    #[test]
    fn deeply_nested_negations_error_instead_of_overflowing() {
        let expr = format!("{}true", "!".repeat(100_000));
        assert!(matches!(
            validate_expression(&expr),
            Err(ExpressionError::TooDeep(_))
        ));
    }

    /// Expressions within the ceiling must keep working unchanged.
    #[test]
    fn expressions_within_depth_limit_still_parse() {
        use super::expr_parser::MAX_EXPRESSION_DEPTH;

        // `parse_expr` consumes one level for the root, so this is the
        // largest accepted parenthesized/unary nesting.
        let n = MAX_EXPRESSION_DEPTH - 1;
        let expr = format!("{}true{}", "(".repeat(n), ")".repeat(n));
        assert!(eval_bool(&expr, &Context::default()).unwrap());

        let unary_n = n - (n % 2);
        let negated = format!("{}true", "!".repeat(unary_n));
        assert!(eval_bool(&negated, &Context::default()).unwrap());

        let too_deep = format!(
            "{}true{}",
            "(".repeat(MAX_EXPRESSION_DEPTH),
            ")".repeat(MAX_EXPRESSION_DEPTH)
        );
        assert!(matches!(
            validate_expression(&too_deep),
            Err(ExpressionError::TooDeep(_))
        ));

        let too_deep_negated = format!("{}true", "!".repeat(MAX_EXPRESSION_DEPTH));
        assert!(matches!(
            validate_expression(&too_deep_negated),
            Err(ExpressionError::TooDeep(_))
        ));
    }

    #[test]
    fn deeply_nested_binary_chains_error_instead_of_overflowing() {
        for operator in ["||", "&&", "=="] {
            let mut expr = "true".to_owned();
            for _ in 0..1_000 {
                expr.push_str(operator);
                expr.push_str("true");
            }
            assert!(
                matches!(validate_expression(&expr), Err(ExpressionError::TooDeep(_))),
                "operator chain should be depth-limited: {operator}"
            );
        }
    }
}

#[test]
fn bracket_expression_key_indexes_dynamically() {
    // nushell ci.yml: ${{ env[matrix.target.options] }}
    let mut context = Context::default();
    context.insert("matrix", json!({"target": {"options": "--workspace"}}));
    context.insert("env", json!({"--workspace": "--workspace", "other": "x"}));

    assert_eq!(
        eval_expression("${{ env[matrix.target.options] }}", &context).unwrap(),
        Value::String("--workspace".to_owned())
    );
    // Missing key coalesces to null like other failed lookups.
    assert_eq!(
        eval_expression("${{ env[matrix.target.missing] }}", &context).unwrap(),
        Value::Null
    );
    // Literal keys keep the historical Path shape and value.
    assert_eq!(
        eval_expression("${{ env['other'] }}", &context).unwrap(),
        Value::String("x".to_owned())
    );
    assert_eq!(
        eval_expression("${{ env[other] }}", &context).unwrap(),
        Value::String("x".to_owned())
    );
    // Chained indexing past a dynamic key.
    assert_eq!(
        eval_expression("${{ matrix[matrix.target.options] }}", &context).unwrap(),
        Value::Null
    );
}
