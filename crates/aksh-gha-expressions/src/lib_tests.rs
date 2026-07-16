use proptest::prelude::*;
use serde_json::json;

use super::*;

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
fn evaluates_json_join_and_comparisons() {
    let context = Context::default();

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
        eval_expression("hashFiles('Cargo.toml')", &context).unwrap(),
        Value::String(String::new())
    );
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
    let dir = std::path::Path::new("/tmp/aksh-hashfiles-test");
    std::fs::create_dir_all(dir).unwrap();

    // Write two known files
    let a = dir.join("a.txt");
    let b = dir.join("b.txt");
    std::fs::write(&a, b"hello aksh").unwrap();
    std::fs::write(&b, b"hello world").unwrap();

    let ctx = Context::default().with_workspace(dir.to_string_lossy().to_string());

    // Single file: hashFiles('a.txt') should equal SHA256(binary concat of SHA256(content))
    let ha = eval_expression("hashFiles('a.txt')", &ctx).unwrap();
    let expected_a: String = {
        let inner = Sha256::digest(b"hello aksh");
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
