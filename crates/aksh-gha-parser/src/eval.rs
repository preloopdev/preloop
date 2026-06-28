//! Expression evaluation for workflow fields.
//!
//! Wires `aksh-gha-expressions` to resolve `${{ }}` in workflow fields
//! and build the `contextData` the runner needs for expression evaluation.

//!
//! Design principle: we resolve `${{ }}` in string fields that the
//! *server* owns (env, with, run, runs-on). We emit the raw expression
//! string for `if` conditions — the runner evaluates those itself.

use std::collections::BTreeMap;

use aksh_gha_expressions::{eval_expression, Context};
use indexmap::IndexMap;
use serde_json::{Map, Value};

/// Resolve all `${{ }}` expressions in a string using the given context.
pub fn resolve_string(input: &str, context: &Context) -> Result<String, String> {
    if !input.contains("${{") {
        return Ok(input.to_owned());
    }

    let mut result = String::new();
    let mut remaining = input;

    while let Some(start) = remaining.find("${{") {
        result.push_str(&remaining[..start]);
        remaining = &remaining[start + 3..];

        let end = find_expression_end(remaining).ok_or("unclosed ${{ expression")?;
        let expr = &remaining[..end].trim();
        remaining = &remaining[end + 2..];

        let value = eval_expression(expr, context).map_err(|e| format!("{e}"))?;
        result.push_str(&stringify_value(&value));
    }

    result.push_str(remaining);
    Ok(result)
}

fn find_expression_end(input: &str) -> Option<usize> {
    // Mirror the single-quoted string rules of `aksh-gha-expressions`'s lexer:
    // only `'` opens/closes a string, doubled `''` is an escaped quote, and
    // backslash is treated as an ordinary character (no C-style escapes).
    let mut chars = input.char_indices().peekable();
    let mut in_string = false;
    while let Some((index, ch)) = chars.next() {
        if in_string {
            if ch == '\'' {
                if matches!(chars.peek(), Some(&(_, '\''))) {
                    chars.next();
                    continue;
                }
                in_string = false;
            }
            continue;
        }
        match ch {
            '\'' => in_string = true,
            '}' if input[index..].starts_with("}}") => return Some(index),
            _ => {}
        }
    }
    None
}

/// Check if a string contains any `${{ }}` expressions.
pub fn has_expressions(input: &str) -> bool {
    input.contains("${{")
}

/// Resolve a map of string values, evaluating all `${{ }}` expressions.
pub fn resolve_map(
    map: &BTreeMap<String, String>,
    context: &Context,
) -> Result<BTreeMap<String, String>, String> {
    let mut resolved = BTreeMap::new();
    for (key, value) in map {
        resolved.insert(key.clone(), resolve_string(value, context)?);
    }
    Ok(resolved)
}

/// Resolve a JSON value, evaluating any `${{ }}` expressions in strings.
pub fn resolve_json(value: &Value, context: &Context) -> Result<Value, String> {
    match value {
        Value::String(s) => {
            if has_expressions(s) {
                Ok(Value::String(resolve_string(s, context)?))
            } else {
                Ok(value.clone())
            }
        }
        Value::Array(arr) => {
            let resolved: Result<Vec<_>, _> =
                arr.iter().map(|v| resolve_json(v, context)).collect();
            Ok(Value::Array(resolved?))
        }
        Value::Object(map) => {
            let mut resolved = Map::new();
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_json(v, context)?);
            }
            Ok(Value::Object(resolved))
        }
        _ => Ok(value.clone()),
    }
}

/// Convert a JSON value to its string representation.
fn stringify_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Array(a) => serde_json::to_string(a).unwrap_or_default(),
        Value::Object(o) => serde_json::to_string(o).unwrap_or_default(),
    }
}

/// Build an expression evaluation context from workflow data.
pub fn build_context(
    github: &Value,
    env: &BTreeMap<String, String>,
    vars: &BTreeMap<String, String>,
    matrix: &IndexMap<String, Value>,
    strategy: &Value,
    secrets: &BTreeMap<String, String>,
) -> Context {
    let mut ctx = Context::default();

    ctx.insert("github", github.clone());

    let env_value: Map<String, Value> = env
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    ctx.insert("env", Value::Object(env_value));

    let vars_value: Map<String, Value> = vars
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    ctx.insert("vars", Value::Object(vars_value));

    let matrix_value: Map<String, Value> =
        matrix.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    ctx.insert("matrix", Value::Object(matrix_value));

    ctx.insert("strategy", strategy.clone());
    ctx.insert("needs", Value::Object(Map::new()));

    let secrets_value: Map<String, Value> = secrets
        .iter()
        .map(|(k, _)| (k.clone(), Value::String("***".to_owned())))
        .collect();
    ctx.insert("secrets", Value::Object(secrets_value));

    ctx
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_context() -> Context {
        let mut ctx = Context::default();
        ctx.insert(
            "github",
            json!({
                "event_name": "push",
                "ref": "refs/heads/main",
                "sha": "abc123"
            }),
        );
        let mut env = Map::new();
        env.insert("MY_VAR".to_owned(), Value::String("hello".to_owned()));
        ctx.insert("env", Value::Object(env));
        ctx.insert("matrix", json!({"os": "ubuntu-latest", "node": "18"}));
        ctx
    }

    #[test]
    fn resolve_literal_string() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("no expressions", &ctx).unwrap(),
            "no expressions"
        );
    }

    #[test]
    fn resolve_single_expression() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("${{ github.event_name }}", &ctx).unwrap(),
            "push"
        );
    }

    #[test]
    fn resolve_mixed_literal_and_expression() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("ref=${{ github.ref }}", &ctx).unwrap(),
            "ref=refs/heads/main"
        );
    }

    #[test]
    fn expression_end_ignores_braces_inside_string_literals() {
        let source = r#" format('value }} still inside') }}"#;
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn expression_end_treats_backslash_as_literal_in_single_quotes() {
        // Backslash is NOT an escape in the lexer's single-quoted strings.
        // The first `'` closes the string, then `}}` should terminate.
        let source = r"'C:\' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn expression_end_handles_doubled_quote_escape() {
        // `''` inside a single-quoted string is an escaped quote, not a close.
        let source = r"'it''s fine' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn resolve_matrix_value() {
        let ctx = make_context();
        assert_eq!(
            resolve_string("${{ matrix.os }}", &ctx).unwrap(),
            "ubuntu-latest"
        );
    }

    #[test]
    fn resolve_env_value() {
        let ctx = make_context();
        assert_eq!(resolve_string("${{ env.MY_VAR }}", &ctx).unwrap(), "hello");
    }

    #[test]
    fn resolve_map_expressions() {
        let ctx = make_context();
        let mut map = BTreeMap::new();
        map.insert("key".to_owned(), "value".to_owned());
        map.insert("expr".to_owned(), "${{ matrix.os }}".to_owned());
        let resolved = resolve_map(&map, &ctx).unwrap();
        assert_eq!(resolved["key"], "value");
        assert_eq!(resolved["expr"], "ubuntu-latest");
    }

    #[test]
    fn unclosed_expression_returns_error() {
        let ctx = make_context();
        assert!(resolve_string("${{ github.event_name", &ctx).is_err());
    }
}
