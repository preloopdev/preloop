//! Template token evaluation for step fields.
//!
//! Evaluates `${{ }}` expressions in step fields (condition, inputs,
//! env, displayName, run script) using the runner-side expression engine.

use anyhow::Result;
use tracing::debug;

/// Evaluate all `${{ }}` expressions in a string.
pub fn evaluate_template(input: &str, ctx: &aksh_gha_expressions::Context) -> Result<String> {
    if !input.contains("${{") {
        return Ok(input.to_string());
    }

    let mut result = String::new();
    let mut rest = input;

    while let Some(start) = rest.find("${{") {
        // Copy text before the expression
        result.push_str(&rest[..start]);

        let expr_start = start + 3;
        let remaining = &rest[expr_start..];

        // Find the matching closing }} — must handle nested parens and quoted strings
        // that may contain }} (e.g. format('...{0}...', arg) where the template has
        // bash {{ }} brace groups).
        if let Some(end) = find_expression_end(remaining) {
            let expr = remaining[..end].trim();
            debug!("Evaluating expression: {expr}");

            match aksh_gha_expressions::eval_expression(expr, ctx) {
                Ok(value) => {
                    result.push_str(&value_to_string(&value));
                }
                Err(e) => {
                    // On expression error, preserve the original token
                    debug!("Expression evaluation failed: {e}");
                    result.push_str(&rest[start..expr_start + end + 2]);
                }
            }
            rest = &rest[start + 3 + end + 2..];
        } else {
            // No closing }}, copy the rest literally
            result.push_str(&rest[start..]);
            rest = "";
        }
    }

    result.push_str(rest);
    Ok(result)
}

/// Find the closing `}}` of a `${{ ... }}` expression, respecting string literals.
///
/// GitHub's control plane wraps multi-line `run:` scripts containing expressions
/// into `format('...{0}...', expr1, expr2)` calls. The format template string
/// may contain `}}` as literal brace escapes (e.g. bash `${{ }}` or `{ }` blocks).
/// We track single-quote depth so `}}` inside `'...'` literals is not treated
/// as the expression closer.
fn find_expression_end(s: &str) -> Option<usize> {
    let mut in_single_quote = false;
    let mut paren_depth: usize = 0;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' if !in_single_quote => {
                in_single_quote = true;
            }
            '\'' if in_single_quote => {
                // Check for escaped quote ('') inside string literal
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    i += 1; // skip escaped quote
                } else {
                    in_single_quote = false;
                }
            }
            '(' if !in_single_quote => {
                paren_depth += 1;
            }
            ')' if !in_single_quote && paren_depth > 0 => {
                paren_depth -= 1;
            }
            '}' if !in_single_quote && paren_depth == 0 => {
                // Check for }}
                if i + 1 < chars.len() && chars[i + 1] == '}' {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Convert a serde_json::Value to its display string (matching GitHub Actions semantics).
fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx() -> aksh_gha_expressions::Context {
        let mut ctx = aksh_gha_expressions::Context::new();
        ctx.insert(
            "github",
            serde_json::json!({
                "repository": "test/repo",
                "ref": "refs/heads/main",
            }),
        );
        ctx
    }

    #[test]
    fn no_expressions() {
        let ctx = make_ctx();
        assert_eq!(
            evaluate_template("hello world", &ctx).unwrap(),
            "hello world"
        );
    }

    #[test]
    fn simple_expression() {
        let ctx = make_ctx();
        let result = evaluate_template("repo: ${{ github.repository }}", &ctx).unwrap();
        assert_eq!(result, "repo: test/repo");
    }

    #[test]
    fn multiple_expressions() {
        let ctx = make_ctx();
        let result =
            evaluate_template("${{ github.repository }} on ${{ github.ref }}", &ctx).unwrap();
        assert_eq!(result, "test/repo on refs/heads/main");
    }

    #[test]
    fn passthrough_literal() {
        let ctx = make_ctx();
        let result = evaluate_template("plain text no expressions", &ctx).unwrap();
        assert_eq!(result, "plain text no expressions");
    }
}
