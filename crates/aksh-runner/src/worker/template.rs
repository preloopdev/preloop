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

/// Evaluate an `if:` condition expression as a boolean.
///
/// GitHub Actions `if:` conditions are implicitly wrapped in `${{ }}` if they
/// don't already contain it. The result is coerced to a boolean per GitHub's
/// truthy rules: empty string, "0", "false", null → false; everything else → true.
pub fn evaluate_condition(condition: &str, ctx: &aksh_gha_expressions::Context) -> Result<bool> {
    // If the condition already contains ${{ }}, evaluate as template
    let evaluated = if condition.contains("${{") {
        evaluate_template(condition, ctx)?
    } else {
        // Wrap in ${{ }} for implicit expression evaluation
        evaluate_template(&format!("${{{{ {condition} }}}}"), ctx)?
    };
    // Coerce to boolean per GitHub truthy rules
    let trimmed = evaluated.trim();
    Ok(!trimmed.is_empty()
        && trimmed != "0"
        && trimmed.to_lowercase() != "false"
        && trimmed != "null"
        && trimmed != "")
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

    // --- P1 expressions/templates gap coverage ---

    #[test]
    fn template_with_matrix_context() {
        let mut ctx = make_ctx();
        ctx.insert(
            "matrix",
            serde_json::json!({"os": "ubuntu-latest", "node": "20"}),
        );
        let result =
            evaluate_template("OS=${{ matrix.os }}, Node=${{ matrix.node }}", &ctx).unwrap();
        assert_eq!(result, "OS=ubuntu-latest, Node=20");
    }

    #[test]
    fn template_with_needs_context() {
        let mut ctx = make_ctx();
        ctx.insert(
            "needs",
            serde_json::json!({
                "build": {"outputs": {"sha": "abc123"}}
            }),
        );
        let result = evaluate_template("SHA=${{ needs.build.outputs.sha }}", &ctx).unwrap();
        assert_eq!(result, "SHA=abc123");
    }

    #[test]
    fn template_with_env_context() {
        let mut ctx = make_ctx();
        ctx.insert("env", serde_json::json!({"MY_VAR": "hello"}));
        let result = evaluate_template("val=${{ env.MY_VAR }}", &ctx).unwrap();
        assert_eq!(result, "val=hello");
    }

    #[test]
    fn template_evaluates_boolean_to_string() {
        let ctx = make_ctx();
        // success() returns true, which should render as "true"
        let result = evaluate_template("status=${{ success() }}", &ctx).unwrap();
        assert_eq!(result, "status=true");
    }

    #[test]
    fn template_evaluates_number_to_string() {
        let mut ctx = make_ctx();
        ctx.insert("matrix", serde_json::json!({"timeout": 10}));
        let result = evaluate_template("timeout=${{ matrix.timeout }}", &ctx).unwrap();
        assert_eq!(result, "timeout=10");
    }

    #[test]
    fn template_null_renders_empty() {
        let mut ctx = make_ctx();
        ctx.insert("matrix", serde_json::json!({"missing": null}));
        let result = evaluate_template("val=${{ matrix.missing }}", &ctx).unwrap();
        assert_eq!(result, "val=");
    }

    #[test]
    fn template_unresolved_context_renders_empty() {
        let ctx = make_ctx();
        let result = evaluate_template("val=${{ matrix.nonexistent }}", &ctx).unwrap();
        assert_eq!(result, "val=");
    }

    #[test]
    fn template_mixed_literal_and_expression() {
        let ctx = make_ctx();
        let result = evaluate_template(
            "echo Running on ${{ github.repository }} ref ${{ github.ref }} done",
            &ctx,
        )
        .unwrap();
        assert_eq!(result, "echo Running on test/repo ref refs/heads/main done");
    }
}
