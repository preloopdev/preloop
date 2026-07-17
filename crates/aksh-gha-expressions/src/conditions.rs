use serde_json::Value;

use super::trim_expression_markers;

/// Whether an expression contains a status-check call outside string literals.
///
/// GitHub's condition conversion adds an implicit `success()` gate unless the
/// expression calls `success`, `failure`, `cancelled`, or `always` itself.
pub fn contains_status_check_function(condition: &str) -> bool {
    let mut chars = condition.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            while let Some(quoted) = chars.next() {
                if quoted == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        if ch == '_' || ch.is_ascii_alphabetic() {
            let mut ident = String::from(ch);
            while let Some(next) = chars.peek().copied() {
                if next == '_' || next.is_ascii_alphanumeric() {
                    ident.push(next);
                    chars.next();
                } else {
                    break;
                }
            }
            while let Some(whitespace) = chars.peek().copied() {
                if whitespace.is_whitespace() {
                    chars.next();
                } else {
                    break;
                }
            }
            if matches!(
                ident.to_ascii_lowercase().as_str(),
                "success" | "failure" | "cancelled" | "always"
            ) && chars.peek() == Some(&'(')
            {
                return true;
            }
        }
    }
    false
}

/// Apply GitHub's implicit success gate to a job or step condition.
pub fn effective_condition(raw: Option<&str>) -> String {
    let condition = match raw {
        Some(condition) if !condition.trim().is_empty() => condition,
        _ => return "success()".to_owned(),
    };
    let stripped = trim_expression_markers(condition);
    if contains_status_check_function(stripped) {
        stripped.to_owned()
    } else {
        format!("success() && ({stripped})")
    }
}

/// GitHub Actions truthiness approximation.
pub fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0 && !n.is_nan()),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}
