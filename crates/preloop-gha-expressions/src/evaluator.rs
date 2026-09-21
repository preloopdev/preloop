use serde_json::Value;
use std::borrow::Cow;
use std::io::Write;

use super::{
    ast::{BinaryOp, Expr},
    conditions::is_truthy,
    context::Context,
    ContextFunctionCall, ExpressionError, ExpressionReferences,
};

fn function_arity(name: &str) -> Option<(usize, usize)> {
    match name {
        "always" | "cancelled" => Some((0, 0)),
        // Job-level status functions accept dependency names. Step-level
        // validation narrows these to zero arguments from the schema context.
        "success" | "failure" => Some((0, 255)),
        "contains" | "startswith" | "endswith" => Some((2, 2)),
        "format" => Some((1, 255)),
        "fromjson" | "tojson" => Some((1, 1)),
        "join" => Some((1, 2)),
        "hashfiles" => Some((1, 255)),
        "case" => Some((3, 255)),
        _ => None,
    }
}

fn validate_function_arity(name: &str, actual: usize) -> Result<(), ExpressionError> {
    let lower = name.to_ascii_lowercase();
    let Some((min, max)) = function_arity(&lower) else {
        return Err(ExpressionError::UnknownFunction(name.to_owned()));
    };
    if lower == "case" && actual.is_multiple_of(2) {
        return Err(ExpressionError::EvenCaseParameters);
    }
    if !(min..=max).contains(&actual) {
        return Err(ExpressionError::InvalidFunctionArity {
            name: name.to_owned(),
            min,
            max,
            actual,
        });
    }
    Ok(())
}

pub(super) fn validate_function_calls(expr: &Expr) -> Result<(), ExpressionError> {
    match expr {
        Expr::Literal(_) | Expr::Path(_) => Ok(()),
        Expr::UnaryNot(inner) | Expr::MemberAccess { expr: inner, .. } => {
            validate_function_calls(inner)
        }
        Expr::Index { base, key } => {
            validate_function_calls(base)?;
            validate_function_calls(key)
        }
        Expr::Binary { left, right, .. } => {
            validate_function_calls(left)?;
            validate_function_calls(right)
        }
        Expr::Call { name, args } => {
            validate_function_arity(name, args.len())?;
            args.iter().try_for_each(validate_function_calls)
        }
    }
}

/// Collect top-level data contexts and context-sensitive function calls.
pub(super) fn collect_expression_references_from_expr(
    expr: &Expr,
    references: &mut ExpressionReferences,
) {
    match expr {
        Expr::Path(path) => {
            if let Some(first) = path.first() {
                references.contexts.insert(first.to_ascii_lowercase());
            }
        }
        Expr::Literal(_) => {}
        Expr::UnaryNot(inner) | Expr::MemberAccess { expr: inner, .. } => {
            collect_expression_references_from_expr(inner, references);
        }
        Expr::Index { base, key } => {
            collect_expression_references_from_expr(base, references);
            collect_expression_references_from_expr(key, references);
        }
        Expr::Binary { left, right, .. } => {
            collect_expression_references_from_expr(left, references);
            collect_expression_references_from_expr(right, references);
        }
        Expr::Call { name, args } => {
            let lower = name.to_ascii_lowercase();
            if matches!(
                lower.as_str(),
                "always" | "success" | "failure" | "cancelled" | "hashfiles"
            ) {
                references.contexts.insert(lower.clone());
                references.functions.push(ContextFunctionCall {
                    name: lower,
                    argument_count: args.len(),
                });
            }
            for arg in args {
                collect_expression_references_from_expr(arg, references);
            }
        }
    }
}

pub(super) const MAX_EVALUATED_VALUE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub(super) struct EvalBudget {
    used_bytes: usize,
}

impl EvalBudget {
    fn remaining(&self) -> usize {
        MAX_EVALUATED_VALUE_BYTES.saturating_sub(self.used_bytes)
    }

    fn charge(&mut self, value: &Value) -> Result<(), ExpressionError> {
        self.used_bytes = self.used_bytes.checked_add(value_size(value)).ok_or(
            ExpressionError::EvaluationTooLarge(MAX_EVALUATED_VALUE_BYTES),
        )?;
        if self.used_bytes > MAX_EVALUATED_VALUE_BYTES {
            return Err(ExpressionError::EvaluationTooLarge(
                MAX_EVALUATED_VALUE_BYTES,
            ));
        }
        Ok(())
    }
}

fn value_size(value: &Value) -> usize {
    match value {
        // Include conservative per-value/container overhead. Counting only
        // payload bytes lets large arrays of nulls or empty strings evade the
        // evaluation budget while still consuming substantial heap memory.
        Value::Null => 16,
        Value::Bool(_) => 16,
        Value::Number(_) => 32,
        Value::String(value) => 24 + value.len(),
        Value::Array(values) => {
            24 + values
                .iter()
                .map(|value| 16 + value_size(value))
                .sum::<usize>()
        }
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| 32 + key.len() + value_size(value))
            .sum(),
    }
}

pub(super) fn eval(
    expr: &Expr,
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    // Keep this dispatcher thin: expressions nested up to the parser's depth
    // ceiling recurse through it, and every frame on the recursion path is
    // stack a within-ceiling expression may consume. Heavy per-op logic lives
    // in non-recursive helpers so the frames pushed per nesting level stay
    // small (a fat frame here made `!`×254 and `a==b==c` chains overflow the
    // test thread's 2 MiB stack).
    let value = match expr {
        Expr::Literal(value) => {
            budget.charge(value)?;
            return Ok(value.clone());
        }
        Expr::Path(path) => {
            if let Some(value) = context.resolve_ref(path) {
                budget.charge(value)?;
                return Ok(value.clone());
            }
            Ok(context.resolve(path))
        }
        Expr::UnaryNot(expr) => eval_not(expr, context, budget),
        Expr::Binary { .. } => eval_binary(expr, context, budget),
        Expr::Call { name, args } => eval_call(name, args, context, budget),
        Expr::MemberAccess { expr, path } => eval_member(expr, path, context, budget),
        Expr::Index { base, key } => eval_index(base, key, context, budget),
    }?;
    budget.charge(&value)?;
    Ok(value)
}

fn eval_not(
    expr: &Expr,
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    Ok(Value::Bool(!is_truthy(&eval(expr, context, budget)?)))
}

fn eval_member(
    expr: &Expr,
    path: &[String],
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    let base = eval(expr, context, budget)?;
    Ok(Context::resolve_value(base, path))
}

fn eval_index(
    base: &Expr,
    key: &Expr,
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    let base = eval(base, context, budget)?;
    let key = string_value(&eval(key, context, budget)?);
    match &base {
        Value::Object(map) => Ok(map.get(&key).cloned().unwrap_or(Value::Null)),
        Value::Array(items) => Ok(key
            .parse::<usize>()
            .ok()
            .and_then(|index| items.get(index).cloned())
            .unwrap_or(Value::Null)),
        _ => Ok(Value::Null),
    }
}

fn eval_binary(
    expr: &Expr,
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    let Expr::Binary { op, left, right } = expr else {
        unreachable!("eval_binary requires a binary expression");
    };
    let left = eval(left, context, budget)?;
    // Short-circuiting must not evaluate the right operand.
    match op {
        BinaryOp::Or if is_truthy(&left) => return Ok(left),
        BinaryOp::And if !is_truthy(&left) => return Ok(left),
        _ => {}
    }
    let right = eval(right, context, budget)?;
    Ok(combine_binary(op, left, right))
}

fn combine_binary(op: &BinaryOp, left: Value, right: Value) -> Value {
    match op {
        // Short-circuiting already returned when `left` decides the result;
        // `right` is only evaluated when it must be the value.
        BinaryOp::Or | BinaryOp::And => right,
        BinaryOp::Eq => Value::Bool(abstract_equal(&left, &right)),
        BinaryOp::Ne => Value::Bool(!abstract_equal(&left, &right)),
        BinaryOp::Gt => Value::Bool(compare_values(&left, &right, |ordering| ordering.is_gt())),
        BinaryOp::Ge => Value::Bool(compare_values(&left, &right, |ordering| ordering.is_ge())),
        BinaryOp::Lt => Value::Bool(compare_values(&left, &right, |ordering| ordering.is_lt())),
        BinaryOp::Le => Value::Bool(compare_values(&left, &right, |ordering| ordering.is_le())),
    }
}

fn abstract_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => eq_ordinal_ignore_case(left, right),
        (Value::Array(_), Value::Array(_)) | (Value::Object(_), Value::Object(_)) => {
            std::ptr::eq(left, right)
        }
        (Value::Array(_) | Value::Object(_), _) | (_, Value::Array(_) | Value::Object(_)) => false,
        _ => {
            let left = numeric_value(left);
            let right = numeric_value(right);
            left.zip(right)
                .is_some_and(|(left, right)| !left.is_nan() && !right.is_nan() && left == right)
        }
    }
}

fn compare_values(
    left_value: &Value,
    right_value: &Value,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> bool {
    // Official runner (EvaluationResult.AbstractGreaterThan): when both
    // operands are strings, they compare ordinally with OrdinalIgnoreCase —
    // numeric-looking strings are NEVER coerced to numbers. Previously we
    // tried numeric conversion first, which inverted results like
    // '10' > '9' (we said true, official says false).
    if let (Value::String(left), Value::String(right)) = (left_value, right_value) {
        return predicate(compare_ordinal_ignore_case(left, right));
    }
    if let (Some(left), Some(right)) = (numeric_value(left_value), numeric_value(right_value)) {
        if let Some(ordering) = left.partial_cmp(&right) {
            return predicate(ordering);
        }
        // A failed numeric conversion for mixed values is not a string
        // comparison; preserve the runner's false result for NaN.
        return false;
    }
    predicate(compare_ordinal_ignore_case(
        &string_value(left_value),
        &string_value(right_value),
    ))
}

/// Simple uppercase mapping with no multi-character expansion.
///
/// Matches the per-character simple case mapping .NET uses for
/// `StringComparison.OrdinalIgnoreCase`: a character whose uppercase form
/// expands (e.g. 'ß' → "SS", 'İ' → 'i' + combining dot) keeps its original
/// form, exactly as the invariant simple-case table does. Lowercasing the
/// whole string instead would wrongly equate 'İ' with 'i\u{0307}' and miss
/// equivalences like final sigma 'ς' == 'Σ'.
fn to_simple_upper(c: char) -> char {
    let mut upper = c.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(mapped), None) => mapped,
        // No mapping or an expanding mapping: keep the character as-is.
        _ => c,
    }
}

/// Case-insensitive ordinal string comparison.
///
/// Matches .NET `StringComparison.OrdinalIgnoreCase`: each character is
/// mapped through the simple (non-expanding) uppercase mapping, then
/// compared ordinally. Comparing scalar values is equivalent to comparing
/// UTF-16 code units because UTF-16 preserves code point order.
fn compare_ordinal_ignore_case(left: &str, right: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if left.is_ascii() && right.is_ascii() {
        return left.to_ascii_uppercase().cmp(&right.to_ascii_uppercase());
    }
    let mut left_chars = left.chars().map(to_simple_upper);
    let mut right_chars = right.chars().map(to_simple_upper);
    loop {
        match (left_chars.next(), right_chars.next()) {
            (Some(l), Some(r)) => match l.cmp(&r) {
                Ordering::Equal => {}
                ordering => return ordering,
            },
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => return Ordering::Equal,
        }
    }
}

/// Case-insensitive ordinal prefix test.
/// Matches .NET `string.StartsWith(..., OrdinalIgnoreCase)`.
fn starts_with_ordinal_ignore_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.is_ascii() && needle.is_ascii() {
        let haystack = haystack.as_bytes();
        let needle = needle.as_bytes();
        return haystack.len() >= needle.len()
            && haystack[..needle.len()]
                .iter()
                .zip(needle.iter())
                .all(|(h, n)| h.eq_ignore_ascii_case(n));
    }
    let mut haystack_chars = haystack.chars().map(to_simple_upper);
    needle
        .chars()
        .map(to_simple_upper)
        .all(|n| match haystack_chars.next() {
            Some(h) => h == n,
            None => false,
        })
}

/// Case-insensitive ordinal suffix test.
/// Matches .NET `string.EndsWith(..., OrdinalIgnoreCase)`.
fn ends_with_ordinal_ignore_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.is_ascii() && needle.is_ascii() {
        let haystack = haystack.as_bytes();
        let needle = needle.as_bytes();
        return haystack.len() >= needle.len()
            && haystack[haystack.len() - needle.len()..]
                .iter()
                .zip(needle.iter())
                .all(|(h, n)| h.eq_ignore_ascii_case(n));
    }
    // The mapping is per-character, so comparing the reversed streams is
    // equivalent to comparing the suffix.
    let mut haystack_chars = haystack.chars().rev().map(to_simple_upper);
    needle
        .chars()
        .rev()
        .map(to_simple_upper)
        .all(|n| match haystack_chars.next() {
            Some(h) => h == n,
            None => false,
        })
}

/// Case-insensitive ordinal substring test.
/// Matches .NET `string.Contains(..., OrdinalIgnoreCase)`.
fn contains_ordinal_ignore_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.is_ascii() && needle.is_ascii() {
        return haystack
            .to_ascii_uppercase()
            .contains(&needle.to_ascii_uppercase());
    }
    let needle: Vec<char> = needle.chars().map(to_simple_upper).collect();
    let haystack: Vec<char> = haystack.chars().map(to_simple_upper).collect();
    haystack
        .windows(needle.len())
        .any(|w| w == needle.as_slice())
}

/// Case-insensitive string equality, Unicode-aware.
/// Matches .NET String.Equals(..., OrdinalIgnoreCase).
fn eq_ordinal_ignore_case(left: &str, right: &str) -> bool {
    compare_ordinal_ignore_case(left, right) == std::cmp::Ordering::Equal
}

fn numeric_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => Some(parse_number(value)),
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) | Value::Null => Some(0.0),
        _ => None,
    }
}

fn parse_number(value: &str) -> f64 {
    let value = value.trim();
    if value.is_empty() {
        return 0.0;
    }
    if value == "Infinity" {
        return f64::INFINITY;
    }
    if value == "-Infinity" {
        return f64::NEG_INFINITY;
    }
    if let Some(hex) = value.strip_prefix("0x") {
        return i32::from_str_radix(hex, 16)
            .map(f64::from)
            .unwrap_or(f64::NAN);
    }
    if let Some(octal) = value.strip_prefix("0o") {
        return i32::from_str_radix(octal, 8)
            .map(f64::from)
            .unwrap_or(f64::NAN);
    }
    if is_decimal_number(value) {
        value.parse().unwrap_or(f64::NAN)
    } else {
        f64::NAN
    }
}

fn is_decimal_number(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let mut digits = 0;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index += 1;
        digits += 1;
    }
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return false;
    }
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index == exponent_start {
            return false;
        }
    }
    index == bytes.len()
}

fn eval_call(
    name: &str,
    args: &[Expr],
    context: &Context,
    budget: &mut EvalBudget,
) -> Result<Value, ExpressionError> {
    let lower = name.to_ascii_lowercase();
    validate_function_arity(name, args.len())?;
    // case() uses lazy evaluation — handle before eager collect
    if lower == "case" {
        // Evaluate predicate-result pairs lazily
        for i in (0..args.len() - 1).step_by(2) {
            let predicate = eval(&args[i], context, budget)?;
            if !predicate.is_boolean() {
                return Err(ExpressionError::NonBooleanCasePredicate);
            }
            if predicate.as_bool().unwrap_or(false) {
                return eval(&args[i + 1], context, budget);
            }
        }
        // No predicate matched — return default (last arg)
        return eval(&args[args.len() - 1], context, budget);
    }
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        values.push(eval(arg, context, budget)?);
    }

    match lower.as_str() {
        "always" => Ok(Value::Bool(true)),
        "success" => Ok(Value::Bool(context.success)),
        "failure" => Ok(Value::Bool(context.failure)),
        "cancelled" => Ok(Value::Bool(context.cancelled)),
        "contains" => {
            Ok(Value::Bool(values.first().zip(values.get(1)).is_some_and(
                |(haystack, needle)| contains(haystack, needle),
            )))
        }
        "startswith" => Ok(Value::Bool({
            // Unicode-aware case-insensitive (official: OrdinalIgnoreCase).
            starts_with_ordinal_ignore_case(&string_arg(&values, 0), &string_arg(&values, 1))
        })),
        "endswith" => Ok(Value::Bool({
            // Unicode-aware case-insensitive (official: OrdinalIgnoreCase).
            ends_with_ordinal_ignore_case(&string_arg(&values, 0), &string_arg(&values, 1))
        })),
        "format" => format_args(&values, budget).map(Value::String),
        "fromjson" => {
            // Official runner (FromJson.cs): JToken.ReadFrom throws on invalid
            // JSON — the error propagates and fails the job. Previously we
            // swallowed the error and returned null, letting workflows continue
            // that official would fail.
            let input = values.first().map(string_value).unwrap_or_default();
            from_json_lenient(&input).map_err(ExpressionError::InvalidJson)
        }
        "join" => join_args(&values, budget).map(Value::String),
        "hashfiles" => hash_files(&values, context).map(Value::String),
        "tojson" => {
            to_json_pretty(values.first().unwrap_or(&Value::Null), budget).map(Value::String)
        }
        _ => Err(ExpressionError::UnknownFunction(name.to_owned())),
    }
}

fn contains(haystack: &Value, needle: &Value) -> bool {
    match haystack {
        // Unicode-aware case-insensitive contains (official: OrdinalIgnoreCase).
        Value::String(value) => contains_ordinal_ignore_case(value, &string_value(needle)),
        Value::Array(values) => values.iter().any(|value| abstract_equal(value, needle)),
        _ => false,
    }
}

/// Parse JSON the way the official runner's `fromJSON` does.
///
/// The official implementation reads through Newtonsoft's `JsonTextReader`,
/// which accepts two extensions over strict JSON: single-quoted strings
/// (including property names) and trailing commas before `}`/`]`. Try strict
/// parsing first; only when that fails, normalize those two extensions and
/// retry. Anything else is still an error — unlike JSON5 we do not accept
/// unquoted keys, comments, hex numbers, or `NaN`/`Infinity`.
fn from_json_lenient(input: &str) -> Result<Value, String> {
    match serde_json::from_str(input) {
        Ok(value) => Ok(value),
        Err(strict_error) => {
            if let Some(normalized) = normalize_newtonsoft_json(input) {
                if let Ok(value) = serde_json::from_str(&normalized) {
                    return Ok(value);
                }
            }
            Err(strict_error.to_string())
        }
    }
}

/// Rewrite the Newtonsoft `JsonTextReader` extensions into strict JSON:
/// single-quoted strings become double-quoted, comments are removed, and
/// trailing commas before `}`/`]` are dropped only after a value. Returns
/// `None` when the input needed no rewriting, or when it contains an
/// unterminated block comment.
fn normalize_newtonsoft_json(input: &str) -> Option<String> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut containers: Vec<char> = Vec::new();
    let mut previous_was_value = false;
    let mut changed = false;
    let mut index = 0;

    // Find the next non-whitespace, non-comment character without consuming
    // the original input. This lets comma handling distinguish `[1,]` from
    // `[,]` and still recognize comments between a value and its closer.
    let next_non_comment_character = |mut index: usize| -> Option<char> {
        loop {
            while chars
                .get(index)
                .is_some_and(|character| character.is_whitespace())
            {
                index += 1;
            }
            match chars.get(index).copied() {
                Some('/') if chars.get(index + 1) == Some(&'/') => {
                    index += 2;
                    while chars.get(index).is_some_and(|character| *character != '\n') {
                        index += 1;
                    }
                }
                Some('/') if chars.get(index + 1) == Some(&'*') => {
                    index += 2;
                    let mut closed = false;
                    while index + 1 < chars.len() {
                        if chars[index] == '*' && chars[index + 1] == '/' {
                            index += 2;
                            closed = true;
                            break;
                        }
                        index += 1;
                    }
                    if !closed {
                        return None;
                    }
                }
                Some(character) => return Some(character),
                None => return None,
            }
        }
    };

    while index < chars.len() {
        match chars[index] {
            '"' => {
                // Copy double-quoted strings verbatim (escapes included) so
                // quotes and comment markers inside them stay data.
                out.push('"');
                index += 1;
                let mut escaped = false;
                while index < chars.len() {
                    let character = chars[index];
                    out.push(character);
                    index += 1;
                    if escaped {
                        escaped = false;
                    } else if character == '\\' {
                        escaped = true;
                    } else if character == '"' {
                        break;
                    }
                }
                previous_was_value = true;
            }
            '\'' => {
                // Single-quoted string -> double-quoted string.
                changed = true;
                out.push('"');
                index += 1;
                let mut escaped = false;
                while index < chars.len() {
                    let character = chars[index];
                    index += 1;
                    if escaped {
                        escaped = false;
                        match character {
                            '\'' => out.push('\''),
                            '"' => out.push_str("\\\""),
                            _ => {
                                out.push('\\');
                                out.push(character);
                            }
                        }
                    } else if character == '\\' {
                        escaped = true;
                    } else if character == '\'' {
                        out.push('"');
                        break;
                    } else if character == '"' {
                        out.push_str("\\\"");
                    } else {
                        out.push(character);
                    }
                }
                previous_was_value = true;
            }
            '/' if chars.get(index + 1) == Some(&'/') => {
                // Newtonsoft accepts both line and block comments. Replace
                // comments with whitespace so adjacent tokens do not merge;
                // preserve the line ending for line-comment formatting.
                changed = true;
                out.push(' ');
                index += 2;
                while chars.get(index).is_some_and(|character| *character != '\n') {
                    index += 1;
                }
            }
            '/' if chars.get(index + 1) == Some(&'*') => {
                changed = true;
                out.push(' ');
                index += 2;
                let mut closed = false;
                while index + 1 < chars.len() {
                    if chars[index] == '*' && chars[index + 1] == '/' {
                        index += 2;
                        closed = true;
                        break;
                    }
                    index += 1;
                }
                if !closed {
                    return None;
                }
            }
            ',' => {
                let trailing = matches!(
                    (
                        containers.last().copied(),
                        next_non_comment_character(index + 1),
                    ),
                    (Some('{'), Some('}')) | (Some('['), Some(']'))
                );
                if previous_was_value && trailing {
                    changed = true;
                } else {
                    out.push(',');
                    previous_was_value = false;
                }
                index += 1;
            }
            '{' | '[' => {
                let opener = chars[index];
                containers.push(opener);
                out.push(opener);
                previous_was_value = false;
                index += 1;
            }
            '}' | ']' => {
                let closer = chars[index];
                let opener = if closer == '}' { '{' } else { '[' };
                if containers.last().copied() == Some(opener) {
                    containers.pop();
                }
                out.push(closer);
                previous_was_value = true;
                index += 1;
            }
            ':' => {
                out.push(':');
                previous_was_value = false;
                index += 1;
            }
            character if character.is_whitespace() => {
                out.push(character);
                index += 1;
            }
            character => {
                // Numbers, literals, and invalid bare tokens are validated by
                // the strict serde_json retry after normalization.
                out.push(character);
                previous_was_value = true;
                index += 1;
            }
        }
    }
    changed.then_some(out)
}

fn string_arg(values: &[Value], index: usize) -> String {
    values.get(index).map(string_value).unwrap_or_default()
}

fn string_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => {
            // GitHub Actions renders whole numbers as integers, not floats.
            // serde_yaml 0.9 may deserialise YAML integer `1` as f64(1.0),
            // which serde_json prints as "1.0".  Normalise: if the number has
            // no fractional part, emit it as a plain integer string.
            if let Some(i) = value.as_i64() {
                return i.to_string();
            }
            if let Some(u) = value.as_u64() {
                return u.to_string();
            }
            if let Some(f) = value.as_f64() {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    return (f as i64).to_string();
                }
            }
            value.to_string()
        }
        Value::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

struct CappedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for CappedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.bytes.len());
        if bytes.len() > remaining {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "serialized value exceeds evaluation budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn string_value_bounded<'a>(
    value: &'a Value,
    limit: usize,
    overflow: impl Fn() -> ExpressionError,
) -> Result<Cow<'a, str>, ExpressionError> {
    match value {
        Value::String(value) => Ok(Cow::Borrowed(value)),
        Value::Null => Ok(Cow::Borrowed("")),
        Value::Bool(value) => Ok(Cow::Owned(value.to_string())),
        Value::Number(value) => Ok(Cow::Owned(string_value(&Value::Number(value.clone())))),
        other => {
            let mut writer = CappedJsonWriter {
                bytes: Vec::with_capacity(limit.min(256)),
                limit,
            };
            serde_json::to_writer(&mut writer, other).map_err(|_| overflow())?;
            let value = String::from_utf8(writer.bytes).map_err(|_| overflow())?;
            Ok(Cow::Owned(value))
        }
    }
}

fn string_value_capped<'a>(
    value: &'a Value,
    limit: usize,
) -> Result<Cow<'a, str>, ExpressionError> {
    string_value_bounded(value, limit, || {
        ExpressionError::EvaluationTooLarge(MAX_EVALUATED_VALUE_BYTES)
    })
}

/// Serialize a value the way the official runner's `toJSON` does.
///
/// Official (ToJson.cs) writes through Newtonsoft's `JsonTextWriter` with
/// `Formatting.Indented`: two-space indent and `Environment.NewLine`
/// separators — `\r\n` on Windows, `\n` elsewhere. serde_json always emits
/// `\n`, so on Windows the bytes pass through a translating writer.
///
/// Serialization is bounded by the remaining evaluation budget: pretty
/// indentation can multiply the size of a deeply nested value, and an
/// uncapped `to_string_pretty` would allocate the whole string before the
/// result is charged, defeating the temporary-memory ceiling.
fn to_json_pretty(value: &Value, budget: &EvalBudget) -> Result<String, ExpressionError> {
    let too_large = || ExpressionError::EvaluationTooLarge(MAX_EVALUATED_VALUE_BYTES);
    let bytes = to_json_pretty_bytes(value, budget.remaining()).map_err(|_| too_large())?;
    String::from_utf8(bytes).map_err(|_| too_large())
}

#[cfg(not(windows))]
fn to_json_pretty_bytes(value: &Value, limit: usize) -> Result<Vec<u8>, serde_json::Error> {
    let mut writer = CappedJsonWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer_pretty(&mut writer, value)?;
    Ok(writer.bytes)
}

#[cfg(windows)]
fn to_json_pretty_bytes(value: &Value, limit: usize) -> Result<Vec<u8>, serde_json::Error> {
    let mut writer = CrLfWriter {
        inner: CappedJsonWriter {
            bytes: Vec::new(),
            limit,
        },
    };
    serde_json::to_writer_pretty(&mut writer, value)?;
    Ok(writer.inner.bytes)
}

/// Translates `\n` into `\r\n` so pretty JSON uses `Environment.NewLine`
/// like the official runner's `JsonTextWriter` on Windows.
#[cfg(windows)]
struct CrLfWriter {
    inner: CappedJsonWriter,
}

#[cfg(windows)]
impl Write for CrLfWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for &byte in bytes {
            if byte == b'\n' {
                self.inner.write_all(b"\r\n")?;
            } else {
                self.inner.write_all(&[byte])?;
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Cap on the output of `format()`.
///
/// Nested `format()` calls multiply output per level — each `{0}{0}{0}`
/// triples its argument — so a ~1.5 KB expression could otherwise demand
/// gigabytes (reproduced: 6.2 GB resident, process killed) without ever
/// tripping an input-size limit. One megabyte is far above any real workflow
/// usage.
const MAX_FORMAT_OUTPUT_BYTES: usize = 1024 * 1024;

fn push_format_capped(
    output: &mut String,
    segment: &str,
    budget: &EvalBudget,
) -> Result<(), ExpressionError> {
    if segment.len() > MAX_FORMAT_OUTPUT_BYTES - output.len() {
        return Err(ExpressionError::FormatOutputTooLarge(
            MAX_FORMAT_OUTPUT_BYTES,
        ));
    }
    if segment.len() > budget.remaining().saturating_sub(output.len()) {
        return Err(ExpressionError::EvaluationTooLarge(
            MAX_EVALUATED_VALUE_BYTES,
        ));
    }
    output.push_str(segment);
    Ok(())
}

fn format_args(values: &[Value], budget: &EvalBudget) -> Result<String, ExpressionError> {
    // Bound serialization by the format capacity, not the evaluation budget:
    // a container argument must not allocate up to the full 8 MiB evaluation
    // budget before the 1 MiB format cap rejects it.
    let format_overflow = || ExpressionError::FormatOutputTooLarge(MAX_FORMAT_OUTPUT_BYTES);
    let format = string_value_bounded(
        values.first().unwrap_or(&Value::Null),
        MAX_FORMAT_OUTPUT_BYTES,
        format_overflow,
    )?;
    let bytes = format.as_bytes();
    let mut output = String::with_capacity(format.len().min(MAX_FORMAT_OUTPUT_BYTES));
    let mut segment_start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                push_format_capped(&mut output, &format[segment_start..index], budget)?;
                if bytes.get(index + 1) == Some(&b'{') {
                    push_format_capped(&mut output, "{", budget)?;
                    index += 2;
                    segment_start = index;
                    continue;
                }

                let digit_start = index + 1;
                let mut cursor = digit_start;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor += 1;
                }
                if cursor == digit_start {
                    return Err(ExpressionError::InvalidFormat(format.into_owned()));
                }
                let argument_index = format[digit_start..cursor]
                    .parse::<u8>()
                    .map_err(|_| ExpressionError::InvalidFormat(format.to_string()))?
                    as usize;
                match bytes.get(cursor) {
                    Some(b'}') => {}
                    Some(b':') => {
                        return Err(ExpressionError::InvalidFormat(format.into_owned()));
                    }
                    _ => return Err(ExpressionError::InvalidFormat(format.into_owned())),
                }
                let value = values
                    .get(argument_index + 1)
                    .ok_or_else(|| ExpressionError::InvalidFormat(format.to_string()))?;
                let rendered = string_value_bounded(
                    value,
                    MAX_FORMAT_OUTPUT_BYTES.saturating_sub(output.len()),
                    format_overflow,
                )?;
                push_format_capped(&mut output, rendered.as_ref(), budget)?;
                index = cursor + 1;
                segment_start = index;
            }
            b'}' => {
                push_format_capped(&mut output, &format[segment_start..index], budget)?;
                if bytes.get(index + 1) != Some(&b'}') {
                    return Err(ExpressionError::InvalidFormat(format.into_owned()));
                }
                push_format_capped(&mut output, "}", budget)?;
                index += 2;
                segment_start = index;
            }
            _ => index += 1,
        }
    }
    push_format_capped(&mut output, &format[segment_start..], budget)?;
    Ok(output)
}

fn join_args<'a>(values: &'a [Value], budget: &EvalBudget) -> Result<String, ExpressionError> {
    // Official runner (Join.cs): the separator defaults to "," and is only
    // honored when the provided value IsPrimitive — arrays and objects fall
    // back to ",". A non-array, non-primitive first argument (e.g. an
    // object) yields "". Live-verified against GitHub-hosted runners
    // 2026-09-19: join(['a','b'], ['x']) == 'a,b', join({'a':1}) == ''.
    let separator: Cow<'a, str> = match values.get(1) {
        Some(value) if is_primitive(value) => string_value_capped(value, budget.remaining())?,
        _ => Cow::Borrowed(","),
    };
    let mut output = String::new();
    match values.first() {
        Some(Value::Array(items)) => {
            for (index, value) in items.iter().enumerate() {
                if index > 0 {
                    push_evaluation_capped(&mut output, separator.as_ref(), budget)?;
                }
                let rendered =
                    string_value_capped(value, budget.remaining().saturating_sub(output.len()))?;
                push_evaluation_capped(&mut output, rendered.as_ref(), budget)?;
            }
        }
        Some(value) if is_primitive(value) => {
            let rendered = string_value_capped(value, budget.remaining())?;
            push_evaluation_capped(&mut output, rendered.as_ref(), budget)?;
        }
        // Objects and other non-primitives → empty string per official.
        _ => {}
    }
    Ok(output)
}

/// Matches official EvaluationResult.IsPrimitive: null, booleans, numbers,
/// and strings — not arrays or objects.
fn is_primitive(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn push_evaluation_capped(
    output: &mut String,
    segment: &str,
    budget: &EvalBudget,
) -> Result<(), ExpressionError> {
    if segment.len() > budget.remaining().saturating_sub(output.len()) {
        return Err(ExpressionError::EvaluationTooLarge(
            MAX_EVALUATED_VALUE_BYTES,
        ));
    }
    output.push_str(segment);
    Ok(())
}

/// Confirm an opened handle still resolves under the workspace root.
///
/// Canonicalization gates the entry *before* open; a swapped alias, target,
/// or parent directory in between would redirect a path-based open outside
/// the root. Resolving `/proc/self/fd` reports the kernel's path for the
/// handle itself, so verify-then-read shares one handle with no window.
/// A `(deleted)` suffix (replaced entry) fails the prefix check, which is
/// the safe direction. Non-Linux targets keep the pre-open gate only.
#[cfg(target_os = "linux")]
pub(crate) fn handle_under_root(file: &std::fs::File, workspace_root: &std::path::Path) -> bool {
    use std::os::unix::io::AsRawFd;
    let fd_path = format!("/proc/self/fd/{}", file.as_raw_fd());
    std::fs::read_link(fd_path)
        .map(|p| p.starts_with(workspace_root))
        .unwrap_or(false)
}

/// Implementation of `hashFiles(pattern, ...)` (F027).
///
/// Globs each argument pattern relative to `context.workspace_dir`, collects
/// all matching file paths (sorted), SHA-256 hashes each file, then
/// SHA-256 hashes the concatenated hex digests. Returns `""` on no match.
///
/// R1-7: every match is confined to the workspace. Absolute patterns and
/// parent traversal are rejected outright via platform-native components
/// (so Unix `/`/`..` and Windows `..\`, `C:\`, `\` all fail loudly rather
/// than becoming a file-content oracle like `hashFiles('/etc/passwd')`),
/// each candidate is canonicalized and required to stay under the canonical
/// workspace root (so escaping symlinks are skipped), opened handles are
/// re-verified against the root on Linux (no check-to-open window), and the
/// visited entries, hashed files, and total input bytes are capped. Files
/// are streamed through the hasher instead of being `fs::read` into memory
/// whole.
///
/// F055: Supports `--follow-symbolic-links` as an optional first argument.
/// When set, symbolic links are followed during file enumeration.
/// Matches official `HashFilesFunction.cs:44-51`.
fn hash_files(values: &[Value], context: &Context) -> Result<String, ExpressionError> {
    use sha2::{Digest, Sha256};
    use std::io::Read as _;

    /// Maximum files hashed per `hashFiles()` call. Bounds enumeration cost
    /// of adversarial patterns like `**/*`.
    const MAX_FILES: usize = 10_000;
    /// Maximum entries visited while expanding patterns per `hashFiles()`
    /// call. The file cap only bounds retained matches; a tree with
    /// hundreds of thousands of entries would otherwise burn traversal
    /// cost without ever tripping it.
    const MAX_VISITED: usize = 100_000;
    /// Maximum total bytes hashed per `hashFiles()` call (100 MiB).
    const MAX_TOTAL_BYTES: u64 = 100 * 1024 * 1024;

    let workspace = match &context.workspace_dir {
        Some(dir) => dir.as_str(),
        None => return Ok(String::new()),
    };

    // F055: Parse optional flags from the first argument.
    // Official runner only recognises `--follow-symbolic-links`.
    let mut follow_symlinks = false;
    let mut patterns: Vec<String> = Vec::new();
    let mut first = true;
    for val in values {
        let s = string_value(val);
        if s.is_empty() {
            continue;
        }
        if first {
            first = false;
            if s.starts_with("--") {
                if s.eq_ignore_ascii_case("--follow-symbolic-links") {
                    follow_symlinks = true;
                    continue;
                }
                return Err(ExpressionError::InvalidHashFilesOption(s));
            }
        }
        patterns.push(s);
    }

    // R1-7: dedup by matched path (overlapping patterns must not trip the
    // file cap with duplicates) and bound the retained set so adversarial
    // globs like `**/*` cannot grow it without limit. Canonicalization is
    // only the confinement check — hashing/sorting keep the matched path so
    // symlink aliases matching the same target hash per match like official.
    let mut seen_paths: std::collections::HashSet<std::path::PathBuf> =
        std::collections::HashSet::new();

    // R1-7: canonical workspace root for the confinement check below. If the
    // workspace itself cannot be canonicalized there is nothing safe to
    // match, so return "" like the no-workspace case.
    let workspace_root = match std::fs::canonicalize(workspace) {
        Ok(root) => root,
        Err(_) => return Ok(String::new()),
    };

    // Total glob entries pulled across all patterns; bounds traversal work
    // even when few entries are retained.
    let mut visited = 0usize;

    for pattern in &patterns {
        // R1-7: reject absolute patterns and `..` traversal outright.
        // Silently remapping `/etc/passwd` to a workspace-relative path, or
        // skipping escaping `../` matches, would hide attacker intent and
        // turn hashFiles() into a quiet file-content oracle. Fail loudly.
        // Platform-native components so Windows `..\`, `C:\`, `\` forms are
        // rejected on Windows (on Unix they are literal filenames).
        let disallowed = std::path::Path::new(pattern).components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        });
        if disallowed {
            return Err(ExpressionError::HashFilesDisallowedPattern(pattern.clone()));
        }
        let abs_pattern = format!("{workspace}/{pattern}");
        match glob::glob(&abs_pattern) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    visited += 1;
                    if visited > MAX_VISITED {
                        return Err(ExpressionError::HashFilesTraversalLimit(MAX_VISITED));
                    }
                    // Use symlink_metadata so symlinks are not followed
                    // implicitly here; following is decided below.
                    let metadata = match entry.symlink_metadata() {
                        Ok(m) => m,
                        Err(_) => continue,
                    };
                    if metadata.file_type().is_symlink() {
                        // F055: without --follow-symbolic-links, symlinks are
                        // never followed.
                        if !follow_symlinks {
                            continue;
                        }
                        // With follow mode the target is resolved below and
                        // must be a regular file under the workspace.
                    } else if !metadata.is_file() {
                        continue;
                    }
                    // Canonicalize (resolves symlinks and `..`) and require
                    // the result to stay under the workspace root; anything
                    // escaping the workspace is skipped. The canonical path
                    // is only the confinement check: the matched `entry` is
                    // what gets hashed, so symlink aliases to the same target
                    // stay distinct matches like official path-based hashing.
                    let canonical = match std::fs::canonicalize(&entry) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    if !canonical.starts_with(&workspace_root) {
                        continue;
                    }
                    // A symlink to a directory must not contribute an empty
                    // hash in follow mode.
                    if !canonical.is_file() {
                        continue;
                    }
                    if seen_paths.insert(entry) && seen_paths.len() > MAX_FILES {
                        return Err(ExpressionError::HashFilesTooManyFiles(MAX_FILES));
                    }
                }
            }
            Err(_) => continue,
        }
    }

    if seen_paths.is_empty() {
        return Ok(String::new());
    }

    let mut all_paths: Vec<std::path::PathBuf> = seen_paths.into_iter().collect();
    all_paths.sort();

    // Hash each file's bytes; concatenate raw 32-byte binary digests (NOT hex strings).
    // Official hashFiles.ts:29-35 feeds binary digest bytes directly into the outer SHA-256.
    // Concatenating hex-string representations produces a completely different key.
    //
    // R1-7: stream each file through the hasher instead of fs::read()-ing it
    // whole, enforcing the total byte budget so one huge match cannot OOM
    // the evaluator.
    let mut combined: Vec<u8> = Vec::new();
    let mut total_bytes: u64 = 0;
    for path in &all_paths {
        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        // The entry was canonicalized and confinement-checked before open;
        // re-verify the OPEN handle so an alias, target, or parent swapped
        // in between cannot redirect hashing outside the workspace. The
        // verify-then-use pair shares one handle, leaving no window.
        #[cfg(target_os = "linux")]
        if !handle_under_root(&file, &workspace_root) {
            continue;
        }
        let budget = MAX_TOTAL_BYTES.saturating_sub(total_bytes);
        // Read one byte past the remaining budget so an over-budget file is
        // reported instead of silently truncated.
        let mut limited = file.take(budget.saturating_add(1));
        let mut hasher = Sha256::new();
        let hashed = match std::io::copy(&mut limited, &mut hasher) {
            Ok(n) => n,
            Err(_) => continue,
        };
        if hashed > budget {
            return Err(ExpressionError::HashFilesTooLarge(MAX_TOTAL_BYTES));
        }
        total_bytes += hashed;
        combined.extend_from_slice(&hasher.finalize());
    }

    if combined.is_empty() {
        return Ok(String::new());
    }

    // Hash the concatenated binary digests
    let final_hash = Sha256::digest(&combined);
    Ok(format!("{final_hash:x}"))
}
