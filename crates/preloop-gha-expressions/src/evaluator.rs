use serde_json::Value;
use std::borrow::Cow;

use super::{
    ast::{BinaryOp, Expr},
    conditions::is_truthy,
    context::Context,
    ExpressionError,
};

pub(super) fn validate_function_calls(expr: &Expr) -> Result<(), ExpressionError> {
    match expr {
        Expr::Literal(_) | Expr::Path(_) => Ok(()),
        Expr::UnaryNot(inner) | Expr::MemberAccess { expr: inner, .. } => {
            validate_function_calls(inner)
        }
        Expr::Binary { left, right, .. } => {
            validate_function_calls(left)?;
            validate_function_calls(right)
        }
        Expr::Call { name, args } => {
            if !matches!(
                name.to_ascii_lowercase().as_str(),
                "always"
                    | "success"
                    | "failure"
                    | "cancelled"
                    | "contains"
                    | "startswith"
                    | "endswith"
                    | "format"
                    | "fromjson"
                    | "join"
                    | "hashfiles"
                    | "tojson"
                    | "case"
            ) {
                return Err(ExpressionError::UnknownFunction(name.clone()));
            }
            if name.eq_ignore_ascii_case("case") && (args.len() < 3 || args.len().is_multiple_of(2))
            {
                return Err(ExpressionError::EvenCaseParameters);
            }
            args.iter().try_for_each(validate_function_calls)
        }
    }
}

/// Collect all top-level context names referenced in an expression AST.
pub(super) fn collect_contexts_from_expr(expr: &Expr, out: &mut std::collections::HashSet<String>) {
    match expr {
        Expr::Path(path) => {
            if let Some(first) = path.first() {
                out.insert(first.to_ascii_lowercase());
            }
        }
        Expr::Literal(_) => {}
        Expr::UnaryNot(inner) | Expr::MemberAccess { expr: inner, .. } => {
            collect_contexts_from_expr(inner, out);
        }
        Expr::Binary { left, right, .. } => {
            collect_contexts_from_expr(left, out);
            collect_contexts_from_expr(right, out);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_contexts_from_expr(arg, out);
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
            24 + values.iter().map(|value| 16 + value_size(value)).sum::<usize>()
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
    let value = match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Path(path) => Ok(context.resolve(path)),
        Expr::UnaryNot(expr) => Ok(Value::Bool(!is_truthy(&eval(expr, context, budget)?))),
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Or => {
                let left = eval(left, context, budget)?;
                if is_truthy(&left) {
                    Ok(left)
                } else {
                    eval(right, context, budget)
                }
            }
            BinaryOp::And => {
                let left = eval(left, context, budget)?;
                if is_truthy(&left) {
                    eval(right, context, budget)
                } else {
                    Ok(left)
                }
            }
            BinaryOp::Eq => Ok(Value::Bool(abstract_equal(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
            ))),
            BinaryOp::Ne => Ok(Value::Bool(!abstract_equal(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
            ))),
            BinaryOp::Gt => Ok(Value::Bool(compare_values(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
                |ordering| ordering.is_gt(),
            ))),
            BinaryOp::Ge => Ok(Value::Bool(compare_values(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
                |ordering| ordering.is_ge(),
            ))),
            BinaryOp::Lt => Ok(Value::Bool(compare_values(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
                |ordering| ordering.is_lt(),
            ))),
            BinaryOp::Le => Ok(Value::Bool(compare_values(
                &eval(left, context, budget)?,
                &eval(right, context, budget)?,
                |ordering| ordering.is_le(),
            ))),
        },
        Expr::Call { name, args } => eval_call(name, args, context, budget),
        Expr::MemberAccess { expr, path } => {
            let base = eval(expr, context, budget)?;
            Ok(Context::resolve_value(base, path))
        }
    }?;
    budget.charge(&value)?;
    Ok(value)
}

fn abstract_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(left), Value::Bool(right)) => left == right,
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::String(left), Value::String(right)) => left.eq_ignore_ascii_case(right),
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
    if let (Some(left), Some(right)) = (numeric_value(left_value), numeric_value(right_value)) {
        if let Some(ordering) = left.partial_cmp(&right) {
            return predicate(ordering);
        }
        // A failed numeric conversion for mixed values is not a string
        // comparison; preserve the runner's false result for NaN.
        if !matches!(
            (left_value, right_value),
            (Value::String(_), Value::String(_))
        ) {
            return false;
        }
    }
    predicate(
        string_value(left_value)
            .to_ascii_lowercase()
            .cmp(&string_value(right_value).to_ascii_lowercase()),
    )
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
    // case() uses lazy evaluation — handle before eager collect
    if lower == "case" {
        if args.len() < 3 || args.len().is_multiple_of(2) {
            return Err(ExpressionError::EvenCaseParameters);
        }
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
        "startswith" => Ok(Value::Bool(
            string_arg(&values, 0)
                .to_ascii_lowercase()
                .starts_with(&string_arg(&values, 1).to_ascii_lowercase()),
        )),
        "endswith" => Ok(Value::Bool(
            string_arg(&values, 0)
                .to_ascii_lowercase()
                .ends_with(&string_arg(&values, 1).to_ascii_lowercase()),
        )),
        "format" => format_args(&values).map(Value::String),
        "fromjson" => Ok(values
            .first()
            .and_then(|value| serde_json::from_str(&string_value(value)).ok())
            .unwrap_or(Value::Null)),
        "join" => join_args(&values).map(Value::String),
        "hashfiles" => hash_files(&values, context).map(Value::String),
        "tojson" => Ok(Value::String(
            serde_json::to_string(values.first().unwrap_or(&Value::Null)).unwrap_or_default(),
        )),
        _ => Err(ExpressionError::UnknownFunction(name.to_owned())),
    }
}

fn contains(haystack: &Value, needle: &Value) -> bool {
    match haystack {
        Value::String(value) => value
            .to_ascii_lowercase()
            .contains(&string_value(needle).to_ascii_lowercase()),
        Value::Array(values) => values.iter().any(|value| abstract_equal(value, needle)),
        _ => false,
    }
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

fn string_value_cow(value: &Value) -> Cow<'_, str> {
    match value {
        Value::String(value) => Cow::Borrowed(value),
        _ => Cow::Owned(string_value(value)),
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

fn push_capped(output: &mut String, segment: &str) -> Result<(), ExpressionError> {
    if segment.len() > MAX_FORMAT_OUTPUT_BYTES - output.len() {
        return Err(ExpressionError::FormatOutputTooLarge(
            MAX_FORMAT_OUTPUT_BYTES,
        ));
    }
    output.push_str(segment);
    Ok(())
}

fn format_args(values: &[Value]) -> Result<String, ExpressionError> {
    let format = string_value_cow(values.first().unwrap_or(&Value::Null));
    let bytes = format.as_bytes();
    let mut output = String::with_capacity(format.len().min(MAX_FORMAT_OUTPUT_BYTES));
    let mut segment_start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                push_capped(&mut output, &format[segment_start..index])?;
                if bytes.get(index + 1) == Some(&b'{') {
                    push_capped(&mut output, "{")?;
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
                let rendered = string_value_cow(value);
                push_capped(&mut output, rendered.as_ref())?;
                index = cursor + 1;
                segment_start = index;
            }
            b'}' => {
                push_capped(&mut output, &format[segment_start..index])?;
                if bytes.get(index + 1) != Some(&b'}') {
                    return Err(ExpressionError::InvalidFormat(format.into_owned()));
                }
                push_capped(&mut output, "}")?;
                index += 2;
                segment_start = index;
            }
            _ => index += 1,
        }
    }
    push_capped(&mut output, &format[segment_start..])?;
    Ok(output)
}

fn join_args(values: &[Value]) -> Result<String, ExpressionError> {
    let separator = string_value_cow(values.get(1).unwrap_or(&Value::Null));
    let mut output = String::new();
    match values.first() {
        Some(Value::Array(values)) => {
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    push_evaluation_capped(&mut output, separator.as_ref())?;
                }
                let rendered = string_value_cow(value);
                push_evaluation_capped(&mut output, rendered.as_ref())?;
            }
        }
        Some(value) => {
            let rendered = string_value_cow(value);
            push_evaluation_capped(&mut output, rendered.as_ref())?;
        }
        None => {}
    }
    Ok(output)
}

fn push_evaluation_capped(
    output: &mut String,
    segment: &str,
) -> Result<(), ExpressionError> {
    if segment.len() > MAX_EVALUATED_VALUE_BYTES.saturating_sub(output.len()) {
        return Err(ExpressionError::EvaluationTooLarge(
            MAX_EVALUATED_VALUE_BYTES,
        ));
    }
    output.push_str(segment);
    Ok(())
}

/// Implementation of `hashFiles(pattern, ...)` (F027).
///
/// Globs each argument pattern relative to `context.workspace_dir`, collects
/// all matching file paths (sorted), SHA-256 hashes each file, then
/// SHA-256 hashes the concatenated hex digests. Returns `""` on no match.
///
/// F055: Supports `--follow-symbolic-links` as an optional first argument.
/// When set, symbolic links are followed during file enumeration.
/// Matches official `HashFilesFunction.cs:44-51`.
fn hash_files(values: &[Value], context: &Context) -> Result<String, ExpressionError> {
    use sha2::{Digest, Sha256};

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

    let mut all_paths: Vec<std::path::PathBuf> = Vec::new();
    for pattern in &patterns {
        // Make pattern relative to workspace
        let abs_pattern = if std::path::Path::new(pattern).is_absolute() {
            pattern.clone()
        } else {
            format!("{workspace}/{pattern}")
        };
        match glob::glob(&abs_pattern) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    // F055: When follow_symlinks is true, also include symlinks
                    // that point to regular files. `is_file()` already follows
                    // symlinks via `fs::metadata`, so both paths include targets
                    // of symlinks. The distinction matters for broken symlinks:
                    // `is_file()` returns false for dangling symlinks but
                    // `symlink_metadata().is_symlink()` would be true. We match
                    // the official behavior which uses the globber's follow mode
                    // (broken symlinks are silently skipped either way).
                    if entry.is_file() {
                        all_paths.push(entry);
                    } else if follow_symlinks
                        && entry
                            .symlink_metadata()
                            .map(|m| m.is_symlink())
                            .unwrap_or(false)
                    {
                        // Broken symlink with follow mode — skip (matches official)
                        continue;
                    }
                }
            }
            Err(_) => continue,
        }
    }

    if all_paths.is_empty() {
        return Ok(String::new());
    }

    all_paths.sort();

    // Hash each file's bytes; concatenate raw 32-byte binary digests (NOT hex strings).
    // Official hashFiles.ts:29-35 feeds binary digest bytes directly into the outer SHA-256.
    // Concatenating hex-string representations produces a completely different key.
    let mut combined: Vec<u8> = Vec::new();
    for path in &all_paths {
        match std::fs::read(path) {
            Ok(bytes) => {
                let digest = Sha256::digest(&bytes);
                combined.extend_from_slice(&digest);
            }
            Err(_) => continue,
        }
    }

    if combined.is_empty() {
        return Ok(String::new());
    }

    // Hash the concatenated binary digests
    let final_hash = Sha256::digest(&combined);
    Ok(format!("{final_hash:x}"))
}
