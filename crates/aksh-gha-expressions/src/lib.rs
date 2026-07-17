//! GitHub Actions expression parsing and evaluation.

use std::collections::BTreeMap;

use serde_json::Value;

/// Hierarchical expression context.
#[derive(Debug, Clone)]
pub struct Context {
    roots: BTreeMap<String, Value>,
    success: bool,
    failure: bool,
    cancelled: bool,
    /// Workspace directory for hashFiles() evaluation.
    workspace_dir: Option<String>,
}

impl Context {
    /// Create an empty context with default successful status.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set status-function values for this evaluation.
    pub fn with_status(mut self, success: bool, failure: bool, cancelled: bool) -> Self {
        self.success = success;
        self.failure = failure;
        self.cancelled = cancelled;
        self
    }

    /// Set workspace directory for hashFiles() evaluation (F027).
    pub fn with_workspace(mut self, dir: String) -> Self {
        self.workspace_dir = Some(dir);
        self
    }

    /// Insert a root object.
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.roots.insert(key.into(), value);
    }

    /// Resolve a dotted path such as `github.event_name`.
    /// Resolve a path such as `github.event_name`, with bracket access and wildcard support.
    ///
    /// - Numeric segment: array index (e.g. path built from `a[0]`)
    /// - `*` segment: collect all values from an object/array, then apply next segment
    pub fn resolve(&self, path: &[String]) -> Value {
        let Some((first, rest)) = path.split_first() else {
            return Value::Null;
        };
        let mut current = self.roots.get(first).cloned().unwrap_or(Value::Null);
        for segment in rest {
            if segment == "*" {
                // Object filter: collect values from object or array
                current = match current {
                    Value::Object(map) => Value::Array(map.into_values().collect()),
                    Value::Array(arr) => Value::Array(arr),
                    _ => Value::Null,
                };
                continue;
            }
            current = match current {
                Value::Object(map) => map.get(segment).cloned().unwrap_or(Value::Null),
                // After a wildcard, apply the next segment to each element
                Value::Array(arr) => Value::Array(
                    arr.into_iter()
                        .filter_map(|v| match v {
                            Value::Object(ref m) => m.get(segment).cloned(),
                            Value::Array(ref a) => segment
                                .parse::<usize>()
                                .ok()
                                .and_then(|i| a.get(i))
                                .cloned(),
                            _ => None,
                        })
                        .collect(),
                ),
                // Numeric index into array (from bracket access `a[0]`)
                _ => Value::Null,
            };
        }
        current
    }

    /// Resolve a path against an existing value (used for member access on expression results).
    pub fn resolve_value(mut current: Value, path: &[String]) -> Value {
        for segment in path {
            if segment == "*" {
                current = match current {
                    Value::Object(map) => Value::Array(map.into_values().collect()),
                    Value::Array(arr) => Value::Array(arr),
                    _ => Value::Null,
                };
                continue;
            }
            current = match current {
                Value::Object(map) => map.get(segment).cloned().unwrap_or(Value::Null),
                Value::Array(arr) => Value::Array(
                    arr.into_iter()
                        .filter_map(|v| match v {
                            Value::Object(ref m) => m.get(segment).cloned(),
                            Value::Array(ref a) => segment
                                .parse::<usize>()
                                .ok()
                                .and_then(|i| a.get(i))
                                .cloned(),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => Value::Null,
            };
        }
        current
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            roots: BTreeMap::new(),
            success: true,
            failure: false,
            cancelled: false,
            workspace_dir: None,
        }
    }
}

/// Expression evaluation error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExpressionError {
    /// Unexpected end of input.
    #[error("unexpected end of expression")]
    Eof,
    /// Unexpected token.
    #[error("unexpected token `{0}`")]
    Unexpected(String),
    /// Unknown function.
    #[error("unknown function `{0}`")]
    UnknownFunction(String),
    /// Invalid `format()` template or argument reference.
    #[error("invalid format string: {0}")]
    InvalidFormat(String),
    /// Invalid leading option passed to `hashFiles()`.
    #[error("invalid hashFiles option `{0}`")]
    InvalidHashFilesOption(String),
}

/// Parse an expression without evaluating it.
pub fn validate_expression(input: &str) -> Result<(), ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let tokens = Lexer::new(trimmed).lex()?;
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;
    parser.expect_end()?;
    validate_function_calls(&expr)
}

/// Parse and evaluate a GitHub Actions expression.
pub fn eval_expression(input: &str, context: &Context) -> Result<Value, ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let tokens = Lexer::new(trimmed).lex()?;
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;
    parser.expect_end()?;
    eval(&expr, context)
}

/// Evaluate an expression as GitHub Actions truthiness.
pub fn eval_bool(input: &str, context: &Context) -> Result<bool, ExpressionError> {
    eval_expression(input, context).map(|value| is_truthy(&value))
}

/// Remove `${{` and `}}` delimiters if present.
pub fn trim_expression_markers(input: &str) -> &str {
    let value = input.trim();
    if let Some(inner) = value.strip_prefix("${{").and_then(|s| s.strip_suffix("}}")) {
        inner.trim()
    } else {
        value
    }
}
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

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    Literal(Value),
    Path(Vec<String>),
    UnaryNot(Box<Expr>),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    /// Member access on an expression result, e.g. `fromJSON('...').*.name`
    MemberAccess {
        expr: Box<Expr>,
        path: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOp {
    Or,
    And,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

fn validate_function_calls(expr: &Expr) -> Result<(), ExpressionError> {
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
            ) {
                return Err(ExpressionError::UnknownFunction(name.clone()));
            }
            args.iter().try_for_each(validate_function_calls)
        }
    }
}

fn eval(expr: &Expr, context: &Context) -> Result<Value, ExpressionError> {
    match expr {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::Path(path) => Ok(context.resolve(path)),
        Expr::UnaryNot(expr) => Ok(Value::Bool(!is_truthy(&eval(expr, context)?))),
        Expr::Binary { op, left, right } => match op {
            BinaryOp::Or => {
                let left = eval(left, context)?;
                if is_truthy(&left) {
                    Ok(left)
                } else {
                    eval(right, context)
                }
            }
            BinaryOp::And => {
                let left = eval(left, context)?;
                if is_truthy(&left) {
                    eval(right, context)
                } else {
                    Ok(left)
                }
            }
            BinaryOp::Eq => Ok(Value::Bool(abstract_equal(
                &eval(left, context)?,
                &eval(right, context)?,
            ))),
            BinaryOp::Ne => Ok(Value::Bool(!abstract_equal(
                &eval(left, context)?,
                &eval(right, context)?,
            ))),
            BinaryOp::Gt => Ok(Value::Bool(compare_values(
                &eval(left, context)?,
                &eval(right, context)?,
                |ordering| ordering.is_gt(),
            ))),
            BinaryOp::Ge => Ok(Value::Bool(compare_values(
                &eval(left, context)?,
                &eval(right, context)?,
                |ordering| ordering.is_ge(),
            ))),
            BinaryOp::Lt => Ok(Value::Bool(compare_values(
                &eval(left, context)?,
                &eval(right, context)?,
                |ordering| ordering.is_lt(),
            ))),
            BinaryOp::Le => Ok(Value::Bool(compare_values(
                &eval(left, context)?,
                &eval(right, context)?,
                |ordering| ordering.is_le(),
            ))),
        },
        Expr::Call { name, args } => eval_call(name, args, context),
        Expr::MemberAccess { expr, path } => {
            let base = eval(expr, context)?;
            Ok(Context::resolve_value(base, path))
        }
    }
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
    left: &Value,
    right: &Value,
    predicate: impl FnOnce(std::cmp::Ordering) -> bool,
) -> bool {
    if let (Some(left), Some(right)) = (numeric_value(left), numeric_value(right)) {
        return left.partial_cmp(&right).is_some_and(predicate);
    }
    predicate(string_value(left).cmp(&string_value(right)))
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

fn eval_call(name: &str, args: &[Expr], context: &Context) -> Result<Value, ExpressionError> {
    let lower = name.to_ascii_lowercase();
    let values = args
        .iter()
        .map(|arg| eval(arg, context))
        .collect::<Result<Vec<_>, _>>()?;

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
        "join" => Ok(Value::String(join_args(&values))),
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

fn format_args(values: &[Value]) -> Result<String, ExpressionError> {
    let format = string_arg(values, 0);
    let bytes = format.as_bytes();
    let mut output = String::with_capacity(format.len());
    let mut segment_start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                output.push_str(&format[segment_start..index]);
                if bytes.get(index + 1) == Some(&b'{') {
                    output.push('{');
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
                    return Err(ExpressionError::InvalidFormat(format));
                }
                let argument_index = format[digit_start..cursor]
                    .parse::<u8>()
                    .map_err(|_| ExpressionError::InvalidFormat(format.clone()))?
                    as usize;
                match bytes.get(cursor) {
                    Some(b'}') => {}
                    Some(b':') => {
                        return Err(ExpressionError::InvalidFormat(format));
                    }
                    _ => return Err(ExpressionError::InvalidFormat(format)),
                }
                let value = values
                    .get(argument_index + 1)
                    .ok_or_else(|| ExpressionError::InvalidFormat(format.clone()))?;
                output.push_str(&string_value(value));
                index = cursor + 1;
                segment_start = index;
            }
            b'}' => {
                output.push_str(&format[segment_start..index]);
                if bytes.get(index + 1) != Some(&b'}') {
                    return Err(ExpressionError::InvalidFormat(format));
                }
                output.push('}');
                index += 2;
                segment_start = index;
            }
            _ => index += 1,
        }
    }
    output.push_str(&format[segment_start..]);
    Ok(output)
}

fn join_args(values: &[Value]) -> String {
    let separator = string_arg(values, 1);
    match values.first() {
        Some(Value::Array(values)) => values
            .iter()
            .map(string_value)
            .collect::<Vec<_>>()
            .join(&separator),
        Some(value) => string_value(value),
        None => String::new(),
    }
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

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    String(String),
    Number(String),
    Bool(bool),
    Null,
    Dot,
    Comma,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Star,
    Bang,
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    End,
}

struct Lexer<'a> {
    input: &'a str,
    chars: std::str::Chars<'a>,
    offset: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars(),
            offset: 0,
        }
    }

    fn lex(mut self) -> Result<Vec<Token>, ExpressionError> {
        let mut tokens = Vec::new();
        while let Some(ch) = self.peek() {
            match ch {
                c if c.is_whitespace() => {
                    self.bump();
                }
                '\'' => tokens.push(Token::String(self.string()?)),
                '0'..='9' => tokens.push(Token::Number(
                    self.take_while(|c| c.is_ascii_digit() || c == '.'),
                )),
                'a'..='z' | 'A'..='Z' | '_' => {
                    let ident =
                        self.take_while(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
                    match ident.to_ascii_lowercase().as_str() {
                        "true" => tokens.push(Token::Bool(true)),
                        "false" => tokens.push(Token::Bool(false)),
                        "null" => tokens.push(Token::Null),
                        _ => tokens.push(Token::Ident(ident)),
                    }
                }
                '.' => {
                    self.bump();
                    tokens.push(Token::Dot);
                }
                ',' => {
                    self.bump();
                    tokens.push(Token::Comma);
                }
                '(' => {
                    self.bump();
                    tokens.push(Token::LParen);
                }
                ')' => {
                    self.bump();
                    tokens.push(Token::RParen);
                }
                '!' => {
                    self.bump();
                    if self.consume('=') {
                        tokens.push(Token::Ne);
                    } else {
                        tokens.push(Token::Bang);
                    }
                }
                '=' => {
                    self.bump();
                    if self.consume('=') {
                        tokens.push(Token::Eq);
                    } else {
                        return Err(ExpressionError::Unexpected("=".to_owned()));
                    }
                }
                '>' => {
                    self.bump();
                    if self.consume('=') {
                        tokens.push(Token::Ge);
                    } else {
                        tokens.push(Token::Gt);
                    }
                }
                '<' => {
                    self.bump();
                    if self.consume('=') {
                        tokens.push(Token::Le);
                    } else {
                        tokens.push(Token::Lt);
                    }
                }
                '&' => {
                    self.bump();
                    if self.consume('&') {
                        tokens.push(Token::And);
                    } else {
                        return Err(ExpressionError::Unexpected("&".to_owned()));
                    }
                }
                '|' => {
                    self.bump();
                    if self.consume('|') {
                        tokens.push(Token::Or);
                    } else {
                        return Err(ExpressionError::Unexpected("|".to_owned()));
                    }
                }
                '*' => {
                    self.bump();
                    tokens.push(Token::Star);
                }
                '[' => {
                    self.bump();
                    tokens.push(Token::LBracket);
                }
                ']' => {
                    self.bump();
                    tokens.push(Token::RBracket);
                }
                other => return Err(ExpressionError::Unexpected(other.to_string())),
            }
        }
        tokens.push(Token::End);
        Ok(tokens)
    }

    fn string(&mut self) -> Result<String, ExpressionError> {
        self.bump();
        let mut out = String::new();
        while let Some(ch) = self.bump() {
            match ch {
                '\'' if self.peek() == Some('\'') => {
                    self.bump();
                    out.push('\'');
                }
                '\'' => return Ok(out),
                other => out.push(other),
            }
        }
        Err(ExpressionError::Eof)
    }

    fn take_while(&mut self, mut predicate: impl FnMut(char) -> bool) -> String {
        let start = self.offset;
        while self.peek().is_some_and(&mut predicate) {
            self.bump();
        }
        self.input[start..self.offset].to_owned()
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.peek() == Some(expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.clone().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.chars.next()?;
        self.offset += ch.len_utf8();
        Some(ch)
    }
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    fn parse_expr(&mut self) -> Result<Expr, ExpressionError> {
        self.parse_or()
    }

    fn expect_end(&self) -> Result<(), ExpressionError> {
        match self.current() {
            Token::End => Ok(()),
            token => Err(ExpressionError::Unexpected(format!("{token:?}"))),
        }
    }

    fn parse_or(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_and()?;
        while matches!(self.current(), Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            expr = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_eq()?;
        while matches!(self.current(), Token::And) {
            self.advance();
            let right = self.parse_eq()?;
            expr = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_eq(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_unary()?;
        loop {
            let op = match self.current() {
                Token::Eq => BinaryOp::Eq,
                Token::Ne => BinaryOp::Ne,
                Token::Gt => BinaryOp::Gt,
                Token::Ge => BinaryOp::Ge,
                Token::Lt => BinaryOp::Lt,
                Token::Le => BinaryOp::Le,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            expr = Expr::Binary {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ExpressionError> {
        if matches!(self.current(), Token::Bang) {
            self.advance();
            Ok(Expr::UnaryNot(Box::new(self.parse_unary()?)))
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ExpressionError> {
        match self.current().clone() {
            Token::String(value) => {
                self.advance();
                Ok(Expr::Literal(Value::String(value)))
            }
            Token::Number(value) => {
                self.advance();
                let number = value
                    .parse::<serde_json::Number>()
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                Ok(Expr::Literal(number))
            }
            Token::Bool(value) => {
                self.advance();
                Ok(Expr::Literal(Value::Bool(value)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Value::Null))
            }
            Token::Ident(name) => self.parse_ident_or_call(name),
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Token::End => Err(ExpressionError::Eof),
            token => Err(ExpressionError::Unexpected(format!("{token:?}"))),
        }
    }

    fn parse_ident_or_call(&mut self, name: String) -> Result<Expr, ExpressionError> {
        self.advance();
        if matches!(self.current(), Token::LParen) {
            self.advance();
            let mut args = Vec::new();
            if !matches!(self.current(), Token::RParen) {
                loop {
                    args.push(self.parse_expr()?);
                    if matches!(self.current(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
            self.expect(Token::RParen)?;
            let call = Expr::Call { name, args };
            // Check for trailing member access: fromJSON('...').*.name or fn()[0]
            let suffix = self.parse_member_suffix();
            if suffix.is_empty() {
                Ok(call)
            } else {
                Ok(Expr::MemberAccess {
                    expr: Box::new(call),
                    path: suffix,
                })
            }
        } else {
            let mut path = vec![name];
            loop {
                match self.current() {
                    // Dot access: a.b or a.*
                    Token::Dot => {
                        self.advance();
                        match self.current().clone() {
                            Token::Ident(segment) => {
                                self.advance();
                                path.push(segment);
                            }
                            Token::Star => {
                                self.advance();
                                path.push("*".to_string());
                            }
                            other => {
                                return Err(ExpressionError::Unexpected(format!("{other:?}")));
                            }
                        }
                    }
                    // Bracket access: a['key'] or a[0]
                    Token::LBracket => {
                        self.advance();
                        let segment = match self.current().clone() {
                            Token::String(s) => {
                                self.advance();
                                s
                            }
                            Token::Number(n) => {
                                self.advance();
                                n
                            }
                            Token::Ident(s) => {
                                self.advance();
                                s
                            }
                            other => {
                                return Err(ExpressionError::Unexpected(format!("{other:?}")));
                            }
                        };
                        self.expect(Token::RBracket)?;
                        path.push(segment);
                    }
                    _ => break,
                }
            }
            Ok(Expr::Path(path))
        }
    }

    /// Parse trailing `.ident`, `.*`, or `['key']` segments after an expression.
    fn parse_member_suffix(&mut self) -> Vec<String> {
        let mut path = Vec::new();
        loop {
            match self.current() {
                Token::Dot => {
                    self.advance();
                    match self.current().clone() {
                        Token::Ident(segment) => {
                            self.advance();
                            path.push(segment);
                        }
                        Token::Star => {
                            self.advance();
                            path.push("*".to_string());
                        }
                        _ => break,
                    }
                }
                Token::LBracket => {
                    self.advance();
                    let segment = match self.current().clone() {
                        Token::String(s) => {
                            self.advance();
                            s
                        }
                        Token::Number(n) => {
                            self.advance();
                            n
                        }
                        Token::Ident(s) => {
                            self.advance();
                            s
                        }
                        _ => break,
                    };
                    if self.expect(Token::RBracket).is_err() {
                        break;
                    }
                    path.push(segment);
                }
                _ => break,
            }
        }
        path
    }

    fn expect(&mut self, expected: Token) -> Result<(), ExpressionError> {
        if std::mem::discriminant(self.current()) == std::mem::discriminant(&expected) {
            self.advance();
            Ok(())
        } else {
            Err(ExpressionError::Unexpected(format!("{:?}", self.current())))
        }
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.index).unwrap_or(&Token::End)
    }

    fn advance(&mut self) {
        self.index += 1;
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
