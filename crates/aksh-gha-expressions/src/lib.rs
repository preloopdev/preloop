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

    /// Get the raw roots map.
    pub fn roots(&self) -> &BTreeMap<String, Value> {
        &self.roots
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

/// GitHub Actions truthiness approximation.
pub fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::Object(values) => !values.is_empty(),
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
            BinaryOp::Eq => Ok(Value::Bool(values_equal(
                &eval(left, context)?,
                &eval(right, context)?,
            ))),
            BinaryOp::Ne => Ok(Value::Bool(!values_equal(
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

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left.eq_ignore_ascii_case(right),
        _ => left == right,
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
        Value::String(value) => value.parse().ok(),
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) | Value::Null => Some(0.0),
        _ => None,
    }
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
            string_arg(&values, 0).starts_with(&string_arg(&values, 1)),
        )),
        "endswith" => Ok(Value::Bool(
            string_arg(&values, 0).ends_with(&string_arg(&values, 1)),
        )),
        "format" => Ok(Value::String(format_args(&values))),
        "fromjson" => Ok(values
            .first()
            .and_then(|value| serde_json::from_str(&string_value(value)).ok())
            .unwrap_or(Value::Null)),
        "join" => Ok(Value::String(join_args(&values))),
        "hashfiles" => Ok(Value::String(hash_files(&values, context))),
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
        Value::Array(values) => values.iter().any(|value| values_equal(value, needle)),
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
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn format_args(values: &[Value]) -> String {
    let mut out = string_arg(values, 0);
    for (index, value) in values.iter().enumerate().skip(1) {
        out = out.replace(&format!("{{{}}}", index - 1), &string_value(value));
    }
    // Handle escaped braces: {{ → { and }} → }
    // (matches C#'s String.Format / GitHub Actions format() behavior)
    out = out.replace("{{", "{").replace("}}", "}");
    out
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
fn hash_files(values: &[Value], context: &Context) -> String {
    use sha2::{Digest, Sha256};

    let workspace = match &context.workspace_dir {
        Some(dir) => dir.as_str(),
        None => return String::new(),
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
                // Official throws on unknown flags; we silently skip to avoid
                // breaking expressions, but the pattern won't match anything
                // useful either way.
                continue;
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
        return String::new();
    }

    all_paths.sort();

    // Hash each file, collect hex digests
    let mut combined = String::new();
    for path in &all_paths {
        match std::fs::read(path) {
            Ok(bytes) => {
                let digest = Sha256::digest(&bytes);
                combined.push_str(&format!("{digest:x}"));
            }
            Err(_) => continue,
        }
    }

    if combined.is_empty() {
        return String::new();
    }

    // Hash the concatenated hex digests
    let final_hash = Sha256::digest(combined.as_bytes());
    format!("{final_hash:x}")
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
mod tests {
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
}
