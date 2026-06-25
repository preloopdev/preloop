//! GitHub Actions expression parsing and evaluation.

use std::collections::BTreeMap;

use serde_json::Value;

/// Hierarchical expression context.
#[derive(Debug, Clone, Default)]
pub struct Context {
    roots: BTreeMap<String, Value>,
}

impl Context {
    /// Insert a root object.
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.roots.insert(key.into(), value);
    }

    /// Resolve a dotted path such as `github.event_name`.
    pub fn resolve(&self, path: &[String]) -> Value {
        let Some((first, rest)) = path.split_first() else {
            return Value::Null;
        };
        let mut current = self.roots.get(first).cloned().unwrap_or(Value::Null);
        for segment in rest {
            current = match current {
                Value::Object(map) => map.get(segment).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            };
        }
        current
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryOp {
    Or,
    And,
    Eq,
    Ne,
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
        },
        Expr::Call { name, args } => eval_call(name, args, context),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(left), Value::String(right)) => left.eq_ignore_ascii_case(right),
        _ => left == right,
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
        "success" => Ok(Value::Bool(true)),
        "failure" => Ok(Value::Bool(false)),
        "cancelled" => Ok(Value::Bool(false)),
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
    out
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
    Bang,
    And,
    Or,
    Eq,
    Ne,
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
            Ok(Expr::Call { name, args })
        } else {
            let mut path = vec![name];
            while matches!(self.current(), Token::Dot) {
                self.advance();
                let Token::Ident(segment) = self.current().clone() else {
                    return Err(ExpressionError::Unexpected(format!("{:?}", self.current())));
                };
                self.advance();
                path.push(segment);
            }
            Ok(Expr::Path(path))
        }
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
}
