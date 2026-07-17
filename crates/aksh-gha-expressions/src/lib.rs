//! GitHub Actions expression parsing and evaluation.

use serde_json::Value;

mod ast;
mod conditions;
mod context;
mod evaluator;
mod expr_parser;
mod lexer;

pub use conditions::{contains_status_check_function, effective_condition, is_truthy};
pub use context::Context;

use evaluator::{eval, validate_function_calls};
use expr_parser::Parser;
use lexer::Lexer;

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

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
