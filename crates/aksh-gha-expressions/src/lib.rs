//! GitHub Actions expression parsing and evaluation.

use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

mod ast;
mod conditions;
mod context;
mod evaluator;
mod expr_parser;
mod lexer;

pub use conditions::{contains_status_check_function, effective_condition, is_truthy};
pub use context::Context;

use evaluator::{collect_contexts_from_expr, eval, validate_function_calls};
use expr_parser::Parser;
use lexer::Lexer;

const PARSED_EXPRESSION_CACHE_CAPACITY: usize = 1024;

struct ParsedExpressionCache {
    entries: HashMap<String, Arc<ast::Expr>>,
    insertion_order: VecDeque<String>,
}

impl ParsedExpressionCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
        }
    }
}

static PARSED_EXPRESSION_CACHE: OnceLock<Mutex<ParsedExpressionCache>> = OnceLock::new();

fn parse_cached(input: &str) -> Result<Arc<ast::Expr>, ExpressionError> {
    let cache = PARSED_EXPRESSION_CACHE.get_or_init(|| Mutex::new(ParsedExpressionCache::new()));
    if let Some(expr) = cache
        .lock()
        .expect("expression cache lock is not poisoned")
        .entries
        .get(input)
        .cloned()
    {
        return Ok(expr);
    }

    let tokens = Lexer::new(input).lex()?;
    let mut parser = Parser::new(tokens);
    let expr = Arc::new(parser.parse_expr()?);
    parser.expect_end()?;

    let mut cache = cache.lock().expect("expression cache lock is not poisoned");
    if let Some(existing) = cache.entries.get(input).cloned() {
        return Ok(existing);
    }
    cache.entries.insert(input.to_owned(), Arc::clone(&expr));
    cache.insertion_order.push_back(input.to_owned());
    if cache.insertion_order.len() > PARSED_EXPRESSION_CACHE_CAPACITY {
        if let Some(oldest) = cache.insertion_order.pop_front() {
            cache.entries.remove(&oldest);
        }
    }
    Ok(expr)
}

/// Errors encountered when parsing or evaluating `${{ }}` expressions.
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
    /// `case()` must have predicate/result pairs followed by a default.
    #[error("case() requires an odd number of arguments (at least 3)")]
    EvenCaseParameters,
    /// `case()` predicates must evaluate to booleans.
    #[error("case() predicate must evaluate to a boolean value")]
    NonBooleanCasePredicate,
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
    let expr = parse_cached(trimmed)?;
    validate_function_calls(&expr)
}

/// Collect top-level context names (e.g. "github", "matrix") from an expression string.
pub fn collect_contexts(input: &str) -> Result<std::collections::HashSet<String>, ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let expr = parse_cached(trimmed)?;
    let mut contexts = std::collections::HashSet::new();
    collect_contexts_from_expr(&expr, &mut contexts);
    Ok(contexts)
}

/// Parse and evaluate a GitHub Actions expression.
pub fn eval_expression(input: &str, context: &Context) -> Result<Value, ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let expr = parse_cached(trimmed)?;
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
