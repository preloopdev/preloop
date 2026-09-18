//! GitHub Actions expression parsing and evaluation.

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock};

mod ast;
mod conditions;
mod context;
mod evaluator;
mod expr_parser;
mod lexer;

pub use conditions::{contains_status_check_function, effective_condition, is_truthy};
pub use context::Context;

use evaluator::{
    collect_expression_references_from_expr, eval, validate_function_calls, EvalBudget,
};
use expr_parser::Parser;
use lexer::Lexer;

const PARSED_EXPRESSION_CACHE_CAPACITY: usize = 1024;
const MAX_CACHEABLE_EXPRESSION_BYTES: usize = 4096;

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
    if input.len() > MAX_CACHEABLE_EXPRESSION_BYTES {
        let tokens = Lexer::new(input).lex()?;
        let mut parser = Parser::new(tokens);
        let expr = Arc::new(parser.parse_expr()?);
        parser.expect_end()?;
        return Ok(expr);
    }

    let cache = PARSED_EXPRESSION_CACHE.get_or_init(|| Mutex::new(ParsedExpressionCache::new()));
    if let Some(expr) = cache.lock().entries.get(input).cloned() {
        return Ok(expr);
    }

    let tokens = Lexer::new(input).lex()?;
    let mut parser = Parser::new(tokens);
    let expr = Arc::new(parser.parse_expr()?);
    parser.expect_end()?;

    let mut cache = cache.lock();
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
    /// A known function was called with an unsupported number of arguments.
    #[error("function `{name}` expects {min}..={max} arguments, got {actual}")]
    InvalidFunctionArity {
        /// Function spelling from the expression.
        name: String,
        /// Minimum accepted arguments.
        min: usize,
        /// Maximum accepted arguments.
        max: usize,
        /// Arguments present in the call.
        actual: usize,
    },
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
    /// `hashFiles()` matched more files than the per-call limit.
    #[error("hashFiles() matched more than the maximum of {0} files")]
    HashFilesTooManyFiles(usize),
    /// `hashFiles()` visited more entries than the per-call traversal budget.
    #[error("hashFiles() visited more than the maximum of {0} entries while expanding patterns")]
    HashFilesTraversalLimit(usize),
    /// `hashFiles()` input bytes exceeded the per-call budget.
    #[error("hashFiles() input exceeds the maximum of {0} bytes")]
    HashFilesTooLarge(u64),
    /// `hashFiles()` pattern is absolute or contains parent traversal.
    #[error("hashFiles() pattern `{0}` is not allowed: patterns must be workspace-relative without absolute or parent components")]
    HashFilesDisallowedPattern(String),
    /// `format()` output exceeded the maximum length.
    #[error("format() output exceeds the maximum of {0} bytes")]
    FormatOutputTooLarge(usize),
    /// Temporary values created while evaluating an expression exceeded the
    /// memory budget.
    #[error("expression evaluation exceeds the temporary value budget of {0} bytes")]
    EvaluationTooLarge(usize),
    /// Expression nesting exceeded the parser's depth ceiling.
    #[error("expression nesting exceeds the maximum depth of {0}")]
    TooDeep(usize),
}

/// Parse an expression without evaluating it.
pub fn validate_expression(input: &str) -> Result<(), ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let expr = parse_cached(trimmed)?;
    validate_function_calls(&expr)
}

/// One context-sensitive function call found in an expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextFunctionCall {
    /// Lowercase function name.
    pub name: String,
    /// Number of supplied arguments.
    pub argument_count: usize,
}

/// Contexts and context-sensitive function calls referenced by an expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpressionReferences {
    /// Lowercase top-level context and context-sensitive function names.
    pub contexts: std::collections::HashSet<String>,
    /// Context-sensitive calls with their actual argument counts.
    pub functions: Vec<ContextFunctionCall>,
}

/// Collect top-level data contexts and context-sensitive function calls.
pub fn collect_expression_references(input: &str) -> Result<ExpressionReferences, ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let expr = parse_cached(trimmed)?;
    let mut references = ExpressionReferences {
        contexts: std::collections::HashSet::new(),
        functions: Vec::new(),
    };
    collect_expression_references_from_expr(&expr, &mut references);
    Ok(references)
}

/// Collect top-level data contexts and context-sensitive function names.
pub fn collect_contexts(input: &str) -> Result<std::collections::HashSet<String>, ExpressionError> {
    collect_expression_references(input).map(|references| references.contexts)
}

/// Parse and evaluate a GitHub Actions expression.
pub fn eval_expression(input: &str, context: &Context) -> Result<Value, ExpressionError> {
    let trimmed = trim_expression_markers(input);
    let expr = parse_cached(trimmed)?;
    let mut budget = EvalBudget::default();
    eval(&expr, context, &mut budget)
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
