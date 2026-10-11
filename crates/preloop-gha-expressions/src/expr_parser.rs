use serde_json::Value;

use super::{
    ExpressionError,
    ast::{BinaryOp, Expr},
    lexer::Token,
};

/// Maximum expression nesting depth.
///
/// The parser is recursive descent and the evaluator recurses over the same
/// AST, so unbounded nesting (e.g. megabytes of `(((…` or `!!!…`) overflows
/// the thread stack, which aborts the process instead of unwinding. The
/// ceiling must hold on a default 2 MiB thread in a debug build, where one
/// nesting level costs ~10 KiB on aarch64 Linux: 256 needed 2-3 MiB and
/// aborted there before the guard fired. The official runner caps expression
/// trees at depth 50, so nothing a real workflow can express comes near this.
///
/// Two limits enforce it. Recursion (parentheses, call arguments, computed
/// keys, `!`) is capped while parsing. The depth of the tree is tracked as
/// each node is built, a leaf being 1 and every node one deeper than its
/// deepest child: calls, `!`, index folds, operators, and member access on a
/// computed base all add a level, while literal segments appended to an open
/// path stay flat. A node past the ceiling is refused before it is built, so
/// no parsed tree exceeds it and the AST walkers and the evaluator's own
/// recursion guard stay clear of the stack.
pub(crate) const MAX_EXPRESSION_DEPTH: usize = 128;

pub(crate) struct Parser {
    tokens: Vec<Token>,
    index: usize,
    /// Recursion levels on the parse stack; see `enter`.
    depth: usize,
}

/// A parsed subtree with the depth of the tree it roots. Folds check the
/// ceiling from these fields alone, so no tree is walked to measure it.
struct ParsedExpr {
    expr: Expr,
    depth: usize,
}

impl ParsedExpr {
    fn leaf(expr: Expr) -> Self {
        Self { expr, depth: 1 }
    }

    fn binary(op: BinaryOp, left: Self, right: Self) -> Result<Self, ExpressionError> {
        let depth = node_depth(left.depth.max(right.depth))?;
        Ok(Self {
            expr: Expr::Binary {
                op,
                left: Box::new(left.expr),
                right: Box::new(right.expr),
            },
            depth,
        })
    }

    fn index(base: Self, key: Self) -> Result<Self, ExpressionError> {
        let depth = node_depth(base.depth.max(key.depth))?;
        Ok(Self {
            expr: Expr::Index {
                base: Box::new(base.expr),
                key: Box::new(key.expr),
            },
            depth,
        })
    }
}

/// Depth of a node built over children no deeper than `deepest_child`.
/// Callers construct the node only after this passes.
fn node_depth(deepest_child: usize) -> Result<usize, ExpressionError> {
    let depth = deepest_child + 1;
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(ExpressionError::TooDeep(MAX_EXPRESSION_DEPTH));
    }
    Ok(depth)
}

impl Parser {
    pub(crate) fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            index: 0,
            depth: 0,
        }
    }

    /// Enter one nesting level, refusing input past the depth ceiling. An
    /// over-deep error aborts the whole parse, so the counter is not unwound.
    fn enter(&mut self) -> Result<(), ExpressionError> {
        self.depth += 1;
        if self.depth > MAX_EXPRESSION_DEPTH {
            return Err(ExpressionError::TooDeep(MAX_EXPRESSION_DEPTH));
        }
        Ok(())
    }

    pub(crate) fn parse_expr(&mut self) -> Result<Expr, ExpressionError> {
        self.parse_tracked_expr().map(|parsed| parsed.expr)
    }

    /// Parse one expression together with its tree depth. Parenthesized
    /// groups, call arguments, and computed keys all re-enter here, so the
    /// recursion level is counted here.
    fn parse_tracked_expr(&mut self) -> Result<ParsedExpr, ExpressionError> {
        self.enter()?;
        let parsed = self.parse_or();
        self.depth -= 1;
        parsed
    }

    pub(crate) fn expect_end(&self) -> Result<(), ExpressionError> {
        match self.current() {
            Token::End => Ok(()),
            token => Err(ExpressionError::Unexpected(format!("{token:?}"))),
        }
    }

    fn parse_or(&mut self) -> Result<ParsedExpr, ExpressionError> {
        let mut expr = self.parse_and()?;
        while matches!(self.current(), Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            expr = ParsedExpr::binary(BinaryOp::Or, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<ParsedExpr, ExpressionError> {
        let mut expr = self.parse_eq()?;
        while matches!(self.current(), Token::And) {
            self.advance();
            let right = self.parse_eq()?;
            expr = ParsedExpr::binary(BinaryOp::And, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_eq(&mut self) -> Result<ParsedExpr, ExpressionError> {
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
            expr = ParsedExpr::binary(op, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<ParsedExpr, ExpressionError> {
        if matches!(self.current(), Token::Bang) {
            self.advance();
            self.enter()?;
            let inner = self.parse_unary();
            self.depth -= 1;
            let inner = inner?;
            let depth = node_depth(inner.depth)?;
            Ok(ParsedExpr {
                expr: Expr::UnaryNot(Box::new(inner.expr)),
                depth,
            })
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<ParsedExpr, ExpressionError> {
        match self.current().clone() {
            Token::String(value) => {
                self.advance();
                Ok(ParsedExpr::leaf(Expr::Literal(Value::String(value))))
            }
            Token::Number(value) => {
                self.advance();
                let number = value
                    .parse::<serde_json::Number>()
                    .map(Value::Number)
                    .unwrap_or(Value::Null);
                Ok(ParsedExpr::leaf(Expr::Literal(number)))
            }
            Token::Bool(value) => {
                self.advance();
                Ok(ParsedExpr::leaf(Expr::Literal(Value::Bool(value))))
            }
            Token::Null => {
                self.advance();
                Ok(ParsedExpr::leaf(Expr::Literal(Value::Null)))
            }
            Token::Ident(name) => self.parse_ident_or_call(name),
            Token::LParen => {
                self.advance();
                let expr = self.parse_tracked_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            Token::End => Err(ExpressionError::Eof),
            token => Err(ExpressionError::Unexpected(format!("{token:?}"))),
        }
    }

    fn parse_ident_or_call(&mut self, name: String) -> Result<ParsedExpr, ExpressionError> {
        self.advance();
        if matches!(self.current(), Token::LParen) {
            self.advance();
            let mut args = Vec::new();
            let mut deepest_arg = 0usize;
            if !matches!(self.current(), Token::RParen) {
                loop {
                    let arg = self.parse_tracked_expr()?;
                    deepest_arg = deepest_arg.max(arg.depth);
                    args.push(arg.expr);
                    if matches!(self.current(), Token::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
            self.expect(Token::RParen)?;
            let depth = node_depth(deepest_arg)?;
            let call = ParsedExpr {
                expr: Expr::Call { name, args },
                depth,
            };
            // Check for trailing member access: fromJSON('...').*.name,
            // fn()[0], or fn()[expr.key]
            self.parse_member_suffix(call)
        } else {
            let mut base = ParsedExpr::leaf(Expr::Path(vec![name]));
            loop {
                match self.current() {
                    // Dot access: a.b or a.*
                    Token::Dot => {
                        self.advance();
                        match self.current().clone() {
                            Token::Ident(segment) => {
                                self.advance();
                                push_path_segment(&mut base, segment)?;
                            }
                            Token::Star => {
                                self.advance();
                                push_path_segment(&mut base, "*".to_string())?;
                            }
                            other => {
                                return Err(ExpressionError::Unexpected(format!("{other:?}")));
                            }
                        }
                    }
                    // Bracket access: a['key'], a[0], a[ident], or
                    // a[full.expression] (e.g. env[matrix.target.options]).
                    Token::LBracket => {
                        self.advance();
                        // Literal fast path: a single string/number/ident
                        // followed by `]` keeps the historical Path shape.
                        let literal = match self.current().clone() {
                            Token::String(s) => {
                                self.advance();
                                Some(s)
                            }
                            Token::Number(n) => {
                                self.advance();
                                Some(n)
                            }
                            Token::Ident(s) => {
                                // Only a bare ident: `a[b]` stays a literal
                                // key; anything dotted (`a[b.c]`) parses as
                                // an expression below.
                                if matches!(self.tokens.get(self.index + 1), Some(Token::RBracket))
                                {
                                    self.advance();
                                    Some(s)
                                } else {
                                    None
                                }
                            }
                            _ => None,
                        };
                        match literal {
                            Some(segment) => {
                                self.expect(Token::RBracket)?;
                                push_path_segment(&mut base, segment)?;
                            }
                            None => {
                                let key = self.parse_tracked_expr()?;
                                self.expect(Token::RBracket)?;
                                base = ParsedExpr::index(base, key)?;
                            }
                        }
                    }
                    _ => break,
                }
            }
            Ok(base)
        }
    }

    /// Parse trailing `.ident`, `.*`, `['key']`, or `[expr]` segments after
    /// an expression, folding them onto `base`.
    ///
    /// A depth violation is fatal to the parse (unlike a malformed key, which
    /// still backs off); it must never be swallowed into a silently truncated
    /// expression.
    fn parse_member_suffix(&mut self, mut base: ParsedExpr) -> Result<ParsedExpr, ExpressionError> {
        loop {
            match self.current() {
                Token::Dot => {
                    self.advance();
                    match self.current().clone() {
                        Token::Ident(segment) => {
                            self.advance();
                            push_path_segment(&mut base, segment)?;
                        }
                        Token::Star => {
                            self.advance();
                            push_path_segment(&mut base, "*".to_string())?;
                        }
                        _ => break,
                    }
                }
                Token::LBracket => {
                    self.advance();
                    let literal = match self.current().clone() {
                        Token::String(s) => {
                            self.advance();
                            Some(s)
                        }
                        Token::Number(n) => {
                            self.advance();
                            Some(n)
                        }
                        Token::Ident(s) => {
                            if matches!(self.tokens.get(self.index + 1), Some(Token::RBracket)) {
                                self.advance();
                                Some(s)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    match literal {
                        Some(segment) => {
                            if self.expect(Token::RBracket).is_err() {
                                break;
                            }
                            push_path_segment(&mut base, segment)?;
                        }
                        None => {
                            // Suffix parsing never fails its caller: only
                            // fold the index when the key and bracket parse.
                            let save = self.index;
                            let saved_depth = self.depth;
                            match self.parse_tracked_expr().and_then(|key| {
                                self.expect(Token::RBracket)?;
                                Ok(key)
                            }) {
                                Ok(key) => base = ParsedExpr::index(base, key)?,
                                Err(e @ ExpressionError::TooDeep(_)) => return Err(e),
                                Err(_) => {
                                    // Rewind the cursor and recursion depth to
                                    // the pre-key state.
                                    self.index = save;
                                    self.depth = saved_depth;
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        Ok(base)
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

/// Append a literal path segment onto a base expression, extending a
/// trailing static path when one is open. Only wrapping a non-path base adds
/// a level to the tree.
fn push_path_segment(base: &mut ParsedExpr, segment: String) -> Result<(), ExpressionError> {
    match &mut base.expr {
        Expr::Path(path) => path.push(segment),
        Expr::MemberAccess { path, .. } => path.push(segment),
        other => {
            let depth = node_depth(base.depth)?;
            let drained = std::mem::replace(other, Expr::Literal(serde_json::Value::Null));
            *other = Expr::MemberAccess {
                expr: Box::new(drained),
                path: vec![segment],
            };
            base.depth = depth;
        }
    }
    Ok(())
}
