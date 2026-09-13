use serde_json::Value;

use super::{
    ast::{BinaryOp, Expr},
    lexer::Token,
    ExpressionError,
};

/// Maximum expression nesting depth.
///
/// The parser is recursive descent and the evaluator recurses over the same
/// AST, so unbounded nesting (e.g. megabytes of `(((…` or `!!!…`) overflows
/// the thread stack, which aborts the process instead of unwinding. Real
/// workflow expressions are shallow; nothing legitimate nests near this.
pub(crate) const MAX_EXPRESSION_DEPTH: usize = 256;

pub(crate) struct Parser {
    tokens: Vec<Token>,
    index: usize,
    depth: usize,
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
        self.enter()?;
        let expr = self.parse_or();
        self.depth -= 1;
        expr
    }

    pub(crate) fn expect_end(&self) -> Result<(), ExpressionError> {
        match self.current() {
            Token::End => Ok(()),
            token => Err(ExpressionError::Unexpected(format!("{token:?}"))),
        }
    }

    fn parse_or(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_and()?;
        let mut chain_depth = 0;
        while matches!(self.current(), Token::Or) {
            self.advance();
            self.enter()?;
            chain_depth += 1;
            let right = self.parse_and()?;
            expr = Expr::Binary {
                op: BinaryOp::Or,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        self.depth -= chain_depth;
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_eq()?;
        let mut chain_depth = 0;
        while matches!(self.current(), Token::And) {
            self.advance();
            self.enter()?;
            chain_depth += 1;
            let right = self.parse_eq()?;
            expr = Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        self.depth -= chain_depth;
        Ok(expr)
    }

    fn parse_eq(&mut self) -> Result<Expr, ExpressionError> {
        let mut expr = self.parse_unary()?;
        let mut chain_depth = 0;
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
            self.enter()?;
            chain_depth += 1;
            let right = self.parse_unary()?;
            expr = Expr::Binary {
                op,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        self.depth -= chain_depth;
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<Expr, ExpressionError> {
        if matches!(self.current(), Token::Bang) {
            self.advance();
            self.enter()?;
            let inner = self.parse_unary();
            self.depth -= 1;
            Ok(Expr::UnaryNot(Box::new(inner?)))
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
            // Check for trailing member access: fromJSON('...').*.name,
            // fn()[0], or fn()[expr.key]
            Ok(self.parse_member_suffix(call))
        } else {
            let mut base = Expr::Path(vec![name]);
            loop {
                match self.current() {
                    // Dot access: a.b or a.*
                    Token::Dot => {
                        self.advance();
                        match self.current().clone() {
                            Token::Ident(segment) => {
                                self.advance();
                                push_path_segment(&mut base, segment);
                            }
                            Token::Star => {
                                self.advance();
                                push_path_segment(&mut base, "*".to_string());
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
                                push_path_segment(&mut base, segment);
                            }
                            None => {
                                let key = self.parse_expr()?;
                                self.expect(Token::RBracket)?;
                                base = Expr::Index {
                                    base: Box::new(base),
                                    key: Box::new(key),
                                };
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
    fn parse_member_suffix(&mut self, mut base: Expr) -> Expr {
        loop {
            match self.current() {
                Token::Dot => {
                    self.advance();
                    match self.current().clone() {
                        Token::Ident(segment) => {
                            self.advance();
                            push_path_segment(&mut base, segment);
                        }
                        Token::Star => {
                            self.advance();
                            push_path_segment(&mut base, "*".to_string());
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
                            push_path_segment(&mut base, segment);
                        }
                        None => {
                            // Suffix parsing never fails its caller: only
                            // fold the index when the key and bracket parse.
                            let save = self.index;
                            match self.parse_expr().and_then(|key| {
                                self.expect(Token::RBracket)?;
                                Ok(key)
                            }) {
                                Ok(key) => {
                                    base = Expr::Index {
                                        base: Box::new(base),
                                        key: Box::new(key),
                                    };
                                }
                                Err(_) => {
                                    self.index = save;
                                    break;
                                }
                            }
                        }
                    }
                }
                _ => break,
            }
        }
        base
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
/// trailing static path when one is open.
fn push_path_segment(base: &mut Expr, segment: String) {
    match base {
        Expr::Path(path) => path.push(segment),
        Expr::MemberAccess { path, .. } => path.push(segment),
        other => {
            let drained = std::mem::replace(other, Expr::Literal(serde_json::Value::Null));
            *other = Expr::MemberAccess {
                expr: Box::new(drained),
                path: vec![segment],
            };
        }
    }
}
