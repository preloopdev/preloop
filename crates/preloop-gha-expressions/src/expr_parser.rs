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
