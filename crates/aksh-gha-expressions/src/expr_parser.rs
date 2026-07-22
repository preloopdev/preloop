use serde_json::Value;

use super::{
    ast::{BinaryOp, Expr},
    lexer::Token,
    ExpressionError,
};

pub(crate) struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    pub(crate) fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, index: 0 }
    }

    pub(crate) fn parse_expr(&mut self) -> Result<Expr, ExpressionError> {
        self.parse_or()
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
        let mut expr = if matches!(self.current(), Token::LParen) {
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
            Expr::Call { name, args }
        } else {
            Expr::Path(vec![name])
        };

        loop {
            match self.current() {
                Token::Dot => {
                    self.advance();
                    match self.current().clone() {
                        Token::Ident(segment) => {
                            self.advance();
                            expr = match expr {
                                Expr::Path(mut path) => {
                                    path.push(segment);
                                    Expr::Path(path)
                                }
                                other => Expr::MemberAccess {
                                    expr: Box::new(other),
                                    path: vec![segment],
                                },
                            };
                        }
                        Token::Star => {
                            self.advance();
                            expr = match expr {
                                Expr::Path(mut path) => {
                                    path.push("*".to_string());
                                    Expr::Path(path)
                                }
                                other => Expr::MemberAccess {
                                    expr: Box::new(other),
                                    path: vec!["*".to_string()],
                                },
                            };
                        }
                        other => {
                            return Err(ExpressionError::Unexpected(format!("{other:?}")));
                        }
                    }
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    self.expect(Token::RBracket)?;
                    expr = match (expr, &index) {
                        (Expr::Path(mut path), Expr::Literal(Value::String(s))) => {
                            path.push(s.clone());
                            Expr::Path(path)
                        }
                        (Expr::Path(mut path), Expr::Literal(Value::Number(n))) => {
                            path.push(n.to_string());
                            Expr::Path(path)
                        }
                        (Expr::Path(mut path), Expr::Path(p)) if p.len() == 1 => {
                            path.push(p[0].clone());
                            Expr::Path(path)
                        }
                        (
                            Expr::MemberAccess {
                                expr: base,
                                mut path,
                            },
                            Expr::Literal(Value::String(s)),
                        ) => {
                            path.push(s.clone());
                            Expr::MemberAccess { expr: base, path }
                        }
                        (
                            Expr::MemberAccess {
                                expr: base,
                                mut path,
                            },
                            Expr::Literal(Value::Number(n)),
                        ) => {
                            path.push(n.to_string());
                            Expr::MemberAccess { expr: base, path }
                        }
                        (
                            Expr::MemberAccess {
                                expr: base,
                                mut path,
                            },
                            Expr::Path(p),
                        ) if p.len() == 1 => {
                            path.push(p[0].clone());
                            Expr::MemberAccess { expr: base, path }
                        }
                        (base, idx) => Expr::IndexAccess {
                            expr: Box::new(base),
                            index: Box::new(idx.clone()),
                        },
                    };
                }
                _ => break,
            }
        }
        Ok(expr)
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
