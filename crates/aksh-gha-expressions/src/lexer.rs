use super::ExpressionError;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
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

pub(crate) struct Lexer<'a> {
    input: &'a str,
    chars: std::str::Chars<'a>,
    offset: usize,
}

impl<'a> Lexer<'a> {
    pub(crate) fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars(),
            offset: 0,
        }
    }

    pub(crate) fn lex(mut self) -> Result<Vec<Token>, ExpressionError> {
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
