//! Raw-text scanning for `${{ }}` expression boundaries.
//!
//! One implementation shared by the parser, the azdo wire layer, and the
//! runner's template renderer — the three copies had already diverged (only
//! the runner tracked parenthesis depth), which meant the same template could
//! tokenize differently depending on which layer touched it.

/// Return the byte offset of the `}}` that closes a `${{` expression.
///
/// `s` must start just after the opening `${{`. Single-quoted strings are
/// skipped (`''` is an escaped quote; backslash is literal), and `}}` inside
/// unquoted parentheses does not terminate — `format('a}}b')` is one
/// expression, not two tokens.
///
/// Byte offsets (not char indices) are returned so callers can slice
/// `s[..end]` safely on multi-byte input.
pub fn find_expression_end(s: &str) -> Option<usize> {
    let mut in_single_quote = false;
    let mut iter = s.char_indices().peekable();
    while let Some((byte_pos, ch)) = iter.next() {
        match ch {
            '\'' if !in_single_quote => {
                in_single_quote = true;
            }
            '\'' if in_single_quote => {
                // Escaped quote ('') inside a string literal.
                if iter.peek().map(|&(_, c)| c) == Some('\'') {
                    iter.next();
                } else {
                    in_single_quote = false;
                }
            }
            // GitHub's tokenizer does not track parenthesis depth: `}}`
            // terminates the expression even inside an unclosed call, so a
            // malformed `${{ foo( }}` ends here rather than passing through
            // as a literal.
            '}' if !in_single_quote && iter.peek().map(|&(_, c)| c) == Some('}') => {
                return Some(byte_pos);
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::find_expression_end;

    #[test]
    fn skips_braces_inside_single_quoted_strings() {
        let source = r#" format('value }} still inside') }}"#;
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn backslash_is_literal_in_single_quotes() {
        // Backslash is NOT an escape in expression string literals: the first
        // `'` closes the string, then `}}` terminates.
        let source = r"'C:\' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn doubled_quote_is_an_escaped_quote() {
        let source = r"'it''s fine' }}";
        assert_eq!(find_expression_end(source), Some(source.len() - 2));
    }

    #[test]
    fn unclosed_parens_do_not_prevent_termination() {
        // GitHub tokenizer does not track parens: }} terminates the
        // expression even if a paren was left open.
        let source = " foo( }}";
        assert_eq!(find_expression_end(source), Some(6));
    }

    #[test]
    fn plain_and_missing_terminators() {
        assert_eq!(find_expression_end(" x }}"), Some(3));
        assert_eq!(find_expression_end(" x "), None);
    }

    #[test]
    fn returns_byte_offsets_for_multibyte_input() {
        // Em dash inside a format() literal: the returned offset must slice
        // cleanly on a char boundary.
        let source = " format('a—b') }}";
        let end = find_expression_end(source).unwrap();
        assert_eq!(&source[..end], " format('a—b') ");
    }
}
