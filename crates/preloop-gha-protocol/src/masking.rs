//! Shared secret-masking logic.
//!
//! All secret redaction across the codebase (runner durable logs, live logs,
//! server log persistence, DAP debugger output) must use this canonical
//! implementation to guarantee consistent, longest-first replacement order.
//!
//! Two masking strengths exist:
//!
//! * [`mask_secrets`] / [`mask_secrets_preserving_lines`]: exact,
//!   case-sensitive matching of the registered values. Used where the input
//!   is protocol-shaped and over-masking would break framing (DAP transport).
//! * [`mask_secrets_transform_aware`] /
//!   [`mask_secrets_preserving_lines_transform_aware`]: the log-masking
//!   strength. Every secret is additionally matched by its transformed
//!   variants ([`secret_variants`]: base64 standard + URL-safe, padded and
//!   unpadded, percent-encoding, hex) and matching is ASCII case-insensitive,
//!   so `::add-mask::` secrets cannot be exfiltrated through trivial
//!   encodings. All runner log paths and the server-side log persistence
//!   pass must use these.
//!
//! [`StreamingMasker`] provides the same transform-aware strength for
//! chunked writers: secrets split across write-chunk boundaries are matched
//! on the reassembled stream via a trailing-overlap buffer, never per-chunk.

/// The redaction marker used across all masking paths.
pub const MASK_MARKER: &str = "***";

/// Mask secret values in `input`, replacing each occurrence with [`MASK_MARKER`].
///
/// Secrets are replaced longest-first to prevent a shorter secret that is a
/// substring of a longer one from partially exposing the longer secret.
///
/// Empty secrets are silently skipped.
///
/// If `exclude` is non-empty, secrets whose value appears in the exclusion list
/// are preserved. This supports the DAP protocol-keyword allow-list
/// (`response`, `initialize`, `event`) — those strings must not be redacted on
/// the DAP transport even if they collide with a secret value.
pub fn mask_secrets<'a, I>(input: &str, secrets: I, exclude: &[&str]) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    mask_secrets_inner(input, secrets, exclude, false, false)
}

/// Transform-aware masking for log output.
///
/// Every secret is matched together with its transformed variants (see
/// [`secret_variants`]) and matching is ASCII case-insensitive, so a secret
/// registered via `::add-mask::` cannot leak through base64, percent- or
/// hex-encoding, or case changes. This is the strength all log-masking paths
/// (runner durable/live logs, server-side persistence) must use;
/// [`mask_secrets`] stays exact for protocol-shaped traffic.
pub fn mask_secrets_transform_aware<'a, I>(input: &str, secrets: I, exclude: &[&str]) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    mask_secrets_inner(input, secrets, exclude, false, true)
}

/// Mask secret values like [`mask_secrets`], but preserve the newline
/// structure of the input: a secret spanning N lines is replaced with one
/// [`MASK_MARKER`] per line instead of a single marker.
///
/// Retroactive masking rewrites the durable log after line checkpoints were
/// already captured (`StepContext::log_line_count` /
/// `log_content_since`). Collapsing a multi-line secret into one marker would
/// silently invalidate those checkpoints, so the retroactive path must keep
/// every physical record intact.
pub fn mask_secrets_preserving_lines<'a, I>(input: &str, secrets: I, exclude: &[&str]) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    mask_secrets_inner(input, secrets, exclude, true, false)
}

/// Transform-aware variant of [`mask_secrets_preserving_lines`]: variant
/// expansion plus ASCII case-insensitive matching, while still emitting one
/// [`MASK_MARKER`] per physical line so line checkpoints stay valid.
pub fn mask_secrets_preserving_lines_transform_aware<'a, I>(
    input: &str,
    secrets: I,
    exclude: &[&str],
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    mask_secrets_inner(input, secrets, exclude, true, true)
}

/// Byte-safety guard for slicing `haystack` at a secret's byte length.
/// `str::starts_with` would panic on a mid-character split; callers that
/// pre-slice for case-insensitive comparison must check this first.
fn char_boundary_at(haystack: &str, len: usize) -> bool {
    haystack.len() >= len && haystack.is_char_boundary(len)
}

/// Longest-first match of any secret at the start of `remaining`.
///
/// When `case_insensitive` is set, comparison folds ASCII case on both
/// sides; non-ASCII bytes still require exact equality.
fn match_secret_at<'s>(
    remaining: &str,
    secrets: &'s [String],
    case_insensitive: bool,
) -> Option<&'s str> {
    secrets.iter().find_map(|secret| {
        if !char_boundary_at(remaining, secret.len()) {
            return None;
        }
        let head = &remaining[..secret.len()];
        let matched = if case_insensitive {
            head.eq_ignore_ascii_case(secret)
        } else {
            head == secret.as_str()
        };
        matched.then_some(secret.as_str())
    })
}

fn mask_secrets_inner<'a, I>(
    input: &str,
    secrets: I,
    exclude: &[&str],
    preserve_lines: bool,
    transform_aware: bool,
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut sorted: Vec<String> = Vec::new();
    for secret in secrets.into_iter().filter(|s| !s.is_empty()) {
        if exclude.contains(&secret) {
            continue;
        }
        if !sorted.iter().any(|existing| existing == secret) {
            sorted.push(secret.to_owned());
        }
        if transform_aware {
            for variant in secret_variants(secret) {
                if !sorted.iter().any(|existing| existing == &variant) {
                    sorted.push(variant);
                }
            }
        }
    }
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));

    // Scan the original input once. A marker run that overlaps a short
    // marker-valued secret is canonicalized to one replacement marker, rather
    // than being mistaken for unmasked input or expanded on every pass.
    let mut result = String::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        let remaining = &input[offset..];
        let case_insensitive = transform_aware;
        let matching_secret = match_secret_at(remaining, &sorted, case_insensitive);
        if let Some(secret) = matching_secret {
            if remaining.starts_with(MASK_MARKER) && secret.len() <= MASK_MARKER.len() {
                // `***`, `**`, and `*` are all represented by the canonical
                // marker. Consume the complete marker span so re-masking stays
                // idempotent while the secret is still treated as matched.
                result.push_str(MASK_MARKER);
                offset += MASK_MARKER.len();
            } else if preserve_lines {
                // Keep the physical record count stable: re-emit every
                // newline inside the matched secret so line checkpoints
                // captured before this retroactive pass stay valid.
                result.push_str(MASK_MARKER);
                for ch in secret.chars() {
                    if ch == '\n' || ch == '\r' {
                        result.push(ch);
                        result.push_str(MASK_MARKER);
                    }
                }
                offset += secret.len();
            } else {
                result.push_str(MASK_MARKER);
                offset += secret.len();
            }
        } else if remaining.starts_with(MASK_MARKER) {
            // Existing redaction output is opaque when no registered secret
            // starts there.
            result.push_str(MASK_MARKER);
            offset += MASK_MARKER.len();
        } else {
            let character = remaining
                .chars()
                .next()
                .expect("offset always points at a character boundary");
            result.push(character);
            offset += character.len_utf8();
        }
    }
    result
}

/// Transformed variants of a secret value that must all be masked.
///
/// A secret registered via `::add-mask::` (or any other mask source) is
/// matched not only literally but also as:
/// - base64, standard alphabet, padded and unpadded
/// - base64, URL-safe alphabet, padded and unpadded
/// - percent-encoding (`%XX`, uppercase hex digits)
/// - hex, lowercase and uppercase
///
/// The raw value itself is NOT included; callers mask the raw value plus
/// these variants (see [`mask_secrets_transform_aware`]). Empty results and
/// duplicates are omitted. Percent-encoding uses uppercase hex digits, but
/// matching is case-insensitive in the transform-aware paths, so lowercase
/// `%xx` forms are covered too.
pub fn secret_variants(secret: &str) -> Vec<String> {
    use base64::engine::Engine as _;

    fn push_unique(out: &mut Vec<String>, raw: &str, value: String) {
        if !value.is_empty() && value != raw && !out.iter().any(|existing| existing == &value) {
            out.push(value);
        }
    }

    let mut variants = Vec::new();
    if secret.is_empty() {
        return variants;
    }
    let bytes = secret.as_bytes();
    push_unique(
        &mut variants,
        secret,
        base64::engine::general_purpose::STANDARD.encode(bytes),
    );
    push_unique(
        &mut variants,
        secret,
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes),
    );
    push_unique(
        &mut variants,
        secret,
        base64::engine::general_purpose::URL_SAFE.encode(bytes),
    );
    push_unique(
        &mut variants,
        secret,
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes),
    );
    // Percent-encoding: RFC 3986 unreserved characters stay literal, every
    // other byte becomes %XX.
    let mut encoded = String::with_capacity(secret.len());
    for byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(*byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    push_unique(&mut variants, secret, encoded);
    push_unique(
        &mut variants,
        secret,
        bytes.iter().map(|b| format!("{b:02x}")).collect(),
    );
    push_unique(
        &mut variants,
        secret,
        bytes.iter().map(|b| format!("{b:02X}")).collect(),
    );
    variants
}

/// Strip userinfo (`user:password@`) from a URL before logging.
///
/// Best-effort and dependency-free: finds `://`, then drops everything up to
/// the last `@` before the authority terminator (`/`, `?`, `#`). Non-URL
/// input passes through unchanged. This is "don't print", not "mask after
/// printing": credentials must never reach a log line, even redacted.
pub fn strip_url_userinfo(url: &str) -> String {
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &url[after_scheme..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    match authority.rfind('@') {
        Some(at) => format!(
            "{}{}{}",
            &url[..after_scheme],
            &authority[at + 1..],
            remainder
        ),
        None => url.to_owned(),
    }
}

/// Transform-aware masking for chunked writers.
///
/// Secrets can split across write-chunk boundaries, where per-chunk naive
/// matching misses them. `StreamingMasker` masks on the reassembled stream:
/// each [`push`](Self::push) appends the chunk to a trailing-overlap buffer,
/// masks the reassembled window, emits every byte that can no longer be part
/// of a secret head, and withholds the rest. [`finish`](Self::finish) flushes
/// the withheld tail once the stream ends.
///
/// Matching strength equals [`mask_secrets_transform_aware`]: raw values
/// plus [`secret_variants`], ASCII case-insensitive. The withheld tail is
/// bounded by the longest variant length (minus one byte).
///
/// Byte streams that are not valid UTF-8 are masked lossily, like the rest
/// of this module: the carry always holds valid UTF-8.
pub struct StreamingMasker {
    /// Raw values plus variants, longest-first.
    secrets: Vec<String>,
    /// Longest variant byte length; the withheld tail never exceeds this
    /// minus one byte.
    max_len: usize,
    /// Withheld raw tail (always valid UTF-8) from the previous push.
    carry: Vec<u8>,
}

impl StreamingMasker {
    /// Build a masker from raw secret values; variants are expanded here, so
    /// callers pass each secret once.
    pub fn new<'a, I>(secrets: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut expanded: Vec<String> = Vec::new();
        for secret in secrets.into_iter().filter(|s| !s.is_empty()) {
            if !expanded.iter().any(|existing| existing == secret) {
                expanded.push(secret.to_owned());
            }
            for variant in secret_variants(secret) {
                if !expanded.iter().any(|existing| existing == &variant) {
                    expanded.push(variant);
                }
            }
        }
        expanded.sort_by_key(|s| std::cmp::Reverse(s.len()));
        let max_len = expanded.iter().map(String::len).max().unwrap_or(0);
        Self {
            secrets: expanded,
            max_len,
            carry: Vec::new(),
        }
    }

    /// Number of distinct match patterns (raw values plus variants).
    pub fn pattern_count(&self) -> usize {
        self.secrets.len()
    }

    /// Mask `chunk`, returning the bytes that are safe to emit now.
    ///
    /// A secret split across this chunk and the next is matched on the
    /// reassembled window; only bytes that cannot begin an incomplete
    /// secret are emitted. The withheld tail is returned by a later
    /// [`push`](Self::push) or by [`finish`](Self::finish).
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut window = std::mem::take(&mut self.carry);
        window.extend_from_slice(chunk);
        // Lossy like the rest of this module; the carry stays valid UTF-8.
        let text = String::from_utf8_lossy(&window).into_owned();
        // Never emit an unmasked byte at or after `emit_until`: it could be
        // the head of a secret completed by the next chunk. Bytes consumed
        // by a complete match are always safe to emit redacted.
        let emit_until = text.len().saturating_sub(self.max_len.saturating_sub(1));
        let mut out = String::with_capacity(text.len());
        let mut pos = 0;
        while pos < text.len() {
            let remaining = &text[pos..];
            if let Some(secret) = match_secret_at(remaining, &self.secrets, true) {
                out.push_str(MASK_MARKER);
                pos += secret.len();
                continue;
            }
            if pos >= emit_until && is_secret_prefix(remaining, &self.secrets) {
                // Possible secret head: withhold until the next chunk (or
                // finish) decides.
                break;
            }
            let ch = remaining
                .chars()
                .next()
                .expect("pos always points at a character boundary");
            out.push(ch);
            pos += ch.len_utf8();
        }
        self.carry = text.as_bytes()[pos..].to_vec();
        out.into_bytes()
    }

    /// Mask and return the withheld tail. A trailing partial secret cannot
    /// be completed (the stream ended) so it is emitted as-is; only complete
    /// matches are redacted.
    pub fn finish(&mut self) -> Vec<u8> {
        let carry = std::mem::take(&mut self.carry);
        let text = String::from_utf8_lossy(&carry);
        let mut out = String::with_capacity(text.len());
        let mut pos = 0;
        while pos < text.len() {
            let remaining = &text[pos..];
            if let Some(secret) = match_secret_at(remaining, &self.secrets, true) {
                out.push_str(MASK_MARKER);
                pos += secret.len();
                continue;
            }
            let ch = remaining
                .chars()
                .next()
                .expect("pos always points at a character boundary");
            out.push(ch);
            pos += ch.len_utf8();
        }
        out.into_bytes()
    }
}

/// Whether `remaining` is a proper prefix of any secret (some secret is
/// strictly longer and starts with it, ASCII case-insensitive). Used by
/// [`StreamingMasker`] to decide which tail bytes must be withheld.
fn is_secret_prefix(remaining: &str, secrets: &[String]) -> bool {
    secrets.iter().any(|secret| {
        secret.len() > remaining.len()
            && char_boundary_at(secret, remaining.len())
            && secret[..remaining.len()].eq_ignore_ascii_case(remaining)
    })
}

/// Resolve an entire [`crate::SecretMap`] to plaintext, keyed by secret name.
///
/// This function and [`expose_values`] are the only sanctioned boundary where a
/// whole collection of secrets becomes plaintext. Callers must resolve once and
/// then iterate the returned collection; re-exposing per element inside a loop
/// or iterator closure scatters plaintext across the codebase and defeats the
/// audit rule `rules/no-expose-in-loop.yml`.
pub fn expose_all(secrets: &crate::SecretMap) -> std::collections::BTreeMap<String, String> {
    secrets
        .iter()
        .map(|(name, secret)| (name.clone(), secret.expose().to_owned()))
        .collect()
}

/// Resolve an iterator of secrets to their plaintext values, order preserved.
///
/// Duplicates are kept: the result mirrors the input one-for-one so that
/// callers can hand it straight to [`mask_secrets`], which does its own
/// filtering and longest-first ordering.
///
/// See [`expose_all`] for why callers must not re-expose per element instead.
pub fn expose_values<'a, I>(secrets: I) -> Vec<String>
where
    I: IntoIterator<Item = &'a crate::SecretString>,
{
    secrets
        .into_iter()
        .map(|secret| secret.expose().to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SecretMap, SecretString};

    #[test]
    fn empty_secrets_are_skipped() {
        assert_eq!(
            mask_secrets("hello", ["", ""].iter().copied(), &[]),
            "hello"
        );
    }

    #[test]
    fn longest_first_prevents_partial_exposure() {
        // "sec" is a substring of "secret". Longest-first means "secret" is
        // replaced as a whole, not partially as "***ret".
        let result = mask_secrets("my secret value", ["sec", "secret"].iter().copied(), &[]);
        assert_eq!(result, "my *** value");
    }

    #[test]
    fn overlapping_secrets_both_masked() {
        let result = mask_secrets("ab abc", ["ab", "abc"].iter().copied(), &[]);
        // "abc" replaced first (longest), then "ab" in the remaining text.
        assert_eq!(result, "*** ***");
    }

    #[test]
    fn exclusion_list_preserves_keywords() {
        let result = mask_secrets(
            "response: secret",
            ["response", "secret"].iter().copied(),
            &["response"],
        );
        assert_eq!(result, "response: ***");
    }

    #[test]
    fn idempotent_double_mask() {
        let input = "my secret data";
        let once = mask_secrets(input, ["secret"].iter().copied(), &[]);
        let twice = mask_secrets(&once, ["secret"].iter().copied(), &[]);
        assert_eq!(once, twice);
    }

    #[test]
    fn marker_valued_secrets_are_redacted_without_expansion() {
        for secret in ["*", "**", "***"] {
            let input = format!("before {secret} after");
            let masked = mask_secrets(&input, [secret].iter().copied(), &[]);
            assert_eq!(masked, "before *** after");
            assert_eq!(mask_secrets(&masked, [secret].iter().copied(), &[]), masked);
        }
    }
    #[test]
    fn wildcard_masks_do_not_expand_existing_markers() {
        let input = "before *** after";
        let once = mask_secrets(input, ["*"].iter().copied(), &[]);
        let twice = mask_secrets(&once, ["*"].iter().copied(), &[]);
        assert_eq!(once, input);
        assert_eq!(twice, input);
    }

    #[test]
    fn no_secrets_returns_input_unchanged() {
        let input = "nothing to mask here";
        assert_eq!(mask_secrets(input, std::iter::empty(), &[]), input);
    }

    #[test]
    fn preserving_lines_keeps_newline_count() {
        let input = "before token-a\ntoken-b after";
        let masked =
            mask_secrets_preserving_lines(input, ["token-a\ntoken-b"].iter().copied(), &[]);
        assert_eq!(masked, "before ***\n*** after");
        assert_eq!(
            masked.bytes().filter(|b| *b == b'\n').count(),
            input.bytes().filter(|b| *b == b'\n').count()
        );
    }

    #[test]
    fn preserving_lines_matches_collapsing_for_single_line_secrets() {
        let input = "my secret value";
        assert_eq!(
            mask_secrets_preserving_lines(input, ["secret"].iter().copied(), &[]),
            mask_secrets(input, ["secret"].iter().copied(), &[]),
        );
    }

    #[test]
    fn preserving_lines_is_idempotent() {
        let input = "leak token-a\ntoken-b here";
        let once = mask_secrets_preserving_lines(input, ["token-a\ntoken-b"].iter().copied(), &[]);
        let twice = mask_secrets_preserving_lines(&once, ["token-a\ntoken-b"].iter().copied(), &[]);
        assert_eq!(once, twice);
    }

    #[test]
    fn expose_all_round_trips_names_and_values() {
        let mut secrets = SecretMap::new();
        secrets.insert("TOKEN".to_owned(), SecretString::new("t0p"));
        secrets.insert("EMPTY".to_owned(), SecretString::new(""));
        let exposed = expose_all(&secrets);
        assert_eq!(exposed.len(), 2);
        assert_eq!(exposed["TOKEN"], "t0p");
        assert_eq!(exposed["EMPTY"], "");
    }

    #[test]
    fn expose_values_preserves_every_value_including_duplicates() {
        let secrets = [
            SecretString::new("dup"),
            SecretString::new("other"),
            SecretString::new("dup"),
        ];
        assert_eq!(expose_values(secrets.iter()), vec!["dup", "other", "dup"]);
    }

    #[test]
    fn empty_input_yields_empty_collection() {
        assert!(expose_all(&SecretMap::new()).is_empty());
        assert!(expose_values(std::iter::empty()).is_empty());
    }

    // ── transform-aware masking ──────────────────────────────────────

    #[test]
    fn secret_variants_cover_encodings() {
        use base64::engine::Engine as _;
        let secret = "pentest-mask-9f3k2";
        let variants = secret_variants(secret);
        let b64 = base64::engine::general_purpose::STANDARD.encode(secret);
        assert!(
            variants.contains(&b64),
            "missing standard base64: {variants:?}"
        );
        assert!(
            variants.contains(&base64::engine::general_purpose::STANDARD_NO_PAD.encode(secret)),
            "missing unpadded base64"
        );
        assert!(
            variants.contains(&base64::engine::general_purpose::URL_SAFE.encode(secret)),
            "missing url-safe base64"
        );
        assert!(
            variants.contains(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret)),
            "missing url-safe unpadded base64"
        );
        // '-' is unreserved, so only the raw bytes change nothing here; use a
        // secret with bytes that must be escaped.
        let variants2 = secret_variants("a+b/c=");
        assert!(
            variants2
                .iter()
                .any(|v| v.contains("%2B") && v.contains("%2F") && v.contains("%3D")),
            "missing percent-encoding: {variants2:?}"
        );
        assert!(
            variants2.contains(&"612b622f633d".to_string()),
            "missing lowercase hex: {variants2:?}"
        );
        assert!(
            variants2.contains(&"612B622F633D".to_string()),
            "missing uppercase hex: {variants2:?}"
        );
        assert!(
            !variants.contains(&secret.to_string()),
            "raw value is not a variant"
        );
        assert!(secret_variants("").is_empty());
    }

    #[test]
    fn transform_aware_masks_base64_echo_of_add_mask_secret() {
        // Round-3 §9 repro: ::add-mask::pentest-mask-9f3k2 then base64 echo.
        use base64::engine::Engine as _;
        let secret = "pentest-mask-9f3k2";
        let b64 = base64::engine::general_purpose::STANDARD.encode(secret);
        let log = format!("raw={secret} b64={b64}");
        let masked = mask_secrets_transform_aware(&log, [secret].iter().copied(), &[]);
        assert!(!masked.contains(secret), "raw secret leaked: {masked}");
        assert!(!masked.contains(&b64), "base64 secret leaked: {masked}");
        assert_eq!(masked, "raw=*** b64=***");
    }

    #[test]
    fn transform_aware_masks_percent_and_hex_encodings() {
        let secret = "s3cr+t";
        let log = "pct=s3cr%2Bt hex=733363722b74 HEX=733363722B74".to_string();
        let masked = mask_secrets_transform_aware(&log, [secret].iter().copied(), &[]);
        assert_eq!(masked, "pct=*** hex=*** HEX=***", "leaked: {masked}");
        // Lowercase %xx is covered by case-insensitive matching.
        let masked = mask_secrets_transform_aware("pct=s3cr%2bt", [secret].iter().copied(), &[]);
        assert_eq!(masked, "pct=***");
    }

    #[test]
    fn transform_aware_is_case_insensitive() {
        let masked = mask_secrets_transform_aware(
            "leak SeCrEt and SECRET here",
            ["secret"].iter().copied(),
            &[],
        );
        assert_eq!(masked, "leak *** and *** here");
    }

    #[test]
    fn transform_aware_honors_exclusion_list() {
        let masked = mask_secrets_transform_aware(
            "response: hunter2",
            ["response", "hunter2"].iter().copied(),
            &["response"],
        );
        assert_eq!(masked, "response: ***");
    }

    #[test]
    fn transform_aware_preserving_lines_masks_variants_per_line() {
        use base64::engine::Engine as _;
        let secret = "line-secret";
        let b64 = base64::engine::general_purpose::STANDARD.encode(secret);
        let input = format!("before {b64}\nafter");
        let masked =
            mask_secrets_preserving_lines_transform_aware(&input, [secret].iter().copied(), &[]);
        assert_eq!(masked, "before ***\nafter");
        assert_eq!(
            masked.bytes().filter(|b| *b == b'\n').count(),
            input.bytes().filter(|b| *b == b'\n').count()
        );
    }

    #[test]
    fn exact_mask_secrets_stays_case_sensitive() {
        // The DAP-strength path must not change behavior.
        assert_eq!(
            mask_secrets("SECRET secret", ["secret"].iter().copied(), &[]),
            "SECRET ***"
        );
    }

    #[test]
    fn strip_url_userinfo_removes_credentials() {
        assert_eq!(
            strip_url_userinfo("http://user:p%40ss@proxy.example:8080/path?q=1"),
            "http://proxy.example:8080/path?q=1"
        );
        assert_eq!(
            strip_url_userinfo("https://user@proxy.example/"),
            "https://proxy.example/"
        );
        assert_eq!(
            strip_url_userinfo("http://proxy.example:8080"),
            "http://proxy.example:8080"
        );
        assert_eq!(strip_url_userinfo("not a url"), "not a url");
        assert_eq!(strip_url_userinfo(""), "");
        // Userinfo-like text without a scheme still strips at the last @.
        assert_eq!(strip_url_userinfo("user:pass@host"), "host");
    }

    // ── StreamingMasker ──────────────────────────────────────────────

    fn stream_chunks(masker: &mut StreamingMasker, chunks: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in chunks {
            out.extend_from_slice(&masker.push(chunk));
        }
        out.extend_from_slice(&masker.finish());
        out
    }

    #[test]
    fn streaming_masker_masks_secret_split_across_chunks() {
        // The chunk-boundary regression test: the secret never appears whole
        // in any single chunk.
        let secret = "split-across-chunks-secret";
        for split in 1..secret.len() {
            let mut masker = StreamingMasker::new([secret].iter().copied());
            let (head, tail) = secret.split_at(split);
            let out = stream_chunks(
                &mut masker,
                &[b"log ", head.as_bytes(), tail.as_bytes(), b" end"],
            );
            let text = String::from_utf8(out).unwrap();
            assert_eq!(text, "log *** end", "split at {split}: {text}");
        }
    }

    #[test]
    fn streaming_masker_masks_encoded_secret_split_across_chunks() {
        use base64::engine::Engine as _;
        let secret = "pentest-mask-9f3k2";
        let b64 = base64::engine::general_purpose::STANDARD.encode(secret);
        for split in [1, 7, b64.len() - 1] {
            let mut masker = StreamingMasker::new([secret].iter().copied());
            let (head, tail) = b64.split_at(split);
            let out = stream_chunks(&mut masker, &[b"echo ", head.as_bytes(), tail.as_bytes()]);
            let text = String::from_utf8(out).unwrap();
            assert_eq!(text, "echo ***", "split at {split}: {text}");
        }
    }

    #[test]
    fn streaming_masker_withholds_only_secret_heads() {
        // Non-secret bytes stream through immediately, chunk for chunk.
        let mut masker = StreamingMasker::new(["zz-secret-zz"].iter().copied());
        let first = masker.push(b"hello world ");
        assert_eq!(first, b"hello world ", "safe bytes must not be delayed");
        let second = masker.push(b"zz-sec");
        assert!(
            second.is_empty(),
            "secret head must be withheld, got {second:?}"
        );
        let third = masker.push(b"ret-zz done");
        let tail = masker.finish();
        let text = String::from_utf8([second, third, tail].concat()).unwrap();
        assert_eq!(text, "*** done");
    }

    #[test]
    fn streaming_masker_trailing_partial_secret_is_not_a_secret() {
        // The stream ends mid-secret: nothing more can complete it, so the
        // partial bytes are emitted as-is by finish().
        let mut masker = StreamingMasker::new(["complete-secret"].iter().copied());
        let mut out = masker.push(b"prefix complete-sec");
        out.extend_from_slice(&masker.finish());
        assert_eq!(String::from_utf8(out).unwrap(), "prefix complete-sec");
    }

    #[test]
    fn streaming_masker_is_case_insensitive() {
        let mut masker = StreamingMasker::new(["MiXeD"].iter().copied());
        let out = stream_chunks(&mut masker, &[b"a ", b"mIx", b"eD z"]);
        assert_eq!(String::from_utf8(out).unwrap(), "a *** z");
    }

    #[test]
    fn streaming_masker_single_push_matches_batch_mask() {
        let secrets = ["alpha", "beta-secret", "gamma+delta"];
        let input = "alpha and beta-secret with gamma+delta inside";
        let mut masker = StreamingMasker::new(secrets.iter().copied());
        let streamed = stream_chunks(&mut masker, &[input.as_bytes()]);
        let batched = mask_secrets_transform_aware(input, secrets.iter().copied(), &[]);
        assert_eq!(String::from_utf8(streamed).unwrap(), batched);
    }
}
