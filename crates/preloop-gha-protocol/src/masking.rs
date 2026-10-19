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
//!   unpadded, percent-encoding, hex), matching is ASCII case-insensitive,
//!   and every base64 segment is decoded at all four group alignments, so a
//!   shifted encoding (`printf 'x%s' "$SECRET" | base64`) is redacted too —
//!   including one wrapped across a 76-column line break, one whose run mixes
//!   the two alphabet families, and one preceded by a label (`token=…`).
//!   Segments are decoded on the original input, so a variant match inside a
//!   token cannot split it into undecodable fragments, and trailing `=`
//!   padding is masked with the token so the encoded length is not left on
//!   display. Percent- and hex-encodings of a *shifted* payload stay outside
//!   that decode pass. All runner log paths and the server-side log
//!   persistence pass must use these.
//!
//! [`StreamingMasker`] provides the same transform-aware strength for
//! chunked writers: secrets split across write-chunk boundaries are matched
//! on the reassembled stream via a trailing-overlap buffer, never per-chunk.
//! The trailing-overlap buffer also withholds an unresolved trailing base64
//! run (bounded by the longest encoded secret span) so an alignment-shifted
//! encoding split across chunks is decided before any of its bytes are
//! emitted.

use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};

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
    let mut raw_secrets: Vec<String> = Vec::new();
    for secret in secrets.into_iter().filter(|s| !s.is_empty()) {
        if exclude.contains(&secret) {
            continue;
        }
        if !sorted.iter().any(|existing| existing == secret) {
            sorted.push(secret.to_owned());
        }
        if !raw_secrets.iter().any(|existing| existing == secret) {
            raw_secrets.push(secret.to_owned());
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

    // A shifted base64 encoding of a secret is decided on the original input,
    // before any variant replacement: masking a variant that sits inside the
    // token would insert a non-base64 [`MASK_MARKER`] into it, the fragments
    // would no longer decode, and the rest of the encoded secret would stay in
    // the output. The whole span is replaced instead, one marker per physical
    // line so line checkpoints stay valid.
    let alignment_spans = if transform_aware {
        base64_alignment_spans(input, &raw_secrets)
    } else {
        Vec::new()
    };

    // Scan the original input once. A marker run that overlaps a short
    // marker-valued secret is canonicalized to one replacement marker, rather
    // than being mistaken for unmasked input or expanded on every pass.
    let mut result = String::with_capacity(input.len());
    let mut offset = 0;
    let mut span_index = 0;
    while offset < input.len() {
        while span_index < alignment_spans.len() && alignment_spans[span_index].1 <= offset {
            span_index += 1;
        }
        if span_index < alignment_spans.len() && alignment_spans[span_index].0 <= offset {
            // One marker per physical line, like the variant pass, so line
            // checkpoints stay valid.
            let end = alignment_spans[span_index].1;
            push_line_preserving_marker(&mut result, &input[offset..end]);
            offset = end;
            continue;
        }
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

/// Byte ranges of `input` that decode to a registered secret at *any*
/// alignment — the base64 pass of [`mask_secrets_inner`].
///
/// [`secret_variants`] precomputes encodings that start at byte 0 of the
/// secret, so a one- or two-byte prefix shifts the base64 group alignment and
/// slips past them: `printf 'x%s' "$SECRET" | base64` leaks the value. This
/// pass decodes every base64-looking segment at the four possible group
/// alignments and reports the whole segment when the decoded bytes contain a
/// secret, which covers every prefix length (`len % 4` is all that changes the
/// byte grouping) and, because the whole segment is replaced, also hides the
/// prefix-dependent leading characters.
///
/// A label immediately before the token (`b64-shift1=eHBl…`) merges into the
/// same run, because `=` is a base64 byte itself; segments are split on `=`
/// so the token decodes on its own and only the token is replaced. A run that
/// mixes the two alphabet families is split at the characters the family it
/// is decoded under cannot contain, so `+eH5-fn4=` is still decoded as the
/// URL-safe token `eH5-fn4` instead of being rejected wholesale. `=` padding
/// that only trails a segment is masked with it, so a partial mask does not
/// leave the encoded length on display.
///
/// Base64 wrapped at a fixed column is reassembled across the line break
/// before decoding, so a secret whose encoding straddles the wrap is still
/// found; the replacement keeps one marker per physical line so line
/// checkpoints stay valid.
///
/// Secrets shorter than [`MIN_ELIGIBLE_SECRET_LEN`] are skipped here: they
/// already match most text by design, and scanning decodes makes false
/// positives worse. The exact and variant passes still cover them.
///
/// Work is linear in the length of the input: every segment is decoded at
/// most four times (once per group alignment) with a single alphabet family
/// chosen from the segment's own bytes, and each segment is tested as it is
/// produced, so only tainted spans are retained. No prefix cap is applied on
/// purpose — truncating the decode would let a shifted encoding hide behind
/// megabytes of base64 filler, which is exactly the leak this pass exists to
/// close.
///
/// Only base64 is covered here. Percent-encoded (`%78%70…`) and hex (`7870…`)
/// forms of a *shifted* payload are not, because the raw secret bytes do not
/// appear when those encodings are base64-decoded; the precomputed variants
/// cover percent/hex of the unshifted value.
fn base64_alignment_spans(input: &str, raw_secrets: &[String]) -> Vec<(usize, usize)> {
    let Some(scanner) = AlignmentScanner::new(raw_secrets) else {
        return Vec::new();
    };
    tainted_segment_ranges(input, &scanner, true)
}

/// Append one [`MASK_MARKER`] per physical line of `span`, keeping the
/// original newline bytes (including CR LF pairs) so the line structure stays
/// intact for line checkpoints captured before a retroactive masking pass.
fn push_line_preserving_marker(out: &mut String, span: &str) {
    out.push_str(MASK_MARKER);
    let mut chars = span.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\r' => {
                out.push('\r');
                if chars.peek() == Some(&'\n') {
                    out.push('\n');
                    chars.next();
                }
                out.push_str(MASK_MARKER);
            }
            '\n' => {
                out.push('\n');
                out.push_str(MASK_MARKER);
            }
            _ => {}
        }
    }
}

/// Secrets shorter than this are never searched inside base64 decodes: three
/// bytes — and even more so one or two — match so much unrelated text that
/// decoding would mostly produce false positives. Shorter secrets stay
/// covered by the exact and variant passes.
const MIN_ELIGIBLE_SECRET_LEN: usize = 4;

/// Unpadded base64 length of `byte_len` bytes.
fn encoded_len(byte_len: usize) -> usize {
    (4 * byte_len).div_ceil(3)
}

/// Padding-indifferent, trailing-bit-tolerant base64 engines.
///
/// The alignment pass decodes segments whose padding and final quantum are
/// unknown: a token can be truncated by the surrounding log text (adjacent
/// base64-alphabet bytes), so strict `NO_PAD` decoding rejects inputs whose
/// trailing bits or length validation fails and hides a detectable secret.
/// These engines accept padded and unpadded input alike and tolerate trailing
/// bits; see [`decode_lenient`] for the dangling-quantum case.
static STANDARD_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true),
);
static URL_SAFE_LENIENT: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(DecodePaddingMode::Indifferent)
        .with_decode_allow_trailing_bits(true),
);

/// Lenient base64 decode of one segment at one group alignment.
///
/// Padding is optional and the trailing bits of an unpadded final quantum are
/// accepted, so a valid token followed by adjacent base64-alphabet text still
/// decodes instead of being rejected wholesale. A dangling single-character
/// quantum cannot encode a byte, so the longest valid prefix is decoded.
fn decode_lenient(engine: &GeneralPurpose, input: &[u8]) -> Option<Vec<u8>> {
    use base64::engine::Engine as _;

    let mut len = input.len();
    if len % 4 == 1 {
        len -= 1;
    }
    engine.decode(&input[..len]).ok()
}

/// Whether `byte` can appear in a base64 run: the standard and URL-safe
/// alphabets plus `=` padding.
fn is_base64_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'-' | b'_' | b'=')
}

/// Shared base64-alignment scanner: the eligible needle set plus the bounds
/// derived from it. Both the batch pass and [`StreamingMasker`] decide with
/// [`AlignmentScanner::segment_tainted`], so they cannot drift apart.
struct AlignmentScanner {
    /// Eligible raw secret bytes, longest first.
    ///
    /// ASCII-lowercased: matching inside a decode is case-insensitive like
    /// the rest of the transform-aware API, so registering `AbCd` also covers
    /// a shifted encoding of `xabcd`.
    needles: Vec<Vec<u8>>,
    /// Shortest segment that can carry an eligible secret.
    min_len: usize,
    /// Longest base64 span that can carry one eligible secret, including the
    /// group-alignment overhead of a two-byte prefix.
    span_bound: usize,
}

impl AlignmentScanner {
    fn new(raw_secrets: &[String]) -> Option<Self> {
        let mut needles: Vec<Vec<u8>> = raw_secrets
            .iter()
            .filter(|secret| secret.len() >= MIN_ELIGIBLE_SECRET_LEN)
            .map(|secret| secret.as_bytes().to_ascii_lowercase())
            .collect();
        needles.sort_by_key(|needle| std::cmp::Reverse(needle.len()));
        needles.dedup();
        let longest = needles.first()?.len();
        Some(Self {
            needles,
            min_len: encoded_len(MIN_ELIGIBLE_SECRET_LEN),
            span_bound: encoded_len(longest + 2) + 2,
        })
    }

    /// Bytes of an undecided trailing run that [`StreamingMasker`] withholds:
    /// enough to see an eligible secret at any alignment and phase trim, plus
    /// one trailing partial quantum and a small margin.
    fn hold_bound(&self) -> usize {
        self.span_bound + 8
    }

    /// Whether `segment` decodes to bytes containing one of the secrets.
    ///
    /// The group alignment is unknown — a shifted encoding or preceding
    /// base64 text can start the token at any of the four phases — so the
    /// segment is decoded at each trim 0..=3. The decoded-bytes containment
    /// check is the deciding condition; the length floor only keeps very
    /// short runs (which cannot carry an eligible secret anyway) out of the
    /// decode loop.
    fn segment_tainted(&self, segment: &str) -> bool {
        let bytes = segment.as_bytes();
        if bytes.len() < self.min_len {
            return false;
        }
        // A segment cannot mix the two alphabet families and decode under
        // either engine; pick the one its special bytes belong to.
        // Alphanumeric-only segments decode identically under both.
        let engines: &[&GeneralPurpose] = match (
            bytes.iter().any(|byte| matches!(byte, b'+' | b'/')),
            bytes.iter().any(|byte| matches!(byte, b'-' | b'_')),
        ) {
            (true, true) => return false,
            (false, true) => &[&URL_SAFE_LENIENT],
            _ => &[&STANDARD_LENIENT],
        };
        for trim in 0..=3 {
            let Some(candidate) = bytes.get(trim..) else {
                break;
            };
            if candidate.len() < self.min_len {
                break;
            }
            if engines.iter().any(|engine| {
                decode_lenient(engine, candidate).is_some_and(|decoded| {
                    self.needles
                        .iter()
                        .any(|needle| contains_subslice(&decoded, needle))
                })
            }) {
                return true;
            }
        }
        false
    }
}

/// Whether `haystack` contains `needle`, ignoring ASCII case.
///
/// Decoded payloads can be large, so positions whose first byte cannot start a
/// match are skipped instead of comparing the full needle at every offset.
/// `needle` is ASCII-lowercased by [`AlignmentScanner::new`].
fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }
    let first = needle[0];
    let mut start = 0;
    while let Some(offset) = haystack[start..]
        .iter()
        .position(|byte| byte.eq_ignore_ascii_case(&first))
    {
        let index = start + offset;
        if haystack[index..].len() >= needle.len()
            && haystack[index..index + needle.len()].eq_ignore_ascii_case(needle)
        {
            return true;
        }
        start = index + 1;
    }
    false
}

/// Which base64 alphabet family a segment is decoded under. The two families
/// share the alphanumeric characters and differ only in their last two.
#[derive(Clone, Copy)]
enum AlphabetFamily {
    Standard,
    UrlSafe,
}

impl AlphabetFamily {
    /// Whether `byte` can carry payload in this family. `=` padding is a
    /// boundary for both families and is handled by the caller.
    fn contains(self, byte: u8) -> bool {
        match self {
            Self::Standard => byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'),
            Self::UrlSafe => byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'),
        }
    }
}

/// Length of the newline sequence at `position`: 0, 1 (`\n`) or 2 (`\r\n`).
fn newline_len_at(bytes: &[u8], position: usize) -> usize {
    if bytes.get(position) == Some(&b'\n') {
        1
    } else if bytes.get(position) == Some(&b'\r') && bytes.get(position + 1) == Some(&b'\n') {
        2
    } else {
        0
    }
}

/// Whether the base64 run `start..end` looks like one line of a base64 wrap
/// whose token continues on the next line: the whole line consists of base64
/// alphabet bytes without padding, its length is a whole number of
/// four-character groups, and the next line starts with a base64 byte.
/// Returns the newline length that ends the line.
fn wrapped_line_continues(bytes: &[u8], start: usize, end: usize) -> Option<usize> {
    const MIN_WRAPPED_LINE: usize = 8;

    let len = end.checked_sub(start)?;
    if len < MIN_WRAPPED_LINE || len % 4 != 0 {
        return None;
    }
    if bytes[start..end]
        .iter()
        .any(|byte| !is_base64_byte(*byte) || *byte == b'=')
    {
        return None;
    }
    let newline = newline_len_at(bytes, end);
    if newline == 0 {
        return None;
    }
    bytes
        .get(end + newline)
        .copied()
        .filter(|b| is_base64_byte(*b))?;
    Some(newline)
}

/// Visit every base64 segment of `text` that can carry an alignment-shifted
/// secret, as `(start, end, decode)`.
///
/// `start..end` is the byte range to replace when the segment is tainted
/// (including `=` padding that only trails it) and `decode` is the bytes to
/// decode: the segment without padding, or the segment with the line breaks of
/// a reassembled wrap removed.
///
/// `=` splits segments, so an embedded separator (`label=token`) does not fail
/// the decode. A run that mixes the two alphabet families is scanned once per
/// family, split at the characters that family cannot carry, so `+eH5-fn4=`
/// is decoded as the URL-safe token `eH5-fn4` instead of being rejected
/// wholesale. With `join_wraps`, a segment continuing on the next line as a
/// wrapped base64 line is reassembled first, so the decode sees the token's
/// bytes in order; a wrap continues the run's payload segment — after the last
/// `=` separator, without trailing padding — so a label is not mistaken for
/// part of the wrapped line.
///
/// Segments are offered as they are produced and the caller decides on each
/// one immediately: a body of millions of short segments never accumulates
/// candidates, only the spans the caller keeps.
///
/// Runs are discovered as they are walked, never collected: the input is a
/// log body an attacker controls, and a body of alternating one-byte runs and
/// delimiters would otherwise cost a tuple per run for the run list and again
/// for the wrap list — a multiple of the body size, held while the caller's
/// masker lock is taken.
fn for_each_candidate(text: &str, join_wraps: bool, visit: &mut impl FnMut(usize, usize, &str)) {
    let bytes = text.as_bytes();
    for (run_start, run_end) in Base64Runs::new(bytes) {
        let run = &bytes[run_start..run_end];
        let mixed = run.iter().any(|byte| matches!(byte, b'+' | b'/'))
            && run.iter().any(|byte| matches!(byte, b'-' | b'_'));
        if mixed {
            for family in [AlphabetFamily::Standard, AlphabetFamily::UrlSafe] {
                visit_run_segments(text, bytes, run_start, run_end, Some(family), visit);
            }
        } else {
            visit_run_segments(text, bytes, run_start, run_end, None, visit);
        }
    }

    if join_wraps {
        visit_wrap_chains(text, bytes, visit);
    }
}

/// The maximal base64 runs of a byte slice, in order, without collecting them.
struct Base64Runs<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl<'a> Base64Runs<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, index: 0 }
    }
}

impl Iterator for Base64Runs<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        let bytes = self.bytes;
        while self.index < bytes.len() && !is_base64_byte(bytes[self.index]) {
            self.index += 1;
        }
        if self.index >= bytes.len() {
            return None;
        }
        let start = self.index;
        while self.index < bytes.len() && is_base64_byte(bytes[self.index]) {
            self.index += 1;
        }
        Some((start, self.index))
    }
}

/// The segment of a run that a wrap continues: after the last `=` separator,
/// with trailing `=` padding removed. A label before the token (`token=…`) is
/// not part of the wrapped line, and padding ends it. Empty for an all-padding
/// run.
fn payload_segment(bytes: &[u8], run_start: usize, run_end: usize) -> (usize, usize) {
    let end = bytes[run_start..run_end]
        .iter()
        .rposition(|byte| *byte != b'=')
        .map(|offset| run_start + offset + 1)
        .unwrap_or(run_start);
    let start = bytes[run_start..end]
        .iter()
        .rposition(|byte| *byte == b'=')
        .map(|offset| run_start + offset + 1)
        .unwrap_or(run_start);
    (start, end)
}

/// Visit reassembled wrap chains: a wrapped token continues on the line right
/// after the line that ends it, so each run only needs to be compared with the
/// one chain still open and the previous run's payload. Nothing per run is
/// retained, so run density cannot amplify memory use.
fn visit_wrap_chains(text: &str, bytes: &[u8], visit: &mut impl FnMut(usize, usize, &str)) {
    // (candidate start, decode buffer, chain end, end of the run it ends in).
    let mut chain: Option<(usize, String, usize, usize)> = None;
    let mut previous: Option<(usize, usize)> = None;
    for (run_start, run_end) in Base64Runs::new(bytes) {
        let (segment_start, segment_end) = payload_segment(bytes, run_start, run_end);
        let wrap_line = segment_start != segment_end
            && wrapped_line_continues(bytes, segment_start, segment_end).is_some();
        let mut close = false;
        match chain.as_mut() {
            Some((_, decode, chain_end, chain_run_end)) => {
                // The continuation must begin on the line right after the wrap.
                let newline = newline_len_at(bytes, *chain_end);
                if newline != 0
                    && segment_end > segment_start
                    && segment_start == *chain_end + newline
                {
                    decode.push_str(&text[segment_start..segment_end]);
                    *chain_end = segment_end;
                    *chain_run_end = run_end;
                    close = !wrap_line;
                } else {
                    close = true;
                }
            }
            None => {
                // Only start at the head of a wrap chain, so a continuation
                // line does not produce a second, nested candidate for the
                // same wrap.
                let previous_is_wrap = previous.is_some_and(|(start, end)| {
                    start != end && wrapped_line_continues(bytes, start, end).is_some()
                });
                if wrap_line && !previous_is_wrap {
                    chain = Some((
                        segment_start,
                        String::from(&text[segment_start..segment_end]),
                        segment_end,
                        run_end,
                    ));
                }
            }
        }
        if close {
            let (chain_start, decode, chain_end, chain_run_end) =
                chain.take().expect("closed chain is open");
            visit(
                chain_start,
                trailing_padding_end(bytes, chain_end, chain_run_end),
                &decode,
            );
        }
        previous = Some((segment_start, segment_end));
    }
    if let Some((chain_start, decode, chain_end, chain_run_end)) = chain {
        visit(
            chain_start,
            trailing_padding_end(bytes, chain_end, chain_run_end),
            &decode,
        );
    }
}

/// Visit the `=`-separated segments of one run, additionally splitting at the
/// bytes `family` (when given) cannot carry.
fn visit_run_segments(
    text: &str,
    bytes: &[u8],
    run_start: usize,
    run_end: usize,
    family: Option<AlphabetFamily>,
    visit: &mut impl FnMut(usize, usize, &str),
) {
    let mut segment_start = run_start;
    for position in run_start..run_end {
        let byte = bytes[position];
        let boundary = byte == b'=' || family.is_some_and(|family| !family.contains(byte));
        if boundary {
            visit_segment(text, bytes, run_end, segment_start, position, visit);
            segment_start = position + 1;
        }
    }
    visit_segment(text, bytes, run_end, segment_start, run_end, visit);
}

/// Visit one non-empty segment `start..end`.
///
/// The masked range is extended over `=` padding that only trails the segment,
/// matching the unshifted variants (a padded base64 variant is replaced whole):
/// the padding is part of the token's length, not payload, so a partial mask
/// must not leave it behind to advertise how many bytes the token encodes.
fn visit_segment(
    text: &str,
    bytes: &[u8],
    run_end: usize,
    start: usize,
    end: usize,
    visit: &mut impl FnMut(usize, usize, &str),
) {
    if end <= start {
        return;
    }
    visit(
        start,
        trailing_padding_end(bytes, end, run_end),
        &text[start..end],
    );
}

/// End of the `=` padding that immediately follows `end` inside a run: `end`
/// when payload follows (`a=b`, `label=token`), the run's end when only padding
/// is left (`token==`).
fn trailing_padding_end(bytes: &[u8], end: usize, run_end: usize) -> usize {
    if end < run_end && bytes[end..run_end].iter().all(|byte| *byte == b'=') {
        run_end
    } else {
        end
    }
}

/// Byte ranges of `text` that must be masked because their decoded base64
/// bytes contain a secret, sorted and merged so nested spans (a wrapped token
/// contains its own continuation line) collapse into one span.
///
/// A segment that still runs to the end of `text` is decided on the bytes
/// available: a secret found in its decoded prefix is a stable match, so the
/// segment only grows more masked bytes, never fewer.
fn tainted_segment_ranges(
    text: &str,
    scanner: &AlignmentScanner,
    join_wraps: bool,
) -> Vec<(usize, usize)> {
    // Each segment is decoded and dropped as it is produced, so only the
    // tainted spans are retained.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for_each_candidate(text, join_wraps, &mut |start, end, decode| {
        if scanner.segment_tainted(decode) {
            ranges.push((start, end));
        }
    });
    ranges.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some((_, last_end)) if start < *last_end => *last_end = (*last_end).max(end),
            _ => merged.push((start, end)),
        }
    }
    merged
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

/// Mask values for the password in a URL's userinfo (`user:password@`).
///
/// For log paths that print a URL as given, such as a `docker create`
/// command line carrying `-e HTTPS_PROXY=<url>`. The official runner splits
/// a proxy URL's userinfo at the first `:` and registers the password with
/// its secret masker (`RunnerWebProxy`, `HostContext`), so those lines read
/// `user:***@host`. Returns the password as written in the URL and, when it
/// differs, its percent-decoded form; empty when the URL has no password.
/// The authority is found the same way as in [`strip_url_userinfo`].
pub fn url_password_masks(url: &str) -> Vec<String> {
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &url[after_scheme..];
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    let Some((_, password)) = authority
        .rfind('@')
        .and_then(|at| authority[..at].split_once(':'))
    else {
        return Vec::new();
    };
    if password.is_empty() {
        return Vec::new();
    }
    let mut masks = vec![password.to_owned()];
    if let Some(decoded) = percent_decode(password)
        && !decoded.is_empty()
        && decoded != password
    {
        masks.push(decoded);
    }
    masks
}

/// Decode `%XX` escapes; any other byte, including a malformed escape, is
/// kept as is. `None` when the decoded bytes are not UTF-8.
fn percent_decode(input: &str) -> Option<String> {
    let hex = |byte: Option<&u8>| byte.and_then(|b| (*b as char).to_digit(16));
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let (Some(high), Some(low)) = (hex(bytes.get(i + 1)), hex(bytes.get(i + 2)))
        {
            out.push((high * 16 + low) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
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
/// plus [`secret_variants`], ASCII case-insensitive, and the same
/// base64-alignment scan (an undecided trailing base64 run is withheld up to
/// [`AlignmentScanner::hold_bound`] bytes, then flushed, so non-secret text
/// is never held indefinitely).
///
/// Line breaks inside one window are reassembled like the batch pass, so a
/// wrapped token delivered in a single push — the whole body of one
/// `append_log` request, say — is redacted. A wrap split across two pushes is
/// only covered up to the withheld tail's bound; longer wraps are decided by
/// the caller's batch masking (the runner masks each write before sending),
/// which is the defense-in-depth layer this streaming strength backs up.
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
    /// Base64-alignment scanner; `None` when no eligible secret is
    /// registered, so no alignment-shifted encoding can exist.
    scanner: Option<AlignmentScanner>,
}

impl StreamingMasker {
    /// Build a masker from raw secret values; variants are expanded here, so
    /// callers pass each secret once.
    pub fn new<'a, I>(secrets: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut expanded: Vec<String> = Vec::new();
        let mut raw: Vec<String> = Vec::new();
        for secret in secrets.into_iter().filter(|s| !s.is_empty()) {
            if !raw.iter().any(|existing| existing == secret) {
                raw.push(secret.to_owned());
            }
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
            scanner: AlignmentScanner::new(&raw),
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
    /// secret are emitted. An unresolved trailing base64 run is withheld as
    /// a whole — up to [`AlignmentScanner::hold_bound`] bytes — because more
    /// base64 bytes can still complete an alignment-shifted encoding of a
    /// secret; a run longer than that bound is flushed from its head, after
    /// the withheld window (which contains the flushed bytes) has been
    /// decoded and found clean. A decided span is emitted even inside that
    /// region: a complete match or a tainted token does not depend on later
    /// bytes, so holding it back would only delay masking. The withheld tail
    /// is returned by a later [`push`](Self::push) or by
    /// [`finish`](Self::finish).
    pub fn push(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut window = std::mem::take(&mut self.carry);
        window.extend_from_slice(chunk);
        // Lossy like the rest of this module; the carry stays valid UTF-8.
        let text = String::from_utf8_lossy(&window).into_owned();
        let ranges = self.alignment_ranges(&text);
        // Never emit a byte at or after `hold_from`: it belongs to the
        // unresolved trailing base64 run and can still become part of an
        // alignment-shifted token. Bytes at or after `soft_hold_from` are
        // withheld only when they can start a variant match.
        let hold_from = self.trailing_run_hold(&text);
        let soft_hold_from = text.len().saturating_sub(self.max_len.saturating_sub(1));
        let (out, emitted) = mask_window(&text, &self.secrets, &ranges, hold_from, soft_hold_from);
        self.carry = text.as_bytes()[emitted..].to_vec();
        out.into_bytes()
    }

    /// Mask and return the withheld tail. The stream ended, so every segment
    /// is complete: a partial secret cannot be completed and is emitted
    /// as-is, while an alignment-shifted token wholly inside the tail is
    /// redacted.
    pub fn finish(&mut self) -> Vec<u8> {
        let carry = std::mem::take(&mut self.carry);
        let text = String::from_utf8_lossy(&carry);
        let ranges = self.alignment_ranges(&text);
        let (out, _) = mask_window(&text, &self.secrets, &ranges, usize::MAX, usize::MAX);
        out.into_bytes()
    }

    /// Tainted byte ranges of one window. A segment that can still grow (it
    /// runs to the end of the window) is decided on the bytes available: a
    /// match found in the decoded prefix is stable, so more bytes only ever
    /// extend the masked span.
    ///
    /// Wrapped base64 is reassembled here like the batch pass: the server
    /// persists `append_log` bodies straight from this masker, and a
    /// compatible client may send a whole wrapped token in one body, so the
    /// lines of a wrap must be joined before decoding. A wrap split across
    /// two pushes is only bounded by [`AlignmentScanner::hold_bound`], so it
    /// stays the caller's batch masking that decides it.
    fn alignment_ranges(&self, text: &str) -> Vec<(usize, usize)> {
        match &self.scanner {
            Some(scanner) => tainted_segment_ranges(text, scanner, true),
            None => Vec::new(),
        }
    }

    /// Index at which the unresolved trailing base64 run must be withheld:
    /// the start of the run's last [`AlignmentScanner::hold_bound`] bytes,
    /// or the whole run when it is shorter.
    fn trailing_run_hold(&self, text: &str) -> usize {
        let Some(scanner) = &self.scanner else {
            return text.len();
        };
        let bytes = text.as_bytes();
        let mut run_start = bytes.len();
        while run_start > 0 && is_base64_byte(bytes[run_start - 1]) {
            run_start -= 1;
        }
        run_start + (bytes.len() - run_start).saturating_sub(scanner.hold_bound())
    }
}

/// Emit `text` with secret matches and pre-computed alignment ranges redacted.
///
/// A byte at or after `hold_from` belongs to the unresolved trailing base64 run
/// and may still become part of a token decided by a later chunk, so it is
/// written speculatively: if no decided span follows it in this window the
/// speculative tail is cut off and withheld. A decided span — a complete
/// match or a tainted base64 range — does not depend on later bytes, so
/// everything before it is emitted (a token must never be left half-masked
/// just because it sits in the hold region), and multi-line spans keep one
/// marker per physical line. A byte at or after `soft_hold_from` that is the
/// head of a possible secret is withheld whatever follows it; `usize::MAX`
/// disables either bound.
///
/// Returns the masked output and the index where the withheld tail starts.
fn mask_window(
    text: &str,
    secrets: &[String],
    ranges: &[(usize, usize)],
    hold_from: usize,
    soft_hold_from: usize,
) -> (String, usize) {
    let mut out = String::with_capacity(text.len());
    let mut pos = 0;
    let mut range = 0;
    // (text offset, output length) where the speculative tail starts.
    let mut speculative: Option<(usize, usize)> = None;
    while pos < text.len() {
        while range < ranges.len() && ranges[range].1 <= pos {
            range += 1;
        }
        let range_end = (range < ranges.len() && ranges[range].0 <= pos).then(|| ranges[range].1);
        let matched = match range_end {
            Some(_) => None,
            None => match_secret_at(&text[pos..], secrets, true),
        };
        if let Some(end) = range_end.or_else(|| matched.map(|secret| pos + secret.len())) {
            // A tainted base64 segment is opaque as a whole, whether or not
            // an earlier match redacted its head.
            speculative = None;
            push_line_preserving_marker(&mut out, &text[pos..end]);
            pos = end;
            continue;
        }
        let remaining = &text[pos..];
        if pos >= soft_hold_from && is_secret_prefix(remaining, secrets) {
            break;
        }
        if speculative.is_none() && pos >= hold_from {
            speculative = Some((pos, out.len()));
        }
        let ch = remaining
            .chars()
            .next()
            .expect("pos always points at a character boundary");
        out.push(ch);
        pos += ch.len_utf8();
    }
    match speculative {
        Some((text_offset, out_len)) => {
            out.truncate(out_len);
            (out, text_offset)
        }
        None => (out, pos),
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

    #[test]
    fn url_password_masks_returns_raw_and_decoded_password() {
        assert_eq!(
            url_password_masks("http://proxyuser:s3cr3t-pw@proxy.corp:8080/"),
            ["s3cr3t-pw"]
        );
        // As written in the URL and percent-decoded: a tool may echo either.
        assert_eq!(
            url_password_masks("http://user:p%40ss%3Aw0rd@proxy.example:8080"),
            ["p%40ss%3Aw0rd", "p@ss:w0rd"]
        );
        // The password runs to the last `@` and may itself contain `:`.
        assert_eq!(url_password_masks("http://u:a:b@c@proxy:1"), ["a:b@c"]);
        // No scheme: the official runner prepends `http://`, same authority.
        assert_eq!(url_password_masks("user:pw@proxy:3128"), ["pw"]);
        // No password: nothing to mask, the username stays visible.
        assert!(url_password_masks("http://user@proxy.example:8080").is_empty());
        assert!(url_password_masks("http://user:@proxy.example:8080").is_empty());
        assert!(url_password_masks("http://proxy.example:8080").is_empty());
        // An `@` past the authority is not userinfo.
        assert!(url_password_masks("http://proxy.example/a:b@c").is_empty());
        assert!(url_password_masks("").is_empty());
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

    /// `printf 'x%s' "$SECRET" | base64` shifts the base64 group alignment;
    /// the precomputed variants cannot match it, so the decoder pass must mask
    /// the run (including the prefix-dependent leading characters).
    #[test]
    fn alignment_shifted_base64_is_masked() {
        let secret = "pentest-mask-9f3k2";
        for token in [
            // base64("x" + secret)  — one-byte prefix
            "eHBlbnRlc3QtbWFzay05ZjNrMg==",
            // base64("xx" + secret) — two-byte prefix
            "eHhwZW50ZXN0LW1hc2stOWYzazI=",
        ] {
            let masked =
                mask_secrets_transform_aware(&format!("step output: {token}"), [secret], &[]);
            assert!(!masked.contains(token), "leaked {token}: {masked}");
            assert!(masked.contains(MASK_MARKER), "no marker: {masked}");
        }
    }

    #[test]
    fn unshifted_base64_is_still_masked() {
        let secret = "pentest-mask-9f3k2";
        let token = "cGVudGVzdC1tYXNrLTlmM2sy";
        let masked = mask_secrets_transform_aware(token, [secret], &[]);
        assert!(!masked.contains(token), "leaked: {masked}");
    }

    #[test]
    fn unrelated_base64_runs_survive() {
        let secret = "pentest-mask-9f3k2";
        // base64("hello world") — long enough to scan, does not contain it.
        let token = "aGVsbG8gd29ybGQ=";
        assert_eq!(mask_secrets_transform_aware(token, [secret], &[]), token);
    }

    /// Real runner log shape: the label merges with the token into one run
    /// (`=` and `-` are base64 bytes), so the scanner must split on `=`
    /// instead of failing on the embedded separator.
    #[test]
    fn labeled_alignment_shifted_base64_is_masked() {
        let secret = "pentest-mask-9f3k2";
        for line in [
            "b64-shift1=eHBlbnRlc3QtbWFzay05ZjNrMg==",
            "2026-10-18T04:51:46.4825600Z b64-shift2=eHhwZW50ZXN0LW1hc2stOWYzazI=",
        ] {
            let masked = mask_secrets_transform_aware(line, [secret], &[]);
            assert!(
                !masked.contains("eHBlbnRlc3QtbWFzay05ZjNrMg=="),
                "leaked shift1: {masked}"
            );
            assert!(
                !masked.contains("eHhwZW50ZXN0LW1hc2stOWYzazI="),
                "leaked shift2: {masked}"
            );
        }
    }

    #[test]
    fn alignment_masking_is_idempotent() {
        let secret = "pentest-mask-9f3k2";
        let once = mask_secrets_transform_aware("out eHBlbnRlc3QtbWFzay05ZjNrMg==", [secret], &[]);
        let twice = mask_secrets_transform_aware(&once, [secret], &[]);
        assert_eq!(once, twice);
    }

    /// A secret encoded after a one-byte prefix and wrapped at column 76: the
    /// token straddles the line break, so the lines must be reassembled before
    /// decoding, and the replacement must keep one marker per physical line
    /// (line checkpoints captured before a retroactive pass stay valid).
    #[test]
    fn wrapped_base64_token_is_masked_and_keeps_line_breaks() {
        let secret = "pentest-mask-9f3k2";
        let input = format!(
            "{}\n{}",
            "eHh4".repeat(18) + "eHBl",
            "bnRlc3QtbWFzay05ZjNrMg=="
        );
        let masked = mask_secrets_transform_aware(&input, [secret], &[]);
        assert_eq!(masked, "***\n***", "leaked wrapped token: {masked}");
        assert_eq!(
            masked.bytes().filter(|byte| *byte == b'\n').count(),
            input.bytes().filter(|byte| *byte == b'\n').count()
        );
        // Preserve-lines mode must emit the same one-marker-per-line shape.
        let preserved = mask_secrets_preserving_lines_transform_aware(&input, [secret], &[]);
        assert_eq!(preserved, "***\n***");
    }

    /// CR LF pairs inside a wrapped token stay adjacent, with one marker per
    /// physical line, so line checkpoints captured before a retroactive pass
    /// stay valid.
    #[test]
    fn wrapped_base64_token_with_crlf_keeps_line_breaks() {
        let secret = "pentest-mask-9f3k2";
        let input = format!(
            "{}\r\n{}",
            "eHh4".repeat(18) + "eHBl",
            "bnRlc3QtbWFzay05ZjNrMg=="
        );
        let masked = mask_secrets_preserving_lines_transform_aware(&input, [secret], &[]);
        assert_eq!(masked, "***\r\n***");
    }

    /// The server live-feed path masks one line at a time and cannot see a
    /// token split by a line break; the line-continuation alone therefore
    /// carries only the secret's tail. Pins down why the batch pass has to
    /// reassemble wrapped base64 (and why the live-feed path cannot).
    #[test]
    fn wrapped_base64_continuation_line_alone_is_not_a_secret() {
        let secret = "pentest-mask-9f3k2";
        let line = "bnRlc3QtbWFzay05ZjNrMg==";
        assert_eq!(mask_secrets_transform_aware(line, [secret], &[]), line);
    }

    /// The run-length floor is derived from the four-byte eligibility
    /// threshold: `base64("xabcd")` is seven unpadded characters, and a fixed
    /// floor of 16 dropped the complete encoding.
    #[test]
    fn short_secret_shifted_base64_is_masked() {
        let masked = mask_secrets_transform_aware("eGFiY2Q=", ["abcd"], &[]);
        assert_eq!(masked, "***");
    }

    /// Adjacent base64-alphabet text after a valid unpadded token used to make
    /// the strict decode fail on length/trailing-bit validation, hiding an
    /// otherwise detectable token.
    #[test]
    fn shifted_token_followed_by_base64_alphabet_is_masked() {
        let secret = "pentest-mask-9f3k2";
        let masked = mask_secrets_transform_aware("eHBlbnRlc3QtbWFzay05ZjNrMgB", [secret], &[]);
        assert_eq!(masked, "***");
    }

    /// Registering a four-byte secret must not redact ordinary identifiers:
    /// the decoded-containment check decides, and none of these decodes to the
    /// secret's bytes at any group alignment.
    #[test]
    fn ordinary_identifiers_survive_short_needle_scan() {
        let secret = "abcd";
        for input in [
            "3f2a91c0de5b78461a2c93f0d18e4b76a5c2093e",
            "f47ac10b-58cc-4372-a567-0e02b2c3d479",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc123",
            "/var/lib/my_app/data_file-1",
        ] {
            assert_eq!(
                mask_secrets_transform_aware(input, [secret], &[]),
                input,
                "over-masked {input}"
            );
        }
    }

    /// A log body an attacker controls can be dense in tiny base64 runs
    /// (alternating runs, both alphabet families, padding, delimiters). The
    /// scanner discovers runs as it walks them and keeps nothing per run, so
    /// run density cannot amplify its memory beyond the tainted spans; the
    /// body comes back byte-for-byte because no run is long enough to decode
    /// to the secret.
    #[test]
    fn adversarial_run_density_is_returned_unchanged() {
        let secret = "pentest-mask-9f3k2";
        // Roughly 300k one- and two-byte runs in a 4 MiB body.
        let mut input = String::with_capacity(4 * 1024 * 1024 + 16);
        while input.len() < 4 * 1024 * 1024 {
            input.push_str("A+b B_c A d=!aAdD=");
        }
        assert_eq!(mask_secrets_transform_aware(&input, [secret], &[]), input);
    }

    /// A multi-megabyte base64-alphabet run must complete and come back
    /// unchanged when it holds no secret. The pass is linear (each segment is
    /// decoded at most four times, once per group alignment), so the bound is
    /// documented on `mask_base64_alignment_runs` instead of asserted here.
    #[test]
    fn huge_base64_alphabet_line_is_returned_unchanged() {
        let input = "A".repeat(4 * 1024 * 1024);
        let masked = mask_secrets_transform_aware(&input, ["pentest-mask-9f3k2"], &[]);
        assert_eq!(masked.len(), input.len());
        assert_eq!(masked, input);
    }

    /// Server-side log persistence writes [`StreamingMasker`] output directly:
    /// a single push must already redact an alignment-shifted token.
    #[test]
    fn streaming_masker_masks_alignment_shifted_base64_in_one_push() {
        let secret = "pentest-mask-9f3k2";
        let token = "eHBlbnRlc3QtbWFzay05ZjNrMg==";
        let mut masker = StreamingMasker::new([secret].iter().copied());
        let out = stream_chunks(&mut masker, &[format!("step output: {token}\n").as_bytes()]);
        assert_eq!(String::from_utf8(out).unwrap(), "step output: ***\n");
    }

    /// Every split position of a shifted token across two chunks must be
    /// redacted: the undecided trailing base64 bytes are withheld until the
    /// token is decided, instead of being matched chunk by chunk.
    #[test]
    fn streaming_masker_masks_alignment_shifted_base64_across_every_split() {
        let secret = "pentest-mask-9f3k2";
        let token = "eHBlbnRlc3QtbWFzay05ZjNrMg==";
        for split in 0..=token.len() {
            let (head, tail) = token.split_at(split);
            let mut masker = StreamingMasker::new([secret].iter().copied());
            let out = stream_chunks(&mut masker, &[head.as_bytes(), tail.as_bytes()]);
            let text = String::from_utf8(out).unwrap();
            // The padding is masked with the token body; when the split falls
            // inside the padding itself the later chunk carries it, and a
            // chunk of nothing but padding is not a secret.
            assert_eq!(
                text.trim_end_matches('='),
                "***",
                "split at {split}: {text}"
            );
        }
    }

    /// The withheld tail is bounded: a long untainted base64 run is flushed
    /// (never held indefinitely) and comes back byte-for-byte intact.
    #[test]
    fn streaming_masker_flushes_long_untainted_base64_runs() {
        let chunk = "A".repeat(4096);
        let mut masker = StreamingMasker::new(["pentest-mask-9f3k2"].iter().copied());
        let first = masker.push(chunk.as_bytes());
        assert!(
            first.len() * 2 > chunk.len(),
            "long untainted run must be flushed, got {} of {} bytes",
            first.len(),
            chunk.len()
        );
        let rest = masker.finish();
        assert_eq!(String::from_utf8([first, rest].concat()).unwrap(), chunk);
    }

    /// A run can hold a character of the other alphabet family from adjacent
    /// log text (`+` here, in front of a URL-safe token). Rejecting the run
    /// because it mixes families leaves the shifted token decodable, so the
    /// run is scanned once per family instead.
    #[test]
    fn mixed_alphabet_run_is_decoded_per_family() {
        // base64url("x~~~~") — the secret is `~~~~`. The `+` in front is a
        // standard-alphabet character that cannot appear in that token.
        let masked = mask_secrets_transform_aware("+eH5-fn4=", ["~~~~"], &[]);
        assert_eq!(masked, "+***");
    }

    /// The transform-aware API matches ASCII case-insensitively, so a shifted
    /// encoding of the case-folded secret must be masked like the raw value.
    #[test]
    fn shifted_encoding_of_a_case_folded_secret_is_masked() {
        use base64::engine::Engine as _;
        // Registering `AbCd` masks raw `abcd`; base64("xabcd") decodes to the
        // same case-folded bytes.
        let token = base64::engine::general_purpose::STANDARD.encode("xabcd");
        assert_eq!(token, "eGFiY2Q=");
        assert_eq!(mask_secrets_transform_aware(&token, ["AbCd"], &[]), "***");
    }

    /// A variant match inside a shifted token must not split it: the marker is
    /// not a base64 byte, so the fragments would stop decoding and the rest of
    /// the encoded secret would stay in the log. The token spans are decided
    /// on the original input, before any replacement.
    #[test]
    fn variant_match_inside_a_shifted_token_does_not_split_it() {
        let token = "eHBlbnRlc3QtbWFzay05ZjNrMg==";
        // The second secret is a literal substring of the token.
        let masked =
            mask_secrets_transform_aware(token, ["pentest-mask-9f3k2", "bnRlc3QtbWFz"], &[]);
        assert_eq!(masked, "***", "fragment survived: {masked}");
    }

    /// A label before a wrapped token must not stop the reassembly: the first
    /// line is `token=` plus the wrapped bytes, so the `=` is a segment
    /// boundary inside the run rather than part of the wrapped line.
    #[test]
    fn labeled_wrapped_base64_token_is_masked() {
        let secret = "pentest-mask-9f3k2";
        let input = format!(
            "token={}\n{}",
            "eHh4".repeat(18) + "eHBl",
            "bnRlc3QtbWFzay05ZjNrMg=="
        );
        let masked = mask_secrets_transform_aware(&input, [secret], &[]);
        assert!(
            !masked.contains("bnRlc3QtbWFzay05ZjNrMg"),
            "leaked wrapped token: {masked}"
        );
        assert!(masked.starts_with("token="), "label must survive: {masked}");
    }

    /// `append_log` persists `push` output straight away, so a body that ends
    /// in a complete secret must be redacted on that push rather than held for
    /// `finish` (the whole body is one base64 run: `=` and `-` are base64
    /// bytes, so the trailing-run hold covers it).
    #[test]
    fn streaming_masker_masks_a_secret_that_ends_the_chunk() {
        let mut masker = StreamingMasker::new(["super-secret"].iter().copied());
        let out = masker.push(b"token=super-secret");
        assert_eq!(String::from_utf8(out).unwrap(), "token=***");
    }

    /// A compatible client can send a whole wrapped token in one body, so the
    /// streaming masker must reassemble the wrap like the batch pass; the
    /// replacement keeps one marker per physical line.
    #[test]
    fn streaming_masker_masks_a_wrapped_token_in_one_push() {
        let secret = "pentest-mask-9f3k2";
        let input = format!(
            "{}\n{}",
            "eHh4".repeat(18) + "eHBl",
            "bnRlc3QtbWFzay05ZjNrMg=="
        );
        let mut masker = StreamingMasker::new([secret].iter().copied());
        let out = stream_chunks(&mut masker, &[input.as_bytes()]);
        let text = String::from_utf8(out).unwrap();
        assert!(
            !text.contains("bnRlc3QtbWFzay05ZjNrMg"),
            "leaked wrapped token: {text}"
        );
        assert_eq!(text, "***\n***");
    }
}
