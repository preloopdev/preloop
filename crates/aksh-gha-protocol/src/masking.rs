//! Shared secret-masking logic.
//!
//! All secret redaction across the codebase (runner durable logs, live logs,
//! server log persistence, DAP debugger output) must use this canonical
//! implementation to guarantee consistent, longest-first replacement order.

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
    let mut sorted: Vec<&str> = secrets
        .into_iter()
        .filter(|s| !s.is_empty())
        .filter(|s| !exclude.contains(s))
        .collect();
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));
    let mut result = input.to_owned();
    for secret in sorted {
        result = result.replace(secret, MASK_MARKER);
    }
    result
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
    fn no_secrets_returns_input_unchanged() {
        let input = "nothing to mask here";
        assert_eq!(mask_secrets(input, std::iter::empty(), &[]), input);
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
}
