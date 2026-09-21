//! Commit-identity helpers shared by the control plane and the runner.
//!
//! A ref is *pinned* when it names one immutable commit (a full 40-character
//! commit SHA) rather than a mutable branch, tag, or short SHA. SHA pinning is
//! the only reason a resolved-to-commit ref is safe to cache and to download
//! by: a mutable ref can be retargeted between resolution and download.
//!
//! This lives in the protocol crate because every layer that resolves a ref
//! needs the same predicate: the server (ref resolution, push, OIDC claims,
//! scheduling), the runner (action downloads), and the CLI.

/// The all-zero commit SHA. GitHub sends it for "no commit" positions, such as
/// the `before` field of a branch-delete push, so callers that require a real
/// committed HEAD must reject it explicitly.
pub const ZERO_SHA: &str = "0000000000000000000000000000000000000000";

/// Whether `value` is a full 40-character commit SHA, as opposed to a mutable
/// branch/tag or a short SHA.
///
/// Case-insensitive: Git emits lowercase, but the value may have been produced
/// by a caller that uppercased it.
pub fn is_commit_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// [`is_commit_sha`] plus the zero-SHA rejection: "this is a real commit".
pub fn is_commit_sha_not_zero(value: &str) -> bool {
    is_commit_sha(value) && value != ZERO_SHA
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_full_commit_shas() {
        assert!(is_commit_sha("5239f8c5f6f71c92711e68dbe821c8e4ec1e3541"));
        assert!(is_commit_sha("A1B2C3D4E5F60718293A4B5C6D7E8F9012345678"));
    }

    #[test]
    fn rejects_mutable_and_short_refs() {
        for value in ["v4", "main", "a5ac7e5", "not-a-sha", "", "  "] {
            assert!(!is_commit_sha(value), "should reject {value:?}");
        }
        // 39 or 41 hex characters are not commit SHAs.
        assert!(!is_commit_sha("5239f8c5f6f71c92711e68dbe821c8e4ec1e354"));
        assert!(!is_commit_sha("5239f8c5f6f71c92711e68dbe821c8e4ec1e35411"));
    }

    #[test]
    fn rejects_non_hex_of_commit_length() {
        assert!(!is_commit_sha(&"z".repeat(40)));
        assert!(!is_commit_sha("5239f8c5f6f71c92711e68dbe821c8e4ec1e354g"));
    }

    #[test]
    fn zero_sha_is_a_sha_but_not_a_commit() {
        assert!(is_commit_sha(ZERO_SHA));
        assert!(!is_commit_sha_not_zero(ZERO_SHA));
        assert!(is_commit_sha_not_zero(
            "5239f8c5f6f71c92711e68dbe821c8e4ec1e3541"
        ));
    }
}
