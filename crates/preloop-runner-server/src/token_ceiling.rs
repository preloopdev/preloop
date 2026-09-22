//! `GITHUB_TOKEN` permissions ceiling: the operator's hard cap on what a
//! workflow's token may carry.
//!
//! Effective permissions per scope are the minimum of the workflow-declared
//! set, this ceiling, the fork-restricted profile (which arrives already
//! downgraded in the token request, so it stays the floor for fork PR jobs),
//! and the GitHub App installation's grants (clamped at mint time).
//!
//! The ceiling only ever *removes* authority: it is applied to the permission
//! map requested from GitHub before the installation token is minted, and the
//! clamped set is what the wire `system.github.token.permissions` variable
//! reports. The PAT fallback is the operator's own credential and ignores
//! `permissions:` by design, so the ceiling does not apply to it.

use std::collections::BTreeMap;

use crate::config::TokenPermissionsCeiling;

/// Rank of a workflow-declared level string. Unknown levels rank above every
/// known level so they clamp down to the ceiling instead of slipping past it.
fn declared_rank(level: &str) -> u8 {
    match level.to_ascii_lowercase().as_str() {
        "none" => 0,
        "read" => 1,
        "write" => 2,
        "admin" => 3,
        _ => u8::MAX,
    }
}

/// One scope the ceiling reduced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CeilingClampDetail {
    pub scope: String,
    /// What the workflow asked for.
    pub requested: String,
    /// What the ceiling granted.
    pub granted: String,
}

/// Result of applying the ceiling to a token request's permission map.
#[derive(Debug, Clone)]
pub struct CeilingOutcome {
    /// The permission map to request from GitHub: every scope capped.
    pub permissions: BTreeMap<String, String>,
    /// Scopes the ceiling reduced (empty when nothing was clamped).
    pub clamped: Vec<CeilingClampDetail>,
}

/// Cap `requested` at the operator ceiling. `None` (no ceiling configured)
/// returns the request unchanged with no clamps.
pub fn apply_ceiling(
    ceiling: Option<&TokenPermissionsCeiling>,
    requested: &BTreeMap<String, String>,
) -> CeilingOutcome {
    let Some(ceiling) = ceiling else {
        return CeilingOutcome {
            permissions: requested.clone(),
            clamped: Vec::new(),
        };
    };
    let mut permissions = BTreeMap::new();
    let mut clamped = Vec::new();
    for (scope, level) in requested {
        let mut cap = ceiling
            .scopes
            .get(scope)
            .copied()
            .unwrap_or(ceiling.r#default);
        // The create/approve-PR toggle: when off, `pull-requests` can never
        // exceed read, no matter what the ceiling table says. This is the
        // scope-model approximation of GitHub's toggle — the token cannot
        // create or approve pull requests (nor comment on them).
        if scope == "pull-requests" && !ceiling.allow_create_approve_pr {
            let read = crate::config::PermissionLevel::Read;
            if cap.rank() > read.rank() {
                cap = read;
            }
        }
        if declared_rank(level) > cap.rank() {
            permissions.insert(scope.clone(), cap.as_str().to_owned());
            clamped.push(CeilingClampDetail {
                scope: scope.clone(),
                requested: level.clone(),
                granted: cap.as_str().to_owned(),
            });
        } else {
            permissions.insert(scope.clone(), level.clone());
        }
    }
    CeilingOutcome {
        permissions,
        clamped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PermissionLevel;

    fn ceiling(
        default_level: PermissionLevel,
        scopes: &[(&str, PermissionLevel)],
        allow_create_approve_pr: bool,
    ) -> TokenPermissionsCeiling {
        TokenPermissionsCeiling {
            r#default: default_level,
            scopes: scopes
                .iter()
                .map(|(scope, level)| ((*scope).to_owned(), *level))
                .collect(),
            allow_create_approve_pr,
        }
    }

    fn requested(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(scope, level)| ((*scope).to_owned(), (*level).to_owned()))
            .collect()
    }

    #[test]
    fn no_ceiling_leaves_request_unchanged() {
        let req = requested(&[("contents", "write"), ("pull-requests", "write")]);
        let outcome = apply_ceiling(None, &req);
        assert_eq!(outcome.permissions, req);
        assert!(outcome.clamped.is_empty());
    }

    #[test]
    fn write_all_workflow_plus_read_ceiling_yields_read_token() {
        let policy = ceiling(PermissionLevel::Read, &[], false);
        let outcome = apply_ceiling(
            Some(&policy),
            &requested(&[
                ("contents", "write"),
                ("issues", "write"),
                ("pull-requests", "write"),
            ]),
        );
        assert_eq!(outcome.permissions["contents"], "read");
        assert_eq!(outcome.permissions["issues"], "read");
        // pull-requests: write is clamped by the read default; the
        // create/approve toggle (off) agrees.
        assert_eq!(outcome.permissions["pull-requests"], "read");
        assert_eq!(outcome.clamped.len(), 3);
        let clamp = outcome
            .clamped
            .iter()
            .find(|detail| detail.scope == "contents")
            .unwrap();
        assert_eq!(clamp.requested, "write");
        assert_eq!(clamp.granted, "read");
    }

    #[test]
    fn unlisted_scope_uses_default_cap() {
        let policy = ceiling(
            PermissionLevel::Read,
            &[("contents", PermissionLevel::Write)],
            false,
        );
        let outcome = apply_ceiling(
            Some(&policy),
            &requested(&[("contents", "write"), ("packages", "write")]),
        );
        // Explicitly listed scope keeps write.
        assert_eq!(outcome.permissions["contents"], "write");
        // Unlisted scope falls back to the default cap.
        assert_eq!(outcome.permissions["packages"], "read");
        assert_eq!(outcome.clamped.len(), 1);
        assert_eq!(outcome.clamped[0].scope, "packages");
    }

    #[test]
    fn fork_profile_stays_the_floor_under_permissive_ceiling() {
        // A fork PR job arrives with an already-downgraded read-only request;
        // a permissive ceiling must not raise it.
        let policy = ceiling(PermissionLevel::Write, &[], true);
        let outcome = apply_ceiling(
            Some(&policy),
            &requested(&[("contents", "read"), ("pull-requests", "read")]),
        );
        assert_eq!(outcome.permissions["contents"], "read");
        assert_eq!(outcome.permissions["pull-requests"], "read");
        assert!(outcome.clamped.is_empty());
    }

    #[test]
    fn create_approve_toggle_caps_pull_requests_at_read() {
        let policy = ceiling(
            PermissionLevel::Write,
            &[("pull-requests", PermissionLevel::Write)],
            false,
        );
        let outcome = apply_ceiling(Some(&policy), &requested(&[("pull-requests", "write")]));
        assert_eq!(outcome.permissions["pull-requests"], "read");
        assert_eq!(outcome.clamped.len(), 1);
        assert_eq!(outcome.clamped[0].granted, "read");
    }

    #[test]
    fn create_approve_toggle_on_defers_to_ceiling() {
        let policy = ceiling(
            PermissionLevel::Read,
            &[("pull-requests", PermissionLevel::Write)],
            true,
        );
        let outcome = apply_ceiling(Some(&policy), &requested(&[("pull-requests", "write")]));
        assert_eq!(outcome.permissions["pull-requests"], "write");
        assert!(outcome.clamped.is_empty());
    }

    #[test]
    fn ceiling_never_raises_a_lower_request() {
        let policy = ceiling(PermissionLevel::Admin, &[], true);
        let outcome = apply_ceiling(Some(&policy), &requested(&[("contents", "read")]));
        assert_eq!(outcome.permissions["contents"], "read");
        assert!(outcome.clamped.is_empty());
    }

    #[test]
    fn unknown_requested_level_clamps_down() {
        let policy = ceiling(PermissionLevel::Read, &[], true);
        let outcome = apply_ceiling(Some(&policy), &requested(&[("contents", "banana")]));
        assert_eq!(outcome.permissions["contents"], "read");
        assert_eq!(outcome.clamped.len(), 1);
    }

    #[test]
    fn config_parses_from_toml() {
        let config: crate::config::ConfigFile = toml::from_str(
            r#"
[token_permissions_ceiling]
default = "read"
contents = "read"
pull-requests = "write"
allow_create_approve_pr = true
"#,
        )
        .expect("token ceiling config must parse");
        let policy = config
            .token_permissions_ceiling
            .expect("ceiling must be present");
        assert_eq!(policy.r#default, PermissionLevel::Read);
        assert_eq!(policy.scopes["contents"], PermissionLevel::Read);
        assert_eq!(policy.scopes["pull-requests"], PermissionLevel::Write);
        assert!(policy.allow_create_approve_pr);
    }

    #[test]
    fn config_defaults_are_read_and_toggle_off() {
        let config: crate::config::ConfigFile =
            toml::from_str("[token_permissions_ceiling]\n").expect("empty ceiling must parse");
        let policy = config
            .token_permissions_ceiling
            .expect("ceiling must be present");
        assert_eq!(policy.r#default, PermissionLevel::Read);
        assert!(policy.scopes.is_empty());
        assert!(!policy.allow_create_approve_pr);
    }

    #[test]
    fn absent_table_means_no_ceiling() {
        let config: crate::config::ConfigFile =
            toml::from_str("").expect("empty config must parse");
        assert!(config.token_permissions_ceiling.is_none());
    }

    #[test]
    fn unknown_level_is_rejected() {
        let result = toml::from_str::<crate::config::ConfigFile>(
            "[token_permissions_ceiling]\ncontents = \"banana\"\n",
        );
        assert!(result.is_err(), "unknown level must fail closed");
    }
}
