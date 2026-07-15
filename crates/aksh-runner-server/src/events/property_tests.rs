//! Property tests for event adapters using proptest.
//!
//! Each test verifies invariants on the EffectiveEvent output for
//! valid and edge-case inputs. Strategies generate realistic payload
//! shapes based on GitHub webhook documentation.

#[cfg(test)]
mod tests {
    use crate::events::trust_tier::TrustTier;
    use crate::events::EventAdapter;
    use proptest::prelude::*;

    // Strategies for common webhook fields
    fn sha_str() -> impl Strategy<Value = String> {
        "[a-f0-9]{40}"
    }
    fn ref_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9/._-]{1,64}"
    }
    fn branch_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9/._-]{1,32}"
    }
    fn tag_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9._-]{1,32}"
    }
    fn action_str() -> impl Strategy<Value = String> {
        "[a-z_]{2,20}"
    }

    // --- Push adapter ---

    proptest! {
        #[test]
        fn push_adapter_never_panics_on_garbage(s in "\\PC*") {
            let v = serde_json::Value::String(s);
            let _ = crate::events::push::Adapter.project(&v);
        }

        #[test]
        fn push_skip_ci_suppresses(
            label in r"(\[skip ci\]|\[ci skip\]|\[no ci\]|\[skip actions\]|\[actions skip\])",
            rest in "\\PC{0,50}",
        ) {
            let payload = serde_json::json!({
                "ref": "refs/heads/main",
                "after": "abc123abc123abc123abc123abc123abc123abc1",
                "repository": { "default_branch": "main" },
                "commits": [{"message": format!("{}{}", rest, label)}]
            });
            let events = crate::events::push::Adapter.project(&payload);
            prop_assert!(events.is_empty(),
                "push should be suppressed when commit contains '{}'", label);
        }

        #[test]
        fn push_without_skip_ci_fires(
            message in "[A-Za-z0-9 :,.!]{1,100}",
        ) {
            // Ensure message doesn't contain any skip-ci label
            let msg_lower = message.to_lowercase();
            prop_assume!(!msg_lower.contains("[skip ci]")
                && !msg_lower.contains("[ci skip]")
                && !msg_lower.contains("[no ci]")
                && !msg_lower.contains("[skip actions]")
                && !msg_lower.contains("[actions skip]"));

            let payload = serde_json::json!({
                "ref": "refs/heads/main",
                "after": "abc123abc123abc123abc123abc123abc123abc1",
                "repository": { "default_branch": "main" },
                "commits": [{"message": message}]
            });
            let events = crate::events::push::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
        }

        #[test]
        fn push_default_branch_is_trusted(
            default_branch in branch_name(),
        ) {
            let payload = serde_json::json!({
                "ref": format!("refs/heads/{}", default_branch),
                "after": "abc123abc123abc123abc123abc123abc123abc1",
                "repository": { "default_branch": &default_branch },
                "commits": []
            });
            let events = crate::events::push::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::Trusted));
        }

        #[test]
        fn push_non_default_branch_is_internal(
            default_branch in branch_name(),
            feature_branch in branch_name(),
        ) {
            prop_assume!(default_branch != feature_branch);
            let payload = serde_json::json!({
                "ref": format!("refs/heads/{}", feature_branch),
                "after": "abc123abc123abc123abc123abc123abc123abc1",
                "repository": { "default_branch": &default_branch },
                "commits": []
            });
            let events = crate::events::push::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::Internal));
        }
    }

    // --- Pull request adapter ---

    proptest! {
        #[test]
        fn pr_valid_payload_emits_both_events(
            number in 1u64..100_000u64,
            base_ref in branch_name(),
            head_sha in sha_str(),
            merge_sha in sha_str(),
        ) {
            let payload = serde_json::json!({
                "action": "opened",
                "number": number,
                "pull_request": {
                    "number": number,
                    "base": { "ref": &base_ref, "sha": "base-sha" },
                    "head": { "ref": "feature/x", "sha": &head_sha, "repo": { "fork": false } },
                    "merge_commit_sha": &merge_sha
                }
            });
            let events = crate::events::pull_request::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 2,
                "expected 2 events (target + pr), got {}", events.len());

            let target = events.iter().find(|e| e.event == "pull_request_target").unwrap();
            prop_assert_eq!(&target.git_ref, &format!("refs/heads/{}", base_ref));
            prop_assert_eq!(&target.trust_tier, &Some(TrustTier::PullRequestTarget));

            let pr = events.iter().find(|e| e.event == "pull_request").unwrap();
            prop_assert_eq!(&pr.git_ref, &format!("refs/pull/{}/merge", number));
            prop_assert_eq!(&pr.trust_tier, &Some(TrustTier::InternalPullRequest));
        }

        #[test]
        fn fork_pr_emits_untrusted_pull_request(
            number in 1u64..100_000u64,
            head_sha in sha_str(),
        ) {
            let payload = serde_json::json!({
                "action": "opened",
                "number": number,
                "pull_request": {
                    "number": number,
                    "base": { "ref": "main" },
                    "head": { "ref": "fork/x", "sha": &head_sha, "repo": { "fork": true } },
                }
            });
            let events = crate::events::pull_request::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 2);
            let pr = events.iter().find(|event| event.event == "pull_request").unwrap();
            prop_assert_eq!(&pr.trust_tier, &Some(TrustTier::UntrustedForkPullRequest));
        }

        #[test]
        fn pr_without_merge_sha_uses_head(
            number in 1u64..100_000u64,
            head_sha in sha_str(),
        ) {
            let payload = serde_json::json!({
                "action": "synchronize",
                "number": number,
                "pull_request": {
                    "number": number,
                    "base": { "ref": "develop" },
                    "head": { "ref": "fix/bug", "sha": &head_sha, "repo": { "fork": false } },
                }
            });
            let events = crate::events::pull_request::Adapter.project(&payload);
            let pr = events.iter().find(|e| e.event == "pull_request");
            prop_assert!(pr.is_some(), "should emit a pull_request event");
            let pr = pr.unwrap();
            prop_assert_eq!(&pr.git_ref, &format!("refs/pull/{}/head", number));
        }
    }

    // --- Release adapter ---

    proptest! {
        #[test]
        fn release_with_tag_generates_refs_tags(
            tag in tag_name(),
            action in action_str(),
        ) {
            let payload = serde_json::json!({
                "action": &action,
                "release": { "tag_name": &tag }
            });
            let events = crate::events::release::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].event, "release");
            prop_assert_eq!(&events[0].git_ref, &format!("refs/tags/{}", tag));
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::Deployment));
        }

        #[test]
        fn release_without_tag_returns_empty(action in action_str()) {
            let payload = serde_json::json!({
                "action": &action,
                "release": {}
            });
            let events = crate::events::release::Adapter.project(&payload);
            prop_assert!(events.is_empty());
        }

        #[test]
        fn release_with_empty_tag_returns_empty(action in action_str()) {
            let payload = serde_json::json!({
                "action": &action,
                "release": { "tag_name": "" }
            });
            let events = crate::events::release::Adapter.project(&payload);
            prop_assert!(events.is_empty());
        }
    }

    // --- Create adapter ---

    proptest! {
        #[test]
        fn create_branch_uses_refs_heads(
            name in branch_name(),
        ) {
            let payload = serde_json::json!({
                "ref_type": "branch",
                "ref": &name,
            });
            let events = crate::events::create::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].git_ref, &format!("refs/heads/{}", name));
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::Internal));
        }

        #[test]
        fn create_tag_uses_refs_tags(
            name in tag_name(),
        ) {
            let payload = serde_json::json!({
                "ref_type": "tag",
                "ref": &name,
            });
            let events = crate::events::create::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].git_ref, &format!("refs/tags/{}", name));
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::Deployment));
        }

        #[test]
        fn create_empty_ref_returns_empty(ref_type in "(branch|tag)") {
            let payload = serde_json::json!({
                "ref_type": ref_type,
                "ref": "",
            });
            let events = crate::events::create::Adapter.project(&payload);
            prop_assert!(events.is_empty());
        }
    }

    // --- Default branch adapter ---

    proptest! {
        #[test]
        fn default_branch_adapters_use_correct_ref(
            default_branch in branch_name(),
            action in action_str(),
        ) {
            let payload = serde_json::json!({
                "action": &action,
                "repository": { "default_branch": &default_branch },
            });

            let adapters: &[(&dyn EventAdapter, &str)] = &[
                (&crate::events::issues::Adapter, "issues"),
                (&crate::events::issue_comment::Adapter, "issue_comment"),
                (&crate::events::discussion::Adapter, "discussion"),
                (&crate::events::discussion_comment::Adapter, "discussion_comment"),
                (&crate::events::label::Adapter, "label"),
                (&crate::events::milestone::Adapter, "milestone"),
                (&crate::events::watch::Adapter, "watch"),
                (&crate::events::fork::Adapter, "fork"),
                (&crate::events::member::Adapter, "member"),
                (&crate::events::public::Adapter, "public"),
                (&crate::events::page_build::Adapter, "page_build"),
                (&crate::events::repository_dispatch::Adapter, "repository_dispatch"),
            ];

            for (adapter, expected_event) in adapters {
                let events = adapter.project(&payload);
                prop_assert_eq!(events.len(), 1,
                    "adapter for {} returned {} events", expected_event, events.len());
                prop_assert_eq!(&events[0].event, expected_event);
                prop_assert_eq!(&events[0].git_ref, &format!("refs/heads/{}", default_branch));
                prop_assert_eq!(&events[0].activity_type, &Some(action.clone()));
            }
        }
    }

    // --- Workflow dispatch adapter ---

    proptest! {
        #[test]
        fn workflow_dispatch_trust_tier(
            default_branch in branch_name(),
        ) {
            let payload = serde_json::json!({
                "repository": { "default_branch": &default_branch },
                "inputs": {}
            });
            let events = crate::events::workflow_dispatch::Adapter.project(&payload);
            prop_assert_eq!(events.len(), 1);
            prop_assert_eq!(&events[0].trust_tier, &Some(TrustTier::AdminManual));
            prop_assert_eq!(&events[0].git_ref, &format!("refs/heads/{}", default_branch));
        }
    }

    // --- Non-proptest tests for parser filter validity ---

    #[test]
    fn valid_filter_keys_non_empty_for_known_events() {
        let events = [
            "push",
            "pull_request",
            "pull_request_target",
            "workflow_run",
            "workflow_dispatch",
            "schedule",
            "issues",
            "release",
            "create",
            "delete",
            "fork",
            "watch",
            "gollum",
            "page_build",
            "member",
            "public",
            "deployment",
            "deployment_status",
            "label",
            "milestone",
            "discussion",
            "discussion_comment",
            "issue_comment",
            "pull_request_review",
            "repository_dispatch",
        ];
        for event in &events {
            let keys = aksh_gha_parser::Trigger::valid_filter_keys(event);
            assert!(
                !keys.is_empty(),
                "valid_filter_keys({}) returned empty slice",
                event
            );
        }
    }

    #[test]
    fn pull_request_default_types_exclude_closed() {
        let workflow_yaml = r#"
on:
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        let workflow = aksh_gha_parser::parse_workflow(workflow_yaml).unwrap();

        assert!(workflow.on.matches_with_context(
            "pull_request",
            Some("main"),
            None,
            &[],
            Some("opened"),
            &[],
        ));
        assert!(!workflow.on.matches_with_context(
            "pull_request",
            Some("main"),
            None,
            &[],
            Some("closed"),
            &[],
        ));
        assert!(workflow.on.matches_with_context(
            "pull_request",
            Some("main"),
            None,
            &[],
            Some("synchronize"),
            &[],
        ));
        assert!(workflow.on.matches_with_context(
            "pull_request",
            Some("main"),
            None,
            &[],
            Some("reopened"),
            &[],
        ));
        assert!(workflow.on.matches_with_context(
            "pull_request",
            Some("main"),
            None,
            &[],
            Some("synchronized"),
            &[],
        ));
    }
}
