//! Job-message templates: stored `AgentJobRequestMessage` minus every secret
//! value and token, filled back at acquire.
//!
//! The control plane persists the **template** (built by `runs.rs::
//! build_job_artifacts` with secret *names* only — values are structurally
//! absent) plus a `preloop_secret_spec` inside it saying what to resolve.
//! When a runner claims the job, [`fill_template`] resolves real values
//! through the [`SecretProvider`] (run > environment > repository > global)
//! and stamps them into the in-memory message — which is then discarded,
//! never written back.
//!
//! Tokens (`system.github.token`, `github_token`) arrive already minted per
//! claim via [`template_token_fill`] — the broker mints a fresh App token or
//! falls back to the runtime token, so nothing token-shaped ever persists
//! either.

use std::collections::{BTreeMap, BTreeSet};

use preloop_gha_protocol::JobPlan;
use preloop_gha_protocol::azdo::{
    AgentJobRequestMessage, MaskHint, MaskType, MessageSecretSpec, VariableValue,
};

use crate::secret_provider::{SecretProvider, SecretScope};

/// Everything [`fill_template`] resolved back into the template.
///
/// `values` is the resolved `name -> value` map injected as secret
/// `variables` (token fills are applied separately and are not listed here).
/// `masked` is broader on purpose: it is every value the scope resolved —
/// injected or not — plus resolved `secrets:` map literals, exposed once
/// for the caller's masking needs. Unreferenced values can still reach a
/// log (a literal echoed through `env`, a workflow file body, a map
/// literal), so the node masker covers the whole scope even though the job
/// message no longer does.
#[derive(Debug, Default)]
pub(crate) struct FillOutcome {
    /// Secret variable names now present on the message.
    pub(crate) names: Vec<String>,
    /// Resolved plaintext values injected as `isSecret` variables.
    pub(crate) values: BTreeMap<String, String>,
    /// Every resolved scoped value (injected or not) plus map literals —
    /// the merge input for the node masker.
    pub(crate) masked: Vec<String>,
}

/// Build the [`MessageSecretSpec`] for a job at submit.
///
/// `names` is the job's *referenced* subset of the caller-scope name set
/// (`runs.rs::build_job_artifacts` computes it); `run_names` is the
/// submission's run-tier name set (non-secret, recorded for every job of
/// the run).
///
/// A reusable callee never carries the caller's name set: it receives only
/// the secrets its call mapped (`secrets: {...}`) or, with `secrets: inherit`,
/// every caller secret — the same rule
/// `preloop_gha_parser::job_builder` applies when it builds the message.
pub(crate) fn secret_spec_for(
    job: &JobPlan,
    names: &BTreeSet<String>,
    run_names: &BTreeSet<String>,
) -> MessageSecretSpec {
    let callee = job.workflow_file.is_some();
    MessageSecretSpec {
        names: if callee {
            BTreeSet::new()
        } else {
            names.clone()
        },
        environment: job.oidc_environment.clone(),
        inherit: callee && job.secrets_inherit,
        map: if callee && !job.secrets_inherit {
            job.secrets_map.clone()
        } else {
            BTreeMap::new()
        },
        run_names: run_names.clone(),
    }
}

/// The number of value-derived secret mask hints the builder appended.
///
/// `build_agent_job_message_with_normalized_context` appends one hint per
/// non-empty secret value, after the default regexes. With a names-only
/// (empty-value) input only `secrets:` map entries produce values, so this
/// count equals the map size — count `is_secret` variables with a non-empty
/// value so the caller does not depend on that detail.
pub(crate) fn secret_hint_count(msg: &AgentJobRequestMessage) -> usize {
    msg.variables
        .values()
        .filter(|v| v.is_secret == Some(true) && !v.value.as_deref().unwrap_or("").is_empty())
        .count()
}

/// Turn a built `AgentJobRequestMessage` into its stored template.
///
/// Drops every `is_secret` variable (their `value`/`isSecret` could carry a
/// real secret — keys-only builds leave them empty but the template must not
/// depend on that), truncates the trailing secret-derived mask hints, and
/// blanks snapshot step `token` inputs (the pinned step ids in
/// `preloop_snapshot_token_steps` survive — the fill re-mints by id).
///
/// `secret_hints` is how many trailing `mask_hints` entries were derived
/// from secret values (see [`secret_hint_count`]); they encode the value as
/// a regex literal and would leak it.
pub(crate) fn strip_template(msg: &mut AgentJobRequestMessage, secret_hints: usize) {
    msg.variables.retain(|_, v| v.is_secret != Some(true));
    msg.mask_hints
        .truncate(msg.mask_hints.len().saturating_sub(secret_hints));
    if let Some(pinned) = &msg.preloop_snapshot_token_steps {
        let ids: std::collections::HashSet<&str> = pinned.iter().map(String::as_str).collect();
        for step in &mut msg.steps {
            if ids.contains(step.id.to_string().as_str()) {
                step.inputs.remove("token");
            }
        }
    }
    // The origin-rewrite header is a Basic credential wrapping the same
    // runtime token. Blank just the header (keeping the URLs the rewrite
    // exists for): every delivery path re-mints it from a fresh token via
    // `broker::re_mint_snapshot_credentials`.
    if let Some(rewrite) = msg.preloop_snapshot_origin_rewrite.as_mut() {
        rewrite.auth_header.clear();
    }
}

/// Fill a stored template with the secret surface its spec describes.
///
/// `repository` and `run_id` scope the provider call: the run tier holds the
/// values the submission supplied, outranking every stored tier.
///
/// `None` spec = a fully-formed message stored before templates carried a
/// spec: filled as-is (no secret slot exists to populate) and `spec` left
/// `None`.
///
/// The claim must already have applied the minted token (see
/// `broker::apply_minted_token_to_message`): `secrets:` maps and job `env:`
/// that read `secrets.GITHUB_TOKEN` or `github.token` resolve here.
pub(crate) fn fill_template(
    msg: &mut AgentJobRequestMessage,
    provider: &dyn SecretProvider,
    repository: &str,
    run_id: preloop_gha_protocol::RunId,
) -> anyhow::Result<FillOutcome> {
    let Some(spec) = msg.preloop_secret_spec.clone() else {
        return Ok(FillOutcome::default());
    };

    // Resolve the scope once: run > env > repo > global, merged by the
    // provider.
    let scoped = provider.resolve(SecretScope {
        repository,
        environment: spec.environment.as_deref(),
        run_id: Some(run_id),
    })?;
    let scoped: BTreeMap<String, String> = preloop_gha_protocol::masking::expose_all(&scoped);

    // Every submission-supplied name must resolve from the run tier. A
    // missing tier (a node that never saw the submission, a lost or pruned
    // file) must fail loudly here — otherwise the job would silently run
    // with empty `secrets.*` values. Checked against the run tier alone:
    // the merged scope could otherwise mask the gap with a coarser tier's
    // value of the same name.
    if !spec.run_names.is_empty() {
        let run_tier = provider.run_tier(run_id)?;
        let missing: Vec<&str> = spec
            .run_names
            .iter()
            .filter(|name| !run_tier.contains_key(*name))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            anyhow::bail!(
                "run {run_id} declared submission secrets that cannot be resolved: {}",
                missing.join(", ")
            );
        }
    }

    let mut resolved: BTreeMap<String, String> = if spec.inherit {
        // `secrets: inherit`: every name in scope.
        scoped.clone()
    } else {
        BTreeMap::new()
    };
    for name in &spec.names {
        if let Some(value) = scoped.get(name) {
            resolved.insert(name.clone(), value.clone());
        }
    }
    if !spec.map.is_empty() {
        // Caller-side `secrets:` mapping: expressions resolve against the
        // caller's context — the full caller scope (not `resolved`, which is
        // empty for a callee), plus github/inputs/vars/matrix/strategy pulled
        // back out of the stored `context_data` so `${{ vars.X }}`-style maps
        // keep working. This is the sanctioned resolution point;
        // `build_context` masks everywhere else.
        let ctx = fill_expr_ctx(msg, &scoped);
        for (callee_name, expr) in &spec.map {
            let value = preloop_gha_parser::eval::resolve_string(expr, &ctx)
                .unwrap_or_else(|_| expr.clone());
            resolved.insert(callee_name.clone(), value);
        }
    }
    // The claim owns the GitHub token variables: a `secrets:` map entry or a
    // stored secret of the same name must never replace the minted token.
    resolved.retain(|name, _| !is_claim_owned_variable(name));

    // `env:` values referencing `secrets.*` ship as expression tokens: the
    // builder cannot resolve them (values are absent by design) and the
    // runner has no secrets context at run time, so the fill evaluates them
    // here and stamps the literal — the same resolution point as `spec.map`.
    if !resolved.is_empty() {
        let ctx = fill_expr_ctx(msg, &resolved);
        for entry in &mut msg.environment_variables {
            let Some(pair) = entry
                .get_mut("map")
                .and_then(|map| map.as_array_mut())
                .and_then(|map| map.first_mut())
            else {
                continue;
            };
            let Some(value_token) = pair.get_mut("Value") else {
                continue;
            };
            let rewritten: Option<String> = match value_token.get("type").and_then(|t| t.as_u64()) {
                Some(3) => value_token
                    .get("expr")
                    .and_then(|expr| expr.as_str())
                    .filter(|expr| expr.contains("secrets."))
                    .map(|expr| resolve_token_expr(expr, &ctx)),
                Some(1) => value_token
                    .get("exprs")
                    .and_then(|exprs| exprs.as_array())
                    .filter(|exprs| {
                        exprs
                            .iter()
                            .filter_map(|e| e.as_str())
                            .any(|e| e.contains("secrets."))
                    })
                    .map(|exprs| {
                        let template = value_token
                            .get("fmt")
                            .and_then(|fmt| fmt.as_str())
                            .unwrap_or_default()
                            .to_owned();
                        let mut out = template;
                        for (index, expr) in exprs.iter().enumerate() {
                            let value = resolve_token_expr(expr.as_str().unwrap_or_default(), &ctx);
                            out = out.replace(&format!("{{{index}}}"), &value);
                        }
                        out
                    }),
                _ => None,
            };
            if let Some(literal) = rewritten {
                *value_token = serde_json::json!({"type": 0, "lit": literal});
            }
        }
    }

    // Stamp variables + rebuild the value-derived mask hints in the exact
    // slot the strip removed them from (end of the hint list, matching the
    // builder's append order).
    let mut hints = Vec::with_capacity(resolved.len());
    for (name, value) in &resolved {
        msg.variables
            .insert(name.clone(), VariableValue::secret(value.clone()));
        if !value.is_empty() {
            hints.push(MaskHint {
                hint_type: MaskType::Regex,
                value: preloop_gha_parser::job_builder::regex_escape(value),
            });
        }
    }
    msg.mask_hints.extend(hints);
    // The spec is a server-internal carrier; strip it before the message is
    // serialized onto the wire.
    msg.preloop_secret_spec = None;

    // Mask breadth stays wider than the injected set: unreferenced secrets
    // are off the wire but can still reach a log through literals, so the
    // node masker gets the whole resolved scope.
    let mut masked: Vec<String> = scoped.values().cloned().collect();
    masked.extend(resolved.values().cloned());

    Ok(FillOutcome {
        names: resolved.keys().cloned().collect(),
        values: resolved,
        masked,
    })
}

/// Expression context for the `${{ }}` a fill resolves: the stored
/// `context_data` (whose `github.token` the claim has already patched) and a
/// `secrets` context holding the resolved values plus the claim's own
/// `GITHUB_TOKEN`.
fn fill_expr_ctx(
    msg: &AgentJobRequestMessage,
    secrets: &BTreeMap<String, String>,
) -> preloop_gha_expressions::Context {
    let mut ctx = preloop_gha_expressions::Context::new();
    for (key, value) in &msg.context_data {
        ctx.insert(key.clone(), value.to_json());
    }
    let mut values: serde_json::Map<String, serde_json::Value> = secrets
        .iter()
        .filter(|(name, _)| !is_claim_owned_variable(name))
        .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
        .collect();
    // `secrets.GITHUB_TOKEN` is the value the claim stamped into
    // `github_token`: the same token `github.token` reads.
    if let Some(token) = msg
        .variables
        .get("github_token")
        .and_then(|variable| variable.value.as_deref())
        .filter(|token| !token.is_empty())
    {
        values.insert(
            "GITHUB_TOKEN".to_owned(),
            serde_json::Value::String(token.to_owned()),
        );
    }
    ctx.insert("secrets", serde_json::Value::Object(values));
    ctx
}

/// Variables the claim owns: the minted GitHub token. A stored secret or a
/// `secrets:` map entry never replaces them.
fn is_claim_owned_variable(name: &str) -> bool {
    name.eq_ignore_ascii_case("github_token") || name.eq_ignore_ascii_case("system.github.token")
}

/// Evaluate one bare expression body (`secrets.X`, `format(...)`, …) as a
/// `${{ }}` source; an evaluation failure coalesces to "" rather than
/// shipping the template raw.
fn resolve_token_expr(expr: &str, ctx: &preloop_gha_expressions::Context) -> String {
    preloop_gha_parser::eval::resolve_string(&format!("${{{{ {expr} }}}}"), ctx).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret_provider::BuiltinSecretProvider;
    use preloop_gha_protocol::azdo::AgentJobRequestMessage;
    use preloop_gha_protocol::{RunId, SecretString};

    fn provider(dir: &std::path::Path) -> BuiltinSecretProvider {
        BuiltinSecretProvider::new(
            std::sync::Arc::new(parking_lot::RwLock::new(
                crate::state::SecretStore::default(),
            )),
            dir.join("run-secrets"),
            crate::store::Envelope::new(b"test-cluster-key"),
        )
    }

    /// A stored template that records `names` as both the scope names and the
    /// run-tier names, the way `secret_spec_for` does at submit.
    fn template(names: &[&str]) -> AgentJobRequestMessage {
        let names: Vec<&str> = names.to_vec();
        serde_json::from_value(serde_json::json!({
            "jobId": "00000000-0000-0000-0000-000000000000",
            "requestId": 1,
            "plan": {
                "planId": "plan",
                "planType": "actions",
                "version": 0,
                "artifactUri": "",
                "artifactLocation": ""
            },
            "timeline": {"id": "00000000-0000-0000-0000-000000000000", "changeId": 0},
            "jobName": "__job",
            "lockedUntil": "0001-01-01T00:00:00",
            "resources": {"endpoints": []},
            "preloopSecretSpec": {"names": names, "runNames": names}
        }))
        .expect("template decodes")
    }

    #[test]
    fn unresolvable_run_tier_is_an_error_not_an_empty_secret() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let run_id = RunId::new();
        provider(dir_a.path())
            .put_run(
                run_id,
                &BTreeMap::from([("TOKEN".to_owned(), SecretString::new("s3cr3t"))]),
            )
            .unwrap();

        // A node that never saw the submission (different run-secrets dir,
        // same cluster key) cannot resolve the run tier. The fill must fail
        // loudly instead of delivering an empty `secrets.TOKEN`.
        let mut other_node = template(&["TOKEN"]);
        let error = fill_template(&mut other_node, &provider(dir_b.path()), "o/r", run_id)
            .expect_err("an unresolvable run tier must be an error");
        assert!(error.to_string().contains("TOKEN"), "{error}");

        // The node that owns the tier fills the real value.
        let mut owner = template(&["TOKEN"]);
        let filled = fill_template(&mut owner, &provider(dir_a.path()), "o/r", run_id).unwrap();
        assert_eq!(
            filled.values.get("TOKEN").map(String::as_str),
            Some("s3cr3t")
        );
    }

    /// The claim stamps the minted token into `github_token` before the fill.
    /// A `secrets:` map entry and a job `env:` literal that read
    /// `secrets.GITHUB_TOKEN` must carry that token, and no stored secret of the
    /// same name may replace the claim's variable.
    #[test]
    fn fill_reads_the_claim_minted_token_for_secrets_github_token() {
        let dir = tempfile::tempdir().unwrap();
        let run_id = RunId::new();
        provider(dir.path())
            .put_run(
                run_id,
                &BTreeMap::from([
                    ("DEPLOY_KEY".to_owned(), SecretString::new("deploy-value")),
                    ("github_token".to_owned(), SecretString::new("stored-value")),
                ]),
            )
            .unwrap();
        let mut message = template(&["DEPLOY_KEY", "github_token"]);
        message.variables.insert(
            "github_token".to_owned(),
            VariableValue::secret("ghs_claim_minted"),
        );
        let spec = message.preloop_secret_spec.as_mut().expect("template spec");
        spec.map.insert(
            "MAPPED".to_owned(),
            "${{ secrets.GITHUB_TOKEN }}".to_owned(),
        );
        message.environment_variables = vec![serde_json::json!({
            "type": 2,
            "map": [{
                "Key": {"type": 0, "lit": "VIA_TOKEN"},
                "Value": {"type": 3, "expr": "secrets.GITHUB_TOKEN"}
            }]
        })];

        let filled = fill_template(&mut message, &provider(dir.path()), "o/r", run_id).unwrap();

        assert_eq!(
            message.variables["MAPPED"].value.as_deref(),
            Some("ghs_claim_minted"),
            "a `secrets:` map reading `secrets.GITHUB_TOKEN` gets the claim's token"
        );
        assert_eq!(
            message.environment_variables[0]["map"][0]["Value"],
            serde_json::json!({"type": 0, "lit": "ghs_claim_minted"}),
            "a job env reading `secrets.GITHUB_TOKEN` is stamped with the claim's token"
        );
        assert_eq!(
            message.variables["github_token"].value.as_deref(),
            Some("ghs_claim_minted"),
            "a stored secret must never replace the claim's `github_token`"
        );
        assert!(
            filled.names.iter().all(|name| name != "github_token"),
            "the claim-owned variable is not a filled name: {:?}",
            filled.names
        );
        assert_eq!(
            filled.values.get("DEPLOY_KEY").map(String::as_str),
            Some("deploy-value")
        );
    }
}
