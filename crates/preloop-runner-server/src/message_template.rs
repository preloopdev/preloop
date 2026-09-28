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

use preloop_gha_protocol::azdo::{
    AgentJobRequestMessage, MaskHint, MaskType, MessageSecretSpec, VariableValue,
};
use preloop_gha_protocol::JobPlan;

use crate::secret_provider::{SecretProvider, SecretScope};

/// Everything [`fill_template`] resolved back into the template.
///
/// `values` is the resolved `name -> value` map injected as secret
/// `variables` (token fills are applied separately and are not listed here).
/// `masked` is `values`' values exposed once for the caller's masking needs —
/// the whole point of returning a summary rather than having the caller
/// re-scan the message is that it never has to iterate `variables` for
/// `is_secret` itself.
#[derive(Debug, Default)]
pub(crate) struct FillOutcome {
    /// Secret variable names now present on the message.
    pub(crate) names: Vec<String>,
    /// Resolved plaintext values (for caller-side mask/log purposes).
    pub(crate) values: BTreeMap<String, String>,
}

/// Build the [`MessageSecretSpec`] for a job at submit.
///
/// `names` is the caller-scope name set. Reusable callees never come through
/// here carrying a name set — their surface is `inherit`/`map` on `job`.
pub(crate) fn secret_spec_for(job: &JobPlan, names: &BTreeSet<String>) -> MessageSecretSpec {
    MessageSecretSpec {
        names: names.clone(),
        environment: job.oidc_environment.clone(),
        inherit: job.workflow_file.is_some() && job.secrets_inherit,
        map: if job.workflow_file.is_some() && !job.secrets_inherit {
            job.secrets_map.clone()
        } else {
            BTreeMap::new()
        },
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
}

/// Fill a stored template with the secret surface its spec describes.
///
/// `repository` and `run_id` scope the provider call: the run tier holds the
/// values the submission supplied, outranking every stored tier.
///
/// `None` spec = pre-M2 fully-formed message: filled as-is (no secret slot
/// exists to populate) and `spec` left `None`.
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
        // caller's context — secrets (real values — this is the sanctioned
        // resolution point, `build_context` masks everywhere else), plus
        // github/inputs/vars/matrix/strategy pulled back out of the stored
        // `context_data` so `${{ vars.X }}`-style maps keep working.
        let mut ctx = preloop_gha_expressions::Context::new();
        for (key, value) in &msg.context_data {
            ctx.insert(key.clone(), value.to_json());
        }
        ctx.insert(
            "secrets",
            serde_json::Value::Object(
                resolved
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                    .collect(),
            ),
        );
        for (callee_name, expr) in &spec.map {
            let value = preloop_gha_parser::eval::resolve_string(expr, &ctx)
                .unwrap_or_else(|_| expr.clone());
            resolved.insert(callee_name.clone(), value);
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

    Ok(FillOutcome {
        names: resolved.keys().cloned().collect(),
        values: resolved,
    })
}
