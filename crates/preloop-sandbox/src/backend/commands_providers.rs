//! Provider registration, durable work, reports and reconciliation.

use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use crate::api::*;
use crate::error::{Code, Error, Result};
use crate::service::ProviderIdentity;

use super::engine::SqlTx;
use super::logic;
use super::rows;
use super::seal::SealKey;
use super::store::{
    SqlStore, bump_version, fence_provider, now_ms, now_secs, outbox, provider_by_id,
    provider_by_name, provider_id_for, sandbox_by_id, touch_provider,
};
use super::types::{EnrollOutcome, Owner, WorkResult};

/// The durable envelope around a [`WorkItem`].
///
/// The wire work item is embedded so a poll can return it verbatim; the extra
/// fields are receipts the service needs at completion (source template for a
/// snapshot, the original capability hash, the owning node).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct WorkPayload {
    /// The wire item.
    pub work: WorkItem,
    /// Snapshot source template.
    #[serde(default)]
    pub source_template: Option<Template>,
    /// SHA-256 of the source lease, for snapshot replay checks.
    #[serde(default)]
    pub lease_sha256: String,
    /// Snapshot template name.
    #[serde(default)]
    pub snapshot_name: String,
    /// Node that accepted the operation.
    #[serde(default)]
    pub owner_node: String,
    /// Its session.
    #[serde(default)]
    pub owner_session: String,
}

impl WorkPayload {
    /// A payload for a plain work item.
    pub(crate) fn new(work: WorkItem) -> Self {
        Self {
            work,
            ..Self::default()
        }
    }

    /// A payload owned by `owner`.
    pub(crate) fn owned(work: WorkItem, owner: &Owner) -> Self {
        Self {
            work,
            owner_node: owner.node_id.clone(),
            owner_session: owner.session_id.clone(),
            ..Self::default()
        }
    }
}

/// Insert one work item. Returns `None` when an active item with the same
/// dedup key already exists (the caller observes that item instead).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_work(
    tx: &mut dyn SqlTx,
    seal: Option<&SealKey>,
    provider_id: &str,
    generation: u64,
    work: &WorkItem,
    kind: &str,
    payload: &WorkPayload,
    dedup_key: &str,
    visible_at: i64,
    deadline_at: i64,
) -> Result<Option<String>> {
    if !dedup_key.is_empty() {
        let active = tx
            .query(
                "SELECT id FROM work_items WHERE dedup_key = $1 AND status <> 'done' LIMIT 1",
                &[dedup_key.into()],
            )
            .await?;
        if !active.is_empty() {
            return Ok(None);
        }
    }
    let mut work = work.clone();
    let mut secret = None;
    if let Some(work_item::Work::Exec(exec)) = &mut work.work {
        let env = exec
            .request
            .as_option_mut()
            .map(|request| std::mem::take(&mut request.env))
            .unwrap_or_default();
        if !env.is_empty() {
            let seal = seal.ok_or_else(|| {
                Error::internal("cannot persist an exec env without a store seal key")
            })?;
            let bytes = serde_json::to_vec(&env).unwrap_or_default();
            secret = Some(seal.seal(&bytes, rows::exec_aad(&exec.exec_id).as_bytes())?);
        }
    }
    let mut payload = payload.clone();
    payload.work = work.clone();
    let payload_json = serde_json::to_string(&payload).unwrap_or_default();
    let version = bump_version(tx).await?;
    tx.execute(
        "INSERT INTO work_items \
         (id, provider_id, generation, kind, dedup_key, status, payload_json, secret_sealed, \
          result_json, attempt, visible_at, deadline_at, created_at, completed_at) \
         VALUES ($1, $2, $3, $4, $5, 'queued', $6, $7, NULL, 0, $8, $9, $10, NULL)",
        &[
            work.id.as_str().into(),
            provider_id.into(),
            generation.into(),
            kind.into(),
            dedup_key.into(),
            payload_json.as_str().into(),
            secret.into(),
            visible_at.into(),
            deadline_at.into(),
            now_ms().into(),
        ],
    )
    .await?;
    if let Some(work_item::Work::Create(create)) = &work.work
        && let Some(template) = create.template.as_option()
        && !template.native_ref.is_empty()
    {
        tx.execute(
            "INSERT INTO template_pins (base_ref, work_id) VALUES ($1, $2) \
             ON CONFLICT (base_ref, work_id) DO NOTHING",
            &[template.native_ref.as_str().into(), work.id.as_str().into()],
        )
        .await?;
    }
    outbox(
        tx,
        "work",
        &work.id,
        version,
        "queued",
        &format!("{{\"kind\":\"{kind}\",\"provider\":\"{provider_id}\"}}"),
    )
    .await?;
    Ok(Some(work.id))
}

/// Mark one work item done and apply its domain transition.
pub(crate) async fn complete_work(
    tx: &mut dyn SqlTx,
    work_id: &str,
    result: WorkResult,
) -> Result<()> {
    let row = tx
        .query(
            "SELECT id, provider_id, generation, kind, dedup_key, status, payload_json \
             FROM work_items WHERE id = $1",
            &[work_id.into()],
        )
        .await?
        .into_iter()
        .next();
    let Some(row) = row else {
        return Err(Error::not_found("unknown work item"));
    };
    if row.text("status")? == "done" {
        return Ok(());
    }
    let payload: WorkPayload = rows::json(row.text("payload_json")?, "work payload")?;
    let kind = row.text("kind")?.to_owned();
    let provider_id = row.text("provider_id")?.to_owned();
    let generation = row.u64("generation")?;
    let mut result = result;
    if kind == "snapshot" {
        let snapshot_sandbox = match &payload.work.work {
            Some(work_item::Work::Snapshot(snapshot)) => Some(snapshot.target.sandbox_id.clone()),
            _ => None,
        };
        if result.ok {
            match apply_snapshot_receipt(tx, &payload, &result).await? {
                SnapshotApplication::Applied(template) => {
                    result.template = Some(*template);
                }
                // The source changed mid-capture: the provider's work is
                // acknowledged (so it is not retried forever) and the
                // caller's wait ends with the domain failure.
                SnapshotApplication::Invalid(reason) => {
                    result = WorkResult::failed(reason);
                }
            }
        } else if let Some(sandbox_id) = snapshot_sandbox {
            tx.execute(
                "DELETE FROM snapshot_claims WHERE sandbox_id = $1 AND work_id = $2",
                &[sandbox_id.as_str().into(), work_id.into()],
            )
            .await?;
        }
    }
    let result_json = serde_json::to_string(&result).unwrap_or_default();
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE work_items SET status = 'done', result_json = $2, completed_at = $3, \
         secret_sealed = NULL WHERE id = $1",
        &[work_id.into(), result_json.as_str().into(), now_ms().into()],
    )
    .await?;
    outbox(
        tx,
        "work",
        work_id,
        version,
        if result.ok { "done" } else { "failed" },
        &result_json,
    )
    .await?;
    if let Some(work_item::Work::Create(create)) = &payload.work.work
        && let Some(template) = create.template.as_option()
        && !template.native_ref.is_empty()
    {
        tx.execute(
            "DELETE FROM template_pins WHERE work_id = $1",
            &[work_id.into()],
        )
        .await?;
        super::commands_templates::drop_snapshot_if_unpinned(tx, &template.native_ref).await?;
    }
    match kind.as_str() {
        "create" => {
            let sandbox_id = match &payload.work.work {
                Some(work_item::Work::Create(create)) => create.sandbox_id.clone(),
                _ => String::new(),
            };
            if result.ok {
                if let Some(mut sandbox) = sandbox_by_id(tx, &sandbox_id).await? {
                    let native_ref = result.native_ref.clone();
                    let state = if sandbox.lease_id.is_empty() {
                        SandboxState::Ready
                    } else {
                        SandboxState::Leased
                    };
                    sandbox.native_ref = native_ref;
                    sandbox.state = state.into();
                    save_sandbox(tx, &sandbox).await?;
                } else if !result.native_ref.is_empty() {
                    // Released while provisioning: destroy the late machine.
                    let driver = match &payload.work.work {
                        Some(work_item::Work::Create(create)) => create.driver.clone(),
                        _ => String::new(),
                    };
                    queue_destroy_target(
                        tx,
                        &provider_id,
                        generation,
                        &sandbox_id,
                        &driver,
                        &result.native_ref,
                        &logic::destroy_dedup_key(&sandbox_id),
                        now_ms() + 86_400_000,
                    )
                    .await?;
                }
            } else if !sandbox_id.is_empty() {
                super::commands_leases::fail_sandbox(tx, &sandbox_id, &result.error).await?;
            }
        }
        "destroy" => {
            let target = match &payload.work.work {
                Some(work_item::Work::Destroy(target)) => Some((**target).clone()),
                _ => None,
            };
            let Some(target) = target else {
                return Ok(());
            };
            if target.sandbox_id.is_empty() {
                // A captured-base cleanup, not a sandbox machine.
                return Ok(());
            }
            if result.ok {
                purge_sandbox(tx, &target.sandbox_id).await?;
            } else if sandbox_by_id(tx, &target.sandbox_id).await?.is_some() {
                // A failed destroy is retried by Reconcile while the provider
                // still reports the machine. Counting the failure here (not
                // at requeue time) is what bounds the retries; the record
                // holds through the first two and becomes a GONE tombstone at
                // the third, still reclaimable by reconcile.
                let stored_attempts = tx
                    .query(
                        "SELECT delete_attempts FROM sandboxes WHERE id = $1",
                        &[target.sandbox_id.as_str().into()],
                    )
                    .await?
                    .first()
                    .map(|row| row.int("delete_attempts"))
                    .transpose()?
                    .unwrap_or(0);
                let attempts = u32::try_from(stored_attempts)
                    .unwrap_or(u32::MAX)
                    .saturating_add(1);
                let version = bump_version(tx).await?;
                if attempts >= super::commands_runtime::MAX_DESTROY_ATTEMPTS {
                    let reason = format!("destroy failed {attempts} times: {}", result.error);
                    tx.execute(
                        "UPDATE sandboxes SET state = 'gone', error = $2, lease_id = '', \
                         delete_attempts = $3, version = $4 WHERE id = $1",
                        &[
                            target.sandbox_id.as_str().into(),
                            reason.as_str().into(),
                            i64::from(attempts).into(),
                            version.into(),
                        ],
                    )
                    .await?;
                } else {
                    tx.execute(
                        "UPDATE sandboxes SET delete_attempts = $2, version = $3 WHERE id = $1",
                        &[
                            target.sandbox_id.as_str().into(),
                            i64::from(attempts).into(),
                            version.into(),
                        ],
                    )
                    .await?;
                }
                outbox(
                    tx,
                    "sandbox",
                    &target.sandbox_id,
                    version,
                    "destroy-failed",
                    "{}",
                )
                .await?;
            }
        }
        "exec" => {
            if !result.ok
                && let Some(work_item::Work::Exec(exec)) = &payload.work.work
            {
                super::commands_runtime::fail_exec_inner(tx, &exec.exec_id, &result.error).await?;
            }
        }
        "pause" | "stop" | "start" | "resume" => {
            if result.ok
                && let Some(target) = payload.work.work.as_ref().and_then(sandbox_target)
                && let Some(mut sandbox) = sandbox_by_id(tx, &target.sandbox_id).await?
            {
                // A used machine stays a debug hold even while its preserved
                // lease survives: powering it on must never put it back in
                // the warm set.
                let resumed = if sandbox.preserve_until > 0 {
                    SandboxState::Preserved
                } else if sandbox.lease_id.is_empty() {
                    SandboxState::Ready
                } else {
                    SandboxState::Leased
                };
                sandbox.state = match kind.as_str() {
                    "pause" => SandboxState::Paused,
                    "stop" => SandboxState::Stopped,
                    "start" | "resume" => resumed,
                    _ => sandbox.state.known(),
                }
                .into();
                if kind == "start" || kind == "resume" {
                    sandbox.error.clear();
                    if sandbox.state.known() != SandboxState::Preserved {
                        sandbox.preserve_until = 0;
                    }
                }
                save_sandbox(tx, &sandbox).await?;
            }
        }
        // The snapshot receipt is applied before the work row is finalized,
        // so `result_json` can carry the created template.
        "snapshot" => {}
        _ => {}
    }
    Ok(())
}

fn sandbox_target(work: &work_item::Work) -> Option<SandboxWork> {
    match work {
        work_item::Work::Destroy(target)
        | work_item::Work::Pause(target)
        | work_item::Work::Resume(target)
        | work_item::Work::Stop(target) => Some((**target).clone()),
        work_item::Work::Start(start) => start.target.as_option().cloned(),
        work_item::Work::Exec(exec) => exec.target.as_option().cloned(),
        work_item::Work::Copy(copy) => copy.target.as_option().cloned(),
        work_item::Work::Snapshot(snapshot) => snapshot.target.as_option().cloned(),
        work_item::Work::CancelExec(cancel) => cancel.target.as_option().cloned(),
        work_item::Work::Create(_) => None,
    }
}

async fn save_sandbox(tx: &mut dyn SqlTx, sandbox: &Sandbox) -> Result<()> {
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE sandboxes SET name = $2, state = $3, lease_id = $4, native_ref = $5, \
         error = $6, provider_id = $7, preserve_until = $8, version = $9 WHERE id = $1",
        &[
            sandbox.id.as_str().into(),
            sandbox.name.as_str().into(),
            rows::state_label(sandbox.state.known()).into(),
            sandbox.lease_id.as_str().into(),
            sandbox.native_ref.as_str().into(),
            sandbox.error.as_str().into(),
            sandbox.provider_id.as_str().into(),
            sandbox.preserve_until.into(),
            version.into(),
        ],
    )
    .await?;
    outbox(tx, "sandbox", &sandbox.id, version, "update", "{}").await?;
    Ok(())
}

/// What a snapshot report means once checked against the live source.
///
/// The applied template is boxed: a `Template` is several hundred bytes and
/// the failure arm is a short reason, so keeping it inline would make every
/// value of the enum pay the larger size.
enum SnapshotApplication {
    /// The capture is installed as a template.
    Applied(Box<Template>),
    /// The source changed while the provider was working; the captured base
    /// is dropped and the caller gets this failure.
    Invalid(String),
}

async fn apply_snapshot_receipt(
    tx: &mut dyn SqlTx,
    payload: &WorkPayload,
    result: &WorkResult,
) -> Result<SnapshotApplication> {
    let (sandbox_id, template_name, target_native_ref, target_driver) = match &payload.work.work {
        Some(work_item::Work::Snapshot(snapshot)) => (
            snapshot.target.sandbox_id.clone(),
            snapshot.template.clone(),
            snapshot.target.native_ref.clone(),
            snapshot.target.driver.clone(),
        ),
        _ => return Err(Error::internal("snapshot work has no target")),
    };
    // A release, failure or lifecycle transition while the provider worked
    // invalidates the capture: installing the template would point at a
    // machine that is being destroyed or has changed.
    let source = sandbox_by_id(tx, &sandbox_id).await?;
    let valid = source.as_ref().is_some_and(|sandbox| {
        sandbox.state.known() == SandboxState::Leased
            && !payload.lease_sha256.is_empty()
            && super::logic::constant_time_eq(
                super::logic::token_hash(&sandbox.lease_id).as_bytes(),
                payload.lease_sha256.as_bytes(),
            )
            && (target_native_ref.is_empty() || sandbox.native_ref == target_native_ref)
    });
    if !valid {
        tx.execute(
            "DELETE FROM snapshot_claims WHERE sandbox_id = $1",
            &[sandbox_id.as_str().into()],
        )
        .await?;
        if !result.native_ref.is_empty()
            && let Some(source) = &source
        {
            // The driver may have left a base behind. Drop it as a snapshot
            // cleanup rather than as a sandbox machine.
            let generation = tx
                .query(
                    "SELECT generation FROM providers WHERE id = $1",
                    &[source.provider_id.as_str().into()],
                )
                .await?
                .first()
                .map(|row| row.u64("generation"))
                .transpose()?
                .unwrap_or(0);
            queue_destroy_target(
                tx,
                &source.provider_id,
                generation,
                "",
                &target_driver,
                &format!("snapshot:{}", result.native_ref),
                &format!("drop-snapshot:{}", result.native_ref),
                now_ms() + 86_400_000,
            )
            .await?;
        }
        return Ok(SnapshotApplication::Invalid(
            "failed_precondition: the sandbox changed while its snapshot was \
             in flight; result discarded"
                .to_owned(),
        ));
    }
    let source_template = payload.source_template.clone().unwrap_or_default();
    let spec = TemplateSpec {
        name: template_name.clone(),
        os: source_template.spec.os.clone(),
        source: Some(template_spec::Source::SnapshotOf(sandbox_id.clone())),
        requires: source_template.spec.requires.clone(),
        labels: source_template.spec.labels.clone(),
        ..TemplateSpec::default()
    };
    let digest = crate::service::template_digest(&spec);
    let spec_json = serde_json::to_string(&spec).unwrap_or_default();
    let version = bump_version(tx).await?;
    let old = tx
        .query(
            "SELECT provider_id, native_ref, driver FROM templates WHERE name = $1",
            &[template_name.as_str().into()],
        )
        .await?
        .into_iter()
        .next();
    if let Some(old) = &old {
        let native_ref = old.text("native_ref")?.to_owned();
        if !native_ref.is_empty() && native_ref != result.native_ref {
            super::commands_templates::retire_snapshot(
                tx,
                &native_ref,
                old.text("provider_id")?,
                old.text("driver")?,
            )
            .await?;
        }
    }
    let provider_id = source
        .as_ref()
        .map(|sandbox| sandbox.provider_id.clone())
        .unwrap_or_default();
    let driver = target_driver;
    tx.execute(
        "INSERT INTO templates \
         (name, digest, spec_json, provider_id, native_ref, driver, created_at, version, \
          retired, pending_drop) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, FALSE) \
         ON CONFLICT (name) DO UPDATE SET digest = $2, spec_json = $3, provider_id = $4, \
          native_ref = $5, driver = $6, created_at = $7, version = $8, retired = FALSE, \
          pending_drop = FALSE",
        &[
            template_name.as_str().into(),
            digest.as_str().into(),
            spec_json.as_str().into(),
            provider_id.as_str().into(),
            result.native_ref.as_str().into(),
            driver.as_str().into(),
            now_secs().into(),
            version.into(),
        ],
    )
    .await?;
    outbox(
        tx,
        "template",
        &template_name,
        version,
        "snapshot",
        &spec_json,
    )
    .await?;
    // The source sandbox is consumed by the snapshot. Its open execs fail
    // with "snapshotted" and the provider is asked to stop them.
    tx.execute(
        "DELETE FROM snapshot_claims WHERE sandbox_id = $1",
        &[sandbox_id.as_str().into()],
    )
    .await?;
    purge_sandbox_with_reason(tx, &sandbox_id, "sandbox was snapshotted").await?;
    Ok(SnapshotApplication::Applied(Box::new(Template {
        spec: spec.into(),
        digest,
        provider_id,
        native_ref: result.native_ref.clone(),
        created_at: now_secs(),
        driver,
        ..Template::default()
    })))
}

/// Delete a sandbox record and everything that could still act on it.
pub(crate) async fn purge_sandbox(tx: &mut dyn SqlTx, sandbox_id: &str) -> Result<()> {
    purge_sandbox_with_reason(tx, sandbox_id, "sandbox was destroyed").await
}

/// Delete a sandbox record, failing its live handles with `reason`. The
/// reason is what the caller's open exec stream reports, so a snapshot says
/// the sandbox was snapshotted rather than destroyed.
pub(crate) async fn purge_sandbox_with_reason(
    tx: &mut dyn SqlTx,
    sandbox_id: &str,
    reason: &str,
) -> Result<()> {
    let leases = tx
        .query(
            "SELECT id, idempotency_key FROM leases WHERE sandbox_id = $1",
            &[sandbox_id.into()],
        )
        .await?;
    for row in &leases {
        let lease_id = row.text("id")?.to_owned();
        let key = row.text("idempotency_key")?.to_owned();
        tx.execute(
            "DELETE FROM leases WHERE id = $1",
            &[lease_id.as_str().into()],
        )
        .await?;
        if !key.is_empty() {
            tx.execute("DELETE FROM lease_keys WHERE key = $1", &[key.into()])
                .await?;
        }
    }
    // Fail each live exec through the same path a caller disconnect uses:
    // the row is finalized and the provider is asked to kill the command
    // that would otherwise keep running against a machine being destroyed.
    let execs = tx
        .query(
            "SELECT id FROM execs WHERE sandbox_id = $1 AND status = 'running'",
            &[sandbox_id.into()],
        )
        .await?;
    for row in execs {
        super::commands_runtime::fail_exec_inner(tx, row.text("id")?, reason).await?;
    }
    tx.execute(
        "UPDATE tickets SET status = 'aborted', reason = $2 \
         WHERE sandbox_id = $1 AND status IN ('open', 'ready')",
        &[sandbox_id.into(), reason.into()],
    )
    .await?;
    tx.execute(
        "DELETE FROM snapshot_claims WHERE sandbox_id = $1",
        &[sandbox_id.into()],
    )
    .await?;
    tx.execute("DELETE FROM sandboxes WHERE id = $1", &[sandbox_id.into()])
        .await?;
    tx.execute(
        "DELETE FROM sandbox_specs WHERE id = $1",
        &[sandbox_id.into()],
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn queue_destroy_target(
    tx: &mut dyn SqlTx,
    provider_id: &str,
    generation: u64,
    sandbox_id: &str,
    driver: &str,
    native_ref: &str,
    dedup_key: &str,
    deadline_at: i64,
) -> Result<()> {
    let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let target = SandboxWork {
        sandbox_id: sandbox_id.to_owned(),
        driver: driver.to_owned(),
        native_ref: native_ref.to_owned(),
        ..SandboxWork::default()
    };
    let work = WorkItem::new(work_id.clone(), work_item::Work::Destroy(target.into()));
    let payload = WorkPayload::new(work.clone());
    insert_work(
        tx,
        None,
        provider_id,
        generation,
        &work,
        "destroy",
        &payload,
        dedup_key,
        now_ms(),
        deadline_at,
    )
    .await?;
    Ok(())
}

// ── Provider registration and identity ─────────────────────────────

pub(crate) async fn register(
    store: &SqlStore,
    request: RegisterRequest,
    enrolled: Option<&str>,
    owner: &Owner,
) -> Result<RegisterResponse> {
    if request.name.is_empty() {
        return Err(Error::invalid_argument("name is required"));
    }
    if request.protocol_version != crate::PROTOCOL_VERSION {
        return Err(Error::failed_precondition(format!(
            "agent speaks protocol version {}, this service speaks protocol version {}",
            request.protocol_version,
            crate::PROTOCOL_VERSION
        )));
    }
    store
        .run_tx(|tx| {
            let request = request.clone();
            let enrolled = enrolled.map(str::to_owned);
            let owner = owner.clone();
            Box::pin(async move {
                if let Some(provider_id) = &enrolled {
                    let provider = provider_by_id(tx, provider_id).await?.ok_or_else(|| {
                        Error::new(Code::PermissionDenied, "client certificate is not enrolled")
                    })?;
                    if provider.name != request.name {
                        return Err(Error::new(
                            Code::PermissionDenied,
                            "provider name does not match the client certificate",
                        ));
                    }
                }
                let existing = match &enrolled {
                    Some(provider_id) => provider_by_id(tx, provider_id).await?,
                    None => provider_by_name(tx, &request.name).await?,
                };
                let id = existing
                    .as_ref()
                    .map(|provider| provider.id.clone())
                    .unwrap_or_else(|| {
                        format!("prv-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
                    });
                let generation = existing
                    .as_ref()
                    .map_or(1, |provider| provider.generation + 1);
                let cordoned = existing.as_ref().is_some_and(|provider| provider.cordoned);
                let certificates = existing
                    .as_ref()
                    .map(|provider| provider.certificates.clone())
                    .unwrap_or_default();
                let drivers_json = serde_json::to_string(&request.drivers).unwrap_or_default();
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO providers \
                     (id, name, generation, drivers_json, last_seen, seen_at, cordoned, \
                      host_json, host_sampled_at, stats_json, owner_node, owner_session) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, '[]', $8, $9) \
                     ON CONFLICT (id) DO UPDATE SET name = $2, generation = $3, \
                      drivers_json = $4, last_seen = $5, seen_at = $6, cordoned = $7, \
                      host_json = NULL, host_sampled_at = NULL, stats_json = '[]', \
                      owner_node = $8, owner_session = $9",
                    &[
                        id.as_str().into(),
                        request.name.as_str().into(),
                        generation.into(),
                        drivers_json.as_str().into(),
                        now_secs().into(),
                        now_ms().into(),
                        cordoned.into(),
                        owner.node_id.as_str().into(),
                        owner.session_id.as_str().into(),
                    ],
                )
                .await?;
                // Everything the previous generation held is unacknowledged:
                // hand it to the new generation. Attempts advance only when
                // it is delivered again.
                tx.execute(
                    "UPDATE work_items SET status = 'queued', visible_at = 0, generation = $2 \
                     WHERE provider_id = $1 AND status = 'inflight'",
                    &[id.as_str().into(), generation.into()],
                )
                .await?;
                for certificate in &certificates {
                    tx.execute(
                        "INSERT INTO provider_certificates (provider_id, sha256, expires_at) \
                         VALUES ($1, $2, $3) ON CONFLICT (sha256) DO NOTHING",
                        &[
                            id.as_str().into(),
                            certificate.sha256.as_str().into(),
                            certificate.expires_at.into(),
                        ],
                    )
                    .await?;
                }
                outbox(tx, "provider", &id, version, "register", "{}").await?;
                Ok(RegisterResponse {
                    provider_id: id,
                    generation,
                    ..RegisterResponse::default()
                })
            })
        })
        .await
}

pub(crate) async fn heartbeat(store: &SqlStore, request: HeartbeatRequest) -> Result<()> {
    store
        .run_tx(|tx| {
            let request = request.clone();
            Box::pin(async move {
                let _ = fence_provider(tx, &request.provider_id, request.generation).await?;
                let host = request
                    .host
                    .into_option()
                    .map(|host| serde_json::to_string(&host).unwrap_or_default());
                let stats = serde_json::to_string(&request.sandboxes).unwrap_or_default();
                tx.execute(
                    "UPDATE providers SET last_seen = $2, seen_at = $3, \
                     host_json = COALESCE($4, host_json), \
                     host_sampled_at = CASE WHEN $4 IS NULL THEN host_sampled_at ELSE $3 END, \
                     stats_json = $5 WHERE id = $1",
                    &[
                        request.provider_id.as_str().into(),
                        now_secs().into(),
                        now_ms().into(),
                        host.into(),
                        stats.as_str().into(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn set_cordon(
    store: &SqlStore,
    reference: &str,
    cordoned: bool,
) -> Result<Provider> {
    store
        .run_tx(|tx| {
            let reference = reference.to_owned();
            Box::pin(async move {
                let id = provider_id_for(tx, &reference).await?;
                let version = bump_version(tx).await?;
                tx.execute(
                    "UPDATE providers SET cordoned = $2 WHERE id = $1",
                    &[id.as_str().into(), cordoned.into()],
                )
                .await?;
                outbox(tx, "provider", &id, version, "cordon", "{}").await?;
                provider_by_id(tx, &id)
                    .await?
                    .ok_or_else(|| Error::not_found("provider not found"))
            })
        })
        .await
}

pub(crate) async fn drain(store: &SqlStore, reference: &str) -> Result<DrainProviderResult> {
    store
        .run_tx(|tx| {
            let reference = reference.to_owned();
            Box::pin(async move {
                let id = provider_id_for(tx, &reference).await?;
                let version = bump_version(tx).await?;
                tx.execute(
                    "UPDATE providers SET cordoned = TRUE WHERE id = $1",
                    &[id.as_str().into()],
                )
                .await?;
                let idle = tx
                    .query(
                        "SELECT s.id FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.provider_id = $1 AND p.pool <> '' AND s.lease_id = '' \
                           AND s.state NOT IN ('gone', 'failed', 'terminating')",
                        &[id.as_str().into()],
                    )
                    .await?;
                let released = u32::try_from(idle.len()).unwrap_or(u32::MAX);
                for row in idle {
                    let sandbox_id = row.text("id")?.to_owned();
                    super::commands_leases::destroy_sandbox(tx, &sandbox_id).await?;
                }
                let leased = tx
                    .query(
                        "SELECT COUNT(*) AS leased FROM sandboxes \
                         WHERE provider_id = $1 AND lease_id <> '' \
                           AND state NOT IN ('gone', 'failed')",
                        &[id.as_str().into()],
                    )
                    .await?
                    .first()
                    .map(|row| row.int("leased"))
                    .transpose()?
                    .unwrap_or(0);
                outbox(tx, "provider", &id, version, "drain", "{}").await?;
                Ok(DrainProviderResult {
                    released_pool_members: released,
                    leased_remaining: u32::try_from(leased).unwrap_or(u32::MAX),
                    ..DrainProviderResult::default()
                })
            })
        })
        .await
}

pub(crate) async fn delete_provider(store: &SqlStore, name: &str) -> Result<()> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let provider = provider_by_name(tx, &name).await?.ok_or_else(|| {
                    Error::not_found(format!("provider `{name}` is not registered"))
                })?;
                let version = bump_version(tx).await?;
                let work = tx
                    .query(
                        "SELECT id FROM work_items WHERE provider_id = $1 AND status <> 'done'",
                        &[provider.id.as_str().into()],
                    )
                    .await?;
                for row in work {
                    complete_work(
                        tx,
                        row.text("id")?,
                        WorkResult::failed("provider was removed"),
                    )
                    .await?;
                }
                let sandboxes = tx
                    .query(
                        "SELECT id FROM sandboxes WHERE provider_id = $1",
                        &[provider.id.as_str().into()],
                    )
                    .await?;
                for row in sandboxes {
                    purge_sandbox(tx, row.text("id")?).await?;
                }
                tx.execute(
                    "DELETE FROM provider_certificates WHERE provider_id = $1",
                    &[provider.id.as_str().into()],
                )
                .await?;
                tx.execute(
                    "DELETE FROM providers WHERE id = $1",
                    &[provider.id.as_str().into()],
                )
                .await?;
                outbox(tx, "provider", &provider.id, version, "delete", "{}").await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn list_providers(
    store: &SqlStore,
    request: &ListRequest,
) -> Result<ListProvidersResponse> {
    let window = logic::page_window("provider", request)?;
    let after = window.after.clone().unwrap_or_default();
    let limit = i64::try_from(window.limit).unwrap_or(i64::MAX);
    store
        .run_tx(|tx| {
            let after = after.clone();
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT id, name, generation, drivers_json, last_seen, cordoned \
                         FROM providers WHERE id > $1 ORDER BY id LIMIT $2",
                        &[after.into(), (limit + 1).into()],
                    )
                    .await?;
                let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
                let mut providers = Vec::new();
                let mut ids = Vec::new();
                for row in rows
                    .iter()
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                {
                    ids.push(row.text("id")?.to_owned());
                    let mut provider = rows::provider_from_row(row)?;
                    provider.certificates = tx
                        .query(
                            "SELECT sha256, expires_at FROM provider_certificates \
                             WHERE provider_id = $1",
                            &[provider.id.as_str().into()],
                        )
                        .await?
                        .iter()
                        .map(|row| {
                            Ok(ProviderCertificate {
                                sha256: row.text("sha256")?.to_owned(),
                                expires_at: row.int("expires_at")?,
                                ..ProviderCertificate::default()
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    providers.push(provider);
                }
                let token = if has_more {
                    logic::next_page_token("provider", &ids, ids.len(), window.limit)
                } else {
                    String::new()
                };
                Ok(ListProvidersResponse {
                    providers,
                    next_page_token: token,
                    ..ListProvidersResponse::default()
                })
            })
        })
        .await
}

pub(crate) async fn list_providers_all(store: &SqlStore) -> Result<Vec<Provider>> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT id, name, generation, drivers_json, last_seen, cordoned \
                         FROM providers ORDER BY id",
                        &[],
                    )
                    .await?;
                let mut providers = Vec::new();
                for row in rows {
                    let mut provider = rows::provider_from_row(&row)?;
                    provider.certificates = tx
                        .query(
                            "SELECT sha256, expires_at FROM provider_certificates \
                             WHERE provider_id = $1",
                            &[provider.id.as_str().into()],
                        )
                        .await?
                        .iter()
                        .map(|row| {
                            Ok(ProviderCertificate {
                                sha256: row.text("sha256")?.to_owned(),
                                expires_at: row.int("expires_at")?,
                                ..ProviderCertificate::default()
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    providers.push(provider);
                }
                Ok(providers)
            })
        })
        .await
}

pub(crate) async fn provider_identity(
    store: &SqlStore,
    fingerprint_sha256: &str,
) -> Result<ProviderIdentity> {
    store
        .run_tx(|tx| {
            let fingerprint = fingerprint_sha256.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT p.id, p.name FROM providers p \
                         JOIN provider_certificates c ON c.provider_id = p.id \
                         WHERE c.sha256 = $1 AND c.expires_at > $2 LIMIT 1",
                        &[fingerprint.into(), now_secs().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                match row {
                    Some(row) => Ok::<_, Error>(ProviderIdentity {
                        provider_id: row.text("id")?.to_owned(),
                        name: row.text("name")?.to_owned(),
                    }),
                    None => Err(Error::new(
                        Code::PermissionDenied,
                        "client certificate is not enrolled",
                    )),
                }
            })
        })
        .await
}

pub(crate) async fn create_join_token(
    store: &SqlStore,
    id: &str,
    secret_sha256: &str,
    expires_at: i64,
    provider_name: &str,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            let hash = secret_sha256.to_owned();
            let provider_name = provider_name.to_owned();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO join_tokens (id, secret_sha256, expires_at, provider_name) \
                     VALUES ($1, $2, $3, $4)",
                    &[
                        id.into(),
                        hash.into(),
                        expires_at.into(),
                        provider_name.into(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn enroll(
    store: &SqlStore,
    token_id: &str,
    secret_sha256: &str,
    name: &str,
    csr_sha256: &str,
    cert_sha256: &str,
    cert_pem: &str,
    cert_expires_at: i64,
) -> Result<EnrollOutcome> {
    store
        .run_tx(|tx| {
            let token_id = token_id.to_owned();
            let secret = secret_sha256.to_owned();
            let name = name.to_owned();
            let csr = csr_sha256.to_owned();
            let cert = cert_sha256.to_owned();
            let cert_pem = cert_pem.to_owned();
            Box::pin(async move {
                let now = now_secs();
                let invalid = || Error::new(Code::Unauthenticated, "invalid join token");
                // Retry of an accepted enrollment: same token, same secret,
                // same CSR and same name gets the same certificate back. A
                // different CSR or name is a different request and is
                // refused. The secret is re-checked, so knowing the token id
                // is not enough to fetch the certificate.
                if let Some(receipt) = tx
                    .query(
                        "SELECT secret_sha256, csr_sha256, provider_name, cert_pem, \
                                cert_expires_at, expires_at \
                         FROM join_receipts WHERE token_id = $1",
                        &[token_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                {
                    if receipt.int("expires_at")? <= now {
                        tx.execute(
                            "DELETE FROM join_receipts WHERE token_id = $1",
                            &[token_id.as_str().into()],
                        )
                        .await?;
                    } else {
                        if !logic::constant_time_eq(
                            receipt.text("secret_sha256")?.as_bytes(),
                            secret.as_bytes(),
                        ) {
                            return Err(invalid());
                        }
                        if receipt.text("csr_sha256")? == csr
                            && receipt.text("provider_name")? == name
                        {
                            return Ok(EnrollOutcome::Replay {
                                cert_pem: receipt.text("cert_pem")?.to_owned(),
                                expires_at: receipt.int("cert_expires_at")?,
                            });
                        }
                        return Err(Error::new(Code::Unauthenticated, "join token already used"));
                    }
                }
                let row = tx
                    .query(
                        "SELECT id, secret_sha256, expires_at, provider_name \
                         FROM join_tokens WHERE id = $1",
                        &[token_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(invalid)?;
                if row.int("expires_at")? <= now {
                    return Err(invalid());
                }
                if !logic::constant_time_eq(
                    row.text("secret_sha256")?.as_bytes(),
                    secret.as_bytes(),
                ) {
                    return Err(invalid());
                }
                let bound_name = row.text("provider_name")?.to_owned();
                let existing = provider_by_name(tx, &name).await?;
                // A token is only usable for the identity it was minted for.
                // An unbound token may enroll a new name, never take over an
                // existing one; replacing a live provider's credentials
                // requires a token bound to its name.
                if !bound_name.is_empty() && bound_name != name {
                    return Err(Error::new(
                        Code::PermissionDenied,
                        "join token was issued for a different provider name",
                    ));
                }
                if bound_name.is_empty() && existing.is_some() {
                    return Err(Error::new(
                        Code::AlreadyExists,
                        "provider name is already enrolled: re-enrolling it requires \
                         a join token bound to that name",
                    ));
                }
                let deleted = tx
                    .execute(
                        "DELETE FROM join_tokens WHERE id = $1 AND secret_sha256 = $2",
                        &[token_id.as_str().into(), secret.as_str().into()],
                    )
                    .await?;
                if deleted != 1 {
                    return Err(invalid());
                }
                let id = existing
                    .as_ref()
                    .map(|provider| provider.id.clone())
                    .unwrap_or_else(|| {
                        format!("prv-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
                    });
                let generation = existing.as_ref().map_or(0, |provider| provider.generation);
                let drivers_json = existing
                    .as_ref()
                    .map(|provider| serde_json::to_string(&provider.drivers).unwrap_or_default())
                    .unwrap_or_else(|| "[]".to_owned());
                let cordoned = existing.as_ref().is_some_and(|provider| provider.cordoned);
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO providers \
                     (id, name, generation, drivers_json, last_seen, seen_at, cordoned, \
                      host_json, host_sampled_at, stats_json) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, '[]') \
                     ON CONFLICT (id) DO UPDATE SET name = $2, drivers_json = $4, \
                      cordoned = $7",
                    &[
                        id.as_str().into(),
                        name.as_str().into(),
                        generation.into(),
                        drivers_json.as_str().into(),
                        now_secs().into(),
                        now_ms().into(),
                        cordoned.into(),
                    ],
                )
                .await?;
                // Re-enrolling replaces earlier credentials: issuing a new
                // token means these credentials, not accumulated old ones.
                tx.execute(
                    "DELETE FROM provider_certificates WHERE provider_id = $1",
                    &[id.as_str().into()],
                )
                .await?;
                tx.execute(
                    "INSERT INTO provider_certificates (provider_id, sha256, expires_at) \
                     VALUES ($1, $2, $3)",
                    &[
                        id.as_str().into(),
                        cert.as_str().into(),
                        cert_expires_at.into(),
                    ],
                )
                .await?;
                // Work the previous credential set held is unacknowledged:
                // return it to the queue instead of leaving it in flight for
                // a session that may never poll again.
                tx.execute(
                    "UPDATE work_items SET status = 'queued', visible_at = 0 \
                     WHERE provider_id = $1 AND status = 'inflight'",
                    &[id.as_str().into()],
                )
                .await?;
                tx.execute(
                    "INSERT INTO join_receipts \
                     (token_id, secret_sha256, csr_sha256, provider_name, cert_sha256, \
                      cert_pem, cert_expires_at, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        token_id.as_str().into(),
                        secret.as_str().into(),
                        csr.as_str().into(),
                        name.as_str().into(),
                        cert.as_str().into(),
                        cert_pem.as_str().into(),
                        cert_expires_at.into(),
                        row.int("expires_at")?.into(),
                    ],
                )
                .await?;
                outbox(tx, "provider", &id, version, "enroll", "{}").await?;
                Ok(EnrollOutcome::Issued)
            })
        })
        .await
}

pub(crate) async fn renew_certificate(
    store: &SqlStore,
    fingerprint_sha256: &str,
    cert_sha256: &str,
    cert_expires_at: i64,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let fingerprint = fingerprint_sha256.to_owned();
            let cert = cert_sha256.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT p.id FROM providers p \
                         JOIN provider_certificates c ON c.provider_id = p.id \
                         WHERE c.sha256 = $1 AND c.expires_at > $2 LIMIT 1",
                        &[fingerprint.into(), now_secs().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        Error::new(Code::PermissionDenied, "client certificate is not enrolled")
                    })?;
                let id = row.text("id")?.to_owned();
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO provider_certificates (provider_id, sha256, expires_at) \
                     VALUES ($1, $2, $3) ON CONFLICT (sha256) DO UPDATE SET expires_at = $3",
                    &[
                        id.as_str().into(),
                        cert.as_str().into(),
                        cert_expires_at.into(),
                    ],
                )
                .await?;
                tx.execute(
                    "DELETE FROM provider_certificates WHERE provider_id = $1 AND expires_at <= $2",
                    &[id.as_str().into(), now_secs().into()],
                )
                .await?;
                // Keep the newest certificates and evict the rest, so a
                // compromised certificate cannot live forever by renewing.
                let retained = tx
                    .query(
                        "SELECT sha256 FROM provider_certificates WHERE provider_id = $1 \
                         ORDER BY expires_at DESC, sha256 DESC",
                        &[id.as_str().into()],
                    )
                    .await?;
                for stale in retained
                    .into_iter()
                    .skip(crate::service::MAX_PROVIDER_CERTIFICATES)
                {
                    tx.execute(
                        "DELETE FROM provider_certificates WHERE provider_id = $1 AND sha256 = $2",
                        &[id.as_str().into(), stale.text("sha256")?.into()],
                    )
                    .await?;
                }
                outbox(tx, "provider", &id, version, "renew-certificate", "{}").await?;
                Ok(())
            })
        })
        .await
}

// ── Poll, report, reconcile ──────────────────────────────────────

pub(crate) async fn poll_work(
    store: &SqlStore,
    provider_id: &str,
    generation: u64,
    batch: usize,
    delivery_timeout: std::time::Duration,
) -> Result<Vec<WorkItem>> {
    let seal = store.seal.clone();
    store
        .run_tx(|tx| {
            let seal = seal.clone();
            let provider_id = provider_id.to_owned();
            Box::pin(async move {
                let _ = fence_provider(tx, &provider_id, generation).await?;
                touch_provider(tx, &provider_id).await?;
                requeue_expired(tx, &provider_id, generation).await?;
                let now = now_ms();
                let batch = i64::try_from(batch.max(1)).unwrap_or(i64::MAX);
                let sql = if tx.dialect() == super::engine::Dialect::Postgres {
                    "SELECT id, attempt FROM work_items \
                     WHERE provider_id = $1 AND status = 'queued' AND visible_at <= $2 \
                     ORDER BY created_at, id LIMIT $3 FOR UPDATE SKIP LOCKED"
                } else {
                    "SELECT id, attempt FROM work_items \
                     WHERE provider_id = $1 AND status = 'queued' AND visible_at <= $2 \
                     ORDER BY created_at, id LIMIT $3"
                };
                let candidates = tx
                    .query(
                        sql,
                        &[provider_id.as_str().into(), now.into(), batch.into()],
                    )
                    .await?;
                let mut work = Vec::new();
                for row in candidates {
                    let id = row.text("id")?.to_owned();
                    let attempt = row.u64("attempt")? + 1;
                    let updated = tx
                        .execute(
                            "UPDATE work_items SET status = 'inflight', attempt = $2, \
                             visible_at = $3, generation = $4 \
                             WHERE id = $1 AND status = 'queued'",
                            &[
                                id.as_str().into(),
                                attempt.into(),
                                (now + i64::try_from(delivery_timeout.as_millis()).unwrap_or(0))
                                    .into(),
                                generation.into(),
                            ],
                        )
                        .await?;
                    if updated != 1 {
                        continue;
                    }
                    let payload_row = tx
                        .query(
                            "SELECT payload_json, secret_sealed FROM work_items WHERE id = $1",
                            &[id.as_str().into()],
                        )
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| Error::internal("claimed work vanished"))?;
                    let mut payload: WorkPayload =
                        rows::json(payload_row.text("payload_json")?, "work payload")?;
                    if let Some(sealed) = payload_row.opt_bytes("secret_sealed")?
                        && let Some(work_item::Work::Exec(exec)) = &mut payload.work.work
                    {
                        let plain =
                            seal.unseal(sealed, rows::exec_aad(&exec.exec_id).as_bytes())?;
                        let env: StringMap =
                            rows::json(&String::from_utf8_lossy(&plain), "exec env")?;
                        if let Some(request) = exec.request.as_option_mut() {
                            request.env = env;
                        }
                    }
                    payload.work.id = id;
                    payload.work.attempt = attempt;
                    work.push(payload.work);
                }
                Ok(work)
            })
        })
        .await
}

async fn requeue_expired(tx: &mut dyn SqlTx, provider_id: &str, generation: u64) -> Result<()> {
    let expired = tx
        .query(
            "SELECT w.id, w.kind, w.payload_json, e.started, e.id AS exec_id \
             FROM work_items w LEFT JOIN execs e ON e.work_id = w.id \
             WHERE w.provider_id = $1 AND w.status = 'inflight' AND w.visible_at <= $2",
            &[provider_id.into(), now_ms().into()],
        )
        .await?;
    for row in expired {
        let id = row.text("id")?.to_owned();
        let kind = row.text("kind")?.to_owned();
        let started = row.opt_int("started")?.unwrap_or(0) != 0;
        if kind == "exec" && started {
            // A started command is never redelivered: its outcome is
            // ambiguous, so the caller gets an explicit failure.
            let exec_id = row
                .opt_text("exec_id")?
                .map(str::to_owned)
                .unwrap_or_default();
            complete_work(
                tx,
                &id,
                WorkResult::failed("provider stopped reporting an exec that had started"),
            )
            .await?;
            if !exec_id.is_empty() {
                super::commands_runtime::fail_exec_inner(
                    tx,
                    &exec_id,
                    "provider stopped reporting an exec that had started",
                )
                .await?;
            }
        } else {
            tx.execute(
                "UPDATE work_items SET status = 'queued', visible_at = 0, generation = $2 \
                 WHERE id = $1 AND status = 'inflight'",
                &[id.into(), generation.into()],
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn work_result(store: &SqlStore, work_id: &str) -> Result<Option<WorkResult>> {
    store
        .run_tx(|tx| {
            let work_id = work_id.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT result_json FROM work_items WHERE id = $1 AND status = 'done'",
                        &[work_id.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                row.map(|row| rows::json(row.text("result_json")?, "work result"))
                    .transpose()
            })
        })
        .await
}

pub(crate) async fn report(store: &SqlStore, request: ReportRequest) -> Result<()> {
    store
        .run_tx(|tx| {
            let request = request.clone();
            Box::pin(async move {
                let _ = fence_provider(tx, &request.provider_id, request.generation).await?;
                let (work_id, encoded_attempt) = logic::parse_delivery_id(&request.work_id);
                let attempt = if request.attempt != 0 {
                    Some(request.attempt)
                } else {
                    encoded_attempt
                };
                let row = tx
                    .query(
                        "SELECT provider_id, status, attempt FROM work_items WHERE id = $1",
                        &[work_id.into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::not_found("unknown or completed work"))?;
                if row.text("provider_id")? != request.provider_id {
                    return Err(Error::new(
                        Code::PermissionDenied,
                        "work belongs to another provider",
                    ));
                }
                if let Some(attempt) = attempt
                    && attempt != row.u64("attempt")?
                {
                    return Err(Error::aborted("stale work delivery attempt"));
                }
                if row.text("status")? == "done" {
                    // A retried positive attempt is an acknowledgement. An
                    // unsequenced duplicate is "unknown or completed" for
                    // idempotent commands, while an exec's success is already
                    // durable and acknowledging it stops redelivery.
                    if attempt.is_some() {
                        return Ok(());
                    }
                    let kind = tx
                        .query(
                            "SELECT kind FROM work_items WHERE id = $1",
                            &[work_id.into()],
                        )
                        .await?
                        .first()
                        .map(|row| row.text("kind").map(str::to_owned))
                        .transpose()?
                        .unwrap_or_default();
                    return if kind == "exec" {
                        Ok(())
                    } else {
                        Err(Error::not_found("unknown or completed work"))
                    };
                }
                let result = if request.ok {
                    WorkResult::ok(request.native_ref)
                } else {
                    WorkResult::failed(request.error)
                };
                complete_work(tx, work_id, result).await?;
                Ok(())
            })
        })
        .await
}

/// How long after a create completes an inventory report may lag behind it.
///
/// A provider's report lists the machines its driver sample contained a
/// moment earlier. The service compares that list with rows whose create
/// report it may have processed after the sample was taken, so a machine
/// created in that window is missing from a report that is otherwise
/// current — and failing it would fail a live sandbox. The window is one
/// round trip plus queueing; this bound is generous for that and short
/// enough that a machine that really vanished is still failed promptly.
const INVENTORY_GRACE_MS: i64 = 3_000;

/// Whether a record's create is recent enough that an inventory sample may
/// legitimately predate it. A record with no completed create work is not
/// fresh: absence is a real loss.
fn missing_within_grace(created_at_ms: i64, now_ms: i64) -> bool {
    created_at_ms > 0 && now_ms.saturating_sub(created_at_ms) < INVENTORY_GRACE_MS
}

pub(crate) async fn reconcile(
    store: &SqlStore,
    request: ReconcileRequest,
) -> Result<ReconcileResponse> {
    store
        .run_tx(|tx| {
            let request = request.clone();
            Box::pin(async move {
                let _ = fence_provider(tx, &request.provider_id, request.generation).await?;
                let observed: HashSet<&str> = request
                    .sandboxes
                    .iter()
                    .map(|item| item.native_ref.as_str())
                    .collect();
                let rows = tx
                    .query(
                        "SELECT s.id, s.native_ref, s.state, s.delete_attempts, p.driver, \
                                COALESCE((SELECT MAX(w.completed_at) FROM work_items w \
                                          WHERE w.dedup_key = 'create:' || s.id), 0) \
                                    AS created_at_ms \
                         FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.provider_id = $1 AND s.state <> 'failed'",
                        &[request.provider_id.as_str().into()],
                    )
                    .await?;
                let mut records = Vec::new();
                for row in rows {
                    records.push((
                        row.text("id")?.to_owned(),
                        row.text("native_ref")?.to_owned(),
                        row.text("state")?.to_owned(),
                        u32::try_from(row.int("delete_attempts")?).unwrap_or(0),
                        row.text("driver")?.to_owned(),
                        row.int("created_at_ms")?,
                    ));
                }
                // A record the provider no longer reports is done: terminated
                // and GONE tombstones are removed, anything else is failed.
                for (id, native_ref, state, _attempts, _driver, created_at_ms) in &records {
                    if native_ref.is_empty() || observed.contains(native_ref.as_str()) {
                        continue;
                    }
                    if state == "terminating" || state == "gone" {
                        purge_sandbox(tx, id).await?;
                    } else if missing_within_grace(*created_at_ms, now_ms()) {
                        // The inventory is a snapshot taken before it is sent,
                        // so a machine whose create landed in between is
                        // absent from a report that is otherwise current.
                        // Absence this soon after a create proves nothing;
                        // the next report decides.
                        tracing::debug!(sandbox = %id, "inventory reported no machine yet; in grace");
                    } else {
                        super::commands_leases::fail_sandbox(
                            tx,
                            id,
                            "lost: missing from provider inventory",
                        )
                        .await?;
                        tx.execute(
                            "UPDATE sandboxes SET native_ref = '' WHERE id = $1",
                            &[id.as_str().into()],
                        )
                        .await?;
                    }
                }
                for (id, native_ref, state, attempts, driver, _created_at_ms) in &records {
                    if state != "terminating"
                        || native_ref.is_empty()
                        || !observed.contains(native_ref.as_str())
                    {
                        continue;
                    }
                    if *attempts >= super::commands_runtime::MAX_DESTROY_ATTEMPTS {
                        // The machine is still reported after the retry
                        // budget: keep the tombstone (it still proves the
                        // machine is the service's) and let reconcile reclaim
                        // it once the provider stops reporting it.
                        let version = bump_version(tx).await?;
                        tx.execute(
                            "UPDATE sandboxes SET state = 'gone', lease_id = '', version = $2 \
                             WHERE id = $1",
                            &[id.as_str().into(), version.into()],
                        )
                        .await?;
                        continue;
                    }
                    // A destroy already queued or in flight is awaited, not
                    // asked again: a slow driver must not run two destroys.
                    let destroy_key = format!("destroy:{id}");
                    let requeue_key = format!("destroy-requeue:{id}");
                    let active = tx
                        .query(
                            "SELECT 1 AS present FROM work_items \
                             WHERE provider_id = $1 AND status <> 'done' \
                               AND dedup_key IN ($2, $3) LIMIT 1",
                            &[
                                request.provider_id.as_str().into(),
                                destroy_key.as_str().into(),
                                requeue_key.as_str().into(),
                            ],
                        )
                        .await?;
                    if !active.is_empty() {
                        continue;
                    }
                    queue_destroy_target(
                        tx,
                        &request.provider_id,
                        request.generation,
                        id,
                        driver,
                        native_ref,
                        &format!("destroy-requeue:{id}"),
                        now_ms() + 86_400_000,
                    )
                    .await?;
                }
                let bases = tx
                    .query(
                        "SELECT native_ref FROM templates WHERE native_ref <> '' \
                         UNION SELECT native_ref FROM snapshot_bases",
                        &[],
                    )
                    .await?
                    .into_iter()
                    .map(|row| row.text("native_ref").map(str::to_owned))
                    .collect::<Result<BTreeSet<_>>>()?;
                let all_records = tx
                    .query(
                        "SELECT s.id, s.provider_id, s.native_ref, s.state, s.name \
                         FROM sandboxes s",
                        &[],
                    )
                    .await?;
                // Machines no record owns. Only proof that this service
                // created a machine makes it reapable: its sandbox id is one
                // of ours, its caller-given name is a provisioning create of
                // this provider, or it is a tombstone's machine. Anything
                // else on the host belongs to another service sharing the
                // registry and is left alone.
                let mut destroy = Vec::new();
                for item in &request.sandboxes {
                    if bases.contains(&item.native_ref) {
                        continue;
                    }
                    let owned = all_records.iter().any(|row| {
                        row.text("provider_id").ok() == Some(request.provider_id.as_str())
                            && row.text("native_ref").ok() == Some(item.native_ref.as_str())
                            && row.text("state").ok() != Some("gone")
                    });
                    let ours = (!item.sandbox_id.is_empty()
                        && all_records
                            .iter()
                            .any(|row| row.text("id").ok() == Some(item.sandbox_id.as_str())))
                        || all_records.iter().any(|row| {
                            row.text("provider_id").ok() == Some(request.provider_id.as_str())
                                && row.text("state").ok() == Some("provisioning")
                                && !row.text("name").unwrap_or_default().is_empty()
                                && row.text("name").ok() == Some(item.native_ref.as_str())
                        })
                        || all_records.iter().any(|row| {
                            row.text("provider_id").ok() == Some(request.provider_id.as_str())
                                && row.text("state").ok() == Some("gone")
                                && row.text("native_ref").ok() == Some(item.native_ref.as_str())
                        });
                    // Another provider's record (live, or a tombstone) that
                    // names this machine: its reclaim belongs to that provider.
                    let foreign = !item.native_ref.is_empty()
                        && all_records.iter().any(|row| {
                            row.text("provider_id").ok() != Some(request.provider_id.as_str())
                                && row.text("native_ref").ok() == Some(item.native_ref.as_str())
                                && row.text("state").ok() != Some("failed")
                        });
                    if !owned && ours && !foreign {
                        destroy.push(SandboxWork {
                            sandbox_id: item.sandbox_id.clone(),
                            driver: item.driver.clone(),
                            native_ref: item.native_ref.clone(),
                            ..SandboxWork::default()
                        });
                    }
                }
                Ok(ReconcileResponse {
                    destroy,
                    ..ReconcileResponse::default()
                })
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_inventory_may_lag_a_create_only_inside_the_grace() {
        // No completed create work: absence is a real loss, not lag.
        assert!(!missing_within_grace(0, 1_000_000));
        // A create that landed a moment ago can be missing from a report
        // whose sample predates it.
        assert!(missing_within_grace(1_000_000, 1_000_000));
        assert!(missing_within_grace(1_000_000, 1_002_999));
        // Past the grace the report is authoritative.
        assert!(!missing_within_grace(1_000_000, 1_003_000));
        assert!(!missing_within_grace(1_000_000, 9_000_000));
        // A service clock behind the recorded completion still counts fresh.
        assert!(missing_within_grace(1_000_000, 999_000));
    }
}
