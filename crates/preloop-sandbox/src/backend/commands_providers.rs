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
use super::types::{Owner, WorkResult};

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
    if kind == "snapshot" && result.ok {
        let template = apply_snapshot_receipt(tx, &payload, &result).await?;
        result.template = Some(template);
    }
    let result_json = serde_json::to_string(&result).unwrap_or_default();
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE work_items SET status = 'done', result_json = $2, completed_at = $3, \
         secret_sealed = NULL WHERE id = $1",
        &[
            work_id.into(),
            result_json.as_str().into(),
            now_ms().into(),
        ],
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
            let sandbox_id = match &payload.work.work {
                Some(work_item::Work::Destroy(target)) => target.sandbox_id.clone(),
                _ => String::new(),
            };
            if !sandbox_id.is_empty() {
                purge_sandbox(tx, &sandbox_id).await?;
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
                sandbox.state = match kind.as_str() {
                    "pause" => SandboxState::Paused,
                    "stop" => SandboxState::Stopped,
                    "start" => {
                        if sandbox.lease_id.is_empty() {
                            SandboxState::Ready
                        } else {
                            SandboxState::Leased
                        }
                    }
                    "resume" => {
                        if sandbox.lease_id.is_empty() {
                            SandboxState::Preserved
                        } else {
                            SandboxState::Leased
                        }
                    }
                    _ => sandbox.state.known(),
                }
                .into();
                if kind == "start" {
                    sandbox.error.clear();
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

async fn apply_snapshot_receipt(
    tx: &mut dyn SqlTx,
    payload: &WorkPayload,
    result: &WorkResult,
) -> Result<Template> {
    let (sandbox_id, template_name) = match &payload.work.work {
        Some(work_item::Work::Snapshot(snapshot)) => {
            (snapshot.target.sandbox_id.clone(), snapshot.template.clone())
        }
        _ => return Err(Error::internal("snapshot work has no target")),
    };
    let source = payload.source_template.clone().unwrap_or_default();
    let spec = TemplateSpec {
        name: template_name.clone(),
        os: source.spec.os.clone(),
        source: Some(template_spec::Source::SnapshotOf(sandbox_id.clone())),
        requires: source.spec.requires.clone(),
        labels: source.spec.labels.clone(),
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
    let provider_id = match &payload.work.work {
        Some(work_item::Work::Snapshot(snapshot)) => {
            // The source sandbox's provider; read before it is removed.
            tx.query(
                "SELECT provider_id FROM sandboxes WHERE id = $1",
                &[snapshot.target.sandbox_id.as_str().into()],
            )
            .await?
            .first()
            .map(|row| row.text("provider_id").map(str::to_owned))
            .transpose()?
            .unwrap_or_default()
        }
        _ => String::new(),
    };
    let driver = match &payload.work.work {
        Some(work_item::Work::Snapshot(snapshot)) => snapshot.target.driver.clone(),
        _ => String::new(),
    };
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
    // The source sandbox is consumed by the snapshot.
    purge_sandbox(tx, &sandbox_id).await?;
    Ok(Template {
        spec: spec.into(),
        digest,
        provider_id,
        native_ref: result.native_ref.clone(),
        created_at: now_secs(),
        driver,
        ..Template::default()
    })
}

/// Delete a sandbox record and everything that could still act on it.
pub(crate) async fn purge_sandbox(tx: &mut dyn SqlTx, sandbox_id: &str) -> Result<()> {
    let leases = tx
        .query(
            "SELECT id, idempotency_key FROM leases WHERE sandbox_id = $1",
            &[sandbox_id.into()],
        )
        .await?;
    for row in &leases {
        let lease_id = row.text("id")?.to_owned();
        let key = row.text("idempotency_key")?.to_owned();
        tx.execute("DELETE FROM leases WHERE id = $1", &[lease_id.as_str().into()])
            .await?;
        if !key.is_empty() {
            tx.execute("DELETE FROM lease_keys WHERE key = $1", &[key.into()])
                .await?;
        }
    }
    tx.execute(
        "UPDATE execs SET status = 'failed', error = 'sandbox was destroyed' \
         WHERE sandbox_id = $1 AND status = 'running'",
        &[sandbox_id.into()],
    )
    .await?;
    tx.execute(
        "UPDATE tickets SET status = 'aborted', reason = 'sandbox was destroyed' \
         WHERE sandbox_id = $1 AND status IN ('open', 'ready')",
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
                        Error::new(
                            Code::PermissionDenied,
                            "client certificate is not enrolled",
                        )
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
                    .unwrap_or_else(|| format!("prv-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]));
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
                let host = request.host.into_option().map(|host| {
                    serde_json::to_string(&host).unwrap_or_default()
                });
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

pub(crate) async fn drain(
    store: &SqlStore,
    reference: &str,
) -> Result<DrainProviderResult> {
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
                let provider = provider_by_name(tx, &name)
                    .await?
                    .ok_or_else(|| Error::not_found(format!("provider `{name}` is not registered")))?;
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
                tx.execute("DELETE FROM providers WHERE id = $1", &[provider.id.as_str().into()])
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
                for row in rows.iter().take(usize::try_from(limit).unwrap_or(usize::MAX)) {
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
) -> Result<()> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            let hash = secret_sha256.to_owned();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO join_tokens (id, secret_sha256, expires_at) \
                     VALUES ($1, $2, $3)",
                    &[id.into(), hash.into(), expires_at.into()],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn enroll(
    store: &SqlStore,
    token_id: &str,
    secret_sha256: &str,
    name: &str,
    cert_sha256: &str,
    cert_expires_at: i64,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let token_id = token_id.to_owned();
            let secret = secret_sha256.to_owned();
            let name = name.to_owned();
            let cert = cert_sha256.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT id, secret_sha256, expires_at FROM join_tokens WHERE id = $1",
                        &[token_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        Error::new(Code::Unauthenticated, "unknown or used join token")
                    })?;
                if row.int("expires_at")? <= now_secs() {
                    return Err(Error::new(Code::Unauthenticated, "join token expired"));
                }
                if !logic::constant_time_eq(row.text("secret_sha256")?.as_bytes(), secret.as_bytes())
                {
                    return Err(Error::new(Code::Unauthenticated, "bad join token"));
                }
                let deleted = tx
                    .execute(
                        "DELETE FROM join_tokens WHERE id = $1 AND secret_sha256 = $2",
                        &[token_id.as_str().into(), secret.as_str().into()],
                    )
                    .await?;
                if deleted != 1 {
                    return Err(Error::new(Code::Unauthenticated, "join token was used"));
                }
                let existing = provider_by_name(tx, &name).await?;
                let id = existing
                    .as_ref()
                    .map(|provider| provider.id.clone())
                    .unwrap_or_else(|| {
                        format!("prv-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
                    });
                let generation = existing
                    .as_ref()
                    .map_or(0, |provider| provider.generation);
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
                // Re-enrolling replaces earlier credentials.
                tx.execute(
                    "DELETE FROM provider_certificates WHERE provider_id = $1",
                    &[id.as_str().into()],
                )
                .await?;
                tx.execute(
                    "INSERT INTO provider_certificates (provider_id, sha256, expires_at) \
                     VALUES ($1, $2, $3)",
                    &[id.as_str().into(), cert.as_str().into(), cert_expires_at.into()],
                )
                .await?;
                outbox(tx, "provider", &id, version, "enroll", "{}").await?;
                Ok(())
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
                    &[id.as_str().into(), cert.as_str().into(), cert_expires_at.into()],
                )
                .await?;
                tx.execute(
                    "DELETE FROM provider_certificates WHERE provider_id = $1 AND expires_at <= $2",
                    &[id.as_str().into(), now_secs().into()],
                )
                .await?;
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
                    .query(sql, &[provider_id.as_str().into(), now.into(), batch.into()])
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
                    if let Some(sealed) = payload_row.opt_bytes("secret_sealed")? {
                        if let Some(work_item::Work::Exec(exec)) = &mut payload.work.work {
                            let plain =
                                seal.unseal(sealed, rows::exec_aad(&exec.exec_id).as_bytes())?;
                            let env: StringMap = rows::json(
                                &String::from_utf8_lossy(&plain),
                                "exec env",
                            )?;
                            if let Some(request) = exec.request.as_option_mut() {
                                request.env = env;
                            }
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

async fn requeue_expired(
    tx: &mut dyn SqlTx,
    provider_id: &str,
    generation: u64,
) -> Result<()> {
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
                row.map(|row| {
                    rows::json(
                        row.text("result_json")?,
                        "work result",
                    )
                })
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
                        .query("SELECT kind FROM work_items WHERE id = $1", &[work_id.into()])
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
                        "SELECT s.id, s.native_ref, s.state, s.delete_attempts, p.driver \
                         FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.provider_id = $1 AND s.state NOT IN ('gone', 'failed')",
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
                    ));
                }
                for (id, native_ref, state, _attempts, _driver) in &records {
                    if native_ref.is_empty() || observed.contains(native_ref.as_str()) {
                        continue;
                    }
                    if state == "terminating" {
                        purge_sandbox(tx, id).await?;
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
                for (id, native_ref, state, attempts, driver) in &records {
                    if state != "terminating"
                        || native_ref.is_empty()
                        || !observed.contains(native_ref.as_str())
                    {
                        continue;
                    }
                    if *attempts >= super::commands_runtime::MAX_DESTROY_ATTEMPTS {
                        purge_sandbox(tx, id).await?;
                        continue;
                    }
                    tx.execute(
                        "UPDATE sandboxes SET delete_attempts = delete_attempts + 1 \
                         WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?;
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
                let mut destroy = Vec::new();
                for item in &request.sandboxes {
                    if bases.contains(&item.native_ref) {
                        continue;
                    }
                    let owned = all_records.iter().any(|row| {
                        row.text("provider_id").ok() == Some(request.provider_id.as_str())
                            && row.text("native_ref").ok() == Some(item.native_ref.as_str())
                    });
                    let provisioning = (!item.sandbox_id.is_empty()
                        && all_records.iter().any(|row| {
                            row.text("id").ok() == Some(item.sandbox_id.as_str())
                                && row.text("state").ok() == Some("provisioning")
                        }))
                        || all_records.iter().any(|row| {
                            row.text("provider_id").ok() == Some(request.provider_id.as_str())
                                && row.text("state").ok() == Some("provisioning")
                                && !row.text("name").unwrap_or_default().is_empty()
                                && row.text("name").ok() == Some(item.native_ref.as_str())
                        });
                    let foreign = all_records.iter().any(|row| {
                        row.text("provider_id").ok() != Some(request.provider_id.as_str())
                            && row.text("state").ok() != Some("gone")
                            && row.text("state").ok() != Some("failed")
                            && (row.text("native_ref").ok() == Some(item.native_ref.as_str())
                                || (!item.sandbox_id.is_empty()
                                    && row.text("id").ok() == Some(item.sandbox_id.as_str())))
                    });
                    if !owned && !provisioning && !foreign {
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
