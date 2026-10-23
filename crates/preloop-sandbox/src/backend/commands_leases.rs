//! Acquire, leases, injection claims and sandbox lifecycle transitions.

use std::time::Duration;

use crate::api::*;
use crate::error::{Code, Error, Result};

use super::backend::Waiter;
use super::engine::SqlTx;
use super::logic;
use super::rows;
use super::store::{
    SqlStore, active_lease, bump_version, lease_by_id, lease_row, now_ms, now_secs, outbox,
    provider_by_id, sandbox_by_id, sandbox_version,
};
use super::commands_providers::purge_sandbox;
use super::types::{
    AcquireCommand, AcquireStep, NewSandbox, Owner, RefillOutcome,
};

/// Insert one immutable spec plus its mutable sandbox row and create work.
/// `None` means capacity or the pool revision moved; the caller re-evaluates.
pub(crate) async fn create_member(
    seal: &super::seal::SealKey,
    tx: &mut dyn SqlTx,
    member: &NewSandbox,
    create_timeout: Duration,
    pool_fill: bool,
) -> Result<Option<String>> {
    logic::validate_new_sandbox(member)?;
    // Lock the provider row so slot checks serialize per provider.
    let lock = if tx.dialect() == super::engine::Dialect::Postgres {
        "SELECT id FROM providers WHERE id = $1 FOR UPDATE"
    } else {
        "SELECT id FROM providers WHERE id = $1"
    };
    let provider_row = tx
        .query(lock, &[member.provider_id.as_str().into()])
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            Error::unavailable(format!("provider {} is gone", member.provider_id))
        })?;
    let _ = provider_row;
    let provider = provider_by_id(tx, &member.provider_id)
        .await?
        .ok_or_else(|| logic::provider_gone(&member.provider_id))?;
    let driver = logic::provider_driver(&provider, &member.driver)
        .ok_or_else(|| {
            Error::failed_precondition(format!(
                "provider `{}` no longer advertises driver `{}`",
                provider.name, member.driver
            ))
        })?;
    if !logic::fits_shape(&driver.capabilities, &member.shape) {
        return Ok(None);
    }
    let live = tx
        .query(
            "SELECT COUNT(*) AS live FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
             WHERE s.provider_id = $1 AND p.driver = $2 \
               AND s.state NOT IN ('gone', 'failed', 'terminating')",
            &[member.provider_id.as_str().into(), member.driver.as_str().into()],
        )
        .await?
        .first()
        .map(|row| row.int("live"))
        .transpose()?
        .unwrap_or(0);
    if driver.capabilities.slots > 0 && live >= i64::from(driver.capabilities.slots) {
        return Ok(None);
    }
    if !member.pool.is_empty() {
        let pool = tx
            .query(
                "SELECT revision, warm, max_members FROM pools WHERE name = $1",
                &[member.pool.as_str().into()],
            )
            .await?
            .into_iter()
            .next();
        let Some(pool) = pool else {
            return Ok(None);
        };
        if pool.u64("revision")? != member.pool_revision {
            return Ok(None);
        }
        let live_in_pool = tx
            .query(
                "SELECT COUNT(*) AS live FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                 WHERE p.pool = $1 AND s.state NOT IN ('gone', 'failed', 'terminating')",
                &[member.pool.as_str().into()],
            )
            .await?
            .first()
            .map(|row| row.int("live"))
            .transpose()?
            .unwrap_or(0);
        let max = pool.int("max_members")?;
        if max > 0 && live_in_pool >= max {
            return Ok(None);
        }
        if pool_fill {
            let warm = pool.int("warm")?;
            let idle = tx
                .query(
                    "SELECT COUNT(*) AS idle FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                     WHERE p.pool = $1 AND s.lease_id = '' \
                       AND s.state IN ('ready', 'provisioning')",
                    &[member.pool.as_str().into()],
                )
                .await?
                .first()
                .map(|row| row.int("idle"))
                .transpose()?
                .unwrap_or(0);
            if idle >= warm {
                return Ok(None);
            }
        }
    }
    if !member.name.is_empty() {
        let conflict = tx
            .query(
                "SELECT 1 AS present FROM sandboxes \
                 WHERE name = $1 AND state NOT IN ('gone', 'failed') LIMIT 1",
                &[member.name.as_str().into()],
            )
            .await?;
        if !conflict.is_empty() {
            return Err(Error::new(
                Code::AlreadyExists,
                format!("a live sandbox is already named `{}`", member.name),
            ));
        }
    }
    let sandbox_id = format!("sbx-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let shape = logic::effective_shape(
        Some(&driver.capabilities),
        &member.shape,
    );
    let spec_json = serde_json::to_string(&member.template).unwrap_or_default();
    let shape_json = serde_json::to_string(&shape).unwrap_or_default();
    let network_json = serde_json::to_string(&member.network).unwrap_or_default();
    let mounts_json = serde_json::to_string(&member.mounts).unwrap_or_default();
    let sockets_json = serde_json::to_string(&member.sockets).unwrap_or_default();
    let options_json = serde_json::to_string(&member.options).unwrap_or_default();
    let requires_json = serde_json::to_string(&member.requires).unwrap_or_default();
    let version = bump_version(tx).await?;
    tx.execute(
        "INSERT INTO sandbox_specs \
         (id, template_json, shape_json, network_json, mounts_json, sockets_json, options_json, \
          requires_json, snapshot_capable, purpose, pool, pool_revision, driver, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        &[
            sandbox_id.as_str().into(),
            spec_json.as_str().into(),
            shape_json.as_str().into(),
            network_json.as_str().into(),
            mounts_json.as_str().into(),
            sockets_json.as_str().into(),
            options_json.as_str().into(),
            requires_json.as_str().into(),
            member.snapshot_capable.into(),
            rows::purpose_label(member.purpose).into(),
            member.pool.as_str().into(),
            i64::try_from(member.pool_revision).unwrap_or(i64::MAX).into(),
            member.driver.as_str().into(),
            now_secs().into(),
        ],
    )
    .await?;
    tx.execute(
        "INSERT INTO sandboxes \
         (id, name, state, lease_id, native_ref, error, provider_id, created_at, \
          preserve_until, delete_attempts, version) \
         VALUES ($1, $2, 'provisioning', '', '', '', $3, $4, 0, 0, $5)",
        &[
            sandbox_id.as_str().into(),
            member.name.as_str().into(),
            member.provider_id.as_str().into(),
            now_secs().into(),
            version.into(),
        ],
    )
    .await?;
    let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let create = CreateWork {
        sandbox_id: sandbox_id.clone(),
        driver: member.driver.clone(),
        template: member.template.clone().into(),
        shape: member.shape.clone().into(),
        network: member.network.clone().into(),
        mounts: member.mounts.clone(),
        sockets: member.sockets.clone(),
        options: member.options.clone(),
        snapshot_capable: member.snapshot_capable,
        name: member.name.clone(),
        ..CreateWork::default()
    };
    let work = WorkItem::new(work_id, work_item::Work::Create(create.into()));
    let payload = super::commands_providers::WorkPayload {
        work: work.clone(),
        source_template: Some(member.template.clone()),
        ..Default::default()
    };
    super::commands_providers::insert_work(
        tx,
        Some(seal),
        &member.provider_id,
        provider.generation,
        &work,
        "create",
        &payload,
        &logic::create_dedup_key(&sandbox_id),
        now_ms(),
        now_ms() + i64::try_from(create_timeout.as_millis()).unwrap_or(0),
    )
    .await?;
    outbox(tx, "sandbox", &sandbox_id, version, "create", "{}").await?;
    Ok(Some(sandbox_id))
}

pub(crate) async fn create_pool_member(
    store: &SqlStore,
    member: NewSandbox,
    create_timeout: Duration,
) -> Result<RefillOutcome> {
    let seal = store.seal.clone();
    store
        .run_tx(|tx| {
            let member = member.clone();
            let seal = seal.clone();
            Box::pin(async move {
                match create_member(&seal, tx, &member, create_timeout, true).await? {
                    Some(id) => {
                        let sandbox = sandbox_by_id(tx, &id)
                            .await?
                            .ok_or_else(|| Error::internal("created sandbox vanished"))?;
                        Ok(RefillOutcome::Created(sandbox))
                    }
                    None => Ok(RefillOutcome::NotNeeded),
                }
            })
        })
        .await
}

pub(crate) async fn acquire(
    store: &SqlStore,
    command: AcquireCommand,
) -> Result<AcquireStep> {
    if command.request.idempotency_key.is_empty() {
        return Err(Error::invalid_argument("idempotencyKey is required"));
    }
    if command.request.pool.is_empty() == command.request.template.is_empty() {
        return Err(Error::invalid_argument(
            "exactly one of pool or template is required",
        ));
    }
    let request = command.request.clone();
    let fingerprint = command.fingerprint.clone();
    let ttl = command.ttl_secs.max(1);
    let seal = store.seal.clone();
    store
        .run_tx(|tx| {
            let seal = seal.clone();
            let request = request.clone();
            let fingerprint = fingerprint.clone();
            let owner = command.owner.clone();
            let placement = command.placement.clone();
            Box::pin(async move {
                if let Some(row) = tx
                    .query(
                        "SELECT id, sandbox_id, pool, idempotency_key, expires_at, env_sealed, \
                                request_hash, inject_state, inject_owner_node, \
                                inject_owner_session, inject_claim_expires_at \
                         FROM leases WHERE idempotency_key = $1",
                        &[request.idempotency_key.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                {
                    let lease_id = row.text("id")?.to_owned();
                    let sandbox_id = row.text("sandbox_id")?.to_owned();
                    let stored = row.text("request_hash")?.to_owned();
                    if !stored.is_empty() && stored != fingerprint {
                        return Ok(AcquireStep::Failed(logic::idempotency_conflict(
                            &request.idempotency_key,
                        )));
                    }
                    let Some(mut sandbox) = sandbox_by_id(tx, &sandbox_id).await? else {
                        release_lease(tx, &lease_id, ReleaseMode::Destroy, 0, 0).await?;
                        return Ok(AcquireStep::Failed(Error::not_found(
                            "leased sandbox is gone",
                        )));
                    };
                    let state = sandbox.state.known();
                    match state {
                        SandboxState::Leased
                        | SandboxState::Paused
                        | SandboxState::Preserved
                        | SandboxState::Ready => {
                            let lease = lease_by_id(tx, &lease_id, &seal)
                                .await?
                                .ok_or_else(|| Error::not_found("lease not found"))?;
                            let inject_state = row.text("inject_state")?.to_owned();
                            if inject_state == "done" {
                                let lease = deliver_lease(tx, &lease_id, &seal).await?;
                                return Ok(AcquireStep::Ready { lease, sandbox });
                            }
                            if claim_injection(tx, &lease_id, &owner, Duration::from_secs(60))
                                .await?
                            {
                                return Ok(AcquireStep::Inject { lease, sandbox });
                            }
                            return Ok(AcquireStep::Wait {
                                last: None,
                                retry_after: Duration::from_millis(100),
                                sandbox_id: Some(sandbox.id),
                                lease_id: Some(lease_id),
                            });
                        }
                        SandboxState::Provisioning => {
                            let lease = lease_by_id(tx, &lease_id, &seal)
                                .await?
                                .ok_or_else(|| Error::not_found("lease not found"))?;
                            // Files are uploaded only after the machine
                            // exists: the copy work must see a native ref.
                            let _ = lease;
                            return Ok(AcquireStep::Wait {
                                last: None,
                                retry_after: Duration::from_millis(100),
                                sandbox_id: Some(sandbox.id),
                                lease_id: Some(lease_id),
                            });
                        }
                        _ => {
                            let error = if sandbox.error.is_empty() {
                                Error::failed_precondition(format!(
                                    "sandbox {} is {:?}",
                                    sandbox.id, state
                                ))
                            } else {
                                crate::error::from_reported(sandbox.error.clone())
                            };
                            release_lease(tx, &lease_id, ReleaseMode::Destroy, 0, 0).await?;
                            return Ok(AcquireStep::Failed(error));
                        }
                    }
                }
                // Reserve the key before names or capacity: exactly one
                // acquire wins it; the rest wait for its outcome.
                let reserved = tx
                    .execute(
                        "INSERT INTO lease_keys (key, lease_id, created_at) \
                         VALUES ($1, '', $2) ON CONFLICT (key) DO NOTHING",
                        &[request.idempotency_key.as_str().into(), now_ms().into()],
                    )
                    .await?;
                if reserved != 1 {
                    return Ok(AcquireStep::Wait {
                        last: None,
                        retry_after: Duration::from_millis(50),
                        sandbox_id: None,
                        lease_id: None,
                    });
                }
                let pool = if request.pool.is_empty() {
                    None
                } else {
                    let row = tx
                        .query(
                            "SELECT name, revision, warm, max_members, lease_ttl_secs \
                             FROM pools WHERE name = $1",
                            &[request.pool.as_str().into()],
                        )
                        .await?
                        .into_iter()
                        .next();
                    match row {
                        Some(row) => Some(row),
                        None => {
                            release_key(tx, &request.idempotency_key).await?;
                            return Ok(AcquireStep::Failed(Error::not_found(format!(
                                "pool `{}` not found",
                                request.pool
                            ))));
                        }
                    }
                };
                let ttl_secs = if request.ttl_secs > 0 {
                    request.ttl_secs
                } else {
                    pool.as_ref()
                        .map(|pool| pool.int("lease_ttl_secs"))
                        .transpose()?
                        .filter(|ttl| *ttl > 0)
                        .map(|ttl| u64::try_from(ttl).unwrap_or(0))
                        .unwrap_or(ttl)
                }
                .max(1);
                // Until the caller actually receives the lease, its expiry
                // is capped: provisioning may legitimately outlast a short
                // requested TTL, and the caller must not lose a machine it
                // is still waiting for.
                let unclaimed_ttl = command.unclaimed_ttl_secs.max(1);
                let initial_ttl = ttl_secs.min(unclaimed_ttl);
                let sandbox_id = if let Some(pool) = &pool {
                    let pool_name = pool.text("name")?.to_owned();
                    let revision = pool.u64("revision")?;
                    let warm = find_warm_member(
                        tx,
                        &pool_name,
                        revision,
                        &request.requires,
                    )
                    .await?;
                    match warm {
                        Some(id) => {
                            let claim_version = bump_version(tx).await?;
                            let claimed = tx
                                .execute(
                                    "UPDATE sandboxes SET lease_id = $2, state = 'leased', \
                                     version = $3 WHERE id = $1 AND lease_id = '' \
                                     AND state = 'ready'",
                                    &[
                                        id.as_str().into(),
                                        format!("pending-{}", request.idempotency_key).into(),
                                        claim_version.into(),
                                    ],
                                )
                                .await?;
                            if claimed != 1 {
                                release_key(tx, &request.idempotency_key).await?;
                                return Ok(AcquireStep::Wait {
                                    last: None,
                                    retry_after: Duration::from_millis(25),
                                    sandbox_id: None,
                                    lease_id: None,
                                });
                            }
                            id
                        }
                        None => {
                            if provisioning_member(
                                tx,
                                &pool_name,
                                revision,
                                &request.requires,
                            )
                            .await?
                            {
                                release_key(tx, &request.idempotency_key).await?;
                                return Ok(AcquireStep::Wait {
                                    last: None,
                                    retry_after: Duration::from_millis(100),
                                    sandbox_id: None,
                                    lease_id: None,
                                });
                            }
                            let live = tx
                                .query(
                                    "SELECT COUNT(*) AS live FROM sandboxes s \
                                     JOIN sandbox_specs p ON p.id = s.id \
                                     WHERE p.pool = $1 AND s.state NOT IN ('gone', 'failed')",
                                    &[pool_name.as_str().into()],
                                )
                                .await?
                                .first()
                                .map(|row| row.int("live"))
                                .transpose()?
                                .unwrap_or(0);
                            let max = pool.int("max_members")?;
                            if max > 0 && live >= max {
                                release_key(tx, &request.idempotency_key).await?;
                                return Ok(AcquireStep::Wait {
                                    last: Some(Error::new(
                                        Code::ResourceExhausted,
                                        format!("pool `{pool_name}` is at max ({max})"),
                                    )),
                                    retry_after: Duration::from_millis(250),
                                    sandbox_id: None,
                                    lease_id: None,
                                });
                            }
                            let Some(member) = placement.clone() else {
                                release_key(tx, &request.idempotency_key).await?;
                                return Ok(AcquireStep::PlacementNeeded);
                            };
                            if member.pool != pool_name || member.pool_revision != revision {
                                release_key(tx, &request.idempotency_key).await?;
                                return Ok(AcquireStep::PlacementNeeded);
                            }
                            match create_member(&seal, tx, &member, command.create_timeout, false)
                                .await
                            {
                                Ok(Some(id)) => id,
                                Ok(None) => {
                                    release_key(tx, &request.idempotency_key).await?;
                                    return Ok(AcquireStep::Wait {
                                        last: Some(Error::new(
                                            Code::ResourceExhausted,
                                            "every matching provider is at capacity",
                                        )),
                                        retry_after: Duration::from_millis(250),
                                        sandbox_id: None,
                                        lease_id: None,
                                    });
                                }
                                Err(error) => {
                                    release_key(tx, &request.idempotency_key).await?;
                                    return Ok(AcquireStep::Failed(error));
                                }
                            }
                        }
                    }
                } else {
                    let template = tx
                        .query(
                            "SELECT name FROM templates WHERE name = $1 AND retired = FALSE",
                            &[request.template.as_str().into()],
                        )
                        .await?
                        .into_iter()
                        .next();
                    if template.is_none() {
                        release_key(tx, &request.idempotency_key).await?;
                        return Ok(AcquireStep::Failed(Error::not_found(format!(
                            "template `{}` not found",
                            request.template
                        ))));
                    }
                    let Some(member) = placement.clone() else {
                        release_key(tx, &request.idempotency_key).await?;
                        return Ok(AcquireStep::PlacementNeeded);
                    };
                    match create_member(&seal, tx, &member, command.create_timeout, false).await {
                        Ok(Some(id)) => id,
                        Ok(None) => {
                            release_key(tx, &request.idempotency_key).await?;
                            return Ok(AcquireStep::Wait {
                                last: Some(Error::new(
                                    Code::ResourceExhausted,
                                    "every matching provider is at capacity",
                                )),
                                retry_after: Duration::from_millis(250),
                                sandbox_id: None,
                                lease_id: None,
                            });
                        }
                        Err(error) => {
                            release_key(tx, &request.idempotency_key).await?;
                            return Ok(AcquireStep::Failed(error));
                        }
                    }
                };
                // The sandbox may have been claimed by a racing acquire with
                // a different key; the lease-key reservation prevents the same
                // key, and the live-name/lease constraints fence the rest.
                let lease_id = format!("lse-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
                let env_sealed = seal_env(&seal, &lease_id, &request.inject.env).await?;
                let inject_state = if request.inject.files.is_empty() {
                    "done"
                } else {
                    "pending"
                };
                let lease_version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO leases \
                     (id, sandbox_id, pool, idempotency_key, expires_at, requested_ttl_secs, \
                      env_sealed, request_hash, inject_state, inject_owner_node, \
                      inject_owner_session, inject_claim_expires_at, version) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '', '', 0, $10)",
                    &[
                        lease_id.as_str().into(),
                        sandbox_id.as_str().into(),
                        request.pool.as_str().into(),
                        request.idempotency_key.as_str().into(),
                        (now_secs() + i64::try_from(initial_ttl).unwrap_or(i64::MAX)).into(),
                        ttl_secs.into(),
                        env_sealed.into(),
                        fingerprint.as_str().into(),
                        inject_state.into(),
                        lease_version.into(),
                    ],
                )
                .await?;
                tx.execute(
                    "UPDATE lease_keys SET lease_id = $2 WHERE key = $1",
                    &[
                        request.idempotency_key.as_str().into(),
                        lease_id.as_str().into(),
                    ],
                )
                .await?;
                let sandbox_version = bump_version(tx).await?;
                tx.execute(
                    "UPDATE sandboxes SET lease_id = $2, name = $3, \
                     state = CASE WHEN state = 'ready' THEN 'leased' ELSE state END, \
                     version = $4 WHERE id = $1",
                    &[
                        sandbox_id.as_str().into(),
                        lease_id.as_str().into(),
                        request.name.as_str().into(),
                        sandbox_version.into(),
                    ],
                )
                .await?;
                tx.execute(
                    "UPDATE sandbox_specs SET purpose = $2 WHERE id = $1",
                    &[
                        sandbox_id.as_str().into(),
                        rows::purpose_label(
                            request.purpose.as_known().unwrap_or(Purpose::PurposeUnspecified),
                        )
                        .into(),
                    ],
                )
                .await?;
                outbox(tx, "lease", &lease_id, lease_version, "acquire", "{}").await?;
                let lease = lease_by_id(tx, &lease_id, &seal)
                    .await?
                    .ok_or_else(|| Error::internal("inserted lease vanished"))?;
                let sandbox = sandbox_by_id(tx, &sandbox_id)
                    .await?
                    .ok_or_else(|| Error::internal("inserted sandbox vanished"))?;
                if inject_state == "pending"
                    && sandbox.state.known() != SandboxState::Provisioning
                {
                    if claim_injection(tx, &lease_id, &owner, Duration::from_secs(60)).await? {
                        Ok(AcquireStep::Inject { lease, sandbox })
                    } else {
                        Ok(AcquireStep::Wait {
                            last: None,
                            retry_after: Duration::from_millis(100),
                            sandbox_id: Some(sandbox_id),
                            lease_id: Some(lease_id),
                        })
                    }
                } else if sandbox.state.known() == SandboxState::Leased {
                    let lease = deliver_lease(tx, &lease_id, &seal).await?;
                    Ok(AcquireStep::Ready { lease, sandbox })
                } else {
                    Ok(AcquireStep::Wait {
                        last: None,
                        retry_after: Duration::from_millis(100),
                        sandbox_id: Some(sandbox_id),
                        lease_id: Some(lease_id),
                    })
                }
            })
        })
        .await
}

async fn release_key(tx: &mut dyn SqlTx, key: &str) -> Result<()> {
    tx.execute("DELETE FROM lease_keys WHERE key = $1", &[key.into()])
        .await?;
    Ok(())
}

/// Mark a lease delivered: its expiry becomes the caller's full requested
/// TTL. Until this point the expiry is capped so provisioning cannot outlast
/// a short TTL before the caller ever sees the lease.
async fn deliver_lease(
    tx: &mut dyn SqlTx,
    lease_id: &str,
    seal: &super::seal::SealKey,
) -> Result<Lease> {
    let row = tx
        .query(
            "SELECT requested_ttl_secs FROM leases WHERE id = $1",
            &[lease_id.into()],
        )
        .await?
        .into_iter()
        .next();
    let requested = row.map(|row| row.u64("requested_ttl_secs")).transpose()?.unwrap_or(0);
    if requested > 0 {
        let version = bump_version(tx).await?;
        tx.execute(
            "UPDATE leases SET expires_at = $2 + requested_ttl_secs, version = $3 \
             WHERE id = $1",
            &[lease_id.into(), now_secs().into(), version.into()],
        )
        .await?;
        outbox(tx, "lease", lease_id, version, "deliver", "{}").await?;
    }
    lease_by_id(tx, lease_id, seal)
        .await?
        .ok_or_else(|| Error::not_found("lease not found"))
}

async fn seal_env(
    seal: &super::seal::SealKey,
    lease_id: &str,
    env: &StringMap,
) -> Result<Option<Vec<u8>>> {
    if env.is_empty() {
        return Ok(None);
    }
    let bytes = serde_json::to_vec(env).unwrap_or_default();
    Ok(Some(seal.seal(&bytes, rows::lease_aad(lease_id).as_bytes())?))
}

async fn find_warm_member(
    tx: &mut dyn SqlTx,
    pool: &str,
    revision: u64,
    requires: &Requirements,
) -> Result<Option<String>> {
    let rows = tx
        .query(
            "SELECT s.id, s.provider_id, p.driver FROM sandboxes s \
             JOIN sandbox_specs p ON p.id = s.id \
             WHERE p.pool = $1 AND p.pool_revision = $2 AND s.lease_id = '' \
               AND s.state = 'ready' ORDER BY s.created_at, s.id",
            &[pool.into(), i64::try_from(revision).unwrap_or(i64::MAX).into()],
        )
        .await?;
    for row in rows {
        let provider = provider_by_id(tx, row.text("provider_id")?).await?;
        if logic::sandbox_meets(provider.as_ref(), row.text("driver")?, requires) {
            return Ok(Some(row.text("id")?.to_owned()));
        }
    }
    Ok(None)
}

async fn provisioning_member(
    tx: &mut dyn SqlTx,
    pool: &str,
    revision: u64,
    requires: &Requirements,
) -> Result<bool> {
    let rows = tx
        .query(
            "SELECT s.id, s.provider_id, p.driver FROM sandboxes s \
             JOIN sandbox_specs p ON p.id = s.id \
             WHERE p.pool = $1 AND p.pool_revision = $2 AND s.lease_id = '' \
               AND s.state = 'provisioning'",
            &[pool.into(), i64::try_from(revision).unwrap_or(i64::MAX).into()],
        )
        .await?;
    for row in rows {
        let provider = provider_by_id(tx, row.text("provider_id")?).await?;
        if logic::sandbox_meets(provider.as_ref(), row.text("driver")?, requires) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn claim_injection(
    tx: &mut dyn SqlTx,
    lease_id: &str,
    owner: &Owner,
    timeout: Duration,
) -> Result<bool> {
    let changed = tx
        .execute(
            "UPDATE leases SET inject_state = 'claimed', inject_owner_node = $2, \
             inject_owner_session = $3, inject_claim_expires_at = $4 \
             WHERE id = $1 AND (inject_state = 'pending' OR \
             (inject_state = 'claimed' AND inject_claim_expires_at <= $5))",
            &[
                lease_id.into(),
                owner.node_id.as_str().into(),
                owner.session_id.as_str().into(),
                (now_ms() + i64::try_from(timeout.as_millis()).unwrap_or(0)).into(),
                now_ms().into(),
            ],
        )
        .await?;
    Ok(changed == 1)
}

pub(crate) async fn finish_injection(
    store: &SqlStore,
    lease_id: &str,
    owner: &Owner,
    ok: bool,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let lease_id = lease_id.to_owned();
            let owner = owner.clone();
            Box::pin(async move {
                let state = if ok { "done" } else { "pending" };
                let (owner_node, owner_session) = if ok {
                    (
                        owner.node_id.clone(),
                        owner.session_id.clone(),
                    )
                } else {
                    (String::new(), String::new())
                };
                let changed = tx
                    .execute(
                        "UPDATE leases SET inject_state = $2, inject_owner_node = $3, \
                         inject_owner_session = $4, inject_claim_expires_at = 0 \
                         WHERE id = $1 AND inject_owner_node = $5 AND inject_owner_session = $6 \
                           AND inject_state = 'claimed'",
                        &[
                            lease_id.as_str().into(),
                            state.into(),
                            owner_node.as_str().into(),
                            owner_session.as_str().into(),
                            owner.node_id.as_str().into(),
                            owner.session_id.as_str().into(),
                        ],
                    )
                    .await?;
                if changed != 1 {
                    return Err(Error::aborted(
                        "injection claim was lost to another node or an expiry",
                    ));
                }
                Ok(())
            })
        })
        .await
}

pub(crate) async fn renew(
    store: &SqlStore,
    lease_id: &str,
    ttl_secs: u64,
    default_ttl_secs: u64,
) -> Result<Lease> {
    let seal = store.seal.clone();
    store
        .run_tx(|tx| {
            let lease_id = lease_id.to_owned();
            let seal = seal.clone();
            Box::pin(async move {
                let row = lease_row(tx, &lease_id)
                    .await?
                    .ok_or_else(|| Error::not_found("lease not found"))?;
                let sandbox_id = row.text("sandbox_id")?.to_owned();
                let sandbox = sandbox_by_id(tx, &sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("leased sandbox is gone"))?;
                match sandbox.state.known() {
                    SandboxState::Failed => {
                        let detail = if sandbox.error.is_empty() {
                            String::new()
                        } else {
                            format!(": {}", sandbox.error)
                        };
                        return Err(Error::failed_precondition(format!(
                            "sandbox {sandbox_id} failed{detail}"
                        )));
                    }
                    SandboxState::Terminating | SandboxState::Gone => {
                        return Err(Error::failed_precondition(format!(
                            "sandbox {sandbox_id} is being destroyed"
                        )));
                    }
                    _ => {}
                }
                let requested = row.u64("requested_ttl_secs")?;
                let ttl = if ttl_secs == 0 {
                    // Zero means "keep the TTL this lease was acquired
                    // with", not the service default.
                    if requested > 0 {
                        requested
                    } else {
                        default_ttl_secs
                    }
                } else {
                    ttl_secs
                };
                let version = bump_version(tx).await?;
                tx.execute(
                    "UPDATE leases SET expires_at = $2, requested_ttl_secs = $3, version = $4 \
                     WHERE id = $1",
                    &[
                        lease_id.as_str().into(),
                        (now_secs() + i64::try_from(ttl).unwrap_or(i64::MAX)).into(),
                        ttl.into(),
                        version.into(),
                    ],
                )
                .await?;
                outbox(tx, "lease", &lease_id, version, "renew", "{}").await?;
                lease_by_id(tx, &lease_id, &seal)
                    .await?
                    .ok_or_else(|| Error::not_found("lease not found"))
            })
        })
        .await
}

pub(crate) async fn release(
    store: &SqlStore,
    lease_id: &str,
    mode: ReleaseMode,
    preserve_ttl_secs: u64,
    default_preserve_secs: u64,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let lease_id = lease_id.to_owned();
            Box::pin(async move {
                let exists = tx
                    .query("SELECT 1 AS present FROM leases WHERE id = $1", &[lease_id.as_str().into()])
                    .await?;
                if exists.is_empty() {
                    return Err(Error::not_found("lease not found"));
                }
                release_lease(
                    tx,
                    &lease_id,
                    mode,
                    preserve_ttl_secs,
                    default_preserve_secs,
                )
                .await
            })
        })
        .await
}

/// Release one lease. Commits its cleanup as part of the caller's transaction.
pub(crate) async fn release_lease(
    tx: &mut dyn SqlTx,
    lease_id: &str,
    mode: ReleaseMode,
    preserve_ttl_secs: u64,
    default_preserve_secs: u64,
) -> Result<()> {
    let Some(row) = lease_row(tx, lease_id).await? else {
        return Ok(());
    };
    let sandbox_id = row.text("sandbox_id")?.to_owned();
    let key = row.text("idempotency_key")?.to_owned();
    let sandbox = sandbox_by_id(tx, &sandbox_id).await?;
    let preserve = mode == ReleaseMode::Preserve;
    let failed = sandbox
        .as_ref()
        .is_some_and(|sandbox| sandbox.state.known() == SandboxState::Failed);
    if preserve
        && !failed
        && sandbox.as_ref().is_some_and(|sandbox| {
            matches!(
                sandbox.state.known(),
                SandboxState::Leased | SandboxState::Preserved
            )
        })
    {
        let ttl = if preserve_ttl_secs == 0 {
            default_preserve_secs
        } else {
            preserve_ttl_secs
        };
        let until = now_secs() + i64::try_from(ttl).unwrap_or(i64::MAX);
        let version = bump_version(tx).await?;
        tx.execute(
            "UPDATE sandboxes SET state = 'preserved', preserve_until = $2, version = $3 \
             WHERE id = $1",
            &[sandbox_id.as_str().into(), until.into(), version.into()],
        )
        .await?;
        tx.execute(
            "UPDATE leases SET expires_at = $2, idempotency_key = '', version = $3 \
             WHERE id = $1",
            &[lease_id.into(), until.into(), version.into()],
        )
        .await?;
        if !key.is_empty() {
            release_key(tx, &key).await?;
        }
        outbox(tx, "lease", lease_id, version, "preserve", "{}").await?;
        return Ok(());
    }
    let version = bump_version(tx).await?;
    tx.execute("DELETE FROM leases WHERE id = $1", &[lease_id.into()])
        .await?;
    if !key.is_empty() {
        release_key(tx, &key).await?;
    }
    if sandbox.is_some() {
        tx.execute(
            "UPDATE sandboxes SET lease_id = '' WHERE id = $1 AND lease_id = $2",
            &[sandbox_id.as_str().into(), lease_id.into()],
        )
        .await?;
    }
    outbox(tx, "lease", lease_id, version, "release", "{}").await?;
    destroy_sandbox(tx, &sandbox_id).await?;
    Ok(())
}

pub(crate) async fn detach_lease(tx: &mut dyn SqlTx, sandbox_id: &str) -> Result<()> {
    let row = tx
        .query(
            "SELECT id, idempotency_key FROM leases WHERE sandbox_id = $1",
            &[sandbox_id.into()],
        )
        .await?
        .into_iter()
        .next();
    let Some(row) = row else {
        return Ok(());
    };
    let lease_id = row.text("id")?.to_owned();
    let key = row.text("idempotency_key")?.to_owned();
    tx.execute("DELETE FROM leases WHERE id = $1", &[lease_id.as_str().into()])
        .await?;
    if !key.is_empty() {
        release_key(tx, &key).await?;
    }
    tx.execute(
        "UPDATE sandboxes SET lease_id = '' WHERE id = $1",
        &[sandbox_id.into()],
    )
    .await?;
    Ok(())
}

/// Destroy a sandbox: drop a never-created record, or queue one destroy.
pub(crate) async fn destroy_sandbox(tx: &mut dyn SqlTx, sandbox_id: &str) -> Result<()> {
    let Some(sandbox) = sandbox_by_id(tx, sandbox_id).await? else {
        return Ok(());
    };
    match sandbox.state.known() {
        SandboxState::Gone | SandboxState::Terminating => return Ok(()),
        _ => {}
    }
    if sandbox.native_ref.is_empty() {
        purge_sandbox(tx, sandbox_id).await?;
        return Ok(());
    }
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE sandboxes SET state = 'terminating', lease_id = '', delete_attempts = 0, \
         version = $2 WHERE id = $1",
        &[sandbox_id.into(), version.into()],
    )
    .await?;
    outbox(tx, "sandbox", sandbox_id, version, "destroy", "{}").await?;
    let provider = provider_by_id(tx, &sandbox.provider_id).await?;
    let Some(provider) = provider else {
        purge_sandbox(tx, sandbox_id).await?;
        return Ok(());
    };
    super::commands_providers::queue_destroy_target(
        tx,
        &sandbox.provider_id,
        provider.generation,
        sandbox_id,
        &sandbox.driver,
        &sandbox.native_ref,
        &logic::destroy_dedup_key(sandbox_id),
        now_ms() + 86_400_000,
    )
    .await?;
    Ok(())
}

/// Fail a sandbox and close every durable handle attached to it.
pub(crate) async fn fail_sandbox(
    tx: &mut dyn SqlTx,
    sandbox_id: &str,
    reason: &str,
) -> Result<()> {
    let Some(sandbox) = sandbox_by_id(tx, sandbox_id).await? else {
        return Ok(());
    };
    if sandbox.state.known().is_terminal() {
        return Ok(());
    }
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE sandboxes SET state = 'failed', error = $2, version = $3 WHERE id = $1",
        &[
            sandbox_id.into(),
            reason.into(),
            version.into(),
        ],
    )
    .await?;
    tx.execute(
        "UPDATE execs SET status = 'failed', error = $2 \
         WHERE sandbox_id = $1 AND status = 'running'",
        &[sandbox_id.into(), reason.into()],
    )
    .await?;
    tx.execute(
        "UPDATE tickets SET status = 'aborted', reason = $2 \
         WHERE sandbox_id = $1 AND status IN ('open', 'ready')",
        &[sandbox_id.into(), reason.into()],
    )
    .await?;
    outbox(tx, "sandbox", sandbox_id, version, "failed", "{}").await?;
    Ok(())
}

pub(crate) async fn get(store: &SqlStore, id: &str, lease_id: &str) -> Result<Sandbox> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            let lease_id = lease_id.to_owned();
            Box::pin(async move {
                let sandbox = sandbox_by_id(tx, &id)
                    .await?
                    .ok_or_else(|| logic::sandbox_not_found(&id))?;
                authorize(tx, &sandbox, &lease_id).await?;
                Ok(sandbox)
            })
        })
        .await
}

async fn authorize(tx: &mut dyn SqlTx, sandbox: &Sandbox, lease_id: &str) -> Result<()> {
    if lease_id.is_empty() {
        return Err(Error::new(
            Code::PermissionDenied,
            format!(
                "leaseId is required to use sandbox `{}`; pass the lease returned by Acquire",
                sandbox.id
            ),
        ));
    }
    if sandbox.lease_id == lease_id && active_lease(tx, &sandbox.id, lease_id).await? {
        Ok(())
    } else {
        Err(Error::new(
            Code::PermissionDenied,
            format!("lease `{lease_id}` does not hold sandbox `{}`", sandbox.id),
        ))
    }
}

pub(crate) async fn list(
    store: &SqlStore,
    request: &ListRequest,
) -> Result<ListSandboxesResponse> {
    let window = logic::page_window("sandbox", request)?;
    let after = window.after.clone().unwrap_or_default();
    let limit = i64::try_from(window.limit).unwrap_or(i64::MAX);
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT s.id, s.name, s.state, s.lease_id, s.native_ref, s.error, \
                                s.provider_id, s.created_at, s.preserve_until, s.version, \
                                p.template_json, p.shape_json, p.pool, p.purpose, p.driver \
                         FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.id > $1 ORDER BY s.id LIMIT $2",
                        &[after.into(), (limit + 1).into()],
                    )
                    .await?;
                let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
                let mut sandboxes = Vec::new();
                let mut ids = Vec::new();
                for row in rows.iter().take(usize::try_from(limit).unwrap_or(usize::MAX)) {
                    ids.push(row.text("id")?.to_owned());
                    let mut sandbox = rows::sandbox_from_row(row)?;
                    sandbox.lease_id.clear();
                    sandboxes.push(sandbox);
                }
                let token = if has_more {
                    logic::next_page_token("sandbox", &ids, ids.len(), window.limit)
                } else {
                    String::new()
                };
                Ok(ListSandboxesResponse {
                    sandboxes,
                    next_page_token: token,
                    ..ListSandboxesResponse::default()
                })
            })
        })
        .await
}

pub(crate) async fn list_all(store: &SqlStore) -> Result<Vec<Sandbox>> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT s.id, s.name, s.state, s.lease_id, s.native_ref, s.error, \
                                s.provider_id, s.created_at, s.preserve_until, s.version, \
                                p.template_json, p.shape_json, p.pool, p.purpose, p.driver \
                         FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id ORDER BY s.id",
                        &[],
                    )
                    .await?;
                rows.iter()
                    .map(|row| {
                        let mut sandbox = rows::sandbox_from_row(row)?;
                        sandbox.lease_id.clear();
                        Ok(sandbox)
                    })
                    .collect()
            })
        })
        .await
}

pub(crate) async fn lease_env(store: &SqlStore, lease_id: &str) -> Result<StringMap> {
    let seal = store.seal.clone();
    store
        .run_tx(|tx| {
            let lease_id = lease_id.to_owned();
            let seal = seal.clone();
            Box::pin(async move {
                let lease = lease_by_id(tx, &lease_id, &seal)
                    .await?
                    .ok_or_else(|| Error::not_found("lease not found"))?;
                Ok(lease.env)
            })
        })
        .await
}

pub(crate) async fn register_waiter(store: &SqlStore, waiter: Waiter) -> Result<()> {
    store
        .run_tx(|tx| {
            let waiter = waiter.clone();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO acquire_waiters \
                     (key, sandbox_id, lease_id, node_id, session_id, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (key) DO UPDATE SET sandbox_id = $2, lease_id = $3, \
                      node_id = $4, session_id = $5, expires_at = $6",
                    &[
                        waiter.key.as_str().into(),
                        waiter.sandbox_id.as_str().into(),
                        waiter.lease_id.as_str().into(),
                        waiter.owner.node_id.as_str().into(),
                        waiter.owner.session_id.as_str().into(),
                        waiter.expires_at_ms.into(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn clear_waiter(
    store: &SqlStore,
    key: &str,
    owner: &Owner,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let key = key.to_owned();
            let owner = owner.clone();
            Box::pin(async move {
                tx.execute(
                    "DELETE FROM acquire_waiters \
                     WHERE key = $1 AND node_id = $2 AND session_id = $3",
                    &[
                        key.into(),
                        owner.node_id.as_str().into(),
                        owner.session_id.as_str().into(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

/// Maintenance helper: expire a lease only when no live waiter depends on it.
pub(crate) async fn expire_lease_if_unwaited(tx: &mut dyn SqlTx, lease_id: &str) -> Result<bool> {
    let waited = tx
        .query(
            "SELECT 1 AS present FROM acquire_waiters \
             WHERE lease_id = $1 AND expires_at > $2 LIMIT 1",
            &[lease_id.into(), now_ms().into()],
        )
        .await?;
    if !waited.is_empty() {
        return Ok(false);
    }
    release_lease(tx, lease_id, ReleaseMode::Destroy, 0, 0).await?;
    Ok(true)
}

/// The version of a sandbox's mutable row, for callers that need it.
pub(crate) async fn version(store: &SqlStore, sandbox_id: &str) -> Result<Option<u64>> {
    store
        .run_tx(|tx| {
            let sandbox_id = sandbox_id.to_owned();
            Box::pin(async move { sandbox_version(tx, &sandbox_id).await })
        })
        .await
}
