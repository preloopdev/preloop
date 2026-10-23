//! Execs, transfer tickets, snapshot/power dispatch, stats, node sessions and
//! the leaderless maintenance pass.

use std::time::Duration;

use crate::api::*;
use crate::error::{Code, Error, Result};

use super::backend::{ExecStatus, SnapshotCommand};
use super::engine::SqlTx;
use super::logic;
use super::rows;
use super::store::{
    SqlStore, active_lease, bump_version, fence_provider, lease_by_id, node_alive, now_ms,
    now_secs, outbox, provider_by_id, sandbox_by_id,
};
use super::types::SnapshotStart;
use super::types::{
    ExecHandle, MaintenanceConfig, MaintenanceOutcome, Owner, OwnerKind, PoolDeficit, PowerOp,
    ReportExecOutcome, TicketAuthority, TicketClaim, TicketSide, WorkResult,
};

/// Maximum destroy requeues for one terminating record.
pub(crate) const MAX_DESTROY_ATTEMPTS: u32 = 3;

// ── Execs ─────────────────────────────────────────────────────────

pub(crate) async fn create_exec(
    store: &SqlStore,
    exec_id: &str,
    request: ExecRequest,
    timeout_secs: u64,
    owner: &Owner,
) -> Result<ExecHandle> {
    if request.argv.is_empty() {
        return Err(Error::invalid_argument("argv is required"));
    }
    let seal = store.seal.clone();
    let exec_id = exec_id.to_owned();
    store
        .run_tx(|tx| {
            let seal = seal.clone();
            let request = request.clone();
            let owner = owner.clone();
            let exec_id = exec_id.clone();
            Box::pin(async move {
                let sandbox = sandbox_by_id(tx, &request.sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("sandbox not found"))?;
                authorize(tx, &sandbox, &request.lease_id).await?;
                logic::usable(&sandbox)?;
                let mut merged = request.clone();
                if let Some(lease) = lease_by_id(tx, &request.lease_id, &seal).await? {
                    let mut env = lease.env;
                    env.extend(request.env.clone());
                    merged.env = env;
                }
                merged.lease_id.clear();
                let timeout = timeout_secs.max(1);
                let deadline = now_secs() + i64::try_from(timeout).unwrap_or(i64::MAX);
                let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO execs \
                     (id, sandbox_id, work_id, provider_id, sequence, deadline, status, \
                      exit_code, error, started, owner_node, owner_session, version) \
                     VALUES ($1, $2, $3, $4, 0, $5, 'running', NULL, '', FALSE, $6, $7, $8)",
                    &[
                        exec_id.as_str().into(),
                        sandbox.id.as_str().into(),
                        work_id.as_str().into(),
                        sandbox.provider_id.as_str().into(),
                        deadline.into(),
                        owner.node_id.as_str().into(),
                        owner.session_id.as_str().into(),
                        version.into(),
                    ],
                )
                .await?;
                let target = SandboxWork {
                    sandbox_id: sandbox.id.clone(),
                    driver: sandbox.driver.clone(),
                    native_ref: sandbox.native_ref.clone(),
                    ..SandboxWork::default()
                };
                let work = WorkItem::new(
                    work_id.clone(),
                    work_item::Work::Exec(
                        ExecWork {
                            exec_id: exec_id.clone(),
                            target: target.into(),
                            request: merged.into(),
                            ..ExecWork::default()
                        }
                        .into(),
                    ),
                );
                let payload = super::commands_providers::WorkPayload::owned(work.clone(), &owner);
                let generation = provider_generation(tx, &sandbox.provider_id).await?;
                super::commands_providers::insert_work(
                    tx,
                    Some(&seal),
                    &sandbox.provider_id,
                    generation,
                    &work,
                    "exec",
                    &payload,
                    "",
                    now_ms(),
                    now_ms()
                        + i64::try_from((timeout + 5).saturating_mul(1000)).unwrap_or(i64::MAX),
                )
                .await?;
                outbox(tx, "exec", &exec_id, version, "start", "{}").await?;
                Ok(ExecHandle {
                    exec_id,
                    work_id,
                    deadline,
                    provider_id: sandbox.provider_id,
                })
            })
        })
        .await
}

async fn provider_generation(tx: &mut dyn SqlTx, provider_id: &str) -> Result<u64> {
    Ok(tx
        .query(
            "SELECT generation FROM providers WHERE id = $1",
            &[provider_id.into()],
        )
        .await?
        .first()
        .map(|row| row.u64("generation"))
        .transpose()?
        .unwrap_or(0))
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

pub(crate) async fn report_exec(
    store: &SqlStore,
    request: ReportExecRequest,
) -> Result<ReportExecOutcome> {
    store
        .run_tx(|tx| {
            let request = request.clone();
            Box::pin(async move {
                let _ = fence_provider(tx, &request.provider_id, request.generation).await?;
                let row = tx
                    .query(
                        "SELECT e.id, e.sandbox_id, e.work_id, e.sequence, e.status, \
                                e.started, e.started_attempt, e.owner_node, e.owner_session, \
                                w.attempt, s.provider_id \
                         FROM execs e JOIN work_items w ON w.id = e.work_id \
                         JOIN sandboxes s ON s.id = e.sandbox_id \
                         WHERE e.id = $1",
                        &[request.exec_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                let Some(row) = row else {
                    return Ok(ReportExecOutcome::Gone);
                };
                if row.text("provider_id")? != request.provider_id {
                    return Err(Error::new(
                        Code::PermissionDenied,
                        "exec belongs to another provider",
                    ));
                }
                let owner = Owner {
                    node_id: row.text("owner_node")?.to_owned(),
                    session_id: row.text("owner_session")?.to_owned(),
                };
                if !node_alive(tx, &owner).await? {
                    fail_exec_inner(tx, &request.exec_id, "the owning node is gone").await?;
                    return Ok(ReportExecOutcome::Gone);
                }
                // Output from any delivery of this execution is that same
                // execution's output: only the start claim is fenced by the
                // delivery attempt, so a redelivery racing a long command
                // cannot be mistaken for a stale report and cut it off.
                let start_claim = request.events.is_empty() && request.sequence == 0;
                let status = row.text("status")?;
                if status != "running" {
                    if request.sequence != 0 && request.sequence <= row.u64("sequence")? {
                        return Ok(ReportExecOutcome::Duplicate);
                    }
                    return Ok(ReportExecOutcome::Gone);
                }
                if start_claim {
                    if row.bool("started")? {
                        let claimed = row.u64("started_attempt")?;
                        if request.attempt != 0 && request.attempt != claimed {
                            // A different delivery wants to start an exec
                            // that already started: the outcome of that
                            // first execution may be unknown, so fail it
                            // explicitly instead of running the command a
                            // second time.
                            let reason = "exec was already started by an earlier delivery; \
                                          refusing to run it twice";
                            fail_exec_inner(tx, &request.exec_id, reason).await?;
                            return Ok(ReportExecOutcome::AlreadyStarted {
                                exec_id: request.exec_id,
                                reason: reason.to_owned(),
                            });
                        }
                        return Ok(ReportExecOutcome::Events {
                            exec_id: request.exec_id,
                            events: Vec::new(),
                            terminal: false,
                        });
                    }
                    tx.execute(
                        "UPDATE execs SET started = TRUE, started_attempt = $2 WHERE id = $1",
                        &[request.exec_id.as_str().into(), request.attempt.into()],
                    )
                    .await?;
                    return Ok(ReportExecOutcome::Events {
                        exec_id: request.exec_id,
                        events: Vec::new(),
                        terminal: false,
                    });
                }
                if request.sequence != 0 && request.sequence <= row.u64("sequence")? {
                    return Ok(ReportExecOutcome::Duplicate);
                }
                let terminal = request
                    .events
                    .iter()
                    .any(|event| matches!(event.event, Some(exec_event::Event::ExitCode(_))));
                let exit_code = request.events.iter().find_map(|event| match event.event {
                    Some(exec_event::Event::ExitCode(code)) => Some(code),
                    _ => None,
                });
                tx.execute(
                    "UPDATE execs SET sequence = $2, started = TRUE WHERE id = $1",
                    &[request.exec_id.as_str().into(), request.sequence.into()],
                )
                .await?;
                if let Some(code) = exit_code {
                    tx.execute(
                        "UPDATE execs SET status = 'done', exit_code = $2, started = TRUE \
                         WHERE id = $1",
                        &[request.exec_id.as_str().into(), i64::from(code).into()],
                    )
                    .await?;
                    let work_id = row.text("work_id")?.to_owned();
                    super::commands_providers::complete_work(tx, &work_id, WorkResult::ok(""))
                        .await?;
                }
                Ok(ReportExecOutcome::Events {
                    exec_id: request.exec_id,
                    events: request.events,
                    terminal,
                })
            })
        })
        .await
}

pub(crate) async fn finish_exec(store: &SqlStore, exec_id: &str, code: i32) -> Result<()> {
    store
        .run_tx(|tx| {
            let exec_id = exec_id.to_owned();
            Box::pin(async move {
                tx.execute(
                    "UPDATE execs SET status = 'done', exit_code = $2 \
                     WHERE id = $1 AND status = 'running'",
                    &[exec_id.as_str().into(), i64::from(code).into()],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn fail_exec(store: &SqlStore, exec_id: &str, reason: &str) -> Result<()> {
    store
        .run_tx(|tx| {
            let exec_id = exec_id.to_owned();
            let reason = reason.to_owned();
            Box::pin(async move { fail_exec_inner(tx, &exec_id, &reason).await })
        })
        .await
}

pub(crate) async fn fail_exec_inner(tx: &mut dyn SqlTx, exec_id: &str, reason: &str) -> Result<()> {
    let row = tx
        .query(
            "SELECT id, sandbox_id, work_id, provider_id, status FROM execs WHERE id = $1",
            &[exec_id.into()],
        )
        .await?
        .into_iter()
        .next();
    let Some(row) = row else {
        return Ok(());
    };
    let status = row.text("status")?.to_owned();
    if status != "running" {
        return Ok(());
    }
    let version = bump_version(tx).await?;
    tx.execute(
        "UPDATE execs SET status = 'failed', error = $2, version = $3 WHERE id = $1",
        &[exec_id.into(), reason.into(), version.into()],
    )
    .await?;
    let work_id = row.text("work_id")?.to_owned();
    let result = serde_json::to_string(&WorkResult::failed(reason)).unwrap_or_default();
    tx.execute(
        "UPDATE work_items SET status = 'done', result_json = $2, completed_at = $3, \
         secret_sealed = NULL WHERE id = $1 AND status <> 'done'",
        &[
            work_id.as_str().into(),
            result.as_str().into(),
            now_ms().into(),
        ],
    )
    .await?;
    outbox(tx, "exec", exec_id, version, "failed", "{}").await?;
    // Ask the provider to kill the guest command, once.
    let sandbox_id = row.text("sandbox_id")?.to_owned();
    queue_cancel_exec(tx, &sandbox_id, exec_id).await?;
    Ok(())
}

/// Queue one `CancelExec` for a sandbox that still holds the command.
async fn queue_cancel_exec(tx: &mut dyn SqlTx, sandbox_id: &str, exec_id: &str) -> Result<()> {
    if let Some(sandbox) = sandbox_by_id(tx, sandbox_id).await?
        && !sandbox.native_ref.is_empty()
        && sandbox.state.known() != SandboxState::Terminating
        && let Some(provider) = provider_by_id(tx, &sandbox.provider_id).await?
    {
        let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let target = SandboxWork {
            sandbox_id: sandbox.id.clone(),
            driver: sandbox.driver.clone(),
            native_ref: sandbox.native_ref.clone(),
            ..SandboxWork::default()
        };
        let work = WorkItem::new(
            work_id.clone(),
            work_item::Work::CancelExec(
                CancelExecWork {
                    exec_id: exec_id.to_owned(),
                    target: target.into(),
                    ..CancelExecWork::default()
                }
                .into(),
            ),
        );
        let payload = super::commands_providers::WorkPayload::new(work.clone());
        super::commands_providers::insert_work(
            tx,
            None,
            &sandbox.provider_id,
            provider.generation,
            &work,
            "cancel_exec",
            &payload,
            &format!("cancel-exec:{exec_id}"),
            now_ms(),
            now_ms() + 600_000,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn cancel_exec(store: &SqlStore, exec_id: &str, owner: &Owner) -> Result<()> {
    store
        .run_tx(|tx| {
            let exec_id = exec_id.to_owned();
            let owner = owner.clone();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT owner_node, owner_session, status FROM execs WHERE id = $1",
                        &[exec_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                let Some(row) = row else {
                    return Ok(());
                };
                if row.text("owner_node")? != owner.node_id
                    || row.text("owner_session")? != owner.session_id
                {
                    return Ok(());
                }
                if row.text("status")? == "running" {
                    fail_exec_inner(tx, &exec_id, "the caller disconnected").await?;
                }
                Ok(())
            })
        })
        .await
}

pub(crate) async fn exec_status(store: &SqlStore, exec_id: &str) -> Result<Option<ExecStatus>> {
    store
        .run_tx(|tx| {
            let exec_id = exec_id.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT status, exit_code, error FROM execs WHERE id = $1",
                        &[exec_id.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                let Some(row) = row else {
                    return Ok(None);
                };
                Ok(Some(match row.text("status")? {
                    "running" => ExecStatus::Running,
                    "done" => ExecStatus::Done {
                        code: i32::try_from(row.opt_int("exit_code")?.unwrap_or(0)).unwrap_or(0),
                    },
                    _ => ExecStatus::Failed {
                        reason: row.text("error")?.to_owned(),
                    },
                }))
            })
        })
        .await
}

// ── Tickets ───────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_ticket(
    store: &SqlStore,
    id: &str,
    sandbox_id: &str,
    lease_id: &str,
    kind: StreamKind,
    guest_path: &str,
    token_sha256: &str,
    expires_at: i64,
    owner: &Owner,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            let sandbox_id = sandbox_id.to_owned();
            let lease_id = lease_id.to_owned();
            let path = guest_path.to_owned();
            let token = token_sha256.to_owned();
            let owner = owner.clone();
            Box::pin(async move {
                if expires_at <= now_secs() {
                    return Err(Error::invalid_argument(
                        "ticket expiry must be in the future",
                    ));
                }
                let sandbox = sandbox_by_id(tx, &sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("sandbox not found"))?;
                authorize(tx, &sandbox, &lease_id).await?;
                // Provisioning is allowed: lease injection and in-process
                // copies are queued behind the create and start after it.
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO tickets \
                     (id, sandbox_id, lease_id, kind, guest_path, token_sha256, expires_at, \
                      producer_claimed, consumer_claimed, status, reason, owner_node, \
                      owner_session, version) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, FALSE, 'open', '', $8, $9, $10)",
                    &[
                        id.as_str().into(),
                        sandbox_id.as_str().into(),
                        lease_id.as_str().into(),
                        rows::stream_kind_label(kind).into(),
                        path.as_str().into(),
                        token.as_str().into(),
                        expires_at.into(),
                        owner.node_id.as_str().into(),
                        owner.session_id.as_str().into(),
                        version.into(),
                    ],
                )
                .await?;
                outbox(tx, "ticket", &id, version, "open", "{}").await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn claim_ticket(
    store: &SqlStore,
    id: &str,
    side: TicketSide,
    expected_kind: StreamKind,
    claim_ttl_secs: i64,
    authority: TicketAuthority<'_>,
) -> Result<TicketClaim> {
    let authority_owned = match authority {
        TicketAuthority::Caller { token } => TicketAuthorityOwned::Caller(token.to_owned()),
        TicketAuthority::Provider {
            provider_id,
            generation,
        } => TicketAuthorityOwned::Provider(provider_id.to_owned(), generation),
        TicketAuthority::Internal => TicketAuthorityOwned::Internal,
    };
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT id, sandbox_id, lease_id, kind, guest_path, token_sha256, \
                                expires_at, producer_claimed, consumer_claimed, status, \
                                owner_node, owner_session FROM tickets WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::not_found("ticket not found or expired"))?;
                if row.text("status")? != "open" && row.text("status")? != "ready" {
                    return Err(Error::not_found("ticket not found or expired"));
                }
                let owner = Owner {
                    node_id: row.text("owner_node")?.to_owned(),
                    session_id: row.text("owner_session")?.to_owned(),
                };
                if !node_alive(tx, &owner).await? {
                    return Err(Error::unavailable(
                        "the node that opened this ticket is gone",
                    ));
                }
                if row.int("expires_at")? <= now_secs() {
                    return Err(Error::not_found("ticket not found or expired"));
                }
                let sandbox_id = row.text("sandbox_id")?.to_owned();
                let lease_id = row.text("lease_id")?.to_owned();
                if !active_lease(tx, &sandbox_id, &lease_id).await? {
                    tx.execute(
                        "UPDATE tickets SET status = 'aborted', reason = 'lease is no longer active' \
                         WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?;
                    return Err(Error::failed_precondition(
                        "the sandbox lease is no longer active",
                    ));
                }
                let kind = rows::stream_kind(row.text("kind")?)?;
                if kind != expected_kind {
                    return Err(Error::invalid_argument("ticket direction does not match"));
                }
                match &authority_owned {
                    TicketAuthorityOwned::Caller(token) => {
                        if !logic::constant_time_eq(
                            logic::token_hash(token).as_bytes(),
                            row.text("token_sha256")?.as_bytes(),
                        ) {
                            return Err(Error::new(Code::Unauthenticated, "bad ticket token"));
                        }
                        let expected = match side {
                            TicketSide::Producer => StreamKind::Upload,
                            TicketSide::Consumer => StreamKind::Download,
                        };
                        if kind != expected {
                            return Err(Error::invalid_argument(if expected == StreamKind::Upload {
                                "ticket is not an upload"
                            } else {
                                "ticket is not a download"
                            }));
                        }
                    }
                    TicketAuthorityOwned::Provider(provider_id, generation) => {
                        let _ = fence_provider(tx, provider_id, *generation).await?;
                        let provider = tx
                            .query(
                                "SELECT provider_id FROM sandboxes WHERE id = $1",
                                &[sandbox_id.as_str().into()],
                            )
                            .await?
                            .into_iter()
                            .next()
                            .map(|row| row.text("provider_id").map(str::to_owned))
                            .transpose()?
                            .unwrap_or_default();
                        if provider != *provider_id {
                            return Err(Error::new(
                                Code::PermissionDenied,
                                "ticket belongs to another provider",
                            ));
                        }
                        let expected = match side {
                            TicketSide::Consumer => StreamKind::Upload,
                            TicketSide::Producer => StreamKind::Download,
                        };
                        if kind != expected {
                            return Err(Error::invalid_argument(
                                "ticket direction does not match",
                            ));
                        }
                    }
                    TicketAuthorityOwned::Internal => {}
                }
                let column = match side {
                    TicketSide::Producer => "producer_claimed",
                    TicketSide::Consumer => "consumer_claimed",
                };
                let changed = tx
                    .execute(
                        &format!(
                            "UPDATE tickets SET {column} = TRUE WHERE id = $1 AND {column} = FALSE"
                        ),
                        &[id.as_str().into()],
                    )
                    .await?;
                if changed != 1 {
                    return Err(Error::failed_precondition(
                        "that side of the transfer was already claimed",
                    ));
                }
                let both = tx
                    .query(
                        "SELECT producer_claimed, consumer_claimed FROM tickets WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::internal("ticket vanished"))?;
                let both_claimed = both.bool("producer_claimed")? && both.bool("consumer_claimed")?;
                let version = bump_version(tx).await?;
                if both_claimed {
                    tx.execute(
                        "UPDATE tickets SET status = 'ready', \
                         expires_at = CASE WHEN expires_at < $2 THEN $2 ELSE expires_at END, \
                         version = $3 WHERE id = $1",
                        &[
                            id.as_str().into(),
                            (now_secs() + claim_ttl_secs.max(1)).into(),
                            version.into(),
                        ],
                    )
                    .await?;
                } else {
                    tx.execute(
                        "UPDATE tickets SET version = $2 WHERE id = $1",
                        &[id.as_str().into(), version.into()],
                    )
                    .await?;
                }
                outbox(tx, "ticket", &id, version, "claim", "{}").await?;
                let updated = tx
                    .query(
                        "SELECT id, sandbox_id, lease_id, kind, guest_path, expires_at, \
                                producer_claimed, consumer_claimed, status, owner_node, \
                                owner_session FROM tickets WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::internal("ticket vanished"))?;
                Ok(TicketClaim {
                    ticket: rows::ticket_from_row(&updated)?,
                    both_claimed,
                })
            })
        })
        .await
}

enum TicketAuthorityOwned {
    Caller(String),
    Provider(String, u64),
    Internal,
}

pub(crate) async fn finish_ticket(
    store: &SqlStore,
    id: &str,
    status: &str,
    reason: &str,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            let status = status.to_owned();
            let reason = reason.to_owned();
            Box::pin(async move {
                tx.execute(
                    "UPDATE tickets SET status = $2, reason = $3 \
                     WHERE id = $1 AND status IN ('open', 'ready')",
                    &[id.into(), status.into(), reason.into()],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn ticket_owner(store: &SqlStore, id: &str) -> Result<Option<Owner>> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT owner_node, owner_session FROM tickets \
                         WHERE id = $1 AND status IN ('open', 'ready') AND expires_at > $2",
                        &[id.into(), now_secs().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                let Some(row) = row else {
                    return Ok(None);
                };
                let owner = Owner {
                    node_id: row.text("owner_node")?.to_owned(),
                    session_id: row.text("owner_session")?.to_owned(),
                };
                Ok(if node_alive(tx, &owner).await? {
                    Some(owner)
                } else {
                    None
                })
            })
        })
        .await
}

pub(crate) async fn start_copy(
    store: &SqlStore,
    sandbox_id: &str,
    ticket: &str,
    guest_path: &str,
    kind: StreamKind,
    owner: &Owner,
) -> Result<String> {
    store
        .run_tx(|tx| {
            let sandbox_id = sandbox_id.to_owned();
            let ticket = ticket.to_owned();
            let guest_path = guest_path.to_owned();
            let owner = owner.clone();
            Box::pin(async move {
                let sandbox = sandbox_by_id(tx, &sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("sandbox not found"))?;
                let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
                let target = SandboxWork {
                    sandbox_id: sandbox.id.clone(),
                    driver: sandbox.driver.clone(),
                    native_ref: sandbox.native_ref.clone(),
                    ..SandboxWork::default()
                };
                let work = WorkItem::new(
                    work_id.clone(),
                    work_item::Work::Copy(
                        CopyWork {
                            target: target.into(),
                            ticket: ticket.clone(),
                            path: guest_path,
                            kind: kind.into(),
                            ..CopyWork::default()
                        }
                        .into(),
                    ),
                );
                let generation = provider_generation(tx, &sandbox.provider_id).await?;
                let payload = super::commands_providers::WorkPayload::owned(work.clone(), &owner);
                super::commands_providers::insert_work(
                    tx,
                    None,
                    &sandbox.provider_id,
                    generation,
                    &work,
                    "copy",
                    &payload,
                    &format!("copy:{ticket}"),
                    now_ms(),
                    now_ms() + 86_400_000,
                )
                .await?;
                Ok(work_id)
            })
        })
        .await
}

// ── Snapshot / power / stats ──────────────────────────────────────

pub(crate) async fn snapshot_start(
    store: &SqlStore,
    command: SnapshotCommand,
) -> Result<SnapshotStart> {
    store
        .run_tx(|tx| {
            let command = command.clone();
            Box::pin(async move {
                logic::validate_name("template", &command.name)?;
                let dedup_key = if command.idempotency_key.is_empty() {
                    logic::snapshot_dedup_key(&command.sandbox_id, &command.name)
                } else {
                    format!("snapshot-key:{}", command.idempotency_key)
                };
                let lease_hash = logic::token_hash(&command.lease_id);
                let completed = tx
                    .query(
                        "SELECT result_json, payload_json FROM work_items \
                         WHERE dedup_key = $1 AND status = 'done' \
                         ORDER BY completed_at DESC LIMIT 1",
                        &[dedup_key.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                if let Some(row) = completed {
                    let payload: super::commands_providers::WorkPayload =
                        rows::json(row.text("payload_json")?, "snapshot payload")?;
                    if !payload.lease_sha256.is_empty()
                        && !logic::constant_time_eq(
                            payload.lease_sha256.as_bytes(),
                            lease_hash.as_bytes(),
                        )
                    {
                        return Err(Error::new(
                            Code::PermissionDenied,
                            "snapshot idempotency key belongs to another lease",
                        ));
                    }
                    let result: WorkResult = rows::json(row.text("result_json")?, "snapshot result")?;
                    if !result.ok {
                        return Err(crate::error::from_reported(result.error));
                    }
                    if let Some(template) = result.template {
                        return Ok(SnapshotStart::Replay(template));
                    }
                    let template = tx
                        .query(
                            "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                    created_at, version FROM templates \
                             WHERE name = $1 AND retired = FALSE",
                            &[command.name.as_str().into()],
                        )
                        .await?
                        .into_iter()
                        .next();
                    if let Some(row) = template {
                        return Ok(SnapshotStart::Replay(rows::template_from_row(&row)?));
                    }
                }
                let active = tx
                    .query(
                        "SELECT id FROM work_items WHERE dedup_key = $1 AND status <> 'done' LIMIT 1",
                        &[dedup_key.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                if let Some(row) = active {
                    return Ok(SnapshotStart::Queued {
                        work_id: row.text("id")?.to_owned(),
                    });
                }
                let sandbox = sandbox_by_id(tx, &command.sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("sandbox not found"))?;
                authorize(tx, &sandbox, &command.lease_id).await?;
                if sandbox.state.known() != SandboxState::Leased {
                    return Err(Error::failed_precondition(
                        "only a leased sandbox can be snapshotted",
                    ));
                }
                let caps = super::store::driver_caps(tx, &sandbox.provider_id, &sandbox.driver)
                    .await?
                    .ok_or_else(|| Error::unavailable("sandbox provider is gone"))?;
                if !caps.snapshot {
                    return Err(Error::failed_precondition(format!(
                        "{}: snapshot unsupported",
                        sandbox.driver
                    )));
                }
                let source_template = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates WHERE name = $1",
                        &[sandbox.template.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .map(|row| rows::template_from_row(&row))
                    .transpose()?
                    .unwrap_or_default();
                let provider_generation = provider_generation(tx, &sandbox.provider_id).await?;
                let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
                let target = SandboxWork {
                    sandbox_id: sandbox.id.clone(),
                    driver: sandbox.driver.clone(),
                    native_ref: sandbox.native_ref.clone(),
                    ..SandboxWork::default()
                };
                let work = WorkItem::new(
                    work_id.clone(),
                    work_item::Work::Snapshot(
                        SnapshotWork {
                            target: target.into(),
                            template: command.name.clone(),
                            ..SnapshotWork::default()
                        }
                        .into(),
                    ),
                );
                let payload = super::commands_providers::WorkPayload {
                    work: work.clone(),
                    source_template: Some(source_template),
                    lease_sha256: lease_hash,
                    snapshot_name: command.name.clone(),
                    owner_node: command.owner.node_id.clone(),
                    owner_session: command.owner.session_id.clone(),
                };
                super::commands_providers::insert_work(
                    tx,
                    None,
                    &sandbox.provider_id,
                    provider_generation,
                    &work,
                    "snapshot",
                    &payload,
                    &dedup_key,
                    now_ms(),
                    now_ms()
                        + i64::try_from(command.timeout.as_millis()).unwrap_or(i64::MAX),
                )
                .await?;
                Ok(SnapshotStart::Queued { work_id })
            })
        })
        .await
}

pub(crate) async fn power(
    store: &SqlStore,
    sandbox_id: &str,
    lease_id: &str,
    op: PowerOp,
    timeout: Duration,
) -> Result<String> {
    store
        .run_tx(|tx| {
            let sandbox_id = sandbox_id.to_owned();
            let lease_id = lease_id.to_owned();
            Box::pin(async move {
                let sandbox = sandbox_by_id(tx, &sandbox_id)
                    .await?
                    .ok_or_else(|| Error::not_found("sandbox not found"))?;
                authorize(tx, &sandbox, &lease_id).await?;
                let caps = super::store::driver_caps(tx, &sandbox.provider_id, &sandbox.driver)
                    .await?
                    .ok_or_else(|| Error::unavailable("sandbox provider is gone"))?;
                if !caps.pause {
                    return Err(Error::failed_precondition(format!(
                        "{}: stop/start unsupported",
                        sandbox.driver
                    )));
                }
                let target = SandboxWork {
                    sandbox_id: sandbox.id.clone(),
                    driver: sandbox.driver.clone(),
                    native_ref: sandbox.native_ref.clone(),
                    ..SandboxWork::default()
                };
                let (work, kind) = match op {
                    PowerOp::Stop => (
                        WorkItem::new(
                            format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                            work_item::Work::Stop(target.into()),
                        ),
                        "stop",
                    ),
                    PowerOp::Start { forkable } => (
                        WorkItem::new(
                            format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                            work_item::Work::Start(
                                StartWork {
                                    target: target.into(),
                                    forkable,
                                    ..StartWork::default()
                                }
                                .into(),
                            ),
                        ),
                        "start",
                    ),
                    PowerOp::Pause => (
                        WorkItem::new(
                            format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                            work_item::Work::Pause(target.into()),
                        ),
                        "pause",
                    ),
                    PowerOp::Resume => (
                        WorkItem::new(
                            format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                            work_item::Work::Resume(target.into()),
                        ),
                        "resume",
                    ),
                };
                let generation = provider_generation(tx, &sandbox.provider_id).await?;
                let payload = super::commands_providers::WorkPayload::new(work.clone());
                super::commands_providers::insert_work(
                    tx,
                    None,
                    &sandbox.provider_id,
                    generation,
                    &work,
                    kind,
                    &payload,
                    "",
                    now_ms(),
                    now_ms() + i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX),
                )
                .await?;
                Ok(work.id)
            })
        })
        .await
}

pub(crate) async fn stats(store: &SqlStore) -> Result<StatsResponse> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let providers = tx
                    .query(
                        "SELECT id, name, generation, drivers_json, last_seen, cordoned, \
                                host_json, host_sampled_at, stats_json FROM providers ORDER BY id",
                        &[],
                    )
                    .await?;
                let mut out = Vec::new();
                for row in providers {
                    let provider = rows::provider_from_row(&row)?;
                    let host: HostSample = row
                        .opt_text("host_json")?
                        .map(|text| rows::json(text, "host sample"))
                        .transpose()?
                        .unwrap_or_default();
                    let runtime: Vec<SandboxRuntimeStats> = rows::json(
                        row.text("stats_json").unwrap_or("[]"),
                        "sandbox runtime stats",
                    )?;
                    let age = row
                        .opt_int("host_sampled_at")?
                        .map(|at| (now_ms() - at) / 1000)
                        .unwrap_or(0);
                    let sandboxes = tx
                        .query(
                            "SELECT s.id, s.name, s.state, s.lease_id, s.native_ref, s.error, \
                                    s.provider_id, s.created_at, s.preserve_until, s.version, \
                                    p.template_json, p.shape_json, p.pool, p.purpose, p.driver \
                             FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                             WHERE s.provider_id = $1 ORDER BY s.id",
                            &[provider.id.as_str().into()],
                        )
                        .await?;
                    let mut stats = Vec::new();
                    for sandbox_row in sandboxes {
                        let sandbox = rows::sandbox_from_row(&sandbox_row)?;
                        let facts = runtime
                            .iter()
                            .find(|facts| facts.native_ref == sandbox.native_ref)
                            .cloned()
                            .unwrap_or_default();
                        stats.push(SandboxStats {
                            sandbox: sandbox.into(),
                            runtime: facts.into(),
                            ..SandboxStats::default()
                        });
                    }
                    out.push(ProviderStats {
                        provider_id: provider.id,
                        name: provider.name,
                        drivers: provider.drivers,
                        host: host.into(),
                        host_sample_age_secs: age,
                        sandboxes: stats,
                        ..ProviderStats::default()
                    });
                }
                Ok(StatsResponse {
                    providers: out,
                    ..StatsResponse::default()
                })
            })
        })
        .await
}

// ── Node sessions ─────────────────────────────────────────────────

pub(crate) async fn node_register(
    store: &SqlStore,
    node_id: &str,
    session_id: &str,
    credential_sha256: &str,
    peer_url: &str,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let node_id = node_id.to_owned();
            let session_id = session_id.to_owned();
            let credential = credential_sha256.to_owned();
            let peer_url = peer_url.to_owned();
            Box::pin(async move {
                // A new session for this node fences the previous process's
                // provider sessions and live channels; durable leases and
                // sandboxes stay, and providers owned by other nodes are
                // never touched merely because this node started.
                let previous = tx
                    .query(
                        "SELECT session_id FROM nodes WHERE node_id = $1",
                        &[node_id.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .map(|row| row.text("session_id").map(str::to_owned))
                    .transpose()?;
                if let Some(previous) = previous.filter(|previous| previous != &session_id) {
                    tx.execute(
                        "UPDATE providers SET generation = generation + 1 \
                         WHERE owner_node = $1 AND owner_session = $2",
                        &[node_id.as_str().into(), previous.as_str().into()],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE work_items SET status = 'queued', visible_at = 0, \
                         generation = (SELECT generation FROM providers p \
                                       WHERE p.id = work_items.provider_id) \
                         WHERE provider_id IN (SELECT id FROM providers \
                                               WHERE owner_node = $1 AND owner_session = $2) \
                           AND status = 'inflight'",
                        &[node_id.as_str().into(), previous.as_str().into()],
                    )
                    .await?;
                    // Injection bytes are memory-only at the caller. A
                    // restarted node must not claim files it never delivered.
                    tx.execute(
                        "UPDATE leases SET inject_state = 'pending' \
                         WHERE inject_owner_node = $1 AND inject_owner_session = $2 \
                           AND inject_state = 'done'",
                        &[node_id.as_str().into(), previous.as_str().into()],
                    )
                    .await?;
                }
                fail_node_channels(tx, &node_id, Some(&session_id)).await?;
                tx.execute(
                    "INSERT INTO nodes \
                     (node_id, session_id, credential_sha256, peer_url, started_at, \
                      last_seen, alive) \
                     VALUES ($1, $2, $3, $4, $5, $5, TRUE) \
                     ON CONFLICT (node_id) DO UPDATE SET session_id = $2, \
                      credential_sha256 = $3, peer_url = $4, started_at = $5, \
                      last_seen = $5, alive = TRUE",
                    &[
                        node_id.as_str().into(),
                        session_id.as_str().into(),
                        credential.as_str().into(),
                        peer_url.as_str().into(),
                        now_ms().into(),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn node_heartbeat(
    store: &SqlStore,
    node_id: &str,
    session_id: &str,
) -> Result<()> {
    store
        .run_tx(|tx| {
            let node_id = node_id.to_owned();
            let session_id = session_id.to_owned();
            Box::pin(async move {
                let changed = tx
                    .execute(
                        "UPDATE nodes SET last_seen = $3 \
                         WHERE node_id = $1 AND session_id = $2 AND alive = TRUE",
                        &[node_id.into(), session_id.into(), now_ms().into()],
                    )
                    .await?;
                if changed != 1 {
                    return Err(Error::aborted("this node session is no longer live"));
                }
                Ok(())
            })
        })
        .await
}

pub(crate) async fn node_authenticate(
    store: &SqlStore,
    node_id: &str,
    session_id: &str,
    credential_sha256: &str,
) -> Result<bool> {
    store
        .run_tx(|tx| {
            let node_id = node_id.to_owned();
            let session_id = session_id.to_owned();
            let credential = credential_sha256.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT credential_sha256 FROM nodes \
                         WHERE node_id = $1 AND session_id = $2 AND alive = TRUE",
                        &[node_id.into(), session_id.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                Ok(row.is_some_and(|row| {
                    row.text("credential_sha256").is_ok_and(|stored| {
                        logic::constant_time_eq(stored.as_bytes(), credential.as_bytes())
                    })
                }))
            })
        })
        .await
}

pub(crate) async fn node_close(store: &SqlStore, node_id: &str, session_id: &str) -> Result<()> {
    store
        .run_tx(|tx| {
            let node_id = node_id.to_owned();
            let session_id = session_id.to_owned();
            Box::pin(async move {
                fail_node_channels(tx, &node_id, Some(&session_id)).await?;
                tx.execute(
                    "UPDATE nodes SET alive = FALSE \
                     WHERE node_id = $1 AND session_id = $2",
                    &[node_id.into(), session_id.into()],
                )
                .await?;
                Ok(())
            })
        })
        .await
}

async fn fail_node_channels(
    tx: &mut dyn SqlTx,
    node_id: &str,
    keep_session: Option<&str>,
) -> Result<()> {
    let filter = match keep_session {
        Some(_) => "owner_node = $1 AND owner_session <> $2",
        None => "owner_node = $1",
    };
    let params: Vec<super::engine::SqlValue> = match keep_session {
        Some(session) => vec![node_id.into(), session.into()],
        None => vec![node_id.into()],
    };
    let execs = tx
        .query(
            &format!("SELECT id, work_id FROM execs WHERE {filter} AND status = 'running'"),
            &params,
        )
        .await?;
    for row in execs {
        let exec_id = row.text("id")?.to_owned();
        let work_id = row.text("work_id")?.to_owned();
        tx.execute(
            "UPDATE execs SET status = 'failed', error = 'the owning node session ended' \
             WHERE id = $1",
            &[exec_id.as_str().into()],
        )
        .await?;
        let result = serde_json::to_string(&WorkResult::failed("the owning node session ended"))
            .unwrap_or_default();
        tx.execute(
            "UPDATE work_items SET status = 'done', result_json = $2, completed_at = $3 \
             WHERE id = $1 AND status <> 'done'",
            &[
                work_id.as_str().into(),
                result.as_str().into(),
                now_ms().into(),
            ],
        )
        .await?;
    }
    tx.execute(
        &format!(
            "UPDATE tickets SET status = 'aborted', reason = 'the owning node session ended' \
             WHERE {filter} AND status IN ('open', 'ready')"
        ),
        &params,
    )
    .await?;
    let waiter_filter = match keep_session {
        Some(_) => "node_id = $1 AND session_id <> $2",
        None => "node_id = $1",
    };
    tx.execute(
        &format!("DELETE FROM acquire_waiters WHERE {waiter_filter}"),
        &params,
    )
    .await?;
    let lease_filter = match keep_session {
        Some(_) => "inject_owner_node = $1 AND inject_owner_session <> $2",
        None => "inject_owner_node = $1",
    };
    tx.execute(
        &format!(
            "UPDATE leases SET inject_state = 'pending', inject_owner_node = '', \
             inject_owner_session = '', inject_claim_expires_at = 0 WHERE {lease_filter}"
        ),
        &params,
    )
    .await?;
    Ok(())
}

pub(crate) async fn owner_of(store: &SqlStore, kind: OwnerKind, id: &str) -> Result<Option<Owner>> {
    store
        .run_tx(|tx| {
            let id = id.to_owned();
            Box::pin(async move {
                let (sql, status) = match kind {
                    OwnerKind::Exec => (
                        "SELECT owner_node, owner_session FROM execs \
                         WHERE id = $1 AND status = 'running'",
                        "running",
                    ),
                    OwnerKind::Ticket => (
                        "SELECT owner_node, owner_session FROM tickets \
                         WHERE id = $1 AND status IN ('open', 'ready')",
                        "open",
                    ),
                };
                let _ = status;
                let row = tx.query(sql, &[id.into()]).await?.into_iter().next();
                let Some(row) = row else {
                    return Ok(None);
                };
                let owner = Owner {
                    node_id: row.text("owner_node")?.to_owned(),
                    session_id: row.text("owner_session")?.to_owned(),
                };
                Ok(if node_alive(tx, &owner).await? {
                    Some(owner)
                } else {
                    None
                })
            })
        })
        .await
}

// ── Maintenance ──────────────────────────────────────────────────

pub(crate) async fn maintain(
    store: &SqlStore,
    config: &MaintenanceConfig,
) -> Result<MaintenanceOutcome> {
    let config = config.clone();
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let mut outcome = MaintenanceOutcome::default();
                // Node sessions that stopped heartbeating.
                let lost_after = i64::try_from(config.node_lost_after.as_millis())
                    .unwrap_or(i64::MAX);
                let lost_nodes = tx
                    .query(
                        "SELECT node_id, session_id FROM nodes \
                         WHERE alive = TRUE AND last_seen < $1",
                        &[(now_ms() - lost_after).into()],
                    )
                    .await?;
                for row in lost_nodes {
                    let node_id = row.text("node_id")?.to_owned();
                    let session_id = row.text("session_id")?.to_owned();
                    fail_node_channels(tx, &node_id, Some(&session_id)).await?;
                    tx.execute(
                        "UPDATE nodes SET alive = FALSE \
                         WHERE node_id = $1 AND session_id = $2",
                        &[node_id.as_str().into(), session_id.as_str().into()],
                    )
                    .await?;
                    outcome.changed = true;
                }
                // Same-key reservations that never produced a lease (a crash
                // between reserve and insert) or whose lease row was removed
                // externally must not block the key forever.
                let stale_keys = tx
                    .execute(
                        "DELETE FROM lease_keys \
                         WHERE (lease_id <> '' AND NOT EXISTS \
                                (SELECT 1 FROM leases l WHERE l.id = lease_keys.lease_id)) \
                            OR (lease_id = '' AND created_at < $1)",
                        &[(now_ms() - 60_000).into()],
                    )
                    .await?;
                outcome.changed |= stale_keys > 0;
                // Expired injection claims and waiters.
                let claims = tx
                    .execute(
                        "UPDATE leases SET inject_state = 'pending', inject_owner_node = '', \
                         inject_owner_session = '', inject_claim_expires_at = 0 \
                         WHERE inject_state = 'claimed' AND inject_claim_expires_at <= $1",
                        &[now_ms().into()],
                    )
                    .await?;
                let waiters = tx
                    .execute(
                        "DELETE FROM acquire_waiters WHERE expires_at <= $1",
                        &[now_ms().into()],
                    )
                    .await?;
                outcome.changed |= claims + waiters > 0;
                // Expired leases, unless a live waiter still depends on one.
                let leases = tx
                    .query(
                        "SELECT id FROM leases WHERE expires_at <= $1 LIMIT 100",
                        &[now_secs().into()],
                    )
                    .await?;
                for row in leases {
                    if super::commands_leases::expire_lease_if_unwaited(
                        tx,
                        row.text("id")?,
                    )
                    .await?
                    {
                        outcome.changed = true;
                    }
                }
                // Preservation windows.
                let preserved = tx
                    .query(
                        "SELECT id FROM sandboxes \
                         WHERE state = 'preserved' AND preserve_until <= $1",
                        &[now_secs().into()],
                    )
                    .await?;
                for row in preserved {
                    super::commands_leases::destroy_sandbox(tx, row.text("id")?).await?;
                    outcome.changed = true;
                }
                // Creates whose work deadline passed with no live waiter.
                let timed_out = tx
                    .query(
                        "SELECT s.id FROM sandboxes s \
                         JOIN sandbox_specs p ON p.id = s.id \
                         JOIN work_items w ON w.dedup_key = 'create:' || s.id \
                         WHERE s.state = 'provisioning' AND w.status <> 'done' \
                           AND w.deadline_at <= $1 \
                           AND NOT EXISTS (SELECT 1 FROM acquire_waiters a \
                                           WHERE a.sandbox_id = s.id AND a.expires_at > $2)",
                        &[now_ms().into(), now_ms().into()],
                    )
                    .await?;
                for row in timed_out {
                    super::commands_leases::fail_sandbox(
                        tx,
                        row.text("id")?,
                        "create timed out",
                    )
                    .await?;
                    outcome.changed = true;
                }
                // Execs past their deadline: same timeout exit the driver
                // reports, never a scheduling-dependent error.
                let grace = i64::try_from(config.exec_sweep_grace.as_millis()).unwrap_or(0);
                let execs = tx
                    .query(
                        "SELECT id, work_id, sandbox_id FROM execs \
                         WHERE status = 'running' AND deadline <= $1",
                        &[(now_secs() - grace / 1000).into()],
                    )
                    .await?;
                for row in execs {
                    let exec_id = row.text("id")?.to_owned();
                    let work_id = row.text("work_id")?.to_owned();
                    let sandbox_id = row.text("sandbox_id")?.to_owned();
                    tx.execute(
                        "UPDATE execs SET status = 'done', exit_code = 124 \
                         WHERE id = $1 AND status = 'running'",
                        &[exec_id.as_str().into()],
                    )
                    .await?;
                    let result = serde_json::to_string(&WorkResult::ok("")).unwrap_or_default();
                    tx.execute(
                        "UPDATE work_items SET status = 'done', result_json = $2, \
                         completed_at = $3 WHERE id = $1 AND status <> 'done'",
                        &[work_id.as_str().into(), result.as_str().into(), now_ms().into()],
                    )
                    .await?;
                    queue_cancel_exec(tx, &sandbox_id, &exec_id).await?;
                    outcome
                        .failed_execs
                        .push((exec_id, "timeout".to_owned()));
                    outcome.changed = true;
                }
                // Work nobody will ever acknowledge.
                let stale = tx
                    .query(
                        "SELECT id, kind, payload_json FROM work_items \
                         WHERE status <> 'done' AND deadline_at <= $1 LIMIT 200",
                        &[now_ms().into()],
                    )
                    .await?;
                for row in stale {
                    let id = row.text("id")?.to_owned();
                    let kind = row.text("kind")?.to_owned();
                    let payload: super::commands_providers::WorkPayload =
                        rows::json(row.text("payload_json")?, "work payload")?;
                    let message = match kind.as_str() {
                        "create" => "create timed out",
                        "exec" => "exec timed out",
                        _ => "work timed out",
                    };
                    super::commands_providers::complete_work(
                        tx,
                        &id,
                        WorkResult::failed(message),
                    )
                    .await?;
                    if kind == "exec"
                        && let Some(work_item::Work::Exec(exec)) = &payload.work.work
                    {
                        outcome
                            .failed_execs
                            .push((exec.exec_id.clone(), message.to_owned()));
                    }
                    outcome.changed = true;
                }
                // Silent providers: their sandboxes cannot be reached again.
                let silent_after = i64::try_from(
                    config.provider_timeout.as_millis()
                        * u128::from(config.provider_lost_factor.max(1)),
                )
                .unwrap_or(i64::MAX);
                let silent = tx
                    .query(
                        "SELECT id, generation FROM providers WHERE seen_at < $1",
                        &[(now_ms() - silent_after).into()],
                    )
                    .await?;
                for row in silent {
                    let provider_id = row.text("id")?.to_owned();
                    let generation = row.u64("generation")?;
                    let sandboxes = tx
                        .query(
                            "SELECT id FROM sandboxes \
                             WHERE provider_id = $1 AND state NOT IN ('gone', 'failed')",
                            &[provider_id.as_str().into()],
                        )
                        .await?;
                    for sandbox in sandboxes {
                        let sandbox_id = sandbox.text("id")?.to_owned();
                        super::commands_leases::detach_lease(tx, &sandbox_id).await?;
                        super::commands_leases::fail_sandbox(
                            tx,
                            &sandbox_id,
                            "provider lost (no heartbeat)",
                        )
                        .await?;
                    }
                    let work = tx
                        .query(
                            "SELECT id FROM work_items WHERE provider_id = $1 AND status <> 'done'",
                            &[provider_id.as_str().into()],
                        )
                        .await?;
                    for item in work {
                        super::commands_providers::complete_work(
                            tx,
                            item.text("id")?,
                            WorkResult::failed("provider lost (no heartbeat)"),
                        )
                        .await?;
                    }
                    let _ = generation;
                    outcome.changed = true;
                }
                // A sandbox whose lease row is missing holds a machine with
                // no capability to reach it. Clear the dangling reference and
                // fail it so the unleased sweep below frees the machine.
                let orphans = tx
                    .query(
                        "SELECT s.id FROM sandboxes s WHERE s.lease_id <> '' \
                         AND NOT EXISTS (SELECT 1 FROM leases l WHERE l.id = s.lease_id)",
                        &[],
                    )
                    .await?;
                let mut just_failed = std::collections::HashSet::new();
                for row in orphans {
                    let id = row.text("id")?.to_owned();
                    tx.execute(
                        "UPDATE sandboxes SET lease_id = '' WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?;
                    super::commands_leases::fail_sandbox(
                        tx,
                        &id,
                        "restored without a lease",
                    )
                    .await?;
                    just_failed.insert(id);
                    outcome.changed = true;
                }
                // Failed, unleased sandboxes free whatever the host holds.
                // A record failed by this pass waits for the next one, so the
                // caller observes the durable Failed state before Terminating.
                let failed = tx
                    .query(
                        "SELECT id FROM sandboxes WHERE state = 'failed' AND lease_id = ''",
                        &[],
                    )
                    .await?;
                for row in failed {
                    let id = row.text("id")?.to_owned();
                    if just_failed.contains(&id) {
                        continue;
                    }
                    super::commands_leases::destroy_sandbox(tx, &id).await?;
                    outcome.changed = true;
                }
                // Expired join tokens and certificates.
                let tokens = tx
                    .execute(
                        "DELETE FROM join_tokens WHERE expires_at <= $1",
                        &[now_secs().into()],
                    )
                    .await?;
                let certs = tx
                    .execute(
                        "DELETE FROM provider_certificates WHERE expires_at <= $1",
                        &[now_secs().into()],
                    )
                    .await?;
                outcome.changed |= tokens + certs > 0;
                // Expired tickets are aborted durably; the local rendezvous is
                // torn down by the service after this commit.
                let tickets = tx
                    .query(
                        "SELECT id FROM tickets \
                         WHERE status IN ('open', 'ready') AND expires_at <= $1",
                        &[now_secs().into()],
                    )
                    .await?;
                for row in tickets {
                    let id = row.text("id")?.to_owned();
                    tx.execute(
                        "UPDATE tickets SET status = 'aborted', reason = 'transfer ticket expired' \
                         WHERE id = $1",
                        &[id.as_str().into()],
                    )
                    .await?;
                    outcome.aborted_tickets.push(id);
                    outcome.changed = true;
                }
                // Warm pool deficits, computed in SQL.
                let pools = tx
                    .query(
                        "SELECT name, template, revision, warm, max_members FROM pools ORDER BY name",
                        &[],
                    )
                    .await?;
                for pool in pools {
                    let name = pool.text("name")?.to_owned();
                    let warm = pool.int("warm")?;
                    let max = pool.int("max_members")?;
                    let counts = tx
                        .query(
                            "SELECT \
                               SUM(CASE WHEN s.lease_id = '' \
                                     AND s.state IN ('ready', 'provisioning') THEN 1 ELSE 0 END) \
                                 AS idle, \
                               SUM(CASE WHEN s.state <> 'terminating' THEN 1 ELSE 0 END) AS live \
                             FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                             WHERE p.pool = $1 AND s.state NOT IN ('gone', 'failed')",
                            &[name.as_str().into()],
                        )
                        .await?
                        .into_iter()
                        .next();
                    let idle = counts.as_ref().and_then(|row| row.opt_int("idle").ok().flatten()).unwrap_or(0);
                    let live = counts.as_ref().and_then(|row| row.opt_int("live").ok().flatten()).unwrap_or(0);
                    let mut missing = warm.saturating_sub(idle);
                    if max > 0 {
                        missing = missing.min(max.saturating_sub(live));
                    }
                    if missing > 0 {
                        outcome.pool_deficits.push(PoolDeficit {
                            pool: name,
                            template: pool.text("template")?.to_owned(),
                            revision: pool.u64("revision")?,
                            missing: u32::try_from(missing).unwrap_or(u32::MAX),
                        });
                    }
                }
                Ok(outcome)
            })
        })
        .await
}
