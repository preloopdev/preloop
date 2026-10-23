//! Pure decisions shared by SQLite, Postgres and the service.
//!
//! Nothing here touches a database or a lock: these functions turn rows into
//! a decision, so both backends and the service agree on pagination, pool
//! build revisions, capability matching and delivery identities.

use sha2::{Digest as _, Sha256};

use crate::api::*;
use crate::error::{Code, Error, Result};

use super::types::NewSandbox;

/// A decoded page cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageWindow {
    /// Return rows strictly after this id.
    pub after: Option<String>,
    /// Maximum rows to return.
    pub limit: usize,
}

/// Decode a page request for one listing kind.
///
/// The token is `base64url("v1:<kind>:<last id>")`; a token minted for
/// another listing is rejected rather than silently restarting at page one.
pub fn page_window(kind: &str, request: &ListRequest) -> Result<PageWindow> {
    use base64::Engine as _;
    let limit = match request.page_size {
        0 => crate::service::DEFAULT_PAGE_SIZE,
        size => size.min(crate::service::MAX_PAGE_SIZE),
    } as usize;
    if request.page_token.is_empty() {
        return Ok(PageWindow { after: None, limit });
    }
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let decoded = engine
        .decode(request.page_token.as_bytes())
        .map_err(|_| Error::invalid_argument("invalid page token"))?;
    let decoded =
        String::from_utf8(decoded).map_err(|_| Error::invalid_argument("invalid page token"))?;
    let (token_kind, id) = decoded
        .strip_prefix("v1:")
        .and_then(|rest| rest.split_once(':'))
        .ok_or_else(|| Error::invalid_argument("invalid page token"))?;
    if token_kind != kind || id.is_empty() {
        return Err(Error::invalid_argument("invalid page token"));
    }
    Ok(PageWindow {
        after: Some(id.to_owned()),
        limit,
    })
}

/// Encode the next page token, when the caller returned a full page.
pub fn next_page_token(kind: &str, ids: &[String], returned: usize, limit: usize) -> String {
    use base64::Engine as _;
    if returned < limit {
        return String::new();
    }
    let Some(last) = ids.last() else {
        return String::new();
    };
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("v1:{kind}:{last}"))
}

/// The pool's build revision: everything that changes what a warm member is
/// built from, excluding scaling (`warm`, `max`) and `lease_ttl_secs`.
pub fn pool_revision(pool: &PoolSpec, template_digest: &str) -> u64 {
    #[derive(serde::Serialize)]
    struct Identity<'a> {
        template: &'a str,
        template_digest: &'a str,
        shape: &'a Shape,
        network: &'a NetworkPolicy,
        requires: &'a Requirements,
        mounts: &'a [Mount],
        sockets: &'a [SocketMount],
        options: &'a StringMap,
    }
    let identity = Identity {
        template: &pool.template,
        template_digest,
        shape: &pool.shape,
        network: &pool.network,
        requires: &pool.requires,
        mounts: &pool.mounts,
        sockets: &pool.sockets,
        options: &pool.options,
    };
    let bytes = serde_json::to_vec(&identity).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(first)
}

/// Whether a driver's declared capacity can host `shape`. Zero means "not
/// reported", never "zero capacity".
pub fn fits_shape(caps: &Capabilities, shape: &Shape) -> bool {
    let cpus_fit = caps.max_vcpus == 0 || shape.cpus == 0 || shape.cpus <= caps.max_vcpus;
    let memory_fit =
        caps.max_memory_mib == 0 || shape.memory_mib == 0 || shape.memory_mib <= caps.max_memory_mib;
    cpus_fit && memory_fit
}

/// The effective shape: requested fields with the driver's defaults filling
/// the zeros.
pub fn effective_shape(caps: Option<&Capabilities>, requested: &Shape) -> Shape {
    let defaults = caps.map(|caps| &caps.default_shape).cloned().unwrap_or_default();
    let pick = |requested: u32, default: u32| {
        if requested == 0 {
            default
        } else {
            requested
        }
    };
    Shape {
        cpus: pick(requested.cpus, defaults.cpus),
        memory_mib: pick(requested.memory_mib, defaults.memory_mib),
        disk_gib: pick(requested.disk_gib, defaults.disk_gib),
        ..Shape::default()
    }
}

/// Whether a provider driver satisfies `requires`.
pub fn driver_meets(driver: &DriverInfo, requires: &Requirements) -> bool {
    let caps = &driver.capabilities;
    (requires.os.is_empty() || caps.os == requires.os)
        && (requires.arch.is_empty() || caps.arch.contains(&requires.arch))
        && requires.features.iter().all(|f| caps.features.contains(f))
        && requires.capabilities.iter().all(|c| caps.has(c))
        && (requires.driver.is_empty() || requires.driver == driver.driver)
}

/// Whether an existing warm member satisfies `requires`.
pub fn sandbox_meets(provider: Option<&Provider>, driver: &str, requires: &Requirements) -> bool {
    if requires == &Requirements::default() {
        return true;
    }
    provider
        .and_then(|provider| provider.drivers.iter().find(|info| info.driver == driver))
        .is_some_and(|info| driver_meets(info, requires))
}

/// Whether a sandbox state can serve a caller.
pub fn usable(sandbox: &Sandbox) -> Result<()> {
    match sandbox.state.known() {
        SandboxState::Leased | SandboxState::Preserved | SandboxState::Ready => Ok(()),
        SandboxState::Stopped => Err(Error::failed_precondition(format!(
            "sandbox {} is stopped; start it first",
            sandbox.id
        ))),
        state => Err(Error::failed_precondition(format!(
            "sandbox {} is {state:?}",
            sandbox.id
        ))),
    }
}

/// Validate a template/pool/provider name.
pub fn validate_name(kind: &str, name: &str) -> Result<()> {
    let bad = name.is_empty()
        || name.len() > 128
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.chars().any(|c| c.is_control());
    if bad {
        return Err(Error::invalid_argument(format!(
            "invalid {kind} name `{name}`"
        )));
    }
    Ok(())
}

/// Delivery identity for one work attempt.
pub fn delivery_id(work_id: &str, attempt: u64) -> String {
    format!("{work_id}@{attempt}")
}

/// Split a delivery identity back into `(work_id, attempt)`. A plain id is
/// an unsequenced hand-written client and maps to attempt `None`.
pub fn parse_delivery_id(delivery: &str) -> (&str, Option<u64>) {
    match delivery.rsplit_once('@') {
        Some((work_id, attempt)) if !work_id.is_empty() => match attempt.parse::<u64>() {
            Ok(attempt) => (work_id, Some(attempt)),
            Err(_) => (delivery, None),
        },
        _ => (delivery, None),
    }
}

/// The fingerprint of an acquire request's identity fields.
pub fn acquire_fingerprint(request: &AcquireRequest) -> String {
    let identity = (
        &request.pool,
        &request.template,
        &request.shape,
        &request.requires,
        &request.purpose,
        &request.network,
        &request.mounts,
        &request.sockets,
        &request.options,
        &request.name,
    );
    let canonical = serde_json::to_vec(&identity).unwrap_or_default();
    format!("sha256:{}", hex::encode(Sha256::digest(&canonical)))
}

/// SHA-256 hex of a ticket/join token, for storage.
pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Constant-time byte equality.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The dedup key for a snapshot request.
pub fn snapshot_dedup_key(sandbox_id: &str, name: &str) -> String {
    format!("snapshot:{sandbox_id}:{name}")
}

/// The dedup key for a create work item.
pub fn create_dedup_key(sandbox_id: &str) -> String {
    format!("create:{sandbox_id}")
}

/// The dedup key for a destroy work item.
pub fn destroy_dedup_key(sandbox_id: &str) -> String {
    format!("destroy:{sandbox_id}")
}

/// Error for a caller that lost an idempotency-key race.
pub fn idempotency_conflict(key: &str) -> Error {
    Error::invalid_argument(format!(
        "idempotency key `{key}` was used for a different acquire request"
    ))
}

/// Error code helper for "not found" sandboxes.
pub fn sandbox_not_found(id: &str) -> Error {
    Error::not_found(format!("sandbox `{id}` not found"))
}

/// Validate a `NewSandbox` before inserting it.
pub fn validate_new_sandbox(member: &NewSandbox) -> Result<()> {
    if member.provider_id.is_empty() || member.driver.is_empty() {
        return Err(Error::internal("placement has no provider/driver"));
    }
    Ok(())
}

/// Whether a state is terminal.
pub fn terminal(state: &str) -> bool {
    matches!(state, "gone" | "failed")
}

/// The provider's driver record.
pub fn provider_driver<'a>(provider: &'a Provider, driver: &str) -> Option<&'a DriverInfo> {
    provider.drivers.iter().find(|info| info.driver == driver)
}

/// Error for a provider that is gone.
pub fn provider_gone(id: &str) -> Error {
    Error::unavailable(format!("provider {id} is gone"))
}

/// Whether a failure is a uniqueness conflict.
pub fn is_conflict(error: &Error) -> bool {
    error.code == Code::AlreadyExists
}
