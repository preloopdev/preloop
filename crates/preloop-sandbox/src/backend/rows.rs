//! Row decoding. Every column name here matches `schema.sqlite.sql` and
//! `schema.postgres.sql`; both dialects share the same query surface.

use serde::de::DeserializeOwned;

use crate::api::*;
use crate::error::{Error, Result};

use super::engine::Row;
use super::seal::SealKey;
use super::types::TicketRecord;

pub(crate) fn json<T: DeserializeOwned>(text: &str, what: &str) -> Result<T> {
    serde_json::from_str(text)
        .map_err(|error| Error::internal(format!("sandbox store: decoding {what}: {error}")))
}

pub(crate) fn template_from_row(row: &Row) -> Result<Template> {
    let spec: TemplateSpec = json(row.text("spec_json")?, "template spec")?;
    Ok(Template {
        spec: spec.into(),
        digest: row.text("digest")?.to_owned(),
        provider_id: row.text("provider_id")?.to_owned(),
        native_ref: row.text("native_ref")?.to_owned(),
        created_at: row.int("created_at")?,
        driver: row.text("driver")?.to_owned(),
        ..Template::default()
    })
}

pub(crate) fn pool_from_row(row: &Row) -> Result<PoolSpec> {
    json(row.text("spec_json")?, "pool spec")
}

pub(crate) fn driver_list(text: &str) -> Result<Vec<DriverInfo>> {
    json(text, "provider drivers")
}

pub(crate) fn provider_from_row(row: &Row) -> Result<Provider> {
    let drivers = driver_list(row.text("drivers_json")?)?;
    Ok(Provider {
        id: row.text("id")?.to_owned(),
        name: row.text("name")?.to_owned(),
        generation: row.u64("generation")?,
        drivers,
        last_seen: row.int("last_seen")?,
        cordoned: row.bool("cordoned")?,
        ..Provider::default()
    })
}

pub(crate) fn sandbox_from_row(row: &Row) -> Result<Sandbox> {
    let template: Template = json(row.text("template_json")?, "sandbox template")?;
    let shape: Shape = json(row.text("shape_json")?, "sandbox shape")?;
    Ok(Sandbox {
        id: row.text("id")?.to_owned(),
        name: row.text("name")?.to_owned(),
        template: template.spec.as_option().map(|spec| spec.name.clone()).unwrap_or_default(),
        pool: row.text("pool")?.to_owned(),
        provider_id: row.text("provider_id")?.to_owned(),
        driver: row.text("driver")?.to_owned(),
        native_ref: row.text("native_ref")?.to_owned(),
        state: sandbox_state(row.text("state")?)?.into(),
        purpose: purpose(row.text("purpose")?)?.into(),
        lease_id: row.text("lease_id")?.to_owned(),
        error: row.text("error")?.to_owned(),
        created_at: row.int("created_at")?,
        preserve_until: row.int("preserve_until")?,
        shape: shape.into(),
        ..Sandbox::default()
    })
}

pub(crate) fn lease_from_row(row: &Row, seal: &SealKey) -> Result<Lease> {
    let env = match row.opt_bytes("env_sealed")? {
        Some(sealed) if !sealed.is_empty() => {
            let plain = seal.unseal(sealed, lease_aad(row.text("id")?).as_bytes())?;
            json(&String::from_utf8_lossy(&plain), "lease env")?
        }
        _ => StringMap::default(),
    };
    Ok(Lease {
        id: row.text("id")?.to_owned(),
        sandbox_id: row.text("sandbox_id")?.to_owned(),
        pool: row.text("pool")?.to_owned(),
        idempotency_key: row.text("idempotency_key")?.to_owned(),
        expires_at: row.int("expires_at")?,
        requested_ttl_secs: row.u64("requested_ttl_secs")?,
        env,
        request_hash: row.text("request_hash")?.to_owned(),
        ..Lease::default()
    })
}

pub(crate) fn lease_aad(lease_id: &str) -> String {
    format!("lease:{lease_id}")
}

pub(crate) fn exec_aad(exec_id: &str) -> String {
    format!("exec:{exec_id}")
}

pub(crate) fn sandbox_state(value: &str) -> Result<SandboxState> {
    Ok(match value {
        "provisioning" => SandboxState::Provisioning,
        "ready" => SandboxState::Ready,
        "leased" => SandboxState::Leased,
        "paused" => SandboxState::Paused,
        "stopped" => SandboxState::Stopped,
        "preserved" => SandboxState::Preserved,
        "terminating" => SandboxState::Terminating,
        "gone" => SandboxState::Gone,
        "failed" => SandboxState::Failed,
        other => {
            return Err(Error::internal(format!(
                "sandbox store: unknown sandbox state `{other}`"
            )));
        }
    })
}

pub(crate) fn purpose(value: &str) -> Result<Purpose> {
    Ok(match value {
        "runner" => Purpose::Runner,
        "build" => Purpose::Build,
        "debug" | "exec" => Purpose::Exec,
        "session" => Purpose::Session,
        "" | "unspecified" => Purpose::PurposeUnspecified,
        other => {
            return Err(Error::internal(format!(
                "sandbox store: unknown purpose `{other}`"
            )));
        }
    })
}

pub(crate) fn purpose_label(value: Purpose) -> &'static str {
    match value {
        Purpose::Runner => "runner",
        Purpose::Build => "build",
        Purpose::Exec => "exec",
        Purpose::Session => "session",
        Purpose::PurposeUnspecified => "unspecified",
    }
}

pub(crate) fn state_label(state: SandboxState) -> &'static str {
    state.as_metric_label()
}

pub(crate) fn stream_kind(value: &str) -> Result<StreamKind> {
    Ok(match value {
        "upload" => StreamKind::Upload,
        "download" => StreamKind::Download,
        other => {
            return Err(Error::internal(format!(
                "sandbox store: unknown stream kind `{other}`"
            )));
        }
    })
}

pub(crate) fn stream_kind_label(value: StreamKind) -> &'static str {
    match value {
        StreamKind::Upload => "upload",
        StreamKind::Download => "download",
        StreamKind::StreamKindUnspecified => "unspecified",
    }
}

pub(crate) fn ticket_from_row(row: &Row) -> Result<TicketRecord> {
    Ok(TicketRecord {
        id: row.text("id")?.to_owned(),
        sandbox_id: row.text("sandbox_id")?.to_owned(),
        lease_id: row.text("lease_id")?.to_owned(),
        kind: stream_kind(row.text("kind")?)?,
        guest_path: row.text("guest_path")?.to_owned(),
        expires_at: row.int("expires_at")?,
        producer_claimed: row.bool("producer_claimed")?,
        consumer_claimed: row.bool("consumer_claimed")?,
        status: row.text("status")?.to_owned(),
        owner: super::types::Owner {
            node_id: row.text("owner_node")?.to_owned(),
            session_id: row.text("owner_session")?.to_owned(),
        },
    })
}

pub(crate) fn sandbox_state_from_wire(value: buffa::EnumValue<SandboxState>) -> SandboxState {
    value
        .as_known()
        .unwrap_or(SandboxState::SandboxStateUnspecified)
}
