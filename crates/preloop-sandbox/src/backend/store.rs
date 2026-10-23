//! The shared SQL backend: construction, metadata, transactions and helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;

use crate::api::*;
use crate::error::{Error, Result};

use super::engine::{Dialect, Row, SqlEngine, SqlTx, SqlValue};
use super::logic;
use super::postgres::PgEngine;
use super::seal::SealKey;
use super::sqlite::SqliteEngine;
use super::backend::SchemaInfo;
use super::types::BackendConfig;
use super::{backend_kind, validate_url};

pub(crate) struct SqlStore {
    pub(crate) engine: Arc<dyn SqlEngine>,
    pub(crate) seal: SealKey,
    pub(crate) node_id: String,
    pub(crate) session_id: String,
    pub(crate) credential_sha256: String,
    pub(crate) peer_url: String,
    pub(crate) node_lost_after: Duration,
    pub(crate) notify: Arc<Notify>,
    pub(crate) schema: SchemaInfo,
}

impl SqlStore {
    pub(crate) async fn open(config: BackendConfig) -> Result<Arc<dyn super::SandboxBackend>> {
        Self::open_inner(config, true).await
    }

    pub(crate) async fn open_embedded(
        config: BackendConfig,
    ) -> Result<Arc<dyn super::SandboxBackend>> {
        Self::open_inner(config, false).await
    }

    async fn open_inner(
        config: BackendConfig,
        register_node: bool,
    ) -> Result<Arc<dyn super::SandboxBackend>> {
        validate_url(&config.url)?;
        std::fs::create_dir_all(&config.state_dir)
            .map_err(|error| Error::internal(format!("sandbox state dir: {error}")))?;
        let seal = SealKey::load(&config.state_dir, config.seal_key)?;
        let notify = Arc::new(Notify::new());
        let engine: Arc<dyn SqlEngine> = match backend_kind(&config.url) {
            "postgres" => {
                let pg = PgEngine::open(&config.url, config.pool_size).await?;
                pg.apply_schema(include_str!("schema.postgres.sql")).await?;
                pg.listen(notify.clone()).await;
                Arc::new(pg)
            }
            "sqlite" => {
                let path = sqlite_path(&config.url);
                let sqlite = SqliteEngine::open(&path)?;
                sqlite.apply_schema(include_str!("schema.sqlite.sql")).await?;
                Arc::new(sqlite)
            }
            _ => {
                return Err(Error::invalid_argument(format!(
                    "unsupported sandbox store URL {:?}",
                    config.url
                )));
            }
        };
        let store = Arc::new(Self {
            engine,
            seal,
            node_id: config.node_id.clone(),
            session_id: config.session_id.clone(),
            credential_sha256: config.node_credential_sha256.clone(),
            peer_url: config.peer_url.clone(),
            node_lost_after: config.node_lost_after,
            notify,
            schema: SchemaInfo {
                version: 1,
                seal_fingerprint: String::new(),
                trust_fingerprint: None,
            },
        });
        let schema = store.ensure_meta().await?;
        let store = Arc::new(Self {
            schema,
            ..Arc::try_unwrap(store).map_err(|_| Error::internal("store still shared"))?
        });
        if register_node {
            super::SandboxBackend::node_register(&*store, &config).await?;
        }
        Ok(store)
    }

    async fn ensure_meta(&self) -> Result<SchemaInfo> {
        let mut tx = self.engine.begin().await?;
        let outcome: Result<SchemaInfo> = async {
            let existing = tx
                .query(
                    "SELECT value FROM schema_meta WHERE key = 'seal_key_fingerprint'",
                    &[],
                )
                .await?;
            match existing.first() {
                Some(row) => {
                    if !logic::constant_time_eq(
                        row.text("value")?.as_bytes(),
                        self.seal.fingerprint().as_bytes(),
                    ) {
                        return Err(Error::failed_precondition(
                            "sandbox database was sealed with a different store key; \
                             configure the same PRELOOP_SANDBOX_SEAL_KEY on every replica",
                        ));
                    }
                }
                None => {
                    tx.execute(
                        "INSERT INTO schema_meta (key, value) VALUES ('seal_key_fingerprint', $1)",
                        &[self.seal.fingerprint().into()],
                    )
                    .await?;
                }
            }
            tx.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('schema_version', '1') \
                 ON CONFLICT (key) DO NOTHING",
                &[],
            )
            .await?;
            let trust = tx
                .query(
                    "SELECT value FROM schema_meta WHERE key = 'trust_fingerprint'",
                    &[],
                )
                .await?
                .first()
                .map(|row| row.text("value").map(str::to_owned))
                .transpose()?;
            let version = match tx
                .query("SELECT value FROM schema_meta WHERE key = 'schema_version'", &[])
                .await?
                .first()
            {
                Some(row) => row.text("value")?.parse::<u64>().unwrap_or(1),
                None => 1,
            };
            Ok(SchemaInfo {
                version,
                seal_fingerprint: self.seal.fingerprint().to_owned(),
                trust_fingerprint: trust,
            })
        }
        .await;
        Self::finish_tx(tx, outcome).await
    }

    /// Commit on `Ok`, roll back on `Err`.
    pub(crate) async fn finish_tx<T>(
        tx: Box<dyn SqlTx>,
        outcome: Result<T>,
    ) -> Result<T> {
        match outcome {
            Ok(value) => {
                tx.commit().await?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    pub(crate) async fn run_tx<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a> FnOnce(
            &'a mut (dyn SqlTx + 'a),
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
    {
        let mut tx = self.engine.begin().await?;
        let outcome = f(&mut *tx).await;
        Self::finish_tx(tx, outcome).await
    }

    pub(crate) fn dialect(&self) -> Dialect {
        self.engine.dialect()
    }
}

/// Map a store URL to a SQLite path.
fn sqlite_path(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed == "memory:" {
        return ":memory:".to_owned();
    }
    if let Some(rest) = trimmed
        .strip_prefix("sqlite://")
        .or_else(|| trimmed.strip_prefix("sqlite:"))
    {
        return if rest.is_empty() {
            ":memory:".to_owned()
        } else {
            rest.to_owned()
        };
    }
    trimmed.to_owned()
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

pub(crate) async fn bump_version(tx: &mut dyn SqlTx) -> Result<u64> {
    let rows = tx
        .query(
            "UPDATE version_seq SET next = next + 1 WHERE id = 1 RETURNING next",
            &[],
        )
        .await?;
    rows.first()
        .map(|row| row.u64("next"))
        .transpose()
        .map(|value| value.unwrap_or(1))
}

pub(crate) async fn outbox(
    tx: &mut dyn SqlTx,
    kind: &str,
    id: &str,
    version: u64,
    event: &str,
    payload: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO outbox (resource_kind, resource_id, version, event, payload_json, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            kind.into(),
            id.into(),
            version.into(),
            event.into(),
            payload.into(),
            now_ms().into(),
        ],
    )
    .await?;
    Ok(())
}

pub(crate) async fn provider_by_id(tx: &mut dyn SqlTx, id: &str) -> Result<Option<Provider>> {
    let row = tx
        .query(
            "SELECT id, name, generation, drivers_json, last_seen, cordoned FROM providers WHERE id = $1",
            &[id.into()],
        )
        .await?
        .into_iter()
        .next();
    match row {
        Some(row) => {
            let mut provider = super::rows::provider_from_row(&row)?;
            provider.certificates = certificates_for(tx, &provider.id).await?;
            Ok(Some(provider))
        }
        None => Ok(None),
    }
}

pub(crate) async fn provider_by_name(tx: &mut dyn SqlTx, name: &str) -> Result<Option<Provider>> {
    let row = tx
        .query(
            "SELECT id, name, generation, drivers_json, last_seen, cordoned FROM providers WHERE name = $1",
            &[name.into()],
        )
        .await?
        .into_iter()
        .next();
    match row {
        Some(row) => {
            let mut provider = super::rows::provider_from_row(&row)?;
            provider.certificates = certificates_for(tx, &provider.id).await?;
            Ok(Some(provider))
        }
        None => Ok(None),
    }
}

async fn certificates_for(tx: &mut dyn SqlTx, provider_id: &str) -> Result<Vec<ProviderCertificate>> {
    let rows = tx
        .query(
            "SELECT sha256, expires_at FROM provider_certificates WHERE provider_id = $1",
            &[provider_id.into()],
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ProviderCertificate {
                sha256: row.text("sha256")?.to_owned(),
                expires_at: row.int("expires_at")?,
                ..ProviderCertificate::default()
            })
        })
        .collect()
}

pub(crate) async fn fence_provider(
    tx: &mut dyn SqlTx,
    provider_id: &str,
    generation: u64,
) -> Result<Provider> {
    let provider = provider_by_id(tx, provider_id)
        .await?
        .ok_or_else(|| Error::not_found("provider not registered"))?;
    if provider.generation != generation {
        return Err(Error::aborted(format!(
            "stale generation {generation} (current {})",
            provider.generation
        )));
    }
    Ok(provider)
}

pub(crate) async fn provider_id_for(
    tx: &mut dyn SqlTx,
    reference: &str,
) -> Result<String> {
    if reference.is_empty() {
        return Err(Error::invalid_argument("provider name is required"));
    }
    let row = tx
        .query(
            "SELECT id FROM providers WHERE id = $1 OR name = $1 LIMIT 1",
            &[reference.into()],
        )
        .await?
        .into_iter()
        .next();
    row.map(|row| row.text("id").map(str::to_owned))
        .transpose()?
        .ok_or_else(|| Error::not_found(format!("provider `{reference}` not found")))
}

pub(crate) async fn sandbox_by_id(
    tx: &mut dyn SqlTx,
    id: &str,
) -> Result<Option<Sandbox>> {
    let row = tx
        .query(
            "SELECT s.id, s.name, s.state, s.lease_id, s.native_ref, s.error, s.provider_id, \
                    s.created_at, s.preserve_until, s.version, p.template_json, p.shape_json, \
                    p.pool, p.purpose, p.driver \
             FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id WHERE s.id = $1",
            &[id.into()],
        )
        .await?
        .into_iter()
        .next();
    row.as_ref().map(super::rows::sandbox_from_row).transpose()
}

pub(crate) async fn sandbox_version(tx: &mut dyn SqlTx, id: &str) -> Result<Option<u64>> {
    Ok(tx
        .query("SELECT version FROM sandboxes WHERE id = $1", &[id.into()])
        .await?
        .first()
        .map(|row| row.u64("version"))
        .transpose()?)
}

pub(crate) async fn lease_by_id(
    tx: &mut dyn SqlTx,
    id: &str,
    seal: &SealKey,
) -> Result<Option<Lease>> {
    match lease_row(tx, id).await? {
        Some(row) => Ok(Some(super::rows::lease_from_row(&row, seal)?)),
        None => Ok(None),
    }
}

/// One raw lease row; commands decode it with the store's seal key.
pub(crate) async fn lease_row(tx: &mut dyn SqlTx, id: &str) -> Result<Option<Row>> {
    Ok(tx
        .query(
            "SELECT id, sandbox_id, pool, idempotency_key, expires_at, env_sealed, request_hash \
             FROM leases WHERE id = $1",
            &[id.into()],
        )
        .await?
        .into_iter()
        .next())
}

pub(crate) async fn active_lease(
    tx: &mut dyn SqlTx,
    sandbox_id: &str,
    lease_id: &str,
) -> Result<bool> {
    if lease_id.is_empty() {
        return Ok(false);
    }
    let rows = tx
        .query(
            "SELECT 1 AS present FROM sandboxes s JOIN leases l ON l.id = s.lease_id \
             WHERE s.id = $1 AND s.lease_id = $2 AND l.sandbox_id = s.id AND l.expires_at > $3",
            &[sandbox_id.into(), lease_id.into(), now_secs().into()],
        )
        .await?;
    Ok(!rows.is_empty())
}

pub(crate) async fn node_alive(tx: &mut dyn SqlTx, owner: &super::types::Owner) -> Result<bool> {
    let rows = tx
        .query(
            "SELECT 1 AS present FROM nodes \
             WHERE node_id = $1 AND session_id = $2 AND alive = TRUE",
            &[owner.node_id.as_str().into(), owner.session_id.as_str().into()],
        )
        .await?;
    Ok(!rows.is_empty())
}

pub(crate) async fn driver_caps(
    tx: &mut dyn SqlTx,
    provider_id: &str,
    driver: &str,
) -> Result<Option<Capabilities>> {
    let Some(provider) = provider_by_id(tx, provider_id).await? else {
        return Ok(None);
    };
    Ok(provider
        .drivers
        .iter()
        .find(|info| info.driver == driver)
        .map(|info| (*info.capabilities).clone()))
}

pub(crate) async fn touch_provider(tx: &mut dyn SqlTx, provider_id: &str) -> Result<()> {
    tx.execute(
        "UPDATE providers SET last_seen = $2, seen_at = $3 WHERE id = $1",
        &[provider_id.into(), now_secs().into(), now_ms().into()],
    )
    .await?;
    Ok(())
}

pub(crate) async fn work_exists(tx: &mut dyn SqlTx, work_id: &str) -> Result<bool> {
    Ok(!tx
        .query("SELECT 1 AS present FROM work_items WHERE id = $1", &[work_id.into()])
        .await?
        .is_empty())
}

pub(crate) fn json_param<T: serde::Serialize>(value: &T) -> SqlValue {
    SqlValue::Text(serde_json::to_string(value).unwrap_or_default())
}

// ── The trait implementation: every method is one command ─────────

#[async_trait::async_trait]
impl super::SandboxBackend for SqlStore {
    async fn schema_info(&self) -> Result<SchemaInfo> {
        super::commands_templates::schema_info(self).await
    }

    async fn set_trust_fingerprint(&self, fingerprint: &str) -> Result<()> {
        super::commands_templates::set_trust_fingerprint(self, fingerprint).await
    }

    async fn health(&self) -> Result<()> {
        super::commands_templates::health(self).await
    }

    async fn close(&self) {
        super::commands_templates::close(self).await;
    }

    async fn template_get(&self, name: &str) -> Result<Option<super::Versioned<Template>>> {
        super::commands_templates::template_get(self, name).await
    }

    async fn pool_get(&self, name: &str) -> Result<Option<super::Versioned<PoolSpec>>> {
        super::commands_templates::pool_get(self, name).await
    }

    async fn driver_get(
        &self,
        provider_id: &str,
        driver: &str,
    ) -> Result<Option<super::Versioned<DriverInfo>>> {
        super::commands_templates::driver_get(self, provider_id, driver).await
    }

    async fn resource_version(
        &self,
        kind: super::ResourceKind,
        id: &str,
    ) -> Result<Option<u64>> {
        super::commands_templates::resource_version(self, kind, id).await
    }

    async fn template_page_meta(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<super::ResourceMeta>, bool)> {
        super::commands_templates::page_meta(self, "template", after, limit).await
    }

    async fn pool_page_meta(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<super::ResourceMeta>, bool)> {
        super::commands_templates::page_meta(self, "pool", after, limit).await
    }

    async fn template_get_at(
        &self,
        name: &str,
        version: u64,
    ) -> Result<Option<super::Versioned<Template>>> {
        super::commands_templates::template_get_at(self, name, version).await
    }

    async fn pool_get_at(
        &self,
        name: &str,
        version: u64,
    ) -> Result<Option<super::Versioned<PoolSpec>>> {
        super::commands_templates::pool_get_at(self, name, version).await
    }

    async fn put_template(&self, spec: TemplateSpec) -> Result<Template> {
        super::commands_templates::put_template(self, spec).await
    }

    async fn delete_template(&self, name: &str) -> Result<()> {
        super::commands_templates::delete_template(self, name).await
    }

    async fn list_templates(&self, request: &ListRequest) -> Result<ListTemplatesResponse> {
        super::commands_templates::list_templates(self, request).await
    }

    async fn list_templates_all(&self) -> Result<Vec<Template>> {
        super::commands_templates::list_templates_all(self).await
    }

    async fn put_pool(&self, pool: PoolSpec) -> Result<PoolSpec> {
        super::commands_templates::put_pool(self, pool).await
    }

    async fn delete_pool(&self, name: &str) -> Result<()> {
        super::commands_templates::delete_pool(self, name).await
    }

    async fn list_pools(&self, request: &ListRequest) -> Result<ListPoolsResponse> {
        super::commands_templates::list_pools(self, request).await
    }

    async fn list_pools_all(&self) -> Result<Vec<PoolSpec>> {
        super::commands_templates::list_pools_all(self).await
    }

    async fn placement_snapshot(&self) -> Result<super::PlacementSnapshot> {
        super::commands_templates::placement_snapshot(self).await
    }

    async fn acquire(&self, command: super::AcquireCommand) -> Result<super::AcquireStep> {
        super::commands_leases::acquire(self, command).await
    }

    async fn create_pool_member(
        &self,
        member: super::NewSandbox,
        create_timeout: std::time::Duration,
    ) -> Result<super::RefillOutcome> {
        super::commands_leases::create_pool_member(self, member, create_timeout).await
    }

    async fn finish_injection(
        &self,
        lease_id: &str,
        owner: &super::Owner,
        ok: bool,
    ) -> Result<()> {
        super::commands_leases::finish_injection(self, lease_id, owner, ok).await
    }

    async fn renew(
        &self,
        lease_id: &str,
        ttl_secs: u64,
        default_ttl_secs: u64,
    ) -> Result<Lease> {
        super::commands_leases::renew(self, lease_id, ttl_secs, default_ttl_secs).await
    }

    async fn release(
        &self,
        lease_id: &str,
        mode: ReleaseMode,
        preserve_ttl_secs: u64,
        default_preserve_secs: u64,
    ) -> Result<()> {
        super::commands_leases::release(
            self,
            lease_id,
            mode,
            preserve_ttl_secs,
            default_preserve_secs,
        )
        .await
    }

    async fn get(&self, id: &str, lease_id: &str) -> Result<Sandbox> {
        super::commands_leases::get(self, id, lease_id).await
    }

    async fn list(&self, request: &ListRequest) -> Result<ListSandboxesResponse> {
        super::commands_leases::list(self, request).await
    }

    async fn list_all(&self) -> Result<Vec<Sandbox>> {
        super::commands_leases::list_all(self).await
    }

    async fn lease_env(&self, lease_id: &str) -> Result<StringMap> {
        super::commands_leases::lease_env(self, lease_id).await
    }

    async fn register_waiter(&self, waiter: super::Waiter) -> Result<()> {
        super::commands_leases::register_waiter(self, waiter).await
    }

    async fn clear_waiter(&self, key: &str, owner: &super::Owner) -> Result<()> {
        super::commands_leases::clear_waiter(self, key, owner).await
    }

    async fn register(
        &self,
        request: RegisterRequest,
        enrolled: Option<&str>,
    ) -> Result<RegisterResponse> {
        super::commands_providers::register(self, request, enrolled).await
    }

    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<()> {
        super::commands_providers::heartbeat(self, request).await
    }

    async fn set_cordon(&self, reference: &str, cordoned: bool) -> Result<Provider> {
        super::commands_providers::set_cordon(self, reference, cordoned).await
    }

    async fn drain(&self, reference: &str) -> Result<DrainProviderResult> {
        super::commands_providers::drain(self, reference).await
    }

    async fn delete_provider(&self, name: &str) -> Result<()> {
        super::commands_providers::delete_provider(self, name).await
    }

    async fn list_providers(&self, request: &ListRequest) -> Result<ListProvidersResponse> {
        super::commands_providers::list_providers(self, request).await
    }

    async fn list_providers_all(&self) -> Result<Vec<Provider>> {
        super::commands_providers::list_providers_all(self).await
    }

    async fn provider_identity(&self, fingerprint_sha256: &str) -> Result<crate::service::ProviderIdentity> {
        super::commands_providers::provider_identity(self, fingerprint_sha256).await
    }

    async fn create_join_token(
        &self,
        id: &str,
        secret_sha256: &str,
        expires_at: i64,
    ) -> Result<()> {
        super::commands_providers::create_join_token(self, id, secret_sha256, expires_at).await
    }

    async fn enroll(
        &self,
        token_id: &str,
        secret_sha256: &str,
        name: &str,
        cert_sha256: &str,
        cert_expires_at: i64,
    ) -> Result<()> {
        super::commands_providers::enroll(
            self,
            token_id,
            secret_sha256,
            name,
            cert_sha256,
            cert_expires_at,
        )
        .await
    }

    async fn renew_certificate(
        &self,
        fingerprint_sha256: &str,
        cert_sha256: &str,
        cert_expires_at: i64,
    ) -> Result<()> {
        super::commands_providers::renew_certificate(
            self,
            fingerprint_sha256,
            cert_sha256,
            cert_expires_at,
        )
        .await
    }

    async fn poll_work(
        &self,
        provider_id: &str,
        generation: u64,
        batch: usize,
        delivery_timeout: std::time::Duration,
    ) -> Result<Vec<WorkItem>> {
        super::commands_providers::poll_work(
            self,
            provider_id,
            generation,
            batch,
            delivery_timeout,
        )
        .await
    }

    async fn work_result(&self, work_id: &str) -> Result<Option<super::WorkResult>> {
        super::commands_providers::work_result(self, work_id).await
    }

    async fn report(&self, request: ReportRequest) -> Result<()> {
        super::commands_providers::report(self, request).await
    }

    async fn reconcile(&self, request: ReconcileRequest) -> Result<ReconcileResponse> {
        super::commands_providers::reconcile(self, request).await
    }

    async fn create_exec(
        &self,
        request: ExecRequest,
        timeout_secs: u64,
        owner: &super::Owner,
    ) -> Result<super::ExecHandle> {
        super::commands_runtime::create_exec(self, request, timeout_secs, owner).await
    }

    async fn report_exec(
        &self,
        request: ReportExecRequest,
    ) -> Result<super::ReportExecOutcome> {
        super::commands_runtime::report_exec(self, request).await
    }

    async fn finish_exec(&self, exec_id: &str, code: i32) -> Result<()> {
        super::commands_runtime::finish_exec(self, exec_id, code).await
    }

    async fn fail_exec(&self, exec_id: &str, reason: &str) -> Result<()> {
        super::commands_runtime::fail_exec(self, exec_id, reason).await
    }

    async fn cancel_exec(&self, exec_id: &str, owner: &super::Owner) -> Result<()> {
        super::commands_runtime::cancel_exec(self, exec_id, owner).await
    }

    async fn exec_status(&self, exec_id: &str) -> Result<Option<super::ExecStatus>> {
        super::commands_runtime::exec_status(self, exec_id).await
    }

    async fn create_ticket(
        &self,
        id: &str,
        sandbox_id: &str,
        lease_id: &str,
        kind: StreamKind,
        guest_path: &str,
        token_sha256: &str,
        expires_at: i64,
        owner: &super::Owner,
    ) -> Result<()> {
        super::commands_runtime::create_ticket(
            self,
            id,
            sandbox_id,
            lease_id,
            kind,
            guest_path,
            token_sha256,
            expires_at,
            owner,
        )
        .await
    }

    async fn claim_ticket(
        &self,
        id: &str,
        side: super::TicketSide,
        expected_kind: StreamKind,
        claim_ttl_secs: i64,
        authority: super::TicketAuthority<'_>,
    ) -> Result<super::TicketClaim> {
        super::commands_runtime::claim_ticket(
            self,
            id,
            side,
            expected_kind,
            claim_ttl_secs,
            authority,
        )
        .await
    }

    async fn finish_ticket(&self, id: &str, status: &str, reason: &str) -> Result<()> {
        super::commands_runtime::finish_ticket(self, id, status, reason).await
    }

    async fn ticket_owner(&self, id: &str) -> Result<Option<super::Owner>> {
        super::commands_runtime::ticket_owner(self, id).await
    }

    async fn snapshot_start(
        &self,
        command: super::SnapshotCommand,
    ) -> Result<super::SnapshotStart> {
        super::commands_runtime::snapshot_start(self, command).await
    }

    async fn power(
        &self,
        sandbox_id: &str,
        lease_id: &str,
        op: super::PowerOp,
        timeout: std::time::Duration,
    ) -> Result<String> {
        super::commands_runtime::power(self, sandbox_id, lease_id, op, timeout).await
    }

    async fn stats(&self) -> Result<StatsResponse> {
        super::commands_runtime::stats(self).await
    }

    async fn node_register(&self, config: &BackendConfig) -> Result<()> {
        super::commands_runtime::node_register(
            self,
            &config.node_id,
            &config.session_id,
            &config.node_credential_sha256,
            &config.peer_url,
        )
        .await
    }

    async fn node_heartbeat(&self, node_id: &str, session_id: &str) -> Result<()> {
        super::commands_runtime::node_heartbeat(self, node_id, session_id).await
    }

    async fn node_authenticate(
        &self,
        node_id: &str,
        session_id: &str,
        credential_sha256: &str,
    ) -> Result<bool> {
        super::commands_runtime::node_authenticate(
            self,
            node_id,
            session_id,
            credential_sha256,
        )
        .await
    }

    async fn node_close(&self, node_id: &str, session_id: &str) -> Result<()> {
        super::commands_runtime::node_close(self, node_id, session_id).await
    }

    async fn owner_of(
        &self,
        kind: super::OwnerKind,
        id: &str,
    ) -> Result<Option<super::Owner>> {
        super::commands_runtime::owner_of(self, kind, id).await
    }

    async fn maintain(
        &self,
        config: &super::MaintenanceConfig,
    ) -> Result<super::MaintenanceOutcome> {
        super::commands_runtime::maintain(self, config).await
    }
}
