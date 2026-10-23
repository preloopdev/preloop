//! The shared SQL backend: construction, metadata, transactions and helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;

use crate::api::*;
use crate::error::{Error, Result};

use super::backend::SchemaInfo;
use super::engine::{Row, SqlEngine, SqlTx, is_retryable_conflict};
use super::logic;
use super::postgres::PgEngine;
use super::seal::SealKey;
use super::sqlite::SqliteEngine;
use super::types::{BackendConfig, EnrollOutcome};
use super::{backend_kind, validate_url};

pub(crate) struct SqlStore {
    pub(crate) engine: Arc<dyn SqlEngine>,
    pub(crate) seal: SealKey,
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
                sqlite
                    .apply_schema(include_str!("schema.sqlite.sql"))
                    .await?;
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
                    // Two replicas may reach an empty database together.
                    // One wins the insert; the other waits on the row lock,
                    // then must agree with the fingerprint that landed.
                    tx.execute(
                        "INSERT INTO schema_meta (key, value) VALUES ('seal_key_fingerprint', $1) \
                         ON CONFLICT (key) DO NOTHING",
                        &[self.seal.fingerprint().into()],
                    )
                    .await?;
                    let stored = tx
                        .query(
                            "SELECT value FROM schema_meta WHERE key = 'seal_key_fingerprint'",
                            &[],
                        )
                        .await?
                        .first()
                        .map(|row| row.text("value").map(str::to_owned))
                        .transpose()?
                        .unwrap_or_default();
                    if !logic::constant_time_eq(
                        stored.as_bytes(),
                        self.seal.fingerprint().as_bytes(),
                    ) {
                        return Err(Error::failed_precondition(
                            "sandbox database was sealed with a different store key; \
                             configure the same PRELOOP_SANDBOX_SEAL_KEY on every replica",
                        ));
                    }
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
                .query(
                    "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                    &[],
                )
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
    ///
    /// Domain failures that must persist cleanup are returned as `Ok`
    /// outcomes by the command itself, never through this wrapper's `Err`:
    /// rolling back a failed acquire's cleanup would recreate the bug fixed
    /// in #619.
    pub(crate) async fn finish_tx<T>(tx: Box<dyn SqlTx>, outcome: Result<T>) -> Result<T> {
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

    /// Run one command in a transaction, retrying the whole closure when the
    /// engine reports a transaction conflict.
    ///
    /// Postgres may abort a transaction with a serialization failure
    /// (`40001`) or a detected deadlock (`40P01`) even though every statement
    /// was individually valid. The loser's writes are rolled back, so the only
    /// correct recovery is to run the command again from the top against the
    /// state the winner left behind — never to replay a partial attempt.
    ///
    /// The closure is [`Fn`] rather than [`FnOnce`] precisely so it can be
    /// re-entered; commands capture their inputs by value once and clone what
    /// each attempt moves. Only conflicts — never a command's own error —
    /// trigger a retry, so a domain failure is returned to the caller
    /// unchanged and its effects are not repeated.
    ///
    /// SQLite has a single writer and no conflicts of this kind; an embedded
    /// store therefore runs exactly one attempt.
    pub(crate) async fn run_tx<T, F>(&self, f: F) -> Result<T>
    where
        F: for<'a> Fn(
            &'a mut (dyn SqlTx + 'a),
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
    {
        let mut attempt = 1u32;
        loop {
            match self.run_tx_once(&f).await {
                Err(error) if attempt < TX_ATTEMPTS && is_retryable_conflict(&error) => {
                    tracing::debug!(
                        attempt,
                        "sandbox store transaction lost a conflict; retrying"
                    );
                    tokio::time::sleep(tx_retry_delay(attempt)).await;
                    attempt += 1;
                }
                outcome => return outcome,
            }
        }
    }

    async fn run_tx_once<T, F>(&self, f: &F) -> Result<T>
    where
        F: for<'a> Fn(
            &'a mut (dyn SqlTx + 'a),
        ) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
    {
        let mut tx = self.engine.begin().await?;
        let outcome = f(&mut *tx).await;
        Self::finish_tx(tx, outcome).await
    }
}

/// How many times a conflicting transaction is attempted in total.
const TX_ATTEMPTS: u32 = 5;

/// Jittered exponential pause before attempt `attempt + 1`, capped low enough
/// that a request never stalls on retries: two replicas that collided must not
/// march back into the lock in step, so each waits somewhere in the upper half
/// of its window. Time is used as the jitter source to avoid a dependency on
/// an RNG for one call site.
fn tx_retry_delay(attempt: u32) -> std::time::Duration {
    const FIRST_MS: u64 = 5;
    const CAP_MS: u64 = 200;
    let window = FIRST_MS
        .checked_shl(attempt.saturating_sub(1).min(6))
        .unwrap_or(CAP_MS)
        .min(CAP_MS);
    let spread = window / 2;
    let jitter = u64::from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.subsec_nanos())
            .unwrap_or_default(),
    );
    std::time::Duration::from_millis(window - spread + jitter % (spread + 1))
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

async fn certificates_for(
    tx: &mut dyn SqlTx,
    provider_id: &str,
) -> Result<Vec<ProviderCertificate>> {
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

pub(crate) async fn provider_id_for(tx: &mut dyn SqlTx, reference: &str) -> Result<String> {
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

pub(crate) async fn sandbox_by_id(tx: &mut dyn SqlTx, id: &str) -> Result<Option<Sandbox>> {
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
            "SELECT id, sandbox_id, pool, idempotency_key, expires_at, requested_ttl_secs, \
                    env_sealed, request_hash \
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
            &[
                owner.node_id.as_str().into(),
                owner.session_id.as_str().into(),
            ],
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

    async fn pool_revision(&self, name: &str) -> Result<Option<u64>> {
        super::commands_templates::pool_build_revision(self, name).await
    }

    async fn driver_get(
        &self,
        provider_id: &str,
        driver: &str,
    ) -> Result<Option<super::Versioned<DriverInfo>>> {
        super::commands_templates::driver_get(self, provider_id, driver).await
    }

    async fn resource_version(&self, kind: super::ResourceKind, id: &str) -> Result<Option<u64>> {
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

    async fn finish_injection(&self, lease_id: &str, owner: &super::Owner, ok: bool) -> Result<()> {
        super::commands_leases::finish_injection(self, lease_id, owner, ok).await
    }

    async fn renew(&self, lease_id: &str, ttl_secs: u64, default_ttl_secs: u64) -> Result<Lease> {
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
        owner: &super::Owner,
    ) -> Result<RegisterResponse> {
        super::commands_providers::register(self, request, enrolled, owner).await
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

    async fn provider_identity(
        &self,
        fingerprint_sha256: &str,
    ) -> Result<crate::service::ProviderIdentity> {
        super::commands_providers::provider_identity(self, fingerprint_sha256).await
    }

    async fn create_join_token(
        &self,
        id: &str,
        secret_sha256: &str,
        expires_at: i64,
        provider_name: &str,
    ) -> Result<()> {
        super::commands_providers::create_join_token(
            self,
            id,
            secret_sha256,
            expires_at,
            provider_name,
        )
        .await
    }

    async fn enroll(
        &self,
        token_id: &str,
        secret_sha256: &str,
        name: &str,
        csr_sha256: &str,
        cert_sha256: &str,
        cert_pem: &str,
        cert_expires_at: i64,
    ) -> Result<EnrollOutcome> {
        super::commands_providers::enroll(
            self,
            token_id,
            secret_sha256,
            name,
            csr_sha256,
            cert_sha256,
            cert_pem,
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
        super::commands_providers::poll_work(self, provider_id, generation, batch, delivery_timeout)
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

    async fn start_copy(
        &self,
        sandbox_id: &str,
        ticket: &str,
        guest_path: &str,
        kind: StreamKind,
        owner: &super::Owner,
    ) -> Result<String> {
        super::commands_runtime::start_copy(self, sandbox_id, ticket, guest_path, kind, owner).await
    }

    async fn create_exec(
        &self,
        exec_id: &str,
        request: ExecRequest,
        timeout_secs: u64,
        owner: &super::Owner,
    ) -> Result<super::ExecHandle> {
        super::commands_runtime::create_exec(self, exec_id, request, timeout_secs, owner).await
    }

    async fn report_exec(
        &self,
        request: ReportExecRequest,
        delivery_timeout: std::time::Duration,
    ) -> Result<super::ReportExecOutcome> {
        super::commands_runtime::report_exec(self, request, delivery_timeout).await
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
        super::commands_runtime::node_authenticate(self, node_id, session_id, credential_sha256)
            .await
    }

    async fn node_close(&self, node_id: &str, session_id: &str) -> Result<()> {
        super::commands_runtime::node_close(self, node_id, session_id).await
    }

    async fn owner_of(&self, kind: super::OwnerKind, id: &str) -> Result<Option<super::Owner>> {
        super::commands_runtime::owner_of(self, kind, id).await
    }

    async fn maintain(
        &self,
        config: &super::MaintenanceConfig,
    ) -> Result<super::MaintenanceOutcome> {
        super::commands_runtime::maintain(self, config).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::backend::engine::{Dialect, RetryableTxConflict, SqlValue};
    use crate::backend::seal::SealKey;
    use crate::backend::sqlite::SqliteEngine;
    use crate::error::Code;

    /// A SQLite engine whose first transaction loses a lock race: its commit
    /// rolls the attempt back and reports a Postgres serialization failure, so
    /// the wrapper has to run the command again against a clean database.
    struct ConflictOnce {
        inner: SqliteEngine,
        conflict: u32,
        commits: Arc<AtomicU32>,
        begins: Arc<AtomicU32>,
    }

    #[async_trait]
    impl SqlEngine for ConflictOnce {
        async fn begin(&self) -> Result<Box<dyn SqlTx>> {
            self.begins.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(ConflictTx {
                inner: self.inner.begin().await?,
                conflict: self.conflict,
                commits: self.commits.clone(),
            }))
        }
    }

    struct ConflictTx {
        inner: Box<dyn SqlTx>,
        /// How many commits lose the race before one is allowed through.
        conflict: u32,
        commits: Arc<AtomicU32>,
    }

    #[async_trait]
    impl SqlTx for ConflictTx {
        async fn execute(&mut self, sql: &str, params: &[SqlValue]) -> Result<u64> {
            self.inner.execute(sql, params).await
        }

        async fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>> {
            self.inner.query(sql, params).await
        }

        async fn commit(self: Box<Self>) -> Result<()> {
            let Self {
                inner,
                conflict,
                commits,
            } = *self;
            if conflict > commits.fetch_add(1, Ordering::SeqCst) {
                inner.rollback().await?;
                return Err(Error::unavailable("sandbox Postgres serialization failure")
                    .with_shared_source(Arc::new(RetryableTxConflict)));
            }
            inner.commit().await
        }

        async fn rollback(self: Box<Self>) -> Result<()> {
            self.inner.rollback().await
        }

        fn dialect(&self) -> Dialect {
            self.inner.dialect()
        }
    }

    /// A store over an in-memory SQLite schema, optionally wrapped so its
    /// first commit is a conflict.
    async fn store(dir: &std::path::Path, conflicts: u32) -> (SqlStore, Arc<AtomicU32>) {
        let sqlite = SqliteEngine::open(":memory:").unwrap();
        sqlite
            .apply_schema(include_str!("schema.sqlite.sql"))
            .await
            .unwrap();
        let begins = Arc::new(AtomicU32::new(0));
        let engine: Arc<dyn SqlEngine> = Arc::new(ConflictOnce {
            inner: sqlite,
            conflict: conflicts,
            commits: Arc::new(AtomicU32::new(0)),
            begins: begins.clone(),
        });
        let seal = SealKey::load(dir, None).unwrap();
        let store = SqlStore {
            engine,
            schema: SchemaInfo {
                version: 1,
                seal_fingerprint: seal.fingerprint().to_owned(),
                trust_fingerprint: None,
            },
            seal,
        };
        (store, begins)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_conflicted_command_runs_its_effect_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let (store, begins) = store(dir.path(), 1).await;
        store
            .run_tx(|tx| {
                Box::pin(async move {
                    tx.execute(
                        "CREATE TABLE retry_effect (id INTEGER PRIMARY KEY, note TEXT NOT NULL)",
                        &[],
                    )
                    .await?;
                    tx.execute("INSERT INTO retry_effect (note) VALUES ('once')", &[])
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        // The first attempt lost its commit and rolled back, so the row the
        // retry inserted is the only one — no partial or doubled effect.
        let count = store
            .run_tx(|tx| {
                Box::pin(async move {
                    tx.query("SELECT COUNT(*) AS total FROM retry_effect", &[])
                        .await?
                        .first()
                        .map(|row| row.int("total"))
                        .transpose()
                        .map(|total| total.unwrap_or_default())
                })
            })
            .await
            .unwrap();
        assert_eq!(count, 1, "the effect must land exactly once");
        assert_eq!(begins.load(Ordering::SeqCst), 3, "one retry plus the read");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_domain_failure_is_returned_without_rerunning_the_command() {
        let dir = tempfile::tempdir().unwrap();
        let (store, begins) = store(dir.path(), 0).await;
        let attempts = Arc::new(AtomicU32::new(0));
        let error = store
            .run_tx(|tx| {
                let attempts = attempts.clone();
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    tx.query("SELECT 1 AS one", &[]).await?;
                    Err::<(), _>(Error::failed_precondition("no capacity"))
                })
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, Code::FailedPrecondition);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "a command's own failure is never retried"
        );
        assert_eq!(begins.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retries_stop_after_the_bounded_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let (store, begins) = store(dir.path(), u32::MAX).await;
        let error = store
            .run_tx(|tx| {
                Box::pin(async move {
                    tx.query("SELECT 1 AS one", &[]).await?;
                    Ok(())
                })
            })
            .await
            .unwrap_err();
        assert!(
            error
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("serialization failure"),
            "the conflict is what the caller finally sees: {error:?}"
        );
        assert_eq!(
            begins.load(Ordering::SeqCst),
            TX_ATTEMPTS,
            "a losing command gives up after {TX_ATTEMPTS} attempts"
        );
    }
}
