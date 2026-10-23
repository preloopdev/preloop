//! Templates, pools and the read/version surface.

use crate::api::*;
use crate::error::{Error, Result};

use super::backend::SchemaInfo;
use super::engine::SqlTx;
use super::logic;
use super::rows;
use super::store::{SqlStore, bump_version, now_ms, outbox, provider_by_id};
use super::types::{PlacementSnapshot, ResourceKind, ResourceMeta, Versioned};

pub(crate) async fn schema_info(store: &SqlStore) -> Result<SchemaInfo> {
    Ok(store.schema.clone())
}

pub(crate) async fn set_trust_fingerprint(store: &SqlStore, fingerprint: &str) -> Result<()> {
    store
        .run_tx(|tx| {
            let fingerprint = fingerprint.to_owned();
            Box::pin(async move {
                let existing = tx
                    .query(
                        "SELECT value FROM schema_meta WHERE key = 'trust_fingerprint'",
                        &[],
                    )
                    .await?;
                match existing.first() {
                    Some(row) => {
                        if !logic::constant_time_eq(
                            row.text("value")?.as_bytes(),
                            fingerprint.as_bytes(),
                        ) {
                            return Err(Error::failed_precondition(
                                "sandbox database is bound to a different service CA; \
                                 every replica must share the same CA certificate",
                            ));
                        }
                    }
                    None => {
                        tx.execute(
                            "INSERT INTO schema_meta (key, value) \
                             VALUES ('trust_fingerprint', $1)",
                            &[fingerprint.into()],
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        })
        .await
}

pub(crate) async fn health(store: &SqlStore) -> Result<()> {
    store
        .run_tx(|tx| Box::pin(async move { tx.query("SELECT 1 AS one", &[]).await.map(|_| ()) }))
        .await
}

pub(crate) async fn close(store: &SqlStore) {
    store.engine.close().await;
}

pub(crate) async fn template_get(
    store: &SqlStore,
    name: &str,
) -> Result<Option<Versioned<Template>>> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates \
                         WHERE name = $1 AND retired = FALSE",
                        &[name.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                row.map(|row| {
                    Ok(Versioned {
                        value: rows::template_from_row(&row)?,
                        version: row.u64("version")?,
                    })
                })
                .transpose()
            })
        })
        .await
}

pub(crate) async fn pool_get(store: &SqlStore, name: &str) -> Result<Option<Versioned<PoolSpec>>> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT spec_json, version FROM pools WHERE name = $1",
                        &[name.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                row.map(|row| {
                    Ok(Versioned {
                        value: rows::pool_from_row(&row)?,
                        version: row.u64("version")?,
                    })
                })
                .transpose()
            })
        })
        .await
}

pub(crate) async fn pool_build_revision(store: &SqlStore, name: &str) -> Result<Option<u64>> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                tx.query("SELECT revision FROM pools WHERE name = $1", &[name.into()])
                    .await?
                    .first()
                    .map(|row| row.u64("revision"))
                    .transpose()
            })
        })
        .await
}

pub(crate) async fn driver_get(
    store: &SqlStore,
    provider_id: &str,
    driver: &str,
) -> Result<Option<Versioned<DriverInfo>>> {
    store
        .run_tx(|tx| {
            let provider_id = provider_id.to_owned();
            let driver = driver.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT drivers_json, generation FROM providers WHERE id = $1",
                        &[provider_id.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                let Some(row) = row else {
                    return Ok(None);
                };
                let generation = row.u64("generation")?;
                let drivers = rows::driver_list(row.text("drivers_json")?)?;
                Ok(drivers
                    .into_iter()
                    .find(|info| info.driver == driver)
                    .map(|value| Versioned {
                        value,
                        version: generation,
                    }))
            })
        })
        .await
}

pub(crate) async fn resource_version(
    store: &SqlStore,
    kind: ResourceKind,
    id: &str,
) -> Result<Option<u64>> {
    let (sql, id) = match kind {
        ResourceKind::Template => (
            "SELECT version FROM templates WHERE name = $1 AND retired = FALSE",
            id,
        ),
        ResourceKind::Pool => ("SELECT version FROM pools WHERE name = $1", id),
        ResourceKind::Sandbox => ("SELECT version FROM sandboxes WHERE id = $1", id),
        ResourceKind::Provider | ResourceKind::Driver => (
            "SELECT generation AS version FROM providers WHERE id = $1",
            id,
        ),
    };
    store
        .run_tx(|tx| {
            let sql = sql.to_owned();
            let id = id.to_owned();
            Box::pin(async move {
                tx.query(&sql, &[id.into()])
                    .await?
                    .first()
                    .map(|row| row.u64("version"))
                    .transpose()
            })
        })
        .await
}

pub(crate) async fn page_meta(
    store: &SqlStore,
    kind: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<(Vec<ResourceMeta>, bool)> {
    let table = match kind {
        "template" => "templates",
        "pool" => "pools",
        _ => return Err(Error::internal("unknown resource page kind")),
    };
    let retired = if table == "templates" {
        " AND retired = FALSE"
    } else {
        ""
    };
    let after = after.unwrap_or("").to_owned();
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    store
        .run_tx(|tx| {
            let sql = format!(
                "SELECT name AS id, version FROM {table} \
                 WHERE name > $1{retired} ORDER BY name LIMIT $2"
            );
            Box::pin(async move {
                let rows = tx.query(&sql, &[after.into(), (limit + 1).into()]).await?;
                let mut metas = Vec::with_capacity(rows.len());
                for row in &rows {
                    metas.push(ResourceMeta {
                        id: row.text("id")?.to_owned(),
                        version: row.u64("version")?,
                    });
                }
                let has_more = metas.len() > usize::try_from(limit).unwrap_or(usize::MAX);
                metas.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
                Ok((metas, has_more))
            })
        })
        .await
}

pub(crate) async fn template_get_at(
    store: &SqlStore,
    name: &str,
    version: u64,
) -> Result<Option<Versioned<Template>>> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates \
                         WHERE name = $1 AND version = $2 AND retired = FALSE",
                        &[name.into(), version.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                row.map(|row| {
                    Ok(Versioned {
                        value: rows::template_from_row(&row)?,
                        version: row.u64("version")?,
                    })
                })
                .transpose()
            })
        })
        .await
}

pub(crate) async fn pool_get_at(
    store: &SqlStore,
    name: &str,
    version: u64,
) -> Result<Option<Versioned<PoolSpec>>> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let row = tx
                    .query(
                        "SELECT spec_json, version FROM pools WHERE name = $1 AND version = $2",
                        &[name.into(), version.into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                row.map(|row| {
                    Ok(Versioned {
                        value: rows::pool_from_row(&row)?,
                        version: row.u64("version")?,
                    })
                })
                .transpose()
            })
        })
        .await
}

pub(crate) async fn put_template(store: &SqlStore, spec: TemplateSpec) -> Result<Template> {
    logic::validate_name("template", &spec.name)?;
    let sources = [
        !spec.artifacts.is_empty(),
        spec.recipe().is_some(),
        spec.snapshot_of().is_some(),
    ];
    if sources.iter().filter(|set| **set).count() != 1 {
        return Err(Error::invalid_argument(
            "exactly one template source is required: artifacts, recipe, or snapshotOf",
        ));
    }
    if spec.snapshot_of().is_some() {
        return Err(Error::invalid_argument(
            "snapshot templates are created by Snapshot",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for artifact in &spec.artifacts {
        artifact.validate().map_err(Error::invalid_argument)?;
        if !seen.insert((artifact.format.as_str(), artifact.arch.as_str())) {
            return Err(Error::invalid_argument(format!(
                "artifact for format `{}` and arch `{}` is declared twice",
                artifact.format, artifact.arch
            )));
        }
    }
    let digest = crate::service::template_digest(&spec);
    let name = spec.name.clone();
    let spec_json = serde_json::to_string(&spec).unwrap_or_default();
    let artifacts = spec.artifacts.clone();
    store
        .run_tx(|tx| {
            let spec = spec.clone();
            let name = name.clone();
            let spec_json = spec_json.clone();
            let artifacts = artifacts.clone();
            let digest = digest.clone();
            Box::pin(async move {
                let existing = tx
                    .query(
                        "SELECT digest, version, retired, provider_id, native_ref, driver, \
                                spec_json, created_at \
                         FROM templates WHERE name = $1",
                        &[name.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                if let Some(row) = &existing
                    && row.text("digest")? == digest
                    && !row.bool("retired")?
                {
                    return rows::template_from_row(row);
                }
                if let Some(row) = &existing {
                    let native_ref = row.text("native_ref")?.to_owned();
                    if !native_ref.is_empty() {
                        retire_snapshot(
                            tx,
                            &native_ref,
                            row.text("provider_id")?,
                            row.text("driver")?,
                        )
                        .await?;
                    }
                }
                let version = bump_version(tx).await?;
                let created_at = now_ms() / 1000;
                tx.execute(
                    "INSERT INTO templates \
                     (name, digest, spec_json, provider_id, native_ref, driver, created_at, \
                      version, retired, pending_drop) \
                     VALUES ($1, $2, $3, '', '', '', $4, $5, FALSE, FALSE) \
                     ON CONFLICT (name) DO UPDATE SET digest = $2, spec_json = $3, \
                      provider_id = '', native_ref = '', driver = '', created_at = $4, \
                      version = $5, retired = FALSE, pending_drop = FALSE",
                    &[
                        name.as_str().into(),
                        digest.as_str().into(),
                        spec_json.as_str().into(),
                        created_at.into(),
                        version.into(),
                    ],
                )
                .await?;
                tx.execute(
                    "DELETE FROM template_artifacts WHERE template = $1",
                    &[name.as_str().into()],
                )
                .await?;
                for artifact in &artifacts {
                    tx.execute(
                        "INSERT INTO template_artifacts \
                         (template, format, arch, digest, ref, size_bytes) \
                         VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            name.as_str().into(),
                            artifact.format.as_str().into(),
                            artifact.arch.as_str().into(),
                            artifact.digest.as_str().into(),
                            artifact.r#ref.as_str().into(),
                            i64::try_from(artifact.size_bytes)
                                .unwrap_or(i64::MAX)
                                .into(),
                        ],
                    )
                    .await?;
                }
                outbox(tx, "template", &name, version, "put", &spec_json).await?;
                Ok(Template {
                    spec: spec.into(),
                    digest,
                    created_at,
                    ..Template::default()
                })
            })
        })
        .await
}

pub(crate) async fn delete_template(store: &SqlStore, name: &str) -> Result<()> {
    store
        .run_tx(|tx| {
            let name = name.to_owned();
            Box::pin(async move {
                let pool = tx
                    .query(
                        "SELECT name FROM pools WHERE template = $1 LIMIT 1",
                        &[name.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next();
                if let Some(pool) = pool {
                    return Err(Error::failed_precondition(format!(
                        "template `{name}` is used by pool `{}`",
                        pool.text("name")?
                    )));
                }
                let row = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates \
                         WHERE name = $1 AND retired = FALSE",
                        &[name.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::not_found(format!("template `{name}` not found")))?;
                let template = rows::template_from_row(&row)?;
                // A live sandbox was created from this template: deleting
                // the logical template must not strand the running clone's
                // identity (its spec is immutable and still names the
                // template). Refuse until the clone is gone.
                let live = tx
                    .query(
                        "SELECT p.template_json FROM sandboxes s \
                         JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.state NOT IN ('gone', 'failed')",
                        &[],
                    )
                    .await?;
                for spec in live {
                    let used: Template =
                        rows::json(spec.text("template_json")?, "sandbox template")?;
                    if used.spec.as_option().is_some_and(|used| used.name == name) {
                        return Err(Error::failed_precondition(format!(
                            "template `{name}` is in use by a live sandbox; \
                             release the clone first"
                        )));
                    }
                }
                let version = bump_version(tx).await?;
                tx.execute(
                    "DELETE FROM templates WHERE name = $1",
                    &[name.as_str().into()],
                )
                .await?;
                tx.execute(
                    "DELETE FROM template_artifacts WHERE template = $1",
                    &[name.as_str().into()],
                )
                .await?;
                if !template.native_ref.is_empty() {
                    retire_snapshot(
                        tx,
                        &template.native_ref,
                        &template.provider_id,
                        &template.driver,
                    )
                    .await?;
                }
                outbox(tx, "template", &name, version, "delete", "{}").await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn list_templates(
    store: &SqlStore,
    request: &ListRequest,
) -> Result<ListTemplatesResponse> {
    let window = logic::page_window("template", request)?;
    let after = window.after.clone().unwrap_or_default();
    let limit = i64::try_from(window.limit).unwrap_or(i64::MAX);
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates \
                         WHERE name > $1 AND retired = FALSE ORDER BY name LIMIT $2",
                        &[after.into(), (limit + 1).into()],
                    )
                    .await?;
                let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
                let mut templates = Vec::new();
                let mut ids = Vec::new();
                for row in rows
                    .iter()
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                {
                    ids.push(row.text("name")?.to_owned());
                    templates.push(rows::template_from_row(row)?);
                }
                let token = if has_more {
                    logic::next_page_token("template", &ids, ids.len(), window.limit)
                } else {
                    String::new()
                };
                Ok(ListTemplatesResponse {
                    templates,
                    next_page_token: token,
                    ..ListTemplatesResponse::default()
                })
            })
        })
        .await
}

pub(crate) async fn list_templates_all(store: &SqlStore) -> Result<Vec<Template>> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT name, digest, spec_json, provider_id, native_ref, driver, \
                                created_at, version FROM templates \
                         WHERE retired = FALSE ORDER BY name",
                        &[],
                    )
                    .await?;
                rows.iter().map(rows::template_from_row).collect()
            })
        })
        .await
}

pub(crate) async fn put_pool(store: &SqlStore, pool: PoolSpec) -> Result<PoolSpec> {
    logic::validate_name("pool", &pool.name)?;
    let name = pool.name.clone();
    let template_name = pool.template.clone();
    let spec_json = serde_json::to_string(&pool).unwrap_or_default();
    store
        .run_tx(|tx| {
            let pool = pool.clone();
            let name = name.clone();
            let template_name = template_name.clone();
            let spec_json = spec_json.clone();
            Box::pin(async move {
                let template = tx
                    .query(
                        "SELECT digest FROM templates WHERE name = $1 AND retired = FALSE",
                        &[template_name.as_str().into()],
                    )
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::not_found(format!("template `{template_name}` not found")))?;
                let revision = logic::pool_revision(&pool, template.text("digest")?);
                let version = bump_version(tx).await?;
                tx.execute(
                    "INSERT INTO pools \
                     (name, template, spec_json, revision, version, warm, max_members, lease_ttl_secs) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                     ON CONFLICT (name) DO UPDATE SET template = $2, spec_json = $3, \
                      revision = $4, version = $5, warm = $6, max_members = $7, lease_ttl_secs = $8",
                    &[
                        name.as_str().into(),
                        template_name.as_str().into(),
                        spec_json.as_str().into(),
                        i64::try_from(revision).unwrap_or(i64::MAX).into(),
                        version.into(),
                        i64::from(pool.warm).into(),
                        i64::from(pool.max).into(),
                        i64::try_from(pool.lease_ttl_secs).unwrap_or(i64::MAX).into(),
                    ],
                )
                .await?;
                outbox(tx, "pool", &name, version, "put", &spec_json).await?;
                Ok(pool)
            })
        })
        .await
}

pub(crate) async fn delete_pool(store: &SqlStore, name: &str) -> Result<()> {
    let name = name.to_owned();
    store
        .run_tx(|tx| {
            let name = name.clone();
            Box::pin(async move {
                let existing = tx
                    .query(
                        "SELECT 1 AS present FROM pools WHERE name = $1",
                        &[name.as_str().into()],
                    )
                    .await?;
                if existing.is_empty() {
                    return Err(Error::not_found(format!("pool `{name}` not found")));
                }
                let idle = tx
                    .query(
                        "SELECT s.id FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE p.pool = $1 AND s.lease_id = '' \
                           AND s.state NOT IN ('gone', 'failed', 'terminating')",
                        &[name.as_str().into()],
                    )
                    .await?;
                let version = bump_version(tx).await?;
                tx.execute("DELETE FROM pools WHERE name = $1", &[name.as_str().into()])
                    .await?;
                for row in idle {
                    let id = row.text("id")?.to_owned();
                    super::commands_leases::destroy_sandbox(tx, &id).await?;
                }
                outbox(tx, "pool", &name, version, "delete", "{}").await?;
                Ok(())
            })
        })
        .await
}

pub(crate) async fn list_pools(
    store: &SqlStore,
    request: &ListRequest,
) -> Result<ListPoolsResponse> {
    let window = logic::page_window("pool", request)?;
    let after = window.after.clone().unwrap_or_default();
    let limit = i64::try_from(window.limit).unwrap_or(i64::MAX);
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query(
                        "SELECT spec_json, name FROM pools WHERE name > $1 \
                         ORDER BY name LIMIT $2",
                        &[after.into(), (limit + 1).into()],
                    )
                    .await?;
                let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
                let mut pools = Vec::new();
                let mut ids = Vec::new();
                for row in rows
                    .iter()
                    .take(usize::try_from(limit).unwrap_or(usize::MAX))
                {
                    ids.push(row.text("name")?.to_owned());
                    pools.push(rows::pool_from_row(row)?);
                }
                let token = if has_more {
                    logic::next_page_token("pool", &ids, ids.len(), window.limit)
                } else {
                    String::new()
                };
                Ok(ListPoolsResponse {
                    pools,
                    next_page_token: token,
                    ..ListPoolsResponse::default()
                })
            })
        })
        .await
}

pub(crate) async fn list_pools_all(store: &SqlStore) -> Result<Vec<PoolSpec>> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let rows = tx
                    .query("SELECT spec_json FROM pools ORDER BY name", &[])
                    .await?;
                rows.iter().map(rows::pool_from_row).collect()
            })
        })
        .await
}

pub(crate) async fn placement_snapshot(store: &SqlStore) -> Result<PlacementSnapshot> {
    store
        .run_tx(|tx| {
            Box::pin(async move {
                let providers = tx
                    .query(
                        "SELECT id, name, generation, drivers_json, last_seen, cordoned \
                         FROM providers ORDER BY id",
                        &[],
                    )
                    .await?
                    .iter()
                    .map(rows::provider_from_row)
                    .collect::<Result<Vec<_>>>()?;
                let counts = tx
                    .query(
                        "SELECT s.provider_id, p.driver, COUNT(*) AS live \
                         FROM sandboxes s JOIN sandbox_specs p ON p.id = s.id \
                         WHERE s.state NOT IN ('gone', 'failed', 'terminating') \
                         GROUP BY s.provider_id, p.driver",
                        &[],
                    )
                    .await?;
                let mut live = std::collections::BTreeMap::new();
                for row in counts {
                    live.insert(
                        (
                            row.text("provider_id")?.to_owned(),
                            row.text("driver")?.to_owned(),
                        ),
                        u32::try_from(row.int("live")?).unwrap_or(u32::MAX),
                    );
                }
                Ok(PlacementSnapshot { providers, live })
            })
        })
        .await
}

/// Remember a native snapshot base that a logical template no longer names.
/// It is destroyed once every queued create that embeds it has finished.
pub(crate) async fn retire_snapshot(
    tx: &mut dyn SqlTx,
    native_ref: &str,
    provider_id: &str,
    driver: &str,
) -> Result<()> {
    if native_ref.is_empty() {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO snapshot_bases (native_ref, provider_id, driver, drop_pending) \
         VALUES ($1, $2, $3, TRUE) \
         ON CONFLICT (native_ref) DO UPDATE SET drop_pending = TRUE",
        &[native_ref.into(), provider_id.into(), driver.into()],
    )
    .await?;
    drop_snapshot_if_unpinned(tx, native_ref).await
}

/// Drop a retired base once no pin remains.
pub(crate) async fn drop_snapshot_if_unpinned(tx: &mut dyn SqlTx, native_ref: &str) -> Result<()> {
    let pinned = tx
        .query(
            "SELECT 1 AS present FROM template_pins WHERE base_ref = $1 LIMIT 1",
            &[native_ref.into()],
        )
        .await?;
    if !pinned.is_empty() {
        return Ok(());
    }
    let base = tx
        .query(
            "SELECT native_ref, provider_id, driver FROM snapshot_bases WHERE native_ref = $1",
            &[native_ref.into()],
        )
        .await?
        .into_iter()
        .next();
    let Some(base) = base else {
        return Ok(());
    };
    let provider_id = base.text("provider_id")?.to_owned();
    let driver = base.text("driver")?.to_owned();
    if provider_by_id(tx, &provider_id).await?.is_some() {
        let work_id = format!("wrk-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
        let target = SandboxWork {
            sandbox_id: String::new(),
            driver,
            native_ref: format!("snapshot:{native_ref}"),
            ..SandboxWork::default()
        };
        let work = WorkItem::new(work_id.clone(), work_item::Work::Destroy(target.into()));
        let generation = provider_by_id(tx, &provider_id)
            .await?
            .map(|provider| provider.generation)
            .unwrap_or(0);
        super::commands_providers::insert_work(
            tx,
            None,
            &provider_id,
            generation,
            &work,
            "destroy",
            &super::commands_providers::WorkPayload {
                work: work.clone(),
                ..Default::default()
            },
            &format!("snapshot-drop:{native_ref}"),
            now_ms(),
            now_ms() + 86_400_000,
        )
        .await?;
    }
    tx.execute(
        "DELETE FROM snapshot_bases WHERE native_ref = $1",
        &[native_ref.into()],
    )
    .await?;
    Ok(())
}
