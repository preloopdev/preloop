//! Azure-protocol timelines (`timelines` / `timeline_records`), keyed by the
//! attempt's `timeline_id`.

use super::codec::now_us;
use super::{LiteBackend, db};
use crate::control::types::{ControlError, MAX_TIMELINE_RECORDS, stamp_timeline_records};
use preloop_gha_protocol::azdo::TimelineRecord;
use rusqlite::{OptionalExtension, params};

/// The timeline id addressed by a `'{plan_id}/{timeline_id}'` key: the uuid
/// after the last `/` (decision round 1, Q1). `None` when it is not a uuid.
fn timeline_id(timeline_key: &str) -> Option<String> {
    let tail = timeline_key.rsplit('/').next().unwrap_or(timeline_key);
    tail.parse::<uuid::Uuid>().ok().map(|id| id.to_string())
}

fn decode_records(
    stmt: &mut rusqlite::CachedStatement<'_>,
    params: impl rusqlite::Params,
) -> Result<Vec<TimelineRecord>, ControlError> {
    let rows = stmt
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(db)?;
    let mut records = Vec::new();
    for row in rows {
        let json = row.map_err(db)?;
        let record = serde_json::from_str(&json).map_err(ControlError::backend)?;
        records.push(record);
    }
    Ok(records)
}

impl LiteBackend {
    /// Apply one timeline PATCH in one transaction:
    /// 1. the attempt must exist (`job_requests.timeline_id`), else `NotFound`;
    /// 2. `INSERT INTO timelines .. ON CONFLICT DO UPDATE SET change_id =
    ///    change_id + 1 RETURNING change_id`;
    /// 3. upsert each stamped record into `timeline_records`;
    /// 4. evict the lowest record ids past [`MAX_TIMELINE_RECORDS`],
    ///    protecting the records this PATCH wrote;
    /// 5. return the change id and every stored record ordered by record id.
    pub(crate) async fn patch_timeline(
        &self,
        timeline_key: &str,
        mut records: Vec<TimelineRecord>,
    ) -> Result<(i32, Vec<TimelineRecord>), ControlError> {
        let not_found = || ControlError::NotFound(format!("timeline {timeline_key}"));
        let timeline = timeline_id(timeline_key).ok_or_else(not_found)?;
        self.write(|tx| {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM job_requests WHERE timeline_id = ?1)",
                    [&timeline],
                    |row| row.get(0),
                )
                .map_err(db)?;
            if !exists {
                return Err(not_found());
            }
            let change_id: i64 = tx
                .query_row(
                    "INSERT INTO timelines (timeline_id, change_id) VALUES (?1, 1) \
                     ON CONFLICT (timeline_id) DO UPDATE SET change_id = change_id + 1 \
                     RETURNING change_id",
                    [&timeline],
                    |row| row.get(0),
                )
                .map_err(db)?;
            let now = super::codec::us_to_system(now_us());
            {
                let mut upsert = tx
                    .prepare_cached(
                        "INSERT INTO timeline_records (timeline_id, record_id, change_id, record) \
                         VALUES (?1, ?2, ?3, ?4) \
                         ON CONFLICT (timeline_id, record_id) DO UPDATE SET \
                             change_id = excluded.change_id, record = excluded.record",
                    )
                    .map_err(db)?;
                for (record_id, body) in stamp_timeline_records(&mut records, change_id, now) {
                    upsert
                        .execute(params![timeline, record_id, change_id, body])
                        .map_err(db)?;
                }
            }
            // Write-side bound: the read LIMIT alone let `timeline_records`
            // grow without limit and could omit a record this PATCH wrote
            // from the response. Evict the lowest record ids past the cap,
            // never a record stamped with this PATCH's `change_id`; a PATCH
            // larger than the cap falls through to evicting the lowest
            // regardless so the bound always holds.
            let cap = MAX_TIMELINE_RECORDS as i64;
            let count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM timeline_records WHERE timeline_id = ?1",
                    [&timeline],
                    |row| row.get(0),
                )
                .map_err(db)?;
            if count > cap {
                let excess = count - cap;
                tx.prepare_cached(
                    "DELETE FROM timeline_records WHERE timeline_id = ?1 AND change_id <> ?2 \
                       AND record_id IN (SELECT record_id FROM timeline_records \
                                         WHERE timeline_id = ?1 AND change_id <> ?2 \
                                         ORDER BY record_id LIMIT ?3)",
                )
                .map_err(db)?
                .execute(params![timeline, change_id, excess])
                .map_err(db)?;
                let count: i64 = tx
                    .query_row(
                        "SELECT COUNT(*) FROM timeline_records WHERE timeline_id = ?1",
                        [&timeline],
                        |row| row.get(0),
                    )
                    .map_err(db)?;
                if count > cap {
                    tx.prepare_cached(
                        "DELETE FROM timeline_records WHERE timeline_id = ?1 \
                           AND record_id IN (SELECT record_id FROM timeline_records \
                                             WHERE timeline_id = ?1 \
                                             ORDER BY record_id LIMIT ?2)",
                    )
                    .map_err(db)?
                    .execute(params![timeline, count - cap])
                    .map_err(db)?;
                }
            }
            let mut stmt = tx
                .prepare_cached(
                    "SELECT record FROM timeline_records WHERE timeline_id = ?1 \
                     ORDER BY record_id LIMIT ?2",
                )
                .map_err(db)?;
            let stored = decode_records(&mut stmt, params![timeline, MAX_TIMELINE_RECORDS as i64])?;
            Ok((change_id as i32, stored))
        })
    }

    /// A timeline's change id (0 when never patched) and its records ordered
    /// by record id, `skip`/`top` paged (`top` capped at
    /// [`MAX_TIMELINE_RECORDS`]).
    pub(crate) async fn get_timeline(
        &self,
        timeline_key: &str,
        skip: usize,
        top: usize,
    ) -> Result<(i32, Vec<TimelineRecord>), ControlError> {
        let Some(timeline) = timeline_id(timeline_key) else {
            return Ok((0, Vec::new()));
        };
        self.read(|tx| {
            let change_id: i64 = tx
                .query_row(
                    "SELECT change_id FROM timelines WHERE timeline_id = ?1",
                    [&timeline],
                    |row| row.get(0),
                )
                .optional()
                .map_err(db)?
                .unwrap_or(0);
            let mut stmt = tx
                .prepare_cached(
                    "SELECT record FROM timeline_records WHERE timeline_id = ?1 \
                     ORDER BY record_id LIMIT ?2 OFFSET ?3",
                )
                .map_err(db)?;
            let records = decode_records(
                &mut stmt,
                params![
                    timeline,
                    top.min(MAX_TIMELINE_RECORDS) as i64,
                    skip.min(i64::MAX as usize) as i64
                ],
            )?;
            Ok((change_id as i32, records))
        })
    }

    /// Drop the timelines of attempts settled before `before_us`
    /// (`job_requests.result IS NOT NULL AND finished_at < before_us`);
    /// their records cascade. Returns timelines removed.
    pub(crate) async fn prune_timelines(&self, before_us: i64) -> Result<u64, ControlError> {
        self.write(|tx| {
            tx.execute(
                "DELETE FROM timelines WHERE timeline_id IN ( \
                     SELECT timeline_id FROM job_requests \
                     WHERE result IS NOT NULL AND finished_at < ?1)",
                [before_us],
            )
            .map(|n| n as u64)
            .map_err(db)
        })
    }
}
