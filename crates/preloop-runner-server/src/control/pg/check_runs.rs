use super::{PgBackend, codec, db};
use crate::control::types::{CheckRunUpdate, ControlError, OutboxBookmark, OutboxRow};
use preloop_gha_protocol::{JobId, RunId};
use tokio_postgres::Transaction;

const CONSUMER: &str = "check-runs";

fn relevant(topic: &str) -> bool {
    matches!(
        topic,
        "run.created.v1"
            | "run.completed.v1"
            | "expansion.queued.v1"
            | "job.queued.v1"
            | "job.started.v1"
            | "job.completed.v1"
            | "job_status.v1"
            | "run_status.v1"
            | "check_run_projection.v1"
    )
}

fn run_text(run: RunId) -> String {
    run.0.to_string()
}

impl PgBackend {
    pub(crate) async fn enqueue_check_run_update(
        &self,
        update: crate::control::types::CheckRunUpdateInput,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        let payload = serde_json::to_string(&update.payload).map_err(ControlError::backend)?;
        client.execute("INSERT INTO check_run_updates(run_id,job_id,installation_id,check_run_id,version,payload,not_before,attempts) VALUES($1::text::uuid,$2,$3,$4,$5,$6::text::jsonb,now(),0) ON CONFLICT(run_id,job_id) DO UPDATE SET payload=EXCLUDED.payload,version=EXCLUDED.version,not_before=now() WHERE EXCLUDED.version>check_run_updates.version", &[&update.run_id.0.to_string(),&update.job_id.0,&update.installation_id,&update.check_run_id.map(|v|v as i64),&update.version,&payload]).await.map(|_|()).map_err(db)
    }
    pub(crate) async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        tx.execute(
            "INSERT INTO consumer_offsets (consumer_name,last_txid,last_event_id) VALUES ($1,'0',0) ON CONFLICT DO NOTHING",
            &[&CONSUMER],
        ).await.map_err(db)?;
        let leased = tx.query_opt(
            "UPDATE consumer_offsets SET lease_owner=$2, lease_until=now()+make_interval(secs=>$3), updated_at=now() \
             WHERE consumer_name=$1 AND (lease_owner IS NULL OR lease_until < now() OR lease_owner=$2) RETURNING last_txid::text,last_event_id",
            &[&CONSUMER, &owner, &lease_for.as_secs_f64()],
        ).await.map_err(db)?;
        let Some(offset) = leased else {
            tx.rollback().await.map_err(db)?;
            return Ok(0);
        };
        let after = OutboxBookmark {
            txid: offset
                .get::<_, String>(0)
                .parse()
                .map_err(ControlError::backend)?,
            event_id: offset.get(1),
        };
        let rows = read_rows(&tx, after, limit).await?;
        let count = rows.len();
        let mut bookmark = after;
        for row in rows {
            bookmark = row.bookmark;
            if relevant(&row.topic) {
                project_row(&tx, &row).await?;
            }
        }
        if count != 0 {
            tx.execute("UPDATE consumer_offsets SET last_txid=$2::text::xid8,last_event_id=$3,updated_at=now() WHERE consumer_name=$1", &[&CONSUMER, &bookmark.txid.to_string(), &bookmark.event_id]).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(count)
    }

    pub(crate) async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let rows = tx.query(
            "WITH picked AS (SELECT run_id,job_id FROM check_run_updates WHERE not_before<=now() AND (leased_until IS NULL OR leased_until<now() OR lease_owner=$1) ORDER BY not_before,run_id,job_id FOR UPDATE SKIP LOCKED LIMIT $2) \
             UPDATE check_run_updates u SET lease_owner=$1,leased_until=now()+make_interval(secs=>$3) FROM picked p WHERE u.run_id=p.run_id AND u.job_id=p.job_id \
             RETURNING u.run_id::text,u.job_id,u.installation_id,u.check_run_id,u.version,u.payload::text,u.attempts",
            &[&owner, &(limit as i64), &lease_for.as_secs_f64()]).await.map_err(db)?;
        let out = rows
            .into_iter()
            .map(|r| {
                Ok(CheckRunUpdate {
                    run_id: codec::run_id(r.get(0))?,
                    job_id: JobId(r.get(1)),
                    installation_id: r.get(2),
                    check_run_id: r.get::<_, Option<i64>>(3).map(|v| v as u64),
                    version: r.get(4),
                    payload: codec::from_json(r.get::<_, String>(5).as_str())?,
                    attempts: r.get(6),
                })
            })
            .collect::<Result<Vec<_>, ControlError>>();
        tx.commit().await.map_err(db)?;
        out
    }

    pub(crate) async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        id: u64,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client.execute("UPDATE check_run_updates SET check_run_id=$5,leased_until=now()+interval '5 minutes' WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3 AND version=$4", &[&owner,&run_text(run_id),&job_id.0,&version,&(id as i64)]).await.map(|_|()).map_err(db)
    }
    pub(crate) async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client.execute("DELETE FROM check_run_updates WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3 AND version=$4", &[&owner,&run_text(run_id),&job_id.0,&version]).await.map(|_|()).map_err(db)
    }
    pub(crate) async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        if permanent {
            client.execute("DELETE FROM check_run_updates WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3", &[&owner,&run_text(run_id),&job_id.0]).await.map_err(db)?;
        } else {
            client.execute("UPDATE check_run_updates SET attempts=attempts+1,not_before=now()+make_interval(secs=>$4),leased_until=NULL,lease_owner=NULL WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3", &[&owner,&run_text(run_id),&job_id.0,&delay.as_secs_f64()]).await.map_err(db)?;
        }
        Ok(())
    }
    pub(crate) async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client.execute("UPDATE check_run_updates SET check_run_id=NULL WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3 AND check_run_id=$4", &[&owner,&run_text(run_id),&job_id.0,&(expected as i64)]).await.map(|_|()).map_err(db)
    }

    /// Release a lease without counting an attempt: the sender is waiting out
    /// a rate limit, not failing to deliver.
    pub(crate) async fn defer_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
    ) -> Result<(), ControlError> {
        let client = self.writer().await?;
        client.execute("UPDATE check_run_updates SET not_before=now()+make_interval(secs=>$4),leased_until=NULL,lease_owner=NULL WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3", &[&owner,&run_text(run_id),&job_id.0,&delay.as_secs_f64()]).await.map(|_|()).map_err(db)
    }

    /// Extend `owner`'s lease on one row. A row another sender re-leased has
    /// a different `lease_owner`, so ownership alone decides: `false` means
    /// the caller lost the row and must not call GitHub for it.
    pub(crate) async fn renew_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        lease_for: std::time::Duration,
    ) -> Result<bool, ControlError> {
        let client = self.writer().await?;
        let renewed = client
            .execute(
                "UPDATE check_run_updates SET leased_until=now()+make_interval(secs=>$4) \
                 WHERE lease_owner=$1 AND run_id=$2::text::uuid AND job_id=$3",
                &[
                    &owner,
                    &run_text(run_id),
                    &job_id.0,
                    &lease_for.as_secs_f64(),
                ],
            )
            .await
            .map_err(db)?;
        Ok(renewed == 1)
    }

    /// Append a durable projection wake for one run (or one job) *after* a
    /// reporter stamped `reports_check_runs`.
    ///
    /// Unlike a `JobStatus` event this is never stamped `Stale`, so a job
    /// already terminal when the run's reporting flag lands (an `if: false`
    /// leg concluded during submit) still reaches the projector.
    pub(crate) async fn append_check_run_projection(
        &self,
        run_id: RunId,
        job_id: Option<&JobId>,
    ) -> Result<(), ControlError> {
        let mut client = self.writer().await?;
        let tx = client.transaction().await.map_err(db)?;
        let payload = serde_json::json!({"job_id": job_id.map(|job| job.0.clone())});
        super::dispatch::emit_outbox(&tx, Some(run_id), "check_run_projection.v1", payload).await?;
        tx.commit().await.map_err(db)?;
        Ok(())
    }
}

async fn read_rows(
    tx: &Transaction<'_>,
    after: OutboxBookmark,
    limit: usize,
) -> Result<Vec<OutboxRow>, ControlError> {
    let rows=tx.query("SELECT txid::text,event_id,run_id::text,job_id,version,origin,topic,payload::text FROM outbox_events WHERE (txid,event_id)>($1::text::xid8,$2) AND txid<pg_snapshot_xmin(pg_current_snapshot()) ORDER BY txid,event_id LIMIT $3", &[&after.txid.to_string(),&after.event_id,&(limit as i64)]).await.map_err(db)?;
    rows.into_iter()
        .map(|r| {
            Ok(OutboxRow {
                bookmark: OutboxBookmark {
                    txid: r
                        .get::<_, String>(0)
                        .parse()
                        .map_err(ControlError::backend)?,
                    event_id: r.get(1),
                },
                run_id: r
                    .get::<_, Option<String>>(2)
                    .map(|v| codec::run_id(&v))
                    .transpose()?,
                job_id: r.get(3),
                version: r.get(4),
                origin: r.get(5),
                topic: r.get(6),
                payload: codec::from_json(r.get::<_, String>(7).as_str())?,
            })
        })
        .collect()
}

async fn project_row(tx: &Transaction<'_>, row: &OutboxRow) -> Result<(), ControlError> {
    let Some(run_id) = row.run_id else {
        return Ok(());
    };
    let filter = row.job_id.as_deref();
    let sql = if filter.is_some() {
        "SELECT r.reports_check_runs,r.repository,COALESCE(ps.effective_sha,s.submission->>'status_check_sha',s.submission->>'sha',r.head_sha),j.job_id,j.status,j.version,COALESCE(js.display_name,j.job_id),j.check_run_id,j.kind FROM runs r JOIN jobs j ON j.run_id=r.run_id LEFT JOIN job_specs js ON js.run_id=j.run_id AND js.job_id=j.job_id LEFT JOIN run_submissions s ON s.run_id=r.run_id LEFT JOIN run_push_states ps ON ps.run_id=r.run_id WHERE r.run_id=$1::text::uuid AND j.job_id=$2"
    } else {
        "SELECT r.reports_check_runs,r.repository,COALESCE(ps.effective_sha,s.submission->>'status_check_sha',s.submission->>'sha',r.head_sha),j.job_id,j.status,j.version,COALESCE(js.display_name,j.job_id),j.check_run_id,j.kind FROM runs r JOIN jobs j ON j.run_id=r.run_id LEFT JOIN job_specs js ON js.run_id=j.run_id AND js.job_id=j.job_id LEFT JOIN run_submissions s ON s.run_id=r.run_id LEFT JOIN run_push_states ps ON ps.run_id=r.run_id WHERE r.run_id=$1::text::uuid"
    };
    let rows = if let Some(job) = filter {
        tx.query(sql, &[&run_text(run_id), &job]).await
    } else {
        tx.query(sql, &[&run_text(run_id)]).await
    }
    .map_err(db)?;
    for r in rows {
        let reports: bool = r.get(0);
        let kind: String = r.get(8);
        if !reports || kind == "matrix_parent" || kind == "reusable_caller" {
            continue;
        }
        let status: String = r.get(4);
        let version: i64 = r.get(5);
        let payload = serde_json::json!({"repository":r.get::<_,String>(1),"sha":r.get::<_,String>(2),"job_id":r.get::<_,String>(3),"status":status,"name":r.get::<_,String>(6)});
        let id: r#Option<i64> = r.get(7);
        tx.execute("INSERT INTO check_run_updates(run_id,job_id,installation_id,check_run_id,version,payload,not_before,attempts,leased_until,lease_owner) VALUES($1::text::uuid,$2,0,$3,$4,$5::text::jsonb,now(),0,NULL,NULL) ON CONFLICT(run_id,job_id) DO UPDATE SET payload=EXCLUDED.payload,version=EXCLUDED.version,check_run_id=COALESCE(EXCLUDED.check_run_id,check_run_updates.check_run_id),not_before=now(),leased_until=NULL,lease_owner=NULL,attempts=0 WHERE EXCLUDED.version>check_run_updates.version", &[&run_text(run_id),&r.get::<_,String>(3),&id,&version,&payload.to_string()]).await.map_err(db)?;
    }
    Ok(())
}
