use super::{LiteBackend, db};
use crate::control::types::{CheckRunUpdate, ControlError, OutboxBookmark, OutboxRow};
use preloop_gha_protocol::{JobId, RunId};
use rusqlite::{Transaction, params};
use serde_json::Value;

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
fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

impl LiteBackend {
    pub(crate) async fn enqueue_check_run_update(
        &self,
        update: crate::control::types::CheckRunUpdateInput,
    ) -> Result<(), ControlError> {
        self.write(|tx|{tx.execute("INSERT INTO check_run_updates(run_id,job_id,installation_id,check_run_id,version,payload,not_before,attempts) VALUES(?1,?2,?3,?4,?5,?6,?7,0) ON CONFLICT(run_id,job_id) DO UPDATE SET payload=excluded.payload,version=excluded.version,not_before=excluded.not_before WHERE excluded.version>check_run_updates.version",params![run_text(update.run_id),update.job_id.0,update.installation_id,update.check_run_id.map(|v|v as i64),update.version,update.payload.to_string(),now_us()]).map_err(db)?;Ok(())})
    }
    pub(crate) async fn consume_check_run_outbox(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<usize, ControlError> {
        self.write(|tx|{
   let now=now_us();
   tx.execute("INSERT INTO consumer_offsets(consumer_name,last_event_id) VALUES(?1,0) ON CONFLICT(consumer_name) DO NOTHING",params![CONSUMER]).map_err(db)?;
   let lease=now+lease_for.as_micros() as i64;
   let n=tx.execute("UPDATE consumer_offsets SET lease_owner=?2,lease_until=?3,updated_at=?4 WHERE consumer_name=?1 AND (lease_owner IS NULL OR lease_until<?4 OR lease_owner=?2)",params![CONSUMER,owner,lease,now]).map_err(db)?;
   if n==0{return Ok(0)}
   let after:i64=tx.query_row("SELECT last_event_id FROM consumer_offsets WHERE consumer_name=?1",params![CONSUMER],|r|r.get(0)).map_err(db)?;
   let mut rows=Vec::new();
   {
    let mut st=tx.prepare("SELECT event_id,run_id,job_id,version,'' AS origin,topic,payload FROM outbox_events WHERE event_id>?1 ORDER BY event_id LIMIT ?2").map_err(db)?;
    let it=st.query_map(params![after,limit as i64],|r|{
     let run_id=match r.get::<_,Option<String>>(1)? {
      Some(v)=>Some(v.parse::<uuid::Uuid>().map(RunId).map_err(|_|rusqlite::Error::InvalidQuery)?),
      None=>None,
     };
     Ok(OutboxRow{bookmark:OutboxBookmark{txid:0,event_id:r.get(0)?},run_id,job_id:r.get(2)?,version:r.get(3)?,origin:r.get(4)?,topic:r.get(5)?,payload:serde_json::from_str::<Value>(&r.get::<_,String>(6)?).map_err(|_|rusqlite::Error::InvalidQuery)?})
    }).map_err(db)?;
    for row in it { rows.push(row.map_err(db)?); }
   }
   let count=rows.len(); let mut last=after;
   for row in rows { last=row.bookmark.event_id; if relevant(&row.topic){project_row(tx,&row)?;} }
   if count>0 { tx.execute("UPDATE consumer_offsets SET last_event_id=?2,updated_at=?3 WHERE consumer_name=?1",params![CONSUMER,last,now]).map_err(db)?; }
   Ok(count)
  })
    }
    pub(crate) async fn lease_check_run_updates(
        &self,
        owner: &str,
        lease_for: std::time::Duration,
        limit: usize,
    ) -> Result<Vec<CheckRunUpdate>, ControlError> {
        self.write(|tx|{let now=now_us();let until=now+lease_for.as_micros() as i64;let mut st=tx.prepare("SELECT run_id,job_id,installation_id,check_run_id,version,payload,attempts FROM check_run_updates WHERE not_before<=?1 AND (leased_until IS NULL OR leased_until<?1 OR lease_owner=?2) ORDER BY not_before,run_id,job_id LIMIT ?3").map_err(db)?;let rows=st.query_map(params![now,owner,limit as i64],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,Option<i64>>(3)?,r.get::<_,i64>(4)?,r.get::<_,String>(5)?,r.get::<_,i32>(6)?))).map_err(db)?;let mut out=Vec::new();for row in rows{let (run,job,inst,id,ver,payload,attempts)=row.map_err(db)?;tx.execute("UPDATE check_run_updates SET lease_owner=?3,leased_until=?4 WHERE run_id=?1 AND job_id=?2",params![run,job,owner,until]).map_err(db)?;out.push(CheckRunUpdate{run_id:run.parse().map(RunId).map_err(|_|ControlError::backend(anyhow::anyhow!("bad run id")))?,job_id:JobId(job),installation_id:inst,check_run_id:id.map(|v|v as u64),version:ver,payload:serde_json::from_str(&payload).map_err(ControlError::backend)?,attempts});}Ok(out)})
    }
    pub(crate) async fn set_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
        id: u64,
    ) -> Result<(), ControlError> {
        self.write(|tx|{tx.execute("UPDATE check_run_updates SET check_run_id=?,leased_until=? WHERE lease_owner=? AND run_id=? AND job_id=? AND version=?",params![id as i64,now_us()+300_000_000,owner,run_text(run_id),job_id.0,version]).map_err(db)?;Ok(())})
    }
    pub(crate) async fn finish_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        version: i64,
    ) -> Result<(), ControlError> {
        self.write(|tx|{tx.execute("DELETE FROM check_run_updates WHERE lease_owner=? AND run_id=? AND job_id=? AND version=?",params![owner,run_text(run_id),job_id.0,version]).map_err(db)?;Ok(())})
    }
    pub(crate) async fn retry_check_run_update(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        delay: std::time::Duration,
        permanent: bool,
    ) -> Result<(), ControlError> {
        self.write(|tx|{if permanent{tx.execute("DELETE FROM check_run_updates WHERE lease_owner=? AND run_id=? AND job_id=?",params![owner,run_text(run_id),job_id.0]).map_err(db)?;}else{tx.execute("UPDATE check_run_updates SET attempts=attempts+1,not_before=?,leased_until=NULL,lease_owner=NULL WHERE lease_owner=? AND run_id=? AND job_id=?",params![now_us()+delay.as_micros() as i64,owner,run_text(run_id),job_id.0]).map_err(db)?;}Ok(())})
    }
    pub(crate) async fn clear_check_run_update_id(
        &self,
        owner: &str,
        run_id: RunId,
        job_id: &JobId,
        expected: u64,
    ) -> Result<(), ControlError> {
        self.write(|tx|{tx.execute("UPDATE check_run_updates SET check_run_id=NULL WHERE lease_owner=? AND run_id=? AND job_id=? AND check_run_id=?",params![owner,run_text(run_id),job_id.0,expected as i64]).map_err(db)?;Ok(())})
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
        self.write(|tx|{tx.execute("UPDATE check_run_updates SET not_before=?,leased_until=NULL,lease_owner=NULL WHERE lease_owner=? AND run_id=? AND job_id=?",params![now_us()+delay.as_micros() as i64,owner,run_text(run_id),job_id.0]).map_err(db)?;Ok(())})
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
        self.write(|tx| {
            let payload = serde_json::json!({"job_id": job_id.map(|job| job.0.clone())});
            super::jobs::insert_outbox(
                tx,
                super::jobs::namespace_of(tx, run_id)?.as_str(),
                Some(run_id),
                job_id.map(|job| job.0.as_str()),
                None,
                "check_run_projection.v1",
                &payload.to_string(),
            )
        })
    }
}
fn project_row(tx: &Transaction<'_>, row: &OutboxRow) -> Result<(), ControlError> {
    let Some(run_id) = row.run_id else {
        return Ok(());
    };
    let mut st=tx.prepare("SELECT r.reports_check_runs,r.repository,COALESCE(ps.effective_sha,json_extract(s.submission,'$.status_check_sha'),json_extract(s.submission,'$.sha'),r.head_sha),j.job_id,j.status,j.version,COALESCE(js.display_name,j.job_id),j.check_run_id,j.kind FROM runs r JOIN jobs j ON j.run_id=r.run_id LEFT JOIN job_specs js ON js.run_id=j.run_id AND js.job_id=j.job_id LEFT JOIN run_submissions s ON s.run_id=r.run_id LEFT JOIN run_push_states ps ON ps.run_id=r.run_id WHERE r.run_id=?1 AND (?2 IS NULL OR j.job_id=?2)").map_err(db)?;
    let mut rows = st
        .query(params![run_text(run_id), row.job_id.as_deref()])
        .map_err(db)?;
    while let Some(r) = rows.next().map_err(db)? {
        let reports: bool = r.get(0).map_err(db)?;
        let kind: String = r.get(8).map_err(db)?;
        if !reports || kind == "matrix_parent" || kind == "reusable_caller" {
            continue;
        }
        let job: String = r.get(3).map_err(db)?;
        let ver: i64 = r.get(5).map_err(db)?;
        let payload = serde_json::json!({"repository":r.get::<_,String>(1).map_err(db)?,"sha":r.get::<_,String>(2).map_err(db)?,"job_id":job,"status":r.get::<_,String>(4).map_err(db)?,"name":r.get::<_,String>(6).map_err(db)?});
        let id: Option<i64> = r.get(7).map_err(db)?;
        tx.execute("INSERT INTO check_run_updates(run_id,job_id,installation_id,check_run_id,version,payload,not_before,attempts,leased_until,lease_owner) VALUES(?1,?2,0,?3,?4,?5,?6,0,NULL,NULL) ON CONFLICT(run_id,job_id) DO UPDATE SET payload=excluded.payload,version=excluded.version,check_run_id=COALESCE(excluded.check_run_id,check_run_updates.check_run_id),not_before=excluded.not_before,leased_until=NULL,lease_owner=NULL,attempts=0 WHERE excluded.version>check_run_updates.version",params![run_text(run_id),job,id,ver,payload.to_string(),now_us()]).map_err(db)?;
    }
    Ok(())
}
