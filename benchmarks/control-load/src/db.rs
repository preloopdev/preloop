//! Periodic Postgres samples: commit rate, lock waits, connection states,
//! dead tuples on the hot tables, and (at the end) the top statements.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::metrics::Metrics;

pub async fn sample_loop(url: String, metrics: Arc<Metrics>, deadline: Instant) {
    let Ok((client, conn)) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await else {
        eprintln!("db sampler: cannot connect");
        return;
    };
    tokio::spawn(conn);
    let mut previous_commits: Option<(i64, Instant)> = None;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut sample = serde_json::Map::new();
        sample.insert("t".into(), metrics.elapsed().as_secs().into());
        if let Ok(row) = client
            .query_one(
                "SELECT xact_commit, xact_rollback, deadlocks, blks_hit, blks_read, \
                 tup_inserted, tup_updated, tup_deleted \
                 FROM pg_stat_database WHERE datname = current_database()",
                &[],
            )
            .await
        {
            let commits: i64 = row.get(0);
            if let Some((prev, at)) = previous_commits {
                let rate = (commits - prev) as f64 / at.elapsed().as_secs_f64();
                sample.insert("commits_per_sec".into(), rate.into());
            }
            previous_commits = Some((commits, Instant::now()));
            sample.insert("rollbacks".into(), row.get::<_, i64>(1).into());
            sample.insert("deadlocks".into(), row.get::<_, i64>(2).into());
            let hit: i64 = row.get(3);
            let read: i64 = row.get(4);
            sample.insert(
                "cache_hit_ratio".into(),
                (hit as f64 / (hit + read).max(1) as f64).into(),
            );
            sample.insert("tup_updated".into(), row.get::<_, i64>(6).into());
        }
        if let Ok(rows) = client
            .query(
                "SELECT coalesce(wait_event_type,'none'), state, count(*) \
                 FROM pg_stat_activity WHERE datname = current_database() \
                 GROUP BY 1, 2",
                &[],
            )
            .await
        {
            let activity: serde_json::Map<_, _> = rows
                .iter()
                .map(|r| {
                    let wait: String = r.get(0);
                    let state: Option<String> = r.get(1);
                    (
                        format!("{}:{}", state.unwrap_or_default(), wait),
                        serde_json::json!(r.get::<_, i64>(2)),
                    )
                })
                .collect();
            sample.insert("activity".into(), activity.into());
        }
        if let Ok(rows) = client
            .query(
                "SELECT relname, n_live_tup, n_dead_tup, n_tup_hot_upd, n_tup_upd \
                 FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 6",
                &[],
            )
            .await
        {
            let tables: serde_json::Map<_, _> = rows
                .iter()
                .map(|r| {
                    let name: String = r.get(0);
                    (
                        name,
                        serde_json::json!({
                            "live": r.get::<_, i64>(1),
                            "dead": r.get::<_, i64>(2),
                            "hot_upd": r.get::<_, i64>(3),
                            "upd": r.get::<_, i64>(4),
                        }),
                    )
                })
                .collect();
            sample.insert("tables".into(), tables.into());
        }
        metrics.db_sample(sample.into());
    }
}

/// Top statements by total time, when `pg_stat_statements` is loaded.
pub async fn top_statements(url: &str) -> serde_json::Value {
    let Ok((client, conn)) = tokio_postgres::connect(url, tokio_postgres::NoTls).await else {
        return serde_json::Value::Null;
    };
    tokio::spawn(conn);
    match client
        .query(
            "SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 160), calls, \
             total_exec_time, mean_exec_time, rows \
             FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 15",
            &[],
        )
        .await
    {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "query": r.get::<_, String>(0),
                    "calls": r.get::<_, i64>(1),
                    "total_ms": r.get::<_, f64>(2),
                    "mean_ms": r.get::<_, f64>(3),
                    "rows": r.get::<_, i64>(4),
                })
            })
            .collect(),
        Err(error) => serde_json::json!({"unavailable": error.to_string()}),
    }
}

/// Reset `pg_stat_statements` at round start (ignored when not loaded).
pub async fn reset_statements(url: &str) {
    if let Ok((client, conn)) = tokio_postgres::connect(url, tokio_postgres::NoTls).await {
        tokio::spawn(conn);
        let _ = client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements; SELECT pg_stat_statements_reset();")
            .await;
    }
}
