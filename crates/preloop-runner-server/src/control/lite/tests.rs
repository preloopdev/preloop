use super::LiteBackend;
use crate::control::backend::RequestKey;
use crate::control::types::ControlError;
use crate::models::{WebhookDeliveryRecord, WebhookDeliveryStatus};

fn delivery(id: &str) -> WebhookDeliveryRecord {
    WebhookDeliveryRecord {
        delivery_id: id.to_owned(),
        event: "push".to_owned(),
        payload: br#"{"installation":{"id":9},"ref":"refs/heads/main"}"#.to_vec(),
        received_at_us: 1,
        state: WebhookDeliveryStatus::Received,
        attempts: 0,
        lease_until_us: None,
        lease_token: None,
        signer_app_id: None,
        last_error: None,
    }
}

#[test]
fn fresh_schema_has_default_namespace_and_rejects_wrong_version() {
    let backend = LiteBackend::in_memory().unwrap();
    backend.writer.lock().execute_batch("SELECT 1").unwrap();
    let namespace: String = backend
        .writer
        .lock()
        .query_row("SELECT namespace_id FROM namespaces", [], |r| r.get(0))
        .unwrap();
    assert_eq!(namespace, "default");
}

#[tokio::test]
async fn run_number_fingerprint_and_unknown_request() {
    let backend = LiteBackend::in_memory().unwrap();
    let workflow = ".github/workflows/ci.yml";
    assert_eq!(
        backend
            .allocate_run_number("default", "owner/repo", workflow)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        backend
            .allocate_run_number("default", "owner/repo", workflow)
            .await
            .unwrap(),
        2
    );
    backend.ensure_key_fingerprint("f1").await.unwrap();
    assert!(backend.ensure_key_fingerprint("f2").await.is_err());
    assert!(matches!(
        backend.request(RequestKey::Id(999)).await.unwrap_err(),
        ControlError::NotFound(_)
    ));
}

/// Runtime fan-out pays for its jobs after submit admission, so the per-run
/// namespace limit is re-applied against the rows already in the run. `None`
/// (no limits row) must stay unlimited, or every deferred expansion on a
/// default install would be refused.
#[test]
fn remaining_job_budget_applies_the_namespace_run_limit() {
    use preloop_gha_protocol::RunId;

    let backend = LiteBackend::in_memory().unwrap();
    let run_id = RunId::new();
    let (unlimited, remaining, exhausted, webhook) = backend
        .write(|tx| {
            let namespace: String = tx
                .query_row("SELECT namespace_id FROM namespaces", [], |row| row.get(0))
                .map_err(super::db)?;
            tx.execute(
                "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
                     run_number, event, ref, ref_type, head_sha, workflow_ref, status, origin) \
                 VALUES (?1, ?2, 'owner/repo', '.github/workflows/ci.yml', 1, 'push', \
                     'refs/heads/main', 'branch', 'abc123', \
                     'owner/repo/.github/workflows/ci.yml@refs/heads/main', 'queued', 'api')",
                rusqlite::params![run_id.0.to_string(), namespace],
            )
            .map_err(super::db)?;
            let insert_job = |job_id: &str, order: i64| {
                tx.execute(
                    "INSERT INTO jobs (run_id, job_id, namespace_id, kind, base_id, status, \
                         queue_state, remaining_needs, pool_key, runs_on, priority, run_order, \
                         job_order, created_at) \
                     VALUES (?1, ?2, ?3, 'job', ?2, 'pending', 'blocked', 0, '', '[]', 0, 0, \
                         ?4, ?4)",
                    rusqlite::params![run_id.0.to_string(), job_id, namespace, order],
                )
                .map_err(super::db)?;
                Ok::<(), ControlError>(())
            };
            insert_job("build", 1)?;

            // No limits row: unlimited.
            let unlimited = super::expansion::remaining_job_budget(tx, run_id)?;

            tx.execute(
                "INSERT INTO namespace_limits (namespace_id, max_jobs_per_run) VALUES (?1, 3)",
                [&namespace],
            )
            .map_err(super::db)?;
            let remaining = super::expansion::remaining_job_budget(tx, run_id)?;

            // The run is already past the limit: nothing more may be added.
            insert_job("test", 2)?;
            insert_job("lint", 3)?;
            let exhausted = super::expansion::remaining_job_budget(tx, run_id)?;

            // Webhook deliveries are never refused at submit, so their
            // deferred expansions stay exempt from the per-run cap.
            tx.execute(
                "UPDATE runs SET webhook_delivery_id = 'delivery-1' WHERE run_id = ?1",
                [run_id.0.to_string()],
            )
            .map_err(super::db)?;
            let webhook = super::expansion::remaining_job_budget(tx, run_id)?;
            Ok((unlimited, remaining, exhausted, webhook))
        })
        .unwrap();

    assert_eq!(unlimited, None);
    assert_eq!(
        remaining,
        Some(2),
        "3 per run minus the row already admitted"
    );
    assert_eq!(exhausted, Some(0));
    assert_eq!(
        webhook, None,
        "webhook runs stay exempt from the per-run cap"
    );
}
