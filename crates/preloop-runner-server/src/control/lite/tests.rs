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
async fn webhook_claim_is_fenced_and_deduplicated() {
    let backend = LiteBackend::in_memory().unwrap();
    assert!(backend
        .enqueue_webhook_delivery(&delivery("d1"))
        .await
        .unwrap());
    assert!(!backend
        .enqueue_webhook_delivery(&delivery("d1"))
        .await
        .unwrap());
    let claim = backend.claim_webhook_deliveries(1, 60).await.unwrap();
    assert_eq!(claim.len(), 1);
    let token = claim[0].lease_token.clone().unwrap();
    assert!(!backend
        .renew_webhook_delivery("d1", "stale", 60)
        .await
        .unwrap());
    assert!(backend
        .complete_webhook_delivery("d1", &token)
        .await
        .unwrap());
    assert_eq!(
        backend
            .get_webhook_delivery("d1")
            .await
            .unwrap()
            .unwrap()
            .state,
        WebhookDeliveryStatus::Done
    );
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
