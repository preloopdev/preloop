//! Provider enrollment, mTLS identity, renewal and revocation.
//!
//! These run against a real TLS listener: the acceptance criteria for the
//! credential model (single-use tokens, CA pinning, name binding, renewal and
//! revocation) only mean something on the wire.

mod common;

use std::path::Path;
use std::time::Duration;

use common::TestService;
use preloop_sandbox::agent::{ProviderLink, RemoteLink};
use preloop_sandbox::api::*;
use preloop_sandbox::connect::Client as ConnectClient;
use preloop_sandbox::{Code, SandboxApi, SandboxClient};

fn register(name: &str) -> RegisterRequest {
    RegisterRequest {
        name: name.into(),
        drivers: vec![DriverInfo {
            driver: "fake".into(),
            capabilities: Capabilities::default(),
        }],
    }
}

fn copy_identity(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

#[tokio::test]
async fn enroll_binds_a_name_and_issues_a_provider_certificate() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let session = link.register(register("host-a")).await.unwrap();
    assert_eq!(session.generation, 1);
    let providers = t.service.list_providers();
    let provider = providers
        .iter()
        .find(|provider| provider.name == "host-a")
        .expect("provider registered");
    assert_eq!(provider.id, session.provider_id);
    assert_eq!(provider.certificates.len(), 1);
    assert_eq!(provider.certificates[0].sha256.len(), 64);
    // The certificate is good for a day.
    let remaining = provider.certificates[0].expires_at
        - std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
    assert!(
        remaining > 23 * 3600 && remaining <= 24 * 3600,
        "{remaining}"
    );
    assert!(
        link.heartbeat(HeartbeatRequest {
            provider_id: session.provider_id,
            generation: session.generation,
        })
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn join_tokens_are_single_use() {
    let t = TestService::start().await;
    let token = t.join_token(Duration::from_secs(60)).await;
    RemoteLink::enroll(&t.base, &token, t.dir.path().join("first"), "host-a")
        .await
        .unwrap();
    let error = RemoteLink::enroll(&t.base, &token, t.dir.path().join("second"), "host-a")
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    assert!(error.message.contains("join token"), "{}", error.message);
}

#[tokio::test]
async fn expired_join_tokens_are_rejected() {
    let t = TestService::start().await;
    let token = t.join_token(Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let error = RemoteLink::enroll(&t.base, &token, t.dir.path().join("late"), "host-a")
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    // Maintenance may already have pruned it, so "expired" and "unknown" are
    // both correct rejects.
    assert!(error.message.contains("join token"), "{}", error.message);
}

#[tokio::test]
async fn a_wrong_ca_pin_is_rejected_before_the_csr_is_sent() {
    let first = TestService::start().await;
    let second = TestService::start().await;
    // A token minted by the first service, presented to the second: the CA it
    // serves does not hash to the token's pin.
    let token = first.join_token(Duration::from_secs(60)).await;
    let error = RemoteLink::enroll(&second.base, &token, second.dir.path().join("id"), "host-a")
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    assert!(error.message.contains("pin"), "{}", error.message);
}

#[tokio::test]
async fn a_token_pinned_to_another_ca_is_rejected_by_the_service() {
    let t = TestService::start().await;
    let token = t.join_token(Duration::from_secs(60)).await;
    let (head, _) = token.rsplit_once('.').unwrap();
    let tampered = format!("{head}.{}", "00".repeat(32));
    // Go straight to the service: the agent would refuse the pin before
    // sending, so only a raw call exercises the service-side check.
    let anonymous = ConnectClient::with_tls(&t.base, None, &t.ca_pem(), None).unwrap();
    let error = anonymous
        .unary::<_, EnrollResponse>(
            "preloop.sandbox.v1.Providers/Enroll",
            &EnrollRequest {
                token: tampered,
                name: "host-a".into(),
                csr_pem: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    assert!(error.message.contains("CA"), "{}", error.message);
}

#[tokio::test]
async fn register_with_a_mismatched_name_is_denied() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let error = link.register(register("host-b")).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    assert!(error.message.contains("name"), "{}", error.message);
    // The name from the certificate still works.
    link.register(register("host-a")).await.unwrap();
}

#[tokio::test]
async fn provider_calls_without_a_client_certificate_are_denied() {
    let t = TestService::start().await;
    let anonymous = ConnectClient::with_tls(&t.base, None, &t.ca_pem(), None).unwrap();
    let error = anonymous
        .unary::<_, RegisterResponse>("preloop.sandbox.v1.Providers/Register", &register("ghost"))
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    let error = anonymous
        .unary::<_, Empty>(
            "preloop.sandbox.v1.Providers/Heartbeat",
            &HeartbeatRequest {
                provider_id: "prv-missing".into(),
                generation: 1,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    // Enroll is the one method that does not need a client certificate; it
    // still needs a valid token.
    let error = anonymous
        .unary::<_, EnrollResponse>(
            "preloop.sandbox.v1.Providers/Enroll",
            &EnrollRequest {
                token: "plj1.missing.secret".into(),
                name: "ghost".into(),
                csr_pem: String::new(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
}

#[tokio::test]
async fn renewal_keeps_the_previous_certificate_valid_until_it_expires() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let first = link.register(register("host-a")).await.unwrap();
    let identity_dir = t.dir.path().join("identity").join("host-a");
    let previous_dir = t.dir.path().join("previous-identity");
    copy_identity(&identity_dir, &previous_dir);

    link.renew().await.unwrap();
    assert!(
        link.heartbeat(HeartbeatRequest {
            provider_id: first.provider_id.clone(),
            generation: first.generation,
        })
        .await
        .is_ok()
    );
    // Both fingerprints are on the record: the new one from renewal and the
    // old one, still valid until it expires.
    let certificates = t
        .service
        .list_providers()
        .into_iter()
        .find(|provider| provider.name == "host-a")
        .map(|provider| provider.certificates)
        .unwrap();
    assert_eq!(certificates.len(), 2, "{certificates:?}");

    // A client holding the old certificate still authenticates.
    let previous = RemoteLink::connect(&previous_dir, Some(&t.base), None, "host-a")
        .await
        .unwrap();
    let second = previous.register(register("host-a")).await.unwrap();
    assert_eq!(second.provider_id, first.provider_id);
    assert_eq!(second.generation, first.generation + 1);
}

#[tokio::test]
async fn removing_a_provider_revokes_its_certificate() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    link.register(register("host-a")).await.unwrap();
    t.client().delete_provider("host-a").await.unwrap();
    assert!(t.service.list_providers().is_empty());
    let error = link.register(register("host-a")).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    assert!(error.message.contains("not enrolled"), "{}", error.message);
}

#[tokio::test]
async fn an_identity_from_an_old_service_ca_reports_re_enrollment() {
    let first = TestService::start().await;
    let link = first.enroll("host-a").await;
    link.register(register("host-a")).await.unwrap();
    let identity_dir = first.dir.path().join("identity").join("host-a");

    // A second service is a re-keyed service: its certificate does not chain
    // to the ca.pem stored with the identity.
    let second = TestService::start().await;
    let link = RemoteLink::connect(&identity_dir, Some(&second.base), None, "host-a")
        .await
        .unwrap();
    let error = link.register(register("host-a")).await.unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated, "{}", error.message);
    assert!(error.message.contains("different CA"), "{}", error.message);
    assert!(error.message.contains("--join"), "{}", error.message);
    assert!(error.message.contains("--token"), "{}", error.message);
}

#[tokio::test]
async fn an_identity_the_service_does_not_know_reports_re_enrollment() {
    let first = TestService::start().await;
    let link = first.enroll("host-a").await;
    link.register(register("host-a")).await.unwrap();
    let identity_dir = first.dir.path().join("identity").join("host-a");

    // The agent trusts the new service CA but keeps a certificate the new
    // service never issued, so only the server side of the handshake fails.
    let second = TestService::start().await;
    let ca_pem = second.ca_pem();
    let mut identity = preloop_sandbox::AgentIdentity::load(&identity_dir)
        .unwrap()
        .expect("stored identity");
    identity.metadata.base = second.base.clone();
    identity.metadata.ca_sha256 = preloop_sandbox::tls::pem_fingerprint(&ca_pem).unwrap();
    identity.ca_pem = ca_pem;
    identity.save().unwrap();

    let link = RemoteLink::connect(&identity_dir, None, None, "host-a")
        .await
        .unwrap();
    let error = link.register(register("host-a")).await.unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated, "{}", error.message);
    assert!(error.message.contains("re-enroll"), "{}", error.message);
}

#[tokio::test]
async fn the_caller_token_is_still_required_over_tls() {
    let t = TestService::start().await;
    let ca = t.ca_pem();
    let missing = SandboxClient::with_tls(&t.base, None, &ca, None).unwrap();
    assert_eq!(
        missing.list().await.unwrap_err().code,
        Code::Unauthenticated
    );
    let wrong = SandboxClient::with_tls(&t.base, Some("nope".into()), &ca, None).unwrap();
    assert_eq!(wrong.list().await.unwrap_err().code, Code::Unauthenticated);
    t.client().list().await.unwrap();
}
