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
use preloop_sandbox::generated::preloop::sandbox::v1 as rpc;
use preloop_sandbox::{Code, SandboxApi, SandboxClient};

fn register(name: &str) -> RegisterRequest {
    RegisterRequest {
        name: name.into(),
        drivers: vec![DriverInfo {
            driver: "fake".into(),
            capabilities: Capabilities::default().into(),
            ..DriverInfo::default()
        }],
        protocol_version: preloop_sandbox::PROTOCOL_VERSION,
        ..RegisterRequest::default()
    }
}

fn anonymous_providers(
    base: &str,
    ca_pem: &str,
) -> rpc::ProvidersClient<connectrpc::client::HttpClient> {
    let uri: axum::http::Uri = base.parse().unwrap();
    let config = connectrpc::client::ClientConfig::new(uri).json();
    let transport = connectrpc::client::HttpClient::with_tls(
        preloop_sandbox::tls::client_config(ca_pem, None).unwrap(),
    );
    rpc::ProvidersClient::new(transport, config)
}

fn copy_identity(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

#[tokio::test]
async fn register_with_a_mismatched_protocol_version_is_rejected() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let agent_version = preloop_sandbox::PROTOCOL_VERSION + 1;
    let error = link
        .register(RegisterRequest {
            name: "host-a".into(),
            protocol_version: agent_version,
            ..RegisterRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::FailedPrecondition);
    let message = preloop_sandbox::error::message(&error);
    // The caller is told both versions: what it spoke and what to speak.
    assert!(
        message.contains(&agent_version.to_string()),
        "agent version missing from {message:?}"
    );
    assert!(
        message.contains(&preloop_sandbox::PROTOCOL_VERSION.to_string()),
        "service version missing from {message:?}"
    );
    // A matching version still registers.
    link.register(register("host-a")).await.unwrap();
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
            ..HeartbeatRequest::default()
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
    let message = preloop_sandbox::error::message(&error);
    assert!(message.contains("join token"), "{message}");
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
    let message = preloop_sandbox::error::message(&error);
    assert!(message.contains("join token"), "{message}");
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
    assert!(
        preloop_sandbox::error::message(&error).contains("pin"),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_token_pinned_to_another_ca_is_rejected_by_the_service() {
    let t = TestService::start().await;
    let token = t.join_token(Duration::from_secs(60)).await;
    let (head, _) = token.rsplit_once('.').unwrap();
    let tampered = format!("{head}.{}", "00".repeat(32));
    // Go straight to the service: the agent would refuse the pin before
    // sending, so only a raw call exercises the service-side check.
    let anonymous = anonymous_providers(&t.base, &t.ca_pem());
    let error = anonymous
        .enroll(EnrollRequest {
            token: tampered,
            name: "host-a".into(),
            csr_pem: String::new(),
            ..EnrollRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    // Token-validation failures are indistinguishable by design: the same
    // code and message covers a wrong CA pin, a bad secret, an unknown id
    // and an expired token, so the endpoint is not a validity oracle.
    assert_eq!(
        preloop_sandbox::error::message(&error),
        "invalid join token",
        "{error:?}"
    );
}

#[tokio::test]
async fn register_with_a_mismatched_name_is_denied() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let error = link.register(register("host-b")).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    assert!(
        preloop_sandbox::error::message(&error).contains("name"),
        "{error:?}"
    );
    // The name from the certificate still works.
    link.register(register("host-a")).await.unwrap();
}

#[tokio::test]
async fn provider_calls_without_a_client_certificate_are_denied() {
    let t = TestService::start().await;
    let anonymous = anonymous_providers(&t.base, &t.ca_pem());
    let error = anonymous.register(register("ghost")).await.unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    let error = anonymous
        .heartbeat(HeartbeatRequest {
            provider_id: "prv-missing".into(),
            generation: 1,
            ..HeartbeatRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    // Enroll is the one method that does not need a client certificate; it
    // still needs a valid token.
    let error = anonymous
        .enroll(EnrollRequest {
            token: "plj1.missing.secret".into(),
            name: "ghost".into(),
            csr_pem: String::new(),
            ..EnrollRequest::default()
        })
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
            ..HeartbeatRequest::default()
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
    assert!(
        preloop_sandbox::error::message(&error).contains("not enrolled"),
        "{error:?}"
    );
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
    assert_eq!(
        error.code,
        Code::Unauthenticated,
        "{}",
        preloop_sandbox::error::message(&error)
    );
    assert!(
        preloop_sandbox::error::message(&error).contains("different CA"),
        "{}",
        preloop_sandbox::error::message(&error)
    );
    assert!(
        preloop_sandbox::error::message(&error).contains("--join"),
        "{}",
        preloop_sandbox::error::message(&error)
    );
    assert!(
        preloop_sandbox::error::message(&error).contains("--token"),
        "{}",
        preloop_sandbox::error::message(&error)
    );
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
    assert_eq!(
        error.code,
        Code::Unauthenticated,
        "{}",
        preloop_sandbox::error::message(&error)
    );
    assert!(
        preloop_sandbox::error::message(&error).contains("re-enroll"),
        "{}",
        preloop_sandbox::error::message(&error)
    );
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

fn register_full(name: &str) -> RegisterRequest {
    RegisterRequest {
        name: name.into(),
        drivers: vec![DriverInfo {
            driver: "fake".into(),
            capabilities: preloop_sandbox::drivers::fake::FakeDriver::full_capabilities().into(),
            ..DriverInfo::default()
        }],
        protocol_version: preloop_sandbox::PROTOCOL_VERSION,
        ..RegisterRequest::default()
    }
}

#[tokio::test]
async fn an_unbound_token_cannot_take_over_an_existing_provider() {
    let t = TestService::start().await;
    let victim = t.enroll("host-a").await;
    let session = victim.register(register("host-a")).await.unwrap();
    let certificate_fingerprints = |t: &TestService| {
        t.service
            .list_providers()
            .into_iter()
            .find(|provider| provider.name == "host-a")
            .map(|provider| {
                provider
                    .certificates
                    .iter()
                    .map(|certificate| certificate.sha256.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap()
    };
    let before = certificate_fingerprints(&t);
    assert_eq!(before.len(), 1);
    // A fresh unbound token presented under the victim's name is refused:
    // replacing a live provider's certificates requires a token minted
    // bound to that name.
    let token = t.join_token(Duration::from_secs(60)).await;
    let error = RemoteLink::enroll(&t.base, &token, t.dir.path().join("attacker"), "host-a")
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        Code::AlreadyExists,
        "{}",
        preloop_sandbox::error::message(&error)
    );
    // The victim is untouched: same provider, same single certificate, and
    // the certificate still authenticates.
    let providers = t.service.list_providers();
    let provider = providers
        .iter()
        .find(|provider| provider.name == "host-a")
        .expect("provider still present");
    assert_eq!(provider.id, session.provider_id);
    assert_eq!(certificate_fingerprints(&t), before);
    victim
        .heartbeat(HeartbeatRequest {
            provider_id: session.provider_id,
            generation: session.generation,
            ..HeartbeatRequest::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_name_bound_token_reenrolls_its_provider_and_replaces_its_certificates() {
    let t = TestService::start().await;
    let victim = t.enroll("host-a").await;
    let first = victim.register(register("host-a")).await.unwrap();
    let identity_dir = t.dir.path().join("identity").join("host-a");
    let saved_dir = t.dir.path().join("saved-identity");
    copy_identity(&identity_dir, &saved_dir);

    // The operator mints a token bound to host-a for the replacement host:
    // that is the one credential allowed to take the name over.
    let token = t.join_token_for(Duration::from_secs(60), "host-a").await;
    let replacement =
        RemoteLink::enroll(&t.base, &token, t.dir.path().join("replacement"), "host-a")
            .await
            .unwrap();
    let second = replacement.register(register("host-a")).await.unwrap();
    assert_eq!(second.provider_id, first.provider_id);
    assert_eq!(second.generation, first.generation + 1);

    // The replaced certificate no longer authenticates.
    let stale = RemoteLink::connect(&saved_dir, Some(&t.base), None, "host-a")
        .await
        .unwrap();
    let error = stale.register(register("host-a")).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    // The replacement's does.
    replacement
        .heartbeat(HeartbeatRequest {
            provider_id: second.provider_id,
            generation: second.generation,
            ..HeartbeatRequest::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn a_name_bound_token_cannot_enroll_a_different_provider() {
    let t = TestService::start().await;
    let token = t.join_token_for(Duration::from_secs(60), "host-b").await;
    let error = RemoteLink::enroll(&t.base, &token, t.dir.path().join("wrong"), "host-a")
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        Code::PermissionDenied,
        "{}",
        preloop_sandbox::error::message(&error)
    );
    // The refusal does not consume the token: its own name still enrolls.
    let link = RemoteLink::enroll(&t.base, &token, t.dir.path().join("right"), "host-b")
        .await
        .unwrap();
    link.register(register("host-b")).await.unwrap();
}

async fn enroll_failure(t: &TestService, token: &str) -> (Code, String) {
    let anonymous = anonymous_providers(&t.base, &t.ca_pem());
    let (_key, csr) = preloop_sandbox::tls::generate_csr("host-a").unwrap();
    let error = anonymous
        .enroll(EnrollRequest {
            token: token.into(),
            name: "host-a".into(),
            csr_pem: csr,
            ..EnrollRequest::default()
        })
        .await
        .unwrap_err();
    (
        error.code,
        preloop_sandbox::error::message(&error).to_owned(),
    )
}

#[tokio::test]
async fn enroll_token_failures_are_indistinguishable() {
    let t = TestService::start().await;
    let real = t.join_token(Duration::from_secs(60)).await;
    let parts: Vec<&str> = real.split('.').collect();
    assert_eq!(parts.len(), 4);
    let wrong_pin = format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], "00".repeat(32));
    let unknown_id = format!("{}.jt-unknown.{}.{}", parts[0], parts[2], parts[3]);
    let bad_secret = format!("{}.{}.{}.{}", parts[0], parts[1], "ab".repeat(32), parts[3]);
    // Malformed, issued for another CA, unknown id and wrong secret all
    // return the identical code and message, so a caller holding part of a
    // token cannot probe which component is wrong.
    for token in ["garbage".to_owned(), wrong_pin, unknown_id, bad_secret] {
        let (code, message) = enroll_failure(&t, &token).await;
        assert_eq!(code, Code::Unauthenticated, "{token}");
        assert_eq!(message, "invalid join token", "{token}");
    }
    // Expired tokens collapse into the same response (whether the record
    // was pruned by maintenance or rejected on its expiry).
    let stale = t.join_token(Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let (code, message) = enroll_failure(&t, &stale).await;
    assert_eq!(code, Code::Unauthenticated);
    assert_eq!(message, "invalid join token");
    // The failures above neither consumed nor locked the genuine token.
    RemoteLink::enroll(&t.base, &real, t.dir.path().join("genuine"), "host-a")
        .await
        .unwrap();
}

#[tokio::test]
async fn renewal_prunes_the_oldest_certificate_beyond_the_cap() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    link.register(register("host-a")).await.unwrap();
    let identity_dir = t.dir.path().join("identity").join("host-a");
    let oldest_dir = t.dir.path().join("oldest-identity");
    copy_identity(&identity_dir, &oldest_dir);

    // Renew up to the cap: the retained set stops growing there, and one
    // renewal past it prunes the original certificate even though it has
    // not expired — rotation can evict a compromised certificate.
    let cap = preloop_sandbox::service::MAX_PROVIDER_CERTIFICATES;
    for _ in 0..cap {
        link.renew().await.unwrap();
    }
    let certificates = t
        .service
        .list_providers()
        .into_iter()
        .find(|provider| provider.name == "host-a")
        .map(|provider| provider.certificates)
        .unwrap();
    assert_eq!(certificates.len(), cap, "{certificates:?}");

    // The current certificate still authenticates.
    link.register(register("host-a")).await.unwrap();
    // The pruned one no longer does.
    let stale = RemoteLink::connect(&oldest_dir, Some(&t.base), None, "host-a")
        .await
        .unwrap();
    let error = stale.register(register("host-a")).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
}

#[tokio::test]
async fn reenrollment_redelivers_in_flight_work() {
    let t = TestService::start().await;
    let link = t.enroll("host-a").await;
    let session = link.register(register_full("host-a")).await.unwrap();
    t.client()
        .put_template(common::template("alpine"))
        .await
        .unwrap();
    // Acquire blocks until the sandbox is ready (or fails); drive it in
    // the background while the provider side is manipulated by hand.
    let client = t.client();
    let acquire = tokio::spawn(async move {
        client
            .acquire(AcquireRequest {
                idempotency_key: "mtls-inflight".into(),
                template: "alpine".into(),
                wait_secs: Some(60),
                ..AcquireRequest::default()
            })
            .await
    });
    // The create is delivered to the provider and sits in flight,
    // unacknowledged.
    let first = link
        .poll(PollRequest {
            provider_id: session.provider_id.clone(),
            generation: session.generation,
            wait_secs: 10,
            ..PollRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(first.work.len(), 1, "{first:?}");
    let work_id = first.work[0].id.clone();

    // The provider re-enrolls (bound token) and registers a new session.
    let token = t.join_token_for(Duration::from_secs(60), "host-a").await;
    let replacement =
        RemoteLink::enroll(&t.base, &token, t.dir.path().join("replacement"), "host-a")
            .await
            .unwrap();
    let session = replacement.register(register_full("host-a")).await.unwrap();
    // The in-flight item is redelivered to the new session rather than
    // dropped: the sandbox must not wedge in PROVISIONING.
    let second = replacement
        .poll(PollRequest {
            provider_id: session.provider_id.clone(),
            generation: session.generation,
            wait_secs: 10,
            ..PollRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(second.work.len(), 1, "{second:?}");
    assert_eq!(second.work[0].id, work_id);
    // Fail the work so the blocked acquire unwinds.
    replacement
        .report(ReportRequest {
            provider_id: session.provider_id,
            generation: session.generation,
            work_id,
            ok: false,
            error: "test over".into(),
            ..ReportRequest::default()
        })
        .await
        .unwrap();
    assert!(acquire.await.unwrap().is_err());
}
