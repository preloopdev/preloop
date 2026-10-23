//! Authorization behavior over the real TLS service.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::TestService;
use preloop_sandbox::agent::{Agent, AgentConfig};
use preloop_sandbox::api::*;
use preloop_sandbox::authz::{Action, Authorizer, Principal, PrincipalKind};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::{Code, Driver, Error, SandboxApi, Service};

#[derive(Clone, Default)]
struct Policy {
    denied: Option<Action>,
    calls: Arc<AtomicUsize>,
}

impl Policy {
    fn denying(action: Action) -> Self {
        Self {
            denied: Some(action),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Authorizer for Policy {
    fn authorize(&self, principal: &Principal, action: Action) -> Result<(), Error> {
        if principal.kind == PrincipalKind::Client {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
        if self.denied == Some(action) {
            return Err(Error::new(
                Code::PermissionDenied,
                format!("test policy denied {action:?}"),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "test-policy"
    }
}

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        image: Some("alpine:3.20".into()),
        ..TemplateSpec::default()
    }
}

async fn wait_for_provider(service: &Service) {
    for _ in 0..200 {
        if service
            .list_providers()
            .iter()
            .any(|provider| provider.generation > 0 && !provider.drivers.is_empty())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("provider did not register");
}

#[tokio::test]
async fn denying_write_blocks_put_pool_and_acquire_without_mutation() {
    let policy = Arc::new(Policy::denying(Action::Write));
    let t = TestService::start_with_authorizer(policy).await;
    let client = t.client();

    let error = client
        .put_pool(PoolSpec {
            name: "denied-pool".into(),
            template: "missing-template".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);

    let error = client
        .acquire(AcquireRequest {
            idempotency_key: "denied-acquire".into(),
            template: "missing-template".into(),
            wait_secs: Some(0),
            ..AcquireRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);

    assert!(client.list().await.unwrap().is_empty());
    assert!(client.list_pools().await.unwrap().is_empty());
    assert!(t.service.list().is_empty());
    assert!(t.service.list_pools().is_empty());
}

#[tokio::test]
async fn failed_bearer_authentication_precedes_policy_evaluation() {
    let policy = Arc::new(Policy::default());
    let t = TestService::start_with_authorizer(policy.clone()).await;

    for token in [None, Some("wrong-token")] {
        let client = preloop_sandbox::SandboxClient::with_tls(
            &t.base,
            token.map(str::to_owned),
            &t.ca_pem(),
            None,
        )
        .unwrap();
        let error = client.list().await.unwrap_err();
        assert_eq!(error.code, Code::Unauthenticated);
        assert_eq!(error.message, "missing or bad bearer token");
        assert_eq!(policy.calls(), 0);
    }
}

#[tokio::test]
async fn denied_transfer_does_not_consume_ticket_and_bad_ticket_auth_skips_policy() {
    let policy = Arc::new(Policy::denying(Action::Transfer));
    let t = TestService::start_with_authorizer(policy.clone()).await;
    let link = t.enroll("host-a").await;
    let fake = Arc::new(FakeDriver::new(
        "fake",
        t.dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let agent = Agent::new(
        Arc::new(link),
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("host-a", t.dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(None, t.shutdown_token()));

    let client = t.client();
    wait_for_provider(&t.service).await;
    client.put_template(template("alpine")).await.unwrap();
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "transfer-authz".into(),
            template: "alpine".into(),
            wait_secs: Some(10),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let ticket = t
        .service
        .open_stream(OpenStreamRequest {
            sandbox_id: lease.sandbox.id,
            kind: StreamKind::Download,
            path: "/work".into(),
        })
        .unwrap();
    let ticket_id = ticket.url.rsplit('/').next().unwrap();
    let policy_calls = policy.calls();
    let http = preloop_sandbox::tls::http_client(&t.ca_pem(), None).unwrap();
    let url = format!("{}{}", t.base, ticket.url);

    let response = http
        .get(&url)
        .bearer_auth("wrong-ticket-token")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    let error: Error = response.json().await.unwrap();
    assert_eq!(error.code, Code::Unauthenticated);
    assert_eq!(policy.calls(), policy_calls);

    let response = http
        .get(&url)
        .bearer_auth(&ticket.token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    let error: Error = response.json().await.unwrap();
    assert_eq!(error.code, Code::PermissionDenied);
    assert_eq!(policy.calls(), policy_calls + 1);

    let (kind, _) = t.service.claim_ticket(ticket_id, &ticket.token).unwrap();
    assert_eq!(kind, StreamKind::Download);
    t.service.drop_ticket(ticket_id);
}

#[tokio::test]
async fn enrollment_and_ca_retrieval_respect_enroll_policy() {
    let t = TestService::start_with_authorizer(Arc::new(Policy::denying(Action::Enroll))).await;
    let http = preloop_sandbox::tls::http_client(&t.ca_pem(), None).unwrap();

    for response in [
        http.get(format!("{}/sandbox/v1/ca.pem", t.base))
            .send()
            .await
            .unwrap(),
        http.post(format!("{}/preloop.sandbox.v1.Providers/Enroll", t.base))
            .json(&EnrollRequest::default())
            .send()
            .await
            .unwrap(),
    ] {
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
        let error: Error = response.json().await.unwrap();
        assert_eq!(error.code, Code::PermissionDenied);
    }
}
