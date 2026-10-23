//! Authorization seam coverage over the real TLS service.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::TestService;
use preloop_sandbox::agent::{Agent, AgentConfig};
use preloop_sandbox::api::*;
use preloop_sandbox::authz::{Action, Authorizer, Principal, PrincipalKind};
use preloop_sandbox::drivers::fake::FakeDriver;
use preloop_sandbox::{Code, Driver, SandboxApi};
use preloop_sandbox::{Error, Service};

#[derive(Clone, Default)]
struct RecordingAuthorizer {
    calls: Arc<Mutex<Vec<(Principal, Action)>>>,
    denied: Option<Action>,
}

impl RecordingAuthorizer {
    fn denying(action: Action) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            denied: Some(action),
        }
    }

    fn calls(&self) -> Vec<(Principal, Action)> {
        self.calls.lock().unwrap().clone()
    }
}

impl Authorizer for RecordingAuthorizer {
    fn authorize(&self, principal: &Principal, action: Action) -> Result<(), Error> {
        self.calls.lock().unwrap().push((principal.clone(), action));
        if self.denied == Some(action) {
            return Err(Error::new(
                Code::PermissionDenied,
                format!("recording policy denied {action:?}"),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

/// A syntactically valid digest-pinned artifact for the fake driver, which
/// boots nothing: this test exercises the authorization seam, not a registry.
const TEST_DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

fn template(name: &str) -> TemplateSpec {
    TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        artifacts: vec![Artifact {
            format: Artifact::OCI_IMAGE.into(),
            arch: host_arch().into(),
            r#ref: format!("ghcr.io/preloop/alpine@{TEST_DIGEST}"),
            digest: TEST_DIGEST.into(),
            size_bytes: 0,
            ..Artifact::default()
        }],
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

fn has_call(
    calls: &[(Principal, Action)],
    kind: PrincipalKind,
    name: &str,
    action: Action,
) -> bool {
    calls.iter().any(|(principal, seen)| {
        principal.kind == kind && principal.name == name && *seen == action
    })
}

#[tokio::test]
async fn every_client_and_provider_surface_reaches_the_authorizer() {
    let authorizer = Arc::new(RecordingAuthorizer::default());
    let t = TestService::start_with_authorizer(authorizer.clone()).await;
    let link = t.enroll("host-a").await;
    let fake = Arc::new(FakeDriver::new(
        "fake",
        t.dir.path().join("host"),
        FakeDriver::full_capabilities(),
    ));
    let agent = Agent::new(
        Arc::new(link),
        vec![fake as Arc<dyn Driver>],
        AgentConfig::new("host-a").with_scratch(t.dir.path().join("scratch")),
    );
    tokio::spawn(agent.run(None, t.shutdown_token()));

    let client = t.client();
    wait_for_provider(&t.service).await;
    client.put_template(template("alpine")).await.unwrap();
    client
        .put_pool(PoolSpec {
            name: "default".into(),
            template: "alpine".into(),
            ..PoolSpec::default()
        })
        .await
        .unwrap();
    assert!(client.list().await.unwrap().is_empty());
    let lease = client
        .acquire(AcquireRequest {
            idempotency_key: "authorization".into(),
            pool: "default".into(),
            wait_secs: Some(10),
            ..AcquireRequest::default()
        })
        .await
        .unwrap();
    let _ = client
        .exec_collect(ExecRequest {
            sandbox_id: lease.sandbox.id.clone(),
            lease_id: lease.lease.id.clone(),
            argv: vec!["true".into()],
            ..ExecRequest::default()
        })
        .await
        .unwrap();

    let source = t.dir.path().join("input.tar");
    {
        let mut builder = tar::Builder::new(std::fs::File::create(&source).unwrap());
        let data = b"payload";
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, "file.txt", &data[..])
            .unwrap();
        builder.finish().unwrap();
    }
    client
        .upload(&lease.sandbox.id, &lease.lease.id, "/work", &source)
        .await
        .unwrap();
    let destination = t.dir.path().join("output.tar");
    client
        .download(&lease.sandbox.id, &lease.lease.id, "/work", &destination)
        .await
        .unwrap();

    client.create_join_token(60, "").await.unwrap();
    let calls = authorizer.calls();
    assert!(has_call(
        &calls,
        PrincipalKind::Anonymous,
        "anonymous",
        Action::Enroll
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Read
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Write
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Admin
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Exec
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Transfer
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Provider,
        "host-a",
        Action::Provider
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Provider,
        "host-a",
        Action::Transfer
    ));
}

#[tokio::test]
async fn a_write_denial_blocks_mutations_but_not_reads() {
    let authorizer = Arc::new(RecordingAuthorizer::denying(Action::Write));
    let t = TestService::start_with_authorizer(authorizer.clone()).await;
    let client = t.client();

    let error = client.put_pool(PoolSpec::default()).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    let error = client.acquire(AcquireRequest::default()).await.unwrap_err();
    assert_eq!(error.code, Code::PermissionDenied);
    assert!(client.list().await.is_ok());

    let calls = authorizer.calls();
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Write
    ));
    assert!(has_call(
        &calls,
        PrincipalKind::Client,
        "client",
        Action::Read
    ));
}

#[tokio::test]
async fn authentication_rejects_before_authorization() {
    let authorizer = Arc::new(RecordingAuthorizer::default());
    let t = TestService::start_with_authorizer(authorizer.clone()).await;
    let bad = preloop_sandbox::SandboxClient::with_tls(
        &t.base,
        Some("wrong-token".into()),
        &t.ca_pem(),
        None,
    )
    .unwrap();

    let error = bad.list().await.unwrap_err();
    assert_eq!(error.code, Code::Unauthenticated);
    assert!(authorizer.calls().is_empty());
}
