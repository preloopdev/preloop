//! Shared TLS harness: a real Sandbox Service on an ephemeral port with its
//! own CA, plus helpers to enroll providers against it.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use preloop_sandbox::agent::RemoteLink;
use preloop_sandbox::server::{Auth, serve};
use preloop_sandbox::store;
use preloop_sandbox::{SandboxClient, ServerTls, Service, ServiceConfig};
use tokio_util::sync::CancellationToken;

/// A running service and its network endpoint.
pub struct TestService {
    /// The service core.
    pub service: Arc<Service>,
    /// `https://127.0.0.1:<port>`.
    pub base: String,
    /// The caller bearer token.
    pub client_token: String,
    /// Temp root (identity directories live here).
    pub dir: tempfile::TempDir,
    shutdown: CancellationToken,
}

impl Drop for TestService {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl TestService {
    /// Start a fresh service.
    pub async fn start() -> Self {
        Self::start_in(tempfile::tempdir().unwrap()).await
    }

    /// Start a fresh service rooted in `dir`.
    pub async fn start_in(dir: tempfile::TempDir) -> Self {
        let state = dir.path().join("state");
        let tls = ServerTls::load_or_create(
            &state,
            vec!["127.0.0.1".to_owned(), "localhost".to_owned()],
            None,
            None,
        )
        .expect("server tls");
        let mut config = ServiceConfig::new(dir.path().join("blobs"));
        config.ca = Some(tls.ca.clone());
        let service = Service::open(store::open("memory:").await.unwrap(), config)
            .await
            .unwrap();
        let client_token = "test-client-token".to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("https://{}", listener.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        tokio::spawn(serve(
            listener,
            service.clone(),
            Auth::new(client_token.clone()),
            tls,
            shutdown.clone(),
        ));
        tokio::spawn(service.clone().run_maintenance(shutdown.clone()));
        Self {
            service,
            base,
            client_token,
            dir,
            shutdown,
        }
    }

    /// A caller client that trusts the service CA.
    pub fn client(&self) -> SandboxClient {
        SandboxClient::with_tls(
            &self.base,
            Some(self.client_token.clone()),
            &self.ca_pem(),
            None,
        )
        .unwrap()
    }

    /// The shutdown token this harness and its agents share.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// The service CA PEM.
    pub fn ca_pem(&self) -> String {
        self.service.ca_pem().expect("service CA")
    }

    /// Mint a join token.
    pub async fn join_token(&self, ttl: Duration) -> String {
        self.service.create_join_token(ttl).await.unwrap().token
    }

    /// Enroll `name` with a fresh single-use token and return its link.
    pub async fn enroll(&self, name: &str) -> RemoteLink {
        let token = self.join_token(Duration::from_secs(60)).await;
        RemoteLink::enroll(
            &self.base,
            &token,
            self.dir.path().join("identity").join(name),
            name,
        )
        .await
        .unwrap()
    }
}

/// A registered template spec used by the network tests.
pub fn template(name: &str) -> preloop_sandbox::api::TemplateSpec {
    use preloop_sandbox::api::Artifact;
    const DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    preloop_sandbox::api::TemplateSpec {
        name: name.into(),
        os: "linux".into(),
        artifacts: vec![Artifact {
            format: Artifact::OCI_IMAGE.into(),
            arch: preloop_sandbox::api::host_arch().into(),
            r#ref: format!("ghcr.io/preloop/alpine@{DIGEST}"),
            digest: DIGEST.into(),
            size_bytes: 0,
            ..Artifact::default()
        }],
        ..preloop_sandbox::api::TemplateSpec::default()
    }
}
