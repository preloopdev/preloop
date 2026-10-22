//! A CA-less client must build without a process-level rustls provider.
//!
//! Deliberately its own test binary: starting the service (`ServerTls`)
//! installs the ring provider, and an installed default would hide the
//! `CryptoProvider` panic that aborted `preloop sandbox --service https://…
//! provider ls` before it could report the certificate problem.

use std::sync::Arc;

use preloop_sandbox::{Code, SandboxApi, SandboxClient};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

#[tokio::test]
async fn a_client_without_a_ca_reports_an_untrusted_certificate() {
    // A publicly-unverifiable certificate for the address the client dials.
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
    let certificate = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate], key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handshake = tokio::spawn(async move {
        // The client aborts the handshake on verification; whether the server
        // sees an alert or a reset does not matter to this assertion.
        if let Ok((stream, _)) = listener.accept().await {
            let _ = acceptor.accept(stream).await;
        }
    });

    // No CA and no installed default: this used to panic while building the
    // rustls config, which is the regression this test binary guards.
    let client = SandboxClient::new(
        format!("https://127.0.0.1:{port}"),
        Some("token".to_owned()),
    )
    .expect("a CA-less client must build without a process-level crypto provider");
    let error = client
        .list_templates()
        .await
        .expect_err("a certificate from an unknown CA must be an error");
    assert_eq!(
        error.code,
        Code::Unauthenticated,
        "{}",
        preloop_sandbox::error::message(&error)
    );
    handshake.await.unwrap();
}
