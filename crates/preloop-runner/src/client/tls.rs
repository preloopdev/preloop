//! The process-wide TLS root set for the runner's outbound clients.
//!
//! Same policy as `preloop_runner_server::shared_http::tls_roots`, pinned onto
//! every runner client with a preconfigured rustls config so no handshake ever
//! reaches the OS platform verifier (on macOS that verifier's
//! `SecTrustEvaluateWithError` asks `trustd`/`securityd` synchronously with no
//! deadline, so a hung or prompting daemon would hang the runner):
//!
//!   1. the bundled Mozilla set (`webpki-roots`), which needs no OS call;
//!   2. the certificates installed in the OS store, read once with a deadline;
//!   3. the extra `PRELOOP_GITHUB_CA_FILE` / `SSL_CERT_FILE` bundles.
//!
//! The runner re-expresses the shared root set instead of calling it: it does
//! not depend on the server crate (that crate carries the control plane), and
//! the workspace's supply-chain policy ships dependency additions in a
//! separate PR, so `shared_http` cannot host the loader for both binaries.
//! Keep the two in step — same three sources, same 5 s OS-store deadline.

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Context as _;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;

/// How long the OS certificate store read may take.
///
/// The store is served by a daemon (`securityd`/`trustd` on macOS, the Secret
/// Service on Linux) that can hang or wait on a console prompt. A healthy
/// store answers in milliseconds; 5 s is far beyond that, yet short enough
/// that the first client build on a broken host still finishes promptly.
const NATIVE_ROOTS_DEADLINE: Duration = Duration::from_secs(5);

/// Root certificates every runner client verifies against, resolved once per
/// process (see the module docs for the three sources).
static ROOTS: LazyLock<rustls::RootCertStore> = LazyLock::new(build_roots);

fn build_roots() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let native = load_native_root_certs(
        NATIVE_ROOTS_DEADLINE,
        rustls_native_certs::load_native_certs,
    );
    let (added, ignored) = roots.add_parsable_certificates(native);
    if ignored > 0 {
        tracing::warn!(ignored, "ignoring unusable certificate from the OS store");
    }
    let (added_bundles, ignored_bundles) =
        roots.add_parsable_certificates(ca_certificates(|name| std::env::var(name).ok()));
    if ignored_bundles > 0 {
        tracing::warn!(
            ignored_bundles,
            "ignoring unusable certificate from a CA bundle"
        );
    }
    tracing::info!(
        bundled = webpki_roots::TLS_SERVER_ROOTS.len(),
        os_store = added,
        bundles = added_bundles,
        "loaded the runner's TLS root certificates"
    );
    roots
}

/// Absorb the OS certificate store on a dedicated thread, waiting at most
/// `deadline`: the store's daemon can hang, and whoever builds the first
/// client must not wait on it longer than that. On timeout the loader thread is
/// detached and leaks until the daemon answers, which is the price of not
/// blocking on it.
///
/// `load` is a parameter so tests can hand in a store that never answers.
fn load_native_root_certs(
    deadline: Duration,
    load: impl FnOnce() -> rustls_native_certs::CertificateResult + Send + 'static,
) -> Vec<CertificateDer<'static>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let spawned = std::thread::Builder::new()
        .name("runner-native-root-certs".to_owned())
        .spawn(move || {
            let _ = sender.send(load());
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "could not spawn the OS root certificate loader thread");
        return Vec::new();
    }
    match receiver.recv_timeout(deadline) {
        Ok(loaded) => {
            for error in &loaded.errors {
                tracing::warn!(%error, "reading the OS certificate store");
            }
            loaded.certs
        }
        Err(error) => {
            tracing::warn!(%error, timeout_secs = deadline.as_secs(),
                "OS certificate store did not answer; continuing without its certificates");
            Vec::new()
        }
    }
}

/// Extra PEM bundles from `PRELOOP_GITHUB_CA_FILE` (a GHES or emulator forge
/// CA) and `SSL_CERT_FILE` (the OpenSSL/native convention the runner already
/// honours for its control client). Missing or unreadable files log and add
/// nothing: verification stays on, we just gain no extra roots.
fn ca_certificates(env: impl Fn(&str) -> Option<String>) -> Vec<CertificateDer<'static>> {
    let mut out = Vec::new();
    for var in ["PRELOOP_GITHUB_CA_FILE", "SSL_CERT_FILE"] {
        let Some(path) = env(var) else {
            continue;
        };
        if path.trim().is_empty() {
            continue;
        }
        match std::fs::read(&path)
            .map_err(|error| format!("reading {path}: {error}"))
            .and_then(|pem| {
                CertificateDer::pem_slice_iter(&pem)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| format!("parsing {path}: {error}"))
            }) {
            Ok(certs) => {
                tracing::info!(path = %path, env = var, count = certs.len(),
                    "loaded additional TLS root certificates");
                out.extend(certs);
            }
            Err(error) => tracing::warn!(%error, env = var, "ignoring unusable CA bundle"),
        }
    }
    out
}

/// Resolve the process-wide root set now, on the calling thread, and report
/// how many anchors it holds.
///
/// Hosts call this from a blocking thread during startup so the OS-store read
/// (bounded, but up to 5 s against a hung daemon) never lands on an async
/// worker that happens to build the first client.
pub fn preload_roots() -> usize {
    ROOTS.len()
}

/// A rustls client config verifying against the shared root set plus
/// `extra_ca_pem`, ready for `reqwest::ClientBuilder::use_preconfigured_tls`.
///
/// `extra_ca_pem` is the runner's `--ca-bundle` path (or `SSL_CERT_FILE`
/// fallback), already read by the caller; it must stay honoured now that the
/// platform verifier is out of the picture.
pub fn client_config(extra_ca_pem: Option<&[u8]>) -> anyhow::Result<rustls::ClientConfig> {
    let mut roots = ROOTS.clone();
    if let Some(pem) = extra_ca_pem {
        let certs = CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .context("parsing the CA bundle")?;
        for cert in certs {
            roots
                .add(cert)
                .context("adding a certificate from the CA bundle")?;
        }
    }
    let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("building the runner TLS client config")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    // The runner speaks HTTP/1.1 only (no `http2` feature on its reqwest), so
    // advertise exactly that: an empty ALPN list would make some proxies and
    // GHES front ends negotiate nothing and hang.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Blocking-client twin of [`client_config`], for the OTLP exporters'
/// blocking reqwest client (passed to
/// `preloop_observability::ObservabilityConfig::with_otel_client_builder`).
///
/// Telemetry is fail-open: if the config cannot be built the builder is left
/// on reqwest's platform verifier and the failure is logged, rather than
/// dropping the whole observability handle.
pub fn with_shared_roots_blocking(
    builder: reqwest::blocking::ClientBuilder,
) -> reqwest::blocking::ClientBuilder {
    match client_config(None) {
        Ok(config) => builder.use_preconfigured_tls(config),
        Err(error) => {
            tracing::warn!(%error,
                "could not install the shared TLS roots on the OTLP client; \
                 falling back to the platform verifier");
            builder
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bundle named by `PRELOOP_GITHUB_CA_FILE` is parsed into the root set;
    /// a missing file adds nothing instead of failing every client.
    #[test]
    fn env_ca_bundle_is_loaded_and_missing_files_are_ignored() {
        let dir = std::env::temp_dir().join(format!("runner-tls-roots-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let Some(fixture) = TestCert::generate(&dir) else {
            eprintln!("skipping: no usable openssl CLI to generate a test certificate");
            return;
        };
        let bundle_path = fixture.cert_path.to_string_lossy().into_owned();
        let lookup =
            move |name: &str| (name == "PRELOOP_GITHUB_CA_FILE").then(|| bundle_path.clone());
        assert_eq!(ca_certificates(&lookup).len(), 1);
        let missing = |name: &str| {
            (name == "PRELOOP_GITHUB_CA_FILE").then(|| "/nonexistent/ca.pem".to_owned())
        };
        assert!(ca_certificates(missing).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The root store must survive an OS store that never answers.
    #[test]
    fn os_root_store_that_never_answers_is_abandoned_at_the_deadline() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(10));
            let _ = release.send(());
        });
        let started = std::time::Instant::now();
        let certs = load_native_root_certs(Duration::from_millis(100), move || {
            let _ = blocked.recv();
            rustls_native_certs::CertificateResult::default()
        });
        assert!(certs.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a hung OS store must be abandoned at the deadline, waited {:?}",
            started.elapsed()
        );
    }

    /// Behavioural check: the config this module builds completes a TLS
    /// handshake against a server whose certificate chains to the
    /// `PRELOOP_GITHUB_CA_FILE` bundle, and the same config without that
    /// bundle does not. This is the regression `use_preconfigured_tls` could
    /// introduce (dropping the runner's CA bundle would leave a private forge
    /// permanently unreachable), and it exercises the merged root set rather
    /// than asserting builder wiring. Loopback only; no OS trust store.
    #[cfg(unix)]
    #[test]
    fn client_config_trusts_the_ca_bundle_over_tls() {
        let dir = std::env::temp_dir().join(format!("runner-tls-handshake-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let Some(fixture) = TestCert::generate(&dir) else {
            eprintln!("skipping: no usable openssl CLI to generate a test certificate");
            return;
        };
        let port = fixture.spawn_server();
        let pem = std::fs::read(&fixture.cert_path).unwrap();

        let without_bundle = client_config(None).expect("build the client config");
        assert!(
            handshake(without_bundle, port).is_err(),
            "the test certificate must not be trusted without the CA bundle"
        );

        let with_bundle = client_config(Some(&pem)).expect("build the config with the CA bundle");
        handshake(with_bundle, port).expect("the CA bundle must be trusted");
    }

    /// Run one HTTPS request through a rustls client config.
    #[cfg(unix)]
    fn handshake(config: rustls::ClientConfig, port: u16) -> std::io::Result<Vec<u8>> {
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let connection = rustls::ClientConnection::new(std::sync::Arc::new(config), name).unwrap();
        let socket = std::net::TcpStream::connect(("127.0.0.1", port))?;
        socket.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut tls = rustls::StreamOwned::new(connection, socket);
        std::io::Write::write_all(&mut tls, b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n")?;
        let mut response = vec![0u8; 128];
        let n = std::io::Read::read(&mut tls, &mut response)?;
        response.truncate(n);
        Ok(response)
    }

    /// A throwaway self-signed `localhost` certificate, generated with the
    /// `openssl` CLI so the runner crate needs no certificate-generation
    /// dependency.
    struct TestCert {
        cert_path: std::path::PathBuf,
        certs: Vec<CertificateDer<'static>>,
        key: rustls::pki_types::PrivateKeyDer<'static>,
    }

    impl TestCert {
        fn generate(dir: &std::path::Path) -> Option<Self> {
            let cert_path = dir.join("cert.pem");
            let key_path = dir.join("key.pem");
            let config_path = dir.join("cert.cnf");
            std::fs::write(
                &config_path,
                "[req]\ndistinguished_name = dn\nx509_extensions = v3_req\nprompt = no\n\
                 [dn]\nCN = localhost\n\
                 [v3_req]\nsubjectAltName = DNS:localhost,IP:127.0.0.1\n",
            )
            .ok()?;
            let generated = std::process::Command::new("openssl")
                .args([
                    "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                ])
                .arg("-keyout")
                .arg(&key_path)
                .arg("-out")
                .arg(&cert_path)
                .arg("-config")
                .arg(&config_path)
                .output()
                .ok()?;
            if !generated.status.success() {
                return None;
            }
            let certs = CertificateDer::pem_file_iter(&cert_path)
                .ok()?
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            let key = rustls::pki_types::PrivateKeyDer::from_pem_file(&key_path).ok()?;
            Some(Self {
                cert_path,
                certs,
                key,
            })
        }

        /// Serve one HTTPS response on a loopback port, then exit.
        fn spawn_server(&self) -> u16 {
            let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .with_no_client_auth()
            .with_single_cert(self.certs.clone(), self.key.clone_key())
            .expect("server config");
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
            let port = listener.local_addr().unwrap().port();
            std::thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    let Ok(connection) =
                        rustls::ServerConnection::new(std::sync::Arc::new(config.clone()))
                    else {
                        continue;
                    };
                    let mut tls = rustls::StreamOwned::new(connection, stream);
                    // A refused handshake surfaces here; only a complete
                    // request gets the response the test asserts on.
                    if std::io::Read::read(&mut tls, &mut [0u8; 1024]).is_err() {
                        continue;
                    }
                    let _ = std::io::Write::write_all(&mut tls, b"ok");
                }
            });
            port
        }
    }
}
