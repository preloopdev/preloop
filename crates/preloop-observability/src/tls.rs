//! The process-wide TLS root set, and the helpers that install it on clients.
//!
//! Every Preloop binary (engine, CLI, runner) talks to engines, forges, and
//! OTLP backends over HTTPS, and the platform verifiers must stay out of that
//! path: on macOS reqwest's default verifier calls `SecTrustEvaluateWithError`
//! synchronously inside the handshake, which asks `securityd`/`trustd` with no
//! deadline, so a hung or prompting trust daemon can stall the process for as
//! long as it likes.
//!
//! The set resolved here, once per process, comes from three sources:
//!
//!   1. the bundled Mozilla anchors (`webpki-root-certs`), which need no OS
//!      call;
//!   2. the certificates installed in the OS store (macOS Keychain, Linux
//!      `/etc/ssl/certs`, Windows certificate store), read with a deadline —
//!      see [`NATIVE_ROOTS_DEADLINE`];
//!   3. the `PRELOOP_GITHUB_CA_FILE` / `SSL_CERT_FILE` bundles.
//!
//! Verification stays on — rustls checks the chain and the hostname in-process
//! against exactly these anchors — and the trust data is fetched once, with a
//! bound.
//!
//! Hosts install the set with one of the `with_*` helpers: [`with_roots`] and
//! [`with_roots_blocking`] pin reqwest's own rustls config to the list with
//! `tls_certs_only`; [`client_config`] (and its blocking installer,
//! [`with_client_config_blocking`]) build a rustls config for
//! `use_preconfigured_tls`, for clients that merge in a CA bundle of their own.

use std::convert::Infallible;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Context as _;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;

use crate::deadline::DeadlineGuard;

/// How long the OS certificate store read may take.
///
/// The store is served by a daemon (`securityd`/`trustd` on macOS, the Secret
/// Service on Linux) that can hang or wait on a console prompt. A healthy
/// store answers in milliseconds; 5 s is far beyond that, yet short enough
/// that the first client build on a broken host still finishes promptly.
pub const NATIVE_ROOTS_DEADLINE: Duration = Duration::from_secs(5);

/// Reported when the OS store does not answer. It names the dependency and
/// what happens instead, as [`DeadlineGuard`] expects of its message.
const OS_STORE_UNRESPONSIVE: &str = "the OS certificate store did not answer within its deadline; \
     using the bundled and environment roots only";

/// Every root certificate the process trusts for outbound TLS, resolved once
/// per process from the three sources described in the [module
/// documentation](self).
///
/// The list is DER, which is what reqwest's `tls_certs_only` takes, and rustls
/// parses the same list for a preconfigured [`client_config`], so both client
/// styles verify against byte-identical anchors.
pub fn root_certs() -> &'static [CertificateDer<'static>] {
    &ROOT_CERTS
}

static ROOT_CERTS: LazyLock<Vec<CertificateDer<'static>>> = LazyLock::new(|| {
    let roots = root_certs_from(process_env);
    tracing::info!(
        bundled = webpki_root_certs::TLS_SERVER_ROOT_CERTS.len(),
        total = roots.len(),
        "loaded the process-wide TLS root certificates"
    );
    roots
});

fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Resolve the whole set: bundled Mozilla anchors, then the OS store (bounded
/// by [`NATIVE_ROOTS_DEADLINE`]), then the environment CA bundles. `env` is a
/// parameter so tests can drive the bundle path without mutating
/// process-global environment.
fn root_certs_from(env: impl Fn(&str) -> Option<String>) -> Vec<CertificateDer<'static>> {
    let mut roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS.to_vec();
    // A store that never answers costs at most the deadline; see
    // [`load_native_root_certs`].
    roots.extend(load_native_root_certs(
        NATIVE_ROOTS_DEADLINE,
        rustls_native_certs::load_native_certs,
    ));
    roots.extend(ca_certificates(env));
    roots
}

/// Absorb the OS certificate store, running the read on a worker thread and
/// waiting at most `deadline`: the store is served by a daemon that can hang
/// or wait on a console prompt, and whoever builds the first client must not
/// wait on it longer than that. On timeout, spawn failure, or a panicking
/// loader we warn once and continue with the other roots; the worker thread is
/// detached and leaks until the daemon answers, which is the price of not
/// blocking on it.
///
/// `load` is a parameter so tests can hand in a store that never answers.
fn load_native_root_certs(
    deadline: Duration,
    load: impl FnOnce() -> rustls_native_certs::CertificateResult + Send + 'static,
) -> Vec<CertificateDer<'static>> {
    // The loader hands back certificates rather than a `Result`, so the
    // guard's operation result is wrapped in an `Infallible` error.
    let guard = DeadlineGuard::new(deadline, "native-root-certs", OS_STORE_UNRESPONSIVE);
    let loaded = match guard.run(move || Ok::<_, Infallible>(load())) {
        Ok(loaded) => loaded,
        Err(error) => {
            tracing::warn!(%error, timeout_secs = deadline.as_secs(),
                "OS certificate store did not answer; continuing without its certificates");
            return Vec::new();
        }
    };
    let mut usable = rustls::RootCertStore::empty();
    let mut roots = Vec::new();
    for cert in loaded.certs {
        // Skip whatever the WebPKI anchor parser rejects: one unusable
        // keychain item must not fail every client built from this list.
        if let Err(error) = usable.add(cert.clone()) {
            tracing::warn!(%error, "ignoring unusable certificate from the OS store");
            continue;
        }
        roots.push(cert);
    }
    for error in &loaded.errors {
        tracing::warn!(%error, "reading the OS certificate store");
    }
    tracing::info!(count = roots.len(), "loaded OS root certificates");
    roots
}

/// Extra PEM bundles from `PRELOOP_GITHUB_CA_FILE` — a CA that should
/// additionally be trusted when talking to a GHES / GitHub-emulator forge
/// (gh-simulate serves a self-signed CA) — and from `SSL_CERT_FILE`, the
/// OpenSSL/native convention the runner honors in `client::http::with_control`,
/// kept here so the engine's GitHub clients follow the same environment
/// override regardless of reqwest backend.
///
/// Both are optional and additive to the bundled Mozilla roots and the OS
/// store, all unioned once per process in [`root_certs`]. A missing or
/// unreadable file fails closed (the client simply has no extra roots) — we
/// log rather than abort startup, since TLS verification itself stays on.
fn ca_certificates(env: impl Fn(&str) -> Option<String>) -> Vec<CertificateDer<'static>> {
    let mut out = Vec::new();
    for var in ["PRELOOP_GITHUB_CA_FILE", "SSL_CERT_FILE"] {
        let Some(path) = env(var) else {
            continue;
        };
        if path.trim().is_empty() {
            continue;
        }
        out.extend(load_ca_bundle(std::path::Path::new(&path), var));
    }
    out
}

/// Read one PEM bundle from `path` and parse its certificates. Missing or
/// invalid files log and return empty — verification stays on, we just add no
/// roots. Split out so tests exercise it without mutating process env.
fn load_ca_bundle(path: &std::path::Path, env: &str) -> Vec<CertificateDer<'static>> {
    match std::fs::read(path) {
        Ok(pem) => match CertificateDer::pem_slice_iter(&pem).collect::<Result<Vec<_>, _>>() {
            Ok(certs) => {
                tracing::info!(path = %path.display(), env, count = certs.len(),
                    "loaded additional TLS root certificates");
                certs
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), env, error = %e,
                    "ignoring CA bundle with invalid PEM");
                Vec::new()
            }
        },
        Err(e) => {
            tracing::warn!(path = %path.display(), env, error = %e,
                "ignoring unreadable CA bundle");
            Vec::new()
        }
    }
}

/// The root set as a rustls store, for clients that install a whole
/// [`rustls::ClientConfig`] instead of reqwest's own rustls config.
fn store() -> rustls::RootCertStore {
    let mut store = rustls::RootCertStore::empty();
    let (added, ignored) = store.add_parsable_certificates(root_certs().iter().cloned());
    if ignored > 0 {
        tracing::warn!(ignored, "ignoring unusable TLS root certificate");
    }
    tracing::debug!(added, "parsed the shared TLS root set");
    store
}

/// The root set as reqwest `Certificate`s, skipping anything reqwest rejects
/// so one bad keychain item cannot fail every client.
fn reqwest_certificates(roots: &[CertificateDer<'static>]) -> Vec<reqwest::Certificate> {
    roots
        .iter()
        .filter_map(|der| match reqwest::Certificate::from_der(der.as_ref()) {
            Ok(cert) => Some(cert),
            Err(error) => {
                tracing::warn!(%error, "ignoring unreadable TLS root certificate");
                None
            }
        })
        .collect()
}

/// Resolve the process-wide root set now, on the calling thread, and report
/// how many certificates it holds.
///
/// Hosts call this from a blocking thread during startup so the OS-store read
/// (bounded, but up to [`NATIVE_ROOTS_DEADLINE`] against a hung daemon) never
/// lands on an async worker that happens to build the first client.
pub fn preload_roots() -> usize {
    root_certs().len()
}

/// Install the process-wide root set on an async `builder` via
/// `tls_certs_only`, so the client verifies against that fixed list instead of
/// delegating the handshake to the OS platform verifier. Use this for every
/// outbound HTTPS client.
pub fn with_roots(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    install_roots(builder, root_certs())
}

/// Install an explicit root list, so tests can drive the installed set without
/// mutating process-global environment.
fn install_roots(
    builder: reqwest::ClientBuilder,
    roots: &[CertificateDer<'static>],
) -> reqwest::ClientBuilder {
    builder.tls_certs_only(reqwest_certificates(roots))
}

/// Blocking equivalent of [`with_roots`], for the OTLP exporters (which run on
/// plain SDK worker threads and use a blocking reqwest client) and any other
/// blocking client that must trust the same anchors. Pass it to
/// `ObservabilityConfig::with_otel_client_builder`.
pub fn with_roots_blocking(
    builder: reqwest::blocking::ClientBuilder,
) -> reqwest::blocking::ClientBuilder {
    install_roots_blocking(builder, root_certs())
}

fn install_roots_blocking(
    builder: reqwest::blocking::ClientBuilder,
    roots: &[CertificateDer<'static>],
) -> reqwest::blocking::ClientBuilder {
    builder.tls_certs_only(reqwest_certificates(roots))
}

/// A rustls client config verifying against the shared root set plus
/// `extra_ca_pem`, ready for `reqwest::ClientBuilder::use_preconfigured_tls`.
///
/// `extra_ca_pem` is an already-read CA bundle (the runner's `--ca-bundle`
/// path or its `SSL_CERT_FILE` fallback), which has to stay honored now that
/// the platform verifier is out of the picture.
pub fn client_config(extra_ca_pem: Option<&[u8]>) -> anyhow::Result<rustls::ClientConfig> {
    let mut roots = store();
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
    .context("building the TLS client config")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    // The clients that install this config speak HTTP/1.1 only (their reqwest
    // has no `http2` feature), so advertise exactly that: an empty ALPN list
    // would make some proxies and GHES front ends negotiate nothing and hang.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Blocking-client twin of [`client_config`], for a blocking reqwest client
/// that must stay off the platform verifier (the OTLP exporters).
///
/// Telemetry is fail-open: if the config cannot be built the builder is left
/// on reqwest's platform verifier and the failure is logged, rather than
/// dropping the observability handle.
pub fn with_client_config_blocking(
    builder: reqwest::blocking::ClientBuilder,
) -> reqwest::blocking::ClientBuilder {
    match client_config(None) {
        Ok(config) => builder.use_preconfigured_tls(config),
        Err(error) => {
            tracing::warn!(%error,
                "could not install the shared TLS roots on the client; \
                 falling back to the platform verifier");
            builder
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throwaway self-signed certificate written as PEM, generated
    /// in-process so the tests need neither the `openssl` CLI nor a network.
    fn write_test_cert_pem(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        let cert = rcgen::generate_simple_self_signed(vec!["gh-simulate-test".to_owned()])
            .expect("generate a test cert");
        std::fs::create_dir_all(dir).expect("create the test certificate directory");
        let path = dir.join(name);
        std::fs::write(&path, cert.cert.pem()).expect("write the test certificate");
        path
    }

    #[test]
    fn parses_pem_bundle() {
        let dir = std::env::temp_dir().join(format!("preloop-tls-pem-{}", std::process::id()));
        let path = write_test_cert_pem(&dir, "ghsim_ca_file.pem");
        let certs = load_ca_bundle(&path, "TEST");
        assert_eq!(certs.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_ca_file_returns_empty_not_panic() {
        let certs = load_ca_bundle(std::path::Path::new("/nonexistent/ca.pem"), "TEST");
        assert!(certs.is_empty());
    }

    #[test]
    fn invalid_pem_returns_empty_not_panic() {
        let path = std::env::temp_dir().join(format!("preloop-tls-bad-{}.pem", std::process::id()));
        std::fs::write(&path, b"not a pem").unwrap();
        assert!(load_ca_bundle(&path, "TEST").is_empty());
        let _ = std::fs::remove_file(&path);
    }

    /// A bundle named by `PRELOOP_GITHUB_CA_FILE` is parsed into the root set;
    /// a missing file adds nothing instead of failing every client.
    #[test]
    fn env_ca_bundle_is_loaded_and_missing_files_are_ignored() {
        let dir = std::env::temp_dir().join(format!("preloop-tls-env-{}", std::process::id()));
        let bundle = write_test_cert_pem(&dir, "env-ca.pem")
            .to_string_lossy()
            .into_owned();
        let lookup = move |name: &str| (name == "PRELOOP_GITHUB_CA_FILE").then(|| bundle.clone());
        assert_eq!(ca_certificates(lookup).len(), 1);
        let missing = |name: &str| {
            (name == "PRELOOP_GITHUB_CA_FILE").then(|| "/nonexistent/ca.pem".to_owned())
        };
        assert!(ca_certificates(missing).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A store that never answers (the hung-trust-daemon incident) must cost
    /// the deadline at most, never the root-set caller's blocking future.
    #[test]
    fn os_root_store_that_never_answers_is_abandoned_at_the_deadline() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        // Unblock after a generous margin so a regression fails the elapsed
        // assertion below instead of hanging the test binary.
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

    /// Behavioural check of the whole mechanism: the blocking client built
    /// from this root set completes a TLS handshake against a server whose CA
    /// arrives through the same `PRELOOP_GITHUB_CA_FILE` path the engine uses,
    /// and the same client refuses that server without the bundle. No OS trust
    /// store, no real network: a throwaway CA generated in-process plus a
    /// loopback TLS server.
    #[test]
    fn blocking_client_trusts_the_shared_roots_including_the_env_ca_bundle() {
        let dir = std::env::temp_dir().join(format!("preloop-tls-roots-{}", std::process::id()));
        let fixture = make_test_ca(&dir);
        let port = spawn_tls_server(&fixture).expect("start the loopback TLS server");
        let url = format!("https://localhost:{port}/");

        let without_bundle = install_roots_blocking(
            reqwest::blocking::Client::builder().timeout(Duration::from_secs(5)),
            webpki_root_certs::TLS_SERVER_ROOT_CERTS,
        )
        .build()
        .unwrap();
        assert!(
            without_bundle.get(&url).send().is_err(),
            "the test CA must not be trusted without PRELOOP_GITHUB_CA_FILE"
        );

        // Exactly the set `with_roots_blocking` installs, with the CA
        // arriving through the same environment override the engine honours.
        let bundle_path = fixture.ca_pem.to_string_lossy().into_owned();
        let lookup =
            move |name: &str| (name == "PRELOOP_GITHUB_CA_FILE").then(|| bundle_path.clone());
        let mut roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS.to_vec();
        roots.extend(ca_certificates(lookup));
        let with_bundle = install_roots_blocking(
            reqwest::blocking::Client::builder().timeout(Duration::from_secs(5)),
            &roots,
        )
        .build()
        .unwrap();
        let response = with_bundle
            .get(&url)
            .send()
            .expect("the CA named by PRELOOP_GITHUB_CA_FILE must be trusted");
        assert_eq!(response.status(), 200);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Behavioural check: the config this module builds completes a TLS
    /// handshake against a server whose certificate chains to the
    /// `PRELOOP_GITHUB_CA_FILE` bundle, and the same config without that
    /// bundle does not. This is the regression `use_preconfigured_tls` could
    /// introduce (dropping the runner's CA bundle would leave a private forge
    /// permanently unreachable), and it exercises the merged root set rather
    /// than asserting builder wiring. Loopback only.
    #[test]
    fn client_config_trusts_the_ca_bundle_over_tls() {
        let dir =
            std::env::temp_dir().join(format!("preloop-tls-handshake-{}", std::process::id()));
        let fixture = make_test_ca(&dir);
        let port = spawn_tls_server(&fixture).expect("start the loopback TLS server");
        let pem = std::fs::read(&fixture.ca_pem).expect("read the test CA bundle");

        let without_bundle = client_config(None).expect("build the client config");
        assert!(
            handshake(without_bundle, port).is_err(),
            "the test CA must not be trusted without the CA bundle"
        );

        let with_bundle = client_config(Some(&pem)).expect("build the config with the CA bundle");
        handshake(with_bundle, port).expect("the CA bundle must be trusted");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Run one HTTPS request through a rustls client config.
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

    struct TestCa {
        /// PEM file the client is pointed at through `PRELOOP_GITHUB_CA_FILE`.
        ca_pem: std::path::PathBuf,
        /// `leaf, CA` chain the server presents.
        chain: Vec<CertificateDer<'static>>,
        key: rustls::pki_types::PrivateKeyDer<'static>,
    }

    /// A CA and a `localhost` leaf signed by it, generated in-process so the
    /// test needs neither the `openssl` CLI nor a real network.
    fn make_test_ca(dir: &std::path::Path) -> TestCa {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};

        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "preloop tls-roots test CA");
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-signed CA");
        let issuer = Issuer::from_params(&ca_params, &ca_key);

        let leaf_key = KeyPair::generate().expect("generate leaf key");
        let leaf_params =
            CertificateParams::new(vec!["localhost".to_owned()]).expect("leaf params");
        let leaf_cert = leaf_params
            .signed_by(&leaf_key, &issuer)
            .expect("CA-signed leaf");

        let ca_pem = dir.join("ca.pem");
        std::fs::create_dir_all(dir).expect("create test CA dir");
        std::fs::write(&ca_pem, ca_cert.pem()).expect("write CA PEM");
        TestCa {
            ca_pem,
            chain: vec![leaf_cert.der().clone(), ca_cert.der().clone()],
            key: rustls::pki_types::PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
        }
    }

    /// Serve one HTTP response over TLS on a loopback port for as long as the
    /// test binary lives.
    fn spawn_tls_server(fixture: &TestCa) -> std::io::Result<u16> {
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(std::io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(fixture.chain.clone(), fixture.key.clone_key())
        .map_err(std::io::Error::other)?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let Ok(connection) =
                    rustls::ServerConnection::new(std::sync::Arc::new(config.clone()))
                else {
                    continue;
                };
                let mut tls = rustls::StreamOwned::new(connection, stream);
                let mut request = [0u8; 1024];
                // A refused handshake (the client not trusting our CA)
                // surfaces here; only a complete request gets a response.
                if std::io::Read::read(&mut tls, &mut request).is_err() {
                    continue;
                }
                let _ = std::io::Write::write_all(
                    &mut tls,
                    b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
                );
            }
        });
        Ok(port)
    }
}
