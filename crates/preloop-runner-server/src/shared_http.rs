use std::sync::LazyLock;
use std::time::Duration;

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;

/// How long [`load_native_root_certs`] waits for the OS certificate store.
const NATIVE_ROOTS_DEADLINE: Duration = Duration::from_secs(5);

/// Every root certificate the engine trusts for outbound TLS, resolved once
/// per process:
///
///   1. the bundled Mozilla set (`webpki-root-certs`), which needs no OS call;
///   2. the certificates installed in the OS store (macOS Keychain, Linux
///      `/etc/ssl/certs`, Windows certificate store), read with a deadline;
///   3. the extra `PRELOOP_GITHUB_CA_FILE` / `SSL_CERT_FILE` bundles.
///
/// Clients install this list with `tls_certs_only` (see [`with_tls_roots`]),
/// which keeps reqwest off the platform verifier: on macOS that verifier's
/// `SecTrustEvaluateWithError` runs synchronously inside the handshake and
/// asks `securityd`/`trustd`, so a hung or prompting daemon can stall the
/// process for as long as it likes. Verification stays on — rustls still
/// checks the chain and the hostname in-process against exactly these
/// anchors — but the trust data is fetched once, with a bound.
pub fn tls_roots() -> &'static [CertificateDer<'static>] {
    &ROOT_CERTS
}

static ROOT_CERTS: LazyLock<Vec<CertificateDer<'static>>> = LazyLock::new(build_root_certs);

fn build_root_certs() -> Vec<CertificateDer<'static>> {
    let mut roots = webpki_root_certs::TLS_SERVER_ROOT_CERTS.to_vec();
    // A store that never answers costs at most the deadline; see
    // [`load_native_root_certs`].
    roots.extend(load_native_root_certs(
        NATIVE_ROOTS_DEADLINE,
        rustls_native_certs::load_native_certs,
    ));
    roots.extend(ca_certificates());
    roots
}

/// Absorb the OS certificate store, running the read on a thread of its own
/// and waiting at most `deadline`: the store is served by a daemon that can
/// hang or wait on a console prompt, and whoever builds the first client must
/// not wait on it longer than that. On timeout or lookup error we warn once
/// and continue with the other roots; the loader thread is detached and leaks
/// until the daemon answers, which is the price of not blocking on it.
///
/// `load` is a parameter so tests can hand in a store that never answers.
fn load_native_root_certs(
    deadline: Duration,
    load: impl FnOnce() -> rustls_native_certs::CertificateResult + Send + 'static,
) -> Vec<CertificateDer<'static>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let spawned = std::thread::Builder::new()
        .name("native-root-certs".to_owned())
        .spawn(move || {
            let _ = sender.send(load());
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "could not spawn the OS root certificate loader thread");
        return Vec::new();
    }
    let loaded = match receiver.recv_timeout(deadline) {
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
/// OpenSSL/native convention the runner already honors in
/// `client::http::with_control`, kept here so the engine's GitHub clients
/// follow the same environment override regardless of reqwest backend.
///
/// Both are optional and additive to the bundled Mozilla roots and the OS
/// store, all unioned once per process in [`tls_roots`]. A missing or
/// unreadable file fails closed (the client simply has no extra roots) — we
/// log rather than abort startup, since TLS verification itself stays on.
fn ca_certificates() -> Vec<CertificateDer<'static>> {
    let mut out = Vec::new();
    for var in ["PRELOOP_GITHUB_CA_FILE", "SSL_CERT_FILE"] {
        let Ok(path) = std::env::var(var) else {
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
                    "loaded additional TLS root certificates for outbound GitHub calls");
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

/// Install the process-wide root set from [`tls_roots`] on `builder` via
/// `tls_certs_only`, so the client verifies against that fixed list instead
/// of delegating the handshake to the OS platform verifier. Use this for
/// every outbound HTTPS client.
pub fn with_tls_roots(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    let certs = tls_roots()
        .iter()
        .filter_map(|der| match reqwest::Certificate::from_der(der.as_ref()) {
            Ok(cert) => Some(cert),
            Err(error) => {
                tracing::warn!(%error, "ignoring unreadable TLS root certificate");
                None
            }
        })
        .collect::<Vec<_>>();
    builder.tls_certs_only(certs)
}

/// Build a reqwest client with the shared timeouts and the root set from
/// [`tls_roots`]. Use this for every server-side outbound HTTPS call that can
/// target a GHES/emulator forge instead of ad-hoc `reqwest::Client::builder()`
/// so custom roots apply uniformly.
pub fn github_client_builder() -> reqwest::ClientBuilder {
    with_tls_roots(
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .user_agent("preloop-runner-server"),
    )
}

/// Shared HTTP client for server-side outbound requests (GitHub API, scheduler).
/// Configured with standard timeouts, user-agent, and the TLS roots from
/// [`tls_roots`] so `PRELOOP_GITHUB_CA_FILE`/`SSL_CERT_FILE` reach every
/// caller.
pub static CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    github_client_builder()
        .build()
        .expect("HTTP client builder")
});

#[cfg(test)]
mod tests {
    use super::*;

    fn write_self_signed_pem(path: &std::path::Path) {
        let key = std::env::temp_dir().join("ghsim_ca_test_key.pem");
        let _ = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                key.to_str().unwrap(),
                "-out",
                path.to_str().unwrap(),
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=gh-simulate-test",
            ])
            .output();
    }

    #[test]
    fn parses_pem_bundle() {
        let p = std::env::temp_dir().join("ghsim_ca_file.pem");
        write_self_signed_pem(&p);
        let certs = load_ca_bundle(&p, "TEST");
        assert!(!certs.is_empty());
    }

    #[test]
    fn missing_ca_file_returns_empty_not_panic() {
        let certs = load_ca_bundle(std::path::Path::new("/nonexistent/ca.pem"), "TEST");
        assert!(certs.is_empty());
    }

    #[test]
    fn invalid_pem_returns_empty_not_panic() {
        let p = std::env::temp_dir().join("ghsim_bad.pem");
        std::fs::write(&p, b"not a pem").unwrap();
        assert!(load_ca_bundle(&p, "TEST").is_empty());
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
}
