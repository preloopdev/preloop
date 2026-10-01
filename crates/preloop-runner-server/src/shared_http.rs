use std::time::Duration;

/// Root certificates loaded for server-side outbound HTTPS.
///
/// Sources (in order):
///   1. `PRELOOP_GITHUB_CA_FILE` — a PEM bundle that should additionally be
///      trusted when talking to a GHES / GitHub-emulator forge (gh-simulate
///      serves a self-signed CA). Forge-scoped, additive to the system roots.
///   2. `SSL_CERT_FILE` — the OpenSSL/native convention the runner already
///      honors in `client::http::with_control`; kept here so the engine's
///      GitHub clients follow the same environment override on platforms
///      whose reqwest backend does not read it.
///
/// Both are optional and additive to the bundled webpki/native roots. A
/// missing or unreadable file fails closed (the client simply has the default
/// roots) — we log rather than abort startup, since TLS verification itself
/// stays on.
fn ca_certificates() -> Vec<reqwest::Certificate> {
    let mut out = Vec::new();
    for var in ["PRELOOP_GITHUB_CA_FILE", "SSL_CERT_FILE"] {
        let Ok(path) = std::env::var(var) else { continue };
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
fn load_ca_bundle(path: &std::path::Path, env: &str) -> Vec<reqwest::Certificate> {
    match std::fs::read(path) {
        Ok(pem) => match reqwest::Certificate::from_pem_bundle(&pem) {
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

/// Build a reqwest client with the shared timeouts and the extra CA roots
/// from [`ca_certificates`]. Use this for every server-side outbound HTTPS
/// call that can target a GHES/emulator forge instead of ad-hoc
/// `reqwest::Client::builder()` so custom roots apply uniformly.
pub fn github_client_builder() -> reqwest::ClientBuilder {
    let mut b = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .user_agent("preloop-runner-server");
    for cert in ca_certificates() {
        b = b.add_root_certificate(cert);
    }
    b
}

/// Shared HTTP client for server-side outbound requests (GitHub API, scheduler).
/// Configured with standard timeouts, user-agent, and the CA roots from
/// [`ca_certificates`] so `PRELOOP_GITHUB_CA_FILE`/`SSL_CERT_FILE` reach every
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
        let _ = std::process::Command::new("openssl").args([
            "req","-x509","-newkey","rsa:2048","-keyout",
            key.to_str().unwrap(),"-out",path.to_str().unwrap(),
            "-days","1","-nodes","-subj","/CN=gh-simulate-test",
        ]).output();
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
}
