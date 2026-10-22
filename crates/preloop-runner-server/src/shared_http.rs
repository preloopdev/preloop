//! Shared HTTP client pieces for the engine's outbound calls.
//!
//! The TLS root policy lives in [`preloop_observability::tls`], which both this
//! crate and `preloop-runner` depend on: one loader, one root set, one OS-store
//! deadline. The names kept here are the engine-facing ones, so the existing
//! call sites (`with_tls_roots(...)`, `CLIENT`, ...) read as before.

use std::time::Duration;

use rustls::pki_types::CertificateDer;

/// Every root certificate the engine trusts for outbound TLS, resolved once
/// per process: the bundled Mozilla set, the OS certificate store read with a
/// deadline, and the `PRELOOP_GITHUB_CA_FILE` / `SSL_CERT_FILE` bundles.
///
/// See [`preloop_observability::tls::root_certs`] for the whole policy; this is
/// a thin name for it.
pub fn tls_roots() -> &'static [CertificateDer<'static>] {
    preloop_observability::tls::root_certs()
}

/// Install the process-wide root set from [`tls_roots`] on an async `builder`
/// via `tls_certs_only`, so the client verifies against that fixed list
/// instead of delegating the handshake to the OS platform verifier. Use this
/// for every outbound HTTPS client.
pub fn with_tls_roots(builder: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    preloop_observability::tls::with_roots(builder)
}

/// Blocking equivalent of [`with_tls_roots`], for the OTLP exporters (which
/// run on plain SDK worker threads and use a blocking reqwest client) and any
/// other blocking client that must trust the same anchors. Pass it to
/// `preloop_observability::ObservabilityConfig::with_otel_client_builder`.
pub fn with_tls_roots_blocking(
    builder: reqwest::blocking::ClientBuilder,
) -> reqwest::blocking::ClientBuilder {
    preloop_observability::tls::with_roots_blocking(builder)
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

    /// These names are the shared loader, not a second copy of it: a change
    /// that reintroduced a local root set would fail here.
    #[test]
    fn tls_roots_is_the_shared_root_set() {
        assert!(std::ptr::eq(
            tls_roots(),
            preloop_observability::tls::root_certs()
        ));
    }
}
