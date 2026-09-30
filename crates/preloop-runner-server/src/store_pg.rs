//! Postgres connection helpers shared by the control backend and the
//! webhook watchdog's health probe: TLS connector construction and the
//! libpq→tokio-postgres `sslmode` rewrite. The `PgStore` snapshot backend is
//! gone — `control/pg` owns all scheduling state now.
use postgres_rustls::MakeTlsConnector;
use std::sync::Arc;

/// Build a rustls connector when the URL asks for TLS (`sslmode=require`,
/// `verify-ca`, or `verify-full`); `None` for `sslmode=disable` or when no
/// `sslmode` is present. Chain + hostname verification always use the system
/// root store, so `verify-full` semantics apply to every TLS mode.
pub fn tls_connector(url: &str) -> anyhow::Result<Option<MakeTlsConnector>> {
    let query = url.split('?').nth(1).unwrap_or("");
    let sslmode = query
        .split('&')
        .find_map(|part| part.strip_prefix("sslmode="));
    match sslmode {
        Some("require" | "verify-ca" | "verify-full") => {
            // rustls 0.23 needs a process-level crypto provider. The server
            // binaries install one at startup; installing here makes the
            // library self-sufficient (tests, embedded use) without breaking
            // an already-installed provider.
            rustls::crypto::ring::default_provider()
                .install_default()
                .ok();
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots
                    .add(cert)
                    .map_err(|error| anyhow::anyhow!("adding native root cert: {error}"))?;
            }
            let mut config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            postgres_rustls::set_postgresql_alpn(&mut config);
            Ok(Some(MakeTlsConnector::new(
                tokio_rustls::TlsConnector::from(Arc::new(config)),
            )))
        }
        _ => Ok(None),
    }
}

/// URL to hand to `tokio-postgres::connect`. `sslmode` values `verify-ca` /
/// `verify-full` are libpq extensions that tokio-postgres rejects; the
/// rustls connector always performs full chain + hostname verification, so
/// mapping them onto `require` (TLS stays mandatory) loses nothing.
pub fn connect_url(url: &str) -> String {
    let (before, after) = match url.split_once("sslmode=verify-") {
        Some((before, after)) => (before, after),
        None => return url.to_owned(),
    };
    let (mode, rest) = match after.split_once('&') {
        Some((mode, rest)) => (mode, rest),
        None => (after, ""),
    };
    if !matches!(mode, "ca" | "full") {
        return url.to_owned();
    }
    let rest = if rest.is_empty() {
        ""
    } else {
        &format!("&{rest}")
    };
    format!("{before}sslmode=require{rest}")
}
