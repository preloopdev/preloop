//! Postgres connection helpers shared by the control backend and the
//! webhook watchdog's health probe: TLS connector construction and the
//! libpq→tokio-postgres `sslmode` rewrite. The `PgStore` snapshot backend is
//! gone — `control/pg` owns all scheduling state now.
use postgres_rustls::MakeTlsConnector;
use std::sync::{Arc, LazyLock};

/// rustls config for every Postgres connection, built once per process from
/// the shared root set ([`crate::shared_http::tls_roots`]) so connecting
/// never reads the OS trust store on the runtime thread.
static TLS_CONFIG: LazyLock<Result<Arc<rustls::ClientConfig>, String>> =
    LazyLock::new(build_tls_config);

fn build_tls_config() -> Result<Arc<rustls::ClientConfig>, String> {
    // rustls 0.23 needs a process-level crypto provider. The server
    // binaries install one at startup; installing here makes the library
    // self-sufficient (tests, embedded use) without breaking an
    // already-installed provider. The config itself is built with an explicit
    // provider because several are compiled in (ring via this crate's
    // rustls feature, aws-lc-rs via reqwest), which makes the implicit
    // `ClientConfig::builder()` ambiguous.
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut roots = rustls::RootCertStore::empty();
    for cert in crate::shared_http::tls_roots() {
        roots
            .add(cert.clone())
            .map_err(|error| format!("adding TLS root cert: {error}"))?;
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| format!("TLS protocol versions: {error}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    postgres_rustls::set_postgresql_alpn(&mut config);
    Ok(Arc::new(config))
}

/// Build a rustls connector when the URL asks for TLS (`sslmode=require`,
/// `verify-ca`, or `verify-full`); `None` for `sslmode=disable` or when no
/// `sslmode` is present. Chain + hostname verification always use the shared
/// in-process root set, so `verify-full` semantics apply to every TLS mode.
pub fn tls_connector(url: &str) -> anyhow::Result<Option<MakeTlsConnector>> {
    let query = url.split('?').nth(1).unwrap_or("");
    let sslmode = query
        .split('&')
        .find_map(|part| part.strip_prefix("sslmode="));
    match sslmode {
        Some("require" | "verify-ca" | "verify-full") => {
            let config = TLS_CONFIG
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(Some(MakeTlsConnector::new(
                tokio_rustls::TlsConnector::from(config.clone()),
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
