//! TLS for the Sandbox Service: the service CA, its server certificate,
//! provider enrollment, join tokens and agent identities.
//!
//! One CA per service lives in `<state>/sandbox-ca/` (`ca.pem`, `ca.key`,
//! mode 0600) and is reused across restarts. It signs the server certificate
//! (SANs from `PRELOOP_SANDBOX_TLS_SANS`) and every provider's client
//! certificate (issued from a CSR at enrollment, 24h validity, renewed by the
//! agent before it expires). Providers authenticate with that certificate:
//! the service looks the leaf's SHA-256 fingerprint up in its store, so no
//! X.509 parsing is needed past the TLS handshake.
//!
//! Join tokens (`plj1.<id>.<secret>.<ca_sha256>`) are single use and bound to
//! this CA: the agent verifies that the CA it fetches hashes to the token's
//! pin before it sends a CSR.

use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::connect_info::Connected;
use axum::serve::{IncomingStream, Listener};
use base64::Engine as _;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use crate::connect::{Code, Error, Result};

/// Directory holding the service CA, under the service state directory.
pub const CA_DIR: &str = "sandbox-ca";
/// CA certificate file name.
pub const CA_CERT_FILE: &str = "ca.pem";
/// CA private key file name.
pub const CA_KEY_FILE: &str = "ca.key";
/// Join token prefix.
pub const JOIN_TOKEN_PREFIX: &str = "plj1";

/// How long an issued provider certificate is valid.
pub const CLIENT_CERT_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

/// Server certificates are re-issued on every start, so the lifetime only
/// bounds the gap between a start and its clients.
const SERVER_CERT_LIFETIME: Duration = Duration::from_secs(365 * 24 * 60 * 60);

fn tls_error(error: impl std::fmt::Display) -> Error {
    Error::internal(format!("sandbox tls: {error}"))
}

/// Lowercase hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// `count` random bytes, lowercase hex.
pub fn random_hex(count: usize) -> String {
    let mut bytes = vec![0u8; count];
    rand::fill(&mut bytes[..]);
    hex::encode(bytes)
}

/// Write a secret with mode 0600, creating its directory.
pub fn write_private(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(tls_error)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(tls_error)?;
    // `mode` only applies on creation; chmod in case the file pre-existed
    // with wider permissions.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(tls_error)?;
    }
    file.write_all(contents).map_err(tls_error)?;
    file.flush().map_err(tls_error)
}

/// Write non-secret material (a CA certificate), creating its directory.
fn write_public(path: &Path, contents: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(tls_error)?;
    }
    std::fs::write(path, contents).map_err(tls_error)
}

fn read_text(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| tls_error(format!("{}: {e}", path.display())))
}

/// All DER blocks with `label` in a PEM document.
pub fn pem_blocks(pem: &str, label: &str) -> Vec<Vec<u8>> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut blocks = Vec::new();
    let mut rest = pem;
    while let Some(start) = rest.find(&begin) {
        let after = &rest[start + begin.len()..];
        let Some(stop) = after.find(&end) else {
            break;
        };
        let body: String = after[..stop]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        if let Ok(der) = base64::engine::general_purpose::STANDARD.decode(body) {
            blocks.push(der);
        }
        rest = &after[stop + end.len()..];
    }
    blocks
}

/// Certificates in a PEM document.
pub fn pem_certificates(pem: &str) -> Vec<CertificateDer<'static>> {
    pem_blocks(pem, "CERTIFICATE")
        .into_iter()
        .map(CertificateDer::from)
        .collect()
}

/// The private key in a PEM document (PKCS#8, PKCS#1 or SEC1).
pub fn pem_private_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    if let Some(der) = pem_blocks(pem, "PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(der.into()));
    }
    if let Some(der) = pem_blocks(pem, "RSA PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs1(der.into()));
    }
    if let Some(der) = pem_blocks(pem, "EC PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Sec1(der.into()));
    }
    Err(Error::invalid("no private key in PEM document"))
}

/// SHA-256 of the first certificate in a PEM document.
pub fn pem_fingerprint(pem: &str) -> Result<String> {
    pem_certificates(pem)
        .first()
        .map(|der| sha256_hex(der))
        .ok_or_else(|| Error::invalid("no certificate in PEM document"))
}

/// Whether `value` is a plausible DNS name for a SAN.
fn is_dns_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'.')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

/// The SAN list used when none is configured: this host's name, localhost and
/// the loopback addresses.
pub fn default_sans() -> Vec<String> {
    let mut sans = Vec::new();
    if let Ok(host) = hostname::get()
        && let Ok(host) = host.into_string()
        && is_dns_name(&host)
    {
        sans.push(host);
    }
    sans.push("localhost".to_owned());
    sans.push("127.0.0.1".to_owned());
    sans.push("::1".to_owned());
    sans
}

/// SANs from `PRELOOP_SANDBOX_TLS_SANS` (comma list), or the defaults.
pub fn sans_from_env() -> Vec<String> {
    let Some(value) = std::env::var("PRELOOP_SANDBOX_TLS_SANS").ok() else {
        return default_sans();
    };
    let sans: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|san| !san.is_empty())
        .map(str::to_owned)
        .collect();
    if sans.is_empty() {
        default_sans()
    } else {
        sans
    }
}

/// The service's certificate authority.
pub struct ServiceCa {
    cert_pem: String,
    cert_der: CertificateDer<'static>,
    sha256_hex: String,
    issuer: Issuer<'static, KeyPair>,
}

impl std::fmt::Debug for ServiceCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceCa")
            .field("sha256", &self.sha256_hex)
            .finish()
    }
}

fn ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "Preloop Sandbox CA");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
}

/// A certificate issued by the service CA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCertificate {
    /// PEM-encoded leaf certificate.
    pub cert_pem: String,
    /// Lowercase hex SHA-256 of the leaf DER.
    pub sha256: String,
    /// Expiry, Unix seconds.
    pub expires_at: i64,
}

impl ServiceCa {
    /// Load the CA from `dir`, creating and persisting it on first start.
    pub fn load_or_create(dir: &Path) -> Result<Self> {
        let cert_path = dir.join(CA_CERT_FILE);
        let key_path = dir.join(CA_KEY_FILE);
        match (cert_path.is_file(), key_path.is_file()) {
            (true, true) => {
                let cert_pem = read_text(&cert_path)?;
                let key_pem = read_text(&key_path)?;
                Self::from_pem(&cert_pem, &key_pem)
            }
            (false, false) => {
                let params = ca_params();
                let key = KeyPair::generate().map_err(tls_error)?;
                let cert = params.self_signed(&key).map_err(tls_error)?;
                let cert_pem = cert.pem();
                let cert_der = cert.der().clone();
                let key_pem = key.serialize_pem();
                write_private(&key_path, key_pem.as_bytes())?;
                write_public(&cert_path, cert_pem.as_bytes())?;
                Ok(Self {
                    sha256_hex: sha256_hex(&cert_der),
                    cert_pem,
                    cert_der,
                    issuer: Issuer::new(params, key),
                })
            }
            _ => Err(Error::internal(format!(
                "sandbox CA in {} is incomplete: {CA_CERT_FILE} and {CA_KEY_FILE} must both exist",
                dir.display()
            ))),
        }
    }

    /// Build from an existing PEM certificate and key.
    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<Self> {
        let cert_der = pem_certificates(cert_pem)
            .into_iter()
            .next()
            .ok_or_else(|| Error::invalid("CA certificate PEM holds no certificate"))?;
        let key = KeyPair::from_pem(key_pem).map_err(tls_error)?;
        // Only the issuer's subject and key identity matter when signing, so
        // the parameters are reconstructed deterministically rather than
        // re-parsed from the certificate.
        Ok(Self {
            sha256_hex: sha256_hex(&cert_der),
            cert_pem: cert_pem.to_owned(),
            cert_der,
            issuer: Issuer::new(ca_params(), key),
        })
    }

    /// PEM-encoded CA certificate.
    pub fn pem(&self) -> &str {
        &self.cert_pem
    }

    /// Lowercase hex SHA-256 of the CA DER; the value pinned by join tokens.
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }

    /// DER of the CA certificate.
    pub fn der(&self) -> &CertificateDer<'static> {
        &self.cert_der
    }

    /// Sign a provider's CSR into a 24h client certificate.
    ///
    /// The subject and all extensions are chosen here: only the public key is
    /// taken from the request, so a CSR cannot widen its own permissions.
    pub fn sign_client(
        &self,
        csr_pem: &str,
        name: &str,
        lifetime: Duration,
    ) -> Result<SignedCertificate> {
        let csr = CertificateSigningRequestParams::from_pem(csr_pem)
            .map_err(|e| Error::invalid(format!("bad certificate signing request: {e}")))?;
        let mut params = csr.params;
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.subject_alt_names.clear();
        let issued_at = OffsetDateTime::now_utc();
        params.not_before = issued_at - time::Duration::minutes(5);
        params.not_after = issued_at + time::Duration::seconds(lifetime.as_secs() as i64);
        let cert = params
            .signed_by(&csr.public_key, &self.issuer)
            .map_err(tls_error)?;
        Ok(SignedCertificate {
            cert_pem: cert.pem(),
            sha256: sha256_hex(cert.der()),
            expires_at: params.not_after.unix_timestamp(),
        })
    }

    /// Issue a fresh server certificate for `sans`.
    fn issue_server(
        &self,
        sans: &[String],
    ) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
        let key = KeyPair::generate().map_err(tls_error)?;
        let mut params = CertificateParams::new(sans.to_vec()).map_err(tls_error)?;
        params
            .distinguished_name
            .push(DnType::CommonName, "preloop-sandbox");
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let issued_at = OffsetDateTime::now_utc();
        params.not_before = issued_at - time::Duration::minutes(5);
        params.not_after =
            issued_at + time::Duration::seconds(SERVER_CERT_LIFETIME.as_secs() as i64);
        let cert = params.signed_by(&key, &self.issuer).map_err(tls_error)?;
        Ok((
            vec![cert.der().clone()],
            PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        ))
    }
}

/// Everything the network listener needs: the CA, its public PEM and the
/// rustls acceptor (server certificate plus optional client authentication
/// rooted at the CA).
pub struct ServerTls {
    /// The service CA that signs provider certificates.
    pub ca: Arc<ServiceCa>,
    /// TLS acceptor for inbound connections.
    pub acceptor: TlsAcceptor,
}

impl ServerTls {
    /// Load or create the CA under `state/sandbox-ca`, then configure TLS.
    ///
    /// `cert`/`key` override the server certificate; both must be set
    /// together.
    pub fn load_or_create(
        state: &Path,
        sans: Vec<String>,
        cert: Option<&Path>,
        key: Option<&Path>,
    ) -> Result<Arc<Self>> {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        let ca = Arc::new(ServiceCa::load_or_create(&state.join(CA_DIR))?);
        let (chain, key_der) = match (cert, key) {
            (Some(cert), Some(key)) => {
                let cert_pem = read_text(cert)?;
                let key_pem = read_text(key)?;
                (pem_certificates(&cert_pem), pem_private_key(&key_pem)?)
            }
            (None, None) => ca.issue_server(&sans)?,
            _ => {
                return Err(Error::invalid(
                    "PRELOOP_SANDBOX_TLS_CERT and PRELOOP_SANDBOX_TLS_KEY must be set together",
                ));
            }
        };
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.der().clone())
            .map_err(|e| tls_error(format!("CA certificate: {e}")))?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            // Certificates are optional at the TLS layer: `Enroll` and the
            // data plane use other credentials. The RPC handler is what
            // requires a client certificate for the Providers API.
            .allow_unauthenticated()
            .build()
            .map_err(|e| tls_error(format!("client verifier: {e}")))?;
        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key_der)
            .map_err(|e| tls_error(format!("server certificate: {e}")))?;
        Ok(Arc::new(Self {
            ca,
            acceptor: TlsAcceptor::from(Arc::new(config)),
        }))
    }
}

/// What the TLS handshake learned about the peer.
#[derive(Clone, Debug, Default)]
pub struct PeerIdentity {
    /// Lowercase hex SHA-256 of the peer's leaf certificate, when it sent one.
    pub fingerprint_sha256: Option<String>,
}

impl PeerIdentity {
    /// Build from the peer's certificate chain.
    pub fn from_certificates(certificates: Option<&[CertificateDer<'static>]>) -> Self {
        Self {
            fingerprint_sha256: certificates
                .and_then(|certs| certs.first())
                .map(|der| sha256_hex(der)),
        }
    }

    /// The peer's fingerprint, or `unauthenticated` when it sent no
    /// certificate.
    pub fn fingerprint(&self) -> Result<&str> {
        self.fingerprint_sha256
            .as_deref()
            .ok_or_else(|| Error::new(Code::Unauthenticated, "a client certificate is required"))
    }
}

/// What a TLS listener reports per accepted connection: the local address and
/// the peer identity.
///
/// The local address is what `axum::Serve::local_addr` exposes; handlers use
/// the [`PeerIdentity`] through `ConnectInfo<PeerIdentity>`.
#[derive(Clone, Debug)]
pub struct TlsConnectionInfo {
    /// Address the listener accepted on.
    pub local: SocketAddr,
    /// What the handshake learned about the peer.
    pub peer: PeerIdentity,
}

/// A TCP listener that completes a TLS handshake per connection and reports
/// the peer identity as the connection info.
///
/// axum's `serve` drives it, so graceful shutdown and per-connection tasks are
/// the same as a plain listener. Handshakes run in their own tasks (a peer
/// that stalls mid-handshake must not block other accepts); completed
/// connections are handed to the server over a bounded channel.
pub struct TlsListener {
    connections: tokio::sync::mpsc::Receiver<(
        tokio_rustls::server::TlsStream<TcpStream>,
        TlsConnectionInfo,
    )>,
    local: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl TlsListener {
    /// Wrap a bound TCP listener.
    pub fn new(inner: TcpListener, acceptor: TlsAcceptor) -> Self {
        let local = inner
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0)));
        let (sender, connections) = tokio::sync::mpsc::channel(64);
        let accept = tokio::spawn(async move {
            loop {
                let stream = match inner.accept().await {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        // Per the `Listener` contract: log and retry rather
                        // than ending the accept loop.
                        tracing::debug!(%error, "sandbox API accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let sender = sender.clone();
                tokio::spawn(async move {
                    match acceptor.accept(stream).await {
                        Ok(tls) => {
                            let peer = PeerIdentity::from_certificates(
                                tls.get_ref().1.peer_certificates(),
                            );
                            let info = TlsConnectionInfo { local, peer };
                            // A closed channel means the server is gone.
                            let _ = sender.send((tls, info)).await;
                        }
                        Err(error) => {
                            tracing::debug!(%error, "sandbox API TLS handshake failed");
                        }
                    }
                });
            }
        });
        Self {
            connections,
            local,
            accept,
        }
    }
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

impl Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<TcpStream>;
    type Addr = TlsConnectionInfo;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Some(connection) = self.connections.recv().await {
                return connection;
            }
            // The accept task only ends when this listener is dropped; if the
            // channel closes first, park instead of spinning.
            std::future::pending::<()>().await;
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(TlsConnectionInfo {
            local: self.local,
            peer: PeerIdentity::default(),
        })
    }
}

impl<'a> Connected<IncomingStream<'a, TlsListener>> for PeerIdentity {
    fn connect_info(stream: IncomingStream<'a, TlsListener>) -> Self {
        stream.remote_addr().peer.clone()
    }
}

/// A parsed `plj1.<id>.<secret>.<ca_sha256>` join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinTokenParts<'a> {
    /// Token id (the store key).
    pub id: &'a str,
    /// Secret; only its hash is stored.
    pub secret: &'a str,
    /// Lowercase hex SHA-256 of the CA certificate the issuer pinned.
    pub ca_sha256: &'a str,
}

/// Format a join token.
pub fn join_token(id: &str, secret: &str, ca_sha256: &str) -> String {
    format!("{JOIN_TOKEN_PREFIX}.{id}.{secret}.{ca_sha256}")
}

/// Parse a join token.
pub fn parse_join_token(token: &str) -> Result<JoinTokenParts<'_>> {
    let mut parts = token.trim().split('.');
    let fields = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    );
    let (Some(prefix), Some(id), Some(secret), Some(ca_sha256), None) = fields else {
        return Err(Error::new(Code::Unauthenticated, "malformed join token"));
    };
    let valid_pin = ca_sha256.len() == 64 && ca_sha256.bytes().all(|byte| byte.is_ascii_hexdigit());
    if prefix != JOIN_TOKEN_PREFIX || id.is_empty() || secret.is_empty() || !valid_pin {
        return Err(Error::new(Code::Unauthenticated, "malformed join token"));
    }
    Ok(JoinTokenParts {
        id,
        secret,
        ca_sha256,
    })
}

/// Generate a fresh key and CSR for `name`.
pub fn generate_csr(name: &str) -> Result<(String, String)> {
    let key = KeyPair::generate().map_err(tls_error)?;
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    // The CSR carries only the subject and public key: the service replaces
    // the subject and extensions when it signs, and identity is the leaf
    // fingerprint, so no SAN is needed (and provider names are not DNS names).
    let request = params.serialize_request(&key).map_err(tls_error)?;
    Ok((key.serialize_pem(), request.pem().map_err(tls_error)?))
}

/// A reqwest client trusting only `ca_pem`, optionally presenting a client
/// certificate (`identity_pem` holds the certificate chain and the key).
pub fn http_client(ca_pem: &str, identity_pem: Option<&[u8]>) -> Result<reqwest::Client> {
    let ca = reqwest::Certificate::from_pem(ca_pem.as_bytes())
        .map_err(|e| Error::invalid(format!("CA certificate: {e}")))?;
    // Only this CA is trusted: a public certificate for the same hostname
    // must not be able to stand in for the Sandbox Service.
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .tls_certs_only([ca]);
    if let Some(identity) = identity_pem {
        builder = builder.identity(
            reqwest::Identity::from_pem(identity)
                .map_err(|e| Error::invalid(format!("client identity: {e}")))?,
        );
    }
    builder
        .build()
        .map_err(|e| Error::internal(format!("tls client: {e}")))
}

/// Fetch the service CA over TLS without verifying it.
///
/// The caller must check the returned certificate against the token's pin;
/// that check is what authenticates the service during enrollment.
pub async fn fetch_ca(base: &str) -> Result<String> {
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .tls_danger_accept_invalid_certs(true)
        .build()
        .map_err(|e| Error::internal(format!("tls client: {e}")))?;
    let url = format!("{}/sandbox/v1/ca.pem", base.trim_end_matches('/'));
    let response = http
        .get(&url)
        .send()
        .await
        .map_err(|e| Error::unavailable(format!("fetch CA: {e}")))?;
    if !response.status().is_success() {
        return Err(Error::unavailable(format!(
            "fetch CA: HTTP {}",
            response.status()
        )));
    }
    let pem = response
        .text()
        .await
        .map_err(|e| Error::unavailable(format!("fetch CA: {e}")))?;
    if pem_certificates(&pem).is_empty() {
        return Err(Error::unavailable(
            "fetch CA: response holds no certificate",
        ));
    }
    Ok(pem)
}

const IDENTITY_KEY: &str = "key.pem";
const IDENTITY_CERT: &str = "cert.pem";
const IDENTITY_CA: &str = "ca.pem";
const IDENTITY_META: &str = "identity.json";

/// What an agent remembers about its enrollment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentityMetadata {
    /// Provider name bound to the certificate.
    pub name: String,
    /// Service base URL.
    pub base: String,
    /// SHA-256 of the CA the certificate chains to.
    pub ca_sha256: String,
    /// Unix seconds the certificate was issued.
    pub issued_at: i64,
    /// Unix seconds the certificate expires.
    pub expires_at: i64,
}

/// An agent's stored identity: key, certificate and CA.
#[derive(Clone)]
pub struct AgentIdentity {
    /// Directory the identity is stored in.
    pub dir: PathBuf,
    /// Enrollment metadata.
    pub metadata: IdentityMetadata,
    /// CA certificate PEM.
    pub ca_pem: String,
    /// Leaf certificate PEM.
    pub cert_pem: String,
    /// Private key PEM.
    pub key_pem: String,
}

impl std::fmt::Debug for AgentIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never render the private key: this type is easy to `{:?}` by
        // accident while debugging an agent.
        f.debug_struct("AgentIdentity")
            .field("dir", &self.dir)
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl AgentIdentity {
    /// Load a stored identity, or `None` when the directory holds none.
    pub fn load(dir: &Path) -> Result<Option<Self>> {
        let meta_path = dir.join(IDENTITY_META);
        if !meta_path.is_file() {
            return Ok(None);
        }
        let metadata: IdentityMetadata = serde_json::from_str(&read_text(&meta_path)?)
            .map_err(|e| Error::internal(format!("sandbox identity: {e}")))?;
        let ca_pem = read_text(&dir.join(IDENTITY_CA))?;
        let cert_pem = read_text(&dir.join(IDENTITY_CERT))?;
        let key_pem = read_text(&dir.join(IDENTITY_KEY))?;
        if pem_fingerprint(&ca_pem)? != metadata.ca_sha256 {
            return Err(Error::internal(
                "sandbox identity: ca.pem does not match the recorded CA fingerprint",
            ));
        }
        Ok(Some(Self {
            dir: dir.to_path_buf(),
            metadata,
            ca_pem,
            cert_pem,
            key_pem,
        }))
    }

    /// Persist all files (private material mode 0600).
    pub fn save(&self) -> Result<()> {
        write_private(&self.dir.join(IDENTITY_KEY), self.key_pem.as_bytes())?;
        write_private(&self.dir.join(IDENTITY_CERT), self.cert_pem.as_bytes())?;
        write_private(&self.dir.join(IDENTITY_CA), self.ca_pem.as_bytes())?;
        let metadata = serde_json::to_vec_pretty(&self.metadata)
            .map_err(|e| Error::internal(format!("sandbox identity: {e}")))?;
        write_private(&self.dir.join(IDENTITY_META), &metadata)
    }

    /// Certificate chain and key, in the form reqwest expects.
    pub fn identity_pem(&self) -> Vec<u8> {
        format!("{}{}", self.cert_pem, self.key_pem).into_bytes()
    }

    /// Renew once two thirds of the certificate's lifetime have passed.
    pub fn needs_renewal(&self, at: i64) -> bool {
        let lifetime = (self.metadata.expires_at - self.metadata.issued_at).max(1);
        at >= self.metadata.issued_at + lifetime * 2 / 3
    }

    /// The same identity with a freshly issued certificate.
    pub fn renewed(
        &self,
        cert_pem: String,
        key_pem: String,
        issued_at: i64,
        expires_at: i64,
    ) -> Self {
        let mut next = self.clone();
        next.cert_pem = cert_pem;
        next.key_pem = key_pem;
        next.metadata.issued_at = issued_at;
        next.metadata.expires_at = expires_at;
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_tokens_parse_and_reject_malformed_input() {
        let pin = "ab".repeat(32);
        let token = join_token("tok-1", "secret", &pin);
        let parts = parse_join_token(&token).unwrap();
        assert_eq!(parts.id, "tok-1");
        assert_eq!(parts.secret, "secret");
        assert_eq!(parts.ca_sha256, pin);
        for bad in [
            "",
            "plj1.tok-1.secret",
            "plj2.tok-1.secret.ab",
            "plj1..secret.ab",
        ] {
            assert!(parse_join_token(bad).is_err(), "{bad}");
        }
        let short = format!("plj1.tok-1.secret.{}", "ab");
        assert!(parse_join_token(&short).is_err());
    }

    #[test]
    fn ca_round_trips_and_issues_certificates() {
        let dir = tempfile::tempdir().unwrap();
        let ca = ServiceCa::load_or_create(&dir.path().join(CA_DIR)).unwrap();
        let reloaded = ServiceCa::load_or_create(&dir.path().join(CA_DIR)).unwrap();
        assert_eq!(ca.sha256_hex(), reloaded.sha256_hex());
        assert_eq!(ca.pem(), reloaded.pem());
        let (_, csr) = generate_csr("host-a").unwrap();
        let signed = ca
            .sign_client(&csr, "host-a", CLIENT_CERT_LIFETIME)
            .unwrap();
        assert_eq!(signed.sha256.len(), 64);
        assert_eq!(pem_certificates(&signed.cert_pem).len(), 1);
        assert!(signed.expires_at > OffsetDateTime::now_utc().unix_timestamp());
    }
}
