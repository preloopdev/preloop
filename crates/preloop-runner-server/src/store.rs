//! Remnants of the old durable-state store: URL selection, the AEAD
//! envelope, timestamp helpers, and run-record serialization. The
//! `ControlBackend` under `control/` owns all scheduling state now; the
//! `Store` trait and its snapshots are gone.
use crate::models::RunRecord;
use hmac::{Hmac, Mac};
type HmacSha256 = Hmac<Sha256>;
use hkdf::Hkdf;
use preloop_gha_protocol::crypto::SessionEncryption;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const SNAPSHOT_FORMAT: u8 = 2;
const MIGRATION_DOMAIN: &[u8] = b"preloop-store-v2";
const KEY_INFO_ENCRYPT: &[u8] = b"aks-store-aead/v1";
const KEY_INFO_MAC: &[u8] = b"aks-store-mac/v1";

/// Where the server should look for durable state. Parsed from `PRELOOP_STORE_URL`
/// (or an explicit override); see [`parse_store_url`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreUrl {
    /// `sqlite://<path>`, `sqlite:<path>`, or a bare filesystem path.
    /// An empty path means the default `<state_dir>/preloop.db`.
    Sqlite(std::path::PathBuf),
    /// `postgres://<user>:<pass>@<host>:<port>/<db>`.
    Postgres(String),
}

/// Environment variable selecting the store backend when no explicit URL is
/// given. Values: `sqlite://<path>`, `postgres://…`, or a bare path.
pub const STORE_URL_ENV: &str = "PRELOOP_STORE_URL";

/// Parse a store URL. Bare paths and `sqlite:` forms map to the SQLite
/// backend; `postgres://` / `postgresql://` map to the Postgres backend.
pub fn parse_store_url(value: &str) -> anyhow::Result<StoreUrl> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::new()));
    }
    if let Some(rest) = value
        .strip_prefix("sqlite://")
        .or_else(|| value.strip_prefix("sqlite:"))
    {
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::from(rest)));
    }
    if value.starts_with("postgres://") || value.starts_with("postgresql://") {
        return Ok(StoreUrl::Postgres(value.to_owned()));
    }
    if !value.contains("://") {
        // Bare path: keep the SQLite default behaviour.
        return Ok(StoreUrl::Sqlite(std::path::PathBuf::from(value)));
    }
    anyhow::bail!(
        "unsupported store URL {value:?}: expected sqlite://<path>, \
         postgres://<user>:<pass>@<host>/<db>, or a bare sqlite path"
    )
}

/// AEAD envelope used to seal persisted blobs (runs, requests, session keys,
/// metadata snapshot). Backend-independent: SQLite and Postgres both store
/// the sealed bytes as opaque blobs.
#[derive(Clone)]
pub struct Envelope {
    aead: [u8; 32],
    mac: [u8; 32],
}

impl Envelope {
    /// Derive the AEAD + MAC sub-keys from the root HMAC key (HKDF-SHA256,
    /// domain-separated per purpose).
    pub fn new(root: &[u8]) -> Self {
        let keys = derive_keys(root);
        Self {
            aead: keys.aead,
            mac: keys.mac,
        }
    }

    /// AES-256-CBC + HMAC-SHA256 over the migration domain, associated data,
    /// IV, and ciphertext. Returns `(ciphertext, iv, tag)`.
    pub fn encrypt_sealed(&self, plaintext: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        self.encrypt_sealed_with_associated_data(plaintext, &[])
    }

    /// Encrypt and authenticate a blob with caller-provided associated data.
    pub fn encrypt_sealed_with_associated_data(
        &self,
        plaintext: &[u8],
        associated_data: &[u8],
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let cipher = SessionEncryption::from_key(self.aead.to_vec());
        let (ciphertext, iv) = cipher
            .encrypt(plaintext)
            .expect("AES-256-CBC encrypt is infallible for in-spec inputs");
        let mut mac = HmacSha256::new_from_slice(&self.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(associated_data);
        mac.update(&iv);
        mac.update(&ciphertext);
        (ciphertext, iv, mac.finalize().into_bytes().to_vec())
    }

    /// Verify the MAC and decrypt. Fails on tampering or a wrong key.
    pub fn decrypt_sealed(
        &self,
        ciphertext: &[u8],
        iv: &[u8],
        tag: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        self.decrypt_sealed_with_associated_data(ciphertext, iv, tag, &[])
    }

    /// Verify and decrypt a blob with caller-provided associated data.
    pub fn decrypt_sealed_with_associated_data(
        &self,
        ciphertext: &[u8],
        iv: &[u8],
        tag: &[u8],
        associated_data: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(&self.mac).expect("HMAC accepts any key length");
        mac.update(MIGRATION_DOMAIN);
        mac.update(associated_data);
        mac.update(iv);
        mac.update(ciphertext);
        mac.verify_slice(tag)
            .map_err(|_| anyhow::anyhow!("store envelope authentication failed"))?;
        SessionEncryption::from_key(self.aead.to_vec())
            .decrypt(ciphertext, iv)
            .map_err(|error| anyhow::anyhow!("store envelope decryption failed: {error}"))
    }

    /// Seal a plaintext blob: `version || iv || ciphertext || tag`.
    pub fn seal(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.seal_with_associated_data(plaintext, &[])
    }

    /// Seal a plaintext blob bound to associated data.
    pub fn seal_with_associated_data(
        &self,
        plaintext: &[u8],
        associated_data: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let (ciphertext, iv, tag) =
            self.encrypt_sealed_with_associated_data(plaintext, associated_data);
        let mut sealed = Vec::with_capacity(1 + iv.len() + ciphertext.len() + tag.len());
        sealed.push(SNAPSHOT_FORMAT);
        sealed.extend_from_slice(&iv);
        sealed.extend_from_slice(&ciphertext);
        sealed.extend_from_slice(&tag);
        Ok(sealed)
    }

    /// Unseal a blob written by [`Envelope::seal`]. Rejects foreign envelope
    /// versions (v1 used a different, unauthenticated scheme).
    pub fn unseal(&self, sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.unseal_with_associated_data(sealed, &[])
    }

    /// Unseal a blob and require the supplied associated data.
    pub fn unseal_with_associated_data(
        &self,
        sealed: &[u8],
        associated_data: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        anyhow::ensure!(sealed.len() >= 1 + 16 + 32, "invalid store envelope");
        let version = sealed[0];
        if version != SNAPSHOT_FORMAT {
            // Old envelopes (v1) used a different key derivation (raw key or
            // SHA-256 of env input) and an unauthenticated AES-CBC layer.
            // We don't try to decrypt them — the user must drop the state
            // directory to start fresh. This is a hard cut-over; see the
            // migration notes in the architecture doc.
            anyhow::bail!(
                "unsupported store envelope version {version}; current version is {SNAPSHOT_FORMAT}"
            );
        }
        let iv = &sealed[1..17];
        let tag_start = sealed.len() - 32;
        let ciphertext = &sealed[17..tag_start];
        let tag = &sealed[tag_start..];
        self.decrypt_sealed_with_associated_data(ciphertext, iv, tag, associated_data)
    }
}

struct DerivedKeys {
    aead: [u8; 32],
    mac: [u8; 32],
}

/// Derive independent sub-keys from the loaded HMAC key via HKDF-SHA256.
///
/// The same root key is used for both the JWT HMAC and the store AEAD
/// (so callers can keep one `<state_dir>/hmac-key.bin`); HKDF gives each
/// purpose a domain-separated 32-byte key. This costs nothing when the keys
/// are derived at startup once.
fn derive_keys(root: &[u8]) -> DerivedKeys {
    // Salt = the root key itself, so two preloop installs that happen to load
    // the same weak env var don't end up with the same sub-keys.
    let mut aead_out = [0u8; 32];
    let mut mac_out = [0u8; 32];
    let salt = Sha256::digest(root);
    Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_ENCRYPT, &mut aead_out)
        .expect("HKDF expand to 32 bytes is infallible");
    Hkdf::<Sha256>::new(Some(salt.as_slice()), root)
        .expand(KEY_INFO_MAC, &mut mac_out)
        .expect("HKDF expand to 32 bytes is infallible");
    DerivedKeys {
        aead: aead_out,
        mac: mac_out,
    }
}

/// Serialize a run record for storage. The fields `#[serde(skip)]`-ped off
/// the wire shape are injected as JSON so the persisted blob is
/// self-contained: `submission` (through the sanctioned expose boundary),
/// `job_needs`, the webhook delivery id, and the expansion-only fields
/// (`caller_plans`, `github`, `head_sha`, `workflow_ref`, `workspace_snapshot`)
/// that the scheduler needs to materialize a deferred reusable-caller or
/// matrix subtree after a restart.
pub fn run_record_value(run: &RunRecord) -> anyhow::Result<serde_json::Value> {
    let mut value = serde_json::to_value(run)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("submission".to_owned(), run.submission.to_request_json()?);
        object.insert(
            "job_needs".to_owned(),
            serde_json::to_value(&run.job_needs)?,
        );
        object.insert(
            "webhook_delivery_id".to_owned(),
            serde_json::to_value(&run.webhook_delivery_id)?,
        );
        object.insert(
            "caller_plans".to_owned(),
            serde_json::to_value(&run.caller_plans)?,
        );
        object.insert("github".to_owned(), run.github.clone());
        object.insert("head_sha".to_owned(), serde_json::to_value(&run.head_sha)?);
        object.insert(
            "workflow_ref".to_owned(),
            serde_json::to_value(&run.workflow_ref)?,
        );
        object.insert(
            "workspace_snapshot".to_owned(),
            serde_json::to_value(&run.workspace_snapshot)?,
        );
    }
    Ok(value)
}

pub(crate) fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

/// The microsecond instant `delay` after `now_us`, saturating instead of
/// wrapping.
///
/// The retry ladder is configurable per state, so a deployment (or a test) can
/// hand this a duration long enough that the sum overflows the microsecond
/// epoch. Wrapping there would turn a deliberate long backoff into a deadline
/// in the past — an immediately claimable row, i.e. the hot retry loop the
/// ladder exists to prevent.
pub fn retry_deadline_us(now_us: i64, delay: std::time::Duration) -> i64 {
    now_us.saturating_add(delay.as_micros().min(i64::MAX as u128) as i64)
}

pub fn unix_us(value: chrono::DateTime<chrono::Utc>) -> i64 {
    value.timestamp_micros()
}

/// Deduplicate label strings case-insensitively, preserving first occurrence.
///
/// The handler layer already runs this when a runner registers; this is the
/// database-side backstop so a direct mutation of `inner.runners[id].labels`
/// from another code path still produces a valid `(runner_id, label)` insert.
/// Matches the case-insensitive semantics of
/// `runtime_scheduling::job_matches_runner`.
pub fn dedupe_labels_ci(labels: &[String]) -> Vec<String> {
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(labels.len());
    let mut out: Vec<String> = Vec::with_capacity(labels.len());
    for label in labels {
        if seen.insert(label.to_lowercase()) {
            out.push(label.clone());
        }
    }
    out
}
