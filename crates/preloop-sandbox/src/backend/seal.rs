//! Sealed envelopes for the few secrets that must survive a retry.
//!
//! Only lease and exec `env` maps are sealed: bearer tokens, join tokens,
//! ticket tokens, CSR keys and inject file payloads are never persisted.
//! The construction is AES-256-GCM from `ring` (an already-locked transitive
//! dependency), with the service name and resource identity as associated
//! data, so a ciphertext cannot be moved between a lease and an exec.
//!
//! The key lives outside the database (a 0600 file or `PRELOOP_SANDBOX_SEAL_KEY`)
//! and its fingerprint is recorded in `schema_meta`. A node that opens the
//! same database with a different key fails closed before serving traffic.

use std::path::Path;

use ring::aead;
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};

use crate::error::{Error, Result};

const FORMAT: u8 = 1;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const KEY_FILE: &str = "sandbox-seal.key";

/// A 32-byte envelope key with its hex SHA-256 fingerprint.
#[derive(Clone)]
pub(crate) struct SealKey {
    key: aead::LessSafeKey,
    fingerprint: String,
}

impl std::fmt::Debug for SealKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealKey")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

impl SealKey {
    /// Load `PRELOOP_SANDBOX_SEAL_KEY` (64 hex chars), an explicit key, or
    /// `<state_dir>/sandbox-seal.key`; create the file when it is absent.
    pub(crate) fn load(state_dir: &Path, explicit: Option<[u8; 32]>) -> Result<Self> {
        let key = match explicit {
            Some(key) => key,
            None => match std::env::var("PRELOOP_SANDBOX_SEAL_KEY") {
                Ok(text) => parse_hex_key(text.trim())?,
                Err(_) => load_or_create_file(state_dir)?,
            },
        };
        let fingerprint = hex::encode(Sha256::digest(key));
        Ok(Self {
            key: aead::LessSafeKey::new(
                aead::UnboundKey::new(&aead::AES_256_GCM, &key)
                    .map_err(|_| Error::internal("invalid sandbox seal key"))?,
            ),
            fingerprint,
        })
    }

    /// Hex SHA-256 of the key, recorded in `schema_meta`.
    pub(crate) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Seal `plaintext` bound to `aad`.
    pub(crate) fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| Error::internal("sandbox seal: nonce generation failed"))?;
        let mut body = plaintext.to_vec();
        let nonce_value = nonce;
        let nonce = aead::Nonce::assume_unique_for_key(nonce_value);
        self.key
            .seal_in_place_append_tag(nonce, aead::Aad::from(aad), &mut body)
            .map_err(|_| Error::internal("sandbox seal: encryption failed"))?;
        let mut output = Vec::with_capacity(1 + NONCE_LEN + body.len());
        output.push(FORMAT);
        output.extend_from_slice(&nonce_value);
        output.extend_from_slice(&body);
        Ok(output)
    }

    /// Verify and decrypt a value sealed with the same key and `aad`.
    pub(crate) fn unseal(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < 1 + NONCE_LEN + TAG_LEN || sealed[0] != FORMAT {
            return Err(Error::internal("sandbox seal: invalid envelope"));
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&sealed[1..1 + NONCE_LEN]);
        let mut body = sealed[1 + NONCE_LEN..].to_vec();
        let plaintext = self
            .key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut body,
            )
            .map_err(|_| {
                Error::new(
                    crate::error::Code::FailedPrecondition,
                    "sealed sandbox secret failed authentication (wrong store key?)",
                )
            })?;
        Ok(plaintext.to_vec())
    }
}

fn parse_hex_key(text: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(text)
        .map_err(|_| Error::failed_precondition("PRELOOP_SANDBOX_SEAL_KEY is not hex"))?;
    bytes.try_into().map_err(|_| {
        Error::failed_precondition("PRELOOP_SANDBOX_SEAL_KEY must be exactly 32 bytes (64 hex)")
    })
}

fn load_or_create_file(state_dir: &Path) -> Result<[u8; 32]> {
    let path = state_dir.join(KEY_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_hex_key(text.trim()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut key = [0u8; 32];
            SystemRandom::new()
                .fill(&mut key)
                .map_err(|_| Error::internal("sandbox seal: key generation failed"))?;
            std::fs::create_dir_all(state_dir)
                .map_err(|e| Error::internal(format!("sandbox state dir: {e}")))?;
            let text = hex::encode(key);
            crate::tls::write_private(&path, text.as_bytes())?;
            Ok(key)
        }
        Err(error) => Err(Error::internal(format!(
            "reading sandbox seal key {}: {error}",
            path.display()
        ))),
    }
}
