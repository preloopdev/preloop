//! RSA/AES session crypto for the runner protocol.
//!
//! The runner protocol encrypts every `TaskAgentMessage` body with AES-CBC.
//! The AES key is generated per-session and RSA-OAEP wrapped with the
//! runner's public key before being sent in `TaskAgentSession.encryptionKey`.
//!
//! Algorithm choices (matching upstream runner.server):
//! - RSA: 2048-bit, OAEP with SHA-1 padding (default)
//! - AES: 256-bit CBC with PKCS#7 padding
//!
//! Source: `MessageListener.cs` → `GetMessageDecryptor()`
//! Source: `AgentSessionController.cs` → session creation

use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use rsa::Oaep;
use sha1::Sha1;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

/// An RSA keypair used for session key wrapping.
///
/// Stored server-side, one per runner agent. The private key wraps
/// AES session keys; the public key is sent to the runner during
/// registration.
#[derive(Clone)]
pub struct AgentRsaKeypair {
    private_key: rsa::RsaPrivateKey,
    public_key: rsa::RsaPublicKey,
}

impl AgentRsaKeypair {
    /// Generate a new 2048-bit RSA keypair.
    pub fn generate() -> Result<Self, CryptoError> {
        let mut rng = rand::thread_rng();
        let private_key = rsa::RsaPrivateKey::new(&mut rng, 2048)
            .map_err(|e| CryptoError::KeyGen(e.to_string()))?;
        let public_key = rsa::RsaPublicKey::from(&private_key);
        Ok(Self {
            private_key,
            public_key,
        })
    }

    /// Wrap (encrypt) a symmetric key with RSA-OAEP.
    ///
    /// The runner decrypts this with its copy of the private key.
    /// Returns the wrapped key bytes.
    pub fn wrap_key(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.public_key
            .encrypt(&mut rand::thread_rng(), Oaep::new::<Sha1>(), plaintext)
            .map_err(|e| CryptoError::Wrap(e.to_string()))
    }

    /// Unwrap (decrypt) a symmetric key with RSA-OAEP.
    pub fn unwrap_key(&self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.private_key
            .decrypt(Oaep::new::<Sha1>(), ciphertext)
            .map_err(|e| CryptoError::Unwrap(e.to_string()))
    }
}

/// A session encryption context — holds the AES key for encrypting/decrypting
/// message bodies within a session.
pub struct SessionEncryption {
    /// The raw AES-256 key.
    pub key: Vec<u8>,
}

impl SessionEncryption {
    /// Create a new session with a random AES-256 key.
    pub fn generate() -> Self {
        let mut key = vec![0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut key);
        Self { key }
    }

    /// Create a session from an existing key (e.g. after unwrapping).
    pub fn from_key(key: Vec<u8>) -> Self {
        Self { key }
    }

    /// Encrypt a plaintext body with AES-256-CBC + PKCS#7 padding.
    ///
    /// Returns `(ciphertext, iv)` where `iv` is the random 16-byte IV.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CryptoError> {
        let mut iv = vec![0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut iv);

        let cipher = Aes256CbcEnc::new(self.key[..].into(), iv[..].into());
        let ciphertext = cipher.encrypt_padded_vec_mut::<Pkcs7>(plaintext);

        Ok((ciphertext, iv))
    }

    /// Decrypt a ciphertext body with AES-256-CBC + PKCS#7 padding.
    pub fn decrypt(&self, ciphertext: &[u8], iv: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if iv.len() != 16 {
            return Err(CryptoError::InvalidIv("IV must be 16 bytes".to_owned()));
        }
        let cipher = Aes256CbcDec::new(self.key[..].into(), iv[..].into());
        cipher
            .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
            .map_err(|e| CryptoError::Decrypt(e.to_string()))
    }
}

/// Errors from crypto operations.
#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    /// Key generation failed.
    #[error("key generation failed: {0}")]
    KeyGen(String),
    /// Key wrapping failed.
    #[error("RSA key wrap failed: {0}")]
    Wrap(String),
    /// Key unwrapping failed.
    #[error("RSA key unwrap failed: {0}")]
    Unwrap(String),
    /// AES decryption failed.
    #[error("AES decryption failed: {0}")]
    Decrypt(String),
    /// Invalid IV length.
    #[error("invalid IV: {0}")]
    InvalidIv(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsa_keypair_generation() {
        let kp = AgentRsaKeypair::generate().unwrap();
        assert!(!kp.wrap_key(b"test").unwrap().is_empty());
    }

    #[test]
    fn rsa_wrap_unwrap_roundtrip() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let symmetric_key = vec![42u8; 32];
        let wrapped = kp.wrap_key(&symmetric_key).unwrap();
        let unwrapped = kp.unwrap_key(&wrapped).unwrap();
        assert_eq!(symmetric_key, unwrapped);
    }

    #[test]
    fn aes_encrypt_decrypt_roundtrip() {
        let session = SessionEncryption::generate();
        let plaintext = b"hello runner protocol";
        let (ciphertext, iv) = session.encrypt(plaintext).unwrap();
        assert_ne!(ciphertext, plaintext);
        let decrypted = session.decrypt(&ciphertext, &iv).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn aes_different_iv_produces_different_ciphertext() {
        let session = SessionEncryption::generate();
        let plaintext = b"same plaintext";
        let (ct1, _) = session.encrypt(plaintext).unwrap();
        let (ct2, _) = session.encrypt(plaintext).unwrap();
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn aes_invalid_iv_rejected() {
        let session = SessionEncryption::generate();
        let result = session.decrypt(b"fake", b"short");
        assert!(result.is_err());
    }

    #[test]
    fn full_session_roundtrip() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let session = SessionEncryption::generate();

        // Server wraps the AES key with the runner's public key
        let wrapped_key = kp.wrap_key(&session.key).unwrap();

        // Runner unwraps with its private key
        let unwrapped_key = kp.unwrap_key(&wrapped_key).unwrap();
        let runner_session = SessionEncryption::from_key(unwrapped_key);

        // Runner encrypts a job message
        let body = r#"{"jobId":"test","steps":[]}"#;
        let (ciphertext, iv) = runner_session.encrypt(body.as_bytes()).unwrap();

        // Server decrypts
        let decrypted = session.decrypt(&ciphertext, &iv).unwrap();
        assert_eq!(decrypted, body.as_bytes());
    }
}
