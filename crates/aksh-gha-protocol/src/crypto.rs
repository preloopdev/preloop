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
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::pkcs8::DecodePublicKey;
use rsa::traits::PublicKeyParts;
use rsa::{BigUint, Oaep};
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

/// An RSA public key supplied by a runner during registration.
#[derive(Clone)]
pub struct AgentRsaPublicKey {
    public_key: rsa::RsaPublicKey,
}

impl AgentRsaPublicKey {
    /// Parse runner public-key material from XML, JWK, or PEM.
    pub fn parse(value: &str) -> Result<Self, CryptoError> {
        let trimmed = value.trim();
        let public_key = if trimmed.starts_with("<") {
            parse_xml_public_key(trimmed)?
        } else if trimmed.starts_with("{") {
            parse_jwk_public_key(trimmed)?
        } else if trimmed.contains("BEGIN PUBLIC KEY") {
            rsa::RsaPublicKey::from_public_key_pem(trimmed)
                .map_err(|e| CryptoError::ParseKey(e.to_string()))?
        } else if trimmed.contains("BEGIN RSA PUBLIC KEY") {
            rsa::RsaPublicKey::from_pkcs1_pem(trimmed)
                .map_err(|e| CryptoError::ParseKey(e.to_string()))?
        } else {
            return Err(CryptoError::ParseKey(
                "unsupported public key format".to_owned(),
            ));
        };

        Ok(Self { public_key })
    }

    /// Wrap (encrypt) a symmetric key with this runner's public key.
    pub fn wrap_key(&self, plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.public_key
            .encrypt(&mut rand::thread_rng(), Oaep::new::<Sha1>(), plaintext)
            .map_err(|e| CryptoError::Wrap(e.to_string()))
    }
}

fn parse_xml_public_key(value: &str) -> Result<rsa::RsaPublicKey, CryptoError> {
    let modulus = xml_tag(value, "Modulus")
        .ok_or_else(|| CryptoError::ParseKey("missing XML Modulus".to_owned()))?;
    let exponent = xml_tag(value, "Exponent")
        .ok_or_else(|| CryptoError::ParseKey("missing XML Exponent".to_owned()))?;
    rsa_public_key_from_components(
        &BASE64_STANDARD
            .decode(modulus)
            .map_err(|e| CryptoError::ParseKey(e.to_string()))?,
        &BASE64_STANDARD
            .decode(exponent)
            .map_err(|e| CryptoError::ParseKey(e.to_string()))?,
    )
}

fn parse_jwk_public_key(value: &str) -> Result<rsa::RsaPublicKey, CryptoError> {
    let value: serde_json::Value =
        serde_json::from_str(value).map_err(|e| CryptoError::ParseKey(e.to_string()))?;
    let n = value
        .get("n")
        .and_then(|value| value.as_str())
        .ok_or_else(|| CryptoError::ParseKey("missing JWK n".to_owned()))?;
    let e = value
        .get("e")
        .and_then(|value| value.as_str())
        .ok_or_else(|| CryptoError::ParseKey("missing JWK e".to_owned()))?;
    rsa_public_key_from_components(
        &URL_SAFE_NO_PAD
            .decode(n)
            .map_err(|e| CryptoError::ParseKey(e.to_string()))?,
        &URL_SAFE_NO_PAD
            .decode(e)
            .map_err(|e| CryptoError::ParseKey(e.to_string()))?,
    )
}

fn rsa_public_key_from_components(
    modulus: &[u8],
    exponent: &[u8],
) -> Result<rsa::RsaPublicKey, CryptoError> {
    rsa::RsaPublicKey::new(
        BigUint::from_bytes_be(modulus),
        BigUint::from_bytes_be(exponent),
    )
    .map_err(|e| CryptoError::ParseKey(e.to_string()))
}

fn xml_tag<'a>(value: &'a str, tag: &str) -> Option<&'a str> {
    let mut search_idx = 0;
    while let Some(pos) = value[search_idx..].find('<') {
        let tag_start = search_idx + pos + 1;
        if let Some(tag_end) = value[tag_start..].find('>') {
            let full_tag = value[tag_start..tag_start + tag_end].trim();
            let base_tag = full_tag.split_whitespace().next().unwrap_or("");
            if base_tag == tag || base_tag.ends_with(&format!(":{tag}")) {
                let content_start = tag_start + tag_end + 1;
                let close_str = format!("</");
                let mut close_search_idx = content_start;
                while let Some(close_pos) = value[close_search_idx..].find(&close_str) {
                    let close_tag_start = close_search_idx + close_pos + 2;
                    if let Some(close_tag_end) = value[close_tag_start..].find('>') {
                        let full_close_tag = value[close_tag_start..close_tag_start + close_tag_end].trim();
                        let base_close_tag = full_close_tag.split_whitespace().next().unwrap_or("");
                        if base_close_tag == tag || base_close_tag.ends_with(&format!(":{tag}")) {
                            return Some(value[content_start..close_search_idx + close_pos].trim());
                        }
                        close_search_idx = close_tag_start + close_tag_end + 1;
                    } else {
                        break;
                    }
                }
            }
            search_idx = tag_start + tag_end + 1;
        } else {
            break;
        }
    }
    None
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

    /// Borrow this keypair's public key.
    pub fn public_key(&self) -> AgentRsaPublicKey {
        AgentRsaPublicKey {
            public_key: self.public_key.clone(),
        }
    }

    /// Export this keypair's public key in the XML shape the runner protocol accepts.
    pub fn public_key_xml(&self) -> String {
        format!(
            "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
            BASE64_STANDARD.encode(self.public_key.n().to_bytes_be()),
            BASE64_STANDARD.encode(self.public_key.e().to_bytes_be())
        )
    }
}

/// Trait for RSA parameter sources (settings::RsaParameters, etc.).
pub trait RsaParamsLike {
    /// Base64 `D` private exponent.
    fn d(&self) -> &str;
    /// Base64 `Exponent` public exponent.
    fn exponent(&self) -> &str;
    /// Base64 `Modulus`.
    fn modulus(&self) -> &str;
    /// Base64 `P` prime.
    fn p(&self) -> &str;
    /// Base64 `Q` prime.
    fn q(&self) -> &str;
}

/// Exported RSA parameters (C# RSAParameters format).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RsaParametersExport {
    #[serde(rename = "D")]
    pub d: String,
    #[serde(rename = "DP")]
    pub dp: String,
    #[serde(rename = "DQ")]
    pub dq: String,
    #[serde(rename = "Exponent")]
    pub exponent: String,
    #[serde(rename = "InverseQ")]
    pub inverse_q: String,
    #[serde(rename = "Modulus")]
    pub modulus: String,
    #[serde(rename = "P")]
    pub p: String,
    #[serde(rename = "Q")]
    pub q: String,
}

impl RsaParamsLike for RsaParametersExport {
    fn d(&self) -> &str {
        &self.d
    }
    fn exponent(&self) -> &str {
        &self.exponent
    }
    fn modulus(&self) -> &str {
        &self.modulus
    }
    fn p(&self) -> &str {
        &self.p
    }
    fn q(&self) -> &str {
        &self.q
    }
}

impl AgentRsaKeypair {
    /// Export this keypair as C# `RSAParameters`-compatible JSON fields.
    pub fn to_rsaparams(&self) -> RsaParametersExport {
        use rsa::traits::PrivateKeyParts;
        let primes = self.private_key.primes();
        let p = &primes[0];
        let q = &primes[1];
        let dp = self.private_key.dp().cloned().unwrap_or_default();
        let dq = self.private_key.dq().cloned().unwrap_or_default();
        let qi = self
            .private_key
            .qinv()
            .cloned()
            .and_then(|v| v.to_biguint())
            .unwrap_or_default();

        RsaParametersExport {
            d: BASE64_STANDARD.encode(self.private_key.d().to_bytes_be()),
            dp: BASE64_STANDARD.encode(dp.to_bytes_be()),
            dq: BASE64_STANDARD.encode(dq.to_bytes_be()),
            exponent: BASE64_STANDARD.encode(self.public_key.e().to_bytes_be()),
            inverse_q: BASE64_STANDARD.encode(qi.to_bytes_be()),
            modulus: BASE64_STANDARD.encode(self.public_key.n().to_bytes_be()),
            p: BASE64_STANDARD.encode(p.to_bytes_be()),
            q: BASE64_STANDARD.encode(q.to_bytes_be()),
        }
    }

    /// Import a keypair from C# `RSAParameters`-format fields.
    pub fn from_rsaparams(params: &dyn RsaParamsLike) -> Result<Self, CryptoError> {
        let n = BigUint::from_bytes_be(
            &BASE64_STANDARD
                .decode(params.modulus())
                .map_err(|e| CryptoError::KeyGen(e.to_string()))?,
        );
        let e = BigUint::from_bytes_be(
            &BASE64_STANDARD
                .decode(params.exponent())
                .map_err(|e| CryptoError::KeyGen(e.to_string()))?,
        );
        let d = BigUint::from_bytes_be(
            &BASE64_STANDARD
                .decode(params.d())
                .map_err(|e| CryptoError::KeyGen(e.to_string()))?,
        );
        let p_val = BigUint::from_bytes_be(
            &BASE64_STANDARD
                .decode(params.p())
                .map_err(|e| CryptoError::KeyGen(e.to_string()))?,
        );
        let q_val = BigUint::from_bytes_be(
            &BASE64_STANDARD
                .decode(params.q())
                .map_err(|e| CryptoError::KeyGen(e.to_string()))?,
        );

        let public_key = rsa::RsaPublicKey::new(n.clone(), e)
            .map_err(|err| CryptoError::KeyGen(err.to_string()))?;
        let private_key =
            rsa::RsaPrivateKey::from_components(n, public_key.e().clone(), d, vec![p_val, q_val])
                .map_err(|err| CryptoError::KeyGen(err.to_string()))?;

        Ok(Self {
            private_key,
            public_key,
        })
    }
}

/// Sign a JWT with PS256 (RSA-PSS SHA-256) — the algorithm the official runner uses.
///
/// Produces: base64url(header).base64url(claims).base64url(signature)
pub fn sign_jwt_ps256(
    header: &serde_json::Value,
    claims: &serde_json::Value,
    params: &dyn RsaParamsLike,
) -> Result<String, anyhow::Error> {
    use rsa::pss::SigningKey;
    use rsa::signature::RandomizedSigner;
    use sha2::Sha256;

    let keypair = AgentRsaKeypair::from_rsaparams(params)?;

    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_string(header)?.as_bytes());
    let claims_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_string(claims)?.as_bytes());
    let signing_input = format!("{header_b64}.{claims_b64}");

    let signing_key = SigningKey::<Sha256>::new(keypair.private_key);
    let mut rng = rand::thread_rng();
    let signature = signing_key.sign_with_rng(&mut rng, signing_input.as_bytes());
    let sig_bytes: Box<[u8]> = signature.into();
    let sig_b64 = URL_SAFE_NO_PAD.encode(&*sig_bytes);

    Ok(format!("{signing_input}.{sig_b64}"))
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
    /// RSA public-key parsing failed.
    #[error("RSA public key parse failed: {0}")]
    ParseKey(String),
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
    use rsa::traits::PublicKeyParts;

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
    fn parses_xml_public_key_for_wrapping() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let xml = format!(
            "<RSAKeyValue><Modulus>{}</Modulus><Exponent>{}</Exponent></RSAKeyValue>",
            BASE64_STANDARD.encode(kp.public_key.n().to_bytes_be()),
            BASE64_STANDARD.encode(kp.public_key.e().to_bytes_be())
        );
        let public_key = AgentRsaPublicKey::parse(&xml).unwrap();

        let wrapped = public_key.wrap_key(b"secret").unwrap();
        assert_eq!(kp.unwrap_key(&wrapped).unwrap(), b"secret");
    }
    #[test]
    fn parses_xml_public_key_with_namespaces_and_attributes() {
        let xml = "<ds:RSAKeyValue xmlns:ds=\"http://www.w3.org/2000/09/xmldsig#\">\
            <ds:Modulus attr=\"val\">AAAA</ds:Modulus>\
            <ds:Exponent >AQAB</ds:Exponent>\
            </ds:RSAKeyValue>";
        assert_eq!(xml_tag(xml, "Modulus").unwrap(), "AAAA");
        assert_eq!(xml_tag(xml, "Exponent").unwrap(), "AQAB");
    }

    #[test]
    fn parses_jwk_public_key_for_wrapping() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let jwk = serde_json::json!({
            "kty": "RSA",
            "n": URL_SAFE_NO_PAD.encode(kp.public_key.n().to_bytes_be()),
            "e": URL_SAFE_NO_PAD.encode(kp.public_key.e().to_bytes_be()),
        })
        .to_string();
        let public_key = AgentRsaPublicKey::parse(&jwk).unwrap();

        let wrapped = public_key.wrap_key(b"secret").unwrap();
        assert_eq!(kp.unwrap_key(&wrapped).unwrap(), b"secret");
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

    #[test]
    fn rsa_params_roundtrip() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let params = kp.to_rsaparams();

        // Verify field names are populated
        assert!(!params.d.is_empty());
        assert!(!params.modulus.is_empty());
        assert!(!params.exponent.is_empty());
        assert!(!params.p.is_empty());
        assert!(!params.q.is_empty());

        // Round-trip: reconstruct keypair from params
        let kp2 = AgentRsaKeypair::from_rsaparams(&params).unwrap();

        // Verify the reconstructed keypair can wrap/unwrap
        let data = vec![1u8; 32];
        let wrapped = kp2.wrap_key(&data).unwrap();
        let unwrapped = kp2.unwrap_key(&wrapped).unwrap();
        assert_eq!(data, unwrapped);

        // Verify cross-compatibility: encrypt with original, decrypt with reconstructed
        let wrapped2 = kp.wrap_key(&data).unwrap();
        let unwrapped2 = kp2.unwrap_key(&wrapped2).unwrap();
        assert_eq!(data, unwrapped2);
    }

    #[test]
    fn rsa_params_serde_field_names() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let params = kp.to_rsaparams();
        let json = serde_json::to_string(&params).unwrap();
        // Verify C# RSAParameters field naming
        assert!(json.contains("\"D\""));
        assert!(json.contains("\"DP\""));
        assert!(json.contains("\"DQ\""));
        assert!(json.contains("\"Exponent\""));
        assert!(json.contains("\"InverseQ\""));
        assert!(json.contains("\"Modulus\""));
        assert!(json.contains("\"P\""));
        assert!(json.contains("\"Q\""));
    }

    #[test]
    fn sign_jwt_ps256_produces_three_parts() {
        let kp = AgentRsaKeypair::generate().unwrap();
        let params = kp.to_rsaparams();

        let header = serde_json::json!({"typ": "JWT", "alg": "PS256"});
        let claims = serde_json::json!({
            "sub": "client-id",
            "iss": "client-id",
            "aud": "https://vstoken.example.com",
            "jti": "unique-id",
            "nbf": 1700000000u64,
            "exp": 1700000300u64,
        });

        let jwt = super::sign_jwt_ps256(&header, &claims, &params).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 dot-separated parts");
        assert!(!parts[0].is_empty(), "header segment should not be empty");
        assert!(!parts[1].is_empty(), "claims segment should not be empty");
        assert!(
            !parts[2].is_empty(),
            "signature segment should not be empty"
        );

        // Verify header decodes correctly
        let decoded_header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[0])
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&decoded_header).unwrap();
        assert_eq!(parsed["alg"], "PS256");
        assert_eq!(parsed["typ"], "JWT");
    }
}
