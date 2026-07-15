//! GitHub-compatible OIDC id-token provider.
//!
//! Implements the server-side of GitHub Actions OIDC: mints RS256-signed JWTs
//! that are byte-compatible with `https://token.actions.githubusercontent.com`,
//! exposes `/.well-known/openid-configuration` and `/.well-known/jwks.json`,
//! and formats the `sub` claim per GitHub's documented subject-claim rules.
//!
//! The runner is a passive pass-through: it copies `GenerateIdTokenUrl` and
//! `ACTIONS_ID_TOKEN_REQUEST_TOKEN` from the job message into the step
//! environment. The toolkit's `OidcClient.getIDToken()` hits this endpoint
//! and reads `{"value":"<jwt>"}`.

use aksh_gha_protocol::crypto::{sign_jwt_rs256, AgentRsaKeypair, RsaParametersExport};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// The issuer URL that real GitHub OIDC tokens carry.
pub const GITHUB_OIDC_ISSUER: &str = "https://token.actions.githubusercontent.com";

/// Token validity window in seconds (GitHub uses up to 3600; we use 600).
const TOKEN_TTL_SECS: u64 = 600;

// ---------------------------------------------------------------------------
// Key management
// ---------------------------------------------------------------------------

/// An RSA keypair dedicated to OIDC token signing, with a precomputed `kid`
/// (RFC 7638 JWK thumbprint).
#[derive(Clone)]
pub struct OidcKeypair {
    keypair: AgentRsaKeypair,
    kid: String,
    /// X.509 SHA-1 thumbprint (base64url). GitHub includes this in the JWT header.
    x5t: String,
}

impl OidcKeypair {
    /// Generate a fresh 2048-bit RSA keypair and compute its `kid` and `x5t`.
    pub fn generate() -> Result<Self, anyhow::Error> {
        let keypair = AgentRsaKeypair::generate()
            .map_err(|e| anyhow::anyhow!("OIDC keypair generation failed: {e}"))?;
        let kid = compute_kid(&keypair);
        let x5t = compute_x5t(&keypair);
        Ok(Self { keypair, kid, x5t })
    }

    /// Reconstruct from persisted `RsaParametersExport` (C# RSAParameters JSON).
    pub fn from_params(params: &RsaParametersExport) -> Result<Self, anyhow::Error> {
        let keypair = AgentRsaKeypair::from_rsaparams(params)
            .map_err(|e| anyhow::anyhow!("OIDC keypair import failed: {e}"))?;
        let kid = compute_kid(&keypair);
        let x5t = compute_x5t(&keypair);
        Ok(Self { keypair, kid, x5t })
    }

    /// Export for persistence.
    pub fn params(&self) -> RsaParametersExport {
        self.keypair.to_rsaparams()
    }

    /// The RFC 7638 JWK thumbprint used as the JWT `kid`.
    pub fn kid(&self) -> &str {
        &self.kid
    }

    /// Build the JWKS document (`{"keys":[…]}`).
    pub fn jwks(&self) -> serde_json::Value {
        let (n, e) = self.keypair.jwk_components();
        serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "kid": self.kid,
                "alg": "RS256",
                "use": "sig",
                "n": n,
                "e": e,
            }]
        })
    }

    /// Sign a JWT with RS256 using this keypair. Includes `x5t` in header
    /// to match GitHub's OIDC token format.
    pub fn sign_jwt(&self, claims: &serde_json::Value) -> Result<String, anyhow::Error> {
        let header = serde_json::json!({
            "alg": "RS256",
            "typ": "JWT",
            "kid": self.kid,
            "x5t": self.x5t,
        });
        sign_jwt_rs256(&header, claims, &self.params())
    }
}

/// Compute the RFC 7638 JWK thumbprint (base64url(SHA-256 of the canonical JWK)).
fn compute_kid(keypair: &AgentRsaKeypair) -> String {
    let (n, e) = keypair.jwk_components();
    // RFC 7638 §3.1: the canonical JSON is {"e":"…","kty":"RSA","n":"…"} (lexicographic key order).
    let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
    let hash = Sha256::digest(canonical.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

/// Compute the X.509 SHA-1 thumbprint (base64url(SHA-1 of the canonical JWK)).
/// GitHub includes this as `x5t` in the JWT header.
fn compute_x5t(keypair: &AgentRsaKeypair) -> String {
    let (n, e) = keypair.jwk_components();
    let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
    let hash = Sha1::digest(canonical.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

// ---------------------------------------------------------------------------
// Discovery document
// ---------------------------------------------------------------------------

/// Build the OpenID Connect discovery document.
pub fn discovery_document(issuer: &str, jwks_uri: &str) -> serde_json::Value {
    serde_json::json!({
        "issuer": issuer,
        "jwks_uri": jwks_uri,
        "id_token_signing_alg_values_supported": ["RS256"],
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "claims_supported": [
            "sub", "aud", "iss", "exp", "iat", "nbf", "jti",
            "actor", "actor_id",
            "repository", "repository_id", "repository_owner", "repository_owner_id",
            "repository_visibility",
            "ref", "ref_type",
            "head_ref", "base_ref",
            "event_name",
            "workflow", "workflow_ref", "workflow_sha",
            "job_workflow_ref", "job_workflow_sha",
            "run_id", "run_number", "run_attempt",
            "runner_environment",
            "environment",
            "sha",
        ],
    })
}

// ---------------------------------------------------------------------------
// Claims input and building
// ---------------------------------------------------------------------------

/// All data needed to build a GitHub-compatible OIDC token's claims.
///
/// Extracted from the workflow submission and run record at mint time.
#[derive(Debug, Clone)]
pub struct OidcClaimsInput {
    /// `"owner/repo"` — the repository slug.
    pub repository: String,
    /// `"owner"` — extracted from `repository`.
    pub repository_owner: String,
    /// `"refs/heads/main"` — the git ref that triggered the run.
    pub git_ref: String,
    /// `"push"`, `"pull_request"`, `"workflow_dispatch"`, etc.
    pub event_name: String,
    /// Commit SHA.
    pub sha: String,
    /// The account that initiated the run.
    pub actor: String,
    /// Numeric actor ID (GitHub user ID as string). Empty if unknown.
    pub actor_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Run ID (aksh-generated).
    pub run_id: String,
    /// Run number (aksh defaults to `"1"`).
    pub run_number: String,
    /// Run attempt (aksh defaults to `"1"`).
    pub run_attempt: String,
    /// Source branch for pull requests (`None` for non-PR events).
    pub head_ref: Option<String>,
    /// Target branch for pull requests (`None` for non-PR events).
    pub base_ref: Option<String>,
    /// Environment name if the job references an environment (`None` otherwise).
    pub environment: Option<String>,
    /// `"private"`, `"internal"`, or `"public"`.
    pub repository_visibility: String,
    /// Numeric repository ID (as string). Empty if unknown.
    pub repository_id: String,
    /// Numeric repository owner ID (as string). Empty if unknown.
    pub repository_owner_id: String,
    /// Full workflow ref path (e.g. `owner/repo/.github/workflows/ci.yml@refs/heads/main`).
    pub workflow_ref: Option<String>,
    /// Workflow SHA (same as `sha` for non-reusable workflows).
    pub workflow_sha: Option<String>,
    /// For reusable workflows: the ref path to the reusable workflow.
    pub job_workflow_ref: Option<String>,
    /// For reusable workflows: the commit SHA for the reusable workflow file.
    pub job_workflow_sha: Option<String>,
}

/// Derive `ref_type` from a git ref: `"branch"` for `refs/heads/*`, `"tag"` for
/// `refs/tags/*`, empty otherwise.
pub fn ref_type(git_ref: &str) -> &str {
    if git_ref.starts_with("refs/heads/") {
        "branch"
    } else if git_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        ""
    }
}

/// Extract the short branch/tag name from a ref.
pub fn short_ref(git_ref: &str) -> &str {
    if let Some(rest) = git_ref.strip_prefix("refs/heads/") {
        rest
    } else if let Some(rest) = git_ref.strip_prefix("refs/tags/") {
        rest
    } else {
        git_ref
    }
}

/// URL-encode colons in a string as `%3A`, per GitHub's `sub` formatting rules.
fn encode_colons(s: &str) -> String {
    s.replace(':', "%3A")
}

/// Format the `sub` (subject) claim per GitHub's documented rules.
///
/// - With environment: `repo:OWNER/REPO:environment:NAME`
/// - Pull request (no env): `repo:OWNER/REPO:pull_request`
/// - Branch (no env, no PR): `repo:OWNER/REPO:ref:refs/heads/BRANCH`
/// - Tag (no env, no PR): `repo:OWNER/REPO:ref:refs/tags/TAG`
///
/// Colons in environment names are `%3A`-encoded.
pub fn format_sub(input: &OidcClaimsInput) -> String {
    let repo = &input.repository;
    if let Some(env) = &input.environment {
        return format!("repo:{repo}:environment:{}", encode_colons(env));
    }
    if input.event_name == "pull_request" {
        return format!("repo:{repo}:pull_request");
    }
    if input.git_ref.starts_with("refs/heads/") || input.git_ref.starts_with("refs/tags/") {
        return format!("repo:{repo}:ref:{}", input.git_ref);
    }
    // Fallback for unusual refs (e.g. refs/pull/*/merge).
    format!("repo:{repo}:ref:{}", input.git_ref)
}

/// The default `aud` claim: `https://github.com/<owner>`.
pub fn default_audience(repository_owner: &str) -> String {
    format!("https://github.com/{repository_owner}")
}

/// Build the full JWT claims JSON for an OIDC id-token.
///
/// `now` is the current unix timestamp. `audience` is the requested audience
/// (or the default if none was supplied). `issuer` is the OIDC issuer URL.
pub fn build_claims(
    input: &OidcClaimsInput,
    audience: &str,
    issuer: &str,
    now: u64,
) -> serde_json::Value {
    let jti = uuid::Uuid::new_v4().to_string();
    let sub = format_sub(input);
    let mut claims = serde_json::json!({
        "jti": jti,
        "sub": sub,
        "aud": audience,
        "iss": issuer,
        "iat": now,
        "nbf": now.saturating_sub(300),
        "exp": now + TOKEN_TTL_SECS,
        "actor": input.actor,
        "actor_id": input.actor_id,
        "repository": input.repository,
        "repository_id": input.repository_id,
        "repository_owner": input.repository_owner,
        "repository_owner_id": input.repository_owner_id,
        "repository_visibility": input.repository_visibility,
        "ref": input.git_ref,
        "ref_type": ref_type(&input.git_ref),
        "ref_protected": "false",
        "event_name": input.event_name,
        "sha": input.sha,
        "run_id": input.run_id,
        "run_number": input.run_number,
        "run_attempt": input.run_attempt,
        "runner_environment": "self-hosted",
        "workflow": input.workflow,
        "head_ref": input.head_ref.as_deref().unwrap_or(""),
        "base_ref": input.base_ref.as_deref().unwrap_or(""),
    });

    if let Some(wf) = &input.workflow_ref {
        claims["workflow_ref"] = serde_json::json!(wf);
    }
    if let Some(ws) = &input.workflow_sha {
        claims["workflow_sha"] = serde_json::json!(ws);
    }
    if let Some(jw) = &input.job_workflow_ref {
        claims["job_workflow_ref"] = serde_json::json!(jw);
    }
    if let Some(js) = &input.job_workflow_sha {
        claims["job_workflow_sha"] = serde_json::json!(js);
    }
    if let Some(env) = &input.environment {
        claims["environment"] = serde_json::json!(env);
    }

    claims
}

pub fn parse_id_token_grant(workflow_yaml: &str, job_id: Option<&str>) -> bool {
    let Ok(parsed) = serde_yaml::from_str::<serde_yaml::Value>(workflow_yaml) else {
        return false;
    };
    let Some(jobs) = parsed.get("jobs") else {
        return false;
    };

    // Helper: "write" (string) or true (bool) means granted.
    fn permission_is_write(v: &serde_yaml::Value) -> bool {
        v.as_bool().unwrap_or(false) || v.as_str() == Some("write")
    }

    // Workflow-level permissions.
    let wf_grant = parsed
        .get("permissions")
        .and_then(|p| p.get("id-token"))
        .map(permission_is_write)
        .unwrap_or(false);
    if wf_grant {
        return true;
    }

    // Job-level permissions (if a specific job is requested).
    if let Some(jid) = job_id {
        if let Some(job) = jobs.get(jid) {
            let job_grant = job
                .get("permissions")
                .and_then(|p| p.get("id-token"))
                .map(permission_is_write)
                .unwrap_or(false);
            if job_grant {
                return true;
            }
        }
    }

    false
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // -- sub formatting -----------------------------------------------------

    #[test]
    fn sub_branch_ref() {
        let input = test_input("refs/heads/main", "push", None);
        assert_eq!(format_sub(&input), "repo:owner/repo:ref:refs/heads/main");
    }

    #[test]
    fn sub_tag_ref() {
        let input = test_input("refs/tags/v1.0", "push", None);
        assert_eq!(format_sub(&input), "repo:owner/repo:ref:refs/tags/v1.0");
    }

    #[test]
    fn sub_pull_request() {
        let input = test_input("refs/pull/42/merge", "pull_request", None);
        assert_eq!(format_sub(&input), "repo:owner/repo:pull_request");
    }

    #[test]
    fn sub_environment_overrides_event() {
        let input = test_input("refs/heads/main", "push", Some("production"));
        assert_eq!(format_sub(&input), "repo:owner/repo:environment:production");
    }

    #[test]
    fn sub_environment_overrides_pr() {
        let input = test_input("refs/pull/42/merge", "pull_request", Some("staging"));
        assert_eq!(format_sub(&input), "repo:owner/repo:environment:staging");
    }

    #[test]
    fn sub_environment_with_colon_is_encoded() {
        let input = test_input("refs/heads/main", "push", Some("Production:V1"));
        assert_eq!(
            format_sub(&input),
            "repo:owner/repo:environment:Production%3AV1"
        );
    }

    // -- default audience ---------------------------------------------------

    #[test]
    fn default_audience_is_github_owner_url() {
        assert_eq!(default_audience("octo-org"), "https://github.com/octo-org");
    }

    // -- ref_type -----------------------------------------------------------

    #[test]
    fn ref_type_branch() {
        assert_eq!(ref_type("refs/heads/main"), "branch");
    }

    #[test]
    fn ref_type_tag() {
        assert_eq!(ref_type("refs/tags/v1.0"), "tag");
    }

    #[test]
    fn ref_type_other() {
        assert_eq!(ref_type("refs/pull/42/merge"), "");
    }

    // -- short_ref ----------------------------------------------------------

    #[test]
    fn short_ref_branch() {
        assert_eq!(short_ref("refs/heads/feature/x"), "feature/x");
    }

    #[test]
    fn short_ref_tag() {
        assert_eq!(short_ref("refs/tags/v2.0"), "v2.0");
    }

    // -- claims completeness ------------------------------------------------

    #[test]
    fn claims_contain_all_required_fields() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(
            &input,
            "api://AzureADTokenExchange",
            GITHUB_OIDC_ISSUER,
            1_700_000_000,
        );

        for key in &[
            "jti",
            "sub",
            "aud",
            "iss",
            "iat",
            "nbf",
            "exp",
            "actor",
            "actor_id",
            "repository",
            "repository_id",
            "repository_owner",
            "repository_owner_id",
            "repository_visibility",
            "ref",
            "ref_type",
            "ref_protected",
            "event_name",
            "sha",
            "run_id",
            "run_number",
            "run_attempt",
            "runner_environment",
            "workflow",
            "head_ref",
            "base_ref",
        ] {
            assert!(
                claims.get(*key).is_some(),
                "claims missing required field: {key}"
            );
        }
    }

    #[test]
    fn claims_iss_is_github_oidc_issuer() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_eq!(claims["iss"], GITHUB_OIDC_ISSUER);
    }

    #[test]
    fn claims_aud_uses_provided_audience() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(
            &input,
            "api://AzureADTokenExchange",
            GITHUB_OIDC_ISSUER,
            100,
        );
        assert_eq!(claims["aud"], "api://AzureADTokenExchange");
    }

    #[test]
    fn claims_exp_is_iat_plus_600() {
        let input = test_input("refs/heads/main", "push", None);
        let now = 1_700_000_000u64;
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, now);
        assert_eq!(claims["iat"], now);
        assert_eq!(claims["nbf"], now - 300);
        assert_eq!(claims["exp"], now + TOKEN_TTL_SECS);
    }

    #[test]
    fn claims_jti_is_unique() {
        let input = test_input("refs/heads/main", "push", None);
        let c1 = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        let c2 = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_ne!(c1["jti"], c2["jti"]);
    }

    #[test]
    fn claims_environment_included_when_present() {
        let input = test_input("refs/heads/main", "push", Some("prod"));
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_eq!(claims["environment"], "prod");
    }

    #[test]
    fn claims_environment_absent_when_none() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert!(claims.get("environment").is_none());
    }

    #[test]
    fn claims_runner_environment_is_self_hosted() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_eq!(claims["runner_environment"], "self-hosted");
    }

    #[test]
    fn claims_workflow_ref_included_when_present() {
        let mut input = test_input("refs/heads/main", "push", None);
        input.workflow_ref =
            Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_string());
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_eq!(
            claims["workflow_ref"],
            "owner/repo/.github/workflows/ci.yml@refs/heads/main"
        );
    }

    #[test]
    fn claims_head_base_ref_for_pr() {
        let mut input = test_input("refs/pull/42/merge", "pull_request", None);
        input.head_ref = Some("feature-branch".to_string());
        input.base_ref = Some("main".to_string());
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        assert_eq!(claims["head_ref"], "feature-branch");
        assert_eq!(claims["base_ref"], "main");
    }

    // -- keypair / JWKS / discovery -----------------------------------------

    #[test]
    fn keypair_kid_is_rfc7638_thumbprint() {
        let kp = OidcKeypair::generate().unwrap();
        let (n, e) = kp.keypair.jwk_components();
        let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
        let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(canonical.as_bytes()));
        assert_eq!(kp.kid(), expected);
    }

    #[test]
    fn keypair_roundtrip_preserves_kid() {
        let kp = OidcKeypair::generate().unwrap();
        let params = kp.params();
        let kp2 = OidcKeypair::from_params(&params).unwrap();
        assert_eq!(kp.kid(), kp2.kid());
    }

    #[test]
    fn jwks_has_single_rsa_key_with_kid() {
        let kp = OidcKeypair::generate().unwrap();
        let jwks = kp.jwks();
        let keys = jwks["keys"].as_array().unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0]["kty"], "RSA");
        assert_eq!(keys[0]["alg"], "RS256");
        assert_eq!(keys[0]["use"], "sig");
        assert_eq!(keys[0]["kid"], kp.kid());
        assert!(keys[0]["n"].as_str().unwrap().len() > 100);
        assert_eq!(keys[0]["e"], "AQAB");
    }

    #[test]
    fn discovery_document_has_required_fields() {
        let doc = discovery_document(
            GITHUB_OIDC_ISSUER,
            "https://token.actions.githubusercontent.com/.well-known/jwks.json",
        );
        assert_eq!(doc["issuer"], GITHUB_OIDC_ISSUER);
        assert!(doc["jwks_uri"].is_string());
        let algs = doc["id_token_signing_alg_values_supported"]
            .as_array()
            .unwrap();
        assert!(algs.iter().any(|a| a == "RS256"));
        let claims = doc["claims_supported"].as_array().unwrap();
        assert!(claims.iter().any(|c| c == "sub"));
        assert!(claims.iter().any(|c| c == "aud"));
        assert!(claims.iter().any(|c| c == "iss"));
    }

    // -- RS256 sign + verify round-trip -------------------------------------

    #[test]
    fn signed_jwt_verifies_against_jwks_public_key() {
        let kp = OidcKeypair::generate().unwrap();
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test-aud", GITHUB_OIDC_ISSUER, 1_700_000_000);
        let jwt = kp.sign_jwt(&claims).unwrap();

        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);

        // Verify header
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["kid"], kp.kid());

        // Verify signature against the public key
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        assert!(kp
            .keypair
            .public_key()
            .verify_signature_rs256(signing_input.as_bytes(), &sig_bytes)
            .is_ok());
    }

    // -- permission parsing --------------------------------------------------

    #[test]
    fn parse_workflow_level_id_token_write() {
        let yaml = r#"
name: test
on: push
permissions:
  id-token: write
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        assert!(parse_id_token_grant(yaml, Some("build")));
        assert!(parse_id_token_grant(yaml, None));
    }

    #[test]
    fn parse_job_level_id_token_write() {
        let yaml = r#"
name: test
on: push
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    steps:
      - run: echo hi
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        assert!(parse_id_token_grant(yaml, Some("deploy")));
        assert!(!parse_id_token_grant(yaml, Some("build")));
    }

    #[test]
    fn parse_no_id_token_grant() {
        let yaml = r#"
name: test
on: push
permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
"#;
        assert!(!parse_id_token_grant(yaml, Some("build")));
    }

    #[test]
    fn parse_workflow_level_grant_applies_to_all_jobs() {
        let yaml = r#"
name: test
on: push
permissions:
  id-token: write
jobs:
  job-a:
    runs-on: ubuntu-latest
    steps: [{ run: "echo a" }]
  job-b:
    runs-on: ubuntu-latest
    steps: [{ run: "echo b" }]
"#;
        assert!(parse_id_token_grant(yaml, Some("job-a")));
        assert!(parse_id_token_grant(yaml, Some("job-b")));
    }

    // -- property tests ------------------------------------------------------

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn prop_sub_branch_format(
            owner in "[a-z][a-z0-9-]{1,20}",
            repo in "[a-z][a-z0-9-]{1,20}",
            branch in "[a-z][a-z0-9/_-]{0,30}",
        ) {
            let input = OidcClaimsInput {
                repository: format!("{owner}/{repo}"),
                repository_owner: owner.clone(),
                git_ref: format!("refs/heads/{branch}"),
                event_name: "push".to_string(),
                sha: "abc".to_string(),
                actor: "test".to_string(),
                actor_id: String::new(),
                repository_id: String::new(),
                repository_owner_id: String::new(),
                workflow: "test".to_string(),
                run_id: "1".to_string(),
                run_number: "1".to_string(),
                run_attempt: "1".to_string(),
                head_ref: None,
                base_ref: None,
                environment: None,
                repository_visibility: "private".to_string(),
                workflow_ref: None,
                workflow_sha: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            };
            prop_assert_eq!(
                format_sub(&input),
                format!("repo:{owner}/{repo}:ref:refs/heads/{branch}")
            );
        }

        #[test]
        fn prop_sub_tag_format(
            owner in "[a-z][a-z0-9-]{1,20}",
            repo in "[a-z][a-z0-9-]{1,20}",
            tag in "v[0-9]+(\\.[0-9]+)*",
        ) {
            let input = OidcClaimsInput {
                repository: format!("{owner}/{repo}"),
                repository_owner: owner,
                git_ref: format!("refs/tags/{tag}"),
                event_name: "push".to_string(),
                sha: "abc".to_string(),
                actor: "test".to_string(),
                actor_id: String::new(),
                repository_id: String::new(),
                repository_owner_id: String::new(),
                workflow: "test".to_string(),
                run_id: "1".to_string(),
                run_number: "1".to_string(),
                run_attempt: "1".to_string(),
                head_ref: None,
                base_ref: None,
                environment: None,
                repository_visibility: "private".to_string(),
                workflow_ref: None,
                workflow_sha: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            };
            let sub = format_sub(&input);
            prop_assert!(sub.starts_with("repo:"));
            prop_assert!(sub.contains(":ref:refs/tags/"));
        }

        #[test]
        fn prop_sub_environment_overrides(
            owner in "[a-z][a-z0-9-]{1,20}",
            repo in "[a-z][a-z0-9-]{1,20}",
            env in "[a-z][a-z0-9-]{1,20}",
            event in "(push|pull_request|workflow_dispatch)",
        ) {
            let input = OidcClaimsInput {
                repository: format!("{owner}/{repo}"),
                repository_owner: owner.clone(),
                git_ref: "refs/heads/main".to_string(),
                event_name: event,
                sha: "abc".to_string(),
                actor: "test".to_string(),
                actor_id: String::new(),
                repository_id: String::new(),
                repository_owner_id: String::new(),
                workflow: "test".to_string(),
                run_id: "1".to_string(),
                run_number: "1".to_string(),
                run_attempt: "1".to_string(),
                head_ref: None,
                base_ref: None,
                environment: Some(env.clone()),
                repository_visibility: "private".to_string(),
                workflow_ref: None,
                workflow_sha: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            };
            prop_assert_eq!(
                format_sub(&input),
                format!("repo:{owner}/{repo}:environment:{env}")
            );
        }

        #[test]
        fn prop_sub_pr_without_env(
            owner in "[a-z][a-z0-9-]{1,20}",
            repo in "[a-z][a-z0-9-]{1,20}",
        ) {
            let input = OidcClaimsInput {
                repository: format!("{owner}/{repo}"),
                repository_owner: owner.clone(),
                git_ref: "refs/pull/42/merge".to_string(),
                event_name: "pull_request".to_string(),
                sha: "abc".to_string(),
                actor: "test".to_string(),
                actor_id: String::new(),
                repository_id: String::new(),
                repository_owner_id: String::new(),
                workflow: "test".to_string(),
                run_id: "1".to_string(),
                run_number: "1".to_string(),
                run_attempt: "1".to_string(),
                head_ref: None,
                base_ref: None,
                environment: None,
                repository_visibility: "private".to_string(),
                workflow_ref: None,
                workflow_sha: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            };
            prop_assert_eq!(
                format_sub(&input),
                format!("repo:{owner}/{repo}:pull_request")
            );
        }

        #[test]
        fn prop_claims_exp_within_bounds(
            now in 1_000_000_000u64..2_000_000_000u64,
        ) {
            let input = test_input("refs/heads/main", "push", None);
            let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, now);
            let iat = claims["iat"].as_u64().unwrap();
            let exp = claims["exp"].as_u64().unwrap();
            prop_assert_eq!(iat, now);
            prop_assert!(exp > iat);
            prop_assert!(exp - iat <= 3600);
        }

        #[test]
        fn prop_claims_jti_unique(
            _ in 0..32u32,
        ) {
            let input = test_input("refs/heads/main", "push", None);
            let c1 = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
            let c2 = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
            prop_assert_ne!(&c1["jti"], &c2["jti"]);
        }

        #[test]
        fn prop_default_audience_format(
            owner in "[a-z][a-z0-9-]{1,20}",
        ) {
            let aud = default_audience(&owner);
            prop_assert_eq!(aud, format!("https://github.com/{owner}"));
        }

        #[test]
        fn prop_colon_encoding_in_environment(
            prefix in "[a-z]{1,10}",
            suffix in "[a-z]{1,10}",
        ) {
            let env = format!("{prefix}:{suffix}");
            let input = OidcClaimsInput {
                repository: "o/r".to_string(),
                repository_owner: "o".to_string(),
                git_ref: "refs/heads/main".to_string(),
                event_name: "push".to_string(),
                sha: "abc".to_string(),
                actor: "test".to_string(),
                actor_id: String::new(),
                repository_id: String::new(),
                repository_owner_id: String::new(),
                workflow: "test".to_string(),
                run_id: "1".to_string(),
                run_number: "1".to_string(),
                run_attempt: "1".to_string(),
                head_ref: None,
                base_ref: None,
                environment: Some(env),
                repository_visibility: "private".to_string(),
                workflow_ref: None,
                workflow_sha: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            };
            let sub = format_sub(&input);
            prop_assert!(sub.contains("%3A"));
            prop_assert!(!sub.contains(":") || sub.matches(':').count() <= 3);
        }

        #[test]
        fn prop_signed_jwt_verifies(
            _ in 0..4u32,
        ) {
            let kp = OidcKeypair::generate().unwrap();
            let input = test_input("refs/heads/main", "push", None);
            let claims = build_claims(&input, "test-aud", GITHUB_OIDC_ISSUER, 1_700_000_000);
            let jwt = kp.sign_jwt(&claims).unwrap();
            let parts: Vec<&str> = jwt.split('.').collect();
            prop_assert_eq!(parts.len(), 3);

            // Verify with the public key via verify_signature_rs256
            let signing_input = format!("{}.{}", parts[0], parts[1]);
            let sig_bytes = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
            let result = kp
                .keypair
                .public_key()
                .verify_signature_rs256(signing_input.as_bytes(), &sig_bytes);
            prop_assert!(
                result.is_ok(),
                "RS256 signature must verify against the public key: {:?}",
                result
            );
        }

        #[test]
        fn prop_kid_matches_thumbprint(
            _ in 0..4u32,
        ) {
            let kp = OidcKeypair::generate().unwrap();
            let (n, e) = kp.keypair.jwk_components();
            let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
            let expected = URL_SAFE_NO_PAD.encode(Sha256::digest(canonical.as_bytes()));
            prop_assert_eq!(kp.kid(), expected);
        }
    }

    // -- helpers --------------------------------------------------------------

    fn test_input(git_ref: &str, event: &str, env: Option<&str>) -> OidcClaimsInput {
        OidcClaimsInput {
            repository: "owner/repo".to_string(),
            repository_owner: "owner".to_string(),
            git_ref: git_ref.to_string(),
            event_name: event.to_string(),
            sha: "abc123def456789012345678901234567890abcd".to_string(),
            actor: "octocat".to_string(),
            actor_id: "46893322".to_string(),
            workflow: "CI".to_string(),
            run_id: "12345".to_string(),
            run_number: "1".to_string(),
            run_attempt: "1".to_string(),
            head_ref: None,
            base_ref: None,
            environment: env.map(|s| s.to_string()),
            repository_visibility: "private".to_string(),
            repository_id: "1285198891".to_string(),
            repository_owner_id: "297229730".to_string(),
            workflow_ref: None,
            workflow_sha: None,
            job_workflow_ref: None,
            job_workflow_sha: None,
        }
    }
}

/// Integration tests comparing aksh OIDC output against real GitHub golden captures.
#[cfg(test)]
mod lie_github_tests {
    use super::*;

    fn test_input(git_ref: &str, event: &str, env: Option<&str>) -> OidcClaimsInput {
        OidcClaimsInput {
            repository: "owner/repo".to_string(),
            repository_owner: "owner".to_string(),
            git_ref: git_ref.to_string(),
            event_name: event.to_string(),
            sha: "abc123def456789012345678901234567890abcd".to_string(),
            actor: "octocat".to_string(),
            actor_id: "46893322".to_string(),
            workflow: "CI".to_string(),
            run_id: "12345".to_string(),
            run_number: "1".to_string(),
            run_attempt: "1".to_string(),
            head_ref: None,
            base_ref: None,
            environment: env.map(|s| s.to_string()),
            repository_visibility: "private".to_string(),
            repository_id: "1285198891".to_string(),
            repository_owner_id: "297229730".to_string(),
            workflow_ref: None,
            workflow_sha: None,
            job_workflow_ref: None,
            job_workflow_sha: None,
        }
    }

    /// The exact claim keys present in the real GitHub token from
    /// `.runner-watch/golden/v2.335.1/15-oidc-id-token/flows.jsonl`.
    /// This is the "lie GitHub" test: our token must have ALL these keys
    /// (matching types optional — GitHub's are dynamic).
    const GITHUB_GOLDEN_CLAIM_KEYS: &[&str] = &[
        "actor",
        "actor_id",
        "aud",
        "base_ref",
        "event_name",
        "exp",
        "head_ref",
        "iat",
        "iss",
        "job_workflow_ref",
        "job_workflow_sha",
        "jti",
        "nbf",
        "ref",
        "ref_protected",
        "ref_type",
        "repository",
        "repository_id",
        "repository_owner",
        "repository_owner_id",
        "repository_visibility",
        "run_attempt",
        "run_id",
        "run_number",
        "runner_environment",
        "sha",
        "sub",
        "workflow",
        "workflow_ref",
        "workflow_sha",
    ];

    #[test]
    fn our_claims_match_github_golden_key_set() {
        let input = OidcClaimsInput {
            repository: "preloopdev/aksh-conformance-sample".to_string(),
            repository_owner: "preloopdev".to_string(),
            git_ref: "refs/heads/main".to_string(),
            event_name: "workflow_dispatch".to_string(),
            sha: "bcf619156004119fad31a4a57809d7448fd9777d".to_string(),
            actor: "Bnjoroge1".to_string(),
            actor_id: "46893322".to_string(),
            workflow: "mitm oidc".to_string(),
            run_id: "28458434874".to_string(),
            run_number: "1".to_string(),
            run_attempt: "1".to_string(),
            head_ref: None,
            base_ref: None,
            environment: None,
            repository_visibility: "private".to_string(),
            repository_id: "1285198891".to_string(),
            repository_owner_id: "297229730".to_string(),
            workflow_ref: Some(
                "preloopdev/aksh-conformance-sample/.github/workflows/15-oidc-id-token.yml@refs/heads/main"
                    .to_string(),
            ),
            workflow_sha: Some("bcf619156004119fad31a4a57809d7448fd9777d".to_string()),
            job_workflow_ref: Some(
                "preloopdev/aksh-conformance-sample/.github/workflows/15-oidc-id-token.yml@refs/heads/main"
                    .to_string(),
            ),
            job_workflow_sha: Some("bcf619156004119fad31a4a57809d7448fd9777d".to_string()),
        };
        let claims = build_claims(&input, "api://aksh", GITHUB_OIDC_ISSUER, 1_782_835_521);

        // Every key GitHub includes must be present in our output.
        let our_keys: std::collections::BTreeSet<&str> = claims
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();

        for &key in GITHUB_GOLDEN_CLAIM_KEYS {
            assert!(
                our_keys.contains(key),
                "missing GitHub claim key: {key}. Our keys: {our_keys:?}"
            );
        }

        // Verify specific values GitHub expects.
        assert_eq!(claims["iss"], "https://token.actions.githubusercontent.com");
        assert_eq!(
            claims["sub"],
            "repo:preloopdev/aksh-conformance-sample:ref:refs/heads/main"
        );
        assert_eq!(claims["aud"], "api://aksh");
        assert_eq!(claims["ref_protected"], "false");
        assert_eq!(claims["runner_environment"], "self-hosted");

        // nbf should be iat - 300 (5-min clock skew).
        assert_eq!(
            claims["nbf"].as_u64().unwrap(),
            claims["iat"].as_u64().unwrap() - 300
        );
        // exp should be iat + TTL.
        assert!(claims["exp"].as_u64().unwrap() > claims["iat"].as_u64().unwrap());

        // jti must be a valid UUID.
        assert!(uuid::Uuid::parse_str(claims["jti"].as_str().unwrap()).is_ok());
    }

    #[test]
    fn our_token_has_no_extra_keys_beyond_github_pattern() {
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        let our_keys: std::collections::BTreeSet<&str> = claims
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();

        // We allow a few extra keys that GitHub might not include due to optional
        // population (like environment, check_run_id). This test ensures we don't
        // add unexpected non-GitHub keys.
        let github_compatible_keys: std::collections::BTreeSet<&str> =
            GITHUB_GOLDEN_CLAIM_KEYS.iter().copied().collect();
        let extras: Vec<&str> = our_keys
            .difference(&github_compatible_keys)
            .copied()
            .collect();
        // Allowed extras: keys that are valid GitHub claims but not always present.
        for key in extras {
            assert!(
                key == "check_run_id" || key == "environment" || key == "actor_id",
                "unexpected non-GitHub claim key: {key}"
            );
        }
    }

    #[test]
    fn x5t_in_header_matches_sha1_of_jwk() {
        let kp = OidcKeypair::generate().unwrap();
        let input = test_input("refs/heads/main", "push", None);
        let claims = build_claims(&input, "test", GITHUB_OIDC_ISSUER, 100);
        let jwt = kp.sign_jwt(&claims).unwrap();
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert!(header["x5t"].is_string(), "JWT header must contain x5t");

        // Verify x5t is the base64url(SHA-1(canonical JWK)).
        let (n, e) = kp.keypair.jwk_components();
        let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
        let expected_x5t = URL_SAFE_NO_PAD.encode(Sha1::digest(canonical.as_bytes()));
        assert_eq!(header["x5t"].as_str().unwrap(), expected_x5t);
    }

    #[test]
    fn keypair_roundtrip_preserves_x5t() {
        let kp1 = OidcKeypair::generate().unwrap();
        let params = kp1.params();
        let kp2 = OidcKeypair::from_params(&params).unwrap();
        assert_eq!(kp1.kid(), kp2.kid());
        assert_eq!(
            kp1.x5t, kp2.x5t,
            "x5t must be stable across serialize/deserialize"
        );
    }
}
