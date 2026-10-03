//! One anonymous OCI registry client shared by both consumers.
//!
//! The engine's packed-golden downloader ([`crate::download_oci_golden`]) and
//! `preloop init`'s manifest inspector (`preloop-cli`) each grew their own copy
//! of the same three moving parts: reference parsing, the anonymous `Bearer`
//! challenge handshake, and one level of OCI index indirection. Only what they
//! do with the resolved manifest differs — the downloader streams a layer,
//! `init` reports platforms and sizes — so the mechanism lives here and each
//! caller keeps its own presentation (log lines, exit codes, the private-
//! registry hint).

use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;

/// Manifest media types both consumers accept.
///
/// Indexes are included deliberately: `smolvm pack push` publishes the packed
/// artifact under an OCI index, so a tag or digest resolves to one
/// indirection, and registries serve multi-platform images behind a manifest
/// list.
pub const MANIFEST_ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
     application/vnd.oci.image.manifest.v1+json, \
     application/vnd.docker.distribution.manifest.list.v2+json, \
     application/vnd.docker.distribution.manifest.v2+json";

/// A registry failure, split into the cases each caller must present
/// differently: `init` maps refusal to its private-registry hint and a 404 to
/// a "check the repository and tag" message, while the downloader only logs.
#[derive(Debug)]
pub enum OciError {
    /// The registry refused (401/403), anonymously or with the token it issued.
    Unauthorized,
    /// The requested manifest does not exist.
    NotFound,
    /// Any other non-success status.
    Status(StatusCode),
    /// The body parsed as JSON but is not a manifest this client can read.
    ManifestUnreadable(String),
    /// A human-readable failure: transport, body read, token endpoint, or a
    /// malformed reference.
    Message(String),
}

impl std::fmt::Display for OciError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => {
                f.write_str("the registry refused anonymous access (HTTP 401 or 403)")
            }
            Self::NotFound => f.write_str("the registry has no such manifest (HTTP 404)"),
            Self::Status(status) => write!(f, "the registry returned HTTP {status}"),
            Self::ManifestUnreadable(error) => {
                write!(f, "the manifest is not JSON this client can read ({error})")
            }
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for OciError {}

/// A failure status from a request that was not otherwise handled specially.
fn status_error(status: StatusCode) -> OciError {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => OciError::Unauthorized,
        StatusCode::NOT_FOUND => OciError::NotFound,
        other => OciError::Status(other),
    }
}

/// A parsed image reference: the registry to talk to, the repository path, and
/// the tag or digest that identifies one manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciReference {
    pub registry: String,
    pub repository: String,
    pub reference: String,
}

impl OciReference {
    /// Split a reference into registry, repository, and tag/digest.
    ///
    /// A digest always wins over a tag (`repo:tag@sha256:…` resolves by the
    /// digest and the tag must not leak into the repository path). A name
    /// without a registry host is Docker Hub, and a single-component Docker Hub
    /// name means the official `library/` namespace. The empty/whitespace
    /// checks a CLI flag wants are the caller's, not this function's.
    pub fn parse(raw: &str) -> Result<Self, OciError> {
        let (name, reference) = match raw.split_once('@') {
            Some((name, digest)) => (strip_tag(name).to_owned(), digest.to_owned()),
            None => match split_tag(raw) {
                Some((name, tag)) => (name.to_owned(), tag.to_owned()),
                None => (raw.to_owned(), "latest".to_owned()),
            },
        };
        if name.is_empty() || reference.is_empty() {
            return Err(OciError::Message(format!(
                "`{raw}` is not a valid image reference"
            )));
        }
        let (registry, repository) = match name.split_once('/') {
            Some((first, rest))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (normalize_registry(first), rest.to_owned())
            }
            Some(_) => ("registry-1.docker.io".to_owned(), name.clone()),
            None => ("registry-1.docker.io".to_owned(), format!("library/{name}")),
        };
        Ok(Self {
            registry,
            repository,
            reference,
        })
    }

    /// The reference as a human reads it: Docker Hub's `library/` namespace and
    /// the `docker.io` host alias are hidden again.
    pub fn display(&self) -> String {
        let repository = self
            .repository
            .strip_prefix("library/")
            .filter(|_| self.registry == "registry-1.docker.io")
            .unwrap_or(&self.repository);
        let separator = if self.is_digest_pinned() { '@' } else { ':' };
        format!(
            "{}/{repository}{separator}{}",
            self.registry, self.reference
        )
    }

    /// Whether the reference is pinned by digest rather than a mutable tag.
    pub fn is_digest_pinned(&self) -> bool {
        self.reference.starts_with("sha256:")
    }

    /// URL of a manifest by tag or digest under this reference's repository.
    pub fn manifest_url(&self, reference: &str) -> String {
        format!(
            "https://{}/v2/{}/manifests/{}",
            self.registry, self.repository, reference
        )
    }

    /// URL of a blob by digest under this reference's repository.
    pub fn blob_url(&self, digest: &str) -> String {
        format!(
            "https://{}/v2/{}/blobs/{}",
            self.registry, self.repository, digest
        )
    }
}

/// The part of a reference before its tag, or the whole string when there is
/// none. Only a `:` in the last path segment is a tag separator, so a registry
/// port (`localhost:5000/team/base`) survives.
fn strip_tag(reference: &str) -> &str {
    split_tag(reference)
        .map(|(name, _)| name)
        .unwrap_or(reference)
}

fn split_tag(reference: &str) -> Option<(&str, &str)> {
    let last = reference.rsplit('/').next().unwrap_or(reference);
    let (_, tag) = last.split_once(':')?;
    if tag.is_empty() {
        return None;
    }
    Some((&reference[..reference.len() - tag.len() - 1], tag))
}

fn normalize_registry(host: &str) -> String {
    match host {
        "docker.io" | "index.docker.io" => "registry-1.docker.io".to_owned(),
        other => other.to_owned(),
    }
}

/// The fields of a `Bearer` challenge that drive a token request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerChallenge {
    pub realm: String,
    pub service: Option<String>,
    pub scope: Option<String>,
}

/// Parse a `WWW-Authenticate` value into a bearer challenge, or `None` when it
/// is not a `Bearer` challenge or names no realm.
pub fn bearer_challenge(header: &str) -> Option<BearerChallenge> {
    let parameters = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))?;
    let params = split_challenge_params(parameters);
    let value = |key: &str| {
        params
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.clone())
    };
    Some(BearerChallenge {
        realm: value("realm")?,
        service: value("service"),
        scope: value("scope"),
    })
}

/// The anonymous token dance Docker Hub and GHCR both require: their manifest
/// endpoints answer 401 with a `Bearer realm=…` challenge even for public
/// images.
///
/// Returns `Ok(None)` when the token endpoint does not hand one back — the
/// caller then treats the resource as refused.
pub async fn request_bearer_token(
    client: &reqwest::Client,
    challenge: &BearerChallenge,
    repository: &str,
) -> Result<Option<String>, OciError> {
    let scope = challenge
        .scope
        .clone()
        .unwrap_or_else(|| format!("repository:{repository}:pull"));
    let mut url = reqwest::Url::parse(&challenge.realm).map_err(|error| {
        OciError::Message(format!(
            "registry token realm `{}`: {error}",
            challenge.realm
        ))
    })?;
    if url.scheme() != "https" {
        return Err(OciError::Message(format!(
            "registry token realm `{}` is not https; refusing to request a token over plain \
             HTTP",
            challenge.realm
        )));
    }
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("scope", &scope);
        if let Some(service) = &challenge.service {
            query.append_pair("service", service);
        }
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| OciError::Message(format!("requesting a registry token: {error}")))?;
    if !response.status().is_success() {
        return Ok(None);
    }
    let body: Value = response
        .json()
        .await
        .map_err(|error| OciError::Message(format!("reading the registry token: {error}")))?;
    Ok(body
        .get("token")
        .or_else(|| body.get("access_token"))
        .and_then(Value::as_str)
        .map(str::to_owned))
}

fn split_challenge_params(raw: &str) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in raw.chars() {
        match character {
            '"' => {
                quoted = !quoted;
                current.push(character);
            }
            ',' if !quoted => push_challenge_param(&mut params, &mut current),
            _ => current.push(character),
        }
    }
    push_challenge_param(&mut params, &mut current);
    params
}

fn push_challenge_param(params: &mut Vec<(String, String)>, raw: &mut String) {
    if let Some((key, value)) = raw.split_once('=') {
        params.push((
            key.trim().to_ascii_lowercase(),
            value.trim().trim_matches('"').to_owned(),
        ));
    }
    raw.clear();
}

/// GET a registry URL, doing the anonymous `Bearer` handshake when the first
/// answer is 401.
///
/// A `token` already in hand short-circuits the challenge; `resume_from` adds a
/// byte range on both the first and the retried request, which is what makes a
/// resumed blob transfer authenticate the same way as a fresh one.
pub async fn registry_get(
    client: &reqwest::Client,
    url: &str,
    repository: &str,
    accept: &str,
    token: Option<&str>,
    resume_from: Option<u64>,
) -> Result<reqwest::Response, OciError> {
    let send = |token: Option<&str>| {
        let mut request = client.get(url).header(reqwest::header::ACCEPT, accept);
        if let Some(offset) = resume_from {
            request = request.header(reqwest::header::RANGE, format!("bytes={offset}-"));
        }
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send()
    };
    let response = send(token)
        .await
        .map_err(|error| OciError::Message(format!("cannot reach {url}: {error}")))?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if status == StatusCode::UNAUTHORIZED && token.is_none() {
        let challenge = response
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .and_then(bearer_challenge);
        let Some(challenge) = challenge else {
            return Err(OciError::Unauthorized);
        };
        let Some(token) = request_bearer_token(client, &challenge, repository).await? else {
            return Err(OciError::Unauthorized);
        };
        let response = send(Some(&token))
            .await
            .map_err(|error| OciError::Message(format!("cannot reach {url}: {error}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        return Err(status_error(status));
    }
    Err(status_error(status))
}

/// A manifest resolved to the single-image manifest it names.
#[derive(Debug)]
pub struct ResolvedManifest {
    /// The JSON served at the requested reference — an index for a
    /// multi-platform artifact.
    pub top: Value,
    /// The single-image manifest: the selected index entry, or `top` when the
    /// reference is not an index or nothing matched.
    pub image: Value,
    /// Digest of the index entry that was followed, when one was.
    pub selected_digest: Option<String>,
    /// Set when an index entry was selected but its manifest could not be
    /// fetched; `image` then falls back to `top` so a best-effort caller can
    /// still report what the index held.
    pub follow_error: Option<OciError>,
}

/// Fetch a manifest and follow one OCI index level.
///
/// `select` chooses among an index's entries: `None` takes the first listed
/// entry (the engine's packs publish exactly one platform entry), `Some` takes
/// the entry whose `os/arch` matches and leaves the index unresolved when none
/// does. Only one level is followed — the entries of a manifest list name
/// single-image manifests, not further indexes.
pub async fn get_manifest(
    client: &reqwest::Client,
    reference: &OciReference,
    accept: &str,
    select: Option<&(dyn Fn(&str) -> bool + Sync)>,
) -> Result<ResolvedManifest, OciError> {
    let top = fetch_json(
        client,
        &reference.manifest_url(&reference.reference),
        reference,
        accept,
    )
    .await?;
    let entries = top.get("manifests").and_then(Value::as_array);
    let layers_empty = top
        .get("layers")
        .and_then(Value::as_array)
        .is_none_or(|layers| layers.is_empty());
    let Some(digest) = entries
        .filter(|entries| layers_empty && !entries.is_empty())
        .and_then(|entries| select_index_entry(entries, select))
        .and_then(|entry| entry.get("digest").and_then(Value::as_str))
        .map(str::to_owned)
    else {
        return Ok(ResolvedManifest {
            image: top.clone(),
            top,
            selected_digest: None,
            follow_error: None,
        });
    };
    match fetch_json(client, &reference.manifest_url(&digest), reference, accept).await {
        Ok(image) => Ok(ResolvedManifest {
            top,
            image,
            selected_digest: Some(digest),
            follow_error: None,
        }),
        Err(error) => Ok(ResolvedManifest {
            image: top.clone(),
            top,
            selected_digest: Some(digest),
            follow_error: Some(error),
        }),
    }
}

/// Pick an index entry: the first with a digest, or the first whose declared
/// platform the selector matches.
fn select_index_entry<'a>(
    entries: &'a [Value],
    select: Option<&(dyn Fn(&str) -> bool + Sync)>,
) -> Option<&'a Value> {
    let has_digest = |entry: &Value| entry.get("digest").and_then(Value::as_str).is_some();
    match select {
        None => entries.iter().find(|entry| has_digest(entry)),
        Some(matches) => entries.iter().find(|entry| {
            has_digest(entry) && entry_platform(entry).is_some_and(|platform| matches(&platform))
        }),
    }
}

/// `os/arch` an index entry declares, when it declares one.
fn entry_platform(entry: &Value) -> Option<String> {
    let platform = entry.get("platform")?;
    let os = platform.get("os")?.as_str()?;
    let arch = platform.get("architecture")?.as_str()?;
    Some(format!("{os}/{arch}"))
}

/// GET and parse a JSON body, with the transport errors already phrased for a
/// caller that wants to log or wrap them.
async fn fetch_json(
    client: &reqwest::Client,
    url: &str,
    reference: &OciReference,
    accept: &str,
) -> Result<Value, OciError> {
    let response = registry_get(client, url, &reference.repository, accept, None, None).await?;
    let body = response.bytes().await.map_err(|error| {
        OciError::Message(format!("cannot read the response from {url}: {error}"))
    })?;
    serde_json::from_slice(&body).map_err(|error| OciError::ManifestUnreadable(error.to_string()))
}

/// Registries and Docker report GOARCH (`arm64`, `amd64`) while
/// `std::env::consts::ARCH` says `aarch64`/`x86_64`; both spellings mean this
/// host.
pub fn host_arch_aliases() -> [&'static str; 2] {
    match std::env::consts::ARCH {
        "aarch64" => ["arm64", "aarch64"],
        "x86_64" => ["amd64", "x86_64"],
        other => [other, other],
    }
}

/// Whether an `os/arch` platform names a linux image for this host.
pub fn platform_matches_host(platform: &str) -> bool {
    let Some((os, arch)) = platform.split_once('/') else {
        return false;
    };
    os == "linux" && host_arch_aliases().contains(&arch)
}

/// Media types that mark the packed-VM layer: the engine's own historical
/// artifacts and smolvm's native `pack push` media type.
pub fn is_packed_vm_layer(media_type: &str) -> bool {
    media_type == "application/vnd.preloop.smolmachine.v1+zstd"
        || media_type == "application/vnd.smolmachines.smolmachine.v1"
}

/// An OCI manifest: layers for a single image, manifests for an index.
#[derive(Debug, Deserialize)]
pub struct OciManifest {
    #[serde(default)]
    pub layers: Vec<OciLayer>,
    /// Present on OCI indexes: the listed image manifests to select from.
    #[serde(default)]
    pub manifests: Vec<OciDescriptor>,
}

#[derive(Debug, Deserialize)]
pub struct OciDescriptor {
    pub digest: String,
}

#[derive(Debug, Deserialize)]
pub struct OciLayer {
    pub digest: String,
    #[serde(default)]
    pub size: Option<u64>,
    /// OCI descriptors name this field `mediaType`; without the rename every
    /// standard manifest fails to parse and the OCI path silently falls back
    /// to the release asset.
    #[serde(rename = "mediaType")]
    pub media_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn image_references_split_into_registry_and_repository() {
        let hub = OciReference::parse("ubuntu:24.04").unwrap();
        assert_eq!(hub.registry, "registry-1.docker.io");
        assert_eq!(hub.repository, "library/ubuntu");
        assert_eq!(hub.reference, "24.04");
        assert!(!hub.is_digest_pinned());

        let namespaced = OciReference::parse("ghcr.io/preloopdev/base:1.2").unwrap();
        assert_eq!(namespaced.registry, "ghcr.io");
        assert_eq!(namespaced.repository, "preloopdev/base");
        assert_eq!(namespaced.reference, "1.2");

        let local = OciReference::parse("localhost:5000/team/base").unwrap();
        assert_eq!(local.registry, "localhost:5000");
        assert_eq!(local.repository, "team/base");
        assert_eq!(local.reference, "latest", "a bare repository pulls :latest");

        let pinned =
            OciReference::parse(&format!("ghcr.io/x/y@sha256:{}", "a".repeat(64))).unwrap();
        assert_eq!(pinned.repository, "x/y");
        assert!(pinned.is_digest_pinned());
        assert_eq!(pinned.reference, format!("sha256:{}", "a".repeat(64)));

        // `repo:tag@sha256:…` is how a digest is recorded next to a tag: the
        // tag must not leak into the repository path, or the manifest request
        // 404s.
        let tag_and_digest =
            OciReference::parse(&format!("ghcr.io/x/y:1.2@sha256:{}", "b".repeat(64))).unwrap();
        assert_eq!(tag_and_digest.repository, "x/y");
        assert_eq!(
            tag_and_digest.reference,
            format!("sha256:{}", "b".repeat(64))
        );
        assert_eq!(
            tag_and_digest.manifest_url(&tag_and_digest.reference),
            format!("https://ghcr.io/v2/x/y/manifests/sha256:{}", "b".repeat(64)),
        );
        assert_eq!(
            tag_and_digest.display(),
            format!("ghcr.io/x/y@sha256:{}", "b".repeat(64)),
        );

        // A digest for a Docker Hub repository keeps the library/ namespace.
        let hub_library =
            OciReference::parse(&format!("ubuntu@sha256:{}", "c".repeat(64))).unwrap();
        assert_eq!(hub_library.repository, "library/ubuntu");
        assert_eq!(
            hub_library.display(),
            format!("registry-1.docker.io/ubuntu@sha256:{}", "c".repeat(64))
        );

        assert!(OciReference::parse("").is_err());
        assert!(OciReference::parse("@sha256:00").is_err());
    }

    /// The engine's pack publishes one platform entry, but the selector must
    /// still follow the entry naming this host — and leave an index unresolved
    /// when it names only foreign platforms.
    #[test]
    fn index_platform_selection_picks_the_host_entry() {
        let index = json!([
            {"digest": "sha256:1", "platform": {"os": "linux", "architecture": "amd64"}},
            {"digest": "sha256:2", "platform": {"os": "linux", "architecture": "arm64"}},
            {"digest": "sha256:3", "platform": {"os": "windows", "architecture": "amd64"}},
        ]);
        let entries = index.as_array().unwrap();
        let expected = if std::env::consts::ARCH == "aarch64" {
            "sha256:2"
        } else {
            "sha256:1"
        };
        let selected = select_index_entry(entries, Some(&platform_matches_host))
            .and_then(|entry| entry.get("digest"))
            .and_then(Value::as_str);
        assert_eq!(selected, Some(expected));

        // No selector takes the first entry regardless of platform.
        assert_eq!(
            select_index_entry(entries, None)
                .and_then(|entry| entry.get("digest"))
                .and_then(Value::as_str),
            Some("sha256:1")
        );

        let foreign = json!([
            {"digest": "sha256:1", "platform": {"os": "linux", "architecture": "s390x"}}
        ]);
        assert!(
            select_index_entry(foreign.as_array().unwrap(), Some(&platform_matches_host)).is_none()
        );
    }

    #[test]
    fn challenge_parameters_survive_quoted_commas() {
        let params = split_challenge_params(
            "realm=\"https://auth.example/token\",service=\"registry.example\",scope=\"repository:a/b:pull\"",
        );
        assert_eq!(
            params
                .iter()
                .find(|(key, _)| key == "realm")
                .map(|(_, value)| value.as_str()),
            Some("https://auth.example/token")
        );
        assert_eq!(
            params
                .iter()
                .find(|(key, _)| key == "service")
                .map(|(_, value)| value.as_str()),
            Some("registry.example")
        );
    }

    #[test]
    fn bearer_challenge_parameters_parse() {
        let challenge = bearer_challenge(
            r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:preloopdev/preloop-arm64-smolvm-golden:pull""#,
        )
        .expect("a bearer challenge");
        assert_eq!(challenge.realm, "https://ghcr.io/token");
        assert_eq!(challenge.service.as_deref(), Some("ghcr.io"));
        assert_eq!(
            challenge.scope.as_deref(),
            Some("repository:preloopdev/preloop-arm64-smolvm-golden:pull")
        );
        assert!(bearer_challenge(r#"Basic realm="x""#).is_none());
        assert!(
            bearer_challenge("Bearer service=\"x\"").is_none(),
            "no realm"
        );
    }

    #[test]
    fn oci_layer_deserializes_camel_case_media_type() {
        let manifest: OciManifest = serde_json::from_str(
            r#"{"layers":[{"digest":"sha256:00","size":42,"mediaType":"application/vnd.preloop.smolmachine.v1+zstd"}]}"#,
        )
        .expect("standard OCI manifest must parse");
        let layer = manifest
            .layers
            .into_iter()
            .find(|layer| is_packed_vm_layer(&layer.media_type))
            .expect("packed VM layer present");
        assert_eq!(layer.digest, "sha256:00");
        assert_eq!(layer.size, Some(42));
    }
}
