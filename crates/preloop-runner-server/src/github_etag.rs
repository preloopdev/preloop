//! Conditional GETs for small, idempotent GitHub lookups.
//!
//! An unchanged lookup (a ref's commit, a branch, an environment's rules)
//! costs GitHub nothing once it has been fetched: the request carries the
//! `ETag` of the last `200` as `If-None-Match`, and GitHub answers
//! `304 Not Modified` with no body. The cached copy then answers the caller.
//! That `304` is also GitHub's proof that the calling credential may still
//! read the resource, so a cached body is only ever served on a `304`.
//!
//! Entries are keyed by credential bucket as well as URL, so one credential's
//! private response is never replayed for another. Only `GET` may go through
//! [`send_conditional_get`]: writes, archive downloads and anything else that
//! is not an idempotent read must use the labeled sender directly.

use std::collections::HashMap;

use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::{ETAG, HeaderValue, IF_NONE_MATCH};

use crate::github_breaker::{
    GithubBreakerRegistry, GithubConsumption, GithubCredentialBucket, GithubSubsystem,
    send_observed_labeled,
};

/// Most entries kept; the oldest stored entry is evicted first.
const MAX_ENTRIES: usize = 512;
/// Largest body kept. A bigger `200` is served uncached.
const MAX_BODY_BYTES: usize = 256 * 1024;

type Key = (GithubCredentialBucket, String);

#[derive(Clone, Debug)]
struct Entry {
    etag: HeaderValue,
    body: Bytes,
    /// Store order: the smallest stamp is the oldest entry.
    stored: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<Key, Entry>,
    next_stamp: u64,
}

/// The `ETag` and body of recent GitHub `200`s, bounded by [`MAX_ENTRIES`]
/// and [`MAX_BODY_BYTES`]. Shared through `AppState`.
#[derive(Debug, Default)]
pub struct EtagCache {
    inner: parking_lot::Mutex<CacheState>,
}

impl EtagCache {
    fn get(&self, key: &Key) -> Option<Entry> {
        self.inner.lock().entries.get(key).cloned()
    }

    /// Store or refresh the `200` for `key`. A body too large to keep drops
    /// any older copy, so the cache never answers with something older than
    /// the last `200` it saw.
    fn store(&self, key: Key, etag: HeaderValue, body: Bytes) {
        let mut inner = self.inner.lock();
        if body.len() > MAX_BODY_BYTES {
            inner.entries.remove(&key);
            return;
        }
        if !inner.entries.contains_key(&key) && inner.entries.len() >= MAX_ENTRIES {
            let oldest = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.stored)
                .map(|(oldest_key, _)| oldest_key.clone());
            if let Some(oldest) = oldest {
                inner.entries.remove(&oldest);
            }
        }
        inner.next_stamp += 1;
        let stored = inner.next_stamp;
        inner.entries.insert(key, Entry { etag, body, stored });
    }

    fn remove(&self, key: &Key) {
        self.inner.lock().entries.remove(key);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }
}

/// A GitHub GET answered through the conditional-request cache.
#[derive(Debug)]
pub struct ConditionalResponse {
    /// The status GitHub answered with: `304` when `from_cache` is set.
    pub status: StatusCode,
    /// The response body, or the cached copy that a `304` revalidated.
    pub body: Bytes,
    /// `body` is the cached copy that GitHub's `304` revalidated.
    pub from_cache: bool,
}

impl ConditionalResponse {
    /// A `2xx` answer, or a cache hit: a revalidating `304` is a success.
    pub fn is_success(&self) -> bool {
        self.from_cache || self.status.is_success()
    }
}

/// Send a `GET` through the labeled sender, revalidating against `cache`.
///
/// `request` carries the caller's auth and headers; this adds
/// `If-None-Match` when `cache` holds a `200` for this URL under `bucket`.
/// A `304` returns that copy (`from_cache`) and records no advertised bytes.
/// A `200` stores or refreshes its `ETag`, or drops a stale entry when it has
/// none. Every other status is returned untouched and is never answered from
/// the cache. Non-`GET` requests are refused before anything is sent.
pub async fn send_conditional_get(
    cache: &EtagCache,
    registry: &GithubBreakerRegistry,
    consumption: &GithubConsumption,
    bucket: GithubCredentialBucket,
    subsystem: GithubSubsystem,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<ConditionalResponse> {
    let (client, built) = request.build_split();
    let mut built = built?;
    anyhow::ensure!(
        *built.method() == reqwest::Method::GET,
        "conditional requests are GET-only, not {}",
        built.method()
    );
    // Keyed by the URL that is actually requested, so the key and the
    // validator cannot describe different resources.
    let key = (bucket, built.url().as_str().to_owned());
    let cached = cache.get(&key);
    if let Some(entry) = &cached {
        built.headers_mut().insert(IF_NONE_MATCH, entry.etag.clone());
    }
    let response = send_observed_labeled(
        registry,
        bucket,
        consumption,
        subsystem,
        reqwest::RequestBuilder::from_parts(client, built),
    )
    .await?;
    let status = response.status();
    if status == StatusCode::NOT_MODIFIED {
        // Only sent with a validator, so `cached` is set; a `304` without one
        // carries no body and is not a success.
        return Ok(match cached {
            Some(entry) => ConditionalResponse {
                status,
                body: entry.body,
                from_cache: true,
            },
            None => ConditionalResponse {
                status,
                body: Bytes::new(),
                from_cache: false,
            },
        });
    }
    consumption.record_advertised_bytes(subsystem, &response);
    let etag = response.headers().get(ETAG).cloned();
    let body = response.bytes().await?;
    if status == StatusCode::OK {
        match etag {
            Some(etag) => cache.store(key, etag, body.clone()),
            None => cache.remove(&key),
        }
    }
    Ok(ConditionalResponse {
        status,
        body,
        from_cache: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const BODY: &[u8] = br#"{"sha":"abc"}"#;

    type Reply = (u16, Vec<(&'static str, String)>, String);
    /// The `If-None-Match` of each request the stub received, in order.
    type Seen = Arc<parking_lot::Mutex<Vec<Option<String>>>>;

    /// Stub GitHub: records each request's `If-None-Match` and answers with
    /// `reply(if_none_match, authorization)`.
    async fn serve(
        reply: impl Fn(Option<String>, Option<String>) -> Reply + Send + Sync + 'static,
    ) -> (String, Seen) {
        let seen: Seen = Arc::default();
        let recorded = seen.clone();
        let reply = Arc::new(reply);
        let app = axum::Router::new().fallback(move |request: axum::extract::Request| {
            let recorded = recorded.clone();
            let reply = reply.clone();
            async move {
                let header = |name: axum::http::HeaderName| {
                    request
                        .headers()
                        .get(name)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned)
                };
                let if_none_match = header(axum::http::header::IF_NONE_MATCH);
                let authorization = header(axum::http::header::AUTHORIZATION);
                recorded.lock().push(if_none_match.clone());
                let (status, headers, body) = reply(if_none_match, authorization);
                let mut response = axum::response::Response::new(axum::body::Body::from(body));
                *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
                for (name, value) in headers {
                    response.headers_mut().insert(
                        axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        axum::http::HeaderValue::from_str(&value).unwrap(),
                    );
                }
                response
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), seen)
    }

    #[derive(Default)]
    struct Harness {
        cache: EtagCache,
        consumption: GithubConsumption,
        registry: GithubBreakerRegistry,
    }

    impl Harness {
        async fn get(
            &self,
            bucket: GithubCredentialBucket,
            url: &str,
            token: &str,
        ) -> ConditionalResponse {
            send_conditional_get(
                &self.cache,
                &self.registry,
                &self.consumption,
                bucket,
                GithubSubsystem::Env,
                crate::shared_http::CLIENT
                    .get(url)
                    .header("Authorization", format!("Bearer {token}")),
            )
            .await
            .unwrap()
        }

        fn env_usage(&self) -> crate::github_breaker::GithubSubsystemUsage {
            self.consumption
                .snapshot()
                .into_iter()
                .find(|usage| usage.subsystem == GithubSubsystem::Env.as_str())
                .expect("every subsystem is reported")
        }
    }

    #[tokio::test]
    async fn revalidation_serves_the_cached_body_on_304() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (base, seen) = serve(|if_none_match, _| {
            if if_none_match.as_deref() == Some("\"v1\"") {
                (304, vec![("content-length", "4096".to_owned())], String::new())
            } else {
                (200, vec![("etag", "\"v1\"".to_owned())], String::from_utf8_lossy(BODY).into_owned())
            }
        })
        .await;
        let url = format!("{base}/repos/o/r/commits/main");
        let harness = Harness::default();
        let bucket = GithubCredentialBucket::Installation(7);

        let first = harness.get(bucket, &url, "t").await;
        assert_eq!(first.status, StatusCode::OK);
        assert!(!first.from_cache);
        assert_eq!(first.body, Bytes::from_static(BODY));
        let body_len = first.body.len() as u64;

        let second = harness.get(bucket, &url, "t").await;
        assert_eq!(second.status, StatusCode::NOT_MODIFIED);
        assert!(second.from_cache && second.is_success());
        assert_eq!(second.body, Bytes::from_static(BODY));

        assert_eq!(
            *seen.lock(),
            vec![None, Some("\"v1\"".to_owned())],
            "the first GET is unconditional; the second revalidates its ETag"
        );
        let usage = harness.env_usage();
        assert_eq!(usage.requests, 2, "a 304 still counts as a request");
        assert_eq!(
            usage.bytes, body_len,
            "a 304 records no advertised bytes; only the 200 does"
        );
    }

    #[tokio::test]
    async fn a_changed_resource_replaces_the_cached_copy() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let current = Arc::new(parking_lot::Mutex::new((
            "\"v1\"".to_owned(),
            "old".to_owned(),
        )));
        let served = current.clone();
        let (base, seen) = serve(move |if_none_match, _| {
            let (etag, body) = served.lock().clone();
            if if_none_match.as_deref() == Some(etag.as_str()) {
                (304, vec![], String::new())
            } else {
                (200, vec![("etag", etag)], body)
            }
        })
        .await;
        let url = format!("{base}/repos/o/r/branches/main");
        let harness = Harness::default();
        let bucket = GithubCredentialBucket::Pat;

        assert_eq!(
            harness.get(bucket, &url, "t").await.body,
            Bytes::from_static(b"old")
        );
        let revalidated = harness.get(bucket, &url, "t").await;
        assert!(revalidated.from_cache);
        assert_eq!(revalidated.body, Bytes::from_static(b"old"));

        *current.lock() = ("\"v2\"".to_owned(), "new".to_owned());
        let changed = harness.get(bucket, &url, "t").await;
        assert_eq!(changed.status, StatusCode::OK);
        assert!(!changed.from_cache);
        assert_eq!(changed.body, Bytes::from_static(b"new"));

        let after = harness.get(bucket, &url, "t").await;
        assert!(after.from_cache);
        assert_eq!(after.body, Bytes::from_static(b"new"));
        assert_eq!(
            *seen.lock(),
            vec![
                None,
                Some("\"v1\"".to_owned()),
                Some("\"v1\"".to_owned()),
                Some("\"v2\"".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn only_a_200_is_ever_served_from_the_cache() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        // Once set, every request is answered with this status, never the resource.
        let refusal = Arc::new(parking_lot::Mutex::new(None::<u16>));
        let refuse = refusal.clone();
        let (base, seen) = serve(move |_, _| match *refuse.lock() {
            Some(status) => (status, vec![], format!("refused {status}")),
            None => (
                200,
                vec![("etag", "\"v1\"".to_owned())],
                "cached".to_owned(),
            ),
        })
        .await;
        let url = format!("{base}/repos/o/r/environments/prod");
        let harness = Harness::default();
        let bucket = GithubCredentialBucket::Pat;

        assert_eq!(
            harness.get(bucket, &url, "t").await.body,
            Bytes::from_static(b"cached")
        );

        *refusal.lock() = Some(404);
        let not_found = harness.get(bucket, &url, "t").await;
        assert_eq!(not_found.status, StatusCode::NOT_FOUND);
        assert!(!not_found.from_cache && !not_found.is_success());
        assert_eq!(not_found.body, Bytes::from_static(b"refused 404"));

        *refusal.lock() = Some(403);
        let forbidden = harness.get(bucket, &url, "t").await;
        assert_eq!(forbidden.status, StatusCode::FORBIDDEN);
        assert!(!forbidden.from_cache && !forbidden.is_success());
        assert_eq!(forbidden.body, Bytes::from_static(b"refused 403"));

        // The stored validator was still sent (the entry existed), yet the
        // refusals were returned as they came.
        let last = seen.lock().last().cloned().flatten();
        assert_eq!(last, Some("\"v1\"".to_owned()));
    }

    #[tokio::test]
    async fn one_credentials_cached_copy_is_never_served_to_another() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let (base, seen) = serve(|if_none_match, authorization| {
            let (etag, body) = match authorization.as_deref() {
                Some("Bearer token-a") => ("\"a\"", "body for a"),
                _ => ("\"b\"", "body for b"),
            };
            if if_none_match.as_deref() == Some(etag) {
                (304, vec![], String::new())
            } else {
                (200, vec![("etag", etag.to_owned())], body.to_owned())
            }
        })
        .await;
        let url = format!("{base}/repos/o/r/actions/runs");
        let harness = Harness::default();

        let installation = GithubCredentialBucket::Installation(1);
        let a = harness.get(installation, &url, "token-a").await;
        assert_eq!(a.body, Bytes::from_static(b"body for a"));

        let pat = GithubCredentialBucket::Pat;
        let b = harness.get(pat, &url, "token-b").await;
        assert!(!b.from_cache);
        assert_eq!(b.body, Bytes::from_static(b"body for b"));
        let last = seen.lock().last().cloned().flatten();
        assert_eq!(last, None, "the PAT bucket must not reuse installation 1's validator");

        let again = harness.get(installation, &url, "token-a").await;
        assert!(again.from_cache);
        assert_eq!(again.body, Bytes::from_static(b"body for a"));
    }

    #[tokio::test]
    async fn writes_are_refused_before_anything_is_sent() {
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        let harness = Harness::default();
        let post = crate::shared_http::CLIENT.post("http://127.0.0.1:9/never-sent");
        let result = send_conditional_get(
            &harness.cache,
            &harness.registry,
            &harness.consumption,
            GithubCredentialBucket::Pat,
            GithubSubsystem::Env,
            post,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(harness.env_usage().requests, 0);
    }

    #[test]
    fn the_cache_is_bounded_and_evicts_the_oldest_entry() {
        let cache = EtagCache::default();
        let key = |index: usize| {
            (
                GithubCredentialBucket::Pat,
                format!("https://api.example/{index}"),
            )
        };
        let etag = HeaderValue::from_static("\"v\"");
        for index in 0..=MAX_ENTRIES {
            cache.store(key(index), etag.clone(), Bytes::from_static(b"body"));
        }
        assert_eq!(cache.len(), MAX_ENTRIES);
        assert!(cache.get(&key(0)).is_none(), "the oldest entry is evicted");
        assert!(
            cache.get(&key(MAX_ENTRIES)).is_some(),
            "the newest entry is kept"
        );

        // Refreshing a key already held replaces it in place and evicts nothing.
        cache.store(key(MAX_ENTRIES), etag, Bytes::from_static(b"newer"));
        assert_eq!(cache.len(), MAX_ENTRIES);
        assert!(cache.get(&key(1)).is_some());
    }

    #[test]
    fn oversized_bodies_are_not_kept_and_drop_the_stale_copy() {
        let cache = EtagCache::default();
        let key = (GithubCredentialBucket::Pat, "https://api.example/big".to_owned());
        let etag = HeaderValue::from_static("\"v\"");
        cache.store(key.clone(), etag.clone(), Bytes::from_static(b"small"));
        assert!(cache.get(&key).is_some());

        cache.store(key.clone(), etag, Bytes::from(vec![0u8; MAX_BODY_BYTES + 1]));
        assert!(
            cache.get(&key).is_none(),
            "a stale copy must not outlive a 200 that cannot be kept"
        );
        assert_eq!(cache.len(), 0);
    }
}
