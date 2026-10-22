//! Shared circuit breaker for GitHub dependency calls.
//!
//! Every webhook delivery preloop processes needs GitHub again — workflow
//! contents, ref resolution, PR file lists, check runs. When GitHub itself is
//! unavailable, each queued delivery independently discovers that, burns an
//! attempt, and adds load to a service that is already failing. Six attempts
//! at 1/5/15/30s covers a blip; it does not cover a 30-minute incident, so
//! good pushes get dead-lettered for something that was never their fault.
//!
//! One breaker per credential fixes both halves: the queue stops claiming work
//! while GitHub is down (nothing to hammer it with, no attempts spent), and
//! the delivery that discovered the outage is *parked* — returned to
//! `received` with its attempt refunded — rather than counted as a failure.
//! Partitioning by credential matters because GitHub's budgets are per
//! credential: a 429 for installation A must not block installation B or the
//! PAT, whose independent budgets remain usable.
//!
//! Rate limiting is treated as a first-class outage signal, not as a generic
//! 5xx: GitHub tells us exactly when to come back (`retry-after`,
//! `x-ratelimit-reset`), and ignoring that is how a rate limit becomes a ban.
//!
//! The breaker registry lives in [`crate::state::AppState`], not in a global,
//! so parallel tests that point at stub APIs cannot trip each other's
//! breakers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::http::HeaderMap;

use crate::credential_store::SecretString;

/// Why a GitHub call counted against the breaker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GithubFailureKind {
    /// Transport error or 5xx: GitHub could not answer.
    Unavailable,
    /// Rate limited or secondary-limited, with GitHub's own resume time when
    /// it supplied one.
    RateLimited { retry_after: Option<Duration> },
}

/// Breaker tuning. Defaults are deliberately conservative: three consecutive
/// dependency failures before opening, so one unlucky 502 does not stall the
/// queue, and a five-minute ceiling so a long outage is retried steadily
/// rather than exponentially forgotten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub base_open: Duration,
    pub max_open: Duration,
    /// How long a half-open probe may run before another caller is allowed
    /// through. Without this a probe whose task died would wedge the breaker
    /// half-open forever.
    pub probe_timeout: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 3,
            base_open: Duration::from_secs(30),
            max_open: Duration::from_secs(300),
            probe_timeout: Duration::from_secs(60),
        }
    }
}

impl BreakerConfig {
    /// The `PRELOOP_GITHUB_BREAKER*` overrides as explicit values (resolved
    /// at startup by [`crate::config::StartupConfig`]): `enabled: Some(false)`
    /// disables tripping entirely (calls are still observed, so the status
    /// snapshot keeps reporting failures); `failure_threshold: Some(n)`
    /// overrides the failure count (`0` and smaller keep the default).
    pub fn from_overrides(enabled: Option<bool>, failure_threshold: Option<u32>) -> Self {
        let mut config = Self::default();
        if let Some(enabled) = enabled {
            config.enabled = enabled;
        }
        if let Some(threshold) = failure_threshold.filter(|threshold| *threshold > 0) {
            config.failure_threshold = threshold;
        }
        config
    }
}

/// Point-in-time breaker state for the operational snapshot and the health
/// API. Instant-free so it can be serialized and compared in tests.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct BreakerSnapshot {
    pub open: bool,
    pub retry_in_seconds: Option<u64>,
    pub consecutive_failures: u32,
    pub trips: u64,
    pub rate_limited: bool,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
struct BreakerInner {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    probe_started: Option<Instant>,
    trips: u64,
    /// Consecutive opens, used to grow the open window. Reset by a success.
    open_streak: u32,
    rate_limited: bool,
    last_error: Option<String>,
}

/// Shared breaker guarding every GitHub-dependent call in the webhook path.
#[derive(Debug)]
pub struct GithubBreaker {
    config: BreakerConfig,
    inner: parking_lot::Mutex<BreakerInner>,
}

impl Default for GithubBreaker {
    fn default() -> Self {
        Self::new(BreakerConfig::default())
    }
}

impl GithubBreaker {
    pub fn new(config: BreakerConfig) -> Self {
        Self {
            config,
            inner: parking_lot::Mutex::new(BreakerInner::default()),
        }
    }

    /// How long the caller must wait before GitHub work is worth attempting,
    /// or `None` when the breaker is closed (or ready for a probe).
    ///
    /// Unlike [`GithubBreaker::acquire`] this reserves nothing, so the queue
    /// worker can use it to decide whether to claim a row at all.
    pub fn retry_after(&self) -> Option<Duration> {
        let now = Instant::now();
        let inner = self.inner.lock();
        if let Some(remaining) = Self::remaining(&inner, now) {
            return Some(remaining);
        }
        if inner.open_until.is_some()
            && let Some(started) = inner.probe_started
            && let Some(elapsed) = now.checked_duration_since(started)
            && elapsed < self.config.probe_timeout
        {
            return Some(self.config.probe_timeout - elapsed);
        }
        None
    }

    pub fn is_open(&self) -> bool {
        self.retry_after().is_some()
    }

    /// Ask permission to call GitHub.
    ///
    /// `Ok(())` while closed. Once the open window elapses exactly one caller
    /// is let through as a probe; everyone else keeps waiting until that
    /// probe reports back (or its timeout lapses), so recovery costs GitHub
    /// one request rather than the whole backlog at once.
    pub fn acquire(&self) -> Result<(), Duration> {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        if let Some(remaining) = Self::remaining(&inner, now) {
            return Err(remaining);
        }
        if inner.open_until.is_some() {
            // Open window elapsed: this is the half-open transition.
            let probe_live = inner
                .probe_started
                .is_some_and(|started| now.duration_since(started) < self.config.probe_timeout);
            if probe_live {
                return Err(self.config.probe_timeout);
            }
            inner.probe_started = Some(now);
        }
        Ok(())
    }

    /// Record a GitHub call that answered — any answer at all, including
    /// 404 or 422. Those prove reachability, which is the only thing this
    /// breaker is about; payload-level failures are the delivery's problem.
    pub fn record_success(&self) {
        let mut inner = self.inner.lock();
        inner.consecutive_failures = 0;
        inner.open_until = None;
        inner.probe_started = None;
        inner.open_streak = 0;
        inner.rate_limited = false;
        inner.last_error = None;
    }

    /// Record a dependency failure, opening the breaker once the threshold is
    /// reached. A rate limit opens immediately for as long as GitHub says.
    pub fn record_failure(&self, kind: &GithubFailureKind, error: impl Into<String>) {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        inner.last_error = Some(error.into());
        inner.probe_started = None;
        if !self.config.enabled {
            return;
        }
        match kind {
            GithubFailureKind::RateLimited { retry_after } => {
                inner.rate_limited = true;
                // GitHub named a resume time: honour exactly that, clamped so
                // a bogus header cannot park the queue for a day.
                let wait = retry_after
                    .unwrap_or(self.config.base_open)
                    .clamp(Duration::from_secs(1), Duration::from_secs(3600));
                Self::open_for(&mut inner, now, wait);
            }
            GithubFailureKind::Unavailable => {
                inner.rate_limited = false;
                if inner.consecutive_failures < self.config.failure_threshold {
                    return;
                }
                let backoff = self
                    .config
                    .base_open
                    .saturating_mul(1u32 << inner.open_streak.min(4))
                    .min(self.config.max_open);
                Self::open_for(&mut inner, now, backoff);
            }
        }
    }

    /// Observe a completed HTTP exchange and update the breaker.
    ///
    /// Returns the failure classification when the exchange counted as a
    /// dependency failure, so the caller can decide to park rather than fail
    /// the work item.
    pub fn observe_status(
        &self,
        status: axum::http::StatusCode,
        headers: &HeaderMap,
    ) -> Option<GithubFailureKind> {
        match classify_status(status, headers) {
            Some(kind) => {
                self.record_failure(&kind, format!("GitHub responded {status}"));
                Some(kind)
            }
            None => {
                self.record_success();
                None
            }
        }
    }

    /// Honour a *primary* rate limit advertised on an otherwise successful
    /// response.
    ///
    /// GitHub sends `x-ratelimit-remaining: 0` plus `x-ratelimit-reset` on the
    /// last allowed response. Recording the advertised window here means the
    /// caller parks itself until the reset instead of spending the next
    /// request on a guaranteed 403. A response without rate headers, or one
    /// that still has budget, changes nothing.
    ///
    /// Like [`GithubBreaker::record_failure`], this is a no-op when the
    /// breaker is disabled (`PRELOOP_GITHUB_BREAKER=off`): an operator who
    /// turned tripping off must not have it re-enabled by the budget path.
    pub fn observe_rate_budget(&self, headers: &HeaderMap) {
        if !self.config.enabled {
            return;
        }
        let remaining = header_str(headers, "x-ratelimit-remaining")
            .and_then(|value| value.trim().parse::<i64>().ok());
        if !remaining.is_some_and(|remaining| remaining <= 0) {
            return;
        }
        let Some(wait) = retry_after_from_headers(headers) else {
            return;
        };
        let now = Instant::now();
        let mut inner = self.inner.lock();
        inner.rate_limited = true;
        inner.last_error = Some("GitHub primary rate limit exhausted".to_owned());
        Self::open_for(
            &mut inner,
            now,
            wait.clamp(Duration::from_secs(1), Duration::from_secs(3600)),
        );
    }

    /// Observe a transport-level failure (DNS, TLS, connect, timeout).
    pub fn observe_transport_error(&self, error: &reqwest::Error) -> GithubFailureKind {
        let kind = GithubFailureKind::Unavailable;
        self.record_failure(&kind, error.to_string());
        kind
    }

    pub fn snapshot(&self) -> BreakerSnapshot {
        let now = Instant::now();
        let inner = self.inner.lock();
        let remaining = Self::remaining(&inner, now);
        BreakerSnapshot {
            open: remaining.is_some(),
            retry_in_seconds: remaining.map(|remaining| remaining.as_secs()),
            consecutive_failures: inner.consecutive_failures,
            trips: inner.trips,
            rate_limited: inner.rate_limited,
            last_error: inner.last_error.clone(),
        }
    }

    fn open_for(inner: &mut BreakerInner, now: Instant, wait: Duration) {
        inner.open_until = Some(now + wait);
        inner.open_streak = inner.open_streak.saturating_add(1);
        inner.trips = inner.trips.saturating_add(1);
    }

    fn remaining(inner: &BreakerInner, now: Instant) -> Option<Duration> {
        inner
            .open_until
            .filter(|open_until| *open_until > now)
            .map(|open_until| open_until - now)
    }
}

/// Rate-limit identity of the credential an outbound GitHub call
/// authenticates with.
///
/// GitHub's budgets are per credential: one installation token's 429 must
/// not park another installation's traffic, and the static PAT, each App's JWT,
/// and anonymous calls each have their own budget. Breaker state is
/// partitioned on this key so a throttled credential only blocks its own
/// traffic; consumption accounting keeps its per-subsystem labels separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GithubCredentialBucket {
    /// A GitHub App installation token, keyed by installation id.
    Installation(u64),
    /// The engine's static PAT (`PRELOOP_GITHUB_TOKEN` / config `github.pat`).
    Pat,
    /// A GitHub App JWT, keyed by App id (webhook delivery management, app
    /// health checks, installation lookup and token minting). Each App is its
    /// own rate-limit identity.
    App(u64),
    /// An installation token whose installation id this call cannot name —
    /// dispatch auth validating a third-party token, where the id only
    /// arrives in the response being made.
    ///
    /// Keyed on a fingerprint of the token itself, because the token *is* the
    /// rate-limit identity until its installation is known. The PAT's bucket
    /// belongs to the operator and inventing an id would charge another
    /// installation, so the alternative — one bucket shared by every
    /// unattributable token — would let one tenant's throttled token park
    /// every other tenant's dispatch validation. Callers supply arbitrary
    /// bearer values, so the registry bounds how many of these it keeps.
    InstallationToken(u64),
    /// The forge relay's Git smart-HTTP transport.
    ///
    /// Git transport is a different dependency from the REST API: for
    /// github.com it is a different host (`github.com` vs `api.github.com`)
    /// and it is not metered by the anonymous 60-requests-per-hour API budget.
    /// Sharing [`GithubCredentialBucket::Anonymous`] would let an exhausted
    /// REST budget refuse a public checkout, and a failing Git endpoint refuse
    /// anonymous API calls.
    Relay,
    /// No credential on the anonymous REST API (60 req/hour/IP).
    Anonymous,
}

impl GithubCredentialBucket {
    /// The App-JWT bucket for `app_id`.
    ///
    /// GitHub App ids are numeric. An id that does not parse (emulators, test
    /// doubles) is hashed instead, so every App still gets its own budget.
    pub fn for_app(app_id: &str) -> Self {
        let app_id = app_id.trim();
        let id = app_id
            .parse::<u64>()
            .unwrap_or_else(|_| fnv1a_64(app_id.as_bytes()));
        Self::App(id)
    }

    /// The bucket for an installation token whose installation id the caller
    /// cannot name, keyed on the token.
    ///
    /// The fingerprint is FNV-1a for the same reason [`for_app`] uses it: a
    /// bucket names a rate-limit budget, and that meaning must survive a
    /// standard-library upgrade. Tokens are GitHub-minted opaque strings, so
    /// the digest is not a security boundary and collision resistance is not
    /// required of it.
    pub fn for_installation_token(token: &str) -> Self {
        Self::InstallationToken(fnv1a_64(token.as_bytes()))
    }

    /// Stable label for snapshots and traces.
    pub fn label(&self) -> String {
        match self {
            GithubCredentialBucket::Installation(id) => format!("installation:{id}"),
            GithubCredentialBucket::Pat => "pat".to_owned(),
            GithubCredentialBucket::App(id) => format!("app:{id}"),
            GithubCredentialBucket::InstallationToken(fingerprint) => {
                format!("installation-token:{fingerprint:016x}")
            }
            GithubCredentialBucket::Relay => "relay".to_owned(),
            GithubCredentialBucket::Anonymous => "anonymous".to_owned(),
        }
    }
}

/// FNV-1a over `bytes`. Chosen over `DefaultHasher`, whose output the standard
/// library does not promise to keep across releases: a bucket names a budget,
/// and that meaning must survive an upgrade.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

/// The service origin of `url`: `host[:port]`, lowercased, with scheme, path,
/// and a port that is only the scheme's default dropped.
///
/// This is the identity a breaker partition carries for the host an outbound
/// call reaches. It is applied to the API base the engine resolved from
/// configuration as well as to request URLs, so a value that is not a URL at
/// all still names a stable partition of its own rather than collapsing every
/// misconfigured caller into the same one.
pub fn service_origin(url: &str) -> String {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return url.trim().to_ascii_lowercase();
    };
    let Some(host) = parsed.host_str() else {
        return url.trim().to_ascii_lowercase();
    };
    let host = host.to_ascii_lowercase();
    // `Url` keeps no port that is only the scheme's default, so a port that
    // arrives here is one that distinguishes the service.
    match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

/// The origin the engine's own GitHub calls resolve to: the
/// `PRELOOP_GITHUB_API_URL` override every other origin-aware helper in the
/// server reads, else github.com's API.
fn default_github_origin() -> String {
    std::env::var("PRELOOP_GITHUB_API_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| service_origin(&value))
        .unwrap_or_else(|| "api.github.com".to_owned())
}

/// One breaker partition: a credential's budget *at* one service origin.
///
/// A credential alone cannot name a budget. GitHub's anonymous allowance is per
/// address *per host*, and one process reaches several GitHub-family services —
/// the engine's configured API origin, a GHES instance, GHCR, a release-asset
/// host, a golden mirror an operator named with `PRELOOP_GOLDEN_URL`. Keying on
/// the credential alone would park healthy traffic for an outage at another
/// host, and let a success at the wrong host clear a real outage's streak.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GithubBreakerKey {
    origin: String,
    bucket: GithubCredentialBucket,
}

/// Registry of per-credential, per-origin breakers.
///
/// Every outbound GitHub call resolves its credential's
/// [`GithubCredentialBucket`] — and, when it reaches a service other than the
/// engine's own GitHub origin, that origin — and uses that pair's breaker, so
/// unrelated rate-limit budgets and unrelated hosts never block each other
/// while a genuine GitHub outage still parks every bucket that observes it.
#[derive(Debug)]
pub struct GithubBreakerRegistry {
    config: BreakerConfig,
    github_origin: String,
    inner: parking_lot::Mutex<BreakerRegistryInner>,
}

/// Unattributable installation-token buckets the registry keeps at once.
///
/// Every other bucket is named by something the server chose (an installation
/// id from the mint ledger, an App id from its own registry, the PAT, the
/// anonymous and relay buckets), so their number is bounded by the server's
/// own configuration. An [`GithubCredentialBucket::InstallationToken`] bucket
/// is named by a bearer a caller supplied, so it needs a bound of its own:
/// past this many, the least recently used one is dropped. Losing a breaker
/// costs a backoff streak, never correctness — the next call from that token
/// simply probes GitHub again.
const MAX_UNNAMED_INSTALLATION_BUCKETS: usize = 256;

#[derive(Debug)]
struct BreakerRegistryInner {
    breakers: HashMap<GithubBreakerKey, Arc<GithubBreaker>>,
    /// Outstanding `InstallationToken` partitions, least recently used first.
    unnamed: std::collections::VecDeque<GithubBreakerKey>,
}

impl GithubBreakerRegistry {
    pub fn new(config: BreakerConfig) -> Self {
        Self::for_origin(config, default_github_origin())
    }

    /// A registry whose bucket-keyed methods speak for `origin` — the GitHub
    /// service the engine's own calls resolve to.
    ///
    /// [`GithubBreakerRegistry::new`] derives it from `PRELOOP_GITHUB_API_URL`
    /// (else github.com's API); a caller that resolved the base from the config
    /// file instead passes it here, so a GHES install never shares a partition
    /// with the public API. Calls to any other host use
    /// [`GithubBreakerRegistry::breaker_at`].
    pub fn for_origin(config: BreakerConfig, origin: impl Into<String>) -> Self {
        Self {
            config,
            github_origin: origin.into(),
            inner: parking_lot::Mutex::new(BreakerRegistryInner {
                breakers: HashMap::new(),
                unnamed: std::collections::VecDeque::new(),
            }),
        }
    }

    /// The GitHub origin this registry's own calls speak for.
    pub fn github_origin(&self) -> &str {
        &self.github_origin
    }

    /// The breaker for `bucket` at the engine's own GitHub origin, creating it
    /// on first use. Breakers are never removed except to bound the
    /// unnamed-installation set: the key set is otherwise bounded
    /// (installations and hosts the server actually talks to), and dropping one
    /// would lose its backoff streak.
    pub fn breaker_for(&self, bucket: GithubCredentialBucket) -> Arc<GithubBreaker> {
        let origin = self.github_origin.clone();
        self.breaker_at(&origin, bucket)
    }

    /// The breaker for `bucket` at `origin` — for a call to a service that is
    /// not the engine's GitHub origin (a mirror, a registry, a token realm).
    pub fn breaker_at(&self, origin: &str, bucket: GithubCredentialBucket) -> Arc<GithubBreaker> {
        let key = GithubBreakerKey {
            origin: origin.to_owned(),
            bucket,
        };
        let mut inner = self.inner.lock();
        inner.touch_unnamed(&key);
        if let Some(existing) = inner.breakers.get(&key).cloned() {
            return existing;
        }
        let breaker = Arc::new(GithubBreaker::new(self.config));
        inner.breakers.insert(key.clone(), breaker.clone());
        if matches!(bucket, GithubCredentialBucket::InstallationToken(_)) {
            inner.unnamed.push_back(key);
            while inner.unnamed.len() > MAX_UNNAMED_INSTALLATION_BUCKETS {
                if let Some(evicted) = inner.unnamed.pop_front() {
                    inner.breakers.remove(&evicted);
                }
            }
        }
        breaker
    }

    /// Per-bucket retry hint, for callers that know their credential.
    pub fn retry_after_for(&self, bucket: GithubCredentialBucket) -> Option<Duration> {
        self.breaker_for(bucket).retry_after()
    }

    /// Minimum remaining wait across buckets open for non-rate-limit reasons.
    ///
    /// Unreachability (5xx, DNS, connect) is (almost) always GitHub-wide,
    /// while rate limits are per credential by nature. The webhook queue
    /// uses this to decide whether claiming work is worth it: a throttled
    /// installation must not stop other installations' deliveries from being
    /// claimed, but a GitHub outage means every claim would burn an attempt.
    ///
    /// Speaks for the engine's own GitHub origin
    /// ([`GithubBreakerRegistry::github_origin`]) only: an outage at some other
    /// host the process talks to — a GHES instance, a golden mirror — says
    /// nothing about whether GitHub work is worth attempting.
    ///
    /// A bucket whose window has elapsed but whose half-open probe is still
    /// running is still down. [`GithubBreaker::retry_after`] covers the probe;
    /// [`BreakerSnapshot::open`] covers only the window, so it cannot be used.
    pub fn unavailable_retry_after(&self) -> Option<Duration> {
        let origin = self.github_origin.clone();
        self.unavailable_retry_after_at(&origin)
    }

    /// [`GithubBreakerRegistry::unavailable_retry_after`] for `origin`:
    /// GitHub-wide across the credentials that spend budget there, and only
    /// there.
    pub fn unavailable_retry_after_at(&self, origin: &str) -> Option<Duration> {
        self.inner
            .lock()
            .breakers
            .iter()
            .filter(|(key, _)| key.origin == origin)
            .map(|(_, breaker)| breaker)
            .filter(|breaker| !breaker.snapshot().rate_limited)
            .filter_map(|breaker| breaker.retry_after())
            .min()
    }

    /// Per-bucket breaker state for the operational snapshot, in stable
    /// bucket-label order.
    ///
    /// Partitions at the engine's own origin keep the plain
    /// [`GithubCredentialBucket::label`] the snapshots have always published;
    /// a foreign origin is named after it, so an operator can tell which host a
    /// parked bucket is parked for.
    pub fn snapshot(&self) -> Vec<GithubBucketSnapshot> {
        let inner = self.inner.lock();
        let mut snapshots: Vec<GithubBucketSnapshot> = inner
            .breakers
            .iter()
            .map(|(key, breaker)| GithubBucketSnapshot {
                bucket: if key.origin == self.github_origin {
                    key.bucket.label()
                } else {
                    format!("{}@{}", key.bucket.label(), key.origin)
                },
                breaker: breaker.snapshot(),
            })
            .collect();
        snapshots.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        snapshots
    }
}

impl BreakerRegistryInner {
    /// Move `key` to the most-recently-used end of the unnamed order.
    fn touch_unnamed(&mut self, key: &GithubBreakerKey) {
        if !matches!(key.bucket, GithubCredentialBucket::InstallationToken(_)) {
            return;
        }
        if let Some(position) = self.unnamed.iter().position(|candidate| candidate == key)
            && let Some(key) = self.unnamed.remove(position)
        {
            self.unnamed.push_back(key);
        }
    }
}

impl Default for GithubBreakerRegistry {
    fn default() -> Self {
        Self::new(BreakerConfig::default())
    }
}

/// One row of [`GithubBreakerRegistry::snapshot`]: the breaker state of a
/// single credential bucket. Serializable so the operational snapshot and
/// the webhooks health endpoint can publish it as JSON.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct GithubBucketSnapshot {
    pub bucket: String,
    pub breaker: BreakerSnapshot,
}

/// One process's outbound-GitHub funnel: the breaker registry every GitHub
/// call is admitted through, together with the recorder its spend is
/// attributed to.
///
/// The two travel as one value because a call admitted by one registry but
/// accounted in another would report a budget it never spent. [`crate::state::AppState`]
/// holds a funnel, and so does anything outside it that talks to GitHub on the
/// engine's behalf — the runner pool downloads the packed golden over GitHub
/// before a VM ever boots, and that traffic must resolve through the same
/// breakers as the webhook path or a GitHub outage is discovered once per
/// caller again. Cloning a funnel shares it; [`GithubFunnel::default`] builds a
/// private one, which is what a caller with no engine to share with (the
/// standalone one-shot CLI paths, tests) gets.
#[derive(Debug, Clone)]
pub struct GithubFunnel {
    pub breakers: Arc<GithubBreakerRegistry>,
    pub consumption: Arc<GithubConsumption>,
}

impl Default for GithubFunnel {
    fn default() -> Self {
        Self {
            breakers: Arc::new(GithubBreakerRegistry::default()),
            consumption: Arc::new(GithubConsumption::default()),
        }
    }
}

impl GithubFunnel {
    /// A funnel whose breakers use the configured overrides
    /// (`PRELOOP_GITHUB_BREAKER`, `PRELOOP_GITHUB_BREAKER_THRESHOLD`), so the
    /// engine that shares this funnel and the registry built inside
    /// [`crate::AppState::new`] agree on when GitHub counts as down.
    pub fn from_overrides(enabled: Option<bool>, threshold: Option<u32>) -> Self {
        Self {
            breakers: Arc::new(GithubBreakerRegistry::new(BreakerConfig::from_overrides(
                enabled, threshold,
            ))),
            consumption: Arc::new(GithubConsumption::default()),
        }
    }
}

/// Classify a GitHub response status as a dependency failure.
///
/// A 403 is only a rate limit when GitHub says so — `x-ratelimit-remaining: 0`
/// or a `retry-after` header. A plain 403 is a permission problem, which is
/// the delivery's fault and must dead-letter normally instead of parking the
/// whole queue behind a breaker.
pub fn classify_status(
    status: axum::http::StatusCode,
    headers: &HeaderMap,
) -> Option<GithubFailureKind> {
    let retry_after = retry_after_from_headers(headers);
    if status == axum::http::StatusCode::TOO_MANY_REQUESTS {
        return Some(GithubFailureKind::RateLimited { retry_after });
    }
    if status == axum::http::StatusCode::FORBIDDEN {
        let exhausted = header_str(headers, "x-ratelimit-remaining")
            .and_then(|value| value.trim().parse::<i64>().ok())
            .is_some_and(|remaining| remaining <= 0);
        if exhausted || retry_after.is_some() {
            return Some(GithubFailureKind::RateLimited { retry_after });
        }
        return None;
    }
    if status.is_server_error() {
        return Some(GithubFailureKind::Unavailable);
    }
    None
}

/// Resume time from `retry-after` (delta seconds) or `x-ratelimit-reset`
/// (absolute unix seconds), whichever is present.
fn retry_after_from_headers(headers: &HeaderMap) -> Option<Duration> {
    if let Some(seconds) = header_str(headers, "retry-after")
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
    {
        return Some(Duration::from_secs(seconds));
    }
    let reset_at = header_str(headers, "x-ratelimit-reset")
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    reset_at
        .checked_sub(now)
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

fn header_str<'headers>(headers: &'headers HeaderMap, name: &str) -> Option<&'headers str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// A GitHub call refused by an open breaker before anything reached GitHub.
///
/// Typed so callers can tell a deferral (no budget spent; retry once the wait
/// has passed) from a failure by downcasting, rather than matching on text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakerOpen {
    /// How long until GitHub work is worth attempting again.
    pub retry_after: Duration,
}

impl std::fmt::Display for BreakerOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GitHub circuit breaker is open (retry after {}s)",
            self.retry_after.as_secs_f64()
        )
    }
}

impl std::error::Error for BreakerOpen {}

/// Send a GitHub request through the calling credential's breaker.
///
/// Breaker coverage without consumption accounting, for the pre-`AppState`
/// bootstrap paths (GitHub App startup reads) that have no consumption
/// recorder yet. Everything that runs with an `AppState` should prefer
/// [`send_observed_labeled`].
///
/// Every GitHub call on the webhook path goes through here so the breaker
/// sees the whole dependency, not the one endpoint someone remembered to
/// instrument. The response is returned untouched: a 404 still means what
/// it meant to the caller, it just also counts as proof GitHub is up.
pub async fn send_observed(
    registry: &GithubBreakerRegistry,
    bucket: GithubCredentialBucket,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let breaker = registry.breaker_for(bucket);
    if let Err(remaining) = breaker.acquire() {
        return Err(anyhow::Error::new(BreakerOpen {
            retry_after: remaining,
        }));
    }
    match request.send().await {
        Ok(response) => {
            breaker.observe_status(response.status(), response.headers());
            Ok(response)
        }
        Err(error) => {
            breaker.observe_transport_error(&error);
            Err(error.into())
        }
    }
}

// ---------------------------------------------------------------------------
// Unified outbound GitHub client: breaker coverage + per-subsystem
// consumption accounting for every GitHub call, not just the check-runs
// reporting path.
// ---------------------------------------------------------------------------

/// Which part of the server is spending the GitHub API budget. Labels stay a
/// small closed set so counters and traces never grow a series per repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GithubSubsystem {
    /// Check-run / check-suite reporting (`github.rs` sender path).
    CheckRuns,
    /// Action tarball downloads (`actions.rs`).
    Actions,
    /// Action ref → SHA resolution (`actions.rs`).
    RefResolve,
    /// Environment protection-rule resolution (`environment_resolver.rs`).
    Env,
    /// Snapshot checkout-cache and on-demand LFS fetches (`snapshots.rs`).
    Snapshots,
    /// Reusable-workflow expansion and PR/workflow discovery (`github.rs`,
    /// `remote_workflows.rs`).
    Workflows,
    /// Webhook delivery management (watchdog repair, health checks).
    Webhooks,
    /// App-JWT calls that authenticate the App itself: installation lookup and
    /// listing, installation reads, and installation-token minting
    /// (`github_app.rs`), plus the one-time manifest code exchange that
    /// registers the App (`github.rs`).
    AppAuth,
    /// Packed-golden provisioning in the orchestrator: the release listing,
    /// the OCI manifest and token lookups, `.sha256` sidecars, and the golden
    /// transfers themselves (`preloop-orchestrator/src/lib.rs`).
    Golden,
    /// GitHub-compatible dispatch: auth validation and dispatch-target
    /// resolution (`dispatch_auth.rs`, `dispatch.rs`).
    Dispatch,
    /// Push-back sync and webhook-driven auto-PR (`github_push.rs`,
    /// `github_pr.rs`).
    PushBack,
    /// Credential introspection that is not an App JWT: the static PAT's
    /// classic OAuth scope probe (`runs.rs`).
    Credentials,
}

impl GithubSubsystem {
    pub fn as_str(&self) -> &'static str {
        match self {
            GithubSubsystem::CheckRuns => "check-runs",
            GithubSubsystem::Actions => "actions",
            GithubSubsystem::RefResolve => "ref-resolve",
            GithubSubsystem::Env => "env",
            GithubSubsystem::Snapshots => "snapshots",
            GithubSubsystem::Workflows => "workflows",
            GithubSubsystem::Webhooks => "webhooks",
            GithubSubsystem::AppAuth => "app-auth",
            GithubSubsystem::Golden => "golden",
            GithubSubsystem::Dispatch => "dispatch",
            GithubSubsystem::PushBack => "push-back",
            GithubSubsystem::Credentials => "credentials",
        }
    }

    const ALL: [GithubSubsystem; 12] = [
        GithubSubsystem::CheckRuns,
        GithubSubsystem::Actions,
        GithubSubsystem::RefResolve,
        GithubSubsystem::Env,
        GithubSubsystem::Snapshots,
        GithubSubsystem::Workflows,
        GithubSubsystem::Webhooks,
        GithubSubsystem::AppAuth,
        GithubSubsystem::Golden,
        GithubSubsystem::Dispatch,
        GithubSubsystem::PushBack,
        GithubSubsystem::Credentials,
    ];
}

/// Per-subsystem counters for one [`GithubConsumption`].
#[derive(Debug, Default)]
struct SubsystemCounters {
    /// Completed HTTP exchanges actually sent to GitHub.
    requests: std::sync::atomic::AtomicU64,
    /// Response body bytes attributed to the subsystem (advertised
    /// `content-length`, or the exact streamed byte count where the caller
    /// measures it).
    bytes: std::sync::atomic::AtomicU64,
    /// Response body bytes from presigned cross-host downloads (LFS blobs):
    /// real bandwidth the server spent, but not GitHub API budget, so kept
    /// out of `bytes`.
    cross_host_bytes: std::sync::atomic::AtomicU64,
    /// Exchanges GitHub answered with a rate limit (429, or 403 with an
    /// exhausted budget).
    rate_limited: std::sync::atomic::AtomicU64,
    /// Attempts refused without touching GitHub because the breaker was open.
    breaker_blocked: std::sync::atomic::AtomicU64,
}

/// Process-wide consumption accounting for outbound GitHub traffic.
///
/// Lives in [`crate::state::AppState`] next to the breaker (atomics only, no
/// lock), so every subsystem's spend is visible in one place: the
/// operational snapshot, the webhooks health endpoint, and tracing events.
/// This is deliberately not a second metrics framework — it mirrors the
/// [`BreakerSnapshot`] pattern the breaker already uses for operational
/// visibility.
#[derive(Debug, Default)]
pub struct GithubConsumption {
    counters: [SubsystemCounters; GithubSubsystem::ALL.len()],
}

impl GithubConsumption {
    fn counters(&self, subsystem: GithubSubsystem) -> &SubsystemCounters {
        let index = GithubSubsystem::ALL
            .iter()
            .position(|candidate| *candidate == subsystem)
            .expect("every GithubSubsystem is in ALL");
        &self.counters[index]
    }

    /// One HTTP exchange was sent to GitHub for `subsystem`.
    pub fn record_request(&self, subsystem: GithubSubsystem) {
        self.counters(subsystem)
            .requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Attribute `bytes` of response body to `subsystem`.
    pub fn record_bytes(&self, subsystem: GithubSubsystem, bytes: u64) {
        self.counters(subsystem)
            .bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Attribute `bytes` downloaded from a presigned cross-host URL to
    /// `subsystem`. Kept separate from [`GithubConsumption::record_bytes`]:
    /// the bytes are real bandwidth but spend no GitHub API budget, so
    /// mixing them into the GitHub byte totals would lie about budget spend.
    pub fn record_cross_host_bytes(&self, subsystem: GithubSubsystem, bytes: u64) {
        self.counters(subsystem)
            .cross_host_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Attribute a response's advertised body size (`content-length`, when
    /// present) to `subsystem`. Call before consuming the body. Streaming
    /// callers that measure the exact byte count should use
    /// [`GithubConsumption::record_bytes`] after the stream instead — not
    /// both.
    pub fn record_advertised_bytes(
        &self,
        subsystem: GithubSubsystem,
        response: &reqwest::Response,
    ) {
        let bytes = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if bytes > 0 {
            self.record_bytes(subsystem, bytes);
        }
    }

    /// GitHub answered with a rate limit for `subsystem`.
    pub fn record_rate_limited(&self, subsystem: GithubSubsystem) {
        self.counters(subsystem)
            .rate_limited
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// An attempt for `subsystem` was refused because the breaker was open —
    /// no GitHub budget was spent.
    pub fn record_breaker_blocked(&self, subsystem: GithubSubsystem) {
        self.counters(subsystem)
            .breaker_blocked
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Point-in-time per-subsystem usage, in [`GithubSubsystem::ALL`] order.
    pub fn snapshot(&self) -> Vec<GithubSubsystemUsage> {
        GithubSubsystem::ALL
            .iter()
            .map(|subsystem| {
                let counters = self.counters(*subsystem);
                let load = |counter: &std::sync::atomic::AtomicU64| {
                    counter.load(std::sync::atomic::Ordering::Relaxed)
                };
                GithubSubsystemUsage {
                    subsystem: subsystem.as_str(),
                    requests: load(&counters.requests),
                    bytes: load(&counters.bytes),
                    cross_host_bytes: load(&counters.cross_host_bytes),
                    rate_limited: load(&counters.rate_limited),
                    breaker_blocked: load(&counters.breaker_blocked),
                }
            })
            .collect()
    }
}

/// One row of [`GithubConsumption::snapshot`]: the GitHub spend of a single
/// subsystem. Serializable so the operational snapshot and the webhooks
/// health endpoint can publish it as JSON.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct GithubSubsystemUsage {
    pub subsystem: &'static str,
    pub requests: u64,
    pub bytes: u64,
    pub cross_host_bytes: u64,
    pub rate_limited: u64,
    pub breaker_blocked: u64,
}

/// Send a GitHub request through the calling credential's breaker with
/// per-subsystem consumption accounting.
///
/// This is the one funnel for outbound GitHub traffic: the breaker sees the
/// whole dependency partitioned by credential *at the engine's own GitHub
/// origin* (a 429 on installation A's action tarball parks installation A's
/// check runs too, because the installation budget they share is what is
/// actually exhausted — but installation B and the PAT keep flowing), and the
/// accounting attributes every request, byte, and rate limit to the subsystem
/// that caused it. Calls to any other host go through
/// [`send_observed_labeled_at`], which adds that host to the partition.
/// Response semantics are unchanged from [`send_observed`]: a 404 still
/// means what it meant to the caller, it just also proves GitHub is up.
///
/// Beyond [`send_observed`], the primary rate budget advertised on the
/// response (`x-ratelimit-remaining: 0` + `x-ratelimit-reset`) is honoured
/// for every subsystem, so a call that spends the last request parks the
/// next one instead of burning it on a guaranteed 403.
pub async fn send_observed_labeled(
    registry: &GithubBreakerRegistry,
    bucket: GithubCredentialBucket,
    consumption: &GithubConsumption,
    subsystem: GithubSubsystem,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let origin = registry.github_origin.clone();
    send_observed_labeled_at(
        registry,
        &origin,
        bucket,
        consumption,
        subsystem,
        &[],
        request,
    )
    .await
}

/// [`send_observed_labeled`] for a call to `origin`, with statuses the caller
/// knows are protocol steps rather than verdicts.
///
/// `origin` is the service the request actually reaches
/// ([`service_origin`]): the engine's own GitHub origin for a call to the
/// configured API, or another host when the caller talks to one on GitHub's
/// behalf — a golden mirror, a container registry, a token realm. Breaker state
/// is per `(origin, bucket)`, so an outage or a rate limit at one host never
/// parks traffic to another.
///
/// A status in `expect` is accounted and returned like any other, but it
/// neither proves nor disproves reachability, so it leaves the breaker's
/// failure streak alone. That is what keeps a multi-step protocol honest: the
/// anonymous `Bearer` handshake answers the manifest request with `401` before
/// it will hand out a token, and recording that expected challenge as a success
/// would clear the streak the failing token endpoint accumulates, so the
/// endpoint's outage would never trip the breaker.
pub async fn send_observed_labeled_at(
    registry: &GithubBreakerRegistry,
    origin: &str,
    bucket: GithubCredentialBucket,
    consumption: &GithubConsumption,
    subsystem: GithubSubsystem,
    expect: &[axum::http::StatusCode],
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let breaker = registry.breaker_at(origin, bucket);
    if let Err(remaining) = breaker.acquire() {
        consumption.record_breaker_blocked(subsystem);
        tracing::debug!(
            subsystem = subsystem.as_str(),
            bucket = bucket.label(),
            origin,
            retry_in_secs = remaining.as_secs(),
            "GitHub request refused without touching GitHub: circuit breaker open"
        );
        return Err(anyhow::Error::new(BreakerOpen {
            retry_after: remaining,
        }));
    }
    match request.send().await {
        Ok(response) => {
            consumption.record_request(subsystem);
            let status = response.status();
            if expect.contains(&status) {
                // A protocol step, not a verdict about the dependency: the
                // exchange still spent the host's budget, but it must not
                // record a success — that would clear an accumulating failure
                // streak (or close the breaker) on the strength of a challenge
                // the caller expects to see before its real call is made.
                breaker.observe_rate_budget(response.headers());
                return Ok(response);
            }
            let kind = breaker.observe_status(status, response.headers());
            if matches!(kind, Some(GithubFailureKind::RateLimited { .. })) {
                consumption.record_rate_limited(subsystem);
                tracing::warn!(
                    subsystem = subsystem.as_str(),
                    bucket = bucket.label(),
                    %status,
                    "GitHub rate-limited a request; breaker is backing off"
                );
            } else {
                // Honour the primary budget advertised on the response: once
                // remaining hits zero, wait for the reset instead of spending
                // the next call on a guaranteed 403. This runs only when the
                // status itself did not already rate-limit: a 429 that also
                // advertises `x-ratelimit-remaining: 0` must trip the breaker
                // once, not twice (a second open would double-count the trip
                // and double-advance the backoff streak).
                breaker.observe_rate_budget(response.headers());
            }
            Ok(response)
        }
        Err(error) => {
            breaker.observe_transport_error(&error);
            // No HTTP exchange completed (DNS, connect, TLS), so there is
            // no GitHub request to account for: counting it here would
            // inflate the request totals during connectivity outages.
            Err(error.into())
        }
    }
}

// ---------------------------------------------------------------------------
// The engine's GitHub REST funnel: one credential type, one send, one decode.
// ---------------------------------------------------------------------------

/// A GitHub credential and the rate-limit bucket it spends.
///
/// The two travel together on purpose: a call that authenticates with a token
/// must be admitted, refused, and accounted for under that credential's own
/// budget, and passing them separately is how a call ends up observed against
/// the wrong installation (or, worse, against the anonymous bucket while
/// spending an installation's budget).
///
/// The token is a [`SecretString`], so `Debug` prints `<redacted>`: this value
/// is logged in every refusal and outage warning, and a bearer in the log is
/// an operator PAT or a live installation token for whoever can read logs.
#[derive(Debug, Clone)]
pub struct GithubCredential {
    token: Option<SecretString>,
    bucket: GithubCredentialBucket,
}

impl GithubCredential {
    /// A bearer credential: the static PAT, an installation token, or an App
    /// JWT.
    pub fn bearer(token: impl Into<String>, bucket: GithubCredentialBucket) -> Self {
        Self {
            token: Some(SecretString::new(token)),
            bucket,
        }
    }

    /// No credential of the engine's, charged to `bucket`.
    ///
    /// The forge relay needs this: it forwards anonymous Git traffic, which
    /// the forge meters separately from the anonymous REST budget, so it
    /// carries no credential but its own bucket.
    pub fn unauthenticated(bucket: GithubCredentialBucket) -> Self {
        Self {
            token: None,
            bucket,
        }
    }

    /// No credential at all: public-repository access, charged to GitHub's
    /// 60-requests-per-hour-per-IP anonymous budget.
    pub fn anonymous() -> Self {
        Self::unauthenticated(GithubCredentialBucket::Anonymous)
    }

    /// The breaker bucket this credential's calls are keyed on.
    pub fn bucket(&self) -> GithubCredentialBucket {
        self.bucket
    }

    /// Attach the `Authorization` header for this credential.
    ///
    /// An anonymous credential contributes no header, which is what the forge
    /// relay and the App-manifest exchange need: both must reach GitHub with
    /// no credential of the engine's attached. This is the credential's one
    /// expose, at the header the request is actually sent with.
    pub fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.header("Authorization", format!("Bearer {}", token.expose())),
            None => request,
        }
    }
}

/// GitHub answered an engine API call with a non-success status.
///
/// Typed so a caller can branch on the status without parsing text (dispatch
/// auth turns a 401/403 into a refused credential and everything else into a
/// gateway failure), while [`std::fmt::Display`] keeps the `status {code}`
/// wording the push-back classifier already reads.
#[derive(Debug)]
pub struct GithubApiError {
    method: reqwest::Method,
    url: String,
    /// The status GitHub answered with.
    pub status: reqwest::StatusCode,
    /// The response body, truncated to [`GithubApiError::BODY_LIMIT`] chars:
    /// enough to explain the refusal, bounded because it lands in logs and
    /// error messages.
    pub body: String,
}

impl GithubApiError {
    const BODY_LIMIT: usize = 1024;

    fn new(method: reqwest::Method, url: &str, status: reqwest::StatusCode, body: String) -> Self {
        Self {
            method,
            url: url.to_owned(),
            status,
            body: body.chars().take(Self::BODY_LIMIT).collect(),
        }
    }
}

impl std::fmt::Display for GithubApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GitHub API {} {} failed with status {}: {}",
            self.method,
            self.url,
            self.status.as_u16(),
            self.body
        )
    }
}

impl std::error::Error for GithubApiError {}

/// Send one outbound GitHub request through the calling credential's breaker
/// with per-subsystem consumption accounting.
///
/// This is [`send_observed_labeled`] with the engine's credential in hand, so
/// no call site has to resolve the bucket and the bearer separately. Use it
/// for calls that need their own client, headers, or response handling — a
/// streaming relay, a header-only read, a per-request timeout;
/// [`send_github_json`] covers the engine's standard JSON call.
///
/// A refusal while the breaker is open is an error downcastable to
/// [`BreakerOpen`], and no GitHub budget was spent: callers that decide their
/// own retry (the queue parks work, best-effort lookups fall back) can tell
/// that case apart from a real failure.
pub async fn send_github(
    state: &crate::state::AppState,
    credential: &GithubCredential,
    subsystem: GithubSubsystem,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    let response = send_observed_labeled(
        &state.github_breakers,
        credential.bucket(),
        &state.github_consumption,
        subsystem,
        credential.authorize(request),
    )
    .await?;
    state
        .github_consumption
        .record_advertised_bytes(subsystem, &response);
    Ok(response)
}

/// Send one engine-standard GitHub REST JSON call through the breaker.
///
/// The shape every JSON call in the engine shares: the shared outbound client,
/// the standard GitHub headers, the credential's bearer (none when anonymous),
/// an optional JSON body, and a decoded body. A non-success status becomes a
/// [`GithubApiError`] carrying the status and the response text, so callers
/// keep their own mapping (fail closed, 404, 500) without re-parsing.
pub async fn send_github_json(
    state: &crate::state::AppState,
    credential: &GithubCredential,
    subsystem: GithubSubsystem,
    method: reqwest::Method,
    url: &str,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    let mut request = crate::shared_http::CLIENT
        .request(method.clone(), url)
        .header("User-Agent", "preloop")
        .header("Accept", "application/vnd.github+json");
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = send_github(state, credential, subsystem, request).await?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(GithubApiError::new(method, url, status, text).into());
    }
    serde_json::from_str(&text).map_err(|error| {
        anyhow::anyhow!("GitHub API {method} {url} returned a non-JSON body: {error}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_config() -> BreakerConfig {
        BreakerConfig {
            enabled: true,
            failure_threshold: 3,
            base_open: Duration::from_millis(60),
            max_open: Duration::from_millis(240),
            probe_timeout: Duration::from_millis(500),
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        headers
    }

    #[test]
    fn stays_closed_below_threshold() {
        let breaker = GithubBreaker::new(test_config());
        breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        assert!(breaker.acquire().is_ok());
        assert!(!breaker.snapshot().open);
    }

    #[test]
    fn opens_at_threshold_and_admits_one_probe_after_the_window() {
        let breaker = GithubBreaker::new(test_config());
        for _ in 0..3 {
            breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        assert!(breaker.acquire().is_err(), "breaker must refuse while open");
        std::thread::sleep(Duration::from_millis(80));
        assert!(breaker.acquire().is_ok(), "probe must be admitted");
        assert!(
            breaker.retry_after().is_some(),
            "retry_after must report remaining probe timeout while probe is live"
        );
        assert!(
            breaker.acquire().is_err(),
            "a second caller must wait for the probe verdict"
        );
        breaker.record_success();
        assert!(breaker.retry_after().is_none());
        assert!(breaker.acquire().is_ok());
        assert!(!breaker.snapshot().open);
    }

    #[test]
    fn open_window_grows_with_consecutive_trips() {
        let breaker = GithubBreaker::new(test_config());
        for _ in 0..3 {
            breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        let first = breaker.retry_after().expect("open");
        for _ in 0..3 {
            breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        let second = breaker.retry_after().expect("open");
        assert!(
            second > first,
            "second trip must back off further: {second:?} <= {first:?}"
        );
        assert!(second <= test_config().max_open);
    }

    #[test]
    fn rate_limit_opens_immediately_for_the_advertised_window() {
        let breaker = GithubBreaker::new(test_config());
        let kind = classify_status(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            &headers(&[("retry-after", "2")]),
        )
        .expect("429 is a rate limit");
        breaker.record_failure(&kind, "429");
        let remaining = breaker.retry_after().expect("open");
        assert!(
            remaining > Duration::from_millis(1500),
            "must honour retry-after, got {remaining:?}"
        );
        assert!(breaker.snapshot().rate_limited);
    }

    #[test]
    fn plain_403_is_not_a_dependency_failure() {
        assert_eq!(
            classify_status(axum::http::StatusCode::FORBIDDEN, &HeaderMap::new()),
            None
        );
        assert!(matches!(
            classify_status(
                axum::http::StatusCode::FORBIDDEN,
                &headers(&[("x-ratelimit-remaining", "0")])
            ),
            Some(GithubFailureKind::RateLimited { .. })
        ));
    }

    #[test]
    fn client_errors_count_as_reachability_successes() {
        let breaker = GithubBreaker::new(test_config());
        breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        assert_eq!(
            breaker.observe_status(axum::http::StatusCode::NOT_FOUND, &HeaderMap::new()),
            None
        );
        assert_eq!(breaker.snapshot().consecutive_failures, 0);
    }

    #[test]
    fn ratelimit_reset_header_is_converted_to_a_delay() {
        let reset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 30;
        let kind = classify_status(
            axum::http::StatusCode::FORBIDDEN,
            &headers(&[
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset", &reset.to_string()),
            ]),
        );
        match kind {
            Some(GithubFailureKind::RateLimited {
                retry_after: Some(delay),
            }) => assert!(delay > Duration::from_secs(25) && delay <= Duration::from_secs(30)),
            other => panic!("expected a rate limit with a delay, got {other:?}"),
        }
    }

    #[test]
    fn disabled_breaker_never_opens_but_still_counts() {
        let breaker = GithubBreaker::new(BreakerConfig {
            enabled: false,
            ..test_config()
        });
        for _ in 0..10 {
            breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        assert!(breaker.acquire().is_ok());
        let snapshot = breaker.snapshot();
        assert!(!snapshot.open);
        assert_eq!(snapshot.consecutive_failures, 10);
    }

    #[test]
    fn subsystem_labels_are_a_small_closed_set() {
        let labels: Vec<&str> = GithubSubsystem::ALL
            .iter()
            .map(GithubSubsystem::as_str)
            .collect();
        assert_eq!(
            labels,
            vec![
                "check-runs",
                "actions",
                "ref-resolve",
                "env",
                "snapshots",
                "workflows",
                "webhooks",
                "app-auth",
                "golden",
                "dispatch",
                "push-back",
                "credentials"
            ]
        );
    }

    #[test]
    fn consumption_snapshot_starts_at_zero_for_every_subsystem() {
        let consumption = GithubConsumption::default();
        let snapshot = consumption.snapshot();
        assert_eq!(snapshot.len(), GithubSubsystem::ALL.len());
        for usage in &snapshot {
            assert_eq!(usage.requests, 0);
            assert_eq!(usage.bytes, 0);
            assert_eq!(usage.cross_host_bytes, 0);
            assert_eq!(usage.rate_limited, 0);
            assert_eq!(usage.breaker_blocked, 0);
        }
    }

    /// An App's JWT budget is its own: App 1's 429 parks App 1 and leaves
    /// App 2 flowing.
    #[test]
    fn app_jwt_budget_is_partitioned_by_app_id() {
        let registry = GithubBreakerRegistry::new(test_config());
        registry
            .breaker_for(GithubCredentialBucket::App(1))
            .record_failure(
                &GithubFailureKind::RateLimited {
                    retry_after: Some(Duration::from_secs(60)),
                },
                "429",
            );
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::App(1))
                .is_open()
        );
        assert!(
            !registry
                .breaker_for(GithubCredentialBucket::App(2))
                .is_open(),
            "App 2 must keep flowing while App 1 is rate limited"
        );
    }

    #[test]
    fn numeric_app_id_names_its_bucket_directly() {
        assert_eq!(
            GithubCredentialBucket::for_app("123"),
            GithubCredentialBucket::App(123)
        );
        assert_eq!(GithubCredentialBucket::App(123).label(), "app:123");
    }

    #[test]
    fn non_numeric_app_id_maps_to_a_stable_distinct_bucket() {
        let bucket = GithubCredentialBucket::for_app("emulated-app");
        assert!(matches!(bucket, GithubCredentialBucket::App(_)));
        assert_eq!(bucket, GithubCredentialBucket::for_app("emulated-app"));
        assert_ne!(
            bucket,
            GithubCredentialBucket::for_app("other-emulated-app")
        );
    }

    /// What one request to [`StubGitHub`] carried, so a test can assert what
    /// the engine actually sent — in particular that a credential went to the
    /// host it was minted for and nowhere else.
    #[derive(Debug, Clone)]
    struct StubRequest {
        method: String,
        uri: String,
        authorization: Option<String>,
    }

    /// A tiny stub GitHub: serves scripted responses and counts hits, so
    /// tests prove the breaker engages without touching the real github.com.
    struct StubGitHub {
        base: String,
        hits: Arc<std::sync::atomic::AtomicU64>,
        requests: Arc<std::sync::Mutex<Vec<StubRequest>>>,
    }

    impl StubGitHub {
        async fn serve(
            status: axum::http::StatusCode,
            headers: Vec<(String, String)>,
            body: &'static str,
        ) -> Self {
            use std::sync::atomic::Ordering;
            let hits = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let hits_route = hits.clone();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let requests_route = requests.clone();
            let app = axum::Router::new().route(
                "/{*path}",
                axum::routing::any(move |request: axum::extract::Request| {
                    let hits_route = hits_route.clone();
                    let requests_route = requests_route.clone();
                    async move {
                        hits_route.fetch_add(1, Ordering::SeqCst);
                        requests_route.lock().unwrap().push(StubRequest {
                            method: request.method().to_string(),
                            uri: request.uri().to_string(),
                            authorization: request
                                .headers()
                                .get(axum::http::header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                        });
                        let mut builder = axum::http::Response::builder().status(status);
                        for (name, value) in &headers {
                            builder = builder.header(name, value);
                        }
                        builder.body(axum::body::Body::from(body)).unwrap()
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self {
                base,
                hits,
                requests,
            }
        }

        fn count(&self) -> u64 {
            self.hits.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn requests(&self) -> Vec<StubRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[tokio::test]
    async fn rate_limited_download_opens_breaker_and_is_accounted() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let stub = StubGitHub::serve(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            vec![("x-ratelimit-reset".to_owned(), reset.to_string())],
            "rate limited",
        )
        .await;
        let breaker = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        // First exchange: the 429 reaches GitHub, trips the breaker
        // immediately (rate limits do not wait for the failure threshold),
        // and is accounted to the actions subsystem.
        let response = send_observed_labeled(
            &breaker,
            GithubCredentialBucket::Pat,
            &consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/tarball/main", stub.base)),
        )
        .await
        .expect("the 429 response itself is returned, not an error");
        assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert!(
            breaker.breaker_for(GithubCredentialBucket::Pat).is_open(),
            "a 429 must open the breaker at once"
        );
        assert!(
            breaker
                .breaker_for(GithubCredentialBucket::Pat)
                .snapshot()
                .rate_limited
        );

        // Second attempt: refused without touching GitHub — the hammering
        // the pre-fix code path did is exactly what the breaker now stops.
        let error = send_observed_labeled(
            &breaker,
            GithubCredentialBucket::Pat,
            &consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/tarball/main", stub.base)),
        )
        .await
        .expect_err("breaker-open must fail fast");
        assert!(
            error
                .downcast_ref::<BreakerOpen>()
                .is_some_and(|open| open.retry_after > Duration::ZERO),
            "a refusal must carry its typed wait, got: {error:#}"
        );
        assert_eq!(
            stub.hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the first exchange may reach GitHub"
        );

        let usage: std::collections::HashMap<&str, GithubSubsystemUsage> = consumption
            .snapshot()
            .into_iter()
            .map(|usage| (usage.subsystem, usage))
            .collect();
        let actions = &usage["actions"];
        assert_eq!(actions.requests, 1);
        assert_eq!(actions.rate_limited, 1);
        assert_eq!(actions.breaker_blocked, 1);
        // Other subsystems stay untouched: attribution, not a global count.
        for (label, usage) in &usage {
            if *label != "actions" {
                assert_eq!(usage.requests, 0, "{label} must not be charged");
            }
        }
    }

    #[tokio::test]
    async fn successful_response_records_advertised_bytes() {
        let body = "hello-github";
        let stub = StubGitHub::serve(
            axum::http::StatusCode::OK,
            vec![("content-length".to_owned(), body.len().to_string())],
            body,
        )
        .await;
        let breaker = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        let response = send_observed_labeled(
            &breaker,
            GithubCredentialBucket::Pat,
            &consumption,
            GithubSubsystem::RefResolve,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/commits/main", stub.base)),
        )
        .await
        .unwrap();
        consumption.record_advertised_bytes(GithubSubsystem::RefResolve, &response);
        assert!(!breaker.breaker_for(GithubCredentialBucket::Pat).is_open());

        let usage: std::collections::HashMap<&str, GithubSubsystemUsage> = consumption
            .snapshot()
            .into_iter()
            .map(|usage| (usage.subsystem, usage))
            .collect();
        let ref_resolve = &usage["ref-resolve"];
        assert_eq!(ref_resolve.requests, 1);
        assert_eq!(ref_resolve.bytes, body.len() as u64);
        assert_eq!(ref_resolve.rate_limited, 0);
    }

    #[tokio::test]
    async fn exhausted_primary_budget_parks_the_next_call() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let stub = StubGitHub::serve(
            axum::http::StatusCode::OK,
            vec![
                ("x-ratelimit-remaining".to_owned(), "0".to_owned()),
                ("x-ratelimit-reset".to_owned(), reset.to_string()),
                ("content-length".to_owned(), "2".to_owned()),
            ],
            "ok",
        )
        .await;
        let breaker = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        // A 200 that spends the last request still parks the breaker: the
        // next call would be a guaranteed 403.
        send_observed_labeled(
            &breaker,
            GithubCredentialBucket::Installation(42),
            &consumption,
            GithubSubsystem::Env,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/environments/prod", stub.base)),
        )
        .await
        .unwrap();
        assert!(
            breaker
                .breaker_for(GithubCredentialBucket::Installation(42))
                .is_open(),
            "remaining=0 on a 200 must park the breaker until the reset"
        );
        let usage: std::collections::HashMap<&str, GithubSubsystemUsage> = consumption
            .snapshot()
            .into_iter()
            .map(|usage| (usage.subsystem, usage))
            .collect();
        assert_eq!(usage["env"].requests, 1);
        assert_eq!(usage["env"].rate_limited, 0);
    }

    /// A 429 for one credential parks only that credential's bucket:
    /// installation B and the PAT keep flowing on their own budgets.
    #[tokio::test]
    async fn breaker_state_is_partitioned_by_credential_bucket() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let stub = StubGitHub::serve(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            vec![("x-ratelimit-reset".to_owned(), reset.to_string())],
            "rate limited",
        )
        .await;
        let registry = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        send_observed_labeled(
            &registry,
            GithubCredentialBucket::Installation(1),
            &consumption,
            GithubSubsystem::CheckRuns,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r", stub.base)),
        )
        .await
        .expect("the 429 response itself is returned");
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::Installation(1))
                .is_open(),
            "installation 1 must be parked"
        );
        // Unrelated budgets are untouched.
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::Installation(2))
                .acquire()
                .is_ok(),
            "installation 2 must still flow"
        );
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::Pat)
                .acquire()
                .is_ok(),
            "the PAT bucket must still flow"
        );
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::Anonymous)
                .acquire()
                .is_ok(),
            "the anonymous bucket must still flow"
        );
        // The operational snapshot reports per-bucket state.
        let open_buckets: Vec<String> = registry
            .snapshot()
            .into_iter()
            .filter(|row| row.breaker.open)
            .map(|row| row.bucket)
            .collect();
        assert_eq!(open_buckets, vec!["installation:1".to_owned()]);
    }

    /// With `PRELOOP_GITHUB_BREAKER=off`, an exhausted primary budget must
    /// not open the breaker either — the disable setting covers the budget
    /// path, not just `record_failure`.
    #[test]
    fn disabled_breaker_ignores_exhausted_rate_budget() {
        let registry = GithubBreakerRegistry::new(BreakerConfig {
            enabled: false,
            ..test_config()
        });
        let reset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        registry
            .breaker_for(GithubCredentialBucket::Pat)
            .observe_rate_budget(&headers(&[
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset", &reset.to_string()),
            ]));
        assert!(
            registry
                .breaker_for(GithubCredentialBucket::Pat)
                .retry_after()
                .is_none(),
            "breaker-off must not park on an exhausted budget"
        );
    }

    /// A 429 that also advertises `x-ratelimit-remaining: 0` trips the
    /// breaker exactly once: the status observation and the budget
    /// observation must not double-count the trip or double-advance the
    /// backoff streak.
    #[tokio::test]
    async fn rate_limited_response_with_exhausted_budget_trips_once() {
        let reset = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let stub = StubGitHub::serve(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            vec![
                ("x-ratelimit-remaining".to_owned(), "0".to_owned()),
                ("x-ratelimit-reset".to_owned(), reset.to_string()),
            ],
            "rate limited",
        )
        .await;
        let registry = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        send_observed_labeled(
            &registry,
            GithubCredentialBucket::Pat,
            &consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/tarball/main", stub.base)),
        )
        .await
        .expect("the 429 response itself is returned");
        let snapshot = registry.breaker_for(GithubCredentialBucket::Pat).snapshot();
        assert_eq!(
            snapshot.trips, 1,
            "one response must record exactly one trip, got {}",
            snapshot.trips
        );
        assert!(snapshot.rate_limited);
    }

    /// A transport failure (no completed HTTP exchange) still feeds the
    /// breaker, but must not inflate the accounted request total.
    #[tokio::test]
    async fn transport_failure_does_not_count_as_a_request() {
        let registry = GithubBreakerRegistry::new(test_config());
        let consumption = GithubConsumption::default();

        // Port 1 on loopback is closed: connection refused, no HTTP exchange.
        let error = send_observed_labeled(
            &registry,
            GithubCredentialBucket::Pat,
            &consumption,
            GithubSubsystem::Env,
            crate::shared_http::CLIENT.get("http://127.0.0.1:1/repos/o/r"),
        )
        .await
        .expect_err("connection refused must fail");
        assert!(
            error.to_string().contains("error sending request"),
            "unexpected error: {error:#}"
        );
        let usage: std::collections::HashMap<&str, GithubSubsystemUsage> = consumption
            .snapshot()
            .into_iter()
            .map(|usage| (usage.subsystem, usage))
            .collect();
        assert_eq!(
            usage["env"].requests, 0,
            "a failed connection is not a completed GitHub exchange"
        );
        // The breaker still saw the dependency failure.
        assert_eq!(
            registry
                .breaker_for(GithubCredentialBucket::Pat)
                .snapshot()
                .consecutive_failures,
            1
        );
    }

    /// A bucket whose open window has elapsed but whose half-open probe is
    /// still running is still down for claiming: the probe's verdict is
    /// pending, and the window-only snapshot flag would miss it.
    #[test]
    fn unavailable_retry_after_covers_a_live_probe() {
        let registry = GithubBreakerRegistry::new(test_config());
        let bucket = GithubCredentialBucket::Installation(7);
        let breaker = registry.breaker_for(bucket);
        for _ in 0..3 {
            breaker.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        std::thread::sleep(Duration::from_millis(80));
        assert!(breaker.acquire().is_ok(), "the probe must be admitted");
        assert!(
            !breaker.snapshot().open,
            "the window has elapsed, so the snapshot reports closed"
        );
        assert!(
            registry.unavailable_retry_after().is_some(),
            "a live probe must keep claiming paused"
        );
    }

    /// Rate limits are per credential: a throttled installation must not
    /// pause claiming for everyone else.
    #[test]
    fn unavailable_retry_after_ignores_rate_limited_buckets() {
        let registry = GithubBreakerRegistry::new(test_config());
        let bucket = GithubCredentialBucket::Installation(7);
        registry.breaker_for(bucket).record_failure(
            &GithubFailureKind::RateLimited {
                retry_after: Some(Duration::from_secs(30)),
            },
            "429",
        );
        assert!(registry.breaker_for(bucket).is_open());
        assert_eq!(registry.unavailable_retry_after(), None);
    }

    #[test]
    fn breaker_refusal_keeps_its_message_text() {
        let refusal = BreakerOpen {
            retry_after: Duration::from_secs(30),
        };
        assert_eq!(
            refusal.to_string(),
            "GitHub circuit breaker is open (retry after 30s)"
        );
    }

    #[test]
    fn service_origin_names_the_host_and_only_a_real_port() {
        assert_eq!(
            service_origin("https://api.github.com/repos/preloopdev/preloop/releases"),
            "api.github.com"
        );
        assert_eq!(
            service_origin("https://API.GitHub.com:443/x"),
            "api.github.com",
            "a port that is only the scheme's default is the same service"
        );
        assert_eq!(
            service_origin("http://127.0.0.1:8123/v2/x"),
            "127.0.0.1:8123"
        );
        assert_eq!(
            service_origin("https://ghcr.io/token?scope=x"),
            "ghcr.io",
            "the query names the token request, not the service"
        );
        // A configured base that is not a URL still names its own partition
        // rather than collapsing into the default one.
        assert_eq!(
            service_origin("ghe.corp.example:8443"),
            "ghe.corp.example:8443"
        );
        assert_eq!(service_origin(""), "");
    }

    /// Breaker state is per `(origin, bucket)`: an outage at one host must not
    /// park another, and the GitHub-wide claim the webhook queue makes speaks
    /// only for the engine's own GitHub origin.
    #[test]
    fn origins_do_not_share_a_partition() {
        let registry = GithubBreakerRegistry::for_origin(test_config(), "api.github.com");
        let anonymous = GithubCredentialBucket::Anonymous;
        for _ in 0..3 {
            registry
                .breaker_at("ghcr.io", anonymous)
                .record_failure(&GithubFailureKind::Unavailable, "503");
        }
        assert!(registry.breaker_at("ghcr.io", anonymous).is_open());
        assert!(
            registry.unavailable_retry_after_at("ghcr.io").is_some(),
            "the host's own traffic is blocked"
        );
        assert!(
            !registry.breaker_for(anonymous).is_open(),
            "public GitHub is a different service and must keep flowing"
        );
        assert_eq!(
            registry.unavailable_retry_after(),
            None,
            "a mirror outage says nothing about whether GitHub work is worth attempting"
        );
        let labels: Vec<String> = registry
            .snapshot()
            .into_iter()
            .map(|snapshot| snapshot.bucket)
            .collect();
        assert_eq!(
            labels,
            vec!["anonymous", "anonymous@ghcr.io"],
            "the engine's own partition keeps the published label; the foreign host is named"
        );
    }

    /// A status the caller expects holds no verdict about reachability: it must
    /// not clear the failure streak a real outage is building. This is the
    /// anonymous `Bearer` handshake — a `401` challenge before the token
    /// request that is the actual call.
    #[tokio::test]
    async fn expected_challenge_status_holds_no_verdict() {
        let stub = StubGitHub::serve(
            axum::http::StatusCode::UNAUTHORIZED,
            vec![(
                "www-authenticate".to_owned(),
                "Bearer realm=\"x\"".to_owned(),
            )],
            "",
        )
        .await;
        let origin = service_origin(&stub.base);
        let registry = GithubBreakerRegistry::for_origin(test_config(), origin.clone());
        let consumption = GithubConsumption::default();
        let anonymous = GithubCredentialBucket::Anonymous;
        let bucket = registry.breaker_for(anonymous);
        for _ in 0..2 {
            bucket.record_failure(&GithubFailureKind::Unavailable, "503");
        }

        let url = format!("{}/v2/x/y/manifests/latest", stub.base);
        send_observed_labeled_at(
            &registry,
            &origin,
            anonymous,
            &consumption,
            GithubSubsystem::Golden,
            &[axum::http::StatusCode::UNAUTHORIZED],
            crate::shared_http::CLIENT.get(&url),
        )
        .await
        .expect("the challenge response itself is returned");

        assert_eq!(
            bucket.snapshot().consecutive_failures,
            2,
            "an expected challenge must not clear the streak the token endpoint is building"
        );
        // The third real failure is what trips it, exactly as it would have
        // without the challenge in between.
        bucket.record_failure(&GithubFailureKind::Unavailable, "503");
        assert!(
            bucket.is_open(),
            "three failures with a challenge between them must still open the breaker"
        );
        assert_eq!(
            consumption
                .snapshot()
                .into_iter()
                .find(|usage| usage.subsystem == "golden")
                .map(|usage| usage.requests),
            Some(1),
            "the challenge is still a completed exchange for accounting"
        );

        // The same 401 without the expectation is a success verdict, which is
        // the behaviour that hid the token outage.
        let cleared = GithubBreakerRegistry::for_origin(test_config(), origin.clone());
        let cleared_bucket = cleared.breaker_for(anonymous);
        for _ in 0..2 {
            cleared_bucket.record_failure(&GithubFailureKind::Unavailable, "503");
        }
        send_observed_labeled_at(
            &cleared,
            &origin,
            anonymous,
            &GithubConsumption::default(),
            GithubSubsystem::Golden,
            &[],
            crate::shared_http::CLIENT.get(&url),
        )
        .await
        .expect("the 401 is returned");
        assert_eq!(
            cleared_bucket.snapshot().consecutive_failures,
            0,
            "an unannounced 401 is treated as reachability, as before"
        );
    }

    // ─── the REST funnel (send_github / send_github_json) ──────────────────

    /// An `AppState` as a running server builds it, so the funnel is driven
    /// against the same breaker registry and consumption counters.
    async fn funnel_state() -> crate::state::AppState {
        let temp = tempfile::tempdir().unwrap();
        crate::state::AppState::new_for_tests(temp.path().to_path_buf())
            .await
            .unwrap()
    }

    /// A failing stub opens the credential's breaker, and the next call is
    /// refused without touching GitHub: the whole point of routing every
    /// engine call through the funnel is that an outage costs one round trip
    /// per credential, not one per caller.
    #[tokio::test]
    async fn an_open_breaker_refuses_a_json_call_without_touching_github() {
        let stub = StubGitHub::serve(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Vec::new(),
            "{\"message\":\"boom\"}",
        )
        .await;
        let state = funnel_state().await;
        let credential = GithubCredential::bearer("ghp_test", GithubCredentialBucket::Pat);
        let url = format!("{}/repos/o/r/commits/main", stub.base);

        for attempt in 0..3 {
            send_github_json(
                &state,
                &credential,
                GithubSubsystem::PushBack,
                reqwest::Method::GET,
                &url,
                None,
            )
            .await
            .map(|body| panic!("attempt {attempt} must fail, decoded {body}"))
            .expect_err("a 500 must fail");
        }
        assert_eq!(stub.count(), 3, "every attempt reached the stub");
        assert_eq!(
            stub.requests()[0].authorization.as_deref(),
            Some("Bearer ghp_test"),
            "the credential must actually reach GitHub"
        );
        let breaker = state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Pat);
        assert!(
            breaker.is_open(),
            "three dependency failures must open the breaker"
        );
        let usage = |subsystem: &str| {
            state
                .github_consumption
                .snapshot()
                .into_iter()
                .find(|usage| usage.subsystem == subsystem)
                .expect("subsystem counters exist")
        };
        assert_eq!(usage("push-back").requests, 3);
        assert_eq!(usage("push-back").breaker_blocked, 0);

        let error = send_github_json(
            &state,
            &credential,
            GithubSubsystem::PushBack,
            reqwest::Method::GET,
            &url,
            None,
        )
        .await
        .expect_err("an open breaker must refuse the call");
        assert!(
            error.downcast_ref::<BreakerOpen>().is_some(),
            "a breaker refusal must stay downcastable: {error:#}"
        );
        assert_eq!(stub.count(), 3, "the refusal must not reach GitHub");
        assert_eq!(usage("push-back").breaker_blocked, 1);
    }

    /// An answer resets the credential's failures, and a bucket parks only its
    /// own credential: GitHub's budgets are per credential, so a throttled PAT
    /// must not stop the anonymous relay — which must also reach the forge
    /// with no credential at all.
    #[tokio::test]
    async fn a_reachable_answer_closes_the_breaker_and_buckets_stay_independent() {
        let failing =
            StubGitHub::serve(axum::http::StatusCode::BAD_GATEWAY, Vec::new(), "{}").await;
        let healthy =
            StubGitHub::serve(axum::http::StatusCode::OK, Vec::new(), "{\"ok\":true}").await;
        let state = funnel_state().await;
        let pat = GithubCredential::bearer("ghp_test", GithubCredentialBucket::Pat);

        for _ in 0..2 {
            let _ = send_github_json(
                &state,
                &pat,
                GithubSubsystem::PushBack,
                reqwest::Method::GET,
                &format!("{}/repos/o/r", failing.base),
                None,
            )
            .await
            .expect_err("a 502 must fail");
        }
        assert_eq!(failing.count(), 2);
        assert!(
            !state
                .github_breakers
                .breaker_for(GithubCredentialBucket::Pat)
                .is_open(),
            "two failures are below the threshold"
        );

        let body = send_github_json(
            &state,
            &pat,
            GithubSubsystem::PushBack,
            reqwest::Method::GET,
            &format!("{}/repos/o/r", healthy.base),
            None,
        )
        .await
        .expect("a 200 must decode");
        assert_eq!(body["ok"], true);
        let breaker = state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Pat);
        assert!(
            !breaker.is_open() && breaker.snapshot().consecutive_failures == 0,
            "an answer must reset the credential's failure count"
        );

        // Same stub, anonymous credential: the funnel authorizes nothing and
        // consults the anonymous bucket, which the PAT's failures never touched.
        let response = send_github(
            &state,
            &GithubCredential::anonymous(),
            GithubSubsystem::Snapshots,
            crate::shared_http::CLIENT
                .get(format!("{}/repos/o/r/tarball/main", healthy.base))
                .header("Accept", "application/vnd.github+json"),
        )
        .await
        .expect("an anonymous relay call must succeed");
        assert!(response.status().is_success());
        assert_eq!(healthy.count(), 2);
        assert!(
            !state
                .github_breakers
                .breaker_for(GithubCredentialBucket::Anonymous)
                .is_open()
        );
        let anonymous = healthy.requests();
        assert_eq!(anonymous.len(), 2);
        assert_eq!(
            anonymous[1].authorization, None,
            "an anonymous credential must not put an Authorization header on the wire"
        );
        assert_eq!(anonymous[1].uri, "/repos/o/r/tarball/main");
    }

    /// A transport failure is a dependency failure too: the engine learns
    /// GitHub is unreachable from the callback, not only from a 5xx body.
    #[tokio::test]
    async fn transport_failures_open_the_credential_breaker() {
        let state = funnel_state().await;
        let token = "ghs_transport_test";
        let credential =
            GithubCredential::bearer(token, GithubCredentialBucket::for_installation_token(token));
        // Port 1 on loopback is closed: connection refused, no HTTP exchange.
        for _ in 0..3 {
            let _ = send_github_json(
                &state,
                &credential,
                GithubSubsystem::Dispatch,
                reqwest::Method::GET,
                "http://127.0.0.1:1/installation",
                None,
            )
            .await
            .expect_err("connection refused must fail");
        }
        assert!(
            state
                .github_breakers
                .breaker_for(GithubCredentialBucket::for_installation_token(token))
                .is_open(),
            "an unreachable GitHub must open the unnamed-installation bucket"
        );
    }

    /// A stub that rate-limits one bearer and answers the other, so a test can
    /// prove two third-party tokens keep independent budgets.
    async fn per_token_rate_limit_stub(throttled_bearer: &'static str) -> String {
        use axum::response::IntoResponse;
        use std::sync::atomic::{AtomicU64, Ordering};
        let hits = Arc::new(AtomicU64::new(0));
        let app = axum::Router::new().route(
            "/installation",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let hits = hits.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    let bearer = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    if bearer == format!("Bearer {throttled_bearer}") {
                        (
                            axum::http::StatusCode::TOO_MANY_REQUESTS,
                            [(axum::http::header::RETRY_AFTER, "60")],
                            axum::Json(serde_json::json!({"message": "API rate limit exceeded"})),
                        )
                            .into_response()
                    } else {
                        axum::Json(serde_json::json!({"id": 7})).into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    /// One third-party installation token's rate limit parks that token, not
    /// every other third-party token: unattributable installation tokens are
    /// partitioned per token, and a 429 is the trigger that used to deny every
    /// tenant sharing the single unnamed bucket.
    #[tokio::test]
    async fn a_rate_limited_third_party_token_does_not_park_another() {
        let throttled_token = "ghs_throttled";
        let healthy_token = "ghs_healthy";
        let base = per_token_rate_limit_stub(throttled_token).await;
        let state = funnel_state().await;
        let throttled = GithubCredential::bearer(
            throttled_token,
            GithubCredentialBucket::for_installation_token(throttled_token),
        );
        let healthy = GithubCredential::bearer(
            healthy_token,
            GithubCredentialBucket::for_installation_token(healthy_token),
        );
        let url = format!("{base}/installation");

        let error = send_github_json(
            &state,
            &throttled,
            GithubSubsystem::Dispatch,
            reqwest::Method::GET,
            &url,
            None,
        )
        .await
        .expect_err("a 429 must fail");
        assert_eq!(
            error
                .downcast_ref::<GithubApiError>()
                .map(|error| error.status),
            Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
        );
        assert!(
            state
                .github_breakers
                .breaker_for(GithubCredentialBucket::for_installation_token(
                    throttled_token
                ))
                .is_open(),
            "a rate limit must park the token that hit it"
        );

        // The second token's budget is its own: its call is admitted, reaches
        // GitHub, and validates while the first one is parked.
        let body = send_github_json(
            &state,
            &healthy,
            GithubSubsystem::Dispatch,
            reqwest::Method::GET,
            &url,
            None,
        )
        .await
        .expect("another token's bucket must not be open");
        assert_eq!(body["id"], 7);
        assert!(
            !state
                .github_breakers
                .breaker_for(GithubCredentialBucket::for_installation_token(
                    healthy_token
                ))
                .is_open(),
            "a 200 must leave the other token's bucket closed"
        );
    }

    /// The relay's Git transport is not the anonymous REST budget: a throttled
    /// anonymous API must not refuse a public checkout, and the two buckets are
    /// independent.
    #[tokio::test]
    async fn relay_and_anonymous_api_buckets_stay_independent() {
        let stub = StubGitHub::serve(axum::http::StatusCode::OK, Vec::new(), "{\"ok\":true}").await;
        let state = funnel_state().await;
        let anonymous = state
            .github_breakers
            .breaker_for(GithubCredentialBucket::Anonymous);
        for _ in 0..3 {
            anonymous.record_failure(&GithubFailureKind::Unavailable, "502");
        }
        assert!(anonymous.is_open(), "the anonymous API budget is parked");

        let response = send_github(
            &state,
            &GithubCredential::unauthenticated(GithubCredentialBucket::Relay),
            GithubSubsystem::Snapshots,
            crate::shared_http::CLIENT.get(format!(
                "{}/o/r/info/refs?service=git-upload-pack",
                stub.base
            )),
        )
        .await
        .expect("an exhausted anonymous REST budget must not refuse a public Git fetch");
        assert!(response.status().is_success());
        assert_eq!(stub.count(), 1, "the relay call reached the forge");
        assert!(
            !state
                .github_breakers
                .breaker_for(GithubCredentialBucket::Relay)
                .is_open(),
            "the relay's own bucket answers this exchange"
        );
        assert_ne!(
            GithubCredentialBucket::Relay.label(),
            GithubCredentialBucket::Anonymous.label(),
            "the two dependencies must not share a bucket"
        );
    }

    /// Bearers are operator PATs and live installation tokens: `Debug` (used in
    /// every refusal and outage log line) must not print them.
    #[test]
    fn debug_never_prints_the_bearer() {
        let credential = GithubCredential::bearer("ghp_secret_value", GithubCredentialBucket::Pat);
        let rendered = format!("{credential:?}");
        assert!(
            !rendered.contains("ghp_secret_value"),
            "a bearer must not reach Debug output: {rendered}"
        );
        assert!(
            rendered.contains("redacted"),
            "the token must be visibly redacted, not omitted: {rendered}"
        );
    }

    /// Callers supply arbitrary bearer values, so the unnamed-installation set
    /// is bounded: past the cap the least recently used bucket is evicted
    /// rather than growing the registry without limit.
    #[test]
    fn unnamed_installation_buckets_stay_bounded() {
        let registry = GithubBreakerRegistry::new(test_config());
        for index in 0..(MAX_UNNAMED_INSTALLATION_BUCKETS + 32) {
            registry.breaker_for(GithubCredentialBucket::for_installation_token(&format!(
                "ghs_{index}"
            )));
        }
        let labels = || {
            registry
                .snapshot()
                .into_iter()
                .filter(|row| row.bucket.starts_with("installation-token:"))
                .count()
        };
        assert_eq!(labels(), MAX_UNNAMED_INSTALLATION_BUCKETS);
        // The most recent bucket survives; the first one was evicted.
        assert!(
            !registry
                .snapshot()
                .iter()
                .any(|row| row.bucket
                    == GithubCredentialBucket::for_installation_token("ghs_0").label()),
            "the least recently used bucket must be the one evicted"
        );
    }

    /// The funnel keeps the failure text the push-back classifier reads: a
    /// non-2xx must still read as `status {code}`, with the body for the
    /// operator, and the status must be reachable without parsing that text.
    #[tokio::test]
    async fn api_errors_carry_status_and_body() {
        let stub = StubGitHub::serve(
            axum::http::StatusCode::NOT_FOUND,
            Vec::new(),
            "{\"message\":\"Not Found\"}",
        )
        .await;
        let state = funnel_state().await;
        let error = send_github_json(
            &state,
            &GithubCredential::anonymous(),
            GithubSubsystem::Workflows,
            reqwest::Method::GET,
            &format!("{}/repos/o/r", stub.base),
            None,
        )
        .await
        .expect_err("a 404 must fail");
        let api = error
            .downcast_ref::<GithubApiError>()
            .expect("a non-success status must be typed");
        assert_eq!(api.status, reqwest::StatusCode::NOT_FOUND);
        assert_eq!(api.body, "{\"message\":\"Not Found\"}");
        assert!(
            error.to_string().contains("status 404"),
            "the classifier reads the status off the text: {error}"
        );
        assert_eq!(stub.count(), 1);
    }
}
