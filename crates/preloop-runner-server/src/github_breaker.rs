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
    /// `PRELOOP_GITHUB_BREAKER=off` disables tripping entirely (calls are
    /// still observed, so the status snapshot keeps reporting failures);
    /// `PRELOOP_GITHUB_BREAKER_THRESHOLD` overrides the failure count.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Ok(value) = std::env::var("PRELOOP_GITHUB_BREAKER") {
            let value = value.trim().to_ascii_lowercase();
            if matches!(value.as_str(), "off" | "0" | "false" | "disabled") {
                config.enabled = false;
            }
        }
        if let Some(threshold) = std::env::var("PRELOOP_GITHUB_BREAKER_THRESHOLD")
            .ok()
            .and_then(|value| value.trim().parse::<u32>().ok())
            .filter(|threshold| *threshold > 0)
        {
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
        Self::new(BreakerConfig::from_env())
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
/// not park another installation's traffic, and the static PAT, the App JWT,
/// and anonymous calls each have their own budget. Breaker state is
/// partitioned on this key so a throttled credential only blocks its own
/// traffic; consumption accounting keeps its per-subsystem labels separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GithubCredentialBucket {
    /// A GitHub App installation token, keyed by installation id.
    Installation(u64),
    /// The engine's static PAT (`PRELOOP_GITHUB_TOKEN` / config `github.pat`).
    Pat,
    /// A GitHub App JWT (webhook delivery management, app health checks).
    App,
    /// No credential (anonymous API, 60 req/hour/IP).
    Anonymous,
}

impl GithubCredentialBucket {
    /// Stable label for snapshots and traces.
    pub fn label(&self) -> String {
        match self {
            GithubCredentialBucket::Installation(id) => format!("installation:{id}"),
            GithubCredentialBucket::Pat => "pat".to_owned(),
            GithubCredentialBucket::App => "app".to_owned(),
            GithubCredentialBucket::Anonymous => "anonymous".to_owned(),
        }
    }
}

/// Registry of per-credential breakers.
///
/// Every outbound GitHub call resolves its credential's
/// [`GithubCredentialBucket`] and uses that bucket's breaker, so unrelated
/// rate-limit budgets never block each other while a genuine GitHub outage
/// still parks every bucket that observes it.
#[derive(Debug)]
pub struct GithubBreakerRegistry {
    config: BreakerConfig,
    breakers: parking_lot::Mutex<HashMap<GithubCredentialBucket, Arc<GithubBreaker>>>,
}

impl GithubBreakerRegistry {
    pub fn new(config: BreakerConfig) -> Self {
        Self {
            config,
            breakers: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// The breaker for `bucket`, creating it on first use. Breakers are never
    /// removed: the bucket set is bounded (installations the server actually
    /// talks to), and dropping one would lose its backoff streak.
    pub fn breaker_for(&self, bucket: GithubCredentialBucket) -> Arc<GithubBreaker> {
        let mut breakers = self.breakers.lock();
        breakers
            .entry(bucket)
            .or_insert_with(|| Arc::new(GithubBreaker::new(self.config)))
            .clone()
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
    /// A bucket whose window has elapsed but whose half-open probe is still
    /// running is still down. [`GithubBreaker::retry_after`] covers the probe;
    /// [`BreakerSnapshot::open`] covers only the window, so it cannot be used.
    pub fn unavailable_retry_after(&self) -> Option<Duration> {
        self.breakers
            .lock()
            .values()
            .filter(|breaker| !breaker.snapshot().rate_limited)
            .filter_map(|breaker| breaker.retry_after())
            .min()
    }

    /// Per-bucket breaker state for the operational snapshot, in stable
    /// bucket-label order.
    pub fn snapshot(&self) -> Vec<GithubBucketSnapshot> {
        let breakers = self.breakers.lock();
        let mut snapshots: Vec<GithubBucketSnapshot> = breakers
            .iter()
            .map(|(bucket, breaker)| GithubBucketSnapshot {
                bucket: bucket.label(),
                breaker: breaker.snapshot(),
            })
            .collect();
        snapshots.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        snapshots
    }
}

impl Default for GithubBreakerRegistry {
    fn default() -> Self {
        Self::new(BreakerConfig::from_env())
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
        }
    }

    const ALL: [GithubSubsystem; 7] = [
        GithubSubsystem::CheckRuns,
        GithubSubsystem::Actions,
        GithubSubsystem::RefResolve,
        GithubSubsystem::Env,
        GithubSubsystem::Snapshots,
        GithubSubsystem::Workflows,
        GithubSubsystem::Webhooks,
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
/// whole dependency partitioned by credential (a 429 on installation A's
/// action tarball parks installation A's check runs too, because the
/// installation budget they share is what is actually exhausted — but
/// installation B and the PAT keep flowing), and the accounting attributes
/// every request, byte, and rate limit to the subsystem that caused it.
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
    let breaker = registry.breaker_for(bucket);
    if let Err(remaining) = breaker.acquire() {
        consumption.record_breaker_blocked(subsystem);
        tracing::debug!(
            subsystem = subsystem.as_str(),
            bucket = bucket.label(),
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
                "webhooks"
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

    /// A tiny stub GitHub: serves scripted responses and counts hits, so
    /// tests prove the breaker engages without touching the real github.com.
    struct StubGitHub {
        base: String,
        hits: Arc<std::sync::atomic::AtomicU64>,
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
            let app = axum::Router::new().route(
                "/{*path}",
                axum::routing::any(move || {
                    let hits_route = hits_route.clone();
                    async move {
                        hits_route.fetch_add(1, Ordering::SeqCst);
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
            Self { base, hits }
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
}
