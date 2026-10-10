//! Shared circuit breaker for GitHub dependency calls.
//!
//! Every webhook delivery preloop processes needs GitHub again — workflow
//! contents, ref resolution, PR file lists, check runs. When GitHub itself is
//! unavailable, each queued delivery independently discovers that, burns an
//! attempt, and adds load to a service that is already failing. Six attempts
//! at 1/5/15/30s covers a blip; it does not cover a 30-minute incident, so
//! good pushes get dead-lettered for something that was never their fault.
//!
//! One process-wide breaker fixes both halves: the queue stops claiming work
//! while GitHub is down (nothing to hammer it with, no attempts spent), and
//! the delivery that discovered the outage is *parked* — returned to
//! `received` with its attempt refunded — rather than counted as a failure.
//!
//! Rate limiting is treated as a first-class outage signal, not as a generic
//! 5xx: GitHub tells us exactly when to come back (`retry-after`,
//! `x-ratelimit-reset`), and ignoring that is how a rate limit becomes a ban.
//!
//! The breaker lives in [`crate::state::AppState`], not in a global, so
//! parallel tests that point at stub APIs cannot trip each other's breakers.

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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub fn observe_rate_budget(&self, headers: &HeaderMap) {
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

/// Send a GitHub request and report the outcome to `breaker`.
///
/// Every GitHub call on the webhook path goes through here so the breaker
/// sees the whole dependency, not the one endpoint someone remembered to
/// instrument. The response is returned untouched: a 404 still means what
/// it meant to the caller, it just also counts as proof GitHub is up.
pub async fn send_observed(
    breaker: &GithubBreaker,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    if let Err(remaining) = breaker.acquire() {
        anyhow::bail!(
            "GitHub circuit breaker is open (retry after {}s)",
            remaining.as_secs_f64()
        );
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
}

impl GithubSubsystem {
    pub fn as_str(&self) -> &'static str {
        match self {
            GithubSubsystem::CheckRuns => "check-runs",
            GithubSubsystem::Actions => "actions",
            GithubSubsystem::RefResolve => "ref-resolve",
            GithubSubsystem::Env => "env",
            GithubSubsystem::Snapshots => "snapshots",
        }
    }

    const ALL: [GithubSubsystem; 5] = [
        GithubSubsystem::CheckRuns,
        GithubSubsystem::Actions,
        GithubSubsystem::RefResolve,
        GithubSubsystem::Env,
        GithubSubsystem::Snapshots,
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
    counters: [SubsystemCounters; 5],
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
    pub rate_limited: u64,
    pub breaker_blocked: u64,
}

/// Send a GitHub request through the shared breaker with per-subsystem
/// consumption accounting.
///
/// This is the one funnel for outbound GitHub traffic: the breaker sees the
/// whole dependency (a 429 on an action tarball parks webhook deliveries too,
/// because the PAT budget they share is what is actually exhausted), and the
/// accounting attributes every request, byte, and rate limit to the
/// subsystem that caused it. Response semantics are unchanged from
/// [`send_observed`]: a 404 still means what it meant to the caller, it just
/// also proves GitHub is up.
///
/// Beyond [`send_observed`], the primary rate budget advertised on the
/// response (`x-ratelimit-remaining: 0` + `x-ratelimit-reset`) is honoured
/// for every subsystem, so a call that spends the last request parks the
/// next one instead of burning it on a guaranteed 403.
pub async fn send_observed_labeled(
    breaker: &GithubBreaker,
    consumption: &GithubConsumption,
    subsystem: GithubSubsystem,
    request: reqwest::RequestBuilder,
) -> anyhow::Result<reqwest::Response> {
    if let Err(remaining) = breaker.acquire() {
        consumption.record_breaker_blocked(subsystem);
        tracing::debug!(
            subsystem = subsystem.as_str(),
            retry_in_secs = remaining.as_secs(),
            "GitHub request refused without touching GitHub: circuit breaker open"
        );
        anyhow::bail!(
            "GitHub circuit breaker is open (retry after {}s)",
            remaining.as_secs_f64()
        );
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
                    %status,
                    "GitHub rate-limited a request; breaker is backing off"
                );
            }
            // Honour the primary budget advertised on the response: once
            // remaining hits zero, wait for the reset instead of spending
            // the next call on a guaranteed 403.
            breaker.observe_rate_budget(response.headers());
            Ok(response)
        }
        Err(error) => {
            breaker.observe_transport_error(&error);
            consumption.record_request(subsystem);
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
            vec!["check-runs", "actions", "ref-resolve", "env", "snapshots"]
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
                "/*path",
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
        let breaker = GithubBreaker::new(test_config());
        let consumption = GithubConsumption::default();

        // First exchange: the 429 reaches GitHub, trips the breaker
        // immediately (rate limits do not wait for the failure threshold),
        // and is accounted to the actions subsystem.
        let response = send_observed_labeled(
            &breaker,
            &consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/tarball/main", stub.base)),
        )
        .await
        .expect("the 429 response itself is returned, not an error");
        assert_eq!(response.status(), axum::http::StatusCode::TOO_MANY_REQUESTS);
        assert!(breaker.is_open(), "a 429 must open the breaker at once");
        assert!(breaker.snapshot().rate_limited);

        // Second attempt: refused without touching GitHub — the hammering
        // the pre-fix code path did is exactly what the breaker now stops.
        let blocked = send_observed_labeled(
            &breaker,
            &consumption,
            GithubSubsystem::Actions,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/tarball/main", stub.base)),
        )
        .await;
        assert!(blocked.is_err(), "breaker-open must fail fast");
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
        let breaker = GithubBreaker::new(test_config());
        let consumption = GithubConsumption::default();

        let response = send_observed_labeled(
            &breaker,
            &consumption,
            GithubSubsystem::RefResolve,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/commits/main", stub.base)),
        )
        .await
        .unwrap();
        consumption.record_advertised_bytes(GithubSubsystem::RefResolve, &response);
        assert!(!breaker.is_open());

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
        let breaker = GithubBreaker::new(test_config());
        let consumption = GithubConsumption::default();

        // A 200 that spends the last request still parks the breaker: the
        // next call would be a guaranteed 403.
        send_observed_labeled(
            &breaker,
            &consumption,
            GithubSubsystem::Env,
            crate::shared_http::CLIENT.get(format!("{}/repos/o/r/environments/prod", stub.base)),
        )
        .await
        .unwrap();
        assert!(
            breaker.is_open(),
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
}
