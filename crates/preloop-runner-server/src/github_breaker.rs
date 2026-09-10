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
pub(crate) enum GithubFailureKind {
    /// Transport error or 5xx: GitHub could not answer.
    Unavailable,
    /// Rate limited or secondary-limited, with GitHub's own resume time when
    /// it supplied one.
    RateLimited { retry_after: Option<Duration> },
}

impl GithubFailureKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::RateLimited { .. } => "rate_limited",
        }
    }
}

/// Breaker tuning. Defaults are deliberately conservative: three consecutive
/// dependency failures before opening, so one unlucky 502 does not stall the
/// queue, and a five-minute ceiling so a long outage is retried steadily
/// rather than exponentially forgotten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BreakerConfig {
    pub(crate) enabled: bool,
    pub(crate) failure_threshold: u32,
    pub(crate) base_open: Duration,
    pub(crate) max_open: Duration,
    /// How long a half-open probe may run before another caller is allowed
    /// through. Without this a probe whose task died would wedge the breaker
    /// half-open forever.
    pub(crate) probe_timeout: Duration,
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
    pub(crate) fn from_env() -> Self {
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
pub(crate) struct BreakerSnapshot {
    pub(crate) open: bool,
    pub(crate) retry_in_seconds: Option<u64>,
    pub(crate) consecutive_failures: u32,
    pub(crate) trips: u64,
    pub(crate) rate_limited: bool,
    pub(crate) last_error: Option<String>,
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
pub(crate) struct GithubBreaker {
    config: BreakerConfig,
    inner: parking_lot::Mutex<BreakerInner>,
}

impl Default for GithubBreaker {
    fn default() -> Self {
        Self::new(BreakerConfig::from_env())
    }
}

impl GithubBreaker {
    pub(crate) fn new(config: BreakerConfig) -> Self {
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
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        let inner = self.inner.lock();
        Self::remaining(&inner, Instant::now())
    }

    pub(crate) fn is_open(&self) -> bool {
        self.retry_after().is_some()
    }

    /// Ask permission to call GitHub.
    ///
    /// `Ok(())` while closed. Once the open window elapses exactly one caller
    /// is let through as a probe; everyone else keeps waiting until that
    /// probe reports back (or its timeout lapses), so recovery costs GitHub
    /// one request rather than the whole backlog at once.
    pub(crate) fn acquire(&self) -> Result<(), Duration> {
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
    pub(crate) fn record_success(&self) {
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
    pub(crate) fn record_failure(&self, kind: &GithubFailureKind, error: impl Into<String>) {
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
    pub(crate) fn observe_status(
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

    /// Observe a transport-level failure (DNS, TLS, connect, timeout).
    pub(crate) fn observe_transport_error(&self, error: &reqwest::Error) -> GithubFailureKind {
        let kind = GithubFailureKind::Unavailable;
        self.record_failure(&kind, error.to_string());
        kind
    }

    pub(crate) fn snapshot(&self) -> BreakerSnapshot {
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
pub(crate) fn classify_status(
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
pub(crate) async fn send_observed(
    breaker: &GithubBreaker,
    request: reqwest::RequestBuilder,
) -> reqwest::Result<reqwest::Response> {
    match request.send().await {
        Ok(response) => {
            breaker.observe_status(response.status(), response.headers());
            Ok(response)
        }
        Err(error) => {
            breaker.observe_transport_error(&error);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            breaker.acquire().is_err(),
            "a second caller must wait for the probe verdict"
        );
        breaker.record_success();
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
}
