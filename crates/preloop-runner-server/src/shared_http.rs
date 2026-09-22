use std::time::Duration;

/// Shared HTTP client for server-side outbound requests (GitHub API, scheduler).
/// Configured with standard timeouts and user-agent.
pub(crate) static CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .user_agent("preloop-runner-server")
        .build()
        .expect("HTTP client builder")
});

/// Client for credential-bearing GitHub API requests (the static PAT, the
/// GitHub App JWT). Same timeouts and user-agent as [`CLIENT`], but
/// redirects are only followed while the target still allows credentials
/// (HTTPS; loopback HTTP in test builds), with the chain capped at 10 hops.
/// A downgrade redirect therefore surfaces as a 3xx response instead of
/// resending the `Authorization` header to a cleartext URL (CWE-319).
/// Custom redirect policies bypass reqwest's built-in redirect limit, hence
/// the explicit cap: a longer chain is a loop or an attack, not a healthy
/// API endpoint.
pub(crate) static CREDENTIAL_SAFE_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .user_agent("preloop-runner-server")
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 {
                    attempt.stop()
                } else if crate::runs::github_api_url_allows_credential(attempt.url()) {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()
            .expect("credential-safe HTTP client builder")
    });
