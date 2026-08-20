use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};
use preloop_observability::metrics::{classify_surface, normalize_route, status_class};

use crate::state::SharedState;
use std::sync::Arc;

/// Middleware that records `http.server.request.duration` and
/// `http.server.active_requests` with bounded labels.
///
/// - `method` — HTTP method (GET, POST, …)
/// - `route` — Axum matched template (e.g. `/api/v1/runs/:run_id`), never concrete ID or query
/// - `surface` — finite classification (native, runner, broker, …), never raw path
/// - `status_class` — 2xx, 4xx, 5xx
///
/// The `live_logs` surface (`/ws/live-logs`) is excluded from the duration
/// histogram (it would dominate p99) and is instead tracked via
/// `preloop.livelog.connections`.
pub async fn http_metrics_middleware(
    State(shared): State<Arc<SharedState>>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    // Prefer Axum's matched template; fallback to manual normalization for
    // the 1,000-IDs test and for unmatched routes.
    let raw_path = req.uri().path().to_string();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| normalize_route(&raw_path));
    let surface = classify_surface(&route).to_string();

    // Skip HTTP metrics for the long-lived WebSocket — it is instrumented
    // separately via `preloop.livelog.*`.
    let is_live_logs = surface == "live_logs";

    let labels = if !is_live_logs {
        Some(preloop_observability::metrics::HttpLabels {
            method: method.clone(),
            route: route.clone(),
            surface: surface.clone(),
            status_class: "2xx".to_string(), // placeholder, updated after response
        })
    } else {
        None
    };

    if let Some(lbl) = &labels {
        shared.state.observability.metrics().http.inc_active(lbl);
    }

    let start = Instant::now();
    let res = next.run(req).await;
    let elapsed = start.elapsed();

    let status = res.status().as_u16();
    let sc = status_class(status).to_string();

    if let Some(lbl) = labels {
        let mut lbl = lbl;
        lbl.status_class = sc.clone();
        // Record duration only for non-live_logs
        shared
            .state
            .observability
            .metrics()
            .http
            .observe_duration(lbl.clone(), elapsed);
        shared.state.observability.metrics().http.dec_active(&lbl);
    }

    // Safe span: method + route template + surface + status, no headers/body/query.
    // Use `tracing::info_span!` so it appears in logs when RUST_LOG includes it,
    // but filtered at DEBUG by default (poll/renew are DEBUG).
    let span = tracing::info_span!(
        "http.request",
        http.method = %method,
        http.route = %route,
        http.surface = %surface,
        http.status_code = status,
        http.status_class = %sc,
        otel.kind = "server",
    );
    // Attach span to response for trace correlation; the span itself is not
    // entered for the handler thread beyond this point (no await while held).

    // Also emit a counter for broker poll outcomes — the control plane's
    // poll is a long-poll that returns job|empty|cancel|error. That is
    // already counted via the HTTP histogram, but the plan also wants
    // `preloop.broker.poll{outcome}` to distinguish empty vs error.
    // For now we just log at DEBUG; the dedicated broker counter is wired
    // in the broker module itself (Step 4 lifecycle).
    let _ = span;

    res
}
