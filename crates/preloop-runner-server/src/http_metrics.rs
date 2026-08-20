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

    // Adopt an inbound W3C trace so a caller's trace continues through the
    // control plane; otherwise start a root. Health and metrics probes are
    // suppressed from trace export per the signal policy — they would swamp
    // the trace store and tell an operator nothing.
    let traced = shared.state.observability.tracing_enabled() && surface != "public";
    let span_context = traced.then(|| {
        preloop_observability::export::SpanContext::from_traceparent(
            req.headers()
                .get("traceparent")
                .and_then(|value| value.to_str().ok()),
        )
    });
    let span_start = preloop_observability::export::now_nanos();

    let start = Instant::now();
    let res = next.run(req).await;
    let elapsed = start.elapsed();

    let status = res.status().as_u16();
    let sc = status_class(status).to_string();

    if let Some(lbl) = labels {
        let mut lbl = lbl;
        lbl.status_class = sc.clone();
        let metrics = shared.state.observability.metrics();
        metrics.http.observe_duration(lbl.clone(), elapsed);
        metrics.http.dec_active(&lbl);
    }

    if let Some(context) = span_context {
        // Attributes are allowlisted, never derived from the raw URI: the
        // route is the matched template and the surface is a finite set.
        let attributes = vec![
            ("http.request.method".to_string(), method.clone()),
            ("http.route".to_string(), route.clone()),
            ("preloop.surface".to_string(), surface.clone()),
            ("http.response.status_code".to_string(), status.to_string()),
        ];
        shared
            .state
            .observability
            .export_span(preloop_observability::export::SpanRecord {
                context,
                name: format!("{method} {route}"),
                start_nanos: span_start,
                end_nanos: preloop_observability::export::now_nanos(),
                // Only 5xx is the server's fault; a 4xx is the caller's and
                // marking it Error would make every unauthenticated probe
                // look like an outage.
                status: if status >= 500 {
                    preloop_observability::export::SpanStatus::Error
                } else {
                    preloop_observability::export::SpanStatus::Unset
                },
                attributes,
            });
    }

    res
}
