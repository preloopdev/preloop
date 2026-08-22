use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{Span, Status, Tracer};
use opentelemetry::KeyValue;
use preloop_observability::metrics::{classify_surface, normalize_route, status_class};
use preloop_observability::{HeaderExtractor, TraceContextPropagator};

use crate::state::SharedState;
use std::sync::Arc;

/// Middleware that records `http.server.request.duration` and
/// `http.server.active_requests` with bounded labels, and exports a span for
/// every non-public request when a traces endpoint is configured.
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
    // HTTP permits arbitrary extension-method tokens; a verbatim copy would
    // give an unauthenticated caller an unbounded duration-series key.
    let method = match req.method().as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" | "CONNECT" | "TRACE" => {
            req.method().as_str().to_string()
        }
        _ => "other".to_string(),
    };
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

    // The in-flight gauge is keyed without `status_class`: the status is
    // unknown until the response completes, so any status in the key would
    // let the increment (pre-response) and decrement (post-response)
    // disagree and leak the gauge permanently.
    let active_guard = (!is_live_logs).then(|| {
        let labels = preloop_observability::metrics::ActiveLabels {
            method: method.clone(),
            route: route.clone(),
            surface: surface.clone(),
        };
        shared
            .state
            .observability
            .metrics()
            .http
            .inc_active(&labels);
        ActiveGuard {
            shared: shared.clone(),
            labels,
        }
    });

    // Adopt an inbound W3C trace so a caller's trace continues through the
    // control plane; otherwise start a root. Health and metrics probes are
    // suppressed from trace export per the signal policy — they would swamp
    // the trace store and tell an operator nothing.
    let traced = shared.state.observability.tracing_enabled() && surface != "public";
    let parent_cx =
        traced.then(|| TraceContextPropagator::new().extract(&HeaderExtractor(req.headers())));
    let tracer = traced
        .then(|| shared.state.observability.tracer())
        .flatten();

    let start = Instant::now();
    let mut span = tracer.as_ref().map(|tracer| {
        let mut span = tracer.start_with_context(
            format!("{method} {route}"),
            parent_cx
                .as_ref()
                .unwrap_or(&opentelemetry::Context::current()),
        );
        // Attributes are allowlisted, never derived from the raw URI: the
        // route is the matched template and the surface is a finite set.
        span.set_attribute(KeyValue::new("http.request.method", method.clone()));
        span.set_attribute(KeyValue::new("http.route", route.clone()));
        span.set_attribute(KeyValue::new("preloop.surface", surface.clone()));
        span
    });

    let res = next.run(req).await;
    let elapsed = start.elapsed();

    let status = res.status().as_u16();
    let sc = status_class(status).to_string();

    if active_guard.is_some() {
        shared.state.observability.metrics().http.observe_duration(
            preloop_observability::metrics::HttpLabels {
                method: method.clone(),
                route: route.clone(),
                surface: surface.clone(),
                status_class: sc.clone(),
            },
            elapsed,
        );
    }
    // Drop the guard (and the gauge slot) even when the inner future is
    // cancelled or panics; the request is not in flight anymore either way.
    drop(active_guard);

    if let Some(span) = &mut span {
        span.set_attribute(KeyValue::new(
            "http.response.status_code",
            status.to_string(),
        ));
        // Only 5xx is the server's fault; a 4xx is the caller's and
        // marking it Error would make every unauthenticated probe
        // look like an outage.
        if status >= 500 {
            span.set_status(Status::error("server error"));
        }
        span.end();
    }

    res
}

/// Releases the active-request gauge on drop, so cancellation, panics, and
/// client disconnects during a long poll cannot leak the series.
struct ActiveGuard {
    shared: Arc<SharedState>,
    labels: preloop_observability::metrics::ActiveLabels,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.shared
            .state
            .observability
            .metrics()
            .http
            .dec_active(&self.labels);
    }
}
