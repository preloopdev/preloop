#![allow(missing_docs, dead_code, clippy::too_many_arguments)]

//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod concurrency;
pub mod config;
pub mod credential_store;
pub mod errors;
mod event_feed;
pub mod events;
pub mod execution_protection;
pub mod fork_policy;
pub mod github;
pub mod github_app;
pub mod github_breaker;
pub mod github_pr;
pub mod github_push;
pub mod scheduler;
pub mod shared_http;
pub mod token_ceiling;
pub mod webhook_api;
pub mod webhook_health;
pub mod webhook_status;
pub mod webhook_watchdog;
pub(crate) use crate::control::backend::ControlBackend;
pub use errors::ApiError;
pub mod actions;
use actions::*;
pub mod secrets_api;
use secrets_api::*;
pub mod execution_protection_api;
use execution_protection_api::*;
mod message_template;
pub mod remote_workflows;
pub mod reusable_workflows;
pub mod runs;
pub mod secret_provider;
#[cfg(any(test, feature = "test-support"))]
pub mod test_pg;
use runs::*;
pub mod runtime_scheduling;
use runtime_scheduling::*;
pub mod control;
// One-time legacy-store importer (`preloop store import-legacy`). The
// implementation lives under the SQLite backend because it writes with the
// backend's own row vocabulary; this re-export is its public surface.
pub use control::lite::legacy_import;
pub mod timeline_logs;
use timeline_logs::*;
mod live_log_segments;
pub(crate) use live_log_segments::LiveLogSegments;
pub mod routes;
use routes::build_app;
pub use routes::{app, app_with_test_api};
pub mod live_logs;
pub mod openapi;
use live_logs::*;
pub mod debug;
mod http_metrics;
use debug::*;
pub mod debug_sessions;
pub mod runner_lifecycle;
use runner_lifecycle::*;
pub mod broker;
use broker::*;
pub mod distributed_task;
use distributed_task::*;
pub mod auth;
use auth::*;
pub mod dispatch;
pub mod dispatch_auth;
pub mod oauth;
use oauth::*;
pub mod runner_deprecations;
use runner_deprecations::*;
pub mod oidc_handlers;
use oidc_handlers::*;
pub mod results_twirp;
use results_twirp::*;
pub mod artifact_twirp;
use artifact_twirp::*;
pub mod compat_ghes;
use compat_ghes::*;
pub mod cache_artifacts;
use cache_artifacts::*;
pub mod snapshots;
use snapshots::*;
pub mod recording;
use recording::*;
pub mod state;
use state::*;
pub use state::{AppState, SharedState};
pub mod models;
use models::*;
pub mod bootstrap;
pub mod store;
pub mod store_pg;
#[cfg(test)]
#[allow(unused_imports)]
use bootstrap::reap_once;
pub use bootstrap::{SelfSignedCert, ServerConfig, TlsMode, generate_self_signed_cert, serve};
pub mod blob_store;
use blob_store::*;
pub mod connection;
use connection::*;
pub mod memory_caps;
use memory_caps::*;
/// Run/check/status retention sweep (GitHub Actions retention setting).
pub mod retention;

/// Pure job-graph scheduler model and property tests.
pub mod scheduling;

#[cfg(test)]
mod concurrency_http_properties;
#[cfg(test)]
mod dispatch_tests;
/// GitHub-compatible OIDC id-token provider.
pub mod oidc;

use axum_server::{Handle, tls_rustls::RustlsConfig};
use rcgen::generate_simple_self_signed;

use axum::body::{Body, to_bytes};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::{StreamExt, stream};
use hmac::{Hmac, Mac};
use preloop_artifacts::{ArtifactStore, validate_artifact_name};
use preloop_cache::CacheStore;
use preloop_gha_parser::eval::build_context;
use preloop_gha_parser::parse_workflow;
use preloop_gha_protocol::{
    AnnotationLevel, ExecutionStatus, JobCompletion, JobId, NdjsonEvent, PROTOCOL_VERSION,
    RegisteredRunner, RunAccepted, RunId, RunnerRegistrationRequest, RunnerSessionRequest,
    WorkflowSubmission, azdo,
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[cfg(any(test, feature = "test-support"))]
/// Deterministic administrator token used only by in-process tests.
pub const DEFAULT_PRELOOP_SYSTEM_TOKEN: &str = "preloop-system-token";
#[cfg(any(test, feature = "test-support"))]
pub const TEST_LOCAL_JWT_KEY: &[u8] = b"preloop-test-local-jwt-signing-key";

// Re-export from protocol crate — shared wire type with the runner.
use preloop_gha_protocol::LiveLogFeedLinesWrapper;

// The former `#[cfg(test)] mod tests` unit (lib_tests.rs, ~27k lines) was split
// into separate `tests/*.rs` integration crates — a single rustc unit that
// large was OOM-killed inside the 4 GiB CI runner guest. Those crates link
// against this lib with the `test-support` feature for the deterministic
// hooks above.
