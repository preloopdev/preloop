#![allow(missing_docs, dead_code, clippy::too_many_arguments)]

//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod concurrency;
mod errors;
pub mod events;
pub mod github;
pub mod scheduler;
mod shared_http;
pub use errors::ApiError;
mod actions;
use actions::*;
mod reusable_workflows;
use reusable_workflows::*;
mod remote_workflows;
mod runs;
use runs::*;
mod runtime_scheduling;
use runtime_scheduling::*;
mod timeline_logs;
use timeline_logs::*;
mod routes;
use routes::build_app;
pub use routes::{app, app_with_test_api};
mod live_logs;
use live_logs::*;
mod debug;
use debug::*;
mod runner_lifecycle;
use runner_lifecycle::*;
mod broker;
use broker::*;
mod distributed_task;
use distributed_task::*;
mod auth;
use auth::*;
mod oauth;
use oauth::*;
mod oidc_handlers;
use oidc_handlers::*;
mod results_twirp;
use results_twirp::*;
mod artifact_twirp;
use artifact_twirp::*;
mod compat_ghes;
use compat_ghes::*;
mod cache_artifacts;
use cache_artifacts::*;
mod snapshots;
use snapshots::*;
mod recording;
use recording::*;
mod state;
use state::*;
pub use state::{AppState, SharedState};
mod models;
use models::*;
mod bootstrap;
#[cfg(test)]
use bootstrap::reap_once;
pub use bootstrap::{generate_self_signed_cert, serve, SelfSignedCert, ServerConfig, TlsMode};
mod blob_store;
use blob_store::*;
mod connection;
use connection::*;

/// Pure job-graph scheduler model and property tests.
pub mod scheduling;

#[cfg(test)]
mod concurrency_http_properties;
#[cfg(test)]
mod concurrency_properties;
/// GitHub-compatible OIDC id-token provider.
pub mod oidc;

use axum_server::{tls_rustls::RustlsConfig, Handle};
use rcgen::generate_simple_self_signed;

use aksh_artifacts::{validate_artifact_name, ArtifactStore};
use aksh_cache::CacheStore;
use aksh_gha_parser::eval::build_context;
use aksh_gha_parser::parse_workflow;
use aksh_gha_protocol::{
    azdo,
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson, AnnotationLevel, ExecutionStatus, JobCompletion, JobId, NdjsonEvent,
    RegisteredRunner, RunAccepted, RunId, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use futures::{stream, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

/// Default local token used when `AKSH_SYSTEM_TOKEN` is not configured.
const DEFAULT_AKSH_SYSTEM_TOKEN: &str = "aksh-system-token";
#[cfg(test)]
const TEST_LOCAL_JWT_KEY: &[u8] = b"aksh-test-local-jwt-signing-key";

// Re-export from protocol crate — shared wire type with the runner.
use aksh_gha_protocol::LiveLogFeedLinesWrapper;

#[cfg(test)]
/// Production-path DAG/workflow properties.
///
/// Oracle sources:
/// - `needs`, skipped dependencies, and job-level conditions:
///   <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
/// - status functions: <https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions>.
/// - runner v2.335.1: `src/Runner.Worker/StepsRunner.cs` and
///   `src/Runner.Worker/Expressions/{Success,Failure,Cancelled,Always}Function.cs`.
///
/// These tests submit YAML through the real parser/router and use only the
/// explicitly gated internal test API to simulate worker completions. The
/// oracle compares observable job/run state; it does not copy scheduler code.
#[path = "lib_tests.rs"]
mod tests;
