//! Preloop Sandbox Service.
//!
//! A service that hands out running machines from pluggable backends.
//! Callers use the `Sandboxes` API ([`client::SandboxApi`]); capacity owners
//! (host agents, connectors) attach through the `Providers` API
//! ([`agent`]). The wire contract is `proto/preloop/sandbox/v1/sandbox.proto`,
//! compiled to Rust by [`gen`] and served over TLS with ConnectRPC: callers
//! hold a bearer token, while providers enroll with a single-use join token
//! and then authenticate with a client certificate issued by the service CA
//! ([`tls`]).
//!
//! This crate must not depend on the CI control plane (`preloop-runner-server`,
//! `preloop-gha-*`, `preloop-orchestrator`): CI is one client among others.

pub mod agent;
pub mod api;
pub mod client;
pub mod driver;
pub mod drivers;
pub mod error;
#[path = "gen/mod.rs"]
pub mod generated;
pub mod guest_privilege;
pub mod metrics;
pub mod placement;
pub mod server;
pub mod service;
pub mod store;
pub mod tls;

pub use client::{InProcess, SandboxApi, SandboxClient};

/// Major version of the agent-to-service protocol.
///
/// The wire schema is additive within a major version; a change that an older
/// peer would misinterpret bumps this. Agents send it on `Register` and the
/// service refuses a mismatch instead of half-speaking a newer protocol.
pub const PROTOCOL_VERSION: u32 = 1;
pub use driver::Driver;
pub use error::{Code, Error, Result};
pub use placement::{Candidate, CandidateId, LeastLoaded, Placement, PlacementRequest};
pub use service::{MAX_TTL_SECS, ProviderIdentity, Service, ServiceConfig};
pub use tls::{AgentIdentity, PeerIdentity, ServerTls, ServiceCa};
