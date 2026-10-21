//! Preloop Sandbox Service.
//!
//! A service that hands out running machines from pluggable backends.
//! Callers use the `Sandboxes` API ([`client::SandboxApi`]); capacity owners
//! (host agents, connectors) attach through the `Providers` API
//! ([`agent`]). The wire contract is `proto/preloop/sandbox/v1/sandbox.proto`,
//! served with the Connect protocol ([`connect`]) over TLS: callers hold a
//! bearer token, while providers enroll with a single-use join token and then
//! authenticate with a client certificate issued by the service CA ([`tls`]).
//!
//! This crate must not depend on the CI control plane (`preloop-runner-server`,
//! `preloop-gha-*`, `preloop-orchestrator`): CI is one client among others.

pub mod agent;
pub mod api;
pub mod client;
pub mod connect;
pub mod driver;
pub mod drivers;
pub mod guest_privilege;
pub mod server;
pub mod service;
pub mod store;
pub mod tls;

pub use client::{InProcess, SandboxApi, SandboxClient};
pub use connect::{Code, Error, Result};
pub use driver::Driver;
pub use service::{ProviderIdentity, Service, ServiceConfig};
pub use tls::{AgentIdentity, PeerIdentity, ServerTls, ServiceCa};
