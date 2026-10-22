//! Clients for the engine, the Sandbox Service and GitHub.
//!
//! - [`engine`]: the engine's native API, with the NDJSON and SSE framing its
//!   live feeds use.
//! - [`sandbox`]: read-only access to the Sandbox Service.
//! - [`github`]: GraphQL through the `gh` CLI.

pub mod engine;
pub mod github;
pub mod sandbox;

pub use engine::{EngineClient, EngineError, RunsQuery};
pub use sandbox::{SandboxHandle, SandboxSummary};
