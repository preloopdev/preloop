#![allow(missing_docs)]

//! Rust reimplementation of the GitHub Actions runner.
//!
//! This crate implements the runner protocol client (Runner.Listener + Runner.Worker)
//! faithful to the official `actions/runner` release pinned by `runner_version` in
//! the workspace `versions.toml` (advertised as [`PROTOCOL_COMPAT_VERSION`]). It
//! speaks the same wire protocol as the C# runner so it can register with GitHub,
//! poll for jobs, execute workflow steps, and report results.
//!
//! # Architecture
//!
//! Single binary with subcommands:
//! - `configure` — registers with a GitHub repo/org and persists credentials
//! - `remove` — deregisters the runner
//! - `run` — listener: polls for jobs, spawns worker processes
//! - `worker` — hidden: receives job via stdin NDJSON, executes steps, reports results
//!
//! The listener spawns `preloop-runner worker` as a child process per job, talking over
//! stdin with newline-delimited JSON. This mirrors the official Listener/Worker process
//! split for crash isolation.

pub mod cli;
pub mod client;
pub mod configure;
pub mod control_bridge;

include!(concat!(env!("OUT_DIR"), "/pins.rs"));

pub mod node_externals;

pub mod listener;
pub mod process;
pub mod settings;
pub mod worker;

/// Protocol compatibility version: the `actions/runner` release this build
/// advertises everywhere the wire protocol exposes a version (agent body,
/// session body, `runnerVersion=` poll/ack params, User-Agent).
///
/// Sourced from `runner_version` in the workspace `versions.toml` via the
/// build script's `pins.rs` (`RUNNER_VERSION`); bumping that pin is the only
/// edit an advertised-version change needs. GitHub stops queuing jobs to a
/// runner 30 days after a newer release, so the pin must stay current —
/// `configure` warns when the deprecations endpoint reports a near
/// `runtime_deprecates_at` for this version.
pub const PROTOCOL_COMPAT_VERSION: &str = RUNNER_VERSION;

/// Crate version for display.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// OS description string for runner registration, matching official runner.
///
/// Reads `PRETTY_NAME` from `/etc/os-release` on Linux.
/// Falls back to `os arch` (e.g. `linux aarch64`).
pub fn os_description() -> String {
    if let Ok(contents) = std::fs::read_to_string("/etc/os-release") {
        for line in contents.lines() {
            if let Some(val) = line.strip_prefix("PRETTY_NAME=") {
                let pretty = val.trim_matches('"');
                if !pretty.is_empty() {
                    return pretty.to_string();
                }
            }
        }
    }
    format!("{} {}", std::env::consts::OS, std::env::consts::ARCH)
}
