#![allow(missing_docs)]

//! Rust reimplementation of the GitHub Actions runner.
//!
//! This crate implements the runner protocol client (Runner.Listener + Runner.Worker)
//! faithful to the official `actions/runner` v2.335.1. It speaks the same wire protocol
//! as the C# runner so it can register with GitHub, poll for jobs, execute workflow
//! steps, and report results.
//!
//! # Architecture
//!
//! Single binary with subcommands:
//! - `configure` — registers with a GitHub repo/org and persists credentials
//! - `remove` — deregisters the runner
//! - `run` — listener: polls for jobs, spawns worker processes
//! - `worker` — hidden: receives job via stdin NDJSON, executes steps, reports results
//!
//! The listener spawns `aksh-runner worker` as a child process per job, talking over
//! stdin with newline-delimited JSON. This mirrors the official Listener/Worker process
//! split for crash isolation.

pub mod cli;
pub mod client;
pub mod configure;
pub mod control_bridge;
pub mod listener;
pub mod process;
pub mod settings;
pub mod worker;

/// Protocol compatibility version (matches actions/runner release we target).
pub const PROTOCOL_COMPAT_VERSION: &str = "2.335.1";

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
