//! Where to get the real `preloop`.
//!
//! `preloop` is not a Rust library and does not install with `cargo install`:
//! a single binary embeds the whole control plane (`preloop serve`), the job
//! orchestrator, and the microVM runner pool, and it ships as a prebuilt
//! binary. This crate holds the name on crates.io so nothing else can take it,
//! and documents the channels that do install the CLI.
//!
//! ```sh
//! brew install preloopdev/tap/preloop   # Homebrew (macOS, Linuxbrew)
//! npm install -g @preloop-dev/cli       # npm
//! ```
//!
//! Or take `preloop-cli-<target>.tar.gz` from
//! <https://github.com/preloopdev/preloop/releases/latest>, verify it against
//! the published `.sha256`, and check its build provenance with
//! `gh attestation verify <asset> --repo preloopdev/preloop`.
//!
//! Documentation: <https://github.com/preloopdev/preloop#readme>

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// The channels that install the real `preloop` binary, for anything that
/// lands here looking for it.
pub const INSTALL_CHANNELS: [&str; 3] = [
    "brew install preloopdev/tap/preloop",
    "npm install -g @preloop-dev/cli",
    "https://github.com/preloopdev/preloop/releases/latest",
];
