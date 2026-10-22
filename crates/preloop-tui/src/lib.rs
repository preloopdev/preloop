//! `preloop tui`: a terminal UI for the Preloop CI control plane (the engine)
//! and the Sandbox Service.
//!
//! The crate is a library. `preloop-cli` resolves credentials and contexts,
//! then calls [`run`] with an [`Options`] value and a set of [`Hooks`] for the
//! actions that need the CLI binary (starting the engine, the setup wizard,
//! and shelling out to `preloop debug`).
//!
//! # Architecture
//!
//! The UI follows an Elm-style loop:
//!
//! - `app` owns [`App`](crate::app) state and `App::update(Msg) -> Vec<Effect>`.
//!   Rendering is a pure function of that state.
//! - `runtime` owns the terminal (alternate screen, raw mode, panic hook), an
//!   input thread feeding a tokio channel, a 250 ms tick, a redraw gate, and the
//!   effect executor. Streams run as tokio tasks; switching a run or a job
//!   aborts the previous stream of the same kind.
//! - `views` implements the `View` trait for each tab. `widgets` holds the shared
//!   drawing blocks. `store` keeps the loaded data as typed `Loadable` slots.
//!
//! # Configuration: `~/.preloop/tui.toml`
//!
//! Optional. Without it there is one context, `local`, which uses the engine
//! URL and token that `preloop` itself resolves, and the local sandbox defaults.
//!
//! ```toml
//! [[context]]
//! name = "cpane"                          # required, unique
//! engine_url = "https://preloop.example"  # engine base URL
//! engine_token_env = "CPANE_TOKEN"        # or engine_token_file = "..."
//! sandbox_url = "https://10.0.0.5:9191"   # optional Sandbox Service
//! sandbox_token_file = "/secrets/sbx"     # remote contexts name their own token
//! sandbox_ca_file = "/secrets/ca.pem"     # ... and their own CA
//!
//! [stack]
//! repos = ["preloopdev/preloop"]          # repos shown by the stack view
//! author = "octocat"                      # filter for your own PRs
//! extra_edges = [["#619", "#685"]]        # manual mirror edges (parent, child)
//! ```
//!
//! The local default context honours `PRELOOP_SANDBOX_URL`,
//! `PRELOOP_SANDBOX_TOKEN` and `PRELOOP_SANDBOX_CA`, and falls back to the
//! files under `<PRELOOP_HOME>/state`. Those local files are never sent to a
//! remote context.
//!
//! Keys: `1`-`7` switch views (`Tab` cycles), `:` opens the command palette,
//! `⇧C` switches context, `?` shows help, `q` quits.

pub mod api;
pub mod client;
pub mod config;
pub mod options;
pub mod wizard;

mod actions;
mod app;
mod detectors;
mod runtime;
mod store;
mod theme;
mod views;
mod widgets;

#[cfg(test)]
mod snapshot_tests;

pub use options::{Context, Hooks, Options, SandboxTarget, Secret, StartView};
pub use runtime::run;
