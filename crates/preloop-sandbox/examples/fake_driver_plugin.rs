//! Reference out-of-process driver plugin.
//!
//! Serves the in-tree fake driver over the driver protocol on the socket the
//! host agent passes:
//!
//! ```text
//! cargo run -p preloop-sandbox --example fake_driver_plugin -- --socket /tmp/fake-driver.sock
//! ```
//!
//! Register it with the agent as `fake=/path/to/target/debug/examples/fake_driver_plugin`
//! (the agent appends `--socket <runtime_dir>/fake.sock`), or point it at an
//! already-running instance with `fake=unix:///tmp/fake-driver.sock`.

use std::sync::Arc;

use preloop_sandbox::driver_plugin;
use preloop_sandbox::drivers::fake::FakeDriver;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = driver_plugin::socket_arg()
        .ok_or_else(|| anyhow::anyhow!("usage: fake_driver_plugin --socket <path>"))?;

    // The fake driver keeps each sandbox as a directory on this host.
    let root = std::env::temp_dir().join("preloop-fake-driver-plugin");
    let driver = Arc::new(FakeDriver::new(
        "fake",
        root,
        FakeDriver::full_capabilities(),
    ));

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });

    driver_plugin::serve(driver, &socket, shutdown).await
}
