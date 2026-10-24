//! Client commands must never start an engine (#658).
//!
//! Every case runs the real `preloop` binary against an isolated
//! `PRELOOP_HOME` and an endpoint nothing listens on, then asserts the
//! refusal and its blast radius: no `preloop.pid`, no `engine.log`, no
//! control database, and the probed port still closed. `run` is in the table
//! on purpose — it used to be the command that started a control plane and a
//! smolVM from a pre-push hook.
//!
//! `serve` is the only command that may start an engine, so it is the only
//! one that prepares a fresh home; the last test drives that preparation at
//! the earliest point `serve` can stop, without letting it bind or provision
//! anything.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

const PRELOOP: &str = env!("CARGO_BIN_EXE_preloop");

/// A loopback port nothing listens on: bind it, take the port, drop it.
fn dead_endpoint() -> (String, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    (format!("http://{addr}"), addr)
}

fn run(home: &Path, endpoint: &str, args: &[&str]) -> Output {
    Command::new(PRELOOP)
        .args(args)
        .current_dir(home)
        .env("PRELOOP_HOME", home)
        .env("PRELOOP_URL", endpoint)
        .env_remove("PRELOOP_TOKEN")
        .env_remove("PRELOOP_SYSTEM_TOKEN")
        .env_remove("PRELOOP_STORE_URL")
        .env_remove("PRELOOP_LISTEN")
        .env_remove("PRELOOP_PUBLIC_URL")
        .output()
        .expect("run the preloop binary")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn port_is_closed(addr: SocketAddr) -> bool {
    TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err()
}

/// The refusal contract: non-zero exit, the probed endpoint, both ways to
/// start an engine, and the pre-push hook's fail-open marker.
fn assert_refused(home: &Path, addr: SocketAddr, endpoint: &str, output: &Output) {
    assert!(
        !output.status.success(),
        "the command must exit non-zero: {}",
        stderr(output)
    );
    let err = stderr(output);
    assert!(err.contains("no engine is running"), "{err}");
    assert!(err.contains(endpoint), "{err}");
    assert!(err.contains("preloop serve"), "{err}");
    assert!(err.contains("preloop server install"), "{err}");
    // The pre-push hook fails open only on this marker; without it a missing
    // engine reads as a failed run and blocks the push.
    assert!(err.contains("PRELOOP_UNREACHABLE"), "{err}");

    assert!(
        !home.join("preloop.pid").exists(),
        "no pid file may be written"
    );
    assert!(
        !home.join("engine.log").exists(),
        "no engine log may be written"
    );
    assert!(
        !home.join("state").join("preloop.db").exists(),
        "no control database may be created"
    );
    assert!(port_is_closed(addr), "the probed port must stay closed");
}

const TINY_WORKFLOW: &str = r#"name: tiny
on: push
jobs:
  build:
    runs-on: self-hosted
    steps:
      - run: echo hi
"#;

/// Every client command refuses when no engine answers, and starts nothing.
#[test]
fn client_commands_refuse_without_a_running_engine() {
    let commands: &[&[&str]] = &[
        &["status"],
        &["status", "run-42"],
        &["logs"],
        &["logs", "--follow"],
        &["cancel"],
        &["rerun"],
        &["rerun", "run-42", "--failed"],
        &["shell"],
        &["debug"],
        &["debug", "--verdict", "abort"],
        &["dap", "run-42"],
        &["push"],
        &["webhooks", "list"],
        &["webhooks", "health"],
        &["run"],
        &["run", "-f", "tiny.yml"],
    ];

    for args in commands {
        let (endpoint, addr) = dead_endpoint();
        let home = tempfile::tempdir().expect("temp home");
        std::fs::write(home.path().join("tiny.yml"), TINY_WORKFLOW).expect("workflow fixture");
        let output = run(home.path(), &endpoint, args);
        assert_refused(home.path(), addr, &endpoint, &output);
    }
}

/// A reachable endpoint is not refused: the command proceeds to its own
/// request. The stub is not an engine, so that request fails — which is the
/// point: the gate is a probe, not a blanket refusal.
#[test]
fn a_reachable_endpoint_lets_the_command_through_to_its_own_request() {
    let stub = StubServer::start();
    let home = tempfile::tempdir().expect("temp home");
    let output = run(home.path(), &stub.endpoint(), &["status"]);
    let err = stderr(&output);

    assert!(!err.contains("no engine is running"), "{err}");
    assert!(!err.contains("PRELOOP_UNREACHABLE"), "{err}");
    assert!(
        !home.path().join("preloop.pid").exists(),
        "the gate never writes a pid file"
    );
}

/// A command that names its own engine is probed there: `dap --url` against a
/// live endpoint must not be refused because `PRELOOP_URL` happens to be dead.
#[test]
fn an_endpoint_override_is_probed_instead_of_the_default() {
    let stub = StubServer::start();
    let (endpoint, _addr) = dead_endpoint();
    let home = tempfile::tempdir().expect("temp home");
    let output = run(
        home.path(),
        &endpoint,
        &["dap", "run-42", "--url", &stub.endpoint()],
    );
    let err = stderr(&output);

    assert!(!err.contains("no engine is running"), "{err}");
    assert!(!err.contains("PRELOOP_UNREACHABLE"), "{err}");
    assert!(
        !home.path().join("preloop.pid").exists(),
        "the gate never writes a pid file"
    );
}

/// `serve` owns the zero-config first run: on a fresh `PRELOOP_HOME` it
/// initializes the brand-new control database before it binds anything.
///
/// A full `serve` boot is deliberately not driven here — it would start an
/// engine and provision VMs. `--save` without credential flags stops the
/// command at the earliest deterministic point after the preparation, so the
/// test can prove the database exists and is current, and that no engine
/// artifact was produced.
#[test]
fn serve_prepares_a_fresh_home_and_starts_nothing_here() {
    let (endpoint, _addr) = dead_endpoint();
    let home = tempfile::tempdir().expect("temp home");

    let output = run(home.path(), &endpoint, &["serve", "--save"]);
    let err = stderr(&output);
    assert!(!output.status.success(), "{err}");
    assert!(
        err.contains("needs at least one GitHub credential flag to save"),
        "{err}"
    );
    assert!(
        !home.path().join("preloop.pid").exists(),
        "no pid file may be written"
    );
    assert!(
        !home.path().join("preloop.sock").exists(),
        "serve must not reach the socket"
    );
    assert!(
        !home.path().join("state").join("serve.lock").exists(),
        "serve must not take the lock"
    );

    assert!(
        home.path().join("state").join("preloop.db").is_file(),
        "a fresh home must get a control database"
    );

    // The boot's schema check requires exactly this: current, nothing pending.
    let status = run(home.path(), &endpoint, &["store", "status"]);
    let stdout = String::from_utf8_lossy(&status.stdout).into_owned();
    assert!(status.status.success(), "{stdout}{}", stderr(&status));
    assert!(stdout.contains("initialized"), "{stdout}");
    assert!(!stdout.contains("not at this build's schema"), "{stdout}");
}

/// `serve --store` owns the first run of the store it names, not of the
/// state-dir default: prepared against the default, the boot would open an
/// empty database elsewhere and refuse it.
#[test]
fn serve_prepares_the_store_it_was_pointed_at() {
    let (endpoint, _addr) = dead_endpoint();
    let home = tempfile::tempdir().expect("temp home");
    let store = home.path().join("elsewhere").join("custom.db");
    std::fs::create_dir_all(store.parent().expect("store parent")).expect("store parent");
    let store_url = format!("sqlite://{}", store.display());

    let output = run(
        home.path(),
        &endpoint,
        &["serve", "--save", "--store", &store_url],
    );
    let err = stderr(&output);
    assert!(!output.status.success(), "{err}");
    assert!(
        err.contains("needs at least one GitHub credential flag to save"),
        "{err}"
    );

    assert!(
        store.is_file(),
        "serve must initialize the store it was pointed at"
    );
    assert!(
        !home.path().join("state").join("preloop.db").exists(),
        "the state-dir default must stay untouched"
    );

    let status = run(
        home.path(),
        &endpoint,
        &["store", "status", "--store", &store_url],
    );
    let stdout = String::from_utf8_lossy(&status.stdout).into_owned();
    assert!(status.status.success(), "{stdout}{}", stderr(&status));
    assert!(stdout.contains("initialized"), "{stdout}");
    assert!(!stdout.contains("not at this build's schema"), "{stdout}");
}

/// Answers `404 Not Found` to every request. The liveness probe only needs a
/// live socket, and returning an error keeps the test away from any engine
/// behavior: the command's own request is what fails.
struct StubServer {
    addr: SocketAddr,
    _handle: std::thread::JoinHandle<()>,
}

impl StubServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let addr = listener.local_addr().expect("stub addr");
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request);
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
}
