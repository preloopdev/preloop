//! The journal line an operator sees when a bound expires.
//!
//! This is an integration test on purpose: it installs the process-global
//! tracing subscriber, so it must own its own test process. A thread-local
//! scoped subscriber races with the other unit tests' callsite-interest
//! caching when the crate's tests run in parallel.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use preloop_observability::deadline;
use tokio::process::Command;

/// A `MakeWriter` that keeps everything the module logs for assertion.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl CapturedLog {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// A dependency that misses its deadline must leave a warning naming the
/// operation, the program, and the deadline — the same text as the returned
/// error, for callers that wrap or discard it.
#[tokio::test]
async fn expiry_logs_the_program_and_deadline() {
    let captured = CapturedLog::default();
    tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .without_time()
        .init();

    let mut cmd = Command::new("sh");
    cmd.args(["-c", "sleep 30"]);
    let error = deadline::run_command(cmd, Duration::from_millis(250), "logged sleep")
        .await
        .unwrap_err();
    assert!(error.timed_out(), "expected a timeout: {error}");

    let logged = captured.contents();
    assert!(
        logged.contains("WARN"),
        "the kill must be a warning: {logged}"
    );
    assert!(logged.contains("logged sleep"), "missing what: {logged}");
    assert!(logged.contains("250ms"), "missing deadline: {logged}");
    assert!(logged.contains("sh"), "missing program: {logged}");
}
