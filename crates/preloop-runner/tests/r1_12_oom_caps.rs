//! R1-12 regression tests: runner OOM primitives must be bounded.
//!
//! Each test fails on the unfixed code (verified by stashing the fix) and
//! passes with the caps in place.

use preloop_runner::process;
use preloop_runner::worker::contexts::JobContext;
use preloop_runner::worker::execution_context::StepContext;
use preloop_runner::worker::file_commands::{parse_kv_file, parse_path_file};
use std::collections::HashMap;

fn make_job() -> JobContext {
    JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    )
}

/// R1-12 (output lines): 2 MiB of newline-free output must not be retained
/// 1:1 in the partial-line buffer.
#[test]
fn write_chunk_caps_newline_free_output() {
    let mut job = make_job();
    let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
    let blob = vec![b'A'; 2 * 1024 * 1024];
    ctx.write_chunk(&blob);
    ctx.flush_line_buffer();
    let content = ctx.log_content();
    assert!(
        content.contains("partial-line buffer"),
        "expected a truncation warning in the log"
    );
    assert!(
        content.len() < 1_500_000,
        "partial-line buffer must be capped near 1 MiB, got {} bytes",
        content.len()
    );
}

/// R1-12 (log rematerialization): `log_content` must not read the whole log
/// file into memory.
#[test]
fn log_content_does_not_rematerialize_whole_file() {
    let mut job = make_job();
    let mut ctx = StepContext::new(&mut job, "s1".into(), "Step".into());
    // ~100k lines x ~90B -> well over the 8 MiB rematerialization cap once
    // timestamp prefixes are added.
    let mut chunk = Vec::with_capacity(10 * 1024 * 1024);
    for _ in 0..100_000 {
        chunk.extend_from_slice(&[b'a'; 90]);
        chunk.push(b'\n');
    }
    ctx.write_chunk(&chunk);
    let content = ctx.log_content();
    assert!(
        content.len() <= 8 * 1024 * 1024,
        "log_content must be capped at 8 MiB, got {} bytes",
        content.len()
    );
}

/// R1-12 (file commands): GITHUB_ENV-style files over 1 MiB are rejected
/// before being read whole.
#[test]
fn parse_kv_file_rejects_overlarge_input() {
    let dir = std::env::temp_dir().join(format!("preloop-r112-kv-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("env");
    let mut data = Vec::with_capacity(2 * 1024 * 1024);
    while data.len() < 2 * 1024 * 1024 {
        data.extend_from_slice(b"K=vvvvvvvvvvvvvvvvvvvv\n");
    }
    std::fs::write(&path, &data).unwrap();
    let err = parse_kv_file(&path).unwrap_err();
    assert!(
        err.to_string().contains("file-command size limit"),
        "expected a size-limit error, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// R1-12 (file commands): GITHUB_PATH-style files over 1 MiB are rejected
/// before being read whole.
#[test]
fn parse_path_file_rejects_overlarge_input() {
    let dir = std::env::temp_dir().join(format!("preloop-r112-path-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("path");
    let mut data = Vec::with_capacity(2 * 1024 * 1024);
    while data.len() < 2 * 1024 * 1024 {
        data.extend_from_slice(b"/some/very/long/path/entry\n");
    }
    std::fs::write(&path, &data).unwrap();
    let err = parse_path_file(&path).unwrap_err();
    assert!(
        err.to_string().contains("file-command size limit"),
        "expected a size-limit error, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// R1-12 (service logs / docker invocations): retained output lines are
/// capped even when the subprocess is chatty.
#[tokio::test]
async fn invoke_caps_retained_output_lines() {
    let result = process::invoke(
        "sh",
        &[
            "-c",
            "i=0; while [ $i -lt 200000 ]; do echo line$i; i=$((i+1)); done",
        ],
        &std::env::temp_dir(),
        &HashMap::new(),
        None,
        None,
        true,
    )
    .await
    .expect("invoke failed");
    assert!(
        result.lines.len() <= 100_000,
        "retained lines must be capped, got {}",
        result.lines.len()
    );
    assert!(
        result.stdout_lines.len() <= 100_000,
        "retained stdout lines must be capped, got {}",
        result.stdout_lines.len()
    );
    // The head of the output is intact; only the tail is dropped.
    assert_eq!(result.stdout_lines[0], "line0");
}
