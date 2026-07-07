//! Memory saturation test: generates 1M lines of output and measures RSS.
//! Compares chunk-based callback (raw bytes to disk) vs line-based.
//!
//! Usage: cargo run --example memtest --target aarch64-unknown-linux-musl

use std::io::Write;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let line_count: u64 = args.get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    eprintln!("=== Chunk-based memory saturation test ===");
    eprintln!("Generating {line_count} lines of ~50 bytes each");
    eprintln!("Total output: ~{} MB", line_count * 50 / 1_000_000);

    // Memory before
    let rss_before = get_rss_kb();
    eprintln!("RSS before: {rss_before} KB");

    // Create a temp file for the chunk callback to write to
    let mut log_file = tempfile::tempfile().expect("create temp file");

    let start = Instant::now();

    // Run a script that generates many lines
    let script = format!(
        "i=0; while [ $i -lt {line_count} ]; do echo \"line-$i: some padding text to make each line about fifty bytes\"; i=$((i+1)); done"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let result = aksh_runner::process::invoke(
            "sh",
            &["-c", &script],
            std::path::Path::new("."),
            &std::collections::HashMap::new(),
            Some(Box::new(move |chunk: &[u8]| {
                let _ = log_file.write_all(chunk);
            })),
            None,
            false,
        )
        .await;

        match result {
            Ok(out) => eprintln!("Process exit code: {}", out.exit_code),
            Err(e) => eprintln!("Process error: {e}"),
        }
    });

    let elapsed = start.elapsed();
    let rss_after = get_rss_kb();
    let rss_peak_kb = get_peak_rss_kb();

    eprintln!();
    eprintln!("=== Results ===");
    eprintln!("Lines:           {line_count}");
    eprintln!("Elapsed:         {:.2}s", elapsed.as_secs_f64());
    eprintln!("RSS before:      {rss_before} KB");
    eprintln!("RSS after:       {rss_after} KB");
    eprintln!("RSS peak:        {rss_peak_kb} KB");
    eprintln!("RSS delta (peak - before): {} KB", rss_peak_kb.saturating_sub(rss_before));
    eprintln!("RSS delta (after - before): {} KB", rss_after.saturating_sub(rss_before));

    if rss_peak_kb.saturating_sub(rss_before) < 10_000 {
        eprintln!();
        eprintln!("VERDICT: Memory is CONSTANT (<10 MB growth for 1M lines)");
    } else {
        eprintln!();
        eprintln!("VERDICT: Memory GROWS significantly ({} MB growth)", (rss_peak_kb - rss_before) / 1024);
    }
}

#[cfg(target_os = "linux")]
fn get_rss_kb() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in stat.lines() {
        if line.starts_with("VmRSS:") {
            return line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

#[cfg(target_os = "linux")]
fn get_peak_rss_kb() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in stat.lines() {
        if line.starts_with("VmHWM:") {
            return line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    0
}

#[cfg(not(target_os = "linux"))]
fn get_rss_kb() -> u64 { 0 }
#[cfg(not(target_os = "linux"))]
fn get_peak_rss_kb() -> u64 { 0 }
