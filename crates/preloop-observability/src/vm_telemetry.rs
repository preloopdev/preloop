#![allow(missing_docs)]
#[cfg(target_os = "linux")]
use std::path::Path;

/// Sample host memory reality: totals, availability, engine RSS, VM RSS,
/// and the top RSS processes. Linux reads `/proc` (`VmRSS` is already KiB,
/// so no page-size dependency); every other platform reports unavailable.
/// Pure parsing lives in testable helpers below; this is thin I/O.
pub fn sample_host() -> HostSample {
    #[cfg(target_os = "linux")]
    {
        sample_host_linux()
    }
    #[cfg(not(target_os = "linux"))]
    {
        HostSample::unavailable()
    }
}

/// One process's measured footprint for top-consumer reporting.
#[derive(Debug, Clone)]
pub struct HostTopProcess {
    pub pid: u32,
    pub rss_bytes: u64,
    /// First token of the command line (truncated); identity, not args.
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct HostSample {
    pub cpu_time_secs: Option<f64>,
    pub throttled_secs: Option<f64>,
    pub memory_bytes: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub pids_current: Option<u32>,
    pub sparse_allocated_bytes: Option<u64>,
    pub pid_valid: bool,
    /// Host RAM totals from `/proc/meminfo`. `None` = unmeasured.
    pub mem_total_bytes: Option<u64>,
    pub mem_available_bytes: Option<u64>,
    pub swap_total_bytes: Option<u64>,
    pub swap_free_bytes: Option<u64>,
    /// This process's RSS (`VmRSS`, the engine itself).
    pub engine_rss_bytes: Option<u64>,
    /// Summed RSS of guest VM processes (libkrun `_boot-vm` children).
    pub vm_rss_bytes: Option<u64>,
    /// Top RSS processes host-wide, descending.
    pub top_processes: Vec<HostTopProcess>,
}

impl HostSample {
    pub fn unavailable() -> Self {
        Self {
            cpu_time_secs: None,
            throttled_secs: None,
            memory_bytes: None,
            memory_limit_bytes: None,
            pids_current: None,
            sparse_allocated_bytes: None,
            pid_valid: false,
            mem_total_bytes: None,
            mem_available_bytes: None,
            swap_total_bytes: None,
            swap_free_bytes: None,
            engine_rss_bytes: None,
            vm_rss_bytes: None,
            top_processes: Vec::new(),
        }
    }

    /// Fraction of RAM consumed (1.0 = exhausted), or `None` unmeasured.
    pub fn ram_used_fraction(&self) -> Option<f64> {
        match (self.mem_total_bytes, self.mem_available_bytes) {
            (Some(total), Some(available)) if total > 0 => {
                Some(1.0 - available as f64 / total as f64)
            }
            _ => None,
        }
    }

    /// Fraction of swap consumed, or `None` when the host has no swap or
    /// it is unmeasured. Swap is pressure, never headroom.
    pub fn swap_used_fraction(&self) -> Option<f64> {
        match (self.swap_total_bytes, self.swap_free_bytes) {
            (Some(total), Some(free)) if total > 0 => Some(1.0 - free as f64 / total as f64),
            _ => None,
        }
    }
}

/// Parse a `/proc/meminfo`-style `Key: <kB> kB` value.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_meminfo_kib(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (name, rest) = line.split_once(':')?;
        if name.trim() != key {
            return None;
        }
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })
}

/// Parse a `VmRSS: <kB> kB` line from `/proc/<pid>/status`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_status_rss_kib(text: &str) -> Option<u64> {
    parse_meminfo_kib(text, "VmRSS")
}

/// First command-line token for process identity (no args leak into telemetry).
#[cfg(target_os = "linux")]
fn short_command_name(cmdline: &[u8]) -> String {
    let first = cmdline.split(|byte| *byte == 0).next().unwrap_or_default();
    let text = String::from_utf8_lossy(first);
    const LIMIT: usize = 64;
    let short: String = text.chars().take(LIMIT).collect();
    if short.is_empty() {
        return "[kernel]".to_owned();
    }
    // Basename: `/proc/self/exe` noise and temp paths don't identify anything.
    short.rsplit('/').next().unwrap_or(&short).to_owned()
}

/// Scan `proc_root` (`/proc`) for per-process RSS without forking or extra
/// dependencies. Returns `(own_rss_kib, vm_rss_kib, top)` where `top` is at
#[cfg(target_os = "linux")]
fn scan_processes(proc_root: &Path) -> (Option<u64>, u64, Vec<HostTopProcess>) {
    let own_pid = std::process::id();
    let mut own_rss = None;
    let mut vm_rss = 0u64;
    let mut all: Vec<(u32, u64)> = Vec::new();
    let entries = std::fs::read_dir(proc_root).map(|read| {
        read.filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.bytes().all(|byte| byte.is_ascii_digit()))
            })
            .collect::<Vec<_>>()
    });
    for entry in entries.into_iter().flatten() {
        let pid: u32 = match entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        {
            Some(pid) => pid,
            None => continue,
        };
        let status = std::fs::read_to_string(entry.path().join("status")).unwrap_or_default();
        let Some(rss_kib) = parse_status_rss_kib(&status) else {
            continue;
        };
        if pid == own_pid {
            own_rss = Some(rss_kib);
        }
        all.push((pid, rss_kib));
    }
    // Guest VMs are libkrun children whose argv carries `_boot-vm`
    // (argv[0] is `/proc/self/exe`, so the basename alone never matches —
    // scan the whole command line). Cmdline reads stay bounded: top
    // candidates first, then only unclassified PIDs for the VM total.
    let is_guest_vm = |cmdline: &[u8]| cmdline.windows(8).any(|w| w == b"_boot-vm");
    let mut top = Vec::new();
    for (pid, rss_kib) in all.iter().take(8) {
        let cmdline =
            std::fs::read(proc_root.join(pid.to_string()).join("cmdline")).unwrap_or_default();
        let guest = is_guest_vm(&cmdline);
        if guest {
            vm_rss += rss_kib;
        }
        top.push(HostTopProcess {
            pid: *pid,
            rss_bytes: rss_kib.saturating_mul(1024),
            name: if guest {
                "_boot-vm".to_owned()
            } else {
                short_command_name(&cmdline)
            },
        });
    }
    if all.len() > 8 {
        for (pid, rss_kib) in all.iter().skip(8) {
            let cmdline =
                std::fs::read(proc_root.join(pid.to_string()).join("cmdline")).unwrap_or_default();
            if is_guest_vm(&cmdline) {
                vm_rss += rss_kib;
            }
        }
    }
    (own_rss, vm_rss, top)
}

#[cfg(target_os = "linux")]
fn sample_host_linux() -> HostSample {
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let kib = |key: &str| parse_meminfo_kib(&meminfo, key).map(|kib| kib.saturating_mul(1024));
    let (own_rss_kib, vm_rss_kib, top) = scan_processes(Path::new("/proc"));
    HostSample {
        cpu_time_secs: None,
        throttled_secs: None,
        memory_bytes: None,
        memory_limit_bytes: None,
        pids_current: None,
        sparse_allocated_bytes: None,
        pid_valid: true,
        mem_total_bytes: kib("MemTotal"),
        mem_available_bytes: kib("MemAvailable"),
        swap_total_bytes: kib("SwapTotal"),
        swap_free_bytes: kib("SwapFree"),
        engine_rss_bytes: own_rss_kib.map(|kib| kib.saturating_mul(1024)),
        vm_rss_bytes: Some(vm_rss_kib.saturating_mul(1024)),
        top_processes: top,
    }
}

/// One process's RSS (`VmRSS` from `/proc/<pid>/status`, already KiB) for a
/// driver reporting per-sandbox memory at `Heartbeat`. Linux only; every
/// other platform returns `None` rather than a fabricated value.
pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        parse_status_rss_kib(&status).map(|kib| kib.saturating_mul(1024))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meminfo_parses_kib_values() {
        let text = "MemTotal:       31305472 kB\nMemFree:          976896 kB\nMemAvailable:    2749252 kB\nSwapTotal:       8388604 kB\nSwapFree:        8191996 kB\n";
        assert_eq!(parse_meminfo_kib(text, "MemTotal"), Some(31305472));
        assert_eq!(parse_meminfo_kib(text, "MemAvailable"), Some(2749252));
        assert_eq!(parse_meminfo_kib(text, "SwapFree"), Some(8191996));
        assert_eq!(parse_meminfo_kib(text, "NoSuchKey"), None);
        assert_eq!(parse_meminfo_kib("MemTotal: lots kB\n", "MemTotal"), None);
    }

    #[test]
    fn status_rss_reads_vmrss_only() {
        let text =
            "Name:\tpreloop\nVmPeak:\t  8123456 kB\nVmRSS:\t  7416772 kB\nVmSwap:\t       0 kB\n";
        assert_eq!(parse_status_rss_kib(text), Some(7416772));
        assert_eq!(parse_status_rss_kib("Name:\tpreloop\n"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn command_name_uses_basename_without_args() {
        assert_eq!(
            short_command_name(b"/proc/self/exe\0_boot-vm\0/var/x\0"),
            "exe"
        );
        assert_eq!(short_command_name(b"/usr/bin/docker\0info\0"), "docker");
        assert_eq!(short_command_name(b""), "[kernel]");
    }

    #[test]
    fn fractions_measure_pressure_not_headroom() {
        let full = HostSample {
            mem_total_bytes: Some(100),
            mem_available_bytes: Some(7),
            swap_total_bytes: Some(100),
            swap_free_bytes: Some(0),
            ..HostSample::unavailable()
        };
        assert!((full.ram_used_fraction().unwrap() - 0.93).abs() < f64::EPSILON);
        assert!((full.swap_used_fraction().unwrap() - 1.0).abs() < f64::EPSILON);
        let no_swap = HostSample {
            swap_total_bytes: Some(0),
            swap_free_bytes: Some(0),
            ..HostSample::unavailable()
        };
        assert_eq!(no_swap.swap_used_fraction(), None);
        assert_eq!(HostSample::unavailable().ram_used_fraction(), None);
    }

    #[test]
    fn process_rss_bytes_of_a_bogus_pid_is_none() {
        // Never a fabricated value when the process cannot be read (also
        // covers non-Linux platforms, where this always returns `None`).
        assert_eq!(process_rss_bytes(u32::MAX), None);
    }
}
