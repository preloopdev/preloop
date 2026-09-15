//! Managed AgentENV egress exception for the engine's runner-facing address.
//!
//! AgentENV denies every private destination inside each sandbox netns
//! (`[network.egress].always_denied_cidrs`, evaluated before any per-sandbox
//! rule — a per-sandbox allow can never lift it), and the host `INPUT` chain
//! additionally rejects new guest→host connections. A guest runner can
//! therefore never reach the control plane without two host-side exceptions:
//!
//! 1. the node deny-list must stop covering exactly the engine's /32 (every
//!    other private address stays denied), and
//! 2. the engine port must be ACCEPTed from the sandbox pools ahead of the
//!    blanket veth REJECT (which AgentENV re-inserts on every service start).
//!
//! This module reconciles both from the engine process itself when the
//! AgentENV backend is selected, so the exception is code instead of a
//! runbook: verify first, mutate only on drift, restart the `aenv` service
//! only when its config actually changed. Everything decision-shaped is a
//! pure function with unit tests; the privileged shell-outs are a thin skin
//! around `sudo -n`, `iptables`, and `systemctl`.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::Context;

/// Node config path, unless `PRELOOP_AENV_CONFIG` overrides it (testing).
const DEFAULT_CONFIG_PATH: &str = "/var/lib/aenv/config/config.toml";
/// Opt-out: `PRELOOP_AENV_MANAGE_EGRESS=0|false|no|off` skips reconciliation
/// with a warning. `dry-run` logs the planned actions and changes nothing.
const MANAGE_ENV: &str = "PRELOOP_AENV_MANAGE_EGRESS";
/// Explicit engine address when it cannot be read off the runner URL.
const ENGINE_IP_ENV: &str = "PRELOOP_AENV_ENGINE_IP";
/// Config path override for tests and unusual layouts.
const CONFIG_ENV: &str = "PRELOOP_AENV_CONFIG";
/// Fallback sandbox pools when the node config does not name them.
const DEFAULT_POOLS: [&str; 2] = ["10.11.0.0/16", "10.12.0.0/16"];

/// An IPv4 CIDR: address masked to `prefix` length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Cidr {
    addr: u32,
    prefix: u8,
}

impl Cidr {
    fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        let (ip, prefix) = match text.split_once('/') {
            Some((ip, prefix)) => (ip, prefix.parse::<u8>().ok()?),
            None => (text, 32),
        };
        if prefix > 32 {
            return None;
        }
        let octets: Vec<u8> = ip
            .split('.')
            .map(str::parse)
            .collect::<Result<_, _>>()
            .ok()?;
        if octets.len() != 4 {
            return None;
        }
        let addr = u32::from_be_bytes([octets[0], octets[1], octets[2], octets[3]]);
        let mask = if prefix == 0 {
            0
        } else {
            u32::MAX << (32 - prefix)
        };
        Some(Self {
            addr: addr & mask,
            prefix,
        })
    }

    fn contains_ip(self, ip: u32) -> bool {
        let mask = if self.prefix == 0 {
            0
        } else {
            u32::MAX << (32 - self.prefix)
        };
        (ip & mask) == self.addr
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.addr.to_be_bytes();
        write!(f, "{}.{}.{}.{}/{}", b[0], b[1], b[2], b[3], self.prefix)
    }
}

/// The exact complement of `allowed/32` inside `covering`: the minimal set of
/// CIDRs covering every address of `covering` except `allowed`. Walking from
/// the /32 up to the covering prefix, each level contributes the sibling the
/// allowed address is not in. Returns `None` when `covering` does not
/// strictly contain the address.
fn exclude_host(covering: Cidr, allowed: u32) -> Option<Vec<Cidr>> {
    if covering.prefix >= 32 || !covering.contains_ip(allowed) {
        return None;
    }
    let mut out = Vec::new();
    // Level `p` is the sibling /p of the /p prefix containing `allowed`.
    for p in (covering.prefix + 1..=32).rev() {
        let mask = u32::MAX << (32 - p);
        let sibling = (allowed & mask) ^ (1 << (32 - p));
        out.push(Cidr {
            addr: sibling,
            prefix: p,
        });
    }
    out.sort_by_key(|c| (c.addr, c.prefix));
    Some(out)
}

/// Rewrite the `always_denied_cidrs` array so `engine` is reachable and every
/// other listed range stays denied. Returns `None` when the text already
/// encodes that state (idempotent: a second run is a no-op).
fn rewrite_denied_list(text: &str, engine: Ipv4Addr) -> Option<String> {
    let engine_u32 = u32::from_be_bytes(engine.octets());
    let start = text.find("always_denied_cidrs")?;
    let open = text[start..].find('[')? + start;
    let close = text[open..].find(']')? + open;
    let mut kept: Vec<String> = Vec::new();
    let mut complements: Vec<Cidr> = Vec::new();
    let mut seen: Vec<Cidr> = Vec::new();
    for raw in text[open + 1..close].split(',') {
        let cidr_text = raw.trim().trim_matches('"').trim();
        if cidr_text.is_empty() {
            continue;
        }
        let Some(network) = Cidr::parse(cidr_text) else {
            // Not a CIDR we understand: keep verbatim rather than dropping.
            kept.push(cidr_text.to_owned());
            continue;
        };
        if network.prefix == 32 && network.addr == engine_u32 {
            // An explicit deny of the engine itself: drop it.
            continue;
        }
        match exclude_host(network, engine_u32) {
            Some(complement) => complements.extend(complement),
            None => {
                if !seen.contains(&network) {
                    seen.push(network);
                    kept.push(network.to_string());
                }
            }
        }
    }
    // Entries that never covered the engine stay first, in original order;
    // the surgical complement follows. Dedupe across both.
    let mut rendered: Vec<String> = Vec::new();
    for entry in kept {
        if !rendered.contains(&entry) {
            rendered.push(entry);
        }
    }
    for cidr in complements {
        let text = cidr.to_string();
        if !rendered.contains(&text) {
            rendered.push(text);
        }
    }
    let mut out = String::with_capacity(text.len() + 64);
    // `text[..open]` already ends with the `always_denied_cidrs = ` prefix;
    // the replacement starts at the bracket so a second run parses back the
    // same entries and reaches a fixpoint.
    out.push_str("[\n");
    for entry in &rendered {
        out.push_str(&format!("  \"{entry}\",\n"));
    }
    out.push(']');
    let rewritten = format!("{}{}{}", &text[..open], out, &text[close + 1..]);
    (rewritten != text).then_some(rewritten)
}

/// Read `host_interaction_cidr` and `veth_cidr` from the node config,
/// falling back to the shipped pools when they are absent or unparsable.
fn sandbox_pools(config_text: &str) -> Vec<String> {
    let mut pools = Vec::new();
    for key in ["host_interaction_cidr", "veth_cidr"] {
        let mut value = None;
        for line in config_text.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix(&format!("{key} =")) {
                value = Some(
                    rest.trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .trim()
                        .to_owned(),
                );
            }
        }
        if let Some(value) = value {
            if Cidr::parse(&value).is_some() {
                pools.push(value);
                continue;
            }
        }
        pools.push(DEFAULT_POOLS[pools.len()].to_owned());
    }
    pools
}

fn config_path() -> PathBuf {
    std::env::var_os(CONFIG_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH))
}

/// Run `program` with `sudo -n` (never interactive: either the operator
/// granted NOPASSWD for it or the engine must fail loudly, not hang).
fn sudo(program: &str, args: &[&str]) -> anyhow::Result<std::process::Output> {
    let output = Command::new("sudo")
        .args(["-n", "--", program])
        .args(args)
        .output()
        .with_context(|| format!("spawning sudo {program}"))?;
    Ok(output)
}

fn sudo_ok(program: &str, args: &[&str], what: &str) -> anyhow::Result<()> {
    let output = sudo(program, args)?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    anyhow::bail!(
        "{what} needs privileged `{program}` (sudo -n failed: {}). Grant NOPASSWD for it, run the engine as root, or set {MANAGE_ENV}=0 to manage egress by hand",
        stderr.trim()
    )
}

fn service_active() -> bool {
    Command::new("systemctl")
        .args(["is-active", "--quiet", "aenv"])
        .status()
        .is_ok_and(|status| status.success())
}

/// Ensure the engine at `server_url` is reachable from AgentENV sandboxes:
/// surgical node deny-list exception plus ordered host firewall rules.
/// See the module docs for the two gates this reconciles.
pub(crate) fn ensure_engine_egress(server_url: &str) -> anyhow::Result<()> {
    let mode = std::env::var(MANAGE_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "on".to_owned());
    if matches!(mode.as_str(), "0" | "false" | "no" | "off") {
        tracing::warn!(
            "{MANAGE_ENV} is {mode}: skipping AgentENV egress reconciliation; \
             sandboxes cannot reach a private engine unless the node deny-list \
             and host firewall were opened by hand"
        );
        return Ok(());
    }
    let dry_run = mode == "dry-run";

    let engine: Ipv4Addr = match std::env::var(ENGINE_IP_ENV).ok() {
        Some(value) => value
            .trim()
            .parse()
            .with_context(|| format!("{ENGINE_IP_ENV} must be an IPv4 address, got `{value}`"))?,
        None => {
            let parsed = reqwest::Url::parse(server_url)
                .with_context(|| format!("parsing runner URL `{server_url}`"))?;
            let host = parsed.host_str().unwrap_or_default().to_owned();
            // Strip the brackets reqwest keeps on IPv6 literals (unsupported
            // below regardless; the strip keeps the error about the address,
            // not the punctuation).
            let bare = host
                .strip_prefix('[')
                .and_then(|inner| inner.strip_suffix(']'))
                .unwrap_or(&host);
            bare.parse().with_context(|| {
                format!(
                    "runner URL host `{host}` is not an IPv4 literal; set {ENGINE_IP_ENV} \
                     to the engine's LAN address so sandboxes can be pointed at it"
                )
            })?
        }
    };
    let port: u16 = reqwest::Url::parse(server_url)
        .ok()
        .and_then(|url| url.port())
        .with_context(|| {
            format!("runner URL `{server_url}` has no explicit port for the firewall exception")
        })?;

    let path = config_path();
    let current = std::fs::read_to_string(&path)
        .with_context(|| format!("reading AgentENV node config {}", path.display()))?;
    let pools = sandbox_pools(&current);

    if dry_run {
        tracing::info!(%engine, port, config = %path.display(), pools = ?pools, "egress dry-run: no changes made");
    }
    if let Some(rewritten) = rewrite_denied_list(&current, engine) {
        if dry_run {
            tracing::info!("egress dry-run: node deny-list would be rewritten");
        } else {
            tracing::warn!(%engine, "engine address is node-denied; rewriting the deny-list complement");
            write_config_atomically(&path, &rewritten)?;
            restart_aenv()?;
        }
    }
    ensure_input_rules(&pools, port, dry_run)?;
    if !dry_run {
        tracing::info!(%engine, port, "AgentENV engine egress reconciled");
    }
    Ok(())
}

/// The engine-port ACCEPT for each sandbox pool, ordered ahead of AgentENV's
/// blanket veth REJECT (which the service re-inserts at the head on every
/// start — later inserts would be shadowed, so dedupe-then-prepend).
fn ensure_input_rules(pools: &[String], port: u16, dry_run: bool) -> anyhow::Result<()> {
    fn refs(owned: &[String]) -> Vec<&str> {
        owned.iter().map(String::as_str).collect()
    }
    let port = port.to_string();
    // `sudo`/`iptables` take argv slices; build owned strings once per pool so
    // the borrowed views below never dangle.
    let with_prefix = |prefix: &[&str], rule: &[String]| -> Vec<String> {
        prefix
            .iter()
            .map(|s| s.to_string())
            .chain(rule.iter().cloned())
            .collect()
    };
    for pool in pools {
        let rule: Vec<String> = [
            "-s",
            pool.as_str(),
            "-p",
            "tcp",
            "--dport",
            port.as_str(),
            "-j",
            "ACCEPT",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        if dry_run {
            tracing::info!(%pool, port = %port, "egress dry-run: would ensure INPUT ACCEPT");
            continue;
        }
        // Drain duplicates first so repeated runs cannot pile up rules.
        loop {
            let check = with_prefix(&["-C", "INPUT"], &rule);
            if !sudo("iptables", &refs(&check))?.status.success() {
                break;
            }
            let delete = with_prefix(&["-D", "INPUT"], &rule);
            sudo_ok("iptables", &refs(&delete), "firewall dedupe")?;
        }
        let insert = with_prefix(&["-I", "INPUT", "1"], &rule);
        sudo_ok("iptables", &refs(&insert), "firewall exception")?;
    }
    Ok(())
}

/// Backup, then atomically replace the node config.
fn write_config_atomically(path: &Path, rewritten: &str) -> anyhow::Result<()> {
    let backup = path.with_extension(format!(
        "toml.bak.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    ));
    let backup_str = backup.to_string_lossy().into_owned();
    sudo_ok(
        "cp",
        &[&path.to_string_lossy(), &backup_str],
        "node config backup",
    )?;
    let parent = path.parent().unwrap_or(Path::new("/tmp"));
    let staged = parent.join(format!(".preloop-egress.{}.tmp", std::process::id()));
    let staged_str = staged.to_string_lossy().into_owned();
    std::fs::write(&staged, rewritten).context("staging rewritten node config")?;
    // The staged file is ours (no sudo needed to move it over, but the
    // destination is root-owned, so the rename itself goes through sudo).
    sudo_ok(
        "mv",
        &[&staged_str, &path.to_string_lossy()],
        "node config install",
    )?;
    Ok(())
}

/// Restart the AgentENV service after a config change and wait for it to
/// come back. Nothing else re-reads the node deny-list.
fn restart_aenv() -> anyhow::Result<()> {
    tracing::warn!("restarting the `aenv` service for the deny-list change; sandboxes keep running, new starts pick up the policy");
    sudo_ok(
        "systemctl",
        &["restart", "aenv"],
        "AgentENV service restart",
    )?;
    for _ in 0..30 {
        if service_active() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    anyhow::bail!(
        "`aenv` service did not become active after restart; inspect `systemctl status aenv`"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complement_excludes_exactly_one_host() {
        // The textbook case: one /32 out of a /16 is 16 prefixes.
        let covering = Cidr::parse("192.168.0.0/16").unwrap();
        let allowed = u32::from_be_bytes(Ipv4Addr::new(192, 168, 8, 174).octets());
        let complement = exclude_host(covering, allowed).unwrap();
        assert_eq!(complement.len(), 16);
        // Nothing in the complement contains the allowed host...
        assert!(complement.iter().all(|c| !c.contains_ip(allowed)));
        // ...and every other address of the /16 is covered exactly once.
        let mut covered = 0u64;
        for cidr in &complement {
            assert!(covering.prefix <= cidr.prefix && covering.contains_ip(cidr.addr));
            covered += 1u64 << (32 - cidr.prefix);
        }
        assert_eq!(covered, (1u64 << 16) - 1);
        // Spot-check the interesting edge: the allowed /32's sibling /32.
        assert!(complement
            .iter()
            .any(|c| c.to_string() == "192.168.8.175/32"));
        // A covering prefix that does not strictly contain the host refuses.
        assert!(exclude_host(Cidr::parse("10.0.0.0/8").unwrap(), allowed).is_none());
        assert!(exclude_host(Cidr::parse("192.168.8.174/32").unwrap(), allowed).is_none());
    }

    #[test]
    fn rewrite_is_idempotent_and_surgical() {
        let engine = Ipv4Addr::new(192, 168, 8, 174);
        let config = "always_denied_cidrs = [\n  \"10.0.0.0/8\",\n  \"192.168.0.0/16\",\n]";
        let once = rewrite_denied_list(config, engine).expect("first run rewrites");
        assert!(once.contains("\"10.0.0.0/8\""));
        assert!(!once.contains("\"192.168.0.0/16\""));
        assert!(!once.contains("\"192.168.8.174/32\""));
        assert!(once.contains("\"192.168.8.175/32\""));
        // Second run: steady state, no rewrite.
        assert!(rewrite_denied_list(&once, engine).is_none());
        // An explicit /32 deny of the engine is dropped.
        let explicit = "always_denied_cidrs = [\n  \"192.168.8.174/32\",\n]";
        let fixed = rewrite_denied_list(explicit, engine).expect("explicit deny is dropped");
        assert!(!fixed.contains("192.168.8.174"));
    }

    #[test]
    fn pools_fall_back_to_shipped_defaults() {
        let config = "[network.internal]\nhost_interaction_cidr = \"10.11.0.0/16\"\n";
        let pools = sandbox_pools(config);
        assert_eq!(
            pools,
            vec!["10.11.0.0/16".to_owned(), "10.12.0.0/16".to_owned()]
        );
        assert_eq!(
            sandbox_pools(""),
            vec!["10.11.0.0/16".to_owned(), "10.12.0.0/16".to_owned()]
        );
    }

    fn with_egress_env(vars: &[(&str, &str)], test: impl FnOnce()) {
        let previous: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(key, _)| (key.to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in vars {
            // SAFETY: single-threaded test manipulation, restored below.
            unsafe { std::env::set_var(key, value) };
        }
        test();
        for (key, value) in previous {
            // SAFETY: restoring what the setup above changed.
            unsafe {
                match value {
                    Some(value) => std::env::set_var(&key, value),
                    None => std::env::remove_var(&key),
                }
            }
        }
    }

    #[test]
    fn dry_run_reconciles_nothing_but_validates_everything() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        std::fs::write(
            &config,
            "always_denied_cidrs = [\n  \"10.0.0.0/8\",\n  \"192.168.0.0/16\",\n]\n",
        )
        .unwrap();
        let before = std::fs::read_to_string(&config).unwrap();
        with_egress_env(
            &[
                (MANAGE_ENV, "dry-run"),
                (CONFIG_ENV, config.to_str().expect("fixture path is UTF-8")),
            ],
            || {
                ensure_engine_egress("http://192.168.8.174:9491").expect("dry run succeeds");
            },
        );
        // Dry run changes nothing on disk...
        assert_eq!(std::fs::read_to_string(&config).unwrap(), before);
        // ...while a loopback URL without an override is rejected, not probed.
        with_egress_env(
            &[
                (MANAGE_ENV, "dry-run"),
                (CONFIG_ENV, config.to_str().expect("fixture path is UTF-8")),
            ],
            || {
                // 127.0.0.1 parses as an IP, so it is accepted as an address
                // here (the loopback *policy* decision lives in pool config);
                // a hostname without an override is what must fail.
                assert!(ensure_engine_egress("http://engine.local:9491").is_err());
            },
        );
    }
}
