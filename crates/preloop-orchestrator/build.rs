//! Generates version-pin constants from `versions.toml` at the workspace
//! root, so every pinned version lives in one version-controlled file
//! instead of as literals scattered through the code.
//!
//! The file is deliberately flat (`key = "value"` lines, `#` comments) so
//! this parser stays trivial and any tooling (Renovate regex managers, etc.)
//! can bump keys without understanding TOML structure. SHA pins under
//! `[node_externals.sha256]` are collected into a map for checksum verification.
//! Node externals versions fall back to embedded defaults when absent so the
//! code compiles against an older `versions.toml` (task 1 owns version bumps).

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::PathBuf;

const DEFAULT_NODE_SHA256: &[(&str, &str)] = &[
    (
        "node20_20.19.0_linux-arm64",
        "618e4294602b78e97118a39050116b70d088b16197cd3819bba1fc18b473dfc4",
    ),
    (
        "node20_20.19.0_linux-x64",
        "8a4dbcdd8bccef3132d21e8543940557e55dcf44f00f0a99ba8a062f4552e722",
    ),
    (
        "node20_20.19.0_darwin-arm64",
        "c016cd1975a264a29dc1b07c6fbe60d5df0a0c2beb4113c0450e3d998d1a0d9c",
    ),
    (
        "node20_20.19.0_darwin-x64",
        "a8554af97d6491fdbdabe63d3a1cfb9571228d25a3ad9aed2df856facb131b20",
    ),
    (
        "node20_20.19.0_win-x64",
        "be72284c7bc62de07d5a9fd0ae196879842c085f11f7f2b60bf8864c0c9d6a4f",
    ),
    (
        "node20_20.19.0_win-arm64",
        "773325a26ad51a5ba857963825dee3a871eacef653c31d62e5492574c965accb",
    ),
    (
        "node24_24.3.0_linux-arm64",
        "371fc060d5dd4de565586c3cc70034956db67a8f3dae0f0e5724fa56147c472a",
    ),
    (
        "node24_24.3.0_linux-x64",
        "bbeb5fb8113b44fc30f5a5887dbc0ab66af8e56139f5f9fbe7c7a1aa056246dc",
    ),
    (
        "node24_24.3.0_darwin-arm64",
        "fee91aa5febeda47ef9f6c0afd2f2bcd3dacb0e656c29de0b5274e0ea1ca3565",
    ),
    (
        "node24_24.3.0_darwin-x64",
        "0c065ffa4e53b1a172ab9cd8ca08ae141b187aca8a07403c6856a7b8d0024804",
    ),
    (
        "node24_24.3.0_win-x64",
        "c0c8efbca1b57e5b074bbdf7cef1ccca40979d6b46e5bcadaad5d4b07cbb3b10",
    ),
    (
        "node24_24.3.0_win-arm64",
        "95ff08f6b2763d8491faba46b3a0ba1fb2045e029484494235b20b17c9053208",
    ),
    (
        "node20_20.20.2_linux-arm64",
        "47ef73d543ecf6eb19435f6c03a0ac4809b3bf0dd6b26c7c571efc2a6572a74d",
    ),
    (
        "node20_20.20.2_linux-x64",
        "19e56f0825510207dd904f087fe52faa0a4eb6b2aab5f0ea7a33830d04888b8b",
    ),
    (
        "node20_20.20.2_darwin-arm64",
        "466e05f3477c20dfb723054dfebffe55bc74660ee77f612166fca121dacb65b6",
    ),
    (
        "node20_20.20.2_darwin-x64",
        "8be6f5e4bb128c82774f8a0b8d7a1cc1365a7977d9657cece0ca647b3fe04e61",
    ),
    (
        "node20_20.20.2_win-x64",
        "dc3700fdd57a63eedb8fd7e3c7baaa32e6a740a1b904167ff4204bc68ed8bf77",
    ),
    (
        "node20_20.20.2_win-arm64",
        "f066ba3f80363f8e16a2737a945052ea910733f22c93821519f53667614bafd0",
    ),
    (
        "node24_24.19.0_linux-arm64",
        "d28c8a5bf0a808f0ed434a1dce8c54ae98f0371c0bd86ac58abc613f73e6643f",
    ),
    (
        "node24_24.19.0_linux-x64",
        "f625d97cd707df4ff96254916fbc5ff014f09c09effe5a1e0ca8f6d41a8789d4",
    ),
    (
        "node24_24.19.0_darwin-arm64",
        "8294b7aa9b03997481c06babf1e8b270c859358f27da57a11509afe537ac381d",
    ),
    (
        "node24_24.19.0_darwin-x64",
        "d1b5e999db158c62fe8f7267a4476b035d8bd93b1a605bac24a3f0dd166e3316",
    ),
    (
        "node24_24.19.0_win-x64",
        "57f71ab3652e797d84acddc79c81cc9ff1c6ddb2a1974cdb83f00fee9bff4c73",
    ),
    (
        "node24_24.19.0_win-arm64",
        "755b023e729dac63c4d746883ca97903abfc278813d7b9b106ed3aea87b3278c",
    ),
];

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let versions_path = manifest_dir.join("../../versions.toml");
    let text = fs::read_to_string(&versions_path).expect("versions.toml must exist");

    let mut pins: BTreeMap<String, String> = BTreeMap::new();
    let mut sha_pins: BTreeMap<String, String> = BTreeMap::new();
    let mut in_sha_section = false;

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Section headers: [node_externals.sha256] etc.
        if line.starts_with('[') && line.ends_with(']') {
            let section = line[1..line.len() - 1].trim().to_ascii_lowercase();
            in_sha_section =
                section == "node_externals.sha256" || section == "node_externals.sha256\"";
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().to_owned();
        let value = value.trim();
        if !(value.starts_with('"') && value.ends_with('"')) {
            continue;
        }
        let value = value[1..value.len() - 1].to_owned();
        pins.insert(key.clone(), value.clone());
        if in_sha_section {
            sha_pins.insert(key.clone(), value.clone());
        }
    }

    // Populate fallback SHA pins for default versions if not overridden in versions.toml
    for (k, v) in DEFAULT_NODE_SHA256 {
        sha_pins
            .entry((*k).to_owned())
            .or_insert_with(|| (*v).to_owned());
    }

    let mut out = String::from("// Generated by build.rs from versions.toml. Do not edit.\n");

    // Emit every pin as UPPER_SNAKE constant for direct access.
    for (key, value) in &pins {
        let const_name = key.trim().to_uppercase().replace(['.', '-'], "_");
        out.push_str(&format!("pub const {const_name}: &str = \"{value}\";\n"));
    }

    // Ensure node externals version constants always exist with fallback.
    // Priority: node20_externals_version -> node20_version (hypothetical rename) -> default.
    let node20 = pins
        .get("node20_externals_version")
        .or_else(|| pins.get("node20_version"))
        .cloned()
        .unwrap_or_else(|| "20.19.0".to_owned());
    let node24 = pins
        .get("node24_externals_version")
        .or_else(|| pins.get("node24_version"))
        .cloned()
        .unwrap_or_else(|| "24.3.0".to_owned());

    if !pins.contains_key("node20_externals_version") && !pins.contains_key("node20_version") {
        out.push_str(&format!(
            "pub const NODE20_EXTERNALS_VERSION: &str = \"{node20}\";\n"
        ));
    }
    if pins.contains_key("node20_version") && !pins.contains_key("node20_externals_version") {
        out.push_str(&format!(
            "pub const NODE20_EXTERNALS_VERSION: &str = \"{node20}\";\n"
        ));
    }
    if !pins.contains_key("node24_externals_version") && !pins.contains_key("node24_version") {
        out.push_str(&format!(
            "pub const NODE24_EXTERNALS_VERSION: &str = \"{node24}\";\n"
        ));
    }
    if pins.contains_key("node24_version") && !pins.contains_key("node24_externals_version") {
        out.push_str(&format!(
            "pub const NODE24_EXTERNALS_VERSION: &str = \"{node24}\";\n"
        ));
    }

    // Emit SHA map. Keys are kept as-is (e.g. node20_20.19.0_linux-arm64) for lookup.
    out.push_str("pub const NODE_EXTERNALS_SHA256: &[(&str, &str)] = &[\n");
    for (key, value) in &sha_pins {
        let escaped_key = key.replace('\\', "\\\\").replace('"', "\\\"");
        let escaped_val = value.replace('\\', "\\\\").replace('"', "\\\"");
        out.push_str(&format!("    (\"{escaped_key}\", \"{escaped_val}\"),\n"));
    }
    out.push_str("];\n");

    // Helper to look up SHA by key at runtime.
    out.push_str("pub fn node_externals_pinned_sha256(key: &str) -> Option<&'static str> {\n");
    out.push_str("    NODE_EXTERNALS_SHA256.iter().find_map(|(k, v)| if *k == key { Some(*v) } else { None })\n");
    out.push_str("}\n");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    fs::write(out_dir.join("pins.rs"), out).expect("write pins.rs");
    println!("cargo:rerun-if-changed=../../versions.toml");
}
