//! Compiles the SmolVM runtime floor from `versions.toml` at the workspace
//! root into this crate.
//!
//! The floor is read here because this crate both enforces the runtime
//! contract (`machine fork --freeze-source`) and explains a host that ran out
//! of virtio devices: the device-budget diagnostic names the release an
//! operator must upgrade to. `crates/preloop-cli` reads the same constant from
//! `preloop-vm`, so the pin stays one compiled value instead of two literals
//! that drift.
//!
//! The file is deliberately flat (`key = "value"` lines, `#` comments), the
//! same shape `crates/preloop-cli/build.rs` parses, so the parser stays
//! trivial and Renovate's regex managers can bump keys without understanding
//! TOML structure.

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let versions_path = manifest_dir.join("../../versions.toml");
    let text = fs::read_to_string(&versions_path).unwrap_or_else(|error| {
        panic!("read {}: {error}", versions_path.display());
    });
    let floor = text
        .lines()
        .find_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            if key.trim() != "smolvm_min_version" {
                return None;
            }
            let value = value.trim();
            (value.starts_with('"') && value.ends_with('"'))
                .then(|| value[1..value.len() - 1].to_owned())
        })
        .expect("smolvm_min_version must be pinned as a quoted version in versions.toml");
    println!("cargo:rustc-env=PRELOOP_SMOLVM_MIN_VERSION={floor}");
    println!("cargo:rerun-if-changed={}", versions_path.display());
}
