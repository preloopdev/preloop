use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let versions_path = manifest_dir.join("../../versions.toml");
    let text = fs::read_to_string(&versions_path).expect("versions.toml must exist");

    let value = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == "smolvm_min_version").then(|| value.trim())
        })
        .and_then(|value| {
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .expect("smolvm_min_version must be a quoted value");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    fs::write(
        out_dir.join("pins.rs"),
        format!("pub const SMOLVM_MIN_VERSION: &str = \"{value}\";\n"),
    )
    .expect("write generated SmolVM pins");
    println!("cargo:rerun-if-changed=../../versions.toml");
}
