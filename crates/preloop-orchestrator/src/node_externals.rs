//! Node externals manifest and cache validation helpers.
//!
//! Shared reasoning between host (`ensure_host_externals`), bundle materialization,
//! and runner-root (`preloop-runner configure`) — all three store each runtime
//! under `externals/node20` / `externals/node24` with a `preloop-node.json`
//! manifest that records the exact version, platform, archive digest, and source URL.
//!
//! Validation is: manifest exists + version matches expected + `bin/node --version`
//! prints `v<version>`. Any mismatch means the directory is stale/corrupt and
//! must be re-materialized (download into temp, verify checksums, atomic rename).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::process::Command;

/// Manifest written next to each `externals/nodeXX` directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeManifest {
    pub runtime: String,
    pub version: String,
    pub platform: String,
    pub archive_sha256: String,
    pub source: String,
}

impl NodeManifest {
    pub fn new(
        runtime: &str,
        version: &str,
        platform: &str,
        archive_sha256: &str,
        source: &str,
    ) -> Self {
        Self {
            runtime: runtime.to_owned(),
            version: version.to_owned(),
            platform: platform.to_owned(),
            archive_sha256: archive_sha256.to_owned(),
            source: source.to_owned(),
        }
    }
}

/// Current host platform string used in manifest and tarball name.
/// Matches Node's `linux-arm64` / `linux-x64` / `darwin-arm64` / `darwin-x64`
/// plus `win-x64` / `win-arm64` for Windows (extracted from Windows runner).
pub fn current_platform() -> String {
    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win"
    } else {
        "linux"
    };
    let arch = if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "x64"
    };
    format!("{os}-{arch}")
}

/// Tarball name for a given runtime version and platform.
/// Node's naming: `node-v20.19.0-linux-arm64.tar.gz` / `node-v20.19.0-win-x64.zip`.
pub fn archive_name(version: &str, platform: &str) -> String {
    let ver = if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    };
    if platform.starts_with("win") {
        format!("node-{ver}-{platform}.zip")
    } else {
        format!("node-{ver}-{platform}.tar.gz")
    }
}

/// Source URL for a given version + archive name.
pub fn source_url(version: &str, archive_name: &str) -> String {
    let ver = if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    };
    format!("https://nodejs.org/dist/{ver}/{archive_name}")
}

/// SHASUMS URL for a version.
pub fn shasums_url(version: &str) -> String {
    let ver = if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    };
    format!("https://nodejs.org/dist/{ver}/SHASUMS256.txt")
}

/// Expected SHA key for lookup in the compiled pinned table.
/// Format: `node20_20.19.0_linux-arm64` (matches `[node_externals.sha256]` keys).
pub fn pinned_key(runtime: &str, version: &str, platform: &str) -> String {
    let ver = version.trim_start_matches('v');
    format!("{runtime}_{ver}_{platform}")
}

/// Compute hex SHA256 of bytes.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Try to read the manifest at `dir/preloop-node.json`.
pub fn read_manifest(dir: &Path) -> Option<NodeManifest> {
    let path = dir.join("preloop-node.json");
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

/// Write manifest atomically via temp file + rename.
pub fn write_manifest(dir: &Path, manifest: &NodeManifest) -> std::io::Result<()> {
    let path = dir.join("preloop-node.json");
    let tmp = dir.join(".preloop-node.json.tmp");
    let data = serde_json::to_string_pretty(manifest).expect("manifest serializes");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Validate that `dir` is a correctly materialized externals dir for `runtime`/`expected_version`.
///
/// Checks:
/// - `dir/preloop-node.json` exists and its `version` equals `expected_version`
/// - `dir/bin/node` (or `node.exe` on Windows) exists and `bin/node --version` prints `v<version>`
///
/// Returns `true` only if all checks pass.
pub fn is_valid_externals_dir(dir: &Path, runtime: &str, expected_version: &str) -> bool {
    // Manifest check.
    let manifest = match read_manifest(dir) {
        Some(m) => m,
        None => return false,
    };
    let expected = expected_version.trim_start_matches('v');
    let got = manifest.version.trim_start_matches('v');
    if got != expected {
        return false;
    }
    if manifest.runtime != runtime {
        return false;
    }
    // Binary existence + version output.
    let node_bin = if cfg!(target_os = "windows") {
        dir.join("node.exe")
    } else {
        dir.join("bin/node")
    };
    if !node_bin.is_file() {
        return false;
    }

    let host_os = if cfg!(target_os = "macos") {
        "darwin"
    } else if cfg!(target_os = "windows") {
        "win"
    } else {
        "linux"
    };

    if manifest.platform.starts_with(host_os) {
        // Native binary: probe `bin/node --version` directly.
        let Ok(output) = Command::new(&node_bin).arg("--version").output() else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        stdout == format!("v{expected}")
    } else {
        // Non-native guest binary (e.g. linux-arm64 binary on macOS host).
        // If the binary can be executed locally (e.g. shell scripts in test fixtures), verify version.
        if let Ok(output) = Command::new(&node_bin).arg("--version").output() {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                return stdout == format!("v{expected}");
            }
        }
        // Foreign ELF binary cannot be executed on macOS host without virtualization;
        // validate via manifest provenance and non-empty binary file.
        std::fs::metadata(&node_bin)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    }
}

/// Invalidate a single externals dir if its manifest version does not match expected.
/// Deletes `preloop-node.json` so the next startup re-materializes via R2.
/// Returns true if it was stale and deleted, false otherwise (already valid or not present).
pub fn invalidate_if_stale(dir: &Path, expected_version: &str) -> bool {
    let manifest_path = dir.join("preloop-node.json");
    let Some(manifest) = read_manifest(dir) else {
        // No manifest — already considered stale, but we treat missing as not needing deletion
        // because validation will force re-download anyway. Returning false avoids noisy deletes.
        return false;
    };
    let expected = expected_version.trim_start_matches('v');
    let got = manifest.version.trim_start_matches('v');
    if got != expected {
        let _ = std::fs::remove_file(&manifest_path);
        return true;
    }
    false
}

/// Convenience: invalidate both runtimes under a given externals root.
/// `externals_root` is the directory containing `node20` and `node24` subdirs,
/// i.e. `.../externals`. Uses compiled-in pin versions for comparison.
pub fn invalidate_stale_manifests(
    externals_root: &Path,
    node20_version: &str,
    node24_version: &str,
) {
    let _ = invalidate_if_stale(&externals_root.join("node20"), node20_version);
    let _ = invalidate_if_stale(&externals_root.join("node24"), node24_version);
}

/// Parse a SHASUMS256.txt content for the hex digest of a given archive name.
/// File format: `<sha256>  <filename>`, one per line.
pub fn parse_shasums(shasums: &str, archive_name: &str) -> Option<String> {
    for line in shasums.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let hash = parts.next()?;
        let name = parts.next()?;
        // SHASUMS may list `node-v20...tar.gz` directly; compare exact and also basename
        if name == archive_name || name.ends_with(&format!("/{archive_name}")) {
            let lower = hash.to_ascii_lowercase();
            if lower.len() == 64 && lower.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(lower);
            }
        }
    }
    None
}

/// Verify a tarball digest against both the pinned table (authoritative) and SHASUMS (belt-and-braces).
/// Fails closed: returns an error if a mismatch is found or if neither a committed pin nor a
/// SHASUMS entry is available to verify the archive.
pub fn verify_digest(
    digest_hex: &str,
    archive_name: &str,
    pinned_sha: Option<&str>,
    shasums_content: Option<&str>,
) -> Result<(), String> {
    let digest_lc = digest_hex.to_ascii_lowercase();
    let mut verified = false;

    if let Some(pinned) = pinned_sha {
        let pinned_lc = pinned.to_ascii_lowercase();
        if digest_lc != pinned_lc {
            return Err(format!(
                "SHA256 mismatch for {archive_name}: got {digest_lc}, pinned {pinned_lc}"
            ));
        }
        verified = true;
    }
    if let Some(shasums) = shasums_content {
        if let Some(expected) = parse_shasums(shasums, archive_name) {
            if digest_lc != expected {
                return Err(format!(
                    "SHA256 mismatch for {archive_name} vs SHASUMS256.txt: got {digest_lc}, expected {expected}"
                ));
            }
            verified = true;
        }
    }
    if !verified {
        return Err(format!(
            "No trusted checksum found for {archive_name} (neither pinned SHA nor SHASUMS entry available)"
        ));
    }
    Ok(())
}

/// All expected runtimes with their compiled-in pinned versions.
/// Uses the constants generated by build.rs with fallback.
pub fn expected_runtimes() -> Vec<(&'static str, &'static str)> {
    vec![
        ("node20", crate::NODE20_EXTERNALS_VERSION),
        ("node24", crate::NODE24_EXTERNALS_VERSION),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn sha256_hex_known() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn parse_shasums_finds_entry() {
        let a64 = "a".repeat(64);
        let b64 = "b".repeat(64);
        let content = format!(
            "{a64}  node-v20.19.0-linux-arm64.tar.gz\n{b64}  node-v24.3.0-linux-x64.tar.gz\n"
        );
        let got = parse_shasums(&content, "node-v24.3.0-linux-x64.tar.gz").unwrap();
        assert_eq!(got, b64);
    }

    #[test]
    fn parse_shasums_ignores_wrong_name() {
        let a64 = "a".repeat(64);
        let content = format!("{a64}  other.tar.gz\n");
        assert!(parse_shasums(&content, "node-v20.19.0-linux-arm64.tar.gz").is_none());
    }

    #[test]
    fn verify_digest_pinned_authoritative() {
        // Correct digest matches pinned and shasums.
        let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let pinned = Some(digest);
        let shasums = format!("{digest}  node-v20.19.0-linux-arm64.tar.gz");
        assert!(verify_digest(
            digest,
            "node-v20.19.0-linux-arm64.tar.gz",
            pinned,
            Some(&shasums)
        )
        .is_ok());

        // Mismatch vs pinned fails even if shasums would match.
        let wrong = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        assert!(verify_digest(
            wrong,
            "node-v20.19.0-linux-arm64.tar.gz",
            pinned,
            Some(&shasums)
        )
        .is_err());

        // Absent pin and absent shasums fails closed.
        assert!(verify_digest(digest, "node-v20.19.0-linux-arm64.tar.gz", None, None).is_err());
    }

    #[test]
    fn current_platform_nonempty() {
        assert!(!current_platform().is_empty());
        assert!(current_platform().contains('-'));
    }

    #[test]
    fn archive_name_formats() {
        assert_eq!(
            archive_name("20.19.0", "linux-arm64"),
            "node-v20.19.0-linux-arm64.tar.gz"
        );
        assert_eq!(
            archive_name("v20.19.0", "linux-arm64"),
            "node-v20.19.0-linux-arm64.tar.gz"
        );
    }

    #[test]
    fn is_valid_rejects_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node24");
        std::fs::create_dir_all(runtime_dir.join("bin")).unwrap();
        let fake_node = runtime_dir.join("bin/node");
        std::fs::write(&fake_node, "#!/bin/sh\necho v24.3.0\n").unwrap();
        std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755)).unwrap();
        // No manifest -> invalid.
        assert!(!is_valid_externals_dir(&runtime_dir, "node24", "24.3.0"));
    }

    #[test]
    fn is_valid_rejects_version_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node24");
        std::fs::create_dir_all(runtime_dir.join("bin")).unwrap();
        let manifest = NodeManifest::new(
            "node24",
            "24.2.0",
            "linux-arm64",
            "abc",
            "https://example.com",
        );
        write_manifest(&runtime_dir, &manifest).unwrap();
        let fake_node = runtime_dir.join("bin/node");
        std::fs::write(&fake_node, "#!/bin/sh\necho v24.2.0\n").unwrap();
        std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!is_valid_externals_dir(&runtime_dir, "node24", "24.3.0"));
    }

    #[test]
    fn is_valid_accepts_correct_manifest_and_binary() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node24");
        std::fs::create_dir_all(runtime_dir.join("bin")).unwrap();
        let manifest = NodeManifest::new(
            "node24",
            "24.3.0",
            "linux-arm64",
            "abc",
            "https://example.com",
        );
        write_manifest(&runtime_dir, &manifest).unwrap();
        let fake_node = runtime_dir.join("bin/node");
        std::fs::write(&fake_node, "#!/bin/sh\necho v24.3.0\n").unwrap();
        std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_valid_externals_dir(&runtime_dir, "node24", "24.3.0"));
    }

    #[test]
    fn invalidate_if_stale_deletes_mismatched() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node20");
        std::fs::create_dir_all(&runtime_dir).unwrap();
        let manifest = NodeManifest::new(
            "node20",
            "20.18.0",
            "linux-arm64",
            "abc",
            "https://example.com",
        );
        write_manifest(&runtime_dir, &manifest).unwrap();
        assert!(runtime_dir.join("preloop-node.json").exists());
        let deleted = invalidate_if_stale(&runtime_dir, "20.19.0");
        assert!(deleted);
        assert!(!runtime_dir.join("preloop-node.json").exists());
    }

    #[test]
    fn invalidate_if_stale_keeps_matching() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node20");
        std::fs::create_dir_all(&runtime_dir).unwrap();
        let manifest = NodeManifest::new(
            "node20",
            "20.19.0",
            "linux-arm64",
            "abc",
            "https://example.com",
        );
        write_manifest(&runtime_dir, &manifest).unwrap();
        let deleted = invalidate_if_stale(&runtime_dir, "20.19.0");
        assert!(!deleted);
        assert!(runtime_dir.join("preloop-node.json").exists());
    }
}
