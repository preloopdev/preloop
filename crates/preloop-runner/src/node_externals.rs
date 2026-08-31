//! Node externals manifest and validation (runner side).
//! Mirrors `preloop-orchestrator/src/node_externals.rs` — keep the two in sync.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::process::Command;

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

pub fn source_url(version: &str, archive_name: &str) -> String {
    let ver = if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    };
    format!("https://nodejs.org/dist/{ver}/{archive_name}")
}

pub fn shasums_url(version: &str) -> String {
    let ver = if version.starts_with('v') {
        version.to_owned()
    } else {
        format!("v{version}")
    };
    format!("https://nodejs.org/dist/{ver}/SHASUMS256.txt")
}

pub fn pinned_key(runtime: &str, version: &str, platform: &str) -> String {
    let ver = version.trim_start_matches('v');
    format!("{runtime}_{ver}_{platform}")
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn read_manifest(dir: &Path) -> Option<NodeManifest> {
    let path = dir.join("preloop-node.json");
    let data = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&data).ok()
}

pub fn write_manifest(dir: &Path, manifest: &NodeManifest) -> std::io::Result<()> {
    let path = dir.join("preloop-node.json");
    let tmp = dir.join(".preloop-node.json.tmp");
    let data = serde_json::to_string_pretty(manifest).expect("manifest serializes");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

pub fn is_valid_externals_dir(dir: &Path, runtime: &str, expected_version: &str) -> bool {
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
        let Ok(output) = Command::new(&node_bin).arg("--version").output() else {
            return false;
        };
        if !output.status.success() {
            return false;
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        stdout == format!("v{expected}")
    } else {
        if let Ok(output) = Command::new(&node_bin).arg("--version").output() {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                return stdout == format!("v{expected}");
            }
        }
        std::fs::metadata(&node_bin)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    }
}

pub fn invalidate_if_stale(dir: &Path, expected_version: &str) -> bool {
    let manifest_path = dir.join("preloop-node.json");
    let Some(manifest) = read_manifest(dir) else {
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

pub fn parse_shasums(shasums: &str, archive_name: &str) -> Option<String> {
    for line in shasums.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let hash = parts.next()?;
        let name = parts.next()?;
        if name == archive_name || name.ends_with(&format!("/{archive_name}")) {
            let lower = hash.to_ascii_lowercase();
            if lower.len() == 64 && lower.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(lower);
            }
        }
    }
    None
}

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
    fn sha256_known() {
        assert_eq!(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn verify_digest_fails_closed() {
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
        assert!(verify_digest(digest, "node-v20.19.0-linux-arm64.tar.gz", None, None).is_err());
    }

    #[test]
    fn valid_accepts_correct() {
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
        let fake = runtime_dir.join("bin/node");
        std::fs::write(&fake, "#!/bin/sh\necho v24.3.0\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_valid_externals_dir(&runtime_dir, "node24", "24.3.0"));
    }

    #[test]
    fn valid_rejects_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("node24");
        std::fs::create_dir_all(runtime_dir.join("bin")).unwrap();
        let fake = runtime_dir.join("bin/node");
        std::fs::write(&fake, "#!/bin/sh\necho v24.3.0\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(!is_valid_externals_dir(&runtime_dir, "node24", "24.3.0"));
    }
}
