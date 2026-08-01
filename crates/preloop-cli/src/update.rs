//! Release polling and atomic executable replacement.

use anyhow::{bail, Context};
use flate2::read::GzDecoder;
use reqwest::Client;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tar::Archive;
use tokio::io::AsyncWriteExt;

const DEFAULT_REPOSITORY: &str = "preloopdev/preloop";
const USER_AGENT: &str = concat!("preloop/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, clap::Args)]
pub(crate) struct UpdateArgs {
    /// Only check for a newer release; do not install it.
    #[arg(long)]
    pub check: bool,

    /// Release tag or semantic version to install instead of the latest release.
    #[arg(long)]
    pub version: Option<String>,

    /// GitHub repository in OWNER/NAME form.
    #[arg(long, env = "PRELOOP_RELEASE_REPOSITORY")]
    pub repository: Option<String>,

    /// Override the GitHub releases API endpoint.
    #[arg(long, env = "PRELOOP_RELEASES_API", hide = true)]
    pub api_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Release {
    tag_name: String,
    prerelease: bool,
    draft: bool,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    digest: Option<String>,
}

#[derive(Debug)]
struct SelectedAsset<'a> {
    archive: &'a ReleaseAsset,
    checksum: Option<&'a ReleaseAsset>,
}

pub(crate) async fn run(args: UpdateArgs) -> anyhow::Result<()> {
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("build release API client")?;
    let repository = args
        .repository
        .as_deref()
        .unwrap_or(DEFAULT_REPOSITORY)
        .trim_matches('/');
    let api_url = args
        .api_url
        .unwrap_or_else(|| format!("https://api.github.com/repos/{repository}/releases"));
    let release = fetch_release(&client, &api_url, args.version.as_deref()).await?;
    if release.draft || release.prerelease {
        bail!("release {} is not a stable release", release.tag_name);
    }

    let remote_version = parse_release_version(&release.tag_name)?;
    let current_version = Version::parse(env!("CARGO_PKG_VERSION"))?;
    if remote_version <= current_version {
        println!("preloop {} is already up to date", current_version);
        return Ok(());
    }
    let target = target_triple();
    let selected = select_asset(&release.assets, &remote_version, target)
        .with_context(|| format!("release {} has no asset for {target}", release.tag_name))?;
    println!(
        "preloop {} -> {} ({target})",
        current_version, remote_version
    );
    if args.check {
        return Ok(());
    }

    let lock_path = update_lock_path()?;
    let _lock = UpdateLock::acquire(&lock_path)?;
    let temp_dir = tempfile::tempdir().context("create update staging directory")?;
    let archive_path = temp_dir.path().join(&selected.archive.name);
    download(
        &client,
        &selected.archive.browser_download_url,
        &archive_path,
    )
    .await?;
    verify_checksum(&client, selected.archive, selected.checksum, &archive_path).await?;

    let staged_binary = temp_dir.path().join(binary_name());
    extract_binary(&archive_path, &staged_binary)?;
    let executable = std::env::current_exe().context("locate running preloop executable")?;
    self_replace::self_replace(&staged_binary)
        .with_context(|| format!("atomically replace {}", executable.display()))?;

    println!("installed preloop {}", remote_version);
    restart_systemd_service().await?;
    Ok(())
}

async fn fetch_release(
    client: &Client,
    api_url: &str,
    requested_version: Option<&str>,
) -> anyhow::Result<Release> {
    let url = if let Some(version) = requested_version {
        let tag = version.strip_prefix('v').unwrap_or(version);
        format!("{}/tags/v{}", api_url.trim_end_matches('/'), tag)
    } else {
        format!("{}/latest", api_url.trim_end_matches('/'))
    };
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("poll GitHub releases API: {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("GitHub releases API returned {status} for {url}");
    }
    response
        .json()
        .await
        .with_context(|| format!("decode release metadata from {url}"))
}

async fn download(client: &Client, url: &str, destination: &Path) -> anyhow::Result<()> {
    let mut response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("download release asset: {url}"))?
        .error_for_status()
        .with_context(|| format!("download release asset: {url}"))?;
    let mut file = tokio::fs::File::create(destination)
        .await
        .with_context(|| format!("create {}", destination.display()))?;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
    }
    file.sync_all().await?;
    Ok(())
}

async fn verify_checksum(
    client: &Client,
    archive: &ReleaseAsset,
    checksum_asset: Option<&ReleaseAsset>,
    archive_path: &Path,
) -> anyhow::Result<()> {
    let expected = archive
        .digest
        .as_deref()
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .filter(|digest| digest.len() == 64 && digest.chars().all(|c| c.is_ascii_hexdigit()))
        .map(str::to_owned);
    let expected = if expected.is_some() {
        expected
    } else if let Some(asset) = checksum_asset {
        let response = client
            .get(&asset.browser_download_url)
            .send()
            .await?
            .error_for_status()?;
        let contents = response.text().await?;
        parse_checksum(&contents, &archive.name)
    } else {
        None
    }
    .ok_or_else(|| anyhow::anyhow!("release asset {} has no SHA-256 checksum", archive.name))?;

    let mut file = std::fs::File::open(archive_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = format!("{:x}", hasher.finalize());
    if !actual.eq_ignore_ascii_case(&expected) {
        bail!(
            "checksum mismatch for {}: expected {expected}, got {actual}",
            archive.name
        );
    }
    Ok(())
}

fn extract_binary(archive_path: &Path, destination: &Path) -> anyhow::Result<()> {
    let mut output = std::fs::File::create(destination)?;
    let name = archive_path.to_string_lossy();
    let copied = if name.ends_with(".tar.gz") {
        let file = std::fs::File::open(archive_path)?;
        let mut archive = Archive::new(GzDecoder::new(file));
        copy_tar_binary(&mut archive, &mut output)?
    } else if name.ends_with(".zip") {
        let file = std::fs::File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(file)?;
        let mut copied = false;
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index)?;
            if entry.is_dir() || !is_binary_entry(Path::new(entry.name())) {
                continue;
            }
            std::io::copy(&mut entry, &mut output)?;
            copied = true;
            break;
        }
        copied
    } else {
        bail!("unsupported release archive: {}", archive_path.display());
    };
    if !copied {
        bail!("release archive contains no {}", binary_name());
    }
    output.flush()?;
    output.sync_all()?;
    set_executable_permissions(destination)?;
    Ok(())
}

fn copy_tar_binary<R: Read>(
    archive: &mut Archive<R>,
    output: &mut std::fs::File,
) -> anyhow::Result<bool> {
    for entry in archive.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() || !is_binary_entry(entry.path()?.as_ref()) {
            continue;
        }
        std::io::copy(&mut entry, output)?;
        return Ok(true);
    }
    Ok(false)
}

fn is_binary_entry(path: &Path) -> bool {
    !path.components().any(|component| {
        matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::RootDir
        )
    }) && path
        .file_name()
        .is_some_and(|name| name == std::ffi::OsStr::new(binary_name()))
}

fn select_asset<'a>(
    assets: &'a [ReleaseAsset],
    version: &Version,
    target: &str,
) -> Option<SelectedAsset<'a>> {
    let version_fragment = format!("v{version}");
    let archive = assets
        .iter()
        .filter(|asset| {
            asset.name.contains(target)
                && (!asset.name.contains("-v") || asset.name.contains(&version_fragment))
                && (asset.name.ends_with(".tar.gz") || asset.name.ends_with(".zip"))
        })
        .min_by_key(|asset| (!asset.name.starts_with("preloop-v"), asset.name.len()))?;
    let checksum = assets.iter().find(|asset| {
        asset.name == format!("{}.sha256", archive.name)
            || asset.name == format!("{}.sha256sum", archive.name)
            || asset.name == "checksums.txt"
            || asset.name == "sha256sums.txt"
            || asset.name == "sha256.sum"
    });
    Some(SelectedAsset { archive, checksum })
}

fn parse_checksum(contents: &str, archive_name: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let digest = fields.next()?;
        let name = fields.next()?.trim_start_matches('*');
        (name == archive_name
            && digest.len() == 64
            && digest
                .chars()
                .all(|character| character.is_ascii_hexdigit()))
        .then(|| digest.to_owned())
    })
}

fn parse_release_version(tag: &str) -> anyhow::Result<Version> {
    Version::parse(tag.trim_start_matches('v'))
        .with_context(|| format!("release tag is not semantic version: {tag}"))
}

fn target_triple() -> &'static str {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        "aarch64-apple-darwin"
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        "x86_64-apple-darwin"
    } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
        "aarch64-pc-windows-msvc"
    } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
        "x86_64-pc-windows-msvc"
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        "aarch64-unknown-linux-gnu"
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        "x86_64-unknown-linux-gnu"
    } else {
        "unsupported"
    }
}

fn binary_name() -> &'static str {
    if cfg!(windows) {
        "preloop.exe"
    } else {
        "preloop"
    }
}

fn update_lock_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("PRELOOP_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".preloop")))
        .unwrap_or_else(|| PathBuf::from(".preloop"));
    std::fs::create_dir_all(&home)?;
    Ok(home.join("update.lock"))
}

struct UpdateLock {
    path: PathBuf,
}

impl UpdateLock {
    fn acquire(path: &Path) -> anyhow::Result<Self> {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(_) => Ok(Self {
                path: path.to_owned(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                bail!("another preloop update is already running")
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for UpdateLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn set_executable_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

async fn restart_systemd_service() -> anyhow::Result<()> {
    if !cfg!(target_os = "linux") || std::env::var_os("PRELOOP_NO_SYSTEMD_RESTART").is_some() {
        return Ok(());
    }
    let active = tokio::process::Command::new("systemctl")
        .args(["is-active", "--quiet", "preloop.service"])
        .status()
        .await
        .context("check preloop systemd service")?;
    if !active.success() {
        return Ok(());
    }
    let result = tokio::process::Command::new("systemctl")
        .args(["try-restart", "--no-block", "preloop.service"])
        .status()
        .await
        .context("restart preloop systemd service")?;
    if !result.success() {
        bail!("systemctl try-restart preloop.service failed with {result}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sha256sum_format() {
        assert_eq!(
            parse_checksum(
                "deadbeef00000000000000000000000000000000000000000000000000000000  preloop.tar.gz\n",
                "preloop.tar.gz"
            ),
            Some("deadbeef00000000000000000000000000000000000000000000000000000000".into())
        );
    }

    #[test]
    fn selects_target_archive_and_sidecar() {
        let assets = vec![
            ReleaseAsset {
                name: "preloop-cli-aarch64-apple-darwin.tar.gz".into(),
                browser_download_url: String::new(),
                digest: None,
            },
            ReleaseAsset {
                name: "preloop-cli-aarch64-apple-darwin.tar.gz.sha256".into(),
                browser_download_url: String::new(),
                digest: None,
            },
        ];
        let selected = select_asset(&assets, &Version::new(0, 22, 0), "aarch64-apple-darwin")
            .expect("matching target");
        assert_eq!(selected.archive.name, assets[0].name);
        assert_eq!(selected.checksum.expect("sidecar").name, assets[1].name);
    }

    #[test]
    fn rejects_path_traversal_as_binary_entry() {
        assert!(!is_binary_entry(Path::new("../preloop")));
        assert!(is_binary_entry(Path::new(
            "preloop-v0.22.0-aarch64-apple-darwin/preloop"
        )));
    }

    #[test]
    fn extracts_binary_from_cargo_dist_tarball() {
        let temp = tempfile::tempdir().expect("staging directory");
        let archive_path = temp.path().join("preloop-cli-aarch64-apple-darwin.tar.gz");
        let output_path = temp.path().join(binary_name());
        let file = std::fs::File::create(&archive_path).expect("archive");
        let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        let contents = b"test executable";
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        builder
            .append_data(
                &mut header,
                "preloop-cli-aarch64-apple-darwin/preloop",
                &contents[..],
            )
            .expect("binary entry");
        builder
            .into_inner()
            .expect("gzip stream")
            .finish()
            .expect("archive");

        extract_binary(&archive_path, &output_path).expect("extract binary");
        assert_eq!(std::fs::read(output_path).expect("binary"), contents);
    }
}
