//! Release polling and atomic executable replacement.

use anyhow::{bail, Context};
use flate2::read::GzDecoder;
use reqwest::Client;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use tar::Archive;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

const DEFAULT_REPOSITORY: &str = "preloopdev/preloop";
const USER_AGENT: &str = concat!("preloop/", env!("CARGO_PKG_VERSION"));
/// Temporary Preloop fork carrying the retained-checkpoint fix until SmolVM
/// publishes the next official release.
const SMOLVM_REPOSITORY: &str = "preloopdev/smolvm";

/// SmolVM version `preloop update` installs from the temporary Preloop fork
/// when the resolved binary cannot mount sockets.
///
/// Pinned to the last release whose macOS artifact ships a NET=1 libkrun and
/// used as the temporary fork's compatibility version:
/// 1.7.5's `lib/libkrun.dylib` does not export `krun_add_net_unixstream`,
/// so `--net-backend virtio-net` (the egress-floor backend preloop-vm
/// selects) cannot boot a machine on macOS with it. Revisit when upstream
/// ships a release with the symbol again.
const SMOLVM_VERSION: &str = "1.7.4";

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

    /// Install and verify Preloop's pinned VM runtime without updating Preloop.
    #[arg(long, hide = true)]
    pub ensure_runtime: bool,
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
    if args.ensure_runtime {
        #[cfg(unix)]
        {
            ensure_smolvm(&client).await?;
            return Ok(());
        }
        #[cfg(not(unix))]
        bail!("the smolvm runtime is supported only on Unix hosts");
    }
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

    // macOS installs execute workflows inside Linux VMs, so the engine is
    // unusable without the bundled Linux guest runner. Ensure it is present
    // on every `preloop update`, not just when a newer CLI is downloaded:
    // a source-built install (install.sh) matches the release version and
    // would otherwise never receive the bundle — the engine then warns that
    // provisioning is unavailable and every job queues forever. Idempotent:
    // the destination is simply overwritten with the current release's
    // runner. A release missing the asset is a warning, not a failure —
    // the engine's startup message explains the consequence.
    #[cfg(target_os = "macos")]
    match update_linux_runner_bundle(&client, &release).await {
        Ok(()) => println!("installed Linux runner bundle"),
        Err(error) => println!("warning: Linux runner bundle not updated: {error:#}"),
    }

    // The engine cannot provision VMs without the pinned compatible smolvm.
    // Checking only `--mount-socket` accepted 1.7.5 on macOS even though its
    // libkrun omitted krun_add_net_unixstream, so require both the capability
    // and the exact release whose packaged runtime was verified.
    #[cfg(unix)]
    if !args.check {
        match ensure_smolvm(&client).await {
            Ok(()) => {}
            Err(error) => println!(
                "warning: smolvm not updated: {error:#}\n  \
                 install it with: curl -sSL https://smolmachines.com/install.sh | bash"
            ),
        }
    }

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

/// Download the release's Linux runner bundle and install it at
/// `<prefix>/lib/preloop/runner/<triple>/preloop-runner`, the layout
/// `local_runner_pool_config` discovers.
#[cfg(target_os = "macos")]
async fn update_linux_runner_bundle(client: &Client, release: &Release) -> anyhow::Result<()> {
    let triple = crate::linux_guest_triple();
    let asset_name = format!("preloop-runner-{triple}");
    let asset = release
        .assets
        .iter()
        .find(|asset| asset.name == asset_name)
        .with_context(|| format!("release {} has no {asset_name} asset", release.tag_name))?;
    let checksum = release
        .assets
        .iter()
        .find(|asset| asset.name == format!("{asset_name}.sha256"));
    let staging = tempfile::tempdir().context("create runner bundle staging directory")?;
    let bundle_path = staging.path().join(&asset.name);
    download(client, &asset.browser_download_url, &bundle_path).await?;
    verify_checksum(client, asset, checksum, &bundle_path).await?;

    let executable = std::env::current_exe().context("locate running preloop executable")?;
    let prefix = executable
        .parent()
        .and_then(|dir| dir.parent())
        .context("expected install layout <prefix>/bin/preloop")?;
    let destination_dir = prefix.join("lib/preloop/runner").join(triple);
    std::fs::create_dir_all(&destination_dir)
        .with_context(|| format!("create {}", destination_dir.display()))?;
    let destination = destination_dir.join("preloop-runner");
    tokio::fs::copy(&bundle_path, &destination)
        .await
        .with_context(|| format!("install {}", destination.display()))?;
    set_executable_permissions(&destination)?;
    Ok(())
}

/// Install layout for smolvm, matching the official installer's locations.
/// Tests point these at tempdirs.
#[derive(Debug, Clone)]
struct SmolvmInstall {
    /// `~/.smolvm`: the wrapper, binary, `lib/`, and disk templates.
    prefix: PathBuf,
    /// macOS: `~/Library/Application Support/smolvm`; Linux:
    /// `$XDG_DATA_HOME/smolvm` or `~/.local/share/smolvm`.
    data_dir: PathBuf,
    /// `~/.local/bin`: where the `smolvm` symlink lives.
    bin_dir: PathBuf,
}

fn default_smolvm_install() -> Option<SmolvmInstall> {
    let home = PathBuf::from(std::env::var_os("HOME")?);
    #[cfg(target_os = "macos")]
    let data_dir = home
        .join("Library")
        .join("Application Support")
        .join("smolvm");
    #[cfg(target_os = "linux")]
    let data_dir = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local").join("share"))
        .join("smolvm");
    Some(SmolvmInstall {
        prefix: home.join(".smolvm"),
        data_dir,
        bin_dir: home.join(".local").join("bin"),
    })
}

/// Host triple naming used by the smolvm release assets.
fn smolvm_platform() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Some("darwin-arm64"),
        ("macos", "x86_64") => Some("darwin-x86_64"),
        ("linux", "aarch64") => Some("linux-arm64"),
        ("linux", "x86_64") => Some("linux-x86_64"),
        _ => None,
    }
}

/// Whether a smolvm binary's `machine create` accepts `--mount-socket`.
///
/// The wrapper scripts can report a recent `--version` while resolving to
/// an old binary, so the flag's presence in the help text is the reliable
/// check (the same probe preloop-vm runs before provisioning).
async fn probe_mount_socket(binary: &Path) -> bool {
    let mut command = tokio::process::Command::new(binary);
    command
        .args(["machine", "create", "--help"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let Ok(mut child) = command.spawn() else {
        return false;
    };
    let stdout = child.stdout.take();
    let Ok(status) = child.wait().await else {
        return false;
    };
    if !status.success() {
        return false;
    }
    let Some(mut stdout) = stdout else {
        return false;
    };
    let mut output = String::new();
    stdout.read_to_string(&mut output).await.is_ok() && output.contains("--mount-socket")
}

async fn probe_smolvm_version(binary: &Path) -> Option<String> {
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = command.output().await.ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .split_whitespace()
        .last()
        .map(|version| version.trim_start_matches('v').to_owned())
}

fn smolvm_release_repository() -> String {
    std::env::var("PRELOOP_SMOLVM_RELEASE_REPOSITORY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| SMOLVM_REPOSITORY.to_owned())
}

fn smolvm_release_version() -> String {
    std::env::var("PRELOOP_SMOLVM_RELEASE_VERSION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| SMOLVM_VERSION.to_owned())
}

async fn smolvm_is_compatible(binary: &Path, expected_version: &str) -> bool {
    probe_mount_socket(binary).await
        && probe_smolvm_version(binary).await.as_deref() == Some(expected_version)
}

/// Probe the resolved smolvm and install the exact verified release when its
/// version or required socket-mount capability differs.
async fn ensure_smolvm(client: &Client) -> anyhow::Result<()> {
    let expected_version = smolvm_release_version();
    if smolvm_is_compatible(Path::new("smolvm"), &expected_version).await {
        return Ok(());
    }
    let platform = smolvm_platform().ok_or_else(|| {
        anyhow::anyhow!(
            "no smolvm release asset for {}-{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    let repository = smolvm_release_repository();
    let release = fetch_release(
        client,
        &format!("https://api.github.com/repos/{repository}/releases"),
        Some(&expected_version),
    )
    .await?;
    let version = release
        .tag_name
        .strip_prefix('v')
        .unwrap_or(&release.tag_name);
    let archive_name = format!("smolvm-{version}-{platform}.tar.gz");
    let archive_asset = release
        .assets
        .iter()
        .find(|asset| asset.name == archive_name)
        .with_context(|| format!("release {} has no {archive_name} asset", release.tag_name))?;
    let checksum_asset = release
        .assets
        .iter()
        .find(|asset| asset.name == "checksums.sha256");
    let install = default_smolvm_install().context("HOME is not set")?;
    let staging = tempfile::tempdir().context("create smolvm download directory")?;
    let archive_path = staging.path().join(&archive_name);
    download(client, &archive_asset.browser_download_url, &archive_path).await?;
    verify_checksum(client, archive_asset, checksum_asset, &archive_path).await?;
    install_smolvm_from_archive(&archive_path, version, &install)?;
    println!("installed smolvm {version}");
    // The install lands in `~/.local/bin`; if PATH still resolves another
    // binary, the engine keeps failing and the user is left confused about
    // why the update did not help. Say so explicitly.
    if !smolvm_is_compatible(Path::new("smolvm"), &expected_version).await {
        bail!(
            "installed smolvm {version} to {}, but `smolvm` on PATH still resolves \
             to an incompatible version or lacks --mount-socket; make sure {} comes first",
            install.prefix.display(),
            install.bin_dir.display()
        );
    }
    Ok(())
}

/// Install a downloaded (and checksum-verified) smolvm release archive,
/// replicating the official installer's layout.
fn install_smolvm_from_archive(
    archive_path: &Path,
    version: &str,
    install: &SmolvmInstall,
) -> anyhow::Result<()> {
    let staging = tempfile::tempdir().context("create smolvm staging directory")?;
    let extracted = staging.path().join("extracted");
    extract_tar_gz(archive_path, &extracted)?;

    // The archive carries a single top-level `smolvm-<version>-<platform>/`.
    let top = fs::read_dir(&extracted)?
        .next()
        .context("smolvm archive is empty")?
        .context("read smolvm archive entry")?
        .path();

    fs::create_dir_all(&install.prefix)?;
    // `lib/` is replaced wholesale; everything else in the prefix (machine
    // state, databases) is left alone.
    let lib = top.join("lib");
    if lib.is_dir() {
        let target = install.prefix.join("lib");
        let _ = fs::remove_dir_all(&target);
        copy_dir_all(&lib, &target)?;
    }
    for name in [
        "smolvm",
        "smolvm-bin",
        "storage-template.ext4",
        "overlay-template.ext4",
    ] {
        let source = top.join(name);
        if source.is_file() {
            fs::copy(&source, install.prefix.join(name))?;
        }
    }
    set_executable_permissions(&install.prefix.join("smolvm"))?;
    set_executable_permissions(&install.prefix.join("smolvm-bin"))?;
    fs::write(install.prefix.join(".version"), format!("{version}\n"))?;

    let agent_rootfs = top.join("agent-rootfs");
    if agent_rootfs.is_dir() {
        fs::create_dir_all(&install.data_dir)?;
        let target = install.data_dir.join("agent-rootfs");
        let _ = fs::remove_dir_all(&target);
        copy_dir_all(&agent_rootfs, &target)?;
    }
    #[cfg(target_os = "linux")]
    {
        let init_krun = top.join("init.krun");
        if init_krun.is_file() {
            fs::create_dir_all(&install.data_dir)?;
            fs::copy(&init_krun, install.data_dir.join("init.krun"))?;
            set_executable_permissions(&install.data_dir.join("init.krun"))?;
        }
    }

    fs::create_dir_all(&install.bin_dir)?;
    let link = install.bin_dir.join("smolvm");
    let _ = fs::remove_file(&link);
    #[cfg(unix)]
    std::os::unix::fs::symlink(install.prefix.join("smolvm"), &link)?;
    Ok(())
}

/// Extract a .tar.gz into `destination`, rejecting entries that escape it.
fn extract_tar_gz(archive_path: &Path, destination: &Path) -> anyhow::Result<()> {
    let file = fs::File::open(archive_path)?;
    let mut archive = Archive::new(GzDecoder::new(file));
    fs::create_dir_all(destination)?;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let mut safe = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(part) => safe.push(part),
                Component::CurDir => {}
                _ => bail!("unsafe path in smolvm archive: {}", path.display()),
            }
        }
        if safe.as_os_str().is_empty() {
            continue;
        }
        entry.unpack(destination.join(safe))?;
    }
    Ok(())
}

fn copy_dir_all(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        let meta = fs::symlink_metadata(&from)?;
        if meta.is_dir() {
            copy_dir_all(&from, &to)?;
        } else if meta.file_type().is_symlink() {
            // Preserve links instead of following them: `agent-rootfs`
            // contains busybox-style absolute links whose targets do not
            // resolve on the host, so `fs::copy` would fail with ENOENT.
            // This mirrors the official installer's `cp -a`.
            let link = fs::read_link(&from)?;
            #[cfg(unix)]
            std::os::unix::fs::symlink(&link, &to)?;
        } else {
            fs::copy(&from, &to)?;
        }
    }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn probe_detects_mount_socket_in_help_text() {
        let directory = tempfile::tempdir().unwrap();
        for (flag, expected) in [
            ("--mount-socket <HOST:GUEST>", true),
            ("--docker-socket [-- <COMMAND>...]", false),
        ] {
            let executable = directory.path().join(format!("smolvm-{expected}"));
            std::fs::write(
                &executable,
                format!(
                    "#!/bin/sh\nif [ \"${{1-}}:${{2-}}:${{3-}}\" = \"machine:create:--help\" ]; then\n  printf '%s\\n' \"Usage: smolvm machine create {flag}\"\n  exit 0\nfi\nexit 0\n"
                ),
            )
            .unwrap();
            let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(0o755);
            std::fs::set_permissions(&executable, permissions).unwrap();

            assert_eq!(
                probe_mount_socket(&executable).await,
                expected,
                "help containing `{flag}`"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn compatibility_requires_pinned_version_and_mount_socket() {
        let directory = tempfile::tempdir().unwrap();
        for (version, flag, expected) in [
            (SMOLVM_VERSION, "--mount-socket <HOST:GUEST>", true),
            ("1.7.5", "--mount-socket <HOST:GUEST>", false),
            (SMOLVM_VERSION, "--docker-socket", false),
        ] {
            let executable = directory
                .path()
                .join(format!("smolvm-{version}-{expected}"));
            std::fs::write(
                &executable,
                format!(
                    "#!/bin/sh\n\
                     if [ \"${{1-}}\" = \"--version\" ]; then\n\
                       printf '%s\\n' \"smolvm {version}\"\n\
                       exit 0\n\
                     fi\n\
                     if [ \"${{1-}}:${{2-}}:${{3-}}\" = \"machine:create:--help\" ]; then\n\
                       printf '%s\\n' \"Usage: smolvm machine create {flag}\"\n\
                       exit 0\n\
                     fi\n\
                     exit 1\n"
                ),
            )
            .unwrap();
            let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(0o755);
            std::fs::set_permissions(&executable, permissions).unwrap();

            assert_eq!(
                smolvm_is_compatible(&executable, SMOLVM_VERSION).await,
                expected,
                "version={version}, flag={flag}"
            );
        }
    }

    #[test]
    fn platform_naming_covers_apple_silicon_and_linux() {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => assert_eq!(smolvm_platform(), Some("darwin-arm64")),
            ("macos", "x86_64") => assert_eq!(smolvm_platform(), Some("darwin-x86_64")),
            ("linux", "aarch64") => assert_eq!(smolvm_platform(), Some("linux-arm64")),
            ("linux", "x86_64") => assert_eq!(smolvm_platform(), Some("linux-x86_64")),
            other => assert_eq!(smolvm_platform(), None, "unexpected host {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn install_from_archive_places_the_official_layout() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let directory = tempfile::tempdir().unwrap();
        // Build a fake smolvm release tree.
        let source = directory.path().join("smolvm-9.9.9-darwin-arm64");
        let lib = source.join("lib");
        std::fs::create_dir_all(lib.join("nested")).unwrap();
        std::fs::write(lib.join("nested").join("libfile"), b"lib").unwrap();
        std::fs::write(source.join("smolvm"), "#!/bin/sh\n").unwrap();
        std::fs::write(source.join("smolvm-bin"), "#!/bin/sh\n").unwrap();
        std::fs::write(source.join("storage-template.ext4"), b"template").unwrap();
        let rootfs = source.join("agent-rootfs");
        std::fs::create_dir_all(&rootfs).unwrap();
        std::fs::write(rootfs.join("rootfs.txt"), b"rootfs").unwrap();
        #[cfg(target_os = "linux")]
        std::fs::write(source.join("init.krun"), b"init").unwrap();

        // Tar it up the way the release does.
        let archive_path = directory.path().join("smolvm-9.9.9-darwin-arm64.tar.gz");
        let file = std::fs::File::create(&archive_path).unwrap();
        let encoder = GzEncoder::new(file, Compression::default());
        let mut builder = tar::Builder::new(encoder);
        builder
            .append_dir_all("smolvm-9.9.9-darwin-arm64", &source)
            .unwrap();
        // A busybox-style absolute link whose target does not resolve on
        // the host. `append_dir_all` follows links and would fail on it,
        // so append the link entry by hand; the install must preserve the
        // link, not follow it.
        #[cfg(unix)]
        {
            let mut link_header = tar::Header::new_gnu();
            link_header.set_entry_type(tar::EntryType::Symlink);
            link_header.set_size(0);
            link_header.set_mode(0o777);
            link_header.set_cksum();
            builder
                .append_link(
                    &mut link_header,
                    "smolvm-9.9.9-darwin-arm64/agent-rootfs/sh",
                    "/bin/busybox",
                )
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap();

        let install = SmolvmInstall {
            prefix: directory.path().join("prefix"),
            data_dir: directory.path().join("data"),
            bin_dir: directory.path().join("bin"),
        };
        if let Err(error) = install_smolvm_from_archive(&archive_path, "9.9.9", &install) {
            eprintln!("install error: {error:?}");
            panic!("install failed: {error}");
        }

        assert_eq!(
            std::fs::read_to_string(install.prefix.join(".version")).unwrap(),
            "9.9.9\n"
        );
        assert_eq!(
            std::fs::read(install.prefix.join("lib/nested/libfile")).unwrap(),
            b"lib"
        );
        assert!(install.prefix.join("smolvm").is_file());
        assert!(install.prefix.join("smolvm-bin").is_file());
        assert_eq!(
            std::fs::read(install.prefix.join("storage-template.ext4")).unwrap(),
            b"template"
        );
        assert_eq!(
            std::fs::read(install.data_dir.join("agent-rootfs/rootfs.txt")).unwrap(),
            b"rootfs"
        );
        #[cfg(unix)]
        {
            let link_meta =
                std::fs::symlink_metadata(install.data_dir.join("agent-rootfs/sh")).unwrap();
            assert!(
                link_meta.file_type().is_symlink(),
                "symlink must be preserved"
            );
            assert_eq!(
                std::fs::read_link(install.data_dir.join("agent-rootfs/sh")).unwrap(),
                std::path::Path::new("/bin/busybox")
            );
        }
        #[cfg(target_os = "linux")]
        assert_eq!(
            std::fs::read(install.data_dir.join("init.krun")).unwrap(),
            b"init"
        );
        use std::os::unix::fs::PermissionsExt;
        assert_ne!(
            std::fs::metadata(install.prefix.join("smolvm"))
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0,
            "smolvm wrapper must stay executable"
        );
        let link = std::fs::symlink_metadata(install.bin_dir.join("smolvm")).unwrap();
        assert!(link.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(install.bin_dir.join("smolvm")).unwrap(),
            install.prefix.join("smolvm")
        );
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
