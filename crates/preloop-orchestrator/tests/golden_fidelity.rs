//! The golden image must not drift from GitHub's `ubuntu-latest` baseline.
//!
//! Workflows are written against `ubuntu-latest`. Every tool it ships that the
//! golden lacks is a latent "works on GitHub, fails locally" bug — the exact
//! class Preloop exists to eliminate. These tests pin the baseline so a package
//! cannot quietly fall out of the list.
//!
//! Scope is deliberate: the *apt package* baseline only, not `ubuntu-latest`'s
//! preinstalled toolchains. Android SDK, five JDKs, .NET, browsers, and cloud
//! CLIs come to roughly 90 GB and are the job of `actions/setup-*` and
//! `container:` — which is also what keeps workflows portable.

use preloop_orchestrator::{base_packages, docker_data_root, loopback_hosts};

/// Commands a workflow may reasonably assume exist, because `ubuntu-latest`
/// ships them. Grouped by the failure each omission causes.
const REQUIRED: &[(&str, &str)] = &[
    // Bootstrap: nothing else works without these.
    ("git", "checkout"),
    ("curl", "action downloads and countless run steps"),
    ("wget", "run steps"),
    ("ca-certificates", "TLS verification"),
    (
        "sudo",
        "`sudo apt-get install` appears in a large share of workflows",
    ),
    ("gnupg2", "apt key management and signing"),
    ("openssh-client", "git over SSH and deploy keys"),
    // Archive handling: most setup-* actions extract archives.
    ("unzip", "actions/setup-* extract zip archives"),
    ("zip", "artifact packaging"),
    ("tar", "archive extraction"),
    ("xz-utils", "archive extraction"),
    ("zstd", "actions/cache compresses with zstd by default"),
    ("bzip2", "archive extraction"),
    ("p7zip-full", "archive extraction"),
    // Toolchain: native builds and build scripts.
    ("build-essential", "any native build; libc, gcc, g++"),
    ("make", "build scripts"),
    ("pkg-config", "native dependency discovery"),
    ("libssl-dev", "crates and gems that link OpenSSL"),
    ("autoconf", "autotools builds"),
    ("automake", "autotools builds"),
    ("libtool", "autotools builds"),
    // Runtimes assumed present by actions and scripts.
    ("nodejs", "the JavaScript action runtime"),
    ("npm", "Ubuntu packages npm separately from nodejs"),
    ("python3", "scripts and composite actions"),
    ("python-is-python3", "scripts invoking bare `python`"),
    // Shell-step staples.
    ("jq", "JSON handling in run steps"),
    ("file", "type detection"),
    ("rsync", "deploy and copy actions"),
    ("sqlite3", "test fixtures"),
    // Network diagnostics workflows use when something breaks.
    ("dnsutils", "dig/nslookup"),
    ("iputils-ping", "connectivity checks"),
    ("net-tools", "netstat/ifconfig"),
];

#[test]
fn golden_carries_every_baseline_package() {
    let packages: Vec<&str> = base_packages().split_whitespace().collect();
    let missing: Vec<&(&str, &str)> = REQUIRED
        .iter()
        .filter(|(package, _)| !packages.contains(package))
        .collect();

    assert!(
        missing.is_empty(),
        "golden image is missing packages that `ubuntu-latest` ships:\n{}",
        missing
            .iter()
            .map(|(package, why)| format!("  {package:<20} needed for: {why}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn baseline_has_no_duplicate_entries() {
    // A duplicate is harmless to apt but signals a botched merge, and the list
    // is edited by hand often enough for that to happen.
    let packages: Vec<&str> = base_packages().split_whitespace().collect();
    let mut seen = std::collections::BTreeSet::new();
    let duplicates: Vec<&&str> = packages.iter().filter(|p| !seen.insert(**p)).collect();
    assert!(
        duplicates.is_empty(),
        "duplicate packages in the baseline: {duplicates:?}"
    );
}

#[test]
fn container_storage_avoids_the_overlay_root() {
    // Two constraints, both verified on live VMs, and only `/storage` meets both.
    //
    // 1. Must not be the overlay root. containerd mounts each container rootfs
    //    as an overlay whose lowerdir is an image layer; when those layers are
    //    themselves on overlayfs the mount fails `invalid argument` and every
    //    `docker create` exits 1. This hides in single-VM testing, where pulls
    //    land in that VM's own upper layer and run fine -- it only breaks in a
    //    fork, where the layers arrive through a *lower* overlay.
    //
    // 2. Must be inherited by forks, so images preloaded into the golden are
    //    free for every runner. `smolvm machine fork` copy-on-writes the ext4
    //    disk as well as the overlay root (verified: a 64 MiB file written to
    //    the golden's `/dev/vda` mount is present in the clone).
    let root = docker_data_root();
    assert!(
        root.starts_with("/storage/") || root.starts_with("/workspace/"),
        "container data root {root} must be the forked ext4 volume, not the overlay root"
    );
    assert_ne!(
        root, "/var/lib/docker",
        "the default path lands on the overlay root, where container rootfs mounts fail"
    );
}

#[test]
fn baseline_excludes_toolchains_that_belong_to_setup_actions() {
    // Adding these would bloat the image and, worse, let a workflow pass
    // locally while depending on something it never declared — which breaks on
    // real GitHub. Toolchains belong to `actions/setup-*` or `container:`.
    let packages: Vec<&str> = base_packages().split_whitespace().collect();
    for forbidden in [
        "openjdk-17-jdk",
        "golang-go",
        "rustc",
        "cargo",
        "dotnet-sdk-8.0",
        "php",
        "ruby-full",
        "google-chrome-stable",
        "firefox",
        "postgresql",
        "mysql-server",
    ] {
        assert!(
            !packages.contains(&forbidden),
            "`{forbidden}` must not be baked into the golden — a workflow that \
             relies on it without declaring `setup-*`/`container:`/`services:` \
             passes locally and fails on GitHub"
        );
    }
}

#[test]
fn loopback_resolves_by_name() {
    // The base image ships an empty /etc/hosts and `nsswitch.conf` is
    // `hosts: files dns`, so `localhost` falls through to the upstream resolver
    // and fails. Everything still works over 127.0.0.1, which is exactly why
    // this hid until a `services:` job tried to reach localhost:5432.
    let hosts = loopback_hosts();
    assert!(
        hosts.contains("127.0.0.1 localhost"),
        "IPv4 loopback must resolve by name; `services:` are reached at localhost:<port>"
    );
    assert!(
        hosts.contains("::1 localhost"),
        "IPv6 loopback must resolve by name"
    );
    for entry in ["ip6-localhost", "ip6-loopback", "ip6-allnodes"] {
        assert!(
            hosts.contains(entry),
            "missing standard entry `{entry}` present on a stock Ubuntu host"
        );
    }
}
