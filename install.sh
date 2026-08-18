#!/bin/sh
# Preloop installer — instant install from release binaries.
#
#   curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh
#
# Downloads the prebuilt binaries for your platform (preloop, preloop-server,
# preloop-runner) from the latest GitHub release, verifies the sha256, and
# installs into ~/.local/bin. When no release exists for the platform yet it
# falls back to building from source (git + cargo + zig).
#
# Options: --version <tag>  install a specific release (default: latest)
#          --prefix <dir>   install under <dir>/bin (default: ~/.local)

set -e

PREFIX="${PREFIX:-$HOME/.local}"
VERSION="${VERSION:-latest}"
REPO="preloopdev/preloop"
BIN_DIR="$PREFIX/bin"

say() { printf '\033[1;32m[preloop]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[preloop] error:\033[0m %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
        --prefix) PREFIX="${2:?--prefix needs a value}"; shift 2 ;;
        -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -12; exit 0 ;;
        *) die "unknown option: $1" ;;
    esac
done
BIN_DIR="$PREFIX/bin"

# --- platform ---------------------------------------------------------------

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
    linux) os="linux" ;;
    darwin) os="darwin" ;;
    *) die "unsupported operating system: $os (want linux or darwin)" ;;
esac

arch="$(uname -m | tr '[:upper:]' '[:lower:]')"
case "$arch" in
    x86_64|amd64) arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) die "unsupported architecture: $arch (want x86_64 or aarch64)" ;;
esac

full_triple() {
    case "$os/$arch" in
        linux/x86_64) echo "x86_64-unknown-linux-gnu" ;;
        linux/aarch64) echo "aarch64-unknown-linux-gnu" ;;
        darwin/x86_64) echo "x86_64-apple-darwin" ;;
        darwin/aarch64) echo "aarch64-apple-darwin" ;;
    esac
}

# --- release download -------------------------------------------------------

release_json() { # tag or latest
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "https://api.github.com/repos/$REPO/releases/$1" 2>/dev/null && return 0
    fi
    if command -v gh >/dev/null 2>&1; then
        gh api "repos/$REPO/releases/$1" 2>/dev/null && return 0
    fi
    return 1
}

# --- smolvm runtime --------------------------------------------------------

ensure_runtime() {
    say "installing smolvm runtime..."
    PATH="$BIN_DIR:$HOME/.local/bin:$PATH" \
        "$BIN_DIR/preloop" update --ensure-runtime \
        || die "could not install the smolvm runtime"

    local smolvm_bin="$HOME/.local/bin/smolvm"
    [ -x "$smolvm_bin" ] || die "smolvm was not installed at $smolvm_bin"
    local smolvm_version
    smolvm_version="$("$smolvm_bin" --version 2>/dev/null | awk '{print $NF}')"
    [ -n "$smolvm_version" ] || die "installed smolvm did not report a version"

    # Keep custom-prefix installs self-contained and ahead of any incompatible
    # system smolvm already on PATH.
    if [ "$BIN_DIR" != "$HOME/.local/bin" ]; then
        ln -sfn "$smolvm_bin" "$BIN_DIR/smolvm"
    fi
    say "installed smolvm $smolvm_version"
}

install_from_release() {
    local tag="$VERSION"
    local json
    if [ "$tag" = "latest" ]; then
        json="$(release_json latest)" || return 1
    else
        json="$(release_json "tags/$tag")" || return 1
    fi
    tag="$(printf '%s' "$json" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
    [ -n "$tag" ] || return 1

    # cargo-dist names the per-platform archive after the package
    # (`preloop-cli-<triple>.tar.gz`); the tag is not part of the filename.
    local short="preloop-cli-${os}-${arch}.tar.gz"
    local full="preloop-cli-$(full_triple).tar.gz"
    local url asset
    asset="$(printf '%s' "$json" | sed -n "s/.*\"browser_download_url\":[[:space:]]*\"\([^\"]*\/$short\)\".*/\1/p" | head -1)"
    if [ -z "$asset" ]; then
        asset="$(printf '%s' "$json" | sed -n "s/.*\"browser_download_url\":[[:space:]]*\"\([^\"]*\/$full\)\".*/\1/p" | head -1)"
    fi
    [ -n "$asset" ] || return 1

    say "downloading $tag ($os/$arch)..."
    local tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT

    local archive="$tmp/$(basename "$asset")"
    local asset_name="$(basename "$asset")"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$asset" -o "$archive" 2>/dev/null || archive=""
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$asset" -O "$archive" 2>/dev/null || archive=""
    fi
    if [ -z "${archive:-}" ] || [ ! -s "$archive" ]; then
        # Private repositories 404 unauthenticated downloads; the authenticated
        # gh client covers them, and works just the same once public.
        if command -v gh >/dev/null 2>&1; then
            archive="$tmp/$asset_name"
            rm -f "$archive"
            (cd "$tmp" && gh release download "$tag" --repo "$REPO" --pattern "$asset_name" --clobber >/dev/null 2>&1) \
                || archive=""
        fi
    fi
    [ -n "${archive:-}" ] && [ -s "$archive" ] || return 1

    # cargo-dist checksum files bake the build machine's absolute path, so
    # compare hashes by value instead of `sha256sum -c`.
    local sha_url="${asset}.sha256" expected actual
    local sha_name="$(basename "$sha_url")"
    expected="$(curl -fsSL "$sha_url" 2>/dev/null | awk '{print $1}')"
    if [ -z "$expected" ] && command -v gh >/dev/null 2>&1; then
        (cd "$tmp" && gh release download "$tag" --repo "$REPO" --pattern "$sha_name" --clobber >/dev/null 2>&1) \
            && expected="$(awk '{print $1}' "$tmp/$sha_name" 2>/dev/null)"
    fi
    if [ -n "$expected" ]; then
        if command -v sha256sum >/dev/null 2>&1; then
            actual="$(sha256sum "$archive" | awk '{print $1}')"
        else
            actual="$(shasum -a 256 "$archive" | awk '{print $1}')"
        fi
        [ "$actual" = "$expected" ] || die "checksum mismatch — refusing to install"
        say "sha256 verified"
    fi

    mkdir -p "$tmp/extract"
    tar -xzf "$archive" -C "$tmp/extract" --strip-components=1
    mkdir -p "$BIN_DIR"
    for bin in preloop preloop-server preloop-runner; do
        if [ -f "$tmp/extract/$bin" ]; then
            install -m 0755 "$tmp/extract/$bin" "$BIN_DIR/$bin"
            say "installed $BIN_DIR/$bin"
        fi
    done
    [ -x "$BIN_DIR/preloop" ] || return 1

    # macOS installs execute workflows inside Linux microVMs, so the engine
    # needs the Linux guest runner at the path it discovers
    # (<prefix>/lib/preloop/runner/<linux-triple>/preloop-runner). Without it
    # every job queues forever with "Linux runner bundle unavailable". A
    # release missing the asset is a warning, not a failure — the engine's
    # startup message explains the consequence.
    if [ "$os" = "darwin" ]; then
        local runner_triple
        case "$arch" in
            x86_64) runner_triple="x86_64-unknown-linux-gnu" ;;
            aarch64) runner_triple="aarch64-unknown-linux-gnu" ;;
        esac
        local runner_asset="preloop-runner-${runner_triple}"
        local runner_url
        runner_url="$(printf '%s' "$json" | sed -n "s/.*\"browser_download_url\":[[:space:]]*\"\([^\"]*\/$runner_asset\)\".*/\1/p" | head -1)"
        if [ -n "$runner_url" ]; then
            local runner_dest="$PREFIX/lib/preloop/runner/$runner_triple"
            mkdir -p "$runner_dest"
            curl -fsSL "$runner_url" -o "$runner_dest/preloop-runner" 2>/dev/null \
                && chmod 0755 "$runner_dest/preloop-runner" \
                && say "installed $runner_dest/preloop-runner" \
                || say "warning: could not install Linux runner bundle — microVM jobs need it"
        else
            say "warning: release $tag has no $runner_asset asset — microVM jobs need it"
        fi
    fi
    ensure_runtime
    return 0
}

if install_from_release; then
    case ":$PATH:" in
        *":$BIN_DIR:"*) ;;
        *) say "add $BIN_DIR to your PATH:  export PATH=\"$BIN_DIR:\$PATH\"" ;;
    esac
    cat <<EOF

[preloop] next steps:
    preloop serve                      # start the engine on 127.0.0.1:9090
    preloop setup github               # GitHub App or fine-grained PAT
    cd your-repo && preloop run -f .github/workflows/ci.yml
    preloop run --push --create-pr     # CI first, then a draft PR

[preloop] full guide: https://github.com/preloopdev/preloop/blob/main/docs/setup.md
EOF
    exit 0
fi

# --- source fallback (no release for this platform yet) ----------------------

say "no prebuilt binary for $os/$arch in release $VERSION — building from source"

command -v git >/dev/null 2>&1 || die "git is required"
command -v cargo >/dev/null 2>&1 || {
    command -v rustup >/dev/null 2>&1 || die "rustup/cargo is required — install from https://rustup.rs"
    die "cargo not found — run: rustup toolchain install stable && rustup default stable"
}
if command -v zig >/dev/null 2>&1; then
    command -v cargo-zigbuild >/dev/null 2>&1 || say "zig found; cargo-zigbuild missing — the Linux microVM runner will be skipped (run: cargo install cargo-zigbuild)"
    ZIGBUILD=1
else
    say "zig not found — building host binaries only; microVM runner needs zig (macOS: brew install zig)"
    ZIGBUILD=0
fi

PRELOOP_SRC="${PRELOOP_SRC:-$HOME/.preloop-src}"
REPO_URL="${PRELOOP_REPO:-https://github.com/preloopdev/preloop.git}"
mkdir -p "$PRELOOP_SRC"
if [ -d "$PRELOOP_SRC/.git" ]; then
    say "refreshing $PRELOOP_SRC"
    git -C "$PRELOOP_SRC" fetch --quiet --depth=1 origin main
    git -C "$PRELOOP_SRC" checkout --quiet FETCH_HEAD
else
    say "cloning $REPO_URL into $PRELOOP_SRC"
    git clone --quiet --depth=1 "$REPO_URL" "$PRELOOP_SRC"
fi
cd "$PRELOOP_SRC"

say "building preloop (release)..."
cargo build --release -p preloop-cli -p preloop-runner-server 2>/dev/null \
    || cargo build --release -p preloop-cli -p aksh-runner-server
if [ "$ZIGBUILD" = 1 ] && command -v cargo-zigbuild >/dev/null 2>&1; then
    say "cross-compiling the Linux microVM runner (aarch64)..."
    cargo zigbuild --release -p preloop-runner --target aarch64-unknown-linux-gnu 2>/dev/null || \
        cargo zigbuild --release -p aksh-runner --target aarch64-unknown-linux-gnu 2>/dev/null || \
        say "runner build failed — host CLI works, but microVM jobs need it (see docs/setup.md)"
else
    say "skipping microVM runner cross-build (no zig/cargo-zigbuild)"
fi

mkdir -p "$BIN_DIR"
ln -sfn "$PRELOOP_SRC/target/release/preloop" "$BIN_DIR/preloop"
say "installed $BIN_DIR/preloop (source build)"
ensure_runtime
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) say "add $BIN_DIR to your PATH:  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
cat <<EOF

[preloop] next steps:
    preloop serve                      # start the engine on 127.0.0.1:9090
    preloop setup github               # GitHub App or fine-grained PAT
    cd your-repo && preloop run -f .github/workflows/ci.yml

[preloop] full guide: https://github.com/preloopdev/preloop/blob/main/docs/setup.md
EOF
