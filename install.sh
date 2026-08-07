#!/bin/sh
# Preloop installer — builds from source and installs the CLI.
#
#   curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh
#
# What it does:
#   1. Checks prerequisites (git, cargo/rustup; zig for the microVM runner).
#   2. Clones preloopdev/preloop into $PRELOOP_SRC (default ~/.preloop-src).
#   3. Builds preloop-cli + preloop-runner-server (release) on the host, and
#      cross-compiles the Linux microVM runner when zig is available.
#   4. Symlinks the binary into $PREFIX/bin (default ~/.local/bin).
#   5. Prints next steps.
#
# No release binaries exist yet — `preloop update` will install them once
# releases are published. Until then this builds from source.

set -e

PREFIX="${PREFIX:-$HOME/.local}"
PRELOOP_SRC="${PRELOOP_SRC:-$HOME/.preloop-src}"
REPO="${PRELOOP_REPO:-https://github.com/preloopdev/preloop.git}"
BIN_DIR="$PREFIX/bin"
BINARY="$BIN_DIR/preloop"

say() { printf '\033[1;32m[preloop]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[preloop] error:\033[0m %s\n' "$*" >&2; exit 1; }

# --- 1. prerequisites ------------------------------------------------------

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

# --- 2. clone / refresh -----------------------------------------------------

mkdir -p "$PRELOOP_SRC"
if [ -d "$PRELOOP_SRC/.git" ]; then
    say "refreshing $PRELOOP_SRC"
    git -C "$PRELOOP_SRC" fetch --quiet --depth=1 origin main
    git -C "$PRELOOP_SRC" checkout --quiet FETCH_HEAD
else
    say "cloning $REPO into $PRELOOP_SRC"
    git clone --quiet --depth=1 "$REPO" "$PRELOOP_SRC"
fi
cd "$PRELOOP_SRC"

# --- 3. build ---------------------------------------------------------------

say "building preloop (release)..."
cargo build --release -p preloop-cli -p preloop-runner-server

if [ "$ZIGBUILD" = 1 ] && command -v cargo-zigbuild >/dev/null 2>&1; then
    say "cross-compiling the Linux microVM runner (aarch64)..."
    cargo zigbuild --release -p preloop-runner --target aarch64-unknown-linux-gnu || \
        say "runner build failed — host CLI works, but microVM jobs need it (see docs/setup.md)"
else
    say "skipping microVM runner cross-build (no zig/cargo-zigbuild) — see https://github.com/preloopdev/smolvm"
fi

# --- 4. install -------------------------------------------------------------

mkdir -p "$BIN_DIR"
ln -sfn "$PRELOOP_SRC/target/release/preloop" "$BINARY"
say "installed $BINARY"
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) say "add $BIN_DIR to your PATH:  export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

# --- 5. next steps ----------------------------------------------------------

cat <<EOF

[preloop] next steps:
    preloop serve                      # start the engine on 127.0.0.1:9090
    preloop setup github               # GitHub App or fine-grained PAT
    preloop doctor --repo owner/repo   # verify credentials
    cd your-repo && preloop run -f .github/workflows/ci.yml --event push

[preloop] full guide: https://github.com/preloopdev/preloop/blob/main/docs/setup.md
EOF
