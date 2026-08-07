#!/usr/bin/env bash
# preloop — one-line installer.
#
# Downloads the prebuilt release binaries (preloop, preloop-server,
# preloop-runner) from GitHub Releases, verifies the sha256 checksum, and
# installs them into ~/.local/bin (or /usr/local/bin when that is writable).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh
#   install.sh --version v0.25 --dir "$HOME/bin"
#   install.sh --dry-run            # print the plan without touching anything
#
# Options:
#   --version <tag>   Release to install (default: latest). "v" prefix optional.
#   --dir <path>      Install directory (default: ~/.local/bin, else /usr/local/bin).
#   --skip-doctor     Do not run `preloop doctor` after installing.
#   --dry-run         Resolve everything and print the plan, then exit 0.

set -euo pipefail

VERSION="latest"
DIR=""
SKIP_DOCTOR=0
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
    --dir) DIR="${2:?--dir needs a value}"; shift 2 ;;
    --skip-doctor) SKIP_DOCTOR=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -20
      exit 0 ;;
    *) echo "install.sh: unknown option: $1" >&2; exit 2 ;;
  esac
done

say()  { printf '\033[1;32m%s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m%s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m%s\033[0m\n' "$*" >&2; exit 1; }

REPO="preloopdev/preloop"
GITHUB="https://github.com/$REPO"

# --- platform detection -----------------------------------------------------
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
  linux)  os="linux" ;;
  darwin) os="darwin" ;;
  *) die "unsupported operating system: $os (want linux or darwin)" ;;
esac

arch="$(uname -m | tr '[:upper:]' '[:lower:]')"
case "$arch" in
  x86_64|amd64)  arch="x86_64" ;;
  aarch64|arm64) arch="aarch64" ;;
  *) die "unsupported architecture: $arch (want x86_64 or aarch64)" ;;
esac

# --- resolve the release ----------------------------------------------------
case "$VERSION" in
  latest)
    api="https://api.github.com/repos/$REPO/releases/latest"
    VERSION="$(curl -fsSL "$api" 2>/dev/null | grep -o '"tag_name"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/' || true)"
    if [ -z "$VERSION" ] && command -v gh >/dev/null 2>&1; then
      # Unauthenticated API access fails while the repository is private;
      # the authenticated client resolves the tag for us.
      VERSION="$(gh api "repos/$REPO/releases/latest" --jq .tag_name 2>/dev/null || true)"
    fi
    [ -n "$VERSION" ] || die "could not resolve the latest release from $api"
    ;;
  v*) ;;
  *) VERSION="v$VERSION" ;;
esac

ARTIFACT="preloop-${os}-${arch}-${VERSION}.tar.gz"
URL="$GITHUB/releases/download/$VERSION/$ARTIFACT"
SHA_URL="$URL.sha256"

# --- install directory ------------------------------------------------------
if [ -z "$DIR" ]; then
  if [ -n "${HOME:-}" ] && [ -d "$HOME" ] && mkdir -p "$HOME/.local/bin" 2>/dev/null; then
    DIR="$HOME/.local/bin"
  elif mkdir -p /usr/local/bin 2>/dev/null; then
    DIR="/usr/local/bin"
  else
    die "could not create an install directory; pass --dir <path>"
  fi
fi
mkdir -p "$DIR"

say "preloop installer"
echo "  version:   $VERSION"
echo "  platform:  $os/$arch"
echo "  artifact:  $ARTIFACT"
echo "  install:   $DIR"

# --- no prebuilt binary? say so before downloading --------------------------
case "$os/$arch" in
  linux/x86_64|linux/aarch64) ;;
  darwin/*)
    # The macOS builds are not published yet. Fail fast with the source path.
    warn "no prebuilt binary for darwin/$arch yet — build from source:"
    warn "  cargo build --release -p preloop-cli -p aksh-runner-server -p aksh-runner"
    warn "  (binaries land in target/release/{preloop,preloop-server,preloop-runner})"
    [ "$DRY_RUN" -eq 1 ] && exit 0
    exit 1
    ;;
esac

[ "$DRY_RUN" -eq 1 ] && { echo "dry run: nothing downloaded or installed"; exit 0; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

download() { # url, dest
  if curl -fsSL "$1" -o "$2" 2>/dev/null; then
    return 0
  fi
  # The repo may be private (or the CDN unreachable): fall back to the
  # authenticated `gh` client, which also covers private repositories.
  if command -v gh >/dev/null 2>&1; then
    local asset="$2"
    (cd "$TMP" && gh release download "$VERSION" --repo "$REPO" --pattern "$(basename "$1")" --clobber >/dev/null 2>&1) \
      && mv "$TMP/$(basename "$1")" "$asset" \
      && return 0
  fi
  return 1
}

echo "downloading $ARTIFACT ..."
download "$URL" "$TMP/$ARTIFACT" \
  || die "download failed: $URL (is the release published? try --version)"
download "$SHA_URL" "$TMP/$ARTIFACT.sha256" \
  || warn "checksum file unavailable; skipping verification"

if [ -f "$TMP/$ARTIFACT.sha256" ]; then
  echo "verifying sha256 ..."
  # cargo-dist bakes the build machine's absolute path into the .sha256
  # file, so `-c` cannot be trusted to find our copy: compare hashes by
  # value instead.
  expected="$(awk '{print $1}' "$TMP/$ARTIFACT.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$TMP/$ARTIFACT" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$TMP/$ARTIFACT" | awk '{print $1}')"
  fi
  [ -n "$expected" ] && [ "$actual" = "$expected" ] \
    || die "checksum verification failed — refusing to install"
fi

echo "extracting ..."
tar -xzf "$TMP/$ARTIFACT" -C "$TMP"

for bin in preloop preloop-server preloop-runner; do
  [ -f "$TMP/$bin" ] || die "archive is missing $bin (broken release?)"
  install -m 0755 "$TMP/$bin" "$DIR/$bin"
  say "installed $DIR/$bin"
done

if ! printf '%s' "$PATH" | tr ':' '\n' | grep -qx "$DIR"; then
  warn "$DIR is not on your PATH — add it, e.g.:"
  echo "  echo 'export PATH=\"$DIR:\$PATH\"' >> ~/.$(basename "${SHELL:-sh}")rc"
fi

if [ "$SKIP_DOCTOR" -eq 0 ]; then
  echo
  say "checking your setup:"
  if "$DIR/preloop" doctor 2>&1; then
    :
  else
    warn "doctor found something to fix — the checks above show what."
  fi
fi

cat <<EOF

$(say "done. next steps:")
  1. preloop setup github            # configure the GitHub App or PAT once
  2. preloop serve                   # start the control plane + runner pool
  3. preloop run -f .github/workflows/ci.yml
     preloop run --push --create-pr  # run CI, then push the tested commit
                                     # and open a draft PR

  Docs: https://github.com/$REPO#readme
EOF
