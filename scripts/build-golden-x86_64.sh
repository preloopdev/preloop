#!/bin/bash
# Build the x86_64 golden on the main (x86_64 Linux) host.
# Mirrors .github/workflows/release-golden.yml's golden job for x86_64.
# Run from the repo root on the main host (Linux x86_64, smolvm + KVM available).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Building host CLI (release)"
cargo build --release -p preloop-cli

echo "==> Cross-building x86_64 runner bundle"
rustup target add x86_64-unknown-linux-gnu 2>/dev/null || true
cargo zigbuild --release -p preloop-runner --target x86_64-unknown-linux-gnu

echo "==> Building x86_64 golden (bakes tier 1-4: git-lfs/cmake/sshpass, pnpm/yarn/nvm, python 3.10, go 1.24, gh, yq)"
mkdir -p dist
rm -f dist/preloop-ubuntu-24.04-x86_64
./target/release/preloop build-golden \
  --runner-bundle target/x86_64-unknown-linux-gnu/release \
  --output dist/preloop-ubuntu-24.04-x86_64
test -s dist/preloop-ubuntu-24.04-x86_64

echo "==> Golden built: dist/preloop-ubuntu-24.04-x86_64"
echo "    Sanity-check the bake manifest inside a VM:"
echo "    ./target/release/preloop serve --listen 127.0.0.1:9090  (then run a job)"
