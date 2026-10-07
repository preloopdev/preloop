#!/bin/bash
# Build the x86_64 golden on the main (x86_64 Linux) host.
# Mirrors .github/workflows/release-golden.yml's golden job for x86_64.
# Run from the repo root on the main host (Linux x86_64, smolvm + KVM available).
set -euo pipefail

cd "$(dirname "$0")/.."

# The official golden is baked from the digest-pinned official runner image.
# There is no stock-Ubuntu default: `build-golden` needs the image it bakes,
# and this script bakes — it never fetches the published packed golden.
BASE_IMAGE="${BASE_IMAGE:-$(awk -F'"' '/^official_runner_image_base_amd64 = / { print $2; exit }' official-image.toml)}"
test -n "$BASE_IMAGE" || { echo "official_runner_image_base_amd64 missing from official-image.toml" >&2; exit 1; }

echo "==> Building host CLI (release)"
cargo build --release -p preloop-cli

echo "==> Cross-building x86_64 runner bundle"
rustup target add x86_64-unknown-linux-gnu 2>/dev/null || true
cargo zigbuild --release -p preloop-runner --target x86_64-unknown-linux-gnu

echo "==> Building x86_64 golden from $BASE_IMAGE"
mkdir -p dist
rm -f dist/preloop-ubuntu-24.04-x86_64
./target/release/preloop build-golden \
  --runner-bundle target/x86_64-unknown-linux-gnu/release \
  --base-image "$BASE_IMAGE" \
  --output dist/preloop-ubuntu-24.04-x86_64
test -s dist/preloop-ubuntu-24.04-x86_64

echo "==> Golden built: dist/preloop-ubuntu-24.04-x86_64"
echo "    The bake adds the GitHub-runner machinery only; /etc/preloop-bake.json"
echo "    inside a VM records the image and what the bake resolved."
echo "    Sanity-check it with ./target/release/preloop serve --listen 127.0.0.1:9090 (then run a job)"
