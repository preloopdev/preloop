#!/usr/bin/env bash
set -euo pipefail
# Clean stale runner binary caches. Keeps the current version only.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

RUNNER_VERSION=$(grep runner_version "$MITM_DIR/versions.toml" | cut -d'"' -f2)

echo "Current runner version: v$RUNNER_VERSION"
echo ""

cleaned=0
for d in "$CACHE"/runner-*/; do
    [ -d "$d" ] || continue
    dir_name=$(basename "$d")
    # Check if this dir has the current version's runner binary.
    if [ -f "$d/run.sh" ]; then
        # Try to detect version from the binary.
        if [ -f "$d/bin/Runner.Listener" ]; then
            dir_version=$("$d/bin/Runner.Listener" --version 2>/dev/null || echo "unknown")
            if [ "$dir_version" != "$RUNNER_VERSION" ] && [ "$dir_version" != "unknown" ]; then
                echo "removing stale cache: $d (version $dir_version)"
                rm -rf "$d"
                cleaned=$((cleaned + 1))
            else
                echo "keeping: $d (version $dir_version)"
            fi
        else
            echo "keeping: $d (no version info)"
        fi
    else
        echo "removing incomplete cache: $d"
        rm -rf "$d"
        cleaned=$((cleaned + 1))
    fi
done

echo ""
echo "cleaned $cleaned stale cache(s)"
