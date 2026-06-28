#!/usr/bin/env bash
set -euo pipefail
# Clean old capture directories.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 [--older-than <days>] [--dry-run]" >&2
    echo "  Default: --older-than 7" >&2
    exit 1
}

OLDER_THAN=7
DRY_RUN=false
while [ $# -gt 0 ]; do
    case "$1" in
        --older-than) OLDER_THAN="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) usage ;;
    esac
done

CAPTURES_ROOT="$MITM_DIR/captures"
if [ ! -d "$CAPTURES_ROOT" ]; then
    echo "no captures directory"
    exit 0
fi

echo "Cleaning captures older than ${OLDER_THAN} days..."
[ "$DRY_RUN" = true ] && echo "(dry run — no files will be deleted)"
echo ""

cleaned=0
# Find timestamp-named directories (format: YYYY-MM-DDTHH-MM-SSZ).
find "$CAPTURES_ROOT" -mindepth 3 -maxdepth 3 -type d | while read -r dir; do
    dirname=$(basename "$dir")
    # Skip 'latest' symlinks.
    [ "$dirname" = "latest" ] && continue
    # Parse timestamp.
    if [[ "$dirname" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]; then
        # Check age.
        if [ "$(find "$dir" -maxdepth 0 -mtime +"$OLDER_THAN" 2>/dev/null | wc -l)" -gt 0 ]; then
            echo "removing: $dir"
            if [ "$DRY_RUN" = false ]; then
                # Remove 'latest' symlink if it points to this dir.
                parent=$(dirname "$dir")
                if [ -L "$parent/latest" ]; then
                    target=$(readlink "$parent/latest")
                    if [ "$target" = "$dir" ] || [ "$target" = "$dirname" ]; then
                        rm -f "$parent/latest"
                    fi
                fi
                rm -rf "$dir"
            fi
            cleaned=$((cleaned + 1))
        fi
    fi
done

echo ""
echo "found $cleaned capture(s) to clean"
