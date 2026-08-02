#!/usr/bin/env bash
# generate-report.sh — Produce the conformance campaign report from captures.
set -uo pipefail
cd "$(dirname "$0")/../../.."

OUT=benchmarks/real-world/results/conformance-4repos
REPORT="$OUT/REPORT.md"

{
cat <<'EOF'
# Real-World Conformance Campaign — Official Runner vs aksh Server

Cells:
- **A** (golden): official runner vs GitHub — recent successful runs, captured via the GitHub API
- **B**: official runner v2.336.0 vs local aksh server
- **C**: aksh runner vs local aksh server

Repos: sharkdp/bat, vitejs/vite, astral-sh/uv, nextcloud/server.
Workflows are the exact upstream files; only `runs-on:` labels were rewritten
to `[self-hosted, Linux, X64]`.

EOF

for repo in bat vite uv nextcloud; do
  for cell in official aksh; do
    echo "## $repo / $cell"
    echo ""
    if [ -f "$OUT/$repo/$cell/run.json" ]; then
      python3 benchmarks/real-world/conformance-4repos/compare-goldens.py --repo "$repo" 2>/dev/null \
        | grep -A100 "== $repo/$cell" | head -60
    else
      echo "_not captured yet_"
    fi
    echo ""
  done
done

echo "---"
echo "Generated $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$REPORT"
echo "wrote $REPORT"
