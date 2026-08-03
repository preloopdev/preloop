#!/usr/bin/env bash
# setup-workspaces.sh — Prepare the four real-world workspaces for the
# conformance campaign.
#
# Per repo:
#   1. shallow clone (depth 2 — changed-file actions need HEAD^)
#   2. exact upstream workflow files copied in, with ONLY the `runs-on:`
#      labels rewritten to self-hosted (matrix values, jobs, steps untouched)
#   3. a small real change applied to the working tree so changed-file gates
#      open (paths-filter/changed-files see a non-empty diff)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
WS_ROOT="${WS_ROOT:-/tmp/conformance-workspaces}"
SRC=/tmp/conformance-src
mkdir -p "$WS_ROOT"

# runs-on rewrite: everything becomes the self-hosted label set.
rewrite_runs_on() {
  local file="$1"
  python3 - "$file" <<'PY'
import re, sys
path = sys.argv[1]
src = open(path).read()
def repl(m):
    return re.sub(r'runs-on\s*:\s*.*', 'runs-on: [self-hosted, Linux, X64]', m.group(0), count=1)
# Match `runs-on:` lines including matrix expressions and inline lists.
out = re.sub(r'runs-on:\s*(?:\[[^\]]*\]|"[^"]*"|\'[^\']*\'|\$\{\{[^}]*\}\}|\S+)', lambda m: 'runs-on: [self-hosted, Linux, X64]', src)
open(path, 'w').write(out)
PY
}

fetch_workflow() {
  local repo="$1" path="$2" dest="$3"
  gh api "repos/$repo/contents/$path" --jq '.content' 2>/dev/null | base64 -d > "$dest"
}

clone_repo() {
  local repo="$1" branch="$2" dir="$3"
  if [ ! -d "$dir/.git" ]; then
    mkdir -p "$dir"
    git clone --depth 2 --branch "$branch" "https://github.com/$repo.git" "$dir" 2>&1 | tail -1
  fi
  git -C "$dir" checkout "$branch" 2>/dev/null || true
}

# ── sharkdp/bat — master, push event, no changed-files gates ──────────
echo "=== bat ==="
clone_repo sharkdp/bat master "$WS_ROOT/bat"
fetch_workflow sharkdp/bat .github/workflows/CICD.yml "$WS_ROOT/bat/.github/workflows/CICD.yml"
rewrite_runs_on "$WS_ROOT/bat/.github/workflows/CICD.yml"

# ── vitejs/vite — main, push event, tj-actions/changed-files gate ─────
echo "=== vite ==="
clone_repo vitejs/vite main "$WS_ROOT/vite"
fetch_workflow vitejs/vite .github/workflows/ci.yml "$WS_ROOT/vite/.github/workflows/ci.yml"
rewrite_runs_on "$WS_ROOT/vite/.github/workflows/ci.yml"
# Dirty change: touch a real tracked source file so changed-files opens the gate.
echo "// conformance campaign dirty change" >> "$WS_ROOT/vite/packages/vite/src/node/index.ts"

# ── astral-sh/uv — main, pull_request event, plan.yml gates ───────────
echo "=== uv ==="
clone_repo astral-sh/uv main "$WS_ROOT/uv"
for wf in ci.yml plan.yml check-fmt.yml check-lint.yml check-docs.yml check-release.yml check-lock.yml check-zizmor.yml check-publish.yml check-generated-files.yml; do
  fetch_workflow astral-sh/uv ".github/workflows/$wf" "$WS_ROOT/uv/.github/workflows/$wf" || true
  [ -f "$WS_ROOT/uv/.github/workflows/$wf" ] && rewrite_runs_on "$WS_ROOT/uv/.github/workflows/$wf"
done
# Docs-only change: excluded from uv's code-change gate → check-* run, heavy
# test/build jobs stay skipped, mirroring the golden PR run.
echo "<!-- conformance campaign dirty change -->" >> "$WS_ROOT/uv/docs/concepts/index.md"

# ── nextcloud/server — master, pull_request event, paths-filter gate ───
echo "=== nextcloud ==="
clone_repo nextcloud/server master "$WS_ROOT/nextcloud"
fetch_workflow nextcloud/server .github/workflows/phpunit-sqlite.yml "$WS_ROOT/nextcloud/.github/workflows/phpunit-sqlite.yml"
rewrite_runs_on "$WS_ROOT/nextcloud/.github/workflows/phpunit-sqlite.yml"
# Dirty PHP change under lib/ so paths-filter's `src` filter matches.
echo "// conformance campaign dirty change" >> "$WS_ROOT/nextcloud/lib/versioncheck.php"

echo ""
echo "Workspaces ready:"
for d in bat vite uv nextcloud; do
  echo "  $WS_ROOT/$d: $(git -C $WS_ROOT/$d rev-parse --short HEAD 2>/dev/null) $(git -C $WS_ROOT/$d status --short | wc -l | tr -d ' ') dirty files"
done
